use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use irc::client::Client;
use irc::client::data::Config as IrcClientConfig;
use irc::proto::{Command, Message as IrcMessage};
use sha2::{Digest, Sha256};

use crate::adapters::irc::IrcMessageSender;
use crate::application::commands::store_received_message::{
    StoreReceivedMessage, StoreReceivedMessageHandler, StoreReceivedMessageOutcome,
};
use crate::application::ports::{ConnectionHealth, DeliveryWaker};
use crate::config::IrcNetworkConfig;
use crate::domain::{
    Channel, IrcChannel, IrcChannelName, Message, MessageBody, MessageKind, Network,
};

pub struct IrcEntrypoint {
    pub config: IrcNetworkConfig,
    /// PEM file with the TLS client certificate and key services know the
    /// bridge by (CertFP), generated on first launch.
    pub client_cert: PathBuf,
    pub channels: Vec<IrcChannelName>,
    pub store_received_message: Arc<StoreReceivedMessageHandler>,
    pub sender: Arc<IrcMessageSender>,
    pub waker: Arc<dyn DeliveryWaker>,
    pub health: Arc<dyn ConnectionHealth>,
}

impl IrcEntrypoint {
    pub async fn run(self) {
        let client_cert = if self.config.tls() {
            match self.load_client_cert() {
                Ok(pem) => Some(pem),
                Err(error) => {
                    tracing::error!(network = %self.config.name(), "{error:#}");
                    return;
                }
            }
        } else {
            None
        };

        let mut backoff = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            match self.session(client_cert.as_deref()).await {
                Ok(()) => tracing::warn!(network = %self.config.name(), "irc connection closed"),
                Err(error) => {
                    tracing::warn!(network = %self.config.name(), "irc connection failed: {error:#}")
                }
            }
            self.sender.disconnected();

            if started.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_secs(1);
            }
            tracing::info!(network = %self.config.name(), "reconnecting in {backoff:?}");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(300));
        }
    }

    fn load_client_cert(&self) -> anyhow::Result<String> {
        let path = &self.client_cert;
        let pem = load_or_generate_cert(path)
            .with_context(|| format!("client certificate {}", path.display()))?;
        let fingerprint = fingerprint(&pem)?;
        tracing::info!(
            network = %self.config.name(),
            "client certificate fingerprint (sha256) {fingerprint}, register it with `/msg NickServ CERT ADD {fingerprint}`"
        );
        Ok(pem)
    }

    async fn session(&self, client_cert: Option<&str>) -> anyhow::Result<()> {
        let mut client = Client::from_config(self.client_config(client_cert)).await?;
        client.identify()?;
        let mut stream = client.stream()?;
        self.sender.connected(client.sender());
        tracing::info!(network = %self.config.name(), "connected to irc");

        while let Some(message) = stream.next().await.transpose()? {
            self.health
                .record_healthy(&Network::Irc(self.config.name().clone()));
            tracing::debug!(network = %self.config.name(), "[RECV] {}", message.to_string().trim_end());
            self.on_message(&client, message).await;
        }
        Ok(())
    }

    fn client_config(&self, client_cert: Option<&str>) -> IrcClientConfig {
        let config = &self.config;
        IrcClientConfig {
            nickname: Some(config.nickname().to_owned()),
            ping_time: Some(60),
            alt_nicks: vec![
                format!("{}_", config.nickname()),
                format!("{}__", config.nickname()),
            ],
            username: Some(config.nickname().to_owned()),
            realname: Some("irc-bridge".into()),
            server: Some(config.server().to_owned()),
            port: Some(config.port()),
            use_tls: Some(config.tls()),
            password: config.password().map(ToOwned::to_owned),
            nick_password: config.nick_password().map(ToOwned::to_owned),
            // With rustls the irc crate reads the certificate from the path and
            // the private key from the PEM text in `client_cert_pass`.
            client_cert_path: client_cert.map(|_| self.client_cert.to_string_lossy().into_owned()),
            client_cert_pass: client_cert.map(ToOwned::to_owned),
            channels: self
                .channels
                .iter()
                .map(|channel| channel.as_str().to_owned())
                .collect(),
            ..IrcClientConfig::default()
        }
    }

    async fn on_message(&self, client: &Client, message: IrcMessage) {
        let me = client.current_nickname();
        let from = message.source_nickname();
        let from_me = from.is_some_and(|nick| nick.eq_ignore_ascii_case(me));

        match &message.command {
            Command::JOIN(channels, _, _) if from_me => {
                for channel in channel_names(channels) {
                    tracing::info!(network = %self.config.name(), %channel, "joined");
                    self.sender.joined(channel);
                }
                self.waker.wake(&Network::Irc(self.config.name().clone()));
            }
            Command::PART(channels, _) if from_me => {
                channel_names(channels).for_each(|channel| self.sender.left(&channel));
            }
            Command::KICK(channel, nick, reason) if nick.eq_ignore_ascii_case(me) => {
                tracing::warn!(network = %self.config.name(), %channel, ?reason, "kicked, rejoining");
                channel_names(channel).for_each(|name| self.sender.left(&name));
                if let Err(error) = client.send_join(channel) {
                    tracing::warn!("rejoin failed: {error}");
                }
            }
            Command::PRIVMSG(target, text) if !from_me => {
                let Some(nick) = from else { return };
                // Private messages to the bridge target its nick, not a channel.
                let Ok(channel) = IrcChannelName::new(target) else {
                    return;
                };
                let Some((kind, body)) = parse_privmsg(text) else {
                    return;
                };
                self.store(channel, nick, kind, body).await;
            }
            _ => {}
        }
    }

    async fn store(
        &self,
        channel: IrcChannelName,
        nick: &str,
        kind: MessageKind,
        body: MessageBody,
    ) {
        let message = Message::new(
            Channel::Irc(IrcChannel::new(self.config.name().clone(), channel.clone())),
            None,
            nick.to_owned(),
            kind,
            body,
        );
        match self
            .store_received_message
            .handle(StoreReceivedMessage { message })
            .await
        {
            Ok(
                StoreReceivedMessageOutcome::Stored
                | StoreReceivedMessageOutcome::NotBridged
                | StoreReceivedMessageOutcome::Duplicate,
            ) => {}
            Err(error) => {
                tracing::error!(network = %self.config.name(), %channel, "storing message failed: {error:#}")
            }
        }
    }
}

/// Reads the PEM file with the certificate and key, generating a self-signed
/// one on first use.
fn load_or_generate_cert(path: &Path) -> anyhow::Result<String> {
    if path.exists() {
        return Ok(std::fs::read_to_string(path)?);
    }
    let certified = rcgen::generate_simple_self_signed(Vec::<String>::new())?;
    let pem = format!(
        "{}{}",
        certified.cert.pem(),
        certified.signing_key.serialize_pem()
    );

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)?.write_all(pem.as_bytes())?;
    tracing::info!(path = %path.display(), "generated irc client certificate");
    Ok(pem)
}

/// SHA-256 of the certificate as lowercase hex, the format hackint's NickServ uses.
fn fingerprint(pem: &str) -> anyhow::Result<String> {
    let cert = pem::parse_many(pem)?
        .into_iter()
        .find(|block| block.tag() == "CERTIFICATE")
        .context("no certificate in pem")?;
    Ok(Sha256::digest(cert.contents())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn channel_names(channels: &str) -> impl Iterator<Item = IrcChannelName> {
    channels
        .split(',')
        .filter_map(|channel| IrcChannelName::new(channel).ok())
}

fn parse_privmsg(text: &str) -> Option<(MessageKind, MessageBody)> {
    let (kind, text) = match text.strip_prefix('\u{1}') {
        Some(ctcp) => {
            let action = ctcp.trim_end_matches('\u{1}').strip_prefix("ACTION ")?;
            (MessageKind::Action, action)
        }
        None => (MessageKind::Normal, text),
    };
    let body = MessageBody::new(&strip_formatting(text)).ok()?;
    Some((kind, body))
}

fn strip_formatting(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\u{2}' | '\u{f}' | '\u{11}' | '\u{16}' | '\u{1d}' | '\u{1e}' | '\u{1f}' => {}
            '\u{3}' => {
                skip_digits(&mut chars);
                if chars.peek() == Some(&',') {
                    let mut lookahead = chars.clone();
                    lookahead.next();
                    if lookahead.peek().is_some_and(char::is_ascii_digit) {
                        chars.next();
                        skip_digits(&mut chars);
                    }
                }
            }
            c => out.push(c),
        }
    }
    out
}

fn skip_digits(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for _ in 0..2 {
        if chars.next_if(char::is_ascii_digit).is_none() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_actions_and_drops_other_ctcp() {
        let body = |text| MessageBody::new(text).unwrap();
        assert_eq!(
            parse_privmsg("\u{1}ACTION waves\u{1}"),
            Some((MessageKind::Action, body("waves")))
        );
        assert_eq!(parse_privmsg("\u{1}VERSION\u{1}"), None);
        assert_eq!(parse_privmsg("hi"), Some((MessageKind::Normal, body("hi"))));
        assert_eq!(parse_privmsg("\u{2}\u{2} "), None);
    }

    #[test]
    fn generates_client_cert_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");

        let pem = load_or_generate_cert(&path).unwrap();

        assert!(pem.contains("BEGIN CERTIFICATE") && pem.contains("BEGIN PRIVATE KEY"));
        assert_eq!(load_or_generate_cert(&path).unwrap(), pem);
    }

    #[test]
    fn strips_formatting_codes() {
        assert_eq!(
            strip_formatting("\u{2}bold\u{2} \u{3}04,12red\u{3} 5,5"),
            "bold red 5,5"
        );
        assert_eq!(strip_formatting("\u{3}4,x"), ",x");
    }
}
