use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use irc::client::Client;
use irc::client::data::Config as IrcClientConfig;
use irc::proto::{Command, Message as IrcMessage};

use crate::adapters::irc::IrcMessageSender;
use crate::application::commands::store_received_message::{
    StoreReceivedMessage, StoreReceivedMessageHandler, StoreReceivedMessageOutcome,
};
use crate::application::ports::DeliveryWaker;
use crate::config::IrcNetworkConfig;
use crate::domain::channel::{Channel, IrcChannel, IrcChannelName, Network};
use crate::domain::message::{Message, MessageKind};

pub struct IrcEntrypoint {
    pub config: IrcNetworkConfig,
    pub channels: Vec<IrcChannelName>,
    pub store_received_message: Arc<StoreReceivedMessageHandler>,
    pub sender: Arc<IrcMessageSender>,
    pub waker: Arc<dyn DeliveryWaker>,
}

impl IrcEntrypoint {
    pub async fn run(self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            match self.session().await {
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

    async fn session(&self) -> anyhow::Result<()> {
        let mut client = Client::from_config(self.client_config()).await?;
        client.identify()?;
        let mut stream = client.stream()?;
        self.sender.connected(client.sender());
        tracing::info!(network = %self.config.name(), "connected to irc");

        while let Some(message) = stream.next().await.transpose()? {
            self.on_message(&client, message).await;
        }
        Ok(())
    }

    fn client_config(&self) -> IrcClientConfig {
        let config = &self.config;
        IrcClientConfig {
            nickname: Some(config.nickname().to_owned()),
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

    async fn store(&self, channel: IrcChannelName, nick: &str, kind: MessageKind, body: String) {
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

fn channel_names(channels: &str) -> impl Iterator<Item = IrcChannelName> {
    channels
        .split(',')
        .filter_map(|channel| IrcChannelName::new(channel).ok())
}

fn parse_privmsg(text: &str) -> Option<(MessageKind, String)> {
    let (kind, text) = match text.strip_prefix('\u{1}') {
        Some(ctcp) => {
            let action = ctcp.trim_end_matches('\u{1}').strip_prefix("ACTION ")?;
            (MessageKind::Action, action)
        }
        None => (MessageKind::Normal, text),
    };
    let text = strip_formatting(text);
    (!text.trim().is_empty()).then_some((kind, text))
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
        assert_eq!(
            parse_privmsg("\u{1}ACTION waves\u{1}"),
            Some((MessageKind::Action, "waves".into()))
        );
        assert_eq!(parse_privmsg("\u{1}VERSION\u{1}"), None);
        assert_eq!(
            parse_privmsg("hi"),
            Some((MessageKind::Normal, "hi".into()))
        );
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
