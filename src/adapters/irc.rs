use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use irc::client::Client;

use crate::application::ports::{MemberLister, MessageSender};
use crate::domain::{Channel, Delivery, IrcChannelName, MemberName};

const MAX_LINE_BYTES: usize = 400;

#[derive(Default)]
struct Connection {
    client: Option<Arc<Client>>,
    joined: HashSet<IrcChannelName>,
}

impl Connection {
    fn client_in(&self, channel: &IrcChannelName) -> anyhow::Result<&Client> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("not connected"))?;
        if !self.joined.contains(channel) {
            bail!("not joined to {channel}");
        }
        Ok(client)
    }
}

#[derive(Default)]
pub struct IrcMessageSender {
    connection: Mutex<Connection>,
}

impl IrcMessageSender {
    pub fn connected(&self, client: Arc<Client>) {
        let mut connection = self.connection.lock().unwrap();
        connection.client = Some(client);
        connection.joined.clear();
    }

    pub fn disconnected(&self) {
        *self.connection.lock().unwrap() = Connection::default();
    }

    pub fn joined(&self, channel: IrcChannelName) {
        self.connection.lock().unwrap().joined.insert(channel);
    }

    pub fn left(&self, channel: &IrcChannelName) {
        self.connection.lock().unwrap().joined.remove(channel);
    }

    pub fn notice(&self, channel: &IrcChannelName, text: &str) -> anyhow::Result<()> {
        let connection = self.connection.lock().unwrap();
        let client = connection.client_in(channel)?;
        let budget = MAX_LINE_BYTES.saturating_sub(channel.as_str().len());
        for chunk in split_at_byte_limit(text, budget) {
            client.send_notice(channel.as_str(), chunk)?;
        }
        Ok(())
    }
}

#[async_trait]
impl MessageSender for IrcMessageSender {
    async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
        let Channel::Irc(destination) = delivery.destination() else {
            bail!("{} is not an irc channel", delivery.destination());
        };
        let channel = destination.name();
        let connection = self.connection.lock().unwrap();
        let client = connection.client_in(channel)?;

        let message = delivery.message();
        let overhead = message.relay_line("").len();
        let budget = MAX_LINE_BYTES
            .saturating_sub(channel.as_str().len() + overhead)
            .max(64);
        for line in message
            .body()
            .as_str()
            .lines()
            .filter(|line| !line.trim().is_empty())
        {
            for chunk in split_at_byte_limit(line, budget) {
                client.send_privmsg(channel.as_str(), message.relay_line(chunk))?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl MemberLister for IrcMessageSender {
    async fn members(&self, channel: &Channel) -> anyhow::Result<Vec<MemberName>> {
        let Channel::Irc(irc_channel) = channel else {
            bail!("{channel} is not an irc channel");
        };
        let connection = self.connection.lock().unwrap();
        let client = connection.client_in(irc_channel.name())?;
        let users = client
            .list_channels()
            .into_iter()
            .flatten()
            .find(|name| IrcChannelName::new(name).is_ok_and(|name| &name == irc_channel.name()))
            .and_then(|name| client.list_users(&name))
            .with_context(|| format!("no member list for {channel}"))?;
        let me = client.current_nickname();
        Ok(users
            .iter()
            .map(|user| user.get_nickname())
            .filter(|nick| !nick.eq_ignore_ascii_case(me))
            .filter_map(|nick| MemberName::new(nick).ok())
            .collect())
    }
}

fn split_at_byte_limit(mut text: &str, limit: usize) -> Vec<&str> {
    let mut chunks = Vec::new();
    while text.len() > limit {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        if let Some(space) = text[..end].rfind(' ').filter(|&i| i > 0) {
            end = space;
        }
        chunks.push(&text[..end]);
        text = text[end..].trim_start();
    }
    if !text.is_empty() {
        chunks.push(text);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_on_spaces_and_char_boundaries() {
        assert_eq!(
            split_at_byte_limit("hello world", 8),
            vec!["hello", "world"]
        );
        assert_eq!(
            split_at_byte_limit("ééééé", 3),
            vec!["é", "é", "é", "é", "é"]
        );
        assert_eq!(split_at_byte_limit("short", 100), vec!["short"]);
    }
}
