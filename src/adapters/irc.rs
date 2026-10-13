use std::collections::HashSet;
use std::sync::Mutex;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use irc::client::Sender;

use crate::application::ports::MessageSender;
use crate::domain::channel::{Channel, IrcChannel};
use crate::domain::delivery::Delivery;

const MAX_LINE_BYTES: usize = 400;

#[derive(Default)]
struct Connection {
    sender: Option<Sender>,
    joined: HashSet<String>,
}

#[derive(Default)]
pub struct IrcMessageSender {
    connection: Mutex<Connection>,
}

impl IrcMessageSender {
    pub fn connected(&self, sender: Sender) {
        let mut connection = self.connection.lock().unwrap();
        connection.sender = Some(sender);
        connection.joined.clear();
    }

    pub fn disconnected(&self) {
        *self.connection.lock().unwrap() = Connection::default();
    }

    pub fn joined(&self, channel: &str) {
        self.connection
            .lock()
            .unwrap()
            .joined
            .insert(IrcChannel::normalize_name(channel));
    }

    pub fn left(&self, channel: &str) {
        self.connection
            .lock()
            .unwrap()
            .joined
            .remove(&IrcChannel::normalize_name(channel));
    }
}

#[async_trait]
impl MessageSender for IrcMessageSender {
    async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
        let Channel::Irc(destination) = &delivery.destination else {
            bail!("{} is not an irc channel", delivery.destination);
        };
        let channel = destination.name();
        let connection = self.connection.lock().unwrap();
        let sender = connection
            .sender
            .as_ref()
            .ok_or_else(|| anyhow!("not connected"))?;
        if !connection.joined.contains(channel) {
            bail!("not joined to {channel}");
        }

        let message = &delivery.message;
        let overhead = message.relay_line("").len();
        let budget = MAX_LINE_BYTES
            .saturating_sub(channel.len() + overhead)
            .max(64);
        for line in message.body.lines().filter(|line| !line.trim().is_empty()) {
            for chunk in split_at_byte_limit(line, budget) {
                sender.send_privmsg(channel, message.relay_line(chunk))?;
            }
        }
        Ok(())
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
