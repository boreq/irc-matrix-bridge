use std::fmt;

use uuid::Uuid;

use crate::domain::channel::Channel;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageId(pub Uuid);

impl MessageId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Normal,
    Action,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub source: Channel,
    pub external_id: Option<String>,
    pub sender: String,
    pub kind: MessageKind,
    pub body: String,
}

impl Message {
    pub fn relay_line(&self, text: &str) -> String {
        match self.kind {
            MessageKind::Normal => format!("<{}> {}", self.sender, text),
            MessageKind::Action => format!("* {} {}", self.sender, text),
        }
    }

    pub fn relay_text(&self) -> String {
        self.relay_line(&self.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::channel::IrcChannel;

    fn message(kind: MessageKind) -> Message {
        Message {
            source: Channel::Irc(IrcChannel::new("libera", "#rust")),
            external_id: None,
            sender: "alice".into(),
            kind,
            body: "hello".into(),
        }
    }

    #[test]
    fn prepends_sender_nick() {
        assert_eq!(message(MessageKind::Normal).relay_text(), "<alice> hello");
        assert_eq!(message(MessageKind::Action).relay_text(), "* alice hello");
    }
}
