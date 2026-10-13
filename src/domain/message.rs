use std::fmt;

use uuid::Uuid;

use crate::domain::channel::Channel;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MessageId {
    uuid: Uuid,
}

impl MessageId {
    pub fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }

    pub fn generate() -> Self {
        Self {
            uuid: Uuid::new_v4(),
        }
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.uuid.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Normal,
    Action,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    source: Channel,
    external_id: Option<String>,
    sender: String,
    kind: MessageKind,
    body: String,
}

impl Message {
    pub fn new(
        source: Channel,
        external_id: Option<String>,
        sender: String,
        kind: MessageKind,
        body: String,
    ) -> Self {
        Self {
            source,
            external_id,
            sender,
            kind,
            body,
        }
    }

    pub fn source(&self) -> &Channel {
        &self.source
    }

    pub fn external_id(&self) -> Option<&str> {
        self.external_id.as_deref()
    }

    pub fn sender(&self) -> &str {
        &self.sender
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    pub fn body(&self) -> &str {
        &self.body
    }

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
    use crate::domain::channel::{IrcChannel, IrcChannelName, IrcNetworkName};

    fn message(kind: MessageKind) -> Message {
        Message::new(
            Channel::Irc(IrcChannel::new(
                IrcNetworkName::new("libera"),
                IrcChannelName::new("#channel").unwrap(),
            )),
            None,
            "alice".into(),
            kind,
            "hello".into(),
        )
    }

    #[test]
    fn prepends_sender_nick() {
        assert_eq!(message(MessageKind::Normal).relay_text(), "<alice> hello");
        assert_eq!(message(MessageKind::Action).relay_text(), "* alice hello");
    }
}
