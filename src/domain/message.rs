use crate::domain::endpoint::Endpoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Text,
    Emote,
}

impl MessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Emote => "emote",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "emote" => Some(Self::Emote),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub source: Endpoint,
    pub external_id: Option<String>,
    pub sender: String,
    pub kind: MessageKind,
    pub body: String,
}

impl Message {
    pub fn relay_line(&self, text: &str) -> String {
        match self.kind {
            MessageKind::Text => format!("<{}> {}", self.sender, text),
            MessageKind::Emote => format!("* {} {}", self.sender, text),
        }
    }

    pub fn relay_text(&self) -> String {
        self.relay_line(&self.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::endpoint::NetworkId;

    fn message(kind: MessageKind) -> Message {
        Message {
            source: Endpoint::new(NetworkId::new("irc:libera"), "#rust"),
            external_id: None,
            sender: "alice".into(),
            kind,
            body: "hello".into(),
        }
    }

    #[test]
    fn prepends_sender_nick() {
        assert_eq!(message(MessageKind::Text).relay_text(), "<alice> hello");
        assert_eq!(message(MessageKind::Emote).relay_text(), "* alice hello");
    }
}
