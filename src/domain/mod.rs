pub mod metrics;
pub mod time;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Duration;

use anyhow::anyhow;
use uuid::Uuid;

use crate::domain::time::DateTime;
use crate::errors::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Bridges {
    bridges: Vec<Bridge>,
}

impl Bridges {
    pub fn new(bridges: Vec<Bridge>) -> Result<Self> {
        let mut names = HashSet::new();
        let mut owners: HashMap<Channel, &BridgeName> = HashMap::new();
        for bridge in &bridges {
            if !names.insert(&bridge.name) {
                return Err(Error::DuplicateBridgeName {
                    name: bridge.name.clone(),
                });
            }
            let channels = [
                Channel::Matrix(bridge.matrix.clone()),
                Channel::Irc(bridge.irc.clone()),
            ];
            for channel in channels {
                if let Some(first) = owners.insert(channel.clone(), &bridge.name) {
                    return Err(Error::ChannelInTwoBridges {
                        channel,
                        first: first.clone(),
                        second: bridge.name.clone(),
                    });
                }
            }
        }
        Ok(Self { bridges })
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bridge> {
        self.bridges.iter()
    }

    pub fn destination(&self, source: &Channel) -> Option<Channel> {
        match source {
            Channel::Matrix(room) => self
                .bridges
                .iter()
                .find(|bridge| &bridge.matrix == room)
                .map(|bridge| Channel::Irc(bridge.irc.clone())),
            Channel::Irc(channel) => self
                .bridges
                .iter()
                .find(|bridge| &bridge.irc == channel)
                .map(|bridge| Channel::Matrix(bridge.matrix.clone())),
        }
    }

    pub fn matrix_rooms(&self) -> impl Iterator<Item = &MatrixRoom> {
        self.bridges.iter().map(|bridge| &bridge.matrix)
    }

    pub fn irc_channels(&self, network: &IrcNetworkName) -> impl Iterator<Item = &IrcChannel> {
        self.bridges
            .iter()
            .map(|bridge| &bridge.irc)
            .filter(move |channel| channel.network() == network)
    }
}

#[derive(Clone, Debug)]
pub struct Bridge {
    name: BridgeName,
    matrix: MatrixRoom,
    irc: IrcChannel,
}

impl Bridge {
    pub fn new(name: BridgeName, matrix: MatrixRoom, irc: IrcChannel) -> Self {
        Self { name, matrix, irc }
    }

    pub fn name(&self) -> &BridgeName {
        &self.name
    }

    pub fn matrix(&self) -> &MatrixRoom {
        &self.matrix
    }

    pub fn irc(&self) -> &IrcChannel {
        &self.irc
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BridgeName {
    name: String,
}

impl BridgeName {
    pub fn new(name: &str) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::Unknown(anyhow!("bridge name is empty")));
        }
        Ok(Self {
            name: name.to_owned(),
        })
    }
}

impl fmt::Display for BridgeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(Clone, Debug)]
pub struct Delivery {
    id: MessageId,
    message: Message,
    destination: Channel,
    attempts: u32,
    next_attempt_at: DateTime,
}

impl Delivery {
    pub fn new(
        id: MessageId,
        message: Message,
        destination: Channel,
        attempts: u32,
        next_attempt_at: DateTime,
    ) -> Self {
        Self {
            id,
            message,
            destination,
            attempts,
            next_attempt_at,
        }
    }

    pub fn id(&self) -> MessageId {
        self.id
    }

    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn destination(&self) -> &Channel {
        &self.destination
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    pub fn next_attempt_at(&self) -> DateTime {
        self.next_attempt_at
    }
}

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

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 30,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10 * 60),
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay,
        }
    }

    pub fn delay_after(&self, failed_attempts: u32) -> Option<Duration> {
        if failed_attempts >= self.max_attempts {
            return None;
        }
        let exp = failed_attempts.saturating_sub(1).min(30);
        Some(self.base_delay.saturating_mul(1 << exp).min(self.max_delay))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    source: Channel,
    matrix_event_id: Option<MatrixEventId>,
    sender: String,
    kind: MessageKind,
    body: MessageBody,
}

impl Message {
    pub fn new(
        source: Channel,
        matrix_event_id: Option<MatrixEventId>,
        sender: String,
        kind: MessageKind,
        body: MessageBody,
    ) -> Self {
        Self {
            source,
            matrix_event_id,
            sender,
            kind,
            body,
        }
    }

    pub fn source(&self) -> &Channel {
        &self.source
    }

    pub fn matrix_event_id(&self) -> Option<&MatrixEventId> {
        self.matrix_event_id.as_ref()
    }

    pub fn sender(&self) -> &str {
        &self.sender
    }

    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    pub fn body(&self) -> &MessageBody {
        &self.body
    }

    pub fn relay_line(&self, text: &str) -> String {
        match self.kind {
            MessageKind::Normal => format!("<{}> {}", self.sender, text),
            MessageKind::Action => format!("* {} {}", self.sender, text),
        }
    }

    pub fn relay_text(&self) -> String {
        self.relay_line(self.body.as_str())
    }

    pub fn command(&self) -> Option<BotCommand> {
        match self.kind {
            MessageKind::Normal => BotCommand::parse(&self.body),
            MessageKind::Action => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatrixEventId {
    id: String,
}

impl MatrixEventId {
    pub fn new(id: &str) -> Result<Self> {
        if !id.starts_with('$') {
            return Err(Error::Unknown(anyhow!(
                "matrix event id `{id}` must start with `$`"
            )));
        }
        if id.len() > 255 {
            return Err(Error::Unknown(anyhow!(
                "matrix event id `{id}` is longer than 255 bytes"
            )));
        }
        Ok(Self { id: id.to_owned() })
    }

    pub fn as_str(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Normal,
    Action,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageBody {
    text: String,
}

impl MessageBody {
    pub fn new(text: &str) -> Result<Self> {
        if text.trim().is_empty() {
            return Err(Error::Unknown(anyhow!("message body is empty")));
        }
        Ok(Self {
            text: text.to_owned(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BotCommand {
    WhoIsOnTheOtherSide,
}

impl BotCommand {
    fn parse(body: &MessageBody) -> Option<Self> {
        match body.as_str().trim() {
            ":whoisontheotherside" | ":bridge" => Some(Self::WhoIsOnTheOtherSide),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Members {
    names: Vec<MemberName>,
}

impl Members {
    pub fn new(mut names: Vec<MemberName>) -> Self {
        names.sort_by_cached_key(|name| name.name.to_lowercase());
        Self { names }
    }

    pub fn reply(&self) -> String {
        if self.names.is_empty() {
            return "nobody is on the other side".into();
        }
        let names: Vec<&str> = self.names.iter().map(MemberName::as_str).collect();
        format!("on the other side ({}): {}", names.len(), names.join(", "))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberName {
    name: String,
}

impl MemberName {
    pub fn new(name: &str) -> Result<Self> {
        if name.trim().is_empty() {
            return Err(Error::Unknown(anyhow!("member name is empty")));
        }
        Ok(Self {
            name: name.to_owned(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
    Matrix(MatrixRoom),
    Irc(IrcChannel),
}

impl Channel {
    pub fn network(&self) -> Network {
        match self {
            Self::Matrix(_) => Network::Matrix,
            Self::Irc(channel) => Network::Irc(channel.network.clone()),
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Matrix(room) => write!(f, "{}/{}", self.network(), room.id),
            Self::Irc(channel) => write!(f, "{}/{}", self.network(), channel.name),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Network {
    Matrix,
    Irc(IrcNetworkName),
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Matrix => f.write_str("matrix"),
            Self::Irc(name) => write!(f, "irc:{name}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatrixRoom {
    id: String,
}

impl MatrixRoom {
    pub fn new(id: &str) -> Result<Self> {
        if !id.starts_with('!') {
            return Err(Error::Unknown(anyhow!(
                "matrix room id `{id}` must start with `!`"
            )));
        }
        if id.len() > 255 {
            return Err(Error::Unknown(anyhow!(
                "matrix room id `{id}` is longer than 255 bytes"
            )));
        }
        Ok(Self { id: id.to_owned() })
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IrcChannel {
    network: IrcNetworkName,
    name: IrcChannelName,
}

impl IrcChannel {
    pub fn new(network: IrcNetworkName, name: IrcChannelName) -> Self {
        Self { network, name }
    }

    pub fn network(&self) -> &IrcNetworkName {
        &self.network
    }

    pub fn name(&self) -> &IrcChannelName {
        &self.name
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IrcNetworkName {
    name: String,
}

impl IrcNetworkName {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl fmt::Display for IrcNetworkName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IrcChannelName {
    name: String,
}

impl IrcChannelName {
    pub fn new(name: &str) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::Unknown(anyhow!("irc channel name is empty")));
        }
        if !name.starts_with(['#', '&']) {
            return Err(Error::Unknown(anyhow!(
                "irc channel name `{name}` must start with `#` or `&`"
            )));
        }
        Ok(Self {
            name: name.to_lowercase(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for IrcChannelName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matrix(room: &str) -> Channel {
        Channel::Matrix(MatrixRoom::new(room).unwrap())
    }

    fn irc_channel(name: &str) -> IrcChannel {
        IrcChannel::new(
            IrcNetworkName::new("libera"),
            IrcChannelName::new(name).unwrap(),
        )
    }

    fn irc(channel: &str) -> Channel {
        Channel::Irc(irc_channel(channel))
    }

    fn bridge(name: &str, room: &str, channel: &str) -> Bridge {
        Bridge::new(
            BridgeName::new(name).unwrap(),
            MatrixRoom::new(room).unwrap(),
            irc_channel(channel),
        )
    }

    fn message(kind: MessageKind) -> Message {
        message_with_body(kind, "hello")
    }

    fn message_with_body(kind: MessageKind, body: &str) -> Message {
        Message::new(
            irc("#channel"),
            None,
            "alice".into(),
            kind,
            MessageBody::new(body).unwrap(),
        )
    }

    fn members(names: &[&str]) -> Members {
        Members::new(
            names
                .iter()
                .map(|name| MemberName::new(name).unwrap())
                .collect(),
        )
    }

    #[test]
    fn relays_to_the_other_end_of_the_bridge() {
        let bridges = Bridges::new(vec![bridge("a", "!a", "#a"), bridge("b", "!b", "#b")]).unwrap();

        assert_eq!(bridges.destination(&irc("#a")), Some(matrix("!a")));
        assert_eq!(bridges.destination(&matrix("!b")), Some(irc("#b")));
        assert_eq!(bridges.destination(&irc("#unbridged")), None);
    }

    #[test]
    fn rejects_channel_used_by_two_bridges() {
        let result = Bridges::new(vec![
            bridge("a", "!a", "#shared"),
            bridge("b", "!b", "#shared"),
        ]);
        assert!(matches!(result, Err(Error::ChannelInTwoBridges { .. })));
    }

    #[test]
    fn rejects_duplicate_bridge_names() {
        let result = Bridges::new(vec![bridge("a", "!a", "#a"), bridge("a", "!b", "#b")]);
        assert!(matches!(result, Err(Error::DuplicateBridgeName { .. })));
    }

    #[test]
    fn validates_bridge_names() {
        let cases = [("bridge", true), ("", false)];
        for (name, valid) in cases {
            assert_eq!(BridgeName::new(name).is_ok(), valid, "{name:?}");
        }
    }

    #[test]
    fn backs_off_exponentially_and_gives_up() {
        let policy = RetryPolicy::new(5, Duration::from_millis(100), Duration::from_millis(500));
        assert_eq!(policy.delay_after(1), Some(Duration::from_millis(100)));
        assert_eq!(policy.delay_after(2), Some(Duration::from_millis(200)));
        assert_eq!(policy.delay_after(3), Some(Duration::from_millis(400)));
        assert_eq!(policy.delay_after(4), Some(Duration::from_millis(500)));
        assert_eq!(policy.delay_after(5), None);
    }

    #[test]
    fn prepends_sender_nick() {
        assert_eq!(message(MessageKind::Normal).relay_text(), "<alice> hello");
        assert_eq!(message(MessageKind::Action).relay_text(), "* alice hello");
    }

    #[test]
    fn recognizes_bot_commands() {
        let who = Some(BotCommand::WhoIsOnTheOtherSide);
        let cases = [
            (MessageKind::Normal, ":whoisontheotherside", who),
            (MessageKind::Normal, " :bridge ", who),
            (MessageKind::Normal, ":bridge please", None),
            (MessageKind::Normal, "bridge", None),
            (MessageKind::Action, ":bridge", None),
        ];
        for (kind, body, command) in cases {
            assert_eq!(message_with_body(kind, body).command(), command, "{body:?}");
        }
    }

    #[test]
    fn lists_members_alphabetically() {
        assert_eq!(
            members(&["carol", "Bob", "alice"]).reply(),
            "on the other side (3): alice, Bob, carol"
        );
        assert_eq!(members(&[]).reply(), "nobody is on the other side");
    }

    #[test]
    fn member_name_must_not_be_blank() {
        let cases = [("alice", true), ("", false), (" ", false)];
        for (name, valid) in cases {
            assert_eq!(MemberName::new(name).is_ok(), valid, "{name:?}");
        }
    }

    #[test]
    fn matrix_event_id_must_start_with_a_dollar_and_fit_255_bytes() {
        let too_long = format!("${}", "a".repeat(255));
        let cases = [
            ("$event", true),
            ("", false),
            ("event", false),
            (too_long.as_str(), false),
        ];
        for (id, valid) in cases {
            assert_eq!(MatrixEventId::new(id).is_ok(), valid, "{id:?}");
        }
    }

    #[test]
    fn message_body_must_not_be_blank() {
        let cases = [("hello", true), ("", false), (" \n", false)];
        for (text, valid) in cases {
            assert_eq!(MessageBody::new(text).is_ok(), valid, "{text:?}");
        }
    }

    #[test]
    fn matrix_room_id_must_start_with_a_bang_and_fit_255_bytes() {
        let too_long = format!("!{}", "a".repeat(255));
        let cases = [
            ("!room:example.org", true),
            ("!r0oJFfidHsLY6yUNkH6HePF-FTUo6a2MfuLuOvRtra8", true),
            ("", false),
            ("#room:example.org", false),
            (too_long.as_str(), false),
        ];
        for (id, valid) in cases {
            assert_eq!(MatrixRoom::new(id).is_ok(), valid, "{id:?}");
        }
    }

    #[test]
    fn irc_channel_name_must_be_non_empty_and_start_with_a_channel_prefix() {
        let cases = [
            ("#channel", true),
            ("&channel", true),
            ("", false),
            ("channel", false),
        ];
        for (name, valid) in cases {
            assert_eq!(IrcChannelName::new(name).is_ok(), valid, "{name:?}");
        }
    }

    #[test]
    fn irc_channel_name_ignores_case() {
        assert_eq!(
            IrcChannelName::new("#Channel").unwrap(),
            IrcChannelName::new("#channel").unwrap()
        );
    }
}
