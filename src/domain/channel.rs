use std::fmt;

use anyhow::anyhow;

use crate::errors::{Error, Result};

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

#[cfg(test)]
mod tests {
    use super::*;

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
