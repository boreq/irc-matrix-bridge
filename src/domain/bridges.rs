use std::collections::{HashMap, HashSet};
use std::fmt;

use anyhow::anyhow;

use crate::domain::channel::{Channel, IrcChannel, IrcNetworkName, MatrixRoom};
use crate::errors::{Error, Result};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::channel::IrcChannelName;

    fn matrix(room: &str) -> Channel {
        Channel::Matrix(MatrixRoom::new(room))
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
            MatrixRoom::new(room),
            irc_channel(channel),
        )
    }

    #[test]
    fn validates_bridge_names() {
        let cases = [("bridge", true), ("", false)];
        for (name, valid) in cases {
            assert_eq!(BridgeName::new(name).is_ok(), valid, "{name:?}");
        }
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
}
