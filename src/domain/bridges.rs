use std::collections::HashMap;

use crate::domain::channel::{Channel, IrcChannel, MatrixRoom};

#[derive(Clone, Debug)]
pub struct Bridge {
    pub name: String,
    pub matrix: MatrixRoom,
    pub irc: IrcChannel,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgesError {
    #[error("{channel} is used by both bridge `{first}` and bridge `{second}`")]
    ChannelInTwoBridges {
        channel: Channel,
        first: String,
        second: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct Bridges {
    bridges: Vec<Bridge>,
}

impl Bridges {
    pub fn new(bridges: Vec<Bridge>) -> Result<Self, BridgesError> {
        let mut owners: HashMap<Channel, &str> = HashMap::new();
        for bridge in &bridges {
            let channels = [
                Channel::Matrix(bridge.matrix.clone()),
                Channel::Irc(bridge.irc.clone()),
            ];
            for channel in channels {
                if let Some(first) = owners.insert(channel.clone(), &bridge.name) {
                    return Err(BridgesError::ChannelInTwoBridges {
                        channel,
                        first: first.to_owned(),
                        second: bridge.name.clone(),
                    });
                }
            }
        }
        Ok(Self { bridges })
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

    pub fn irc_channels(&self, network: &str) -> impl Iterator<Item = &IrcChannel> {
        self.bridges
            .iter()
            .map(|bridge| &bridge.irc)
            .filter(move |channel| channel.network() == network)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matrix(room: &str) -> Channel {
        Channel::Matrix(MatrixRoom::new(room))
    }

    fn irc(channel: &str) -> Channel {
        Channel::Irc(IrcChannel::new("libera", channel))
    }

    fn bridge(name: &str, room: &str, channel: &str) -> Bridge {
        Bridge {
            name: name.into(),
            matrix: MatrixRoom::new(room),
            irc: IrcChannel::new("libera", channel),
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
        assert!(matches!(
            result,
            Err(BridgesError::ChannelInTwoBridges { .. })
        ));
    }
}
