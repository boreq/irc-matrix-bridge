use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;

use crate::application::ports::MemberLister;
use crate::domain::{Bridges, Channel, Members, Network};

pub struct WhoIsOnTheOtherSide {
    pub channel: Channel,
}

pub struct WhoIsOnTheOtherSideHandler {
    bridges: Arc<Bridges>,
    member_listers: HashMap<Network, Arc<dyn MemberLister>>,
}

impl WhoIsOnTheOtherSideHandler {
    pub fn new(
        bridges: Arc<Bridges>,
        member_listers: HashMap<Network, Arc<dyn MemberLister>>,
    ) -> Self {
        Self {
            bridges,
            member_listers,
        }
    }

    pub async fn handle(&self, query: WhoIsOnTheOtherSide) -> anyhow::Result<Option<Members>> {
        let Some(other_side) = self.bridges.destination(&query.channel) else {
            return Ok(None);
        };
        let network = other_side.network();
        let lister = self
            .member_listers
            .get(&network)
            .with_context(|| format!("no member lister for network {network}"))?;
        Ok(Some(Members::new(lister.members(&other_side).await?)))
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::domain::{
        Bridge, BridgeName, IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom, MemberName,
    };

    struct FakeLister;

    #[async_trait]
    impl MemberLister for FakeLister {
        async fn members(&self, channel: &Channel) -> anyhow::Result<Vec<MemberName>> {
            Ok(vec![MemberName::new(&channel.to_string())?])
        }
    }

    fn irc(name: &str) -> IrcChannel {
        IrcChannel::new(
            IrcNetworkName::new("libera"),
            IrcChannelName::new(name).unwrap(),
        )
    }

    fn handler() -> WhoIsOnTheOtherSideHandler {
        let bridge = Bridge::new(
            BridgeName::new("bridge").unwrap(),
            MatrixRoom::new("!room").unwrap(),
            irc("#channel"),
        );
        let listers: HashMap<Network, Arc<dyn MemberLister>> = HashMap::from([
            (
                Network::Matrix,
                Arc::new(FakeLister) as Arc<dyn MemberLister>,
            ),
            (
                Network::Irc(IrcNetworkName::new("libera")),
                Arc::new(FakeLister),
            ),
        ]);
        WhoIsOnTheOtherSideHandler::new(Arc::new(Bridges::new(vec![bridge]).unwrap()), listers)
    }

    #[tokio::test]
    async fn lists_members_of_the_other_end_of_the_bridge() {
        let handler = handler();
        let cases = [
            (
                Channel::Irc(irc("#channel")),
                Some("on the other side (1): matrix/!room"),
            ),
            (
                Channel::Matrix(MatrixRoom::new("!room").unwrap()),
                Some("on the other side (1): irc:libera/#channel"),
            ),
            (Channel::Irc(irc("#unbridged")), None),
        ];
        for (channel, reply) in cases {
            let members = handler
                .handle(WhoIsOnTheOtherSide {
                    channel: channel.clone(),
                })
                .await
                .unwrap();
            assert_eq!(
                members.as_ref().map(Members::reply).as_deref(),
                reply,
                "{channel}"
            );
        }
    }
}
