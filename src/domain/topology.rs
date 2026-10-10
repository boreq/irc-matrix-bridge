use std::collections::HashSet;

use crate::domain::endpoint::{Endpoint, NetworkId};

#[derive(Clone, Debug)]
pub struct Bridge {
    pub name: String,
    pub endpoints: Vec<Endpoint>,
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("bridge `{0}` needs at least two endpoints")]
    TooFewEndpoints(String),
    #[error("bridge `{bridge}` lists endpoint {endpoint} more than once")]
    DuplicateEndpoint { bridge: String, endpoint: Endpoint },
}

#[derive(Clone, Debug, Default)]
pub struct Topology {
    bridges: Vec<Bridge>,
}

impl Topology {
    pub fn new(bridges: Vec<Bridge>) -> Result<Self, TopologyError> {
        for bridge in &bridges {
            if bridge.endpoints.len() < 2 {
                return Err(TopologyError::TooFewEndpoints(bridge.name.clone()));
            }
            let mut seen = HashSet::new();
            for endpoint in &bridge.endpoints {
                if !seen.insert(endpoint) {
                    return Err(TopologyError::DuplicateEndpoint {
                        bridge: bridge.name.clone(),
                        endpoint: endpoint.clone(),
                    });
                }
            }
        }
        Ok(Self { bridges })
    }

    pub fn destinations(&self, source: &Endpoint) -> Vec<Endpoint> {
        let mut seen = HashSet::new();
        self.bridges
            .iter()
            .filter(|bridge| bridge.endpoints.contains(source))
            .flat_map(|bridge| bridge.endpoints.iter())
            .filter(|endpoint| *endpoint != source && seen.insert(*endpoint))
            .cloned()
            .collect()
    }

    pub fn channels_on(&self, network: &NetworkId) -> Vec<String> {
        let mut seen = HashSet::new();
        self.bridges
            .iter()
            .flat_map(|bridge| bridge.endpoints.iter())
            .filter(|endpoint| &endpoint.network == network && seen.insert(*endpoint))
            .map(|endpoint| endpoint.channel.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn irc(channel: &str) -> Endpoint {
        Endpoint::new(NetworkId::new("irc:libera"), channel)
    }

    fn matrix(room: &str) -> Endpoint {
        Endpoint::new(NetworkId::new("matrix"), room)
    }

    #[test]
    fn relays_to_every_other_endpoint_in_shared_bridges() {
        let topology = Topology::new(vec![
            Bridge {
                name: "a".into(),
                endpoints: vec![irc("#a"), matrix("!a"), matrix("!shared")],
            },
            Bridge {
                name: "b".into(),
                endpoints: vec![irc("#b"), matrix("!shared")],
            },
        ])
        .unwrap();

        assert_eq!(
            topology.destinations(&irc("#a")),
            vec![matrix("!a"), matrix("!shared")]
        );
        assert_eq!(
            topology.destinations(&matrix("!shared")),
            vec![irc("#a"), matrix("!a"), irc("#b")]
        );
        assert!(topology.destinations(&irc("#unbridged")).is_empty());
    }

    #[test]
    fn rejects_single_endpoint_bridge() {
        let result = Topology::new(vec![Bridge {
            name: "lonely".into(),
            endpoints: vec![irc("#a")],
        }]);
        assert!(matches!(result, Err(TopologyError::TooFewEndpoints(_))));
    }
}
