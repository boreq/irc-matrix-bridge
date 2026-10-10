use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use matrix_sdk::ruma::OwnedRoomId;
use serde::Deserialize;

use crate::adapters::irc::normalize_channel;
use crate::adapters::matrix::MatrixSettings;
use crate::domain::endpoint::{Endpoint, NetworkId};
use crate::domain::topology::{Bridge, Topology};
use crate::entrypoints::irc::IrcSettings;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub database: PathBuf,
    pub matrix: MatrixSettings,
    #[serde(default)]
    pub irc: BTreeMap<String, IrcSettings>,
    #[serde(default, rename = "bridge")]
    pub bridges: Vec<BridgeConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    pub name: String,
    #[serde(default)]
    pub matrix: Vec<OwnedRoomId>,
    #[serde(default)]
    pub irc: Vec<IrcChannelConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IrcChannelConfig {
    pub network: String,
    pub channel: String,
}

pub fn matrix_network() -> NetworkId {
    NetworkId::new("matrix")
}

pub fn irc_network(name: &str) -> NetworkId {
    NetworkId::new(format!("irc:{name}"))
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn topology(&self) -> anyhow::Result<Topology> {
        let mut bridges = Vec::new();
        for bridge in &self.bridges {
            let mut endpoints: Vec<Endpoint> = bridge
                .matrix
                .iter()
                .map(|room| Endpoint::new(matrix_network(), room.as_str()))
                .collect();
            for irc in &bridge.irc {
                anyhow::ensure!(
                    self.irc.contains_key(&irc.network),
                    "bridge `{}` uses unknown irc network `{}`",
                    bridge.name,
                    irc.network
                );
                endpoints.push(Endpoint::new(
                    irc_network(&irc.network),
                    normalize_channel(&irc.channel),
                ));
            }
            bridges.push(Bridge {
                name: bridge.name.clone(),
                endpoints,
            });
        }
        Ok(Topology::new(bridges)?)
    }
}
