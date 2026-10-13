use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context;
use matrix_sdk::ruma::OwnedRoomId;
use serde::Deserialize;

use crate::adapters::matrix::MatrixSettings;
use crate::domain::bridges::{Bridge, Bridges};
use crate::domain::channel::{IrcChannel, MatrixRoom};
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
    pub matrix: OwnedRoomId,
    pub irc: IrcChannelConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IrcChannelConfig {
    pub network: String,
    pub channel: String,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn to_bridges(&self) -> anyhow::Result<Bridges> {
        let mut bridges = Vec::new();
        for bridge in &self.bridges {
            anyhow::ensure!(
                self.irc.contains_key(&bridge.irc.network),
                "bridge `{}` uses unknown irc network `{}`",
                bridge.name,
                bridge.irc.network
            );
            bridges.push(Bridge {
                name: bridge.name.clone(),
                matrix: MatrixRoom::new(bridge.matrix.as_str()),
                irc: IrcChannel::new(&bridge.irc.network, &bridge.irc.channel),
            });
        }
        Ok(Bridges::new(bridges)?)
    }
}
