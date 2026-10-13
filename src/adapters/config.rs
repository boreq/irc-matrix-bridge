use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Context;
use matrix_sdk::ruma::OwnedRoomId;
use serde::Deserialize;

use crate::config::{Config, IrcNetworkConfig, MatrixConfig};
use crate::domain::bridges::{Bridge, Bridges};
use crate::domain::channel::{IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom};

pub struct TomlConfigLoader {
    path: PathBuf,
}

impl TomlConfigLoader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> anyhow::Result<Config> {
        let text = std::fs::read_to_string(&self.path)
            .with_context(|| format!("reading {}", self.path.display()))?;
        parse(&text).with_context(|| format!("loading {}", self.path.display()))
    }
}

fn parse(text: &str) -> anyhow::Result<Config> {
    let config: TomlConfig = toml::from_str(text)?;
    config.into_config()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlConfig {
    database: PathBuf,
    matrix: TomlMatrix,
    #[serde(default)]
    irc: BTreeMap<String, TomlIrcNetwork>,
    #[serde(default, rename = "bridge")]
    bridges: Vec<TomlBridge>,
}

impl TomlConfig {
    fn into_config(self) -> anyhow::Result<Config> {
        let irc_networks = self
            .irc
            .into_iter()
            .map(|(name, irc)| irc.into_config(name))
            .collect();
        let bridges = self
            .bridges
            .into_iter()
            .map(TomlBridge::into_bridge)
            .collect::<anyhow::Result<Vec<_>>>()?;
        Config::new(
            self.database,
            self.matrix.into_config(),
            irc_networks,
            Bridges::new(bridges)?,
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlMatrix {
    homeserver: String,
    user: String,
    password: String,
    data_dir: PathBuf,
    store_passphrase: Option<String>,
    recovery_key: Option<String>,
    #[serde(default = "default_device_name")]
    device_name: String,
}

impl TomlMatrix {
    fn into_config(self) -> MatrixConfig {
        MatrixConfig::new(
            self.homeserver,
            self.user,
            self.password,
            self.data_dir,
            self.store_passphrase,
            self.recovery_key,
            self.device_name,
        )
    }
}

fn default_device_name() -> String {
    "irc-bridge".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlIrcNetwork {
    server: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_tls")]
    tls: bool,
    nickname: String,
    password: Option<String>,
    nick_password: Option<String>,
}

impl TomlIrcNetwork {
    fn into_config(self, name: String) -> IrcNetworkConfig {
        IrcNetworkConfig::new(
            IrcNetworkName::new(name),
            self.server,
            self.port,
            self.tls,
            self.nickname,
            self.password,
            self.nick_password,
        )
    }
}

fn default_port() -> u16 {
    6697
}

fn default_tls() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlBridge {
    name: String,
    matrix: OwnedRoomId,
    irc: TomlIrcChannel,
}

impl TomlBridge {
    fn into_bridge(self) -> anyhow::Result<Bridge> {
        let channel = IrcChannelName::new(&self.irc.channel)
            .with_context(|| format!("bridge `{}`", self.name))?;
        Ok(Bridge::new(
            self.name,
            MatrixRoom::new(self.matrix.as_str()),
            IrcChannel::new(IrcNetworkName::new(self.irc.network), channel),
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlIrcChannel {
    network: String,
    channel: String,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const CONFIG: &str = r##"
database = "bridge.db"

[matrix]
homeserver = "https://matrix.example.org"
user = "@bridge:example.org"
password = "secret"
data_dir = "matrix"

[irc.network]
server = "irc.example.org"
nickname = "bridge"

[[bridge]]
name = "bridge"
matrix = "!room:example.org"
irc = { network = "network", channel = "#channel" }
"##;

    #[test]
    fn loads_example_config() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
        TomlConfigLoader::new(path).load().unwrap();
    }

    #[test]
    fn applies_defaults() {
        let config = parse(CONFIG).unwrap();

        assert_eq!(config.matrix().device_name(), "irc-bridge");
        let irc = &config.irc_networks()[0];
        assert_eq!(irc.name(), &IrcNetworkName::new("network"));
        assert_eq!(irc.port(), 6697);
        assert!(irc.tls());
        assert_eq!(config.bridges().iter().count(), 1);
    }

    #[test]
    fn rejects_invalid_configs() {
        let cases = [
            (
                "unknown field",
                CONFIG.replace("nickname =", "nick_pasword = \"typo\"\nnickname ="),
            ),
            (
                "unknown network",
                CONFIG.replace("network = \"network\"", "network = \"other\""),
            ),
            (
                "invalid channel name",
                CONFIG.replace("\"#channel\"", "\"channel\""),
            ),
        ];
        for (name, text) in cases {
            assert!(parse(&text).is_err(), "{name}");
        }
    }
}
