use std::path::{Path, PathBuf};

use crate::domain::bridges::Bridges;
use crate::domain::channel::IrcNetworkName;

#[derive(Clone, Debug)]
pub struct Config {
    database: PathBuf,
    matrix: MatrixConfig,
    irc_networks: Vec<IrcNetworkConfig>,
    bridges: Bridges,
}

impl Config {
    pub fn new(
        database: PathBuf,
        matrix: MatrixConfig,
        irc_networks: Vec<IrcNetworkConfig>,
        bridges: Bridges,
    ) -> anyhow::Result<Self> {
        for bridge in bridges.iter() {
            let network = bridge.irc().network();
            anyhow::ensure!(
                irc_networks.iter().any(|irc| irc.name() == network),
                "bridge `{}` uses unknown irc network `{network}`",
                bridge.name()
            );
        }
        Ok(Self {
            database,
            matrix,
            irc_networks,
            bridges,
        })
    }

    pub fn database(&self) -> &Path {
        &self.database
    }

    pub fn matrix(&self) -> &MatrixConfig {
        &self.matrix
    }

    pub fn irc_networks(&self) -> &[IrcNetworkConfig] {
        &self.irc_networks
    }

    pub fn bridges(&self) -> &Bridges {
        &self.bridges
    }
}

#[derive(Clone, Debug)]
pub struct MatrixConfig {
    homeserver: String,
    data_dir: PathBuf,
    store_passphrase: Option<String>,
    recovery_key: Option<String>,
    device_name: String,
    bootstrap_link: Option<String>,
}

impl MatrixConfig {
    pub fn new(
        homeserver: String,
        data_dir: PathBuf,
        store_passphrase: Option<String>,
        recovery_key: Option<String>,
        device_name: String,
        bootstrap_link: Option<String>,
    ) -> Self {
        Self {
            homeserver,
            data_dir,
            store_passphrase,
            recovery_key,
            device_name,
            bootstrap_link,
        }
    }

    pub fn homeserver(&self) -> &str {
        &self.homeserver
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn store_passphrase(&self) -> Option<&str> {
        self.store_passphrase.as_deref()
    }

    pub fn recovery_key(&self) -> Option<&str> {
        self.recovery_key.as_deref()
    }

    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Address the browser ends up on after SSO login, used once to log in.
    pub fn bootstrap_link(&self) -> Option<&str> {
        self.bootstrap_link.as_deref()
    }
}

#[derive(Clone, Debug)]
pub struct IrcNetworkConfig {
    name: IrcNetworkName,
    server: String,
    port: u16,
    tls: bool,
    nickname: String,
    password: Option<String>,
    nick_password: Option<String>,
}

impl IrcNetworkConfig {
    pub fn new(
        name: IrcNetworkName,
        server: String,
        port: u16,
        tls: bool,
        nickname: String,
        password: Option<String>,
        nick_password: Option<String>,
    ) -> Self {
        Self {
            name,
            server,
            port,
            tls,
            nickname,
            password,
            nick_password,
        }
    }

    pub fn name(&self) -> &IrcNetworkName {
        &self.name
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn tls(&self) -> bool {
        self.tls
    }

    pub fn nickname(&self) -> &str {
        &self.nickname
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn nick_password(&self) -> Option<&str> {
        self.nick_password.as_deref()
    }
}
