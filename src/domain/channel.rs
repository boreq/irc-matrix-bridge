use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
    Matrix(MatrixRoom),
    Irc(IrcChannel),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatrixRoom {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IrcChannel {
    network: String,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Network {
    Matrix,
    Irc(String),
}

impl Channel {
    pub fn network(&self) -> Network {
        match self {
            Self::Matrix(_) => Network::Matrix,
            Self::Irc(channel) => Network::Irc(channel.network.clone()),
        }
    }
}

impl MatrixRoom {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

impl IrcChannel {
    pub fn new(network: impl Into<String>, name: &str) -> Self {
        Self {
            network: network.into(),
            name: Self::normalize_name(name),
        }
    }

    pub fn normalize_name(name: &str) -> String {
        name.to_lowercase()
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    pub fn name(&self) -> &str {
        &self.name
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

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Matrix => f.write_str("matrix"),
            Self::Irc(name) => write!(f, "irc:{name}"),
        }
    }
}
