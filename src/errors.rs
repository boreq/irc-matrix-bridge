use thiserror::Error;

use crate::domain::bridges::BridgeName;
use crate::domain::channel::Channel;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{channel} is used by both bridge `{first}` and bridge `{second}`")]
    ChannelInTwoBridges {
        channel: Channel,
        first: BridgeName,
        second: BridgeName,
    },

    #[error("bridge name `{name}` is used twice")]
    DuplicateBridgeName { name: BridgeName },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
