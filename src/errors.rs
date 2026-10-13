use thiserror::Error;

use crate::domain::channel::Channel;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{channel} is used by both bridge `{first}` and bridge `{second}`")]
    ChannelInTwoBridges {
        channel: Channel,
        first: String,
        second: String,
    },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
