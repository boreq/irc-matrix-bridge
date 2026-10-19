use std::sync::Arc;

use crate::application::ports::MessageRepository;
use crate::domain::{Channel, Network};

pub struct PendingDestinations {
    pub network: Network,
}

pub struct PendingDestinationsHandler {
    repository: Arc<dyn MessageRepository>,
}

impl PendingDestinationsHandler {
    pub fn new(repository: Arc<dyn MessageRepository>) -> Self {
        Self { repository }
    }

    pub async fn handle(&self, query: PendingDestinations) -> anyhow::Result<Vec<Channel>> {
        self.repository.pending_channels(&query.network).await
    }
}
