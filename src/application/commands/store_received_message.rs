use std::collections::HashSet;
use std::sync::Arc;

use crate::application::ports::{Clock, DeliveryWaker, MessageRepository, StoreOutcome};
use crate::domain::message::Message;
use crate::domain::topology::Topology;

pub struct StoreReceivedMessage {
    pub message: Message,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StoreReceivedMessageOutcome {
    Stored { destinations: usize },
    NotBridged,
    Duplicate,
}

pub struct StoreReceivedMessageHandler {
    topology: Arc<Topology>,
    repository: Arc<dyn MessageRepository>,
    waker: Arc<dyn DeliveryWaker>,
    clock: Arc<dyn Clock>,
}

impl StoreReceivedMessageHandler {
    pub fn new(
        topology: Arc<Topology>,
        repository: Arc<dyn MessageRepository>,
        waker: Arc<dyn DeliveryWaker>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            topology,
            repository,
            waker,
            clock,
        }
    }

    pub async fn handle(
        &self,
        command: StoreReceivedMessage,
    ) -> anyhow::Result<StoreReceivedMessageOutcome> {
        let message = command.message;
        let destinations = self.topology.destinations(&message.source);
        if destinations.is_empty() {
            return Ok(StoreReceivedMessageOutcome::NotBridged);
        }

        let outcome = self
            .repository
            .store(&message, &destinations, self.clock.now_ms())
            .await?;
        if outcome == StoreOutcome::Duplicate {
            return Ok(StoreReceivedMessageOutcome::Duplicate);
        }

        let mut woken = HashSet::new();
        for destination in &destinations {
            if woken.insert(&destination.network) {
                self.waker.wake(&destination.network);
            }
        }
        Ok(StoreReceivedMessageOutcome::Stored {
            destinations: destinations.len(),
        })
    }
}
