use std::sync::Arc;

use crate::application::ports::{Clock, DeliveryWaker, MessageRepository, StoreOutcome};
use crate::domain::bridges::Bridges;
use crate::domain::message::{Message, MessageId};

pub struct StoreReceivedMessage {
    pub message: Message,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StoreReceivedMessageOutcome {
    Stored,
    NotBridged,
    Duplicate,
}

pub struct StoreReceivedMessageHandler {
    bridges: Arc<Bridges>,
    repository: Arc<dyn MessageRepository>,
    waker: Arc<dyn DeliveryWaker>,
    clock: Arc<dyn Clock>,
}

impl StoreReceivedMessageHandler {
    pub fn new(
        bridges: Arc<Bridges>,
        repository: Arc<dyn MessageRepository>,
        waker: Arc<dyn DeliveryWaker>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            bridges,
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
        let Some(destination) = self.bridges.destination(&message.source) else {
            return Ok(StoreReceivedMessageOutcome::NotBridged);
        };

        let outcome = self
            .repository
            .store(
                MessageId::generate(),
                &message,
                &destination,
                self.clock.now(),
            )
            .await?;
        if outcome == StoreOutcome::Duplicate {
            return Ok(StoreReceivedMessageOutcome::Duplicate);
        }

        self.waker.wake(&destination.network());
        Ok(StoreReceivedMessageOutcome::Stored)
    }
}
