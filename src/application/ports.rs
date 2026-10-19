use async_trait::async_trait;

use crate::domain::time::DateTime;
use crate::domain::{Channel, Delivery, Message, MessageId, Network};

#[derive(Debug, PartialEq, Eq)]
pub enum StoreOutcome {
    Stored,
    Duplicate,
}

#[async_trait]
pub trait MessageRepository: Send + Sync {
    async fn store(
        &self,
        id: MessageId,
        message: &Message,
        destination: &Channel,
        now: DateTime,
    ) -> anyhow::Result<StoreOutcome>;

    async fn pending_channels(&self, network: &Network) -> anyhow::Result<Vec<Channel>>;

    async fn next_delivery(&self, destination: &Channel) -> anyhow::Result<Option<Delivery>>;

    async fn mark_delivered(&self, id: MessageId, now: DateTime) -> anyhow::Result<()>;

    async fn schedule_retry(
        &self,
        id: MessageId,
        attempts: u32,
        next_attempt_at: DateTime,
        error: &str,
    ) -> anyhow::Result<()>;

    async fn mark_failed(&self, id: MessageId, attempts: u32, error: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MessageSender: Send + Sync {
    async fn send(&self, delivery: &Delivery) -> anyhow::Result<()>;
}

pub trait DeliveryWaker: Send + Sync {
    fn wake(&self, network: &Network);
}

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime;
}
