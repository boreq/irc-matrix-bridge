use async_trait::async_trait;

use crate::domain::delivery::{Delivery, DeliveryId};

use crate::domain::endpoint::{Endpoint, NetworkId};

use crate::domain::message::Message;

#[derive(Debug, PartialEq, Eq)]
pub enum StoreOutcome {
    Stored,
    Duplicate,
}

#[async_trait]
pub trait MessageRepository: Send + Sync {
    async fn store(
        &self,
        message: &Message,
        destinations: &[Endpoint],
        now: i64,
    ) -> anyhow::Result<StoreOutcome>;

    async fn pending_channels(&self, network: &NetworkId) -> anyhow::Result<Vec<String>>;

    async fn pending_deliveries(
        &self,
        destination: &Endpoint,
        limit: usize,
    ) -> anyhow::Result<Vec<Delivery>>;

    async fn mark_delivered(&self, id: DeliveryId, now: i64) -> anyhow::Result<()>;

    async fn schedule_retry(
        &self,
        id: DeliveryId,
        attempts: u32,
        next_attempt_at: i64,
        error: &str,
    ) -> anyhow::Result<()>;

    async fn mark_failed(&self, id: DeliveryId, attempts: u32, error: &str) -> anyhow::Result<()>;
}

#[async_trait]
pub trait MessageSender: Send + Sync {
    async fn send(&self, delivery: &Delivery) -> anyhow::Result<()>;
}

pub trait DeliveryWaker: Send + Sync {
    fn wake(&self, network: &NetworkId);
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}
