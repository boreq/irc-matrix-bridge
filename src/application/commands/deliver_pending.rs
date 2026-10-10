use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};

use crate::application::ports::{Clock, MessageRepository, MessageSender};
use crate::domain::delivery::RetryPolicy;
use crate::domain::endpoint::{Endpoint, NetworkId};

pub struct DeliverPending {
    pub network: NetworkId,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeliverPendingResult {
    pub delivered: usize,
    pub next_attempt_at: Option<i64>,
}

pub struct DeliverPendingHandler {
    repository: Arc<dyn MessageRepository>,
    senders: HashMap<NetworkId, Arc<dyn MessageSender>>,
    clock: Arc<dyn Clock>,
    retry: RetryPolicy,
    batch_size: usize,
    send_timeout: Duration,
}

impl DeliverPendingHandler {
    pub fn new(
        repository: Arc<dyn MessageRepository>,
        senders: HashMap<NetworkId, Arc<dyn MessageSender>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            repository,
            senders,
            clock,
            retry,
            batch_size: 50,
            send_timeout: Duration::from_secs(30),
        }
    }

    pub async fn handle(&self, command: DeliverPending) -> anyhow::Result<DeliverPendingResult> {
        let sender = self
            .senders
            .get(&command.network)
            .with_context(|| format!("no sender for network {}", command.network))?;

        let mut result = DeliverPendingResult::default();
        for channel in self.repository.pending_channels(&command.network).await? {
            let destination = Endpoint::new(command.network.clone(), channel);
            self.deliver_to(&destination, sender.as_ref(), &mut result)
                .await?;
        }
        Ok(result)
    }

    async fn deliver_to(
        &self,
        destination: &Endpoint,
        sender: &dyn MessageSender,
        result: &mut DeliverPendingResult,
    ) -> anyhow::Result<()> {
        loop {
            let batch = self
                .repository
                .pending_deliveries(destination, self.batch_size)
                .await?;

            for delivery in &batch {
                let now = self.clock.now_ms();
                if delivery.next_attempt_at > now {
                    result.wait_until(delivery.next_attempt_at);
                    return Ok(());
                }

                let sent = tokio::time::timeout(self.send_timeout, sender.send(delivery))
                    .await
                    .unwrap_or_else(|_| Err(anyhow!("timed out")));

                match sent {
                    Ok(()) => {
                        self.repository.mark_delivered(delivery.id, now).await?;
                        result.delivered += 1;
                    }
                    Err(error) => {
                        let attempts = delivery.attempts + 1;
                        let error = format!("{error:#}");
                        match self.retry.delay_after(attempts) {
                            Some(delay) => {
                                tracing::warn!(
                                    %destination, attempts, %error,
                                    "delivery failed, will retry"
                                );
                                let at = now + delay;
                                self.repository
                                    .schedule_retry(delivery.id, attempts, at, &error)
                                    .await?;
                                result.wait_until(at);
                                return Ok(());
                            }
                            None => {
                                tracing::error!(
                                    %destination, attempts, %error,
                                    "delivery failed, giving up"
                                );
                                self.repository
                                    .mark_failed(delivery.id, attempts, &error)
                                    .await?;
                            }
                        }
                    }
                }
            }

            if batch.len() < self.batch_size {
                return Ok(());
            }
        }
    }
}

impl DeliverPendingResult {
    fn wait_until(&mut self, at: i64) {
        self.next_attempt_at = Some(self.next_attempt_at.map_or(at, |current| current.min(at)));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::ports::StoreOutcome;
    use crate::domain::delivery::{Delivery, DeliveryId};
    use crate::domain::message::{Message, MessageKind};

    #[derive(Default)]
    struct FakeRepository {
        pending: Mutex<Vec<Delivery>>,
        delivered: Mutex<Vec<DeliveryId>>,
        failed: Mutex<Vec<DeliveryId>>,
    }

    #[async_trait]
    impl MessageRepository for FakeRepository {
        async fn store(&self, _: &Message, _: &[Endpoint], _: i64) -> anyhow::Result<StoreOutcome> {
            unimplemented!()
        }

        async fn pending_channels(&self, network: &NetworkId) -> anyhow::Result<Vec<String>> {
            let mut channels: Vec<_> = self
                .pending
                .lock()
                .unwrap()
                .iter()
                .filter(|d| &d.destination.network == network)
                .map(|d| d.destination.channel.clone())
                .collect();
            channels.dedup();
            Ok(channels)
        }

        async fn pending_deliveries(
            &self,
            destination: &Endpoint,
            limit: usize,
        ) -> anyhow::Result<Vec<Delivery>> {
            Ok(self
                .pending
                .lock()
                .unwrap()
                .iter()
                .filter(|d| &d.destination == destination)
                .take(limit)
                .cloned()
                .collect())
        }

        async fn mark_delivered(&self, id: DeliveryId, _: i64) -> anyhow::Result<()> {
            self.pending.lock().unwrap().retain(|d| d.id != id);
            self.delivered.lock().unwrap().push(id);
            Ok(())
        }

        async fn schedule_retry(
            &self,
            id: DeliveryId,
            attempts: u32,
            next_attempt_at: i64,
            _: &str,
        ) -> anyhow::Result<()> {
            for d in self.pending.lock().unwrap().iter_mut() {
                if d.id == id {
                    d.attempts = attempts;
                    d.next_attempt_at = next_attempt_at;
                }
            }
            Ok(())
        }

        async fn mark_failed(&self, id: DeliveryId, _: u32, _: &str) -> anyhow::Result<()> {
            self.pending.lock().unwrap().retain(|d| d.id != id);
            self.failed.lock().unwrap().push(id);
            Ok(())
        }
    }

    struct FakeSender {
        broken: Vec<&'static str>,
        sent: Mutex<Vec<DeliveryId>>,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
            if self.broken.contains(&delivery.destination.channel.as_str()) {
                anyhow::bail!("broken");
            }
            self.sent.lock().unwrap().push(delivery.id);
            Ok(())
        }
    }

    struct FixedClock(i64);

    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0
        }
    }

    fn net() -> NetworkId {
        NetworkId::new("irc:test")
    }

    fn delivery(id: i64, channel: &str) -> Delivery {
        Delivery {
            id: DeliveryId(id),
            message: Message {
                source: Endpoint::new(NetworkId::new("matrix"), "!room"),
                external_id: None,
                sender: "alice".into(),
                kind: MessageKind::Text,
                body: format!("message {id}"),
            },
            destination: Endpoint::new(net(), channel),
            attempts: 0,
            next_attempt_at: 0,
        }
    }

    fn handler(
        repository: Arc<FakeRepository>,
        sender: Arc<FakeSender>,
        retry: RetryPolicy,
    ) -> DeliverPendingHandler {
        DeliverPendingHandler::new(
            repository,
            HashMap::from([(net(), sender as Arc<dyn MessageSender>)]),
            Arc::new(FixedClock(1_000)),
            retry,
        )
    }

    #[tokio::test]
    async fn failure_blocks_only_its_own_channel() {
        let repository = Arc::new(FakeRepository::default());
        *repository.pending.lock().unwrap() = vec![
            delivery(1, "#broken"),
            delivery(2, "#ok"),
            delivery(3, "#broken"),
            delivery(4, "#ok"),
        ];
        let sender = Arc::new(FakeSender {
            broken: vec!["#broken"],
            sent: Mutex::default(),
        });

        let result = handler(repository.clone(), sender.clone(), RetryPolicy::default())
            .handle(DeliverPending { network: net() })
            .await
            .unwrap();

        assert_eq!(result.delivered, 2);
        assert_eq!(result.next_attempt_at, Some(2_000));
        assert_eq!(
            *sender.sent.lock().unwrap(),
            vec![DeliveryId(2), DeliveryId(4)]
        );
        let pending = repository.pending.lock().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].attempts, 1);
        assert_eq!(
            pending[1].attempts, 0,
            "later message must not jump the queue"
        );
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts_and_moves_on() {
        let repository = Arc::new(FakeRepository::default());
        let mut exhausted = delivery(1, "#broken");
        exhausted.attempts = 2;
        *repository.pending.lock().unwrap() = vec![exhausted];
        let sender = Arc::new(FakeSender {
            broken: vec!["#broken"],
            sent: Mutex::default(),
        });
        let retry = RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        };

        let result = handler(repository.clone(), sender, retry)
            .handle(DeliverPending { network: net() })
            .await
            .unwrap();

        assert_eq!(result.next_attempt_at, None);
        assert_eq!(*repository.failed.lock().unwrap(), vec![DeliveryId(1)]);
    }
}
