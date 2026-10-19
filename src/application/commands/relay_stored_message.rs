use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};

use crate::application::ports::{Clock, MessageRepository, MessageSender};
use crate::domain::time::DateTime;
use crate::domain::{Channel, Network, RetryPolicy};

pub struct RelayStoredMessage {
    pub destination: Channel,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RelayStoredMessageOutcome {
    Delivered,
    RetryScheduled { at: DateTime },
    GaveUp,
    NotDue { at: DateTime },
    NothingPending,
}

pub struct RelayStoredMessageHandler {
    repository: Arc<dyn MessageRepository>,
    senders: HashMap<Network, Arc<dyn MessageSender>>,
    clock: Arc<dyn Clock>,
    retry: RetryPolicy,
    send_timeout: Duration,
}

impl RelayStoredMessageHandler {
    pub fn new(
        repository: Arc<dyn MessageRepository>,
        senders: HashMap<Network, Arc<dyn MessageSender>>,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            repository,
            senders,
            clock,
            retry,
            send_timeout: Duration::from_secs(30),
        }
    }

    pub async fn handle(
        &self,
        command: RelayStoredMessage,
    ) -> anyhow::Result<RelayStoredMessageOutcome> {
        let destination = command.destination;
        let network = destination.network();
        let sender = self
            .senders
            .get(&network)
            .with_context(|| format!("no sender for network {network}"))?;

        let Some(delivery) = self.repository.next_delivery(&destination).await? else {
            return Ok(RelayStoredMessageOutcome::NothingPending);
        };
        let now = self.clock.now();
        if delivery.next_attempt_at() > now {
            return Ok(RelayStoredMessageOutcome::NotDue {
                at: delivery.next_attempt_at(),
            });
        }

        let sent = tokio::time::timeout(self.send_timeout, sender.send(&delivery))
            .await
            .unwrap_or_else(|_| Err(anyhow!("timed out")));
        let error = match sent {
            Ok(()) => {
                self.repository.mark_delivered(delivery.id(), now).await?;
                return Ok(RelayStoredMessageOutcome::Delivered);
            }
            Err(error) => format!("{error:#}"),
        };

        let attempts = delivery.attempts() + 1;
        match self.retry.delay_after(attempts) {
            Some(delay) => {
                tracing::warn!(%destination, attempts, %error, "delivery failed, will retry");
                let at = now + delay;
                self.repository
                    .schedule_retry(delivery.id(), attempts, at, &error)
                    .await?;
                Ok(RelayStoredMessageOutcome::RetryScheduled { at })
            }
            None => {
                tracing::error!(%destination, attempts, %error, "delivery failed, giving up");
                self.repository
                    .mark_failed(delivery.id(), attempts, &error)
                    .await?;
                Ok(RelayStoredMessageOutcome::GaveUp)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::ports::StoreOutcome;
    use crate::domain::{
        Delivery, IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom, Message, MessageBody,
        MessageId, MessageKind,
    };
    use uuid::Uuid;

    #[derive(Default)]
    struct FakeRepository {
        pending: Mutex<Vec<Delivery>>,
        delivered: Mutex<Vec<MessageId>>,
        failed: Mutex<Vec<MessageId>>,
    }

    #[async_trait]
    impl MessageRepository for FakeRepository {
        async fn store(
            &self,
            _: MessageId,
            _: &Message,
            _: &Channel,
            _: DateTime,
        ) -> anyhow::Result<StoreOutcome> {
            unimplemented!()
        }

        async fn pending_channels(&self, _: &Network) -> anyhow::Result<Vec<Channel>> {
            unimplemented!()
        }

        async fn next_delivery(&self, destination: &Channel) -> anyhow::Result<Option<Delivery>> {
            Ok(self
                .pending
                .lock()
                .unwrap()
                .iter()
                .find(|d| d.destination() == destination)
                .cloned())
        }

        async fn mark_delivered(&self, id: MessageId, _: DateTime) -> anyhow::Result<()> {
            self.pending.lock().unwrap().retain(|d| d.id() != id);
            self.delivered.lock().unwrap().push(id);
            Ok(())
        }

        async fn schedule_retry(
            &self,
            id: MessageId,
            attempts: u32,
            next_attempt_at: DateTime,
            _: &str,
        ) -> anyhow::Result<()> {
            for d in self.pending.lock().unwrap().iter_mut() {
                if d.id() == id {
                    *d = Delivery::new(
                        id,
                        d.message().clone(),
                        d.destination().clone(),
                        attempts,
                        next_attempt_at,
                    );
                }
            }
            Ok(())
        }

        async fn mark_failed(&self, id: MessageId, _: u32, _: &str) -> anyhow::Result<()> {
            self.pending.lock().unwrap().retain(|d| d.id() != id);
            self.failed.lock().unwrap().push(id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeSender {
        broken: bool,
        sent: Mutex<Vec<MessageId>>,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
            if self.broken {
                anyhow::bail!("broken");
            }
            self.sent.lock().unwrap().push(delivery.id());
            Ok(())
        }
    }

    struct FixedClock {
        now: DateTime,
    }

    impl Clock for FixedClock {
        fn now(&self) -> DateTime {
            self.now
        }
    }

    fn destination() -> Channel {
        Channel::Irc(IrcChannel::new(
            IrcNetworkName::new("test"),
            IrcChannelName::new("#chan").unwrap(),
        ))
    }

    fn id(n: u128) -> MessageId {
        MessageId::new(Uuid::from_u128(n))
    }

    fn delivery(n: u128, attempts: u32) -> Delivery {
        Delivery::new(
            id(n),
            Message::new(
                Channel::Matrix(MatrixRoom::new("!room").unwrap()),
                None,
                "alice".into(),
                MessageKind::Normal,
                MessageBody::new(&format!("message {n}")).unwrap(),
            ),
            destination(),
            attempts,
            DateTime::from_unix_millis(0).unwrap(),
        )
    }

    fn handler(
        repository: Arc<FakeRepository>,
        sender: Arc<FakeSender>,
        retry: RetryPolicy,
    ) -> RelayStoredMessageHandler {
        RelayStoredMessageHandler::new(
            repository,
            HashMap::from([(destination().network(), sender as Arc<dyn MessageSender>)]),
            Arc::new(FixedClock {
                now: DateTime::from_unix_millis(1_000).unwrap(),
            }),
            retry,
        )
    }

    fn command() -> RelayStoredMessage {
        RelayStoredMessage {
            destination: destination(),
        }
    }

    #[tokio::test]
    async fn relays_stored_messages_oldest_first() {
        let repository = Arc::new(FakeRepository::default());
        *repository.pending.lock().unwrap() = vec![delivery(1, 0), delivery(2, 0)];
        let sender = Arc::new(FakeSender::default());
        let handler = handler(repository.clone(), sender.clone(), RetryPolicy::default());

        for _ in 0..2 {
            assert_eq!(
                handler.handle(command()).await.unwrap(),
                RelayStoredMessageOutcome::Delivered
            );
        }
        assert_eq!(
            handler.handle(command()).await.unwrap(),
            RelayStoredMessageOutcome::NothingPending
        );
        assert_eq!(*sender.sent.lock().unwrap(), vec![id(1), id(2)]);
        assert_eq!(*repository.delivered.lock().unwrap(), vec![id(1), id(2)]);
    }

    #[tokio::test]
    async fn failed_message_holds_back_later_ones_until_retry_is_due() {
        let repository = Arc::new(FakeRepository::default());
        *repository.pending.lock().unwrap() = vec![delivery(1, 0), delivery(2, 0)];
        let sender = Arc::new(FakeSender {
            broken: true,
            ..FakeSender::default()
        });
        let handler = handler(repository.clone(), sender.clone(), RetryPolicy::default());

        assert_eq!(
            handler.handle(command()).await.unwrap(),
            RelayStoredMessageOutcome::RetryScheduled {
                at: DateTime::from_unix_millis(2_000).unwrap()
            }
        );
        assert_eq!(
            handler.handle(command()).await.unwrap(),
            RelayStoredMessageOutcome::NotDue {
                at: DateTime::from_unix_millis(2_000).unwrap()
            }
        );

        let pending = repository.pending.lock().unwrap();
        assert_eq!(pending[0].attempts(), 1);
        assert_eq!(
            pending[1].attempts(),
            0,
            "later message must not jump the queue"
        );
    }

    #[tokio::test]
    async fn gives_up_after_max_attempts() {
        let repository = Arc::new(FakeRepository::default());
        *repository.pending.lock().unwrap() = vec![delivery(1, 2)];
        let sender = Arc::new(FakeSender {
            broken: true,
            ..FakeSender::default()
        });
        let retry = RetryPolicy::new(3, Duration::from_secs(1), Duration::from_secs(600));
        let handler = handler(repository.clone(), sender, retry);

        assert_eq!(
            handler.handle(command()).await.unwrap(),
            RelayStoredMessageOutcome::GaveUp
        );
        assert_eq!(*repository.failed.lock().unwrap(), vec![id(1)]);
        assert_eq!(
            handler.handle(command()).await.unwrap(),
            RelayStoredMessageOutcome::NothingPending
        );
    }
}
