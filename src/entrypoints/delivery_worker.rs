use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use crate::application::commands::relay_stored_message::{
    RelayStoredMessage, RelayStoredMessageHandler, RelayStoredMessageOutcome,
};
use crate::application::ports::Clock;
use crate::application::queries::pending_destinations::{
    PendingDestinations, PendingDestinationsHandler,
};
use crate::domain::channel::Network;
use crate::domain::time::DateTime;

const MAX_IDLE: Duration = Duration::from_secs(60);

pub async fn run(
    network: Network,
    pending_destinations: Arc<PendingDestinationsHandler>,
    relay_stored_message: Arc<RelayStoredMessageHandler>,
    wakeup: Arc<Notify>,
    clock: Arc<dyn Clock>,
) {
    loop {
        let idle =
            match relay_pending(&network, &pending_destinations, &relay_stored_message).await {
                Ok(next_attempt_at) => next_attempt_at
                    .map_or(MAX_IDLE, |at| clock.now().duration_until(at).min(MAX_IDLE)),
                Err(error) => {
                    tracing::error!(%network, "relaying stored messages failed: {error:#}");
                    Duration::from_secs(5)
                }
            };

        tokio::select! {
            _ = wakeup.notified() => {}
            _ = tokio::time::sleep(idle) => {}
        }
    }
}

async fn relay_pending(
    network: &Network,
    pending_destinations: &PendingDestinationsHandler,
    relay_stored_message: &RelayStoredMessageHandler,
) -> anyhow::Result<Option<DateTime>> {
    let destinations = pending_destinations
        .handle(PendingDestinations {
            network: network.clone(),
        })
        .await?;

    let mut delivered = 0;
    let mut next_attempt_at: Option<DateTime> = None;
    for destination in destinations {
        loop {
            let outcome = relay_stored_message
                .handle(RelayStoredMessage {
                    destination: destination.clone(),
                })
                .await?;
            match outcome {
                RelayStoredMessageOutcome::Delivered => delivered += 1,
                RelayStoredMessageOutcome::GaveUp => {}
                RelayStoredMessageOutcome::NothingPending => break,
                RelayStoredMessageOutcome::RetryScheduled { at }
                | RelayStoredMessageOutcome::NotDue { at } => {
                    next_attempt_at = Some(next_attempt_at.map_or(at, |current| current.min(at)));
                    break;
                }
            }
        }
    }

    if delivered > 0 {
        tracing::debug!(%network, delivered, "delivered");
    }
    Ok(next_attempt_at)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::adapters::sqlite::SqliteMessageRepository;
    use crate::application::ports::{MessageRepository, MessageSender};
    use crate::domain::channel::{Channel, IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom};
    use crate::domain::delivery::{Delivery, RetryPolicy};
    use crate::domain::message::{Message, MessageId, MessageKind};

    struct FakeSender {
        broken: Channel,
        sent: Mutex<Vec<Channel>>,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(&self, delivery: &Delivery) -> anyhow::Result<()> {
            if delivery.destination() == &self.broken {
                anyhow::bail!("broken");
            }
            self.sent
                .lock()
                .unwrap()
                .push(delivery.destination().clone());
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

    #[tokio::test]
    async fn failure_on_one_destination_does_not_block_others() {
        let network = Network::Irc(IrcNetworkName::new("test"));
        let irc = |name| {
            Channel::Irc(IrcChannel::new(
                IrcNetworkName::new("test"),
                IrcChannelName::new(name).unwrap(),
            ))
        };
        let broken = irc("#broken");
        let ok = irc("#ok");
        let repository = Arc::new(SqliteMessageRepository::in_memory().unwrap());
        for _ in 0..2 {
            let message = Message::new(
                Channel::Matrix(MatrixRoom::new("!room")),
                None,
                "alice".into(),
                MessageKind::Normal,
                "hello".into(),
            );
            for destination in [&broken, &ok] {
                repository
                    .store(
                        MessageId::generate(),
                        &message,
                        destination,
                        DateTime::from_unix_millis(0).unwrap(),
                    )
                    .await
                    .unwrap();
            }
        }
        let sender = Arc::new(FakeSender {
            broken: broken.clone(),
            sent: Mutex::default(),
        });
        let pending_destinations = PendingDestinationsHandler::new(repository.clone());
        let relay_stored_message = RelayStoredMessageHandler::new(
            repository,
            HashMap::from([(network.clone(), sender.clone() as Arc<dyn MessageSender>)]),
            Arc::new(FixedClock {
                now: DateTime::from_unix_millis(1_000).unwrap(),
            }),
            RetryPolicy::default(),
        );

        let next_attempt_at = relay_pending(&network, &pending_destinations, &relay_stored_message)
            .await
            .unwrap();

        assert_eq!(*sender.sent.lock().unwrap(), vec![ok.clone(), ok]);
        assert_eq!(
            next_attempt_at,
            Some(DateTime::from_unix_millis(2_000).unwrap())
        );
    }
}
