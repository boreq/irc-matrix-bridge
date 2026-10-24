use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::application::ports::{Clock, ConnectionHealth, QueueStatsReader};
use crate::domain::{BridgeName, Bridges, Channel, Network};
use crate::errors::Result;

pub struct BridgeMetrics {
    pub bridge: BridgeName,
    pub matrix_last_success_age: Option<Duration>,
    pub irc_last_success_age: Option<Duration>,
    pub pending_to_matrix_age: Duration,
    pub pending_to_irc_age: Duration,
}

pub struct BridgeMetricsHandler {
    bridges: Arc<Bridges>,
    queue: Arc<dyn QueueStatsReader>,
    health: Arc<dyn ConnectionHealth>,
    clock: Arc<dyn Clock>,
}

impl BridgeMetricsHandler {
    pub fn new(
        bridges: Arc<Bridges>,
        queue: Arc<dyn QueueStatsReader>,
        health: Arc<dyn ConnectionHealth>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            bridges,
            queue,
            health,
            clock,
        }
    }

    pub async fn handle(&self) -> Result<Vec<BridgeMetrics>> {
        let pending = self.queue.oldest_pending().await?;
        let now = self.clock.now();
        let ages: HashMap<_, _> = pending
            .into_iter()
            .map(|entry| {
                (
                    entry.destination().clone(),
                    entry.received_at().duration_until(now),
                )
            })
            .collect();
        let health_age = |network| {
            self.health
                .last_healthy(&network)
                .map(|last| last.duration_until(now))
        };
        let matrix_age = health_age(Network::Matrix);
        Ok(self
            .bridges
            .iter()
            .map(|bridge| BridgeMetrics {
                bridge: bridge.name().clone(),
                matrix_last_success_age: matrix_age,
                irc_last_success_age: health_age(Network::Irc(bridge.irc().network().clone())),
                pending_to_matrix_age: ages
                    .get(&Channel::Matrix(bridge.matrix().clone()))
                    .copied()
                    .unwrap_or_default(),
                pending_to_irc_age: ages
                    .get(&Channel::Irc(bridge.irc().clone()))
                    .copied()
                    .unwrap_or_default(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::connection_health::InMemoryConnectionHealth;
    use crate::adapters::sqlite::SqliteMessageRepository;
    use crate::application::ports::MessageRepository;
    use crate::domain::time::DateTime;
    use crate::domain::{
        Bridge, IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom, Message, MessageBody,
        MessageId, MessageKind,
    };

    struct FixedClock {
        now: DateTime,
    }

    impl Clock for FixedClock {
        fn now(&self) -> DateTime {
            self.now
        }
    }

    #[tokio::test]
    async fn reports_health_and_oldest_pending_by_bridge_and_destination() {
        let bridge = |name, network, room, channel| {
            Bridge::new(
                BridgeName::new(name).unwrap(),
                MatrixRoom::new(room).unwrap(),
                IrcChannel::new(
                    IrcNetworkName::new(network),
                    IrcChannelName::new(channel).unwrap(),
                ),
            )
        };
        let first = bridge("first", "one", "!one", "#one");
        let second = bridge("second", "two", "!two", "#two");
        let third = bridge("third", "one", "!three", "#three");
        let repo = Arc::new(SqliteMessageRepository::in_memory().unwrap());
        let now = DateTime::from_unix_millis(20_000).unwrap();
        let health = Arc::new(InMemoryConnectionHealth::new(Arc::new(FixedClock {
            now: DateTime::from_unix_millis(15_000).unwrap(),
        })));
        health.record_healthy(&Network::Matrix);
        health.record_healthy(&Network::Irc(first.irc().network().clone()));
        let irc = Channel::Irc(first.irc().clone());
        let matrix = Channel::Matrix(first.matrix().clone());
        for (source, destination, received) in [
            (matrix.clone(), irc.clone(), 1_000),
            (matrix.clone(), irc.clone(), 5_000),
            (irc, matrix, 10_000),
        ] {
            let message = Message::new(
                source,
                None,
                "alice".into(),
                MessageKind::Normal,
                MessageBody::new("hello").unwrap(),
            );
            repo.store(
                MessageId::generate(),
                &message,
                &destination,
                DateTime::from_unix_millis(received).unwrap(),
            )
            .await
            .unwrap();
        }
        let handler = BridgeMetricsHandler::new(
            Arc::new(Bridges::new(vec![first, second, third]).unwrap()),
            repo,
            health,
            Arc::new(FixedClock { now }),
        );
        let metrics = handler.handle().await.unwrap();
        assert_eq!(metrics.len(), 3);
        assert_eq!(metrics[0].pending_to_irc_age, Duration::from_secs(19));
        assert_eq!(metrics[0].pending_to_matrix_age, Duration::from_secs(10));
        assert_eq!(
            metrics[0].matrix_last_success_age,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            metrics[0].irc_last_success_age,
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            metrics[1].matrix_last_success_age,
            metrics[0].matrix_last_success_age
        );
        assert_eq!(metrics[1].irc_last_success_age, None);
        assert_eq!(metrics[1].pending_to_irc_age, Duration::ZERO);
        assert_eq!(metrics[1].pending_to_matrix_age, Duration::ZERO);
        assert_eq!(
            metrics[2].irc_last_success_age,
            metrics[0].irc_last_success_age
        );
        assert_eq!(metrics[2].pending_to_irc_age, Duration::ZERO);
    }
}
