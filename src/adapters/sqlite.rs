use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::application::ports::{MessageRepository, StoreOutcome};
use crate::domain::channel::{
    Channel, IrcChannel, IrcChannelName, IrcNetworkName, MatrixRoom, Network,
};
use crate::domain::delivery::Delivery;
use crate::domain::message::{Message, MessageId, MessageKind};
use crate::domain::time::DateTime;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages (
    seq                  INTEGER PRIMARY KEY, -- insertion order; ids are random
    id                   TEXT    NOT NULL UNIQUE,
    source_network       TEXT    NOT NULL,
    source_channel       TEXT    NOT NULL,
    external_id          TEXT,
    sender               TEXT    NOT NULL,
    kind                 TEXT    NOT NULL,
    body                 TEXT    NOT NULL,
    received_at          INTEGER NOT NULL,
    destination_network  TEXT    NOT NULL,
    destination_channel  TEXT    NOT NULL,
    status               TEXT    NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts             INTEGER NOT NULL DEFAULT 0,
    next_attempt_at      INTEGER NOT NULL,
    last_error           TEXT,
    delivered_at         INTEGER,
    UNIQUE (source_network, external_id)
);

CREATE INDEX IF NOT EXISTS messages_pending
    ON messages (destination_network, destination_channel, seq) WHERE status = 'pending';
";

#[derive(Clone)]
pub struct SqliteMessageRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteMessageRepository {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening database {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::init(conn)
    }

    #[cfg(test)]
    pub fn in_memory() -> anyhow::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> anyhow::Result<Self> {
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn with_conn<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> anyhow::Result<T> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = conn
                .lock()
                .map_err(|_| anyhow!("database mutex poisoned"))?;
            f(&mut conn)
        })
        .await?
    }

    #[cfg(test)]
    fn delivery_status(&self, id: MessageId) -> Option<String> {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM messages WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    }

    #[cfg(test)]
    fn pending_count(&self, destination: &Channel) -> usize {
        let (network, channel) = channel_columns(destination);
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE destination_network = ?1 AND destination_channel = ?2
                   AND status = 'pending'",
                params![network, channel],
                |row| row.get(0),
            )
            .unwrap()
    }
}

#[async_trait]
impl MessageRepository for SqliteMessageRepository {
    async fn store(
        &self,
        id: MessageId,
        message: &Message,
        destination: &Channel,
        now: DateTime,
    ) -> anyhow::Result<StoreOutcome> {
        let message = message.clone();
        let (source_network, source_channel) = channel_columns(message.source());
        let (destination_network, destination_channel) = channel_columns(destination);
        self.with_conn(move |conn| {
            let inserted = conn.execute(
                "INSERT INTO messages
                     (id, source_network, source_channel, external_id, sender, kind, body,
                      received_at, destination_network, destination_channel, status,
                      next_attempt_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', ?8)
                 ON CONFLICT (source_network, external_id) DO NOTHING",
                params![
                    id.to_string(),
                    source_network,
                    source_channel,
                    message.external_id(),
                    message.sender(),
                    kind_column(message.kind()),
                    message.body(),
                    now.unix_millis(),
                    destination_network,
                    destination_channel,
                ],
            )?;
            Ok(if inserted == 0 {
                StoreOutcome::Duplicate
            } else {
                StoreOutcome::Stored
            })
        })
        .await
    }

    async fn pending_channels(&self, network: &Network) -> anyhow::Result<Vec<Channel>> {
        let network = network_column(network);
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT destination_channel FROM messages
                 WHERE destination_network = ?1 AND status = 'pending'",
            )?;
            let names = stmt
                .query_map([&network], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            names
                .into_iter()
                .map(|name| channel_from_columns(&network, name))
                .collect()
        })
        .await
    }

    async fn next_delivery(&self, destination: &Channel) -> anyhow::Result<Option<Delivery>> {
        let destination = destination.clone();
        let (network, channel) = channel_columns(&destination);
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT id, attempts, next_attempt_at, source_network, source_channel,
                            external_id, sender, kind, body
                     FROM messages
                     WHERE destination_network = ?1 AND destination_channel = ?2
                       AND status = 'pending'
                     ORDER BY seq
                     LIMIT 1",
                    params![network, channel],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, u32>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, Option<String>>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                        ))
                    },
                )
                .optional()?;

            let Some((
                id,
                attempts,
                next_attempt_at,
                source_network,
                source_channel,
                external_id,
                sender,
                kind,
                body,
            )) = row
            else {
                return Ok(None);
            };
            Ok(Some(Delivery::new(
                id_from_column(&id)?,
                Message::new(
                    channel_from_columns(&source_network, source_channel)?,
                    external_id,
                    sender,
                    kind_from_column(&kind)?,
                    body,
                ),
                destination,
                attempts,
                DateTime::from_unix_millis(next_attempt_at)
                    .with_context(|| format!("invalid timestamp {next_attempt_at}"))?,
            )))
        })
        .await
    }

    async fn mark_delivered(&self, id: MessageId, now: DateTime) -> anyhow::Result<()> {
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages SET status = 'delivered', delivered_at = ?2 WHERE id = ?1",
                params![id.to_string(), now.unix_millis()],
            )?;
            Ok(())
        })
        .await
    }

    async fn schedule_retry(
        &self,
        id: MessageId,
        attempts: u32,
        next_attempt_at: DateTime,
        error: &str,
    ) -> anyhow::Result<()> {
        let error = error.to_owned();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages SET attempts = ?2, next_attempt_at = ?3, last_error = ?4
                 WHERE id = ?1",
                params![
                    id.to_string(),
                    attempts,
                    next_attempt_at.unix_millis(),
                    error
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn mark_failed(&self, id: MessageId, attempts: u32, error: &str) -> anyhow::Result<()> {
        let error = error.to_owned();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages SET status = 'failed', attempts = ?2, last_error = ?3
                 WHERE id = ?1",
                params![id.to_string(), attempts, error],
            )?;
            Ok(())
        })
        .await
    }
}

fn id_from_column(id: &str) -> anyhow::Result<MessageId> {
    let uuid = Uuid::parse_str(id).with_context(|| format!("invalid message id `{id}`"))?;
    Ok(MessageId::new(uuid))
}

fn kind_column(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Normal => "normal",
        MessageKind::Action => "action",
    }
}

fn kind_from_column(kind: &str) -> anyhow::Result<MessageKind> {
    match kind {
        "normal" => Ok(MessageKind::Normal),
        "action" => Ok(MessageKind::Action),
        _ => anyhow::bail!("unknown message kind `{kind}`"),
    }
}

fn network_column(network: &Network) -> String {
    match network {
        Network::Matrix => "matrix".to_owned(),
        Network::Irc(name) => format!("irc:{name}"),
    }
}

fn channel_columns(channel: &Channel) -> (String, String) {
    let name = match channel {
        Channel::Matrix(room) => room.id(),
        Channel::Irc(channel) => channel.name().as_str(),
    };
    (network_column(&channel.network()), name.to_owned())
}

fn channel_from_columns(network: &str, name: String) -> anyhow::Result<Channel> {
    if network == "matrix" {
        return Ok(Channel::Matrix(MatrixRoom::new(name)));
    }
    let irc_network = network
        .strip_prefix("irc:")
        .with_context(|| format!("unknown network `{network}`"))?;
    Ok(Channel::Irc(IrcChannel::new(
        IrcNetworkName::new(irc_network),
        IrcChannelName::new(&name)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(external_id: Option<&str>) -> Message {
        Message::new(
            Channel::Matrix(MatrixRoom::new("!room:example.org")),
            external_id.map(Into::into),
            "alice".into(),
            MessageKind::Action,
            "waves".into(),
        )
    }

    fn irc(channel: &str) -> Channel {
        Channel::Irc(IrcChannel::new(
            IrcNetworkName::new("libera"),
            IrcChannelName::new(channel).unwrap(),
        ))
    }

    fn at(unix_millis: i64) -> DateTime {
        DateTime::from_unix_millis(unix_millis).unwrap()
    }

    #[tokio::test]
    async fn stores_message_with_a_pending_delivery() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        let id = MessageId::generate();
        let outcome = repo
            .store(id, &message(Some("$event")), &irc("#a"), at(10))
            .await
            .unwrap();
        assert_eq!(outcome, StoreOutcome::Stored);

        let channels = repo
            .pending_channels(&Network::Irc(IrcNetworkName::new("libera")))
            .await
            .unwrap();
        assert_eq!(channels, vec![irc("#a")]);

        assert_eq!(repo.pending_count(&irc("#a")), 1);
        let next = repo.next_delivery(&irc("#a")).await.unwrap().unwrap();
        assert_eq!(next.id(), id);
        assert_eq!(next.message(), &message(Some("$event")));
        assert_eq!(next.destination(), &irc("#a"));
        assert_eq!(next.next_attempt_at(), at(10));
    }

    #[tokio::test]
    async fn drops_duplicate_external_ids_but_not_messages_without_one() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        let store = |external_id| {
            let repo = repo.clone();
            async move {
                repo.store(
                    MessageId::generate(),
                    &message(external_id),
                    &irc("#a"),
                    at(0),
                )
                .await
                .unwrap()
            }
        };

        assert_eq!(store(Some("$e")).await, StoreOutcome::Stored);
        assert_eq!(store(Some("$e")).await, StoreOutcome::Duplicate);
        assert_eq!(store(None).await, StoreOutcome::Stored);
        assert_eq!(store(None).await, StoreOutcome::Stored);
        assert_eq!(repo.pending_count(&irc("#a")), 3);
    }

    #[tokio::test]
    async fn next_delivery_is_oldest_pending_and_status_transitions_advance_it() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        // The ids sort opposite to insertion order, so ordering must not rely on them.
        let first = MessageId::new(Uuid::from_u128(2));
        let second = MessageId::new(Uuid::from_u128(1));
        for id in [first, second] {
            repo.store(id, &message(None), &irc("#a"), at(0))
                .await
                .unwrap();
        }

        let next = repo.next_delivery(&irc("#a")).await.unwrap().unwrap();
        assert_eq!(next.id(), first);

        repo.schedule_retry(first, 1, at(500), "boom")
            .await
            .unwrap();
        let next = repo.next_delivery(&irc("#a")).await.unwrap().unwrap();
        assert_eq!(
            (next.id(), next.attempts(), next.next_attempt_at()),
            (first, 1, at(500))
        );

        repo.mark_delivered(first, at(600)).await.unwrap();
        let next = repo.next_delivery(&irc("#a")).await.unwrap().unwrap();
        assert_eq!(next.id(), second);

        repo.mark_failed(second, 30, "gone").await.unwrap();
        assert!(repo.next_delivery(&irc("#a")).await.unwrap().is_none());
        assert_eq!(repo.delivery_status(first).as_deref(), Some("delivered"));
        assert_eq!(repo.delivery_status(second).as_deref(), Some("failed"));
    }
}
