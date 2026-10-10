use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use rusqlite::{Connection, params};

use crate::application::ports::{MessageRepository, StoreOutcome};
use crate::domain::delivery::{Delivery, DeliveryId};
use crate::domain::endpoint::{Endpoint, NetworkId};
use crate::domain::message::{Message, MessageKind};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY,
    source_network  TEXT    NOT NULL,
    source_channel  TEXT    NOT NULL,
    external_id     TEXT,
    sender          TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    body            TEXT    NOT NULL,
    received_at     INTEGER NOT NULL,
    UNIQUE (source_network, external_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id               INTEGER PRIMARY KEY,
    message_id       INTEGER NOT NULL REFERENCES messages (id),
    network          TEXT    NOT NULL,
    channel          TEXT    NOT NULL,
    status           TEXT    NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  INTEGER NOT NULL,
    last_error       TEXT,
    delivered_at     INTEGER
);

CREATE INDEX IF NOT EXISTS deliveries_pending
    ON deliveries (network, channel, id) WHERE status = 'pending';
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
        conn.pragma_update(None, "foreign_keys", "ON")?;
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
}

#[async_trait]
impl MessageRepository for SqliteMessageRepository {
    async fn store(
        &self,
        message: &Message,
        destinations: &[Endpoint],
        now: i64,
    ) -> anyhow::Result<StoreOutcome> {
        let message = message.clone();
        let destinations = destinations.to_vec();
        self.with_conn(move |conn| {
            let tx = conn.transaction()?;
            let inserted = tx.execute(
                "INSERT INTO messages
                     (source_network, source_channel, external_id, sender, kind, body, received_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT (source_network, external_id) DO NOTHING",
                params![
                    message.source.network.as_str(),
                    message.source.channel,
                    message.external_id,
                    message.sender,
                    message.kind.as_str(),
                    message.body,
                    now,
                ],
            )?;
            if inserted == 0 {
                return Ok(StoreOutcome::Duplicate);
            }

            let message_id = tx.last_insert_rowid();
            {
                let mut insert = tx.prepare(
                    "INSERT INTO deliveries (message_id, network, channel, status, next_attempt_at)
                     VALUES (?1, ?2, ?3, 'pending', ?4)",
                )?;
                for destination in &destinations {
                    insert.execute(params![
                        message_id,
                        destination.network.as_str(),
                        destination.channel,
                        now,
                    ])?;
                }
            }
            tx.commit()?;
            Ok(StoreOutcome::Stored)
        })
        .await
    }

    async fn pending_channels(&self, network: &NetworkId) -> anyhow::Result<Vec<String>> {
        let network = network.as_str().to_owned();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT channel FROM deliveries
                 WHERE network = ?1 AND status = 'pending'",
            )?;
            let channels = stmt
                .query_map([network], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            Ok(channels)
        })
        .await
    }

    async fn pending_deliveries(
        &self,
        destination: &Endpoint,
        limit: usize,
    ) -> anyhow::Result<Vec<Delivery>> {
        let destination = destination.clone();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT d.id, d.attempts, d.next_attempt_at,
                        m.source_network, m.source_channel, m.external_id,
                        m.sender, m.kind, m.body
                 FROM deliveries d JOIN messages m ON m.id = d.message_id
                 WHERE d.network = ?1 AND d.channel = ?2 AND d.status = 'pending'
                 ORDER BY d.id
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![
                    destination.network.as_str(),
                    destination.channel,
                    limit as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
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
            )?;

            let mut deliveries = Vec::new();
            for row in rows {
                let (
                    id,
                    attempts,
                    next_attempt_at,
                    network,
                    channel,
                    external_id,
                    sender,
                    kind,
                    body,
                ) = row?;
                let kind = MessageKind::parse(&kind)
                    .with_context(|| format!("unknown message kind `{kind}`"))?;
                deliveries.push(Delivery {
                    id: DeliveryId(id),
                    message: Message {
                        source: Endpoint::new(NetworkId::new(network), channel),
                        external_id,
                        sender,
                        kind,
                        body,
                    },
                    destination: destination.clone(),
                    attempts,
                    next_attempt_at,
                });
            }
            Ok(deliveries)
        })
        .await
    }

    async fn mark_delivered(&self, id: DeliveryId, now: i64) -> anyhow::Result<()> {
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE deliveries SET status = 'delivered', delivered_at = ?2 WHERE id = ?1",
                params![id.0, now],
            )?;
            Ok(())
        })
        .await
    }

    async fn schedule_retry(
        &self,
        id: DeliveryId,
        attempts: u32,
        next_attempt_at: i64,
        error: &str,
    ) -> anyhow::Result<()> {
        let error = error.to_owned();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE deliveries SET attempts = ?2, next_attempt_at = ?3, last_error = ?4
                 WHERE id = ?1",
                params![id.0, attempts, next_attempt_at, error],
            )?;
            Ok(())
        })
        .await
    }

    async fn mark_failed(&self, id: DeliveryId, attempts: u32, error: &str) -> anyhow::Result<()> {
        let error = error.to_owned();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE deliveries SET status = 'failed', attempts = ?2, last_error = ?3
                 WHERE id = ?1",
                params![id.0, attempts, error],
            )?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
impl SqliteMessageRepository {
    fn delivery_status(&self, id: DeliveryId) -> Option<String> {
        use rusqlite::OptionalExtension;

        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM deliveries WHERE id = ?1",
                [id.0],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(external_id: Option<&str>) -> Message {
        Message {
            source: Endpoint::new(NetworkId::new("matrix"), "!room:example.org"),
            external_id: external_id.map(Into::into),
            sender: "alice".into(),
            kind: MessageKind::Emote,
            body: "waves".into(),
        }
    }

    fn irc(channel: &str) -> Endpoint {
        Endpoint::new(NetworkId::new("irc:libera"), channel)
    }

    #[tokio::test]
    async fn stores_message_and_one_delivery_per_destination() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        let outcome = repo
            .store(&message(Some("$event")), &[irc("#a"), irc("#b")], 10)
            .await
            .unwrap();
        assert_eq!(outcome, StoreOutcome::Stored);

        let mut channels = repo
            .pending_channels(&NetworkId::new("irc:libera"))
            .await
            .unwrap();
        channels.sort();
        assert_eq!(channels, vec!["#a", "#b"]);

        let pending = repo.pending_deliveries(&irc("#a"), 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message, message(Some("$event")));
        assert_eq!(pending[0].next_attempt_at, 10);
    }

    #[tokio::test]
    async fn drops_duplicate_external_ids_but_not_messages_without_one() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        let dest = [irc("#a")];
        assert_eq!(
            repo.store(&message(Some("$e")), &dest, 0).await.unwrap(),
            StoreOutcome::Stored
        );
        assert_eq!(
            repo.store(&message(Some("$e")), &dest, 0).await.unwrap(),
            StoreOutcome::Duplicate
        );
        assert_eq!(
            repo.store(&message(None), &dest, 0).await.unwrap(),
            StoreOutcome::Stored
        );
        assert_eq!(
            repo.store(&message(None), &dest, 0).await.unwrap(),
            StoreOutcome::Stored
        );
        assert_eq!(
            repo.pending_deliveries(&irc("#a"), 10).await.unwrap().len(),
            3
        );
    }

    #[tokio::test]
    async fn status_transitions_remove_from_pending() {
        let repo = SqliteMessageRepository::in_memory().unwrap();
        repo.store(&message(None), &[irc("#a")], 0).await.unwrap();
        repo.store(&message(None), &[irc("#a")], 0).await.unwrap();
        let pending = repo.pending_deliveries(&irc("#a"), 10).await.unwrap();
        let (first, second) = (pending[0].id, pending[1].id);

        repo.schedule_retry(first, 1, 500, "boom").await.unwrap();
        let pending = repo.pending_deliveries(&irc("#a"), 10).await.unwrap();
        assert_eq!((pending[0].attempts, pending[0].next_attempt_at), (1, 500));

        repo.mark_delivered(first, 600).await.unwrap();
        repo.mark_failed(second, 30, "gone").await.unwrap();
        assert!(
            repo.pending_deliveries(&irc("#a"), 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(repo.delivery_status(first).as_deref(), Some("delivered"));
        assert_eq!(repo.delivery_status(second).as_deref(), Some("failed"));
    }
}
