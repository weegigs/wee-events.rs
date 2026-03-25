use std::path::Path;
use std::sync::Mutex;

use libsql::{Connection, TransactionBehavior};
use tokio::sync::Mutex as AsyncMutex;
use ulid::Generator;
use wee_events::{
    Aggregate, AggregateId, AggregateType, ChangeSet, CorrelationId, EventData, EventId,
    EventMetadata, EventStore, PublishOptions, RawEvent, RecordedEvent, Revision,
};

use crate::{database, Error};

/// SQLite-compatible event store backed by libSQL.
pub struct SqliteEventStore {
    conn: AsyncMutex<Connection>,
    generator: Mutex<Generator>,
}

impl SqliteEventStore {
    /// Opens (or creates) the database at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            conn: AsyncMutex::new(database::open_event_store_local_connection(path).await?),
            generator: Mutex::new(Generator::new()),
        })
    }

    /// Opens an in-memory database. Useful for testing.
    pub async fn open_in_memory() -> Result<Self, Error> {
        Ok(Self {
            conn: AsyncMutex::new(database::open_event_store_in_memory_connection().await?),
            generator: Mutex::new(Generator::new()),
        })
    }

    /// Returns all distinct aggregate IDs in the store.
    pub async fn enumerate_aggregates(&self) -> Result<Vec<AggregateId>, Error> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT DISTINCT aggregate_type, aggregate_key FROM events",
                (),
            )
            .await?;

        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            let aggregate_type: String = row.get(0)?;
            let aggregate_key: String = row.get(1)?;
            ids.push(AggregateId::new(aggregate_type, aggregate_key));
        }

        Ok(ids)
    }

    /// Returns all distinct aggregate IDs of a given type.
    pub async fn enumerate_aggregates_by_type(
        &self,
        aggregate_type: &AggregateType,
    ) -> Result<Vec<AggregateId>, Error> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT DISTINCT aggregate_key FROM events WHERE aggregate_type = ?1",
                [aggregate_type.as_str()],
            )
            .await?;

        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            let aggregate_key: String = row.get(0)?;
            ids.push(AggregateId::new(aggregate_type.as_str(), aggregate_key));
        }

        Ok(ids)
    }

    fn generate_ulid(&self) -> Result<String, Error> {
        self.generator
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?
            .generate()
            .map(|u| u.to_string())
            .map_err(|e| Error::Internal(e.to_string()))
    }

    async fn load_from_connection(conn: &Connection, id: &AggregateId) -> Result<Aggregate, Error> {
        let mut rows = conn
            .query(
                "SELECT event_id, event_type, revision, causation_id, correlation_id, encoding, data
                 FROM events
                 WHERE aggregate_type = ?1 AND aggregate_key = ?2
                 ORDER BY revision",
                (id.aggregate_type.as_str(), id.aggregate_key.as_str()),
            )
            .await?;

        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            let event_id: String = row.get(0)?;
            let event_type: String = row.get(1)?;
            let revision: String = row.get(2)?;
            let causation_id: Option<String> = row.get(3)?;
            let correlation_id: Option<String> = row.get(4)?;
            let encoding: String = row.get(5)?;
            let data: Vec<u8> = row.get(6)?;

            events.push(RecordedEvent {
                event_id: EventId::new(event_id),
                event_type: wee_events::EventType::new(event_type),
                revision: Revision::new(revision),
                metadata: EventMetadata {
                    causation_id: causation_id.map(EventId::new),
                    correlation_id: correlation_id.map(CorrelationId::new),
                },
                data: EventData::raw(encoding, data),
            });
        }

        Ok(Aggregate::from_events(id.clone(), events))
    }

    async fn current_revision(
        conn: &Connection,
        aggregate_id: &AggregateId,
    ) -> Result<Option<String>, Error> {
        let mut rows = conn
            .query(
                "SELECT MAX(revision) FROM events
                 WHERE aggregate_type = ?1 AND aggregate_key = ?2",
                (
                    aggregate_id.aggregate_type.as_str(),
                    aggregate_id.aggregate_key.as_str(),
                ),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        row.get(0).map_err(Into::into)
    }

    const MAX_AUTO_RETRIES: usize = 5;

    async fn publish_with_connection(
        &self,
        conn: &Connection,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, Error> {
        if events.is_empty() {
            let aggregate = Self::load_from_connection(conn, aggregate_id).await?;
            return Ok(ChangeSet {
                aggregate_id: aggregate_id.clone(),
                revision: aggregate.revision().clone(),
                events: Vec::new(),
            });
        }

        let can_auto_retry = options.expected_revision.is_none();
        let max_attempts = if can_auto_retry {
            Self::MAX_AUTO_RETRIES
        } else {
            1
        };

        let mut last_error = None;
        for _attempt in 0..max_attempts {
            match self
                .try_publish_once(conn, aggregate_id, &options, &events)
                .await
            {
                Ok(changeset) => return Ok(changeset),
                Err(Error::WeeEvents(wee_events::Error::RevisionConflict { .. }))
                    if can_auto_retry =>
                {
                    last_error = Some(Error::WeeEvents(wee_events::Error::RetryExhausted {
                        attempts: max_attempts,
                    }));
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.expect("retry loop always records the last conflict"))
    }

    async fn try_publish_once(
        &self,
        conn: &Connection,
        aggregate_id: &AggregateId,
        options: &PublishOptions,
        events: &[RawEvent],
    ) -> Result<ChangeSet, Error> {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let metadata = EventMetadata {
            causation_id: options.causation_id.clone(),
            correlation_id: options.correlation_id.clone(),
        };

        let mut recorded = Vec::with_capacity(events.len());
        for (index, raw) in events.iter().enumerate() {
            let event_id = EventId::new(self.generate_ulid()?);
            let revision = self.generate_ulid()?;
            let changes = execute_publish_statement(
                &tx,
                PublishRow {
                    index,
                    aggregate_id,
                    options,
                    raw,
                    metadata: &metadata,
                    event_id: &event_id,
                    revision: &revision,
                },
            )
            .await?;

            if changes == 0 {
                let actual = match Self::current_revision(&tx, aggregate_id).await? {
                    Some(value) => Revision::new(value),
                    None => Revision::zero(),
                };
                let expected = options
                    .expected_revision
                    .clone()
                    .unwrap_or_else(|| actual.clone());

                return Err(Error::WeeEvents(wee_events::Error::RevisionConflict {
                    expected,
                    actual,
                }));
            }

            recorded.push(RecordedEvent {
                event_id,
                event_type: raw.event_type.clone(),
                revision: Revision::new(revision),
                metadata: metadata.clone(),
                data: raw.data.clone(),
            });
        }

        tx.commit().await?;

        Ok(ChangeSet {
            aggregate_id: aggregate_id.clone(),
            revision: recorded
                .last()
                .expect("recorded is non-empty because events was non-empty")
                .revision
                .clone(),
            events: recorded,
        })
    }
}

impl EventStore for SqliteEventStore {
    async fn load(&self, id: &AggregateId) -> Result<Aggregate, wee_events::Error> {
        let conn = self.conn.lock().await;
        Self::load_from_connection(&conn, id)
            .await
            .map_err(Into::into)
    }

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, wee_events::Error> {
        let conn = self.conn.lock().await;
        self.publish_with_connection(&conn, aggregate_id, options, events)
            .await
            .map_err(Into::into)
    }
}

const ADVANCE_SQL: &str =
    "INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                         causation_id, correlation_id, encoding, data)
     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
     WHERE ?5 > COALESCE(
         (SELECT MAX(revision) FROM events
          WHERE aggregate_type = ?2 AND aggregate_key = ?3),
         '00000000000000000000000000'
     )";

const INITIAL_SQL: &str =
    "INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                         causation_id, correlation_id, encoding, data)
     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
     WHERE NOT EXISTS (
         SELECT 1 FROM events
         WHERE aggregate_type = ?2 AND aggregate_key = ?3
     )";

const EXACT_SQL: &str =
    "INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                         causation_id, correlation_id, encoding, data)
     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
     WHERE ?5 > COALESCE(
         (SELECT MAX(revision) FROM events
          WHERE aggregate_type = ?2 AND aggregate_key = ?3),
         '00000000000000000000000000'
     )
     AND (SELECT MAX(revision) FROM events
          WHERE aggregate_type = ?2 AND aggregate_key = ?3) = ?10";

struct PublishRow<'a> {
    index: usize,
    aggregate_id: &'a AggregateId,
    options: &'a PublishOptions,
    raw: &'a RawEvent,
    metadata: &'a EventMetadata,
    event_id: &'a EventId,
    revision: &'a str,
}

async fn execute_publish_statement(
    tx: &libsql::Transaction,
    row: PublishRow<'_>,
) -> Result<u64, Error> {
    match (row.index, &row.options.expected_revision) {
        (0, Some(expected)) if expected.is_zero() => tx
            .execute(
                INITIAL_SQL,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type.as_str(),
                    row.aggregate_id.aggregate_key.as_str(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    row.metadata.causation_id.as_ref().map(|id| id.as_str()),
                    row.metadata.correlation_id.as_ref().map(|id| id.as_str()),
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                ],
            )
            .await
            .map_err(Into::into),
        (0, Some(expected)) => tx
            .execute(
                EXACT_SQL,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type.as_str(),
                    row.aggregate_id.aggregate_key.as_str(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    row.metadata.causation_id.as_ref().map(|id| id.as_str()),
                    row.metadata.correlation_id.as_ref().map(|id| id.as_str()),
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                    expected.as_str(),
                ],
            )
            .await
            .map_err(Into::into),
        _ => tx
            .execute(
                ADVANCE_SQL,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type.as_str(),
                    row.aggregate_id.aggregate_key.as_str(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    row.metadata.causation_id.as_ref().map(|id| id.as_str()),
                    row.metadata.correlation_id.as_ref().map(|id| id.as_str()),
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                ],
            )
            .await
            .map_err(Into::into),
    }
}
