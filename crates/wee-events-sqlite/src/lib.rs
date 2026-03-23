mod documents;
mod factory;
mod projection;
mod schema;

pub use documents::{Document, DocumentStore};
pub use factory::SqliteStore;
pub use projection::{apply_projection, rebuild_projection};

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use ulid::Generator;
use wee_events::{
    Aggregate, ChangeSet, EventData, EventMetadata, EventStore, PublishOptions, RawEvent,
    RecordedEvent,
};
use wee_events::{AggregateId, AggregateType, CorrelationId, EventId, Revision};

/// SQLite-backed event store. Thread-safe via `Mutex` around the connection.
///
/// Uses a monotonic ULID generator for event IDs and revisions — guarantees
/// strictly increasing values even within the same millisecond. The store
/// validates at write time that each revision is greater than the aggregate's
/// current latest.
pub struct SqliteEventStore {
    conn: Arc<Mutex<Connection>>,
    generator: Mutex<Generator>,
}

impl SqliteEventStore {
    /// Opens (or creates) the database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let conn = Connection::open(path)?;
        Self::configure_and_migrate(conn)
    }

    /// Opens an in-memory database. Useful for testing.
    pub fn open_in_memory() -> Result<Self, Error> {
        let conn = Connection::open_in_memory()?;
        Self::configure_and_migrate(conn)
    }

    /// Creates a store from a shared connection. Used by `SqliteStore` factory.
    pub(crate) fn from_shared(conn: Arc<Mutex<Connection>>) -> Self {
        Self {
            conn,
            generator: Mutex::new(Generator::new()),
        }
    }

    fn configure_and_migrate(conn: Connection) -> Result<Self, Error> {
        schema::configure_and_migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            generator: Mutex::new(Generator::new()),
        })
    }

    fn generate_ulid(&self) -> Result<String, Error> {
        self.generator
            .lock()
            .map_err(|e| Error::Data(e.to_string()))?
            .generate()
            .map(|u| u.to_string())
            .map_err(|e| Error::Data(e.to_string()))
    }

    fn load_sync(conn: &mut Connection, id: &AggregateId) -> Result<Aggregate, Error> {
        let mut stmt = conn.prepare(
            "SELECT event_id, event_type, revision, causation_id, correlation_id, encoding, data
             FROM events
             WHERE aggregate_type = ?1 AND aggregate_key = ?2
             ORDER BY revision",
        )?;

        let events: Vec<RecordedEvent> = stmt
            .query_map(
                params![id.aggregate_type.as_str(), &id.aggregate_key],
                |row| {
                    let event_id: String = row.get(0)?;
                    let event_type: String = row.get(1)?;
                    let revision: String = row.get(2)?;
                    let causation_id: Option<String> = row.get(3)?;
                    let correlation_id: Option<String> = row.get(4)?;
                    let encoding: String = row.get(5)?;
                    let data: Vec<u8> = row.get(6)?;

                    Ok(RecordedEvent {
                        event_id: EventId::new(event_id),
                        event_type: wee_events::EventType::new(event_type),
                        revision: Revision::new(revision),
                        metadata: EventMetadata {
                            causation_id: causation_id.map(EventId::new),
                            correlation_id: correlation_id.map(CorrelationId::new),
                        },
                        data: EventData::raw(encoding, data),
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Aggregate::from_events(id.clone(), events))
    }

    /// Maximum auto-retry attempts when no expected revision is specified.
    ///
    /// Auto-retry exists for forward-compatibility with multi-connection setups
    /// where two writers can race on the same aggregate. With the current
    /// single-mutex design, revisions never conflict in the auto-retry path —
    /// the retry logic is exercised only under multi-writer configurations.
    /// Matches the DynamoDB store's `maxAttempts: 5`.
    const MAX_AUTO_RETRIES: usize = 5;

    fn publish_sync(
        &self,
        conn: &mut Connection,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, Error> {
        if events.is_empty() {
            let agg = Self::load_sync(conn, aggregate_id)?;
            return Ok(ChangeSet {
                aggregate_id: aggregate_id.clone(),
                revision: agg.revision().clone(),
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
            match self.try_publish_once(conn, aggregate_id, &options, &events) {
                Ok(changeset) => return Ok(changeset),
                Err(Error::WeeEvents(wee_events::Error::RevisionConflict { .. }))
                    if can_auto_retry =>
                {
                    // Auto-retry: the monotonic generator will produce greater
                    // ULIDs on the next attempt.
                    last_error = Some(Error::WeeEvents(wee_events::Error::RetryExhausted {
                        attempts: max_attempts,
                    }));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.expect("loop ran at least once, last_error is set"))
    }

    /// Single publish attempt. Generates fresh ULIDs for event IDs and revisions,
    /// inserts with a conditional check that the revision advances the aggregate.
    fn try_publish_once(
        &self,
        conn: &mut Connection,
        aggregate_id: &AggregateId,
        options: &PublishOptions,
        events: &[RawEvent],
    ) -> Result<ChangeSet, Error> {
        let tx = conn.transaction()?;

        let metadata = EventMetadata {
            causation_id: options.causation_id.clone(),
            correlation_id: options.correlation_id.clone(),
        };

        // Conditional INSERT: the revision must be strictly greater than the
        // aggregate's current latest. The condition is enforced atomically
        // inside the INSERT — no separate SELECT/check.
        //
        // Three modes (matching DynamoDB's latestCondition):
        //   1. expected_revision = None: revision must advance (or aggregate is new)
        //   2. expected_revision = zero: aggregate must have no events
        //   3. expected_revision = specific: current latest must match exactly
        //
        // If the condition fails, the SELECT returns no rows, INSERT inserts
        // nothing, and changes() == 0 → RevisionConflict.

        // SQL for mode 1: any new revision greater than current max is accepted.
        const ADVANCE_SQL: &str =
            "INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                                 causation_id, correlation_id, encoding, data)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
             WHERE ?5 > COALESCE(
                 (SELECT MAX(revision) FROM events
                  WHERE aggregate_type = ?2 AND aggregate_key = ?3),
                 '00000000000000000000000000'
             )";

        // SQL for mode 2: aggregate must not yet exist.
        const INITIAL_SQL: &str =
            "INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                                 causation_id, correlation_id, encoding, data)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
             WHERE NOT EXISTS (
                 SELECT 1 FROM events
                 WHERE aggregate_type = ?2 AND aggregate_key = ?3
             )";

        // SQL for mode 3: current latest must equal the expected revision (?10).
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

        let mut recorded = Vec::with_capacity(events.len());

        for (i, raw) in events.iter().enumerate() {
            let event_id = EventId::new(self.generate_ulid()?);
            let revision_str = self.generate_ulid()?;

            let changes = match (i, &options.expected_revision) {
                // First event, mode 2 (expected = zero): aggregate must not exist
                (0, Some(expected)) if expected.is_zero() => tx.execute(
                    INITIAL_SQL,
                    params![
                        event_id.as_str(),
                        aggregate_id.aggregate_type.as_str(),
                        &aggregate_id.aggregate_key,
                        raw.event_type.as_str(),
                        &revision_str,
                        metadata.causation_id.as_ref().map(|id| id.as_str()),
                        metadata.correlation_id.as_ref().map(|id| id.as_str()),
                        &raw.data.encoding,
                        &raw.data.data,
                    ],
                )?,
                // First event, mode 3 (expected = specific revision): exact match required
                (0, Some(expected)) => tx.execute(
                    EXACT_SQL,
                    params![
                        event_id.as_str(),
                        aggregate_id.aggregate_type.as_str(),
                        &aggregate_id.aggregate_key,
                        raw.event_type.as_str(),
                        &revision_str,
                        metadata.causation_id.as_ref().map(|id| id.as_str()),
                        metadata.correlation_id.as_ref().map(|id| id.as_str()),
                        &raw.data.encoding,
                        &raw.data.data,
                        expected.as_str(),
                    ],
                )?,
                // First event, mode 1 (no expected revision): any advance accepted
                // All subsequent events: always use ADVANCE_SQL
                _ => tx.execute(
                    ADVANCE_SQL,
                    params![
                        event_id.as_str(),
                        aggregate_id.aggregate_type.as_str(),
                        &aggregate_id.aggregate_key,
                        raw.event_type.as_str(),
                        &revision_str,
                        metadata.causation_id.as_ref().map(|id| id.as_str()),
                        metadata.correlation_id.as_ref().map(|id| id.as_str()),
                        &raw.data.encoding,
                        &raw.data.data,
                    ],
                )?,
            };

            if changes == 0 {
                // Condition failed — read current state for the error
                let actual_str: Option<String> = tx
                    .query_row(
                        "SELECT MAX(revision) FROM events
                         WHERE aggregate_type = ?1 AND aggregate_key = ?2",
                        params![
                            aggregate_id.aggregate_type.as_str(),
                            &aggregate_id.aggregate_key
                        ],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();

                let actual = match actual_str {
                    Some(s) => Revision::new(s),
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
                revision: Revision::new(revision_str),
                metadata: metadata.clone(),
                data: raw.data.clone(),
            });
        }

        tx.commit()?;

        let revision = recorded
            .last()
            .expect("recorded is non-empty: events.is_empty() was false")
            .revision
            .clone();
        Ok(ChangeSet {
            aggregate_id: aggregate_id.clone(),
            revision,
            events: recorded,
        })
    }

    /// Returns all distinct aggregate IDs in the store.
    ///
    /// This is an inherent method, not part of the `EventStore` trait —
    /// enumeration is a store-specific capability used for startup replay.
    pub fn enumerate_aggregates(&self) -> Result<Vec<AggregateId>, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let mut stmt = conn.prepare("SELECT DISTINCT aggregate_type, aggregate_key FROM events")?;

        let ids = stmt
            .query_map([], |row| {
                let aggregate_type: String = row.get(0)?;
                let aggregate_key: String = row.get(1)?;
                Ok(AggregateId::new(aggregate_type, aggregate_key))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }

    /// Returns all distinct aggregate IDs of a given type.
    ///
    /// This is an inherent method, not part of the `EventStore` trait —
    /// enumeration is a store-specific capability used for startup replay.
    pub fn enumerate_aggregates_by_type(
        &self,
        aggregate_type: &AggregateType,
    ) -> Result<Vec<AggregateId>, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let mut stmt =
            conn.prepare("SELECT DISTINCT aggregate_key FROM events WHERE aggregate_type = ?1")?;

        let ids = stmt
            .query_map(params![aggregate_type.as_str()], |row| {
                let aggregate_key: String = row.get(0)?;
                Ok(AggregateId::new(aggregate_type.as_str(), aggregate_key))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ids)
    }
}

impl EventStore for SqliteEventStore {
    async fn load(&self, id: &AggregateId) -> Result<Aggregate, wee_events::Error> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| wee_events::Error::Store(e.to_string()))?;
        Self::load_sync(&mut conn, id).map_err(Into::into)
    }

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, wee_events::Error> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| wee_events::Error::Store(e.to_string()))?;
        self.publish_sync(&mut conn, aggregate_id, options, events)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("data error: {0}")]
    Data(String),

    #[error(transparent)]
    WeeEvents(#[from] wee_events::Error),
}

impl From<Error> for wee_events::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::WeeEvents(inner) => inner,
            other => wee_events::Error::Store(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wee_events::EventStore;

    #[tokio::test]
    async fn file_backed_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test-events.db");
        let id = AggregateId::new("counter", "persist");

        let event = wee_events::RawEvent {
            event_type: wee_events::EventType::new("counter:incremented"),
            data: EventData::json(&serde_json::json!({"amount": 42})).unwrap(),
        };

        {
            let store = SqliteEventStore::open(&db_path).unwrap();
            store
                .publish(&id, PublishOptions::default(), vec![event])
                .await
                .unwrap();
        }

        {
            let store = SqliteEventStore::open(&db_path).unwrap();
            let agg = store.load(&id).await.unwrap();
            assert_eq!(agg.len(), 1);

            assert!(agg.events()[0].data.is_json());
            let data: serde_json::Value = agg.events()[0].data.deserialize_json().unwrap();
            assert_eq!(data["amount"], 42);
        }
    }

    #[tokio::test]
    async fn binary_encoding_round_trip() {
        let store = SqliteEventStore::open_in_memory().unwrap();
        let id = AggregateId::new("binary-test", "test-1");

        let raw_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let event = wee_events::RawEvent {
            event_type: wee_events::EventType::new("test:binary-event"),
            data: EventData::raw("application/octet-stream", raw_bytes.clone()),
        };

        store
            .publish(&id, PublishOptions::default(), vec![event])
            .await
            .unwrap();

        let agg = store.load(&id).await.unwrap();
        assert_eq!(agg.events()[0].data.encoding, "application/octet-stream");
        assert_eq!(agg.events()[0].data.data, raw_bytes);
        assert!(!agg.events()[0].data.is_json());
    }

    #[test]
    fn enumerate_empty_store() {
        let store = SqliteEventStore::open_in_memory().unwrap();
        let ids = store.enumerate_aggregates().unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn enumerate_returns_all_aggregates() {
        let store = SqliteEventStore::open_in_memory().unwrap();

        let ids = vec![
            AggregateId::new("campaign", "c1"),
            AggregateId::new("campaign", "c2"),
            AggregateId::new("character", "ch1"),
        ];

        for id in &ids {
            let event = RawEvent {
                event_type: wee_events::EventType::new("test:created"),
                data: EventData::json(&serde_json::json!({})).unwrap(),
            };
            store
                .publish(id, PublishOptions::default(), vec![event])
                .await
                .unwrap();
        }

        let mut result = store.enumerate_aggregates().unwrap();
        result.sort_by_key(|a| a.to_string());
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], AggregateId::new("campaign", "c1"));
        assert_eq!(result[1], AggregateId::new("campaign", "c2"));
        assert_eq!(result[2], AggregateId::new("character", "ch1"));
    }

    #[tokio::test]
    async fn enumerate_by_type_filters() {
        let store = SqliteEventStore::open_in_memory().unwrap();

        let ids = vec![
            AggregateId::new("campaign", "c1"),
            AggregateId::new("campaign", "c2"),
            AggregateId::new("character", "ch1"),
        ];

        for id in &ids {
            let event = RawEvent {
                event_type: wee_events::EventType::new("test:created"),
                data: EventData::json(&serde_json::json!({})).unwrap(),
            };
            store
                .publish(id, PublishOptions::default(), vec![event])
                .await
                .unwrap();
        }

        let campaign_type = AggregateType::new("campaign");
        let result = store.enumerate_aggregates_by_type(&campaign_type).unwrap();
        assert_eq!(result.len(), 2);

        let character_type = AggregateType::new("character");
        let result = store.enumerate_aggregates_by_type(&character_type).unwrap();
        assert_eq!(result.len(), 1);

        let unknown_type = AggregateType::new("unknown");
        let result = store.enumerate_aggregates_by_type(&unknown_type).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn schema_migration_v1_to_v2() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("migrate-test.db");

        // Simulate a v1 database by creating events table only
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS events (
                     event_id        TEXT    NOT NULL CHECK(length(event_id) = 26),
                     aggregate_type  TEXT    NOT NULL,
                     aggregate_key   TEXT    NOT NULL,
                     event_type      TEXT    NOT NULL,
                     revision        TEXT    NOT NULL CHECK(length(revision) = 26),
                     causation_id    TEXT,
                     correlation_id  TEXT,
                     encoding        TEXT    NOT NULL,
                     data            BLOB    NOT NULL,
                     PRIMARY KEY (event_id)
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS idx_events_aggregate
                     ON events (aggregate_type, aggregate_key, revision);
                 PRAGMA user_version = 1;
                 COMMIT;",
            )
            .unwrap();
        }

        // Open with new schema — should migrate to v2
        let store = SqliteStore::open(&db_path).unwrap();

        // Verify documents table exists by performing an operation
        let result = store.documents().list("test-collection");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
