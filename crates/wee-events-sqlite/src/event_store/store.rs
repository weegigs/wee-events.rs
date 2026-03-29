use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use libsql::{Connection, TransactionBehavior};
use tokio::sync::Mutex as AsyncMutex;
use ulid::Generator;
use wee_events::{
    Aggregate, AggregateId, AggregateType, ChangeSet, CorrelationId, EventData, EventId,
    EventMetadata, EventStore, PublishOptions, RawEvent, RecordedEvent, Revision,
};

use crate::{database, Error};

use super::backends::{InMemoryTargetResolver, LocalPartitionCatalog, ResolverPartitionCatalog};
use super::types::{
    SqliteBackend, SqlitePartition, SqlitePartitionCatalog, SqlitePartitioning,
    SqlitePartitioningStrategy,
};

type SharedConnection = Arc<AsyncMutex<Connection>>;

/// SQLite-compatible event store backed by libSQL.
pub struct SqliteEventStore {
    catalog: Arc<dyn SqlitePartitionCatalog>,
    connections: AsyncMutex<HashMap<SqlitePartition, SharedConnection>>,
    known_partitions: AsyncMutex<Vec<SqlitePartition>>,
    generator: Mutex<Generator>,
}

impl SqliteEventStore {
    /// Opens (or creates) a single local database at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_backend(
            SqliteBackend::local(path.as_ref().to_path_buf()),
            SqlitePartitioning::Strict(SqlitePartitioningStrategy::Global),
        )
        .await
    }

    /// Opens a local store with the requested strict partitioning strategy.
    ///
    /// For `Global`, `path` is the database file.
    /// For the sharded variants, `path` is the root directory.
    pub async fn open_with_partitioning(
        path: impl AsRef<Path>,
        partitioning: SqlitePartitioningStrategy,
    ) -> Result<Self, Error> {
        Self::open_backend(
            SqliteBackend::local(path.as_ref().to_path_buf()),
            SqlitePartitioning::Strict(partitioning),
        )
        .await
    }

    /// Opens a local store with the requested best-effort partitioning policy.
    ///
    /// `Auto` lets the local backend choose the storage layout it prefers.
    pub async fn open_with_partitioning_policy(
        path: impl AsRef<Path>,
        partitioning: SqlitePartitioning,
    ) -> Result<Self, Error> {
        Self::open_backend(
            SqliteBackend::local(path.as_ref().to_path_buf()),
            partitioning,
        )
        .await
    }

    /// Builds a store from a custom partition catalog.
    pub fn from_catalog(catalog: impl SqlitePartitionCatalog + 'static) -> Self {
        Self {
            catalog: Arc::new(catalog),
            connections: AsyncMutex::new(HashMap::new()),
            known_partitions: AsyncMutex::new(Vec::new()),
            generator: Mutex::new(Generator::new()),
        }
    }

    /// Opens an in-memory database. Useful for testing.
    pub async fn open_in_memory() -> Result<Self, Error> {
        Self::open_in_memory_with_partitioning(SqlitePartitioningStrategy::Global).await
    }

    pub async fn open_in_memory_with_partitioning(
        partitioning: SqlitePartitioningStrategy,
    ) -> Result<Self, Error> {
        Self::open_backend(
            SqliteBackend::in_memory(),
            SqlitePartitioning::Strict(partitioning),
        )
        .await
    }

    pub async fn open_in_memory_with_partitioning_policy(
        partitioning: SqlitePartitioning,
    ) -> Result<Self, Error> {
        Self::open_backend(SqliteBackend::in_memory(), partitioning).await
    }

    /// Opens a store with a backend-specific best-effort partitioning policy.
    pub async fn open_backend(
        backend: SqliteBackend,
        partitioning: SqlitePartitioning,
    ) -> Result<Self, Error> {
        let store = match backend {
            SqliteBackend::InMemory => Self::from_catalog(ResolverPartitionCatalog::new(
                SqlitePartitioning::Strict(
                    partitioning.resolve(SqlitePartitioningStrategy::Global),
                ),
                Arc::new(InMemoryTargetResolver),
            )),
            SqliteBackend::Local(root) => {
                Self::from_catalog(LocalPartitionCatalog::new(root, partitioning)?)
            }
            SqliteBackend::Remote(provisioner) => {
                Self::from_catalog(ResolverPartitionCatalog::new(partitioning, provisioner))
            }
        };

        if matches!(
            partitioning.resolve(SqlitePartitioningStrategy::Aggregate),
            SqlitePartitioningStrategy::Global
        ) {
            let _ = store
                .ensure_partition_open(&SqlitePartition::Single)
                .await?;
        }

        Ok(store)
    }

    /// Returns all distinct aggregate IDs in the store.
    pub async fn enumerate_aggregates(&self) -> Result<Vec<AggregateId>, Error> {
        let mut ids = Vec::new();
        for partition in self.all_known_partitions().await? {
            match &partition {
                SqlitePartition::Single | SqlitePartition::Hashed(_) => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(Self::enumerate_all_from_connection(&conn).await?);
                }
                SqlitePartition::AggregateType(aggregate_type) => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(
                        Self::enumerate_by_type_from_connection(&conn, aggregate_type).await?,
                    );
                }
                SqlitePartition::Aggregate(aggregate_id) => ids.push(aggregate_id.clone()),
            }
        }

        Ok(Self::sorted_unique_ids(ids))
    }

    /// Returns all distinct aggregate IDs of a given type.
    pub async fn enumerate_aggregates_by_type(
        &self,
        aggregate_type: &AggregateType,
    ) -> Result<Vec<AggregateId>, Error> {
        let mut ids = Vec::new();
        for partition in self.all_known_partitions().await? {
            match &partition {
                SqlitePartition::Single | SqlitePartition::Hashed(_) => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(
                        Self::enumerate_by_type_from_connection(&conn, aggregate_type).await?,
                    );
                }
                SqlitePartition::AggregateType(partition_type)
                    if partition_type == aggregate_type =>
                {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(
                        Self::enumerate_by_type_from_connection(&conn, aggregate_type).await?,
                    );
                }
                SqlitePartition::Aggregate(aggregate_id)
                    if &aggregate_id.aggregate_type == aggregate_type =>
                {
                    ids.push(aggregate_id.clone());
                }
                _ => {}
            }
        }

        Ok(Self::sorted_unique_ids(ids))
    }

    async fn ensure_partition_open(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SharedConnection, Error> {
        self.remember_partition(partition).await;
        if let Some(conn) = self.connections.lock().await.get(partition).cloned() {
            return Ok(conn);
        }

        let target = self.catalog.ensure_target_for_partition(partition).await?;
        let new_conn = Arc::new(AsyncMutex::new(self.open_target(&target).await?));

        let mut connections = self.connections.lock().await;
        Ok(connections
            .entry(partition.clone())
            .or_insert_with(|| Arc::clone(&new_conn))
            .clone())
    }

    async fn open_partition_if_exists(
        &self,
        partition: &SqlitePartition,
    ) -> Result<Option<SharedConnection>, Error> {
        self.remember_partition(partition).await;
        if let Some(conn) = self.connections.lock().await.get(partition).cloned() {
            return Ok(Some(conn));
        }

        let Some(target) = self
            .catalog
            .target_for_existing_partition(partition)
            .await?
        else {
            return Ok(None);
        };

        let new_conn = Arc::new(AsyncMutex::new(self.open_target(&target).await?));
        let mut connections = self.connections.lock().await;
        Ok(Some(
            connections
                .entry(partition.clone())
                .or_insert_with(|| Arc::clone(&new_conn))
                .clone(),
        ))
    }

    async fn remember_partition(&self, partition: &SqlitePartition) {
        let mut known = self.known_partitions.lock().await;
        if !known.contains(partition) {
            known.push(partition.clone());
        }
    }

    async fn all_known_partitions(&self) -> Result<Vec<SqlitePartition>, Error> {
        let mut partitions = self.catalog.partitions().await?;
        {
            let known = self.known_partitions.lock().await;
            for partition in known.iter() {
                if !partitions.contains(partition) {
                    partitions.push(partition.clone());
                }
            }
        }

        partitions.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        partitions.dedup();
        Ok(partitions)
    }

    async fn open_target(
        &self,
        target: &super::types::SqliteDatabaseTarget,
    ) -> Result<Connection, Error> {
        database::open_event_store_connection(target).await
    }

    fn sorted_unique_ids(mut ids: Vec<AggregateId>) -> Vec<AggregateId> {
        ids.sort_by(|left, right| {
            left.aggregate_type
                .as_str()
                .cmp(right.aggregate_type.as_str())
                .then_with(|| left.aggregate_key.cmp(&right.aggregate_key))
        });
        ids.dedup();
        ids
    }

    async fn enumerate_all_from_connection(conn: &Connection) -> Result<Vec<AggregateId>, Error> {
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

    async fn enumerate_by_type_from_connection(
        conn: &Connection,
        aggregate_type: &AggregateType,
    ) -> Result<Vec<AggregateId>, Error> {
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

    async fn load_aggregate(&self, id: &AggregateId) -> Result<Aggregate, Error> {
        let partition = self.catalog.partition_for_aggregate(id)?;
        self.remember_partition(&partition).await;
        let Some(conn) = self.open_partition_if_exists(&partition).await? else {
            return Ok(Aggregate::empty(id.clone()));
        };
        let conn = conn.lock().await;
        Self::load_from_connection(&conn, id).await
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

    async fn publish_to_aggregate(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, Error> {
        let partition = self.catalog.partition_for_aggregate(aggregate_id)?;
        self.remember_partition(&partition).await;
        let conn = self.ensure_partition_open(&partition).await?;
        let conn = conn.lock().await;
        self.publish_with_connection(&conn, aggregate_id, options, events)
            .await
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
        self.load_aggregate(id).await.map_err(Into::into)
    }

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, wee_events::Error> {
        self.publish_to_aggregate(aggregate_id, options, events)
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
