use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::future::try_join_all;
use libsql::{Connection, TransactionBehavior};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::sleep;
use ulid::{Generator, Ulid};
use wee_events::{
    Aggregate, AggregateId, AggregateType, ChangeSet, CorrelationId, Encoding, EventData, EventId,
    EventMetadata, EventStore as EventStoreApi, PublishOptions, RawEvent, RecordedEvent,
    RetryDiagnostics, Revision,
};

use crate::{Error, database};

use super::backends::{
    InMemoryTargetResolver, LocalPartitionCatalog, NamedTargetCatalog, SingleTargetCatalog,
};
use super::strategies::{
    GlobalStrategy, LocalPartitionStrategy, PartitionNamingStrategy, PartitionRead,
    PartitionStrategy, SingleTargetPartitionStrategy, SqldNamespacedPartitionStrategy,
};
use super::types::{
    PartitionCatalog, SqldDefaultProvisioner, SqldNamespacedProvisioner, TursoProvisioner,
};

type SharedConnection = Arc<AsyncMutex<Connection>>;
const MAX_AUTO_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 2;
const MAX_RETRY_DELAY_MS: u64 = 64;
const LAZY_CREATE_PARTITION_READY_ATTEMPTS: usize = 30;
const LAZY_CREATE_PARTITION_READY_DELAY_MS: u64 = 1_000;
const MAX_BUSY_RETRIES: usize = 8;
const BUSY_BASE_DELAY_MS: u64 = 5;
const BUSY_MAX_DELAY_MS: u64 = 250;
const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;

/// SQLite-compatible event store backed by libSQL.
///
/// # Connection lifecycle
///
/// Connections are opened lazily per partition and cached for the lifetime of
/// the store. For strategies that produce many partitions (e.g.,
/// [`AggregateStrategy`] — one per aggregate), the connection map grows
/// monotonically. Long-running processes with many distinct aggregates should
/// consider a bounded strategy like [`HashedStrategy`] or [`TypeStrategy`].
pub struct EventStore<S = GlobalStrategy, C = LocalPartitionCatalog<GlobalStrategy>>
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
    strategy: S,
    catalog: C,
    encoding: Encoding,
    connections: AsyncMutex<HashMap<S::Partition, SharedConnection>>,
    generator: Mutex<Generator>,
}

pub type LocalStore<S> = EventStore<S, LocalPartitionCatalog<S>>;
pub type InMemoryStore<S> =
    EventStore<S, SingleTargetCatalog<<S as PartitionStrategy>::Partition, InMemoryTargetResolver>>;
pub type SingleRemoteStore<S, R> =
    EventStore<S, SingleTargetCatalog<<S as PartitionStrategy>::Partition, R>>;
pub type NamedRemoteStore<S, R> = EventStore<S, NamedTargetCatalog<S, R>>;
pub type RemoteStore<S, C> = EventStore<S, C>;

impl<S> EventStore<S, LocalPartitionCatalog<S>>
where
    S: LocalPartitionStrategy,
{
    /// Opens a file-backed store at `path`. For `GlobalStrategy`, `path` is
    /// the database file; for sharded strategies, `path` is the root directory.
    /// Encoding defaults to JSON — call [`with_encoding`](Self::with_encoding)
    /// to override.
    pub async fn open_local(path: impl AsRef<Path>, strategy: S) -> Result<Self, Error> {
        let catalog = LocalPartitionCatalog::new(path.as_ref().to_path_buf(), strategy.clone())?;
        bootstrap(EventStore::from_catalog(strategy, catalog, Encoding::Json)).await
    }
}

impl<S> EventStore<S, SingleTargetCatalog<S::Partition, InMemoryTargetResolver>>
where
    S: PartitionStrategy + SingleTargetPartitionStrategy,
{
    /// Opens an ephemeral in-memory store. Encoding defaults to JSON — call
    /// [`with_encoding`](Self::with_encoding) to override.
    pub async fn open_in_memory(strategy: S) -> Result<Self, Error> {
        let catalog = SingleTargetCatalog::new(InMemoryTargetResolver);
        bootstrap(EventStore::from_catalog(strategy, catalog, Encoding::Json)).await
    }
}

impl<S, R> EventStore<S, SingleTargetCatalog<S::Partition, R>>
where
    S: PartitionStrategy + SingleTargetPartitionStrategy,
    R: SqldDefaultProvisioner,
{
    /// Opens a single-target sqld-backed store. Encoding defaults to JSON.
    pub async fn open_sqld_default(provisioner: R, strategy: S) -> Result<Self, Error> {
        let catalog = SingleTargetCatalog::new(provisioner);
        bootstrap(EventStore::from_catalog(strategy, catalog, Encoding::Json)).await
    }
}

impl<S, R> EventStore<S, NamedTargetCatalog<S, R>>
where
    S: PartitionStrategy + SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
    R: SqldNamespacedProvisioner,
{
    /// Opens a namespaced sqld-backed store. The strategy supplies stable
    /// partition names; the provisioner maps them to sqld namespaces.
    /// Encoding defaults to JSON.
    pub async fn open_sqld_namespaced(provisioner: R, strategy: S) -> Result<Self, Error> {
        let catalog = NamedTargetCatalog::new(strategy.clone(), provisioner);
        bootstrap(EventStore::from_catalog(strategy, catalog, Encoding::Json)).await
    }
}

impl<S, R> EventStore<S, NamedTargetCatalog<S, R>>
where
    S: PartitionStrategy + PartitionNamingStrategy,
    R: TursoProvisioner,
{
    /// Opens a Turso-backed store. Encoding defaults to JSON.
    pub async fn open_turso(provisioner: R, strategy: S) -> Result<Self, Error> {
        let catalog = NamedTargetCatalog::new(strategy.clone(), provisioner);
        bootstrap(EventStore::from_catalog(strategy, catalog, Encoding::Json)).await
    }
}

/// Bootstraps partitions for a freshly-built store and returns it.
async fn bootstrap<S, C>(store: EventStore<S, C>) -> Result<EventStore<S, C>, Error>
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
    for partition in store.strategy.bootstrap_partitions() {
        let _ = store.ensure_partition_open(&partition).await?;
    }
    Ok(store)
}

impl<S, C> EventStore<S, C>
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
    /// Overrides the on-disk payload encoding. Defaults to [`Encoding::Json`]
    /// at construction. Safe to call after `open_*` — encoding only affects
    /// future writes.
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Builds a store from a custom partition catalog.
    pub fn from_catalog(strategy: S, catalog: C, encoding: Encoding) -> Self {
        Self {
            strategy,
            catalog,
            encoding,
            connections: AsyncMutex::new(HashMap::new()),
            generator: Mutex::new(Generator::new()),
        }
    }

    /// Returns all distinct aggregate IDs in the store.
    pub async fn enumerate_aggregates(&self) -> Result<Vec<AggregateId>, Error> {
        let ids = try_join_all(
            self.all_known_partitions()
                .await?
                .into_iter()
                .map(|partition| {
                    let plan = self.strategy.read_plan(&partition);
                    self.enumerate_partition(partition, plan)
                }),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();

        Ok(Self::sorted_unique_ids(ids))
    }

    /// Returns all distinct aggregate IDs of a given type.
    pub async fn enumerate_aggregates_by_type(
        &self,
        aggregate_type: &AggregateType,
    ) -> Result<Vec<AggregateId>, Error> {
        let ids = try_join_all(
            self.all_known_partitions()
                .await?
                .into_iter()
                .map(|partition| {
                    let plan = self.strategy.read_plan_by_type(&partition, aggregate_type);
                    self.enumerate_partition(partition, plan)
                }),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();

        Ok(Self::sorted_unique_ids(ids))
    }

    async fn enumerate_partition(
        &self,
        partition: S::Partition,
        plan: PartitionRead,
    ) -> Result<Vec<AggregateId>, Error> {
        match plan {
            PartitionRead::ScanAll => {
                let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                    return Ok(Vec::new());
                };
                let conn = conn.lock().await;
                Self::enumerate_all_from_connection(&conn).await
            }
            PartitionRead::ScanType(aggregate_type) => {
                let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                    return Ok(Vec::new());
                };
                let conn = conn.lock().await;
                Self::enumerate_by_type_from_connection(&conn, &aggregate_type).await
            }
            PartitionRead::Direct(aggregate_id) => Ok(vec![aggregate_id]),
            PartitionRead::Skip => Ok(Vec::new()),
        }
    }

    async fn ensure_partition_open(
        &self,
        partition: &S::Partition,
    ) -> Result<SharedConnection, Error> {
        let mut connections = self.connections.lock().await;
        if let Some(conn) = connections.get(partition).cloned() {
            return Ok(conn);
        }

        let target = self.catalog.ensure_target_for_partition(partition).await?;
        let conn = self.open_target(&target).await?;
        self.catalog
            .prepare_connection_for_partition(partition, &conn)
            .await?;
        let new_conn = Arc::new(AsyncMutex::new(conn));
        connections.insert(partition.clone(), Arc::clone(&new_conn));
        Ok(new_conn)
    }

    async fn open_partition_if_exists(
        &self,
        partition: &S::Partition,
    ) -> Result<Option<SharedConnection>, Error> {
        let mut connections = self.connections.lock().await;
        if let Some(conn) = connections.get(partition).cloned() {
            return Ok(Some(conn));
        }

        let Some(target) = self
            .catalog
            .target_for_existing_partition(partition)
            .await?
        else {
            return Ok(None);
        };

        let conn = self.open_target(&target).await?;
        self.catalog
            .prepare_connection_for_partition(partition, &conn)
            .await?;
        let new_conn = Arc::new(AsyncMutex::new(conn));
        connections.insert(partition.clone(), Arc::clone(&new_conn));
        Ok(Some(new_conn))
    }

    async fn all_known_partitions(&self) -> Result<Vec<S::Partition>, Error> {
        let mut partitions: BTreeSet<S::Partition> =
            self.catalog.partitions().await?.into_iter().collect();
        partitions.extend(self.connections.lock().await.keys().cloned());
        Ok(partitions.into_iter().collect())
    }

    async fn open_target(
        &self,
        target: &super::types::DatabaseTarget,
    ) -> Result<Connection, Error> {
        database::open_event_store_connection(target).await
    }

    fn sorted_unique_ids(mut ids: Vec<AggregateId>) -> Vec<AggregateId> {
        ids.sort_by(|left, right| {
            left.aggregate_type()
                .as_str()
                .cmp(right.aggregate_type().as_str())
                .then_with(|| left.aggregate_key().cmp(right.aggregate_key()))
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

    fn generate_ulid(&self) -> Result<Ulid, Error> {
        self.generator
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?
            .generate()
            .map_err(|e| Error::Internal(e.to_string()))
    }

    async fn load_from_connection(conn: &Connection, id: &AggregateId) -> Result<Aggregate, Error> {
        let mut rows = conn
            .query(
                "SELECT event_id, event_type, revision, causation_id, correlation_id, encoding, data
                 FROM events
                 WHERE aggregate_type = ?1 AND aggregate_key = ?2
                 ORDER BY revision",
                (id.aggregate_type().as_str(), id.aggregate_key()),
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
            let encoding = Encoding::from_encoding_str(&encoding)
                .map_err(|e| Error::Internal(format!("event row has unknown encoding: {e}")))?;

            let revision = Revision::try_from(revision)
                .map_err(|e| Error::Internal(format!("event row has invalid revision: {e}")))?;
            events.push(RecordedEvent {
                event_id: EventId::new(event_id),
                event_type: wee_events::EventType::new(event_type),
                revision,
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
        let partition = self.strategy.partition_for_aggregate(id)?;
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
                    aggregate_id.aggregate_type().as_str(),
                    aggregate_id.aggregate_key(),
                ),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        row.get(0).map_err(Into::into)
    }

    async fn publish_with_connection(
        //TODO: should probably consume, why do you still need some thing after publishing it?
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
        let max_attempts = if can_auto_retry { MAX_AUTO_RETRIES } else { 1 };

        let mut last_conflict = None;
        for attempt in 0..max_attempts {
            let mut publish_result = self
                .try_publish_once(conn, aggregate_id, &options, &events)
                .await;
            for busy_attempt in 0..MAX_BUSY_RETRIES {
                match &publish_result {
                    Err(PublishAttemptError::Store(error)) if is_sqlite_busy(error) => {
                        sleep(busy_retry_delay(busy_attempt)).await;
                        publish_result = self
                            .try_publish_once(conn, aggregate_id, &options, &events)
                            .await;
                    }
                    _ => break,
                }
            }
            match publish_result {
                Ok(changeset) => return Ok(changeset),
                Err(PublishAttemptError::Conflict(conflict)) if can_auto_retry => {
                    last_conflict = Some(RetryDiagnostics {
                        last_attempted_revision: conflict.attempted_revision.clone(),
                        observed_max_revision: conflict.actual.clone(),
                        possible_clock_skew_ms: possible_clock_skew_ms(
                            &conflict.attempted_revision,
                            &conflict.actual,
                        ),
                    });
                    if attempt + 1 < max_attempts {
                        sleep(retry_delay(attempt)).await;
                    }
                }
                Err(PublishAttemptError::Conflict(conflict)) => {
                    return Err(Error::WeeEvents(wee_events::Error::RevisionConflict {
                        expected: conflict.expected,
                        actual: conflict.actual,
                    }));
                }
                Err(PublishAttemptError::Store(error)) => return Err(error),
            }
        }

        Err(Error::WeeEvents(wee_events::Error::retry_exhausted(
            max_attempts,
            last_conflict.expect("retry loop always records the last conflict"),
        )))
    }

    async fn publish_to_aggregate(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, Error> {
        let partition = self.strategy.partition_for_aggregate(aggregate_id)?;

        for attempt in 0..LAZY_CREATE_PARTITION_READY_ATTEMPTS {
            let result = async {
                let conn = self.ensure_partition_open(&partition).await?;
                let conn = conn.lock().await;
                self.publish_with_connection(&conn, aggregate_id, options.clone(), events.clone())
                    .await
            }
            .await;

            match result {
                Ok(changeset) => return Ok(changeset),
                Err(error)
                    if is_lazy_create_partition_not_ready(&error)
                        && attempt + 1 < LAZY_CREATE_PARTITION_READY_ATTEMPTS =>
                {
                    self.connections.lock().await.remove(&partition);
                    sleep(lazy_create_partition_ready_delay()).await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("lazy create retry loop always returns on the last attempt")
    }

    async fn try_publish_once(
        &self,
        conn: &Connection,
        aggregate_id: &AggregateId,
        options: &PublishOptions,
        events: &[RawEvent],
    ) -> Result<ChangeSet, PublishAttemptError> {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(Error::from)?;
        let metadata = EventMetadata {
            causation_id: options.causation_id.clone(),
            correlation_id: options.correlation_id.clone(),
        };

        let mut recorded = Vec::with_capacity(events.len());
        for (index, raw) in events.iter().enumerate() {
            let event_id_ulid = self.generate_ulid()?;
            let revision_ulid = self.generate_ulid()?;
            let event_id = EventId::new(event_id_ulid.to_string());
            let revision_str = revision_ulid.to_string();
            let changes = execute_publish_statement(
                &tx,
                PublishRow {
                    index,
                    aggregate_id,
                    options,
                    raw,
                    metadata: &metadata,
                    event_id: &event_id,
                    revision: &revision_str,
                },
            )
            .await?;

            if changes == 0 {
                let attempted_revision = Revision::from_ulid(revision_ulid);
                let actual = match Self::current_revision(&tx, aggregate_id).await? {
                    Some(value) => Revision::try_from(value)
                        .map_err(|e| Error::Internal(format!("stored revision is invalid: {e}")))?,
                    None => Revision::zero(),
                };
                let expected = options
                    .expected_revision
                    .clone()
                    .unwrap_or_else(|| attempted_revision.clone());

                return Err(PublishAttemptError::Conflict(PublishConflict {
                    expected,
                    actual,
                    attempted_revision,
                }));
            }

            recorded.push(std::sync::Arc::new(RecordedEvent {
                event_id,
                event_type: raw.event_type.clone(),
                revision: Revision::from_ulid(revision_ulid),
                metadata: metadata.clone(),
                data: raw.data.clone(),
            }));
        }

        tx.commit().await.map_err(Error::from)?;

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

#[derive(Debug, Clone)]
struct PublishConflict {
    expected: Revision,
    actual: Revision,
    attempted_revision: Revision,
}

#[derive(Debug)]
enum PublishAttemptError {
    Conflict(PublishConflict),
    Store(Error),
}

impl From<Error> for PublishAttemptError {
    fn from(error: Error) -> Self {
        Self::Store(error)
    }
}

fn retry_delay(attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.min(5)).expect("clamped to 5");
    let backoff_ms = (BASE_RETRY_DELAY_MS << exponent).min(MAX_RETRY_DELAY_MS);
    let jitter_ms = if backoff_ms == 0 {
        0
    } else {
        next_jitter() % (backoff_ms + 1)
    };

    Duration::from_millis(backoff_ms + jitter_ms)
}

fn lazy_create_partition_ready_delay() -> Duration {
    Duration::from_millis(LAZY_CREATE_PARTITION_READY_DELAY_MS)
}

fn busy_retry_delay(attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.min(6)).expect("clamped to 6");
    let backoff_ms = (BUSY_BASE_DELAY_MS << exponent).min(BUSY_MAX_DELAY_MS);
    let jitter_ms = next_jitter() % (backoff_ms + 1);
    Duration::from_millis(backoff_ms + jitter_ms)
}

/// Lock-free xorshift64* PRNG for retry jitter. Seeded from the system clock
/// on first call. Relaxed ordering — occasional double-step under racy callers
/// is fine; we only need decorrelated short delays.
fn next_jitter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Truncation to low 64 bits is intentional: this is a PRNG seed,
        // not a wall-clock timestamp.
        #[allow(clippy::cast_possible_truncation)]
        let seed = (nanos as u64) | 1;
        x = seed;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    STATE.store(x, Ordering::Relaxed);
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn is_sqlite_busy(error: &Error) -> bool {
    match error {
        Error::Libsql(
            libsql::Error::SqliteFailure(code, _) | libsql::Error::RemoteSqliteFailure(_, code, _),
        ) => *code == SQLITE_BUSY || *code == SQLITE_LOCKED,
        _ => false,
    }
}

/// Detects the lazy-create "namespace doesn't exist yet" condition for sqld /
/// Turso remote backends. The underlying libsql error doesn't carry a
/// structured code for this case — it bubbles up as a 404 inside one of the
/// remote-transport variants. We narrow to those variants so unrelated local
/// failures can't accidentally match, then string-test the inner message.
fn is_lazy_create_partition_not_ready(error: &Error) -> bool {
    let Error::Libsql(libsql_error) = error else {
        return false;
    };
    let message = match libsql_error {
        libsql::Error::ConnectionFailed(s) => s.clone(),
        // Different inner types implementing Display; can't be merged via `|`.
        #[allow(clippy::match_same_arms)]
        libsql::Error::Hrana(e) => e.to_string(),
        libsql::Error::WriteDelegation(e) => e.to_string(),
        _ => return false,
    };
    is_lazy_create_partition_not_ready_message(&message)
}

fn is_lazy_create_partition_not_ready_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    if !message.contains("namespace") {
        return false;
    }
    message.contains("doesn't exist")
        || message.contains("does not exist")
        || message.contains("not found")
        || message.contains("404")
}

fn possible_clock_skew_ms(attempted: &Revision, actual: &Revision) -> Option<u64> {
    let attempted_ulid = Ulid::from_string(attempted.as_str()).ok()?;
    let actual_ulid = Ulid::from_string(actual.as_str()).ok()?;
    actual_ulid
        .timestamp_ms()
        .checked_sub(attempted_ulid.timestamp_ms())
        .filter(|delta| *delta > 0)
}

impl<S, C> wee_events::EncodesEvents for EventStore<S, C>
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
    #[inline]
    fn encoding(&self) -> Encoding {
        self.encoding
    }
}

impl<S, C> EventStoreApi for EventStore<S, C>
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
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

/// Initial-publish INSERT: aggregate must have no prior events.
const SQL_INITIAL: &str = "\
    INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                        causation_id, correlation_id, encoding, data)
    SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
    WHERE NOT EXISTS (
        SELECT 1 FROM events
        WHERE aggregate_type = ?2 AND aggregate_key = ?3
    )";

/// Exact-revision INSERT: monotonic advance + caller-supplied prior revision must match current max.
const SQL_EXACT: &str = "\
    INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                        causation_id, correlation_id, encoding, data)
    SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
    WHERE ?5 > COALESCE(
        (SELECT MAX(revision) FROM events
         WHERE aggregate_type = ?2 AND aggregate_key = ?3),
        '00000000000000000000000000'
    )
    AND (SELECT MAX(revision) FROM events
         WHERE aggregate_type = ?2 AND aggregate_key = ?3) = ?10";

/// Advance INSERT: monotonic advance only, no exact-prior check.
const SQL_ADVANCE: &str = "\
    INSERT INTO events (event_id, aggregate_type, aggregate_key, event_type, revision,
                        causation_id, correlation_id, encoding, data)
    SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
    WHERE ?5 > COALESCE(
        (SELECT MAX(revision) FROM events
         WHERE aggregate_type = ?2 AND aggregate_key = ?3),
        '00000000000000000000000000'
    )";

struct PublishRow<'a> {
    index: usize,
    aggregate_id: &'a AggregateId,
    options: &'a PublishOptions,
    raw: &'a RawEvent,
    metadata: &'a EventMetadata,
    event_id: &'a EventId,
    revision: &'a str,
}

/// Determines the correct concurrency mode and executes the publish INSERT.
///
/// Three modes, all sharing `INSERT_PREFIX`:
/// - **initial**: first event for this aggregate (`expected_revision` is zero)
/// - **exact**: caller expects a specific prior revision (`expected_revision` is non-zero)
/// - **advance**: no expected revision — just ensure monotonic ordering
async fn execute_publish_statement(
    tx: &libsql::Transaction,
    row: PublishRow<'_>,
) -> Result<u64, Error> {
    let causation = row
        .metadata
        .causation_id
        .as_ref()
        .map(wee_events::EventId::as_str);
    let correlation = row
        .metadata
        .correlation_id
        .as_ref()
        .map(wee_events::CorrelationId::as_str);

    match (row.index, &row.options.expected_revision) {
        (0, Some(expected)) if expected.is_zero() => tx
            .execute(
                SQL_INITIAL,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type().as_str(),
                    row.aggregate_id.aggregate_key(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    causation,
                    correlation,
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                ],
            )
            .await
            .map_err(Into::into),
        (0, Some(expected)) => tx
            .execute(
                SQL_EXACT,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type().as_str(),
                    row.aggregate_id.aggregate_key(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    causation,
                    correlation,
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                    expected.as_str(),
                ],
            )
            .await
            .map_err(Into::into),
        _ => tx
            .execute(
                SQL_ADVANCE,
                libsql::params![
                    row.event_id.as_str(),
                    row.aggregate_id.aggregate_type().as_str(),
                    row.aggregate_id.aggregate_key(),
                    row.raw.event_type.as_str(),
                    row.revision,
                    causation,
                    correlation,
                    row.raw.data.encoding.as_str(),
                    row.raw.data.data.clone(),
                ],
            )
            .await
            .map_err(Into::into),
    }
}

#[cfg(test)]
mod tests {
    use super::{is_lazy_create_partition_not_ready_message, possible_clock_skew_ms};
    use ulid::Ulid;
    use wee_events::Revision;

    #[test]
    fn partition_not_ready_detector_matches_turso_namespace_404() {
        let message = r#"Hrana(Api("status=404 Not Found, body={\"error\":\"Namespace `01HXQG8N7R1RJ2XJ8SQ7K6A8SP` doesn't exist\"}"))"#;

        assert!(is_lazy_create_partition_not_ready_message(message));
    }

    #[test]
    fn partition_not_ready_detector_ignores_unrelated_errors() {
        assert!(!is_lazy_create_partition_not_ready_message(
            "database is locked"
        ));
        assert!(!is_lazy_create_partition_not_ready_message(
            "revision conflict"
        ));
    }

    #[test]
    fn possible_clock_skew_reports_positive_timestamp_gap() {
        let attempted = Revision::from_ulid(Ulid::from_parts(1_000, 7));
        let actual = Revision::from_ulid(Ulid::from_parts(1_017, 3));

        assert_eq!(possible_clock_skew_ms(&attempted, &actual), Some(17));
    }

    #[test]
    fn possible_clock_skew_ignores_non_positive_gaps() {
        let attempted = Revision::from_ulid(Ulid::from_parts(1_000, 7));
        let same_ms = Revision::from_ulid(Ulid::from_parts(1_000, 99));
        let earlier = Revision::from_ulid(Ulid::from_parts(999, 3));

        assert_eq!(possible_clock_skew_ms(&attempted, &same_ms), None);
        assert_eq!(possible_clock_skew_ms(&attempted, &earlier), None);
    }
}
