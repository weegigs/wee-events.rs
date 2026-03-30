use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use libsql::{Connection, TransactionBehavior};
use tokio::sync::Mutex as AsyncMutex;
use ulid::Generator;
use wee_events::{
    Aggregate, AggregateId, AggregateType, ChangeSet, CorrelationId, EventData, EventId,
    EventMetadata, EventStore, PublishOptions, RawEvent, RecordedEvent, Revision,
};

use crate::{database, Error};

use super::backends::{
    InMemoryTargetResolver, LocalPartitionCatalog, NamedTargetCatalog, SingleTargetCatalog,
};
use super::strategies::{
    GlobalStrategy, SqliteLocalPartitionStrategy, SqlitePartitionNamingStrategy,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteSingleRemotePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
};
use super::types::{
    SqlitePartitionCatalog, SqliteSqldDefaultProvisioner, SqliteSqldNamespacedProvisioner,
    SqliteTursoProvisioner,
};

type SharedConnection = Arc<AsyncMutex<Connection>>;

/// SQLite-compatible event store backed by libSQL.
pub struct SqliteEventStore<S = GlobalStrategy, C = LocalPartitionCatalog<GlobalStrategy>>
where
    S: SqlitePartitionStrategy,
    C: SqlitePartitionCatalog<S::Partition>,
{
    strategy: S,
    catalog: C,
    connections: AsyncMutex<HashMap<S::Partition, SharedConnection>>,
    known_partitions: AsyncMutex<Vec<S::Partition>>,
    generator: Mutex<Generator>,
}

pub type SqliteLocalStore<S> = SqliteEventStore<S, LocalPartitionCatalog<S>>;
pub type SqliteInMemoryStore<S> = SqliteEventStore<
    S,
    SingleTargetCatalog<<S as SqlitePartitionStrategy>::Partition, InMemoryTargetResolver>,
>;
pub type SqliteSingleRemoteStore<S, R> =
    SqliteEventStore<S, SingleTargetCatalog<<S as SqlitePartitionStrategy>::Partition, R>>;
pub type SqliteNamedRemoteStore<S, R> = SqliteEventStore<S, NamedTargetCatalog<S, R>>;
pub type SqliteRemoteStore<S, C> = SqliteEventStore<S, C>;

impl SqliteEventStore<GlobalStrategy, LocalPartitionCatalog<GlobalStrategy>> {
    pub fn builder() -> SqliteEventStoreBuilder<MissingBackend, MissingStrategy<()>> {
        SqliteEventStoreBuilder::new()
    }
}

impl<S> SqliteEventStore<S, LocalPartitionCatalog<S>>
where
    S: SqliteLocalPartitionStrategy,
{
    /// Opens a local store with the requested partitioning strategy.
    ///
    /// For `GlobalStrategy`, `path` is the database file.
    /// For sharded strategies, `path` is the root directory.
    pub async fn open_with_strategy(path: impl AsRef<Path>, strategy: S) -> Result<Self, Error> {
        let store = SqliteEventStore::from_catalog(
            strategy.clone(),
            LocalPartitionCatalog::new(path.as_ref().to_path_buf(), strategy)?,
        );

        for partition in store.strategy.bootstrap_partitions() {
            let _ = store.ensure_partition_open(&partition).await?;
        }

        Ok(store)
    }
}

impl<S, C> SqliteEventStore<S, C>
where
    S: SqlitePartitionStrategy,
    C: SqlitePartitionCatalog<S::Partition>,
{
    /// Builds a store from a custom partition catalog.
    pub fn from_catalog(strategy: S, catalog: C) -> Self {
        Self {
            strategy,
            catalog,
            connections: AsyncMutex::new(HashMap::new()),
            known_partitions: AsyncMutex::new(Vec::new()),
            generator: Mutex::new(Generator::new()),
        }
    }

    /// Returns all distinct aggregate IDs in the store.
    pub async fn enumerate_aggregates(&self) -> Result<Vec<AggregateId>, Error> {
        let mut ids = Vec::new();
        for partition in self.all_known_partitions().await? {
            match self.strategy.read_plan(&partition) {
                SqlitePartitionRead::ScanAll => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(Self::enumerate_all_from_connection(&conn).await?);
                }
                SqlitePartitionRead::ScanType(aggregate_type) => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(
                        Self::enumerate_by_type_from_connection(&conn, &aggregate_type).await?,
                    );
                }
                SqlitePartitionRead::Direct(aggregate_id) => ids.push(aggregate_id),
                SqlitePartitionRead::Skip => {}
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
            match self.strategy.read_plan_by_type(&partition, aggregate_type) {
                SqlitePartitionRead::ScanAll => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(Self::enumerate_all_from_connection(&conn).await?);
                }
                SqlitePartitionRead::ScanType(aggregate_type) => {
                    let Some(conn) = self.open_partition_if_exists(&partition).await? else {
                        continue;
                    };
                    let conn = conn.lock().await;
                    ids.extend(
                        Self::enumerate_by_type_from_connection(&conn, &aggregate_type).await?,
                    );
                }
                SqlitePartitionRead::Direct(aggregate_id) => ids.push(aggregate_id),
                SqlitePartitionRead::Skip => {}
            }
        }

        Ok(Self::sorted_unique_ids(ids))
    }

    async fn ensure_partition_open(
        &self,
        partition: &S::Partition,
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
        partition: &S::Partition,
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

    async fn remember_partition(&self, partition: &S::Partition) {
        let mut known = self.known_partitions.lock().await;
        if !known.contains(partition) {
            known.push(partition.clone());
        }
    }

    async fn all_known_partitions(&self) -> Result<Vec<S::Partition>, Error> {
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
        let partition = self.strategy.partition_for_aggregate(id)?;
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
        let partition = self.strategy.partition_for_aggregate(aggregate_id)?;
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

pub struct MissingBackend;
pub struct InMemoryBackend;
pub struct LocalBackend {
    path: PathBuf,
}
pub struct SqldDefaultBackend<R> {
    provisioner: R,
}
pub struct SqldNamespacedBackend<R> {
    provisioner: R,
}
pub struct TursoBackend<R> {
    provisioner: R,
}

pub struct MissingStrategy<P>(PhantomData<P>);
pub struct WithStrategy<S>(S);

pub struct SqliteEventStoreBuilder<B, T> {
    backend: B,
    strategy: T,
}

impl SqliteEventStoreBuilder<MissingBackend, MissingStrategy<()>> {
    fn new() -> Self {
        Self {
            backend: MissingBackend,
            strategy: MissingStrategy(PhantomData),
        }
    }

    pub fn local(
        self,
        path: impl AsRef<Path>,
    ) -> SqliteEventStoreBuilder<LocalBackend, MissingStrategy<()>> {
        SqliteEventStoreBuilder {
            backend: LocalBackend {
                path: path.as_ref().to_path_buf(),
            },
            strategy: self.strategy,
        }
    }

    /// Configures an ephemeral in-memory backend.
    ///
    /// This backend is still partition-aware: non-global strategies create one
    /// in-memory SQLite connection per logical partition. Those partitions are
    /// tracked in-process by the store; they are not converted into external
    /// names or on-disk files.
    pub fn in_memory(self) -> SqliteEventStoreBuilder<InMemoryBackend, MissingStrategy<()>> {
        SqliteEventStoreBuilder {
            backend: InMemoryBackend,
            strategy: self.strategy,
        }
    }

    pub fn sqld_default<R>(
        self,
        provisioner: R,
    ) -> SqliteEventStoreBuilder<SqldDefaultBackend<R>, MissingStrategy<()>>
    where
        R: SqliteSqldDefaultProvisioner,
    {
        SqliteEventStoreBuilder {
            backend: SqldDefaultBackend { provisioner },
            strategy: MissingStrategy(PhantomData),
        }
    }

    /// Configures a namespaced sqld backend.
    ///
    /// Strategies used with this backend must provide stable partition names.
    /// The provisioner is responsible for mapping those names to sqld
    /// namespaces.
    pub fn sqld_namespaced<R>(
        self,
        provisioner: R,
    ) -> SqliteEventStoreBuilder<SqldNamespacedBackend<R>, MissingStrategy<()>>
    where
        R: SqliteSqldNamespacedProvisioner,
    {
        SqliteEventStoreBuilder {
            backend: SqldNamespacedBackend { provisioner },
            strategy: MissingStrategy(PhantomData),
        }
    }

    pub fn turso<R>(
        self,
        provisioner: R,
    ) -> SqliteEventStoreBuilder<TursoBackend<R>, MissingStrategy<()>>
    where
        R: SqliteTursoProvisioner,
    {
        SqliteEventStoreBuilder {
            backend: TursoBackend { provisioner },
            strategy: MissingStrategy(PhantomData),
        }
    }

    pub fn strategy<S>(
        self,
        strategy: S,
    ) -> SqliteEventStoreBuilder<MissingBackend, WithStrategy<S>>
    where
        S: SqlitePartitionStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl SqliteEventStoreBuilder<LocalBackend, MissingStrategy<()>> {
    pub fn strategy<S>(self, strategy: S) -> SqliteEventStoreBuilder<LocalBackend, WithStrategy<S>>
    where
        S: SqliteLocalPartitionStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl SqliteEventStoreBuilder<InMemoryBackend, MissingStrategy<()>> {
    pub fn strategy<S>(
        self,
        strategy: S,
    ) -> SqliteEventStoreBuilder<InMemoryBackend, WithStrategy<S>>
    where
        S: SqlitePartitionStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl<R> SqliteEventStoreBuilder<SqldDefaultBackend<R>, MissingStrategy<()>>
where
    R: SqliteSqldDefaultProvisioner,
{
    pub fn strategy<S>(
        self,
        strategy: S,
    ) -> SqliteEventStoreBuilder<SqldDefaultBackend<R>, WithStrategy<S>>
    where
        S: SqliteSingleRemotePartitionStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl<R> SqliteEventStoreBuilder<SqldNamespacedBackend<R>, MissingStrategy<()>>
where
    R: SqliteSqldNamespacedProvisioner,
{
    pub fn strategy<S>(
        self,
        strategy: S,
    ) -> SqliteEventStoreBuilder<SqldNamespacedBackend<R>, WithStrategy<S>>
    where
        S: SqliteSqldNamespacedPartitionStrategy + SqlitePartitionNamingStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl<R> SqliteEventStoreBuilder<TursoBackend<R>, MissingStrategy<()>>
where
    R: SqliteTursoProvisioner,
{
    pub fn strategy<S>(
        self,
        strategy: S,
    ) -> SqliteEventStoreBuilder<TursoBackend<R>, WithStrategy<S>>
    where
        S: SqliteSingleRemotePartitionStrategy,
    {
        SqliteEventStoreBuilder {
            backend: self.backend,
            strategy: WithStrategy(strategy),
        }
    }
}

impl<S> SqliteEventStoreBuilder<MissingBackend, WithStrategy<S>>
where
    S: SqlitePartitionStrategy,
{
    pub fn in_memory(self) -> SqliteEventStoreBuilder<InMemoryBackend, WithStrategy<S>> {
        SqliteEventStoreBuilder {
            backend: InMemoryBackend,
            strategy: self.strategy,
        }
    }
}

impl<S> SqliteEventStoreBuilder<MissingBackend, WithStrategy<S>>
where
    S: SqliteLocalPartitionStrategy,
{
    pub fn local(
        self,
        path: impl AsRef<Path>,
    ) -> SqliteEventStoreBuilder<LocalBackend, WithStrategy<S>> {
        SqliteEventStoreBuilder {
            backend: LocalBackend {
                path: path.as_ref().to_path_buf(),
            },
            strategy: self.strategy,
        }
    }
}

impl<S> SqliteEventStoreBuilder<MissingBackend, WithStrategy<S>>
where
    S: SqliteSingleRemotePartitionStrategy,
{
    pub fn sqld_default(
        self,
        provisioner: impl SqliteSqldDefaultProvisioner,
    ) -> SqliteEventStoreBuilder<
        SqldDefaultBackend<impl SqliteSqldDefaultProvisioner>,
        WithStrategy<S>,
    > {
        SqliteEventStoreBuilder {
            backend: SqldDefaultBackend { provisioner },
            strategy: self.strategy,
        }
    }

    pub fn turso(
        self,
        provisioner: impl SqliteTursoProvisioner,
    ) -> SqliteEventStoreBuilder<TursoBackend<impl SqliteTursoProvisioner>, WithStrategy<S>> {
        SqliteEventStoreBuilder {
            backend: TursoBackend { provisioner },
            strategy: self.strategy,
        }
    }
}

impl<S> SqliteEventStoreBuilder<MissingBackend, WithStrategy<S>>
where
    S: SqliteSqldNamespacedPartitionStrategy + SqlitePartitionNamingStrategy,
{
    pub fn sqld_namespaced(
        self,
        provisioner: impl SqliteSqldNamespacedProvisioner,
    ) -> SqliteEventStoreBuilder<
        SqldNamespacedBackend<impl SqliteSqldNamespacedProvisioner>,
        WithStrategy<S>,
    > {
        SqliteEventStoreBuilder {
            backend: SqldNamespacedBackend { provisioner },
            strategy: self.strategy,
        }
    }
}

impl<S> SqliteEventStoreBuilder<LocalBackend, WithStrategy<S>>
where
    S: SqliteLocalPartitionStrategy,
{
    pub async fn open(self) -> Result<SqliteLocalStore<S>, Error> {
        SqliteEventStore::open_with_strategy(self.backend.path, self.strategy.0).await
    }
}

impl<S> SqliteEventStoreBuilder<InMemoryBackend, WithStrategy<S>>
where
    S: SqlitePartitionStrategy,
{
    pub async fn open(self) -> Result<SqliteInMemoryStore<S>, Error> {
        let store = SqliteEventStore::from_catalog(
            self.strategy.0.clone(),
            SingleTargetCatalog::new(InMemoryTargetResolver),
        );

        for partition in store.strategy.bootstrap_partitions() {
            let _ = store.ensure_partition_open(&partition).await?;
        }

        Ok(store)
    }
}

impl<S, R> SqliteEventStoreBuilder<SqldDefaultBackend<R>, WithStrategy<S>>
where
    S: SqliteSingleRemotePartitionStrategy,
    R: SqliteSqldDefaultProvisioner,
{
    pub async fn open(self) -> Result<SqliteSingleRemoteStore<S, R>, Error> {
        let store = SqliteEventStore::from_catalog(
            self.strategy.0.clone(),
            SingleTargetCatalog::new(self.backend.provisioner),
        );

        for partition in store.strategy.bootstrap_partitions() {
            let _ = store.ensure_partition_open(&partition).await?;
        }

        Ok(store)
    }
}

impl<S, R> SqliteEventStoreBuilder<SqldNamespacedBackend<R>, WithStrategy<S>>
where
    S: SqliteSqldNamespacedPartitionStrategy + SqlitePartitionNamingStrategy,
    R: SqliteSqldNamespacedProvisioner,
{
    pub async fn open(self) -> Result<SqliteNamedRemoteStore<S, R>, Error> {
        let store = SqliteEventStore::from_catalog(
            self.strategy.0.clone(),
            NamedTargetCatalog::new(self.strategy.0.clone(), self.backend.provisioner),
        );

        for partition in store.strategy.bootstrap_partitions() {
            let _ = store.ensure_partition_open(&partition).await?;
        }

        Ok(store)
    }
}

impl<S, R> SqliteEventStoreBuilder<TursoBackend<R>, WithStrategy<S>>
where
    S: SqliteSingleRemotePartitionStrategy,
    R: SqliteTursoProvisioner,
{
    pub async fn open(self) -> Result<SqliteSingleRemoteStore<S, R>, Error> {
        let store = SqliteEventStore::from_catalog(
            self.strategy.0.clone(),
            SingleTargetCatalog::new(self.backend.provisioner),
        );

        for partition in store.strategy.bootstrap_partitions() {
            let _ = store.ensure_partition_open(&partition).await?;
        }

        Ok(store)
    }
}

impl<S, C> EventStore for SqliteEventStore<S, C>
where
    S: SqlitePartitionStrategy,
    C: SqlitePartitionCatalog<S::Partition>,
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
