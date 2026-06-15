//! Comprehensive performance benchmarks for all `SQLite` store variants and
//! partitioning strategies.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_main};
use reqwest::StatusCode;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use wee_events::{
    Aggregate, AggregateId, ChangeSet, EventStore as EventStoreTrait, PublishOptions, RawEvent,
};
use wee_events_sqlite::{
    AggregateStrategy, DatabaseTarget, Error, GlobalStrategy, HashedStrategy, InMemoryStore,
    LocalPartitionStrategy, LocalStore, NamedRemoteStore, NamedTargetProvisioner,
    PartitionByStrategy, PartitionCatalog, PartitionName, PartitionNamingStrategy,
    PartitionStrategy, SingleRemoteStore, SingleTargetPartitionStrategy, SingleTargetProvisioner,
    SqldDefaultProvisioner as SqldDefaultProvisionerTrait, SqldNamespacedPartitionStrategy,
    SqldNamespacedProvisioner, SqliteEventStore, TursoProvisioner, TypeStrategy,
};

// ---------------------------------------------------------------------------
// Store wrapper (mirrors conformance.rs)
// ---------------------------------------------------------------------------

struct TempStore<T> {
    _guard: TestStoreGuard,
    store: T,
}

impl<T: EventStoreTrait> EventStoreTrait for TempStore<T> {
    async fn load(&self, id: &AggregateId) -> Result<Aggregate, wee_events::Error> {
        self.store.load(id).await
    }

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, wee_events::Error> {
        self.store.publish(aggregate_id, options, events).await
    }
}

enum TestStoreGuard {
    None,
    TempDir { _temp_dir: tempfile::TempDir },
}

// ---------------------------------------------------------------------------
// Local store helpers
// ---------------------------------------------------------------------------

trait LocalStorePath {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf;
}

impl LocalStorePath for GlobalStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().join("store.db")
    }
}

impl LocalStorePath for TypeStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for AggregateStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for HashedStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for PartitionByStrategy<fn(&AggregateId) -> String> {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

async fn make_in_memory_store<S>(strategy: S) -> TempStore<InMemoryStore<S>>
where
    S: SingleTargetPartitionStrategy,
{
    TempStore {
        _guard: TestStoreGuard::None,
        store: SqliteEventStore::open_in_memory(strategy).await.unwrap(),
    }
}

async fn make_local_store<S>(strategy: S) -> TempStore<LocalStore<S>>
where
    S: LocalPartitionStrategy + LocalStorePath,
{
    let temp_dir = tempfile::tempdir().unwrap();
    let store = SqliteEventStore::open_local(S::local_store_path(&temp_dir), strategy)
        .await
        .unwrap();

    TempStore {
        _guard: TestStoreGuard::TempDir {
            _temp_dir: temp_dir,
        },
        store,
    }
}

fn partition_by_user(aggregate_id: &AggregateId) -> String {
    aggregate_id
        .aggregate_key()
        .split(':')
        .next()
        .expect("split always yields at least one segment")
        .to_string()
}

// ---------------------------------------------------------------------------
// Remote sqld helpers
// ---------------------------------------------------------------------------

struct SharedSqldInstance {
    _container: &'static ContainerAsync<GenericImage>,
    url: String,
    admin_url: String,
}

// Safety: SharedSqldInstance is only written once via OnceLock.
unsafe impl Sync for SharedSqldInstance {}

static SHARED_SQLD: OnceLock<SharedSqldInstance> = OnceLock::new();

async fn ensure_shared_sqld() -> &'static SharedSqldInstance {
    if let Some(instance) = SHARED_SQLD.get() {
        return instance;
    }

    let image = GenericImage::new("ghcr.io/tursodatabase/libsql-server", "latest")
        .with_exposed_port(8080.tcp())
        .with_exposed_port(9090.tcp())
        .with_wait_for(WaitFor::seconds(2))
        .with_startup_timeout(Duration::from_secs(30))
        .with_env_var("SQLD_NODE", "primary")
        .with_cmd([
            "/bin/sqld",
            "--admin-listen-addr",
            "0.0.0.0:9090",
            "--enable-namespaces",
        ]);
    let container = Box::new(image.start().await.unwrap());
    let container = Box::leak(container);
    let host = container.get_host().await.unwrap().to_string();
    let port = container.get_host_port_ipv4(8080.tcp()).await.unwrap();
    let admin_port = container.get_host_port_ipv4(9090.tcp()).await.unwrap();

    let instance = SharedSqldInstance {
        _container: container,
        url: format!("http://{host}:{port}"),
        admin_url: format!("http://{host}:{admin_port}"),
    };

    let _ = SHARED_SQLD.set(instance);
    SHARED_SQLD.get().unwrap()
}

#[derive(Debug, Clone)]
struct TestSqldDefaultProvisioner {
    url: String,
}

impl SingleTargetProvisioner for TestSqldDefaultProvisioner {
    async fn ensure_target(&self) -> Result<DatabaseTarget, Error> {
        Ok(DatabaseTarget::SqldDefault {
            url: self.url.clone(),
            auth_token: String::new(),
        })
    }

    async fn existing_target(&self) -> Result<Option<DatabaseTarget>, Error> {
        Ok(Some(DatabaseTarget::SqldDefault {
            url: self.url.clone(),
            auth_token: String::new(),
        }))
    }
}

impl SqldDefaultProvisionerTrait for TestSqldDefaultProvisioner {}

#[derive(Debug, Clone)]
struct TestSqldNamespaceProvisioner {
    url: String,
    admin_url: String,
    known_names: Arc<Mutex<HashSet<String>>>,
    client: reqwest::Client,
}

impl TestSqldNamespaceProvisioner {
    fn new(url: String, admin_url: String) -> Self {
        Self {
            url,
            admin_url,
            known_names: Arc::new(Mutex::new(HashSet::new())),
            client: reqwest::Client::new(),
        }
    }

    fn target_for_name(&self, name: PartitionName<'_>) -> DatabaseTarget {
        match name {
            PartitionName::Named(name) => DatabaseTarget::SqldNamespace {
                url: self.url.clone(),
                auth_token: String::new(),
                namespace: format!("bench-{}", sanitize(name)),
            },
            PartitionName::Default => DatabaseTarget::SqldDefault {
                url: self.url.clone(),
                auth_token: String::new(),
            },
        }
    }
}

impl NamedTargetProvisioner for TestSqldNamespaceProvisioner {
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        let PartitionName::Named(name) = name else {
            return Ok(self.target_for_name(PartitionName::Default));
        };

        let namespace = format!("bench-{}", sanitize(name));
        let target = self.target_for_name(PartitionName::Named(name));
        if self.known_names.lock().unwrap().contains(name) {
            return Ok(target);
        }

        let response = self
            .client
            .post(format!(
                "{}/v1/namespaces/{namespace}/create",
                self.admin_url
            ))
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|error| Error::Internal(format!("sqld admin create failed: {error}")))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable body>".to_string());
        let already_exists = status == StatusCode::BAD_REQUEST && body.contains("already exists");

        if !matches!(
            status,
            StatusCode::OK | StatusCode::CREATED | StatusCode::CONFLICT
        ) && !already_exists
        {
            return Err(Error::Internal(format!(
                "sqld admin create namespace failed with {status}: {body}"
            )));
        }

        self.known_names.lock().unwrap().insert(name.to_string());
        Ok(target)
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let PartitionName::Named(name) = name else {
            return Ok(Some(self.target_for_name(PartitionName::Default)));
        };

        Ok(self
            .known_names
            .lock()
            .unwrap()
            .contains(name)
            .then(|| self.target_for_name(PartitionName::Named(name))))
    }

    async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(self.known_names.lock().unwrap().iter().cloned().collect())
    }
}

impl SqldNamespacedProvisioner for TestSqldNamespaceProvisioner {}

async fn open_sqld_default_store_with_retry(
    strategy: GlobalStrategy,
    provisioner: TestSqldDefaultProvisioner,
) -> SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner> {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match SqliteEventStore::open_sqld_default(provisioner.clone(), strategy).await {
            Ok(store) => return store,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "sqld default did not become ready in time: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn open_sqld_store_with_retry<S>(
    strategy: S,
    provisioner: TestSqldNamespaceProvisioner,
) -> NamedRemoteStore<S, TestSqldNamespaceProvisioner>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
{
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match SqliteEventStore::open_sqld_namespaced(provisioner.clone(), strategy.clone()).await {
            Ok(store) => return store,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "sqld did not become ready in time: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn wait_until_remote_store_is_ready<S, C>(store: &SqliteEventStore<S, C>)
where
    S: PartitionStrategy,
    C: PartitionCatalog<S::Partition>,
{
    let probe = AggregateId::new("health", "probe");
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match store.load(&probe).await {
            Ok(_) => return,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "remote sqld did not become ready in time: {error}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn make_remote_sqld_default_store(
    strategy: GlobalStrategy,
) -> TempStore<SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner>> {
    let instance = ensure_shared_sqld().await;
    let provisioner = TestSqldDefaultProvisioner {
        url: instance.url.clone(),
    };

    let store = open_sqld_default_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_remote_sqld_store<S>(
    strategy: S,
) -> TempStore<NamedRemoteStore<S, TestSqldNamespaceProvisioner>>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
{
    let instance = ensure_shared_sqld().await;
    let provisioner =
        TestSqldNamespaceProvisioner::new(instance.url.clone(), instance.admin_url.clone());

    let store = open_sqld_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

// ---------------------------------------------------------------------------
// Turso helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FixedRemoteTargetProvisioner {
    url: String,
    auth_token: String,
}

impl NamedTargetProvisioner for FixedRemoteTargetProvisioner {
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        match name {
            PartitionName::Default => Ok(DatabaseTarget::Turso {
                url: self.url.clone(),
                auth_token: self.auth_token.clone(),
            }),
            PartitionName::Named(name) => Err(Error::Configuration(format!(
                "test Turso provisioner does not support named partitions: {name}"
            ))),
        }
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        match name {
            PartitionName::Default => Ok(Some(DatabaseTarget::Turso {
                url: self.url.clone(),
                auth_token: self.auth_token.clone(),
            })),
            PartitionName::Named(name) => Err(Error::Configuration(format!(
                "test Turso provisioner does not support named partitions: {name}"
            ))),
        }
    }
}

impl SingleTargetProvisioner for FixedRemoteTargetProvisioner {
    async fn ensure_target(&self) -> Result<DatabaseTarget, Error> {
        Ok(DatabaseTarget::Turso {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
        })
    }

    async fn existing_target(&self) -> Result<Option<DatabaseTarget>, Error> {
        Ok(Some(DatabaseTarget::Turso {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
        }))
    }
}

impl TursoProvisioner for FixedRemoteTargetProvisioner {}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character.to_ascii_lowercase(),
            _ => '-',
        })
        .collect()
}

// ===========================================================================
// In-memory benchmarks
// ===========================================================================

wee_events::testing::store_bench_suite!(in_memory_global, {
    make_in_memory_store(GlobalStrategy).await
});

// ===========================================================================
// Local filesystem benchmarks — all partition strategies
// ===========================================================================

wee_events::testing::store_bench_suite!(local_global, make_local_store(GlobalStrategy).await);

wee_events::testing::store_bench_suite!(local_by_type, make_local_store(TypeStrategy).await);

wee_events::testing::store_bench_suite!(local_by_aggregate, {
    make_local_store(AggregateStrategy).await
});

wee_events::testing::store_bench_suite!(local_hashed, {
    make_local_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
});

wee_events::testing::store_bench_suite!(local_partition_by, {
    make_local_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
});

// ===========================================================================
// Remote sqld benchmarks — default provisioner + all namespaced strategies
// ===========================================================================

wee_events::testing::store_bench_suite!(sqld_default_global, {
    make_remote_sqld_default_store(GlobalStrategy).await
});

wee_events::testing::store_bench_suite!(sqld_global, {
    make_remote_sqld_store(GlobalStrategy).await
});

wee_events::testing::store_bench_suite!(sqld_by_type, {
    make_remote_sqld_store(TypeStrategy).await
});

// These strategies create a new namespace per aggregate/partition on sqld,
// so we limit concurrency to avoid overwhelming the admin API.
const REMOTE_PARTITIONED_LEVELS: &[usize] = &[2, 4, 8, 16];

wee_events::testing::store_bench_suite!(sqld_by_aggregate, &REMOTE_PARTITIONED_LEVELS, {
    make_remote_sqld_store(AggregateStrategy).await
});

wee_events::testing::store_bench_suite!(sqld_hashed, {
    make_remote_sqld_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
});

wee_events::testing::store_bench_suite!(sqld_partition_by, &REMOTE_PARTITIONED_LEVELS, {
    make_remote_sqld_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
});

// ===========================================================================
// Turso benchmarks (optional — requires TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)
// ===========================================================================

/// Turso benchmarks are registered as a group but skip gracefully when env
/// vars are absent — criterion simply reports zero samples.
fn turso_benchmarks(c: &mut Criterion) {
    let Some(url) = std::env::var("TURSO_DATABASE_URL").ok() else {
        eprintln!("TURSO_DATABASE_URL not set — skipping Turso benchmarks");
        return;
    };
    let Some(auth_token) = std::env::var("TURSO_AUTH_TOKEN").ok() else {
        eprintln!("TURSO_AUTH_TOKEN not set — skipping Turso benchmarks");
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = rt.block_on(async {
        let store = SqliteEventStore::open_turso(
            FixedRemoteTargetProvisioner { url, auth_token },
            GlobalStrategy,
        )
        .await
        .unwrap();
        TempStore {
            _guard: TestStoreGuard::None,
            store,
        }
    });

    let store_arc = std::sync::Arc::new(store);
    let store_ref = &*store_arc;
    let prefix = "turso_global";
    let levels = wee_events::testing::CONCURRENCY_LEVELS;

    // Creation
    wee_events::testing::bench_create_aggregate(c, &rt, store_ref, prefix);
    wee_events::testing::bench_create_spread(c, &rt, &store_arc, prefix, levels);
    wee_events::testing::bench_create_concentrated(c, &rt, &store_arc, prefix, levels);

    // Steady-state writes
    wee_events::testing::bench_publish_batch(c, &rt, store_ref, prefix);
    wee_events::testing::bench_publish_with_revision(c, &rt, store_ref, prefix);
    wee_events::testing::bench_publish_append(c, &rt, store_ref, prefix);

    // Load scaling
    wee_events::testing::bench_load_scaling(c, &rt, store_ref, prefix);

    // Partition patterns
    wee_events::testing::bench_write_spread(c, &rt, &store_arc, prefix, levels);
    wee_events::testing::bench_write_concentrated(c, &rt, &store_arc, prefix, levels);
    wee_events::testing::bench_write_contention(c, &rt, &store_arc, prefix, levels);
    wee_events::testing::bench_read_spread(c, &rt, &store_arc, prefix, levels);
    wee_events::testing::bench_read_concentrated(c, &rt, &store_arc, prefix, levels);

    // Mixed
    wee_events::testing::bench_mixed_read_write(c, &rt, &store_arc, prefix, levels);
}

criterion::criterion_group!(turso, turso_benchmarks);

// ===========================================================================
// Entry point — all groups
// ===========================================================================

criterion_main!(
    // In-memory
    in_memory_global::benches,
    // Local filesystem — all partition strategies
    local_global::benches,
    local_by_type::benches,
    local_by_aggregate::benches,
    local_hashed::benches,
    local_partition_by::benches,
    // Remote sqld — default + all namespaced partition strategies
    sqld_default_global::benches,
    sqld_global::benches,
    sqld_by_type::benches,
    sqld_by_aggregate::benches,
    sqld_hashed::benches,
    sqld_partition_by::benches,
    // Turso (optional)
    turso,
);
