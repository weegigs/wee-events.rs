//! Runs the conformance test suite against `SqliteEventStore`.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::StatusCode;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;
use tokio::time::sleep;
use wee_events::{Aggregate, AggregateId, ChangeSet, EventStore, PublishOptions, RawEvent};
use wee_events_sqlite::{
    AggregatePartition, AggregateStrategy, BucketPartition, Error, GlobalPartition, GlobalStrategy,
    HashedStrategy, SqliteDatabaseTarget, SqliteEventStore, SqliteInMemoryStore,
    SqliteLocalPartitionStrategy, SqliteLocalStore, SqlitePartitionCatalog,
    SqlitePartitionStrategy, SqliteRemoteStore, SqliteSingleRemotePartitionStrategy,
    SqliteSqldDefaultProvisioner, SqliteSqldNamespacedPartitionStrategy,
    SqliteSqldNamespacedProvisioner, SqliteTargetProvisioner, SqliteTursoProvisioner,
    TypePartition, TypeStrategy,
};

macro_rules! optional_store_test_suite {
    ($mod_name:ident, $make_store:expr) => {
        mod $mod_name {
            use super::*;

            fn turso_env_present() -> bool {
                std::env::var("TURSO_DATABASE_URL").is_ok()
                    && std::env::var("TURSO_AUTH_TOKEN").is_ok()
            }

            #[tokio::test]
            async fn load_initial() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::load_initial(&store).await;
            }

            #[tokio::test]
            async fn loads_revision_with_events() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::loads_revision_with_events(&store).await;
            }

            #[tokio::test]
            async fn publishes_single_event() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_single_event(&store).await;
            }

            #[tokio::test]
            async fn publishes_multiple_events() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_multiple_events(&store).await;
            }

            #[tokio::test]
            async fn validate_event_content() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::validate_event_content(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_initial_revision() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_with_expected_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_revision() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_with_expected_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_initial_revision() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::revision_conflict_on_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_subsequent_revision() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::revision_conflict_on_subsequent_revision(&store).await;
            }

            #[tokio::test]
            async fn causation() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::causation(&store).await;
            }

            #[tokio::test]
            async fn stale_revision_detected_and_retry_succeeds() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::stale_revision_detected_and_retry_succeeds(&store).await;
            }

            #[tokio::test]
            async fn empty_publish_returns_current_revision() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::empty_publish_returns_current_revision(&store).await;
            }

            #[tokio::test]
            async fn event_ordering_preserved() {
                if !turso_env_present() {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::event_ordering_preserved(&store).await;
            }
        }
    };
}

wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_single,
    make_in_memory_store(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_per_type,
    make_in_memory_store(TypeStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_per_aggregate,
    make_in_memory_store(AggregateStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_hashed,
    make_in_memory_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
);

wee_events::testing::store_test_suite!(
    sqlite_store_local_single,
    make_local_store(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_type,
    make_local_store(TypeStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_aggregate,
    make_local_store(AggregateStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_hashed,
    make_local_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
);

wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_default_single,
    make_remote_sqld_default_store(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_single,
    make_remote_sqld_store(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_per_type,
    make_remote_sqld_store(TypeStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_per_aggregate,
    make_remote_sqld_store(AggregateStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_hashed,
    make_remote_sqld_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
);

optional_store_test_suite!(sqlite_store_turso_single, make_turso_store(GlobalStrategy));

async fn make_in_memory_store<S>(strategy: S) -> TempStore<SqliteInMemoryStore<S>>
where
    S: SqlitePartitionStrategy,
{
    TempStore {
        _guard: TestStoreGuard::None,
        store: SqliteEventStore::builder()
            .in_memory()
            .strategy(strategy)
            .open()
            .await
            .unwrap(),
    }
}

async fn make_local_store<S>(strategy: S) -> TempStore<SqliteLocalStore<S>>
where
    S: SqliteLocalPartitionStrategy + LocalStorePath,
{
    let temp_dir = tempfile::tempdir().unwrap();
    let store = SqliteEventStore::builder()
        .local(S::local_store_path(&temp_dir))
        .strategy(strategy)
        .open()
        .await
        .unwrap();

    TempStore {
        _guard: TestStoreGuard::TempDir {
            _temp_dir: temp_dir,
        },
        store,
    }
}

async fn make_remote_sqld_store<S>(
    strategy: S,
) -> TempStore<SqliteRemoteStore<S, SqldNamespaceProvisioner<S::Partition>>>
where
    S: SqliteSqldNamespacedPartitionStrategy,
    S::Partition: PartitionNamespace,
{
    let instance = shared_sqld_instance().await;
    let provisioner = SqldNamespaceProvisioner::<S::Partition>::new(
        instance.url.clone(),
        instance.admin_url.clone(),
    );

    let store = open_sqld_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_remote_sqld_default_store(
    strategy: GlobalStrategy,
) -> TempStore<SqliteRemoteStore<GlobalStrategy, SqldDefaultProvisioner>> {
    let instance = shared_sqld_instance().await;
    let store = open_sqld_default_store_with_retry(
        strategy,
        SqldDefaultProvisioner {
            url: instance.url.clone(),
        },
    )
    .await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_turso_store<S>(
    strategy: S,
) -> TempStore<SqliteRemoteStore<S, FixedRemoteTargetProvisioner<S::Partition>>>
where
    S: SqliteSingleRemotePartitionStrategy,
{
    let url = std::env::var("TURSO_DATABASE_URL").unwrap();
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap();

    let store = SqliteEventStore::builder()
        .turso(FixedRemoteTargetProvisioner::<S::Partition> {
            url,
            auth_token,
            _marker: PhantomData,
        })
        .strategy(strategy)
        .open()
        .await
        .unwrap();

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn open_sqld_default_store_with_retry(
    strategy: GlobalStrategy,
    provisioner: SqldDefaultProvisioner,
) -> SqliteRemoteStore<GlobalStrategy, SqldDefaultProvisioner> {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match SqliteEventStore::builder()
            .sqld_default(provisioner.clone())
            .strategy(strategy)
            .open()
            .await
        {
            Ok(store) => return store,
            Err(error) => {
                if Instant::now() >= deadline {
                    panic!("sqld default did not become ready in time: {error}");
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn open_sqld_store_with_retry<S>(
    strategy: S,
    provisioner: SqldNamespaceProvisioner<S::Partition>,
) -> SqliteRemoteStore<S, SqldNamespaceProvisioner<S::Partition>>
where
    S: SqliteSqldNamespacedPartitionStrategy,
    S::Partition: PartitionNamespace,
{
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match SqliteEventStore::builder()
            .sqld_namespaced(provisioner.clone())
            .strategy(strategy.clone())
            .open()
            .await
        {
            Ok(store) => return store,
            Err(error) => {
                if Instant::now() >= deadline {
                    panic!("sqld did not become ready in time: {error}");
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn wait_until_remote_store_is_ready<S, C>(store: &SqliteEventStore<S, C>)
where
    S: SqlitePartitionStrategy,
    C: SqlitePartitionCatalog<S::Partition>,
{
    let probe = AggregateId::new("health", "probe");
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match store.load(&probe).await {
            Ok(_) => return,
            Err(error) => {
                if Instant::now() >= deadline {
                    panic!("remote sqld did not become ready in time: {error}");
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn shared_sqld_instance() -> &'static SharedSqldInstance {
    SHARED_SQLD
        .get_or_init(|| async {
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

            SharedSqldInstance {
                _container: container,
                url: format!("http://{host}:{port}"),
                admin_url: format!("http://{host}:{admin_port}"),
            }
        })
        .await
}

struct TempStore<T> {
    _guard: TestStoreGuard,
    store: T,
}

impl<T> EventStore for TempStore<T>
where
    T: EventStore,
{
    fn load(
        &self,
        id: &AggregateId,
    ) -> impl std::future::Future<Output = Result<Aggregate, wee_events::Error>> + Send {
        self.store.load(id)
    }

    fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> impl std::future::Future<Output = Result<ChangeSet, wee_events::Error>> + Send {
        self.store.publish(aggregate_id, options, events)
    }
}

enum TestStoreGuard {
    None,
    TempDir { _temp_dir: tempfile::TempDir },
}

struct SharedSqldInstance {
    _container: &'static ContainerAsync<GenericImage>,
    url: String,
    admin_url: String,
}

static SHARED_SQLD: OnceCell<SharedSqldInstance> = OnceCell::const_new();

#[derive(Debug, Clone)]
struct SqldDefaultProvisioner {
    url: String,
}

#[async_trait]
impl SqliteTargetProvisioner<GlobalPartition> for SqldDefaultProvisioner {
    async fn ensure_target_for_partition(
        &self,
        _partition: &GlobalPartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::SqldDefault {
            url: self.url.clone(),
            auth_token: String::new(),
        })
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &GlobalPartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::SqldDefault {
            url: self.url.clone(),
            auth_token: String::new(),
        }))
    }
}

impl SqliteSqldDefaultProvisioner<GlobalPartition> for SqldDefaultProvisioner {}

#[derive(Debug, Clone)]
struct FixedRemoteTargetProvisioner<P> {
    url: String,
    auth_token: String,
    _marker: PhantomData<P>,
}

#[async_trait]
impl<P> SqliteTargetProvisioner<P> for FixedRemoteTargetProvisioner<P>
where
    P: Clone + Send + Sync + 'static,
{
    async fn ensure_target_for_partition(
        &self,
        _partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::Turso {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
        })
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::Turso {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
        }))
    }
}

impl<P> SqliteTursoProvisioner<P> for FixedRemoteTargetProvisioner<P> where
    P: Clone + Send + Sync + 'static
{
}

#[derive(Debug, Clone)]
struct SqldNamespaceProvisioner<P> {
    url: String,
    admin_url: String,
    known_partitions: Arc<Mutex<HashSet<P>>>,
    client: reqwest::Client,
}

impl<P> SqldNamespaceProvisioner<P>
where
    P: PartitionNamespace,
{
    fn new(url: String, admin_url: String) -> Self {
        Self {
            url,
            admin_url,
            known_partitions: Arc::new(Mutex::new(HashSet::new())),
            client: reqwest::Client::new(),
        }
    }

    fn target_for_partition(&self, partition: &P) -> SqliteDatabaseTarget {
        match partition.namespace() {
            Some(namespace) => SqliteDatabaseTarget::SqldNamespace {
                url: self.url.clone(),
                auth_token: String::new(),
                namespace,
            },
            None => SqliteDatabaseTarget::SqldDefault {
                url: self.url.clone(),
                auth_token: String::new(),
            },
        }
    }
}

#[async_trait]
impl<P> SqliteTargetProvisioner<P> for SqldNamespaceProvisioner<P>
where
    P: PartitionNamespace,
{
    async fn ensure_target_for_partition(
        &self,
        partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error> {
        if partition.namespace().is_none() {
            return Ok(self.target_for_partition(partition));
        }

        let namespace = partition
            .namespace()
            .expect("namespace is present because global partitions return early");
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

        self.known_partitions
            .lock()
            .unwrap()
            .insert(partition.clone());
        Ok(self.target_for_partition(partition))
    }

    async fn target_for_existing_partition(
        &self,
        partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        if partition.namespace().is_none() {
            return Ok(Some(self.target_for_partition(partition)));
        }

        Ok(self
            .known_partitions
            .lock()
            .unwrap()
            .contains(partition)
            .then(|| self.target_for_partition(partition)))
    }

    async fn partitions(&self) -> Result<Vec<P>, Error> {
        Ok(self
            .known_partitions
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect())
    }
}

impl<P> SqliteSqldNamespacedProvisioner<P> for SqldNamespaceProvisioner<P> where
    P: PartitionNamespace
{
}

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

trait PartitionNamespace: Clone + Eq + std::hash::Hash + Send + Sync + 'static {
    fn namespace(&self) -> Option<String>;
}

impl PartitionNamespace for GlobalPartition {
    fn namespace(&self) -> Option<String> {
        None
    }
}

impl PartitionNamespace for TypePartition {
    fn namespace(&self) -> Option<String> {
        Some(format!("type-{}", sanitize(self.0.as_str())))
    }
}

impl PartitionNamespace for BucketPartition {
    fn namespace(&self) -> Option<String> {
        Some(format!("bucket-{}", self.0))
    }
}

impl PartitionNamespace for AggregatePartition {
    fn namespace(&self) -> Option<String> {
        Some(format!(
            "agg-{}-{}",
            sanitize(self.0.aggregate_type.as_str()),
            sanitize(&self.0.aggregate_key)
        ))
    }
}

fn sanitize(input: &str) -> String {
    input
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character.to_ascii_lowercase(),
            _ => '-',
        })
        .collect()
}
