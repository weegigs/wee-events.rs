//! Runs the conformance test suite against `SqliteEventStore`.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;
use tokio::time::sleep;
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

macro_rules! optional_store_test_suite {
    ($mod_name:ident, $guard:expr, $make_store:expr) => {
        mod $mod_name {
            use super::*;

            #[tokio::test]
            async fn load_initial() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::load_initial(&store).await;
            }

            #[tokio::test]
            async fn loads_revision_with_events() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::loads_revision_with_events(&store).await;
            }

            #[tokio::test]
            async fn publishes_single_event() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_single_event(&store).await;
            }

            #[tokio::test]
            async fn publishes_multiple_events() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_multiple_events(&store).await;
            }

            #[tokio::test]
            async fn validate_event_content() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::validate_event_content(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_initial_revision() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_with_expected_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_revision() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::publishes_with_expected_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_initial_revision() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::revision_conflict_on_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_subsequent_revision() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::revision_conflict_on_subsequent_revision(&store).await;
            }

            #[tokio::test]
            async fn causation() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::causation(&store).await;
            }

            #[tokio::test]
            async fn stale_revision_detected_and_retry_succeeds() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::stale_revision_detected_and_retry_succeeds(&store).await;
            }

            #[tokio::test]
            async fn empty_publish_returns_current_revision() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::empty_publish_returns_current_revision(&store).await;
            }

            #[tokio::test]
            async fn event_ordering_preserved() {
                if !($guard).await {
                    return;
                }
                let store = $make_store.await;
                wee_events::testing::event_ordering_preserved(&store).await;
            }
        }
    };
}

macro_rules! optional_shared_store_test_suite {
    ($mod_name:ident, $guard:expr, $make_store_pair:expr) => {
        mod $mod_name {
            use super::*;

            #[tokio::test]
            async fn blind_appends_succeed_across_store_instances() {
                if !($guard).await {
                    return;
                }
                let (store_a, store_b) = $make_store_pair.await;
                wee_events::testing::blind_appends_succeed_across_store_instances(
                    &store_a, &store_b,
                )
                .await;
            }

            #[tokio::test]
            async fn stale_revision_conflicts_across_store_instances() {
                if !($guard).await {
                    return;
                }
                let (store_a, store_b) = $make_store_pair.await;
                wee_events::testing::stale_revision_conflicts_across_store_instances(
                    &store_a, &store_b,
                )
                .await;
            }
        }
    };
}

wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_single,
    make_in_memory_store(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_single,
    make_local_store(GlobalStrategy).await
);
wee_events::testing::shared_store_test_suite!(
    sqlite_store_local_single_shared_backing,
    make_local_store_pair(GlobalStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_type,
    make_local_store(TypeStrategy).await
);
wee_events::testing::shared_store_test_suite!(
    sqlite_store_local_per_type_shared_backing,
    make_local_store_pair(TypeStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_aggregate,
    make_local_store(AggregateStrategy).await
);
wee_events::testing::shared_store_test_suite!(
    sqlite_store_local_per_aggregate_shared_backing,
    make_local_store_pair(AggregateStrategy).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_hashed,
    make_local_store(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
);
wee_events::testing::shared_store_test_suite!(
    sqlite_store_local_hashed_shared_backing,
    make_local_store_pair(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_partition_by,
    make_local_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
);
wee_events::testing::shared_store_test_suite!(
    sqlite_store_local_partition_by_shared_backing,
    make_local_store_pair(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
);

optional_store_test_suite!(
    sqlite_store_remote_sqld_default_single,
    sqld_available(),
    make_remote_sqld_default_store(GlobalStrategy)
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_default_single_shared_backing,
    sqld_available(),
    make_remote_sqld_default_store_pair(GlobalStrategy)
);
optional_store_test_suite!(
    sqlite_store_remote_sqld_single,
    sqld_available(),
    make_remote_sqld_store(GlobalStrategy)
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_single_shared_backing,
    sqld_available(),
    make_remote_sqld_store_pair(GlobalStrategy)
);
optional_store_test_suite!(
    sqlite_store_remote_sqld_per_type,
    sqld_available(),
    make_remote_sqld_store(TypeStrategy)
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_per_type_shared_backing,
    sqld_available(),
    make_remote_sqld_store_pair(TypeStrategy)
);
optional_store_test_suite!(
    sqlite_store_remote_sqld_per_aggregate,
    sqld_available(),
    make_remote_sqld_store(AggregateStrategy)
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_per_aggregate_shared_backing,
    sqld_available(),
    make_remote_sqld_store_pair(AggregateStrategy)
);
optional_store_test_suite!(
    sqlite_store_remote_sqld_hashed,
    sqld_available(),
    make_remote_sqld_store(HashedStrategy::new(NonZeroU32::new(8).unwrap()))
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_hashed_shared_backing,
    sqld_available(),
    make_remote_sqld_store_pair(HashedStrategy::new(NonZeroU32::new(8).unwrap()))
);
optional_store_test_suite!(
    sqlite_store_remote_sqld_partition_by,
    sqld_available(),
    make_remote_sqld_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
);
optional_shared_store_test_suite!(
    sqlite_store_remote_sqld_partition_by_shared_backing,
    sqld_available(),
    make_remote_sqld_store_pair(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
);

optional_store_test_suite!(
    sqlite_store_turso_default_partition,
    async { turso_env_present() },
    make_turso_store(GlobalStrategy)
);
optional_shared_store_test_suite!(
    sqlite_store_turso_default_partition_shared_backing,
    async { turso_env_present() },
    make_turso_store_pair(GlobalStrategy)
);

fn turso_env_present() -> bool {
    std::env::var("TURSO_DATABASE_URL").is_ok() && std::env::var("TURSO_AUTH_TOKEN").is_ok()
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
    let temp_dir = Arc::new(tempfile::tempdir().unwrap());
    let store = SqliteEventStore::open_local(S::local_store_path(temp_dir.as_ref()), strategy)
        .await
        .unwrap();

    TempStore {
        _guard: TestStoreGuard::TempDir {
            _temp_dir: temp_dir,
        },
        store,
    }
}

async fn make_local_store_pair<S>(
    strategy: S,
) -> (TempStore<LocalStore<S>>, TempStore<LocalStore<S>>)
where
    S: LocalPartitionStrategy + LocalStorePath + Clone,
{
    let temp_dir = Arc::new(tempfile::tempdir().unwrap());
    let path = S::local_store_path(temp_dir.as_ref());
    let store_a = SqliteEventStore::open_local(&path, strategy.clone())
        .await
        .unwrap();
    let store_b = SqliteEventStore::open_local(&path, strategy).await.unwrap();

    (
        TempStore {
            _guard: TestStoreGuard::TempDir {
                _temp_dir: Arc::clone(&temp_dir),
            },
            store: store_a,
        },
        TempStore {
            _guard: TestStoreGuard::TempDir {
                _temp_dir: temp_dir,
            },
            store: store_b,
        },
    )
}

async fn make_remote_sqld_store<S>(
    strategy: S,
) -> TempStore<NamedRemoteStore<S, TestSqldNamespaceProvisioner>>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
{
    let instance = shared_sqld_instance()
        .await
        .expect("sqld availability should be checked before creating a store");
    let provisioner =
        TestSqldNamespaceProvisioner::new(instance.url.clone(), instance.admin_url.clone());

    let store = open_sqld_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_remote_sqld_store_pair<S>(
    strategy: S,
) -> (
    TempStore<NamedRemoteStore<S, TestSqldNamespaceProvisioner>>,
    TempStore<NamedRemoteStore<S, TestSqldNamespaceProvisioner>>,
)
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy + Clone,
{
    let instance = shared_sqld_instance()
        .await
        .expect("sqld availability should be checked before creating a store");
    let provisioner =
        TestSqldNamespaceProvisioner::new(instance.url.clone(), instance.admin_url.clone());

    let store_a = open_sqld_store_with_retry(strategy.clone(), provisioner.clone()).await;
    let store_b = open_sqld_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store_a).await;
    wait_until_remote_store_is_ready(&store_b).await;

    (
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_a,
        },
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_b,
        },
    )
}

async fn make_remote_sqld_default_store(
    strategy: GlobalStrategy,
) -> TempStore<SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner>> {
    let instance = shared_sqld_instance()
        .await
        .expect("sqld availability should be checked before creating a store");
    let store = open_sqld_default_store_with_retry(
        strategy,
        TestSqldDefaultProvisioner {
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

async fn make_remote_sqld_default_store_pair(
    strategy: GlobalStrategy,
) -> (
    TempStore<SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner>>,
    TempStore<SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner>>,
) {
    let instance = shared_sqld_instance()
        .await
        .expect("sqld availability should be checked before creating a store");
    let provisioner = TestSqldDefaultProvisioner {
        url: instance.url.clone(),
    };

    let store_a = open_sqld_default_store_with_retry(strategy, provisioner.clone()).await;
    let store_b = open_sqld_default_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store_a).await;
    wait_until_remote_store_is_ready(&store_b).await;

    (
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_a,
        },
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_b,
        },
    )
}

async fn make_turso_store<S>(
    strategy: S,
) -> TempStore<NamedRemoteStore<S, FixedRemoteTargetProvisioner>>
where
    S: PartitionNamingStrategy,
{
    let url = std::env::var("TURSO_DATABASE_URL").unwrap();
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap();

    let store =
        SqliteEventStore::open_turso(FixedRemoteTargetProvisioner { url, auth_token }, strategy)
            .await
            .unwrap();

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_turso_store_pair<S>(
    strategy: S,
) -> (
    TempStore<NamedRemoteStore<S, FixedRemoteTargetProvisioner>>,
    TempStore<NamedRemoteStore<S, FixedRemoteTargetProvisioner>>,
)
where
    S: PartitionNamingStrategy + Clone,
{
    let url = std::env::var("TURSO_DATABASE_URL").unwrap();
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap();
    let provisioner = FixedRemoteTargetProvisioner { url, auth_token };

    let store_a = SqliteEventStore::open_turso(provisioner.clone(), strategy.clone())
        .await
        .unwrap();
    let store_b = SqliteEventStore::open_turso(provisioner, strategy)
        .await
        .unwrap();

    (
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_a,
        },
        TempStore {
            _guard: TestStoreGuard::None,
            store: store_b,
        },
    )
}

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
                sleep(Duration::from_millis(250)).await;
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
                sleep(Duration::from_millis(250)).await;
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
                sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

async fn sqld_available() -> bool {
    shared_sqld_instance().await.is_ok()
}

async fn shared_sqld_instance() -> Result<&'static SharedSqldInstance, String> {
    match SHARED_SQLD
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
            let container = Box::new(
                image
                    .start()
                    .await
                    .map_err(|error| format!("sqld container unavailable: {error}"))?,
            );
            let container = Box::leak(container);
            let host = container
                .get_host()
                .await
                .map_err(|error| format!("sqld host lookup failed: {error}"))?
                .to_string();
            let port = container
                .get_host_port_ipv4(8080.tcp())
                .await
                .map_err(|error| format!("sqld port lookup failed: {error}"))?;
            let admin_port = container
                .get_host_port_ipv4(9090.tcp())
                .await
                .map_err(|error| format!("sqld admin port lookup failed: {error}"))?;

            Ok(SharedSqldInstance {
                _container: container,
                url: format!("http://{host}:{port}"),
                admin_url: format!("http://{host}:{admin_port}"),
            })
        })
        .await
    {
        Ok(instance) => Ok(instance),
        Err(error) => Err(error.clone()),
    }
}

struct TempStore<T> {
    _guard: TestStoreGuard,
    store: T,
}

impl<T> EventStoreTrait for TempStore<T>
where
    T: EventStoreTrait,
{
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
    TempDir { _temp_dir: Arc<tempfile::TempDir> },
}

struct SharedSqldInstance {
    _container: &'static ContainerAsync<GenericImage>,
    url: String,
    admin_url: String,
}

static SHARED_SQLD: OnceCell<Result<SharedSqldInstance, String>> = OnceCell::const_new();

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

#[derive(Debug, Clone)]
struct TestSqldNamespaceProvisioner {
    url: String,
    admin_url: String,
    namespace_prefix: String,
    created_namespaces: Arc<Mutex<HashSet<String>>>,
    known_names: Arc<Mutex<HashSet<String>>>,
    client: reqwest::Client,
}

impl TestSqldNamespaceProvisioner {
    fn new(url: String, admin_url: String) -> Self {
        Self {
            url,
            admin_url,
            namespace_prefix: format!("test-{}", ulid::Ulid::new().to_string().to_lowercase()),
            created_namespaces: Arc::new(Mutex::new(HashSet::new())),
            known_names: Arc::new(Mutex::new(HashSet::new())),
            client: reqwest::Client::new(),
        }
    }

    fn namespace_for_name(&self, name: PartitionName<'_>) -> String {
        let suffix = match name {
            PartitionName::Default => "default".to_string(),
            PartitionName::Named(name) => sanitize(name),
        };

        format!("{}-{suffix}", self.namespace_prefix)
    }

    fn target_for_name(&self, name: PartitionName<'_>) -> DatabaseTarget {
        DatabaseTarget::SqldNamespace {
            url: self.url.clone(),
            auth_token: String::new(),
            namespace: self.namespace_for_name(name),
        }
    }
}

impl NamedTargetProvisioner for TestSqldNamespaceProvisioner {
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        let namespace = self.namespace_for_name(name);
        let target = self.target_for_name(name);
        if self.created_namespaces.lock().unwrap().contains(&namespace) {
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

        self.created_namespaces
            .lock()
            .unwrap()
            .insert(namespace.clone());
        if let PartitionName::Named(name) = name {
            self.known_names.lock().unwrap().insert(name.to_string());
        }
        Ok(target)
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        Ok(self
            .created_namespaces
            .lock()
            .unwrap()
            .contains(&self.namespace_for_name(name))
            .then(|| self.target_for_name(name)))
    }

    async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(self.known_names.lock().unwrap().iter().cloned().collect())
    }
}

impl SqldNamespacedProvisioner for TestSqldNamespaceProvisioner {}

#[test]
fn sqld_namespace_provisioners_use_unique_namespaces() {
    let first = TestSqldNamespaceProvisioner::new(
        "http://localhost:8080".to_string(),
        "http://localhost:9090".to_string(),
    );
    let second = TestSqldNamespaceProvisioner::new(
        "http://localhost:8080".to_string(),
        "http://localhost:9090".to_string(),
    );

    assert_ne!(
        first.target_for_name(PartitionName::Named("orders")),
        second.target_for_name(PartitionName::Named("orders"))
    );
    assert_ne!(
        first.target_for_name(PartitionName::Default),
        second.target_for_name(PartitionName::Default)
    );
}

#[test]
fn cloned_sqld_namespace_provisioners_share_namespaces() {
    let first = TestSqldNamespaceProvisioner::new(
        "http://localhost:8080".to_string(),
        "http://localhost:9090".to_string(),
    );
    let second = first.clone();

    assert_eq!(
        first.target_for_name(PartitionName::Named("orders")),
        second.target_for_name(PartitionName::Named("orders"))
    );
    assert_eq!(
        first.target_for_name(PartitionName::Default),
        second.target_for_name(PartitionName::Default)
    );
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

impl LocalStorePath for PartitionByStrategy<fn(&AggregateId) -> String> {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
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
// Turso Platform API integration tests (env-gated)
// ===========================================================================

#[cfg(feature = "turso")]
mod turso_platform_integration {
    use super::*;
    use wee_events_sqlite::{TursoPlatformConfig, TursoPlatformProvisioner};

    fn turso_platform_env_present() -> bool {
        std::env::var("TURSO_ORG").is_ok()
            && std::env::var("TURSO_API_TOKEN").is_ok()
            && std::env::var("TURSO_GROUP_TOKEN").is_ok()
    }

    async fn turso_platform_available() -> bool {
        if !turso_platform_env_present() {
            return false;
        }

        let Ok(config) = TursoPlatformConfig::from_env() else {
            return false;
        };

        TursoPlatformProvisioner::new(config).names().await.is_ok()
    }

    /// Creates a provisioner with a strategy-specific prefix so parallel
    /// test suites don't interfere with each other's cleanup.
    fn make_config(strategy_suffix: &str) -> TursoPlatformConfig {
        let mut config = TursoPlatformConfig::from_env().expect("turso platform config from env");
        let suffix: String = strategy_suffix
            .chars()
            .map(|c| if c == '_' { '-' } else { c })
            .collect();
        config.prefix = format!("{}-{suffix}", config.prefix);
        config
    }

    /// Runs all conformance tests for a strategy sequentially, with cleanup
    /// before and after. Each strategy gets its own database prefix to avoid
    /// cross-suite interference when tests run in parallel.
    macro_rules! turso_platform_test_suite {
        ($mod_name:ident, $strategy:expr) => {
            mod $mod_name {
                use super::*;

                #[tokio::test]
                async fn conformance() {
                    if !turso_platform_available().await {
                        return;
                    }

                    let config = make_config(stringify!($mod_name));
                    let provisioner = TursoPlatformProvisioner::new(config.clone());

                    // Clean up leftover databases from previous runs
                    provisioner.cleanup().await.expect("pre-test cleanup");

                    let store = SqliteEventStore::open_turso(
                        TursoPlatformProvisioner::new(config.clone()),
                        $strategy,
                    )
                    .await
                    .unwrap();

                    // Run all conformance tests sequentially
                    wee_events::testing::load_initial(&store).await;
                    wee_events::testing::loads_revision_with_events(&store).await;
                    wee_events::testing::publishes_single_event(&store).await;
                    wee_events::testing::publishes_multiple_events(&store).await;
                    wee_events::testing::validate_event_content(&store).await;
                    wee_events::testing::publishes_with_expected_initial_revision(&store).await;
                    wee_events::testing::publishes_with_expected_revision(&store).await;
                    wee_events::testing::revision_conflict_on_initial_revision(&store).await;
                    wee_events::testing::revision_conflict_on_subsequent_revision(&store).await;
                    wee_events::testing::causation(&store).await;
                    wee_events::testing::stale_revision_detected_and_retry_succeeds(&store).await;
                    wee_events::testing::empty_publish_returns_current_revision(&store).await;
                    wee_events::testing::event_ordering_preserved(&store).await;

                    let store_a = SqliteEventStore::open_turso(
                        TursoPlatformProvisioner::new(config.clone()),
                        $strategy,
                    )
                    .await
                    .unwrap();
                    let store_b = SqliteEventStore::open_turso(
                        TursoPlatformProvisioner::new(config.clone()),
                        $strategy,
                    )
                    .await
                    .unwrap();
                    wee_events::testing::blind_appends_succeed_across_store_instances(
                        &store_a, &store_b,
                    )
                    .await;
                    wee_events::testing::stale_revision_conflicts_across_store_instances(
                        &store_a, &store_b,
                    )
                    .await;

                    // Clean up databases created during this test
                    let cleanup = TursoPlatformProvisioner::new(config);
                    cleanup.cleanup().await.expect("post-test cleanup");
                }
            }
        };
    }

    turso_platform_test_suite!(tp_global, GlobalStrategy);
    turso_platform_test_suite!(tp_type, TypeStrategy);
    turso_platform_test_suite!(tp_agg, AggregateStrategy);
    turso_platform_test_suite!(tp_hash, HashedStrategy::new(NonZeroU32::new(8).unwrap()));
    turso_platform_test_suite!(
        tp_part,
        PartitionByStrategy::new(partition_by_user as fn(&AggregateId) -> String)
    );
}
