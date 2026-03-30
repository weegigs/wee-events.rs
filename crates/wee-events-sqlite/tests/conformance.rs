//! Runs the conformance test suite against `EventStore`.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::OnceCell;
use tokio::time::sleep;
use wee_events::{
    Aggregate, AggregateId, ChangeSet, EventStore as EventStoreTrait, PublishOptions, RawEvent,
};
use wee_events_sqlite::{
    AggregateStrategy, DatabaseTarget, Error, EventStore, GlobalStrategy, HashedStrategy,
    InMemoryStore, LocalPartitionStrategy, LocalStore, NamedRemoteStore, NamedTargetProvisioner,
    PartitionByStrategy, PartitionCatalog, PartitionNamingStrategy, PartitionStrategy,
    SingleRemotePartitionStrategy, SingleRemoteStore, SingleTargetProvisioner,
    SqldDefaultProvisioner as SqldDefaultProvisionerTrait, SqldNamespacedPartitionStrategy,
    SqldNamespacedProvisioner, TursoProvisioner, TypeStrategy,
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
    sqlite_store_in_memory_partition_by,
    make_in_memory_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
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
    sqlite_store_local_partition_by,
    make_local_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
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
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_partition_by,
    make_remote_sqld_store(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await
);

optional_store_test_suite!(sqlite_store_turso_single, make_turso_store(GlobalStrategy));

async fn make_in_memory_store<S>(strategy: S) -> TempStore<InMemoryStore<S>>
where
    S: PartitionStrategy,
{
    TempStore {
        _guard: TestStoreGuard::None,
        store: EventStore::builder()
            .in_memory()
            .strategy(strategy)
            .open()
            .await
            .unwrap(),
    }
}

async fn make_local_store<S>(strategy: S) -> TempStore<LocalStore<S>>
where
    S: LocalPartitionStrategy + LocalStorePath,
{
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::builder()
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
) -> TempStore<NamedRemoteStore<S, TestSqldNamespaceProvisioner>>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
{
    let instance = shared_sqld_instance().await;
    let provisioner =
        TestSqldNamespaceProvisioner::new(instance.url.clone(), instance.admin_url.clone());

    let store = open_sqld_store_with_retry(strategy, provisioner).await;
    wait_until_remote_store_is_ready(&store).await;

    TempStore {
        _guard: TestStoreGuard::None,
        store,
    }
}

async fn make_remote_sqld_default_store(
    strategy: GlobalStrategy,
) -> TempStore<SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner>> {
    let instance = shared_sqld_instance().await;
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

async fn make_turso_store<S>(
    strategy: S,
) -> TempStore<SingleRemoteStore<S, FixedRemoteTargetProvisioner>>
where
    S: SingleRemotePartitionStrategy,
{
    let url = std::env::var("TURSO_DATABASE_URL").unwrap();
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap();

    let store = EventStore::builder()
        .turso(FixedRemoteTargetProvisioner { url, auth_token })
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
    provisioner: TestSqldDefaultProvisioner,
) -> SingleRemoteStore<GlobalStrategy, TestSqldDefaultProvisioner> {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match EventStore::builder()
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
    provisioner: TestSqldNamespaceProvisioner,
) -> NamedRemoteStore<S, TestSqldNamespaceProvisioner>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
{
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match EventStore::builder()
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

async fn wait_until_remote_store_is_ready<S, C>(store: &EventStore<S, C>)
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
    TempDir { _temp_dir: tempfile::TempDir },
}

struct SharedSqldInstance {
    _container: &'static ContainerAsync<GenericImage>,
    url: String,
    admin_url: String,
}

static SHARED_SQLD: OnceCell<SharedSqldInstance> = OnceCell::const_new();

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

    fn target_for_name(&self, name: Option<&str>) -> DatabaseTarget {
        match name {
            Some(name) => DatabaseTarget::SqldNamespace {
                url: self.url.clone(),
                auth_token: String::new(),
                namespace: format!("partition-{}", sanitize(name)),
            },
            None => DatabaseTarget::SqldDefault {
                url: self.url.clone(),
                auth_token: String::new(),
            },
        }
    }
}

impl NamedTargetProvisioner for TestSqldNamespaceProvisioner {
    async fn ensure_target_for_name(&self, name: Option<&str>) -> Result<DatabaseTarget, Error> {
        let Some(name) = name else {
            return Ok(self.target_for_name(None));
        };

        let namespace = format!("partition-{}", sanitize(name));
        let target = self.target_for_name(Some(name));
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
        name: Option<&str>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let Some(name) = name else {
            return Ok(Some(self.target_for_name(None)));
        };

        Ok(self
            .known_names
            .lock()
            .unwrap()
            .contains(name)
            .then(|| self.target_for_name(Some(name))))
    }

    async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(self.known_names.lock().unwrap().iter().cloned().collect())
    }
}

impl SqldNamespacedProvisioner for TestSqldNamespaceProvisioner {}

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
        .aggregate_key
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
