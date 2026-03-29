//! Runs the conformance test suite against `SqliteEventStore`.

use std::collections::HashSet;
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
    Error, SqliteBackend, SqliteDatabaseTarget, SqliteEventStore, SqlitePartition,
    SqlitePartitioning, SqlitePartitioningStrategy, SqliteTargetProvisioner,
};

#[derive(Debug, Clone)]
struct FixedRemoteTargetProvisioner {
    url: String,
    auth_token: String,
}

#[async_trait]
impl SqliteTargetProvisioner for FixedRemoteTargetProvisioner {
    async fn ensure_target_for_partition(
        &self,
        _partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::Remote {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
            namespace: None,
        })
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::Remote {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
            namespace: None,
        }))
    }
}

#[derive(Debug, Clone)]
struct SqldNamespaceProvisioner {
    url: String,
    admin_url: String,
    known_partitions: Arc<Mutex<HashSet<SqlitePartition>>>,
    client: reqwest::Client,
}

impl SqldNamespaceProvisioner {
    fn new(url: String, admin_url: String) -> Self {
        Self {
            url,
            admin_url,
            known_partitions: Arc::new(Mutex::new(HashSet::new())),
            client: reqwest::Client::new(),
        }
    }

    fn target_for_partition(&self, partition: &SqlitePartition) -> SqliteDatabaseTarget {
        let namespace = match partition {
            SqlitePartition::Single => None,
            _ => Some(namespace_for_partition(partition)),
        };

        SqliteDatabaseTarget::Remote {
            url: self.url.clone(),
            auth_token: String::new(),
            namespace,
        }
    }
}

#[async_trait]
impl SqliteTargetProvisioner for SqldNamespaceProvisioner {
    async fn ensure_target_for_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        if matches!(partition, SqlitePartition::Single) {
            return Ok(self.target_for_partition(partition));
        }

        let namespace = namespace_for_partition(partition);
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
        partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        if matches!(partition, SqlitePartition::Single) {
            return Ok(Some(self.target_for_partition(partition)));
        }

        Ok(self
            .known_partitions
            .lock()
            .unwrap()
            .contains(partition)
            .then(|| self.target_for_partition(partition)))
    }

    async fn partitions(&self) -> Result<Vec<SqlitePartition>, Error> {
        Ok(self
            .known_partitions
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect())
    }
}

fn namespace_for_partition(partition: &SqlitePartition) -> String {
    fn sanitize(input: &str) -> String {
        input
            .chars()
            .map(|character| match character {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character.to_ascii_lowercase(),
                _ => '-',
            })
            .collect()
    }

    match partition {
        SqlitePartition::Single => "default".to_string(),
        SqlitePartition::AggregateType(aggregate_type) => {
            format!("type-{}", sanitize(aggregate_type.as_str()))
        }
        SqlitePartition::Hashed(bucket) => format!("bucket-{bucket}"),
        SqlitePartition::Aggregate(aggregate_id) => format!(
            "agg-{}-{}",
            sanitize(aggregate_id.aggregate_type.as_str()),
            sanitize(&aggregate_id.aggregate_key)
        ),
    }
}

enum TestStoreKind {
    InMemory,
    Local,
    RemoteSqld,
    TursoCloud,
}

enum TestStoreGuard {
    None,
    TempDir { _temp_dir: tempfile::TempDir },
}

struct TempStore {
    _guard: TestStoreGuard,
    store: SqliteEventStore,
}

impl EventStore for TempStore {
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

async fn wait_until_remote_store_is_ready(store: &SqliteEventStore) {
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

async fn open_sqld_store_with_retry(
    partitioning: SqlitePartitioning,
    provisioner: SqldNamespaceProvisioner,
) -> SqliteEventStore {
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        match SqliteEventStore::open_backend(
            SqliteBackend::remote(provisioner.clone()),
            partitioning,
        )
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

struct SharedSqldInstance {
    _container: &'static ContainerAsync<GenericImage>,
    url: String,
    admin_url: String,
}

static SHARED_SQLD: OnceCell<SharedSqldInstance> = OnceCell::const_new();

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

async fn make_store(kind: TestStoreKind, partitioning: SqlitePartitioningStrategy) -> TempStore {
    match kind {
        TestStoreKind::InMemory => TempStore {
            _guard: TestStoreGuard::None,
            store: SqliteEventStore::open_backend(
                SqliteBackend::in_memory(),
                SqlitePartitioning::Strict(partitioning),
            )
            .await
            .unwrap(),
        },
        TestStoreKind::Local => {
            let temp_dir = tempfile::tempdir().unwrap();
            let backend = match partitioning {
                SqlitePartitioningStrategy::Global => {
                    SqliteBackend::local(temp_dir.path().join("store.db"))
                }
                _ => SqliteBackend::local(temp_dir.path()),
            };
            let store =
                SqliteEventStore::open_backend(backend, SqlitePartitioning::Strict(partitioning))
                    .await
                    .unwrap();

            TempStore {
                _guard: TestStoreGuard::TempDir {
                    _temp_dir: temp_dir,
                },
                store,
            }
        }
        TestStoreKind::RemoteSqld => {
            let instance = shared_sqld_instance().await;
            let provisioner =
                SqldNamespaceProvisioner::new(instance.url.clone(), instance.admin_url.clone());

            let store =
                open_sqld_store_with_retry(SqlitePartitioning::Strict(partitioning), provisioner)
                    .await;

            wait_until_remote_store_is_ready(&store).await;

            TempStore {
                _guard: TestStoreGuard::None,
                store,
            }
        }
        TestStoreKind::TursoCloud => {
            let url = std::env::var("TURSO_DATABASE_URL").unwrap();
            let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap();

            let store = SqliteEventStore::open_backend(
                SqliteBackend::remote(FixedRemoteTargetProvisioner { url, auth_token }),
                SqlitePartitioning::Strict(partitioning),
            )
            .await
            .unwrap();

            TempStore {
                _guard: TestStoreGuard::None,
                store,
            }
        }
    }
}

wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_single,
    make_store(TestStoreKind::InMemory, SqlitePartitioningStrategy::Global).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_per_type,
    make_store(TestStoreKind::InMemory, SqlitePartitioningStrategy::Type).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_per_aggregate,
    make_store(
        TestStoreKind::InMemory,
        SqlitePartitioningStrategy::Aggregate
    )
    .await
);
wee_events::testing::store_test_suite!(
    sqlite_store_in_memory_hashed,
    make_store(
        TestStoreKind::InMemory,
        SqlitePartitioningStrategy::Hashed { buckets: 8 }
    )
    .await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_single,
    make_store(TestStoreKind::Local, SqlitePartitioningStrategy::Global).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_type,
    make_store(TestStoreKind::Local, SqlitePartitioningStrategy::Type).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_per_aggregate,
    make_store(TestStoreKind::Local, SqlitePartitioningStrategy::Aggregate).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_local_hashed,
    make_store(
        TestStoreKind::Local,
        SqlitePartitioningStrategy::Hashed { buckets: 8 }
    )
    .await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_single,
    make_store(
        TestStoreKind::RemoteSqld,
        SqlitePartitioningStrategy::Global
    )
    .await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_per_type,
    make_store(TestStoreKind::RemoteSqld, SqlitePartitioningStrategy::Type).await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_per_aggregate,
    make_store(
        TestStoreKind::RemoteSqld,
        SqlitePartitioningStrategy::Aggregate
    )
    .await
);
wee_events::testing::store_test_suite!(
    sqlite_store_remote_sqld_hashed,
    make_store(
        TestStoreKind::RemoteSqld,
        SqlitePartitioningStrategy::Hashed { buckets: 8 }
    )
    .await
);

macro_rules! optional_store_test_suite {
    ($mod_name:ident, $kind:expr, $partitioning:expr) => {
        mod $mod_name {
            use super::*;

            fn turso_env_present() -> bool {
                std::env::var("TURSO_DATABASE_URL").is_ok()
                    && std::env::var("TURSO_AUTH_TOKEN").is_ok()
            }

            async fn maybe_store() -> Option<TempStore> {
                if !turso_env_present() {
                    return None;
                }

                Some(make_store($kind, $partitioning).await)
            }

            #[tokio::test]
            async fn load_initial() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::load_initial(&store).await;
            }

            #[tokio::test]
            async fn loads_revision_with_events() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::loads_revision_with_events(&store).await;
            }

            #[tokio::test]
            async fn publishes_single_event() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::publishes_single_event(&store).await;
            }

            #[tokio::test]
            async fn publishes_multiple_events() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::publishes_multiple_events(&store).await;
            }

            #[tokio::test]
            async fn validate_event_content() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::validate_event_content(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_initial_revision() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::publishes_with_expected_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_revision() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::publishes_with_expected_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_initial_revision() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::revision_conflict_on_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_subsequent_revision() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::revision_conflict_on_subsequent_revision(&store).await;
            }

            #[tokio::test]
            async fn causation() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::causation(&store).await;
            }

            #[tokio::test]
            async fn stale_revision_detected_and_retry_succeeds() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::stale_revision_detected_and_retry_succeeds(&store).await;
            }

            #[tokio::test]
            async fn empty_publish_returns_current_revision() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::empty_publish_returns_current_revision(&store).await;
            }

            #[tokio::test]
            async fn event_ordering_preserved() {
                let Some(store) = maybe_store().await else {
                    return;
                };
                wee_events::testing::event_ordering_preserved(&store).await;
            }
        }
    };
}

optional_store_test_suite!(
    sqlite_store_turso_single,
    TestStoreKind::TursoCloud,
    SqlitePartitioningStrategy::Global
);
optional_store_test_suite!(
    sqlite_store_turso_per_type,
    TestStoreKind::TursoCloud,
    SqlitePartitioningStrategy::Type
);
optional_store_test_suite!(
    sqlite_store_turso_per_aggregate,
    TestStoreKind::TursoCloud,
    SqlitePartitioningStrategy::Aggregate
);
optional_store_test_suite!(
    sqlite_store_turso_hashed,
    TestStoreKind::TursoCloud,
    SqlitePartitioningStrategy::Hashed { buckets: 8 }
);
