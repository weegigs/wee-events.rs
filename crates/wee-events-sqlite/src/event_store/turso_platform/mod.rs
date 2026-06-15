mod api;
mod sanitize;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Mutex;

use futures_util::future::try_join_all;

use crate::Error;
use crate::database;
use api::{ApiError, DatabaseInfo, TursoPlatformApi};
use sanitize::{named_database_prefix, sanitize_database_name};

use super::strategies::PartitionName;
use super::types::{DatabaseTarget, NamedTargetProvisioner, TursoProvisioner};

trait PartitionMetadataStore: Send + Sync {
    fn ensure_partition_name(
        &self,
        target: &DatabaseTarget,
        logical_name: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn load_partition_name(
        &self,
        target: &DatabaseTarget,
    ) -> impl Future<Output = Result<Option<String>, Error>> + Send;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LibsqlPartitionMetadataStore;

impl PartitionMetadataStore for LibsqlPartitionMetadataStore {
    async fn ensure_partition_name(
        &self,
        target: &DatabaseTarget,
        logical_name: &str,
    ) -> Result<(), Error> {
        let conn = database::open_event_store_connection(target).await?;
        database::ensure_partition_name(&conn, logical_name).await
    }

    async fn load_partition_name(&self, target: &DatabaseTarget) -> Result<Option<String>, Error> {
        let conn = database::open_event_store_connection(target).await?;
        database::load_partition_name(&conn).await
    }
}

/// Configuration for connecting to the Turso Platform API.
///
/// Use [`from_env`](Self::from_env) to load from standard environment variables,
/// or construct directly with named fields.
///
/// # Warning
///
/// Changing `prefix` after databases have been created will make existing
/// partitions invisible to [`TursoPlatformProvisioner`]. The provisioner
/// identifies its databases by prefix — orphaned databases must be cleaned
/// up manually via the Turso Platform API.
#[derive(Debug, Clone)]
pub struct TursoPlatformConfig {
    /// Turso organization slug.
    pub org: String,
    /// Database group name. Determines the regions databases are placed in.
    pub group: String,
    /// Prefix for database names. Each partition becomes `{prefix}-{sanitized_name}`.
    pub prefix: String,
    /// Platform API token for managing databases.
    pub api_token: String,
    /// Group-level auth token for connecting to databases. Create with
    /// `turso group tokens create <group>`.
    pub group_token: String,
    /// Override the Platform API base URL. Defaults to `https://api.turso.tech`.
    pub base_url: Option<String>,
}

impl TursoPlatformConfig {
    const DEFAULT_BASE_URL: &str = "https://api.turso.tech";

    /// Load configuration from environment variables.
    ///
    /// | Variable | Field |
    /// |----------|-------|
    /// | `TURSO_ORG` | `org` |
    /// | `TURSO_GROUP` | `group` |
    /// | `TURSO_DB_PREFIX` | `prefix` |
    /// | `TURSO_API_TOKEN` | `api_token` |
    /// | `TURSO_GROUP_TOKEN` | `group_token` |
    /// | `TURSO_API_BASE_URL` | `base_url` (optional) |
    pub fn from_env() -> Result<Self, Error> {
        let read = |name: &str| -> Result<String, Error> {
            std::env::var(name)
                .map_err(|_| Error::Configuration(format!("missing environment variable: {name}")))
        };

        Ok(Self {
            org: read("TURSO_ORG")?,
            group: read("TURSO_GROUP")?,
            prefix: read("TURSO_DB_PREFIX")?,
            api_token: read("TURSO_API_TOKEN")?,
            group_token: read("TURSO_GROUP_TOKEN")?,
            base_url: std::env::var("TURSO_API_BASE_URL").ok(),
        })
    }
}

/// Production provisioner that creates per-partition Turso databases via the
/// Platform API.
///
/// Each partition name is sanitized into a valid Turso database name,
/// created on demand, and connected to using the group-level auth token.
pub type TursoPlatformProvisioner<A = api::TursoHttpClient> =
    TursoPlatformProvisionerImpl<A, LibsqlPartitionMetadataStore>;

pub struct TursoPlatformProvisionerImpl<A, M> {
    api: A,
    metadata: M,
    group: String,
    prefix: String,
    group_token: String,
    cache: Mutex<HashMap<String, DatabaseTarget>>,
    known_names: Mutex<HashSet<String>>,
}

#[cfg(feature = "turso")]
impl TursoPlatformProvisionerImpl<api::TursoHttpClient, LibsqlPartitionMetadataStore> {
    /// Create a new provisioner from configuration.
    pub fn new(config: TursoPlatformConfig) -> Self {
        let base_url = config
            .base_url
            .unwrap_or_else(|| TursoPlatformConfig::DEFAULT_BASE_URL.to_string());
        let client = api::TursoHttpClient::new(base_url, config.api_token, config.org);
        Self::with_api(client, config.group, config.prefix, config.group_token)
    }
}

impl<A: TursoPlatformApi> TursoPlatformProvisionerImpl<A, LibsqlPartitionMetadataStore> {
    fn with_api(api: A, group: String, prefix: String, group_token: String) -> Self {
        Self {
            api,
            group,
            prefix,
            group_token,
            metadata: LibsqlPartitionMetadataStore,
            cache: Mutex::new(HashMap::new()),
            known_names: Mutex::new(HashSet::new()),
        }
    }
}

#[allow(private_bounds)]
impl<A: TursoPlatformApi, M: PartitionMetadataStore> TursoPlatformProvisionerImpl<A, M> {
    #[cfg(test)]
    fn with_api_and_metadata(
        api: A,
        metadata: M,
        group: String,
        prefix: String,
        group_token: String,
    ) -> Self {
        Self {
            api,
            metadata,
            group,
            prefix,
            group_token,
            cache: Mutex::new(HashMap::new()),
            known_names: Mutex::new(HashSet::new()),
        }
    }

    /// Delete all databases created by this provisioner (tracked in cache).
    /// Also deletes any databases matching the prefix found via the API.
    pub async fn cleanup(&self) -> Result<(), Error> {
        let default_db_name = self.db_name_for(PartitionName::Default);
        let named_db_prefix = self.named_db_prefix();

        let cached: Vec<String> = self.cache.lock().unwrap().keys().cloned().collect();
        let api_dbs = self
            .api
            .list_databases(&self.group)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        let mut to_delete: HashSet<String> = cached.into_iter().collect();
        if self.cache.lock().unwrap().contains_key(&default_db_name) {
            to_delete.insert(default_db_name.clone());
        }
        for db in &api_dbs {
            if db.name == default_db_name || db.name.starts_with(&named_db_prefix) {
                to_delete.insert(db.name.clone());
            }
        }

        for name in &to_delete {
            self.api
                .delete_database(name)
                .await
                .map_err(|e| Error::Internal(format!("failed to delete {name}: {e}")))?;
        }

        self.cache.lock().unwrap().clear();
        self.known_names.lock().unwrap().clear();

        Ok(())
    }

    fn db_name_for(&self, name: PartitionName<'_>) -> String {
        match name {
            PartitionName::Default => sanitize_database_name("", &self.prefix),
            PartitionName::Named(n) => sanitize_database_name(n, &self.prefix),
        }
    }

    fn named_db_prefix(&self) -> String {
        format!("{}-", named_database_prefix(&self.prefix))
    }

    fn is_managed_named_database(&self, db_name: &str) -> bool {
        db_name != self.db_name_for(PartitionName::Default)
            && db_name.starts_with(&self.named_db_prefix())
    }

    async fn lookup_existing_database(&self, db_name: &str) -> Result<Option<DatabaseInfo>, Error> {
        self.api
            .get_database(db_name)
            .await
            .map_err(|e| Error::Internal(e.to_string()))
    }

    async fn find_existing_database(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<(String, DatabaseInfo)>, Error> {
        let db_name = self.db_name_for(name);
        if let Some(info) = self.lookup_existing_database(&db_name).await? {
            return Ok(Some((db_name, info)));
        }
        Ok(None)
    }

    fn record_name(&self, name: PartitionName<'_>) {
        if let PartitionName::Named(n) = name {
            self.known_names.lock().unwrap().insert(n.to_string());
        }
    }

    fn make_target(&self, hostname: &str) -> DatabaseTarget {
        DatabaseTarget::Turso {
            url: format!("libsql://{hostname}"),
            auth_token: self.group_token.clone(),
        }
    }

    async fn persist_logical_name(
        &self,
        name: PartitionName<'_>,
        target: &DatabaseTarget,
    ) -> Result<(), Error> {
        let PartitionName::Named(logical_name) = name else {
            return Ok(());
        };

        self.metadata
            .ensure_partition_name(target, logical_name)
            .await
    }
}

impl<A: TursoPlatformApi, M: PartitionMetadataStore> NamedTargetProvisioner
    for TursoPlatformProvisionerImpl<A, M>
{
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        let db_name = self.db_name_for(name);

        if let Some(target) = self.cache.lock().unwrap().get(&db_name) {
            return Ok(target.clone());
        }

        if let Some((existing_name, info)) = self.find_existing_database(name).await? {
            let target = self.make_target(&info.hostname);
            self.persist_logical_name(name, &target).await?;
            self.cache
                .lock()
                .unwrap()
                .insert(existing_name, target.clone());
            self.record_name(name);
            return Ok(target);
        }

        let info = match self.api.create_database(&db_name, &self.group).await {
            Ok(info) => info,
            Err(ApiError::AlreadyExists) => self
                .api
                .get_database(&db_name)
                .await
                .map_err(|e| Error::Internal(e.to_string()))?
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "database '{db_name}' reported as existing but not found"
                    ))
                })?,
            Err(ApiError::AuthFailure(msg)) => {
                return Err(Error::Configuration(format!(
                    "Turso API auth failure: {msg}"
                )));
            }
            Err(e) => return Err(Error::Internal(e.to_string())),
        };

        let target = self.make_target(&info.hostname);
        self.persist_logical_name(name, &target).await?;
        self.cache.lock().unwrap().insert(db_name, target.clone());
        self.record_name(name);
        Ok(target)
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let db_name = self.db_name_for(name);
        if let Some(target) = self.cache.lock().unwrap().get(&db_name) {
            return Ok(Some(target.clone()));
        }

        let Some((db_name, info)) = self.find_existing_database(name).await? else {
            return Ok(None);
        };

        let target = self.make_target(&info.hostname);
        self.persist_logical_name(name, &target).await?;
        self.cache.lock().unwrap().insert(db_name, target.clone());
        self.record_name(name);
        Ok(Some(target))
    }

    async fn names(&self) -> Result<Vec<String>, Error> {
        let known_names: Vec<String> = {
            let known = self.known_names.lock().unwrap();
            known.iter().cloned().collect()
        };
        if !known_names.is_empty() {
            return Ok(known_names);
        }

        let databases = self
            .api
            .list_databases(&self.group)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        let mut names: Vec<String> = try_join_all(databases.into_iter().filter_map(|db| {
            if !self.is_managed_named_database(&db.name) {
                return None;
            }

            Some(async move {
                let target = self.make_target(&db.hostname);
                self.cache
                    .lock()
                    .unwrap()
                    .insert(db.name.clone(), target.clone());

                self.metadata
                    .load_partition_name(&target)
                    .await?
                    .ok_or_else(|| {
                        Error::Configuration(format!(
                            "database '{}' is missing logical partition metadata",
                            db.name
                        ))
                    })
            })
        }))
        .await?;
        names.sort();
        names.dedup();
        self.known_names
            .lock()
            .unwrap()
            .extend(names.iter().cloned());
        Ok(names)
    }

    async fn named_targets(&self) -> Result<Vec<(String, DatabaseTarget)>, Error> {
        let databases = self
            .api
            .list_databases(&self.group)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        let default_db_name = self.db_name_for(PartitionName::Default);
        let named_db_prefix = self.named_db_prefix();
        let mut targets = Vec::new();
        let mut cache = self.cache.lock().unwrap();
        for db in databases {
            if db.name == default_db_name {
                continue;
            }
            let Some(stripped_name) = db.name.strip_prefix(&named_db_prefix) else {
                continue;
            };

            let target = self.make_target(&db.hostname);
            cache.insert(db.name.clone(), target.clone());
            targets.push((stripped_name.to_string(), target));
        }

        Ok(targets)
    }
}

impl<A: TursoPlatformApi, M: PartitionMetadataStore> TursoProvisioner
    for TursoPlatformProvisionerImpl<A, M>
{
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use api::fake::FakeTursoPlatformApi;

    #[derive(Default)]
    struct FakePartitionMetadataStore {
        names: Mutex<HashMap<String, String>>,
        ensure_calls: AtomicUsize,
        load_calls: AtomicUsize,
    }

    impl FakePartitionMetadataStore {
        fn new() -> Self {
            Self::default()
        }

        fn target_key(target: &DatabaseTarget) -> String {
            match target {
                DatabaseTarget::Turso { url, .. } => url.clone(),
                other => panic!("unexpected target for fake metadata store: {other:?}"),
            }
        }

        fn seed(&self, target: &DatabaseTarget, logical_name: &str) {
            self.names
                .lock()
                .unwrap()
                .insert(Self::target_key(target), logical_name.to_string());
        }
    }

    impl PartitionMetadataStore for Arc<FakePartitionMetadataStore> {
        async fn ensure_partition_name(
            &self,
            target: &DatabaseTarget,
            logical_name: &str,
        ) -> Result<(), Error> {
            self.ensure_calls.fetch_add(1, Ordering::Relaxed);
            let key = FakePartitionMetadataStore::target_key(target);
            let mut names = self.names.lock().unwrap();

            if let Some(recorded_name) = names.get(&key) {
                if recorded_name != logical_name {
                    return Err(Error::Configuration(format!(
                        "logical partition name mismatch: target is recorded as '{recorded_name}' but was opened for '{logical_name}'"
                    )));
                }
                return Ok(());
            }

            names.insert(key, logical_name.to_string());
            Ok(())
        }

        async fn load_partition_name(
            &self,
            target: &DatabaseTarget,
        ) -> Result<Option<String>, Error> {
            self.load_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .names
                .lock()
                .unwrap()
                .get(&FakePartitionMetadataStore::target_key(target))
                .cloned())
        }
    }

    type TestApi = Arc<FakeTursoPlatformApi>;
    type TestMetadata = Arc<FakePartitionMetadataStore>;
    type TestProvisioner = TursoPlatformProvisionerImpl<TestApi, TestMetadata>;

    fn test_provisioner() -> (TestApi, TestMetadata, TestProvisioner) {
        let api = Arc::new(FakeTursoPlatformApi::new());
        let metadata = Arc::new(FakePartitionMetadataStore::new());
        let provisioner = TestProvisioner::with_api_and_metadata(
            api.clone(),
            metadata.clone(),
            "default".to_string(),
            "myapp".to_string(),
            "group-tok-123".to_string(),
        );
        (api, metadata, provisioner)
    }

    #[tokio::test]
    async fn ensure_creates_database_and_returns_turso_target() {
        let (_api, _metadata, provisioner) = test_provisioner();
        let target = provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        assert_eq!(
            target,
            DatabaseTarget::Turso {
                url: format!(
                    "libsql://{}-testorg.turso.io",
                    sanitize_database_name("orders", "myapp")
                ),
                auth_token: "group-tok-123".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn ensure_default_partition_uses_prefix_only() {
        let (_api, _metadata, provisioner) = test_provisioner();
        let target = provisioner
            .ensure_target_for_name(PartitionName::Default)
            .await
            .unwrap();

        assert_eq!(
            target,
            DatabaseTarget::Turso {
                url: "libsql://myapp-testorg.turso.io".to_string(),
                auth_token: "group-tok-123".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn ensure_returns_cached_on_second_call() {
        let (api, _metadata, provisioner) = test_provisioner();
        provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();
        provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        assert_eq!(api.create_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn ensure_handles_already_exists_by_fetching() {
        let (api, _metadata, provisioner) = test_provisioner();
        api.create_database(&sanitize_database_name("orders", "myapp"), "default")
            .await
            .unwrap();

        let target = provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        assert_eq!(api.create_calls.load(Ordering::Relaxed), 1);
        assert_eq!(api.get_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(target, DatabaseTarget::Turso { .. }));
    }

    #[tokio::test]
    async fn ensure_sanitizes_partition_name() {
        let (api, _metadata, provisioner) = test_provisioner();
        provisioner
            .ensure_target_for_name(PartitionName::Named("Tenant:ACME"))
            .await
            .unwrap();

        assert!(
            api.get_database(&sanitize_database_name("Tenant:ACME", "myapp"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn ensure_records_original_name() {
        let (_api, _metadata, provisioner) = test_provisioner();
        provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        let names = provisioner.names().await.unwrap();
        assert_eq!(names, vec!["orders".to_string()]);
    }

    #[tokio::test]
    async fn ensure_persists_partition_metadata_for_named_targets() {
        let (_api, metadata, provisioner) = test_provisioner();
        let target = provisioner
            .ensure_target_for_name(PartitionName::Named("tenant/acme:blue"))
            .await
            .expect("target should resolve");

        assert_eq!(
            metadata
                .load_partition_name(&target)
                .await
                .expect("metadata lookup should succeed"),
            Some("tenant/acme:blue".to_string())
        );
    }

    #[tokio::test]
    async fn existing_returns_cached() {
        let (api, _metadata, provisioner) = test_provisioner();
        provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();
        api.get_calls.store(0, Ordering::Relaxed);

        let target = provisioner
            .target_for_existing_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        assert!(target.is_some());
        assert_eq!(api.get_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn existing_falls_back_to_api() {
        let (api, metadata, provisioner) = test_provisioner();
        api.create_database(&sanitize_database_name("orders", "myapp"), "default")
            .await
            .unwrap();

        let target = provisioner
            .target_for_existing_name(PartitionName::Named("orders"))
            .await
            .unwrap();

        assert!(target.is_some());
        assert_eq!(api.get_calls.load(Ordering::Relaxed), 1);
        assert_eq!(metadata.ensure_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn existing_returns_none_for_unknown() {
        let (_api, _metadata, provisioner) = test_provisioner();
        let target = provisioner
            .target_for_existing_name(PartitionName::Named("missing"))
            .await
            .unwrap();
        assert!(target.is_none());
    }

    #[tokio::test]
    async fn names_returns_known_names_from_cache() {
        let (_api, _metadata, provisioner) = test_provisioner();
        provisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .unwrap();
        provisioner
            .ensure_target_for_name(PartitionName::Named("users"))
            .await
            .unwrap();

        let mut names = provisioner.names().await.unwrap();
        names.sort();
        assert_eq!(names, vec!["orders", "users"]);
    }

    #[tokio::test]
    async fn names_reads_exact_logical_names_from_metadata() {
        let (api, metadata, provisioner) = test_provisioner();
        let created = api
            .create_database(&sanitize_database_name("orders", "myapp"), "default")
            .await
            .expect("database should be created");
        metadata.seed(
            &provisioner.make_target(&created.hostname),
            "tenant/acme:blue",
        );

        let names = provisioner.names().await.expect("names should enumerate");
        assert_eq!(names, vec!["tenant/acme:blue".to_string()]);
    }

    #[tokio::test]
    async fn names_errors_when_metadata_is_missing() {
        let (api, _metadata, provisioner) = test_provisioner();
        api.create_database(&sanitize_database_name("orders", "myapp"), "default")
            .await
            .expect("database should be created");

        let error = provisioner
            .names()
            .await
            .expect_err("missing metadata should fail");

        assert!(
            error
                .to_string()
                .contains("missing logical partition metadata")
        );
    }

    #[tokio::test]
    async fn names_fallback_uses_sanitized_default_prefix() {
        let api = Arc::new(FakeTursoPlatformApi::new());
        let metadata = Arc::new(FakePartitionMetadataStore::new());
        let provisioner = TestProvisioner::with_api_and_metadata(
            api.clone(),
            metadata.clone(),
            "default".to_string(),
            "My_App".to_string(),
            "group-tok-123".to_string(),
        );
        let created = api
            .create_database(&sanitize_database_name("orders", "My_App"), "default")
            .await
            .unwrap();
        metadata.seed(&provisioner.make_target(&created.hostname), "orders");

        let names = provisioner.names().await.unwrap();
        assert_eq!(names, vec!["orders".to_string()]);
    }

    #[test]
    fn from_env_returns_error_on_missing_var() {
        // SAFETY: Rust 2024 marks env mutation unsafe due to multi-thread
        // hazards. Test process is single-threaded here.
        unsafe { std::env::remove_var("TURSO_ORG") };
        let result = TursoPlatformConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("TURSO_ORG"));
    }
}
