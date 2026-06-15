use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use wee_events::{AggregateId, Encoding, EventData, EventStore as _, PublishOptions, RawEvent};
use wee_events_sqlite::{
    DatabaseTarget, Error, NamedTargetProvisioner, PartitionByStrategy, PartitionName,
    SqldNamespacedProvisioner, SqliteEventStore, TypeStrategy,
};

#[derive(Clone)]
struct SuffixedLocalProvisioner {
    root: Arc<PathBuf>,
    targets: Arc<Mutex<BTreeMap<String, PathBuf>>>,
}

impl SuffixedLocalProvisioner {
    fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: Arc::new(root.as_ref().to_path_buf()),
            targets: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn backend_name(name: &str) -> String {
        format!("backend-{name}-ns")
    }

    fn path_for_backend_name(&self, backend_name: &str) -> PathBuf {
        self.root.join(format!("{backend_name}.db"))
    }
}

impl NamedTargetProvisioner for SuffixedLocalProvisioner {
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        let PartitionName::Named(name) = name else {
            return Err(Error::Configuration(
                "test provisioner only supports named partitions".to_string(),
            ));
        };

        let backend_name = Self::backend_name(name);
        let path = self.path_for_backend_name(&backend_name);
        self.targets
            .lock()
            .unwrap()
            .insert(backend_name, path.clone());
        Ok(DatabaseTarget::Local(path))
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let PartitionName::Named(name) = name else {
            return Ok(None);
        };

        let backend_name = Self::backend_name(name);
        let path = self.path_for_backend_name(&backend_name);
        Ok(path.exists().then_some(DatabaseTarget::Local(path)))
    }

    async fn named_targets(&self) -> Result<Vec<(String, DatabaseTarget)>, Error> {
        let targets = self.targets.lock().unwrap();
        Ok(targets
            .iter()
            .map(|(backend_name, path)| (backend_name.clone(), DatabaseTarget::Local(path.clone())))
            .collect())
    }
}

impl SqldNamespacedProvisioner for SuffixedLocalProvisioner {}

#[derive(Clone)]
struct CollidingLocalProvisioner {
    path: Arc<PathBuf>,
}

impl CollidingLocalProvisioner {
    fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: Arc::new(root.as_ref().join("shared.db")),
        }
    }
}

impl NamedTargetProvisioner for CollidingLocalProvisioner {
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error> {
        let PartitionName::Named(_) = name else {
            return Err(Error::Configuration(
                "test provisioner only supports named partitions".to_string(),
            ));
        };

        Ok(DatabaseTarget::Local((*self.path).clone()))
    }

    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let PartitionName::Named(_) = name else {
            return Ok(None);
        };

        Ok(self
            .path
            .exists()
            .then_some(DatabaseTarget::Local((*self.path).clone())))
    }
}

impl SqldNamespacedProvisioner for CollidingLocalProvisioner {}

fn partition_by_tenant(aggregate_id: &AggregateId) -> String {
    aggregate_id
        .aggregate_key()
        .split(':')
        .next()
        .expect("split always yields at least one segment")
        .to_string()
}

#[tokio::test]
async fn enumerate_after_restart_uses_logical_partition_name_not_backend_name() {
    let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
    let provisioner = SuffixedLocalProvisioner::new(temp_dir.path());
    let aggregate_id = AggregateId::new("invoice", "tenant/acme:123");

    let store = SqliteEventStore::open_sqld_namespaced(
        provisioner.clone(),
        PartitionByStrategy::new(partition_by_tenant as fn(&AggregateId) -> String),
    )
    .await
    .expect("store should open");

    store
        .publish(
            &aggregate_id,
            PublishOptions::default(),
            vec![RawEvent {
                event_type: "invoice-created".into(),
                data: EventData::raw(Encoding::Json, b"{}".to_vec()),
            }],
        )
        .await
        .expect("publish should succeed");

    drop(store);

    let reopened = SqliteEventStore::open_sqld_namespaced(
        provisioner,
        PartitionByStrategy::new(partition_by_tenant as fn(&AggregateId) -> String),
    )
    .await
    .expect("store should reopen");

    let ids = reopened
        .enumerate_aggregates()
        .await
        .expect("enumeration should succeed");

    assert_eq!(ids, vec![aggregate_id]);
}

#[tokio::test]
async fn rejects_distinct_logical_partitions_that_alias_to_the_same_target() {
    let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
    let provisioner = CollidingLocalProvisioner::new(temp_dir.path());
    let first = AggregateId::new("tenant/acme", "123");
    let second = AggregateId::new("tenant:acme", "456");

    let store = SqliteEventStore::open_sqld_namespaced(provisioner, TypeStrategy)
        .await
        .expect("store should open");

    store
        .publish(
            &first,
            PublishOptions::default(),
            vec![RawEvent {
                event_type: "created".into(),
                data: EventData::raw(Encoding::Json, b"{}".to_vec()),
            }],
        )
        .await
        .expect("first publish should succeed");

    let error = store
        .publish(
            &second,
            PublishOptions::default(),
            vec![RawEvent {
                event_type: "created".into(),
                data: EventData::raw(Encoding::Json, b"{}".to_vec()),
            }],
        )
        .await
        .expect_err("second publish should reject the aliased partition");

    assert!(
        error
            .to_string()
            .contains("logical partition name mismatch"),
        "unexpected error: {error}"
    );
}
