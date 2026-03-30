use std::path::PathBuf;

use data_encoding::BASE32_NOPAD;

use crate::Error;

use super::super::partitioning::PartitionCatalog;
use super::super::store::LocalBackend;
use super::super::strategies::{LocalPartitionLayout, LocalPartitionStrategy, PartitionName};
use super::super::types::DatabaseTarget;
use super::BackendBinding;

#[derive(Debug)]
pub struct LocalPartitionCatalog<S> {
    root: PathBuf,
    strategy: S,
}

impl<S> LocalPartitionCatalog<S>
where
    S: LocalPartitionStrategy,
{
    pub(crate) fn new(root: PathBuf, strategy: S) -> Result<Self, Error> {
        strategy.initialize_root(&root)?;
        Ok(Self { root, strategy })
    }

    fn path_for_partition(&self, partition: &S::Partition) -> Result<PathBuf, Error> {
        match self.strategy.local_partition_layout() {
            LocalPartitionLayout::SingleDatabase => Ok(self.root.clone()),
            LocalPartitionLayout::NamedDatabases => {
                let name = match self.strategy.partition_name(partition) {
                    PartitionName::Named(name) => name,
                    PartitionName::Default => {
                        return Err(Error::Configuration(
                            "named local partition strategy returned the default partition"
                                .to_string(),
                        ))
                    }
                };
                Ok(self
                    .root
                    .join(format!("{}.db", encode_path_component(name))))
            }
        }
    }

    fn discover_partitions(&self) -> Result<Vec<S::Partition>, Error> {
        match self.strategy.local_partition_layout() {
            LocalPartitionLayout::SingleDatabase => Ok(self.strategy.bootstrap_partitions()),
            LocalPartitionLayout::NamedDatabases => {
                let mut partitions = Vec::new();
                for entry in std::fs::read_dir(&self.root)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.extension().is_none_or(|extension| extension != "db") {
                        continue;
                    }
                    let Some(stem) = path.file_stem() else {
                        continue;
                    };
                    let partition_name = decode_path_component(&stem.to_string_lossy())?;
                    partitions.push(self.strategy.partition_from_name(&partition_name)?);
                }
                Ok(partitions)
            }
        }
    }
}

impl<S> PartitionCatalog<S::Partition> for LocalPartitionCatalog<S>
where
    S: LocalPartitionStrategy,
{
    async fn ensure_target_for_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<DatabaseTarget, Error> {
        let path = self.path_for_partition(partition)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(DatabaseTarget::Local(path))
    }

    async fn target_for_existing_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<Option<DatabaseTarget>, Error> {
        let path = self.path_for_partition(partition)?;
        Ok(path.exists().then_some(DatabaseTarget::Local(path)))
    }

    async fn partitions(&self) -> Result<Vec<S::Partition>, Error> {
        self.discover_partitions()
    }
}

impl<S> BackendBinding<S> for LocalBackend
where
    S: LocalPartitionStrategy,
{
    type Catalog = LocalPartitionCatalog<S>;

    fn into_catalog(self, strategy: &S) -> Result<Self::Catalog, Error> {
        LocalPartitionCatalog::new(self.path, strategy.clone())
    }
}

const ENCODED_PATH_PREFIX: &str = "b32-";

fn encode_path_component(value: &str) -> String {
    let encoded_value = BASE32_NOPAD.encode(value.as_bytes());
    let mut encoded = String::with_capacity(ENCODED_PATH_PREFIX.len() + encoded_value.len());
    encoded.push_str(ENCODED_PATH_PREFIX);
    encoded.push_str(&encoded_value);
    encoded
}

fn decode_path_component(value: &str) -> Result<String, Error> {
    let encoded = value.strip_prefix(ENCODED_PATH_PREFIX).ok_or_else(|| {
        Error::Configuration(format!("invalid encoded partition path component: {value}"))
    })?;

    let bytes = BASE32_NOPAD.decode(encoded.as_bytes()).map_err(|error| {
        Error::Configuration(format!(
            "invalid encoded partition path component '{value}': {error}"
        ))
    })?;

    String::from_utf8(bytes).map_err(|error| {
        Error::Configuration(format!(
            "invalid utf-8 in encoded partition path component '{value}': {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use wee_events::{AggregateId, AggregateType};

    use super::*;
    use crate::event_store::strategies::{
        AggregatePartition, AggregateStrategy, GlobalPartition, GlobalStrategy, TypePartition,
        TypeStrategy,
    };

    #[tokio::test]
    async fn named_partition_paths_are_base32_encoded() {
        let root = tempfile::tempdir().expect("tempdir should succeed");
        let catalog = LocalPartitionCatalog::new(root.path().to_path_buf(), TypeStrategy)
            .expect("catalog should be created");
        let partition = TypePartition::new("a/b:c", AggregateType::new("a/b:c"));

        let target = catalog
            .ensure_target_for_partition(&partition)
            .await
            .expect("path generation should succeed");

        let DatabaseTarget::Local(path) = target else {
            panic!("expected local target");
        };
        let file_name = path
            .file_name()
            .expect("file name should be present")
            .to_string_lossy();
        assert!(file_name.starts_with("b32-"));
        assert!(!file_name.contains('/'));
        assert!(!file_name.contains('\\'));
        assert!(!file_name.contains(':'));
    }

    #[tokio::test]
    async fn named_partition_discovery_restores_typed_partitions() {
        let root = tempfile::tempdir().expect("tempdir should succeed");
        let catalog = LocalPartitionCatalog::new(root.path().to_path_buf(), AggregateStrategy)
            .expect("catalog should be created");
        let aggregate_id = AggregateId::new("campaign/run", "urn:uuid:abc/123");
        let partition = AggregatePartition::new(aggregate_id.to_string(), aggregate_id);

        let target = catalog
            .ensure_target_for_partition(&partition)
            .await
            .expect("path generation should succeed");
        let DatabaseTarget::Local(path) = target else {
            panic!("expected local target");
        };
        std::fs::write(path, b"").expect("write should succeed");

        let partitions = catalog
            .partitions()
            .await
            .expect("discovery should succeed");

        assert_eq!(partitions, vec![partition]);
    }

    #[tokio::test]
    async fn single_database_layout_uses_root_path() {
        let root = tempfile::tempdir().expect("tempdir should succeed");
        let database_path = root.path().join("store.db");
        let catalog = LocalPartitionCatalog::new(database_path.clone(), GlobalStrategy)
            .expect("catalog should be created");

        let target = catalog
            .ensure_target_for_partition(&GlobalPartition)
            .await
            .expect("path generation should succeed");

        assert_eq!(target, DatabaseTarget::Local(database_path));
    }
}
