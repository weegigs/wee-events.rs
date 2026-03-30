use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    decode_path_component, encode_path_component, NamedPartition, SqliteLocalPartitionStrategy,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Clone)]
pub struct PartitionByStrategy<F> {
    partitioner: F,
}

impl<F> PartitionByStrategy<F> {
    pub fn new(partitioner: F) -> Self {
        Self { partitioner }
    }
}

impl<F> std::fmt::Debug for PartitionByStrategy<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PartitionByStrategy(..)")
    }
}

impl<F> SqlitePartitionStrategy for PartitionByStrategy<F>
where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static,
{
    type Partition = NamedPartition<String>;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        let name = (self.partitioner)(aggregate_id);
        Ok(NamedPartition::new(name.clone(), name))
    }

    fn read_plan(&self, _partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanAll
    }

    fn read_plan_by_type(
        &self,
        _partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanType(aggregate_type.clone())
    }
}

impl<F> SqliteLocalPartitionStrategy for PartitionByStrategy<F>
where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static,
{
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn path_for_partition(
        &self,
        root: &Path,
        partition: &Self::Partition,
    ) -> Result<PathBuf, Error> {
        Ok(root.join(format!("{}.db", encode_path_component(partition.name()))))
    }

    fn discover_partitions(&self, root: &Path) -> Result<Vec<Self::Partition>, Error> {
        let mut partitions = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_none_or(|extension| extension != "db") {
                continue;
            }
            let Some(stem) = path.file_stem() else {
                continue;
            };
            let partition_name = decode_path_component(&stem.to_string_lossy())?;
            partitions.push(NamedPartition::new(partition_name.clone(), partition_name));
        }
        Ok(partitions)
    }
}

impl<F> SqliteSqldNamespacedPartitionStrategy for PartitionByStrategy<F> where
    F: Fn(&AggregateId) -> String + Clone + Send + Sync + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition_by_user(aggregate_id: &AggregateId) -> String {
        aggregate_id
            .aggregate_key
            .split(':')
            .next()
            .expect("split always yields at least one segment")
            .to_string()
    }

    #[test]
    fn partition_by_strategy_routes_with_partitioner() {
        let strategy = PartitionByStrategy::new(partition_by_user);
        let order = AggregateId::new("order", "kevin:123");
        let payment = AggregateId::new("payment", "kevin:123");

        assert_eq!(
            strategy
                .partition_for_aggregate(&order)
                .expect("routing should succeed"),
            NamedPartition::new("kevin", "kevin".to_string())
        );
        assert_eq!(
            strategy
                .partition_for_aggregate(&payment)
                .expect("routing should succeed"),
            NamedPartition::new("kevin", "kevin".to_string())
        );
    }

    #[test]
    fn path_for_partition_encodes_partition_names() {
        let strategy = PartitionByStrategy::new(partition_by_user);
        let path = strategy
            .path_for_partition(
                Path::new("/tmp/root"),
                &NamedPartition::new("kevin/team:1", "kevin/team:1".to_string()),
            )
            .expect("path generation should succeed");

        let file_name = path
            .file_name()
            .expect("file name should be present")
            .to_string_lossy();
        assert!(file_name.starts_with("b32-"));
        assert!(!file_name.contains('/'));
        assert!(!file_name.contains('\\'));
        assert!(!file_name.contains(':'));
    }

    #[test]
    fn discover_partitions_decodes_encoded_names() {
        let strategy = PartitionByStrategy::new(partition_by_user);
        let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
        let partition = NamedPartition::new("kevin/team:1", "kevin/team:1".to_string());
        let path = strategy
            .path_for_partition(temp_dir.path(), &partition)
            .expect("path generation should succeed");
        std::fs::write(path, b"").expect("write should succeed");

        let partitions = strategy
            .discover_partitions(temp_dir.path())
            .expect("discovery should succeed");

        assert_eq!(partitions, vec![partition]);
    }
}
