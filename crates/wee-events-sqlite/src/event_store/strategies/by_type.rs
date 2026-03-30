use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    decode_path_component, encode_path_component, NamedPartition, SqliteLocalPartitionStrategy,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TypeStrategy;

pub type TypePartition = NamedPartition<AggregateType>;

impl SqlitePartitionStrategy for TypeStrategy {
    type Partition = TypePartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        let aggregate_type = aggregate_id.aggregate_type.clone();
        Ok(TypePartition::new(
            aggregate_type.as_str().to_string(),
            aggregate_type,
        ))
    }

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanType(partition.key().clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        if partition.key() == aggregate_type {
            SqlitePartitionRead::ScanType(aggregate_type.clone())
        } else {
            SqlitePartitionRead::Skip
        }
    }
}

impl SqliteLocalPartitionStrategy for TypeStrategy {
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
            let aggregate_type = decode_path_component(&stem.to_string_lossy())?;
            let aggregate_type = AggregateType::new(aggregate_type);
            partitions.push(TypePartition::new(
                aggregate_type.as_str().to_string(),
                aggregate_type,
            ));
        }
        Ok(partitions)
    }
}

impl SqliteSqldNamespacedPartitionStrategy for TypeStrategy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_for_partition_encodes_type_names() {
        let path = TypeStrategy
            .path_for_partition(
                Path::new("/tmp/root"),
                &TypePartition::new("a/b:c", AggregateType::new("a/b:c")),
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
        let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
        let partition = TypePartition::new("a/b:c", AggregateType::new("a/b:c"));
        let path = TypeStrategy
            .path_for_partition(temp_dir.path(), &partition)
            .expect("path generation should succeed");
        std::fs::write(path, b"").expect("write should succeed");

        let partitions = TypeStrategy
            .discover_partitions(temp_dir.path())
            .expect("discovery should succeed");

        assert_eq!(partitions, vec![partition]);
    }
}
