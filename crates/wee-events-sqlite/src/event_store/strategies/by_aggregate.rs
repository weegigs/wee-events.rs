use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    decode_path_component, encode_path_component, SqliteLocalPartitionStrategy,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AggregateStrategy;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregatePartition(pub AggregateId);

impl SqlitePartitionStrategy for AggregateStrategy {
    type Partition = AggregatePartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        Ok(AggregatePartition(aggregate_id.clone()))
    }

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::Direct(partition.0.clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        if &partition.0.aggregate_type == aggregate_type {
            SqlitePartitionRead::Direct(partition.0.clone())
        } else {
            SqlitePartitionRead::Skip
        }
    }
}

impl SqliteLocalPartitionStrategy for AggregateStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn path_for_partition(
        &self,
        root: &Path,
        partition: &Self::Partition,
    ) -> Result<PathBuf, Error> {
        Ok(root
            .join(encode_path_component(partition.0.aggregate_type.as_str()))
            .join(format!(
                "{}.db",
                encode_path_component(&partition.0.aggregate_key)
            )))
    }

    fn discover_partitions(&self, root: &Path) -> Result<Vec<Self::Partition>, Error> {
        let mut partitions = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let aggregate_type = decode_path_component(&entry.file_name().to_string_lossy())?;
            for child in std::fs::read_dir(entry.path())? {
                let child = child?;
                let child_path = child.path();
                if child_path
                    .extension()
                    .is_none_or(|extension| extension != "db")
                {
                    continue;
                }
                let Some(stem) = child_path.file_stem() else {
                    continue;
                };
                let aggregate_key = decode_path_component(&stem.to_string_lossy())?;
                partitions.push(AggregatePartition(AggregateId::new(
                    aggregate_type.clone(),
                    aggregate_key,
                )));
            }
        }
        Ok(partitions)
    }
}

impl SqliteSqldNamespacedPartitionStrategy for AggregateStrategy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_for_partition_encodes_type_and_key() {
        let partition = AggregatePartition(AggregateId::new("campaign/run", "urn:uuid:abc/123"));
        let path = AggregateStrategy
            .path_for_partition(Path::new("/tmp/root"), &partition)
            .expect("path generation should succeed");

        let parent = path
            .parent()
            .and_then(Path::file_name)
            .expect("parent directory name should be present")
            .to_string_lossy();
        let file_name = path
            .file_name()
            .expect("file name should be present")
            .to_string_lossy();

        assert!(parent.starts_with("b32-"));
        assert!(file_name.starts_with("b32-"));
        assert!(!parent.contains('/'));
        assert!(!parent.contains('\\'));
        assert!(!file_name.contains('/'));
        assert!(!file_name.contains('\\'));
        assert!(!file_name.contains(':'));
    }

    #[test]
    fn discover_partitions_decodes_encoded_names() {
        let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
        let partition = AggregatePartition(AggregateId::new("campaign/run", "urn:uuid:abc/123"));
        let path = AggregateStrategy
            .path_for_partition(temp_dir.path(), &partition)
            .expect("path generation should succeed");
        std::fs::create_dir_all(path.parent().expect("parent should be present"))
            .expect("create_dir_all should succeed");
        std::fs::write(path, b"").expect("write should succeed");

        let partitions = AggregateStrategy
            .discover_partitions(temp_dir.path())
            .expect("discovery should succeed");

        assert_eq!(partitions, vec![partition]);
    }
}
