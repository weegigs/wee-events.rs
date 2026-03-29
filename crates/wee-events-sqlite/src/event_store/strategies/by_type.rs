use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    SqliteLocalPartitionStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TypeStrategy;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypePartition(pub AggregateType);

impl SqlitePartitionStrategy for TypeStrategy {
    type Partition = TypePartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        Ok(TypePartition(aggregate_id.aggregate_type.clone()))
    }

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead {
        SqlitePartitionRead::ScanType(partition.0.clone())
    }

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead {
        if &partition.0 == aggregate_type {
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
        Ok(root.join(format!("{}.db", partition.0.as_str())))
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
            partitions.push(TypePartition(AggregateType::new(
                stem.to_string_lossy().into_owned(),
            )));
        }
        Ok(partitions)
    }
}

impl SqliteSqldNamespacedPartitionStrategy for TypeStrategy {}
