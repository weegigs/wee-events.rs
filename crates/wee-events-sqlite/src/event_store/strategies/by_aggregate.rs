use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    SqliteLocalPartitionStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
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
            .join(partition.0.aggregate_type.as_str())
            .join(format!("{}.db", partition.0.aggregate_key)))
    }

    fn discover_partitions(&self, root: &Path) -> Result<Vec<Self::Partition>, Error> {
        let mut partitions = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let aggregate_type = entry.file_name().to_string_lossy().into_owned();
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
                partitions.push(AggregatePartition(AggregateId::new(
                    aggregate_type.clone(),
                    stem.to_string_lossy().into_owned(),
                )));
            }
        }
        Ok(partitions)
    }
}

impl SqliteSqldNamespacedPartitionStrategy for AggregateStrategy {}
