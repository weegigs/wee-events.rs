use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    SqliteLocalPartitionStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSingleRemotePartitionStrategy, SqliteSqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GlobalStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlobalPartition;

impl SqlitePartitionStrategy for GlobalStrategy {
    type Partition = GlobalPartition;

    fn bootstrap_partitions(&self) -> Vec<Self::Partition> {
        vec![GlobalPartition]
    }

    fn partition_for_aggregate(
        &self,
        _aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        Ok(GlobalPartition)
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

impl SqliteLocalPartitionStrategy for GlobalStrategy {
    fn initialize_root(&self, _root: &Path) -> Result<(), Error> {
        Ok(())
    }

    fn path_for_partition(
        &self,
        root: &Path,
        _partition: &Self::Partition,
    ) -> Result<PathBuf, Error> {
        Ok(root.to_path_buf())
    }

    fn discover_partitions(&self, _root: &Path) -> Result<Vec<Self::Partition>, Error> {
        Ok(vec![GlobalPartition])
    }
}

impl SqliteSingleRemotePartitionStrategy for GlobalStrategy {}

impl SqliteSqldNamespacedPartitionStrategy for GlobalStrategy {}
