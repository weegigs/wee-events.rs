use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    SqliteLocalPartitionLayout, SqliteLocalPartitionStrategy, SqlitePartitionNamingStrategy,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteSingleRemotePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
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

impl SqlitePartitionNamingStrategy for GlobalStrategy {
    fn partition_name<'a>(&self, _partition: &'a Self::Partition) -> Option<&'a str> {
        None
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        Err(Error::Configuration(format!(
            "global strategy does not support named local partitions: {name}"
        )))
    }
}

impl SqliteLocalPartitionStrategy for GlobalStrategy {
    fn initialize_root(&self, _root: &Path) -> Result<(), Error> {
        Ok(())
    }

    fn local_partition_layout(&self) -> SqliteLocalPartitionLayout {
        SqliteLocalPartitionLayout::SingleDatabase
    }
}

impl SqliteSingleRemotePartitionStrategy for GlobalStrategy {}

impl SqliteSqldNamespacedPartitionStrategy for GlobalStrategy {}
