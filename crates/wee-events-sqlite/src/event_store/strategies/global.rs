use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    LocalPartitionLayout, LocalPartitionStrategy, PartitionNamingStrategy, PartitionRead,
    PartitionStrategy, SingleRemotePartitionStrategy, SqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GlobalStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlobalPartition;

impl PartitionStrategy for GlobalStrategy {
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

    fn read_plan(&self, _partition: &Self::Partition) -> PartitionRead {
        PartitionRead::ScanAll
    }

    fn read_plan_by_type(
        &self,
        _partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> PartitionRead {
        PartitionRead::ScanType(aggregate_type.clone())
    }
}

impl PartitionNamingStrategy for GlobalStrategy {
    fn partition_name<'a>(&self, _partition: &'a Self::Partition) -> Option<&'a str> {
        None
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        Err(Error::Configuration(format!(
            "global strategy does not support named local partitions: {name}"
        )))
    }
}

impl LocalPartitionStrategy for GlobalStrategy {
    fn initialize_root(&self, _root: &Path) -> Result<(), Error> {
        Ok(())
    }

    fn local_partition_layout(&self) -> LocalPartitionLayout {
        LocalPartitionLayout::SingleDatabase
    }
}

impl SingleRemotePartitionStrategy for GlobalStrategy {}

impl SqldNamespacedPartitionStrategy for GlobalStrategy {}
