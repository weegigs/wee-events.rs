use std::num::NonZeroU32;
use std::path::Path;

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    LocalPartitionLayout, LocalPartitionStrategy, NamedPartition, PartitionName,
    PartitionNamingStrategy, PartitionRead, PartitionStrategy, SqldNamespacedPartitionStrategy,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HashedStrategy {
    buckets: NonZeroU32,
}

impl HashedStrategy {
    pub fn new(buckets: NonZeroU32) -> Self {
        Self { buckets }
    }

    pub fn buckets(&self) -> NonZeroU32 {
        self.buckets
    }
}

pub type BucketPartition = NamedPartition<u32>;

impl PartitionStrategy for HashedStrategy {
    type Partition = BucketPartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        let bucket = hash_aggregate_id(aggregate_id) % self.buckets.get();
        Ok(BucketPartition::new(format!("bucket-{bucket}"), bucket))
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

impl PartitionNamingStrategy for HashedStrategy {
    fn partition_name<'a>(&self, partition: &'a Self::Partition) -> PartitionName<'a> {
        PartitionName::Named(partition.name())
    }

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error> {
        let bucket = name
            .strip_prefix("bucket-")
            .ok_or_else(|| Error::Configuration(format!("invalid hashed partition name '{name}'")))?
            .parse::<u32>()
            .map_err(|error| {
                Error::Configuration(format!("invalid hashed partition name '{name}': {error}"))
            })?;
        Ok(BucketPartition::new(format!("bucket-{bucket}"), bucket))
    }
}

impl LocalPartitionStrategy for HashedStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn local_partition_layout(&self) -> LocalPartitionLayout {
        LocalPartitionLayout::NamedDatabases
    }
}

impl SqldNamespacedPartitionStrategy for HashedStrategy {}

fn hash_aggregate_id(aggregate_id: &AggregateId) -> u32 {
    let mut hash = 0x811c9dc5_u32;

    for byte in aggregate_id
        .aggregate_type
        .as_str()
        .bytes()
        .chain([b':'])
        .chain(aggregate_id.aggregate_key.bytes())
    {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }

    hash
}
