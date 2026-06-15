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
    #[must_use]
    pub fn new(buckets: NonZeroU32) -> Self {
        Self { buckets }
    }

    #[must_use]
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

/// FNV-1a over the aggregate type's length (4 LE bytes), then the type bytes,
/// then the key bytes. Length-prefixing the type prevents type/key boundary
/// collisions: `("foo:", "bar")` and `("foo", ":bar")` must hash differently.
fn hash_aggregate_id(aggregate_id: &AggregateId) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    let agg_type = aggregate_id.aggregate_type().as_str().as_bytes();
    let agg_key = aggregate_id.aggregate_key().as_bytes();

    let len_prefix = u32::try_from(agg_type.len())
        .unwrap_or(u32::MAX)
        .to_le_bytes();
    for byte in len_prefix
        .iter()
        .copied()
        .chain(agg_type.iter().copied())
        .chain(agg_key.iter().copied())
    {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }

    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_key_boundary_does_not_collide() {
        let a = AggregateId::new("foo", ":bar");
        let b = AggregateId::new("foo:", "bar");
        assert_ne!(hash_aggregate_id(&a), hash_aggregate_id(&b));
    }

    #[test]
    fn distinct_aggregates_have_distinct_hashes() {
        let a = AggregateId::new("user", "alice");
        let b = AggregateId::new("user", "bob");
        assert_ne!(hash_aggregate_id(&a), hash_aggregate_id(&b));
    }
}
