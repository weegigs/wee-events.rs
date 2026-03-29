use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use wee_events::{AggregateId, AggregateType};

use crate::Error;

use super::{
    SqliteLocalPartitionStrategy, SqlitePartitionRead, SqlitePartitionStrategy,
    SqliteSqldNamespacedPartitionStrategy,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BucketPartition(pub u32);

impl SqlitePartitionStrategy for HashedStrategy {
    type Partition = BucketPartition;

    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<Self::Partition, Error> {
        Ok(BucketPartition(
            hash_aggregate_id(aggregate_id) % self.buckets.get(),
        ))
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

impl SqliteLocalPartitionStrategy for HashedStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error> {
        std::fs::create_dir_all(root)?;
        Ok(())
    }

    fn path_for_partition(
        &self,
        root: &Path,
        partition: &Self::Partition,
    ) -> Result<PathBuf, Error> {
        Ok(root.join(format!("bucket-{}.db", partition.0)))
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
            let stem = stem.to_string_lossy();
            let Some(bucket) = stem.strip_prefix("bucket-") else {
                continue;
            };
            let Ok(bucket) = bucket.parse::<u32>() else {
                continue;
            };
            partitions.push(BucketPartition(bucket));
        }
        Ok(partitions)
    }
}

impl SqliteSqldNamespacedPartitionStrategy for HashedStrategy {}

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
