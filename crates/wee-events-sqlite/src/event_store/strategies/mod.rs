use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};

use data_encoding::BASE32_NOPAD;
use wee_events::{AggregateId, AggregateType};

use crate::Error;

mod by_aggregate;
mod by_type;
mod global;
mod hashed;

pub use by_aggregate::{AggregatePartition, AggregateStrategy};
pub use by_type::{TypePartition, TypeStrategy};
pub use global::{GlobalPartition, GlobalStrategy};
pub use hashed::{BucketPartition, HashedStrategy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlitePartitionRead {
    ScanAll,
    ScanType(AggregateType),
    Direct(AggregateId),
    Skip,
}

pub trait SqlitePartitionKey: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

impl<T> SqlitePartitionKey for T where T: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

pub trait SqlitePartitionStrategy: Clone + Send + Sync + 'static {
    type Partition: SqlitePartitionKey;

    fn bootstrap_partitions(&self) -> Vec<Self::Partition> {
        Vec::new()
    }

    fn partition_for_aggregate(&self, aggregate_id: &AggregateId)
        -> Result<Self::Partition, Error>;

    fn read_plan(&self, partition: &Self::Partition) -> SqlitePartitionRead;

    fn read_plan_by_type(
        &self,
        partition: &Self::Partition,
        aggregate_type: &AggregateType,
    ) -> SqlitePartitionRead;
}

pub trait SqliteLocalPartitionStrategy: SqlitePartitionStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error>;

    fn path_for_partition(
        &self,
        root: &Path,
        partition: &Self::Partition,
    ) -> Result<PathBuf, Error>;

    fn discover_partitions(&self, root: &Path) -> Result<Vec<Self::Partition>, Error>;
}

pub trait SqliteSingleRemotePartitionStrategy: SqlitePartitionStrategy {}

pub trait SqliteSqldNamespacedPartitionStrategy: SqlitePartitionStrategy {}

const ENCODED_PATH_PREFIX: &str = "b32-";

pub(super) fn encode_path_component(value: &str) -> String {
    let encoded_value = BASE32_NOPAD.encode(value.as_bytes());
    let mut encoded = String::with_capacity(ENCODED_PATH_PREFIX.len() + encoded_value.len());
    encoded.push_str(ENCODED_PATH_PREFIX);
    encoded.push_str(&encoded_value);
    encoded
}

pub(super) fn decode_path_component(value: &str) -> Result<String, Error> {
    let encoded = value.strip_prefix(ENCODED_PATH_PREFIX).ok_or_else(|| {
        Error::Configuration(format!("invalid encoded partition path component: {value}"))
    })?;

    let bytes = BASE32_NOPAD.decode(encoded.as_bytes()).map_err(|error| {
        Error::Configuration(format!(
            "invalid encoded partition path component '{value}': {error}"
        ))
    })?;

    String::from_utf8(bytes).map_err(|error| {
        Error::Configuration(format!(
            "invalid utf-8 in encoded partition path component '{value}': {error}"
        ))
    })
}
