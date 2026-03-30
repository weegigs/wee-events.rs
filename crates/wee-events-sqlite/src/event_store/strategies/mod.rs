use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;
use wee_events::{AggregateId, AggregateType};

use crate::Error;

mod by_aggregate;
mod by_type;
mod global;
mod hashed;
mod partition_by;

pub use by_aggregate::{AggregatePartition, AggregateStrategy};
pub use by_type::{TypePartition, TypeStrategy};
pub use global::{GlobalPartition, GlobalStrategy};
pub use hashed::{BucketPartition, HashedStrategy};
pub use partition_by::PartitionByStrategy;

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

/// Adds a stable backend-facing name for a logical partition.
///
/// The name is a logical identifier, not a storage format. Different backends
/// can realize the same partition name differently:
/// - local: partition name -> encoded file path
/// - sqld namespaced: partition name -> namespace
/// - other backends may use the name as an in-process routing key
///
/// A partition name does not imply anything about on-disk filenames.
pub trait SqlitePartitionNamingStrategy: SqlitePartitionStrategy {
    fn partition_name<'a>(&self, partition: &'a Self::Partition) -> Option<&'a str>;

    fn partition_from_name(&self, name: &str) -> Result<Self::Partition, Error>;
}

/// Partition strategy behavior needed by the local filesystem-backed catalog.
///
/// Local storage uses `partition_name()` plus the layout to derive concrete file
/// paths. Strategies do not encode filenames themselves.
pub trait SqliteLocalPartitionStrategy: SqlitePartitionNamingStrategy {
    fn initialize_root(&self, root: &Path) -> Result<(), Error>;

    fn local_partition_layout(&self) -> SqliteLocalPartitionLayout;
}

pub trait SqliteSingleRemotePartitionStrategy: SqlitePartitionStrategy {}

pub trait SqliteSqldNamespacedPartitionStrategy: SqlitePartitionStrategy {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteLocalPartitionLayout {
    /// A single database file at the configured root path.
    SingleDatabase,
    /// One database file per logical partition name.
    NamedDatabases,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamedPartition<T> {
    name: String,
    key: T,
}

impl<T> NamedPartition<T> {
    /// Creates a partition with both a stable backend-facing name and a typed key.
    pub fn new(name: impl Into<String>, key: T) -> Self {
        Self {
            name: name.into(),
            key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> &T {
        &self.key
    }

    pub fn into_key(self) -> T {
        self.key
    }
}
