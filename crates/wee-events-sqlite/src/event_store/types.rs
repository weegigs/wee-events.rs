use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use wee_events::{AggregateId, AggregateType};

use crate::Error;

pub use super::partitioning::SqlitePartitionCatalog;

/// The concrete storage topology to aim for when partitioning event streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlitePartitioningStrategy {
    Global,
    Type,
    Aggregate,
    Hashed { buckets: u32 },
}

/// Best-effort partitioning policy for an event store backend.
///
/// `Strict` requests an exact strategy. `Auto` lets the backend choose its
/// preferred strategy for the underlying storage.
///
/// This is a storage-topology tuning policy, not a correctness guarantee.
/// Backends always do their best to honor the requested partitioning while
/// preserving event-store semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlitePartitioning {
    Auto,
    Strict(SqlitePartitioningStrategy),
}

impl SqlitePartitioning {
    pub(crate) fn resolve(
        self,
        auto_strategy: SqlitePartitioningStrategy,
    ) -> SqlitePartitioningStrategy {
        match self {
            Self::Auto => auto_strategy,
            Self::Strict(strategy) => strategy,
        }
    }
}

/// A concrete database target for a partition.
#[derive(Clone, PartialEq, Eq)]
pub enum SqliteDatabaseTarget {
    InMemory,
    Local(PathBuf),
    Remote {
        url: String,
        auth_token: String,
        namespace: Option<String>,
    },
}

impl std::fmt::Debug for SqliteDatabaseTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory => f.write_str("InMemory"),
            Self::Local(path) => f.debug_tuple("Local").field(path).finish(),
            Self::Remote { url, namespace, .. } => f
                .debug_struct("Remote")
                .field("url", url)
                .field("auth_token", &"[REDACTED]")
                .field("namespace", namespace)
                .finish(),
        }
    }
}

pub enum SqliteBackend {
    InMemory,
    Local(PathBuf),
    Remote(Arc<dyn SqliteTargetProvisioner>),
}

impl SqliteBackend {
    pub fn in_memory() -> Self {
        Self::InMemory
    }

    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local(path.into())
    }

    pub fn remote(provisioner: impl SqliteTargetProvisioner + 'static) -> Self {
        Self::Remote(Arc::new(provisioner))
    }
}

/// A logical partition in the store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SqlitePartition {
    Single,
    AggregateType(AggregateType),
    Aggregate(AggregateId),
    Hashed(u32),
}

impl SqlitePartitioningStrategy {
    pub(crate) fn partition_for_aggregate(
        self,
        aggregate_id: &AggregateId,
    ) -> Result<SqlitePartition, Error> {
        match self {
            Self::Global => Ok(SqlitePartition::Single),
            Self::Type => Ok(SqlitePartition::AggregateType(
                aggregate_id.aggregate_type.clone(),
            )),
            Self::Aggregate => Ok(SqlitePartition::Aggregate(aggregate_id.clone())),
            Self::Hashed { buckets } => {
                if buckets == 0 {
                    return Err(Error::Configuration(
                        "hashed partitioning requires at least one bucket".to_string(),
                    ));
                }

                Ok(SqlitePartition::Hashed(
                    hash_aggregate_id(aggregate_id) % buckets,
                ))
            }
        }
    }
}

fn hash_aggregate_id(aggregate_id: &AggregateId) -> u32 {
    // Stable FNV-1a so bucket assignment is reproducible across processes.
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

#[async_trait]
pub trait SqliteTargetProvisioner: Send + Sync {
    /// Returns a target for a partition, creating or provisioning it if needed.
    async fn ensure_target_for_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error>;

    /// Returns a target for an already-existing partition.
    ///
    /// This should avoid creating new storage as a side effect.
    async fn target_for_existing_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error>;

    /// Enumerates partitions known to this provisioner.
    async fn partitions(&self) -> Result<Vec<SqlitePartition>, Error> {
        Ok(Vec::new())
    }
}
