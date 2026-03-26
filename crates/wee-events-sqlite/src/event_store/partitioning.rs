use async_trait::async_trait;
use wee_events::AggregateId;

use crate::Error;

use super::types::{SqliteDatabaseTarget, SqlitePartition};

/// Maps aggregates to partitions and partitions to concrete database targets.
///
/// Implementations own all backend-specific knowledge about how a requested
/// partitioning policy is realized.
#[async_trait]
pub trait SqlitePartitionCatalog: Send + Sync {
    fn partition_for_aggregate(&self, aggregate_id: &AggregateId)
        -> Result<SqlitePartition, Error>;

    async fn ensure_target_for_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error>;

    async fn target_for_existing_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error>;

    async fn partitions(&self) -> Result<Vec<SqlitePartition>, Error>;
}
