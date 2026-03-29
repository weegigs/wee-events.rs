use std::sync::Arc;

use async_trait::async_trait;
use wee_events::AggregateId;

use crate::Error;

use super::super::partitioning::SqlitePartitionCatalog;
use super::super::types::{
    SqliteDatabaseTarget, SqlitePartition, SqlitePartitioning, SqlitePartitioningStrategy,
    SqliteTargetProvisioner,
};

pub(crate) struct ResolverPartitionCatalog {
    strategy: SqlitePartitioningStrategy,
    provisioner: Arc<dyn SqliteTargetProvisioner>,
}

impl ResolverPartitionCatalog {
    pub(crate) fn new(
        partitioning: SqlitePartitioning,
        provisioner: Arc<dyn SqliteTargetProvisioner>,
    ) -> Self {
        Self {
            strategy: partitioning.resolve(SqlitePartitioningStrategy::Aggregate),
            provisioner,
        }
    }
}

#[async_trait]
impl SqlitePartitionCatalog for ResolverPartitionCatalog {
    fn partition_for_aggregate(
        &self,
        aggregate_id: &AggregateId,
    ) -> Result<SqlitePartition, Error> {
        self.strategy.partition_for_aggregate(aggregate_id)
    }

    async fn ensure_target_for_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        self.provisioner
            .ensure_target_for_partition(partition)
            .await
    }

    async fn target_for_existing_partition(
        &self,
        partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        self.provisioner
            .target_for_existing_partition(partition)
            .await
    }

    async fn partitions(&self) -> Result<Vec<SqlitePartition>, Error> {
        self.provisioner.partitions().await
    }
}
