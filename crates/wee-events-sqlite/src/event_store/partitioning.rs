use async_trait::async_trait;

use crate::Error;

use super::types::SqliteDatabaseTarget;

/// Maps logical partitions to concrete database targets.
#[async_trait]
pub trait SqlitePartitionCatalog<P>: Send + Sync {
    async fn ensure_target_for_partition(
        &self,
        partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error>;

    async fn target_for_existing_partition(
        &self,
        partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error>;

    async fn partitions(&self) -> Result<Vec<P>, Error>;
}
