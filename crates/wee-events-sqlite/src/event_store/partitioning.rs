use crate::Error;

use super::types::DatabaseTarget;

/// Maps logical partitions to concrete database targets.
///
/// This is a static extension point; catalogs are composed into concrete store
/// types rather than used behind trait objects.
#[allow(async_fn_in_trait)]
pub trait PartitionCatalog<P>: Send + Sync {
    async fn ensure_target_for_partition(&self, partition: &P) -> Result<DatabaseTarget, Error>;

    async fn target_for_existing_partition(
        &self,
        partition: &P,
    ) -> Result<Option<DatabaseTarget>, Error>;

    async fn partitions(&self) -> Result<Vec<P>, Error>;
}
