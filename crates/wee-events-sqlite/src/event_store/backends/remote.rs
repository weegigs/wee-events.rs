use async_trait::async_trait;

use crate::Error;

use super::super::partitioning::SqlitePartitionCatalog;
use super::super::types::{SqliteDatabaseTarget, SqliteTargetProvisioner};

pub struct ResolverPartitionCatalog<P, R> {
    provisioner: R,
    _marker: std::marker::PhantomData<P>,
}

impl<P, R> ResolverPartitionCatalog<P, R> {
    pub(crate) fn new(provisioner: R) -> Self {
        Self {
            provisioner,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<P, R> SqlitePartitionCatalog<P> for ResolverPartitionCatalog<P, R>
where
    P: Clone + Send + Sync + 'static,
    R: SqliteTargetProvisioner<P>,
{
    async fn ensure_target_for_partition(
        &self,
        partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error> {
        self.provisioner
            .ensure_target_for_partition(partition)
            .await
    }

    async fn target_for_existing_partition(
        &self,
        partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        self.provisioner
            .target_for_existing_partition(partition)
            .await
    }

    async fn partitions(&self) -> Result<Vec<P>, Error> {
        self.provisioner.partitions().await
    }
}
