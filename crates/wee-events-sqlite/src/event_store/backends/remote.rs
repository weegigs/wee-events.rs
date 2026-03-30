use async_trait::async_trait;

use crate::Error;

use super::super::partitioning::SqlitePartitionCatalog;
use super::super::strategies::SqlitePartitionNamingStrategy;
use super::super::types::{
    SqliteDatabaseTarget, SqliteNamedTargetProvisioner, SqliteSingleTargetProvisioner,
};

pub struct SingleTargetCatalog<P, R> {
    provisioner: R,
    _marker: std::marker::PhantomData<P>,
}

impl<P, R> SingleTargetCatalog<P, R> {
    pub(crate) fn new(provisioner: R) -> Self {
        Self {
            provisioner,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<P, R> SqlitePartitionCatalog<P> for SingleTargetCatalog<P, R>
where
    P: Clone + Send + Sync + 'static,
    R: SqliteSingleTargetProvisioner,
{
    async fn ensure_target_for_partition(
        &self,
        _partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error> {
        self.provisioner.ensure_target().await
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        self.provisioner.existing_target().await
    }

    async fn partitions(&self) -> Result<Vec<P>, Error> {
        Ok(Vec::new())
    }
}

pub struct NamedTargetCatalog<S, R> {
    strategy: S,
    provisioner: R,
}

impl<S, R> NamedTargetCatalog<S, R> {
    pub(crate) fn new(strategy: S, provisioner: R) -> Self {
        Self {
            strategy,
            provisioner,
        }
    }
}

#[async_trait]
impl<S, R> SqlitePartitionCatalog<S::Partition> for NamedTargetCatalog<S, R>
where
    S: SqlitePartitionNamingStrategy,
    R: SqliteNamedTargetProvisioner,
{
    async fn ensure_target_for_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        self.provisioner
            .ensure_target_for_name(self.strategy.partition_name(partition))
            .await
    }

    async fn target_for_existing_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        self.provisioner
            .target_for_existing_name(self.strategy.partition_name(partition))
            .await
    }

    async fn partitions(&self) -> Result<Vec<S::Partition>, Error> {
        let names = self.provisioner.names().await?;
        let mut partitions = Vec::with_capacity(names.len());
        for name in names {
            partitions.push(self.strategy.partition_from_name(&name)?);
        }
        partitions.extend(
            self.strategy
                .bootstrap_partitions()
                .into_iter()
                .filter(|partition| self.strategy.partition_name(partition).is_none()),
        );
        partitions.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        partitions.dedup();
        Ok(partitions)
    }
}
