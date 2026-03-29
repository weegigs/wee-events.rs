use std::path::PathBuf;

use async_trait::async_trait;

use crate::Error;

use super::super::partitioning::SqlitePartitionCatalog;
use super::super::strategies::SqliteLocalPartitionStrategy;
use super::super::types::SqliteDatabaseTarget;

#[derive(Debug)]
pub struct LocalPartitionCatalog<S> {
    root: PathBuf,
    strategy: S,
}

impl<S> LocalPartitionCatalog<S>
where
    S: SqliteLocalPartitionStrategy,
{
    pub(crate) fn new(root: PathBuf, strategy: S) -> Result<Self, Error> {
        strategy.initialize_root(&root)?;
        Ok(Self { root, strategy })
    }
}

#[async_trait]
impl<S> SqlitePartitionCatalog<S::Partition> for LocalPartitionCatalog<S>
where
    S: SqliteLocalPartitionStrategy,
{
    async fn ensure_target_for_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        let path = self.strategy.path_for_partition(&self.root, partition)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(SqliteDatabaseTarget::Local(path))
    }

    async fn target_for_existing_partition(
        &self,
        partition: &S::Partition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        let path = self.strategy.path_for_partition(&self.root, partition)?;
        Ok(path.exists().then_some(SqliteDatabaseTarget::Local(path)))
    }

    async fn partitions(&self) -> Result<Vec<S::Partition>, Error> {
        self.strategy.discover_partitions(&self.root)
    }
}
