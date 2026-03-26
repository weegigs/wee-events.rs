use crate::Error;
use async_trait::async_trait;

use super::super::types::{SqliteDatabaseTarget, SqlitePartition, SqliteTargetProvisioner};

#[derive(Debug)]
pub(crate) struct InMemoryTargetResolver;

#[async_trait]
impl SqliteTargetProvisioner for InMemoryTargetResolver {
    async fn ensure_target_for_partition(
        &self,
        _partition: &SqlitePartition,
    ) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::InMemory)
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &SqlitePartition,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::InMemory))
    }
}
