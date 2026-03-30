use crate::Error;
use async_trait::async_trait;

use super::super::types::{SqliteDatabaseTarget, SqliteSingleTargetProvisioner};

#[derive(Debug)]
pub struct InMemoryTargetResolver;

impl Default for InMemoryTargetResolver {
    fn default() -> Self {
        Self
    }
}

#[async_trait]
impl SqliteSingleTargetProvisioner for InMemoryTargetResolver {
    async fn ensure_target(&self) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::InMemory)
    }

    async fn existing_target(&self) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::InMemory))
    }
}
