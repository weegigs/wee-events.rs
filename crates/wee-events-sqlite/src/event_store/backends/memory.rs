use crate::Error;

use super::super::types::{DatabaseTarget, SingleTargetProvisioner};

#[derive(Debug)]
pub struct InMemoryTargetResolver;

impl Default for InMemoryTargetResolver {
    fn default() -> Self {
        Self
    }
}

impl SingleTargetProvisioner for InMemoryTargetResolver {
    async fn ensure_target(&self) -> Result<DatabaseTarget, Error> {
        Ok(DatabaseTarget::InMemory)
    }

    async fn existing_target(&self) -> Result<Option<DatabaseTarget>, Error> {
        Ok(Some(DatabaseTarget::InMemory))
    }
}
