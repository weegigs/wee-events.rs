use std::marker::PhantomData;

use crate::Error;
use async_trait::async_trait;

use super::super::types::{SqliteDatabaseTarget, SqliteTargetProvisioner};

#[derive(Debug)]
pub struct InMemoryTargetResolver<P> {
    _marker: PhantomData<P>,
}

impl<P> Default for InMemoryTargetResolver<P> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P> SqliteTargetProvisioner<P> for InMemoryTargetResolver<P>
where
    P: Send + Sync + 'static,
{
    async fn ensure_target_for_partition(
        &self,
        _partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error> {
        Ok(SqliteDatabaseTarget::InMemory)
    }

    async fn target_for_existing_partition(
        &self,
        _partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error> {
        Ok(Some(SqliteDatabaseTarget::InMemory))
    }
}
