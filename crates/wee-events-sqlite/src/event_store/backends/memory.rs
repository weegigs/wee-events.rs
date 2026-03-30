use crate::Error;

use super::super::store::InMemoryBackend;
use super::super::strategies::{PartitionStrategy, SingleTargetPartitionStrategy};
use super::super::types::{DatabaseTarget, SingleTargetProvisioner};
use super::{BackendBinding, SingleTargetCatalog};

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

impl<S> BackendBinding<S> for InMemoryBackend
where
    S: PartitionStrategy + SingleTargetPartitionStrategy,
{
    type Catalog = SingleTargetCatalog<S::Partition, InMemoryTargetResolver>;

    fn into_catalog(self, _strategy: &S) -> Result<Self::Catalog, Error> {
        Ok(SingleTargetCatalog::new(InMemoryTargetResolver))
    }
}
