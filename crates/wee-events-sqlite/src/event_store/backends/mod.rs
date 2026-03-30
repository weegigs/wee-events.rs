mod local;
mod memory;
mod remote;
mod sqld_default;
mod sqld_namespaced;
mod turso;

use crate::Error;

use super::partitioning::PartitionCatalog;
use super::strategies::PartitionStrategy;

pub(super) use local::LocalPartitionCatalog;
pub(super) use memory::InMemoryTargetResolver;
pub(super) use remote::{NamedTargetCatalog, SingleTargetCatalog};

#[doc(hidden)]
pub trait BackendBinding<S>: Sized
where
    S: PartitionStrategy,
{
    type Catalog: PartitionCatalog<S::Partition>;

    fn into_catalog(self, strategy: &S) -> Result<Self::Catalog, Error>;
}
