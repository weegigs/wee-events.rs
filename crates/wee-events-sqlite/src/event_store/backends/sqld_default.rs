use crate::Error;

use super::super::store::SqldDefaultBackend;
use super::super::strategies::{PartitionStrategy, SingleTargetPartitionStrategy};
use super::super::types::SqldDefaultProvisioner;
use super::{BackendBinding, SingleTargetCatalog};

impl<S, R> BackendBinding<S> for SqldDefaultBackend<R>
where
    S: PartitionStrategy + SingleTargetPartitionStrategy,
    R: SqldDefaultProvisioner,
{
    type Catalog = SingleTargetCatalog<S::Partition, R>;

    fn into_catalog(self, _strategy: &S) -> Result<Self::Catalog, Error> {
        Ok(SingleTargetCatalog::new(self.provisioner))
    }
}
