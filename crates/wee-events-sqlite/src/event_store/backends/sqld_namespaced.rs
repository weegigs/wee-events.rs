use crate::Error;

use super::super::store::SqldNamespacedBackend;
use super::super::strategies::{PartitionNamingStrategy, SqldNamespacedPartitionStrategy};
use super::super::types::SqldNamespacedProvisioner;
use super::{BackendBinding, NamedTargetCatalog};

impl<S, R> BackendBinding<S> for SqldNamespacedBackend<R>
where
    S: SqldNamespacedPartitionStrategy + PartitionNamingStrategy,
    R: SqldNamespacedProvisioner,
{
    type Catalog = NamedTargetCatalog<S, R>;

    fn into_catalog(self, strategy: &S) -> Result<Self::Catalog, Error> {
        Ok(NamedTargetCatalog::new(strategy.clone(), self.provisioner))
    }
}
