use crate::Error;

use super::super::store::TursoBackend;
use super::super::strategies::PartitionNamingStrategy;
use super::super::types::TursoProvisioner;
use super::{BackendBinding, NamedTargetCatalog};

impl<S, R> BackendBinding<S> for TursoBackend<R>
where
    S: PartitionNamingStrategy,
    R: TursoProvisioner,
{
    type Catalog = NamedTargetCatalog<S, R>;

    fn into_catalog(self, strategy: &S) -> Result<Self::Catalog, Error> {
        Ok(NamedTargetCatalog::new(strategy.clone(), self.provisioner))
    }
}
