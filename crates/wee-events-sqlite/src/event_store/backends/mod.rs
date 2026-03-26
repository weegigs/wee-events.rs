mod local;
mod memory;
mod remote;

pub(super) use local::LocalPartitionCatalog;
pub(super) use memory::InMemoryTargetResolver;
pub(super) use remote::ResolverPartitionCatalog;
