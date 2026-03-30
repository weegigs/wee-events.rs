mod backends;
mod partitioning;
mod store;
mod strategies;
mod types;

pub type SqliteEventStore<
    S = strategies::GlobalStrategy,
    C = backends::LocalPartitionCatalog<strategies::GlobalStrategy>,
> = store::EventStore<S, C>;
pub use store::{InMemoryStore, LocalStore, NamedRemoteStore, RemoteStore, SingleRemoteStore};
pub use strategies::{
    AggregatePartition, AggregateStrategy, BucketPartition, GlobalPartition, GlobalStrategy,
    HashedStrategy, LocalPartitionLayout, LocalPartitionStrategy, NamedPartition,
    PartitionByStrategy, PartitionKey, PartitionName, PartitionNamingStrategy, PartitionRead,
    PartitionStrategy, SingleTargetPartitionStrategy, SqldNamespacedPartitionStrategy,
    TypePartition, TypeStrategy,
};
pub use types::{
    DatabaseTarget, NamedTargetProvisioner, PartitionCatalog, SingleTargetProvisioner,
    SqldDefaultProvisioner, SqldNamespacedProvisioner, TursoProvisioner,
};
