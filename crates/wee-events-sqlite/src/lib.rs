mod database;
mod document_store;
mod error;
mod event_store;
mod projections;

pub use document_store::{Document, DocumentStore};
pub use error::Error;
pub use event_store::{
    AggregatePartition, AggregateStrategy, BucketPartition, DatabaseTarget, GlobalPartition,
    GlobalStrategy, HashedStrategy, InMemoryStore, LocalPartitionLayout, LocalPartitionStrategy,
    LocalStore, NamedPartition, NamedRemoteStore, NamedTargetProvisioner, PartitionByStrategy,
    PartitionCatalog, PartitionKey, PartitionName, PartitionNamingStrategy, PartitionRead,
    PartitionStrategy, RemoteStore, SingleRemoteStore, SingleTargetPartitionStrategy,
    SingleTargetProvisioner, SqldDefaultProvisioner, SqldNamespacedPartitionStrategy,
    SqldNamespacedProvisioner, SqliteEventStore, TursoProvisioner, TypePartition, TypeStrategy,
};
pub use projections::{apply_projection, rebuild_projection};
