mod database;
mod document_store;
mod error;
mod event_store;
mod projections;

pub use document_store::{Document, DocumentStore};
pub use error::Error;
pub use event_store::{
    AggregatePartition, AggregateStrategy, BucketPartition, GlobalPartition, GlobalStrategy,
    HashedStrategy, NamedPartition, PartitionByStrategy, SqliteDatabaseTarget, SqliteEventStore,
    SqliteInMemoryStore, SqliteLocalPartitionStrategy, SqliteLocalStore, SqliteNamedRemoteStore,
    SqliteNamedTargetProvisioner, SqlitePartitionCatalog, SqlitePartitionKey,
    SqlitePartitionNamingStrategy, SqlitePartitionRead, SqlitePartitionStrategy, SqliteRemoteStore,
    SqliteSingleRemotePartitionStrategy, SqliteSingleRemoteStore, SqliteSingleTargetProvisioner,
    SqliteSqldDefaultProvisioner, SqliteSqldNamespacedPartitionStrategy,
    SqliteSqldNamespacedProvisioner, SqliteTursoProvisioner, TypePartition, TypeStrategy,
};
pub use projections::{apply_projection, rebuild_projection};
