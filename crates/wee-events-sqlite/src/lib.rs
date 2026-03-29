mod database;
mod document_store;
mod error;
mod event_store;
mod projections;

pub use document_store::{Document, DocumentStore};
pub use error::Error;
pub use event_store::{
    AggregatePartition, AggregateStrategy, BucketPartition, GlobalPartition, GlobalStrategy,
    HashedStrategy, SqliteDatabaseTarget, SqliteEventStore, SqliteInMemoryStore,
    SqliteLocalPartitionStrategy, SqliteLocalStore, SqlitePartitionCatalog, SqlitePartitionKey,
    SqlitePartitionRead, SqlitePartitionStrategy, SqliteRemoteStore,
    SqliteSingleRemotePartitionStrategy, SqliteSqldDefaultProvisioner,
    SqliteSqldNamespacedPartitionStrategy, SqliteSqldNamespacedProvisioner,
    SqliteTargetProvisioner, SqliteTursoProvisioner, TypePartition, TypeStrategy,
};
pub use projections::{apply_projection, rebuild_projection};
