mod database;
mod document_store;
mod error;
mod event_store;
mod projections;

pub use document_store::{Document, DocumentStore};
pub use error::Error;
pub use event_store::{
    SqliteBackend, SqliteDatabaseTarget, SqliteEventStore, SqlitePartition, SqlitePartitionCatalog,
    SqlitePartitioning, SqlitePartitioningStrategy, SqliteTargetProvisioner,
};
pub use projections::{apply_projection, rebuild_projection};
