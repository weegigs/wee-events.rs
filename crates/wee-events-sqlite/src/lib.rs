mod database;
mod document_store;
mod error;
mod event_store;
mod projections;

pub use document_store::{Document, DocumentStore};
pub use error::Error;
pub use event_store::SqliteEventStore;
pub use projections::{apply_projection, rebuild_projection};
