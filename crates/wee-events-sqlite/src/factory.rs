use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::documents::DocumentStore;
use crate::{schema, Error, SqliteEventStore};

/// Combined store factory — creates `SqliteEventStore` and `DocumentStore`
/// sharing a single `Arc<Mutex<Connection>>`.
///
/// Both stores are pure persistence. The owning actor decides when/whether
/// to run projections and notify downstream consumers.
pub struct SqliteStore {
    event_store: SqliteEventStore,
    document_store: DocumentStore,
}

impl SqliteStore {
    /// Opens (or creates) the database at `path`, configures pragmas,
    /// runs migrations, and returns both stores sharing one connection.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let conn = Connection::open(path)?;
        Self::build(conn)
    }

    /// Opens an in-memory database. Useful for testing.
    pub fn open_in_memory() -> Result<Self, Error> {
        let conn = Connection::open_in_memory()?;
        Self::build(conn)
    }

    /// Access the event store.
    pub fn events(&self) -> &SqliteEventStore {
        &self.event_store
    }

    /// Access the document store.
    pub fn documents(&self) -> &DocumentStore {
        &self.document_store
    }

    fn build(conn: Connection) -> Result<Self, Error> {
        schema::configure_and_migrate(&conn)?;

        let shared = Arc::new(Mutex::new(conn));
        let event_store = SqliteEventStore::from_shared(Arc::clone(&shared));
        let document_store = DocumentStore::new(shared);

        Ok(Self {
            event_store,
            document_store,
        })
    }
}
