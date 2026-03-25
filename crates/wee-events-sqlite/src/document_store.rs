use std::path::Path;

use libsql::Connection;
use tokio::sync::Mutex;
use wee_events::Revision;

use crate::{database, Error};

/// A projected document stored in the `documents` table.
#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub revision: Revision,
    pub data: serde_json::Value,
}

/// Pure persistence for projected state. Stores JSON documents keyed by
/// (collection, key) with revision tracking for stale-write prevention.
pub struct DocumentStore {
    conn: Mutex<Connection>,
}

impl DocumentStore {
    /// Opens (or creates) the database at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            conn: Mutex::new(database::open_document_store_local_connection(path).await?),
        })
    }

    /// Opens an in-memory database. Useful for testing.
    pub async fn open_in_memory() -> Result<Self, Error> {
        Ok(Self {
            conn: Mutex::new(database::open_document_store_in_memory_connection().await?),
        })
    }

    /// Inserts or updates a document. The upsert is revision-aware: a stale
    /// revision (not greater than the current stored revision) is silently
    /// ignored. Returns the number of rows affected (0 = stale/no-op, 1 = written).
    pub async fn upsert(
        &self,
        collection: &str,
        key: &str,
        revision: &Revision,
        data: &serde_json::Value,
    ) -> Result<u64, Error> {
        let conn = self.conn.lock().await;
        let json_str = serde_json::to_string(data)?;

        conn.execute(
            "INSERT INTO documents (collection, key, revision, data)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (collection, key) DO UPDATE
                 SET revision = excluded.revision, data = excluded.data
                 WHERE excluded.revision > documents.revision",
            (collection, key, revision.as_str(), json_str),
        )
        .await
        .map_err(Into::into)
    }

    /// Retrieves a single document by collection and key.
    pub async fn get(&self, collection: &str, key: &str) -> Result<Option<Document>, Error> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT key, revision, data FROM documents
                 WHERE collection = ?1 AND key = ?2",
                (collection, key),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let key: String = row.get(0)?;
        let revision: String = row.get(1)?;
        let data: String = row.get(2)?;
        let value = serde_json::from_str(&data)?;

        Ok(Some(Document {
            key,
            revision: Revision::new(revision),
            data: value,
        }))
    }

    /// Lists all documents in a collection.
    pub async fn list(&self, collection: &str) -> Result<Vec<Document>, Error> {
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT key, revision, data FROM documents WHERE collection = ?1",
                [collection],
            )
            .await?;

        let mut documents = Vec::new();
        while let Some(row) = rows.next().await? {
            let key: String = row.get(0)?;
            let revision: String = row.get(1)?;
            let data: String = row.get(2)?;
            let value = serde_json::from_str(&data)?;

            documents.push(Document {
                key,
                revision: Revision::new(revision),
                data: value,
            });
        }

        Ok(documents)
    }

    /// Deletes a document. Returns true if a document was removed.
    pub async fn delete(&self, collection: &str, key: &str) -> Result<bool, Error> {
        let conn = self.conn.lock().await;
        let rows = conn
            .execute(
                "DELETE FROM documents WHERE collection = ?1 AND key = ?2",
                (collection, key),
            )
            .await?;

        Ok(rows > 0)
    }
}
