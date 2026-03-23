use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use wee_events::Revision;

use crate::Error;

/// A projected document stored in the `documents` table.
#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub revision: Revision,
    pub data: serde_json::Value,
}

/// Pure persistence for projected state. Stores JSON documents keyed by
/// (collection, key) with revision tracking for stale-write prevention.
///
/// Knows nothing about subscribers or broadcast — the owning actor decides
/// when to notify downstream consumers.
///
/// All methods are synchronous — they acquire a `std::sync::Mutex` and
/// perform blocking SQLite operations. The owning actor calls these inline.
pub struct DocumentStore {
    conn: Arc<Mutex<Connection>>,
}

impl DocumentStore {
    pub(crate) fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Inserts or updates a document. The upsert is revision-aware: a stale
    /// revision (not greater than the current stored revision) is silently
    /// ignored. Returns the number of rows affected (0 = stale/no-op, 1 = written).
    ///
    /// Stale-write prevention relies on lexicographic string comparison of
    /// revisions. This is correct for ULID-based revisions (the only generator
    /// in use), which are monotonically increasing and lexicographically ordered.
    pub fn upsert(
        &self,
        collection: &str,
        key: &str,
        revision: &Revision,
        data: &serde_json::Value,
    ) -> Result<usize, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let json_str = serde_json::to_string(data).map_err(|e| Error::Data(e.to_string()))?;

        let rows = conn.execute(
            "INSERT INTO documents (collection, key, revision, data)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (collection, key) DO UPDATE
                 SET revision = excluded.revision, data = excluded.data
                 WHERE excluded.revision > documents.revision",
            params![collection, key, revision.as_str(), json_str],
        )?;

        Ok(rows)
    }

    /// Retrieves a single document by collection and key.
    pub fn get(&self, collection: &str, key: &str) -> Result<Option<Document>, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let result = conn
            .query_row(
                "SELECT key, revision, data FROM documents
                 WHERE collection = ?1 AND key = ?2",
                params![collection, key],
                |row| {
                    let key: String = row.get(0)?;
                    let revision: String = row.get(1)?;
                    let data: String = row.get(2)?;
                    Ok((key, revision, data))
                },
            )
            .optional()?;

        match result {
            Some((key, revision, data)) => {
                let value: serde_json::Value =
                    serde_json::from_str(&data).map_err(|e| Error::Data(e.to_string()))?;
                Ok(Some(Document {
                    key,
                    revision: Revision::new(revision),
                    data: value,
                }))
            }
            None => Ok(None),
        }
    }

    /// Lists all documents in a collection.
    pub fn list(&self, collection: &str) -> Result<Vec<Document>, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let mut stmt =
            conn.prepare("SELECT key, revision, data FROM documents WHERE collection = ?1")?;

        let rows = stmt
            .query_map(params![collection], |row| {
                let key: String = row.get(0)?;
                let revision: String = row.get(1)?;
                let data: String = row.get(2)?;
                Ok((key, revision, data))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter()
            .map(|(key, revision, data)| {
                let value: serde_json::Value =
                    serde_json::from_str(&data).map_err(|e| Error::Data(e.to_string()))?;
                Ok(Document {
                    key,
                    revision: Revision::new(revision),
                    data: value,
                })
            })
            .collect()
    }

    /// Deletes a document. Returns true if a document was removed.
    pub fn delete(&self, collection: &str, key: &str) -> Result<bool, Error> {
        let conn = self.conn.lock().map_err(|e| Error::Data(e.to_string()))?;

        let rows = conn.execute(
            "DELETE FROM documents WHERE collection = ?1 AND key = ?2",
            params![collection, key],
        )?;

        Ok(rows > 0)
    }
}
