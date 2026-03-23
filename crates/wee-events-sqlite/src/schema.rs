pub const SCHEMA_VERSION: u32 = 2;

const EVENTS_DDL: &str = "
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT    NOT NULL CHECK(length(event_id) = 26),
    aggregate_type  TEXT    NOT NULL,
    aggregate_key   TEXT    NOT NULL,
    event_type      TEXT    NOT NULL,
    revision        TEXT    NOT NULL CHECK(length(revision) = 26),
    causation_id    TEXT,
    correlation_id  TEXT,
    encoding        TEXT    NOT NULL,
    data            BLOB    NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_aggregate
    ON events (aggregate_type, aggregate_key, revision);
";

const DOCUMENTS_DDL: &str = "
CREATE TABLE IF NOT EXISTS documents (
    collection  TEXT NOT NULL,
    key         TEXT NOT NULL,
    revision    TEXT NOT NULL,
    data        TEXT NOT NULL CHECK(json_valid(data)),
    PRIMARY KEY (collection, key)
);
";

/// Configures pragmas (WAL mode, busy timeout, foreign keys) and runs
/// schema migrations. Call once when opening a connection.
pub fn configure_and_migrate(conn: &rusqlite::Connection) -> Result<(), crate::Error> {
    let journal_mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if journal_mode != "memory" {
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        let confirmed: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if confirmed != "wal" {
            return Err(crate::Error::Data(format!(
                "expected WAL journal mode, got: {confirmed}"
            )));
        }
    }
    conn.busy_timeout(std::time::Duration::from_millis(5000))?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;
    migrate(conn)?;
    Ok(())
}

/// Apply schema and advance `user_version` to `SCHEMA_VERSION`.
///
/// Handles incremental migration:
///   - version 0 → 2: fresh database, apply events + documents DDL
///   - version 1 → 2: existing database with events, add documents DDL
pub fn migrate(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;

    if current >= SCHEMA_VERSION {
        return Ok(());
    }

    let mut ddl = String::new();

    if current < 1 {
        ddl.push_str(EVENTS_DDL);
    }
    if current < 2 {
        ddl.push_str(DOCUMENTS_DDL);
    }

    conn.execute_batch(&format!(
        "BEGIN;
         {ddl}
         PRAGMA user_version = {SCHEMA_VERSION};
         COMMIT;"
    ))?;

    Ok(())
}
