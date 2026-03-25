use std::path::Path;
use std::time::Duration;

use libsql::{Builder, Connection};

use crate::Error;

const SCHEMA_VERSION: u32 = 2;

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

pub(crate) async fn open_local_connection(path: impl AsRef<Path>) -> Result<Connection, Error> {
    let db = Builder::new_local(path).build().await?;
    let conn = db.connect()?;
    prepare_connection(&conn).await?;
    Ok(conn)
}

pub(crate) async fn open_in_memory_connection() -> Result<Connection, Error> {
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;
    prepare_connection(&conn).await?;
    Ok(conn)
}

async fn prepare_connection(conn: &Connection) -> Result<(), Error> {
    let journal_mode = query_required_string(conn, "PRAGMA journal_mode").await?;
    if journal_mode != "memory" {
        let confirmed = query_required_string(conn, "PRAGMA journal_mode=WAL").await?;
        if confirmed != "wal" {
            return Err(Error::Configuration(format!(
                "expected WAL journal mode, got: {confirmed}"
            )));
        }
    }

    conn.busy_timeout(Duration::from_millis(5000))?;
    conn.execute_batch("PRAGMA foreign_keys=ON;").await?;
    migrate(conn).await?;
    Ok(())
}

async fn migrate(conn: &Connection) -> Result<(), Error> {
    let current = query_required_u32(conn, "PRAGMA user_version").await?;

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
    ))
    .await?;

    Ok(())
}

async fn query_required_string(conn: &Connection, sql: &str) -> Result<String, Error> {
    let mut rows = conn.query(sql, ()).await?;
    let Some(row) = rows.next().await? else {
        return Err(Error::Configuration(format!(
            "expected row when querying pragma: {sql}"
        )));
    };

    row.get(0).map_err(Into::into)
}

async fn query_required_u32(conn: &Connection, sql: &str) -> Result<u32, Error> {
    let mut rows = conn.query(sql, ()).await?;
    let Some(row) = rows.next().await? else {
        return Err(Error::Configuration(format!(
            "expected row when querying pragma: {sql}"
        )));
    };

    row.get(0).map_err(Into::into)
}
