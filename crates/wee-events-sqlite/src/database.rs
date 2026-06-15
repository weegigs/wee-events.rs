use std::path::Path;
use std::time::Duration;

use libsql::{Builder, Connection};

use crate::{Error, event_store::DatabaseTarget};

const EVENT_STORE_SCHEMA_VERSION: u32 = 2;
const DOCUMENT_STORE_SCHEMA_VERSION: u32 = 1;

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

//FIXME: hard-commit to json here...
const DOCUMENTS_DDL: &str = "
CREATE TABLE IF NOT EXISTS documents (
    collection  TEXT NOT NULL,
    key         TEXT NOT NULL,
    revision    TEXT NOT NULL,
    data        TEXT NOT NULL CHECK(json_valid(data)),
    PRIMARY KEY (collection, key)
);
";

const PARTITION_METADATA_DDL: &str = "
CREATE TABLE IF NOT EXISTS _wee_events_partition_metadata (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);
";

pub(crate) async fn open_event_store_connection(
    target: &DatabaseTarget,
) -> Result<Connection, Error> {
    let conn = match target {
        DatabaseTarget::InMemory => {
            let db = Builder::new_local(":memory:").build().await?;
            let conn = db.connect()?;
            prepare_local_connection(&conn).await?;
            conn
        }
        DatabaseTarget::Local(path) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let db = Builder::new_local(path).build().await?;
            let conn = db.connect()?;
            prepare_local_connection(&conn).await?;
            conn
        }
        DatabaseTarget::SqldDefault { url, auth_token }
        | DatabaseTarget::Turso { url, auth_token } => {
            let db = Builder::new_remote(url.clone(), auth_token.clone())
                .build()
                .await?;
            db.connect()?
        }
        DatabaseTarget::SqldNamespace {
            url,
            auth_token,
            namespace,
        } => {
            let builder =
                Builder::new_remote(url.clone(), auth_token.clone()).namespace(namespace.clone());
            let db = builder.build().await?;
            db.connect()?
        }
    };

    migrate_event_store(&conn).await?;
    Ok(conn)
}

pub(crate) async fn open_document_store_local_connection(
    path: impl AsRef<Path>,
) -> Result<Connection, Error> {
    let db = Builder::new_local(path).build().await?;
    let conn = db.connect()?;
    prepare_local_connection(&conn).await?;
    migrate_document_store(&conn).await?;
    Ok(conn)
}

pub(crate) async fn open_document_store_in_memory_connection() -> Result<Connection, Error> {
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;
    prepare_local_connection(&conn).await?;
    migrate_document_store(&conn).await?;
    Ok(conn)
}

async fn prepare_local_connection(conn: &Connection) -> Result<(), Error> {
    let journal_mode = query_required_string(conn, "PRAGMA journal_mode").await?;
    if journal_mode != "memory" && journal_mode != "wal" {
        let confirmed = query_required_string(conn, "PRAGMA journal_mode=WAL").await?;
        if confirmed != "wal" {
            return Err(Error::Configuration(format!(
                "expected WAL journal mode, got: {confirmed}"
            )));
        }
    }

    conn.busy_timeout(Duration::from_secs(30))?; //TODO: const
    conn.execute_batch("PRAGMA foreign_keys=ON;").await?;
    Ok(())
}

async fn migrate_event_store(conn: &Connection) -> Result<(), Error> {
    migrate_schema(
        conn,
        "_wee_events_event_store_user_version",
        EVENT_STORE_SCHEMA_VERSION,
        &[(1, EVENTS_DDL), (2, PARTITION_METADATA_DDL)],
    )
    .await
}

async fn migrate_document_store(conn: &Connection) -> Result<(), Error> {
    migrate_schema(
        conn,
        "_wee_events_document_store_user_version",
        DOCUMENT_STORE_SCHEMA_VERSION,
        &[(1, DOCUMENTS_DDL)],
    )
    .await
}

async fn migrate_schema(
    conn: &Connection,
    version_key: &str,
    target_version: u32,
    migrations: &[(u32, &str)],
) -> Result<(), Error> {
    let current = current_schema_version(conn, version_key).await?;

    if current >= target_version {
        return Ok(());
    }

    let mut ddl = String::new();
    for (version, migration) in migrations {
        if current < *version {
            ddl.push_str(migration);
        }
    }

    // DDL must use execute_batch (multiple statements, no parameters).
    // The version upsert uses a parameterized query to avoid interpolation.
    conn.execute_batch(&format!("BEGIN;{ddl}")).await?;

    conn.execute(
        "INSERT INTO _wee_events_schema_versions (schema_name, version)
         VALUES (?1, ?2)
         ON CONFLICT(schema_name) DO UPDATE SET version = excluded.version",
        (version_key, target_version),
    )
    .await?;

    conn.execute_batch("COMMIT;").await?;

    Ok(())
}

async fn current_schema_version(conn: &Connection, version_key: &str) -> Result<u32, Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _wee_events_schema_versions (
             schema_name TEXT PRIMARY KEY,
             version     INTEGER NOT NULL
         );",
    )
    .await?;

    let mut rows = conn
        .query(
            "SELECT version FROM _wee_events_schema_versions WHERE schema_name = ?1",
            [version_key],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(0);
    };

    row.get(0).map_err(Into::into)
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

pub(crate) async fn ensure_partition_name(
    conn: &Connection,
    logical_name: &str,
) -> Result<(), Error> {
    conn.execute(
        "INSERT OR IGNORE INTO _wee_events_partition_metadata (key, value)
         VALUES ('logical_name', ?1)",
        [logical_name],
    )
    .await?;

    let Some(recorded_name) = load_partition_name(conn).await? else {
        return Err(Error::Internal(
            "partition metadata insert succeeded without recording logical_name".to_string(),
        ));
    };
    if recorded_name != logical_name {
        return Err(Error::Configuration(format!(
            "logical partition name mismatch: target is recorded as '{recorded_name}' but was opened for '{logical_name}'"
        )));
    }

    Ok(())
}

pub(crate) async fn load_partition_name(conn: &Connection) -> Result<Option<String>, Error> {
    let mut rows = conn
        .query(
            "SELECT value FROM _wee_events_partition_metadata WHERE key = 'logical_name'",
            (),
        )
        .await?;

    let Some(row) = rows.next().await? else {
        return Ok(None);
    };

    row.get(0).map_err(Into::into)
}
