use std::path::PathBuf;

use async_trait::async_trait;

use crate::Error;

pub use super::partitioning::SqlitePartitionCatalog;

/// A concrete database target for a partition.
#[derive(Clone, PartialEq, Eq)]
pub enum SqliteDatabaseTarget {
    InMemory,
    Local(PathBuf),
    SqldDefault {
        url: String,
        auth_token: String,
    },
    SqldNamespace {
        url: String,
        auth_token: String,
        namespace: String,
    },
    Turso {
        url: String,
        auth_token: String,
    },
}

impl std::fmt::Debug for SqliteDatabaseTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory => f.write_str("InMemory"),
            Self::Local(path) => f.debug_tuple("Local").field(path).finish(),
            Self::SqldDefault { url, .. } => f
                .debug_struct("SqldDefault")
                .field("url", url)
                .field("auth_token", &"[REDACTED]")
                .finish(),
            Self::SqldNamespace { url, namespace, .. } => f
                .debug_struct("SqldNamespace")
                .field("url", url)
                .field("auth_token", &"[REDACTED]")
                .field("namespace", namespace)
                .finish(),
            Self::Turso { url, .. } => f
                .debug_struct("Turso")
                .field("url", url)
                .field("auth_token", &"[REDACTED]")
                .finish(),
        }
    }
}

#[async_trait]
pub trait SqliteTargetProvisioner<P>: Send + Sync {
    /// Returns a target for a partition, creating or provisioning it if needed.
    async fn ensure_target_for_partition(
        &self,
        partition: &P,
    ) -> Result<SqliteDatabaseTarget, Error>;

    /// Returns a target for an already-existing partition.
    ///
    /// This should avoid creating new storage as a side effect.
    async fn target_for_existing_partition(
        &self,
        partition: &P,
    ) -> Result<Option<SqliteDatabaseTarget>, Error>;

    /// Enumerates partitions known to this provisioner.
    async fn partitions(&self) -> Result<Vec<P>, Error> {
        Ok(Vec::new())
    }
}

pub trait SqliteSqldDefaultProvisioner<P>: SqliteTargetProvisioner<P> {}

pub trait SqliteSqldNamespacedProvisioner<P>: SqliteTargetProvisioner<P> {}

pub trait SqliteTursoProvisioner<P>: SqliteTargetProvisioner<P> {}
