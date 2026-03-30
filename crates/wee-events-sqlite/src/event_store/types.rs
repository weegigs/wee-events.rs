use std::path::PathBuf;

use crate::Error;

pub use super::partitioning::PartitionCatalog;
use super::strategies::PartitionName;

/// A concrete database target for a partition.
#[derive(Clone, PartialEq, Eq)]
pub enum DatabaseTarget {
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

impl std::fmt::Debug for DatabaseTarget {
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

#[allow(async_fn_in_trait)]
pub trait SingleTargetProvisioner: Send + Sync {
    /// Returns the single concrete target, creating or provisioning it if needed.
    ///
    /// Use this for backends where every logical partition is realized by the
    /// same external target, such as `sqld_default` and `turso`.
    async fn ensure_target(&self) -> Result<DatabaseTarget, Error>;

    /// Returns the single target if it already exists.
    ///
    /// This should avoid creating new storage as a side effect.
    async fn existing_target(&self) -> Result<Option<DatabaseTarget>, Error>;
}

#[allow(async_fn_in_trait)]
pub trait NamedTargetProvisioner: Send + Sync {
    /// Returns a target for a stable partition name, creating or provisioning it if needed.
    ///
    /// The partition name is a logical identifier supplied by
    /// `PartitionNamingStrategy`. Backends are free to translate that name
    /// into whatever concrete addressing scheme they need, such as a namespace.
    async fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<DatabaseTarget, Error>;

    /// Returns a target for a stable partition name if it already exists.
    ///
    /// This should avoid creating new storage as a side effect.
    async fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> Result<Option<DatabaseTarget>, Error>;

    /// Enumerates partition names known to this provisioner.
    async fn names(&self) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }
}

/// Provisioner for a single default sqld database.
pub trait SqldDefaultProvisioner: SingleTargetProvisioner {}

/// Provisioner for sqld databases addressed by logical partition name.
pub trait SqldNamespacedProvisioner: NamedTargetProvisioner {}

/// Provisioner for a single Turso database.
pub trait TursoProvisioner: SingleTargetProvisioner {}
