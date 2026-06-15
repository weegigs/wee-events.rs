use std::future::Future;
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

pub trait SingleTargetProvisioner: Send + Sync {
    /// Returns the single concrete target, creating or provisioning it if needed.
    ///
    /// Use this for backends where every logical partition is realized by the
    /// same external target, such as `sqld_default`.
    fn ensure_target(&self) -> impl Future<Output = Result<DatabaseTarget, Error>> + Send;

    /// Returns the single target if it already exists.
    ///
    /// This should avoid creating new storage as a side effect.
    fn existing_target(&self)
    -> impl Future<Output = Result<Option<DatabaseTarget>, Error>> + Send;
}

pub trait NamedTargetProvisioner: Send + Sync {
    /// Returns a target for a stable partition name, creating or provisioning it if needed.
    ///
    /// The partition name is a logical identifier supplied by
    /// `PartitionNamingStrategy`. Backends are free to translate that name
    /// into whatever concrete addressing scheme they need, such as a namespace.
    fn ensure_target_for_name(
        &self,
        name: PartitionName<'_>,
    ) -> impl Future<Output = Result<DatabaseTarget, Error>> + Send;

    /// Returns a target for a stable partition name if it already exists.
    ///
    /// This should avoid creating new storage as a side effect.
    fn target_for_existing_name(
        &self,
        name: PartitionName<'_>,
    ) -> impl Future<Output = Result<Option<DatabaseTarget>, Error>> + Send;

    /// Enumerates partition names known to this provisioner.
    fn names(&self) -> impl Future<Output = Result<Vec<String>, Error>> + Send {
        async { Ok(Vec::new()) }
    }

    /// Enumerates existing backend targets keyed by their backend-facing names.
    fn named_targets(
        &self,
    ) -> impl Future<Output = Result<Vec<(String, DatabaseTarget)>, Error>> + Send {
        async move {
            let mut targets = Vec::new();
            for name in self.names().await? {
                let Some(target) = self
                    .target_for_existing_name(PartitionName::Named(&name))
                    .await?
                else {
                    continue;
                };
                targets.push((name, target));
            }
            Ok(targets)
        }
    }
}

/// Provisioner for a single default sqld database.
pub trait SqldDefaultProvisioner: SingleTargetProvisioner {}

/// Provisioner for sqld databases addressed by logical partition name.
pub trait SqldNamespacedProvisioner: NamedTargetProvisioner {}

/// Provisioner for Turso databases addressed by logical partition name.
///
/// The event-store partition name is the stable backend-facing key, and the
/// provisioner maps that key to a concrete Turso database URL. Each partition
/// gets its own database, created on demand via the Turso Platform API.
///
/// Enable the `turso` feature for the ready-to-use `TursoPlatformProvisioner`
/// implementation.
pub trait TursoProvisioner: NamedTargetProvisioner {}

#[cfg(test)]
mod tests {
    use super::{DatabaseTarget, NamedTargetProvisioner, PartitionName, TursoProvisioner};
    use crate::Error;

    #[derive(Debug, Clone)]
    struct TestTursoProvisioner;

    impl NamedTargetProvisioner for TestTursoProvisioner {
        async fn ensure_target_for_name(
            &self,
            name: PartitionName<'_>,
        ) -> Result<DatabaseTarget, Error> {
            Ok(match name {
                PartitionName::Default => DatabaseTarget::Turso {
                    url: "libsql://root-org.turso.io".to_string(),
                    auth_token: "token".to_string(),
                },
                PartitionName::Named(name) => DatabaseTarget::Turso {
                    url: format!("libsql://{name}-org.turso.io"),
                    auth_token: "token".to_string(),
                },
            })
        }

        async fn target_for_existing_name(
            &self,
            name: PartitionName<'_>,
        ) -> Result<Option<DatabaseTarget>, Error> {
            Ok(Some(self.ensure_target_for_name(name).await?))
        }

        async fn names(&self) -> Result<Vec<String>, Error> {
            Ok(vec!["orders".to_string(), "users".to_string()])
        }
    }

    impl TursoProvisioner for TestTursoProvisioner {}

    #[tokio::test]
    async fn turso_provisioners_resolve_named_partitions() {
        let default_target = TestTursoProvisioner
            .ensure_target_for_name(PartitionName::Default)
            .await
            .expect("default target should resolve");
        let named_target = TestTursoProvisioner
            .ensure_target_for_name(PartitionName::Named("orders"))
            .await
            .expect("named target should resolve");

        assert_eq!(
            default_target,
            DatabaseTarget::Turso {
                url: "libsql://root-org.turso.io".to_string(),
                auth_token: "token".to_string(),
            }
        );
        assert_eq!(
            named_target,
            DatabaseTarget::Turso {
                url: "libsql://orders-org.turso.io".to_string(),
                auth_token: "token".to_string(),
            }
        );
        assert_eq!(
            TestTursoProvisioner
                .names()
                .await
                .expect("names should enumerate"),
            vec!["orders".to_string(), "users".to_string()]
        );
    }
}
