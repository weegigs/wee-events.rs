/// Information about a Turso database returned by the Platform API.
#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub hostname: String,
    pub name: String,
}

/// Errors from the Turso Platform API.
#[derive(Debug)]
pub enum ApiError {
    /// Database already exists (409 Conflict).
    AlreadyExists,
    /// Authentication or authorization failure (401/403).
    AuthFailure(String),
    /// Any other error.
    Unexpected(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists => write!(f, "database already exists"),
            Self::AuthFailure(msg) => write!(f, "auth failure: {msg}"),
            Self::Unexpected(msg) => write!(f, "unexpected error: {msg}"),
        }
    }
}

/// Abstraction over the Turso Platform API.
///
/// The reqwest implementation ([`TursoHttpClient`]) is the production version;
/// tests use [`FakeTursoPlatformApi`].
use std::future::Future;

pub trait TursoPlatformApi: Send + Sync {
    fn create_database(
        &self,
        name: &str,
        group: &str,
    ) -> impl Future<Output = Result<DatabaseInfo, ApiError>> + Send;

    fn get_database(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<DatabaseInfo>, ApiError>> + Send;

    fn list_databases(
        &self,
        group: &str,
    ) -> impl Future<Output = Result<Vec<DatabaseInfo>, ApiError>> + Send;

    fn delete_database(&self, name: &str) -> impl Future<Output = Result<(), ApiError>> + Send;
}

impl<T: TursoPlatformApi> TursoPlatformApi for std::sync::Arc<T> {
    async fn create_database(&self, name: &str, group: &str) -> Result<DatabaseInfo, ApiError> {
        (**self).create_database(name, group).await
    }
    async fn get_database(&self, name: &str) -> Result<Option<DatabaseInfo>, ApiError> {
        (**self).get_database(name).await
    }
    async fn list_databases(&self, group: &str) -> Result<Vec<DatabaseInfo>, ApiError> {
        (**self).list_databases(group).await
    }
    async fn delete_database(&self, name: &str) -> Result<(), ApiError> {
        (**self).delete_database(name).await
    }
}

// ---------------------------------------------------------------------------
// In-memory fake for testing
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod fake {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory fake that tracks databases and call counts.
    pub(crate) struct FakeTursoPlatformApi {
        databases: Mutex<HashMap<String, DatabaseInfo>>,
        pub(crate) create_calls: AtomicUsize,
        pub(crate) get_calls: AtomicUsize,
        pub(crate) list_calls: AtomicUsize,
    }

    impl FakeTursoPlatformApi {
        pub(crate) fn new() -> Self {
            Self {
                databases: Mutex::new(HashMap::new()),
                create_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
                list_calls: AtomicUsize::new(0),
            }
        }
    }

    impl TursoPlatformApi for FakeTursoPlatformApi {
        async fn create_database(
            &self,
            name: &str,
            _group: &str,
        ) -> Result<DatabaseInfo, ApiError> {
            self.create_calls.fetch_add(1, Ordering::Relaxed);
            let mut dbs = self.databases.lock().unwrap();
            if dbs.contains_key(name) {
                return Err(ApiError::AlreadyExists);
            }
            let info = DatabaseInfo {
                hostname: format!("{name}-testorg.turso.io"),
                name: name.to_string(),
            };
            dbs.insert(name.to_string(), info.clone());
            Ok(info)
        }

        async fn get_database(&self, name: &str) -> Result<Option<DatabaseInfo>, ApiError> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.databases.lock().unwrap().get(name).cloned())
        }

        async fn list_databases(&self, _group: &str) -> Result<Vec<DatabaseInfo>, ApiError> {
            self.list_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.databases.lock().unwrap().values().cloned().collect())
        }

        async fn delete_database(&self, name: &str) -> Result<(), ApiError> {
            self.databases.lock().unwrap().remove(name);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Reqwest HTTP client (production)
// ---------------------------------------------------------------------------

#[cfg(feature = "turso")]
pub struct TursoHttpClient {
    client: reqwest::Client,
    base_url: String,
    api_token: String,
    org: String,
}

#[cfg(feature = "turso")]
impl TursoHttpClient {
    pub(crate) fn new(base_url: String, api_token: String, org: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            api_token,
            org,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/v1/organizations/{}{}", self.base_url, self.org, path)
    }
}

#[cfg(feature = "turso")]
mod http_types {
    #[derive(serde::Deserialize)]
    pub(crate) struct CreateDatabaseResponse {
        pub database: DatabaseResponseInfo,
    }

    #[derive(serde::Deserialize)]
    pub(crate) struct GetDatabaseResponse {
        pub database: DatabaseResponseInfo,
    }

    #[derive(serde::Deserialize)]
    pub(crate) struct ListDatabasesResponse {
        pub databases: Vec<DatabaseResponseInfo>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    pub(crate) struct DatabaseResponseInfo {
        pub hostname: String,
        pub name: String,
    }
}

#[cfg(feature = "turso")]
impl TursoPlatformApi for TursoHttpClient {
    async fn create_database(&self, name: &str, group: &str) -> Result<DatabaseInfo, ApiError> {
        let response = self
            .client
            .post(self.url("/databases"))
            .bearer_auth(&self.api_token)
            .json(&serde_json::json!({ "name": name, "group": group }))
            .send()
            .await
            .map_err(|e| ApiError::Unexpected(format!("request failed: {e}")))?;

        match response.status().as_u16() {
            200 | 201 => {
                let body: http_types::CreateDatabaseResponse = response
                    .json()
                    .await
                    .map_err(|e| ApiError::Unexpected(format!("bad response body: {e}")))?;
                Ok(DatabaseInfo {
                    hostname: body.database.hostname,
                    name: body.database.name,
                })
            }
            409 => Err(ApiError::AlreadyExists),
            401 | 403 => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::AuthFailure(body))
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::Unexpected(format!("status {status}: {body}")))
            }
        }
    }

    async fn get_database(&self, name: &str) -> Result<Option<DatabaseInfo>, ApiError> {
        let response = self
            .client
            .get(self.url(&format!("/databases/{name}")))
            .bearer_auth(&self.api_token)
            .send()
            .await
            .map_err(|e| ApiError::Unexpected(format!("request failed: {e}")))?;

        match response.status().as_u16() {
            200 => {
                let body: http_types::GetDatabaseResponse = response
                    .json()
                    .await
                    .map_err(|e| ApiError::Unexpected(format!("bad response body: {e}")))?;
                Ok(Some(DatabaseInfo {
                    hostname: body.database.hostname,
                    name: body.database.name,
                }))
            }
            404 => Ok(None),
            401 | 403 => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::AuthFailure(body))
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::Unexpected(format!("status {status}: {body}")))
            }
        }
    }

    async fn list_databases(&self, _group: &str) -> Result<Vec<DatabaseInfo>, ApiError> {
        let response = self
            .client
            .get(self.url("/databases"))
            .bearer_auth(&self.api_token)
            .send()
            .await
            .map_err(|e| ApiError::Unexpected(format!("request failed: {e}")))?;

        match response.status().as_u16() {
            200 => {
                let body: http_types::ListDatabasesResponse = response
                    .json()
                    .await
                    .map_err(|e| ApiError::Unexpected(format!("bad response body: {e}")))?;
                Ok(body
                    .databases
                    .into_iter()
                    .map(|db| DatabaseInfo {
                        hostname: db.hostname,
                        name: db.name,
                    })
                    .collect())
            }
            401 | 403 => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::AuthFailure(body))
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::Unexpected(format!("status {status}: {body}")))
            }
        }
    }

    async fn delete_database(&self, name: &str) -> Result<(), ApiError> {
        let response = self
            .client
            .delete(self.url(&format!("/databases/{name}")))
            .bearer_auth(&self.api_token)
            .send()
            .await
            .map_err(|e| ApiError::Unexpected(format!("request failed: {e}")))?;

        match response.status().as_u16() {
            200 | 404 => Ok(()),
            401 | 403 => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::AuthFailure(body))
            }
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(ApiError::Unexpected(format!("status {status}: {body}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeTursoPlatformApi;
    use super::*;

    #[tokio::test]
    async fn fake_create_succeeds_then_returns_already_exists() {
        let api = FakeTursoPlatformApi::new();
        let info = api
            .create_database("myapp-orders", "default")
            .await
            .unwrap();
        assert_eq!(info.hostname, "myapp-orders-testorg.turso.io");
        assert_eq!(info.name, "myapp-orders");

        let err = api
            .create_database("myapp-orders", "default")
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::AlreadyExists));
    }

    #[tokio::test]
    async fn fake_get_returns_none_for_unknown() {
        let api = FakeTursoPlatformApi::new();
        assert!(api.get_database("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fake_get_returns_some_for_known() {
        let api = FakeTursoPlatformApi::new();
        api.create_database("myapp-orders", "default")
            .await
            .unwrap();
        let info = api.get_database("myapp-orders").await.unwrap().unwrap();
        assert_eq!(info.name, "myapp-orders");
    }

    #[tokio::test]
    async fn fake_list_returns_all_created() {
        let api = FakeTursoPlatformApi::new();
        api.create_database("myapp-a", "default").await.unwrap();
        api.create_database("myapp-b", "default").await.unwrap();
        let dbs = api.list_databases("default").await.unwrap();
        assert_eq!(dbs.len(), 2);
    }
}
