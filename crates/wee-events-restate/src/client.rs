use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use wee_events::{AggregateId, CommandName, Entity, Rejection, ServiceDefinition};

use crate::error::Error;
use crate::names;
use crate::types::{CommandRequest, ExecuteRequest, Metadata};

/// Restate-backed service client, generic over a service definition `D`.
///
/// Implements `TypedService<D::State>` and `Handles<C>` for every command `C`
/// that `D` declares via `HasCommand<C>`. No per-service codegen is needed —
/// a single blanket impl covers all definitions.
pub struct RestateClient<D: ServiceDefinition> {
    http: reqwest::Client,
    ingress_url: String,
    _def: PhantomData<fn() -> D>,
}

impl<D: ServiceDefinition> RestateClient<D> {
    pub fn new(ingress_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            ingress_url: ingress_url.into(),
            _def: PhantomData,
        }
    }

    fn executor_name() -> String {
        names::executor_name(D::SERVICE_NAME)
    }

    fn encode_key(target: &AggregateId) -> String {
        format!("{}:{}", target.aggregate_type(), target.aggregate_key())
    }

    fn generate_correlation_id(target: &AggregateId, command_name: &CommandName) -> String {
        let key = Self::encode_key(target);
        format!("{}-{}-{}", key, command_name, nanoid::nanoid!())
    }

    /// Execute a command with an explicit idempotency key. Resubmitting
    /// the same key returns the original result without re-executing.
    ///
    /// The key is used as the Restate workflow ID, so it must be unique
    /// across all commands for this service.
    pub async fn execute_idempotent(
        &self,
        name: &CommandName,
        target: &AggregateId,
        command: serde_json::Value,
        idempotency_key: impl Into<String>,
    ) -> Result<Entity<D::State>, Error>
    where
        D::State: DeserializeOwned,
    {
        let idempotency_key = idempotency_key.into();
        let correlation_id = Self::generate_correlation_id(target, name);

        let request = ExecuteRequest {
            command: CommandRequest {
                name: name.clone(),
                target: target.clone(),
                command,
            },
            metadata: Metadata {
                correlation_id,
                causation_id: None,
                idempotency_key: Some(idempotency_key.clone()),
            },
        };

        // Use the idempotency key as the workflow ID
        let url = format!(
            "{}/{}/{}/run",
            self.ingress_url,
            Self::executor_name(),
            idempotency_key,
        );

        let resp = self.http.post(&url).json(&request).send().await?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            if let Ok(rejection) = serde_json::from_str::<Rejection>(&text) {
                return Err(Error::Rejection(rejection));
            }
            if let Ok(envelope) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(message) = envelope.get("message").and_then(|m| m.as_str())
                && let Ok(rejection) = serde_json::from_str::<Rejection>(message)
            {
                return Err(Error::Rejection(rejection));
            }
            return Err(Error::Backend(text));
        }

        let exec_resp: crate::types::EntityResponse = resp.json().await?;

        let state: D::State = serde_json::from_value(exec_resp.state)?;
        Ok(Entity {
            aggregate_id: exec_resp.aggregate,
            revision: exec_resp.revision,
            state,
        })
    }
}

impl<D: ServiceDefinition> Clone for RestateClient<D> {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            ingress_url: self.ingress_url.clone(),
            _def: PhantomData::<fn() -> D>,
        }
    }
}

// ---------------------------------------------------------------------------
// Private trait impls required for TypedService / Handles
// ---------------------------------------------------------------------------

impl<D: ServiceDefinition> wee_events::__private::ServiceState for RestateClient<D> {
    type State = D::State;
}

impl<D, C> wee_events::__private::DispatchCommand<C> for RestateClient<D>
where
    D: ServiceDefinition + wee_events::HasCommand<C>,
    C: wee_events::Command + serde::Serialize + Send + 'static,
    D::State: DeserializeOwned + Send + 'static,
{
    type Error = Error;

    async fn dispatch_command(&self, id: AggregateId, cmd: C) -> Result<Entity<D::State>, Error> {
        let http = self.http.clone();
        let url = self.ingress_url.clone();
        let value = serde_json::to_value(&cmd)?;
        crate::generated::execute::<D::State>(
            &http,
            &url,
            D::SERVICE_NAME,
            id,
            C::NAME.into(),
            value,
        )
        .await
    }
}

impl<D, C> wee_events::Handles<C> for RestateClient<D>
where
    D: ServiceDefinition,
    Self: wee_events::__private::DispatchCommand<C>,
    C: wee_events::Command,
{
}

impl<D> wee_events::TypedService<D::State> for RestateClient<D>
where
    D: ServiceDefinition,
    D::State: DeserializeOwned + Send + 'static,
{
    type Error = Error;

    async fn load(&self, id: AggregateId) -> Result<Entity<D::State>, Error> {
        let http = self.http.clone();
        let url = self.ingress_url.clone();
        crate::generated::load::<D::State>(&http, &url, D::SERVICE_NAME, id).await
    }
}
