//! Runtime helpers called by `restate_service!`-generated code.
//!
//! These functions encapsulate the Restate ingress HTTP protocol so the macro
//! does not need to emit verbose networking code.

use wee_events::{AggregateId, CommandName, Entity, Rejection};

use crate::error::Error;
use crate::names;
use crate::types::{CommandRequest, EntityResponse, ExecuteRequest, Metadata};

fn encode_key(target: &AggregateId) -> String {
    format!("{}:{}", target.aggregate_type(), target.aggregate_key())
}

fn generate_correlation_id(target: &AggregateId, command_name: &CommandName) -> String {
    let key = encode_key(target);
    format!("{}-{}-{}", key, command_name, nanoid::nanoid!())
}

fn parse_entity_response<S>(resp: EntityResponse) -> Result<Entity<S>, Error>
where
    S: serde::de::DeserializeOwned,
{
    let state: S = serde_json::from_value(resp.state)?;
    Ok(Entity {
        aggregate_id: resp.aggregate,
        revision: resp.revision,
        state,
    })
}

async fn parse_error_response(resp: reqwest::Response) -> Error {
    let text = resp.text().await.unwrap_or_default();
    if let Ok(rejection) = serde_json::from_str::<Rejection>(&text) {
        return Error::Rejection(rejection);
    }
    if let Ok(envelope) = serde_json::from_str::<serde_json::Value>(&text)
        && let Some(message) = envelope.get("message").and_then(|m| m.as_str())
        && let Ok(rejection) = serde_json::from_str::<Rejection>(message)
    {
        return Error::Rejection(rejection);
    }
    Error::Backend(text)
}

/// Load an entity via the Restate ingress loader endpoint.
pub async fn load<S>(
    http: &reqwest::Client,
    ingress_url: &str,
    service_name: &str,
    id: AggregateId,
) -> Result<Entity<S>, Error>
where
    S: serde::de::DeserializeOwned,
{
    let loader = names::loader_name(service_name);
    let url = format!("{ingress_url}/{loader}/load");

    let resp = http.post(&url).json(&id).send().await?;

    if !resp.status().is_success() {
        return Err(parse_error_response(resp).await);
    }

    let entity_resp: EntityResponse = resp.json().await?;

    parse_entity_response(entity_resp)
}

/// Execute a typed command via the Restate ingress executor endpoint.
pub async fn execute<S>(
    http: &reqwest::Client,
    ingress_url: &str,
    service_name: &str,
    id: AggregateId,
    name: CommandName,
    command: serde_json::Value,
) -> Result<Entity<S>, Error>
where
    S: serde::de::DeserializeOwned,
{
    let executor = names::executor_name(service_name);
    let correlation_id = generate_correlation_id(&id, &name);

    let request = ExecuteRequest {
        command: CommandRequest {
            name,
            target: id,
            command,
        },
        metadata: Metadata {
            correlation_id: correlation_id.clone(),
            causation_id: None,
            idempotency_key: None,
        },
    };

    let url = format!("{ingress_url}/{executor}/{correlation_id}/run");

    let resp = http.post(&url).json(&request).send().await?;

    if !resp.status().is_success() {
        return Err(parse_error_response(resp).await);
    }

    let entity_resp: EntityResponse = resp.json().await?;

    parse_entity_response(entity_resp)
}
