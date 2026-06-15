//! Verifies that `service!` accepts an optional `effects:` block with
//! all three filter forms.

// handler/loader bodies are async by macro contract; they need not await
#![allow(clippy::unused_async)]

use wee_events::{AggregateId, Command, Entity, Revision};

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Inc;
impl Command for Inc {
    const NAME: &'static str = "counter:increment";
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Adj;
impl Command for Adj {
    const NAME: &'static str = "counter:adjust";
}

#[restate_sdk::workflow]
pub trait SendWelcomeEmail {
    async fn run(
        notification: restate_sdk::serde::Json<wee_events_restate::ExecuteNotification>,
    ) -> Result<(), restate_sdk::errors::HandlerError>;
}

#[restate_sdk::workflow]
pub trait UpdateAnalytics {
    async fn run(
        notification: restate_sdk::serde::Json<wee_events_restate::ExecuteNotification>,
    ) -> Result<(), restate_sdk::errors::HandlerError>;
}

#[restate_sdk::workflow]
pub trait AuditLog {
    async fn run(
        notification: restate_sdk::serde::Json<wee_events_restate::ExecuteNotification>,
    ) -> Result<(), restate_sdk::errors::HandlerError>;
}

#[wee_events::loader]
pub async fn load<R: Send + Sync + 'static>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter,
    })
}

#[wee_events::handler(command = Inc)]
pub async fn inc<R: Send + Sync + 'static>(
    _env: &R,
    e: &Entity<Counter>,
    _cmd: Inc,
) -> wee_events::Result<Entity<Counter>> {
    Ok(e.clone())
}

#[wee_events::handler(command = Adj)]
pub async fn adj<R: Send + Sync + 'static>(
    _env: &R,
    e: &Entity<Counter>,
    _cmd: Adj,
) -> wee_events::Result<Entity<Counter>> {
    Ok(e.clone())
}

wee_events::service! {
    pub CounterService("counter") for Counter {
        loader: load,
        handlers: [inc, adj],
        effects: [
            SendWelcomeEmail on [Inc],
            UpdateAnalytics on all,
            AuditLog on predicate(|n| n.command.name.as_str() == "counter:increment"),
        ],
    }
}

#[test]
fn parses_and_still_emits_definition_traits() {
    assert_eq!(
        <CounterService as wee_events::ServiceDefinition>::SERVICE_NAME,
        "counter"
    );
}
