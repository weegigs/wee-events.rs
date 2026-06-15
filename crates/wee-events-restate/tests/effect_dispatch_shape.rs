//! Proves that `service!` with `effects:` declarations compiles and that
//! the emitted registration type still binds under restate-sdk's endpoint
//! builder. End-to-end wire behaviour is out of scope for a unit test;
//! correctness of the emitted match arms is asserted via compilation.

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

#[restate_sdk::workflow]
pub trait Notifier {
    async fn run(
        notification: restate_sdk::serde::Json<wee_events_restate::ExecuteNotification>,
    ) -> Result<(), restate_sdk::errors::HandlerError>;
}

struct NotifierImpl;
impl Notifier for NotifierImpl {
    async fn run(
        &self,
        _ctx: restate_sdk::prelude::WorkflowContext<'_>,
        _n: restate_sdk::serde::Json<wee_events_restate::ExecuteNotification>,
    ) -> Result<(), restate_sdk::errors::HandlerError> {
        Ok(())
    }
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

wee_events::service! {
    pub CounterService("counter") for Counter {
        loader: load,
        handlers: [inc],
        effects: [
            Notifier on all,
        ],
    }
}

#[test]
fn registration_compiles_with_effects() {
    use restate_sdk::prelude::*;
    let _endpoint = wee_events_restate::create(CounterService)
        .with_env(())
        .with_effect(NotifierImpl)
        .attach_to(Endpoint::builder())
        .build();
}
