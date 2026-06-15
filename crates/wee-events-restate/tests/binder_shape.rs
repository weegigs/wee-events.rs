//! Proves that `service!` emits Restate Virtual Object internals whose
//! registration type can be attached to a Restate endpoint.
//! We don't hit the wire -- compilation of the builder chain is the
//! assertion.

// handler/loader bodies are async by macro contract; they need not await
#![allow(clippy::unused_async)]

use wee_events::{AggregateId, Command, Entity, Revision};

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter {
    value: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Inc;
impl Command for Inc {
    const NAME: &'static str = "counter:increment";
}

#[wee_events::loader]
pub async fn load<R: Send + Sync + 'static>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter::default(),
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
    }
}

#[derive(Clone)]
struct Env;

#[test]
fn registration_can_be_attached() {
    use restate_sdk::prelude::*;
    let _endpoint = wee_events_restate::create(CounterService)
        .with_env(Env)
        .attach_to(Endpoint::builder())
        .build();
}

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct WeirdCounter;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct WeirdInc;
impl Command for WeirdInc {
    const NAME: &'static str = "weird:increment";
}

#[wee_events::loader]
pub async fn weird_load<R: Send + Sync + 'static>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<WeirdCounter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: WeirdCounter,
    })
}

#[wee_events::handler(command = WeirdInc)]
pub async fn weird_inc<R: Send + Sync + 'static>(
    _env: &R,
    e: &Entity<WeirdCounter>,
    _cmd: WeirdInc,
) -> wee_events::Result<Entity<WeirdCounter>> {
    Ok(e.clone())
}

wee_events::service! {
    pub WeirdCounterService("weird-counter") for WeirdCounter {
        loader: weird_load as "type",
        handlers: [weird_inc as "weird_load"],
    }
}

#[test]
fn registration_internal_method_names_do_not_collide_with_wire_names() {
    use restate_sdk::prelude::*;
    let _endpoint = wee_events_restate::create(WeirdCounterService)
        .with_env(Env)
        .attach_to(Endpoint::builder())
        .build();
}
