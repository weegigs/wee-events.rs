//! Verifies that `service!` accepts `as "wire"` overrides on handlers and
//! loader, and that the resulting types compile. The wire names themselves
//! are consumed by the Restate binder (Task 7); here we only prove parse
//! success and no regression on the core in-process path.

// handler/loader bodies are async by macro contract; they need not await
#![allow(clippy::unused_async)]

use wee_events::{AggregateId, Command, Entity, Revision, TypedService};

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter {
    value: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Bump;
impl Command for Bump {
    const NAME: &'static str = "counter:bump";
}

#[wee_events::loader]
pub async fn fetch_counter<R: Send + Sync + 'static>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter::default(),
    })
}

#[wee_events::handler(command = Bump)]
pub async fn do_bump<R: Send + Sync + 'static>(
    _env: &R,
    entity: &Entity<Counter>,
    _cmd: Bump,
) -> wee_events::Result<Entity<Counter>> {
    Ok(entity.clone())
}

wee_events::service! {
    pub CounterService("counter") for Counter {
        loader: fetch_counter as "state",
        handlers: [do_bump as "bump"],
    }
}

#[tokio::test]
async fn create_still_works_with_renamed_handler() {
    #[derive(Clone)]
    struct Env;
    let service = wee_events::create(CounterService)
        .with_store(Env)
        .with_env(())
        .build();
    let id: AggregateId = "counter:c1".parse().unwrap();
    let entity = service.execute(id.clone(), Bump).await.unwrap();
    assert_eq!(entity.state.value, 0);
}
