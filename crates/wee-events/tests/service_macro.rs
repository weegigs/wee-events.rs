//! Tests for the rewritten `service!` macro.

//!
//! Handlers and loaders are annotated with `#[handler]`/`#[loader]` and are
//! generic over a context type `R`. The `service!` macro consumes bare function
//! names and derives the command types and capability requirements from the
//! companion `HandlerSpec`/`LoaderSpec` items.

// handler/loader bodies are async by macro contract; they need not await
#![allow(clippy::unused_async)]

use wee_events::{AggregateId, Command, Entity, Handles, Revision, TypedService};

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter {
    value: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Increment {
    amount: i64,
}

impl Command for Increment {
    const NAME: &'static str = "counter:increment";
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Adjust;

impl Command for Adjust {
    const NAME: &'static str = "counter:adjust";
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Touch;

impl Command for Touch {
    const NAME: &'static str = "counter:touch";
}

// ---------------------------------------------------------------------------
// Capability traits
// ---------------------------------------------------------------------------

pub trait HasStore: Send + Sync + 'static {
    fn store_info(&self) -> &str;
}

pub trait HasRandomSource: Send + Sync + 'static {
    fn random_bonus(&self) -> i64;
}

// ---------------------------------------------------------------------------
// Annotated loader and handlers — generic over R
// ---------------------------------------------------------------------------

#[wee_events::loader(requires(HasStore))]
pub async fn load_counter<R: HasStore>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter::default(),
    })
}

#[wee_events::handler(command = Increment, requires(HasRandomSource))]
pub async fn increment<R: HasRandomSource>(
    env: &R,
    entity: &Entity<Counter>,
    cmd: Increment,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: entity.aggregate_id.clone(),
        revision: entity.revision.clone(),
        state: Counter {
            value: entity.state.value + cmd.amount + env.random_bonus(),
        },
    })
}

#[wee_events::handler(command = Adjust, requires(HasRandomSource))]
pub async fn adjust<R: HasRandomSource>(
    _env: &R,
    entity: &Entity<Counter>,
    _cmd: Adjust,
) -> wee_events::Result<Entity<Counter>> {
    Ok(entity.clone())
}

#[wee_events::handler(command = Touch)]
pub async fn touch<R: Send + Sync>(
    _env: &R,
    _entity: &Entity<Counter>,
    _cmd: Touch,
) -> wee_events::Result<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Service declaration — new syntax: bare function names
// ---------------------------------------------------------------------------

wee_events::service! {
    pub CounterService for Counter {
        loader: load_counter,
        handlers: [increment, adjust, touch],
    }
}

// ---------------------------------------------------------------------------
// Concrete context satisfying CounterServiceEnv
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppCtx;

impl HasStore for AppCtx {
    fn store_info(&self) -> &'static str {
        "in-memory"
    }
}

impl HasRandomSource for AppCtx {
    fn random_bonus(&self) -> i64 {
        5
    }
}

impl<Store, Services> HasRandomSource for wee_events::HandlerEnv<Store, Services>
where
    Store: Send + Sync + 'static,
    Services: HasRandomSource + 'static,
{
    fn random_bonus(&self) -> i64 {
        self.services().random_bonus()
    }
}

fn build_service() -> __wee_events_counter_service_core::Service<AppCtx, AppCtx> {
    wee_events::create(CounterService)
        .with_store(AppCtx)
        .with_env(AppCtx)
        .build()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_service_executes_handler() {
    let service = build_service();
    let id: AggregateId = "counter:c1".parse().unwrap();
    let entity = service
        .execute(id.clone(), Increment { amount: 3 })
        .await
        .unwrap();
    // amount 3 + bonus 5 = 8
    assert_eq!(entity.state.value, 8);
}

#[tokio::test]
async fn create_service_loads() {
    let service = build_service();
    let id: AggregateId = "counter:c1".parse().unwrap();
    let entity = service.load(id.clone()).await.unwrap();
    assert_eq!(entity.state.value, 0);
}

#[tokio::test]
async fn create_service_handles_multiple_commands() {
    let service = build_service();
    let id: AggregateId = "counter:c1".parse().unwrap();
    // increment: 3 + 5 = 8
    let entity = service
        .execute(id.clone(), Increment { amount: 3 })
        .await
        .unwrap();
    assert_eq!(entity.state.value, 8);
    // adjust is a no-op (returns entity unchanged)
    let entity = service.execute(id.clone(), Adjust).await.unwrap();
    assert_eq!(entity.state.value, 0); // loader always returns default state
}

#[tokio::test]
async fn create_service_reloads_after_void_handler() {
    let service = build_service();
    let id: AggregateId = "counter:c1".parse().unwrap();
    let entity = service.execute(id.clone(), Touch).await.unwrap();
    assert_eq!(entity.state.value, 0);
}

/// A function that accepts any `TypedService<Counter>` implementation and
/// exercises both commands. This is the "shared caller" pattern — the same
/// business logic works whether given a local service or a remote client.
async fn caller<T>(svc: &T, id: &AggregateId) -> wee_events::Result<Entity<Counter>>
where
    T: TypedService<Counter> + Handles<Increment> + Handles<Adjust>,
    <T as wee_events::__private::DispatchCommand<Increment>>::Error: Into<wee_events::Error>,
    <T as wee_events::__private::DispatchCommand<Adjust>>::Error: Into<wee_events::Error>,
{
    let _ = svc
        .execute(id.clone(), Increment { amount: 1 })
        .await
        .map_err(Into::into)?;
    svc.execute(id.clone(), Adjust).await.map_err(Into::into)
}

#[tokio::test]
async fn shared_caller_pattern() {
    let service = build_service();
    let id: AggregateId = "counter:c1".parse().unwrap();
    // Increment(1) + bonus(5) = 6, then Adjust is a no-op.
    // The loader always returns default (value: 0) so the Adjust call loads
    // fresh state with value 0.
    let entity = caller(&service, &id).await.unwrap();
    assert_eq!(entity.state.value, 0); // loader returns default, adjust is no-op
}

#[test]
fn env_trait_is_generated() {
    // Any type satisfying the underlying capability traits satisfies CounterServiceEnv
    fn assert_env<T: CounterServiceEnv>() {}
    assert_env::<AppCtx>();
}

#[test]
fn full_form_implements_definition_traits() {
    fn require<D: wee_events::ServiceDefinition + wee_events::HasCommand<Increment>>() {}
    require::<CounterService>();
    assert_eq!(
        <CounterService as wee_events::ServiceDefinition>::SERVICE_NAME,
        "counter_service"
    );
}
