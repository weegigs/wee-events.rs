//! Compilation risk verification: generic-R handlers with `ServiceBuilder`.
//!
//! Verifies that `ServiceBuilder::with_handler::<C, _>(generic_fn::<R>)` compiles
//! when called from inside a function that is itself generic over `R: SomeTrait`.
//!
//! The concern: `HandlerBridge` is impl'd for `&'a F where F: Fn(&'a Ctx, ...)`.
//! When `F` is a generic fn item `increment::<R>`, the compiler must accept it
//! satisfies `Fn(&'a R, &'a Entity<S>, C) -> SomeFut` for the HRTB `for<'a>`.

use std::future::Future;

use wee_events::{AggregateId, Entity, Revision, ServiceBuilder};

// ---------------------------------------------------------------------------
// Trivial capability traits
// ---------------------------------------------------------------------------

trait HasStore: Send + Sync + 'static {}
trait HasRandomSource: Send + Sync + 'static {}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct Counter {
    value: i64,
}

#[derive(serde::Serialize)]
struct Increment {
    amount: i64,
}

impl wee_events::Command for Increment {
    const NAME: &'static str = "counter:increment";
}

// ---------------------------------------------------------------------------
// Generic async handlers — the key pattern under test
// ---------------------------------------------------------------------------

async fn load_counter<R: HasStore>(
    _ctx: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter::default(),
    })
}

async fn increment<R: HasRandomSource>(
    _ctx: &R,
    entity: &Entity<Counter>,
    cmd: Increment,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: entity.aggregate_id.clone(),
        revision: entity.revision.clone(),
        state: Counter {
            value: entity.state.value + cmd.amount,
        },
    })
}

// ---------------------------------------------------------------------------
// The function that is generic over R — the compilation hypothesis.
//
// The fn-item return type for async fns is unnameable, so we return `()` and
// drive the service internally.  The purpose of this function is purely to
// check that the ServiceBuilder chain typechecks when R is generic.
// The test functions below call it with a concrete TestCtx.
// ---------------------------------------------------------------------------

async fn build_and_run<R, F, Fut>(
    factory: F,
    id: AggregateId,
) -> wee_events::Result<Entity<Counter>>
where
    R: HasStore + HasRandomSource + Send + Sync + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = wee_events::Result<R>> + Send + 'static,
{
    let service = ServiceBuilder::<Counter>::new()
        .with_loader(load_counter::<R>)
        .with_handler::<Increment, _>(increment::<R>)
        .build(factory);

    service.execute(id.clone(), Increment { amount: 7 }).await
}

async fn build_and_load<R, F, Fut>(
    factory: F,
    id: AggregateId,
) -> wee_events::Result<Entity<Counter>>
where
    R: HasStore + HasRandomSource + Send + Sync + 'static,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = wee_events::Result<R>> + Send + 'static,
{
    let service = ServiceBuilder::<Counter>::new()
        .with_loader(load_counter::<R>)
        .with_handler::<Increment, _>(increment::<R>)
        .build(factory);

    service.load(id.clone()).await
}

// ---------------------------------------------------------------------------
// Concrete TestCtx that implements both traits
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TestCtx;

impl HasStore for TestCtx {}
impl HasRandomSource for TestCtx {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn generic_r_load_returns_default_entity() {
    let id = AggregateId::new("counter", "test-generic-1");
    let entity = build_and_load(|| async { Ok(TestCtx) }, id.clone())
        .await
        .unwrap();

    assert_eq!(entity.state.value, 0);
    assert_eq!(entity.aggregate_id, id);
}

#[tokio::test]
async fn generic_r_execute_dispatches_increment() {
    let id = AggregateId::new("counter", "test-generic-2");
    let entity = build_and_run(|| async { Ok(TestCtx) }, id.clone())
        .await
        .unwrap();

    assert_eq!(entity.state.value, 7);
}
