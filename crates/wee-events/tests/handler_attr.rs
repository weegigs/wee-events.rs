//! Tests that #[handler] and #[loader] emit the expected companion items.

#![allow(dead_code)]
// handler/loader bodies are async by macro contract; they need not await
#![allow(clippy::unused_async)]

use wee_events::{AggregateId, Command, Entity, HandlerSpec, LoaderSpec, Revision};

// Trivial state and command
#[derive(Debug, Default, Clone)]
struct Counter {
    value: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
struct Increment {
    amount: i64,
}

impl Command for Increment {
    const NAME: &'static str = "counter:increment";
}

// Capability traits
trait HasRandomSource: Send + Sync + 'static {
    fn next_random(&self) -> i64;
}

trait HasStore: Send + Sync + 'static {
    fn store_name(&self) -> &str;
}

// Annotated handler
#[wee_events::handler(command = Increment, requires(HasRandomSource))]
async fn increment<R: HasRandomSource>(
    _env: &R,
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

// Annotated loader
#[wee_events::loader(requires(HasStore))]
async fn load_counter<R: HasStore>(
    _env: &R,
    id: &AggregateId,
) -> wee_events::Result<Entity<Counter>> {
    Ok(Entity {
        aggregate_id: id.clone(),
        revision: Revision::zero(),
        state: Counter::default(),
    })
}

// Assert the companion structs exist and implement the right traits
#[test]
fn handler_spec_generated() {
    fn assert_spec<T: HandlerSpec<Command = Increment, State = Counter>>() {}
    assert_spec::<increment_Spec>();
}

#[test]
fn loader_spec_generated() {
    fn assert_spec<T: LoaderSpec<State = Counter>>() {}
    assert_spec::<load_counter_Spec>();
}

#[test]
fn handler_requires_trait_generated() {
    // Any type satisfying HasRandomSource + Send + Sync auto-impls __increment_Requires
    struct TestCtx;
    impl HasRandomSource for TestCtx {
        fn next_random(&self) -> i64 {
            42
        }
    }

    fn assert_requires<T: __increment_Requires>() {}
    assert_requires::<TestCtx>();
}

#[test]
fn loader_requires_trait_generated() {
    struct TestCtx;
    impl HasStore for TestCtx {
        fn store_name(&self) -> &'static str {
            "test"
        }
    }

    fn assert_requires<T: __load_counter_Requires>() {}
    assert_requires::<TestCtx>();
}

// Verify the annotated function is still callable normally
#[tokio::test]
async fn handler_function_still_callable() {
    struct TestCtx;
    impl HasRandomSource for TestCtx {
        fn next_random(&self) -> i64 {
            0
        }
    }

    let entity = Entity {
        aggregate_id: AggregateId::new("counter", "1"),
        revision: Revision::zero(),
        state: Counter { value: 5 },
    };

    let result = increment(&TestCtx, &entity, Increment { amount: 3 })
        .await
        .unwrap();
    assert_eq!(result.state.value, 8);
}
