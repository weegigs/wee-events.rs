//! Explicit `impl Future + Send` on trait impls is deliberate; not desugarable.
#![allow(clippy::manual_async_fn)]

use std::convert::Infallible;
use std::future::Future;

use wee_events::{
    AggregateId, Command, Entity, Handles, Rejection, Revision, ServiceError, TypedService,
};

type TestError = ServiceError<Infallible>;

#[test]
fn rejection_displays_code_and_message() {
    let r = Rejection::new("INSUFFICIENT_FUNDS", "balance too low");
    assert_eq!(r.code, "INSUFFICIENT_FUNDS");
    assert_eq!(r.message, "balance too low");
    assert_eq!(r.to_string(), "INSUFFICIENT_FUNDS: balance too low");
}

#[test]
fn rejection_carries_context() {
    let r = Rejection::with_context(
        "LIMIT_EXCEEDED",
        "over the limit",
        serde_json::json!({ "limit": 100, "actual": 150 }),
    );
    assert_eq!(r.context["actual"], 150);
}

#[test]
fn rejection_default_context_is_empty_object() {
    let r = Rejection::new("CODE", "msg");
    assert_eq!(r.context, serde_json::json!({}));
}

#[test]
fn rejection_converts_to_service_error() {
    let r = Rejection::new("CODE", "msg");
    let err: TestError = r.into();
    assert!(matches!(err, ServiceError::Rejection(_)));
}

#[test]
fn rejection_serde_round_trip() {
    let original = Rejection::with_context(
        "LIMIT_EXCEEDED",
        "over the limit",
        serde_json::json!({ "limit": 100 }),
    );
    let json = serde_json::to_string(&original).unwrap();
    let restored: Rejection = serde_json::from_str(&json).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn rejection_deserialize_without_context_defaults_to_empty_object() {
    let json = r#"{"code":"C","message":"m"}"#;
    let r: Rejection = serde_json::from_str(json).unwrap();
    assert_eq!(r.context, serde_json::json!({}));
}

/// A trivial state for testing.
#[derive(Debug, Default, Clone)]
struct Counter {
    value: i64,
}

// ── Typed service tests ───────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
struct Increment {
    amount: i64,
}

impl Command for Increment {
    const NAME: &'static str = "counter:increment";
}

struct TypedCounterService;

// Implement ServiceState — associates the service with its state type
impl wee_events::__private::ServiceState for TypedCounterService {
    type State = Counter;
}

// Implement DispatchCommand<Increment> — required by Handles<Increment>
impl wee_events::__private::DispatchCommand<Increment> for TypedCounterService {
    type Error = TestError;

    fn dispatch_command(
        &self,
        id: AggregateId,
        cmd: Increment,
    ) -> impl Future<Output = Result<Entity<Counter>, Self::Error>> + Send {
        async move {
            Ok(Entity {
                aggregate_id: id,
                revision: Revision::zero(),
                state: Counter { value: cmd.amount },
            })
        }
    }
}

// Handles<Increment> is satisfied because DispatchCommand<Increment> is implemented
impl Handles<Increment> for TypedCounterService {}

impl TypedService<Counter> for TypedCounterService {
    type Error = TestError;

    fn load(
        &self,
        id: AggregateId,
    ) -> impl Future<Output = Result<Entity<Counter>, Self::Error>> + Send {
        async move {
            Ok(Entity {
                aggregate_id: id,
                revision: Revision::zero(),
                state: Counter { value: 0 },
            })
        }
    }
    // execute() uses the default impl from TypedService which calls DispatchCommand<C>
}

#[tokio::test]
async fn typed_service_loads_state() {
    let svc = TypedCounterService;
    let id: AggregateId = "counter:test-1".parse().unwrap();
    let entity = svc.load(id.clone()).await.unwrap();
    assert_eq!(entity.state.value, 0);
}

#[tokio::test]
async fn typed_service_executes_registered_command() {
    let svc = TypedCounterService;
    let id: AggregateId = "counter:test-1".parse().unwrap();
    let entity = svc
        .execute(id.clone(), Increment { amount: 5 })
        .await
        .unwrap();
    assert_eq!(entity.state.value, 5);
}
