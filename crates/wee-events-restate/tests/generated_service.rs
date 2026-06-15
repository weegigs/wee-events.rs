//! Compile-time shape and trait tests for `service!` definition-only declarations.
//!
//! Verifies:
//! - `service!` macro generates `CounterService` struct
//! - `CounterService::restate_client()` returns a `RestateClient<CounterService>`
//! - `RestateClient<CounterService>` implements `TypedService<Counter>`
//! - `RestateClient<CounterService>` implements `Handles<C>` for registered commands
//!
//! No HTTP calls are made — there is no Restate server running during tests.

use wee_events::{AggregateId, Command, TypedService};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter {
    value: i64,
}

// Command types need both Serialize (client side) and Deserialize (server dispatch).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Increment {
    amount: i64,
}

impl Command for Increment {
    const NAME: &'static str = "counter:increment";
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Adjust;

impl Command for Adjust {
    const NAME: &'static str = "counter:adjust";
}

// ---------------------------------------------------------------------------
// Service definition (definition-only)
// ---------------------------------------------------------------------------

wee_events::service! {
    pub CounterService("counter") for Counter [Increment, Adjust]
}

// ---------------------------------------------------------------------------
// Shared trait-bound helper — accepts any RestateClient that implements
// both TypedService<Counter> and handles the registered commands.
// ---------------------------------------------------------------------------

fn assert_typed_service<T>()
where
    T: wee_events::TypedService<Counter>
        + wee_events::Handles<Increment>
        + wee_events::Handles<Adjust>,
{
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verify that `CounterService::restate_client()` compiles and exposes the
/// expected API. The futures are constructed but never awaited — no network
/// is required.
#[test]
fn generated_client_has_typed_methods() {
    let client = CounterService::restate_client("http://localhost:8080");
    let id: AggregateId = "counter:c1".parse().unwrap();

    // Verify load and execute return futures. We use `_` to drop them without
    // awaiting since there is no Restate server in tests.
    let _load_fut = client.load(id.clone());
    let _inc_fut = client.execute(id.clone(), Increment { amount: 1 });
    let _adj_fut = client.execute(id.clone(), Adjust);
}

/// Verify that `RestateClient<CounterService>` satisfies `TypedService<Counter>`.
/// This is a compile-time check — `assert_typed_service` accepts only types
/// that implement the required traits.
#[test]
fn client_implements_typed_service() {
    use wee_events_restate::RestateClient;
    assert_typed_service::<RestateClient<CounterService>>();
}
