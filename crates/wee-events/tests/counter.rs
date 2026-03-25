//! Counter example — ports the Go counter sample to prove the full event
//! sourcing cycle: command → handler → events → store → load → render → verify.

use serde::{Deserialize, Serialize};
use wee_events::{
    memory::MemoryStore, AggregateId, Command, DomainEvent, Entity, EventData, EventStore,
    EventType, PublishOptions, Renderer,
};

// ---------------------------------------------------------------------------
// Domain events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "counter")]
enum CounterEvent {
    Incremented { amount: i64 },
    Decremented { amount: i64 },
    Reset,
}

// ---------------------------------------------------------------------------
// Domain commands
// ---------------------------------------------------------------------------

#[derive(Debug, Command)]
#[command(prefix = "counter")]
enum CounterCommand {
    Increment { amount: i64 },
    Decrement { amount: i64 },
    Reset,
}

// ---------------------------------------------------------------------------
// Projected state
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq)]
struct CounterState {
    value: i64,
    event_count: u32,
}

// ---------------------------------------------------------------------------
// Reducers — plain function pointers, no trait objects
// ---------------------------------------------------------------------------

fn reduce_incremented(
    state: &mut CounterState,
    _event_type: &EventType,
    data: &EventData,
) -> Result<(), wee_events::Error> {
    let event: CounterEvent = data.deserialize_json()?;
    if let CounterEvent::Incremented { amount } = event {
        state.value += amount;
        state.event_count += 1;
    }
    Ok(())
}

fn reduce_decremented(
    state: &mut CounterState,
    _event_type: &EventType,
    data: &EventData,
) -> Result<(), wee_events::Error> {
    let event: CounterEvent = data.deserialize_json()?;
    if let CounterEvent::Decremented { amount } = event {
        state.value -= amount;
        state.event_count += 1;
    }
    Ok(())
}

fn reduce_reset(
    state: &mut CounterState,
    _event_type: &EventType,
    _data: &EventData,
) -> Result<(), wee_events::Error> {
    state.value = 0;
    state.event_count += 1;
    Ok(())
}

fn build_renderer() -> Renderer<CounterState> {
    Renderer::new()
        .with(CounterEvent::INCREMENTED, reduce_incremented)
        .with(CounterEvent::DECREMENTED, reduce_decremented)
        .with(CounterEvent::RESET, reduce_reset)
}

// ---------------------------------------------------------------------------
// Command handler — produces events, publishes via store
// ---------------------------------------------------------------------------

async fn handle_command(
    store: &MemoryStore,
    aggregate_id: &AggregateId,
    entity: &Entity<CounterState>,
    command: CounterCommand,
) -> Result<(), wee_events::Error> {
    let events: Vec<CounterEvent> = match command {
        CounterCommand::Increment { amount } => {
            vec![CounterEvent::Incremented { amount }]
        }
        CounterCommand::Decrement { amount } => {
            vec![CounterEvent::Decremented { amount }]
        }
        CounterCommand::Reset => {
            if entity.state.value == 0 {
                return Ok(()); // No-op: already at zero
            }
            vec![CounterEvent::Reset]
        }
    };

    let raw_events: Vec<wee_events::RawEvent> = events
        .iter()
        .map(wee_events::to_raw_event)
        .collect::<Result<_, _>>()?;

    let options = PublishOptions {
        expected_revision: Some(entity.revision.clone()),
        ..Default::default()
    };

    store.publish(aggregate_id, options, raw_events).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: load + render
// ---------------------------------------------------------------------------

async fn load_entity(
    store: &MemoryStore,
    renderer: &Renderer<CounterState>,
    id: &AggregateId,
) -> Result<Entity<CounterState>, wee_events::Error> {
    let aggregate = store.load(id).await?;
    renderer.render(&aggregate)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_aggregate_renders_default_state() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let entity = load_entity(&store, &renderer, &id).await.unwrap();

    assert_eq!(entity.state.value, 0);
    assert_eq!(entity.state.event_count, 0);
    assert!(!entity.initialized());
}

#[tokio::test]
async fn increment_updates_state() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &entity,
        CounterCommand::Increment { amount: 5 },
    )
    .await
    .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    assert_eq!(entity.state.value, 5);
    assert_eq!(entity.state.event_count, 1);
    assert!(entity.initialized());
}

#[tokio::test]
async fn multiple_commands_accumulate() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &entity,
        CounterCommand::Increment { amount: 10 },
    )
    .await
    .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &entity,
        CounterCommand::Increment { amount: 5 },
    )
    .await
    .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &entity,
        CounterCommand::Decrement { amount: 3 },
    )
    .await
    .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    assert_eq!(entity.state.value, 12); // 10 + 5 - 3
    assert_eq!(entity.state.event_count, 3);
}

#[tokio::test]
async fn reset_zeroes_value() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &entity,
        CounterCommand::Increment { amount: 42 },
    )
    .await
    .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(&store, &id, &entity, CounterCommand::Reset)
        .await
        .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    assert_eq!(entity.state.value, 0);
    assert_eq!(entity.state.event_count, 2); // increment + reset
}

#[tokio::test]
async fn reset_noop_when_already_zero() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(&store, &id, &entity, CounterCommand::Reset)
        .await
        .unwrap();

    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    assert_eq!(entity.state.value, 0);
    assert_eq!(entity.state.event_count, 0);
    assert!(!entity.initialized());
}

#[tokio::test]
async fn optimistic_concurrency_rejects_stale_revision() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id = AggregateId::new("counter", "test-1");

    let stale_entity = load_entity(&store, &renderer, &id).await.unwrap();

    let fresh_entity = load_entity(&store, &renderer, &id).await.unwrap();
    handle_command(
        &store,
        &id,
        &fresh_entity,
        CounterCommand::Increment { amount: 1 },
    )
    .await
    .unwrap();

    let result = handle_command(
        &store,
        &id,
        &stale_entity,
        CounterCommand::Increment { amount: 1 },
    )
    .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        wee_events::Error::RevisionConflict { .. } => {}
        other => panic!("expected RevisionConflict, got: {other}"),
    }
}

#[tokio::test]
async fn separate_aggregates_are_independent() {
    let store = MemoryStore::new();
    let renderer = build_renderer();
    let id_a = AggregateId::new("counter", "a");
    let id_b = AggregateId::new("counter", "b");

    let entity_a = load_entity(&store, &renderer, &id_a).await.unwrap();
    handle_command(
        &store,
        &id_a,
        &entity_a,
        CounterCommand::Increment { amount: 100 },
    )
    .await
    .unwrap();

    let entity_b = load_entity(&store, &renderer, &id_b).await.unwrap();
    handle_command(
        &store,
        &id_b,
        &entity_b,
        CounterCommand::Increment { amount: 1 },
    )
    .await
    .unwrap();

    let entity_a = load_entity(&store, &renderer, &id_a).await.unwrap();
    let entity_b = load_entity(&store, &renderer, &id_b).await.unwrap();

    assert_eq!(entity_a.state.value, 100);
    assert_eq!(entity_b.state.value, 1);
}

#[tokio::test]
async fn changeset_contains_published_events() {
    let store = MemoryStore::new();
    let id = AggregateId::new("counter", "test-1");

    let events = vec![
        wee_events::to_raw_event(&CounterEvent::Incremented { amount: 1 }).unwrap(),
        wee_events::to_raw_event(&CounterEvent::Incremented { amount: 2 }).unwrap(),
    ];

    let changeset = store
        .publish(&id, PublishOptions::default(), events)
        .await
        .unwrap();

    assert_eq!(changeset.events.len(), 2);
    assert_eq!(changeset.aggregate_id, id);
}

#[tokio::test]
async fn derive_macros_generate_correct_names() {
    let event = CounterEvent::Incremented { amount: 1 };
    assert_eq!(event.event_type(), EventType::new("counter:incremented"));

    let event = CounterEvent::Decremented { amount: 1 };
    assert_eq!(event.event_type(), EventType::new("counter:decremented"));

    let event = CounterEvent::Reset;
    assert_eq!(event.event_type(), EventType::new("counter:reset"));

    assert_eq!(CounterEvent::INCREMENTED, "counter:incremented");
    assert_eq!(CounterEvent::DECREMENTED, "counter:decremented");
    assert_eq!(CounterEvent::RESET, "counter:reset");

    let cmd = CounterCommand::Increment { amount: 1 };
    assert_eq!(
        cmd.command_name(),
        wee_events::CommandName::new("counter:increment")
    );

    let cmd = CounterCommand::Reset;
    assert_eq!(
        cmd.command_name(),
        wee_events::CommandName::new("counter:reset")
    );
}

#[tokio::test]
async fn unmapped_events_silently_skipped() {
    let store = MemoryStore::new();
    let id = AggregateId::new("counter", "test-1");

    let unknown_event = wee_events::RawEvent {
        event_type: EventType::new("counter:unknown-event"),
        data: EventData::json(&serde_json::json!({"foo": "bar"})).unwrap(),
    };
    store
        .publish(&id, PublishOptions::default(), vec![unknown_event])
        .await
        .unwrap();

    let renderer = build_renderer();
    let entity = load_entity(&store, &renderer, &id).await.unwrap();
    assert_eq!(entity.state.value, 0);
    assert_eq!(entity.state.event_count, 0);
    assert!(entity.initialized());
}
