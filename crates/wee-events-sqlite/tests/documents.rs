use serde::{Deserialize, Serialize};
use wee_events::{
    AggregateId, AggregateType, EventData, EventStore, EventType, PublishOptions, RawEvent,
    ReduceFn, Renderer, Revision,
};
use wee_events_sqlite::SqliteStore;

fn test_store() -> SqliteStore {
    SqliteStore::open_in_memory().unwrap()
}

// ---------------------------------------------------------------------------
// Document Store tests
// ---------------------------------------------------------------------------

#[test]
fn upsert_and_get_round_trip() {
    let store = test_store();
    let revision = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data = serde_json::json!({"name": "test", "value": 42});

    let rows = store
        .documents()
        .upsert("campaigns", "c1", &revision, &data)
        .unwrap();
    assert_eq!(rows, 1);

    let doc = store.documents().get("campaigns", "c1").unwrap().unwrap();
    assert_eq!(doc.key, "c1");
    assert_eq!(doc.revision, revision);
    assert_eq!(doc.data, data);
}

#[test]
fn upsert_overwrites_with_newer_revision() {
    let store = test_store();
    let rev1 = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let rev2 = Revision::new("01BBBBBBBBBBBBBBBBBBBBBBBBB");
    let data1 = serde_json::json!({"version": 1});
    let data2 = serde_json::json!({"version": 2});

    store
        .documents()
        .upsert("campaigns", "c1", &rev1, &data1)
        .unwrap();

    let rows = store
        .documents()
        .upsert("campaigns", "c1", &rev2, &data2)
        .unwrap();
    assert_eq!(rows, 1);

    let doc = store.documents().get("campaigns", "c1").unwrap().unwrap();
    assert_eq!(doc.revision, rev2);
    assert_eq!(doc.data, data2);
}

#[test]
fn upsert_ignores_stale_revision() {
    let store = test_store();
    let rev_newer = Revision::new("01BBBBBBBBBBBBBBBBBBBBBBBBB");
    let rev_older = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data_current = serde_json::json!({"version": "current"});
    let data_stale = serde_json::json!({"version": "stale"});

    store
        .documents()
        .upsert("campaigns", "c1", &rev_newer, &data_current)
        .unwrap();

    let rows = store
        .documents()
        .upsert("campaigns", "c1", &rev_older, &data_stale)
        .unwrap();
    assert_eq!(rows, 0, "stale revision should be a no-op");

    let doc = store.documents().get("campaigns", "c1").unwrap().unwrap();
    assert_eq!(doc.data["version"], "current");
}

#[test]
fn upsert_with_equal_revision_is_idempotent() {
    let store = test_store();
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data_original = serde_json::json!({"version": "original"});
    let data_different = serde_json::json!({"version": "different"});

    store
        .documents()
        .upsert("campaigns", "c1", &rev, &data_original)
        .unwrap();

    // Same revision, different data — should be a no-op (not greater)
    let rows = store
        .documents()
        .upsert("campaigns", "c1", &rev, &data_different)
        .unwrap();
    assert_eq!(rows, 0, "equal revision should be a no-op");

    let doc = store.documents().get("campaigns", "c1").unwrap().unwrap();
    assert_eq!(doc.data["version"], "original", "original data preserved");
}

#[test]
fn get_returns_none_for_missing() {
    let store = test_store();
    let result = store.documents().get("campaigns", "nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn list_returns_all_in_collection() {
    let store = test_store();
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    store
        .documents()
        .upsert("campaigns", "c1", &rev, &serde_json::json!({"id": "c1"}))
        .unwrap();
    store
        .documents()
        .upsert("campaigns", "c2", &rev, &serde_json::json!({"id": "c2"}))
        .unwrap();
    store
        .documents()
        .upsert("characters", "ch1", &rev, &serde_json::json!({"id": "ch1"}))
        .unwrap();

    let campaigns = store.documents().list("campaigns").unwrap();
    assert_eq!(campaigns.len(), 2);

    let characters = store.documents().list("characters").unwrap();
    assert_eq!(characters.len(), 1);
}

#[test]
fn delete_removes_document() {
    let store = test_store();
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data = serde_json::json!({"name": "delete me"});

    store
        .documents()
        .upsert("campaigns", "c1", &rev, &data)
        .unwrap();

    let deleted = store.documents().delete("campaigns", "c1").unwrap();
    assert!(deleted);

    let result = store.documents().get("campaigns", "c1").unwrap();
    assert!(result.is_none());
}

#[test]
fn delete_returns_false_for_missing() {
    let store = test_store();
    let deleted = store
        .documents()
        .delete("campaigns", "nonexistent")
        .unwrap();
    assert!(!deleted);
}

#[test]
fn collections_are_isolated() {
    let store = test_store();
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    store
        .documents()
        .upsert(
            "collection-a",
            "key1",
            &rev,
            &serde_json::json!({"from": "a"}),
        )
        .unwrap();
    store
        .documents()
        .upsert(
            "collection-b",
            "key1",
            &rev,
            &serde_json::json!({"from": "b"}),
        )
        .unwrap();

    let doc_a = store
        .documents()
        .get("collection-a", "key1")
        .unwrap()
        .unwrap();
    assert_eq!(doc_a.data["from"], "a");

    let doc_b = store
        .documents()
        .get("collection-b", "key1")
        .unwrap()
        .unwrap();
    assert_eq!(doc_b.data["from"], "b");
}

#[test]
fn json_valid_constraint_accepts_valid_json() {
    let store = test_store();
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    // The CHECK(json_valid(data)) constraint is exercised by every upsert.
    // The DocumentStore API always serializes valid JSON via serde_json, so
    // the constraint can't be triggered through the Rust API — but this test
    // confirms the constraint doesn't reject legitimate JSON values.
    store
        .documents()
        .upsert("test", "valid", &rev, &serde_json::json!({"ok": true}))
        .unwrap();

    let doc = store.documents().get("test", "valid").unwrap();
    assert!(doc.is_some());
}

// ---------------------------------------------------------------------------
// Projection tests
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
struct CounterState {
    value: i64,
    event_count: u32,
}

fn reduce_incremented(
    state: &mut CounterState,
    _event_type: &EventType,
    data: &EventData,
) -> Result<(), wee_events::Error> {
    #[derive(Deserialize)]
    struct Payload {
        amount: i64,
    }
    let payload: Payload = data.deserialize_json()?;
    state.value += payload.amount;
    state.event_count += 1;
    Ok(())
}

fn reduce_decremented(
    state: &mut CounterState,
    _event_type: &EventType,
    data: &EventData,
) -> Result<(), wee_events::Error> {
    #[derive(Deserialize)]
    struct Payload {
        amount: i64,
    }
    let payload: Payload = data.deserialize_json()?;
    state.value -= payload.amount;
    state.event_count += 1;
    Ok(())
}

fn counter_renderer() -> Renderer<CounterState> {
    Renderer::new()
        .with(
            "counter:incremented",
            reduce_incremented as ReduceFn<CounterState>,
        )
        .with(
            "counter:decremented",
            reduce_decremented as ReduceFn<CounterState>,
        )
}

fn make_counter_event(event_type: &str, amount: i64) -> RawEvent {
    RawEvent {
        event_type: EventType::new(event_type),
        data: EventData::json(&serde_json::json!({"amount": amount})).unwrap(),
    }
}

#[tokio::test]
async fn apply_projection_creates_document() {
    let store = test_store();
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    let changeset = store
        .events()
        .publish(
            &id,
            PublishOptions::default(),
            vec![
                make_counter_event("counter:incremented", 10),
                make_counter_event("counter:incremented", 5),
                make_counter_event("counter:decremented", 3),
            ],
        )
        .await
        .unwrap();

    wee_events_sqlite::apply_projection(&renderer, &store, &changeset, "counters")
        .await
        .unwrap();

    let doc = store.documents().get("counters", "c1").unwrap().unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 12);
    assert_eq!(state.event_count, 3);
    assert_eq!(doc.revision, changeset.revision);
}

#[tokio::test]
async fn apply_projection_updates_existing_document() {
    let store = test_store();
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    // First publish
    let cs1 = store
        .events()
        .publish(
            &id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(&renderer, &store, &cs1, "counters")
        .await
        .unwrap();

    // Second publish
    let cs2 = store
        .events()
        .publish(
            &id,
            PublishOptions {
                expected_revision: Some(cs1.revision.clone()),
                ..Default::default()
            },
            vec![make_counter_event("counter:incremented", 5)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(&renderer, &store, &cs2, "counters")
        .await
        .unwrap();

    let doc = store.documents().get("counters", "c1").unwrap().unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 15);
    assert_eq!(state.event_count, 2);
    assert_eq!(doc.revision, cs2.revision);
}

#[tokio::test]
async fn rebuild_projection_populates_all_aggregates() {
    let store = test_store();
    let renderer = counter_renderer();

    // Publish to 3 different counters
    for (key, amount) in [("c1", 10), ("c2", 20), ("c3", 30)] {
        let id = AggregateId::new("counter", key);
        store
            .events()
            .publish(
                &id,
                PublishOptions::default(),
                vec![make_counter_event("counter:incremented", amount)],
            )
            .await
            .unwrap();
    }

    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(&renderer, &store, "counters", &counter_type)
        .await
        .unwrap();

    let docs = store.documents().list("counters").unwrap();
    assert_eq!(docs.len(), 3);

    for doc in docs {
        let state: CounterState = serde_json::from_value(doc.data).unwrap();
        let expected = match doc.key.as_str() {
            "c1" => 10,
            "c2" => 20,
            "c3" => 30,
            _ => panic!("unexpected key: {}", doc.key),
        };
        assert_eq!(state.value, expected);
    }
}

#[tokio::test]
async fn rebuild_projection_skips_current_documents() {
    let store = test_store();
    let renderer = counter_renderer();

    // Create two counters: c1 is current, c2 is stale
    let id1 = AggregateId::new("counter", "c1");
    let cs1 = store
        .events()
        .publish(
            &id1,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(&renderer, &store, &cs1, "counters")
        .await
        .unwrap();

    let id2 = AggregateId::new("counter", "c2");
    let cs2 = store
        .events()
        .publish(
            &id2,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 20)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(&renderer, &store, &cs2, "counters")
        .await
        .unwrap();

    // Publish more events to c2 WITHOUT projecting — c2 is now stale
    store
        .events()
        .publish(
            &id2,
            PublishOptions {
                expected_revision: Some(cs2.revision.clone()),
                ..Default::default()
            },
            vec![make_counter_event("counter:incremented", 5)],
        )
        .await
        .unwrap();

    // Rebuild: c1 should be skipped (current), c2 should be updated (stale)
    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(&renderer, &store, "counters", &counter_type)
        .await
        .unwrap();

    // c1 unchanged (was already current)
    let doc1 = store.documents().get("counters", "c1").unwrap().unwrap();
    let state1: CounterState = serde_json::from_value(doc1.data).unwrap();
    assert_eq!(state1.value, 10);
    assert_eq!(
        doc1.revision, cs1.revision,
        "c1 revision should not have changed"
    );

    // c2 updated (was stale)
    let doc2 = store.documents().get("counters", "c2").unwrap().unwrap();
    let state2: CounterState = serde_json::from_value(doc2.data).unwrap();
    assert_eq!(state2.value, 25, "c2 should include the new event");
    assert_ne!(
        doc2.revision, cs2.revision,
        "c2 revision should have advanced"
    );
}

#[tokio::test]
async fn rebuild_projection_updates_stale_documents() {
    let store = test_store();
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    // Publish and project
    let cs1 = store
        .events()
        .publish(
            &id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(&renderer, &store, &cs1, "counters")
        .await
        .unwrap();

    // Publish more events WITHOUT projecting
    store
        .events()
        .publish(
            &id,
            PublishOptions {
                expected_revision: Some(cs1.revision.clone()),
                ..Default::default()
            },
            vec![make_counter_event("counter:incremented", 5)],
        )
        .await
        .unwrap();

    // Rebuild should detect stale document and re-render
    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(&renderer, &store, "counters", &counter_type)
        .await
        .unwrap();

    let doc = store.documents().get("counters", "c1").unwrap().unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 15);
}

#[tokio::test]
async fn projection_ignores_other_aggregate_types() {
    let store = test_store();
    let renderer = counter_renderer();

    // Publish to a counter and a non-counter
    let counter_id = AggregateId::new("counter", "c1");
    store
        .events()
        .publish(
            &counter_id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();

    let other_id = AggregateId::new("other-type", "o1");
    store
        .events()
        .publish(
            &other_id,
            PublishOptions::default(),
            vec![RawEvent {
                event_type: EventType::new("other:something"),
                data: EventData::json(&serde_json::json!({})).unwrap(),
            }],
        )
        .await
        .unwrap();

    // Rebuild only for counter type
    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(&renderer, &store, "counters", &counter_type)
        .await
        .unwrap();

    let docs = store.documents().list("counters").unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].key, "c1");
}
