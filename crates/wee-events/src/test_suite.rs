//! Event store conformance test suite.
//!
//! Ported from wee-events-go's `EventStoreValidationSuite`. Every `EventStore`
//! implementation must pass all tests in this suite.
//!
//! # Usage
//!
//! ```text
//! wee_events::store_test_suite!(memory_store_suite, || {
//!     wee_events::MemoryStore::new()
//! });
//! ```

use serde::{Deserialize, Serialize};

use crate::event::EventData;
use crate::id::{AggregateId, CorrelationId, EventType, Revision};
use crate::store::{EventStore, PublishOptions, RawEvent};

/// Test event used by the conformance suite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreValidationEvent {
    pub test_string_value: String,
    pub test_int_value: i64,
}

impl StoreValidationEvent {
    pub const EVENT_TYPE: &'static str = "test:store-validation-event";
}

/// Creates a test aggregate id with a unique key.
pub fn make_test_aggregate_id() -> AggregateId {
    AggregateId::new("test", ulid::Ulid::new().to_string())
}

/// Creates a test event with random-ish data.
pub fn make_test_event(index: usize) -> StoreValidationEvent {
    StoreValidationEvent {
        test_string_value: format!("test-value-{}-{}", index, ulid::Ulid::new()),
        test_int_value: index as i64 * 17 + 42,
    }
}

/// Serializes a test event into a `RawEvent` with JSON encoding.
pub fn make_raw_event(event: &StoreValidationEvent) -> RawEvent {
    RawEvent {
        event_type: EventType::new(StoreValidationEvent::EVENT_TYPE),
        data: EventData::json(event).expect("StoreValidationEvent always serializes"),
    }
}

/// Creates `count` raw events.
pub fn make_raw_events(count: usize) -> (Vec<StoreValidationEvent>, Vec<RawEvent>) {
    let events: Vec<StoreValidationEvent> = (0..count).map(make_test_event).collect();
    let raw: Vec<RawEvent> = events.iter().map(make_raw_event).collect();
    (events, raw)
}

/// Returns the last recorded event for an aggregate, or panics.
pub async fn last_event(store: &impl EventStore, id: &AggregateId) -> crate::RecordedEvent {
    let agg = store
        .load(id)
        .await
        .expect("load must succeed in test helper");
    assert!(!agg.is_empty(), "expected events but aggregate was empty");
    agg.into_events()
        .into_iter()
        .last()
        .expect("into_events is non-empty: is_empty() was false")
}

// ---------------------------------------------------------------------------
// Conformance test functions
// ---------------------------------------------------------------------------

/// Loads an initial revision (empty aggregate).
pub async fn load_initial(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let aggregate = store.load(&id).await.expect("load must succeed");

    assert!(aggregate.is_empty());
    assert_eq!(*aggregate.revision(), Revision::zero());
    assert_eq!(aggregate.id, id);
}

/// Loads a revision with events.
pub async fn loads_revision_with_events(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw) = make_raw_events(1);

    store
        .publish(&id, PublishOptions::default(), raw)
        .await
        .expect("publish must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");

    assert!(!aggregate.is_empty());
    assert_eq!(aggregate.id, id);
    assert_ne!(*aggregate.revision(), Revision::zero());
}

/// Publishes a single event.
pub async fn publishes_single_event(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw) = make_raw_events(1);

    store
        .publish(&id, PublishOptions::default(), raw)
        .await
        .expect("publish must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(aggregate.len(), 1);
}

/// Publishes multiple events in a single transaction.
pub async fn publishes_multiple_events(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw) = make_raw_events(17);

    store
        .publish(&id, PublishOptions::default(), raw)
        .await
        .expect("publish must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(aggregate.len(), 17);
}

/// Preserves event content when recording.
pub async fn validate_event_content(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (events, raw) = make_raw_events(17);

    store
        .publish(&id, PublishOptions::default(), raw)
        .await
        .expect("publish must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(events.len(), aggregate.len());

    for (i, recorded) in aggregate.events().iter().enumerate() {
        assert_eq!(
            recorded.event_type,
            EventType::new(StoreValidationEvent::EVENT_TYPE)
        );

        assert!(
            recorded.data.is_json(),
            "expected JSON encoding at index {i}"
        );
        let deserialized: StoreValidationEvent = recorded
            .data
            .deserialize_json()
            .expect("deserialization must succeed");
        assert_eq!(
            events[i], deserialized,
            "event content mismatch at index {i}"
        );
    }
}

/// Publishes with an expected initial revision.
pub async fn publishes_with_expected_initial_revision(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw) = make_raw_events(1);

    let opts = PublishOptions {
        expected_revision: Some(Revision::zero()),
        ..Default::default()
    };

    store
        .publish(&id, opts, raw)
        .await
        .expect("publish with zero revision must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(aggregate.len(), 1);
}

/// Publishes with an expected revision (after prior events).
pub async fn publishes_with_expected_revision(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw1) = make_raw_events(1);

    store
        .publish(&id, PublishOptions::default(), raw1)
        .await
        .expect("first publish must succeed");

    let first = store.load(&id).await.expect("load must succeed");

    let (_, raw2) = make_raw_events(1);
    let opts = PublishOptions {
        expected_revision: Some(first.revision().clone()),
        ..Default::default()
    };
    store
        .publish(&id, opts, raw2)
        .await
        .expect("second publish with expected revision must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(aggregate.len(), 2);
}

/// Returns a revision conflict with an initial revision.
pub async fn revision_conflict_on_initial_revision(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw1) = make_raw_events(1);

    // Publish without expected revision (succeeds)
    store
        .publish(&id, PublishOptions::default(), raw1)
        .await
        .expect("first publish must succeed");

    // Publish with expected=initial (should conflict)
    let (_, raw2) = make_raw_events(1);
    let opts = PublishOptions {
        expected_revision: Some(Revision::zero()),
        ..Default::default()
    };
    let result = store.publish(&id, opts, raw2).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        crate::Error::RevisionConflict { .. } => {}
        other => panic!("expected RevisionConflict, got: {other}"),
    }
}

/// Returns a revision conflict on subsequent revision.
pub async fn revision_conflict_on_subsequent_revision(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw1) = make_raw_events(1);

    store
        .publish(&id, PublishOptions::default(), raw1)
        .await
        .expect("first publish must succeed");

    let first = store.load(&id).await.expect("load must succeed");

    // Publish again (advances revision past first.revision)
    let (_, raw2) = make_raw_events(1);
    store
        .publish(&id, PublishOptions::default(), raw2)
        .await
        .expect("second publish must succeed");

    // Publish with stale expected revision (should conflict)
    let (_, raw3) = make_raw_events(1);
    let opts = PublishOptions {
        expected_revision: Some(first.revision().clone()),
        ..Default::default()
    };
    let result = store.publish(&id, opts, raw3).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        crate::Error::RevisionConflict { .. } => {}
        other => panic!("expected RevisionConflict, got: {other}"),
    }
}

/// Supports causation id tracking.
pub async fn causation(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw1) = make_raw_events(1);

    store
        .publish(&id, PublishOptions::default(), raw1)
        .await
        .expect("first publish must succeed");

    let first = last_event(store, &id).await;

    let correlation_id = CorrelationId::new(format!("event/{}", first.event_id));
    let (_, raw2) = make_raw_events(1);
    let opts = PublishOptions {
        causation_id: Some(first.event_id.clone()),
        correlation_id: Some(correlation_id.clone()),
        ..Default::default()
    };

    store
        .publish(&id, opts, raw2)
        .await
        .expect("second publish must succeed");

    let second = last_event(store, &id).await;

    assert_eq!(
        second.metadata.correlation_id.as_ref(),
        Some(&correlation_id)
    );
    assert_eq!(second.metadata.causation_id, Some(first.event_id));
}

/// Stale expected revision is detected and retry succeeds.
///
/// Simulates the real-world pattern: load entity, another writer advances
/// the aggregate, attempt to publish with the stale revision (fails),
/// reload current state, retry with fresh revision (succeeds).
pub async fn stale_revision_detected_and_retry_succeeds(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw1) = make_raw_events(1);

    // Initial publish
    store
        .publish(&id, PublishOptions::default(), raw1)
        .await
        .expect("initial publish must succeed");

    // "Load" the aggregate — capture its revision
    let snapshot = store.load(&id).await.expect("load must succeed");
    let stale_revision = snapshot.revision().clone();

    // Another writer advances the aggregate. The monotonic ULID generator
    // guarantees the new revision is greater without requiring wall-clock
    // advancement — no sleep needed.
    let (_, raw2) = make_raw_events(1);
    store
        .publish(&id, PublishOptions::default(), raw2)
        .await
        .expect("concurrent publish must succeed");

    // Attempt to publish with the stale revision — must fail
    let (_, raw3) = make_raw_events(1);
    let result = store
        .publish(
            &id,
            PublishOptions {
                expected_revision: Some(stale_revision),
                ..Default::default()
            },
            raw3,
        )
        .await;

    assert!(result.is_err(), "stale revision should be rejected");
    match result.unwrap_err() {
        crate::Error::RevisionConflict { .. } => {}
        other => panic!("expected RevisionConflict, got: {other}"),
    }

    // Retry: reload current state, publish with fresh revision
    let fresh = store.load(&id).await.expect("reload must succeed");
    let (_, raw4) = make_raw_events(1);
    store
        .publish(
            &id,
            PublishOptions {
                expected_revision: Some(fresh.revision().clone()),
                ..Default::default()
            },
            raw4,
        )
        .await
        .expect("retry with fresh revision must succeed");

    // Verify: 3 events total (initial + concurrent writer + retry)
    let final_agg = store.load(&id).await.expect("final load must succeed");
    assert_eq!(final_agg.len(), 3);
}

/// Publish of an empty event list returns the current revision unchanged.
pub async fn empty_publish_returns_current_revision(store: &impl EventStore) {
    let id = make_test_aggregate_id();
    let (_, raw) = make_raw_events(2);

    let changeset = store
        .publish(&id, PublishOptions::default(), raw)
        .await
        .expect("publish must succeed");
    let known_revision = changeset.revision.clone();

    let empty_changeset = store
        .publish(&id, PublishOptions::default(), vec![])
        .await
        .expect("empty publish must succeed");

    assert_eq!(empty_changeset.revision, known_revision);
    assert!(empty_changeset.events.is_empty());
}

/// Multiple batches come back in revision order.
pub async fn event_ordering_preserved(store: &impl EventStore) {
    let id = make_test_aggregate_id();

    let (_, batch1) = make_raw_events(2);
    let (_, batch2) = make_raw_events(3);

    store
        .publish(&id, PublishOptions::default(), batch1)
        .await
        .expect("first batch publish must succeed");

    store
        .publish(&id, PublishOptions::default(), batch2)
        .await
        .expect("second batch publish must succeed");

    let aggregate = store.load(&id).await.expect("load must succeed");
    assert_eq!(aggregate.len(), 5);

    // Revisions must be strictly ascending
    let revisions: Vec<_> = aggregate
        .events()
        .iter()
        .map(|e| e.revision.clone())
        .collect();
    for window in revisions.windows(2) {
        assert!(
            window[0] < window[1],
            "revision {} must precede {}",
            window[0],
            window[1]
        );
    }
}

/// Generates a test module that runs the conformance suite against a store
/// constructed by the provided factory expression.
///
/// # Usage
///
/// ```text
/// wee_events::store_test_suite!(my_suite, || {
///     MyStore::new()
/// });
/// ```
#[macro_export]
macro_rules! store_test_suite {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            #[tokio::test]
            async fn load_initial() {
                let store = $factory;
                $crate::test_suite::load_initial(&store).await;
            }

            #[tokio::test]
            async fn loads_revision_with_events() {
                let store = $factory;
                $crate::test_suite::loads_revision_with_events(&store).await;
            }

            #[tokio::test]
            async fn publishes_single_event() {
                let store = $factory;
                $crate::test_suite::publishes_single_event(&store).await;
            }

            #[tokio::test]
            async fn publishes_multiple_events() {
                let store = $factory;
                $crate::test_suite::publishes_multiple_events(&store).await;
            }

            #[tokio::test]
            async fn validate_event_content() {
                let store = $factory;
                $crate::test_suite::validate_event_content(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_initial_revision() {
                let store = $factory;
                $crate::test_suite::publishes_with_expected_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn publishes_with_expected_revision() {
                let store = $factory;
                $crate::test_suite::publishes_with_expected_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_initial_revision() {
                let store = $factory;
                $crate::test_suite::revision_conflict_on_initial_revision(&store).await;
            }

            #[tokio::test]
            async fn revision_conflict_on_subsequent_revision() {
                let store = $factory;
                $crate::test_suite::revision_conflict_on_subsequent_revision(&store).await;
            }

            #[tokio::test]
            async fn causation() {
                let store = $factory;
                $crate::test_suite::causation(&store).await;
            }

            #[tokio::test]
            async fn stale_revision_detected_and_retry_succeeds() {
                let store = $factory;
                $crate::test_suite::stale_revision_detected_and_retry_succeeds(&store).await;
            }

            #[tokio::test]
            async fn empty_publish_returns_current_revision() {
                let store = $factory;
                $crate::test_suite::empty_publish_returns_current_revision(&store).await;
            }

            #[tokio::test]
            async fn event_ordering_preserved() {
                let store = $factory;
                $crate::test_suite::event_ordering_preserved(&store).await;
            }
        }
    };
}
