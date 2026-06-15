use wee_events::{Encoding, EventData, EventStore as _, PublishOptions, RawEvent};
use wee_events_sqlite::{GlobalStrategy, SqliteEventStore};

fn raw_event(event_type: &str) -> RawEvent {
    RawEvent {
        event_type: event_type.into(),
        data: EventData::raw(Encoding::Json, b"{}".to_vec()),
    }
}

#[tokio::test]
async fn separate_store_instances_observe_each_others_commits() {
    let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
    let path = temp_dir.path().join("store.db");
    let aggregate_id = wee_events::AggregateId::new("order", "123");

    let store_a = SqliteEventStore::open_local(&path, GlobalStrategy)
        .await
        .expect("first store should open");
    let store_b = SqliteEventStore::open_local(&path, GlobalStrategy)
        .await
        .expect("second store should open");

    store_a
        .publish(
            &aggregate_id,
            PublishOptions::default(),
            vec![raw_event("created")],
        )
        .await
        .expect("first publish should succeed");

    let snapshot_from_b = store_b
        .load(&aggregate_id)
        .await
        .expect("second store should observe first commit");
    assert_eq!(snapshot_from_b.len(), 1);

    store_b
        .publish(
            &aggregate_id,
            PublishOptions::default(),
            vec![raw_event("updated")],
        )
        .await
        .expect("second publish should succeed");

    let snapshot_from_a = store_a
        .load(&aggregate_id)
        .await
        .expect("first store should observe second commit");
    assert_eq!(snapshot_from_a.len(), 2);
}

#[tokio::test]
async fn stale_revision_conflicts_across_store_instances() {
    let temp_dir = tempfile::tempdir().expect("tempdir should succeed");
    let path = temp_dir.path().join("store.db");
    let aggregate_id = wee_events::AggregateId::new("order", "123");

    let store_a = SqliteEventStore::open_local(&path, GlobalStrategy)
        .await
        .expect("first store should open");
    let store_b = SqliteEventStore::open_local(&path, GlobalStrategy)
        .await
        .expect("second store should open");

    store_a
        .publish(
            &aggregate_id,
            PublishOptions::default(),
            vec![raw_event("created")],
        )
        .await
        .expect("initial publish should succeed");

    let stale_revision = store_a
        .load(&aggregate_id)
        .await
        .expect("first store should load current state")
        .revision()
        .clone();

    store_b
        .publish(
            &aggregate_id,
            PublishOptions::default(),
            vec![raw_event("updated")],
        )
        .await
        .expect("concurrent publish should succeed");

    let error = store_a
        .publish(
            &aggregate_id,
            PublishOptions {
                expected_revision: Some(stale_revision),
                ..Default::default()
            },
            vec![raw_event("stale-write")],
        )
        .await
        .expect_err("stale revision should be rejected");

    assert!(
        matches!(error, wee_events::Error::RevisionConflict { .. }),
        "unexpected error: {error}"
    );
}
