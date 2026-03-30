use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use libsql::Builder;
use serde::{Deserialize, Serialize};
use wee_events::{
    AggregateId, AggregateType, EventData, EventStore as _, EventType, PublishOptions, RawEvent,
    ReduceFn, Renderer, Revision,
};
use wee_events_sqlite::{
    AggregateStrategy, DocumentStore, EventStore, GlobalStrategy, HashedStrategy,
    LocalPartitionStrategy, PartitionByStrategy, TypeStrategy,
};

trait LocalStorePath {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf;
}

impl LocalStorePath for GlobalStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().join("store.db")
    }
}

impl LocalStorePath for TypeStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for AggregateStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for HashedStrategy {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

impl LocalStorePath for PartitionByStrategy<fn(&AggregateId) -> String> {
    fn local_store_path(temp_dir: &tempfile::TempDir) -> PathBuf {
        temp_dir.path().to_path_buf()
    }
}

fn partition_by_user(aggregate_id: &AggregateId) -> String {
    aggregate_id
        .aggregate_key
        .split(':')
        .next()
        .expect("split always yields at least one segment")
        .to_string()
}

async fn test_document_store() -> DocumentStore {
    DocumentStore::open_in_memory().await.unwrap()
}

struct SharedStores {
    event_db_path: PathBuf,
    document_db_path: PathBuf,
    event_store: EventStore,
    document_store: DocumentStore,
}

impl SharedStores {
    async fn new() -> Self {
        let temp_dir = std::env::temp_dir();
        let event_db_path =
            temp_dir.join(format!("wee-events-sqlite-events-{}.db", ulid::Ulid::new()));
        let document_db_path = temp_dir.join(format!(
            "wee-events-sqlite-documents-{}.db",
            ulid::Ulid::new()
        ));
        let event_store = EventStore::builder()
            .local(&event_db_path)
            .strategy(GlobalStrategy)
            .open()
            .await
            .unwrap();
        let document_store = DocumentStore::open(&document_db_path).await.unwrap();

        Self {
            event_db_path,
            document_db_path,
            event_store,
            document_store,
        }
    }
}

impl Drop for SharedStores {
    fn drop(&mut self) {
        remove_db_artifacts(&self.event_db_path);
        remove_db_artifacts(&self.document_db_path);
    }
}

fn remove_db_artifacts(db_path: &Path) {
    let db_path = db_path.to_string_lossy();
    for suffix in ["", "-shm", "-wal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
}

async fn table_exists(db_path: &Path, table_name: &str) -> bool {
    let db = Builder::new_local(db_path).build().await.unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table_name],
        )
        .await
        .unwrap();

    rows.next().await.unwrap().is_some()
}

// ---------------------------------------------------------------------------
// Document Store tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upsert_and_get_round_trip() {
    let store = test_document_store().await;
    let revision = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data = serde_json::json!({"name": "test", "value": 42});

    let rows = store
        .upsert("campaigns", "c1", &revision, &data)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    let doc = store.get("campaigns", "c1").await.unwrap().unwrap();
    assert_eq!(doc.key, "c1");
    assert_eq!(doc.revision, revision);
    assert_eq!(doc.data, data);
}

#[tokio::test]
async fn upsert_overwrites_with_newer_revision() {
    let store = test_document_store().await;
    let rev1 = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let rev2 = Revision::new("01BBBBBBBBBBBBBBBBBBBBBBBBB");
    let data1 = serde_json::json!({"version": 1});
    let data2 = serde_json::json!({"version": 2});

    store
        .upsert("campaigns", "c1", &rev1, &data1)
        .await
        .unwrap();

    let rows = store
        .upsert("campaigns", "c1", &rev2, &data2)
        .await
        .unwrap();
    assert_eq!(rows, 1);

    let doc = store.get("campaigns", "c1").await.unwrap().unwrap();
    assert_eq!(doc.revision, rev2);
    assert_eq!(doc.data, data2);
}

#[tokio::test]
async fn upsert_ignores_stale_revision() {
    let store = test_document_store().await;
    let rev_newer = Revision::new("01BBBBBBBBBBBBBBBBBBBBBBBBB");
    let rev_older = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data_current = serde_json::json!({"version": "current"});
    let data_stale = serde_json::json!({"version": "stale"});

    store
        .upsert("campaigns", "c1", &rev_newer, &data_current)
        .await
        .unwrap();

    let rows = store
        .upsert("campaigns", "c1", &rev_older, &data_stale)
        .await
        .unwrap();
    assert_eq!(rows, 0, "stale revision should be a no-op");

    let doc = store.get("campaigns", "c1").await.unwrap().unwrap();
    assert_eq!(doc.data["version"], "current");
}

#[tokio::test]
async fn upsert_with_equal_revision_is_idempotent() {
    let store = test_document_store().await;
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data_original = serde_json::json!({"version": "original"});
    let data_different = serde_json::json!({"version": "different"});

    store
        .upsert("campaigns", "c1", &rev, &data_original)
        .await
        .unwrap();

    let rows = store
        .upsert("campaigns", "c1", &rev, &data_different)
        .await
        .unwrap();
    assert_eq!(rows, 0, "equal revision should be a no-op");

    let doc = store.get("campaigns", "c1").await.unwrap().unwrap();
    assert_eq!(doc.data["version"], "original", "original data preserved");
}

#[tokio::test]
async fn get_returns_none_for_missing() {
    let store = test_document_store().await;
    let result = store.get("campaigns", "nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn list_returns_all_in_collection() {
    let store = test_document_store().await;
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    store
        .upsert("campaigns", "c1", &rev, &serde_json::json!({"id": "c1"}))
        .await
        .unwrap();
    store
        .upsert("campaigns", "c2", &rev, &serde_json::json!({"id": "c2"}))
        .await
        .unwrap();
    store
        .upsert("characters", "ch1", &rev, &serde_json::json!({"id": "ch1"}))
        .await
        .unwrap();

    let campaigns = store.list("campaigns").await.unwrap();
    assert_eq!(campaigns.len(), 2);

    let characters = store.list("characters").await.unwrap();
    assert_eq!(characters.len(), 1);
}

#[tokio::test]
async fn delete_removes_document() {
    let store = test_document_store().await;
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");
    let data = serde_json::json!({"name": "delete me"});

    store.upsert("campaigns", "c1", &rev, &data).await.unwrap();

    let deleted = store.delete("campaigns", "c1").await.unwrap();
    assert!(deleted);

    let result = store.get("campaigns", "c1").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn delete_returns_false_for_missing() {
    let store = test_document_store().await;
    let deleted = store.delete("campaigns", "nonexistent").await.unwrap();
    assert!(!deleted);
}

#[tokio::test]
async fn collections_are_isolated() {
    let store = test_document_store().await;
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    store
        .upsert(
            "collection-a",
            "key1",
            &rev,
            &serde_json::json!({"from": "a"}),
        )
        .await
        .unwrap();
    store
        .upsert(
            "collection-b",
            "key1",
            &rev,
            &serde_json::json!({"from": "b"}),
        )
        .await
        .unwrap();

    let doc_a = store.get("collection-a", "key1").await.unwrap().unwrap();
    assert_eq!(doc_a.data["from"], "a");

    let doc_b = store.get("collection-b", "key1").await.unwrap().unwrap();
    assert_eq!(doc_b.data["from"], "b");
}

#[tokio::test]
async fn json_valid_constraint_accepts_valid_json() {
    let store = test_document_store().await;
    let rev = Revision::new("01AAAAAAAAAAAAAAAAAAAAAAAAA");

    store
        .upsert("test", "valid", &rev, &serde_json::json!({"ok": true}))
        .await
        .unwrap();

    let doc = store.get("test", "valid").await.unwrap();
    assert!(doc.is_some());
}

#[tokio::test]
async fn event_store_open_only_creates_event_schema() {
    let db_path = std::env::temp_dir().join(format!(
        "wee-events-sqlite-events-only-{}.db",
        ulid::Ulid::new()
    ));
    let _store = EventStore::builder()
        .local(&db_path)
        .strategy(GlobalStrategy)
        .open()
        .await
        .unwrap();

    assert!(table_exists(&db_path, "events").await);
    assert!(!table_exists(&db_path, "documents").await);

    remove_db_artifacts(&db_path);
}

#[tokio::test]
async fn document_store_open_only_creates_document_schema() {
    let db_path = std::env::temp_dir().join(format!(
        "wee-events-sqlite-documents-only-{}.db",
        ulid::Ulid::new()
    ));
    let _store = DocumentStore::open(&db_path).await.unwrap();

    assert!(table_exists(&db_path, "documents").await);
    assert!(!table_exists(&db_path, "events").await);

    remove_db_artifacts(&db_path);
}

#[tokio::test]
async fn enumerate_aggregates_works_across_partitioning_strategies() {
    assert_enumerates_aggregates(GlobalStrategy).await;
    assert_enumerates_aggregates(TypeStrategy).await;
    assert_enumerates_aggregates(AggregateStrategy).await;
    assert_enumerates_aggregates(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await;
    assert_enumerates_aggregates(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await;
}

#[tokio::test]
async fn enumerate_aggregates_by_type_works_across_partitioning_strategies() {
    assert_enumerates_aggregates_by_type(GlobalStrategy).await;
    assert_enumerates_aggregates_by_type(TypeStrategy).await;
    assert_enumerates_aggregates_by_type(AggregateStrategy).await;
    assert_enumerates_aggregates_by_type(HashedStrategy::new(NonZeroU32::new(8).unwrap())).await;
    assert_enumerates_aggregates_by_type(PartitionByStrategy::new(
        partition_by_user as fn(&AggregateId) -> String,
    ))
    .await;
}

async fn assert_enumerates_aggregates<S>(strategy: S)
where
    S: LocalPartitionStrategy + LocalStorePath,
{
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::builder()
        .local(S::local_store_path(&temp_dir))
        .strategy(strategy)
        .open()
        .await
        .unwrap();

    for id in [
        AggregateId::new("counter", "c1"),
        AggregateId::new("counter", "c2"),
        AggregateId::new("character", "ch1"),
    ] {
        store
            .publish(
                &id,
                PublishOptions::default(),
                vec![make_counter_event("counter:incremented", 1)],
            )
            .await
            .unwrap();
    }

    let ids = store.enumerate_aggregates().await.unwrap();
    let rendered: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect();
    assert_eq!(rendered, vec!["character:ch1", "counter:c1", "counter:c2"]);
}

async fn assert_enumerates_aggregates_by_type<S>(strategy: S)
where
    S: LocalPartitionStrategy + LocalStorePath,
{
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::builder()
        .local(S::local_store_path(&temp_dir))
        .strategy(strategy)
        .open()
        .await
        .unwrap();

    for id in [
        AggregateId::new("counter", "c1"),
        AggregateId::new("counter", "c2"),
        AggregateId::new("character", "ch1"),
    ] {
        store
            .publish(
                &id,
                PublishOptions::default(),
                vec![make_counter_event("counter:incremented", 1)],
            )
            .await
            .unwrap();
    }

    let counter_type = AggregateType::new("counter");
    let ids = store
        .enumerate_aggregates_by_type(&counter_type)
        .await
        .unwrap();
    let rendered: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect();
    assert_eq!(rendered, vec!["counter:c1", "counter:c2"]);
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
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    let changeset = stores
        .event_store
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

    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &changeset,
        "counters",
    )
    .await
    .unwrap();

    let doc = stores
        .document_store
        .get("counters", "c1")
        .await
        .unwrap()
        .unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 12);
    assert_eq!(state.event_count, 3);
    assert_eq!(doc.revision, changeset.revision);
}

#[tokio::test]
async fn apply_projection_updates_existing_document() {
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    let cs1 = stores
        .event_store
        .publish(
            &id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &cs1,
        "counters",
    )
    .await
    .unwrap();

    let cs2 = stores
        .event_store
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
    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &cs2,
        "counters",
    )
    .await
    .unwrap();

    let doc = stores
        .document_store
        .get("counters", "c1")
        .await
        .unwrap()
        .unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 15);
    assert_eq!(state.event_count, 2);
    assert_eq!(doc.revision, cs2.revision);
}

#[tokio::test]
async fn rebuild_projection_populates_all_aggregates() {
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();

    for (key, amount) in [("c1", 10), ("c2", 20), ("c3", 30)] {
        let id = AggregateId::new("counter", key);
        stores
            .event_store
            .publish(
                &id,
                PublishOptions::default(),
                vec![make_counter_event("counter:incremented", amount)],
            )
            .await
            .unwrap();
    }

    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        "counters",
        &counter_type,
    )
    .await
    .unwrap();

    let docs = stores.document_store.list("counters").await.unwrap();
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
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();

    let id1 = AggregateId::new("counter", "c1");
    let cs1 = stores
        .event_store
        .publish(
            &id1,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &cs1,
        "counters",
    )
    .await
    .unwrap();

    let id2 = AggregateId::new("counter", "c2");
    let cs2 = stores
        .event_store
        .publish(
            &id2,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 20)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &cs2,
        "counters",
    )
    .await
    .unwrap();

    stores
        .event_store
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

    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        "counters",
        &counter_type,
    )
    .await
    .unwrap();

    let doc1 = stores
        .document_store
        .get("counters", "c1")
        .await
        .unwrap()
        .unwrap();
    let state1: CounterState = serde_json::from_value(doc1.data).unwrap();
    assert_eq!(state1.value, 10);
    assert_eq!(
        doc1.revision, cs1.revision,
        "c1 revision should not have changed"
    );

    let doc2 = stores
        .document_store
        .get("counters", "c2")
        .await
        .unwrap()
        .unwrap();
    let state2: CounterState = serde_json::from_value(doc2.data).unwrap();
    assert_eq!(state2.value, 25, "c2 should include the new event");
    assert_ne!(
        doc2.revision, cs2.revision,
        "c2 revision should have advanced"
    );
}

#[tokio::test]
async fn rebuild_projection_updates_stale_documents() {
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();
    let id = AggregateId::new("counter", "c1");

    let cs1 = stores
        .event_store
        .publish(
            &id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();
    wee_events_sqlite::apply_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        &cs1,
        "counters",
    )
    .await
    .unwrap();

    stores
        .event_store
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

    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        "counters",
        &counter_type,
    )
    .await
    .unwrap();

    let doc = stores
        .document_store
        .get("counters", "c1")
        .await
        .unwrap()
        .unwrap();
    let state: CounterState = serde_json::from_value(doc.data).unwrap();
    assert_eq!(state.value, 15);
}

#[tokio::test]
async fn projection_ignores_other_aggregate_types() {
    let stores = SharedStores::new().await;
    let renderer = counter_renderer();

    let counter_id = AggregateId::new("counter", "c1");
    stores
        .event_store
        .publish(
            &counter_id,
            PublishOptions::default(),
            vec![make_counter_event("counter:incremented", 10)],
        )
        .await
        .unwrap();

    let other_id = AggregateId::new("other-type", "o1");
    stores
        .event_store
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

    let counter_type = AggregateType::new("counter");
    wee_events_sqlite::rebuild_projection(
        &renderer,
        &stores.event_store,
        &stores.document_store,
        "counters",
        &counter_type,
    )
    .await
    .unwrap();

    let docs = stores.document_store.list("counters").await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].key, "c1");
}
