use wee_events::{
    memory::MemoryStore, AggregateId, AggregateType, EventData, EventStore, EventType,
    PublishOptions, RawEvent,
};

fn make_event() -> RawEvent {
    RawEvent {
        event_type: EventType::new("test:created"),
        data: EventData::json(&serde_json::json!({})).unwrap(),
    }
}

#[test]
fn enumerate_empty_store() {
    let store = MemoryStore::new();
    let ids = store.enumerate_aggregates();
    assert!(ids.is_empty());
}

#[tokio::test]
async fn enumerate_returns_all_aggregates() {
    let store = MemoryStore::new();

    for (agg_type, key) in [("campaign", "c1"), ("campaign", "c2"), ("character", "ch1")] {
        let id = AggregateId::new(agg_type, key);
        store
            .publish(&id, PublishOptions::default(), vec![make_event()])
            .await
            .unwrap();
    }

    let mut result = store.enumerate_aggregates();
    result.sort_by_key(|a| a.to_string());
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], AggregateId::new("campaign", "c1"));
    assert_eq!(result[1], AggregateId::new("campaign", "c2"));
    assert_eq!(result[2], AggregateId::new("character", "ch1"));
}

#[tokio::test]
async fn enumerate_by_type_filters() {
    let store = MemoryStore::new();

    for (agg_type, key) in [("campaign", "c1"), ("campaign", "c2"), ("character", "ch1")] {
        let id = AggregateId::new(agg_type, key);
        store
            .publish(&id, PublishOptions::default(), vec![make_event()])
            .await
            .unwrap();
    }

    let campaign_type = AggregateType::new("campaign");
    let result = store.enumerate_aggregates_by_type(&campaign_type);
    assert_eq!(result.len(), 2);

    let character_type = AggregateType::new("character");
    let result = store.enumerate_aggregates_by_type(&character_type);
    assert_eq!(result.len(), 1);

    let unknown_type = AggregateType::new("unknown");
    let result = store.enumerate_aggregates_by_type(&unknown_type);
    assert!(result.is_empty());
}
