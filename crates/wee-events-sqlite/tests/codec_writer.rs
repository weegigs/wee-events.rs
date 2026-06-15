#![cfg(feature = "cbor")]

use serde::{Deserialize, Serialize};
use wee_events::{
    AggregateId, DomainEvent, Encoding, Entity, EventStore as _, Publisher, Revision,
};
use wee_events_sqlite::{GlobalStrategy, SqliteEventStore};

#[derive(Debug, Clone, Serialize, Deserialize, DomainEvent)]
#[domain_event(prefix = "counter")]
enum CounterEvent {
    Incremented { amount: i64 },
}

#[derive(Debug, Clone, Default)]
struct Counter;

fn entity() -> Entity<Counter> {
    Entity {
        aggregate_id: AggregateId::new("counter", "codec"),
        revision: Revision::zero(),
        state: Counter,
    }
}

#[tokio::test]
async fn sqlite_writer_encodes_typed_published_events_as_cbor() {
    let store = SqliteEventStore::open_in_memory(GlobalStrategy)
        .await
        .expect("store should open")
        .with_encoding(Encoding::Cbor);

    Publisher::new(&store)
        .publish(&entity(), vec![CounterEvent::Incremented { amount: 5 }])
        .await
        .expect("publish should succeed");

    let aggregate = store
        .load(&AggregateId::new("counter", "codec"))
        .await
        .expect("load should work");
    let recorded = &aggregate.events()[0];

    assert_eq!(recorded.data.encoding, Encoding::Cbor);

    let decoded: CounterEvent = recorded
        .data
        .encoding
        .decode(&recorded.data)
        .expect("consumer decoder should decode cbor");

    assert!(matches!(decoded, CounterEvent::Incremented { amount: 5 }));
}

#[tokio::test]
async fn sqlite_writer_can_remain_json_for_compatibility() {
    let store = SqliteEventStore::open_in_memory(GlobalStrategy)
        .await
        .expect("store should open");

    Publisher::new(&store)
        .publish(&entity(), vec![CounterEvent::Incremented { amount: 3 }])
        .await
        .expect("publish should succeed");

    let aggregate = store
        .load(&AggregateId::new("counter", "codec"))
        .await
        .expect("load should work");
    assert_eq!(aggregate.events()[0].data.encoding, Encoding::Json);
}
