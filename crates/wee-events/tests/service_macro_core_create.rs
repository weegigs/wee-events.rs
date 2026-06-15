use wee_events::{
    AggregateId, DomainEvent, Entity, EventStore, HasPublisher, Revision, ServiceError,
    TypedService,
};

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Counter {
    value: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Increment {
    amount: i64,
}

impl wee_events::Command for Increment {
    const NAME: &'static str = "counter:increment";
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, DomainEvent)]
#[domain_event(prefix = "counter")]
pub enum CounterEvent {
    Incremented { amount: i64 },
}

#[wee_events::loader(requires(wee_events::EventStore))]
pub async fn load<R: EventStore>(
    store: &R,
    id: &AggregateId,
) -> Result<Entity<Counter>, ServiceError<wee_events::Error>> {
    let aggregate = store.load(id).await.map_err(ServiceError::Store)?;
    let mut state = Counter::default();
    for event in aggregate.events() {
        if event.event_type.as_str() == "counter:incremented" {
            let event: CounterEvent = event.data.deserialize_json()?;
            match event {
                CounterEvent::Incremented { amount } => state.value += amount,
            }
        }
    }

    Ok(Entity {
        aggregate_id: id.clone(),
        revision: aggregate.revision().clone(),
        state,
    })
}

#[wee_events::handler(command = Increment, requires(wee_events::HasPublisher))]
pub async fn increment<R: HasPublisher>(
    env: &R,
    entity: &Entity<Counter>,
    command: Increment,
) -> Result<(), ServiceError<wee_events::Error>> {
    env.publisher()
        .publish(
            entity,
            vec![CounterEvent::Incremented {
                amount: command.amount,
            }],
        )
        .await?;
    Ok(())
}

wee_events::service! {
    CounterService("counter") for Counter {
        loader: load,
        handlers: [increment],
    }
}

#[tokio::test]
async fn create_builds_typed_in_process_service_and_reloads_after_void_handler() {
    let service = wee_events::create(CounterService)
        .with_store(wee_events::memory::MemoryStore::new())
        .with_env(())
        .build();

    let id: AggregateId = "counter:c1".parse().unwrap();
    let entity = service
        .execute(id.clone(), Increment { amount: 3 })
        .await
        .unwrap();

    assert_eq!(entity.aggregate_id, id);
    assert_ne!(entity.revision, Revision::zero());
    assert_eq!(entity.state.value, 3);
}
