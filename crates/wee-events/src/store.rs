use crate::aggregate::Aggregate;
use crate::event::{ChangeSet, EventData};
use crate::id::{AggregateId, CorrelationId, EventId, EventType, Revision};

/// Options for publishing events — optimistic concurrency, causation, correlation.
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    pub expected_revision: Option<Revision>,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<EventId>,
}

/// A pre-serialized event ready for storage. Domain code serializes its typed
/// events into this form before handing them to the store.
#[derive(Debug, Clone)]
pub struct RawEvent {
    pub event_type: EventType,
    pub data: EventData,
}

/// Domain-agnostic event store. A single store instance serves all aggregates —
/// load and publish take `AggregateId` as a parameter.
///
/// Implementors provide the persistence mechanism (in-memory, SQLite, etc.).
///
/// This trait is intended for static dispatch. Implementations and callers in
/// this workspace use concrete store types, so native `async fn` keeps the API
/// clear without reintroducing erased futures.
#[allow(async_fn_in_trait)]
pub trait EventStore: Send + Sync {
    async fn load(&self, id: &AggregateId) -> Result<Aggregate, crate::Error>;

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, crate::Error>;
}
