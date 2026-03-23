use std::future::Future;

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
pub trait EventStore: Send + Sync {
    fn load(
        &self,
        id: &AggregateId,
    ) -> impl Future<Output = Result<Aggregate, crate::Error>> + Send;

    fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> impl Future<Output = Result<ChangeSet, crate::Error>> + Send;
}
