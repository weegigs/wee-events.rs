use crate::aggregate::Aggregate;
use crate::event::{ChangeSet, EventData};
use crate::id::{AggregateId, CorrelationId, EventId, EventType, Revision};
use std::future::Future;
use std::sync::Arc;

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
/// Implementors provide the persistence mechanism (in-memory, `SQLite`, etc.).
/// The error type is fixed to `crate::Error`: structural failures (revision
/// conflicts, encoding mismatches, retry exhaustion) are already represented
/// there, and backend-specific failures should be wrapped through
/// `Error::Custom(...)` to flow through that unified type.
///
/// This trait is intended for static dispatch. Methods return `Send` futures so
/// generated durable adapters can hold store references across async boundaries.
///
/// # Reentrancy
///
/// Implementations may hold internal locks (mutexes, async mutexes, connection
/// pool slots) while executing `load` / `publish`. A handler invoked while one
/// of these methods is in-flight **must not call back into the same store**
/// instance, or it will deadlock. The framework dispatches handlers between
/// store calls, never inside them; this invariant is load-bearing.
///
/// If reentrancy is required (e.g. composing a saga that publishes to another
/// aggregate from within a handler), use a separate store handle or a
/// dedicated reentrant primitive — do not rely on the public surface of an
/// implementation being reentrant by default.
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

/// Blanket impl so `Arc<Store>` can be passed wherever `impl EventStore`
/// is expected. Useful for stores whose internals are **not** themselves
/// cheaply cloneable (e.g. concrete `SqliteEventStore`, which holds
/// async mutexes + connection pools).
///
/// **Footgun:** if your store implementation is `Clone` (with a cheap
/// internal `Arc`, like [`crate::memory::MemoryStore`]), prefer
/// `store.clone()` over `Arc::new(store)`. Wrapping a `Clone` store in
/// `Arc` produces `Arc<Arc<Inner>>` — two indirections per access and two
/// atomic increments per clone, with no observable benefit.
impl<T> EventStore for Arc<T>
where
    T: EventStore,
{
    fn load(
        &self,
        id: &AggregateId,
    ) -> impl Future<Output = Result<Aggregate, crate::Error>> + Send {
        (**self).load(id)
    }

    fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> impl Future<Output = Result<ChangeSet, crate::Error>> + Send {
        (**self).publish(aggregate_id, options, events)
    }
}
