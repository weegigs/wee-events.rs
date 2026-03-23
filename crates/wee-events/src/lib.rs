mod aggregate;
mod command;
mod entity;
mod error;
mod event;
mod id;
mod memory_store;
mod renderer;
mod store;

#[cfg(any(test, feature = "testing"))]
pub mod test_suite;

pub use aggregate::Aggregate;
pub use command::Command;
pub use entity::Entity;
pub use error::Error;
pub use event::{ChangeSet, DomainEvent, EventData, EventMetadata, RecordedEvent};
pub use id::{
    AggregateId, AggregateIdParseError, AggregateType, CommandName, CorrelationId, EventId,
    EventType, Revision,
};
pub use memory_store::MemoryStore;
pub use renderer::{ReduceFn, Renderer};
pub use store::{EventStore, PublishOptions, RawEvent};
pub use wee_events_macros::{Command, DomainEvent};

pub type Result<T> = std::result::Result<T, Error>;

/// Helper to serialize a domain event into a `RawEvent` with JSON encoding.
pub fn to_raw_event<E: DomainEvent + serde::Serialize>(event: &E) -> Result<RawEvent> {
    Ok(RawEvent {
        event_type: event.event_type(),
        data: EventData::json(event)?,
    })
}
