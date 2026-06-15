mod aggregate;
mod codec;
mod command;
mod create;
mod entity;
mod error;
mod event;
mod handler_env;
mod id;
mod memory_store;
mod publisher;
mod renderer;
mod service;
mod service_builder;
mod spec;
mod store;

#[cfg(any(test, feature = "testing"))]
mod bench_suite;
#[cfg(any(test, feature = "testing"))]
mod test_suite;

pub use aggregate::Aggregate;
pub use codec::{
    CodecError, DecodeError, EncodeError, EncodesEvents, Encoding, EventDecoder, EventEncoder,
};
pub mod encoding {
    //! Per-encoding modules. Each exposes `Encoder`, `Decoder`, and an
    //! `ENCODING` string constant. CBOR is feature-gated.
    #[cfg(feature = "cbor")]
    pub use crate::codec::cbor;
    pub use crate::codec::json;
}
pub use command::Command;
pub use create::{
    InProcessServiceDefinition, ServiceCreateBuilder, ServiceCreateEnvBuilder,
    ServiceCreateStoreBuilder, create,
};
pub use entity::Entity;
pub use error::{Error, RetryDiagnostics, RetryExhausted};
pub use event::{ChangeSet, DomainEvent, EventData, EventMetadata, RecordedEvent};
pub use handler_env::HandlerEnv;
pub use id::{
    AggregateId, AggregateIdParseError, AggregateType, CommandName, CorrelationId, EventId,
    EventType, Revision, RevisionParseError,
};
pub use publisher::{HasPublisher, Publisher};
pub use renderer::{EventPattern, EventPatternError, ReduceFn, RenderError, Renderer};
#[doc(hidden)]
pub use service::__private;
pub use service::{Handles, HasCommand, Rejection, ServiceDefinition, ServiceError, TypedService};
pub use service_builder::{BuiltService, ServiceBuilder};
#[doc(hidden)]
pub use service_builder::{EmptyHandlers, Here, There};
#[doc(hidden)]
pub use service_builder::{FactoryBridge, HandleCommand, HandlerBridge, HandlerList, LoaderBridge};
#[doc(hidden)]
pub use service_builder::{HandlerOutcome, IntoHandlerOutcome};
pub use spec::{HandlerRuntimeSpec, HandlerSpec, LoaderRuntimeSpec, LoaderSpec};
pub use store::{EventStore, PublishOptions, RawEvent};
pub use wee_events_macros::{Command, DomainEvent, capability, handler, loader, service};

pub mod memory {
    pub use crate::memory_store::MemoryStore;
}

#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::bench_suite::*;
    pub use crate::shared_store_test_suite;
    pub use crate::store_bench_suite;
    pub use crate::store_test_suite;
    pub use crate::test_suite::*;
}

pub type Result<T> = std::result::Result<T, Error>;

/// Helper to serialize a domain event into a `RawEvent` with JSON encoding.
pub fn to_raw_event<E: DomainEvent + serde::Serialize>(
    event: &E,
) -> std::result::Result<RawEvent, EncodeError> {
    Ok(RawEvent {
        event_type: event.event_type(),
        data: Encoding::Json.encode(event)?,
    })
}
