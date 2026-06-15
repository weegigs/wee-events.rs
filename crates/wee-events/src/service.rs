use crate::command::Command;
use crate::entity::Entity;
use crate::id::AggregateId;

/// A structured rejection from the domain layer. Indicates a command was
/// refused by business logic (as opposed to an infrastructure failure).
///
/// Carries a machine-readable code, human message, and arbitrary JSON context.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Rejection {
    pub code: String,
    pub message: String,
    #[serde(default = "default_context")]
    pub context: serde_json::Value,
}

fn default_context() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

impl Rejection {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            context: default_context(),
        }
    }

    pub fn with_context(
        code: impl Into<String>,
        message: impl Into<String>,
        context: serde_json::Value,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            context,
        }
    }
}

/// Error returned by service-layer operations.
///
/// `Rejection` is a domain-level refusal from a command handler.
/// `Store(E)` is a backend failure from the underlying store.
/// `Codec(CodecError)` is a serialization or deserialization failure at the
/// service boundary, covering both encode and decode directions.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError<E: std::error::Error + Send + Sync + 'static> {
    #[error(transparent)]
    Rejection(#[from] Rejection),
    #[error(transparent)]
    Store(E),
    #[error(transparent)]
    Codec(#[from] crate::CodecError),
}

impl<E> From<crate::EncodeError> for ServiceError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: crate::EncodeError) -> Self {
        ServiceError::Codec(err.into())
    }
}

impl<E> From<crate::DecodeError> for ServiceError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: crate::DecodeError) -> Self {
        ServiceError::Codec(err.into())
    }
}

impl<E> From<crate::RenderError<crate::DecodeError>> for ServiceError<E>
where
    E: From<crate::Error> + std::error::Error + Send + Sync + 'static,
{
    fn from(err: crate::RenderError<crate::DecodeError>) -> Self {
        match err {
            crate::RenderError::UnhandledEventType { context } => {
                ServiceError::Store(E::from(crate::Error::UnhandledEventType {
                    event_type: context.event_type.to_string(),
                }))
            }
            crate::RenderError::ApplyFailed { source, .. } => ServiceError::Codec(source.into()),
        }
    }
}

/// Lift a [`crate::Error`] into a [`ServiceError`] via the inner store error's
/// `From<crate::Error>` impl. Used when `ServiceError<E>` plays the role of
/// `EL` in `BuiltService` — `EL: From<crate::Error>` is the canonical gateway
/// from infrastructure failures (factory, loader, store) into the service
/// error world.
impl<E> From<crate::Error> for ServiceError<E>
where
    E: From<crate::Error> + std::error::Error + Send + Sync + 'static,
{
    fn from(err: crate::Error) -> Self {
        ServiceError::Store(E::from(err))
    }
}

/// Hidden implementation details used by generated code.
#[doc(hidden)]
pub mod __private {
    use super::{AggregateId, Entity};
    use core::future::Future;

    /// Marker trait that associates a service with its state type via an
    /// associated type.
    ///
    /// Using an associated type rather than a type parameter keeps the public
    /// `Handles<C>` trait single-parameter. The impl
    /// `impl ServiceState for PubService { type State = PrivateState; }` is
    /// always valid regardless of `PrivateState`'s visibility — associated type
    /// values in impl blocks may reference private types.
    pub trait ServiceState: Send + Sync {
        type State;
    }

    /// Carries the compile-time dispatch witness for command `C`.
    ///
    /// `S` is recovered through `Self::State` from the `ServiceState` supertrait,
    /// so callers only need one type parameter. Implemented per concrete command
    /// by the `service!` macro with the specific `Idx` type computed from the
    /// handler registration order.
    pub trait DispatchCommand<C>: ServiceState + Send + Sync {
        type Error;

        fn dispatch_command(
            &self,
            id: AggregateId,
            cmd: C,
        ) -> impl Future<Output = Result<Entity<Self::State>, Self::Error>> + Send;
    }
}

/// Marker supertrait declaring that a service can handle command type `C`.
///
/// The `__private::DispatchCommand<C>` supertrait carries the actual dispatch
/// implementation. Callers only see `Handles<C>` — a single type parameter.
///
/// Implement via the `service!` macro (which generates the concrete
/// `DispatchCommand<C>` impl) or manually by implementing both
/// `ServiceState`, `DispatchCommand<C>`, and `Handles<C>`.
pub trait Handles<C>: __private::DispatchCommand<C> {}

/// A typed service contract combining state loading with type-safe command dispatch.
///
/// `TypedService<S>` dispatches over concrete command types. The `Handles<C>`
/// bound on `execute` ensures only registered commands can be dispatched —
/// unregistered commands produce a compile error rather than a runtime
/// rejection.
///
/// The trait is transport-agnostic: no `Serialize`, `Deserialize`, or
/// transport-specific bounds appear here. Serialization is an adapter concern
/// handled by generated macro code.
///
/// # Error type asymmetry
///
/// `load` returns `Self::Error` while `execute<C>` returns
/// `<Self as DispatchCommand<C>>::Error`. The two are independent because
/// command execution can fail with domain rejections (a service-layer
/// concept) that do not apply to plain entity loads. Callers that need a
/// uniform error surface across both should pattern-match on each impl's
/// concrete error type.
///
/// # Implementation note
///
/// The `service!` macro emits typed in-process implementations via
/// `wee_events::create(Service).with_store(store).with_env(services).build()`
/// and Restate bindings through `wee-events-restate`.
pub trait TypedService<S>: __private::ServiceState<State = S> + Send + Sync {
    type Error;

    fn load(
        &self,
        id: AggregateId,
    ) -> impl core::future::Future<Output = Result<Entity<S>, Self::Error>> + Send;

    fn execute<C>(
        &self,
        id: AggregateId,
        cmd: C,
    ) -> impl core::future::Future<
        Output = Result<Entity<S>, <Self as __private::DispatchCommand<C>>::Error>,
    > + Send
    where
        C: crate::Command + Send + 'static,
        Self: Handles<C>,
    {
        __private::DispatchCommand::<C>::dispatch_command(self, id, cmd)
    }
}

/// Defines the identity and state type of a service.
///
/// This trait links a service marker type to its aggregate state and a human-readable
/// service name. It is the minimal contract for service definitions.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a service definition",
    note = "use the `service!` macro to generate a ServiceDefinition impl"
)]
pub trait ServiceDefinition: Send + Sync + 'static {
    type State;
    const SERVICE_NAME: &'static str;
}

/// Declares that a service can handle command type `C`.
///
/// Used to statically assert that a command has been declared in a service.
/// Typically implemented automatically by the `service!` macro when a command
/// is registered in the command list.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not declare command `{C}`",
    label = "add `{C}` to the `service!` command list",
    note = "commands must be listed in the service! declaration for routing"
)]
pub trait HasCommand<C: Command>: Send + Sync {}
