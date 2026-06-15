mod client;
mod correlation;
mod effects;
mod error;
#[doc(hidden)]
pub mod generated;
#[doc(hidden)]
pub mod names;
mod types;

use std::marker::PhantomData;

pub use client::RestateClient;
pub use correlation::correlation_id;
pub use effects::{EffectRouter, EffectTrigger, SideEffectFilter};
pub use error::Error;
pub use names::{executor_name, loader_name, runner_name};
pub use types::{CommandRequest, EntityResponse, ExecuteNotification, ExecuteRequest, Metadata};
pub use wee_events::HandlerEnv;

pub struct Ready;

pub struct Needs<Requirement, Tail> {
    _marker: PhantomData<fn() -> (Requirement, Tail)>,
}

impl<Requirement, Tail> Needs<Requirement, Tail> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Requirement, Tail> Default for Needs<Requirement, Tail> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait WithEffect<Effect, Requirement> {
    type Output;

    fn with_effect(self, effect: Effect) -> Self::Output;
}

pub trait RestateServiceDefinition {
    type Registration<Store, Services>;

    fn register<Store, Services>(
        store: Store,
        services: Services,
    ) -> Self::Registration<Store, Services>;
}

pub struct RestateServiceBuilder<Service> {
    service: Service,
}

pub struct RestateServiceStoreBuilder<Service, Store> {
    service: Service,
    store: Store,
}

pub fn create<Service>(service: Service) -> RestateServiceBuilder<Service> {
    RestateServiceBuilder { service }
}

impl<Service> RestateServiceBuilder<Service>
where
    Service: RestateServiceDefinition,
{
    pub fn with_store<Store>(self, store: Store) -> RestateServiceStoreBuilder<Service, Store> {
        RestateServiceStoreBuilder {
            service: self.service,
            store,
        }
    }

    pub fn with_env<Services>(self, services: Services) -> Service::Registration<Services, Services>
    where
        Services: Clone,
    {
        let _ = self.service;
        Service::register(services.clone(), services)
    }
}

impl<Service, Store> RestateServiceStoreBuilder<Service, Store>
where
    Service: RestateServiceDefinition,
{
    pub fn with_env<Services>(self, services: Services) -> Service::Registration<Store, Services> {
        let _ = self.service;
        Service::register(self.store, services)
    }
}

/// Hidden re-exports and helpers consumed by macro-generated code.
#[doc(hidden)]
pub mod __private {
    use crate::types::EntityResponse;

    pub use restate_sdk::context::Context;
    pub use restate_sdk::{context, errors, object, serde};
    pub use serde_json;

    pub type AttachEffect =
        Box<dyn FnOnce(restate_sdk::endpoint::Builder) -> restate_sdk::endpoint::Builder + Send>;

    #[must_use]
    pub fn attach_effects_to(
        mut builder: restate_sdk::endpoint::Builder,
        effects: Vec<AttachEffect>,
    ) -> restate_sdk::endpoint::Builder {
        for effect in effects {
            builder = effect(builder);
        }
        builder
    }

    /// Implemented by error types that the macro-generated Restate path may
    /// produce. Centralising the conversion here lets a single helper
    /// (`to_handler_error`) accept any of the supported shapes — structural
    /// `wee_events::Error` (from older handlers/loaders), structural
    /// rejections via `ServiceError<E>::Rejection`, and store-specific errors
    /// behind `ServiceError<E>::Store`.
    pub trait IntoHandlerError {
        fn into_handler_error(self) -> restate_sdk::errors::HandlerError;
    }

    impl IntoHandlerError for wee_events::Error {
        fn into_handler_error(self) -> restate_sdk::errors::HandlerError {
            // `wee_events::Error` carries only structural store-contract
            // failures after the error-type restructure — surface them as
            // terminal so Restate doesn't retry.
            restate_sdk::errors::TerminalError::new(self.to_string()).into()
        }
    }

    impl<E> IntoHandlerError for wee_events::ServiceError<E>
    where
        E: ::std::error::Error + ::std::marker::Send + ::std::marker::Sync + 'static,
    {
        fn into_handler_error(self) -> restate_sdk::errors::HandlerError {
            match self {
                wee_events::ServiceError::Rejection(r) => {
                    let payload = ::serde_json::json!({
                        "code": r.code,
                        "message": r.message,
                        "context": r.context,
                    });
                    restate_sdk::errors::TerminalError::new(payload.to_string()).into()
                }
                wee_events::ServiceError::Codec(err) => {
                    restate_sdk::errors::TerminalError::new(err.to_string()).into()
                }
                wee_events::ServiceError::Store(err) => {
                    restate_sdk::errors::TerminalError::new(err.to_string()).into()
                }
            }
        }
    }

    /// Lift a service-layer error into a Restate handler error. See
    /// [`IntoHandlerError`].
    pub fn to_handler_error<E: IntoHandlerError>(e: E) -> restate_sdk::errors::HandlerError {
        e.into_handler_error()
    }

    pub fn to_entity_response<S: ::serde::Serialize>(
        entity: wee_events::Entity<S>,
    ) -> Result<EntityResponse, restate_sdk::errors::HandlerError> {
        Ok(EntityResponse {
            aggregate: entity.aggregate_id,
            revision: entity.revision,
            state: serde_json::to_value(&entity.state)
                .map_err(|e| restate_sdk::errors::TerminalError::new(e.to_string()))?,
        })
    }
}
