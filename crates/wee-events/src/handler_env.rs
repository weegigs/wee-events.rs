use crate::{EncodesEvents, EventStore, Publisher};

/// Core service execution environment passed to typed handlers.
///
/// Loaders receive the store directly. Handlers receive this environment so
/// they can publish through the same store while also depending on service
/// capabilities supplied by the caller.
pub struct HandlerEnv<Store, Services> {
    store: Store,
    services: Services,
}

impl<Store, Services> HandlerEnv<Store, Services> {
    pub fn new(store: Store, services: Services) -> Self {
        Self { store, services }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn services(&self) -> &Services {
        &self.services
    }
}

impl<Store, Services> crate::HasPublisher for HandlerEnv<Store, Services>
where
    Store: EventStore + EncodesEvents,
    Services: Send + Sync,
{
    type Store = Store;

    fn publisher(&self) -> Publisher<'_, Self::Store> {
        Publisher::new(self.store())
    }
}
