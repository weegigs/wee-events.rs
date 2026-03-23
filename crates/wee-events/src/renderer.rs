use std::collections::HashMap;

use crate::aggregate::Aggregate;
use crate::entity::Entity;
use crate::event::EventData;
use crate::id::EventType;

/// A reducer function pointer. Receives mutable state, the event type, and the
/// event data (encoding + bytes). Responsible for deserializing and applying.
pub type ReduceFn<S> = fn(&mut S, &EventType, &EventData) -> Result<(), crate::Error>;

/// Stateless projection engine. Folds an aggregate's event stream through
/// registered reducers to produce an `Entity<S>`.
///
/// Supports cross-domain projections by registering reducers from multiple
/// event sources.
pub struct Renderer<S> {
    reducers: HashMap<EventType, ReduceFn<S>>,
}

impl<S: Default> Renderer<S> {
    pub fn new() -> Self {
        Self {
            reducers: HashMap::new(),
        }
    }

    /// Registers a reducer for a given event type. Builder pattern.
    /// Accepts `&str`, `String`, or `EventType` via `Into<EventType>`.
    pub fn with(mut self, event_type: impl Into<EventType>, reducer: ReduceFn<S>) -> Self {
        self.reducers.insert(event_type.into(), reducer);
        self
    }

    /// Registers a reducer for a given event type (mutating).
    pub fn register(&mut self, event_type: impl Into<EventType>, reducer: ReduceFn<S>) {
        self.reducers.insert(event_type.into(), reducer);
    }

    /// Folds the aggregate's event stream into projected state. Unmapped event
    /// types are silently skipped — this allows cross-domain projections to
    /// selectively process only the events they care about.
    pub fn render(&self, aggregate: &Aggregate) -> Result<Entity<S>, crate::Error> {
        let mut state = S::default();

        for event in aggregate.events() {
            if let Some(reducer) = self.reducers.get(&event.event_type) {
                reducer(&mut state, &event.event_type, &event.data)?;
            }
        }

        Ok(Entity {
            aggregate_id: aggregate.id.clone(),
            revision: aggregate.revision().clone(),
            state,
        })
    }
}

impl<S: Default> Default for Renderer<S> {
    fn default() -> Self {
        Self::new()
    }
}
