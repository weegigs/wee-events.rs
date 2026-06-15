use std::sync::Arc;

use crate::event::RecordedEvent;
use crate::id::{AggregateId, Revision};

/// An event container — the full stream of recorded events for a given
/// aggregate. Contains no domain logic or projected state.
///
/// Events are stored as `Arc<RecordedEvent>` so cloning an `Aggregate` (and
/// the `events.clone()` an `EventStore::load` performs on every call) is a
/// refcount bump per event rather than a deep copy. Each `RecordedEvent`
/// carries a `Vec<u8>` payload that can be megabytes; sharing matters.
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub id: AggregateId,
    events: Vec<Arc<RecordedEvent>>,
    revision: Revision,
}

impl Aggregate {
    /// Creates an empty aggregate with no events.
    #[must_use]
    pub fn empty(id: AggregateId) -> Self {
        Self {
            id,
            events: Vec::new(),
            revision: Revision::zero(),
        }
    }

    /// Constructs an aggregate from a vec of owned events. Each event is
    /// wrapped in `Arc` at this boundary; subsequent clones of the aggregate
    /// will be cheap. Revision is derived from the last event; if the slice
    /// is empty, returns an empty aggregate.
    pub fn from_events(id: AggregateId, events: Vec<RecordedEvent>) -> Self {
        Self::from_shared_events(id, events.into_iter().map(Arc::new).collect())
    }

    /// Constructs an aggregate from already-shared events. Use this path
    /// when the caller (e.g. an in-memory store) already holds `Arc`-wrapped
    /// events — no allocation occurs.
    #[must_use]
    pub fn from_shared_events(id: AggregateId, events: Vec<Arc<RecordedEvent>>) -> Self {
        let revision = match events.last() {
            Some(e) => e.revision.clone(),
            None => Revision::zero(),
        };
        Self {
            id,
            events,
            revision,
        }
    }

    /// The recorded events in this aggregate's stream, as a slice of shared
    /// references. Most consumers iterate `for event in agg.events()` and
    /// access fields via auto-deref through `Arc<RecordedEvent>`.
    #[must_use]
    pub fn events(&self) -> &[Arc<RecordedEvent>] {
        &self.events
    }

    /// The current revision of this aggregate.
    #[must_use]
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Consumes the aggregate and returns owned events. Deep-clones each
    /// event because callers historically expected `Vec<RecordedEvent>`;
    /// prefer [`Self::into_shared_events`] in new code.
    #[must_use]
    pub fn into_events(self) -> Vec<RecordedEvent> {
        self.events
            .into_iter()
            .map(|e| Arc::try_unwrap(e).unwrap_or_else(|arc| (*arc).clone()))
            .collect()
    }

    /// Consumes the aggregate and returns the shared-event vec. Cheap.
    #[must_use]
    pub fn into_shared_events(self) -> Vec<Arc<RecordedEvent>> {
        self.events
    }

    /// Consumes the aggregate, returning `(id, events, revision)`. The
    /// renderer uses this to move identifiers into `Entity` without cloning.
    #[must_use]
    pub fn into_parts(self) -> (AggregateId, Vec<Arc<RecordedEvent>>, Revision) {
        (self.id, self.events, self.revision)
    }

    /// Returns true if this aggregate has no recorded events.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of events in the stream.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.len()
    }
}
