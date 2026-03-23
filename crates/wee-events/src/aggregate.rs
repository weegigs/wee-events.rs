use crate::event::RecordedEvent;
use crate::id::{AggregateId, Revision};

/// An event container — the full stream of recorded events for a given
/// aggregate. Contains no domain logic or projected state.
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub id: AggregateId,
    events: Vec<RecordedEvent>,
    revision: Revision,
}

impl Aggregate {
    /// Creates an empty aggregate with no events.
    pub fn empty(id: AggregateId) -> Self {
        Self {
            id,
            events: Vec::new(),
            revision: Revision::zero(),
        }
    }

    /// Constructs an aggregate from a recorded event slice. Revision is
    /// derived from the last event; if the slice is empty, returns an empty
    /// aggregate.
    pub fn from_events(id: AggregateId, events: Vec<RecordedEvent>) -> Self {
        let revision = events
            .last()
            .map(|e| e.revision.clone())
            .unwrap_or_else(Revision::zero);
        Self {
            id,
            events,
            revision,
        }
    }

    /// The recorded events in this aggregate's stream.
    pub fn events(&self) -> &[RecordedEvent] {
        &self.events
    }

    /// The current revision of this aggregate.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Consumes the aggregate and returns the events.
    pub fn into_events(self) -> Vec<RecordedEvent> {
        self.events
    }

    /// Returns true if this aggregate has no recorded events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the number of events in the stream.
    pub fn len(&self) -> usize {
        self.events.len()
    }
}
