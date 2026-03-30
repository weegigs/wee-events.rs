use std::collections::HashMap;
use std::sync::Mutex;

use ulid::Generator;

use crate::aggregate::Aggregate;
use crate::event::{ChangeSet, EventMetadata, RecordedEvent};
use crate::id::{AggregateId, AggregateType, EventId, Revision};
use crate::store::{EventStore, PublishOptions, RawEvent};
use crate::Error;

/// In-memory event store for testing. Thread-safe via `Mutex`.
///
/// Uses a monotonic ULID generator — guarantees strictly increasing
/// revisions even within the same millisecond.
pub struct MemoryStore {
    streams: Mutex<HashMap<AggregateId, Vec<RecordedEvent>>>,
    generator: Mutex<Generator>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            generator: Mutex::new(Generator::new()),
        }
    }

    fn generate_ulid(&self) -> Result<String, Error> {
        self.generator
            .lock()
            .expect("ULID generator mutex poisoned")
            .generate()
            .map(|u| u.to_string())
            .map_err(|e| Error::Store(Box::new(e)))
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    /// Returns all distinct aggregate IDs in the store.
    pub fn enumerate_aggregates(&self) -> Vec<AggregateId> {
        let streams = self.streams.lock().expect("streams mutex poisoned");
        streams.keys().cloned().collect()
    }

    /// Returns all distinct aggregate IDs of a given type.
    pub fn enumerate_aggregates_by_type(&self, aggregate_type: &AggregateType) -> Vec<AggregateId> {
        let streams = self.streams.lock().expect("streams mutex poisoned");
        streams
            .keys()
            .filter(|id| *id.aggregate_type() == *aggregate_type)
            .cloned()
            .collect()
    }
}

impl EventStore for MemoryStore {
    async fn load(&self, id: &AggregateId) -> Result<Aggregate, Error> {
        let streams = self.streams.lock().expect("streams mutex poisoned");

        match streams.get(id) {
            Some(events) if !events.is_empty() => {
                Ok(Aggregate::from_events(id.clone(), events.clone()))
            }
            _ => Ok(Aggregate::empty(id.clone())),
        }
    }

    async fn publish(
        &self,
        aggregate_id: &AggregateId,
        options: PublishOptions,
        events: Vec<RawEvent>,
    ) -> Result<ChangeSet, Error> {
        let mut streams = self.streams.lock().expect("streams mutex poisoned");

        if events.is_empty() {
            let revision = streams
                .get(aggregate_id)
                .and_then(|v| v.last())
                .map(|e| e.revision.clone())
                .unwrap_or_else(Revision::zero);
            return Ok(ChangeSet {
                aggregate_id: aggregate_id.clone(),
                revision,
                events: Vec::new(),
            });
        }

        let existing = streams.entry(aggregate_id.clone()).or_default();

        // Optimistic concurrency check
        if let Some(expected) = &options.expected_revision {
            let actual = existing
                .last()
                .map(|e| e.revision.clone())
                .unwrap_or_else(Revision::zero);
            if *expected != actual {
                return Err(Error::RevisionConflict {
                    expected: expected.clone(),
                    actual,
                });
            }
        }

        let metadata = EventMetadata {
            causation_id: options.causation_id,
            correlation_id: options.correlation_id,
        };

        let mut recorded = Vec::with_capacity(events.len());
        for raw in events {
            let event = RecordedEvent {
                event_id: EventId::new(self.generate_ulid()?),
                event_type: raw.event_type,
                revision: Revision::new(self.generate_ulid()?),
                metadata: metadata.clone(),
                data: raw.data,
            };
            recorded.push(event);
        }

        let revision = recorded
            .last()
            .expect("recorded is non-empty: events.is_empty() was false")
            .revision
            .clone();
        existing.extend(recorded.clone());

        Ok(ChangeSet {
            aggregate_id: aggregate_id.clone(),
            revision,
            events: recorded,
        })
    }
}
