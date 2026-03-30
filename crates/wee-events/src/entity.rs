use crate::id::{AggregateId, AggregateType, Revision};

/// Projected state wrapper — the result of rendering an aggregate's event
/// stream through a set of reducers.
#[derive(Debug, Clone)]
pub struct Entity<S> {
    pub aggregate_id: AggregateId,
    pub revision: Revision,
    pub state: S,
}

impl<S> Entity<S> {
    /// Returns true if the entity has been initialized (has at least one event).
    pub fn initialized(&self) -> bool {
        !self.revision.is_zero()
    }

    /// Returns the aggregate type from the aggregate id.
    pub fn aggregate_type(&self) -> &AggregateType {
        self.aggregate_id.aggregate_type()
    }

    /// Maps the state to a different type.
    pub fn map<T>(self, f: impl FnOnce(S) -> T) -> Entity<T> {
        Entity {
            aggregate_id: self.aggregate_id,
            revision: self.revision,
            state: f(self.state),
        }
    }
}
