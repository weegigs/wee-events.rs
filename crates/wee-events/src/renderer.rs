use crate::aggregate::Aggregate;
use crate::codec::DecodeError;
use crate::entity::Entity;
use crate::event::{EventData, RecordedEvent};
use crate::id::{AggregateId, EventId, EventType, Revision};

/// A reducer function pointer. Receives mutable state, the event type, and the
/// event data (encoding + bytes). Responsible for deserializing and applying.
pub type ReduceFn<S, E = DecodeError> = fn(&mut S, &EventType, &EventData) -> Result<(), E>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventPattern {
    Exact(EventType),
    Glob(String),
}

impl EventPattern {
    pub fn exact(event_type: impl Into<EventType>) -> Self {
        Self::Exact(event_type.into())
    }

    pub fn glob(pattern: impl Into<String>) -> Result<Self, EventPatternError> {
        let pattern = pattern.into();
        if pattern.is_empty() {
            return Err(EventPatternError::EmptyGlob);
        }
        Ok(Self::Glob(pattern))
    }

    fn matches(&self, event_type: &EventType) -> bool {
        match self {
            Self::Exact(pattern) => pattern == event_type,
            Self::Glob(pattern) => glob_matches(pattern, event_type.as_str()),
        }
    }
}

impl From<EventType> for EventPattern {
    fn from(event_type: EventType) -> Self {
        Self::Exact(event_type)
    }
}

impl From<&str> for EventPattern {
    fn from(event_type: &str) -> Self {
        Self::Exact(EventType::new(event_type))
    }
}

impl From<String> for EventPattern {
    fn from(event_type: String) -> Self {
        Self::Exact(EventType::new(event_type))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EventPatternError {
    #[error("glob event pattern cannot be empty")]
    EmptyGlob,
}

#[derive(Debug, thiserror::Error)]
pub enum RenderError<E> {
    #[error("unhandled event type {context}")]
    UnhandledEventType { context: Box<RenderEventContext> },
    #[error("failed to apply event {context}: {source}")]
    ApplyFailed {
        context: Box<RenderEventContext>,
        #[source]
        source: E,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderEventContext {
    pub aggregate_id: AggregateId,
    pub event_id: EventId,
    pub event_type: EventType,
    pub revision: Revision,
    pub encoding: String,
}

impl std::fmt::Display for RenderEventContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at revision {} in aggregate {}",
            self.event_type, self.revision, self.aggregate_id
        )
    }
}

impl<E> From<RenderError<E>> for crate::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: RenderError<E>) -> Self {
        match err {
            RenderError::UnhandledEventType { context } => crate::Error::UnhandledEventType {
                event_type: context.event_type.to_string(),
            },
            RenderError::ApplyFailed { source, .. } => crate::Error::custom(source),
        }
    }
}

/// Action taken when a rule matches: reduce the event, or skip it.
enum Action<S, E> {
    Reduce(ReduceFn<S, E>),
    Ignore,
}

/// Stateless projection engine. Folds an aggregate's event stream through
/// registered rules to produce an `Entity<S>`.
///
/// # Precedence
///
/// Rules are walked in registration order; the **first matching** rule
/// fires. Register more-specific patterns before more-general ones, exactly
/// like `match` arms:
///
/// ```ignore
/// Renderer::new()
///     .with("counter:legacy", legacy_reducer)        // exact, registered first
///     .with(EventPattern::glob("counter:*"), reduce) // glob, fallback
///     .ignore(EventPattern::glob("audit:*"))         // skip noise
/// ```
///
/// Entity rendering is strict: events with no matching rule fail rendering.
pub struct Renderer<S, E = DecodeError> {
    rules: Vec<(EventPattern, Action<S, E>)>,
}

impl<S: Default, E> Renderer<S, E> {
    #[must_use]
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Registers a reducer for an event type or pattern. Earlier registrations
    /// take precedence; see [`Renderer`] for full semantics.
    #[must_use]
    pub fn with(mut self, pattern: impl Into<EventPattern>, reducer: ReduceFn<S, E>) -> Self {
        self.rules.push((pattern.into(), Action::Reduce(reducer)));
        self
    }

    /// Mutating sibling of [`with`](Self::with).
    pub fn register(&mut self, pattern: impl Into<EventPattern>, reducer: ReduceFn<S, E>) {
        self.rules.push((pattern.into(), Action::Reduce(reducer)));
    }

    /// Registers a skip rule for an event type or pattern. Earlier registrations
    /// take precedence; see [`Renderer`] for full semantics.
    #[must_use]
    pub fn ignore(mut self, pattern: impl Into<EventPattern>) -> Self {
        self.rules.push((pattern.into(), Action::Ignore));
        self
    }

    /// Mutating sibling of [`ignore`](Self::ignore).
    pub fn register_ignore(&mut self, pattern: impl Into<EventPattern>) {
        self.rules.push((pattern.into(), Action::Ignore));
    }

    //TODO: work out a way to make this work with some kinda call to Fold<T> .i.e .fold(||{}) so it's more rusty..
    /// Folds the aggregate's event stream into projected state.
    ///
    /// Consumes the aggregate so its `AggregateId` and `Revision` move into
    /// the resulting `Entity` without cloning. Callers that need the
    /// aggregate after rendering should clone it first.
    pub fn render(&self, aggregate: Aggregate) -> Result<Entity<S>, RenderError<E>> {
        let mut state = S::default();
        let (id, events, revision) = aggregate.into_parts();

        for event in &events {
            let action = self
                .rules
                .iter()
                .find(|(pattern, _)| pattern.matches(&event.event_type))
                .map(|(_, action)| action);
            match action {
                Some(Action::Reduce(reducer)) => {
                    reducer(&mut state, &event.event_type, &event.data).map_err(|source| {
                        RenderError::ApplyFailed {
                            context: Box::new(RenderEventContext::new(&id, event.as_ref())),
                            source,
                        }
                    })?;
                }
                Some(Action::Ignore) => {}
                None => {
                    return Err(RenderError::UnhandledEventType {
                        context: Box::new(RenderEventContext::new(&id, event.as_ref())),
                    });
                }
            }
        }

        Ok(Entity {
            aggregate_id: id,
            revision,
            state,
        })
    }
}

impl RenderEventContext {
    fn new(aggregate_id: &AggregateId, event: &RecordedEvent) -> Self {
        Self {
            aggregate_id: aggregate_id.clone(),
            event_id: event.event_id.clone(),
            event_type: event.event_type.clone(),
            revision: event.revision.clone(),
            encoding: event.data.encoding.as_str().to_string(),
        }
    }
}

//FIXME: hand rolled `str::starts_with`, just use that -- you'll struggle
// to beat the std lib.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let (mut pattern_idx, mut text_idx) = (0, 0);
    let mut star_idx = None;
    let mut star_text_idx = 0;

    while text_idx < text.len() {
        if pattern_idx < pattern.len() && pattern[pattern_idx] == text[text_idx] {
            pattern_idx += 1;
            text_idx += 1;
        } else if pattern_idx < pattern.len() && pattern[pattern_idx] == b'*' {
            star_idx = Some(pattern_idx);
            pattern_idx += 1;
            star_text_idx = text_idx;
        } else if let Some(star) = star_idx {
            pattern_idx = star + 1;
            star_text_idx += 1;
            text_idx = star_text_idx;
        } else {
            return false;
        }
    }

    while pattern_idx < pattern.len() && pattern[pattern_idx] == b'*' {
        pattern_idx += 1;
    }

    pattern_idx == pattern.len()
}

impl<S: Default, E> Default for Renderer<S, E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod precedence_tests {
    // reducers match `Renderer::with` signature; the wrap is part of the contract
    #![allow(clippy::unnecessary_wraps)]
    use super::{Aggregate, EventPattern, Renderer};
    use crate::codec::{DecodeError, Encoding};
    use crate::event::RecordedEvent;
    use crate::id::{AggregateId, EventId, EventType, Revision};

    #[derive(Default)]
    struct State {
        legacy_hits: u32,
        glob_hits: u32,
    }

    fn legacy_reducer(
        s: &mut State,
        _t: &EventType,
        _d: &crate::event::EventData,
    ) -> Result<(), DecodeError> {
        s.legacy_hits += 1;
        Ok(())
    }

    fn glob_reducer(
        s: &mut State,
        _t: &EventType,
        _d: &crate::event::EventData,
    ) -> Result<(), DecodeError> {
        s.glob_hits += 1;
        Ok(())
    }

    fn aggregate_with_event(event_type: &str) -> Aggregate {
        Aggregate::from_events(
            AggregateId::new("counter", "p"),
            vec![RecordedEvent {
                event_id: EventId::new("event-1"),
                event_type: EventType::new(event_type),
                revision: Revision::generate(),
                metadata: crate::event::EventMetadata::default(),
                data: Encoding::Json.encode(&serde_json::json!({})).unwrap(),
            }],
        )
    }

    #[test]
    fn earlier_specific_rule_beats_later_glob() {
        let renderer = Renderer::<State>::new()
            .with("counter:legacy", legacy_reducer)
            .with(EventPattern::glob("counter:*").unwrap(), glob_reducer);
        let entity = renderer
            .render(aggregate_with_event("counter:legacy"))
            .unwrap();
        assert_eq!(entity.state.legacy_hits, 1);
        assert_eq!(entity.state.glob_hits, 0);
    }

    #[test]
    fn earlier_ignore_beats_later_reducer() {
        let renderer = Renderer::<State>::new()
            .ignore(EventPattern::glob("counter:legacy-*").unwrap())
            .with(EventPattern::glob("counter:*").unwrap(), glob_reducer);
        let entity = renderer
            .render(aggregate_with_event("counter:legacy-reset"))
            .unwrap();
        assert_eq!(entity.state.glob_hits, 0, "earlier ignore should skip");
    }

    #[test]
    fn earlier_reducer_beats_later_ignore() {
        let renderer = Renderer::<State>::new()
            .with("counter:legacy", legacy_reducer)
            .ignore(EventPattern::glob("counter:legacy*").unwrap());
        let entity = renderer
            .render(aggregate_with_event("counter:legacy"))
            .unwrap();
        assert_eq!(entity.state.legacy_hits, 1, "earlier reducer should fire");
    }

    #[test]
    fn no_matching_rule_fails() {
        let renderer = Renderer::<State>::new().with("counter:other", legacy_reducer);
        assert!(
            renderer
                .render(aggregate_with_event("counter:unhandled"))
                .is_err()
        );
    }
}
