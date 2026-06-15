//! Correlation-id construction for Restate command invocations.
//!
//! A correlation id uniquely identifies a single invocation of a command
//! against an aggregate, and is used as the key for side-effect workflows
//! so each notification becomes its own workflow instance.

use wee_events::{AggregateId, CommandName};

/// Build a correlation id for an invocation of `command` against `id`.
///
/// Format: `<aggregate_id>-<command_name>-<ulid>`. ULID supplies a
/// monotonic, opaque tail so repeat invocations don't collide.
#[must_use]
pub fn correlation_id(id: &AggregateId, command: &CommandName) -> String {
    format!("{}-{}-{}", id, command.as_str(), ulid::Ulid::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_aggregate_and_command() {
        let id: AggregateId = "counter:c1".parse().unwrap();
        let cmd = CommandName::from("counter:increment");
        let corr = correlation_id(&id, &cmd);
        assert!(corr.starts_with("counter:c1-counter:increment-"));
    }

    #[test]
    fn successive_calls_produce_distinct_ids() {
        let id: AggregateId = "counter:c1".parse().unwrap();
        let cmd = CommandName::from("counter:increment");
        let a = correlation_id(&id, &cmd);
        let b = correlation_id(&id, &cmd);
        assert_ne!(a, b);
    }
}
