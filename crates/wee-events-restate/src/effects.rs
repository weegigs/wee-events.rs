use wee_events::CommandName;

use crate::names;
use crate::types::ExecuteNotification;

pub enum SideEffectFilter {
    All,
    Name(CommandName),
    Names(Vec<CommandName>),
    Predicate(Box<dyn Fn(&ExecuteNotification) -> bool + Send + Sync>),
}

impl SideEffectFilter {
    #[must_use]
    pub fn matches(&self, notification: &ExecuteNotification) -> bool {
        match self {
            SideEffectFilter::All => true,
            SideEffectFilter::Name(name) => notification.command.name == *name,
            SideEffectFilter::Names(names) => names.contains(&notification.command.name),
            SideEffectFilter::Predicate(f) => f(notification),
        }
    }
}

pub struct EffectTrigger {
    pub workflow_name: String,
    pub filter: SideEffectFilter,
}

impl EffectTrigger {
    pub fn new(workflow_name: impl Into<String>) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            filter: SideEffectFilter::All,
        }
    }

    pub fn with_filter(workflow_name: impl Into<String>, filter: SideEffectFilter) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            filter,
        }
    }
}

pub struct EffectRouter {
    pub(crate) effects: Vec<EffectTrigger>,
    service_name: String,
}

impl EffectRouter {
    pub fn new(service_name: impl Into<String>, effects: Vec<EffectTrigger>) -> Self {
        Self {
            effects,
            service_name: service_name.into(),
        }
    }

    #[must_use]
    pub fn matching_effects(&self, notification: &ExecuteNotification) -> Vec<&str> {
        self.effects
            .iter()
            .filter(|e| e.filter.matches(notification))
            .map(|e| e.workflow_name.as_str())
            .collect()
    }

    #[must_use]
    pub fn runner_name(&self) -> String {
        names::runner_name(&self.service_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommandRequest, EntityResponse, Metadata};
    use wee_events::{AggregateId, Revision};

    fn test_notification(command_name: &str) -> ExecuteNotification {
        ExecuteNotification {
            command: CommandRequest {
                name: CommandName::from(command_name),
                target: AggregateId::new("test", "1"),
                command: serde_json::json!({}),
            },
            response: EntityResponse {
                aggregate: AggregateId::new("test", "1"),
                revision: Revision::zero(),
                state: serde_json::json!({}),
            },
            metadata: Metadata {
                correlation_id: "test-corr".to_string(),
                causation_id: None,
                idempotency_key: None,
            },
        }
    }

    #[test]
    fn filter_all_matches_everything() {
        assert!(SideEffectFilter::All.matches(&test_notification("anything")));
    }

    #[test]
    fn filter_name_matches_exact() {
        let filter = SideEffectFilter::Name(CommandName::from("create"));
        assert!(filter.matches(&test_notification("create")));
        assert!(!filter.matches(&test_notification("delete")));
    }

    #[test]
    fn filter_names_matches_any() {
        let filter = SideEffectFilter::Names(vec![
            CommandName::from("create"),
            CommandName::from("update"),
        ]);
        assert!(filter.matches(&test_notification("create")));
        assert!(filter.matches(&test_notification("update")));
        assert!(!filter.matches(&test_notification("delete")));
    }

    #[test]
    fn filter_predicate_uses_closure() {
        let filter = SideEffectFilter::Predicate(Box::new(|n| {
            n.command.name.as_str().starts_with("admin:")
        }));
        assert!(filter.matches(&test_notification("admin:reset")));
        assert!(!filter.matches(&test_notification("user:login")));
    }

    #[test]
    fn router_matching_effects_filters_correctly() {
        let router = EffectRouter::new(
            "test",
            vec![
                EffectTrigger::new("audit-log"),
                EffectTrigger::with_filter(
                    "notifications",
                    SideEffectFilter::Name(CommandName::from("create")),
                ),
            ],
        );

        let create = test_notification("create");
        assert_eq!(
            router.matching_effects(&create),
            vec!["audit-log", "notifications"]
        );

        let delete = test_notification("delete");
        assert_eq!(router.matching_effects(&delete), vec!["audit-log"]);
    }
}
