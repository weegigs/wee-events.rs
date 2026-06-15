use std::future::Future;
use wee_events::{AggregateId, Command, Entity, Handles, Revision, TypedService};

#[derive(Debug, Clone, serde::Serialize)]
struct Increment {
    amount: i64,
}

impl Command for Increment {
    const NAME: &'static str = "counter:increment";
}

#[derive(Debug, Clone, serde::Serialize)]
struct Unsupported;

impl Command for Unsupported {
    const NAME: &'static str = "counter:unsupported";
}

#[derive(Debug, Default, Clone)]
struct Counter {
    value: i64,
}

struct TestService;

// Implement ServiceState — associates the service with its state type
impl wee_events::__private::ServiceState for TestService {
    type State = Counter;
}

// Implement DispatchCommand<Increment> — required by Handles<Increment>
impl wee_events::__private::DispatchCommand<Increment> for TestService {
    type Error = wee_events::Error;

    fn dispatch_command(
        &self,
        id: &AggregateId,
        _cmd: Increment,
    ) -> impl Future<Output = wee_events::Result<Entity<Counter>>> + Send {
        let id = id.clone();
        async move {
            Ok(Entity {
                aggregate_id: id,
                revision: Revision::zero(),
                state: Counter { value: 0 },
            })
        }
    }
}

impl Handles<Increment> for TestService {}

impl TypedService<Counter> for TestService {
    type Error = wee_events::Error;

    fn load(
        &self,
        id: &AggregateId,
    ) -> impl Future<Output = wee_events::Result<Entity<Counter>>> + Send {
        let id = id.clone();
        async move {
            Ok(Entity {
                aggregate_id: id,
                revision: Revision::zero(),
                state: Counter { value: 0 },
            })
        }
    }
    // execute uses default impl from TypedService
}

fn main() {
    let service = TestService;
    let id: AggregateId = "counter:c1".parse().unwrap();
    // This should fail — TestService does not implement Handles<Unsupported, Counter>.
    // Note: the compiler diagnostic says "mismatched types" (Increment vs Unsupported)
    // rather than "missing Handles impl" — this is a known RPIT inference artifact
    // where the compiler infers C from the only Handles impl, then rejects the mismatch.
    let _ = service.execute(id.clone(), Unsupported);
}
