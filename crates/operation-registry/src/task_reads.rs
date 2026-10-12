//! Task and scope-bound coordination reads. Selectors never supply authority.
use crate::{scope_reads::NoArguments, *};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListArguments {
    // Existing handlers clamp to 1..=50 rows.
    pub limit: Option<u64>,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryArguments {
    // Existing handlers truncate query to 512 characters and clamp limit to 1..=20.
    pub query: Option<String>,
    pub limit: Option<u64>,
}
#[async_trait::async_trait]
pub trait TaskReadContext<E>: Send + Sync {
    async fn task_summary(&self, input: NoArguments) -> Result<Value, E>;
    async fn work_read(&self, input: ListArguments) -> Result<Value, E>;
    async fn events_read(&self, input: ListArguments) -> Result<Value, E>;
    async fn inbox_read(&self, input: ListArguments) -> Result<Value, E>;
    async fn commitments_read(&self, input: ListArguments) -> Result<Value, E>;
    async fn delivery_read(&self, input: ListArguments) -> Result<Value, E>;
    async fn memory_read(&self, input: MemoryArguments) -> Result<Value, E>;
    async fn decisions_read(&self, input: MemoryArguments) -> Result<Value, E>;
}
pub const IDS: &[&str] = &[
    "task.summary",
    "work.read",
    "events.read",
    "inbox.read",
    "commitments.read",
    "delivery.read",
    "memory.read",
    "decisions.read",
];
const SURFACE: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_read",
    projection: FieldProjection::ReadArguments,
}];
const SCOPED: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::BoundIdentity,
    permissions: &[
        ("project", "read_project"),
        ("agent_chat", "read_agent_chat"),
        ("task", "read_task"),
    ],
    binding: "admitted scope",
};
const ACCOUNT_SCOPED: AuthorityRule = AuthorityRule {
    permissions: &[
        ("account", "read_account"),
        ("project", "read_project"),
        ("agent_chat", "read_agent_chat"),
        ("task", "read_task"),
    ],
    ..SCOPED
};
const MEMORY: AuthorityRule = AuthorityRule {
    permissions: &[
        ("account", "read_memory"),
        ("project", "read_memory"),
        ("agent_chat", "read_memory"),
        ("task", "read_memory"),
    ],
    ..SCOPED
};
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed(
            "task.summary",
            AuthorityRule {
                permissions: &[("task", "read_task")],
                ..SCOPED
            },
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read this Task and its informational workflow offers.",
            "Task-run offers are informational: this tool grants no workflow mutation.",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.task_summary(i)),
        ),
        OperationSpec::typed(
            "work.read",
            AuthorityRule {
                principal: authority::PrincipalRule::ProjectOrTask,
                ..SCOPED
            },
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read work in the admitted scope.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.work_read(i)),
        ),
        OperationSpec::typed(
            "events.read",
            SCOPED,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read events in the admitted scope.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.events_read(i)),
        ),
        OperationSpec::typed(
            "inbox.read",
            ACCOUNT_SCOPED,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read this identity's scoped inbox.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.inbox_read(i)),
        ),
        OperationSpec::typed(
            "commitments.read",
            ACCOUNT_SCOPED,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read this identity's scoped commitments.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.commitments_read(i)),
        ),
        OperationSpec::typed(
            "delivery.read",
            ACCOUNT_SCOPED,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Read this identity's scoped delivery status.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.delivery_read(i)),
        ),
        OperationSpec::typed(
            "memory.read",
            MEMORY,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Search scoped memory.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.memory_read(i)),
        )
        .outside_inquiry(),
        OperationSpec::typed(
            "decisions.read",
            MEMORY,
            EffectClass::Query,
            AvailabilityRule::Always,
            SURFACE,
            "Search scoped decisions.",
            "",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.decisions_read(i)),
        )
        .outside_inquiry(),
    ]
}
