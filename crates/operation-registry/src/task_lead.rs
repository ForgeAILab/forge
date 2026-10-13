//! Operations of a coordination root's lead. The principal fact
//! (`Principal::TaskAgent::lead_root_id`) admits them; the service binding
//! checks once, for every one of them, that the target is in that root's
//! subtree. No input field names a root, a parent or an actor.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Read {
    /// A subtask of this root for its detail; omit for the breakdown.
    pub task_id: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Subtask {
    Create {
        #[schemars(length(min = 1, max = 200))]
        title: String,
        #[schemars(length(max = 16000))]
        description: Option<String>,
        assignee_id: Option<String>,
    },
    Update {
        #[schemars(length(min = 1))]
        task_id: String,
        #[schemars(range(min = 1))]
        version: i64,
        #[schemars(length(min = 1, max = 200))]
        title: Option<String>,
        #[schemars(length(max = 16000))]
        description: Option<String>,
    },
    Cancel {
        #[schemars(length(min = 1))]
        task_id: String,
        #[schemars(range(min = 1))]
        version: i64,
        #[schemars(length(min = 1, max = 2000))]
        reason: String,
    },
    Reorder {
        #[schemars(length(min = 1))]
        ordered_task_ids: Vec<String>,
    },
    Assign {
        /// Omit to set the root's default worker.
        task_id: Option<String>,
        #[schemars(length(min = 1))]
        agent_id: String,
    },
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Recover {
    #[schemars(length(min = 1))]
    pub task_id: String,
    pub action: api_types::TaskAction,
    #[schemars(range(min = 1))]
    pub version: i64,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Handoff {
    #[schemars(length(min = 1))]
    pub task_id: String,
    #[schemars(length(min = 1, max = 4000))]
    pub note: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestInput {
    #[schemars(length(min = 1, max = 4000))]
    pub question: String,
}
#[async_trait::async_trait]
pub trait TaskLeadContext<E>: Send + Sync {
    async fn lead_read(&self, input: Read) -> Result<Value, E>;
    async fn lead_subtask(&self, input: Subtask) -> Result<Value, E>;
    async fn lead_recover(&self, input: Recover) -> Result<Value, E>;
    async fn lead_handoff(&self, input: Handoff) -> Result<Value, E>;
    async fn lead_request_input(&self, input: RequestInput) -> Result<Value, E>;
}
pub const IDS: &[&str] = &[
    "lead.read",
    "lead.subtask",
    "lead.recover",
    "lead.handoff",
    "lead.request_input",
];
/// Longest handoff note and question, in characters.
pub const MAX_NOTE_CHARS: usize = 4000;
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::TaskLead,
    permissions: &[("task", "task_read")],
    binding: "running lead execution of this coordination root",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_propose",
    projection: FieldProjection::ProposalPayload,
}];
const CONSTRAINTS: &[StructuralConstraint] = &[
    StructuralConstraint::ClosedObject,
    StructuralConstraint::MaxSerializedBytes(65536),
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        // A read, issued through the command envelope like every lead
        // operation: it is answered for the running execution that leads the
        // root, which only that envelope's session identifies. It writes
        // nothing. The host classifies the propose aggregate's operations as
        // direct commands, so it is declared as one.
        OperationSpec::typed("lead.read", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Read this root's subtasks: id, version, state, park, whether started, hand-off notes.",
            "Bounded. Ids and versions for the other lead operations come from here.",
            CONSTRAINTS, |c, i| Box::pin(c.lead_read(i))),
        OperationSpec::typed("lead.subtask", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Create, update, cancel, reorder or assign subtasks of this root.",
            "update, cancel and reorder apply only to subtasks that never started; a started one takes lead.recover, assign or lead.handoff. create appends; the number per run and per root is limited.",
            CONSTRAINTS, |c, i| Box::pin(c.lead_subtask(i))),
        OperationSpec::typed("lead.recover", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Apply one recovery action offered to the lead on a parked subtask.",
            "retry, restart or cancel, at the subtask's version; limited per subtask.",
            CONSTRAINTS, |c, i| Box::pin(c.lead_recover(i))),
        OperationSpec::typed("lead.handoff", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Leave a note the subtask's next implementation run reads first.",
            "",
            CONSTRAINTS, |c, i| Box::pin(c.lead_handoff(i))),
        OperationSpec::typed("lead.request_input", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Ask the owner one question; the root waits for the answer.",
            "From a planning run on the root only. End the run after asking: it starts again with the answer.",
            CONSTRAINTS, |c, i| Box::pin(c.lead_request_input(i))),
    ]
}
