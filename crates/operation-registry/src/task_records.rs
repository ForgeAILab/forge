//! Execution-bound Task observations and private plan candidates.
use crate::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlanAction {
    Write,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorklogAction {
    Append,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceAction {
    Capture,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorklogKind {
    Progress,
    Decision,
    Validation,
    Blocker,
}
impl WorklogKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::Decision => "decision",
            Self::Validation => "validation",
            Self::Blocker => "blocker",
        }
    }
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub action: PlanAction,
    #[schemars(length(min = 1))]
    pub content: String,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Worklog {
    pub action: WorklogAction,
    pub kind: WorklogKind,
    #[schemars(length(min = 1, max = 4000))]
    pub summary: String,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub action: EvidenceAction,
    // Custom non-empty kinds retain their label and are stored as `other`.
    #[schemars(length(min = 1))]
    pub kind: String,
    #[schemars(length(min = 1))]
    pub caption: String,
    pub path: Option<String>,
    pub content: Option<String>,
    pub filename: Option<String>,
}
#[async_trait::async_trait]
pub trait TaskRecordContext<E>: Send + Sync {
    async fn task_plan(&self, input: Plan) -> Result<Value, E>;
    async fn task_worklog(&self, input: Worklog) -> Result<Value, E>;
    async fn task_evidence(&self, input: Evidence) -> Result<Value, E>;
}
pub const IDS: &[&str] = &["task.plan", "task.worklog", "task.evidence"];
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::TaskRecord,
    permissions: &[("task", "task_read")],
    binding: "exact running Task execution and session",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_propose",
    projection: FieldProjection::ProposalPayload,
}];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed("task.plan", AuthorityRule { principal: authority::PrincipalRule::TaskPlan, ..AUTHORITY },
            EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Stage this execution's Markdown checklist; publish only on successful settlement.",
            "Content <=1048576 UTF-8 bytes and at least one Markdown checklist item; planner or implementation role only.",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::MaxUtf8Bytes { field: "content", bound: 1048576 }],
            |c, i| Box::pin(c.task_plan(i))),
        OperationSpec::typed("task.worklog", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Append one concise observation with this execution's identity and role.",
            "Summary <=4000 characters. A worklog never moves a Task or satisfies an acceptance check.",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::MaxSerializedBytes(65536)],
            |c, i| Box::pin(c.task_worklog(i))),
        OperationSpec::typed("task.evidence", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::Always, SURFACES,
            "Capture bytes this execution observed; return the stored asset and checksum.",
            "Exactly one of path (inside this execution's workspace) or content (verbatim output). Custom kinds are stored as other with their label retained.",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::MaxSerializedBytes(65536), StructuralConstraint::ExactlyOne(&["path", "content"])],
            |c, i| Box::pin(c.task_evidence(i))),
    ]
}
