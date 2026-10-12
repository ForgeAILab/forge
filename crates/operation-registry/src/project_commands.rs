//! Receipt-bearing Project commands. Exact receipts are retrieved before fresh
//! admission; their typed contracts govern only a new effect.
use crate::hand_proposals::charter::{ProjectCharterContent, RevisionProvenance};
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DraftRevision {
    DraftRevision,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CharterAdoption {
    pub action: DraftRevision,
    pub charter_id: Option<String>,
    pub base_revision_id: Option<String>,
    pub expected_digest: Option<String>,
    #[schemars(range(min = 0))]
    pub expected_charter_version: i64,
    pub project_mode: String,
    pub maturity: String,
    pub content: ProjectCharterContent,
    pub rendered_view: Option<String>,
    pub render_version: Option<String>,
    pub provenance: RevisionProvenance,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceAction {
    Attach,
    Capture,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Evidence {
    pub action: EvidenceAction,
    #[schemars(length(min = 1))]
    pub milestone_id: String,
    #[schemars(range(min = 1))]
    pub expected_milestone_version: i64,
    pub asset_id: Option<String>,
    pub task_id: Option<String>,
    pub source_run_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub acceptance_check_ids: Vec<String>,
    #[schemars(length(min = 1))]
    pub caption: String,
    pub kind: String,
    pub checksum: Option<String>,
    pub content: Option<String>,
    pub path: Option<String>,
    pub filename: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Evaluate {
    Evaluate,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Readiness {
    pub action: Evaluate,
    #[schemars(length(min = 1))]
    pub milestone_id: String,
    #[schemars(range(min = 1))]
    pub milestone_version: i64,
}
#[async_trait::async_trait]
pub trait ProjectCommandContext<E>: Send + Sync {
    async fn charter_adoption(&self, input: CharterAdoption) -> Result<Value, E>;
    async fn project_evidence(&self, input: Evidence) -> Result<Value, E>;
    async fn readiness(&self, input: Readiness) -> Result<Value, E>;
}
pub const IDS: &[&str] = &[
    "project.charter.adoption",
    "project.evidence",
    "project.readiness",
];
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::ProjectAgent,
    permissions: &[
        ("project", "propose_project"),
        ("agent_chat", "propose_project"),
    ],
    binding: "bound Project Agent",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_project_orchestration_propose",
    projection: FieldProjection::ProposalPayload,
}];
const CONSTRAINTS: &[StructuralConstraint] = &[
    StructuralConstraint::ClosedObject,
    StructuralConstraint::MaxSerializedBytes(65536),
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed("project.charter.adoption", AUTHORITY, EffectClass::DirectCommand,
            AvailabilityRule::Always, SURFACES,
            "Draft an adoption or amendment Charter; only the user can approve it.",
            "The bound Project may have no current Charter. Omit charter_id to start; expected_charter_version is 0. For a revision, copy the returned Charter id/version. The server renders the canonical view.",
            CONSTRAINTS, |c, i| Box::pin(c.charter_adoption(i))).with_receipt_replay(),
        OperationSpec::typed("project.evidence", AUTHORITY, EffectClass::DirectCommand,
            AvailabilityRule::ReadyOnly, SURFACES,
            "Attach milestone proof, or capture the artifact your verification produced.",
            "attach requires asset_id and checksum. capture requires exactly one of content or path in the Project workspace, without asset_id/checksum. source_validation_id is the recorded validation RESULT id, never the check id.",
            CONSTRAINTS, |c, i| Box::pin(c.project_evidence(i))).with_receipt_replay(),
        OperationSpec::typed("project.readiness", AUTHORITY, EffectClass::DirectCommand,
            AvailabilityRule::ReadyOnly, SURFACES,
            "Evaluate milestone readiness against its exact version.", "",
            CONSTRAINTS, |c, i| Box::pin(c.readiness(i))).with_receipt_replay(),
    ]
}
