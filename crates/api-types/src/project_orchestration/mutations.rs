//! Revision-addressed Project mutation request and response contracts.

use super::*;

// ---------------------------------------------------------------------------
// Replay-safe mutation envelopes and typed actions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MutationEnvelope {
    pub expected_version: i64,
    #[serde(default)]
    pub expected_digest: Option<String>,
    pub idempotency_key: String,
    #[serde(default)]
    pub deduplication_key: Option<String>,
    pub authorization: AuthorizationProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SaveProjectCharterRevisionRequest {
    pub mutation: MutationEnvelope,
    pub charter_id: String,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub project_mode: ProjectMode,
    pub maturity: ProductMaturity,
    pub content: ProjectCharterContent,
    pub rendered_view: String,
    pub render_version: String,
    pub provenance: RevisionProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ApproveProjectCharterRequest {
    pub mutation: MutationEnvelope,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
    pub expected_charter_version: i64,
    /// Project version observed while the user reviewed this exact Charter
    /// revision. Genesis approvals have no Project and must omit this field;
    /// Project adoption/amendment approvals must provide a positive version.
    pub expected_project_version: Option<i64>,
    pub approved_project_name: String,
    #[serde(default)]
    pub approved_project_slug: Option<String>,
    pub project_mode: ProjectMode,
    pub selected_project_agent_identity_id: String,
    pub selected_project_agent_profile_revision_id: String,
    pub selected_project_agent_operating_skill_revision: String,
    pub selected_project_agent_policy_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateProjectFromCharterApprovalRequest {
    pub approval_id: String,
    pub idempotency_key: String,
    pub authorization: AuthorizationProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CreateProjectFromCharterApprovalResponse {
    pub project_id: String,
    pub project_agent_binding_id: String,
    pub project_chat_id: String,
    pub charter_id: String,
    pub charter_revision_id: String,
    pub handoff_id: String,
    pub target_message_id: String,
    pub target_turn_id: String,
    /// Current post-commit execution setup. This is read after provisioning
    /// reconciliation on both fresh creates and response-loss replays.
    pub execution_setup: crate::ProjectExecutionSetupResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ApproveProjectDocumentRequest {
    pub mutation: MutationEnvelope,
    pub document_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateProjectDocumentRequest {
    pub mutation: MutationEnvelope,
    pub kind: ProjectDocumentKind,
    pub title: String,
    pub approval_policy: ProjectDocumentApprovalPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct DecisionCandidateContext {
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub affected_artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    #[serde(default)]
    pub affected_milestone_ids: Vec<String>,
    /// Exact governing Charter revision for this candidate, when the
    /// decision is bound to Charter scope.
    #[serde(default)]
    pub governing_charter_revision_id: Option<String>,
    /// Exact governing execution-baseline revision for this candidate, when
    /// the decision is an implementation choice inside an active baseline.
    #[serde(default)]
    pub supersedes_decision_id: Option<String>,
    #[serde(default)]
    pub invalidates_decision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SaveProjectDocumentRevisionRequest {
    pub mutation: MutationEnvelope,
    pub base_revision_id: Option<String>,
    pub lifecycle: DocumentRevisionLifecycle,
    pub content: ProjectDocumentContent,
    pub change_summary: String,
    pub provenance: RevisionProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateDecisionCandidateRequest {
    pub mutation: MutationEnvelope,
    pub question: String,
    #[serde(default)]
    pub context: DecisionCandidateContext,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default)]
    pub selected_outcome: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    pub decision_class: DecisionClass,
    #[serde(default)]
    pub source_refs: Vec<ProvenanceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ApproveDecisionCandidateRequest {
    pub mutation: MutationEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RejectDecisionCandidateRequest {
    pub mutation: MutationEnvelope,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocumentRevisionDiffResponse {
    pub document_id: String,
    pub base_revision_id: Option<String>,
    pub revision_id: String,
    pub diff: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocumentListResponse {
    pub items: Vec<ProjectDocument>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocumentRevisionListResponse {
    pub items: Vec<ProjectDocumentRevision>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DecisionCandidateListResponse {
    pub items: Vec<DecisionCandidate>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DecisionRecordListResponse {
    pub items: Vec<DecisionRecord>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct EvaluateMilestoneReadinessRequest {
    pub mutation: MutationEnvelope,
    pub milestone_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AttachEvidenceRequest {
    pub mutation: MutationEnvelope,
    pub milestone_id: String,
    pub asset_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub source_run_id: Option<String>,
    #[serde(default)]
    pub source_validation_id: Option<String>,
    #[serde(default)]
    pub acceptance_check_ids: Vec<String>,
    pub caption: String,
    pub kind: EvidenceKind,
    pub checksum: String,
}

/// Multipart upload metadata.  The binary part is named `file`; clients
/// send this JSON value in the `mutation` part so the idempotency and explicit
/// user authorization are covered by the same public contract as other
/// Project mutations.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectMediaUploadRequest {
    pub mutation: MutationEnvelope,
}

/// An audited user-authorized disposition of a Project media asset.  The
/// storage key and bytes remain internal; callers address the asset only by
/// its stable Project URL and opaque asset id.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectMediaTombstoneRequest {
    pub mutation: MutationEnvelope,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReleaseMilestoneRequest {
    pub mutation: MutationEnvelope,
    pub milestone_id: String,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SetPrimaryMilestoneRequest {
    pub mutation: MutationEnvelope,
    #[serde(default)]
    pub primary_milestone_id: Option<String>,
}
