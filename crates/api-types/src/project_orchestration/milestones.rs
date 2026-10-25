//! Milestone definitions, readiness, evidence, and release contracts.

use super::*;

// ---------------------------------------------------------------------------
// Milestones, checks, readiness, evidence, and releases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum MilestoneDefinitionLifecycle {
    Draft,
    Proposed,
    Approved,
    Superseded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum MilestoneLifecycle {
    Planned,
    Active,
    ReadyForRelease,
    Released,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum MilestoneProjectionReasonKind {
    Blocker,
    Stale,
    ReconciliationRequired,
    DependencyMissing,
    CheckFailed,
    EvidenceUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneProjectionReason {
    pub kind: MilestoneProjectionReasonKind,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub source_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AcceptanceCheckSourceKind {
    TaskValidation,
    DocumentApproval,
    Manual,
    PolicyWaiver,
    MediaEvidence,
    GitRef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AcceptanceCheckResultStatus {
    Pass,
    Fail,
    Pending,
    Blocked,
    Stale,
    Unavailable,
    Waived,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneAcceptanceCheck {
    pub id: String,
    pub description: String,
    pub required: bool,
    pub source_kind: AcceptanceCheckSourceKind,
    pub expected_result: String,
    #[serde(default)]
    pub latest_result: Option<AcceptanceCheckResultStatus>,
    #[serde(default)]
    pub latest_result_id: Option<String>,
    #[serde(default)]
    pub latest_result_digest: Option<String>,
}

/// Live acceptance-check state for Project Overview actions. This is kept
/// outside immutable `MilestoneDefinitionContent`, so optimistic-concurrency
/// metadata can never change a definition digest.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneAcceptanceCheckState {
    pub id: String,
    pub description: String,
    pub required: bool,
    pub source_kind: AcceptanceCheckSourceKind,
    pub expected_result: String,
    pub version: i64,
    #[serde(default)]
    pub latest_result: Option<AcceptanceCheckResultStatus>,
    #[serde(default)]
    pub latest_result_id: Option<String>,
    #[serde(default)]
    pub latest_result_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneDefinitionContent {
    pub name: String,
    pub outcome: String,
    #[serde(default)]
    pub included_scope: Vec<String>,
    #[serde(default)]
    pub excluded_scope: Vec<String>,
    #[serde(default)]
    pub charter_revision: Option<ArtifactRef>,
    #[serde(default)]
    pub document_revisions: Vec<ArtifactRef>,
    #[serde(default)]
    pub task_ids: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
    #[serde(default)]
    pub acceptance_checks: Vec<MilestoneAcceptanceCheck>,
    #[serde(default)]
    pub evidence_requirements: Vec<AcceptanceEvidenceRequirement>,
    #[serde(default)]
    pub known_issues: Vec<String>,
    #[serde(default)]
    pub target_date: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneDefinitionRevision {
    pub id: String,
    pub milestone_id: String,
    pub project_id: String,
    pub revision_number: i64,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub lifecycle: MilestoneDefinitionLifecycle,
    pub schema_version: String,
    pub content: MilestoneDefinitionContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    pub created_at: String,
}

/// Create the first immutable definition revision for a Project-local
/// milestone.  The server derives the revision number and canonical digests;
/// callers provide the exact authored content and provenance.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateMilestoneRequest {
    pub mutation: MutationEnvelope,
    pub display_label: Option<String>,
    pub lifecycle: MilestoneDefinitionLifecycle,
    pub content: MilestoneDefinitionContent,
    pub rendered_view: String,
    pub render_version: String,
    pub change_summary: String,
    pub provenance: RevisionProvenance,
}

/// Append one immutable definition revision using the exact UUID of its base
/// revision for optimistic concurrency.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SaveMilestoneRevisionRequest {
    pub mutation: MutationEnvelope,
    pub base_revision_id: String,
    pub lifecycle: MilestoneDefinitionLifecycle,
    pub content: MilestoneDefinitionContent,
    pub rendered_view: String,
    pub render_version: String,
    pub change_summary: String,
    pub provenance: RevisionProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct TransitionMilestoneRevisionRequest {
    pub mutation: MutationEnvelope,
    pub lifecycle: MilestoneDefinitionLifecycle,
}

/// Transition the mutable milestone instance lifecycle.  This is deliberately
/// separate from definition-revision lifecycle transitions: a revision is an
/// immutable definition with a small approval state, while the milestone
/// instance owns planned/active/ready/released/cancelled progress.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct TransitionMilestoneRequest {
    pub mutation: MutationEnvelope,
    pub lifecycle: MilestoneLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct RecordMilestoneCheckRequest {
    pub mutation: MutationEnvelope,
    pub check_id: String,
    pub definition_revision_id: String,
    pub status: AcceptanceCheckResultStatus,
    pub result: String,
    pub input_digest: String,
    pub governing_revision_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WaiveMilestoneCheckRequest {
    pub mutation: MutationEnvelope,
    pub check_id: String,
    pub definition_revision_id: String,
    pub reason: String,
    pub input_digest: String,
    pub governing_revision_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectMilestone {
    pub id: String,
    pub project_id: String,
    pub milestone_sequence: i64,
    pub canonical_id: String,
    #[serde(default)]
    pub display_label: Option<String>,
    pub definition_revision_id: String,
    pub lifecycle: MilestoneLifecycle,
    #[serde(default)]
    pub projection_reasons: Vec<MilestoneProjectionReason>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectMilestoneListResponse {
    pub items: Vec<ProjectMilestone>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MilestoneDefinitionRevisionListResponse {
    pub items: Vec<MilestoneDefinitionRevision>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReadinessSnapshotListResponse {
    pub items: Vec<ReadinessSnapshot>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PrimaryMilestonePointer {
    pub project_id: String,
    #[serde(default)]
    pub primary_milestone_id: Option<String>,
    pub expected_project_version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ValidationResult {
    pub id: String,
    pub project_id: String,
    pub check_id: String,
    pub status: AcceptanceCheckResultStatus,
    pub result: String,
    pub principal: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub input_digest: String,
    pub governing_revision_ids: Vec<String>,
    pub expected_version: i64,
    pub event_id: String,
    pub evaluated_at: String,
    pub result_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReadinessInput {
    pub source_kind: String,
    pub source_id: String,
    pub source_version: i64,
    pub source_digest: String,
    pub observed_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReadinessResult {
    Ready,
    Blocked,
    Failed,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReadinessReason {
    pub code: String,
    pub message: String,
    pub blocking: bool,
    #[serde(default)]
    pub check_id: Option<String>,
    #[serde(default)]
    pub source_ids: Vec<String>,
}

/// Freshness overlay for the immutable readiness snapshot shown in a live
/// Overview. The snapshot remains inspectable when stale; this overlay is the
/// only field that tells callers whether it may be used for a release.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReadinessFreshnessStatus {
    Current,
    Stale,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReadinessFreshness {
    pub status: ReadinessFreshnessStatus,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub snapshot_source_event_watermark: Option<String>,
    #[serde(default)]
    pub current_source_event_watermark: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReadinessSnapshot {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    /// The immutable milestone CAS version used to compute this candidate.
    /// A readiness request is bound to this exact version; it is not inferred
    /// from the mutable milestone returned later.
    pub expected_milestone_version: i64,
    pub milestone_definition_revision_id: String,
    pub input_manifest: Vec<ReadinessInput>,
    pub source_event_watermark: String,
    pub result: ReadinessResult,
    #[serde(default)]
    pub reasons: Vec<ReadinessReason>,
    pub check_results: Vec<ValidationResult>,
    pub waiver_ids: Vec<String>,
    pub evidence_attachment_ids: Vec<String>,
    pub evidence_digests: Vec<String>,
    pub evidence_availability: Vec<EvidenceAvailability>,
    pub commit_build_check_context: Vec<String>,
    pub computing_policy_revision: String,
    pub readiness_digest: String,
    pub computed_at: String,
    /// The complete authority receipt for the readiness computation.  This
    /// is persisted and replay-compared; it is never reconstructed from the
    /// current authenticated user.
    pub requesting_principal: PrincipalRef,
    pub authorization: AuthorizationProvenance,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EvidenceKind {
    Screenshot,
    WalkthroughVideo,
    Log,
    Report,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EvidenceAvailability {
    Available,
    Quarantined,
    Redacted,
    Purged,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct MediaAsset {
    pub id: String,
    pub project_id: String,
    pub original_filename: String,
    pub content_type: String,
    pub byte_size: u64,
    pub checksum: String,
    pub availability: EvidenceAvailability,
    #[serde(default)]
    pub task_media_ids: Vec<String>,
    #[serde(default)]
    pub stable_project_url: Option<String>,
    pub created_at: String,
    #[serde(default)]
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectMediaListResponse {
    pub items: Vec<MediaAsset>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct EvidenceAttachment {
    pub id: String,
    pub project_id: String,
    pub asset_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub source_task_id: Option<String>,
    #[serde(default)]
    pub source_run_id: Option<String>,
    #[serde(default)]
    pub source_validation_id: Option<String>,
    /// Exact Task row version observed when this evidence was captured.
    /// A missing value is legacy/unpinned and cannot satisfy a required
    /// release-gating evidence requirement.
    #[serde(default)]
    pub source_task_version: Option<i64>,
    /// Digest of the repository/execution/review context observed when this
    /// evidence was captured.  It is compared with the current readiness
    /// context before evidence can support release.
    #[serde(default)]
    pub source_context_digest: Option<String>,
    /// The immutable milestone definition revision that governed the
    /// attachment.  This is separate from the attachment's own row version.
    #[serde(default)]
    pub source_definition_revision_id: Option<String>,
    #[serde(default)]
    pub milestone_id: Option<String>,
    #[serde(default)]
    pub acceptance_check_ids: Vec<String>,
    pub caption: String,
    pub kind: EvidenceKind,
    pub checksum: String,
    pub availability: EvidenceAvailability,
    pub author: PrincipalRef,
    pub captured_at: String,
    pub version: i64,
    pub created_at: String,
    #[serde(default)]
    pub removed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct EvidenceAttachmentListResponse {
    pub items: Vec<EvidenceAttachment>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct EvidencePin {
    pub id: String,
    pub release_id: String,
    pub attachment_id: String,
    pub asset_id: String,
    pub attachment_digest: String,
    pub asset_checksum: String,
    pub availability: EvidenceAvailability,
    /// Read-time overlay derived from an immutable, audited media tombstone.
    /// The historical `availability` above is never mutated after pinning.
    pub availability_projection: ReleaseEvidenceAvailability,
    #[serde(default)]
    pub task_media_id: Option<String>,
    #[serde(default)]
    pub stable_project_url: Option<String>,
    pub pinned_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReleaseEvidenceAvailability {
    Available,
    Quarantined,
    Redacted,
    Purged,
    EvidenceUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReleaseTaskReference {
    pub task_id: String,
    pub task_version: i64,
    pub task_type: String,
    pub task_state: String,
    #[serde(default)]
    pub acceptance_check_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReleaseDecisionReference {
    pub decision_id: String,
    pub state: DecisionRecordState,
    pub digest: String,
    pub rationale: String,
    pub authorization: AuthorizationProvenance,
    /// A decision may govern a whole baseline/Charter rather than one check;
    /// scope is therefore explicit and nullable rather than fabricated.
    pub affected_milestone_id: Option<String>,
    pub affected_check_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReleaseValidationReference {
    pub validation_id: String,
    pub result_digest: String,
    pub evaluated_at: String,
    pub principal: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub status: AcceptanceCheckResultStatus,
    pub result: String,
    pub input_digest: String,
    pub governing_revision_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ReleaseSnapshot {
    pub schema_version: String,
    pub project_id: String,
    pub milestone_id: String,
    pub milestone_canonical_id: String,
    pub release_revision: i64,
    pub release_identity: String,
    pub milestone_definition_revision_id: String,
    pub milestone_definition_digest: String,
    pub expected_milestone_version: i64,
    #[serde(default)]
    pub display_label: Option<String>,
    pub summary: String,
    #[serde(default)]
    pub changelog: Vec<String>,
    #[serde(default)]
    pub known_issues: Vec<String>,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub source_event_watermark: String,
    pub charter_revision: ArtifactRef,
    #[serde(default)]
    pub document_revisions: Vec<ArtifactRef>,
    #[serde(default)]
    pub included_decisions: Vec<ReleaseDecisionReference>,
    #[serde(default)]
    pub included_tasks: Vec<ReleaseTaskReference>,
    #[serde(default)]
    pub validation_results: Vec<ReleaseValidationReference>,
    #[serde(default)]
    pub repository_references: Vec<String>,
    pub evidence_pins: Vec<EvidencePin>,
    pub waived_check_ids: Vec<String>,
    pub released_by: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub released_at: String,
    pub idempotency_key: String,
    pub snapshot_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectRelease {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub release_sequence: i64,
    pub release_identity: String,
    pub snapshot: ReleaseSnapshot,
    pub version: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectReleaseListResponse {
    pub items: Vec<ProjectRelease>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
