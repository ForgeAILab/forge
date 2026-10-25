//! Project Overview and reconciliation projection contracts.

use super::*;

// ---------------------------------------------------------------------------
// Project Overview projections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum OverviewProjectionState {
    Current,
    Loading,
    Stale,
    Error,
    PermissionDenied,
}

/// Server-authored next action for the Project Overview.  Clients render this
/// action and dispatch the named operation; they do not infer a command from
/// free-form text or from other Overview fields.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectNextAction {
    /// Stable machine-readable action identifier.
    pub code: String,
    /// Principal class expected to perform the action (`user`,
    /// `project_agent`, `worker`, `reviewer`, or `system`).
    pub required_principal: String,
    pub target_type: String,
    pub target_id: String,
    pub title: String,
    pub explanation: String,
    /// Stable presentation/interaction category such as `approval`,
    /// `reconciliation`, `setup`, `validation`, `readiness`, or `release`.
    pub action_kind: String,
    /// Canonical operation identifier, not a client-assembled URL.
    pub route_or_operation: String,
    pub blocking: bool,
    #[serde(default)]
    pub expected_version: Option<i64>,
}

/// The five terminal outcomes a reconciliation resolution may record. This
/// mirrors the closed `CHECK` on `project_reconciliation_record.state` and
/// `project_reconciliation_resolution.action` exactly; there is no sixth
/// value and no free-form escape hatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReconciliationResolutionAction {
    Retained,
    Revised,
    Cancelled,
    Superseded,
    Invalidated,
}

/// A reconciliation's lifecycle state: `Required` until an explicit
/// resolution advances it to one of the four other terminal values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ReconciliationState {
    Required,
    Retained,
    Revised,
    Cancelled,
    Superseded,
    Invalidated,
}

/// A typed pointer to one canonical record: its type, id, and the exact
/// revision/digest observed when the conflict or reconciliation was
/// recorded. Bodies are intentionally not inlined here -- callers load the
/// named record through its own typed endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReconciliationRecordRef {
    pub record_type: String,
    pub record_id: String,
    pub record_revision: String,
    pub record_digest: String,
}

/// The exact successor artifact a `revised`/`superseded` resolution names.
/// Required together with the action; the shared service rejects a
/// `revised`/`superseded` resolve request that omits it and rejects one
/// supplied for any other action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReconciliationReplacementRef {
    pub record_type: String,
    pub record_id: String,
    pub record_revision: Option<String>,
}

/// The immutable canonical conflict a reconciliation was opened against.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReconciliationConflictSummary {
    pub id: String,
    pub domain: String,
    pub governing: ReconciliationRecordRef,
    pub conflicting: ReconciliationRecordRef,
    pub affected_paths: Vec<String>,
    pub conflict_code: String,
    pub description: String,
    pub detected_by_type: String,
    pub detected_by_id: Option<String>,
    pub created_at: String,
}

/// The resolution already applied, present only once `state != required`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ReconciliationResolutionSummary {
    pub id: String,
    pub action: ReconciliationResolutionAction,
    pub principal: PrincipalRef,
    pub reason: String,
    pub replacement_ref: Option<ReconciliationReplacementRef>,
    pub occurred_at: String,
}

/// The complete list/detail projection. `allowed_actions` is empty once the
/// record leaves `required`: a resolved reconciliation offers no further
/// action, only its recorded outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectReconciliation {
    pub id: String,
    pub project_id: String,
    pub conflict: ReconciliationConflictSummary,
    /// The record whose claim diverges from `governing`. For the adaptive
    /// Task-boundary fixture this is the Task at the version the divergence
    /// was detected.
    pub affected: ReconciliationRecordRef,
    /// The record whose claim is authoritative until the reconciliation is
    /// resolved.
    pub governing: ReconciliationRecordRef,
    pub state: ReconciliationState,
    pub required_principal: PrincipalKind,
    pub allowed_actions: Vec<ReconciliationResolutionAction>,
    /// Server-validated replacement currently eligible for this resolution.
    /// For `invalid_active_baseline` this may be the server-prepared correction
    /// draft; resolving it is also the interactive user's exact approval event.
    #[serde(default)]
    pub suggested_replacement_ref: Option<ReconciliationReplacementRef>,
    pub resolution: Option<ReconciliationResolutionSummary>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProjectReconciliationListResponse {
    pub items: Vec<ProjectReconciliation>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ResolveProjectReconciliationRequest {
    pub mutation: MutationEnvelope,
    pub action: ReconciliationResolutionAction,
    pub replacement_ref: Option<ReconciliationReplacementRef>,
    pub reason: String,
}

/// The frozen response to a resolve command: the exact final projection plus
/// the receipt/event identities a client can use to prove replay-exactness,
/// and whether the affected Task's dispatcher was woken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ResolveProjectReconciliationResponse {
    pub reconciliation: ProjectReconciliation,
    pub receipt_id: String,
    pub event_id: String,
    pub dispatch_woken: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct TaskProgressCounts {
    pub total: i64,
    pub backlog: i64,
    pub active: i64,
    pub review: i64,
    pub terminal: i64,
    pub blocked: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceCheckSummary {
    pub required_total: i64,
    pub passed: i64,
    pub failed: i64,
    pub missing: i64,
    pub stale: i64,
    pub waived: i64,
    pub unavailable: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DocumentFreshness {
    pub document_id: String,
    pub kind: ProjectDocumentKind,
    #[serde(default)]
    pub approved_revision_id: Option<String>,
    #[serde(default)]
    pub approved_digest: Option<String>,
    #[serde(default)]
    pub working_revision_id: Option<String>,
    #[serde(default)]
    pub working_digest: Option<String>,
    #[serde(default)]
    pub working_lifecycle: Option<DocumentRevisionLifecycle>,
    pub status: DocumentFreshnessStatus,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectMilestoneOverview {
    pub milestone: ProjectMilestone,
    pub definition: MilestoneDefinitionRevision,
    pub task_counts: TaskProgressCounts,
    pub check_summary: AcceptanceCheckSummary,
    #[serde(default)]
    pub current_checks: Vec<MilestoneAcceptanceCheckState>,
    #[serde(default)]
    pub latest_readiness: Option<ReadinessSnapshot>,
    #[serde(default)]
    pub readiness_freshness: Option<ReadinessFreshness>,
    #[serde(default)]
    pub evidence: Vec<EvidenceAttachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectOverview {
    pub project_id: String,
    pub project_name: String,
    pub vision: String,
    pub charter_state: ProjectCharterState,
    #[serde(default)]
    pub current_charter: Option<ProjectCharterRevision>,
    #[serde(default)]
    pub primary_milestone_id: Option<String>,
    #[serde(default)]
    pub active_milestones: Vec<ProjectMilestoneOverview>,
    pub task_counts: TaskProgressCounts,
    pub check_summary: AcceptanceCheckSummary,
    /// Bounded typed summaries of pending Decision candidates (design D19,
    /// finding F15). This replaced the bare `unresolved_decision_ids`
    /// identifier list in a public beta breaking response change: that
    /// field and every call site are gone, with no deprecated alias.
    #[serde(default)]
    pub pending_decisions: Vec<PendingDecisionSummary>,
    /// Effective Decision Log records. Draft/proposed candidates remain
    /// represented separately by `pending_decisions`.
    #[serde(default)]
    pub decisions: Vec<DecisionRecord>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
    #[serde(default)]
    pub document_freshness: Vec<DocumentFreshness>,
    #[serde(default)]
    pub evidence: Vec<EvidenceAttachment>,
    #[serde(default)]
    pub releases: Vec<ProjectRelease>,
    #[serde(default)]
    pub next_action: Option<ProjectNextAction>,
    pub projection_state: OverviewProjectionState,
    pub source_event_watermark: String,
    pub generated_at: String,
    /// Independent coordination/setup/gate projection. This is optional only
    /// for decoding historical cached Overview payloads; the REST route always
    /// supplies the current value.
    #[serde(default)]
    #[ts(optional = nullable)]
    pub execution_setup: Option<crate::ProjectExecutionSetupResponse>,
}
