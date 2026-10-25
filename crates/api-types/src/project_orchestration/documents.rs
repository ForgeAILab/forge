//! Project Documents, Decisions, and adaptive Task governance contracts.

use super::*;

// ---------------------------------------------------------------------------
// Project Documents and Decisions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectDocumentKind {
    Research,
    DeliveryBrief,
    ProductSpec,
    Design,
    Architecture,
    ExecutionPlan,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectDocumentState {
    Active,
    Archived,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectDocumentApprovalPolicy {
    None,
    ProjectAgent,
    User,
    UserOrProjectAgent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DocumentRevisionLifecycle {
    Draft,
    Proposed,
    Approved,
    Rejected,
    Withdrawn,
    Superseded,
}

/// Truth status for a document's approved and working pointers. A working
/// revision ahead of the approved revision is a normal `changes_pending`
/// state, not evidence that approved Project truth is absent or stale.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DocumentFreshnessStatus {
    Current,
    ChangesPending,
    Stale,
    ReconciliationRequired,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ResearchSource {
    pub id: String,
    pub url: String,
    pub title: String,
    pub retrieved_at: String,
    #[serde(default)]
    pub quality: Option<String>,
    pub claim: String,
    pub is_inference: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DocumentAcceptanceItem {
    pub id: String,
    pub statement: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DocumentPlanItem {
    pub id: String,
    pub outcome: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ResearchDocumentContent {
    pub question: String,
    pub decision_informed: String,
    pub scope: String,
    pub stopping_condition: String,
    #[serde(default)]
    pub sources: Vec<ResearchSource>,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub inferences: Vec<String>,
    #[serde(default)]
    pub alternatives: Vec<String>,
    #[serde(default)]
    pub recommendation: Option<String>,
    #[serde(default)]
    pub uncertainty: Vec<String>,
    #[serde(default)]
    pub unresolved_questions: Vec<String>,
    #[serde(default)]
    pub affected_artifact_ids: Vec<String>,
    #[serde(default)]
    pub affected_decision_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBriefContent {
    #[serde(default)]
    pub intended_deliverables: Vec<String>,
    #[serde(default)]
    pub boundaries: Vec<String>,
    #[serde(default)]
    pub plan_items: Vec<DocumentPlanItem>,
    #[serde(default)]
    pub acceptance_matrix: Vec<DocumentAcceptanceItem>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
    #[serde(default)]
    pub rollback_and_recovery: Vec<String>,
    #[serde(default)]
    pub adaptive_envelope: Vec<String>,
    #[serde(default)]
    pub governing_charter_revision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProductSpecContent {
    pub problem_and_outcome: String,
    #[serde(default)]
    pub actors: Vec<String>,
    #[serde(default)]
    pub journeys_and_flows: Vec<String>,
    #[serde(default)]
    pub functional_requirements: Vec<String>,
    #[serde(default)]
    pub loading_empty_error_recovery_states: Vec<String>,
    #[serde(default)]
    pub acceptance_scenarios: Vec<DocumentAcceptanceItem>,
    #[serde(default)]
    pub non_functional_and_safety_requirements: Vec<String>,
    #[serde(default)]
    pub out_of_scope: Vec<String>,
    #[serde(default)]
    pub traceability: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DesignDocumentContent {
    #[serde(default)]
    pub experience_principles: Vec<String>,
    #[serde(default)]
    pub information_architecture: Vec<String>,
    #[serde(default)]
    pub flows: Vec<String>,
    #[serde(default)]
    pub design_tokens_reference: Option<String>,
    #[serde(default)]
    pub component_states: Vec<String>,
    #[serde(default)]
    pub responsive_behavior: Vec<String>,
    #[serde(default)]
    pub accessibility: Vec<String>,
    #[serde(default)]
    pub prototype_or_evidence_links: Vec<String>,
    #[serde(default)]
    pub open_decisions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ArchitectureDocumentContent {
    pub context_and_constraints: String,
    #[serde(default)]
    pub system_boundary: Vec<String>,
    #[serde(default)]
    pub components_and_data: Vec<String>,
    #[serde(default)]
    pub interfaces: Vec<String>,
    #[serde(default)]
    pub security_and_privacy: Vec<String>,
    #[serde(default)]
    pub concurrency: Vec<String>,
    #[serde(default)]
    pub failure_and_recovery: Vec<String>,
    #[serde(default)]
    pub observability_and_operations: Vec<String>,
    #[serde(default)]
    pub migrations: Vec<String>,
    #[serde(default)]
    pub alternatives_and_tradeoffs: Vec<String>,
    #[serde(default)]
    pub validation_plan: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanContent {
    #[serde(default)]
    pub ordered_milestone_outcomes: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
    #[serde(default)]
    pub linked_artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub task_queries_or_ids: Vec<String>,
    #[serde(default)]
    pub acceptance_evidence_contract: Vec<DocumentAcceptanceItem>,
    #[serde(default)]
    pub release_notes: Vec<String>,
    #[serde(default)]
    pub known_issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(tag = "kind", content = "content")]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub enum ProjectDocumentContent {
    Research(ResearchDocumentContent),
    DeliveryBrief(DeliveryBriefContent),
    ProductSpec(ProductSpecContent),
    Design(DesignDocumentContent),
    Architecture(ArchitectureDocumentContent),
    ExecutionPlan(ExecutionPlanContent),
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocument {
    pub id: String,
    pub project_id: String,
    pub kind: ProjectDocumentKind,
    pub title: String,
    pub state: ProjectDocumentState,
    pub approval_required: bool,
    #[serde(default)]
    pub current_draft_revision_id: Option<String>,
    #[serde(default)]
    pub current_approved_revision_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocumentRevision {
    pub id: String,
    pub document_id: String,
    pub project_id: String,
    pub revision_number: i64,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub lifecycle: DocumentRevisionLifecycle,
    pub schema_version: String,
    pub content: ProjectDocumentContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    #[serde(default)]
    pub approved_at: Option<String>,
    #[serde(default)]
    pub superseded_by_revision_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectDocumentApproval {
    pub id: String,
    pub document_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
    pub expected_document_version: i64,
    pub approved_by: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub approved_at: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DecisionRecordState {
    Active,
    Superseded,
    Invalidated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DecisionClass {
    UserScope,
    ProjectImplementation,
    Policy,
    Waiver,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DecisionEditorState {
    Draft,
    Proposed,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DecisionRecord {
    pub id: String,
    pub project_id: String,
    pub state: DecisionRecordState,
    pub question: String,
    #[serde(default)]
    pub context: Option<String>,
    #[serde(default)]
    pub options: Vec<String>,
    pub selected_outcome: String,
    pub rationale: String,
    pub decision_maker: PrincipalRef,
    pub decision_class: DecisionClass,
    #[serde(default)]
    pub authority_basis: Option<String>,
    #[serde(default)]
    pub affected_artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    #[serde(default)]
    pub affected_milestone_ids: Vec<String>,
    #[serde(default)]
    pub supersedes_id: Option<String>,
    #[serde(default)]
    pub provenance: Vec<ProvenanceRef>,
    pub created_at: String,
    pub effective_at: String,
}

/// Editor workflow state is intentionally separate from `DecisionRecordState`;
/// it cannot be used as effective Project truth.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct DecisionCandidate {
    pub id: String,
    pub project_id: String,
    pub editor_state: DecisionEditorState,
    pub question: String,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default)]
    pub selected_outcome: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    pub proposed_by: PrincipalRef,
    pub decision_class: DecisionClass,
    #[serde(default)]
    pub rejection_reason: Option<String>,
    #[serde(default)]
    pub effective_decision_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Whether a pending Decision candidate can currently be approved through
/// the shared candidate command service, or whether it is a historical row
/// that predates the D19/F15 candidate-shape invariant (a non-empty
/// question, at least two distinct non-empty options, a rationale, and a
/// recommendation that names one of those options). A malformed row is
/// preserved verbatim rather than rewritten or deleted; `validity` and
/// `invalid_reason` are how the projection marks it non-approvable instead.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PendingDecisionValidity {
    Valid,
    Malformed,
}

/// The affected artifact/Task/milestone references a pending Decision
/// candidate names, in the same shape as an effective `DecisionRecord`'s
/// affected records.
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PendingDecisionAffectedRecords {
    #[serde(default)]
    pub affected_artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    #[serde(default)]
    pub affected_milestone_ids: Vec<String>,
}

/// The exact REST route and method a pending-candidate action posts to.
/// Every surface renders this rather than hand-building the candidate
/// route.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PendingDecisionActionTarget {
    pub method: String,
    pub path: String,
}

/// A bounded, typed summary of one pending Decision candidate (design D19,
/// finding F15). `ProjectOverview` exposes these in place of the bare
/// `unresolved_decision_ids` identifier list so every surface can render the
/// question, its alternatives, the Project Agent's recommendation, its
/// rationale, and an approve/reject action without opaque UUIDs or a second
/// fetch. `approve_target`/`reject_target` name the same Decision candidates
/// REST routes the dedicated resource already exposes.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PendingDecisionSummary {
    pub id: String,
    pub project_id: String,
    pub lifecycle: DecisionEditorState,
    pub version: i64,
    pub question: String,
    pub options: Vec<String>,
    #[serde(default)]
    pub recommendation: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
    pub decision_class: DecisionClass,
    #[serde(default)]
    pub affected_records: PendingDecisionAffectedRecords,
    pub proposed_by: PrincipalRef,
    /// The principal class permitted to approve/reject this candidate.
    /// Always `user`: the shared candidate command service rejects any
    /// other principal's approval/rejection authorization.
    pub required_principal: PrincipalKind,
    pub validity: PendingDecisionValidity,
    /// Present only when `validity` is `malformed`; the exact reason no
    /// approval action is offered.
    #[serde(default)]
    pub invalid_reason: Option<String>,
    /// Absent when `validity` is `malformed`: approving a candidate whose
    /// shape violates the D19 invariant would promote that malformed shape
    /// into a permanent effective Decision, so no surface may offer it.
    #[serde(default)]
    pub approve_target: Option<PendingDecisionActionTarget>,
    /// Always present: rejecting a candidate never propagates its shape
    /// into anything consequential, so it remains how a malformed row is
    /// cleared.
    pub reject_target: PendingDecisionActionTarget,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceEvidenceRequirement {
    pub id: String,
    pub description: String,
    pub required: bool,
    #[serde(default)]
    pub evidence_kind: Option<String>,
}

/// The closed set of adaptive Task authority verbs a baseline's adaptive
/// envelope may grant. JSON is the bare lowercase string ("split",
/// "sequence", "replace") so every transport and the generated TypeScript
/// union stay closed together; there is no fourth value and no adapter may
/// invent one.
///
/// This is intentionally a different type from `db::AdaptiveTaskOperation`,
/// which additionally carries the payload for one executed split/sequence/
/// replace command. This type is the bare policy vocabulary a baseline
/// grants; it is never itself a command name such as `task.propose` or
/// `task.adaptive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AdaptiveTaskOperation {
    Split,
    Sequence,
    Replace,
}

impl AdaptiveTaskOperation {
    /// Every closed value, in the canonical order used by every generated
    /// schema and diagnostic. Deriving both the parser and the diagnostic
    /// from this one array is what keeps a fourth variant from silently
    /// going missing from an input schema or an error message.
    pub const ALL: [Self; 3] = [Self::Split, Self::Sequence, Self::Replace];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Sequence => "sequence",
            Self::Replace => "replace",
        }
    }

    /// Parse the bare wire verb. Anything outside the closed vocabulary
    /// (including a legacy/free-form value) returns `None` -- callers must
    /// treat that as a typed validation failure, never as an implicit grant.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|op| op.as_str() == value)
    }

    /// The closed vocabulary rendered for a diagnostic, in canonical order.
    #[must_use]
    pub fn supported_values() -> String {
        Self::ALL
            .iter()
            .map(|operation| operation.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl std::fmt::Display for AdaptiveTaskOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveEnvelope {
    #[serde(default)]
    pub allowed_task_operations: Vec<AdaptiveTaskOperation>,
    #[serde(default)]
    pub fixed_outcomes: Vec<String>,
    #[serde(default)]
    pub fixed_acceptance: Vec<String>,
    #[serde(default)]
    pub fixed_risk_classes: Vec<String>,
    #[serde(default)]
    pub forbidden_side_effects: Vec<String>,
    #[serde(default)]
    pub elevated_operations: Vec<String>,
}

/// Server-checked provenance for a Project Task.
///
/// A Charter-backed implementation Task is bound to the Project's current
/// approved Charter. Optional baseline, plan-item, milestone, and document
/// references add traceability but do not authorize execution. The database
/// keeps the immutable copy in `project_task_governance`; the server derives
/// `runnable` from Charter and repository readiness and never trusts a
/// caller-provided flag.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq, schemars::JsonSchema)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct TaskGovernanceRequest {
    #[serde(default)]
    pub charter_revision_id: Option<String>,
    #[serde(default)]
    pub plan_item_id: Option<String>,
    #[serde(default)]
    pub milestone_id: Option<String>,
    #[serde(default)]
    pub document_revision_ids: Vec<String>,
    #[serde(default)]
    pub capability_class: Option<String>,
    #[serde(default)]
    pub risk_class: Option<String>,
    /// Bounded caller provenance (for example adaptive split/replacement
    /// origin).  Forge augments this with the governing baseline digest and
    /// envelope digest before persistence.
    #[serde(default)]
    #[ts(type = "Record<string, unknown> | null")]
    pub provenance: Option<serde_json::Value>,
}
