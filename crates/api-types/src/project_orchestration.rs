//! Public API/domain contracts for Charter-backed Project orchestration.
//!
//! These types deliberately contain only closed, revision-addressable data.
//! Free-form JSON is not used for canonical Project artifacts: every value
//! which can affect approval, execution, readiness, or release is represented
//! by a named field or a closed enum.  The service layer remains responsible
//! for authorization and cross-record validation; this crate owns the wire
//! shape shared by Rust and TypeScript clients.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use ts_rs::TS;

use crate::ProductMaturity;

/// Schema used by the canonical JSON and digest helpers in this module.
pub const PROJECT_ORCHESTRATION_SCHEMA_VERSION: &str = "forge.project-orchestration/v1";
pub const CANONICAL_JSON_SCHEMA_VERSION: &str = "forge.canonical-json/v1";

// ---------------------------------------------------------------------------
// Shared provenance and references
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum PrincipalKind {
    User,
    Agent,
    Worker,
    Reviewer,
    Service,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct PrincipalRef {
    pub kind: PrincipalKind,
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationProvenance {
    pub principal: PrincipalRef,
    pub authorization_basis: String,
    pub action: String,
    pub event_id: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProvenanceSourceKind {
    User,
    MainChat,
    ProjectChat,
    Research,
    Task,
    Validation,
    Document,
    Decision,
    Milestone,
    Release,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceRef {
    pub source_kind: ProvenanceSourceKind,
    pub source_id: String,
    #[serde(default)]
    pub revision_id: Option<String>,
    #[serde(default)]
    pub digest: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub observed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub revision_id: String,
    pub content_digest: String,
    #[serde(default)]
    pub render_version: Option<String>,
    #[serde(default)]
    pub render_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct VersionedDigest {
    pub schema_version: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct RevisionProvenance {
    pub author: PrincipalRef,
    #[serde(default)]
    pub profile_revision: Option<String>,
    #[serde(default)]
    pub operating_skill_revision: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<ProvenanceRef>,
    pub change_summary: String,
    #[serde(default)]
    pub material_diff: Option<String>,
}

// ---------------------------------------------------------------------------
// Charter
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectMode {
    #[default]
    Compact,
    Standard,
}

impl ProjectMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Standard => "standard",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProjectCharterState {
    Approved,
    LegacyUnverified,
    CharterSetupRequired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterRevisionLifecycle {
    Draft,
    Proposed,
    Approved,
    Rejected,
    Withdrawn,
    Superseded,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterKnowledgeKind {
    ObservedFact,
    UserDecision,
    ResearchFinding,
    Assumption,
    Hypothesis,
    OpenDecision,
    ResearchQueue,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterConfidence {
    Low,
    Medium,
    High,
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterKnowledgeItem {
    pub id: String,
    pub statement: String,
    pub kind: CharterKnowledgeKind,
    pub normative: bool,
    pub transfer_approved: bool,
    #[serde(default)]
    pub provenance: Vec<ProvenanceRef>,
    #[serde(default)]
    pub confidence: Option<CharterConfidence>,
    #[serde(default)]
    pub observed_at: Option<String>,
    #[serde(default)]
    pub freshness_expires_at: Option<String>,
    #[serde(default)]
    pub impact: Option<String>,
    #[serde(default)]
    pub owner: Option<PrincipalRef>,
    #[serde(default)]
    pub default_value: Option<String>,
    #[serde(default)]
    pub revisit_trigger: Option<String>,
    #[serde(default)]
    pub falsification_evidence: Option<String>,
    #[serde(default)]
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterIdentity {
    pub working_name: String,
    #[serde(default)]
    pub slug_proposal: Option<String>,
    pub one_line_vision: String,
    pub maturity: ProductMaturity,
    #[serde(default)]
    pub lifecycle_intent: Option<String>,
    #[serde(default)]
    pub project_type: Option<String>,
    #[serde(default)]
    pub value_proposition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterProblemAndPeople {
    pub problem_or_opportunity: String,
    #[serde(default)]
    pub target_users: Vec<String>,
    #[serde(default)]
    pub beneficiaries: Vec<String>,
    #[serde(default)]
    pub jobs_pains_opportunity: Vec<String>,
    #[serde(default)]
    pub current_alternatives: Vec<String>,
    #[serde(default)]
    pub stakeholders: Vec<String>,
    #[serde(default)]
    pub excluded_audiences: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterCoreExperience {
    pub primary_outcome: String,
    #[serde(default)]
    pub core_loop: Option<String>,
    #[serde(default)]
    pub principal_journeys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterScope {
    #[serde(default)]
    pub must_have_outcomes: Vec<String>,
    #[serde(default)]
    pub required_deliverables: Vec<String>,
    #[serde(default)]
    pub later_possibilities: Vec<String>,
    #[serde(default)]
    pub explicit_non_goals: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterSuccessBoundary {
    #[serde(default)]
    pub qualitative_outcome: Option<String>,
    #[serde(default)]
    pub success_signals: Vec<String>,
    #[serde(default)]
    pub acceptance_statements: Vec<String>,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    #[serde(default)]
    pub non_claims: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterRisk {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub impact: Option<String>,
    #[serde(default)]
    pub treatment: Option<String>,
    #[serde(default)]
    pub revisit_trigger: Option<String>,
    #[serde(default)]
    pub owner: Option<PrincipalRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterConstraintsAndRisks {
    #[serde(default)]
    pub product: Vec<String>,
    #[serde(default)]
    pub time_and_budget: Vec<String>,
    #[serde(default)]
    pub technology: Vec<String>,
    #[serde(default)]
    pub data: Vec<String>,
    #[serde(default)]
    pub integrations: Vec<String>,
    #[serde(default)]
    pub security_privacy_compliance: Vec<String>,
    #[serde(default)]
    pub accessibility: Vec<String>,
    #[serde(default)]
    pub operations: Vec<String>,
    #[serde(default)]
    pub migration: Vec<String>,
    #[serde(default)]
    pub launch: Vec<String>,
    #[serde(default)]
    pub agent_authority: Vec<String>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterHandoffNote {
    #[serde(default)]
    pub recommended_first_action: Option<String>,
    #[serde(default)]
    pub bounded_summary: Option<String>,
    #[serde(default)]
    pub unresolved_item_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterKnowledgeLedger {
    #[serde(default)]
    pub items: Vec<CharterKnowledgeItem>,
}

/// The canonical typed payload hashed by a Charter content digest.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectCharterContent {
    pub identity: CharterIdentity,
    pub problem_and_people: CharterProblemAndPeople,
    pub core_experience: CharterCoreExperience,
    pub scope: CharterScope,
    pub success: CharterSuccessBoundary,
    pub constraints_and_risks: CharterConstraintsAndRisks,
    pub knowledge_ledger: CharterKnowledgeLedger,
    #[serde(default)]
    pub handoff_note: Option<CharterHandoffNote>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterReadinessStatus {
    Ready,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterReadinessGapKind {
    MissingContent,
    IncoherentContent,
    UnresolvedBlockingUnknown,
    MissingProvenance,
    MissingAcceptanceBoundary,
    MissingMaterialConcern,
    InvalidTransfer,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterReadinessGap {
    pub kind: CharterReadinessGapKind,
    pub code: String,
    pub message: String,
    pub blocking: bool,
    #[serde(default)]
    pub section: Option<String>,
    #[serde(default)]
    pub knowledge_item_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectCharterReadiness {
    pub status: CharterReadinessStatus,
    pub project_mode: ProjectMode,
    pub maturity: ProductMaturity,
    #[serde(default)]
    pub gaps: Vec<CharterReadinessGap>,
    pub policy_revision: String,
    pub evaluated_at: String,
    pub readiness_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectCharterRevision {
    pub id: String,
    pub charter_id: String,
    pub revision_number: i64,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub lifecycle: CharterRevisionLifecycle,
    pub project_mode: ProjectMode,
    pub maturity: ProductMaturity,
    pub schema_version: String,
    pub content: ProjectCharterContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    #[serde(default)]
    pub readiness: Option<ProjectCharterReadiness>,
    #[serde(default)]
    pub approved_at: Option<String>,
    #[serde(default)]
    pub superseded_by_revision_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectCharter {
    pub id: String,
    #[serde(default)]
    pub genesis_session_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    pub state: ProjectCharterState,
    pub project_mode: ProjectMode,
    pub maturity: ProductMaturity,
    #[serde(default)]
    pub current_draft_revision_id: Option<String>,
    #[serde(default)]
    pub current_approved_revision_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterApprovalType {
    ProjectCreation,
    CharterAmendment,
    Adoption,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterApprovalState {
    Active,
    Consumed,
    Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProjectCharterApproval {
    pub id: String,
    pub approval_type: CharterApprovalType,
    pub charter_id: String,
    pub charter_revision_id: String,
    pub charter_content_digest: String,
    pub charter_render_digest: String,
    pub expected_charter_version: i64,
    pub approved_project_name: String,
    #[serde(default)]
    pub approved_project_slug: Option<String>,
    pub approved_project_mode: ProjectMode,
    pub selected_project_agent_identity_id: String,
    pub selected_project_agent_profile_revision_id: String,
    pub selected_project_agent_operating_skill_revision: String,
    pub selected_project_agent_policy_digest: String,
    pub approved_by: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub approval_event_id: String,
    pub approved_at: String,
    pub state: CharterApprovalState,
    #[serde(default)]
    pub consumed_by_project_id: Option<String>,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProductAgentSelection {
    pub identity_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub profile_revision_id: String,
    pub operating_skill_revision: String,
    pub policy_digest: String,
}

/// Canonical Charter projection rendered inside the singular Main Chat.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ProductGenesisCharterResponse {
    /// Absent until the Main Agent persists the first canonical Charter.
    /// Forge never fabricates a placeholder Charter for projection purposes.
    #[serde(default)]
    pub charter: Option<ProjectCharter>,
    #[serde(default)]
    pub revisions: Vec<ProjectCharterRevision>,
    #[serde(default)]
    pub current_draft_revision: Option<ProjectCharterRevision>,
    #[serde(default)]
    pub current_approved_revision: Option<ProjectCharterRevision>,
    #[serde(default)]
    pub approval: Option<ProjectCharterApproval>,
    #[serde(default)]
    pub selected_project_agent: Option<ProductAgentSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterSupersession {
    pub id: String,
    pub charter_id: String,
    pub previous_revision_id: String,
    pub superseding_revision_id: String,
    pub approval_id: String,
    pub principal: PrincipalRef,
    pub reason: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CharterAmendmentState {
    Draft,
    Proposed,
    Approved,
    Rejected,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterAmendment {
    pub id: String,
    pub charter_id: String,
    pub state: CharterAmendmentState,
    pub base_revision_id: String,
    pub candidate_revision_id: String,
    pub base_content_digest: String,
    pub candidate_content_digest: String,
    pub base_render_digest: String,
    pub candidate_render_digest: String,
    pub rationale: String,
    pub material_diff: String,
    pub requested_by: PrincipalRef,
    pub expected_current_charter_version: i64,
    #[serde(default)]
    pub affected_decision_ids: Vec<String>,
    #[serde(default)]
    pub affected_document_ids: Vec<String>,
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    #[serde(default)]
    pub affected_execution_baseline_ids: Vec<String>,
    #[serde(default)]
    pub affected_milestone_ids: Vec<String>,
    pub created_at: String,
}

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
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
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

// ---------------------------------------------------------------------------
// Canonical JSON / digest helpers
// ---------------------------------------------------------------------------

/// Recursively sort object keys and leave array order untouched.
///
/// `serde_json::Value` uses an implementation-defined map representation when
/// feature flags change.  Converting through a `BTreeMap` makes the ordering
/// explicit at every object depth and keeps digests independent of input map
/// insertion order.
pub fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<&str, Value> = object
                .iter()
                .map(|(key, value)| (key.as_str(), canonicalize_json(value)))
                .collect();
            let mut canonical = Map::new();
            for (key, value) in sorted {
                canonical.insert(key.to_owned(), value);
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        scalar => scalar.clone(),
    }
}

/// Serialize a value into compact, recursively key-sorted JSON.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    serde_json::to_string(&canonicalize_json(&value))
}

/// Serialize a schema-versioned canonical envelope.  The schema participates
/// in the digest domain so changing the wire contract cannot accidentally
/// reuse an old digest.
pub fn canonical_json_with_schema<T: Serialize>(
    schema_version: &str,
    value: &T,
) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut envelope = Map::new();
    envelope.insert(
        "schema_version".to_owned(),
        Value::String(schema_version.to_owned()),
    );
    envelope.insert("value".to_owned(), canonicalize_json(&value));
    serde_json::to_string(&canonicalize_json(&Value::Object(envelope)))
}

/// SHA-256 digest of the default schema-versioned canonical JSON, encoded as
/// lowercase hexadecimal for use in API fields and optimistic comparisons.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    canonical_digest_with_schema(CANONICAL_JSON_SCHEMA_VERSION, value)
}

/// SHA-256 digest of a schema-versioned canonical JSON envelope.
pub fn canonical_digest_with_schema<T: Serialize>(
    schema_version: &str,
    value: &T,
) -> Result<String, serde_json::Error> {
    let canonical = canonical_json_with_schema(schema_version, value)?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(hex_lower(&digest))
}

/// Digest a rendered view with the render version in the canonical payload.
pub fn canonical_render_digest(
    render_version: &str,
    rendered_view: &str,
) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct Render<'a> {
        render_version: &'a str,
        rendered_view: &'a str,
    }

    canonical_digest_with_schema(
        PROJECT_ORCHESTRATION_SCHEMA_VERSION,
        &Render {
            render_version,
            rendered_view,
        },
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

// ---------------------------------------------------------------------------
// Main Chat topic boundary (design D21, live-acceptance finding F18)
// ---------------------------------------------------------------------------

/// Who started a Main Chat topic. `System` is used only for the migration
/// backfill's one initial topic per existing Main Chat; every topic a user
/// starts from the product is `User`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatTopicPrincipalType {
    User,
    System,
}

/// One durable Main Chat topic (D21). A topic is a context epoch *inside*
/// the one account Main Chat -- it is never a second chat, binding, or
/// authority scope. `starting_message_sequence` is the sequence of the
/// visible divider message that opens it; a new Main turn's episodic context
/// is bounded to messages at or after this value in the current topic.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AgentChatTopicResponse {
    pub id: String,
    pub chat_id: String,
    pub sequence: i64,
    pub label: String,
    pub summary: Option<String>,
    pub starting_message_id: Option<String>,
    pub starting_message_sequence: i64,
    pub principal_type: AgentChatTopicPrincipalType,
    pub principal_id: Option<String>,
    pub created_at: String,
    /// True for exactly one topic per chat: the most recently started one.
    pub is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AgentChatTopicListResponse {
    pub items: Vec<AgentChatTopicResponse>,
}

/// `label`/`summary` are optional -- an empty request still starts a topic
/// with a server-assigned default label.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct StartAgentChatTopicRequest {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct StartAgentChatTopicResponse {
    pub topic: AgentChatTopicResponse,
    /// The visible divider message appended to the chat timeline at the
    /// start of this topic.
    pub divider_message_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_sorts_nested_object_keys_without_reordering_arrays() {
        let left = json!({
            "z": {"b": 2, "a": 1},
            "items": [{"second": true, "first": false}]
        });
        let right = json!({
            "items": [{"first": false, "second": true}],
            "z": {"a": 1, "b": 2}
        });

        assert_eq!(
            canonical_json(&left).unwrap(),
            canonical_json(&right).unwrap()
        );
        assert_eq!(
            canonical_digest(&left).unwrap(),
            canonical_digest(&right).unwrap()
        );

        let reversed = json!({"items": [{"first": true, "second": false}], "z": {"a": 1, "b": 2}});
        assert_ne!(
            canonical_digest(&left).unwrap(),
            canonical_digest(&reversed).unwrap()
        );
    }

    #[test]
    fn schema_version_is_part_of_the_digest_domain() {
        let value = json!({"name": "forge"});
        assert_ne!(
            canonical_digest_with_schema("schema/a", &value).unwrap(),
            canonical_digest_with_schema("schema/b", &value).unwrap()
        );
    }

    #[test]
    fn rendered_view_digest_includes_render_version() {
        assert_ne!(
            canonical_render_digest("render/v1", "# Forge").unwrap(),
            canonical_render_digest("render/v2", "# Forge").unwrap()
        );
    }

    #[test]
    fn canonical_digest_is_sha256_hex() {
        let digest = canonical_digest(&json!({"a": 1})).unwrap();
        assert_eq!(digest.len(), 64);
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn nested_authorization_unknown_fields_are_rejected() {
        let value = json!({
            "expected_version": 1,
            "idempotency_key": "mutation-1",
            "authorization": {
                "principal": {
                    "kind": "user",
                    "id": "user-1",
                    "unexpected": "must fail"
                },
                "authorization_basis": "explicit_user_action",
                "action": "project.document.approve",
                "event_id": "event-1",
                "occurred_at": "2026-08-13T00:00:00Z"
            }
        });

        assert!(serde_json::from_value::<MutationEnvelope>(value).is_err());
    }

    #[test]
    fn nested_charter_document_and_milestone_unknown_fields_are_rejected() {
        let mut charter = json!({
            "identity": {
                "working_name": "Forge",
                "one_line_vision": "A bounded project",
                "maturity": "mvp"
            },
            "problem_and_people": {"problem_or_opportunity": "A problem"},
            "core_experience": {"primary_outcome": "An outcome"},
            "scope": {},
            "success": {},
            "constraints_and_risks": {},
            "knowledge_ledger": {}
        });
        charter["identity"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<ProjectCharterContent>(charter).is_err());

        let mut document = json!({
            "kind": "Research",
            "content": {
                "question": "What is known?",
                "decision_informed": "A bounded decision",
                "scope": "Public sources",
                "stopping_condition": "One authoritative source"
            }
        });
        document["content"]["unexpected"] = json!("must fail");
        assert!(serde_json::from_value::<ProjectDocumentContent>(document).is_err());

        let mut milestone = json!({
            "name": "M1",
            "outcome": "A measurable outcome"
        });
        milestone["unexpected"] = json!("must fail");
        assert!(serde_json::from_value::<MilestoneDefinitionContent>(milestone).is_err());
    }

    #[test]
    fn mutation_and_governance_envelopes_reject_unknown_fields() {
        let task_governance = json!({
            "milestone_id": "milestone-1",
            "unknown": "must fail"
        });
        assert!(serde_json::from_value::<TaskGovernanceRequest>(task_governance).is_err());
    }
}
