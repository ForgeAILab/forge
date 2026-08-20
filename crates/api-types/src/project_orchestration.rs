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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionBaselineLifecycle {
    Draft,
    Proposed,
    Approved,
    Active,
    Superseded,
    Revoked,
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
    #[serde(default)]
    pub check_definition_revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveEnvelope {
    #[serde(default)]
    pub allowed_task_operations: Vec<String>,
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

/// The frozen release policy carried by an execution baseline.
///
/// A baseline must bind the concrete checks, evidence, review, dependency,
/// side-effect, and correction rules that were in force when the user
/// approved it.  The policy digest is derived from this closed value by the
/// server; the surrounding revision/digest fields are indexed projections
/// used by admission queries and must match it exactly.
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ExecutionBaselineReleasePolicy {
    pub schema_version: String,
    pub revision: String,
    #[serde(default)]
    pub required_check_definition_revisions: Vec<String>,
    #[serde(default)]
    pub reviewer_independence_rules: Vec<String>,
    #[serde(default)]
    pub manual_attestation_rules: Vec<String>,
    #[serde(default)]
    pub waiver_rules: Vec<String>,
    #[serde(default)]
    pub evidence_kinds: Vec<String>,
    #[serde(default)]
    pub evidence_contexts: Vec<String>,
    #[serde(default)]
    pub evidence_freshness_rules: Vec<String>,
    #[serde(default)]
    pub dependency_rules: Vec<String>,
    #[serde(default)]
    pub stale_input_rules: Vec<String>,
    #[serde(default)]
    pub forbidden_side_effects: Vec<String>,
    #[serde(default)]
    pub known_issue_rules: Vec<String>,
    #[serde(default)]
    pub correction_rules: Vec<String>,
    #[serde(default)]
    pub purge_rules: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaselineContent {
    pub charter_revision: ArtifactRef,
    #[serde(default)]
    pub document_revisions: Vec<ArtifactRef>,
    #[serde(default)]
    pub plan_item_ids: Vec<String>,
    #[serde(default)]
    pub milestone_ids: Vec<String>,
    /// Immutable definition revision paired positionally with each milestone.
    /// Activation revalidates these exact revisions before promoting the
    /// milestone projection.
    pub milestone_definition_revision_ids: Vec<String>,
    #[serde(default)]
    pub primary_milestone_id: Option<String>,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    /// The exact frozen release-policy contract whose canonical digest must
    /// equal `release_policy_digest`.
    pub release_policy: ExecutionBaselineReleasePolicy,
    #[serde(default)]
    pub acceptance_evidence_matrix: Vec<AcceptanceEvidenceRequirement>,
    #[serde(default)]
    pub capability_classes: Vec<String>,
    #[serde(default)]
    pub risk_classes: Vec<String>,
    #[serde(default)]
    pub reviewer_independence_rules: Vec<String>,
    #[serde(default)]
    pub elevated_operations: Vec<String>,
    pub adaptive_envelope: AdaptiveEnvelope,
    #[serde(default)]
    pub rollback_and_recovery: Vec<String>,
    #[serde(default)]
    pub exclusions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaselineRevision {
    pub id: String,
    pub baseline_id: String,
    pub project_id: String,
    pub revision_number: i64,
    #[serde(default)]
    pub base_revision_id: Option<String>,
    pub lifecycle: ExecutionBaselineLifecycle,
    pub schema_version: String,
    pub content: ExecutionBaselineContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
    pub created_at: String,
    #[serde(default)]
    pub activated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaseline {
    pub id: String,
    pub project_id: String,
    #[serde(default)]
    pub current_revision_id: Option<String>,
    pub lifecycle: ExecutionBaselineLifecycle,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaselineApproval {
    pub id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
    pub expected_project_version: i64,
    pub approved_by: PrincipalRef,
    pub authorization: AuthorizationProvenance,
    pub approved_at: String,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaselineResponse {
    pub baseline: ExecutionBaseline,
    #[serde(default)]
    pub current_revision: Option<ExecutionBaselineRevision>,
    /// A proposed/approved successor that is not authoritative until
    /// activation.  This is populated when an active baseline is being
    /// superseded so callers can approve and activate the exact revision
    /// without mistaking it for the currently runnable revision.
    #[serde(default)]
    pub proposed_revision: Option<ExecutionBaselineRevision>,
    #[serde(default)]
    pub approval: Option<ExecutionBaselineApproval>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct CreateExecutionBaselineRequest {
    pub mutation: MutationEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct SaveExecutionBaselineRevisionRequest {
    pub mutation: MutationEnvelope,
    pub base_revision_id: Option<String>,
    pub content: ExecutionBaselineContent,
    pub rendered_view: String,
    pub render_version: String,
    pub content_digest: String,
    pub render_digest: String,
    pub provenance: RevisionProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ApproveExecutionBaselineRequest {
    pub mutation: MutationEnvelope,
    pub revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
    /// Project version observed while the user reviewed this exact revision.
    /// Activation performs a second CAS against the then-current Project
    /// version; this value remains part of the durable approval receipt.
    pub expected_project_version: i64,
}

/// Server-checked provenance for a Project Task.
///
/// A Charter-backed implementation Task must carry this envelope when it is
/// created through a Project Agent action or the Task API.  The database keeps
/// the immutable copy in `project_task_governance`; this request type is only
/// the caller-facing input.  The server derives the final `runnable` value
/// from the active baseline and never trusts a caller-provided flag.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct TaskGovernanceRequest {
    #[serde(default)]
    pub charter_revision_id: Option<String>,
    #[serde(default)]
    pub baseline_id: Option<String>,
    #[serde(default)]
    pub baseline_revision_id: Option<String>,
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
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
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
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
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
    pub release_policy_revision: String,
    pub release_policy_digest: String,
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
    pub current_revision_id: String,
    pub current_digest: String,
    pub stale: bool,
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
    pub latest_readiness: Option<ReadinessSnapshot>,
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
    #[serde(default)]
    pub unresolved_decision_ids: Vec<String>,
    #[serde(default)]
    pub risks: Vec<CharterRisk>,
    #[serde(default)]
    pub document_freshness: Vec<DocumentFreshness>,
    #[serde(default)]
    pub evidence: Vec<EvidenceAttachment>,
    #[serde(default)]
    pub releases: Vec<ProjectRelease>,
    #[serde(default)]
    pub next_action: Option<String>,
    pub projection_state: OverviewProjectionState,
    pub source_event_watermark: String,
    pub generated_at: String,
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
    pub governing_baseline_revision_id: Option<String>,
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
pub struct ActivateExecutionBaselineRequest {
    pub mutation: MutationEnvelope,
    pub baseline_id: String,
    pub revision_id: String,
    pub approval_id: String,
    pub content_digest: String,
    pub render_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct EvaluateMilestoneReadinessRequest {
    pub mutation: MutationEnvelope,
    pub milestone_id: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub release_policy_revision: String,
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
            "baseline_id": "baseline-1",
            "unknown": "must fail"
        });
        assert!(serde_json::from_value::<TaskGovernanceRequest>(task_governance).is_err());

        let mut content = json!({
            "charter_revision": {
                "artifact_id": "charter-1",
                "revision_id": "revision-1",
                "content_digest": "content-1"
            },
            "milestone_definition_revision_ids": []
        });
        content["charter_revision"]["unexpected"] = json!("must fail");
        assert!(serde_json::from_value::<ExecutionBaselineContent>(content).is_err());
    }
}
