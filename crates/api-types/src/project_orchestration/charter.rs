//! Project Charter content, readiness, approval, and amendment contracts.

use super::*;

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

/// The repository scaffold the Project starts from: a spark template plus the
/// feature packs installed on top of it. Absent means the repository starts
/// empty (a README only) — non-web products, or a repository the user brings.
/// The block is a material technology constraint: once approved it changes
/// only through a Charter amendment.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CharterScaffold {
    /// spark template id, for example `nextjs` or `vite-react`.
    pub template: String,
    /// spark pack ids installed after scaffolding; empty means no packs.
    #[serde(default)]
    pub packs: Vec<String>,
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
    /// Omitted from canonical JSON when absent so Charters that predate the
    /// block keep their content digests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub scaffold: Option<CharterScaffold>,
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
