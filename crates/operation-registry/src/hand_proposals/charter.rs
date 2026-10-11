//! Typed Charter wire inputs. Domain rendering, digests and approval remain in services.

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProductMaturity {
    Prototype,
    Mvp,
    Production,
    Critical,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    Agent,
    Worker,
    Reviewer,
    Service,
    System,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrincipalRef {
    pub kind: PrincipalKind,
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectMode {
    Compact,
    Standard,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CharterKnowledgeKind {
    ObservedFact,
    UserDecision,
    ResearchFinding,
    Assumption,
    Hypothesis,
    OpenDecision,
    ResearchQueue,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CharterConfidence {
    Low,
    Medium,
    High,
    NotApplicable,
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterCoreExperience {
    pub primary_outcome: String,
    #[serde(default)]
    pub core_loop: Option<String>,
    #[serde(default)]
    pub principal_journeys: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterHandoffNote {
    #[serde(default)]
    pub recommended_first_action: Option<String>,
    #[serde(default)]
    pub bounded_summary: Option<String>,
    #[serde(default)]
    pub unresolved_item_ids: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterKnowledgeLedger {
    #[serde(default)]
    pub items: Vec<CharterKnowledgeItem>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterScaffold {
    /// spark template id, for example `nextjs` or `vite-react`.
    pub template: String,
    /// spark pack ids installed after scaffolding; empty means no packs.
    #[serde(default)]
    pub packs: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
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
    #[serde(default)]
    pub scaffold: Option<CharterScaffold>,
    #[serde(default)]
    pub handoff_note: Option<CharterHandoffNote>,
}
