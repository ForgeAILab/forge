//! Project command payloads. The host envelope retains exact command bytes;
//! typed inputs select the handler, while domain services own receipts and CAS.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SetCiSteps {
    SetCiSteps,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReviewConfig {
    pub action: SetCiSteps,
    #[schemars(range(min = 1))]
    pub expected_project_version: i64,
    #[schemars(length(max = 16))]
    pub ci_steps: Vec<CommandLine>,
    #[schemars(length(max = 16))]
    pub setup_steps: Option<Vec<CommandLine>>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CommandLine(#[schemars(length(min = 1, max = 2048))] pub String);
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    Research,
    DeliveryBrief,
    ProductSpec,
    Design,
    Architecture,
    ExecutionPlan,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Document {
    DraftRevision {
        document_id: String,
        kind: DocumentKind,
        title: String,
        #[schemars(with = "BTreeMap<String, Value>")]
        content: Value,
        #[schemars(range(min = 1))]
        #[schemars(with = "Option<i64>")]
        expected_document_version: Option<Value>,
        #[schemars(with = "Option<String>")]
        base_revision_id: Option<Value>,
        #[schemars(with = "Option<String>")]
        expected_digest: Option<Value>,
    },
    ProposeApproval {
        document_id: String,
        kind: DocumentKind,
        title: String,
        #[schemars(with = "BTreeMap<String, Value>")]
        content: Value,
        #[schemars(range(min = 1))]
        #[schemars(with = "Option<i64>")]
        expected_document_version: Option<Value>,
        #[schemars(with = "Option<String>")]
        base_revision_id: Option<Value>,
        #[schemars(with = "Option<String>")]
        expected_digest: Option<Value>,
    },
    Approve {
        document_id: String,
        revision_id: String,
        content_digest: String,
        render_digest: String,
        #[schemars(range(min = 1))]
        expected_document_version: i64,
    },
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAction {
    RecordCandidate,
    RecordEffective,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationClass {
    ProjectImplementation,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Decision {
    pub action: DecisionAction,
    pub decision_class: ImplementationClass,
    pub question: String,
    #[serde(default)]
    pub options: Vec<String>,
    #[schemars(with = "Option<String>")]
    pub selected_outcome: Option<Value>,
    #[schemars(with = "Option<String>")]
    pub rationale: Option<Value>,
    #[schemars(with = "Option<String>")]
    pub decision_id: Option<Value>,
    #[schemars(range(min = 1))]
    pub expected_project_version: i64,
    #[serde(default)]
    pub affected_artifact_refs: Vec<Value>,
    #[serde(default)]
    pub affected_task_ids: Vec<String>,
    #[serde(default)]
    pub affected_milestone_ids: Vec<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionLifecycle {
    Draft,
    Proposed,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Milestone {
    Define {
        content: MilestoneContent,
        #[schemars(with = "Option<String>")]
        display_label: Option<Value>,
        lifecycle: Option<DefinitionLifecycle>,
        provenance: Option<Value>,
        #[schemars(range(min = 0))]
        #[schemars(with = "Option<i64>")]
        expected_milestone_version: Option<Value>,
    },
    Revise {
        milestone_id: String,
        content: MilestoneContent,
        #[schemars(with = "Option<String>")]
        display_label: Option<Value>,
        lifecycle: Option<DefinitionLifecycle>,
        provenance: Option<Value>,
        #[schemars(with = "Option<String>")]
        base_revision_id: Option<Value>,
        #[schemars(range(min = 1))]
        expected_milestone_version: i64,
    },
    SetPrimary {
        #[schemars(with = "Option<String>")]
        primary_milestone_id: Option<Value>,
        #[schemars(range(min = 1))]
        expected_milestone_version: i64,
    },
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Record {
    Record,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Pass,
    Fail,
    Blocked,
    Stale,
    Unavailable,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Validation {
    pub action: Record,
    pub milestone_id: String,
    #[schemars(range(min = 1))]
    pub milestone_version: Option<i64>,
    #[schemars(range(min = 1))]
    #[schemars(with = "Option<i64>")]
    pub expected_milestone_version: Option<Value>,
    pub check_id: String,
    pub definition_revision_id: String,
    pub status: ValidationStatus,
    pub result: String,
    pub input_digest: String,
    #[serde(default)]
    pub observed_command_ids: Option<Vec<String>>,
    #[schemars(with = "Option<String>")]
    pub observed_task_id: Option<Value>,
    #[schemars(with = "Option<String>")]
    pub evidence_asset_id: Option<Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProposeCandidate {
    ProposeCandidate,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReleaseRequest {
    pub action: ProposeCandidate,
    pub milestone_id: String,
    #[schemars(range(min = 1))]
    pub milestone_version: i64,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Escalation {
    #[schemars(length(min = 1, max = 4096))]
    pub need: String,
    #[serde(default)]
    #[schemars(length(max = 100))]
    pub task_ids: Vec<NonEmptyId>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct NonEmptyId(#[schemars(length(min = 1))] pub String);

#[async_trait::async_trait]
pub trait ProjectProposalContext<E>: Send + Sync {
    async fn review_config(&self, input: ReviewConfig) -> Result<Value, E>;
    async fn document(&self, input: Document) -> Result<Value, E>;
    async fn decision(&self, input: Decision) -> Result<Value, E>;
    async fn milestone(&self, input: Milestone) -> Result<Value, E>;
    async fn validation(&self, input: Validation) -> Result<Value, E>;
    async fn release_request(&self, input: ReleaseRequest) -> Result<Value, E>;
    async fn escalate(&self, input: Escalation) -> Result<Value, E>;
}
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::ProjectAgent,
    permissions: &[
        ("project", "propose_project"),
        ("agent_chat", "propose_project"),
    ],
    binding: "active bound Project Agent; approvals remain user-only",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_project_orchestration_propose",
    projection: FieldProjection::ProposalPayload,
}];
pub const IDS: &[&str] = &[
    "project.review_config",
    "project.document",
    "project.decision",
    "project.milestone",
    "project.validation",
    "project.escalate",
    "project.release.request",
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    let closed = &[
        StructuralConstraint::ClosedObject,
        StructuralConstraint::MaxSerializedBytes(65536),
    ];
    vec![
        OperationSpec::typed("project.review_config", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Set independent review commands for subsequent Tasks.", "",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::MaxSerializedBytes(65536), StructuralConstraint::NonNullableOptional("setup_steps"), StructuralConstraint::UniqueItems("ci_steps"), StructuralConstraint::UniqueItems("setup_steps")],
            |context, input| Box::pin(context.review_config(input))),
        OperationSpec::typed("project.document", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Draft, propose or policy-authorize a bound Project Document.", "", closed,
            |context, input| Box::pin(context.document(input))),
        OperationSpec::typed("project.decision", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Record implementation decisions within the approved Charter.", "User-scope decisions, policy decisions, waivers and manual approvals remain user-only.", closed,
            |context, input| Box::pin(context.decision(input))),
        OperationSpec::typed("project.milestone", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Define, revise or select a bound Project milestone.", "", closed,
            |context, input| Box::pin(context.milestone(input))),
        OperationSpec::typed("project.validation", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Record disposable-checkout observations; pass/fail require observed commands, manual attestation is user-only.", "",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::MaxSerializedBytes(65536), StructuralConstraint::AtLeastOne(&["milestone_version","expected_milestone_version"])],
            |context, input| Box::pin(context.validation(input))),
        OperationSpec::typed("project.release.request", AUTHORITY, EffectClass::ApprovalRequired, AvailabilityRule::ReadyOnly, SURFACES,
            "Queue an exact release candidate proposal; final release is user-only.", "Project Agent release candidate only. Invoke this only for an exact current ReadinessSnapshot whose result is ready. A blocked, failed, or stale snapshot must be reported with every canonical blocker and must never be described as a release proposal or as Known Issues: None. This submits a user-release request; it never approves, executes, or creates a final release manifest.", closed,
            |context, input| Box::pin(context.release_request(input))),
        OperationSpec::typed("project.escalate", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Ask the owner for the need blocking named Tasks.", "Ask the Project owner for the exact need blocking named Tasks; creates one Notification and Attention item.", closed,
            |context, input| Box::pin(context.escalate(input))),
    ]
}
pub fn catalog<E: Send + 'static>() -> OperationCatalog<E> {
    OperationCatalog::new(specs(), IDS).expect("complete Project proposal catalog")
}
pub static CATALOG: LazyLock<OperationCatalog<std::convert::Infallible>> = LazyLock::new(catalog);

/// The content consumed by the existing milestone writer. Source kinds with
/// an authoritative result path remain the only advertised check kinds.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MilestoneContent {
    #[schemars(length(min = 1))]
    pub name: String,
    #[schemars(length(min = 1))]
    pub outcome: String,
    #[serde(default)]
    pub included_scope: Vec<String>,
    #[serde(default)]
    pub excluded_scope: Vec<String>,
    pub charter_revision: Option<ArtifactReference>,
    #[serde(default)]
    pub document_revisions: Vec<ArtifactReference>,
    #[serde(default)]
    pub task_ids: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub risks: Vec<Risk>,
    #[serde(default)]
    pub acceptance_checks: Vec<AcceptanceCheck>,
    #[serde(default)]
    pub evidence_requirements: Vec<EvidenceRequirement>,
    #[serde(default)]
    pub known_issues: Vec<String>,
    pub target_date: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    pub artifact_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub render_version: Option<String>,
    pub render_digest: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Risk {
    pub id: String,
    pub description: String,
    pub impact: Option<String>,
    pub treatment: Option<String>,
    pub revisit_trigger: Option<String>,
    pub owner: Option<Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckSource {
    Manual,
    PolicyWaiver,
    TaskValidation,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Pass,
    Fail,
    Pending,
    Blocked,
    Stale,
    Unavailable,
    Waived,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceCheck {
    pub id: String,
    pub description: String,
    pub required: bool,
    pub source_kind: CheckSource,
    pub expected_result: String,
    pub latest_result: Option<CheckStatus>,
    pub latest_result_id: Option<String>,
    pub latest_result_digest: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Screenshot,
    WalkthroughVideo,
    Log,
    Report,
    Other,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRequirement {
    pub id: String,
    pub description: String,
    pub required: bool,
    pub evidence_kind: Option<EvidenceKind>,
}
