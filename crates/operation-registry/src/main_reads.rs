//! Account-owned Main Genesis, Charter, portfolio and bounded inquiry queries.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GenesisProjectAgentsQuery {
    pub genesis_session_id: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterReadQuery {
    pub charter_id: Option<String>,
    pub revision_id: Option<String>,
    pub genesis_session_id: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterProjectionQuery {
    pub genesis_session_id: Option<String>,
    #[schemars(length(min = 1))]
    pub charter_id: String,
    #[schemars(length(min = 1))]
    pub revision_id: String,
    #[schemars(length(min = 1))]
    pub content_digest: String,
    #[schemars(length(min = 1))]
    pub render_digest: String,
    #[schemars(range(min = 1))]
    pub expected_charter_version: i64,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CharterDiffQuery {
    pub genesis_session_id: Option<String>,
    #[schemars(length(min = 1))]
    pub charter_id: String,
    #[schemars(length(min = 1))]
    pub base_revision_id: String,
    #[schemars(length(min = 1))]
    pub candidate_revision_id: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BoundedListQuery {
    // The handlers retain their defaults and clamping, including 0 and >20.
    pub limit: Option<u64>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InquiryQuery {
    #[schemars(length(min = 1, max = 120))]
    pub title: String,
    #[schemars(length(min = 1, max = 4000))]
    pub question: String,
    #[schemars(length(max = 8000))]
    pub context: Option<String>,
}

#[async_trait::async_trait]
pub trait MainReadContext<E>: Send + Sync {
    async fn genesis_project_agents(&self, input: GenesisProjectAgentsQuery) -> Result<Value, E>;
    async fn charter_read(&self, input: CharterReadQuery) -> Result<Value, E>;
    async fn charter_readiness(&self, input: CharterProjectionQuery) -> Result<Value, E>;
    async fn charter_diff(&self, input: CharterDiffQuery) -> Result<Value, E>;
    async fn charter_approval_target(&self, input: CharterProjectionQuery) -> Result<Value, E>;
    async fn discovery_read(&self, input: BoundedListQuery) -> Result<Value, E>;
    async fn portfolio_read(&self, input: BoundedListQuery) -> Result<Value, E>;
    async fn inquiry_run(&self, input: InquiryQuery) -> Result<Value, E>;
}
const AUTHORITY: AuthorityRule = AuthorityRule {
    permissions: &[
        ("account", "read_account"),
        ("agent_chat", "read_agent_chat"),
    ],
    binding: "account-owned Main Agent or account inquiry",
};
const MAIN: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_main_orchestration_read",
    projection: FieldProjection::ReadArguments,
}];
const SCOPE: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_read",
    projection: FieldProjection::ReadArguments,
}];
pub const IDS: &[&str] = &[
    "genesis.project_agents.read",
    "charter.read",
    "charter.readiness",
    "charter.diff",
    "charter.approval_target",
    "discovery.read",
    "portfolio.read",
    "inquiry.run",
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed("genesis.project_agents.read", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, MAIN,
            "Read eligible Project Agents and the Genesis selection.",
            "List the exact account-owned Project Agent identities eligible for explicit selection in the active Product Genesis session, plus the currently persisted preference and resolved approval selection.",
            &[StructuralConstraint::ClosedObject], |context, input| Box::pin(context.genesis_project_agents(input))),
        OperationSpec::typed("charter.read", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, MAIN,
            "Read Genesis-owned Charter state.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.charter_read(input))),
        OperationSpec::typed("charter.readiness", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, MAIN,
            "Evaluate an exact Charter revision's readiness.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.charter_readiness(input))),
        OperationSpec::typed("charter.diff", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, MAIN,
            "Compare two Genesis-owned Charter revisions.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.charter_diff(input))),
        OperationSpec::typed("charter.approval_target", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, MAIN,
            "Read an exact Charter approval target.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.charter_approval_target(input))),
        OperationSpec::typed("discovery.read", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, SCOPE,
            "Read bounded account-owned Genesis sessions.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.discovery_read(input))),
        OperationSpec::typed("portfolio.read", AUTHORITY, EffectClass::Query, AvailabilityRule::Always, SCOPE,
            "Read the bounded account-owned portfolio.", "", &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.portfolio_read(input))),
        OperationSpec::typed("inquiry.run", AuthorityRule {
            permissions: &[("account", "propose_discovery"), ("agent_chat", "propose_discovery")],
            binding: "bound Main Chat with inquiry runner; unavailable to account inquiry sessions",
        }, EffectClass::Query, AvailabilityRule::MainChatOnly, MAIN,
            "Run one bounded read-only inquiry and return its findings.",
            "Dispatch one ephemeral read-only sub-agent to answer a bounded question, and wait for its findings. Use this for a research excursion whose working material you do not want to carry for the rest of this conversation -- reading across many Projects, reconciling a long event history, comparing options. The sub-agent gets the account read surface and its own scratch directory; it cannot propose anything, touch a repository, or dispatch further sub-agents. `title` is what the user sees in the inquiry list while it runs, so name the question, not the activity. `question` is the sub-agent's entire brief: it does not see this conversation, so state everything it needs. Put supporting material in `context`. You get back a bounded abstract plus the path to the sub-agent's full findings file, which you can read with the file tools if the abstract is not enough.",
            &[StructuralConstraint::ClosedObject], |context, input| Box::pin(context.inquiry_run(input))),
    ]
}
