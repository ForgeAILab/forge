//! Main payload contracts. Proposal provenance remains in the host envelope.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectAgentSelection {
    pub genesis_session_id: Option<String>,
    #[schemars(range(min = 1))]
    pub expected_session_version: i64,
    #[schemars(length(min = 1))]
    pub project_agent_identity_id: String,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProjectCreate {
    // The id of the user's Charter approval. The agent is its only source: the
    // user executor reads it from the stored action payload and refuses a
    // missing, non-string or blank value, so a new call must carry it.
    // Decoding stays lenient because an action prepared before this contract
    // replays its exact stored arguments.
    #[schemars(with = "String", length(min = 1))]
    pub approval_id: Option<Value>,
    // The executor ignores these fields, including the former action discriminator.
    // Preserve their bytes for dedupe and approval while advertising only approval_id.
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[async_trait::async_trait]
pub trait MainProposalContext<E>: Send + Sync {
    async fn select_project_agent(&self, input: ProjectAgentSelection) -> Result<Value, E>;
    async fn propose_project_create(&self, input: ProjectCreate) -> Result<Value, E>;
}
pub type Catalog<E> = OperationCatalog<E>;
const SURFACES: &[SurfaceBinding] = &[
    SurfaceBinding {
        native_aggregate: "forge_main_orchestration_propose",
        projection: FieldProjection::ProposalPayload,
    },
    SurfaceBinding {
        native_aggregate: "forge_scope_propose",
        projection: FieldProjection::ProposalPayload,
    },
];
pub const IDS: &[&str] = &["genesis.project_agent.select", "project.create"];
pub fn catalog<E: Send + 'static>() -> Catalog<E> {
    OperationCatalog::new(
        vec![
            OperationSpec::typed(
                "genesis.project_agent.select",
                AuthorityRule {
                    principal: authority::PrincipalRule::MainOrInquiry,
                    permissions: &[
                        ("account", "propose_discovery"),
                        ("agent_chat", "propose_discovery"),
                    ],
                    binding: "account-owned active Main identity",
                },
                EffectClass::DirectCommand,
                AvailabilityRule::Always,
                SURFACES,
                "Persist the exact Genesis Project Agent preference.",
                "",
                &[
                    StructuralConstraint::IgnoredField("action"),
                    StructuralConstraint::ClosedObject,
                ],
                |context, input| Box::pin(context.select_project_agent(input)),
            ),
            OperationSpec::typed(
                "project.create",
                AuthorityRule {
                    principal: authority::PrincipalRule::MainOrInquiry,
                    permissions: &[
                        ("account", "propose_project"),
                        ("agent_chat", "propose_project"),
                        // Base permission family also maps Project; the Main
                        // binding rule still excludes Project callers.
                        ("project", "propose_project"),
                    ],
                    binding: "account-owned active Main identity; execution requires the user",
                },
                EffectClass::ApprovalRequired,
                AvailabilityRule::Always,
                SURFACES,
                "Propose Project creation from an exact Charter approval.",
                "",
                &[],
                |context, input| Box::pin(context.propose_project_create(input)),
            ),
        ],
        IDS,
    )
    .expect("complete Main proposal catalog")
}
pub static CATALOG: LazyLock<Catalog<std::convert::Infallible>> = LazyLock::new(catalog);
