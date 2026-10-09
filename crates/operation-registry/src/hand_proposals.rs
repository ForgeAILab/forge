//! Main direct-command contracts, isolated from MCP catalog additions.
pub mod charter;
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CharterDraft {
    pub genesis_session_id: Option<String>,
    #[schemars(length(min = 1))]
    pub charter_id: String,
    #[schemars(range(min = 1))]
    pub expected_charter_version: Option<i64>,
    pub base_revision_id: Option<String>,
    pub project_mode: charter::ProjectMode,
    pub maturity: charter::ProductMaturity,
    pub content: charter::ProjectCharterContent,
    pub change_summary: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<charter::ProvenanceRef>,
    pub provenance: charter::RevisionProvenance,
    pub rendered_view: Option<String>,
    pub render_version: Option<String>,
    pub content_digest: Option<String>,
    pub render_digest: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct GenesisStart {
    pub maturity: Option<charter::ProductMaturity>,
    pub preferred_project_agent_identity_id: Option<String>,
}
#[async_trait::async_trait]
pub trait HandProposalContext<E>: Send + Sync {
    async fn charter_draft(&self, input: CharterDraft) -> Result<Value, E>;
    async fn genesis_start(&self, input: GenesisStart) -> Result<Value, E>;
}
pub const IDS: &[&str] = &["charter.draft", "genesis.start"];
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::OwnedMain,
    permissions: &[
        ("account", "propose_discovery"),
        ("agent_chat", "propose_discovery"),
    ],
    binding:
        "account-owned identity; the command checks the current Main binding before a fresh effect",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_main_orchestration_propose",
    projection: FieldProjection::ProposalPayload,
}];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed(
            "charter.draft",
            AUTHORITY,
            EffectClass::DirectCommand,
            AvailabilityRule::Always,
            SURFACES,
            "Save a Main Charter revision; only the user may approve it.",
            "",
            &[
                StructuralConstraint::IgnoredField("action"),
                StructuralConstraint::ClosedObject,
                StructuralConstraint::MaxSerializedBytes(65536),
            ],
            |context, input| Box::pin(context.charter_draft(input)),
        ),
        OperationSpec::typed(
            "genesis.start",
            AUTHORITY,
            EffectClass::DirectCommand,
            AvailabilityRule::Always,
            SURFACES,
            "Start Product Genesis from the currently leased Main user request.",
            "",
            &[
                StructuralConstraint::IgnoredField("action"),
                StructuralConstraint::ClosedObject,
                StructuralConstraint::MaxSerializedBytes(65536),
            ],
            |context, input| Box::pin(context.genesis_start(input)),
        ),
    ]
}
