//! These operations record pending intent. There is no effect executor.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

// Intent values stay opaque: the old enqueuer did not interpret them. The
// payload was always an open, size-capped object stored verbatim, so the
// declared fields are hints and every other field is kept as recorded.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Message {
    pub body: Option<Value>,
    pub content: Option<Value>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Commitment {
    pub commitment_id: Option<Value>,
    pub status: Option<Value>,
    pub description: Option<Value>,
    pub content: Option<Value>,
    pub due_at: Option<Value>,
    pub expected_version: Option<Value>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Memory {
    pub parameters: Option<Value>,
    pub value: Option<Value>,
    pub memory_id: Option<Value>,
    pub title: Option<Value>,
    pub content: Option<Value>,
    pub description: Option<Value>,
    pub kind: Option<Value>,
    pub visibility: Option<Value>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Review {
    pub task_id: Option<Value>,
    pub content: Option<Value>,
    pub rationale: Option<Value>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Session {
    pub action: Option<Value>,
    pub session_id: Option<Value>,
    pub content: Option<Value>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub recorded_fields: BTreeMap<String, Value>,
}
#[async_trait::async_trait]
pub trait LegacyProposalContext<E>: Send + Sync {
    async fn pending_message(&self, input: Message) -> Result<Value, E>;
    async fn pending_commitment(&self, input: Commitment) -> Result<Value, E>;
    async fn pending_memory_publish(&self, input: Memory) -> Result<Value, E>;
    async fn pending_memory_supersede(&self, input: Memory) -> Result<Value, E>;
    async fn pending_review(&self, input: Review) -> Result<Value, E>;
    async fn pending_session(&self, input: Session) -> Result<Value, E>;
}
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_propose",
    projection: FieldProjection::ProposalPayload,
}];
fn authority(permission: &'static str) -> AuthorityRule {
    AuthorityRule {
        principal: if permission == "propose_message" {
            authority::PrincipalRule::ProjectOrOwnedIdentity
        } else {
            authority::PrincipalRule::ProjectAgent
        },
        permissions: match permission {
            "propose_message" => &[
                ("project", "propose_message"),
                ("agent_chat", "propose_message"),
                ("agent", "propose_message"),
            ],
            "propose_commitment" => &[
                ("project", "propose_commitment"),
                ("agent_chat", "propose_commitment"),
            ],
            "propose_memory" => &[
                ("project", "propose_memory"),
                ("agent_chat", "propose_memory"),
            ],
            "propose_session" => &[
                ("project", "propose_session"),
                ("agent_chat", "propose_session"),
            ],
            "propose_review" => &[("project", "propose_review")],
            _ => unreachable!("declared pending-proposal permission"),
        },
        binding: "bound Project Agent; pending intent only",
    }
}
pub const IDS: &[&str] = &[
    "message.send",
    "commitment.update",
    "memory.publish",
    "memory.supersede",
    "review.request",
    "session.action",
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    let open = &[StructuralConstraint::MaxSerializedBytes(65536)];
    vec![
        OperationSpec::typed(
            "message.send",
            authority("propose_message"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::Always,
            SURFACES,
            "PENDING message proposal; no message is sent.",
            "",
            open,
            |context, input| Box::pin(context.pending_message(input)),
        ),
        OperationSpec::typed(
            "commitment.update",
            authority("propose_commitment"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::ReadyOnly,
            SURFACES,
            "PENDING commitment proposal; no commitment is changed.",
            "",
            open,
            |context, input| Box::pin(context.pending_commitment(input)),
        ),
        OperationSpec::typed(
            "memory.publish",
            authority("propose_memory"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::ReadyOnly,
            SURFACES,
            "PENDING memory proposal; no memory is published.",
            "",
            open,
            |context, input| Box::pin(context.pending_memory_publish(input)),
        ),
        OperationSpec::typed(
            "memory.supersede",
            authority("propose_memory"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::ReadyOnly,
            SURFACES,
            "PENDING memory proposal; no memory is superseded.",
            "",
            open,
            |context, input| Box::pin(context.pending_memory_supersede(input)),
        ),
        OperationSpec::typed(
            "review.request",
            authority("propose_review"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::ReadyOnly,
            SURFACES,
            "PENDING review proposal; no review is dispatched.",
            "",
            open,
            |context, input| Box::pin(context.pending_review(input)),
        ),
        OperationSpec::typed(
            "session.action",
            authority("propose_session"),
            EffectClass::ApprovalRequired,
            AvailabilityRule::ReadyOnly,
            SURFACES,
            "PENDING session proposal; no session is cancelled or steered.",
            "",
            open,
            |context, input| Box::pin(context.pending_session(input)),
        ),
    ]
}
pub fn catalog<E: Send + 'static>() -> OperationCatalog<E> {
    OperationCatalog::new(specs(), IDS).expect("complete legacy proposal catalog")
}
pub static CATALOG: LazyLock<OperationCatalog<std::convert::Infallible>> = LazyLock::new(catalog);
