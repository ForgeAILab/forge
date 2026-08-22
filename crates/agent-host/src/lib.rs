#![forbid(unsafe_code)]

mod interaction;
mod lcm;
mod manifest;
mod native;
pub mod operation_catalog;
pub(crate) mod operation_contract;
mod protected_store;
mod transport;
mod typed_tools;

use std::{collections::BTreeSet, fmt, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

pub use agent_runtime::context::Sensitivity;
pub use agent_runtime::core::clock::Deadline;
pub use agent_runtime::core::content::{Message, Role};
pub use agent_runtime::core::guard::ContentGuardRevision;
pub use agent_runtime::core::ids::{
    ChoiceId, InteractionRequestId, QuestionId, SessionId, ToolCallId, TurnId,
};
pub use agent_runtime::core::interaction::{
    Choice, InteractionBroker, InteractionOrigin, InteractionOutcomeKind, InteractionRequest,
    InteractionSensitivity, Question, Questionnaire,
};
pub use agent_runtime::core::store::Secret;
pub use agent_runtime::lcm::{LcmClassification, LcmSourceMetadata};
pub use agent_runtime::registry::{RegistryRevision, TrustClass};
pub use interaction::{
    InteractionAnswer, InteractionAnswerValue, InteractionBrokerHandle, ProtectedInteractionSummary,
};
pub use lcm::{
    DeterministicLcmSummaryModel, FORGE_LCM_STORE_REVISION, FORGE_TASK_LCM_PROJECTION_REVISION,
    SqliteLcmStore, TaskLcmProjectionPolicy, TaskRuntimeLcmRecord,
};
pub use manifest::{
    RuntimeClassificationLink, RuntimeContextManifestLink, RuntimeContextSegmentLink,
    RuntimeLosslessSummaryLink, RuntimeSummaryCoverageLink,
};
pub use native::NativeAgentRuntimeBackend;
pub use operation_catalog::{
    MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READ_OPERATION, MAIN_CHARTER_READINESS_OPERATION,
    MAIN_PROJECT_CREATE_OPERATION, MIGRATED_OPERATION_CONTRACTS, OperationClassification,
    OperationContract, OperationDescriptor, OperationExposure, OperationInputContract,
    OperationOutputContract, OperationPermission, OperationSetupExposure, OperationSurface,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION, PROJECT_EVIDENCE_OPERATION,
    PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION,
    PROJECT_RELEASE_OPERATION, SHARED_ORCHESTRATION_OUTCOME, TASK_ADAPTIVE_OPERATION,
    TASK_PROPOSE_OPERATION, classify_operation, contains_adaptive_authority_override,
    contains_authority_override, descriptor as operation_descriptor,
    is_allowed_project_direct_payload, is_approval_required_operation, is_denied_operation,
    is_project_orchestration_operation, is_query_operation, operation_contract,
    operation_contract_permission, operation_names_for_surface, operation_permission,
    operation_supported_in_scope,
};
pub use protected_store::{
    CreateOAuthCredential, CredentialRevocationOutcome, OAuthCredentialBundle,
    SqliteProtectedRuntimeStore,
};
pub use typed_tools::{
    FORGE_MAIN_ORCHESTRATION_PROPOSE_TOOL, FORGE_MAIN_ORCHESTRATION_READ_TOOL,
    FORGE_PROJECT_ORCHESTRATION_PROPOSE_TOOL, FORGE_PROJECT_ORCHESTRATION_READ_TOOL,
    FORGE_PUBLIC_WEB_SEARCH_TOOL, FORGE_SCOPE_PROPOSE_PERMISSION, FORGE_SCOPE_READ_PERMISSION,
    ForgeToolProvider, ProjectChatToolContext, PublicSearchScope, ScopeToolComposition,
    TaskToolRole,
};

/// The immutable revision Forge is built and tested against.
pub const AGENT_RUNTIME_REVISION: &str = "b3f966b0e108e6d4683c0a9c94055aaa6aa7d919";
pub const AGENT_RUNTIME_MINIMUM_RUST: &str = "1.86";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalScopeType {
    Account,
    Project,
    AgentChat,
    Task,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalScope {
    pub scope_type: CanonicalScopeType,
    pub scope_id: String,
    pub workspace_access: WorkspaceAccess,
}

/// The immutable scope binding loaded from a persisted Forge session.
///
/// `AgentTurnRequest.scope` remains a transport value for the backend API, but
/// native execution must compare it with this server-loaded binding before
/// composing tools.  In particular, an arbitrary caller cannot select a
/// different identity, Task role, or worktree by changing request JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeScopeBinding {
    pub identity_id: String,
    pub scope: CanonicalScope,
    pub task_role: Option<String>,
    pub workspace_path: Option<String>,
    /// Server-derived owning Project for a Project Agent Chat.  Main Chat and
    /// malformed/legacy chat scopes remain `None`; this is intentionally not
    /// inferred from the opaque canonical scope id or turn input.
    pub agent_chat_project_id: Option<String>,
    /// Server-derived Project Charter setup state. Native composition uses
    /// this to expose only the legacy adoption surface until user approval
    /// commits a Charter; it is never supplied by model input.
    pub project_charter_setup_required: bool,
    /// Effective Forge permission names after intersecting the persisted
    /// identity, profile, membership, and canonical-scope ceilings.  Native
    /// tool registration must use this set as a second boundary; it is never
    /// supplied by the turn request or model input.
    pub allowed_permissions: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccess {
    Deny,
    TaskRead,
    TaskWrite,
}

impl CanonicalScope {
    pub fn validate(&self) -> Result<(), AgentHostError> {
        match (self.scope_type, self.workspace_access) {
            (CanonicalScopeType::Task, WorkspaceAccess::TaskRead | WorkspaceAccess::TaskWrite)
            | (
                CanonicalScopeType::Account
                | CanonicalScopeType::Project
                | CanonicalScopeType::AgentChat,
                WorkspaceAccess::Deny,
            ) => Ok(()),
            _ => Err(AgentHostError::Authority(
                "filesystem access is only valid for an admitted Task scope".to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub native_runtime: bool,
    pub persistent_session: bool,
    pub protected_checkpoints: bool,
    pub lcm: bool,
    pub cancel: bool,
    pub steer: bool,
    pub workspace: WorkspaceAccess,
}

#[derive(Clone)]
pub struct NativeProviderConfig {
    pub provider: String,
    pub base_url: String,
    pub model: String,
    pub credential_handle_id: String,
    pub owner_user_id: String,
    /// The provider-side account the credential belongs to (for OAuth
    /// entries). The ChatGPT Codex backend requires it as the
    /// `chatgpt-account-id` request header.
    pub provider_account_id: Option<String>,
    pub context_tokens: u32,
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
}

impl fmt::Debug for NativeProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeProviderConfig")
            .field("provider", &self.provider)
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("credential", &"[protected handle]")
            .field("context_tokens", &self.context_tokens)
            .field("max_input_tokens", &self.max_input_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct AgentTurnRequest {
    pub forge_session_id: String,
    pub runtime_session_id: String,
    pub scope: CanonicalScope,
    /// The host-issued Task worktree root.  Non-Task scopes must leave this
    /// unset; the native host validates that invariant before composing the
    /// runtime so a caller cannot smuggle a repository path into an account,
    /// Project, or Agent Chat session.
    pub workspace_path: Option<String>,
    pub provider: NativeProviderConfig,
    pub system_prompt: Option<String>,
    pub history: Vec<Message>,
    pub input: String,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnOutput {
    pub runtime_session_id: String,
    pub text: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Final Agent Runtime context/LCM metadata. Bodies and protected state
    /// are intentionally absent; Forge links this to its domain manifest.
    pub context_manifest: Option<RuntimeContextManifestLink>,
    pub pending_interaction_id: Option<String>,
}

#[async_trait]
pub trait TurnEventSink: Send + Sync + fmt::Debug {
    async fn text_delta(&self, _text: &str) {}

    /// A reasoning fragment. `redacted` fragments carry no readable text and
    /// exist only as liveness/progress signals.
    async fn reasoning_delta(&self, _text: &str, _redacted: bool) {}

    /// A validated tool call left the model. Argument values are withheld by
    /// the runtime; only the top-level key names are visible.
    async fn tool_call_started(&self, _call_id: &str, _name: &str, _argument_keys: &[String]) {}

    async fn tool_call_finished(&self, _call_id: &str, _name: &str, _is_error: bool) {}
}

#[derive(Debug, Default)]
pub struct NoopTurnEventSink;

#[async_trait]
impl TurnEventSink for NoopTurnEventSink {}

#[async_trait]
pub trait AgentSessionBackend: Send + Sync + fmt::Debug {
    fn capabilities(&self, scope: &CanonicalScope) -> BackendCapabilities;

    async fn run_turn(
        &self,
        request: AgentTurnRequest,
        sink: Arc<dyn TurnEventSink>,
    ) -> Result<AgentTurnOutput, AgentHostError>;

    async fn cancel(&self, runtime_session_id: &str) -> Result<(), AgentHostError>;

    async fn steer(&self, runtime_session_id: &str, content: String) -> Result<(), AgentHostError>;
}

#[derive(Debug, thiserror::Error)]
pub enum AgentHostError {
    #[error("runtime configuration error: {0}")]
    Configuration(String),
    #[error("runtime authority denied: {0}")]
    Authority(String),
    #[error("runtime session not found")]
    SessionNotFound,
    #[error("credential handle not found")]
    CredentialNotFound,
    #[error("runtime version conflict")]
    VersionConflict,
    #[error("runtime operation unsupported: {0}")]
    Unsupported(String),
    /// A provider-side Forge command/query result that is already safe and
    /// structured for model-facing transport.  Native typed tools must return
    /// this as an in-band, `is_error` tool outcome rather than flattening it
    /// into `RuntimeError` prose.
    #[error("structured Forge outcome")]
    StructuredOutcome(Box<api_types::OrchestrationOutcome>),
    #[error("runtime failed: {0}")]
    Runtime(String),
    #[error("protected persistence failed")]
    ProtectedPersistence,
}
