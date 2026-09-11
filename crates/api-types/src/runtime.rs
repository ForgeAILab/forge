use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorTypeDescriptor {
    #[serde(rename = "type")]
    pub type_name: String,
    pub display_name: String,
    pub config_schema: Value,
    pub default_config: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailabilityResponse {
    pub status: String,
    pub authenticated_at: Option<String>,
    pub config_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredOptionsResponse {
    pub models: Vec<String>,
    pub permission_policies: Vec<String>,
    pub cli_specific: Value,
    #[serde(default)]
    pub available_daemons: Vec<DiscoveredDaemonResponse>,
    #[serde(default)]
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredDaemonResponse {
    pub id: String,
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAvailabilityResponse {
    pub available: bool,
    pub effective_status: String,
    pub resolved_daemon_id: Option<String>,
    pub active_task_count: i64,
    pub max_concurrent_tasks: i64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkspaceResponse {
    pub id: String,
    pub task_id: String,
    pub repo_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub status: String,
    pub before_sha: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAgentRequest {
    pub name: String,
    pub description: Option<String>,
    pub executor_type: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities: Option<Vec<String>>,
    pub config_json: Option<Value>,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: Option<i64>,
    pub heartbeat_interval_seconds: Option<i64>,
    pub max_missed_heartbeats: Option<i64>,
    pub is_default: Option<bool>,
    /// Optional provider entry powering this harness agent. When set, Forge
    /// injects the credential at dispatch (`auth_source: forge_provider`);
    /// when absent the harness uses its own CLI-managed login.
    #[serde(default)]
    pub credential_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAgentRequest {
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<Option<String>>,
    #[serde(default)]
    pub model: Option<Option<String>>,
    #[serde(default)]
    pub reasoning_effort: Option<Option<String>>,
    #[serde(default)]
    pub permission_policy: Option<Option<String>>,
    #[serde(default)]
    pub prompt_template: Option<Option<String>>,
    pub capabilities: Option<Vec<String>>,
    pub config_json: Option<Value>,
    #[serde(default)]
    pub daemon_id: Option<Option<String>>,
    pub max_concurrent_tasks: Option<i64>,
    pub is_default: Option<bool>,
    pub paused: Option<bool>,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateAgentRequest {
    pub name: String,
}

/// Create a direct (embedded-runtime) agent referencing an existing provider
/// entry. Credentials are never part of this request.
#[derive(Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateEmbeddedAgentRequest {
    pub name: String,
    pub description: Option<String>,
    pub credential_id: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub system_prompt: Option<String>,
    #[ts(type = "Record<string, unknown> | null")]
    pub account_permission_ceiling: Option<Value>,
    #[ts(type = "Record<string, unknown> | null")]
    pub tool_policy: Option<Value>,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

/// Publish a replacement profile for an existing embedded identity,
/// referencing an existing provider entry.
#[derive(Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConnectEmbeddedProfileRequest {
    #[ts(type = "number")]
    pub version: i64,
    pub credential_id: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub system_prompt: Option<String>,
    pub permission_policy: Option<String>,
    #[ts(type = "Record<string, unknown> | null")]
    pub tool_policy: Option<Value>,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum CanonicalScopeRequest {
    Account,
    Project { project_id: String },
    AgentChat { chat_id: String },
    Task { task_id: String, role: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateAgentSessionRequest {
    pub profile_id: Option<String>,
    pub scope: CanonicalScopeRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SessionVersionRequest {
    #[ts(type = "number")]
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SteerAgentSessionRequest {
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CredentialHandleResponse {
    pub id: String,
    pub provider: String,
    pub label: String,
    pub credential_method: String,
    pub status: String,
    #[ts(type = "number")]
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProviderRevocationStatus {
    NotSupported,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct DisconnectCredentialResponse {
    pub id: String,
    pub status: String,
    pub provider_revocation: ProviderRevocationStatus,
    /// Agents that referenced the removed entry and are now visibly
    /// unhealthy. They are never silently rebound or deleted.
    pub affected_agents: Vec<crate::ProviderEntryAgentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentProfileResponse {
    pub id: String,
    pub identity_id: String,
    pub backend_kind: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub system_prompt: Option<String>,
    #[ts(type = "Record<string, unknown>")]
    pub capabilities: Value,
    #[ts(type = "Record<string, unknown>")]
    pub tool_policy: Value,
    #[ts(type = "Record<string, unknown>")]
    pub config: Value,
    pub credential_handle_id: Option<String>,
    pub version: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentConnectionHealthResponse {
    pub profile_id: String,
    pub status: String,
    #[ts(type = "Record<string, unknown>")]
    pub capabilities: Value,
    pub checked_at: Option<String>,
    pub error_code: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentSessionResponse {
    pub id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub context_scope_id: String,
    pub backend_kind: String,
    pub status: String,
    #[ts(type = "Record<string, unknown>")]
    pub capabilities: Value,
    pub connection_status: String,
    pub predecessor_session_id: Option<String>,
    pub replaced_by_session_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Redaction-safe metadata for a pending native runtime interaction.  The
/// questionnaire and any answer remain encrypted in the protected runtime
/// store; this type intentionally contains no question/answer bodies.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProtectedInteractionSummaryResponse {
    pub id: String,
    pub session_id: String,
    pub interaction_kind: String,
    pub prompt_redacted: String,
    pub status: String,
    pub expires_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProtectedInteractionAnswerRequest {
    pub expected_version: i64,
    pub values: Vec<ProtectedInteractionAnswerValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(export)]
pub enum ProtectedInteractionAnswerValue {
    Choice {
        question_id: String,
        choice_id: String,
    },
    FreeForm {
        question_id: String,
        value: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct ProtectedInteractionCancelRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConnectedEmbeddedAgentResponse {
    pub agent: crate::AgentResponse,
    pub credential_handle: CredentialHandleResponse,
    pub profile: AgentProfileResponse,
    pub health: AgentConnectionHealthResponse,
    pub session: AgentSessionResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ConnectedEmbeddedProfileResponse {
    pub agent: crate::AgentResponse,
    pub profile: AgentProfileResponse,
    pub credential_handle: CredentialHandleResponse,
    pub health: AgentConnectionHealthResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct EffectivePermissionsResponse {
    pub allowed: Vec<String>,
    pub denied: Vec<String>,
    pub requires_approval: Vec<String>,
}
