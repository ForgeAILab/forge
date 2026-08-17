use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

/// The durable binding state is intentionally separate from identity status.
/// A connected identity is not implicitly a Main or Project Agent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentBindingState {
    Active,
    #[serde(rename = "setup_required")]
    SetupRequired,
    Paused,
    Replaced,
    Revoked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatKind {
    Main,
    Project,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatStatus {
    Ready,
    SetupRequired,
    Archived,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatMessageAuthorType {
    User,
    Agent,
    Handoff,
    System,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatMessageStatus {
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentChatTurnStatus {
    Queued,
    Leased,
    AwaitingInput,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AgentHandoffStatus {
    Pending,
    Delivered,
    Failed,
    Cancelled,
}

/// The account's one active Main Agent binding.  The chat is owned by the
/// account and therefore remains stable when this binding is replaced.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MainAgentBindingResponse {
    pub id: String,
    pub account_id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub chat_id: String,
    pub state: AgentBindingState,
    #[ts(type = "Record<string, unknown>")]
    pub autonomy_policy: Value,
    pub tool_policy_revision: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// The one active Project Agent binding for an operational Project.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct ProjectAgentBindingResponse {
    pub id: String,
    pub project_id: String,
    pub identity_id: Option<String>,
    pub profile_id: Option<String>,
    pub chat_id: String,
    pub state: AgentBindingState,
    #[ts(type = "Record<string, unknown>")]
    pub permission_ceiling: Value,
    #[ts(type = "Record<string, unknown>")]
    pub autonomy_policy: Value,
    pub subscriptions: Vec<String>,
    pub wake_budget: i64,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Binds one agent to the account's Main scope. The binding follows the
/// agent's current settings; the stored profile snapshot is server-resolved.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct SetMainAgentBindingRequest {
    pub identity_id: String,
    pub expected_version: i64,
    #[serde(default)]
    #[ts(type = "Record<string, unknown>")]
    pub autonomy_policy: Value,
}

/// Binds one agent to a Project scope. The binding follows the agent's
/// current settings; the stored profile snapshot is server-resolved.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct SetProjectAgentBindingRequest {
    pub identity_id: String,
    pub expected_version: i64,
    #[serde(default)]
    #[ts(type = "Record<string, unknown>")]
    pub permission_ceiling: Value,
    #[serde(default)]
    #[ts(type = "Record<string, unknown>")]
    pub autonomy_policy: Value,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub wake_budget: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatResponse {
    pub id: String,
    pub kind: AgentChatKind,
    pub account_id: String,
    pub project_id: Option<String>,
    pub title: String,
    pub status: AgentChatStatus,
    pub message_count: i64,
    pub pending_turn_count: i64,
    pub last_message_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatDetailResponse {
    pub chat: AgentChatResponse,
    pub main_binding: Option<MainAgentBindingResponse>,
    pub project_binding: Option<ProjectAgentBindingResponse>,
}

/// A bounded navigation item.  It contains stable labels and state only; it
/// never carries chat bodies or private Project context.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatSwitcherItem {
    pub chat_id: String,
    pub kind: AgentChatKind,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub identity_id: Option<String>,
    pub identity_name: Option<String>,
    pub binding_state: AgentBindingState,
    pub chat_status: AgentChatStatus,
    pub unread_count: i64,
    pub pending_turn_count: i64,
    pub last_message_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatSwitcherResponse {
    pub items: Vec<AgentChatSwitcherItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatMessageResponse {
    pub id: String,
    pub chat_id: String,
    pub author_type: AgentChatMessageAuthorType,
    pub author_id: Option<String>,
    pub content: String,
    #[ts(type = "Record<string, unknown>")]
    pub content_guard: Value,
    pub sensitivity: String,
    pub status: AgentChatMessageStatus,
    pub outcome: Option<String>,
    pub model: Option<String>,
    pub profile_id: Option<String>,
    pub session_id: Option<String>,
    pub context_manifest_id: Option<String>,
    #[ts(type = "Record<string, unknown> | null")]
    pub token_usage_json: Option<Value>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub handoff_id: Option<String>,
    pub source_chat_id: Option<String>,
    pub source_message_id: Option<String>,
    pub sequence: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatTurnJobResponse {
    pub id: String,
    pub chat_id: String,
    pub input_message_id: String,
    pub responder_identity_id: Option<String>,
    pub responder_profile_id: Option<String>,
    pub status: AgentChatTurnStatus,
    pub pending_interaction_id: Option<String>,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub lease_expires_at: Option<String>,
    pub next_attempt_at: Option<String>,
    pub response_message_id: Option<String>,
    pub error: Option<String>,
    pub correlation_id: String,
    /// Optimistic concurrency token for turn updates/cancellation.
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct SendAgentChatMessageRequest {
    pub content: String,
    pub dedupe_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct SendAgentChatMessageResponse {
    pub message: AgentChatMessageResponse,
    pub turn_job: Option<AgentChatTurnJobResponse>,
}

/// Optimistically cancels one visible turn.  The key is persisted through the
/// turn cancellation domain-event dedupe record, making a retry safe even
/// after the caller's original version has advanced.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct CancelAgentChatTurnRequest {
    pub expected_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct CreateAgentHandoffRequest {
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub content: String,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentHandoffResponse {
    pub id: String,
    pub source_chat_id: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub target_project_id: String,
    pub target_chat_id: String,
    pub author_identity_id: Option<String>,
    pub content: String,
    #[ts(type = "Record<string, unknown>")]
    pub content_guard: Value,
    pub sensitivity: String,
    pub status: AgentHandoffStatus,
    pub target_message_id: Option<String>,
    pub target_turn_job_id: Option<String>,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub delivered_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentChatListQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatListResponse {
    pub items: Vec<AgentChatResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentChatMessagesQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub before_sequence: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentChatMessageListResponse {
    pub items: Vec<AgentChatMessageResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}
