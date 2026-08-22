use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionLifecycle {
    Open,
    Acknowledged,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum AttentionCategory {
    HumanInputRequired,
    ValidationFailed,
    RunStalled,
    /// The owner lease is healthy, but semantic progress has exceeded the
    /// configured warning interval.  This is intentionally distinct from
    /// `RunStalled`: silence is a visibility signal, not proof of owner death.
    ProgressWarning,
    RetryExhausted,
    ReviewReady,
    ReviewRisk,
    ExecutionFailed,
    RuntimeOffline,
    BudgetThreshold,
    CommitmentOverdue,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AttentionItem {
    pub id: String,
    pub category: AttentionCategory,
    pub scope_type: String,
    pub scope_id: String,
    pub identity_id: Option<String>,
    pub source_event_id: String,
    pub priority: i64,
    pub lifecycle: AttentionLifecycle,
    pub summary: String,
    #[ts(type = "Record<string, unknown>")]
    pub details: Value,
    pub dedupe_key: String,
    pub occurred_at: String,
    pub updated_at: String,
    pub version: i64,
    pub acknowledged_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub resolved_at: Option<String>,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AttentionListResponse {
    pub items: Vec<AttentionItem>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub total_count: Option<u64>,
    pub consumer_health: Option<AttentionConsumerHealthResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AttentionConsumerHealthResponse {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub last_success_at: Option<String>,
    pub last_error_code: Option<String>,
    pub stale: bool,
    pub processed_events: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AttentionMutationRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AttentionSnoozeRequest {
    pub expected_version: i64,
    pub snoozed_until: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MissionControlWorkItem {
    pub task_id: String,
    pub project_id: String,
    pub title: String,
    pub status: String,
    pub priority: i64,
    pub updated_at: String,
    pub primary_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MissionControlAgentHealth {
    pub identity_id: String,
    pub name: String,
    pub backend_kind: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub identity_status: String,
    pub paused: bool,
    pub connection_status: Option<String>,
    pub last_activity_at: Option<String>,
    pub active_session_count: i64,
    pub project_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MissionControlRecentOutcome {
    pub task_id: String,
    pub project_id: String,
    pub title: String,
    pub outcome: String,
    pub occurred_at: String,
}

/// One bounded, authorized Mission Control audit activity.  Direct command
/// receipts and approval-queue actions share this projection so the operator
/// can see what was committed without exposing the action/command payload
/// bodies.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MissionControlCoordinationActivity {
    pub id: String,
    /// `direct_command` for a committed command receipt, or
    /// `approval_action` for a pending/approved AgentAction.
    pub activity_kind: String,
    pub actor_type: String,
    pub actor_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub input_digest: String,
    pub policy_result: String,
    pub status: String,
    pub correlation_id: String,
    #[ts(type = "Record<string, unknown> | null")]
    pub outcome: Option<Value>,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct MissionControlCapacity {
    pub active_executions: i64,
    pub queued_tasks: i64,
    pub active_sessions: i64,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct MissionControlHomeResponse {
    pub needs_attention: Vec<AttentionItem>,
    pub coordination_activity: Vec<MissionControlCoordinationActivity>,
    pub review_ready: Vec<MissionControlWorkItem>,
    pub active_work: Vec<MissionControlWorkItem>,
    pub agent_health: Vec<MissionControlAgentHealth>,
    pub recent_outcomes: Vec<MissionControlRecentOutcome>,
    pub capacity: MissionControlCapacity,
    pub consumer_health: Option<AttentionConsumerHealthResponse>,
    pub computed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct MissionControlQuery {
    pub project_id: Option<String>,
    pub cursor: Option<String>,
    pub status: Option<String>,
    pub include_snoozed: Option<bool>,
    pub include_total: Option<bool>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentDetailResponse {
    pub identity_id: String,
    pub name: String,
    pub description: Option<String>,
    pub backend_kind: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub identity_status: String,
    pub paused: bool,
    pub bindings: Vec<AgentBindingSummary>,
    pub scopes: Vec<AgentScopeSummary>,
    pub sessions: Vec<AgentSessionSummary>,
    pub current_focus: Option<MissionControlWorkItem>,
    pub open_commitment_count: i64,
    pub open_inbox_count: i64,
    pub memory_namespace_count: i64,
    pub usage: AgentUsageSummary,
    pub continuity: AgentContinuityHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentBindingSummary {
    pub binding_id: String,
    pub binding_type: String,
    pub project_id: Option<String>,
    pub chat_id: String,
    pub state: String,
    pub subscription_count: i64,
    pub wake_budget: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct AgentUsageSummary {
    pub execution_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentScopeSummary {
    pub scope_type: String,
    pub scope_id: String,
    pub task_role: Option<String>,
    pub workspace_access: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentSessionSummary {
    pub session_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub backend_kind: String,
    pub status: String,
    pub connection_status: String,
    pub last_activity_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct AgentContinuityHealth {
    pub status: String,
    pub checkpoint_present: bool,
    pub last_activity_at: Option<String>,
}
