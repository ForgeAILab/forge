//! Agents, credentials, sessions, commitments, and inquiries.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub profile_id: String,
    pub backend_kind: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities_json: String,
    pub tool_policy_json: String,
    pub config_json: String,
    pub credential_ref: Option<String>,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: i64,
    pub heartbeat_interval_seconds: i64,
    pub max_missed_heartbeats: i64,
    pub status: AgentStatus,
    pub last_heartbeat_at: Option<String>,
    pub is_default: bool,
    pub paused: bool,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProfile {
    pub id: String,
    pub identity_id: String,
    pub backend_kind: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities_json: String,
    pub tool_policy_json: String,
    pub config_json: String,
    pub credential_ref: Option<String>,
    pub daemon_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialHandle {
    pub id: String,
    pub owner_user_id: String,
    pub provider: String,
    pub label: String,
    pub status: String,
    pub enabled: bool,
    pub credential_method: String,
    pub metadata_json: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Live health of a provider entry learned from real provider calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEntryHealth {
    pub credential_id: String,
    pub owner_user_id: String,
    /// `healthy`, `backoff` (transient), or `error` (auth / exhausted usage).
    pub status: String,
    pub consecutive_failures: i64,
    pub last_error_kind: Option<String>,
    pub last_error_message: Option<String>,
    pub last_failure_at: Option<String>,
    pub last_success_at: Option<String>,
    pub backoff_until: Option<String>,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialUsage {
    pub credential_id: String,
    pub agent_id: String,
    pub agent_name: String,
    pub runtime: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAuthorizationOperation {
    pub id: String,
    pub owner_user_id: String,
    pub provider: String,
    pub method: String,
    pub status: String,
    pub authorization_url: Option<String>,
    pub user_code: Option<String>,
    pub redirect_origin: String,
    pub callback_state_hash: Option<String>,
    pub request_json: String,
    pub poll_interval_seconds: i64,
    pub expires_at: String,
    pub profile_id: Option<String>,
    pub credential_handle_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProviderAuthorizationOperation {
    pub id: String,
    pub owner_user_id: String,
    pub provider: String,
    pub method: String,
    pub status: String,
    pub authorization_url: Option<String>,
    pub user_code: Option<String>,
    pub redirect_origin: String,
    pub callback_state_hash: Option<String>,
    pub request_json: String,
    pub poll_interval_seconds: i64,
    pub expires_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProviderAuthorizationOperation {
    pub id: String,
    pub expected_version: i64,
    pub status: String,
    pub authorization_url: Option<String>,
    pub user_code: Option<String>,
    pub poll_interval_seconds: i64,
    pub profile_id: Option<String>,
    pub credential_handle_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContextScope {
    pub id: String,
    pub identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub task_role: Option<String>,
    pub workspace_access: String,
    pub workspace_path: Option<String>,
    pub authority_json: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSession {
    pub id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub context_scope_id: String,
    pub backend_kind: String,
    pub runtime_session_id: Option<String>,
    pub status: String,
    pub capabilities_json: String,
    pub connection_status: String,
    pub predecessor_session_id: Option<String>,
    pub replaced_by_session_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConnectionHealth {
    pub profile_id: String,
    pub status: String,
    pub capability_status_json: String,
    pub checked_at: Option<String>,
    pub error_code: Option<String>,
    pub updated_at: String,
}

/// Durable identity/canonical-scope binding for one Agent Runtime LCM
/// timeline.  The runtime-facing typed values remain in the host crate; the
/// database stores their canonical JSON representation and immutable
/// provenance fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmTimeline {
    pub id: String,
    pub identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub authorization_revision: String,
    pub revision: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmEntryRecord {
    pub timeline_id: String,
    pub entry_id: String,
    pub sequence: i64,
    pub content_json: String,
    pub content_fingerprint: String,
    pub source_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmNodeRecord {
    pub timeline_id: String,
    pub node_id: String,
    pub kind: String,
    pub range_start: i64,
    pub range_end: i64,
    pub edges_json: String,
    pub source_fingerprint: String,
    pub summary_revision: String,
    pub summary: String,
    pub policy_revision: String,
    pub algorithm_revision: String,
    pub sizer_revision: String,
    pub provenance_json: String,
    pub token_count: i64,
    pub source_token_count: i64,
    pub classification_json: String,
    pub revision: i64,
    pub superseded_by: Option<String>,
    pub operation_id: String,
    pub operation_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmOperation {
    pub timeline_id: String,
    pub operation_id: String,
    pub operation_kind: String,
    pub operation_fingerprint: String,
    pub result_revision: i64,
    pub result_entries: i64,
    pub result_node_id: Option<String>,
    pub created_at: String,
}

/// A durable obligation owned by an AgentIdentity rather than a runtime
/// session.  Evidence, transfers, and lifecycle changes are append-only rows
/// associated with this record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitment {
    pub id: String,
    pub owner_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: AgentCommitmentStatus,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub originating_action_id: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: bool,
    pub cancellation_reason: Option<String>,
    pub blocked_reason: Option<String>,
    pub completed_at: Option<String>,
    pub cancelled_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitmentEvidence {
    pub id: String,
    pub commitment_id: String,
    pub evidence_type: String,
    pub evidence_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub description: Option<String>,
    pub metadata_json: String,
    pub authorized_by_type: String,
    pub authorized_by_id: String,
    pub dedupe_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitmentTransfer {
    pub id: String,
    pub commitment_id: String,
    pub from_identity_id: String,
    pub to_identity_id: String,
    pub reason: String,
    pub actor_type: String,
    pub actor_id: String,
    pub dedupe_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitmentLifecycle {
    pub id: String,
    pub commitment_id: String,
    pub from_status: Option<AgentCommitmentStatus>,
    pub to_status: AgentCommitmentStatus,
    pub actor_type: String,
    pub actor_id: String,
    pub reason: Option<String>,
    pub evidence_id: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInboxItem {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub kind: AgentInboxKind,
    pub status: AgentInboxStatus,
    pub title: String,
    pub body: String,
    pub payload_json: String,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub read_at: Option<String>,
    pub acknowledged_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentQuestion {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub status: AgentQuestionStatus,
    pub question: String,
    pub context_json: String,
    pub answer: Option<String>,
    pub asked_by_type: String,
    pub asked_by_id: String,
    pub answered_by_type: Option<String>,
    pub answered_by_id: Option<String>,
    pub inbox_item_id: Option<String>,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub version: i64,
    pub answered_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentAction {
    pub id: String,
    pub actor_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub payload_json: String,
    pub payload_hash: String,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub requested_permission: String,
    pub policy_result: AgentActionPolicyResult,
    pub policy_reason: Option<String>,
    pub status: AgentActionStatus,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub outcome_json: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionApproval {
    pub id: String,
    pub action_id: String,
    pub approver_identity_id: String,
    pub decision: AgentActionApprovalDecision,
    pub reason: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionExecution {
    pub id: String,
    pub action_id: String,
    pub attempt: i64,
    pub status: AgentActionExecutionStatus,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// The frozen outcome of one mutating application command.  A receipt is
/// immutable in both the Rust repository surface and the SQLite schema so an
/// identical retry can return the original result without rerunning a domain
/// mutation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommandReceipt {
    pub id: String,
    pub principal_type: String,
    pub principal_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub idempotency_key: String,
    pub input_digest: String,
    pub policy_result: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub event_id: String,
    pub agent_action_execution_id: Option<String>,
    pub outcome_json: String,
    pub committed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentCommitmentStatus {
    Proposed,
    Open,
    Accepted,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentInboxKind {
    Message,
    Question,
    Commitment,
    TaskOutcome,
    ActionResult,
    ReviewRequest,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentInboxStatus {
    Unread,
    Read,
    Acknowledged,
    Dismissed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentQuestionStatus {
    Open,
    Answered,
    Dismissed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActionPolicyResult {
    Allowed,
    ApprovalRequired,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActionStatus {
    Proposed,
    PendingApproval,
    Approved,
    Denied,
    Executing,
    Executed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActionApprovalDecision {
    Approved,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentActionExecutionStatus {
    Started,
    Succeeded,
    Failed,
}

enum_strings!(AgentCommitmentStatus {
    Proposed => "proposed",
    Open => "open",
    Accepted => "accepted",
    InProgress => "in_progress",
    Blocked => "blocked",
    Completed => "completed",
    Cancelled => "cancelled",
});

enum_strings!(AgentInboxKind {
    Message => "message",
    Question => "question",
    Commitment => "commitment",
    TaskOutcome => "task_outcome",
    ActionResult => "action_result",
    ReviewRequest => "review_request",
    System => "system",
});

enum_strings!(AgentInboxStatus {
    Unread => "unread",
    Read => "read",
    Acknowledged => "acknowledged",
    Dismissed => "dismissed",
});

enum_strings!(AgentQuestionStatus {
    Open => "open",
    Answered => "answered",
    Dismissed => "dismissed",
    Expired => "expired",
});

enum_strings!(AgentActionPolicyResult {
    Allowed => "allowed",
    ApprovalRequired => "approval_required",
    Denied => "denied",
});

enum_strings!(AgentActionStatus {
    Proposed => "proposed",
    PendingApproval => "pending_approval",
    Approved => "approved",
    Denied => "denied",
    Executing => "executing",
    Executed => "executed",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentActionApprovalDecision {
    Approved => "approved",
    Denied => "denied",
});

enum_strings!(AgentActionExecutionStatus {
    Started => "started",
    Succeeded => "succeeded",
    Failed => "failed",
});

/// One ephemeral, read-only Main-agent sub-agent run ("inquiry"). It is a
/// run log the Main session can inspect and cancel, not a work item: no
/// repo, no task flow, no state machine. The four token counters mirror
/// `agent_host::AgentTurnOutput` and are disjoint (context size = input +
/// cache_read + cache_write) -- never sum or collapse them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInquiry {
    pub id: String,
    pub chat_id: String,
    pub turn_job_id: Option<String>,
    pub identity_id: String,
    pub owner_user_id: String,
    pub title: String,
    pub question: String,
    pub status: AgentInquiryStatus,
    pub findings: Option<String>,
    pub findings_path: Option<String>,
    pub workspace_path: Option<String>,
    pub error: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub duration_ms: Option<i64>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

/// Closed set. The only user verb over an inquiry is cancel: deliberately no
/// retry, assignment, dependency, milestone, or review concept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentInquiryStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

enum_strings!(AgentInquiryStatus {
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});
