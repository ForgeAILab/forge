use std::{fmt, str::FromStr};

use crate::pagination::PageRequest;
use crate::repository::{
    AdmitAgentChatTurn, CompleteDomainEvent, CreateAgentActionExecution, CreateCommandReceipt,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row};

/// Hourly wake-admission budget a Project Agent binding starts with. Zero
/// disables autonomous wakes entirely, so every binding-creation path must
/// use this default unless the user explicitly configured a budget.
pub const DEFAULT_PROJECT_AGENT_WAKE_BUDGET: i64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub settings: String,
    pub workflow_definition: String,
    pub workflow_template_name: Option<String>,
    pub primary_repo_id: Option<String>,
    pub paused_at: Option<String>,
    pub owner_id: Option<String>,
    pub project_hooks_json: String,
    pub project_work_epoch: i64,
    /// Charter authority state persisted by V076. Existing projects are
    /// deliberately represented as `legacy_unverified` until an explicit
    /// Charter approval establishes a current Charter.
    pub charter_status: String,
    pub charter_setup_required: bool,
    pub current_charter_id: Option<String>,
    pub current_charter_revision_id: Option<String>,
    pub current_charter_version: i64,
    pub primary_milestone_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Durable, retryable work that reconciles a Project's repository and
/// Workspace-capable role setup.  The status is deliberately independent of
/// the Project Agent binding/chat state and the execution-baseline gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectProvisioningOperation {
    pub id: String,
    pub project_id: String,
    pub idempotency_key: String,
    pub status: String,
    pub current_checkpoint: String,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub next_retry_at: Option<String>,
    pub retryable: bool,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectProvisioningOperation {
    pub id: String,
    pub project_id: String,
    pub idempotency_key: String,
    pub status: String,
    pub current_checkpoint: String,
    pub max_attempts: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub next_retry_at: Option<String>,
    pub retryable: bool,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProjectProvisioningOperation {
    pub id: String,
    pub expected_version: i64,
    pub status: Option<String>,
    pub current_checkpoint: Option<String>,
    pub attempt_count: Option<i64>,
    pub max_attempts: Option<i64>,
    pub lease_owner: Option<Option<String>>,
    pub lease_expires_at: Option<Option<String>>,
    pub next_retry_at: Option<Option<String>>,
    pub retryable: Option<bool>,
    pub last_error_code: Option<Option<String>>,
    pub last_error_message: Option<Option<String>>,
    pub completed_at: Option<Option<String>>,
    pub updated_at: String,
}

/// Atomic Project execution-setup mutation plus durable command receipt.
/// `bump_project_version` is false for actions whose durable effect is the
/// provisioning operation rather than the Project row itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyProjectExecutionSetupCommand {
    pub project_id: String,
    pub expected_project_version: Option<i64>,
    pub settings: Option<String>,
    pub primary_repo_id: Option<Option<String>>,
    pub bump_project_version: bool,
    pub provisioning_retry: Option<ScheduleProjectProvisioningRetry>,
    pub provisioning_metadata: Option<ReconcileProjectProvisioningMetadata>,
    pub receipt: CreateCommandReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleProjectProvisioningRetry {
    pub operation_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub lease_expires_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileProjectProvisioningMetadata {
    pub operation_id: String,
    pub expected_version: i64,
    pub status: String,
    pub current_checkpoint: String,
    pub retryable: bool,
    pub completed_at: Option<String>,
    pub updated_at: String,
    /// Checkpoint evidence is prepared from the authoritative repository and
    /// role view, then applied by the same SQLite transaction as the Project
    /// mutation, operation metadata, domain event, and command receipt.
    pub checkpoints: Vec<ReconcileProjectProvisioningCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileProjectProvisioningCheckpoint {
    pub id: String,
    pub operation_id: String,
    pub checkpoint: String,
    pub status: String,
    pub attempt_count: i64,
    pub details_json: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProjectExecutionSetupCommand {
    pub project: Project,
    pub receipt: CommandReceipt,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectProvisioningCheckpoint {
    pub id: String,
    pub operation_id: String,
    pub checkpoint: String,
    pub status: String,
    pub attempt_count: i64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub details_json: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertProjectProvisioningCheckpoint {
    pub id: String,
    pub operation_id: String,
    pub checkpoint: String,
    pub status: String,
    pub attempt_count: i64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub details_json: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectProvisioningError {
    pub id: String,
    pub operation_id: String,
    pub checkpoint_id: Option<String>,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub attempt_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectProvisioningError {
    pub id: String,
    pub operation_id: String,
    pub checkpoint_id: Option<String>,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub attempt_count: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectHookRunStatus {
    Queued,
    Running,
    Dispatched,
    Skipped,
    Failed,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectHookRun {
    pub id: String,
    pub project_id: String,
    pub rule_id: String,
    pub trigger_type: String,
    pub dedupe_key: String,
    pub status: ProjectHookRunStatus,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub automation_task_id: Option<String>,
    pub execution_id: Option<String>,
    pub agent_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectHookRun {
    pub id: String,
    pub project_id: String,
    pub rule_id: String,
    pub trigger_type: String,
    pub dedupe_key: String,
    pub status: ProjectHookRunStatus,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub automation_task_id: Option<String>,
    pub execution_id: Option<String>,
    pub agent_id: Option<String>,
    pub reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProjectHookRun {
    pub id: String,
    pub status: ProjectHookRunStatus,
    // Outer Some means "update this column"; inner None writes SQL NULL.
    pub automation_task_id: Option<Option<String>>,
    pub execution_id: Option<Option<String>>,
    pub agent_id: Option<Option<String>>,
    pub reason: Option<Option<String>>,
    pub updated_at: String,
    pub completed_at: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrationPlatform {
    Github,
    Gitea,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectIntegration {
    pub id: String,
    pub project_id: String,
    pub platform: IntegrationPlatform,
    pub base_url: String,
    pub owner: String,
    pub repo: String,
    pub token_secret_ref: String,
    pub poll_interval_secs: i64,
    pub sync_filter: String,
    pub default_task_state: Option<String>,
    pub default_assignee_type: Option<String>,
    pub default_assignee_id: Option<String>,
    pub enabled: bool,
    pub last_polled_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExternalLink {
    pub id: String,
    pub task_id: String,
    pub integration_id: String,
    pub platform: String,
    pub remote_owner: String,
    pub remote_repo: String,
    pub remote_issue_number: i64,
    pub remote_url: String,
    pub global_id: String,
    pub synced_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectIntegration {
    pub id: String,
    pub project_id: String,
    pub platform: IntegrationPlatform,
    pub base_url: String,
    pub owner: String,
    pub repo: String,
    pub token_secret_ref: String,
    pub poll_interval_secs: i64,
    pub sync_filter: String,
    pub default_task_state: Option<String>,
    pub default_assignee_type: Option<String>,
    pub default_assignee_id: Option<String>,
    pub enabled: bool,
    pub last_polled_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProjectIntegration {
    pub id: String,
    pub updated_at: String,
    pub project_id: Option<String>,
    pub platform: Option<IntegrationPlatform>,
    pub base_url: Option<String>,
    pub owner: Option<String>,
    pub repo: Option<String>,
    pub token_secret_ref: Option<String>,
    pub poll_interval_secs: Option<i64>,
    pub sync_filter: Option<String>,
    pub default_task_state: Option<Option<String>>,
    pub default_assignee_type: Option<Option<String>>,
    pub default_assignee_id: Option<Option<String>>,
    pub enabled: Option<bool>,
    pub last_polled_at: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskExternalLink {
    pub id: String,
    pub task_id: String,
    pub integration_id: String,
    pub platform: String,
    pub remote_owner: String,
    pub remote_repo: String,
    pub remote_issue_number: i64,
    pub remote_url: String,
    pub global_id: String,
    pub synced_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repo {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub remote_url: String,
    pub local_path: Option<String>,
    pub work_mode: WorkMode,
    pub default_branch: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkMode {
    DirectMerge,
    PullRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrProviderConfig {
    pub id: String,
    pub repo_id: String,
    pub provider_type: String,
    pub base_url: Option<String>,
    pub polling_interval_seconds: i64,
    pub token_secret_ref: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrMetadata {
    pub id: String,
    pub task_id: String,
    pub provider_type: String,
    pub provider_pr_id: Option<String>,
    pub pr_url: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    pub pr_state: String,
    pub merge_status: String,
    pub last_synced_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

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
    pub credential_method: String,
    pub metadata_json: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialUsage {
    pub credential_id: String,
    pub agent_id: String,
    pub agent_name: String,
    pub runtime: String,
    pub last_used_at: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainEvent {
    pub sequence: i64,
    pub id: String,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub scope_type: String,
    pub scope_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub dedupe_key: Option<String>,
    pub payload_json: String,
    pub created_at: String,
}

/// The only terminal/processable outcomes a wake consumer may persist for a
/// claimed source event.  Deferred and setup-required outcomes are durable
/// retry points, not cursor-completion shortcuts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentWakeDispositionKind {
    TurnAdmitted,
    DeterministicallySuppressed,
    Deferred,
    SetupRequired,
}

impl fmt::Display for AgentWakeDispositionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TurnAdmitted => "turn_admitted",
            Self::DeterministicallySuppressed => "deterministically_suppressed",
            Self::Deferred => "deferred",
            Self::SetupRequired => "setup_required",
        })
    }
}

impl FromStr for AgentWakeDispositionKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "turn_admitted" => Ok(Self::TurnAdmitted),
            "deterministically_suppressed" => Ok(Self::DeterministicallySuppressed),
            "deferred" => Ok(Self::Deferred),
            "setup_required" => Ok(Self::SetupRequired),
            other => Err(format!("unknown wake disposition: {other}")),
        }
    }
}

/// One immutable wake-disposition attempt.  Retries append a new row and
/// advance `AgentWakeDispositionCurrent`; no prior identity/profile or
/// incident provenance is rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeDisposition {
    pub id: String,
    pub consumer_name: String,
    pub source_event_id: String,
    pub source_event_sequence: i64,
    pub attempt_number: i64,
    pub max_attempts: i64,
    pub disposition: AgentWakeDispositionKind,
    pub reason: String,
    pub turn_job_id: Option<String>,
    pub attention_id: Option<String>,
    pub retry_at: Option<String>,
    pub incident_key: Option<String>,
    pub incident_digest: Option<String>,
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
    pub provenance_json: Option<String>,
    pub parent_disposition_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentWakeDisposition {
    pub id: String,
    pub consumer_name: String,
    pub source_event_id: String,
    pub source_event_sequence: i64,
    pub attempt_number: i64,
    pub max_attempts: i64,
    pub disposition: AgentWakeDispositionKind,
    pub reason: String,
    pub turn_job_id: Option<String>,
    pub attention_id: Option<String>,
    pub retry_at: Option<String>,
    pub incident_key: Option<String>,
    pub incident_digest: Option<String>,
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
    pub provenance_json: Option<String>,
    pub parent_disposition_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Atomically persists the first disposition for a claimed event, its
/// current-pointer row, and the event projection receipt/cursor checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteClaimedWake {
    pub disposition: CreateAgentWakeDisposition,
    pub completion: CompleteDomainEvent,
    /// Required for `turn_admitted`.  The DB inserts/replays this message and
    /// turn inside the same transaction as the disposition and source-event
    /// checkpoint, closing the crash seam between admission and delivery.
    pub admission: Option<AdmitAgentChatTurn>,
    /// Snapshot of the Attention that was used to prepare `admission`.  The
    /// repository re-reads this row under the same IMMEDIATE transaction
    /// before inserting the turn, so a material projection update cannot be
    /// admitted from stale resolver input.
    pub expected_attention: Option<ExpectedAttentionSnapshot>,
}

/// Append a later deferred/setup-required disposition attempt after the
/// authoritative source event has already been checkpointed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryAgentWakeDisposition {
    pub disposition: CreateAgentWakeDisposition,
    pub expected_parent_id: String,
    pub now: String,
    /// Required when the retry advances from deferred/setup-required to
    /// `turn_admitted`; it is committed with the new immutable attempt.
    pub admission: Option<AdmitAgentChatTurn>,
    /// Optional Attention snapshot used when a retry advances to a turn.
    pub expected_attention: Option<ExpectedAttentionSnapshot>,
}

/// Immutable Attention evidence captured while preparing one wake admission.
/// The scope/source fields are intentionally explicit: checking only a
/// projection version is unsafe because legacy upserts did not always bump
/// that version for material source changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedAttentionSnapshot {
    pub id: String,
    pub version: i64,
    pub digest: Option<String>,
    pub status: String,
    pub canonical_scope_type: String,
    pub canonical_scope_id: String,
    pub source_event_id: String,
    pub source_sequence: Option<i64>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeDispositionCurrent {
    pub consumer_name: String,
    pub source_event_id: String,
    pub disposition_id: String,
    pub attempt_number: i64,
    pub updated_at: String,
    pub version: i64,
}

/// Rebuildable, durable Attention materialization.  The source event and
/// dedupe key keep this row derived; the lifecycle columns are the only
/// operator-owned state and use optimistic versions for concurrent clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionProjection {
    pub id: String,
    pub attention_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub identity_id: Option<String>,
    pub source_event_id: String,
    pub priority: i64,
    pub status: String,
    pub summary: String,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: String,
    pub updated_at: String,
    pub version: i64,
    pub acknowledged_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub resolved_at: Option<String>,
    pub updated_by_user_id: Option<String>,
    pub recommended_action: String,
    pub source_sequence: Option<i64>,
}

/// Canonical, redaction-safe digest used to bind a wake decision to the exact
/// Attention materialization it observed.  Keep this in `db` so the atomic
/// admission transaction can validate a snapshot without depending on the
/// services crate.
pub fn canonical_attention_incident_digest(attention: &AttentionProjection) -> String {
    let bounded_details = attention
        .details_json
        .chars()
        .take(8_192)
        .collect::<String>();
    let canonical_details = serde_json::from_str::<serde_json::Value>(&bounded_details)
        .map(|value| value.to_string())
        .unwrap_or(bounded_details);
    let details_digest = wake_incident_digest(&canonical_details);
    let canonical = format!(
        "type={};scope_type={};scope_id={};status={};source_event_id={};source_sequence={:?};details_digest={};recommended_action={};version={}",
        bounded_wake_ref(&attention.attention_type),
        bounded_wake_ref(&attention.scope_type),
        bounded_wake_ref(&attention.scope_id),
        bounded_wake_ref(&attention.status),
        bounded_wake_ref(&attention.source_event_id),
        attention.source_sequence,
        details_digest,
        bounded_wake_ref(&attention.recommended_action),
        attention.version,
    );
    wake_incident_digest(&canonical)
}

fn bounded_wake_ref(value: &str) -> String {
    value.chars().take(256).collect()
}

fn wake_incident_digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hasher.update([0]);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAttentionProjection {
    pub id: String,
    pub attention_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub identity_id: Option<String>,
    pub source_event_id: String,
    pub priority: i64,
    pub status: String,
    pub summary: String,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: String,
    pub updated_at: String,
    pub acknowledged_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub resolved_at: Option<String>,
    pub updated_by_user_id: Option<String>,
    pub recommended_action: String,
    pub source_sequence: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAttentionLifecycle {
    pub id: String,
    pub expected_version: i64,
    pub status: String,
    pub acknowledged_at: Option<Option<String>>,
    pub snoozed_until: Option<Option<String>>,
    pub resolved_at: Option<Option<String>>,
    pub updated_by_user_id: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionListQuery {
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub scope_type: Option<String>,
    pub status: Option<String>,
    pub include_snoozed: bool,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionConsumerHealth {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub last_started_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error_at: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<String>,
    pub processed_events: i64,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertAttentionConsumerHealth {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub last_started_at: Option<String>,
    pub last_success_at: Option<String>,
    pub last_error_at: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<String>,
    pub processed_events_delta: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventConsumerCursor {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventConsumerCutover {
    pub consumer_name: String,
    pub cutover_sequence: i64,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: String,
    pub task_id: String,
    pub repo_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub status: WorkspaceStatus,
    pub before_sha: Option<String>,
    pub cleanup_after: Option<String>,
    pub error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceStatus {
    Creating,
    Ready,
    Error,
    Cleaning,
    Cleaned,
}

/// Scheduler-issued authority for one assigned Task/repository operation.
/// This row is internal: callers receive neither its capability payload nor
/// any filesystem path or bearer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceLease {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    /// Exact Task revision admitted for this execution attempt.
    pub task_version: i64,
    /// Execution identity for the attempt; a lease cannot be replayed by a
    /// later execution of the same Task.
    pub execution_id: String,
    /// Stable operation key for replaying one scheduler lease issue.
    pub operation_idempotency_key: String,
    pub repository_binding_id: String,
    pub base_ref: String,
    pub role: String,
    pub capabilities_json: String,
    pub assigned_principal_type: String,
    pub assigned_principal_id: String,
    pub capability_profile_revision: String,
    pub capability_profile_digest: String,
    pub issuing_principal_type: String,
    pub issuing_principal_id: String,
    pub status: String,
    pub issued_at: String,
    pub expires_at: String,
    pub revoked_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Daemon {
    pub id: String,
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels_json: String,
    pub status: DaemonStatus,
    pub last_report_at: Option<String>,
    pub registration_token_hash: Option<String>,
    pub detected_clis_json: String,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonStatus {
    Online,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Runtime {
    pub id: String,
    pub daemon_id: String,
    pub kind: String,
    pub workspace_root: String,
    pub status: RuntimeStatus,
    pub labels_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeStatus {
    Ready,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentStatus {
    Idle,
    Busy,
    Error,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skill {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub id: String,
    pub project_id: String,
    pub task_id: Option<String>,
    pub event_type: String,
    pub title: String,
    pub body: Option<String>,
    pub read: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNotification {
    pub id: String,
    pub project_id: String,
    pub task_id: Option<String>,
    pub event_type: String,
    pub title: String,
    pub body: Option<String>,
    pub read: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub project_id: String,
    pub repo_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub task_type: String,
    pub status: TaskStatus,
    pub is_automation: bool,
    pub priority: i64,
    pub board_position: f64,
    pub subtask_order: Option<i64>,
    pub task_state_config: Option<String>,
    pub merge_config: Option<String>,
    pub metadata_json: Option<String>,
    pub plan: Option<String>,
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub review_passed_at: Option<String>,
    pub archived_at: Option<String>,
    pub deleted_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

pub type TaskStatus = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveTaskIdentity {
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub board_revision: i64,
    pub target_status: String,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompareAndMoveTask {
    pub operation_id: String,
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub board_revision: i64,
    pub target_status: String,
    pub target_column_statuses: Vec<String>,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub transition_log_id: String,
    pub trigger_name: Option<String>,
    pub triggered_by: String,
    pub trigger_reason: String,
    pub rejection: bool,
    pub updated_at: String,
}

impl CompareAndMoveTask {
    pub fn identity(&self) -> MoveTaskIdentity {
        MoveTaskIdentity {
            project_id: self.project_id.clone(),
            task_id: self.task_id.clone(),
            task_version: self.task_version,
            board_revision: self.board_revision,
            target_status: self.target_status.clone(),
            before_id: self.before_id.clone(),
            after_id: self.after_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MoveTaskResult {
    pub task: Task,
    pub board_revision: i64,
    pub operation_id: String,
    pub old_status: String,
    pub old_board_position: f64,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MoveTaskPersistence {
    Committed {
        result: Box<MoveTaskResult>,
        transition_log: Box<TransitionLog>,
    },
    Replayed(Box<MoveTaskResult>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    pub id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub role: String,
    pub status: ExecutionStatus,
    pub stop_reason: Option<StopReason>,
    pub stopped_by: Option<String>,
    pub resume_policy: Option<ResumePolicy>,
    pub stopped_at: Option<String>,
    pub parent_execution_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub agent_message_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub prompt: Option<String>,
    pub summary: Option<String>,
    pub logs_path: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub error: Option<String>,
    pub executor_config_snapshot_json: Option<String>,
    pub workspace_id: Option<String>,
    /// Monotonic compare-and-swap version for the owner lease and terminal
    /// lifecycle.  This is deliberately separate from Task/version state.
    pub execution_version: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub hard_deadline_at: Option<String>,
    pub last_heartbeat_at: Option<String>,
    pub last_progress_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

pub type ExecutionRole = String;

/// Versioned account-level binding for the singular Main Agent Chat.  Binding
/// rows are retained when replaced so historical messages keep their original
/// identity/profile attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountMainAgentBinding {
    pub id: String,
    pub account_id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub state: String,
    pub autonomy_policy_json: String,
    pub tool_policy_revision: String,
    pub version: i64,
    pub replaced_by_binding_id: Option<String>,
    pub replacement_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Versioned Project Agent binding.  A setup-required row intentionally has
/// no identity/profile; this is an explicit state for migrated Projects and
/// cannot be used to admit a model turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectAgentBinding {
    pub id: String,
    pub project_id: String,
    pub identity_id: Option<String>,
    pub profile_id: Option<String>,
    pub state: String,
    pub autonomy_policy_json: String,
    pub permission_ceiling_json: String,
    pub subscriptions_json: String,
    pub wake_budget: i64,
    pub version: i64,
    pub replaced_by_binding_id: Option<String>,
    pub replacement_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChat {
    pub id: String,
    pub kind: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub status: String,
    pub instruction_revision: i64,
    pub message_count: i64,
    pub last_message_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatSourceRef {
    pub chat_id: String,
    pub source_type: String,
    pub source_id: String,
    pub source_scope_type: Option<String>,
    pub source_scope_id: Option<String>,
    pub source_revision: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatInstructionRevision {
    pub id: String,
    pub chat_id: String,
    pub source_type: String,
    pub source_id: Option<String>,
    pub revision: i64,
    pub body: String,
    pub content_guard_json: String,
    pub sensitivity: String,
    pub created_by_type: String,
    pub created_by_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatMessageAuthorType {
    User,
    Agent,
    System,
    Handoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatMessageStatus {
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatMessage {
    pub id: String,
    pub chat_id: String,
    pub sequence: i64,
    pub author_type: AgentChatMessageAuthorType,
    pub author_id: Option<String>,
    pub content: String,
    pub content_guard_json: String,
    pub sensitivity: String,
    pub status: AgentChatMessageStatus,
    pub outcome: Option<String>,
    pub model: Option<String>,
    pub profile_id: Option<String>,
    pub session_id: Option<String>,
    pub context_manifest_id: Option<String>,
    pub token_usage_json: Option<String>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub handoff_id: Option<String>,
    pub source_type: String,
    pub source_id: Option<String>,
    pub source_message_id: Option<String>,
    pub source_room_id: Option<String>,
    pub source_conversation_id: Option<String>,
    pub source_sequence: Option<i64>,
    pub source_metadata_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatTurnState {
    Queued,
    Leased,
    AwaitingInput,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatTurnJob {
    pub id: String,
    pub chat_id: String,
    pub triggering_message_id: String,
    pub responder_identity_id: Option<String>,
    pub profile_id: Option<String>,
    /// Immutable admission provenance.  Legacy queued rows are nullable and
    /// are handled conservatively until re-admitted by the current resolver.
    pub responder_binding_id: Option<String>,
    pub responder_binding_version: Option<i64>,
    pub responder_identity_version: Option<i64>,
    pub profile_version: Option<i64>,
    pub operating_skill_revision_id: Option<String>,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub permission_policy_digest: Option<String>,
    pub tool_policy_digest: Option<String>,
    pub admission_digest: Option<String>,
    pub canonical_scope_provenance_json: Option<String>,
    pub canonical_scope_type: String,
    pub canonical_scope_id: String,
    pub status: AgentChatTurnState,
    pub pending_interaction_id: Option<String>,
    pub dedupe_key: String,
    pub lease_owner: Option<String>,
    pub leased_until: Option<String>,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub next_attempt_at: Option<String>,
    pub response_message_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentHandoffStatus {
    Pending,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHandoff {
    pub id: String,
    pub source_chat_id: String,
    pub target_chat_id: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub target_message_id: Option<String>,
    pub target_turn_job_id: Option<String>,
    pub author_identity_id: Option<String>,
    pub content: String,
    pub content_guard_json: String,
    pub source_revisions_json: String,
    pub status: AgentHandoffStatus,
    pub error_code: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryKind {
    Observation,
    Decision,
    Handoff,
    Failure,
    ReviewResult,
    ExecutionSummary,
    Comment,
    Transition,
    Artifact,
    Lesson,
    ContextPack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemorySourceType {
    Execution,
    Review,
    Comment,
    Transition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryConfidence {
    Confirmed,
    Partial,
    Unconfirmed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryItem {
    pub row_id: i64,
    pub id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub execution_id: Option<String>,
    pub scope_type: String,
    pub scope_id: String,
    pub visibility: String,
    pub owner_identity_id: Option<String>,
    pub authority: String,
    pub sensitivity: String,
    pub retention_priority: i64,
    pub provenance_json: String,
    pub publication_source_id: Option<String>,
    pub supersedes_id: Option<String>,
    pub valid_from: Option<String>,
    pub valid_until: Option<String>,
    pub source_event_id: Option<String>,
    pub source_scope_type: Option<String>,
    pub source_scope_id: Option<String>,
    pub source_revision: Option<String>,
    pub source_type: String,
    pub kind: String,
    pub title: String,
    pub summary: Option<String>,
    pub body: String,
    pub metadata_json: String,
    pub confidence: Option<String>,
    pub quality_score: Option<i64>,
    pub created_by_type: Option<String>,
    pub created_by_id: Option<String>,
    pub created_at: String,
}

impl MemoryItem {
    pub fn from_row(row: &SqliteRow) -> std::result::Result<MemoryItem, sqlx::Error> {
        Ok(MemoryItem {
            row_id: row.try_get("row_id")?,
            id: row.try_get("id")?,
            project_id: row.try_get("project_id")?,
            task_id: row.try_get("task_id")?,
            execution_id: row.try_get("execution_id")?,
            scope_type: row.try_get("scope_type")?,
            scope_id: row.try_get("scope_id")?,
            visibility: row.try_get("visibility")?,
            owner_identity_id: row.try_get("owner_identity_id")?,
            authority: row.try_get("authority")?,
            sensitivity: row.try_get("sensitivity")?,
            retention_priority: row.try_get("retention_priority")?,
            provenance_json: row.try_get("provenance_json")?,
            publication_source_id: row.try_get("publication_source_id")?,
            supersedes_id: row.try_get("supersedes_id")?,
            valid_from: row.try_get("valid_from")?,
            valid_until: row.try_get("valid_until")?,
            source_event_id: row.try_get("source_event_id")?,
            source_scope_type: row.try_get("source_scope_type")?,
            source_scope_id: row.try_get("source_scope_id")?,
            source_revision: row.try_get("source_revision")?,
            source_type: row.try_get("source_type")?,
            kind: row.try_get("kind")?,
            title: row.try_get("title")?,
            summary: row.try_get("summary")?,
            body: row.try_get("body")?,
            metadata_json: row.try_get("metadata_json")?,
            confidence: row.try_get("confidence")?,
            quality_score: row.try_get("quality_score")?,
            created_by_type: row.try_get("created_by_type")?,
            created_by_id: row.try_get("created_by_id")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// One server-authorized canonical scope that a MemorySource may search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryScopeGrant {
    pub scope_type: String,
    pub scope_id: String,
    pub visibility: Vec<String>,
    pub identity_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryAccessQuery {
    pub identity_id: Option<String>,
    pub grants: Vec<MemoryScopeGrant>,
    pub query: String,
    pub limit: i64,
    pub cursor: Option<String>,
    pub include_retracted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryGetQuery {
    pub id: String,
    pub identity_id: Option<String>,
    pub grants: Vec<MemoryScopeGrant>,
    pub include_retracted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMemoryLifecycleAssertion {
    pub id: String,
    pub memory_item_id: String,
    pub assertion_type: String,
    pub related_memory_id: Option<String>,
    pub reason: Option<String>,
    pub evidence_json: String,
    pub asserted_by_type: String,
    pub asserted_by_id: Option<String>,
    pub source_event_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLifecycleAssertion {
    pub id: String,
    pub memory_item_id: String,
    pub assertion_type: String,
    pub related_memory_id: Option<String>,
    pub reason: Option<String>,
    pub evidence_json: String,
    pub asserted_by_type: String,
    pub asserted_by_id: Option<String>,
    pub source_event_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateForgeMemorySourceBinding {
    pub id: String,
    pub identity_id: String,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub policy_revision: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeMemorySourceBinding {
    pub id: String,
    pub identity_id: String,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub policy_revision: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifest {
    pub id: String,
    pub identity_id: String,
    pub agent_session_id: Option<String>,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub policy_revision: String,
    pub domain_revision: String,
    pub lcm_binding_revision: Option<String>,
    pub runtime_manifest_id: Option<String>,
    pub runtime_manifest_fingerprint: Option<String>,
    pub combined_fingerprint: String,
    pub request_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    pub id: String,
    pub identity_id: String,
    pub agent_session_id: Option<String>,
    pub context_scope_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub policy_revision: String,
    pub domain_revision: String,
    pub lcm_binding_revision: Option<String>,
    pub runtime_manifest_id: Option<String>,
    pub runtime_manifest_fingerprint: Option<String>,
    pub combined_fingerprint: String,
    pub request_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateContextManifestSource {
    pub manifest_id: String,
    pub ordinal: i64,
    pub source_id: String,
    pub source_type: String,
    pub source_revision: String,
    pub selection_reason: String,
    pub disposition: String,
    pub retention_priority: i64,
    pub fragment_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifestSource {
    pub manifest_id: String,
    pub ordinal: i64,
    pub source_id: String,
    pub source_type: String,
    pub source_revision: String,
    pub selection_reason: String,
    pub disposition: String,
    pub retention_priority: i64,
    pub fragment_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    UserCancelled,
    TaskCancelled,
    RoleReassigned,
    GracefulShutdown,
    CrashRecovery,
    AgentTimeout,
    ExecutionStalled,
    DaemonDisconnected,
    ExecutorCancelled,
    ExecutorFailed,
    LegacyUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumePolicy {
    Auto,
    Manual,
    None,
}

/// Scheduler-owned lease claim.  A claim is valid only for the expected
/// execution version and either an unowned/expired lease.  The first claim
/// establishes the immutable hard deadline for the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimExecutionLease {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub lease_expires_at: String,
    pub hard_deadline_at: String,
    pub now: String,
}

/// Fixed-cadence owner heartbeat.  Renewal changes only liveness metadata and
/// never extends the profile/capability hard deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewExecutionLease {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub lease_expires_at: String,
    pub now: String,
}

/// Semantic progress is intentionally not a heartbeat.  The owner/version
/// compare-and-swap prevents a late runner from mutating current execution
/// truth after losing its lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordExecutionProgress {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub progress_at: String,
    pub now: String,
}

/// Atomically claim the right to publish one stale-progress warning.  The
/// expected progress value is part of the CAS so a semantic event that arrives
/// concurrently wins over the monitor and suppresses a stale warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordExecutionProgressWarning {
    pub execution_id: String,
    pub expected_version: i64,
    pub owner: String,
    pub expected_last_progress_at: Option<String>,
    pub stale_before: String,
    pub now: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionLeaseDisposition {
    Revoke,
    Expire,
}

/// One terminal transition input shared by runner, cancellation, and monitor
/// callers.  The optional owner is a compare-and-swap predicate when supplied;
/// monitor and runner callers should always provide it, while an authorized
/// user cancellation may intentionally rely on the execution version alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalizeExecution {
    pub execution_id: String,
    pub expected_version: i64,
    pub lease_owner: Option<String>,
    pub status: ExecutionStatus,
    pub stop_reason: Option<Option<StopReason>>,
    pub stopped_by: Option<Option<String>>,
    pub stopped_at: Option<Option<String>>,
    pub resume_policy: Option<Option<ResumePolicy>>,
    pub agent_session_id: Option<Option<String>>,
    pub agent_message_id: Option<Option<String>>,
    pub last_activity_at: Option<Option<String>>,
    pub last_progress_at: Option<Option<String>>,
    pub summary: Option<Option<String>>,
    pub logs_path: Option<Option<String>>,
    pub before_sha: Option<Option<String>>,
    pub after_sha: Option<Option<String>>,
    pub error: Option<Option<String>>,
    pub executor_config_snapshot_json: Option<Option<String>>,
    pub updated_at: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub lease_disposition: ExecutionLeaseDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionLeaseMutation {
    Updated(Execution),
    Concurrent { current: Option<Execution> },
    HardDeadline { current: Option<Execution> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionProgressWarningOutcome {
    Committed {
        execution: Execution,
        event: DomainEvent,
    },
    Replayed {
        execution: Execution,
        event: DomainEvent,
    },
    Concurrent {
        current: Option<Execution>,
    },
}

/// The terminal CAS result is deliberately not represented by
/// `DbError::VersionConflict`: callers must be able to distinguish a winning
/// committed identity from a concurrent loser without performing a stale
/// read.  Only `Committed` is allowed to cascade Task state or publish a
/// terminal event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionTerminalOutcome {
    Committed {
        execution: Execution,
        event: Box<DomainEvent>,
        workspace_lease_id: Option<String>,
        workspace_lease_status: Option<String>,
    },
    Concurrent {
        current: Option<Execution>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Review {
    pub id: String,
    pub task_id: String,
    pub execution_id: String,
    pub attempt_number: i64,
    pub status: ReviewStatus,
    pub step_results_json: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewStatus {
    Running,
    AwaitingHuman,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentAuthorType {
    User,
    Agent,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskComment {
    pub id: String,
    pub task_id: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMedia {
    pub id: String,
    pub task_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub created_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskMedia {
    pub id: String,
    pub task_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub created_at: String,
}

/// Project-owned metadata for one existing task media blob.
///
/// The `id`, `storage_key`, and legacy task media reference are deliberately
/// kept separate from the Project evidence attachment.  This lets Project
/// evidence and immutable release pins reuse the exact bytes without changing
/// the historical Task media API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaAsset {
    pub id: String,
    pub project_id: String,
    pub legacy_task_media_id: Option<String>,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub checksum: Option<String>,
    pub availability: String,
    pub gc_state: String,
    pub gc_candidate_at: Option<String>,
    pub gc_lease_owner: Option<String>,
    pub gc_lease_expires_at: Option<String>,
    pub version: i64,
    pub deleted_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAttachment {
    pub id: String,
    pub project_id: String,
    pub asset_id: String,
    pub attachment_kind: String,
    pub task_media_id: Option<String>,
    pub task_id: Option<String>,
    pub milestone_id: Option<String>,
    pub milestone_check_id: Option<String>,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub acceptance_check_ids_json: String,
    pub caption: Option<String>,
    pub evidence_kind: Option<String>,
    pub checksum: Option<String>,
    pub availability: String,
    pub project_url: Option<String>,
    pub author_type: String,
    pub author_id: Option<String>,
    pub authorization_json: String,
    pub created_at: String,
}

/// Project media upload metadata.  The bytes are written by the API's
/// storage adapter; this record makes the metadata insert and its durable
/// idempotency event one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAsset {
    pub id: String,
    pub project_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub checksum: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub authorization_event_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginProjectMediaUpload {
    pub project_id: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub asset_id: String,
    pub final_storage_key: String,
    pub staging_storage_key: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub checksum: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaUpload {
    pub project_id: String,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub expected_project_version: i64,
    pub asset_id: String,
    pub final_storage_key: String,
    pub staging_storage_key: Option<String>,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub checksum: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaTombstone {
    pub asset_id: String,
    pub project_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub target_availability: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub authorization_event_id: String,
    pub authorization_json: String,
    pub reason: String,
    pub created_at: String,
}

/// Mutation context for an evidence attachment.  The legacy attachment
/// method remains available to Task/media migration code; Project evidence
/// uses this composite form so the milestone CAS and source event commit
/// together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMediaAttachmentMutation {
    pub attachment: CreateProjectMediaAttachment,
    pub expected_milestone_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub authorization_event_id: String,
    /// Optional shared-command provenance.  When present, the attachment,
    /// event, frozen receipt, and (for approved AgentActions) execution are
    /// committed by one transaction.  Legacy callers leave these fields
    /// unset while they are migrated to the command boundary.
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftDeleteProjectMediaAttachmentMutation {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
    pub mutation_fingerprint: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub authorization_json: String,
    pub authorization_event_id: String,
    pub deleted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMediaAttachment {
    pub id: String,
    pub project_id: String,
    pub asset_id: String,
    pub attachment_kind: String,
    pub task_media_id: Option<String>,
    pub task_id: Option<String>,
    pub milestone_id: Option<String>,
    pub milestone_check_id: Option<String>,
    pub source_task_id: Option<String>,
    pub source_execution_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub acceptance_check_ids_json: String,
    pub caption: Option<String>,
    pub evidence_kind: Option<String>,
    pub checksum: Option<String>,
    pub availability: String,
    pub project_url: Option<String>,
    pub author_type: String,
    pub author_id: Option<String>,
    pub authorization_json: String,
    pub version: i64,
    pub created_at: String,
    pub deleted_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseMediaPin {
    pub id: String,
    pub project_id: String,
    pub release_id: String,
    pub asset_id: String,
    pub attachment_id: Option<String>,
    pub legacy_task_media_id: Option<String>,
    pub asset_checksum: String,
    pub attachment_digest: String,
    pub availability: String,
    pub pin_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReleaseMediaPin {
    pub id: String,
    pub project_id: String,
    pub release_id: String,
    pub asset_id: String,
    pub attachment_id: Option<String>,
    pub legacy_task_media_id: Option<String>,
    pub asset_checksum: String,
    pub attachment_digest: String,
    pub availability: String,
    pub pin_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalSessionStatus {
    Starting,
    Running,
    Exited,
    Terminated,
    TimedOut,
    Orphaned,
    CleanupTerminated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSession {
    pub id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub daemon_id: Option<String>,
    pub status: TerminalSessionStatus,
    pub rows: i64,
    pub cols: i64,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub exit_signal: Option<String>,
    pub exit_reason: Option<String>,
    pub created_by_user_id: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub ended_at: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTerminalSession {
    pub id: String,
    pub task_id: String,
    pub workspace_id: String,
    pub daemon_id: Option<String>,
    pub created_by_user_id: String,
    pub rows: i64,
    pub cols: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTerminalSessionStatus {
    pub status: TerminalSessionStatus,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub ended_at: Option<String>,
    pub pid: Option<i64>,
    pub exit_code: Option<i64>,
    pub exit_signal: Option<String>,
    pub exit_reason: Option<String>,
}

macro_rules! enum_strings {
    ($enum:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        impl fmt::Display for $enum {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let value = match self {
                    $(Self::$variant => $value,)+
                };
                formatter.write_str(value)
            }
        }

        impl FromStr for $enum {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(format!("invalid {} value: {value}", stringify!($enum))),
                }
            }
        }
    };
}

enum_strings!(WorkMode {
    DirectMerge => "direct_merge",
    PullRequest => "pull_request",
});

enum_strings!(IntegrationPlatform {
    Github => "github",
    Gitea => "gitea",
});

enum_strings!(AgentStatus {
    Idle => "idle",
    Busy => "busy",
    Error => "error",
    Offline => "offline",
});

enum_strings!(WorkspaceStatus {
    Creating => "creating",
    Ready => "ready",
    Error => "error",
    Cleaning => "cleaning",
    Cleaned => "cleaned",
});

enum_strings!(DaemonStatus {
    Online => "online",
    Offline => "offline",
});

enum_strings!(RuntimeStatus {
    Ready => "ready",
    Degraded => "degraded",
    Offline => "offline",
});

enum_strings!(ExecutionStatus {
    Running => "running",
    Completed => "completed",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentChatMessageAuthorType {
    User => "user",
    Agent => "agent",
    System => "system",
    Handoff => "handoff",
});

enum_strings!(AgentChatMessageStatus {
    Complete => "complete",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentChatTurnState {
    Queued => "queued",
    Leased => "leased",
    AwaitingInput => "awaiting_input",
    RetryWait => "retry_wait",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(AgentHandoffStatus {
    Pending => "pending",
    Delivered => "delivered",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(MemoryKind {
    Observation => "observation",
    Decision => "decision",
    Handoff => "handoff",
    Failure => "failure",
    ReviewResult => "review_result",
    ExecutionSummary => "execution_summary",
    Comment => "comment",
    Transition => "transition",
    Artifact => "artifact",
    Lesson => "lesson",
    ContextPack => "context_pack",
});

enum_strings!(MemorySourceType {
    Execution => "execution",
    Review => "review",
    Comment => "comment",
    Transition => "transition",
});

enum_strings!(MemoryConfidence {
    Confirmed => "confirmed",
    Partial => "partial",
    Unconfirmed => "unconfirmed",
});

enum_strings!(StopReason {
    UserCancelled => "user_cancelled",
    TaskCancelled => "task_cancelled",
    RoleReassigned => "role_reassigned",
    GracefulShutdown => "graceful_shutdown",
    CrashRecovery => "crash_recovery",
    AgentTimeout => "agent_timeout",
    ExecutionStalled => "execution_stalled",
    DaemonDisconnected => "daemon_disconnected",
    ExecutorCancelled => "executor_cancelled",
    ExecutorFailed => "executor_failed",
    LegacyUnknown => "legacy_unknown",
});

enum_strings!(ResumePolicy {
    Auto => "auto",
    Manual => "manual",
    None => "none",
});

enum_strings!(ReviewStatus {
    Running => "running",
    AwaitingHuman => "awaiting_human",
    Passed => "passed",
    Failed => "failed",
    Cancelled => "cancelled",
});

enum_strings!(ProjectHookRunStatus {
    Queued => "queued",
    Running => "running",
    Dispatched => "dispatched",
    Skipped => "skipped",
    Failed => "failed",
    Completed => "completed",
});

enum_strings!(CommentAuthorType {
    User => "user",
    Agent => "agent",
    System => "system",
});

enum_strings!(TerminalSessionStatus {
    Starting => "starting",
    Running => "running",
    Exited => "exited",
    Terminated => "terminated",
    TimedOut => "timed_out",
    Orphaned => "orphaned",
    CleanupTerminated => "cleanup_terminated",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssigneeKind {
    Agent,
    User,
}

enum_strings!(AssigneeKind {
    Agent => "agent",
    User => "user",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRoleAssignment {
    pub id: String,
    pub task_id: String,
    pub role_name: String,
    pub assignee_type: Option<AssigneeKind>,
    pub assignee_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionLog {
    pub id: String,
    pub task_id: String,
    pub from_state: String,
    pub to_state: String,
    pub trigger_name: Option<String>,
    pub triggered_by: String,
    pub trigger_reason: String,
    pub hook_results_json: Option<String>,
    pub rejection: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRoleAssignment {
    pub id: String,
    pub task_id: String,
    pub role_name: String,
    pub assignee_type: Option<AssigneeKind>,
    pub assignee_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTransitionLog {
    pub id: String,
    pub task_id: String,
    pub from_state: String,
    pub to_state: String,
    pub trigger_name: Option<String>,
    pub triggered_by: String,
    pub trigger_reason: String,
    pub hook_results_json: Option<String>,
    pub rejection: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionUsage {
    pub id: String,
    pub execution_id: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: String,
    pub email: String,
    pub password_hash: String,
    pub display_name: Option<String>,
    pub is_admin: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshToken {
    pub id: String,
    pub user_id: String,
    pub token_hash: String,
    pub family_id: String,
    pub expires_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthClient {
    pub id: String,
    pub client_id: String,
    pub client_name: Option<String>,
    pub redirect_uris_json: String,
    pub token_endpoint_auth_method: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOAuthClient {
    pub id: String,
    pub client_id: String,
    pub client_name: Option<String>,
    pub redirect_uris_json: String,
    pub token_endpoint_auth_method: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthAuthorizationCode {
    pub id: String,
    pub code_hash: String,
    pub user_id: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub code_challenge_method: String,
    pub resource: String,
    pub scopes: String,
    pub expires_at: String,
    pub consumed_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOAuthAuthorizationCode {
    pub id: String,
    pub code_hash: String,
    pub user_id: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub code_challenge_method: String,
    pub resource: String,
    pub scopes: String,
    pub expires_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthRefreshToken {
    pub id: String,
    pub token_hash: String,
    pub family_id: String,
    pub user_id: String,
    pub client_id: String,
    pub resource: String,
    pub scopes: String,
    pub expires_at: String,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateOAuthRefreshToken {
    pub id: String,
    pub token_hash: String,
    pub family_id: String,
    pub user_id: String,
    pub client_id: String,
    pub resource: String,
    pub scopes: String,
    pub expires_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalAccessToken {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub token_hash: String,
    pub prefix: String,
    pub scopes: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePersonalAccessToken {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub token_hash: String,
    pub prefix: String,
    pub scopes: String,
    pub expires_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMember {
    pub id: String,
    pub project_id: String,
    pub user_id: String,
    pub role: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMember {
    pub id: String,
    pub project_id: String,
    pub user_id: String,
    pub role: String,
    pub created_at: String,
    pub updated_at: String,
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
