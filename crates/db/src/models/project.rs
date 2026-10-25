//! Projects, provisioning, integrations, and repository locations.

use super::*;

/// Hourly wake-admission budget a Project Agent binding starts with. Zero
/// disables autonomous wakes entirely, so every binding-creation path must
/// use this default unless the user explicitly configured a budget.
pub const DEFAULT_PROJECT_AGENT_WAKE_BUDGET: i64 = 10;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub settings: String,
    pub workflow_definition: String,
    pub workflow_template_name: Option<String>,
    pub primary_repo_id: Option<String>,
    pub paused_at: Option<String>,
    /// Set alongside `paused_at` for a repository or environment auto-pause;
    /// `None` for a user's own pause. See migrations V128 and V202610010410.
    pub system_pause_reason: Option<String>,
    /// Durable detail and schedule for an environment-owned Project pause.
    pub environment_pause_json: Option<String>,
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

/// Project response inputs and the display revision from the same SELECT.
#[derive(Debug, Clone)]
pub struct ProjectSlotRead {
    pub project: Project,
    pub list_revision: i64,
}

/// Counts and revision fences read together by the batched slot statement.
#[derive(Debug)]
pub struct ProjectSlotCounts {
    pub project_id: String,
    pub project_version: i64,
    pub list_revision: i64,
    pub active: i64,
    pub parked: i64,
    pub queued: i64,
}

/// Durable, retryable work that reconciles a Project's repository and
/// Workspace-capable role setup.  The status is deliberately independent of
/// the Project Agent binding/chat state and optional plan traceability.
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

/// Atomic Project review-policy mutation plus durable command receipt.
/// The caller prepares the complete settings document so the transaction can
/// replace it under the same Project version guard as the event and receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyProjectReviewConfigCommand {
    pub project_id: String,
    pub expected_project_version: i64,
    pub settings: String,
    pub receipt: CreateCommandReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProjectReviewConfigCommand {
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
    pub remote_url: Option<String>,
    pub local_path: Option<String>,
    pub default_branch: String,
    pub created_at: String,
    pub updated_at: String,
}

pub fn normalize_repo_remote_url(remote_url: Option<String>) -> Option<String> {
    remote_url.filter(|value| !value.trim().is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoLocationOwnerKind {
    Server,
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoLocationKind {
    PrimaryCheckout,
    ManagedClone,
    SharedMount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoLocationStatus {
    Unverified,
    Ready,
    Unavailable,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoLocation {
    pub id: String,
    pub repo_id: String,
    pub owner_kind: RepoLocationOwnerKind,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
    pub path: String,
    pub kind: RepoLocationKind,
    pub is_default: bool,
    pub status: RepoLocationStatus,
    pub last_verified_at: Option<String>,
    pub last_error: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}
