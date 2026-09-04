use crate::{
    canonical_attention_incident_digest, new_uuid_v4, AccountMainAgentBinding,
    AccountMainAgentBindingRepo, AdmitAgentChatTurn, AdmitAgentHandoff, AdmittedAgentChatTurn,
    AdmittedAgentHandoff, Agent, AgentAction, AgentActionApproval, AgentActionExecution,
    AgentActionListQuery, AgentActionRepo, AgentChat, AgentChatInstructionRevision,
    AgentChatMessage, AgentChatMessageAuthorType, AgentChatMessageListQuery, AgentChatMessageRepo,
    AgentChatRepo, AgentChatSourceRef, AgentChatTransactionRepo, AgentChatTurnJob,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentCommitment, AgentCommitmentEvidence,
    AgentCommitmentLifecycle, AgentCommitmentListQuery, AgentCommitmentRepo, AgentCommitmentStatus,
    AgentCommitmentTransfer, AgentHandoff, AgentHandoffRepo, AgentInboxItem, AgentInboxListQuery,
    AgentInboxRepo, AgentInquiry, AgentInquiryRepo, AgentListQuery, AgentProfile, AgentProfileRepo,
    AgentQuestion, AgentQuestionListQuery, AgentRepo, AgentStatus, AgentTaskListQuery,
    AgentTokenBreakdown, AgentWakeDisposition, AgentWakeDispositionKind, AgentWakeDispositionRepo,
    AnswerAgentQuestion, AppliedProjectExecutionSetupCommand, ApplyProjectExecutionSetupCommand,
    AttentionConsumerHealth, AttentionListQuery, AttentionProjection, AttentionRepo,
    CancelAgentChatTurn, CiStepStats, ClaimDomainEvents, ClaimExecutionLease, ClaimTask,
    ClaimedTask, CommandReceiptRepo, CompleteAgentChatControlTransfer, CompleteAgentChatTurn,
    CompleteAgentCommitment, CompleteAgentInquiry, CompleteClaimedWake, CompleteDomainEvent,
    CompletedAgentChatTurn, CreateAccountMainAgentBinding, CreateAgent, CreateAgentAction,
    CreateAgentActionApproval, CreateAgentActionExecution, CreateAgentChat, CreateAgentChatMessage,
    CreateAgentChatTurnJob, CreateAgentCommitment, CreateAgentCommitmentEvidence,
    CreateAgentHandoff, CreateAgentIdentity, CreateAgentInboxItem, CreateAgentInquiry,
    CreateAgentProfile, CreateAgentQuestion, CreateAgentWakeDisposition, CreateAttentionProjection,
    CreateDomainEvent, CreateExecution, CreateNotification, CreatePrMetadata,
    CreatePrProviderConfig, CreateProject, CreateProjectAdmissionReceipt,
    CreateProjectAgentBinding, CreateProjectHookRun, CreateProjectIntegration,
    CreateProjectMediaAsset, CreateProjectMediaAttachment, CreateProjectMediaAttachmentMutation,
    CreateProjectProvisioningError, CreateProjectProvisioningOperation,
    CreateProjectReleaseMediaPin, CreateRepo, CreateReview, CreateRuntime, CreateSkill, CreateTask,
    CreateTaskComment, CreateTaskExternalLink, CreateTaskMedia, CreateTerminalSession,
    CreateWorkspace, CreateWorkspaceLease, CurrentProjectBindingAuthority, Daemon, DaemonRepo,
    DbError, DomainEvent, DomainEventRepo, EventConsumerCursor, EventConsumerCutover, Execution,
    ExecutionLeaseDisposition, ExecutionLeaseMutation, ExecutionProgressWarningOutcome,
    ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome, ExecutionUsage, ExecutionUsageRepo,
    ExpectedAttentionSnapshot, ExternalLinkRepo, FailAgentChatTurn, IntegrationRepo, MediaAsset,
    ModelTokenBreakdown, Notification, NotificationListQuery, NotificationRepo, Page, PageRequest,
    ParkAgentChatTurn, PrMetadata, PrMetadataRepo, PrProviderConfig, PrProviderConfigRepo, Project,
    ProjectAdmissionReceipt, ProjectAdmissionReceiptRepo, ProjectAgentBinding,
    ProjectAgentBindingRepo, ProjectAnalyticsRepo, ProjectBindingCommandRepo,
    ProjectExecutionSetupCommandRepo, ProjectHookRun, ProjectHookRunRepo, ProjectHookRunStatus,
    ProjectIntegration, ProjectMediaAttachment, ProjectMediaTombstone,
    ProjectProvisioningCheckpoint, ProjectProvisioningError, ProjectProvisioningOperation,
    ProjectProvisioningRepo, ProjectReleaseMediaPin, ProjectRepo, ProjectReviewSummary,
    ProjectTokenStats, RecordExecutionProgress, RecordExecutionProgressWarning,
    RenewExecutionLease, ReplaceAccountMainAgentBinding, ReplaceProjectAgentBinding, Repo,
    RepoRepo, Result, RetryAgentWakeDisposition, Review, ReviewRepo, ReviewStatus, Runtime,
    RuntimeListQuery, RuntimeRepo, SelectAgentProfile, SetProjectAgentBindingCommand,
    SharedMediaRepo, Skill, SkillRepo, SoftDeleteProjectMediaAttachmentMutation, SortBy, SortOrder,
    SurfaceTokenBreakdown, Task, TaskComment, TaskCommentRepo, TaskDependencyRepo,
    TaskExternalLink, TaskListQuery, TaskMedia, TaskMediaRepo, TaskRepo, TaskUsageSummary,
    TerminalSession, TerminalSessionRepo, TerminalSessionStatus, TerminalizeExecution,
    TransferAgentCommitment, UpdateAgent, UpdateAgentAction, UpdateAgentChat,
    UpdateAgentChatTurnJob, UpdateAgentCommitment, UpdateAgentInboxItem, UpdateAttentionLifecycle,
    UpdateDaemonReport, UpdateExecution, UpdatePrMetadata, UpdatePrProviderConfig, UpdateProject,
    UpdateProjectHookRun, UpdateProjectIntegration, UpdateProjectProvisioningOperation, UpdateRepo,
    UpdateSkill, UpdateTask, UpdateTaskStatus, UpdateTerminalSessionStatus,
    UpsertAttentionConsumerHealth, UpsertDaemon, UpsertExecutionUsage,
    UpsertProjectProvisioningCheckpoint, Workspace, WorkspaceLease, WorkspaceLeaseRepo,
    WorkspaceRepo, WorkspaceStatus,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use std::str::FromStr;

mod action;
mod agent;
mod agent_chat;
pub use agent_chat::supported_main_baseline_revision;
mod agent_chat_topic;
mod agent_inquiry;
mod agent_wake;
mod analytics;
mod attention;
mod command_finalization;
mod command_receipt;
mod commitment;
mod daemon;
mod domain_event;
mod embedded_agent;
mod execution;
mod execution_usage;
mod external_link;
mod inbox;
mod integration;
mod lcm;
mod memory;
mod notification;
mod oauth_authorization_code;
mod oauth_client;
mod oauth_refresh_token;
mod orchestration;
mod personal_access_token;
mod pr_metadata;
mod pr_provider_config;
mod project;
mod project_execution_setup;
mod project_hook_run;
mod project_member;
mod project_provisioning;
mod provider_authorization;
mod repo;
mod review;
mod runtime;
mod shared_media;
mod skill;
mod system_setting;
mod task;
mod task_adaptive;
mod task_comment;
mod task_dependency;
mod task_media;
mod task_move;
mod task_terminal_session;
mod user_auth;
mod workflow;
mod workspace;
mod workspace_lease;

#[derive(Debug, Clone)]
pub struct SqliteDb {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    offset: i64,
}

impl SqliteDb {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

fn decode_offset(cursor: &Option<String>) -> Result<i64> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| DbError::InvalidCursor)?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| DbError::InvalidCursor)?;
    if cursor.offset < 0 {
        return Err(DbError::InvalidCursor);
    }
    Ok(cursor.offset)
}

fn encode_offset(offset: i64) -> Result<String> {
    let bytes = serde_json::to_vec(&Cursor { offset }).map_err(|_| DbError::InvalidCursor)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn order_clause(page: &PageRequest) -> &'static str {
    order_clause_for(page, true)
}

fn order_clause_without_priority(page: &PageRequest) -> &'static str {
    order_clause_for(page, false)
}

fn order_clause_for(page: &PageRequest, supports_priority: bool) -> &'static str {
    match (&page.sort_by, &page.sort_order) {
        (SortBy::CreatedAt, SortOrder::Asc) => "created_at ASC, id ASC",
        (SortBy::CreatedAt, SortOrder::Desc) => "created_at DESC, id DESC",
        (SortBy::UpdatedAt, SortOrder::Asc) => "updated_at ASC, id ASC",
        (SortBy::UpdatedAt, SortOrder::Desc) => "updated_at DESC, id DESC",
        (SortBy::Priority, SortOrder::Asc) if supports_priority => "priority ASC, id ASC",
        (SortBy::Priority, SortOrder::Desc) if supports_priority => "priority DESC, id DESC",
        (SortBy::Priority, SortOrder::Asc) => "created_at ASC, id ASC",
        (SortBy::Priority, SortOrder::Desc) => "created_at DESC, id DESC",
        (SortBy::BoardPosition, SortOrder::Asc) => "board_position ASC, created_at ASC, id ASC",
        (SortBy::BoardPosition, SortOrder::Desc) => "board_position DESC, created_at DESC, id DESC",
        (SortBy::Title, SortOrder::Asc) => "title ASC, id ASC",
        (SortBy::Title, SortOrder::Desc) => "title DESC, id DESC",
        (SortBy::Status, SortOrder::Asc) => "status ASC, id ASC",
        (SortBy::Status, SortOrder::Desc) => "status DESC, id DESC",
        (SortBy::Agent, SortOrder::Asc) => {
            "(SELECT assignee_id FROM task_role_assignment WHERE task_id = task.id AND role_name = 'coder' ORDER BY assignee_id ASC LIMIT 1) ASC, id ASC"
        }
        (SortBy::Agent, SortOrder::Desc) => {
            "(SELECT assignee_id FROM task_role_assignment WHERE task_id = task.id AND role_name = 'coder' ORDER BY assignee_id DESC LIMIT 1) DESC, id DESC"
        }
        (SortBy::TaskType, SortOrder::Asc) => "task_type ASC, id ASC",
        (SortBy::TaskType, SortOrder::Desc) => "task_type DESC, id DESC",
        (SortBy::Id, SortOrder::Asc) => "id ASC",
        (SortBy::Id, SortOrder::Desc) => "id DESC",
    }
}

const TASK_COLUMNS: &str = "id, project_id, repo_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, is_automation, priority, board_position, subtask_order, task_state_config, merge_config, metadata_json, plan, error_annotation, blocked_json, failed_json, entry_barrier_json, review_passed_at, archived_at, deleted_at, version, created_at, updated_at";
const PROJECT_COLUMNS: &str = "id, name, settings, workflow_definition, workflow_template_name, primary_repo_id, paused_at, system_pause_reason, owner_id, project_hooks_json, project_work_epoch, charter_status, charter_setup_required, current_charter_id, current_charter_revision_id, current_charter_version, primary_milestone_id, version, created_at, updated_at";

fn limit(page: &PageRequest) -> i64 {
    page.limit.clamp(1, 500)
}

fn page_from_items<T>(
    mut items: Vec<T>,
    page: &PageRequest,
    offset: i64,
    total_count: Option<i64>,
) -> Result<Page<T>> {
    let limit = limit(page) as usize;
    let has_next = items.len() > limit;
    if has_next {
        items.truncate(limit);
    }
    let next_cursor = if has_next {
        Some(encode_offset(offset + limit as i64)?)
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        total_count,
    })
}

fn parse_enum<T: FromStr<Err = String>>(value: String) -> Result<T> {
    value.parse().map_err(|_| DbError::InvalidTransition)
}

fn check_error(error: sqlx::Error) -> DbError {
    if let sqlx::Error::Database(database_error) = &error {
        if database_error
            .message()
            .to_ascii_lowercase()
            .contains("check constraint failed")
        {
            return DbError::Check(database_error.message().to_owned());
        }
    }
    error.into()
}

fn map_project(row: SqliteRow) -> Result<Project> {
    Ok(Project {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        settings: row.try_get("settings")?,
        workflow_definition: row.try_get("workflow_definition")?,
        workflow_template_name: row.try_get("workflow_template_name")?,
        primary_repo_id: row.try_get("primary_repo_id")?,
        paused_at: row.try_get("paused_at")?,
        system_pause_reason: row.try_get("system_pause_reason")?,
        owner_id: row.try_get("owner_id")?,
        project_hooks_json: row.try_get("project_hooks_json")?,
        project_work_epoch: row.try_get("project_work_epoch")?,
        charter_status: row.try_get("charter_status")?,
        charter_setup_required: row.try_get::<i64, _>("charter_setup_required")? != 0,
        current_charter_id: row.try_get("current_charter_id")?,
        current_charter_revision_id: row.try_get("current_charter_revision_id")?,
        current_charter_version: row.try_get("current_charter_version")?,
        primary_milestone_id: row.try_get("primary_milestone_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_repo(row: SqliteRow) -> Result<Repo> {
    Ok(Repo {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        remote_url: row.try_get("remote_url")?,
        local_path: row.try_get("local_path")?,
        work_mode: parse_enum(row.try_get::<String, _>("work_mode")?)?,
        default_branch: row.try_get("default_branch")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_pr_provider_config(row: SqliteRow) -> Result<PrProviderConfig> {
    Ok(PrProviderConfig {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        provider_type: row.try_get("provider_type")?,
        base_url: row.try_get("base_url")?,
        polling_interval_seconds: row.try_get("polling_interval_seconds")?,
        token_secret_ref: row.try_get("token_secret_ref")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_pr_metadata(row: SqliteRow) -> Result<PrMetadata> {
    Ok(PrMetadata {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        provider_type: row.try_get("provider_type")?,
        provider_pr_id: row.try_get("provider_pr_id")?,
        pr_url: row.try_get("pr_url")?,
        source_branch: row.try_get("source_branch")?,
        target_branch: row.try_get("target_branch")?,
        pr_state: row.try_get("pr_state")?,
        merge_status: row.try_get("merge_status")?,
        last_synced_at: row.try_get("last_synced_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent(row: SqliteRow) -> Result<Agent> {
    Ok(Agent {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        profile_id: row.try_get("profile_id")?,
        backend_kind: row.try_get("backend_kind")?,
        executor_type: row.try_get("executor_type")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        reasoning_effort: row.try_get("reasoning_effort")?,
        permission_policy: row.try_get("permission_policy")?,
        prompt_template: row.try_get("prompt_template")?,
        capabilities_json: row.try_get("capabilities_json")?,
        tool_policy_json: row.try_get("tool_policy_json")?,
        config_json: row.try_get("config_json")?,
        credential_ref: row.try_get("credential_ref")?,
        daemon_id: row.try_get("daemon_id")?,
        max_concurrent_tasks: row.try_get("max_concurrent_tasks")?,
        heartbeat_interval_seconds: row.try_get("heartbeat_interval_seconds")?,
        max_missed_heartbeats: row.try_get("max_missed_heartbeats")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        is_default: row.try_get::<i64, _>("is_default")? != 0,
        paused: row.try_get::<i64, _>("paused")? != 0,
        owner_id: row.try_get("owner_id")?,
        visibility: row.try_get::<String, _>("visibility")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_profile(row: SqliteRow) -> Result<AgentProfile> {
    Ok(AgentProfile {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        backend_kind: row.try_get("backend_kind")?,
        executor_type: row.try_get("executor_type")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        reasoning_effort: row.try_get("reasoning_effort")?,
        permission_policy: row.try_get("permission_policy")?,
        prompt_template: row.try_get("prompt_template")?,
        capabilities_json: row.try_get("capabilities_json")?,
        tool_policy_json: row.try_get("tool_policy_json")?,
        config_json: row.try_get("config_json")?,
        credential_ref: row.try_get("credential_ref")?,
        daemon_id: row.try_get("daemon_id")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_workspace(row: SqliteRow) -> Result<Workspace> {
    Ok(Workspace {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        repo_id: row.try_get("repo_id")?,
        worktree_path: row.try_get("worktree_path")?,
        branch: row.try_get("branch")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        before_sha: row.try_get("before_sha")?,
        cleanup_after: row.try_get("cleanup_after")?,
        error: row.try_get("error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_daemon(row: SqliteRow) -> Result<Daemon> {
    Ok(Daemon {
        id: row.try_get("id")?,
        machine_id: row.try_get("machine_id")?,
        hostname: row.try_get("hostname")?,
        os: row.try_get("os")?,
        arch: row.try_get("arch")?,
        agent_version: row.try_get("agent_version")?,
        labels_json: row.try_get("labels_json")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        last_report_at: row.try_get("last_report_at")?,
        registration_token_hash: row.try_get("registration_token_hash")?,
        detected_clis_json: row.try_get("detected_clis_json")?,
        owner_id: row.try_get("owner_id")?,
        visibility: row.try_get::<String, _>("visibility")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_runtime(row: SqliteRow) -> Result<Runtime> {
    Ok(Runtime {
        id: row.try_get("id")?,
        daemon_id: row.try_get("daemon_id")?,
        kind: row.try_get("kind")?,
        workspace_root: row.try_get("workspace_root")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        labels_json: row.try_get("labels_json")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_skill(row: SqliteRow) -> Result<Skill> {
    Ok(Skill {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        name: row.try_get("name")?,
        content: row.try_get("content")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_task(row: SqliteRow) -> Result<Task> {
    Ok(Task {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        repo_id: row.try_get("repo_id")?,
        parent_task_id: row.try_get("parent_task_id")?,
        assignee_type: row.try_get("assignee_type")?,
        assignee_id: row.try_get("assignee_id")?,
        title: row.try_get("title")?,
        description: row.try_get("description")?,
        task_type: row.try_get("task_type")?,
        status: row.try_get("status")?,
        is_automation: row.try_get::<i64, _>("is_automation")? != 0,
        priority: row.try_get("priority")?,
        board_position: row.try_get("board_position")?,
        subtask_order: row.try_get("subtask_order")?,
        task_state_config: row.try_get("task_state_config")?,
        merge_config: row.try_get("merge_config")?,
        metadata_json: row.try_get("metadata_json")?,
        plan: row.try_get("plan")?,
        error_annotation: row.try_get("error_annotation")?,
        blocked_json: row.try_get("blocked_json")?,
        failed_json: row.try_get("failed_json")?,
        entry_barrier_json: row.try_get("entry_barrier_json")?,
        review_passed_at: row.try_get("review_passed_at")?,
        archived_at: row.try_get("archived_at")?,
        deleted_at: row.try_get("deleted_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_execution(row: SqliteRow) -> Result<Execution> {
    Ok(Execution {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        agent_id: row.try_get("agent_id")?,
        role: row.try_get::<String, _>("role")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        stop_reason: row
            .try_get::<Option<String>, _>("stop_reason")?
            .map(parse_enum)
            .transpose()?,
        stopped_by: row.try_get("stopped_by")?,
        resume_policy: row
            .try_get::<Option<String>, _>("resume_policy")?
            .map(parse_enum)
            .transpose()?,
        stopped_at: row.try_get("stopped_at")?,
        parent_execution_id: row.try_get("parent_execution_id")?,
        agent_session_id: row.try_get("agent_session_id")?,
        agent_message_id: row.try_get("agent_message_id")?,
        last_activity_at: row.try_get("last_activity_at")?,
        prompt: row.try_get("prompt")?,
        summary: row.try_get("summary")?,
        logs_path: row.try_get("logs_path")?,
        before_sha: row.try_get("before_sha")?,
        after_sha: row.try_get("after_sha")?,
        error: row.try_get("error")?,
        executor_config_snapshot_json: row.try_get("executor_config_snapshot_json")?,
        workspace_id: row.try_get("workspace_id")?,
        execution_version: row.try_get("execution_version")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        hard_deadline_at: row.try_get("hard_deadline_at")?,
        last_heartbeat_at: row.try_get("last_heartbeat_at")?,
        last_progress_at: row.try_get("last_progress_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_execution_usage(row: SqliteRow) -> Result<ExecutionUsage> {
    Ok(ExecutionUsage {
        id: row.try_get("id")?,
        execution_id: row.try_get("execution_id")?,
        provider: row.try_get("provider")?,
        model: row.try_get("model")?,
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        cache_read_tokens: row.try_get("cache_read_tokens")?,
        cache_write_tokens: row.try_get("cache_write_tokens")?,
        cost_usd: row.try_get("cost_usd")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_review(row: SqliteRow) -> Result<Review> {
    Ok(Review {
        id: row.get("id"),
        task_id: row.get("task_id"),
        execution_id: row.get("execution_id"),
        attempt_number: row.get("attempt_number"),
        status: parse_enum(row.get::<String, _>("status"))?,
        step_results_json: row.get("step_results_json"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_task_comment(row: SqliteRow) -> Result<TaskComment> {
    Ok(TaskComment {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        author_type: parse_enum(row.try_get::<String, _>("author_type")?)?,
        author_id: row.try_get("author_id")?,
        author_name: row.try_get("author_name")?,
        content: row.try_get("content")?,
        execution_id: row.try_get("execution_id")?,
        role: row.try_get("role")?,
        worklog_kind: row.try_get("worklog_kind")?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_task_media(row: SqliteRow) -> Result<TaskMedia> {
    Ok(TaskMedia {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        display_filename: row.try_get("display_filename")?,
        content_type: row.try_get("content_type")?,
        byte_size: row.try_get("byte_size")?,
        storage_key: row.try_get("storage_key")?,
        author_type: parse_enum(row.try_get::<String, _>("author_type")?)?,
        author_id: row.try_get("author_id")?,
        author_name: row.try_get("author_name")?,
        created_at: row.try_get("created_at")?,
        deleted_at: row.try_get("deleted_at")?,
    })
}

fn map_terminal_session(row: SqliteRow) -> Result<TerminalSession> {
    Ok(TerminalSession {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        workspace_id: row.try_get("workspace_id")?,
        daemon_id: row.try_get("daemon_id")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        rows: row.try_get("rows")?,
        cols: row.try_get("cols")?,
        pid: row.try_get("pid")?,
        exit_code: row.try_get("exit_code")?,
        exit_signal: row.try_get("exit_signal")?,
        exit_reason: row.try_get("exit_reason")?,
        created_by_user_id: row.try_get("created_by_user_id")?,
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        last_activity_at: row.try_get("last_activity_at")?,
        ended_at: row.try_get("ended_at")?,
        version: row.try_get("version")?,
    })
}

fn map_notification(row: SqliteRow) -> Result<Notification> {
    Ok(Notification {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        task_id: row.try_get("task_id")?,
        event_type: row.try_get("event_type")?,
        title: row.try_get("title")?,
        body: row.try_get("body")?,
        read: row.try_get::<i64, _>("read")? != 0,
        created_at: row.try_get("created_at")?,
    })
}

fn review_transition_allowed(from: &ReviewStatus, to: &ReviewStatus) -> bool {
    matches!(from, ReviewStatus::Running | ReviewStatus::AwaitingHuman) || from == to
}

async fn total_count(pool: &SqlitePool, sql: &str) -> Result<Option<i64>> {
    Ok(Some(
        sqlx::query_scalar::<_, i64>(sql).fetch_one(pool).await?,
    ))
}

impl SqliteDb {
    async fn get_task_required(&self, id: &str, include_deleted: bool) -> Result<Task> {
        TaskRepo::get_by_id(self, id, include_deleted)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_execution_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
    ) -> Result<Execution> {
        // The service performs a read-only admission check before preparing a
        // workspace. Recheck the authoritative Charter link in the same
        // transaction as the execution INSERT so a Charter supersession
        // racing that read cannot mint a stale Running execution.
        // The service removes a newly prepared workspace when this guard
        // rejects the execution, so no fresh lease remains behind.
        // Legacy/unverified Projects intentionally bypass this guard.
        if input.status == ExecutionStatus::Running && input.workspace_id.is_some() {
            Self::ensure_execution_admission_in_tx(transaction, &input.task_id).await?;
        }
        let stop_reason = input.stop_reason.as_ref().map(ToString::to_string);
        let resume_policy = input.resume_policy.as_ref().map(ToString::to_string);
        let prompt = input.summary.as_deref();
        sqlx::query(
            "INSERT INTO execution (id, task_id, agent_id, role, status, stop_reason, stopped_by, resume_policy, stopped_at, parent_execution_id, agent_session_id, agent_message_id, last_activity_at, prompt, summary, logs_path, before_sha, after_sha, error, executor_config_snapshot_json, workspace_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(input.agent_id.as_deref())
        .bind(&input.role)
        .bind(input.status.to_string())
        .bind(stop_reason.as_deref())
        .bind(input.stopped_by.as_deref())
        .bind(resume_policy.as_deref())
        .bind(input.stopped_at.as_deref())
        .bind(input.parent_execution_id.as_deref())
        .bind(input.agent_session_id.as_deref())
        .bind(input.agent_message_id.as_deref())
        .bind(input.last_activity_at.as_deref())
        .bind(prompt)
        .bind(input.summary.as_deref())
        .bind(input.logs_path.as_deref())
        .bind(input.before_sha.as_deref())
        .bind(input.after_sha.as_deref())
        .bind(input.error.as_deref())
        .bind(input.executor_config_snapshot_json.as_deref())
        .bind(input.workspace_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut **transaction)
        .await?;

        let row = sqlx::query("SELECT * FROM execution WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await?;
        map_execution(row)
    }

    /// Re-check the current approved Charter in the transaction that mutates
    /// Task/Execution state. The service-level admission query is intentionally
    /// only an early side-effect filter; it cannot be the authority because a
    /// Charter may be superseded between that query and claim/launch.
    async fn ensure_execution_admission_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<()> {
        let blocked: Option<i64> = sqlx::query_scalar(
            "SELECT CASE WHEN p.charter_status = 'charter_backed'
                                  AND p.charter_setup_required = 0
                                  AND t.repo_id IS NOT NULL
                                  AND NOT (
                                      COALESCE(g.runnable, 0) = 1
                                      AND g.charter_revision_id = p.current_charter_revision_id
                                  )
                             THEN 1 ELSE 0 END
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN project_task_governance g ON g.task_id = t.id
             WHERE t.id = ?",
        )
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if blocked == Some(1) {
            return Err(DbError::InvalidTransition);
        }
        Ok(())
    }

    async fn unsatisfied_dependencies_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ? AND depends_on_id NOT IN (SELECT id FROM task WHERE status = 'done')",
        )
        .bind(task_id)
        .fetch_all(&mut **transaction)
        .await?;
        Ok(rows)
    }
}
