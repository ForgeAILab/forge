use crate::ExecutionAdmission;
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
    AgentWakeDisposition, AgentWakeDispositionKind, AgentWakeDispositionRepo, AnswerAgentQuestion,
    AppliedProjectExecutionSetupCommand, ApplyProjectExecutionSetupCommand,
    AttentionConsumerHealth, AttentionListQuery, AttentionProjection, AttentionRepo,
    CancelAgentChatTurn, CancelAgentChatTurnWithUsage, CancelAgentInquiryWithUsage, CiStepStats,
    ClaimDomainEvents, ClaimExecutionLease, ClaimTask, ClaimedTask, CommandReceiptRepo,
    CompleteAgentChatControlTransfer, CompleteAgentChatControlTransferWithUsage,
    CompleteAgentChatTurn, CompleteAgentChatTurnWithUsage, CompleteAgentCommitment,
    CompleteAgentInquiry, CompleteAgentInquiryWithUsage, CompleteClaimedWake, CompleteDomainEvent,
    CompletedAgentChatTurn, CostCoverageReasonCode, CreateAccountMainAgentBinding, CreateAgent,
    CreateAgentAction, CreateAgentActionApproval, CreateAgentActionExecution, CreateAgentChat,
    CreateAgentChatMessage, CreateAgentChatTurnJob, CreateAgentCommitment,
    CreateAgentCommitmentEvidence, CreateAgentHandoff, CreateAgentIdentity, CreateAgentInboxItem,
    CreateAgentInquiry, CreateAgentProfile, CreateAgentQuestion, CreateAgentWakeDisposition,
    CreateAttentionProjection, CreateDomainEvent, CreateExecution, CreateNotification,
    CreatePrMetadata, CreatePrProviderConfig, CreateProject, CreateProjectAdmissionReceipt,
    CreateProjectAgentBinding, CreateProjectHookRun, CreateProjectIntegration,
    CreateProjectMediaAsset, CreateProjectMediaAttachment, CreateProjectMediaAttachmentMutation,
    CreateProjectProvisioningError, CreateProjectProvisioningOperation,
    CreateProjectReleaseMediaPin, CreateRepo, CreateReview, CreateRuntime, CreateSkill, CreateTask,
    CreateTaskComment, CreateTaskExternalLink, CreateTaskMedia, CreateTerminalSession,
    CreateUsageInvocation, CreateWorkspace, CreateWorkspaceLease, CurrentProjectBindingAuthority,
    Daemon, DaemonRepo, DbError, DomainEvent, DomainEventRepo, EventConsumerCursor,
    EventConsumerCutover, Execution, ExecutionLeaseDisposition, ExecutionLeaseMutation,
    ExecutionProgressWarningOutcome, ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome,
    ExecutionTerminalReceipt, ExpectedAttentionSnapshot, ExternalLinkRepo, FailAgentChatTurn,
    FailAgentChatTurnWithUsage, IntegrationRepo, MarkUsageInvocationPendingSettlement, MediaAsset,
    Notification, NotificationListQuery, NotificationRepo, Page, PageRequest, ParkAgentChatTurn,
    ParkAgentChatTurnWithUsage, PrMetadata, PrMetadataRepo, PrProviderConfig, PrProviderConfigRepo,
    Project, ProjectAdmissionReceipt, ProjectAdmissionReceiptRepo, ProjectAgentBinding,
    ProjectAgentBindingRepo, ProjectAnalyticsRepo, ProjectBindingCommandRepo, ProjectDeletionPaths,
    ProjectDeletionRepositoryPath, ProjectExecutionSetupCommandRepo, ProjectHookRun,
    ProjectHookRunRepo, ProjectHookRunStatus, ProjectIntegration, ProjectMediaAttachment,
    ProjectMediaTombstone, ProjectProvisioningCheckpoint, ProjectProvisioningError,
    ProjectProvisioningOperation, ProjectProvisioningRepo, ProjectReleaseMediaPin, ProjectRepo,
    ProjectReviewSummary, RecordExecutionProgress, RecordExecutionProgressWarning,
    RenewExecutionLease, ReplaceAccountMainAgentBinding, ReplaceProjectAgentBinding, Repo,
    RepoRepo, Result, RetryAgentWakeDisposition, Review, ReviewRepo, ReviewStatus, Runtime,
    RuntimeListQuery, RuntimeRepo, SelectAgentProfile, SetProjectAgentBindingCommand,
    SettleUsageInvocation, SharedMediaRepo, Skill, SkillRepo,
    SoftDeleteProjectMediaAttachmentMutation, SortBy, SortOrder, Task, TaskComment,
    TaskCommentRepo, TaskDependencyRepo, TaskExternalLink, TaskListQuery, TaskMedia, TaskMediaRepo,
    TaskMetadata, TaskMetadataMutation, TaskRepo, TerminalSession, TerminalSessionRepo,
    TerminalSessionStatus, TerminalizeExecution, TerminalizeExecutionWithLedger,
    TransferAgentCommitment, UpdateAgent, UpdateAgentAction, UpdateAgentChat,
    UpdateAgentChatTurnJob, UpdateAgentCommitment, UpdateAgentInboxItem, UpdateAttentionLifecycle,
    UpdateDaemonReport, UpdateExecution, UpdatePrMetadata, UpdatePrProviderConfig, UpdateProject,
    UpdateProjectHookRun, UpdateProjectIntegration, UpdateProjectProvisioningOperation, UpdateRepo,
    UpdateSkill, UpdateTask, UpdateTaskStatus, UpdateTerminalSessionStatus,
    UpsertAttentionConsumerHealth, UpsertDaemon, UpsertProjectProvisioningCheckpoint,
    UsageAnalyticsRepo, UsageCostKind, UsageEventProvenanceKind, UsageInvocationLifecycle,
    UsageLedgerRepo, UsageLedgerSettlement, UsageSurface, UsageTelemetryState, Workspace,
    WorkspaceLease, WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use crate::{
    AppliedProjectReviewConfigCommand, ApplyProjectReviewConfigCommand,
    ProjectReviewConfigCommandRepo,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use std::str::FromStr;

mod action;
mod agent;
mod agent_chat;
mod chat_ledger;
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
mod pricing;
mod project;
mod project_execution_setup;
mod project_hook_run;
mod project_member;
mod project_provisioning;
mod project_review_config;
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

const TASK_COLUMNS: &str = "id, project_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, is_automation, priority, board_position, subtask_order, task_state_config, merge_config, metadata_json, plan, error_annotation, blocked_json, failed_json, entry_barrier_json, review_passed_at, archived_at, deleted_at, version, created_at, updated_at";
const PROJECT_COLUMNS: &str = "id, name, settings, workflow_definition, workflow_template_name, primary_repo_id, paused_at, system_pause_reason, owner_id, project_hooks_json, project_work_epoch, charter_status, charter_setup_required, current_charter_id, current_charter_revision_id, current_charter_version, primary_milestone_id, version, created_at, updated_at";

/// Clear dispatch decisions that were derived from Project-level authority.
///
/// This helper deliberately accepts an existing `BEGIN IMMEDIATE` transaction
/// so callers can commit the authoritative Project/repository mutation and the
/// wake together.  Clearing the markers also bumps each affected Task's
/// `version`, forming a generation boundary that rejects an in-flight stale
/// disposition writer after the Project authority changes.
pub(crate) async fn wake_dispatch_for_project_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    updated_at: &str,
) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE task
         SET metadata_json = NULLIF(
                 json_remove(metadata_json, '$.dispatch_disposition', '$.deferred_dispatch'),
                 '{}'
             ),
             updated_at = ?,
             version = version + 1
         WHERE project_id = ?
           AND deleted_at IS NULL
           AND json_valid(metadata_json)
           AND (
               json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
               OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL
           )",
    )
    .bind(updated_at)
    .bind(project_id)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected())
}

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

fn map_review(row: SqliteRow) -> Result<Review> {
    Ok(Review {
        id: row.get("id"),
        task_id: row.get("task_id"),
        execution_id: row.get("execution_id"),
        reviewer_execution_id: row.get("reviewer_execution_id"),
        auditor_execution_id: row.get("auditor_execution_id"),
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

async fn latest_review_candidate_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    review_task_id: &str,
) -> Result<Option<SqliteRow>> {
    let review_task = sqlx::query("SELECT parent_task_id FROM task WHERE id = ?")
        .bind(review_task_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
    let review_task_parent: Option<String> = review_task.try_get("parent_task_id")?;
    let use_direct_children = if review_task_parent.is_none() {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM task
             WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(review_task_id)
        .fetch_one(&mut **transaction)
        .await?
            > 0
    } else {
        false
    };
    let candidate = if use_direct_children {
        sqlx::query(
            "SELECT e.id, e.task_id, e.role, e.status, candidate_task.parent_task_id
             FROM execution e
             JOIN task candidate_task ON candidate_task.id = e.task_id
             WHERE candidate_task.parent_task_id = ?
               AND candidate_task.deleted_at IS NULL
               AND e.status IN ('completed', 'running')
               AND e.role IN ('executor', 'coder', 'worker')
             ORDER BY e.created_at DESC, e.id DESC
             LIMIT 1",
        )
        .bind(review_task_id)
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        sqlx::query(
            "SELECT e.id, e.task_id, e.role, e.status, candidate_task.parent_task_id
             FROM execution e
             JOIN task candidate_task ON candidate_task.id = e.task_id
             WHERE e.task_id = ?
               AND candidate_task.deleted_at IS NULL
               AND e.status IN ('completed', 'running')
               AND e.role IN ('executor', 'coder', 'worker')
             ORDER BY e.created_at DESC, e.id DESC
             LIMIT 1",
        )
        .bind(review_task_id)
        .fetch_optional(&mut **transaction)
        .await?
    };
    Ok(candidate)
}

async fn validate_review_candidate_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    review_task_id: &str,
    candidate_execution_id: &str,
) -> Result<()> {
    let candidate = latest_review_candidate_in_tx(transaction, review_task_id)
        .await?
        .ok_or(DbError::VersionConflict)?;
    let current_candidate_id: String = candidate.try_get("id")?;
    let candidate_task_id: String = candidate.try_get("task_id")?;
    let candidate_parent_task_id: Option<String> = candidate.try_get("parent_task_id")?;
    let candidate_role: String = candidate.try_get("role")?;
    let candidate_status: String = candidate.try_get("status")?;
    if current_candidate_id != candidate_execution_id
        || (candidate_task_id != review_task_id
            && candidate_parent_task_id.as_deref() != Some(review_task_id))
        || !matches!(candidate_role.as_str(), "executor" | "coder" | "worker")
        || candidate_status != ExecutionStatus::Completed.to_string()
    {
        return Err(DbError::VersionConflict);
    }
    Ok(())
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
        admission: Option<&ExecutionAdmission>,
    ) -> Result<Execution> {
        if input.status == ExecutionStatus::Running {
            if let Some(admission) = admission {
                Self::ensure_task_execution_admission_in_tx(transaction, input, admission).await?;
            }
        }
        if input.status == ExecutionStatus::Running {
            let paused_project_id: Option<String> = sqlx::query_scalar(
                "SELECT p.id
                 FROM task t
                 JOIN project p ON p.id = t.project_id
                 WHERE t.id = ? AND p.paused_at IS NOT NULL",
            )
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(project_id) = paused_project_id {
                return Err(DbError::ProjectPaused { project_id });
            }
        }
        // The service performs a read-only admission check before preparing a
        // workspace. Recheck the authoritative Charter link in the same
        // transaction as the execution INSERT so a Charter supersession
        // racing that read cannot mint a stale Running execution.
        // The service removes a newly prepared workspace when this guard
        // rejects the execution, so no fresh lease remains behind.
        // Legacy/unverified Projects intentionally bypass this guard.
        if input.status == ExecutionStatus::Running {
            if let Some(workspace_id) = input.workspace_id.as_deref() {
                Self::ensure_execution_admission_in_tx(transaction, &input.task_id, workspace_id)
                    .await?;
            }
        }
        if input.status == ExecutionStatus::Running {
            // Child admission is rechecked under the same writer lock as the
            // execution insert. A root cancellation/review transition that
            // wins the race therefore prevents a stale child launcher from
            // minting a Running execution after the coordination boundary.
            let parent_row = sqlx::query(
                "SELECT parent.status,
                        parent.blocked_json,
                        parent.failed_json,
                        parent.error_annotation,
                        parent.entry_barrier_json,
                        project.workflow_definition
                 FROM task AS child
                 JOIN task AS parent ON parent.id = child.parent_task_id
                 JOIN project ON project.id = parent.project_id
                 WHERE child.id = ?
                   AND child.deleted_at IS NULL
                   AND parent.deleted_at IS NULL",
            )
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(parent_row) = parent_row {
                let parent_status: String = parent_row.try_get("status")?;
                let workflow_definition: String = parent_row.try_get("workflow_definition")?;
                let parsed_workflow =
                    serde_json::from_str::<api_types::WorkflowDefinition>(&workflow_definition)
                        .ok();
                let parent_state_blocks_execution = parsed_workflow
                    .as_ref()
                    .and_then(|workflow| {
                        workflow
                            .states
                            .iter()
                            .find(|state| state.name == parent_status)
                    })
                    .is_some_and(|state| {
                        matches!(
                            state.kind,
                            api_types::StateKind::Backlog | api_types::StateKind::Terminal
                        ) || state.canonical_phase == Some(api_types::CanonicalPhase::Review)
                    })
                    || parsed_workflow
                        .as_ref()
                        .and_then(|workflow| workflow.cancellation_state.as_deref())
                        == Some(parent_status.as_str())
                    || matches!(
                        parent_status.as_str(),
                        "backlog" | "review" | "merging" | "merge_failed" | "done" | "cancelled"
                    );
                let parent_has_blocker = parent_row
                    .try_get::<Option<String>, _>("blocked_json")?
                    .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("failed_json")?
                        .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("error_annotation")?
                        .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("entry_barrier_json")?
                        .is_some();
                if parent_state_blocks_execution || parent_has_blocker {
                    return Err(DbError::InvalidTransition);
                }
            }

            // `executor` is the historical transport alias for the canonical
            // implementation role. Re-execution/recovery may still carry the
            // alias, but it must observe the same assignment CAS as `coder`.
            let assignment_role =
                canonical_execution_role(Some(input.role.as_str())).unwrap_or(input.role.as_str());
            let role_assignment = sqlx::query(
                "SELECT assignee_type, assignee_id
                 FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
            )
            .bind(&input.task_id)
            .bind(assignment_role)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(role_assignment) = role_assignment {
                // An auditor runs under a separately selected Agent while the
                // reviewer assignment stays the authority, so its principal is
                // the reviewer execution's Agent -- which only the admission
                // snapshot names. Resolve that here, where the comparison
                // actually happens: demanding it for every auditor row
                // rejected an auditor execution created without an admission
                // as a bare version conflict, even with no assignment to
                // compare it against.
                let assignment_agent_id = if input.role == "auditor" {
                    let Some(reviewer_execution_id) = admission
                        .and_then(|admission| admission.expected_reviewer_execution_id.as_deref())
                    else {
                        return Err(DbError::Check(
                            "auditor execution requires the reviewer execution its admission \
                             selected before it can be matched to the reviewer assignment"
                                .to_owned(),
                        ));
                    };
                    Some(
                        sqlx::query_scalar::<_, String>(
                            "SELECT agent_id FROM execution
                         WHERE id = ? AND agent_id IS NOT NULL",
                        )
                        .bind(reviewer_execution_id)
                        .fetch_optional(&mut **transaction)
                        .await?
                        .ok_or(DbError::VersionConflict)?,
                    )
                } else {
                    input.agent_id.clone()
                };
                let assignee_type: Option<String> = role_assignment.try_get("assignee_type")?;
                let assignee_id: Option<String> = role_assignment.try_get("assignee_id")?;
                let matches_agent = assignee_type.as_deref() == Some("agent")
                    && assignee_id.as_deref() == assignment_agent_id.as_deref();
                let matches_user =
                    assignee_type.as_deref() == Some("user") && input.agent_id.is_none();
                if !matches_agent && !matches_user {
                    return Err(DbError::Check(format!(
                        "running execution principal does not match role assignment for {}",
                        assignment_role
                    )));
                }
            }
            if let Some(workspace_id) = input.workspace_id.as_deref() {
                let running_execution = sqlx::query_as::<_, (String, String)>(
                    "SELECT role, id FROM execution
                     WHERE workspace_id = ? AND status = 'running'
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                )
                .bind(workspace_id)
                .fetch_optional(&mut **transaction)
                .await?;
                if let Some((running_role, running_execution_id)) = running_execution {
                    // The public scope names the occupied resource slot, not
                    // the incoming workflow role. A Workspace is the shared
                    // repository slot; an interactive occupant keeps the
                    // dedicated interactive scope.
                    let scope = if running_role == "interactive" {
                        "interactive"
                    } else {
                        "repository"
                    };
                    return Err(DbError::ExecutionAlreadyRunning {
                        scope: scope.to_owned(),
                        execution_id: running_execution_id,
                    });
                }
            }

            // Capacity is an insertion invariant, not a dispatcher hint.
            // Recheck the selected identity's pause/current max, its running
            // Task executions, and any configured daemon session cap while
            // this BEGIN IMMEDIATE transaction still owns SQLite's writer lock.
            // This is intentionally outside the workspace branch so Task
            // claims, which may not have a Workspace row yet, use the same
            // authoritative rule as role/recovery launches.
            Self::ensure_agent_execution_capacity_in_tx(transaction, input, admission).await?;
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
        workspace_id: &str,
    ) -> Result<()> {
        let blocked: Option<i64> = sqlx::query_scalar(
            "SELECT CASE
                        WHEN p.primary_repo_id IS NULL
                          OR NOT EXISTS (
                              SELECT 1 FROM repo r
                              WHERE r.id = p.primary_repo_id
                                AND r.project_id = p.id
                          )
                          OR NOT EXISTS (
                              SELECT 1 FROM workspace w
                              WHERE w.id = ? AND w.repo_id = p.primary_repo_id
                          )
                        THEN 1
                        WHEN p.charter_status = 'charter_backed'
                          AND p.charter_setup_required = 0
                          AND (
                              p.current_charter_revision_id IS NULL
                              OR g.charter_revision_id IS NULL
                              OR g.charter_revision_id != p.current_charter_revision_id
                          )
                        THEN 1
                        ELSE 0
                    END
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN project_task_governance g
               ON g.task_id = t.id AND g.project_id = p.id
             WHERE t.id = ?",
        )
        .bind(workspace_id)
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if blocked == Some(1) {
            return Err(DbError::InvalidTransition);
        }
        Ok(())
    }

    /// Re-read the Task facts that selected a role execution while holding the
    /// same writer transaction that will insert the Running row.  The service
    /// reads are useful preflight filters, but only this compare-and-admit
    /// boundary can close the final-read-to-insert race.
    async fn ensure_task_execution_admission_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: &ExecutionAdmission,
    ) -> Result<()> {
        let task_id = &input.task_id;
        let row = sqlx::query(
            "SELECT t.version, t.status, t.parent_task_id, p.version AS project_version,
                    p.workflow_definition
             FROM task AS t
             JOIN project AS p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let actual_version: i64 = row.try_get("version")?;
        let actual_status: String = row.try_get("status")?;
        if actual_version != admission.expected_task_version
            || actual_status != admission.expected_task_status
        {
            return Err(DbError::VersionConflict);
        }
        if let Some(expected_project_version) = admission.expected_project_version {
            let actual_project_version: i64 = row.try_get("project_version")?;
            if actual_project_version != expected_project_version {
                return Err(DbError::VersionConflict);
            }
        }

        let parent_task_id: Option<String> = row.try_get("parent_task_id")?;
        let workflow_is_inherited =
            parent_task_id.is_some() && inherited_subtask_workflow_state(&actual_status);
        let workflow_definition: String = row.try_get("workflow_definition")?;
        let actual_workflow_definition =
            (!workflow_is_inherited).then_some(workflow_definition.as_str());
        let synthetic_ci_review = canonical_execution_role(Some(input.role.as_str()))
            == Some("reviewer")
            && input.agent_id.is_none()
            && admission.expected_effective_role.is_none()
            && admission.expected_assignment_id.is_none();
        if admission.expected_effective_role.is_none()
            && input.role != "interactive"
            && !synthetic_ci_review
        {
            return Err(DbError::VersionConflict);
        }
        // The workflow definition is pinned by whoever let the workflow choose
        // the role. An interactive execution is the user reaching into the
        // workspace directly, so it owes no pin -- and requiring one rejected
        // every interactive execution in a Project that has a workflow
        // definition, which is every Project, as a bare version conflict. A
        // pin an admission does carry is still compared.
        let pin_required = !workflow_is_inherited && input.role != "interactive";
        match admission.expected_workflow_definition.as_deref() {
            Some(expected) => {
                if Some(expected) != actual_workflow_definition {
                    return Err(DbError::VersionConflict);
                }
            }
            None => {
                if pin_required {
                    return Err(DbError::VersionConflict);
                }
            }
        }

        // Dependency edges are independent rows and do not advance Task
        // version. Re-read them under this same writer transaction so a
        // dependency added after service preflight cannot admit stale work.
        let unsatisfied_dependencies =
            Self::unsatisfied_dependencies_in_tx(transaction, task_id).await?;
        if !unsatisfied_dependencies.is_empty() {
            let agent_id = input.agent_id.as_deref().ok_or(DbError::DependencyGate)?;
            let context_holder_match = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                     SELECT 1
                     FROM execution
                     WHERE task_id IN (SELECT depends_on_id FROM task_dependency WHERE task_id = ?)
                       AND role = 'executor'
                       AND agent_id = ?
                 )",
            )
            .bind(task_id)
            .bind(agent_id)
            .fetch_one(&mut **transaction)
            .await?
                != 0;
            if !context_holder_match {
                return Err(DbError::DependencyGate);
            }
        }

        let Some(expected_role) = admission.expected_effective_role.as_deref() else {
            return Ok(());
        };
        if canonical_execution_role(Some(input.role.as_str()))
            != canonical_execution_role(Some(expected_role))
        {
            return Err(DbError::VersionConflict);
        }
        let actual_role = if workflow_is_inherited {
            inherited_subtask_workflow_role(&actual_status).map(str::to_owned)
        } else {
            let raw = workflow_definition.trim();
            if raw.is_empty() || raw == "{}" {
                default_workflow_role(&actual_status).map(str::to_owned)
            } else {
                let workflow = serde_json::from_str::<api_types::WorkflowDefinition>(raw)
                    .map_err(|_| DbError::VersionConflict)?;
                workflow
                    .states
                    .iter()
                    .find(|state| state.name == actual_status)
                    .and_then(|state| {
                        state.role.as_deref().or_else(|| {
                            (state.kind == api_types::StateKind::Active).then_some("assignee")
                        })
                    })
                    .map(str::to_owned)
            }
        };
        if canonical_execution_role(actual_role.as_deref())
            != canonical_execution_role(Some(expected_role))
        {
            return Err(DbError::VersionConflict);
        }

        // A role decision is inseparable from the principal assignment that
        // produced it.  Require the exact assignment row and its last-write
        // timestamp that the dispatcher observed; a missing, replaced, or
        // reassigned principal therefore fails closed inside the INSERT
        // transaction. `executor` remains the historical alias for coder and
        // `auditor` is a child of the reviewer assignment.
        let assignment_role =
            canonical_execution_role(Some(input.role.as_str())).unwrap_or(input.role.as_str());
        // Auditor work uses a separately selected Agent, while the reviewer
        // Task assignment remains the authority for the Review attempt. Bind
        // that assignment check to the reviewer execution recorded in the
        // admission snapshot instead of requiring reviewer_agent == auditor_agent.
        let assignment_agent_id = if input.role == "auditor" {
            let reviewer_execution_id = admission
                .expected_reviewer_execution_id
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            Some(
                sqlx::query_scalar::<_, String>(
                    "SELECT agent_id FROM execution
                 WHERE id = ? AND agent_id IS NOT NULL",
                )
                .bind(reviewer_execution_id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(DbError::VersionConflict)?,
            )
        } else {
            input.agent_id.clone()
        };
        let assignment = sqlx::query(
            "SELECT id, assignee_type, assignee_id, updated_at
             FROM task_role_assignment
             WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(assignment_role)
        .fetch_optional(&mut **transaction)
        .await?;
        // ReviewRunner's CI-only pass is a deliberately synthetic reviewer:
        // it has no Agent principal and therefore no assignment row to
        // compare. Keep this exception scoped to the canonical reviewer
        // role and an admission that explicitly has no selected assignment;
        // every Agent-backed launch still requires the exact assignment CAS.
        let allows_absent_ci_review = canonical_execution_role(Some(input.role.as_str()))
            == Some("reviewer")
            && input.agent_id.is_none()
            && admission.expected_assignment_id.is_none();
        let allows_null_assignment_ci_review = canonical_execution_role(Some(input.role.as_str()))
            == Some("reviewer")
            && input.agent_id.is_none()
            && admission.expected_assignment_id.is_some()
            && admission.expected_assignment_updated_at.is_some();
        if let Some(assignment) = assignment {
            let actual_assignment_id: String = assignment.try_get("id")?;
            let actual_assignment_updated_at: String = assignment.try_get("updated_at")?;
            if admission.expected_assignment_id.as_deref() != Some(actual_assignment_id.as_str())
                || admission.expected_assignment_updated_at.as_deref()
                    != Some(actual_assignment_updated_at.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            let assignee_type: Option<String> = assignment.try_get("assignee_type")?;
            let assignee_id: Option<String> = assignment.try_get("assignee_id")?;
            let assignment_matches = (assignee_type.as_deref() == Some("agent")
                && assignee_id.as_deref() == assignment_agent_id.as_deref())
                || (assignee_type.as_deref() == Some("user") && input.agent_id.is_none())
                || (allows_null_assignment_ci_review
                    && assignee_type.is_none()
                    && assignee_id.is_none());
            if !assignment_matches {
                return Err(DbError::VersionConflict);
            }
        } else if !allows_absent_ci_review {
            return Err(DbError::VersionConflict);
        }
        if canonical_execution_role(Some(input.role.as_str())) == Some("reviewer") {
            let latest_review = sqlx::query_as::<
                _,
                (
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    i64,
                    String,
                    String,
                ),
            >(
                "SELECT id, execution_id, reviewer_execution_id,
                        auditor_execution_id, attempt_number, status, updated_at
                 FROM review
                 WHERE task_id = ?
                 ORDER BY attempt_number DESC, id DESC
                 LIMIT 1",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            let actual_review = latest_review;
            let expected_review = match (
                admission.expected_reviewer_id.as_ref(),
                admission
                    .expected_latest_review_candidate_execution_id
                    .as_deref(),
                admission.expected_reviewer_execution_id.as_deref(),
                admission.expected_auditor_execution_id.as_deref(),
                admission.expected_reviewer_attempt_number,
                admission.expected_reviewer_status.as_ref(),
                admission.expected_reviewer_updated_at.as_ref(),
            ) {
                (
                    Some(id),
                    Some(snapshot_candidate_execution_id),
                    expected_reviewer_execution_id,
                    expected_auditor_execution_id,
                    Some(attempt_number),
                    Some(status),
                    Some(updated_at),
                ) => Some((
                    id.as_str(),
                    snapshot_candidate_execution_id,
                    expected_reviewer_execution_id,
                    expected_auditor_execution_id,
                    attempt_number,
                    status.as_str(),
                    updated_at.as_str(),
                )),
                // A ReviewRunner first attempt has no prior Review row to
                // snapshot, but still binds the new child to its candidate
                // execution. The candidate is checked separately below.
                (None, None, None, None, None, None, None) => None,
                _ => return Err(DbError::VersionConflict),
            };
            let actual_review = actual_review.as_ref().map(
                |(
                    id,
                    snapshot_candidate_execution_id,
                    reviewer_execution_id,
                    auditor_execution_id,
                    attempt_number,
                    status,
                    updated_at,
                )| {
                    (
                        id.as_str(),
                        snapshot_candidate_execution_id.as_str(),
                        reviewer_execution_id.as_deref(),
                        auditor_execution_id.as_deref(),
                        *attempt_number,
                        status.as_str(),
                        updated_at.as_str(),
                    )
                },
            );
            let fresh_attempt_without_previous_review = expected_review.is_none()
                && admission
                    .expected_latest_review_candidate_execution_id
                    .is_none()
                && admission.expected_reviewer_parent_execution_id.is_some();
            if (fresh_attempt_without_previous_review && actual_review.is_some())
                || (!fresh_attempt_without_previous_review && actual_review != expected_review)
                || input.parent_execution_id != admission.expected_reviewer_parent_execution_id
            {
                return Err(DbError::VersionConflict);
            }
        }
        Ok(())
    }

    async fn ensure_agent_execution_capacity_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: Option<&ExecutionAdmission>,
    ) -> Result<()> {
        let Some(agent_id) = input.agent_id.as_deref() else {
            return Ok(());
        };
        let agent_row = sqlx::query(
            "SELECT max_concurrent_tasks, daemon_id, paused, version
             FROM agent_current
             WHERE id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let paused: i64 = agent_row.try_get("paused")?;
        if paused != 0 {
            return Err(DbError::AgentPaused {
                agent_id: agent_id.to_owned(),
            });
        }
        let actual_version: i64 = agent_row.try_get("version")?;
        let actual_max: i64 = agent_row.try_get("max_concurrent_tasks")?;
        if let Some(admission) = admission {
            let expected_version = admission
                .expected_agent_version
                .ok_or(DbError::VersionConflict)?;
            if actual_version != expected_version {
                return Err(DbError::VersionConflict);
            }
            let expected_max = admission
                .expected_agent_max_concurrent_tasks
                .ok_or(DbError::VersionConflict)?;
            if actual_max != expected_max {
                return Err(DbError::VersionConflict);
            }
        }
        let running_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE agent_id = ? AND status = 'running'",
        )
        .bind(agent_id)
        .fetch_one(&mut **transaction)
        .await?;
        if running_count >= actual_max {
            return Err(DbError::AgentAtCapacity);
        }

        let daemon_id: Option<String> = agent_row.try_get("daemon_id")?;
        let Some(daemon_id) = daemon_id else {
            return Ok(());
        };
        let Some(daemon_row) = sqlx::query("SELECT labels_json FROM daemon WHERE id = ?")
            .bind(&daemon_id)
            .fetch_optional(&mut **transaction)
            .await?
        else {
            return Ok(());
        };
        let labels_json: String = daemon_row.try_get("labels_json")?;
        let Some(session_cap) = daemon_session_cap_from_labels(&labels_json) else {
            return Ok(());
        };
        let daemon_execution_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM execution
             JOIN agent_current AS running_agent
               ON running_agent.id = execution.agent_id
             WHERE running_agent.daemon_id = ?
               AND execution.status = 'running'",
        )
        .bind(&daemon_id)
        .fetch_one(&mut **transaction)
        .await?;
        let daemon_chat_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM agent_chat_turn_job
             JOIN agent_current AS chat_agent
               ON chat_agent.id = agent_chat_turn_job.responder_identity_id
             WHERE chat_agent.daemon_id = ?
               AND agent_chat_turn_job.status IN ('leased', 'running')",
        )
        .bind(&daemon_id)
        .fetch_one(&mut **transaction)
        .await?;
        if daemon_execution_count.saturating_add(daemon_chat_count) >= session_cap {
            return Err(DbError::AgentAtCapacity);
        }
        Ok(())
    }

    /// Bind a reviewer or conformance-auditor execution to the exact Review
    /// attempt whose authority admitted it. The Review snapshot and this
    /// execution INSERT share the same BEGIN IMMEDIATE transaction, so a
    /// newer rerun cannot steal the slot between preflight and persistence.
    /// Existing bindings may only be replaced after their execution is
    /// terminal; a live execution remains the sole owner of its Review role.
    async fn bind_role_execution_to_review_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: Option<&ExecutionAdmission>,
    ) -> Result<()> {
        if !matches!(input.role.as_str(), "reviewer" | "auditor") {
            return Ok(());
        }
        let admission = admission.ok_or(DbError::VersionConflict)?;
        let review_id = admission
            .expected_reviewer_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let row = sqlx::query(
            "SELECT task_id, execution_id, reviewer_execution_id,
                    auditor_execution_id, attempt_number, status, updated_at
             FROM review
             WHERE id = ?",
        )
        .bind(review_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let task_id: String = row.try_get("task_id")?;
        let candidate_execution_id: String = row.try_get("execution_id")?;
        let reviewer_execution_id: Option<String> = row.try_get("reviewer_execution_id")?;
        let auditor_execution_id: Option<String> = row.try_get("auditor_execution_id")?;
        let attempt_number: i64 = row.try_get("attempt_number")?;
        let status: String = row.try_get("status")?;
        let updated_at: String = row.try_get("updated_at")?;
        validate_review_candidate_in_tx(transaction, &task_id, &candidate_execution_id).await?;
        let role_binding_shape_valid = if input.role == "reviewer" {
            // A reviewer may bind an unclaimed Review or replace a terminal
            // reviewer execution, but an already-owned auditor attempt means
            // this Review is no longer available for a new reviewer owner.
            auditor_execution_id.is_none()
        } else {
            // An auditor is subordinate to the reviewer execution for this
            // exact attempt. It may first-bind or replace only a terminal
            // auditor execution; the reviewer binding must remain present.
            reviewer_execution_id.is_some()
        };
        if task_id != input.task_id
            || candidate_execution_id
                != admission
                    .expected_reviewer_parent_execution_id
                    .as_deref()
                    .ok_or(DbError::VersionConflict)?
            || !role_binding_shape_valid
            || admission.expected_reviewer_attempt_number != Some(attempt_number)
            || admission.expected_reviewer_status.as_deref() != Some(status.as_str())
            || admission.expected_reviewer_updated_at.as_deref() != Some(updated_at.as_str())
            || admission.expected_reviewer_execution_id.as_deref()
                != reviewer_execution_id.as_deref()
            || admission.expected_auditor_execution_id.as_deref() != auditor_execution_id.as_deref()
            || status != "running"
        {
            return Err(DbError::VersionConflict);
        }
        let (current_binding, binding_column) = if input.role == "reviewer" {
            (reviewer_execution_id.as_deref(), "reviewer_execution_id")
        } else {
            (auditor_execution_id.as_deref(), "auditor_execution_id")
        };
        if let Some(existing_execution_id) = current_binding {
            let existing_status =
                sqlx::query_scalar::<_, String>("SELECT status FROM execution WHERE id = ?")
                    .bind(existing_execution_id)
                    .fetch_optional(&mut **transaction)
                    .await?
                    .ok_or(DbError::VersionConflict)?;
            if existing_status == "running" {
                return Err(DbError::VersionConflict);
            }
        }
        let result = if binding_column == "reviewer_execution_id" {
            sqlx::query(
                "UPDATE review
                 SET reviewer_execution_id = ?
                 WHERE id = ? AND attempt_number = ? AND status = ? AND updated_at = ?
                   AND ((reviewer_execution_id IS NULL AND ? IS NULL)
                        OR reviewer_execution_id = ?)",
            )
            .bind(&input.id)
            .bind(review_id)
            .bind(attempt_number)
            .bind(&status)
            .bind(&updated_at)
            .bind(current_binding)
            .bind(current_binding)
            .execute(&mut **transaction)
            .await?
        } else {
            if reviewer_execution_id.is_none() {
                return Err(DbError::VersionConflict);
            }
            sqlx::query(
                "UPDATE review
                 SET auditor_execution_id = ?
                 WHERE id = ? AND attempt_number = ? AND status = ? AND updated_at = ?
                   AND ((auditor_execution_id IS NULL AND ? IS NULL)
                        OR auditor_execution_id = ?)",
            )
            .bind(&input.id)
            .bind(review_id)
            .bind(attempt_number)
            .bind(&status)
            .bind(&updated_at)
            .bind(current_binding)
            .bind(current_binding)
            .execute(&mut **transaction)
            .await?
        };
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
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

fn daemon_session_cap_from_labels(labels_json: &str) -> Option<i64> {
    let labels = serde_json::from_str::<serde_json::Value>(labels_json).ok()?;
    [
        "max_concurrent_sessions",
        "max_sessions",
        "active_session_cap",
        "max_concurrent_tasks",
    ]
    .into_iter()
    .find_map(|key| {
        labels
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .filter(|value| *value > 0)
    })
}

fn canonical_execution_role(role: Option<&str>) -> Option<&str> {
    role.map(|role| match role {
        "executor" => "coder",
        // The reviewer assignment authorizes both the ReviewRunner's CI
        // execution and its conformance auditor child.
        "auditor" => "reviewer",
        role => role,
    })
}

fn default_workflow_role(status: &str) -> Option<&'static str> {
    match status {
        "planning" => Some("planner"),
        "in_progress" | "merge_failed" => Some("coder"),
        "review" => Some("reviewer"),
        _ => None,
    }
}

fn inherited_subtask_workflow_state(status: &str) -> bool {
    matches!(status, "todo" | "in_progress" | "done" | "cancelled")
}

fn inherited_subtask_workflow_role(status: &str) -> Option<&'static str> {
    match status {
        "in_progress" => Some("coder"),
        _ => None,
    }
}
