use crate::ExecutionAdmission;
use crate::{
    canonical_attention_incident_digest, new_uuid_v4, normalize_repo_remote_url,
    AccountMainAgentBinding, AccountMainAgentBindingRepo, AdmitAgentChatTurn, AdmitAgentHandoff,
    AdmittedAgentChatTurn, AdmittedAgentHandoff, Agent, AgentAction, AgentActionApproval,
    AgentActionExecution, AgentActionListQuery, AgentActionRepo, AgentChat,
    AgentChatInstructionRevision, AgentChatMessage, AgentChatMessageAuthorType,
    AgentChatMessageListQuery, AgentChatMessageRepo, AgentChatRepo, AgentChatSourceRef,
    AgentChatTransactionRepo, AgentChatTurnJob, AgentChatTurnJobRepo, AgentChatTurnState,
    AgentCommitment, AgentCommitmentEvidence, AgentCommitmentLifecycle, AgentCommitmentListQuery,
    AgentCommitmentRepo, AgentCommitmentStatus, AgentCommitmentTransfer, AgentHandoff,
    AgentHandoffRepo, AgentInboxItem, AgentInboxListQuery, AgentInboxRepo, AgentInquiry,
    AgentInquiryRepo, AgentListQuery, AgentProfile, AgentProfileRepo, AgentQuestion,
    AgentQuestionListQuery, AgentRepo, AgentStatus, AgentTaskListQuery, AgentWakeDisposition,
    AgentWakeDispositionKind, AgentWakeDispositionRepo, AnswerAgentQuestion,
    AppliedProjectExecutionSetupCommand, ApplyProjectExecutionSetupCommand, AttentionListQuery,
    AttentionProjection, AttentionRepo, CancelAgentChatTurn, CancelAgentChatTurnWithUsage,
    CancelAgentInquiryWithUsage, CiStepStats, ClaimExecutionLease, ClaimTask, ClaimedTask,
    CommandReceiptRepo, CompleteAgentChatControlTransfer,
    CompleteAgentChatControlTransferWithUsage, CompleteAgentChatTurn,
    CompleteAgentChatTurnWithUsage, CompleteAgentCommitment, CompleteAgentInquiry,
    CompleteAgentInquiryWithUsage, CompletedAgentChatTurn, CostCoverageReasonCode,
    CreateAccountMainAgentBinding, CreateAgent, CreateAgentAction, CreateAgentActionApproval,
    CreateAgentActionExecution, CreateAgentChat, CreateAgentChatMessage, CreateAgentChatTurnJob,
    CreateAgentCommitment, CreateAgentCommitmentEvidence, CreateAgentHandoff, CreateAgentIdentity,
    CreateAgentInboxItem, CreateAgentInquiry, CreateAgentProfile, CreateAgentQuestion,
    CreateAgentWakeDisposition, CreateAttentionProjection, CreateDomainEvent, CreateExecution,
    CreateNotification, CreateProject, CreateProjectAdmissionReceipt, CreateProjectAgentBinding,
    CreateProjectHookRun, CreateProjectIntegration, CreateProjectMediaAsset,
    CreateProjectMediaAttachment, CreateProjectMediaAttachmentMutation,
    CreateProjectProvisioningError, CreateProjectProvisioningOperation,
    CreateProjectReleaseMediaPin, CreateRepo, CreateReview, CreateRuntime, CreateSkill, CreateTask,
    CreateTaskComment, CreateTaskExternalLink, CreateTaskMedia, CreateTerminalSession,
    CreateUsageInvocation, CreateWorkspace, CreateWorkspaceLease, CurrentProjectBindingAuthority,
    Daemon, DaemonRepo, DbError, DomainEvent, DomainEventRepo, EventConsumerCursor,
    EventConsumerCutover, Execution, ExecutionLeaseDisposition, ExecutionLeaseMutation,
    ExecutionProgressWarningOutcome, ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome,
    ExecutionTerminalReceipt, ExpectedAttentionSnapshot, ExternalLinkRepo, FailAgentChatTurn,
    FailAgentChatTurnWithUsage, IntegrationRepo, LatestExecutionAuthority,
    MarkUsageInvocationPendingSettlement, MediaAsset, Notification, NotificationListQuery,
    NotificationRepo, Page, PageRequest, ParkAgentChatTurn, ParkAgentChatTurnWithUsage,
    PersistAgentWake, Project, ProjectAdmissionReceipt, ProjectAdmissionReceiptRepo,
    ProjectAgentBinding, ProjectAgentBindingRepo, ProjectAnalyticsRepo, ProjectBindingCommandRepo,
    ProjectDeletionPaths, ProjectDeletionRepositoryPath, ProjectExecutionSetupCommandRepo,
    ProjectHookRun, ProjectHookRunRepo, ProjectHookRunStatus, ProjectIntegration,
    ProjectMediaAttachment, ProjectMediaTombstone, ProjectProvisioningCheckpoint,
    ProjectProvisioningError, ProjectProvisioningOperation, ProjectProvisioningRepo,
    ProjectReleaseMediaPin, ProjectRepo, ProjectReviewSummary, RecordExecutionProgress,
    RecordExecutionProgressWarning, RenewExecutionLease, ReplaceAccountMainAgentBinding,
    ReplaceProjectAgentBinding, Repo, RepoRepo, Result, RetryAgentWakeDisposition, Review,
    ReviewRepo, ReviewStatus, Runtime, RuntimeListQuery, RuntimeRepo, SelectAgentProfile,
    SetProjectAgentBindingCommand, SettleUsageInvocation, SharedMediaRepo, Skill, SkillRepo,
    SoftDeleteProjectMediaAttachmentMutation, SortBy, SortOrder, Task, TaskComment,
    TaskCommentRepo, TaskDependencyRepo, TaskExternalLink, TaskListQuery, TaskMedia, TaskMediaRepo,
    TaskMetadata, TaskMetadataMutation, TaskRepo, TerminalSession, TerminalSessionRepo,
    TerminalSessionStatus, TerminalizeExecution, TerminalizeExecutionWithLedger,
    TransferAgentCommitment, UpdateAgent, UpdateAgentAction, UpdateAgentChat,
    UpdateAgentChatTurnJob, UpdateAgentCommitment, UpdateAgentInboxItem, UpdateAttentionLifecycle,
    UpdateDaemonReport, UpdateExecution, UpdateProject, UpdateProjectHookRun,
    UpdateProjectIntegration, UpdateProjectProvisioningOperation, UpdateRepo, UpdateSkill,
    UpdateTask, UpdateTaskStatus, UpdateTerminalSessionStatus, UpsertDaemon,
    UpsertProjectProvisioningCheckpoint, UsageAnalyticsRepo, UsageCostKind,
    UsageEventProvenanceKind, UsageInvocationLifecycle, UsageLedgerRepo, UsageLedgerSettlement,
    UsageSurface, UsageTelemetryState, Workspace, WorkspaceLease, WorkspaceLeaseRepo,
    WorkspaceRepo, WorkspaceStatus,
};
use crate::{
    AppliedProjectReviewConfigCommand, ApplyProjectReviewConfigCommand,
    ProjectReviewConfigCommandRepo,
};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, SqlitePool, Transaction};
use std::{str::FromStr, sync::Arc};
use tokio::sync::Notify;

mod action;
mod agent;
mod session_journal_cleanup;
mod session_journal_probe;
pub use agent::AgentOperatorPressureRow;
pub use session_journal_cleanup::{
    JournalCleanupBatch, JOURNAL_CLEANUP_BYTES, JOURNAL_CLEANUP_PACKS, JOURNAL_CLEANUP_PAGE,
};
pub use session_journal_probe::{
    SessionPersistenceFlags, SESSION_JOURNAL_ORDINARY_STORAGE_ID,
    SESSION_JOURNAL_PROTECTED_STORAGE_ID,
};
mod agent_chat;
mod chat_ledger;
mod chat_read_events;
pub use agent_chat::supported_main_baseline_revision;
mod agent_chat_topic;
mod agent_inquiry;
mod agent_wake;
mod analytics;
pub use analytics::TaskExecutionTimingRead;
mod attention;
mod command_finalization;
mod command_receipt;
mod commitment;
mod conflict_hotspot;
mod daemon;
pub use daemon::DaemonOperatorIssueRow;
mod dead_letter;
pub use dead_letter::{DeadLetter, DeadLetterAction, DeadLetterPage};
mod domain_event;
pub use domain_event::EventSubscription;
mod embedded_agent;
pub(crate) mod environment_readiness;
mod execution;
mod execution_admission;
pub use execution::{ActiveExecutionOperatorRow, RecentFailedExecutionOperatorRow};
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
pub use orchestration::readiness::{
    ActiveCheckWaiverSourceRow, CharterReadinessSourceRow, InsertMilestoneReadinessInput,
    InsertMilestoneReadinessSourceInput, MilestoneCharterAuthorityRow, MilestoneCheckResultRow,
    MilestoneDecisionRow, MilestoneDefinitionReadinessSourceRow, MilestoneDocumentSourceRow,
    MilestoneReadinessSourceRow, MilestoneWaiverAffectedRow, ProjectAgentIdentityRow,
    UpdateMilestoneReadinessProjectionInput,
};
pub use orchestration::releases::{
    InsertMilestoneReleaseInput, InsertReleaseDecisionReferenceInput,
    InsertReleaseDocumentReferenceInput, InsertReleaseGitReferenceInput,
    InsertReleaseReviewReferenceInput, InsertReleaseTaskReferenceInput,
    InsertReleaseValidationReferenceInput, ReleaseCharterRevisionSourceRow, ReleaseReferenceRow,
};
mod outbox;
pub use outbox::{WorkerDeadLetterIssue, WorkerDiagnostic};
mod personal_access_token;
mod pricing;
mod project;
pub use project::WorkflowProjectAuthorityRow;
mod project_execution_setup;
mod project_hook_run;
mod project_member;
mod project_provisioning;
mod project_review_config;
mod provider_authorization;
mod repo;
pub use repo::MilestoneRepositoryContextRow;
mod repo_location;
mod review;
pub use review::AttemptCarryBase;
mod runtime;
mod schedule;
mod shared_media;
pub use shared_media::{
    InsertReleaseEvidencePinInput, MilestoneEvidenceRow, MilestoneReleasePinMetadataRow,
    ProjectEvidenceAttachmentRead, ProjectMediaAssetIdRead, ReleaseEvidencePinRow,
};
mod skill;
mod system_setting;
mod task;
pub use schedule::integration_statement_in_tx;
pub use schedule::{ScheduleRead, INTEGRATION_OWNED_SQL};
pub use task::MilestoneTaskSourceRow;
pub use task::{BlockedTaskOperatorRow, TaskOperatorConditionRow, TaskOperatorRetryRow};
pub use task::{
    ConflictHandoffHistoryRow, ConflictHandoffSourceRow, InsertWorkflowTransitionLogInput,
    WriteWorkflowTransitionStateInput, CONFLICT_HANDOFF_QUERY,
};
mod task_adaptive;
mod task_comment;
pub(crate) mod task_dependency;
mod task_list_read;
mod task_media;
mod task_move;
pub use task_list_read::TaskListRead;
mod task_terminal_session;
mod user_auth;
mod workflow;
mod workspace;
pub use workspace::OverdueWorkspaceCleanupRow;
pub use workspace::{RecordWorkspaceCleanupFailureInput, WorkspaceCleanupAttentionRow};
mod workspace_lease;
mod workspace_placement;

#[derive(Debug, Clone)]
pub struct SqliteDb {
    pub(crate) condition_checks: Arc<std::sync::Mutex<crate::ConditionCheckState>>,
    pub(crate) task_step_controls: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, Arc<crate::task_writer::TaskStepControl>>,
        >,
    >,
    pub(crate) task_step_executor:
        Arc<std::sync::Mutex<Option<std::sync::Weak<dyn crate::task_writer::TaskStepExecutor>>>>,
    pub(crate) claimed_task_step_wait: Arc<std::sync::Mutex<std::time::Duration>>,
    pub server_run_cap: Arc<crate::machine_capacity::MachineRunCap>,
    /// Disk-pressure admission: off until the running server configures it.
    pub disk_admission: Arc<crate::machine_disk::DiskAdmission>,
    pool: SqlitePool,
    pub(crate) task_step_activity:
        Arc<std::sync::Mutex<std::collections::HashMap<String, (String, String)>>>,
    readiness_decode_warnings: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<i64>>>,
    domain_event_hooks: Arc<crate::connection::EventHooks>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cursor {
    offset: i64,
}

impl SqliteDb {
    pub fn new(pool: SqlitePool) -> Self {
        let domain_event_hooks = crate::connection::domain_event_hooks(&pool);
        Self {
            pool,
            condition_checks: Default::default(),
            task_step_controls: Default::default(),
            task_step_executor: Default::default(),
            claimed_task_step_wait: Arc::new(std::sync::Mutex::new(
                std::time::Duration::from_secs(90),
            )),
            task_step_activity: Default::default(),
            readiness_decode_warnings: Default::default(),
            domain_event_hooks,
            server_run_cap: Arc::new(crate::machine_capacity::MachineRunCap::default()),
            disk_admission: Default::default(),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Pool-scoped wakeup for workers that consume committed domain events.
    /// Durable cursors remain authoritative; a missed notification only falls
    /// back to the worker's bounded idle poll.
    pub fn domain_event_notify(&self) -> Arc<Notify> {
        self.domain_event_hooks.notify()
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

const TASK_COLUMNS: &str = "id, project_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, is_automation, priority, board_position, subtask_order, task_state_config, merge_config, metadata_json, plan, condition_json, error_annotation, blocked_json, failed_json, entry_barrier_json, review_passed_at, archived_at, deleted_at, version, created_at, updated_at";
const PROJECT_COLUMNS: &str = "id, name, settings, workflow_definition, workflow_template_name, primary_repo_id, paused_at, system_pause_reason, environment_pause_json, owner_id, project_hooks_json, project_work_epoch, charter_status, charter_setup_required, current_charter_id, current_charter_revision_id, current_charter_version, primary_milestone_id, version, created_at, updated_at";

/// Clear dispatch decisions that were derived from Project-level authority.
///
/// This helper deliberately accepts an existing `BEGIN IMMEDIATE` transaction
/// so callers can commit the authoritative Project/repository mutation and the
/// wake together.  Clearing the markers also bumps each affected Task's
/// `version`, forming a generation boundary that rejects an in-flight stale
/// disposition writer after the Project authority changes.
pub(crate) async fn wake_dispatch_for_project_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    project_id: &str,
    updated_at: &str,
) -> Result<u64> {
    let result = crate::task_writer::BulkTaskQuery::new(
        db,
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
        crate::ConditionChange::Legacy,
    )
    .bind(updated_at)
    .bind(project_id)
    .execute_in_tx(transaction)
    .await?;
    // Open Tasks with nothing stored to clear are reconsidered too: the
    // Project fact they were held on changed without touching their rows.
    // A resumed Project: the pause records of its Tasks state nothing any
    // more (the condition reader ignores them once `paused_at` is null), so
    // remove them and restate those conditions in this commit. No Task shows
    // `project_paused` after the resume commits, and the stored condition
    // never disagrees with the Project row. The scheduler's pass over the
    // Tasks marked below then gives each its real next step or park.
    let resumed: Vec<String> = sqlx::query_scalar(
        "SELECT s.task_id FROM task_schedule_park s JOIN task t ON t.id=s.task_id JOIN project p ON p.id=t.project_id WHERE t.project_id=? AND p.paused_at IS NULL AND json_valid(s.reason_json) AND json_extract(s.reason_json,'$.reason')='ProjectPaused'",
    )
    .bind(project_id)
    .fetch_all(&mut **transaction)
    .await?;
    for task_id in &resumed {
        sqlx::query("DELETE FROM task_schedule_park WHERE task_id=?")
            .bind(task_id)
            .execute(&mut **transaction)
            .await?;
        crate::task_condition::produce(transaction, task_id, crate::ConditionChange::Legacy)
            .await?;
    }
    sqlx::query("INSERT INTO task_schedule_dirty(task_id,external) SELECT id,1 FROM task WHERE project_id=? AND deleted_at IS NULL ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1")
        .bind(project_id)
        .execute(&mut **transaction)
        .await?;
    Ok(result.tasks())
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
        environment_pause_json: row.try_get("environment_pause_json")?,
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
        remote_url: normalize_repo_remote_url(row.try_get("remote_url")?),
        local_path: row.try_get("local_path")?,
        default_branch: row.try_get("default_branch")?,
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
        cleanup_attempts: row.try_get("cleanup_attempts")?,
        last_cleanup_error: row.try_get("last_cleanup_error")?,
        error: row.try_get("error")?,
        disk_bytes: row.try_get("disk_bytes")?,
        disk_measured_at: row.try_get("disk_measured_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_daemon(row: SqliteRow) -> Result<Daemon> {
    Ok(Daemon {
        disk_json: row.try_get("disk_json").unwrap_or(None),
        max_concurrent_runs: row.try_get("max_concurrent_runs")?,
        run_limit: row.try_get("run_limit")?,
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

impl SqliteDb {
    /// Reads a Task, including a soft-deleted one, inside the caller's
    /// transaction.
    pub async fn get_task_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: &str,
    ) -> Result<Option<Task>> {
        sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .map(map_task)
            .transpose()
    }

    /// Reads a page of Tasks (soft-deleted included) in one query.
    pub async fn get_tasks_by_ids(&self, ids: &[&str]) -> Result<Vec<Task>> {
        let ids = serde_json::to_string(ids).map_err(|e| DbError::Check(e.to_string()))?;
        sqlx::query(&format!(
            "SELECT {TASK_COLUMNS} FROM task WHERE id IN (SELECT value FROM json_each(?))"
        ))
        .bind(ids)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_task)
        .collect()
    }
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
        condition: crate::task_condition::decode_or_unknown(row.try_get("condition_json")?),
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

pub(crate) fn map_review(row: SqliteRow) -> Result<Review> {
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
}

fn canonical_execution_role(role: Option<&str>) -> Option<&str> {
    role.map(|role| match role {
        "executor" => "coder",
        // The reviewer assignment also authorizes an `auditor` execution
        // bound to the same review attempt.
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
    crate::inherited_subtask::is_state(status)
}

fn inherited_subtask_workflow_role(status: &str) -> Option<&'static str> {
    crate::inherited_subtask::role(status)
}

pub use action::{AttentionApprovalCoordinationActionsRow, AttentionCoordinationScopesRow};

pub use agent::{
    AttentionAgentBindingRow, AttentionAgentSessionRow, AttentionIdentityOwnerRow,
    AttentionVisibleAgentHealthRow, VisibleAttentionAgentBindingsInput,
};

pub use agent_chat::{AttentionChatScopeRow, WakeChatCanonicalScopeRow};

pub use agent_wake::{
    ActiveIncidentWakeLeaseInTxInput, ActiveIncidentWakeLeaseRow, AdmitIncidentWakeLeaseInTxInput,
    IncrementWakeBudgetWindowInTxInput, RecordLatestAttentionWakeInTxInput,
    StartWakeBudgetWindowInTxInput, WakeBudgetWindowRow,
};

pub use attention::{
    AttentionWorkerHealthRow, FailedTurnIncidentRow, ResolveScopeProgressAttentionInTxInput,
    WakeIncidentContextRow, WakeSweepCandidateRow,
};

pub use command_receipt::AttentionDirectCoordinationReceiptsRow;

pub use execution::AttentionVisibleActiveExecutionCountRow;

pub use memory::AttentionMemoryScopeRow;

pub use repo_location::{
    CloneVerificationLocationRow, InsertPreparedServerRepoLocationInTxInput,
    PreparedServerRepoLocationRow, ReadyServerLocationRow,
};

pub use task::{
    AnnotateOwnerRecoveryInTxInput, AnnotateRemovedMachineTaskInput, AttentionTaskContextRow,
    AttentionVisibleQueuedTaskCountRow, AttentionVisibleRecentOutcomesRow,
    AttentionVisibleWorkItemsRow, ClearRemovedMachineCancelAnnotationInput,
    RecordPlacementRefusalInTxInput,
};

pub use workspace::{
    InsertCreatingAdmissionWorkspaceInTxInput, InsertPreparedServerWorkspaceInTxInput,
    MarkPreparedAdmissionWorkspaceReadyInTxInput, RecordedOwnerWorkspaceHeadInput,
};

pub use agent::{AgentStatusHealthRow, CliRuntimePolicyStatusRow};

pub use daemon::DaemonExecutorStatusRow;

pub use task::{TaskActionScopeAssignmentRow, TaskActionScopeRoleRow};

pub use agent::{
    ActionApproverIdentityRow, ActionApproverProjectBindingRow, TaskActionActorPolicyRow,
};

pub use agent_chat::ActionApproverChatScopeRow;

pub use project::ActionOrchestrationCharterStateRow;

pub use orchestration::decisions::EffectiveProjectDecisionSourceRow;
pub use orchestration::documents::EffectiveProjectApprovedDocumentSourceRow;
pub use orchestration::effective_project::{
    EffectiveProjectGoverningCharterSourceRow, EffectiveProjectValidationOutcomeCountRow,
    ProjectAdoptionCharterSourceRow,
};
pub use orchestration::milestones::EffectiveProjectMilestoneSourceRow;
pub use orchestration::readiness::EffectiveProjectReadinessSourceRow;
pub use orchestration::reconciliation::{
    EffectiveProjectCanonicalConflictSourceRow, EffectiveProjectReconciliationSourceRow,
};
pub use orchestration::releases::EffectiveProjectReleaseSourceRow;

pub use project::EffectiveProjectIdentitySourceRow;

pub use task::EffectiveProjectTaskStatusCountRow;

pub use commitment::EffectiveProjectCommitmentSourceRow;

pub use inbox::EffectiveProjectInboxSourceRow;

pub use domain_event::EffectiveProjectEventWatermarkRow;

pub use agent_wake::{
    EscalationBlockerTurnSourceRow, InsertProjectEscalationInTxInput, ProjectEscalationSourceRow,
};

pub use domain_event::blocker_recovery_transition_exists_in_tx;
pub use project::escalation_project_owner_on_connection;
pub use project_member::escalation_project_member_exists_on_connection;

pub use repo_location::EnvironmentProvisionRetryDetailRow;

pub use attention::UpsertEnvironmentWaitAttentionInTxInput;

pub use orchestration::task_governance::{
    AdaptiveTaskGovernanceSourceRow, TaskExecutionCharterGovernanceSourceRow,
    TaskGovernanceDocumentRevisionSourceRow, WorkspaceLeaseCharterGateRow,
};

pub use task::{WorkspaceLeaseRoleAssignmentRow, WorkspaceLeaseTaskAssignmentRow};

pub use workspace_lease::InsertIssuedWorkspaceLeaseInTxInput;

pub use orchestration::task_governance::task_governance_document_revision_source_on_pool;

pub use agent::{FrozenProjectTurnBindingRow, ProjectTurnBindingAuthorityRow};

pub use orchestration::turn_admission::{
    FrozenTurnOperatingSkillRow, MainTurnGenesisInstructionRow, MainTurnOperatingSkillRow,
    ProjectTurnAdmissionReceiptRow, ProjectTurnApprovalEventRow, ProjectTurnCharterApprovalRow,
    ProjectTurnOperatingSkillRow,
};

pub use project::MainTurnPortfolioProjectRow;

pub use agent_chat::{
    ChatTurnProtectedInteractionRow, ClaimFifoChatTurnInTxInput, ExpiredChatTurnLeaseRow,
    FailChatTurnExpiredInteractionInTxInput, FailChatTurnMissingInteractionInTxInput,
    FailChatTurnWithoutInteractionInTxInput, ParkedChatTurnInteractionRow,
    ProjectTurnAdmissionHandoffRow, RecoverExpiredChatTurnLeaseInTxInput,
};

pub use command_receipt::GenesisControlTransferReceiptRow;

pub use orchestration::genesis::{
    InsertNativeProjectCommandObservationInput, NativeApprovedProjectCharterRow,
    NativeDiscoverySessionRow,
};

pub use project::{NativeOwnedProjectSummaryRow, NativePortfolioProjectRow};

pub use task_comment::NativeProjectWorklogRow;

pub use shared_media::NativeProjectArtifactRow;

pub use task::NativeWorkTaskRow;

pub use domain_event::NativeScopeEventRow;

pub use task_dependency::NativeTaskDependencyRow;

pub use execution::NativeLatestTaskExecutionRow;

pub use agent::{NativeScopeSummaryField, NativeScopeSummaryRow};

pub use agent_chat_topic::TopicRotationIntentRow;

pub use embedded_agent::{
    ReserveTopicRotationSuccessorSessionInTxInput, TopicRotationScopeWorkspaceRow,
};

pub use agent_chat::TopicRotationHistoryMessageRow;

pub use repo_location::{
    ApplyRepositoryProvisionLocationInTxInput, ClaimRepositoryProvisionAttemptInTxInput,
    InsertRepositoryProvisionLocationInTxInput, RecordRepositoryProvisionFailureInTxInput,
    ReleaseStaleRepositoryProvisionAttemptInput, RepositoryProvisionAttemptFailureInput,
    RepositoryProvisionAttemptIsCurrentInput,
};

pub use project::provisioning_project_settings_on_connection;

pub use environment_readiness::RepositoryProvisionReadinessIsCurrentInTxInput;

pub use task::RepositoryProvisionTaskWaitInput;

pub use orchestration::genesis::{
    InsertMainGenesisSessionInTxInput, SetGenesisPreferredProjectIdentityInTxInput,
};

pub use agent_chat::{
    GenesisDraftMainChatScopeRow, GenesisStartLeasedMainTurnRow,
    InsertGenesisChatInstructionInTxInput,
};

pub use agent::GenesisDraftIdentityOwnerRow;

pub use orchestration::documents::ArtifactDocumentApprovalSourceRow;
pub use orchestration::validation::{
    ValidationAcceptanceCheckSourceRow, ValidationCommandObservationSourceRow,
};

pub use project::ValidationProjectCharterSourceRow;

pub use command_receipt::{
    ArtifactCommandConflictingInputDigestInput, ArtifactReplayCommandInputDigestInput,
};

pub use shared_media::EvidenceAttachmentSourceRow;

pub use orchestration::genesis::{
    ApplyProductGenesisSetupInput, InsertProductGenesisSessionInTxInput,
    ProductGenesisSessionSourceRow, ProductGenesisSourceMessagesRow,
    TransitionProductGenesisSessionInput,
};

pub use agent_chat::InsertProductGenesisInstructionInTxInput;

pub use pricing::{
    usage_index_actual_rowids_on_connection, usage_index_affected_agents_on_connection,
    usage_index_changed_estimates_on_connection, usage_index_changed_invocations_on_connection,
    usage_index_citation_event_ids_on_connection,
    usage_index_effective_event_sources_on_connection, usage_index_watermarks_on_connection,
    UsageIndexActualRowidsRow, UsageIndexAffectedAgentRow,
    UsageIndexAffectedAgentsOnConnectionInput, UsageIndexCitationEventIdsOnConnectionInput,
    UsageIndexEffectiveEventRow, UsageIndexEstimateChangeRow, UsageIndexInvocationChangeRow,
    UsageIndexWatermarksRow,
};

pub use execution::{
    usage_index_agent_execution_stats_on_connection, usage_index_changed_executions_on_connection,
    usage_index_execution_domains_on_connection, usage_index_inserted_executions_on_connection,
    UsageIndexAgentExecutionStatsRow, UsageIndexChangedExecutionsOnConnectionInput,
    UsageIndexExecutionDomainRow, UsageIndexExecutionRow,
};

pub use pricing::usage_index_source_batch_on_connection;

pub use pricing::{UsageEventSourceMetadataRead, UsageIndexAffectedAgentField};

pub use execution::UsageIndexExecutionTextField;

pub use project::ChatUsageProjectScopeRow;

pub use orchestration::genesis::ChatUsageHandedOffProjectIdInput;

pub use chat_ledger::ChatTopicSummaryUsageRow;

pub use orchestration::readiness::{
    MilestoneArtifactReferenceSourceRow, MilestoneReleaseReadinessCandidateRow,
    MilestoneReplayReadinessSourceRow,
};

pub use orchestration::decisions::{
    DecisionArtifactRenderSourceInput, DecisionArtifactRenderSourceRow,
    DecisionGoverningCharterSourceRow, DecisionGoverningDocumentSourceRow,
    ProjectDecisionCommandSourceRow,
};

pub use agent::DirectProjectActorStatusRow;

pub use agent_chat::DirectProjectChatScopeRow;

pub use repo_location::SetRepositoryLocationDefaultInTxInput;

pub use repo_location::{RepoLocationPageSort, VisibleRepositoryLocationPageInput};

pub use attention::UpsertPlacementRefusalAttentionInTxInput;
pub use repo_location::PlacementEligibilityLocationRow;
pub use repo_location::PlacementEligibilityProvisionRetryRow;
pub use task::ClearPlacementDispatchRefusalInput;
pub use task::PlacementEligibilityAssignmentRow;
pub use task::RecordPlacementDispatchRefusalInput;

mod workspace_root;
pub use system_setting::record_workspace_root_setting;
pub use workspace_root::ProjectCleanupOwner;
pub use workspace_root::WorkspaceRootEntity;
pub use workspace_root::WorkspaceRootPathRemaining;
pub use workspace_root::WorkspaceRootPathRewrite;
pub use workspace_root::WorkspaceRootStoredPath;
pub use workspace_root::WorkspaceRootTextPredicate;
pub use workspace_root::WORKSPACE_ROOT_STORED_PATHS;

pub use execution::RecordExecutionPlanTransportRetryInTxInput;
pub use execution::RememberExecutionPlanSeedInput;
pub use task::RecordExecutionPlanTaskWaitInTxInput;

pub use action::TaskOutcomeActionOriginSourceRow;

pub use execution::ExecutionTerminalAuthorityRow;

pub use execution::LeadSubtaskExecutionRow;
pub use task_comment::LeadSubtaskCommentRow;

pub use agent_chat::TaskProposalChatScopeRow;
pub use command_receipt::TaskProposalReceiptDigestInput;

pub use command_receipt::WorkspaceMutationOutstandingIntentInput;
pub use execution::RecordWorkspaceMergeEvidenceInTxInput;

pub use agent::CharterSelectedAgentEligibilityRow;

pub use task::TaskRoleAssignmentSourceRow;

pub use agent::AgentRunnableProfileHealthRow;
pub use daemon::AgentRunnableCliPolicyRow;
pub use daemon::AgentRunnableMachineSourceRow;
pub use environment_readiness::EnvironmentReadinessJsonField;
pub use environment_readiness::ProjectEnvironmentReadinessSourceRow;
pub use workspace_placement::EnvironmentMachineCatalogRow;

pub use project::FrozenProjectBindingPolicyRow;

pub use agent_chat::OrchestrationBoundProjectChatScopeRow;
pub use agent_chat::OrchestrationMainChatScopeRow;

pub use attention::RecordWorkspaceWaitAttentionInTxInput;
pub use attention::ResolveWorkspaceAdmissionAttentionInTxInput;
pub use workspace_placement::expired_workspace_reservations_in_tx;
pub use workspace_placement::fail_expired_workspace_reservation_in_tx;
pub use workspace_placement::ExpiredWorkspaceReservationRow;

pub use daemon::PlacementExecutionDaemonSourceRow;
pub use repo_location::PlacementRepoLocationSourceRow;
pub use runtime::PlacementProvisionRuntimeSourceRow;

pub use task::InsertInitialExplicitTaskRoleInTxInput;
pub use task::TaskCreationParentRoleRow;

pub use execution::AgentUsageDomainRunRow;
pub use execution::OperationsUsageDomainRunRow;
pub use execution::TaskUsageDomainRunRow;
pub use pricing::AgentUsageSourceRow;
pub use pricing::EffectiveUsageEventMetadataRow;
pub use pricing::OperationsUsageSourceRow;
pub use pricing::TaskUsageSourceRow;
pub use pricing::TASK_USAGE_SOURCES_SQL;

pub use orchestration::project_creation::ProjectCreationOperatingSkillRow;

pub use pricing::execution_pricing_selection_count_in_tx;
pub use project::execution_admission_project_owner_in_tx;
pub use user_auth::execution_admission_account_in_tx;
pub use user_auth::execution_admission_default_account_in_tx;

pub use repo_location::InsertEmbeddedWorkspaceRepoLocationInTxInput;

pub use memory::MemoryBackfillCommentRow;
pub use memory::MemoryBackfillExecutionRow;
pub use memory::MemoryBackfillReviewRow;
pub use memory::MemoryBackfillTransitionRow;

pub use repo_location::IntegrationTransferOwedReleaseRow;

pub use orchestration::delivery_followup::DeliveryFollowupCheckRow;
pub use orchestration::delivery_followup::DeliveryFollowupMilestoneRow;

pub use project::project_agent_workspace_authority_source;
pub use project::ProjectAgentWorkspaceAuthoritySourceRow;

pub use execution::ExecutionSetupTaskEvidenceRow;
pub use orchestration::reconciliation::ExecutionSetupReconciliationConflictRow;

pub use repo_location::IntegrationTargetLocationRow;

pub use agent_chat::MainActionChatAccountRow;

pub use orchestration::charters::MainCharterReadSourceRow;

pub use project_hook_run::ProjectHookCooldownRunRow;

pub use task::project_work_completion_sources_in_tx;
pub use task::ProjectWorkCompletionSourceRow;

pub use execution::ShutdownExecutionSourceRow;
pub use task::ShutdownTaskSourceRow;

pub use agent::WakeBlockerProjectResponderRow;
pub use agent_wake::WakeBlockerTurnSourceRow;

pub use task::coordination_root_visible_children_in_connection;

pub use task::TaskRepositoryAuthoritySourceRow;

pub use task::LatestTaskStateEntrySourceRow;

pub use workflow::LatestWorkflowTransitionSourceRow;

pub use task::RecordReviewCiFailureInTxInput;

pub use task::ClearDaemonUpgradeDispatchRefusalInput;

/// Restrict human coordination reads before their bound, while native identity
/// reads use their separately admitted canonical scope.
fn push_coordination_visibility<'a>(
    builder: &mut sqlx::QueryBuilder<'a, Sqlite>,
    table: &str,
    user_id: &'a str,
) {
    builder.push(format!(" AND (({table}.scope_type = 'account' AND {table}.scope_id = ")).push_bind(user_id)
        .push(format!(") OR ({table}.scope_type = 'agent' AND EXISTS (SELECT 1 FROM agent_identity WHERE agent_identity.id = {table}.scope_id AND agent_identity.owner_id = ")).push_bind(user_id)
        .push(format!(")) OR EXISTS (SELECT 1 FROM project WHERE (({table}.scope_type = 'project' AND project.id = {table}.scope_id) OR ({table}.scope_type = 'task' AND EXISTS (SELECT 1 FROM task WHERE task.id = {table}.scope_id AND task.deleted_at IS NULL AND task.project_id = project.id)) OR ({table}.scope_type = 'agent_chat' AND EXISTS (SELECT 1 FROM agent_chat WHERE agent_chat.id = {table}.scope_id AND agent_chat.project_id = project.id))) AND (project.owner_id IS NULL OR project.owner_id = ")).push_bind(user_id)
        .push(" OR EXISTS (SELECT 1 FROM project_member WHERE project_member.project_id = project.id AND project_member.user_id = ").push_bind(user_id)
        .push(format!("))) OR ({table}.scope_type = 'agent_chat' AND EXISTS (SELECT 1 FROM agent_chat WHERE agent_chat.id = {table}.scope_id AND agent_chat.kind = 'account_main' AND agent_chat.account_id = ")).push_bind(user_id)
        .push(")))");
}

pub use orchestration::overview::{
    OverviewAcceptanceCheckRead, OverviewCandidateRead, OverviewCharterRead, OverviewCheckRead,
    OverviewDecisionRead, OverviewDocumentRead, OverviewEvidenceRead, OverviewMilestoneRead,
    OverviewMilestoneRevisionRead, OverviewReadinessRead, OverviewReleaseRead,
    OverviewReplacementRead, OverviewTaskRead,
};

pub use orchestration::documents::{ProjectDocumentIdRead, ProjectDocumentRevisionRead};

pub use orchestration::decisions::{ProjectDecisionCandidateRead, ProjectDecisionRead};
