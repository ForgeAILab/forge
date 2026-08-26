use std::{collections::HashMap, str::FromStr};

use api_types::{
    parse_project_hooks_json, AgentResponse, DaemonResponse, ExecutionResponse, PaginatedResponse,
    ProjectResponse, RepoResponse, ReviewDetails, ReviewResponse, StateKind, StepResultEntry,
    StepResultResponse, Task as ApiTask, TaskAnnotation, TaskBlockingAnnotation, TaskResponse,
    TaskRoleAssignmentResponse, TaskType, WorkspaceResponse,
};
use chrono::{DateTime, Utc};
use db::{
    Agent, Daemon, Execution, Page, PageRequest, Project, ProjectRepo, Repo, Review, SortBy,
    SortOrder, Task, TaskRoleAssignment, TaskRoleAssignmentRepo, TransitionLogRepo, Workspace,
    WorkspaceRepo,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use services::workflow::engine::WorkflowEngine;
use services::{
    plan_artifact::{read_plan_for_workspace, PlanArtifactError},
    task_diagnostics::{derive_workflow_exception, derive_workflow_health},
    task_service::action_resolver::resolve_execution_actions,
};
use sqlx::Row;

use crate::errors::{ApiError, ApiResult};

pub mod admin;
pub mod agent_chats;
pub mod agents;
pub mod auth;
pub mod clis;
pub mod coordination;
pub mod daemons;
pub mod embedded_agents;
pub mod events;
pub mod execution_baseline;
pub mod executions;
pub mod executor_types;
pub mod external_links;
pub mod fs;
pub mod integrations;
pub mod mcp_config;
pub mod members;
pub mod memory;
pub mod milestones;
pub mod mission_control;
pub mod notifications;
pub mod oauth;
pub mod operations;
pub mod product_genesis;
pub mod project_agents;
pub mod project_charters;
pub mod project_documents;
pub mod project_execution_setup;
pub mod project_media;
pub mod project_orchestration;
pub mod project_overview;
pub mod projects;
pub mod provider_authorizations;
pub mod providers;
pub mod reconciliations;
pub mod repos;
pub mod reviews;
pub mod scoped_memory;
pub mod settings;
pub mod tasks;
pub mod terminals;
pub mod workflow;
pub mod workflow_templates;
pub mod workspaces;

const SCOPED_IDEMPOTENCY_PREFIX: &str = "forge-idem-v1";

pub(crate) fn scoped_idempotency_key(
    operation: &str,
    project_id: &str,
    principal_id: &str,
    client_key: &str,
) -> String {
    format!(
        "{SCOPED_IDEMPOTENCY_PREFIX}:{}:{}:{}:{client_key}",
        hex::encode(operation),
        hex::encode(project_id),
        hex::encode(principal_id),
    )
}

pub(crate) fn client_idempotency_key(stored_key: &str) -> String {
    let mut parts = stored_key.splitn(5, ':');
    if parts.next() == Some(SCOPED_IDEMPOTENCY_PREFIX)
        && parts.next().is_some()
        && parts.next().is_some()
        && parts.next().is_some()
    {
        if let Some(client_key) = parts.next() {
            return client_key.to_owned();
        }
    }
    stored_key.to_owned()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub include_total: Option<bool>,
    pub q: Option<String>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
    pub status: Option<String>,
    pub canonical_phase: Option<String>,
    pub agent_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub include_archived: Option<bool>,
    pub include_cancelled: Option<bool>,
    pub task_type: Option<String>,
    pub priority: Option<i64>,
    pub capabilities: Option<String>,
    pub executor_type: Option<String>,
    pub daemon_id: Option<String>,
}

pub fn page_request(params: &ListParams) -> ApiResult<PageRequest> {
    Ok(PageRequest {
        cursor: params.cursor.clone(),
        limit: params.limit.unwrap_or(20).clamp(1, 100),
        include_total: params.include_total.unwrap_or(false),
        sort_by: parse_sort_by(params.sort_by.as_deref())?,
        sort_order: parse_sort_order(params.sort_order.as_deref())?,
    })
}

pub fn task_page_request(params: &ListParams) -> ApiResult<PageRequest> {
    if params.sort_by.is_none() {
        return Ok(PageRequest {
            cursor: params.cursor.clone(),
            limit: params.limit.unwrap_or(20).clamp(1, 100),
            include_total: params.include_total.unwrap_or(false),
            sort_by: SortBy::BoardPosition,
            sort_order: SortOrder::Asc,
        });
    }

    Ok(PageRequest {
        cursor: params.cursor.clone(),
        limit: params.limit.unwrap_or(20).clamp(1, 100),
        include_total: params.include_total.unwrap_or(false),
        sort_by: parse_task_sort_by(params.sort_by.as_deref())?,
        sort_order: parse_sort_order(params.sort_order.as_deref())?,
    })
}

pub fn paginated<T, U>(page: Page<T>, map: impl Fn(T) -> U) -> PaginatedResponse<U> {
    let has_more = page.next_cursor.is_some();
    PaginatedResponse {
        items: page.items.into_iter().map(map).collect(),
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
    }
}

pub fn project_response(project: Project) -> ApiResult<ProjectResponse> {
    let settings = parse_json_value(project.settings);
    let default_review_config = settings
        .get("default_review_config")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let project_hooks = parse_project_hooks_json(&project.project_hooks_json).map_err(|error| {
        ApiError::internal(format!(
            "invalid persisted project hooks for project {}: {error}",
            project.id
        ))
    })?;
    Ok(ProjectResponse {
        id: project.id,
        name: project.name,
        settings,
        project_hooks,
        default_review_config,
        primary_repo_id: project.primary_repo_id,
        owner_id: project.owner_id,
        created_at: project.created_at,
        updated_at: project.updated_at,
        workflow_template_name: project.workflow_template_name,
        paused_at: project.paused_at.clone(),
        paused: project.paused_at.is_some(),
        charter_status: project.charter_status,
        charter_setup_required: project.charter_setup_required,
        current_charter_id: project.current_charter_id,
        current_charter_revision_id: project.current_charter_revision_id,
        current_charter_version: project.current_charter_version,
        primary_milestone_id: project.primary_milestone_id,
        version: project.version,
        execution_setup: None,
    })
}

pub fn repo_response(repo: Repo) -> RepoResponse {
    RepoResponse {
        id: repo.id,
        project_id: repo.project_id,
        name: repo.name,
        local_path: repo.local_path,
        remote_url: repo.remote_url,
        default_branch: repo.default_branch,
        work_mode: repo_work_mode_response(repo.work_mode),
        pr_provider: None,
        pr_provider_status: None,
        created_at: repo.created_at,
        updated_at: repo.updated_at,
    }
}

fn repo_work_mode_response(work_mode: db::WorkMode) -> api_types::WorkMode {
    match work_mode {
        db::WorkMode::DirectMerge => api_types::WorkMode::DirectMerge,
        db::WorkMode::PullRequest => api_types::WorkMode::PullRequest,
    }
}

pub async fn task_response(db: &db::SqliteDb, task: Task) -> ApiResult<TaskResponse> {
    let (latest_review, latest_execution) = latest_diagnostic_rows(db, &task.id).await?;
    task_response_inner(db, task, true, false, latest_review, latest_execution).await
}

pub async fn task_response_light(db: &db::SqliteDb, task: Task) -> ApiResult<TaskResponse> {
    let (latest_review, latest_execution) = latest_diagnostic_rows(db, &task.id).await?;
    task_response_inner(db, task, false, false, latest_review, latest_execution).await
}

pub async fn task_response_with_awaiting_human(
    db: &db::SqliteDb,
    task: Task,
    awaiting_human: bool,
) -> ApiResult<TaskResponse> {
    let (latest_review, latest_execution) = latest_diagnostic_rows(db, &task.id).await?;
    task_response_inner(
        db,
        task,
        true,
        awaiting_human,
        latest_review,
        latest_execution,
    )
    .await
}

pub(crate) async fn task_response_light_with_latest(
    db: &db::SqliteDb,
    task: Task,
    latest_review: Option<Review>,
    latest_execution: Option<Execution>,
) -> ApiResult<TaskResponse> {
    task_response_inner(db, task, false, false, latest_review, latest_execution).await
}

async fn latest_diagnostic_rows(
    db: &db::SqliteDb,
    task_id: &str,
) -> std::result::Result<(Option<Review>, Option<Execution>), db::DbError> {
    let task_ids = [task_id];
    let mut reviews = db::ReviewRepo::list_latest_reviews_for_tasks(db, &task_ids).await?;
    let mut executions = db::ExecutionRepo::list_latest_executions_for_tasks(db, &task_ids).await?;
    Ok((reviews.pop(), executions.pop()))
}

async fn task_response_inner(
    db: &db::SqliteDb,
    task: Task,
    include_actions: bool,
    awaiting_human: bool,
    latest_review: Option<Review>,
    latest_execution: Option<Execution>,
) -> ApiResult<TaskResponse> {
    let task_role_assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id).await?;
    let role_assignments = task_role_assignments
        .iter()
        .cloned()
        .map(task_role_assignment_response)
        .collect();

    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", task.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::General),
    );
    let canonical_phase = workflow.canonical_phase_for_state(&task.status);
    let mut remaining_retries = HashMap::new();
    for state in &workflow.states {
        if state.kind != StateKind::Gate {
            continue;
        }
        let Some(max_rejections) = state
            .gate_config
            .as_ref()
            .and_then(|config| config.max_rejections)
        else {
            continue;
        };
        let count = TransitionLogRepo::count_gate_rejections(db, &task.id, &state.name).await?;
        remaining_retries.insert(
            state.name.clone(),
            (i64::from(max_rejections) - count).max(0),
        );
    }
    let workspace_model = WorkspaceRepo::get_by_task_id(db, &task.id).await?;
    let (plan_progress, plan_artifact) = if include_actions {
        match workspace_model.as_ref() {
            Some(workspace) => plan_artifact_response(db, &workspace.id).await?,
            None => (None, None),
        }
    } else {
        (None, None)
    };
    let workspace = workspace_model.map(workspace_response);
    let error_annotation = task.error_annotation.as_deref().map(|s| {
        serde_json::from_str::<TaskAnnotation>(s)
            .unwrap_or_else(|_| TaskAnnotation::Legacy(parse_json_value(s)))
    });
    let execution_actions = if include_actions {
        let executions = db::ExecutionRepo::list_by_task(
            db,
            &task.id,
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?
        .items;
        let blocked_metadata_annotation = blocked_metadata_annotation(&task);
        let error_blocking_annotation = match error_annotation.as_ref() {
            Some(TaskAnnotation::Blocking(annotation)) => Some(annotation),
            _ => None,
        };
        let blocking_annotation = blocked_metadata_annotation
            .as_ref()
            .or(error_blocking_annotation);
        resolve_execution_actions(&task, &workflow, &executions, blocking_annotation)
    } else {
        Vec::new()
    };
    let workflow_exception = derive_workflow_exception(
        &task,
        &workflow,
        latest_review.as_ref(),
        latest_execution.as_ref(),
        &remaining_retries,
    );
    let workflow_health = Some(derive_workflow_health(
        &task,
        &workflow,
        &task_role_assignments,
        latest_review.as_ref(),
        latest_execution.as_ref(),
        awaiting_human,
        workflow_exception.as_ref(),
    ));
    let execution_observability = task_execution_observability(db, &task.id).await?;
    let external_link = db::ExternalLinkRepo::get_by_task_id(db, &task.id).await?;
    // Canonical execution evidence/blocker (D16/D17, F12): computed once here
    // so Task detail, banner, and chat context all render the same server-
    // owned progress language and blocker instead of reinterpreting raw
    // gate/status enums per surface.
    let (execution_evidence, execution_blocker) =
        services::load_task_execution_blocker(db, &task).await?;

    Ok(TaskResponse {
        id: task.id,
        project_id: task.project_id,
        repo_id: task.repo_id,
        parent_task_id: task.parent_task_id.clone(),
        assignee_type: task.assignee_type,
        assignee_id: task.assignee_id,
        title: task.title,
        description: task.description,
        task_type: parse_task_type(&task.task_type),
        status: task.status,
        canonical_phase,
        awaiting_human,
        priority: task.priority,
        board_position: task.board_position,
        subtask_order: task.subtask_order,
        role_assignments,
        remaining_retries,
        execution_actions,
        error_annotation,
        blocked: task
            .blocked_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        failed: task
            .failed_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        workflow_health,
        workflow_exception,
        execution_observability,
        task_state_config: task.task_state_config.map(parse_json_value),
        review_passed_at: task.review_passed_at,
        archived_at: task.archived_at,
        workspace,
        plan_progress,
        plan_artifact,
        external_issue_number: external_link.as_ref().map(|link| link.remote_issue_number),
        external_issue_url: external_link.as_ref().map(|link| link.remote_url.clone()),
        execution_evidence,
        execution_blocker,
        version: task.version,
        created_at: task.created_at,
        updated_at: task.updated_at,
    })
}

fn blocked_metadata_annotation(task: &Task) -> Option<TaskBlockingAnnotation> {
    let metadata: Value = serde_json::from_str(task.blocked_json.as_deref()?).ok()?;
    let kind = metadata
        .get("kind")
        .cloned()
        .and_then(|kind| serde_json::from_value::<api_types::FailureKind>(kind).ok())
        .unwrap_or(api_types::FailureKind::Unknown);
    let reason = metadata
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("Task is blocked")
        .to_owned();
    let execution_id = metadata
        .get("execution_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let recovery_actions = metadata
        .get("recovery_actions")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();

    Some(TaskBlockingAnnotation {
        annotation_type: kind,
        blocking_reason: reason.clone(),
        blocked_by: Some(
            metadata
                .get("blocked_by")
                .and_then(Value::as_str)
                .unwrap_or("system")
                .to_owned(),
        ),
        blocked_at: metadata
            .get("created_at")
            .and_then(Value::as_str)
            .map(str::to_owned),
        blocked_execution_id: execution_id.clone(),
        artifact: execution_id.map(|id| api_types::BlockingArtifact {
            kind: "execution".to_owned(),
            id: Some(id),
            log_path: None,
        }),
        message: Some(reason),
        hook: metadata.get("hook").cloned(),
        recovery_actions,
    })
}

async fn task_execution_observability(
    db: &db::SqliteDb,
    task_id: &str,
) -> std::result::Result<api_types::TaskExecutionObservability, db::DbError> {
    let row = sqlx::query(
        "WITH task_executions AS (
             SELECT * FROM execution WHERE task_id = ?
         ),
         usage_totals AS (
             SELECT
                 COALESCE(SUM(eu.input_tokens), 0) AS total_input_tokens,
                 COALESCE(SUM(eu.output_tokens), 0) AS total_output_tokens,
                 COALESCE(SUM(eu.cache_read_tokens), 0) AS total_cache_read_tokens,
                 COALESCE(SUM(eu.cache_write_tokens), 0) AS total_cache_write_tokens,
                 SUM(eu.cost_usd) AS total_cost_usd
             FROM execution_usage eu
             JOIN task_executions e ON e.id = eu.execution_id
         )
         SELECT
             (SELECT COUNT(*) FROM task_executions) AS execution_count,
             (SELECT COALESCE(SUM(max(COALESCE(
                 (CASE
                     WHEN status = 'running' THEN CAST(strftime('%s', 'now') AS INTEGER)
                     ELSE CAST(strftime('%s', COALESCE(stopped_at, updated_at)) AS INTEGER)
                  END) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)), 0)
              FROM task_executions) AS total_runtime_seconds,
             (SELECT id FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_execution_id,
             (SELECT role FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_role,
             (SELECT created_at FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_started_at,
             (SELECT max(COALESCE(
                 CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)
              FROM task_executions WHERE status = 'running' ORDER BY created_at DESC, id DESC LIMIT 1) AS active_elapsed_seconds,
             (SELECT id FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_execution_id,
             (SELECT status FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_execution_status,
             (SELECT role FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_role,
             (SELECT created_at FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_started_at,
             (SELECT stopped_at FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_stopped_at,
             (SELECT max(COALESCE(
                 (CASE
                     WHEN status = 'running' THEN CAST(strftime('%s', 'now') AS INTEGER)
                     ELSE CAST(strftime('%s', COALESCE(stopped_at, updated_at)) AS INTEGER)
                  END) - CAST(strftime('%s', created_at) AS INTEGER),
                 0), 0)
              FROM task_executions ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_runtime_seconds,
             usage_totals.total_input_tokens,
             usage_totals.total_output_tokens,
             usage_totals.total_cache_read_tokens,
             usage_totals.total_cache_write_tokens,
             usage_totals.total_cost_usd
         FROM usage_totals",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await?;

    let total_input_tokens = row.try_get::<i64, _>("total_input_tokens")?;
    let total_output_tokens = row.try_get::<i64, _>("total_output_tokens")?;
    let total_cache_read_tokens = row.try_get::<i64, _>("total_cache_read_tokens")?;
    let total_cache_write_tokens = row.try_get::<i64, _>("total_cache_write_tokens")?;
    Ok(api_types::TaskExecutionObservability {
        execution_count: row.try_get("execution_count")?,
        active_execution_id: row.try_get("active_execution_id")?,
        active_role: row.try_get("active_role")?,
        active_started_at: row.try_get("active_started_at")?,
        active_elapsed_seconds: row
            .try_get::<Option<i64>, _>("active_elapsed_seconds")?
            .map(|value| value as f64),
        latest_execution_id: row.try_get("latest_execution_id")?,
        latest_execution_status: row.try_get("latest_execution_status")?,
        latest_role: row.try_get("latest_role")?,
        latest_started_at: row.try_get("latest_started_at")?,
        latest_stopped_at: row.try_get("latest_stopped_at")?,
        latest_runtime_seconds: row
            .try_get::<Option<i64>, _>("latest_runtime_seconds")?
            .map(|value| value as f64),
        total_runtime_seconds: row.try_get::<i64, _>("total_runtime_seconds")? as f64,
        total_input_tokens,
        total_output_tokens,
        total_cache_read_tokens,
        total_cache_write_tokens,
        total_tokens: total_input_tokens
            + total_output_tokens
            + total_cache_read_tokens
            + total_cache_write_tokens,
        total_cost_usd: row.try_get("total_cost_usd")?,
    })
}

fn parse_task_type(task_type: &str) -> TaskType {
    match task_type {
        "planning_task" => TaskType::PlanningTask,
        "sub_task" => TaskType::SubTask,
        "discovery" => TaskType::Discovery,
        _ => TaskType::Task,
    }
}

pub fn task_wire(task: Task, agent_name: Option<String>) -> ApiTask {
    let task_type = parse_task_type(&task.task_type);
    let agent_id = if task.assignee_type.as_deref() == Some("agent") {
        task.assignee_id
    } else {
        None
    };
    ApiTask {
        id: task.id,
        project_id: task.project_id,
        title: task.title,
        description: task.description,
        status: task.status,
        task_type,
        priority: task.priority.try_into().unwrap_or_else(|_| {
            if task.priority.is_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        }),
        board_position: task.board_position,
        blocked: task
            .blocked_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        failed: task
            .failed_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        agent_id,
        agent_name,
        external_issue_number: None,
        external_issue_url: None,
        version: task.version,
        updated_at: task.updated_at,
    }
}

pub fn task_role_assignment_response(assignment: TaskRoleAssignment) -> TaskRoleAssignmentResponse {
    TaskRoleAssignmentResponse {
        id: assignment.id,
        task_id: assignment.task_id,
        role_name: assignment.role_name,
        assignee_type: assignment.assignee_type.map(|kind| kind.to_string()),
        assignee_id: assignment.assignee_id,
        created_at: assignment.created_at,
        updated_at: assignment.updated_at,
    }
}

pub fn agent_response(
    agent: Agent,
    active_task_count: Option<i64>,
    effective_status: Option<String>,
    stats: db::AgentExecutionStats,
) -> AgentResponse {
    AgentResponse {
        id: agent.id,
        name: agent.name,
        description: agent.description,
        profile_id: agent.profile_id,
        backend_kind: agent.backend_kind,
        executor_type: agent.executor_type,
        provider: agent.provider,
        model: agent.model,
        reasoning_effort: agent.reasoning_effort,
        permission_policy: agent.permission_policy,
        prompt_template: agent.prompt_template,
        capabilities: serde_json::from_str(&agent.capabilities_json).unwrap_or_default(),
        config_json: redact_sensitive_config(parse_json_value_or_empty_object(agent.config_json)),
        credential_handle_id: agent.credential_ref,
        daemon_id: agent.daemon_id,
        max_concurrent_tasks: agent.max_concurrent_tasks,
        status: agent_status_response(agent.status),
        active_task_count,
        effective_status,
        total_runs: stats.total_runs,
        avg_duration_ms: stats.avg_duration_ms,
        success_rate: stats.success_rate,
        total_input_tokens: stats.total_input_tokens,
        total_output_tokens: stats.total_output_tokens,
        total_cache_read_tokens: stats.total_cache_read_tokens,
        total_cache_write_tokens: stats.total_cache_write_tokens,
        total_tokens: stats.total_input_tokens
            + stats.total_output_tokens
            + stats.total_cache_read_tokens
            + stats.total_cache_write_tokens,
        total_cost_usd: stats.total_cost_usd,
        is_default: agent.is_default,
        paused: agent.paused,
        owner_id: agent.owner_id,
        visibility: agent.visibility,
        version: agent.version,
        created_at: agent.created_at,
        updated_at: agent.updated_at,
    }
}

pub fn daemon_response(daemon: Daemon) -> DaemonResponse {
    DaemonResponse {
        id: daemon.id,
        machine_id: daemon.machine_id,
        hostname: daemon.hostname,
        os: daemon.os,
        arch: daemon.arch,
        agent_version: daemon.agent_version,
        status: daemon.status.to_string(),
        last_report_at: daemon.last_report_at,
        detected_clis: serde_json::from_str(&daemon.detected_clis_json).unwrap_or(Value::Null),
        labels: serde_json::from_str(&daemon.labels_json).unwrap_or(Value::Null),
        owner_id: daemon.owner_id,
        visibility: daemon.visibility,
        version: daemon.version,
        created_at: daemon.created_at,
        updated_at: daemon.updated_at,
    }
}

pub fn workspace_response(workspace: Workspace) -> WorkspaceResponse {
    WorkspaceResponse {
        id: workspace.id,
        task_id: workspace.task_id,
        repo_id: workspace.repo_id,
        worktree_path: workspace.worktree_path,
        branch: workspace.branch,
        status: workspace.status.to_string(),
        before_sha: workspace.before_sha,
        error: workspace.error,
        created_at: workspace.created_at,
        updated_at: workspace.updated_at,
    }
}

pub fn execution_response(execution: Execution) -> ExecutionResponse {
    let now = Utc::now();
    let owner_health = execution_owner_health(&execution, now);
    let liveness_warning = execution_liveness_warning(&execution, owner_health, now);
    let interruption = execution_interruption_response(&execution);
    ExecutionResponse {
        id: execution.id,
        task_id: execution.task_id,
        agent_id: execution.agent_id,
        role: execution.role,
        status: execution_status_response(execution.status),
        parent_execution_id: execution.parent_execution_id,
        agent_session_id: execution.agent_session_id,
        prompt: execution.prompt,
        summary: execution.summary,
        logs_path: execution.logs_path,
        before_sha: execution.before_sha,
        after_sha: execution.after_sha,
        error: execution.error,
        stop_reason: execution.stop_reason.map(stop_reason_response),
        stopped_by: execution.stopped_by,
        resume_policy: execution.resume_policy.map(resume_policy_response),
        stopped_at: execution.stopped_at,
        executor_config_snapshot: execution
            .executor_config_snapshot_json
            .map(parse_json_value),
        workspace_id: execution.workspace_id,
        plan_progress: None,
        plan_artifact: None,
        usage: None,
        execution_version: execution.execution_version,
        lease_owner: execution.lease_owner,
        owner_health,
        lease_expires_at: execution.lease_expires_at,
        hard_deadline_at: execution.hard_deadline_at,
        last_heartbeat_at: execution.last_heartbeat_at,
        last_progress_at: execution.last_progress_at,
        liveness_warning,
        interruption,
        created_at: execution.created_at,
        updated_at: execution.updated_at,
    }
}

fn execution_owner_health(
    execution: &Execution,
    now: DateTime<Utc>,
) -> api_types::ExecutionOwnerHealth {
    if !matches!(execution.status, db::ExecutionStatus::Running) {
        return api_types::ExecutionOwnerHealth::Unowned;
    }

    let Some(owner) = execution.lease_owner.as_deref() else {
        return api_types::ExecutionOwnerHealth::Unknown;
    };
    if owner.is_empty() {
        return api_types::ExecutionOwnerHealth::Unknown;
    }
    let Some(expires_at) = execution.lease_expires_at.as_deref() else {
        return api_types::ExecutionOwnerHealth::Unknown;
    };
    let Ok(expires_at) = DateTime::parse_from_rfc3339(expires_at) else {
        return api_types::ExecutionOwnerHealth::Unknown;
    };
    if expires_at.with_timezone(&Utc) > now {
        api_types::ExecutionOwnerHealth::Healthy
    } else {
        api_types::ExecutionOwnerHealth::Expired
    }
}

fn execution_liveness_warning(
    execution: &Execution,
    owner_health: api_types::ExecutionOwnerHealth,
    now: DateTime<Utc>,
) -> Option<String> {
    if matches!(execution.status, db::ExecutionStatus::Running) {
        if let Some(deadline) = execution.hard_deadline_at.as_deref() {
            if DateTime::parse_from_rfc3339(deadline)
                .map(|value| value.with_timezone(&Utc) <= now)
                .unwrap_or(false)
            {
                return Some("hard_deadline_reached".to_owned());
            }
        }

        return match owner_health {
            api_types::ExecutionOwnerHealth::Expired => Some("owner_lease_expired".to_owned()),
            api_types::ExecutionOwnerHealth::Unknown => Some("owner_lease_unverified".to_owned()),
            api_types::ExecutionOwnerHealth::Healthy | api_types::ExecutionOwnerHealth::Unowned => {
                None
            }
        };
    }

    match execution.stop_reason.as_ref() {
        Some(db::StopReason::AgentTimeout) => Some("hard_deadline_reached".to_owned()),
        // A terminal stalled outcome is owner-loss recovery.  Semantic
        // progress warnings are projected separately and never inferred from
        // this terminal stop reason.
        Some(db::StopReason::ExecutionStalled) => Some("owner_lease_expired".to_owned()),
        Some(db::StopReason::CrashRecovery) => Some("owner_recovery".to_owned()),
        Some(db::StopReason::DaemonDisconnected) => Some("remote_owner_disconnected".to_owned()),
        _ => None,
    }
}

fn execution_interruption_response(
    execution: &Execution,
) -> Option<api_types::ExecutionInterruptionResponse> {
    if matches!(execution.status, db::ExecutionStatus::Running) {
        return None;
    }

    let kind = execution.stop_reason.as_ref().map(stop_reason_code);
    if kind.is_none() && !matches!(execution.status, db::ExecutionStatus::Failed) {
        return None;
    }

    Some(api_types::ExecutionInterruptionResponse {
        reason: kind
            .clone()
            .unwrap_or_else(|| execution_status_code(&execution.status).to_owned()),
        kind,
        created_at: execution
            .stopped_at
            .clone()
            .unwrap_or_else(|| execution.updated_at.clone()),
    })
}

fn stop_reason_code(value: &db::StopReason) -> String {
    match value {
        db::StopReason::UserCancelled => "user_cancelled",
        db::StopReason::TaskCancelled => "task_cancelled",
        db::StopReason::RoleReassigned => "role_reassigned",
        db::StopReason::GracefulShutdown => "graceful_shutdown",
        db::StopReason::CrashRecovery => "crash_recovery",
        db::StopReason::AgentTimeout => "agent_timeout",
        db::StopReason::ExecutionStalled => "execution_stalled",
        db::StopReason::DaemonDisconnected => "daemon_disconnected",
        db::StopReason::ExecutorCancelled => "executor_cancelled",
        db::StopReason::ExecutorFailed => "executor_failed",
        db::StopReason::LegacyUnknown => "legacy_unknown",
    }
    .to_owned()
}

fn execution_status_code(value: &db::ExecutionStatus) -> &'static str {
    match value {
        db::ExecutionStatus::Running => "running",
        db::ExecutionStatus::Completed => "completed",
        db::ExecutionStatus::Failed => "failed",
        db::ExecutionStatus::Cancelled => "cancelled",
    }
}

pub async fn execution_response_with_plan(
    db: &db::SqliteDb,
    execution: Execution,
) -> ApiResult<ExecutionResponse> {
    let workspace_id = execution.workspace_id.clone();
    let mut response = execution_response(execution);
    if let Some(workspace_id) = workspace_id {
        let (plan_progress, plan_artifact) = plan_artifact_response(db, &workspace_id).await?;
        response.plan_progress = plan_progress;
        response.plan_artifact = plan_artifact;
    }
    Ok(response)
}

async fn plan_artifact_response(
    db: &db::SqliteDb,
    workspace_id: &str,
) -> ApiResult<(
    Option<api_types::PlanProgressSummary>,
    Option<api_types::PlanArtifactDetail>,
)> {
    match read_plan_for_workspace(db, workspace_id).await {
        Ok(Some((progress, artifact))) => Ok((Some(progress), Some(artifact))),
        Ok(None) | Err(PlanArtifactError::WorkspaceNotFound { .. }) => Ok((None, None)),
        Err(PlanArtifactError::DbError(error)) => Err(ApiError::from(error)),
        Err(error) => Ok((
            Some(api_types::PlanProgressSummary {
                total: 0,
                completed: 0,
                remaining: 0,
                available: false,
                warnings: vec![error.to_string()],
            }),
            None,
        )),
    }
}

pub fn execution_usage_response(usage: db::ExecutionUsage) -> api_types::ExecutionUsageResponse {
    api_types::ExecutionUsageResponse {
        id: usage.id,
        execution_id: usage.execution_id,
        provider: usage.provider,
        model: usage.model,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        cost_usd: usage.cost_usd,
        created_at: usage.created_at,
    }
}

pub fn task_usage_summary_response(
    summary: db::TaskUsageSummary,
) -> api_types::TaskUsageSummaryResponse {
    api_types::TaskUsageSummaryResponse {
        total_input_tokens: summary.total_input_tokens,
        total_output_tokens: summary.total_output_tokens,
        total_cache_read_tokens: summary.total_cache_read_tokens,
        total_cache_write_tokens: summary.total_cache_write_tokens,
        total_cost_usd: summary.total_cost_usd,
        execution_count: summary.execution_count,
    }
}

pub fn review_response(review: db::Review) -> ReviewResponse {
    let details = parse_review_details(&review.step_results_json).unwrap_or_default();
    review_response_with_details(review, details)
}

pub fn review_response_strict(review: db::Review) -> ApiResult<ReviewResponse> {
    let details = parse_review_details(&review.step_results_json).map_err(|error| {
        ApiError::bad_request(format!("invalid review step_results_json: {error}"))
    })?;
    Ok(review_response_with_details(review, details))
}

fn review_response_with_details(review: db::Review, details: ReviewDetails) -> ReviewResponse {
    let step_results = details.ci_steps.iter().map(step_result_response).collect();

    ReviewResponse {
        id: review.id,
        task_id: review.task_id,
        execution_id: review.execution_id,
        attempt_number: review.attempt_number,
        status: review_status_response(review.status),
        step_results,
        details,
        started_at: review.started_at,
        finished_at: review.finished_at,
        created_at: review.created_at,
        updated_at: review.updated_at,
    }
}

fn parse_review_details(value: &str) -> serde_json::Result<ReviewDetails> {
    let value = serde_json::from_str::<Value>(value)?;
    if value.is_array() {
        return Ok(ReviewDetails {
            ci_steps: serde_json::from_value(value)?,
            auditor: None,
        });
    }
    serde_json::from_value(value)
}

fn step_result_response(step: &StepResultEntry) -> StepResultResponse {
    StepResultResponse {
        index: step.index,
        command: step.command.clone(),
        exit_code: step.exit_code,
        stderr_tail: step.stderr_tail.clone(),
        output_tail: step.output_tail.clone(),
        started_at: step.started_at.clone(),
        finished_at: step.finished_at.clone(),
    }
}

pub fn serialize_json<T>(value: Option<T>) -> ApiResult<Option<String>>
where
    T: Serialize,
{
    value
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(|error| ApiError::bad_request(format!("invalid JSON value: {error}")))
}

pub fn parse_csv<T>(value: Option<&String>, field: &str) -> ApiResult<Vec<T>>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    value
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            item.trim()
                .parse()
                .map_err(|_| ApiError::bad_request(format!("invalid {field}: {item}")))
        })
        .collect()
}

pub fn parse_optional<T>(value: Option<&String>, field: &str) -> ApiResult<Option<T>>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    value
        .map(|value| {
            value
                .parse()
                .map_err(|_| ApiError::bad_request(format!("invalid {field}: {value}")))
        })
        .transpose()
}

fn parse_json_value(value: impl Into<String>) -> Value {
    let value = value.into();
    serde_json::from_str(&value).unwrap_or(Value::String(value))
}

fn parse_json_value_or_empty_object(value: String) -> Value {
    serde_json::from_str(&value).unwrap_or_else(|_| serde_json::json!({}))
}

pub(crate) fn redact_sensitive_config(value: Value) -> Value {
    match value {
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase().replace('-', "_");
                    let sensitive = [
                        "api_key",
                        "token",
                        "secret",
                        "password",
                        "authorization",
                        "credential",
                        "private_key",
                    ]
                    .iter()
                    .any(|candidate| normalized.contains(candidate));
                    (
                        key,
                        if sensitive {
                            Value::String("[redacted]".to_owned())
                        } else {
                            redact_sensitive_config(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(redact_sensitive_config).collect())
        }
        value => value,
    }
}

fn parse_sort_by(value: Option<&str>) -> ApiResult<SortBy> {
    match value.unwrap_or("created_at") {
        "created_at" => Ok(SortBy::CreatedAt),
        "updated_at" => Ok(SortBy::UpdatedAt),
        "priority" => Ok(SortBy::Priority),
        "board_position" => Ok(SortBy::BoardPosition),
        "id" => Ok(SortBy::Id),
        value => Err(ApiError::bad_request(format!("invalid sort_by: {value}"))),
    }
}

fn parse_task_sort_by(value: Option<&str>) -> ApiResult<SortBy> {
    match value.unwrap_or("board_position") {
        "created_at" => Ok(SortBy::CreatedAt),
        "updated_at" => Ok(SortBy::UpdatedAt),
        "priority" => Ok(SortBy::Priority),
        "board_position" => Ok(SortBy::BoardPosition),
        "title" => Ok(SortBy::Title),
        "status" => Ok(SortBy::Status),
        "agent" => Ok(SortBy::Agent),
        "task_type" => Ok(SortBy::TaskType),
        "id" => Ok(SortBy::Id),
        value => Err(ApiError::bad_request(format!("invalid sort_by: {value}"))),
    }
}

fn parse_sort_order(value: Option<&str>) -> ApiResult<SortOrder> {
    match value.unwrap_or("desc") {
        "asc" => Ok(SortOrder::Asc),
        "desc" => Ok(SortOrder::Desc),
        value => Err(ApiError::bad_request(format!(
            "invalid sort_order: {value}"
        ))),
    }
}

fn review_status_response(value: db::ReviewStatus) -> api_types::ReviewStatus {
    match value {
        db::ReviewStatus::Running => api_types::ReviewStatus::Running,
        db::ReviewStatus::AwaitingHuman => api_types::ReviewStatus::AwaitingHuman,
        db::ReviewStatus::Passed => api_types::ReviewStatus::Passed,
        db::ReviewStatus::Failed => api_types::ReviewStatus::Failed,
        db::ReviewStatus::Cancelled => api_types::ReviewStatus::Cancelled,
    }
}

fn stop_reason_response(value: db::StopReason) -> api_types::StopReason {
    match value {
        db::StopReason::UserCancelled => api_types::StopReason::UserCancelled,
        db::StopReason::TaskCancelled => api_types::StopReason::TaskCancelled,
        db::StopReason::RoleReassigned => api_types::StopReason::RoleReassigned,
        db::StopReason::GracefulShutdown => api_types::StopReason::GracefulShutdown,
        db::StopReason::CrashRecovery => api_types::StopReason::CrashRecovery,
        db::StopReason::AgentTimeout => api_types::StopReason::AgentTimeout,
        db::StopReason::ExecutionStalled => api_types::StopReason::ExecutionStalled,
        db::StopReason::DaemonDisconnected => api_types::StopReason::DaemonDisconnected,
        db::StopReason::ExecutorCancelled => api_types::StopReason::ExecutorCancelled,
        db::StopReason::ExecutorFailed => api_types::StopReason::ExecutorFailed,
        db::StopReason::LegacyUnknown => api_types::StopReason::LegacyUnknown,
    }
}

fn resume_policy_response(value: db::ResumePolicy) -> api_types::ResumePolicy {
    match value {
        db::ResumePolicy::Auto => api_types::ResumePolicy::Auto,
        db::ResumePolicy::Manual => api_types::ResumePolicy::Manual,
        db::ResumePolicy::None => api_types::ResumePolicy::None,
    }
}

fn agent_status_response(value: db::AgentStatus) -> api_types::AgentStatus {
    match value {
        db::AgentStatus::Idle => api_types::AgentStatus::Idle,
        db::AgentStatus::Busy => api_types::AgentStatus::Busy,
        db::AgentStatus::Error => api_types::AgentStatus::Error,
        db::AgentStatus::Offline => api_types::AgentStatus::Offline,
    }
}

fn execution_status_response(value: db::ExecutionStatus) -> api_types::ExecutionStatus {
    match value {
        db::ExecutionStatus::Running => api_types::ExecutionStatus::Running,
        db::ExecutionStatus::Completed => api_types::ExecutionStatus::Completed,
        db::ExecutionStatus::Failed => api_types::ExecutionStatus::Failed,
        db::ExecutionStatus::Cancelled => api_types::ExecutionStatus::Cancelled,
    }
}

#[cfg(test)]
mod idempotency_tests {
    use super::{client_idempotency_key, scoped_idempotency_key};

    #[test]
    fn idempotency_storage_keys_are_project_and_principal_scoped() {
        let first = scoped_idempotency_key("approval", "project-a", "user-a", "same:key");
        let other_project = scoped_idempotency_key("approval", "project-b", "user-a", "same:key");
        let other_user = scoped_idempotency_key("approval", "project-a", "user-b", "same:key");
        assert_ne!(first, other_project);
        assert_ne!(first, other_user);
        assert_eq!(client_idempotency_key(&first), "same:key");
    }
}

#[cfg(test)]
mod execution_liveness_response_tests {
    use super::{execution_response, Execution};
    use chrono::{Duration, Utc};

    fn running_execution() -> Execution {
        let now = Utc::now();
        Execution {
            id: "execution-1".to_owned(),
            task_id: "task-1".to_owned(),
            agent_id: Some("agent-1".to_owned()),
            role: "worker".to_owned(),
            status: db::ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some((now - Duration::minutes(3)).to_rfc3339()),
            prompt: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            execution_version: 7,
            lease_owner: Some("execution-owner:opaque".to_owned()),
            lease_expires_at: Some((now + Duration::minutes(1)).to_rfc3339()),
            hard_deadline_at: Some((now + Duration::hours(1)).to_rfc3339()),
            last_heartbeat_at: Some(now.to_rfc3339()),
            last_progress_at: Some((now - Duration::minutes(3)).to_rfc3339()),
            created_at: (now - Duration::minutes(5)).to_rfc3339(),
            updated_at: now.to_rfc3339(),
        }
    }

    #[test]
    fn response_keeps_owner_heartbeat_distinct_from_semantic_progress() {
        let response = execution_response(running_execution());

        assert_eq!(response.execution_version, 7);
        assert_eq!(
            response.lease_owner.as_deref(),
            Some("execution-owner:opaque")
        );
        assert_eq!(
            response.owner_health,
            api_types::ExecutionOwnerHealth::Healthy
        );
        assert_ne!(response.last_heartbeat_at, response.last_progress_at);
        assert!(response.liveness_warning.is_none());
        assert!(response.interruption.is_none());
    }

    #[test]
    fn response_surfaces_expiry_without_conflating_it_with_progress() {
        let mut execution = running_execution();
        execution.lease_expires_at = Some((Utc::now() - Duration::seconds(1)).to_rfc3339());

        let response = execution_response(execution);

        assert_eq!(
            response.owner_health,
            api_types::ExecutionOwnerHealth::Expired
        );
        assert_eq!(
            response.liveness_warning.as_deref(),
            Some("owner_lease_expired")
        );
        assert!(response.last_progress_at.is_some());
    }

    #[test]
    fn terminal_timeout_has_bounded_interruption_metadata() {
        let mut execution = running_execution();
        execution.status = db::ExecutionStatus::Failed;
        execution.stop_reason = Some(db::StopReason::AgentTimeout);
        execution.stopped_at = Some(Utc::now().to_rfc3339());
        execution.lease_owner = None;
        execution.lease_expires_at = None;
        execution.last_heartbeat_at = None;

        let response = execution_response(execution);

        assert_eq!(
            response.owner_health,
            api_types::ExecutionOwnerHealth::Unowned
        );
        assert_eq!(
            response.liveness_warning.as_deref(),
            Some("hard_deadline_reached")
        );
        let interruption = response.interruption.expect("timeout interruption");
        assert_eq!(interruption.kind.as_deref(), Some("agent_timeout"));
        assert_eq!(interruption.reason, "agent_timeout");
    }
}
