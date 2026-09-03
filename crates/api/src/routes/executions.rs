use api_types::{ExecutionResponse, FollowUpRequest, LaunchExecutionResponse, PaginatedResponse};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::{ExecutionRepo, ExecutionUsageRepo};
use executors::{ExecutionOverrides, LogReader};
use serde::Deserialize;
use services::ServiceError;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{
        execution_response, execution_response_with_plan, execution_usage_response, page_request,
        paginated, task_response, task_usage_summary_response, workspace_response, ListParams,
    },
    state::AppState,
};

pub async fn list_executions(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<ExecutionResponse>>> {
    let page = ExecutionRepo::list_by_task(&*state.db, &task_id, page_request(&params)?).await?;
    Ok(Json(paginated(page, execution_response)))
}

pub async fn get_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ExecutionResponse>> {
    let execution = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id))?;
    Ok(Json(
        execution_response_with_plan(&state.db, execution).await?,
    ))
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    pub tail: Option<usize>,
    pub from_sequence: Option<u64>,
    pub limit: Option<usize>,
}

const DEFAULT_LOG_LIMIT: usize = 200;
const MAX_LOG_LIMIT: usize = 1_000;

pub async fn get_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<LogsQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let execution = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id.clone()))?;

    let Some(path) = execution.logs_path else {
        return Ok(Json(serde_json::json!({"items": [], "has_more": false})));
    };

    match read_log_page(std::path::Path::new(&path), &params).await? {
        Some(page) => Ok(Json(page)),
        None => Err(ApiError::execution_logs_unavailable(format!(
            "log file not found for execution {id}"
        ))),
    }
}

/// One page of a Forge JSONL log as the public `{items, has_more,
/// next_sequence}` shape. `None` means the file does not exist yet; each
/// caller decides whether that is an error (an execution that claimed a
/// log path) or simply nothing recorded so far (a queued chat turn).
pub(crate) async fn read_log_page(
    log_path: &std::path::Path,
    params: &LogsQuery,
) -> ApiResult<Option<serde_json::Value>> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LOG_LIMIT)
        .clamp(1, MAX_LOG_LIMIT);
    let result = if let Some(n) = params.tail {
        LogReader::tail(log_path, n.clamp(1, MAX_LOG_LIMIT)).await
    } else {
        LogReader::read(log_path, params.from_sequence.unwrap_or(0), limit).await
    };

    let result = match result {
        Ok(r) => r,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ApiError::bad_request(error.to_string())),
    };

    Ok(Some(serde_json::json!({
        "items": result.entries,
        "has_more": result.has_more,
        "next_sequence": result.next_sequence,
    })))
}

pub async fn get_hook_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<serde_json::Value>>> {
    let execution = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id.clone()))?;
    let Some(logs_path) = execution.logs_path.as_deref() else {
        return Ok(Json(Vec::new()));
    };
    let log_dir = match std::path::Path::new(logs_path).parent() {
        Some(dir) => dir,
        None => return Ok(Json(Vec::new())),
    };
    let entries = match std::fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(_) => return Ok(Json(Vec::new())),
    };
    let mut hook_entries = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("hook-") || !name.ends_with(".jsonl") {
            continue;
        }
        let content = match std::fs::read_to_string(entry.path()) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for line in content.lines() {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
                hook_entries.push(value);
            }
        }
    }
    Ok(Json(hook_entries))
}

pub async fn follow_up_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<FollowUpRequest>,
) -> ApiResult<Json<LaunchExecutionResponse>> {
    let overrides = request.overrides.map(|overrides| ExecutionOverrides {
        model_id: overrides.model_id,
        reasoning_effort: overrides.reasoning_effort,
        permission_policy: overrides.permission_policy,
    });
    let launched = state
        .task_service
        .follow_up_execution(id, request.message, request.agent_id, overrides)
        .await
        .map_err(map_follow_up_error)?;

    let execution_id = launched.execution.id.clone();
    state.task_service.start_execution(execution_id).await?;

    let execution_behavior = Some(api_types::ExecutionBehavior {
        kind: api_types::ExecutionBehaviorKind::SessionFollowUp,
        propagates: false,
        cascade_role: None,
        cascade_state: None,
        description:
            "Session follow-up — resumes prior context without auto-transitioning the task"
                .to_owned(),
    });

    Ok(Json(LaunchExecutionResponse {
        data: api_types::LaunchExecutionData {
            task: task_response(&state.db, launched.task).await?,
            execution: execution_response(launched.execution),
            workspace: workspace_response(launched.workspace),
            execution_behavior,
        },
    }))
}

pub async fn re_execute_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<LaunchExecutionResponse>> {
    let launched = state
        .task_service
        .re_execute_execution(id)
        .await
        .map_err(map_re_execute_error)?;

    let execution_id = launched.execution.id.clone();
    state.task_service.start_execution(execution_id).await?;

    let execution_behavior = Some(api_types::ExecutionBehavior {
        kind: api_types::ExecutionBehaviorKind::ReExecute,
        propagates: true,
        cascade_role: Some(launched.execution.role.clone()),
        cascade_state: Some(launched.task.status.clone()),
        description: "Re-execute — completion may auto-transition the task".to_owned(),
    });

    Ok(Json(LaunchExecutionResponse {
        data: api_types::LaunchExecutionData {
            task: task_response(&state.db, launched.task).await?,
            execution: execution_response(launched.execution),
            workspace: workspace_response(launched.workspace),
            execution_behavior,
        },
    }))
}

pub async fn cancel_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<ExecutionResponse>> {
    let execution = state
        .task_service
        .cancel_execution(id, "cancelled by user".to_owned())
        .await?;
    Ok(Json(execution_response(execution)))
}

fn map_follow_up_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => {
            if message.contains("follow-up requires a completed, failed, or cancelled execution") {
                ApiError::conflict_with_code("follow_up.execution_active", message)
            } else if message.contains("parent execution has no resumable session") {
                ApiError::conflict_with_code("follow_up.no_session", message)
            } else if message.contains("follow-up requires same executor type") {
                ApiError::conflict_with_code("follow_up.executor_mismatch", message)
            } else if message.contains("interactive execution already running") {
                ApiError::conflict_with_code("execution.already_running", message)
            } else if message.contains("terminal status") {
                ApiError::conflict_with_code("task.terminal", message)
            } else {
                ApiError::invalid_operation_conflict(message)
            }
        }
        other => ApiError::from(other),
    }
}

fn map_re_execute_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => {
            if message.contains("re-execute requires a completed, failed, or cancelled execution") {
                ApiError::conflict_with_code("re_execute.execution_active", message)
            } else if message.contains("execution already running") {
                ApiError::conflict_with_code("execution.already_running", message)
            } else if message.contains("terminal status") {
                ApiError::conflict_with_code("task.terminal", message)
            } else {
                ApiError::invalid_operation_conflict(message)
            }
        }
        other => ApiError::from(other),
    }
}

pub async fn get_execution_usage(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<api_types::ExecutionUsageResponse>>> {
    let _ = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id.clone()))?;
    let usage = ExecutionUsageRepo::list_by_execution(&*state.db, &id).await?;
    Ok(Json(
        usage.into_iter().map(execution_usage_response).collect(),
    ))
}

pub async fn get_task_usage(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<api_types::TaskUsageSummaryResponse>> {
    let summary = ExecutionUsageRepo::get_task_usage_summary(&*state.db, &task_id).await?;
    Ok(Json(task_usage_summary_response(summary)))
}
