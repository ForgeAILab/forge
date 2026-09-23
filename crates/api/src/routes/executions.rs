use std::{
    fs,
    io::Read,
    path::{Path as StdPath, PathBuf},
};

use api_types::{ExecutionResponse, FollowUpRequest, LaunchExecutionResponse, PaginatedResponse};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use db::ExecutionRepo;
use executors::{ExecutionOverrides, LogReader};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use serde::Deserialize;
use services::ServiceError;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{
        execution_response, execution_response_with_plan, execution_response_with_usage,
        page_request, task_response, workspace_response, ListParams,
    },
    state::AppState,
};

const EXECUTION_USAGE_CONCURRENCY: usize = 8;
const DEFAULT_HOOK_LOG_LIMIT: usize = 500;
const MAX_HOOK_LOG_LIMIT: usize = 5_000;
const DEFAULT_HOOK_LOG_BYTES: usize = 1024 * 1024;
const MAX_HOOK_LOG_BYTES: usize = 8 * 1024 * 1024;
const MAX_HOOK_LOG_DIRECTORY_ENTRIES: usize = 16_384;
const MAX_HOOK_LOG_FILES: usize = 256;

pub async fn list_executions(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<ExecutionResponse>>> {
    let page = ExecutionRepo::list_by_task(&*state.db, &task_id, page_request(&params)?).await?;
    // A maximum page is larger than the SQLite pool. Keep enough parallelism
    // to hide individual projection latency without queueing one query per row.
    let items = stream::iter(
        page.items
            .into_iter()
            .map(|execution| execution_response_with_usage(&state.db, execution)),
    )
    .buffered(EXECUTION_USAGE_CONCURRENCY)
    .try_collect::<Vec<_>>()
    .await?;
    let has_more = page.next_cursor.is_some();
    Ok(Json(PaginatedResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
    }))
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

#[derive(Debug, Deserialize)]
pub struct HookLogsQuery {
    pub limit: Option<usize>,
    pub max_bytes: Option<usize>,
}

pub async fn get_hook_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<HookLogsQuery>,
) -> ApiResult<Json<Vec<serde_json::Value>>> {
    let execution = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id.clone()))?;
    let Some(logs_path) = execution.logs_path.as_deref() else {
        return Ok(Json(Vec::new()));
    };
    let Some(log_dir) = StdPath::new(logs_path).parent().map(StdPath::to_path_buf) else {
        return Ok(Json(Vec::new()));
    };
    let limit = params
        .limit
        .unwrap_or(DEFAULT_HOOK_LOG_LIMIT)
        .clamp(1, MAX_HOOK_LOG_LIMIT);
    let max_bytes = params
        .max_bytes
        .unwrap_or(DEFAULT_HOOK_LOG_BYTES)
        .clamp(1, MAX_HOOK_LOG_BYTES);

    // Directory walking and line-oriented file reads are blocking. Keep them
    // off Tokio workers and cap directory entries, files, bytes, and decoded
    // entries so a large execution directory cannot cause an unbounded scan.
    let hook_entries =
        tokio::task::spawn_blocking(move || read_hook_log_entries(&log_dir, limit, max_bytes))
            .await
            .map_err(|error| ApiError::internal(format!("hook log reader failed: {error}")))?;
    Ok(Json(hook_entries))
}

/// The newest hook entries that fit the budgets, returned oldest first.
///
/// Hook files are named by event and index, so names do not sort by time;
/// modification time does. Budgets are spent newest-first (the tail of a file
/// that does not fit), so truncation drops the oldest hooks rather than the
/// recent runs a reader is usually looking for.
fn read_hook_log_entries(
    log_dir: &StdPath,
    limit: usize,
    max_bytes: usize,
) -> Vec<serde_json::Value> {
    let entries = match fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut files = entries
        .take(MAX_HOOK_LOG_DIRECTORY_ENTRIES)
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("hook-") && name.ends_with(".jsonl")
        })
        .filter_map(|entry| {
            let modified = entry.metadata().and_then(|meta| meta.modified()).ok()?;
            Some((modified, entry.path()))
        })
        .collect::<Vec<(std::time::SystemTime, PathBuf)>>();
    files.sort_by(|a, b| b.cmp(a));
    files.truncate(MAX_HOOK_LOG_FILES);

    // Newest file first; each chunk keeps its file's own line order.
    let mut chunks: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut collected = 0;
    let mut remaining_bytes = max_bytes;
    for (_, path) in files {
        if collected >= limit || remaining_bytes == 0 {
            break;
        }
        let Some((bytes, starts_mid_line)) = read_file_tail(&path, remaining_bytes) else {
            continue;
        };
        remaining_bytes -= bytes.len();
        let text = String::from_utf8_lossy(&bytes);
        let mut lines = text.lines();
        if starts_mid_line {
            lines.next();
        }
        let values = lines
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect::<Vec<_>>();
        let keep = values.len().min(limit - collected);
        collected += keep;
        chunks.push(values[values.len() - keep..].to_vec());
    }
    chunks.into_iter().rev().flatten().collect()
}

/// Up to `max_bytes` from the end of `path`, and whether that cut a line.
fn read_file_tail(path: &StdPath, max_bytes: usize) -> Option<(Vec<u8>, bool)> {
    use std::io::{Seek, SeekFrom};
    let mut file = fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let start = len.saturating_sub(max_bytes as u64);
    if start > 0 {
        file.seek(SeekFrom::Start(start)).ok()?;
    }
    let mut bytes = Vec::with_capacity((len - start) as usize);
    file.take(max_bytes as u64).read_to_end(&mut bytes).ok()?;
    Some((bytes, start > 0))
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
        .follow_up_interactive_execution(id, request.message, request.agent_id, overrides)
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
            } else if message.contains("terminal status") {
                ApiError::conflict_with_code("task.terminal", message)
            } else {
                ApiError::invalid_operation_conflict(message)
            }
        }
        other => ApiError::from(other),
    }
}

pub async fn get_usage_breakdowns(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<api_types::UsageBreakdown>>> {
    let _ = ExecutionRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("execution", id.clone()))?;
    let usage = services::usage_projection::usage_breakdowns_for_source(&state.db, &id).await?;
    Ok(Json(usage))
}

pub async fn get_task_usage(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<api_types::UsageAggregate>> {
    let usage = services::usage_projection::usage_aggregate_for_task(&state.db, &task_id).await?;
    Ok(Json(usage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn scratch_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("forge-hook-logs-{name}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_hook(dir: &StdPath, name: &str, seqs: std::ops::Range<u32>, age_secs: u64) {
        let body: String = seqs.map(|seq| format!("{{\"seq\":{seq}}}\n")).collect();
        let path = dir.join(name);
        fs::write(&path, body).unwrap();
        let file = fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(SystemTime::now() - Duration::from_secs(age_secs))
            .unwrap();
    }

    fn seqs(entries: &[serde_json::Value]) -> Vec<u64> {
        entries
            .iter()
            .map(|entry| entry["seq"].as_u64().unwrap())
            .collect()
    }

    #[test]
    fn hook_logs_keep_the_newest_entries_in_order() {
        let dir = scratch_dir("limit");
        // Named so that name order and time order disagree.
        write_hook(&dir, "hook-a-0.jsonl", 0..3, 300);
        write_hook(&dir, "hook-z-0.jsonl", 3..6, 200);
        write_hook(&dir, "hook-m-0.jsonl", 6..9, 100);
        fs::write(dir.join("execution.jsonl"), "{\"seq\":99}\n").unwrap();

        let all = read_hook_log_entries(&dir, 100, 1024 * 1024);
        assert_eq!(seqs(&all), (0..9).collect::<Vec<_>>());

        let newest = read_hook_log_entries(&dir, 4, 1024 * 1024);
        assert_eq!(seqs(&newest), vec![5, 6, 7, 8]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn hook_logs_spend_the_byte_budget_on_the_newest_file_tail() {
        let dir = scratch_dir("bytes");
        write_hook(&dir, "hook-old-0.jsonl", 0..5, 200);
        write_hook(&dir, "hook-new-0.jsonl", 10..15, 100);

        // Each line is 11 bytes; 25 bytes cut mid-line into the newest file,
        // and the partial line is dropped rather than half-parsed.
        let tail = read_hook_log_entries(&dir, 100, 25);
        assert_eq!(seqs(&tail), vec![13, 14]);
        fs::remove_dir_all(dir).unwrap();
    }
}
