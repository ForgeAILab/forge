use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ::time::{format_description::well_known::Rfc3339, OffsetDateTime};
use anyhow::Result;
use api_types::{
    DaemonErrorPayload, DaemonFrame, ExecutionCancelParams, ExecutionCancelResult,
    ExecutionStartParams, ExecutionStartResult, ExecutionTerminalAckParams,
    ExecutionTerminalAckResult, ExecutionTerminalNotification, FsBranchesParams, FsListParams,
    RemoteExecutionFailureClass, RemoteResolvedCandidate, RemoteRouteAttempt, RemoteUsageReport,
    UsageTelemetryState, DAEMON_CAPABILITY_TERMINAL_ACK, DAEMON_CAPABILITY_USAGE_REPORTS,
    DAEMON_PROTOCOL_REVISION, INVALID_FRAME, METHOD_DAEMON_HANDSHAKE, METHOD_EXECUTION_CANCEL,
    METHOD_EXECUTION_LOG, METHOD_EXECUTION_START, METHOD_EXECUTION_TERMINAL,
    METHOD_EXECUTION_TERMINAL_ACK, METHOD_FS_BRANCHES, METHOD_FS_LIST, METHOD_TERMINAL_INPUT,
    METHOD_TERMINAL_RESIZE, METHOD_TERMINAL_START, METHOD_TERMINAL_TERMINATE,
    TERMINAL_REPORT_CONFLICT, UNSUPPORTED_METHOD,
};
use executors::{
    ExecutionContext, ExecutionFailureClass, ExecutionOutcome, ExecutionResult, ExecutorError,
    FallbackExecutor, LogEntry, TaskExecutor,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::{
    daemon_fs,
    daemon_link::{run_dispatch_loop, run_with_reconnect, DaemonClient},
};

pub use crate::daemon_persistence::{
    DaemonTerminalStore, MAX_TERMINAL_REPORTS, MAX_TERMINAL_REPORT_BYTES, MAX_TERMINAL_REPORT_SIZE,
    TERMINAL_REPORT_DIRECTORY,
};

const TERMINAL_UNAVAILABLE: &str = "terminal_unavailable";
const EXECUTION_ERROR: &str = "execution_error";

/// A finished execution's terminal notification is only queued for the command
/// stream when its guard drops, so a report snapshot taken right after could
/// omit the id before the server has processed the completion — and the server
/// would reconcile the execution as daemon_disconnected. Finished ids therefore
/// stay in reports for this long after the guard drops.
const FINISHED_EXECUTION_LINGER: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct ActiveExecutionTracker {
    inner: Arc<Mutex<TrackerInner>>,
    finished_linger: Duration,
}

#[derive(Default)]
struct TrackerInner {
    active: HashSet<String>,
    recently_finished: HashMap<String, Instant>,
}

impl Default for ActiveExecutionTracker {
    fn default() -> Self {
        Self::with_finished_linger(FINISHED_EXECUTION_LINGER)
    }
}

impl ActiveExecutionTracker {
    pub fn with_finished_linger(finished_linger: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TrackerInner::default())),
            finished_linger,
        }
    }

    pub fn track(&self, execution_id: String) -> ActiveExecutionGuard {
        {
            let mut inner = self.inner.lock().expect("active execution tracker lock");
            inner.recently_finished.remove(&execution_id);
            inner.active.insert(execution_id.clone());
        }
        ActiveExecutionGuard {
            tracker: self.clone(),
            execution_id,
        }
    }

    pub fn active_ids(&self) -> Vec<String> {
        let now = Instant::now();
        let linger = self.finished_linger;
        let mut inner = self.inner.lock().expect("active execution tracker lock");
        inner
            .recently_finished
            .retain(|_, finished_at| now.duration_since(*finished_at) < linger);
        let mut ids: Vec<String> = inner
            .active
            .iter()
            .chain(inner.recently_finished.keys())
            .cloned()
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }
}

pub struct ActiveExecutionGuard {
    tracker: ActiveExecutionTracker,
    execution_id: String,
}

impl Drop for ActiveExecutionGuard {
    fn drop(&mut self) {
        let mut inner = self
            .tracker
            .inner
            .lock()
            .expect("active execution tracker lock");
        if inner.active.remove(&self.execution_id) {
            inner
                .recently_finished
                .insert(self.execution_id.clone(), Instant::now());
        }
    }
}

type CommandResult<T> = std::result::Result<T, DaemonErrorPayload>;

pub async fn run_command_stream(
    client: Arc<DaemonClient>,
    workspace_root: PathBuf,
    shutdown: watch::Receiver<bool>,
    active_executions: ActiveExecutionTracker,
) -> Result<()> {
    let workspace_root = Arc::new(workspace_root);
    run_with_reconnect(client, move |stream| {
        let workspace_root = Arc::clone(&workspace_root);
        let shutdown = shutdown.clone();
        let active_executions = active_executions.clone();
        async move {
            let (responses_tx, responses_rx) = mpsc::unbounded_channel();
            let runtime = DaemonRuntime::new_with_tracker(
                responses_tx.clone(),
                workspace_root.as_ref().clone(),
                active_executions,
            );
            let handler = {
                let runtime = Arc::clone(&runtime);
                move |frame| {
                    let runtime = Arc::clone(&runtime);
                    async move { runtime.handle_request(frame).await }
                }
            };
            run_dispatch_loop(stream, handler, shutdown, responses_tx, responses_rx).await
        }
    })
    .await
}

pub struct DaemonRuntime {
    workspace_root: PathBuf,
    outbound: mpsc::UnboundedSender<DaemonFrame>,
    executor: Arc<FallbackExecutor>,
    active_executions: ActiveExecutionTracker,
    terminal_store: Arc<DaemonTerminalStore>,
}

impl DaemonRuntime {
    pub fn new(outbound: mpsc::UnboundedSender<DaemonFrame>, workspace_root: PathBuf) -> Arc<Self> {
        Self::new_with_tracker(outbound, workspace_root, ActiveExecutionTracker::default())
    }

    pub fn new_with_tracker(
        outbound: mpsc::UnboundedSender<DaemonFrame>,
        workspace_root: PathBuf,
        active_executions: ActiveExecutionTracker,
    ) -> Arc<Self> {
        let registry = Arc::new(cli_adapters::default_registry());
        let terminal_store = Arc::new(DaemonTerminalStore::new(&workspace_root));
        let runtime = Arc::new(Self {
            workspace_root,
            outbound,
            executor: Arc::new(FallbackExecutor::new(registry)),
            active_executions,
            terminal_store,
        });
        runtime.announce_protocol();
        runtime.replay_pending_terminals();
        runtime
    }

    pub fn active_execution_ids(&self) -> Vec<String> {
        self.active_executions.active_ids()
    }

    /// The durable queue is shared by every runtime created during command
    /// stream reconnects. Keeping this accessor public gives the daemon host a
    /// narrow inspection point for diagnostics without exposing raw payload
    /// files or credentials.
    pub fn terminal_store(&self) -> &DaemonTerminalStore {
        &self.terminal_store
    }

    fn announce_protocol(&self) {
        let handshake = api_types::DaemonHandshakeNotification {
            protocol_revision: DAEMON_PROTOCOL_REVISION,
            capabilities: vec![
                DAEMON_CAPABILITY_USAGE_REPORTS.to_owned(),
                DAEMON_CAPABILITY_TERMINAL_ACK.to_owned(),
            ],
        };
        emit_notification(&self.outbound, METHOD_DAEMON_HANDSHAKE, handshake);
    }

    fn replay_pending_terminals(&self) {
        match self.terminal_store.pending() {
            Ok(notifications) => {
                for notification in notifications {
                    emit_notification(&self.outbound, METHOD_EXECUTION_TERMINAL, notification);
                }
            }
            Err(error) => {
                // A malformed or unreadable durable record must not be
                // silently deleted. Keep the daemon connected for ordinary
                // work, but surface the replay failure for operator recovery.
                tracing::error!(%error, "failed to replay retained daemon terminal reports");
            }
        }
    }

    pub async fn handle_request(self: &Arc<Self>, frame: DaemonFrame) -> DaemonFrame {
        let DaemonFrame::Request { id, method, params } = frame else {
            return error_frame(
                None,
                INVALID_FRAME,
                "daemon command handler expected a request frame",
                None,
            );
        };

        match method.as_str() {
            METHOD_FS_LIST => match decode_params::<FsListParams>(&id, params) {
                Ok(params) => match daemon_fs::list_entries(params, &self.workspace_root).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_FS_BRANCHES => match decode_params::<FsBranchesParams>(&id, params) {
                Ok(params) => match daemon_fs::list_branches(params, &self.workspace_root).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_EXECUTION_START => match decode_params::<ExecutionStartParams>(&id, params) {
                Ok(params) => match self.start(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_EXECUTION_CANCEL => match decode_params::<ExecutionCancelParams>(&id, params) {
                Ok(params) => match self.cancel(params).await {
                    Ok(result) => response_frame(id, result),
                    Err(error) => DaemonFrame::Error {
                        id: Some(id),
                        error,
                    },
                },
                Err(frame) => frame,
            },
            METHOD_EXECUTION_TERMINAL_ACK => {
                match decode_params::<ExecutionTerminalAckParams>(&id, params) {
                    Ok(params) => match self.acknowledge_terminal(params).await {
                        Ok(result) => response_frame(id, result),
                        Err(error) => DaemonFrame::Error {
                            id: Some(id),
                            error,
                        },
                    },
                    Err(frame) => frame,
                }
            }
            METHOD_TERMINAL_START
            | METHOD_TERMINAL_INPUT
            | METHOD_TERMINAL_RESIZE
            | METHOD_TERMINAL_TERMINATE => terminal_unavailable_frame(id),
            _ => error_frame(
                Some(id),
                UNSUPPORTED_METHOD,
                format!("unsupported daemon command method: {method}"),
                None,
            ),
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        params: ExecutionStartParams,
    ) -> CommandResult<ExecutionStartResult> {
        let worktree_path = daemon_fs::validate_within_root(
            Path::new(params.workspace_path.trim()),
            &self.workspace_root,
        )?;
        let logs_path = local_execution_log_path(&self.workspace_root, &params.execution_id);
        let description = prompt_description(&params.prompt);
        let ctx = ExecutionContext {
            task_id: params.task_id.clone(),
            execution_id: params.execution_id.clone(),
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            description,
            agent_config: params.executor_config,
            logs_path: logs_path.to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: params.max_turns,
            log_sender: None,
        };

        let execution_id = params.execution_id.clone();
        let executor = Arc::clone(&self.executor);
        let outbound = self.outbound.clone();
        let active_executions = self.active_executions.clone();
        let terminal_store = Arc::clone(&self.terminal_store);
        tokio::spawn(async move {
            run_execution_task(executor, outbound, ctx, active_executions, terminal_store).await;
        });

        Ok(ExecutionStartResult {
            execution_id,
            accepted: true,
        })
    }

    pub async fn cancel(
        &self,
        params: ExecutionCancelParams,
    ) -> CommandResult<ExecutionCancelResult> {
        self.executor
            .cancel(&params.execution_id)
            .await
            .map_err(|error| execution_error(format!("failed to cancel execution: {error}")))?;
        Ok(ExecutionCancelResult {
            execution_id: params.execution_id,
            cancelled: true,
        })
    }

    pub async fn acknowledge_terminal(
        &self,
        params: ExecutionTerminalAckParams,
    ) -> CommandResult<ExecutionTerminalAckResult> {
        self.terminal_store.acknowledge(&params).map_err(|error| {
            let message = error.to_string();
            let code = if message.contains(TERMINAL_REPORT_CONFLICT) {
                TERMINAL_REPORT_CONFLICT
            } else {
                EXECUTION_ERROR
            };
            DaemonErrorPayload {
                code: code.to_owned(),
                message,
                details: None,
            }
        })
    }
}

async fn run_execution_task(
    executor: Arc<FallbackExecutor>,
    outbound: mpsc::UnboundedSender<DaemonFrame>,
    mut ctx: ExecutionContext,
    active_executions: ActiveExecutionTracker,
    terminal_store: Arc<DaemonTerminalStore>,
) {
    let _active_guard = active_executions.track(ctx.execution_id.clone());
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<LogEntry>();
    ctx.log_sender = Some(log_tx);
    let log_outbound = outbound.clone();
    let mut log_forwarder = tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            emit_execution_log(&log_outbound, entry);
        }
    });

    emit_execution_log(
        &outbound,
        daemon_system_log(&ctx.execution_id, "remote daemon execution started"),
    );

    let execution_id = ctx.execution_id.clone();
    let read_only_path = executors::is_worktree_read_only(&ctx.agent_config)
        .then(|| PathBuf::from(&ctx.worktree_path));
    let read_only_head = match read_only_path.as_deref() {
        Some(path) => git::get_current_sha(path).await.map(Some).map_err(|error| {
            ExecutorError::Other(format!(
                "failed to capture read-only worktree state: {error}"
            ))
        }),
        None => Ok(None),
    };
    let result = match read_only_head {
        Ok(read_only_head) => {
            let execution_result = executor.execute(ctx).await;
            let restore_result = match (read_only_path.as_deref(), read_only_head.as_deref()) {
                (Some(path), Some(head)) => {
                    git::restore_worktree(path, head).await.map_err(|error| {
                        ExecutorError::Other(format!(
                            "failed to restore read-only worktree state: {error}"
                        ))
                    })
                }
                _ => Ok(()),
            };
            match (execution_result, restore_result) {
                (_, Err(error)) => Err(error),
                (Ok(mut result), Ok(())) => {
                    if let Some(head) = read_only_head {
                        result.after_sha = Some(head);
                    }
                    Ok(result)
                }
                (Err(error), Ok(())) => Err(error),
            }
        }
        Err(error) => Err(error),
    };
    // The executor owns the only log sender in ctx, so completion should close the
    // channel and let the forwarder drain. If an executor holds a sender clone or
    // emits a very large trailing burst, the timeout favors terminal notification
    // over complete best-effort log delivery.
    if tokio::time::timeout(Duration::from_secs(2), &mut log_forwarder)
        .await
        .is_err()
    {
        log_forwarder.abort();
        let _ = log_forwarder.await;
    }

    let notification = match result {
        Ok(result) => terminal_notification_from_result(execution_id, result),
        Err(error) => ExecutionTerminalNotification {
            terminal_report_id: terminal_report_id_for_execution(&execution_id),
            execution_id,
            exit_code: Some(1),
            signal: None,
            error: Some(error.to_string()),
            ts: rfc3339_now(),
            status: Some("failed".to_owned()),
            agent_session_id: None,
            summary: None,
            after_sha: None,
            usage_reports: Vec::new(),
            failure_class: None,
            retry_at: None,
            resolved_candidate: None,
            route_attempts: None,
        },
    };
    match terminal_store.retain(&notification) {
        Ok(()) => emit_notification(&outbound, METHOD_EXECUTION_TERMINAL, notification),
        Err(error) => tracing::error!(
            %error,
            execution_id = %notification.execution_id,
            terminal_report_id = %notification.terminal_report_id,
            "failed to durably retain daemon terminal report"
        ),
    }
}

fn terminal_notification_from_result(
    execution_id: String,
    result: ExecutionResult,
) -> ExecutionTerminalNotification {
    let (status, exit_code, signal, error) = match result.status {
        ExecutionOutcome::Completed => ("completed", Some(0), None, None),
        ExecutionOutcome::Failed => ("failed", Some(1), None, result.error),
        ExecutionOutcome::Cancelled => ("cancelled", None, Some("cancelled".to_owned()), None),
    };
    ExecutionTerminalNotification {
        terminal_report_id: terminal_report_id_for_execution(&execution_id),
        execution_id,
        exit_code,
        signal,
        error,
        ts: rfc3339_now(),
        status: Some(status.to_owned()),
        agent_session_id: result.agent_session_id,
        summary: result.summary,
        after_sha: result.after_sha,
        usage_reports: result
            .usage_reports
            .into_iter()
            .map(remote_usage_report_from_executor)
            .collect(),
        failure_class: result.failure_class.map(|class| match class {
            ExecutionFailureClass::TaskFailed => RemoteExecutionFailureClass::TaskFailed,
            ExecutionFailureClass::ExecutorUnavailable => {
                RemoteExecutionFailureClass::ExecutorUnavailable
            }
        }),
        retry_at: result.retry_after.and_then(|retry_after| {
            (OffsetDateTime::now_utc() + retry_after)
                .format(&Rfc3339)
                .ok()
        }),
        resolved_candidate: result
            .resolved_candidate
            .map(|candidate| RemoteResolvedCandidate {
                candidate_key: candidate.candidate_key,
                executor_type: candidate.executor_type.to_string(),
                config: candidate.config,
            }),
        route_attempts: if result.route_attempts.is_empty() {
            None
        } else {
            Some(
                result
                    .route_attempts
                    .into_iter()
                    .map(|attempt| RemoteRouteAttempt {
                        candidate_key: attempt.candidate_key,
                        outcome: attempt.outcome.as_str().to_owned(),
                    })
                    .collect(),
            )
        },
    }
}

fn remote_usage_report_from_executor(report: executors::UsageReport) -> RemoteUsageReport {
    RemoteUsageReport {
        report_id: report.report_id,
        request_id: report.request_id,
        report_sequence: report.report_sequence,
        candidate_key: report.candidate_key,
        attempt_ordinal: report.attempt_ordinal,
        provider_id: report.provider_id,
        model_id: report.model_id,
        input_tokens: report.counters.input_tokens,
        output_tokens: report.counters.output_tokens,
        cache_read_tokens: report.counters.cache_read_tokens,
        cache_write_tokens: report.counters.cache_write_tokens,
        telemetry_state: match report.telemetry_state {
            executors::UsageTelemetryState::Metered => UsageTelemetryState::Metered,
            executors::UsageTelemetryState::Unmetered => UsageTelemetryState::Unmetered,
            executors::UsageTelemetryState::Pending => UsageTelemetryState::Pending,
            executors::UsageTelemetryState::Unsettled => UsageTelemetryState::Unsettled,
        },
        context_tokens: report.context_tokens,
        selected_tier: report.selected_tier,
        reported_cost_usd: report.reported_cost_usd,
        partial: report.partial,
    }
}

fn terminal_report_id_for_execution(execution_id: &str) -> String {
    format!("forge:terminal:{execution_id}")
}

fn daemon_system_log(execution_id: &str, line: &str) -> LogEntry {
    LogEntry {
        schema_version: 1,
        sequence: 0,
        timestamp: rfc3339_now(),
        execution_id: execution_id.to_owned(),
        kind: executors::LogKind::System,
        stream: executors::LogStream::Main,
        payload: serde_json::json!({ "line": line }),
        truncated: false,
    }
}

fn emit_execution_log(outbound: &mpsc::UnboundedSender<DaemonFrame>, entry: LogEntry) {
    let line = entry
        .payload
        .get("line")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| entry.payload.to_string());
    let stream = match entry.kind {
        executors::LogKind::Stderr => "stderr",
        _ => "stdout",
    };
    let notification = api_types::ExecutionLogNotification {
        execution_id: entry.execution_id.clone(),
        seq: entry.sequence,
        stream: stream.to_owned(),
        line,
        ts: entry.timestamp.clone(),
        kind: Some(entry.kind.to_string()),
        log_stream: Some(
            match entry.stream {
                executors::LogStream::Heartbeat => "heartbeat",
                executors::LogStream::Main => "main",
            }
            .to_owned(),
        ),
        payload: Some(entry.payload),
        truncated: Some(entry.truncated),
    };
    emit_notification(outbound, METHOD_EXECUTION_LOG, notification);
}

fn emit_notification<T: Serialize>(
    outbound: &mpsc::UnboundedSender<DaemonFrame>,
    method: &str,
    notification: T,
) {
    match serde_json::to_value(notification) {
        Ok(params) => {
            let _ = outbound.send(DaemonFrame::Notification {
                method: method.to_owned(),
                params,
            });
        }
        Err(error) => {
            tracing::warn!(%error, method, "failed to serialize daemon notification");
        }
    }
}

fn terminal_unavailable_frame(id: String) -> DaemonFrame {
    error_frame(
        Some(id),
        TERMINAL_UNAVAILABLE,
        "terminal support is not available in this daemon command context",
        None,
    )
}

fn local_execution_log_path(workspace_root: &Path, execution_id: &str) -> PathBuf {
    workspace_root
        .join(".forge-daemon")
        .join("execution-logs")
        .join(format!("{}.jsonl", safe_path_component(execution_id)))
}

fn safe_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn prompt_description(prompt: &Value) -> String {
    prompt
        .get("description")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| prompt.to_string())
}

fn execution_error(message: impl Into<String>) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: EXECUTION_ERROR.to_owned(),
        message: message.into(),
        details: None,
    }
}

fn decode_params<T: DeserializeOwned>(
    id: &str,
    params: serde_json::Value,
) -> std::result::Result<T, DaemonFrame> {
    serde_json::from_value(params).map_err(|error| {
        error_frame(
            Some(id.to_owned()),
            INVALID_FRAME,
            format!("invalid daemon command params: {error}"),
            None,
        )
    })
}

fn response_frame<T: Serialize>(id: String, result: T) -> DaemonFrame {
    match serde_json::to_value(result) {
        Ok(result) => DaemonFrame::Response { id, result },
        Err(error) => error_frame(
            Some(id),
            INVALID_FRAME,
            format!("failed to serialize daemon command result: {error}"),
            None,
        ),
    }
}

fn error_frame(
    id: Option<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> DaemonFrame {
    DaemonFrame::Error {
        id,
        error: DaemonErrorPayload {
            code: code.into(),
            message: message.into(),
            details,
        },
    }
}

fn rfc3339_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use api_types::{
        ExecutionLogNotification, FsListResult, METHOD_EXECUTION_LOG, METHOD_EXECUTION_TERMINAL,
        METHOD_FS_LIST,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn fs_list_returns_entries_under_workspace_root() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        fs::create_dir_all(dir.path().join("src")).expect("src creates");
        fs::write(dir.path().join("README.md"), "readme").expect("readme writes");
        let (tx, _rx) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new(tx, dir.path().to_path_buf());

        let frame = DaemonFrame::Request {
            id: "fs-1".to_owned(),
            method: METHOD_FS_LIST.to_owned(),
            params: serde_json::json!({ "path": "." }),
        };
        let response = runtime.handle_request(frame).await;

        let DaemonFrame::Response { result, .. } = response else {
            panic!("expected response");
        };
        let result: FsListResult = serde_json::from_value(result).expect("fs result parses");
        let names = result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["src", "README.md"]);
    }

    #[tokio::test]
    async fn shell_execution_reports_completion_notification() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-ok".to_owned();

        let result = runtime
            .start(ExecutionStartParams {
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf ok > marker.txt" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");
        assert!(result.accepted);

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        assert_eq!(notification.status.as_deref(), Some("completed"));
        assert_eq!(
            fs::read_to_string(dir.path().join("marker.txt")).expect("marker exists"),
            "ok"
        );
    }

    #[tokio::test]
    async fn terminal_report_is_retained_until_acknowledged() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-retained".to_owned();

        runtime
            .start(ExecutionStartParams {
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf retained" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        assert_eq!(
            runtime.terminal_store().pending().expect("pending reports"),
            vec![notification.clone()]
        );

        let response = runtime
            .handle_request(DaemonFrame::Request {
                id: "terminal-ack-1".to_owned(),
                method: METHOD_EXECUTION_TERMINAL_ACK.to_owned(),
                params: serde_json::to_value(ExecutionTerminalAckParams {
                    terminal_report_id: notification.terminal_report_id.clone(),
                    execution_id: notification.execution_id.clone(),
                })
                .expect("ack params serialize"),
            })
            .await;
        let DaemonFrame::Response { result, .. } = response else {
            panic!("expected ack response");
        };
        let result: ExecutionTerminalAckResult =
            serde_json::from_value(result).expect("ack result parses");
        assert!(result.acknowledged);
        assert!(runtime
            .terminal_store()
            .pending()
            .expect("empty reports")
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shell_execution_can_be_cancelled() {
        let dir = tempfile::tempdir().expect("temp dir creates");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let runtime = DaemonRuntime::new(tx, dir.path().to_path_buf());
        let execution_id = "exec-shell-cancel".to_owned();

        runtime
            .start(ExecutionStartParams {
                task_id: "task-1".to_owned(),
                execution_id: execution_id.clone(),
                workspace_path: dir.path().to_string_lossy().into_owned(),
                executor_type: "shell".to_owned(),
                executor_config: serde_json::json!({
                    "executor_type": "shell",
                    "config": {}
                }),
                prompt: serde_json::json!({ "description": "printf 'started\\n'; sleep 30" }),
                max_turns: None,
            })
            .await
            .expect("execution starts");
        next_execution_log_line(&mut rx, &execution_id, "started").await;
        runtime
            .cancel(ExecutionCancelParams {
                execution_id: execution_id.clone(),
                reason: Some("test".to_owned()),
            })
            .await
            .expect("execution cancels");

        let notification = next_terminal_notification(&mut rx, &execution_id).await;
        assert_eq!(notification.status.as_deref(), Some("cancelled"));
    }

    async fn next_execution_log_line(
        rx: &mut mpsc::UnboundedReceiver<DaemonFrame>,
        execution_id: &str,
        expected_line: &str,
    ) -> ExecutionLogNotification {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let frame = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("execution log arrives")
                .expect("runtime keeps sender open");
            let DaemonFrame::Notification { method, params } = frame else {
                continue;
            };
            if method != METHOD_EXECUTION_LOG {
                continue;
            }
            let notification: ExecutionLogNotification =
                serde_json::from_value(params).expect("execution log parses");
            if notification.execution_id == execution_id && notification.line == expected_line {
                return notification;
            }
        }
    }

    async fn next_terminal_notification(
        rx: &mut mpsc::UnboundedReceiver<DaemonFrame>,
        execution_id: &str,
    ) -> ExecutionTerminalNotification {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let frame = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .expect("terminal notification arrives")
                .expect("runtime keeps sender open");
            let DaemonFrame::Notification { method, params } = frame else {
                continue;
            };
            if method != METHOD_EXECUTION_TERMINAL {
                continue;
            }
            let notification: ExecutionTerminalNotification =
                serde_json::from_value(params).expect("terminal notification parses");
            if notification.execution_id == execution_id {
                return notification;
            }
        }
    }

    #[test]
    fn tracker_lingers_finished_executions_in_active_ids() {
        let tracker = ActiveExecutionTracker::default();
        let guard = tracker.track("exec-1".to_owned());
        assert_eq!(tracker.active_ids(), ["exec-1"]);

        drop(guard);
        assert_eq!(
            tracker.active_ids(),
            ["exec-1"],
            "finished execution must linger in reports until the terminal notification has settled"
        );
    }

    #[test]
    fn tracker_prunes_finished_executions_after_linger() {
        let tracker = ActiveExecutionTracker::with_finished_linger(Duration::ZERO);
        let guard = tracker.track("exec-1".to_owned());
        drop(guard);
        assert!(tracker.active_ids().is_empty());
    }

    #[test]
    fn tracker_retrack_moves_id_back_to_active() {
        let tracker = ActiveExecutionTracker::with_finished_linger(Duration::ZERO);
        let first = tracker.track("exec-1".to_owned());
        drop(first);
        let _second = tracker.track("exec-1".to_owned());
        assert_eq!(tracker.active_ids(), ["exec-1"]);
    }

    #[test]
    fn terminal_mapping_preserves_each_usage_report_without_flattening() {
        let mut report = executors::UsageReport::metered(
            "provider-report-1",
            executors::UsageCounters {
                input_tokens: Some(10),
                output_tokens: Some(4),
                cache_read_tokens: None,
                cache_write_tokens: Some(2),
            },
        );
        report.request_id = Some("provider-request-1".to_owned());
        report.candidate_key = Some("codex#primary".to_owned());
        report.attempt_ordinal = 1;
        report.provider_id = Some("openai".to_owned());
        report.model_id = Some("gpt-5".to_owned());
        report.context_tokens = Some(128);
        report.selected_tier = Some("long".to_owned());
        report.reported_cost_usd = Some("0.000000123".to_owned());
        report.partial = true;

        let notification = terminal_notification_from_result(
            "execution-1".to_owned(),
            ExecutionResult {
                status: ExecutionOutcome::Completed,
                usage_reports: vec![report],
                ..ExecutionResult::default()
            },
        );

        assert_eq!(
            notification.terminal_report_id,
            "forge:terminal:execution-1"
        );
        assert_eq!(notification.usage_reports.len(), 1);
        let remote = &notification.usage_reports[0];
        assert_eq!(remote.report_id, "provider-report-1");
        assert_eq!(remote.request_id.as_deref(), Some("provider-request-1"));
        assert_eq!(remote.input_tokens, Some(10));
        assert_eq!(remote.output_tokens, Some(4));
        assert_eq!(remote.cache_read_tokens, None);
        assert_eq!(remote.cache_write_tokens, Some(2));
        assert_eq!(remote.reported_cost_usd.as_deref(), Some("0.000000123"));
        assert!(remote.partial);
    }
}
