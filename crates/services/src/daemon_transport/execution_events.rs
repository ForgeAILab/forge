use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db::{
    Execution, ExecutionLeaseMutation, ExecutionRepo, ExecutionStatus, PlacementState,
    RecordExecutionProgress, RenewExecutionLease, TaskRepo, UpdateExecution,
    WorkspacePlacementRepo, WorkspaceRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::{LogKind, LogStream, LogWriter};
use serde_json::json;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    daemon_transport::{
        execution_lease_owner, DaemonExecutionEventHandler, DaemonTerminalDisposition,
    },
    task_service::logs::execution_logs_path,
    Result, ServiceError, TaskService,
};

const REMOTE_LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;
/// The daemon command stream sends a transport heartbeat every 20 seconds.
/// Give an authenticated owner enough room for one delayed frame while still
/// allowing the monitor to detect an unreachable owner before socket closure.
const REMOTE_EXECUTION_LEASE_SECONDS: i64 = 60;

pub struct ServerExecutionEventSink {
    db: Arc<db::SqliteDb>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    task_service: Mutex<Option<Weak<TaskService>>>,
    connection_registry: Mutex<Option<Weak<crate::daemon_transport::DaemonConnectionRegistry>>>,
    terminal_reports: Mutex<HashMap<String, TerminalReportRecord>>,
    writers: AsyncMutex<HashMap<String, Arc<AsyncMutex<LogWriter>>>>,
}

#[derive(Debug, Clone)]
enum TerminalReportRecord {
    Pending {
        daemon_id: String,
        fingerprint: String,
    },
    Committed {
        daemon_id: String,
        fingerprint: String,
    },
}

impl ServerExecutionEventSink {
    pub fn new(db: Arc<db::SqliteDb>, event_bus: Arc<EventBus>, workspace_root: PathBuf) -> Self {
        Self {
            db,
            event_bus,
            workspace_root,
            task_service: Mutex::new(None),
            connection_registry: Mutex::new(None),
            terminal_reports: Mutex::new(HashMap::new()),
            writers: AsyncMutex::new(HashMap::new()),
        }
    }

    pub fn set_task_service(&self, task_service: Weak<TaskService>) {
        *lock(&self.task_service) = Some(task_service);
    }

    pub fn set_connection_registry(
        &self,
        registry: Weak<crate::daemon_transport::DaemonConnectionRegistry>,
    ) {
        *lock(&self.connection_registry) = Some(registry);
    }

    fn connection_is_current(&self, daemon_id: &str, connection_id: u64) -> bool {
        lock(&self.connection_registry)
            .as_ref()
            .and_then(Weak::upgrade)
            .is_none_or(|registry| registry.is_current(daemon_id, connection_id))
    }

    async fn writer_for(
        &self,
        notification: &api_types::ExecutionLogNotification,
        execution: &Execution,
    ) -> Result<Arc<AsyncMutex<LogWriter>>> {
        if let Some(writer) = self.writers.lock().await.get(&notification.execution_id) {
            return Ok(Arc::clone(writer));
        }

        let logs_path = match execution.logs_path.clone() {
            Some(path) => path,
            None => {
                let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
                let workspace_task_id =
                    if let Some(workspace_id) = execution.workspace_id.as_deref() {
                        WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                            .await?
                            .map(|workspace| workspace.task_id)
                            .unwrap_or_else(|| task.id.clone())
                    } else {
                        task.id.clone()
                    };
                let path = execution_logs_path(
                    &self.workspace_root,
                    &task.project_id,
                    &workspace_task_id,
                    &execution.id,
                );
                ExecutionRepo::update(
                    &*self.db,
                    UpdateExecution {
                        id: execution.id.clone(),
                        status: None,
                        stop_reason: None,
                        stopped_by: None,
                        resume_policy: None,
                        stopped_at: None,
                        agent_session_id: None,
                        agent_message_id: None,
                        // Log persistence must not masquerade as execution
                        // liveness. Semantic progress is recorded by the
                        // owner/version CAS in `handle_log`.
                        last_activity_at: None,
                        summary: None,
                        logs_path: Some(Some(path.clone())),
                        before_sha: None,
                        after_sha: None,
                        error: None,
                        executor_config_snapshot_json: None,
                        updated_at: db::now_rfc3339(),
                    },
                )
                .await?;
                path
            }
        };

        let writer = Arc::new(AsyncMutex::new(LogWriter::new(
            logs_path,
            notification.execution_id.clone(),
            REMOTE_LOG_MAX_BYTES,
        )));
        self.writers
            .lock()
            .await
            .insert(notification.execution_id.clone(), Arc::clone(&writer));
        Ok(writer)
    }

    async fn authorized_execution(
        &self,
        daemon_id: &str,
        connection_id: u64,
        execution_id: &str,
    ) -> Result<Option<Execution>> {
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(None);
        }
        let lease_owner = execution_lease_owner(daemon_id, connection_id);
        let Some(execution) = ExecutionRepo::get_by_id(&*self.db, execution_id).await? else {
            tracing::warn!(
                sending_daemon = %daemon_id,
                execution_id = %execution_id,
                "dropping execution notification for missing execution"
            );
            return Ok(None);
        };

        let execution = self
            .resume_suspended_notification(daemon_id, connection_id, execution)
            .await?;

        // The daemon connection is authenticated before it reaches this
        // handler.  The execution lease is the authoritative ownership
        // relation; the mutable Agent daemon binding is only a legacy routing
        // hint and cannot authorize a stale runner after a lease takeover.
        if execution.lease_owner.as_deref() == Some(lease_owner.as_str()) {
            let now = Utc::now();
            let lease_expired = execution
                .lease_expires_at
                .as_deref()
                .and_then(parse_rfc3339)
                .is_none_or(|expires_at| expires_at <= now);
            let hard_deadline_reached = execution
                .hard_deadline_at
                .as_deref()
                .and_then(parse_rfc3339)
                .is_none_or(|deadline| deadline <= now);
            if lease_expired || hard_deadline_reached {
                tracing::debug!(
                    sending_daemon = %daemon_id,
                    execution_id = %execution_id,
                    lease_expired,
                    hard_deadline_reached,
                    "rejecting execution notification from an expired lease"
                );
                return Ok(None);
            }
            return Ok(Some(execution));
        }

        tracing::warn!(
            sending_daemon = %daemon_id,
            execution_id = %execution_id,
            "rejecting execution notification: daemon does not own this execution"
        );
        Ok(None)
    }

    async fn resume_suspended_notification(
        &self,
        daemon_id: &str,
        connection_id: u64,
        mut execution: Execution,
    ) -> Result<Execution> {
        let Some(workspace_id) = execution.workspace_id.as_deref() else {
            return Ok(execution);
        };
        let Some(mut placement) =
            WorkspacePlacementRepo::get_by_workspace_id(&*self.db, workspace_id).await?
        else {
            return Ok(execution);
        };
        let owner = execution_lease_owner(daemon_id, connection_id);
        let prefix = format!("daemon:{daemon_id}:connection:");
        if crate::recovery::placement_execution_daemon_id(&placement) != Some(daemon_id)
            || execution
                .lease_owner
                .as_deref()
                .is_none_or(|owner| !owner.starts_with(&prefix))
        {
            return Ok(execution);
        }
        if execution
            .lease_expires_at
            .as_deref()
            .and_then(parse_rfc3339)
            .is_none_or(|expires_at| expires_at <= Utc::now())
        {
            crate::recovery::suspend_expired_remote_execution(
                &self.db,
                &self.event_bus,
                &execution,
            )
            .await?;
            placement = WorkspacePlacementRepo::get_by_id(&*self.db, &placement.id)
                .await?
                .ok_or_else(|| ServiceError::not_found("placement", &placement.id))?;
        }
        if placement.state == PlacementState::Ready
            && execution.status == ExecutionStatus::Running
            && execution.lease_owner.as_deref() != Some(owner.as_str())
        {
            crate::recovery::disconnect_daemon_placements(&self.db, &self.event_bus, daemon_id)
                .await?;
            placement = WorkspacePlacementRepo::get_by_id(&*self.db, &placement.id)
                .await?
                .ok_or_else(|| ServiceError::not_found("placement", &placement.id))?;
        }
        if placement.state != PlacementState::Disconnected {
            return Ok(execution);
        }
        for _ in 0..3 {
            if !self.connection_is_current(daemon_id, connection_id)
                || execution.status != ExecutionStatus::Running
                || execution
                    .lease_owner
                    .as_deref()
                    .is_none_or(|owner| !owner.starts_with(&prefix))
            {
                return Ok(execution);
            }
            let now = Utc::now();
            if execution.lease_owner.as_deref() == Some(owner.as_str())
                && execution
                    .lease_expires_at
                    .as_deref()
                    .and_then(parse_rfc3339)
                    .is_some_and(|expires_at| expires_at > now)
            {
                return Ok(execution);
            }
            match WorkspacePlacementRepo::resume_disconnected_execution_lease(
                &*self.db,
                &placement,
                &execution,
                RenewExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution.execution_version,
                    owner: owner.clone(),
                    lease_expires_at: (now
                        + ChronoDuration::seconds(REMOTE_EXECUTION_LEASE_SECONDS))
                    .to_rfc3339(),
                    now: now.to_rfc3339(),
                },
            )
            .await?
            {
                ExecutionLeaseMutation::Updated(resumed) => return Ok(resumed),
                ExecutionLeaseMutation::Concurrent {
                    current: Some(current),
                } => execution = current,
                ExecutionLeaseMutation::Concurrent { current: None }
                | ExecutionLeaseMutation::HardDeadline { .. } => return Ok(execution),
            }
        }
        Ok(execution)
    }

    async fn record_semantic_progress(
        &self,
        daemon_id: &str,
        connection_id: u64,
        execution: &Execution,
        progress_at: &str,
    ) -> Result<bool> {
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(false);
        }
        let lease_owner = execution_lease_owner(daemon_id, connection_id);
        if execution.status != ExecutionStatus::Running
            || execution.lease_owner.as_deref() != Some(lease_owner.as_str())
        {
            return Ok(false);
        }
        if progress_recorded_recently(execution.last_progress_at.as_deref(), progress_at) {
            // Ownership is already proven above; a log line this close to
            // the recorded watermark would only add another write.
            return Ok(true);
        }

        // A heartbeat and a semantic event can arrive concurrently. Retry a
        // single CAS with the current row so an otherwise valid log does not
        // disappear merely because the server renewed the same owner first.
        let mut candidate = execution.clone();
        for _ in 0..2 {
            let outcome = ExecutionRepo::record_progress(
                &*self.db,
                RecordExecutionProgress {
                    execution_id: candidate.id.clone(),
                    expected_version: candidate.execution_version,
                    owner: lease_owner.clone(),
                    progress_at: progress_at.to_owned(),
                    now: db::now_rfc3339(),
                },
            )
            .await?;
            match outcome {
                ExecutionLeaseMutation::Updated(_) => return Ok(true),
                ExecutionLeaseMutation::HardDeadline { .. } => return Ok(false),
                ExecutionLeaseMutation::Concurrent { current } => {
                    let Some(current) = current else {
                        return Ok(false);
                    };
                    if current.status != ExecutionStatus::Running
                        || current.lease_owner.as_deref() != Some(lease_owner.as_str())
                    {
                        return Ok(false);
                    }
                    candidate = current;
                }
            }
        }
        Ok(false)
    }

    async fn renew_owned_remote_executions(
        &self,
        daemon_id: &str,
        connection_id: u64,
    ) -> Result<()> {
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(());
        }
        let lease_owner = execution_lease_owner(daemon_id, connection_id);
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let proposed_expiry = now + ChronoDuration::seconds(REMOTE_EXECUTION_LEASE_SECONDS);

        for execution in ExecutionRepo::list_running(&*self.db).await? {
            if !self.connection_is_current(daemon_id, connection_id) {
                return Ok(());
            }
            // The owner is taken from the authenticated transport identity,
            // never from heartbeat payload data. Rows without a claimed lease
            // are left to the scheduler/dispatch claim path.
            if execution
                .lease_owner
                .as_deref()
                .is_none_or(|owner| !owner.starts_with(&format!("daemon:{daemon_id}:connection:")))
            {
                continue;
            }
            let execution = self
                .resume_suspended_notification(daemon_id, connection_id, execution)
                .await?;
            if !self.connection_is_current(daemon_id, connection_id)
                || execution.lease_owner.as_deref() != Some(lease_owner.as_str())
            {
                continue;
            }

            let lease_expires_at = execution
                .hard_deadline_at
                .as_deref()
                .and_then(parse_rfc3339)
                .map_or(proposed_expiry, |hard_deadline| {
                    std::cmp::min(proposed_expiry, hard_deadline)
                });
            if lease_expires_at <= now {
                // The repository will classify this as a hard-deadline
                // refusal; avoid moving a deadline backwards in the common
                // case and let recovery own terminalization.
                continue;
            }

            match ExecutionRepo::renew_lease(
                &*self.db,
                RenewExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution.execution_version,
                    owner: lease_owner.clone(),
                    lease_expires_at: lease_expires_at.to_rfc3339(),
                    now: now_text.clone(),
                },
            )
            .await?
            {
                ExecutionLeaseMutation::Updated(_) => {}
                ExecutionLeaseMutation::Concurrent { .. }
                | ExecutionLeaseMutation::HardDeadline { .. } => {
                    tracing::debug!(
                        daemon_id = %daemon_id,
                        execution_id = %execution.id,
                        "remote execution heartbeat lost its lease CAS"
                    );
                }
            }
        }
        Ok(())
    }

    /// Keep the terminal identity/fingerprint that has completed through this
    /// sink instance. The TaskService/DB transaction remains the accounting
    /// authority; this small in-process index lets reconnect duplicates become
    /// an acknowledgement rather than rerunning the completion side effects,
    /// while a differing payload is surfaced as a conflict.
    fn terminal_replay_disposition(
        &self,
        daemon_id: &str,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<Option<DaemonTerminalDisposition>> {
        let fingerprint = terminal_fingerprint(notification)?;
        let reports = lock(&self.terminal_reports);
        Ok(reports
            .get(&notification.terminal_report_id)
            .map(|record| terminal_record_disposition(record, daemon_id, &fingerprint)))
    }

    fn reserve_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<Option<DaemonTerminalDisposition>> {
        // The connection registry is the authoritative incarnation proof.
        // Check it immediately before taking the report reservation so a
        // delayed frame cannot reserve an id after its socket was replaced.
        // The check in `handle_terminal_inner` remains a cheap fast path, but
        // this one closes the gap between that check and the mutex lock.
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(Some(DaemonTerminalDisposition::Ignore));
        }
        let fingerprint = terminal_fingerprint(notification)?;
        let mut reports = lock(&self.terminal_reports);
        if let Some(record) = reports.get(&notification.terminal_report_id) {
            return Ok(Some(terminal_record_disposition(
                record,
                daemon_id,
                &fingerprint,
            )));
        }
        reports.insert(
            notification.terminal_report_id.clone(),
            TerminalReportRecord::Pending {
                daemon_id: daemon_id.to_owned(),
                fingerprint,
            },
        );
        Ok(None)
    }

    fn release_terminal(
        &self,
        daemon_id: &str,
        notification: &api_types::ExecutionTerminalNotification,
    ) {
        let Ok(fingerprint) = terminal_fingerprint(notification) else {
            return;
        };
        let mut reports = lock(&self.terminal_reports);
        if matches!(
            reports.get(&notification.terminal_report_id),
            Some(TerminalReportRecord::Pending {
                daemon_id: existing_daemon,
                fingerprint: existing,
            }) if existing_daemon == daemon_id && existing == &fingerprint
        ) {
            reports.remove(&notification.terminal_report_id);
        }
    }

    fn remember_terminal(
        &self,
        daemon_id: &str,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<()> {
        let fingerprint = terminal_fingerprint(notification)?;
        let mut reports = lock(&self.terminal_reports);
        if let Some(record) = reports.get_mut(&notification.terminal_report_id) {
            match record {
                TerminalReportRecord::Pending {
                    daemon_id: existing_daemon,
                    fingerprint: existing,
                } => {
                    if existing_daemon == daemon_id && existing == &fingerprint {
                        *record = TerminalReportRecord::Committed {
                            daemon_id: daemon_id.to_owned(),
                            fingerprint,
                        };
                        return Ok(());
                    }
                }
                TerminalReportRecord::Committed {
                    daemon_id: existing_daemon,
                    fingerprint: existing,
                } if existing_daemon == daemon_id && existing == &fingerprint => {
                    return Ok(());
                }
                TerminalReportRecord::Committed { .. } => {}
            }
            return Err(ServiceError::conflict(
                "terminal report ID was reused with a different payload",
            ));
        }
        reports.insert(
            notification.terminal_report_id.clone(),
            TerminalReportRecord::Committed {
                daemon_id: daemon_id.to_owned(),
                fingerprint,
            },
        );
        Ok(())
    }
}

#[async_trait]
impl DaemonExecutionEventHandler for ServerExecutionEventSink {
    async fn handle_disconnected(&self, daemon_id: &str) -> Result<()> {
        crate::recovery::disconnect_daemon_placements(&self.db, &self.event_bus, daemon_id).await?;
        Ok(())
    }

    async fn handle_workspace_cleanup(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::WorkspaceCleanupResult,
    ) -> Result<DaemonTerminalDisposition> {
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        let disposition =
            crate::recovery::apply_owner_cleanup(&self.db, daemon_id, &notification).await?;
        if disposition != DaemonTerminalDisposition::Acknowledge {
            return Ok(disposition);
        }
        let registry = lock(&self.connection_registry)
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(registry) = registry {
            // Retain the replayed result too: the original request may have
            // lost its response, and its journal ACK can be interrupted.
            let placement_id = sqlx::query_scalar::<_, String>(
                "SELECT id FROM workspace_placement WHERE owner_kind = 'daemon' AND daemon_id = ?
                 AND workspace_handle = ? AND generation = ? AND state = 'cleaned'",
            )
            .bind(daemon_id)
            .bind(&notification.workspace_handle)
            .bind(
                i64::try_from(notification.generation)
                    .map_err(|_| ServiceError::invalid_operation("invalid cleanup generation"))?,
            )
            .fetch_optional(self.db.pool())
            .await?;
            let Some(placement_id) = placement_id else {
                return Ok(DaemonTerminalDisposition::Ignore);
            };
            let placement = WorkspacePlacementRepo::get_by_id(&*self.db, &placement_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("placement", &placement_id))?;
            let intent = sqlx::query_scalar::<_, String>(
                "SELECT outcome_json FROM command_receipt WHERE principal_id = 'workspace-backend'
                 AND operation = 'daemon.workspace.cleanup.intent' AND correlation_id = ?
                 AND json_extract(outcome_json, '$.metadata.placement_id') = ?
                 ORDER BY committed_at DESC, id DESC LIMIT 1",
            )
            .bind(&notification.operation_id)
            .bind(&placement.id)
            .fetch_optional(self.db.pool())
            .await?;
            let request = match intent {
                Some(intent) => {
                    let intent: serde_json::Value =
                        serde_json::from_str(&intent).map_err(|error| {
                            ServiceError::invalid_operation(format!(
                                "invalid cleanup intent: {error}"
                            ))
                        })?;
                    intent["owner_result"]["request"].clone()
                }
                None => serde_json::to_value(api_types::WorkspaceCleanupParams {
                    fence: api_types::WorkspaceMutationFence {
                        daemon_id: daemon_id.to_owned(),
                        runtime_id: placement.runtime_id.ok_or_else(|| {
                            ServiceError::invalid_operation("cleanup placement has no runtime")
                        })?,
                        placement_id: placement.id,
                        operation_id: notification.operation_id.clone(),
                        generation: notification.generation,
                        expected: api_types::WorkspaceOperationExpected::Version { version: 0 },
                    },
                    workspace_handle: notification.workspace_handle.clone(),
                })
                .map_err(|error| {
                    ServiceError::invalid_operation(format!("invalid cleanup receipt: {error}"))
                })?,
            };
            let client = super::workspace_client::DaemonWorkspaceClient::new(registry)
                .with_receipts(Arc::clone(&self.db));
            client
                .retain_result(
                    daemon_id,
                    api_types::METHOD_WORKSPACE_CLEANUP,
                    &request,
                    &serde_json::to_value(&notification)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                )
                .await
                .map_err(|error| match error {
                    super::workspace_client::WorkspaceClientError::Transport(error) => error,
                    super::workspace_client::WorkspaceClientError::Daemon(error) => {
                        ServiceError::invalid_operation(error.message)
                    }
                })?;
        }
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        Ok(disposition)
    }

    async fn handle_heartbeat(&self, daemon_id: &str, connection_id: u64, _seq: u64) -> Result<()> {
        self.renew_owned_remote_executions(daemon_id, connection_id)
            .await
    }

    async fn handle_log(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionLogNotification,
    ) -> Result<()> {
        let Some(execution) = self
            .authorized_execution(daemon_id, connection_id, &notification.execution_id)
            .await?
        else {
            return Ok(());
        };

        // Executor-generated heartbeat log records are diagnostic output, not
        // semantic progress.  The command-stream heartbeat above owns lease
        // renewal so a quiet provider cannot accidentally become dependent on
        // these optional records.
        if notification.log_stream.as_deref() != Some("heartbeat")
            && !self
                .record_semantic_progress(daemon_id, connection_id, &execution, &notification.ts)
                .await?
        {
            tracing::debug!(
                daemon_id = %daemon_id,
                execution_id = %notification.execution_id,
                "rejecting remote execution log from a stale lease"
            );
            return Ok(());
        }

        // The authorization read and semantic-progress CAS above can yield
        // while a replacement socket is registered.  Recheck before touching
        // the durable log writer so a delayed frame from the old incarnation
        // cannot append output after ownership moved.
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(());
        }

        let writer = self.writer_for(&notification, &execution).await?;
        let kind = notification
            .kind
            .as_deref()
            .and_then(|value| value.parse::<LogKind>().ok())
            .unwrap_or(match notification.stream.as_str() {
                "stderr" => LogKind::Stderr,
                _ => LogKind::Stdout,
            });
        let stream = match notification.log_stream.as_deref() {
            Some("heartbeat") => LogStream::Heartbeat,
            _ => LogStream::Main,
        };
        let payload = notification.payload.clone().unwrap_or_else(|| {
            json!({
                "line": notification.line,
                "daemon_seq": notification.seq,
                "daemon_ts": notification.ts,
                "stream": notification.stream,
            })
        });
        writer
            .lock()
            .await
            .write(kind.clone(), stream.clone(), payload.clone())
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!("failed to write execution log: {error}"))
            })?;

        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(());
        }

        let execution_id = notification.execution_id.clone();
        let log = json!({
            "schema_version": 1,
            "sequence": notification.seq,
            "timestamp": notification.ts,
            "execution_id": execution_id.clone(),
            "kind": kind,
            "stream": stream,
            "payload": payload,
            "truncated": notification.truncated.unwrap_or(false),
        });
        self.event_bus.publish(ForgeEvent {
            event_type: "execution.log".to_owned(),
            entity_id: execution_id,
            timestamp: event_timestamp(),
            context: EventContext::ExecutionLog {
                task_id: execution.task_id,
                log,
                logs: None,
            },
        });
        Ok(())
    }

    async fn handle_terminal(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<()> {
        self.handle_terminal_inner(daemon_id, connection_id, notification)
            .await
            .map(|_| ())
    }

    async fn handle_terminal_with_ack(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition> {
        self.handle_terminal_inner(daemon_id, connection_id, notification)
            .await
    }
}

impl ServerExecutionEventSink {
    async fn handle_terminal_inner(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition> {
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        if let Some(disposition) = self.reserve_terminal(daemon_id, connection_id, &notification)? {
            return Ok(disposition);
        }

        let result = self
            .handle_terminal_inner_reserved(daemon_id, connection_id, &notification)
            .await;
        if result.is_err()
            || matches!(
                &result,
                Ok(DaemonTerminalDisposition::Ignore | DaemonTerminalDisposition::Conflict)
            )
        {
            self.release_terminal(daemon_id, &notification);
        }
        result
    }

    async fn handle_terminal_inner_reserved(
        &self,
        daemon_id: &str,
        connection_id: u64,
        notification: &api_types::ExecutionTerminalNotification,
    ) -> Result<DaemonTerminalDisposition> {
        let Some(task_service) = lock(&self.task_service).as_ref().and_then(Weak::upgrade) else {
            return Err(ServiceError::invalid_operation(
                "task service is unavailable for daemon terminal notification",
            ));
        };
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        if let Some(execution) =
            ExecutionRepo::get_by_id(&*self.db, &notification.execution_id).await?
        {
            if crate::recovery::owner_execution_failure_cause(&execution).is_some() {
                task_service
                    .record_late_remote_terminal(daemon_id, connection_id, notification)
                    .await?;
                return Ok(DaemonTerminalDisposition::Ignore);
            }
            self.resume_suspended_notification(daemon_id, connection_id, execution)
                .await?;
        }
        if !self.connection_is_current(daemon_id, connection_id) {
            return Ok(DaemonTerminalDisposition::Ignore);
        }
        let outcome = task_service
            .complete_remote_execution(daemon_id, connection_id, notification.clone())
            .await?;
        match outcome {
            db::ExecutionTerminalOutcome::Committed {
                execution,
                replayed,
                ..
            } => {
                // A receipt can survive a crash before outbox ingestion. The
                // owner's report stays retained until these idempotent writes
                // also succeed, including on an exact receipt replay.
                if !notification.outbox_entries.is_empty() {
                    let agent_id = execution.agent_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation("terminal outbox has no execution Agent")
                    })?;
                    let report = task_service
                        .ingest_execution_outbox_entries(
                            &crate::native_tools::ExecutionOutboxInput {
                                task_id: &execution.task_id,
                                execution_id: &execution.id,
                                agent_id,
                                role: Some(&execution.role),
                                worktree_path: "",
                            },
                            notification.outbox_entries.clone(),
                        )
                        .await;
                    if report.worklog_entries + report.evidence_items == 0
                        && report.rejected.is_empty()
                    {
                        return Err(ServiceError::invalid_operation(
                            "terminal outbox ingestion is unavailable",
                        ));
                    }
                    if !report.rejected.is_empty() {
                        return Err(ServiceError::invalid_operation(format!(
                            "terminal outbox ingestion is incomplete: {}",
                            report.rejected.join("; "),
                        )));
                    }
                }
                // A durable receipt replay is already fully committed. Do
                // not publish/cascade/remove mutable transport state again;
                // the only required side effect is the transport ACK.
                if replayed {
                    self.remember_terminal(daemon_id, notification)?;
                    return Ok(DaemonTerminalDisposition::Acknowledge);
                }
                self.remember_terminal(daemon_id, notification)?;
                self.writers.lock().await.remove(&notification.execution_id);
                if execution.status != ExecutionStatus::Running
                    && !crate::recovery::execution_placement_is_disconnected(&self.db, &execution)
                        .await?
                {
                    task_service
                        .maybe_cascade_executor_completion(&notification.execution_id)
                        .await?;
                }
                Ok(DaemonTerminalDisposition::Acknowledge)
            }
            db::ExecutionTerminalOutcome::Concurrent { .. } => {
                // A durable terminal-report receipt is consulted by the
                // TaskService before it returns this outcome. The in-process
                // index handles same-process replay; a concurrent result with
                // no matching receipt still needs authorization before it can
                // be classified as a late/conflicting report.
                if let Some(
                    disposition @ (DaemonTerminalDisposition::Acknowledge
                    | DaemonTerminalDisposition::Conflict),
                ) = self.terminal_replay_disposition(daemon_id, notification)?
                {
                    return Ok(disposition);
                }
                if self
                    .authorized_execution(daemon_id, connection_id, &notification.execution_id)
                    .await?
                    .is_none()
                {
                    task_service
                        .record_late_remote_terminal(daemon_id, connection_id, notification)
                        .await?;
                    return Ok(DaemonTerminalDisposition::Ignore);
                }
                // A heartbeat may have advanced execution_version between the
                // completion attempt and this check. Since this connection
                // still owns the live execution, retain the report for retry
                // and surface the identity conflict without acknowledging it.
                task_service
                    .record_late_remote_terminal(daemon_id, connection_id, notification)
                    .await?;
                Ok(DaemonTerminalDisposition::Conflict)
            }
        }
    }
}

fn terminal_fingerprint(notification: &api_types::ExecutionTerminalNotification) -> Result<String> {
    if notification.terminal_report_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "terminal notification has an empty terminal_report_id",
        ));
    }
    serde_json::to_string(notification).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "failed to fingerprint terminal notification: {error}"
        ))
    })
}

fn terminal_record_disposition(
    record: &TerminalReportRecord,
    daemon_id: &str,
    fingerprint: &str,
) -> DaemonTerminalDisposition {
    let (record_daemon_id, existing, committed) = match record {
        TerminalReportRecord::Pending {
            daemon_id,
            fingerprint,
        } => (daemon_id, fingerprint, false),
        TerminalReportRecord::Committed {
            daemon_id,
            fingerprint,
        } => (daemon_id, fingerprint, true),
    };
    if record_daemon_id != daemon_id {
        DaemonTerminalDisposition::Conflict
    } else if existing == fingerprint {
        if committed {
            DaemonTerminalDisposition::Acknowledge
        } else {
            DaemonTerminalDisposition::Pending
        }
    } else {
        DaemonTerminalDisposition::Conflict
    }
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

/// Remote log notifications arrive per line; the liveness watermark only
/// needs to move once per native stream interval.
fn progress_recorded_recently(last_progress_at: Option<&str>, progress_at: &str) -> bool {
    let (Some(last), Some(candidate)) = (
        last_progress_at.and_then(parse_rfc3339),
        parse_rfc3339(progress_at),
    ) else {
        return false;
    };
    candidate.signed_duration_since(last)
        < ChronoDuration::from_std(crate::turn_log_sink::STREAM_PROGRESS_INTERVAL)
            .unwrap_or_else(|_| ChronoDuration::seconds(1))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn notification() -> api_types::ExecutionTerminalNotification {
        serde_json::from_value(json!({
            "terminal_report_id": "terminal-report-ownership",
            "execution_id": "execution-ownership",
            "exit_code": 0,
            "signal": null,
            "error": null,
            "ts": "2026-09-08T00:00:01Z",
            "status": "completed",
            "usage_reports": []
        }))
        .expect("terminal notification fixture parses")
    }

    #[test]
    fn remote_progress_is_recorded_at_most_once_per_interval() {
        let last = Some("2026-09-08T00:00:00Z");
        assert!(progress_recorded_recently(last, "2026-09-08T00:00:00.500Z"));
        assert!(!progress_recorded_recently(last, "2026-09-08T00:00:01Z"));
        assert!(!progress_recorded_recently(
            None,
            "2026-09-08T00:00:00.500Z"
        ));
        assert!(!progress_recorded_recently(last, "not-a-timestamp"));
    }

    #[test]
    fn exact_in_memory_replay_from_another_daemon_is_a_conflict() {
        let notification = notification();
        let fingerprint = terminal_fingerprint(&notification).expect("fingerprint computes");
        let record = TerminalReportRecord::Committed {
            daemon_id: "owner-daemon".to_owned(),
            fingerprint: fingerprint.clone(),
        };

        assert_eq!(
            terminal_record_disposition(&record, "owner-daemon", &fingerprint,),
            DaemonTerminalDisposition::Acknowledge
        );
        assert_eq!(
            terminal_record_disposition(&record, "different-daemon", &fingerprint),
            DaemonTerminalDisposition::Conflict
        );
    }

    #[test]
    fn exact_replay_waits_for_pending_terminal_outbox() {
        let fingerprint = terminal_fingerprint(&notification()).unwrap();
        let record = TerminalReportRecord::Pending {
            daemon_id: "owner-daemon".to_owned(),
            fingerprint: fingerprint.clone(),
        };
        assert_eq!(
            terminal_record_disposition(&record, "owner-daemon", &fingerprint),
            DaemonTerminalDisposition::Pending
        );
    }
}
