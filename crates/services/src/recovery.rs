use crate::{
    daemon_transport::DaemonConnectionRegistry, embedded_daemon::is_embedded_daemon_machine,
    workflow::engine::WorkflowEngine, workspace_backend::EmbeddedWorkspaceBackend, Result,
    ServiceError, TaskService,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
#[cfg(test)]
use db::UpdateExecution;
use db::{
    now_rfc3339, Agent, AgentListQuery, AgentRepo, AgentSessionRepo, AgentStatus, Daemon,
    DaemonRepo, Execution, ExecutionLeaseDisposition, ExecutionProgressWarningOutcome,
    ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome, MarkUsageInvocationUnsettled,
    PageRequest, PlacementFailureCause, PlacementOwnerKind, PlacementState, Project, ProjectRepo,
    RecordExecutionProgressWarning, ResumePolicy, SortBy, SortOrder, SqliteDb, StopReason, Task,
    TaskListQuery, TaskRepo, TerminalizeExecution, UpdateAgent, UpdateTaskStatus,
    UpdateWorkspacePlacement, UsageLedgerRepo, WorkspaceLeaseRepo, WorkspacePlacement,
    WorkspacePlacementRepo, WorkspaceRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::TaskExecutor;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::Instrument;

async fn discard_execution_plan_artifacts(db: &SqliteDb, execution: &Execution) {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return;
    };
    let workspace = match WorkspaceRepo::get_by_id(db, workspace_id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(
                execution_id = %execution.id,
                %error,
                "failed to resolve workspace while cleaning terminal plan artifacts"
            );
            return;
        }
    };
    let Ok(path) = EmbeddedWorkspaceBackend::recorded_server_path(db, &workspace).await else {
        return;
    };
    let worktree = path.as_path();
    if let Some(outbox) = executors::execution_outbox_path(worktree, &execution.id) {
        if let Err(error) = std::fs::remove_dir_all(&outbox) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    execution_id = %execution.id,
                    path = %outbox.display(),
                    %error,
                    "failed to remove terminal execution outbox"
                );
            }
        }
    }
    crate::task_service::execution::discard_execution_plan_stage(
        &path.to_string_lossy(),
        &execution.id,
    );
}

#[derive(Clone)]
pub struct CrashRecovery {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl CrashRecovery {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<u64> {
        self.run_recovery().await
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_recovery(&self) -> Result<u64> {
        // A server restart loses its sockets, not the CLI running on an owner.
        // Persist that suspension before either execution or grant recovery.
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Ready).await?
        {
            if let Some(daemon_id) = placement_execution_daemon_id(&placement) {
                if placement.owner_kind == PlacementOwnerKind::Daemon
                    || DaemonRepo::get_by_id(&*self.db, daemon_id)
                        .await?
                        .is_some_and(|daemon| !is_embedded_daemon_machine(&daemon.machine_id))
                {
                    disconnect_daemon_placements(&self.db, &self.event_bus, daemon_id).await?;
                }
            }
        }
        // Expire stale grants before recovering active Tasks. The recovery
        // pass below then sees the still-running attempt and requeues/blocks
        // it through the normal crash-recovery state machine.
        expire_workspace_leases(&self.db, &self.event_bus, None, None, false).await?;

        // A user cancellation can commit the execution terminal CAS and move
        // still-running local provider calls to pending settlement immediately
        // before the process exits. There is no daemon to replay those calls
        // after restart, so repair that durable ambiguity before recovering
        // Tasks. Daemon-owned invocations remain pending: their terminal
        // report is replayable across a reconnect.
        let unsettled = reconcile_unreplayable_local_usage_invocations(&self.db).await?;
        if unsettled > 0 {
            tracing::info!(
                unsettled,
                "marked unreplayable local usage invocations unsettled during crash recovery"
            );
        }

        // Native (in-process) runtime sessions cannot survive a restart, so
        // any 'starting'/'ready'/'running'/'degraded' native session left by
        // a previous process lies about liveness. Suspend them: the reuse
        // path (get_active_agent_session) then re-establishes a fresh session
        // on next use. That session rebuilds its history from the chat
        // transcript and retires the old runtime's LCM timeline on first bind.
        let suspended_sessions =
            AgentSessionRepo::suspend_stale_native_sessions(&*self.db, &now_rfc3339()).await?;
        if suspended_sessions > 0 {
            tracing::info!(
                suspended_sessions,
                "suspended stale native agent sessions from previous process"
            );
        }
        let tasks = self.list_in_progress_tasks(None).await?;
        let mut recovered = 0;

        for task in tasks {
            // One live recovery command per Task and running-execution set:
            // repeated restarts before the queue drains do not stack copies.
            let mut running = ExecutionRepo::list_running_by_task(&*self.db, &task.id)
                .await?
                .into_iter()
                .map(|execution| execution.id)
                .collect::<Vec<_>>();
            running.sort();
            let key = format!("recover_task_after_restart:{}", running.join(","));
            let prefix = format!("{key}:");
            let queued: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND substr(causation_key,1,length(?))=?)")
                .bind(&task.id)
                .bind(&prefix)
                .bind(&prefix)
                .fetch_one(self.db.pool())
                .await?;
            if queued {
                continue;
            }
            let id = db::new_uuid_v4();
            db::TaskStepRepo::enqueue_step(
                &*self.db,
                &db::EnqueueTaskStep {
                    id: id.clone(),
                    task_id: task.id.clone(),
                    kind: "command".into(),
                    payload_json: crate::task_service::commands::TaskCommand {
                        operation: "recover_task_after_restart".into(),
                        arguments: json!([task.id]),
                        preempt: false,
                    }
                    .payload_json()?,
                    causation_step_id: None,
                    causation_key: format!("{key}:{id}"),
                    chain_id: id,
                    chain_position: 1,
                    expected_status: task.status,
                    expected_version: task.version,
                    expected_epoch: None,
                    lane: "fast".into(),
                    available_at: now_rfc3339(),
                },
            )
            .await?;
        }

        recovered += sweep_stale_recovery_annotations(&self.db).await?;

        tracing::info!(recovered_tasks = recovered, "crash recovery completed");
        Ok(recovered)
    }

    #[tracing::instrument(skip(self), fields(agent_id = agent_id.unwrap_or("any")))]
    async fn list_in_progress_tasks(&self, agent_id: Option<&str>) -> Result<Vec<Task>> {
        list_in_progress_tasks(&self.db, agent_id).await
    }
}

async fn reconcile_unreplayable_local_usage_invocations(db: &SqliteDb) -> Result<u64> {
    let invocations = UsageLedgerRepo::list_usage_invocations_needing_settlement(db, 5000).await?;
    let mut marked = 0_u64;
    for invocation in invocations {
        let Some(execution_id) = invocation.execution_id.as_deref() else {
            continue;
        };
        let Some(execution) = ExecutionRepo::get_by_id(db, execution_id).await? else {
            continue;
        };
        if execution.status == ExecutionStatus::Running {
            continue;
        }
        let previous_owner = sqlx::query_scalar::<_, Option<String>>(
            "SELECT json_extract(payload_json, '$.previous_lease_owner')
             FROM domain_event
             WHERE entity_type = 'task'
               AND entity_id = ?
               AND event_type IN (
                   'execution.completed', 'execution.failed',
                   'execution.cancelled', 'execution.terminal_report.received'
               )
               AND json_extract(payload_json, '$.execution_id') = ?
             ORDER BY sequence DESC
             LIMIT 1",
        )
        .bind(&execution.task_id)
        .bind(&execution.id)
        .fetch_optional(db.pool())
        .await?
        .flatten();
        if previous_owner
            .as_deref()
            .is_some_and(|owner| owner.starts_with("daemon:"))
        {
            continue;
        }
        match UsageLedgerRepo::mark_usage_invocation_unsettled(
            db,
            MarkUsageInvocationUnsettled {
                id: invocation.id,
                expected_version: invocation.version,
                terminal_reason: "recovery_no_replayable_result".to_owned(),
                settled_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => marked += 1,
            Err(db::DbError::VersionConflict | db::DbError::IdempotencyConflict) => {
                // A late result may have settled the invocation while this
                // repair sweep was reading it. The newer lifecycle wins.
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(marked)
}

pub struct HeartbeatMonitor {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Option<Arc<TaskService>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    daemon_connections: Option<Arc<DaemonConnectionRegistry>>,
    check_interval: Duration,
    execution_stall_timeout: Duration,
    max_disconnect: Duration,
    placement_in_flight: Arc<Mutex<HashSet<String>>>,
    placement_workers: Arc<tokio::sync::Semaphore>,
    placement_worker_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    stopped: AtomicBool,
    stop_notify: tokio::sync::Notify,
}

/// How long a freshly created `dispatch-pending:` lease marker is left alone
/// before recovery may reclaim it. The local runner claims its lease within
/// milliseconds; this only has to outlast that handoff.
const DISPATCH_PENDING_GRACE_SECONDS: i64 = 30;

/// Prefix of the placeholder lease owner installed with a locally dispatched
/// execution, before its runner takes ownership.
const DISPATCH_PENDING_OWNER_PREFIX: &str = "dispatch-pending:";

/// Whether this expired-lease row is a dispatch marker still inside its grace
/// window, and so not yet evidence of a dead owner. A row past its hard
/// deadline is always eligible, marker or not.
fn is_unclaimed_dispatch_marker(execution: &Execution, now: &str) -> bool {
    let Some(owner) = execution.lease_owner.as_deref() else {
        return false;
    };
    if !owner.starts_with(DISPATCH_PENDING_OWNER_PREFIX) {
        return false;
    }
    if execution
        .hard_deadline_at
        .as_deref()
        .is_some_and(|deadline| deadline <= now)
    {
        return false;
    }
    let (Ok(created_at), Ok(now)) = (
        DateTime::parse_from_rfc3339(&execution.created_at),
        DateTime::parse_from_rfc3339(now),
    ) else {
        return false;
    };
    now.signed_duration_since(created_at) < ChronoDuration::seconds(DISPATCH_PENDING_GRACE_SECONDS)
}

impl HeartbeatMonitor {
    const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
    const DEFAULT_EXECUTION_STALL_TIMEOUT: Duration = Duration::from_secs(300);

    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self::with_check_interval(db, event_bus, Self::DEFAULT_CHECK_INTERVAL)
    }

    pub fn with_check_interval(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        check_interval: Duration,
    ) -> Self {
        Self {
            db,
            event_bus,
            task_service: None,
            task_executor: None,
            daemon_connections: None,
            check_interval,
            execution_stall_timeout: Self::DEFAULT_EXECUTION_STALL_TIMEOUT,
            max_disconnect: Duration::from_secs(config::DEFAULT_MAX_DISCONNECT_SECONDS),
            placement_in_flight: Arc::new(Mutex::new(HashSet::new())),
            placement_workers: Arc::new(tokio::sync::Semaphore::new(2)),
            placement_worker_handles: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            stop_notify: tokio::sync::Notify::new(),
        }
    }

    pub fn with_task_service(mut self, task_service: Arc<TaskService>) -> Self {
        self.task_service = Some(task_service);
        self
    }

    pub fn with_task_executor(mut self, task_executor: Arc<dyn TaskExecutor>) -> Self {
        self.task_executor = Some(task_executor);
        self
    }

    pub fn with_daemon_connections(
        mut self,
        daemon_connections: Arc<DaemonConnectionRegistry>,
    ) -> Self {
        self.daemon_connections = Some(daemon_connections);
        self
    }

    pub fn with_execution_stall_timeout(mut self, execution_stall_timeout: Duration) -> Self {
        self.execution_stall_timeout = execution_stall_timeout;
        self
    }

    pub fn with_max_disconnect(mut self, max_disconnect: Duration) -> Self {
        self.max_disconnect = max_disconnect;
        self
    }

    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
    ) -> tokio::task::JoinHandle<()> {
        self.start_with_check(workers, Duration::from_secs(300), |monitor| async move {
            monitor.check_timeouts().await
        })
    }

    fn start_with_check<F, Fut>(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        budget: Duration,
        check: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Arc<Self>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let check = Arc::new(check);
        let stop = Arc::clone(&self);
        workers.worker("heartbeat-monitor").with_stall_budget(budget).start_stoppable(move || stop.is_stopped(), move |worker| {
            let monitor = Arc::clone(&self);
            let check = Arc::clone(&check);
            async move {
                tracing::info!(check_interval_seconds = monitor.check_interval.as_secs(), "heartbeat monitor started");
                let result = worker.run(
                    || monitor.is_stopped(),
                    "heartbeat monitor check failed",
                    || check(Arc::clone(&monitor)),
                    || async {
                        tokio::select! {
                            _ = tokio::time::sleep(monitor.check_interval) => {}
                            _ = monitor.stop_notify.notified() => {}
                            _ = async {
                                match monitor.daemon_connections.as_ref() {
                                    Some(registry) => registry.reconciliation_notify().notified().await,
                                    None => std::future::pending::<()>().await,
                                }
                            } => {}
                        }
                    },
                ).await;
                tracing::info!("heartbeat monitor stopped");
                result
            }.instrument(tracing::info_span!("heartbeat.monitor"))
        })
    }

    #[tracing::instrument(skip(self))]
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.stop_notify.notify_one();
        let mut handles = self
            .placement_worker_handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for handle in handles.drain(..) {
            handle.abort();
        }
    }

    #[cfg(test)]
    pub(crate) async fn finish_placement_workers(&self) {
        let handles = std::mem::take(
            &mut *self
                .placement_worker_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for handle in handles {
            handle.await.expect("placement worker completes");
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    #[tracing::instrument(skip(self))]
    pub async fn check_once(&self) -> Result<u64> {
        // Heartbeat staleness can precede the socket's close/timeout. Persist
        // owner suspension before Agent or WorkspaceLease recovery sees it.
        let now = now_rfc3339();
        let owner_suspension = async {
            for execution in ExecutionRepo::list_expired_leases(&*self.db, &now, 500).await? {
                if !is_unclaimed_dispatch_marker(&execution, &now) {
                    if let Err(error) =
                        suspend_expired_remote_execution(&self.db, &self.event_bus, &execution).await
                    {
                        tracing::warn!(execution_id = %execution.id, %error, "owner suspension remains pending");
                    }
                }
            }
            Ok::<(), ServiceError>(())
        }
        .await;
        let owner_suspension_succeeded = match owner_suspension {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "expired owner suspension pass failed");
                false
            }
        };
        if let Err(error) = self.suspend_unreachable_placements().await {
            tracing::warn!(%error, "placement suspension remains pending");
        }
        let timed_out = self
            .check_agent_timeouts_for_tick(owner_suspension_succeeded)
            .await;

        if timed_out > 0 {
            tracing::info!(
                timed_out_agents = timed_out,
                "heartbeat monitor detected timed out agents"
            );
        }
        let workspace_lease_renewal_succeeded = match renew_workspace_leases(&self.db).await {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(%error, "workspace lease renewal pass failed");
                false
            }
        };
        let progress_warnings = match self.check_stale_progress().await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "execution progress warning pass failed");
                0
            }
        };
        let stalled = match self.check_stalled_executions().await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "execution owner lease expiry pass failed");
                0
            }
        };
        let expired = self
            .expire_workspace_leases_for_tick(workspace_lease_renewal_succeeded)
            .await;
        // Owner RPCs run after the core liveness pass. A bad placement cannot
        // abort heartbeat recovery, and each owner gets a bounded attempt.
        let reservations =
            match crate::placement::admission::sweep_expired_reservations(&self.db, &now).await {
                Ok(count) => count,
                Err(error) => {
                    tracing::warn!(%error, "reservation sweep remains pending");
                    0
                }
            };
        let placements = match self.check_workspace_placements().await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "placement sweep remains pending");
                0
            }
        };
        Ok(reservations + placements + timed_out + progress_warnings + stalled + expired)
    }

    #[tracing::instrument(skip(self))]
    async fn check_timeouts(&self) -> Result<()> {
        self.check_once().await.map(|_| ())
    }

    #[tracing::instrument(skip(self))]
    async fn list_busy_agents(&self) -> Result<Vec<Agent>> {
        let mut agents = Vec::new();
        let mut cursor = None;
        loop {
            let page = AgentRepo::list(
                &*self.db,
                AgentListQuery {
                    status: Some(AgentStatus::Busy),
                    executor_type: None,
                    capabilities: Vec::new(),
                    page: page_request(cursor),
                },
            )
            .await?;
            agents.extend(page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(agents)
    }

    async fn check_agent_timeouts_for_tick(&self, owner_suspension_succeeded: bool) -> u64 {
        if !owner_suspension_succeeded {
            tracing::warn!(
                "agent heartbeat timeout pass skipped because expired owner suspension failed"
            );
            return 0;
        }

        match self.check_agent_timeouts().await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "agent heartbeat timeout pass failed");
                0
            }
        }
    }

    async fn check_agent_timeouts(&self) -> Result<u64> {
        let agents = self.list_busy_agents().await?;
        let mut timed_out = 0;

        for agent in agents {
            if !agent_timed_out(&agent) {
                continue;
            }
            let mut suspended_owner = false;
            for execution in ExecutionRepo::list_running(&*self.db).await? {
                if execution.agent_id.as_deref() == Some(agent.id.as_str())
                    && execution_lease_is_suspended(&self.db, &execution).await?
                {
                    suspended_owner = true;
                    break;
                }
            }
            if suspended_owner {
                continue;
            }

            let last_heartbeat = agent
                .last_heartbeat_at
                .clone()
                .unwrap_or_else(|| "never".to_owned());

            AgentRepo::update(
                &*self.db,
                UpdateAgent {
                    id: agent.id.clone(),
                    expected_version: agent.version,
                    name: None,
                    description: None,
                    max_concurrent_tasks: None,
                    heartbeat_interval_seconds: None,
                    max_missed_heartbeats: None,
                    status: Some(AgentStatus::Error),
                    last_heartbeat_at: None,
                    model: None,
                    reasoning_effort: None,
                    permission_policy: None,
                    capabilities_json: None,
                    config_json: None,
                    daemon_id: None,
                    is_default: None,
                    paused: None,
                    prompt_template: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
            self.publish(ForgeEvent {
                event_type: "agent.timeout".to_owned(),
                entity_id: agent.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::AgentTimeout {
                    last_heartbeat: last_heartbeat.clone(),
                },
            });

            for task in list_in_progress_tasks(&self.db, Some(&agent.id)).await? {
                let outcome = recover_task(
                    &self.db,
                    task,
                    StopReason::AgentTimeout,
                    &api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor),
                )
                .await?;

                if outcome.annotated {
                    self.publish(ForgeEvent {
                        event_type: "task.recovered".to_owned(),
                        entity_id: outcome.task.id.clone(),
                        timestamp: event_timestamp(),
                        context: EventContext::TaskRecovered {
                            project_id: outcome.task.project_id,
                            reason: "agent_timeout".to_owned(),
                        },
                    });
                }
            }

            timed_out += 1;
        }

        Ok(timed_out)
    }

    async fn expire_workspace_leases_for_tick(&self, renewal_succeeded: bool) -> u64 {
        if !renewal_succeeded {
            tracing::warn!(
                "workspace lease expiry pass skipped because workspace lease renewal failed"
            );
            return 0;
        }

        match expire_workspace_leases(
            &self.db,
            &self.event_bus,
            self.task_executor.as_deref(),
            self.task_service.as_deref(),
            true,
        )
        .await
        {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "workspace lease expiry pass failed");
                0
            }
        }
    }

    async fn check_stalled_executions(&self) -> Result<u64> {
        // Semantic progress is deliberately not the owner-death signal. A
        // quiet provider/tool call remains live while its owner lease is
        // current; only an expired lease or hard deadline is eligible for
        // terminal CAS here. The configured stall timeout is retained as a
        // progress-warning threshold for the separate policy projector.
        let now = now_rfc3339();
        let executions = ExecutionRepo::list_expired_leases(&*self.db, &now, 500).await?;
        let mut expired = 0;

        for execution in executions {
            // A locally dispatched execution is created carrying an
            // already-expired `dispatch-pending:` marker, on purpose, so that
            // a crash between the row write and the runner's first claim
            // leaves something reclaimable. It is not orphaned yet: the runner
            // claims it milliseconds later. Terminalizing inside that window
            // kills a run that never started — observed as a reviewer
            // execution failed 8ms after creation with "lease expired".
            // Give the marker a grace period; a genuinely orphaned one is
            // still reclaimed once the grace elapses.
            if is_unclaimed_dispatch_marker(&execution, &now) {
                continue;
            }
            let deadline = execution
                .hard_deadline_at
                .as_deref()
                .filter(|deadline| *deadline <= now.as_str())
                .or(execution.lease_expires_at.as_deref())
                .unwrap_or(&now);
            let hard_deadline_reached = execution
                .hard_deadline_at
                .as_deref()
                .is_some_and(|value| value <= now.as_str());
            if !hard_deadline_reached
                && (suspend_expired_remote_execution(&self.db, &self.event_bus, &execution).await?
                    || execution_lease_is_suspended(&self.db, &execution).await?)
            {
                continue;
            }
            let error = if hard_deadline_reached {
                format!("Execution hard deadline reached at {deadline}")
            } else {
                format!("Execution owner lease expired at {deadline}")
            };
            let stop_reason = if hard_deadline_reached {
                StopReason::AgentTimeout
            } else {
                StopReason::ExecutionStalled
            };
            let stopped_by =
                api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor).display();
            let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
                TerminalizeExecution {
                    execution_id: execution.id.clone(),
                    expected_version: execution.execution_version,
                    lease_owner: execution.lease_owner.clone(),
                    status: ExecutionStatus::Failed,
                    stop_reason: Some(Some(stop_reason)),
                    stopped_by: Some(Some(stopped_by.clone())),
                    stopped_at: Some(Some(now.clone())),
                    resume_policy: Some(Some(ResumePolicy::Manual)),
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    last_progress_at: None,
                    summary: None,
                    before_sha: None,
                    after_sha: None,
                    logs_path: None,
                    error: Some(Some(error)),
                    executor_config_snapshot_json: None,
                    updated_at: now.clone(),
                    actor_type: "system".to_owned(),
                    actor_id: None,
                    correlation_id: None,
                    causation_id: None,
                    causation_depth: 0,
                    lease_disposition: ExecutionLeaseDisposition::Expire,
                },
                Vec::new(),
                None,
                None,
            );
            terminal_input.mark_unreplayable_pending_unsettled = execution
                .lease_owner
                .as_deref()
                .is_none_or(|owner| !owner.starts_with("daemon:"));
            terminal_input.preserve_pending_settlement = execution
                .lease_owner
                .as_deref()
                .is_some_and(|owner| owner.starts_with("daemon:"));
            let outcome = ExecutionRepo::terminalize_with_ledger(&*self.db, terminal_input).await?;

            let ExecutionTerminalOutcome::Committed {
                execution: updated, ..
            } = outcome
            else {
                // A runner, cancellation, or another monitor won the CAS.
                // Its terminal event, lease disposition, and Task cascade are
                // authoritative; this stale observation must be inert.
                continue;
            };

            discard_execution_plan_artifacts(&self.db, &updated).await;

            if let Some(task_executor) = self.task_executor.as_ref() {
                // Agents without a daemon binding run in-process, so only a
                // definitively remote-owned execution skips the embedded
                // cancellation. External cancellation is a side effect of
                // the winning terminal transition and therefore happens only
                // after the CAS commits.
                if !execution_is_remote_owned(&self.db, &updated)
                    .await
                    .unwrap_or(false)
                {
                    if let Err(error) = task_executor.cancel(&updated.id).await {
                        tracing::warn!(
                            execution_id = %updated.id,
                            %error,
                            "failed to cancel expired execution owner"
                        );
                    }
                }
            }

            self.publish(ForgeEvent {
                event_type: if hard_deadline_reached {
                    "execution.hard_deadline_exceeded".to_owned()
                } else {
                    "execution.stalled".to_owned()
                },
                entity_id: updated.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ExecutionStalled {
                    task_id: updated.task_id.clone(),
                    execution_id: updated.id.clone(),
                    stale_before: deadline.to_owned(),
                },
            });
            self.publish(ForgeEvent {
                event_type: "reconciliation.event".to_owned(),
                entity_id: updated.task_id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ReconciliationEvent {
                    task_id: Some(updated.task_id.clone()),
                    execution_id: Some(updated.id.clone()),
                    reason: if hard_deadline_reached {
                        "hard_deadline_reached".to_owned()
                    } else {
                        "execution_lease_expired".to_owned()
                    },
                },
            });

            if let Some(task_service) = self.task_service.as_ref() {
                if let Err(error) = cascade_recovered_execution(
                    task_service,
                    &updated,
                    stalled_execution_should_block_task(&updated),
                )
                .await
                {
                    tracing::warn!(
                        execution_id = %updated.id,
                        task_id = %updated.task_id,
                        %error,
                        "failed to cascade expired execution"
                    );
                }
            }
            expired += 1;
        }

        if expired > 0 {
            tracing::info!(
                expired_executions = expired,
                "heartbeat monitor detected expired execution leases"
            );
        }
        Ok(expired)
    }

    async fn check_stale_progress(&self) -> Result<u64> {
        let now = now_rfc3339();
        let stale_before = Utc::now()
            - ChronoDuration::from_std(self.execution_stall_timeout).unwrap_or_else(|_| {
                ChronoDuration::seconds(Self::DEFAULT_EXECUTION_STALL_TIMEOUT.as_secs() as i64)
            });
        let stale_before = stale_before.to_rfc3339();
        let candidates =
            ExecutionRepo::list_stale_progress(&*self.db, &now, &stale_before, 500).await?;
        let mut warned = 0;

        for candidate in candidates {
            // The list query is advisory. Revalidate owner/version/status,
            // lease and semantic progress while appending the warning in one
            // repository transaction. A fresh progress update or terminal CAS
            // that wins first therefore returns `Concurrent` and cannot leave
            // a warning behind after the authoritative state has changed.
            let Some(owner) = candidate.lease_owner.clone() else {
                continue;
            };
            let outcome = ExecutionRepo::record_progress_warning(
                &*self.db,
                RecordExecutionProgressWarning {
                    execution_id: candidate.id.clone(),
                    expected_version: candidate.execution_version,
                    owner,
                    expected_last_progress_at: candidate.last_progress_at.clone(),
                    stale_before: stale_before.clone(),
                    now: now.clone(),
                },
            )
            .await?;
            let ExecutionProgressWarningOutcome::Committed { .. } = outcome else {
                continue;
            };

            warned += 1;
        }

        Ok(warned)
    }

    fn publish(&self, event: ForgeEvent) {
        self.event_bus.publish(event);
    }
}

const WORKSPACE_LEASE_RENEW_WINDOW_SECONDS: i64 = 5 * 60;
const WORKSPACE_LEASE_EXTENSION_SECONDS: i64 = 15 * 60;

async fn renew_workspace_leases(db: &SqliteDb) -> Result<u64> {
    let now = chrono::Utc::now();
    let renewed = WorkspaceLeaseRepo::renew_active(
        db,
        &now.to_rfc3339(),
        &(now + chrono::Duration::seconds(WORKSPACE_LEASE_RENEW_WINDOW_SECONDS)).to_rfc3339(),
        &(now + chrono::Duration::seconds(WORKSPACE_LEASE_EXTENSION_SECONDS)).to_rfc3339(),
        500,
    )
    .await?;
    if !renewed.is_empty() {
        tracing::debug!(
            renewed_leases = renewed.len(),
            "renewed active WorkspaceLeases"
        );
    }
    Ok(renewed.len() as u64)
}

/// Expire scheduler grants and, during the live heartbeat pass, stop any
/// execution that lost its authority.  Startup recovery deliberately leaves
/// the execution running long enough for `recover_task` to apply its normal
/// requeue/block policy; the heartbeat path terminalizes it immediately.
async fn expire_workspace_leases(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_executor: Option<&dyn TaskExecutor>,
    task_service: Option<&TaskService>,
    terminalize_running: bool,
) -> Result<u64> {
    let expired = if terminalize_running {
        // The live monitor must not mutate a WorkspaceLease before it has
        // won the execution terminal CAS.  There is no bulk read API for
        // leases yet, so inspect the bounded running set and select only
        // active grants whose execution is still running.  `terminalize`
        // then closes the matching grant in its own transaction.  Startup
        // recovery may continue using the standalone expiry mutation because
        // it deliberately leaves running attempts for crash recovery.
        let now = now_rfc3339();
        let running = ExecutionRepo::list_running(db).await?;
        let mut candidates = Vec::new();
        for execution in running {
            if execution_lease_is_suspended(db, &execution).await? {
                continue;
            }
            let Some(lease) =
                WorkspaceLeaseRepo::get_active_for_task(db, &execution.task_id).await?
            else {
                continue;
            };
            if lease.execution_id == execution.id && lease.expires_at <= now {
                candidates.push(lease);
            }
        }
        candidates
    } else {
        WorkspacePlacementRepo::expire_unsuspended_workspace_leases(db, &now_rfc3339(), 500).await?
    };
    let mut expired_count = if terminalize_running {
        0
    } else {
        expired.len() as u64
    };
    if !terminalize_running {
        return Ok(expired_count);
    }

    for lease in expired {
        let Some(execution) = ExecutionRepo::get_by_id(db, &lease.execution_id).await? else {
            continue;
        };
        if execution.status != ExecutionStatus::Running {
            continue;
        }

        let now = now_rfc3339();
        let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Failed,
                stop_reason: Some(Some(StopReason::ExecutionStalled)),
                stopped_by: Some(Some(
                    api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor)
                        .display(),
                )),
                stopped_at: Some(Some(now.clone())),
                resume_policy: Some(Some(ResumePolicy::Manual)),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: Some(Some("scheduler WorkspaceLease expired".to_owned())),
                executor_config_snapshot_json: None,
                updated_at: now,
                actor_type: "system".to_owned(),
                actor_id: None,
                correlation_id: None,
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Expire,
            },
            Vec::new(),
            None,
            None,
        );
        terminal_input.mark_unreplayable_pending_unsettled = execution
            .lease_owner
            .as_deref()
            .is_none_or(|owner| !owner.starts_with("daemon:"));
        terminal_input.preserve_pending_settlement = execution
            .lease_owner
            .as_deref()
            .is_some_and(|owner| owner.starts_with("daemon:"));
        let outcome = match ExecutionRepo::terminalize_with_ledger(db, terminal_input).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    execution_id = %execution.id,
                    %error,
                    "failed to stop execution after WorkspaceLease expiry"
                );
                continue;
            }
        };

        let ExecutionTerminalOutcome::Committed {
            execution: updated, ..
        } = outcome
        else {
            // A concurrent completion/cancellation/monitor winner owns the
            // terminal event and lease disposition. Do not duplicate any
            // downstream recovery side effect for this expired grant.
            continue;
        };

        discard_execution_plan_artifacts(db, &updated).await;

        expired_count += 1;

        if let Some(task_executor) = task_executor {
            if !execution_is_remote_owned(db, &updated)
                .await
                .unwrap_or(false)
            {
                if let Err(error) = task_executor.cancel(&updated.id).await {
                    tracing::warn!(
                        execution_id = %updated.id,
                        %error,
                        "failed to cancel execution after WorkspaceLease expiry"
                    );
                }
            }
        }

        event_bus.publish(ForgeEvent {
            event_type: "execution.stalled".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionStalled {
                task_id: updated.task_id.clone(),
                execution_id: updated.id.clone(),
                stale_before: lease.expires_at.clone(),
            },
        });
        event_bus.publish(ForgeEvent {
            event_type: "reconciliation.event".to_owned(),
            entity_id: updated.task_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ReconciliationEvent {
                task_id: Some(updated.task_id.clone()),
                execution_id: Some(updated.id.clone()),
                reason: "workspace_lease_expired".to_owned(),
            },
        });
        if let Some(task_service) = task_service {
            if let Err(error) = cascade_recovered_execution(task_service, &updated, true).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to cascade expired WorkspaceLease execution"
                );
            }
        }
    }

    Ok(expired_count)
}

pub(crate) struct CancelledExecution {
    pub execution_id: String,
    pub agent_session_id: Option<String>,
}

pub(crate) struct RecoverTaskOutcome {
    pub task: Task,
    pub annotated: bool,
    /// Running executions this recovery settled.
    pub settled_executions: usize,
}

async fn list_in_progress_tasks(db: &SqliteDb, agent_id: Option<&str>) -> Result<Vec<Task>> {
    let mut tasks = Vec::new();
    for project in list_projects(db).await? {
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let statuses: Vec<String> = workflow
            .states
            .iter()
            .filter(|state| state.kind == api_types::StateKind::Active)
            .map(|state| state.name.clone())
            .collect();
        if statuses.is_empty() {
            continue;
        }
        let mut cursor = None;
        loop {
            let page = TaskRepo::list(
                db,
                TaskListQuery {
                    project_id: project.id.clone(),
                    q: None,
                    statuses: statuses.clone(),
                    agent_ids: agent_id.map(str::to_owned).into_iter().collect(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: page_request(cursor),
                },
            )
            .await?;
            tasks.extend(
                page.items
                    .into_iter()
                    .filter(|task| task.assignee_type.as_deref() != Some("user")),
            );
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    Ok(tasks)
}

pub(crate) async fn recover_task(
    db: &SqliteDb,
    task: Task,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
) -> Result<RecoverTaskOutcome> {
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, stopped_by);
    if workflow
        .states
        .iter()
        .all(|state| state.name != task.status)
    {
        return Err(ServiceError::invalid_operation(format!(
            "workflow has no state named {}",
            task.status
        )));
    }
    let should_auto_resume = stop_reason == StopReason::CrashRecovery;
    let cancelled = cancel_running_executions_for_recovery(
        db,
        &task.id,
        stop_reason.clone(),
        stopped_by,
        if should_auto_resume {
            ResumePolicy::Auto
        } else {
            ResumePolicy::Manual
        },
        stop_reason == StopReason::AgentTimeout,
    )
    .await?;

    // Each committed terminal CAS disposes only the WorkspaceLease bound to
    // that execution. Never revoke by Task here: a zero-row CAS is a
    // concurrent winner, and a newer execution may already hold a healthy
    // successor lease for the same Task.

    if cancelled.is_empty() {
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: 0,
        });
    }

    if should_auto_resume {
        tracing::warn!(
            task_id = %task.id,
            execution_count = cancelled.len(),
            "recovered task left in current state for automatic redispatch"
        );
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: cancelled.len(),
        });
    }

    let has_resumable_execution = cancelled
        .iter()
        .any(|execution| execution.agent_session_id.is_some());
    if has_resumable_execution {
        tracing::warn!(
            task_id = %task.id,
            execution_count = cancelled.len(),
            "recovered task left in current state because a resumable execution was cancelled"
        );
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
            settled_executions: cancelled.len(),
        });
    }

    let (blocking_reason, message) = if stop_reason == StopReason::AgentTimeout {
        ("agent_timeout", "Recovered after agent heartbeat timeout")
    } else {
        ("crash_recovery", "Recovered after server restart")
    };
    let blocked_execution_id = cancelled
        .first()
        .map(|execution| execution.execution_id.clone());
    let artifact = blocked_execution_id.as_ref().map(|execution_id| {
        json!({
            "kind": "execution",
            "id": execution_id,
            "log_path": null,
        })
    });
    let annotation = json!({
        "type": api_types::FailureKind::RecoveryRequired,
        "blocking_reason": blocking_reason,
        "blocked_by": stopped_by.display(),
        "blocked_at": now_rfc3339(),
        "blocked_execution_id": blocked_execution_id,
        "artifact": artifact,
        "message": message,
    })
    .to_string();
    // Applied under the recovery step's lease, or synchronously as the
    // Task's own step (bounded) from the heartbeat monitor. The outcome
    // reports a committed annotation only, never a queued one.
    let task = match TaskRepo::update_status(
        db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status.clone(),
            assignee_id: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    {
        Ok(task) => task,
        Err(db::DbError::TaskBusy { pending_steps, .. }) => {
            // Outside the lease only: the accepted annotation stays queued
            // behind running work and publishes task.updated when it lands.
            tracing::info!(task_id = %task.id, pending_steps, "recovery annotation queued behind running Task work");
            return Ok(RecoverTaskOutcome {
                task,
                annotated: false,
                settled_executions: cancelled.len(),
            });
        }
        Err(error) => return Err(error.into()),
    };

    Ok(RecoverTaskOutcome {
        task,
        annotated: true,
        settled_executions: cancelled.len(),
    })
}

async fn sweep_stale_recovery_annotations(db: &Arc<SqliteDb>) -> Result<u64> {
    let mut cleared = 0_u64;

    for project in list_projects(db).await? {
        let mut cursor = None;
        loop {
            let page = TaskRepo::list(
                db.as_ref(),
                TaskListQuery {
                    project_id: project.id.clone(),
                    q: None,
                    statuses: vec![],
                    agent_ids: Vec::new(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: page_request(cursor),
                },
            )
            .await?;

            for task in page.items {
                let Some(annotation_json) = task.error_annotation.as_deref() else {
                    continue;
                };
                let Ok(annotation) = serde_json::from_str::<serde_json::Value>(annotation_json)
                else {
                    continue;
                };
                if annotation.get("type").and_then(serde_json::Value::as_str)
                    != Some("recovery_required")
                {
                    continue;
                }

                let blocked_execution_id =
                    annotation.get("blocked_execution_id").and_then(|value| {
                        if value.is_null() {
                            None
                        } else {
                            value.as_str()
                        }
                    });

                let should_clear = match blocked_execution_id {
                    None => true,
                    Some(execution_id) => {
                        match ExecutionRepo::get_by_id(db.as_ref(), execution_id).await? {
                            None => true,
                            Some(execution) => !execution_awaits_recovery(&execution),
                        }
                    }
                };

                if !should_clear {
                    continue;
                }

                db.enqueue_task_mutation(
                    &task.id,
                    db::TaskMutation::TaskUpdateStatus {
                        input: UpdateTaskStatus {
                            id: task.id.clone(),
                            expected_version: task.version,
                            status: task.status.clone(),
                            assignee_id: None,
                            error_annotation: Some(None),
                            blocked_json: None,
                            failed_json: None,
                            updated_at: now_rfc3339(),
                        },
                    },
                )
                .await?;

                cleared += 1;
            }

            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }

    Ok(cleared)
}

fn execution_awaits_recovery(execution: &db::Execution) -> bool {
    execution.status == ExecutionStatus::Cancelled
        && matches!(execution.resume_policy, Some(ResumePolicy::Manual))
}

pub(crate) async fn cancel_running_executions(
    db: &SqliteDb,
    task_id: &str,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
    resume_policy: ResumePolicy,
) -> Result<Vec<CancelledExecution>> {
    cancel_running_executions_for_recovery(
        db,
        task_id,
        stop_reason,
        stopped_by,
        resume_policy,
        false,
    )
    .await
}

async fn cancel_running_executions_for_recovery(
    db: &SqliteDb,
    task_id: &str,
    stop_reason: StopReason,
    stopped_by: &api_types::Actor,
    resume_policy: ResumePolicy,
    preserve_healthy_owner: bool,
) -> Result<Vec<CancelledExecution>> {
    let executions = ExecutionRepo::list_running_by_task(db, task_id).await?;
    let mut cancelled = Vec::new();
    let now = now_rfc3339();
    for execution in executions {
        if matches!(
            stop_reason,
            StopReason::CrashRecovery | StopReason::AgentTimeout
        ) && execution_lease_is_suspended(db, &execution).await?
        {
            continue;
        }
        if preserve_healthy_owner && execution_owner_lease_is_healthy(&execution, &now) {
            // Legacy AgentStatus heartbeat timeout is not authoritative for
            // an attempt that still has a live execution owner lease. The
            // expiry/deadline monitor remains responsible for this execution.
            continue;
        }
        let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Cancelled,
                stop_reason: Some(Some(stop_reason.clone())),
                stopped_by: Some(Some(stopped_by.display())),
                stopped_at: Some(Some(now_rfc3339())),
                resume_policy: Some(Some(resume_policy.clone())),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: Some(Some("Recovered".to_owned())),
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
                actor_type: if stopped_by.is_system() {
                    "system".to_owned()
                } else if stopped_by.is_agent() {
                    "agent".to_owned()
                } else {
                    "user".to_owned()
                },
                actor_id: None,
                correlation_id: None,
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Revoke,
            },
            Vec::new(),
            None,
            None,
        );
        // Local provider calls cannot survive a process crash with a
        // replayable result.  Ask the composite boundary to move their
        // pending invocations to `unsettled` before committing the terminal
        // CAS. Daemon-owned calls retain their pending state because the
        // daemon keeps its terminal report until it is acknowledged.
        terminal_input.mark_unreplayable_pending_unsettled = execution
            .lease_owner
            .as_deref()
            .is_none_or(|owner| !owner.starts_with("daemon:"));
        let outcome = ExecutionRepo::terminalize_with_ledger(db, terminal_input).await?;
        if let ExecutionTerminalOutcome::Committed {
            execution: cancelled_execution,
            ..
        } = outcome
        {
            discard_execution_plan_artifacts(db, &cancelled_execution).await;
            cancelled.push(CancelledExecution {
                execution_id: cancelled_execution.id,
                agent_session_id: execution.agent_session_id.clone(),
            });
        }
    }
    Ok(cancelled)
}

fn execution_owner_lease_is_healthy(execution: &Execution, now: &str) -> bool {
    if execution.status != ExecutionStatus::Running || execution.lease_owner.is_none() {
        return false;
    }
    let Some(lease_expires_at) = execution.lease_expires_at.as_deref() else {
        return false;
    };
    if !rfc3339_is_after(lease_expires_at, now) {
        return false;
    }
    execution
        .hard_deadline_at
        .as_deref()
        .is_none_or(|hard_deadline_at| rfc3339_is_after(hard_deadline_at, now))
}

fn rfc3339_is_after(value: &str, other: &str) -> bool {
    let Some(value) = DateTime::parse_from_rfc3339(value).ok() else {
        return false;
    };
    let Some(other) = DateTime::parse_from_rfc3339(other).ok() else {
        return false;
    };
    value > other
}

async fn list_projects(db: &SqliteDb) -> Result<Vec<Project>> {
    let mut projects = Vec::new();
    let mut cursor = None;
    loop {
        let page = ProjectRepo::list(db, page_request(cursor)).await?;
        projects.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(projects)
}

fn page_request(cursor: Option<String>) -> PageRequest {
    PageRequest {
        cursor,
        limit: 500,
        include_total: false,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

fn stalled_execution_should_block_task(execution: &db::Execution) -> bool {
    matches!(
        execution.role.as_str(),
        "interactive" | "executor" | crate::workflow::default_roles::CODER
    )
}

/// Reviewers have their own settlement/retry state machine.  Any monitor that
/// wins a terminal execution CAS must enter that cascade; sending a reviewer
/// through the generic worker blocker leaves its `review` row running forever.
async fn cascade_recovered_execution(
    task_service: &TaskService,
    execution: &Execution,
    block_non_reviewer: bool,
) -> Result<()> {
    if execution.role == crate::workflow::default_roles::REVIEWER {
        task_service
            .maybe_cascade_executor_completion(&execution.id)
            .await
    } else if block_non_reviewer {
        task_service
            .annotate_executor_failure_block(execution)
            .await
    } else {
        Ok(())
    }
}

pub(crate) async fn resolve_execution_daemon(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<Option<(String, Daemon)>> {
    let daemon_id = if let Some(workspace_id) = execution.workspace_id.as_deref() {
        let placement = match db::WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
        {
            Some(placement) => placement,
            None => {
                let workspace = db::WorkspaceRepo::get_by_id(db, workspace_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
                EmbeddedWorkspaceBackend::ensure_recorded_server_placement(db, &workspace).await?
            }
        };
        match placement.owner_kind {
            PlacementOwnerKind::Daemon => placement.daemon_id,
            PlacementOwnerKind::Server => placement.execution_daemon_id,
        }
    } else {
        // No workspace means no placement can exist. Preserve task-0 routing
        // for these executions; placed work never falls back to an Agent pin.
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .map(serde_json::from_str::<serde_json::Value>)
            .transpose()
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid executor config snapshot: {error}"
                ))
            })?;
        match snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("daemon_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            Some(id) => Some(id.to_owned()),
            None => match execution.agent_id.as_deref() {
                Some(id) => AgentRepo::get_by_id(db, id)
                    .await?
                    .and_then(|agent| agent.daemon_id),
                None => None,
            },
        }
    };
    let Some(daemon_id) = daemon_id else {
        return Ok(None);
    };
    let Some(daemon) = DaemonRepo::get_by_id(db, &daemon_id).await? else {
        return Ok(None);
    };
    Ok(Some((daemon_id, daemon)))
}

pub(crate) async fn execution_is_remote_owned(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some((_, daemon)) = resolve_execution_daemon(db, execution).await? else {
        return Ok(false);
    };
    Ok(!is_embedded_daemon_machine(&daemon.machine_id))
}

pub(crate) struct FailDaemonDisconnectedExecution<'a> {
    pub execution: &'a Execution,
    pub daemon_id: &'a str,
    pub error_message: String,
    pub stopped_by: &'a str,
    pub reconciliation_reason: &'a str,
}

pub(crate) async fn fail_execution_daemon_disconnected(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_service: Option<&TaskService>,
    input: FailDaemonDisconnectedExecution<'_>,
) -> Result<Option<Execution>> {
    let FailDaemonDisconnectedExecution {
        execution,
        daemon_id,
        error_message,
        stopped_by,
        reconciliation_reason,
    } = input;
    let now = now_rfc3339();
    let mut terminal_input = crate::task_service::execution::ledger::terminal_with_ledger(
        TerminalizeExecution {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            lease_owner: execution.lease_owner.clone(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(StopReason::DaemonDisconnected)),
            stopped_by: Some(Some(stopped_by.to_owned())),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some(error_message)),
            executor_config_snapshot_json: None,
            updated_at: now,
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
        Vec::new(),
        None,
        None,
    );
    terminal_input.preserve_pending_settlement = true;
    let outcome = ExecutionRepo::terminalize_with_ledger(db, terminal_input).await?;

    let ExecutionTerminalOutcome::Committed {
        execution: updated, ..
    } = outcome
    else {
        // Another terminal caller won the version/owner CAS. Its durable
        // event, lease disposition, and Task cascade are authoritative; this
        // stale daemon observation must not emit a second failure.
        return Ok(None);
    };

    discard_execution_plan_artifacts(db, &updated).await;

    event_bus.publish(ForgeEvent {
        event_type: "execution.daemon_disconnected".to_owned(),
        entity_id: updated.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ExecutionDaemonDisconnected {
            task_id: updated.task_id.clone(),
            execution_id: updated.id.clone(),
            daemon_id: daemon_id.to_owned(),
        },
    });
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: updated.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(updated.task_id.clone()),
            execution_id: Some(updated.id.clone()),
            reason: reconciliation_reason.to_owned(),
        },
    });

    if let Some(task_service) = task_service {
        if let Err(error) = cascade_recovered_execution(
            task_service,
            &updated,
            stalled_execution_should_block_task(&updated),
        )
        .await
        {
            tracing::warn!(
                execution_id = %updated.id,
                task_id = %updated.task_id,
                %error,
                "failed to cascade daemon-disconnected execution"
            );
        }
    }

    Ok(Some(updated))
}

fn placement_update(
    placement: &WorkspacePlacement,
    state: PlacementState,
    failure_cause: Option<PlacementFailureCause>,
) -> UpdateWorkspacePlacement {
    UpdateWorkspacePlacement {
        id: placement.id.clone(),
        expected_version: placement.version,
        agent_id: None,
        owner_kind: None,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: None,
        execution_daemon_id: None,
        workspace_handle: None,
        generation: None,
        state: Some(state),
        selected_by: None,
        selection_reason: None,
        reserved_until: None,
        disconnected_at: None,
        failure_cause: Some(failure_cause),
        updated_at: now_rfc3339(),
    }
}

pub(crate) fn placement_execution_daemon_id(placement: &WorkspacePlacement) -> Option<&str> {
    match placement.owner_kind {
        PlacementOwnerKind::Daemon => placement.daemon_id.as_deref(),
        PlacementOwnerKind::Server => placement.execution_daemon_id.as_deref(),
    }
}

pub(crate) async fn suspend_expired_remote_execution(
    db: &SqliteDb,
    event_bus: &EventBus,
    execution: &Execution,
) -> Result<bool> {
    let now = now_rfc3339();
    if execution.status != ExecutionStatus::Running
        || execution
            .hard_deadline_at
            .as_deref()
            .is_some_and(|deadline| !rfc3339_is_after(deadline, &now))
    {
        return Ok(false);
    }
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    let Some(placement) = WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id).await?
    else {
        return Ok(false);
    };
    if placement.owner_kind == PlacementOwnerKind::Server {
        let Some(daemon_id) = placement.execution_daemon_id.as_deref() else {
            return Ok(false);
        };
        if DaemonRepo::get_by_id(db, daemon_id)
            .await?
            .is_none_or(|daemon| is_embedded_daemon_machine(&daemon.machine_id))
        {
            return Ok(false);
        }
    }
    if let Some(disconnected) =
        WorkspacePlacementRepo::suspend_expired_execution_lease(db, &placement, execution, &now)
            .await?
    {
        publish_disconnected_placement(db, event_bus, disconnected).await?;
    }
    // A renewal, socket disconnect, or terminal result may win the CAS. None
    // makes heartbeat expiry a work failure for this remote-owned attempt.
    Ok(true)
}

pub(crate) async fn execution_lease_is_suspended(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    Ok(
        WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
            .is_some_and(|placement| {
                placement.state == PlacementState::Disconnected
                    && execution
                        .hard_deadline_at
                        .as_deref()
                        .is_none_or(|deadline| rfc3339_is_after(deadline, &now_rfc3339()))
            }),
    )
}

pub(crate) async fn execution_placement_is_disconnected(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<bool> {
    let Some(workspace_id) = execution.workspace_id.as_deref() else {
        return Ok(false);
    };
    Ok(
        WorkspacePlacementRepo::get_by_workspace_id(db, workspace_id)
            .await?
            .is_some_and(|placement| placement.state == PlacementState::Disconnected),
    )
}

pub(crate) async fn disconnect_daemon_placements(
    db: &SqliteDb,
    event_bus: &EventBus,
    daemon_id: &str,
) -> Result<u64> {
    if DaemonRepo::get_by_id(db, daemon_id)
        .await?
        .is_some_and(|daemon| is_embedded_daemon_machine(&daemon.machine_id))
    {
        return Ok(0);
    }
    let placements = WorkspacePlacementRepo::list_by_state(db, PlacementState::Ready).await?;
    let mut disconnected = 0;
    for placement in placements {
        if placement_execution_daemon_id(&placement) != Some(daemon_id) {
            continue;
        }
        let mut update = placement_update(
            &placement,
            PlacementState::Disconnected,
            Some(PlacementFailureCause::OwnerDisconnected),
        );
        update.disconnected_at = Some(Some(now_rfc3339()));
        match WorkspacePlacementRepo::update(db, update).await {
            Ok(placement) => {
                publish_disconnected_placement(db, event_bus, placement).await?;
                disconnected += 1;
            }
            Err(db::DbError::VersionConflict) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(disconnected)
}

async fn publish_disconnected_placement(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: WorkspacePlacement,
) -> Result<()> {
    record_disconnected_attention(db, &placement).await?;
    event_bus.publish(ForgeEvent {
        event_type: "workspace.disconnected".to_owned(),
        entity_id: placement.workspace_id,
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(placement.task_id),
            execution_id: None,
            reason: "owner_disconnected".to_owned(),
        },
    });
    Ok(())
}

async fn record_disconnected_attention(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
) -> Result<()> {
    let daemon_id = placement_execution_daemon_id(placement);
    let incident_current: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM attention_projection a JOIN domain_event e ON e.id = a.source_event_id
         WHERE a.dedupe_key = ? AND e.dedupe_key = ? AND a.status != 'resolved')")
        .bind(format!("workspace-owner:{}", placement.id))
        .bind(format!("workspace-disconnect:{}:{}", placement.id, placement.disconnected_at.as_deref().unwrap_or(&placement.updated_at)))
        .fetch_one(db.pool()).await?;
    if incident_current {
        return Ok(());
    }
    let Some(task) = TaskRepo::get_by_id(db, &placement.task_id, false).await? else {
        return Ok(());
    };
    let event = db::DomainEventRepo::append_event(
        db,
        db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "workspace.owner_disconnected".to_owned(),
            entity_type: "workspace_placement".to_owned(),
            entity_id: placement.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: task.project_id.clone(),
            correlation_id: placement.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "workspace-disconnect:{}:{}",
                placement.id,
                placement
                    .disconnected_at
                    .as_deref()
                    .unwrap_or(&placement.updated_at)
            )),
            payload_json: json!({"task_id": task.id, "placement_id": placement.id,
            "daemon_id": daemon_id, "failure_cause": "owner_disconnected"})
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await?;
    db::AttentionRepo::insert_attention(
        db,
        db::CreateAttentionProjection {
            id: db::new_uuid_v4(),
            attention_type: "runtime_offline".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: task.project_id,
            identity_id: placement.agent_id.clone(),
            source_event_id: event.id,
            priority: 70,
            status: "open".to_owned(),
            summary: "Workspace owner disconnected; waiting on the same machine".to_owned(),
            details_json: json!({"task": {"id": task.id, "title": task.title},
            "placement_id": placement.id, "daemon_id": daemon_id,
            "disconnected_at": placement.disconnected_at, "failure_cause": "owner_disconnected",
            "recovery": {"actions": ["reexecute", "cancel_task"], "automatic_retry": false}})
            .to_string(),
            dedupe_key: format!("workspace-owner:{}", placement.id),
            occurred_at: event.created_at,
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "reconnect_owner".to_owned(),
            source_sequence: Some(event.sequence),
        },
    )
    .await?;
    Ok(())
}

// An attempt owns its daemon and each placement until completion or cancellation.
struct PlacementAttemptGuard {
    in_flight: Arc<Mutex<HashSet<String>>>,
    keys: Vec<String>,
}
impl Drop for PlacementAttemptGuard {
    fn drop(&mut self) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in &self.keys {
            in_flight.remove(key);
        }
    }
}

impl HeartbeatMonitor {
    async fn suspend_unreachable_placements(&self) -> Result<()> {
        let Some(registry) = &self.daemon_connections else {
            return Ok(());
        };
        let running = ExecutionRepo::list_running(&*self.db).await?;
        let mut owners = HashSet::new();
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Ready).await?
        {
            let Some(daemon_id) = placement_execution_daemon_id(&placement) else {
                continue;
            };
            let Some(daemon) = DaemonRepo::get_by_id(&*self.db, daemon_id).await? else {
                continue;
            };
            if placement.owner_kind == PlacementOwnerKind::Server
                && is_embedded_daemon_machine(&daemon.machine_id)
            {
                continue;
            }
            let owner_changed = registry.get(daemon_id).is_some_and(|connection| {
                let owner =
                    crate::daemon_transport::execution_lease_owner(daemon_id, connection.id());
                running.iter().any(|execution| {
                    execution.workspace_id.as_deref() == Some(&placement.workspace_id)
                        && execution.lease_owner.as_deref() != Some(owner.as_str())
                })
            });
            if daemon.status != db::DaemonStatus::Online
                || !registry.get(daemon_id).is_some_and(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                })
                || owner_changed
            {
                owners.insert(daemon_id.to_owned());
            }
        }
        for daemon_id in owners {
            if let Err(error) =
                disconnect_daemon_placements(&self.db, &self.event_bus, &daemon_id).await
            {
                tracing::warn!(%daemon_id, %error, "owner suspension remains pending");
            }
        }
        Ok(())
    }

    async fn check_workspace_placements(&self) -> Result<u64> {
        let mut settled = 0;
        // Expiry/repair is local and bounded by rows, never by an owner RPC.
        for state in [PlacementState::Disconnected, PlacementState::Failed] {
            for placement in WorkspacePlacementRepo::list_by_state(&*self.db, state).await? {
                let attempt = async {
                    if placement.state == PlacementState::Disconnected {
                        record_disconnected_attention(&self.db, &placement).await?;
                        repair_owner_recovery(&self.db, &placement).await?;
                        let expired = placement
                            .disconnected_at
                            .as_deref()
                            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
                            .and_then(|at| Utc::now().signed_duration_since(at).to_std().ok())
                            .is_some_and(|elapsed| elapsed >= self.max_disconnect);
                        if !expired {
                            return Ok(0);
                        }
                        let failed = WorkspacePlacementRepo::update(
                            &*self.db,
                            placement_update(
                                &placement,
                                PlacementState::Failed,
                                Some(PlacementFailureCause::OwnerDisconnectedTimeout),
                            ),
                        )
                        .await?;
                        return fail_placement_executions(
                            &self.db,
                            &self.event_bus,
                            &failed,
                            PlacementFailureCause::OwnerDisconnectedTimeout,
                        )
                        .await;
                    }
                    if placement.failure_cause
                        == Some(PlacementFailureCause::OwnerDisconnectedTimeout)
                    {
                        let count = fail_placement_executions(
                            &self.db,
                            &self.event_bus,
                            &placement,
                            PlacementFailureCause::OwnerDisconnectedTimeout,
                        )
                        .await?;
                        repair_owner_recovery(&self.db, &placement).await?;
                        return Ok(count);
                    }
                    Ok::<u64, ServiceError>(0)
                }
                .await;
                match attempt {
                    Ok(count) => settled += count,
                    Err(ServiceError::Db(db::DbError::VersionConflict)) => {}
                    Err(error) => tracing::warn!(placement_id = %placement.id,
                        daemon_id = ?placement_execution_daemon_id(&placement), %error, "placement maintenance remains pending"),
                }
            }
        }
        let Some(registry) = &self.daemon_connections else {
            return Ok(settled);
        };
        let upgraded_daemons: Vec<_> = registry
            .connection_snapshots()
            .into_iter()
            .filter(|(id, _)| {
                registry.get(id).is_some_and(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                })
            })
            .map(|(id, _)| id)
            .collect();
        if !upgraded_daemons.is_empty() {
            if let Err(error) =
                crate::workflow::engine::wake_upgraded_daemon_tasks(&self.db, &upgraded_daemons)
                    .await
            {
                tracing::warn!(%error, "daemon upgrade dispatch wake failed");
            }
        }
        let retained = serde_json::to_string(&registry.retained_terminal_execution_ids())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let recent = (Utc::now() - ChronoDuration::seconds(60)).to_rfc3339();
        let ready_to_retry: HashSet<String> = sqlx::query_scalar(
            "SELECT p.id FROM workspace_placement p WHERE p.state = 'ready' AND (
                EXISTS (SELECT 1 FROM execution e JOIN json_each(?) report ON e.id = report.value WHERE e.workspace_id = p.workspace_id)
                OR EXISTS (SELECT 1 FROM domain_event ev WHERE ev.entity_type = 'workspace_placement' AND ev.entity_id = p.id
                    AND ev.event_type = 'workspace.owner_reconciled' AND ev.created_at >= ?))")
            .bind(retained).bind(recent).fetch_all(self.db.pool()).await?.into_iter().collect();
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        for state in [PlacementState::Disconnected, PlacementState::Ready] {
            for placement in WorkspacePlacementRepo::list_by_state(&*self.db, state).await? {
                if placement.state == PlacementState::Ready
                    && !ready_to_retry.contains(&placement.id)
                {
                    continue;
                }
                if let Some(daemon_id) = placement_execution_daemon_id(&placement) {
                    groups
                        .entry(daemon_id.to_owned())
                        .or_default()
                        .push(placement.id);
                }
            }
        }
        for (daemon_id, facts) in registry.connection_snapshots() {
            if !facts.workspace_incapable {
                groups.entry(daemon_id).or_default();
            }
        }
        for (daemon_id, ids) in groups {
            if !registry.get(&daemon_id).is_some_and(|connection| {
                !connection.is_stale() && connection.protocol_allows_dispatch()
            }) {
                continue;
            }
            let mut keys = vec![format!("daemon:{daemon_id}")];
            keys.extend(ids.iter().map(|id| format!("placement:{id}")));
            {
                let mut in_flight = self
                    .placement_in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if keys.iter().any(|key| in_flight.contains(key)) {
                    continue;
                }
                in_flight.extend(keys.iter().cloned());
            }
            let guard = PlacementAttemptGuard {
                in_flight: Arc::clone(&self.placement_in_flight),
                keys,
            };
            let workers = Arc::clone(&self.placement_workers);
            let db = Arc::clone(&self.db);
            let event_bus = Arc::clone(&self.event_bus);
            let registry = Arc::clone(registry);
            let task_service = self.task_service.clone();
            let mut handles = self
                .placement_worker_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            handles.retain(|handle| !handle.is_finished());
            if self.is_stopped() {
                continue;
            }
            handles.push(tokio::spawn(async move {
                let _guard = guard;
                let Ok(_permit) = workers.acquire_owned().await else {
                    return;
                };
                // Every RPC has its own transport deadline. No cancelling
                // aggregate timeout surrounds database/terminal/cascade work.
                if let Err(error) = reconcile_owner_receipts(&db, &registry, &daemon_id).await {
                    tracing::warn!(%daemon_id, placement_ids = ?ids, %error, "owner receipts remain pending");
                    if matches!(error, ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }) { return; }
                }
                for id in &ids {
                    let attempt = async {
                        let Some(placement) = WorkspacePlacementRepo::get_by_id(&*db, id).await?
                        else {
                            return Ok(());
                        };
                        let reconciled = if placement.state == PlacementState::Disconnected {
                            reconcile_workspace_placement(
                                &db,
                                &event_bus,
                                &registry,
                                task_service.as_deref(),
                                &placement,
                            )
                            .await?
                        } else {
                            false
                        };
                        if !reconciled {
                            if let Some(ready) = WorkspacePlacementRepo::get_by_id(&*db, id)
                                .await?
                                .filter(|placement| placement.state == PlacementState::Ready)
                            {
                                finish_reconciled_cascades(
                                    &db,
                                    &registry,
                                    task_service.as_deref(),
                                    &ready,
                                )
                                .await?;
                            }
                        }
                        Ok::<(), ServiceError>(())
                    }
                    .await;
                    if let Err(error) = attempt {
                        if matches!(error, ServiceError::WorkspaceResetRequired { .. }) {
                            tracing::debug!(placement_id = %id, %daemon_id, %error, "workspace reset remains pending");
                        } else {
                            tracing::warn!(placement_id = %id, %daemon_id, %error, "workspace reconciliation remains pending");
                        }
                        if matches!(error, ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }) { return; }
                    }
                }
                tokio::spawn(async move {
                    if let Err(error) = registry.retry_retained_terminals(&daemon_id).await {
                        tracing::warn!(%daemon_id, placement_ids = ?ids, %error, "terminal acknowledgement remains pending");
                    }
                });
            }));
        }
        Ok(settled)
    }
}

async fn fail_placement_executions(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: &WorkspacePlacement,
    cause: PlacementFailureCause,
) -> Result<u64> {
    let mut count = 0;
    for execution in ExecutionRepo::list_running(db).await? {
        if execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            && fail_owned_execution(db, event_bus, placement, &execution, cause.clone()).await?
        {
            count += 1;
        }
    }
    Ok(count)
}

async fn fail_owned_execution(
    db: &SqliteDb,
    event_bus: &EventBus,
    placement: &WorkspacePlacement,
    execution: &Execution,
    cause: PlacementFailureCause,
) -> Result<bool> {
    let now = now_rfc3339();
    let mut input = crate::task_service::execution::ledger::terminal_with_ledger(
        TerminalizeExecution {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            lease_owner: execution.lease_owner.clone(),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(StopReason::DaemonDisconnected)),
            stopped_by: Some(Some(
                api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor).display(),
            )),
            stopped_at: Some(Some(now.clone())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some(
                json!({"cause": cause.to_string(), "placement_id": placement.id,
            "daemon_id": placement.daemon_id})
                .to_string(),
            )),
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: Some(placement.id.clone()),
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
        Vec::new(),
        None,
        None,
    );
    input.mark_unreplayable_pending_unsettled = true;
    let outcome = ExecutionRepo::terminalize_with_ledger(db, input).await?;
    let ExecutionTerminalOutcome::Committed { execution, .. } = outcome else {
        return Ok(false);
    };
    // Infrastructure settlement parks the Task without entering work/review
    // retry settlement. An explicit same-owner retry is the next admission.
    annotate_owner_recovery(db, &execution, &cause).await?;
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: execution.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(execution.task_id),
            execution_id: Some(execution.id),
            reason: cause.to_string(),
        },
    });
    Ok(true)
}

async fn annotate_owner_recovery(
    db: &SqliteDb,
    execution: &Execution,
    cause: &PlacementFailureCause,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, &execution.task_id, false).await? else {
        return Ok(());
    };
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let dedupe_key = format!("workspace-execution-recovery:{}", execution.id);
    let recorded: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM domain_event WHERE dedupe_key = ?)")
            .bind(&dedupe_key)
            .fetch_one(&mut *transaction)
            .await?;
    if recorded {
        transaction.rollback().await?;
        return Ok(());
    }
    let actor = api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor);
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
    let existing_reason = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|annotation| annotation["blocking_reason"].as_str().map(str::to_owned));
    let can_annotate = task.failed_json.is_none()
        && workflow.state_kind(&task.status) != Some(api_types::StateKind::Terminal)
        && (task.error_annotation.is_none()
            || matches!(
                existing_reason.as_deref(),
                Some("owner_disconnected_timeout" | "owner_lost_execution")
            ));
    let annotation = json!({"type": api_types::FailureKind::RecoveryRequired,
            "blocking_reason": cause.to_string(), "blocked_by": actor.display(),
            "blocked_at": now_rfc3339(), "blocked_execution_id": execution.id,
            "message": format!("Workspace owner recovery required: {cause}"),
    });
    if can_annotate {
        // The heartbeat runs outside the Task lease, so this blocking
        // annotation is usually queued. It must never be dropped by a status
        // change, so it is identity-fenced and its predicate re-derives
        // `can_annotate` when it runs: no failure, a non-terminal status and
        // no newer foreign annotation.
        let terminal = workflow
            .states
            .iter()
            .filter(|state| state.kind == api_types::StateKind::Terminal)
            .map(|state| state.name.clone())
            .collect::<Vec<_>>();
        let updated = db::task_writer::TaskQuery::new(db,&task.id,"UPDATE task SET error_annotation = ?, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?
            AND deleted_at IS NULL AND failed_json IS NULL AND status NOT IN (SELECT value FROM json_each(?))
            AND (error_annotation IS NULL OR (json_valid(error_annotation) AND json_extract(error_annotation, '$.blocking_reason') IN ('owner_disconnected_timeout', 'owner_lost_execution')))")
            .bind(annotation.to_string()).bind(now_rfc3339()).bind(&task.id).bind(task.version)
            .bind(serde_json::to_string(&terminal).expect("terminal states serialize"))
            .identity_fenced()
            .execute_in_tx(&mut transaction).await?;
        if updated.applied().is_some_and(|rows| rows != 1) {
            return Err(db::DbError::VersionConflict.into());
        }
    }
    // The marker commits with the annotation. A sweep can repair the earlier
    // terminal -> annotation crash window, without undoing a later user retry.
    db::DomainEventRepo::append_event_in_tx(
        db,
        &mut transaction,
        &db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "workspace.execution_recovery_required".to_owned(),
            entity_type: "execution".to_owned(),
            entity_id: execution.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: execution.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(dedupe_key),
            payload_json: json!({"task_id": task.id, "execution_id": execution.id,
            "failure_cause": cause.to_string(), "annotated": can_annotate})
            .to_string(),
            created_at: now_rfc3339(),
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) fn owner_execution_failure_cause(
    execution: &Execution,
) -> Option<PlacementFailureCause> {
    if execution.status != ExecutionStatus::Failed
        || execution.stop_reason != Some(StopReason::DaemonDisconnected)
    {
        return None;
    }
    let error: serde_json::Value = serde_json::from_str(execution.error.as_deref()?).ok()?;
    match error["cause"].as_str()? {
        "owner_lost_execution" => Some(PlacementFailureCause::OwnerLostExecution),
        "owner_disconnected_timeout" => Some(PlacementFailureCause::OwnerDisconnectedTimeout),
        _ => None,
    }
}

async fn repair_owner_recovery(db: &SqliteDb, placement: &WorkspacePlacement) -> Result<bool> {
    let execution_ids = sqlx::query_scalar::<_, String>(
        "SELECT e.id FROM execution e WHERE e.workspace_id = ? AND e.status = 'failed'
         AND e.stop_reason = 'daemon_disconnected' AND json_valid(e.error)
         AND json_extract(e.error, '$.cause')
             IN ('owner_disconnected_timeout', 'owner_lost_execution')
         AND NOT EXISTS (SELECT 1 FROM execution newer WHERE newer.task_id = e.task_id
             AND CASE newer.role WHEN 'executor' THEN 'coder' ELSE newer.role END
                 = CASE e.role WHEN 'executor' THEN 'coder' ELSE e.role END
             AND (newer.created_at > e.created_at OR (newer.created_at = e.created_at AND newer.id > e.id)))")
        .bind(&placement.workspace_id).fetch_all(db.pool()).await?;
    let mut lost_execution = false;
    for execution_id in execution_ids {
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else {
            continue;
        };
        let Some(cause) = owner_execution_failure_cause(&execution) else {
            continue;
        };
        lost_execution |= cause == PlacementFailureCause::OwnerLostExecution;
        annotate_owner_recovery(db, &execution, &cause).await?;
    }
    Ok(lost_execution)
}

async fn annotate_owner_workspace_reset(
    db: &SqliteDb,
    placement: &WorkspacePlacement,
    message: &str,
) -> Result<()> {
    let Some(task) = TaskRepo::get_by_id(db, &placement.task_id, false).await? else {
        return Ok(());
    };
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", &task.project_id))?;
    let actor = api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor);
    let workflow =
        WorkflowEngine::resolve_workflow_for_task(&task, &project.workflow_definition, &actor);
    if task.failed_json.is_some()
        || workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
        || task
            .error_annotation
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .is_some_and(|annotation| {
                annotation["blocking_reason"] == "owner_workspace_reset_required"
            })
    {
        return Ok(());
    }
    let annotation = json!({"type": api_types::FailureKind::WorkspaceResetRequired,
        "blocking_reason": "owner_workspace_reset_required", "blocked_by": actor.display(),
        "blocked_at": now_rfc3339(), "message": message});
    TaskRepo::update_status(
        db,
        UpdateTaskStatus {
            id: task.id.clone(),
            expected_version: task.version,
            status: task.status,
            assignee_id: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    tracing::warn!(placement_id = %placement.id, daemon_id = ?placement_execution_daemon_id(placement), message, "workspace owner reset required");
    Ok(())
}

pub(crate) async fn apply_owner_cleanup(
    db: &SqliteDb,
    daemon_id: &str,
    notification: &api_types::WorkspaceCleanupResult,
) -> Result<crate::daemon_transport::DaemonTerminalDisposition> {
    use crate::daemon_transport::DaemonTerminalDisposition;
    if !notification.cleaned
        || notification.entry_id.trim().is_empty()
        || notification.operation_id.trim().is_empty()
    {
        return Ok(DaemonTerminalDisposition::Ignore);
    }
    for state in [PlacementState::Cleaning, PlacementState::Cleaned] {
        for placement in
            WorkspacePlacementRepo::list_by_daemon_and_state(db, daemon_id, state).await?
        {
            if placement.workspace_handle.as_deref() != Some(notification.workspace_handle.as_str())
                || u64::try_from(placement.generation).ok() != Some(notification.generation)
            {
                continue;
            }
            if placement.state == PlacementState::Cleaned {
                return Ok(DaemonTerminalDisposition::Acknowledge);
            }
            let mut transaction = db::begin_immediate(db.pool()).await?;
            WorkspacePlacementRepo::update_in_tx(
                db,
                &mut transaction,
                placement_update(&placement, PlacementState::Cleaned, None),
            )
            .await?;
            sqlx::query("UPDATE workspace SET status = 'cleaned', cleanup_after = NULL, cleanup_attempts = 0, last_cleanup_error = NULL, error = NULL, updated_at = ? WHERE id = ?")
                .bind(now_rfc3339()).bind(&placement.workspace_id).execute(&mut *transaction).await?;
            transaction.commit().await?;
            return Ok(DaemonTerminalDisposition::Acknowledge);
        }
    }
    Ok(DaemonTerminalDisposition::Ignore)
}

async fn reconcile_owner_receipts(
    db: &Arc<SqliteDb>,
    registry: &Arc<DaemonConnectionRegistry>,
    daemon_id: &str,
) -> Result<()> {
    use crate::daemon_transport::workspace_client::{DaemonWorkspaceClient, WorkspaceClientError};
    let client = DaemonWorkspaceClient::new(Arc::clone(registry)).with_receipts(Arc::clone(db));
    let map_error = |error| match error {
        WorkspaceClientError::Transport(error) => error,
        WorkspaceClientError::Daemon(error) => ServiceError::invalid_operation(format!(
            "owner operation reconciliation failed: {}",
            error.message
        )),
    };
    client
        .reconcile_pending_operations(daemon_id)
        .await
        .map_err(map_error)?;
    client
        .retry_acknowledgements(daemon_id)
        .await
        .map_err(map_error)?;
    Ok(())
}

pub(crate) async fn reconcile_workspace_placement(
    db: &Arc<SqliteDb>,
    event_bus: &EventBus,
    registry: &Arc<DaemonConnectionRegistry>,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
) -> Result<bool> {
    let Some(daemon_id) = placement_execution_daemon_id(placement) else {
        return Ok(false);
    };
    if !DaemonRepo::get_by_id(&**db, daemon_id)
        .await?
        .is_some_and(|daemon| daemon.status == db::DaemonStatus::Online)
    {
        return Ok(false);
    }
    let Some(connection) = registry.get(daemon_id) else {
        return Ok(false);
    };
    let owner = crate::daemon_transport::execution_lease_owner(daemon_id, connection.id());
    for execution in ExecutionRepo::list_running(&**db).await? {
        if execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            && execution.lease_owner.as_deref() == Some(owner.as_str())
            && placement
                .disconnected_at
                .as_deref()
                .is_some_and(|disconnected_at| {
                    !rfc3339_is_after(connection.connected_at(), disconnected_at)
                })
            && execution
                .lease_expires_at
                .as_deref()
                .is_none_or(|expiry| !rfc3339_is_after(expiry, &now_rfc3339()))
        {
            // A frozen process can retain the socket from before suspension.
            // A newer connection may reuse a numeric ID after server restart;
            // it must still be allowed to describe and resume the attempt.
            return Ok(false);
        }
    }
    let Some(facts) = connection.snapshot().filter(|facts| {
        placement.owner_kind == PlacementOwnerKind::Server || !facts.workspace_incapable
    }) else {
        return Ok(false);
    };
    if placement.owner_kind == PlacementOwnerKind::Server {
        // A shared-mount provider can replay its terminal journal, but its
        // workspace is owned and described by the server backend. Keep the
        // attempt suspended until its retained terminal report is settled.
        if ExecutionRepo::list_running(&**db)
            .await?
            .iter()
            .any(|execution| {
                execution.workspace_id.as_deref() == Some(placement.workspace_id.as_str())
            })
        {
            return Ok(false);
        }
        let Some(task_service) = task_service else {
            return Ok(false);
        };
        let state = task_service
            .workspace_backend_router()
            .for_placement(placement)?
            .describe(placement)
            .await?;
        if !state.exists || state.head_sha.is_none() {
            annotate_owner_workspace_reset(db, placement, "Server workspace is missing").await?;
            return Err(ServiceError::WorkspaceResetRequired {
                task_id: placement.task_id.clone(),
                reason: "server workspace is missing".to_owned(),
            });
        }
        let described = api_types::WorkspaceDescribeResult {
            workspace_handle: placement.workspace_handle.clone().ok_or_else(|| {
                ServiceError::invalid_operation("disconnected placement has no handle")
            })?,
            generation: u64::try_from(placement.generation)
                .map_err(|_| ServiceError::invalid_operation("invalid placement generation"))?,
            exists: state.exists,
            head_sha: state.head_sha,
            dirty: state.dirty,
            branch: state.branch,
            locked: state.locked,
            active_execution_ids: state.active_execution_ids,
            journaled_execution_ids: state.journaled_execution_ids,
        };
        return complete_workspace_reconciliation(
            db,
            event_bus,
            registry,
            Some(task_service),
            placement,
            described,
            false,
            facts.connection_id,
        )
        .await;
    }
    if !registry.is_current(daemon_id, facts.connection_id) {
        return Ok(false);
    }
    let owner_client =
        crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(Arc::clone(registry))
            .with_receipts(Arc::clone(db));
    let db = &**db;
    let handle = placement
        .workspace_handle
        .as_deref()
        .ok_or_else(|| ServiceError::invalid_operation("disconnected placement has no handle"))?;
    let generation = u64::try_from(placement.generation)
        .map_err(|_| ServiceError::invalid_operation("invalid placement generation"))?;
    let reference = api_types::WorkspaceHandleReference {
        daemon_id: daemon_id.to_owned(),
        runtime_id: placement
            .runtime_id
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("placement has no runtime"))?,
        placement_id: placement.id.clone(),
        workspace_handle: handle.to_owned(),
        generation,
    };
    let mut state: api_types::WorkspaceDescribeResult = registry
        .send_request_for_connection(
            daemon_id,
            facts.connection_id,
            api_types::METHOD_WORKSPACE_DESCRIBE,
            api_types::WorkspaceDescribeParams {
                workspace: reference.clone(),
            },
            api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
        )
        .await?;
    if !registry.is_current(daemon_id, facts.connection_id) {
        return Ok(false);
    }
    if state.workspace_handle != handle || state.generation != generation {
        return Err(ServiceError::invalid_operation(
            "stale_generation: workspace describe returned a different placement",
        ));
    }
    if !registry
        .drain_execution_journal(
            daemon_id,
            facts.connection_id,
            &state.journaled_execution_ids,
        )
        .await?
    {
        return Ok(false);
    }
    let now = Utc::now();
    let owner = crate::daemon_transport::execution_lease_owner(daemon_id, facts.connection_id);
    let mut active = false;
    let mut reconciled = placement.clone();
    for execution in ExecutionRepo::list_running(db).await? {
        if execution.workspace_id.as_deref() != Some(placement.workspace_id.as_str()) {
            continue;
        }
        if state.journaled_execution_ids.contains(&execution.id) {
            // A spawned sink may still be committing, or an ACK/drain may
            // have been interrupted. Stay suspended and let the sweep retry.
            return Ok(false);
        }
        if state.active_execution_ids.contains(&execution.id) {
            let resumed = WorkspacePlacementRepo::resume_disconnected_execution_lease(
                db,
                &reconciled,
                &execution,
                db::RenewExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution.execution_version,
                    owner: owner.clone(),
                    lease_expires_at: (now + ChronoDuration::seconds(60)).to_rfc3339(),
                    now: now.to_rfc3339(),
                },
            )
            .await?;
            if !matches!(resumed, db::ExecutionLeaseMutation::Updated(_)) {
                return Ok(false);
            }
            active = true;
        } else {
            if !fail_owned_execution(
                db,
                event_bus,
                &reconciled,
                &execution,
                PlacementFailureCause::OwnerLostExecution,
            )
            .await?
            {
                return Ok(false);
            }
            reconciled = WorkspacePlacementRepo::update(
                db,
                placement_update(
                    &reconciled,
                    PlacementState::Disconnected,
                    Some(PlacementFailureCause::OwnerLostExecution),
                ),
            )
            .await?;
        }
    }
    if repair_owner_recovery(db, &reconciled).await?
        && reconciled.failure_cause != Some(PlacementFailureCause::OwnerLostExecution)
    {
        reconciled = WorkspacePlacementRepo::update(
            db,
            placement_update(
                &reconciled,
                PlacementState::Disconnected,
                Some(PlacementFailureCause::OwnerLostExecution),
            ),
        )
        .await?;
    }
    if !state.exists || state.head_sha.is_none() {
        // Do not recreate a worktree while an owner still reports a writer.
        if active {
            return Ok(false);
        }
        let workspace = db::WorkspaceRepo::get_by_id(db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", &placement.workspace_id))?;
        let base_sha = workspace
            .before_sha
            .ok_or_else(|| ServiceError::invalid_operation("workspace has no recovery base"))?;
        let operation_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!(
                "forge:workspace.recover:{}:{}:{}",
                placement.id,
                placement.generation,
                placement.disconnected_at.as_deref().unwrap_or("")
            )
            .as_bytes(),
        )
        .to_string();
        let prepared = match owner_client
            .prepare(
                daemon_id,
                api_types::WorkspacePrepareParams {
                    fence: api_types::WorkspaceMutationFence {
                        daemon_id: reference.daemon_id.clone(),
                        runtime_id: reference.runtime_id.clone(),
                        placement_id: placement.id.clone(),
                        operation_id,
                        generation,
                        expected: api_types::WorkspaceOperationExpected::BaseSha {
                            sha: base_sha.clone(),
                        },
                    },
                    repo_location_id: placement.repo_location_id.clone(),
                    workspace_id: workspace.id,
                    task_id: workspace.task_id,
                    base_ref: base_sha,
                    branch: workspace.branch,
                },
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(crate::daemon_transport::workspace_client::WorkspaceClientError::Transport(
                error,
            )) => return Err(error),
            Err(crate::daemon_transport::workspace_client::WorkspaceClientError::Daemon(error)) => {
                if error.code != api_types::INVALID_INPUT && error.code != "version_conflict" {
                    return Err(ServiceError::invalid_operation(format!(
                        "{}: {}",
                        error.code, error.message
                    )));
                }
                annotate_owner_workspace_reset(db, placement, &error.message).await?;
                return Err(ServiceError::WorkspaceResetRequired {
                    task_id: placement.task_id.clone(),
                    reason: error.message,
                });
            }
        };
        if prepared.workspace.generation != generation
            || prepared.workspace.workspace_handle.is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "owner returned an invalid recovered workspace",
            ));
        }
        let mut recovered_reference = reference;
        recovered_reference.workspace_handle = prepared.workspace.workspace_handle;
        state = registry
            .send_request_for_connection(
                daemon_id,
                facts.connection_id,
                api_types::METHOD_WORKSPACE_DESCRIBE,
                api_types::WorkspaceDescribeParams {
                    workspace: recovered_reference.clone(),
                },
                api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
            )
            .await?;
        if !registry.is_current(daemon_id, facts.connection_id) {
            return Ok(false);
        }
        if state.workspace_handle != recovered_reference.workspace_handle
            || state.generation != generation
        {
            return Err(ServiceError::invalid_operation(
                "owner changed the recovered workspace fence",
            ));
        }
        if !state.exists || state.head_sha.is_none() {
            annotate_owner_workspace_reset(
                db,
                placement,
                "Workspace owner reports a missing branch",
            )
            .await?;
            return Err(ServiceError::WorkspaceResetRequired {
                task_id: placement.task_id.clone(),
                reason: "owner workspace branch is missing".to_owned(),
            });
        }
        if !state.active_execution_ids.is_empty() || !state.journaled_execution_ids.is_empty() {
            return Ok(false);
        }
    }
    complete_workspace_reconciliation(
        db,
        event_bus,
        registry,
        task_service,
        &reconciled,
        state,
        active,
        facts.connection_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn complete_workspace_reconciliation(
    db: &SqliteDb,
    event_bus: &EventBus,
    registry: &DaemonConnectionRegistry,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
    state: api_types::WorkspaceDescribeResult,
    active: bool,
    connection_id: u64,
) -> Result<bool> {
    let Some(daemon_id) = placement_execution_daemon_id(placement) else {
        return Ok(false);
    };
    let recorded_head = sqlx::query_scalar::<_, Option<String>>(
        "SELECT head_sha FROM (
         SELECT head_sha, recorded_at AS evidence_at, placement_id AS evidence_id FROM workspace_expected_head
         WHERE placement_id = ? AND generation = ?
         UNION ALL
         SELECT e.after_sha AS head_sha, (COALESCE(e.stopped_at, (SELECT MIN(receipt.created_at) FROM execution_terminal_receipt receipt WHERE receipt.execution_id = e.id), e.created_at)) AS evidence_at, e.id AS evidence_id FROM execution e WHERE e.workspace_id = ? AND e.after_sha IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM command_receipt reset
             WHERE reset.operation = 'daemon.workspace.reset'
               AND json_extract(reset.outcome_json, '$.metadata.placement_id') = ?
               AND json_extract(reset.outcome_json, '$.metadata.generation') = ?
               AND json_extract(reset.outcome_json, '$.metadata.status') = 'result'
               AND julianday(reset.committed_at) > julianday(e.created_at))
         ) ORDER BY julianday(evidence_at) DESC, evidence_id DESC LIMIT 1",
    )
    .bind(&placement.id)
    .bind(placement.generation)
    .bind(&placement.workspace_id)
    .bind(&placement.id)
    .bind(placement.generation)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    let recorded_head = match recorded_head {
        Some(head) => Some(head),
        None => db::WorkspaceRepo::get_by_id(db, &placement.workspace_id)
            .await?
            .and_then(|workspace| workspace.before_sha),
    };
    if !active
        && recorded_head
            .as_ref()
            .is_some_and(|head| state.head_sha.as_ref() != Some(head))
    {
        annotate_owner_workspace_reset(
            db,
            placement,
            "Workspace owner HEAD differs from recorded execution evidence",
        )
        .await?;
        return Err(ServiceError::WorkspaceResetRequired {
            task_id: placement.task_id.clone(),
            reason: "owner head differs from recorded execution evidence".to_owned(),
        });
    }
    if !registry.is_current(daemon_id, connection_id) {
        return Ok(false);
    }
    let cause = (placement.failure_cause == Some(PlacementFailureCause::OwnerLostExecution))
        .then_some(PlacementFailureCause::OwnerLostExecution);
    let mut update = placement_update(placement, PlacementState::Ready, cause);
    update.disconnected_at = Some(None);
    update.workspace_handle = Some(Some(state.workspace_handle.clone()));
    let mut transaction = db::begin_immediate(db.pool()).await?;
    let ready = WorkspacePlacementRepo::update_in_tx(db, &mut transaction, update).await?;
    let event = db::DomainEventRepo::append_event_in_tx(db, &mut transaction, &db::CreateDomainEvent {
        id: db::new_uuid_v4(), event_type: "workspace.owner_reconciled".to_owned(),
        entity_type: "workspace_placement".to_owned(), entity_id: ready.id.clone(),
        actor_type: "system".to_owned(), actor_id: None, scope_type: "task".to_owned(),
        scope_id: ready.task_id.clone(), correlation_id: ready.id.clone(), causation_id: None,
        causation_depth: 0, dedupe_key: Some(format!("workspace-ready:{}:{}", ready.id, ready.version)),
        payload_json: json!({"task_id": ready.task_id, "placement_id": ready.id, "head_sha": state.head_sha}).to_string(),
        created_at: now_rfc3339(),
    }).await?;
    sqlx::query(
        "UPDATE attention_projection SET status = 'resolved', resolved_at = ?,
        snoozed_until = NULL, source_event_id = ?, updated_at = ?, version = version + 1
        WHERE dedupe_key = ? AND status <> 'resolved'",
    )
    .bind(now_rfc3339())
    .bind(&event.id)
    .bind(now_rfc3339())
    .bind(format!("workspace-owner:{}", ready.id))
    .execute(&mut *transaction)
    .await?;
    db::task_writer::BulkTaskQuery::new(db,
        "UPDATE task SET metadata_json = json_remove(COALESCE(metadata_json, '{}'),
        '$.dispatch_disposition', '$.deferred_dispatch', '$.owner_wait'), version = version + 1, updated_at = ?
        WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
             OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL
             OR json_type(metadata_json, '$.owner_wait') IS NOT NULL)",
    )
    .bind(now_rfc3339())
    .bind(&ready.task_id)
    .bind(&ready.task_id)
    .execute_in_tx(&mut transaction)
    .await?;
    let exhausted_tasks:Vec<String>=sqlx::query_scalar("SELECT id FROM task WHERE (id=? OR parent_task_id=?) AND deleted_at IS NULL AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation,'$.blocking_reason')='review_ci_infrastructure_exhausted'").bind(&ready.task_id).bind(&ready.task_id).fetch_all(&mut *transaction).await?;
    for task in exhausted_tasks {
        let window = format!("reconnect:{}:{}", ready.id, ready.version);
        db::budget::reset(
            &mut transaction,
            &task,
            db::budget::Kind::ReviewCiInfrastructure.key(),
            &window,
        )
        .await?;
        sqlx::query("UPDATE task SET entry_barrier_json=json_set(entry_barrier_json,'$.started_at',?) WHERE id=?").bind(&window).bind(&task).execute(&mut *transaction).await?;
    }
    db::task_writer::BulkTaskQuery::new(db,"UPDATE task SET blocked_json = NULL,
        entry_barrier_json = entry_barrier_json,
        error_annotation = json_set(error_annotation, '$.blocking_reason', 'review_ci_infrastructure'),
        version = version + 1, updated_at = ? WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation, '$.blocking_reason') = 'review_ci_infrastructure_exhausted'")
        .bind(now_rfc3339()).bind(&ready.task_id).bind(&ready.task_id).execute_in_tx(&mut transaction).await?;
    sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1
        WHERE status <> 'resolved' AND dedupe_key IN (?, ?)")
        .bind(now_rfc3339()).bind(now_rfc3339()).bind(format!("review-ci:{}", ready.task_id))
        .bind(format!("task-owner-wait:{}", ready.task_id)).execute(&mut *transaction).await?;
    transaction.commit().await?;
    if let Some(task_service) = task_service {
        // Cascades and terminal ACK retries run outside the owner permit.
        // A ready-placement sweep can redrive a crash here.
        if let Err(error) =
            finish_reconciled_cascades(db, registry, Some(task_service), &ready).await
        {
            if matches!(
                error,
                ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }
            ) {
                return Err(error);
            }
            tracing::warn!(placement_id = %ready.id, %daemon_id, %error, "reconciled cascade remains pending");
        }
    }
    event_bus.publish(ForgeEvent {
        event_type: "reconciliation.event".to_owned(),
        entity_id: ready.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReconciliationEvent {
            task_id: Some(ready.task_id),
            execution_id: None,
            reason: "workspace_owner_ready".to_owned(),
        },
    });
    Ok(true)
}

async fn finish_reconciled_cascades(
    db: &SqliteDb,
    registry: &DaemonConnectionRegistry,
    task_service: Option<&TaskService>,
    placement: &WorkspacePlacement,
) -> Result<()> {
    let Some(task_service) = task_service else {
        return Ok(());
    };
    for execution_id in sqlx::query_scalar::<_, String>(
        "SELECT e.id FROM execution e WHERE e.workspace_id = ? AND e.status != 'running'
         AND NOT EXISTS (SELECT 1 FROM execution newer WHERE newer.task_id = e.task_id
             AND CASE newer.role WHEN 'executor' THEN 'coder' ELSE newer.role END
                 = CASE e.role WHEN 'executor' THEN 'coder' ELSE e.role END
             AND (newer.created_at > e.created_at OR (newer.created_at = e.created_at AND newer.id > e.id)))
         ORDER BY e.created_at DESC, e.id DESC")
        .bind(&placement.workspace_id).fetch_all(db.pool()).await? {
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else { continue; };
        if owner_execution_failure_cause(&execution).is_none() {
            task_service.try_cascade_executor_completion(&execution_id).await?;
        }
    }
    if let Some(daemon_id) = placement_execution_daemon_id(placement) {
        let registry = registry.clone();
        let daemon_id = daemon_id.to_owned();
        tokio::spawn(async move {
            if let Err(error) = registry.retry_retained_terminals(&daemon_id).await {
                tracing::warn!(%daemon_id, %error, "reconciled terminal acknowledgement remains pending");
            }
        });
    }
    Ok(())
}

pub(crate) const DAEMON_REPORT_RECONCILE_MIN_AGE: Duration = Duration::from_secs(60);

pub(crate) async fn reconcile_daemon_report_executions(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_service: Option<&TaskService>,
    daemon: &Daemon,
    active_execution_ids: &[String],
) -> Result<u64> {
    if is_embedded_daemon_machine(&daemon.machine_id) {
        return Ok(0);
    }

    let created_before = (Utc::now()
        - ChronoDuration::seconds(DAEMON_REPORT_RECONCILE_MIN_AGE.as_secs() as i64))
    .to_rfc3339();
    let execution_ids = sqlx::query_scalar::<_, String>(
        "SELECT e.id FROM execution e
         LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
         LEFT JOIN agent_current a ON a.id = e.agent_id
         WHERE e.status = 'running' AND e.created_at < ?
           AND (p.owner_kind IS NULL OR p.owner_kind != 'daemon')
           AND CASE WHEN e.workspace_id IS NOT NULL
               THEN CASE p.owner_kind WHEN 'daemon' THEN p.daemon_id ELSE p.execution_daemon_id END
               ELSE COALESCE(NULLIF(TRIM(json_extract(e.executor_config_snapshot_json, '$.daemon_id')), ''), a.daemon_id)
               END = ?
         ORDER BY e.created_at, e.id",
    )
    .bind(&created_before)
    .bind(&daemon.id)
    .fetch_all(db.pool())
    .await?;

    let mut interrupted = 0_u64;
    for execution_id in execution_ids {
        if active_execution_ids.contains(&execution_id) {
            continue;
        }
        let Some(execution) = ExecutionRepo::get_by_id(db, &execution_id).await? else {
            continue;
        };
        let Some(_updated) = fail_execution_daemon_disconnected(
            db,
            event_bus,
            task_service,
            FailDaemonDisconnectedExecution {
                execution: &execution,
                daemon_id: &daemon.id,
                error_message: "daemon no longer running this execution".to_owned(),
                stopped_by: &api_types::Actor::system(api_types::SystemComponent::DaemonReport)
                    .display(),
                reconciliation_reason: "daemon_disconnected",
            },
        )
        .await?
        else {
            continue;
        };
        interrupted += 1;
    }
    Ok(interrupted)
}

fn agent_timed_out(agent: &Agent) -> bool {
    let Some(last_heartbeat_at) = &agent.last_heartbeat_at else {
        return true;
    };
    let Some(last_heartbeat_at) = parse_rfc3339_unix_seconds(last_heartbeat_at) else {
        return true;
    };
    let heartbeat_interval = agent.heartbeat_interval_seconds.max(1) as u64;
    let max_missed = agent.max_missed_heartbeats.max(1) as u64;
    let timeout = heartbeat_interval.saturating_mul(max_missed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);

    now.saturating_sub(last_heartbeat_at) > timeout
}

fn parse_rfc3339_unix_seconds(value: &str) -> Option<u64> {
    let (date, time_with_offset) = value.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;

    let (time, offset_seconds) = split_time_offset(time_with_offset)?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second = time_parts.next()?.split('.').next()?.parse::<u32>().ok()?;

    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add((hour as i64).checked_mul(3_600)?)?
        .checked_add((minute as i64).checked_mul(60)?)?
        .checked_add(second as i64)?
        .checked_sub(offset_seconds as i64)?;

    u64::try_from(seconds).ok()
}

fn split_time_offset(value: &str) -> Option<(&str, i32)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, 0));
    }
    let offset_index = value
        .char_indices()
        .skip(1)
        .find_map(|(index, character)| matches!(character, '+' | '-').then_some(index))?;
    let (time, offset) = value.split_at(offset_index);
    let sign = if offset.starts_with('+') { 1 } else { -1 };
    let mut offset_parts = offset[1..].split(':');
    let hours = offset_parts.next()?.parse::<i32>().ok()?;
    let minutes = offset_parts.next()?.parse::<i32>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((time, sign * (hours * 3_600 + minutes * 60)))
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468).into()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        daemon_service::{DaemonReportInput, DaemonService, DetectedCliInput},
        daemon_transport::DaemonConnectionRegistry,
        workflow::{default_roles, default_states},
        TaskService,
    };
    use db::{
        create_sqlite_pool, new_uuid_v4, run_migrations, AgentContextScopeRepo, AgentProfileRepo,
        AgentSession, CreateAgent, CreateAgentContextScope, CreateAgentProfile, CreateAgentSession,
        CreateExecution, CreateProject, CreateRepo, CreateReview, CreateTask,
        CreateTaskRoleAssignment, CreateWorkspace, CreateWorkspaceLease, DaemonRepo, DaemonStatus,
        DomainEventRepo, RepoRepo, ReviewRepo, ReviewStatus, TaskRoleAssignmentRepo, TaskStatus,
        UpdateProject, UpsertDaemon, WorkspaceRepo, WorkspaceStatus,
    };
    use executors::{ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError};
    use serde_json::Value;

    #[derive(Default)]
    struct RecordingCancelExecutor {
        cancelled: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for RecordingCancelExecutor {
        async fn execute(
            &self,
            _ctx: ExecutionContext,
        ) -> std::result::Result<ExecutionResult, ExecutorError> {
            Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: None,
                agent_session_id: None,
                summary: None,
                error: None,
                usage_reports: Vec::new(),
                ..Default::default()
            })
        }

        async fn cancel(&self, execution_id: &str) -> std::result::Result<(), ExecutorError> {
            self.cancelled
                .lock()
                .expect("cancel log lock")
                .push(execution_id.to_owned());
            Ok(())
        }
    }

    async fn sqlite_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "forge".to_owned(),
                remote_url: Some("https://example.com/forge.git".to_owned()),
                local_path: None,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");
        (project_id, repo_id)
    }

    async fn seed_agent(
        db: &SqliteDb,
        status: AgentStatus,
        last_heartbeat_at: Option<String>,
    ) -> Agent {
        let now = now_rfc3339();
        let daemon_id = new_uuid_v4();
        DaemonRepo::upsert_by_machine_id(
            db,
            UpsertDaemon {
                max_concurrent_runs: None,
                id: daemon_id.clone(),
                machine_id: format!("machine-{daemon_id}"),
                hostname: "test-host".to_owned(),
                os: "linux".to_owned(),
                arch: "x86_64".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon creates");

        AgentRepo::create(
            db,
            CreateAgent {
                id: new_uuid_v4(),
                name: "codex".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: Some(daemon_id),
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 1,
                max_missed_heartbeats: 1,
                status,
                last_heartbeat_at,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("agent creates")
    }

    async fn seed_task(
        db: &SqliteDb,
        project_id: String,
        status: TaskStatus,
        agent_id: Option<String>,
    ) -> Task {
        seed_task_with_assignee(db, project_id, status, "agent", agent_id).await
    }

    async fn seed_task_with_assignee(
        db: &SqliteDb,
        project_id: String,
        status: TaskStatus,
        assignee_type: &str,
        assignee_id: Option<String>,
    ) -> Task {
        let now = now_rfc3339();
        let task = TaskRepo::create(
            db,
            CreateTask {
                id: new_uuid_v4(),
                project_id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: assignee_id.as_ref().map(|_| assignee_type.to_owned()),
                assignee_id: assignee_id.clone(),
                title: "Recover me".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status,
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
        if assignee_type == "agent" {
            if let Some(agent_id) = assignee_id {
                TaskRoleAssignmentRepo::assign(
                    db,
                    CreateTaskRoleAssignment {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        role_name: default_roles::CODER.to_owned(),
                        assignee_type: Some(db::AssigneeKind::Agent),
                        assignee_id: Some(agent_id),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .expect("role assignment creates");
            }
        }
        task
    }

    async fn seed_running_execution(
        db: &SqliteDb,
        task_id: String,
        agent_id: String,
        agent_session_id: Option<String>,
    ) -> db::Execution {
        seed_running_execution_in_workspace(db, task_id, agent_id, agent_session_id, None).await
    }

    async fn seed_running_execution_in_workspace(
        db: &SqliteDb,
        task_id: String,
        agent_id: String,
        agent_session_id: Option<String>,
        workspace_id: Option<String>,
    ) -> db::Execution {
        let project_version: i64 = sqlx::query_scalar(
            "SELECT project.version
             FROM project
             JOIN task ON task.project_id = project.id
             WHERE task.id = ?",
        )
        .bind(&task_id)
        .fetch_one(db.pool())
        .await
        .expect("execution Project version loads");
        let now = now_rfc3339();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: new_uuid_v4(),
                task_id,
                agent_id: Some(agent_id),
                role: "coder".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    json!({
                        "executor_type": "shell",
                        "config": {},
                        "project_version": project_version,
                    })
                    .to_string(),
                ),
                workspace_id,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates")
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_agent_session(
        db: &SqliteDb,
        identity_id: &str,
        profile_id: &str,
        backend_kind: &str,
        scope_tag: &str,
        status: &str,
        connection_status: &str,
    ) -> AgentSession {
        let now = now_rfc3339();
        let scope = AgentContextScopeRepo::create_context_scope(
            db,
            CreateAgentContextScope {
                id: new_uuid_v4(),
                identity_id: identity_id.to_owned(),
                scope_type: "account".to_owned(),
                scope_id: scope_tag.to_owned(),
                project_id: None,
                task_id: None,
                task_role: None,
                workspace_access: "deny".to_owned(),
                workspace_path: None,
                authority_json: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("context scope creates");
        AgentSessionRepo::create_agent_session(
            db,
            CreateAgentSession {
                id: new_uuid_v4(),
                identity_id: identity_id.to_owned(),
                profile_id: profile_id.to_owned(),
                context_scope_id: scope.id,
                backend_kind: backend_kind.to_owned(),
                runtime_session_id: Some(new_uuid_v4()),
                status: status.to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: connection_status.to_owned(),
                predecessor_session_id: None,
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("agent session creates")
    }

    async fn load_session(db: &SqliteDb, id: &str) -> AgentSession {
        AgentSessionRepo::get_agent_session(db, id)
            .await
            .expect("session loads")
            .expect("session exists")
    }

    #[tokio::test]
    async fn crash_recovery_suspends_stale_native_sessions() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let agent = seed_agent(&db, AgentStatus::Idle, None).await;
        let now = now_rfc3339();
        let native_profile = AgentProfileRepo::create_profile(
            &*db,
            CreateAgentProfile {
                id: new_uuid_v4(),
                identity_id: agent.id.clone(),
                backend_kind: "native".to_owned(),
                executor_type: "embedded".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("native profile creates");

        let stale_ready = seed_agent_session(
            &db,
            &agent.id,
            &native_profile.id,
            "native",
            "scope-ready",
            "ready",
            "healthy",
        )
        .await;
        let stale_running = seed_agent_session(
            &db,
            &agent.id,
            &native_profile.id,
            "native",
            "scope-running",
            "running",
            "healthy",
        )
        .await;
        let cancelled = seed_agent_session(
            &db,
            &agent.id,
            &native_profile.id,
            "native",
            "scope-cancelled",
            "cancelled",
            "healthy",
        )
        .await;
        let replaced = seed_agent_session(
            &db,
            &agent.id,
            &native_profile.id,
            "native",
            "scope-replaced",
            "replaced",
            "healthy",
        )
        .await;
        let cli_ready = seed_agent_session(
            &db,
            &agent.id,
            &agent.profile_id,
            "cli",
            "scope-cli",
            "ready",
            "healthy",
        )
        .await;

        CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");

        // Stale native sessions in non-terminal statuses are suspended with
        // an unverified connection, and the version bump preserves the
        // optimistic-concurrency contract.
        let ready_after = load_session(&db, &stale_ready.id).await;
        assert_eq!(ready_after.status, "suspended");
        assert_eq!(ready_after.connection_status, "unknown");
        assert_eq!(ready_after.version, stale_ready.version + 1);
        let running_after = load_session(&db, &stale_running.id).await;
        assert_eq!(running_after.status, "suspended");
        assert_eq!(running_after.connection_status, "unknown");
        assert_eq!(running_after.version, stale_running.version + 1);

        // The suspended session vacates the active-scope slot so the reuse
        // path re-establishes a fresh session on next use.
        assert!(AgentSessionRepo::get_active_agent_session(
            &*db,
            &agent.id,
            &ready_after.context_scope_id
        )
        .await
        .expect("active session query runs")
        .is_none());

        // Terminal native sessions and external-backend sessions are left
        // untouched.
        let cancelled_after = load_session(&db, &cancelled.id).await;
        assert_eq!(cancelled_after.status, "cancelled");
        assert_eq!(cancelled_after.connection_status, "healthy");
        assert_eq!(cancelled_after.version, cancelled.version);
        let replaced_after = load_session(&db, &replaced.id).await;
        assert_eq!(replaced_after.status, "replaced");
        assert_eq!(replaced_after.version, replaced.version);
        let cli_after = load_session(&db, &cli_ready.id).await;
        assert_eq!(cli_after.status, "ready");
        assert_eq!(cli_after.connection_status, "healthy");
        assert_eq!(cli_after.version, cli_ready.version);
    }

    #[tokio::test]
    async fn crash_recovery_ignores_idle_in_progress_tasks() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let in_progress = seed_task(
            &db,
            project_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id),
        )
        .await;
        let todo = seed_task(&db, project_id, "todo".to_owned(), None).await;

        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        let recovered = recovery.run_recovery().await.expect("recovery runs");
        assert_eq!(recovered, 0);

        let recovered_task = TaskRepo::get_by_id(&*db, &in_progress.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(recovered_task.status, "in_progress".to_owned());
        assert_eq!(recovered_task.assignee_id, in_progress.assignee_id);
        assert_eq!(recovered_task.error_annotation, None);

        let unchanged = TaskRepo::get_by_id(&*db, &todo.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(unchanged.error_annotation, None);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn crash_recovery_skips_user_assigned_in_progress_tasks() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let user_task = seed_task_with_assignee(
            &db,
            project_id,
            "in_progress".to_owned(),
            "user",
            Some("human-user".to_owned()),
        )
        .await;

        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        let recovered = recovery.run_recovery().await.expect("recovery runs");
        assert_eq!(recovered, 0);

        let unchanged = TaskRepo::get_by_id(&*db, &user_task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(unchanged.status, "in_progress");
        assert_eq!(unchanged.error_annotation, None);

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn heartbeat_monitor_times_out_busy_agents_and_recovers_tasks() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let old_heartbeat = "1970-01-01T00:00:00+00:00".to_owned();
        let agent = seed_agent(&db, AgentStatus::Busy, Some(old_heartbeat.clone())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let _execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;

        let relay = crate::DomainEventBroadcastConsumer::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            Some(db.domain_event_head().await.unwrap()),
        );
        let monitor = HeartbeatMonitor::with_check_interval(
            Arc::clone(&db),
            event_bus,
            Duration::from_millis(5),
        );
        let timed_out = monitor.check_once().await.expect("monitor runs");
        assert_eq!(timed_out, 1);
        relay.broadcast_once(100).await.unwrap();

        let updated_agent = AgentRepo::get_by_id(&*db, &agent.id)
            .await
            .expect("agent loads")
            .expect("agent exists");
        assert_eq!(updated_agent.status, AgentStatus::Error);

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let recovered_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(recovered_task.status, "in_progress".to_owned());
        assert_eq!(recovered_task.assignee_id, task.assignee_id);
        let annotation: Value =
            serde_json::from_str(recovered_task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "recovery_required");
        assert_eq!(annotation["blocking_reason"], "agent_timeout");
        assert_eq!(annotation["blocked_by"], "system:heartbeat_monitor");
        assert_eq!(
            annotation["message"],
            "Recovered after agent heartbeat timeout"
        );
        assert!(annotation.get("recovery_actions").is_none());

        let mut event_types = Vec::new();
        let mut recovered_event_id = None;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == "task.recovered" {
                recovered_event_id = Some(event.entity_id);
            }
            event_types.push(event.event_type);
        }
        assert!(event_types.iter().any(|event| event == "agent.timeout"));
        assert!(event_types
            .iter()
            .any(|event| event == "domain_event.committed"));
        assert!(event_types.iter().any(|event| event == "task.recovered"));
        assert_eq!(recovered_event_id.as_deref(), Some(task.id.as_str()));
    }

    #[tokio::test]
    async fn heartbeat_monitor_stale_busy_agent_does_not_cancel_healthy_quiet_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(
            &db,
            AgentStatus::Busy,
            Some("1970-01-01T00:00:00+00:00".to_owned()),
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let claimed = match ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: "quiet-owner".to_owned(),
                lease_expires_at: (now + ChronoDuration::minutes(5)).to_rfc3339(),
                hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
                now: now_text.clone(),
            },
        )
        .await
        .expect("execution lease claims")
        {
            db::ExecutionLeaseMutation::Updated(execution) => execution,
            other => panic!("execution lease is not claimed: {other:?}"),
        };
        let renewed = match ExecutionRepo::renew_lease(
            &*db,
            db::RenewExecutionLease {
                execution_id: claimed.id.clone(),
                expected_version: claimed.execution_version,
                owner: "quiet-owner".to_owned(),
                lease_expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
                now: now_text,
            },
        )
        .await
        .expect("execution lease renews")
        {
            db::ExecutionLeaseMutation::Updated(execution) => execution,
            other => panic!("execution lease is not renewed: {other:?}"),
        };

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus));
        let timed_out = monitor.check_once().await.expect("monitor runs");
        assert_eq!(timed_out, 1, "only the legacy agent timeout is observed");

        let updated_agent = AgentRepo::get_by_id(&*db, &agent.id)
            .await
            .expect("agent loads")
            .expect("agent exists");
        assert_eq!(updated_agent.status, AgentStatus::Error);

        let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated_execution.status, ExecutionStatus::Running);
        assert_eq!(
            updated_execution.lease_owner.as_deref(),
            Some("quiet-owner")
        );
        assert_eq!(
            updated_execution.execution_version, renewed.execution_version,
            "the stale agent path did not terminalize or mutate the healthy owner lease"
        );
        assert!(updated_execution
            .lease_expires_at
            .as_deref()
            .is_some_and(|expires_at| rfc3339_is_after(expires_at, &now_rfc3339())));

        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert!(updated_task.error_annotation.is_none());
    }

    #[tokio::test]
    async fn heartbeat_monitor_marks_stalled_executions_and_schedules_retry() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
        ExecutionRepo::update(
            &*db,
            UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("execution activity updates");

        let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution reloads")
            .expect("execution exists");
        let now = now_rfc3339();
        assert!(matches!(
            ExecutionRepo::claim_lease(
                &*db,
                db::ClaimExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution_for_claim.execution_version,
                    owner: "expired-owner".to_owned(),
                    lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                    hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                    now,
                },
            )
            .await
            .expect("execution lease claims"),
            db::ExecutionLeaseMutation::Updated(_)
        ));

        let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        let executor = Arc::new(RecordingCancelExecutor::default());
        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_service(task_service)
            .with_task_executor(executor.clone())
            .with_execution_stall_timeout(Duration::from_secs(1));

        let stalled = monitor.check_once().await.expect("monitor checks");

        assert_eq!(stalled, 1);
        assert_eq!(
            executor
                .cancelled
                .lock()
                .expect("cancel log lock")
                .as_slice(),
            std::slice::from_ref(&execution.id)
        );
        let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated_execution.status, ExecutionStatus::Failed);
        assert_eq!(
            updated_execution.stop_reason,
            Some(StopReason::ExecutionStalled)
        );
        assert_eq!(updated_execution.resume_policy, Some(ResumePolicy::Auto));

        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        let metadata: Value = serde_json::from_str(updated_task.metadata_json.as_deref().unwrap())
            .expect("metadata parses");
        assert_eq!(
            json!(
                db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                    .await
                    .unwrap()
            ),
            1
        );
        assert_eq!(
            metadata["deferred_dispatch"]["reason"],
            "execution retry (attempt 1)"
        );

        let mut event_types = Vec::new();
        for _ in 0..3 {
            event_types.push(rx.recv().await.expect("event receives").event_type);
        }
        assert!(event_types.iter().any(|event| event == "execution.stalled"));
        assert!(event_types
            .iter()
            .any(|event| event == "task.execution_retry"));
    }

    #[tokio::test]
    async fn heartbeat_monitor_routes_expired_reviewer_through_review_retry() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(32));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            default_states::REVIEW.to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let assignment_now = now_rfc3339();
        TaskRoleAssignmentRepo::assign(
            &*db,
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: default_roles::REVIEWER.to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(agent_id.clone()),
                created_at: assignment_now.clone(),
                updated_at: assignment_now,
            },
        )
        .await
        .expect("reviewer role assignment creates");
        sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
            .bind(r#"{"retry_budgets":{"execution":3}}"#)
            .bind("{}")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .expect("retry policy updates");

        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
        sqlx::query("UPDATE execution SET role = ? WHERE id = ?")
            .bind(default_roles::REVIEWER)
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .expect("reviewer role updates");
        let now = now_rfc3339();
        ReviewRepo::create(
            &*db,
            CreateReview {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                execution_id: execution.id.clone(),
                attempt_number: 1,
                status: ReviewStatus::Running,
                step_results_json: json!({ "ci_steps": [] }).to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("running review creates");

        let current = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution reloads")
            .expect("execution exists");
        assert!(matches!(
            ExecutionRepo::claim_lease(
                &*db,
                db::ClaimExecutionLease {
                    execution_id: current.id.clone(),
                    expected_version: current.execution_version,
                    owner: "expired-review-owner".to_owned(),
                    lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                    hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                    now,
                },
            )
            .await
            .expect("execution lease claims"),
            db::ExecutionLeaseMutation::Updated(_)
        ));

        let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        let monitor = HeartbeatMonitor::new(Arc::clone(&db), event_bus)
            .with_task_service(task_service)
            .with_execution_stall_timeout(Duration::from_secs(1));
        assert_eq!(monitor.check_once().await.expect("monitor checks"), 1);

        let reviews = ReviewRepo::list_by_task(&*db, &task.id)
            .await
            .expect("reviews load");
        assert_eq!(reviews[0].status, ReviewStatus::Running);
        assert!(reviews[0].finished_at.is_none());
        let details: Value =
            serde_json::from_str(&reviews[0].step_results_json).expect("review details parse");
        assert_eq!(details["execution_retry"]["execution_id"], execution.id);
        let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task reloads")
            .expect("task exists");
        assert!(current_task.blocked_json.is_none());
        let metadata: Value =
            serde_json::from_str(current_task.metadata_json.as_deref().unwrap_or("{}"))
                .expect("metadata parses");
        assert_eq!(
            json!(
                db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                    .await
                    .unwrap()
            ),
            1
        );
        assert!(metadata.get("deferred_dispatch").is_some());
    }

    #[tokio::test]
    async fn heartbeat_monitor_warns_on_live_stale_progress_without_terminalizing() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(32));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let claimed = match ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: "live-owner".to_owned(),
                lease_expires_at: (now + ChronoDuration::minutes(5)).to_rfc3339(),
                hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
                now: now_text.clone(),
            },
        )
        .await
        .expect("execution lease claims")
        {
            db::ExecutionLeaseMutation::Updated(execution) => execution,
            other => panic!("execution lease is not claimed: {other:?}"),
        };
        sqlx::query("UPDATE execution SET last_progress_at = ? WHERE id = ?")
            .bind("1970-01-01T00:00:00+00:00")
            .bind(&claimed.id)
            .execute(db.pool())
            .await
            .expect("stale progress updates");

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_execution_stall_timeout(Duration::from_secs(1));
        let first = monitor.check_once().await.expect("first monitor pass");
        let second = monitor.check_once().await.expect("second monitor pass");

        assert_eq!(first, 1, "one bounded warning is emitted for the episode");
        assert_eq!(
            second, 0,
            "the warning is deduplicated on subsequent passes"
        );
        let persisted = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(persisted.status, ExecutionStatus::Running);
        let persisted_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert!(persisted_task.error_annotation.is_none());
        assert!(persisted_task.blocked_json.is_none());
        let events = DomainEventRepo::list_events_after(&*db, 0, 100)
            .await
            .expect("domain events load");
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.event_type == "execution.progress_warning" && event.entity_id == task.id
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn heartbeat_monitor_terminal_cas_race_emits_one_terminal_event() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(32));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let claimed = match ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: "race-owner".to_owned(),
                lease_expires_at: (now - ChronoDuration::minutes(1)).to_rfc3339(),
                hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
                now: now_text.clone(),
            },
        )
        .await
        .expect("execution lease claims")
        {
            db::ExecutionLeaseMutation::Updated(execution) => execution,
            other => panic!("execution lease is not claimed: {other:?}"),
        };

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus));
        let db_for_completion = Arc::clone(&db);
        let completion = db::TerminalizeExecution {
            execution_id: claimed.id.clone(),
            expected_version: claimed.execution_version,
            lease_owner: claimed.lease_owner.clone(),
            status: ExecutionStatus::Completed,
            stop_reason: Some(Some(StopReason::LegacyUnknown)),
            stopped_by: Some(Some("runner".to_owned())),
            stopped_at: Some(Some(now_text.clone())),
            resume_policy: Some(Some(ResumePolicy::None)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_text,
            actor_type: "runner".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        };
        let (monitor_result, completion_result) = tokio::join!(monitor.check_once(), async move {
            ExecutionRepo::terminalize(&*db_for_completion, completion).await
        });
        monitor_result.expect("monitor race pass");
        completion_result.expect("completion race pass");

        let persisted = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert!(matches!(
            persisted.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed
        ));
        let events = DomainEventRepo::list_events_after(&*db, 0, 100)
            .await
            .expect("domain events load");
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.event_type.starts_with("execution.")
                        && event.entity_id == task.id
                        && event.payload_json.contains(&execution.id)
                })
                .count(),
            1,
            "the monitor and completion race has one durable terminal event"
        );
    }

    #[tokio::test]
    async fn supervised_heartbeat_recovers_panic_reports_budget_without_cancelling_and_stops() {
        use std::sync::atomic::AtomicUsize;
        struct InFlight(Arc<AtomicBool>);
        impl Drop for InFlight {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let db = Arc::new(sqlite_db().await);
        let workers = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
        let monitor = Arc::new(HeartbeatMonitor::with_check_interval(
            db,
            Arc::new(EventBus::new(16)),
            Duration::from_secs(300),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicBool::new(false));
        let resume = Arc::new(tokio::sync::Notify::new());
        let complete = Arc::new(tokio::sync::Notify::new());
        let handle = Arc::clone(&monitor).start_with_check(&workers, Duration::from_millis(100), {
            let calls = Arc::clone(&calls);
            let in_flight = Arc::clone(&in_flight);
            let resume = Arc::clone(&resume);
            let complete = Arc::clone(&complete);
            move |_| {
                let calls = Arc::clone(&calls);
                let in_flight = Arc::clone(&in_flight);
                let resume = Arc::clone(&resume);
                let complete = Arc::clone(&complete);
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        panic!("heartbeat tick panic");
                    }
                    in_flight.store(true, Ordering::SeqCst);
                    let _in_flight = InFlight(in_flight);
                    resume.notified().await;
                    complete.notify_one();
                    Ok(())
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !workers.status().await.unwrap().iter().any(|row| {
                row.worker_name == "heartbeat-monitor"
                    && row
                        .last_error
                        .as_deref()
                        .is_some_and(|error| error.contains("tick running longer than 100ms"))
            }) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(in_flight.load(Ordering::SeqCst));
        resume.notify_one();
        tokio::time::timeout(Duration::from_secs(5), complete.notified())
            .await
            .unwrap();
        monitor.stop();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
        let row = workers.status().await.unwrap().remove(0);
        assert!(!row.running);
        assert_eq!(row.restart_count, 1);
        assert!(row.last_error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!in_flight.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn heartbeat_monitor_start_stops() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let monitor = Arc::new(HeartbeatMonitor::with_check_interval(
            Arc::clone(&db),
            event_bus,
            Duration::from_millis(1),
        ));

        let handle = Arc::clone(&monitor).start(&crate::worker_runtime::PeriodicWorkers::new(
            Arc::clone(&db),
        ));
        monitor.stop();
        handle.await.expect("monitor task joins");
        assert!(monitor.is_stopped());
    }

    #[tokio::test]
    async fn recovery_monitors_all_active_states() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(
                serde_json::to_string(&crate::workflow::default_workflow::default_workflow())
                    .unwrap(),
            )
            .bind(&project_id)
            .execute(db.pool())
            .await
            .expect("project workflow updates");
        let agent = seed_agent(
            &db,
            AgentStatus::Busy,
            Some("1970-01-01T00:00:00+00:00".to_owned()),
        )
        .await;

        let task_in_progress = seed_task(
            &db,
            project_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let task_merge_failed =
            seed_task(&db, project_id, "merge_failed".to_owned(), Some(agent.id)).await;

        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        let recovered = recovery.run_recovery().await.expect("recovery runs");
        assert_eq!(recovered, 0);

        let t1 = TaskRepo::get_by_id(&*db, &task_in_progress.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(t1.status, "in_progress");

        let t2 = TaskRepo::get_by_id(&*db, &task_merge_failed.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(t2.status, "merge_failed");
    }

    #[tokio::test]
    async fn crash_recovery_auto_redispatches_running_executions_in_any_active_state() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let in_progress_agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let merge_failed_agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let in_progress = seed_task(
            &db,
            project_id.clone(),
            "in_progress".to_owned(),
            Some(in_progress_agent.id.clone()),
        )
        .await;
        let merge_failed = seed_task(
            &db,
            project_id,
            "merge_failed".to_owned(),
            Some(merge_failed_agent.id.clone()),
        )
        .await;
        let in_progress_execution =
            seed_running_execution(&db, in_progress.id.clone(), in_progress_agent.id, None).await;
        let merge_failed_execution =
            seed_running_execution(&db, merge_failed.id.clone(), merge_failed_agent.id, None).await;

        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        let recovered = recovery.run_recovery().await.expect("recovery runs");
        assert_eq!(recovered, 0);

        for (task, execution, expected_status) in [
            (in_progress, in_progress_execution, "in_progress"),
            (merge_failed, merge_failed_execution, "merge_failed"),
        ] {
            for task_id in sqlx::query_scalar::<_, String>(
                "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
            )
            .fetch_all(db.pool())
            .await
            .unwrap()
            {
                crate::test_support::drain_task_steps(&db, &task_id).await;
            }
            let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .expect("task loads")
                .expect("task exists");
            assert_eq!(updated_task.status, expected_status);
            assert!(updated_task.error_annotation.is_none());

            let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .expect("execution loads")
                .expect("execution exists");
            assert_eq!(updated.status, ExecutionStatus::Cancelled);
            assert!(updated.error.as_deref().unwrap().contains("Recovered"));
            assert_eq!(updated.resume_policy, Some(ResumePolicy::Auto));
        }
    }

    #[tokio::test]
    async fn repeated_restart_queues_one_recovery_and_publishes_task_recovered_when_it_settles() {
        use db::TaskStepRepo;
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        // Two restarts before the queue drains: one recovery command for the
        // Task and its dead execution.
        recovery.run_recovery().await.expect("first restart");
        recovery.run_recovery().await.expect("second restart");
        let commands = db
            .task_steps(&task.id)
            .await
            .unwrap()
            .into_iter()
            .filter(|step| {
                step.kind == "command" && step.payload_json.contains("recover_task_after_restart")
            })
            .count();
        assert_eq!(commands, 1);

        let bus = Arc::new(EventBus::new(64));
        let mut rx = bus.subscribe();
        crate::TaskService::new(Arc::clone(&db), Arc::clone(&bus))
            .drain(&task.id)
            .await
            .expect("recovery step settles");
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Cancelled
        );
        let mut recovered = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if event.event_type == "task.recovered" {
                recovered.push(event);
            }
        }
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].entity_id, task.id);
        match &recovered[0].context {
            EventContext::TaskRecovered { reason, .. } => assert_eq!(reason, "crash_recovery"),
            other => panic!("unexpected task.recovered context: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_running_executions_returns_cancelled_execution_metadata() {
        let db = Arc::new(sqlite_db().await);
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(
            &db,
            task.id.clone(),
            agent.id.clone(),
            Some("session-789".to_owned()),
        )
        .await;

        let cancelled = cancel_running_executions(
            &db,
            &task.id,
            StopReason::CrashRecovery,
            &api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
            ResumePolicy::Manual,
        )
        .await
        .expect("cancellation succeeds");

        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].execution_id, execution.id);
        assert_eq!(
            cancelled[0].agent_session_id.as_deref(),
            Some("session-789")
        );
    }

    #[tokio::test]
    async fn recover_task_cas_loss_does_not_revoke_successor_workspace_lease() {
        let db = Arc::new(sqlite_db().await);
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;

        let workspace_id = new_uuid_v4();
        WorkspaceRepo::create(
            &*db,
            CreateWorkspace {
                id: workspace_id.clone(),
                task_id: task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: format!("/tmp/forge-recovery-{workspace_id}"),
                branch: ::workspace::task_branch_name(&task.id),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("successor workspace creates");

        // Leave an active lease for a newer successor attempt. The successor
        // is made terminal through a direct fixture update so the stale
        // recovery calls below only race on the older running attempt; the
        // lease remains the protected task-scoped grant.
        let successor = seed_running_execution_in_workspace(
            &db,
            task.id.clone(),
            agent.id.clone(),
            None,
            Some(workspace_id.clone()),
        )
        .await;
        let now = Utc::now();
        let successor_lease = WorkspaceLeaseRepo::issue(
            &*db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id,
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: successor.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent.id.clone(),
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: (now - ChronoDuration::minutes(1)).to_rfc3339(),
                expires_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .expect("successor lease issues");
        sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(&successor.id)
            .execute(db.pool())
            .await
            .expect("successor fixture terminalizes");

        let stale_execution = seed_running_execution_in_workspace(
            &db,
            task.id.clone(),
            agent.id.clone(),
            None,
            Some(workspace_id),
        )
        .await;
        let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("stale task loads")
            .expect("stale task exists");
        let stopped_by = api_types::Actor::system(api_types::SystemComponent::CrashRecovery);
        let stale_task_for_race = stale_task.clone();
        let (first, second) = tokio::join!(
            recover_task(&db, stale_task, StopReason::CrashRecovery, &stopped_by),
            recover_task(
                &db,
                stale_task_for_race,
                StopReason::CrashRecovery,
                &stopped_by,
            ),
        );
        let first = first.expect("first recovery race call");
        let second = second.expect("second recovery race call");
        // An interrupted implementation execution is now left for automatic
        // redispatch, so neither caller annotates the Task. The race invariant
        // this test exists for is the one below: the loser must not touch the
        // successor's lease.
        assert!(
            !first.annotated && !second.annotated,
            "an auto-resumable recovery does not park the Task behind an annotation"
        );

        let cancelled = ExecutionRepo::get_by_id(&*db, &stale_execution.id)
            .await
            .expect("raced execution loads")
            .expect("raced execution exists");
        assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
        let persisted_lease = WorkspaceLeaseRepo::get_by_id(&*db, &successor_lease.id)
            .await
            .expect("successor lease loads")
            .expect("successor lease exists");
        assert_eq!(persisted_lease.status, "active");
        assert_eq!(
            persisted_lease.version, successor_lease.version,
            "a recovery caller that lost the terminal CAS is inert for successor leases"
        );
    }

    #[tokio::test]
    async fn crash_recovery_keeps_active_task_with_resumable_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(1024));
        let _task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(
            &db,
            task.id.clone(),
            agent.id,
            Some("session-123".to_owned()),
        )
        .await;

        CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated_task.status, "in_progress");

        let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated_execution.status, ExecutionStatus::Cancelled);
        assert_eq!(
            updated_execution.agent_session_id.as_deref(),
            Some("session-123")
        );
        assert_eq!(updated_execution.resume_policy, Some(ResumePolicy::Auto));
    }

    #[tokio::test]
    async fn crash_recovery_does_not_follow_up_resumable_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(1024));
        let _task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(
            &db,
            task.id.clone(),
            agent.id,
            Some("session-456".to_owned()),
        )
        .await;

        CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated_task.status, "in_progress");

        let executions = ExecutionRepo::list_by_task(
            &*db,
            &task.id,
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await
        .expect("executions load");
        assert_eq!(executions.items.len(), 1);
        assert_eq!(executions.items[0].id, execution.id);
        assert_eq!(executions.items[0].status, ExecutionStatus::Cancelled);
    }

    async fn stamp_recovery_annotation(
        db: &SqliteDb,
        task: &Task,
        blocked_execution_id: Option<&str>,
    ) -> Task {
        let annotation = json!({
            "type": api_types::FailureKind::RecoveryRequired,
            "blocking_reason": "crash_recovery",
            "blocked_by": "system:crash_recovery",
            "blocked_at": now_rfc3339(),
            "blocked_execution_id": blocked_execution_id,
            "artifact": blocked_execution_id.map(|execution_id| json!({
                "kind": "execution",
                "id": execution_id,
                "log_path": null,
            })),
            "message": "Recovered after server restart",
        })
        .to_string();
        TaskRepo::update_status(
            db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: None,
                failed_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("annotation stamps")
    }

    #[tokio::test]
    async fn crash_recovery_clears_stale_recovery_annotations_without_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, project_id, "in_progress".to_owned(), None).await;
        stamp_recovery_annotation(&db, &task, None).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 1);

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated.error_annotation, None);
    }

    #[tokio::test]
    async fn crash_recovery_preserves_legitimate_pending_recovery_annotations() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
        let terminalized_at = now_rfc3339();
        ExecutionRepo::terminalize(
            &*db,
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Cancelled,
                stop_reason: Some(Some(StopReason::CrashRecovery)),
                stopped_by: Some(Some("system:crash_recovery".to_owned())),
                resume_policy: Some(Some(ResumePolicy::Manual)),
                stopped_at: Some(Some(terminalized_at.clone())),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: Some(Some("Recovered".to_owned())),
                executor_config_snapshot_json: None,
                updated_at: terminalized_at,
                actor_type: "system".to_owned(),
                actor_id: None,
                correlation_id: None,
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Revoke,
            },
        )
        .await
        .expect("execution terminalizes");
        let annotated = stamp_recovery_annotation(&db, &task, Some(&execution.id)).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 0);

        let updated = TaskRepo::get_by_id(&*db, &annotated.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(
            updated.error_annotation.as_deref(),
            annotated.error_annotation.as_deref()
        );
        let annotation: Value =
            serde_json::from_str(updated.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocked_execution_id"], execution.id);
    }

    #[tokio::test]
    async fn crash_recovery_clears_stale_recovery_annotations_for_missing_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, project_id, "in_progress".to_owned(), None).await;
        stamp_recovery_annotation(&db, &task, Some("missing-execution-id")).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 1);

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated.error_annotation, None);
    }

    async fn seed_agent_with_daemon(
        db: &SqliteDb,
        machine_id: &str,
        status: AgentStatus,
    ) -> (String, Agent) {
        let now = now_rfc3339();
        let daemon_id = new_uuid_v4();
        DaemonRepo::upsert_by_machine_id(
            db,
            UpsertDaemon {
                max_concurrent_runs: None,
                id: daemon_id.clone(),
                machine_id: machine_id.to_owned(),
                hostname: "test-host".to_owned(),
                os: "linux".to_owned(),
                arch: "x86_64".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon creates");

        let agent = AgentRepo::create(
            db,
            CreateAgent {
                id: new_uuid_v4(),
                name: "remote".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: Some(daemon_id),
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("agent creates");
        (agent.id.clone(), agent)
    }

    async fn seed_running_execution_with_created_at(
        db: &SqliteDb,
        task_id: String,
        agent_id: String,
        created_at: &str,
    ) -> db::Execution {
        let execution = seed_running_execution(db, task_id, agent_id, None).await;
        ExecutionRepo::update(
            db,
            UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: created_at.to_owned(),
            },
        )
        .await
        .expect("execution timestamp updates");
        sqlx::query("UPDATE execution SET created_at = ? WHERE id = ?")
            .bind(created_at)
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .expect("execution created_at updates");
        ExecutionRepo::get_by_id(db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists")
    }

    #[tokio::test]
    async fn heartbeat_monitor_skips_embedded_daemon_executions_for_disconnect_check() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_daemon_connections(Arc::clone(&registry));

        monitor.check_once().await.expect("first check");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let interrupted = monitor.check_once().await.expect("second check");
        assert_eq!(interrupted, 0);

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Running);
    }

    #[tokio::test]
    async fn heartbeat_monitor_does_not_cancel_remote_stalled_executions_via_embedded_executor() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) =
            seed_agent_with_daemon(&db, "remote-machine-d", AgentStatus::Idle).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
        ExecutionRepo::update(
            &*db,
            UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("execution activity updates");

        let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution reloads")
            .expect("execution exists");
        let now = now_rfc3339();
        assert!(matches!(
            ExecutionRepo::claim_lease(
                &*db,
                db::ClaimExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution_for_claim.execution_version,
                    owner: "expired-owner".to_owned(),
                    lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                    hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                    now,
                },
            )
            .await
            .expect("execution lease claims"),
            db::ExecutionLeaseMutation::Updated(_)
        ));

        let executor = Arc::new(RecordingCancelExecutor::default());
        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_executor(executor.clone())
            .with_execution_stall_timeout(Duration::from_secs(1));

        let stalled = monitor.check_once().await.expect("monitor checks");
        assert_eq!(stalled, 1);
        assert!(executor
            .cancelled
            .lock()
            .expect("cancel log lock")
            .is_empty());
    }

    #[tokio::test]
    async fn heartbeat_monitor_cancels_stalled_executions_of_daemonless_agents() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let now = now_rfc3339();
        let agent = AgentRepo::create(
            &*db,
            CreateAgent {
                id: new_uuid_v4(),
                name: "embedded-shell".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 1,
                max_missed_heartbeats: 1,
                status: AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("daemonless agent creates");
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
        ExecutionRepo::update(
            &*db,
            UpdateExecution {
                id: execution.id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("execution activity updates");

        let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution reloads")
            .expect("execution exists");
        let now = now_rfc3339();
        assert!(matches!(
            ExecutionRepo::claim_lease(
                &*db,
                db::ClaimExecutionLease {
                    execution_id: execution.id.clone(),
                    expected_version: execution_for_claim.execution_version,
                    owner: "expired-owner".to_owned(),
                    lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                    hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                    now,
                },
            )
            .await
            .expect("execution lease claims"),
            db::ExecutionLeaseMutation::Updated(_)
        ));

        let executor = Arc::new(RecordingCancelExecutor::default());
        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_executor(executor.clone())
            .with_execution_stall_timeout(Duration::from_secs(1));

        let stalled = monitor.check_once().await.expect("monitor checks");
        assert_eq!(stalled, 1);
        assert_eq!(
            executor
                .cancelled
                .lock()
                .expect("cancel log lock")
                .as_slice(),
            std::slice::from_ref(&execution.id)
        );
    }

    #[tokio::test]
    async fn daemon_report_reconcile_interrupts_missing_old_running_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        let service = DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_task_service(task_service);
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-report", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution_with_created_at(
            &db,
            task.id.clone(),
            agent_id,
            "1970-01-01T00:00:00+00:00",
        )
        .await;

        service
            .ingest_report(
                &daemon_id,
                DaemonReportInput {
                    max_concurrent_runs: None,
                    detected_clis: vec![DetectedCliInput {
                        kind: "shell".to_owned(),
                        availability: "authenticated".to_owned(),
                        config_path: None,
                        version: None,
                        path: None,
                    }],
                    runtimes: Vec::new(),
                    labels: None,
                    active_execution_ids: Some(Vec::new()),
                },
            )
            .await
            .expect("report ingests");

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Failed);
        assert_eq!(updated.stop_reason, Some(StopReason::DaemonDisconnected));
    }

    #[tokio::test]
    async fn daemon_report_reconcile_leaves_fresh_running_execution_untouched() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let service = DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-fresh", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        service
            .ingest_report(
                &daemon_id,
                DaemonReportInput {
                    max_concurrent_runs: None,
                    detected_clis: vec![DetectedCliInput {
                        kind: "shell".to_owned(),
                        availability: "authenticated".to_owned(),
                        config_path: None,
                        version: None,
                        path: None,
                    }],
                    runtimes: Vec::new(),
                    labels: None,
                    active_execution_ids: Some(Vec::new()),
                },
            )
            .await
            .expect("report ingests");

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Running);
    }

    #[tokio::test]
    async fn daemon_report_without_active_execution_ids_does_not_reconcile() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let service = DaemonService::new(Arc::clone(&db), Arc::clone(&event_bus));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-none", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution_with_created_at(
            &db,
            task.id.clone(),
            agent_id,
            "1970-01-01T00:00:00+00:00",
        )
        .await;

        service
            .ingest_report(
                &daemon_id,
                DaemonReportInput {
                    max_concurrent_runs: None,
                    detected_clis: vec![DetectedCliInput {
                        kind: "shell".to_owned(),
                        availability: "authenticated".to_owned(),
                        config_path: None,
                        version: None,
                        path: None,
                    }],
                    runtimes: Vec::new(),
                    labels: None,
                    active_execution_ids: None,
                },
            )
            .await
            .expect("report ingests");

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Running);
    }

    #[tokio::test]
    async fn crash_recovery_clears_stale_recovery_annotations_for_completed_execution() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, _repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
        let terminalized_at = now_rfc3339();
        ExecutionRepo::terminalize(
            &*db,
            TerminalizeExecution {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                lease_owner: execution.lease_owner.clone(),
                status: ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: Some(Some(terminalized_at.clone())),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: terminalized_at,
                actor_type: "system".to_owned(),
                actor_id: None,
                correlation_id: None,
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Revoke,
            },
        )
        .await
        .expect("execution terminalizes");
        stamp_recovery_annotation(&db, &task, Some(&execution.id)).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 1);

        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated.error_annotation, None);
    }

    fn dispatch_marker_execution(
        lease_owner: Option<&str>,
        created_at: &str,
        hard_deadline_at: Option<&str>,
    ) -> Execution {
        Execution {
            id: "execution-1".to_owned(),
            task_id: "task-1".to_owned(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            prompt: None,
            last_activity_at: None,
            execution_version: 1,
            lease_owner: lease_owner.map(str::to_owned),
            lease_expires_at: Some(created_at.to_owned()),
            hard_deadline_at: hard_deadline_at.map(str::to_owned),
            last_heartbeat_at: None,
            last_progress_at: None,
        }
    }

    #[test]
    fn a_fresh_dispatch_marker_is_not_treated_as_a_dead_owner() {
        // The reviewer execution that regressed was killed 8ms after creation.
        let execution = dispatch_marker_execution(
            Some("dispatch-pending:execution-1"),
            "2026-09-03T08:33:22.584249+00:00",
            Some("2026-09-03T09:03:22.584249+00:00"),
        );
        assert!(is_unclaimed_dispatch_marker(
            &execution,
            "2026-09-03T08:33:22.592175+00:00"
        ));
    }

    #[test]
    fn a_stale_dispatch_marker_is_reclaimed_once_the_grace_elapses() {
        let execution = dispatch_marker_execution(
            Some("dispatch-pending:execution-1"),
            "2026-09-03T08:33:22.584249+00:00",
            Some("2026-09-03T09:03:22.584249+00:00"),
        );
        assert!(!is_unclaimed_dispatch_marker(
            &execution,
            "2026-09-03T08:34:22.584249+00:00"
        ));
    }

    #[test]
    fn a_real_owner_lease_is_never_given_the_dispatch_grace() {
        let execution = dispatch_marker_execution(
            Some("daemon-7"),
            "2026-09-03T08:33:22.584249+00:00",
            Some("2026-09-03T09:03:22.584249+00:00"),
        );
        assert!(!is_unclaimed_dispatch_marker(
            &execution,
            "2026-09-03T08:33:22.592175+00:00"
        ));
    }

    #[test]
    fn a_dispatch_marker_past_its_hard_deadline_is_still_eligible() {
        let execution = dispatch_marker_execution(
            Some("dispatch-pending:execution-1"),
            "2026-09-03T08:33:22.584249+00:00",
            Some("2026-09-03T08:33:22.584249+00:00"),
        );
        assert!(!is_unclaimed_dispatch_marker(
            &execution,
            "2026-09-03T08:33:22.592175+00:00"
        ));
    }

    #[tokio::test]
    async fn execution_daemon_resolution_follows_placement_instead_of_agent_pin() {
        let db = sqlite_db().await;
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (_, placed_agent) =
            seed_agent_with_daemon(&db, "placement-owner", AgentStatus::Idle).await;
        let (agent_id, _) =
            seed_agent_with_daemon(&db, "different-agent-pin", AgentStatus::Idle).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let now = now_rfc3339();
        let workspace = db::WorkspaceRepo::create(
            &db,
            db::CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: "server-owned-handle".to_owned(),
                branch: "task/placed".to_owned(),
                status: db::WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        let location = db::RepoLocationRepo::create(
            &db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id,
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: "verified-shared-checkout".to_owned(),
                kind: db::RepoLocationKind::SharedMount,
                is_default: true,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("location creates");
        let owner_id = placed_agent.daemon_id.expect("owner daemon exists");
        db::WorkspacePlacementRepo::create(
            &db,
            db::CreateWorkspacePlacement {
                id: new_uuid_v4(),
                workspace_id: workspace.id.clone(),
                task_id: task.id.clone(),
                agent_id: Some(agent_id.clone()),
                owner_kind: db::PlacementOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: location.id,
                execution_daemon_id: Some(owner_id.clone()),
                workspace_handle: Some("server-owned-handle".to_owned()),
                generation: 1,
                state: db::PlacementState::Ready,
                selected_by: db::PlacementSelectedBy::Scheduler,
                selection_reason: "{}".to_owned(),
                reserved_until: None,
                disconnected_at: None,
                failure_cause: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("placement creates");
        let mut execution = seed_running_execution(&db, task.id, agent_id, None).await;
        execution.workspace_id = Some(workspace.id);
        let (resolved_id, _) = resolve_execution_daemon(&db, &execution)
            .await
            .expect("daemon resolves")
            .expect("placed daemon exists");
        assert_eq!(resolved_id, owner_id);
        assert!(execution_is_remote_owned(&db, &execution)
            .await
            .expect("owner classifies"));
    }
    pub(crate) async fn daemon_owned_fixture(
        db: &SqliteDb,
    ) -> (Task, WorkspacePlacement, Execution) {
        let (project_id, repo_id) = seed_project_repo(db).await;
        let agent = seed_agent(db, AgentStatus::Idle, Some(now_rfc3339())).await;
        let daemon_id = agent.daemon_id.clone().unwrap();
        let task = seed_task(
            db,
            project_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let now = Utc::now();
        let runtime = db::RuntimeRepo::create(
            db,
            db::CreateRuntime {
                id: new_uuid_v4(),
                daemon_id: daemon_id.clone(),
                kind: "local".to_owned(),
                workspace_root: "/owner-only".to_owned(),
                status: db::RuntimeStatus::Ready,
                labels_json: "{}".to_owned(),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .unwrap();
        let workspace = db::WorkspaceRepo::create(
            db,
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: "opaque-owner-handle".to_owned(),
                branch: "task/remote".to_owned(),
                status: db::WorkspaceStatus::Ready,
                before_sha: Some("base-head".to_owned()),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .unwrap();
        let location = db::RepoLocationRepo::create(
            db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id,
                owner_kind: db::RepoLocationOwnerKind::Daemon,
                daemon_id: Some(daemon_id.clone()),
                runtime_id: Some(runtime.id.clone()),
                path: "/owner-only/repo".to_owned(),
                kind: db::RepoLocationKind::PrimaryCheckout,
                is_default: true,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now.to_rfc3339()),
                last_error: None,
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .unwrap();
        let placement = WorkspacePlacementRepo::create(
            db,
            db::CreateWorkspacePlacement {
                id: new_uuid_v4(),
                workspace_id: workspace.id.clone(),
                task_id: task.id.clone(),
                agent_id: Some(agent.id.clone()),
                owner_kind: PlacementOwnerKind::Daemon,
                daemon_id: Some(daemon_id.clone()),
                runtime_id: Some(runtime.id),
                repo_location_id: location.id,
                execution_daemon_id: Some(daemon_id.clone()),
                workspace_handle: Some("opaque-owner-handle".to_owned()),
                generation: 1,
                state: PlacementState::Disconnected,
                selected_by: db::PlacementSelectedBy::Scheduler,
                selection_reason: "{}".to_owned(),
                reserved_until: None,
                disconnected_at: Some((now - ChronoDuration::minutes(10)).to_rfc3339()),
                failure_cause: Some(PlacementFailureCause::OwnerDisconnected),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .unwrap();
        let execution = seed_running_execution_in_workspace(
            db,
            task.id.clone(),
            agent.id,
            None,
            Some(workspace.id),
        )
        .await;
        let mutation = ExecutionRepo::claim_lease(
            db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: crate::daemon_transport::execution_lease_owner(&daemon_id, 1),
                lease_expires_at: (now - ChronoDuration::minutes(9)).to_rfc3339(),
                hard_deadline_at: Some((now + ChronoDuration::hours(2)).to_rfc3339()),
                now: (now - ChronoDuration::minutes(10)).to_rfc3339(),
            },
        )
        .await
        .unwrap();
        let db::ExecutionLeaseMutation::Updated(execution) = mutation else {
            panic!("lease fixture claims");
        };
        (task, placement, execution)
    }

    pub(crate) fn owner_connection(
        registry: &DaemonConnectionRegistry,
        daemon_id: &str,
        resume: bool,
    ) -> (u64, tokio::sync::mpsc::Receiver<api_types::DaemonFrame>) {
        let (connection, outbound) =
            crate::daemon_transport::DaemonConnection::new(daemon_id.to_owned());
        let id = connection.id();
        registry.register(daemon_id.to_owned(), connection);
        registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({"protocol_revision": api_types::DAEMON_PROTOCOL_REVISION,
                "capabilities": [api_types::DAEMON_CAPABILITY_USAGE_REPORTS, api_types::DAEMON_CAPABILITY_JOURNAL_ACK, "workspace.v1", api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT],
                "executor_capabilities": {"shell": {"resume": resume}}, "workspace_run_policy": {"allowed_purposes": ["ci_step"]}}),
        });
        (id, outbound)
    }

    async fn expired_owner_grant(
        db: &SqliteDb,
        task: &Task,
        placement: &WorkspacePlacement,
        execution: &Execution,
    ) -> db::WorkspaceLease {
        let workspace = WorkspaceRepo::get_by_id(db, &placement.workspace_id)
            .await
            .unwrap()
            .unwrap();
        let now = Utc::now();
        WorkspaceLeaseRepo::issue(
            db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: execution.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: workspace.repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: execution.agent_id.clone().unwrap(),
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
                expires_at: (now - ChronoDuration::minutes(9)).to_rfc3339(),
                created_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
                updated_at: (now - ChronoDuration::minutes(10)).to_rfc3339(),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn heartbeat_skips_agent_timeout_when_owner_suspension_fails() {
        let db = Arc::new(sqlite_db().await);
        let agent = seed_agent(
            &db,
            AgentStatus::Busy,
            Some("1970-01-01T00:00:00+00:00".to_owned()),
        )
        .await;
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(16)));

        assert_eq!(monitor.check_agent_timeouts_for_tick(false).await, 0);
        assert_eq!(
            AgentRepo::get_by_id(&*db, &agent.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            AgentStatus::Busy
        );
    }

    #[tokio::test]
    async fn heartbeat_skips_workspace_lease_expiry_when_renewal_fails() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(16));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
        let monitor = HeartbeatMonitor::new(db.clone(), bus);

        assert_eq!(monitor.expire_workspace_leases_for_tick(false).await, 0);
        assert_eq!(
            WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "active"
        );
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Running
        );
    }

    #[tokio::test]
    async fn worker_robustness_heartbeat_runs_lease_expiry_after_early_failure() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let ready = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        let now = Utc::now();
        let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: execution.lease_owner.clone().unwrap(),
                lease_expires_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
                hard_deadline_at: execution.hard_deadline_at.clone(),
                now: now.to_rfc3339(),
            },
        )
        .await
        .unwrap() else {
            panic!("execution lease updates");
        };
        let grant = expired_owner_grant(&db, &task, &ready, &execution).await;
        sqlx::query(
            "UPDATE agent_identity
             SET status = 'busy', heartbeat_interval_seconds = 'invalid'
             WHERE id = ?",
        )
        .bind(execution.agent_id.as_deref().unwrap())
        .execute(db.pool())
        .await
        .unwrap();

        HeartbeatMonitor::new(db.clone(), bus)
            .check_once()
            .await
            .expect("later heartbeat passes still run");

        assert_eq!(
            WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "expired"
        );
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Failed
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_lease_expiry_before_socket_disconnect_reconciles_terminal_once()
    {
        assert_owner_disconnect_order(false).await;
    }

    #[tokio::test]
    async fn daemon_owned_workspace_socket_disconnect_before_lease_expiry_reconciles_terminal_once()
    {
        assert_owner_disconnect_order(true).await;
    }

    async fn assert_owner_disconnect_order(socket_first: bool) {
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let mut events = bus.subscribe();
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let sink = Arc::new(ServerExecutionEventSink::new(
            db.clone(),
            bus.clone(),
            root.path().to_path_buf(),
        ));
        let registry = Arc::new(DaemonConnectionRegistry::new(bus.clone(), sink.clone()));
        sink.set_connection_registry(Arc::downgrade(&registry));
        let service = Arc::new(
            TaskService::new(db.clone(), bus.clone()).with_daemon_connections(registry.clone()),
        );
        sink.set_task_service(Arc::downgrade(&service));
        let daemon_id = ready.daemon_id.as_deref().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, daemon_id, false);
        let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: crate::daemon_transport::execution_lease_owner(daemon_id, connection_id),
                lease_expires_at: execution.lease_expires_at.clone().unwrap(),
                hard_deadline_at: execution.hard_deadline_at.clone(),
                now: now_rfc3339(),
            },
        )
        .await
        .unwrap() else {
            panic!("expired owner claims");
        };
        let monitor = HeartbeatMonitor::new(db.clone(), bus.clone())
            .with_daemon_connections(registry.clone());
        if socket_first {
            sink.handle_disconnected(daemon_id).await.unwrap();
        }
        // No close frame or responder exists. The registered socket is still
        // connected; stale heartbeats alone must suspend the owner.
        assert!(registry.is_connected(daemon_id));
        tokio::time::timeout(Duration::from_secs(5), monitor.check_once())
            .await
            .unwrap()
            .unwrap();
        let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected.state, PlacementState::Disconnected);
        assert_eq!(
            disconnected.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnected)
        );
        assert!(disconnected.disconnected_at.is_some());
        assert_eq!(disconnected.version, ready.version + 1);
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
        assert_eq!(
            WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
                .await
                .unwrap()
                .unwrap(),
            grant
        );
        assert_eq!(
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            task
        );
        sink.handle_disconnected(daemon_id).await.unwrap();
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap(),
            disconnected
        );
        let mut disconnect_events = 0;
        while let Ok(event) = events.try_recv() {
            disconnect_events += u64::from(event.event_type == "workspace.disconnected");
            assert_ne!(event.event_type, "execution.stalled");
        }
        assert_eq!(disconnect_events, 1);

        let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
            "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
            "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
        })).unwrap();
        // This also covers an owner thawing on the same still-open socket.
        for _ in 0..2 {
            assert_eq!(
                sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
                    .await
                    .unwrap(),
                DaemonTerminalDisposition::AwaitingCascade
            );
        }
        let responder = {
            let registry = registry.clone();
            let daemon_id = daemon_id.to_owned();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("describe request");
                };
                assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
            })
        };
        assert!(
            reconcile_workspace_placement(&db, &bus, &registry, None, &disconnected)
                .await
                .unwrap()
        );
        responder.await.unwrap();
        assert_eq!(
            sink.handle_terminal_with_ack(daemon_id, connection_id, notification)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Acknowledge
        );
        let finished = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(finished.status, ExecutionStatus::Completed);
        assert_eq!(finished.after_sha.as_deref(), Some("owner-head"));
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Ready
        );
        let terminal_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM domain_event WHERE event_type = 'execution.completed'
             AND json_extract(payload_json, '$.execution_id') = ?",
        )
        .bind(&execution.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(terminal_events, 1);
        let executions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
                .bind(&ready.workspace_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(executions, 1);
    }

    #[tokio::test]
    async fn revision_two_ready_placement_disconnects_and_expires_despite_rest_online() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::default());
        let (_, placement, _) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = ready.daemon_id.as_deref().unwrap();
        let (connection, _outbound) =
            crate::daemon_transport::DaemonConnection::new(daemon_id.into());
        let id = connection.id();
        registry.register(daemon_id.into(), connection);
        registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
        });
        let monitor = HeartbeatMonitor::new(db.clone(), bus)
            .with_daemon_connections(registry)
            .with_max_disconnect(Duration::from_secs(86400));
        monitor.check_once().await.unwrap();
        let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected.state, PlacementState::Disconnected);
        assert!(disconnected.disconnected_at.is_some());
        let mut expired = placement_update(
            &disconnected,
            PlacementState::Disconnected,
            Some(PlacementFailureCause::OwnerDisconnected),
        );
        expired.disconnected_at = Some(Some((Utc::now() - ChronoDuration::hours(25)).to_rfc3339()));
        WorkspacePlacementRepo::update(&*db, expired).await.unwrap();
        monitor.check_once().await.unwrap();
        let failed = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.state, PlacementState::Failed);
        assert_eq!(
            failed.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnectedTimeout)
        );
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_daemon_connections(monitor.daemon_connections.as_ref().unwrap().clone());
        let error = service
            .test_apply_action(
                ready.task_id,
                api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, ServiceError::DaemonUpgradeRequired { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_lease_expiry_cas_loses_to_heartbeat_and_socket_disconnect() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let now = Utc::now();
        let claimed = ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: execution.lease_owner.clone().unwrap(),
                lease_expires_at: (now + ChronoDuration::minutes(1)).to_rfc3339(),
                hard_deadline_at: execution.hard_deadline_at.clone(),
                now: now.to_rfc3339(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(claimed, db::ExecutionLeaseMutation::Updated(_)));
        assert!(WorkspacePlacementRepo::suspend_expired_execution_lease(
            &*db,
            &ready,
            &execution,
            &now.to_rfc3339()
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap(),
            ready
        );
        assert_eq!(
            disconnect_daemon_placements(&db, &bus, ready.daemon_id.as_deref().unwrap())
                .await
                .unwrap(),
            1
        );
        assert!(WorkspacePlacementRepo::suspend_expired_execution_lease(
            &*db,
            &ready,
            &execution,
            &now.to_rfc3339()
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap()
                .version,
            ready.version + 1
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_lease_expiry_terminal_admission_suspends_before_monitor() {
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let sink =
            ServerExecutionEventSink::new(db.clone(), bus.clone(), root.path().to_path_buf());
        let service = Arc::new(TaskService::new(db.clone(), bus));
        sink.set_task_service(Arc::downgrade(&service));
        let notification = serde_json::from_value(json!({
            "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
            "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
        })).unwrap();
        assert_eq!(
            sink.handle_terminal_with_ack(ready.daemon_id.as_deref().unwrap(), 1, notification)
                .await
                .unwrap(),
            DaemonTerminalDisposition::AwaitingCascade
        );
        let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected.state, PlacementState::Disconnected);
        assert_eq!(
            disconnected.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnected)
        );
        assert_eq!(disconnected.version, ready.version + 1);
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Completed
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_lease_expiry_heartbeat_thaw_resumes_suspended_lease() {
        use crate::daemon_transport::{DaemonExecutionEventHandler, ServerExecutionEventSink};
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let sink = ServerExecutionEventSink::new(db.clone(), bus, root.path().to_path_buf());
        sink.handle_heartbeat(ready.daemon_id.as_deref().unwrap(), 1, 1)
            .await
            .unwrap();
        let current = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, ExecutionStatus::Running);
        assert_eq!(current.hard_deadline_at, execution.hard_deadline_at);
        assert!(rfc3339_is_after(
            current.lease_expires_at.as_deref().unwrap(),
            &now_rfc3339()
        ));
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Disconnected
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_lease_expiry_races_socket_disconnect_once() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let mut events = bus.subscribe();
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let (expired, socket) = tokio::join!(
            suspend_expired_remote_execution(&db, &bus, &execution),
            disconnect_daemon_placements(&db, &bus, ready.daemon_id.as_deref().unwrap()),
        );
        assert!(expired.unwrap());
        assert!(socket.unwrap() <= 1);
        let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected.version, ready.version + 1);
        assert_eq!(disconnected.state, PlacementState::Disconnected);
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
        let mut disconnects = 0;
        while let Ok(event) = events.try_recv() {
            disconnects += u64::from(event.event_type == "workspace.disconnected");
        }
        assert_eq!(disconnects, 1);
    }

    #[tokio::test]
    async fn server_workspace_remote_provider_lease_expiry_suspends_execution() {
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
            .await
            .unwrap()
            .unwrap();
        let shared = db::RepoLocationRepo::create(
            &*db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id: location.repo_id,
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: placement.execution_daemon_id.clone(),
                runtime_id: location.runtime_id,
                path: "/server/worktree".to_owned(),
                kind: db::RepoLocationKind::SharedMount,
                is_default: true,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now_rfc3339()),
                last_error: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.owner_kind = Some(PlacementOwnerKind::Server);
        update.daemon_id = Some(None);
        update.runtime_id = Some(None);
        update.repo_location_id = Some(shared.id);
        update.workspace_handle = Some(Some("/server/worktree".to_owned()));
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let monitor = HeartbeatMonitor::new(db.clone(), bus.clone());
        monitor.check_once().await.unwrap();
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
        let suspended = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(suspended.state, PlacementState::Disconnected);
        assert_eq!(
            suspended.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnected)
        );
        let root = tempfile::tempdir().unwrap();
        let sink =
            ServerExecutionEventSink::new(db.clone(), bus.clone(), root.path().to_path_buf());
        let service = Arc::new(TaskService::new(db.clone(), bus));
        sink.set_task_service(Arc::downgrade(&service));
        assert_eq!(sink.handle_terminal_with_ack(ready.execution_daemon_id.as_deref().unwrap(), 1,
            serde_json::from_value(json!({
                "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
                "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
            })).unwrap()).await.unwrap(), DaemonTerminalDisposition::AwaitingCascade);
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Completed
        );
    }

    #[tokio::test]
    async fn server_workspace_embedded_lease_expiry_still_fails_execution() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
            .await
            .unwrap()
            .unwrap();
        let server_location = db::RepoLocationRepo::create(
            &*db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id: location.repo_id,
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: "/server/repo".to_owned(),
                kind: db::RepoLocationKind::PrimaryCheckout,
                is_default: true,
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now_rfc3339()),
                last_error: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.owner_kind = Some(PlacementOwnerKind::Server);
        update.daemon_id = Some(None);
        update.runtime_id = Some(None);
        update.repo_location_id = Some(server_location.id);
        update.execution_daemon_id = Some(None);
        update.disconnected_at = Some(None);
        let ready = WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: "embedded:test".to_owned(),
                lease_expires_at: execution.lease_expires_at.clone().unwrap(),
                hard_deadline_at: execution.hard_deadline_at.clone(),
                now: now_rfc3339(),
            },
        )
        .await
        .unwrap() else {
            panic!("embedded owner claims");
        };
        let executor = Arc::new(RecordingCancelExecutor::default());
        let monitor = HeartbeatMonitor::new(db.clone(), bus).with_task_executor(executor.clone());
        assert_eq!(monitor.check_once().await.unwrap(), 1);
        let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert_eq!(failed.stop_reason, Some(StopReason::ExecutionStalled));
        assert!(failed
            .error
            .as_deref()
            .unwrap()
            .starts_with("Execution owner lease expired at "));
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                .await
                .unwrap()
                .unwrap(),
            ready
        );
        assert_eq!(*executor.cancelled.lock().unwrap(), vec![execution.id]);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_disconnect_freezes_expired_lease_without_duplicate_execution() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        // The release's default has no hard deadline. Disconnect suspension
        // still applies; max_disconnect independently bounds the wait.
        sqlx::query("UPDATE execution SET hard_deadline_at = NULL WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
        let monitor = HeartbeatMonitor::new(db.clone(), bus.clone());
        monitor.check_once().await.unwrap();
        CrashRecovery::new(db.clone(), bus)
            .run_recovery()
            .await
            .unwrap();
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Running
        );
        assert_eq!(
            ExecutionRepo::list_running_by_task(&*db, &task.id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Disconnected
        );
        let attention: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM attention_projection WHERE dedupe_key = ? AND status = 'open'",
        )
        .bind(format!("workspace-owner:{}", placement.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(attention, 1);
        assert_eq!(
            WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
                .await
                .unwrap()
                .unwrap(),
            grant
        );
        let current = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        let suspended = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        let now = now_rfc3339();
        let expires_at = (Utc::now() + ChronoDuration::seconds(60)).to_rfc3339();
        let resumed = WorkspacePlacementRepo::resume_disconnected_execution_lease(
            &*db,
            &suspended,
            &current,
            db::RenewExecutionLease {
                execution_id: current.id.clone(),
                expected_version: current.execution_version,
                owner: current.lease_owner.clone().unwrap(),
                lease_expires_at: expires_at.clone(),
                now,
            },
        )
        .await
        .unwrap();
        let db::ExecutionLeaseMutation::Updated(resumed) = resumed else {
            panic!("unbounded execution resumes");
        };
        assert_eq!(resumed.hard_deadline_at, None);
        assert_eq!(resumed.lease_expires_at, Some(expires_at));
    }

    #[tokio::test]
    async fn daemon_owned_workspace_max_disconnect_fails_once_without_spending_retry_budget() {
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let monitor =
            HeartbeatMonitor::new(db.clone(), bus).with_max_disconnect(Duration::from_secs(300));
        monitor.check_once().await.unwrap();
        monitor.check_once().await.unwrap();
        let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert_eq!(
            failed.executor_config_snapshot_json,
            execution.executor_config_snapshot_json
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .failure_cause,
            Some(PlacementFailureCause::OwnerDisconnectedTimeout)
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(failed.error.as_deref().unwrap()).unwrap()
                ["cause"],
            "owner_disconnected_timeout"
        );
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata = task
            .metadata_json
            .as_deref()
            .map(|raw| serde_json::from_str::<serde_json::Value>(raw).unwrap())
            .unwrap_or_else(|| json!({}));
        assert!(metadata.get("execution_retry_count").is_none());
        assert!(task.failed_json.is_none());
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE event_type = 'execution.failed' AND json_extract(payload_json, '$.execution_id') = ?")
            .bind(&execution.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(events, 1);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_frozen_lease_does_not_extend_hard_deadline() {
        let db = Arc::new(sqlite_db().await);
        let (_, _, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET hard_deadline_at = ? WHERE id = ?")
            .bind((Utc::now() - ChronoDuration::seconds(1)).to_rfc3339())
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .check_once()
            .await
            .unwrap();
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Failed
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_interrupted_reconciliation_completes_through_sweep() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        // The terminal/drain committed before the server stopped; readiness
        // did not. The new monitor receives no reconnect event.
        sqlx::query("UPDATE task SET status = 'review', entry_barrier_json = ?, error_annotation = ?, blocked_json = ? WHERE id = ?")
            .bind(json!({"state": "review", "status": "blocked", "started_at":"episode"}).to_string())
            .bind(json!({"type": "before_work_hook_failed", "blocking_reason": "review_ci_infrastructure_exhausted", "recovery_actions": ["retry_hook"]}).to_string())
            .bind("{}").bind(&placement.task_id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review_ci_infrastructure','episode',5) ON CONFLICT(task_id,kind) DO UPDATE SET spent=5,window_id='episode'").bind(&placement.task_id).execute(db.pool()).await.unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request {
                    id: request_id,
                    method,
                    params,
                } = outbound.recv().await.unwrap()
                else {
                    panic!("describe request");
                };
                assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                registry.dispatch_incoming_for_connection(&daemon_id, id, api_types::DaemonFrame::Response {
                    id: request_id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
            })
        };
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry);
        monitor.check_once().await.unwrap();
        responder.await.unwrap();
        monitor.finish_placement_workers().await;
        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let task = TaskRepo::get_by_id(&*db, &placement.task_id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(task.blocked_json.is_none());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
                .unwrap()["blocking_reason"],
            "review_ci_infrastructure"
        );
        assert_eq!(
            db::budget::spent(
                db.pool(),
                &task.id,
                db::budget::Kind::ReviewCiInfrastructure.key()
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Ready
        );
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution WHERE workspace_id = ?")
                .bind(&placement.workspace_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_replayed_terminal_after_ten_minutes_ingests_outbox_once() {
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let root = tempfile::tempdir().unwrap();
        let sink = Arc::new(ServerExecutionEventSink::new(
            db.clone(),
            bus.clone(),
            root.path().to_path_buf(),
        ));
        let registry = Arc::new(DaemonConnectionRegistry::new(bus.clone(), sink.clone()));
        sink.set_connection_registry(Arc::downgrade(&registry));
        let service = TaskService::new(db.clone(), bus)
            .with_daemon_connections(registry.clone())
            .with_provider_credential_env(Arc::new(
                crate::embedded_agent_service::EmbeddedAgentService::new(
                    db.clone(),
                    b"owner-outbox-test-key",
                ),
            ));
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(
                crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone()),
            ));
        let service = Arc::new(service.with_workspace_backend_router(Arc::new(router)));
        sink.set_task_service(Arc::downgrade(&service));
        let daemon_id = placement.daemon_id.as_deref().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, daemon_id, false);
        let notification: api_types::ExecutionTerminalNotification = serde_json::from_value(json!({
            "terminal_report_id": new_uuid_v4(), "execution_id": execution.id, "exit_code": 0,
            "ts": now_rfc3339(), "status": "completed", "after_sha": "owner-head", "usage_reports": [],
            "outbox_entries": [{"type": "worklog", "position": "1", "kind": "validation", "summary": "Owner-only checks passed"}],
        })).unwrap();
        assert_eq!(
            sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
                .await
                .unwrap(),
            DaemonTerminalDisposition::AwaitingCascade
        );
        assert_eq!(
            sink.handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
                .await
                .unwrap(),
            DaemonTerminalDisposition::AwaitingCascade
        );
        // A fresh sink proves the receipt/outbox path also survives restart.
        let restarted = Arc::new(ServerExecutionEventSink::new(
            db.clone(),
            Arc::new(EventBus::new(16)),
            root.path().to_path_buf(),
        ));
        let restarted_registry = Arc::new(DaemonConnectionRegistry::new(
            Arc::new(EventBus::new(16)),
            restarted.clone(),
        ));
        restarted_registry.register(daemon_id.to_owned(), registry.get(daemon_id).unwrap());
        restarted.set_task_service(Arc::downgrade(&service));
        restarted.set_connection_registry(Arc::downgrade(&restarted_registry));
        assert_eq!(
            restarted
                .handle_terminal_with_ack(daemon_id, connection_id, notification.clone())
                .await
                .unwrap(),
            DaemonTerminalDisposition::AwaitingCascade
        );
        let finished = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(finished.status, ExecutionStatus::Completed);
        assert_eq!(finished.after_sha.as_deref(), Some("owner-head"));
        let comments: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
        )
        .bind(task.id)
        .bind(format!("outbox:{}:worklog:1", execution.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(comments, 1);
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Disconnected
        );
        // The restarted registry retains the replay until reconciliation
        // commits its cascade, then actively ACKs it without another delivery.
        restarted_registry.dispatch_incoming_for_connection(
            daemon_id,
            connection_id,
            api_types::DaemonFrame::Notification {
                method: api_types::METHOD_EXECUTION_TERMINAL.to_owned(),
                params: serde_json::to_value(notification).unwrap(),
            },
        );
        let mut responder = {
            let registry = restarted_registry.clone();
            let daemon_id = daemon_id.to_owned();
            let execution_id = execution.id.clone();
            tokio::spawn(async move {
                while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                    outbound.recv().await
                {
                    let result = match method.as_str() {
                        api_types::METHOD_WORKSPACE_DESCRIBE => json!({
                            "workspace_handle": params["workspace_handle"], "generation": 1,
                            "exists": true, "head_sha": "owner-head", "dirty": false, "branch": "task/remote",
                            "locked": false, "active_execution_ids": [], "journaled_execution_ids": [execution_id],
                        }),
                        api_types::METHOD_WORKSPACE_READ => match params["operation"].as_str() {
                            Some("git") => {
                                json!({"kind": "git", "output": match params["query"]["kind"].as_str() {
                                    Some("head" | "resolve_ref" | "merge_base" | "target_head") => "owner-head\n",
                                    _ => "",
                                }})
                            }
                            Some("paths") => {
                                json!({"kind": "paths", "workspace_path": "/owner-only/workspace", "repo_path": "/owner-only/repo"})
                            }
                            Some("files") => json!({"kind": "files", "files": []}),
                            _ => json!({"path": params["path"], "bytes": [], "truncated": false}),
                        },
                        api_types::METHOD_JOURNAL_ACK => {
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                        _ => panic!("unexpected owner RPC {method}"),
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                    if method == api_types::METHOD_JOURNAL_ACK {
                        return;
                    }
                }
                panic!("journal ACK was not sent");
            })
        };
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(restarted_registry.clone())
            .with_task_service(service);
        monitor.check_once().await.unwrap();
        let acknowledged = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut responder => { result.unwrap(); break; },
                    _ = tokio::time::sleep(Duration::from_millis(25)) => { monitor.check_once().await.unwrap(); },
                }
            }
        }).await;
        match acknowledged {
            Ok(()) => {}
            Err(_) => panic!(
                "terminal ACK missing: placement={:?}, task={:?}, steps={:?}, retained={:?}",
                WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                    .await
                    .unwrap()
                    .map(|p| p.state),
                TaskRepo::get_by_id(&*db, &execution.task_id, false)
                    .await
                    .unwrap()
                    .map(|t| t.status),
                db::TaskStepRepo::task_steps(&*db, &execution.task_id)
                    .await
                    .unwrap()
                    .iter()
                    .map(|s| (
                        &s.kind,
                        &s.status,
                        &s.last_error,
                        serde_json::from_str::<Value>(&s.payload_json)
                            .ok()
                            .map(|p| p["operation"].clone())
                    ))
                    .collect::<Vec<_>>(),
                restarted_registry.retained_terminal_execution_ids()
            ),
        }
        monitor.finish_placement_workers().await;
        let receipts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM execution_terminal_receipt WHERE execution_id = ?",
        )
        .bind(&execution.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(receipts, 1);
        let comments: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
        )
        .bind(&execution.task_id)
        .bind(format!("outbox:{}:worklog:1", execution.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(comments, 1);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_resume_requires_current_online_owner_capability() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE daemon SET detected_clis_json = ? WHERE id = ?")
            .bind(json!([{"kind":"shell","availability":"authenticated"}]).to_string())
            .bind(placement.daemon_id.as_deref().unwrap())
            .execute(db.pool())
            .await
            .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let bus = Arc::new(EventBus::new(16));
        fail_owned_execution(
            &db,
            &bus,
            &placement,
            &execution,
            PlacementFailureCause::OwnerLostExecution,
        )
        .await
        .unwrap();
        WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET agent_session_id = 'owner-session' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let mut task = TaskRepo::get_by_id(&*db, &execution.task_id, false)
            .await
            .unwrap()
            .unwrap();
        let mut annotation: serde_json::Value =
            serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
        annotation["recovery_actions"] = json!(["resume_session", "reexecute", "cancel_task"]);
        task = TaskRepo::update_status(
            &*db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: None,
                failed_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let service = TaskService::new(db.clone(), bus).with_daemon_connections(registry.clone());
        assert!(!service
            .test_action_values(&task.id)
            .await
            .unwrap()
            .contains(&api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            }));
        let (_, _first) =
            owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), false);
        assert!(!service
            .test_action_values(&task.id)
            .await
            .unwrap()
            .contains(&api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            }));
        let (_, _second) =
            owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), true);
        assert!(service
            .test_action_values(&task.id)
            .await
            .unwrap()
            .contains(&api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            }));
        DaemonRepo::mark_offline(
            &*db,
            placement.daemon_id.as_deref().unwrap(),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        assert!(!service
            .test_action_values(&task.id)
            .await
            .unwrap()
            .contains(&api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(false),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            }));
    }

    #[tokio::test]
    async fn daemon_owned_workspace_active_execution_rebinds_without_redispatch() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let grant = expired_owner_grant(&db, &task, &placement, &execution).await;
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        // A server restart can reuse the old socket's numeric incarnation.
        // Its creation time still proves this is a new reconciliation path.
        let db::ExecutionLeaseMutation::Updated(execution) = ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution.execution_version,
                owner: crate::daemon_transport::execution_lease_owner(&daemon_id, connection_id),
                lease_expires_at: execution.lease_expires_at.clone().unwrap(),
                hard_deadline_at: execution.hard_deadline_at.clone(),
                now: now_rfc3339(),
            },
        )
        .await
        .unwrap() else {
            panic!("reused owner fixture claims");
        };
        let execution_id = execution.id.clone();
        let responder = {
            let registry = registry.clone();
            let daemon_id = daemon_id.clone();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request { id, params, .. } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("describe");
                };
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [execution_id], "journaled_execution_ids": []}),
                });
            })
        };
        let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry);
        service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                None,
                None,
            )
            .await
            .unwrap();
        responder.await.unwrap();
        let current = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, ExecutionStatus::Running);
        assert_eq!(
            current.lease_owner.as_deref(),
            Some(
                crate::daemon_transport::execution_lease_owner(&daemon_id, connection_id).as_str()
            )
        );
        assert_eq!(current.hard_deadline_at, execution.hard_deadline_at);
        assert!(rfc3339_is_after(
            current.lease_expires_at.as_deref().unwrap(),
            &now_rfc3339()
        ));
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Ready
        );
        assert_eq!(
            ExecutionRepo::list_running_by_task(&*db, &execution.task_id)
                .await
                .unwrap()
                .len(),
            1
        );
        let resumed_grant = WorkspaceLeaseRepo::get_by_id(&*db, &grant.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed_grant.status, "active");
        assert!(rfc3339_is_after(&resumed_grant.expires_at, &now_rfc3339()));
        assert_eq!(resumed_grant.execution_id, execution.id);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_unknown_execution_fails_with_owner_lost_execution() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request { id, params, .. } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("describe");
                };
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 1,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []}),
                });
            })
        };
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry);
        monitor.check_once().await.unwrap();
        responder.await.unwrap();
        monitor.finish_placement_workers().await;
        let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(failed.error.as_deref().unwrap()).unwrap()
                ["cause"],
            "owner_lost_execution"
        );
        let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.state, PlacementState::Ready);
        assert_eq!(
            ready.failure_cause,
            Some(PlacementFailureCause::OwnerLostExecution)
        );
        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value =
            serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocking_reason"], "owner_lost_execution");
        assert_eq!(annotation["blocked_execution_id"], execution.id);
        assert!(task.failed_json.is_none());
        assert!(task
            .metadata_json
            .as_deref()
            .is_none_or(|raw| serde_json::from_str::<serde_json::Value>(raw)
                .unwrap()
                .get("execution_retry_count")
                .is_none()));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id = ?")
            .bind(task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_sweep_repairs_terminal_before_recovery_annotation() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        WorkspacePlacementRepo::update(
            &*db,
            placement_update(
                &placement,
                PlacementState::Failed,
                Some(PlacementFailureCause::OwnerDisconnectedTimeout),
            ),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL,
            stop_reason = 'daemon_disconnected', error = '{\"cause\":\"owner_disconnected_timeout\"}' WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)));
        monitor.check_once().await.unwrap();
        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                recovered.error_annotation.as_deref().unwrap()
            )
            .unwrap()["blocked_execution_id"],
            execution.id
        );
        // A user retry clearing the annotation must not be undone by the
        // next timeout sweep over this historical failed execution.
        let cleared = TaskRepo::update_status(
            &*db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: recovered.version,
                status: recovered.status,
                assignee_id: None,
                error_annotation: Some(None),
                blocked_json: None,
                failed_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        monitor.check_once().await.unwrap();
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(current.error_annotation.is_none());
        assert_eq!(current.version, cleared.version);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_stale_describe_generation_stays_suspended() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let execution_id = execution.id.clone();
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request { id, params, .. } =
                    outbound.recv().await.unwrap()
                else {
                    panic!("describe");
                };
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Response {
                    id, result: json!({"workspace_handle": params["workspace_handle"], "generation": 2,
                        "exists": true, "head_sha": "base-head", "dirty": false, "branch": "task/remote", "locked": false,
                        "active_execution_ids": [execution_id], "journaled_execution_ids": []}),
                });
            })
        };
        HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry)
            .check_once()
            .await
            .unwrap();
        responder.await.unwrap();
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap(),
            placement
        );
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_daemon_monitor_suspends_ready_placements() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        let mut update = placement_update(&placement, PlacementState::Ready, None);
        update.disconnected_at = Some(None);
        WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        DaemonRepo::mark_offline(
            &*db,
            placement.daemon_id.as_deref().unwrap(),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        crate::daemon_monitor::DaemonMonitor::new(db.clone(), Arc::new(EventBus::new(16)))
            .check_once()
            .await
            .unwrap();
        let disconnected = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected.state, PlacementState::Disconnected);
        assert!(disconnected.disconnected_at.is_some());
        assert_eq!(
            disconnected.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnected)
        );
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_cleanup_ack_is_owner_and_generation_fenced() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Cleaning, None),
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE workspace
             SET cleanup_attempts = 4, last_cleanup_error = 'prior cleanup failure'
             WHERE id = ?",
        )
        .bind(&placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
        let mut report: api_types::WorkspaceCleanupResult = serde_json::from_value(json!({
            "entry_id": new_uuid_v4(), "operation_id": new_uuid_v4(),
            "workspace_handle": placement.workspace_handle, "generation": 2, "cleaned": true,
        }))
        .unwrap();
        use crate::daemon_transport::DaemonTerminalDisposition;
        assert_eq!(
            apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Ignore
        );
        report.generation = 1;
        assert_eq!(
            apply_owner_cleanup(&db, "wrong-owner", &report)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Ignore
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Cleaning
        );
        assert_eq!(
            apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Acknowledge
        );
        assert_eq!(
            apply_owner_cleanup(&db, placement.daemon_id.as_deref().unwrap(), &report)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Acknowledge
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Cleaned
        );
        let workspace = WorkspaceRepo::get_by_id(&*db, &placement.workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
        assert_eq!(workspace.cleanup_attempts, 0);
        assert!(workspace.last_cleanup_error.is_none());
    }

    #[tokio::test]
    async fn daemon_owned_workspace_timeout_rejects_late_terminal_and_outbox() {
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        HeartbeatMonitor::new(db.clone(), bus.clone())
            .with_max_disconnect(Duration::from_secs(300))
            .check_once()
            .await
            .unwrap();
        let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
        let root = tempfile::tempdir().unwrap();
        let sink = ServerExecutionEventSink::new(db.clone(), bus, root.path().to_path_buf());
        sink.set_task_service(Arc::downgrade(&service));
        let terminal = serde_json::from_value(json!({"terminal_report_id": new_uuid_v4(), "execution_id": execution.id,
            "exit_code": 0, "ts": now_rfc3339(), "status": "completed", "usage_reports": [],
            "outbox_entries": [{"type": "worklog", "position": "1", "kind": "validation", "summary": "Late result"}]})).unwrap();
        assert_eq!(
            sink.handle_terminal_with_ack(placement.daemon_id.as_deref().unwrap(), 1, terminal)
                .await
                .unwrap(),
            DaemonTerminalDisposition::Ignore
        );
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap(),
            failed
        );
        let comments: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM task_comment WHERE task_id = ? AND idempotency_key = ?",
        )
        .bind(task.id)
        .bind(format!("outbox:{}:worklog:1", execution.id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(comments, 0);
    }

    #[tokio::test]
    async fn placement_monitor_stop_aborts_workers_and_releases_guards() {
        let db = Arc::new(sqlite_db().await);
        let monitor = HeartbeatMonitor::new(db, Arc::new(EventBus::default()));
        let keys = vec!["daemon:stopped-owner".to_owned()];
        monitor
            .placement_in_flight
            .lock()
            .unwrap()
            .extend(keys.clone());
        let guard = PlacementAttemptGuard {
            in_flight: monitor.placement_in_flight.clone(),
            keys,
        };
        let workers = monitor.placement_workers.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            let _finished = finished_tx;
            let _guard = guard;
            let _permit = workers.acquire_owned().await.unwrap();
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        monitor
            .placement_worker_handles
            .lock()
            .unwrap()
            .push(worker);
        started_rx.await.unwrap();
        monitor.stop();
        assert!(finished_rx.await.is_err(), "stop aborts the owner worker");
        assert!(monitor.placement_in_flight.lock().unwrap().is_empty());
        assert_eq!(monitor.placement_workers.available_permits(), 2);
    }

    #[test]
    fn placement_attempt_guard_recovers_a_poisoned_mutex() {
        let in_flight = Arc::new(Mutex::new(HashSet::from(["placement:poisoned".into()])));
        let poisoned = in_flight.clone();
        assert!(std::panic::catch_unwind(move || {
            let _lock = poisoned.lock().unwrap();
            panic!("poison fixture");
        })
        .is_err());
        drop(PlacementAttemptGuard {
            in_flight: in_flight.clone(),
            keys: vec!["placement:poisoned".into()],
        });
        assert!(in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[tokio::test]
    async fn placement_sweep_skips_ready_workspaces_without_terminal_or_recent_reconcile() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (_, mut outbound) =
            owner_connection(&registry, placement.daemon_id.as_deref().unwrap(), false);
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::default()))
            .with_daemon_connections(registry);
        monitor.check_once().await.unwrap();
        monitor.finish_placement_workers().await;
        assert!(
            outbound.try_recv().is_err(),
            "no workspace RPC for an unrelated ready placement"
        );
    }

    #[tokio::test]
    async fn heartbeat_placement_timeout_does_not_delay_liveness_or_next_owner() {
        let db = Arc::new(sqlite_db().await);
        let (_, bad, bad_execution) = daemon_owned_fixture(&db).await;
        let (_, good, good_execution) = daemon_owned_fixture(&db).await;
        for execution in [&bad_execution, &good_execution] {
            sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'base-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
                .bind(&execution.id).execute(db.pool()).await.unwrap();
        }
        let stale = seed_agent(&db, AgentStatus::Busy, Some("1970-01-01T00:00:00Z".into())).await;
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (_, mut silent) = owner_connection(&registry, bad.daemon_id.as_deref().unwrap(), false);
        let daemon_id = good.daemon_id.clone().unwrap();
        let (connection_id, mut responsive) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let api_types::DaemonFrame::Request { id, method, params } =
                    responsive.recv().await.unwrap()
                else {
                    panic!("describe request")
                };
                assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id,
                    api_types::DaemonFrame::Response { id, result: json!({"workspace_handle": params["workspace_handle"],
                        "generation": 1, "exists": true, "head_sha": "base-head", "dirty": false,
                        "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []}) });
            })
        };
        let bus = Arc::new(EventBus::new(32));
        let mut events = bus.subscribe();
        let monitor =
            Arc::new(HeartbeatMonitor::new(db.clone(), bus).with_daemon_connections(registry));
        let tick = {
            let monitor = monitor.clone();
            tokio::spawn(async move { monitor.check_once().await })
        };
        silent.recv().await.unwrap();
        assert_eq!(
            AgentRepo::get_by_id(&*db, &stale.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            AgentStatus::Error,
            "core liveness must finish before a silent owner RPC starts"
        );
        tick.await.unwrap().unwrap();
        responder.await.unwrap();
        // Wait for the responsive owner's committed reconciliation before
        // pausing time; SQLite work must not race Tokio's auto-advance.
        loop {
            let event = events.recv().await.unwrap();
            if event.event_type == "reconciliation.event" && event.entity_id == good.task_id {
                break;
            }
        }
        // Only the silent owner's RPC deadline remains.

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(
            api_types::DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS,
        ))
        .await;
        tokio::time::resume();
        monitor.finish_placement_workers().await;
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &good.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Ready
        );
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &bad.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Disconnected
        );
    }

    #[tokio::test]
    async fn placement_reconcile_accepts_forge_rebase_head() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'old-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let placement = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            let daemon_id = daemon_id.clone();
            tokio::spawn(async move {
                for step in 0..4 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("owner rebase request")
                    };
                    let result = match step {
                        0 | 3 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                            json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true,
                                "head_sha": if step == 0 { "old-head" } else { "rebased-head" }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                        }
                        1 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                            json!({"entry_id": "rebase-entry", "operation_id": params["operation_id"], "outcome": {"kind": "rebased"}})
                        }
                        _ => {
                            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
            })
        };
        let resolved = crate::workspace_backend::ResolvedWorkspace {
            placement: placement.clone(),
            backend: Arc::new(crate::workspace_backend::DaemonWorkspaceBackend::new(
                db.clone(),
                registry.clone(),
            )),
        };
        assert!(matches!(
            resolved.rebase_target("main", false).await.unwrap(),
            api_types::WorkspaceOwnerOperationOutcome::Rebased
        ));
        resolved.record_rebase_head(&db).await.unwrap();
        sqlx::query("UPDATE execution SET updated_at = ? WHERE id = ?")
            .bind((Utc::now() + ChronoDuration::minutes(1)).to_rfc3339())
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        responder.await.unwrap();
        let placement = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Disconnected, None),
        )
        .await
        .unwrap();
        let state = serde_json::from_value(json!({"workspace_handle": placement.workspace_handle, "generation": 1,
            "exists": true, "head_sha": "rebased-head", "dirty": false, "branch": "task/remote", "locked": false,
            "active_execution_ids": [], "journaled_execution_ids": []})).unwrap();
        assert!(complete_workspace_reconciliation(
            &db,
            &EventBus::new(32),
            &registry,
            None,
            &placement,
            state,
            false,
            connection_id
        )
        .await
        .unwrap());
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Ready
        );
    }

    #[tokio::test]
    async fn placement_crash_orphaned_preparing_without_expiry_releases_capacity() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE workspace_placement SET state = 'preparing', reserved_until = NULL, updated_at = ? WHERE id = ?")
            .bind((Utc::now() - ChronoDuration::minutes(11)).to_rfc3339())
            .bind(&placement.id).execute(db.pool()).await.unwrap();
        assert_eq!(
            crate::placement::admission::sweep_expired_reservations(&db, &now_rfc3339())
                .await
                .unwrap(),
            1
        );
        let expired = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.state, PlacementState::Failed);
        assert!(expired.workspace_handle.is_none());
        assert_eq!(
            expired.failure_cause,
            Some(PlacementFailureCause::PrepareFailed)
        );
        assert_eq!(
            crate::agent_capacity::count_occupied_agent_slots(
                &db,
                placement.agent_id.as_deref().unwrap()
            )
            .await
            .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn daemon_owned_workspace_reservation_expiry_runs_without_active_dispatch() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut update = placement_update(&placement, PlacementState::Reserved, None);
        update.reserved_until = Some(Some((Utc::now() - ChronoDuration::minutes(1)).to_rfc3339()));
        WorkspacePlacementRepo::update(&*db, update).await.unwrap();
        HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .check_once()
            .await
            .unwrap();
        let expired = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.state, PlacementState::Failed);
        assert_eq!(
            expired.failure_cause,
            Some(PlacementFailureCause::PrepareFailed)
        );
        let executions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM execution WHERE task_id = ?")
                .bind(&task.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(executions, 1);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_sweep_reconciles_operations_then_retries_cleaned_acknowledgements(
    ) {
        use crate::daemon_transport::workspace_client::DaemonWorkspaceClient;
        use crate::daemon_transport::{
            DaemonExecutionEventHandler, DaemonTerminalDisposition, ServerExecutionEventSink,
        };
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Cleaning, None),
        )
        .await
        .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let client = DaemonWorkspaceClient::new(registry.clone()).with_receipts(db.clone());
        let params = json!({"daemon_id": daemon_id, "runtime_id": placement.runtime_id,
            "placement_id": placement.id, "workspace_handle": placement.workspace_handle,
            "generation": 1, "operation_id": "run-receipt", "expected": {"kind": "base_sha", "sha": "base-head"},
            "purpose": "ci_step", "command": "printf once", "env": [], "timeout_secs": 0, "max_output_bytes": 1024});
        client
            .remember_mutation(
                &daemon_id,
                api_types::METHOD_WORKSPACE_RUN,
                params.clone(),
                None,
            )
            .await
            .unwrap();
        let sink = ServerExecutionEventSink::new(
            db.clone(),
            Arc::new(EventBus::new(32)),
            std::path::PathBuf::new(),
        );
        sink.set_connection_registry(Arc::downgrade(&registry));
        let cleanup: api_types::WorkspaceCleanupResult = serde_json::from_value(json!({"entry_id": "cleaned-entry", "operation_id": "cleanup-receipt", "workspace_handle": placement.workspace_handle,
            "generation": 1, "cleaned": true})).unwrap();
        for _ in 0..2 {
            assert_eq!(
                sink.handle_workspace_cleanup(&daemon_id, connection_id, cleanup.clone())
                    .await
                    .unwrap(),
                DaemonTerminalDisposition::Acknowledge
            );
        }
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                for step in 0..3 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("receipt reconciliation request");
                    };
                    let result = if step == 0 {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        assert_eq!(params["operation"], "reconcile");
                        assert_eq!(params["operation_id"], "run-receipt");
                        json!({"entry_id": "run-entry", "operation_id": "run-receipt", "outcome": {"kind": "result", "result": {
                            "entry_id": "run-entry", "operation_id": "run-receipt", "exit_code": 0, "stdout": "once", "stderr": "",
                            "duration_ms": 1, "timed_out": false, "stdout_truncated": false, "stderr_truncated": false}}})
                    } else {
                        assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                        assert_eq!(
                            params["entry_id"],
                            if step == 1 {
                                "run-entry"
                            } else {
                                "cleaned-entry"
                            }
                        );
                        json!({"entry_id": params["entry_id"], "acknowledged": true})
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
            })
        };
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry);
        monitor.check_once().await.unwrap();
        responder.await.unwrap();
        monitor.check_once().await.unwrap();
        monitor.finish_placement_workers().await;
        let acknowledgements: i64 = sqlx::query_scalar("SELECT count(*) FROM command_receipt WHERE operation IN ('daemon.workspace.run.ack', 'daemon.workspace.cleanup.ack')")
            .fetch_one(db.pool()).await.unwrap();
        assert_eq!(acknowledgements, 2);
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Cleaned
        );
    }

    #[tokio::test]
    async fn placement_launch_recreates_deleted_daemon_worktree() {
        assert_daemon_launch_recreates_worktree(false).await;
    }

    #[tokio::test]
    async fn placement_launch_leaves_damaged_daemon_worktree_for_reset() {
        assert_daemon_launch_recreates_worktree(true).await;
    }

    async fn assert_daemon_launch_recreates_worktree(damaged: bool) {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let ready = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = ready.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                for step in 0..4 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("owner launch request")
                    };
                    if step == 0 && damaged {
                        assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                        registry.dispatch_incoming_for_connection(
                            &daemon_id,
                            connection_id,
                            api_types::DaemonFrame::Error {
                                id: Some(id),
                                error: api_types::DaemonErrorPayload {
                                    code: "workspace_error".into(),
                                    message: "git: not a git repository".into(),
                                    details: None,
                                },
                            },
                        );
                        break;
                    }
                    let result = match step {
                        0 | 3 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                            json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": step == 3,
                                "head_sha": if step == 3 { Some("owner-head") } else { None }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                        }
                        1 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_PREPARE);
                            assert_eq!(params["expected"]["sha"], "base-head");
                            assert_eq!(params["branch"], "task/remote");
                            json!({"entry_id": "launch-recover-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt", "generation": 1,
                                "base_sha": "base-head", "branch": "task/remote"})
                        }
                        _ => {
                            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
                outbound
            })
        };
        let root = tempfile::TempDir::new().unwrap();
        let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_workspace_root(root.path().to_path_buf());
        let router = (*service.workspace_backend_router())
            .clone()
            .with_daemon(Arc::new(
                crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry),
            ));
        let prepared = crate::task_service::workspace::prepare_workspace(
            &db,
            root.path(),
            &task,
            &task.id,
            None,
            &router,
        )
        .await;
        if damaged {
            assert!(
                matches!(prepared, Err(ServiceError::Git(_))),
                "{prepared:?}"
            );
            assert!(
                responder.await.unwrap().try_recv().is_err(),
                "damaged describe must not send prepare"
            );
            assert_eq!(
                WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
                    .await
                    .unwrap()
                    .unwrap(),
                ready
            );
            return;
        }
        let workspace = prepared.unwrap();
        let reused = crate::task_service::workspace::prepare_workspace(
            &db,
            root.path(),
            &task,
            &task.id,
            None,
            &router,
        )
        .await
        .unwrap();
        responder.await.unwrap();
        assert_eq!(workspace.id, ready.workspace_id);
        assert_eq!(reused.id, workspace.id);
        assert_eq!(reused.branch, "task/remote");
        let stored = WorkspacePlacementRepo::get_by_id(&*db, &ready.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.daemon_id, ready.daemon_id);
        assert_eq!(
            stored.workspace_handle.as_deref(),
            Some("opaque-owner-handle")
        );
        assert_eq!(ExecutionRepo::list_running(&*db).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn daemon_owned_workspace_missing_worktree_recovers_same_owner_branch() {
        let db = Arc::new(sqlite_db().await);
        let (_, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', after_sha = 'owner-head', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let responder = {
            let registry = registry.clone();
            tokio::spawn(async move {
                for step in 0..4 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("same-owner recovery request");
                    };
                    let result = match step {
                        0 | 3 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                            json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": step == 3,
                                "head_sha": if step == 3 { Some("owner-head") } else { None }, "dirty": false,
                                "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                        }
                        1 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_PREPARE);
                            assert_eq!(params["generation"], 1);
                            assert_eq!(params["branch"], "task/remote");
                            json!({"entry_id": "recovered-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt", "generation": 1,
                                "base_sha": "base-head", "branch": "task/remote"})
                        }
                        _ => {
                            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
            })
        };
        let monitor = HeartbeatMonitor::new(db.clone(), Arc::new(EventBus::new(32)))
            .with_daemon_connections(registry);
        monitor.check_once().await.unwrap();
        responder.await.unwrap();
        monitor.finish_placement_workers().await;
        let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.state, PlacementState::Ready);
        assert_eq!(ready.daemon_id, placement.daemon_id);
        assert_eq!(ready.generation, 1);
        assert_eq!(ExecutionRepo::list_running(&*db).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn daemon_owned_ready_workspace_reset_persists_owner_handle_and_next_generation() {
        assert_ready_owner_reset(false).await;
    }

    #[tokio::test]
    async fn daemon_owned_ready_task_reset_to_initial_recreates_on_same_owner() {
        assert_ready_owner_reset(true).await;
    }

    async fn assert_ready_owner_reset(task_reset: bool) {
        use crate::workspace_backend::{
            DaemonWorkspaceBackend, EmbeddedWorkspaceBackend, WorkspaceBackendRouter,
        };
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let placement = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        annotate_owner_workspace_reset(&db, &placement, "reset owner workspace")
            .await
            .unwrap();
        let bus = Arc::new(EventBus::new(32));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let root = tempfile::tempdir().unwrap();
        let merge = Arc::new(crate::MergeService::new(
            db.clone(),
            bus.clone(),
            root.path().to_path_buf(),
        ));
        let router = Arc::new(
            WorkspaceBackendRouter::new(Arc::new(EmbeddedWorkspaceBackend::new(
                db.clone(),
                merge,
                root.path().to_path_buf(),
            )))
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(
                db.clone(),
                registry.clone(),
            ))),
        );
        let service = TaskService::new(db.clone(), bus)
            .with_daemon_connections(registry.clone())
            .with_workspace_backend_router(router);
        let responder = {
            let registry = registry.clone();
            let expected_placement = placement.clone();
            tokio::spawn(async move {
                for step in 0..3 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("expected owner reset request");
                    };
                    let result = match step {
                        0 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                            assert_eq!(params["generation"], 1);
                            json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true,
                                "head_sha": "candidate-head", "dirty": true, "branch": "task/remote", "locked": false,
                                "active_execution_ids": [], "journaled_execution_ids": []})
                        }
                        1 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                            assert_eq!(
                                params["daemon_id"],
                                expected_placement.daemon_id.as_deref().unwrap()
                            );
                            assert_eq!(
                                params["runtime_id"],
                                expected_placement.runtime_id.as_deref().unwrap()
                            );
                            assert_eq!(params["placement_id"], expected_placement.id);
                            assert_eq!(params["workspace_handle"], "opaque-owner-handle");
                            assert_eq!(params["generation"], 2);
                            assert_eq!(params["expected"]["sha"], "candidate-head");
                            assert_eq!(params["base_ref"], "base-head");
                            json!({"entry_id": "reset-entry", "operation_id": params["operation_id"],
                                "workspace_handle": "opaque-owner-handle", "workspace_path": "/owner-only/rebuilt",
                                "generation": 2, "base_sha": "base-head", "branch": "task/remote"})
                        }
                        _ => {
                            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
            })
        };
        if task_reset {
            let recovered = service
                .test_apply_action(
                    &task.id,
                    api_types::TaskAction::Restart { reason: None },
                    None,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(recovered.status, crate::workflow::default_states::TODO);
            assert!(recovered.error_annotation.is_none());
        } else {
            let workspace = service.reset_task_workspace(&task.id).await.unwrap();
            assert_eq!(workspace.id, placement.workspace_id);
            assert_eq!(workspace.before_sha.as_deref(), Some("base-head"));
        }
        responder.await.unwrap();
        let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.generation, 2);
        assert_eq!(ready.version, placement.version + 1);
        assert_eq!(
            ready.workspace_handle.as_deref(),
            Some("opaque-owner-handle")
        );
        assert_eq!(ready.owner_kind, placement.owner_kind);
        assert_eq!(ready.daemon_id, placement.daemon_id);
        assert_eq!(ready.runtime_id, placement.runtime_id);
        assert_eq!(ready.state, PlacementState::Ready);
        assert!(!root.path().join(&task.id).exists());
    }

    #[tokio::test]
    async fn daemon_owned_workspace_reset_receipt_repairs_generation_cas_conflict() {
        use crate::workspace_backend::{
            DaemonWorkspaceBackend, EmbeddedWorkspaceBackend, WorkspaceBackendRouter,
        };
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        annotate_owner_workspace_reset(&db, &placement, "owner HEAD differs")
            .await
            .unwrap();
        let bus = Arc::new(EventBus::new(32));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.clone().unwrap();
        let (connection_id, mut outbound) = owner_connection(&registry, &daemon_id, false);
        let root = tempfile::tempdir().unwrap();
        let merge = Arc::new(crate::MergeService::new(
            db.clone(),
            bus.clone(),
            root.path().to_path_buf(),
        ));
        let router = Arc::new(
            WorkspaceBackendRouter::new(Arc::new(EmbeddedWorkspaceBackend::new(
                db.clone(),
                merge,
                root.path().to_path_buf(),
            )))
            .with_daemon(Arc::new(DaemonWorkspaceBackend::new(
                db.clone(),
                registry.clone(),
            ))),
        );
        let service = TaskService::new(db.clone(), bus)
            .with_daemon_connections(registry.clone())
            .with_workspace_backend_router(router);
        let responder = {
            let registry = registry.clone();
            let db = db.clone();
            let placement = placement.clone();
            tokio::spawn(async move {
                for step in 0..3 {
                    let api_types::DaemonFrame::Request { id, method, params } =
                        outbound.recv().await.unwrap()
                    else {
                        panic!("reset recovery request");
                    };
                    let result = match step {
                        0 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
                            json!({"workspace_handle": params["workspace_handle"], "generation": 1, "exists": true, "head_sha": "unrecorded-head",
                                "dirty": false, "branch": "task/remote", "locked": false, "active_execution_ids": [], "journaled_execution_ids": []})
                        }
                        1 => {
                            assert_eq!(method, api_types::METHOD_WORKSPACE_RESET);
                            assert_eq!(params["generation"], 2);
                            assert_eq!(params["expected"]["sha"], "unrecorded-head");
                            assert_eq!(params["base_ref"], "base-head");
                            WorkspacePlacementRepo::update(
                                &*db,
                                placement_update(
                                    &placement,
                                    PlacementState::Disconnected,
                                    placement.failure_cause.clone(),
                                ),
                            )
                            .await
                            .unwrap();
                            json!({"entry_id": "reset-entry", "operation_id": params["operation_id"], "workspace_handle": "opaque-owner-handle",
                                "workspace_path": "/owner-only/rebuilt", "generation": 2, "base_sha": "base-head", "branch": "task/remote"})
                        }
                        _ => {
                            assert_eq!(method, api_types::METHOD_JOURNAL_ACK);
                            json!({"entry_id": params["entry_id"], "acknowledged": true})
                        }
                    };
                    registry.dispatch_incoming_for_connection(
                        &daemon_id,
                        connection_id,
                        api_types::DaemonFrame::Response { id, result },
                    );
                }
            })
        };
        assert!(matches!(
            service
                .test_apply_action(
                    &task.id,
                    api_types::TaskAction::Restart { reason: None },
                    None,
                    None
                )
                .await,
            Err(ServiceError::Db(db::DbError::VersionConflict))
        ));
        responder.await.unwrap();
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
                .await
                .unwrap()
                .unwrap()
                .generation,
            1
        );
        let recovered = service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Restart { reason: None },
                None,
                None,
            )
            .await
            .unwrap();
        assert!(recovered.error_annotation.is_none());
        let ready = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ready.generation, 2);
        assert_eq!(
            ready.workspace_handle.as_deref(),
            Some("opaque-owner-handle")
        );
        assert_eq!(ready.daemon_id, placement.daemon_id);
        assert_eq!(ready.state, PlacementState::Ready);
    }

    #[tokio::test]
    async fn placement_recovery_refuses_offline_agent_and_preserves_blocker() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'failed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?")
            .bind(&execution.id).execute(db.pool()).await.unwrap();
        let ready = WorkspacePlacementRepo::update(
            &*db,
            placement_update(&placement, PlacementState::Ready, None),
        )
        .await
        .unwrap();
        db::TaskRoleAssignmentRepo::assign(
            &*db,
            db::CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: "coder".into(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: placement.agent_id.clone(),
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(json!({"type":"manual_stop", "blocking_reason":"manual_stop", "blocked_by":"user",
                "blocked_at":now_rfc3339(), "blocked_execution_id":execution.id, "message":"retry on same owner",
}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        DaemonRepo::mark_offline(&*db, ready.daemon_id.as_deref().unwrap(), &now_rfc3339())
            .await
            .unwrap();
        let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)));
        let before = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let error = service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                None,
                None,
            )
            .await
            .expect_err("explicit recovery preserves the base offline-agent refusal");
        assert!(
            matches!(error, ServiceError::TaskActionUnavailable { ref available_actions, .. } if available_actions.iter().all(|offer| offer.action.verb() != "retry")),
            "{error:?}"
        );
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.error_annotation, before.error_annotation);
        assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
        assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn daemon_owned_workspace_offline_retry_waits_on_same_owner_and_can_cancel() {
        let db = Arc::new(sqlite_db().await);
        let (task, placement, execution) = daemon_owned_fixture(&db).await;
        let service = TaskService::new(db.clone(), Arc::new(EventBus::new(32)));
        let actions = service.test_action_values(&task.id).await.unwrap();
        assert_eq!(
            actions,
            vec![
                api_types::TaskAction::Cancel { reason: None },
                api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None
                },
            ]
        );
        let retried = service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(retried.status, task.status);
        let unchanged = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.daemon_id, placement.daemon_id);
        assert_eq!(unchanged.workspace_handle, placement.workspace_handle);
        assert_eq!(
            ExecutionRepo::list_running_by_task(&*db, &task.id)
                .await
                .unwrap()
                .len(),
            1
        );
        let cancelled = service
            .test_apply_action(
                &task.id,
                api_types::TaskAction::Cancel { reason: None },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status, "cancelled");
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Cancelled
        );
    }
    #[tokio::test]
    async fn restart_recovery_queues_dead_execution_settlement_before_worker_runs() {
        use db::TaskStepRepo;
        let db = Arc::new(sqlite_db().await);
        let bus = Arc::new(EventBus::new(32));
        let (project_id, _) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            "in_progress".into(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
        let restarted = Arc::new(SqliteDb::new(db.pool().clone()));
        CrashRecovery::new(restarted.clone(), bus.clone())
            .run_recovery()
            .await
            .unwrap();
        assert_eq!(
            ExecutionRepo::get_by_id(&*restarted, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Running
        );
        let queued = restarted.task_steps(&task.id).await.unwrap();
        assert!(queued.iter().any(|s| s.kind == "command"
            && s.status == "pending"
            && s.payload_json.contains("recover_task_after_restart")));
        let service = TaskService::new(restarted.clone(), bus);
        service.drain(&task.id).await.unwrap();
        let settled = ExecutionRepo::get_by_id(&*restarted, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.status, ExecutionStatus::Cancelled);
        assert_eq!(settled.resume_policy, Some(ResumePolicy::Auto));
        assert!(restarted
            .task_steps(&task.id)
            .await
            .unwrap()
            .iter()
            .any(|s| s.kind == "command" && s.status == "done"));
    }
}

/// Runs under the affected Task's normal lease, including after a restart.
/// Owner-loss terminalization and annotations use the same disconnect settlement
/// as the heartbeat monitor; no Task state is changed by the removal transaction.
pub(crate) async fn settle_removed_machine_task(
    db: &SqliteDb,
    event_bus: &EventBus,
    task_id: &str,
    daemon_id: &str,
) -> Result<()> {
    db::task_writer::debug_assert_task_lease(task_id, "machine removal settlement");
    if !db.daemon_removed(daemon_id).await? {
        return Ok(());
    }
    db::task_writer::TaskQuery::new(db,task_id,
        "UPDATE task SET error_annotation=NULL, metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.dispatch_disposition','$.deferred_dispatch') ELSE metadata_json END, version=version+1,updated_at=? WHERE id=? AND deleted_at IS NULL AND json_valid(error_annotation) AND json_extract(error_annotation,'$.blocking_reason')='pending_remote_cancel' AND NOT EXISTS(SELECT 1 FROM pending_remote_cancel c LEFT JOIN workspace w ON w.id=c.workspace_id LEFT JOIN task_step s ON s.id=c.step_id WHERE w.task_id=? OR s.task_id=? OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=? AND e.workspace_id=c.workspace_id))")
        .bind(now_rfc3339()).bind(task_id).bind(task_id).bind(task_id).bind(task_id)
        .identity_fenced().execute(db.pool()).await?;
    let remaining = db.task_pending_remote_cancel_machines(task_id).await?;
    if !remaining.is_empty() {
        if let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? {
            if let Some(raw) = task.error_annotation.as_deref() {
                if let Ok(mut annotation) = serde_json::from_str::<serde_json::Value>(raw) {
                    if annotation["blocking_reason"] == "pending_remote_cancel" {
                        let machines = remaining.join(", ");
                        annotation["blocked_by"] = json!(format!("machine:{machines}"));
                        annotation["message"] = json!(format!("Waiting for machine {machines} to confirm its remote work has stopped. Reconnect that machine to finish cleanup."));
                        db::task_writer::TaskQuery::new(db, task_id,
                            "UPDATE task SET error_annotation=?,version=version+1,updated_at=? WHERE id=? AND error_annotation=?")
                            .bind(annotation.to_string()).bind(now_rfc3339()).bind(task_id).bind(raw)
                            .identity_fenced().execute(db.pool()).await?;
                    }
                }
            }
        }
    }
    // An unplaced Task may be waiting on this owner's environment/provisioning
    // attempt. Wake its existing scheduler path instead of leaving a stale delay.
    db::task_writer::TaskQuery::new(db,task_id,
        "UPDATE task SET metadata_json=json_remove(metadata_json,'$.environment_wait','$.deferred_dispatch','$.dispatch_disposition') WHERE id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine.daemon_id')=?")
        .bind(task_id).bind(daemon_id).identity_fenced().execute(db.pool()).await?;
    for execution in ExecutionRepo::list_running_by_task(db, task_id).await? {
        let Some((owner, _)) = resolve_execution_daemon(db, &execution).await? else {
            continue;
        };
        if owner != daemon_id {
            continue;
        }
        let placement = match execution.workspace_id.as_deref() {
            Some(id) => WorkspacePlacementRepo::get_by_workspace_id(db, id).await?,
            None => None,
        };
        if let Some(placement) = placement {
            fail_owned_execution(
                db,
                event_bus,
                &placement,
                &execution,
                PlacementFailureCause::OwnerDisconnectedTimeout,
            )
            .await?;
        } else if let Some(failed) = fail_execution_daemon_disconnected(
            db,
            event_bus,
            None,
            FailDaemonDisconnectedExecution {
                execution: &execution,
                daemon_id,
                error_message: json!({"cause":"owner_disconnected_timeout","daemon_id":daemon_id})
                    .to_string(),
                stopped_by: &api_types::Actor::system(api_types::SystemComponent::HeartbeatMonitor)
                    .display(),
                reconciliation_reason: "owner_disconnected_timeout",
            },
        )
        .await?
        {
            annotate_owner_recovery(
                db,
                &failed,
                &PlacementFailureCause::OwnerDisconnectedTimeout,
            )
            .await?;
        }
    }
    // Repair an interrupted terminal -> annotation window through the existing
    // owner-loss repair path, without undoing a subsequent explicit user action.
    let placement_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM workspace_placement WHERE task_id=?")
            .bind(task_id)
            .fetch_all(db.pool())
            .await?;
    for id in placement_ids {
        let Some(placement) = WorkspacePlacementRepo::get_by_id(db, &id).await? else {
            continue;
        };
        if placement_execution_daemon_id(&placement) == Some(daemon_id) {
            repair_owner_recovery(db, &placement).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod removed_machine_tests {
    use super::*;

    #[tokio::test]
    async fn removed_owner_settles_running_execution_and_parks_pinned_workspace() {
        use crate::workspace_backend::WorkspaceBackend;
        let db = Arc::new(SqliteDb::new(
            db::create_sqlite_pool("sqlite::memory:").await.unwrap(),
        ));
        db::run_migrations(db.pool()).await.unwrap();
        let (task, placement, execution) = tests::daemon_owned_fixture(&db).await;
        let daemon_id = placement.daemon_id.as_deref().unwrap();
        DaemonRepo::mark_offline(&*db, daemon_id, &now_rfc3339())
            .await
            .unwrap();
        let bus = Arc::new(EventBus::new(64));
        let service = TaskService::new(db.clone(), bus.clone());
        let original_name = DaemonRepo::get_by_id(&*db, daemon_id)
            .await
            .unwrap()
            .unwrap()
            .hostname;
        sqlx::query("INSERT INTO repo_provision_retry(repo_id,runtime_id,next_attempt_at) SELECT w.repo_id,p.runtime_id,? FROM workspace w JOIN workspace_placement p ON p.workspace_id=w.id WHERE p.id=?")
            .bind(now_rfc3339()).bind(&placement.id).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO project_machine_readiness(project_id,owner_kind,daemon_id,runtime_id,status,checks_digest) VALUES (?,'daemon',?,?,'not_ready','old-digest')")
            .bind(&task.project_id).bind(daemon_id).bind(&placement.runtime_id).execute(db.pool()).await.unwrap();
        let result = db
            .remove_daemon(daemon_id, "admin", true, "local", false)
            .await
            .unwrap();
        assert_eq!(result.provisioning_attempts_cleared, 1);
        assert_eq!(result.readiness_records_cleared, 1);
        assert_eq!(result.placements_failed, 1);
        assert_eq!(result.tasks_queued, 1);
        assert_eq!(
            ExecutionRepo::get_by_id(&*db, &execution.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ExecutionStatus::Running
        );
        let settled = service.drain(&task.id).await.unwrap();
        let failed = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.status, ExecutionStatus::Failed);
        assert_eq!(failed.stop_reason, Some(StopReason::DaemonDisconnected));
        assert_eq!(failed.resume_policy, Some(ResumePolicy::Manual));
        assert_eq!(
            settled.status, task.status,
            "owner loss does not spend workflow retries"
        );
        let annotation: serde_json::Value =
            serde_json::from_str(settled.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocking_reason"], "owner_disconnected_timeout");
        let current = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.state, PlacementState::Failed);
        assert_eq!(
            current.daemon_id, placement.daemon_id,
            "existing owner pin is preserved"
        );
        assert_eq!(
            current.failure_cause,
            Some(PlacementFailureCause::OwnerDisconnectedTimeout)
        );
        let name:String=sqlx::query_scalar("SELECT d.hostname FROM execution e JOIN workspace_placement p ON p.workspace_id=e.workspace_id JOIN daemon d ON d.id=p.daemon_id WHERE e.id=?")
            .bind(&execution.id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(name, original_name);
        let backend = crate::workspace_backend::DaemonWorkspaceBackend::new(
            db.clone(),
            Arc::new(DaemonConnectionRegistry::without_handlers()),
        );
        assert!(
            !backend.cleanup(&current).await.unwrap().removed,
            "removal abandons physical files without an RPC"
        );
        let status = crate::OperatorStatusService::new_for_test(db.clone());
        let status = status.compute_status().await.unwrap();
        assert!(!status
            .daemon_issues
            .iter()
            .any(|row| row.daemon_id == daemon_id));
        assert!(!status
            .daemon_pressure
            .iter()
            .any(|row| row.daemon_id == daemon_id));
    }
}
