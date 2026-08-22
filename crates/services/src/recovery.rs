use crate::{
    daemon_transport::DaemonConnectionRegistry, embedded_daemon::is_embedded_daemon_machine,
    workflow::engine::WorkflowEngine, DomainEventService, Result, ServiceError, TaskService,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db::{
    now_rfc3339, Agent, AgentListQuery, AgentRepo, AgentSessionRepo, AgentStatus, Daemon,
    DaemonRepo, Execution, ExecutionLeaseDisposition, ExecutionProgressWarningOutcome,
    ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome, PageRequest, Project, ProjectRepo,
    RecordExecutionProgressWarning, ResumePolicy, SortBy, SortOrder, SqliteDb, StopReason, Task,
    TaskListQuery, TaskRepo, TerminalizeExecution, UpdateAgent, UpdateExecution, UpdateTaskStatus,
    WorkspaceLeaseRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::TaskExecutor;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::Instrument;

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
        // Expire stale grants before recovering active Tasks. The recovery
        // pass below then sees the still-running attempt and requeues/blocks
        // it through the normal crash-recovery state machine.
        expire_workspace_leases(&self.db, &self.event_bus, None, None, false).await?;

        // Native (in-process) runtime sessions cannot survive a restart, so
        // any 'starting'/'ready'/'running'/'degraded' native session left by
        // a previous process lies about liveness. Suspend them: the reuse
        // path (get_active_agent_session) then re-establishes a fresh session
        // on next use, and conversation continuity is preserved because the
        // LCM timeline is keyed by identity + scope, not the runtime id.
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
            let outcome = recover_task(
                &self.db,
                task,
                StopReason::CrashRecovery,
                &api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
            )
            .await?;

            if outcome.annotated {
                publish_task_status_event(&self.db, &self.event_bus, &outcome.task).await;
                self.publish(ForgeEvent {
                    event_type: "task.recovered".to_owned(),
                    entity_id: outcome.task.id,
                    timestamp: event_timestamp(),
                    context: EventContext::TaskRecovered {
                        project_id: outcome.task.project_id,
                        reason: "crash_recovery".to_owned(),
                    },
                });
                recovered += 1;
            }
        }

        recovered += sweep_stale_recovery_annotations(&self.db, &self.event_bus).await?;

        for project in list_projects(&self.db).await? {
            let mut cursor = None;
            loop {
                let page = TaskRepo::list(
                    &*self.db,
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
                    let Some(entry_barrier_json) = &task.entry_barrier_json else {
                        continue;
                    };
                    let Ok(entry_barrier) =
                        serde_json::from_str::<serde_json::Value>(entry_barrier_json)
                    else {
                        continue;
                    };
                    if entry_barrier
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        != Some("running")
                    {
                        continue;
                    }

                    let blocked_barrier = json!({
                        "state": entry_barrier.get("state").cloned().unwrap_or(serde_json::Value::Null),
                        "started_at": entry_barrier.get("started_at").cloned().unwrap_or(serde_json::Value::Null),
                        "status": "blocked",
                        "updated_at": db::now_rfc3339(),
                        "blocking_reason": "crash recovery: before_enter was interrupted",
                    })
                    .to_string();
                    match TaskRepo::set_entry_barrier(
                        &*self.db,
                        &task.id,
                        task.version,
                        Some(blocked_barrier),
                        &db::now_rfc3339(),
                    )
                    .await
                    {
                        Ok(_) => {
                            recovered += 1;
                        }
                        Err(error) => {
                            tracing::warn!(
                                task_id = %task.id,
                                %error,
                                "failed to recover interrupted entry barrier"
                            );
                        }
                    }
                }
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }

        tracing::info!(recovered_tasks = recovered, "crash recovery completed");
        Ok(recovered)
    }

    #[tracing::instrument(skip(self), fields(agent_id = agent_id.unwrap_or("any")))]
    async fn list_in_progress_tasks(&self, agent_id: Option<&str>) -> Result<Vec<Task>> {
        list_in_progress_tasks(&self.db, agent_id).await
    }

    fn publish(&self, event: ForgeEvent) {
        self.event_bus.publish(event);
    }
}

pub struct HeartbeatMonitor {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Option<Arc<TaskService>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    daemon_connections: Option<Arc<DaemonConnectionRegistry>>,
    check_interval: Duration,
    execution_stall_timeout: Duration,
    daemon_disconnect_grace: Duration,
    disconnect_observed: Mutex<HashMap<String, Instant>>,
    stopped: AtomicBool,
    stop_notify: tokio::sync::Notify,
}

impl HeartbeatMonitor {
    const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
    const DEFAULT_EXECUTION_STALL_TIMEOUT: Duration = Duration::from_secs(300);
    const DEFAULT_DAEMON_DISCONNECT_GRACE: Duration = Duration::from_secs(120);

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
            daemon_disconnect_grace: Self::DEFAULT_DAEMON_DISCONNECT_GRACE,
            disconnect_observed: Mutex::new(HashMap::new()),
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

    pub fn with_daemon_disconnect_grace(mut self, daemon_disconnect_grace: Duration) -> Self {
        self.daemon_disconnect_grace = daemon_disconnect_grace;
        self
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(
            async move {
                tracing::info!(
                    check_interval_seconds = self.check_interval.as_secs(),
                    "heartbeat monitor started"
                );
                while !self.is_stopped() {
                    if let Err(error) = self.check_timeouts().await {
                        tracing::warn!(%error, "heartbeat monitor check failed");
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(self.check_interval) => {}
                        _ = self.stop_notify.notified() => {}
                    }
                }
                tracing::info!("heartbeat monitor stopped");
            }
            .instrument(tracing::info_span!("heartbeat.monitor")),
        )
    }

    #[tracing::instrument(skip(self))]
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.stop_notify.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    #[tracing::instrument(skip(self))]
    pub async fn check_once(&self) -> Result<u64> {
        let agents = self.list_busy_agents().await?;
        let mut timed_out = 0;

        for agent in agents {
            if !agent_timed_out(&agent) {
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
                    publish_task_status_event(&self.db, &self.event_bus, &outcome.task).await;
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

        if timed_out > 0 {
            tracing::info!(
                timed_out_agents = timed_out,
                "heartbeat monitor detected timed out agents"
            );
        }
        renew_workspace_leases(&self.db).await?;
        let progress_warnings = self.check_stale_progress().await?;
        let stalled = self.check_stalled_executions().await?;
        let disconnected = self.check_disconnected_daemon_executions().await?;
        let expired = expire_workspace_leases(
            &self.db,
            &self.event_bus,
            self.task_executor.as_deref(),
            self.task_service.as_deref(),
            true,
        )
        .await?;
        Ok(timed_out + progress_warnings + stalled + disconnected + expired)
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
            let outcome = ExecutionRepo::terminalize(
                &*self.db,
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
            )
            .await?;

            let ExecutionTerminalOutcome::Committed {
                execution: updated, ..
            } = outcome
            else {
                // A runner, cancellation, or another monitor won the CAS.
                // Its terminal event, lease disposition, and Task cascade are
                // authoritative; this stale observation must be inert.
                continue;
            };

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

            if stalled_execution_should_block_task(&updated) {
                if let Some(task_service) = self.task_service.as_ref() {
                    if let Err(error) = task_service.annotate_executor_failure_block(&updated).await
                    {
                        tracing::warn!(
                            execution_id = %updated.id,
                            task_id = %updated.task_id,
                            %error,
                            "failed to cascade expired execution"
                        );
                    }
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
            let ExecutionProgressWarningOutcome::Committed { event, .. } = outcome else {
                continue;
            };
            DomainEventService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus))
                .publish_committed(&event);
            warned += 1;
        }

        Ok(warned)
    }

    async fn check_disconnected_daemon_executions(&self) -> Result<u64> {
        let Some(daemon_connections) = self.daemon_connections.as_ref() else {
            return Ok(0);
        };

        let running = ExecutionRepo::list_running(&*self.db).await?;
        let mut running_ids = HashMap::new();
        for execution in &running {
            running_ids.insert(execution.id.clone(), ());
        }

        {
            let mut observed = self
                .disconnect_observed
                .lock()
                .expect("disconnect observation lock");
            observed.retain(|execution_id, _| running_ids.contains_key(execution_id));
        }

        let mut disconnected = 0_u64;
        let now = Instant::now();

        for execution in running {
            let Some((daemon_id, daemon)) = resolve_execution_daemon(&self.db, &execution).await?
            else {
                continue;
            };
            if is_embedded_daemon_machine(&daemon.machine_id) {
                continue;
            }
            if daemon_connections.is_connected(&daemon_id) {
                self.disconnect_observed
                    .lock()
                    .expect("disconnect observation lock")
                    .remove(&execution.id);
                continue;
            }

            let first_observed = {
                let mut observed = self
                    .disconnect_observed
                    .lock()
                    .expect("disconnect observation lock");
                observed
                    .entry(execution.id.clone())
                    .or_insert_with(|| now)
                    .to_owned()
            };
            if now.duration_since(first_observed) < self.daemon_disconnect_grace {
                continue;
            }

            let updated = fail_execution_daemon_disconnected(
                &self.db,
                &self.event_bus,
                self.task_service.as_deref(),
                FailDaemonDisconnectedExecution {
                    execution: &execution,
                    daemon_id: &daemon_id,
                    error_message: format!("Remote daemon {daemon_id} disconnected"),
                    stopped_by: &api_types::Actor::system(
                        api_types::SystemComponent::HeartbeatMonitor,
                    )
                    .display(),
                    reconciliation_reason: "daemon_disconnected",
                },
            )
            .await?;
            self.disconnect_observed
                .lock()
                .expect("disconnect observation lock")
                .remove(&execution.id);
            if updated.is_some() {
                disconnected += 1;
            }
        }

        if disconnected > 0 {
            tracing::info!(
                disconnected_executions = disconnected,
                "heartbeat monitor interrupted executions on disconnected daemons"
            );
        }
        Ok(disconnected)
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
        WorkspaceLeaseRepo::expire(db, &now_rfc3339(), 500).await?
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
        let outcome = match ExecutionRepo::terminalize(
            db,
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
        )
        .await
        {
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
            if let Err(error) = task_service.annotate_executor_failure_block(&updated).await {
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

async fn recover_task(
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
    let cancelled = cancel_running_executions_for_recovery(
        db,
        &task.id,
        stop_reason.clone(),
        stopped_by,
        ResumePolicy::Manual,
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
        });
    }

    let mut has_resumable_execution = false;
    let should_auto_resume = stop_reason == StopReason::CrashRecovery;
    for execution in cancelled
        .iter()
        .filter(|execution| execution.agent_session_id.is_some())
    {
        has_resumable_execution = true;
        if should_auto_resume {
            ExecutionRepo::update(
                db,
                UpdateExecution {
                    id: execution.execution_id.clone(),
                    status: None,
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: Some(Some(ResumePolicy::Auto)),
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
                    updated_at: now_rfc3339(),
                },
            )
            .await?;
        }
    }
    if has_resumable_execution {
        tracing::warn!(
            task_id = %task.id,
            execution_count = cancelled.len(),
            "recovered task left in current state because a resumable execution was cancelled"
        );
        return Ok(RecoverTaskOutcome {
            task,
            annotated: false,
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
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    let task = TaskRepo::update_status(
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
    .await?;

    Ok(RecoverTaskOutcome {
        task,
        annotated: true,
    })
}

async fn sweep_stale_recovery_annotations(
    db: &Arc<SqliteDb>,
    event_bus: &Arc<EventBus>,
) -> Result<u64> {
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

                let updated = TaskRepo::update_status(
                    db.as_ref(),
                    UpdateTaskStatus {
                        id: task.id.clone(),
                        expected_version: task.version,
                        status: task.status.clone(),
                        assignee_id: None,
                        error_annotation: Some(None),
                        blocked_json: None,
                        failed_json: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                publish_task_status_event(db, event_bus, &updated).await;
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

async fn publish_task_status_event(db: &Arc<SqliteDb>, event_bus: &Arc<EventBus>, task: &Task) {
    let service = DomainEventService::new(Arc::clone(db), Arc::clone(event_bus));
    let dedupe_key = format!("task-status-update:{}:{}", task.id, task.version);
    if let Err(error) = service.publish_by_dedupe(&dedupe_key).await {
        tracing::warn!(task_id = %task.id, %error, "failed to mirror task status domain event");
    }
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
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    let mut cancelled = Vec::new();
    let now = now_rfc3339();
    for execution in page.items {
        if execution.status != ExecutionStatus::Running {
            continue;
        }
        if preserve_healthy_owner && execution_owner_lease_is_healthy(&execution, &now) {
            // Legacy AgentStatus heartbeat timeout is not authoritative for
            // an attempt that still has a live execution owner lease. The
            // expiry/deadline monitor remains responsible for this execution.
            continue;
        }
        let outcome = ExecutionRepo::terminalize(
            db,
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
        )
        .await?;
        if let ExecutionTerminalOutcome::Committed {
            execution: cancelled_execution,
            ..
        } = outcome
        {
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
        .is_some_and(|hard_deadline_at| rfc3339_is_after(hard_deadline_at, now))
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

pub(crate) async fn resolve_execution_daemon(
    db: &SqliteDb,
    execution: &Execution,
) -> Result<Option<(String, Daemon)>> {
    let Some(agent_id) = execution.agent_id.as_deref() else {
        return Ok(None);
    };
    let Some(agent) = AgentRepo::get_by_id(db, agent_id).await? else {
        return Ok(None);
    };
    let Some(daemon_id) = agent.daemon_id else {
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
    let outcome = ExecutionRepo::terminalize(
        db,
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
    )
    .await?;

    let ExecutionTerminalOutcome::Committed {
        execution: updated, ..
    } = outcome
    else {
        // Another terminal caller won the version/owner CAS. Its durable
        // event, lease disposition, and Task cascade are authoritative; this
        // stale daemon observation must not emit a second failure.
        return Ok(None);
    };

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

    if stalled_execution_should_block_task(&updated) {
        if let Some(task_service) = task_service {
            if let Err(error) = task_service.annotate_executor_failure_block(&updated).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to cascade daemon-disconnected execution"
                );
            }
        }
    }

    Ok(Some(updated))
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
    let executions = ExecutionRepo::list_running_for_daemon_not_in(
        db,
        &daemon.id,
        &created_before,
        active_execution_ids,
    )
    .await?;

    let mut interrupted = 0_u64;
    for execution in executions {
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
mod tests {
    use super::*;
    use crate::{
        daemon_service::{DaemonReportInput, DaemonService, DetectedCliInput},
        daemon_transport::{DaemonConnection, DaemonConnectionRegistry},
        workflow::default_roles,
        TaskService,
    };
    use db::{
        create_sqlite_pool, new_uuid_v4, run_migrations, AgentContextScopeRepo, AgentProfileRepo,
        AgentSession, CreateAgent, CreateAgentContextScope, CreateAgentProfile, CreateAgentSession,
        CreateExecution, CreateProject, CreateRepo, CreateTask, CreateTaskRoleAssignment,
        CreateWorkspaceLease, DaemonRepo, DaemonStatus, DomainEventRepo, RepoRepo,
        TaskRoleAssignmentRepo, TaskStatus, UpdateProject, UpsertDaemon,
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
                usage: None,
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
                remote_url: "https://example.com/forge.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
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
        repo_id: String,
        status: TaskStatus,
        agent_id: Option<String>,
    ) -> Task {
        seed_task_with_assignee(db, project_id, repo_id, status, "agent", agent_id).await
    }

    async fn seed_task_with_assignee(
        db: &SqliteDb,
        project_id: String,
        repo_id: String,
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
                repo_id: Some(repo_id),
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
                    r#"{"executor_type":"shell","config":{}}"#.to_owned(),
                ),
                workspace_id: None,
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let in_progress = seed_task(
            &db,
            project_id.clone(),
            repo_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id),
        )
        .await;
        let todo = seed_task(&db, project_id, repo_id, "todo".to_owned(), None).await;

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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let user_task = seed_task_with_assignee(
            &db,
            project_id,
            repo_id,
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
    async fn crash_recovery_blocks_interrupted_entry_barriers() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, project_id, repo_id, "review".to_owned(), None).await;
        TaskRepo::set_entry_barrier(
            &*db,
            &task.id,
            task.version,
            Some(
                r#"{"state":"review","status":"running","started_at":"2026-04-28T00:00:00Z"}"#
                    .to_owned(),
            ),
            &now_rfc3339(),
        )
        .await
        .expect("barrier sets");

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");

        assert_eq!(recovered, 1);
        let recovered_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(recovered_task.status, "review");
        let barrier: Value =
            serde_json::from_str(recovered_task.entry_barrier_json.as_deref().unwrap()).unwrap();
        assert_eq!(barrier["state"], "review");
        assert_eq!(barrier["status"], "blocked");
        assert_eq!(
            barrier["blocking_reason"],
            "crash recovery: before_enter was interrupted"
        );
    }

    #[tokio::test]
    async fn heartbeat_monitor_times_out_busy_agents_and_recovers_tasks() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let mut rx = event_bus.subscribe();
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let old_heartbeat = "1970-01-01T00:00:00+00:00".to_owned();
        let agent = seed_agent(&db, AgentStatus::Busy, Some(old_heartbeat.clone())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let _execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;

        let monitor = HeartbeatMonitor::with_check_interval(
            Arc::clone(&db),
            event_bus,
            Duration::from_millis(5),
        );
        let timed_out = monitor.check_once().await.expect("monitor runs");
        assert_eq!(timed_out, 1);

        let updated_agent = AgentRepo::get_by_id(&*db, &agent.id)
            .await
            .expect("agent loads")
            .expect("agent exists");
        assert_eq!(updated_agent.status, AgentStatus::Error);

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
        assert_eq!(
            annotation["recovery_actions"],
            json!(["reexecute", "reset_to_initial", "cancel_task"])
        );

        let mut event_types = Vec::new();
        let mut recovered_event_id = None;
        for _ in 0..3 {
            let event = rx.recv().await.expect("recovery event receives");
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(
            &db,
            AgentStatus::Busy,
            Some("1970-01-01T00:00:00+00:00".to_owned()),
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
                hard_deadline_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
                    hard_deadline_at: (Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),
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
        assert_eq!(metadata["execution_retry_count"], 1);
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
    async fn heartbeat_monitor_warns_on_live_stale_progress_without_terminalizing() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(32));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
                hard_deadline_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
                hard_deadline_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
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
    async fn heartbeat_monitor_start_stops() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let monitor = Arc::new(HeartbeatMonitor::with_check_interval(
            db,
            event_bus,
            Duration::from_millis(1),
        ));

        let handle = Arc::clone(&monitor).start();
        monitor.stop();
        handle.await.expect("monitor task joins");
        assert!(monitor.is_stopped());
    }

    #[tokio::test]
    async fn recovery_monitors_all_active_states() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
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
            repo_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let task_merge_failed = seed_task(
            &db,
            project_id,
            repo_id,
            "merge_failed".to_owned(),
            Some(agent.id),
        )
        .await;

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
    async fn crash_recovery_cancels_running_executions() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;

        let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
        let recovered = recovery.run_recovery().await.expect("recovery runs");
        assert_eq!(recovered, 1);

        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated_task.status, "in_progress");
        let annotation: Value =
            serde_json::from_str(updated_task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["blocked_execution_id"], execution.id);
        assert_eq!(annotation["artifact"]["kind"], "execution");
        assert_eq!(annotation["artifact"]["id"], execution.id);

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Cancelled);
        assert!(updated.error.as_deref().unwrap().contains("Recovered"));
        assert_eq!(updated.resume_policy, Some(ResumePolicy::Manual));
    }

    #[tokio::test]
    async fn cancel_running_executions_returns_cancelled_execution_metadata() {
        let db = Arc::new(sqlite_db().await);
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
            repo_id.clone(),
            "in_progress".to_owned(),
            Some(agent.id.clone()),
        )
        .await;

        // Leave an active lease for a newer successor attempt. The successor
        // is made terminal through a direct fixture update so the stale
        // recovery calls below only race on the older running attempt; the
        // lease remains the protected task-scoped grant.
        let successor = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
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

        let stale_execution =
            seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
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
        assert_eq!(
            [first.annotated, second.annotated]
                .into_iter()
                .filter(|annotated| *annotated)
                .count(),
            1,
            "only the terminal-CAS winner cascades Task recovery"
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
            "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, project_id, repo_id, "in_progress".to_owned(), None).await;
        stamp_recovery_annotation(&db, &task, None).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 1);

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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let task = seed_task(&db, project_id, repo_id, "in_progress".to_owned(), None).await;
        stamp_recovery_annotation(&db, &task, Some("missing-execution-id")).await;

        let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
            .run_recovery()
            .await
            .expect("recovery runs");
        assert_eq!(recovered, 1);

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
    async fn heartbeat_monitor_fails_remote_execution_when_daemon_stays_disconnected() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) =
            seed_agent_with_daemon(&db, "remote-machine-a", AgentStatus::Idle).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_daemon_connections(Arc::clone(&registry))
            .with_daemon_disconnect_grace(Duration::from_millis(1));

        monitor.check_once().await.expect("first check");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let interrupted = monitor.check_once().await.expect("second check");
        assert_eq!(interrupted, 1);

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Failed);
        assert_eq!(updated.stop_reason, Some(StopReason::DaemonDisconnected));
    }

    #[tokio::test]
    async fn heartbeat_monitor_leaves_remote_execution_alone_within_disconnect_grace() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) =
            seed_agent_with_daemon(&db, "remote-machine-b", AgentStatus::Idle).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_daemon_connections(Arc::clone(&registry))
            .with_daemon_disconnect_grace(Duration::from_secs(120));

        let interrupted = monitor.check_once().await.expect("monitor checks");
        assert_eq!(interrupted, 0);

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Running);
    }

    #[tokio::test]
    async fn heartbeat_monitor_clears_disconnect_tracking_when_daemon_reconnects() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-c", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_daemon_connections(Arc::clone(&registry))
            .with_daemon_disconnect_grace(Duration::from_secs(120));

        monitor.check_once().await.expect("first check");
        let (connection, _rx) = DaemonConnection::new(daemon_id.clone());
        registry.register(daemon_id, connection);
        let interrupted = monitor.check_once().await.expect("second check");
        assert_eq!(interrupted, 0);

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Running);
    }

    #[tokio::test]
    async fn heartbeat_monitor_skips_embedded_daemon_executions_for_disconnect_check() {
        let db = Arc::new(sqlite_db().await);
        let event_bus = Arc::new(EventBus::new(16));
        let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) = seed_agent_with_daemon(
            &db,
            &crate::embedded_daemon::embedded_machine_id(),
            AgentStatus::Idle,
        )
        .await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
            .with_daemon_connections(Arc::clone(&registry))
            .with_daemon_disconnect_grace(Duration::from_millis(1));

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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, _) =
            seed_agent_with_daemon(&db, "remote-machine-d", AgentStatus::Idle).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
                    hard_deadline_at: (Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
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
            repo_id,
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
                    hard_deadline_at: (Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-report", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-fresh", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            repo_id,
            "in_progress".to_owned(),
            Some(agent_id.clone()),
        )
        .await;
        let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

        service
            .ingest_report(
                &daemon_id,
                DaemonReportInput {
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let (agent_id, agent) =
            seed_agent_with_daemon(&db, "remote-machine-none", AgentStatus::Idle).await;
        let daemon_id = agent.daemon_id.clone().expect("daemon id");
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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
        let (project_id, repo_id) = seed_project_repo(&db).await;
        let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
        let task = seed_task(
            &db,
            project_id,
            repo_id,
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

        let updated = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated.error_annotation, None);
    }
}
