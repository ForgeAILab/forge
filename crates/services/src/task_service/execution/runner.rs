use super::*;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const EXECUTION_LOG_BATCH_MAX_ENTRIES: usize = 50;
const EXECUTION_LOG_BATCH_MAX_WAIT: Duration = Duration::from_millis(500);
const EMBEDDED_EXECUTION_LEASE_SECONDS: i64 = 60;
const EMBEDDED_EXECUTION_HEARTBEAT_SECONDS: u64 = 20;
/// A bounded fallback is required for snapshots that predate explicit
/// execution-time policy.  Provider/profile configuration may choose a
/// shorter window, but no embedded execution is admitted without a deadline.
const DEFAULT_EXECUTION_HARD_DEADLINE_SECONDS: u64 = 30 * 60;
const MAX_EXECUTION_HARD_DEADLINE_SECONDS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddedLeaseSignal {
    OwnerLost,
    HardDeadline,
}

pub(crate) fn execution_deadline_seconds(snapshot: &Value) -> u64 {
    const KEYS: &[&str] = &[
        "hard_deadline_seconds",
        "execution_hard_deadline_seconds",
        "deadline_seconds",
        "execution_deadline_seconds",
        "max_duration_seconds",
        "max_execution_seconds",
        "max_execution_duration_seconds",
        "timeout_seconds",
    ];

    let mut candidates = Vec::new();
    for object in [
        snapshot,
        snapshot.get("config").unwrap_or(&Value::Null),
        snapshot.get("capabilities").unwrap_or(&Value::Null),
        snapshot.get("profile").unwrap_or(&Value::Null),
        snapshot.get("policy").unwrap_or(&Value::Null),
    ] {
        for key in KEYS {
            if let Some(seconds) = object.get(*key).and_then(Value::as_u64) {
                if seconds > 0 {
                    candidates.push(seconds);
                }
            }
        }
    }
    candidates
        .into_iter()
        .min()
        .unwrap_or(DEFAULT_EXECUTION_HARD_DEADLINE_SECONDS)
        .clamp(1, MAX_EXECUTION_HARD_DEADLINE_SECONDS)
}

pub(crate) fn rfc3339_after(now: &str, seconds: i64) -> String {
    DateTime::parse_from_rfc3339(now)
        .map(|value| (value + ChronoDuration::seconds(seconds)).to_rfc3339())
        .unwrap_or_else(|_| (Utc::now() + ChronoDuration::seconds(seconds)).to_rfc3339())
}

pub(crate) fn bounded_lease_expiry(now: &str, hard_deadline_at: &str) -> String {
    let proposed = rfc3339_after(now, EMBEDDED_EXECUTION_LEASE_SECONDS);
    match (
        DateTime::parse_from_rfc3339(&proposed),
        DateTime::parse_from_rfc3339(hard_deadline_at),
    ) {
        (Ok(proposed), Ok(deadline)) => proposed.min(deadline).to_rfc3339(),
        _ => proposed,
    }
}

fn late_terminal_diagnostic(
    execution_id: &str,
    task_id: &str,
    project_id: &str,
    attempted_status: &ExecutionStatus,
    attempted_error: Option<&str>,
    current: &Execution,
) -> db::CreateDomainEvent {
    let dedupe_key = format!(
        "execution-late-terminal-rejected:{}:{}:{}",
        execution_id, current.execution_version, attempted_status
    );
    db::CreateDomainEvent {
        id: db::new_uuid_v4(),
        event_type: "execution.late_terminal_rejected".to_owned(),
        entity_type: "task".to_owned(),
        entity_id: task_id.to_owned(),
        actor_type: "system".to_owned(),
        actor_id: Some("embedded-runner".to_owned()),
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        correlation_id: execution_id.to_owned(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(dedupe_key),
        payload_json: json!({
            "execution_id": execution_id,
            "attempted_status": attempted_status.to_string(),
            "attempted_error": attempted_error
                .map(|error| error.chars().take(500).collect::<String>()),
            "current_status": current.status.to_string(),
            "current_execution_version": current.execution_version,
        })
        .to_string(),
        created_at: now_rfc3339(),
    }
}

async fn append_late_terminal_diagnostic(
    db: &SqliteDb,
    execution_id: &str,
    task_id: &str,
    project_id: &str,
    attempted_status: &ExecutionStatus,
    attempted_error: Option<&str>,
    current: &Execution,
) {
    let diagnostic = late_terminal_diagnostic(
        execution_id,
        task_id,
        project_id,
        attempted_status,
        attempted_error,
        current,
    );
    if let Err(error) = db::DomainEventRepo::append_event(db, diagnostic).await {
        tracing::warn!(
            execution_id = %execution_id,
            %error,
            "failed to persist late embedded terminal diagnostic"
        );
    }
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

async fn embedded_execution_heartbeat(
    lease: Arc<crate::embedded_task_executor::EmbeddedExecutionLease>,
    hard_deadline_at: String,
    stop: CancellationToken,
    signal_tx: mpsc::UnboundedSender<EmbeddedLeaseSignal>,
) {
    embedded_execution_heartbeat_with_clock(
        lease,
        hard_deadline_at,
        stop,
        signal_tx,
        Arc::new(now_rfc3339),
    )
    .await;
}

async fn embedded_execution_heartbeat_with_clock(
    lease: Arc<crate::embedded_task_executor::EmbeddedExecutionLease>,
    hard_deadline_at: String,
    stop: CancellationToken,
    signal_tx: mpsc::UnboundedSender<EmbeddedLeaseSignal>,
    now: Arc<dyn Fn() -> String + Send + Sync>,
) {
    let mut ticker =
        tokio::time::interval(Duration::from_secs(EMBEDDED_EXECUTION_HEARTBEAT_SECONDS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = ticker.tick() => {
                let now = now();
                let lease_expires_at = bounded_lease_expiry(&now, &hard_deadline_at);
                if lease_expires_at <= now {
                    let _ = signal_tx.send(EmbeddedLeaseSignal::HardDeadline);
                    break;
                }
                match lease.renew(lease_expires_at, now).await {
                    Ok(db::ExecutionLeaseMutation::Updated(_)) => {}
                    Ok(db::ExecutionLeaseMutation::HardDeadline { .. }) => {
                        let _ = signal_tx.send(EmbeddedLeaseSignal::HardDeadline);
                        break;
                    }
                    Ok(db::ExecutionLeaseMutation::Concurrent { .. }) => {
                        let _ = signal_tx.send(EmbeddedLeaseSignal::OwnerLost);
                        break;
                    }
                    Err(error) => {
                        // A transient database failure is not proof of owner
                        // death. The expiry monitor remains the authority if
                        // renewal cannot recover before the lease expires.
                        tracing::warn!(%error, "embedded execution lease renewal failed");
                    }
                }
            }
        }
    }
}

impl TaskService {
    pub async fn start_execution(
        &self,
        execution_id: impl Into<String>,
    ) -> Result<api_types::ExecutionStartResult> {
        let execution_id = execution_id.into();
        validate_required("execution_id", &execution_id)?;
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(
                "only running executions can be started",
            ));
        }
        // Remote and local adapters both require the scheduler-issued lease;
        // checking at this boundary closes the gap between execution-row
        // creation and adapter launch/recovery.
        if let Err(error) = self.verify_execution_workspace_authority(&execution).await {
            // Authority can be revoked or superseded between execution-row
            // creation and this dispatch attempt.  Stop the attempt and
            // revoke any remaining grant before surfacing the denial.
            let failure_message = error.to_string();
            if let Err(mark_error) = self
                .fail_execution_before_dispatch(&execution.id, failure_message)
                .await
            {
                tracing::warn!(
                    execution_id = %execution.id,
                    %mark_error,
                    "failed to terminalize execution after initial WorkspaceLease verification failure"
                );
            }
            return Err(error);
        }

        let result = async {
            let agent = match execution.agent_id.as_deref() {
                Some(agent_id) => Some(
                    AgentRepo::get_by_id(&*self.db, agent_id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?,
                ),
                None => None,
            };
            let provider = self
                .execution_provider_for_agent(agent.as_ref(), &execution.id)
                .await?;
            let params = self.execution_start_params(&execution).await?;
            if let Some(lease_owner) = provider.execution_lease_owner() {
                let now = Utc::now();
                let preclaimed = execution.lease_owner.as_deref() == Some(lease_owner.as_str())
                    && execution
                        .lease_expires_at
                        .as_deref()
                        .and_then(parse_rfc3339)
                        .is_some_and(|expires_at| expires_at > now)
                    && execution
                        .hard_deadline_at
                        .as_deref()
                        .and_then(parse_rfc3339)
                        .is_none_or(|deadline| deadline > now);
                if !preclaimed {
                    let lease_claimed_at = now_rfc3339();
                    let hard_deadline_at = rfc3339_after(
                        &lease_claimed_at,
                        i64::try_from(execution_deadline_seconds(&params.executor_config))
                            .unwrap_or(i64::MAX),
                    );
                    let lease_expires_at =
                        bounded_lease_expiry(&lease_claimed_at, &hard_deadline_at);
                    match ExecutionRepo::claim_lease(
                        &*self.db,
                        db::ClaimExecutionLease {
                            execution_id: execution.id.clone(),
                            expected_version: execution.execution_version,
                            owner: lease_owner,
                            lease_expires_at,
                            hard_deadline_at,
                            now: lease_claimed_at,
                        },
                    )
                    .await?
                    {
                        db::ExecutionLeaseMutation::Updated(_) => {}
                        db::ExecutionLeaseMutation::Concurrent { current }
                        | db::ExecutionLeaseMutation::HardDeadline { current } => {
                            tracing::info!(
                                execution_id = %execution.id,
                                status = ?current.as_ref().map(|execution| &execution.status),
                                "remote execution dispatch lost its lease claim"
                            );
                            return Ok(api_types::ExecutionStartResult {
                                execution_id: execution.id.clone(),
                                accepted: false,
                            });
                        }
                    }
                }
            }
            provider.start(params).await
        }
        .await;

        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                let failure_message = error.to_string();
                if let Err(mark_error) = self
                    .fail_execution_before_dispatch(&execution.id, failure_message)
                    .await
                {
                    tracing::warn!(
                        execution_id = %execution.id,
                        %mark_error,
                        "failed to mark execution failed after dispatch start error"
                    );
                }
                Err(error)
            }
        }
    }

    pub async fn run_execution(
        &self,
        execution_id: impl Into<String>,
        executor: &dyn TaskExecutor,
    ) -> Result<db::Execution> {
        let execution_id = execution_id.into();
        validate_required("execution_id", &execution_id)?;
        tracing::info!(%execution_id, "execution dispatch starting");
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution.status != ExecutionStatus::Running {
            return Err(ServiceError::invalid_operation(
                "only running executions can be executed",
            ));
        }
        if let Some(failed) = self
            .wait_for_agent_active_before_dispatch(&execution)
            .await?
        {
            return Ok(failed);
        }
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let workspace_id = execution
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("execution missing workspace_id"))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        // A Task-row mutation since lease issuance (role handoff, metadata
        // clear, concurrent transition) fails the exact-match verify; recover
        // once through the normal issuance path instead of hard-failing, and
        // keep working against the fresh Task row.
        let task = self
            .verify_or_reissue_active_workspace_lease(
                task,
                &workspace,
                &execution.role,
                execution.agent_id.as_deref(),
                &execution.id,
            )
            .await?;
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("execution missing executor config snapshot")
            })?;
        let mut agent_config = parse_json_value("executor config snapshot", snapshot)?;
        if read_only_execution_role(&execution.role)
            || matches!(task.task_type.as_str(), "planning_task" | "discovery")
        {
            executors::mark_worktree_read_only(&mut agent_config);
        }
        if agent_config.get("executor_type").and_then(Value::as_str) == Some("embedded") {
            crate::embedded_task_executor::set_task_role_marker(&mut agent_config, &execution.role);
        }
        // Provider-entry-backed harness agents get their API key injected into
        // the in-memory snapshot only; the stored snapshot never holds it.
        if let Some(credential_env) = self.credential_env.as_ref() {
            credential_env
                .inject_provider_env(&mut agent_config)
                .await?;
        }
        let max_turns = self.resolve_max_turns(&task).await?;
        let logs_path = self
            .resolve_execution_logs_path(&execution, &task, &workspace, &execution_id)
            .await?;
        if let Some(parent) = std::path::Path::new(&logs_path).parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ServiceError::invalid_operation(format!("failed to create log directory: {error}"))
            })?;
        }

        let launch_activity_at = now_rfc3339();
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution_id.clone(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: Some(Some(launch_activity_at)),
                summary: None,
                logs_path: Some(Some(logs_path.clone())),
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)?;

        if let Some(terminal_activity) = self.terminal_activity.as_ref() {
            if terminal_activity
                .workspace_has_active_terminal(workspace_id)
                .await
            {
                return Err(ServiceError::TerminalActiveExecution {
                    workspace_id: workspace_id.to_owned(),
                });
            }
        }
        let _exec_lock_guard = if let Some(locks) = self.workspace_exec_locks.as_ref() {
            if let Some(guard) = locks.try_acquire(workspace_id) {
                if let Some(terminal_activity) = self.terminal_activity.as_ref() {
                    if terminal_activity
                        .workspace_has_active_terminal(workspace_id)
                        .await
                    {
                        return Err(ServiceError::TerminalActiveExecution {
                            workspace_id: workspace_id.to_owned(),
                        });
                    }
                }
                Some(guard)
            } else {
                if let Some(terminal_activity) = self.terminal_activity.as_ref() {
                    if terminal_activity
                        .workspace_has_active_terminal(workspace_id)
                        .await
                    {
                        return Err(ServiceError::TerminalActiveExecution {
                            workspace_id: workspace_id.to_owned(),
                        });
                    }
                }
                self.event_bus.publish(events::ForgeEvent {
                    event_type: "workspace.execution_waiting".to_owned(),
                    entity_id: workspace_id.to_owned(),
                    timestamp: events::event_timestamp(),
                    context: events::EventContext::WorkspaceExecutionWaiting {
                        workspace_id: workspace_id.to_owned(),
                        task_id: task.id.clone(),
                    },
                });
                Some(locks.acquire(workspace_id).await)
            }
        } else {
            None
        };

        let execution_before_launch = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if execution_before_launch.status != ExecutionStatus::Running {
            tracing::info!(
                %execution_id,
                status = %execution_before_launch.status,
                "execution dispatch stopped before adapter launch"
            );
            return Ok(execution_before_launch);
        }

        // Workspace lock acquisition and pre-launch preparation can outlive
        // a lease or a baseline supersession.  Re-read the execution/task
        // bindings and acknowledge the lease immediately before handing
        // control to an executor.  A stale lease left behind by a Task-row
        // mutation is reissued once through the normal issuance path.
        if let Err(error) = self
            .verify_or_reissue_execution_workspace_authority(&execution_before_launch)
            .await
        {
            let failure_message = error.to_string();
            if let Err(mark_error) = self
                .fail_execution_before_dispatch(&execution_before_launch.id, failure_message)
                .await
            {
                tracing::warn!(
                    execution_id = %execution_before_launch.id,
                    %mark_error,
                    "failed to terminalize execution after final WorkspaceLease verification failure"
                );
            }
            return Err(error);
        }

        // The scheduler owns the execution lease. Creation normally installs
        // the deterministic embedded owner atomically; older/ownerless rows
        // are claimed here immediately before launch. Every heartbeat,
        // progress update, and terminal CAS then presents the same tuple.
        let deterministic_owner = format!("embedded-execution:{}", execution_before_launch.id);
        let lease_claimed_at = now_rfc3339();
        let requested_hard_deadline_at = rfc3339_after(
            &lease_claimed_at,
            i64::try_from(execution_deadline_seconds(&agent_config)).unwrap_or(i64::MAX),
        );
        let lease_execution = if execution_before_launch.lease_owner.as_deref()
            == Some(deterministic_owner.as_str())
            && execution_before_launch.hard_deadline_at.is_some()
            && execution_before_launch
                .lease_expires_at
                .as_deref()
                .is_some_and(|expires_at| expires_at > lease_claimed_at.as_str())
        {
            execution_before_launch.clone()
        } else {
            match ExecutionRepo::claim_lease(
                &*self.db,
                db::ClaimExecutionLease {
                    execution_id: execution_before_launch.id.clone(),
                    expected_version: execution_before_launch.execution_version,
                    owner: deterministic_owner,
                    lease_expires_at: bounded_lease_expiry(
                        &lease_claimed_at,
                        &requested_hard_deadline_at,
                    ),
                    hard_deadline_at: requested_hard_deadline_at,
                    now: lease_claimed_at,
                },
            )
            .await?
            {
                db::ExecutionLeaseMutation::Updated(execution) => execution,
                db::ExecutionLeaseMutation::Concurrent { current }
                | db::ExecutionLeaseMutation::HardDeadline { current } => {
                    let current = current.ok_or_else(|| {
                        ServiceError::not_found("execution", execution_before_launch.id.clone())
                    })?;
                    tracing::info!(
                        execution_id = %current.id,
                        status = %current.status,
                        "execution dispatch lost lease claim before adapter launch"
                    );
                    return Ok(current);
                }
            }
        };
        let hard_deadline_at = lease_execution.hard_deadline_at.clone().ok_or_else(|| {
            ServiceError::invalid_operation("execution lease claim returned no hard deadline")
        })?;
        let lease_owner = lease_execution
            .lease_owner
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("execution lease has no owner"))?;
        let lease = Arc::new(crate::embedded_task_executor::EmbeddedExecutionLease::new(
            Arc::clone(&self.db),
            lease_execution.id.clone(),
            lease_owner,
            lease_execution.execution_version,
        ));
        crate::embedded_task_executor::register_execution_lease(
            execution_before_launch.id.clone(),
            Arc::clone(&lease),
        );

        let description = execution_description(&execution, &task, &agent_config);

        let (log_tx, mut log_rx) = tokio::sync::mpsc::unbounded_channel::<executors::LogEntry>();
        let max_turns_exceeded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let assistant_turn_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let usage_provider = super::usage_provider_from_agent_config(&agent_config);
        let usage_model_fallback = usage_model_fallback(&agent_config);

        // Spawn a task that forwards log entries to the event bus
        let event_bus = self.event_bus.clone();
        let progress_lease = Arc::clone(&lease);
        let cancellation_executor = self.task_executor.clone();
        let sse_execution_id = execution_id.clone();
        let sse_task_id = task.id.clone();
        let log_max_turns = max_turns;
        let log_max_turns_exceeded = Arc::clone(&max_turns_exceeded);
        let log_assistant_turn_count = Arc::clone(&assistant_turn_count);
        let log_task = tokio::spawn(async move {
            let mut assistant_turn_count = 0_u32;
            let mut pending_batch: Vec<executors::LogEntry> = Vec::new();
            let mut flush_deadline: Option<tokio::time::Instant> = None;

            let flush_batch = |batch: &mut Vec<executors::LogEntry>| {
                if batch.is_empty() {
                    return;
                }
                let logs = batch
                    .iter()
                    .map(|entry| serde_json::to_value(entry).unwrap_or_default())
                    .collect::<Vec<_>>();
                let first_log = logs.first().cloned().unwrap_or_default();
                let timestamp = batch
                    .last()
                    .map(|entry| entry.timestamp.clone())
                    .unwrap_or_else(events::event_timestamp);
                event_bus.publish(events::ForgeEvent {
                    event_type: "execution.log".to_owned(),
                    entity_id: sse_execution_id.clone(),
                    timestamp,
                    context: events::EventContext::ExecutionLog {
                        task_id: sse_task_id.clone(),
                        log: first_log,
                        logs: Some(logs),
                    },
                });
                batch.clear();
            };

            loop {
                let next_entry = if let Some(deadline) = flush_deadline {
                    tokio::select! {
                        biased;
                        maybe_entry = log_rx.recv() => maybe_entry,
                        _ = tokio::time::sleep_until(deadline) => {
                            flush_batch(&mut pending_batch);
                            flush_deadline = None;
                            continue;
                        }
                    }
                } else {
                    log_rx.recv().await
                };

                let Some(entry) = next_entry else {
                    flush_batch(&mut pending_batch);
                    break;
                };

                if let Err(error) = progress_lease
                    .record_progress(entry.timestamp.clone(), db::now_rfc3339())
                    .await
                {
                    tracing::debug!(
                        execution_id = %sse_execution_id,
                        %error,
                        "failed to record execution semantic progress"
                    );
                }
                if entry.kind == executors::LogKind::Assistant {
                    assistant_turn_count = assistant_turn_count.saturating_add(1);
                    log_assistant_turn_count
                        .store(assistant_turn_count, std::sync::atomic::Ordering::SeqCst);
                    if let Some(limit) = log_max_turns {
                        if assistant_turn_count >= limit
                            && !log_max_turns_exceeded
                                .swap(true, std::sync::atomic::Ordering::SeqCst)
                        {
                            tracing::warn!(
                                execution_id = %sse_execution_id,
                                assistant_turn_count,
                                max_turns = limit,
                                "execution exceeded max turns"
                            );
                            if let Some(executor) = cancellation_executor.as_ref() {
                                if let Err(error) = executor.cancel(&sse_execution_id).await {
                                    tracing::warn!(
                                        execution_id = %sse_execution_id,
                                        %error,
                                        "failed to cancel execution after max turns"
                                    );
                                }
                            }
                        }
                    }
                }

                pending_batch.push(entry);
                if flush_deadline.is_none() {
                    flush_deadline =
                        Some(tokio::time::Instant::now() + EXECUTION_LOG_BATCH_MAX_WAIT);
                }
                if pending_batch.len() >= EXECUTION_LOG_BATCH_MAX_ENTRIES {
                    flush_batch(&mut pending_batch);
                    flush_deadline = None;
                }
            }
        });

        let read_only_head = if executors::is_worktree_read_only(&agent_config) {
            match git::get_current_sha(std::path::Path::new(&workspace.worktree_path)).await {
                Ok(head) => Some(head),
                Err(error) => {
                    crate::embedded_task_executor::unregister_execution_lease(
                        &execution_id,
                        &lease,
                    );
                    return Err(ServiceError::from(error));
                }
            }
        } else {
            None
        };
        // Baseline for detecting a "narrating" completion that changed
        // nothing. Only workflow-dispatched (the snapshot carries the
        // dispatcher's `dispatch.target_role` metadata), write-capable
        // implementation executions are measured; user-launched/claimed runs
        // and non-git worktrees are exempt.
        //
        // The measurement is the Task branch's own starting point, not this
        // pass's HEAD. A redispatch onto a branch that already carries the
        // implementation -- exactly what the Project Agent does to unstick a
        // stalled Task -- has legitimate verification-only work to do, and
        // failing it for committing nothing would punish the recovery path
        // for succeeding.
        let noop_completion_baseline = if read_only_head.is_none()
            && matches!(
                execution.role.as_str(),
                crate::workflow::default_roles::WORKER | crate::workflow::default_roles::CODER
            )
            && task.task_type == "task"
            && agent_config
                .get("dispatch")
                .and_then(|dispatch| dispatch.get("target_role"))
                .is_some()
        {
            workspace
                .before_sha
                .clone()
                .map(|branch_point| (branch_point, task.status.clone(), task.version))
        } else {
            None
        };
        let heartbeat_stop = CancellationToken::new();
        let (heartbeat_signal_tx, mut heartbeat_signal_rx) = mpsc::unbounded_channel();
        let heartbeat_task = tokio::spawn(embedded_execution_heartbeat(
            Arc::clone(&lease),
            hard_deadline_at.clone(),
            heartbeat_stop.clone(),
            heartbeat_signal_tx,
        ));
        let execution_future = executor.execute(ExecutionContext {
            task_id: task.id.clone(),
            execution_id: execution_id.clone(),
            worktree_path: workspace.worktree_path.clone(),
            description,
            agent_config,
            logs_path: logs_path.clone(),
            heartbeat_interval_seconds: 30,
            max_turns,
            log_sender: Some(log_tx),
        });
        tokio::pin!(execution_future);
        let mut hard_deadline_exceeded = false;
        let mut lease_owner_lost = false;
        let execution_result = tokio::select! {
            result = &mut execution_future => result,
            signal = heartbeat_signal_rx.recv() => {
                if let Some(signal) = signal {
                    match signal {
                        EmbeddedLeaseSignal::HardDeadline => {
                            hard_deadline_exceeded = true;
                            tracing::warn!(
                                execution_id = %execution_id,
                                hard_deadline_at = %hard_deadline_at,
                                "embedded execution reached its hard deadline"
                            );
                        }
                        EmbeddedLeaseSignal::OwnerLost => {
                            lease_owner_lost = true;
                            tracing::warn!(
                                execution_id = %execution_id,
                                "embedded execution lost its owner lease"
                            );
                        }
                    }
                    // The runner owns the executor reference and therefore
                    // performs cancellation after the heartbeat reports the
                    // typed lease decision.
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        executor.cancel(&execution_id),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => tracing::warn!(
                            execution_id = %execution_id,
                            %error,
                            "failed to cancel embedded execution after lease loss"
                        ),
                        Err(_) => tracing::warn!(
                            execution_id = %execution_id,
                            "embedded execution cancellation timed out after lease loss"
                        ),
                    }
                }
                match tokio::time::timeout(Duration::from_secs(5), &mut execution_future).await {
                    Ok(result) => result,
                    Err(_) => Err(executors::ExecutorError::Other(
                        "executor did not stop after the execution lease decision".to_owned(),
                    )),
                }
            }
        };
        heartbeat_stop.cancel();
        if let Err(error) = heartbeat_task.await {
            tracing::debug!(%error, "embedded execution heartbeat task stopped with an error");
        }
        if parse_rfc3339(&hard_deadline_at).is_some_and(|deadline| deadline <= Utc::now()) {
            hard_deadline_exceeded = true;
        }
        crate::embedded_task_executor::unregister_execution_lease(&execution_id, &lease);
        // Drain semantic events before the terminal CAS.  The forwarding task
        // owns the only progress writer, so joining it ensures no late event
        // can race the final owner/version read.
        let mut log_task = log_task;
        match tokio::time::timeout(Duration::from_secs(1), &mut log_task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::debug!(%error, "execution log forwarding task stopped with an error");
            }
            Err(_) => {
                tracing::warn!(
                    execution_id = %execution_id,
                    "execution log forwarding task did not drain after executor completion"
                );
                log_task.abort();
                let _ = log_task.await;
            }
        }
        if lease_owner_lost {
            let current = ExecutionRepo::get_by_id(&*self.db, &execution_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
            let (attempted_status, attempted_error) = match &execution_result {
                Ok(result) => (
                    match &result.status {
                        ExecutionOutcome::Completed => ExecutionStatus::Completed,
                        ExecutionOutcome::Failed => ExecutionStatus::Failed,
                        ExecutionOutcome::Cancelled => ExecutionStatus::Cancelled,
                    },
                    result.error.clone(),
                ),
                Err(error) => (
                    ExecutionStatus::Failed,
                    Some(format!("executor error after lease loss: {error}")),
                ),
            };
            append_late_terminal_diagnostic(
                &self.db,
                &execution_id,
                &task.id,
                &task.project_id,
                &attempted_status,
                attempted_error.as_deref(),
                &current,
            )
            .await;
            tracing::info!(
                execution_id = %execution_id,
                status = %current.status,
                "embedded execution outcome discarded after losing its lease owner"
            );
            return Ok(current);
        }
        let mut discarded_read_only_changes = false;
        let restore_result = if let Some(head) = read_only_head.as_deref() {
            let worktree_path = std::path::Path::new(&workspace.worktree_path);
            // Untracked build output is not authored work. A reviewer that runs
            // the test suite necessarily leaves caches behind, and failing that
            // execution punishes the verification the role exists to perform.
            discarded_read_only_changes = git::has_authored_changes(worktree_path)
                .await
                .unwrap_or(false)
                || git::get_current_sha(worktree_path).await.ok().as_deref() != Some(head);
            git::restore_worktree(worktree_path, head)
                .await
                .map_err(ServiceError::from)
        } else {
            Ok(())
        };
        let mut result = match execution_result {
            Ok(result) => result,
            Err(error) if hard_deadline_exceeded => executors::ExecutionResult {
                status: ExecutionOutcome::Failed,
                error: Some(format!(
                    "execution hard deadline exceeded at {hard_deadline_at}: {error}"
                )),
                failure_class: Some(executors::ExecutionFailureClass::TaskFailed),
                ..executors::ExecutionResult::default()
            },
            Err(error) => return Err(error.into()),
        };
        restore_result?;
        if let Some(head) = read_only_head {
            result.after_sha = Some(head);
        }
        // A read-only execution that wrote anyway means the work was authored
        // under an authority that never receives write access. The changes are
        // already discarded by policy — fail the execution loudly instead of
        // reporting a clean completion, so the lost work is visible and the
        // task does not advance on nothing. Native (embedded) read-only
        // sessions are additionally enforced at the tool-composition boundary
        // (TaskRead scopes expose no write/command tools); this discard is the
        // backstop for CLI executors, whose processes Forge cannot restrict to
        // a read-only worktree.
        if discarded_read_only_changes && result.status == ExecutionOutcome::Completed {
            result.status = ExecutionOutcome::Failed;
            // Name the actual trigger: read-only is forced either by the
            // execution role (reviewer/planner) or by the task_type
            // (planning_task/discovery). The remedy differs.
            result.error = Some(if read_only_execution_role(&execution.role) {
                format!(
                    "read-only execution produced worktree changes, which were discarded: \
                     role '{}' never receives write access. Implementation work belongs to \
                     the worker/coder role.",
                    execution.role
                )
            } else {
                format!(
                    "read-only execution produced worktree changes, which were discarded: \
                     task_type '{}' never receives write access. If this task implements \
                     code, recreate it as task_type 'task'.",
                    task.task_type
                )
            });
        }
        let max_turns_exceeded = max_turns_exceeded.load(std::sync::atomic::Ordering::SeqCst);
        let assistant_turn_count = assistant_turn_count.load(std::sync::atomic::Ordering::SeqCst);
        if max_turns_exceeded {
            result.status = ExecutionOutcome::Failed;
            result.error = Some(match max_turns {
                Some(limit) => format!("max turns exceeded ({assistant_turn_count}/{limit})"),
                None => "max turns exceeded".to_owned(),
            });
        }
        if hard_deadline_exceeded {
            result.status = ExecutionOutcome::Failed;
            result.error = Some(format!(
                "execution hard deadline exceeded at {hard_deadline_at}"
            ));
        }
        let uncommitted_worktree_failure = result.status == ExecutionOutcome::Failed
            && result.error.as_deref().is_some_and(|error| {
                error.ends_with(crate::embedded_task_executor::UNCOMMITTED_WORKTREE_FAILURE)
            });
        // A write-capable implementation execution that "completes" while
        // leaving the repository untouched and firing no workflow transition
        // has done nothing the pipeline can advance on — the task would sit
        // in its active state forever with a resume_policy no dispatcher pass
        // redispatches. Fail it instead, so the normal executor-failure
        // machinery (retry budget, deferred redispatch, block on exhaustion)
        // governs. A run that legitimately transitioned the task (e.g. a
        // verification turn) moves status/version and is exempt.
        let mut completed_without_repository_effect = false;
        if result.status == ExecutionOutcome::Completed {
            if let Some((branch_point, status_before, version_before)) =
                noop_completion_baseline.as_ref()
            {
                let worktree_path = std::path::Path::new(&workspace.worktree_path);
                let clean = git::is_worktree_clean(worktree_path).await.unwrap_or(false);
                // Nothing committed on this branch at all, not merely nothing
                // committed by this pass.
                let branch_empty = git::get_current_sha(worktree_path).await.ok().as_deref()
                    == Some(branch_point.as_str());
                if clean && branch_empty {
                    let task_after = TaskRepo::get_by_id(&*self.db, &task.id, false)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
                    if task_after.status == *status_before && task_after.version == *version_before
                    {
                        completed_without_repository_effect = true;
                        result.status = ExecutionOutcome::Failed;
                        result.error = Some(
                            "execution completed with nothing committed on the Task branch \
                             and no workflow transition; implement the task in the worktree \
                             and commit the result, or advance the workflow with a reason"
                                .to_owned(),
                        );
                    }
                }
            }
        }

        let current_execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
        if current_execution.status != ExecutionStatus::Running {
            let attempted_status = match &result.status {
                ExecutionOutcome::Completed => ExecutionStatus::Completed,
                ExecutionOutcome::Failed => ExecutionStatus::Failed,
                ExecutionOutcome::Cancelled => ExecutionStatus::Cancelled,
            };
            append_late_terminal_diagnostic(
                &self.db,
                &execution_id,
                &task.id,
                &task.project_id,
                &attempted_status,
                result.error.as_deref(),
                &current_execution,
            )
            .await;
            tracing::info!(
                %execution_id,
                status = %current_execution.status,
                "execution dispatch already stopped externally"
            );
            return Ok(current_execution);
        }

        let executor_unavailable =
            result.failure_class == Some(executors::ExecutionFailureClass::ExecutorUnavailable);
        let unavailable_retry_at = result.retry_after.map(|retry_after| {
            let delay = chrono::Duration::from_std(retry_after)
                .unwrap_or_else(|_| chrono::Duration::minutes(15));
            (chrono::Utc::now() + delay).to_rfc3339()
        });
        let route_outcome = crate::task_service::config::RouteOutcome {
            selected: result.resolved_candidate.as_ref().map(|candidate| {
                (
                    candidate.candidate_key.clone(),
                    candidate.executor_type.to_string(),
                    candidate.config.clone(),
                )
            }),
            attempts: result
                .route_attempts
                .iter()
                .map(|attempt| {
                    (
                        attempt.candidate_key.clone(),
                        attempt.outcome.as_str().to_owned(),
                    )
                })
                .collect(),
            unavailable_retry_at: executor_unavailable.then(|| unavailable_retry_at.clone()),
        };
        let snapshot_update = match current_execution.executor_config_snapshot_json.as_deref() {
            Some(snapshot) => crate::task_service::config::apply_route_outcome_to_snapshot(
                snapshot,
                &route_outcome,
            )?,
            None => None,
        };

        let now = now_rfc3339();
        let (status, stop_reason, stopped_by, stopped_at, resume_policy) = match result.status {
            ExecutionOutcome::Completed => (ExecutionStatus::Completed, None, None, None, None),
            ExecutionOutcome::Failed => (
                ExecutionStatus::Failed,
                Some(if hard_deadline_exceeded {
                    db::StopReason::AgentTimeout
                } else {
                    db::StopReason::ExecutorFailed
                }),
                Some(api_types::Actor::system(api_types::SystemComponent::Executor).display()),
                Some(now.clone()),
                Some(db::ResumePolicy::Manual),
            ),
            ExecutionOutcome::Cancelled => (
                ExecutionStatus::Cancelled,
                Some(db::StopReason::ExecutorCancelled),
                Some(api_types::Actor::system(api_types::SystemComponent::Executor).display()),
                Some(now.clone()),
                Some(db::ResumePolicy::Manual),
            ),
        };
        tracing::info!(
            %execution_id,
            task_id = %task.id,
            status = %status,
            logs_path = %logs_path,
            "execution dispatch completed"
        );

        let (lease_owner, mut expected_version) = lease.owner_and_version().await;
        // Renewal and semantic-progress tasks are stopped above.  A progress
        // write can still have won the last CAS immediately before the stop,
        // so allow one bounded re-read/retry when the row is still running
        // under this same owner.  A terminal row is a definitive late-result
        // rejection and must never be overwritten.
        let mut terminal_attempt = 0;
        let terminal = loop {
            let terminal = ExecutionRepo::terminalize(
                &*self.db,
                db::TerminalizeExecution {
                    execution_id: execution_id.clone(),
                    expected_version,
                    lease_owner: Some(lease_owner.clone()),
                    status: status.clone(),
                    stop_reason: stop_reason.clone().map(Some),
                    stopped_by: stopped_by.clone().map(Some),
                    stopped_at: stopped_at.clone().map(Some),
                    resume_policy: resume_policy.clone().map(Some),
                    agent_session_id: Some(result.agent_session_id.clone()),
                    agent_message_id: None,
                    last_activity_at: Some(Some(now.clone())),
                    last_progress_at: None,
                    summary: Some(result.summary.clone()),
                    logs_path: Some(Some(logs_path.clone())),
                    before_sha: None,
                    after_sha: Some(result.after_sha.clone()),
                    error: Some(result.error.clone()),
                    executor_config_snapshot_json: snapshot_update.clone().map(Some),
                    updated_at: now.clone(),
                    actor_type: "system".to_owned(),
                    actor_id: None,
                    correlation_id: None,
                    causation_id: None,
                    causation_depth: 0,
                    lease_disposition: db::ExecutionLeaseDisposition::Revoke,
                },
            )
            .await?;
            match terminal {
                db::ExecutionTerminalOutcome::Concurrent {
                    current: Some(current),
                } if terminal_attempt == 0
                    && current.status == ExecutionStatus::Running
                    && current.lease_owner.as_deref() == Some(lease_owner.as_str()) =>
                {
                    terminal_attempt += 1;
                    expected_version = current.execution_version;
                    continue;
                }
                terminal => break terminal,
            }
        };
        let updated = match terminal {
            db::ExecutionTerminalOutcome::Committed { execution, .. } => execution,
            db::ExecutionTerminalOutcome::Concurrent { current } => {
                let current = current
                    .ok_or_else(|| ServiceError::not_found("execution", execution_id.clone()))?;
                append_late_terminal_diagnostic(
                    &self.db,
                    &execution_id,
                    &task.id,
                    &task.project_id,
                    &status,
                    result.error.as_deref(),
                    &current,
                )
                .await;
                tracing::info!(
                    execution_id = %execution_id,
                    status = %current.status,
                    "late embedded execution outcome rejected by terminal CAS"
                );
                return Ok(current);
            }
        };

        if let Some(token_usage) = result.usage {
            let model = token_usage
                .model
                .or_else(|| usage_model_fallback.clone())
                .unwrap_or_else(|| "default".to_owned());
            if let Err(error) = ExecutionUsageRepo::upsert(
                &*self.db,
                db::UpsertExecutionUsage {
                    execution_id: updated.id.clone(),
                    provider: usage_provider,
                    model,
                    input_tokens: token_usage.input_tokens,
                    output_tokens: token_usage.output_tokens,
                    cache_read_tokens: token_usage.cache_read_tokens,
                    cache_write_tokens: token_usage.cache_write_tokens,
                    cost_usd: token_usage.cost_usd,
                },
            )
            .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    %error,
                    "failed to record execution token usage"
                );
            }
        }

        super::publish_terminal_execution_event(self, &updated);

        if let Err(error) = self
            .memory_service
            .record_execution_summary_if_present(&task.project_id, &updated)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if updated.status == ExecutionStatus::Completed {
            if let Err(error) = super::clear_execution_retry_metadata(&self.db, &task).await {
                tracing::warn!(
                    task_id = %task.id,
                    execution_id = %updated.id,
                    %error,
                    "failed to clear execution retry metadata"
                );
            }
            if updated.role == crate::workflow::default_roles::PLANNER
                && task.status == crate::workflow::default_states::PLANNING
            {
                if let Err(error) = super::set_planning_awaiting_review_metadata(
                    &self.db,
                    &task,
                    Some(&updated.id),
                    true,
                )
                .await
                {
                    tracing::warn!(
                        task_id = %task.id,
                        execution_id = %updated.id,
                        %error,
                        "failed to mark planning awaiting review"
                    );
                }
            }
        } else if updated.status == ExecutionStatus::Failed && max_turns_exceeded {
            if let Err(error) = self
                .annotate_max_turns_exceeded_block(&updated, max_turns)
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to block task after max turns exceeded"
                );
            }
        } else if updated.status == ExecutionStatus::Failed && executor_unavailable {
            let attempts = serde_json::Value::Array(
                route_outcome
                    .attempts
                    .iter()
                    .map(|(candidate_key, outcome)| {
                        serde_json::json!({"candidate_key": candidate_key, "outcome": outcome})
                    })
                    .collect(),
            );
            if let Err(error) = self
                .annotate_executor_unavailable_block(&updated, unavailable_retry_at, attempts)
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to handle executor-unavailable execution"
                );
            }
        } else if updated.status == ExecutionStatus::Failed
            && (completed_without_repository_effect || uncommitted_worktree_failure)
        {
            // Thread the failure into the next attempt's dispatch prompt (the
            // dispatch context includes task comments), then run the normal
            // executor-failure machinery regardless of role so the retry
            // budget governs the redispatch and exhaustion blocks visibly.
            let corrective_comment = if uncommitted_worktree_failure {
                format!(
                    "Execution {} left uncommitted changes in the Task worktree and did not \
                     advance HEAD. The worktree was preserved. Next attempt: inspect the \
                     existing diff, finish or clean up the changes, run relevant validation, \
                     and commit the result before reporting completion.",
                    updated.id
                )
            } else {
                format!(
                    "Execution {} completed with nothing committed on the Task branch and \
                     no workflow transition. Next attempt: implement the task in the \
                     worktree and commit the result, or advance the workflow with an \
                     explicit reason.",
                    updated.id
                )
            };
            if let Err(error) = self
                .create_system_comment(&task.id, corrective_comment)
                .await
            {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to record corrective execution feedback comment"
                );
            }
            if let Err(error) = self.annotate_executor_failure_block(&updated).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to schedule corrective execution retry"
                );
            }
        } else if updated.status == ExecutionStatus::Failed
            && should_block_task_for_failed_execution(&updated)
        {
            if let Err(error) = self.annotate_executor_failure_block(&updated).await {
                tracing::warn!(
                    execution_id = %updated.id,
                    task_id = %updated.task_id,
                    %error,
                    "failed to block task after executor failure"
                );
            }
        }

        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        AgentRepo, AgentStatus, CreateAgent, CreateExecution, CreateProject, CreateRepo,
        CreateTask, ExecutionRepo, ProjectRepo, RepoRepo, TaskRepo, WorkMode,
    };

    const T0: &str = "2025-01-01T00:00:00+00:00";
    const T1: &str = "2025-01-01T00:00:01+00:00";
    const T20: &str = "2025-01-01T00:00:20+00:00";

    async fn heartbeat_fixture() -> (
        Arc<db::SqliteDb>,
        String,
        Arc<crate::embedded_task_executor::EmbeddedExecutionLease>,
    ) {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("in-memory sqlite pool");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(db::SqliteDb::new(pool));
        let project_id = db::new_uuid_v4();
        ProjectRepo::create(
            &*db,
            CreateProject {
                id: project_id.clone(),
                name: "embedded heartbeat test".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: T0.to_owned(),
                updated_at: T0.to_owned(),
            },
        )
        .await
        .expect("project creates");
        let repo_id = db::new_uuid_v4();
        RepoRepo::create(
            &*db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "heartbeat-test".to_owned(),
                remote_url: "https://example.invalid/heartbeat.git".to_owned(),
                local_path: None,
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: T0.to_owned(),
                updated_at: T0.to_owned(),
            },
        )
        .await
        .expect("repo creates");
        let agent_id = db::new_uuid_v4();
        AgentRepo::create(
            &*db,
            CreateAgent {
                id: agent_id.clone(),
                name: "heartbeat-agent".to_owned(),
                description: None,
                executor_type: "embedded".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 20,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: T0.to_owned(),
                updated_at: T0.to_owned(),
            },
        )
        .await
        .expect("agent creates");
        let task_id = db::new_uuid_v4();
        TaskRepo::create(
            &*db,
            CreateTask {
                id: task_id.clone(),
                project_id,
                repo_id: Some(repo_id),
                parent_task_id: None,
                assignee_type: Some("agent".to_owned()),
                assignee_id: Some(agent_id.clone()),
                title: "heartbeat test".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "in_progress".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: T0.to_owned(),
                updated_at: T0.to_owned(),
            },
        )
        .await
        .expect("task creates");
        let execution_id = db::new_uuid_v4();
        ExecutionRepo::create(
            &*db,
            CreateExecution {
                id: execution_id.clone(),
                task_id,
                agent_id: Some(agent_id),
                role: "executor".to_owned(),
                status: db::ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: T0.to_owned(),
                updated_at: T0.to_owned(),
            },
        )
        .await
        .expect("execution creates");
        let claimed = match ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution_id.clone(),
                expected_version: 1,
                owner: "embedded-heartbeat-test".to_owned(),
                lease_expires_at: "2025-01-01T00:00:05+00:00".to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("execution claims")
        {
            db::ExecutionLeaseMutation::Updated(execution) => execution,
            other => panic!("claim must succeed, got {other:?}"),
        };
        let lease = Arc::new(crate::embedded_task_executor::EmbeddedExecutionLease::new(
            Arc::clone(&db),
            execution_id.clone(),
            claimed.lease_owner.expect("owner persisted"),
            claimed.execution_version,
        ));
        (db, execution_id, lease)
    }

    #[tokio::test(start_paused = true)]
    async fn embedded_heartbeat_renews_without_semantic_progress_then_stops_at_deadline() {
        // sqlx's pool acquisition timeout uses Tokio time; create the fixture
        // on the real clock, then pause the scheduler for heartbeat cadence.
        tokio::time::resume();
        let (db, execution_id, lease) = heartbeat_fixture().await;
        tokio::time::pause();
        let first_stop = CancellationToken::new();
        let (first_signal_tx, _first_signal_rx) = mpsc::unbounded_channel();
        let first_task = tokio::spawn(embedded_execution_heartbeat_with_clock(
            Arc::clone(&lease),
            T20.to_owned(),
            first_stop.clone(),
            first_signal_tx,
            Arc::new(|| T1.to_owned()),
        ));

        // `interval` ticks immediately, then follows the fixed server cadence.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::task::yield_now().await;
        first_stop.cancel();
        first_task.await.expect("heartbeat task joins");

        // Read on the real clock for the same reason the fixture is built on
        // it: a paused scheduler makes sqlx's pool-acquire deadline expire
        // instantly whenever a connection is not already idle.
        tokio::time::resume();
        let renewed = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .expect("execution reads")
            .expect("execution exists");
        tokio::time::pause();
        assert_eq!(renewed.last_heartbeat_at.as_deref(), Some(T1));
        assert_eq!(renewed.last_progress_at, None);
        let renewed_version = renewed.execution_version;

        // Move the paused scheduler to the immutable hard deadline. The next
        // server tick must report the typed deadline without another renewal.
        tokio::time::advance(Duration::from_secs(20)).await;
        let stop = CancellationToken::new();
        let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(embedded_execution_heartbeat_with_clock(
            Arc::clone(&lease),
            T20.to_owned(),
            stop.clone(),
            signal_tx,
            Arc::new(|| T20.to_owned()),
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            signal_rx.recv().await,
            Some(EmbeddedLeaseSignal::HardDeadline)
        );
        task.await.expect("heartbeat task joins");
        stop.cancel();

        tokio::time::resume();
        let at_deadline = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .expect("execution reads")
            .expect("execution exists");
        assert_eq!(at_deadline.execution_version, renewed_version);
        assert_eq!(at_deadline.last_heartbeat_at.as_deref(), Some(T1));
        assert_eq!(at_deadline.last_progress_at, None);
    }

    #[test]
    fn execution_deadline_prefers_shortest_snapshot_policy_and_clamps_fallback() {
        let snapshot = serde_json::json!({
            "config": {"timeout_seconds": 900},
            "capabilities": {"max_execution_seconds": 120},
            "hard_deadline_seconds": 3600,
        });
        assert_eq!(execution_deadline_seconds(&snapshot), 120);
        assert_eq!(
            execution_deadline_seconds(&serde_json::json!({"timeout_seconds": 0})),
            DEFAULT_EXECUTION_HARD_DEADLINE_SECONDS
        );
    }

    #[tokio::test]
    async fn late_terminal_diagnostic_is_bounded_and_deduped_by_current_version() {
        let (db, execution_id, _lease) = heartbeat_fixture().await;
        let current = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .expect("execution reads")
            .expect("execution exists");
        let error = "x".repeat(2_000);
        let event = late_terminal_diagnostic(
            &execution_id,
            &current.task_id,
            "project-id",
            &ExecutionStatus::Completed,
            Some(&error),
            &current,
        );
        let expected_dedupe = format!(
            "execution-late-terminal-rejected:{}:{}:completed",
            execution_id, current.execution_version
        );
        assert_eq!(event.dedupe_key.as_deref(), Some(expected_dedupe.as_str()));
        let payload: Value = serde_json::from_str(&event.payload_json).expect("valid payload");
        assert_eq!(payload["attempted_status"], "completed");
        assert_eq!(payload["current_status"], "running");
        assert_eq!(payload["attempted_error"].as_str().map(str::len), Some(500));
        db::DomainEventRepo::append_event(&*db, event.clone())
            .await
            .expect("late terminal diagnostic appends");
        db::DomainEventRepo::append_event(&*db, event)
            .await
            .expect("duplicate late terminal diagnostic dedupes");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM domain_event
             WHERE event_type = 'execution.late_terminal_rejected' AND dedupe_key = ?",
        )
        .bind(expected_dedupe)
        .fetch_one(db.pool())
        .await
        .expect("late terminal diagnostic count");
        assert_eq!(count, 1);
    }
}

impl TaskService {
    pub(in crate::task_service) async fn cancel_execution_with_provider(
        &self,
        execution: &Execution,
        reason: &str,
    ) -> Result<()> {
        let agent = match execution.agent_id.as_deref() {
            Some(agent_id) => Some(
                AgentRepo::get_by_id(&*self.db, agent_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent", agent_id.to_owned()))?,
            ),
            None => None,
        };
        let provider = self
            .execution_provider_for_agent(agent.as_ref(), &execution.id)
            .await?;
        provider
            .cancel(api_types::ExecutionCancelParams {
                execution_id: execution.id.clone(),
                reason: Some(reason.to_owned()),
            })
            .await?;
        Ok(())
    }

    async fn execution_provider_for_agent(
        &self,
        agent: Option<&Agent>,
        execution_id: &str,
    ) -> Result<Arc<dyn crate::daemon_transport::ExecutionProvider>> {
        let daemon_id = agent.and_then(|agent| agent.daemon_id.as_deref());
        if let Some(registry) = self.daemon_connections.as_ref() {
            return crate::daemon_transport::select_execution_provider(
                daemon_id, &self.db, registry,
            )
            .await
            .inspect_err(|error| {
                if let ServiceError::DaemonUnavailable { daemon_id } = error {
                    tracing::warn!(
                        execution_id = %execution_id,
                        daemon_id = %daemon_id,
                        agent_id = ?agent.map(|agent| agent.id.as_str()),
                        "remote daemon unavailable for execution dispatch"
                    );
                }
            });
        }

        let task_executor = self.task_executor.clone().ok_or_else(|| {
            ServiceError::invalid_operation(
                "task executor is not configured for execution dispatch",
            )
        })?;
        Ok(Arc::new(
            crate::daemon_transport::EmbeddedExecutionProvider::new(
                Arc::new(self.clone()),
                task_executor,
            ),
        ))
    }

    async fn execution_start_params(
        &self,
        execution: &Execution,
    ) -> Result<api_types::ExecutionStartParams> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let workspace_id = execution
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("execution missing workspace_id"))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let snapshot = execution
            .executor_config_snapshot_json
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("execution missing executor config snapshot")
            })?;
        let mut executor_config = parse_json_value("executor config snapshot", snapshot)?;
        if read_only_execution_role(&execution.role)
            || matches!(task.task_type.as_str(), "planning_task" | "discovery")
        {
            executors::mark_worktree_read_only(&mut executor_config);
        }
        let executor_type = executor_config
            .get("executor_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ServiceError::invalid_operation("executor config snapshot missing executor_type")
            })?
            .to_owned();
        let description = execution_description(execution, &task, &executor_config);
        let max_turns = self.resolve_max_turns(&task).await?;

        Ok(api_types::ExecutionStartParams {
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            workspace_path: workspace.worktree_path,
            executor_type,
            executor_config,
            prompt: json!({ "description": description }),
            max_turns,
        })
    }
}

/// Roles whose executions never receive worktree write access, regardless of
/// task_type: the reviewer validates and the planner authors plans through
/// Task metadata/native tools, never through worktree writes.
fn read_only_execution_role(role: &str) -> bool {
    role == crate::workflow::default_roles::REVIEWER
        || role == crate::workflow::default_roles::PLANNER
}

fn execution_description(execution: &Execution, task: &Task, agent_config: &Value) -> String {
    let is_shell_executor =
        agent_config.get("executor_type").and_then(Value::as_str) == Some("shell");
    if is_shell_executor && execution.role == crate::workflow::default_roles::REVIEWER {
        r#"echo "===REVIEW: PASS===""#.to_owned()
    } else {
        execution
            .summary
            .clone()
            .or_else(|| task.description.clone())
            .unwrap_or_else(|| task.title.clone())
    }
}

fn usage_model_fallback(agent_config: &Value) -> Option<String> {
    agent_config
        .get("config")
        .and_then(|config| config.get("model"))
        .and_then(Value::as_str)
        .or_else(|| agent_config.get("model").and_then(Value::as_str))
        .filter(|model| !model.trim().is_empty())
        .map(str::to_owned)
}

fn max_turns_from_value(value: &Value) -> Option<u32> {
    value
        .get("max_turns")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

impl TaskService {
    async fn annotate_max_turns_exceeded_block(
        &self,
        execution: &Execution,
        max_turns: Option<u32>,
    ) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &execution.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", execution.task_id.clone()))?;
        let message = match max_turns {
            Some(limit) => format!("Execution stopped after reaching max_turns={limit}"),
            None => "Execution stopped after reaching max_turns".to_owned(),
        };
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::MaxTurnsExceeded,
            blocking_reason: "max_turns_exceeded".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Executor).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(message.clone()),
            hook: None,
            recovery_actions: vec![
                api_types::RecoveryAction::ResetToInitial,
                api_types::RecoveryAction::CancelTask,
            ],
        };
        let blocked_meta = json!({
            "reason": message,
            "created_at": now_rfc3339(),
            "kind": "max_turns_exceeded",
            "execution_id": execution.id,
        });
        let updated = TaskRepo::update_status(
            &*self.db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: task.status,
                assignee_id: None,
                error_annotation: Some(Some(serde_json::to_string(&annotation).map_err(
                    |error| {
                        ServiceError::invalid_operation(format!(
                            "failed to serialize max-turns annotation: {error}"
                        ))
                    },
                )?)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            updated.id, updated.version
        ))
        .await;
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id,
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: "max_turns_exceeded".to_owned(),
                kind: Some(api_types::FailureKind::MaxTurnsExceeded),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }

    async fn resolve_max_turns(&self, task: &Task) -> Result<Option<u32>> {
        if let Some(value) = task
            .task_state_config
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .and_then(|value| {
                max_turns_from_value(&value)
                    .or_else(|| value.get(&task.status).and_then(max_turns_from_value))
            })
        {
            return Ok(Some(value));
        }

        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Executor),
        );
        if let Some(value) = workflow
            .states
            .iter()
            .find(|state| state.name == task.status)
            .and_then(|state| max_turns_from_value(&state.config))
        {
            return Ok(Some(value));
        }

        Ok(serde_json::from_str::<Value>(&project.settings)
            .ok()
            .and_then(|value| max_turns_from_value(&value)))
    }

    async fn resolve_execution_logs_path(
        &self,
        execution: &Execution,
        task: &Task,
        workspace: &Workspace,
        execution_id: &str,
    ) -> Result<String> {
        let durable_path = execution_logs_path(
            &self.workspace_root,
            &task.project_id,
            &workspace.task_id,
            execution_id,
        );
        let Some(stored_path) = execution.logs_path.as_deref() else {
            return Ok(durable_path);
        };
        if stored_path == durable_path {
            return Ok(durable_path);
        }

        let stored = std::path::Path::new(stored_path);
        if !stored.exists() {
            return Ok(durable_path);
        }

        let durable = std::path::Path::new(&durable_path);
        if let Some(parent) = durable.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                ServiceError::invalid_operation(format!("failed to create log directory: {error}"))
            })?;
        }
        if !durable.exists() {
            std::fs::rename(stored, durable)
                .or_else(|_| {
                    std::fs::copy(stored, durable)?;
                    std::fs::remove_file(stored)
                })
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "failed to move execution log: {error}"
                    ))
                })?;
        }

        Ok(durable_path)
    }
}

#[cfg(test)]
mod usage_tests {
    use super::*;

    #[test]
    fn usage_provider_and_model_come_from_execution_snapshot() {
        let snapshot = json!({
            "executor_type": "codex",
            "model": "agent-model",
            "config": {
                "model": "gpt-5.5"
            }
        });

        assert_eq!(super::usage_provider_from_agent_config(&snapshot), "openai");
        assert_eq!(usage_model_fallback(&snapshot).as_deref(), Some("gpt-5.5"));
    }

    #[test]
    fn usage_model_falls_back_to_top_level_model() {
        let snapshot = json!({
            "executor_type": "claude_code",
            "model": "claude-haiku-4-5",
            "config": {}
        });

        assert_eq!(
            super::usage_provider_from_agent_config(&snapshot),
            "anthropic"
        );
        assert_eq!(
            usage_model_fallback(&snapshot).as_deref(),
            Some("claude-haiku-4-5")
        );
    }

    #[test]
    fn cursor_usage_provider_maps_to_cursor() {
        let snapshot = json!({
            "executor_type": "cursor",
            "config": {}
        });

        assert_eq!(super::usage_provider_from_agent_config(&snapshot), "cursor");
    }
}
