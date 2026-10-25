//! Heartbeat supervision and execution liveness checks.

use super::*;

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

    pub(super) fn start_with_check<F, Fut>(
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

    pub(super) async fn check_agent_timeouts_for_tick(
        &self,
        owner_suspension_succeeded: bool,
    ) -> u64 {
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

    pub(super) async fn expire_workspace_leases_for_tick(&self, renewal_succeeded: bool) -> u64 {
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
                // The lease owner names the retired pipeline; the terminal
                // CAS just released it, so it is read from the row as it was.
                let settled = if crate::task_service::retired_review_rerun(&execution) {
                    task_service.settle_retired_review_rerun(&updated).await
                } else {
                    cascade_recovered_execution(
                        task_service,
                        &updated,
                        stalled_execution_should_block_task(&updated),
                    )
                    .await
                };
                if let Err(error) = settled {
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
