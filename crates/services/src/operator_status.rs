use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Instant,
};

use api_types::{
    ActiveExecutionSummary, AgentPressureSummary, BlockedTaskSummary, DaemonIssueSummary,
    DaemonPressureSummary, DatabaseStorageStatus, EffectiveExecutionPolicy, EventConsumerStatus,
    EventRelayStatus, OperatorSeverity, OperatorStatusResponse, PlanProgressSummary,
    RecentErrorSummary, RetryPressureSummary, TokenTotalsSummary, UsageSummary,
    WorkspaceCleanupSummary,
};
use chrono::{DateTime, Duration, Utc};
use db::{SqliteDb, WorkspaceRepo};
use serde_json::Value;
use sqlx::Row;

use crate::{
    plan_artifact::{read_plan_for_resolved_workspace, PlanArtifactError},
    usage_projection::{usage_aggregate_for_source_state, UsageLedgerIndex},
    workspace_backend::{ResolvedWorkspace, WorkspaceBackendRouter},
    ServiceError,
};

mod log_snapshot;

use log_snapshot::ExecutionLogSnapshots;

pub struct OperatorStatusService {
    run_process_policy: Arc<executors::run_process::MachineRunPolicy>,
    db: Arc<SqliteDb>,
    periodic_workers: Arc<crate::worker_runtime::PeriodicWorkers>,
    log_snapshots: ExecutionLogSnapshots,
    usage_cache: Arc<UsageLedgerIndex>,
    consumer_stall_seconds: u32,
    expected_event_consumers: RwLock<Vec<&'static str>>,
    event_relay: RwLock<Option<Arc<crate::DomainEventBroadcastConsumer>>>,
    relay_enabled: AtomicBool,
    daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    workspace_backend_router: Arc<WorkspaceBackendRouter>,
}

impl OperatorStatusService {
    pub fn new_with_router(
        db: Arc<SqliteDb>,
        workspace_backend_router: Arc<crate::workspace_backend::WorkspaceBackendRouter>,
    ) -> Self {
        Self {
            periodic_workers: Arc::new(crate::worker_runtime::PeriodicWorkers::new(Arc::clone(
                &db,
            ))),
            db: Arc::clone(&db),
            run_process_policy: Arc::new(executors::run_process::MachineRunPolicy::default()),
            log_snapshots: ExecutionLogSnapshots::default(),
            usage_cache: Arc::new(UsageLedgerIndex::new(Arc::clone(&db))),
            consumer_stall_seconds: config::DEFAULT_EVENT_CONSUMER_STALL_SECONDS,
            expected_event_consumers: RwLock::new(Vec::new()),
            event_relay: RwLock::new(None),
            relay_enabled: AtomicBool::new(false),
            daemon_connections: None,
            workspace_backend_router,
        }
    }

    #[cfg(test)]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self::new_for_test(db)
    }

    /// Embedded-only fixture constructor.
    pub fn new_for_test(db: Arc<SqliteDb>) -> Self {
        db.server_run_cap
            .initialize_identity(&config::embedded_machine_id());
        Self {
            workspace_backend_router: crate::diff::embedded_read_router_for_test(Arc::clone(&db)),
            periodic_workers: Arc::new(crate::worker_runtime::PeriodicWorkers::new(Arc::clone(
                &db,
            ))),
            db: Arc::clone(&db),
            run_process_policy: Arc::new(executors::run_process::MachineRunPolicy::default()),
            log_snapshots: ExecutionLogSnapshots::default(),
            usage_cache: Arc::new(UsageLedgerIndex::new(Arc::clone(&db))),
            consumer_stall_seconds: config::DEFAULT_EVENT_CONSUMER_STALL_SECONDS,
            expected_event_consumers: RwLock::new(Vec::new()),
            event_relay: RwLock::new(None),
            relay_enabled: AtomicBool::new(false),
            daemon_connections: None,
        }
    }

    pub fn with_run_process_policy(
        mut self,
        policy: Arc<executors::run_process::MachineRunPolicy>,
    ) -> Self {
        self.run_process_policy = policy;
        self
    }

    pub fn periodic_workers(&self) -> Arc<crate::worker_runtime::PeriodicWorkers> {
        Arc::clone(&self.periodic_workers)
    }

    pub fn usage_ledger_index(&self) -> Arc<UsageLedgerIndex> {
        Arc::clone(&self.usage_cache)
    }

    pub fn with_workspace_backend_router(mut self, router: Arc<WorkspaceBackendRouter>) -> Self {
        self.workspace_backend_router = router;
        self
    }

    pub fn with_daemon_connections(
        mut self,
        registry: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    ) -> Self {
        self.daemon_connections = Some(registry);
        self
    }

    pub fn with_consumer_stall_seconds(mut self, seconds: u32) -> Self {
        self.consumer_stall_seconds = seconds;
        self
    }

    /// Monitor only durable consumers owned by the process's started workers.
    /// Persisted cursors from other assemblies are not evidence of a live worker.
    pub fn set_runtime_workers(&self, workers: &[crate::runtime::RuntimeWorker]) {
        self.relay_enabled.store(
            workers.contains(&crate::runtime::RuntimeWorker::DomainEventBroadcast),
            Ordering::SeqCst,
        );

        *self
            .expected_event_consumers
            .write()
            .expect("event consumer set lock") = workers
            .iter()
            .filter_map(|worker| worker.event_consumer_name())
            .collect();
    }

    pub fn set_event_relay(&self, relay: Arc<crate::DomainEventBroadcastConsumer>) {
        *self.event_relay.write().expect("relay status lock") = Some(relay);
    }
    async fn event_relay_status(&self) -> Result<EventRelayStatus, ServiceError> {
        let relay = self.event_relay.read().expect("relay status lock").clone();
        if let Some(relay) = relay {
            return relay.status().await;
        }
        Ok(EventRelayStatus {
            running: false,
            position: None,
            head: self.db.domain_event_head().await.ok(),
            last_error: None,
            last_error_at: None,
        })
    }
    pub async fn compute_status(&self) -> Result<OperatorStatusResponse, ServiceError> {
        let started = Instant::now();
        let result = self.compute_status_inner().await;
        tracing::debug!(
            target: "forge::perf",
            operation = "operations_status",
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            success = result.is_ok(),
            "performance sample"
        );
        result
    }

    async fn compute_status_inner(&self) -> Result<OperatorStatusResponse, ServiceError> {
        let now = Utc::now();
        let computed_at = now.to_rfc3339();

        let active_executions = self.active_executions(now).await?;
        let blocked_tasks = self.blocked_tasks().await?;
        let daemon_issues = self.daemon_issues(now).await?;
        let daemon_error_count = self.daemon_error_count().await?;
        let daemon_pressure = self.daemon_pressure().await?;
        let agent_pressure = self.agent_pressure().await?;
        let workspace_cleanup = self.workspace_cleanup_backlog(now).await?;
        let retry_pressure = self.retry_pressure().await?;
        let active_execution_count = u32::try_from(active_executions.len())
            .map_err(|_| ServiceError::Db(db::DbError::InvalidTransition))?;
        let usage_summary = Some(self.usage_summary(active_execution_count).await?);
        let event_consumers = self.event_consumers(now).await?;
        let periodic_workers = self.periodic_workers.status().await?;
        let task_steps = self.task_step_status(now).await?;
        let pending_remote_cancels: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pending_remote_cancel")
                .fetch_one(self.db.pool())
                .await?;
        let storage = db::sqlite_storage_status(self.db.pool()).await?;
        let database = DatabaseStorageStatus {
            incremental_vacuum: storage.incremental_vacuum,
            free_pages: storage.free_pages,
        };
        let mut recent_errors = self.recent_errors(now).await?;
        for worker in &periodic_workers {
            if let (Some(error), Some(occurred_at)) = (&worker.last_error, &worker.last_error_at) {
                recent_errors.push(RecentErrorSummary {
                    entity_type: "periodic_worker".to_owned(),
                    entity_id: worker.worker_name.clone(),
                    error: error.clone(),
                    occurred_at: occurred_at.clone(),
                    severity: OperatorSeverity::Attention,
                });
            }
        }
        let names: Vec<&str> = event_consumers
            .iter()
            .map(|consumer| consumer.consumer_name.as_str())
            .collect();
        let diagnostics = self.db.worker_operator_diagnostics(&names).await?;
        for consumer in &event_consumers {
            let diagnostic = diagnostics
                .iter()
                .find(|row| row.worker_name == consumer.consumer_name);
            let mut messages = Vec::new();
            let mut occurred_at = None;
            let mut attention = consumer.stalled;
            if let Some(diagnostic) = diagnostic {
                for (kind, cause, time) in &diagnostic.errors {
                    let label = match kind {
                        db::HealthErrorKind::Runtime => "Runtime",
                        db::HealthErrorKind::Item => "Event",
                        db::HealthErrorKind::Tick => "Tick",
                        db::HealthErrorKind::AfterCommit => "After commit",
                    };
                    messages.push(format!("{label}: {cause}"));
                    occurred_at = Some(time.clone());
                    attention = true;
                }
                if let (Some(reason), Some(since)) =
                    (&diagnostic.deferred_reason, &diagnostic.deferred_since)
                {
                    messages.push(format!("Deferred since {since}: {reason}"));
                    occurred_at.get_or_insert_with(|| since.clone());
                }
            }
            if consumer.stalled {
                messages.push(format!("Event consumer stalled: cursor has not advanced for more than {} seconds; lag {}",
                    self.consumer_stall_seconds, consumer.lag));
            }
            if !messages.is_empty() {
                recent_errors.push(RecentErrorSummary {
                    entity_type: "event_consumer".to_owned(),
                    entity_id: consumer.consumer_name.clone(),
                    error: messages.join("; "),
                    occurred_at: occurred_at
                        .or_else(|| consumer.last_advanced_at.clone())
                        .or_else(|| consumer.oldest_unprocessed_at.clone())
                        .unwrap_or_else(|| computed_at.clone()),
                    severity: if attention {
                        OperatorSeverity::Attention
                    } else {
                        OperatorSeverity::Healthy
                    },
                });
            }
        }
        for dead in self
            .db
            .worker_dead_letter_issues(&names, &(now - Duration::hours(1)).to_rfc3339())
            .await?
        {
            recent_errors.push(RecentErrorSummary {
                entity_type: "worker_dead_letter".to_owned(),
                entity_id: format!("{}:{}", dead.worker_name, dead.source_key),
                error: format!(
                    "{} quarantined {} {}: {}",
                    dead.worker_name, dead.item_type, dead.source_key, dead.reason
                ),
                occurred_at: dead.occurred_at,
                severity: OperatorSeverity::Attention,
            });
        }

        let event_relay = self.event_relay_status().await?;
        // A process that declares the relay but has not attached one (a
        // service built without the runtime) has nothing to report on.
        let relay_attached = self
            .event_relay
            .read()
            .expect("relay status lock")
            .is_some();
        if self.relay_enabled.load(Ordering::SeqCst)
            && relay_attached
            && (!event_relay.running || event_relay.last_error.is_some())
        {
            recent_errors.push(RecentErrorSummary {
                entity_type: "event_relay".into(),
                entity_id: "domain-event-relay".into(),
                error: event_relay
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "event relay is restarting or stopped".into()),
                occurred_at: event_relay
                    .last_error_at
                    .clone()
                    .unwrap_or_else(|| computed_at.clone()),
                severity: OperatorSeverity::Attention,
            });
        }
        let mut overall_severity = OperatorSeverity::Healthy;
        if !daemon_issues.is_empty() || !workspace_cleanup.is_empty() {
            raise_severity(&mut overall_severity, OperatorSeverity::Attention);
        }
        if daemon_pressure.iter().any(|item| item.at_capacity)
            || agent_pressure.iter().any(|item| item.at_capacity)
        {
            raise_severity(&mut overall_severity, OperatorSeverity::Attention);
        }
        if !blocked_tasks.is_empty() {
            raise_severity(&mut overall_severity, OperatorSeverity::Blocked);
        }
        // A clean pass is a log line. Only a completed pass that had to
        // repair a row is an operator issue: a producer missed its write.
        let last_pass = self.db.condition_check_status().last_pass;
        // Quarantined conditions stay reported for as long as they exist:
        // nothing in this build repairs them.
        if let Some(pass) = last_pass.as_ref().filter(|pass| pass.quarantined > 0) {
            recent_errors.push(RecentErrorSummary {
                severity: api_types::OperatorSeverity::Attention,
                entity_type: "task_condition_quarantined".into(),
                entity_id: "task-dispatcher".into(),
                error: format!(
                    "{} Task condition(s) were written by a newer Forge build and are quarantined ({}{}). They are not repaired, and holds and releases on them are refused. Run a build that understands them",
                    pass.quarantined,
                    pass.quarantined_ids.join(", "),
                    if pass.quarantined as usize > pass.quarantined_ids.len() { ", ..." } else { "" },
                ),
                occurred_at: pass.completed_at.clone(),
            });
        }
        if let Some(pass) = last_pass.filter(|pass| pass.repaired > 0) {
            recent_errors.push(RecentErrorSummary {
                severity: api_types::OperatorSeverity::Attention,
                entity_type: "task_condition_invariant".into(),
                entity_id: "task-dispatcher".into(),
                error: format!(
                    "The last Task condition check repaired {} of {} Tasks",
                    pass.repaired, pass.checked
                ),
                occurred_at: pass.completed_at,
            });
        }
        for issue in &recent_errors {
            raise_severity(&mut overall_severity, issue.severity.clone());
        }
        if daemon_error_count > 0 {
            raise_severity(&mut overall_severity, OperatorSeverity::Error);
        }

        Ok(OperatorStatusResponse {
            overall_severity,
            active_executions,
            blocked_tasks,
            daemon_issues,
            daemon_pressure,
            agent_pressure,
            workspace_cleanup,
            retry_pressure,
            usage_summary,
            usage_index: self.usage_cache.status().await,
            recent_errors,
            event_consumers,
            periodic_workers,
            task_steps,
            pending_remote_cancels,
            event_relay,
            database,
            computed_at,
        })
    }

    async fn task_step_status(
        &self,
        now: DateTime<Utc>,
    ) -> Result<api_types::TaskStepQueueStatus, ServiceError> {
        // Failed and parked count only while the Task's annotation still
        // points at that step: resolved rows are history, not queue health.
        let (pending,claimed,oldest): (i64,i64,Option<String>) = sqlx::query_as("SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='claimed'),0),MIN(CASE WHEN status='pending' THEN created_at END) FROM task_step WHERE status IN ('pending','claimed')")
            .fetch_one(self.db.pool()).await?;
        let (failed,parked): (i64,i64) = sqlx::query_as("SELECT COALESCE(SUM(s.status='failed'),0),COALESCE(SUM(s.status='parked'),0) FROM task_step s JOIN task t ON t.id=s.task_id WHERE s.status IN ('failed','parked') AND t.deleted_at IS NULL AND json_extract(t.condition_json,'$.evidence.presentation.failed_step_id')=s.id")
            .fetch_one(self.db.pool()).await?;
        let (last_error,last_error_at,restart_count): (Option<String>,Option<String>,i64) = sqlx::query_as("SELECT last_error,last_error_at,restart_count FROM worker_health WHERE worker_name='task_steps'")
            .fetch_optional(self.db.pool()).await?.unwrap_or_default();
        Ok(api_types::TaskStepQueueStatus {
            worker_name: "task_steps".into(),
            pending,
            claimed,
            failed,
            parked,
            in_flight: self.db.active_step_count(),
            oldest_pending_age_seconds: oldest.as_deref().map(|at| seconds_since(at, now)),
            last_error,
            last_error_at,
            restart_count,
        })
    }

    async fn event_consumers(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<EventConsumerStatus>, ServiceError> {
        let expected = self
            .expected_event_consumers
            .read()
            .expect("event consumer set lock")
            .clone();
        let lag = self.db.domain_event_consumer_lag(&expected).await?;
        let mut statuses = Vec::with_capacity(lag.len());
        for consumer in lag {
            let (dead_letter_count, recent_dead_letters) = self
                .db
                .worker_dead_letter_history(&consumer.consumer_name)
                .await?;
            let recent_dead_letters = recent_dead_letters
                .into_iter()
                .map(|dead| crate::dead_letter_service::summary(&dead))
                .collect();
            let stalled = consumer.stalled(now, i64::from(self.consumer_stall_seconds));
            statuses.push(EventConsumerStatus {
                consumer_name: consumer.consumer_name,
                last_sequence: consumer.last_sequence,
                lag: consumer.lag,
                oldest_unprocessed_age_seconds: consumer
                    .oldest_unprocessed_at
                    .as_deref()
                    .map(|created| seconds_since(created, now)),
                oldest_unprocessed_at: consumer.oldest_unprocessed_at,
                last_advanced_at: consumer.last_advanced_at,
                stalled,
                dead_letter_count,
                recent_dead_letters,
            });
        }
        Ok(statuses)
    }

    async fn active_executions(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<ActiveExecutionSummary>, ServiceError> {
        let rows = sqlx::query(
            "SELECT
                e.id AS execution_id,
                e.task_id,
                t.title AS task_title,
                e.role,
                e.agent_id,
                a.name AS agent_name,
                e.workspace_id,
                e.agent_session_id,
                e.created_at AS started_at,
                e.logs_path,
                e.executor_config_snapshot_json
             FROM execution e
             JOIN task t ON t.id = e.task_id
             LEFT JOIN agent_current a ON a.id = e.agent_id
             WHERE e.status = 'running'
             ORDER BY e.created_at ASC, e.id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        let mut active_executions = Vec::with_capacity(rows.len());
        for row in rows {
            let started_at: String = row.try_get("started_at")?;
            let workspace_id: Option<String> = row.try_get("workspace_id")?;
            let resolved = match workspace_id.as_deref() {
                Some(id) => match WorkspaceRepo::get_by_id(&*self.db, id).await? {
                    Some(workspace) => Some(
                        self.workspace_backend_router
                            .resolve(&self.db, &workspace)
                            .await?,
                    ),
                    None => None,
                },
                None => None,
            };
            let workspace_path = resolved
                .as_ref()
                .map(|workspace| workspace.handle().map(str::to_owned))
                .transpose()?;
            let daemon_id = resolved.as_ref().and_then(|workspace| {
                workspace
                    .placement
                    .execution_daemon_id
                    .clone()
                    .or_else(|| workspace.placement.daemon_id.clone())
            });
            let runtime_seconds = seconds_since(&started_at, now);
            let snapshot_json: Option<String> = row.try_get("executor_config_snapshot_json")?;
            let effective_policy =
                effective_policy(snapshot_json.as_deref(), workspace_path.as_deref());
            let rate_limit_snapshot = rate_limit_snapshot(snapshot_json.as_deref());
            let plan_progress = match resolved.as_ref() {
                Some(workspace) => plan_progress(workspace).await?,
                None => None,
            };
            let log_snapshot = self
                .log_snapshots
                .read(row.try_get::<Option<String>, _>("logs_path")?)
                .await;
            let execution_id: String = row.try_get("execution_id")?;
            let usage = usage_aggregate_for_source_state(&self.db, &execution_id, true).await?;
            let token_totals = Some(TokenTotalsSummary {
                tokens: usage.tokens.clone(),
                cost: usage.cost.clone(),
            });

            active_executions.push(ActiveExecutionSummary {
                execution_id,
                task_id: row.try_get("task_id")?,
                task_title: row.try_get("task_title")?,
                role: row.try_get("role")?,
                agent_id: row.try_get("agent_id")?,
                agent_name: row.try_get("agent_name")?,
                daemon_id,
                workspace_id,
                workspace_path,
                session_id: row.try_get("agent_session_id")?,
                started_at,
                runtime_seconds,
                elapsed_seconds: runtime_seconds,
                latest_event: log_snapshot.last_event.clone(),
                last_event: log_snapshot.last_event,
                last_event_time: log_snapshot.last_event_time,
                turn_count: log_snapshot.turn_count,
                token_totals,
                rate_limit_snapshot,
                effective_policy,
                plan_progress,
            });
        }

        Ok(active_executions)
    }

    async fn blocked_tasks(&self) -> Result<Vec<BlockedTaskSummary>, ServiceError> {
        let rows = sqlx::query(
            "SELECT id, title, condition_json, updated_at
             FROM task
             WHERE (status = 'blocked' OR json_extract(condition_json,'$.evidence.presentation.interruption_present') = 1)
               AND deleted_at IS NULL
               AND archived_at IS NULL
               AND status NOT IN ('done', 'cancelled')
             ORDER BY updated_at DESC, id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        rows.into_iter()
            .map(|row| {
                let condition = db::task_condition::decode_or_unknown(
                    &row.try_get::<String, _>("condition_json")?,
                );
                Ok(BlockedTaskSummary {
                    task_id: row.try_get("id")?,
                    title: row.try_get("title")?,
                    blocked_reason: condition.read().operator_reason,
                    blocked_since: Some(row.try_get("updated_at")?),
                })
            })
            .collect()
    }

    async fn daemon_issues(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<DaemonIssueSummary>, ServiceError> {
        let stale_before = now - Duration::minutes(5);
        let rows = sqlx::query(
            "SELECT id, hostname, status, last_report_at, updated_at
             FROM daemon
             WHERE removed_at IS NULL
             ORDER BY updated_at DESC, id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        rows.into_iter()
            .filter(|row| {
                let id: String = row.get("id");
                let status: String = row.get("status");
                let last: Option<String> = row.get("last_report_at");
                matches!(status.as_str(), "offline" | "error")
                    || last.is_some_and(|last| last < stale_before.to_rfc3339())
                    || self
                        .daemon_connections
                        .as_ref()
                        .and_then(|registry| registry.get(&id))
                        .is_some_and(|connection| connection.needs_upgrade())
            })
            .map(|row| {
                let status: String = row.try_get("status")?;
                let last_report_at: Option<String> = row.try_get("last_report_at")?;
                let (issue, severity) = match status.as_str() {
                    "error" => ("error".to_owned(), OperatorSeverity::Error),
                    "offline" => ("offline".to_owned(), OperatorSeverity::Attention),
                    _ => ("stale".to_owned(), OperatorSeverity::Attention),
                };
                let id: String = row.try_get("id")?;
                let issue = if self
                    .daemon_connections
                    .as_ref()
                    .and_then(|registry| registry.get(&id))
                    .is_some_and(|connection| connection.needs_upgrade())
                {
                    format!(
                        "upgrade_required: {}",
                        api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE
                    )
                } else {
                    issue
                };
                Ok(DaemonIssueSummary {
                    daemon_id: id,
                    hostname: row.try_get("hostname")?,
                    issue,
                    severity,
                    detected_at: last_report_at.or_else(|| row.try_get("updated_at").ok()),
                })
            })
            .collect()
    }

    async fn daemon_error_count(&self) -> Result<i64, ServiceError> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM daemon WHERE status = 'error'")
                .fetch_one(self.db.pool())
                .await?,
        )
    }

    async fn daemon_pressure(&self) -> Result<Vec<DaemonPressureSummary>, ServiceError> {
        let budget = self.run_process_policy.get();
        Ok(crate::placement::machine_precheck::snapshot(&self.db)
            .await?
            .into_iter()
            .map(|row| DaemonPressureSummary {
                daemon_id: row
                    .daemon_id
                    .clone()
                    .unwrap_or_else(|| "server_host".to_owned()),
                hostname: Some(row.hostname),
                active_runs: row.capacity.active_runs().max(0) as u32,
                max_concurrent_runs: row.capacity.max_concurrent_runs,
                logical_cores: row
                    .daemon_id
                    .is_none()
                    .then(|| u32::try_from(config::logical_cores()).unwrap_or(u32::MAX)),
                build_jobs_per_run: row.daemon_id.is_none().then(|| budget.build_jobs()),
                run_nice: row.daemon_id.is_none().then_some(budget.run_nice),
                at_capacity: !row.capacity.has_capacity(),
            })
            .collect())
    }

    async fn agent_pressure(&self) -> Result<Vec<AgentPressureSummary>, ServiceError> {
        let rows = sqlx::query(
            "SELECT
                a.id AS agent_id,
                a.name AS agent_name,
                a.daemon_id,
                a.max_concurrent_tasks,
                (
                    SELECT COUNT(*)
                    FROM execution e
                    WHERE e.agent_id = a.id AND e.status = 'running'
                ) + (
                    SELECT COUNT(*) FROM workspace_placement p
                    WHERE p.agent_id = a.id AND p.state IN ('reserved', 'preparing')
                       AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
                      AND NOT EXISTS (SELECT 1 FROM execution e
                          WHERE e.workspace_id = p.workspace_id AND e.status = 'running')
                ) AS running_executions
             FROM agent_current a
             ORDER BY a.name ASC, a.id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        let mut pressure = Vec::new();
        for row in rows {
            let active_tasks = row.try_get::<i64, _>("running_executions")?.max(0) as u32;
            let max_concurrent_tasks = row.try_get::<i64, _>("max_concurrent_tasks")?.max(0) as u32;
            let at_capacity = max_concurrent_tasks > 0 && active_tasks >= max_concurrent_tasks;
            if active_tasks == 0 && !at_capacity {
                continue;
            }
            pressure.push(AgentPressureSummary {
                agent_id: row.try_get("agent_id")?,
                agent_name: row.try_get("agent_name")?,
                daemon_id: row.try_get("daemon_id")?,
                active_tasks,
                max_concurrent_tasks,
                at_capacity,
            });
        }
        pressure.sort_by(|left, right| {
            right
                .active_tasks
                .cmp(&left.active_tasks)
                .then_with(|| left.agent_name.cmp(&right.agent_name))
        });
        Ok(pressure)
    }

    async fn workspace_cleanup_backlog(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<WorkspaceCleanupSummary>, ServiceError> {
        let rows = sqlx::query(
            "SELECT id, task_id, cleanup_after
             FROM workspace
             WHERE status IN ('ready', 'cleaning')
               AND cleanup_after IS NOT NULL
               AND cleanup_after < ?
             ORDER BY cleanup_after ASC, id ASC",
        )
        .bind(now.to_rfc3339())
        .fetch_all(self.db.pool())
        .await?;

        let mut backlog = Vec::with_capacity(rows.len());
        for row in rows {
            let workspace_id: String = row.try_get("id")?;
            let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.clone()))?;
            let resolved = self
                .workspace_backend_router
                .resolve(&self.db, &workspace)
                .await?;
            backlog.push(WorkspaceCleanupSummary {
                workspace_id,
                task_id: row.try_get("task_id")?,
                worktree_path: Some(resolved.handle()?.to_owned()),
                cleanup_after: row.try_get("cleanup_after")?,
            });
        }
        Ok(backlog)
    }

    async fn retry_pressure(&self) -> Result<Vec<RetryPressureSummary>, ServiceError> {
        // `attempt_count` counts transition rejections, as it always has; the
        // Execution spend comes from the budget ledger. Both are page queries.
        let rows = sqlx::query(
            "SELECT
                t.id AS task_id,
                t.title,
                t.status,
                t.condition_json,
                COUNT(tl.id) AS attempt_count,
                COALESCE((
                    SELECT b.spent FROM task_budget b
                    WHERE b.task_id = t.id AND b.kind = 'execution'
                ), 0) AS execution_spent,
                (
                    SELECT e.error
                    FROM execution e
                    WHERE e.task_id = t.id AND e.status = 'failed'
                    ORDER BY COALESCE(e.stopped_at, e.updated_at) DESC, e.id DESC
                    LIMIT 1
                ) AS last_error
             FROM task t
             LEFT JOIN transition_log tl ON tl.task_id = t.id AND tl.rejection = 1
             WHERE t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')
             GROUP BY t.id, t.title, t.status, t.condition_json
             HAVING COUNT(tl.id) >= 1 OR json_extract(t.condition_json,'$.evidence.presentation.retry_display.reason') IS NOT NULL OR execution_spent > 0
             ORDER BY attempt_count DESC, t.updated_at DESC, t.id ASC",
        )
        .fetch_all(self.db.pool())
        .await?;

        let retried = rows
            .iter()
            .filter(|row| row.try_get::<i64, _>("execution_spent").unwrap_or(0) > 0)
            .map(|row| row.try_get::<String, _>("task_id"))
            .collect::<Result<Vec<_>, _>>()?;
        let retried = self
            .db
            .get_tasks_by_ids(&retried.iter().map(String::as_str).collect::<Vec<_>>())
            .await?;
        let mut workflows = std::collections::HashMap::<String, String>::new();
        let mut execution_limits = std::collections::HashMap::<String, i32>::new();
        for task in &retried {
            if !workflows.contains_key(&task.project_id) {
                let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                workflows.insert(task.project_id.clone(), project.workflow_definition);
            }
            let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
                task,
                &workflows[&task.project_id],
                &api_types::Actor::system(api_types::SystemComponent::General),
            );
            let state = workflow.states.iter().find(|s| s.name == task.status);
            execution_limits.insert(
                task.id.clone(),
                db::budget::limit(
                    task,
                    db::budget::Kind::Execution,
                    state.map(|s| &s.config),
                    state.and_then(|s| s.gate_config.as_ref()),
                )?,
            );
        }

        let mut pressure = Vec::new();
        for row in rows {
            let attempt_count: i64 = row.try_get("attempt_count")?;
            let condition =
                db::task_condition::decode_or_unknown(&row.try_get::<String, _>("condition_json")?);
            let task_id: String = row.try_get("task_id")?;
            let execution_retry_count = row.try_get::<i64, _>("execution_spent")?.max(0) as u32;
            let deferred = condition.read().retry_display;
            let retry_reason = deferred
                .as_ref()
                .and_then(|d| d.reason.clone())
                .or_else(|| (execution_retry_count > 0).then(|| "execution retry".to_owned()))
                .or_else(|| (attempt_count > 0).then(|| "transition rejection".to_owned()));
            let due_time = deferred.and_then(|d| d.not_before);
            let attempt_count = (attempt_count.max(0) as u32)
                .max(execution_retry_count)
                .max(u32::from(retry_reason.is_some()));
            if attempt_count == 0 && retry_reason.is_none() {
                continue;
            }
            pressure.push(RetryPressureSummary {
                max_attempts: execution_limits
                    .get(&task_id)
                    .map(|limit| (*limit).max(0) as u32),
                task_id,
                title: row.try_get("title")?,
                attempt_count,
                current_state: row.try_get("status")?,
                retry_reason,
                due_time,
                last_error: row.try_get("last_error")?,
            });
        }
        Ok(pressure)
    }

    async fn usage_summary(
        &self,
        active_execution_count: u32,
    ) -> Result<UsageSummary, ServiceError> {
        let usage = self.usage_cache.operations().await?;
        Ok(UsageSummary {
            counts: usage.counts,
            tokens: usage.tokens,
            cost: usage.cost,
            active_execution_count,
        })
    }

    async fn recent_errors(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<RecentErrorSummary>, ServiceError> {
        let cutoff = now - Duration::hours(1);
        let rows = sqlx::query(
            "SELECT
                e.id AS execution_id,
                e.task_id,
                e.error,
                COALESCE(e.stopped_at, e.updated_at) AS occurred_at
             FROM execution e INDEXED BY idx_execution_usage_failed_recent
             JOIN task t ON t.id = e.task_id
             WHERE e.status = 'failed'
               AND t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')
               AND COALESCE(e.stopped_at, e.updated_at) >= ?
             ORDER BY occurred_at DESC, e.id ASC",
        )
        .bind(cutoff.to_rfc3339())
        .fetch_all(self.db.pool())
        .await?;

        rows.into_iter()
            .map(|row| {
                let error: Option<String> = row.try_get("error")?;
                Ok(RecentErrorSummary {
                    entity_type: "task".to_owned(),
                    entity_id: row.try_get("task_id")?,
                    error: error.unwrap_or_else(|| "execution failed".to_owned()),
                    occurred_at: row.try_get("occurred_at")?,
                    severity: OperatorSeverity::Error,
                })
            })
            .collect()
    }
}

fn raise_severity(current: &mut OperatorSeverity, candidate: OperatorSeverity) {
    if candidate > *current {
        *current = candidate;
    }
}

fn seconds_since(started_at: &str, now: DateTime<Utc>) -> f64 {
    parse_rfc3339(started_at)
        .map(|started_at| (now - started_at).num_milliseconds().max(0) as f64 / 1000.0)
        .unwrap_or(0.0)
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn rate_limit_snapshot(snapshot_json: Option<&str>) -> Option<Value> {
    let snapshot = serde_json::from_str::<Value>(snapshot_json?).ok()?;
    let config = snapshot.get("config").unwrap_or(&snapshot);
    snapshot
        .get("rate_limit_snapshot")
        .or_else(|| snapshot.get("rate_limit"))
        .or_else(|| config.get("rate_limit_snapshot"))
        .or_else(|| config.get("rate_limit"))
        .cloned()
}

fn effective_policy(
    snapshot_json: Option<&str>,
    workspace_path: Option<&str>,
) -> Option<EffectiveExecutionPolicy> {
    let snapshot = serde_json::from_str::<Value>(snapshot_json?).ok()?;
    let config = snapshot.get("config").unwrap_or(&Value::Null);
    let executor_kind = snapshot
        .get("executor_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let permission_policy = config
        .get("permission_policy")
        .and_then(Value::as_str)
        .or_else(|| snapshot.get("permission_policy").and_then(Value::as_str))
        .unwrap_or("supervised")
        .to_owned();
    let isolation_posture = isolation_posture(&executor_kind, &permission_policy, config);
    let is_high_risk = matches!(isolation_posture.as_str(), "danger-full-access")
        || config
            .get("dangerously_skip_permissions")
            .and_then(Value::as_bool)
            .unwrap_or(false);

    Some(EffectiveExecutionPolicy {
        executor_kind,
        permission_policy,
        isolation_posture,
        is_high_risk,
        effective_cwd: workspace_path.map(str::to_owned),
        workspace_root: workspace_path.map(str::to_owned),
        environment_posture: environment_posture(config),
        scoped_tools: string_array(config.get("scoped_tools")),
        mcp_servers: string_array(config.get("mcp_servers")),
    })
}

fn isolation_posture(executor_kind: &str, permission_policy: &str, config: &Value) -> String {
    match executor_kind {
        "codex" => config
            .get("sandbox")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| match permission_policy {
                "plan" => "read-only".to_owned(),
                _ => "workspace-write".to_owned(),
            }),
        "claude_code" => {
            if config
                .get("dangerously_skip_permissions")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                "dangerously_skip_permissions".to_owned()
            } else if config.get("plan").and_then(Value::as_bool).unwrap_or(false) {
                "plan".to_owned()
            } else {
                config
                    .get("approvals")
                    .and_then(Value::as_str)
                    .unwrap_or("default")
                    .to_owned()
            }
        }
        "cursor" => {
            if config
                .get("force")
                .and_then(Value::as_bool)
                .unwrap_or(permission_policy != "plan")
            {
                "force".to_owned()
            } else {
                "propose_only".to_owned()
            }
        }
        _ => "not_applicable".to_owned(),
    }
}

fn environment_posture(config: &Value) -> String {
    match config.get("env").and_then(Value::as_object) {
        Some(env) if !env.is_empty() => "custom".to_owned(),
        _ => "inherits_process".to_owned(),
    }
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

async fn plan_progress(
    workspace: &ResolvedWorkspace,
) -> Result<Option<PlanProgressSummary>, ServiceError> {
    match read_plan_for_resolved_workspace(workspace).await {
        Ok(Some((progress, _))) => Ok(Some(progress)),
        Ok(None) | Err(PlanArtifactError::NotFound) => Ok(None),
        Err(error) => Ok(Some(PlanProgressSummary {
            total: 0,
            completed: 0,
            remaining: 0,
            available: false,
            warnings: vec![error.to_string()],
        })),
    }
}

#[cfg(test)]
mod tests {
    async fn sync_fixtures(service: &super::OperatorStatusService) {
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM task")
            .fetch_all(service.db.pool())
            .await
            .unwrap();
        service.db.check_task_conditions_of(&ids).await.unwrap();
    }

    // Fixture tests for operator status payloads. Each test sets up specific DB rows and
    // workspace state, calls compute_status(), and asserts the resulting payload shape.
    // To add a new fixture: insert rows using the helpers in this module, call compute_status(),
    // and assert the fields you care about. Tests are independent (in-memory SQLite per test).

    use super::*;
    use std::fs;

    use db::{create_sqlite_pool, new_uuid_v4, run_migrations};
    use tempfile::tempdir;

    #[tokio::test]
    async fn machine_capacity_operations_includes_server_host() {
        let (db, service) = test_service().await;
        db.server_run_cap.set(
            Some(4),
            config::resolved_run_cap(Some(4)),
            &config::embedded_machine_id(),
        );
        let pressure = service.daemon_pressure().await.unwrap();
        assert_eq!(pressure.len(), 1);
        assert_eq!(pressure[0].daemon_id, "server_host");
        assert_eq!(pressure[0].active_runs, 0);
        assert_eq!(pressure[0].max_concurrent_runs, Some(4));
        assert_eq!(
            pressure[0].logical_cores,
            Some(config::logical_cores() as u32)
        );
        assert_eq!(
            pressure[0].build_jobs_per_run,
            Some(executors::run_process::machine_policy().get().build_jobs())
        );
        assert_eq!(pressure[0].run_nice, Some(10));
        assert!(!pressure[0].at_capacity);
    }

    async fn test_service() -> (Arc<SqliteDb>, OperatorStatusService) {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(SqliteDb::new(pool));
        let service = OperatorStatusService::new(Arc::clone(&db));
        service.set_runtime_workers(&crate::runtime::COMMON_WORKERS);
        (db, service)
    }

    #[tokio::test]
    async fn periodic_workers_are_listed_with_running_tick_error_and_restart_health() {
        let (db, service) = test_service().await;
        let registry = service.periodic_workers();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let entered = Arc::new(tokio::sync::Notify::new());
        let handle = registry.worker("daemon-monitor").start(rx, || false, {
            let entered = Arc::clone(&entered);
            move |worker, _| {
                let entered = Arc::clone(&entered);
                async move {
                    let _ = worker
                        .tick(async {
                            Err::<(), _>(ServiceError::invalid_operation("fixture tick failure"))
                        })
                        .await;
                    entered.notify_one();
                    std::future::pending::<crate::Result<()>>().await
                }
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        db::WorkerHealth::new(db, "daemon-monitor")
            .record_restart("fixture restart")
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
        assert!(status
            .recent_errors
            .iter()
            .any(|error| error.entity_id == "daemon-monitor"));
        let row = &status.periodic_workers[0];
        assert_eq!(row.worker_name, "daemon-monitor");
        assert!(row.running);
        assert!(row.last_tick_at.is_some());
        assert!(row.last_error.is_some());
        assert!(row.last_error_at.is_some());
        assert_eq!(row.restart_count, 1);
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn machine_capacity_agent_pressure_excludes_expired_reservations() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql("CREATE TABLE agent_current (id TEXT, name TEXT, daemon_id TEXT, max_concurrent_tasks INTEGER);
            CREATE TABLE workspace_placement (workspace_id TEXT, agent_id TEXT, state TEXT, reserved_until TEXT, updated_at TEXT);
            CREATE TABLE execution (workspace_id TEXT, agent_id TEXT, status TEXT);
            INSERT INTO agent_current VALUES ('agent','Agent',NULL,2);
            INSERT INTO workspace_placement VALUES ('expired','agent','reserved','2020-01-01T00:00:00Z',CURRENT_TIMESTAMP),
              ('orphan','agent','preparing',NULL,'2020-01-01T00:00:00Z'),
              ('live','agent','reserved','2099-01-01T00:00:00Z',CURRENT_TIMESTAMP);")
            .execute(&pool).await.unwrap();
        let service = OperatorStatusService::new_for_test(Arc::new(SqliteDb::new(pool)));
        let pressure = service.agent_pressure().await.unwrap();
        assert_eq!(pressure.len(), 1);
        assert_eq!(pressure[0].active_tasks, 1);
        assert!(!pressure[0].at_capacity);
    }

    #[tokio::test]
    async fn machine_capacity_operations_uses_handle_identity() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(r#"CREATE TABLE daemon (id TEXT, hostname TEXT, machine_id TEXT, max_concurrent_runs INTEGER, run_limit INTEGER, removed_at TEXT);
            CREATE TABLE workspace_placement (workspace_id TEXT, agent_id TEXT, daemon_id TEXT, execution_daemon_id TEXT, state TEXT, reserved_until TEXT, updated_at TEXT);
            CREATE TABLE execution (workspace_id TEXT, agent_id TEXT, status TEXT, executor_config_snapshot_json TEXT);
            CREATE TABLE agent_current (id TEXT, daemon_id TEXT);
            CREATE TABLE agent_chat_turn_job (responder_identity_id TEXT, status TEXT);
            INSERT INTO daemon VALUES ('provider','Host','capacity-test-host',100,NULL,NULL);
            INSERT INTO execution VALUES (NULL,NULL,'running','{"daemon_id":"provider"}');"#)
            .execute(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        db.server_run_cap.set(Some(1), 1, "capacity-test-host");
        let service = OperatorStatusService::new_for_test(db.clone());
        let pressure = service.daemon_pressure().await.unwrap();
        assert_eq!(pressure.len(), 1);
        assert_eq!(pressure[0].daemon_id, "server_host");
        assert_eq!(pressure[0].active_runs, 1);
        assert_eq!(pressure[0].max_concurrent_runs, Some(1));
        assert!(pressure[0].at_capacity);
    }

    async fn seed_project_repo(db: &SqliteDb) -> (String, String) {
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let now = Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO project (id, name, settings, workflow_definition, created_at, updated_at)
             VALUES (?, ?, '{}', '{}', ?, ?)",
        )
        .bind(&project_id)
        .bind(format!("Project {project_id}"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("project inserts");

        sqlx::query(
            "INSERT INTO repo (id, project_id, name, remote_url, local_path, default_branch, created_at, updated_at)
             VALUES (?, ?, ?, ?, NULL, 'main', ?, ?)",
        )
        .bind(&repo_id)
        .bind(&project_id)
        .bind(format!("Repo {repo_id}"))
        .bind(format!("https://example.com/{repo_id}.git"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("repo inserts");
        sqlx::query("UPDATE project SET primary_repo_id = ? WHERE id = ?")
            .bind(&repo_id)
            .bind(&project_id)
            .execute(db.pool())
            .await
            .expect("project primary repo updates");

        (project_id, repo_id)
    }

    async fn insert_task(db: &SqliteDb, status: &str) -> String {
        let (project_id, _repo_id) = seed_project_repo(db).await;
        let task_id = new_uuid_v4();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO task (id, project_id, title, status, priority, created_at, updated_at)
             VALUES (?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(&task_id)
        .bind(&project_id)
        .bind(format!("Task {task_id}"))
        .bind(status)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("task inserts");
        task_id
    }

    async fn insert_execution(
        db: &SqliteDb,
        task_id: &str,
        status: &str,
        error: Option<&str>,
        updated_at: DateTime<Utc>,
    ) -> String {
        let execution_id = new_uuid_v4();
        let now = updated_at.to_rfc3339();
        sqlx::query(
            "INSERT INTO execution (id, task_id, role, status, error, created_at, updated_at, stopped_at)
             VALUES (?, ?, 'executor', ?, ?, ?, ?, ?)",
        )
        .bind(&execution_id)
        .bind(task_id)
        .bind(status)
        .bind(error)
        .bind(&now)
        .bind(&now)
        .bind((status != "running").then_some(now.as_str()))
        .execute(db.pool())
        .await
        .expect("execution inserts");
        execution_id
    }

    async fn insert_rejected_transition(db: &SqliteDb, task_id: &str) {
        let transition_id = new_uuid_v4();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, rejection, created_at)
             VALUES (?, ?, 'review', 'in_progress', NULL, 'system', 'fixture rejection', 1, ?)",
        )
        .bind(&transition_id)
        .bind(task_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("transition log inserts");
    }

    async fn insert_daemon(db: &SqliteDb, status: &str) -> String {
        let daemon_id = new_uuid_v4();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO daemon (id, machine_id, hostname, os, arch, labels_json, status, detected_clis_json, created_at, updated_at)
             VALUES (?, ?, 'host', 'macos', 'aarch64', '{}', ?, '[]', ?, ?)",
        )
        .bind(&daemon_id)
        .bind(format!("machine-{daemon_id}"))
        .bind(status)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("daemon inserts");
        daemon_id
    }

    async fn insert_workspace_at_path(
        db: &SqliteDb,
        task_id: &str,
        status: &str,
        cleanup_after: &str,
        worktree_path: &str,
    ) -> String {
        let repo_id = sqlx::query_scalar::<_, String>(
            "SELECT p.primary_repo_id
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ?",
        )
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .expect("project repo exists");
        let workspace_id = new_uuid_v4();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, cleanup_after, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'main', ?, ?, ?, ?)",
        )
        .bind(&workspace_id)
        .bind(task_id)
        .bind(&repo_id)
        .bind(worktree_path)
        .bind(status)
        .bind(cleanup_after)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("workspace inserts");
        workspace_id
    }

    async fn insert_workspace(db: &SqliteDb, task_id: &str, status: &str, cleanup_after: &str) {
        let workspace_path = format!("/tmp/worktree-{}", new_uuid_v4());
        insert_workspace_at_path(db, task_id, status, cleanup_after, &workspace_path).await;
    }

    #[tokio::test]
    async fn notify_and_project_hook_workers_expose_health_and_live_lag() {
        let (db, operator) = test_service().await;
        let bus = Arc::new(events::EventBus::new(1));
        let notifications = Arc::new(crate::NotificationService::new(
            Arc::clone(&db),
            Arc::clone(&bus),
        ));
        let hooks = Arc::new(crate::ProjectHookService::new(
            Arc::clone(&db),
            Arc::clone(&bus),
            Arc::new(crate::TaskService::new(Arc::clone(&db), bus)),
            Arc::clone(&notifications),
        ));
        crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), notifications)
            .run_once(1)
            .await
            .unwrap();
        crate::worker_runtime::WorkerRuntime::new(Arc::clone(&db), hooks)
            .run_once(1)
            .await
            .unwrap();
        operator.set_runtime_workers(&[
            crate::RuntimeWorker::NotificationProjection,
            crate::RuntimeWorker::ProjectHooks,
        ]);
        let event_id = new_uuid_v4();
        db::DomainEventRepo::append_event(
            &*db,
            db::CreateDomainEvent::task_transition(
                event_id,
                "lag-task",
                "lag-project",
                "review",
                "done",
                Some("complete"),
                "system",
                "done",
                false,
                db::now_rfc3339(),
                serde_json::json!({}),
            ),
        )
        .await
        .unwrap();
        let status = operator.compute_status().await.unwrap();
        assert_eq!(status.event_consumers.len(), 2);
        for name in ["notifications", "project-hooks"] {
            let consumer = status
                .event_consumers
                .iter()
                .find(|consumer| consumer.consumer_name == name)
                .unwrap();
            assert_eq!(consumer.lag, 1);
            let subscription: Option<String> = sqlx::query_scalar(
                "SELECT subscription_json FROM worker_health WHERE worker_name = ?",
            )
            .bind(name)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert!(subscription.unwrap().contains("task.transitioned"));
        }
    }

    #[tokio::test]
    async fn empty_db_is_healthy() {
        let (_db, service) = test_service().await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
        assert!(status.active_executions.is_empty());
        assert!(status.blocked_tasks.is_empty());
        assert!(status.daemon_issues.is_empty());
        assert!(status.workspace_cleanup.is_empty());
        assert!(status.retry_pressure.is_empty());
        assert!(status.recent_errors.is_empty());
    }

    #[tokio::test]
    async fn usage_summary_keeps_active_count_live_on_a_memo_hit() {
        let (_db, service) = test_service().await;
        let first = service.usage_summary(0).await.unwrap();
        let second = service.usage_summary(17).await.unwrap();
        assert_eq!(first.active_execution_count, 0);
        assert_eq!(second.active_execution_count, 17);
        assert_eq!(
            serde_json::to_value(first.cost).unwrap(),
            serde_json::to_value(second.cost).unwrap()
        );
    }

    #[tokio::test]
    async fn running_execution_healthy() {
        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "todo").await;
        insert_execution(&db, &task_id, "running", None, Utc::now()).await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
        assert_eq!(status.active_executions.len(), 1);
        assert_eq!(status.active_executions[0].task_id, task_id);
    }

    #[tokio::test]
    async fn running_execution_reports_plan_progress() {
        let workspace_parent = tempdir().expect("tempdir creates");
        let workspace_root = workspace_parent.path().join("worktree");
        fs::create_dir_all(&workspace_root).expect("workspace dir creates");
        fs::write(
            workspace_parent.path().join("plan.md"),
            "- [x] Task one\n- [x] Task two\n- [x] Task three\n- [ ] Task four\n- [ ] Task five\n",
        )
        .expect("plan writes");

        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "in_progress").await;
        let execution_id = insert_execution(&db, &task_id, "running", None, Utc::now()).await;
        let cleanup_after = (Utc::now() + Duration::hours(1)).to_rfc3339();
        let workspace_id = insert_workspace_at_path(
            &db,
            &task_id,
            "ready",
            &cleanup_after,
            workspace_root.to_str().expect("workspace path is utf-8"),
        )
        .await;

        sqlx::query("UPDATE execution SET workspace_id = ? WHERE id = ?")
            .bind(&workspace_id)
            .bind(&execution_id)
            .execute(db.pool())
            .await
            .expect("execution workspace updates");

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.active_executions.len(), 1);
        let progress = status.active_executions[0]
            .plan_progress
            .as_ref()
            .expect("plan progress exists");
        assert_eq!(progress.total, 5);
        assert_eq!(progress.completed, 3);
        assert_eq!(progress.remaining, 2);
        assert!(progress.available);
    }

    #[tokio::test]
    async fn blocked_task_severity() {
        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "blocked").await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Blocked);
        assert_eq!(status.blocked_tasks.len(), 1);
        assert_eq!(status.blocked_tasks[0].task_id, task_id);
    }

    #[tokio::test]
    async fn revision_two_daemon_waits_for_upgrade_and_operator_sees_action() {
        let (db, service) = test_service().await;
        let daemon_id = insert_daemon(&db, "online").await;
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let (connection, mut outbound) =
            crate::daemon_transport::DaemonConnection::new(daemon_id.clone());
        let id = connection.id();
        registry.register(daemon_id.clone(), connection);
        registry.dispatch_incoming_for_connection(&daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: serde_json::json!({"protocol_revision":2, "capabilities":["execution.terminal.usage_reports", "execution.terminal.ack"]}),
        });
        let api_types::DaemonFrame::Error { error, .. } = outbound.recv().await.unwrap() else {
            panic!("upgrade error expected")
        };
        assert_eq!(error.code, api_types::DAEMON_UPGRADE_REQUIRED);
        assert_eq!(error.message, api_types::DAEMON_UPGRADE_REQUIRED_MESSAGE);
        let result: Result<Value, ServiceError> = registry
            .send_request(
                &daemon_id,
                api_types::METHOD_EXECUTION_START,
                serde_json::json!({}),
                1,
            )
            .await;
        assert!(matches!(
            result,
            Err(ServiceError::DaemonUpgradeRequired { .. })
        ));
        assert!(outbound.try_recv().is_err());
        let service = service.with_daemon_connections(registry);
        service.set_runtime_workers(&crate::runtime::COMMON_WORKERS);
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('upgrade-lag', 'test', 'test', 'test', 'system', 'system', 'system', 'test', '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(status
            .recent_errors
            .iter()
            .any(|issue| issue.entity_type == "event_consumer"));
        assert!(status.database.incremental_vacuum);
        let issue = status
            .daemon_issues
            .iter()
            .find(|issue| issue.daemon_id == daemon_id)
            .unwrap();
        assert_eq!(issue.issue, format!("upgrade_required: {}", error.message));
        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
    }

    #[tokio::test]
    async fn offline_daemon_attention() {
        let (db, service) = test_service().await;
        let daemon_id = insert_daemon(&db, "offline").await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
        assert_eq!(status.daemon_issues.len(), 1);
        assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
        assert_eq!(status.daemon_issues[0].issue, "offline");
        assert_eq!(
            status.daemon_issues[0].severity,
            OperatorSeverity::Attention
        );
    }

    #[tokio::test]
    async fn error_daemon_reports_daemon_issue() {
        let (db, service) = test_service().await;
        // This fixture exercises a forward-compatible status value consumed by
        // operator status even though current daemon lifecycle only emits
        // online/offline.
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(db.pool())
            .await
            .expect("check constraints disabled for fixture");
        let daemon_id = insert_daemon(&db, "error").await;
        sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(db.pool())
            .await
            .expect("check constraints restored");

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Error);
        assert_eq!(status.daemon_issues.len(), 1);
        assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
        assert_eq!(status.daemon_issues[0].issue, "error");
        assert_eq!(status.daemon_issues[0].severity, OperatorSeverity::Error);
    }

    #[tokio::test]
    async fn cleanup_backlog_attention() {
        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "done").await;
        let cleanup_after = (Utc::now() - Duration::hours(1)).to_rfc3339();
        insert_workspace(&db, &task_id, "ready", &cleanup_after).await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
        assert_eq!(status.workspace_cleanup.len(), 1);
        assert_eq!(status.workspace_cleanup[0].task_id, task_id);
    }

    #[tokio::test]
    async fn failed_plus_blocked_error() {
        let (db, service) = test_service().await;
        let blocked_task_id = insert_task(&db, "blocked").await;
        let failed_task_id = insert_task(&db, "todo").await;
        insert_execution(
            &db,
            &failed_task_id,
            "failed",
            Some("executor failed"),
            Utc::now(),
        )
        .await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Error);
        assert_eq!(status.blocked_tasks.len(), 1);
        assert_eq!(status.blocked_tasks[0].task_id, blocked_task_id);
        assert_eq!(status.recent_errors.len(), 1);
        assert_eq!(status.recent_errors[0].entity_type, "task");
        assert_eq!(status.recent_errors[0].entity_id, failed_task_id);
    }

    #[tokio::test]
    async fn terminal_tasks_do_not_report_stale_execution_errors() {
        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "done").await;
        insert_execution(
            &db,
            &task_id,
            "failed",
            Some("superseded by successful retry"),
            Utc::now(),
        )
        .await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert!(status.recent_errors.is_empty());
        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    }

    #[tokio::test]
    async fn terminal_tasks_do_not_report_retry_pressure() {
        let (db, service) = test_service().await;
        let done_task_id = insert_task(&db, "done").await;
        let active_task_id = insert_task(&db, "review").await;
        insert_rejected_transition(&db, &done_task_id).await;
        insert_rejected_transition(&db, &active_task_id).await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.retry_pressure.len(), 1);
        assert_eq!(status.retry_pressure[0].task_id, active_task_id);
    }

    #[tokio::test]
    async fn mixed_operational_indicators_use_highest_severity() {
        let (db, service) = test_service().await;
        let running_task_id = insert_task(&db, "in_progress").await;
        insert_execution(&db, &running_task_id, "running", None, Utc::now()).await;

        let blocked_task_id = insert_task(&db, "blocked").await;
        let daemon_id = insert_daemon(&db, "offline").await;

        let cleanup_task_id = insert_task(&db, "done").await;
        let cleanup_after = (Utc::now() - Duration::hours(1)).to_rfc3339();
        insert_workspace(&db, &cleanup_task_id, "ready", &cleanup_after).await;

        let failed_task_id = insert_task(&db, "todo").await;
        insert_execution(
            &db,
            &failed_task_id,
            "failed",
            Some("executor failed"),
            Utc::now(),
        )
        .await;

        sync_fixtures(&service).await;
        let status = service.compute_status().await.expect("status computes");

        assert_eq!(status.overall_severity, OperatorSeverity::Error);
        assert!(!status.active_executions.is_empty());
        assert!(!status.blocked_tasks.is_empty());
        assert_eq!(status.blocked_tasks[0].task_id, blocked_task_id);
        assert!(!status.daemon_issues.is_empty());
        assert_eq!(status.daemon_issues[0].daemon_id, daemon_id);
        assert!(!status.workspace_cleanup.is_empty());
    }

    #[tokio::test]
    async fn blocked_annotation_preserves_workflow_phase_and_clears_independently() {
        let (db, service) = test_service().await;
        let task_id = insert_task(&db, "review").await;
        sqlx::query("UPDATE task SET blocked_json = ?, error_annotation = ? WHERE id = ?")
            .bind(r#"{"reason":"dependency unavailable"}"#)
            .bind("old diagnostic")
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.overall_severity, OperatorSeverity::Blocked);
        assert_eq!(status.blocked_tasks.len(), 1);
        assert_eq!(status.blocked_tasks[0].task_id, task_id);
        assert_eq!(
            status.blocked_tasks[0].blocked_reason.as_deref(),
            Some("old diagnostic")
        );
        let phase: String = sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(phase, "review");
        sqlx::query("UPDATE task SET blocked_json = NULL WHERE id = ?")
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        sync_fixtures(&service).await;
        assert!(service
            .compute_status()
            .await
            .unwrap()
            .blocked_tasks
            .is_empty());
    }

    #[tokio::test]
    async fn inactive_tasks_do_not_report_stale_blocked_annotations() {
        let (db, service) = test_service().await;
        for phase in ["done", "cancelled", "review", "todo"] {
            let task_id = insert_task(&db, phase).await;
            sqlx::query(
                "UPDATE task SET blocked_json = '{\"reason\":\"old blocker\"}',
                 archived_at = CASE WHEN status = 'review' THEN ? ELSE NULL END,
                 deleted_at = CASE WHEN status = 'todo' THEN ? ELSE NULL END WHERE id = ?",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(Utc::now().to_rfc3339())
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        }
        sync_fixtures(&service).await;
        assert!(service
            .compute_status()
            .await
            .unwrap()
            .blocked_tasks
            .is_empty());
    }
    #[tokio::test]
    async fn consumer_lag_counts_live_rows_and_stalls_use_existing_attention_alerts() {
        let (db, service) = test_service().await;
        let now = Utc::now();
        let old = (now - Duration::minutes(10)).to_rfc3339();
        sqlx::query("INSERT INTO domain_event (sequence, id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES (1, 'processed', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?), (10, 'pending', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(&old).bind(&old).execute(db.pool()).await.unwrap();
        for consumer in crate::runtime::COMMON_WORKERS
            .into_iter()
            .filter_map(|worker| worker.event_consumer_name())
        {
            sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 10, ?) ON CONFLICT(consumer_name) DO UPDATE SET last_sequence = 10, updated_at = excluded.updated_at")
                .bind(consumer).bind(&old).execute(db.pool()).await.unwrap();
        }
        sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 1 WHERE consumer_name = 'attention_projection'").execute(db.pool()).await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        let stalled = status
            .event_consumers
            .iter()
            .find(|c| c.consumer_name == "attention_projection")
            .unwrap();
        assert_eq!(stalled.lag, 1); // Deleted sequence gaps do not count.
        assert!(stalled.stalled);
        assert!(stalled.oldest_unprocessed_age_seconds.unwrap() >= 600.0);
        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
        assert_eq!(status.recent_errors.len(), 1);
        assert_eq!(status.recent_errors[0].entity_type, "event_consumer");
        assert_eq!(
            status.recent_errors[0].severity,
            OperatorSeverity::Attention
        );
        assert!(status.database.incremental_vacuum);
        assert_eq!(status.event_consumers.len(), 7);
        assert!(status
            .event_consumers
            .iter()
            .filter(|c| c.consumer_name != "attention_projection")
            .all(|c| c.lag == 0
                && !c.stalled
                && c.oldest_unprocessed_at.is_none()
                && c.last_advanced_at.as_deref() == Some(old.as_str())));
        sqlx::query("UPDATE event_consumer_cursor SET last_sequence = 10, updated_at = ? WHERE consumer_name = 'attention_projection'").bind(now.to_rfc3339()).execute(db.pool()).await.unwrap();
        sync_fixtures(&service).await;
        let recovered = service.compute_status().await.unwrap();
        assert_eq!(recovered.overall_severity, OperatorSeverity::Healthy);
        assert!(recovered.recent_errors.is_empty());
        assert!(recovered
            .event_consumers
            .iter()
            .all(|c| c.lag == 0 && !c.stalled));
    }

    #[tokio::test]
    async fn consumer_migrated_health_reports_live_lag_and_age_without_worker_updates() {
        let (db, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::Memory]);
        let now = Utc::now();
        let old = (now - Duration::minutes(10)).to_rfc3339();
        let name = crate::memory_consumer_name();
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, 0, ?)")
            .bind(name).bind(&old).execute(db.pool()).await.unwrap();
        crate::AgentChatMemoryConsumer::new(Arc::clone(&db))
            .run_once(1)
            .await
            .unwrap();
        // Model a worker that has been running for ten minutes; a newly
        // initialized worker gets time to drain an upgraded checkpoint.
        sqlx::query("UPDATE worker_health SET created_at = ? WHERE worker_name = ?")
            .bind(&old)
            .bind(name)
            .execute(db.pool())
            .await
            .unwrap();
        // No health update follows either append, as when a handler is hung.
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type,
            scope_type, scope_id, correlation_id, created_at) VALUES
            ('wanted', 'agent_chat.message.admitted', 'test', 'test', 'system', 'system', 'system', 'test', ?),
            ('ignored', 'irrelevant', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(&old).bind(&old).execute(db.pool()).await.unwrap();
        let status = service.event_consumers(now).await.unwrap();
        let memory = status.iter().find(|c| c.consumer_name == name).unwrap();
        assert_eq!(memory.lag, 1);
        assert_eq!(memory.oldest_unprocessed_at.as_deref(), Some(old.as_str()));
        assert!(memory.oldest_unprocessed_age_seconds.unwrap() >= 600.0);
        assert!(memory.stalled);
        let later = service
            .event_consumers(now + Duration::minutes(1))
            .await
            .unwrap();
        let memory = later.iter().find(|c| c.consumer_name == name).unwrap();
        assert!(memory.oldest_unprocessed_age_seconds.unwrap() >= 660.0);
        crate::worker_runtime::WorkerHealth::new(Arc::clone(&db), name)
            .report_error("persistent worker failure")
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(status
            .recent_errors
            .iter()
            .any(|issue| issue.entity_id == name
                && issue.error.contains("persistent worker failure")));
        assert_eq!(status.overall_severity, OperatorSeverity::Attention);
        crate::AgentChatMemoryConsumer::new(Arc::clone(&db))
            .run_once(10)
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(!status
            .recent_errors
            .iter()
            .any(|issue| issue.error.contains("persistent worker failure")));
        // Skip has a lazy checkpoint. Simulate that subsequent flush to test
        // recovery against the sole cursor authority, not a health copy.
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        db.advance_domain_event_cursor_in_tx(&mut tx, name, 0, 2, &now.to_rfc3339())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(status.recent_errors.is_empty());
        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    }

    #[tokio::test]
    async fn consumer_deferral_text_and_dead_letters_have_distinct_operator_lifetimes() {
        let (db, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::Memory]);
        let name = crate::memory_consumer_name();
        crate::AgentChatMemoryConsumer::new(Arc::clone(&db))
            .run_once(1)
            .await
            .unwrap();
        let health = db::WorkerHealth::new(Arc::clone(&db), name);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        health
            .defer_in_tx(
                &mut tx,
                "1",
                std::time::Duration::from_secs(3600),
                "waiting for settlement",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        let issue = status
            .recent_errors
            .iter()
            .find(|i| i.entity_id == name)
            .unwrap();
        assert!(issue.error.contains("waiting for settlement"));
        assert!(issue.error.contains("Deferred since"));
        assert_eq!(issue.severity, OperatorSeverity::Healthy);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        health.clear_pending_in_tx(&mut tx).await.unwrap();
        health
            .dead_letter_in_tx(
                &mut tx,
                db::WorkItem {
                    source_key: "1",
                    item_type: "test",
                },
                db::FailureState {
                    attempts: 8,
                    first_failed_at: "2000-01-01T00:00:00Z",
                },
                "poison event",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(status
            .recent_errors
            .iter()
            .any(|i| i.entity_type == "worker_dead_letter" && i.error.contains("poison event")));
        sqlx::query("UPDATE worker_dead_letter SET dead_lettered_at = ?")
            .bind((Utc::now() - Duration::hours(2)).to_rfc3339())
            .execute(db.pool())
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert!(status.recent_errors.is_empty());
        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    }

    #[tokio::test]
    async fn consumer_stall_threshold_missing_cursors_and_empty_outbox_are_visible() {
        let (db, service) = test_service().await;
        let now = Utc::now();
        let empty = service.event_consumers(now).await.unwrap();
        assert_eq!(empty.len(), 7);
        assert!(empty.iter().all(|c| c.lag == 0 && !c.stalled));
        let old = (now - Duration::seconds(301)).to_rfc3339();
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('old', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)").bind(&old).execute(db.pool()).await.unwrap();
        let missing = service.event_consumers(now).await.unwrap();
        let consumer = missing
            .iter()
            .find(|c| c.consumer_name == "attention_projection")
            .unwrap();
        assert_eq!(consumer.lag, 1);
        assert_eq!(consumer.last_advanced_at, None);
        assert!(consumer.stalled);
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES ('attention_projection', 0, ?)").bind((now - Duration::seconds(300)).to_rfc3339()).execute(db.pool()).await.unwrap();
        let at_threshold = service.event_consumers(now).await.unwrap();
        assert!(
            !at_threshold
                .iter()
                .find(|c| c.consumer_name == "attention_projection")
                .unwrap()
                .stalled
        );
        let shorter = service
            .with_consumer_stall_seconds(299)
            .event_consumers(now)
            .await
            .unwrap();
        assert!(
            shorter
                .iter()
                .find(|c| c.consumer_name == "attention_projection")
                .unwrap()
                .stalled
        );
    }

    #[tokio::test]
    async fn idle_consumer_with_old_cursor_is_never_stalled() {
        let (db, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::Attention]);
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('processed', 'test', 'test', 'test', 'system', 'system', 'system', 'test', '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES ('attention_projection', 1, '2000-01-01T00:00:00Z')").execute(db.pool()).await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.event_consumers.len(), 1);
        let consumer = &status.event_consumers[0];
        assert_eq!(consumer.lag, 0);
        assert!(!consumer.stalled);
        assert_eq!(consumer.oldest_unprocessed_age_seconds, None);
        assert_eq!(
            consumer.last_advanced_at.as_deref(),
            Some("2000-01-01T00:00:00Z")
        );
        assert!(status.recent_errors.is_empty());
        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);

        // An event that just arrived is pending, not a stall, even though the
        // idle cursor's last advance is decades old.
        sqlx::query("INSERT INTO domain_event (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id, correlation_id, created_at) VALUES ('fresh', 'test', 'test', 'test', 'system', 'system', 'system', 'test', ?)")
            .bind(Utc::now().to_rfc3339())
            .execute(db.pool())
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.event_consumers[0].lag, 1);
        assert!(!status.event_consumers[0].stalled);
        assert!(status.recent_errors.is_empty());
    }
    #[tokio::test]
    async fn consumer_tail_has_no_durable_lag_entry_or_stall() {
        let (_, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::DomainEventBroadcast]);
        assert!(service
            .compute_status()
            .await
            .unwrap()
            .event_consumers
            .is_empty());
    }
    #[tokio::test]
    async fn audit2_dead_letter_history_does_not_expire_or_degrade_old_failures() {
        let (db, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::Coordination]);
        let name = crate::coordination_consumer_name();
        let health = db::WorkerHealth::new(Arc::clone(&db), name);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        health.ensure_in_tx(&mut tx).await.unwrap();
        for n in 0..7 {
            let key = format!("event:{n}:commitment:c-{n}");
            health
                .dead_letter_in_tx(
                    &mut tx,
                    db::WorkItem {
                        source_key: &key,
                        item_type: "coordination_commitment",
                    },
                    db::FailureState {
                        attempts: 0,
                        first_failed_at: "2000-01-01T00:00:00Z",
                    },
                    "domain rejection",
                )
                .await
                .unwrap();
        }
        sqlx::query("UPDATE worker_dead_letter SET dead_lettered_at = '2000-01-01T00:00:00Z'")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        let worker = &status.event_consumers[0];
        assert_eq!(worker.dead_letter_count, 7);
        assert_eq!(worker.recent_dead_letters.len(), 5);
        assert!(worker
            .recent_dead_letters
            .iter()
            .all(|item| !item.id.is_empty() && item.event_sequence.is_some()));
        let ids: Vec<_> = worker
            .recent_dead_letters
            .iter()
            .map(|item| item.id.clone())
            .collect();
        sync_fixtures(&service).await;
        let again = service.compute_status().await.unwrap();
        assert_eq!(
            again.event_consumers[0]
                .recent_dead_letters
                .iter()
                .map(|item| item.id.clone())
                .collect::<Vec<_>>(),
            ids
        );
        assert!(status.recent_errors.is_empty());
        assert_eq!(status.overall_severity, OperatorSeverity::Healthy);
    }
    #[tokio::test]
    async fn resolved_dead_letters_are_excluded_from_counts_recent_history_and_alerts() {
        let (db, service) = test_service().await;
        service.set_runtime_workers(&[crate::runtime::RuntimeWorker::Coordination]);
        let name = crate::coordination_consumer_name();
        let health = db::WorkerHealth::new(Arc::clone(&db), name);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        health.ensure_in_tx(&mut tx).await.unwrap();
        health
            .dead_letter_in_tx(
                &mut tx,
                db::WorkItem {
                    source_key: "12",
                    item_type: "task.done",
                },
                db::FailureState {
                    attempts: 8,
                    first_failed_at: &db::now_rfc3339(),
                },
                "old failure",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.event_consumers[0].dead_letter_count, 1);
        assert_eq!(status.event_consumers[0].recent_dead_letters[0].attempts, 8);
        assert!(status
            .recent_errors
            .iter()
            .any(|error| error.entity_type == "worker_dead_letter"));
        let id = status.event_consumers[0].recent_dead_letters[0].id.clone();
        crate::dead_letter_service::DeadLetterService::new(db.clone())
            .dismiss(
                crate::dead_letter_service::DeadLetterActor {
                    user_id: "admin",
                    is_admin: true,
                },
                &id,
                None,
            )
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.event_consumers[0].dead_letter_count, 0);
        assert!(status.event_consumers[0].recent_dead_letters.is_empty());
        assert!(!status
            .recent_errors
            .iter()
            .any(|error| error.entity_type == "worker_dead_letter"));
    }
    #[tokio::test]
    async fn task_step_queue_is_visible_with_counts_and_age() {
        use db::TaskStepRepo;
        let (db, service) = test_service().await;
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('queue-project','queue',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('queue-task','queue-project','queue','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        db.enqueue_step(&db::EnqueueTaskStep {
            kind: "cascade".into(),
            id: "queue-step".into(),
            task_id: "queue-task".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: "queue".into(),
            chain_id: "queue".into(),
            chain_position: 1,
            expected_status: "todo".into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: now.clone(),
        })
        .await
        .unwrap();
        // Resolved history does not count; a step the Task still points at does.
        for (id, status) in [("old-failure", "failed"), ("live-park", "parked")] {
            sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,completed_at) VALUES (?,'queue-task',(SELECT MAX(seq)+1 FROM task_step),'cascade','{}',?,?,1,'todo',1,?,?,?,?,?)")
                .bind(id).bind(id).bind(id).bind(status).bind(&now).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        }
        sqlx::query("UPDATE task SET error_annotation=? WHERE id='queue-task'")
            .bind(r#"{"type":"workflow_loop","task_step_id":"live-park"}"#)
            .execute(db.pool())
            .await
            .unwrap();
        sync_fixtures(&service).await;
        let status = service.compute_status().await.unwrap();
        assert_eq!(status.task_steps.worker_name, "task_steps");
        assert_eq!(status.task_steps.pending, 1);
        assert_eq!(status.task_steps.claimed, 0);
        assert_eq!(status.task_steps.failed, 0);
        assert_eq!(status.task_steps.parked, 1);
        assert!(status.task_steps.oldest_pending_age_seconds.is_some());
    }
}

#[cfg(test)]
mod condition_check_tests {
    async fn status(db: &std::sync::Arc<db::SqliteDb>) -> api_types::OperatorStatusResponse {
        super::OperatorStatusService::new_for_test(db.clone())
            .compute_status()
            .await
            .unwrap()
    }

    /// A check that found nothing to repair is a log line, never a row: the
    /// Operations empty state must stay empty on a healthy database.
    #[tokio::test]
    async fn clean_condition_check_adds_no_operator_row() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = std::sync::Arc::new(db::SqliteDb::new(pool));
        for _ in 0..3 {
            db.check_task_conditions(db::CONDITION_CHECK_PAGE)
                .await
                .unwrap();
        }
        let checks = db.condition_check_status();
        assert_eq!(checks.ticks, 3);
        assert!(checks.last_pass.is_some(), "a pass completed");
        let status = status(&db).await;
        assert!(status.recent_errors.is_empty());
        assert_eq!(
            status.overall_severity,
            api_types::OperatorSeverity::Healthy
        );
    }

    /// Only a completed pass that repaired a row is reported, and the row
    /// goes away with the next clean pass.
    #[tokio::test]
    async fn repairing_pass_is_reported_until_the_next_clean_pass() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = std::sync::Arc::new(db::SqliteDb::new(pool));
        let now = db::now_rfc3339();
        db::ProjectRepo::create(
            &*db,
            db::CreateProject {
                id: "p".into(),
                owner_id: None,
                name: "Conditions".into(),
                primary_repo_id: None,
                updated_at: now.clone(),
                settings: "{}".into(),
                workflow_definition: "{}".into(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
        for id in ["a", "b"] {
            db::TaskRepo::create(
                &*db,
                db::CreateTask {
                    id: id.into(),
                    project_id: "p".into(),
                    parent_task_id: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: id.into(),
                    description: None,
                    task_type: "task".into(),
                    status: "backlog".into(),
                    is_automation: false,
                    priority: 0,
                    task_state_config: None,
                    merge_config: None,
                    subtask_order: None,
                    plan: None,
                    updated_at: now.clone(),
                    created_at: now.clone(),
                },
            )
            .await
            .unwrap();
        }
        // Corrupt, not a newer encoding: the check restates it.
        sqlx::query("UPDATE task SET condition_json='{}' WHERE id='a'")
            .execute(db.pool())
            .await
            .unwrap();
        db.check_task_conditions(1).await.unwrap();
        assert!(
            status(&db).await.recent_errors.is_empty(),
            "a page that repaired is not yet a completed pass"
        );
        db.check_task_conditions(db::CONDITION_CHECK_PAGE)
            .await
            .unwrap();
        let reported = status(&db).await;
        let rows: Vec<_> = reported
            .recent_errors
            .iter()
            .filter(|row| row.entity_type == "task_condition_invariant")
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].severity, api_types::OperatorSeverity::Attention);
        assert_eq!(
            rows[0].error,
            "The last Task condition check repaired 1 of 2 Tasks"
        );
        assert_eq!(
            reported.overall_severity,
            api_types::OperatorSeverity::Attention
        );
        db.check_task_conditions(db::CONDITION_CHECK_PAGE)
            .await
            .unwrap();
        assert!(status(&db).await.recent_errors.is_empty());

        // A condition a newer build wrote is quarantined: never repaired, and
        // reported with its Task id for as long as it exists.
        let newer = r#"{"kind":"from_a_newer_build"}"#;
        sqlx::query("UPDATE task SET condition_json=? WHERE id='b'")
            .bind(newer)
            .execute(db.pool())
            .await
            .unwrap();
        for _ in 0..2 {
            db.check_task_conditions(db::CONDITION_CHECK_PAGE)
                .await
                .unwrap();
            let reported = status(&db).await;
            let rows: Vec<_> = reported.recent_errors.iter().collect();
            assert_eq!(rows.len(), 1, "{rows:?}");
            assert_eq!(rows[0].entity_type, "task_condition_quarantined");
            assert_eq!(rows[0].severity, api_types::OperatorSeverity::Attention);
            assert!(
                rows[0].error.starts_with("1 Task condition(s)") && rows[0].error.contains("(b)"),
                "{}",
                rows[0].error
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id='b'")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            newer
        );
    }
}
