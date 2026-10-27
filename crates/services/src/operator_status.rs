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
use db::{CheckRunRepo, IntegrationQueueRepo, SqliteDb, WorkspaceRepo};
use serde_json::Value;

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
    agent_event_consumer_names:
        Option<Arc<dyn crate::worker_runtime::assembly::AgentEventConsumerNames>>,
    event_relay: RwLock<Option<Arc<crate::DomainEventBroadcastConsumer>>>,
    relay_enabled: AtomicBool,
    daemon_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    workspace_backend_router: Arc<WorkspaceBackendRouter>,
    /// The start-time integration import that has not completed: its last
    /// error and when it first failed. While it is set the merge queue does
    /// not run (`RuntimeSupervisor::start` retries the import).
    integration_import: RwLock<Option<(String, String)>>,
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
            agent_event_consumer_names: None,
            event_relay: RwLock::new(None),
            relay_enabled: AtomicBool::new(false),
            daemon_connections: None,
            integration_import: RwLock::new(None),
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
            agent_event_consumer_names: None,
            event_relay: RwLock::new(None),
            relay_enabled: AtomicBool::new(false),
            daemon_connections: None,
            integration_import: RwLock::new(None),
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

    pub fn with_agent_event_consumer_names(
        mut self,
        names: Arc<dyn crate::worker_runtime::assembly::AgentEventConsumerNames>,
    ) -> Self {
        self.agent_event_consumer_names = Some(names);
        self
    }

    /// Monitor only durable consumers owned by the process's started workers.
    /// Persisted cursors from other assemblies are not evidence of a live worker.
    pub fn set_runtime_workers(&self, workers: &[crate::worker_runtime::assembly::RuntimeWorker]) {
        self.relay_enabled.store(
            workers.contains(&crate::worker_runtime::assembly::RuntimeWorker::DomainEventBroadcast),
            Ordering::SeqCst,
        );

        *self
            .expected_event_consumers
            .write()
            .expect("event consumer set lock") = workers
            .iter()
            .filter_map(|worker| {
                worker.event_consumer_name(self.agent_event_consumer_names.as_deref())
            })
            .collect();
    }

    /// `Some(error)`: the start-time integration import failed and is being
    /// retried; `None`: it completed. Reported as the `integration_import`
    /// entry of `recent_errors` for as long as it is set.
    pub fn set_integration_import_failure(&self, error: Option<String>) {
        let mut state = self.integration_import.write().expect("import status lock");
        *state = error.map(|error| {
            let since = state
                .as_ref()
                .map_or_else(db::now_rfc3339, |(_, since)| since.clone());
            (error, since)
        });
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
        let pending_remote_cancels: i64 = self.db.pending_remote_cancel_count().await?;
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

        // A Task parked because its checks produced no verdict is an
        // infrastructure problem the operator must see: it is not a failure
        // of the change, and only `retry` or `cancel` ends it.
        for row in self.db.tasks_with_exhausted_infrastructure_checks().await? {
            let condition = db::task_condition::decode_or_unknown(&row.condition_json.clone());
            if !condition
                .check_wait()
                .is_some_and(|wait| wait.requires_intervention())
            {
                continue;
            }
            recent_errors.push(RecentErrorSummary {
                entity_type: "task_check_exhausted".to_owned(),
                entity_id: row.id.clone(),
                error: format!(
                    "Checks for Task \"{}\" produced no result after the automatic retries. Retry the Task's checks or cancel it",
                    row.title.clone()
                ),
                occurred_at: row.updated_at.clone(),
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
        // A workspace root this database does not own is never garbage
        // collected: say so for as long as it lasts, the disk fills silently
        // otherwise.
        let gc_status = self
            .db
            .setting_value_with_updated_at(crate::workspace_cleanup::GC_STATUS_KEY)
            .await?;
        if let Some((value, since)) = gc_status {
            let status: Value = serde_json::from_str(&value).unwrap_or_default();
            let state = status["state"].as_str().unwrap_or("unknown");
            if state != "owned" {
                recent_errors.push(RecentErrorSummary {
                    severity: api_types::OperatorSeverity::Attention,
                    entity_type: "workspace_gc".into(),
                    entity_id: status["root"].as_str().unwrap_or_default().to_owned(),
                    error: format!(
                        "Workspace garbage collection is off ({state}{}): nothing under the workspace root is reclaimed and the disk can fill. The root is owned by another Forge database or cannot be a workspace root. If it belongs to this server (its database was reset), stop Forge and start it once with --reclaim-workspace-gc",
                        status["reason"]
                            .as_str()
                            .map(|reason| format!(": {reason}"))
                            .unwrap_or_default(),
                    ),
                    occurred_at: since,
                });
            }
        }
        // Tasks configured before v0.14 can still name an auditor
        // (`review.auditor_agent_id`). Nothing runs an auditor pass any more
        // and new configurations that name one are refused: say which Tasks
        // still carry it, for as long as they do.
        let auditor_tasks: Vec<String> = self.db.open_tasks_with_retired_auditor().await?;
        if !auditor_tasks.is_empty() {
            recent_errors.push(RecentErrorSummary {
                severity: api_types::OperatorSeverity::Attention,
                entity_type: "retired_review_auditor_config".into(),
                entity_id: auditor_tasks[0].clone(),
                error: format!(
                    "{}{} open Task(s) still name an auditor ({}{}). {}",
                    auditor_tasks.len().min(10),
                    if auditor_tasks.len() > 10 { "+" } else { "" },
                    auditor_tasks[..auditor_tasks.len().min(10)].join(", "),
                    if auditor_tasks.len() > 10 {
                        ", ..."
                    } else {
                        ""
                    },
                    crate::task_service::RETIRED_AUDITOR_CONFIG,
                ),
                occurred_at: now.to_rfc3339(),
            });
        }
        // The start-time import of merges that were in flight before the
        // queue has not completed: nothing merges until it does.
        let import = self
            .integration_import
            .read()
            .expect("import status lock")
            .clone();
        if let Some((error, since)) = import {
            recent_errors.push(RecentErrorSummary {
                severity: api_types::OperatorSeverity::Error,
                entity_type: "integration_import".into(),
                entity_id: "start".into(),
                error: format!(
                    "The import of merges that were in progress when the server was upgraded has not completed: {error}. The merge queue is not running and no Task is admitted to it; the server retries the import every 30 seconds."
                ),
                occurred_at: since,
            });
        }
        // A root in the system temp directory loses worktrees to the
        // operating system's cleaner: say so until the operator moves it.
        let root_status = self
            .db
            .setting_value_with_updated_at(crate::workspace_root::STATUS_KEY)
            .await?;
        if let Some((value, since)) = root_status {
            let status: Value = serde_json::from_str(&value).unwrap_or_default();
            let root = status["root"].as_str().unwrap_or_default().to_owned();
            if status["state"].as_str() == Some("system_temp") {
                recent_errors.push(RecentErrorSummary {
                    severity: api_types::OperatorSeverity::Attention,
                    entity_type: "workspace_root".into(),
                    error: crate::workspace_root::system_temp_warning(
                        std::path::Path::new(&root),
                        status["migrate_command"]
                            .as_str()
                            .unwrap_or(crate::workspace_root::MIGRATE_COMMAND),
                    ),
                    entity_id: root.clone(),
                    occurred_at: since.clone(),
                });
            }
            // What the last start found and went on with: stored paths
            // outside the root, a root that was missing, a foreign owner.
            for warning in status["warnings"].as_array().into_iter().flatten() {
                if let Some(warning) = warning.as_str() {
                    recent_errors.push(RecentErrorSummary {
                        severity: api_types::OperatorSeverity::Attention,
                        entity_type: "workspace_root".into(),
                        error: warning.to_owned(),
                        entity_id: root.clone(),
                        occurred_at: since.clone(),
                    });
                }
            }
        }
        for issue in &recent_errors {
            raise_severity(&mut overall_severity, issue.severity.clone());
        }
        if daemon_error_count > 0 {
            raise_severity(&mut overall_severity, OperatorSeverity::Error);
        }

        let integration_counts = self.db.integration_queue_counts().await?;
        let integration_now = Utc::now();
        let integration_ages = db::IntegrationActivationRepo::integration_queue_ages(
            &*self.db,
            &integration_now.to_rfc3339(),
        )
        .await?;
        let integration_samples = db::IntegrationActivationRepo::integration_timing_samples(
            &*self.db,
            &(integration_now - Duration::seconds(INTEGRATION_TIMING_WINDOW_SECONDS)).to_rfc3339(),
            INTEGRATION_TIMING_SAMPLE_LIMIT,
        )
        .await?;
        let integration_backlog = |age: db::IntegrationAge| api_types::IntegrationBacklog {
            count: age.count,
            oldest_age_seconds: age.oldest_seconds,
        };
        let integration_queues =
            api_types::IntegrationQueueStatus {
                queues_by_state: integration_counts.queues_by_state,
                current_attempts_by_state: integration_counts.current_attempts_by_state,
                quarantined_imports: integration_counts.quarantined_imports,
                waiting: integration_backlog(integration_ages.queued),
                heads: integration_backlog(integration_ages.heads),
                parked: integration_backlog(integration_ages.parked),
                timings: integration_timing_status(&integration_samples),
                finished_with_receipts:
                    db::IntegrationActivationRepo::integration_evidence_retained(&*self.db).await?,
            };
        let check_counts = self.db.check_run_counts().await?;
        let check_summary = self.db.check_operator_summary().await?;
        let backlog = |(count, oldest_age_seconds): db::CheckBacklogRow| api_types::CheckBacklog {
            count,
            oldest_age_seconds,
        };
        let checks = api_types::ChecksStatus {
            by_state: check_counts.by_state,
            reusable_results: check_counts.reusable_results,
            admitted_runs: check_counts.admitted_runs,
            borrowed_runs: check_counts.borrowed_runs,
            queued: backlog(check_summary.queued),
            running: backlog(check_summary.running),
            waiting_for_slot: backlog(check_summary.waiting_for_slot),
            waiting_for_disk: backlog(check_summary.waiting_for_disk),
            reuse_hits_24h: check_summary.reuse_hits_24h,
            runs_24h: check_summary.runs_24h,
            last_worker_error: periodic_workers
                .iter()
                .find(|worker| worker.worker_name == crate::check_runner::worker::WORKER_NAME)
                .and_then(|worker| {
                    Some(api_types::CheckWorkerError {
                        error: worker.last_error.clone()?,
                        occurred_at: worker.last_error_at.clone()?,
                    })
                }),
        };
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
            integration_queues,
            checks,
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
        let (pending, claimed, oldest): (i64, i64, Option<String>) =
            self.db.pending_and_claimed_task_step_counts().await?;
        let (failed, parked): (i64, i64) =
            self.db.current_failed_and_parked_task_step_counts().await?;
        let (last_error, last_error_at, restart_count): (Option<String>, Option<String>, i64) =
            self.db.task_step_worker_error().await?.unwrap_or_default();
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
        let rows = self.db.active_execution_operator_rows().await?;

        let mut active_executions = Vec::with_capacity(rows.len());
        for row in rows {
            let started_at: String = row.started_at.clone();
            let workspace_id: Option<String> = row.workspace_id.clone();
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
            let snapshot_json: Option<String> = row.executor_config_snapshot_json.clone();
            let effective_policy =
                effective_policy(snapshot_json.as_deref(), workspace_path.as_deref());
            let rate_limit_snapshot = rate_limit_snapshot(snapshot_json.as_deref());
            let plan_progress = match resolved.as_ref() {
                Some(workspace) => plan_progress(workspace).await?,
                None => None,
            };
            let log_snapshot = self.log_snapshots.read(row.logs_path.clone()).await;
            let execution_id: String = row.execution_id.clone();
            let usage = usage_aggregate_for_source_state(&self.db, &execution_id, true).await?;
            let token_totals = Some(TokenTotalsSummary {
                tokens: usage.tokens.clone(),
                cost: usage.cost.clone(),
            });

            active_executions.push(ActiveExecutionSummary {
                execution_id,
                task_id: row.task_id.clone(),
                task_title: row.task_title.clone(),
                role: row.role.clone(),
                agent_id: row.agent_id.clone(),
                agent_name: row.agent_name.clone(),
                daemon_id,
                workspace_id,
                workspace_path,
                session_id: row.agent_session_id.clone(),
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
        let rows = self.db.blocked_task_operator_rows().await?;

        rows.into_iter()
            .map(|row| {
                let condition = db::task_condition::decode_or_unknown(&row.condition_json.clone());
                Ok(BlockedTaskSummary {
                    task_id: row.id.clone(),
                    title: row.title.clone(),
                    blocked_reason: condition.read().operator_reason,
                    blocked_since: Some(row.updated_at.clone()),
                })
            })
            .collect()
    }

    async fn daemon_issues(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<DaemonIssueSummary>, ServiceError> {
        let stale_before = now - Duration::minutes(5);
        let rows = self.db.daemon_operator_issue_rows().await?;

        rows.into_iter()
            .filter(|row| {
                let id: String = row.id.clone();
                let status: String = row.status.clone();
                let last: Option<String> = row.last_report_at.clone();
                matches!(status.as_str(), "offline" | "error")
                    || last.is_some_and(|last| last < stale_before.to_rfc3339())
                    || self
                        .daemon_connections
                        .as_ref()
                        .and_then(|registry| registry.get(&id))
                        .is_some_and(|connection| connection.needs_upgrade())
            })
            .map(|row| {
                let status: String = row.status.clone();
                let last_report_at: Option<String> = row.last_report_at.clone();
                let (issue, severity) = match status.as_str() {
                    "error" => ("error".to_owned(), OperatorSeverity::Error),
                    "offline" => ("offline".to_owned(), OperatorSeverity::Attention),
                    _ => ("stale".to_owned(), OperatorSeverity::Attention),
                };
                let id: String = row.id.clone();
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
                    hostname: row.hostname.clone(),
                    issue,
                    severity,
                    detected_at: last_report_at.or_else(|| Some(row.updated_at.clone())),
                })
            })
            .collect()
    }

    async fn daemon_error_count(&self) -> Result<i64, ServiceError> {
        Ok(self.db.daemon_error_count().await?)
    }

    async fn daemon_pressure(&self) -> Result<Vec<DaemonPressureSummary>, ServiceError> {
        let budget = self.run_process_policy.get();
        let mut disks: std::collections::HashMap<Option<String>, api_types::MachineDisk> =
            db::machine_disk::list_machine_disks(&self.db)
                .await?
                .into_iter()
                .map(|row| (row.daemon_id, row.disk))
                .collect();
        Ok(crate::placement::machine_precheck::snapshot(&self.db)
            .await?
            .into_iter()
            .map(|row| DaemonPressureSummary {
                disk: disks.remove(&row.daemon_id),
                daemon_id: row
                    .daemon_id
                    .clone()
                    .unwrap_or_else(|| "server_host".to_owned()),
                hostname: Some(row.hostname),
                active_runs: row.capacity.active_runs().max(0) as u32,
                check_runs: row.capacity.check_runs.max(0) as u32,
                borrowed_check_runs: row.capacity.borrowed_check_runs.max(0) as u32,
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
        let rows = self.db.agent_operator_pressure_rows().await?;

        let mut pressure = Vec::new();
        for row in rows {
            let active_tasks = row.running_executions.max(0) as u32;
            let max_concurrent_tasks = row.max_concurrent_tasks.max(0) as u32;
            let at_capacity = max_concurrent_tasks > 0 && active_tasks >= max_concurrent_tasks;
            if active_tasks == 0 && !at_capacity {
                continue;
            }
            pressure.push(AgentPressureSummary {
                agent_id: row.agent_id.clone(),
                agent_name: row.agent_name.clone(),
                daemon_id: row.daemon_id.clone(),
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
        let rows = self
            .db
            .overdue_workspace_cleanup_rows(&(now.to_rfc3339()))
            .await?;

        let mut backlog = Vec::with_capacity(rows.len());
        for row in rows {
            let workspace_id: String = row.id.clone();
            let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.clone()))?;
            let resolved = self
                .workspace_backend_router
                .resolve(&self.db, &workspace)
                .await?;
            backlog.push(WorkspaceCleanupSummary {
                workspace_id,
                task_id: row.task_id.clone(),
                worktree_path: Some(resolved.handle()?.to_owned()),
                cleanup_after: row.cleanup_after.clone(),
            });
        }
        Ok(backlog)
    }

    async fn retry_pressure(&self) -> Result<Vec<RetryPressureSummary>, ServiceError> {
        // `attempt_count` counts transition rejections, as it always has; the
        // Execution spend comes from the budget ledger. Both are page queries.
        let rows = self.db.task_operator_retry_rows().await?;

        let retried = rows
            .iter()
            .filter(|row| row.execution_spent > 0)
            .map(|row| row.task_id.clone())
            .collect::<Vec<_>>();
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
            let attempt_count: i64 = row.attempt_count;
            let condition = db::task_condition::decode_or_unknown(&row.condition_json.clone());
            let task_id: String = row.task_id.clone();
            let execution_retry_count = row.execution_spent.max(0) as u32;
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
                title: row.title.clone(),
                attempt_count,
                current_state: row.status.clone(),
                retry_reason,
                due_time,
                last_error: row.last_error.clone(),
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
        let rows = self
            .db
            .recent_failed_execution_operator_rows(&(cutoff.to_rfc3339()))
            .await?;

        rows.into_iter()
            .map(|row| {
                let error: Option<String> = row.error.clone();
                Ok(RecentErrorSummary {
                    entity_type: "task".to_owned(),
                    entity_id: row.task_id.clone(),
                    error: error.unwrap_or_else(|| "execution failed".to_owned()),
                    occurred_at: row.occurred_at.clone(),
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

/// The window and the bound of the merge timings in the operator status.
const INTEGRATION_TIMING_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const INTEGRATION_TIMING_SAMPLE_LIMIT: u32 = 500;

/// Nearest-rank percentiles of the values that are present.
fn integration_percentiles(
    values: impl Iterator<Item = Option<i64>>,
) -> api_types::IntegrationPercentiles {
    let mut values = values.flatten().collect::<Vec<_>>();
    values.sort_unstable();
    let rank = |percent: usize| {
        (!values.is_empty()).then(|| values[(values.len() * percent).div_ceil(100).max(1) - 1])
    };
    api_types::IntegrationPercentiles {
        samples: values.len() as i64,
        p50_ms: rank(50),
        p95_ms: rank(95),
        max_ms: values.last().copied(),
    }
}

/// Where the sampled merges spent their time: the wait in line, the time at
/// the head, and the head's split into rebase, check and fast-forward. A
/// merge whose target had not moved has no rebase and no check; it counts in
/// the wait and the head time only.
fn integration_timing_status(
    samples: &[db::IntegrationPhaseTimings],
) -> api_types::IntegrationTimingStatus {
    api_types::IntegrationTimingStatus {
        window_seconds: INTEGRATION_TIMING_WINDOW_SECONDS,
        completed: samples.len() as i64,
        sample_limit: i64::from(INTEGRATION_TIMING_SAMPLE_LIMIT),
        queue_wait: integration_percentiles(samples.iter().map(|sample| sample.queued_ms)),
        head_time: integration_percentiles(samples.iter().map(|sample| sample.head_total_ms)),
        rebase: integration_percentiles(samples.iter().map(|sample| sample.rebase_ms)),
        check: integration_percentiles(samples.iter().map(|sample| match &sample.check {
            Some(db::IntegrationCheckTiming::Ran { run_ms, .. }) => Some(*run_ms),
            _ => None,
        })),
        fast_forward: integration_percentiles(samples.iter().map(|sample| sample.ff_ms)),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod condition_check_tests;
