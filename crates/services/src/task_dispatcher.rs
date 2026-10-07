use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use db::{ExecutionRepo, Task};
#[cfg(test)]
use db::{PageRequest, Project, ProjectRepo, SortBy, SortOrder, TaskListQuery, TaskRepo};
use events::EventBus;
use tokio::sync::Notify;
use tracing::Instrument;

#[cfg(test)]
use crate::workflow::engine::WorkflowEngine;
use crate::{Result, TaskService};

mod active_recovery;
mod environment_pause_sync;
mod helpers;
mod next_step;
mod reconciliation;
mod snapshot;
pub(crate) use helpers::is_blocking_annotation_type;
mod initial_scheduling;
mod legacy_park;
mod repo_pause_sync;
pub mod slots;
#[cfg(test)]
mod stranded_hooks;
mod workspace_blocking;

/// A handle to the one dispatcher instance of a runtime. The instance owns
/// the stop fence and the reconciliation state; a queued role command reaches
/// the same instance through the Task service instead of building its own.
pub struct TaskDispatcher {
    inner: Arc<DispatcherInstance>,
}

impl std::ops::Deref for TaskDispatcher {
    type Target = DispatcherInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct DispatcherInstance {
    db: Arc<db::SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Arc<TaskService>,
    check_interval: Duration,
    stopped: AtomicBool,
    stop_notify: Arc<Notify>,
    /// Primary Repo snapshots (`id@updated_at`) already verified ready by
    /// `sync_repository_pause`.
    ready_repositories: Mutex<HashSet<String>>,
    environment_rechecks: Mutex<HashMap<String, tokio::task::JoinHandle<Result<bool>>>>,
    environment_settings_observer: std::sync::OnceLock<tokio::task::JoinHandle<()>>,
    periodic_workers: Arc<crate::worker_runtime::PeriodicWorkers>,
    schedule_state: Mutex<reconciliation::ScheduleState>,
    reconcile_lock: tokio::sync::Mutex<()>,
    /// One slice of the sweep at a time.
    sweep_lock: tokio::sync::Mutex<()>,
    observer_shutdown: crate::runtime::ShutdownSignal,
    #[cfg(test)]
    fixture_worker: std::sync::OnceLock<tests::FixtureWorker>,
}

impl TaskDispatcher {
    const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);

    pub fn new(
        db: Arc<db::SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
    ) -> Self {
        Self::with_check_interval(db, event_bus, task_service, Self::DEFAULT_CHECK_INTERVAL)
    }

    pub fn with_check_interval(
        db: Arc<db::SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
        check_interval: Duration,
    ) -> Self {
        let inner = Arc::new(DispatcherInstance {
            periodic_workers: Arc::new(crate::worker_runtime::PeriodicWorkers::new(Arc::clone(
                &db,
            ))),
            db,
            event_bus,
            task_service: Arc::clone(&task_service),
            check_interval,
            stopped: AtomicBool::new(false),
            stop_notify: task_service.dispatch_notify(),
            ready_repositories: Mutex::new(HashSet::new()),
            environment_rechecks: Mutex::new(HashMap::new()),
            environment_settings_observer: std::sync::OnceLock::new(),
            schedule_state: Mutex::default(),
            reconcile_lock: tokio::sync::Mutex::default(),
            sweep_lock: tokio::sync::Mutex::default(),
            observer_shutdown: crate::runtime::ShutdownSignal::new(),
            #[cfg(test)]
            fixture_worker: std::sync::OnceLock::new(),
        });
        Self { inner }
    }

    #[cfg(test)]
    fn instance_mut(&mut self) -> &mut DispatcherInstance {
        Arc::get_mut(&mut self.inner).expect("configured before the dispatcher runs")
    }

    /// Make this instance the one queued role commands run on. Idempotent;
    /// called by every pass, so a command always finds the instance that
    /// enqueued it.
    fn register(&self) {
        self.task_service
            .register_dispatcher(Arc::downgrade(&self.inner));
    }

    /// The instance a queued role command was enqueued by, while it lives.
    pub(crate) fn registered(task_service: &TaskService) -> Option<Self> {
        task_service
            .registered_dispatcher()
            .map(|inner| Self { inner })
    }

    pub fn with_periodic_workers(
        mut self,
        workers: Arc<crate::worker_runtime::PeriodicWorkers>,
    ) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("periodic workers are configured before the dispatcher runs")
            .periodic_workers = workers;
        self
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.register();
        self.start_with_check(Duration::from_secs(3600), |dispatcher| async move {
            dispatcher.tick(false).await
        })
    }

    fn start_with_check<F, Fut>(
        self: Arc<Self>,
        timeout: Duration,
        check: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(Arc<Self>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<u64>> + Send + 'static,
    {
        let check = Arc::new(check);
        let event_bus_strong_count = Arc::strong_count(&self.event_bus);
        let stop = Arc::clone(&self);
        self.periodic_workers
            .worker("task-dispatcher")
            .with_stall_budget(timeout)
            .start_stoppable(
                move || stop.is_stopped(),
                move |worker| {
                    let dispatcher = Arc::clone(&self);
                    let check = Arc::clone(&check);
                    async move {
                        tracing::info!(
                            check_interval_seconds = dispatcher.check_interval.as_secs(),
                            "task dispatcher started"
                        );
                        let result = worker
                            .run(
                                || dispatcher.is_stopped(),
                                "task dispatcher check failed",
                                || check(Arc::clone(&dispatcher)),
                                || async {
                                    let changes = dispatcher.db.domain_event_notify();
                                    tokio::select! {
                                        _ = tokio::time::sleep(dispatcher.schedule_sleep()) => {}
                                        _ = dispatcher.stop_notify.notified() => {}
                                        _ = dispatcher.task_service.dispatch_wake.notified() => {}
                                        _ = changes.notified() => {}
                                    }
                                },
                            )
                            .await;
                        tracing::info!("task dispatcher stopped");
                        result
                    }
                    .instrument(tracing::info_span!(
                        "task.dispatcher",
                        event_bus_strong_count
                    ))
                },
            )
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.observer_shutdown.request();
        self.stop_notify.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    /// Reconcile now, as one scan did: every Task a commit, a deadline or
    /// the sweep marked, every Task held on a fact no commit announces, and
    /// on an instance's first direct request every Task that is not settled.
    /// Then advance the paged sweep by one bounded slice. The runtime does
    /// not start this way: see `startup_reconcile`.
    #[tracing::instrument(skip(self))]
    pub async fn check_once(&self) -> Result<u64> {
        self.tick(true).await
    }

    /// The supervised loop's tick. It re-reads the Tasks held on unannounced
    /// facts at the scan interval; a direct `check_once` re-reads them now.
    async fn tick(&self, asked: bool) -> Result<u64> {
        let dispatched = self.reconcile_once(asked).await?;
        if let Err(error) = self.sweep_slice(reconciliation::SWEEP_SLICE).await {
            tracing::warn!(%error, "Task scheduler sweep deferred");
        }
        Ok(dispatched)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    async fn legacy_check_once(&self) -> Result<u64> {
        let mut dispatched = 0;
        let environment_changed = match self.sync_due_environment_checks().await {
            Ok(changed) => changed,
            Err(error) => {
                tracing::warn!(%error,"environment readiness scan failed; continuing Project dispatch");
                HashSet::new()
            }
        };
        self.observe_environment_settings();
        for project in self.list_projects().await? {
            if self.is_stopped() {
                break;
            }
            // Reconciles pause state before the paused-project skip below,
            // since it is also what resumes a Project once its repository
            // shows up. Either direction leaves this scan's in-memory
            // `project` stale, so skip acting on it this tick either way.
            let pause_changed = match self.sync_repository_pause(&project).await {
                Ok(changed) => changed,
                Err(error) => {
                    tracing::warn!(project_id = %project.id, %error, "repository pause synchronization failed; skipping Project");
                    continue;
                }
            };
            let pause_changed = pause_changed || environment_changed.contains(&project.id);
            match self.reconcile_plan_publication_claims(&project).await {
                Ok(count) => dispatched += count,
                Err(error) => {
                    tracing::warn!(project_id = %project.id, %error, "plan publication reconciliation failed; continuing Project scan")
                }
            }
            if pause_changed {
                continue;
            }
            if project.paused_at.is_some() {
                continue;
            }
            let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
            match self.dispatch_queued_recoveries(&project).await {
                Ok(count) => dispatched += count,
                Err(error) => {
                    tracing::warn!(project_id = %project.id, %error, "queued recovery failed; continuing Project scan")
                }
            }
            // Work already in flight goes first. Scheduling new Tasks first
            // let every fresh `todo` claim a single-slot agent before an
            // interrupted Task — one whose execution a restart stopped with
            // an automatic resume — was even considered, so it waited behind
            // the whole ready queue while holding its worktree.
            // Admission retains large reserve/prepare futures. Keep each
            // phase off the scan's inline future, including concurrent scans.
            match Box::pin(self.recover_active_tasks(&project, &workflow)).await {
                Ok(count) => dispatched += count,
                Err(error) => {
                    tracing::warn!(project_id = %project.id, %error, "active recovery failed; continuing Project scan")
                }
            }
            if self.is_stopped() {
                break;
            }
            match Box::pin(self.dispatch_initial_tasks(&project, &workflow)).await {
                Ok(count) => dispatched += count,
                Err(error) => {
                    tracing::warn!(project_id = %project.id, %error, "initial scheduling failed; skipping Project")
                }
            }
        }

        // After the dispatch pass, never before it: the check reuses this
        // supervised loop and must not delay a dispatch. It reads off the
        // writer, takes it only for a row it repairs, and after a mapping
        // change re-runs the backfill here in slices instead of at startup.
        if let Err(error) = self.db.backfill_task_conditions_if_stale().await {
            tracing::warn!(%error, "Task condition invariant check deferred");
        }
        tracing::info!(
            dispatched_tasks = dispatched,
            "task dispatcher check completed"
        );
        Ok(dispatched)
    }

    #[cfg(test)]
    async fn list_projects(&self) -> Result<Vec<Project>> {
        let mut items = Vec::new();
        let mut cursor = None;
        loop {
            let page = ProjectRepo::list(
                &*self.db,
                PageRequest {
                    cursor,
                    limit: 100,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            )
            .await?;
            items.extend(page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(items)
    }

    #[cfg(test)]
    async fn dispatch_queued_recoveries(&self, project: &Project) -> Result<u64> {
        let tasks = TaskRepo::list_by_project_with_metadata_key(
            &*self.db,
            &project.id,
            crate::deferred_dispatch::QUEUED_RECOVERY_KEY,
        )
        .await?;
        let mut dispatched = 0;
        for task in tasks {
            if self.is_stopped() {
                break;
            }
            match self.task_service.dispatch_queued_recovery(&task).await {
                Ok(true) => dispatched += 1,
                Ok(false) => {}
                Err(crate::ServiceError::Db(db::DbError::VersionConflict))
                | Err(crate::ServiceError::Db(db::DbError::TaskVersionConflict { .. })) => {
                    tracing::debug!(task_id = %task.id, "queued recovery lost version race");
                }
                Err(error) => {
                    tracing::warn!(task_id = %task.id, %error, "queued recovery failed");
                }
            }
        }
        Ok(dispatched)
    }

    /// Reconcile host-owned plan publication claims before workflow-state and
    /// Project-pause filtering. A workflow edit can remove/reclassify the
    /// claimed state, and a governance edit can pause the Project; neither may
    /// strand the canonical plan rollback or block every manual Task action.
    #[cfg(test)]
    async fn reconcile_plan_publication_claims(&self, project: &Project) -> Result<u64> {
        let mut tasks = TaskRepo::list_by_project_with_metadata_key(
            &*self.db,
            &project.id,
            "plan_publication_claim",
        )
        .await?;
        for task in TaskRepo::list_by_project_with_metadata_key(
            &*self.db,
            &project.id,
            "plan_publication_cleanup",
        )
        .await?
        {
            if !tasks.iter().any(|candidate| candidate.id == task.id) {
                tasks.push(task);
            }
        }
        let mut reconciled = 0;
        for mut task in tasks {
            let task_id = task.id.clone();
            let result: Result<()> = async {
                if let Some(execution_id) =
                    crate::task_service::execution::pending_plan_publication_cleanup_owner(&task)?
                {
                    match crate::task_service::execution::cleanup_execution_plan_private_files(
                        &self.db,
                        &self.task_service.workspace_backend_router(),
                        &task,
                        &execution_id,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication private-file cleanup failed");
                            return Ok(());
                        }
                    }
                    match crate::task_service::execution::clear_plan_publication_cleanup(
                        &self.db,
                        &task,
                        &execution_id,
                    )
                    .await
                    {
                        Ok(updated) => {
                            task = updated;
                            reconciled += 1;
                        }
                        Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => return Ok(()),
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication cleanup marker release failed");
                            return Ok(());
                        }
                    }
                }
                task = match crate::task_service::execution::clear_stale_plan_publication_claim(
                    &self.db, &self.task_service.workspace_backend_router(), &task,
                )
                .await
                {
                    Ok(task) => task,
                    Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => return Ok(()),
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, %error, "plan publication claim cleanup failed");
                        return Ok(());
                    }
                };
                let Some(execution_id) =
                    crate::task_service::execution::active_plan_publication_claim_owner(&task)?
                else {
                    return Ok(());
                };
                let owner = ExecutionRepo::get_by_id(&*self.db, &execution_id).await?;
                if owner.as_ref().is_none_or(|execution| {
                    execution.task_id != task.id || execution.status != db::ExecutionStatus::Completed
                }) {
                    match self
                        .task_service
                        .abandon_plan_publication_claim(&task, &execution_id)
                        .await
                    {
                        Ok(()) => reconciled += 1,
                        Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => {}
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "invalid plan publication claim cleanup failed");
                        }
                    }
                    return Ok(());
                }
                match self
                    .task_service
                    .maybe_cascade_executor_completion(&execution_id)
                    .await
                {
                    Ok(()) => reconciled += 1,
                    Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => {}
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication claim reconciliation failed");
                    }
                }
                Ok(())
            }.await;
            if let Err(error) = result {
                tracing::warn!(%task_id, %error, "plan publication Task reconciliation failed; continuing scan");
            }
        }
        Ok(reconciled)
    }

    async fn reconcile_publication_task(&self, original: &Task) -> Result<u64> {
        let mut task = original.clone();
        let mut reconciled = 0;
        let task_id = task.id.clone();
        let result: Result<()> = async {
                if let Some(execution_id) =
                    crate::task_service::execution::pending_plan_publication_cleanup_owner(&task)?
                {
                    match crate::task_service::execution::cleanup_execution_plan_private_files(
                        &self.db,
                        &self.task_service.workspace_backend_router(),
                        &task,
                        &execution_id,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication private-file cleanup failed");
                            return Ok(());
                        }
                    }
                    match crate::task_service::execution::clear_plan_publication_cleanup(
                        &self.db,
                        &task,
                        &execution_id,
                    )
                    .await
                    {
                        Ok(updated) => {
                            task = updated;
                            reconciled += 1;
                        }
                        Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => return Ok(()),
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication cleanup marker release failed");
                            return Ok(());
                        }
                    }
                }
                task = match crate::task_service::execution::clear_stale_plan_publication_claim(
                    &self.db, &self.task_service.workspace_backend_router(), &task,
                )
                .await
                {
                    Ok(task) => task,
                    Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => return Ok(()),
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, %error, "plan publication claim cleanup failed");
                        return Ok(());
                    }
                };
                let Some(execution_id) =
                    crate::task_service::execution::active_plan_publication_claim_owner(&task)?
                else {
                    return Ok(());
                };
                let owner = ExecutionRepo::get_by_id(&*self.db, &execution_id).await?;
                if owner.as_ref().is_none_or(|execution| {
                    execution.task_id != task.id || execution.status != db::ExecutionStatus::Completed
                }) {
                    match self
                        .task_service
                        .abandon_plan_publication_claim(&task, &execution_id)
                        .await
                    {
                        Ok(()) => reconciled += 1,
                        Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => {}
                        Err(error) => {
                            tracing::warn!(task_id = %task.id, execution_id, %error, "invalid plan publication claim cleanup failed");
                        }
                    }
                    return Ok(());
                }
                match self
                    .task_service
                    .maybe_cascade_executor_completion(&execution_id)
                    .await
                {
                    Ok(()) => reconciled += 1,
                    Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => {}
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication claim reconciliation failed");
                    }
                }
                Ok(())
            }.await;
        if let Err(error) = result {
            tracing::warn!(%task_id, %error, "plan publication Task reconciliation failed; continuing scan");
        }
        Ok(reconciled)
    }

    #[cfg(test)]
    async fn list_tasks(&self, project_id: &str, statuses: Vec<String>) -> Result<Vec<Task>> {
        let mut items = Vec::new();
        let mut cursor = None;
        loop {
            let page = TaskRepo::list(
                &*self.db,
                TaskListQuery {
                    project_id: project_id.to_owned(),
                    q: None,
                    statuses: statuses.clone(),
                    agent_ids: Vec::new(),
                    assignee_types: Vec::new(),
                    assignee_ids: Vec::new(),
                    priority: None,
                    include_archived: false,
                    include_cancelled: false,
                    include_deleted: false,
                    page: PageRequest {
                        cursor,
                        limit: 200,
                        include_total: false,
                        sort_by: SortBy::CreatedAt,
                        sort_order: SortOrder::Asc,
                    },
                },
            )
            .await?;
            items.extend(page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(items)
    }
}

#[cfg(test)]
#[path = "task_dispatcher/tests.rs"]
mod tests;
