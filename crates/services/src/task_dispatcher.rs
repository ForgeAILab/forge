use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use db::{
    ExecutionRepo, PageRequest, Project, ProjectRepo, SortBy, SortOrder, Task, TaskListQuery,
    TaskRepo,
};
use events::EventBus;
use tokio::sync::Notify;
use tracing::Instrument;

use crate::{workflow::engine::WorkflowEngine, Result, TaskService};

mod active_recovery;
mod helpers;
mod initial_scheduling;
mod repo_pause_sync;
mod workspace_blocking;

pub struct TaskDispatcher {
    db: Arc<db::SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Arc<TaskService>,
    check_interval: Duration,
    stopped: AtomicBool,
    stop_notify: Notify,
    /// Primary Repo snapshots (`id@updated_at`) already verified ready by
    /// `sync_repository_pause`.
    ready_repositories: Mutex<HashSet<String>>,
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
        Self {
            db,
            event_bus,
            task_service,
            check_interval,
            stopped: AtomicBool::new(false),
            stop_notify: Notify::new(),
            ready_repositories: Mutex::new(HashSet::new()),
        }
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let event_bus_strong_count = Arc::strong_count(&self.event_bus);
        tokio::spawn(
            async move {
                tracing::info!(
                    check_interval_seconds = self.check_interval.as_secs(),
                    "task dispatcher started"
                );
                while !self.is_stopped() {
                    if let Err(error) = self.check_once().await {
                        tracing::warn!(%error, "task dispatcher check failed");
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(self.check_interval) => {}
                        _ = self.stop_notify.notified() => {}
                    }
                }
                tracing::info!("task dispatcher stopped");
            }
            .instrument(tracing::info_span!(
                "task.dispatcher",
                event_bus_strong_count = event_bus_strong_count
            )),
        )
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.stop_notify.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    #[tracing::instrument(skip(self))]
    pub async fn check_once(&self) -> Result<u64> {
        let mut dispatched = 0;
        for project in self.list_projects().await? {
            if self.is_stopped() {
                break;
            }
            // Reconciles pause state before the paused-project skip below,
            // since it is also what resumes a Project once its repository
            // shows up. Either direction leaves this scan's in-memory
            // `project` stale, so skip acting on it this tick either way.
            let pause_changed = self.sync_repository_pause(&project).await?;
            dispatched += self.reconcile_plan_publication_claims(&project).await?;
            if pause_changed {
                continue;
            }
            if project.paused_at.is_some() {
                continue;
            }
            let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
            dispatched += self.dispatch_queued_recoveries(&project).await?;
            // Work already in flight goes first. Scheduling new Tasks first
            // let every fresh `todo` claim a single-slot agent before an
            // interrupted Task — one whose execution a restart stopped with
            // an automatic resume — was even considered, so it waited behind
            // the whole ready queue while holding its worktree.
            dispatched += self.recover_active_tasks(&project, &workflow).await?;
            if self.is_stopped() {
                break;
            }
            dispatched += self.dispatch_initial_tasks(&project, &workflow).await?;
        }

        tracing::info!(
            dispatched_tasks = dispatched,
            "task dispatcher check completed"
        );
        Ok(dispatched)
    }

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
            if let Some(execution_id) =
                crate::task_service::execution::pending_plan_publication_cleanup_owner(&task)?
            {
                match crate::task_service::execution::cleanup_execution_plan_private_files(
                    &self.db,
                    &task,
                    &execution_id,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication private-file cleanup failed");
                        continue;
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
                    Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => continue,
                    Err(error) => {
                        tracing::warn!(task_id = %task.id, execution_id, %error, "plan publication cleanup marker release failed");
                        continue;
                    }
                }
            }
            task = match crate::task_service::execution::clear_stale_plan_publication_claim(
                &self.db, &task,
            )
            .await
            {
                Ok(task) => task,
                Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => continue,
                Err(error) => {
                    tracing::warn!(task_id = %task.id, %error, "plan publication claim cleanup failed");
                    continue;
                }
            };
            let Some(execution_id) =
                crate::task_service::execution::active_plan_publication_claim_owner(&task)?
            else {
                continue;
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
                continue;
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
        }
        Ok(reconciled)
    }

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
