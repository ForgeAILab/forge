use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use db::{PageRequest, Project, ProjectRepo, SortBy, SortOrder, Task, TaskListQuery, TaskRepo};
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
            if self.sync_repository_pause(&project).await? {
                continue;
            }
            if project.paused_at.is_some() {
                continue;
            }
            let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
            dispatched += self.dispatch_initial_tasks(&project, &workflow).await?;
            if self.is_stopped() {
                break;
            }
            dispatched += self.recover_active_tasks(&project, &workflow).await?;
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
