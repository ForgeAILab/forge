use std::sync::Arc;

use async_trait::async_trait;
use db::{DomainEvent, ProjectRepo, SqliteDb, TaskRepo};
use events::EventBus;
use sqlx::{Sqlite, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::worker_runtime::{
    consumer_error, Outcome, Subscription, Worker, WorkerError, WorkerRuntime,
};
use crate::{NotificationService, Result, TaskService};

pub(crate) const CONSUMER_NAME: &str = "project-hooks";

pub mod actions;
mod engine;
pub mod evaluator;
pub mod triggers;

#[cfg(test)]
mod tests;

pub use evaluator::EvaluationCause;

#[derive(Clone)]
pub struct ProjectHookService {
    pub(crate) db: Arc<SqliteDb>,
    pub(crate) event_bus: Arc<EventBus>,
    pub(crate) task_service: Arc<TaskService>,
    pub(crate) notification_service: Arc<NotificationService>,
}

impl ProjectHookService {
    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
        notification_service: Arc<NotificationService>,
    ) -> Self {
        Self {
            db,
            event_bus,
            task_service,
            notification_service,
        }
    }

    pub fn start_with_shutdown(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self)).start(shutdown)
    }

    pub async fn evaluate_for_project(
        &self,
        project_id: impl Into<String>,
        cause: EvaluationCause,
    ) -> Result<()> {
        evaluator::evaluate_for_project(self, project_id.into(), cause).await
    }
}

#[async_trait]
impl Worker<Vec<Option<engine::CommittedHook>>> for ProjectHookService {
    type Prepared = Vec<engine::PreparedHook>;
    fn name(&self) -> &str {
        CONSUMER_NAME
    }
    fn subscription(&self) -> Subscription {
        Subscription::Exact(vec![
            "task.transitioned".into(),
            "task.status_changed".into(),
            "project_hook.task_created".into(),
            "project_hook.task_archived".into(),
        ])
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
        if event.entity_type != "task" {
            return Ok(Outcome::Skip);
        }
        let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
            .map_err(|_| WorkerError::terminal("invalid project hook event payload"))?;
        let cause = match event.event_type.as_str() {
            "project_hook.task_created" => EvaluationCause::TaskCreated {
                task_id: event.entity_id.clone(),
            },
            "project_hook.task_archived" => EvaluationCause::TaskArchived {
                task_id: event.entity_id.clone(),
            },
            "task.transitioned" if payload["from_state"] != payload["to_state"] => {
                EvaluationCause::TaskTransitioned {
                    task_id: event.entity_id.clone(),
                }
            }
            "task.status_changed" if payload["from_status"] != payload["to_status"] => {
                EvaluationCause::TaskTransitioned {
                    task_id: event.entity_id.clone(),
                }
            }
            _ => return Ok(Outcome::Skip),
        };
        let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true)
            .await
            .map_err(|e| WorkerError::database("project hook task", e))?
        else {
            return Ok(Outcome::Skip);
        };
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await
            .map_err(|e| WorkerError::database("project hook project", e))?
        else {
            return Ok(Outcome::Skip);
        };
        let prepared = evaluator::prepare_for_project(self, &project, &cause)
            .await
            .map_err(consumer_error)?;
        if prepared.is_empty() {
            Ok(Outcome::Skip)
        } else {
            Ok(Outcome::Done(prepared))
        }
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<Vec<Option<engine::CommittedHook>>, WorkerError> {
        let engine = engine::ProjectHookEngine::new(self);
        let mut committed = Vec::with_capacity(prepared.len());
        for hook in prepared {
            committed.push(
                if engine
                    .still_matches(tx, hook)
                    .await
                    .map_err(consumer_error)?
                {
                    engine.commit(tx, hook).await.map_err(consumer_error)?
                } else {
                    None
                },
            );
        }
        Ok(committed)
    }
    async fn after_commit(
        &self,
        _: &DomainEvent,
        prepared: &Self::Prepared,
        committed: &Vec<Option<engine::CommittedHook>>,
    ) -> std::result::Result<(), WorkerError> {
        let engine = engine::ProjectHookEngine::new(self);
        let mut first_error = None;
        for (hook, result) in prepared.iter().zip(committed) {
            if let Some(result) = result {
                if let Err(error) = engine.after_commit(hook, result).await {
                    tracing::warn!(%error, "project hook post-commit action failed");
                    first_error.get_or_insert_with(|| consumer_error(error));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
