use std::sync::Arc;

use db::SqliteDb;
use events::{EventBus, EventContext, ForgeEvent};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::{NotificationService, Result, TaskService};

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

    /// Start project-hook evaluation with compatibility lifecycle semantics.
    ///
    /// New runtime assembly should use [`Self::start_with_shutdown`] so all
    /// event-triggered evaluations are owned by the returned task.
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        self.start_inner(None)
    }

    /// Start project-hook evaluation and stop/settle all child evaluations
    /// when shutdown is requested.
    pub fn start_with_shutdown(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        self.start_inner(Some(shutdown))
    }

    fn start_inner(self: Arc<Self>, shutdown: Option<watch::Receiver<bool>>) -> JoinHandle<()> {
        tokio::spawn(async move {
            if shutdown.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                return;
            }

            let mut receiver = self.event_bus.subscribe();
            let shutdown = wait_for_shutdown(shutdown);
            tokio::pin!(shutdown);
            let mut evaluations = JoinSet::new();

            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    Some(result) = evaluations.join_next(), if !evaluations.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "project hook evaluation task failed");
                        }
                    }
                    result = receiver.recv() => {
                        let Ok(event) = result else {
                            break;
                        };
                        let Some((project_id, cause)) = evaluation_cause_from_event(&event) else {
                            continue;
                        };
                        let service = Arc::clone(&self);
                        evaluations.spawn(async move {
                            if let Err(error) = service.evaluate_for_project(project_id, cause).await {
                                tracing::warn!(%error, "project hook evaluation failed");
                            }
                        });
                    }
                }
            }

            // A shutdown can race with a child evaluation that is blocked on
            // storage or an action. Do not drop the JoinSet with work still
            // in flight: abort and join every child before the worker exits.
            evaluations.abort_all();
            while let Some(result) = evaluations.join_next().await {
                if let Err(error) = result {
                    tracing::debug!(%error, "project hook evaluation task stopped");
                }
            }
        })
    }

    pub async fn evaluate_for_project(
        &self,
        project_id: impl Into<String>,
        cause: EvaluationCause,
    ) -> Result<()> {
        evaluator::evaluate_for_project(self, project_id.into(), cause).await
    }
}

async fn wait_for_shutdown(mut shutdown: Option<watch::Receiver<bool>>) {
    let Some(mut shutdown) = shutdown.take() else {
        std::future::pending::<()>().await;
        return;
    };

    if *shutdown.borrow_and_update() {
        return;
    }

    loop {
        match shutdown.changed().await {
            Ok(()) if *shutdown.borrow() => return,
            Ok(()) => {}
            Err(_) => return,
        }
    }
}

fn evaluation_cause_from_event(event: &ForgeEvent) -> Option<(String, EvaluationCause)> {
    match &event.context {
        EventContext::TaskCreated { project_id, .. } => Some((
            project_id.clone(),
            EvaluationCause::TaskCreated {
                task_id: event.entity_id.clone(),
            },
        )),
        EventContext::TaskStatusChanged { project_id, .. } => Some((
            project_id.clone(),
            EvaluationCause::TaskTransitioned {
                task_id: event.entity_id.clone(),
            },
        )),
        EventContext::TaskMoved(payload) if payload.old_status != payload.new_status => Some((
            payload.project_id.clone(),
            EvaluationCause::TaskTransitioned {
                task_id: event.entity_id.clone(),
            },
        )),
        EventContext::TaskUpdated { project_id } if event.event_type == "task.archived" => Some((
            project_id.clone(),
            EvaluationCause::TaskArchived {
                task_id: event.entity_id.clone(),
            },
        )),
        _ => None,
    }
}
