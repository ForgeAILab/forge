use std::{path::Path, path::PathBuf, sync::Arc};

use api_types::{LifecycleHookDef, LifecycleHooks, ProjectSettings, StateKind, WorkflowDefinition};
use db::{
    Execution, ExecutionRepo, ProjectRepo, RepoRepo, Task, TaskRepo, TransitionLogRepo, Workspace,
    WorkspaceRepo,
};
use events::{EventContext, ForgeEvent};
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use crate::{
    lifecycle::{LifecycleHookContext, LifecycleHookRunner, PluginRegistry},
    workflow::engine::WorkflowEngine,
};

#[derive(Clone)]
pub struct LifecycleEventEmitter {
    db: Arc<db::SqliteDb>,
    plugin_registry: Arc<PluginRegistry>,
}

impl LifecycleEventEmitter {
    pub fn new(db: Arc<db::SqliteDb>, plugin_registry: Arc<PluginRegistry>) -> Self {
        Self {
            db,
            plugin_registry,
        }
    }

    /// Run the emitter until the event bus closes.
    ///
    /// This is retained for callers that own the receiver directly. Runtime
    /// assembly should prefer [`Self::run_with_shutdown`] so the receiver
    /// loop has an explicit lifecycle boundary.
    pub async fn run(&self, rx: broadcast::Receiver<ForgeEvent>) {
        self.run_until_shutdown(rx, None).await;
    }

    /// Run the emitter until the event bus closes or shutdown is requested.
    ///
    /// The receiver is checked before subscribing to the loop, so a worker
    /// started after shutdown exits without consuming any more events.
    pub async fn run_with_shutdown(
        &self,
        rx: broadcast::Receiver<ForgeEvent>,
        shutdown: watch::Receiver<bool>,
    ) {
        self.run_until_shutdown(rx, Some(shutdown)).await;
    }

    async fn run_until_shutdown(
        &self,
        mut rx: broadcast::Receiver<ForgeEvent>,
        shutdown: Option<watch::Receiver<bool>>,
    ) {
        if shutdown.as_ref().is_some_and(|receiver| *receiver.borrow()) {
            return;
        }

        let shutdown = wait_for_shutdown(shutdown);
        tokio::pin!(shutdown);
        info!("lifecycle event emitter started");

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("lifecycle event emitter stopped");
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Ok(event) => {
                            if let Err(error) = self.handle_event(event).await {
                                warn!(%error, "lifecycle event emitter failed");
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "lifecycle event emitter lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("lifecycle event emitter stopped");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_event(&self, event: ForgeEvent) -> Result<(), String> {
        let ForgeEvent {
            event_type,
            entity_id,
            context,
            ..
        } = event;

        match context {
            EventContext::TaskStatusChanged {
                project_id,
                old_status,
                new_status,
            } if event_type == "task.status_changed" => {
                self.handle_status_changed(&entity_id, &project_id, &old_status, &new_status)
                    .await
            }
            EventContext::TaskMoved(payload)
                if event_type == events::TASK_MOVED_EVENT
                    && payload.old_status != payload.new_status =>
            {
                self.handle_status_changed(
                    &entity_id,
                    &payload.project_id,
                    &payload.old_status,
                    &payload.new_status,
                )
                .await
            }
            EventContext::TaskAssigned {
                project_id,
                agent_id,
                execution_id,
            } if event_type == "task.execution_launched" => {
                self.emit_lifecycle_event(
                    &entity_id,
                    Some(&project_id),
                    api_types::LifecycleEvent::OnWorkStart,
                    None,
                    Some(agent_id),
                    Some(execution_id),
                )
                .await
            }
            EventContext::ExecutionStarted { task_id, agent_id } => {
                self.emit_lifecycle_event(
                    &task_id,
                    None,
                    api_types::LifecycleEvent::OnWorkStart,
                    None,
                    agent_id,
                    Some(entity_id),
                )
                .await
            }
            _ => Ok(()),
        }
    }

    async fn handle_status_changed(
        &self,
        task_id: &str,
        project_id: &str,
        old_status: &str,
        new_status: &str,
    ) -> Result<(), String> {
        let task = self.load_task(task_id).await?;
        let project = self.load_project(project_id).await?;
        let workflow = resolve_workflow(&project.workflow_definition);
        let old_kind = workflow.state_kind(old_status);
        let new_kind = workflow.state_kind(new_status);
        let cancellation_state = workflow.cancellation_state.as_deref();

        if matches!(new_kind, Some(StateKind::Active))
            && !matches!(old_kind, Some(StateKind::Active))
        {
            self.emit_lifecycle_event(
                &task.id,
                Some(project_id),
                api_types::LifecycleEvent::BeforeWork,
                Some(old_status.to_owned()),
                task.assignee_id.clone(),
                None,
            )
            .await?;
        }

        if matches!(new_kind, Some(StateKind::Terminal))
            && cancellation_state.is_some_and(|state| state == new_status)
        {
            self.emit_lifecycle_event(
                &task.id,
                Some(project_id),
                api_types::LifecycleEvent::OnTaskCancel,
                Some(old_status.to_owned()),
                task.assignee_id.clone(),
                None,
            )
            .await?;
            return Ok(());
        }

        if matches!(new_kind, Some(StateKind::Terminal)) {
            self.emit_lifecycle_event(
                &task.id,
                Some(project_id),
                api_types::LifecycleEvent::OnTaskDone,
                Some(old_status.to_owned()),
                task.assignee_id.clone(),
                None,
            )
            .await?;
        }

        if matches!(old_kind, Some(StateKind::Active))
            && !matches!(new_kind, Some(StateKind::Active | StateKind::Terminal))
        {
            self.emit_lifecycle_event(
                &task.id,
                Some(project_id),
                api_types::LifecycleEvent::OnWorkStop,
                Some(old_status.to_owned()),
                task.assignee_id.clone(),
                None,
            )
            .await?;
        }

        Ok(())
    }

    async fn emit_lifecycle_event(
        &self,
        task_id: &str,
        project_id_hint: Option<&str>,
        event: api_types::LifecycleEvent,
        previous_status: Option<String>,
        agent_id: Option<String>,
        execution_id: Option<String>,
    ) -> Result<(), String> {
        let task = self.load_task(task_id).await?;
        let project = self
            .load_project(project_id_hint.unwrap_or(&task.project_id))
            .await?;
        let settings = parse_project_settings(&project.settings)?;
        let hooks = lifecycle_hooks_for(&settings.lifecycle_hooks, event);

        if hooks.is_empty() {
            return Ok(());
        }

        let before_execution =
            event == api_types::LifecycleEvent::BeforeWork && execution_id.is_none();
        let execution = if before_execution {
            None
        } else {
            self.resolve_execution(task_id, execution_id.as_deref())
                .await
                .map_err(|error| {
                    format!("failed to resolve execution for task {task_id}: {error}")
                })?
        };
        let resolved_execution_id =
            execution_id.or_else(|| execution.as_ref().map(|item| item.id.clone()));
        let resolved_agent_id = agent_id
            .or_else(|| execution.as_ref().and_then(|item| item.agent_id.clone()))
            .or_else(|| task.assignee_id.clone());
        let previous_status = match previous_status {
            Some(previous_status) => previous_status,
            None => self
                .latest_previous_status(task_id)
                .await
                .unwrap_or_else(|| task.status.clone()),
        };
        let workspace = if before_execution {
            None
        } else {
            self.resolve_workspace(&task, execution.as_ref()).await
        };
        let worktree_path = workspace
            .as_ref()
            .filter(|workspace| Path::new(&workspace.worktree_path).exists())
            .map(|workspace| workspace.worktree_path.clone());
        let repo_path = self
            .resolve_repo_path(
                &project,
                execution.as_ref(),
                workspace.as_ref(),
                worktree_path.as_deref(),
            )
            .await;
        let log_dir = resolve_log_dir(execution.as_ref(), resolved_execution_id.as_deref());

        let ctx = LifecycleHookContext {
            event,
            task_id: task.id.clone(),
            task_title: task.title.clone(),
            task_status: task.status.clone(),
            previous_status,
            project_id: project.id.clone(),
            project_name: project.name.clone(),
            repo_path,
            worktree_path,
            agent_id: resolved_agent_id,
            execution_id: resolved_execution_id,
            log_dir,
        };

        LifecycleHookRunner::run_hooks(ctx, &hooks, Arc::clone(&self.plugin_registry)).await;
        Ok(())
    }

    async fn load_task(&self, task_id: &str) -> Result<Task, String> {
        TaskRepo::get_by_id(&*self.db, task_id, false)
            .await
            .map_err(|error| format!("failed to load task {task_id}: {error}"))?
            .ok_or_else(|| format!("task {task_id} not found"))
    }

    async fn load_project(&self, project_id: &str) -> Result<db::Project, String> {
        ProjectRepo::get_by_id(&*self.db, project_id)
            .await
            .map_err(|error| format!("failed to load project {project_id}: {error}"))?
            .ok_or_else(|| format!("project {project_id} not found"))
    }

    async fn latest_previous_status(&self, task_id: &str) -> Option<String> {
        let transition_logs = TransitionLogRepo::list_by_task(&*self.db, task_id)
            .await
            .ok()?;
        transition_logs.last().map(|entry| entry.from_state.clone())
    }

    async fn resolve_execution(
        &self,
        task_id: &str,
        execution_id: Option<&str>,
    ) -> Result<Option<Execution>, db::DbError> {
        if let Some(execution_id) = execution_id {
            if let Some(execution) = ExecutionRepo::get_by_id(&*self.db, execution_id).await? {
                return Ok(Some(execution));
            }
        }

        let fallback = ExecutionRepo::list_latest_executions_for_tasks(&*self.db, &[task_id])
            .await?
            .into_iter()
            .next();
        let executor =
            ExecutionRepo::latest_execution_by_task_and_roles(&*self.db, task_id, &["executor"])
                .await?;
        Ok(executor.or(fallback))
    }

    async fn resolve_workspace(
        &self,
        task: &Task,
        execution: Option<&Execution>,
    ) -> Option<Workspace> {
        if let Some(execution) = execution {
            let workspace_id = execution.workspace_id.as_deref()?;
            return WorkspaceRepo::get_by_id(&*self.db, workspace_id)
                .await
                .ok()?;
        }

        WorkspaceRepo::get_by_task_id(&*self.db, &task.id)
            .await
            .ok()?
    }

    async fn resolve_repo_path(
        &self,
        project: &db::Project,
        execution: Option<&Execution>,
        workspace: Option<&Workspace>,
        worktree_path: Option<&str>,
    ) -> String {
        let repo_id = match (execution, workspace) {
            (_, Some(workspace)) => Some(workspace.repo_id.as_str()),
            (Some(_), None) => None,
            (None, None) => project.primary_repo_id.as_deref(),
        };
        let repo_path = match repo_id {
            Some(id) => RepoRepo::get_by_id(&*self.db, id)
                .await
                .ok()
                .flatten()
                .filter(|repo| repo.project_id == project.id)
                .and_then(|repo| repo.local_path),
            None => None,
        };

        match repo_path {
            Some(repo_path) => repo_path,
            None => worktree_path.unwrap_or_default().to_owned(),
        }
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

fn lifecycle_hooks_for(
    hooks: &LifecycleHooks,
    event: api_types::LifecycleEvent,
) -> Vec<LifecycleHookDef> {
    let hooks = hooks.get(&event).cloned().unwrap_or_default();
    if event != api_types::LifecycleEvent::BeforeWork {
        return hooks;
    }

    hooks
        .into_iter()
        .filter(|hook| !matches!(hook, LifecycleHookDef::Script { blocking: true, .. }))
        .collect()
}

fn parse_project_settings(settings: &str) -> Result<ProjectSettings, String> {
    serde_json::from_str::<ProjectSettings>(settings)
        .map_err(|error| format!("invalid project settings: {error}"))
}

fn resolve_workflow(workflow_definition: &str) -> WorkflowDefinition {
    WorkflowEngine::resolve_workflow(workflow_definition)
}

fn resolve_log_dir(execution: Option<&Execution>, execution_id: Option<&str>) -> Option<PathBuf> {
    if let Some(logs_path) = execution.and_then(|execution| execution.logs_path.as_deref()) {
        return Path::new(logs_path).parent().map(Path::to_path_buf);
    }

    execution_id
        .map(|execution_id| {
            std::env::temp_dir()
                .join("forge")
                .join("logs")
                .join(execution_id)
                .with_extension("jsonl")
        })
        .and_then(|path| path.parent().map(Path::to_path_buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, SqliteDb};
    use events::EventBus;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn run_with_shutdown_stops_the_receiver_loop() {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        let emitter = LifecycleEventEmitter::new(
            Arc::new(SqliteDb::new(pool)),
            Arc::new(PluginRegistry::new()),
        );
        let event_bus = Arc::new(EventBus::new(16));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            emitter
                .run_with_shutdown(event_bus.subscribe(), shutdown_rx)
                .await;
        });

        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("shutdown receiver is alive");

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("emitter stops promptly")
            .expect("emitter task joins");
    }
}
