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
    workspace_backend::{ResolvedWorkspace, WorkspaceBackendRouter},
};

#[derive(Clone)]
pub struct LifecycleEventEmitter {
    db: Arc<db::SqliteDb>,
    plugin_registry: Arc<PluginRegistry>,
    workspace_backend_router: Arc<WorkspaceBackendRouter>,
}

impl LifecycleEventEmitter {
    pub fn new_with_router(
        db: Arc<db::SqliteDb>,
        plugin_registry: Arc<PluginRegistry>,
        workspace_backend_router: Arc<crate::workspace_backend::WorkspaceBackendRouter>,
    ) -> Self {
        Self {
            db,
            plugin_registry,
            workspace_backend_router,
        }
    }

    #[cfg(test)]
    pub fn new(db: Arc<db::SqliteDb>, plugin_registry: Arc<PluginRegistry>) -> Self {
        Self::new_for_test(db, plugin_registry)
    }

    /// Embedded-only fixture constructor.
    pub fn new_for_test(db: Arc<db::SqliteDb>, plugin_registry: Arc<PluginRegistry>) -> Self {
        let workspace_backend_router =
            crate::lifecycle::context::embedded_workspace_router_for_test(
                Arc::clone(&db),
                crate::task_service::workspace::default_workspace_root(),
                None,
            );
        Self {
            db,
            plugin_registry,
            workspace_backend_router,
        }
    }

    pub fn with_workspace_backend_router(mut self, router: Arc<WorkspaceBackendRouter>) -> Self {
        self.workspace_backend_router = router;
        self
    }

    pub fn start(
        self: Arc<Self>,
        bus: Arc<events::EventBus>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let initial = std::sync::Mutex::new(Some(bus.subscribe()));
        workers
            .worker("lifecycle-projection")
            .with_tick_timeout(std::time::Duration::from_secs(3600))
            .start(
                shutdown,
                || false,
                move |worker, shutdown| {
                    let emitter = Arc::clone(&self);
                    let rx = initial
                        .lock()
                        .expect("lifecycle receiver")
                        .take()
                        .unwrap_or_else(|| bus.subscribe());
                    async move {
                        emitter
                            .run_until_shutdown(rx, Some(shutdown), Some(worker))
                            .await;
                        Ok(())
                    }
                },
            )
    }

    /// Run the emitter until the event bus closes.
    ///
    /// This is retained for callers that own the receiver directly. Runtime
    /// assembly should prefer [`Self::run_with_shutdown`] so the receiver
    /// loop has an explicit lifecycle boundary.
    pub async fn run(&self, rx: broadcast::Receiver<ForgeEvent>) {
        self.run_until_shutdown(rx, None, None).await;
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
        self.run_until_shutdown(rx, Some(shutdown), None).await;
    }

    async fn run_until_shutdown(
        &self,
        mut rx: broadcast::Receiver<ForgeEvent>,
        shutdown: Option<watch::Receiver<bool>>,
        worker: Option<crate::worker_runtime::PeriodicWorker>,
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
                            if !handles_event(&event) { continue; }
                            if let Some(worker) = &worker {
                                if let Err(error) = worker.tick(async {
                                    self.handle_event(event).await.map_err(crate::ServiceError::invalid_operation)
                                }).await {
                                    warn!(worker = worker.name(), %error, "lifecycle event emitter failed");
                                }
                            } else if let Err(error) = self.handle_event(event).await {
                                warn!(worker = "lifecycle-projection", %error, "lifecycle event emitter failed");
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
        let workspace = self.resolve_workspace(&task, execution.as_ref()).await;
        let resolved = match workspace.as_ref() {
            Some(workspace) => Some(
                crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
                    &self.workspace_backend_router,
                    &self.db,
                    workspace,
                    &crate::task_service::workspace::default_workspace_root(),
                )
                .await
                .map_err(|error| error.to_string())?,
            ),
            None => None,
        };
        // Before preparation, server hooks retain their primary-checkout
        // context. An existing daemon placement remains the routing authority.
        let resolved = resolved.filter(|workspace| {
            !before_execution || workspace.placement.owner_kind != db::PlacementOwnerKind::Server
        });
        // A server workspace Forge has already reclaimed is, like one not yet
        // prepared, no workspace: its hooks keep the primary-checkout context.
        let reclaimed = workspace
            .as_ref()
            .is_some_and(|workspace| workspace.status == db::WorkspaceStatus::Cleaned);
        let resolved = resolved.filter(|workspace| {
            !reclaimed || workspace.placement.owner_kind != db::PlacementOwnerKind::Server
        });
        let (repo_path, worktree_path) = match resolved.as_ref() {
            Some(resolved) => {
                let (repo_path, handle) =
                    crate::lifecycle::context::workspace_context_paths(&self.db, resolved)
                        .await
                        .map_err(|error| error.to_string())?;
                let exists = workspace_exists(resolved).await?;
                if !exists && resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon {
                    return Err(format!(
                        "workspace {} does not exist on its owner",
                        resolved.placement.workspace_id
                    ));
                }
                if !exists {
                    // The Task has a worktree on record and it is not usable.
                    // Its hook must not run anywhere else, least of all in the
                    // user's own checkout.
                    return Err(crate::ServiceError::WorkspaceResetRequired {
                        task_id: task.id.clone(),
                        reason: format!(
                            "worktree of workspace {} is missing or is not a Git worktree; \
                             lifecycle hook {event:?} was not run",
                            resolved.placement.workspace_id
                        ),
                    }
                    .to_string());
                }
                (repo_path, Some(handle))
            }
            None => (
                self.resolve_repo_path(
                    &project,
                    execution.as_ref(),
                    workspace.as_ref().filter(|_| reclaimed),
                    None,
                )
                .await,
                None,
            ),
        };
        let log_dir = resolve_log_dir(execution.as_ref(), resolved_execution_id.as_deref());

        let ctx = LifecycleHookContext {
            env: crate::lifecycle::project_env(&project.settings),
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

        match resolved.as_ref().filter(|_| ctx.worktree_path.is_some()) {
            Some(resolved) => {
                LifecycleHookRunner::run_workspace_hooks(
                    ctx,
                    &hooks,
                    Arc::clone(&self.plugin_registry),
                    resolved,
                )
                .await
            }
            None => {
                LifecycleHookRunner::run_hooks(ctx, &hooks, Arc::clone(&self.plugin_registry)).await
            }
        }
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

        WorkspaceRepo::get_by_task_id(
            &*self.db,
            task.parent_task_id.as_deref().unwrap_or(&task.id),
        )
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

async fn workspace_exists(workspace: &ResolvedWorkspace) -> Result<bool, String> {
    if workspace.placement.owner_kind == db::PlacementOwnerKind::Server {
        // A directory without Git metadata is not a worktree: a hook run
        // there would act on a tree that is not the Task's.
        return workspace
            .embedded_path()
            .map(|path| path.join(".git").exists())
            .map_err(|error| error.to_string());
    }
    workspace
        .backend
        .describe(&workspace.placement)
        .await
        .map(|state| state.exists)
        .map_err(|error| error.to_string())
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

// Match the handler's existing context/type gates before allocating tick health.
fn handles_event(event: &ForgeEvent) -> bool {
    match &event.context {
        EventContext::TaskStatusChanged { .. } => event.event_type == "task.status_changed",
        EventContext::TaskMoved(payload) => {
            event.event_type == events::TASK_MOVED_EVENT && payload.old_status != payload.new_status
        }
        EventContext::TaskAssigned { .. } => event.event_type == "task.execution_launched",
        EventContext::ExecutionStarted { .. } => true,
        _ => false,
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
    async fn ignored_streaming_events_do_not_tick_or_touch_health() {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let workers = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
        let emitter = LifecycleEventEmitter::new_for_test(db, Arc::new(PluginRegistry::default()));
        let bus = EventBus::new(1024);
        let rx = bus.subscribe();
        for _ in 0..512 {
            bus.publish(ForgeEvent {
                event_type: "execution.log".to_owned(),
                entity_id: "execution".to_owned(),
                timestamp: events::event_timestamp(),
                context: EventContext::ReconciliationEvent {
                    task_id: None,
                    execution_id: None,
                    reason: "stream".to_owned(),
                },
            });
        }
        // A closed bus drains its queued events before Closed: the loop must
        // process all 512 hints without creating even its first health tick.
        drop(bus);
        timeout(
            Duration::from_secs(5),
            emitter.run_until_shutdown(rx, None, Some(workers.worker("lifecycle-projection"))),
        )
        .await
        .unwrap();
        assert!(workers.status().await.unwrap()[0].last_tick_at.is_none());
    }

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

    /// A Project whose `on_task_done` script hook drops a marker in its
    /// working directory, a Task, and a workspace row for a worktree that is
    /// not on disk. Returns the user's repository and the workspace id.
    async fn seed_hook_fixture(
        db: &SqliteDb,
        root: &Path,
        status: db::WorkspaceStatus,
    ) -> (PathBuf, String, String) {
        use db::{ProjectRepo, RepoRepo, TaskRepo, WorkspaceRepo};

        let now = db::now_rfc3339();
        let (project_id, repo_id, task_id, workspace_id) = (
            db::new_uuid_v4(),
            db::new_uuid_v4(),
            db::new_uuid_v4(),
            db::new_uuid_v4(),
        );
        let user_repo = root.join("user-repository");
        std::fs::create_dir_all(&user_repo).unwrap();
        git::init(&user_repo).await.unwrap();
        git::commit_all(&user_repo, "initial commit").await.unwrap();
        let settings = serde_json::json!({
            "lifecycle_hooks": {
                "on_task_done": [{"type": "script", "command": "touch hook-ran"}]
            }
        });
        ProjectRepo::create(
            db,
            db::CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: settings.to_string(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        RepoRepo::create(
            db,
            db::CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: None,
                local_path: Some(user_repo.to_string_lossy().into_owned()),
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        TaskRepo::create(
            db,
            db::CreateTask {
                id: task_id.clone(),
                project_id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Hook task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "done".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        WorkspaceRepo::create(
            db,
            db::CreateWorkspace {
                id: workspace_id.clone(),
                task_id: task_id.clone(),
                repo_id,
                worktree_path: root
                    .join("worktrees")
                    .join(&task_id)
                    .join("repo")
                    .to_string_lossy()
                    .into_owned(),
                branch: ::workspace::task_branch_name(&task_id),
                status,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        (user_repo, task_id, workspace_id)
    }

    async fn migrated_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        Arc::new(SqliteDb::new(pool))
    }

    #[tokio::test]
    async fn script_hook_never_falls_back_to_the_user_repository_when_the_worktree_is_gone() {
        let db = migrated_db().await;
        let temp = tempfile::TempDir::new().unwrap();
        let (user_repo, task_id, workspace_id) =
            seed_hook_fixture(&db, temp.path(), db::WorkspaceStatus::Ready).await;
        let emitter = LifecycleEventEmitter::new(Arc::clone(&db), Arc::new(PluginRegistry::new()));

        // Missing directory.
        let error = emitter
            .emit_lifecycle_event(
                &task_id,
                None,
                api_types::LifecycleEvent::OnTaskDone,
                Some("review".to_owned()),
                None,
                None,
            )
            .await
            .expect_err("a missing worktree fails the hook");
        assert!(error.contains("workspace reset required"), "{error}");
        assert!(error.contains(&workspace_id), "{error}");
        assert!(!user_repo.join("hook-ran").exists());

        // A directory that is not a Git worktree is not the Task's tree either.
        let worktree = temp.path().join("worktrees").join(&task_id).join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        emitter
            .emit_lifecycle_event(
                &task_id,
                None,
                api_types::LifecycleEvent::OnTaskDone,
                Some("review".to_owned()),
                None,
                None,
            )
            .await
            .expect_err("a directory without Git metadata fails the hook");
        assert!(!user_repo.join("hook-ran").exists());
        assert!(!worktree.join("hook-ran").exists());
    }

    #[tokio::test]
    async fn script_hook_of_a_reclaimed_workspace_keeps_its_primary_checkout_context() {
        let db = migrated_db().await;
        let temp = tempfile::TempDir::new().unwrap();
        let (user_repo, task_id, _) =
            seed_hook_fixture(&db, temp.path(), db::WorkspaceStatus::Cleaned).await;
        let emitter = LifecycleEventEmitter::new(Arc::clone(&db), Arc::new(PluginRegistry::new()));

        emitter
            .emit_lifecycle_event(
                &task_id,
                None,
                api_types::LifecycleEvent::OnTaskDone,
                Some("review".to_owned()),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(user_repo.join("hook-ran").exists());
    }
}
