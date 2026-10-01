use crate::{
    workspace_backend::{EmbeddedWorkspaceBackend, ResolvedWorkspace, WorkspaceBackendRouter},
    MergeService, Result, ServiceError,
};
use async_trait::async_trait;
use db::{
    now_rfc3339, PlacementState, SqliteDb, UpdateWorkspacePlacement, WorkspacePlacement,
    WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{interval, timeout},
};
use tracing::info;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const TICK_INTERVAL: Duration = Duration::from_secs(60);
const ACTIVE_EXECUTION_RETRY_DELAY: Duration = Duration::from_secs(60);

pub struct WorkspaceCleanupScheduler {
    db: Arc<SqliteDb>,
    workspace_backend_router: RwLock<Arc<WorkspaceBackendRouter>>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    terminal_cleanup: RwLock<Option<Arc<dyn WorkspaceCleanupObserver>>>,
}

#[async_trait]
pub trait WorkspaceCleanupObserver: Send + Sync {
    async fn cleanup_workspace_terminals(&self, workspace_id: &str) -> Result<()>;
}

impl WorkspaceCleanupScheduler {
    pub fn new_for_test(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        workspace_root: PathBuf,
    ) -> Self {
        let merge_service = Arc::new(MergeService::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            workspace_root.clone(),
        ));
        let router = Arc::new(WorkspaceBackendRouter::new(Arc::new(
            EmbeddedWorkspaceBackend::new(Arc::clone(&db), merge_service, workspace_root.clone()),
        )));
        Self::new_with_router(db, event_bus, workspace_root, router)
    }

    pub fn new_with_router(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        workspace_root: PathBuf,
        router: Arc<WorkspaceBackendRouter>,
    ) -> Self {
        Self {
            db,
            workspace_backend_router: RwLock::new(router),
            event_bus,
            workspace_root,
            terminal_cleanup: RwLock::new(None),
        }
    }

    pub fn set_workspace_backend_router(&self, router: Arc<WorkspaceBackendRouter>) {
        match self.workspace_backend_router.write() {
            Ok(mut current) => *current = router,
            Err(error) => tracing::warn!(%error, "cleanup workspace backend router lock poisoned"),
        }
    }

    fn workspace_backend_router(&self) -> Result<Arc<WorkspaceBackendRouter>> {
        self.workspace_backend_router
            .read()
            .map(|router| Arc::clone(&router))
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "cleanup workspace backend router lock poisoned: {error}"
                ))
            })
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn set_terminal_cleanup_handler(&self, handler: Arc<dyn WorkspaceCleanupObserver>) {
        match self.terminal_cleanup.write() {
            Ok(mut terminal_cleanup) => {
                *terminal_cleanup = Some(handler);
            }
            Err(error) => {
                tracing::warn!(%error, "workspace cleanup terminal handler lock poisoned");
            }
        }
    }

    pub fn spawn(self: Arc<Self>, mut shutdown_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(TICK_INTERVAL);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(error) = self.tick().await {
                            tracing::warn!(%error, "workspace cleanup tick failed");
                        }
                    }
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    }

    pub async fn cleanup_now(&self, workspace_id: impl Into<String>) -> Result<()> {
        let workspace_id = workspace_id.into();
        let cleanup = async {
            loop {
                match self.cleanup_workspace(&workspace_id, true).await {
                    // Disconnect or reconciliation may advance the placement
                    // between its read and cleanup CAS. Resolve it again.
                    Err(ServiceError::Db(db::DbError::VersionConflict)) => continue,
                    result => return result,
                }
            }
        };
        match timeout(CLEANUP_TIMEOUT, cleanup).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                tracing::warn!(%workspace_id, %error, "workspace cleanup failed");
                Ok(())
            }
            Err(_) => {
                tracing::warn!(%workspace_id, "workspace cleanup timed out");
                Ok(())
            }
        }
    }

    pub async fn schedule(&self, workspace_id: impl AsRef<str>, delay: Duration) -> Result<()> {
        let workspace_id = workspace_id.as_ref();
        let cleanup_after = chrono::Utc::now()
            + chrono::Duration::from_std(delay).map_err(|error| {
                crate::ServiceError::invalid_operation(format!("invalid cleanup delay: {error}"))
            })?;
        WorkspaceRepo::set_cleanup_after(
            &*self.db,
            workspace_id,
            Some(cleanup_after.to_rfc3339()),
            &now_rfc3339(),
        )
        .await?;
        info!(
            workspace_id,
            workspace_root = %self.workspace_root.display(),
            cleanup_after = %cleanup_after.to_rfc3339(),
            "workspace cleanup scheduled"
        );
        Ok(())
    }

    pub(crate) async fn tick(&self) -> Result<()> {
        let now = now_rfc3339();
        let workspaces = WorkspaceRepo::list_pending_cleanup(&*self.db, &now).await?;
        let mut pending_ids = workspaces
            .into_iter()
            .map(|workspace| workspace.id)
            .collect::<std::collections::BTreeSet<_>>();
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Cleaning).await?
        {
            pending_ids.insert(placement.workspace_id);
        }
        for workspace_id in pending_ids {
            if let Err(error) = self.cleanup_workspace(&workspace_id, false).await {
                tracing::warn!(%workspace_id, %error, "workspace cleanup remains pending");
            }
        }
        Ok(())
    }

    async fn cleanup_workspace(&self, workspace_id: &str, force: bool) -> Result<()> {
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let placement = EmbeddedWorkspaceBackend::ensure_server_placement(
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        if placement.state == PlacementState::Cleaned
            && workspace.status == WorkspaceStatus::Cleaned
        {
            if placement.owner_kind == db::PlacementOwnerKind::Daemon {
                // Reconciliation may have committed cleanup while its journal
                // acknowledgement is still in flight.
                let resolved = self
                    .workspace_backend_router()?
                    .resolve(&self.db, &workspace)
                    .await?;
                Self::acknowledge_cleanup_receipts(&resolved).await?;
            }
            return Ok(());
        }
        if !force && placement.state != PlacementState::Cleaning {
            let Some(cleanup_after) = workspace.cleanup_after.as_deref() else {
                // A pending-row snapshot may race with a child reusing this
                // workspace. Reuse clears the stale deadline; never let the
                // earlier scheduler snapshot delete the revived worktree.
                tracing::debug!(
                    workspace_id,
                    "skipping workspace cleanup after schedule was cleared"
                );
                return Ok(());
            };
            if chrono::DateTime::parse_from_rfc3339(cleanup_after)
                .map(|deadline| deadline.with_timezone(&chrono::Utc) > chrono::Utc::now())
                .unwrap_or(false)
            {
                return Ok(());
            }
        }

        let running_executions = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution
             WHERE workspace_id = ? AND status = 'running'",
        )
        .bind(workspace_id)
        .fetch_one(self.db.pool())
        .await?;
        if running_executions > 0 {
            let retry_at = chrono::Utc::now()
                + chrono::Duration::from_std(ACTIVE_EXECUTION_RETRY_DELAY).map_err(|error| {
                    crate::ServiceError::invalid_operation(format!(
                        "invalid active-execution cleanup delay: {error}"
                    ))
                })?;
            WorkspaceRepo::set_cleanup_after(
                &*self.db,
                workspace_id,
                Some(retry_at.to_rfc3339()),
                &now_rfc3339(),
            )
            .await?;
            tracing::info!(
                workspace_id,
                running_executions,
                cleanup_after = %retry_at.to_rfc3339(),
                "deferring workspace cleanup while execution is active"
            );
            return Ok(());
        }

        let placement = self.begin_cleanup(&placement).await?;
        info!(
            workspace_id,
            task_id = %workspace.task_id,
            placement_id = %placement.id,
            owner_kind = %placement.owner_kind,
            workspace_root = %self.workspace_root.display(),
            "cleaning up workspace"
        );
        let terminal_cleanup = self
            .terminal_cleanup
            .read()
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "workspace cleanup terminal handler lock poisoned: {error}"
                ))
            })?
            .clone();
        if let Some(terminal_cleanup) = terminal_cleanup {
            terminal_cleanup
                .cleanup_workspace_terminals(workspace_id)
                .await?;
        }
        let router = self.workspace_backend_router()?;
        let resolved = router.resolve(&self.db, &workspace).await?;
        let ack = resolved.backend.cleanup(&resolved.placement).await?;
        if !ack.removed {
            info!(workspace_id, task_id = %workspace.task_id, "workspace worktree already absent");
        }
        let workspace = self.acknowledge_cleanup(&resolved.placement, &ack).await?;
        // The state CAS is committed before the owner's durable removal
        // receipt is acknowledged. Reconnect sweeps retry a lost ack.
        Self::acknowledge_cleanup_receipts(&resolved).await?;
        info!(
            workspace_id = %workspace.id,
            "workspace cleaned"
        );
        self.event_bus.publish(ForgeEvent {
            event_type: "workspace.cleaned".to_owned(),
            entity_id: workspace.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::WorkspaceCleaned {
                workspace_id: workspace.id,
                task_id: workspace.task_id,
                status: "cleaned".to_owned(),
            },
        });
        Ok(())
    }

    async fn acknowledge_cleanup_receipts(resolved: &ResolvedWorkspace) -> Result<()> {
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon {
            let daemon_id = resolved.placement.daemon_id.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("cleanup placement has no daemon")
            })?;
            resolved
                .owner_client()?
                .retry_acknowledgements(daemon_id)
                .await
                .map_err(|error| ServiceError::from(resolved.owner_error(error)))?;
        }
        Ok(())
    }

    async fn begin_cleanup(&self, placement: &WorkspacePlacement) -> Result<WorkspacePlacement> {
        if matches!(
            placement.state,
            PlacementState::Cleaning | PlacementState::Cleaned
        ) {
            return Ok(placement.clone());
        }
        let now = now_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let updated = WorkspacePlacementRepo::update_in_tx(
            &*self.db,
            &mut transaction,
            cleanup_state_update(placement, PlacementState::Cleaning, &now),
        )
        .await?;
        sqlx::query("UPDATE workspace SET status = 'cleaning', cleanup_after = COALESCE(cleanup_after, ?), updated_at = ? WHERE id = ?")
            .bind(&now).bind(&now).bind(&placement.workspace_id)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(updated)
    }

    async fn acknowledge_cleanup(
        &self,
        placement: &WorkspacePlacement,
        _ack: &crate::workspace_backend::CleanupAck,
    ) -> Result<db::Workspace> {
        let now = now_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        if placement.state != PlacementState::Cleaned {
            WorkspacePlacementRepo::update_in_tx(
                &*self.db,
                &mut transaction,
                cleanup_state_update(placement, PlacementState::Cleaned, &now),
            )
            .await?;
        }
        sqlx::query("UPDATE workspace SET status = 'cleaned', cleanup_after = NULL, error = NULL, updated_at = ? WHERE id = ?")
            .bind(&now).bind(&placement.workspace_id)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
        WorkspaceRepo::get_by_id(&*self.db, &placement.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", placement.workspace_id.clone()))
    }
}

fn cleanup_state_update(
    placement: &WorkspacePlacement,
    state: PlacementState,
    now: &str,
) -> UpdateWorkspacePlacement {
    UpdateWorkspacePlacement {
        id: placement.id.clone(),
        expected_version: placement.version,
        agent_id: None,
        owner_kind: None,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: None,
        execution_daemon_id: None,
        workspace_handle: None,
        generation: None,
        state: Some(state),
        selected_by: None,
        selection_reason: None,
        reserved_until: Some(None),
        disconnected_at: None,
        failure_cause: None,
        updated_at: now.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, run_migrations, CreateProject, CreateRepo, CreateTask,
        CreateWorkspace, ProjectRepo, RepoRepo, TaskRepo, UpdateProject, WorkspaceStatus,
    };
    use tempfile::TempDir;

    async fn sqlite_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        Arc::new(SqliteDb::new(pool))
    }

    async fn seed_workspace(
        db: &SqliteDb,
        workspace_root: &std::path::Path,
        status: WorkspaceStatus,
    ) -> (String, PathBuf) {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let task_id = new_uuid_v4();
        let workspace_id = new_uuid_v4();
        let worktree_path = workspace_root.join(&task_id).join("repo");
        let branch = workspace::task_branch_name(&task_id);

        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "Forge".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: "https://example.com/repo.git".to_owned(),
                local_path: None,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");
        TaskRepo::create(
            db,
            CreateTask {
                id: task_id.clone(),
                project_id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Cleanup task".to_owned(),
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
        .expect("task creates");
        WorkspaceRepo::create(
            db,
            CreateWorkspace {
                id: workspace_id.clone(),
                task_id,
                repo_id,
                worktree_path: worktree_path.to_string_lossy().into_owned(),
                branch,
                status,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("workspace creates");
        std::fs::create_dir_all(&worktree_path).expect("worktree creates");

        (workspace_id, worktree_path)
    }

    #[tokio::test]
    async fn cleanup_now_removes_worktree_and_marks_cleaned() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let scheduler = WorkspaceCleanupScheduler::new_for_test(
            Arc::clone(&db),
            event_bus,
            temp.path().to_path_buf(),
        );

        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup succeeds");

        assert!(!worktree_path.exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    }

    #[tokio::test]
    async fn cleanup_marks_cleaned_when_worktree_is_missing() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        std::fs::remove_dir_all(worktree_path.parent().expect("task root exists"))
            .expect("worktree removes");
        let scheduler = WorkspaceCleanupScheduler::new_for_test(
            Arc::clone(&db),
            event_bus,
            temp.path().to_path_buf(),
        );

        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup succeeds");

        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
        assert!(workspace.cleanup_after.is_none());
    }

    #[tokio::test]
    async fn schedule_writes_cleanup_after() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let scheduler = WorkspaceCleanupScheduler::new_for_test(
            Arc::clone(&db),
            event_bus,
            temp.path().to_path_buf(),
        );

        scheduler
            .schedule(&workspace_id, Duration::from_secs(60))
            .await
            .expect("cleanup schedules");

        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert!(workspace.cleanup_after.is_some());
    }

    #[tokio::test]
    async fn loop_replays_past_due_workspaces() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let cleanup_after = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
        WorkspaceRepo::set_cleanup_after(&*db, &workspace_id, Some(cleanup_after), &now_rfc3339())
            .await
            .expect("cleanup deadline sets");
        let scheduler = WorkspaceCleanupScheduler::new_for_test(
            Arc::clone(&db),
            event_bus,
            temp.path().to_path_buf(),
        );

        scheduler.tick().await.expect("tick succeeds");

        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    }

    #[tokio::test]
    async fn cleanup_backend_failure_keeps_cleaning_until_retry_is_acknowledged() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let mut events = event_bus.subscribe();
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task_root = worktree_path.parent().expect("task root exists");
        std::fs::remove_dir_all(task_root).expect("worktree removes");
        std::fs::write(task_root, "cleanup cannot remove a file as a directory")
            .expect("cleanup obstruction writes");
        let scheduler = WorkspaceCleanupScheduler::new_for_test(
            Arc::clone(&db),
            Arc::clone(&event_bus),
            temp.path().to_path_buf(),
        );

        scheduler
            .cleanup_now(&workspace_id)
            .await
            .expect("cleanup is best effort");

        let pending = WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace_id)
            .await
            .expect("placement loads")
            .expect("placement exists");
        assert_eq!(pending.state, PlacementState::Cleaning);
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaning);
        assert!(events.try_recv().is_err());

        std::fs::remove_file(task_root).expect("cleanup obstruction removes");
        std::fs::create_dir_all(&worktree_path).expect("worktree recreates");
        WorkspaceRepo::set_cleanup_after(&*db, &workspace_id, None, &now_rfc3339())
            .await
            .expect("deadline clears");
        scheduler
            .tick()
            .await
            .expect("pending placement cleanup retries");

        let cleaned = WorkspacePlacementRepo::get_by_workspace_id(&*db, &workspace_id)
            .await
            .expect("placement loads")
            .expect("placement exists");
        assert_eq!(cleaned.id, pending.id);
        assert_eq!(cleaned.generation, pending.generation);
        assert_eq!(cleaned.state, PlacementState::Cleaned);
        assert_eq!(cleaned.version, pending.version + 1);
        assert!(!worktree_path.exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
        assert!(workspace.cleanup_after.is_none());
        assert_eq!(
            events
                .try_recv()
                .expect("cleanup event publishes")
                .event_type,
            "workspace.cleaned"
        );
        scheduler
            .cleanup_now(&workspace_id)
            .await
            .expect("ack replay is harmless");
        assert!(events.try_recv().is_err());
    }
}
