use crate::{
    merge_service::MergeService,
    workflow::engine::WorkflowEngine,
    workspace_backend::{EmbeddedWorkspaceBackend, ResolvedWorkspace, WorkspaceBackendRouter},
    workspace_execution_lock::WorkspaceExecutionLockManager,
    Result, ServiceError,
};
use async_trait::async_trait;
use db::{
    now_rfc3339, PlacementState, ProjectRepo, SqliteDb, TaskRepo, UpdateWorkspacePlacement,
    WorkspacePlacement, WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    sync::{watch, Mutex, OwnedMutexGuard},
    task::JoinHandle,
    time::{interval, timeout, Instant, MissedTickBehavior},
};
use tracing::info;
use workspace::{RepoCacheLockManager, WorkspaceManager};

const SWEEP_BUDGET: Duration = Duration::from_secs(60);
const TICK_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CLEANUP_BACKOFF: Duration = Duration::from_secs(60 * 60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);
const SWEEP_LIMIT: i64 = 64;
/// Failed attempts (about half an hour of backoff) after which a cleanup that
/// keeps failing becomes one operator attention item.
const CLEANUP_ATTENTION_ATTEMPTS: i64 = 5;

fn cleanup_attention_key(workspace_id: &str) -> String {
    format!("workspace-cleanup:{workspace_id}")
}

/// The branch a Task delivers into: its merge configuration's
/// `target_branch`, else the repository default. Mirrors the merge path.
fn delivery_target_branch(merge_config: Option<&str>, repo_default_branch: &str) -> Option<String> {
    if let Some(merge_config) = merge_config {
        // An unreadable configuration proves nothing: keep the branch.
        let value = serde_json::from_str::<serde_json::Value>(merge_config).ok()?;
        if let Some(target_branch) = value
            .get("target_branch")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|target_branch| !target_branch.is_empty())
        {
            return Some(target_branch.to_owned());
        }
    }
    let repo_default_branch = repo_default_branch.trim();
    Some(if repo_default_branch.is_empty() {
        "main".to_owned()
    } else {
        repo_default_branch.to_owned()
    })
}

fn cleanup_backoff(previous_attempts: i64) -> Duration {
    let exponent = u32::try_from(previous_attempts.max(0))
        .unwrap_or(u32::MAX)
        .min(6);
    TICK_INTERVAL
        .saturating_mul(1_u32 << exponent)
        .min(MAX_CLEANUP_BACKOFF)
}

#[derive(Default)]
struct SweepCursor {
    task_id: String,
    repo_id: String,
}

pub struct WorkspaceCleanupScheduler {
    db: Arc<SqliteDb>,
    workspace_backend_router: RwLock<Arc<WorkspaceBackendRouter>>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    terminal_cleanup: RwLock<Option<Arc<dyn WorkspaceCleanupObserver>>>,
    repo_cache_locks: RwLock<Arc<RepoCacheLockManager>>,
    lifecycle_locks: WorkspaceExecutionLockManager,
    sweep_cursor: Mutex<SweepCursor>,
}

#[async_trait]
pub trait WorkspaceCleanupObserver: Send + Sync {
    async fn cleanup_workspace_terminals(&self, workspace_id: &str) -> Result<()>;
}

impl WorkspaceCleanupScheduler {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, workspace_root: PathBuf) -> Self {
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
            repo_cache_locks: RwLock::new(Arc::new(RepoCacheLockManager::new())),
            lifecycle_locks: WorkspaceExecutionLockManager::new(),
            sweep_cursor: Mutex::new(SweepCursor::default()),
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

    pub(crate) fn set_repo_cache_locks(&self, locks: Arc<RepoCacheLockManager>) {
        match self.repo_cache_locks.write() {
            Ok(mut current) => *current = locks,
            Err(error) => tracing::warn!(%error, "workspace cleanup repository lock poisoned"),
        }
    }

    fn repo_cache_locks(&self) -> Result<Arc<RepoCacheLockManager>> {
        self.repo_cache_locks
            .read()
            .map(|locks| Arc::clone(&locks))
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "workspace cleanup repository lock poisoned: {error}"
                ))
            })
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

    // Reopening a terminal Task must wait for its physical cleanup to finish.
    pub(crate) async fn lock_task(&self, task: &db::Task) -> OwnedMutexGuard<()> {
        self.lifecycle_locks
            .acquire(task.parent_task_id.as_deref().unwrap_or(&task.id))
            .await
    }

    pub fn spawn(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("workspace-cleanup").with_tick_timeout(Duration::from_secs(3600))
            .start(shutdown, || false, move |worker, mut shutdown_rx| {
                let scheduler = Arc::clone(&self);
                async move {
                    let mut ticker = interval(TICK_INTERVAL);
                    let mut sweeper = interval(SWEEP_INTERVAL);
                    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    sweeper.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = ticker.tick() => { if let Err(error) = worker.tick(scheduler.tick()).await { tracing::warn!(worker = worker.name(), %error, "workspace cleanup tick failed"); } }
                            _ = sweeper.tick() => { if let Err(error) = worker.tick(scheduler.sweep()).await { tracing::warn!(worker = worker.name(), %error, "terminal Task workspace sweep failed"); } }
                            result = shutdown_rx.changed() => {
                                if result.is_err() || *shutdown_rx.borrow() { return Ok(()); }
                            }
                        }
                    }
                }
            })
    }

    pub async fn cleanup_now(&self, workspace_id: impl Into<String>) -> Result<()> {
        let workspace_id = workspace_id.into();
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id))?;
        // Immediate cleanup is still terminal-only: a non-terminal Task's
        // worktree and build output are never removed.
        let result = self.cleanup_task(&workspace.task_id, true).await;
        if let Err(cleanup_error) = &result {
            if let Err(error) = self
                .record_cleanup_failure(&workspace.task_id, cleanup_error)
                .await
            {
                tracing::warn!(task_id = %workspace.task_id, %error, %cleanup_error, "failed to back off immediate Task cleanup");
            }
            if matches!(cleanup_error, ServiceError::DaemonUnavailable { .. }) {
                return Ok(());
            }
        }
        result
    }

    pub(crate) async fn cleanup_terminal_task(&self, task_id: &str) -> Result<()> {
        // Filesystem deletion can outlive a cancelled future. Keep the lifecycle
        // lock until removal finishes so reopening cannot race a timed-out job.
        let result = self.cleanup_task(task_id, true).await;
        if let Err(cleanup_error) = &result {
            if let Err(error) = self.record_cleanup_failure(task_id, cleanup_error).await {
                tracing::warn!(%task_id, %error, %cleanup_error, "failed to back off Task cleanup");
            }
        }
        result
    }

    async fn record_cleanup_failure(
        &self,
        task_id: &str,
        cleanup_error: &ServiceError,
    ) -> Result<()> {
        // This path must also handle malformed workspace rows, so read and
        // update only the retry columns instead of decoding the full model.
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let previous_attempts = sqlx::query_scalar::<_, i64>(
            "SELECT cleanup_attempts FROM workspace
             WHERE task_id = ? AND status != 'cleaned'",
        )
        .bind(task_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(previous_attempts) = previous_attempts else {
            transaction.commit().await?;
            return Ok(());
        };
        let owner_unreachable = matches!(cleanup_error, ServiceError::DaemonUnavailable { .. });
        let delay = if owner_unreachable {
            TICK_INTERVAL
        } else {
            cleanup_backoff(previous_attempts)
        };
        let cleanup_after = chrono::Utc::now()
            + chrono::Duration::from_std(delay).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid cleanup delay: {error}"))
            })?;
        let last_cleanup_error = cleanup_error.to_string();
        let last_cleanup_error =
            crate::project_environment::bounded_output_tail(&last_cleanup_error);
        sqlx::query(
            "UPDATE workspace
             SET cleanup_attempts = cleanup_attempts + ?,
                 last_cleanup_error = ?, cleanup_after = ?, updated_at = ?
             WHERE task_id = ? AND status != 'cleaned'",
        )
        .bind(if owner_unreachable { 0_i64 } else { 1_i64 })
        .bind(last_cleanup_error)
        .bind(cleanup_after.to_rfc3339())
        .bind(now_rfc3339())
        .bind(task_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        if !owner_unreachable && previous_attempts + 1 >= CLEANUP_ATTENTION_ATTEMPTS {
            // The retry is already recorded; a failed notice must not undo it.
            if let Err(error) = self.raise_cleanup_attention(task_id).await {
                tracing::warn!(%task_id, %error, "failed to raise workspace cleanup attention");
            }
        }
        Ok(())
    }

    /// One open item per workspace, however many retries follow. It is
    /// reopened only after a success resolved it.
    async fn raise_cleanup_attention(&self, task_id: &str) -> Result<()> {
        use sqlx::Row as _;

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let Some(row) = sqlx::query(
            "SELECT w.id, w.worktree_path, w.cleanup_attempts, w.last_cleanup_error,
                    t.project_id, t.title
             FROM workspace w JOIN task t ON t.id = w.task_id
             WHERE w.task_id = ? AND w.status != 'cleaned'",
        )
        .bind(task_id)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(());
        };
        let workspace_id: String = row.try_get("id")?;
        let dedupe_key = cleanup_attention_key(&workspace_id);
        let open = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (SELECT 1 FROM attention_projection
                            WHERE dedupe_key = ? AND status <> 'resolved')",
        )
        .bind(&dedupe_key)
        .fetch_one(&mut *transaction)
        .await?;
        if open != 0 {
            transaction.commit().await?;
            return Ok(());
        }
        let project_id: String = row.try_get("project_id")?;
        let title: String = row.try_get("title")?;
        let path: String = row.try_get("worktree_path")?;
        let attempts: i64 = row.try_get("cleanup_attempts")?;
        let last_error: Option<String> = row.try_get("last_cleanup_error")?;
        let now = now_rfc3339();
        let event = db::DomainEventRepo::append_event_in_tx(
            &*self.db,
            &mut transaction,
            &db::CreateDomainEvent {
                id: db::new_uuid_v4(),
                event_type: "workspace.cleanup_failed".to_owned(),
                entity_type: "workspace".to_owned(),
                entity_id: workspace_id.clone(),
                actor_type: "system".to_owned(),
                actor_id: Some("workspace-cleanup".to_owned()),
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                correlation_id: task_id.to_owned(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("{dedupe_key}:{now}")),
                payload_json: serde_json::json!({
                    "task_id": task_id, "workspace_id": workspace_id, "attempts": attempts,
                })
                .to_string(),
                created_at: now.clone(),
            },
        )
        .await?;
        self.db
            .insert_attention_in_tx(
                &mut transaction,
                db::CreateAttentionProjection {
                    id: db::new_uuid_v4(),
                    attention_type: "progress_warning".to_owned(),
                    scope_type: "project".to_owned(),
                    scope_id: project_id,
                    identity_id: None,
                    source_event_id: event.id,
                    priority: 60,
                    status: "open".to_owned(),
                    summary: format!(
                        "Workspace cleanup keeps failing ({attempts} attempts): {title}"
                    ),
                    details_json: serde_json::json!({
                        "task": {"id": task_id, "title": title},
                        "workspace_id": workspace_id,
                        "path": path,
                        "attempts": attempts,
                        "last_cleanup_error": last_error,
                    })
                    .to_string(),
                    dedupe_key,
                    occurred_at: now.clone(),
                    updated_at: now,
                    acknowledged_at: None,
                    snoozed_until: None,
                    resolved_at: None,
                    updated_by_user_id: None,
                    recommended_action: "inspect".to_owned(),
                    source_sequence: Some(event.sequence),
                },
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn clear_cleanup_after(&self, task_id: &str) -> Result<()> {
        let now = now_rfc3339();
        sqlx::query(
            "UPDATE workspace
             SET cleanup_after = NULL, cleanup_attempts = 0,
                 last_cleanup_error = NULL, updated_at = ?
             WHERE task_id = ?
               AND (cleanup_after IS NOT NULL OR cleanup_attempts != 0
                    OR last_cleanup_error IS NOT NULL)",
        )
        .bind(&now)
        .bind(task_id)
        .execute(self.db.pool())
        .await?;
        // Nothing is left to reclaim for this Task, so nothing is left to report.
        sqlx::query(
            "UPDATE attention_projection
             SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                 updated_at = ?, version = version + 1
             WHERE status <> 'resolved'
               AND dedupe_key IN (SELECT 'workspace-cleanup:' || id FROM workspace
                                  WHERE task_id = ?)",
        )
        .bind(&now)
        .bind(&now)
        .bind(task_id)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }

    pub(crate) async fn reset_workspace_now(&self, workspace_id: String) -> Result<()> {
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id))?;
        self.cleanup_task(&workspace.task_id, false).await
    }

    pub async fn schedule(&self, workspace_id: impl AsRef<str>, delay: Duration) -> Result<()> {
        let workspace_id = workspace_id.as_ref();
        let cleanup_after = chrono::Utc::now()
            + chrono::Duration::from_std(delay).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid cleanup delay: {error}"))
            })?;
        WorkspaceRepo::set_cleanup_after(
            &*self.db,
            workspace_id,
            Some(cleanup_after.to_rfc3339()),
            &now_rfc3339(),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn tick(&self) -> Result<()> {
        let task_ids = sqlx::query_scalar::<_, String>(
            "SELECT task_id FROM workspace
             WHERE status != 'cleaned' AND (cleanup_after <= ? OR (status = 'cleaning' AND cleanup_after IS NULL))
             ORDER BY cleanup_after, id LIMIT ?",
        )
        .bind(now_rfc3339())
        .bind(SWEEP_LIMIT)
        .fetch_all(self.db.pool())
        .await?;
        let started = Instant::now();
        for task_id in task_ids {
            if let Err(error) = self.cleanup_terminal_task(&task_id).await {
                tracing::warn!(%task_id, %error, "scheduled Task workspace cleanup failed");
            }
            if started.elapsed() >= SWEEP_BUDGET {
                break;
            }
        }
        Ok(())
    }

    pub(crate) async fn sweep(&self) -> Result<()> {
        let mut cursor = self.sweep_cursor.lock().await;
        let task_ids =
            sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE id > ? ORDER BY id LIMIT ?")
                .bind(&cursor.task_id)
                .bind(SWEEP_LIMIT)
                .fetch_all(self.db.pool())
                .await?;
        let started = Instant::now();
        for task_id in &task_ids {
            cursor.task_id = task_id.clone();
            if let Err(error) = self.cleanup_terminal_task(task_id).await {
                tracing::warn!(%task_id, %error, "terminal Task workspace backfill failed");
            }
            if started.elapsed() >= SWEEP_BUDGET {
                return Ok(());
            }
        }
        if task_ids.len() < SWEEP_LIMIT as usize {
            cursor.task_id.clear();
        }

        let repo_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM repo WHERE id > ?
             UNION SELECT repo_id AS id FROM workspace WHERE repo_id > ?
             ORDER BY id LIMIT ?",
        )
        .bind(&cursor.repo_id)
        .bind(&cursor.repo_id)
        .bind(SWEEP_LIMIT)
        .fetch_all(self.db.pool())
        .await?;
        for repo_id in &repo_ids {
            cursor.repo_id = repo_id.clone();
            // Broad pruning is safe only in Forge-owned repository caches.
            // User local_path repositories are cleaned by exact worktree path.
            if let Some(source) = self.cached_repo_source(repo_id) {
                let locks = self.repo_cache_locks()?;
                match timeout(SWEEP_BUDGET, async {
                    let _guard = locks.acquire(&source.to_string_lossy()).await;
                    WorkspaceManager::prune_worktrees(&source).await
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(%repo_id, %error, "worktree prune failed"),
                    Err(_) => tracing::warn!(%repo_id, "worktree prune timed out"),
                }
            }
            if started.elapsed() >= SWEEP_BUDGET {
                return Ok(());
            }
        }
        if repo_ids.len() < SWEEP_LIMIT as usize {
            cursor.repo_id.clear();
        }
        Ok(())
    }

    fn cached_repo_source(&self, repo_id: &str) -> Option<PathBuf> {
        let cache = self.workspace_root.join(".repos").join(repo_id);
        cache.exists().then_some(cache)
    }

    async fn cleanup_task(&self, task_id: &str, terminal_only: bool) -> Result<()> {
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, true).await? else {
            return self.clear_cleanup_after(task_id).await;
        };
        let _guard = self.lock_task(&task).await;
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, true).await? else {
            return self.clear_cleanup_after(task_id).await;
        };
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return self.clear_cleanup_after(task_id).await;
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        if terminal_only
            && workflow.state_kind(&task.status) != Some(api_types::StateKind::Terminal)
        {
            return self.clear_cleanup_after(task_id).await;
        }
        let workspace = WorkspaceRepo::get_by_task_id(&*self.db, task_id).await?;
        if terminal_only {
            let now = chrono::Utc::now();
            // The sweep also sees Tasks with no workspace/deadline, such as
            // terminal subtasks and historical managed homes. Preserve their
            // grace period using the terminal Task's timestamp.
            let grace_until = match workflow.cleanup_policy_for(&task.status) {
                Some(api_types::CleanupPolicy::Delayed { seconds }) => {
                    let updated_at = chrono::DateTime::parse_from_rfc3339(&task.updated_at)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    Some(
                        updated_at
                            + chrono::Duration::from_std(Duration::from_secs(seconds)).map_err(
                                |error| ServiceError::invalid_operation(error.to_string()),
                            )?,
                    )
                }
                _ => None,
            };
            if let Some(grace_until) = grace_until.filter(|deadline| *deadline > now) {
                if let Some(workspace) = workspace.as_ref() {
                    WorkspaceRepo::set_cleanup_after(
                        &*self.db,
                        &workspace.id,
                        Some(grace_until.to_rfc3339()),
                        &now_rfc3339(),
                    )
                    .await?;
                }
                return Ok(());
            }
            if let Some(workspace) = workspace.as_ref() {
                if let Some(deadline) = workspace.cleanup_after.as_deref() {
                    let deadline = chrono::DateTime::parse_from_rfc3339(deadline)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    if deadline > now {
                        return Ok(());
                    }
                }
                if workspace.status != WorkspaceStatus::Cleaned {
                    self.schedule(&workspace.id, TICK_INTERVAL).await?;
                }
            }
        }
        let workspace_id = workspace.as_ref().map(|workspace| workspace.id.as_str());
        let active = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM execution
                WHERE (task_id = ? OR workspace_id = ?) AND status = 'running'
             ) OR EXISTS (
                SELECT 1 FROM workspace_lease wl
                LEFT JOIN execution e ON e.id = wl.execution_id
                WHERE (wl.task_id = ? OR e.workspace_id = ?) AND wl.status = 'active'
             )",
        )
        .bind(task_id)
        .bind(workspace_id)
        .bind(task_id)
        .bind(workspace_id)
        .fetch_one(self.db.pool())
        .await?;
        if active != 0 {
            info!(
                task_id,
                "deferring Task cleanup while execution or lease is active"
            );
            return Ok(());
        }
        let cleanup_result = if let Some(workspace) = workspace {
            let children = TaskRepo::list_subtasks_ordered(&*self.db, task_id).await?;
            if children
                .iter()
                .any(|child| !crate::task_hierarchy::subtask_is_terminal(child, &workflow))
            {
                return Ok(());
            }
            let router = self.workspace_backend_router()?;
            let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
                &router,
                &self.db,
                &workspace,
                &self.workspace_root,
            )
            .await?;
            let present = if workspace.status != WorkspaceStatus::Cleaned {
                true
            } else {
                match resolved.backend.describe(&resolved.placement).await {
                    Ok(state) => state.exists,
                    Err(error)
                        if resolved.placement.state == PlacementState::Cleaned
                            && crate::workspace_backend::is_unknown_workspace_handle(&error) =>
                    {
                        false
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if present {
                // A reset keeps the branch: only a terminal Task's delivered
                // branch is reclaimed.
                let delivered_into = if terminal_only {
                    self.delivery_target(&task, &workspace).await?
                } else {
                    None
                };
                self.cleanup_workspace(workspace, delivered_into).await
            } else {
                Self::acknowledge_cleanup_receipts(&resolved).await?;
                Ok(())
            }
        } else {
            Ok(())
        };
        if terminal_only {
            let home = self
                .workspace_root
                .join(".forge")
                .join("logs")
                .join(&task.project_id)
                .join(task_id)
                .join(".codex-managed-home");
            match tokio::fs::remove_dir_all(home).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(ServiceError::Git(git::GitError::Io(error))),
            }
        }
        cleanup_result
    }

    /// The target branch to prove delivery against, or `None` when this
    /// workspace's branch must be kept whatever Git says: the name is not the
    /// one Forge gave this Task, the repository is unknown, or another live
    /// workspace of the repository uses the same branch name (Task branches
    /// carry only the first eight characters of the Task id).
    async fn delivery_target(
        &self,
        task: &db::Task,
        workspace: &db::Workspace,
    ) -> Result<Option<String>> {
        if workspace.task_id != task.id
            || workspace.branch != workspace::task_branch_name(&workspace.task_id)
        {
            return Ok(None);
        }
        let Some(repo) = db::RepoRepo::get_by_id(&*self.db, &workspace.repo_id).await? else {
            return Ok(None);
        };
        let shared = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (SELECT 1 FROM workspace
                            WHERE repo_id = ? AND branch = ? AND id != ? AND status != 'cleaned')",
        )
        .bind(&workspace.repo_id)
        .bind(&workspace.branch)
        .bind(&workspace.id)
        .fetch_one(self.db.pool())
        .await?;
        if shared != 0 {
            return Ok(None);
        }
        Ok(delivery_target_branch(
            task.merge_config.as_deref(),
            &repo.default_branch,
        ))
    }

    async fn cleanup_workspace(
        &self,
        workspace: db::Workspace,
        delivered_into: Option<String>,
    ) -> Result<()> {
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
                .cleanup_workspace_terminals(&workspace.id)
                .await?;
        }
        let router = self.workspace_backend_router()?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            &router,
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        let placement = self.begin_cleanup(&resolved.placement).await?;
        let resolved = ResolvedWorkspace {
            placement,
            ..resolved
        };
        let ack = resolved.backend.cleanup(&resolved.placement).await?;
        if workspace.status == WorkspaceStatus::Cleaned {
            // Leftovers of a workspace that was reclaimed earlier. Its branch
            // was settled then (or predates branch reclamation) and stays.
            Self::acknowledge_cleanup_receipts(&resolved).await?;
            return Ok(());
        }
        // Before the row is marked cleaned, so a failure here is retried.
        if let Some(target_branch) = delivered_into.as_deref() {
            resolved
                .backend
                .reclaim_delivered_branch(&resolved.placement, target_branch)
                .await?;
        }
        let workspace = self.acknowledge_cleanup(&resolved.placement, &ack).await?;
        Self::acknowledge_cleanup_receipts(&resolved).await?;
        info!(workspace_id = %workspace.id, task_id = %workspace.task_id, "workspace cleaned");
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
        let mut current = placement.clone();
        for _ in 0..3 {
            if current.generation != placement.generation
                || current.workspace_handle != placement.workspace_handle
                || !matches!(
                    current.state,
                    PlacementState::Cleaning | PlacementState::Cleaned
                )
            {
                return Err(db::DbError::VersionConflict.into());
            }
            let now = now_rfc3339();
            let mut transaction = db::begin_immediate(self.db.pool()).await?;
            if current.state != PlacementState::Cleaned {
                match WorkspacePlacementRepo::update_in_tx(
                    &*self.db,
                    &mut transaction,
                    cleanup_state_update(&current, PlacementState::Cleaned, &now),
                )
                .await
                {
                    Ok(_) => {}
                    Err(db::DbError::VersionConflict) => {
                        transaction.rollback().await?;
                        current = WorkspacePlacementRepo::get_by_id(&*self.db, &current.id)
                            .await?
                            .ok_or_else(|| {
                                ServiceError::not_found("placement", current.id.clone())
                            })?;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            sqlx::query(
                "UPDATE workspace
                         SET status = 'cleaned', cleanup_after = NULL,
                             cleanup_attempts = 0, last_cleanup_error = NULL,
                             error = NULL, updated_at = ?
                         WHERE id = ?",
            )
            .bind(&now)
            .bind(&current.workspace_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE attention_projection
                 SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                     updated_at = ?, version = version + 1
                 WHERE dedupe_key = ? AND status <> 'resolved'",
            )
            .bind(&now)
            .bind(&now)
            .bind(cleanup_attention_key(&current.workspace_id))
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return WorkspaceRepo::get_by_id(&*self.db, &current.workspace_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("workspace", current.workspace_id));
        }
        Err(db::DbError::VersionConflict.into())
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

    #[test]
    fn cleanup_backoff_starts_at_one_minute_and_caps_at_one_hour() {
        assert_eq!(cleanup_backoff(0), Duration::from_secs(60));
        assert_eq!(cleanup_backoff(1), Duration::from_secs(120));
        assert_eq!(cleanup_backoff(6), Duration::from_secs(3600));
        assert_eq!(cleanup_backoff(7), Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn cleanup_accepts_owner_notification_winning_the_cleaned_state_cas() {
        let db = sqlite_db().await;
        let (_, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();
        let cleaning = WorkspacePlacementRepo::update(
            &*db,
            cleanup_state_update(&placement, PlacementState::Cleaning, &now_rfc3339()),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE workspace SET status = 'cleaning' WHERE id = ?")
            .bind(&cleaning.workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        let notification: api_types::WorkspaceCleanupResult =
            serde_json::from_value(serde_json::json!({
                "entry_id": "cleanup-entry",
                "operation_id": "cleanup-operation",
                "workspace_handle": cleaning.workspace_handle,
                "generation": cleaning.generation,
                "cleaned": true,
            }))
            .unwrap();
        assert_eq!(
            crate::recovery::apply_owner_cleanup(
                &db,
                cleaning.daemon_id.as_deref().unwrap(),
                &notification,
            )
            .await
            .unwrap(),
            crate::daemon_transport::DaemonTerminalDisposition::Acknowledge
        );

        let temp = TempDir::new().unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            db.clone(),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        let workspace = scheduler
            .acknowledge_cleanup(
                &cleaning,
                &crate::workspace_backend::CleanupAck { removed: true },
            )
            .await
            .expect("the already committed owner acknowledgement is idempotent");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
        assert_eq!(
            WorkspacePlacementRepo::get_by_id(&*db, &cleaning.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            PlacementState::Cleaned
        );
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
        let repo_path = workspace_root.join(".repos").join(&repo_id);
        std::fs::create_dir_all(&repo_path).expect("repo creates");
        git::init(&repo_path).await.expect("git initializes");
        git::commit_all(&repo_path, "initial commit")
            .await
            .expect("initial commit");
        WorkspaceManager::new(workspace_root.to_path_buf())
            .create_worktree_named(
                repo_path.to_str().expect("repo path"),
                &task_id,
                "repo",
                "main",
            )
            .await
            .expect("worktree creates");
        // Undelivered work: a branch with no commit of its own is already
        // contained in `main` and would be reclaimed with the worktree.
        std::fs::write(worktree_path.join("task-change.txt"), "task change")
            .expect("task change writes");
        git::commit_all(&worktree_path, "task change")
            .await
            .expect("task change commits");

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
                remote_url: Some("https://example.com/repo.git".to_owned()),
                local_path: None,
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
                project_id: project_id.clone(),
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
                task_id: task_id.clone(),
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
        std::fs::create_dir_all(worktree_path.join("node_modules")).expect("build output creates");
        std::fs::write(
            worktree_path.join("node_modules/build-output"),
            "build output",
        )
        .expect("build output writes");
        let logs = workspace_root
            .join(".forge/logs")
            .join(project_id)
            .join(task_id);
        std::fs::create_dir_all(logs.join(".codex-managed-home/task-scratch"))
            .expect("managed home creates");
        std::fs::write(logs.join("execution.jsonl"), "retained log").expect("log writes");

        (workspace_id, worktree_path)
    }

    #[tokio::test]
    async fn cleanup_now_removes_worktree_and_marks_cleaned() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let scheduler =
            WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup succeeds");

        assert!(!worktree_path.exists());
        let task = fixture_task(&db, &workspace_id).await;
        assert_retained_logs(temp.path(), &task);
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
        let scheduler =
            WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

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
        let scheduler =
            WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

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
        let scheduler =
            WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

        scheduler.tick().await.expect("tick succeeds");

        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    }

    async fn fixture_task(db: &SqliteDb, workspace_id: &str) -> db::Task {
        let workspace = WorkspaceRepo::get_by_id(db, workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        TaskRepo::get_by_id(db, &workspace.task_id, false)
            .await
            .expect("task loads")
            .expect("task exists")
    }

    fn logs_dir(root: &Path, task: &db::Task) -> PathBuf {
        root.join(".forge/logs")
            .join(&task.project_id)
            .join(&task.id)
    }

    fn assert_retained_logs(root: &Path, task: &db::Task) {
        let logs = logs_dir(root, task);
        assert!(!logs.join(".codex-managed-home").exists());
        assert_eq!(
            std::fs::read_to_string(logs.join("execution.jsonl")).expect("log remains"),
            "retained log"
        );
    }

    async fn set_fixture_status(db: &SqliteDb, task: &db::Task, status: &str) -> db::Task {
        TaskRepo::update_status(
            db,
            db::UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: status.to_owned(),
                assignee_id: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .expect("fixture status updates")
    }

    async fn fixture_execution(
        db: &SqliteDb,
        task: &db::Task,
        workspace_id: &str,
        status: db::ExecutionStatus,
    ) -> db::Execution {
        let now = now_rfc3339();
        db::ExecutionRepo::create(
            db,
            db::CreateExecution {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                agent_id: None,
                role: "coder".to_owned(),
                status,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: Some(workspace_id.to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates")
    }

    #[tokio::test]
    async fn terminal_transitions_schedule_cleanup_and_preserve_cancelled_grace() {
        for (terminal, with_hooks) in [
            ("done", false),
            ("done", true),
            ("cancelled", false),
            ("cancelled", true),
        ] {
            let db = sqlite_db().await;
            let bus = Arc::new(EventBus::new(16));
            let temp = TempDir::new().expect("temp dir creates");
            let (workspace_id, worktree_path) =
                seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
            let task = fixture_task(&db, &workspace_id).await;
            let task = set_fixture_status(&db, &task, "todo").await;
            let mut workflow = crate::workflow::default_workflow::default_workflow();
            if !with_hooks {
                for state in &mut workflow.states {
                    state.hooks = api_types::StateHooks::default();
                }
            }
            let project = ProjectRepo::get_by_id(&*db, &task.project_id)
                .await
                .expect("project loads")
                .expect("project exists");
            ProjectRepo::update_workflow(
                &*db,
                &project.id,
                &serde_json::to_string(&workflow).expect("workflow serializes"),
                None,
                project.version,
                &now_rfc3339(),
            )
            .await
            .expect("workflow updates");
            let scheduler = Arc::new(WorkspaceCleanupScheduler::new(
                Arc::clone(&db),
                Arc::clone(&bus),
                temp.path().to_path_buf(),
            ));
            let service = crate::TaskService::new(Arc::clone(&db), bus)
                .with_cleanup_scheduler(Arc::clone(&scheduler))
                .with_workspace_root(temp.path().to_path_buf());
            let updated = service
                .transition(
                    task.id.clone(),
                    terminal.to_owned(),
                    (task.version, Some("finish cleanup fixture".to_owned())),
                )
                .await
                .expect("terminal transition succeeds");
            assert_eq!(updated.task.status, terminal);
            service.drain(&task.id).await.unwrap();
            assert!(
                worktree_path.exists(),
                "deletion must run outside the transition"
            );
            assert!(logs_dir(temp.path(), &task)
                .join(".codex-managed-home")
                .exists());
            let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .expect("workspace loads")
                .expect("workspace exists");
            assert_eq!(workspace.status, WorkspaceStatus::Ready);
            let deadline = chrono::DateTime::parse_from_rfc3339(
                workspace
                    .cleanup_after
                    .as_deref()
                    .expect("cleanup schedules"),
            )
            .unwrap();
            if terminal == "cancelled" {
                assert!(deadline > chrono::Utc::now() + chrono::Duration::hours(23));
                scheduler.tick().await.expect("tick respects grace");
                scheduler.sweep().await.expect("sweep respects grace");
                assert!(worktree_path.exists());
                assert!(logs_dir(temp.path(), &task)
                    .join(".codex-managed-home")
                    .exists());
                // Advance the persisted terminal timestamp and deadline past grace.
                sqlx::query("UPDATE task SET updated_at = ?, version = version + 1 WHERE id = ?")
                    .bind((chrono::Utc::now() - chrono::Duration::hours(25)).to_rfc3339())
                    .bind(&task.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                scheduler
                    .schedule(&workspace_id, Duration::ZERO)
                    .await
                    .unwrap();
            }
            scheduler.tick().await.expect("worker cleans due workspace");
            assert!(!worktree_path.exists());
            assert_retained_logs(temp.path(), &task);
            assert_eq!(
                WorkspaceRepo::get_by_id(&*db, &workspace_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                WorkspaceStatus::Cleaned
            );
            let source = temp.path().join(".repos").join(&workspace.repo_id);
            assert!(git::branch_exists(&source, &workspace.branch)
                .await
                .expect("branch lookup"));
        }
    }

    #[tokio::test]
    async fn nonterminal_tasks_are_untouched_even_with_completed_executions_and_stale_deadlines() {
        for status in ["review", "merge_failed"] {
            let db = sqlite_db().await;
            let temp = TempDir::new().expect("temp dir creates");
            let (workspace_id, worktree_path) =
                seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
            let task = fixture_task(&db, &workspace_id).await;
            let task = set_fixture_status(&db, &task, status).await;
            fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Completed).await;
            let scheduler = WorkspaceCleanupScheduler::new(
                Arc::clone(&db),
                Arc::new(EventBus::new(16)),
                temp.path().to_path_buf(),
            );
            scheduler
                .schedule(&workspace_id, Duration::ZERO)
                .await
                .expect("deadline sets");
            scheduler
                .cleanup_now(workspace_id.clone())
                .await
                .expect("cleanup skips");
            scheduler.tick().await.expect("tick succeeds");
            scheduler.sweep().await.expect("sweep succeeds");
            assert!(worktree_path.join("node_modules/build-output").exists());
            assert!(logs_dir(temp.path(), &task)
                .join(".codex-managed-home")
                .exists());
            let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(workspace.status, WorkspaceStatus::Ready);
            assert!(workspace.cleanup_after.is_none());
        }
    }

    struct FailingTerminalCleanup;

    #[async_trait]
    impl WorkspaceCleanupObserver for FailingTerminalCleanup {
        async fn cleanup_workspace_terminals(&self, _workspace_id: &str) -> Result<()> {
            Err(ServiceError::invalid_operation("cleanup observer failed"))
        }
    }

    struct FailingWorkspaceCleanup {
        workspace_id: String,
    }

    #[async_trait]
    impl WorkspaceCleanupObserver for FailingWorkspaceCleanup {
        async fn cleanup_workspace_terminals(&self, workspace_id: &str) -> Result<()> {
            if workspace_id == self.workspace_id {
                return Err(ServiceError::invalid_operation("cleanup observer failed"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn cleanup_failure_persistence_bounds_errors_and_keeps_offline_retry_fixed() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (offline_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let offline_task_id = WorkspaceRepo::get_by_id(&*db, &offline_id)
            .await
            .unwrap()
            .unwrap()
            .task_id;
        sqlx::query(
            "UPDATE workspace
             SET cleanup_attempts = 6, last_cleanup_error = 'prior failure'
             WHERE id = ?",
        )
        .bind(&offline_id)
        .execute(db.pool())
        .await
        .unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        let before = chrono::Utc::now();
        scheduler
            .record_cleanup_failure(
                &offline_task_id,
                &ServiceError::DaemonUnavailable {
                    daemon_id: "offline-owner".to_owned(),
                },
            )
            .await
            .unwrap();
        let after = chrono::Utc::now();
        let offline = WorkspaceRepo::get_by_id(&*db, &offline_id)
            .await
            .unwrap()
            .unwrap();
        let retry_at = chrono::DateTime::parse_from_rfc3339(
            offline
                .cleanup_after
                .as_deref()
                .expect("retry is scheduled"),
        )
        .unwrap()
        .with_timezone(&chrono::Utc);
        assert_eq!(offline.cleanup_attempts, 6);
        assert!(retry_at >= before + chrono::Duration::seconds(60));
        assert!(retry_at <= after + chrono::Duration::seconds(60));

        let (bounded_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let bounded_task_id = WorkspaceRepo::get_by_id(&*db, &bounded_id)
            .await
            .unwrap()
            .unwrap()
            .task_id;
        let long_error = format!("{}diagnostic-tail", "界".repeat(2_000));
        scheduler
            .record_cleanup_failure(
                &bounded_task_id,
                &ServiceError::invalid_operation(long_error),
            )
            .await
            .unwrap();
        let bounded = WorkspaceRepo::get_by_id(&*db, &bounded_id)
            .await
            .unwrap()
            .unwrap();
        let error = bounded
            .last_cleanup_error
            .as_deref()
            .expect("last cleanup error is stored");
        assert_eq!(
            error.chars().count(),
            crate::project_environment::BLOCK_MESSAGE_OUTPUT_CHARS
        );
        assert!(error.ends_with("diagnostic-tail"));
    }

    #[tokio::test]
    async fn worker_robustness_cleanup_backs_off_failed_head_and_continues() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (failed_id, failed_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let (healthy_id, healthy_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        WorkspaceRepo::set_cleanup_after(
            &*db,
            &failed_id,
            Some("2000-01-01T00:00:00Z".to_owned()),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        WorkspaceRepo::set_cleanup_after(
            &*db,
            &healthy_id,
            Some("2001-01-01T00:00:00Z".to_owned()),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
            workspace_id: failed_id.clone(),
        }));

        scheduler.tick().await.unwrap();

        let failed = WorkspaceRepo::get_by_id(&*db, &failed_id)
            .await
            .unwrap()
            .unwrap();
        let healthy = WorkspaceRepo::get_by_id(&*db, &healthy_id)
            .await
            .unwrap()
            .unwrap();
        let first_retry = chrono::DateTime::parse_from_rfc3339(
            failed.cleanup_after.as_deref().expect("retry is scheduled"),
        )
        .unwrap();
        assert!(failed_path.exists());
        assert_eq!(failed.cleanup_attempts, 1);
        assert!(failed.last_cleanup_error.is_some());
        assert!(first_retry > chrono::Utc::now());
        assert_eq!(healthy.status, WorkspaceStatus::Cleaned);
        assert!(!healthy_path.exists());

        WorkspaceRepo::set_cleanup_after(
            &*db,
            &failed_id,
            Some("2002-01-01T00:00:00Z".to_owned()),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        scheduler.tick().await.unwrap();

        let failed = WorkspaceRepo::get_by_id(&*db, &failed_id)
            .await
            .unwrap()
            .unwrap();
        let second_retry = chrono::DateTime::parse_from_rfc3339(
            failed
                .cleanup_after
                .as_deref()
                .expect("retry is rescheduled"),
        )
        .unwrap();
        assert_eq!(failed.cleanup_attempts, 2);
        assert!(second_retry > first_retry);
    }

    #[tokio::test]
    async fn cleanup_failure_does_not_emit_transition_effect_failed() {
        let db = sqlite_db().await;
        let bus = Arc::new(EventBus::new(256));
        let mut events = bus.subscribe();
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "todo").await;
        let scheduler = Arc::new(WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::clone(&bus),
            temp.path().to_path_buf(),
        ));
        scheduler.set_terminal_cleanup_handler(Arc::new(FailingTerminalCleanup));
        let service = crate::TaskService::new(Arc::clone(&db), bus)
            .with_cleanup_scheduler(Arc::clone(&scheduler))
            .with_workspace_root(temp.path().to_path_buf());

        // Cleanup holds this same lock during physical deletion. The transition
        // must finish without waiting for the worker to acquire it.
        let guard = scheduler.lock_task(&task).await;
        let outcome = timeout(
            Duration::from_secs(5),
            service.transition(task.id.clone(), "done".to_owned(), (task.version, None)),
        )
        .await
        .expect("transition does not wait for cleanup")
        .expect("transition succeeds");
        drop(guard);
        assert_eq!(outcome.task.status, "done");
        service.drain(&task.id).await.unwrap();
        assert!(worktree_path.exists());
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        scheduler
            .tick()
            .await
            .expect("worker isolates cleanup failure");
        assert!(worktree_path.exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(workspace.cleanup_after.as_deref().unwrap())
                .unwrap()
                > chrono::Utc::now()
        );
        while let Ok(event) = events.try_recv() {
            assert_ne!(event.event_type, "transition.effect_failed");
        }
    }

    #[tokio::test]
    async fn tick_clears_ineligible_batch_and_reaches_later_terminal_task() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let repo_id = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap()
            .repo_id;
        let mut stale_ids = Vec::new();
        for index in 0..SWEEP_LIMIT {
            let stale_task = seed_log_only_task(
                &db,
                temp.path(),
                &task,
                new_uuid_v4(),
                None,
                if index < 2 { "done" } else { "review" },
            )
            .await;
            let id = new_uuid_v4();
            WorkspaceRepo::create(
                &*db,
                CreateWorkspace {
                    id: id.clone(),
                    task_id: stale_task.id.clone(),
                    repo_id: repo_id.clone(),
                    worktree_path: temp
                        .path()
                        .join(&stale_task.id)
                        .join("repo")
                        .to_string_lossy()
                        .into_owned(),
                    branch: workspace::task_branch_name(&stale_task.id),
                    status: WorkspaceStatus::Ready,
                    before_sha: None,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            WorkspaceRepo::set_cleanup_after(
                &*db,
                &id,
                Some((chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339()),
                &now_rfc3339(),
            )
            .await
            .unwrap();
            if index < 2 {
                // Reproduce historical dangling references without changing
                // the schema's normal foreign-key guarantees.
                let mut connection = db.pool().acquire().await.unwrap();
                sqlx::query("PRAGMA foreign_keys = OFF")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                if index == 0 {
                    sqlx::query(
                        "UPDATE task SET project_id = ?, version = version + 1 WHERE id = ?",
                    )
                    .bind(new_uuid_v4())
                    .bind(&stale_task.id)
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                } else {
                    sqlx::query("UPDATE workspace SET task_id = ? WHERE id = ?")
                        .bind(new_uuid_v4())
                        .bind(&id)
                        .execute(&mut *connection)
                        .await
                        .unwrap();
                }
                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            }
            stale_ids.push(id);
        }
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .unwrap();
        scheduler.tick().await.unwrap();
        assert!(
            worktree_path.exists(),
            "first batch contains only ineligible rows"
        );
        for id in stale_ids {
            let workspace = WorkspaceRepo::get_by_id(&*db, &id).await.unwrap().unwrap();
            assert_eq!(workspace.status, WorkspaceStatus::Ready);
            assert!(workspace.cleanup_after.is_none());
        }
        scheduler.tick().await.unwrap();
        assert!(
            !worktree_path.exists(),
            "ineligible rows must not starve later cleanup"
        );
    }

    #[tokio::test]
    async fn tick_backs_off_errors_before_workspace_is_loaded() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .unwrap();
        let mut connection = db.pool().acquire().await.unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("UPDATE workspace SET status = 'invalid' WHERE id = ?")
            .bind(&workspace_id)
            .execute(&mut *connection)
            .await
            .unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        scheduler.tick().await.unwrap();
        let deadline =
            sqlx::query_scalar::<_, String>("SELECT cleanup_after FROM workspace WHERE id = ?")
                .bind(&workspace_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(&deadline).unwrap() > chrono::Utc::now());
        assert!(worktree_path.exists());
        sqlx::query("UPDATE workspace SET status = 'ready' WHERE id = ?")
            .bind(&workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        scheduler.tick().await.unwrap();
        assert!(worktree_path.exists(), "failed row respects retry backoff");
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .unwrap();
        scheduler.tick().await.unwrap();
        assert!(!worktree_path.exists());
    }

    #[tokio::test]
    async fn sweep_preserves_cancelled_managed_home_without_workspace_until_grace_expires() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let root = fixture_task(&db, &workspace_id).await;
        let task =
            seed_log_only_task(&db, temp.path(), &root, new_uuid_v4(), None, "cancelled").await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.unwrap();
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        sqlx::query("UPDATE task SET updated_at = ?, version = version + 1 WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::Duration::hours(25)).to_rfc3339())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        scheduler.sweep().await.unwrap();
        assert_retained_logs(temp.path(), &task);
    }

    #[tokio::test]
    async fn sweep_never_prunes_user_owned_local_repository() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        set_fixture_status(&db, &task, "review").await;
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        let user_repo = temp.path().join("user-repo");
        std::fs::create_dir_all(&user_repo).unwrap();
        git::init(&user_repo).await.unwrap();
        git::commit_all(&user_repo, "initial commit").await.unwrap();
        let user_worktree = temp.path().join("user-worktree");
        let output = tokio::process::Command::new("git")
            .args(["worktree", "add", "--detach"])
            .arg(&user_worktree)
            .arg("HEAD")
            .current_dir(&user_repo)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        std::fs::remove_dir_all(&user_worktree).unwrap();
        sqlx::query("UPDATE repo SET local_path = ? WHERE id = ?")
            .bind(user_repo.to_string_lossy().as_ref())
            .bind(&workspace.repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.unwrap();
        let output = tokio::process::Command::new("git")
            .args(["worktree", "list", "--porcelain"])
            .current_dir(&user_repo)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains(user_worktree.to_str().unwrap()));
    }

    #[tokio::test]
    async fn sweep_backfills_terminal_tasks_and_prunes_missing_registrations() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        // An earlier cleaner left a registration and only an outbox directory.
        std::fs::remove_dir_all(&worktree_path).expect("worktree removes");
        std::fs::create_dir_all(worktree_path.join(".forge-outbox")).expect("outbox remains");
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        assert!(WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap()
            .cleanup_after
            .is_none());
        scheduler.sweep().await.expect("sweep succeeds");
        assert!(!worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        let source = temp.path().join(".repos").join(&workspace.repo_id);
        let output = tokio::process::Command::new("git")
            .args(["worktree", "list", "--porcelain"])
            .current_dir(&source)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .expect("worktree list");
        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(worktree_path.to_str().unwrap()));
        assert!(git::branch_exists(&source, &workspace.branch)
            .await
            .expect("branch kept"));
    }

    #[tokio::test]
    async fn running_execution_blocks_terminal_cleanup_until_settled() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup defers");
        scheduler.sweep().await.expect("sweep succeeds");
        assert!(worktree_path.exists());
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .expect("execution settles");
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .unwrap();
        scheduler.sweep().await.expect("backfill retries");
        assert!(!worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
    }

    #[tokio::test]
    async fn active_workspace_lease_blocks_cleanup_even_after_execution_stops() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let now = now_rfc3339();
        let agent = db::AgentRepo::create(
            &*db,
            db::CreateAgent {
                id: new_uuid_v4(),
                name: "Cleanup lease worker".to_owned(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("agent creates");
        let execution =
            fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
        sqlx::query("UPDATE execution SET agent_id = ? WHERE id = ?")
            .bind(&agent.id)
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .expect("execution agent binds");
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        let lease = db::WorkspaceLeaseRepo::issue(
            &*db,
            db::CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: execution.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: workspace.repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: "[\"repository_write\"]".to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent.id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: now.clone(),
                expires_at: (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("lease issues");
        // Simulate interrupted historical settlement that left the lease active.
        sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .expect("execution stops");
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup defers");
        scheduler.sweep().await.expect("sweep defers");
        assert!(worktree_path.exists());
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        db::WorkspaceLeaseRepo::revoke(&*db, &lease.id, lease.version, &now_rfc3339())
            .await
            .expect("lease revokes");
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .unwrap();
        scheduler.sweep().await.expect("cleanup retries");
        assert!(!worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
    }

    async fn seed_log_only_task(
        db: &SqliteDb,
        root: &Path,
        task: &db::Task,
        id: String,
        parent_task_id: Option<String>,
        status: &str,
    ) -> db::Task {
        let now = now_rfc3339();
        let task = TaskRepo::create(
            db,
            CreateTask {
                id,
                project_id: task.project_id.clone(),
                parent_task_id,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Log cleanup".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: status.to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("log-only task creates");
        let logs = logs_dir(root, &task);
        std::fs::create_dir_all(logs.join(".codex-managed-home")).expect("managed home creates");
        std::fs::write(logs.join("execution.jsonl"), "retained log").expect("log writes");
        task
    }

    #[tokio::test]
    async fn terminal_subtask_keeps_shared_root_worktree() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let root = fixture_task(&db, &workspace_id).await;
        let root = set_fixture_status(&db, &root, "review").await;
        let child = seed_log_only_task(
            &db,
            temp.path(),
            &root,
            new_uuid_v4(),
            Some(root.id.clone()),
            "done",
        )
        .await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.expect("sweep succeeds");
        assert!(worktree_path.join("node_modules/build-output").exists());
        assert!(logs_dir(temp.path(), &root)
            .join(".codex-managed-home")
            .exists());
        assert_retained_logs(temp.path(), &child);
    }

    #[tokio::test]
    async fn sweep_is_bounded_and_cleans_homes_without_workspaces() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Cleaned).await;
        let root = fixture_task(&db, &workspace_id).await;
        let mut tasks = Vec::new();
        for index in 0..SWEEP_LIMIT + 1 {
            tasks.push(
                seed_log_only_task(
                    &db,
                    temp.path(),
                    &root,
                    format!("{}-backfill-{index:03}", root.id),
                    None,
                    "done",
                )
                .await,
            );
        }
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.expect("first sweep succeeds");
        assert_eq!(
            tasks
                .iter()
                .filter(|task| logs_dir(temp.path(), task)
                    .join(".codex-managed-home")
                    .exists())
                .count(),
            2
        );
        scheduler.sweep().await.expect("next page succeeds");
        for task in &tasks {
            assert_retained_logs(temp.path(), task);
        }
        assert_retained_logs(temp.path(), &root);
    }

    #[tokio::test]
    async fn sweep_prunes_missing_registration_without_cleaning_live_task() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "review").await;
        std::fs::remove_dir_all(&worktree_path).expect("external cleaner removes worktree");
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.expect("sweep succeeds");
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        let source = temp.path().join(".repos").join(&workspace.repo_id);
        let output = tokio::process::Command::new("git")
            .args(["worktree", "list", "--porcelain"])
            .current_dir(&source)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .expect("worktree list");
        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(worktree_path.to_str().unwrap()));
    }

    #[tokio::test]
    async fn terminal_codex_home_is_removed_when_repository_cleanup_fails() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        // A vanished repository no longer fails cleanup (the Task root is
        // simply removed). A checkout of some other repository at the
        // recorded path still does: it is not Forge's to delete.
        let source = temp.path().join(".repos").join(&workspace.repo_id);
        fixture_git(
            &source,
            &[
                "worktree",
                "remove",
                "--force",
                worktree_path.to_str().unwrap(),
            ],
        )
        .await;
        let other = temp.path().join("other-repository");
        std::fs::create_dir_all(&other).unwrap();
        git::init(&other).await.unwrap();
        git::commit_all(&other, "initial commit").await.unwrap();
        fixture_git(
            &other,
            &[
                "worktree",
                "add",
                "--detach",
                worktree_path.to_str().unwrap(),
                "HEAD",
            ],
        )
        .await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        assert!(scheduler.cleanup_now(workspace_id.clone()).await.is_err());
        assert!(worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Cleaning);
        assert!(
            chrono::DateTime::parse_from_rfc3339(workspace.cleanup_after.as_deref().unwrap())
                .unwrap()
                > chrono::Utc::now()
        );
    }

    #[tokio::test]
    async fn terminal_cleanup_reclaims_the_task_root_when_its_repository_is_gone() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(temp.path().join(".repos").join(&workspace.repo_id))
            .expect("source disappears");
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.cleanup_now(workspace_id.clone()).await.unwrap();
        assert!(!worktree_path.parent().unwrap().exists());
        assert_eq!(
            WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkspaceStatus::Cleaned
        );
    }

    #[tokio::test]
    async fn sweep_cleans_historical_workspace_after_repo_row_is_deleted() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        RepoRepo::delete(&*db, &workspace.repo_id)
            .await
            .expect("repo row deletes");
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.sweep().await.expect("sweep succeeds");
        assert!(!worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
        assert!(git::branch_exists(
            &temp.path().join(".repos").join(&workspace.repo_id),
            &workspace.branch
        )
        .await
        .expect("branch kept"));
    }
    #[tokio::test]
    async fn cleanup_terminal_sweep_removes_managed_home_after_owner_handle_retirement() {
        let db = sqlite_db().await;
        let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        sqlx::query(
            "UPDATE task SET status = 'done', updated_at = '1970-01-01T00:00:00Z' WHERE id = ?",
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE execution SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL WHERE id = ?").bind(&execution.id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE workspace SET status = 'cleaned' WHERE id = ?")
            .bind(&placement.workspace_id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE workspace_placement SET state = 'cleaned' WHERE id = ?")
            .bind(&placement.id)
            .execute(db.pool())
            .await
            .unwrap();
        let root = TempDir::new().unwrap();
        let home = root
            .path()
            .join(".forge/logs")
            .join(&task.project_id)
            .join(&task.id)
            .join(".codex-managed-home");
        std::fs::create_dir_all(&home).unwrap();
        let registry =
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
        let daemon_id = placement.daemon_id.unwrap();
        let (connection_id, mut outbound) =
            crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
        let service = crate::TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(root.path().into());
        let router = Arc::new(
            (*service.workspace_backend_router())
                .clone()
                .with_daemon(Arc::new(
                    crate::workspace_backend::DaemonWorkspaceBackend::new(
                        db.clone(),
                        registry.clone(),
                    ),
                )),
        );
        let scheduler = WorkspaceCleanupScheduler::new(
            db.clone(),
            Arc::new(EventBus::default()),
            root.path().into(),
        );
        scheduler.set_workspace_backend_router(router);
        let responder = tokio::spawn(async move {
            let api_types::DaemonFrame::Request { id, method, .. } = outbound.recv().await.unwrap()
            else {
                panic!("describe request");
            };
            assert_eq!(method, api_types::METHOD_WORKSPACE_DESCRIBE);
            registry.dispatch_incoming_for_connection(
                &daemon_id,
                connection_id,
                api_types::DaemonFrame::Error {
                    id: Some(id),
                    error: api_types::DaemonErrorPayload {
                        code: "invalid_input".into(),
                        message: "unknown workspace_handle".into(),
                        details: None,
                    },
                },
            );
        });
        scheduler.sweep().await.unwrap();
        responder.await.unwrap();
        assert!(!home.exists());
    }

    async fn fixture_git(cwd: &Path, args: &[&str]) -> String {
        let output = git::command_output(cwd, args).await.expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    async fn fixture_source(db: &SqliteDb, root: &Path, workspace_id: &str) -> (PathBuf, String) {
        let workspace = WorkspaceRepo::get_by_id(db, workspace_id)
            .await
            .unwrap()
            .unwrap();
        (
            root.join(".repos").join(&workspace.repo_id),
            workspace.branch,
        )
    }

    async fn clean_due(scheduler: &WorkspaceCleanupScheduler, workspace_id: &str) {
        scheduler
            .schedule(workspace_id, Duration::ZERO)
            .await
            .unwrap();
        scheduler.tick().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_cleanup_deletes_the_branch_only_when_git_proves_delivery() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        // Delivered: the tip is contained in the target branch.
        let (delivered_id, delivered_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let (source, branch) = fixture_source(&db, temp.path(), &delivered_id).await;
        fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        clean_due(&scheduler, &delivered_id).await;
        assert!(!delivered_path.exists());
        assert!(!git::branch_exists(&source, &branch).await.unwrap());
        assert!(git::branch_exists(&source, "main").await.unwrap());
        // Idempotent re-run over an already reclaimed workspace.
        scheduler.cleanup_now(delivered_id.clone()).await.unwrap();
        scheduler.sweep().await.unwrap();
        assert_eq!(
            WorkspaceRepo::get_by_id(&*db, &delivered_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkspaceStatus::Cleaned
        );

        // Delivered once, but the target moved and no longer contains the tip.
        let (moved_id, moved_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let (source, branch) = fixture_source(&db, temp.path(), &moved_id).await;
        let before = fixture_git(&source, &["rev-parse", "HEAD"]).await;
        fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        fixture_git(&source, &["reset", "--hard", &before]).await;
        clean_due(&scheduler, &moved_id).await;
        assert!(!moved_path.exists());
        assert!(git::branch_exists(&source, &branch).await.unwrap());
    }

    #[tokio::test]
    async fn reset_and_shared_branch_names_keep_a_delivered_branch() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        // A reset of a live Task rebuilds from the branch, so it stays.
        let (reset_id, reset_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &reset_id).await;
        set_fixture_status(&db, &task, "todo").await;
        let (source, branch) = fixture_source(&db, temp.path(), &reset_id).await;
        fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        scheduler
            .reset_workspace_now(reset_id.clone())
            .await
            .unwrap();
        assert!(!reset_path.exists());
        assert!(git::branch_exists(&source, &branch).await.unwrap());

        // Two Task ids can share the eight-character branch name. While the
        // other workspace is live, the name is not this Task's to delete.
        let (shared_id, shared_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &shared_id).await;
        let (source, branch) = fixture_source(&db, temp.path(), &shared_id).await;
        fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        let other =
            seed_log_only_task(&db, temp.path(), &task, new_uuid_v4(), None, "in_progress").await;
        let shared = WorkspaceRepo::get_by_id(&*db, &shared_id)
            .await
            .unwrap()
            .unwrap();
        let now = now_rfc3339();
        WorkspaceRepo::create(
            &*db,
            CreateWorkspace {
                id: new_uuid_v4(),
                task_id: other.id.clone(),
                repo_id: shared.repo_id.clone(),
                worktree_path: temp
                    .path()
                    .join(&other.id)
                    .join("repo")
                    .to_string_lossy()
                    .into_owned(),
                branch: branch.clone(),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        clean_due(&scheduler, &shared_id).await;
        assert!(!shared_path.exists());
        assert!(git::branch_exists(&source, &branch).await.unwrap());
    }

    #[tokio::test]
    async fn terminal_cleanup_never_deletes_a_branch_forge_did_not_name_for_the_task() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        // Merged branches of the user's, one of them under `task/`, and the
        // target itself: a row that names one of them deletes nothing.
        for recorded in ["task/users-own", "feature/mine", "main"] {
            let (workspace_id, worktree_path) =
                seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
            let (source, task_branch) = fixture_source(&db, temp.path(), &workspace_id).await;
            fixture_git(
                &source,
                &["merge", "--no-ff", "-m", "deliver", &task_branch],
            )
            .await;
            if recorded != "main" {
                fixture_git(&source, &["branch", recorded, "main"]).await;
            }
            sqlx::query("UPDATE workspace SET branch = ? WHERE id = ?")
                .bind(recorded)
                .bind(&workspace_id)
                .execute(db.pool())
                .await
                .unwrap();

            clean_due(&scheduler, &workspace_id).await;

            assert!(!worktree_path.exists());
            assert_eq!(
                WorkspaceRepo::get_by_id(&*db, &workspace_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                WorkspaceStatus::Cleaned
            );
            assert!(git::branch_exists(&source, recorded).await.unwrap());
            // The Task's own branch is not what the row names, so it stays too.
            assert!(git::branch_exists(&source, &task_branch).await.unwrap());
        }
    }

    #[tokio::test]
    async fn leftovers_of_an_already_cleaned_workspace_keep_its_branch() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Cleaned).await;
        let (source, branch) = fixture_source(&db, temp.path(), &workspace_id).await;
        fixture_git(&source, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        scheduler.sweep().await.unwrap();

        assert!(!worktree_path.exists());
        assert!(git::branch_exists(&source, &branch).await.unwrap());
    }

    async fn cleanup_attention(db: &SqliteDb, workspace_id: &str) -> Vec<(String, i64)> {
        sqlx::query_as::<_, (String, i64)>(
            "SELECT status, version FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(cleanup_attention_key(workspace_id))
        .fetch_all(db.pool())
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn five_failed_cleanups_raise_one_attention_item_that_success_clears() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
            workspace_id: workspace_id.clone(),
        }));

        for attempt in 1..=4 {
            clean_due(&scheduler, &workspace_id).await;
            assert!(
                cleanup_attention(&db, &workspace_id).await.is_empty(),
                "attempt {attempt} is still an ordinary retry"
            );
        }
        clean_due(&scheduler, &workspace_id).await;
        let raised = cleanup_attention(&db, &workspace_id).await;
        assert_eq!(raised.len(), 1);
        assert_eq!(raised[0].0, "open");
        let details = sqlx::query_scalar::<_, String>(
            "SELECT details_json FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(cleanup_attention_key(&workspace_id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        let details: serde_json::Value = serde_json::from_str(&details).unwrap();
        assert_eq!(details["attempts"], 5);
        assert_eq!(details["path"], worktree_path.to_string_lossy().as_ref());
        assert!(details["last_cleanup_error"]
            .as_str()
            .unwrap()
            .contains("cleanup observer failed"));

        // Later retries keep failing and keep the one item untouched.
        for _ in 0..3 {
            clean_due(&scheduler, &workspace_id).await;
        }
        assert_eq!(cleanup_attention(&db, &workspace_id).await, raised);
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.cleanup_attempts, 8);
        assert!(worktree_path.exists());
        let events = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE event_type = 'workspace.cleanup_failed'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(events, 1);

        // Success clears it.
        scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
            workspace_id: "another-workspace".to_owned(),
        }));
        clean_due(&scheduler, &workspace_id).await;
        assert!(!worktree_path.exists());
        let cleared = cleanup_attention(&db, &workspace_id).await;
        assert_eq!(cleared.len(), 1);
        assert_eq!(cleared[0].0, "resolved");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminal_cleanup_reclaims_read_only_trees_and_broken_copies() {
        use std::os::unix::fs::PermissionsExt;

        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task_root = worktree_path.parent().unwrap().to_path_buf();
        let cache = worktree_path.join("pkg/mod");
        let broken = task_root.join("repo.broken-1700000000000");
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(&broken).unwrap();
        std::fs::write(cache.join("module.go"), "package module\n").unwrap();
        std::fs::write(broken.join("leftover"), "leftover").unwrap();
        for (path, mode) in [
            (cache.join("module.go"), 0o400),
            (cache.clone(), 0o500),
            (worktree_path.join("pkg"), 0o500),
            (broken.join("leftover"), 0o400),
            (broken.clone(), 0o500),
        ] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        clean_due(&scheduler, &workspace_id).await;

        assert!(!task_root.exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
        assert_eq!(workspace.cleanup_attempts, 0);
    }
}
