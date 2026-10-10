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

mod gc;
pub use gc::{GcSettings, LiveCheckCounter, STATUS_KEY as GC_STATUS_KEY};

const SWEEP_BUDGET: Duration = Duration::from_secs(60);
/// The part of [`SWEEP_BUDGET`] the Task and repository backfill may use. The
/// rest is kept for the garbage-collection pass, so a long backfill cannot
/// starve it.
const BACKFILL_BUDGET: Duration = Duration::from_secs(40);
const TICK_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CLEANUP_BACKOFF: Duration = Duration::from_secs(60 * 60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);
const SWEEP_LIMIT: i64 = 64;
/// Failed attempts (about half an hour of backoff) after which a cleanup that
/// keeps failing becomes one operator attention item.
const CLEANUP_ATTENTION_ATTEMPTS: i64 = 5;

pub(crate) fn cleanup_attention_key(workspace_id: &str) -> String {
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
    /// Last Task-root directory name the garbage-collection pass finished.
    root_name: String,
}

/// How long a garbage-collection pass waits for one Task's lifecycle lock.
pub(crate) const GC_LOCK_WAIT: Duration = Duration::from_secs(1);

pub struct WorkspaceCleanupScheduler {
    db: Arc<SqliteDb>,
    workspace_backend_router: RwLock<Arc<WorkspaceBackendRouter>>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    terminal_cleanup: RwLock<Option<Arc<dyn WorkspaceCleanupObserver>>>,
    repo_cache_locks: RwLock<Arc<RepoCacheLockManager>>,
    lifecycle_locks: WorkspaceExecutionLockManager,
    sweep_cursor: Mutex<SweepCursor>,
    gc_settings: RwLock<GcSettings>,
    /// What the last ownership check of the root found, for the disk reading.
    gc_state: RwLock<Option<&'static str>>,
    /// When garbage was last collected because the disk was short.
    last_reclaim: std::sync::Mutex<Option<std::time::Instant>>,
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
            gc_settings: RwLock::new(GcSettings::default()),
            gc_state: RwLock::new(None),
            last_reclaim: std::sync::Mutex::new(None),
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

    /// [`Self::lock_task`] for the garbage collector, which must never wait
    /// for long: a pass can run inside an admission (a claim that reclaims
    /// before it is refused for disk), and whoever holds this Task's lock
    /// may be that very claim, or a reopen waiting on the admission. A Task
    /// whose lock is not free within [`GC_LOCK_WAIT`] is busy; the pass
    /// skips it and the next one finds it again.
    pub(crate) async fn lock_task_for_gc(&self, task: &db::Task) -> Option<OwnedMutexGuard<()>> {
        tokio::time::timeout(GC_LOCK_WAIT, self.lock_task(task))
            .await
            .ok()
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
        // Short of disk: collect now instead of waiting for the sweep timer.
        self.reclaim_under_pressure().await;
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

    /// One periodic pass: the terminal-Task and repository backfill, then
    /// the garbage collection of the workspace root, all inside
    /// [`SWEEP_BUDGET`]. Both resume from the shared cursor.
    pub(crate) async fn sweep(&self) -> Result<()> {
        let mut cursor = self.sweep_cursor.lock().await;
        let started = Instant::now();
        let backfill = self.sweep_backfill(&mut cursor, started).await;
        let left = SWEEP_BUDGET.saturating_sub(started.elapsed());
        self.gc_pass(&mut cursor, left).await;
        backfill
    }

    async fn sweep_backfill(&self, cursor: &mut SweepCursor, started: Instant) -> Result<()> {
        let task_ids =
            sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE id > ? ORDER BY id LIMIT ?")
                .bind(&cursor.task_id)
                .bind(SWEEP_LIMIT)
                .fetch_all(self.db.pool())
                .await?;
        for task_id in &task_ids {
            cursor.task_id = task_id.clone();
            if let Err(error) = self.cleanup_terminal_task(task_id).await {
                tracing::warn!(%task_id, %error, "terminal Task workspace backfill failed");
            }
            if started.elapsed() >= BACKFILL_BUDGET {
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
                match timeout(BACKFILL_BUDGET.saturating_sub(started.elapsed()), async {
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
            if started.elapsed() >= BACKFILL_BUDGET {
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
        // A deleted Task never runs again, whatever state it was deleted in:
        // its directories are reclaimed like a terminal Task's. Deletion
        // itself stops nothing, so a run still in flight is waited for below
        // exactly as for a terminal Task.
        if terminal_only
            && task.deleted_at.is_none()
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
        // A deleted Task can still have work in its worktree that no
        // execution row or lease shows: a dispatched check run (entry or
        // review CI; deleting cancels its consumers, the run itself goes on
        // to its deadline) and any hook, check or tool command of this
        // process. Neither is stopped by deletion; both end by themselves
        // (a check by its wall limit, at most a day), and the cleanup comes
        // due again every tick until they have.
        let checking = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                WHERE c.task_id = ?
                  AND r.state IN ('running', 'cancelling', 'cleaning', 'uncertain')
             )",
        )
        .bind(task_id)
        .fetch_one(self.db.pool())
        .await?;
        // A server-owned Task root is `<workspace root>/<root Task id>`.
        let task_root = self
            .workspace_root
            .join(task.parent_task_id.as_deref().unwrap_or(&task.id));
        let live_run = executors::sandbox::has_live_run_in(&task_root)
            || std::fs::canonicalize(&task_root)
                .is_ok_and(|resolved| executors::sandbox::has_live_run_in(&resolved));
        if checking != 0 || live_run {
            info!(
                task_id,
                checking = checking != 0,
                live_run,
                "deferring Task cleanup while a check or a run of this server is live in its worktree"
            );
            if let Some(workspace) = workspace.as_ref() {
                if workspace.status != WorkspaceStatus::Cleaned {
                    self.schedule(&workspace.id, TICK_INTERVAL).await?;
                }
            }
            return Ok(());
        }
        let cleanup_result = if let Some(workspace) = workspace {
            let children = TaskRepo::list_subtasks_ordered(&*self.db, task_id).await?;
            if children.iter().any(|child| {
                child.deleted_at.is_none()
                    && !crate::task_hierarchy::subtask_is_terminal(child, &workflow)
            }) {
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
            // The Task is terminal and nothing runs for it: its logs are
            // history, kept for the configured retention.
            self.expire_task_logs(&task).await;
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
        // While the worktree can still reach its repository: the refs that
        // kept off-branch commits for this Task go with its workspace.
        crate::workspace_manager::delete_rescued_refs(&self.workspace_root, &workspace, &resolved)
            .await;
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

    /// A workspace whose source is the user's own checkout at
    /// `<root>/vol/user-repo`, recorded on the Repo, with Forge holding no
    /// clone. `vol` stands for the volume the checkout lives on.
    async fn user_repo_fixture(db: &Arc<SqliteDb>, root: &Path) -> (String, PathBuf, PathBuf) {
        let (workspace_id, worktree_path) = seed_workspace(db, root, WorkspaceStatus::Ready).await;
        let (cache, _branch) = fixture_source(db, root, &workspace_id).await;
        let workspace = WorkspaceRepo::get_by_id(&**db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        let user_repo = root.join("vol").join("user-repo");
        std::fs::create_dir_all(user_repo.parent().unwrap()).unwrap();
        std::fs::rename(&cache, &user_repo).unwrap();
        fixture_git(
            &user_repo,
            &["worktree", "repair", &worktree_path.to_string_lossy()],
        )
        .await;
        sqlx::query("UPDATE repo SET local_path = ? WHERE id = ?")
            .bind(user_repo.to_string_lossy().as_ref())
            .bind(&workspace.repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        (workspace_id, worktree_path, user_repo)
    }

    async fn worktree_registered(repo: &Path, worktree: &Path) -> bool {
        fixture_git(repo, &["worktree", "list", "--porcelain"])
            .await
            .contains(worktree.to_string_lossy().as_ref())
    }

    /// A user's repository that is away right now (an unmounted volume) is
    /// not a repository that is gone: cleanup keeps the worktree, retries, and
    /// removes the registration from the user's repository once it is back.
    #[tokio::test]
    async fn recorded_repository_missing_right_now_is_retried_not_treated_as_gone() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path, user_repo) = user_repo_fixture(&db, temp.path()).await;
        assert!(worktree_registered(&user_repo, &worktree_path).await);
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        // The volume is unmounted: the repository's parent is gone with it.
        let volume = temp.path().join("vol");
        let away = temp.path().join("vol.unmounted");
        std::fs::rename(&volume, &away).unwrap();
        for attempt in 1..=CLEANUP_ATTENTION_ATTEMPTS {
            // An unmounted mount point may also be left behind as an empty
            // directory: that is still "away".
            if attempt == 2 {
                std::fs::create_dir(&volume).unwrap();
            }
            clean_due(&scheduler, &workspace_id).await;
            let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(row.status, WorkspaceStatus::Cleaned, "attempt {attempt}");
            assert_eq!(row.cleanup_attempts, attempt);
            assert!(row
                .last_cleanup_error
                .as_deref()
                .unwrap()
                .contains("is not reachable right now"));
            assert!(worktree_path.exists(), "the worktree is kept for the retry");
            assert_eq!(
                cleanup_attention(&db, &workspace_id).await.len(),
                usize::from(attempt == CLEANUP_ATTENTION_ATTEMPTS),
                "attention is raised at the threshold, attempt {attempt}"
            );
        }

        std::fs::remove_dir(&volume).unwrap();
        std::fs::rename(&away, &volume).unwrap();
        clean_due(&scheduler, &workspace_id).await;
        let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, WorkspaceStatus::Cleaned);
        assert!(!worktree_path.parent().unwrap().exists());
        assert!(
            !worktree_registered(&user_repo, &worktree_path).await,
            "the registration is removed from the user's repository"
        );
        assert_eq!(cleanup_attention(&db, &workspace_id).await[0].0, "resolved");
    }

    /// A checkout deleted for good, in a directory that is still there, took
    /// its worktree registrations with it: cleanup removes the Task root at
    /// once instead of retrying for a repository that will never come back.
    #[tokio::test]
    async fn recorded_repository_deleted_for_good_is_cleaned_without_retries() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path, user_repo) = user_repo_fixture(&db, temp.path()).await;
        std::fs::write(temp.path().join("vol").join("other-project"), "still here").unwrap();
        std::fs::remove_dir_all(&user_repo).unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );

        clean_due(&scheduler, &workspace_id).await;

        let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, WorkspaceStatus::Cleaned);
        assert_eq!(row.cleanup_attempts, 0);
        assert!(!worktree_path.parent().unwrap().exists());
        assert!(cleanup_attention(&db, &workspace_id).await.is_empty());
    }

    /// Waiting for a repository that is away is bounded: a set time after the
    /// attention item was raised, the Task root is removed without it and the
    /// item records why.
    #[tokio::test]
    async fn recorded_repository_that_stays_away_stops_being_waited_for() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path, _user_repo) = user_repo_fixture(&db, temp.path()).await;
        std::fs::rename(temp.path().join("vol"), temp.path().join("vol.unmounted")).unwrap();
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        for _ in 0..CLEANUP_ATTENTION_ATTEMPTS + 2 {
            clean_due(&scheduler, &workspace_id).await;
        }
        assert!(worktree_path.exists(), "still waiting inside the bound");
        assert_eq!(cleanup_attention(&db, &workspace_id).await[0].0, "open");

        let raised = (chrono::Utc::now() - chrono::Duration::days(8)).to_rfc3339();
        sqlx::query("UPDATE attention_projection SET occurred_at = ? WHERE dedupe_key = ?")
            .bind(raised)
            .bind(cleanup_attention_key(&workspace_id))
            .execute(db.pool())
            .await
            .unwrap();
        clean_due(&scheduler, &workspace_id).await;

        let row = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, WorkspaceStatus::Cleaned);
        assert!(!worktree_path.parent().unwrap().exists());
        let (status, settled) = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT status, json_extract(details_json, '$.settled')
             FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(cleanup_attention_key(&workspace_id))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(status, "resolved");
        assert!(
            settled
                .as_deref()
                .is_some_and(|settled| settled.contains("stayed unreachable for 7 days")),
            "{settled:?}"
        );
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

    const GC_TEST_BUDGET: Duration = Duration::from_secs(30);
    const HOUR: Duration = Duration::from_secs(60 * 60);

    /// A scheduler on `root` after the start-up step a running server does:
    /// the root is adopted for this database.
    async fn gc_scheduler(db: &Arc<SqliteDb>, root: &Path) -> WorkspaceCleanupScheduler {
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(db),
            Arc::new(EventBus::new(16)),
            root.to_path_buf(),
        );
        scheduler.adopt_workspace_root().await.unwrap();
        scheduler
    }

    /// An unrecorded Task root that Forge made (it carries `.forge-task`).
    fn forge_made_root(root: &Path) -> PathBuf {
        let task_root = root.join(new_uuid_v4());
        std::fs::create_dir_all(task_root.join(".forge-task")).unwrap();
        std::fs::create_dir_all(task_root.join("repo")).unwrap();
        std::fs::write(task_root.join("repo/file"), "content").unwrap();
        task_root
    }

    /// Make `path` and what is directly inside it look `days` old.
    fn backdate(path: &Path, days: u64) {
        let then = std::time::SystemTime::now() - Duration::from_secs(days * 24 * 60 * 60);
        let mut paths = vec![path.to_path_buf()];
        paths.extend(
            std::fs::read_dir(path)
                .unwrap()
                .flatten()
                .map(|entry| entry.path()),
        );
        for path in paths.iter().rev() {
            std::fs::File::open(path)
                .unwrap()
                .set_modified(then)
                .unwrap();
        }
    }

    #[tokio::test]
    async fn gc_does_nothing_on_a_root_no_running_server_adopted() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let orphan = forge_made_root(&root);
        // Built the way every test and tool builds one: nothing adopts.
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            root.clone(),
        );
        let report = gc_at(&scheduler, 30 * 24 * HOUR).await;
        assert_eq!(report, executors::gc::GcReport::default());
        scheduler.sweep().await.unwrap();
        assert!(orphan.exists());
        // Not even the marker is written.
        assert!(!root.join(".forge").exists());
    }

    #[tokio::test]
    async fn gc_refuses_a_root_that_is_a_repository_and_reports_it() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        let orphan = forge_made_root(&root);
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            root.clone(),
        );
        assert!(matches!(
            scheduler.adopt_workspace_root().await.unwrap(),
            executors::gc::Ownership::Refused(_)
        ));
        assert_eq!(
            gc_at(&scheduler, 30 * 24 * HOUR).await,
            executors::gc::GcReport::default()
        );
        assert!(orphan.exists() && !root.join(".forge").exists());
        // The operator can see why nothing is reclaimed.
        let status = sqlx::query_scalar::<_, String>(
            "SELECT value FROM system_setting WHERE key = 'workspace_gc_status'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert!(status.contains("\"state\":\"refused\"") && status.contains("git repository"));
    }

    #[tokio::test]
    async fn gc_leaves_a_task_root_whose_create_is_in_flight_however_old() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let slow = forge_made_root(&root);
        let scheduler = gc_scheduler(&db, &root).await;
        // A worktree create that has been running for a month.
        let creating = workspace::creating(&slow);
        assert_eq!(
            gc_at(&scheduler, 30 * 24 * HOUR).await,
            executors::gc::GcReport::default()
        );
        assert!(slow.join("repo/file").exists());
        drop(creating);
        assert_eq!(gc_at(&scheduler, 30 * 24 * HOUR).await.quarantined, 1);
    }

    #[tokio::test]
    async fn gc_pages_through_more_roots_than_one_pass_takes() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let total = SWEEP_LIMIT as usize + 6;
        let orphans: Vec<PathBuf> = (0..total).map(|_| forge_made_root(&root)).collect();
        let scheduler = gc_scheduler(&db, &root).await;
        let mut cursor = SweepCursor::default();
        let later = std::time::SystemTime::now() + 25 * HOUR;

        let first = scheduler
            .gc_pass_at(&mut cursor, GC_TEST_BUDGET, later)
            .await;
        assert_eq!(first.quarantined, SWEEP_LIMIT as usize);
        assert!(!cursor.root_name.is_empty());
        let second = scheduler
            .gc_pass_at(&mut cursor, GC_TEST_BUDGET, later)
            .await;
        assert_eq!(second.quarantined, 6);
        assert!(cursor.root_name.is_empty());
        assert!(orphans.iter().all(|orphan| !orphan.exists()));

        // Out of time from the start: a pass still finishes one entry and
        // the cursor moves past it, so no entry can starve the ones after it.
        let more: Vec<PathBuf> = (0..3).map(|_| forge_made_root(&root)).collect();
        let mut names: Vec<String> = more
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        let starved = scheduler
            .gc_pass_at(&mut cursor, Duration::ZERO, later)
            .await;
        assert_eq!(starved.quarantined, 1);
        assert_eq!(cursor.root_name, names[0]);
        let next = scheduler
            .gc_pass_at(&mut cursor, Duration::ZERO, later)
            .await;
        assert_eq!(next.quarantined, 1);
        assert_eq!(cursor.root_name, names[1]);
    }

    #[tokio::test]
    async fn gc_evicts_build_output_under_the_floor_but_never_of_a_busy_task() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let mut builds = Vec::new();
        let mut hook = None;
        for kind in ["idle", "running", "hook", "done"] {
            let (workspace_id, worktree_path) =
                seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
            let task = fixture_task(&db, &workspace_id).await;
            let task =
                set_fixture_status(&db, &task, if kind == "done" { "done" } else { "todo" }).await;
            let task_root = worktree_path.parent().unwrap().to_path_buf();
            executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
            let build = task_root.join(".forge-task/build");
            std::fs::create_dir_all(build.join("cargo")).unwrap();
            std::fs::write(build.join("cargo/marker"), "x").unwrap();
            match kind {
                "running" => {
                    fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running)
                        .await;
                }
                "hook" => {
                    hook = Some(executors::sandbox::SandboxEnv::for_command(
                        &worktree_path,
                        executors::sandbox::RunPurpose::Hook,
                    ));
                }
                _ => {}
            }
            builds.push(build);
        }
        let hook = hook.unwrap();
        let scheduler = gc_scheduler(&db, &root).await;
        let under = executors::gc::FreeFloor::of_bytes(u64::MAX, 100);

        // Plenty of room: nothing is evicted.
        scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(0, 0));
        assert_eq!(gc_at(&scheduler, Duration::ZERO).await.builds_evicted, 0);

        // Under the floor while a check runs somewhere: nothing is evicted.
        scheduler.set_gc_limits(30, under);
        scheduler.set_live_check_counter(Arc::new(|| 1));
        assert_eq!(gc_at(&scheduler, Duration::ZERO).await.builds_evicted, 0);
        assert!(builds
            .iter()
            .all(|build| build.join("cargo/marker").exists()));

        // Under the floor: only the idle, non-terminal Task loses its build.
        scheduler.set_live_check_counter(Arc::new(|| 0));
        let report = gc_at(&scheduler, Duration::ZERO).await;
        assert_eq!((report.builds_evicted, report.errors), (1, 0));
        assert!(!builds[0].exists());
        assert!(builds[1].join("cargo/marker").exists());
        if hook.env().tmp_dir().is_some() {
            assert!(builds[2].join("cargo/marker").exists());
        }
        assert!(builds[3].join("cargo/marker").exists());
    }

    /// Plan 3.4 F: under the floor the server's collector trims the shared
    /// compiler cache, also while a check runs (an sccache store tolerates
    /// it), and every pass measures the cache for the server's disk facts.
    #[tokio::test]
    async fn gc_trims_the_shared_compiler_cache_under_the_floor_and_measures_it() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let store = root
            .join(executors::compiler_cache::CACHE_DIR)
            .join("repo-a");
        std::fs::create_dir_all(store.join("0")).unwrap();
        std::fs::write(
            store.join(executors::compiler_cache::MARKER_FILE),
            "sccache",
        )
        .unwrap();
        let entry = store.join("0").join("entry");
        std::fs::write(&entry, vec![0_u8; 4096]).unwrap();
        let scheduler = gc_scheduler(&db, &root).await;

        scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(0, 0));
        let report = gc_at(&scheduler, Duration::ZERO).await;
        assert_eq!(report.cache_entries_evicted, 0);
        assert!(entry.exists());
        assert!(executors::compiler_cache::measured_bytes(&root).is_some_and(|bytes| bytes >= 4096));

        scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(u64::MAX, 100));
        scheduler.set_live_check_counter(Arc::new(|| 1));
        let report = gc_at(&scheduler, Duration::ZERO).await;
        assert_eq!((report.cache_entries_evicted, report.errors), (1, 0));
        assert!(!entry.exists() && store.join("0").is_dir());
    }

    async fn gc_at(
        scheduler: &WorkspaceCleanupScheduler,
        after: Duration,
    ) -> executors::gc::GcReport {
        scheduler
            .gc_pass_at(
                &mut SweepCursor::default(),
                GC_TEST_BUDGET,
                std::time::SystemTime::now() + after,
            )
            .await
    }

    fn quarantined(root: &Path) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(root.join(executors::gc::GC_DIR)) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| uuid::Uuid::parse_str(name.get(..36).unwrap_or_default()).is_ok())
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn gc_quarantines_unknown_task_roots_then_deletes_them_a_day_later() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "todo").await;
        // A create in progress: the Task exists, its workspace row does not yet.
        let creating =
            seed_log_only_task(&db, &root, &task, new_uuid_v4(), None, "in_progress").await;
        let orphan = forge_made_root(&root);
        // A directory with a Task-shaped name that a user made.
        let users = root.join(new_uuid_v4());
        let kept = [
            root.join(&creating.id).join("repo"),
            root.join(&task.project_id).join("checkout"),
            root.join("main-agents").join("agent"),
            root.join(".forge").join("probes"),
            root.join("notes").join("mine"),
            users.join("photos"),
            orphan.join("repo"),
        ];
        for path in &kept {
            std::fs::create_dir_all(path).unwrap();
            std::fs::write(path.join("file"), "content").unwrap();
        }
        let scheduler = gc_scheduler(&db, &root).await;

        // First sight of a directory made a moment ago: nothing moves.
        assert_eq!(gc_at(&scheduler, Duration::ZERO).await.quarantined, 0);
        assert!(orphan.exists());

        // Eleven hours old: a slow clone may still be filling it.
        assert_eq!(gc_at(&scheduler, 11 * HOUR).await.quarantined, 0);
        assert!(orphan.exists());

        // Older than the creation grace: only the unknown Task root is moved
        // aside, and it is not deleted.
        let report = gc_at(&scheduler, 25 * HOUR).await;
        assert_eq!(
            (report.quarantined, report.removed, report.errors),
            (1, 0, 0)
        );
        assert!(!orphan.exists());
        let held = quarantined(&root);
        assert_eq!(held.len(), 1);
        assert_eq!(
            std::fs::read_to_string(
                root.join(executors::gc::GC_DIR)
                    .join(&held[0])
                    .join("repo/file")
            )
            .unwrap(),
            "content"
        );
        for path in &kept[..6] {
            assert!(path.join("file").exists(), "{} was touched", path.display());
        }
        assert!(worktree_path.exists());
        assert!(root.join(".repos").exists());

        // Still held 23 hours after it was quarantined; gone after 24.
        assert_eq!(gc_at(&scheduler, 48 * HOUR).await.removed, 0);
        assert_eq!(quarantined(&root).len(), 1);
        assert_eq!(gc_at(&scheduler, 50 * HOUR).await.removed, 1);
        assert!(quarantined(&root).is_empty());
        for path in &kept[..6] {
            assert!(path.join("file").exists(), "{} was touched", path.display());
        }
        assert!(worktree_path.exists());
    }

    #[tokio::test]
    async fn gc_restores_a_quarantined_root_whose_task_appears() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let late = forge_made_root(&root);
        let late_id = late.file_name().unwrap().to_string_lossy().into_owned();
        let scheduler = gc_scheduler(&db, &root).await;
        assert_eq!(gc_at(&scheduler, 25 * HOUR).await.quarantined, 1);
        assert!(!late.exists());

        // Its record shows up while it sits in quarantine.
        seed_log_only_task(&db, &root, &task, late_id, None, "in_progress").await;
        let report = gc_at(&scheduler, 30 * 24 * HOUR).await;
        assert_eq!((report.restored, report.removed), (1, 0));
        assert_eq!(
            std::fs::read_to_string(late.join("repo/file")).unwrap(),
            "content"
        );
        assert!(quarantined(&root).is_empty());
    }

    #[tokio::test]
    async fn gc_never_sweeps_a_root_another_database_claimed() {
        let first = sqlite_db().await;
        let second = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        // A live Task root of the first server, unknown to the second.
        let (_, worktree_path) = seed_workspace(&first, &root, WorkspaceStatus::Ready).await;
        let owner = gc_scheduler(&first, &root).await;
        assert_eq!(
            gc_at(&owner, HOUR).await,
            executors::gc::GcReport::default()
        );

        // A second server on the same root with another database: adopting
        // changes nothing, and it never sweeps.
        let stranger = WorkspaceCleanupScheduler::new(
            Arc::clone(&second),
            Arc::new(EventBus::new(16)),
            root.clone(),
        );
        assert_eq!(
            stranger.adopt_workspace_root().await.unwrap(),
            executors::gc::Ownership::Other
        );
        let report = gc_at(&stranger, 30 * 24 * HOUR).await;
        assert_eq!(report, executors::gc::GcReport::default());
        stranger.sweep().await.unwrap();
        let status = sqlx::query_scalar::<_, String>(
            "SELECT value FROM system_setting WHERE key = 'workspace_gc_status'",
        )
        .fetch_one(second.pool())
        .await
        .unwrap();
        assert!(status.contains("claimed_by_other"), "{status}");
        // The owner is still the owner.
        assert_eq!(
            owner.adopt_workspace_root().await.unwrap(),
            executors::gc::Ownership::Mine
        );
        assert!(worktree_path.join("node_modules/build-output").exists());
        assert!(quarantined(&root).is_empty());
    }

    #[tokio::test]
    async fn gc_removes_leftovers_of_a_cleaned_terminal_task_only() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let scheduler = gc_scheduler(&db, &root).await;
        let mut roots = Vec::new();
        for _ in 0..2 {
            let (workspace_id, worktree_path) =
                seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
            clean_due(&scheduler, &workspace_id).await;
            let task_root = worktree_path.parent().unwrap().to_path_buf();
            assert!(!task_root.exists());
            // Something wrote into the Task root after it was reclaimed.
            std::fs::create_dir_all(task_root.join(".forge-outbox/late")).unwrap();
            std::fs::write(task_root.join(".forge-outbox/late/result.json"), "{}").unwrap();
            roots.push((workspace_id, task_root));
        }
        // The second Task was reopened: its root is about to be used again.
        let reopened = fixture_task(&db, &roots[1].0).await;
        set_fixture_status(&db, &reopened, "todo").await;

        let report = gc_at(&scheduler, Duration::ZERO).await;
        assert_eq!((report.removed, report.errors), (1, 0));
        assert!(!roots[0].1.exists());
        assert!(roots[1].1.join(".forge-outbox/late/result.json").exists());
    }

    #[tokio::test]
    async fn gc_keeps_broken_copies_seven_days_and_live_run_directories_always() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        set_fixture_status(&db, &task, "todo").await;
        let task_root = worktree_path.parent().unwrap().to_path_buf();
        executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
        let millis = chrono::Utc::now().timestamp_millis();
        let broken = task_root.join(format!("repo.broken-{millis}"));
        std::fs::create_dir_all(broken.join("src")).unwrap();
        let hook = executors::sandbox::SandboxEnv::for_command(
            &worktree_path,
            executors::sandbox::RunPurpose::Hook,
        );
        let scheduler = gc_scheduler(&db, &root).await;

        gc_at(&scheduler, 6 * 24 * HOUR).await;
        assert!(broken.exists());
        let report = gc_at(&scheduler, 8 * 24 * HOUR).await;
        assert_eq!((report.removed, report.run_dirs_removed), (1, 0));
        assert!(!broken.exists());
        assert!(worktree_path.join("node_modules/build-output").exists());
        if let Some(tmp) = hook.env().tmp_dir() {
            assert!(tmp.exists(), "a live hook lost its temp directory");
        }
    }

    #[tokio::test]
    async fn gc_measures_live_task_roots() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        std::fs::write(worktree_path.join("blob"), vec![7_u8; 256 * 1024]).unwrap();
        let before = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((before.disk_bytes, before.disk_measured_at), (None, None));

        gc_at(&gc_scheduler(&db, &root).await, Duration::ZERO).await;

        let measured = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert!(measured.disk_bytes.is_some_and(|bytes| bytes >= 256 * 1024));
        assert!(measured.disk_measured_at.is_some());
        assert_eq!(measured.updated_at, before.updated_at);
    }

    #[tokio::test]
    async fn sweep_deletes_logs_of_terminal_tasks_after_the_retention() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
        let base = fixture_task(&db, &workspace_id).await;
        let mut tasks = Vec::new();
        for (status, days) in [("done", 31), ("done", 29), ("in_progress", 400)] {
            let task = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, status).await;
            sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
                .bind((chrono::Utc::now() - chrono::Duration::days(days)).to_rfc3339())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            backdate(&logs_dir(&root, &task), 60);
            tasks.push(task);
        }
        // Terminal for 31 days by its record, but its logs were written
        // inside the retention (a Task imported or restored): kept.
        let recent = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
        sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339())
            .bind(&recent.id)
            .execute(db.pool())
            .await
            .unwrap();
        // The record says the future (a skewed clock): kept.
        let skewed = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
        sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
            .bind((chrono::Utc::now() + chrono::Duration::days(400)).to_rfc3339())
            .bind(&skewed.id)
            .execute(db.pool())
            .await
            .unwrap();
        backdate(&logs_dir(&root, &skewed), 60);
        let scheduler = gc_scheduler(&db, &root).await;

        // `0` keeps every log.
        scheduler.set_gc_limits(0, executors::gc::FreeFloor::default());
        scheduler.sweep().await.unwrap();
        assert!(logs_dir(&root, &tasks[0]).join("execution.jsonl").exists());

        // That sweep removed the legacy agent home from each log directory,
        // which is a write to it: age them again.
        for task in tasks.iter().chain([&skewed]) {
            backdate(&logs_dir(&root, task), 60);
        }
        scheduler.set_gc_limits(30, executors::gc::FreeFloor::default());
        scheduler.sweep().await.unwrap();
        assert!(!logs_dir(&root, &tasks[0]).exists());
        assert!(logs_dir(&root, &tasks[1]).join("execution.jsonl").exists());
        assert!(logs_dir(&root, &tasks[2]).join("execution.jsonl").exists());
        assert!(logs_dir(&root, &recent).join("execution.jsonl").exists());
        assert!(logs_dir(&root, &skewed).join("execution.jsonl").exists());
    }

    #[tokio::test]
    async fn gc_eviction_takes_the_task_lifecycle_lock_and_reads_the_task_again() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, &root, WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "todo").await;
        let task_root = worktree_path.parent().unwrap().to_path_buf();
        executors::sandbox::TaskRoot::reserve(&task_root).unwrap();
        let marker = task_root.join(".forge-task/build/cargo/marker");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "x").unwrap();
        let scheduler = Arc::new(gc_scheduler(&db, &root).await);
        scheduler.set_gc_limits(30, executors::gc::FreeFloor::of_bytes(u64::MAX, 100));

        // The pass saw an idle Task. Something holds the Task's lifecycle
        // lock; a run starts before the pass gets its turn.
        let guard = scheduler.lock_task(&task).await;
        let pass = tokio::spawn({
            let scheduler = Arc::clone(&scheduler);
            async move { gc_at(&scheduler, Duration::ZERO).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!pass.is_finished(), "eviction waits for the lifecycle lock");
        assert!(marker.exists());
        let run = fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
        drop(guard);
        let report = pass.await.unwrap();
        assert_eq!((report.builds_evicted, report.errors), (0, 0));
        assert!(
            marker.exists(),
            "the run that started keeps its build output"
        );

        // Once the run is over the same Task is evicted.
        sqlx::query("UPDATE execution SET status = 'failed' WHERE id = ?")
            .bind(&run.id)
            .execute(db.pool())
            .await
            .unwrap();
        // A lock that stays held (the claim that asked for this very pass,
        // a reopen in flight) never hangs the pass: the Task is skipped, and
        // the pass does not run out its budget waiting.
        let guard = scheduler.lock_task(&task).await;
        let started = std::time::Instant::now();
        let report =
            tokio::time::timeout(Duration::from_secs(10), gc_at(&scheduler, Duration::ZERO))
                .await
                .expect("a held lifecycle lock does not hang the pass");
        assert_eq!((report.builds_evicted, report.errors), (0, 0));
        assert!(started.elapsed() >= GC_LOCK_WAIT);
        assert!(marker.exists());
        drop(guard);

        let report = gc_at(&scheduler, Duration::ZERO).await;
        assert_eq!((report.builds_evicted, report.errors), (1, 0));
        assert!(!marker.exists());
        assert!(worktree_path.exists(), "only the build output goes");
    }

    /// A burst of admissions on a short disk runs one collection, and the
    /// rest neither run their own nor wait behind it.
    #[tokio::test]
    async fn a_burst_of_admissions_under_the_floor_runs_one_reclaim_and_nobody_queues() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let scheduler = Arc::new(gc_scheduler(&db, &root).await);
        let free = Arc::new(std::sync::atomic::AtomicU64::new(150));
        let reading = Arc::clone(&free);
        db.disk_admission.configure(
            api_types::DiskFloor::of_bytes(100, 0),
            Arc::new(move || {
                Some(api_types::MachineDiskFacts {
                    free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                    total_bytes: 1_000,
                    free_inodes: None,
                    total_inodes: None,
                    measured_at: now_rfc3339(),
                    gc_state: Some("owned".to_owned()),
                    compiler_cache_bytes: None,
                })
            }),
        );
        // Over the floor but under the collector mark: an admission would
        // not be refused, so it does not collect (the cleanup tick does).
        assert!(!scheduler.reclaim_before_refusing().await);
        // Under the floor: fifty admissions at once.
        free.store(10, std::sync::atomic::Ordering::SeqCst);
        db.disk_admission.refresh();
        let started = std::time::Instant::now();
        let mut burst = tokio::task::JoinSet::new();
        for _ in 0..50 {
            let scheduler = Arc::clone(&scheduler);
            burst.spawn(async move { scheduler.reclaim_before_refusing().await });
        }
        let mut ran = 0;
        while let Some(result) = burst.join_next().await {
            ran += usize::from(result.unwrap());
        }
        assert_eq!(ran, 1, "one pass for the whole burst");
        assert!(started.elapsed() < Duration::from_secs(15));
        // The same minute: nothing more, from an admission or from the tick.
        assert!(!scheduler.reclaim_before_refusing().await);
        assert!(!scheduler.reclaim_under_pressure().await);
    }

    /// Before the server is counted as short of disk its garbage is
    /// collected once and its disk read again: reclaimable trash present
    /// means the machine is admitted after the pass.
    #[tokio::test]
    async fn disk_pressure_collects_garbage_once_then_reads_the_disk_again() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let scheduler = gc_scheduler(&db, &root).await;
        let trash = root.join(executors::gc::TRASH_DIR);
        std::fs::create_dir_all(trash.join("condemned-1-0/target")).unwrap();
        std::fs::write(trash.join("condemned-1-0/target/big"), "x").unwrap();
        // The injected disk: short exactly while the trash holds something.
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (counted, watched) = (Arc::clone(&reads), trash.clone());
        db.disk_admission.configure(
            api_types::DiskFloor::of_bytes(100, 0),
            Arc::new(move || {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let full =
                    std::fs::read_dir(&watched).is_ok_and(|mut entries| entries.next().is_some());
                Some(api_types::MachineDiskFacts {
                    free_bytes: if full { 10 } else { 900 },
                    total_bytes: 1_000,
                    free_inodes: None,
                    total_inodes: None,
                    measured_at: now_rfc3339(),
                    gc_state: Some("owned".to_owned()),
                    compiler_cache_bytes: None,
                })
            }),
        );
        assert_eq!(
            db.disk_admission.server_pressure(),
            Some(api_types::DiskPressureKind::Bytes)
        );
        assert!(scheduler.reclaim_under_pressure().await, "a pass ran");
        assert!(!trash.join("condemned-1-0").exists());
        assert_eq!(
            db.disk_admission.server_pressure(),
            None,
            "read again after the pass"
        );
        // Asked again at once: no second pass, whatever the disk says.
        std::fs::create_dir_all(trash.join("condemned-2-0")).unwrap();
        db.disk_admission.refresh();
        assert!(!scheduler.reclaim_under_pressure().await);
        assert!(trash.join("condemned-2-0").exists());
        // With room to spare nothing is collected ahead of the timer.
        let unconfigured = sqlite_db().await;
        let idle = gc_scheduler(&unconfigured, &root).await;
        assert!(!idle.reclaim_under_pressure().await);
        assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn deleted_task_is_reclaimed_like_a_terminal_one_once_its_run_has_stopped() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "todo").await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        let status = || async {
            WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .status
        };

        // Not terminal and not deleted: untouched.
        scheduler.cleanup_terminal_task(&task.id).await.unwrap();
        assert!(worktree_path.exists());
        assert_eq!(status().await, WorkspaceStatus::Ready);

        // Deleting stops nothing by itself: a run still in flight keeps the
        // directory until it has stopped.
        let run = fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
        let now = now_rfc3339();
        TaskRepo::soft_delete(
            &*db,
            db::SoftDeleteTask {
                id: task.id.clone(),
                expected_version: task.version,
                deleted_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("Task is deleted");
        scheduler.cleanup_terminal_task(&task.id).await.unwrap();
        assert!(worktree_path.exists(), "a running execution defers cleanup");
        assert_eq!(status().await, WorkspaceStatus::Ready);

        // The run stops; a lease on the workspace still holds it.
        sqlx::query("UPDATE execution SET status = 'cancelled' WHERE id = ?")
            .bind(&run.id)
            .execute(db.pool())
            .await
            .unwrap();
        let due = || async {
            sqlx::query("UPDATE workspace SET cleanup_after = ? WHERE id = ?")
                .bind(now_rfc3339())
                .bind(&workspace_id)
                .execute(db.pool())
                .await
                .unwrap();
        };
        // A hook, check or tool command of this server that is live in the
        // Task root (in no table at all) keeps it as well, with or without
        // a temp directory of its own.
        let live = executors::sandbox::SandboxEnv::for_run(
            &worktree_path,
            "deleted-task-hook",
            executors::sandbox::RunPurpose::Hook,
        )
        .without_tmp()
        .prepared();
        due().await;
        scheduler.tick().await.unwrap();
        assert!(worktree_path.exists(), "a live run defers cleanup");
        assert_eq!(status().await, WorkspaceStatus::Ready);
        assert!(
            WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .cleanup_after
                .is_some(),
            "and the cleanup comes due again"
        );
        live.settle();

        // Everything has stopped; the deferred cleanup comes due.
        due().await;
        scheduler.tick().await.unwrap();
        assert!(!worktree_path.exists());
        assert_eq!(status().await, WorkspaceStatus::Cleaned);
    }

    #[tokio::test]
    async fn log_retention_skips_a_task_with_an_open_review_or_attention_item() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
        let base = fixture_task(&db, &workspace_id).await;
        let mut tasks = Vec::new();
        for _ in 0..3 {
            let task = seed_log_only_task(&db, &root, &base, new_uuid_v4(), None, "done").await;
            sqlx::query("UPDATE task SET updated_at = ? WHERE id = ?")
                .bind((chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            tasks.push(task);
        }
        let (reviewed, flagged, plain) = (&tasks[0], &tasks[1], &tasks[2]);
        // A review a person has not decided yet.
        let execution =
            fixture_execution(&db, reviewed, &workspace_id, db::ExecutionStatus::Completed).await;
        let now = now_rfc3339();
        sqlx::query("INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, 1, 'awaiting_human', '[]', ?, ?, ?)")
            .bind(new_uuid_v4())
            .bind(&reviewed.id)
            .bind(&execution.id)
            .bind(&now)
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        // An attention item that points a person at the Task.
        let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
        let event = db::DomainEventRepo::append_event_in_tx(
            &*db,
            &mut transaction,
            &db::CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "task.needs_attention".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: flagged.id.clone(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "project".to_owned(),
                scope_id: flagged.project_id.clone(),
                correlation_id: flagged.id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
        db.insert_attention_in_tx(
            &mut transaction,
            db::CreateAttentionProjection {
                id: new_uuid_v4(),
                attention_type: "progress_warning".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: flagged.project_id.clone(),
                identity_id: None,
                source_event_id: event.id,
                priority: 60,
                status: "open".to_owned(),
                summary: "look at this Task".to_owned(),
                details_json: serde_json::json!({"task": {"id": flagged.id}}).to_string(),
                dedupe_key: format!("test:{}", flagged.id),
                occurred_at: now.clone(),
                updated_at: now.clone(),
                acknowledged_at: None,
                snoozed_until: None,
                resolved_at: None,
                updated_by_user_id: None,
                recommended_action: "inspect".to_owned(),
                source_sequence: Some(event.sequence),
            },
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let scheduler = gc_scheduler(&db, &root).await;
        scheduler.set_gc_limits(30, executors::gc::FreeFloor::default());
        let age = |tasks: &[db::Task]| {
            for task in tasks {
                if logs_dir(&root, task).exists() {
                    backdate(&logs_dir(&root, task), 60);
                }
            }
        };
        // The first sweep removes the legacy agent home from each log
        // directory, which is a write to it: age them, then sweep again.
        scheduler.sweep().await.unwrap();
        age(&tasks);
        scheduler.sweep().await.unwrap();
        assert!(!logs_dir(&root, plain).exists());
        assert!(logs_dir(&root, reviewed).join("execution.jsonl").exists());
        assert!(logs_dir(&root, flagged).join("execution.jsonl").exists());

        // Decided and resolved: both are ordinary old logs now.
        sqlx::query("UPDATE review SET status = 'passed' WHERE task_id = ?")
            .bind(&reviewed.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE attention_projection SET status = 'resolved' WHERE dedupe_key = ?")
            .bind(format!("test:{}", flagged.id))
            .execute(db.pool())
            .await
            .unwrap();
        age(&tasks);
        scheduler.sweep().await.unwrap();
        assert!(!logs_dir(&root, reviewed).exists());
        assert!(!logs_dir(&root, flagged).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn gc_removes_only_legacy_temp_locations_that_are_provably_its_own_and_long_untouched() {
        let db = sqlite_db().await;
        let temp = TempDir::new().unwrap();
        let base = temp.path().canonicalize().unwrap();
        let (root, legacy, outside) = (base.join("root"), base.join("tmp"), base.join("outside"));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(outside.join("user")).unwrap();
        // Two Tasks of this database; the third id belongs to another Forge
        // on the same machine.
        let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
        let mine = fixture_task(&db, &workspace_id).await;
        let (workspace_id, _) = seed_workspace(&db, &root, WorkspaceStatus::Cleaned).await;
        let linked = fixture_task(&db, &workspace_id).await;
        let (old_task, other_task, linked_task) =
            (mine.id.clone(), new_uuid_v4(), linked.id.clone());
        let logs = legacy.join("forge/logs");
        for path in [
            legacy.join("forge-gemini-api-key-home/.gemini"),
            logs.join(&old_task).join("hooks"),
            logs.join(&other_task).join("hooks"),
            logs.join("not-a-task").join("hooks"),
            legacy.join("forge/worktrees/someone"),
            legacy.join("unrelated"),
        ] {
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("file"), "content").unwrap();
        }
        // A planted link where a hook-log directory used to be.
        std::fs::create_dir_all(logs.join(&linked_task)).unwrap();
        std::os::unix::fs::symlink(&outside, logs.join(&linked_task).join("hooks")).unwrap();
        let scheduler = gc_scheduler(&db, &root).await;

        // Not told about a temp directory: nothing outside the root is read.
        gc_at(&scheduler, 30 * 24 * HOUR).await;
        assert!(legacy.join("forge-gemini-api-key-home").exists());

        // Touched within a week: another Forge on the machine may be using
        // it, so everything stays.
        scheduler.set_legacy_temp_dir(legacy.clone());
        gc_at(&scheduler, 6 * 24 * HOUR).await;
        assert!(legacy
            .join("forge-gemini-api-key-home/.gemini/file")
            .exists());
        assert!(logs.join(&old_task).join("hooks/file").exists());

        gc_at(&scheduler, 8 * 24 * HOUR).await;
        assert!(!legacy.join("forge-gemini-api-key-home").exists());
        assert!(!logs.join(&old_task).exists());
        // Not a Task of this database: not ours.
        assert!(logs.join(&other_task).join("hooks/file").exists());
        assert!(logs.join("not-a-task/hooks/file").exists());
        assert!(legacy.join("forge/worktrees/someone/file").exists());
        assert!(legacy.join("unrelated/file").exists());
        assert!(!logs.join(&linked_task).exists());
        assert!(outside.join("user").exists());
    }
}
