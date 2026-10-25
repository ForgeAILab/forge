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
pub use gc::{
    GcSettings, LiveCheckCounter, OWNER_KEY as GC_OWNER_KEY, STATUS_KEY as GC_STATUS_KEY,
};

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
        let merge_service = Arc::new(MergeService::new(Arc::clone(&db), workspace_root.clone()));
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
        let previous_attempts = self
            .db
            .workspace_cleanup_attempts_in_tx(&mut transaction, task_id)
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
        self.db
            .record_workspace_cleanup_failure_in_tx(
                &mut transaction,
                &db::RecordWorkspaceCleanupFailureInput {
                    increment: if owner_unreachable { 0_i64 } else { 1_i64 },
                    last_error: &last_cleanup_error,
                    cleanup_after: &(cleanup_after.to_rfc3339()),
                    updated_at: &(now_rfc3339()),
                    task_id,
                },
            )
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
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let Some(row) = self
            .db
            .workspace_cleanup_attention_in_tx(&mut transaction, task_id)
            .await?
        else {
            transaction.commit().await?;
            return Ok(());
        };
        let workspace_id: String = row.id.clone();
        let dedupe_key = cleanup_attention_key(&workspace_id);
        let open = self
            .db
            .cleanup_attention_is_open_in_tx(&mut transaction, &dedupe_key)
            .await?;
        if open != 0 {
            transaction.commit().await?;
            return Ok(());
        }
        let project_id: String = row.project_id.clone();
        let title: String = row.title.clone();
        let path: String = row.worktree_path.clone();
        let attempts: i64 = row.cleanup_attempts;
        let last_error: Option<String> = row.last_cleanup_error.clone();
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
        self.db.clear_workspace_cleanup_retry(&now, task_id).await?;
        // Nothing is left to reclaim for this Task, so nothing is left to report.
        self.db
            .resolve_task_workspace_cleanup_attention(&now, task_id)
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
        let task_ids = self
            .db
            .due_workspace_cleanup_task_ids(&(now_rfc3339()), SWEEP_LIMIT)
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
        let task_ids = self
            .db
            .workspace_cleanup_task_ids_after(&cursor.task_id, SWEEP_LIMIT)
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

        let repo_ids = self
            .db
            .workspace_cleanup_repo_ids_after(&cursor.repo_id, SWEEP_LIMIT)
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
        let active = self
            .db
            .workspace_cleanup_has_active_execution_or_lease(task_id, workspace_id)
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
        let checking = self.db.task_has_live_cleanup_check(task_id).await?;
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
        let shared = self
            .db
            .workspace_branch_is_shared(&workspace.repo_id, &workspace.branch, &workspace.id)
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
        self.db
            .mark_workspace_cleaning_in_tx(&mut transaction, &now, &placement.workspace_id)
            .await?;
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
            self.db
                .mark_workspace_cleaned_in_tx(&mut transaction, &now, &current.workspace_id)
                .await?;
            self.db
                .resolve_workspace_cleanup_attention_in_tx(
                    &mut transaction,
                    &now,
                    &cleanup_attention_key(&current.workspace_id),
                )
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
mod tests;
