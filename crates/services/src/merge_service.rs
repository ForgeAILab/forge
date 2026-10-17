use crate::{
    workspace_backend::{EmbeddedWorkspaceBackend, WorkspaceBackendRouter},
    Result, ServiceError,
};
use db::{
    now_rfc3339, ExecutionRepo, RepoRepo, SqliteDb, TaskRepo, WorkspacePlacementRepo, WorkspaceRepo,
};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock, Weak},
};
use tokio::process::Command;

/// Candidate admission and read-only delivery/review facts. Integration
/// effects belong to the per-target queue and its fenced owner ports.
///
/// The old direct merge API is deliberately absent:
/// ```compile_fail
/// fn old_merge(service: &services::MergeService) {
///     let _future = service.merge("task");
/// }
/// ```
#[derive(Clone)]
pub struct MergeService {
    db: Arc<SqliteDb>,
    workspace_root: PathBuf,
    integration_locks: Arc<workspace::RepoCacheLockManager>,
    workspace_backend_router: Arc<RwLock<Option<Weak<WorkspaceBackendRouter>>>>,
    test_workspace_backend: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MergeOutcome {
    ReviewRequired {
        reason: String,
    },
    /// The integration target moved while this Task was being reviewed.
    ///
    /// Distinct from [`MergeOutcome::ReviewRequired`] because it is not a
    /// fault: under any concurrency it is *guaranteed* whenever another Task
    /// merges first. Treating it as a merge failure spent the Task's single
    /// merge-fix retry on ordinary contention and blocked it dead on the
    /// second occurrence. The caller rebases onto the new target and asks for
    /// a fresh review instead of charging the budget.
    TargetMoved {
        reason: String,
        target_branch: String,
    },
    Done {
        before_sha: String,
        after_sha: String,
        branch: String,
    },
    /// Only an unreviewed-by-agent candidate (no review contract) takes the
    /// plain merge path that can produce this; the caller rebases it onto
    /// `target_branch` and hands any conflict back to the Worker.
    Conflict {
        details: String,
        conflict_paths: Vec<PathBuf>,
        target_branch: String,
    },
    Dirty {
        files: Vec<String>,
    },
    TargetDirty {
        files: Vec<String>,
    },
    /// A file handed to the Worker still adds Git conflict marker lines.
    UnresolvedConflictMarkers {
        paths: Vec<String>,
    },
}

/// The immutable object and delivery identity selected before a hook attempts
/// integration. On restart, target ancestry can prove completion independently
/// of later policy edits or sibling commits.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct MergeIntent {
    pub execution_id: String,
    pub workspace_id: String,
    pub candidate_sha: String,
    pub target_branch: String,
}

/// See [`MergeService::integration_admission_facts`].
pub(crate) struct IntegrationAdmissionFacts {
    pub execution_id: Option<String>,
    pub workspace: db::Workspace,
    pub placement: db::WorkspacePlacement,
    pub target_branch: String,
    pub candidate_sha: String,
}

/// Git facts a review-authority carry needs about the Task's current HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewCarryFacts {
    Ready {
        /// Worktree HEAD, a descendant of `base_sha`.
        commit_sha: String,
        /// Tip of the integration target the HEAD is built on.
        base_sha: String,
        /// Every path HEAD changes relative to `base_sha`, sorted.
        changed_paths: Vec<String>,
    },
    /// The candidate cannot be integrated without a fresh review.
    Unavailable { reason: String },
}

impl MergeService {
    pub fn new(db: Arc<SqliteDb>, workspace_root: PathBuf) -> Self {
        Self {
            db,
            workspace_root,
            integration_locks: Arc::new(workspace::RepoCacheLockManager::new()),
            workspace_backend_router: Arc::new(RwLock::new(None)),
            test_workspace_backend: false,
        }
    }

    /// Handed-off conflict files whose current `HEAD` still adds Git conflict
    /// markers relative to the target branch. Empty when the Task was never
    /// handed a conflict or has no workspace.
    /// Lets the workflow reject an unresolved repair before it is re-reviewed;
    /// Queue admission uses this guard before the worker can rebase the candidate.
    pub async fn unresolved_handoff_markers(&self, task_id: &str) -> Result<Vec<String>> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.to_owned(),
            })?;
        let Some(execution) =
            crate::task_service::latest_executor_execution_for_task(&self.db, &task).await?
        else {
            return Ok(Vec::new());
        };
        let workspace = resolve_delivery_workspace(&self.db, &execution).await?;
        let Some(repo) = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
        else {
            return Ok(Vec::new());
        };
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Server {
            let worktree_path = resolved.embedded_path()?;
            if !matches!(
                crate::task_service::workspace::worktree_readiness(&worktree_path).await?,
                crate::task_service::workspace::WorktreeReadiness::Ready
            ) {
                return Ok(Vec::new());
            }
        }
        let transitions = db::TransitionLogRepo::list_by_task(&*self.db, task_id).await?;
        let handed_off = crate::workflow::handed_off_conflict_paths(&transitions);
        let markers = resolved
            .git_query(
                api_types::WorkspaceGitQuery::MarkerPaths {
                    base: target_branch,
                    head: "HEAD".to_owned(),
                },
                false,
            )
            .await?
            .unwrap_or_default();
        Ok(markers
            .lines()
            .filter(|path| handed_off.iter().any(|handed| handed == *path))
            .map(str::to_owned)
            .collect())
    }

    /// Read-only Git facts for carrying a passed review across a mechanical
    /// rebase or conflict repair. Anything that is not a clean, marker-free
    /// descendant of the current target tip is `Unavailable`, so the caller
    /// falls back to a full review.
    pub async fn review_carry_facts(&self, task_id: &str) -> Result<ReviewCarryFacts> {
        let unavailable = |reason: &str| ReviewCarryFacts::Unavailable {
            reason: reason.to_owned(),
        };
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.to_owned(),
            })?;
        if task.parent_task_id.is_some() {
            return Ok(unavailable("subtasks do not integrate"));
        }
        let Some(execution) =
            crate::task_service::latest_executor_execution_for_task(&self.db, &task).await?
        else {
            return Ok(unavailable("no executor execution"));
        };
        let workspace = resolve_delivery_workspace(&self.db, &execution).await?;
        let Some(repo) = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
        else {
            return Ok(unavailable("repository not found"));
        };
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        if resolved.placement.owner_kind == db::PlacementOwnerKind::Server {
            let worktree_path = resolved.embedded_path()?;
            if !matches!(
                crate::task_service::workspace::worktree_readiness(&worktree_path).await?,
                crate::task_service::workspace::WorktreeReadiness::Ready
            ) {
                return Ok(unavailable("workspace is unavailable"));
            }
        }
        let worktree_path = &resolved;
        if resolved.backend.describe(&resolved.placement).await?.dirty {
            return Ok(unavailable("worktree has uncommitted changes"));
        }
        let commit_sha = ::review::contract::git_read(worktree_path, &["rev-parse", "HEAD"])
            .await
            .map_err(ServiceError::invalid_operation)?
            .trim()
            .to_owned();
        let Ok(base_sha) = ::review::contract::git_read(
            worktree_path,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{target_branch}"),
            ],
        )
        .await
        else {
            return Ok(unavailable("integration target is unreadable"));
        };
        let base_sha = base_sha.trim().to_owned();
        // The recorded base must be an ancestor of HEAD, or the later
        // fast-forward would integrate something other than what was checked.
        if ::review::contract::git_read(
            worktree_path,
            &["merge-base", "--is-ancestor", &base_sha, &commit_sha],
        )
        .await
        .is_err()
        {
            return Ok(unavailable("the candidate is not based on the target tip"));
        }
        if !self.unresolved_handoff_markers(task_id).await?.is_empty() {
            return Ok(unavailable("conflict markers remain in handed-off files"));
        }
        let Ok(changed_paths) =
            ::review::contract::candidate_changed_paths(worktree_path, &base_sha, &commit_sha)
                .await
        else {
            return Ok(unavailable("changed paths are unreadable"));
        };
        Ok(ReviewCarryFacts::Ready {
            commit_sha,
            base_sha,
            changed_paths,
        })
    }

    pub fn new_for_test(db: Arc<SqliteDb>, workspace_root: PathBuf) -> Self {
        Self {
            test_workspace_backend: true,
            ..Self::new(db, workspace_root)
        }
    }

    pub fn set_workspace_backend_router(&self, router: Arc<WorkspaceBackendRouter>) {
        match self.workspace_backend_router.write() {
            Ok(mut current) => *current = Some(Arc::downgrade(&router)),
            Err(error) => {
                tracing::warn!(%error, "merge workspace backend router lock poisoned");
            }
        }
    }

    pub(crate) fn workspace_backend_router(&self) -> Result<Arc<WorkspaceBackendRouter>> {
        let router = self.workspace_backend_router.read().map_err(|error| {
            ServiceError::invalid_operation(format!(
                "merge workspace backend router lock poisoned: {error}"
            ))
        })?;
        if let Some(router) = router.as_ref().and_then(Weak::upgrade) {
            return Ok(router);
        }
        if !self.test_workspace_backend {
            return Err(ServiceError::invalid_operation(
                "merge workspace backend router is not configured",
            ));
        }
        Ok(Arc::new(WorkspaceBackendRouter::new(Arc::new(
            EmbeddedWorkspaceBackend::new(
                Arc::clone(&self.db),
                Arc::new(self.clone()),
                self.workspace_root.clone(),
            ),
        ))))
    }

    async fn ensure_delivery_workspace(
        &self,
        task: &db::Task,
        execution: &db::Execution,
    ) -> Result<(db::Workspace, crate::workspace_backend::ResolvedWorkspace)> {
        let expected = resolve_delivery_workspace(&self.db, execution).await?;
        self.ensure_integration_workspace(task, expected).await
    }

    async fn ensure_integration_workspace(
        &self,
        task: &db::Task,
        expected: db::Workspace,
    ) -> Result<(db::Workspace, crate::workspace_backend::ResolvedWorkspace)> {
        let router = self.workspace_backend_router()?;
        let daemon_owned = WorkspacePlacementRepo::get_by_workspace_id(&*self.db, &expected.id)
            .await?
            .is_some_and(|placement| placement.owner_kind == db::PlacementOwnerKind::Daemon);
        let workspace = if daemon_owned {
            expected.clone()
        } else {
            crate::task_service::workspace::ensure_valid(
                &self.db,
                &self.workspace_root,
                task,
                expected.clone(),
                Some(Arc::clone(&self.integration_locks)),
                false,
                &router,
            )
            .await?
        };
        if workspace.id != expected.id {
            return Err(ServiceError::conflict(
                "validated workspace differs from the implementation execution workspace",
            ));
        }
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            &router,
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        Ok((workspace, resolved))
    }

    /// What admission to the integration queue records about a Task entering
    /// `merging`: the implementation execution's own workspace (validated,
    /// and re-created from the Task branch when a server-owned worktree was
    /// lost), the placement it merges from, the target branch and the
    /// candidate commit. Reads only; the queue worker performs every effect.
    pub(crate) async fn integration_admission_facts(
        &self,
        task: &db::Task,
    ) -> Result<IntegrationAdmissionFacts> {
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtasks do not merge; only root tasks merge to the default branch",
            ));
        }
        let execution =
            crate::task_service::latest_executor_execution_for_task(&self.db, task).await?;
        let incomplete_import = match task.condition.integration_wait() {
            Some(api_types::IntegrationReason::Deferred {
                attempt_id,
                cause: api_types::IntegrationDeferralCause::OwnerRequired | api_types::IntegrationDeferralCause::OwnerOffline,
                ..
            }) => self
                .db
                .unowned_quarantined_import(&task.id)
                .await?
                .is_some_and(|import| import.id == attempt_id.as_str()),
            _ => false,
        }
        // A later admission of the entry the owner already retried (its
        // placement was verified again, its attempt was sent back or parked
        // unbound): the same source, or the Task would be blocked for an
        // execution it never had.
            || self.db.retried_quarantined_import(&task.id).await?;
        let (workspace, resolved) = if incomplete_import {
            // The owner retries the Task's present branch, never the import's
            // old candidate. Its successor requires the full head check.
            let workspace = WorkspaceRepo::get_by_task_id(&*self.db, &task.id).await?
                .ok_or_else(|| ServiceError::invalid_operation("the quarantined Task has no current workspace; restore its branch before Retry"))?;
            self.ensure_integration_workspace(task, workspace).await?
        } else {
            let execution = execution.as_ref().ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "task {} has no executor execution",
                    task.id
                ))
            })?;
            self.ensure_delivery_workspace(task, execution).await?
        };
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let candidate_sha = resolved
            .git_query(api_types::WorkspaceGitQuery::Head, false)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("merge candidate has no HEAD"))?
            .trim()
            .to_owned();
        Ok(IntegrationAdmissionFacts {
            execution_id: execution.map(|execution| execution.id),
            target_branch: target_branch(&task.merge_config, &repo.default_branch)?,
            placement: resolved.placement.clone(),
            workspace,
            candidate_sha,
        })
    }

    pub(crate) async fn completed_hook_merge(
        &self,
        intent: &MergeIntent,
    ) -> Result<Option<MergeOutcome>> {
        let execution = ExecutionRepo::get_by_id(&*self.db, &intent.execution_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("execution", intent.execution_id.clone()))?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &intent.workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", intent.workspace_id.clone()))?;
        let resolved = EmbeddedWorkspaceBackend::resolve_workspace(
            self.workspace_backend_router()?.as_ref(),
            &self.db,
            &workspace,
            &self.workspace_root,
        )
        .await?;
        let target = resolved
            .git_query(
                api_types::WorkspaceGitQuery::ResolveRef {
                    reference: intent.target_branch.clone(),
                },
                false,
            )
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("merge target has no HEAD"))?
            .trim()
            .to_owned();
        if resolved
            .git_query(
                api_types::WorkspaceGitQuery::IsAncestor {
                    base: intent.candidate_sha.clone(),
                    head: target.clone(),
                },
                true,
            )
            .await?
            .is_none()
        {
            return Ok(None);
        }
        // Reviewed integration fast-forwards the target to this exact
        // object, so the candidate is the merge result. The target's tip may
        // since be a sibling's commit and is never recorded here.
        let merged_sha = intent.candidate_sha.clone();
        self.record_merge_execution_evidence(
            &execution.id,
            crate::integration_effects::MergeExecutionEvidence {
                before_sha: None,
                after_sha: Some(merged_sha.clone()),
            },
        )
        .await?;
        Ok(Some(MergeOutcome::Done {
            before_sha: intent.candidate_sha.clone(),
            after_sha: merged_sha,
            branch: intent.target_branch.clone(),
        }))
    }

    /// Today's Task-step projection, including the original repository
    /// transaction and its Project/usage trigger effects.
    pub(crate) async fn record_merge_execution_evidence(
        &self,
        execution_id: &str,
        evidence: crate::integration_effects::MergeExecutionEvidence,
    ) -> Result<()> {
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution_id.to_owned(),
                status: None,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: evidence.before_sha.map(Some),
                after_sha: evidence.after_sha.map(Some),
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn resolve_repo_source(&self, repo: &db::Repo) -> Result<String> {
        crate::server_commands::ensure_repository_allowed(repo).await?;
        if let Some(local_path) = repo
            .local_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if Path::new(local_path).exists() {
                return Ok(local_path.to_owned());
            }
        }
        let managed = self.managed_repo_path(&repo.id);
        if managed.exists() {
            return Ok(managed.to_string_lossy().into_owned());
        }
        let remote_url = repo
            .remote_url
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "repository has no available local path or remote URL",
                )
            })?;
        ensure_managed_clone(remote_url, &managed).await
    }

    pub(crate) fn managed_repo_path(&self, repo_id: &str) -> PathBuf {
        self.workspace_root.join(".repos").join(repo_id)
    }
}

/// Resolve the exact worktree pinned to the selected implementation attempt.
/// Child executions may point at the coordination root's shared Workspace,
/// but the execution reference remains the immutable repository provenance.
async fn resolve_delivery_workspace(
    db: &SqliteDb,
    execution: &db::Execution,
) -> Result<db::Workspace> {
    let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
        ServiceError::invalid_operation("executor execution missing workspace_id")
    })?;
    WorkspaceRepo::get_by_id(db, workspace_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))
}

async fn ensure_managed_clone(remote_url: &str, clone_path: &Path) -> Result<String> {
    if clone_path.exists() {
        return Ok(clone_path.to_string_lossy().into_owned());
    }
    if let Some(parent) = clone_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ServiceError::invalid_operation(format!("failed to create repo cache: {error}"))
        })?;
    }
    let output = Command::new("git")
        .args(["clone", remote_url, &clone_path.to_string_lossy()])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!("failed to clone repo: {error}"))
        })?;
    if !output.status.success() {
        return Err(ServiceError::invalid_operation(format!(
            "failed to clone repo from {remote_url}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(clone_path.to_string_lossy().into_owned())
}

pub(crate) fn target_branch(
    merge_config: &Option<String>,
    repo_default_branch: &str,
) -> Result<String> {
    if let Some(merge_config) = merge_config {
        let value: Value =
            serde_json::from_str(merge_config).map_err(|error| ServiceError::InvalidOperation {
                message: format!("invalid merge_config: {error}"),
            })?;
        if let Some(target_branch) = value
            .get("target_branch")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|target_branch| !target_branch.is_empty())
        {
            crate::server_commands::validate_branch_name(target_branch)?;
            return Ok(target_branch.to_owned());
        }
    }
    let repo_default_branch = repo_default_branch.trim();
    if repo_default_branch.is_empty() {
        Ok("main".to_owned())
    } else {
        crate::server_commands::validate_branch_name(repo_default_branch)?;
        Ok(repo_default_branch.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, new_uuid_v4, run_migrations, CreateExecution, CreateProject,
        CreateRepo, CreateTask, CreateWorkspace, ExecutionStatus, ProjectRepo, RepoRepo,
        UpdateProject, WorkspaceStatus,
    };
    use std::process::Stdio;
    use tempfile::TempDir;
    use tokio::process::Command;

    async fn sqlite_db() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        run_migrations(&pool).await.expect("migrations run");
        Arc::new(SqliteDb::new(pool))
    }

    async fn run_git(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {} failed\nstdout: {}\nstderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn setup_repo(temp: &TempDir) -> std::path::PathBuf {
        let repo_path = temp.path().join("repo");
        std::fs::create_dir_all(&repo_path).expect("repo dir creates");
        git::init(&repo_path).await.expect("repo initializes");
        run_git(&repo_path, &["checkout", "-B", "main"]).await;
        std::fs::write(repo_path.join("file.txt"), "base\n").expect("file writes");
        git::commit_all(&repo_path, "initial")
            .await
            .expect("initial commit creates");
        repo_path
    }

    async fn seed_merge_rows(
        db: &SqliteDb,
        repo_path: &Path,
        worktree_path: &Path,
        task_id: &str,
    ) -> String {
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        let repo_id = new_uuid_v4();
        let workspace_id = new_uuid_v4();
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
                remote_url: Some(repo_path.to_string_lossy().into_owned()),
                local_path: Some(repo_path.to_string_lossy().into_owned()),
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
                id: task_id.to_owned(),
                project_id: RepoRepo::get_by_id(db, &repo_id)
                    .await
                    .expect("repo loads")
                    .expect("repo exists")
                    .project_id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "review".to_owned(),
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
                task_id: task_id.to_owned(),
                repo_id,
                worktree_path: worktree_path.to_string_lossy().into_owned(),
                branch: workspace::task_branch_name(task_id),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        let execution_id = new_uuid_v4();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.to_owned(),
                agent_id: None,
                role: "executor".to_owned(),
                status: ExecutionStatus::Completed,
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
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");
        execution_id
    }

    #[tokio::test]
    async fn read_only_checks_do_not_repair_unavailable_workspace() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let workspace = WorkspaceRepo::get_by_task_id(&*db, &task_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        std::fs::remove_dir_all(&worktree_path).expect("worktree directory removes");
        run_git(&repo_path, &["worktree", "prune"]).await;
        let service = MergeService::new_for_test(Arc::clone(&db), temp.path().to_path_buf());

        assert!(service
            .unresolved_handoff_markers(&task_id)
            .await
            .expect("marker check treats a missing workspace as unavailable")
            .is_empty());
        assert!(matches!(
            service
                .review_carry_facts(&task_id)
                .await
                .expect("review carry treats a missing workspace as unavailable"),
            ReviewCarryFacts::Unavailable { .. }
        ));
        assert!(
            !worktree_path.exists(),
            "read-only checks must not recreate"
        );

        sqlx::query("UPDATE project SET primary_repo_id = NULL WHERE id = ?")
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .expect("project primary repo clears");
        sqlx::query("DELETE FROM repo WHERE id = ?")
            .bind(&workspace.repo_id)
            .execute(db.pool())
            .await
            .expect("repo deletes while workspace provenance remains");
        assert!(service
            .unresolved_handoff_markers(&task_id)
            .await
            .expect("missing repo keeps the empty marker result")
            .is_empty());
        assert_eq!(
            service
                .review_carry_facts(&task_id)
                .await
                .expect("missing repo keeps the unavailable review result"),
            ReviewCarryFacts::Unavailable {
                reason: "repository not found".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn marker_gate_applies_only_to_handed_off_paths_and_clears_after_repair() {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("marker_worktree");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(
            worktree_path.join("handoff.txt"),
            "<<<<<<< ours\n>>>>>>> theirs\n",
        )
        .expect("handed-off file writes");
        std::fs::write(worktree_path.join("example.txt"), "<<<<<<< example\n")
            .expect("legitimate example writes");
        git::commit_all(&worktree_path, "add marker-shaped lines")
            .await
            .expect("branch commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let service = MergeService::new_for_test(Arc::clone(&db), temp.path().to_path_buf());

        // Without a handoff, the branch is allowed to document marker lines.
        assert!(service
            .unresolved_handoff_markers(&task_id)
            .await
            .unwrap()
            .is_empty());

        // A second Task is handed only handoff.txt. An unrelated example
        // remains legitimate even while the handoff gate is active.
        let second_id = new_uuid_v4();
        let second_path = temp.path().join("second_marker_worktree");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&second_id),
            &second_path,
        )
        .await
        .expect("second worktree creates");
        std::fs::write(
            second_path.join("handoff.txt"),
            "<<<<<<< worker\n>>>>>>> target\n",
        )
        .expect("second handed-off file writes");
        std::fs::write(
            second_path.join("example2.txt"),
            "<<<<<<< documented example\n",
        )
        .expect("unrelated example writes");
        git::commit_all(&second_path, "unresolved handoff")
            .await
            .expect("second branch commits");
        seed_merge_rows(&db, &repo_path, &second_path, &second_id).await;
        db::TransitionLogRepo::insert(
            &*db,
            db::CreateTransitionLog {
                id: new_uuid_v4(),
                task_id: second_id.clone(),
                from_state: "merging".to_owned(),
                to_state: "merge_failed".to_owned(),
                trigger_name: None,
                triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow)
                    .display(),
                bridge: api_types::TransitionBridge::conflict_handoff(&["handoff.txt".into()]),
                trigger_reason:
                    "rebased onto main; conflicts were committed with markers in: handoff.txt"
                        .to_owned(),
                hook_results_json: None,
                rejection: false,
                created_at: now_rfc3339(),
            },
        )
        .await
        .expect("handoff records");
        assert_eq!(
            service
                .unresolved_handoff_markers(&second_id)
                .await
                .expect("marker gate evaluates"),
            vec!["handoff.txt".to_owned()]
        );

        std::fs::write(second_path.join("handoff.txt"), "both sides resolved\n")
            .expect("repair writes");
        git::commit_all(&second_path, "resolve handoff")
            .await
            .expect("repair commits");
        assert!(service
            .unresolved_handoff_markers(&second_id)
            .await
            .unwrap()
            .is_empty());
    }
}
