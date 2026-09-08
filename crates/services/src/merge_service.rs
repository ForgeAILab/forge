use crate::{Result, ServiceError};
use db::{
    now_rfc3339, Execution, ExecutionRepo, PageRequest, RepoRepo, ReviewConformanceRepo, SortBy,
    SortOrder, SqliteDb, TaskRepo, WorkMode, WorkspaceRepo,
};
use events::EventBus;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::process::Command;

pub struct MergeService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    workspace_root: PathBuf,
    integration_locks: workspace::RepoCacheLockManager,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    ReviewRequired {
        reason: String,
    },
    Done {
        before_sha: String,
        after_sha: String,
        branch: String,
    },
    PullRequest {
        pr_url: Option<String>,
        branch: String,
        target_branch: String,
    },
    Conflict {
        details: String,
        conflict_paths: Vec<PathBuf>,
    },
    Dirty {
        files: Vec<String>,
    },
    TargetDirty {
        files: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum MergeStrategy {
    #[default]
    Merge,
    Rebase,
}

impl MergeService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, workspace_root: PathBuf) -> Self {
        Self {
            db,
            event_bus,
            workspace_root,
            integration_locks: workspace::RepoCacheLockManager::new(),
        }
    }

    pub async fn merge(&self, task_id: impl Into<String>) -> Result<MergeOutcome> {
        let _ = self.event_bus.receiver_count();
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "task",
                id: task_id.clone(),
            })?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtasks do not merge; only root tasks merge to the default branch",
            ));
        }
        let execution = latest_executor_execution(&self.db, &task_id).await?;
        let workspace_id =
            execution
                .workspace_id
                .as_deref()
                .ok_or_else(|| ServiceError::InvalidOperation {
                    message: "executor execution missing workspace_id".to_owned(),
                })?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "workspace",
                id: workspace_id.to_owned(),
            })?;
        let repo_id = task
            .repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::NotFound {
                entity: "repo",
                id: repo_id.to_owned(),
            })?;
        if repo.work_mode == WorkMode::PullRequest {
            return self.publish_pr(&task_id).await;
        }
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let repo_source = self.resolve_repo_source(&repo).await?;
        let repo_path = Path::new(&repo_source);
        let worktree_path = Path::new(&workspace.worktree_path);

        let _integration_lock = self.integration_locks.acquire(repo_id).await;

        if !git::is_worktree_clean(worktree_path).await? {
            return Ok(MergeOutcome::Dirty {
                files: git::status_porcelain(worktree_path).await?,
            });
        }
        if !git::is_worktree_clean(repo_path).await? {
            return Ok(MergeOutcome::TargetDirty {
                files: git::status_porcelain(repo_path).await?,
            });
        }

        let before_sha = git::get_current_sha(repo_path).await?;
        let worktree_sha = git::get_current_sha(worktree_path).await?;
        ExecutionRepo::update(
            &*self.db,
            db::UpdateExecution {
                id: execution.id.clone(),
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
                before_sha: Some(Some(worktree_sha)),
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;

        let review_guard = match self.db.lock_review_integration(&task_id).await {
            Ok(guard) => guard,
            Err(error) => {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: error.to_string(),
                });
            }
        };
        let reviewed_sha = review_guard
            .contract
            .as_ref()
            .map(|contract| contract.commit_sha.clone());
        if let Some(contract) = &review_guard.contract {
            let target_sha = ::review::contract::git_read(
                repo_path,
                &[
                    "rev-parse",
                    "--verify",
                    &format!("refs/heads/{target_branch}"),
                ],
            )
            .await
            .map_err(ServiceError::invalid_operation)?;
            let current_sha = git::get_current_sha(worktree_path).await?;
            if contract.commit_sha != current_sha || contract.base_sha != target_sha.trim() {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: "reviewed commit or integration target changed; fresh review required"
                        .into(),
                });
            }
        }
        git::checkout_branch(repo_path, &target_branch).await?;
        let task_branch = workspace::task_branch_name(&task_id);
        let merged = if let Some(sha) = reviewed_sha {
            // Integrate the immutable reviewed object. Fast-forward refuses to
            // synthesize an unreviewed merge tree if target ancestry changed.
            let result =
                ::review::contract::git_read(repo_path, &["merge", "--ff-only", &sha]).await;
            if result.is_ok() && git::get_current_sha(repo_path).await? != sha {
                return Ok(MergeOutcome::ReviewRequired { reason: "integration target changed during merge; reviewed content was not integrated".into() });
            }
            review_guard.release().await?;
            match result {
                Ok(_) => Ok(()),
                Err(reason) => return Ok(MergeOutcome::ReviewRequired { reason }),
            }
        } else {
            let result = git::merge_branch_into(repo_path, &task_branch).await;
            review_guard.release().await?;
            result
        };
        match merged {
            Ok(()) => {
                let after_sha = git::get_current_sha(repo_path).await?;
                ExecutionRepo::update(
                    &*self.db,
                    db::UpdateExecution {
                        id: execution.id,
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
                        before_sha: None,
                        after_sha: Some(Some(after_sha.clone())),
                        error: None,
                        executor_config_snapshot_json: None,
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                Ok(MergeOutcome::Done {
                    before_sha,
                    after_sha,
                    branch: target_branch,
                })
            }
            Err(git::GitError::MergeConflict { stderr, .. }) => {
                let conflict_paths = read_conflict_paths(repo_path).await;
                if let Err(error) = git::abort_merge(repo_path).await {
                    tracing::warn!(%task_id, %error, "failed to abort merge");
                }
                Ok(MergeOutcome::Conflict {
                    details: stderr,
                    conflict_paths,
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn publish_pr(&self, task_id: impl Into<String>) -> Result<MergeOutcome> {
        let task_id = task_id.into();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        if task.parent_task_id.is_some() {
            return Err(ServiceError::invalid_operation(
                "subtasks do not publish pull requests; only root tasks publish",
            ));
        }
        let execution = latest_executor_execution(&self.db, &task_id).await?;
        let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("executor execution missing workspace_id")
        })?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let repo_id = task
            .repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        if repo.work_mode != WorkMode::PullRequest {
            return Err(ServiceError::invalid_operation(
                "publish_pr requires pull_request work mode",
            ));
        }
        let target_branch = target_branch(&task.merge_config, &repo.default_branch)?;
        let source_branch = workspace::task_branch_name(&task_id);
        let worktree_path = Path::new(&workspace.worktree_path);

        if !git::is_worktree_clean(worktree_path).await? {
            return Ok(MergeOutcome::Dirty {
                files: git::status_porcelain(worktree_path).await?,
            });
        }

        let _integration_lock = self.integration_locks.acquire(repo_id).await;
        let guard = match self.db.lock_review_integration(&task_id).await {
            Ok(guard) => guard,
            Err(error) => {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: error.to_string(),
                });
            }
        };
        let candidate = if let Some(contract) = &guard.contract {
            let head = git::get_current_sha(worktree_path).await?;
            if head != contract.commit_sha {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: "PR candidate changed since review".into(),
                });
            }
            Some(contract.commit_sha.clone())
        } else {
            None
        };
        // Remote publication is not local integration. Push the exact accepted
        // object, then recheck authority before recording the PR publication.
        guard.release().await?;
        let refspec = candidate
            .as_ref()
            .map(|sha| format!("{sha}:refs/heads/{source_branch}"));
        push_branch(worktree_path, refspec.as_deref().unwrap_or(&source_branch)).await?;
        let guard = match self.db.lock_review_integration(&task_id).await {
            Ok(guard) => guard,
            Err(error) => {
                return Ok(MergeOutcome::ReviewRequired {
                    reason: error.to_string(),
                });
            }
        };
        guard.release().await?;
        let pr_service = crate::pr_service::PrService::new(Arc::clone(&self.db));
        let published = pr_service
            .publish_pr(&task, &repo, &source_branch, &target_branch)
            .await?;

        Ok(MergeOutcome::PullRequest {
            pr_url: published.pr_url,
            branch: source_branch,
            target_branch,
        })
    }

    async fn resolve_repo_source(&self, repo: &db::Repo) -> Result<String> {
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
        ensure_managed_clone(&repo.remote_url, &managed).await
    }

    fn managed_repo_path(&self, repo_id: &str) -> PathBuf {
        self.workspace_root.join(".repos").join(repo_id)
    }
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

async fn push_branch(worktree_path: &Path, branch: &str) -> Result<()> {
    let output = Command::new("git")
        .args(["push", "-u", "origin", branch])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!("failed to push branch: {error}"))
        })?;
    if !output.status.success() {
        return Err(ServiceError::invalid_operation(format!(
            "failed to push branch {branch}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

async fn read_conflict_paths(worktree_path: &Path) -> Vec<PathBuf> {
    let output = Command::new("git")
        .args(["diff", "--name-only", "--diff-filter=U"])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await;

    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(PathBuf::from)
            .collect(),
        Ok(output) => {
            tracing::warn!(
                worktree_path = %worktree_path.display(),
                stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                "failed to read merge conflict paths"
            );
            Vec::new()
        }
        Err(error) => {
            tracing::warn!(
                worktree_path = %worktree_path.display(),
                %error,
                "failed to run git diff for merge conflict paths"
            );
            Vec::new()
        }
    }
}

async fn latest_executor_execution(db: &SqliteDb, task_id: &str) -> Result<Execution> {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 500,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    page.items
        .into_iter()
        .find(|execution| matches!(execution.role.as_str(), "executor" | "coder" | "worker"))
        .ok_or_else(|| ServiceError::InvalidOperation {
            message: format!("task {task_id} has no executor execution"),
        })
}

fn target_branch(merge_config: &Option<String>, repo_default_branch: &str) -> Result<String> {
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
            return Ok(target_branch.to_owned());
        }
    }
    let repo_default_branch = repo_default_branch.trim();
    if repo_default_branch.is_empty() {
        Ok("main".to_owned())
    } else {
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
                remote_url: repo_path.to_string_lossy().into_owned(),
                local_path: Some(repo_path.to_string_lossy().into_owned()),
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
                id: task_id.to_owned(),
                project_id: RepoRepo::get_by_id(db, &repo_id)
                    .await
                    .expect("repo loads")
                    .expect("repo exists")
                    .project_id,
                repo_id: Some(repo_id.clone()),
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
    async fn clean_merge_returns_done() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("feature writes");
        let worktree_sha = git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        let execution_id = seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;
        let before_sha = git::get_current_sha(&repo_path)
            .await
            .expect("before sha reads");

        let service = MergeService::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge succeeds");

        match outcome {
            MergeOutcome::Done {
                before_sha: actual_before,
                after_sha,
                branch: _,
            } => {
                assert_eq!(actual_before, before_sha);
                assert_eq!(after_sha, worktree_sha);
            }
            other => panic!("expected done, got {other:?}"),
        }
        let execution = ExecutionRepo::get_by_id(&*db, &execution_id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(execution.before_sha, Some(worktree_sha.clone()));
        assert_eq!(execution.after_sha, Some(worktree_sha));
    }

    #[tokio::test]
    async fn conformance_merge_refuses_changed_candidate_target_policy_and_legacy_pass() {
        for changed in ["none", "candidate", "target", "policy", "legacy"] {
            let db = sqlite_db().await;
            let temp = TempDir::new().unwrap();
            let repo = setup_repo(&temp).await;
            let task_id = new_uuid_v4();
            let worktree = temp.path().join("worktree");
            git::create_worktree(&repo, &workspace::task_branch_name(&task_id), &worktree)
                .await
                .unwrap();
            std::fs::write(worktree.join("feature.txt"), "reviewed\n").unwrap();
            let accepted_sha = git::commit_all(&worktree, "candidate").await.unwrap();
            let execution_id = seed_merge_rows(&db, &repo, &worktree, &task_id).await;
            sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES ('reviewer-role',?,'reviewer','agent','reviewer','now','now')")
                .bind(&task_id).execute(db.pool()).await.unwrap();
            let conformance = if changed == "legacy" {
                api_types::ReviewConformance::default()
            } else {
                sqlx::query("UPDATE execution SET status='running' WHERE id=?")
                    .bind(&execution_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                let contract = ::review::contract::admit(&db, &execution_id, &task_id, &worktree)
                    .await
                    .unwrap();
                let report = serde_json::json!({"contract_digest":contract.digest,"verdict":"pass","findings":[],
                    "requirements":contract.context.requirements.iter().map(|r|serde_json::json!({"requirement_id":r.id,"disposition":"satisfied","rationale":"Feature exists", "evidence":[{"kind":"file","path":"feature.txt","commit_sha":accepted_sha,"start_line":1,"end_line":1}]})).collect::<Vec<_>>()});
                let result = ::review::contract::evaluate(
                    &db,
                    &execution_id,
                    &worktree,
                    &report.to_string(),
                )
                .await
                .unwrap();
                assert_eq!(result.status, api_types::ConformanceStatus::Passed);
                sqlx::query("UPDATE execution SET status='completed' WHERE id=?")
                    .bind(&execution_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                result
            };
            let now = now_rfc3339();
            let review = db::ReviewRepo::create(
                &*db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    execution_id,
                    attempt_number: 1,
                    status: db::ReviewStatus::Running,
                    step_results_json: "{}".into(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await
            .unwrap();
            db::ReviewRepo::update_status(
                &*db,
                &review.id,
                db::ReviewStatus::Passed,
                serde_json::json!({"ci_steps":[],"conformance":conformance}).to_string(),
                Some(now.clone()),
                &now,
            )
            .await
            .unwrap();
            match changed {
                "candidate" => {
                    std::fs::write(worktree.join("feature.txt"), "unreviewed\n").unwrap();
                    git::commit_all(&worktree, "repair").await.unwrap();
                }
                "target" => {
                    std::fs::write(repo.join("target.txt"), "new target\n").unwrap();
                    git::commit_all(&repo, "target moved").await.unwrap();
                }
                "policy" => {
                    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
                        .bind(r#"{"review":{"ci_steps":["false"]}}"#)
                        .bind(&task_id)
                        .execute(db.pool())
                        .await
                        .unwrap();
                }
                _ => {}
            }
            let before = git::get_current_sha(&repo).await.unwrap();
            let service =
                MergeService::new(db.clone(), Arc::new(EventBus::new(16)), temp.path().into());
            let outcome = service.merge(task_id).await.unwrap();
            if changed == "none" {
                assert!(matches!(outcome, MergeOutcome::Done { .. }), "{outcome:?}");
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), accepted_sha);
            } else {
                assert!(
                    matches!(outcome, MergeOutcome::ReviewRequired { .. }),
                    "{changed}: {outcome:?}"
                );
                assert_eq!(git::get_current_sha(&repo).await.unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn conflicting_merge_returns_conflict_and_aborts() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("file.txt"), "feature\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        std::fs::write(repo_path.join("file.txt"), "main\n").expect("main writes");
        let repo_head = git::commit_all(&repo_path, "main")
            .await
            .expect("main commits");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service = MergeService::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::Conflict {
                details,
                conflict_paths,
            } => {
                assert!(details.contains("CONFLICT"));
                assert_eq!(conflict_paths, vec![PathBuf::from("file.txt")]);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(
            git::get_current_sha(&repo_path).await.expect("head reads"),
            repo_head
        );
        assert!(!git::detect_interrupted_merge(&repo_path)
            .await
            .expect("merge state reads"));
    }

    #[tokio::test]
    async fn dirty_worktree_returns_dirty() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("dirty.txt"), "dirty\n").expect("dirty writes");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service = MergeService::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::Dirty { files } => assert!(files.contains(&"dirty.txt".to_owned())),
            other => panic!("expected dirty, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dirty_target_repo_returns_target_dirty() {
        let db = sqlite_db().await;
        let event_bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp creates");
        let repo_path = setup_repo(&temp).await;
        let task_id = new_uuid_v4();
        let worktree_path = temp.path().join("worktrees").join(&task_id).join("repo");
        git::create_worktree(
            &repo_path,
            &workspace::task_branch_name(&task_id),
            &worktree_path,
        )
        .await
        .expect("worktree creates");
        std::fs::write(worktree_path.join("feature.txt"), "hello\n").expect("feature writes");
        git::commit_all(&worktree_path, "feature")
            .await
            .expect("feature commits");
        std::fs::write(repo_path.join("target-dirty.txt"), "dirty\n").expect("dirty writes");
        let before_sha = git::get_current_sha(&repo_path)
            .await
            .expect("before sha reads");
        seed_merge_rows(&db, &repo_path, &worktree_path, &task_id).await;

        let service = MergeService::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());
        let outcome = service.merge(task_id).await.expect("merge returns outcome");

        match outcome {
            MergeOutcome::TargetDirty { files } => {
                assert!(
                    files.iter().any(|file| file.contains("target-dirty.txt")),
                    "{files:?}"
                )
            }
            other => panic!("expected target dirty, got {other:?}"),
        }
        assert_eq!(
            git::get_current_sha(&repo_path).await.expect("head reads"),
            before_sha
        );
    }
}
