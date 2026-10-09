use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use db::{
    CreateWorkspacePlacement, PlacementOwnerKind, PlacementSelectedBy, PlacementState,
    RepoLocation, RepoLocationKind, RepoLocationOwnerKind, RepoLocationRepo, RepoRepo, SqliteDb,
    Workspace, WorkspacePlacement, WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use workspace::{RepoCacheLockManager, WorkspaceError, WorkspaceManager};

use super::{
    CleanupAck, Diff, DiffSpec, MergeOutcome, MergeSpec, OutboxHarvest, PrepareSpec,
    PreparedWorkspace, ResetSpec, ResolvedWorkspace, Result, RunResult, RunSpec, WorkspaceBackend,
    WorkspaceBackendError, WorkspaceBackendRouter, WorkspaceState,
};
use crate::{
    merge_service::{MergeService, WorkspaceMergeInput},
    ServiceError,
};

/// Where the repository a workspace was created from is right now.
enum RecordedRepoSource {
    Present(PathBuf),
    /// The Repo records this checkout and it is not on disk at this moment.
    MissingNow(PathBuf),
    /// The Repo row is gone or records no checkout, and Forge holds no clone.
    NotRecorded,
}

pub struct EmbeddedWorkspaceBackend {
    db: Arc<SqliteDb>,
    manager: WorkspaceManager,
    workspace_root: PathBuf,
    merge_service: Arc<MergeService>,
    repo_cache_locks: Arc<RepoCacheLockManager>,
}

impl EmbeddedWorkspaceBackend {
    pub fn new(
        db: Arc<SqliteDb>,
        merge_service: Arc<MergeService>,
        workspace_root: PathBuf,
    ) -> Self {
        Self {
            db,
            manager: WorkspaceManager::new(workspace_root.clone()),
            workspace_root,
            repo_cache_locks: Arc::new(RepoCacheLockManager::new()),
            merge_service,
        }
    }

    /// The repository a workspace was created from: the Repo's own checkout,
    /// else Forge's clone of it. A checkout that is recorded and not on disk
    /// at this moment (an unmounted volume, a moved directory) is reported as
    /// such, never as "no repository": its worktree registration is still
    /// there to remove once it is reachable again.
    async fn recorded_repo_source(&self, workspace: &db::Workspace) -> Result<RecordedRepoSource> {
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id).await?;
        let local = repo.as_ref().and_then(|repo| {
            repo.local_path
                .as_deref()
                .filter(|path| !path.trim().is_empty())
                .map(PathBuf::from)
        });
        if let Some(local) = local.as_ref().filter(|path| path.exists()) {
            return Ok(RecordedRepoSource::Present(local.clone()));
        }
        let cache = self.repo_cache_path(workspace);
        if cache.exists() {
            return Ok(RecordedRepoSource::Present(cache));
        }
        Ok(match local {
            Some(local) => RecordedRepoSource::MissingNow(local),
            None => RecordedRepoSource::NotRecorded,
        })
    }

    fn repo_cache_path(&self, workspace: &db::Workspace) -> PathBuf {
        self.workspace_root.join(".repos").join(&workspace.repo_id)
    }

    async fn workspace_repo_source(&self, workspace: &db::Workspace) -> Result<PathBuf> {
        if let RecordedRepoSource::Present(source) = self.recorded_repo_source(workspace).await? {
            return Ok(source);
        }
        // Workspace.repo_id survives Repo deletion. A surviving worktree can
        // still identify its original Git repository without cloning anything.
        let output = tokio::process::Command::new("git")
            .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
            .current_dir(workspace.embedded_worktree_path_for_backend())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
        if !output.status.success() {
            return Err(ServiceError::Git(git::GitError::CommandFailed {
                command: "git rev-parse --path-format=absolute --git-common-dir".to_owned(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
            .into());
        }
        Ok(PathBuf::from(
            String::from_utf8_lossy(&output.stdout).trim(),
        ))
    }

    pub fn with_repo_cache_locks(mut self, locks: Arc<RepoCacheLockManager>) -> Self {
        self.manager = self.manager.with_repo_cache_locks(Arc::clone(&locks));
        self.repo_cache_locks = locks;
        self
    }

    pub(crate) async fn resolve_workspace(
        router: &WorkspaceBackendRouter,
        db: &SqliteDb,
        workspace: &Workspace,
        workspace_root: &Path,
    ) -> Result<ResolvedWorkspace> {
        Self::ensure_server_placement(db, workspace, workspace_root).await?;
        router.resolve(db, workspace).await
    }

    /// Materialize the same server placement as creation for workspaces
    /// inserted directly, without replacing an existing owner's decision.
    pub(crate) async fn ensure_server_placement(
        db: &SqliteDb,
        workspace: &Workspace,
        workspace_root: &Path,
    ) -> Result<WorkspacePlacement> {
        Self::ensure_placement(db, workspace, Some(workspace_root)).await
    }

    pub(crate) async fn ensure_recorded_server_placement(
        db: &SqliteDb,
        workspace: &Workspace,
    ) -> Result<WorkspacePlacement> {
        Self::ensure_placement(db, workspace, None).await
    }

    pub(crate) async fn recorded_server_path(
        db: &SqliteDb,
        workspace: &Workspace,
    ) -> Result<PathBuf> {
        let placement = Self::ensure_recorded_server_placement(db, workspace).await?;
        if placement.owner_kind != PlacementOwnerKind::Server {
            return Err(WorkspaceBackendError::OwnerUnsupported {
                owner_kind: placement.owner_kind,
            });
        }
        placement
            .workspace_handle
            .map(PathBuf::from)
            .ok_or_else(|| {
                crate::ServiceError::invalid_operation("workspace has no server handle").into()
            })
    }

    async fn ensure_placement(
        db: &SqliteDb,
        workspace: &Workspace,
        workspace_root: Option<&Path>,
    ) -> Result<WorkspacePlacement> {
        if let Some(placement) =
            WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await?
        {
            return Ok(placement);
        }
        let repo = RepoRepo::get_by_id(db, &workspace.repo_id).await?;
        let local_path = repo
            .as_ref()
            .and_then(|repo| repo.local_path.as_deref())
            .map(str::trim)
            .filter(|path| !path.is_empty() && Path::new(path).exists());
        let recorded_path = Path::new(workspace.embedded_worktree_path_for_backend());
        let workspace_root = workspace_root.or_else(|| {
            recorded_path
                .parent()
                .filter(|parent| {
                    parent.file_name().and_then(|name| name.to_str())
                        == Some(workspace.task_id.as_str())
                })
                .and_then(Path::parent)
        });
        let (source, kind) = match local_path {
            Some(path) => (path.to_owned(), RepoLocationKind::PrimaryCheckout),
            None => (
                workspace_root
                    .map(|root| root.join(".repos").join(&workspace.repo_id))
                    .unwrap_or_else(|| recorded_path.to_owned())
                    .to_string_lossy()
                    .into_owned(),
                RepoLocationKind::ManagedClone,
            ),
        };
        let mut transaction = db::begin_immediate(db.pool())
            .await
            .map_err(ServiceError::from)?;
        if let Some(placement) =
            WorkspacePlacementRepo::get_by_workspace_id_in_tx(db, &mut transaction, &workspace.id)
                .await?
        {
            transaction.commit().await.map_err(ServiceError::from)?;
            return Ok(placement);
        }
        let location_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM repo_location WHERE repo_id = ? AND owner_kind = 'server'
             AND daemon_id IS NULL AND runtime_id IS NULL AND path = ? AND kind = ?
             ORDER BY created_at, id LIMIT 1",
        )
        .bind(&workspace.repo_id)
        .bind(&source)
        .bind(kind.to_string())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(ServiceError::from)?;
        let now = db::now_rfc3339();
        let location_id = match location_id {
            Some(id) => id,
            None => {
                let id = db::new_uuid_v4();
                sqlx::query(
                    "INSERT INTO repo_location (id, repo_id, owner_kind, path, kind,
                     is_default, status, created_at, updated_at)
                     VALUES (?, ?, 'server', ?, ?, ?, ?, ?, ?)",
                )
                .bind(&id)
                .bind(&workspace.repo_id)
                .bind(&source)
                .bind(kind.to_string())
                .bind(kind == RepoLocationKind::PrimaryCheckout)
                .bind(if kind == RepoLocationKind::PrimaryCheckout {
                    "ready"
                } else {
                    "unverified"
                })
                .bind(&now)
                .bind(&now)
                .execute(&mut *transaction)
                .await
                .map_err(ServiceError::from)?;
                id
            }
        };
        let agent_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT agent_id FROM execution WHERE workspace_id = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&workspace.id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(ServiceError::from)?
        .flatten();
        let placement = WorkspacePlacementRepo::create_in_tx(
            db,
            &mut transaction,
            CreateWorkspacePlacement {
                id: db::new_uuid_v4(),
                workspace_id: workspace.id.clone(),
                task_id: workspace.task_id.clone(),
                agent_id,
                owner_kind: PlacementOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: location_id,
                execution_daemon_id: None,
                workspace_handle: Some(workspace.embedded_worktree_path_for_backend().to_owned()),
                generation: 1,
                state: match workspace.status {
                    WorkspaceStatus::Creating => PlacementState::Preparing,
                    WorkspaceStatus::Ready => PlacementState::Ready,
                    WorkspaceStatus::Error => PlacementState::Failed,
                    WorkspaceStatus::Cleaning => PlacementState::Cleaning,
                    WorkspaceStatus::Cleaned => PlacementState::Cleaned,
                },
                selected_by: PlacementSelectedBy::Scheduler,
                selection_reason: serde_json::json!({
                    "rule": "server_default",
                    "rejected_candidates": [],
                    "workspace_status": workspace.status.to_string(),
                })
                .to_string(),
                reserved_until: None,
                disconnected_at: None,
                failure_cause: (workspace.status == WorkspaceStatus::Error)
                    .then_some(db::PlacementFailureCause::PrepareFailed),
                created_at: workspace.created_at.clone(),
                updated_at: now,
            },
        )
        .await?;
        transaction.commit().await.map_err(ServiceError::from)?;
        tracing::debug!(workspace_id = %workspace.id, placement_id = %placement.id,
            "created server placement for workspace without a recorded owner");
        Ok(placement)
    }

    async fn workspace(&self, placement: &WorkspacePlacement) -> Result<Workspace> {
        if placement.owner_kind != PlacementOwnerKind::Server {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        let current = WorkspacePlacementRepo::get_by_id(&*self.db, &placement.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace placement", placement.id.clone()))?;
        if current.owner_kind != placement.owner_kind
            || current.workspace_id != placement.workspace_id
            || current.repo_location_id != placement.repo_location_id
        {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        if current.generation != placement.generation {
            return Err(WorkspaceBackendError::StaleGeneration {
                placement_id: placement.id.clone(),
                expected: placement.generation,
                actual: current.generation,
            });
        }
        if current.task_id != placement.task_id
            || current.workspace_handle != placement.workspace_handle
        {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        if current.version != placement.version {
            return Err(db::DbError::VersionConflict.into());
        }
        WorkspaceRepo::get_by_id(&*self.db, &placement.workspace_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("workspace", placement.workspace_id.clone()).into()
            })
    }

    async fn location(
        &self,
        placement: &WorkspacePlacement,
        workspace: &Workspace,
    ) -> Result<RepoLocation> {
        let location = RepoLocationRepo::get_by_id(&*self.db, &placement.repo_location_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("repository location", placement.repo_location_id.clone())
            })?;
        if location.owner_kind != RepoLocationOwnerKind::Server
            || location.repo_id != workspace.repo_id
        {
            return Err(WorkspaceBackendError::WrongOwner {
                placement_id: placement.id.clone(),
            });
        }
        Ok(location)
    }

    async fn repo_source(&self, location: &RepoLocation) -> Result<String> {
        if Path::new(&location.path).exists() || location.kind != RepoLocationKind::ManagedClone {
            return Ok(location.path.clone());
        }
        // Legacy managed-clone locations are backfilled without a filesystem scan.
        let repo = RepoRepo::get_by_id(&*self.db, &location.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", location.repo_id.clone()))?;
        Ok(self.merge_service.resolve_repo_source(&repo).await?)
    }

    fn path<'a>(&self, placement: &'a WorkspacePlacement, workspace: &'a Workspace) -> &'a Path {
        Path::new(
            placement
                .workspace_handle
                .as_deref()
                .unwrap_or_else(|| workspace.embedded_worktree_path_for_backend()),
        )
    }

    async fn prepared(
        &self,
        path: &Path,
        base_sha: Option<&str>,
        branch: &str,
    ) -> Result<PreparedWorkspace> {
        Ok(PreparedWorkspace {
            handle: path.to_string_lossy().into_owned(),
            base_sha: match base_sha {
                Some(sha) => sha.to_owned(),
                None => git::get_current_sha(path).await?,
            },
            branch: branch.to_owned(),
        })
    }
}

#[async_trait]
impl WorkspaceBackend for EmbeddedWorkspaceBackend {
    async fn prepare(
        &self,
        placement: &WorkspacePlacement,
        base: &PrepareSpec,
    ) -> Result<PreparedWorkspace> {
        let workspace = self.workspace(placement).await?;
        let path = self.path(placement, &workspace);
        if path.exists() {
            git::get_current_sha(path).await?;
            return self
                .prepared(path, workspace.before_sha.as_deref(), &workspace.branch)
                .await;
        }
        let location = self.location(placement, &workspace).await?;
        let source = self.repo_source(&location).await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let path = if git::branch_exists(Path::new(&source), &workspace.branch).await? {
            self.manager
                .recover_worktree_named(&source, &workspace.task_id, &repo.name, &workspace.branch)
                .await?
        } else {
            self.manager
                .create_worktree_named(&source, &workspace.task_id, &repo.name, &base.base_ref)
                .await?
        };
        self.prepared(&path, None, &workspace.branch).await
    }

    async fn describe(&self, placement: &WorkspacePlacement) -> Result<WorkspaceState> {
        let workspace = self.workspace(placement).await?;
        let path = self.path(placement, &workspace);
        let exists = tokio::fs::try_exists(path).await?;
        Ok(WorkspaceState {
            exists,
            head_sha: if exists {
                Some(git::get_current_sha(path).await?)
            } else {
                None
            },
            dirty: exists && !git::is_worktree_clean(path).await?,
            branch: if exists {
                git::list_branches(path).await?.default_branch
            } else {
                None
            },
            // Server worktrees have no lock file; in-process keyed locks
            // serialize their use.
            locked: false,
            active_execution_ids: Vec::new(),
            journaled_execution_ids: Vec::new(),
        })
    }

    async fn run(&self, placement: &WorkspacePlacement, spec: &RunSpec) -> Result<RunResult> {
        let workspace = self.workspace(placement).await?;
        crate::integration_effects::check::run_at(self.path(placement, &workspace), spec).await
    }

    async fn diff(&self, placement: &WorkspacePlacement, spec: &DiffSpec) -> Result<Diff> {
        let workspace = self.workspace(placement).await?;
        crate::diff::ensure_workspace_diffable(&workspace)?;
        let mut response = crate::diff::read_workspace_diff(
            &self.path(placement, &workspace).to_string_lossy(),
            &workspace.branch,
            workspace.before_sha.as_deref(),
            &spec.base_ref,
            spec.head_ref.as_deref(),
        )
        .await?;
        let truncated = response.diff.len() > spec.max_bytes;
        if truncated {
            let mut end = spec.max_bytes;
            while !response.diff.is_char_boundary(end) {
                end -= 1;
            }
            response.diff.truncate(end);
        }
        Ok(Diff {
            response,
            truncated,
        })
    }

    async fn read(
        &self,
        placement: &WorkspacePlacement,
        rel_path: &str,
        limit: u64,
    ) -> Result<Vec<u8>> {
        let workspace = self.workspace(placement).await?;
        crate::plan_artifact::read_workspace_bytes(
            self.path(placement, &workspace),
            rel_path,
            limit,
        )
        .map_err(|error| match error {
            crate::plan_artifact::PlanArtifactError::DbError(error) => ServiceError::from(error),
            error => ServiceError::invalid_operation(error.to_string()),
        })
        .map_err(Into::into)
    }

    async fn merge(
        &self,
        placement: &WorkspacePlacement,
        spec: &MergeSpec,
    ) -> Result<MergeOutcome> {
        let workspace = self.workspace(placement).await?;
        let location = self.location(placement, &workspace).await?;
        let source = self.repo_source(&location).await?;
        let effect_workspace = super::effect_workspace(placement);
        Ok(self
            .merge_service
            .merge_workspace(
                &placement.task_id,
                WorkspaceMergeInput {
                    workspace: &effect_workspace,
                    worktree_path: self.path(placement, &workspace),
                    repo_path: Path::new(&source),
                    spec,
                },
            )
            .await?)
    }

    async fn reset(
        &self,
        placement: &WorkspacePlacement,
        spec: &ResetSpec,
    ) -> Result<PreparedWorkspace> {
        let workspace = self.workspace(placement).await?;
        let path = self.path(placement, &workspace);
        let current_sha = git::get_current_sha(path).await?;
        if current_sha != spec.expected_head_sha {
            return Err(ServiceError::conflict("workspace HEAD changed before reset").into());
        }
        if spec.base_ref != current_sha {
            let output = tokio::process::Command::new("git")
                .args(["reset", "--hard", &spec.base_ref])
                .current_dir(path)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .env_remove("GIT_INDEX_FILE")
                .output()
                .await?;
            if !output.status.success() {
                return Err(git::GitError::CommandFailed {
                    command: "git reset --hard".to_owned(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                }
                .into());
            }
        }
        let repo_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                ServiceError::invalid_operation("workspace repository name is invalid")
            })?;
        self.manager
            .reset_worktree(&workspace.task_id, repo_name)
            .await?;
        self.prepared(path, None, &workspace.branch).await
    }

    async fn cleanup(&self, placement: &WorkspacePlacement) -> Result<CleanupAck> {
        let workspace = self.workspace(placement).await?;
        let path = self.path(placement, &workspace);
        let source = match self.recorded_repo_source(&workspace).await? {
            RecordedRepoSource::Present(source) => source,
            // Removing the Task root now would report success and leave the
            // registration in the user's repository for good. Fail instead:
            // the scheduler retries with backoff and raises one attention
            // item when the repository stays away.
            RecordedRepoSource::MissingNow(source) => {
                return Err(ServiceError::invalid_operation(format!(
                    "repository {} is recorded for this workspace and is not reachable right now; \
                     workspace cleanup will be retried",
                    source.display()
                ))
                .into());
            }
            RecordedRepoSource::NotRecorded => match self.workspace_repo_source(&workspace).await {
                Ok(source) => source,
                // Neither the repository nor a worktree that can name it is
                // left, so there is no registration to remove: only the Task
                // root remains. The absent cache path tells the manager that.
                Err(_) => self.repo_cache_path(&workspace),
            },
        };
        let _guard = self
            .repo_cache_locks
            .acquire(&source.to_string_lossy())
            .await;
        let existed = tokio::fs::try_exists(&path).await?;
        match self
            .manager
            .cleanup_worktree(&workspace.task_id, &source, path)
            .await
        {
            Ok(()) => {
                if !existed && workspace.status != WorkspaceStatus::Cleaned {
                    // Success with nothing at the recorded path: either the
                    // directory was already removed, or the row names the
                    // wrong place and the real one is still on disk.
                    tracing::warn!(
                        workspace_id = %workspace.id,
                        task_id = %workspace.task_id,
                        path = %path.display(),
                        "workspace cleanup found no directory at the recorded path"
                    );
                }
                Ok(CleanupAck { removed: existed })
            }
            Err(WorkspaceError::NotFound) => Ok(CleanupAck { removed: false }),
            Err(error) => Err(error.into()),
        }
    }

    async fn reclaim_delivered_branch(
        &self,
        placement: &WorkspacePlacement,
        target_branch: &str,
    ) -> Result<bool> {
        let workspace = self.workspace(placement).await?;
        let RecordedRepoSource::Present(source) = self.recorded_repo_source(&workspace).await?
        else {
            return Ok(false);
        };
        let _guard = self
            .repo_cache_locks
            .acquire(&source.to_string_lossy())
            .await;
        let outcome =
            workspace::delete_delivered_task_branch(&source, &workspace.branch, target_branch)
                .await?;
        match &outcome {
            workspace::TaskBranchReclaim::Deleted { tip } => tracing::info!(
                task_id = %workspace.task_id,
                branch = %workspace.branch,
                %tip,
                target_branch,
                "deleted delivered Task branch"
            ),
            kept => tracing::debug!(
                task_id = %workspace.task_id,
                branch = %workspace.branch,
                target_branch,
                ?kept,
                "kept Task branch"
            ),
        }
        Ok(matches!(
            outcome,
            workspace::TaskBranchReclaim::Deleted { .. }
        ))
    }

    async fn harvest_outbox(
        &self,
        placement: &WorkspacePlacement,
        execution_id: &str,
    ) -> Result<OutboxHarvest> {
        let workspace = self.workspace(placement).await?;
        Ok(super::outbox::harvest(
            self.path(placement, &workspace),
            execution_id,
        ))
    }

    async fn consume_outbox(
        &self,
        placement: &WorkspacePlacement,
        execution_id: &str,
    ) -> Result<()> {
        let workspace = self.workspace(placement).await?;
        super::consume_embedded_execution_outbox(
            &self.path(placement, &workspace).to_string_lossy(),
            execution_id,
        )
    }
}
