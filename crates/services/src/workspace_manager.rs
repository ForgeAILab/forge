//! The one place a Task workspace becomes something a caller may run in.
//!
//! A `ready` row is a record, not a fact about the disk. [`WorkspaceManager::ensure_valid`]
//! checks the recorded directory against the filesystem and Git immediately
//! before use and either returns a [`ValidWorkspace`], repairs it through the
//! existing recreate path, or returns a typed [`WorkspaceUnavailable`]. No
//! caller runs a command in a directory this module has not just checked.
//!
//! The manager never writes Task rows: callers map the outcome through their
//! existing producers. The one thing it records itself is a Forge comment
//! naming the ref that keeps commits it moved HEAD away from.
//! `From<WorkspaceUnavailable> for ServiceError` keeps the error variants the dispatcher, HTTP and MCP layers already match on.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use db::{
    PlacementOwnerKind, PlacementState, RepoRepo, SqliteDb, Task, Workspace,
    WorkspacePlacementRepo, WorkspaceStatus,
};
use workspace::RepoCacheLockManager;

use crate::{
    task_service::workspace::{
        clear_workspace_cleanup_after, recover_missing_worktree, resolve_workspace_backend,
        worktree_readiness, WorktreeReadiness,
    },
    workspace_backend::{ResolvedWorkspace, WorkspaceBackendRouter},
    ServiceError,
};

/// Why a caller needs the workspace. It decides what the manager may repair
/// and whether HEAD must be on the Task branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    /// Launch an execution. May recreate the worktree and may put a clean
    /// worktree back on the Task branch.
    Execute,
    /// Review the candidate. May recreate; never moves HEAD.
    Review,
    /// Run a check or CI step. May recreate; never moves HEAD.
    Check,
    /// Rebase, merge or deliver. May recreate; never moves HEAD.
    Integrate,
    /// Run a lifecycle hook. May recreate; never moves HEAD.
    Hook,
    /// Discard uncommitted work before a reassigned worker starts. May
    /// recreate; a worktree off the Task branch is returned as it is
    /// (`on_task_branch() == false`), because the caller's reset is the
    /// repair and must not be refused for the state it is about to clear.
    Reset,
    /// Read-only projection. Never changes disk or rows and does not require
    /// a `ready` row; an unusable workspace is reported as
    /// [`WorkspaceUnavailable::Absent`].
    Inspect,
}

impl Purpose {
    fn repairs(self) -> bool {
        self != Self::Inspect
    }
}

/// What the manager changed to make the workspace valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Repair {
    None,
    /// The worktree was recreated or relinked from the Task branch.
    Recovered,
    /// A clean worktree was put back on the Task branch.
    CheckedOutTaskBranch,
    /// HEAD was strictly ahead of the Task branch: the branch was advanced
    /// to HEAD and checked out. No commit and no file changed.
    FastForwardedTaskBranch,
    /// HEAD had diverged from the Task branch: its commits are kept under
    /// [`ValidWorkspace::rescued_ref`] and the Task branch was checked out.
    RescuedOffBranchCommits,
}

/// A workspace checked against disk and Git by [`WorkspaceManager::ensure_valid`].
#[derive(Clone)]
pub(crate) struct ValidWorkspace {
    workspace: Workspace,
    resolved: ResolvedWorkspace,
    path: Option<PathBuf>,
    repair: Repair,
    on_task_branch: bool,
    rescued_ref: Option<String>,
}

impl ValidWorkspace {
    /// The Forge-owned ref that keeps the commits HEAD was on, for
    /// [`Repair::RescuedOffBranchCommits`].
    pub(crate) fn rescued_ref(&self) -> Option<&str> {
        self.rescued_ref.as_deref()
    }

    pub(crate) fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    pub(crate) fn into_workspace(self) -> Workspace {
        self.workspace
    }

    pub(crate) fn resolved(&self) -> &ResolvedWorkspace {
        &self.resolved
    }

    /// The validated directory on the Forge host. `None` for a daemon-owned
    /// placement, whose handle is never a local path.
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub(crate) fn repair(&self) -> Repair {
        self.repair
    }

    /// False only for [`Purpose::Inspect`] and [`Purpose::Reset`] on a
    /// worktree whose HEAD is not on the Task branch; every other purpose
    /// puts it back or refuses that state.
    pub(crate) fn on_task_branch(&self) -> bool {
        self.on_task_branch
    }
}

/// Why no valid workspace was returned.
#[derive(Debug)]
pub(crate) enum WorkspaceUnavailable {
    /// [`Purpose::Inspect`] only: the workspace is not usable right now and
    /// nothing was changed.
    Absent { reason: String },
    /// The workspace cannot be repaired in place; it needs the explicit reset.
    ResetRequired { task_id: String, reason: String },
    /// The owning daemon is disconnected; the existing owner-wait path applies.
    OwnerUnreachable { daemon_id: String },
    /// A repair is needed while an execution is still running there.
    Busy { reason: String },
    /// A database, filesystem or Git failure, carried unchanged so callers
    /// keep their existing retry classification.
    Infrastructure(Box<ServiceError>),
}

impl std::fmt::Display for WorkspaceUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent { reason } => write!(f, "workspace is not available: {reason}"),
            Self::ResetRequired { task_id, reason } => {
                write!(f, "workspace reset required for task {task_id}: {reason}")
            }
            Self::OwnerUnreachable { daemon_id } => {
                write!(f, "workspace owner daemon {daemon_id} is unreachable")
            }
            Self::Busy { reason } => write!(f, "{reason}"),
            Self::Infrastructure(error) => write!(f, "{error}"),
        }
    }
}

impl From<ServiceError> for WorkspaceUnavailable {
    fn from(error: ServiceError) -> Self {
        match error {
            ServiceError::WorkspaceResetRequired { task_id, reason } => {
                Self::ResetRequired { task_id, reason }
            }
            ServiceError::DaemonUnavailable { daemon_id } => Self::OwnerUnreachable { daemon_id },
            error => Self::Infrastructure(Box::new(error)),
        }
    }
}

impl From<db::DbError> for WorkspaceUnavailable {
    fn from(error: db::DbError) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<sqlx::Error> for WorkspaceUnavailable {
    fn from(error: sqlx::Error) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<crate::workspace_backend::WorkspaceBackendError> for WorkspaceUnavailable {
    fn from(error: crate::workspace_backend::WorkspaceBackendError) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<WorkspaceUnavailable> for ServiceError {
    fn from(unavailable: WorkspaceUnavailable) -> Self {
        match unavailable {
            WorkspaceUnavailable::Absent { reason } => {
                Self::invalid_operation(format!("workspace is not available: {reason}"))
            }
            WorkspaceUnavailable::ResetRequired { task_id, reason } => {
                Self::WorkspaceResetRequired { task_id, reason }
            }
            WorkspaceUnavailable::OwnerUnreachable { daemon_id } => {
                Self::DaemonUnavailable { daemon_id }
            }
            WorkspaceUnavailable::Busy { reason } => Self::conflict(reason),
            WorkspaceUnavailable::Infrastructure(error) => *error,
        }
    }
}

/// What the recorded directory is right now.
#[derive(Debug, PartialEq, Eq)]
enum Observed {
    /// A linked worktree of the recorded repository on the Task branch (or
    /// mid-rebase, where Git detaches HEAD on purpose).
    Healthy,
    Missing,
    /// The directory exists and Git cannot use it.
    Invalid,
    /// A linked worktree of a repository other than the recorded one.
    Foreign {
        reason: String,
    },
    /// The recorded path leaves the Task root or runs through a symbolic link.
    Escapes {
        reason: String,
    },
    /// A healthy worktree whose HEAD is not on the Task branch.
    OffBranch {
        head: String,
    },
}

pub(crate) struct WorkspaceManager<'a> {
    db: &'a SqliteDb,
    workspace_root: &'a Path,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &'a WorkspaceBackendRouter,
}

impl<'a> WorkspaceManager<'a> {
    pub(crate) fn new(
        db: &'a SqliteDb,
        workspace_root: &'a Path,
        repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
        router: &'a WorkspaceBackendRouter,
    ) -> Self {
        Self {
            db,
            workspace_root,
            repo_cache_locks,
            router,
        }
    }

    /// Check `workspace` against disk and Git for `purpose`, repairing it
    /// through the recreate path when the purpose allows. The row is kept when
    /// its Task branch is gone, so the reset stays an explicit operation.
    pub(crate) async fn ensure_valid(
        &self,
        task: &Task,
        workspace: Workspace,
        purpose: Purpose,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        self.ensure_valid_inner(task, workspace, purpose, false)
            .await
    }

    /// [`Self::ensure_valid`] for the create-or-reuse path of the Task that
    /// owns the row: when both the worktree and its branch are gone the row is
    /// deleted, so the next launch starts from the default branch.
    pub(crate) async fn ensure_valid_or_forget(
        &self,
        task: &Task,
        workspace: Workspace,
        purpose: Purpose,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        self.ensure_valid_inner(task, workspace, purpose, true)
            .await
    }

    /// The directory an execution the claim just admitted starts in.
    ///
    /// The claim checked this workspace against Git moments ago
    /// (`prepare_claim_workspace`), so the launch does not start another Git
    /// process between the spawn and the executor: it confirms on the
    /// filesystem alone that the recorded worktree is still confined to its
    /// Task root and still there. Anything else (directory gone, no Git
    /// link, a symbolic link, a row that is not `ready`, a daemon placement)
    /// takes the full [`Self::ensure_valid`] check and its repairs.
    pub(crate) async fn claimed_path(
        &self,
        task: &Task,
        workspace: Workspace,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        let on_server = WorkspacePlacementRepo::get_by_workspace_id(self.db, &workspace.id)
            .await?
            .is_some_and(|placement| {
                placement.owner_kind == PlacementOwnerKind::Server
                    && placement.state == PlacementState::Ready
            });
        if on_server && workspace.status == WorkspaceStatus::Ready {
            let resolved =
                resolve_workspace_backend(self.db, self.workspace_root, &workspace, self.router)
                    .await?;
            let path = resolved.embedded_path()?;
            if self
                .confinement_violation(&workspace, &path)
                .await?
                .is_none()
                && tokio::fs::try_exists(path.join(".git"))
                    .await
                    .unwrap_or(false)
            {
                return Ok(valid(workspace, resolved, path, Repair::None, true));
            }
        }
        self.ensure_valid(task, workspace, Purpose::Execute).await
    }

    async fn ensure_valid_inner(
        &self,
        task: &Task,
        workspace: Workspace,
        purpose: Purpose,
        delete_missing_workspace: bool,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        if let Some(placement) =
            WorkspacePlacementRepo::get_by_workspace_id(self.db, &workspace.id).await?
        {
            if placement.owner_kind == PlacementOwnerKind::Daemon {
                return self
                    .ensure_valid_on_daemon(task, workspace, placement, purpose)
                    .await;
            }
        }

        // A read-only projection reports what is on disk whatever the row
        // says: a worktree that is still there while its row is `cleaning`
        // is still the Task's tree.
        if workspace.status != WorkspaceStatus::Ready && purpose != Purpose::Inspect {
            return Err(ServiceError::invalid_operation(format!(
                "workspace for task {} is not ready",
                workspace.task_id
            ))
            .into());
        }
        // A workspace outlives its Repo row. A healthy worktree does not need
        // the row, so only a repair asks for it; a row that belongs to
        // another Project is refused outright, as before.
        let repo_missing = || ServiceError::not_found("repo", workspace.repo_id.clone());
        let repo = RepoRepo::get_by_id(self.db, &workspace.repo_id).await?;
        if repo
            .as_ref()
            .is_some_and(|repo| repo.project_id != task.project_id)
        {
            return Err(repo_missing().into());
        }
        let resolved =
            resolve_workspace_backend(self.db, self.workspace_root, &workspace, self.router)
                .await?;
        let path = resolved.embedded_path()?;
        let observed = self
            .observe(&workspace, repo.as_ref(), &resolved, &path)
            .await?;

        if purpose == Purpose::Inspect {
            return match observed {
                Observed::Healthy => Ok(valid(workspace, resolved, path, Repair::None, true)),
                Observed::OffBranch { .. } => {
                    Ok(valid(workspace, resolved, path, Repair::None, false))
                }
                Observed::Missing => Err(WorkspaceUnavailable::Absent {
                    reason: "worktree directory is missing".to_owned(),
                }),
                Observed::Invalid => Err(WorkspaceUnavailable::Absent {
                    reason: "worktree directory is not a Git worktree".to_owned(),
                }),
                Observed::Foreign { reason } | Observed::Escapes { reason } => {
                    Err(WorkspaceUnavailable::Absent { reason })
                }
            };
        }

        match observed {
            Observed::Healthy => {
                let workspace = clear_workspace_cleanup_after(self.db, workspace).await?;
                Ok(valid(workspace, resolved, path, Repair::None, true))
            }
            Observed::Escapes { reason } => Err(WorkspaceUnavailable::ResetRequired {
                task_id: workspace.task_id.clone(),
                reason,
            }),
            Observed::OffBranch { .. } if purpose == Purpose::Reset => {
                let workspace = clear_workspace_cleanup_after(self.db, workspace).await?;
                Ok(valid(workspace, resolved, path, Repair::None, false))
            }
            Observed::OffBranch { head } => {
                let target = repo.as_ref().map(|repo| repo.default_branch.as_str());
                self.return_to_task_branch(task, workspace, resolved, path, purpose, &head, target)
                    .await
            }
            observed @ (Observed::Missing | Observed::Invalid | Observed::Foreign { .. }) => {
                let repo = repo.ok_or_else(repo_missing)?;
                let owner_task_id = workspace.task_id.clone();
                let foreign = matches!(observed, Observed::Foreign { .. });
                let recovered = recover_missing_worktree(
                    self.db,
                    self.workspace_root,
                    &repo,
                    &owner_task_id,
                    workspace,
                    self.repo_cache_locks.clone(),
                    // A directory Git can still use is never left without
                    // its row, whatever the recorded repository says.
                    delete_missing_workspace && !foreign,
                    self.router,
                    foreign,
                )
                .await?;
                let workspace = clear_workspace_cleanup_after(self.db, recovered).await?;
                let resolved = resolve_workspace_backend(
                    self.db,
                    self.workspace_root,
                    &workspace,
                    self.router,
                )
                .await?;
                let path = resolved.embedded_path()?;
                Ok(valid(workspace, resolved, path, Repair::Recovered, true))
            }
        }
    }

    /// Put HEAD back on the Task branch without losing a commit.
    ///
    /// - HEAD is the Task branch's own commit: the checkout changes no file,
    ///   so every repairing purpose does it, with or without local changes.
    /// - HEAD is strictly ahead of the Task branch (the agent committed on a
    ///   detached HEAD, or after an interrupted rebase finished): the Task
    ///   branch is advanced to HEAD and checked out. No file changes.
    /// - HEAD is ahead of the Task branch only by commits the target branch
    ///   already has (someone checked the target out here): the Task branch
    ///   is never advanced to them, which would make the Task look delivered
    ///   with nothing to review. Nothing needs keeping, so this is handled
    ///   like a HEAD behind the Task branch.
    /// - HEAD has diverged from the Task branch: its commits are kept under
    ///   `refs/forge/rescued/<task>/<ts>`, a Forge comment on the Task says
    ///   so, and the Task branch is checked out. Git refuses the checkout
    ///   when it would overwrite uncommitted changes; the workspace then
    ///   needs the explicit reset and the reason names the ref.
    /// - HEAD is behind the Task branch: the checkout moves the tree, so only
    ///   [`Purpose::Execute`] does it, and only on a clean worktree.
    #[allow(clippy::too_many_arguments)]
    async fn return_to_task_branch(
        &self,
        task: &Task,
        workspace: Workspace,
        resolved: ResolvedWorkspace,
        path: PathBuf,
        purpose: Purpose,
        head: &str,
        target_branch: Option<&str>,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        let reset_required = |detail: &str| WorkspaceUnavailable::ResetRequired {
            task_id: workspace.task_id.clone(),
            reason: format!(
                "worktree HEAD is on {head}, not on Task branch '{}'; {detail}",
                workspace.branch
            ),
        };
        let task_ref = format!("refs/heads/{}", workspace.branch);
        let compared = match resolve_commits(&path, &task_ref).await {
            Some((head_sha, branch_sha)) => divergence(&path, &head_sha, &branch_sha)
                .await
                .map(|counts| (head_sha, branch_sha, counts)),
            None => None,
        };
        let Some((head_sha, branch_sha, (ahead, behind))) = compared else {
            return Err(reset_required(
                "HEAD could not be compared with the Task branch",
            ));
        };
        let mut repair = Repair::CheckedOutTaskBranch;
        let mut rescued_ref = None;
        let only_target_history = ahead > 0
            && behind == 0
            && target_branch_contains(&path, target_branch, &workspace.branch, &head_sha).await;
        if ahead > 0 && behind == 0 && !only_target_history {
            // Compare-and-swap on the commit just compared: a Task branch
            // that moved in between is not overwritten.
            if !git_succeeds(&path, &["update-ref", &task_ref, &head_sha, &branch_sha]).await {
                return Err(reset_required(&format!(
                    "HEAD has {ahead} commit(s) that are not on the Task branch and the branch \
                     could not be advanced to them"
                )));
            }
            repair = Repair::FastForwardedTaskBranch;
        } else if ahead > 0 && behind > 0 {
            let Some(kept) = self
                .rescue_off_branch_commits(task, &workspace, &path, head, &head_sha, ahead, behind)
                .await
            else {
                return Err(reset_required(&format!(
                    "HEAD has {ahead} commit(s) that are not on the Task branch and they could \
                     not be kept under a Forge ref, so the branch was not checked out"
                )));
            };
            repair = Repair::RescuedOffBranchCommits;
            rescued_ref = Some(kept);
        } else if behind > 0 || only_target_history {
            if purpose != Purpose::Execute {
                return Err(reset_required(
                    "the candidate is not moved for this operation",
                ));
            }
            let clean = git::is_worktree_clean(&path)
                .await
                .map_err(ServiceError::from)?;
            if !clean {
                return Err(reset_required(
                    "the worktree has uncommitted changes, so the branch was not checked out",
                ));
            }
        }
        // Never forced: Git carries local changes over or refuses.
        let checkout = git_at(&path)
            .args(["checkout", "--quiet", &workspace.branch, "--"])
            .output()
            .await;
        match checkout {
            Ok(output) if output.status.success() => {}
            failed => {
                let error = match failed {
                    Ok(output) => String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                    Err(error) => error.to_string(),
                };
                tracing::warn!(
                    task_id = %workspace.task_id,
                    workspace_id = %workspace.id,
                    branch = %workspace.branch,
                    %error,
                    "could not put the worktree back on its Task branch"
                );
                return Err(match &rescued_ref {
                    Some(kept) => reset_required(&format!(
                        "its {ahead} commit(s) are kept under {kept}, and checking the Task \
                         branch out failed (uncommitted changes would be overwritten)"
                    )),
                    None => reset_required("checking the Task branch out failed"),
                });
            }
        }
        tracing::info!(
            task_id = %workspace.task_id,
            workspace_id = %workspace.id,
            branch = %workspace.branch,
            previous_head = head,
            purpose = ?purpose,
            repair = ?repair,
            rescued_ref = rescued_ref.as_deref().unwrap_or(""),
            "worktree put back on its Task branch"
        );
        let workspace = clear_workspace_cleanup_after(self.db, workspace).await?;
        let mut valid = valid(workspace, resolved, path, repair, true);
        valid.rescued_ref = rescued_ref;
        Ok(valid)
    }

    /// Keep the commits of a diverged HEAD reachable and say so on the Task.
    /// One ref and one comment per rescued commit, however often the
    /// workspace is checked while it stays in that state. `None` when the
    /// ref could not be written; nothing is checked out then.
    #[allow(clippy::too_many_arguments)]
    async fn rescue_off_branch_commits(
        &self,
        task: &Task,
        workspace: &Workspace,
        path: &Path,
        head: &str,
        head_sha: &str,
        ahead: u64,
        behind: u64,
    ) -> Option<String> {
        let namespace = format!("{RESCUED_REF_NAMESPACE}/{}", workspace.task_id);
        let kept = match existing_ref_at(path, &namespace, head_sha).await {
            Some(kept) => kept,
            None => {
                let kept = format!(
                    "{namespace}/{}",
                    chrono::Utc::now().format("%Y%m%dT%H%M%S%3fZ")
                );
                if !git_succeeds(path, &["update-ref", &kept, head_sha]).await {
                    return None;
                }
                prune_rescued_refs(path, &namespace, RESCUED_REFS_PER_TASK).await;
                kept
            }
        };
        let now = db::now_rfc3339();
        let created = db::TaskCommentRepo::create_comment(
            self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: task.id.clone(),
                author_type: db::CommentAuthorType::System,
                author_id: None,
                author_name: "Forge".to_owned(),
                content: format!(
                    "The Task worktree was on {head} with {ahead} commit(s) that are not on Task \
                     branch '{branch}', which has {behind} commit(s) they do not build on. Forge \
                     kept those commits under `{kept}` ({head_sha}) and put the worktree back on \
                     the Task branch. They are not part of what is reviewed and delivered; bring \
                     them in with `git cherry-pick` or `git merge {kept}` if they belong to this \
                     Task.",
                    branch = workspace.branch
                ),
                execution_id: None,
                role: None,
                worklog_kind: None,
                idempotency_key: Some(format!("{RESCUED_COMMENT_KEY}{}:{head_sha}", task.id)),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        if let Err(error) = created {
            tracing::warn!(task_id = %task.id, %error, "failed to record rescued worktree commits");
        }
        Some(kept)
    }

    /// Daemon placements keep the describe / prepare contract: Forge never
    /// interprets their handle, so validity is the owner's answer.
    async fn ensure_valid_on_daemon(
        &self,
        task: &Task,
        workspace: Workspace,
        placement: db::WorkspacePlacement,
        purpose: Purpose,
    ) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        match placement.state {
            PlacementState::Ready => {}
            PlacementState::Disconnected => {
                return Err(WorkspaceUnavailable::OwnerUnreachable {
                    daemon_id: placement.daemon_id.unwrap_or_default(),
                })
            }
            _ => {
                return Err(WorkspaceUnavailable::ResetRequired {
                    task_id: task.id.clone(),
                    reason: format!("workspace placement is {}", placement.state),
                })
            }
        }
        let resolved = self.router.resolve(self.db, &workspace).await?;
        let exists = resolved.backend.describe(&resolved.placement).await?.exists;
        if exists {
            let workspace = if purpose.repairs() {
                clear_workspace_cleanup_after(self.db, workspace).await?
            } else {
                workspace
            };
            return Ok(ValidWorkspace {
                workspace,
                resolved,
                path: None,
                repair: Repair::None,
                on_task_branch: true,
                rescued_ref: None,
            });
        }
        if !purpose.repairs() {
            return Err(WorkspaceUnavailable::Absent {
                reason: "workspace does not exist on its owner".to_owned(),
            });
        }
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
        )
        .bind(&workspace.id)
        .fetch_one(self.db.pool())
        .await?
        {
            return Err(WorkspaceUnavailable::Busy {
                reason: "workspace still has a running execution".to_owned(),
            });
        }
        let recovered = resolved
            .backend
            .prepare(
                &resolved.placement,
                &crate::workspace_backend::PrepareSpec {
                    base_ref: workspace.before_sha.clone().ok_or_else(|| {
                        WorkspaceUnavailable::ResetRequired {
                            task_id: workspace.task_id.clone(),
                            reason: "workspace has no recorded recovery base".to_owned(),
                        }
                    })?,
                },
            )
            .await?;
        let mut update = crate::placement::admission::placement_update(&resolved.placement);
        update.workspace_handle = Some(Some(recovered.handle));
        WorkspacePlacementRepo::update(self.db, update).await?;
        let workspace = clear_workspace_cleanup_after(self.db, workspace).await?;
        let resolved = self.router.resolve(self.db, &workspace).await?;
        Ok(ValidWorkspace {
            workspace,
            resolved,
            path: None,
            repair: Repair::Recovered,
            on_task_branch: true,
            rescued_ref: None,
        })
    }

    /// No network and, for a healthy worktree, one Git process: a `rev-parse`
    /// that resolves HEAD and names the repository, the Git directory and the
    /// checked-out ref. Only when that fails is the launch-path probe run, so
    /// an unusable directory and a transient Git failure are told apart by
    /// the rule recovery has always used.
    async fn observe(
        &self,
        workspace: &Workspace,
        repo: Option<&db::Repo>,
        resolved: &ResolvedWorkspace,
        path: &Path,
    ) -> Result<Observed, WorkspaceUnavailable> {
        if let Some(reason) = self.confinement_violation(workspace, path).await? {
            return Ok(Observed::Escapes { reason });
        }
        let io = |error| ServiceError::Git(git::GitError::Io(error));
        if !tokio::fs::try_exists(path).await.map_err(io)? {
            return Ok(Observed::Missing);
        }
        if !tokio::fs::try_exists(path.join(".git")).await.map_err(io)? {
            return Ok(Observed::Invalid);
        }
        #[cfg(test)]
        let identity = if crate::task_service::workspace::worktree_probe_failure_injected(path) {
            None
        } else {
            worktree_identity(path).await?
        };
        #[cfg(not(test))]
        let identity = worktree_identity(path).await?;
        let Some(identity) = identity else {
            // Git could not describe the directory. The probe decides whether
            // it is unusable (recreate) or Git failed for another reason
            // (the error is returned and the caller retries). A directory the
            // probe accepts is used as before, without the newer checks.
            return Ok(match worktree_readiness(path).await? {
                WorktreeReadiness::Missing => Observed::Missing,
                WorktreeReadiness::Invalid => Observed::Invalid,
                WorktreeReadiness::Ready => Observed::Healthy,
            });
        };
        // A linked worktree names the repository it belongs to; it is judged
        // against a recorded repository that is on disk now. A directory that
        // is a repository of its own is not judged here (see the module
        // tests): Git works in it, and the delivery owners refuse it.
        if identity.git_dir != identity.common_dir {
            let recorded = self.recorded_common_dirs(workspace, repo, resolved).await;
            let task_ref = format!("refs/heads/{}", workspace.branch);
            let foreign = !recorded.is_empty() && !recorded.contains(&identity.common_dir);
            // Replacing it needs the Task branch in a recorded repository.
            // When none has it and this worktree is checked out on exactly
            // the Task branch (a worktree made before its Repo moved to
            // another location), it is the only home of the Task's work and
            // stays the one it runs in, as it always was. A worktree of
            // another repository on any other branch is not this Task's and
            // nothing is run in it.
            if foreign
                && identity.head_ref == task_ref
                && !any_repository_has_ref(&recorded, &task_ref).await
            {
                tracing::warn!(
                    task_id = %workspace.task_id,
                    workspace_id = %workspace.id,
                    worktree_repository = %identity.common_dir.display(),
                    "worktree on the Task branch belongs to a repository other than the \
                     recorded one, which does not have that branch; the worktree is used as \
                     it is"
                );
            } else if foreign {
                return Ok(Observed::Foreign {
                    reason: format!(
                        "{} is a worktree of {}, not of the recorded repository",
                        path.display(),
                        identity.common_dir.display()
                    ),
                });
            }
        }
        // Off the Task branch means the branch is there and HEAD is somewhere
        // else. A Task branch that no longer exists is the "branch gone"
        // fault, which only matters once the directory is unusable too.
        let task_ref = format!("refs/heads/{}", workspace.branch);
        if identity.head_ref != task_ref
            && !identity.rebase_in_progress
            && ref_exists(path, &task_ref).await
        {
            return Ok(Observed::OffBranch {
                head: if identity.head_ref == "HEAD" {
                    "a detached HEAD".to_owned()
                } else {
                    identity.head_ref
                },
            });
        }
        Ok(Observed::Healthy)
    }

    /// The worktree, and the Task root above it, must be real directories:
    /// a symbolic link there would have a command run somewhere Forge did not
    /// create. A path of the managed shape `<root>/<task_id>/<name>` is held
    /// to the rule cleanup deletes under, against the root the row records
    /// (rows outlive a changed `workspace.root`); any other recorded path
    /// has no Task root, and only the worktree itself is checked.
    async fn confinement_violation(
        &self,
        workspace: &Workspace,
        path: &Path,
    ) -> Result<Option<String>, WorkspaceUnavailable> {
        let recorded_root = path
            .parent()
            .filter(|task_root| {
                task_root.file_name().and_then(|name| name.to_str())
                    == Some(workspace.task_id.as_str())
            })
            .and_then(Path::parent);
        let Some(root) = recorded_root else {
            return match tokio::fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.file_type().is_symlink() => Ok(Some(format!(
                    "recorded worktree path {} is a symbolic link",
                    path.display()
                ))),
                _ => Ok(None),
            };
        };
        match workspace::WorkspaceManager::new(root.to_path_buf())
            .confine_worktree(&workspace.task_id, path)
            .await
        {
            Ok(()) => Ok(None),
            Err(workspace::WorkspaceError::PathEscape) => Ok(Some(format!(
                "recorded worktree path {} runs through a symbolic link",
                path.display()
            ))),
            Err(workspace::WorkspaceError::Io(error)) => {
                Err(ServiceError::Git(git::GitError::Io(error)).into())
            }
            Err(error) => Err(ServiceError::invalid_operation(error.to_string()).into()),
        }
    }

    /// The Git common directories a worktree of this workspace may belong to:
    /// the placement's repository location, the Repo's own checkout and
    /// Forge's clone of it. Only those on disk now; none means the check is
    /// skipped and recovery reports the missing repository itself.
    async fn recorded_common_dirs(
        &self,
        workspace: &Workspace,
        repo: Option<&db::Repo>,
        resolved: &ResolvedWorkspace,
    ) -> Vec<PathBuf> {
        let mut sources = Vec::new();
        if let Ok(Some(location)) =
            db::RepoLocationRepo::get_by_id(self.db, &resolved.placement.repo_location_id).await
        {
            sources.push(PathBuf::from(location.path));
        }
        if let Some(local) = repo
            .and_then(|repo| repo.local_path.as_deref())
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            sources.push(PathBuf::from(local));
        }
        sources.push(self.workspace_root.join(".repos").join(&workspace.repo_id));
        let mut dirs = Vec::new();
        for source in sources {
            let Ok(source) = tokio::fs::canonicalize(&source).await else {
                continue;
            };
            let dot_git = source.join(".git");
            let common = match tokio::fs::metadata(&dot_git).await {
                Ok(metadata) if metadata.is_dir() => dot_git,
                // The recorded checkout is itself a linked worktree or a
                // submodule: only Git knows its common directory.
                Ok(_) => match git_common_dir(&source).await {
                    Some(common) => common,
                    None => continue,
                },
                Err(_) => source,
            };
            if !dirs.contains(&common) {
                dirs.push(common);
            }
        }
        dirs
    }
}

/// The Task worktree on the Forge host for a read-only use, checked by
/// [`Purpose::Inspect`]: `None` when it is not usable right now (gone, not a
/// worktree of the recorded repository, or owned by a daemon). Nothing on
/// disk or in a row changes.
pub(crate) async fn inspect_path(
    db: &SqliteDb,
    workspace_root: &Path,
    router: &WorkspaceBackendRouter,
    task: &Task,
    workspace: Workspace,
) -> Result<Option<PathBuf>, ServiceError> {
    match WorkspaceManager::new(db, workspace_root, None, router)
        .ensure_valid(task, workspace, Purpose::Inspect)
        .await
    {
        Ok(valid) => Ok(valid.path().map(Path::to_path_buf)),
        Err(WorkspaceUnavailable::Absent { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Where the Task-root files of a server workspace are found: the plan, the
/// staged plans and the execution outboxes sit beside the worktree, are
/// located from its recorded path and outlive it (a worktree that was
/// recreated or is gone still has its plan). This is not a directory to run
/// anything in; the readers confine every file to the Task root themselves.
/// A daemon placement has no such path on this host.
///
/// The recorded path is refused before anything is read, written or deleted
/// beside it when it is not absolute, contains `..`, has no Task root above
/// it, or when it or its Task root is a symbolic link: the files would then
/// land somewhere Forge did not create.
pub(crate) fn task_root_anchor(
    resolved: &ResolvedWorkspace,
) -> Result<PathBuf, crate::workspace_backend::WorkspaceBackendError> {
    confined_anchor(resolved.embedded_path()?).map_err(Into::into)
}

fn confined_anchor(path: PathBuf) -> Result<PathBuf, ServiceError> {
    let refused = |why: &str| {
        ServiceError::invalid_operation(format!(
            "recorded workspace path {} {why}; the Task-root files beside it are not touched",
            path.display()
        ))
    };
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(refused("is not an absolute path without `..`"));
    }
    let Some(task_root) = path.parent().filter(|root| root.parent().is_some()) else {
        return Err(refused("has no Task root above it"));
    };
    for checked in [task_root, path.as_path()] {
        if std::fs::symlink_metadata(checked).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return Err(refused("runs through a symbolic link"));
        }
    }
    Ok(path)
}

/// [`task_root_anchor`] for a caller that holds only the placement row.
pub(crate) fn task_root_anchor_of(
    placement: &db::WorkspacePlacement,
) -> Result<PathBuf, ServiceError> {
    if placement.owner_kind != PlacementOwnerKind::Server {
        return Err(ServiceError::invalid_operation(
            "workspace is not owned by the Forge host",
        ));
    }
    let path = placement
        .workspace_handle
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| ServiceError::invalid_operation("workspace has no server handle"))?;
    confined_anchor(path)
}

fn valid(
    workspace: Workspace,
    resolved: ResolvedWorkspace,
    path: PathBuf,
    repair: Repair,
    on_task_branch: bool,
) -> ValidWorkspace {
    ValidWorkspace {
        workspace,
        resolved,
        path: Some(path),
        repair,
        on_task_branch,
        rescued_ref: None,
    }
}

struct WorktreeIdentity {
    common_dir: PathBuf,
    git_dir: PathBuf,
    /// `refs/heads/<branch>`, or `HEAD` when detached.
    head_ref: String,
    rebase_in_progress: bool,
}

fn git_at(path: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("git");
    command
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .kill_on_drop(true);
    command
}

/// `None` when Git cannot describe the directory, which the caller treats as
/// an unusable worktree.
async fn worktree_identity(path: &Path) -> Result<Option<WorktreeIdentity>, WorkspaceUnavailable> {
    let output = git_at(path)
        .args([
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
            "--absolute-git-dir",
            "HEAD",
            "--symbolic-full-name",
            "HEAD",
        ])
        .output()
        .await
        .map_err(|error| ServiceError::Git(git::GitError::Io(error)))?;
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    let (Some(common_dir), Some(git_dir), Some(_head_sha), Some(head_ref)) =
        (lines.next(), lines.next(), lines.next(), lines.next())
    else {
        return Ok(None);
    };
    let git_dir = PathBuf::from(git_dir);
    let rebase_in_progress = tokio::fs::try_exists(git_dir.join("rebase-merge"))
        .await
        .unwrap_or(false)
        || tokio::fs::try_exists(git_dir.join("rebase-apply"))
            .await
            .unwrap_or(false);
    Ok(Some(WorktreeIdentity {
        common_dir: canonical(Path::new(common_dir)).await,
        git_dir: canonical(&git_dir).await,
        head_ref: head_ref.to_owned(),
        rebase_in_progress,
    }))
}

/// Refs that keep commits Forge moved a worktree's HEAD away from.
pub(crate) const RESCUED_REF_NAMESPACE: &str = "refs/forge/rescued";

/// The newest rescue refs kept per Task; older ones are deleted when a new
/// one is written. All of them go when the workspace is reclaimed.
pub(crate) const RESCUED_REFS_PER_TASK: usize = 5;

/// Idempotency-key prefix of the Forge comment that names a rescue ref. It
/// is a note for the people operating the Task and stays out of agent
/// prompts.
pub(crate) const RESCUED_COMMENT_KEY: &str = "workspace-rescued:";

/// Rescue refs under `namespace`, newest first (their names end in a UTC
/// timestamp).
async fn rescued_refs_newest_first(git_dir: &Path, namespace: &str) -> Vec<String> {
    let output = git_at(git_dir)
        .args(["for-each-ref", "--sort=-refname", "--format=%(refname)"])
        .arg(namespace)
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

async fn prune_rescued_refs(git_dir: &Path, namespace: &str, keep: usize) {
    for stale in rescued_refs_newest_first(git_dir, namespace)
        .await
        .into_iter()
        .skip(keep)
    {
        git_succeeds(git_dir, &["update-ref", "-d", &stale]).await;
    }
}

/// Delete every rescue ref of the Task that owns `workspace`. Called when
/// the workspace is reclaimed, after the retention its cleanup waited for:
/// from then on the refs would only accumulate in the user's repository.
/// Best effort; a daemon placement has none.
pub(crate) async fn delete_rescued_refs(
    workspace_root: &Path,
    workspace: &Workspace,
    resolved: &ResolvedWorkspace,
) {
    if resolved.placement.owner_kind != PlacementOwnerKind::Server {
        return;
    }
    let namespace = format!("{RESCUED_REF_NAMESPACE}/{}", workspace.task_id);
    let worktree = resolved.embedded_path().ok();
    let clone = workspace_root.join(".repos").join(&workspace.repo_id);
    for git_dir in worktree.iter().chain([&clone]) {
        if tokio::fs::try_exists(git_dir).await.unwrap_or(false) {
            prune_rescued_refs(git_dir, &namespace, 0).await;
        }
    }
}

/// Whether `sha` is already part of the target branch (local or its
/// `origin` copy). An unknown target falls back to any branch other than
/// the Task branch.
async fn target_branch_contains(
    path: &Path,
    target_branch: Option<&str>,
    task_branch: &str,
    sha: &str,
) -> bool {
    let patterns = match target_branch.filter(|target| *target != task_branch) {
        Some(target) => vec![
            format!("refs/heads/{target}"),
            format!("refs/remotes/origin/{target}"),
        ],
        None => vec!["refs/heads".to_owned(), "refs/remotes".to_owned()],
    };
    let output = git_at(path)
        .args(["for-each-ref", "--format=%(refname)", "--contains", sha])
        .args(&patterns)
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|name| name != format!("refs/heads/{task_branch}")),
        _ => false,
    }
}

async fn git_succeeds(path: &Path, args: &[&str]) -> bool {
    match git_at(path).args(args).output().await {
        Ok(output) if output.status.success() => true,
        Ok(output) => {
            tracing::warn!(
                path = %path.display(),
                ?args,
                error = %String::from_utf8_lossy(&output.stderr).trim(),
                "git command failed while repairing a workspace"
            );
            false
        }
        Err(error) => {
            tracing::warn!(path = %path.display(), ?args, %error, "git could not be started");
            false
        }
    }
}

/// The commits HEAD and `full_ref` name. `None` when Git cannot resolve them.
async fn resolve_commits(path: &Path, full_ref: &str) -> Option<(String, String)> {
    let output = git_at(path)
        .args([
            "rev-parse",
            "HEAD^{commit}",
            &format!("{full_ref}^{{commit}}"),
        ])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    Some((lines.next()?.to_owned(), lines.next()?.to_owned()))
}

/// Commits `left` has that `right` lacks, and commits `right` has that
/// `left` lacks. `None` when Git cannot compare them.
async fn divergence(path: &Path, left: &str, right: &str) -> Option<(u64, u64)> {
    let output = git_at(path)
        .args([
            "rev-list",
            "--left-right",
            "--count",
            &format!("{left}...{right}"),
        ])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut counts = stdout.split_whitespace();
    Some((counts.next()?.parse().ok()?, counts.next()?.parse().ok()?))
}

/// A ref under `namespace` that already points at `sha`.
async fn existing_ref_at(path: &Path, namespace: &str, sha: &str) -> Option<String> {
    let output = git_at(path)
        .args([
            "for-each-ref",
            "--count=1",
            "--format=%(refname)",
            "--points-at",
            sha,
            namespace,
        ])
        .output()
        .await
        .ok()?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (output.status.success() && !name.is_empty()).then_some(name)
}

async fn ref_exists(path: &Path, full_ref: &str) -> bool {
    git_at(path)
        .args(["show-ref", "--verify", "--quiet", full_ref])
        .status()
        .await
        .is_ok_and(|status| status.success())
}

async fn any_repository_has_ref(common_dirs: &[PathBuf], full_ref: &str) -> bool {
    for common_dir in common_dirs {
        let found = tokio::process::Command::new("git")
            .arg("--git-dir")
            .arg(common_dir)
            .args(["show-ref", "--verify", "--quiet", full_ref])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .kill_on_drop(true)
            .status()
            .await
            .is_ok_and(|status| status.success());
        if found {
            return true;
        }
    }
    false
}

async fn git_common_dir(path: &Path) -> Option<PathBuf> {
    let output = git_at(path)
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(canonical(Path::new(String::from_utf8_lossy(&output.stdout).trim())).await)
}

async fn canonical(path: &Path) -> PathBuf {
    tokio::fs::canonicalize(path)
        .await
        .unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests;
