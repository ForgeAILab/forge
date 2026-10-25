use super::project_agent_workspace::{cleanup_repo_cache_if_authority_gone, resolve_repo_source};
use super::*;
use crate::placement::context::connection_handshakes;
use db::{
    CreateWorkspacePlacement, PlacementOwnerKind, PlacementSelectedBy, PlacementState,
    RepoLocationKind, UpdateWorkspacePlacement, WorkspacePlacementRepo,
};
use sqlx::{Sqlite, Transaction};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use crate::workspace_manager::Purpose;

/// Preserve Git and filesystem failures as typed transient service errors.
/// Dispatch dispositions are for stable governance refusals; converting these
/// failures to `InvalidOperation` text would park a Task until an unrelated
/// version change even though the repository/process may recover on its own.
fn map_workspace_error(error: ::workspace::WorkspaceError) -> ServiceError {
    match error {
        ::workspace::WorkspaceError::Git(error) => ServiceError::Git(error),
        ::workspace::WorkspaceError::Io(error) => ServiceError::Git(git::GitError::Io(error)),
        error => ServiceError::invalid_operation(error.to_string()),
    }
}

pub(crate) async fn resolve_workspace_backend(
    db: &SqliteDb,
    workspace_root: &Path,
    workspace: &Workspace,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
) -> Result<crate::workspace_backend::ResolvedWorkspace> {
    Ok(
        crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            router,
            db,
            workspace,
            workspace_root,
        )
        .await?,
    )
}

const PLACEMENT_RESERVATION_SECONDS: i64 = 600;

pub(super) struct WorkspaceAdmission {
    pub workspace: Workspace,
    claiming_task: Task,
    environment_admission: crate::placement::selection::EnvironmentAdmission,
    pub placement: db::WorkspacePlacement,
    selection_context: Option<crate::placement::SelectionContext>,
    server_facts: crate::placement::ServerFacts,
    review_config: api_types::ReviewConfig,
    settings: ProjectSettings,
    task_owner_id: Option<String>,
}

#[cfg(test)]
impl TaskService {
    pub(crate) async fn ready_workspace_for_capacity_test(
        &self,
        task: &Task,
        agent: &Agent,
    ) -> Result<String> {
        let reserved = self
            .reserve_claim_workspace(task, Some(agent), "coder")
            .await?;
        let ready = self.prepare_claim_workspace(reserved).await?;
        Ok(ready.placement.id)
    }
}

#[cfg(test)]
pub(crate) async fn prepare_workspace_for_test(
    db: &SqliteDb,
    workspace_root: &Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
) -> Result<Workspace> {
    let router = crate::lifecycle::context::embedded_workspace_router_for_test(
        Arc::new(db.clone()),
        workspace_root.to_path_buf(),
        repo_cache_locks.clone(),
    );
    prepare_workspace(db, workspace_root, task, task_id, repo_cache_locks, &router).await
}

#[derive(Debug)]
pub(crate) enum WorktreeReadiness {
    Ready,
    Missing,
    Invalid,
}

/// Prepare a workspace and report whether this call won creation ownership.
/// The ownership bit is consumed by admission-failure cleanup; callers must
/// never infer it from a racy preflight existence query.
pub(crate) async fn prepare_workspace_owned(
    db: &SqliteDb,
    workspace_root: &std::path::Path,
    task: &Task,
    task_id: &str,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    purpose: Purpose,
) -> Result<(Workspace, bool)> {
    let authority = resolve_task_repository_authority(db, task).await?;
    let owner_task_id = task.parent_task_id.as_deref().unwrap_or(task_id);
    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, owner_task_id).await? {
        if let Some(placement) =
            WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await?
        {
            if placement.owner_kind == PlacementOwnerKind::Daemon {
                ensure_workspace_repository_current(
                    db,
                    task,
                    &workspace,
                    &authority.repo,
                    owner_task_id,
                )
                .await?;
                return Ok((
                    ensure_valid_for(
                        db,
                        workspace_root,
                        task,
                        workspace,
                        repo_cache_locks,
                        false,
                        router,
                        purpose,
                    )
                    .await?,
                    false,
                ));
            }
        }
    }
    if let Some(parent_task_id) = task.parent_task_id.as_deref() {
        let parent_task = TaskRepo::get_by_id(db, parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.to_owned()))?;
        if parent_task.project_id != authority.project.id {
            return Err(ServiceError::RepoMismatch {
                project_id: task.project_id.clone(),
            });
        }
        let Some(workspace) = WorkspaceRepo::get_by_task_id(db, parent_task_id).await? else {
            // The root is a coordination container, so its first runnable
            // child creates the shared root-owned worktree on demand. A child
            // admission failure must not clean up that shared workspace.
            let (workspace, _) = create_fresh_workspace(
                db,
                workspace_root,
                &authority.repo,
                parent_task_id,
                repo_cache_locks,
            )
            .await?;
            return Ok((workspace, false));
        };
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, parent_task_id)
            .await?;
        if workspace.status == WorkspaceStatus::Ready {
            return Ok((
                ensure_valid_for(
                    db,
                    workspace_root,
                    task,
                    workspace,
                    repo_cache_locks,
                    false,
                    router,
                    purpose,
                )
                .await?,
                false,
            ));
        }
        return Err(ServiceError::parent_workspace_required(parent_task_id));
    }

    if let Some(workspace) = WorkspaceRepo::get_by_task_id(db, task_id).await? {
        ensure_workspace_repository_current(db, task, &workspace, &authority.repo, task_id).await?;
        if workspace.status == WorkspaceStatus::Cleaned {
            // A deliberate teardown (a reassignment reset, cancellation
            // cleanup) removes the worktree but keeps the row, so the Task's
            // next execution rebuilds it here instead of failing admission
            // on a row that can never become ready by itself.
            let repo_source = resolve_repo_source(&authority.repo, workspace_root).await?;
            let branch_exists =
                recovery_branch_exists(Path::new(&repo_source), &workspace.branch).await?;
            if !branch_exists {
                // Nothing left to recover: start over from the default
                // branch. Deleting the row only unlinks past executions
                // (`ON DELETE SET NULL`); their own records are kept.
                WorkspaceRepo::delete(db, &workspace.id).await?;
                return create_fresh_workspace(
                    db,
                    workspace_root,
                    &authority.repo,
                    task_id,
                    repo_cache_locks,
                )
                .await;
            }
            info!(
                task_id = task_id,
                workspace_id = %workspace.id,
                branch = %workspace.branch,
                "recreating cleaned workspace from its task branch"
            );
            return Ok((
                clear_workspace_cleanup_after(
                    db,
                    recover_missing_worktree(
                        db,
                        workspace_root,
                        &authority.repo,
                        task_id,
                        workspace,
                        repo_cache_locks,
                        true,
                        router,
                        false,
                        waits_for_disk(purpose)
                            .then(|| db.disk_admission.server_pressure())
                            .flatten(),
                    )
                    .await?,
                )
                .await?,
                false,
            ));
        }
        if workspace.status == WorkspaceStatus::Ready {
            return Ok((
                ensure_valid_for(
                    db,
                    workspace_root,
                    task,
                    workspace,
                    repo_cache_locks,
                    true,
                    router,
                    purpose,
                )
                .await?,
                false,
            ));
        }
        return Err(ServiceError::invalid_operation(format!(
            "workspace for task {task_id} is not ready"
        )));
    }

    create_fresh_workspace(
        db,
        workspace_root,
        &authority.repo,
        task_id,
        repo_cache_locks,
    )
    .await
}

/// The `disk_pressure` placement refusal for a worktree of `workspace` that
/// would have to be made again for `task_id`: the refusal a new worktree
/// gets on a machine under its floor, so the disk capacity wait is the same
/// one (`placement::capacity_wait_of`).
pub(crate) async fn recreation_refused_for_disk(
    db: &SqliteDb,
    task_id: &str,
    workspace: &Workspace,
) -> Result<ServiceError> {
    let placement = WorkspacePlacementRepo::get_by_workspace_id(db, &workspace.id).await?;
    Ok(ServiceError::PlacementUnavailable(
        crate::placement::PlacementUnavailable {
            task_id: task_id.to_owned(),
            repo_id: workspace.repo_id.clone(),
            rejected_candidates: vec![crate::placement::CandidateRejection {
                failing_checks: Vec::new(),
                repo_location_id: placement
                    .as_ref()
                    .map(|placement| placement.repo_location_id.clone())
                    .unwrap_or_default(),
                owner_kind: placement.as_ref().map_or_else(
                    || PlacementOwnerKind::Server.to_string(),
                    |placement| placement.owner_kind.to_string(),
                ),
                daemon_id: placement
                    .as_ref()
                    .and_then(|placement| placement.daemon_id.clone()),
                runtime_id: placement.and_then(|placement| placement.runtime_id),
                filter_codes: vec![crate::placement::PlacementFilterCode::DiskPressure],
            }],
        },
    ))
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum InjectedWorktreeProbeFailure {
    Io,
    NotRepository,
}

#[cfg(test)]
fn injected_worktree_probe_failures() -> &'static std::sync::Mutex<
    std::collections::HashMap<std::path::PathBuf, InjectedWorktreeProbeFailure>,
> {
    static FAILURES: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<std::path::PathBuf, InjectedWorktreeProbeFailure>,
        >,
    > = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn inject_worktree_probe_failure(path: &Path, failure: InjectedWorktreeProbeFailure) {
    injected_worktree_probe_failures()
        .lock()
        .expect("worktree probe injection lock")
        .insert(path.to_path_buf(), failure);
}

/// Whether a probe failure is waiting for `path`, so the workspace manager
/// lets the probe report it instead of its own cheaper check.
#[cfg(test)]
pub(crate) fn worktree_probe_failure_injected(path: &Path) -> bool {
    injected_worktree_probe_failures()
        .lock()
        .expect("worktree probe injection lock")
        .contains_key(path)
}

#[cfg(test)]
fn take_injected_worktree_probe_failure(path: &Path) -> Option<InjectedWorktreeProbeFailure> {
    injected_worktree_probe_failures()
        .lock()
        .expect("worktree probe injection lock")
        .remove(path)
}

#[cfg(test)]
fn injected_branch_lookup_failures(
) -> &'static std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>> {
    static FAILURES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
    > = std::sync::OnceLock::new();
    FAILURES.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

#[cfg(test)]
fn inject_branch_lookup_failure(path: &Path) {
    injected_branch_lookup_failures()
        .lock()
        .expect("branch lookup injection lock")
        .insert(path.to_path_buf());
}

#[cfg(test)]
fn take_injected_branch_lookup_failure(path: &Path) -> bool {
    injected_branch_lookup_failures()
        .lock()
        .expect("branch lookup injection lock")
        .remove(path)
}

/// Reusing a workspace revives its shared delivery branch. Any cleanup
/// deadline left by a previously terminal child is stale and must be cleared
/// before a new execution can rely on the worktree.
pub(crate) async fn clear_workspace_cleanup_after(
    db: &SqliteDb,
    workspace: Workspace,
) -> Result<Workspace> {
    if workspace.cleanup_after.is_none() {
        return Ok(workspace);
    }
    Ok(WorkspaceRepo::set_cleanup_after(db, &workspace.id, None, &now_rfc3339()).await?)
}

/// The account-owned scratch root, holding one directory per Main Agent
/// account. It sits beside the Project Agent workspaces rather than inside
/// any of them: nothing here belongs to a Project.
pub const MAIN_AGENT_WORKSPACES_DIR: &str = "main-agents";
/// The Main Agent's own durable notes, inside its scratch directory.
pub const MAIN_AGENT_NOTES_DIR: &str = "notes";
/// The parent of every ephemeral inquiry's private directory. It lives
/// inside the Main Agent's scratch root on purpose: the dispatcher can read
/// a sub-agent's findings file without the sub-agent's own directory ever
/// being writable by anything else.
pub const MAIN_AGENT_INQUIRIES_DIR: &str = "inquiries";

/// Ensure the Main Agent's scratch directory.
///
/// This is deliberately not a repository: no clone, no worktree, no remote,
/// nothing to push. It exists so the Main Agent and the ephemeral inquiries
/// it dispatches can keep findings and working notes on disk instead of
/// carrying them in a conversation that has to survive all day. Because no
/// repository is ever placed here, "cannot write to a repository" holds
/// because there is none, not because a tool was withheld.
pub async fn ensure_main_agent_workspace(
    workspaces_root: &Path,
    account_id: &str,
) -> Result<PathBuf> {
    let workspaces_root = std::fs::canonicalize(workspaces_root).unwrap_or_else(|_| {
        std::env::current_dir()
            .map(|cwd| cwd.join(workspaces_root))
            .unwrap_or_else(|_| workspaces_root.to_path_buf())
    });
    let workspace = workspaces_root
        .join(MAIN_AGENT_WORKSPACES_DIR)
        .join(account_id);
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_NOTES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    std::fs::create_dir_all(workspace.join(MAIN_AGENT_INQUIRIES_DIR)).map_err(|error| {
        ServiceError::invalid_operation(format!("Main Agent workspace is unavailable: {error}"))
    })?;
    write_scratch_workspace_boundary(&workspace)?;
    Ok(workspace)
}

/// Ensure one inquiry's private directory inside the dispatching account's
/// scratch root. Each inquiry gets its own so that concurrent sub-agents
/// cannot overwrite each other's findings.
pub async fn ensure_inquiry_workspace(
    workspaces_root: &Path,
    account_id: &str,
    inquiry_id: &str,
) -> Result<PathBuf> {
    let workspace = ensure_main_agent_workspace(workspaces_root, account_id).await?;
    let inquiry = workspace.join(MAIN_AGENT_INQUIRIES_DIR).join(inquiry_id);
    std::fs::create_dir_all(&inquiry).map_err(|error| {
        ServiceError::invalid_operation(format!("inquiry workspace is unavailable: {error}"))
    })?;
    Ok(inquiry)
}

/// The same upward-search guard the verification workspace uses. A scratch
/// directory lives inside the Forge data directory, which on a development
/// server sits inside Forge's own repository, so a `cargo` invocation here
/// would otherwise climb into Forge's workspace and be refused.
fn write_scratch_workspace_boundary(workspace: &Path) -> Result<()> {
    let manifest = workspace.join("Cargo.toml");
    if manifest.exists() {
        return Ok(());
    }
    std::fs::write(
        &manifest,
        "# Forge Main Agent scratch boundary.\n\
         # Cargo searches upward for a workspace root; this manifest is that root, so\n\
         # a build here never resolves against whatever repository holds this data\n\
         # directory. Nothing in this tree is a repository.\n\
         [workspace]\n\
         members = []\n\
         resolver = \"2\"\n",
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Main Agent workspace boundary could not be written: {error}"
        ))
    })
}

mod admission;

mod claim;

mod preparation;
#[cfg(test)]
use preparation::persist_prepared_workspace;
use preparation::{
    create_fresh_workspace, ensure_valid_for, ensure_workspace_repository_current,
    persist_recovered_workspace, server_repo_location_in_tx, waits_for_disk,
};

mod worktree_recovery;
use worktree_recovery::recovery_branch_exists;

mod reset;

pub(crate) use preparation::{ensure_valid, prepare_workspace, prepare_workspace_for};
pub(super) use reset::{reset_daemon_workspace, reset_workspace};
pub(crate) use worktree_recovery::{recover_missing_worktree, worktree_readiness};
#[cfg(test)]
pub(crate) mod tests;
