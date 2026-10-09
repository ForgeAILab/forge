#![forbid(unsafe_code)]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{fs, process::Command};

pub mod repo_cache;

pub use repo_cache::RepoCacheLockManager;

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("workspace already exists")]
    AlreadyExists,

    #[error("path escapes worktree root")]
    PathEscape,

    #[error("workspace not found")]
    NotFound,

    #[error("git error: {0}")]
    Git(#[from] git::GitError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, WorkspaceError>;

fn git_command() -> Command {
    let mut command = Command::new("git");
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE");
    command
}

#[derive(Debug, Clone)]
pub struct WorkspaceManager {
    root: PathBuf,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
}

impl WorkspaceManager {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            repo_cache_locks: None,
        }
    }

    pub fn with_repo_cache_locks(mut self, locks: Arc<RepoCacheLockManager>) -> Self {
        self.repo_cache_locks = Some(locks);
        self
    }

    pub async fn create_worktree(
        &self,
        repo_url: &str,
        task_id: &str,
        base_branch: &str,
    ) -> Result<PathBuf> {
        let repo_name = repo_name(repo_url);
        self.create_worktree_named(repo_url, task_id, &repo_name, base_branch)
            .await
    }

    pub async fn create_worktree_named(
        &self,
        repo_url: &str,
        task_id: &str,
        repo_name: &str,
        base_branch: &str,
    ) -> Result<PathBuf> {
        let task_root = self.root.join(task_id);
        let worktree_path = task_root.join(repo_name);

        if fs::try_exists(&worktree_path).await? {
            return Err(WorkspaceError::AlreadyExists);
        }

        fs::create_dir_all(&task_root).await?;

        let _repo_cache_guard = if let Some(locks) = &self.repo_cache_locks {
            Some(locks.acquire(repo_url).await)
        } else {
            None
        };

        let branch_name = task_branch_name(task_id);
        let mut args = vec![
            "worktree".to_string(),
            "add".to_string(),
            "-b".to_string(),
            branch_name,
            worktree_path.to_string_lossy().to_string(),
        ];

        if !base_branch.is_empty() {
            args.push(base_branch.to_string());
        }

        let output = git_command()
            .args(&args)
            .current_dir(repo_url)
            .output()
            .await?;

        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: format!("git {}", args.join(" ")),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .into());
        }

        Ok(worktree_path)
    }

    /// Create a detached worktree for reading and running only.
    ///
    /// No branch is created, so the checkout never occupies the task branch
    /// namespace and there is nothing for work to accumulate on. Callers use
    /// this for copies that exist to be exercised rather than edited.
    pub async fn create_detached_worktree_named(
        &self,
        repo_url: &str,
        owner_id: &str,
        name: &str,
        base_branch: &str,
    ) -> Result<PathBuf> {
        let owner_root = self.root.join(owner_id);
        let worktree_path = owner_root.join(name);

        if fs::try_exists(&worktree_path).await? {
            return Err(WorkspaceError::AlreadyExists);
        }
        fs::create_dir_all(&owner_root).await?;

        let _repo_cache_guard = if let Some(locks) = &self.repo_cache_locks {
            Some(locks.acquire(repo_url).await)
        } else {
            None
        };

        let mut args = vec![
            "worktree".to_string(),
            "add".to_string(),
            "--detach".to_string(),
            worktree_path.to_string_lossy().to_string(),
        ];
        if !base_branch.is_empty() {
            args.push(base_branch.to_string());
        }

        let output = git_command()
            .args(&args)
            .current_dir(repo_url)
            .output()
            .await?;
        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: format!("git {}", args.join(" ")),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .into());
        }
        Ok(worktree_path)
    }

    /// Re-point an existing detached worktree at the current tip of
    /// `base_branch` and drop any local edits and untracked files. The
    /// worktree shares its source repository's object database, so no fetch
    /// is involved — the branch ref is re-resolved as of now. Ignored files
    /// (build caches) survive so a refresh does not force a full rebuild.
    pub async fn refresh_detached_worktree(
        &self,
        worktree_path: &Path,
        base_branch: &str,
    ) -> Result<()> {
        let mut steps: Vec<Vec<String>> = vec![vec![
            "reset".to_string(),
            "--hard".to_string(),
            "HEAD".to_string(),
        ]];
        if !base_branch.is_empty() {
            steps.push(vec![
                "checkout".to_string(),
                "--detach".to_string(),
                base_branch.to_string(),
            ]);
        }
        steps.push(vec!["clean".to_string(), "-fd".to_string()]);
        for args in steps {
            let output = git_command()
                .args(&args)
                .current_dir(worktree_path)
                .output()
                .await?;
            if !output.status.success() {
                return Err(git::GitError::CommandFailed {
                    command: format!("git {}", args.join(" ")),
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    pub async fn recover_worktree(
        &self,
        repo_url: &str,
        task_id: &str,
        existing_branch: &str,
    ) -> Result<PathBuf> {
        let repo_name = repo_name(repo_url);
        self.recover_worktree_named(repo_url, task_id, &repo_name, existing_branch)
            .await
    }

    pub async fn recover_worktree_named(
        &self,
        repo_url: &str,
        task_id: &str,
        repo_name: &str,
        existing_branch: &str,
    ) -> Result<PathBuf> {
        let task_root = self.root.join(task_id);
        let worktree_path = task_root.join(repo_name);

        if fs::try_exists(&worktree_path).await? {
            return Err(WorkspaceError::AlreadyExists);
        }

        fs::create_dir_all(&task_root).await?;

        let _repo_cache_guard = if let Some(locks) = &self.repo_cache_locks {
            Some(locks.acquire(repo_url).await)
        } else {
            None
        };

        // Prune stale worktree references before re-adding
        let _ = git_command()
            .args(["worktree", "prune"])
            .current_dir(repo_url)
            .output()
            .await;

        let args = [
            "worktree",
            "add",
            &worktree_path.to_string_lossy(),
            existing_branch,
        ];

        let output = git_command()
            .args(args)
            .current_dir(repo_url)
            .output()
            .await?;

        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: format!("git {}", args.join(" ")),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .into());
        }

        Ok(worktree_path)
    }

    pub async fn reset_worktree(&self, task_id: &str, repo_name: &str) -> Result<()> {
        let worktree_path = self.root.join(task_id).join(repo_name);

        if !fs::try_exists(&worktree_path).await? {
            return Err(WorkspaceError::NotFound);
        }

        let reset_args = ["reset", "--hard", "HEAD"];
        let output = git_command()
            .args(reset_args)
            .current_dir(&worktree_path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: format!("git {}", reset_args.join(" ")),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .into());
        }

        let clean_args = ["clean", "-fd"];
        let output = git_command()
            .args(clean_args)
            .current_dir(&worktree_path)
            .output()
            .await?;

        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: format!("git {}", clean_args.join(" ")),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .into());
        }

        Ok(())
    }

    /// Reclaim one Task root: the worktree at `worktree_path`, its
    /// registration in `repo_path`, and everything else under the Task root
    /// (outboxes, plans, `<name>.broken-<ms>` copies left by a recovery).
    ///
    /// Every step is idempotent: a missing directory, a missing registration
    /// and a missing repository are all success. Read-only files and
    /// directories are made owner-writable first, without following links out
    /// of the Task root. Only registrations under this Task root are ever
    /// removed; a broad `git worktree prune` runs only when `repo_path` is a
    /// repository cache this manager owns (`<root>/.repos/...`).
    ///
    /// The Task branch is not touched here; see
    /// [`delete_delivered_task_branch`].
    pub async fn cleanup_worktree(
        &self,
        task_id: &str,
        repo_path: &Path,
        worktree_path: &Path,
    ) -> Result<()> {
        let (task_root, absolute_root, absolute_path) =
            self.confined_task_root(task_id, worktree_path).await?;
        // A repository that no longer exists holds no registration to remove.
        let repo_present = fs::try_exists(repo_path).await?;
        if repo_present {
            // A repository inside the Task root would be deleted with it.
            let repo = fs::canonicalize(repo_path).await?;
            if repo.starts_with(&absolute_root) || repo_path.starts_with(&task_root) {
                return Err(WorkspaceError::PathEscape);
            }
        }
        let is_this_worktree =
            |path: &PathBuf| path == &absolute_path || path.as_path() == worktree_path;
        let registered = repo_present
            && linked_worktrees(repo_path)
                .await?
                .iter()
                .any(is_this_worktree);
        if repo_present
            && !registered
            && fs::try_exists(worktree_path.join(".git")).await?
            && !unregistered_worktree_is_reclaimable(repo_path, worktree_path).await
        {
            return Err(git::GitError::CommandFailed {
                command: "git worktree remove --force".to_owned(),
                stdout: String::new(),
                stderr: "worktree is not registered in its workspace repository".to_owned(),
            }
            .into());
        }
        // Best effort: the removal below reports what is still not removable.
        let _ = git::make_tree_owner_writable(&task_root).await;
        if registered {
            let output = remove_registered_worktree(repo_path, worktree_path).await?;
            if !output.status.success() && fs::try_exists(worktree_path.join(".git")).await? {
                return Err(git::GitError::CommandFailed {
                    command: "git worktree remove --force".to_owned(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                }
                .into());
            }
            // Git may have dropped the registration and left files behind, or
            // an old cleaner left only build/outbox files. The Task-root
            // removal and the exact prune below finish either case.
        }
        remove_dir_all_writable(&task_root).await?;
        if repo_present {
            // Exact prune: only registrations that lived under this Task root.
            for path in linked_worktrees(repo_path).await? {
                if (path.starts_with(&absolute_root) || path.starts_with(&task_root))
                    && !fs::try_exists(&path).await?
                {
                    let output = remove_registered_worktree(repo_path, &path).await?;
                    if !output.status.success() {
                        return Err(git::GitError::CommandFailed {
                            command: "git worktree remove --force".to_owned(),
                            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                        }
                        .into());
                    }
                }
            }
            // A user's own repository may hold registrations Forge did not
            // create, so the broad prune is limited to Forge's own caches.
            if repo_path.starts_with(self.root.join(".repos")) {
                Self::prune_worktrees(repo_path).await?;
            }
        }
        Ok(())
    }

    /// The only directory `cleanup_worktree` may delete: `<root>/<task_id>`,
    /// a real directory (or nothing) that is a direct child of this manager's
    /// root, with the worktree as its direct child.
    ///
    /// Refused with [`WorkspaceError::PathEscape`]: an id that is not one
    /// plain path component (empty, `.`, `..`, absolute, nested) or is a
    /// dot-directory of the root (`.repos`, `.forge`); a worktree path that is
    /// not `<root>/<task_id>/<name>`; and a Task root or worktree path that is
    /// a symbolic link, whose target is not Forge's to delete.
    ///
    /// Returns the Task root as given, and the Task root and worktree path
    /// with the manager root resolved, which is how Git lists a worktree
    /// (also after its directory is gone).
    async fn confined_task_root(
        &self,
        task_id: &str,
        worktree_path: &Path,
    ) -> Result<(PathBuf, PathBuf, PathBuf)> {
        use std::path::Component;

        let mut components = Path::new(task_id).components();
        let plain = matches!(
            (components.next(), components.next()),
            (Some(Component::Normal(_)), None)
        );
        if !plain || task_id.starts_with('.') {
            return Err(WorkspaceError::PathEscape);
        }
        let task_root = self.root.join(task_id);
        let name = match worktree_path.components().next_back() {
            Some(Component::Normal(name)) => name,
            _ => return Err(WorkspaceError::PathEscape),
        };
        if worktree_path.parent() != Some(task_root.as_path()) {
            return Err(WorkspaceError::PathEscape);
        }
        for path in [task_root.as_path(), worktree_path] {
            match fs::symlink_metadata(path).await {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(WorkspaceError::PathEscape)
                }
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                    ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        // The root may sit behind a link (`/tmp`, a relocated data
        // directory); the Task root and the worktree, checked above, do not.
        let root = match fs::canonicalize(&self.root).await {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::path::absolute(&self.root)?
            }
            Err(error) => return Err(error.into()),
        };
        let absolute_root = root.join(task_id);
        let absolute_path = absolute_root.join(name);
        Ok((task_root, absolute_root, absolute_path))
    }

    /// Whether `worktree_path` is `<root>/<task_id>/<name>` with neither the
    /// Task root nor the worktree a symbolic link: the rule
    /// [`Self::cleanup_worktree`] deletes under, for callers that are about
    /// to run something there. [`WorkspaceError::PathEscape`] otherwise.
    pub async fn confine_worktree(&self, task_id: &str, worktree_path: &Path) -> Result<()> {
        self.confined_task_root(task_id, worktree_path)
            .await
            .map(|_| ())
    }

    pub async fn prune_worktrees(repo_path: &Path) -> Result<()> {
        let output = git_command()
            .args(["worktree", "prune", "--expire", "now"])
            .current_dir(repo_path)
            .kill_on_drop(true)
            .output()
            .await?;
        if !output.status.success() {
            return Err(git::GitError::CommandFailed {
                command: "git worktree prune --expire now".to_owned(),
                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            }
            .into());
        }
        Ok(())
    }

    pub fn validate_path(worktree_root: &Path, target_path: &Path) -> Result<()> {
        let worktree_root = worktree_root.canonicalize()?;
        let target_path = target_path.canonicalize()?;

        if target_path.starts_with(worktree_root) {
            Ok(())
        } else {
            Err(WorkspaceError::PathEscape)
        }
    }
}

pub fn task_branch_name(task_id: &str) -> String {
    format!("{TASK_BRANCH_PREFIX}{}", &task_id[..task_id.len().min(8)])
}

const TASK_BRANCH_PREFIX: &str = "task/";

/// What [`delete_delivered_task_branch`] did with a Task branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskBranchReclaim {
    /// The branch was delivered and is deleted; `tip` is the commit it held.
    Deleted { tip: String },
    /// The branch no longer exists.
    Absent,
    /// The tip is not contained in the target branch: the work is undelivered.
    Undelivered,
    /// The target branch does not exist, so delivery cannot be proven.
    TargetMissing,
    /// A worktree still has the branch checked out.
    CheckedOut,
    /// The name is not a Forge Task branch, or it is the target itself.
    NotATaskBranch,
}

/// Delete a Task branch only when Git proves, now, that its tip is contained
/// in the target branch (`refs/heads/<target>`, or `origin/<target>` when the
/// change was delivered through the remote). Nothing stored is trusted.
///
/// An undelivered branch, a branch another worktree has checked out, and any
/// name outside `task/` are kept. Callers hold the repository lock.
pub async fn delete_delivered_task_branch(
    repo_path: &Path,
    branch: &str,
    target_branch: &str,
) -> Result<TaskBranchReclaim> {
    if !branch.starts_with(TASK_BRANCH_PREFIX) || branch == target_branch {
        return Ok(TaskBranchReclaim::NotATaskBranch);
    }
    let branch_ref = format!("refs/heads/{branch}");
    let Some(tip) = resolve_commit(repo_path, &branch_ref).await? else {
        return Ok(TaskBranchReclaim::Absent);
    };
    let mut target_found = false;
    let mut delivered = false;
    for target_ref in [
        format!("refs/heads/{target_branch}"),
        format!("refs/remotes/origin/{target_branch}"),
    ] {
        if resolve_commit(repo_path, &target_ref).await?.is_none() {
            continue;
        }
        target_found = true;
        let args = ["merge-base", "--is-ancestor", tip.as_str(), &target_ref];
        let output = git::command_output(repo_path, &args).await?;
        match output.status.code() {
            Some(0) => {
                delivered = true;
                break;
            }
            Some(1) => {}
            _ => {
                return Err(git::GitError::CommandFailed {
                    command: format!("git {}", args.join(" ")),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                }
                .into());
            }
        }
    }
    if !target_found {
        return Ok(TaskBranchReclaim::TargetMissing);
    }
    if !delivered {
        return Ok(TaskBranchReclaim::Undelivered);
    }
    let checked_out = format!("branch {branch_ref}");
    let output = git::command_output(repo_path, &["worktree", "list", "--porcelain"]).await?;
    if !output.status.success() {
        return Err(git::GitError::CommandFailed {
            command: "git worktree list --porcelain".to_owned(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
        .into());
    }
    if String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line == checked_out)
    {
        return Ok(TaskBranchReclaim::CheckedOut);
    }
    // Compare-and-delete: a tip that moved since the ancestry check stays.
    let args = ["update-ref", "-d", branch_ref.as_str(), tip.as_str()];
    let output = git::command_output(repo_path, &args).await?;
    if !output.status.success() {
        return Err(git::GitError::CommandFailed {
            command: format!("git {}", args.join(" ")),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
        .into());
    }
    Ok(TaskBranchReclaim::Deleted { tip })
}

async fn resolve_commit(repo_path: &Path, reference: &str) -> Result<Option<String>> {
    let spec = format!("{reference}^{{commit}}");
    let output =
        git::command_output(repo_path, &["rev-parse", "--verify", "--quiet", &spec]).await?;
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok((output.status.success() && !sha.is_empty()).then_some(sha))
}

/// Linked worktrees of `repo_path`. The first entry Git lists is the main
/// working tree (or the bare repository), which is never a Task worktree.
async fn linked_worktrees(repo_path: &Path) -> Result<Vec<PathBuf>> {
    let output = git_command()
        .args(["worktree", "list", "--porcelain"])
        .current_dir(repo_path)
        .kill_on_drop(true)
        .output()
        .await?;
    if !output.status.success() {
        return Err(git::GitError::CommandFailed {
            command: "git worktree list --porcelain".to_owned(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .skip(1)
        .map(PathBuf::from)
        .collect())
}

async fn remove_registered_worktree(
    repo_path: &Path,
    worktree_path: &Path,
) -> Result<std::process::Output> {
    Ok(git_command()
        .args(["worktree", "remove", "--force"])
        .arg(worktree_path)
        .current_dir(repo_path)
        .kill_on_drop(true)
        .output()
        .await?)
}

/// A directory that still has a `.git` entry but no registration in
/// `repo_path` is reclaimable when it is a leftover of that repository: its
/// gitfile points at administrative data that is gone, or that lives inside
/// this repository. A checkout of some other repository is left alone.
async fn unregistered_worktree_is_reclaimable(repo_path: &Path, worktree_path: &Path) -> bool {
    let Ok(gitfile) = fs::read_to_string(worktree_path.join(".git")).await else {
        // A `.git` directory is a repository of its own, not a worktree.
        return false;
    };
    let Some(admin_dir) = gitfile.trim().strip_prefix("gitdir:").map(str::trim) else {
        return false;
    };
    let admin_dir = worktree_path.join(admin_dir);
    let Ok(admin_dir) = fs::canonicalize(&admin_dir).await else {
        return true;
    };
    let Ok(output) = git_command()
        .args(["rev-parse", "--path-format=absolute", "--git-common-dir"])
        .current_dir(repo_path)
        .kill_on_drop(true)
        .output()
        .await
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let common_dir = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    match fs::canonicalize(&common_dir).await {
        Ok(common_dir) => admin_dir.starts_with(common_dir),
        Err(_) => false,
    }
}

/// `remove_dir_all` that treats a missing directory as done and repairs
/// permissions once when the first attempt is refused.
async fn remove_dir_all_writable(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path).await {
        Ok(()) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) if error.kind() != std::io::ErrorKind::PermissionDenied => {
            return Err(error.into())
        }
        Err(_) => {}
    }
    let _ = git::make_tree_owner_writable(path).await;
    match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn repo_name(repo_url: &str) -> String {
    let trimmed = repo_url.trim_end_matches(['/', '\\']);
    let last_component = trimmed
        .rsplit(['/', '\\'])
        .next()
        .filter(|component| !component.is_empty())
        .unwrap_or("repo");

    last_component
        .strip_suffix(".git")
        .unwrap_or(last_component)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    async fn setup_repo() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let repo_path = dir.path().join("repo");

        fs::create_dir_all(&repo_path).await.unwrap();
        git::init(&repo_path).await.unwrap();
        fs::write(repo_path.join("README.md"), "# Test\n")
            .await
            .unwrap();
        git::commit_all(&repo_path, "initial commit").await.unwrap();

        (dir, repo_path)
    }

    #[tokio::test]
    async fn test_create_worktree() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());

        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();

        assert_eq!(
            worktree_path,
            workspace_dir.path().join("task-1").join("repo")
        );
        assert!(fs::try_exists(worktree_path.join("README.md"))
            .await
            .unwrap());
        let branches = git::list_branches(&repo_path).await.unwrap();
        assert!(branches.branches.contains(&task_branch_name("task-1")));

        let sha = git::get_current_sha(&worktree_path).await.unwrap();
        assert_eq!(sha.len(), 40);
    }

    #[tokio::test]
    async fn test_path_validation() {
        let workspace_dir = TempDir::new().unwrap();
        let worktree_root = workspace_dir.path().join("worktree");
        let inside = worktree_root.join("src").join("lib.rs");
        let outside = workspace_dir.path().join("outside.txt");

        fs::create_dir_all(inside.parent().unwrap()).await.unwrap();
        fs::write(&inside, "").await.unwrap();
        fs::write(&outside, "").await.unwrap();

        WorkspaceManager::validate_path(&worktree_root, &inside).unwrap();
        assert!(matches!(
            WorkspaceManager::validate_path(&worktree_root, &outside),
            Err(WorkspaceError::PathEscape)
        ));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let task_root = workspace_dir.path().join("task-1");
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        fs::create_dir_all(worktree_path.join("target"))
            .await
            .unwrap();
        fs::write(worktree_path.join("target/build-output"), "build output")
            .await
            .unwrap();

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();
        assert!(!fs::try_exists(&task_root).await.unwrap());
        assert!(git::branch_exists(&repo_path, &task_branch_name("task-1"))
            .await
            .unwrap());
        let output = git_command()
            .args(["worktree", "list", "--porcelain"])
            .current_dir(&repo_path)
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(worktree_path.to_str().unwrap()));
        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_preserves_user_worktree_registrations() {
        for missing in [false, true] {
            let (_repo_dir, repo_path) = setup_repo().await;
            let workspace_dir = TempDir::new().unwrap();
            let manager = WorkspaceManager::new(workspace_dir.path().join("forge"));
            let worktree_path = manager
                .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
                .await
                .unwrap();
            let user_worktree = workspace_dir.path().join("user-worktree");
            let output = git_command()
                .args(["worktree", "add", "--detach"])
                .arg(&user_worktree)
                .arg("HEAD")
                .current_dir(&repo_path)
                .output()
                .await
                .unwrap();
            assert!(output.status.success());
            fs::remove_dir_all(&user_worktree).await.unwrap();
            if missing {
                fs::remove_dir_all(&worktree_path).await.unwrap();
            }
            manager
                .cleanup_worktree("task-1", &repo_path, &worktree_path)
                .await
                .unwrap();
            let output = git_command()
                .args(["worktree", "list", "--porcelain"])
                .current_dir(&repo_path)
                .output()
                .await
                .unwrap();
            assert!(output.status.success());
            let registrations = String::from_utf8_lossy(&output.stdout);
            assert!(registrations.contains(user_worktree.to_str().unwrap()));
            assert!(!registrations.contains(worktree_path.to_str().unwrap()));
        }
    }

    async fn git_ok(cwd: &Path, args: &[&str]) -> String {
        let output = git_command()
            .args(args)
            .current_dir(cwd)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    async fn registrations(repo_path: &Path) -> String {
        git_ok(repo_path, &["worktree", "list", "--porcelain"]).await
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_reclaims_read_only_files_directories_and_broken_copies() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let outside = workspace_dir.path().join("outside");
        fs::create_dir_all(&outside).await.unwrap();
        fs::write(outside.join("kept"), "kept").await.unwrap();
        set_mode(&outside.join("kept"), 0o400);
        let manager = WorkspaceManager::new(workspace_dir.path().join("forge"));
        let task_root = workspace_dir.path().join("forge").join("task-1");
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        // A module cache the toolchain wrote read-only, inside the worktree.
        let cache = worktree_path.join("pkg/mod");
        fs::create_dir_all(&cache).await.unwrap();
        fs::write(cache.join("module.go"), "package module\n")
            .await
            .unwrap();
        std::os::unix::fs::symlink(&outside, cache.join("escape")).unwrap();
        // A copy an earlier recovery moved aside, and an outbox beside it.
        let broken = task_root.join("repo.broken-1700000000000");
        fs::create_dir_all(broken.join("target")).await.unwrap();
        fs::write(broken.join("target/output"), "output")
            .await
            .unwrap();
        fs::create_dir_all(task_root.join(".forge-outbox/execution-1"))
            .await
            .unwrap();
        for (path, mode) in [
            (cache.join("module.go"), 0o400),
            (cache.clone(), 0o500),
            (worktree_path.join("pkg"), 0o500),
            (broken.join("target/output"), 0o400),
            (broken.join("target"), 0o500),
            (broken.clone(), 0o500),
        ] {
            set_mode(&path, mode);
        }

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!fs::try_exists(&task_root).await.unwrap());
        assert!(!registrations(&repo_path)
            .await
            .contains(worktree_path.to_str().unwrap()));
        // The link was removed, never followed.
        use std::os::unix::fs::PermissionsExt;
        let kept = std::fs::metadata(outside.join("kept")).unwrap();
        assert_eq!(kept.permissions().mode() & 0o777, 0o400);
        // Idempotent re-run.
        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cleanup_unregisters_a_worktree_whose_directory_is_gone() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        fs::remove_dir_all(workspace_dir.path().join("task-1"))
            .await
            .unwrap();
        assert!(registrations(&repo_path).await.contains("task-1"));

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!registrations(&repo_path).await.contains("task-1"));
    }

    #[tokio::test]
    async fn cleanup_removes_a_directory_whose_registration_is_gone() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        fs::remove_dir_all(repo_path.join(".git/worktrees"))
            .await
            .unwrap();
        assert!(fs::try_exists(worktree_path.join(".git")).await.unwrap());
        assert!(!registrations(&repo_path).await.contains("task-1"));

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!fs::try_exists(workspace_dir.path().join("task-1"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn cleanup_keeps_a_checkout_that_belongs_to_another_repository() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let (_other_dir, other_repo) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let worktree_path = manager
            .create_worktree(other_repo.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();

        let result = manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await;

        assert!(matches!(result, Err(WorkspaceError::Git(_))));
        assert!(fs::try_exists(worktree_path.join("README.md"))
            .await
            .unwrap());
        assert!(registrations(&other_repo).await.contains("task-1"));
    }

    #[tokio::test]
    async fn cleanup_removes_the_directory_when_the_repository_is_gone() {
        let (repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        drop(repo_dir);

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!fs::try_exists(workspace_dir.path().join("task-1"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn cleanup_prunes_broadly_only_in_a_forge_owned_repository_cache() {
        let workspace_dir = TempDir::new().unwrap();
        let root = workspace_dir.path().join("forge");
        let cache = root.join(".repos").join("repo-1");
        fs::create_dir_all(&cache).await.unwrap();
        git::init(&cache).await.unwrap();
        fs::write(cache.join("README.md"), "# Test\n")
            .await
            .unwrap();
        git::commit_all(&cache, "initial commit").await.unwrap();
        let manager = WorkspaceManager::new(root.clone());
        let worktree_path = manager
            .create_worktree_named(cache.to_str().unwrap(), "task-1", "repo", "HEAD")
            .await
            .unwrap();
        let stale = workspace_dir.path().join("stale");
        git_ok(
            &cache,
            &[
                "worktree",
                "add",
                "--detach",
                stale.to_str().unwrap(),
                "HEAD",
            ],
        )
        .await;
        fs::remove_dir_all(&stale).await.unwrap();

        manager
            .cleanup_worktree("task-1", &cache, &worktree_path)
            .await
            .unwrap();

        let registrations = registrations(&cache).await;
        assert!(!registrations.contains(stale.to_str().unwrap()));
        assert!(!registrations.contains("task-1"));
    }

    #[tokio::test]
    async fn cleanup_refuses_an_id_that_is_not_one_task_root() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let root = workspace_dir.path().join("forge");
        let manager = WorkspaceManager::new(root.clone());
        let kept = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        let cache = root.join(".repos").join("repo-1");
        fs::create_dir_all(&cache).await.unwrap();
        let outside = workspace_dir.path().join("outside");
        fs::create_dir_all(outside.join("repo")).await.unwrap();
        fs::write(outside.join("repo/kept"), "kept").await.unwrap();

        let outside_id = outside.to_str().unwrap();
        for (task_id, worktree_path) in [
            // The workspace root itself.
            ("", root.join("repo")),
            (".", root.join("repo")),
            (".", root.join(".").join("repo")),
            // Above the root, and an absolute id that replaces it.
            ("..", root.join("..").join("repo")),
            ("..", workspace_dir.path().join("repo")),
            (outside_id, outside.join("repo")),
            ("/", PathBuf::from("/repo")),
            // Forge's own directories under the root, and a nested id.
            (".repos", cache.clone()),
            (".forge", root.join(".forge").join("logs")),
            ("task-1/repo", kept.join("src")),
            // A worktree path that is the Task root, or climbs out of it.
            ("task-1", root.join("task-1")),
            ("task-1", root.join("task-1").join("..")),
            ("task-1", root.join("task-1").join("repo").join("..")),
            ("task-1", root.join("task-2").join("repo")),
            ("task-1", PathBuf::new()),
            ("task-1", PathBuf::from("repo")),
        ] {
            let result = manager
                .cleanup_worktree(task_id, &repo_path, &worktree_path)
                .await;
            assert!(
                matches!(result, Err(WorkspaceError::PathEscape)),
                "{task_id:?} {worktree_path:?}: {result:?}"
            );
        }

        assert!(fs::try_exists(kept.join("README.md")).await.unwrap());
        assert!(fs::try_exists(&cache).await.unwrap());
        assert!(fs::try_exists(outside.join("repo/kept")).await.unwrap());
        assert!(registrations(&repo_path).await.contains("task-1"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_refuses_a_task_root_or_worktree_that_is_a_link() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let root = workspace_dir.path().join("forge");
        fs::create_dir_all(&root).await.unwrap();
        let manager = WorkspaceManager::new(root.clone());
        // The user's own directory, holding a worktree of the same repository
        // that the user registered, with uncommitted work in it.
        let users = workspace_dir.path().join("users");
        fs::create_dir_all(&users).await.unwrap();
        let user_worktree = users.join("repo");
        git_ok(
            &repo_path,
            &[
                "worktree",
                "add",
                "-b",
                "feature/mine",
                user_worktree.to_str().unwrap(),
                "HEAD",
            ],
        )
        .await;
        fs::write(user_worktree.join("uncommitted"), "mine")
            .await
            .unwrap();
        set_mode(&user_worktree.join("uncommitted"), 0o400);

        // The Task root is a link to the user's directory.
        std::os::unix::fs::symlink(&users, root.join("task-1")).unwrap();
        let result = manager
            .cleanup_worktree("task-1", &repo_path, &root.join("task-1").join("repo"))
            .await;
        assert!(matches!(result, Err(WorkspaceError::PathEscape)));

        // The Task root is real; the worktree path is a link to the user's.
        fs::create_dir_all(root.join("task-2")).await.unwrap();
        std::os::unix::fs::symlink(&user_worktree, root.join("task-2").join("repo")).unwrap();
        let result = manager
            .cleanup_worktree("task-2", &repo_path, &root.join("task-2").join("repo"))
            .await;
        assert!(matches!(result, Err(WorkspaceError::PathEscape)));

        use std::os::unix::fs::PermissionsExt;
        let uncommitted = user_worktree.join("uncommitted");
        assert_eq!(fs::read_to_string(&uncommitted).await.unwrap(), "mine");
        assert_eq!(
            std::fs::metadata(&uncommitted)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
        assert!(registrations(&repo_path)
            .await
            .contains(user_worktree.canonicalize().unwrap().to_str().unwrap()));
        assert!(git::branch_exists(&repo_path, "feature/mine")
            .await
            .unwrap());
        assert!(fs::symlink_metadata(root.join("task-1")).await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_leaves_the_target_of_a_link_inside_the_tree_alone() {
        use std::os::unix::fs::PermissionsExt;

        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let outside = workspace_dir.path().join("outside");
        fs::create_dir_all(outside.join("dir")).await.unwrap();
        fs::write(outside.join("file"), "kept").await.unwrap();
        fs::write(outside.join("dir/inner"), "kept").await.unwrap();
        set_mode(&outside.join("file"), 0o400);
        set_mode(&outside.join("dir/inner"), 0o400);
        set_mode(&outside.join("dir"), 0o500);
        let manager = WorkspaceManager::new(workspace_dir.path().join("forge"));
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        let locked = worktree_path.join("locked");
        fs::create_dir_all(&locked).await.unwrap();
        std::os::unix::fs::symlink(outside.join("file"), locked.join("file-link")).unwrap();
        std::os::unix::fs::symlink(outside.join("dir"), locked.join("dir-link")).unwrap();
        set_mode(&locked, 0o500);

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!fs::try_exists(workspace_dir.path().join("forge/task-1"))
            .await
            .unwrap());
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&outside.join("file")), 0o400);
        assert_eq!(mode(&outside.join("dir")), 0o500);
        assert_eq!(mode(&outside.join("dir/inner")), 0o400);
        assert_eq!(
            fs::read_to_string(outside.join("file")).await.unwrap(),
            "kept"
        );
        assert_eq!(
            fs::read_to_string(outside.join("dir/inner")).await.unwrap(),
            "kept"
        );
        set_mode(&outside.join("dir"), 0o700);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_unregisters_a_missing_worktree_behind_a_linked_workspace_root() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let real_root = workspace_dir.path().join("real");
        fs::create_dir_all(&real_root).await.unwrap();
        let linked_root = workspace_dir.path().join("linked");
        std::os::unix::fs::symlink(&real_root, &linked_root).unwrap();
        let manager = WorkspaceManager::new(linked_root.clone());
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), "task-1", "HEAD")
            .await
            .unwrap();
        // Git records the resolved path, which the missing directory can no
        // longer be resolved to.
        fs::remove_dir_all(real_root.join("task-1")).await.unwrap();
        assert!(registrations(&repo_path).await.contains("task-1"));

        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();

        assert!(!registrations(&repo_path).await.contains("task-1"));
        // And with the directory present, through the link.
        let worktree_path = manager
            .recover_worktree(
                repo_path.to_str().unwrap(),
                "task-1",
                &task_branch_name("task-1"),
            )
            .await
            .unwrap();
        manager
            .cleanup_worktree("task-1", &repo_path, &worktree_path)
            .await
            .unwrap();
        assert!(!fs::try_exists(real_root.join("task-1")).await.unwrap());
        assert!(!registrations(&repo_path).await.contains("task-1"));
        assert!(fs::try_exists(&real_root).await.unwrap());
    }

    #[tokio::test]
    async fn cleanup_keeps_a_full_clone_and_a_repository_inside_the_task_root() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let root = workspace_dir.path().to_path_buf();
        let manager = WorkspaceManager::new(root.clone());

        // A repository of its own (`.git` is a directory) at the recorded path.
        let clone = root.join("task-1").join("repo");
        fs::create_dir_all(&clone).await.unwrap();
        git::init(&clone).await.unwrap();
        fs::write(clone.join("work.txt"), "work").await.unwrap();
        let result = manager.cleanup_worktree("task-1", &repo_path, &clone).await;
        assert!(matches!(result, Err(WorkspaceError::Git(_))));
        assert!(fs::try_exists(clone.join("work.txt")).await.unwrap());

        // The workspace's repository is the recorded path itself (a row that
        // names the user's primary checkout), or lives under the Task root.
        for worktree_path in [clone.clone(), root.join("task-1").join("other")] {
            let result = manager
                .cleanup_worktree("task-1", &clone, &worktree_path)
                .await;
            assert!(matches!(result, Err(WorkspaceError::PathEscape)));
        }
        assert!(fs::try_exists(clone.join("work.txt")).await.unwrap());
        assert!(fs::try_exists(clone.join(".git")).await.unwrap());
    }

    async fn commit_on_task_branch(manager: &WorkspaceManager, repo_path: &Path, task_id: &str) {
        let worktree_path = manager
            .create_worktree(repo_path.to_str().unwrap(), task_id, "HEAD")
            .await
            .unwrap();
        fs::write(worktree_path.join(format!("{task_id}.txt")), task_id)
            .await
            .unwrap();
        git::commit_all(&worktree_path, "task change")
            .await
            .unwrap();
        manager
            .cleanup_worktree(task_id, repo_path, &worktree_path)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn task_branch_is_deleted_only_when_its_tip_is_in_the_target() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let target = git_ok(&repo_path, &["rev-parse", "--abbrev-ref", "HEAD"]).await;
        let branch = task_branch_name("task-1");
        commit_on_task_branch(&manager, &repo_path, "task-1").await;
        let tip = git_ok(&repo_path, &["rev-parse", &branch]).await;

        // Undelivered: the commit exists only on the Task branch.
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, &target)
                .await
                .unwrap(),
            TaskBranchReclaim::Undelivered
        );
        assert!(git::branch_exists(&repo_path, &branch).await.unwrap());
        // No target to compare against: delivery cannot be proven.
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, "no-such-target")
                .await
                .unwrap(),
            TaskBranchReclaim::TargetMissing
        );

        // Delivered, then the target is moved back: no longer an ancestor.
        let before_merge = git_ok(&repo_path, &["rev-parse", "HEAD"]).await;
        git_ok(&repo_path, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        let delivered_target = git_ok(&repo_path, &["rev-parse", "HEAD"]).await;
        git_ok(&repo_path, &["reset", "--hard", &before_merge]).await;
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, &target)
                .await
                .unwrap(),
            TaskBranchReclaim::Undelivered
        );
        assert!(git::branch_exists(&repo_path, &branch).await.unwrap());

        // Delivered and still contained in the target.
        git_ok(&repo_path, &["reset", "--hard", &delivered_target]).await;
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, &target)
                .await
                .unwrap(),
            TaskBranchReclaim::Deleted { tip }
        );
        assert!(!git::branch_exists(&repo_path, &branch).await.unwrap());
        // Idempotent re-run.
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, &target)
                .await
                .unwrap(),
            TaskBranchReclaim::Absent
        );
    }

    #[tokio::test]
    async fn branch_reclaim_never_touches_user_branches_or_checked_out_branches() {
        let (_repo_dir, repo_path) = setup_repo().await;
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().join("forge"));
        let target = git_ok(&repo_path, &["rev-parse", "--abbrev-ref", "HEAD"]).await;

        // A merged branch the user created, and the target itself.
        git_ok(&repo_path, &["branch", "feature/mine"]).await;
        for name in ["feature/mine", target.as_str()] {
            assert_eq!(
                delete_delivered_task_branch(&repo_path, name, &target)
                    .await
                    .unwrap(),
                TaskBranchReclaim::NotATaskBranch
            );
            assert!(git::branch_exists(&repo_path, name).await.unwrap());
        }

        // A delivered Task branch the user has checked out in their own worktree.
        let branch = task_branch_name("task-1");
        commit_on_task_branch(&manager, &repo_path, "task-1").await;
        git_ok(&repo_path, &["merge", "--no-ff", "-m", "deliver", &branch]).await;
        let user_worktree = workspace_dir.path().join("user-worktree");
        git_ok(
            &repo_path,
            &["worktree", "add", user_worktree.to_str().unwrap(), &branch],
        )
        .await;
        assert_eq!(
            delete_delivered_task_branch(&repo_path, &branch, &target)
                .await
                .unwrap(),
            TaskBranchReclaim::CheckedOut
        );
        assert!(git::branch_exists(&repo_path, &branch).await.unwrap());
        assert!(registrations(&repo_path)
            .await
            .contains(user_worktree.to_str().unwrap()));
    }
}
