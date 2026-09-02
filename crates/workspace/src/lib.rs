#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
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

    #[error("workspace is locked")]
    Locked,

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

    pub async fn acquire_lock(&self, task_id: &str) -> Result<()> {
        let task_root = self.root.join(task_id);
        fs::create_dir_all(&task_root).await?;

        let lock_path = task_root.join(".forge.lock");
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(WorkspaceError::Locked)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn release_lock(&self, task_id: &str) -> Result<()> {
        let lock_path = self.root.join(task_id).join(".forge.lock");
        match fs::remove_file(lock_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(WorkspaceError::NotFound)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn cleanup_worktree(&self, task_id: &str) -> Result<()> {
        let task_root = self.root.join(task_id);
        match fs::remove_dir_all(task_root).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(WorkspaceError::NotFound)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn detect_orphans(&self, active_task_ids: &[String]) -> Result<Vec<String>> {
        let active_task_ids = active_task_ids.iter().collect::<HashSet<_>>();
        let mut orphans = Vec::new();

        let mut entries = match fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(orphans),
            Err(error) => return Err(error.into()),
        };

        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }

            let task_id = entry.file_name().to_string_lossy().to_string();
            if !active_task_ids.contains(&task_id) {
                orphans.push(task_id);
            }
        }

        orphans.sort();
        Ok(orphans)
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
    format!("task/{}", &task_id[..task_id.len().min(8)])
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
    async fn test_lock_unlock() {
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());

        manager.acquire_lock("task-1").await.unwrap();
        assert!(matches!(
            manager.acquire_lock("task-1").await,
            Err(WorkspaceError::Locked)
        ));

        manager.release_lock("task-1").await.unwrap();
        manager.acquire_lock("task-1").await.unwrap();
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
    async fn test_orphan_detection() {
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());

        fs::create_dir_all(workspace_dir.path().join("active"))
            .await
            .unwrap();
        fs::create_dir_all(workspace_dir.path().join("orphan-a"))
            .await
            .unwrap();
        fs::create_dir_all(workspace_dir.path().join("orphan-b"))
            .await
            .unwrap();
        fs::write(workspace_dir.path().join("not-a-task"), "")
            .await
            .unwrap();

        let active = vec!["active".to_string()];
        let orphans = manager.detect_orphans(&active).await.unwrap();

        assert_eq!(orphans, vec!["orphan-a", "orphan-b"]);
    }

    #[tokio::test]
    async fn test_cleanup() {
        let workspace_dir = TempDir::new().unwrap();
        let manager = WorkspaceManager::new(workspace_dir.path().to_path_buf());
        let task_root = workspace_dir.path().join("task-1");

        fs::create_dir_all(task_root.join("repo")).await.unwrap();
        fs::write(task_root.join("repo").join("README.md"), "")
            .await
            .unwrap();

        manager.cleanup_worktree("task-1").await.unwrap();
        assert!(!fs::try_exists(&task_root).await.unwrap());
    }
}
