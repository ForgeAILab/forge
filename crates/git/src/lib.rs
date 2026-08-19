#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::process::Command;

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("git command failed: {command}\nstdout: {stdout}\nstderr: {stderr}")]
    CommandFailed {
        command: String,
        stdout: String,
        stderr: String,
    },

    #[error("merge conflict in {path}: {stderr}")]
    MergeConflict { path: String, stderr: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, GitError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchList {
    pub branches: Vec<String>,
    pub default_branch: Option<String>,
    pub origin_url: Option<String>,
}

async fn run_git(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;

    if !output.status.success() {
        return Err(GitError::CommandFailed {
            command: format!("git {}", args.join(" ")),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub async fn is_git_repo(path: &Path) -> bool {
    tokio::fs::symlink_metadata(path.join(".git")).await.is_ok()
}

pub async fn list_branches(path: &Path) -> Result<BranchList> {
    let output = run_git(
        path,
        &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
    )
    .await?;
    let branches = output
        .lines()
        .map(str::trim)
        .filter(|branch| !branch.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    let default_branch = match run_git(path, &["symbolic-ref", "--short", "HEAD"]).await {
        Ok(branch) if !branch.is_empty() => Some(branch),
        Ok(_) => None,
        Err(GitError::CommandFailed { .. }) => None,
        Err(error) => return Err(error),
    };
    let origin_url = match run_git(path, &["remote", "get-url", "origin"]).await {
        Ok(url) if !url.is_empty() => Some(url),
        Ok(_) => None,
        Err(GitError::CommandFailed { .. }) => None,
        Err(error) => return Err(error),
    };

    Ok(BranchList {
        branches,
        default_branch,
        origin_url,
    })
}

/// Create a new worktree from an existing repo.
pub async fn create_worktree(
    repo_path: &Path,
    branch_name: &str,
    worktree_path: &Path,
) -> Result<()> {
    run_git(
        repo_path,
        &[
            "worktree",
            "add",
            "-b",
            branch_name,
            &worktree_path.to_string_lossy(),
        ],
    )
    .await?;
    Ok(())
}

/// Remove a worktree.
pub async fn remove_worktree(repo_path: &Path, worktree_path: &Path) -> Result<()> {
    run_git(
        repo_path,
        &[
            "worktree",
            "remove",
            "--force",
            &worktree_path.to_string_lossy(),
        ],
    )
    .await?;
    Ok(())
}

/// Get the current HEAD SHA.
pub async fn get_current_sha(worktree_path: &Path) -> Result<String> {
    run_git(worktree_path, &["rev-parse", "HEAD"]).await
}

/// Check if the worktree has no uncommitted changes.
pub async fn is_worktree_clean(worktree_path: &Path) -> Result<bool> {
    let output = run_git(worktree_path, &["status", "--porcelain"]).await?;
    Ok(output.is_empty())
}

/// List uncommitted worktree changes using git porcelain output.
pub async fn status_porcelain(worktree_path: &Path) -> Result<Vec<String>> {
    let output = run_git(worktree_path, &["status", "--porcelain"]).await?;
    Ok(output
        .lines()
        .map(|line| line.get(3..).unwrap_or(line).trim().to_owned())
        .filter(|line| !line.is_empty())
        .collect())
}

/// Restore a managed worktree to an exact commit and remove untracked files.
///
/// This enforces read-only execution roles even when an underlying CLI creates
/// commits automatically.
pub async fn restore_worktree(worktree_path: &Path, commit_sha: &str) -> Result<()> {
    run_git(worktree_path, &["reset", "--hard", commit_sha]).await?;
    run_git(worktree_path, &["clean", "-fd"]).await?;
    Ok(())
}

/// Check out a branch in a repo or worktree.
pub async fn branch_exists(repo_path: &Path, branch_name: &str) -> Result<bool> {
    let result = run_git(
        repo_path,
        &[
            "rev-parse",
            "--verify",
            &format!("refs/heads/{branch_name}"),
        ],
    )
    .await;
    match result {
        Ok(_) => Ok(true),
        Err(GitError::CommandFailed { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

pub async fn checkout_branch(repo_path: &Path, branch_name: &str) -> Result<()> {
    run_git(repo_path, &["checkout", branch_name]).await?;
    Ok(())
}

/// Attempt to merge a branch into the current worktree HEAD.
pub async fn merge(worktree_path: &Path, target_branch: &str) -> Result<()> {
    let result = run_git(worktree_path, &["merge", target_branch, "--no-edit"]).await;
    match result {
        Ok(_) => Ok(()),
        Err(GitError::CommandFailed { stdout, stderr, .. })
            if stdout.contains("CONFLICT") || stderr.contains("CONFLICT") =>
        {
            let details = if stderr.trim().is_empty() {
                stdout
            } else {
                stderr
            };
            Err(GitError::MergeConflict {
                path: worktree_path.to_string_lossy().to_string(),
                stderr: details,
            })
        }
        Err(e) => Err(e),
    }
}

/// Attempt to merge a branch into the currently checked-out branch.
pub async fn merge_branch_into(repo_path: &Path, branch_name: &str) -> Result<()> {
    merge(repo_path, branch_name).await
}

/// Abort an in-progress merge.
pub async fn abort_merge(worktree_path: &Path) -> Result<()> {
    run_git(worktree_path, &["merge", "--abort"]).await?;
    Ok(())
}

/// Detect if there is an interrupted merge.
pub async fn detect_interrupted_merge(worktree_path: &Path) -> Result<bool> {
    // For worktrees, .git is a file pointing to the actual git dir.
    // Check via git rev-parse instead.
    let result = Command::new("git")
        .args(["rev-parse", "--verify", "MERGE_HEAD"])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;
    Ok(result.status.success())
}

/// Get diff between a base SHA and HEAD.
pub async fn get_diff(worktree_path: &Path, base_sha: &str) -> Result<String> {
    run_git(worktree_path, &["diff", &format!("{base_sha}..HEAD")]).await
}

/// Stage all worktree changes.
pub async fn stage_all(path: &Path) -> Result<()> {
    run_git(path, &["add", "-A"]).await?;
    Ok(())
}

/// Commit currently staged changes.
pub async fn commit_with_message(path: &Path, message: &str) -> Result<String> {
    run_git(path, &["commit", "-m", message]).await?;
    get_current_sha(path).await
}

/// Count commits in the half-open range before_sha..after_sha.
pub async fn count_commits_between(path: &Path, before_sha: &str, after_sha: &str) -> Result<u64> {
    let range = format!("{before_sha}..{after_sha}");
    let output = run_git(path, &["rev-list", "--count", &range]).await?;
    output
        .parse::<u64>()
        .map_err(|error| GitError::CommandFailed {
            command: format!("git rev-list --count {range}"),
            stdout: output,
            stderr: error.to_string(),
        })
}

// ---------------------------------------------------------------------------
// Rebase
// ---------------------------------------------------------------------------

/// Rebase the current branch onto `onto_branch`.
pub async fn rebase(worktree_path: &Path, onto_branch: &str) -> Result<()> {
    let result = run_git(worktree_path, &["rebase", onto_branch]).await;
    match result {
        Ok(_) => Ok(()),
        Err(GitError::CommandFailed { stdout, stderr, .. })
            if stdout.contains("CONFLICT")
                || stderr.contains("CONFLICT")
                || stderr.contains("could not apply") =>
        {
            let details = if stderr.trim().is_empty() {
                stdout
            } else {
                stderr
            };
            Err(GitError::MergeConflict {
                path: worktree_path.to_string_lossy().to_string(),
                stderr: details,
            })
        }
        Err(e) => Err(e),
    }
}

/// Abort an in-progress rebase.
pub async fn abort_rebase(worktree_path: &Path) -> Result<()> {
    run_git(worktree_path, &["rebase", "--abort"]).await?;
    Ok(())
}

/// Continue a paused rebase (after conflict resolution).
pub async fn continue_rebase(worktree_path: &Path) -> Result<()> {
    run_git(worktree_path, &["rebase", "--continue"]).await?;
    Ok(())
}

/// Detect if a rebase is in progress via `git rev-parse`.
pub async fn detect_rebase_in_progress(worktree_path: &Path) -> Result<bool> {
    let result = Command::new("git")
        .args(["rev-parse", "--git-path", "rebase-merge"])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;
    if result.status.success() {
        let git_path = String::from_utf8_lossy(&result.stdout).trim().to_string();
        let abs = if Path::new(&git_path).is_absolute() {
            PathBuf::from(&git_path)
        } else {
            worktree_path.join(&git_path)
        };
        if tokio::fs::symlink_metadata(&abs).await.is_ok() {
            return Ok(true);
        }
    }

    let result = Command::new("git")
        .args(["rev-parse", "--git-path", "rebase-apply"])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;
    if result.status.success() {
        let git_path = String::from_utf8_lossy(&result.stdout).trim().to_string();
        let abs = if Path::new(&git_path).is_absolute() {
            PathBuf::from(&git_path)
        } else {
            worktree_path.join(&git_path)
        };
        if tokio::fs::symlink_metadata(&abs).await.is_ok() {
            return Ok(true);
        }
    }

    Ok(false)
}

// ---------------------------------------------------------------------------
// Conflict helpers
// ---------------------------------------------------------------------------

/// The kind of conflict operation currently in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictOperation {
    Merge,
    Rebase,
    None,
}

/// Detect what conflict operation is in progress (if any).
pub async fn detect_conflict_state(worktree_path: &Path) -> Result<ConflictOperation> {
    if detect_interrupted_merge(worktree_path).await? {
        return Ok(ConflictOperation::Merge);
    }
    if detect_rebase_in_progress(worktree_path).await? {
        return Ok(ConflictOperation::Rebase);
    }
    Ok(ConflictOperation::None)
}

/// List paths with unresolved conflicts.
pub async fn conflict_paths(worktree_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .args(["diff", "--name-only", "--diff-filter=U"])
        .current_dir(worktree_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect())
}

/// Abort whatever conflict operation is in progress.
pub async fn abort_conflict(worktree_path: &Path) -> Result<()> {
    match detect_conflict_state(worktree_path).await? {
        ConflictOperation::Merge => abort_merge(worktree_path).await,
        ConflictOperation::Rebase => abort_rebase(worktree_path).await,
        ConflictOperation::None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

/// Fetch from a remote (defaults to "origin").
pub async fn fetch(repo_path: &Path, remote: Option<&str>) -> Result<()> {
    let remote = remote.unwrap_or("origin");
    run_git(repo_path, &["fetch", remote]).await?;
    Ok(())
}

/// Fetch a specific branch from a remote.
pub async fn fetch_branch(repo_path: &Path, remote: &str, branch: &str) -> Result<()> {
    run_git(repo_path, &["fetch", remote, branch]).await?;
    Ok(())
}

/// Pull from the configured upstream using `--ff-only` to avoid creating
/// surprise merge commits. Returns the trimmed stdout from git.
pub async fn pull_ff_only(repo_path: &Path) -> Result<String> {
    run_git(repo_path, &["pull", "--ff-only"]).await
}

/// Push the current branch to its upstream (no force).
pub async fn push(repo_path: &Path) -> Result<String> {
    run_git(repo_path, &["push"]).await
}

// ---------------------------------------------------------------------------
// Testing
// ---------------------------------------------------------------------------

/// Initialize a new git repo (for testing).
pub async fn init(path: &Path) -> Result<()> {
    run_git(path, &["init"]).await?;
    // Set required config for commits
    run_git(path, &["config", "user.email", "test@forge.dev"]).await?;
    run_git(path, &["config", "user.name", "Forge Test"]).await?;
    Ok(())
}

/// Rename the currently checked-out branch (e.g. normalize a fresh repo's
/// init.defaultBranch to the branch Forge registered).
pub async fn rename_current_branch(repo_path: &Path, new_name: &str) -> Result<()> {
    run_git(repo_path, &["branch", "-m", new_name]).await?;
    Ok(())
}

/// Stage all files and commit.
pub async fn commit_all(path: &Path, message: &str) -> Result<String> {
    stage_all(path).await?;
    run_git(path, &["commit", "-m", message, "--allow-empty"]).await?;
    get_current_sha(path).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    async fn setup_repo() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let repo_path = dir.path().to_path_buf();
        init(&repo_path).await.unwrap();

        // Create initial commit
        fs::write(repo_path.join("README.md"), "# Test")
            .await
            .unwrap();
        commit_all(&repo_path, "initial commit").await.unwrap();

        (dir, repo_path)
    }

    #[tokio::test]
    async fn is_git_repo_true() {
        let dir = TempDir::new().unwrap();
        init(dir.path()).await.unwrap();

        assert!(is_git_repo(dir.path()).await);
    }

    #[tokio::test]
    async fn is_git_repo_false() {
        let dir = TempDir::new().unwrap();

        assert!(!is_git_repo(dir.path()).await);
    }

    #[tokio::test]
    async fn is_git_repo_worktree() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(".git"), "gitdir: /some/path")
            .await
            .unwrap();

        assert!(is_git_repo(dir.path()).await);
    }

    #[tokio::test]
    async fn is_git_repo_subfolder_of_repo_is_false() {
        let dir = TempDir::new().unwrap();
        let status = std::process::Command::new("git")
            .arg("init")
            .current_dir(dir.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .status()
            .unwrap();
        assert!(status.success());

        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).await.unwrap();

        assert!(!is_git_repo(&subdir).await);
    }

    #[tokio::test]
    async fn list_branches_basic() {
        let dir = TempDir::new().unwrap();
        let repo_path = dir.path().to_path_buf();
        init(&repo_path).await.unwrap();
        fs::write(repo_path.join("README.md"), "# Test")
            .await
            .unwrap();
        commit_all(&repo_path, "initial commit").await.unwrap();

        let initial_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"])
            .await
            .unwrap();
        let extra_branch = "extra";
        run_git(&repo_path, &["branch", extra_branch])
            .await
            .unwrap();

        let branch_list = list_branches(&repo_path).await.unwrap();

        assert!(branch_list.branches.contains(&initial_branch));
        assert!(branch_list.branches.contains(&extra_branch.to_owned()));
        assert_eq!(branch_list.default_branch, Some(initial_branch));
    }

    #[tokio::test]
    async fn test_create_worktree_and_get_sha() {
        let (dir, repo_path) = setup_repo().await;
        let worktree_path = dir.path().join("worktree1");

        create_worktree(&repo_path, "forge/task-1", &worktree_path)
            .await
            .unwrap();

        let sha = get_current_sha(&worktree_path).await.unwrap();
        assert!(!sha.is_empty());
        assert_eq!(sha.len(), 40); // SHA-1 hex

        let clean = is_worktree_clean(&worktree_path).await.unwrap();
        assert!(clean);

        // Write a file in worktree
        fs::write(worktree_path.join("new_file.txt"), "hello")
            .await
            .unwrap();

        let dirty = is_worktree_clean(&worktree_path).await.unwrap();
        assert!(!dirty);

        // Commit and get new SHA
        let new_sha = commit_all(&worktree_path, "add file").await.unwrap();
        assert_ne!(sha, new_sha);

        // Get diff
        let diff = get_diff(&worktree_path, &sha).await.unwrap();
        assert!(diff.contains("new_file.txt"));

        // Cleanup
        remove_worktree(&repo_path, &worktree_path).await.unwrap();
    }

    #[tokio::test]
    async fn restore_worktree_discards_commits_and_untracked_files() {
        let (_dir, repo_path) = setup_repo().await;
        let original_sha = get_current_sha(&repo_path).await.unwrap();

        fs::write(repo_path.join("README.md"), "changed by reviewer")
            .await
            .unwrap();
        fs::write(repo_path.join("reviewer.tmp"), "untracked")
            .await
            .unwrap();
        commit_all(&repo_path, "reviewer mutation").await.unwrap();
        fs::write(repo_path.join("leftover.tmp"), "untracked")
            .await
            .unwrap();

        restore_worktree(&repo_path, &original_sha).await.unwrap();

        assert_eq!(get_current_sha(&repo_path).await.unwrap(), original_sha);
        assert_eq!(
            fs::read_to_string(repo_path.join("README.md"))
                .await
                .unwrap(),
            "# Test"
        );
        assert!(!repo_path.join("reviewer.tmp").exists());
        assert!(!repo_path.join("leftover.tmp").exists());
        assert!(is_worktree_clean(&repo_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_merge_conflict() {
        let (dir, repo_path) = setup_repo().await;

        // Create two branches with conflicting changes
        let wt1 = dir.path().join("wt1");
        create_worktree(&repo_path, "branch-a", &wt1).await.unwrap();
        fs::write(wt1.join("conflict.txt"), "branch a content")
            .await
            .unwrap();
        commit_all(&wt1, "branch a change").await.unwrap();

        let wt2 = dir.path().join("wt2");
        create_worktree(&repo_path, "branch-b", &wt2).await.unwrap();
        fs::write(wt2.join("conflict.txt"), "branch b content")
            .await
            .unwrap();
        commit_all(&wt2, "branch b change").await.unwrap();

        // Merge branch-a into branch-b should conflict
        let result = merge(&wt2, "branch-a").await;
        assert!(result.is_err());

        // Detect interrupted merge
        let has_merge = detect_interrupted_merge(&wt2).await.unwrap();
        assert!(has_merge);

        // Abort merge
        abort_merge(&wt2).await.unwrap();

        let has_merge_after = detect_interrupted_merge(&wt2).await.unwrap();
        assert!(!has_merge_after);

        // Cleanup
        remove_worktree(&repo_path, &wt1).await.unwrap();
        remove_worktree(&repo_path, &wt2).await.unwrap();
    }

    #[tokio::test]
    async fn test_rebase_clean() {
        let (dir, repo_path) = setup_repo().await;

        let wt = dir.path().join("wt_rebase");
        create_worktree(&repo_path, "feature", &wt).await.unwrap();
        fs::write(wt.join("feature.txt"), "feature work")
            .await
            .unwrap();
        commit_all(&wt, "feature commit").await.unwrap();

        // Add a commit on main so there's something to rebase onto
        fs::write(repo_path.join("main_file.txt"), "main work")
            .await
            .unwrap();
        commit_all(&repo_path, "main commit").await.unwrap();

        // Rebase feature onto main (via the default branch ref)
        let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"])
            .await
            .unwrap();
        rebase(&wt, &default_branch).await.unwrap();

        assert!(!detect_rebase_in_progress(&wt).await.unwrap());
        assert!(wt.join("feature.txt").exists());

        remove_worktree(&repo_path, &wt).await.unwrap();
    }

    #[tokio::test]
    async fn test_rebase_conflict_and_abort() {
        let (dir, repo_path) = setup_repo().await;

        let wt = dir.path().join("wt_rebase_conflict");
        create_worktree(&repo_path, "feat-conflict", &wt)
            .await
            .unwrap();
        fs::write(wt.join("README.md"), "feature change")
            .await
            .unwrap();
        commit_all(&wt, "feature change").await.unwrap();

        // Conflicting commit on main
        fs::write(repo_path.join("README.md"), "main change")
            .await
            .unwrap();
        commit_all(&repo_path, "main change").await.unwrap();

        let default_branch = run_git(&repo_path, &["symbolic-ref", "--short", "HEAD"])
            .await
            .unwrap();
        let result = rebase(&wt, &default_branch).await;
        assert!(result.is_err());

        assert!(detect_rebase_in_progress(&wt).await.unwrap());
        assert_eq!(
            detect_conflict_state(&wt).await.unwrap(),
            ConflictOperation::Rebase
        );

        let paths = conflict_paths(&wt).await.unwrap();
        assert!(paths.contains(&"README.md".to_owned()));

        abort_rebase(&wt).await.unwrap();
        assert!(!detect_rebase_in_progress(&wt).await.unwrap());

        remove_worktree(&repo_path, &wt).await.unwrap();
    }

    #[tokio::test]
    async fn test_detect_conflict_state_merge() {
        let (dir, repo_path) = setup_repo().await;

        let wt1 = dir.path().join("wt_cs1");
        create_worktree(&repo_path, "cs-a", &wt1).await.unwrap();
        fs::write(wt1.join("conflict.txt"), "a").await.unwrap();
        commit_all(&wt1, "a").await.unwrap();

        let wt2 = dir.path().join("wt_cs2");
        create_worktree(&repo_path, "cs-b", &wt2).await.unwrap();
        fs::write(wt2.join("conflict.txt"), "b").await.unwrap();
        commit_all(&wt2, "b").await.unwrap();

        let _ = merge(&wt2, "cs-a").await;
        assert_eq!(
            detect_conflict_state(&wt2).await.unwrap(),
            ConflictOperation::Merge
        );

        abort_conflict(&wt2).await.unwrap();
        assert_eq!(
            detect_conflict_state(&wt2).await.unwrap(),
            ConflictOperation::None
        );

        remove_worktree(&repo_path, &wt1).await.unwrap();
        remove_worktree(&repo_path, &wt2).await.unwrap();
    }
}
