use std::{collections::HashMap, path::Path, process::Stdio, sync::Arc};

use api_types::{DiffFileStatus, DiffResponse, DiffStats, FileDiffSummary};
use db::{RepoRepo, SqliteDb, TaskRepo, Workspace, WorkspaceRepo, WorkspaceStatus};
use tokio::process::Command;

use crate::{
    workspace_backend::{DiffSpec, EmbeddedWorkspaceBackend, WorkspaceBackendRouter},
    MergeService, Result, ServiceError,
};

#[derive(Clone)]
pub struct DiffService {
    db: Arc<SqliteDb>,
    workspace_backend_router: Arc<WorkspaceBackendRouter>,
}

impl DiffService {
    pub fn new_with_router(
        db: Arc<SqliteDb>,
        workspace_backend_router: Arc<crate::workspace_backend::WorkspaceBackendRouter>,
    ) -> Self {
        Self {
            db,
            workspace_backend_router,
        }
    }

    #[cfg(test)]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self::new_for_test(db)
    }

    /// Embedded-only fixture constructor.
    pub fn new_for_test(db: Arc<SqliteDb>) -> Self {
        Self {
            workspace_backend_router: embedded_read_router_for_test(Arc::clone(&db)),
            db,
        }
    }

    pub fn with_workspace_backend_router(mut self, router: Arc<WorkspaceBackendRouter>) -> Self {
        self.workspace_backend_router = router;
        self
    }

    pub async fn task_diff(&self, task_id: &str) -> Result<DiffResponse> {
        let workspace = WorkspaceRepo::get_by_task_id(&*self.db, task_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", task_id.to_owned()))?;
        self.workspace_diff(&workspace.id).await
    }

    pub async fn workspace_diff(&self, workspace_id: &str) -> Result<DiffResponse> {
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        ensure_workspace_diffable(&workspace)?;
        self.workspace_diff_inner(&workspace).await
    }

    async fn workspace_diff_inner(&self, workspace: &Workspace) -> Result<DiffResponse> {
        let task = TaskRepo::get_by_id(&*self.db, &workspace.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", workspace.task_id.clone()))?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;

        let resolved = self
            .workspace_backend_router
            .resolve(&self.db, workspace)
            .await?;
        Ok(resolved
            .backend
            .diff(
                &resolved.placement,
                &DiffSpec {
                    base_ref: repo.default_branch,
                    head_ref: None,
                    max_bytes: usize::MAX,
                },
            )
            .await?
            .response)
    }
}

pub(crate) fn embedded_read_router_for_test(db: Arc<SqliteDb>) -> Arc<WorkspaceBackendRouter> {
    let root = config::legacy_temp_workspace_root();
    let merge_service = Arc::new(MergeService::new(
        Arc::clone(&db),
        Arc::new(events::EventBus::default()),
        root.clone(),
    ));
    Arc::new(WorkspaceBackendRouter::new(Arc::new(
        EmbeddedWorkspaceBackend::new(db, merge_service, root),
    )))
}

pub(crate) async fn read_workspace_diff(
    worktree_path: &str,
    branch: &str,
    before_sha: Option<&str>,
    default_branch: &str,
    head: Option<&str>,
) -> Result<DiffResponse> {
    let default_branch = default_branch.to_owned();
    let before_sha = before_sha.map(str::trim).filter(|sha| !sha.is_empty());
    let (base_sha, base_is_commit) = match try_run_git(
        worktree_path,
        ["merge-base", &default_branch, head.unwrap_or("HEAD")],
    )
    .await?
    {
        Some(merge_base) if !merge_base.is_empty() => (merge_base, true),
        _ => {
            let base_spec = before_sha.unwrap_or(default_branch.as_str());
            (
                run_git(worktree_path, &["rev-parse", base_spec]).await?,
                before_sha.is_some(),
            )
        }
    };
    let base_ref = if base_is_commit {
        format!("{default_branch}@{}", short_sha(&base_sha))
    } else {
        default_branch
    };
    let head_ref = head.unwrap_or(branch).to_owned();
    let head_sha = run_git(worktree_path, &["rev-parse", head.unwrap_or("HEAD")]).await?;
    let diff = run_git(
        worktree_path,
        &diff_args(&["--find-renames"], &base_sha, head),
    )
    .await?;

    let statuses = run_git(
        worktree_path,
        &diff_args(&["--name-status", "--find-renames"], &base_sha, head),
    )
    .await?;
    let numstat = run_git(
        worktree_path,
        &diff_args(&["--numstat", "--find-renames"], &base_sha, head),
    )
    .await?;
    let files = merge_diff_summaries(&statuses, &numstat);

    let stats = DiffStats {
        files_changed: files.len() as u64,
        total_additions: files.iter().map(|file| file.additions).sum(),
        total_deletions: files.iter().map(|file| file.deletions).sum(),
    };

    Ok(DiffResponse {
        base_ref,
        head_ref,
        base_sha,
        head_sha,
        files,
        stats,
        diff,
    })
}

fn diff_args<'a>(options: &[&'a str], base: &'a str, head: Option<&'a str>) -> Vec<&'a str> {
    let mut args = vec!["diff"];
    args.extend_from_slice(options);
    args.push(base);
    if let Some(head) = head {
        args.push(head);
    }
    args
}

fn short_sha(sha: &str) -> &str {
    sha.get(..12).unwrap_or(sha)
}

pub(crate) fn ensure_workspace_diffable(workspace: &Workspace) -> Result<()> {
    match &workspace.status {
        WorkspaceStatus::Ready => Ok(()),
        WorkspaceStatus::Error => Err(ServiceError::invalid_operation(format!(
            "workspace {} is in error state: {}",
            workspace.id,
            workspace
                .error
                .clone()
                .unwrap_or_else(|| "unknown error".to_owned())
        ))),
        status => Err(ServiceError::invalid_operation(format!(
            "workspace {} is not ready (status={status})",
            workspace.id
        ))),
    }
}

async fn run_git(worktree_path: &str, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(Path::new(worktree_path))
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|error| ServiceError::invalid_operation(format!("failed to run git: {error}")))?;

    if !output.status.success() {
        return Err(ServiceError::invalid_operation(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn try_run_git<const N: usize>(
    worktree_path: &str,
    args: [&str; N],
) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(Path::new(worktree_path))
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|error| ServiceError::invalid_operation(format!("failed to run git: {error}")))?;

    if !output.status.success() {
        return Ok(None);
    }

    Ok(Some(
        String::from_utf8_lossy(&output.stdout).trim().to_owned(),
    ))
}

fn merge_diff_summaries(statuses: &str, numstat: &str) -> Vec<FileDiffSummary> {
    let mut map = HashMap::<String, FileDiffSummary>::new();

    for line in statuses.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split('\t');
        let Some(raw_status) = fields.next() else {
            continue;
        };
        let status = parse_file_status(raw_status);
        let path = match status {
            DiffFileStatus::Renamed => fields.nth(1).unwrap_or_default().to_owned(),
            _ => fields.next().unwrap_or_default().to_owned(),
        };
        if path.is_empty() {
            continue;
        }
        map.entry(path.clone()).or_insert(FileDiffSummary {
            path,
            status,
            additions: 0,
            deletions: 0,
        });
    }

    for line in numstat.lines().filter(|line| !line.trim().is_empty()) {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() < 3 {
            continue;
        }
        let additions = fields[0].parse::<u64>().unwrap_or(0);
        let deletions = fields[1].parse::<u64>().unwrap_or(0);
        let path = if fields.len() >= 4 {
            fields[3].to_owned()
        } else {
            fields[2].to_owned()
        };
        if path.is_empty() {
            continue;
        }
        let entry = map.entry(path.clone()).or_insert(FileDiffSummary {
            path,
            status: DiffFileStatus::Modified,
            additions: 0,
            deletions: 0,
        });
        entry.additions = additions;
        entry.deletions = deletions;
    }

    let mut files = map.into_values().collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

fn parse_file_status(raw_status: &str) -> DiffFileStatus {
    let code = raw_status.chars().next().unwrap_or('M');
    match code {
        'A' => DiffFileStatus::Added,
        'D' => DiffFileStatus::Deleted,
        'R' => DiffFileStatus::Renamed,
        _ => DiffFileStatus::Modified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_diff_lines_into_file_summaries() {
        let statuses =
            "M\tsrc/main.rs\nA\tsrc/new.rs\nD\tsrc/old.rs\nR100\tsrc/from.rs\tsrc/to.rs\n";
        let numstat = "10\t2\tsrc/main.rs\n5\t0\tsrc/new.rs\n0\t3\tsrc/old.rs\n1\t1\tsrc/from.rs\tsrc/to.rs\n";
        let files = merge_diff_summaries(statuses, numstat);

        assert_eq!(files.len(), 4);
        assert_eq!(files[0].path, "src/main.rs");
        assert_eq!(files[0].status, DiffFileStatus::Modified);
        assert_eq!(files[0].additions, 10);
        assert_eq!(files[0].deletions, 2);

        assert_eq!(files[1].path, "src/new.rs");
        assert_eq!(files[1].status, DiffFileStatus::Added);

        assert_eq!(files[2].path, "src/old.rs");
        assert_eq!(files[2].status, DiffFileStatus::Deleted);

        assert_eq!(files[3].path, "src/to.rs");
        assert_eq!(files[3].status, DiffFileStatus::Renamed);
        assert_eq!(files[3].additions, 1);
        assert_eq!(files[3].deletions, 1);
    }
}
