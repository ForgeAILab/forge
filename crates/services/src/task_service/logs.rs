use std::path::{Path, PathBuf};

/// Where hooks that run outside an execution log: the Task's own log
/// directory, so whatever retains or removes a Task's logs covers them.
pub(crate) fn task_hook_logs_dir(
    workspace_root: &Path,
    project_id: &str,
    task_id: &str,
) -> PathBuf {
    workspace_root
        .join(".forge")
        .join("logs")
        .join(project_id)
        .join(task_id)
        .join("hooks")
}

pub(crate) fn execution_logs_path(
    workspace_root: &Path,
    project_id: &str,
    task_id: &str,
    execution_id: &str,
) -> String {
    workspace_root
        .join(".forge")
        .join("logs")
        .join(project_id)
        .join(task_id)
        .join(format!("{execution_id}.jsonl"))
        .to_string_lossy()
        .into_owned()
}
