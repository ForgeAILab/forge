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

/// Create an execution's log file, empty, when its path is recorded. The row
/// claims the path before the first line is written (a remote execution
/// writes it when its daemon's first log frame arrives), and a claimed path
/// with no file reads as `execution.logs_unavailable`: without this, the log
/// of an execution that had started and not yet printed was a 404.
pub(crate) fn create_execution_log(path: &str) -> crate::Result<()> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            crate::ServiceError::invalid_operation(format!(
                "failed to create log directory: {error}"
            ))
        })?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            crate::ServiceError::invalid_operation(format!("failed to create log file: {error}"))
        })?;
    Ok(())
}
