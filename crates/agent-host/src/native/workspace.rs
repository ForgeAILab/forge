//! Scope-bound workspace paths and traversal protections.

use super::*;

/// Build the fail-closed workspace boundary for one server-authorized scope.
///
/// The runtime's workspace contract deliberately answers only whether a path
/// is inside a boundary.  Forge keeps the higher-level read/write distinction
/// in the canonical scope/tool policy and in the existing Task reviewer
/// worktree restoration path; this adapter makes sure only Task scopes receive
/// a repository root at all.
pub(super) fn workspace_for_scope(
    scope: &CanonicalScope,
    workspace_path: Option<&str>,
) -> Result<Arc<dyn agent_runtime::core::workspace::Workspace>, AgentHostError> {
    match scope.scope_type {
        CanonicalScopeType::Task => {
            let path = workspace_path
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "Task scope requires a host-issued workspace path".to_owned(),
                    )
                })?;
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                AgentHostError::Authority(
                    "Task scope workspace path is not an existing directory".to_owned(),
                )
            })?;
            if !canonical.is_dir() {
                return Err(AgentHostError::Authority(
                    "Task scope workspace path is not a directory".to_owned(),
                ));
            }
            Ok(Arc::new(TaskWorkspace::new(canonical)))
        }
        CanonicalScopeType::AgentChat
            if scope.workspace_access == WorkspaceAccess::ProjectVerify =>
        {
            let path = workspace_path
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "Project verification scope requires a host-issued checkout".to_owned(),
                    )
                })?;
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                AgentHostError::Authority(
                    "Project verification checkout is not an existing directory".to_owned(),
                )
            })?;
            if !canonical.is_dir() {
                return Err(AgentHostError::Authority(
                    "Project verification checkout is not a directory".to_owned(),
                ));
            }
            Ok(Arc::new(TaskWorkspace::new(canonical)))
        }
        CanonicalScopeType::Account | CanonicalScopeType::AgentChat
            if scope.workspace_access == WorkspaceAccess::AccountScratch =>
        {
            let path = workspace_path
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "account scratch scope requires a host-issued directory".to_owned(),
                    )
                })?;
            let canonical = std::fs::canonicalize(path).map_err(|_| {
                AgentHostError::Authority(
                    "account scratch directory is not an existing directory".to_owned(),
                )
            })?;
            if !canonical.is_dir() {
                return Err(AgentHostError::Authority(
                    "account scratch path is not a directory".to_owned(),
                ));
            }
            Ok(Arc::new(TaskWorkspace::new(canonical)))
        }
        CanonicalScopeType::Account
        | CanonicalScopeType::Project
        | CanonicalScopeType::AgentChat => {
            if workspace_path.is_some() {
                return Err(AgentHostError::Authority(
                    "non-Task scope cannot receive a workspace path".to_owned(),
                ));
            }
            Ok(Arc::new(DenyAllWorkspace))
        }
    }
}

/// A filesystem-aware, fail-closed Task workspace boundary.
///
/// Existing paths are canonicalized before the component-aware boundary check.
/// For a not-yet-created path, the nearest existing ancestor is canonicalized;
/// this prevents a symlinked directory from redirecting a later write outside
/// the admitted root while still allowing tools to create new files.
#[derive(Debug, Clone)]
struct TaskWorkspace {
    root: PathBuf,
}

impl TaskWorkspace {
    fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            root: std::fs::canonicalize(&root).unwrap_or(root),
        }
    }
}

impl agent_runtime::core::workspace::Workspace for TaskWorkspace {
    fn root(&self) -> &str {
        self.root.to_str().unwrap_or("<invalid-task-workspace>")
    }

    fn contains(&self, path: &str) -> bool {
        if path.is_empty()
            || Path::new(path)
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return false;
        }
        let root = &self.root;
        let candidate = Path::new(path);
        let candidate = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            root.join(candidate)
        };
        let Ok(relative) = candidate.strip_prefix(root) else {
            return false;
        };
        let mut current = root.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                continue;
            };
            current.push(component);
            match std::fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    // Do not follow symlinks even when their current target is
                    // inside the root.  This closes both existing escapes and
                    // broken-link write escapes where `Path::exists()` would
                    // otherwise skip the link and accept its parent.
                    return false;
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    // New descendants are allowed after the last existing,
                    // non-symlink parent.  The typed write tool rechecks after
                    // creating parents to close the create-then-follow race.
                    return true;
                }
                Err(_) => return false,
            }
        }
        std::fs::canonicalize(&candidate)
            .map(|canonical| canonical.as_path() == root.as_path() || canonical.starts_with(root))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests;
