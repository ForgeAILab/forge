use std::{
    fs,
    path::{Path, PathBuf},
};

use api_types::{
    DaemonErrorPayload, FsBranchesParams, FsBranchesResult, FsEntry, FsListParams, FsListResult,
    PATH_GUARDRAIL,
};

const SKIP_NAMES: &[&str] = &[
    ".Trashes",
    ".Spotlight-V100",
    ".fseventsd",
    ".DS_Store",
    "Library",
    "$RECYCLE.BIN",
    "System Volume Information",
    "AppData",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".cache",
    "__pycache__",
    ".venv",
    "venv",
    ".git",
    ".forge",
];

pub type CommandResult<T> = std::result::Result<T, DaemonErrorPayload>;

pub async fn list_entries(
    params: FsListParams,
    workspace_root: &Path,
) -> CommandResult<FsListResult> {
    let path = validate_within_root(Path::new(params.path.trim()), workspace_root)?;
    let mut entries = Vec::new();

    for entry in fs::read_dir(&path).map_err(|error| {
        path_guardrail_error(format!("read directory {}: {error}", path.display()))
    })? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(error = %error, "failed to read directory entry");
                continue;
            }
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        if SKIP_NAMES.contains(&name.as_str()) {
            continue;
        }

        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                tracing::warn!(error = %error, path = %entry.path().display(), "failed to read file type");
                continue;
            }
        };

        let entry_path = entry.path();
        let is_dir = file_type.is_dir();
        entries.push(FsEntry {
            name,
            path: canonical_or_absolute(&entry_path)
                .to_string_lossy()
                .into_owned(),
            is_dir,
            is_git_repo: is_dir && git::is_git_repo(&entry_path).await,
        });
    }

    entries.sort_by(|left, right| {
        right
            .is_dir
            .cmp(&left.is_dir)
            .then_with(|| {
                left.name
                    .to_ascii_lowercase()
                    .cmp(&right.name.to_ascii_lowercase())
            })
            .then_with(|| left.name.cmp(&right.name))
    });

    Ok(FsListResult {
        path: path.to_string_lossy().into_owned(),
        entries,
    })
}

pub async fn list_branches(
    params: FsBranchesParams,
    workspace_root: &Path,
) -> CommandResult<FsBranchesResult> {
    let path = validate_within_root(Path::new(params.path.trim()), workspace_root)?;
    if !git::is_git_repo(&path).await {
        return Err(path_guardrail_error("path is not a git repository"));
    }

    let branches = git::list_branches(&path).await.map_err(|error| {
        path_guardrail_error(format!(
            "failed to list branches for {}: {error}",
            path.display()
        ))
    })?;

    Ok(FsBranchesResult {
        branches: branches.branches,
        default_branch: branches.default_branch,
        origin_url: branches.origin_url,
    })
}

pub fn validate_within_root(requested: &Path, root: &Path) -> CommandResult<PathBuf> {
    let resolved = resolve_requested_path(requested, root)?;
    // Existing paths are canonicalized so symlinks cannot bypass containment.
    let canonical = resolved.canonicalize().map_err(|error| {
        path_guardrail_error(format!(
            "failed to resolve path '{}': {error}",
            requested.display()
        ))
    })?;
    let canonical_root = root.canonicalize().map_err(|error| {
        path_guardrail_error(format!(
            "failed to resolve daemon workspace root '{}': {error}",
            root.display()
        ))
    })?;

    if !canonical.starts_with(&canonical_root) {
        return Err(path_escape_error(requested));
    }

    Ok(canonical)
}

fn resolve_requested_path(requested: &Path, root: &Path) -> CommandResult<PathBuf> {
    let requested_text = requested.to_string_lossy();
    if requested_text == "~" {
        return home_dir();
    }

    if let Some(remainder) = requested_text.strip_prefix("~/") {
        return Ok(home_dir()?.join(remainder));
    }

    if let Some(remainder) = requested_text.strip_prefix("~\\") {
        return Ok(home_dir()?.join(remainder));
    }

    if requested_text.starts_with('~') {
        return Err(path_guardrail_error("only ~ or ~/... paths are supported"));
    }

    if requested.is_absolute() {
        Ok(requested.to_path_buf())
    } else {
        Ok(root.join(requested))
    }
}

fn home_dir() -> CommandResult<PathBuf> {
    dirs::home_dir().ok_or_else(|| path_guardrail_error("failed to resolve home directory"))
}

fn path_escape_error(requested: &Path) -> DaemonErrorPayload {
    path_guardrail_error(format!(
        "path '{}' escapes the daemon's workspace root",
        requested.display()
    ))
}

fn path_guardrail_error(message: impl Into<String>) -> DaemonErrorPayload {
    DaemonErrorPayload {
        code: PATH_GUARDRAIL.to_owned(),
        message: message.into(),
        details: None,
    }
}

fn canonical_or_absolute(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
