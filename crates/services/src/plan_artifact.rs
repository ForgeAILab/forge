use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;

use crate::workspace_backend::{ResolvedWorkspace, WorkspaceBackendError, WorkspaceBackendRouter};
use api_types::{PlanArtifactDetail, PlanChecklistItem, PlanProgressSummary};
use db::{SqliteDb, WorkspaceRepo};

pub const DEFAULT_PLAN_ARTIFACT_PATH: &str = executors::OUTBOX_PLAN_FILE;
pub const PLAN_ARTIFACT_AGENT_INSTRUCTION: &str = "Write the implementation plan with `task.plan` (`write`) as a Markdown checklist. Forge publishes that execution-scoped candidate as the Task's canonical plan only after this execution completes successfully. This plan will be handed to the coder agent for execution. Each checklist item represents implementation work or verification the coder should complete. Use `- [ ]` for pending work and `- [x]` only for work that is already complete. Nest sub-items with 2-space indentation.";
pub const OUTBOX_PLAN_ARTIFACT_AGENT_INSTRUCTION: &str = "Write an implementation plan as a Markdown checklist to the file named by `$FORGE_PLAN_PATH`. Forge publishes that file as the Task's canonical plan only after this execution completes successfully. This plan will be handed to the coder agent for execution. Each checklist item represents implementation work or verification the coder should complete. Use `- [ ]` for pending work and `- [x]` only for work that is already complete. Nest sub-items with 2-space indentation.";

pub(crate) const MAX_PLAN_ARTIFACT_SIZE_BYTES: u64 = 1_048_576;
const PLAN_STAGE_DIR: &str = ".forge-plan-staging";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanArtifactMetadata {
    pub task_id: Option<String>,
    pub workspace_id: Option<String>,
    pub execution_id: Option<String>,
    pub source_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPlanItem {
    pub checked: bool,
    pub label: String,
    pub nesting_level: usize,
    pub line_number: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPlanArtifact {
    pub items: Vec<ParsedPlanItem>,
    pub warnings: Vec<String>,
}

#[derive(Debug)]
pub enum PlanArtifactError {
    NotFound,
    WorkspaceNotFound { workspace_id: String },
    PathEscape { path: PathBuf },
    InvalidFileType { path: PathBuf },
    MultipleHardLinks { path: PathBuf },
    InvalidUtf8 { path: PathBuf },
    MissingChecklist { path: PathBuf },
    StagedConflict { path: PathBuf },
    IoError(io::Error),
    DbError(db::DbError),
    BackendError(WorkspaceBackendError),
    FileTooLarge { size: u64, max: u64 },
}

impl fmt::Display for PlanArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "plan artifact not found"),
            Self::WorkspaceNotFound { workspace_id } => {
                write!(f, "workspace not found: {workspace_id}")
            }
            Self::PathEscape { path } => {
                write!(
                    f,
                    "plan artifact path escapes workspace: {}",
                    path.display()
                )
            }
            Self::InvalidFileType { path } => {
                write!(f, "plan artifact is not a regular file: {}", path.display())
            }
            Self::MultipleHardLinks { path } => write!(
                f,
                "plan artifact must be an independent file: {}",
                path.display()
            ),
            Self::InvalidUtf8 { path } => {
                write!(f, "plan artifact is not valid UTF-8: {}", path.display())
            }
            Self::MissingChecklist { path } => write!(
                f,
                "plan artifact has no checklist items: {}",
                path.display()
            ),
            Self::StagedConflict { path } => write!(
                f,
                "a different plan candidate is already staged: {}",
                path.display()
            ),
            Self::IoError(error) => write!(f, "plan artifact I/O failed: {error}"),
            Self::DbError(error) => write!(f, "failed to read workspace: {error}"),
            Self::BackendError(WorkspaceBackendError::Other(error)) => match error.as_ref() {
                crate::ServiceError::InvalidOperation { message } => write!(f, "{message}"),
                error => write!(f, "{error}"),
            },
            Self::BackendError(error) => write!(f, "{error}"),
            Self::FileTooLarge { size, max } => write!(
                f,
                "plan artifact is too large: {size} bytes exceeds {max} bytes"
            ),
        }
    }
}

impl Error for PlanArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IoError(error) => Some(error),
            Self::DbError(error) => Some(error),
            Self::BackendError(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for PlanArtifactError {
    fn from(error: io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<db::DbError> for PlanArtifactError {
    fn from(error: db::DbError) -> Self {
        Self::DbError(error)
    }
}

impl From<WorkspaceBackendError> for PlanArtifactError {
    fn from(error: WorkspaceBackendError) -> Self {
        match error {
            WorkspaceBackendError::Other(error) => match *error {
                crate::ServiceError::Db(error) => Self::DbError(error),
                crate::ServiceError::InvalidOperation { ref message }
                    if message == "plan artifact not found" =>
                {
                    Self::NotFound
                }
                error => Self::BackendError(error.into()),
            },
            error => Self::BackendError(error),
        }
    }
}

pub fn parse_plan_markdown(content: &str) -> ParsedPlanArtifact {
    let mut items = Vec::new();
    let mut warnings = Vec::new();

    for (line_index, line) in content.lines().enumerate() {
        let line_number = line_index + 1;

        if let Some(item) = parse_checkbox_line(line, line_number) {
            items.push(item);
        } else if looks_like_checkbox_line(line) {
            warnings.push(format!("line {line_number}: malformed checkbox item"));
        }
    }

    ParsedPlanArtifact { items, warnings }
}

#[cfg(test)]
async fn read_plan_for_workspace(
    db: &SqliteDb,
    workspace_id: &str,
) -> Result<Option<(PlanProgressSummary, PlanArtifactDetail)>, PlanArtifactError> {
    let router = crate::diff::embedded_read_router_for_test(Arc::new(db.clone()));
    read_plan_with_router(db, &router, workspace_id).await
}

pub async fn read_plan_with_router(
    db: &SqliteDb,
    router: &WorkspaceBackendRouter,
    workspace_id: &str,
) -> Result<Option<(PlanProgressSummary, PlanArtifactDetail)>, PlanArtifactError> {
    let workspace = WorkspaceRepo::get_by_id(db, workspace_id)
        .await?
        .ok_or_else(|| PlanArtifactError::WorkspaceNotFound {
            workspace_id: workspace_id.to_string(),
        })?;
    let resolved = router.resolve(db, &workspace).await?;
    read_plan_for_resolved_workspace(&resolved).await
}

pub(crate) async fn read_plan_for_resolved_workspace(
    resolved: &ResolvedWorkspace,
) -> Result<Option<(PlanProgressSummary, PlanArtifactDetail)>, PlanArtifactError> {
    let artifact = match resolved
        .backend
        .read(
            &resolved.placement,
            "../plan.md",
            MAX_PLAN_ARTIFACT_SIZE_BYTES,
        )
        .await
    {
        Ok(bytes) => String::from_utf8(bytes)
            .map(|content| parse_plan_markdown(&content))
            .map_err(|_| {
                PlanArtifactError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "stream did not contain valid UTF-8",
                ))
            }),
        Err(error) => Err(error.into()),
    };
    match artifact {
        Ok(artifact) => {
            let source_path = match resolved.placement.owner_kind {
                db::PlacementOwnerKind::Server => {
                    default_plan_artifact_path(&resolved.embedded_path()?)
                        .to_string_lossy()
                        .to_string()
                }
                db::PlacementOwnerKind::Daemon => "../plan.md".to_owned(),
            };
            Ok(Some((
                to_plan_progress_summary(&artifact),
                to_plan_artifact_detail(&artifact, Some(source_path), None),
            )))
        }
        Err(PlanArtifactError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

pub fn read_plan_artifact(
    workspace_root: &Path,
    plan_path: Option<&str>,
) -> Result<ParsedPlanArtifact, PlanArtifactError> {
    let candidate = match plan_path {
        Some(plan_path) => workspace_root.join(plan_path),
        None => default_plan_artifact_path(workspace_root),
    };
    let allowed_root = match plan_path {
        Some(_) => workspace_root,
        None => workspace_root.parent().unwrap_or(workspace_root),
    };
    let content = read_bounded_plan_text(&candidate, allowed_root, false)?;
    Ok(parse_plan_markdown(&content))
}

/// Read a bounded owner-relative file with plan containment and file-type checks.
pub(crate) fn read_workspace_bytes(
    workspace_root: &Path,
    rel_path: &str,
    limit: u64,
) -> Result<Vec<u8>, PlanArtifactError> {
    let plan_path = if rel_path == "../plan.md" {
        None
    } else {
        Some(rel_path)
    };
    let path = bounded_artifact_path(workspace_root, plan_path, limit)?;
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(PlanArtifactError::FileTooLarge {
            size: bytes.len() as u64,
            max: limit,
        });
    }
    Ok(bytes)
}

fn bounded_artifact_path(
    workspace_root: &Path,
    plan_path: Option<&str>,
    limit: u64,
) -> Result<PathBuf, PlanArtifactError> {
    let candidate = match plan_path {
        Some(plan_path) => workspace_root.join(plan_path),
        None => default_plan_artifact_path(workspace_root),
    };
    let allowed_root = match plan_path {
        Some(_) => workspace_root,
        None => workspace_root.parent().unwrap_or(workspace_root),
    };
    let metadata = fs::symlink_metadata(&candidate).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            PlanArtifactError::NotFound
        } else {
            PlanArtifactError::IoError(error)
        }
    })?;
    if !metadata.file_type().is_file() {
        return Err(PlanArtifactError::InvalidFileType { path: candidate });
    }
    let canonical = candidate.canonicalize()?;
    if !canonical.starts_with(allowed_root.canonicalize()?) {
        return Err(PlanArtifactError::PathEscape { path: canonical });
    }
    let size = fs::metadata(&canonical)?.len();
    if size > limit {
        return Err(PlanArtifactError::FileTooLarge { size, max: limit });
    }
    Ok(candidate)
}

/// Read the canonical Task plan text for dispatch without bypassing the plan
/// artifact's containment, file-type, UTF-8, and size checks.
pub fn read_canonical_plan_text(worktree_root: &Path) -> Result<Option<String>, PlanArtifactError> {
    let candidate = default_plan_artifact_path(worktree_root);
    let allowed_root = worktree_root.parent().unwrap_or(worktree_root);
    match read_bounded_plan_text(&candidate, allowed_root, false) {
        Ok(content) => Ok(Some(content)),
        Err(PlanArtifactError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Prepare the per-execution plan file before a CLI harness starts.
///
/// An existing canonical plan is copied to a distinct inode so an
/// implementation role can update checklist state without gaining write
/// access to the Task directory. An existing outbox file is validated and
/// preserved, which keeps crash recovery from overwriting prior output.
pub fn prepare_execution_plan_outbox(
    worktree_root: &Path,
    execution_id: &str,
    seed_existing_plan: bool,
    fallback_plan: Option<&str>,
) -> Result<bool, PlanArtifactError> {
    let outbox = executors::prepare_execution_outbox(worktree_root, execution_id)
        .map_err(|error| map_outbox_error(error, execution_id))?;
    let outbox_plan = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
    recover_private_installation(&outbox_plan)?;
    match read_bounded_plan_text(&outbox_plan, &outbox, true) {
        Ok(_) => return Ok(false),
        Err(PlanArtifactError::NotFound) => {}
        Err(error) => return Err(error),
    }

    if !seed_existing_plan {
        return Ok(false);
    }

    let canonical = default_plan_artifact_path(worktree_root);
    let task_root = worktree_root.parent().unwrap_or(worktree_root);
    let content = match read_bounded_plan_text(&canonical, task_root, false) {
        Ok(content) => content,
        Err(PlanArtifactError::NotFound) => {
            match fallback_plan.filter(|content| !parse_plan_markdown(content).items.is_empty()) {
                Some(content) if content.len() as u64 <= MAX_PLAN_ARTIFACT_SIZE_BYTES => {
                    content.to_owned()
                }
                Some(content) => {
                    return Err(PlanArtifactError::FileTooLarge {
                        size: content.len() as u64,
                        max: MAX_PLAN_ARTIFACT_SIZE_BYTES,
                    });
                }
                None => return Ok(false),
            }
        }
        Err(error) => return Err(error),
    };
    install_private_file_no_replace(&outbox_plan, content.as_bytes())?;
    Ok(true)
}

/// Replace one native execution's brokered plan candidate. The native tool
/// handler supplies the bytes; the model never receives filesystem authority
/// over the Task directory or another execution's outbox.
pub fn write_execution_outbox_plan(
    worktree_root: &Path,
    execution_id: &str,
    content: &str,
) -> Result<(), PlanArtifactError> {
    if content.len() as u64 > MAX_PLAN_ARTIFACT_SIZE_BYTES {
        return Err(PlanArtifactError::FileTooLarge {
            size: content.len() as u64,
            max: MAX_PLAN_ARTIFACT_SIZE_BYTES,
        });
    }
    let outbox = executors::prepare_execution_outbox(worktree_root, execution_id)
        .map_err(|error| map_outbox_error(error, execution_id))?;
    let parsed = parse_plan_markdown(content);
    if parsed.items.is_empty() {
        return Err(PlanArtifactError::MissingChecklist {
            path: outbox.join(DEFAULT_PLAN_ARTIFACT_PATH),
        });
    }

    let destination = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
    let temporary = outbox.join(format!(
        ".{}.{}.tmp",
        DEFAULT_PLAN_ARTIFACT_PATH,
        db::new_uuid_v4()
    ));
    write_new_private_file(&temporary, content.as_bytes())?;
    if let Err(error) = replace_plan_file(&temporary, &destination) {
        let _ = fs::remove_file(&temporary);
        return Err(PlanArtifactError::IoError(error));
    }
    read_bounded_plan_text(&destination, &outbox, true)?;
    sync_parent_directory(&outbox)?;
    Ok(())
}

/// Validate whether an authorized execution left a publishable plan candidate.
pub fn validate_execution_outbox_plan(outbox: &Path) -> Result<bool, PlanArtifactError> {
    let path = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
    let content = match read_bounded_plan_text(&path, outbox, true) {
        Ok(content) => content,
        Err(PlanArtifactError::NotFound) => return Ok(false),
        Err(error) => return Err(error),
    };
    if parse_plan_markdown(&content).items.is_empty() {
        return Err(PlanArtifactError::MissingChecklist { path });
    }
    Ok(true)
}

/// Freeze one validated candidate outside the agent-writable outbox.
///
/// This runs before terminal settlement. The returned stage is bounded,
/// private, and tied to one execution id; the caller may then remove the whole
/// outbox. A terminal-CAS loser deletes the stage, while the winner publishes
/// these exact bytes. Repeating the operation with the same bytes is
/// idempotent, which supports remote terminal redelivery.
pub fn stage_execution_outbox_plan(
    outbox: &Path,
    worktree_root: &Path,
    execution_id: &str,
) -> Result<bool, PlanArtifactError> {
    let expected = executors::existing_execution_outbox(worktree_root, execution_id)
        .map_err(|error| map_outbox_error(error, execution_id))?;
    let Some(expected) = expected else {
        return Ok(false);
    };
    if normalize_lexical(outbox) != normalize_lexical(&expected) {
        return Err(PlanArtifactError::PathEscape {
            path: outbox.to_path_buf(),
        });
    }
    let source = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
    let content = match read_bounded_plan_text(&source, outbox, true) {
        Ok(content) => content,
        Err(PlanArtifactError::NotFound) => return Ok(false),
        Err(error) => return Err(error),
    };
    if parse_plan_markdown(&content).items.is_empty() {
        return Err(PlanArtifactError::MissingChecklist { path: source });
    }

    let stage = execution_plan_stage_path(worktree_root, execution_id)?;
    let stage_root = stage
        .parent()
        .ok_or_else(|| PlanArtifactError::PathEscape {
            path: stage.clone(),
        })?;
    ensure_private_stage_directory(worktree_root, stage_root)?;
    recover_private_installation(&stage)?;
    match read_bounded_plan_text(&stage, stage_root, true) {
        Ok(existing) if existing == content => return Ok(true),
        Ok(_) => return Err(PlanArtifactError::StagedConflict { path: stage }),
        Err(PlanArtifactError::NotFound) => {}
        Err(error) => return Err(error),
    }

    if let Err(error) = install_private_file_no_replace(&stage, content.as_bytes()) {
        let PlanArtifactError::IoError(error) = error else {
            return Err(error);
        };
        if error.kind() == io::ErrorKind::AlreadyExists {
            let existing = read_bounded_plan_text(&stage, stage_root, true)?;
            if existing == content {
                return Ok(true);
            }
            return Err(PlanArtifactError::StagedConflict { path: stage });
        }
        return Err(PlanArtifactError::IoError(error));
    }
    sync_parent_directory(stage_root)?;
    Ok(true)
}

/// Publish the host-frozen candidate for a terminal-CAS winning execution.
///
/// The stage remains in place until the workflow cascade commits its state
/// transition (or installs the matching human-approval marker), so recovery
/// can repeat publication after a crash at either boundary.
pub fn publish_staged_execution_plan(
    worktree_root: &Path,
    execution_id: &str,
) -> Result<bool, PlanArtifactError> {
    let source = execution_plan_stage_path(worktree_root, execution_id)?;
    let stage_root = source
        .parent()
        .ok_or_else(|| PlanArtifactError::PathEscape {
            path: source.clone(),
        })?;
    let content = match read_bounded_plan_text(&source, stage_root, true) {
        Ok(content) => content,
        Err(PlanArtifactError::NotFound) => return Ok(false),
        Err(error) => return Err(error),
    };
    if parse_plan_markdown(&content).items.is_empty() {
        return Err(PlanArtifactError::MissingChecklist { path: source });
    }

    ensure_plan_publication_backup(worktree_root, execution_id)?;

    let destination = default_plan_artifact_path(worktree_root);
    let parent = destination.parent().unwrap_or(worktree_root);
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        DEFAULT_PLAN_ARTIFACT_PATH,
        db::new_uuid_v4()
    ));
    write_new_private_file(&temporary, content.as_bytes())?;
    if let Err(error) = replace_plan_file(&temporary, &destination) {
        let _ = fs::remove_file(&temporary);
        return Err(PlanArtifactError::IoError(error));
    }

    let published = fs::symlink_metadata(&destination)?;
    if !published.file_type().is_file() {
        return Err(PlanArtifactError::InvalidFileType { path: destination });
    }
    sync_parent_directory(parent)?;
    Ok(true)
}

/// Restore the canonical plan snapshot captured before this execution first
/// published. Restoration is compare-safe: if the current canonical bytes no
/// longer equal this execution's frozen candidate, Forge refuses to clobber
/// them and retains the claim for operator-visible recovery.
pub fn restore_plan_before_abandon(
    worktree_root: &Path,
    execution_id: &str,
) -> Result<(), PlanArtifactError> {
    let stage = execution_plan_stage_path(worktree_root, execution_id)?;
    let stage_root = stage
        .parent()
        .ok_or_else(|| PlanArtifactError::PathEscape {
            path: stage.clone(),
        })?;
    let previous = execution_plan_previous_path(&stage);
    let absent = execution_plan_previous_absent_path(&stage);
    recover_private_installation(&previous)?;
    recover_private_installation(&absent)?;
    let has_previous = fs::symlink_metadata(&previous).is_ok();
    let was_absent = fs::symlink_metadata(&absent).is_ok();
    if !has_previous && !was_absent {
        return Ok(());
    }
    let staged = read_bounded_plan_text(&stage, stage_root, true)?;
    let destination = default_plan_artifact_path(worktree_root);
    let task_root = worktree_root.parent().unwrap_or(worktree_root);
    let previous_content = if has_previous {
        Some(read_bounded_plan_text(&previous, stage_root, true)?)
    } else {
        None
    };
    match read_bounded_plan_text(&destination, task_root, false) {
        Ok(current) if current == staged => {}
        Ok(current) if previous_content.as_deref() == Some(current.as_str()) => return Ok(()),
        Ok(_) => return Err(PlanArtifactError::StagedConflict { path: destination }),
        Err(PlanArtifactError::NotFound) if was_absent => return Ok(()),
        Err(error) => return Err(error),
    }

    if let Some(content) = previous_content {
        let temporary = task_root.join(format!(
            ".{}.{}.restore",
            DEFAULT_PLAN_ARTIFACT_PATH,
            db::new_uuid_v4()
        ));
        write_new_private_file(&temporary, content.as_bytes())?;
        if let Err(error) = replace_plan_file(&temporary, &destination) {
            let _ = fs::remove_file(&temporary);
            return Err(PlanArtifactError::IoError(error));
        }
    } else {
        fs::remove_file(&destination)?;
    }
    sync_parent_directory(task_root)?;
    Ok(())
}

/// Remove the bounded host-owned candidate after settlement loses authority or
/// its workflow outcome has committed.
pub fn discard_staged_execution_plan(
    worktree_root: &Path,
    execution_id: &str,
) -> Result<(), PlanArtifactError> {
    let stage = execution_plan_stage_path(worktree_root, execution_id)?;
    let previous = execution_plan_previous_path(&stage);
    let absent = execution_plan_previous_absent_path(&stage);
    for path in [
        stage.clone(),
        private_installation_path(&stage)?,
        previous.clone(),
        private_installation_path(&previous)?,
        absent.clone(),
        private_installation_path(&absent)?,
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(PlanArtifactError::IoError(error)),
        }
    }
    if let Some(parent) = stage.parent() {
        match fs::remove_dir(parent) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => return Err(PlanArtifactError::IoError(error)),
        }
    }
    Ok(())
}

fn ensure_plan_publication_backup(
    worktree_root: &Path,
    execution_id: &str,
) -> Result<(), PlanArtifactError> {
    let stage = execution_plan_stage_path(worktree_root, execution_id)?;
    let stage_root = stage
        .parent()
        .ok_or_else(|| PlanArtifactError::PathEscape {
            path: stage.clone(),
        })?;
    let previous = execution_plan_previous_path(&stage);
    let absent = execution_plan_previous_absent_path(&stage);
    let has_previous = fs::symlink_metadata(&previous).is_ok();
    let has_absent = fs::symlink_metadata(&absent).is_ok();
    if has_previous && has_absent {
        return Err(PlanArtifactError::StagedConflict { path: previous });
    }
    if has_previous {
        let content = read_bounded_plan_text(&previous, stage_root, false)?;
        install_private_file_no_replace(&previous, content.as_bytes())?;
        return Ok(());
    }
    if has_absent {
        let content = read_bounded_plan_text(&absent, stage_root, false)?;
        install_private_file_no_replace(&absent, content.as_bytes())?;
        return Ok(());
    }

    let destination = default_plan_artifact_path(worktree_root);
    let task_root = worktree_root.parent().unwrap_or(worktree_root);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => {
            let content = read_bounded_plan_text(&destination, task_root, false)?;
            install_private_file_no_replace(&previous, content.as_bytes())?;
        }
        Ok(_) => install_private_file_no_replace(&absent, b"absent\n")?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            install_private_file_no_replace(&absent, b"absent\n")?;
        }
        Err(error) => return Err(PlanArtifactError::IoError(error)),
    }
    sync_parent_directory(stage_root)?;
    Ok(())
}

fn execution_plan_previous_path(stage: &Path) -> PathBuf {
    stage.with_extension("previous.md")
}

fn execution_plan_previous_absent_path(stage: &Path) -> PathBuf {
    stage.with_extension("previous-absent")
}

fn execution_plan_stage_path(
    worktree_root: &Path,
    execution_id: &str,
) -> Result<PathBuf, PlanArtifactError> {
    if !executors::execution_id_is_path_safe(execution_id) {
        return Err(PlanArtifactError::PathEscape {
            path: PathBuf::from(execution_id),
        });
    }
    Ok(worktree_root
        .parent()
        .unwrap_or(worktree_root)
        .join(PLAN_STAGE_DIR)
        .join(format!("{execution_id}.md")))
}

fn map_outbox_error(
    error: executors::ExecutionOutboxError,
    execution_id: &str,
) -> PlanArtifactError {
    match error {
        executors::ExecutionOutboxError::InvalidDirectory(path) => {
            PlanArtifactError::InvalidFileType { path }
        }
        executors::ExecutionOutboxError::PathEscape(path)
        | executors::ExecutionOutboxError::MissingTaskRoot(path) => {
            PlanArtifactError::PathEscape { path }
        }
        executors::ExecutionOutboxError::InvalidExecutionId => PlanArtifactError::PathEscape {
            path: PathBuf::from(execution_id),
        },
        executors::ExecutionOutboxError::Io(error) => PlanArtifactError::IoError(error),
    }
}

fn ensure_private_stage_directory(
    worktree_root: &Path,
    stage_root: &Path,
) -> Result<(), PlanArtifactError> {
    let task_root = worktree_root.parent().unwrap_or(worktree_root);
    match fs::symlink_metadata(stage_root) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(PlanArtifactError::InvalidFileType {
                path: stage_root.to_path_buf(),
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(stage_root)?,
        Err(error) => return Err(PlanArtifactError::IoError(error)),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(stage_root, fs::Permissions::from_mode(0o700))?;
    }
    let canonical_task_root = fs::canonicalize(task_root)?;
    let canonical_stage_root = fs::canonicalize(stage_root)?;
    if !canonical_stage_root.starts_with(&canonical_task_root) {
        return Err(PlanArtifactError::PathEscape {
            path: canonical_stage_root,
        });
    }
    Ok(())
}

fn read_bounded_plan_text(
    candidate: &Path,
    allowed_root: &Path,
    require_single_link: bool,
) -> Result<String, PlanArtifactError> {
    let normalized_root = normalize_lexical(allowed_root);
    let normalized_candidate = normalize_lexical(candidate);
    if !normalized_candidate.starts_with(&normalized_root) {
        return Err(PlanArtifactError::PathEscape {
            path: normalized_candidate,
        });
    }

    let leaf_metadata = match fs::symlink_metadata(&normalized_candidate) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(PlanArtifactError::NotFound);
        }
        Err(error) => return Err(PlanArtifactError::IoError(error)),
    };
    if !leaf_metadata.file_type().is_file() {
        return Err(PlanArtifactError::InvalidFileType {
            path: normalized_candidate,
        });
    }

    let canonical_root = fs::canonicalize(&normalized_root)?;
    let canonical_candidate = fs::canonicalize(&normalized_candidate)?;
    if !canonical_candidate.starts_with(&canonical_root) {
        return Err(PlanArtifactError::PathEscape {
            path: canonical_candidate,
        });
    }

    let path_metadata = fs::symlink_metadata(&canonical_candidate)?;
    if !path_metadata.file_type().is_file() {
        return Err(PlanArtifactError::InvalidFileType {
            path: canonical_candidate,
        });
    }
    let file = File::open(&canonical_candidate)?;
    let opened_metadata = file.metadata()?;
    if !same_file(&path_metadata, &opened_metadata) {
        return Err(PlanArtifactError::InvalidFileType {
            path: canonical_candidate,
        });
    }
    #[cfg(unix)]
    if require_single_link {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.nlink() != 1 {
            return Err(PlanArtifactError::MultipleHardLinks {
                path: canonical_candidate,
            });
        }
    }

    let size = opened_metadata.len();
    if size > MAX_PLAN_ARTIFACT_SIZE_BYTES {
        return Err(PlanArtifactError::FileTooLarge {
            size,
            max: MAX_PLAN_ARTIFACT_SIZE_BYTES,
        });
    }
    let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
    file.take(MAX_PLAN_ARTIFACT_SIZE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_PLAN_ARTIFACT_SIZE_BYTES {
        return Err(PlanArtifactError::FileTooLarge {
            size: bytes.len() as u64,
            max: MAX_PLAN_ARTIFACT_SIZE_BYTES,
        });
    }
    String::from_utf8(bytes).map_err(|_| PlanArtifactError::InvalidUtf8 {
        path: canonical_candidate,
    })
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> Result<(), PlanArtifactError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path)?;
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(PlanArtifactError::IoError(error));
    }
    Ok(())
}

fn install_private_file_no_replace(path: &Path, bytes: &[u8]) -> Result<(), PlanArtifactError> {
    let parent = path.parent().ok_or_else(|| PlanArtifactError::PathEscape {
        path: path.to_path_buf(),
    })?;
    let temporary = private_installation_path(path)?;
    if let Ok(existing) = read_bounded_plan_text(path, parent, false) {
        if existing.as_bytes() != bytes {
            return Err(PlanArtifactError::IoError(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "plan destination already exists with different content",
            )));
        }
        match fs::symlink_metadata(&temporary) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::remove_file(&temporary)?;
                sync_parent_directory(parent)?;
            }
            Ok(_) => return Err(PlanArtifactError::InvalidFileType { path: temporary }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(PlanArtifactError::IoError(error)),
        }
        read_bounded_plan_text(path, parent, true)?;
        return Ok(());
    }
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.file_type().is_file() => fs::remove_file(&temporary)?,
        Ok(_) => {
            return Err(PlanArtifactError::InvalidFileType { path: temporary });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(PlanArtifactError::IoError(error)),
    }
    write_new_private_file(&temporary, bytes)?;
    if let Err(error) = fs::hard_link(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        if error.kind() == io::ErrorKind::AlreadyExists {
            let existing = read_bounded_plan_text(path, parent, true)?;
            if existing.as_bytes() == bytes {
                return Ok(());
            }
        }
        return Err(PlanArtifactError::IoError(error));
    }
    fs::remove_file(&temporary)?;
    sync_parent_directory(parent)?;
    read_bounded_plan_text(path, parent, true)?;
    Ok(())
}

fn recover_private_installation(path: &Path) -> Result<(), PlanArtifactError> {
    let parent = path.parent().ok_or_else(|| PlanArtifactError::PathEscape {
        path: path.to_path_buf(),
    })?;
    let temporary = private_installation_path(path)?;
    let destination = fs::symlink_metadata(path);
    let installing = fs::symlink_metadata(&temporary);
    match (destination, installing) {
        (Ok(destination), Ok(installing))
            if destination.file_type().is_file() && installing.file_type().is_file() =>
        {
            // Whether the crash happened before or after hard-linking, the
            // destination is the committed name. The deterministic private
            // temporary can be discarded, then the single-link invariant is
            // revalidated by the caller.
            fs::remove_file(&temporary)?;
            sync_parent_directory(parent)?;
        }
        (Ok(destination), Ok(_)) if !destination.file_type().is_file() => {
            return Err(PlanArtifactError::InvalidFileType {
                path: path.to_path_buf(),
            });
        }
        (Ok(_), Ok(_)) => {
            return Err(PlanArtifactError::InvalidFileType { path: temporary });
        }
        (Ok(destination), Err(error)) if error.kind() == io::ErrorKind::NotFound => {
            if !destination.file_type().is_file() {
                return Err(PlanArtifactError::InvalidFileType {
                    path: path.to_path_buf(),
                });
            }
        }
        (Err(error), Ok(installing)) if error.kind() == io::ErrorKind::NotFound => {
            if !installing.file_type().is_file() {
                return Err(PlanArtifactError::InvalidFileType { path: temporary });
            }
            fs::remove_file(&temporary)?;
            sync_parent_directory(parent)?;
        }
        (Err(error), Err(installing_error))
            if error.kind() == io::ErrorKind::NotFound
                && installing_error.kind() == io::ErrorKind::NotFound => {}
        (Err(error), _) | (_, Err(error)) => return Err(PlanArtifactError::IoError(error)),
    }
    Ok(())
}

fn private_installation_path(path: &Path) -> Result<PathBuf, PlanArtifactError> {
    let parent = path.parent().ok_or_else(|| PlanArtifactError::PathEscape {
        path: path.to_path_buf(),
    })?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| PlanArtifactError::PathEscape {
            path: path.to_path_buf(),
        })?;
    Ok(parent.join(format!(".{name}.installing")))
}

#[cfg(unix)]
fn replace_plan_file(source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(not(unix))]
fn replace_plan_file(source: &Path, destination: &Path) -> io::Result<()> {
    let backup = destination.with_extension(format!("backup-{}", db::new_uuid_v4()));
    let had_destination = match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "plan destination is a directory",
            ));
        }
        Ok(_) => {
            fs::rename(destination, &backup)?;
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    match fs::rename(source, destination) {
        Ok(()) => {
            if had_destination {
                fs::remove_file(backup)?;
            }
            Ok(())
        }
        Err(error) => {
            if had_destination {
                let _ = fs::rename(&backup, destination);
            }
            Err(error)
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), PlanArtifactError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), PlanArtifactError> {
    Ok(())
}

pub fn default_plan_artifact_path(worktree_root: &Path) -> PathBuf {
    worktree_root
        .parent()
        .unwrap_or(worktree_root)
        .join(DEFAULT_PLAN_ARTIFACT_PATH)
}

pub fn to_plan_progress_summary(artifact: &ParsedPlanArtifact) -> PlanProgressSummary {
    let total = u32::try_from(artifact.items.len()).unwrap_or(u32::MAX);
    let completed = u32::try_from(artifact.items.iter().filter(|item| item.checked).count())
        .unwrap_or(u32::MAX);

    PlanProgressSummary {
        total,
        completed,
        remaining: total.saturating_sub(completed),
        available: true,
        warnings: artifact.warnings.clone(),
    }
}

pub fn to_plan_artifact_detail(
    artifact: &ParsedPlanArtifact,
    source_path: Option<String>,
    last_modified: Option<String>,
) -> PlanArtifactDetail {
    PlanArtifactDetail {
        items: artifact
            .items
            .iter()
            .map(|item| PlanChecklistItem {
                checked: item.checked,
                label: item.label.clone(),
                nesting_level: u32::try_from(item.nesting_level).unwrap_or(u32::MAX),
                line_number: u32::try_from(item.line_number).unwrap_or(u32::MAX),
            })
            .collect(),
        warnings: artifact.warnings.clone(),
        source_path,
        last_modified,
    }
}

fn parse_checkbox_line(line: &str, line_number: usize) -> Option<ParsedPlanItem> {
    let leading_spaces = line.bytes().take_while(|byte| *byte == b' ').count();
    let rest = &line[leading_spaces..];
    let bytes = rest.as_bytes();

    if bytes.len() < 6 {
        return None;
    }

    if !matches!(bytes[0], b'-' | b'*') || bytes[1] != b' ' || bytes[2] != b'[' {
        return None;
    }

    let checked = match bytes[3] {
        b' ' => false,
        b'x' | b'X' => true,
        _ => return None,
    };

    if bytes[4] != b']' || bytes[5] != b' ' {
        return None;
    }

    let label = rest[6..].trim().to_string();
    Some(ParsedPlanItem {
        checked,
        label,
        nesting_level: leading_spaces / 2,
        line_number,
    })
}

fn looks_like_checkbox_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("- [") || trimmed.starts_with("* [")
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }

    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_plan_returns_not_found() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");

        let error = read_plan_artifact(&worktree, None).expect_err("missing plan fails");

        assert!(matches!(error, PlanArtifactError::NotFound));
    }

    #[test]
    fn empty_file_returns_empty_items() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::write(tempdir.path().join("plan.md"), "").expect("write plan");

        let artifact = read_plan_artifact(&worktree, None).expect("read plan");

        assert!(artifact.items.is_empty());
        assert!(artifact.warnings.is_empty());
    }

    #[test]
    fn valid_nested_checklist_with_mixed_levels() {
        let content = "\
# Plan
- [ ] root
  - [x] child
    * [X] grandchild
   - [ ] odd indentation
";

        let artifact = parse_plan_markdown(content);

        assert_eq!(
            artifact.items,
            vec![
                ParsedPlanItem {
                    checked: false,
                    label: "root".to_string(),
                    nesting_level: 0,
                    line_number: 2,
                },
                ParsedPlanItem {
                    checked: true,
                    label: "child".to_string(),
                    nesting_level: 1,
                    line_number: 3,
                },
                ParsedPlanItem {
                    checked: true,
                    label: "grandchild".to_string(),
                    nesting_level: 2,
                    line_number: 4,
                },
                ParsedPlanItem {
                    checked: false,
                    label: "odd indentation".to_string(),
                    nesting_level: 1,
                    line_number: 5,
                },
            ]
        );
        assert!(artifact.warnings.is_empty());
    }

    #[test]
    fn malformed_markdown_produces_warnings() {
        let content = "\
- [o] invalid marker
* [] missing marker
\t- [ ] tab indentation
- [x] valid
";

        let artifact = parse_plan_markdown(content);

        assert_eq!(artifact.items.len(), 1);
        assert_eq!(artifact.items[0].label, "valid");
        assert_eq!(artifact.warnings.len(), 3);
    }

    #[test]
    fn large_file_returns_file_too_large() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::write(
            tempdir.path().join("plan.md"),
            vec![b'a'; MAX_PLAN_ARTIFACT_SIZE_BYTES as usize + 1],
        )
        .expect("write large plan");

        let error = read_plan_artifact(&worktree, None).expect_err("large plan fails");

        assert!(matches!(
            error,
            PlanArtifactError::FileTooLarge {
                size,
                max: MAX_PLAN_ARTIFACT_SIZE_BYTES
            } if size == MAX_PLAN_ARTIFACT_SIZE_BYTES + 1
        ));
    }

    #[test]
    fn path_escape_returns_path_escape() {
        let tempdir = tempfile::tempdir().expect("create tempdir");

        let error = read_plan_artifact(tempdir.path(), Some("../outside-plan.md"))
            .expect_err("escape fails");

        assert!(matches!(error, PlanArtifactError::PathEscape { .. }));
    }

    #[test]
    fn non_regular_plan_is_rejected() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::create_dir(tempdir.path().join("plan.md")).expect("create plan directory");

        let error = read_plan_artifact(&worktree, None).expect_err("directory plan fails");

        assert!(matches!(error, PlanArtifactError::InvalidFileType { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn plan_reader_rejects_leaf_and_intermediate_symlink_escapes() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        let outside = tempfile::tempdir().expect("outside tempdir");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::write(outside.path().join("secret.md"), "- [ ] secret\n").expect("outside plan");

        symlink(
            outside.path().join("secret.md"),
            tempdir.path().join("plan.md"),
        )
        .expect("leaf symlink");
        let leaf = read_plan_artifact(&worktree, None).expect_err("leaf symlink fails");
        assert!(matches!(leaf, PlanArtifactError::InvalidFileType { .. }));

        fs::remove_file(tempdir.path().join("plan.md")).expect("remove leaf symlink");
        symlink(outside.path(), worktree.join("linked")).expect("intermediate symlink");
        let intermediate = read_plan_artifact(&worktree, Some("linked/secret.md"))
            .expect_err("intermediate escape fails");
        assert!(matches!(intermediate, PlanArtifactError::PathEscape { .. }));
    }

    #[test]
    fn staged_plan_publication_uses_frozen_bytes_and_replaces_existing_plan() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        let outbox = tempdir.path().join(".forge-outbox").join("exec-1");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::write(tempdir.path().join("plan.md"), "- [ ] old\n").expect("old plan");
        fs::write(outbox.join("plan.md"), "- [x] replacement\n").expect("outbox plan");

        assert!(
            stage_execution_outbox_plan(&outbox, &worktree, "exec-1").expect("candidate stages")
        );
        fs::write(outbox.join("plan.md"), "- [ ] changed after staging\n")
            .expect("mutate agent-writable source");
        assert!(publish_staged_execution_plan(&worktree, "exec-1").expect("plan publishes"));

        assert_eq!(
            fs::read_to_string(tempdir.path().join("plan.md")).expect("published plan reads"),
            "- [x] replacement\n"
        );
        discard_staged_execution_plan(&worktree, "exec-1").expect("stage discards");
    }

    #[test]
    fn failed_publication_preserves_the_host_owned_stage_for_recovery() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        let outbox = tempdir.path().join(".forge-outbox").join("exec-1");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::create_dir(tempdir.path().join("plan.md")).expect("blocking destination directory");
        fs::write(outbox.join("plan.md"), "- [ ] retained\n").expect("outbox plan");

        assert!(
            stage_execution_outbox_plan(&outbox, &worktree, "exec-1").expect("candidate stages")
        );
        fs::remove_dir_all(&outbox).expect("settle outbox");

        publish_staged_execution_plan(&worktree, "exec-1")
            .expect_err("directory destination rejects publication");

        fs::remove_dir(tempdir.path().join("plan.md")).expect("remove blocker");
        assert!(publish_staged_execution_plan(&worktree, "exec-1").expect("staged retry publishes"));
        assert_eq!(
            fs::read_to_string(tempdir.path().join("plan.md")).unwrap(),
            "- [ ] retained\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn publication_replaces_destination_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let worktree = tempdir.path().join("repo");
        let outbox = tempdir.path().join(".forge-outbox").join("exec-1");
        let outside_target = outside.path().join("hook");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::write(&outside_target, "untouched\n").expect("outside target");
        symlink(&outside_target, tempdir.path().join("plan.md")).expect("destination symlink");
        fs::write(outbox.join("plan.md"), "- [ ] safe\n").expect("outbox plan");

        assert!(
            stage_execution_outbox_plan(&outbox, &worktree, "exec-1").expect("candidate stages")
        );
        assert!(publish_staged_execution_plan(&worktree, "exec-1").expect("plan publishes"));

        assert_eq!(
            fs::read_to_string(&outside_target).expect("outside target reads"),
            "untouched\n"
        );
        assert!(fs::symlink_metadata(tempdir.path().join("plan.md"))
            .expect("published metadata")
            .file_type()
            .is_file());
    }

    #[cfg(unix)]
    #[test]
    fn outbox_plan_validation_rejects_symlinks_and_hard_links() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let outbox = tempdir.path().join("outbox");
        fs::create_dir_all(&outbox).expect("create outbox");
        let outside_plan = outside.path().join("plan.md");
        fs::write(&outside_plan, "- [ ] outside\n").expect("outside plan");
        symlink(&outside_plan, outbox.join("plan.md")).expect("source symlink");

        let symlink_error =
            validate_execution_outbox_plan(&outbox).expect_err("source symlink fails");
        assert!(matches!(
            symlink_error,
            PlanArtifactError::InvalidFileType { .. }
        ));

        fs::remove_file(outbox.join("plan.md")).expect("remove source symlink");
        fs::hard_link(&outside_plan, outbox.join("plan.md")).expect("source hard link");
        let hard_link_error =
            validate_execution_outbox_plan(&outbox).expect_err("source hard link fails");
        assert!(matches!(
            hard_link_error,
            PlanArtifactError::MultipleHardLinks { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn plan_outbox_creation_rejects_symlinked_root_and_execution_directory() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        fs::write(outside.path().join("keep"), "untouched\n").expect("outside marker");

        symlink(outside.path(), tempdir.path().join(".forge-outbox")).expect("outbox root symlink");
        let root_error = write_execution_outbox_plan(&worktree, "exec-1", "- [ ] safe\n")
            .expect_err("symlinked root fails");
        assert!(matches!(
            root_error,
            PlanArtifactError::InvalidFileType { .. }
        ));
        assert!(!outside.path().join("exec-1").exists());

        fs::remove_file(tempdir.path().join(".forge-outbox")).expect("remove root symlink");
        fs::create_dir(tempdir.path().join(".forge-outbox")).expect("create outbox root");
        symlink(
            outside.path(),
            tempdir.path().join(".forge-outbox").join("exec-1"),
        )
        .expect("execution symlink");
        let execution_error = prepare_execution_plan_outbox(&worktree, "exec-1", false, None)
            .expect_err("symlinked execution fails");
        assert!(matches!(
            execution_error,
            PlanArtifactError::InvalidFileType { .. }
        ));
        assert_eq!(
            fs::read_to_string(outside.path().join("keep")).expect("outside marker reads"),
            "untouched\n"
        );
        assert!(!outside.path().join(DEFAULT_PLAN_ARTIFACT_PATH).exists());
    }

    #[test]
    fn outbox_plan_validation_rejects_oversize_invalid_utf8_and_empty_checklist() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let outbox = tempdir.path().join("outbox");
        fs::create_dir_all(&outbox).expect("create outbox");
        let plan = outbox.join("plan.md");

        fs::write(&plan, vec![b'a'; MAX_PLAN_ARTIFACT_SIZE_BYTES as usize + 1])
            .expect("oversize plan");
        assert!(matches!(
            validate_execution_outbox_plan(&outbox).expect_err("oversize fails"),
            PlanArtifactError::FileTooLarge { .. }
        ));

        fs::write(&plan, [0xff, 0xfe]).expect("invalid UTF-8 plan");
        assert!(matches!(
            validate_execution_outbox_plan(&outbox).expect_err("invalid UTF-8 fails"),
            PlanArtifactError::InvalidUtf8 { .. }
        ));

        fs::write(&plan, "# No checklist\n").expect("empty checklist plan");
        assert!(matches!(
            validate_execution_outbox_plan(&outbox).expect_err("empty checklist fails"),
            PlanArtifactError::MissingChecklist { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn seeded_outbox_recovers_interrupted_private_install_after_link() {
        use std::os::unix::fs::MetadataExt;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        fs::create_dir_all(&outbox).expect("create outbox");
        let destination = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
        let installing = private_installation_path(&destination).expect("installation path");
        fs::write(&installing, "- [ ] recovered candidate\n").expect("write installation file");
        fs::hard_link(&installing, &destination).expect("link installation into place");
        assert_eq!(
            fs::metadata(&destination)
                .expect("destination metadata")
                .nlink(),
            2
        );

        let seeded = prepare_execution_plan_outbox(
            &worktree,
            "exec-1",
            true,
            Some("- [ ] fallback must not replace the candidate\n"),
        )
        .expect("recover seeded outbox");

        assert!(!seeded, "the linked destination was already committed");
        assert_eq!(
            fs::read_to_string(&destination).expect("read recovered candidate"),
            "- [ ] recovered candidate\n"
        );
        assert!(!installing.exists(), "installation name is cleaned up");
        assert_eq!(
            fs::metadata(&destination)
                .expect("destination metadata")
                .nlink(),
            1
        );
    }

    #[test]
    fn seeded_outbox_discards_interrupted_install_before_link() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        fs::create_dir_all(&outbox).expect("create outbox");
        let destination = outbox.join(DEFAULT_PLAN_ARTIFACT_PATH);
        let installing = private_installation_path(&destination).expect("installation path");
        fs::write(&installing, "- [ ] uncommitted bytes\n").expect("write installation file");

        let seeded = prepare_execution_plan_outbox(
            &worktree,
            "exec-1",
            true,
            Some("- [ ] authoritative fallback\n"),
        )
        .expect("recover and seed outbox");

        assert!(seeded, "the unlinked temporary was not committed");
        assert_eq!(
            fs::read_to_string(&destination).expect("read seeded candidate"),
            "- [ ] authoritative fallback\n"
        );
        assert!(!installing.exists(), "installation name is cleaned up");
    }

    #[cfg(unix)]
    #[test]
    fn staging_recovers_interrupted_private_install_after_link() {
        use std::os::unix::fs::MetadataExt;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::write(outbox.join(DEFAULT_PLAN_ARTIFACT_PATH), "- [ ] frozen\n")
            .expect("write outbox plan");

        let stage = execution_plan_stage_path(&worktree, "exec-1").expect("stage path");
        fs::create_dir_all(stage.parent().expect("stage parent")).expect("create stage root");
        let installing = private_installation_path(&stage).expect("installation path");
        fs::write(&installing, "- [ ] frozen\n").expect("write installation file");
        fs::hard_link(&installing, &stage).expect("link installation into place");
        assert_eq!(fs::metadata(&stage).expect("stage metadata").nlink(), 2);

        assert!(stage_execution_outbox_plan(&outbox, &worktree, "exec-1")
            .expect("recover staged candidate"));

        assert!(!installing.exists(), "installation name is cleaned up");
        assert_eq!(
            fs::read_to_string(&stage).expect("read stage"),
            "- [ ] frozen\n"
        );
        assert_eq!(fs::metadata(&stage).expect("stage metadata").nlink(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn restore_recovers_previous_marker_and_is_idempotent() {
        use std::os::unix::fs::MetadataExt;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::write(
            tempdir.path().join(DEFAULT_PLAN_ARTIFACT_PATH),
            "- [ ] previous\n",
        )
        .expect("write previous canonical plan");
        fs::write(
            outbox.join(DEFAULT_PLAN_ARTIFACT_PATH),
            "- [ ] replacement\n",
        )
        .expect("write outbox plan");
        assert!(stage_execution_outbox_plan(&outbox, &worktree, "exec-1").expect("stage candidate"));
        assert!(publish_staged_execution_plan(&worktree, "exec-1").expect("publish candidate"));

        let stage = execution_plan_stage_path(&worktree, "exec-1").expect("stage path");
        let previous = execution_plan_previous_path(&stage);
        let installing = private_installation_path(&previous).expect("installation path");
        fs::hard_link(&previous, &installing).expect("recreate interrupted installation link");
        assert_eq!(
            fs::metadata(&previous).expect("previous metadata").nlink(),
            2
        );

        restore_plan_before_abandon(&worktree, "exec-1").expect("restore previous plan");
        restore_plan_before_abandon(&worktree, "exec-1").expect("repeated restore is idempotent");

        assert_eq!(
            fs::read_to_string(tempdir.path().join(DEFAULT_PLAN_ARTIFACT_PATH))
                .expect("read restored plan"),
            "- [ ] previous\n"
        );
        assert!(!installing.exists(), "installation name is cleaned up");
        assert_eq!(
            fs::metadata(&previous).expect("previous metadata").nlink(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn restore_recovers_absent_marker_and_is_idempotent() {
        use std::os::unix::fs::MetadataExt;

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        fs::create_dir_all(&outbox).expect("create outbox");
        fs::write(
            outbox.join(DEFAULT_PLAN_ARTIFACT_PATH),
            "- [ ] replacement\n",
        )
        .expect("write outbox plan");
        assert!(stage_execution_outbox_plan(&outbox, &worktree, "exec-1").expect("stage candidate"));
        assert!(publish_staged_execution_plan(&worktree, "exec-1").expect("publish candidate"));

        let stage = execution_plan_stage_path(&worktree, "exec-1").expect("stage path");
        let absent = execution_plan_previous_absent_path(&stage);
        let installing = private_installation_path(&absent).expect("installation path");
        fs::hard_link(&absent, &installing).expect("recreate interrupted installation link");
        assert_eq!(fs::metadata(&absent).expect("absent metadata").nlink(), 2);

        restore_plan_before_abandon(&worktree, "exec-1").expect("restore absent plan");
        restore_plan_before_abandon(&worktree, "exec-1").expect("repeated restore is idempotent");

        assert!(
            !tempdir.path().join(DEFAULT_PLAN_ARTIFACT_PATH).exists(),
            "canonical plan remains absent"
        );
        assert!(!installing.exists(), "installation name is cleaned up");
        assert_eq!(fs::metadata(&absent).expect("absent metadata").nlink(), 1);
    }

    #[test]
    fn invalid_native_plan_write_preserves_existing_candidate() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let worktree = tempdir.path().join("repo");
        fs::create_dir_all(&worktree).expect("create worktree");
        write_execution_outbox_plan(&worktree, "exec-1", "- [ ] keep me\n")
            .expect("write initial plan");

        let error = write_execution_outbox_plan(&worktree, "exec-1", "# No checklist\n")
            .expect_err("invalid replacement fails");

        assert!(matches!(error, PlanArtifactError::MissingChecklist { .. }));
        let outbox =
            executors::execution_outbox_path(&worktree, "exec-1").expect("execution id is valid");
        assert_eq!(
            fs::read_to_string(outbox.join(DEFAULT_PLAN_ARTIFACT_PATH))
                .expect("read preserved candidate"),
            "- [ ] keep me\n"
        );
    }

    #[test]
    fn plan_progress_summary_counts_checked_and_unchecked_items() {
        let artifact = ParsedPlanArtifact {
            items: vec![
                ParsedPlanItem {
                    checked: true,
                    label: "one".to_string(),
                    nesting_level: 0,
                    line_number: 1,
                },
                ParsedPlanItem {
                    checked: true,
                    label: "two".to_string(),
                    nesting_level: 0,
                    line_number: 2,
                },
                ParsedPlanItem {
                    checked: true,
                    label: "three".to_string(),
                    nesting_level: 0,
                    line_number: 3,
                },
                ParsedPlanItem {
                    checked: false,
                    label: "four".to_string(),
                    nesting_level: 0,
                    line_number: 4,
                },
                ParsedPlanItem {
                    checked: false,
                    label: "five".to_string(),
                    nesting_level: 0,
                    line_number: 5,
                },
            ],
            warnings: vec!["line 6: malformed checkbox item".to_string()],
        };

        let summary = to_plan_progress_summary(&artifact);

        assert_eq!(summary.total, 5);
        assert_eq!(summary.completed, 3);
        assert_eq!(summary.remaining, 2);
        assert!(summary.available);
        assert_eq!(summary.warnings, artifact.warnings);
    }

    #[test]
    fn plan_artifact_detail_preserves_nesting_and_line_numbers() {
        let artifact = ParsedPlanArtifact {
            items: vec![
                ParsedPlanItem {
                    checked: false,
                    label: "root".to_string(),
                    nesting_level: 0,
                    line_number: 3,
                },
                ParsedPlanItem {
                    checked: true,
                    label: "child".to_string(),
                    nesting_level: 1,
                    line_number: 4,
                },
                ParsedPlanItem {
                    checked: false,
                    label: "grandchild".to_string(),
                    nesting_level: 2,
                    line_number: 8,
                },
            ],
            warnings: vec!["line 9: malformed checkbox item".to_string()],
        };

        let detail = to_plan_artifact_detail(
            &artifact,
            Some("/tmp/worktree/.forge/plan.md".to_string()),
            Some("2026-04-29T00:00:00Z".to_string()),
        );

        assert_eq!(detail.items.len(), 3);
        assert_eq!(detail.items[0].label, "root");
        assert_eq!(detail.items[0].nesting_level, 0);
        assert_eq!(detail.items[0].line_number, 3);
        assert!(!detail.items[0].checked);
        assert_eq!(detail.items[1].label, "child");
        assert_eq!(detail.items[1].nesting_level, 1);
        assert_eq!(detail.items[1].line_number, 4);
        assert!(detail.items[1].checked);
        assert_eq!(detail.items[2].label, "grandchild");
        assert_eq!(detail.items[2].nesting_level, 2);
        assert_eq!(detail.items[2].line_number, 8);
        assert!(!detail.items[2].checked);
        assert_eq!(detail.warnings, artifact.warnings);
        assert_eq!(
            detail.source_path.as_deref(),
            Some("/tmp/worktree/.forge/plan.md")
        );
        assert_eq!(
            detail.last_modified.as_deref(),
            Some("2026-04-29T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn read_plan_for_workspace_returns_error_for_absent_workspace() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = db::SqliteDb::new(pool);

        let error = read_plan_for_workspace(&db, "missing-workspace")
            .await
            .expect_err("missing workspace fails");

        assert!(matches!(
            error,
            PlanArtifactError::WorkspaceNotFound { workspace_id }
                if workspace_id == "missing-workspace"
        ));
    }
}
