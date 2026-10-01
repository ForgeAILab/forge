use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;

use api_types::{PlanArtifactDetail, PlanChecklistItem, PlanProgressSummary};
use db::{SqliteDb, WorkspaceRepo};

use crate::workspace_backend::{ResolvedWorkspace, WorkspaceBackendError, WorkspaceBackendRouter};

pub const DEFAULT_PLAN_ARTIFACT_PATH: &str = "plan.md";
pub const PLAN_ARTIFACT_AGENT_INSTRUCTION: &str = "Write an implementation plan as a Markdown checklist at `../plan.md`, next to the repository worktree and outside the git repository. This plan will be handed to the coder agent for execution. Each checklist item represents implementation work or verification the coder should complete. Use `- [ ]` for pending work and `- [x]` only for work that is already complete. Nest sub-items with 2-space indentation.";

const MAX_PLAN_ARTIFACT_SIZE_BYTES: u64 = 1_048_576;

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
            Self::IoError(error) => write!(f, "failed to read plan artifact: {error}"),
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
    let path = bounded_artifact_path(workspace_root, plan_path, MAX_PLAN_ARTIFACT_SIZE_BYTES)?;
    let content = fs::read_to_string(path)?;
    Ok(parse_plan_markdown(&content))
}

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
    let normalized_workspace_root = normalize_lexical(allowed_root);
    let normalized_candidate = normalize_lexical(&candidate);

    if !normalized_candidate.starts_with(&normalized_workspace_root) {
        return Err(PlanArtifactError::PathEscape {
            path: normalized_candidate,
        });
    }

    let metadata = match fs::metadata(&normalized_candidate) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(PlanArtifactError::NotFound);
        }
        Err(error) => return Err(PlanArtifactError::IoError(error)),
    };

    let size = metadata.len();
    if size > limit {
        return Err(PlanArtifactError::FileTooLarge { size, max: limit });
    }
    Ok(normalized_candidate)
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
