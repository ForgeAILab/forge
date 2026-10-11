//! Local Git effects and owner-result facts. Authority and evidence are callers' work.
use super::{EffectWorkspace, MergeOutcome};
use crate::{Result, ServiceError};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReviewedMergeObject {
    pub commit_sha: String,
    pub base_sha: String,
}

pub struct MergeEffectInput<'a> {
    pub workspace: &'a EffectWorkspace,
    pub worktree_path: &'a Path,
    pub repo_path: &'a Path,
    pub target_branch: &'a str,
    pub task_branch: &'a str,
    /// Retained only for the existing diagnostic when abort fails.
    pub diagnostic_entity_id: &'a str,
    pub before_sha: &'a str,
    pub expected_head_sha: &'a str,
    pub observed_target_sha: &'a str,
    pub reviewed: Option<ReviewedMergeObject>,
}

pub enum MergeCandidateOutcome {
    Ready { already_merged: bool },
    Refused(MergeOutcome),
}

pub enum MergeApplyOutcome {
    Applied,
    ManualFailed(git::GitError),
    ReviewRequired { reason: String },
    ExactObjectMismatch,
}

pub async fn merge_cleanliness(
    worktree_path: &Path,
    repo_path: &Path,
) -> Result<Option<MergeOutcome>> {
    if !git::is_worktree_clean(worktree_path).await? {
        return Ok(Some(MergeOutcome::Dirty {
            files: git::status_porcelain(worktree_path).await?,
        }));
    }
    if !git::is_worktree_clean(repo_path).await? {
        return Ok(Some(MergeOutcome::TargetDirty {
            files: git::status_porcelain(repo_path).await?,
        }));
    }
    Ok(None)
}

pub async fn expected_target(
    repo_path: &Path,
    target_branch: &str,
    expected_target_sha: &str,
) -> Result<Option<MergeOutcome>> {
    if !expected_target_sha.is_empty() {
        let target_sha = ::review::contract::git_read(
            repo_path,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{target_branch}"),
            ],
        )
        .await
        .map_err(ServiceError::invalid_operation)?;
        if target_sha.trim() != expected_target_sha {
            return Ok(Some(MergeOutcome::TargetMoved {
                reason: format!(
                    "{target_branch} advanced to {} since this Task was reviewed against {}",
                    short_sha(target_sha.trim()),
                    short_sha(expected_target_sha)
                ),
                target_branch: target_branch.to_owned(),
            }));
        }
    }
    Ok(None)
}

/// These reads remain separate because today's candidate evidence is recorded
/// before acquiring the review guard; the target probe runs under that guard.
pub async fn merge_heads(repo_path: &Path, worktree_path: &Path) -> Result<(String, String)> {
    Ok((
        git::get_current_sha(repo_path).await?,
        git::get_current_sha(worktree_path).await?,
    ))
}

pub async fn target_tip(repo_path: &Path, target_branch: &str) -> Result<String> {
    ::review::contract::git_read(
        repo_path,
        &[
            "rev-parse",
            "--verify",
            &format!("refs/heads/{target_branch}"),
        ],
    )
    .await
    .map(|sha| sha.trim().to_owned())
    .map_err(ServiceError::invalid_operation)
}

pub async fn validate_merge_candidate(
    input: &MergeEffectInput<'_>,
) -> Result<MergeCandidateOutcome> {
    let already_merged = ::review::contract::git_read(
        input.repo_path,
        &[
            "merge-base",
            "--is-ancestor",
            input.expected_head_sha,
            input.observed_target_sha,
        ],
    )
    .await
    .is_ok();
    if let Some(candidate) = &input.reviewed {
        let current_sha = git::get_current_sha(input.worktree_path).await?;
        if candidate.commit_sha != current_sha {
            return Ok(MergeCandidateOutcome::Refused(
                MergeOutcome::ReviewRequired {
                    reason: "reviewed commit changed since review; fresh review required".into(),
                },
            ));
        }
        if !already_merged && candidate.base_sha != input.observed_target_sha {
            return Ok(MergeCandidateOutcome::Refused(MergeOutcome::TargetMoved {
                reason: format!(
                    "{} advanced to {} since this Task was reviewed against {}",
                    input.target_branch,
                    short_sha(input.observed_target_sha),
                    short_sha(&candidate.base_sha)
                ),
                target_branch: input.target_branch.to_owned(),
            }));
        }
    }
    Ok(MergeCandidateOutcome::Ready { already_merged })
}

/// The reviewed mode fast-forwards only the immutable approved object; manual
/// mode retains its existing merge-commit behavior. No evidence is written here.
pub async fn apply_merge(
    input: &MergeEffectInput<'_>,
    already_merged: bool,
) -> Result<MergeApplyOutcome> {
    let candidate = input
        .reviewed
        .as_ref()
        .map(|object| object.commit_sha.as_str())
        .unwrap_or(input.task_branch);
    Ok(
        match git::integration::apply_merge(
            input.repo_path,
            input.target_branch,
            candidate,
            input.reviewed.is_some(),
            already_merged,
            None,
            git::integration::FastForwardLimits {
                deadline: std::time::Duration::from_secs(30),
                output_bytes: ::review::contract::MAX_EVIDENCE_BYTES,
            },
        )
        .await?
        {
            git::integration::MergeApplyOutcome::Applied => MergeApplyOutcome::Applied,
            git::integration::MergeApplyOutcome::ManualFailed(error) => {
                MergeApplyOutcome::ManualFailed(error)
            }
            git::integration::MergeApplyOutcome::ReviewRequired { reason } => {
                MergeApplyOutcome::ReviewRequired { reason }
            }
            git::integration::MergeApplyOutcome::ExactObjectMismatch
            | git::integration::MergeApplyOutcome::TargetMoved => {
                MergeApplyOutcome::ExactObjectMismatch
            }
        },
    )
}

/// Finish fact collection after today's authority guard has been released.
/// This preserves the old read/abort ordering relative to that transaction.
pub async fn merge_result(
    input: &MergeEffectInput<'_>,
    already_merged: bool,
    applied: MergeApplyOutcome,
) -> Result<MergeOutcome> {
    match applied {
        MergeApplyOutcome::Applied => {
            let after_sha = if already_merged {
                input.expected_head_sha.to_owned()
            } else {
                git::get_current_sha(input.repo_path).await?
            };
            Ok(MergeOutcome::Done {
                before_sha: input.before_sha.to_owned(),
                after_sha,
                branch: input.target_branch.to_owned(),
            })
        }
        MergeApplyOutcome::ReviewRequired { reason } => Ok(MergeOutcome::ReviewRequired { reason }),
        MergeApplyOutcome::ExactObjectMismatch => Ok(MergeOutcome::TargetMoved {
            reason: "integration target changed during merge; reviewed content was not integrated"
                .into(),
            target_branch: input.target_branch.to_owned(),
        }),
        MergeApplyOutcome::ManualFailed(git::GitError::MergeConflict { stderr, .. }) => {
            let conflict_paths = read_conflict_paths(input.repo_path).await;
            if let Err(error) = git::abort_merge(input.repo_path).await {
                tracing::warn!(target: "services::merge_service",task_id = %input.diagnostic_entity_id, %error, "failed to abort merge");
            }
            Ok(MergeOutcome::Conflict {
                details: stderr,
                conflict_paths,
                target_branch: input.target_branch.to_owned(),
            })
        }
        MergeApplyOutcome::ManualFailed(error) => Err(error.into()),
    }
}

pub async fn unresolved_marker_paths(
    worktree_path: &Path,
    target_branch: &str,
    handed_off_paths: &[String],
) -> Result<Vec<String>> {
    if handed_off_paths.is_empty() {
        return Ok(Vec::new());
    }
    let marker_paths =
        git::paths_adding_conflict_markers(worktree_path, target_branch, "HEAD").await?;
    Ok(marker_paths
        .into_iter()
        .filter(|path| handed_off_paths.contains(path))
        .collect())
}

pub fn owner_merge_outcome(
    result: api_types::WorkspaceMergeOutcome,
    target_branch: &str,
) -> MergeOutcome {
    use api_types::WorkspaceMergeOutcome;
    match result {
        WorkspaceMergeOutcome::ReviewRequired { reason } => MergeOutcome::ReviewRequired { reason },
        WorkspaceMergeOutcome::TargetMoved {
            reason,
            target_branch,
        } => MergeOutcome::TargetMoved {
            reason,
            target_branch,
        },
        WorkspaceMergeOutcome::Done {
            before_sha,
            after_sha,
            branch,
        } => MergeOutcome::Done {
            before_sha,
            after_sha,
            branch,
        },
        WorkspaceMergeOutcome::Conflict {
            details,
            conflict_paths,
        } => MergeOutcome::Conflict {
            details,
            conflict_paths: conflict_paths.into_iter().map(PathBuf::from).collect(),
            target_branch: target_branch.to_owned(),
        },
        WorkspaceMergeOutcome::Dirty { files } => MergeOutcome::Dirty { files },
        WorkspaceMergeOutcome::TargetDirty { files } => MergeOutcome::TargetDirty { files },
        WorkspaceMergeOutcome::UnresolvedConflictMarkers { paths } => {
            MergeOutcome::UnresolvedConflictMarkers { paths }
        }
    }
}

fn short_sha(sha: &str) -> &str {
    sha.get(..12).unwrap_or(sha)
}

async fn read_conflict_paths(worktree_path: &Path) -> Vec<PathBuf> {
    match git::conflict_paths(worktree_path).await {
        Ok(paths) => paths.into_iter().map(PathBuf::from).collect(),
        Err(error) => {
            tracing::warn!(target: "services::merge_service", worktree_path = %worktree_path.display(), %error, "failed to read merge conflict paths");
            Vec::new()
        }
    }
}
