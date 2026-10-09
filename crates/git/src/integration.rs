//! Shared owner-local integration effects. No storage or Task authority.
use crate::{GitError, Result};
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RebaseOutcome {
    Rebased,
    Conflict {
        details: String,
        conflict_paths: Vec<String>,
    },
    UnsupportedConflict {
        details: String,
    },
    Dirty {
        files: Vec<String>,
    },
}

pub async fn rebase(
    worktree: &Path,
    target: &str,
    handoff_conflicts: bool,
) -> Result<RebaseOutcome> {
    let in_progress = crate::detect_rebase_in_progress(worktree).await?;
    if in_progress && !handoff_conflicts {
        // Resume an interrupted rebase the way a fresh conflicting one
        // ends without handoff: abort, restoring the branch.
        crate::abort_rebase(worktree).await?;
        return Ok(RebaseOutcome::Conflict {
            details: "aborted interrupted rebase".into(),
            conflict_paths: Vec::new(),
        });
    }
    if in_progress {
        return match crate::continue_rebase_keeping_conflicts(worktree).await {
            Ok(conflict_paths) => Ok(RebaseOutcome::Conflict {
                details: "resumed interrupted rebase".into(),
                conflict_paths,
            }),
            Err(crate::GitError::UnsupportedRebaseConflict { details }) => {
                Ok(RebaseOutcome::UnsupportedConflict { details })
            }
            Err(error) => Err(error),
        };
    }
    if !crate::is_worktree_clean(worktree).await? {
        return Ok(RebaseOutcome::Dirty {
            files: crate::status_porcelain(worktree).await?,
        });
    }
    match crate::rebase(worktree, target).await {
        Ok(()) => Ok(RebaseOutcome::Rebased),
        Err(crate::GitError::MergeConflict { stderr, .. }) => {
            if !handoff_conflicts {
                crate::abort_rebase(worktree).await?;
                return Ok(RebaseOutcome::Conflict {
                    details: stderr,
                    conflict_paths: Vec::new(),
                });
            }
            match crate::continue_rebase_keeping_conflicts(worktree).await {
                Ok(conflict_paths) => Ok(RebaseOutcome::Conflict {
                    details: stderr,
                    conflict_paths,
                }),
                Err(crate::GitError::UnsupportedRebaseConflict { details }) => {
                    Ok(RebaseOutcome::UnsupportedConflict { details })
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// The same reviewed fast-forward algorithm serves both owners. Manual mode
/// retains the caller's merge-commit candidate. Witness checks belong to gates.
#[derive(Debug)]
pub enum MergeApplyOutcome {
    Applied,
    ManualFailed(GitError),
    ReviewRequired { reason: String },
    ExactObjectMismatch,
    TargetMoved,
}

pub struct FastForwardLimits {
    pub deadline: std::time::Duration,
    pub output_bytes: usize,
}

pub async fn apply_merge(
    repo: &Path,
    target_branch: &str,
    candidate: &str,
    reviewed: bool,
    already_merged: bool,
    expected_target: Option<&str>,
    limits: FastForwardLimits,
) -> Result<MergeApplyOutcome> {
    if !already_merged {
        crate::checkout_branch(repo, target_branch).await?;
    }
    if already_merged {
        return Ok(MergeApplyOutcome::Applied);
    }
    if let Some(expected) = expected_target {
        if crate::get_current_sha(repo).await? != expected {
            return Ok(MergeApplyOutcome::TargetMoved);
        }
    }
    if reviewed {
        let output = match tokio::time::timeout(
            limits.deadline,
            crate::command_output_bounded(
                repo,
                &["merge", "--ff-only", candidate],
                limits.output_bytes,
            ),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                return Ok(MergeApplyOutcome::ReviewRequired {
                    reason: match error {
                        GitError::Io(error) => error.to_string(),
                        error => error.to_string(),
                    },
                })
            }
            Err(_) => {
                return Ok(MergeApplyOutcome::ReviewRequired {
                    reason: "review command timed out".into(),
                })
            }
        };
        if !output.status.success() {
            return Ok(MergeApplyOutcome::ReviewRequired {
                reason: format!(
                    "git evidence unavailable: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            });
        }
        if crate::get_current_sha(repo).await? != candidate {
            return Ok(MergeApplyOutcome::ExactObjectMismatch);
        }
        Ok(MergeApplyOutcome::Applied)
    } else {
        match crate::merge_branch_into(repo, candidate).await {
            Ok(()) => Ok(MergeApplyOutcome::Applied),
            Err(error) => Ok(MergeApplyOutcome::ManualFailed(error)),
        }
    }
}
