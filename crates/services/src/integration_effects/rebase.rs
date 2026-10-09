//! Rebase effects and restart facts; callers retain their checkpoints.
use super::EffectWorkspace;
use crate::Result;
use api_types::{WorkspaceGitQuery, WorkspaceOwnerOperationOutcome};
use std::path::Path;

/// HEAD facts from the existing owner describe phase, collected only where
/// today's consumer already probes HEAD. Unknown HEAD is not invented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebaseHeadFacts {
    pub head_sha: String,
}

pub fn rebase_head_facts(
    state: crate::workspace_backend::WorkspaceState,
) -> Result<RebaseHeadFacts> {
    Ok(RebaseHeadFacts {
        head_sha: state.head_sha.ok_or_else(|| {
            crate::ServiceError::invalid_operation("rebased workspace has no HEAD")
        })?,
    })
}

pub struct RebaseEffectInput<'a> {
    pub workspace: &'a EffectWorkspace,
    pub worktree_path: &'a Path,
    pub target_branch: &'a str,
    pub handoff_conflicts: bool,
    pub expected_head_sha: Option<&'a str>,
    pub expected_target_sha: Option<&'a str>,
    /// None is today's unbounded local rebase; there is no default.
    pub deadline: Option<std::time::Duration>,
}

pub async fn rebase(input: &RebaseEffectInput<'_>) -> Result<WorkspaceOwnerOperationOutcome> {
    // The owner gate supplies a placement witness. Verify its local handle
    // and the frozen objects before starting or continuing a rebase.
    if input.workspace.owner == super::EffectOwner::Server
        && !input.workspace.handle.is_empty()
        && Path::new(&input.workspace.handle) != input.worktree_path
    {
        return Err(crate::ServiceError::conflict(
            "rebase workspace witness mismatch",
        ));
    }
    if let Some(expected) = input.expected_head_sha {
        if git::get_current_sha(input.worktree_path).await? != expected {
            return Err(crate::ServiceError::conflict(
                "rebase HEAD witness mismatch",
            ));
        }
    }
    if let Some(expected) = input.expected_target_sha {
        if super::merge::target_tip(input.worktree_path, input.target_branch).await? != expected {
            return Err(crate::ServiceError::conflict(
                "rebase target witness mismatch",
            ));
        }
    }
    match input.deadline {
        None => rebase_inner(input).await,
        Some(deadline) => tokio::time::timeout(deadline, rebase_inner(input))
            .await
            .map_err(|_| crate::ServiceError::invalid_operation("rebase command timed out"))?,
    }
}

async fn rebase_inner(input: &RebaseEffectInput<'_>) -> Result<WorkspaceOwnerOperationOutcome> {
    let in_progress = git::detect_rebase_in_progress(input.worktree_path).await?;
    if in_progress && !input.handoff_conflicts {
        // Resume an interrupted rebase the way a fresh conflicting one
        // ends without handoff: abort, restoring the branch.
        git::abort_rebase(input.worktree_path).await?;
        return Ok(WorkspaceOwnerOperationOutcome::Conflict {
            details: "aborted interrupted rebase".into(),
            conflict_paths: Vec::new(),
        });
    }
    if in_progress {
        return match git::continue_rebase_keeping_conflicts(input.worktree_path).await {
            Ok(conflict_paths) => Ok(WorkspaceOwnerOperationOutcome::Conflict {
                details: "resumed interrupted rebase".into(),
                conflict_paths,
            }),
            Err(git::GitError::UnsupportedRebaseConflict { details }) => {
                Ok(WorkspaceOwnerOperationOutcome::UnsupportedConflict { details })
            }
            Err(error) => Err(error.into()),
        };
    }
    if !git::is_worktree_clean(input.worktree_path).await? {
        return Ok(WorkspaceOwnerOperationOutcome::Dirty {
            files: git::status_porcelain(input.worktree_path).await?,
        });
    }
    match git::rebase(input.worktree_path, input.target_branch).await {
        Ok(()) => Ok(WorkspaceOwnerOperationOutcome::Rebased),
        Err(git::GitError::MergeConflict { stderr, .. }) => {
            if !input.handoff_conflicts {
                git::abort_rebase(input.worktree_path).await?;
                return Ok(WorkspaceOwnerOperationOutcome::Conflict {
                    details: stderr,
                    conflict_paths: Vec::new(),
                });
            }
            match git::continue_rebase_keeping_conflicts(input.worktree_path).await {
                Ok(conflict_paths) => Ok(WorkspaceOwnerOperationOutcome::Conflict {
                    details: stderr,
                    conflict_paths,
                }),
                Err(git::GitError::UnsupportedRebaseConflict { details }) => {
                    Ok(WorkspaceOwnerOperationOutcome::UnsupportedConflict { details })
                }
                Err(error) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// A read-only port: no effect, receipt, Task or event capability.
#[async_trait::async_trait]
pub trait GitFacts: Send + Sync {
    async fn query(&self, query: WorkspaceGitQuery, optional: bool) -> Result<Option<String>>;
}

pub struct RebaseRecoveryInput<'a> {
    pub previous_target: Option<&'a str>,
    pub recorded_target: &'a str,
    pub handoff_conflicts: bool,
}

pub enum RebaseRecoveryOutcome {
    Recorded(WorkspaceOwnerOperationOutcome),
    Perform { in_progress: bool },
}

async fn committed_marker_paths(facts: &impl GitFacts, target: &str) -> Result<Vec<String>> {
    Ok(facts
        .query(
            WorkspaceGitQuery::MarkerPaths {
                base: target.to_owned(),
                head: "HEAD".into(),
            },
            false,
        )
        .await?
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect())
}

pub async fn recover_rebase(
    facts: &impl GitFacts,
    input: &RebaseRecoveryInput<'_>,
) -> Result<RebaseRecoveryOutcome> {
    // Recovery must inspect the stopped rebase before ancestry: Git may have
    // already moved HEAD onto the target without finishing the rebase.
    let in_progress = input.previous_target.is_some()
        && facts
            .query(WorkspaceGitQuery::RebaseInProgress, false)
            .await?
            .is_some_and(|value| value.trim() == "true");
    let landed = input.previous_target.is_some()
        && !in_progress
        && facts
            .query(
                WorkspaceGitQuery::IsAncestor {
                    base: input.recorded_target.to_owned(),
                    head: "HEAD".into(),
                },
                true,
            )
            .await?
            .is_some();
    if landed {
        let paths = committed_marker_paths(facts, input.recorded_target).await?;
        let outcome = if input.handoff_conflicts && !paths.is_empty() {
            WorkspaceOwnerOperationOutcome::Conflict {
                details: "resumed committed conflict handoff".into(),
                conflict_paths: paths,
            }
        } else {
            WorkspaceOwnerOperationOutcome::Rebased
        };
        Ok(RebaseRecoveryOutcome::Recorded(outcome))
    } else {
        Ok(RebaseRecoveryOutcome::Perform { in_progress })
    }
}

pub async fn finish_rebase_recovery(
    facts: &impl GitFacts,
    target: &str,
    in_progress: bool,
    handoff_conflicts: bool,
    outcome: WorkspaceOwnerOperationOutcome,
) -> Result<WorkspaceOwnerOperationOutcome> {
    Ok(match outcome {
        WorkspaceOwnerOperationOutcome::Conflict {
            details,
            mut conflict_paths,
        } if in_progress && handoff_conflicts => {
            // Stops committed before the crash are absent from the resumed
            // continuation's list; preserve their union and encounter order.
            for path in committed_marker_paths(facts, target).await? {
                if !conflict_paths.contains(&path) {
                    conflict_paths.push(path);
                }
            }
            WorkspaceOwnerOperationOutcome::Conflict {
                details,
                conflict_paths,
            }
        }
        outcome => outcome,
    })
}
