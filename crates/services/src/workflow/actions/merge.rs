use async_trait::async_trait;
use db::TransitionLogRepo;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{
    merge_service::MergeOutcome,
    task_service::config::{runtime_retry_budget, RetryBudgetKind},
    workflow::{default_states, HookAction, HookContext, HookResult},
};

use super::common::{
    block_task, create_system_comment, merge_fix_budget_result,
    merge_fix_rejections_since_boundary, persist_merge_error, persist_target_repo_dirty_error,
    task, workspace_id, TARGET_MOVED_MARKER,
};

pub struct RunMerge;

#[async_trait]
impl HookAction for RunMerge {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(merge_service) = ctx.merge_service.as_ref() else {
            return HookResult::Skipped {
                reason: "merge service not configured".to_string(),
            };
        };
        if workspace_id(ctx).await.is_none() {
            return HookResult::Skipped {
                reason: "no worktree".to_string(),
            };
        }

        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };

        match merge_service.merge(ctx.task_id.clone()).await {
            Ok(MergeOutcome::ReviewRequired { reason }) => {
                if let Err(error) =
                    db::TaskRepo::set_review_passed_at(&*ctx.db, &task.id, None, &db::now_rfc3339())
                        .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                let current = match super::common::task(ctx).await {
                    Ok(task) => task,
                    Err(reason) => return HookResult::Failed { reason },
                };
                if let Err(error) = persist_merge_error(
                    ctx,
                    &current,
                    api_types::FailureKind::ReviewGateFailed,
                    &reason,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                merge_failure_result(
                    ctx,
                    &current,
                    format!("conformance review required: {reason}"),
                )
                .await
            }
            Ok(MergeOutcome::Done {
                after_sha, branch, ..
            }) => {
                if let Err(error) = create_system_comment(
                    ctx,
                    format!("Changes merged to {branch} (SHA: {after_sha})"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Cascade {
                    to: default_states::DONE.to_string(),
                    reason: "merge succeeded".to_string(),
                }
            }
            Ok(MergeOutcome::PullRequest {
                pr_url,
                branch,
                target_branch,
            }) => {
                let location = pr_url.unwrap_or_else(|| "provider URL pending".to_string());
                if let Err(error) = create_system_comment(
                    ctx,
                    format!("Pull request published from {branch} to {target_branch}: {location}"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Ok
            }
            Ok(MergeOutcome::TargetMoved {
                reason,
                target_branch,
            }) => target_moved_result(ctx, &task, &reason, &target_branch).await,
            Ok(MergeOutcome::Conflict {
                details,
                conflict_paths,
            }) => {
                let conflict_summary = if conflict_paths.is_empty() {
                    "unknown".to_string()
                } else {
                    conflict_paths
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                if let Err(error) = create_system_comment(
                    ctx,
                    format!("Merge conflict on files: {conflict_summary}"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                if let Err(error) =
                    persist_merge_error(ctx, &task, api_types::FailureKind::MergeConflict, &details)
                        .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "merge.failed".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::MergeFailed {
                        task_id: ctx.task_id.clone(),
                        reason: details.clone(),
                    },
                });
                merge_failure_result(ctx, &task, format!("merge conflict: {details}")).await
            }
            Ok(MergeOutcome::Dirty { files }) => {
                let details = if files.is_empty() {
                    "worktree has uncommitted changes".to_string()
                } else {
                    format!("worktree has uncommitted changes: {}", files.join(", "))
                };
                if let Err(error) =
                    persist_merge_error(ctx, &task, api_types::FailureKind::DirtyWorktree, &details)
                        .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                // The merging gate's only defined reject edge is merge_failed;
                // cascading to review wedges the task in merging forever.
                merge_failure_result(ctx, &task, details).await
            }
            Ok(MergeOutcome::TargetDirty { files }) => {
                let details = if files.is_empty() {
                    "target repository has uncommitted changes".to_string()
                } else {
                    format!(
                        "target repository has uncommitted changes: {}",
                        files.join(", ")
                    )
                };
                if let Err(error) = create_system_comment(
                    ctx,
                    format!("{details}. Clean, commit, or stash those changes, then retry merge."),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                if let Err(error) =
                    persist_target_repo_dirty_error(ctx, &task, &details, &files).await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                ctx.event_bus.publish(ForgeEvent {
                    event_type: "merge.failed".to_string(),
                    entity_id: ctx.task_id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::MergeFailed {
                        task_id: ctx.task_id.clone(),
                        reason: details.clone(),
                    },
                });
                if let Err(error) = block_task(
                    ctx,
                    &task,
                    &details,
                    api_types::FailureKind::TargetRepoDirty,
                    None,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Ok
            }
            Err(error) => HookResult::Failed {
                reason: error.to_string(),
            },
        }
    }
}

/// Give a Task blocked on an exhausted merge budget an annotation a client can
/// act on.
///
/// `block_task` only writes `blocked_json`; the `error_annotation` left behind
/// by `persist_merge_error` carries no `blocking_reason` and an empty
/// `recovery_actions`, so the Task advertised no way out at all. This mirrors
/// what the review-budget path already writes in
/// `task_service::execution::cascade`.
///
/// Only actions the recover endpoint actually accepts for a Task parked in
/// `merging` are advertised: `proceed_once` is review-only and would be
/// rejected, so it is deliberately left out.
async fn persist_merge_budget_annotation(ctx: &HookContext, reason: &str) -> db::Result<()> {
    let annotation = serde_json::json!({
        "type": api_types::FailureKind::MergeFixBudgetExhausted,
        "blocking_reason": reason,
        "message": reason,
        "detected_at": db::now_rfc3339(),
        "recovery_actions": [
            api_types::RecoveryAction::ResetRetryWindow,
            api_types::RecoveryAction::OpenInteractive,
            api_types::RecoveryAction::CancelTask,
        ],
    });
    let mut current = db::TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await?
        .ok_or(db::DbError::NotFound)?;
    for attempt in 0..3 {
        match db::TaskRepo::update(
            &*ctx.db,
            db::UpdateTask {
                id: current.id.clone(),
                expected_version: current.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(db::DbError::VersionConflict) if attempt < 2 => {
                current = db::TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// How many times one Task may be bounced back by contention before Forge
/// stops and asks a human. This is deliberately *not* the `merge_fix` budget
/// of 1: losing a merge race is normal, and a Task competing with N workers
/// can legitimately lose several in a row. Five is well above realistic
/// contention for a single repository while still bounding a livelock where
/// the target branch is being written faster than this Task can be reviewed.
const MAX_TARGET_MOVED_REBASES: i64 = 5;

fn target_moved_rebases_since_boundary(entries: &[db::TransitionLog]) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.to_state == default_states::DONE
            || entry.trigger_name.as_deref() == Some("reset_retry_window")
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| entry.trigger_reason.contains(TARGET_MOVED_MARKER))
        .count() as i64
}

/// The integration target moved while this Task was in review.
///
/// This is contention, not a fault, so it must not spend the Task's single
/// `merge_fix` retry — doing so blocked Tasks dead on the second lost race.
/// Forge does the mechanical part itself (rebase onto the new target) rather
/// than spending an LLM round trip on `git rebase`, then sends the Task back
/// through `merge_failed` so the rebased commit is re-reviewed before it can
/// merge. A rebase conflict is a genuine failure and falls through to the
/// normal budgeted path, because only the Worker can resolve it.
async fn target_moved_result(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    target_branch: &str,
) -> HookResult {
    let entries = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
        Ok(entries) => entries,
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };
    if target_moved_rebases_since_boundary(&entries) >= MAX_TARGET_MOVED_REBASES {
        let block_reason = format!(
            "{target_branch} kept advancing during review; stopped after {MAX_TARGET_MOVED_REBASES} rebase attempts"
        );
        if let Err(error) = block_task(
            ctx,
            task,
            &block_reason,
            api_types::FailureKind::MergeFixBudgetExhausted,
            None,
        )
        .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        if let Err(error) = persist_merge_budget_annotation(ctx, &block_reason).await {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        return HookResult::Ok;
    }

    let workspace = match db::WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => {
            return merge_failure_result(ctx, task, format!("{reason}; no workspace to rebase"))
                .await;
        }
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };
    let worktree_path = std::path::Path::new(&workspace.worktree_path);

    match git::is_worktree_clean(worktree_path).await {
        Ok(true) => {}
        Ok(false) => {
            // Uncommitted work in the worktree is the Worker's to resolve; a
            // rebase would refuse anyway.
            return merge_failure_result(
                ctx,
                task,
                format!("{reason}; worktree has uncommitted changes and cannot be rebased"),
            )
            .await;
        }
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    }

    match git::rebase(worktree_path, target_branch).await {
        Ok(()) => {
            if let Err(error) = create_system_comment(
                ctx,
                format!("Rebased onto {target_branch} after it advanced during review: {reason}"),
            )
            .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            if let Err(error) =
                crate::task_service::mark_coordination_review_pending_if_root(&ctx.db, task).await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            HookResult::Cascade {
                to: default_states::MERGE_FAILED.to_string(),
                reason: format!("{TARGET_MOVED_MARKER} {reason}; rebased onto {target_branch}, re-review required"),
            }
        }
        Err(git::GitError::MergeConflict { stderr, .. }) => {
            let _ = git::abort_rebase(worktree_path).await;
            merge_failure_result(
                ctx,
                task,
                format!("rebase onto {target_branch} conflicted: {stderr}"),
            )
            .await
        }
        Err(error) => HookResult::Failed {
            reason: error.to_string(),
        },
    }
}

async fn merge_failure_result(ctx: &HookContext, task: &db::Task, reason: String) -> HookResult {
    let budget = match runtime_retry_budget(
        task,
        RetryBudgetKind::MergeFix,
        Some(&ctx.state_config),
        ctx.gate_config.as_ref(),
    ) {
        Ok(budget) => budget,
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };
    let existing_follow_ups = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
        Ok(entries) => merge_fix_rejections_since_boundary(&entries),
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };

    if existing_follow_ups >= i64::from(budget) {
        let block_reason = "merge-fix retry budget exhausted";
        if let Err(error) = block_task(
            ctx,
            task,
            block_reason,
            api_types::FailureKind::MergeFixBudgetExhausted,
            None,
        )
        .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        if let Err(error) = persist_merge_budget_annotation(ctx, block_reason).await {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        HookResult::Ok
    } else {
        HookResult::Cascade {
            to: default_states::MERGE_FAILED.to_string(),
            reason,
        }
    }
}

pub struct CheckMergeFixBudget;

#[async_trait]
impl HookAction for CheckMergeFixBudget {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        merge_fix_budget_result(ctx).await.unwrap_or(HookResult::Ok)
    }
}

pub struct AutoCascadeOnMergeResult;

#[async_trait]
impl HookAction for AutoCascadeOnMergeResult {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.merge_service.is_none() {
            return HookResult::Skipped {
                reason: "merge service not configured".to_string(),
            };
        }
        if workspace_id(ctx).await.is_none() {
            return HookResult::Skipped {
                reason: "run_merge was skipped".to_string(),
            };
        }
        HookResult::Ok
    }
}
