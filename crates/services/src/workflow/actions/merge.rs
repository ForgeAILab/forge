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
    task, workspace_id,
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
