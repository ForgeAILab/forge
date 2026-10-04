use async_trait::async_trait;
use db::TransitionLogRepo;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{
    merge_service::MergeOutcome,
    task_service::config::{runtime_retry_budget, RetryBudgetKind},
    workflow::{
        default_states, HookAction, HookContext, HookResult, CONFLICT_HANDOFF_MARKER,
        REVIEW_REFRESH_MARKER, TARGET_MOVED_MARKER,
    },
};

use super::common::{
    block_task, block_task_with_annotation, create_system_comment, merge_fix_budget_result,
    persist_merge_error, persist_target_repo_dirty_error, task, workspace_id,
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

        let outcome = match crate::workflow::engine::durable::hook_effect(
            &ctx.task_id,
            "merge_outcome",
        )
        .await
        {
            Ok(Some(recorded)) => serde_json::from_str::<MergeOutcome>(&recorded)
                .map_err(|e| crate::ServiceError::invalid_operation(e.to_string())),
            Ok(None) => {
                let result = async {
                    use crate::workflow::engine::durable::{
                        current_hook, hook_effect, record_hook_effect,
                    };
                    if current_hook(&ctx.task_id).is_some() {
                        if let Some(value) = hook_effect(&ctx.task_id, "merge_intent").await? {
                            let intent: crate::merge_service::MergeIntent =
                                serde_json::from_str(&value).map_err(|e| {
                                    crate::ServiceError::invalid_operation(e.to_string())
                                })?;
                            if let Some(done) = merge_service.completed_hook_merge(&intent).await? {
                                return Ok(done);
                            }
                        } else {
                            let intent = merge_service.hook_merge_intent(&ctx.task_id).await?;
                            record_hook_effect(
                                &ctx.task_id,
                                "merge_intent",
                                &serde_json::to_value(intent).map_err(|e| {
                                    crate::ServiceError::invalid_operation(e.to_string())
                                })?,
                            )
                            .await?;
                        }
                    }
                    merge_service.merge(ctx.task_id.clone()).await
                }
                .await;
                if let Ok(outcome) = &result {
                    if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                        &ctx.task_id,
                        "merge_outcome",
                        &serde_json::to_value(outcome).expect("merge outcome serializes"),
                    )
                    .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                }
                result
            }
            Err(error) => Err(error),
        };
        // The merge writes through the workspace and execution ledgers, so a
        // Task snapshot read before it is already stale. Every compare-and-set
        // below has to carry the version the merge left behind: with the
        // pre-merge version they all fail as a version conflict, and because
        // this hook is not blocking that failure is swallowed -- the Task sits
        // in `merging` with no blocker, no annotation and no follow-up, which
        // is what every real merge conflict did.
        let mut task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };

        match outcome {
            Ok(MergeOutcome::ReviewRequired { reason }) => {
                if let Err(error) = db::TaskRepo::set_review_passed_at_cas(
                    &*ctx.db,
                    &task.id,
                    task.version,
                    None,
                    &db::now_rfc3339(),
                )
                .await
                .map(|updated| {
                    task = updated;
                }) {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Cascade {
                    to: default_states::MERGE_FAILED.to_string(),
                    reason: format!(
                        "{REVIEW_REFRESH_MARKER} conformance review required: {reason}"
                    ),
                }
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
            Ok(MergeOutcome::TargetMoved {
                reason,
                target_branch,
            }) => {
                if let Err(error) = db::TaskRepo::set_review_passed_at_cas(
                    &*ctx.db,
                    &task.id,
                    task.version,
                    None,
                    &db::now_rfc3339(),
                )
                .await
                .map(|updated| {
                    task = updated;
                }) {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                target_moved_result(ctx, &task, &reason, &target_branch).await
            }
            Ok(MergeOutcome::Conflict {
                details,
                conflict_paths,
                target_branch,
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
                match persist_merge_error(
                    ctx,
                    &task,
                    api_types::FailureKind::MergeConflict,
                    &details,
                )
                .await
                {
                    Ok(updated) => task = updated,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        }
                    }
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
                // A conflict is the Worker's to reconcile, not a human's: the
                // candidate is rebased onto the target and any conflict is
                // committed with markers and handed back, exactly as when the
                // target moves under an agent-reviewed candidate. A merge-fix
                // budget of 0 opts the Project out of automatic merge repair.
                match runtime_retry_budget(
                    &task,
                    RetryBudgetKind::MergeFix,
                    Some(&ctx.state_config),
                    ctx.gate_config.as_ref(),
                ) {
                    Ok(0) => {
                        return merge_failure_result(
                            ctx,
                            &task,
                            format!("merge conflict: {details}"),
                            api_types::FailureKind::MergeConflict,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                }
                if let Err(error) = db::TaskRepo::set_review_passed_at_cas(
                    &*ctx.db,
                    &task.id,
                    task.version,
                    None,
                    &db::now_rfc3339(),
                )
                .await
                .map(|updated| {
                    task = updated;
                }) {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                target_moved_result(
                    ctx,
                    &task,
                    &format!("merge into {target_branch} conflicted on {conflict_summary}"),
                    &target_branch,
                )
                .await
            }
            Ok(MergeOutcome::Dirty { files }) => {
                let details = if files.is_empty() {
                    "worktree has uncommitted changes".to_string()
                } else {
                    format!("worktree has uncommitted changes: {}", files.join(", "))
                };
                match persist_merge_error(
                    ctx,
                    &task,
                    api_types::FailureKind::DirtyWorktree,
                    &details,
                )
                .await
                {
                    Ok(updated) => task = updated,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        }
                    }
                }
                // The merging gate's only defined reject edge is merge_failed;
                // cascading to review wedges the task in merging forever.
                merge_failure_result(ctx, &task, details, api_types::FailureKind::DirtyWorktree)
                    .await
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
                match persist_target_repo_dirty_error(ctx, &task, &details, &files).await {
                    Ok(updated) => task = updated,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        }
                    }
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
            Ok(MergeOutcome::UnresolvedConflictMarkers { paths }) => {
                // The Worker was handed this conflict and did not reconcile
                // it; escalate rather than merge the markers or loop.
                let block_reason = format!(
                    "unresolved Git conflict markers remain in handed-off file(s) {} after the Worker's conflict repair",
                    paths.join(", ")
                );
                if let Err(error) = block_task_with_annotation(
                    ctx,
                    &task,
                    &block_reason,
                    api_types::FailureKind::MergeConflict,
                    None,
                    Some(manual_merge_recovery_annotation(
                        ctx,
                        api_types::FailureKind::MergeConflict,
                        &block_reason,
                        "manual_workspace_repair",
                    )),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Ok
            }
            Err(crate::ServiceError::ProjectPaused { .. }) => {
                if let Err(error) =
                    crate::deferred_dispatch::defer_integration_for_pause(&ctx.db, &task).await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                HookResult::Skipped {
                    reason: "project paused; integration deferred".to_owned(),
                }
            }
            Err(error) => {
                merge_failure_result(
                    ctx,
                    &task,
                    error.to_string(),
                    api_types::FailureKind::WorkspaceError,
                )
                .await
            }
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
fn merge_budget_annotation(reason: &str) -> String {
    serde_json::json!({
        "type": api_types::FailureKind::MergeFixBudgetExhausted,
        "blocking_reason": reason,
        "message": reason,
        "detected_at": db::now_rfc3339(),
    })
    .to_string()
}

/// How many times one Task may be bounced back by contention before Forge
/// stops and asks a human. This is deliberately *not* the `merge_fix` budget
/// of 1: losing a merge race is normal, and a Task competing with N workers
/// can legitimately lose several in a row. Five is well above realistic
/// contention for a single repository while still bounding a livelock where
/// the target branch is being written faster than this Task can be reviewed.
const MAX_TARGET_MOVED_REBASES: i64 = 5;

fn target_moved_rebases_since_boundary(entries: &[db::TransitionLog]) -> i64 {
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    let entries = crate::task_diagnostics::entries_since_retry_window_boundary(entries, None);
    entries
        .iter()
        .filter(|entry| {
            entry.trigger_reason.contains(TARGET_MOVED_MARKER)
                && entry.triggered_by == workflow_actor
        })
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
/// manual-repair path. Managed Task agents cannot rebase or write the linked
/// Git metadata, so Forge must not dispatch a follow-up they cannot complete.
pub(crate) async fn target_moved_result(
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
        if let Err(error) = block_task_with_annotation(
            ctx,
            task,
            &block_reason,
            api_types::FailureKind::MergeFixBudgetExhausted,
            None,
            Some(merge_budget_annotation(&block_reason)),
        )
        .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        return HookResult::Ok;
    }

    let workspace = match db::WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id).await {
        Ok(Some(workspace)) => workspace,
        Ok(None) => {
            return merge_failure_result(
                ctx,
                task,
                format!("{reason}; no workspace to rebase"),
                api_types::FailureKind::WorkspaceError,
            )
            .await;
        }
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };
    let daemon_owned =
        match db::WorkspacePlacementRepo::get_by_workspace_id(&*ctx.db, &workspace.id).await {
            Ok(placement) => placement
                .is_some_and(|placement| placement.owner_kind == db::PlacementOwnerKind::Daemon),
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                }
            }
        };
    let workspace = if daemon_owned {
        workspace
    } else {
        match crate::task_service::workspace::ensure_valid(
            &ctx.db,
            &ctx.workspace_root,
            task,
            workspace,
            ctx.repo_cache_locks.clone(),
            false,
            &ctx.workspace_backend_router,
        )
        .await
        {
            Ok(workspace) => workspace,
            Err(error) => {
                return merge_failure_result(
                    ctx,
                    task,
                    error.to_string(),
                    api_types::FailureKind::WorkspaceError,
                )
                .await;
            }
        }
    };
    let resolved = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
        &ctx.workspace_backend_router,
        &ctx.db,
        &workspace,
        &ctx.workspace_root,
    )
    .await
    .map_err(crate::ServiceError::from);
    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            }
        }
    };
    let handoff_conflicts =
        match crate::task_hierarchy::coordination_root_has_subtasks(&ctx.db, task).await {
            Ok(coordination_root) => !coordination_root,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                }
            }
        };
    let durable = async {
        use crate::workflow::engine::durable::{hook_effect, record_hook_effect};
        if let Some(recorded) = hook_effect(&ctx.task_id, "rebase_outcome").await? {
            return serde_json::from_str::<api_types::WorkspaceOwnerOperationOutcome>(&recorded)
                .map_err(|e| crate::ServiceError::invalid_operation(e.to_string()));
        }
        let previous = hook_effect(&ctx.task_id, "rebase_target").await?;
        let target = match previous.as_deref() {
            Some(value) => serde_json::from_str::<String>(value)
                .map_err(|e| crate::ServiceError::invalid_operation(e.to_string()))?,
            None => {
                let target = resolved
                    .git_query(
                        api_types::WorkspaceGitQuery::ResolveRef {
                            reference: target_branch.to_owned(),
                        },
                        false,
                    )
                    .await?
                    .ok_or_else(|| crate::ServiceError::invalid_operation("rebase target missing"))?
                    .trim()
                    .to_owned();
                record_hook_effect(&ctx.task_id, "rebase_target", &serde_json::json!(target))
                    .await?;
                target
            }
        };
        // A committed clean/conflicted rebase contains the recorded target.
        // Reconstruct its handoff rather than applying another rebase.
        let landed = previous.is_some()
            && resolved
                .git_query(
                    api_types::WorkspaceGitQuery::IsAncestor {
                        base: target.clone(),
                        head: "HEAD".into(),
                    },
                    true,
                )
                .await?
                .is_some();
        let outcome = if landed {
            let paths = resolved
                .git_query(
                    api_types::WorkspaceGitQuery::MarkerPaths {
                        base: target,
                        head: "HEAD".into(),
                    },
                    false,
                )
                .await?
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if handoff_conflicts && !paths.is_empty() {
                api_types::WorkspaceOwnerOperationOutcome::Conflict {
                    details: "resumed committed conflict handoff".into(),
                    conflict_paths: paths,
                }
            } else {
                api_types::WorkspaceOwnerOperationOutcome::Rebased
            }
        } else {
            resolved
                .rebase_target(target_branch, handoff_conflicts)
                .await?
        };
        record_hook_effect(
            &ctx.task_id,
            "rebase_outcome",
            &serde_json::to_value(&outcome).expect("rebase outcome serializes"),
        )
        .await?;
        Ok::<_, crate::ServiceError>(outcome)
    }
    .await;
    match durable {
        Ok(api_types::WorkspaceOwnerOperationOutcome::Rebased) => {
            resolved.record_head_best_effort(&ctx.db).await;
            if let Err(error) = create_system_comment(ctx,
                format!("Rebased onto {target_branch} after it advanced during review: {reason}"))
                .await
            {
                return HookResult::Failed { reason: error.to_string() };
            }
            HookResult::Cascade {
                to: default_states::MERGE_FAILED.to_string(),
                reason: format!("{REVIEW_REFRESH_MARKER} {TARGET_MOVED_MARKER} {reason}; rebased onto {target_branch}, re-review required"),
            }
        }
        Ok(api_types::WorkspaceOwnerOperationOutcome::Dirty { .. }) => merge_failure_result(ctx, task,
            format!("{reason}; worktree has uncommitted changes and cannot be rebased"), api_types::FailureKind::DirtyWorktree).await,
        Ok(api_types::WorkspaceOwnerOperationOutcome::Conflict { details, conflict_paths }) => {
            if handoff_conflicts && !conflict_paths.is_empty() {
                resolved.record_head_best_effort(&ctx.db).await;
                conflict_handoff_result(ctx, task, target_branch, &conflict_paths).await
            } else {
                merge_failure_result(ctx, task, format!("rebase onto {target_branch} conflicted: {details}"), api_types::FailureKind::MergeConflict).await
            }
        }
        Ok(api_types::WorkspaceOwnerOperationOutcome::UnsupportedConflict { details }) => merge_failure_result(ctx, task,
            format!("rebase onto {target_branch} has a conflict requiring manual workspace repair: {details}"), api_types::FailureKind::MergeConflict).await,
        Ok(_) => HookResult::Failed { reason: "workspace owner returned an invalid rebase outcome".into() },
        Err(error) => HookResult::Failed { reason: error.to_string() },
    }
}

/// How many rebase conflicts one Task may be handed back to its Worker before
/// Forge escalates. Each handoff is a *new* conflict — a sibling landed first
/// again — not a failed repair; a repair that leaves markers behind escalates
/// on its own through [`MergeOutcome::UnresolvedConflictMarkers`].
const MAX_CONFLICT_HANDOFFS: i64 = 5;

fn conflict_handoffs_since_boundary(entries: &[db::TransitionLog]) -> i64 {
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    crate::task_diagnostics::entries_since_retry_window_boundary(entries, None)
        .iter()
        .filter(|entry| {
            entry.trigger_reason.contains(CONFLICT_HANDOFF_MARKER)
                && entry.triggered_by == workflow_actor
        })
        .count() as i64
}

/// A rebase onto the moved target conflicted and Forge committed the conflict
/// with its markers. Send the Task to `merge_failed` so its Worker reconciles
/// the marked files — editing files is all that takes, and the Worker cannot
/// run the rebase itself — after which the repair gets a fresh review.
async fn conflict_handoff_result(
    ctx: &HookContext,
    task: &db::Task,
    target_branch: &str,
    paths: &[String],
) -> HookResult {
    let entries = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
        Ok(entries) => entries,
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };
    let files = paths.join(", ");
    if conflict_handoffs_since_boundary(&entries) >= MAX_CONFLICT_HANDOFFS {
        let block_reason = format!(
            "rebase onto {target_branch} conflicted again after {MAX_CONFLICT_HANDOFFS} conflict repairs (now in {files}); the branch is committed with conflict markers for repair"
        );
        if let Err(error) = block_task_with_annotation(
            ctx,
            task,
            &block_reason,
            api_types::FailureKind::MergeConflict,
            None,
            Some(manual_merge_recovery_annotation(
                ctx,
                api_types::FailureKind::MergeConflict,
                &block_reason,
                "manual_workspace_repair",
            )),
        )
        .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        return HookResult::Ok;
    }

    let details =
        format!("rebased onto {target_branch}; conflicts were committed with markers in: {files}");
    if let Err(error) = create_system_comment(
        ctx,
        format!("Merge conflict handed back to the Worker: {details}"),
    )
    .await
    {
        return HookResult::Failed {
            reason: error.to_string(),
        };
    }
    if let Err(error) =
        persist_merge_error(ctx, task, api_types::FailureKind::MergeConflict, &details).await
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
    HookResult::Cascade {
        to: default_states::MERGE_FAILED.to_string(),
        reason: format!(
            "{CONFLICT_HANDOFF_MARKER} {details}{}{encoded}",
            crate::workflow::CONFLICT_HANDOFF_PATHS_PREFIX,
            encoded = serde_json::to_string(paths).expect("path list serializes"),
        ),
    }
}

pub(super) async fn merge_failure_result(
    ctx: &HookContext,
    task: &db::Task,
    reason: String,
    kind: api_types::FailureKind,
) -> HookResult {
    let mut task = task.clone();
    if kind == api_types::FailureKind::MergeConflict && task.review_passed_at.is_some() {
        match db::TaskRepo::set_review_passed_at_cas(
            &*ctx.db,
            &task.id,
            task.version,
            None,
            &db::now_rfc3339(),
        )
        .await
        {
            Ok(updated) => task = updated,
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        }
    }

    match crate::task_hierarchy::coordination_root_has_subtasks(&ctx.db, &task).await {
        Ok(true) => {
            let block_reason = format!(
                "coordination root requires manual workspace repair before integration can continue: {reason}"
            );
            if let Err(error) = block_task_with_annotation(
                ctx,
                &task,
                &block_reason,
                kind,
                None,
                Some(manual_merge_recovery_annotation(
                    ctx,
                    kind,
                    &block_reason,
                    "coordination_root",
                )),
            )
            .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Ok;
        }
        Ok(false) => {}
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    }

    if kind == api_types::FailureKind::MergeConflict {
        let block_reason = format!(
            "manual task-worktree repair is required before integration can continue: {reason}"
        );
        if let Err(error) = block_task_with_annotation(
            ctx,
            &task,
            &block_reason,
            kind,
            None,
            Some(manual_merge_recovery_annotation(
                ctx,
                kind,
                &block_reason,
                "manual_workspace_repair",
            )),
        )
        .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        return HookResult::Ok;
    }

    let budget = match runtime_retry_budget(
        &task,
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
        Ok(entries) => crate::task_diagnostics::count_gate_rejections_since_boundary(
            &entries,
            default_states::MERGING,
        ),
        Err(error) => {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    };

    if existing_follow_ups >= i64::from(budget) {
        let block_reason = "merge-fix retry budget exhausted";
        if let Err(error) = block_task_with_annotation(
            ctx,
            &task,
            block_reason,
            api_types::FailureKind::MergeFixBudgetExhausted,
            None,
            Some(merge_budget_annotation(block_reason)),
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

fn manual_merge_recovery_annotation(
    ctx: &HookContext,
    kind: api_types::FailureKind,
    reason: &str,
    blocked_by: &str,
) -> String {
    let annotation = api_types::TaskAnnotation::Blocking(api_types::TaskBlockingAnnotation {
        annotation_type: kind,
        blocking_reason: reason.to_owned(),
        blocked_by: Some(blocked_by.to_owned()),
        blocked_at: Some(db::now_rfc3339()),
        blocked_execution_id: ctx.execution_id.clone(),
        artifact: None,
        message: Some(reason.to_owned()),
        hook: None,
    });
    serde_json::to_string(&annotation).expect("a blocking annotation always serializes")
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

/// Blocking `merge_failed` exit guard: a conflict-handoff repair must not
/// reach a full review while its handed-off files still add Git conflict
/// markers. Failing here routes through the workflow guard-retry machinery,
/// which resumes the Worker with these file names and blocks the Task once the
/// execution retry budget is spent. The merge-time check stays the final
/// safety net.
pub struct RequireConflictMarkersResolved;

#[async_trait]
impl HookAction for RequireConflictMarkersResolved {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        if ctx.to_state != default_states::REVIEW {
            return HookResult::Skipped {
                reason: "only the repair-to-review exit is checked".to_string(),
            };
        }
        if ctx.triggered_by.is_user() {
            return HookResult::Skipped {
                reason: "user-managed transition bypasses the conflict marker guard".to_string(),
            };
        }
        let Some(merge_service) = ctx.merge_service.as_ref() else {
            return HookResult::Skipped {
                reason: "merge service unavailable".to_string(),
            };
        };
        match merge_service.unresolved_handoff_markers(&ctx.task_id).await {
            Ok(paths) if paths.is_empty() => HookResult::Ok,
            Ok(paths) => HookResult::Failed {
                reason: format!(
                    "Unresolved Git conflict markers (`<<<<<<<`, `|||||||`, `>>>>>>>`) remain in committed file(s): {}. Open each file, reconcile both sides into the final code, delete every marker line (including nested or duplicated marker blocks from earlier conflict rounds), and commit the result.",
                    paths.join(", ")
                ),
            },
            Err(error) => HookResult::Failed {
                reason: format!("conflict marker check failed: {error}"),
            },
        }
    }
}
