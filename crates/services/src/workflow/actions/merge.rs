use async_trait::async_trait;
use events::{event_timestamp, EventContext, ForgeEvent};

use crate::{
    merge_service::MergeOutcome,
    workflow::{default_states, HookAction, HookContext, HookResult},
};

use super::common::{
    block_task_with_annotation, create_system_comment, merge_fix_budget_result,
    persist_merge_error, task, workspace_id,
};

/// The `merging` entry hook. It only admits the Task to the integration
/// queue of its repository's target branch (`integration_steps::admission`):
/// the queue worker rebases it if the target moved, has the head check run,
/// and fast-forwards; its Task steps (`integration_steps`) report every
/// result. The Task stays in `merging` meanwhile.
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
        #[cfg(test)]
        if let Some(error) = test_faults::take(&ctx.task_id) {
            return failed(ctx, error);
        }
        // A hooks step that was already merging when the server was upgraded
        // recorded its effects on the step. A merge it finished is reported
        // here, once, as that step would have; nothing is merged again.
        match legacy_merge_result(ctx, merge_service).await {
            Ok(Some((branch, after_sha))) => {
                if let Err(error) = create_system_comment(
                    ctx,
                    format!("Changes merged to {branch} (SHA: {after_sha})"),
                )
                .await
                {
                    return failed(ctx, error);
                }
                return HookResult::Cascade {
                    to: merge_role_target(ctx, default_states::DONE),
                    reason: "merge succeeded".to_string(),
                    bridge: Default::default(),
                };
            }
            Ok(None) => {}
            Err(error) => return failed(ctx, error),
        }
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        if task.parent_task_id.is_some() {
            return HookResult::Skipped {
                reason: "subtasks do not merge".to_string(),
            };
        }
        // The same step may have died inside the rebase it started: that
        // rebase is finished here, by the rule it was started under.
        match legacy_rebase_result(ctx, &task).await {
            Ok(Some(result)) => return result,
            Ok(None) => {}
            Err(error) => return failed(ctx, error),
        }
        // The Worker was handed a conflict and did not reconcile it: escalate
        // rather than queue a candidate that still carries the markers. The
        // `merge_failed` exit guard refuses the same thing earlier; this is
        // the last check before the queue.
        match merge_service.unresolved_handoff_markers(&ctx.task_id).await {
            Ok(paths) if paths.is_empty() => {}
            Ok(paths) => {
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
                    return failed(ctx, error);
                }
                return HookResult::Ok;
            }
            Err(error) if crate::worker_runtime::queue::retryable(&error) => {
                return failed(ctx, error)
            }
            // Unreadable here is unreadable for admission too, which reports it.
            Err(_) => {}
        }
        let attempt = match crate::integration_steps::admission::admit_released(
            &ctx.db,
            merge_service,
            &task,
            ctx.task_service.integration_worker().as_deref(),
        )
        .await
        {
            Ok(attempt) => attempt,
            Err(error) if crate::worker_runtime::queue::retryable(&error) => {
                return failed(ctx, error)
            }
            // No candidate can be named (no implementation run, the
            // workspace or its repository is gone): the same failure the
            // merge itself reported.
            Err(error) => {
                return merge_failure_result(
                    ctx,
                    &task,
                    error.to_string(),
                    api_types::FailureKind::WorkspaceError,
                )
                .await
            }
        };
        if let Err(error) =
            crate::integration_steps::admission::state_waiting(&ctx.db, &attempt).await
        {
            return failed(ctx, error);
        }
        // After the commit: the worker claims the queue if it has no head.
        if let (Some(worker), Some(queue_id)) = (
            ctx.task_service.integration_worker(),
            attempt.queue_id.as_deref(),
        ) {
            use crate::integration_worker::IntegrationEnqueuePort;
            worker.notify_enqueued(queue_id);
        }
        HookResult::Ok
    }
}

/// The merge a pre-upgrade hooks step finished: its recorded `merge_outcome`,
/// or its recorded `merge_intent` when the target already contains that
/// exact candidate. `(target branch, merged commit)`.
async fn legacy_merge_result(
    ctx: &HookContext,
    merge_service: &crate::merge_service::MergeService,
) -> crate::Result<Option<(String, String)>> {
    use crate::workflow::engine::durable::{current_hook, hook_effect};
    if current_hook(&ctx.task_id).is_none() {
        return Ok(None);
    }
    let done = |outcome: MergeOutcome| match outcome {
        MergeOutcome::Done {
            after_sha, branch, ..
        } => Some((branch, after_sha)),
        _ => None,
    };
    if let Some(recorded) = hook_effect(&ctx.task_id, "merge_outcome").await? {
        let outcome = serde_json::from_str::<MergeOutcome>(&recorded)
            .map_err(|error| crate::ServiceError::invalid_operation(error.to_string()))?;
        return Ok(done(outcome));
    }
    if let Some(recorded) = hook_effect(&ctx.task_id, "merge_intent").await? {
        let intent: crate::merge_service::MergeIntent = serde_json::from_str(&recorded)
            .map_err(|error| crate::ServiceError::invalid_operation(error.to_string()))?;
        return Ok(merge_service
            .completed_hook_merge(&intent)
            .await?
            .and_then(done));
    }
    Ok(None)
}

/// The rebase a pre-upgrade hooks step started (`rebase_target` recorded on
/// the step) and did not report. It is finished by the interrupted-rebase
/// rule it was started under, once: a clean result goes on to admission
/// (`None`), a conflict is handed to the Worker as that step would have.
/// A step that recorded no rebase has nothing to finish.
async fn legacy_rebase_result(
    ctx: &HookContext,
    task: &db::Task,
) -> crate::Result<Option<HookResult>> {
    use crate::integration_effects::rebase::{
        finish_rebase_recovery, recover_rebase, RebaseRecoveryInput, RebaseRecoveryOutcome,
    };
    use crate::workflow::engine::durable::{current_hook, hook_effect, record_hook_effect};
    use api_types::WorkspaceOwnerOperationOutcome as Outcome;
    if current_hook(&ctx.task_id).is_none() {
        return Ok(None);
    }
    let invalid =
        |error: serde_json::Error| crate::ServiceError::invalid_operation(error.to_string());
    let Some(previous) = hook_effect(&ctx.task_id, "rebase_target").await? else {
        return Ok(None);
    };
    let target: String = serde_json::from_str(&previous).map_err(invalid)?;
    // The branch that rebase was onto: the step recorded it with the merge
    // outcome that sent it to rebase.
    let target_branch = match hook_effect(&ctx.task_id, "merge_outcome").await? {
        Some(recorded) => match serde_json::from_str::<MergeOutcome>(&recorded).map_err(invalid)? {
            MergeOutcome::TargetMoved { target_branch, .. }
            | MergeOutcome::Conflict { target_branch, .. } => target_branch,
            _ => return Ok(None),
        },
        None => return Ok(None),
    };
    let Some(workspace) = db::WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id).await? else {
        return Ok(None);
    };
    let resolved = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
        &ctx.workspace_backend_router,
        &ctx.db,
        &workspace,
        &ctx.workspace_root,
    )
    .await
    .map_err(crate::ServiceError::from)?;
    let handoff_conflicts =
        !crate::task_hierarchy::coordination_root_has_subtasks(&ctx.db, task).await?;
    let outcome = match hook_effect(&ctx.task_id, "rebase_outcome").await? {
        Some(recorded) => serde_json::from_str::<Outcome>(&recorded).map_err(invalid)?,
        None => {
            let outcome = match recover_rebase(
                &resolved,
                &RebaseRecoveryInput {
                    previous_target: Some(previous.as_str()),
                    recorded_target: &target,
                    handoff_conflicts,
                },
            )
            .await?
            {
                RebaseRecoveryOutcome::Recorded(outcome) => outcome,
                RebaseRecoveryOutcome::Perform { in_progress } => {
                    let outcome = resolved
                        .rebase_target(&target_branch, handoff_conflicts)
                        .await?;
                    finish_rebase_recovery(
                        &resolved,
                        &target,
                        in_progress,
                        handoff_conflicts,
                        outcome,
                    )
                    .await?
                }
            };
            record_hook_effect(
                &ctx.task_id,
                "rebase_outcome",
                &serde_json::to_value(&outcome).expect("rebase outcome serializes"),
            )
            .await?;
            outcome
        }
    };
    let cleared = || async {
        if task.review_passed_at.is_none() {
            return Ok::<_, crate::ServiceError>(task.clone());
        }
        Ok(db::TaskRepo::set_review_passed_at_cas(
            &*ctx.db,
            &task.id,
            task.version,
            None,
            &db::now_rfc3339(),
        )
        .await?)
    };
    Ok(match outcome {
        // The rebased commit is the candidate the queue is given.
        Outcome::Rebased => {
            resolved.record_head_best_effort(&ctx.db).await;
            None
        }
        Outcome::Conflict {
            details,
            conflict_paths,
        } => {
            let task = cleared().await?;
            if handoff_conflicts && !conflict_paths.is_empty() {
                resolved.record_head_best_effort(&ctx.db).await;
                Some(conflict_handoff_result(ctx, &task, &target_branch, &conflict_paths).await)
            } else {
                Some(
                    merge_failure_result(
                        ctx,
                        &task,
                        format!("rebase onto {target_branch} conflicted: {details}"),
                        api_types::FailureKind::MergeConflict,
                    )
                    .await,
                )
            }
        }
        Outcome::Dirty { .. } => Some(
            merge_failure_result(
                ctx,
                task,
                "worktree has uncommitted changes and cannot be rebased".to_owned(),
                api_types::FailureKind::DirtyWorktree,
            )
            .await,
        ),
        Outcome::UnsupportedConflict { details } => Some(
            merge_failure_result(
                ctx,
                task,
                format!(
                    "rebase onto {target_branch} has a conflict requiring manual workspace repair: {details}"
                ),
                api_types::FailureKind::MergeConflict,
            )
            .await,
        ),
        _ => {
            return Err(crate::ServiceError::invalid_operation(
                "workspace owner returned an invalid rebase outcome",
            ))
        }
    })
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

/// How many rebase conflicts one Task may be handed back to its Worker before
/// Forge escalates. Each handoff is a *new* conflict — a sibling landed first
/// again — not a failed repair; a repair that leaves markers behind escalates
/// on its own through [`MergeOutcome::UnresolvedConflictMarkers`].
const MAX_CONFLICT_HANDOFFS: i64 = db::budget::Kind::ConflictHandoff.default_limit() as i64;

/// A rebase onto the moved target conflicted and Forge committed the conflict
/// with its markers. Send the Task to `merge_failed` so its Worker reconciles
/// the marked files — editing files is all that takes, and the Worker cannot
/// run the rebase itself — after which the repair gets a fresh review.
pub(crate) async fn conflict_handoff_result(
    ctx: &HookContext,
    task: &db::Task,
    target_branch: &str,
    paths: &[String],
) -> HookResult {
    let count = match db::budget::spent(
        ctx.db.pool(),
        &ctx.task_id,
        db::budget::Kind::ConflictHandoff.key(),
    )
    .await
    {
        Ok(n) => n,
        Err(error) => return failed(ctx, error),
    };
    let files = paths.join(", ");
    if !db::budget::allows_retry(MAX_CONFLICT_HANDOFFS, count) {
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
            return failed(ctx, error);
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
        return failed(ctx, error);
    }
    if let Err(error) =
        persist_merge_error(ctx, task, api_types::FailureKind::MergeConflict, &details).await
    {
        return failed(ctx, error);
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
        to: merge_role_target(ctx, default_states::MERGE_FAILED),
        reason: details,

        bridge: api_types::TransitionBridge::conflict_handoff(paths),
    }
}

pub(crate) async fn merge_failure_result(
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
                return failed(ctx, error);
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
                return failed(ctx, error);
            }
            return HookResult::Ok;
        }
        Ok(false) => {}
        Err(error) => {
            return failed(ctx, error);
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
            return failed(ctx, error);
        }
        return HookResult::Ok;
    }

    let budget = match db::budget::limit(
        &task,
        db::budget::Kind::MergeFix,
        Some(&ctx.state_config),
        ctx.gate_config.as_ref(),
    ) {
        Ok(budget) => budget,
        Err(error) => {
            return failed(ctx, error);
        }
    };
    let existing_follow_ups = match db::budget::spent(
        ctx.db.pool(),
        &ctx.task_id,
        db::budget::Kind::MergeFix.key(),
    )
    .await
    {
        Ok(n) => n,
        Err(error) => return failed(ctx, error),
    };
    if !db::budget::allows_retry(i64::from(budget), existing_follow_ups) {
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
            return failed(ctx, error);
        }
        HookResult::Ok
    } else {
        HookResult::Cascade {
            to: merge_role_target(ctx, default_states::MERGE_FAILED),
            reason,

            bridge: Default::default(),
        }
    }
}

/// Shared with the integration queue's `send_back` step (plan 3.2 stage D1b):
/// the failure annotation and `merge.failed` event `run_merge` records before
/// it hands a failed integration on, with the optional system comment first.
pub(crate) async fn record_integration_failure(
    ctx: &HookContext,
    task: &db::Task,
    kind: api_types::FailureKind,
    details: &str,
    comment: Option<String>,
) -> db::Result<db::Task> {
    if let Some(comment) = comment {
        create_system_comment(ctx, comment).await?;
    }
    let task = persist_merge_error(ctx, task, kind, details).await?;
    ctx.event_bus.publish(ForgeEvent {
        event_type: "merge.failed".to_string(),
        entity_id: ctx.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::MergeFailed {
            task_id: ctx.task_id.clone(),
            reason: details.to_owned(),
        },
    });
    Ok(task)
}

/// A `run_merge` failure. A transient one (the step queue's own retry
/// classification) asks the hook step to retry with back-off instead of
/// settling; anything else settles the step failed with a merge annotation.
fn failed(ctx: &HookContext, error: impl Into<crate::ServiceError>) -> HookResult {
    let error = error.into();
    crate::workflow::engine::durable::note_hook_failure(&ctx.task_id, &error);
    HookResult::Failed {
        reason: error.to_string(),
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
        // The review state by its role: a workflow may call it anything.
        let review = crate::workflow::review_refresh_target(&ctx.workflow, &ctx.from_state);
        if ctx.to_state != default_states::REVIEW && review.as_deref() != Some(&ctx.to_state) {
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

/// Test seam: queue errors for a Task's next `run_merge` attempts.
#[cfg(test)]
pub(crate) mod test_faults {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{Mutex, OnceLock},
    };

    type Faults = Mutex<HashMap<String, VecDeque<crate::ServiceError>>>;

    fn faults() -> &'static Faults {
        static FAULTS: OnceLock<Faults> = OnceLock::new();
        FAULTS.get_or_init(Default::default)
    }

    pub(crate) fn inject(task_id: &str, error: crate::ServiceError) {
        faults()
            .lock()
            .expect("merge faults")
            .entry(task_id.to_owned())
            .or_default()
            .push_back(error);
    }

    pub(super) fn take(task_id: &str) -> Option<crate::ServiceError> {
        faults()
            .lock()
            .expect("merge faults")
            .get_mut(task_id)
            .and_then(VecDeque::pop_front)
    }
}

/// Where an outcome of the merge state this hook runs in goes, by the role
/// the target plays in the Task's workflow (`reconcile::role_target`): a
/// workflow that renamed `done` or its repair state is followed, one that
/// kept the default names keeps them.
fn merge_role_target(ctx: &HookContext, default_name: &str) -> String {
    crate::integration_steps::reconcile::role_target(&ctx.workflow, &ctx.to_state, default_name)
}
