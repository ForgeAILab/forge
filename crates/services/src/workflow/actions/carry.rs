use async_trait::async_trait;
use db::{
    now_rfc3339, ReviewConformanceRepo, ReviewRepo, ReviewStatus, SettleCarriedReview,
    TransitionLogRepo,
};
use serde_json::{json, Value};

use crate::{
    merge_service::ReviewCarryFacts,
    workflow::{default_states, review_carry_entry_kind, HookAction, HookContext, HookResult},
};

use super::common::{latest_review, publish_review_passed, review_ci_steps, task};

/// How many consecutive mechanical integrations one passed review may cover
/// before a real review is required again. Each carry is a fresh chance for
/// the tree to drift from what the reviewer saw, so the bound keeps a Task that
/// keeps losing merge races from riding one approval indefinitely.
pub(crate) const MAX_REVIEW_CARRIES: i64 = db::budget::Kind::ReviewCarry.default_limit() as i64;

/// `on_enter` hook of `review`, ahead of reviewer dispatch: when the Task
/// arrives here only because Forge rebased it (or its Worker reconciled a
/// Forge-committed rebase conflict) and everything a reviewer would have
/// judged is provably unchanged, keep the previous approval and go straight
/// back to `merging`. Every failed condition is `Skipped`, so the reviewer is
/// dispatched exactly as before.
pub struct CarryReviewAuthority;

#[async_trait]
impl HookAction for CarryReviewAuthority {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        match carry(ctx).await {
            Ok(result) => result,
            Err(CarryError::Ineligible(reason)) => HookResult::Skipped {
                reason: format!("full review required: {reason}"),
            },
            Err(CarryError::Failed(reason)) => HookResult::Failed { reason },
        }
    }
}

pub(crate) enum CarryError {
    /// A carry condition does not hold; nothing was written.
    Ineligible(String),
    Failed(String),
}

// One rule for "may a passed review cover this mechanically changed
// candidate", shared by the two places that ask it: a Task entering `review`
// on a mechanical bridge (the hook below) and an integration attempt whose
// candidate the queue rebased while the Task stayed in `merging`
// (`integration_steps::settle`). The callers differ only in where the facts
// come from (transition log and open Review row, or the attempt's lineage and
// the queue's check).

/// The review gate itself insists on a person.
pub(crate) fn gate_refuses_carry(
    gate_config: Option<&api_types::GateConfig>,
) -> Option<&'static str> {
    gate_config
        .is_some_and(|gate_config| gate_config.requires_user_approval())
        .then_some("the review gate requires user approval")
}

/// What about the Task rules a carry out. `Ok(n)`: nothing does, and `n`
/// checks are configured for the review state (`review_state_config`).
pub(crate) async fn task_carry_checks(
    db: &db::SqliteDb,
    task: &db::Task,
    review_state_config: &Value,
) -> Result<usize, CarryError> {
    if task.blocked_json.is_some() {
        return Err(ineligible("task is blocked"));
    }
    if crate::task_hierarchy::coordination_root_has_subtasks(db, task)
        .await
        .map_err(failed)?
    {
        return Err(ineligible("coordination roots keep the full review"));
    }
    if super::common::task_is_read_only(db, task)
        .await
        .map_err(failed)?
    {
        return Err(ineligible("read-only Tasks run no implementation checks"));
    }
    // Without checks nothing verified the rebased or repaired tree.
    let ci_steps = review_ci_steps(review_state_config).map_err(failed)?;
    if ci_steps.is_empty() {
        return Err(ineligible("no ci_steps are configured"));
    }
    Ok(ci_steps.len())
}

/// One approval covers a bounded number of mechanical integrations.
pub(crate) fn carry_budget_refusal(carries_since_review: i64) -> Option<String> {
    (!db::budget::allows_retry(MAX_REVIEW_CARRIES, carries_since_review)).then(|| {
        format!("{MAX_REVIEW_CARRIES} integrations were already carried under this review")
    })
}

/// The candidate may only change what the reviewer saw changed.
pub(crate) fn carry_path_refusal(
    changed_paths: &[String],
    reviewed_paths: &[String],
) -> Option<String> {
    changed_paths
        .iter()
        .find(|path| !reviewed_paths.contains(path))
        .map(|path| format!("`{path}` is outside the reviewed change set"))
}

fn ineligible(reason: impl Into<String>) -> CarryError {
    CarryError::Ineligible(reason.into())
}

fn failed(error: impl ToString) -> CarryError {
    CarryError::Failed(error.to_string())
}

async fn carry(ctx: &HookContext) -> Result<HookResult, CarryError> {
    if ctx.to_state != default_states::REVIEW {
        return Err(ineligible("only entry into review can carry"));
    }
    if ctx.triggered_by.is_user() {
        return Err(ineligible("user-managed transitions get a real review"));
    }
    if let Some(reason) = gate_refuses_carry(ctx.gate_config.as_ref()) {
        return Err(ineligible(reason));
    }
    let Some(merge_service) = ctx.merge_service.as_ref() else {
        return Err(ineligible("merge service unavailable"));
    };
    let (Some(project_version), Some(workflow_definition)) =
        (ctx.project_version, ctx.project_workflow_definition.clone())
    else {
        return Err(ineligible("no Project workflow snapshot"));
    };
    match db::ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
        Ok(Some(project)) if project.paused_at.is_some() => {
            return Err(ineligible("project paused"));
        }
        Ok(Some(_)) => {}
        Ok(None) => return Err(failed(format!("project not found: {}", ctx.project_id))),
        Err(error) => return Err(failed(error)),
    }
    let task = task(ctx).await.map_err(failed)?;
    let ci_step_count = task_carry_checks(&ctx.db, &task, &ctx.state_config).await?;

    let entries = TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .map_err(failed)?;
    let Some(kind) = review_carry_entry_kind(&entries) else {
        return Err(ineligible("not a mechanical merge-contention entry"));
    };

    if let Some(attempt) = crate::workflow::engine::durable::current_hook(&ctx.task_id) {
        if let Some(review) = latest_review(ctx).await.map_err(failed)? {
            let details: Value = serde_json::from_str(&review.step_results_json).map_err(failed)?;
            if review.status == ReviewStatus::Passed
                && details["carry_hook_step_id"] == attempt.step.id
                && details["carry_hook_index"] == attempt.index
            {
                return Ok(HookResult::Cascade {
                    to: default_states::MERGING.into(),
                    reason: "resumed carried review authority; CI passed; re-review skipped".into(),

                    bridge: api_types::TransitionBridge::new(
                        api_types::TransitionBridgeKind::ReviewCarry,
                    ),
                });
            }
        }
    }

    // `run_ci_steps` (a blocking before_enter hook) opened this Review row and
    // recorded the checks it ran. Insist that every configured step ran and
    // exited zero for this entry.
    let Some(review) = latest_review(ctx).await.map_err(failed)? else {
        return Err(ineligible("no review attempt was opened for this entry"));
    };
    if review.status != ReviewStatus::Running {
        return Err(ineligible("the review attempt is not open"));
    }
    let running: Value = serde_json::from_str(&review.step_results_json).map_err(failed)?;
    let ci_results = running
        .get("ci_steps")
        .and_then(Value::as_array)
        .filter(|results| {
            results.len() == ci_step_count
                && results
                    .iter()
                    .all(|result| result.get("exit_code").and_then(Value::as_i64) == Some(0))
        })
        .cloned()
        .ok_or_else(|| ineligible("this entry's ci_steps did not all pass"))?;

    let base = match ctx.db.review_carry_base(&ctx.task_id).await {
        Ok(base) => base,
        Err(db::DbError::Check(reason)) => return Err(ineligible(reason)),
        Err(error) => return Err(failed(error)),
    };
    if base.review_id != review.id {
        return Err(ineligible("the open review attempt changed"));
    }
    if let Some(reason) = carry_budget_refusal(base.carries_since_review) {
        return Err(ineligible(reason));
    }

    let (commit_sha, base_sha, changed_paths) = match merge_service
        .review_carry_facts(&ctx.task_id)
        .await
        .map_err(failed)?
    {
        ReviewCarryFacts::Ready {
            commit_sha,
            base_sha,
            changed_paths,
        } => (commit_sha, base_sha, changed_paths),
        ReviewCarryFacts::Unavailable { reason } => return Err(ineligible(reason)),
    };
    if let Some(reason) = carry_path_refusal(&changed_paths, &base.contract.candidate_changed_paths)
    {
        return Err(ineligible(reason));
    }

    // Keep the previous verdict exactly as recorded; only this entry's checks
    // are new.
    let mut details = json!({ "ci_steps": ci_results });
    if let Some(attempt) = crate::workflow::engine::durable::current_hook(&ctx.task_id) {
        details["carry_hook_step_id"] = json!(attempt.step.id);
        details["carry_hook_index"] = json!(attempt.index);
    }
    for key in ["conformance", "auditor"] {
        if let Some(value) = base.prior_details.get(key).filter(|value| !value.is_null()) {
            details[key] = value.clone();
        }
    }
    let now = now_rfc3339();
    let settled = ctx
        .db
        .settle_carried_review(SettleCarriedReview {
            review_id: review.id.clone(),
            expected_review_updated_at: review.updated_at.clone(),
            step_results_json: details.to_string(),
            candidate_execution_id: review.execution_id.clone(),
            expected_task_version: task.version,
            expected_task_status: ctx.to_state.clone(),
            expected_project_version: project_version,
            expected_workflow_definition: workflow_definition,
            carry: db::NewReviewAuthorityCarry {
                task_id: ctx.task_id.clone(),
                contract_execution_id: base.contract.execution_id.clone(),
                commit_sha,
                base_sha,
                kind,
                changed_paths,
            },
            occurred_at: now.clone(),
        })
        .await
        .map_err(failed)?;

    publish_review_passed(ctx, &settled);

    let what = match kind {
        db::ReviewCarryKind::CleanRebase => "clean rebase",
        db::ReviewCarryKind::ConflictRepair => "conflict repair",
    };
    if let Err(error) = super::common::create_system_comment(
        ctx,
        format!("Carried review authority across {what}; CI passed; skipped re-review."),
    )
    .await
    {
        tracing::warn!(task_id = %ctx.task_id, %error, "failed to record review carry comment");
    }
    Ok(HookResult::Cascade {
        to: default_states::MERGING.to_string(),
        reason: format!("carried review authority across {what}; CI passed; re-review skipped"),

        bridge: api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewCarry),
    })
}
