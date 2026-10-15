//! Review entry costs nothing when a repair delivered the same clean candidate.
use super::common::block_task_with_annotation;
use crate::workflow::{effective_role, HookContext};
use db::{budget::RepairNoChangeDecision, now_rfc3339};
use serde_json::json;

pub(crate) async fn repair_no_change_gate(
    ctx: &HookContext,
    task: &db::Task,
    step: &db::TaskStep,
) -> crate::Result<RepairNoChangeDecision> {
    let (recorded, _) = ctx
        .db
        .start_hook(step, db::budget::REPAIR_NO_CHANGE_CHECKPOINT)
        .await?;
    if let Some(recorded) = recorded {
        return serde_json::from_str(&recorded)
            .map_err(|error| crate::ServiceError::invalid_operation(error.to_string()));
    }
    let unchanged = unchanged_attempt(ctx, task).await?;
    Ok(db::budget::settle_repair_no_change(&ctx.db, task, step, unchanged).await?)
}

async fn unchanged_attempt(ctx: &HookContext, task: &db::Task) -> crate::Result<Option<i64>> {
    let from_role = ctx
        .workflow
        .states
        .iter()
        .find(|state| state.name == ctx.from_state)
        .and_then(effective_role);
    if ctx.triggered_by.is_owner()
        || task.review_passed_at.is_some()
        || !matches!(from_role, Some("coder" | "worker" | "executor"))
    {
        return Ok(None);
    }
    if db::budget::review_rerun_entry(ctx.db.pool(), &task.id, None)
        .await?
        .is_some_and(|(actor, _)| actor.is_owner())
    {
        return Ok(None);
    }
    let Some(candidate) = crate::task_service::latest_executor_execution_for_task(&ctx.db, task)
        .await?
        .filter(|execution| {
            matches!(execution.role.as_str(), "coder" | "worker" | "executor")
                && execution.task_id == task.id
                && execution.status == db::ExecutionStatus::Completed
        })
    else {
        return Ok(None);
    };
    let Some((review, Some(judged))) =
        db::budget::failed_review_for_no_change(&ctx.db, &task.id).await?
    else {
        return Ok(None);
    };
    if candidate.id == review.execution_id || candidate.created_at < review.created_at {
        return Ok(None);
    }
    let Some(workspace) = db::WorkspaceRepo::get_by_task_id(
        &*ctx.db,
        task.parent_task_id.as_deref().unwrap_or(&task.id),
    )
    .await?
    else {
        return Ok(None);
    };
    // An owner that cannot answer (away daemon, timeout, disk wait) proves
    // nothing: the ordinary entry meets that outage with its typed wait.
    let Ok(resolved) = super::review::resolve_workspace(ctx, &workspace).await else {
        return Ok(None);
    };
    let Ok(state) = resolved.backend.describe(&resolved.placement).await else {
        return Ok(None);
    };
    Ok(
        (!state.dirty && state.head_sha.as_deref() == Some(judged.as_str()))
            .then_some(review.attempt_number),
    )
}

pub(crate) async fn park_no_change(ctx: &HookContext, task: &db::Task) -> crate::Result<()> {
    let message = "no change after repair round";
    let kind = api_types::FailureKind::ReviewNeedsOwner;
    let annotation = json!({
        "type": kind, "blocking_reason": kind, "message": message,
        "blocked_at": now_rfc3339(), "blocked_execution_id": ctx.execution_id,
    });
    block_task_with_annotation(ctx, task, message, kind, None, Some(annotation.to_string()))
        .await?;
    Ok(())
}
