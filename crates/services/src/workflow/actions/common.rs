use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CommentAuthorType, CreateTaskComment, DbError, Execution,
    ExecutionRepo, ProjectRepo, ReviewRepo, ReviewStatus, TaskCommentRepo, TaskRepo,
    TaskRoleAssignment, TaskRoleAssignmentRepo, TransitionLogRepo, UpdateTask, WorkspaceRepo,
};
use events::{event_timestamp, EventContext, ForgeEvent};
use serde_json::{json, Value};
use tokio::process::Command;

use crate::workflow::{
    default_states,
    engine::{WorkflowAuthority, WorkflowEngine},
    inherited_subtask_workflow, HookContext, HookResult,
};

pub(super) async fn publish_domain_event(ctx: &HookContext, dedupe_key: &str) {
    let service = crate::DomainEventService::new(Arc::clone(&ctx.db), Arc::clone(&ctx.event_bus));
    if let Err(error) = service.publish_by_dedupe(dedupe_key).await {
        tracing::warn!(dedupe_key, %error, "failed to mirror committed domain event");
    }
}

pub(super) async fn get_role_assignment(
    ctx: &HookContext,
    role: &str,
) -> Result<Option<TaskRoleAssignment>, String> {
    match TaskRoleAssignmentRepo::get_by_task_and_role(&*ctx.db, &ctx.task_id, role).await {
        Ok(assignment) => Ok(assignment),
        Err(DbError::NotFound) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

pub(super) fn execution_guard_roles(role: &str) -> Vec<&str> {
    let mut roles = vec![role];
    if role == crate::workflow::default_roles::CODER {
        roles.push("executor");
    }
    roles
}

pub(super) async fn has_running_execution_for_roles(
    ctx: &HookContext,
    roles: &[&str],
) -> Result<bool, String> {
    let executions = ExecutionRepo::list_running_by_task(&*ctx.db, &ctx.task_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(executions
        .iter()
        .any(|execution| roles.iter().any(|role| execution.role == *role)))
}

pub(super) async fn task(ctx: &HookContext) -> Result<db::Task, String> {
    TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("task not found: {}", ctx.task_id))
}

pub(super) async fn task_execution_is_read_only(
    ctx: &HookContext,
    task: &db::Task,
) -> Result<bool, String> {
    let capability_class = sqlx::query_scalar::<_, Option<String>>(
        "SELECT capability_class FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&task.id)
    .fetch_optional(ctx.db.pool())
    .await
    .map_err(|error| error.to_string())?
    .flatten();
    crate::execution_setup::classify_task_execution(&task.task_type, capability_class.as_deref())
        .map(|class| class.is_read_only())
        .map_err(|error| error.to_string())
}

pub(super) async fn latest_executor_execution(ctx: &HookContext) -> Option<Execution> {
    let task = TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .ok()??;
    crate::task_service::latest_executor_execution_for_task(&ctx.db, &task)
        .await
        .ok()
        .flatten()
}

pub(super) async fn workspace_id(ctx: &HookContext) -> Option<String> {
    if let Some(workspace_id) = ctx.workspace_id.clone() {
        return Some(workspace_id);
    }
    if let Some(execution) = latest_executor_execution(ctx).await {
        if execution.workspace_id.is_some() {
            return execution.workspace_id;
        }
    }
    WorkspaceRepo::get_by_task_id(&*ctx.db, &ctx.task_id)
        .await
        .ok()
        .flatten()
        .map(|workspace| workspace.id)
}

pub(super) async fn cancel_subtask_with_effective_workflow(
    ctx: &HookContext,
    subtask: db::Task,
) -> Result<(), String> {
    let inherited = inherited_subtask_workflow();
    let workflow = if inherited
        .states
        .iter()
        .any(|state| state.name == subtask.status)
    {
        inherited
    } else {
        ctx.workflow.as_ref().clone()
    };
    if workflow.state_kind(&subtask.status) == Some(api_types::StateKind::Terminal) {
        return Ok(());
    }
    let target_state = workflow
        .cancellation_state
        .as_deref()
        .unwrap_or(default_states::CANCELLED)
        .to_owned();
    let authority = match (
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
    ) {
        (Some(project_version), Some(workflow_definition)) => WorkflowAuthority {
            project_version,
            workflow_definition: workflow_definition.to_owned(),
        },
        _ => {
            let project = ProjectRepo::get_by_id(&*ctx.db, &subtask.project_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("project {} not found", subtask.project_id))?;
            WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
            }
        }
    };
    let current_effective_workflow = WorkflowEngine::resolve_workflow_for_task(
        &subtask,
        &authority.workflow_definition,
        &api_types::Actor::system(api_types::SystemComponent::Workflow),
    );
    if current_effective_workflow != workflow {
        return Err("project workflow authority changed while cancelling subtask".to_owned());
    }
    let engine = WorkflowEngine {
        db: Arc::clone(&ctx.db),
        event_bus: Arc::clone(&ctx.event_bus),
        review_runner: ctx.review_runner.clone(),
        merge_service: ctx.merge_service.clone(),
        cleanup_scheduler: ctx.cleanup_scheduler.clone(),
        task_executor: ctx.task_executor.clone(),
        daemon_connections: ctx.daemon_connections.clone(),
        workspace_exec_locks: ctx.workspace_exec_locks.clone(),
        terminal_activity: ctx.terminal_activity.clone(),
        workspace_root: ctx.workspace_root.clone(),
        repo_cache_locks: ctx.repo_cache_locks.clone(),
    };
    engine
        .transition_with_authority(
            &subtask.id,
            &target_state,
            subtask.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "root subtask cascade",
            false,
            authority,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) async fn latest_review(ctx: &HookContext) -> Result<Option<db::Review>, String> {
    let reviews = ReviewRepo::list_by_task(&*ctx.db, &ctx.task_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(reviews
        .into_iter()
        .max_by_key(|review| review.attempt_number))
}

pub(super) fn review_is_ci_only(review: &db::Review) -> bool {
    serde_json::from_str::<Value>(&review.step_results_json)
        .ok()
        .and_then(|value| value.get("auditor").cloned())
        .and_then(|auditor| auditor.get("verdict").cloned())
        .and_then(|verdict| verdict.as_str().map(str::to_owned))
        .is_some_and(|verdict| verdict == "pass_ci_only")
}

pub(super) fn review_has_auditor_verdict(review: &db::Review) -> bool {
    serde_json::from_str::<Value>(&review.step_results_json)
        .ok()
        .and_then(|value| value.get("auditor").cloned())
        .and_then(|auditor| auditor.get("verdict").cloned())
        .and_then(|verdict| verdict.as_str().map(str::to_owned))
        .is_some()
}

pub(super) async fn merge_fix_budget_result(ctx: &HookContext) -> Option<HookResult> {
    let task = match task(ctx).await {
        Ok(task) => task,
        Err(reason) => return Some(HookResult::Failed { reason }),
    };
    let budget = match crate::task_service::config::runtime_retry_budget(
        &task,
        crate::task_service::config::RetryBudgetKind::MergeFix,
        Some(&ctx.state_config),
        ctx.gate_config.as_ref(),
    ) {
        Ok(budget) => budget,
        Err(error) => {
            return Some(HookResult::Failed {
                reason: error.to_string(),
            });
        }
    };
    let count = match TransitionLogRepo::list_by_task(&*ctx.db, &ctx.task_id).await {
        Ok(entries) => crate::task_diagnostics::count_gate_rejections_since_boundary(
            &entries,
            default_states::MERGING,
        ),
        Err(error) => {
            return Some(HookResult::Failed {
                reason: error.to_string(),
            });
        }
    };
    // This runs after `merging -> merge_failed` has been logged. The current
    // merge_failed entry consumes one allowed merge-fix follow-up, so exhaustion
    // is count > budget here; budget=0 blocks on the first conflict.
    if count > i64::from(budget) {
        let reason = "merge-fix follow-up failed: conflict";
        if let Err(error) = block_task(
            ctx,
            &task,
            reason,
            api_types::FailureKind::MergeConflict,
            None,
        )
        .await
        {
            return Some(HookResult::Failed {
                reason: error.to_string(),
            });
        }
        Some(HookResult::Ok)
    } else {
        None
    }
}

pub(super) fn follow_up_trigger(ctx: &HookContext) -> &'static str {
    if ctx.to_state == default_states::MERGE_FAILED
        || ctx.from_state == default_states::MERGE_FAILED
    {
        "merge_failed"
    } else if ctx.from_state == default_states::REVIEW {
        "review_failed"
    } else {
        "role_follow_up"
    }
}

pub(super) async fn create_system_comment(ctx: &HookContext, content: String) -> db::Result<()> {
    let now = now_rfc3339();
    let comment = TaskCommentRepo::create_comment(
        &*ctx.db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            author_type: CommentAuthorType::System,
            author_id: None,
            author_name: "Forge".to_string(),
            content,
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
    if let Err(error) = memory_service
        .record_task_comment(&ctx.project_id, &comment)
        .await
    {
        tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
    }
    ctx.event_bus.publish(ForgeEvent {
        event_type: "comment.created".to_string(),
        entity_id: comment.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::CommentCreated {
            task_id: ctx.task_id.clone(),
            comment_id: comment.id,
            author_type: "system".to_string(),
            author_name: "Forge".to_string(),
        },
    });
    Ok(())
}

pub(super) async fn persist_merge_error(
    ctx: &HookContext,
    task: &db::Task,
    error_type: api_types::FailureKind,
    message: &str,
) -> db::Result<()> {
    let detected_at = now_rfc3339();
    let annotation = json!({
        "type": error_type,
        "message": message,
        "detected_at": detected_at,
    });
    TaskRepo::update(
        &*ctx.db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
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
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(())
}

pub(super) async fn persist_target_repo_dirty_error(
    ctx: &HookContext,
    task: &db::Task,
    message: &str,
    _files: &[String],
) -> db::Result<()> {
    persist_merge_error(ctx, task, api_types::FailureKind::TargetRepoDirty, message).await
}

pub(super) async fn block_task(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    kind: api_types::FailureKind,
    source: Option<&str>,
) -> db::Result<()> {
    let now = now_rfc3339();
    let blocked_meta = json!({
        "reason": reason,
        "created_at": now.clone(),
        "kind": kind,
        "source": source,
        "execution_id": ctx.execution_id.clone(),
    });
    let mut current = task.clone();
    for attempt in 0..3 {
        match TaskRepo::update(
            &*ctx.db,
            UpdateTask {
                id: current.id.clone(),
                expected_version: current.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: None,
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => {
                tracing::info!(
                    task_id = %ctx.task_id,
                    status = %task.status,
                    kind = %kind,
                    reason = %reason,
                    source = ?source,
                    execution_id = ?ctx.execution_id,
                    "task blocked"
                );
                break;
            }
            Err(DbError::VersionConflict) if attempt < 2 => {
                current = TaskRepo::get_by_id(&*ctx.db, &task.id, false)
                    .await?
                    .ok_or(DbError::NotFound)?;
            }
            Err(error) => return Err(error),
        }
    }
    ctx.event_bus.publish(ForgeEvent {
        event_type: "task.blocked".to_string(),
        entity_id: ctx.task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::TaskBlocked {
            project_id: ctx.project_id.clone(),
            reason: reason.to_string(),
            kind: Some(kind),
            source: source.map(str::to_string),
            execution_id: ctx.execution_id.clone(),
        },
    });
    Ok(())
}

pub(super) fn review_ci_steps(value: &Value) -> Result<Vec<String>, String> {
    let value = value.get("review").unwrap_or(value);
    match value.get("ci_steps") {
        Some(steps) => {
            let Some(steps) = steps.as_array() else {
                return Err("review ci_steps must be an array".to_string());
            };
            steps
                .iter()
                .map(|step| {
                    step.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "review ci_steps entries must be strings".to_string())
                })
                .collect()
        }
        None => Ok(Vec::new()),
    }
}

/// Create a Review only for the Task/candidate snapshot that the blocking
/// review hook captured. Review rows are not themselves versioned with the
/// Task, so the pre/post checks below turn a raced insert into a cancelled
/// attempt instead of leaving a stale Running row that controls diagnostics.
pub(super) async fn create_review_attempt_with_authority(
    ctx: &HookContext,
    execution_id: &str,
    expected_task_version: i64,
    expected_candidate_execution_id: &str,
) -> Result<db::Review, String> {
    ensure_review_authority(ctx, expected_task_version, expected_candidate_execution_id).await?;
    let now = now_rfc3339();
    let review = ReviewRepo::create_with_task_authority(
        &*ctx.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            execution_id: execution_id.to_string(),
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
        expected_task_version,
        &ctx.to_state,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        Some(expected_candidate_execution_id),
    )
    .await
    .map_err(|error| error.to_string())?;

    if let Err(reason) =
        ensure_review_authority(ctx, expected_task_version, expected_candidate_execution_id).await
    {
        cancel_review_after_authority_loss(ctx, &review, &reason).await;
        return Err(reason);
    }
    Ok(review)
}

/// Validate the immutable Task/project/candidate facts captured by a review
/// hook. The check is intentionally repeated after non-transactional Review
/// writes; a competing Task transition can win between the two repository
/// calls and must invalidate the new row.
pub(super) async fn ensure_review_authority(
    ctx: &HookContext,
    expected_task_version: i64,
    expected_candidate_execution_id: &str,
) -> Result<db::Task, String> {
    let current_task = task(ctx).await?;
    if current_task.version != expected_task_version {
        return Err(format!(
            "review authority changed: task version {} is no longer {}",
            current_task.version, expected_task_version
        ));
    }
    if current_task.status != ctx.to_state {
        return Err(format!(
            "review authority changed: task is in '{}' instead of '{}'",
            current_task.status, ctx.to_state
        ));
    }
    let Some(candidate) = latest_executor_execution(ctx).await else {
        return Err("review authority changed: executor candidate disappeared".to_owned());
    };
    if candidate.id != expected_candidate_execution_id {
        return Err("review authority changed: executor candidate changed".to_owned());
    }
    let (Some(expected_project_version), Some(expected_workflow_definition)) = (
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
    ) else {
        return Err("review authority is missing the Project version/workflow snapshot".to_owned());
    };
    let project = ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("project not found: {}", ctx.project_id))?;
    if project.version != expected_project_version
        || project.workflow_definition != expected_workflow_definition
    {
        return Err("review authority changed: project workflow changed".to_owned());
    }
    Ok(current_task)
}

/// A review that lost its Task authority is not allowed to remain Running or
/// AwaitingHuman. Reconcile it with a Review-only CAS: the Task/candidate may
/// already have moved, so cancellation must not require the stale authority
/// snapshot or mutate the new Task projection. Terminal failures are written
/// through the atomic task-authority API by the caller and therefore never
/// need this fallback.
pub(super) async fn cancel_review_after_authority_loss(
    ctx: &HookContext,
    review: &db::Review,
    reason: &str,
) {
    let current_review = match ReviewRepo::get_by_id(&*ctx.db, &review.id).await {
        Ok(Some(review)) => review,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(
                review_id = %review.id,
                %error,
                "failed to reload review after authority loss"
            );
            return;
        }
    };
    if !matches!(
        current_review.status,
        ReviewStatus::Running | ReviewStatus::AwaitingHuman
    ) {
        return;
    }
    let mut details = serde_json::from_str::<Value>(&current_review.step_results_json)
        .unwrap_or_else(|_| json!({ "ci_steps": [] }));
    if !details.is_object() {
        details = json!({ "ci_steps": [] });
    }
    details["execution_retry"] = json!({
        "status": "cancelled_authority_lost",
        "reason": reason,
        "cancelled_at": now_rfc3339(),
    });
    let now = now_rfc3339();
    match ReviewRepo::cancel_if_unchanged(
        &*ctx.db,
        &current_review.id,
        current_review.status.clone(),
        &current_review.updated_at,
        details.to_string(),
        &now,
        &now,
    )
    .await
    {
        Ok(Some(cancelled)) => {
            publish_domain_event(
                ctx,
                &format!(
                    "review-status:{}:{}:{}",
                    cancelled.id, cancelled.status, now
                ),
            )
            .await;
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                review_id = %review.id,
                %error,
                "failed to cancel review after authority loss"
            );
        }
    }
}

/// Update a non-terminal Review state while retaining the Task/candidate
/// authority checks that the atomic terminal-settlement API provides for
/// Passed/Failed. If the Task changes during the write, reconcile the row to
/// Cancelled so it cannot remain a stale diagnostic blocker.
#[allow(clippy::too_many_arguments)]
pub(super) async fn update_review_status_with_authority_checks(
    ctx: &HookContext,
    review: &db::Review,
    status: ReviewStatus,
    step_results_json: String,
    finished_at: Option<String>,
    updated_at: &str,
    expected_task_version: i64,
    expected_candidate_execution_id: &str,
) -> Result<db::Review, String> {
    let updated = match ReviewRepo::update_status_with_review_authority_and_task_projection(
        &*ctx.db,
        &review.id,
        status,
        step_results_json,
        finished_at,
        updated_at,
        expected_task_version,
        &ctx.to_state,
        ctx.project_version,
        ctx.project_workflow_definition.as_deref(),
        review.status.clone(),
        &review.updated_at,
        expected_candidate_execution_id,
        None,
    )
    .await
    {
        Ok(review) => review,
        Err(error) => {
            cancel_review_after_authority_loss(ctx, review, &error.to_string()).await;
            return Err(error.to_string());
        }
    };
    Ok(updated)
}

pub(super) async fn ensure_review_record_for_dispatch(
    ctx: &HookContext,
    _execution_id: &str,
) -> Result<(), String> {
    if ctx.to_state != default_states::REVIEW {
        return Ok(());
    }

    match latest_review(ctx).await? {
        Some(review)
            if matches!(
                review.status,
                ReviewStatus::Running | ReviewStatus::AwaitingHuman
            ) =>
        {
            Ok(())
        }
        _ => {
            let current_task = task(ctx).await?;
            let Some(candidate) = latest_executor_execution(ctx).await else {
                return Err(
                    "review dispatch requires a current implementation candidate".to_owned(),
                );
            };
            create_review_attempt_with_authority(
                ctx,
                &candidate.id,
                current_task.version,
                &candidate.id,
            )
            .await
            .map(|_| ())
        }
    }
}

/// Establish the review-attempt boundary before reviewer capacity is checked.
///
/// A completed review is authority for one candidate only. Re-entering review
/// after a merge repair or mechanical rebase clears `review_passed_at`, but a
/// saturated reviewer can prevent dispatch from creating the next execution.
/// Without a new Running row, recovery sees the old terminal reviewer and can
/// reconcile its passed assessment into the new review cycle indefinitely.
pub(super) async fn ensure_review_attempt_for_current_candidate(
    ctx: &HookContext,
    task: &db::Task,
) -> Result<(), String> {
    if ctx.to_state != default_states::REVIEW || task.review_passed_at.is_some() {
        return Ok(());
    }

    let Some(candidate) = latest_executor_execution(ctx).await else {
        // A review attempt has no authority without an implementation
        // candidate. The human-review marker may still be used by the caller,
        // but no Review row is created until a candidate exists.
        return Ok(());
    };
    match latest_review(ctx).await? {
        Some(review)
            if review.execution_id == candidate.id
                && matches!(
                    review.status,
                    ReviewStatus::Running | ReviewStatus::AwaitingHuman
                ) =>
        {
            Ok(())
        }
        _ => create_review_attempt_with_authority(ctx, &candidate.id, task.version, &candidate.id)
            .await
            .map(|_| ()),
    }
}

pub(super) async fn ensure_review_awaiting_human(ctx: &HookContext) -> Result<(), String> {
    if ctx.to_state != default_states::REVIEW {
        return Ok(());
    }

    let task_snapshot = task(ctx).await?;
    let candidate_execution_id = latest_executor_execution(ctx)
        .await
        .map(|execution| execution.id);
    let review = match latest_review(ctx).await? {
        Some(review)
            if matches!(
                review.status,
                ReviewStatus::Running | ReviewStatus::AwaitingHuman
            ) =>
        {
            review
        }
        _ => {
            let Some(candidate_execution_id) = candidate_execution_id.as_deref() else {
                set_review_awaiting_human_metadata(ctx).await?;
                return Ok(());
            };
            create_review_attempt_with_authority(
                ctx,
                candidate_execution_id,
                task_snapshot.version,
                candidate_execution_id,
            )
            .await?
        }
    };
    let now = now_rfc3339();
    crate::task_service::strict_review_details(&review).map_err(|error| error.to_string())?;
    let review_details = review.step_results_json.clone();
    let Some(candidate_execution_id) = candidate_execution_id.as_deref() else {
        cancel_review_after_authority_loss(
            ctx,
            &review,
            "review authority has no current implementation candidate",
        )
        .await;
        return Err("review authority has no current implementation candidate".to_owned());
    };
    let review = update_review_status_with_authority_checks(
        ctx,
        &review,
        ReviewStatus::AwaitingHuman,
        review_details,
        None,
        &now,
        task_snapshot.version,
        candidate_execution_id,
    )
    .await?;
    publish_domain_event(
        ctx,
        &format!("review-status:{}:{}:{}", review.id, review.status, now),
    )
    .await;
    let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
    if let Err(error) = memory_service
        .record_review_result_if_final(&ctx.project_id, &review)
        .await
    {
        tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
    }
    Ok(())
}

async fn set_review_awaiting_human_metadata(ctx: &HookContext) -> Result<(), String> {
    let task = task(ctx).await?;
    TaskRepo::mutate_metadata(
        &*ctx.db,
        &task.id,
        Some(task.version),
        vec![
            db::TaskMetadataMutation::Set {
                key: "awaiting_human".to_owned(),
                value: json!(true),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_reason".to_owned(),
                value: json!("manual_review"),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_marker_id".to_owned(),
                value: json!(db::new_uuid_v4()),
            },
        ],
        &now_rfc3339(),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) async fn run_ci_steps_in_worktree(
    worktree_path: &str,
    ci_steps: &[String],
) -> Result<(Vec<Value>, Option<usize>), String> {
    let mut results = Vec::with_capacity(ci_steps.len());

    for (index, step) in ci_steps.iter().enumerate() {
        // `StepResultEntry` declares `started_at`/`finished_at` and the review
        // API publishes them, so stamp each step here — this is the only place
        // that knows when a step actually ran.
        let started_at = now_rfc3339();
        let output = Command::new("bash")
            .arg("-lc")
            .arg(step)
            .current_dir(worktree_path)
            .output()
            .await
            .map_err(|error| error.to_string())?;
        let finished_at = now_rfc3339();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let output_tail = if stdout.is_empty() {
            stderr.clone()
        } else if stderr.is_empty() {
            stdout.clone()
        } else {
            format!("{stdout}\n{stderr}")
        };
        let exit_code = output.status.code().unwrap_or(1);
        results.push(json!({
            "index": index,
            "command": step,
            "exit_code": exit_code,
            "stderr_tail": tail_bytes(&stderr, 4096),
            "output_tail": tail_bytes(&output_tail, 4096),
            "started_at": started_at,
            "finished_at": finished_at,
        }));

        if exit_code != 0 {
            return Ok((results, Some(index)));
        }
    }

    Ok((results, None))
}

pub(super) fn tail_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }

    let mut start = text.len().saturating_sub(max_bytes);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

pub(super) fn publish_review_passed(ctx: &HookContext, review: &db::Review) {
    ctx.event_bus.publish(ForgeEvent {
        event_type: "review.passed".to_string(),
        entity_id: review.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReviewPassed {
            task_id: ctx.task_id.clone(),
            review_id: review.id.clone(),
            attempt_number: review.attempt_number,
        },
    });
}

pub(super) fn publish_review_failed(
    ctx: &HookContext,
    review: &db::Review,
    failed_step_index: usize,
) {
    ctx.event_bus.publish(ForgeEvent {
        event_type: "review.failed".to_string(),
        entity_id: review.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::ReviewFailed {
            task_id: ctx.task_id.clone(),
            review_id: review.id.clone(),
            attempt_number: review.attempt_number,
            failed_step_index,
        },
    });
}
