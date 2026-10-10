use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CommentAuthorType, CreateTaskComment, Execution, ExecutionRepo,
    ProjectRepo, ReviewRepo, ReviewStatus, TaskCommentRepo, TaskRepo, TaskRoleAssignment,
    UpdateTask, WorkspaceRepo,
};
use events::{event_timestamp, EventContext, ForgeEvent};
use serde_json::{json, Value};

use crate::workflow::{
    default_states,
    engine::{WorkflowAuthority, WorkflowEngine},
    inherited_subtask_workflow, HookContext, HookResult,
};

pub(super) async fn get_role_assignment(
    ctx: &HookContext,
    role: &str,
) -> Result<Option<TaskRoleAssignment>, String> {
    let task = task(ctx).await?;
    crate::task_hierarchy::effective_role_assignment(&ctx.db, &task, role)
        .await
        .map(|resolved| resolved.map(|resolved| resolved.assignment))
        .map_err(|error| error.to_string())
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
    task_is_read_only(&ctx.db, task).await
}

pub(crate) async fn task_is_read_only(db: &db::SqliteDb, task: &db::Task) -> Result<bool, String> {
    let capability_class = sqlx::query_scalar::<_, Option<String>>(
        "SELECT capability_class FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&task.id)
    .fetch_optional(db.pool())
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

pub(super) fn workspace_backend_router(
    ctx: &HookContext,
) -> Arc<crate::workspace_backend::WorkspaceBackendRouter> {
    Arc::clone(&ctx.workspace_backend_router)
}

pub(super) async fn resolve_workspace_backend(
    ctx: &HookContext,
    workspace: &db::Workspace,
) -> crate::Result<crate::workspace_backend::ResolvedWorkspace> {
    Ok(
        crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
            &workspace_backend_router(ctx),
            &ctx.db,
            workspace,
            &ctx.workspace_root,
        )
        .await?,
    )
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
    // The root's cancel wins over a subtask's settling plan artifact.
    let subtask = ctx
        .task_service
        .abandon_plan_publication_for_cancel(subtask)
        .await
        .map_err(|error| error.to_string())?;
    crate::task_service::execution::ensure_plan_publication_transition_authority(&subtask, None)
        .map_err(|error| error.to_string())?;
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
            clear_review_passed_at_on_commit: false,
        },
        _ => {
            let project = ProjectRepo::get_by_id(&*ctx.db, &subtask.project_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| format!("project {} not found", subtask.project_id))?;
            WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
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
    let engine = ctx.task_service.workflow_execution();
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
    let budget = match db::budget::limit(
        &task,
        db::budget::Kind::MergeFix,
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
    let count = match db::budget::spent(
        ctx.db.pool(),
        &ctx.task_id,
        db::budget::Kind::MergeFix.key(),
    )
    .await
    {
        Ok(n) => n,
        Err(error) => {
            return Some(HookResult::Failed {
                reason: error.to_string(),
            })
        }
    };
    // This runs after `merging -> merge_failed` has been logged. The current
    // merge_failed entry consumes one allowed merge-fix follow-up, so exhaustion
    // is count > budget here; budget=0 blocks on the first conflict.
    if db::budget::after_charge_exhausted(i64::from(budget), count) {
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
    let idempotency_key = crate::workflow::engine::durable::current_hook(&ctx.task_id)
        .map(|a| format!("hook-comment:{}:{}:{}", a.step.id, a.index, content));
    system_comment(
        &ctx.db,
        &ctx.event_bus,
        &ctx.project_id,
        &ctx.task_id,
        content,
        idempotency_key,
    )
    .await
}

/// A "Forge" comment on the Task, indexed and announced as every system
/// comment is. `idempotency_key` makes a redelivered step write it once.
pub(crate) async fn system_comment(
    db: &Arc<db::SqliteDb>,
    event_bus: &Arc<events::EventBus>,
    project_id: &str,
    task_id: &str,
    content: String,
    idempotency_key: Option<String>,
) -> db::Result<()> {
    let now = now_rfc3339();
    let comment = TaskCommentRepo::create_comment(
        &**db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            author_type: CommentAuthorType::System,
            author_id: None,
            author_name: "Forge".to_string(),
            content,
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    let memory_service = crate::MemoryService::new(Arc::clone(db));
    if let Err(error) = memory_service
        .record_task_comment(project_id, &comment)
        .await
    {
        tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
    }
    event_bus.publish(ForgeEvent {
        event_type: "comment.created".to_string(),
        entity_id: comment.id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::CommentCreated {
            task_id: task_id.to_owned(),
            comment_id: comment.id,
            author_type: "system".to_string(),
            author_name: "Forge".to_string(),
        },
    });
    Ok(())
}

/// Records why integration failed and returns the Task as that write left it.
///
/// The caller's next compare-and-set has to carry this version: two sequential
/// writes from one snapshot make the second fail as a version conflict, and in
/// a non-blocking hook that failure is swallowed -- leaving the Task in
/// `merging` with nothing recorded at all.
pub(super) async fn persist_merge_error(
    ctx: &HookContext,
    task: &db::Task,
    error_type: api_types::FailureKind,
    message: &str,
) -> db::Result<db::Task> {
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
    .await
}

pub(super) async fn persist_target_repo_dirty_error(
    ctx: &HookContext,
    task: &db::Task,
    message: &str,
    _files: &[String],
) -> db::Result<db::Task> {
    persist_merge_error(ctx, task, api_types::FailureKind::TargetRepoDirty, message).await
}

pub(super) async fn block_task(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    kind: api_types::FailureKind,
    source: Option<&str>,
) -> db::Result<()> {
    block_task_with_annotation(ctx, task, reason, kind, source, None).await
}

/// Block the Task and, when given, replace its error annotation in the same
/// write.
///
/// A block followed by a separate annotation write emits two interruption
/// changes a few milliseconds apart. The wake consumer admits a wake for the
/// first, suppresses the second as a duplicate of that live wake, then drops
/// the admitted one because the incident changed under it — so the Project
/// Agent never hears about the block. One write, one change.
pub(super) async fn block_task_with_annotation(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    kind: api_types::FailureKind,
    source: Option<&str>,
    annotation: Option<String>,
) -> db::Result<()> {
    let now = now_rfc3339();
    let blocked_meta = json!({
        "reason": reason,
        "created_at": now.clone(),
        "kind": kind,
        "source": source,
        "execution_id": ctx.execution_id.clone(),
    });
    let current = task.clone();
    {
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
                error_annotation: annotation.clone().map(Some),
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

pub(crate) fn review_ci_steps(value: &Value) -> Result<Vec<String>, String> {
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
        Ok(Some(_)) | Ok(None) => {}
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
    workspace: &crate::workspace_backend::ResolvedWorkspace,
    ci_steps: &[String],
    env: &std::collections::BTreeMap<String, String>,
) -> Result<
    crate::integration_effects::check::CheckRunOutcome,
    crate::integration_effects::check::CheckRunFailure,
> {
    let binding = crate::workspace_backend::effect_workspace(&workspace.placement);
    let mut checks = crate::integration_effects::check::CheckRun::new(
        crate::integration_effects::check::CheckRunInput {
            workspace: &binding,
            commands: ci_steps,
            purpose: api_types::WorkspaceRunPurpose::CiStep,
            environment: env,
            deadline: None,
            max_output_bytes: usize::MAX,
        },
    );
    while let Some(command) = checks.next_command() {
        let output = match workspace
            .backend
            .run(&workspace.placement, &command.spec)
            .await
        {
            Ok(output) => output,
            Err(error) => return Err(checks.infrastructure_failed(error)),
        };
        checks.completed(command, output);
    }
    Ok(checks.outcome())
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
