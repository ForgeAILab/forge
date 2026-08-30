use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CommentAuthorType, CreateTaskComment, DbError, Execution,
    ExecutionRepo, PageRequest, ReviewRepo, ReviewStatus, SortBy, SortOrder, TaskCommentRepo,
    TaskMetadata, TaskRepo, TaskRoleAssignment, TaskRoleAssignmentRepo, TransitionLog,
    TransitionLogRepo, UpdateTask, WorkspaceRepo,
};
use events::{event_timestamp, EventContext, ForgeEvent};
use serde_json::{json, Value};
use tokio::process::Command;

use crate::workflow::{
    default_states, engine::WorkflowEngine, inherited_subtask_workflow, HookContext, HookResult,
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
    let page = ExecutionRepo::list_by_task(
        &*ctx.db,
        &ctx.task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(page.items.iter().any(|execution| {
        execution.status == db::ExecutionStatus::Running
            && roles.iter().any(|role| execution.role == *role)
    }))
}

pub(super) async fn task(ctx: &HookContext) -> Result<db::Task, String> {
    TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("task not found: {}", ctx.task_id))
}

pub(super) async fn latest_executor_execution(ctx: &HookContext) -> Option<Execution> {
    let page = ExecutionRepo::list_by_task(
        &*ctx.db,
        &ctx.task_id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .ok()?;
    page.items
        .into_iter()
        .find(|execution| matches!(execution.role.as_str(), "executor" | "coder" | "worker"))
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

pub(super) async fn transition_subtask_with_inherited_workflow(
    ctx: &HookContext,
    subtask: db::Task,
    target_state: &str,
) -> Result<(), String> {
    let workflow = inherited_subtask_workflow();
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
    let mut current = subtask;

    if target_state == default_states::DONE && current.status == default_states::TODO {
        current = engine
            .transition(
                &current.id,
                default_states::IN_PROGRESS,
                current.version,
                &workflow,
                &api_types::Actor::system(api_types::SystemComponent::Workflow),
                "root done propagation",
                false,
            )
            .await
            .map_err(|error| error.to_string())?
            .task;
    }

    engine
        .transition(
            &current.id,
            target_state,
            current.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "root subtask cascade",
            false,
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
        Ok(entries) => merge_fix_rejections_since_boundary(&entries),
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

pub(super) fn merge_fix_rejections_since_boundary(entries: &[TransitionLog]) -> i64 {
    let boundary = entries.iter().rposition(|entry| {
        entry.from_state == default_states::MERGING
            && !entry.rejection
            && (entry.to_state != default_states::MERGING
                || entry.trigger_name.as_deref() == Some("reset_retry_window"))
    });
    let entries = boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries);
    entries
        .iter()
        .filter(|entry| {
            entry.from_state == default_states::MERGING
                && entry.to_state == default_states::MERGE_FAILED
                && entry.rejection
        })
        .count() as i64
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

pub(super) async fn create_review_attempt(
    ctx: &HookContext,
    execution_id: &str,
) -> Result<db::Review, String> {
    let attempt_number = ReviewRepo::next_attempt_number(&*ctx.db, &ctx.task_id)
        .await
        .map_err(|error| error.to_string())?;
    let now = now_rfc3339();
    ReviewRepo::create(
        &*ctx.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: ctx.task_id.clone(),
            execution_id: execution_id.to_string(),
            attempt_number,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .map_err(|error| error.to_string())
}

pub(super) async fn ensure_review_record_for_dispatch(
    ctx: &HookContext,
    execution_id: &str,
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
        _ => create_review_attempt(ctx, execution_id).await.map(|_| ()),
    }
}

pub(super) async fn ensure_review_awaiting_human(ctx: &HookContext) -> Result<(), String> {
    if ctx.to_state != default_states::REVIEW {
        return Ok(());
    }

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
            let execution_id = match ctx.execution_id.clone() {
                Some(execution_id) => execution_id,
                None => match latest_executor_execution(ctx)
                    .await
                    .map(|execution| execution.id)
                {
                    Some(execution_id) => execution_id,
                    None => {
                        set_review_awaiting_human_metadata(ctx).await?;
                        return Ok(());
                    }
                },
            };
            create_review_attempt(ctx, &execution_id).await?
        }
    };
    let now = now_rfc3339();
    let review = ReviewRepo::update_status(
        &*ctx.db,
        &review.id,
        ReviewStatus::AwaitingHuman,
        review.step_results_json,
        None,
        &now,
    )
    .await
    .map_err(|error| error.to_string())?;
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
    let mut metadata =
        TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| error.to_string())?;
    metadata
        .extra
        .insert("awaiting_human".to_owned(), json!(true));
    metadata
        .extra
        .insert("awaiting_human_reason".to_owned(), json!("manual_review"));
    TaskRepo::set_metadata_json(&*ctx.db, &task.id, metadata.to_json(), &now_rfc3339())
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
        let output = Command::new("bash")
            .arg("-lc")
            .arg(step)
            .current_dir(worktree_path)
            .output()
            .await
            .map_err(|error| error.to_string())?;
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
