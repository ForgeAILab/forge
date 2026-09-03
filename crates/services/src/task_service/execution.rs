use super::*;
use crate::agent_capacity::count_running_executions;
use crate::workflow::dispatch::{
    build_effective_prompt, dispatch_intent_from_workflow_dispatch, effective_prompt_selection,
    loader::load_agent_dispatch_context,
};
use db::{CreateReview, ExecutionUsageRepo, UpdateTask, UpdateTaskStatus};

mod cascade;
mod follow_up;
mod guards;
mod hooks;
mod launch;
mod recovery;
mod runner;
pub(in crate::task_service) mod subtasks;

pub(super) use runner::{bounded_lease_expiry, execution_deadline_seconds, rfc3339_after};

pub(super) use cascade::should_block_task_for_failed_execution;

pub(super) fn publish_terminal_execution_event(service: &TaskService, execution: &Execution) {
    match execution.status {
        ExecutionStatus::Completed => service.publish(ForgeEvent {
            event_type: "execution.completed".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionCompleted {
                task_id: execution.task_id.clone(),
            },
        }),
        ExecutionStatus::Failed => service.publish(ForgeEvent {
            event_type: "execution.failed".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionFailed {
                task_id: execution.task_id.clone(),
                error: execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "execution failed".to_owned()),
            },
        }),
        ExecutionStatus::Cancelled => service.publish(ForgeEvent {
            event_type: "execution.cancelled".to_owned(),
            entity_id: execution.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ExecutionCancelled {
                task_id: execution.task_id.clone(),
                reason: execution
                    .error
                    .clone()
                    .unwrap_or_else(|| "execution cancelled".to_owned()),
            },
        }),
        ExecutionStatus::Running => {}
    };
}

pub(super) async fn clear_execution_retry_metadata(db: &SqliteDb, task: &Task) -> Result<()> {
    clear_execution_retry_metadata_inner(db, task, true).await
}

pub(super) async fn clear_execution_retry_metadata_preserving_dispatch(
    db: &SqliteDb,
    task: &Task,
) -> Result<()> {
    clear_execution_retry_metadata_inner(db, task, false).await
}

async fn clear_execution_retry_metadata_inner(
    db: &SqliteDb,
    task: &Task,
    clear_deferred_dispatch: bool,
) -> Result<()> {
    let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let mut changed = false;
    let mut keys = vec!["execution_retry_count", "last_execution_failure_at"];
    if clear_deferred_dispatch {
        keys.push("deferred_dispatch");
    }
    for key in keys {
        changed |= metadata.extra.remove(key).is_some();
    }
    if changed {
        TaskRepo::set_metadata_json(db, &task.id, metadata.to_json(), &now_rfc3339()).await?;
    }
    Ok(())
}

pub(super) fn usage_provider_from_agent_config(agent_config: &Value) -> String {
    usage_provider_for_executor_type(agent_config.get("executor_type").and_then(Value::as_str))
}

pub(super) fn usage_provider_from_snapshot(snapshot_json: Option<&str>) -> String {
    let executor_type = snapshot_json
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| {
            value
                .get("executor_type")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    usage_provider_for_executor_type(executor_type.as_deref())
}

fn usage_provider_for_executor_type(executor_type: Option<&str>) -> String {
    match executor_type.unwrap_or_default() {
        "codex" => "openai",
        "claude_code" => "anthropic",
        "cursor" => "cursor",
        "opencode" => "opencode",
        other if !other.is_empty() => other,
        _ => "unknown",
    }
    .to_owned()
}

pub(super) async fn set_planning_awaiting_review_metadata(
    db: &SqliteDb,
    task: &Task,
    execution_id: Option<&str>,
    awaiting: bool,
) -> Result<Task> {
    let mut metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    if awaiting {
        metadata
            .extra
            .insert("awaiting_human".to_owned(), json!(true));
        metadata
            .extra
            .insert("awaiting_human_reason".to_owned(), json!("plan_review"));
        metadata.extra.insert(
            "planning_completed_at".to_owned(),
            Value::String(now_rfc3339()),
        );
        if let Some(execution_id) = execution_id {
            metadata.extra.insert(
                "planning_execution_id".to_owned(),
                Value::String(execution_id.to_owned()),
            );
        }
    } else if metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
        == Some("plan_review")
    {
        metadata.extra.remove("awaiting_human");
        metadata.extra.remove("awaiting_human_reason");
        metadata.extra.remove("planning_completed_at");
        metadata.extra.remove("planning_execution_id");
    } else {
        return Ok(task.clone());
    }

    TaskRepo::set_metadata_json(db, &task.id, metadata.to_json(), &now_rfc3339()).await?;
    TaskRepo::get_by_id(db, &task.id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))
}
