use super::*;
use crate::agent_capacity::count_running_executions;
use crate::workflow::dispatch::{
    build_effective_prompt, dispatch_intent_from_workflow_dispatch, effective_prompt_selection,
    loader::load_agent_dispatch_context,
};
use db::{UpdateTask, UpdateTaskStatus};

mod cascade;
mod follow_up;
mod guards;
mod hooks;
mod launch;
pub(crate) mod ledger;
mod recovery;
mod runner;

pub(super) use runner::{bounded_lease_expiry, execution_deadline_seconds, rfc3339_after};

pub(super) use cascade::should_block_task_for_failed_execution;
pub(crate) use cascade::{
    exact_review_for_execution, reviewer_execution_lacks_exact_review_binding,
    terminal_review_is_bound_to_execution,
};

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
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let mut mutations = Vec::new();
    if let Some(expected_count) = metadata.extra.get("execution_retry_count").cloned() {
        let mut nested = vec![
            db::TaskMetadataMutation::Remove {
                key: "execution_retry_count".to_owned(),
            },
            db::TaskMetadataMutation::Remove {
                key: "last_execution_failure_at".to_owned(),
            },
        ];
        if clear_deferred_dispatch {
            nested.push(db::TaskMetadataMutation::Remove {
                key: "deferred_dispatch".to_owned(),
            });
        }
        mutations.push(db::TaskMetadataMutation::CompareAndMutate {
            key: "execution_retry_count".to_owned(),
            expected: expected_count,
            mutations: nested,
        });
    } else if clear_deferred_dispatch {
        if let Some(expected) = metadata.extra.get("deferred_dispatch").cloned() {
            mutations.push(db::TaskMetadataMutation::RemoveIf {
                key: "deferred_dispatch".to_owned(),
                expected,
            });
        }
    }
    if !mutations.is_empty() {
        TaskRepo::mutate_metadata(db, &task.id, None, mutations, &now_rfc3339()).await?;
    }
    Ok(())
}

pub(super) async fn set_planning_awaiting_review_metadata(
    db: &SqliteDb,
    task: &Task,
    execution_id: Option<&str>,
    awaiting: bool,
) -> Result<Task> {
    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let mutations = if awaiting {
        let completed_at = now_rfc3339();
        let marker_id = new_uuid_v4();
        let mut next = vec![
            db::TaskMetadataMutation::Set {
                key: "awaiting_human".to_owned(),
                value: json!(true),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_reason".to_owned(),
                value: json!("plan_review"),
            },
            db::TaskMetadataMutation::Set {
                key: "planning_completed_at".to_owned(),
                value: Value::String(completed_at),
            },
            db::TaskMetadataMutation::Set {
                key: "awaiting_human_marker_id".to_owned(),
                value: Value::String(marker_id),
            },
        ];
        if let Some(execution_id) = execution_id {
            next.push(db::TaskMetadataMutation::Set {
                key: "planning_execution_id".to_owned(),
                value: Value::String(execution_id.to_owned()),
            });
        }
        next
    } else if metadata
        .extra
        .get("awaiting_human_reason")
        .and_then(Value::as_str)
        == Some("plan_review")
    {
        let Some((identity_key, expected)) = metadata
            .extra
            .get("awaiting_human_marker_id")
            .cloned()
            .map(|value| ("awaiting_human_marker_id", value))
            .or_else(|| {
                metadata
                    .extra
                    .get("planning_execution_id")
                    .cloned()
                    .map(|value| ("planning_execution_id", value))
            })
        else {
            // Legacy markers have no stable identity. Leaving one in place is
            // safer than allowing a stale transition to clear a newer marker
            // with the same reason.
            return Ok(task.clone());
        };
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: identity_key.to_owned(),
            expected,
            mutations: vec![
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_reason".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_completed_at".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "planning_execution_id".to_owned(),
                },
                db::TaskMetadataMutation::Remove {
                    key: "awaiting_human_marker_id".to_owned(),
                },
            ],
        }]
    } else {
        return Ok(task.clone());
    };

    TaskRepo::mutate_metadata(db, &task.id, Some(task.version), mutations, &now_rfc3339())
        .await
        .map_err(Into::into)
}
