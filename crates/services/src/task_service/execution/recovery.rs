use super::*;
use db::TaskMetadataMutation;

tokio::task_local! {
    pub(crate) static REPLAYING_RECOVERY: (String, String);
}

impl TaskService {
    pub(crate) fn recovery_context() -> Option<(String, String)> {
        REPLAYING_RECOVERY.try_with(Clone::clone).ok()
    }
    pub(crate) async fn with_recovery_command_context<T>(
        payload: &str,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        let payload = serde_json::from_str::<Value>(payload).unwrap_or(Value::Null);
        let replay =
            serde_json::from_value::<(String, String)>(payload["replaying_recovery"].clone()).ok();
        let actor = serde_json::from_value::<Actor>(payload["action_actor"].clone()).ok();
        let action_command = payload["action_command"] == true;
        let replayed = async move {
            match replay {
                Some(replay) => REPLAYING_RECOVERY.scope(replay, future).await,
                None => future.await,
            }
        };
        let acted = async move {
            match actor {
                Some(actor) => {
                    crate::task_service::actions::TASK_ACTION_ACTOR
                        .scope(actor, replayed)
                        .await
                }
                None => replayed.await,
            }
        };
        if action_command {
            crate::task_service::actions::TASK_ACTION_COMMAND
                .scope((), acted)
                .await
        } else {
            acted.await
        }
    }
    pub(crate) fn is_replaying_recovery(task_id: &str) -> bool {
        REPLAYING_RECOVERY
            .try_with(|(id, _)| id == task_id)
            .unwrap_or(false)
    }
}

async fn clear_skip_before_work_hook_once_override(
    db: &Arc<SqliteDb>,
    task: &Task,
    state_name: &str,
) -> Result<Task> {
    let Some(raw) = task.task_state_config.as_deref() else {
        return Ok(task.clone());
    };
    let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Ok(task.clone());
    };
    let Some(root) = parsed.as_object_mut() else {
        return Ok(task.clone());
    };
    let Some(state_value) = root.get_mut(state_name) else {
        return Ok(task.clone());
    };
    let Some(state_object) = state_value.as_object_mut() else {
        return Ok(task.clone());
    };
    if state_object.remove("skip_before_work_hook_once").is_none() {
        return Ok(task.clone());
    }
    if state_object.is_empty() {
        root.remove(state_name);
    }
    let next_config = if root.is_empty() {
        None
    } else {
        Some(parsed.to_string())
    };
    let updated = TaskRepo::update(
        &**db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: Some(next_config),
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    Ok(updated)
}

fn should_clear_assignments_for_reset(annotation: &api_types::TaskBlockingAnnotation) -> bool {
    // recovery_required covers crash-recovery and agent-timeout interruptions;
    // workspace failures arrive either as a live annotation
    // (workspace_reset_required / workspace_error) or, once fail_task has
    // cleared the annotation, synthesized from failed_json (workspace_failed).
    annotation.annotation_type == api_types::FailureKind::RecoveryRequired
        || annotation.annotation_type.is_workspace_failure()
}

fn workflow_initial_state(workflow: &api_types::WorkflowDefinition) -> Result<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == api_types::StateKind::Initial)
        .map(|state| state.name.clone())
        .ok_or_else(|| ServiceError::invalid_operation("workflow has no initial state"))
}

fn default_target_branch(repo_default_branch: &str) -> String {
    let trimmed = repo_default_branch.trim();
    if trimmed.is_empty() {
        "main".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn optional_recovery_reason(reason: Option<String>, action_kind: &str) -> String {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| action_kind.to_owned())
}

fn required_recovery_reason(reason: Option<String>, action_kind: &str) -> Result<String> {
    reason
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!("{action_kind} requires a recovery reason"))
        })
}

fn recovery_marker(
    task_id: &str,
    state: &str,
    action: &str,
    actor: &api_types::Actor,
    reason: &str,
) -> db::CreateTransitionLog {
    db::CreateTransitionLog {
        id: db::new_uuid_v4(),
        task_id: task_id.to_owned(),
        from_state: state.to_owned(),
        to_state: state.to_owned(),
        trigger_name: Some(action.to_owned()),
        triggered_by: actor.display(),
        bridge: api_types::TransitionBridge::recovery(
            action,
            matches!(
                action,
                "restart" | "reset_to_initial" | "reset_retry_window"
            ),
        ),
        trigger_reason: reason.to_owned(),
        hook_results_json: None,
        rejection: false,
        created_at: db::now_rfc3339(),
    }
}

fn overlapping_execution_roles(role: &str) -> Vec<String> {
    // Restoration must never put an old blocker over a live replacement in
    // another workflow role. The DB helper interprets an empty list as the
    // fail-closed all-role wildcard.
    let _ = role;
    Vec::new()
}

struct ResumeProcessPlan {
    gate_state: String,
    target_state: String,
    budget: i32,
    count: i64,
}

fn is_retry_exhausted_annotation(raw_annotation: &str) -> bool {
    match serde_json::from_str::<api_types::TaskAnnotation>(raw_annotation) {
        Ok(api_types::TaskAnnotation::Blocking(ref annotation)) => {
            crate::task_diagnostics::is_retry_budget_exhausted(annotation)
        }
        _ => false,
    }
}

fn blocked_metadata_kind(raw_metadata: &str) -> Option<api_types::FailureKind> {
    let metadata: Value = serde_json::from_str(raw_metadata).ok()?;
    serde_json::from_value(metadata.get("kind")?.clone()).ok()
}

fn is_retry_exhausted_blocked_metadata(raw_metadata: &str) -> bool {
    blocked_metadata_kind(raw_metadata)
        .is_some_and(api_types::FailureKind::is_retry_exhausted_metadata)
}

/// The text of a dependency wait that is a cycle: the Tasks waited for
/// cannot finish before this one (sibling order or parentage makes them wait
/// for it). Admission refuses such a graph, but a database written before it
/// did, or a terminal Task that re-opened, can still hold one. Removing the
/// dependency link (which wakes the Task) or cancelling one of the Tasks
/// ends it.
fn dependency_cycle_reason(dependency_ids: &[String]) -> String {
    format!(
        "dependency cycle: {} cannot finish before this Task (sibling order or parentage makes {} wait for it); remove the dependency (DELETE /api/v1/tasks/{{id}}/dependencies/{{depends_on_id}}) or cancel one of the Tasks",
        dependency_ids.join(", "),
        if dependency_ids.len() == 1 { "it" } else { "them" },
    )
}

mod action_owner;

mod queued_actions;

mod failure_metadata;

mod retry_budget;

mod continue_process;

mod restart;

mod manual_review;

mod entry_recovery;
