use api_types::{StateKind, WorkflowDefinition};
use db::{
    ExecutionRepo, ExecutionStatus, PageRequest, ResumePolicy, ReviewRepo, ReviewStatus, SortBy,
    SortOrder,
};
use serde_json::Value;

use crate::{Result, ServiceError};

pub(super) fn is_io_or_workspace_error(error: &ServiceError) -> bool {
    match error {
        ServiceError::InvalidOperation { message } => {
            message.contains("io error:") || message.contains("No such file or directory")
        }
        _ => false,
    }
}

/// Whether a dispatch refusal is *deterministic*: re-running the identical
/// attempt against the identical Task version and governance state produces
/// the identical refusal.
///
/// Only a deterministic refusal earns a dispatch disposition. Quiescing on a
/// transient failure — a busy database, an unreachable daemon, a rate limit —
/// would park the Task until its `version` happened to change or someone
/// called `wake_task_dispatch`, turning a momentary blip into a permanent
/// stall. Those keep the previous behavior of being retried next scan.
///
/// Classification is by typed variant, never by matching refusal text: the
/// governance/denial wording belongs to the blocker projection and is
/// expected to keep changing.
pub(super) fn is_deterministic_dispatch_refusal(error: &ServiceError) -> bool {
    match error {
        // Governance and admission outcomes: setup a user must complete, an
        // authority the active baseline does not grant, an unresolved
        // canonical conflict, or an action this Task cannot currently offer.
        ServiceError::ExecutionSetupRequired { .. }
        | ServiceError::AuthorizationDenied { .. }
        | ServiceError::Conflict(_)
        | ServiceError::TaskActionUnavailable { .. }
        | ServiceError::DependencyGate
        | ServiceError::MissingPrimaryRepo { .. }
        | ServiceError::RepoMismatch { .. }
        | ServiceError::PrProviderMissing { .. } => true,
        // A malformed or contract-violating request cannot fix itself, but an
        // I/O or workspace failure surfaced through the same variant can.
        ServiceError::InvalidOperation { .. } => !is_io_or_workspace_error(error),
        // Everything else — database, git, review, daemon, rate limiting — is
        // treated as potentially transient and retried.
        _ => false,
    }
}

pub(super) fn has_blocking_annotation(task: &db::Task) -> bool {
    let Some(raw_annotation) = task.error_annotation.as_deref() else {
        return false;
    };
    let Ok(annotation) = serde_json::from_str::<Value>(raw_annotation) else {
        return false;
    };
    let Some(kind) = annotation.get("type").and_then(Value::as_str) else {
        return false;
    };
    matches!(
        kind,
        "manual_stop"
            | "workspace_error"
            | "agent_timeout"
            | "recovery_required"
            | "workspace_reset_required"
            | "max_turns_exceeded"
            | "before_work_hook_failed"
            | "before_work_hook_timeout"
            | crate::workflow::engine::DISPATCH_FAILED_ANNOTATION
    )
}

pub(super) fn auto_cascades_on_unassigned_role(state: &api_types::StateDefinition) -> bool {
    if !state
        .gate_config
        .as_ref()
        .is_some_and(|config| config.optional_when_unassigned())
    {
        return false;
    }
    state
        .hooks
        .after_enter
        .iter()
        .any(|hook| hook.action == "auto_cascade_on_unassigned_role")
}

pub(super) fn role_assignment_unassigned(assignment: Option<&db::TaskRoleAssignment>) -> bool {
    !assignment.is_some_and(|assignment| {
        assignment.assignee_type.is_some() && assignment.assignee_id.is_some()
    })
}

pub(super) fn execution_guard_roles(role: &str) -> Vec<&str> {
    let mut roles = vec![role];
    if role == crate::workflow::default_roles::CODER {
        roles.push("executor");
    }
    roles
}

pub(super) async fn reviewer_dispatch_ready(
    db: &db::SqliteDb,
    task_id: &str,
    state_config: &Value,
) -> Result<bool> {
    if !review_ci_steps_configured(state_config) {
        return Ok(true);
    }

    let latest_review = ReviewRepo::list_by_task(db, task_id)
        .await?
        .into_iter()
        .max_by_key(|review| review.attempt_number);
    let Some(review) = latest_review else {
        return Ok(false);
    };
    if review.status != ReviewStatus::Running {
        return Ok(false);
    }

    Ok(review_ci_steps_finished(&review.step_results_json))
}

fn review_ci_steps_configured(state_config: &Value) -> bool {
    let review_config = state_config.get("review").unwrap_or(state_config);
    review_config
        .get("ci_steps")
        .and_then(Value::as_array)
        .is_some_and(|steps| !steps.is_empty())
}

fn review_ci_steps_finished(step_results_json: &str) -> bool {
    let Ok(details) = serde_json::from_str::<Value>(step_results_json) else {
        return false;
    };
    details
        .get("ci_steps")
        .and_then(Value::as_array)
        .is_some_and(|steps| {
            !steps.is_empty()
                && steps
                    .iter()
                    .all(|step| step.get("exit_code").and_then(Value::as_i64).is_some())
        })
}

pub(super) async fn latest_stopped_execution_blocks_dispatch(
    db: &db::SqliteDb,
    task_id: &str,
    role_name: &str,
) -> Result<bool> {
    let page = ExecutionRepo::list_by_task_and_role(
        db,
        task_id,
        role_name,
        PageRequest {
            cursor: None,
            limit: 1,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;

    let Some(execution) = page.items.into_iter().next() else {
        return Ok(false);
    };
    if execution.status == ExecutionStatus::Running {
        return Ok(false);
    }

    Ok(matches!(
        execution.resume_policy,
        None | Some(ResumePolicy::Manual)
    ))
}

pub(super) async fn has_running_execution_for_roles(
    db: &db::SqliteDb,
    task_id: &str,
    roles: &[&str],
) -> Result<bool> {
    let page = ExecutionRepo::list_by_task(
        db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;
    Ok(page.items.iter().any(|execution| {
        execution.status == ExecutionStatus::Running
            && roles.iter().any(|role| execution.role == *role)
    }))
}

pub(super) fn first_transition_to_kind<'a>(
    workflow: &'a WorkflowDefinition,
    from_state: &str,
    kinds: &[StateKind],
) -> Option<&'a api_types::StateDefinition> {
    workflow
        .outgoing_trigger_targets(from_state)
        .filter(|(trigger, _)| !trigger.system_only())
        .find_map(|(_, target)| {
            workflow
                .states
                .iter()
                .find(|state| state.name == target && kinds.contains(&state.kind))
        })
}

pub(super) fn merged_state_config(
    state: &api_types::StateDefinition,
    project: &db::Project,
    task_state_config_json: Option<&str>,
) -> Value {
    let mut merged = state.config.clone();
    if state.name == crate::workflow::default_states::REVIEW {
        merge_project_review_config(&mut merged, project);
    }

    let Some(task_state_config_json) = task_state_config_json else {
        return merged;
    };
    let Ok(Value::Object(task_config)) = serde_json::from_str::<Value>(task_state_config_json)
    else {
        return merged;
    };
    let Some(Value::Object(overrides)) = task_config.get(&state.name) else {
        return merged;
    };

    match &mut merged {
        Value::Object(defaults) => {
            for (key, value) in overrides {
                defaults.insert(key.clone(), value.clone());
            }
            merged
        }
        _ => Value::Object(overrides.clone()),
    }
}

fn merge_project_review_config(merged: &mut Value, project: &db::Project) {
    let Ok(settings) = serde_json::from_str::<Value>(&project.settings) else {
        return;
    };
    let Some(Value::Object(review_config)) = settings.get("default_review_config") else {
        return;
    };
    match merged {
        Value::Object(defaults) => {
            for (key, value) in review_config {
                defaults.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        _ => {
            *merged = Value::Object(review_config.clone());
        }
    }
}
