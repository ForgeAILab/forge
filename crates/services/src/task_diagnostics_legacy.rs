use std::cmp::Ordering;

use api_types::{
    FailingStepSummary, HealthSeverity, RelatedEvidence, StateKind, TaskBlockingAnnotation,
    WorkflowDefinition, WorkflowExceptionSummary, WorkflowHealthKind, WorkflowHealthSummary,
};
use chrono::{DateTime, Utc};
use db::{
    AssigneeKind, Execution, ExecutionStatus, ResumePolicy, Review, ReviewStatus, Task,
    TaskMetadata, TaskRoleAssignment, TransitionLog,
};
use serde_json::Value;

use crate::workflow::effective_role;

// pub const DISPATCH_GRACE_SECONDS: i64 = 120;
// pub const STALE_DWELL_SECONDS: i64 = 3600;

/// Order rows that are already admitted to the running execution set for a
/// Task. A newly opened interactive attempt can be `Running` while it still
/// only owns its short-lived dispatch lease and has not received a provider
/// session yet. If an older interactive execution is already carrying a
/// session, that is the live user work the health projection must identify;
/// the lease-only row must not hide it merely because it was created later.
///
/// Callers must filter to `ExecutionStatus::Running` before using this
/// comparator. The final timestamp/id tie-break keeps the projection stable
/// when two rows have the same session state.
pub fn compare_running_execution_authority(left: &Execution, right: &Execution) -> Ordering {
    let left_has_session = left
        .agent_session_id
        .as_deref()
        .is_some_and(|session_id| !session_id.trim().is_empty());
    let right_has_session = right
        .agent_session_id
        .as_deref()
        .is_some_and(|session_id| !session_id.trim().is_empty());
    left_has_session
        .cmp(&right_has_session)
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.id.cmp(&right.id))
}

pub fn derive_workflow_health(
    task: &Task,
    workflow: &WorkflowDefinition,
    role_assignments: &[TaskRoleAssignment],
    latest_review: Option<&Review>,
    latest_execution: Option<&Execution>,
    awaiting_human: bool,
    workflow_exception: Option<&WorkflowExceptionSummary>,
) -> WorkflowHealthSummary {
    let current_state = workflow
        .states
        .iter()
        .find(|state| state.name == task.status);
    let role = current_state.and_then(effective_role).map(str::to_owned);
    let awaiting_human = awaiting_human
        || task_metadata_awaiting_human(task)
        || latest_review.is_some_and(|review| review.status == ReviewStatus::AwaitingHuman);

    // An interactive recovery session is live work, even when the Task row
    // still carries a stale blocker/failure/retry projection from the event
    // that opened that session. Surface the live execution first so the board
    // does not tell the user that the Task is failed or waiting while their
    // recovery session is actively running.
    if let Some(execution) = latest_execution.filter(|execution| {
        execution.role == "interactive" && execution.status == ExecutionStatus::Running
    }) {
        return health(
            WorkflowHealthKind::Running,
            HealthSeverity::Info,
            "Interactive",
            Some("Interactive execution is running".to_owned()),
            task,
            Some("interactive".to_owned()),
            Some(execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            execution.created_at.clone(),
            None,
        );
    }

    if task.failed_json.is_some() {
        return health(
            WorkflowHealthKind::Failed,
            HealthSeverity::Error,
            "Failed",
            interruption_message(task.failed_json.as_deref(), "Task failed"),
            task,
            role,
            latest_execution.map(|execution| execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            task.updated_at.clone(),
            None,
        );
    }

    if task.blocked_json.is_some() {
        return health(
            WorkflowHealthKind::Blocked,
            HealthSeverity::Error,
            "Blocked",
            interruption_message(task.blocked_json.as_deref(), "Task is blocked"),
            task,
            role,
            latest_execution.map(|execution| execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            task.updated_at.clone(),
            None,
        );
    }

    if let (Some(role), Some(execution)) = (role.as_deref(), latest_execution) {
        if execution_matches_role(execution, role) && execution.status == ExecutionStatus::Running {
            return health(
                WorkflowHealthKind::Running,
                HealthSeverity::Info,
                "Running",
                Some(format!("{role} execution is running")),
                task,
                Some(role.to_owned()),
                Some(execution.id.clone()),
                latest_review.map(|review| review.id.clone()),
                execution.created_at.clone(),
                None,
            );
        }
    }

    if let Some(deferred) = crate::deferred_dispatch::pending_until(task) {
        // `is_pending` is the dispatcher's admission check. In particular, a
        // malformed timestamp is treated as no deferral and the dispatcher
        // proceeds with the current role. Do not project such metadata as a
        // scheduled retry, or the board will claim that work is delayed when
        // the next scan is actually eligible to dispatch it. A malformed
        // target_state cannot reach this point because `pending_until` only
        // deserializes the required string fields, matching the dispatcher.
        if let Some(eligible_at) = DateTime::parse_from_rfc3339(&deferred.not_before)
            .ok()
            .map(|value| value.with_timezone(&Utc))
        {
            let waiting_for_capacity = eligible_at <= Utc::now();
            let role_label = role.as_deref().unwrap_or("agent");
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                if waiting_for_capacity {
                    "Retry Queued"
                } else {
                    "Retry Scheduled"
                },
                Some(if waiting_for_capacity {
                    format!(
                        "Automatic {role_label} retry has been eligible since {} and is waiting for capacity ({})",
                        deferred.not_before, deferred.reason
                    )
                } else {
                    format!(
                        "Automatic {role_label} retry is scheduled for {} ({})",
                        deferred.not_before, deferred.reason
                    )
                }),
                task,
                role,
                latest_execution.map(|execution| execution.id.clone()),
                latest_review.map(|review| review.id.clone()),
                if waiting_for_capacity {
                    deferred.not_before
                } else {
                    task.updated_at.clone()
                },
                Some(if waiting_for_capacity {
                    "execution_retry_waiting_for_capacity".to_owned()
                } else {
                    "execution_retry_scheduled".to_owned()
                }),
            );
        }
    }

    if awaiting_human {
        return health(
            WorkflowHealthKind::AwaitingHuman,
            HealthSeverity::Info,
            "Awaiting Human",
            Some("Task is awaiting human input".to_owned()),
            task,
            role,
            latest_execution.map(|execution| execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            task.updated_at.clone(),
            None,
        );
    }

    // Capacity waits are reconsidered each tick. Deterministic role refusals
    // stay parked until the Task changes or something wakes it, so show that
    // distinction instead of hiding them as ordinary queueing.
    if let Some(disposition) = crate::deferred_dispatch::current_dispatch_disposition(task) {
        if matches!(
            disposition.capability.as_str(),
            "project_capacity" | "machine_capacity"
        ) {
            let (reason, message) = disposition
                .safe_message
                .split_once(": ")
                .unwrap_or(("project_at_capacity", &disposition.safe_message));
            let label = if reason == "project_waiting_on_owner" {
                "Waiting on Owner"
            } else if reason == "machine_capacity" {
                "Waiting for a Machine Slot"
            } else if reason == "disk_pressure" {
                "Waiting for Disk Space"
            } else {
                "Waiting for a Slot"
            };
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                label,
                Some(message.to_owned()),
                task,
                role,
                latest_execution.map(|execution| execution.id.clone()),
                latest_review.map(|review| review.id.clone()),
                disposition.recorded_at,
                Some(reason.to_owned()),
            );
        }
        return health(
            WorkflowHealthKind::Stuck,
            HealthSeverity::Warning,
            "Dispatch Parked",
            Some(format!(
                "{} dispatch is parked until this Task changes or is woken: {}",
                disposition.capability, disposition.safe_message
            )),
            task,
            role,
            latest_execution.map(|execution| execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            disposition.recorded_at,
            Some("dispatch_parked".to_owned()),
        );
    }

    // Workflow exceptions are surfaced via the exception summary itself;
    // the "Stuck" health label was too noisy for normal dispatcher latency.
    // if let Some(exception) = workflow_exception {
    //     return health(
    //         WorkflowHealthKind::Stuck,
    //         HealthSeverity::Warning,
    //         "Stuck",
    //         Some(exception.message.clone()),
    //         task,
    //         role.or_else(|| exception.role.clone()),
    //         latest_execution.map(|execution| execution.id.clone()),
    //         exception
    //             .review_id
    //             .clone()
    //             .or_else(|| latest_review.map(|review| review.id.clone())),
    //         task.updated_at.clone(),
    //         Some(exception.exception_type.clone()),
    //     );
    // }
    let _ = workflow_exception;

    if let Some(role_name) = role.as_deref() {
        let assignment = role_assignments
            .iter()
            .find(|assignment| assignment.role_name == role_name);
        let agent_assigned = assignment.is_some_and(|assignment| {
            assignment.assignee_type == Some(AssigneeKind::Agent)
                && assignment.assignee_id.is_some()
        });

        if !agent_assigned {
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                "Waiting for Agent",
                Some(format!("Waiting for {role_name} assignment")),
                task,
                Some(role_name.to_owned()),
                None,
                latest_review.map(|review| review.id.clone()),
                task.updated_at.clone(),
                None,
            );
        }

        if let Some(execution) = latest_execution.filter(|execution| {
            execution_matches_role(execution, role_name)
                && stopped_execution_blocks_progress(execution)
        }) {
            return stopped_execution_health(task, role_name, execution, latest_review);
        }

        // Disabled: dispatch_missing_after_grace produced false "Stuck" labels
        // for tasks that are simply waiting for the dispatcher cycle.
        // if dispatch_missing_after_grace(task, latest_execution, role_name) {
        //     return health(
        //         WorkflowHealthKind::Stuck,
        //         HealthSeverity::Warning,
        //         "Stuck",
        //         Some(format!(
        //             "{role_name} is assigned but no execution has started or completed"
        //         )),
        //         task,
        //         Some(role_name.to_owned()),
        //         latest_execution.map(|execution| execution.id.clone()),
        //         latest_review.map(|review| review.id.clone()),
        //         task.updated_at.clone(),
        //         Some("dispatch_missing".to_owned()),
        //     );
        // }

        if latest_execution.is_none_or(|execution| !execution_matches_role(execution, role_name)) {
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                "Waiting for Agent",
                Some(format!("Waiting for {role_name} dispatch")),
                task,
                Some(role_name.to_owned()),
                None,
                latest_review.map(|review| review.id.clone()),
                task.updated_at.clone(),
                None,
            );
        }

        // The role's own attempt stopped in a way that does not need a human
        // decision — the commonest case is an execution terminalised by crash
        // recovery on restart. The Task is waiting for a free slot to run the
        // replacement, which is a different thing from having nothing to do.
        if let Some(execution) = latest_execution.filter(|execution| {
            execution_matches_role(execution, role_name)
                && matches!(
                    execution.status,
                    ExecutionStatus::Failed | ExecutionStatus::Cancelled
                )
                && matches!(execution.resume_policy, Some(ResumePolicy::Auto))
        }) {
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                "Retry Queued",
                Some(format!(
                    "The {role_name} attempt stopped ({}) and a replacement is waiting for capacity",
                    execution
                        .stop_reason
                        .as_ref()
                        .map(|reason| reason.to_string())
                        .or_else(|| execution.error.clone())
                        .unwrap_or_else(|| "no reason recorded".to_owned())
                )),
                task,
                Some(role_name.to_owned()),
                Some(execution.id.clone()),
                latest_review.map(|review| review.id.clone()),
                execution.updated_at.clone(),
                Some("dispatch_waiting_for_capacity".to_owned()),
            );
        }
    } else if current_state.is_some_and(|state| state.kind == StateKind::Initial) {
        // An Initial state carries no role of its own, so everything above is
        // skipped and a Task queued behind Worker capacity reports "Idle" —
        // indistinguishable from one that nothing will ever pick up. Name the
        // role it is queued for when an Agent actually holds it.
        if let Some(role_name) = initial_dispatch_role(workflow, task, role_assignments) {
            return health(
                WorkflowHealthKind::WaitingForAgent,
                HealthSeverity::Info,
                "Queued",
                Some(format!("Queued for {role_name} dispatch")),
                task,
                Some(role_name.to_owned()),
                None,
                latest_review.map(|review| review.id.clone()),
                task.updated_at.clone(),
                Some("queued_for_dispatch".to_owned()),
            );
        }
    }

    // Disabled: stale_dwell produced false "Stuck" labels for tasks
    // legitimately sitting in a state (e.g. todo waiting for assignment).
    // let is_terminal = current_state.is_some_and(|s| s.kind == StateKind::Terminal);
    // if !is_terminal && stale_dwell(task) {
    //     return health(
    //         WorkflowHealthKind::Stuck,
    //         HealthSeverity::Warning,
    //         "Stuck",
    //         Some("Task has not changed state recently".to_owned()),
    //         task,
    //         role,
    //         latest_execution.map(|execution| execution.id.clone()),
    //         latest_review.map(|review| review.id.clone()),
    //         task.updated_at.clone(),
    //         Some("stale_dwell".to_owned()),
    //     );
    // }

    health(
        WorkflowHealthKind::Idle,
        HealthSeverity::Info,
        "Idle",
        None,
        task,
        role,
        latest_execution.map(|execution| execution.id.clone()),
        latest_review.map(|review| review.id.clone()),
        task.updated_at.clone(),
        None,
    )
}

/// Diagnostic evidence is independent of command eligibility. The supplied
/// offers come exclusively from `available_actions` for the same snapshot.
pub fn task_exception(
    snapshot: &crate::TaskSnapshot,
    offers: Vec<api_types::Offer>,
) -> Option<WorkflowExceptionSummary> {
    task_exception_projection(
        &snapshot.task,
        &snapshot.workflow,
        &snapshot.executions,
        snapshot.latest_review.as_ref(),
        offers,
    )
}

pub fn task_exception_projection(
    task: &Task,
    workflow: &WorkflowDefinition,
    executions: &[Execution],
    latest_review: Option<&Review>,
    offers: Vec<api_types::Offer>,
) -> Option<WorkflowExceptionSummary> {
    let annotation = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<api_types::TaskBlockingAnnotation>(raw).ok());
    let latest_execution = executions
        .iter()
        .max_by_key(|execution| (&execution.created_at, &execution.id));
    let kind = crate::task_actions::legacy_task_condition(task);
    let failed_review = latest_failed_review(latest_review);
    if kind.is_none() && failed_review.is_none() && task.entry_barrier_json.is_none() {
        return None;
    }
    let role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(effective_role)
        .map(str::to_owned);
    Some(WorkflowExceptionSummary {
        exception_type: if task.failed_json.is_some() {
            "task_failed".to_owned()
        } else {
            kind.map(|kind| kind.to_string())
                .unwrap_or_else(|| "review_failed".to_owned())
        },
        message: annotation
            .as_ref()
            .and_then(|annotation| {
                annotation
                    .message
                    .clone()
                    .or_else(|| Some(annotation.blocking_reason.clone()))
            })
            .or_else(|| {
                interruption_message(
                    task.failed_json.as_deref().or(task.blocked_json.as_deref()),
                    "Task interrupted",
                )
            })
            .unwrap_or_else(|| "Review requires attention".to_owned()),
        review_id: failed_review.map(|review| review.id.clone()),
        execution_id: annotation
            .as_ref()
            .and_then(|annotation| annotation.blocked_execution_id.clone())
            .or_else(|| latest_execution.map(|execution| execution.id.clone())),
        state: Some(task.status.clone()),
        role: role.clone(),
        target_state: None,
        target_role: role,
        failing_step: failed_review
            .and_then(parse_failing_step)
            .or_else(|| annotation.as_ref().and_then(annotation_hook_failing_step)),
        related_evidence: related_failed_review(latest_review),
        actions: offers,
    })
}

fn annotation_hook_failing_step(annotation: &TaskBlockingAnnotation) -> Option<FailingStepSummary> {
    let hook = annotation.hook.as_ref()?;
    Some(FailingStepSummary {
        index: hook
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0),
        command: hook
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_owned),
        exit_code: hook
            .get("exit_code")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok()),
        output_tail: non_empty_string(hook.get("stdout")),
        stderr_tail: non_empty_string(hook.get("stderr")),
    })
}

#[allow(clippy::too_many_arguments)]
fn health(
    kind: WorkflowHealthKind,
    severity: HealthSeverity,
    label: &str,
    message: Option<String>,
    task: &Task,
    role: Option<String>,
    execution_id: Option<String>,
    review_id: Option<String>,
    since: String,
    stale_reason: Option<String>,
) -> WorkflowHealthSummary {
    WorkflowHealthSummary {
        kind,
        label: label.to_owned(),
        severity,
        message,
        state: Some(task.status.clone()),
        role,
        execution_id,
        review_id,
        since: Some(since),
        stale_reason,
    }
}

fn task_metadata_awaiting_human(task: &Task) -> bool {
    TaskMetadata::parse(task.metadata_json.as_deref())
        .ok()
        .and_then(|metadata| {
            metadata
                .extra
                .get("awaiting_human")
                .and_then(Value::as_bool)
        })
        .unwrap_or(false)
}

fn stopped_execution_blocks_progress(execution: &Execution) -> bool {
    execution.status != ExecutionStatus::Running
        && !matches!(execution.resume_policy, Some(ResumePolicy::Auto))
}

fn stopped_execution_health(
    task: &Task,
    role_name: &str,
    execution: &Execution,
    latest_review: Option<&Review>,
) -> WorkflowHealthSummary {
    let since = execution
        .stopped_at
        .clone()
        .unwrap_or_else(|| execution.updated_at.clone());

    match execution.status {
        ExecutionStatus::Failed => health(
            WorkflowHealthKind::Failed,
            HealthSeverity::Error,
            "Execution Failed",
            Some(execution.error.clone().unwrap_or_else(|| {
                format!(
                    "Latest {role_name} execution failed while task is still {}",
                    task.status
                )
            })),
            task,
            Some(role_name.to_owned()),
            Some(execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            since,
            Some("execution_failed_without_task_block".to_owned()),
        ),
        ExecutionStatus::Completed => health(
            WorkflowHealthKind::Stuck,
            HealthSeverity::Warning,
            "Stuck",
            Some(format!(
                "{role_name} execution completed but task is still {}",
                task.status
            )),
            task,
            Some(role_name.to_owned()),
            Some(execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            since,
            Some("execution_completed_without_transition".to_owned()),
        ),
        ExecutionStatus::Cancelled => health(
            WorkflowHealthKind::Stuck,
            HealthSeverity::Warning,
            "Execution Stopped",
            Some(format!(
                "{role_name} execution stopped but task is still {}",
                task.status
            )),
            task,
            Some(role_name.to_owned()),
            Some(execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            since,
            Some("execution_stopped_without_transition".to_owned()),
        ),
        ExecutionStatus::Running => health(
            WorkflowHealthKind::Running,
            HealthSeverity::Info,
            "Running",
            Some(format!("{role_name} execution is running")),
            task,
            Some(role_name.to_owned()),
            Some(execution.id.clone()),
            latest_review.map(|review| review.id.clone()),
            execution.created_at.clone(),
            None,
        ),
    }
}

pub fn is_retry_budget_exhausted(annotation: &TaskBlockingAnnotation) -> bool {
    annotation.annotation_type.is_budget_exhausted_annotation()
}

/// Return the audit-log suffix after the latest explicit retry-window reset.
///
/// A normal non-rejection transition — including a review-refresh bridge — is
/// not a reset; only an explicit recovery marker establishes a new boundary.
/// `gate_state = None` is used by the separate target-moved contention budget,
/// whose reset marker may be emitted while the Task is in a reject target.
pub fn entries_since_retry_window_boundary<'a>(
    entries: &'a [TransitionLog],
    gate_state: Option<&str>,
) -> &'a [TransitionLog] {
    let boundary = entries.iter().rposition(|entry| {
        !entry.rejection
            && gate_state.is_none_or(|state| entry.from_state == state)
            && entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::RetryWindowReset)
    });
    boundary
        .and_then(|index| entries.get(index + 1..))
        .unwrap_or(entries)
}

/// Count gate rejections in the current retry window.
///
/// Merge-refresh and conflict-handoff rows are mechanical bridges, not
/// merge-fix attempts. Excluding them here keeps
/// recovery, runtime admission, and API diagnostics on the same policy even
/// while the current engine normalizes new bridge rows to `rejection = false`.
#[cfg(test)]
pub fn audit_gate_rejections_since_boundary(entries: &[TransitionLog], gate_state: &str) -> i64 {
    let entries = entries_since_retry_window_boundary(entries, Some(gate_state));
    let workflow_actor = api_types::Actor::system(api_types::SystemComponent::Workflow).display();
    entries
        .iter()
        .filter(|entry| {
            entry.from_state == gate_state
                && entry.rejection
                && !(gate_state == crate::workflow::default_states::MERGING
                    && entry.to_state == crate::workflow::default_states::MERGE_FAILED
                    && (entry.bridge.is_review_refresh()
                        || entry.bridge.bridge_kind
                            == Some(api_types::TransitionBridgeKind::ConflictHandoff))
                    && entry.triggered_by == workflow_actor)
        })
        .count() as i64
}

#[cfg(test)]
pub async fn budget_spent_for_gate(
    db: &db::SqliteDb,
    task_id: &str,
    gate_state: &str,
) -> db::Result<i64> {
    db::budget::spent(db.pool(), task_id, &db::budget::gate_key(gate_state)).await
}

fn latest_failed_review(review: Option<&Review>) -> Option<&Review> {
    review.filter(|review| review.status == ReviewStatus::Failed)
}

fn related_failed_review(review: Option<&Review>) -> Vec<RelatedEvidence> {
    latest_failed_review(review)
        .map(|review| RelatedEvidence {
            kind: "review_failed".to_owned(),
            id: Some(review.id.clone()),
            message: Some(format!(
                "Latest review attempt {} failed",
                review.attempt_number
            )),
        })
        .into_iter()
        .collect()
}

fn parse_failing_step(review: &Review) -> Option<FailingStepSummary> {
    let value = serde_json::from_str::<Value>(&review.step_results_json).ok()?;
    let steps = if value.is_array() {
        value.as_array()
    } else {
        value.get("ci_steps").and_then(Value::as_array)
    }?;
    let (index, step) = steps
        .iter()
        .enumerate()
        .find(|(_, step)| step.get("exit_code").and_then(Value::as_i64).unwrap_or(0) != 0)?;
    Some(FailingStepSummary {
        index: step
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(index),
        command: step
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_owned),
        exit_code: step
            .get("exit_code")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok()),
        output_tail: non_empty_string(step.get("output_tail")),
        stderr_tail: non_empty_string(step.get("stderr_tail")),
    })
}

fn non_empty_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn interruption_message(raw: Option<&str>, fallback: &str) -> Option<String> {
    let value = raw.and_then(|raw| serde_json::from_str::<Value>(raw).ok())?;
    value
        .get("reason")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("kind"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| Some(fallback.to_owned()))
}

// Disabled along with Stuck health labeling — kept for future reuse.
// fn dispatch_missing_after_grace(
//     task: &Task,
//     latest_execution: Option<&Execution>,
//     role: &str,
// ) -> bool {
//     if latest_execution.is_some_and(|execution| {
//         execution_matches_role(execution, role)
//             && newer_or_equal(&execution.created_at, &task.updated_at)
//     }) {
//         return false;
//     }
//     elapsed_seconds_since(&task.updated_at).is_some_and(|seconds| seconds > DISPATCH_GRACE_SECONDS)
// }
//
// fn stale_dwell(task: &Task) -> bool {
//     elapsed_seconds_since(&task.updated_at).is_some_and(|seconds| seconds > STALE_DWELL_SECONDS)
// }
//
// fn elapsed_seconds_since(timestamp: &str) -> Option<i64> {
//     parse_timestamp(timestamp).map(|then| (Utc::now() - then).num_seconds())
// }
//
// fn newer_or_equal(left: &str, right: &str) -> bool {
//     match (parse_timestamp(left), parse_timestamp(right)) {
//         (Some(left), Some(right)) => left >= right,
//         _ => false,
//     }
// }
//
// fn parse_timestamp(timestamp: &str) -> Option<DateTime<Utc>> {
//     DateTime::parse_from_rfc3339(timestamp)
//         .ok()
//         .map(|value| value.with_timezone(&Utc))
// }

fn execution_matches_role(execution: &Execution, role: &str) -> bool {
    execution.role == role
        || (role == crate::workflow::default_roles::CODER && execution.role == "executor")
}

/// The role an Initial-state Task is queued to be dispatched into.
///
/// This mirrors the dispatcher's own `resolve_initial_schedule_target`: follow
/// non-system triggers to the first Active/Gate state, and keep walking
/// through any gate that is configured to cascade when its role is unassigned
/// — a Project with no planner reaches `in_progress`/`coder` that way. It
/// names what the Task is waiting for; it does not predict that dispatch will
/// succeed.
fn initial_dispatch_role<'a>(
    workflow: &'a WorkflowDefinition,
    task: &Task,
    role_assignments: &[TaskRoleAssignment],
) -> Option<&'a str> {
    let mut cursor = task.status.clone();
    let mut target_kinds = vec![StateKind::Active, StateKind::Gate];
    let mut visited = std::collections::HashSet::new();
    loop {
        if !visited.insert(cursor.clone()) {
            return None;
        }
        let target = workflow
            .outgoing_trigger_targets(&cursor)
            .filter(|(trigger, _)| !trigger.system_only())
            .find_map(|(_, target)| {
                workflow
                    .states
                    .iter()
                    .find(|state| state.name == target && target_kinds.contains(&state.kind))
            })?;
        let role = effective_role(target)?;
        match role_assignments
            .iter()
            .find(|assignment| assignment.role_name == role)
        {
            // Keep this in lockstep with the dispatcher's initial scheduler:
            // an Agent assignment is dispatchable, while a User assignment is
            // an explicit human-owned boundary and must stop the cascade even
            // when a later role has an Agent assignment.
            Some(assignment)
                if assignment.assignee_type == Some(AssigneeKind::Agent)
                    && assignment.assignee_id.is_some() =>
            {
                return Some(role);
            }
            Some(assignment) if assignment.assignee_type == Some(AssigneeKind::User) => {
                return None;
            }
            Some(assignment)
                if (assignment.assignee_type.is_none() || assignment.assignee_id.is_none())
                    && cascades_when_role_unassigned(target) =>
            {
                cursor = target.name.clone();
                target_kinds = vec![StateKind::Active];
            }
            Some(_) | None => {
                if cascades_when_role_unassigned(target) {
                    cursor = target.name.clone();
                    target_kinds = vec![StateKind::Active];
                } else {
                    return None;
                }
            }
        }
    }
}

/// Whether this gate is skipped when nobody holds its role, which is how a
/// Project without a planner reaches the implementation state directly.
fn cascades_when_role_unassigned(state: &api_types::StateDefinition) -> bool {
    state
        .gate_config
        .as_ref()
        .is_some_and(api_types::GateConfig::optional_when_unassigned)
        && state
            .hooks
            .after_enter
            .iter()
            .any(|hook| hook.action == "auto_cascade_on_unassigned_role")
}

