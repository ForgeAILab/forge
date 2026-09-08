use std::collections::HashMap;

use api_types::{
    FailingStepSummary, FailureKind, HealthSeverity, RecoveryAction, RelatedEvidence, StateKind,
    TaskAnnotation, TaskBlockingAnnotation, WorkflowDefinition, WorkflowExceptionAction,
    WorkflowExceptionSummary, WorkflowHealthKind, WorkflowHealthSummary,
};
// use chrono::{DateTime, Utc};
use db::{
    AssigneeKind, Execution, ExecutionStatus, ResumePolicy, Review, ReviewStatus, Task,
    TaskMetadata, TaskRoleAssignment,
};
use serde_json::Value;

use crate::workflow::effective_role;

// pub const DISPATCH_GRACE_SECONDS: i64 = 120;
// pub const STALE_DWELL_SECONDS: i64 = 3600;

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

pub fn derive_workflow_exception(
    task: &Task,
    workflow: &WorkflowDefinition,
    role_assignments: &[TaskRoleAssignment],
    latest_review: Option<&Review>,
    latest_execution: Option<&Execution>,
    remaining_retries: &HashMap<String, i64>,
) -> Option<WorkflowExceptionSummary> {
    let current_state = workflow
        .states
        .iter()
        .find(|state| state.name == task.status);
    let role = current_state.and_then(effective_role).map(str::to_owned);

    // A hard failure supersedes any blocking annotation: recover_task only
    // accepts ResetToInitial/CancelTask once failed_json is set, so offering
    // annotation-derived retry actions here would produce guaranteed 400s.
    if task.failed_json.is_some() {
        return Some(task_failed_exception(
            task,
            workflow,
            role,
            latest_review,
            latest_execution,
        ));
    }

    // Interruption-era blocked metadata with a recoverable structured kind is
    // classified directly from the typed kind — no synthesized annotation
    // intermediate, no prose matching.
    if let Some(metadata) = parse_blocked_metadata(task) {
        if metadata.kind.is_retry_exhausted_metadata() {
            let actions = retry_budget_exhausted_annotation_actions(
                task,
                workflow,
                role.clone(),
                latest_execution,
            );
            return Some(blocked_metadata_exception(
                task,
                &metadata,
                "Task is blocked",
                role,
                latest_review,
                latest_execution,
                actions,
            ));
        }
        if task.status == crate::workflow::default_states::MERGING
            && metadata.kind == FailureKind::TargetRepoDirty
        {
            let actions =
                merge_gate_annotation_actions(task, workflow, role.clone(), latest_execution);
            return Some(blocked_metadata_exception(
                task,
                &metadata,
                "Target repository needs attention",
                role,
                latest_review,
                latest_execution,
                actions,
            ));
        }
        if task.status == crate::workflow::default_states::MERGE_FAILED
            && metadata.kind.is_merge_recoverable()
        {
            let actions = merge_fix_annotation_actions(task, role.clone(), latest_execution);
            return Some(blocked_metadata_exception(
                task,
                &metadata,
                "Merge fix is blocked",
                role,
                latest_review,
                latest_execution,
                actions,
            ));
        }
    }

    if let Some(annotation) = active_blocking_annotation(task) {
        let actions = annotation_actions(
            &annotation,
            workflow,
            task,
            role_assignments,
            latest_execution,
        );
        let actions = if actions.is_empty() && is_retry_budget_exhausted(&annotation) {
            retry_budget_exhausted_annotation_actions(
                task,
                workflow,
                role.clone(),
                latest_execution,
            )
        } else if actions.is_empty() && is_recoverable_merge_gate_annotation(task, &annotation) {
            merge_gate_annotation_actions(task, workflow, role.clone(), latest_execution)
        } else if actions.is_empty() && is_recoverable_merge_fix_annotation(task, &annotation) {
            merge_fix_annotation_actions(task, role.clone(), latest_execution)
        } else {
            actions
        };
        let mut summary = WorkflowExceptionSummary {
            exception_type: annotation.annotation_type.to_string(),
            message: annotation
                .message
                .clone()
                .unwrap_or_else(|| annotation.blocking_reason.clone()),
            review_id: latest_failed_review(latest_review).map(|review| review.id.clone()),
            execution_id: annotation
                .blocked_execution_id
                .clone()
                .or_else(|| latest_execution.map(|execution| execution.id.clone())),
            state: Some(task.status.clone()),
            role: role.clone(),
            target_state: None,
            target_role: role.clone(),
            failing_step: latest_failed_review(latest_review)
                .and_then(parse_failing_step)
                .or_else(|| annotation_hook_failing_step(&annotation)),
            related_evidence: related_failed_review(latest_review),
            actions,
        };
        if summary.actions.is_empty() {
            summary
                .actions
                .push(cancel_action(false, task_is_terminal(workflow, task)));
        }
        return Some(summary);
    }

    let is_gate_state = current_state.is_some_and(|state| state.kind == StateKind::Gate);
    if is_gate_state {
        if let Some(review) = latest_failed_review(latest_review) {
            let failing_step = parse_failing_step(review);
            let reviewer_execution_failed = review_failed_by_reviewer_execution(review);
            let reviewer_retry_exhausted =
                reviewer_execution_failed && execution_retry_window_exhausted(task, current_state);
            let has_exhausted_annotation = active_blocking_annotation(task)
                .as_ref()
                .is_some_and(is_retry_budget_exhausted);
            let current_retry_window_exhausted = has_exhausted_annotation
                || remaining_retries
                    .get(&task.status)
                    .is_some_and(|remaining| *remaining == 0);
            let retry_enabled = if reviewer_execution_failed {
                !reviewer_retry_exhausted
            } else {
                !current_retry_window_exhausted
            };
            let mut actions = vec![action(
                RecoveryAction::RetryHook,
                "Retry Review",
                retry_enabled,
                if reviewer_execution_failed && reviewer_retry_exhausted {
                    Some("Reviewer retry budget exhausted".to_owned())
                } else if current_retry_window_exhausted {
                    Some("Retry budget exhausted; reset the retry window first".to_owned())
                } else {
                    None
                },
                false,
                false,
                true,
                Some(task.status.clone()),
                role.clone(),
                Some(review.id.clone()),
            )];
            if reviewer_execution_failed {
                actions.push(action(
                    RecoveryAction::MarkReviewed,
                    "Pass Review",
                    true,
                    None,
                    false,
                    false,
                    true,
                    Some(
                        review_pass_target(workflow, task)
                            .unwrap_or_else(|| crate::workflow::default_states::MERGING.to_owned()),
                    ),
                    role.clone(),
                    None,
                ));
            } else {
                let resume_target = gate_reject_target(workflow, task);
                let resume_role = resume_target
                    .as_deref()
                    .and_then(|target| workflow_role(workflow, target));
                actions.push(action(
                    RecoveryAction::ResumeProcess,
                    "Resume Process",
                    !current_retry_window_exhausted,
                    disabled_unless(
                        !current_retry_window_exhausted,
                        "Retry budget exhausted; reset the retry window first",
                    ),
                    false,
                    false,
                    true,
                    resume_target,
                    resume_role,
                    None,
                ));
                actions.extend([
                    action(
                        RecoveryAction::ResetRetryWindow,
                        "Reset Retry Window",
                        current_retry_window_exhausted,
                        disabled_unless(
                            current_retry_window_exhausted,
                            "Retry window is not exhausted for the current state",
                        ),
                        false,
                        false,
                        false,
                        Some(task.status.clone()),
                        role.clone(),
                        None,
                    ),
                    action(
                        RecoveryAction::ProceedOnce,
                        "Proceed Once",
                        task.status == crate::workflow::default_states::REVIEW
                            && current_retry_window_exhausted,
                        disabled_unless(
                            task.status == crate::workflow::default_states::REVIEW
                                && current_retry_window_exhausted,
                            "Proceed once is only available for exhausted review retry windows",
                        ),
                        true,
                        true,
                        true,
                        gate_reject_target(workflow, task),
                        role.clone(),
                        None,
                    ),
                ]);
                actions.push(open_interactive_action(
                    task,
                    role.clone(),
                    latest_execution,
                ));
            }

            let target_state = gate_reject_target(workflow, task);
            let target_role = target_state
                .as_deref()
                .and_then(|target| workflow_role(workflow, target));
            return Some(WorkflowExceptionSummary {
                exception_type: "review_failed".to_owned(),
                message: failing_step_message(&failing_step, &review.id),
                review_id: Some(review.id.clone()),
                execution_id: Some(review.execution_id.clone()),
                state: Some(task.status.clone()),
                role: role.clone(),
                target_state,
                target_role,
                failing_step,
                related_evidence: Vec::new(),
                actions,
            });
        }
    }

    recovery_unavailable(task, workflow, role, latest_execution)
}

fn task_failed_exception(
    task: &Task,
    workflow: &WorkflowDefinition,
    role: Option<String>,
    latest_review: Option<&Review>,
    latest_execution: Option<&Execution>,
) -> WorkflowExceptionSummary {
    WorkflowExceptionSummary {
        exception_type: "task_failed".to_owned(),
        message: interruption_message(task.failed_json.as_deref(), "Task failed")
            .unwrap_or_else(|| "Task failed".to_owned()),
        review_id: latest_failed_review(latest_review).map(|review| review.id.clone()),
        execution_id: latest_execution.map(|execution| execution.id.clone()),
        state: Some(task.status.clone()),
        role,
        target_state: workflow_initial_state(workflow),
        target_role: None,
        failing_step: latest_failed_review(latest_review).and_then(parse_failing_step),
        related_evidence: related_failed_review(latest_review),
        actions: vec![
            action(
                RecoveryAction::ResetToInitial,
                "Restart Task",
                true,
                None,
                false,
                false,
                true,
                workflow_initial_state(workflow),
                None,
                None,
            ),
            cancel_action(true, task_is_terminal(workflow, task)),
        ],
    }
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
        && matches!(execution.resume_policy, None | Some(ResumePolicy::Manual))
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

fn active_blocking_annotation(task: &Task) -> Option<TaskBlockingAnnotation> {
    match serde_json::from_str::<TaskAnnotation>(task.error_annotation.as_deref()?).ok()? {
        TaskAnnotation::Blocking(annotation) => Some(annotation),
        TaskAnnotation::Legacy(_) => None,
    }
}

struct BlockedMetadataSummary {
    kind: FailureKind,
    reason: Option<String>,
    execution_id: Option<String>,
}

fn parse_blocked_metadata(task: &Task) -> Option<BlockedMetadataSummary> {
    let metadata: Value = serde_json::from_str(task.blocked_json.as_deref()?).ok()?;
    let kind = serde_json::from_value::<FailureKind>(metadata.get("kind")?.clone()).ok()?;
    Some(BlockedMetadataSummary {
        kind,
        reason: metadata
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        execution_id: metadata
            .get("execution_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

#[allow(clippy::too_many_arguments)]
fn blocked_metadata_exception(
    task: &Task,
    metadata: &BlockedMetadataSummary,
    default_reason: &str,
    role: Option<String>,
    latest_review: Option<&Review>,
    latest_execution: Option<&Execution>,
    actions: Vec<WorkflowExceptionAction>,
) -> WorkflowExceptionSummary {
    WorkflowExceptionSummary {
        exception_type: metadata.kind.to_string(),
        message: metadata
            .reason
            .clone()
            .unwrap_or_else(|| default_reason.to_owned()),
        review_id: latest_failed_review(latest_review).map(|review| review.id.clone()),
        execution_id: metadata
            .execution_id
            .clone()
            .or_else(|| latest_execution.map(|execution| execution.id.clone())),
        state: Some(task.status.clone()),
        role: role.clone(),
        target_state: None,
        target_role: role,
        failing_step: latest_failed_review(latest_review).and_then(parse_failing_step),
        related_evidence: related_failed_review(latest_review),
        actions,
    }
}

fn retry_budget_exhausted_annotation_actions(
    task: &Task,
    workflow: &WorkflowDefinition,
    role: Option<String>,
    latest_execution: Option<&Execution>,
) -> Vec<WorkflowExceptionAction> {
    let resume_target = gate_reject_target(workflow, task);
    let resume_role = resume_target
        .as_deref()
        .and_then(|target| workflow_role(workflow, target));
    let reset_resumes_process = matches!(
        task.status.as_str(),
        crate::workflow::default_states::REVIEW | crate::workflow::default_states::MERGING
    );
    vec![
        action(
            RecoveryAction::RetryHook,
            gate_retry_label(task),
            task.status == crate::workflow::default_states::MERGING,
            disabled_unless(
                task.status == crate::workflow::default_states::MERGING,
                "Retry budget exhausted; reset the retry window first",
            ),
            false,
            false,
            true,
            if task.status == crate::workflow::default_states::MERGING {
                resume_target.clone()
            } else {
                Some(task.status.clone())
            },
            if task.status == crate::workflow::default_states::MERGING {
                resume_role.clone()
            } else {
                role.clone()
            },
            latest_execution.map(|execution| execution.id.clone()),
        ),
        action(
            RecoveryAction::ResumeProcess,
            "Resume Process",
            false,
            Some("Retry budget exhausted; reset the retry window first".to_owned()),
            false,
            false,
            true,
            resume_target.clone(),
            resume_role.clone(),
            None,
        ),
        action(
            RecoveryAction::ResetRetryWindow,
            "Reset Retry Window",
            true,
            None,
            false,
            false,
            reset_resumes_process,
            if reset_resumes_process {
                resume_target.clone()
            } else {
                Some(task.status.clone())
            },
            if reset_resumes_process {
                resume_role.clone()
            } else {
                role.clone()
            },
            None,
        ),
        action(
            RecoveryAction::ProceedOnce,
            "Proceed Once",
            task.status == crate::workflow::default_states::REVIEW,
            disabled_unless(
                task.status == crate::workflow::default_states::REVIEW,
                "Proceed once is only available for exhausted review retry windows",
            ),
            true,
            true,
            true,
            resume_target,
            role.clone(),
            None,
        ),
        open_interactive_action(task, role, latest_execution),
    ]
}

fn gate_retry_label(task: &Task) -> &'static str {
    match task.status.as_str() {
        crate::workflow::default_states::REVIEW => "Retry Review",
        crate::workflow::default_states::MERGING => "Retry Merge",
        _ => "Retry",
    }
}

fn is_recoverable_merge_gate_annotation(task: &Task, annotation: &TaskBlockingAnnotation) -> bool {
    task.status == crate::workflow::default_states::MERGING
        && annotation.annotation_type.is_merge_recoverable()
}

fn is_recoverable_merge_fix_annotation(task: &Task, annotation: &TaskBlockingAnnotation) -> bool {
    task.status == crate::workflow::default_states::MERGE_FAILED
        && annotation.annotation_type.is_merge_recoverable()
}

fn merge_gate_annotation_actions(
    task: &Task,
    _workflow: &WorkflowDefinition,
    role: Option<String>,
    latest_execution: Option<&Execution>,
) -> Vec<WorkflowExceptionAction> {
    vec![
        action(
            RecoveryAction::RetryHook,
            "Retry Merge",
            true,
            None,
            false,
            false,
            true,
            Some(task.status.clone()),
            role.clone(),
            latest_execution.map(|execution| execution.id.clone()),
        ),
        action(
            RecoveryAction::ResetRetryWindow,
            "Reset Retry Window",
            false,
            Some("Retry window is not exhausted for the current state".to_owned()),
            false,
            false,
            false,
            Some(task.status.clone()),
            role.clone(),
            None,
        ),
        open_interactive_action(task, role, latest_execution),
    ]
}

fn merge_fix_annotation_actions(
    task: &Task,
    role: Option<String>,
    latest_execution: Option<&Execution>,
) -> Vec<WorkflowExceptionAction> {
    vec![
        action(
            RecoveryAction::RetryHook,
            "Retry Merge Fix",
            true,
            None,
            false,
            false,
            true,
            Some(task.status.clone()),
            role.clone(),
            latest_execution.map(|execution| execution.id.clone()),
        ),
        action(
            RecoveryAction::Reexecute,
            "Re-execute",
            true,
            None,
            false,
            false,
            true,
            Some(task.status.clone()),
            role.clone(),
            latest_execution.map(|execution| execution.id.clone()),
        ),
        open_interactive_action(task, role, latest_execution),
    ]
}

fn annotation_actions(
    annotation: &TaskBlockingAnnotation,
    workflow: &WorkflowDefinition,
    task: &Task,
    role_assignments: &[TaskRoleAssignment],
    latest_execution: Option<&Execution>,
) -> Vec<WorkflowExceptionAction> {
    annotation
        .recovery_actions
        .iter()
        .copied()
        .map(|kind| {
            let terminal = task_is_terminal(workflow, task);
            let resume_unavailable = matches!(kind, RecoveryAction::ResumeSession)
                && !latest_execution.is_some_and(|execution| {
                    annotation
                        .blocked_execution_id
                        .as_deref()
                        .is_none_or(|id| id == execution.id)
                        && execution.agent_id.is_some()
                        && execution.agent_session_id.is_some()
                        && execution.executor_config_snapshot_json.is_some()
                        && execution.agent_id.as_deref().is_some_and(|agent_id| {
                            role_assignments
                                .iter()
                                .find(|assignment| assignment.role_name == execution.role)
                                .map_or_else(
                                    || {
                                        task.assignee_type.as_deref() == Some("agent")
                                            && task.assignee_id.as_deref() == Some(agent_id)
                                    },
                                    |assignment| {
                                        assignment.assignee_type == Some(AssigneeKind::Agent)
                                            && assignment.assignee_id.as_deref() == Some(agent_id)
                                    },
                                )
                        })
                });
            let enabled = !terminal && !resume_unavailable;
            let disabled_reason = if terminal {
                Some("Task is in terminal state".to_owned())
            } else if resume_unavailable {
                Some("The stopped execution has no resumable assigned session".to_owned())
            } else {
                None
            };
            let resume_target = matches!(kind, RecoveryAction::ResumeProcess)
                .then(|| gate_reject_target(workflow, task))
                .flatten();
            let resume_role = resume_target
                .as_deref()
                .and_then(|target| workflow_role(workflow, target));
            let target_execution_id = if matches!(
                kind,
                RecoveryAction::ResumeSession
                    | RecoveryAction::Reexecute
                    | RecoveryAction::OpenInteractive
            ) {
                annotation
                    .blocked_execution_id
                    .clone()
                    .or_else(|| latest_execution.map(|execution| execution.id.clone()))
            } else {
                None
            };
            action(
                kind,
                recovery_label(kind),
                enabled,
                disabled_reason,
                matches!(kind, RecoveryAction::ProceedOnce),
                matches!(kind, RecoveryAction::ProceedOnce),
                matches!(
                    kind,
                    RecoveryAction::ResumeSession
                        | RecoveryAction::Reexecute
                        | RecoveryAction::ProceedOnce
                        | RecoveryAction::ResumeProcess
                        | RecoveryAction::RetryHook
                        | RecoveryAction::UpdateWorkspaceAndRetryHook
                        | RecoveryAction::SkipHookOnce
                ),
                resume_target.or_else(|| Some(task.status.clone())),
                resume_role,
                target_execution_id,
            )
        })
        .collect()
}

fn recovery_unavailable(
    task: &Task,
    _workflow: &WorkflowDefinition,
    _role: Option<String>,
    _latest_execution: Option<&Execution>,
) -> Option<WorkflowExceptionSummary> {
    if task.blocked_json.is_none() && task.error_annotation.is_none() {
        return None;
    }
    None
}

fn review_failed_by_reviewer_execution(review: &Review) -> bool {
    serde_json::from_str::<Value>(&review.step_results_json)
        .ok()
        .and_then(|details| details.get("execution").cloned())
        .is_some()
}

fn execution_retry_window_exhausted(
    task: &Task,
    current_state: Option<&api_types::StateDefinition>,
) -> bool {
    let budget = crate::task_service::config::runtime_retry_budget(
        task,
        crate::task_service::config::RetryBudgetKind::Execution,
        current_state.map(|state| &state.config),
        current_state.and_then(|state| state.gate_config.as_ref()),
    )
    .unwrap_or(0);
    if budget <= 0 {
        return true;
    }
    let retry_count = TaskMetadata::parse(task.metadata_json.as_deref())
        .ok()
        .and_then(|metadata| {
            metadata
                .extra
                .get("execution_retry_count")
                .and_then(Value::as_u64)
                .map(|value| value as i64)
        })
        .unwrap_or(0);
    retry_count >= i64::from(budget)
}

fn open_interactive_action(
    task: &Task,
    role: Option<String>,
    latest_execution: Option<&Execution>,
) -> WorkflowExceptionAction {
    let has_target = latest_execution
        .and_then(|execution| execution.agent_id.as_ref())
        .is_some();
    action(
        RecoveryAction::OpenInteractive,
        "Open Interactive",
        has_target,
        disabled_unless(
            has_target,
            "Open interactive requires an assigned agent or previous execution",
        ),
        false,
        false,
        false,
        Some(task.status.clone()),
        role,
        latest_execution.map(|execution| execution.id.clone()),
    )
}

fn review_pass_target(workflow: &WorkflowDefinition, task: &Task) -> Option<String> {
    workflow
        .auto_transition_target(&task.status)
        .map(str::to_owned)
}

fn cancel_action(_enabled_for_failed_task: bool, is_terminal: bool) -> WorkflowExceptionAction {
    action(
        RecoveryAction::CancelTask,
        "Cancel Task",
        !is_terminal,
        if is_terminal {
            Some("Task is already in terminal state".to_owned())
        } else {
            None
        },
        false,
        false,
        false,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn action(
    kind: RecoveryAction,
    label: impl Into<String>,
    enabled: bool,
    disabled_reason: Option<String>,
    requires_reason: bool,
    requires_guidance: bool,
    propagates: bool,
    target_state: Option<String>,
    target_role: Option<String>,
    target_execution_id: Option<String>,
) -> WorkflowExceptionAction {
    WorkflowExceptionAction {
        kind,
        label: label.into(),
        enabled,
        disabled_reason,
        requires_reason,
        requires_guidance,
        propagates,
        target_state,
        target_role,
        target_execution_id,
    }
}

fn disabled_unless(enabled: bool, reason: &str) -> Option<String> {
    (!enabled).then(|| reason.to_owned())
}

fn recovery_label(kind: RecoveryAction) -> &'static str {
    match kind {
        RecoveryAction::ResumeSession => "Resume Session",
        RecoveryAction::Reexecute => "Re-execute",
        RecoveryAction::ResetToInitial => "Reset to Initial",
        RecoveryAction::CancelTask => "Cancel Task",
        RecoveryAction::MarkReviewed => "Mark Reviewed",
        RecoveryAction::RetryHook => "Retry Hook",
        RecoveryAction::ResumeProcess => "Resume Process",
        RecoveryAction::UpdateWorkspaceAndRetryHook => "Update Workspace and Retry Hook",
        RecoveryAction::SkipHookOnce => "Skip Hook Once",
        RecoveryAction::ResetRetryWindow => "Reset Retry Window",
        RecoveryAction::ProceedOnce => "Proceed Once",
        RecoveryAction::OpenInteractive => "Open Interactive",
    }
}

fn latest_failed_review(review: Option<&Review>) -> Option<&Review> {
    review.filter(|review| review.status == ReviewStatus::Failed)
}

fn workflow_role(workflow: &WorkflowDefinition, state_name: &str) -> Option<String> {
    workflow
        .states
        .iter()
        .find(|state| state.name == state_name)
        .and_then(effective_role)
        .map(str::to_owned)
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

fn failing_step_message(step: &Option<FailingStepSummary>, review_id: &str) -> String {
    match step {
        Some(step) => {
            let command = step.command.as_deref().unwrap_or("CI step");
            let exit = step
                .exit_code
                .map(|code| format!(" with exit code {code}"))
                .unwrap_or_default();
            format!("Review {review_id} failed at {command}{exit}")
        }
        None => format!("Review {review_id} failed"),
    }
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

fn task_is_terminal(workflow: &WorkflowDefinition, task: &Task) -> bool {
    workflow.state_kind(&task.status) == Some(StateKind::Terminal)
}

fn workflow_initial_state(workflow: &WorkflowDefinition) -> Option<String> {
    workflow
        .states
        .iter()
        .find(|state| state.kind == StateKind::Initial)
        .map(|state| state.name.clone())
}

fn gate_reject_target(workflow: &WorkflowDefinition, task: &Task) -> Option<String> {
    workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(|state| state.gate_config.as_ref())
        .and_then(|gate| gate.reject_target.clone())
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
