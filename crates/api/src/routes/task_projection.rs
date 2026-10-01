use super::*;

pub(super) struct TaskDiagnosticRows<'a> {
    pub role_assignments: &'a [TaskRoleAssignment],
    pub transition_logs: &'a [db::TransitionLog],
    pub latest_review: Option<&'a Review>,
    pub latest_execution: Option<&'a Execution>,
    pub execution_authority: &'a [Execution],
    pub running_executions: &'a [Execution],
}

pub(super) struct TaskDiagnosticProjection {
    pub(super) canonical_phase: api_types::CanonicalPhase,
    pub(super) remaining_retries: HashMap<String, i64>,
    pub(super) execution_actions: Vec<api_types::ExecutionAction>,
    pub(super) error_annotation: Option<TaskAnnotation>,
    pub(super) workflow_health: Option<api_types::WorkflowHealthSummary>,
    pub(super) workflow_exception: Option<api_types::WorkflowExceptionSummary>,
}

pub(super) fn task_diagnostic_projection(
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
    rows: TaskDiagnosticRows<'_>,
    include_actions: bool,
    awaiting_human: bool,
) -> TaskDiagnosticProjection {
    let TaskDiagnosticRows {
        role_assignments: task_role_assignments,
        transition_logs,
        latest_review,
        latest_execution,
        execution_authority,
        running_executions,
    } = rows;
    let error_annotation = task.error_annotation.as_deref().map(|s| {
        serde_json::from_str::<TaskAnnotation>(s)
            .unwrap_or_else(|_| TaskAnnotation::Legacy(parse_json_value(s)))
    });
    let blocked_metadata_annotation = blocked_metadata_annotation(task);
    let error_blocking_annotation = match error_annotation.as_ref() {
        Some(TaskAnnotation::Blocking(annotation)) => Some(annotation),
        _ => None,
    };
    // A populated typed annotation is the current recovery contract. Legacy
    // blocked metadata only fills the gap for old/empty annotations; it must
    // never override or widen the explicit action set used by the recovery
    // service and workflow-exception projection.
    let blocking_annotation = blocking_annotation_for_projection(
        task,
        blocked_metadata_annotation.as_ref(),
        error_blocking_annotation,
    );
    let canonical_phase = workflow.canonical_phase_for_state(&task.status);
    let mut remaining_retries = HashMap::new();
    for state in &workflow.states {
        if state.kind != StateKind::Gate {
            continue;
        }
        let Some(max_rejections) = state
            .gate_config
            .as_ref()
            .and_then(|config| config.max_rejections)
        else {
            continue;
        };
        let count = count_gate_rejections_since_boundary(transition_logs, &state.name);
        let exhausted = blocking_annotation.is_some_and(|annotation| {
            retry_budget_exhausted_for_state(&task.status, state, annotation)
        });
        remaining_retries.insert(
            state.name.clone(),
            if exhausted {
                0
            } else {
                (i64::from(max_rejections) - count).max(0)
            },
        );
    }
    let current_role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(services::workflow::effective_role);
    let blocked_execution_id =
        blocking_annotation.and_then(|annotation| annotation.blocked_execution_id.as_deref());
    let open_interactive_target =
        select_open_interactive_target(execution_authority, current_role, blocked_execution_id);
    let open_interactive_launch_authority = has_open_interactive_launch_authority(
        execution_authority,
        task_role_assignments,
        current_role,
        blocked_execution_id,
    );
    let execution_actions = if include_actions {
        resolve_execution_actions(
            task,
            workflow,
            execution_authority,
            blocking_annotation,
            latest_review,
        )
    } else {
        Vec::new()
    };
    let running_interactive_execution = running_executions
        .iter()
        .filter(|execution| execution.role == "interactive")
        .max_by(|left, right| compare_running_execution_authority(left, right))
        .cloned();
    let running_current_role_execution = current_role.and_then(|role| {
        running_executions
            .iter()
            .filter(|execution| {
                execution.role == role
                    || (role == services::workflow::default_roles::CODER
                        && execution.role == "executor")
            })
            .max_by(|left, right| compare_running_execution_authority(left, right))
            .cloned()
    });
    let active_execution = running_executions
        .iter()
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        })
        .cloned();
    let health_execution = running_interactive_execution
        .clone()
        .or(running_current_role_execution)
        .or(active_execution)
        .or_else(|| latest_execution.cloned());
    let running_role_execution = running_executions
        .iter()
        .filter(|execution| execution.role != "interactive")
        .max_by(|left, right| compare_running_execution_authority(left, right));
    let workflow_exception = derive_workflow_exception_with_running_interactive(
        task,
        workflow,
        task_role_assignments,
        latest_review,
        latest_execution,
        running_interactive_execution.as_ref(),
        open_interactive_target,
        open_interactive_launch_authority,
        &remaining_retries,
    )
    .map(|exception| disable_recovery_while_running(exception, running_role_execution));
    let workflow_health = Some(derive_workflow_health(
        task,
        workflow,
        task_role_assignments,
        latest_review,
        health_execution.as_ref(),
        awaiting_human,
        workflow_exception.as_ref(),
    ));
    TaskDiagnosticProjection {
        canonical_phase,
        remaining_retries,
        execution_actions,
        error_annotation,
        workflow_health,
        workflow_exception,
    }
}

pub(super) fn task_list_response(
    task: Task,
    workflow: &api_types::WorkflowDefinition,
    rows: TaskDiagnosticRows<'_>,
    external_link: Option<&db::TaskExternalLink>,
) -> api_types::TaskListItemResponse {
    let role_assignments = rows
        .role_assignments
        .iter()
        .cloned()
        .map(task_role_assignment_response)
        .collect();
    let latest_execution_id = rows.latest_execution.map(|execution| execution.id.clone());
    let projection = task_diagnostic_projection(&task, workflow, rows, false, false);
    api_types::TaskListItemResponse {
        id: task.id,
        project_id: task.project_id,
        parent_task_id: task.parent_task_id,
        assignee_type: task.assignee_type,
        assignee_id: task.assignee_id,
        title: task.title,
        task_type: parse_task_type(&task.task_type),
        status: task.status,
        canonical_phase: projection.canonical_phase,
        awaiting_human: false,
        priority: task.priority,
        board_position: task.board_position,
        subtask_order: task.subtask_order,
        role_assignments,
        remaining_retries: projection.remaining_retries,
        error_annotation: projection.error_annotation,
        blocked: task
            .blocked_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        failed: task
            .failed_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok()),
        workflow_health: projection.workflow_health,
        workflow_exception: projection.workflow_exception,
        review_passed_at: task.review_passed_at,
        archived_at: task.archived_at,
        external_issue_number: external_link.map(|link| link.remote_issue_number),
        external_issue_url: external_link.map(|link| link.remote_url.clone()),
        execution_observability: api_types::TaskListExecutionObservability {
            latest_execution_id,
        },
        version: task.version,
        created_at: task.created_at,
        updated_at: task.updated_at,
    }
}
