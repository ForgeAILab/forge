use super::*;

pub(super) struct TaskDiagnosticRows<'a> {
    pub role_assignments: &'a [TaskRoleAssignment],
    pub remaining_retries: &'a HashMap<String, i64>,
    pub retry_limits: &'a HashMap<String, i64>,
    pub latest_review: Option<&'a Review>,
    pub latest_execution: Option<&'a Execution>,
    pub execution_authority: &'a [Execution],
    pub running_executions: &'a [Execution],
}

pub(super) struct TaskDiagnosticProjection {
    pub(super) canonical_phase: api_types::CanonicalPhase,
    pub(super) remaining_retries: HashMap<String, i64>,
    pub(super) retry_limits: HashMap<String, i64>,
    pub(super) error_annotation: Option<TaskAnnotation>,
    pub(super) workflow_health: Option<api_types::WorkflowHealthSummary>,
    pub(super) workflow_exception: Option<api_types::WorkflowExceptionSummary>,
}

pub(super) fn task_diagnostic_projection(
    task: &Task,
    workflow: &api_types::WorkflowDefinition,
    rows: TaskDiagnosticRows<'_>,
    awaiting_human: bool,
) -> TaskDiagnosticProjection {
    let TaskDiagnosticRows {
        role_assignments: task_role_assignments,
        remaining_retries,
        retry_limits,
        latest_review,
        latest_execution,
        execution_authority,
        running_executions,
    } = rows;
    let error_annotation = task.error_annotation.as_deref().map(|s| {
        serde_json::from_str::<TaskAnnotation>(s)
            .unwrap_or_else(|_| TaskAnnotation::Legacy(parse_json_value(s)))
    });
    let canonical_phase = workflow.canonical_phase_for_state(&task.status);
    let remaining_retries = remaining_retries.clone();
    let retry_limits = retry_limits.clone();
    let current_role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(services::workflow::effective_role);
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
    let workflow_exception = services::task_diagnostics::task_exception_projection(
        task,
        workflow,
        execution_authority,
        latest_review,
        Vec::new(),
    );
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
        retry_limits,
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
    let projection = task_diagnostic_projection(&task, workflow, rows, false);
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
        retry_limits: projection.retry_limits,
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
