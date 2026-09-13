use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentActionAuthority {
    id: String,
    executor_type: String,
    credential_ref: Option<String>,
    is_default: bool,
    paused: bool,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionActionAuthority {
    id: String,
    role: String,
    status: db::ExecutionStatus,
    agent_id: Option<String>,
    agent_session_id: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReviewActionAuthority {
    id: String,
    attempt_number: i64,
    status: db::ReviewStatus,
}

#[derive(Debug, Clone, PartialEq)]
struct TaskActionsAuthoritySnapshot {
    task: db::Task,
    project: db::Project,
    executions: Vec<ExecutionActionAuthority>,
    reviews: Vec<ReviewActionAuthority>,
    agents: Vec<AgentActionAuthority>,
    role_assignments: Vec<(String, Option<db::AssigneeKind>, Option<String>)>,
    unsatisfied_dependencies: Vec<String>,
    latest_state_entry: Option<services::task_service::action_resolver::LatestStateEntryAuthority>,
}

async fn task_actions_authority_snapshot(
    db: &db::SqliteDb,
    task_id: &str,
) -> ApiResult<TaskActionsAuthoritySnapshot> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.to_owned()))?;
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", task.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &Actor::user(api_types::UserActionSource::Api),
    );
    let error_annotation = task
        .error_annotation
        .as_deref()
        .and_then(|raw| serde_json::from_str::<api_types::TaskAnnotation>(raw).ok());
    let blocked_metadata = crate::routes::blocked_metadata_annotation(&task);
    let error_blocking = error_annotation
        .as_ref()
        .and_then(|annotation| match annotation {
            api_types::TaskAnnotation::Blocking(annotation) => Some(annotation),
            api_types::TaskAnnotation::Legacy(_) => None,
        });
    let blocking_annotation = crate::routes::blocking_annotation_for_projection(
        &task,
        blocked_metadata.as_ref(),
        error_blocking,
    );
    let current_role = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(services::workflow::effective_role);
    let blocked_execution_id =
        blocking_annotation.and_then(|annotation| annotation.blocked_execution_id.as_deref());
    let executions = services::task_service::action_resolver::list_execution_action_authority(
        db,
        &task.id,
        current_role,
        blocked_execution_id,
    )
    .await?
    .into_iter()
    .map(|execution| ExecutionActionAuthority {
        id: execution.id,
        role: execution.role,
        status: execution.status,
        agent_id: execution.agent_id,
        agent_session_id: execution.agent_session_id,
        created_at: execution.created_at,
    })
    .collect();
    let latest_state_entry = if current_role.is_some() {
        services::task_service::action_resolver::latest_state_entry_authority(
            db,
            &task.id,
            &task.status,
        )
        .await?
    } else {
        None
    };
    let task_ids = [task.id.as_str()];
    let reviews = ReviewRepo::list_latest_reviews_for_tasks(db, &task_ids)
        .await?
        .into_iter()
        .map(|review| ReviewActionAuthority {
            id: review.id,
            attempt_number: review.attempt_number,
            status: review.status,
        })
        .collect();
    let agents = db::AgentRepo::list(
        db,
        db::AgentListQuery {
            status: None,
            executor_type: None,
            capabilities: Vec::new(),
            page: db::PageRequest {
                cursor: None,
                limit: 500,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        },
    )
    .await?
    .items
    .into_iter()
    .map(|agent| AgentActionAuthority {
        id: agent.id,
        executor_type: agent.executor_type,
        credential_ref: agent.credential_ref,
        is_default: agent.is_default,
        paused: agent.paused,
        created_at: agent.created_at,
    })
    .collect();
    let role_assignments = TaskRoleAssignmentRepo::list_by_task(db, &task.id)
        .await?
        .into_iter()
        .map(|assignment| {
            (
                assignment.role_name,
                assignment.assignee_type,
                assignment.assignee_id,
            )
        })
        .collect();
    let mut unsatisfied_dependencies =
        db::TaskDependencyRepo::unsatisfied_dependencies(db, &task.id).await?;
    // The dependency repository only promises membership. Keep the authority
    // fingerprint stable when SQLite returns the same rows in another scan
    // order between the before/after reads.
    unsatisfied_dependencies.sort();

    Ok(TaskActionsAuthoritySnapshot {
        task,
        project,
        executions,
        reviews,
        agents,
        role_assignments,
        unsatisfied_dependencies,
        latest_state_entry,
    })
}

pub async fn list_task_actions(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<api_types::TaskActionsResponse>> {
    // Both service methods intentionally re-read the Task because they are
    // also used independently by mutation routes. A Task or blocker-metadata
    // mutation between those reads must not produce an action payload
    // assembled from two authority snapshots. Retry a small number of times,
    // then make the race explicit to the client instead of returning a
    // mixed-state success.
    const SNAPSHOT_ATTEMPTS: usize = 3;
    for _ in 0..SNAPSHOT_ATTEMPTS {
        let before = task_actions_authority_snapshot(&state.db, &id).await?;
        let available_actions = state
            .task_service
            .available_task_actions(id.clone())
            .await?;
        let recovery_actions = state
            .task_service
            .available_recovery_actions(id.clone())
            .await?;
        let after = task_actions_authority_snapshot(&state.db, &id).await?;
        if before == after {
            return Ok(Json(api_types::TaskActionsResponse {
                available_actions,
                recovery_actions,
            }));
        }
    }

    Err(ApiError::conflict_with_code(
        "task.actions_changed",
        "task changed while action availability was being read; retry",
    ))
}

pub async fn start_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Start, body).await
}

pub async fn pause_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Pause, body).await
}

pub async fn resume_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Resume, body).await
}

pub async fn submit_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Submit, body).await
}

pub async fn request_changes_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::RequestChanges, body).await
}

pub async fn approve_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Approve, body).await
}

pub async fn cancel_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    execute(&state, id, TaskAction::Cancel, body).await
}

async fn execute(
    state: &AppState,
    id: String,
    action: TaskAction,
    body: Option<Json<Option<TaskActionRequest>>>,
) -> ApiResult<Json<TaskResponse>> {
    let request = body.and_then(|body| body.0).unwrap_or_default();
    let result = state
        .task_service
        .perform_task_action(id, action, request.reason, request.version)
        .await?;
    Ok(Json(task_response(&state.db, result.task).await?))
}
