use super::*;

pub async fn create_task(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult<Json<TaskResponse>> {
    let request: CreateTaskRequest = serde_json::from_value(body)?;
    let review_config = match request.review_config {
        Some(review_config) => Some(review_config),
        None => project_default_review_config(&state.db, &project_id).await?,
    };
    let review_config = serialize_json(
        review_config.map(|review_config| serde_json::json!({ "review": review_config })),
    )?;
    let task_type = request.task_type.map(|t| {
        match t {
            api_types::TaskType::Task => "task",
            api_types::TaskType::PlanningTask => "planning_task",
            api_types::TaskType::SubTask => "sub_task",
            api_types::TaskType::Discovery => "discovery",
        }
        .to_owned()
    });
    let task = state
        .task_service
        .create_task_with_governance(
            project_id,
            request.title,
            request.description,
            request.parent_task_id,
            request.priority,
            task_type,
            review_config,
            request.merge_config,
            request.role_assignments,
            request.governance,
        )
        .await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn list_tasks(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<TasksResponse>> {
    let canonical_phases =
        parse_csv::<CanonicalPhase>(params.canonical_phase.as_ref(), "canonical_phase")?;
    let mut statuses = parse_csv::<String>(params.status.as_ref(), "status")?;
    let mut project_workflow_definition = None;
    if !canonical_phases.is_empty() {
        let project = ProjectRepo::get_by_id(&*state.db, &project_id)
            .await?
            .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
        project_workflow_definition = Some(project.workflow_definition.clone());
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let requested_phases: std::collections::HashSet<CanonicalPhase> =
            canonical_phases.iter().copied().collect();
        let phase_statuses = workflow
            .states
            .iter()
            .filter(|state| {
                requested_phases.contains(&workflow.canonical_phase_for_state(&state.name))
            })
            .map(|state| state.name.clone())
            .collect::<Vec<_>>();

        if statuses.is_empty() {
            // An empty status list means "no status filter" to the database layer. Use a
            // status that cannot be a workflow state when a requested phase has no states.
            statuses = if phase_statuses.is_empty() {
                vec!["__no_matching_canonical_phase__".to_owned()]
            } else {
                phase_statuses
            };
        } else {
            statuses.retain(|status| phase_statuses.iter().any(|phase| phase == status));
            if statuses.is_empty() {
                statuses = vec!["__no_matching_canonical_phase__".to_owned()];
            }
        }
        // `cancelled` is mapped to the Done phase, so phase=done intentionally opts into
        // the existing include-cancelled behavior through the resolved status list.
    }

    // Task decoration performs additional reads, so bracket the assembled page with
    // revision reads and retry if a board mutation races the response.
    for _ in 0..3 {
        let board_revision = TaskBoardRepo::board_revision(&*state.db, &project_id).await?;
        let page = TaskRepo::list(
            &*state.db,
            TaskListQuery {
                project_id: project_id.clone(),
                q: params.q.clone(),
                statuses: statuses.clone(),
                agent_ids: parse_csv::<String>(params.agent_id.as_ref(), "agent_id")?,
                assignee_types: parse_csv::<db::AssigneeKind>(
                    params.assignee_type.as_ref(),
                    "assignee_type",
                )?
                .into_iter()
                .map(|kind| kind.to_string())
                .collect(),
                assignee_ids: parse_csv::<String>(params.assignee_id.as_ref(), "assignee_id")?,
                priority: params.priority,
                include_archived: params.include_archived.unwrap_or(false),
                include_cancelled: params.include_cancelled.unwrap_or(false),
                include_deleted: false,
                page: task_page_request(&params)?,
            },
        )
        .await?;
        let has_more = page.next_cursor.is_some();
        if !page.items.is_empty() && project_workflow_definition.is_none() {
            let project = ProjectRepo::get_by_id(&*state.db, &project_id)
                .await?
                .ok_or_else(|| ApiError::not_found("project", project_id.clone()))?;
            project_workflow_definition = Some(project.workflow_definition);
        }
        let project_workflow_definition =
            project_workflow_definition.as_deref().unwrap_or_default();
        let project_workflow = std::sync::Arc::new(WorkflowEngine::resolve_workflow(
            project_workflow_definition,
        ));
        let tasks = page
            .items
            .into_iter()
            .map(|task| {
                let workflow = if task.parent_task_id.is_none() {
                    project_workflow.clone()
                } else {
                    std::sync::Arc::new(WorkflowEngine::resolve_workflow_for_task(
                        &task,
                        project_workflow_definition,
                        &Actor::system(SystemComponent::General),
                    ))
                };
                (task, workflow)
            })
            .collect::<Vec<_>>();
        let task_ids = tasks
            .iter()
            .map(|(task, _)| task.id.as_str())
            .collect::<Vec<_>>();
        let retry_task_ids = tasks
            .iter()
            .filter(|(_, workflow)| {
                workflow.states.iter().any(|state| {
                    state.kind == StateKind::Gate
                        && state
                            .gate_config
                            .as_ref()
                            .and_then(|config| config.max_rejections)
                            .is_some()
                })
            })
            .map(|(task, _)| task.id.as_str())
            .collect::<Vec<_>>();
        let execution_queries = tasks
            .iter()
            .map(|(task, workflow)| db::TaskExecutionProjectionQuery {
                task_id: task.id.clone(),
                current_role: workflow
                    .states
                    .iter()
                    .find(|state| state.name == task.status)
                    .and_then(services::workflow::effective_role)
                    .map(str::to_owned),
                blocked_execution_id: crate::routes::task_list_blocking_execution_id(task),
            })
            .collect::<Vec<_>>();
        let db = &*state.db;
        let (reviews, executions, assignments, transitions, links) = tokio::try_join!(
            ReviewRepo::list_latest_reviews_for_tasks(db, &task_ids),
            ExecutionRepo::list_task_projection_executions(db, &execution_queries),
            TaskRoleAssignmentRepo::list_by_tasks(db, &task_ids),
            TransitionLogRepo::list_by_tasks(db, &retry_task_ids),
            db::ExternalLinkRepo::list_latest_links_for_tasks(db, &task_ids),
        )?;
        let reviews = reviews
            .into_iter()
            .map(|review| (review.task_id.clone(), review))
            .collect::<std::collections::HashMap<_, _>>();
        let links = links
            .into_iter()
            .map(|link| (link.task_id.clone(), link))
            .collect::<std::collections::HashMap<_, _>>();
        let mut executions_by_task = std::collections::HashMap::<_, Vec<_>>::new();
        for execution in executions {
            executions_by_task
                .entry(execution.task_id.clone())
                .or_default()
                .push(execution);
        }
        let mut assignments_by_task = std::collections::HashMap::<_, Vec<_>>::new();
        for assignment in assignments {
            assignments_by_task
                .entry(assignment.task_id.clone())
                .or_default()
                .push(assignment);
        }
        let mut transitions_by_task = std::collections::HashMap::<_, Vec<_>>::new();
        for transition in transitions {
            transitions_by_task
                .entry(transition.task_id.clone())
                .or_default()
                .push(transition);
        }
        let items = tasks
            .into_iter()
            .map(|(task, workflow)| {
                let executions = executions_by_task.remove(&task.id).unwrap_or_default();
                let latest_execution = executions.iter().max_by(|left, right| {
                    left.created_at
                        .cmp(&right.created_at)
                        .then_with(|| left.id.cmp(&right.id))
                });
                let running_executions = executions
                    .iter()
                    .filter(|execution| execution.status == db::ExecutionStatus::Running)
                    .cloned()
                    .collect::<Vec<_>>();
                let assignments = assignments_by_task.remove(&task.id).unwrap_or_default();
                let transitions = transitions_by_task.remove(&task.id).unwrap_or_default();
                let latest_review = reviews.get(&task.id);
                let external_link = links.get(&task.id);
                crate::routes::task_projection::task_list_response(
                    task,
                    &workflow,
                    crate::routes::task_projection::TaskDiagnosticRows {
                        role_assignments: &assignments,
                        transition_logs: &transitions,
                        latest_review,
                        latest_execution,
                        execution_authority: &executions,
                        running_executions: &running_executions,
                    },
                    external_link,
                )
            })
            .collect();
        let current_revision = TaskBoardRepo::board_revision(&*state.db, &project_id).await?;
        if current_revision == board_revision {
            return Ok(Json(TasksResponse {
                items,
                next_cursor: page.next_cursor,
                has_more,
                total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
                board_revision,
            }));
        }
    }

    Err(ApiError::conflict_with_code(
        "board_snapshot_changed",
        "board changed while the task page was assembled; retry the request",
    ))
}

pub async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id))?;
    let awaiting_human = state.task_service.is_task_awaiting_human(&task).await?;
    let response = task_response_with_awaiting_human(
        &state.db,
        &state.workspace_backend_router,
        task,
        awaiting_human,
    )
    .await?;
    Ok(Json(response))
}

pub async fn update_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTaskRequest>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.update_task(id, request).await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn delete_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    let task = state.task_service.soft_delete(id).await?;
    super::media::delete_task_media_for_task(&state, &task.id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn reorder_subtasks(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(request): Json<ReorderSubtasksRequest>,
) -> ApiResult<Json<TaskResponse>> {
    state
        .task_service
        .reorder_subtasks(task_id.clone(), request.ordered_ids)
        .await?;
    let task = TaskRepo::get_by_id(&*state.db, &task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.clone()))?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn move_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<MoveTaskRequest>,
) -> ApiResult<Json<MoveTaskResponse>> {
    let operation_id = request.operation_id.clone();
    let result = state
        .task_service
        .move_task(id, request)
        .await
        .map_err(|error| match error {
            ServiceError::InvalidOperation { message } => {
                ApiError::unprocessable("invalid_transition", message)
            }
            other => ApiError::from(other),
        })?;
    Ok(Json(MoveTaskResponse {
        task: task_response(&state.db, &state.workspace_backend_router, result.task).await?,
        board_revision: result.board_revision,
        operation_id,
    }))
}

pub async fn archive_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.archive_task(id).await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn advance_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.advance_to_next_state(id).await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn recover_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<RecoverTaskRequest>,
) -> ApiResult<Json<TaskResponse>> {
    // Capacity-only refusals are accepted with the queued Task snapshot.
    let task = state
        .task_service
        .recover_task(id, body.action, body.reason, body.context)
        .await
        .map_err(|error| match &error {
            ServiceError::InvalidOperation { message } if message.contains("terminal status") => {
                ApiError::conflict_with_code("task.terminal", message.clone())
            }
            _ => ApiError::from(error),
        })?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}

pub async fn duplicate_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.duplicate_task(&id).await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, task).await?,
    ))
}
