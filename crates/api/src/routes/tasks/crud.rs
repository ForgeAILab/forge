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
    Ok(Json(task_response(&state.db, task).await?))
}

// Bump when the derived task-list projection changes across deployments.
const TASK_LIST_PROJECTION_VERSION: u32 = 1;

pub async fn list_tasks(
    State(state): State<AppState>,
    Path(project_id): Path<String>,
    Query(params): Query<ListParams>,
    headers: axum::http::HeaderMap,
) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    use sha2::{Digest, Sha256};
    // Validate the page before considering a conditional response.
    let page_request = task_page_request(&params)?;
    let canonical_phases =
        parse_csv::<CanonicalPhase>(params.canonical_phase.as_ref(), "canonical_phase")?;
    let mut statuses = parse_csv::<String>(params.status.as_ref(), "status")?;
    let agent_ids = parse_csv::<String>(params.agent_id.as_ref(), "agent_id")?;
    let assignee_types =
        parse_csv::<db::AssigneeKind>(params.assignee_type.as_ref(), "assignee_type")?
            .into_iter()
            .map(|kind| kind.to_string())
            .collect();
    let assignee_ids = parse_csv::<String>(params.assignee_id.as_ref(), "assignee_id")?;
    let mut snapshot = state.db.begin_task_list_read(&project_id).await?;
    let board_revision = snapshot.board_revision;
    let etag = format!(
        "W/\"tasks-{}\"",
        hex::encode(Sha256::digest(serde_json::to_vec(&(
            project_id.as_str(),
            snapshot.list_revision,
            TASK_LIST_PROJECTION_VERSION,
            &params
        ))?))
    );
    let matches = headers
        .get_all(axum::http::header::IF_NONE_MATCH)
        .iter()
        .any(|value| {
            value.to_str().is_ok_and(|value| {
                value.split(',').any(|tag| {
                    let tag = tag.trim();
                    tag == "*" || tag.trim_start_matches("W/") == etag.trim_start_matches("W/")
                })
            })
        });
    if snapshot.conditional_safe && matches {
        return Ok((
            StatusCode::NOT_MODIFIED,
            [
                (axum::http::header::ETAG, etag),
                (
                    axum::http::header::CACHE_CONTROL,
                    "private, no-cache".to_owned(),
                ),
                (axum::http::header::VARY, "Authorization".to_owned()),
            ],
        )
            .into_response());
    }
    let project_workflow_definition = snapshot.workflow_definition.clone();
    if !canonical_phases.is_empty() {
        let workflow = WorkflowEngine::resolve_workflow(&project_workflow_definition);
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

    let page = snapshot
        .list(TaskListQuery {
            project_id: project_id.clone(),
            q: params.q.clone(),
            statuses: statuses.clone(),
            agent_ids,
            assignee_types,
            assignee_ids,
            priority: params.priority,
            include_archived: params.include_archived.unwrap_or(false),
            include_cancelled: params.include_cancelled.unwrap_or(false),
            include_deleted: false,
            page: page_request,
        })
        .await?;
    let has_more = page.next_cursor.is_some();
    let project_workflow = std::sync::Arc::new(WorkflowEngine::resolve_workflow(
        &project_workflow_definition,
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
                    &project_workflow_definition,
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
    let reviews = snapshot.reviews(&task_ids).await?;
    let executions = snapshot.executions(&execution_queries).await?;
    let assignments = snapshot.roles(&task_ids).await?;
    let transitions = snapshot.transitions(&retry_task_ids).await?;
    let links = snapshot.links(&task_ids).await?;
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
    let response = Json(TasksResponse {
        items,
        next_cursor: page.next_cursor,
        has_more,
        total_count: page.total_count.and_then(|count| u64::try_from(count).ok()),
        board_revision,
    });
    if snapshot.conditional_safe {
        Ok((
            [
                (axum::http::header::ETAG, etag),
                (
                    axum::http::header::CACHE_CONTROL,
                    "private, no-cache".to_owned(),
                ),
                (axum::http::header::VARY, "Authorization".to_owned()),
            ],
            response,
        )
            .into_response())
    } else {
        // Retry health changes at not_before without a write. Never return a stale 304.
        Ok((
            [
                (axum::http::header::CACHE_CONTROL, "private, no-cache"),
                (axum::http::header::VARY, "Authorization"),
            ],
            response,
        )
            .into_response())
    }
}

pub async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = TaskRepo::get_by_id(&*state.db, &id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", id))?;
    let awaiting_human = state.task_service.is_task_awaiting_human(&task).await?;
    let response = task_response_with_awaiting_human(&state.db, task, awaiting_human).await?;
    Ok(Json(response))
}

pub async fn update_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTaskRequest>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.update_task(id, request).await?;
    Ok(Json(task_response(&state.db, task).await?))
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
    Ok(Json(task_response(&state.db, task).await?))
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
        task: task_response(&state.db, result.task).await?,
        board_revision: result.board_revision,
        operation_id,
    }))
}

pub async fn archive_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.archive_task(id).await?;
    Ok(Json(task_response(&state.db, task).await?))
}

pub async fn advance_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.advance_to_next_state(id).await?;
    Ok(Json(task_response(&state.db, task).await?))
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
    Ok(Json(task_response(&state.db, task).await?))
}

pub async fn duplicate_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskResponse>> {
    let task = state.task_service.duplicate_task(&id).await?;
    Ok(Json(task_response(&state.db, task).await?))
}
