use super::*;

pub async fn claim_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<ClaimTaskRequest>,
) -> ApiResult<Json<TaskResponse>> {
    let assignee = Assignee::Agent(request.agent_id.clone());
    let overrides = request.overrides.map(|overrides| ExecutionOverrides {
        model_id: overrides.model_id,
        reasoning_effort: overrides.reasoning_effort,
        permission_policy: overrides.permission_policy,
        hard_deadline_seconds: overrides.hard_deadline_seconds,
    });
    let claimed = state
        .task_service
        .claim_task(id, assignee, overrides)
        .await?;
    let execution_id = claimed.execution.id.clone();
    state.task_service.start_execution(execution_id).await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, claimed.task).await?,
    ))
}

pub async fn launch_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<LaunchExecutionRequest>,
) -> ApiResult<Json<LaunchExecutionResponse>> {
    let overrides = request.overrides.map(|overrides| ExecutionOverrides {
        model_id: overrides.model_id,
        reasoning_effort: overrides.reasoning_effort,
        permission_policy: overrides.permission_policy,
        hard_deadline_seconds: overrides.hard_deadline_seconds,
    });
    let launched = state
        .task_service
        .launch_execution(id, request.agent_id, request.summary, overrides)
        .await
        .map_err(map_launch_error)?;

    let execution_id = launched.execution.id.clone();
    state.task_service.start_execution(execution_id).await?;

    let execution_behavior = Some(api_types::ExecutionBehavior {
        kind: api_types::ExecutionBehaviorKind::ManualLaunch,
        propagates: false,
        cascade_role: None,
        cascade_state: None,
        description: "Manual execution — completion will not auto-transition the task".to_owned(),
    });

    Ok(Json(LaunchExecutionResponse {
        data: api_types::LaunchExecutionData {
            task: task_response(&state.db, &state.workspace_backend_router, launched.task).await?,
            execution: execution_response(launched.execution),
            workspace: workspace_response(
                &state.db,
                &state.workspace_backend_router,
                launched.workspace,
            )
            .await?,
            execution_behavior,
        },
    }))
}
