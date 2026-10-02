use super::*;

pub async fn list_task_actions(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<api_types::TaskActionsResponse>> {
    Ok(Json(
        state
            .task_service
            .task_action_offers(&id, &Actor::user(api_types::UserActionSource::Api))
            .await?,
    ))
}

pub async fn apply_task_action(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<TaskActionRequest>,
) -> ApiResult<Json<TaskResponse>> {
    let result = state
        .task_service
        .perform_task_action(id, request.action, request.version)
        .await?;
    Ok(Json(
        task_response(&state.db, &state.workspace_backend_router, result.task).await?,
    ))
}
