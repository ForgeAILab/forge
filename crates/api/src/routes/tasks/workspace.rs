use super::*;

pub async fn get_task_workspace(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<WorkspaceResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let workspace = WorkspaceRepo::get_by_task_id(&*state.db, &id)
        .await?
        .ok_or_else(|| ApiError::not_found("workspace", id))?;
    Ok(Json(
        workspace_response(&state.db, &state.workspace_backend_router, workspace).await?,
    ))
}

pub async fn reset_task_workspace(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<WorkspaceResponse>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Write,
    )
    .await?;
    let workspace = state.task_service.reset_task_workspace(&id).await?;
    Ok(Json(
        workspace_response(&state.db, &state.workspace_backend_router, workspace).await?,
    ))
}

pub async fn get_task_diff(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<DiffEnvelope>> {
    crate::routes::authorization::require_task_access(
        &state,
        &id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let diff = DiffService::new_with_router(
        std::sync::Arc::clone(&state.db),
        std::sync::Arc::clone(&state.workspace_backend_router),
    )
    .task_diff(&id)
    .await
    .map_err(map_diff_error)?;
    Ok(Json(DiffEnvelope { data: diff }))
}
