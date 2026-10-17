use api_types::DiffEnvelope;
use axum::{
    extract::{Path, State},
    Json,
};
use services::{DiffService, ServiceError};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

pub async fn get_workspace_diff(
    State(state): State<AppState>,
    user: crate::routes::auth::AuthenticatedUser,
    Path(id): Path<String>,
) -> ApiResult<Json<DiffEnvelope>> {
    let addressed_workspace = db::WorkspaceRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| {
            crate::errors::ApiError::not_found_with_code(
                "workspace.not_found",
                "workspace",
                id.clone(),
            )
        })?;
    crate::routes::authorization::require_task_access(
        &state,
        &addressed_workspace.task_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await
    .map_err(|error| {
        error.mask_not_found(crate::errors::ApiError::not_found_with_code(
            "workspace.not_found",
            "workspace",
            id.clone(),
        ))
    })?;

    let diff = DiffService::new_with_router(
        std::sync::Arc::clone(&state.db),
        std::sync::Arc::clone(&state.workspace_backend_router),
    )
    .workspace_diff(&id)
    .await
    .map_err(map_diff_error)?;
    Ok(Json(DiffEnvelope { data: diff }))
}

fn map_diff_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::NotFound { entity, id } if entity == "workspace" => {
            ApiError::not_found_with_code("workspace.not_found", entity, id)
        }
        ServiceError::InvalidOperation { message } if message.contains("error state") => {
            ApiError::conflict_with_code("workspace.error_state", message)
        }
        other => ApiError::from(other),
    }
}
