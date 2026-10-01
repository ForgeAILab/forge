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
    Path(id): Path<String>,
) -> ApiResult<Json<DiffEnvelope>> {
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
