//! REST actions for explicit Project execution setup.

use api_types::{
    AttachPrimaryRepositoryRequest, ProjectExecutionSetupResponse, RetryProvisioningRequest,
    SelectExecutionPrincipalRequest,
};
use axum::{
    extract::{Path, State},
    Json,
};
use services::{ExecutionPrincipalRole, ProjectExecutionSetupService};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};

pub async fn get_execution_setup(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<ProjectExecutionSetupResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let response = ProjectExecutionSetupService::new(state.db.clone())
        .get(&project_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn select_worker(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<SelectExecutionPrincipalRequest>,
) -> ApiResult<Json<ProjectExecutionSetupResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let response = ProjectExecutionSetupService::new(state.db.clone())
        .select_execution_principal(
            &project_id,
            ExecutionPrincipalRole::Worker,
            &request,
            &user.user_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn select_independent_reviewer(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<SelectExecutionPrincipalRequest>,
) -> ApiResult<Json<ProjectExecutionSetupResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let response = ProjectExecutionSetupService::new(state.db.clone())
        .select_execution_principal(
            &project_id,
            ExecutionPrincipalRole::IndependentReviewer,
            &request,
            &user.user_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn attach_primary_repository(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<AttachPrimaryRepositoryRequest>,
) -> ApiResult<Json<ProjectExecutionSetupResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let response = ProjectExecutionSetupService::new(state.db.clone())
        .attach_primary_repository(&project_id, &request, &user.user_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn retry_provisioning(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
    Json(request): Json<RetryProvisioningRequest>,
) -> ApiResult<Json<ProjectExecutionSetupResponse>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Admin,
    )
    .await?;
    let response = ProjectExecutionSetupService::new(state.db.clone())
        .retry_provisioning(&project_id, &request, &user.user_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}
