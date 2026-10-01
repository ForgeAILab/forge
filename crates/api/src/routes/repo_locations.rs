use api_types::{
    CreateRepoLocationRequest, PaginatedResponse, RepoLocationResponse, UpdateRepoLocationRequest,
    VerifyRepoLocationRequest,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};

use crate::{
    errors::ApiResult,
    routes::{auth::AuthenticatedUser, page_request, paginated, ListParams},
    state::AppState,
};

pub async fn list_locations(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(repo_id): Path<String>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PaginatedResponse<RepoLocationResponse>>> {
    let page = state
        .repo_location_service
        .list(
            &repo_id,
            page_request(&params)?,
            &user.user_id,
            user.is_admin,
        )
        .await?;
    Ok(Json(paginated(page, location_response)))
}

pub async fn register_location(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(repo_id): Path<String>,
    Json(request): Json<CreateRepoLocationRequest>,
) -> ApiResult<Json<RepoLocationResponse>> {
    let location = state
        .repo_location_service
        .register(&repo_id, request, &user.user_id, user.is_admin)
        .await?;
    Ok(Json(location_response(location)))
}

pub async fn update_location(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((repo_id, location_id)): Path<(String, String)>,
    Json(request): Json<UpdateRepoLocationRequest>,
) -> ApiResult<Json<RepoLocationResponse>> {
    let location = state
        .repo_location_service
        .update(
            &repo_id,
            &location_id,
            request,
            &user.user_id,
            user.is_admin,
        )
        .await?;
    Ok(Json(location_response(location)))
}

pub async fn verify_location(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((repo_id, location_id)): Path<(String, String)>,
    Json(request): Json<VerifyRepoLocationRequest>,
) -> ApiResult<Json<RepoLocationResponse>> {
    let location = state
        .repo_location_service
        .verify(
            &repo_id,
            &location_id,
            request.version,
            &user.user_id,
            user.is_admin,
        )
        .await?;
    Ok(Json(location_response(location)))
}

pub async fn remove_location(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((repo_id, location_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    state
        .repo_location_service
        .remove(&repo_id, &location_id, &user.user_id, user.is_admin)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn location_response(location: db::RepoLocation) -> RepoLocationResponse {
    RepoLocationResponse {
        id: location.id,
        repo_id: location.repo_id,
        owner_kind: match location.owner_kind {
            db::RepoLocationOwnerKind::Server => api_types::RepoLocationOwnerKind::Server,
            db::RepoLocationOwnerKind::Daemon => api_types::RepoLocationOwnerKind::Daemon,
        },
        daemon_id: location.daemon_id,
        runtime_id: location.runtime_id,
        path: location.path,
        kind: match location.kind {
            db::RepoLocationKind::PrimaryCheckout => api_types::RepoLocationKind::PrimaryCheckout,
            db::RepoLocationKind::ManagedClone => api_types::RepoLocationKind::ManagedClone,
            db::RepoLocationKind::SharedMount => api_types::RepoLocationKind::SharedMount,
        },
        is_default: location.is_default,
        status: match location.status {
            db::RepoLocationStatus::Unverified => api_types::RepoLocationStatus::Unverified,
            db::RepoLocationStatus::Ready => api_types::RepoLocationStatus::Ready,
            db::RepoLocationStatus::Unavailable => api_types::RepoLocationStatus::Unavailable,
            db::RepoLocationStatus::Invalid => api_types::RepoLocationStatus::Invalid,
        },
        last_verified_at: location.last_verified_at,
        last_error: location.last_error,
        version: location.version,
        created_at: location.created_at,
        updated_at: location.updated_at,
    }
}
