//! `GET /api/v1/repos/{id}/integration-queues`: the merge queues of one
//! repository, one per target branch, with their members in queue order.
use api_types::IntegrationQueuesResponse;
use axum::extract::{Path, Query, State};
use db::{ProjectRepo, RepoRepo};
use serde::Deserialize;

use crate::{
    errors::{ApiError, ApiResult},
    json::Json,
    routes::auth::AuthenticatedUser,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct IntegrationQueueParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

pub async fn list_integration_queues(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(id): Path<String>,
    Query(params): Query<IntegrationQueueParams>,
) -> ApiResult<Json<IntegrationQueuesResponse>> {
    let addressed_repo = db::RepoRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(|| crate::errors::ApiError::not_found("repo", id.clone()))?;
    crate::routes::authorization::require_project_access(
        &state,
        &addressed_repo.project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await
    .map_err(|error| {
        error.mask_not_found(crate::errors::ApiError::not_found("repo", id.clone()))
    })?;

    // Visibility is decided before anything is counted or paged. A caller
    // who cannot see the repository's Project is told the repository is
    // unknown, exactly like a repository that does not exist; any other
    // failure of the check is that failure, not a 404.
    let missing = || ApiError::not_found("repo", id.clone());
    let repo = RepoRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(missing)?;
    ProjectRepo::get_visible_by_id(&*state.db, &repo.project_id, &user.user_id)
        .await?
        .ok_or_else(missing)?;
    Ok(Json(
        services::integration_reads::repo_integration_queues(
            &state.db,
            &repo.id,
            &repo.project_id,
            None,
            params.cursor.as_deref(),
            db::clamp_page_limit(params.limit),
        )
        .await?,
    ))
}
