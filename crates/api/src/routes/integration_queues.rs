//! `GET /api/v1/repos/{id}/integration-queues`: the merge queues of one
//! repository, one per target branch, with their members in queue order.
use api_types::IntegrationQueuesResponse;
use axum::extract::{Path, Query, State};
use db::RepoRepo;
use serde::Deserialize;

use crate::{
    errors::{ApiError, ApiResult},
    json::Json,
    routes::{auth::AuthenticatedUser, projects::require_project_visible},
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
    // Visibility is decided before anything is counted or paged: a queue and
    // all of its members belong to the repository's one Project, and a
    // caller who cannot see that Project is told the repository is unknown.
    let missing = || ApiError::not_found("repo", id.clone());
    let repo = RepoRepo::get_by_id(&*state.db, &id)
        .await?
        .ok_or_else(missing)?;
    require_project_visible(&state, &repo.project_id, &user.user_id)
        .await
        .map_err(|_| missing())?;
    Ok(Json(
        services::integration_reads::repo_integration_queues(
            &state.db,
            &repo,
            params.cursor.as_deref(),
            db::clamp_page_limit(params.limit),
        )
        .await?,
    ))
}
