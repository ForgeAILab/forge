use crate::{errors::ApiResult, routes::auth::AuthenticatedUser, state::AppState};
use api_types::{AnswerProjectEscalationRequest, ProjectEscalationResponse};
use axum::{
    extract::{Path, State},
    Json,
};
pub async fn get(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, id)): Path<(String, String)>,
) -> ApiResult<Json<ProjectEscalationResponse>> {
    Ok(Json(
        services::project_escalation::ProjectEscalationService::new(state.db)
            .get_for_owner(&project_id, &id, &user.user_id)
            .await?,
    ))
}
pub async fn answer(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((project_id, id)): Path<(String, String)>,
    Json(request): Json<AnswerProjectEscalationRequest>,
) -> ApiResult<Json<ProjectEscalationResponse>> {
    Ok(Json(
        services::project_escalation::ProjectEscalationService::new(state.db)
            .answer(&project_id, &id, &user.user_id, request)
            .await?,
    ))
}
