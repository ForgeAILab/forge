use api_types::AgentResponse;
use axum::{
    extract::{Path, State},
    Json,
};

use crate::{
    errors::{ApiError, ApiResult},
    routes::{agents::build_agent_responses_for_user, auth::AuthenticatedUser},
    state::AppState,
};

/// List identities that the authenticated user may select for a Project Agent
/// binding. Binding mutation itself lives at `/projects/{id}/project-agent`.
pub async fn list_project_agents(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<Vec<AgentResponse>>> {
    crate::routes::authorization::require_project_access(
        &state,
        &project_id,
        &user.user_id,
        crate::routes::authorization::ProjectAccess::Read,
    )
    .await?;
    let agents = state
        .db
        .list_agents_usable_in_project(&project_id, &user.user_id)
        .await
        .map_err(ApiError::from)?;

    let responses = build_agent_responses_for_user(&state, agents, &user).await?;

    Ok(Json(responses))
}
