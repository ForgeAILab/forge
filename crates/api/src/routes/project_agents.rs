use api_types::AgentResponse;
use axum::{
    extract::{Path, State},
    Json,
};
use db::{AgentRepo, ExecutionRepo, ProjectMember, ProjectMemberRepo};
use services::agent_service::compute_effective_status;

use crate::{
    errors::{ApiError, ApiResult},
    routes::{agent_response, auth::AuthenticatedUser},
    state::AppState,
};

/// List identities that the authenticated user may select for a Project Agent
/// binding. Binding mutation itself lives at `/projects/{id}/project-agent`.
pub async fn list_project_agents(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(project_id): Path<String>,
) -> ApiResult<Json<Vec<AgentResponse>>> {
    require_project_member(&state, &project_id, &user.user_id).await?;
    let agents = state
        .db
        .list_agents_usable_in_project(&project_id, &user.user_id)
        .await
        .map_err(ApiError::from)?;

    let mut responses = Vec::with_capacity(agents.len());
    for agent in agents {
        let active_task_count = AgentRepo::count_active_tasks(&*state.db, &agent.id).await?;
        let effective_status = compute_effective_status(&state.db, &agent)
            .await?
            .as_str()
            .to_owned();
        let stats = ExecutionRepo::stats_by_agent(&*state.db, &agent.id).await?;
        responses.push(agent_response(
            agent,
            Some(active_task_count),
            Some(effective_status),
            stats,
        ));
    }

    Ok(Json(responses))
}

pub async fn require_project_member(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<ProjectMember> {
    ProjectMemberRepo::get_member(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))
}

pub async fn require_project_admin(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> ApiResult<ProjectMember> {
    let member = require_project_member(state, project_id, user_id).await?;
    if member.role != "owner" && member.role != "admin" {
        return Err(ApiError::forbidden_with_code(
            "insufficient_role",
            "project owner or admin role is required",
        ));
    }
    Ok(member)
}
