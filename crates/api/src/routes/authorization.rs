//! Authorization for human REST callers, resolved through the owning Project.

use db::{ProjectMemberRepo, ProjectRepo, TaskRepo};

use crate::{
    errors::{ApiError, ApiResult},
    state::AppState,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectAccess {
    Read,
    Write,
    Admin,
    Owner,
}

/// Resolve visibility before roles so an inaccessible Project has exactly the
/// same response as an unknown Project. Instance administration does not bypass
/// the repository's existing visibility policy.
pub(crate) async fn require_project_access(
    state: &AppState,
    project_id: &str,
    user_id: &str,
    access: ProjectAccess,
) -> ApiResult<db::Project> {
    let project = ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    if access == ProjectAccess::Read {
        return Ok(project);
    }
    let member = ProjectMemberRepo::get_member(&*state.db, project_id, user_id).await?;
    let allowed = member.map_or(
        project.owner_id.as_deref() == Some(user_id),
        |member| match access {
            ProjectAccess::Read => true,
            ProjectAccess::Write => matches!(member.role.as_str(), "owner" | "admin" | "member"),
            ProjectAccess::Admin => matches!(member.role.as_str(), "owner" | "admin"),
            ProjectAccess::Owner => member.role == "owner",
        },
    );
    if !allowed {
        let message = match access {
            ProjectAccess::Read => unreachable!(),
            ProjectAccess::Write => "project member, admin or owner role is required",
            ProjectAccess::Admin => "project owner or admin role is required",
            ProjectAccess::Owner => "project owner role is required",
        };
        return Err(ApiError::forbidden_with_code("insufficient_role", message));
    }
    Ok(project)
}

pub(crate) async fn require_task_access(
    state: &AppState,
    task_id: &str,
    user_id: &str,
    access: ProjectAccess,
) -> ApiResult<db::Task> {
    let task = TaskRepo::get_by_id(&*state.db, task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.to_owned()))?;
    require_project_access(state, &task.project_id, user_id, access)
        .await
        .map_err(|error| {
            error.mask_not_found(crate::errors::ApiError::not_found(
                "task",
                task_id.to_owned(),
            ))
        })?;
    Ok(task)
}

pub(crate) async fn require_scope_access(
    state: &AppState,
    scope_type: &str,
    scope_id: &str,
    user_id: &str,
    access: ProjectAccess,
) -> ApiResult<()> {
    match scope_type {
        "project" => {
            require_project_access(state, scope_id, user_id, access).await?;
        }
        "task" => {
            require_task_access(state, scope_id, user_id, access).await?;
        }
        "agent_chat" => {
            let chat = state
                .agent_chat_service
                .get_authorized_chat(user_id, scope_id)
                .await?;
            if let Some(project_id) = chat.project_id.as_deref() {
                require_project_access(state, project_id, user_id, access).await?;
            }
        }
        "account" if scope_id == user_id => {}
        _ => return Err(ApiError::not_found("scope", scope_id.to_owned())),
    }
    Ok(())
}

pub(crate) async fn require_session_access(
    state: &AppState,
    session_id: &str,
    user_id: &str,
    access: ProjectAccess,
) -> ApiResult<()> {
    let session = db::AgentSessionRepo::get_agent_session(&*state.db, session_id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent_session", session_id.to_owned()))?;
    let identity = db::AgentRepo::get_by_id(&*state.db, &session.identity_id).await?;
    if !identity.is_some_and(|identity| identity.owner_id.as_deref() == Some(user_id)) {
        return Err(ApiError::not_found("agent_session", session_id.to_owned()));
    }
    let scope = db::AgentContextScopeRepo::get_context_scope(&*state.db, &session.context_scope_id)
        .await?
        .ok_or_else(|| ApiError::not_found("agent_session", session_id.to_owned()))?;
    require_scope_access(state, &scope.scope_type, &scope.scope_id, user_id, access)
        .await
        .map_err(|error| {
            error.mask_not_found(ApiError::not_found("agent_session", session_id.to_owned()))
        })
}

pub(crate) async fn require_agent_usable(
    state: &AppState,
    project_id: &str,
    agent_id: &str,
    user_id: &str,
) -> ApiResult<()> {
    if state
        .db
        .list_agents_usable_in_project(project_id, user_id)
        .await?
        .iter()
        .any(|agent| agent.id == agent_id)
    {
        Ok(())
    } else {
        Err(ApiError::not_found("agent", agent_id.to_owned()))
    }
}
