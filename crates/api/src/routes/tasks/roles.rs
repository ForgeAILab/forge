use super::*;
use crate::routes::auth::AuthenticatedUser;

pub async fn assign_task_role(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path((id, role_name)): Path<(String, String)>,
    Json(request): Json<AssignRoleRequest>,
) -> ApiResult<Json<TaskRoleAssignmentResponse>> {
    let project_id = validate_role_name(&state.db, &id, &role_name).await?;
    let assignee_id = required_body_field(request.assignee_id.clone(), "assignee_id")?;
    match request.assignee_type.as_str() {
        "agent" => {
            let usable_agents = state
                .db
                .list_agents_usable_in_project(&project_id, &user.user_id)
                .await
                .map_err(ApiError::from)?;
            let is_usable = usable_agents
                .into_iter()
                .any(|agent| agent.id == assignee_id);
            if !is_usable {
                return Err(ApiError::not_found("agent", assignee_id));
            }
        }
        "user" => {
            let member =
                db::ProjectMemberRepo::get_member(&*state.db, &project_id, &assignee_id).await?;
            if member.is_none() {
                return Err(ApiError::bad_request("assignee must be a project member"));
            }
        }
        _ => {}
    }
    let reset_workspace = request.reset_workspace.unwrap_or(false);
    let reset_worktree = request.reset_worktree.unwrap_or(false);
    let assignment = assign_role_input(id, role_name, request)?;
    let assignment = state
        .task_service
        .reassign_role(assignment, reset_workspace, reset_worktree)
        .await
        .map_err(role_reassignment_error)?;
    Ok(Json(task_role_assignment_response(assignment)))
}

pub async fn list_task_roles(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<TaskRoleAssignmentListResponse>> {
    let items = TaskRoleAssignmentRepo::list_by_task(&*state.db, &id)
        .await?
        .into_iter()
        .map(task_role_assignment_response)
        .collect();
    Ok(Json(TaskRoleAssignmentListResponse { items }))
}

pub async fn remove_task_role(
    State(state): State<AppState>,
    Path((id, role_name)): Path<(String, String)>,
    body: Option<Json<RoleResetRequest>>,
) -> ApiResult<StatusCode> {
    validate_role_name(&state.db, &id, &role_name).await?;
    let reset = body.map(|Json(request)| request).unwrap_or_default();
    state
        .task_service
        .remove_role(
            &id,
            &role_name,
            reset.reset_workspace.unwrap_or(false),
            reset.reset_worktree.unwrap_or(false),
        )
        .await
        .map_err(role_reassignment_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn role_reassignment_error(error: ServiceError) -> ApiError {
    match error {
        ServiceError::InvalidOperation { message } => ApiError::invalid_operation_conflict(message),
        other => other.into(),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct RoleResetRequest {
    reset_workspace: Option<bool>,
    reset_worktree: Option<bool>,
}

#[derive(Serialize)]
pub struct TaskRoleAssignmentListResponse {
    pub items: Vec<TaskRoleAssignmentResponse>,
}

async fn validate_role_name(
    db: &db::SqliteDb,
    task_id: &str,
    role_name: &str,
) -> ApiResult<String> {
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ApiError::not_found("task", task_id.to_owned()))?;
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", task.project_id.clone()))?;
    let workflow = WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &Actor::system(SystemComponent::General),
    );
    if !workflow.roles.iter().any(|role| role.name == role_name) {
        return Err(ApiError::bad_request(format!(
            "role '{role_name}' is not defined in workflow"
        )));
    }
    Ok(task.project_id)
}

fn assign_role_input(
    task_id: String,
    role_name: String,
    request: AssignRoleRequest,
) -> ApiResult<CreateTaskRoleAssignment> {
    let assignee_type = match request.assignee_type.as_str() {
        "agent" => {
            let assignee_id = required_body_field(request.assignee_id, "assignee_id")?;
            (db::AssigneeKind::Agent, assignee_id)
        }
        "user" => {
            let assignee_id = required_body_field(request.assignee_id, "assignee_id")?;
            (db::AssigneeKind::User, assignee_id)
        }
        _ => {
            return Err(ApiError::bad_request(
                "assignee_type must be 'agent' or 'user'",
            ));
        }
    };
    let now = now_rfc3339();
    Ok(CreateTaskRoleAssignment {
        id: db::new_uuid_v4(),
        task_id,
        role_name,
        assignee_type: Some(assignee_type.0),
        assignee_id: Some(assignee_type.1),
        created_at: now.clone(),
        updated_at: now,
    })
}

fn required_body_field(value: Option<String>, field: &'static str) -> ApiResult<String> {
    let Some(value) = value else {
        return Err(ApiError::bad_request(format!("{field} is required")));
    };
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }
    Ok(value)
}
