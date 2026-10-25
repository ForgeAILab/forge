mod agent_chat;
mod memory;

pub(super) use agent_chat::{
    forge_create_agent_handoff, forge_get_agent_chat, forge_get_agent_handoff,
    forge_get_agent_session, forge_get_main_agent, forge_get_project_agent,
    forge_list_agent_chat_messages, forge_list_agent_chats, forge_list_agent_handoffs,
    forge_list_agent_profiles, forge_list_agent_sessions, forge_send_agent_chat_message,
    forge_set_main_agent, forge_set_project_agent,
};
pub(super) use memory::{forge_memory_get, forge_memory_search};

#[cfg(test)]
mod tests;

use std::{collections::HashMap, sync::Arc};

use api_types::{Actor, SystemComponent};
use api_types::{
    AgentBindingState, AgentChatDetailResponse, AgentChatKind, AgentChatListResponse,
    AgentChatMessageAuthorType, AgentChatMessageListResponse, AgentChatMessageResponse,
    AgentChatMessageStatus, AgentChatResponse as ApiAgentChatResponse, AgentChatStatus,
    AgentChatTurnJobResponse, AgentChatTurnStatus, AgentHandoffResponse, AgentHandoffStatus,
    MainAgentBindingResponse, ProjectAgentBindingResponse,
};
use db::{
    new_uuid_v4, now_rfc3339, AccountMainAgentBinding, AccountMainAgentBindingRepo, AgentChat,
    AgentChatMessage, AgentChatMessageAuthorType as DbMessageAuthorType, AgentChatMessageListQuery,
    AgentChatMessageRepo, AgentChatMessageStatus as DbMessageStatus, AgentChatRepo,
    AgentChatTurnJob, AgentChatTurnState, AgentHandoff, AgentHandoffRepo,
    AgentHandoffStatus as DbHandoffStatus, AgentListQuery, AgentProfileRepo, AgentRepo,
    AgentSessionRepo, CreateAccountMainAgentBinding, CreateProject, ExecutionRepo,
    MemoryScopeGrant, PageRequest, ProjectAgentBinding, ProjectAgentBindingRepo, ProjectRepo,
    ReplaceAccountMainAgentBinding, SortBy, SortOrder, TaskDependencyRepo, TaskListQuery, TaskRepo,
    TaskRoleAssignmentRepo, UpdateProject, UserRepo,
};
use executors::ExecutionOverrides;
use operation_registry::mcp::{
    AddTaskDependencyParams, AssignAgentParams, BindMainAgentParams, BindProjectAgentParams,
    CreateAgentHandoffParams, CreateProjectParams, CreateSubTasksParams, CreateTaskParams,
    FollowUpExecutionParams, GetAgentChatParams, GetAgentHandoffParams, GetAgentSessionParams,
    GetMainAgentParams, GetProjectAgentParams, GetProjectParams, GetTaskParams,
    ListAgentChatMessagesParams, ListAgentChatsParams, ListAgentHandoffsParams,
    ListAgentProfilesParams, ListAgentSessionsParams, ListAgentsParams, ListExecutionsParams,
    ListProjectsParams, ListSubTasksParams, ListTaskDependenciesParams, ListTasksParams,
    MemoryGetParams, MemorySearchParams, PreviewPromptParams, ProjectEscalateParams,
    RegisterAgentParams, RemoveTaskDependencyParams, ReorderSubTasksParams,
    SendAgentChatMessageParams, TaskActionParams, TransitionTaskParams,
    UpdateProjectLifecycleHooksParams, UpdateProjectParams, UpdateTaskParams,
};
use serde_json::{json, Value};
use services::{
    workflow::engine::WorkflowEngine, DiffService, MemoryAccessContext, MemorySearchResult,
};
use services_agents::SetProjectAgentBindingInput;
use uuid::Uuid;

use crate::{
    error::McpToolError,
    params::{page_request, task_page_request},
    protocol::McpContext,
    state::AppState,
    values::{
        agent_page_value_for_user, agent_profile_value, agent_session_value, agent_value_for_user,
        execution_page_value, execution_value, project_page_value, project_value,
        task_role_assignment_value, task_value,
    },
};

pub(super) async fn forge_create_task(
    state: &AppState,
    params: CreateTaskParams,
) -> Result<Value, McpToolError> {
    if params.project_id.trim().is_empty() {
        return Err(invalid_field_error(
            "project_id",
            "must be a non-empty string",
            Some(json!({
                "type": "string",
                "non_empty": true
            })),
        ));
    }
    if params.title.trim().is_empty() {
        return Err(invalid_field_error(
            "title",
            "must be a non-empty string",
            Some(json!({
                "type": "string",
                "non_empty": true
            })),
        ));
    }
    if let Some(parent_task_id) = params.parent_task_id.as_deref() {
        if parent_task_id.trim().is_empty() {
            return Err(invalid_field_error(
                "parent_task_id",
                "must be a non-empty string when provided",
                Some(json!({
                    "type": "string",
                    "non_empty": true
                })),
            ));
        }
    }
    if params
        .parent_task_id
        .as_ref()
        .is_some_and(|parent| params.depends_on_ids.contains(parent))
    {
        return Err(invalid_field_error(
            "depends_on_ids",
            "must not contain parent_task_id; parentage shares a workspace, dependencies gate execution",
            Some(
                json!({"type":"array","items":{"type":"string"},"uniqueItems":true,"excludes":"parent_task_id"}),
            ),
        ));
    }
    if ProjectRepo::get_by_id(&*state.db, &params.project_id)
        .await?
        .is_none()
    {
        return Err(invalid_field_error(
            "project_id",
            "must reference an existing project",
            Some(json!({
                "type": "string",
                "constraint": "existing project id"
            })),
        ));
    }
    let dependency_ids = params.depends_on_ids.clone();
    let task = state
        .task_service
        .create_task_with_dependencies(
            params.project_id,
            params.title,
            params.description,
            params.parent_task_id,
            params.priority,
            params.task_type,
            None,
            None,
            None,
            params.depends_on_ids,
        )
        .await
        .map_err(|error| match error {
            services::ServiceError::NotFound { entity: "task", id }
                if dependency_ids.contains(&id) =>
            {
                invalid_field_error(
                    "depends_on_ids",
                    format!("prerequisite task not found: {id}"),
                    Some(json!({
                        "type": "array",
                        "items": { "type": "string" },
                        "constraint": "existing task ids in the same project"
                    })),
                )
            }
            services::ServiceError::NotFound { entity: "task", id } => invalid_field_error(
                "parent_task_id",
                format!("parent task not found: {id}"),
                Some(json!({
                    "type": "string",
                    "constraint": "existing root task id in the same project"
                })),
            ),
            other => other.into(),
        })?;
    let mut result = task_with_offers(state, task).await?;
    if let Some(object) = result.as_object_mut() {
        object.insert("depends_on_ids".to_owned(), json!(dependency_ids));
    }
    Ok(result)
}

pub(super) async fn forge_create_sub_tasks(
    state: &AppState,
    params: CreateSubTasksParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    for subtask in &params.subtasks {
        if let Some(agent_id) = &subtask.assignee_id {
            super::registry::require_task_agent(state, &params.parent_task_id, agent_id, context)
                .await?;
        }
    }
    let inputs = params
        .subtasks
        .into_iter()
        .map(|s| services::NewSubtaskInput {
            title: s.title,
            description: s.description,
            assignee_id: s.assignee_id,
        })
        .collect::<Vec<_>>();
    let tasks = state
        .task_service
        .create_subtasks(params.parent_task_id, inputs)
        .await?;
    Ok(serde_json::json!({
        "subtasks": tasks_with_role_assignments(state, tasks).await?,
    }))
}

async fn tasks_with_role_assignments(
    state: &AppState,
    tasks: Vec<db::Task>,
) -> Result<Vec<Value>, McpToolError> {
    let ids = tasks
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    let mut assignments_by_task = HashMap::<String, Vec<db::TaskRoleAssignment>>::new();
    for assignment in TaskRoleAssignmentRepo::list_by_tasks(&*state.db, &ids).await? {
        assignments_by_task
            .entry(assignment.task_id.clone())
            .or_default()
            .push(assignment);
    }
    let mut budget_spent = db::budget::load_many(state.db.pool(), &ids).await?;
    // The merge-queue attempts of the page, in one statement.
    let integration = db::integration_members_of_tasks(state.db.pool(), &ids).await?;
    let integration = integration
        .iter()
        .map(|row| (row.task_ref.as_str(), row))
        .collect::<HashMap<_, _>>();
    let mut projects = HashMap::<String, db::Project>::new();
    let mut values = Vec::with_capacity(tasks.len());
    for task in tasks {
        if !projects.contains_key(&task.project_id) {
            let project = ProjectRepo::get_by_id(&*state.db, &task.project_id)
                .await?
                .ok_or(db::DbError::NotFound)?;
            projects.insert(task.project_id.clone(), project);
        }
        let project = &projects[&task.project_id];
        let workflow = services::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &Actor::system(api_types::SystemComponent::General),
        );
        let workflow = db::budget::with_project_defaults(&workflow, &project.settings);
        let recovery_limit = db::budget::recovery_limit(&project.settings);
        let counts = budget_spent.remove(&task.id).unwrap_or_default();
        let remaining = db::budget::projection(&task, &workflow, &counts, recovery_limit)?;
        let limits = db::budget::projection(&task, &workflow, &Default::default(), recovery_limit)?;
        let assignments = assignments_by_task.remove(&task.id).unwrap_or_default();
        let summary = integration.get(task.id.as_str()).and_then(|row| {
            services::integration_reads::task_integration_summary(row, Some(&task))
        });
        let mut value = task_value(task);
        value["integration"] = json!(summary);
        value["remaining_retries"] = json!(remaining);
        value["retry_limits"] = json!(limits);
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "role_assignments".to_owned(),
                Value::Array(
                    assignments
                        .into_iter()
                        .map(task_role_assignment_value)
                        .collect(),
                ),
            );
        }
        values.push(value);
    }
    Ok(values)
}

fn invalid_field_error(
    field: &'static str,
    message: impl Into<String>,
    accepted: Option<Value>,
) -> McpToolError {
    let mut data = json!({
        "field": field,
        "details": message.into(),
    });
    if let Some(accepted) = accepted {
        if let Some(object) = data.as_object_mut() {
            object.insert("accepted".to_owned(), accepted);
        }
    }
    McpToolError::new(-32602, "invalid params").with_data(data)
}

pub(super) async fn forge_list_tasks(
    state: &AppState,
    params: ListTasksParams,
) -> Result<Value, McpToolError> {
    let page = TaskRepo::list(
        &*state.db,
        TaskListQuery {
            project_id: params.project_id,
            q: None,
            statuses: params.status.into_vec(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: task_page_request(params.cursor, params.limit, params.sort_by)?,
        },
    )
    .await?;
    let has_more = page.next_cursor.is_some();
    let items = tasks_with_role_assignments(state, page.items).await?;
    Ok(
        json!({ "items": items, "next_cursor": page.next_cursor, "has_more": has_more, "total_count": page.total_count }),
    )
}

pub(super) async fn forge_get_task(
    state: &AppState,
    params: GetTaskParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let actor = Actor::delegated_user(
        authenticated_user(context)?,
        api_types::UserActionSource::Api,
    );
    let snapshot = state
        .task_service
        .task_action_snapshot(&params.task_id, &actor)
        .await?;
    let integration =
        services::integration_reads::task_integration(&state.db, &snapshot.task).await?;
    let mut value = task_snapshot_value(snapshot)?;
    value["integration"] = json!(integration);
    Ok(value)
}

fn task_snapshot_value(snapshot: services::TaskSnapshot) -> Result<Value, McpToolError> {
    let offers = services::available_actions(&snapshot);
    let exception = services::task_diagnostics::task_exception(&snapshot, offers.clone());
    let remaining = db::budget::projection(
        &snapshot.task,
        &snapshot.workflow,
        &snapshot.budget_spent,
        snapshot.recovery_budget_limit,
    )?;
    let limits = db::budget::projection(
        &snapshot.task,
        &snapshot.workflow,
        &Default::default(),
        snapshot.recovery_budget_limit,
    )?;
    let mut value = task_value(snapshot.task);
    value["remaining_retries"] = json!(remaining);
    value["retry_limits"] = json!(limits);
    value["available_actions"] = json!(offers);
    value["workflow_exception"] = serde_json::to_value(exception)
        .map_err(|error| McpToolError::new(-32603, error.to_string()))?;
    Ok(value)
}

async fn task_with_offers(state: &AppState, task: db::Task) -> Result<Value, McpToolError> {
    let snapshot = state
        .task_service
        .task_action_snapshot(&task.id, &Actor::user(api_types::UserActionSource::Api))
        .await?;
    task_snapshot_value(snapshot)
}

pub(super) async fn forge_preview_prompt(
    state: &AppState,
    params: PreviewPromptParams,
) -> Result<Value, McpToolError> {
    if params.role.trim().is_empty() {
        return Err(invalid_field_error(
            "role",
            "must be a non-empty string",
            Some(json!({
                "type": "string",
                "non_empty": true
            })),
        ));
    }

    let prompt = services_agents::preview_effective_prompt(
        Arc::clone(&state.db),
        &state.workspace_backend_router,
        &params.task_id,
        params.role.trim(),
        params.trigger.map(|trigger| match trigger {
            operation_registry::mcp::WorkflowTrigger::Accept => api_types::WorkflowTrigger::Accept,
            operation_registry::mcp::WorkflowTrigger::Reject => api_types::WorkflowTrigger::Reject,
            operation_registry::mcp::WorkflowTrigger::Fail => api_types::WorkflowTrigger::Fail,
            operation_registry::mcp::WorkflowTrigger::Retry => api_types::WorkflowTrigger::Retry,
        }),
    )
    .await?;

    Ok(json!({
        "system": prompt.system,
        "user": prompt.user,
        "tools": non_empty_tools(prompt.tools),
    }))
}

pub(super) async fn forge_assign_agent(
    state: &AppState,
    params: AssignAgentParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    super::registry::require_task_agent(state, &params.task_id, &params.agent_id, context).await?;
    let assignment = state
        .task_service
        .assign_agent_to_task(&params.task_id, &params.agent_id)
        .await?;
    Ok(json!({
        "assignment": task_role_assignment_value(assignment),
        "execution_started": false,
    }))
}

pub(super) async fn task_action(
    state: &AppState,
    params: TaskActionParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    // MCP credentials (PATs, sessions, OAuth tokens) all resolve to the user;
    // none is agent-scoped, so an MCP-hosted Project Agent is indistinguishable
    // from the owner. MCP actions keep owner action authority but spend budgets.
    let actor = Actor::delegated_user(
        authenticated_user(context)?,
        api_types::UserActionSource::Api,
    );
    let task_id = &params.task_id;
    let action = task_action_input(params.action);
    let unblocking = services::project_escalation::is_unblocking_verb(action.verb());
    let result = state
        .task_service
        .perform_task_action_as(task_id, action, params.version, actor.clone())
        .await?;
    if unblocking {
        // An MCP Project Agent's recovery during its blocker turn is an outcome.
        if let Some(identity) =
            ProjectAgentBindingRepo::get_active_project_binding(&*state.db, &result.task.project_id)
                .await?
                .filter(|b| b.state == "active")
                .and_then(|b| b.identity_id)
        {
            services::project_escalation::ProjectEscalationService::new(state.db.clone())
                .record_unblocking_action(&result.task.project_id, &identity, &result.task.id)
                .await?;
        }
    }
    let snapshot = state
        .task_service
        .task_action_snapshot(&result.task.id, &actor)
        .await?;
    task_snapshot_value(snapshot)
}

pub(super) async fn forge_get_task_diff(
    state: &AppState,
    params: GetTaskParams,
) -> Result<Value, McpToolError> {
    let diff = DiffService::new_with_router(
        std::sync::Arc::clone(&state.db),
        std::sync::Arc::clone(&state.workspace_backend_router),
    )
    .task_diff(&params.task_id)
    .await?;
    serde_json::to_value(diff)
        .map_err(|error| McpToolError::new(-32603, format!("failed to serialize diff: {error}")))
}

pub(super) async fn forge_list_executions(
    state: &AppState,
    params: ListExecutionsParams,
) -> Result<Value, McpToolError> {
    let page = ExecutionRepo::list_by_task(
        &*state.db,
        &params.task_id,
        page_request(params.cursor, params.limit, None)?,
    )
    .await?;
    Ok(execution_page_value(page))
}

pub(super) async fn forge_update_task(
    state: &AppState,
    params: UpdateTaskParams,
) -> Result<Value, McpToolError> {
    let task = state
        .task_service
        .update_task(
            params.task_id,
            api_types::UpdateTaskRequest {
                version: params.version,
                title: params.title,
                description: params.description,
                priority: params.priority,
                merge_config: None,
                plan: params.plan,
                task_state_config: None,
                review_requirement_ids: None,
                parent_task_id: None,
            },
        )
        .await?;
    task_with_offers(state, task).await
}

pub(super) async fn forge_transition_task(
    state: &AppState,
    params: TransitionTaskParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    // The transition runs under the workflow policy of the MCP system
    // component, as it always has: defined edges only, dependency and
    // conflict-marker gates enforced, no user override. Attributing it to the
    // delegated user would also hand the caller the interactive user's
    // override policy, which `Actor::is_user` selects throughout the engine.
    let task = state
        .task_service
        .transition(
            params.task_id,
            params.status,
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: params.version,
                reason: None,
                triggered_by: Actor::system(SystemComponent::Mcp),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await?;
    let actor = Actor::delegated_user(
        authenticated_user(context)?,
        api_types::UserActionSource::Api,
    );
    transition_task_value(state, task.task, task.pending_steps, &actor).await
}

async fn transition_task_value(
    state: &AppState,
    task: db::Task,
    pending_steps: i64,
    actor: &Actor,
) -> Result<Value, McpToolError> {
    let snapshot = state
        .task_service
        .task_action_snapshot_for_task(task, actor)
        .await?;
    let mut value = task_snapshot_value(snapshot)?;
    value["pending_steps"] = json!(pending_steps);
    Ok(value)
}

pub(super) async fn forge_register_agent(
    state: &AppState,
    params: RegisterAgentParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user = authenticated_user_record(state, context).await?;
    let is_admin = user.is_admin;
    // Effect-time re-check of the registry's conditional field authority:
    // the administrator flag is read again here, next to the write.
    if !is_admin && params.daemon_id.is_some() {
        return Err(
            McpToolError::new(-32003, "Admin access required to pin an agent to a daemon")
                .with_data(json!({ "code": "admin_required" })),
        );
    }
    let agent = state
        .agent_service
        .register(
            params.name,
            None,
            params.executor_type,
            None,
            None,
            None,
            None,
            "[]".to_owned(),
            "{}".to_owned(),
            None,
            params.daemon_id,
            None,
            None,
            None,
            false,
            Some(user.id),
            Some("account".to_owned()),
        )
        .await?;
    Ok(agent_value_for_user(agent, is_admin))
}

pub(super) async fn forge_list_agents(
    state: &AppState,
    params: ListAgentsParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user = authenticated_user_record(state, context).await?;
    let page = AgentRepo::list_visible(
        &*state.db,
        &user.id,
        AgentListQuery {
            status: params
                .status
                .map(|value| value.parse::<db::AgentStatus>())
                .transpose()
                .map_err(|error| McpToolError::new(-32602, error))?,
            executor_type: None,
            capabilities: Vec::new(),
            page: page_request(params.cursor, params.limit, None)?,
        },
    )
    .await?;
    Ok(agent_page_value_for_user(page, user.is_admin))
}

pub(super) async fn forge_list_projects(
    state: &AppState,
    params: ListProjectsParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    if let Some(project_id) = context.project_id.as_deref() {
        let page = state
            .db
            .list_visible_bound_project(
                project_id,
                authenticated_user(context)?,
                page_request(params.cursor, params.limit, None)?,
            )
            .await?;
        return Ok(project_page_value(page));
    }
    let page = ProjectRepo::list_visible(
        &*state.db,
        authenticated_user(context)?,
        page_request(params.cursor, params.limit, None)?,
    )
    .await?;
    Ok(project_page_value(page))
}

pub(super) async fn forge_create_project(
    state: &AppState,
    params: CreateProjectParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user = authenticated_user_record(state, context).await?;
    if params.name.trim().is_empty() {
        return Err(McpToolError::new(-32602, "name must not be empty"));
    }
    let now = now_rfc3339();
    let project = ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: new_uuid_v4(),
            name: params.name,
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: Some(user.id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    Ok(project_value(project))
}

pub(super) async fn forge_get_project(
    state: &AppState,
    params: GetProjectParams,
) -> Result<Value, McpToolError> {
    let project = ProjectRepo::get_by_id(&*state.db, &params.project_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", params.project_id))?;
    Ok(project_value(project))
}

pub(super) async fn forge_update_project(
    state: &AppState,
    params: UpdateProjectParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    if params
        .name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(McpToolError::new(-32602, "name must not be empty"));
    }

    let settings = match params.settings {
        Some(settings) => {
            validate_project_settings(state, &params.project_id, &settings, context).await?;
            Some(serialize_settings(&settings)?)
        }
        None => None,
    };

    let project = ProjectRepo::update_at_version(
        &*state.db,
        UpdateProject {
            id: params.project_id,
            name: params.name,
            settings,
            primary_repo_id: None,
            paused_at: params.paused.map(|paused| paused.then(now_rfc3339)),
            updated_at: now_rfc3339(),
        },
        params.version,
        None,
    )
    .await?;
    Ok(project_value(project))
}

pub(super) async fn forge_update_project_lifecycle_hooks(
    state: &AppState,
    params: UpdateProjectLifecycleHooksParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let project = ProjectRepo::get_by_id(&*state.db, &params.project_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", params.project_id.clone()))?;
    if project.version != params.version {
        return Err(db::DbError::VersionConflict.into());
    }
    let mut settings: Value = serde_json::from_str(&project.settings).map_err(|error| {
        McpToolError::new(
            -32602,
            format!("invalid existing project settings: {error}"),
        )
    })?;
    let settings_object = settings
        .as_object_mut()
        .ok_or_else(|| McpToolError::new(-32602, "existing project settings must be an object"))?;
    let hooks =
        serde_json::to_value(lifecycle_hooks_input(params.lifecycle_hooks)).map_err(|error| {
            McpToolError::new(
                -32603,
                format!("failed to serialize lifecycle hooks: {error}"),
            )
        })?;
    settings_object.insert("lifecycle_hooks".to_owned(), hooks);
    validate_project_settings(state, &project.id, &settings, context).await?;

    let project = ProjectRepo::update_at_version(
        &*state.db,
        UpdateProject {
            id: project.id,
            name: None,
            settings: Some(serialize_settings(&settings)?),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        params.version,
        None,
    )
    .await?;
    Ok(project_value(project))
}

pub(super) async fn forge_follow_up_execution(
    state: &AppState,
    params: FollowUpExecutionParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    if let Some(agent_id) = &params.agent_id {
        let execution = ExecutionRepo::get_by_id(&*state.db, &params.execution_id)
            .await?
            .ok_or_else(|| McpToolError::not_found("execution", params.execution_id.clone()))?;
        super::registry::require_task_agent(state, &execution.task_id, agent_id, context).await?;
    }
    let overrides = params.overrides.map(|o| ExecutionOverrides {
        model_id: o.model_id,
        reasoning_effort: o.reasoning_effort,
        permission_policy: o.permission_policy,
        hard_deadline_seconds: o.hard_deadline_seconds,
    });
    let launched = state
        .task_service
        .follow_up_interactive_execution(
            params.execution_id,
            params.message,
            params.agent_id,
            overrides,
        )
        .await?;

    let workspace = state
        .workspace_backend_router
        .resolve(&state.db, &launched.workspace)
        .await
        .map_err(services::ServiceError::from)?;
    let workspace_handle = workspace
        .handle()
        .map_err(services::ServiceError::from)?
        .to_owned();

    Ok(json!({
        "task": task_with_offers(state, launched.task).await?,
        "execution": execution_value(launched.execution),
        "workspace": {
            "id": launched.workspace.id,
            "task_id": launched.workspace.task_id,
            "repo_id": launched.workspace.repo_id,
            "worktree_path": workspace_handle,
            "branch": launched.workspace.branch,
            "status": launched.workspace.status.to_string(),
            "before_sha": launched.workspace.before_sha,
            "error": launched.workspace.error,
            "created_at": launched.workspace.created_at,
            "updated_at": launched.workspace.updated_at,
        },
    }))
}

/// Validate settings with the authenticated delegated user, just as REST does.
async fn validate_project_settings(
    state: &AppState,
    project_id: &str,
    settings: &Value,
    context: &McpContext,
) -> Result<(), McpToolError> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", project_id.to_owned()))?;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    services::project_settings::validate_project_settings(
        &state.db,
        settings,
        &workflow,
        Some(project_id),
        Some(authenticated_user(context)?),
    )
    .await
    .map_err(McpToolError::from)
}

fn serialize_settings(settings: &Value) -> Result<String, McpToolError> {
    serde_json::to_string(settings)
        .map_err(|error| McpToolError::new(-32602, format!("invalid settings: {error}")))
}

fn non_empty_tools(tools: Vec<String>) -> Value {
    if tools.is_empty() {
        Value::Null
    } else {
        json!(tools)
    }
}

pub(super) async fn forge_add_task_dependency(
    state: &AppState,
    params: AddTaskDependencyParams,
) -> Result<Value, McpToolError> {
    state
        .task_service
        .add_task_dependency(&params.task_id, &params.depends_on_id)
        .await?;
    Ok(json!({ "task_id": params.task_id, "depends_on_id": params.depends_on_id }))
}

pub(super) async fn forge_remove_task_dependency(
    state: &AppState,
    params: RemoveTaskDependencyParams,
) -> Result<Value, McpToolError> {
    state
        .task_service
        .remove_task_dependency(&params.task_id, &params.depends_on_id)
        .await?;
    Ok(json!({ "task_id": params.task_id, "depends_on_id": params.depends_on_id }))
}

pub(super) async fn forge_list_task_dependencies(
    state: &AppState,
    params: ListTaskDependenciesParams,
) -> Result<Value, McpToolError> {
    let deps = TaskDependencyRepo::list_dependencies(&*state.db, &params.task_id).await?;
    Ok(json!({ "task_id": params.task_id, "depends_on_ids": deps }))
}

pub(super) async fn forge_list_task_dependents(
    state: &AppState,
    params: ListTaskDependenciesParams,
) -> Result<Value, McpToolError> {
    let dependents = TaskDependencyRepo::list_dependents(&*state.db, &params.task_id).await?;
    Ok(json!({
        "depends_on_id": params.task_id,
        "dependent_task_ids": dependents,
    }))
}

pub(super) async fn forge_list_sub_tasks(
    state: &AppState,
    params: ListSubTasksParams,
) -> Result<Value, McpToolError> {
    let subtasks = TaskRepo::list_subtasks_ordered(&*state.db, &params.parent_task_id).await?;
    Ok(json!({
        "parent_task_id": params.parent_task_id,
        "subtasks": tasks_with_role_assignments(state, subtasks).await?,
    }))
}

pub(super) async fn forge_reorder_sub_tasks(
    state: &AppState,
    params: ReorderSubTasksParams,
) -> Result<Value, McpToolError> {
    state
        .task_service
        .reorder_subtasks(params.parent_task_id.clone(), params.ordered_ids)
        .await
        .map_err(|error| match error {
            services::ServiceError::Db(db::DbError::Check(details)) => invalid_field_error(
                "ordered_ids",
                details,
                Some(json!({
                    "constraint": "every current direct child exactly once"
                })),
            ),
            services::ServiceError::Db(db::DbError::InvalidTransition) => invalid_field_error(
                "ordered_ids",
                "the terminal prefix and current first incomplete child must remain fixed; only the untouched todo suffix may be reordered",
                None,
            ),
            other => other.into(),
        })?;
    let subtasks = TaskRepo::list_subtasks_ordered(&*state.db, &params.parent_task_id).await?;
    Ok(json!({
        "parent_task_id": params.parent_task_id,
        "subtasks": tasks_with_role_assignments(state, subtasks).await?,
    }))
}

/// The merge queues of the Project's repository, as the native
/// `repo.integration_queue` read answers them (one service function). The
/// registry admitted the caller as a member of `project_id`; the Project is
/// read again here, by the rule of the REST route: a Project the caller
/// cannot see answers like one that does not exist.
pub(super) async fn forge_get_integration_queue(
    state: &AppState,
    params: operation_registry::mcp::GetIntegrationQueueParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    db::ProjectRepo::get_visible_by_id(&*state.db, &params.project_id, user_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", params.project_id.clone()))?;
    Ok(services::integration_reads::project_integration_queues(
        &state.db,
        &params.project_id,
        params.target_branch.as_deref(),
        params.limit,
    )
    .await?)
}

fn authenticated_user(context: &McpContext) -> Result<&str, McpToolError> {
    context
        .user_id
        .as_deref()
        .filter(|user_id| !user_id.trim().is_empty())
        .ok_or_else(|| McpToolError::new(-32001, "authenticated MCP user is required"))
}

async fn authenticated_user_record(
    state: &AppState,
    context: &McpContext,
) -> Result<db::User, McpToolError> {
    let user_id = authenticated_user(context)?;
    UserRepo::get_user_by_id(&*state.db, user_id)
        .await?
        .ok_or_else(|| McpToolError::new(-32001, "authenticated MCP user is required"))
}

fn serialize_public<T: serde::Serialize>(value: T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

pub(super) async fn forge_project_escalate(
    state: &AppState,
    params: ProjectEscalateParams,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = authenticated_user(context)?;
    let project_id = context
        .project_id
        .as_deref()
        .ok_or_else(|| McpToolError::new(-32602, "project scope is required"))?;
    let request = api_types::ProjectEscalateRequest {
        need: params.need,
        task_ids: params.task_ids,
    };
    Ok(serialize_public(
        services::project_escalation::ProjectEscalationService::new(state.db.clone())
            .escalate(
                project_id,
                services::project_escalation::EscalationAuthority::Owner(user_id),
                request,
                &params.dedupe_key,
            )
            .await?,
    ))
}

fn task_action_input(action: operation_registry::mcp::TaskAction) -> api_types::TaskAction {
    use operation_registry::mcp::TaskAction as Input;
    match action {
        Input::Start {} => api_types::TaskAction::Start,
        Input::Hold { reason } => api_types::TaskAction::Hold { reason },
        Input::Release { reason } => api_types::TaskAction::Release { reason },
        Input::Retry {
            reason,
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        } => api_types::TaskAction::Retry {
            reason,
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        },
        Input::SendBack { guidance } => api_types::TaskAction::SendBack { guidance },
        Input::Approve {
            reason,
            override_checks,
        } => api_types::TaskAction::Approve {
            reason,
            override_checks,
        },
        Input::Restart { reason } => api_types::TaskAction::Restart { reason },
        Input::Cancel { reason } => api_types::TaskAction::Cancel { reason },
    }
}

fn lifecycle_hooks_input(
    hooks: operation_registry::mcp::LifecycleHooks,
) -> api_types::LifecycleHooks {
    use operation_registry::mcp::{LifecycleEvent as Event, LifecycleHookDef as Hook};
    hooks
        .into_iter()
        .map(|(event, hooks)| {
            let event = match event {
                Event::BeforeWork => api_types::LifecycleEvent::BeforeWork,
                Event::OnWorkStart => api_types::LifecycleEvent::OnWorkStart,
                Event::OnWorkStop => api_types::LifecycleEvent::OnWorkStop,
                Event::OnTaskDone => api_types::LifecycleEvent::OnTaskDone,
                Event::OnTaskCancel => api_types::LifecycleEvent::OnTaskCancel,
            };
            let hooks = hooks
                .into_iter()
                .map(|hook| match hook {
                    Hook::Script {
                        command,
                        timeout_seconds,
                        blocking,
                    } => api_types::LifecycleHookDef::Script {
                        command,
                        timeout_seconds: timeout_seconds.get(),
                        blocking,
                    },
                    Hook::Plugin {
                        name,
                        enabled,
                        config,
                    } => api_types::LifecycleHookDef::Plugin {
                        name,
                        enabled,
                        config,
                    },
                })
                .collect();
            (event, hooks)
        })
        .collect()
}
