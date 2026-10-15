//! Server-loaded facts and the typed MCP projection of the operation registry.
use super::handlers;
use crate::{error::McpToolError, protocol::McpContext, state::AppState};
use db::{
    AgentChatRepo, AgentHandoffRepo, AgentRepo, AgentSessionRepo, ExecutionRepo, ProjectMemberRepo,
    ProjectRepo, TaskRepo, UserRepo,
};
use operation_registry::{
    authority::{
        mcp_authority, AuthorityRequirement, EffectiveAuthority, PrincipalRule, RequiredPermission,
    },
    mcp::{McpInput, McpInputError, McpOperation, ResourceRule},
    AvailabilityRule,
};
use serde_json::{json, Value};

/// Serde reports a mistyped value by quoting it (`invalid type: string
/// "...", expected i64`). The correction keeps the kind and the expectation
/// and drops the caller's literal, so no argument value is echoed.
fn without_echoed_value(detail: &str) -> String {
    for prefix in ["invalid type: ", "invalid value: "] {
        if let Some(rest) = detail.strip_prefix(prefix) {
            let kind = rest
                .split(|c: char| !c.is_ascii_alphabetic())
                .next()
                .unwrap_or_default();
            let expected = rest
                .rfind(", expected ")
                .map_or("", |position| &rest[position..]);
            return format!("{prefix}{kind}{expected}");
        }
    }
    if let Some(rest) = detail.strip_prefix("unknown variant `") {
        return rest.split_once('`').map_or_else(
            || "unknown variant".into(),
            |(_, expected)| format!("unknown variant{expected}"),
        );
    }
    detail.to_owned()
}

pub(crate) fn input_error(error: McpInputError) -> McpToolError {
    let detail = without_echoed_value(&error.detail)
        .chars()
        .take(512)
        .collect::<String>();
    let correction = format!(
        "{}: {}; expected {}",
        error.operation, detail, error.expected
    );
    let field = error
        .detail
        .split_once("argument `")
        .and_then(|(_, rest)| rest.split_once('`'))
        .map(|(field, _)| field)
        .or_else(|| {
            error
                .detail
                .strip_prefix("missing field `")
                .and_then(|rest| rest.split_once('`'))
                .map(|(field, _)| field)
        });
    let field = field.filter(|field| field.len() <= 128);
    let mut data =
        json!({"code":"mcp_contract_invalid", "operation":error.operation, "details":correction});
    if let Some(field) = field {
        data["field"] = json!(field);
        if let Some(property) = operation_registry::mcp::lookup(error.operation)
            .and_then(|spec| spec.input.schema["properties"].get(field))
        {
            let accepted = property
                .as_object()
                .map(|property| {
                    property
                        .iter()
                        .filter(|(key, _)| {
                            [
                                "type",
                                "enum",
                                "minimum",
                                "maximum",
                                "minLength",
                                "maxLength",
                            ]
                            .contains(&key.as_str())
                        })
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<serde_json::Map<_, _>>()
                })
                .unwrap_or_default();
            if !accepted.is_empty() {
                data["accepted"] = json!(accepted);
            }
        }
    }
    McpToolError::new(-32602, "invalid params").with_data(data)
}

fn denied() -> McpToolError {
    McpToolError::new(-32001, "tool is outside the delegated MCP grant")
        .with_data(json!({"code":"mcp_scope_denied"}))
}

pub(crate) async fn authority(
    state: &AppState,
    context: &McpContext,
) -> Result<EffectiveAuthority, McpToolError> {
    let user_id = context.user_id.as_deref().unwrap_or_default();
    let mut authority = mcp_authority(user_id, context.project_id.as_deref());
    if context.project_id.is_none() {
        if let Some(user) = UserRepo::get_user_by_id(&*state.db, user_id).await? {
            if user.is_admin {
                authority.ceiling.insert("mcp_account_admin".into());
            }
        }
    }
    if let Some(project_id) = context.project_id.as_deref() {
        load_project_facts(state, &mut authority, project_id, user_id).await?;
    }
    Ok(authority)
}
async fn load_project_facts(
    state: &AppState,
    authority: &mut EffectiveAuthority,
    project_id: &str,
    user_id: &str,
) -> Result<(), McpToolError> {
    let Some(project) = ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id).await?
    else {
        return Ok(());
    };
    authority.ceiling.insert("mcp_project_visible".into());
    // Catalog admission is the frozen contract (`mcp_h_base_authority.json`),
    // unchanged; the role a mutation needs on the Project its arguments
    // reference is enforced in `authorize_resource` (`project_role`).
    let member = ProjectMemberRepo::get_member(&*state.db, project_id, user_id).await?;
    if project.owner_id.as_deref() == Some(user_id) {
        authority.ceiling.insert("mcp_project_owner".into());
    }
    if project.owner_id.as_deref() == Some(user_id) || member.is_some() {
        authority.ceiling.insert("mcp_project_member".into());
    }
    if project.owner_id.as_deref() == Some(user_id)
        || member.is_some_and(|member| matches!(member.role.as_str(), "owner" | "admin"))
    {
        authority.ceiling.insert("mcp_project_admin".into());
    }
    Ok(())
}
pub(crate) fn authorize(authority: &EffectiveAuthority, name: &str) -> Result<(), McpToolError> {
    let spec = operation_registry::mcp::lookup(name)
        .ok_or_else(|| McpToolError::protocol(-32601, "method not found"))?;
    authority.evaluate(spec).map_err(|_| denied())
}
struct ResourcePermission(&'static str);
impl AuthorityRequirement for ResourcePermission {
    fn principal_rule(&self) -> PrincipalRule {
        PrincipalRule::DelegatedUser
    }
    fn permission(&self, _: &str) -> RequiredPermission<'_> {
        RequiredPermission::Named(self.0)
    }
    fn availability(&self) -> AvailabilityRule {
        AvailabilityRule::Always
    }
}
fn evaluate_resource(
    authority: &EffectiveAuthority,
    permission: &'static str,
) -> Result<(), McpToolError> {
    authority
        .evaluate(ResourcePermission(permission))
        .map_err(|_| denied())
}

pub(crate) async fn authorize_resource(
    state: &AppState,
    spec: &McpOperation,
    arguments: &Value,
    context: &McpContext,
) -> Result<(), McpToolError> {
    // References are read from named fields. Anything but an object carries
    // none, and is refused here so that it cannot reach the typed decode.
    if !arguments.is_object() {
        return Err(McpToolError::new(
            -32602,
            "tool arguments must be an object",
        ));
    }
    let user_id = context.user_id.as_deref().unwrap_or_default();
    let mut authority = mcp_authority(user_id, context.project_id.as_deref());
    let get_id = |field: &str| arguments.get(field).and_then(Value::as_str);
    let mut project_id = get_id("project_id").map(str::to_owned);
    let task_field = match spec.resource {
        ResourceRule::Task
        | ResourceRule::DependencyMutation
        | ResourceRule::Dependencies
        | ResourceRule::Dependents => Some("task_id"),
        ResourceRule::ParentTask | ResourceRule::SubTasks | ResourceRule::ReorderSubTasks => {
            Some("parent_task_id")
        }
        _ => None,
    };
    if let Some(task_id) = task_field.and_then(get_id) {
        project_id = Some(
            TaskRepo::get_by_id(&*state.db, task_id, false)
                .await?
                .ok_or_else(|| McpToolError::not_found("task", task_id.to_owned()))?
                .project_id,
        );
    }
    if spec.resource == ResourceRule::Execution {
        if let Some(id) = get_id("execution_id") {
            let execution = ExecutionRepo::get_by_id(&*state.db, id)
                .await?
                .ok_or_else(|| McpToolError::not_found("execution", id.to_owned()))?;
            project_id = Some(
                TaskRepo::get_by_id(&*state.db, &execution.task_id, false)
                    .await?
                    .ok_or_else(|| McpToolError::not_found("execution", id.to_owned()))?
                    .project_id,
            );
        }
    }
    if spec.resource == ResourceRule::Chat {
        if let Some(chat_id) = get_id("chat_id") {
            project_id = state
                .agent_chat_service
                .get_authorized_chat(user_id, chat_id)
                .await?
                .project_id;
        }
    }
    if let Some(project_id) = project_id.as_deref() {
        require_project_access(state, project_id, user_id, project_role(spec)).await?;
        if spec.name == "forge_task_action" {
            if let (Some(task_id), Some(value)) = (get_id("task_id"), arguments.get("action")) {
                if let Ok(action) = serde_json::from_value::<api_types::TaskAction>(value.clone()) {
                    let needs_owner = match &action {
                        api_types::TaskAction::Approve {
                            override_checks: Some(value),
                            ..
                        } => *value,
                        api_types::TaskAction::Approve {
                            override_checks: None,
                            ..
                        } => {
                            let offers = state
                                .task_service
                                .task_action_offers(
                                    task_id,
                                    &api_types::Actor::user(api_types::UserActionSource::Api),
                                )
                                .await?;
                            offers.available_actions.iter().find(|offer| offer.action.verb() == "approve" && operation_registry::task_proposals::action_parameters_allowed(offer, &action, true))
                                .is_some_and(|offer| offer.parameters.iter().find(|parameter| parameter.name == "override").and_then(|parameter| parameter.boolean_values.as_ref()).is_some_and(|values| values.as_slice() == [true]) || matches!(offer.action, api_types::TaskAction::Approve { override_checks: Some(true), .. }))
                        }
                        _ => false,
                    };
                    if needs_owner {
                        require_project_access(
                            state,
                            project_id,
                            user_id,
                            operation_registry::mcp::RequiredRole::Owner,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    if let Some(rule) = &spec.field_authority {
        if rule.applies(arguments) {
            if rule.permission == "mcp_account_admin" {
                if UserRepo::get_user_by_id(&*state.db, user_id)
                    .await?
                    .is_some_and(|user| user.is_admin)
                {
                    authority.ceiling.insert(rule.permission.into());
                }
            } else if let Some(project_id) = arguments.get("project_id").and_then(Value::as_str) {
                load_project_facts(state, &mut authority, project_id, user_id).await?;
            }
            authority.evaluate(rule).map_err(|_| {
                McpToolError::new(-32003, rule.denial_message)
                    .with_data(json!({"code":rule.denial_code}))
            })?;
        }
    }
    if let Some(reference) = spec.agent_reference {
        authorize_agent_reference(state, reference, arguments, context).await?;
    }
    let get = |field: &str| arguments.get(field).and_then(Value::as_str);
    match spec.resource {
        ResourceRule::OwnedIdentity | ResourceRule::OwnedSession => {
            // The denial names only what the caller sent: a session owned by
            // another account answers exactly like a missing one, and never
            // names that account's identity.
            let (identity_id, hidden) = if spec.resource == ResourceRule::OwnedSession {
                let Some(id) = get("session_id") else {
                    return Ok(());
                };
                let hidden = || McpToolError::not_found("agent_session", id.to_owned());
                let session = AgentSessionRepo::get_agent_session(&*state.db, id)
                    .await?
                    .ok_or_else(hidden)?;
                (session.identity_id, hidden())
            } else {
                let Some(id) = get("identity_id") else {
                    return Ok(());
                };
                (
                    id.to_owned(),
                    McpToolError::not_found("agent_identity", id.to_owned()),
                )
            };
            if AgentRepo::get_by_id(&*state.db, &identity_id)
                .await?
                .is_some_and(|identity| identity.owner_id.as_deref() == Some(user_id))
            {
                authority.ceiling.insert("mcp_owned_identity".into());
            }
            evaluate_resource(&authority, "mcp_owned_identity").map_err(|_| hidden)?;
            if spec.resource == ResourceRule::OwnedSession {
                if let Some(id) = get("session_id") {
                    let session = AgentSessionRepo::get_agent_session(&*state.db, id)
                        .await?
                        .ok_or_else(|| McpToolError::not_found("agent_session", id.to_owned()))?;
                    require_session_scope(state, &session, user_id).await?;
                }
            }
        }
        ResourceRule::Project | ResourceRule::Handoff => {
            let Some(project_id) = get("project_id") else {
                return Ok(());
            };
            load_project_facts(state, &mut authority, project_id, user_id).await?;
            let permission = match spec.role {
                operation_registry::mcp::RequiredRole::Admin => "mcp_project_admin",
                operation_registry::mcp::RequiredRole::Member => "mcp_project_member",
                operation_registry::mcp::RequiredRole::Owner => "mcp_project_owner",
                _ => "mcp_project_visible",
            };
            evaluate_resource(&authority, permission)?;
            if spec.resource == ResourceRule::Handoff {
                let Some(id) = get("handoff_id") else {
                    return Ok(());
                };
                let handoff = AgentHandoffRepo::get_agent_handoff(&*state.db, id)
                    .await?
                    .ok_or_else(|| McpToolError::not_found("agent_handoff", id.to_owned()))?;
                let target =
                    AgentChatRepo::get_agent_chat(&*state.db, &handoff.target_chat_id).await?;
                if !target.is_some_and(|target| target.project_id.as_deref() == Some(project_id)) {
                    return Err(McpToolError::not_found("agent_handoff", id.to_owned()));
                }
            }
        }
        ResourceRule::Chat => {
            let Some(chat_id) = get("chat_id") else {
                return Ok(());
            };
            // The service remains the domain's fresh Chat authorization read.
            let chat = state
                .agent_chat_service
                .get_authorized_chat(user_id, chat_id)
                .await?;
            if context
                .project_id
                .as_deref()
                .is_some_and(|id| chat.kind != "project" || chat.project_id.as_deref() != Some(id))
            {
                return Err(McpToolError::new(
                    -32602,
                    "Agent Chat is outside the scoped MCP project",
                ));
            }
        }
        // Task/Execution references have been resolved and bound by RPC.
        ResourceRule::None
        | ResourceRule::Task
        | ResourceRule::ParentTask
        | ResourceRule::CreateTask
        | ResourceRule::DependencyMutation
        | ResourceRule::Dependencies
        | ResourceRule::Dependents
        | ResourceRule::SubTasks
        | ResourceRule::ReorderSubTasks
        | ResourceRule::Execution => {}
    }
    Ok(())
}

pub(super) async fn require_usable_agent(
    state: &AppState,
    project_id: &str,
    agent_id: &str,
    context: &McpContext,
) -> Result<(), McpToolError> {
    let user_id = context.user_id.as_deref().unwrap_or_default();
    if state
        .db
        .list_agents_usable_in_project(project_id, user_id)
        .await?
        .iter()
        .any(|agent| agent.id == agent_id)
    {
        Ok(())
    } else {
        Err(McpToolError::not_found("agent", agent_id.to_owned()))
    }
}
pub(super) async fn require_task_agent(
    state: &AppState,
    task_id: &str,
    agent_id: &str,
    context: &McpContext,
) -> Result<(), McpToolError> {
    let task = TaskRepo::get_by_id(&*state.db, task_id, false)
        .await?
        .ok_or_else(|| McpToolError::not_found("task", task_id.to_owned()))?;
    require_usable_agent(state, &task.project_id, agent_id, context).await
}
async fn authorize_agent_reference(
    state: &AppState,
    reference: operation_registry::mcp::AgentReference,
    arguments: &Value,
    context: &McpContext,
) -> Result<(), McpToolError> {
    use operation_registry::mcp::AgentReference;
    let get = |field| arguments.get(field).and_then(Value::as_str);
    match reference {
        AgentReference::Task { task, agent } => {
            if let (Some(task), Some(agent)) = (get(task), get(agent)) {
                require_task_agent(state, task, agent, context).await?;
            }
        }
        AgentReference::SubTasks {
            parent,
            items,
            agent,
        } => {
            if let Some(parent) = get(parent) {
                for child in arguments
                    .get(items)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(agent) = child.get(agent).and_then(Value::as_str) {
                        require_task_agent(state, parent, agent, context).await?;
                    }
                }
            }
        }
        AgentReference::Execution { execution, agent } => {
            if let (Some(execution), Some(agent)) = (get(execution), get(agent)) {
                let execution = ExecutionRepo::get_by_id(&*state.db, execution)
                    .await?
                    .ok_or_else(|| McpToolError::not_found("execution", execution.to_owned()))?;
                require_task_agent(state, &execution.task_id, agent, context).await?;
            }
        }
    }
    Ok(())
}

struct HandlerContext<'a> {
    state: &'a AppState,
    context: &'a McpContext,
}
#[async_trait::async_trait]
impl operation_registry::mcp::McpHandlerContext<McpToolError> for HandlerContext<'_> {
    async fn project_escalate(
        &self,
        params: operation_registry::mcp::ProjectEscalateParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_project_escalate(self.state, params, self.context).await
    }
    async fn update_project_lifecycle_hooks(
        &self,
        params: operation_registry::mcp::UpdateProjectLifecycleHooksParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_update_project_lifecycle_hooks(self.state, params, self.context).await
    }
    async fn update_project(
        &self,
        params: operation_registry::mcp::UpdateProjectParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_update_project(self.state, params, self.context).await
    }
    async fn get_project(
        &self,
        params: operation_registry::mcp::GetProjectParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_project(self.state, params).await
    }
    async fn follow_up_execution(
        &self,
        params: operation_registry::mcp::FollowUpExecutionParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_follow_up_execution(self.state, params, self.context).await
    }
    async fn reorder_sub_tasks(
        &self,
        params: operation_registry::mcp::ReorderSubTasksParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_reorder_sub_tasks(self.state, params).await
    }
    async fn create_sub_tasks(
        &self,
        params: operation_registry::mcp::CreateSubTasksParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_create_sub_tasks(self.state, params, self.context).await
    }
    async fn remove_task_dependency(
        &self,
        params: operation_registry::mcp::RemoveTaskDependencyParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_remove_task_dependency(self.state, params).await
    }
    async fn add_task_dependency(
        &self,
        params: operation_registry::mcp::AddTaskDependencyParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_add_task_dependency(self.state, params).await
    }
    async fn transition_task(
        &self,
        params: operation_registry::mcp::TransitionTaskParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_transition_task(self.state, params, self.context).await
    }
    async fn update_task(
        &self,
        params: operation_registry::mcp::UpdateTaskParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_update_task(self.state, params).await
    }
    async fn task_action(
        &self,
        params: operation_registry::mcp::TaskActionParams,
    ) -> Result<Value, McpToolError> {
        handlers::task_action(self.state, params, self.context).await
    }
    async fn assign_agent(
        &self,
        params: operation_registry::mcp::AssignAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_assign_agent(self.state, params, self.context).await
    }
    async fn create_task(
        &self,
        params: operation_registry::mcp::CreateTaskParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_create_task(self.state, params).await
    }
    async fn list_sub_tasks(
        &self,
        params: operation_registry::mcp::ListSubTasksParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_sub_tasks(self.state, params).await
    }
    async fn list_task_dependents(
        &self,
        params: operation_registry::mcp::ListTaskDependenciesParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_task_dependents(self.state, params).await
    }
    async fn list_task_dependencies(
        &self,
        params: operation_registry::mcp::ListTaskDependenciesParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_task_dependencies(self.state, params).await
    }
    async fn list_executions(
        &self,
        params: operation_registry::mcp::ListExecutionsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_executions(self.state, params).await
    }
    async fn get_task_diff(
        &self,
        params: operation_registry::mcp::GetTaskParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_task_diff(self.state, params).await
    }
    async fn memory_get(
        &self,
        params: operation_registry::mcp::MemoryGetParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_memory_get(self.state, params, self.context).await
    }
    async fn memory_search(
        &self,
        params: operation_registry::mcp::MemorySearchParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_memory_search(self.state, params, self.context).await
    }
    async fn preview_prompt(
        &self,
        params: operation_registry::mcp::PreviewPromptParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_preview_prompt(self.state, params).await
    }
    async fn get_task(
        &self,
        params: operation_registry::mcp::GetTaskParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_task(self.state, params, self.context).await
    }
    async fn list_tasks(
        &self,
        params: operation_registry::mcp::ListTasksParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_tasks(self.state, params).await
    }
    async fn register_agent(
        &self,
        params: operation_registry::mcp::RegisterAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_register_agent(self.state, params, self.context).await
    }
    async fn list_agents(
        &self,
        params: operation_registry::mcp::ListAgentsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agents(self.state, params, self.context).await
    }
    async fn list_projects(
        &self,
        params: operation_registry::mcp::ListProjectsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_projects(self.state, params, self.context).await
    }
    async fn create_project(
        &self,
        params: operation_registry::mcp::CreateProjectParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_create_project(self.state, params, self.context).await
    }
    async fn list_agent_profiles(
        &self,
        params: operation_registry::mcp::ListAgentProfilesParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agent_profiles(self.state, params, self.context).await
    }
    async fn list_agent_sessions(
        &self,
        params: operation_registry::mcp::ListAgentSessionsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agent_sessions(self.state, params, self.context).await
    }
    async fn get_agent_session(
        &self,
        params: operation_registry::mcp::GetAgentSessionParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_agent_session(self.state, params, self.context).await
    }
    async fn get_main_agent(
        &self,
        params: operation_registry::mcp::GetMainAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_main_agent(self.state, params, self.context).await
    }
    async fn bind_main_agent(
        &self,
        params: operation_registry::mcp::BindMainAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_set_main_agent(self.state, params, self.context).await
    }
    async fn get_project_agent(
        &self,
        params: operation_registry::mcp::GetProjectAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_project_agent(self.state, params, self.context).await
    }
    async fn bind_project_agent(
        &self,
        params: operation_registry::mcp::BindProjectAgentParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_set_project_agent(self.state, params, self.context).await
    }
    async fn get_integration_queue(
        &self,
        params: operation_registry::mcp::GetIntegrationQueueParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_integration_queue(self.state, params, self.context).await
    }
    async fn list_agent_chats(
        &self,
        params: operation_registry::mcp::ListAgentChatsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agent_chats(self.state, params, self.context).await
    }
    async fn get_agent_chat(
        &self,
        params: operation_registry::mcp::GetAgentChatParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_agent_chat(self.state, params, self.context).await
    }
    async fn list_agent_chat_messages(
        &self,
        params: operation_registry::mcp::ListAgentChatMessagesParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agent_chat_messages(self.state, params, self.context).await
    }
    async fn send_agent_chat_message(
        &self,
        params: operation_registry::mcp::SendAgentChatMessageParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_send_agent_chat_message(self.state, params, self.context).await
    }
    async fn list_agent_handoffs(
        &self,
        params: operation_registry::mcp::ListAgentHandoffsParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_list_agent_handoffs(self.state, params, self.context).await
    }
    async fn get_agent_handoff(
        &self,
        params: operation_registry::mcp::GetAgentHandoffParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_get_agent_handoff(self.state, params, self.context).await
    }
    async fn create_agent_handoff(
        &self,
        params: operation_registry::mcp::CreateAgentHandoffParams,
    ) -> Result<Value, McpToolError> {
        handlers::forge_create_agent_handoff(self.state, params, self.context).await
    }
}

pub(crate) async fn dispatch(
    state: &AppState,
    input: McpInput,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    input.dispatch(&HandlerContext { state, context }).await
}

/// The Project role a tool needs on the Project its arguments reference.
/// Catalog roles (`spec.role`) are the frozen admission contract; the REST
/// role rule for the same change is applied here, once the Project is known:
/// Task and dependency mutations need `member`, Project settings `admin`.
fn project_role(spec: &McpOperation) -> operation_registry::mcp::RequiredRole {
    use operation_registry::mcp::RequiredRole;
    match spec.name {
        "forge_create_task"
        | "forge_assign_agent"
        | "forge_task_action"
        | "forge_update_task"
        | "forge_transition_task"
        | "forge_follow_up_execution"
        | "forge_add_task_dependency"
        | "forge_remove_task_dependency"
        | "forge_create_sub_tasks"
        | "forge_reorder_sub_tasks" => RequiredRole::Member,
        "forge_update_project" | "forge_update_project_lifecycle_hooks" => RequiredRole::Admin,
        _ => spec.role,
    }
}

pub(crate) async fn require_project_access(
    state: &AppState,
    project_id: &str,
    user_id: &str,
    role: operation_registry::mcp::RequiredRole,
) -> Result<(), McpToolError> {
    use operation_registry::mcp::RequiredRole;
    let project = ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", project_id.to_owned()))?;
    if !matches!(
        role,
        RequiredRole::Member | RequiredRole::Admin | RequiredRole::Owner
    ) {
        return Ok(());
    }
    // A legacy ownerless Project keeps today's rights for every signed-in
    // user, as in REST (`routes::authorization::require_project_access`).
    let Some(owner_id) = project.owner_id.as_deref() else {
        return Ok(());
    };
    let member = ProjectMemberRepo::get_member(&*state.db, project_id, user_id).await?;
    let allowed = member.map_or(
        owner_id == user_id,
        |member| match role {
            RequiredRole::Member => matches!(member.role.as_str(), "owner" | "admin" | "member"),
            RequiredRole::Admin => matches!(member.role.as_str(), "owner" | "admin"),
            RequiredRole::Owner => member.role == "owner",
            _ => true,
        },
    );
    if !allowed {
        return Err(McpToolError::new(-32003, "insufficient Project role")
            .with_data(json!({"code":"insufficient_role"})));
    }
    Ok(())
}

pub(super) async fn require_session_scope(
    state: &AppState,
    session: &db::AgentSession,
    user_id: &str,
) -> Result<(), McpToolError> {
    let scope = db::AgentContextScopeRepo::get_context_scope(&*state.db, &session.context_scope_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("agent_session", session.id.clone()))?;
    match scope.scope_type.as_str() {
        "account" if scope.scope_id == user_id => Ok(()),
        "project" => {
            require_project_access(
                state,
                &scope.scope_id,
                user_id,
                operation_registry::mcp::RequiredRole::Project,
            )
            .await
        }
        "task" => {
            let task = TaskRepo::get_by_id(&*state.db, &scope.scope_id, false)
                .await?
                .ok_or_else(|| McpToolError::not_found("agent_session", session.id.clone()))?;
            require_project_access(
                state,
                &task.project_id,
                user_id,
                operation_registry::mcp::RequiredRole::Project,
            )
            .await
        }
        "agent_chat" => {
            state
                .agent_chat_service
                .get_authorized_chat(user_id, &scope.scope_id)
                .await?;
            Ok(())
        }
        _ => Err(McpToolError::not_found("agent_session", session.id.clone())),
    }
}
