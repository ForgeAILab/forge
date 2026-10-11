//! Server-loaded facts and the typed MCP projection of the operation registry.
use super::handlers;
use crate::{error::McpToolError, protocol::McpContext, state::AppState};
use db::{
    AgentChatRepo, AgentHandoffRepo, AgentRepo, AgentSessionRepo, ProjectMemberRepo, ProjectRepo,
    UserRepo,
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
    if let Some(rule) = &spec.field_authority {
        if arguments
            .get(rule.field)
            .is_some_and(|value| !value.is_null())
        {
            if UserRepo::get_user_by_id(&*state.db, user_id)
                .await?
                .is_some_and(|user| user.is_admin)
            {
                authority.ceiling.insert(rule.permission.into());
            }
            authority.evaluate(rule).map_err(|_| {
                McpToolError::new(-32003, rule.denial_message)
                    .with_data(json!({"code":rule.denial_code}))
            })?;
        }
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
        }
        ResourceRule::Project | ResourceRule::Handoff => {
            let Some(project_id) = get("project_id") else {
                return Ok(());
            };
            load_project_facts(state, &mut authority, project_id, user_id).await?;
            let permission = if spec.role == operation_registry::mcp::RequiredRole::Admin {
                "mcp_project_admin"
            } else {
                "mcp_project_member"
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

pub(crate) async fn dispatch(
    state: &AppState,
    input: McpInput,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    match input {
        McpInput::ProjectEscalate(params) => {
            handlers::forge_project_escalate(state, params, context).await
        }
        McpInput::UpdateProjectLifecycleHooks(params) => {
            handlers::forge_update_project_lifecycle_hooks(state, params, context).await
        }
        McpInput::UpdateProject(params) => {
            handlers::forge_update_project(state, params, context).await
        }
        McpInput::GetProject(params) => handlers::forge_get_project(state, params).await,
        McpInput::FollowUpExecution(params) => {
            handlers::forge_follow_up_execution(state, params).await
        }
        McpInput::ReorderSubTasks(params) => handlers::forge_reorder_sub_tasks(state, params).await,
        McpInput::CreateSubTasks(params) => handlers::forge_create_sub_tasks(state, params).await,
        McpInput::RemoveTaskDependency(params) => {
            handlers::forge_remove_task_dependency(state, params).await
        }
        McpInput::AddTaskDependency(params) => {
            handlers::forge_add_task_dependency(state, params).await
        }
        McpInput::TransitionTask(params) => {
            handlers::forge_transition_task(state, params, context).await
        }
        McpInput::UpdateTask(params) => handlers::forge_update_task(state, params).await,
        McpInput::TaskAction(params) => handlers::task_action(state, params, context).await,
        McpInput::AssignAgent(params) => handlers::forge_assign_agent(state, params).await,
        McpInput::CreateTask(params) => handlers::forge_create_task(state, params).await,
        McpInput::ListSubTasks(params) => handlers::forge_list_sub_tasks(state, params).await,
        McpInput::ListTaskDependents(params) => {
            handlers::forge_list_task_dependents(state, params).await
        }
        McpInput::ListTaskDependencies(params) => {
            handlers::forge_list_task_dependencies(state, params).await
        }
        McpInput::ListExecutions(params) => handlers::forge_list_executions(state, params).await,
        McpInput::GetTaskDiff(params) => handlers::forge_get_task_diff(state, params).await,
        McpInput::MemoryGet(params) => handlers::forge_memory_get(state, params, context).await,
        McpInput::MemorySearch(params) => {
            handlers::forge_memory_search(state, params, context).await
        }
        McpInput::PreviewPrompt(params) => handlers::forge_preview_prompt(state, params).await,
        McpInput::GetTask(params) => handlers::forge_get_task(state, params, context).await,
        McpInput::ListTasks(params) => handlers::forge_list_tasks(state, params).await,
        McpInput::RegisterAgent(params) => {
            handlers::forge_register_agent(state, params, context).await
        }
        McpInput::ListAgents(params) => handlers::forge_list_agents(state, params, context).await,
        McpInput::ListProjects(params) => {
            handlers::forge_list_projects(state, params, context).await
        }
        McpInput::CreateProject(params) => {
            handlers::forge_create_project(state, params, context).await
        }
        McpInput::ListAgentProfiles(params) => {
            handlers::forge_list_agent_profiles(state, params, context).await
        }
        McpInput::ListAgentSessions(params) => {
            handlers::forge_list_agent_sessions(state, params, context).await
        }
        McpInput::GetAgentSession(params) => {
            handlers::forge_get_agent_session(state, params, context).await
        }
        McpInput::GetMainAgent(params) => {
            handlers::forge_get_main_agent(state, params, context).await
        }
        McpInput::BindMainAgent(params) => {
            handlers::forge_set_main_agent(state, params, context).await
        }
        McpInput::GetProjectAgent(params) => {
            handlers::forge_get_project_agent(state, params, context).await
        }
        McpInput::BindProjectAgent(params) => {
            handlers::forge_set_project_agent(state, params, context).await
        }
        McpInput::GetIntegrationQueue(params) => {
            handlers::forge_get_integration_queue(state, params, context).await
        }
        McpInput::ListAgentChats(params) => {
            handlers::forge_list_agent_chats(state, params, context).await
        }
        McpInput::GetAgentChat(params) => {
            handlers::forge_get_agent_chat(state, params, context).await
        }
        McpInput::ListAgentChatMessages(params) => {
            handlers::forge_list_agent_chat_messages(state, params, context).await
        }
        McpInput::SendAgentChatMessage(params) => {
            handlers::forge_send_agent_chat_message(state, params, context).await
        }
        McpInput::ListAgentHandoffs(params) => {
            handlers::forge_list_agent_handoffs(state, params, context).await
        }
        McpInput::GetAgentHandoff(params) => {
            handlers::forge_get_agent_handoff(state, params, context).await
        }
        McpInput::CreateAgentHandoff(params) => {
            handlers::forge_create_agent_handoff(state, params, context).await
        }
    }
}
