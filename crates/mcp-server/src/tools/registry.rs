//! Server-loaded facts and the typed MCP projection of the operation registry.
use super::handlers;
use crate::{error::McpToolError, protocol::McpContext, state::AppState};
use db::{
    AgentChatRepo, AgentHandoffRepo, AgentRepo, AgentSessionRepo, ProjectMemberRepo, ProjectRepo,
    UserRepo,
};
use operation_registry::{
    authority::{
        mcp_authority, mcp_scope_rule, AuthorityRequirement, EffectiveAuthority, PrincipalRule,
        RequiredPermission,
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
    McpToolError::new(-32602, "invalid params").with_data(
        json!({"code":"mcp_contract_invalid", "operation":error.operation, "details":correction}),
    )
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
    if let Some(user) = UserRepo::get_user_by_id(&*state.db, user_id).await? {
        if user.is_admin {
            authority.ceiling.insert("mcp_account_admin".into());
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
    if let Some(spec) = operation_registry::mcp::lookup(name) {
        authority.evaluate(spec).map_err(|_| denied())
    } else {
        authority
            .evaluate(
                mcp_scope_rule(name)
                    .ok_or_else(|| McpToolError::protocol(-32601, "method not found"))?,
            )
            .map_err(|_| denied())
    }
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
        ResourceRule::None => {}
    }
    Ok(())
}

pub(crate) async fn dispatch(
    state: &AppState,
    input: McpInput,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    match input {
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
