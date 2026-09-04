mod descriptors;
mod handlers;

use serde_json::Value;

use crate::{error::McpToolError, protocol::McpContext, state::AppState};

pub(crate) use descriptors::tool_descriptors;

// NOTE: role reassignment (POST/DELETE /api/v1/tasks/{id}/roles/...) is intentionally
// NOT exposed as an MCP tool. Agents must not re-route work between themselves or
// between agent and human through MCP. Any future change that adds such a tool
// requires its own authorization spec.
pub(crate) async fn dispatch_tool(
    state: &AppState,
    name: &str,
    arguments: Value,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    match name {
        "forge_create_task" => handlers::forge_create_task(state, arguments).await,
        "forge_create_sub_tasks" => handlers::forge_create_sub_tasks(state, arguments).await,
        "forge_add_task_dependency" => handlers::forge_add_task_dependency(state, arguments).await,
        "forge_remove_task_dependency" => {
            handlers::forge_remove_task_dependency(state, arguments).await
        }
        "forge_list_task_dependencies" => {
            handlers::forge_list_task_dependencies(state, arguments).await
        }
        "forge_list_tasks" => handlers::forge_list_tasks(state, arguments).await,
        "forge_get_task" => handlers::forge_get_task(state, arguments).await,
        "forge_preview_prompt" => handlers::forge_preview_prompt(state, arguments).await,
        "forge_memory_search" => handlers::forge_memory_search(state, arguments, context).await,
        "forge_memory_get" => handlers::forge_memory_get(state, arguments, context).await,
        "forge_assign_agent" => handlers::forge_assign_agent(state, arguments).await,
        "forge_cancel_task" => handlers::forge_cancel_task(state, arguments).await,
        "forge_get_task_diff" => handlers::forge_get_task_diff(state, arguments).await,
        "forge_list_executions" => handlers::forge_list_executions(state, arguments).await,
        "forge_update_task" => handlers::forge_update_task(state, arguments).await,
        "forge_transition_task" => handlers::forge_transition_task(state, arguments).await,
        "forge_register_agent" => handlers::forge_register_agent(state, arguments).await,
        "forge_list_agents" => handlers::forge_list_agents(state, arguments).await,
        "forge_list_projects" => handlers::forge_list_projects(state, arguments, context).await,
        "forge_get_project" => handlers::forge_get_project(state, arguments).await,
        "forge_create_project" => handlers::forge_create_project(state, arguments, context).await,
        "forge_update_project" => handlers::forge_update_project(state, arguments).await,
        "forge_update_project_lifecycle_hooks" => {
            handlers::forge_update_project_lifecycle_hooks(state, arguments).await
        }
        "forge_follow_up_execution" => handlers::forge_follow_up_execution(state, arguments).await,
        "forge_list_agent_profiles" => {
            handlers::forge_list_agent_profiles(state, arguments, context).await
        }
        "forge_list_agent_sessions" => {
            handlers::forge_list_agent_sessions(state, arguments, context).await
        }
        "forge_get_agent_session" => {
            handlers::forge_get_agent_session(state, arguments, context).await
        }
        "forge_get_main_agent" => handlers::forge_get_main_agent(state, arguments, context).await,
        "forge_set_main_agent" => handlers::forge_set_main_agent(state, arguments, context).await,
        "forge_get_project_agent" => {
            handlers::forge_get_project_agent(state, arguments, context).await
        }
        "forge_set_project_agent" => {
            handlers::forge_set_project_agent(state, arguments, context).await
        }
        "forge_list_agent_chats" => {
            handlers::forge_list_agent_chats(state, arguments, context).await
        }
        "forge_get_agent_chat" => handlers::forge_get_agent_chat(state, arguments, context).await,
        "forge_list_agent_chat_messages" => {
            handlers::forge_list_agent_chat_messages(state, arguments, context).await
        }
        "forge_send_agent_chat_message" => {
            handlers::forge_send_agent_chat_message(state, arguments, context).await
        }
        "forge_list_agent_handoffs" => {
            handlers::forge_list_agent_handoffs(state, arguments, context).await
        }
        "forge_get_agent_handoff" => {
            handlers::forge_get_agent_handoff(state, arguments, context).await
        }
        "forge_create_agent_handoff" => {
            handlers::forge_create_agent_handoff(state, arguments, context).await
        }
        _ => Err(McpToolError::new(-32601, "method not found")),
    }
}
