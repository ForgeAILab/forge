use db::{ExecutionRepo, ProjectRepo, TaskDependencyRepo, TaskRepo};
use serde_json::{json, Value};

use crate::{
    error::McpToolError,
    params::{parse_params, ToolCallParams},
    protocol::McpContext,
    state::AppState,
    tools::{dispatch_tool, tool_descriptors},
};

#[cfg(test)]
pub(crate) async fn dispatch(
    state: &AppState,
    method: &str,
    params: Value,
) -> Result<Value, McpToolError> {
    dispatch_with_context(
        state,
        &McpContext {
            project_id: None,
            user_id: Some("mcp-test-user".to_owned()),
        },
        method,
        params,
    )
    .await
}

pub(crate) async fn dispatch_with_context(
    state: &AppState,
    context: &McpContext,
    method: &str,
    params: Value,
) -> Result<Value, McpToolError> {
    match method {
        "initialize" => handle_initialize(),
        "notifications/initialized" => Ok(Value::Null),
        "tools/list" => handle_tools_list(context),
        "tools/call" => {
            let params: ToolCallParams = parse_params(params).map_err(|error| {
                error.with_call_context(
                    "tools/call",
                    context.project_id.as_deref(),
                    context.user_id.as_deref(),
                )
            })?;
            if !known_tool(&params.name, context.project_id.is_some()) {
                return Err(McpToolError::protocol(-32601, "method not found"));
            }
            let arguments = match params.arguments {
                Value::Null => json!({}),
                arguments => arguments,
            };
            let arguments = apply_project_scope(state, &params.name, arguments, context)
                .await
                .map_err(|error| {
                    error.with_call_context(
                        &params.name,
                        context.project_id.as_deref(),
                        context.user_id.as_deref(),
                    )
                })?;
            let result = match dispatch_tool(state, &params.name, arguments.clone(), context).await
            {
                Ok(result) => result,
                Err(error) => {
                    let error =
                        enrich_authorized_conflict(state, &params.name, &arguments, context, error)
                            .await;
                    return Err(error.with_call_context(
                        &params.name,
                        context.project_id.as_deref(),
                        context.user_id.as_deref(),
                    ));
                }
            };
            Ok(tool_call_result(result))
        }
        _ => Err(McpToolError::protocol(-32601, "method not found")),
    }
}

fn known_tool(name: &str, scoped_project: bool) -> bool {
    tool_descriptors(scoped_project)
        .as_array()
        .is_some_and(|descriptors| {
            descriptors
                .iter()
                .any(|descriptor| descriptor.get("name").and_then(Value::as_str) == Some(name))
        })
}

/// Attach a current version only after the target has been resolved inside
/// the authenticated user's allowed Project scope. A raw database conflict does not carry an
/// object identity, and returning a guessed id would leak or misdirect the
/// model's retry; unresolved conflicts therefore remain intentionally opaque.
async fn enrich_authorized_conflict(
    state: &AppState,
    tool_name: &str,
    arguments: &Value,
    context: &McpContext,
    error: McpToolError,
) -> McpToolError {
    if error.is_protocol() || !error.is_version_conflict() {
        return error;
    }
    let Some(user_id) = context.user_id.as_deref() else {
        return error;
    };
    let Some(arguments) = arguments.as_object() else {
        return error;
    };
    if matches!(
        tool_name,
        "forge_update_project" | "forge_update_project_lifecycle_hooks"
    ) {
        let Some(project_id) = arguments.get("project_id").and_then(Value::as_str) else {
            return error;
        };
        if context
            .project_id
            .as_deref()
            .is_some_and(|scope| scope != project_id)
        {
            return error;
        }
        let Ok(Some(project)) =
            ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id).await
        else {
            return error;
        };
        return error.with_authorized_current_target(
            "project",
            project.id,
            "version",
            project.version,
        );
    }
    let Some(task_id) = task_version_target(tool_name, arguments) else {
        return error;
    };
    let Ok(Some(task)) = TaskRepo::get_by_id(&*state.db, task_id, false).await else {
        return error;
    };
    if context
        .project_id
        .as_deref()
        .is_some_and(|scope| scope != task.project_id)
    {
        return error;
    }
    if !matches!(
        ProjectRepo::get_visible_by_id(&*state.db, &task.project_id, user_id).await,
        Ok(Some(_))
    ) {
        return error;
    }
    error.with_authorized_current_target("task", task.id, "version", task.version)
}

fn task_version_target<'a>(
    tool_name: &str,
    arguments: &'a serde_json::Map<String, Value>,
) -> Option<&'a str> {
    let field = match tool_name {
        "forge_update_task" | "forge_transition_task" => "task_id",
        _ => return None,
    };
    arguments
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

async fn apply_project_scope(
    state: &AppState,
    tool_name: &str,
    mut arguments: Value,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let user_id = context
        .user_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| McpToolError::new(-32001, "authenticated MCP user is required"))?;
    if let Some(project_id) = context.project_id.as_deref() {
        assert_project_access(state, project_id, user_id).await?;
    }

    let object = arguments
        .as_object_mut()
        .ok_or_else(|| McpToolError::new(-32602, "tool arguments must be an object"))?;

    if tool_accepts_project_id(tool_name) {
        if let Some(project_id) = context.project_id.as_deref() {
            match object.get("project_id").and_then(Value::as_str) {
                Some(existing) if existing == project_id => {}
                Some(_) => {
                    return Err(McpToolError::new(
                        -32602,
                        "project_id does not match scoped MCP project",
                    )
                    .with_data(json!({ "project_id": project_id })));
                }
                None => {
                    object.insert(
                        "project_id".to_owned(),
                        Value::String(project_id.to_owned()),
                    );
                }
            }
        }
        if let Some(project_id) = object
            .get("project_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
        {
            assert_project_access(state, project_id, user_id).await?;
            if tool_name == "forge_create_task" {
                if let Some(parent_id) = object
                    .get("parent_task_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                {
                    assert_task_access(state, Some(project_id), parent_id, user_id).await?;
                }
            }
        }
        return Ok(arguments);
    }

    if let Some(field_name) = task_scope_field(tool_name) {
        let task_id = required_string_arg(object, field_name)?;
        assert_task_access(state, context.project_id.as_deref(), task_id, user_id).await?;
        if matches!(
            tool_name,
            "forge_add_task_dependency" | "forge_remove_task_dependency"
        ) {
            let depends_on_id = required_string_arg(object, "depends_on_id")?;
            assert_task_access(state, context.project_id.as_deref(), depends_on_id, user_id)
                .await?;
        }
        if tool_name == "forge_list_task_dependencies" {
            for depends_on_id in TaskDependencyRepo::list_dependencies(&*state.db, task_id).await? {
                assert_task_access(
                    state,
                    context.project_id.as_deref(),
                    &depends_on_id,
                    user_id,
                )
                .await?;
            }
        }
        return Ok(arguments);
    }

    if tool_name == "forge_follow_up_execution" {
        let execution_id = required_string_arg(object, "execution_id")?;
        assert_execution_access(state, context.project_id.as_deref(), execution_id, user_id)
            .await?;
    }

    Ok(arguments)
}

fn tool_accepts_project_id(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "forge_create_task"
            | "forge_list_tasks"
            | "forge_get_project"
            | "forge_update_project"
            | "forge_update_project_lifecycle_hooks"
            | "forge_memory_search"
            | "forge_get_project_agent"
            | "forge_set_project_agent"
            | "forge_list_agent_handoffs"
            | "forge_get_agent_handoff"
            | "forge_create_agent_handoff"
    )
}

fn task_scope_field(tool_name: &str) -> Option<&'static str> {
    match tool_name {
        "forge_get_task"
        | "forge_preview_prompt"
        | "forge_assign_agent"
        | "forge_cancel_task"
        | "forge_get_task_diff"
        | "forge_list_executions"
        | "forge_update_task"
        | "forge_transition_task"
        | "forge_add_task_dependency"
        | "forge_remove_task_dependency"
        | "forge_list_task_dependencies" => Some("task_id"),
        "forge_create_sub_tasks" => Some("parent_task_id"),
        _ => None,
    }
}

fn required_string_arg<'a>(
    arguments: &'a serde_json::Map<String, Value>,
    field_name: &str,
) -> Result<&'a str, McpToolError> {
    arguments
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| McpToolError::new(-32602, format!("missing required field `{field_name}`")))
}

async fn assert_task_access(
    state: &AppState,
    project_id: Option<&str>,
    task_id: &str,
    user_id: &str,
) -> Result<(), McpToolError> {
    let task = TaskRepo::get_by_id(&*state.db, task_id, false)
        .await?
        .ok_or_else(|| McpToolError::not_found("task", task_id.to_owned()))?;
    if ProjectRepo::get_visible_by_id(&*state.db, &task.project_id, user_id)
        .await?
        .is_none()
    {
        return Err(McpToolError::not_found("task", task_id.to_owned()));
    }
    if project_id.is_some_and(|scope| task.project_id != scope) {
        return Err(
            McpToolError::new(-32602, "task does not belong to scoped MCP project").with_data(
                json!({
                    "project_id": project_id,
                    "task_id": task_id,
                }),
            ),
        );
    }
    Ok(())
}

async fn assert_execution_access(
    state: &AppState,
    project_id: Option<&str>,
    execution_id: &str,
    user_id: &str,
) -> Result<(), McpToolError> {
    let execution = ExecutionRepo::get_by_id(&*state.db, execution_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("execution", execution_id.to_owned()))?;
    assert_task_access(state, project_id, &execution.task_id, user_id).await
}

async fn assert_project_access(
    state: &AppState,
    project_id: &str,
    user_id: &str,
) -> Result<(), McpToolError> {
    ProjectRepo::get_visible_by_id(&*state.db, project_id, user_id)
        .await?
        .ok_or_else(|| McpToolError::not_found("project", project_id.to_owned()))?;
    Ok(())
}

fn handle_initialize() -> Result<Value, McpToolError> {
    Ok(json!({
        "protocolVersion": "2025-03-26",
        "serverInfo": {
            "name": "forge-mcp",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "capabilities": {
            "tools": {},
        },
    }))
}

fn handle_tools_list(context: &McpContext) -> Result<Value, McpToolError> {
    Ok(json!({ "tools": tool_descriptors(context.project_id.is_some()) }))
}

fn tool_call_result(result: Value) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string(&result).unwrap_or_else(|_| result.to_string()),
            }
        ],
    })
}
