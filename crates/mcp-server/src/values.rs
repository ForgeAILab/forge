use db::{Agent, AgentProfile, AgentSession, ClaimedTask, Execution, Page, Project, Task};
use serde_json::{json, Value};

pub(crate) fn task_page_value(page: Page<Task>) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page.items.into_iter().map(task_value).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": has_more,
        "total_count": page.total_count,
    })
}

pub(crate) fn execution_page_value(page: Page<Execution>) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page.items.into_iter().map(execution_value).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": has_more,
        "total_count": page.total_count,
    })
}

pub(crate) fn agent_page_value(page: Page<Agent>) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page.items.into_iter().map(agent_value).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": has_more,
        "total_count": page.total_count,
    })
}

pub(crate) fn project_page_value(page: Page<Project>) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page.items.into_iter().map(project_value).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": has_more,
        "total_count": page.total_count,
    })
}

pub(crate) fn task_value(task: Task) -> Value {
    json!({
        "id": task.id,
        "project_id": task.project_id,
        "repo_id": task.repo_id,
        "parent_task_id": task.parent_task_id,
        "subtask_order": task.subtask_order,
        "assignee_type": task.assignee_type,
        "assignee_id": task.assignee_id,
        "title": task.title,
        "description": task.description,
        "status": task.status.to_string(),
        "priority": task.priority,
        "merge_config": json_string(task.merge_config),
        "plan": task.plan,
        "error_annotation": json_string(task.error_annotation),
        "deleted_at": task.deleted_at,
        "version": task.version,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    })
}

pub(crate) fn agent_value(agent: Agent) -> Value {
    json!({
        "id": agent.id,
        "name": agent.name,
        "description": agent.description,
        "profile_id": agent.profile_id,
        "backend_kind": agent.backend_kind,
        "executor_type": agent.executor_type,
        "provider": agent.provider,
        "model": agent.model,
        "reasoning_effort": agent.reasoning_effort,
        "permission_policy": agent.permission_policy,
        "capabilities": safe_json(&agent.capabilities_json),
        "config_json": safe_json(&agent.config_json),
        // Opaque handle only; the protected credential is never serialized.
        "credential_handle_id": agent.credential_ref,
        "daemon_id": agent.daemon_id,
        "max_concurrent_tasks": agent.max_concurrent_tasks,
        "heartbeat_interval_seconds": agent.heartbeat_interval_seconds,
        "max_missed_heartbeats": agent.max_missed_heartbeats,
        "status": agent.status.to_string(),
        "last_heartbeat_at": agent.last_heartbeat_at,
        "is_default": agent.is_default,
        "version": agent.version,
        "created_at": agent.created_at,
        "updated_at": agent.updated_at,
    })
}

pub(crate) fn project_value(project: Project) -> Value {
    let paused = project.paused_at.is_some();
    json!({
        "id": project.id,
        "name": project.name,
        "settings": json_string(Some(project.settings)),
        "workflow_template_name": project.workflow_template_name,
        "version": project.version,
        "paused_at": project.paused_at,
        "paused": paused,
        "created_at": project.created_at,
        "updated_at": project.updated_at,
    })
}

pub(crate) fn agent_profile_value(profile: AgentProfile) -> Value {
    json!({
        "id": profile.id,
        "identity_id": profile.identity_id,
        "backend_kind": profile.backend_kind,
        "executor_type": profile.executor_type,
        "provider": profile.provider,
        "model": profile.model,
        "reasoning_effort": profile.reasoning_effort,
        "permission_policy": profile.permission_policy,
        "system_prompt": profile.prompt_template,
        "capabilities": safe_json(&profile.capabilities_json),
        "tool_policy": safe_json(&profile.tool_policy_json),
        "config": safe_json(&profile.config_json),
        // This is an opaque database handle, never the protected credential.
        "credential_handle_id": profile.credential_ref,
        "version": profile.version,
        "created_at": profile.created_at,
    })
}

pub(crate) fn agent_session_value(session: AgentSession) -> Value {
    json!({
        "id": session.id,
        "identity_id": session.identity_id,
        "profile_id": session.profile_id,
        "context_scope_id": session.context_scope_id,
        "backend_kind": session.backend_kind,
        "status": session.status,
        "capabilities": json_string(Some(session.capabilities_json)),
        "connection_status": session.connection_status,
        "predecessor_session_id": session.predecessor_session_id,
        "replaced_by_session_id": session.replaced_by_session_id,
        "last_activity_at": session.last_activity_at,
        "version": session.version,
        "created_at": session.created_at,
        "updated_at": session.updated_at,
    })
}

pub(crate) fn claimed_task_value(claimed: ClaimedTask) -> Value {
    json!({
        "task": task_value(claimed.task),
        "execution": execution_value(claimed.execution),
    })
}

pub(crate) fn execution_value(execution: Execution) -> Value {
    json!({
        "id": execution.id,
        "task_id": execution.task_id,
        "agent_id": execution.agent_id,
        "role": execution.role.to_string(),
        "status": execution.status.to_string(),
        "parent_execution_id": execution.parent_execution_id,
        "agent_session_id": execution.agent_session_id,
        "agent_message_id": execution.agent_message_id,
        "prompt": execution.prompt,
        "summary": execution.summary,
        "logs_path": execution.logs_path,
        "before_sha": execution.before_sha,
        "after_sha": execution.after_sha,
        "error": execution.error,
        "created_at": execution.created_at,
        "updated_at": execution.updated_at,
    })
}

fn json_string(value: Option<String>) -> Value {
    value
        .map(|value| serde_json::from_str(&value).unwrap_or(Value::String(value)))
        .unwrap_or(Value::Null)
}

fn safe_json(value: &str) -> Value {
    let parsed = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned()));
    redact_sensitive(parsed)
}

fn redact_sensitive(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .filter_map(|(key, value)| {
                    let normalized = key.to_ascii_lowercase();
                    if normalized.contains("credential")
                        || normalized.contains("secret")
                        || normalized.contains("password")
                        || normalized == "token"
                        || normalized.ends_with("_token")
                        || normalized.contains("api_key")
                    {
                        return None;
                    }
                    Some((key, redact_sensitive(value)))
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(redact_sensitive).collect()),
        other => other,
    }
}
