use db::{Agent, AgentProfile, AgentSession, Execution, Page, Project, Task, TaskRoleAssignment};
use serde_json::{json, Value};

pub(crate) fn execution_page_value(page: Page<Execution>) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page.items.into_iter().map(execution_value).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": has_more,
        "total_count": page.total_count,
    })
}

pub(crate) fn agent_page_value_for_user(page: Page<Agent>, is_admin: bool) -> Value {
    let has_more = page.next_cursor.is_some();
    json!({
        "data": page
            .items
            .into_iter()
            .map(|agent| agent_value_for_user(agent, is_admin))
            .collect::<Vec<_>>(),
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
    let condition = task.condition.public();
    json!({
        "id": task.id,
        "project_id": task.project_id,
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
        "condition": condition,
        "deleted_at": task.deleted_at,
        "version": task.version,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    })
}

pub(crate) fn agent_value_for_user(agent: Agent, is_admin: bool) -> Value {
    let daemon_id = agent.daemon_id.clone().filter(|_| is_admin);
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
        // Daemon identities are sensitive runtime handles. REST redacts this
        // field for non-admins; MCP must do the same for both single-agent
        // and paginated responses.
        "daemon_id": daemon_id,
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

pub(crate) fn task_role_assignment_value(assignment: TaskRoleAssignment) -> Value {
    json!({
        "id": assignment.id,
        "task_id": assignment.task_id,
        "role_name": assignment.role_name,
        "assignee_type": assignment.assignee_type.map(|kind| kind.to_string()),
        "assignee_id": assignment.assignee_id,
        "created_at": assignment.created_at,
        "updated_at": assignment.updated_at,
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

#[cfg(test)]
mod condition_serialization_snapshots {
    use super::*;
    use db::{
        ConditionContinuation, ConditionEvidence, ConditionSource, LegacyConditionField,
        ParkReason, RetryCause, TaskCondition, TerminalOutcome, UnknownConditionProblem,
    };
    #[test]
    fn every_task_condition_kind_has_one_public_shape_without_legacy_fields() {
        let e = ConditionEvidence::default();
        let reason = ParkReason::UnknownCondition {
            source: ConditionSource {
                field: LegacyConditionField::EntryBarrierJson,
                key: None,
            },
            problem: UnknownConditionProblem::UnownedEntry,
        };
        for (kind, condition) in [
            (
                "clear",
                TaskCondition::Clear {
                    evidence: e.clone(),
                },
            ),
            (
                "entering",
                TaskCondition::Entering {
                    state: "review".into(),
                    epoch: 2,
                    step_id: "step".into(),
                    phase: "post_commit_hooks".into(),
                    since: "now".into(),
                    evidence: e.clone(),
                },
            ),
            (
                "running",
                TaskCondition::Running {
                    execution_id: "run".into(),
                    role: "coder".into(),
                    epoch: 2,
                    since: "now".into(),
                    evidence: e.clone(),
                },
            ),
            (
                "deferred",
                TaskCondition::Deferred {
                    until: Some("2099-01-01T00:00:00Z".into()),
                    reason: RetryCause::ExecutionFailure,
                    resume: ConditionContinuation::Dispatch {
                        target_state: "in_progress".into(),
                    },
                    evidence: e.clone(),
                },
            ),
            (
                "parked",
                TaskCondition::Parked {
                    primary: reason.clone(),
                    additional: vec![],
                    resume: ConditionContinuation::Reconcile,
                    since: None,
                    evidence: e.clone(),
                },
            ),
            (
                "failed",
                TaskCondition::Failed {
                    failure: reason,
                    additional: vec![],
                    resume: ConditionContinuation::Reconcile,
                    since: None,
                    evidence: e.clone(),
                },
            ),
            (
                "settled",
                TaskCondition::Settled {
                    outcome: TerminalOutcome::Completed,
                    evidence: e,
                },
            ),
        ] {
            let task: Task = serde_json::from_value(json!({"id":"task","project_id":"project","parent_task_id":null,"assignee_type":null,"assignee_id":null,"title":"Wire snapshot","description":null,"task_type":"task","status":"in_progress","is_automation":false,"priority":0,"board_position":0.0,"subtask_order":null,"task_state_config":null,"merge_config":null,"metadata_json":"{\"secret\":true}","plan":null,"error_annotation":"legacy poison","blocked_json":"legacy poison","failed_json":"legacy poison","entry_barrier_json":null,"review_passed_at":null,"archived_at":null,"deleted_at":null,"version":1,"created_at":"now","updated_at":"now","condition":condition})).unwrap();
            let expected = serde_json::to_value(task.condition.public()).unwrap();
            let value = task_value(task);
            assert_eq!(value["condition"], expected, "{kind}");
            assert_eq!(value["condition"]["kind"], kind);
            for field in [
                "error_annotation",
                "blocked",
                "failed",
                "blocked_json",
                "failed_json",
                "metadata_json",
                "entry_barrier_json",
            ] {
                assert!(value.get(field).is_none(), "{kind}: {field}");
            }
            assert!(value["condition"].get("evidence").is_none());
            // JSON decoded by forge-ctl uses the same api-types enum.
            let decoded: api_types::TaskCondition =
                serde_json::from_value(value["condition"].clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
        }
    }
}
