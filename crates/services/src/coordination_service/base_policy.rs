//! Frozen 49839d1c policy oracle, test-only. Do not update to match the implementation.
use super::*;

async fn action_scope_access(
    db: &SqliteDb,
    actor_identity_id: &str,
    scope_type: &str,
    scope_id: &str,
    requested_permission: &str,
) -> Result<Option<String>> {
    let owner_id =
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(actor_identity_id)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| ServiceError::not_found("agent identity", actor_identity_id))?;

    match scope_type {
        "account" => {
            if owner_id.as_deref() != Some(scope_id) {
                return Ok(Some(
                    "actor identity is not owned by the requested account scope".to_owned(),
                ));
            }
        }
        "project" => {
            let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
                .bind(scope_id)
                .fetch_one(db.pool())
                .await?;
            if exists == 0 {
                return Err(ServiceError::not_found("project", scope_id));
            }
            let binding = sqlx::query(
                "SELECT permission_ceiling_json FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
            )
            .bind(scope_id)
            .bind(actor_identity_id)
            .fetch_optional(db.pool())
            .await?;
            let Some(binding) = binding else {
                return Ok(Some(
                    "actor identity has no active binding in the requested Project".to_owned(),
                ));
            };
            let permission_ceiling: String = binding.try_get("permission_ceiling_json")?;
            if !permission_set(&permission_ceiling).contains(requested_permission) {
                return Ok(Some(
                    "requested permission is outside the Project binding ceiling".to_owned(),
                ));
            }
        }
        "agent_chat" => {
            let chat =
                sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                    .bind(scope_id)
                    .fetch_optional(db.pool())
                    .await?;
            let Some(chat) = chat else {
                return Err(ServiceError::not_found("agent_chat", scope_id));
            };
            let kind: String = chat.try_get("kind")?;
            match kind.as_str() {
                "account_main" => {
                    let account_id: Option<String> = chat.try_get("account_id")?;
                    if owner_id != account_id {
                        return Ok(Some(
                            "actor identity does not own the requested Main Agent Chat".to_owned(),
                        ));
                    }
                }
                "project" => {
                    let project_id: Option<String> = chat.try_get("project_id")?;
                    let Some(project_id) = project_id else {
                        return Ok(Some("Agent Chat has no Project binding".to_owned()));
                    };
                    let binding = sqlx::query_scalar::<_, String>(
                        "SELECT permission_ceiling_json FROM project_agent_binding
                         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
                    )
                    .bind(project_id)
                    .bind(actor_identity_id)
                    .fetch_optional(db.pool())
                    .await?;
                    let Some(permission_ceiling) = binding else {
                        return Ok(Some(
                            "actor identity has no active Agent Chat binding".to_owned(),
                        ));
                    };
                    if !permission_set(&permission_ceiling).contains(requested_permission) {
                        return Ok(Some(
                            "requested permission is outside the Agent Chat binding ceiling"
                                .to_owned(),
                        ));
                    }
                }
                _ => return Ok(Some("Agent Chat kind is not admitted".to_owned())),
            }
        }
        "task" => {
            let task_exists = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM task WHERE id = ? AND deleted_at IS NULL",
            )
            .bind(scope_id)
            .fetch_one(db.pool())
            .await?;
            if task_exists == 0 {
                return Err(ServiceError::not_found("task", scope_id));
            }
            let task =
                sqlx::query("SELECT status, assignee_type, assignee_id FROM task WHERE id = ?")
                    .bind(scope_id)
                    .fetch_one(db.pool())
                    .await?;
            let direct_assignee_type: Option<String> = task.try_get("assignee_type")?;
            let direct_assignee_id: Option<String> = task.try_get("assignee_id")?;
            let status: String = task.try_get("status")?;
            let assignments = sqlx::query(
                "SELECT role_name FROM task_role_assignment
                 WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?",
            )
            .bind(scope_id)
            .bind(actor_identity_id)
            .fetch_all(db.pool())
            .await?;
            let assigned = direct_assignee_type.as_deref() == Some("agent")
                && direct_assignee_id.as_deref() == Some(actor_identity_id)
                || !assignments.is_empty();
            if !assigned {
                return Ok(Some(
                    "actor identity is not assigned to the requested Task".to_owned(),
                ));
            }
            if requested_permission == "task_write"
                && matches!(status.as_str(), "done" | "cancelled")
            {
                return Ok(Some(
                    "the Task workflow no longer admits writes in its terminal state".to_owned(),
                ));
            }
            let roles = assignments
                .iter()
                .filter_map(|row| row.try_get::<String, _>("role_name").ok())
                .collect::<Vec<_>>();
            if requested_permission == "task_write"
                && !roles.is_empty()
                && roles
                    .iter()
                    .all(|role| role.eq_ignore_ascii_case("reviewer"))
            {
                return Ok(Some(
                    "reviewer assignments cannot perform Task writes".to_owned(),
                ));
            }
            if requested_permission == "propose_review"
                && !roles.iter().any(|role| {
                    role.eq_ignore_ascii_case("reviewer") || role.eq_ignore_ascii_case("review")
                })
            {
                return Ok(Some(
                    "the Task assignment does not admit review proposals".to_owned(),
                ));
            }
        }
        "agent" if actor_identity_id != scope_id => {
            return Ok(Some(
                "actor identity cannot act through another identity scope".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(None)
}

pub(super) async fn evaluate_action_policy(
    db: &SqliteDb,
    actor_identity_id: &str,
    scope_type: &str,
    scope_id: &str,
    requested_permission: &str,
    operation: &str,
    payload_json: Option<&str>,
) -> Result<(AgentActionPolicyResult, Option<String>)> {
    if let Some(reason) = action_scope_access(
        db,
        actor_identity_id,
        scope_type,
        scope_id,
        requested_permission,
    )
    .await?
    {
        return Ok((AgentActionPolicyResult::Denied, Some(reason)));
    }
    let row = sqlx::query(
        "SELECT paused, archived_at, account_permission_ceiling, selected_profile_id
         FROM agent_identity WHERE id = ?",
    )
    .bind(actor_identity_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::not_found("agent identity", actor_identity_id))?;
    let paused: i64 = row.try_get("paused")?;
    let archived_at: Option<String> = row.try_get("archived_at")?;
    if paused != 0 || archived_at.is_some() {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("actor identity is paused or archived".to_owned()),
        ));
    }
    let account_policy: String = row.try_get("account_permission_ceiling")?;
    let profile_id: Option<String> = row.try_get("selected_profile_id")?;
    let profile_policy = if let Some(profile_id) = profile_id {
        sqlx::query_scalar::<_, String>(
            "SELECT tool_policy_json FROM agent_profile WHERE id = ? AND identity_id = ?",
        )
        .bind(profile_id)
        .bind(actor_identity_id)
        .fetch_optional(db.pool())
        .await?
        .unwrap_or_else(|| "{}".to_owned())
    } else {
        "{}".to_owned()
    };
    let scope_permissions = scope_permissions(db, scope_type, scope_id).await?;
    let account_permissions = permission_set(&account_policy);
    let profile_permissions = permission_set(&profile_policy);
    let authorized = account_permissions.contains(requested_permission)
        && profile_permissions.contains(requested_permission)
        && scope_permissions.contains(requested_permission);
    if !authorized {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some(format!(
                "permission {requested_permission} is outside the server-issued identity/profile/scope ceiling"
            )),
        ));
    }
    if is_project_orchestration_mutation(operation) {
        let project_id = match scope_type {
            "project" => Some(scope_id.to_owned()),
            "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
                "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project' LIMIT 1",
            )
            .bind(scope_id)
            .fetch_optional(db.pool())
            .await?
            .flatten(),
            _ => None,
        };
        let Some(project_id) = project_id else {
            return Ok((
                AgentActionPolicyResult::Denied,
                Some("Project orchestration requires a bound Project scope".to_owned()),
            ));
        };
        let state = sqlx::query(
            "SELECT charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", scope_id.to_owned()))?;
        let charter_status: String = state.try_get("charter_status")?;
        let setup_required: i64 = state.try_get("charter_setup_required")?;
        let has_charter = state
            .try_get::<Option<String>, _>("current_charter_id")?
            .is_some()
            && state
                .try_get::<Option<String>, _>("current_charter_revision_id")?
                .is_some();
        let is_setup = charter_status == "legacy_unverified" && setup_required != 0;
        let is_charter_amendment =
            charter_status == "charter_backed" && setup_required == 0 && has_charter;
        if operation == PROJECT_CHARTER_ADOPTION_OPERATION {
            if !is_setup && !is_charter_amendment {
                return Ok((
                    AgentActionPolicyResult::Denied,
                    Some(
                        "Project Charter adoption is not valid for the current Project state"
                            .to_owned(),
                    ),
                ));
            }
        } else if charter_status != "charter_backed" || setup_required != 0 || !has_charter {
            return Ok((
                AgentActionPolicyResult::Denied,
                Some("Project orchestration remains blocked until a user-approved Charter adoption is committed".to_owned()),
            ));
        }
    }
    // Classify after the scope/Charter gate above.  The payload is part of the
    // canonical operation identity: a reversible Project subaction can be a
    // direct command while an authority action in the same coarse descriptor
    // remains an approval-backed AgentAction.
    let payload_value =
        payload_json.and_then(|payload| serde_json::from_str::<Value>(payload).ok());
    let classification = classify_operation(operation, payload_value.as_ref());
    let direct_allowed = is_admitted_direct_command(operation, requested_permission, payload_json);
    if matches!(classification, OperationClassification::Denied) {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("operation is denied by the canonical native operation catalog".to_owned()),
        ));
    }
    if matches!(classification, OperationClassification::Query) {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("query operations execute through the read boundary".to_owned()),
        ));
    }
    if matches!(classification, OperationClassification::DirectCommand) && !direct_allowed {
        return Ok((
            AgentActionPolicyResult::Denied,
            Some("direct command is not admitted for this permission or payload".to_owned()),
        ));
    }
    let approval_required = matches!(
        classification,
        OperationClassification::ApprovalRequiredAction
    ) || requested_permission == "task_write"
        || operation.starts_with("protected.");
    Ok(if approval_required {
        (
            AgentActionPolicyResult::ApprovalRequired,
            Some("protected mutation requires an independent approval".to_owned()),
        )
    } else {
        (AgentActionPolicyResult::Allowed, None)
    })
}

async fn scope_permissions(
    db: &SqliteDb,
    scope_type: &str,
    scope_id: &str,
) -> Result<BTreeSet<String>> {
    let mut values: Vec<&str> = match scope_type {
        "account" => &[
            "read_account",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
        ][..],
        "project" => &[
            "read_project",
            "read_memory",
            "propose_project",
            "propose_task",
            "propose_message",
            "propose_commitment",
            "propose_memory",
            "propose_review",
            "propose_decision",
            "propose_session",
        ][..],
        "agent_chat" => &["read_agent_chat", "read_memory"][..],
        "task" => &["read_task", "read_memory", "task_read", "task_write"][..],
        "agent" => &["read_account", "propose_message"][..],
        _ => &[][..],
    }
    .to_vec();
    if scope_type == "agent_chat"
        && sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             WHERE id = ? AND kind = 'project' AND project_id IS NOT NULL",
        )
        .bind(scope_id)
        .fetch_one(db.pool())
        .await?
            > 0
    {
        // The owning Project and active binding are checked separately by
        // action_scope_access.  Only a server-resolved Project Chat receives
        // this extra capability; Main Chat remains permanently denied.
        values.extend([
            "propose_message",
            "propose_project",
            "propose_task",
            "propose_commitment",
            "propose_memory",
            "propose_session",
        ]);
    } else if scope_type == "agent_chat"
        && sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_chat
             WHERE id = ? AND kind = 'account_main' AND account_id IS NOT NULL",
        )
        .bind(scope_id)
        .fetch_one(db.pool())
        .await?
            > 0
    {
        // Main Chat receives only global discovery/project organization and
        // explicit handoff proposals. It never receives Project Task tools.
        values.extend(["propose_discovery", "propose_project", "propose_handoff"]);
    }
    let project_id = match scope_type {
        "project" => Some(scope_id.to_owned()),
        "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
            "SELECT project_id FROM agent_chat
             WHERE id = ? AND kind = 'project' LIMIT 1",
        )
        .bind(scope_id)
        .fetch_optional(db.pool())
        .await?
        .flatten(),
        _ => None,
    };
    if let Some(project_id) = project_id {
        let setup_required = sqlx::query_scalar::<_, i64>(
            "SELECT charter_setup_required FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .is_some_and(|value| value != 0);
        if setup_required {
            values.retain(|permission| {
                matches!(
                    *permission,
                    "read_project"
                        | "read_agent_chat"
                        | "read_memory"
                        | "propose_message"
                        | "propose_project"
                )
            });
        }
    }
    Ok(values.into_iter().map(str::to_owned).collect())
}
