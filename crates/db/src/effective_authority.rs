//! Server fact loader for the shared EffectiveAuthority resolver. No policy
//! parsing or operation decisions live here; transactions retain their CAS.
use crate::{Result, SqliteDb};
use operation_registry::authority::{
    parse_permissions, scope_permissions, AuthorityFacts, EffectiveAuthority, PermissionSet,
    Principal, TaskRole,
};
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};

impl SqliteDb {
    pub async fn resolve_effective_authority(
        &self,
        identity_id: &str,
        admitted_profile_id: Option<&str>,
        scope_type: &str,
        scope_id: &str,
        workspace: &str,
    ) -> Result<EffectiveAuthority> {
        let mut connection = self.pool().acquire().await?;
        load(
            &mut connection,
            identity_id,
            admitted_profile_id,
            scope_type,
            scope_id,
            workspace,
            None,
        )
        .await
    }
    /// Bind Task records to the one running execution of this native session.
    pub async fn resolve_task_effective_authority(
        &self,
        identity_id: &str,
        admitted_profile_id: Option<&str>,
        task_id: &str,
        workspace: &str,
        runtime_session_id: &str,
    ) -> Result<EffectiveAuthority> {
        let mut connection = self.pool().acquire().await?;
        load(
            &mut connection,
            identity_id,
            admitted_profile_id,
            "task",
            task_id,
            workspace,
            Some(runtime_session_id),
        )
        .await
    }
    pub async fn resolve_effective_authority_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        identity_id: &str,
        admitted_profile_id: Option<&str>,
        scope_type: &str,
        scope_id: &str,
        workspace: &str,
    ) -> Result<EffectiveAuthority> {
        load(
            transaction,
            identity_id,
            admitted_profile_id,
            scope_type,
            scope_id,
            workspace,
            None,
        )
        .await
    }
}
async fn load(
    connection: &mut SqliteConnection,
    identity_id: &str,
    admitted_profile_id: Option<&str>,
    scope_type: &str,
    scope_id: &str,
    workspace: &str,
    runtime_session_id: Option<&str>,
) -> Result<EffectiveAuthority> {
    let row = sqlx::query(
            "SELECT i.owner_id, i.paused, i.archived_at, i.account_permission_ceiling,
                    p.id AS profile_id, p.tool_policy_json,
                    selected.tool_policy_json AS selected_policy
             FROM agent_identity i
             JOIN agent_profile p ON p.identity_id = i.id AND p.id = COALESCE(?, i.selected_profile_id)
             LEFT JOIN agent_profile selected ON selected.id = i.selected_profile_id AND selected.identity_id = i.id
             WHERE i.id = ?")
            .bind(admitted_profile_id).bind(identity_id).fetch_optional(&mut *connection).await?
            .ok_or(crate::DbError::NotFound)?;
    let owner: Option<String> = row.try_get("owner_id")?;
    let chat = if scope_type == "agent_chat" {
        Some(
            sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                .bind(scope_id)
                .fetch_optional(&mut *connection)
                .await?
                .ok_or(crate::DbError::NotFound)?,
        )
    } else {
        None
    };
    let chat_project = chat.as_ref().and_then(|chat| {
        chat.try_get::<Option<String>, _>("project_id")
            .ok()
            .flatten()
    });
    let project_id = if scope_type == "project" {
        Some(scope_id.to_owned())
    } else {
        chat_project.clone()
    };
    let mut active = row.try_get::<i64, _>("paused")? == 0
        && row.try_get::<Option<String>, _>("archived_at")?.is_none();
    active &= match scope_type {
        "account" => owner.as_deref() == Some(scope_id),
        "agent" => identity_id == scope_id,
        "agent_chat" if chat_project.is_none() => chat.as_ref().is_some_and(|chat| {
            chat.try_get::<String, _>("kind").ok().as_deref() == Some("account_main")
                && owner.is_some()
                && chat
                    .try_get::<Option<String>, _>("account_id")
                    .ok()
                    .flatten()
                    == owner
        }),
        "project" | "agent_chat" | "task" => true,
        _ => false,
    };
    let mut binding_id = None;
    let mut setup = false;
    // A stored document that fails to parse grants nothing. That is correct,
    // but an agent that silently lost every tool is undiagnosable, so name
    // the identity and the layer where it happens.
    let layer = |name: &str, document: &str| match parse_permissions(document) {
        Ok(set) => set,
        Err(error) => {
            tracing::warn!(
                identity_id,
                layer = name,
                code = error.code(),
                "stored permission document grants no authority"
            );
            PermissionSet::new()
        }
    };
    let mut layers = vec![
        layer(
            "account_permission_ceiling",
            &row.try_get::<String, _>("account_permission_ceiling")?,
        ),
        layer(
            "admitted_profile_tool_policy",
            &row.try_get::<String, _>("tool_policy_json")?,
        ),
    ];
    // Selected policy can revoke the admitted profile, never widen it.
    if let Some(policy) = row.try_get::<Option<String>, _>("selected_policy")? {
        layers.push(layer("selected_profile_tool_policy", &policy));
    } else {
        active = false;
    }
    let principal = if scope_type == "task" {
        // Every Task read or record has one execution principal. A direct read
        // without a session is allowed only when that principal is unambiguous.
        let executions = sqlx::query(
            "SELECT e.id, e.role, e.agent_session_id FROM execution e
             WHERE e.task_id = ? AND e.agent_id = ? AND e.status = 'running'
               AND (? IS NULL OR e.agent_session_id IS NULL OR e.agent_session_id = ?)
               AND (? IS NULL OR EXISTS (
                 SELECT 1 FROM agent_session s JOIN agent_context_scope c ON c.id=s.context_scope_id
                 WHERE s.runtime_session_id=? AND s.identity_id=e.agent_id
                   AND c.scope_type='task' AND c.scope_id=e.task_id AND c.task_id=e.task_id
                   AND ((c.task_role IN ('worker','coder','executor') AND e.role IN ('worker','coder','executor')) OR c.task_role=e.role)))
             ORDER BY e.created_at DESC, e.id DESC LIMIT 2",
        )
        .bind(scope_id)
        .bind(identity_id)
        .bind(runtime_session_id)
        .bind(runtime_session_id)
        .bind(runtime_session_id)
        .bind(runtime_session_id)
        .fetch_all(&mut *connection)
        .await?;
        let execution = if executions.len() == 1 {
            executions.first()
        } else {
            None
        };
        let role = execution
            .and_then(|e| e.try_get::<String, _>("role").ok())
            .and_then(|role| TaskRole::parse(&role));
        active &= execution.is_some() && role.is_some();
        let session = if let Some(runtime) = runtime_session_id {
            sqlx::query(
                "SELECT s.profile_id, c.task_role, c.workspace_access
                FROM agent_session s JOIN agent_context_scope c ON c.id = s.context_scope_id
                WHERE s.runtime_session_id = ? AND s.identity_id = ?
                  AND c.scope_type = 'task' AND c.scope_id = ? AND c.task_id = ?",
            )
            .bind(runtime)
            .bind(identity_id)
            .bind(scope_id)
            .bind(scope_id)
            .fetch_optional(&mut *connection)
            .await?
        } else {
            None
        };
        if runtime_session_id.is_some() {
            active &= session.as_ref().is_some_and(|session| {
                let scope_role = session
                    .try_get::<String, _>("task_role")
                    .ok()
                    .and_then(|role| TaskRole::parse(&role));
                scope_role == role
                    && session
                        .try_get::<String, _>("workspace_access")
                        .ok()
                        .as_deref()
                        == Some(workspace)
                    && session.try_get::<String, _>("profile_id").ok()
                        == row.try_get::<String, _>("profile_id").ok()
            });
        }
        // Preserve the optional Project-binding ceiling a Task session held
        // before Task authority joined this resolver.
        if let Some(binding) = sqlx::query("SELECT b.id, b.permission_ceiling_json FROM project_agent_binding b JOIN task t ON t.project_id = b.project_id WHERE t.id = ? AND b.identity_id = ? AND b.state = 'active'")
            .bind(scope_id).bind(identity_id).fetch_optional(&mut *connection).await? {
            binding_id = Some(binding.try_get("id")?);
            layers.push(layer("project_binding_permission_ceiling", &binding.try_get::<String, _>("permission_ceiling_json")?));
        }
        Principal::TaskAgent {
            identity_id: identity_id.to_owned(),
            execution_id: execution
                .and_then(|e| e.try_get("id").ok())
                .unwrap_or_default(),
            role: role.unwrap_or(TaskRole::Reviewer),
            runtime_session_id: runtime_session_id.map(str::to_owned),
        }
    } else if let Some(project_id) = project_id {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(&mut *connection)
            .await?;
        if exists == 0 {
            return Err(crate::DbError::NotFound);
        }
        let binding = sqlx::query("SELECT b.id, b.permission_ceiling_json, p.charter_setup_required FROM project_agent_binding b JOIN project p ON p.id = b.project_id WHERE b.project_id = ? AND b.identity_id = ? AND b.state = 'active'")
                .bind(&project_id).bind(identity_id).fetch_optional(&mut *connection).await?;
        if let Some(binding) = binding {
            binding_id = Some(binding.try_get("id")?);
            setup = binding.try_get::<i64, _>("charter_setup_required")? != 0;
            layers.push(layer(
                "project_binding_permission_ceiling",
                &binding.try_get::<String, _>("permission_ceiling_json")?,
            ));
        } else {
            active = false;
        }
        Principal::ProjectAgent {
            identity_id: identity_id.to_owned(),
            project_id,
        }
    } else {
        // Inquiry sessions are account-scoped and intentionally read-only.
        // A Main Chat additionally needs the exact active account binding.
        if scope_type == "agent_chat" {
            binding_id = sqlx::query_scalar("SELECT b.id FROM account_main_agent_binding b JOIN agent_chat c ON c.account_id = b.account_id AND c.kind = 'account_main' WHERE c.id = ? AND b.identity_id = ? AND b.account_id IS ? AND b.state = 'active'")
                .bind(scope_id).bind(identity_id).bind(owner.as_deref()).fetch_optional(&mut *connection).await?;
        } else if scope_type == "account" {
            binding_id = sqlx::query_scalar("SELECT id FROM account_main_agent_binding WHERE account_id IS ? AND identity_id = ? AND account_id = ? AND state = 'active'")
                .bind(owner.as_deref()).bind(identity_id).bind(scope_id).fetch_optional(&mut *connection).await?;
            // An owned identity without the Main binding keeps the account
            // ceiling it always had (public research and the legacy account
            // operations are filtered against it). Every registered Main
            // operation still requires the binding through its principal
            // rule, so the missing binding grants nothing new.
        }
        Principal::MainAgent {
            identity_id: identity_id.to_owned(),
        }
    };
    layers.push(if scope_type == "agent" {
        ["read_account", "propose_message"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    } else {
        scope_permissions(scope_type, workspace, chat_project.is_some(), setup)
    });
    Ok(EffectiveAuthority::resolve(AuthorityFacts {
        principal,
        scope_type: scope_type.to_owned(),
        scope_id: scope_id.to_owned(),
        profile_id: row.try_get("profile_id")?,
        layers,
        binding_id,
        setup_required: setup,
        active,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn profile_write_rejects_conflicting_permissions_before_persisting() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = crate::now_rfc3339();
        let input = crate::CreateAgentProfile {
            id: "profile".into(),
            identity_id: "identity".into(),
            backend_kind: "cli".into(),
            executor_type: "codex".into(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".into(),
            tool_policy_json: r#"{"permissions":["read_project"],"allowed":["propose_task"]}"#
                .into(),
            config_json: "{}".into(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        };
        let error = crate::AgentProfileRepo::create_profile(&db, input)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::DbError::PermissionDocument(
                operation_registry::authority::PermissionDocumentError::Conflicting
            )
        ));
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_profile WHERE id = 'profile'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
}
