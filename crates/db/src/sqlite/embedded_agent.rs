use crate::{
    AgentConnectionHealth, AgentConnectionHealthRepo, AgentContextScope, AgentContextScopeRepo,
    AgentSession, AgentSessionRepo, CreateAgentContextScope, CreateAgentSession, CredentialHandle,
    CredentialHandleRepo, CredentialUsage, DbError, Result, RotateAgentSession, SqliteDb,
    UpdateAgentSession, UpsertAgentConnectionHealth,
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};

#[async_trait]
impl CredentialHandleRepo for SqliteDb {
    async fn get_credential_handle(&self, id: &str) -> Result<Option<CredentialHandle>> {
        sqlx::query("SELECT * FROM credential_handle WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_credential_handle)
            .transpose()
    }

    async fn list_credential_handles(&self, owner_user_id: &str) -> Result<Vec<CredentialHandle>> {
        sqlx::query(
            "SELECT * FROM credential_handle
             WHERE owner_user_id = ?
             ORDER BY created_at DESC, id DESC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_credential_handle)
        .collect()
    }

    async fn rename_credential_handle(
        &self,
        id: &str,
        owner_user_id: &str,
        label: &str,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<CredentialHandle> {
        let result = sqlx::query(
            "UPDATE credential_handle
             SET label = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ? AND version = ?",
        )
        .bind(label)
        .bind(updated_at)
        .bind(id)
        .bind(owner_user_id)
        .bind(expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            let exists =
                sqlx::query("SELECT id FROM credential_handle WHERE id = ? AND owner_user_id = ?")
                    .bind(id)
                    .bind(owner_user_id)
                    .fetch_optional(&self.pool)
                    .await?
                    .is_some();
            return Err(if exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        self.get_credential_handle(id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_credential_usage(&self, owner_user_id: &str) -> Result<Vec<CredentialUsage>> {
        let rows = sqlx::query(
            "SELECT ap.credential_ref AS credential_id,
                    ai.id AS agent_id,
                    ai.name AS agent_name,
                    ap.executor_type AS runtime,
                    (SELECT MAX(s.last_activity_at)
                       FROM agent_session s
                      WHERE s.identity_id = ai.id) AS last_used_at
             FROM agent_identity ai
             JOIN agent_profile ap ON ap.id = ai.selected_profile_id
             WHERE ap.credential_ref IS NOT NULL
               AND ai.owner_id = ?
             ORDER BY ai.name ASC, ai.id ASC",
        )
        .bind(owner_user_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(CredentialUsage {
                    credential_id: row.try_get("credential_id")?,
                    agent_id: row.try_get("agent_id")?,
                    agent_name: row.try_get("agent_name")?,
                    runtime: row.try_get("runtime")?,
                    last_used_at: row.try_get("last_used_at")?,
                })
            })
            .collect()
    }

    async fn revoke_credential_handle(
        &self,
        id: &str,
        owner_user_id: &str,
        updated_at: &str,
    ) -> Result<CredentialHandle> {
        let result = sqlx::query(
            "UPDATE credential_handle
             SET status = 'revoked', version = version + 1, updated_at = ?
             WHERE id = ? AND owner_user_id = ?",
        )
        .bind(updated_at)
        .bind(id)
        .bind(owner_user_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        self.get_credential_handle(id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

#[async_trait]
impl AgentContextScopeRepo for SqliteDb {
    async fn create_context_scope(
        &self,
        input: CreateAgentContextScope,
    ) -> Result<AgentContextScope> {
        if !matches!(
            input.scope_type.as_str(),
            "account" | "project" | "agent_chat" | "task"
        ) {
            return Err(DbError::Check(
                "unsupported canonical agent context scope".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO agent_context_scope (
                id, identity_id, scope_type, scope_id, project_id,
                task_id, task_role, workspace_access, authority_json,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(identity_id, scope_type, scope_id) DO NOTHING",
        )
        .bind(&input.id)
        .bind(&input.identity_id)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(input.project_id.as_deref())
        .bind(input.task_id.as_deref())
        .bind(input.task_role.as_deref())
        .bind(&input.workspace_access)
        .bind(&input.authority_json)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;

        let scope = self
            .get_context_scope_for_identity(&input.identity_id, &input.scope_type, &input.scope_id)
            .await?
            .ok_or(DbError::NotFound)?;
        if scope.project_id != input.project_id
            || scope.task_id != input.task_id
            || scope.task_role != input.task_role
            || scope.workspace_access != input.workspace_access
            || scope.authority_json != input.authority_json
        {
            return Err(DbError::Check(
                "canonical context scope already exists with different authority".to_owned(),
            ));
        }
        Ok(scope)
    }

    async fn get_context_scope(&self, id: &str) -> Result<Option<AgentContextScope>> {
        sqlx::query(
            "SELECT * FROM agent_context_scope
             WHERE id = ? AND scope_type IN ('account', 'project', 'agent_chat', 'task')",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_context_scope)
        .transpose()
    }

    async fn get_context_scope_for_identity(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<Option<AgentContextScope>> {
        if !matches!(scope_type, "account" | "project" | "agent_chat" | "task") {
            return Ok(None);
        }
        sqlx::query(
            "SELECT * FROM agent_context_scope
             WHERE identity_id = ? AND scope_type = ? AND scope_id = ?",
        )
        .bind(identity_id)
        .bind(scope_type)
        .bind(scope_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_context_scope)
        .transpose()
    }

    async fn list_context_scopes(&self, identity_id: &str) -> Result<Vec<AgentContextScope>> {
        sqlx::query(
            "SELECT * FROM agent_context_scope
             WHERE identity_id = ?
               AND scope_type IN ('account', 'project', 'agent_chat', 'task')
             ORDER BY created_at DESC, id DESC",
        )
        .bind(identity_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_context_scope)
        .collect()
    }
}

#[async_trait]
impl AgentSessionRepo for SqliteDb {
    async fn create_agent_session(&self, input: CreateAgentSession) -> Result<AgentSession> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        insert_agent_session(&mut transaction, &input).await?;
        transaction.commit().await?;
        self.get_agent_session(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_agent_session(&self, id: &str) -> Result<Option<AgentSession>> {
        sqlx::query("SELECT * FROM agent_session WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_session)
            .transpose()
    }

    async fn list_agent_sessions(&self, identity_id: &str) -> Result<Vec<AgentSession>> {
        sqlx::query(
            "SELECT * FROM agent_session
             WHERE identity_id = ?
             ORDER BY created_at DESC, id DESC",
        )
        .bind(identity_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_session)
        .collect()
    }

    async fn get_active_agent_session(
        &self,
        identity_id: &str,
        context_scope_id: &str,
    ) -> Result<Option<AgentSession>> {
        sqlx::query(
            "SELECT * FROM agent_session
             WHERE identity_id = ? AND context_scope_id = ?
               AND status IN ('starting', 'ready', 'running', 'degraded')
             ORDER BY created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(identity_id)
        .bind(context_scope_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_session)
        .transpose()
    }

    async fn update_agent_session(&self, input: UpdateAgentSession) -> Result<AgentSession> {
        let existing = self
            .get_agent_session(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if existing.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let result = sqlx::query(
            "UPDATE agent_session SET
                runtime_session_id = ?, status = ?, connection_status = ?, last_activity_at = ?,
                version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(
            input
                .runtime_session_id
                .unwrap_or(existing.runtime_session_id),
        )
        .bind(input.status.as_deref().unwrap_or(&existing.status))
        .bind(
            input
                .connection_status
                .as_deref()
                .unwrap_or(&existing.connection_status),
        )
        .bind(input.last_activity_at.unwrap_or(existing.last_activity_at))
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        self.get_agent_session(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn rotate_agent_session(&self, input: RotateAgentSession) -> Result<AgentSession> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE agent_session
             SET status = 'replaced', version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND status IN ('starting', 'ready', 'running', 'suspended', 'degraded', 'failed', 'cancelled')",
        )
        .bind(&input.replacement.updated_at)
        .bind(&input.previous_session_id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            let exists =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_session WHERE id = ?")
                    .bind(&input.previous_session_id)
                    .fetch_one(&mut *transaction)
                    .await?
                    > 0;
            return Err(if exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        insert_agent_session(&mut transaction, &input.replacement).await?;
        sqlx::query(
            "UPDATE agent_session
             SET replaced_by_session_id = ?
             WHERE id = ? AND status = 'replaced'",
        )
        .bind(&input.replacement.id)
        .bind(&input.previous_session_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.get_agent_session(&input.replacement.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn suspend_stale_native_sessions(&self, updated_at: &str) -> Result<u64> {
        // Startup-time bulk sweep. Every affected row still bumps `version`
        // so any in-flight optimistic writer (`WHERE version = ?`) conflicts
        // instead of silently reviving a suspended session.
        let result = sqlx::query(
            "UPDATE agent_session
             SET status = 'suspended',
                 connection_status = 'unknown',
                 version = version + 1,
                 updated_at = ?
             WHERE backend_kind = 'native'
               AND status IN ('starting', 'ready', 'running', 'degraded')",
        )
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl AgentConnectionHealthRepo for SqliteDb {
    async fn upsert_connection_health(
        &self,
        input: UpsertAgentConnectionHealth,
    ) -> Result<AgentConnectionHealth> {
        sqlx::query(
            "INSERT INTO agent_connection_health (
                profile_id, status, capability_status_json, checked_at, error_code, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(profile_id) DO UPDATE SET
                status = excluded.status,
                capability_status_json = excluded.capability_status_json,
                checked_at = excluded.checked_at,
                error_code = excluded.error_code,
                updated_at = excluded.updated_at",
        )
        .bind(&input.profile_id)
        .bind(&input.status)
        .bind(&input.capability_status_json)
        .bind(input.checked_at.as_deref())
        .bind(input.error_code.as_deref())
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;
        self.get_connection_health(&input.profile_id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_connection_health(
        &self,
        profile_id: &str,
    ) -> Result<Option<AgentConnectionHealth>> {
        sqlx::query("SELECT * FROM agent_connection_health WHERE profile_id = ?")
            .bind(profile_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_connection_health)
            .transpose()
    }
}

async fn insert_agent_session(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentSession,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_session (
            id, identity_id, profile_id, context_scope_id, backend_kind,
            runtime_session_id, status, capabilities_json, connection_status,
            predecessor_session_id, last_activity_at, version, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.identity_id)
    .bind(&input.profile_id)
    .bind(&input.context_scope_id)
    .bind(&input.backend_kind)
    .bind(input.runtime_session_id.as_deref())
    .bind(&input.status)
    .bind(&input.capabilities_json)
    .bind(&input.connection_status)
    .bind(input.predecessor_session_id.as_deref())
    .bind(input.last_activity_at.as_deref())
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn map_credential_handle(row: SqliteRow) -> Result<CredentialHandle> {
    Ok(CredentialHandle {
        id: row.try_get("id")?,
        owner_user_id: row.try_get("owner_user_id")?,
        provider: row.try_get("provider")?,
        label: row.try_get("label")?,
        status: row.try_get("status")?,
        credential_method: row.try_get("credential_method")?,
        metadata_json: row.try_get("metadata_json")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_context_scope(row: SqliteRow) -> Result<AgentContextScope> {
    Ok(AgentContextScope {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        project_id: row.try_get("project_id")?,
        task_id: row.try_get("task_id")?,
        task_role: row.try_get("task_role")?,
        workspace_access: row.try_get("workspace_access")?,
        authority_json: row.try_get("authority_json")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_session(row: SqliteRow) -> Result<AgentSession> {
    Ok(AgentSession {
        id: row.try_get("id")?,
        identity_id: row.try_get("identity_id")?,
        profile_id: row.try_get("profile_id")?,
        context_scope_id: row.try_get("context_scope_id")?,
        backend_kind: row.try_get("backend_kind")?,
        runtime_session_id: row.try_get("runtime_session_id")?,
        status: row.try_get("status")?,
        capabilities_json: row.try_get("capabilities_json")?,
        connection_status: row.try_get("connection_status")?,
        predecessor_session_id: row.try_get("predecessor_session_id")?,
        replaced_by_session_id: row.try_get("replaced_by_session_id")?,
        last_activity_at: row.try_get("last_activity_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_connection_health(row: SqliteRow) -> Result<AgentConnectionHealth> {
    Ok(AgentConnectionHealth {
        profile_id: row.try_get("profile_id")?,
        status: row.try_get("status")?,
        capability_status_json: row.try_get("capability_status_json")?,
        checked_at: row.try_get("checked_at")?,
        error_code: row.try_get("error_code")?,
        updated_at: row.try_get("updated_at")?,
    })
}
