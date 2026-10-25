//! Agent assignment counts, Attention visibility, and health queries.

use super::*;

impl SqliteDb {
    /// Live assignment/turn counts; terminal execution statistics are supplied
    /// by the shared read index on collection routes.
    pub async fn agent_list_active_assignments(
        &self,
        agent_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        let ids_json = serde_json::to_string(agent_ids).map_err(|_| DbError::InvalidTransition)?;
        let rows = sqlx::query(
            "WITH requested AS (SELECT DISTINCT value AS agent_id FROM json_each(?)),
             assignments AS MATERIALIZED (
                 SELECT DISTINCT assignee_id, task_id FROM task_role_assignment
                 WHERE assignee_type = 'agent' AND assignee_id IN requested
             ), workflow_states AS MATERIALIZED (
                 SELECT project.id AS project_id,
                        json_extract(workflow_state.value, '$.name') AS state_name,
                        MAX(CASE WHEN json_extract(workflow_state.value, '$.kind') IN ('active', 'gate')
                            THEN 1 ELSE 0 END) AS active
                 FROM project, json_each(
                     CASE WHEN json_valid(project.workflow_definition)
                         THEN project.workflow_definition ELSE '{\"states\":[]}' END,
                     '$.states') AS workflow_state
                 WHERE project.id IN (SELECT project_id FROM task WHERE id IN (SELECT task_id FROM assignments))
                 GROUP BY project.id, state_name
             ), active_tasks AS MATERIALIZED (
                 SELECT task.id FROM task JOIN project ON project.id = task.project_id
                 LEFT JOIN workflow_states ON workflow_states.project_id = task.project_id
                     AND workflow_states.state_name = task.status
                 WHERE task.deleted_at IS NULL AND task.id IN (SELECT task_id FROM assignments)
                   AND CASE WHEN workflow_states.state_name IS NOT NULL THEN workflow_states.active
                       ELSE task.status IN ('in_progress', 'review', 'merging') END
             ), task_counts AS (
                 SELECT assignee_id AS agent_id, COUNT(*) AS active_tasks
                 FROM assignments WHERE task_id IN active_tasks GROUP BY assignee_id
             ), turn_counts AS (
                 SELECT responder_identity_id AS agent_id, COUNT(*) AS active_turns
                 FROM agent_chat_turn_job WHERE responder_identity_id IN requested
                   AND status IN ('leased', 'running') GROUP BY responder_identity_id
             )
             SELECT requested.agent_id,
                    COALESCE(task_counts.active_tasks, 0) + COALESCE(turn_counts.active_turns, 0) AS active_assigned_task_count
             FROM requested LEFT JOIN task_counts USING(agent_id)
               LEFT JOIN turn_counts USING(agent_id)",
        )
        .bind(ids_json)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get("agent_id")?,
                    row.try_get("active_assigned_task_count")?,
                ))
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct AgentOperatorPressureRow {
    pub agent_id: String,
    pub agent_name: String,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: i64,
    pub running_executions: i64,
}

impl SqliteDb {
    pub async fn agent_operator_pressure_rows(
        &self,
    ) -> crate::Result<Vec<AgentOperatorPressureRow>> {
        let rows = sqlx::query("SELECT
                a.id AS agent_id,
                a.name AS agent_name,
                a.daemon_id,
                a.max_concurrent_tasks,
                (
                    SELECT COUNT(*)
                    FROM execution e
                    WHERE e.agent_id = a.id AND e.status = 'running'
                ) + (
                    SELECT COUNT(*) FROM workspace_placement p
                    WHERE p.agent_id = a.id AND p.state IN ('reserved', 'preparing')
                       AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
                      AND NOT EXISTS (SELECT 1 FROM execution e
                          WHERE e.workspace_id = p.workspace_id AND e.status = 'running')
                ) AS running_executions
             FROM agent_current a
             ORDER BY a.name ASC, a.id ASC")
.fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(AgentOperatorPressureRow {
                    agent_id: row.try_get("agent_id")?,
                    agent_name: row.try_get("agent_name")?,
                    daemon_id: row.try_get("daemon_id")?,
                    max_concurrent_tasks: row.try_get("max_concurrent_tasks")?,
                    running_executions: row.try_get("running_executions")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn retired_machine_task_agent_ids(
        &self,
        daemon_id: &str,
        task_id: &str,
        role_task_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT i.id FROM agent_identity i WHERE i.archived_at IS NOT NULL
         AND i.selected_profile_id IN (SELECT id FROM agent_profile WHERE daemon_id=?)
         AND (EXISTS(SELECT 1 FROM task t WHERE t.id=? AND t.assignee_type='agent' AND t.assignee_id=i.id)
           OR EXISTS(SELECT 1 FROM task_role_assignment r WHERE r.task_id=? AND r.assignee_type='agent' AND r.assignee_id=i.id))")
.bind(daemon_id)
.bind(task_id)
.bind(role_task_id)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn workspace_admission_agent_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                .bind(agent_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_claim_agent_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT version FROM agent_current WHERE id = ?")
                .bind(agent_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn active_project_identity_wake_budget_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        identity_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT wake_budget
                 FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
        )
        .bind(identity_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?)
    }
}

#[derive(Debug)]
pub struct VisibleAttentionAgentBindingsInput<'a> {
    pub identity_id: &'a str,
    pub account_id: &'a str,
    pub member_id: &'a str,
    pub project_identity_id: &'a str,
    pub owner_id: &'a str,
    pub limit: i64,
}
/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionAgentBindingRow {
    pub binding_id: crate::Result<String>,
    pub binding_type: crate::Result<String>,
    pub project_id: crate::Result<Option<String>>,
    pub chat_id: crate::Result<String>,
    pub state: crate::Result<String>,
    pub subscriptions_json: crate::Result<String>,
    pub wake_budget: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn visible_attention_agent_bindings(
        &self,
        input: VisibleAttentionAgentBindingsInput<'_>,
    ) -> crate::Result<Vec<AttentionAgentBindingRow>> {
        let rows = sqlx::query(
            "SELECT binding_id, binding_type, project_id, chat_id, state,
                    subscriptions_json, wake_budget
             FROM (
                 SELECT b.id AS binding_id,
                        'main' AS binding_type,
                        NULL AS project_id,
                        c.id AS chat_id,
                        b.state,
                        '[]' AS subscriptions_json,
                        0 AS wake_budget
                 FROM account_main_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'account_main' AND c.account_id = b.account_id
                 WHERE b.identity_id = ? AND b.account_id = ?
                   AND b.state <> 'revoked'
                 UNION ALL
                 SELECT b.id AS binding_id,
                        'project' AS binding_type,
                        b.project_id,
                        c.id AS chat_id,
                        b.state,
                        b.subscriptions_json,
                        b.wake_budget
                 FROM project_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'project' AND c.project_id = b.project_id
                 JOIN project p ON p.id = b.project_id
                 LEFT JOIN project_member pm
                   ON pm.project_id = b.project_id AND pm.user_id = ?
                 WHERE b.identity_id = ? AND b.state <> 'revoked'
                   AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
             )
             ORDER BY project_id ASC, binding_type ASC, binding_id ASC LIMIT ?",
        )
        .bind(input.identity_id)
        .bind(input.account_id)
        .bind(input.member_id)
        .bind(input.project_identity_id)
        .bind(input.owner_id)
        .bind(input.limit)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AttentionAgentBindingRow {
                    binding_id: row.try_get("binding_id").map_err(crate::DbError::from),
                    binding_type: row.try_get("binding_type").map_err(crate::DbError::from),
                    project_id: row.try_get("project_id").map_err(crate::DbError::from),
                    chat_id: row.try_get("chat_id").map_err(crate::DbError::from),
                    state: row.try_get("state").map_err(crate::DbError::from),
                    subscriptions_json: row
                        .try_get("subscriptions_json")
                        .map_err(crate::DbError::from),
                    wake_budget: row.try_get("wake_budget").map_err(crate::DbError::from),
                })
            })
            .collect()
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionAgentSessionRow {
    pub session_id: crate::Result<String>,
    pub scope_type: crate::Result<String>,
    pub scope_id: crate::Result<String>,
    pub backend_kind: crate::Result<String>,
    pub status: crate::Result<String>,
    pub connection_status: crate::Result<String>,
    pub last_activity_at: crate::Result<Option<String>>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_agent_sessions(
        &self,
        identity_id: &str,
    ) -> crate::Result<Vec<AttentionAgentSessionRow>> {
        let rows = sqlx::query(
            "SELECT s.id AS session_id, c.scope_type, c.scope_id,
                    s.backend_kind, s.status, s.connection_status,
                    s.last_activity_at, s.updated_at
             FROM agent_session s
             JOIN agent_context_scope c ON c.id = s.context_scope_id
             WHERE s.identity_id = ?
             ORDER BY s.updated_at DESC, s.id DESC",
        )
        .bind(identity_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AttentionAgentSessionRow {
                    session_id: row.try_get("session_id").map_err(crate::DbError::from),
                    scope_type: row.try_get("scope_type").map_err(crate::DbError::from),
                    scope_id: row.try_get("scope_id").map_err(crate::DbError::from),
                    backend_kind: row.try_get("backend_kind").map_err(crate::DbError::from),
                    status: row.try_get("status").map_err(crate::DbError::from),
                    connection_status: row
                        .try_get("connection_status")
                        .map_err(crate::DbError::from),
                    last_activity_at: row
                        .try_get("last_activity_at")
                        .map_err(crate::DbError::from),
                    updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn attention_actor_identity_count(&self, identity_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                .bind(identity_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_project_responder_identity(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                     WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_main_responder_identity(
        &self,
        account_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM account_main_agent_binding
                     WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_responder_identity(
        &self,
        account_id: Option<String>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM account_main_agent_binding
                             WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_responder_identity(
        &self,
        project_id: Option<String>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT identity_id FROM project_agent_binding
                             WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_requested_identity_count(
        &self,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                .bind(identity_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_main_identity_binding_count(
        &self,
        account_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_main_binding_count(&self, account_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_identity_binding_count(
        &self,
        project_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_binding_count(&self, project_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_identity_binding_count(
        &self,
        account_id: String,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_main_binding_count(
        &self,
        account_id: String,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND state = 'active'",
        )
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_identity_binding_count(
        &self,
        project_id: String,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
        )
        .bind(project_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_chat_project_binding_count(
        &self,
        project_id: String,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND state = 'active'
                                   AND identity_id IS NOT NULL",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_account_identity_access_count(
        &self,
        identity_id: &str,
        account_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_identity
                 WHERE id = ? AND (owner_id IS NULL OR owner_id = ?)",
        )
        .bind(identity_id)
        .bind(account_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_project_identity_access_count(
        &self,
        identity_id: &str,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
        )
        .bind(identity_id)
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionIdentityOwnerRow {
    pub owner_id: crate::Result<Option<String>>,
}

impl SqliteDb {
    pub async fn attention_identity_owner(
        &self,
        identity_id: &str,
    ) -> crate::Result<Option<AttentionIdentityOwnerRow>> {
        let rows = sqlx::query("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(identity_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionIdentityOwnerRow {
                owner_id: row.try_get("owner_id").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_identity_visible_project_count(
        &self,
        member_id: &str,
        identity_id: &str,
        owner_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM project_agent_binding m
             JOIN project p ON p.id = m.project_id
             LEFT JOIN project_member pm ON pm.project_id = m.project_id AND pm.user_id = ?
             WHERE m.identity_id = ?
               AND m.state = 'active'
               AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)",
        )
        .bind(member_id)
        .bind(identity_id)
        .bind(owner_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub(crate) fn attention_agent_visibility_predicate(
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => (
                "EXISTS (
                    SELECT 1 FROM project_agent_binding pm
                    WHERE pm.identity_id = a.id AND pm.project_id = ? AND pm.state = 'active'
                )"
                .to_owned(),
                vec![project_id.to_owned()],
            ),
            None => (
                "(a.owner_id IS NULL OR a.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_agent_binding pam
                    JOIN project p ON p.id = pam.project_id
                    LEFT JOIN project_member pm ON pm.project_id = p.id AND pm.user_id = ?
                    WHERE pam.identity_id = a.id AND pam.state = 'active'
                      AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleAgentHealthRow {
    pub id: crate::Result<String>,
    pub name: crate::Result<String>,
    pub backend_kind: crate::Result<Option<String>>,
    pub provider: crate::Result<Option<String>>,
    pub model: crate::Result<Option<String>>,
    pub status: crate::Result<String>,
    pub paused: crate::Result<i64>,
    pub last_heartbeat_at: crate::Result<Option<String>>,
    pub connection_status: crate::Result<Option<String>>,
    pub active_session_count: crate::Result<i64>,
    pub project_count: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_visible_agent_health(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleAgentHealthRow>> {
        let (predicate, values) = Self::attention_agent_visibility_predicate(user_id, project_id);
        let (project_count, count_values) = if let Some(project_id) = project_id {
            (
                "COUNT(DISTINCT CASE WHEN m.project_id = ? THEN m.project_id END)".to_owned(),
                vec![project_id.to_owned()],
            )
        } else {
            (
                "COUNT(DISTINCT CASE
                    WHEN p2.owner_id IS NULL OR p2.owner_id = ? OR pm2.user_id IS NOT NULL
                    THEN m.project_id END)"
                    .to_owned(),
                vec![user_id.to_owned()],
            )
        };
        let (active_session_count, session_count_values) = match project_id {
            Some(project_id) => (
                "COUNT(DISTINCT CASE WHEN session_scope.project_id = ? THEN s.id END)".to_owned(),
                vec![project_id.to_owned()],
            ),
            None => ("COUNT(DISTINCT s.id)".to_owned(), Vec::new()),
        };
        let sql = format!(
            "SELECT a.id, a.name, a.backend_kind, a.provider, a.model,
                    a.status, a.paused, a.last_heartbeat_at,
                    h.status AS connection_status,
                    {active_session_count} AS active_session_count,
                    {project_count} AS project_count
             FROM agent_current a
             LEFT JOIN agent_connection_health h ON h.profile_id = a.profile_id
             LEFT JOIN agent_session s ON s.identity_id = a.id
                 AND s.status IN ('starting', 'ready', 'running', 'degraded')
             LEFT JOIN agent_context_scope session_scope
                 ON session_scope.id = s.context_scope_id
             LEFT JOIN project_agent_binding m ON m.identity_id = a.id AND m.state = 'active'
             LEFT JOIN project p2 ON p2.id = m.project_id
             LEFT JOIN project_member pm2 ON pm2.project_id = m.project_id AND pm2.user_id = ?
             WHERE {predicate}
             GROUP BY a.id, a.name, a.backend_kind, a.provider, a.model,
                      a.status, a.paused, a.last_heartbeat_at, h.status
             ORDER BY a.name ASC, a.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in session_count_values {
            query = query.bind(value);
        }
        for value in count_values {
            query = query.bind(value);
        }
        query = query.bind(user_id);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleAgentHealthRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                name: row.try_get("name").map_err(crate::DbError::from),
                backend_kind: row.try_get("backend_kind").map_err(crate::DbError::from),
                provider: row.try_get("provider").map_err(crate::DbError::from),
                model: row.try_get("model").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                paused: row.try_get("paused").map_err(crate::DbError::from),
                last_heartbeat_at: row
                    .try_get("last_heartbeat_at")
                    .map_err(crate::DbError::from),
                connection_status: row
                    .try_get("connection_status")
                    .map_err(crate::DbError::from),
                active_session_count: row
                    .try_get("active_session_count")
                    .map_err(crate::DbError::from),
                project_count: row.try_get("project_count").map_err(crate::DbError::from),
            })
            .collect())
    }
}

impl SqliteDb {
    pub async fn attention_visible_active_session_count(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<i64> {
        let (agent_predicate, agent_values) =
            Self::attention_agent_visibility_predicate(user_id, project_id);
        let session_sql = format!(
            "SELECT COUNT(*) FROM agent_session s
             JOIN agent_current a ON a.id = s.identity_id
             WHERE s.status IN ('starting', 'ready', 'running', 'degraded')
               AND {agent_predicate}"
        );
        let mut session_query = sqlx::query_scalar::<_, i64>(&session_sql);
        for value in agent_values {
            session_query = session_query.bind(value);
        }
        let active_sessions = session_query.fetch_one(self.pool()).await?;
        Ok(active_sessions)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct AgentStatusHealthRow {
    row: sqlx::sqlite::SqliteRow,
}

impl AgentStatusHealthRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn connection_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("connection_status")?)
    }
    pub fn credential_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("credential_status")?)
    }
    pub fn credential_enabled(&self) -> crate::Result<Option<bool>> {
        Ok(self.row.try_get("credential_enabled")?)
    }
    pub fn provider_status(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("provider_status")?)
    }
    pub fn last_error_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("last_error_kind")?)
    }
    pub fn backoff_until(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("backoff_until")?)
    }
    pub fn reservations(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("reservations")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct CliRuntimePolicyStatusRow {
    row: sqlx::sqlite::SqliteRow,
}

impl CliRuntimePolicyStatusRow {
    pub fn owner_user_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("owner_user_id")?)
    }
    pub fn daemon_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("daemon_id")?)
    }
    pub fn executor_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("executor_type")?)
    }
    pub fn enabled(&self) -> crate::Result<bool> {
        Ok(self.row.try_get("enabled")?)
    }
}

impl SqliteDb {
    pub async fn agent_status_health_for_requested_agents(
        &self,
        requested_json: &str,
    ) -> crate::Result<Vec<AgentStatusHealthRow>> {
        let rows = sqlx::query("WITH requested AS (
            SELECT json_extract(value,'$.id') AS id,
                   json_extract(value,'$.profile') AS profile,
                   json_extract(value,'$.credential') AS credential FROM json_each(?)
         ), reservations AS (
            SELECT p.agent_id, COUNT(*) AS n FROM workspace_placement p
            WHERE p.agent_id IN (SELECT id FROM requested)
              AND p.state IN ('reserved','preparing')
              AND julianday(COALESCE(p.reserved_until,datetime(p.updated_at,'+10 minutes'))) > julianday('now')
              AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id=p.workspace_id AND e.status='running')
            GROUP BY p.agent_id
         )
         SELECT requested.id, h.status AS connection_status,
                c.status AS credential_status, c.enabled AS credential_enabled,
                p.status AS provider_status, p.last_error_kind, p.backoff_until,
                COALESCE(reservations.n,0) AS reservations
         FROM requested LEFT JOIN agent_connection_health h ON h.profile_id=requested.profile
         LEFT JOIN credential_handle c ON c.id=requested.credential
         LEFT JOIN provider_entry_health p ON p.credential_id=requested.credential
         LEFT JOIN reservations ON reservations.agent_id=requested.id")
            .bind(requested_json)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| AgentStatusHealthRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn cli_runtime_policies_for_executor_types(
        &self,
        executor_types_json: &str,
    ) -> crate::Result<Vec<CliRuntimePolicyStatusRow>> {
        let rows = sqlx::query("SELECT owner_user_id,daemon_id,executor_type,enabled FROM cli_runtime_policy WHERE executor_type IN (SELECT value FROM json_each(?))")
            .bind(executor_types_json)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| CliRuntimePolicyStatusRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn owner_cli_runtime_enabled(
        &self,
        owner_user_id: &str,
        daemon_id: &str,
        executor_type: &str,
    ) -> crate::Result<Option<bool>> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT enabled FROM cli_runtime_policy
             WHERE owner_user_id = ? AND daemon_id = ? AND executor_type = ?",
        )
        .bind(owner_user_id)
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn global_cli_runtime_enabled(
        &self,
        daemon_id: &str,
        executor_type: &str,
    ) -> crate::Result<Option<bool>> {
        Ok(sqlx::query_scalar::<_, Option<bool>>(
            "SELECT MIN(enabled) FROM cli_runtime_policy
         WHERE daemon_id = ? AND executor_type = ?",
        )
        .bind(daemon_id)
        .bind(executor_type)
        .fetch_one(self.pool())
        .await?)
    }
}
