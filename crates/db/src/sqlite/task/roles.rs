//! Task role assignment and creation queries.

use super::*;

pub struct TaskRoleAssignmentSourceRow {
    pub id: crate::Result<String>,
    pub task_id: crate::Result<String>,
    pub role_name: crate::Result<String>,
    pub assignee_type: crate::Result<Option<String>>,
    pub assignee_id: crate::Result<Option<String>>,
    pub created_at: crate::Result<String>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn agent_role_sweep_assignments_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Vec<TaskRoleAssignmentSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at FROM task_role_assignment WHERE assignee_type = 'agent' AND assignee_id = ? ORDER BY task_id, role_name",
        ).bind(agent_id).fetch_all(connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskRoleAssignmentSourceRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                task_id: row.try_get("task_id").map_err(crate::DbError::from),
                role_name: row.try_get("role_name").map_err(crate::DbError::from),
                assignee_type: row.try_get("assignee_type").map_err(crate::DbError::from),
                assignee_id: row.try_get("assignee_id").map_err(crate::DbError::from),
                created_at: row.try_get("created_at").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

impl SqliteDb {
    pub async fn previous_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<Option<TaskRoleAssignmentSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| TaskRoleAssignmentSourceRow {
            id: row.try_get("id").map_err(crate::DbError::from),
            task_id: row.try_get("task_id").map_err(crate::DbError::from),
            role_name: row.try_get("role_name").map_err(crate::DbError::from),
            assignee_type: row.try_get("assignee_type").map_err(crate::DbError::from),
            assignee_id: row.try_get("assignee_id").map_err(crate::DbError::from),
            created_at: row.try_get("created_at").map_err(crate::DbError::from),
            updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
        }))
    }
}

impl SqliteDb {
    pub async fn agent_role_sweep_task_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        agent_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT id FROM task WHERE deleted_at IS NULL AND (assignee_id=? OR id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=?))").bind(agent_id).bind(agent_id).fetch_all(connection).await?)
    }
}

impl SqliteDb {
    pub async fn refresh_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE task_role_assignment SET updated_at = ? WHERE task_id = ? AND role_name = ?",
        )
        .bind(updated_at)
        .bind(task_id)
        .bind(role_name)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn upsert_idle_task_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO task_role_assignment
                (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(task_id, role_name) DO UPDATE SET
                assignee_type = excluded.assignee_type,
                assignee_id = excluded.assignee_id,
                updated_at = excluded.updated_at",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.role_name)
        .bind(input.assignee_type.as_ref().map(ToString::to_string))
        .bind(input.assignee_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn invalidate_idle_task_assignment_review_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE task
             SET review_passed_at = NULL, updated_at = ?, version = version + 1
             WHERE id = ? AND deleted_at IS NULL AND version = ?",
        )
        .bind(updated_at)
        .bind(task_id)
        .bind(version)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn invalidate_deleted_agent_task_authority_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task
                 SET review_passed_at = NULL,
                     version = version + CASE WHEN review_passed_at IS NOT NULL THEN 1 ELSE 0 END,
                     metadata_json = CASE
                         WHEN json_valid(COALESCE(metadata_json, '{}'))
                         THEN NULLIF(json_remove(COALESCE(metadata_json, '{}'), '$.dispatch_disposition', '$.deferred_dispatch'), '{}')
                         ELSE metadata_json
                     END,
                     updated_at = CASE
                         WHEN review_passed_at IS NOT NULL
                              OR (json_valid(COALESCE(metadata_json, '{}'))
                                  AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                       OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL))
                         THEN ? ELSE updated_at END
                 WHERE id = ? AND deleted_at IS NULL
                   AND (review_passed_at IS NOT NULL
                        OR (json_valid(COALESCE(metadata_json, '{}'))
                            AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
                                 OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL)))").bind(updated_at).bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_deleted_agent_task_assignee_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
        agent_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET assignee_id=NULL,updated_at=? WHERE id=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(agent_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_deleted_agent_role_assignments_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        updated_at: String,
        agent_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task_role_assignment SET assignee_id=NULL,updated_at=? WHERE task_id=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(agent_id).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn clear_removed_lead_role_assignment(
        &self,
        task_id: &str,
        updated_at: String,
        role_name: &str,
        agent_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task_role_assignment SET assignee_id=NULL,updated_at=? WHERE task_id=? AND role_name=? AND assignee_type='agent' AND assignee_id=?").bind(updated_at).bind(task_id).bind(role_name).bind(agent_id).identity_fenced().execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn integration_task_waited_owner(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.owner_wait.daemon_id') END FROM task WHERE id=?",
        ).bind(task_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn consume_applied_task_integration_pause_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?").bind(updated_at).bind(task_id).bind(version).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn clear_workspace_admission_environment_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET metadata_json = json_remove(CASE WHEN json_extract(metadata_json, '$.deferred_dispatch.kind') = 'environment_not_ready' THEN json_remove(metadata_json, '$.deferred_dispatch') ELSE metadata_json END, '$.environment_wait') WHERE id = ? AND json_valid(metadata_json) AND json_type(metadata_json, '$.environment_wait') IS NOT NULL").bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

#[derive(Debug)]
pub struct TaskCreationParentRoleRow {
    pub role_name: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub backend_kind: Option<String>,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for TaskCreationParentRoleRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            role_name: row.try_get(0)?,
            assignee_type: row.try_get(1)?,
            assignee_id: row.try_get(2)?,
            backend_kind: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn task_creation_parent_roles_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        parent_task_id: &str,
    ) -> crate::Result<Vec<TaskCreationParentRoleRow>> {
        Ok(sqlx::query_as::<_, TaskCreationParentRoleRow>(
            "SELECT a.role_name, a.assignee_type, a.assignee_id,
                        (SELECT backend_kind FROM agent_current WHERE id = a.assignee_id)
                 FROM task_role_assignment a WHERE a.task_id = ?",
        )
        .bind(parent_task_id)
        .fetch_all(connection)
        .await?)
    }
}

pub struct InsertInitialExplicitTaskRoleInTxInput<'a> {
    pub id: String,
    pub task_id: &'a str,
    pub role_name: String,
    pub assignee_type: String,
    pub assignee_id: String,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_initial_explicit_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertInitialExplicitTaskRoleInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)").bind(input.id).bind(input.task_id).bind(input.role_name).bind(input.assignee_type).bind(input.assignee_id).bind(input.created_at).bind(input.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn insert_initial_default_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        assignment: crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)").bind(assignment.id).bind(task_id).bind(assignment.role_name).bind(assignment.assignee_type.map(|kind|kind.to_string())).bind(assignment.assignee_id).bind(assignment.created_at).bind(assignment.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn insert_project_hook_task_role_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        assignment: &crate::CreateTaskRoleAssignment,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(&assignment.id).bind(task_id).bind(&assignment.role_name).bind(assignment.assignee_type.as_ref().map(|kind|kind.to_string())).bind(&assignment.assignee_id).bind(&assignment.created_at).bind(&assignment.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn advance_project_hook_task_role_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET version = version + 1, updated_at = ? WHERE id = ? AND deleted_at IS NULL AND version = ?").bind(updated_at).bind(task_id).bind(version).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn tasks_naming_retired_auditor(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
        "SELECT id FROM task WHERE deleted_at IS NULL AND status NOT IN ('done','cancelled') AND task_state_config IS NOT NULL AND json_valid(task_state_config) AND COALESCE(json_extract(task_state_config,'$.review.auditor_agent_id'), json_extract(task_state_config,'$.auditor_agent_id')) IS NOT NULL ORDER BY id",
    ).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn review_ci_infrastructure_budget_spent_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
                "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind='review_ci_infrastructure'),0)",
            ).bind(task_id).fetch_one(connection).await?)
    }
}

impl SqliteDb {
    pub async fn integration_step_live_task_version(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT version FROM task WHERE id=? AND deleted_at IS NULL")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn execution_setup_task_capability_class(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance
         WHERE task_id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_admission_task_status_epoch(
        &self,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}
