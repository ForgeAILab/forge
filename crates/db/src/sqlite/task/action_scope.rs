//! Task action scopes and workspace lease assignments.

use super::*;

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskActionScopeAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskActionScopeAssignmentRow {
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn assignee_type(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_type")?)
    }
    pub fn assignee_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_id")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct TaskActionScopeRoleRow {
    row: sqlx::sqlite::SqliteRow,
}

impl TaskActionScopeRoleRow {
    pub fn role_name(&self) -> crate::Result<String> {
        Ok(self.row.try_get("role_name")?)
    }
}

impl SqliteDb {
    pub async fn action_scope_visible_task_count(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn task_action_scope_assignment(
        &self,
        task_id: &str,
    ) -> crate::Result<TaskActionScopeAssignmentRow> {
        let rows = sqlx::query("SELECT status, assignee_type, assignee_id FROM task WHERE id = ?")
            .bind(task_id)
            .fetch_one(self.pool())
            .await?;
        Ok(TaskActionScopeAssignmentRow { row: rows })
    }
}

impl SqliteDb {
    pub async fn task_action_scope_roles(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<Vec<TaskActionScopeRoleRow>> {
        let rows = sqlx::query(
            "SELECT role_name FROM task_role_assignment
                 WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskActionScopeRoleRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn action_approver_task_assignment_exists(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT (
                    EXISTS (
                        SELECT 1 FROM task
                        WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                    OR EXISTS (
                        SELECT 1 FROM task_role_assignment
                        WHERE task_id = ? AND assignee_type = 'agent' AND assignee_id = ?
                    )
                )",
        )
        .bind(task_id)
        .bind(identity_id)
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectTaskStatusCountRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectTaskStatusCountRow {
    pub fn key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("key")?)
    }
    pub fn count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("count")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_task_status_counts(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<EffectiveProjectTaskStatusCountRow>> {
        let rows = sqlx::query(
            "SELECT status AS key, COUNT(*) AS count
         FROM task WHERE project_id = ? AND deleted_at IS NULL
         GROUP BY status ORDER BY status ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectTaskStatusCountRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn escalation_visible_project_task_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL)",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn escalation_unsettled_task_condition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'")
            .bind(task_id)
            .bind(project_id)
            .fetch_optional(connection)
            .await?)
    }
}

impl SqliteDb {
    pub async fn environment_probe_task_metadata(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(
            sqlx::query_scalar::<_, Option<String>>("SELECT metadata_json FROM task WHERE id=?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn environment_wait_task_metadata_at_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        expected_version: i64,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT metadata_json FROM task WHERE id = ? AND version = ?",
        )
        .bind(task_id)
        .bind(expected_version)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_environment_probe_error_for_task_waits_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        error: &str,
        project_id: &str,
        machine_json: String,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self,"UPDATE task SET metadata_json=json_set(metadata_json,'$.environment_wait.last_error',?,'$.deferred_dispatch.reason',COALESCE(json_extract(metadata_json,'$.deferred_dispatch.reason'),'environment_probe_pending') || '; last failure: ' || ?) WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND json_extract(metadata_json,'$.environment_wait.kind')='environment_probe_pending'").bind(error).bind(error).bind(project_id).bind(machine_json).execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn persist_environment_probe_pending_wait_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        marker_json: String,
        wait_json: String,
        expected_version: i64,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self,task_id,"UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?), '$.environment_wait', json(?)) WHERE id = ? AND version = ? AND status = ?").bind(marker_json).bind(wait_json).bind(task_id).bind(expected_version).bind(expected_status).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn persist_environment_machine_wait_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        marker_json: String,
        wait_json: String,
        expected_version: i64,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self,task_id,"UPDATE task SET metadata_json = json_set(json_remove(COALESCE(metadata_json, '{}'), '$.owner_wait'), '$.environment_wait', json(?), '$.deferred_dispatch', json(?)) WHERE id = ? AND version = ? AND status = ?").bind(marker_json).bind(wait_json).bind(task_id).bind(expected_version).bind(expected_status).identity_fenced().execute_in_tx(tx).await
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct WorkspaceLeaseTaskAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl WorkspaceLeaseTaskAssignmentRow {
    pub fn assignee_type_unchecked(&self) -> Option<String> {
        self.row.get("assignee_type")
    }
    pub fn assignee_id_unchecked(&self) -> Option<String> {
        self.row.get("assignee_id")
    }
    pub fn task_type_unchecked(&self) -> String {
        self.row.get("task_type")
    }
    pub fn charter_status_unchecked(&self) -> String {
        self.row.get("charter_status")
    }
    pub fn charter_setup_required_unchecked(&self) -> i64 {
        self.row.get("charter_setup_required")
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct WorkspaceLeaseRoleAssignmentRow {
    row: sqlx::sqlite::SqliteRow,
}

impl WorkspaceLeaseRoleAssignmentRow {
    pub fn assignee_type_unchecked(&self) -> Option<String> {
        self.row.get("assignee_type")
    }
    pub fn assignee_id_unchecked(&self) -> Option<String> {
        self.row.get("assignee_id")
    }
}

impl SqliteDb {
    pub async fn task_governance_replacement_project(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM task WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_lease_task_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<WorkspaceLeaseTaskAssignmentRow>> {
        let rows = sqlx::query(
            "SELECT t.project_id, t.assignee_type, t.assignee_id,
                    t.task_type, p.charter_status, p.charter_setup_required
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WorkspaceLeaseTaskAssignmentRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_lease_role_assignment_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<Option<WorkspaceLeaseRoleAssignmentRow>> {
        let rows = sqlx::query(
            "SELECT assignee_type, assignee_id
             FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_optional(connection)
        .await?;
        Ok(rows.map(|row| WorkspaceLeaseRoleAssignmentRow { row }))
    }
}
