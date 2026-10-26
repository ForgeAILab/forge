//! Native work, provisioning waits, and placement dispatch.

use super::*;

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeWorkTaskRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeWorkTaskRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn condition_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("condition_json")?)
    }
    pub fn parent_task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("parent_task_id")?)
    }
    pub fn subtask_order(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("subtask_order")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn title(&self) -> crate::Result<String> {
        Ok(self.row.try_get("title")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn priority(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("priority")?)
    }
    pub fn assignee_type(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_type")?)
    }
    pub fn assignee_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("assignee_id")?)
    }
}

impl SqliteDb {
    pub async fn native_project_work_tasks(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<NativeWorkTaskRow>> {
        let rows = sqlx::query(
            "SELECT id, parent_task_id, subtask_order, version,
                            title, status, priority, assignee_type, assignee_id,
                            condition_json
                     FROM task WHERE project_id = ? AND deleted_at IS NULL
                     ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeWorkTaskRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_scope_work_task(
        &self,
        task_id: &str,
    ) -> crate::Result<Vec<NativeWorkTaskRow>> {
        let rows = sqlx::query(
            "SELECT id, parent_task_id, subtask_order, version,
                        title, status, priority, assignee_type, assignee_id,
                        condition_json
                     FROM task WHERE id = ? AND deleted_at IS NULL LIMIT 1",
        )
        .bind(task_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeWorkTaskRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn native_task_project_id(&self, task_id: &str) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn native_task_scope_summary(
        &self,
        id: &str,
    ) -> crate::Result<Option<NativeScopeSummaryRow>> {
        let rows =
            sqlx::query("SELECT id, project_id, title, status, priority FROM task WHERE id = ?")
                .bind(id)
                .fetch_optional(self.pool())
                .await?;
        Ok(rows.map(|row| NativeScopeSummaryRow { row }))
    }
}

impl SqliteDb {
    pub async fn workspace_gc_task_log_references(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM review
                WHERE task_id = ?1 AND status IN ('running', 'awaiting_human')
             ) OR EXISTS (
                SELECT 1 FROM attention_projection
                WHERE status <> 'resolved'
                  AND ((scope_type = 'task' AND scope_id = ?1)
                       OR json_extract(details_json, '$.task_id') = ?1
                       OR json_extract(details_json, '$.task.id') = ?1)
             )",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_legacy_log_task_exists(&self, task_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_gc_root_task_exists(&self, task_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT EXISTS (SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_gc_task_active_uses(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>("SELECT EXISTS (
                SELECT 1 FROM execution
                WHERE (task_id = ?1 OR workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND status = 'running'
             ) OR EXISTS (
                SELECT 1 FROM workspace_lease wl
                LEFT JOIN execution e ON e.id = wl.execution_id
                WHERE (wl.task_id = ?1
                       OR e.workspace_id IN (SELECT id FROM workspace WHERE task_id = ?1))
                  AND wl.status = 'active'
             ) OR EXISTS (
                SELECT 1 FROM task child JOIN execution e ON e.task_id = child.id
                WHERE child.parent_task_id = ?1 AND e.status = 'running'
             )",)
.bind(task_id)
.fetch_one(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn repository_provision_environment_wait_tasks(
        &self,
        project_id: &str,
        machine_json: &str,
    ) -> crate::Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as::<_, (String, i64)>("SELECT id,version FROM task WHERE project_id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.environment_wait.machine')=json(?)",)
.bind(project_id)
.bind(machine_json)
.fetch_all(self.pool()).await?)
    }
}

pub struct RepositoryProvisionTaskWaitInput<'a> {
    pub task_id: &'a str,
    pub reason: &'a str,
    pub version: i64,
    pub machine_json: &'a str,
    pub project_id: &'a str,
    pub daemon_id: &'a Option<String>,
    pub runtime_id: &'a Option<String>,
    pub readiness_version: i64,
}
impl SqliteDb {
    pub async fn record_repository_provision_task_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        input: RepositoryProvisionTaskWaitInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET metadata_json=json_set(metadata_json,'$.deferred_dispatch.reason',?) WHERE id=? AND version=? AND json_extract(metadata_json,'$.environment_wait.machine')=json(?) AND COALESCE((SELECT version FROM project_machine_readiness WHERE project_id=? AND owner_kind='daemon' AND daemon_id=? AND runtime_id=?),-1)=?", crate::ConditionChange::Legacy)
.bind(input.reason).bind(input.task_id).bind(input.version).bind(input.machine_json).bind(input.project_id).bind(input.daemon_id).bind(input.runtime_id).bind(input.readiness_version).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn validation_observed_task_status(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT status FROM task
                 WHERE id = ? AND project_id = ? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn milestone_definition_task_reference(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn decision_governed_task_reference(
        &self,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1
                 FROM task AS t
                 JOIN project_task_governance AS g ON g.task_id = t.id
                 WHERE t.id = ? AND t.project_id = ?
                 LIMIT 1",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn decision_task_reference(
        &self,
        id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

#[derive(Debug)]
pub struct PlacementEligibilityAssignmentRow {
    pub role_name: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub updated_at: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PlacementEligibilityAssignmentRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            role_name: row.try_get(0)?,
            assignee_type: row.try_get(1)?,
            assignee_id: row.try_get(2)?,
            updated_at: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_assignments(
        &self,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<Vec<PlacementEligibilityAssignmentRow>> {
        Ok(sqlx::query_as("SELECT role_name,assignee_type,assignee_id,updated_at FROM task_role_assignment WHERE task_id=? OR task_id=? ORDER BY task_id,role_name").bind(task_id).bind(parent_task_id).fetch_all(self.pool()).await?)
    }
}

pub struct ClearPlacementDispatchRefusalInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub version: i64,
    pub metadata_json: &'a Option<String>,
}
impl SqliteDb {
    pub async fn clear_placement_dispatch_refusal(
        &self,
        input: ClearPlacementDispatchRefusalInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET error_annotation=CASE WHEN error_annotation=? THEN NULL ELSE error_annotation END,
            metadata_json=CASE WHEN json_extract(metadata_json,'$.environment_wait.kind') IN ('environment_unverified','provision_failed') THEN json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition','$.environment_wait') ELSE json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition') END,version=version+1,updated_at=? WHERE id=? AND version=? AND metadata_json IS ?", crate::ConditionChange::Legacy).bind(input.annotation).bind(input.updated_at).bind(input.task_id).bind(input.version).bind(input.metadata_json).execute(self.pool()).await
    }
}

pub struct RecordPlacementDispatchRefusalInput<'a> {
    pub annotation: String,
    pub environment_wait: bool,
    pub marker: String,
    pub wait_json: String,
    pub marker_copy: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub version: i64,
}
impl SqliteDb {
    pub async fn record_placement_dispatch_refusal(
        &self,
        input: RecordPlacementDispatchRefusalInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation,'$.type')='dispatch_failed' THEN ? ELSE error_annotation END,
            metadata_json=CASE WHEN ? THEN json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?),'$.environment_wait',json(?)) ELSE json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?)) END,version=version+1,updated_at=? WHERE id=? AND version=?", crate::ConditionChange::Legacy).bind(input.annotation).bind(input.environment_wait).bind(input.marker).bind(input.wait_json).bind(input.marker_copy).bind(input.updated_at).bind(input.task_id).bind(input.version).execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn execution_plan_wait_task_version(
        &self,
        execution_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT version FROM task WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=?").bind(execution_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn clear_execution_plan_task_wait(
        &self,
        task_id: &str,
        execution_id: &str,
        updated_at: String,
        version: Option<i64>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET error_annotation=CASE WHEN json_extract(error_annotation, '$.type')='plan_settlement_wait' AND json_extract(error_annotation, '$.execution_id')=? THEN NULL ELSE error_annotation END,
        metadata_json=CASE WHEN json_extract(metadata_json, '$.owner_wait.plan_execution_id')=?
          THEN json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch', '$.owner_wait')
          ELSE json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch') END,
        version=version+1,updated_at=? WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=? AND version=?", crate::ConditionChange::Legacy).bind(execution_id).bind(execution_id).bind(updated_at).bind(execution_id).bind(version).execute(self.pool()).await
    }
}

impl SqliteDb {
    pub async fn record_execution_plan_owner_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        owner_wait_json: String,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET metadata_json=json_set(metadata_json, '$.owner_wait', json(?)) WHERE id=?", crate::ConditionChange::Legacy).bind(owner_wait_json).bind(task_id).identity_fenced().execute_in_tx(tx).await
    }
}

pub struct RecordExecutionPlanTaskWaitInTxInput<'a> {
    pub task_id: &'a str,
    pub marker: String,
    pub disposition: String,
    pub annotation: String,
    pub updated_at: String,
    pub version: i64,
}

impl SqliteDb {
    pub async fn record_execution_plan_task_wait_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        input: RecordExecutionPlanTaskWaitInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET metadata_json=json_set(COALESCE(metadata_json, '{}'), '$.plan_settlement_wait', json(?), '$.deferred_dispatch', json(?)),
        error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation, '$.type')='plan_settlement_wait' THEN ? ELSE error_annotation END,
        version=version+1, updated_at=? WHERE id=? AND version=?", crate::ConditionChange::Legacy).bind(input.marker).bind(input.disposition).bind(input.annotation).bind(input.updated_at).bind(input.task_id).bind(input.version).identity_fenced().execute_in_tx(tx).await
    }
}

impl SqliteDb {
    pub async fn task_outcome_task_exists_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)")
                .bind(task_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn link_task_hook_execution_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
        step_id: &str,
        hook_index: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task_hook_checkpoint SET execution_id=? WHERE step_id=? AND hook_index=? AND execution_id IS NULL").bind(execution_id).bind(step_id).bind(hook_index).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn lead_budget_charge_recorded_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        budget: &str,
        key: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)",
            ).bind(task_id).bind(budget).bind(key).fetch_one(connection).await?)
    }
}

#[cfg(test)]
mod tests;
