//! Task workflow transitions, completion, and shutdown queries.

use super::*;

pub struct ProjectWorkCompletionSourceRow {
    row: SqliteRow,
}
impl ProjectWorkCompletionSourceRow {
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn parent_task_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("parent_task_id")?)
    }
}

impl SqliteDb {
    pub async fn project_work_completion_sources(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<ProjectWorkCompletionSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, status, parent_task_id \
             FROM task \
             WHERE project_id = ? \
               AND is_automation = 0 \
               AND archived_at IS NULL \
               AND deleted_at IS NULL",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectWorkCompletionSourceRow { row })
            .collect())
    }
}

pub async fn project_work_completion_sources_in_tx(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
) -> crate::Result<Vec<ProjectWorkCompletionSourceRow>> {
    let rows = sqlx::query("SELECT id, status, parent_task_id FROM task WHERE project_id = ? AND is_automation = 0 AND archived_at IS NULL AND deleted_at IS NULL").bind(project_id).fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| ProjectWorkCompletionSourceRow { row })
        .collect())
}

pub struct ShutdownTaskSourceRow {
    row: SqliteRow,
}
impl ShutdownTaskSourceRow {
    pub fn id(&self) -> String {
        self.row.get("id")
    }
    pub fn project_id(&self) -> String {
        self.row.get("project_id")
    }
}

impl SqliteDb {
    pub async fn shutdown_in_progress_task_sources(
        &self,
    ) -> crate::Result<Vec<ShutdownTaskSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, project_id FROM task WHERE status = 'in_progress' AND deleted_at IS NULL",
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ShutdownTaskSourceRow { row })
            .collect())
    }
}

pub async fn coordination_root_visible_children_in_connection(
    connection: &mut sqlx::SqliteConnection,
    task_id: &str,
) -> crate::Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT EXISTS(
             SELECT 1
             FROM task AS root
             JOIN task AS child ON child.parent_task_id = root.id
             WHERE root.id = ?
               AND root.parent_task_id IS NULL
               AND root.deleted_at IS NULL
               AND child.deleted_at IS NULL
         )",
    )
    .bind(task_id)
    .fetch_one(connection)
    .await?)
}

pub struct TaskRepositoryAuthoritySourceRow {
    row: SqliteRow,
}
impl TaskRepositoryAuthoritySourceRow {
    pub fn project_id(&self) -> String {
        self.row.get("project_id")
    }
    pub fn task_version(&self) -> i64 {
        self.row.get("task_version")
    }
    pub fn primary_repo_id(&self) -> Option<String> {
        self.row.get("primary_repo_id")
    }
    pub fn resolved_repo_id(&self) -> Option<String> {
        self.row.get("resolved_repo_id")
    }
    pub fn repo_project_id(&self) -> String {
        self.row.get("repo_project_id")
    }
    pub fn default_branch(&self) -> String {
        self.row.get("default_branch")
    }
}

impl SqliteDb {
    pub async fn task_repository_authority_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<TaskRepositoryAuthoritySourceRow>> {
        Ok(sqlx::query(
            "SELECT t.project_id, t.version AS task_version,
                    p.primary_repo_id,
                    r.id AS resolved_repo_id,
                    r.project_id AS repo_project_id,
                    r.default_branch
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN repo r ON r.id = p.primary_repo_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?
        .map(|row| TaskRepositoryAuthoritySourceRow { row }))
    }
}

pub struct LatestTaskStateEntrySourceRow {
    row: SqliteRow,
}
impl LatestTaskStateEntrySourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn latest_task_state_entry_source(
        &self,
        task_id: &str,
        to_state: &str,
    ) -> crate::Result<Option<LatestTaskStateEntrySourceRow>> {
        let rows = sqlx::query(
        "SELECT id, created_at FROM transition_log WHERE task_id = ? AND to_state = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
    ).bind(task_id).bind(to_state).fetch_optional(self.pool()).await?;
        Ok(rows.map(|row| LatestTaskStateEntrySourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn consume_paused_integration_after_merge_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?")
            .bind(updated_at)
            .bind(task_id)
            .bind(version)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn task_step_settlement_epoch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn annotate_task_step_failure_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        annotation_json: String,
        blocked_json: &Option<String>,
        updated_at: &str,
        task_id: &str,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET error_annotation=?,blocked_json=COALESCE(?,blocked_json),version=version+1,updated_at=? WHERE id=? AND version=? AND deleted_at IS NULL")
            .bind(annotation_json)
            .bind(blocked_json)
            .bind(updated_at)
            .bind(task_id)
            .bind(version)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn conflict_handoff_source(
        &self,
        transition_log_id: &str,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ConflictHandoffSourceRow>> {
        Ok(sqlx::query("SELECT tl.id, tl.from_state, tl.to_state, tl.triggered_by, tl.bridge_kind, tl.bridge_payload, tl.created_at
            FROM transition_log tl JOIN task t ON t.id = tl.task_id
            WHERE tl.id = ? AND t.id = ? AND t.project_id = ?")
            .bind(transition_log_id)
            .bind(task_id)
            .bind(project_id)
            .fetch_optional(self.pool()).await?.map(|row| ConflictHandoffSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn project_conflict_handoffs_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        triggered_by: String,
        after: String,
        through: String,
    ) -> crate::Result<Vec<ConflictHandoffHistoryRow>> {
        Ok(sqlx::query(CONFLICT_HANDOFF_QUERY)
            .bind(project_id)
            .bind(triggered_by)
            .bind(after)
            .bind(through)
            .fetch_all(connection)
            .await?
            .into_iter()
            .map(|row| ConflictHandoffHistoryRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn daemon_upgrade_refused_task_ids(
        &self,
        refusal_code: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE deleted_at IS NULL AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json, '$.daemon_upgrade_refusal.code') END = ?")
            .bind(refusal_code)
            .fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn task_current_entry_first_transition_bridge(
        &self,
        task_id: &str,
        to_state: &str,
    ) -> crate::Result<Option<(String, Option<String>, Option<String>)>> {
        Ok(sqlx::query_as("SELECT id, bridge_kind, bridge_payload FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch = (SELECT status_epoch FROM task WHERE id = ?) ORDER BY created_at, rowid LIMIT 1")
            .bind(task_id)
            .bind(to_state)
            .bind(task_id)
            .fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn workflow_transition_task_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar("SELECT version FROM task WHERE id=?")
            .bind(task_id)
            .fetch_one(connection)
            .await?)
    }
}

pub struct WriteWorkflowTransitionStateInput<'a> {
    pub target_state: &'a str,
    pub updated_at: &'a str,
    pub entry_barrier_json: Option<&'a str>,
    pub clear_review_passed_at: bool,
    pub task_id: &'a str,
    pub has_step: bool,
    pub version: i64,
    pub expected_status: Option<&'a str>,
    pub expected_epoch: Option<i64>,
}

impl SqliteDb {
    pub async fn write_workflow_transition_state_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: WriteWorkflowTransitionStateInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task\n                 SET status = ?, version = version + 1, updated_at = ?, blocked_json = NULL, entry_barrier_json = ?,\n                     review_passed_at = CASE WHEN ? THEN NULL ELSE review_passed_at END,\n                     status_epoch = status_epoch + (status = ?)\n                 WHERE id = ? AND deleted_at IS NULL AND ((? = 0 AND version = ?) OR (? = 1 AND status = ? AND status_epoch = ?))")
            .bind(input.target_state)
            .bind(input.updated_at)
            .bind(input.entry_barrier_json)
            .bind(input.clear_review_passed_at)
            .bind(input.target_state)
            .bind(input.task_id)
            .bind(input.has_step)
            .bind(input.version)
            .bind(input.has_step)
            .bind(input.expected_status)
            .bind(input.expected_epoch)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn set_workflow_deferred_dispatch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        marker_json: &str,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.deferred_dispatch',json(?)) WHERE id=?")
            .bind(marker_json)
            .bind(task_id)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn clear_workflow_deferred_dispatch_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.deferred_dispatch') ELSE metadata_json END WHERE id=?")
            .bind(task_id)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn carry_paused_integration_state_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        target_state: &str,
        task_id: &str,
        current_status: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE task SET metadata_json=json_set(metadata_json,'$.paused_integration.state',?) WHERE id=? AND CASE WHEN json_valid(metadata_json) THEN json_extract(metadata_json,'$.paused_integration.state') END=?")
            .bind(target_state)
            .bind(task_id)
            .bind(current_status)
            .execute(connection).await?)
    }
}

pub struct InsertWorkflowTransitionLogInput<'a> {
    pub id: &'a str,
    pub task_id: &'a str,
    pub from_state: &'a str,
    pub to_state: &'a str,
    pub trigger_name: Option<&'a str>,
    pub triggered_by: String,
    pub trigger_reason: &'a str,
    pub hook_results_json: String,
    pub rejection: i64,
    pub created_at: &'a str,
    pub bridge_kind: Option<&'a str>,
    pub bridge_payload: Option<String>,
}

impl SqliteDb {
    pub async fn insert_workflow_transition_log_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertWorkflowTransitionLogInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO transition_log (
                            id, task_id, from_state, to_state, trigger_name, triggered_by,
                            trigger_reason, hook_results_json, rejection, created_at, bridge_kind, bridge_payload, status_epoch
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT status_epoch FROM task WHERE id = ?))")
            .bind(input.id)
            .bind(input.task_id)
            .bind(input.from_state)
            .bind(input.to_state)
            .bind(input.trigger_name)
            .bind(input.triggered_by)
            .bind(input.trigger_reason)
            .bind(input.hook_results_json)
            .bind(input.rejection)
            .bind(input.created_at)
            .bind(input.bridge_kind)
            .bind(input.bridge_payload)
            .bind(input.task_id)
            .execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn task_governance_capability_class(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT capability_class FROM project_task_governance WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn annotate_before_work_hook_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        annotation: &str,
        updated_at: &str,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
        "UPDATE task SET error_annotation = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
    ).bind(annotation).bind(updated_at).bind(task_id).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn review_hook_task_status_version(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<(String, i64)>> {
        Ok(sqlx::query_as("SELECT status,version FROM task WHERE id=?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn review_candidate_task_epoch(
        &self,
        task_id: &str,
        version: i64,
        status: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT status_epoch FROM task WHERE id=? AND version=? AND status=?",
        )
        .bind(task_id)
        .bind(version)
        .bind(status)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn clear_coordination_root_review_dispatch_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: &str,
        task_id: &str,
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
                                     OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL)))").bind(updated_at).bind(task_id).identity_fenced().execute_in_tx(transaction).await
    }
}

pub struct RecordReviewCiFailureInTxInput<'a> {
    pub entry_barrier_json: String,
    pub error_annotation: String,
    pub blocked_json: Option<String>,
    pub deferral: &'a Option<String>,
    pub updated_at: &'a str,
    pub task_id: &'a str,
    pub version: i64,
    pub status: &'a str,
}
impl SqliteDb {
    pub async fn record_review_ci_failure_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        input: RecordReviewCiFailureInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(
            self,
            input.task_id,
            "UPDATE task SET entry_barrier_json = ?, error_annotation = ?,
            blocked_json = ?, metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            updated_at = ?, version = version + 1
            WHERE id = ? AND version = ? AND status = ? AND deleted_at IS NULL",
        )
        .bind(input.entry_barrier_json)
        .bind(input.error_annotation)
        .bind(input.blocked_json)
        .bind(input.deferral)
        .bind(input.deferral)
        .bind(input.updated_at)
        .bind(input.task_id)
        .bind(input.version)
        .bind(input.status)
        .execute_in_tx(transaction)
        .await
    }
}

impl SqliteDb {
    pub async fn clear_manual_advance_annotation_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: String,
        task_id: &str,
        expected_annotation: &Option<String>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, task_id, "UPDATE task SET error_annotation=NULL,version=version+1,updated_at=? WHERE id=? AND error_annotation IS ? AND deleted_at IS NULL").bind(updated_at).bind(task_id).bind(expected_annotation).execute_in_tx(transaction).await
    }
}

impl SqliteDb {
    pub async fn link_manual_review_follow_up_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        owner_task_id: &str,
        metadata_json: String,
        task_id: &str,
        version: i64,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(
            self,
            owner_task_id,
            "UPDATE task SET metadata_json = ?, version = version + 1
                 WHERE id = ? AND version = ?",
        )
        .bind(metadata_json)
        .bind(task_id)
        .bind(version)
        .execute_in_tx(transaction)
        .await
    }
}

impl SqliteDb {
    pub async fn clear_manual_review_failure_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: &str,
        task_id: &str,
        version: i64,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(
            self,
            task_id,
            "UPDATE task SET error_annotation = NULL, blocked_json = NULL,
                 updated_at = ?, version = version + 1 WHERE id = ? AND version = ?",
        )
        .bind(updated_at)
        .bind(task_id)
        .bind(version)
        .execute_in_tx(transaction)
        .await
    }
}

pub struct ClearDaemonUpgradeDispatchRefusalInput<'a> {
    pub clear_annotation: bool,
    pub updated_at: String,
    pub task_id: &'a str,
    pub version: i64,
    pub metadata_json: &'a Option<String>,
    pub error_annotation: &'a Option<String>,
    pub refusal_code: &'a str,
}
impl SqliteDb {
    pub async fn clear_daemon_upgrade_dispatch_refusal(
        &self,
        input: ClearDaemonUpgradeDispatchRefusalInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, input.task_id, "UPDATE task SET
            error_annotation = CASE WHEN ? THEN NULL ELSE error_annotation END,
            metadata_json = NULLIF(json_remove(metadata_json, '$.daemon_upgrade_refusal', '$.dispatch_disposition', '$.deferred_dispatch'), '{}'),
            version = version + 1, updated_at = ?
         WHERE id = ? AND deleted_at IS NULL AND version = ?
            AND metadata_json IS ? AND error_annotation IS ?
            AND json_extract(metadata_json, '$.daemon_upgrade_refusal.code') = ?").bind(input.clear_annotation).bind(input.updated_at).bind(input.task_id).bind(input.version).bind(input.metadata_json).bind(input.error_annotation).bind(input.refusal_code).execute(self.pool()).await
    }
}
