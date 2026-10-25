//! Task operator queries and recovery annotations.

use super::*;

impl SqliteDb {
    pub async fn workspace_cleanup_task_ids_after(
        &self,
        after: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        let result =
            sqlx::query_scalar::<_, String>("SELECT id FROM task WHERE id > ? ORDER BY id LIMIT ?")
                .bind(after)
                .bind(limit)
                .fetch_all(self.pool())
                .await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct TaskOperatorConditionRow {
    pub id: String,
    pub title: String,
    pub condition_json: String,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn tasks_with_exhausted_infrastructure_checks(
        &self,
    ) -> crate::Result<Vec<TaskOperatorConditionRow>> {
        let mut c = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT id, title, condition_json, updated_at FROM task
             WHERE condition_json LIKE '%infrastructure_exhausted%'
               AND deleted_at IS NULL AND archived_at IS NULL
               AND status NOT IN ('done', 'cancelled')
             ORDER BY updated_at DESC, id ASC LIMIT 50",
        )
        .fetch_all(&mut *c)
        .await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let mut condition =
                crate::task_condition::decode_or_unknown(row.try_get("condition_json")?);
            crate::task_condition::restore_read(&mut c, &id, &mut condition).await?;
            result.push(TaskOperatorConditionRow {
                id: row.try_get("id")?,
                title: row.try_get("title")?,
                condition_json: serde_json::to_string(&condition)
                    .map_err(|e| DbError::Check(e.to_string()))?,
                updated_at: row.try_get("updated_at")?,
            });
        }
        c.rollback().await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn open_tasks_with_retired_auditor(&self) -> crate::Result<Vec<String>> {
        let result = sqlx::query_scalar("SELECT id FROM task WHERE deleted_at IS NULL AND status NOT IN ('done','cancelled') AND task_state_config IS NOT NULL AND json_valid(task_state_config) AND COALESCE(json_extract(task_state_config,'$.review.auditor_agent_id'), json_extract(task_state_config,'$.auditor_agent_id')) IS NOT NULL ORDER BY id LIMIT 11")
            .fetch_all(self.pool()).await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct BlockedTaskOperatorRow {
    pub id: String,
    pub title: String,
    pub condition_json: String,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn blocked_task_operator_rows(&self) -> crate::Result<Vec<BlockedTaskOperatorRow>> {
        let mut c = self.pool.begin().await?;
        let rows = sqlx::query("SELECT id, title, condition_json, updated_at
             FROM task
             WHERE (status = 'blocked' OR json_extract(condition_json,'$.evidence.presentation.interruption_present') = 1)
               AND deleted_at IS NULL
               AND archived_at IS NULL
               AND status NOT IN ('done', 'cancelled')
             ORDER BY updated_at DESC, id ASC")
.fetch_all(&mut *c).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let mut condition =
                crate::task_condition::decode_or_unknown(row.try_get("condition_json")?);
            crate::task_condition::restore_read(&mut c, &id, &mut condition).await?;
            result.push(BlockedTaskOperatorRow {
                id: row.try_get("id")?,
                title: row.try_get("title")?,
                condition_json: serde_json::to_string(&condition)
                    .map_err(|e| DbError::Check(e.to_string()))?,
                updated_at: row.try_get("updated_at")?,
            });
        }
        c.rollback().await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct TaskOperatorRetryRow {
    pub task_id: String,
    pub title: String,
    pub status: String,
    pub condition_json: String,
    pub attempt_count: i64,
    pub execution_spent: i64,
    pub last_error: Option<String>,
}

impl SqliteDb {
    pub async fn task_operator_retry_rows(&self) -> crate::Result<Vec<TaskOperatorRetryRow>> {
        let mut c = self.pool.begin().await?;
        let rows = sqlx::query("SELECT
                t.id AS task_id,
                t.title,
                t.status,
                t.condition_json,
                COUNT(tl.id) AS attempt_count,
                COALESCE((
                    SELECT b.spent FROM task_budget b
                    WHERE b.task_id = t.id AND b.kind = 'execution'
                ), 0) AS execution_spent,
                (
                    SELECT e.error
                    FROM execution e
                    WHERE e.task_id = t.id AND e.status = 'failed'
                    ORDER BY COALESCE(e.stopped_at, e.updated_at) DESC, e.id DESC
                    LIMIT 1
                ) AS last_error
             FROM task t
             LEFT JOIN transition_log tl ON tl.task_id = t.id AND tl.rejection = 1
             WHERE t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')
             GROUP BY t.id, t.title, t.status, t.condition_json
             HAVING COUNT(tl.id) >= 1 OR json_extract(t.condition_json,'$.evidence.presentation.retry_display.reason') IS NOT NULL OR execution_spent > 0
             ORDER BY attempt_count DESC, t.updated_at DESC, t.id ASC")
.fetch_all(&mut *c).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("task_id")?;
            let mut condition =
                crate::task_condition::decode_or_unknown(row.try_get("condition_json")?);
            crate::task_condition::restore_read(&mut c, &id, &mut condition).await?;
            result.push(TaskOperatorRetryRow {
                task_id: row.try_get("task_id")?,
                title: row.try_get("title")?,
                status: row.try_get("status")?,
                condition_json: serde_json::to_string(&condition)
                    .map_err(|e| DbError::Check(e.to_string()))?,
                attempt_count: row.try_get("attempt_count")?,
                execution_spent: row.try_get("execution_spent")?,
                last_error: row.try_get("last_error")?,
            });
        }
        c.rollback().await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneTaskSourceRow {
    pub version: i64,
    pub task_type: String,
    pub status: String,
    pub deleted_at: Option<String>,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn milestone_task_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneTaskSourceRow>> {
        let row = sqlx::query(
            "SELECT version, task_type, status, deleted_at, updated_at
                 FROM task WHERE id = ? AND project_id = ?",
        )
        .bind(task_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(MilestoneTaskSourceRow {
                version: row.try_get("version")?,
                task_type: row.try_get("task_type")?,
                status: row.try_get("status")?,
                deleted_at: row.try_get("deleted_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn pending_recovery_command_exists(
        &self,
        task_id: &str,
        prefix: &str,
        matching_prefix: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND substr(causation_key,1,length(?))=?)")
.bind(task_id)
.bind(prefix)
.bind(matching_prefix)
.fetch_one(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn pending_retired_review_settlement_exists(
        &self,
        task_id: &str,
        prefix: &str,
        matching_prefix: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND kind='command' AND status IN ('pending','claimed') AND substr(causation_key,1,length(?))=?)")
.bind(task_id)
.bind(prefix)
.bind(matching_prefix)
.fetch_one(self.pool())
.await?)
    }
}

#[derive(Debug)]
pub struct AnnotateOwnerRecoveryInTxInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub expected_version: i64,
    pub terminal_states_json: String,
}

impl SqliteDb {
    pub async fn annotate_owner_recovery_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        input: AnnotateOwnerRecoveryInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation = ?, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?
            AND deleted_at IS NULL AND failed_json IS NULL AND status NOT IN (SELECT value FROM json_each(?))
            AND (error_annotation IS NULL OR (json_valid(error_annotation) AND json_extract(error_annotation, '$.blocking_reason') IN ('owner_disconnected_timeout', 'owner_lost_execution')))")
.bind(input.annotation)
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.expected_version)
.bind(input.terminal_states_json)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_reconciled_owner_waits_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: String,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self, "UPDATE task SET metadata_json = json_remove(COALESCE(metadata_json, '{}'),
        '$.dispatch_disposition', '$.deferred_dispatch', '$.owner_wait'), version = version + 1, updated_at = ?
        WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND (json_type(metadata_json, '$.dispatch_disposition') IS NOT NULL
             OR json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL
             OR json_type(metadata_json, '$.owner_wait') IS NOT NULL)")
.bind(updated_at)
.bind(task_id)
.bind(parent_task_id)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn owner_reconnect_exhausted_review_task_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT id FROM task WHERE (id=? OR parent_task_id=?) AND deleted_at IS NULL AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation,'$.blocking_reason')='review_ci_infrastructure_exhausted'")
.bind(task_id)
.bind(parent_task_id)
.fetch_all(connection)
.await?)
    }
}

impl SqliteDb {
    pub async fn restart_owner_reconnect_review_barrier_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        window: &str,
        task_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE task SET entry_barrier_json=json_set(entry_barrier_json,'$.started_at',?) WHERE id=?")
.bind(window)
.bind(task_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn restore_owner_reconnect_review_retry_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        updated_at: String,
        task_id: &str,
        parent_task_id: &str,
    ) -> crate::Result<crate::task_writer::BulkTaskQueryResult> {
        crate::task_writer::BulkTaskQuery::new(self, "UPDATE task SET blocked_json = NULL,
        entry_barrier_json = entry_barrier_json,
        error_annotation = json_set(error_annotation, '$.blocking_reason', 'review_ci_infrastructure'),
        version = version + 1, updated_at = ? WHERE (id = ? OR parent_task_id = ?) AND deleted_at IS NULL
        AND entry_barrier_json IS NOT NULL AND json_extract(error_annotation, '$.blocking_reason') = 'review_ci_infrastructure_exhausted'")
.bind(updated_at)
.bind(task_id)
.bind(parent_task_id)
.execute_in_tx(transaction)
.await
    }
}

#[derive(Debug)]
pub struct ClearRemovedMachineCancelAnnotationInput<'a> {
    pub updated_at: String,
    pub task_id: &'a str,
    pub workspace_task_id: &'a str,
    pub step_task_id: &'a str,
    pub execution_task_id: &'a str,
}

impl SqliteDb {
    pub async fn clear_removed_machine_cancel_annotation(
        &self,
        target_id: &str,
        input: ClearRemovedMachineCancelAnnotationInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=NULL, metadata_json=CASE WHEN json_valid(metadata_json) THEN json_remove(metadata_json,'$.dispatch_disposition','$.deferred_dispatch') ELSE metadata_json END, version=version+1,updated_at=? WHERE id=? AND deleted_at IS NULL AND json_valid(error_annotation) AND json_extract(error_annotation,'$.blocking_reason')='pending_remote_cancel' AND NOT EXISTS(SELECT 1 FROM pending_remote_cancel c LEFT JOIN workspace w ON w.id=c.workspace_id LEFT JOIN task_step s ON s.id=c.step_id WHERE w.task_id=? OR s.task_id=? OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=? AND e.workspace_id=c.workspace_id))")
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.workspace_task_id)
.bind(input.step_task_id)
.bind(input.execution_task_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn refresh_remaining_machine_cancel_annotation(
        &self,
        target_id: &str,
        annotation: String,
        updated_at: String,
        task_id: &str,
        previous_annotation: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=?,version=version+1,updated_at=? WHERE id=? AND error_annotation=?")
.bind(annotation)
.bind(updated_at)
.bind(task_id)
.bind(previous_annotation)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn clear_removed_machine_metadata_wait(
        &self,
        target_id: &str,
        key: &str,
        owner: &str,
        task_id: &str,
        daemon_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, format!("UPDATE task SET metadata_json=json_remove(metadata_json,'$.{key}','$.deferred_dispatch','$.dispatch_disposition') WHERE id=? AND json_valid(metadata_json) AND json_extract(metadata_json,'$.{owner}')=?"))
.bind(task_id)
.bind(daemon_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

impl SqliteDb {
    pub async fn clear_removed_machine_queued_recovery(
        &self,
        target_id: &str,
        metadata_path: String,
        updated_at: String,
        task_id: &str,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json=json_remove(metadata_json,?,'$.deferred_dispatch','$.dispatch_disposition'),version=version+1,updated_at=? WHERE id=? AND json_valid(metadata_json)")
.bind(metadata_path)
.bind(updated_at)
.bind(task_id)
.identity_fenced()
.execute(self.pool())
.await
    }
}

#[derive(Debug)]
pub struct AnnotateRemovedMachineTaskInput<'a> {
    pub annotation: String,
    pub updated_at: String,
    pub task_id: &'a str,
    pub terminal_states_json: String,
    pub owner_loss_reasons_json: String,
}

impl SqliteDb {
    pub async fn annotate_removed_machine_task(
        &self,
        target_id: &str,
        input: AnnotateRemovedMachineTaskInput<'_>,
    ) -> crate::Result<u64> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET error_annotation=?,
            blocked_json=CASE WHEN json_valid(error_annotation) AND json_extract(error_annotation,'$.blocked_by')='system:workflow' THEN NULL ELSE blocked_json END,
            updated_at=?, version=version+1
         WHERE id=? AND deleted_at IS NULL AND failed_json IS NULL
           AND status NOT IN (SELECT value FROM json_each(?))
           AND (error_annotation IS NULL OR (json_valid(error_annotation)
                AND json_extract(error_annotation,'$.blocking_reason') IN (SELECT value FROM json_each(?))))")
.bind(input.annotation)
.bind(input.updated_at)
.bind(input.task_id)
.bind(input.terminal_states_json)
.bind(input.owner_loss_reasons_json)
.identity_fenced()
.execute(self.pool())
.await
    }
}

#[derive(Debug)]
pub struct RecordPlacementRefusalInTxInput<'a> {
    pub deferral: &'a Option<String>,
    pub deferral_value: &'a Option<String>,
    pub annotation: &'a Option<String>,
    pub blocked: &'a Option<String>,
    pub updated_at: String,
    pub version_increment: i64,
    pub task_id: &'a str,
    pub expected_version: i64,
    pub expected_status: &'a str,
}

impl SqliteDb {
    pub async fn record_placement_refusal_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        input: RecordPlacementRefusalInTxInput<'_>,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = CASE WHEN ? IS NULL THEN
            json_remove(COALESCE(metadata_json, '{}'), '$.deferred_dispatch') ELSE
            json_set(COALESCE(metadata_json, '{}'), '$.deferred_dispatch', json(?)) END,
            error_annotation = COALESCE(?, error_annotation),
            blocked_json = COALESCE(?, blocked_json), updated_at = ?, version = version + ? WHERE id = ? AND version = ? AND status = ?")
.bind(input.deferral)
.bind(input.deferral_value)
.bind(input.annotation)
.bind(input.blocked)
.bind(input.updated_at)
.bind(input.version_increment)
.bind(input.task_id)
.bind(input.expected_version)
.bind(input.expected_status)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn record_placement_owner_wait_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        owner_wait: String,
        task_id: &str,
        expected_status: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.owner_wait', json(?)) WHERE id = ? AND status = ?")
.bind(owner_wait)
.bind(task_id)
.bind(expected_status)
.identity_fenced()
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_expired_placement_wait_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        task_id: &str,
        expected_version: i64,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait', '$.deferred_dispatch') WHERE id = ? AND version = ?")
.bind(task_id)
.bind(expected_version)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn clear_claim_placement_waits_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        target_id: &str,
        task_id: &str,
    ) -> crate::Result<crate::task_writer::TaskQueryResult> {
        crate::task_writer::TaskQuery::new(self, target_id, "UPDATE task SET metadata_json = json_remove(metadata_json, '$.owner_wait', '$.environment_wait', '$.deferred_dispatch')
            WHERE id = ? AND (json_type(metadata_json, '$.owner_wait') IS NOT NULL
            OR json_type(metadata_json, '$.environment_wait') IS NOT NULL)")
.bind(task_id)
.execute_in_tx(transaction)
.await
    }
}

impl SqliteDb {
    pub async fn wake_unsettled_task_condition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=? AND deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'")
.bind(task_id)
.fetch_optional(connection)
.await?)
    }
}

impl SqliteDb {
    pub async fn wake_live_task_project_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT project_id FROM task WHERE id=? AND deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn wake_budget_task_project_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}
