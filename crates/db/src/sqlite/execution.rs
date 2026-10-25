use super::workspace_root::under_sql;
use super::*;
use crate::ExecutionAdmission;
use crate::MarkUsageInvocationUnsettled;
use crate::{AgentExecutionStats, StartUsageInvocation};
use std::collections::HashSet;

mod repository;
mod terminal_ledger;

async fn execution_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    execution_id: &str,
) -> Result<Option<Execution>> {
    sqlx::query("SELECT * FROM execution WHERE id = ?")
        .bind(execution_id)
        .fetch_optional(&mut **transaction)
        .await?
        .map(map_execution)
        .transpose()
}

async fn terminal_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    terminal_report_id: &str,
    payload_digest: &str,
    execution_id: &str,
    expected_owner: Option<&str>,
) -> Result<Option<ExecutionTerminalOutcome>> {
    let Some(row) = sqlx::query(
        "SELECT terminal_report_id, execution_id, payload_digest, event_id, created_at
         FROM execution_terminal_receipt
         WHERE terminal_report_id = ?",
    )
    .bind(terminal_report_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let receipt = map_terminal_receipt(row)?;
    if receipt.execution_id != execution_id || receipt.payload_digest != payload_digest {
        return Err(DbError::IdempotencyConflict);
    }
    let event = sqlx::query("SELECT * FROM domain_event WHERE id = ?")
        .bind(&receipt.event_id)
        .fetch_optional(&mut **transaction)
        .await?
        .map(super::domain_event::map_domain_event)
        .transpose()
        .map_err(DbError::from)?
        .ok_or(DbError::IdempotencyConflict)?;
    let execution = execution_in_tx(transaction, execution_id)
        .await?
        .ok_or(DbError::NotFound)?;
    let payload = serde_json::from_str::<serde_json::Value>(&event.payload_json)
        .map_err(|_| DbError::IdempotencyConflict)?;
    if payload
        .get("terminal_report_id")
        .and_then(serde_json::Value::as_str)
        != Some(receipt.terminal_report_id.as_str())
        || payload
            .get("terminal_report_digest")
            .and_then(serde_json::Value::as_str)
            != Some(receipt.payload_digest.as_str())
    {
        return Err(DbError::IdempotencyConflict);
    }
    if let Some(expected_owner) = expected_owner {
        let previous_owner = payload
            .get("previous_lease_owner")
            .and_then(serde_json::Value::as_str);
        if !previous_owner.is_some_and(|owner| same_remote_daemon_owner(owner, expected_owner)) {
            return Err(DbError::IdempotencyConflict);
        }
    }
    let workspace_lease_id = payload
        .get("workspace_lease_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let workspace_lease_status = payload
        .get("workspace_lease_status")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    Ok(Some(ExecutionTerminalOutcome::Committed {
        execution,
        event: Box::new(event),
        workspace_lease_id,
        workspace_lease_status,
        replayed: true,
    }))
}

async fn persist_terminal_receipt_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    receipt: &ExecutionTerminalReceipt,
) -> Result<()> {
    let result = sqlx::query(
        "INSERT INTO execution_terminal_receipt (
            terminal_report_id, execution_id, payload_digest, event_id, created_at
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&receipt.terminal_report_id)
    .bind(&receipt.execution_id)
    .bind(&receipt.payload_digest)
    .bind(&receipt.event_id)
    .bind(&receipt.created_at)
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.to_string().contains("UNIQUE") => {
            let existing = sqlx::query(
                "SELECT terminal_report_id, execution_id, payload_digest, event_id, created_at
                 FROM execution_terminal_receipt
                 WHERE terminal_report_id = ?",
            )
            .bind(&receipt.terminal_report_id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_terminal_receipt)
            .transpose()?;
            if existing.as_ref() == Some(receipt) {
                Ok(())
            } else {
                Err(DbError::IdempotencyConflict)
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn map_terminal_receipt(row: SqliteRow) -> Result<ExecutionTerminalReceipt> {
    Ok(ExecutionTerminalReceipt {
        terminal_report_id: row.try_get("terminal_report_id")?,
        execution_id: row.try_get("execution_id")?,
        payload_digest: row.try_get("payload_digest")?,
        event_id: row.try_get("event_id")?,
        created_at: row.try_get("created_at")?,
    })
}

fn progress_timestamp_is_not_newer(candidate: &str, current: &str) -> bool {
    match (
        chrono::DateTime::parse_from_rfc3339(candidate),
        chrono::DateTime::parse_from_rfc3339(current),
    ) {
        (Ok(candidate), Ok(current)) => candidate <= current,
        _ => candidate <= current,
    }
}

fn same_remote_daemon_owner(previous_owner: &str, expected_owner: &str) -> bool {
    let Some(previous_daemon) = previous_owner
        .strip_prefix("daemon:")
        .and_then(|value| value.split_once(":connection:").map(|(daemon, _)| daemon))
    else {
        return false;
    };
    let Some(expected_daemon) = expected_owner
        .strip_prefix("daemon:")
        .and_then(|value| value.split_once(":connection:").map(|(daemon, _)| daemon))
    else {
        return false;
    };
    previous_daemon == expected_daemon
}

fn progress_timestamp_is_before(value: &str, threshold: &str) -> bool {
    match (
        chrono::DateTime::parse_from_rfc3339(value),
        chrono::DateTime::parse_from_rfc3339(threshold),
    ) {
        (Ok(value), Ok(threshold)) => value < threshold,
        _ => value < threshold,
    }
}

pub(super) async fn projection_executions(
    connection: &mut sqlx::SqliteConnection,
    tasks: &[crate::TaskExecutionProjectionQuery],
) -> Result<Vec<Execution>> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "WITH requested(task_id, current_role, blocked_execution_id) AS (",
    );
    query.push_values(tasks, |mut row, task| {
        row.push_bind(&task.task_id)
            .push_bind(task.current_role.as_deref())
            .push_bind(task.blocked_execution_id.as_deref());
    });
    query.push(
        ") SELECT execution.* FROM execution WHERE id IN (
                 SELECT candidate.id FROM execution candidate
                 JOIN requested ON candidate.task_id = requested.task_id
                 WHERE candidate.status = 'running'
                    OR candidate.id = requested.blocked_execution_id",
    );
    // Each class contributes one representative, regardless of history length.
    // Running rows are kept separately for session-aware health precedence.
    for predicate in [
            "1 = 1",
            "candidate.status != 'running'",
            "candidate.status != 'running' AND candidate.agent_session_id IS NOT NULL",
            "candidate.agent_id IS NOT NULL",
            "candidate.status != 'running' AND (candidate.role = requested.current_role OR (requested.current_role = 'coder' AND candidate.role = 'executor'))",
            "candidate.status != 'running' AND candidate.agent_session_id IS NOT NULL AND (candidate.role = requested.current_role OR (requested.current_role = 'coder' AND candidate.role = 'executor'))",
            "candidate.status != 'running' AND candidate.agent_id IS NOT NULL AND (candidate.role = requested.current_role OR (requested.current_role = 'coder' AND candidate.role = 'executor'))",
        ] {
            query.push(" UNION SELECT (SELECT candidate.id FROM execution candidate WHERE candidate.task_id = requested.task_id AND ")
                .push(predicate)
                .push(" ORDER BY candidate.created_at DESC, candidate.id DESC LIMIT 1) FROM requested");
        }
    query.push(") ORDER BY execution.task_id, execution.id");
    let rows = query.build().fetch_all(&mut *connection).await?;
    rows.into_iter().map(map_execution).collect()
}

#[derive(Debug, Clone)]
pub struct ActiveExecutionOperatorRow {
    pub execution_id: String,
    pub task_id: String,
    pub task_title: Option<String>,
    pub role: String,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub workspace_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub started_at: String,
    pub logs_path: Option<String>,
    pub executor_config_snapshot_json: Option<String>,
}

impl SqliteDb {
    pub async fn active_execution_operator_rows(
        &self,
    ) -> crate::Result<Vec<ActiveExecutionOperatorRow>> {
        let rows = sqlx::query(
            "SELECT
                e.id AS execution_id,
                e.task_id,
                t.title AS task_title,
                e.role,
                e.agent_id,
                a.name AS agent_name,
                e.workspace_id,
                e.agent_session_id,
                e.created_at AS started_at,
                e.logs_path,
                e.executor_config_snapshot_json
             FROM execution e
             JOIN task t ON t.id = e.task_id
             LEFT JOIN agent_current a ON a.id = e.agent_id
             WHERE e.status = 'running'
             ORDER BY e.created_at ASC, e.id ASC",
        )
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ActiveExecutionOperatorRow {
                    execution_id: row.try_get("execution_id")?,
                    task_id: row.try_get("task_id")?,
                    task_title: row.try_get("task_title")?,
                    role: row.try_get("role")?,
                    agent_id: row.try_get("agent_id")?,
                    agent_name: row.try_get("agent_name")?,
                    workspace_id: row.try_get("workspace_id")?,
                    agent_session_id: row.try_get("agent_session_id")?,
                    started_at: row.try_get("started_at")?,
                    logs_path: row.try_get("logs_path")?,
                    executor_config_snapshot_json: row.try_get("executor_config_snapshot_json")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct RecentFailedExecutionOperatorRow {
    pub execution_id: String,
    pub task_id: String,
    pub error: Option<String>,
    pub occurred_at: String,
}

impl SqliteDb {
    pub async fn recent_failed_execution_operator_rows(
        &self,
        cutoff: &str,
    ) -> crate::Result<Vec<RecentFailedExecutionOperatorRow>> {
        let rows = sqlx::query(
            "SELECT
                e.id AS execution_id,
                e.task_id,
                e.error,
                COALESCE(e.stopped_at, e.updated_at) AS occurred_at
             FROM execution e INDEXED BY idx_execution_usage_failed_recent
             JOIN task t ON t.id = e.task_id
             WHERE e.status = 'failed'
               AND t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')
               AND COALESCE(e.stopped_at, e.updated_at) >= ?
             ORDER BY occurred_at DESC, e.id ASC",
        )
        .bind(cutoff)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(RecentFailedExecutionOperatorRow {
                    execution_id: row.try_get("execution_id")?,
                    task_id: row.try_get("task_id")?,
                    error: row.try_get("error")?,
                    occurred_at: row.try_get("occurred_at")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn running_execution_ids_for_temp_sweep(&self) -> crate::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM execution WHERE status = 'running'")
                .fetch_all(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn running_retired_review_task_ids(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT DISTINCT task_id FROM execution WHERE status = 'running' AND role IN ('reviewer','auditor')")
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn latest_owner_failed_execution_ids(
        &self,
        workspace_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT e.id FROM execution e WHERE e.workspace_id = ? AND e.status = 'failed'
         AND e.stop_reason = 'daemon_disconnected' AND json_valid(e.error)
         AND json_extract(e.error, '$.cause')
             IN ('owner_disconnected_timeout', 'owner_lost_execution')
         AND NOT EXISTS (SELECT 1 FROM execution newer WHERE newer.task_id = e.task_id
             AND CASE newer.role WHEN 'executor' THEN 'coder' ELSE newer.role END
                 = CASE e.role WHEN 'executor' THEN 'coder' ELSE e.role END
             AND (newer.created_at > e.created_at OR (newer.created_at = e.created_at AND newer.id > e.id)))")
.bind(workspace_id)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn latest_terminal_workspace_execution_ids(
        &self,
        workspace_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT e.id FROM execution e WHERE e.workspace_id = ? AND e.status != 'running'
         AND NOT EXISTS (SELECT 1 FROM execution newer WHERE newer.task_id = e.task_id
             AND CASE newer.role WHEN 'executor' THEN 'coder' ELSE newer.role END
                 = CASE e.role WHEN 'executor' THEN 'coder' ELSE e.role END
             AND (newer.created_at > e.created_at OR (newer.created_at = e.created_at AND newer.id > e.id)))
         ORDER BY e.created_at DESC, e.id DESC")
.bind(workspace_id)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn unplaced_daemon_running_execution_ids(
        &self,
        created_before: &str,
        daemon_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT e.id FROM execution e
         LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
         LEFT JOIN agent_current a ON a.id = e.agent_id
         WHERE e.status = 'running' AND e.created_at < ?
           AND (p.owner_kind IS NULL OR p.owner_kind != 'daemon')
           AND CASE WHEN e.workspace_id IS NOT NULL
               THEN CASE p.owner_kind WHEN 'daemon' THEN p.daemon_id ELSE p.execution_daemon_id END
               ELSE COALESCE(NULLIF(TRIM(json_extract(e.executor_config_snapshot_json, '$.daemon_id')), ''), a.daemon_id)
               END = ?
         ORDER BY e.created_at, e.id")
.bind(created_before)
.bind(daemon_id)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn workspace_admission_running_execution_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        workspace_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution WHERE workspace_id = ? AND status = 'running' LIMIT 1",
        )
        .bind(workspace_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn owner_workspace_has_running_execution(
        &self,
        workspace_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
        )
        .bind(workspace_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn owner_workspace_has_running_execution_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        workspace_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
        )
        .bind(workspace_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn current_orphan_attempt_task_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT candidate.task_id FROM execution candidate
            WHERE candidate.id = ? AND candidate.status IN ('failed', 'cancelled')
              AND (candidate.resume_policy IS NULL OR candidate.resume_policy = 'manual')
              AND (candidate.stop_reason IS NULL OR candidate.stop_reason NOT IN ('user_cancelled', 'role_reassigned'))
              AND NOT EXISTS (
                  SELECT 1 FROM execution successor
                  WHERE successor.task_id = candidate.task_id AND successor.id <> candidate.id
                    AND (successor.status = 'running'
                         OR successor.parent_execution_id = candidate.id
                         OR successor.created_at > candidate.created_at)
              )")
.bind(execution_id)
.fetch_optional(connection)
.await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleActiveExecutionCountRow {
    pub count: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_visible_active_execution_count(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<Option<AttentionVisibleActiveExecutionCountRow>> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        // Keep the query construction below explicit; it avoids interpolating
        // authorization values while allowing account and project scopes.
        let active_sql = format!(
            "SELECT COUNT(*) AS count FROM execution e
             JOIN task t ON t.id = e.task_id JOIN project p ON p.id = t.project_id
             WHERE e.status = 'running' AND t.deleted_at IS NULL AND {predicate}"
        );
        let mut active_query = sqlx::query(&active_sql);
        for value in &values {
            active_query = active_query.bind(value);
        }
        let active = active_query.fetch_optional(self.pool()).await?;
        Ok(active.map(|row| AttentionVisibleActiveExecutionCountRow {
            count: row.try_get("count").map_err(crate::DbError::from),
        }))
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeLatestTaskExecutionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeLatestTaskExecutionRow {
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn role_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("role")?)
    }
    pub fn agent_session_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("agent_session_id")?)
    }
    pub fn logs_path(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("logs_path")?)
    }
}

impl SqliteDb {
    pub async fn native_work_latest_task_executions(
        &self,
        ids: &[String],
    ) -> crate::Result<Vec<NativeLatestTaskExecutionRow>> {
        let placeholders = vec!["?"; ids.len()].join(", ");
        let sql = format!(
            "SELECT task_id, id, status, role, agent_session_id, logs_path
                 FROM (
                     SELECT task_id, id, status, role, agent_session_id, logs_path,
                            ROW_NUMBER() OVER (
                                PARTITION BY task_id ORDER BY created_at DESC, id DESC
                            ) AS execution_rank
                     FROM execution
                     WHERE task_id IN ({placeholders})
                 )
                 WHERE execution_rank = 1"
        );
        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }

        let rows = query.fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeLatestTaskExecutionRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn workspace_gc_running_execution_ids(&self) -> crate::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT id FROM execution WHERE status = 'running'")
                .fetch_all(self.pool())
                .await?,
        )
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexExecutionRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexExecutionRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
    pub fn duration_ms(&self) -> crate::Result<Option<f64>> {
        Ok(self.row.try_get("duration_ms")?)
    }
    pub fn agent_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("agent_id")?)
    }
    pub fn execution_rowid(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("execution_rowid")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn present_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("present_id")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexExecutionDomainRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexExecutionDomainRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn agent_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("agent_id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct UsageIndexAgentExecutionStatsRow {
    row: sqlx::sqlite::SqliteRow,
}
impl UsageIndexAgentExecutionStatsRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn n(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("n")?)
    }
    pub fn completed(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("completed")?)
    }
    pub fn duration(&self) -> crate::Result<Option<f64>> {
        Ok(self.row.try_get("duration")?)
    }
    pub fn running(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("running")?)
    }
    pub fn unstable(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("unstable")?)
    }
}

pub async fn usage_index_agent_execution_stats_on_connection(
    connection: &mut sqlx::SqliteConnection,
    agent_ids_json: String,
) -> crate::Result<Vec<UsageIndexAgentExecutionStatsRow>> {
    let rows = sqlx::query("WITH requested AS (SELECT value AS id FROM json_each(?)) SELECT requested.id,COUNT(e.id) AS n,COALESCE(SUM(e.status='running'),0) AS running,COALESCE(SUM(e.status='completed'),0) AS completed,AVG(CASE WHEN e.status!='running' THEN (JULIANDAY(e.updated_at)-JULIANDAY(e.created_at))*86400000 END) AS duration,COALESCE(SUM(CASE WHEN e.status!='running' AND (e.created_at NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*' OR e.updated_at NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T*') THEN 1 ELSE 0 END),0) AS unstable FROM requested LEFT JOIN execution e ON e.agent_id=requested.id GROUP BY requested.id",)
.bind(agent_ids_json)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexAgentExecutionStatsRow { row })
        .collect())
}

pub async fn usage_index_execution_domains_on_connection(
    connection: &mut sqlx::SqliteConnection,
    source_ids_json: String,
) -> crate::Result<Vec<UsageIndexExecutionDomainRow>> {
    let rows = sqlx::query(
        "SELECT id,agent_id,status FROM execution WHERE id IN (SELECT value FROM json_each(?))",
    )
    .bind(source_ids_json)
    .fetch_all(connection)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexExecutionDomainRow { row })
        .collect())
}

pub async fn usage_index_inserted_executions_on_connection(
    connection: &mut sqlx::SqliteConnection,
    after_rowid: i64,
    through_rowid: i64,
) -> crate::Result<Vec<UsageIndexExecutionRow>> {
    let rows = sqlx::query("SELECT rowid AS execution_rowid,id,agent_id,status,created_at,updated_at,CASE WHEN status!='running' THEN (JULIANDAY(updated_at)-JULIANDAY(created_at))*86400000 END AS duration_ms FROM execution WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT 400",)
.bind(after_rowid)
.bind(through_rowid)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexExecutionRow { row })
        .collect())
}

pub struct UsageIndexChangedExecutionsOnConnectionInput<'a> {
    pub after_revision: i64,
    pub cursor_revision: i64,
    pub last_id: &'a str,
    pub through_revision: i64,
    pub through_rowid: i64,
    pub old_revision: i64,
}

pub async fn usage_index_changed_executions_on_connection(
    connection: &mut sqlx::SqliteConnection,
    input: UsageIndexChangedExecutionsOnConnectionInput<'_>,
) -> crate::Result<Vec<UsageIndexExecutionRow>> {
    let rows = sqlx::query("SELECT c.id,c.revision,e.id AS present_id,e.agent_id,e.status,e.created_at,e.updated_at,CASE WHEN e.status!='running' THEN (JULIANDAY(e.updated_at)-JULIANDAY(e.created_at))*86400000 END AS duration_ms FROM usage_changed_execution c LEFT JOIN execution e ON e.id=c.id WHERE (c.revision>? OR (c.revision=? AND c.id>?)) AND c.revision<=? AND (e.rowid<=? OR e.id IS NULL) AND c.revision>?6 ORDER BY c.revision,c.id LIMIT 400",)
.bind(input.after_revision)
.bind(input.cursor_revision)
.bind(input.last_id)
.bind(input.through_revision)
.bind(input.through_rowid)
.bind(input.old_revision)
.fetch_all(connection).await?;
    Ok(rows
        .into_iter()
        .map(|row| UsageIndexExecutionRow { row })
        .collect())
}

#[derive(Clone, Copy)]
pub enum UsageIndexExecutionTextField {
    Id,
    AgentId,
    Status,
    CreatedAt,
    UpdatedAt,
}
impl UsageIndexExecutionRow {
    pub fn optional_text(
        &self,
        field: UsageIndexExecutionTextField,
    ) -> crate::Result<Option<String>> {
        Ok(self.row.try_get(match field {
            UsageIndexExecutionTextField::Id => "id",
            UsageIndexExecutionTextField::AgentId => "agent_id",
            UsageIndexExecutionTextField::Status => "status",
            UsageIndexExecutionTextField::CreatedAt => "created_at",
            UsageIndexExecutionTextField::UpdatedAt => "updated_at",
        })?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_log_counts(&self) -> crate::Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as(
            "SELECT substr(logs_path, 1, instr(logs_path, '/.forge/logs/') - 1), COUNT(*)
         FROM execution WHERE instr(logs_path, '/.forge/logs/') > 1 GROUP BY 1",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_running_execution_count(
        &self,
        spelling: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM execution e
                 LEFT JOIN workspace w ON w.id = e.workspace_id
                 WHERE e.status = 'running' AND ({} OR {})",
            under_sql("e.logs_path"),
            under_sql("w.worktree_path")
        ))
        .bind(spelling)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_running_execution_total(&self) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution WHERE status = 'running'")
                .fetch_one(self.pool())
                .await?,
        )
    }
}

pub struct RememberExecutionPlanSeedInput<'a> {
    pub seed_digest: Option<String>,
    pub seed_size: Option<i64>,
    pub updated_at: String,
    pub execution_id: &'a str,
}

impl SqliteDb {
    pub async fn remember_execution_plan_seed(
        &self,
        input: RememberExecutionPlanSeedInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
        "INSERT INTO execution_plan_transport (execution_id, seed_digest, seed_size, updated_at)
        SELECT id, ?, ?, ? FROM execution WHERE id=? AND status='running'
        ON CONFLICT(execution_id) DO NOTHING",
    ).bind(input.seed_digest).bind(input.seed_size).bind(input.updated_at).bind(input.execution_id).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn execution_plan_seed_digest(
        &self,
        execution_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT seed_digest FROM execution_plan_transport WHERE execution_id=?",
        )
        .bind(execution_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn stored_execution_plan_candidate(
        &self,
        execution_id: &str,
    ) -> crate::Result<Option<(Option<String>, Option<String>)>> {
        Ok(sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT candidate_text, candidate_error FROM execution_plan_transport WHERE execution_id=?",
    ).bind(execution_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn execution_plan_retry_at(
        &self,
        execution_id: &str,
        operation: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
        "SELECT retry_at FROM execution_plan_transport WHERE execution_id=? AND retry_operation=?",
    ).bind(execution_id).bind(operation).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn execution_plan_task_id(
        &self,
        execution_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT task_id FROM execution WHERE id=?")
                .bind(execution_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn execution_plan_retry_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
        operation: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar("SELECT retry_count FROM execution_plan_transport WHERE execution_id=? AND retry_operation=?").bind(execution_id).bind(operation).fetch_optional(connection).await?)
    }
}

impl SqliteDb {
    pub async fn execution_plan_required_task_id(
        &self,
        execution_id: &str,
    ) -> crate::Result<String> {
        Ok(
            sqlx::query_scalar("SELECT task_id FROM execution WHERE id=?")
                .bind(execution_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn clear_execution_plan_transport_retry(
        &self,
        execution_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE execution_plan_transport SET retry_operation=NULL,retry_count=0,retry_at=NULL,retry_error=NULL WHERE execution_id=?").bind(execution_id).execute(self.pool()).await?)
    }
}

pub struct RecordExecutionPlanTransportRetryInTxInput<'a> {
    pub execution_id: &'a str,
    pub operation: &'a str,
    pub attempts: i64,
    pub retry_at: &'a str,
    pub message: &'a str,
    pub updated_at: String,
}

impl SqliteDb {
    pub async fn record_execution_plan_transport_retry_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: RecordExecutionPlanTransportRetryInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO execution_plan_transport (execution_id, retry_operation, retry_count, retry_at, retry_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET retry_operation=excluded.retry_operation,
        retry_count=excluded.retry_count, retry_at=excluded.retry_at, retry_error=excluded.retry_error, updated_at=excluded.updated_at").bind(input.execution_id).bind(input.operation).bind(input.attempts).bind(input.retry_at).bind(input.message).bind(input.updated_at).execute(connection).await?)
    }
}

pub struct ExecutionTerminalAuthorityRow {
    pub status: String,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub hard_deadline_at: Option<String>,
}

impl SqliteDb {
    pub async fn execution_terminal_authority_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
    ) -> crate::Result<Option<ExecutionTerminalAuthorityRow>> {
        let row = sqlx::query(
            "SELECT status, lease_owner, lease_expires_at, hard_deadline_at
                 FROM execution WHERE id = ?",
        )
        .bind(execution_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(ExecutionTerminalAuthorityRow {
                status: row.try_get("status")?,
                lease_owner: row.try_get("lease_owner")?,
                lease_expires_at: row.try_get("lease_expires_at")?,
                hard_deadline_at: row.try_get("hard_deadline_at")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn lead_subtask_implementation_started(
        &self,
        task_id: &str,
        step_check_role: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution WHERE task_id = ? AND role <> ?)",
        )
        .bind(task_id)
        .bind(step_check_role)
        .fetch_one(self.pool())
        .await?)
    }
}

#[derive(Debug)]
pub struct LeadSubtaskExecutionRow {
    pub role: String,
    pub status: String,
    pub error: Option<String>,
    pub created_at: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for LeadSubtaskExecutionRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            role: row.try_get(0)?,
            status: row.try_get(1)?,
            error: row.try_get(2)?,
            created_at: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn lead_subtask_recent_executions(
        &self,
        task_id: &str,
    ) -> crate::Result<Vec<LeadSubtaskExecutionRow>> {
        Ok(sqlx::query_as(
                "SELECT role, status, error, created_at FROM execution WHERE task_id=? ORDER BY created_at DESC, id DESC LIMIT 5",
            ).bind(task_id).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn workspace_merge_execution_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        execution_id: &str,
        workspace_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT execution_version FROM execution WHERE id = ? AND workspace_id = ?",
        )
        .bind(execution_id)
        .bind(workspace_id)
        .fetch_optional(connection)
        .await?)
    }
}

pub struct RecordWorkspaceMergeEvidenceInTxInput<'a> {
    pub before_sha: &'a str,
    pub after_sha: &'a str,
    pub updated_at: &'a str,
    pub execution_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn record_workspace_merge_evidence_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: RecordWorkspaceMergeEvidenceInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE execution SET before_sha = ?, after_sha = ?, execution_version = execution_version + 1, updated_at = ? WHERE id = ? AND execution_version = ?").bind(input.before_sha).bind(input.after_sha).bind(input.updated_at).bind(input.execution_id).bind(input.version).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn running_task_role_execution_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        role_name: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
             WHERE task_id = ? AND status = 'running' AND (role = ? OR role = 'executor')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(task_id)
        .bind(role_name)
        .fetch_optional(connection)
        .await?)
    }
}

pub struct AgentUsageDomainRunRow {
    row: SqliteRow,
}
impl AgentUsageDomainRunRow {
    pub fn agent_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("agent_id")?)
    }
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
}

impl SqliteDb {
    pub async fn agent_usage_domain_runs_on_connection(
        connection: &mut sqlx::SqliteConnection,
        ids_json: String,
    ) -> crate::Result<Vec<AgentUsageDomainRunRow>> {
        let rows = sqlx::query("SELECT agent_id, id, status FROM execution WHERE agent_id IN (SELECT value FROM json_each(?))").bind(ids_json).fetch_all(connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| AgentUsageDomainRunRow { row })
            .collect())
    }
}

pub struct TaskUsageDomainRunRow {
    row: SqliteRow,
}
impl TaskUsageDomainRunRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
}

impl SqliteDb {
    pub async fn task_usage_domain_runs_on_connection(
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Vec<TaskUsageDomainRunRow>> {
        let rows = sqlx::query("SELECT id, status FROM execution WHERE task_id = ?")
            .bind(task_id)
            .fetch_all(connection)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| TaskUsageDomainRunRow { row })
            .collect())
    }
}

pub struct OperationsUsageDomainRunRow {
    row: SqliteRow,
}
impl OperationsUsageDomainRunRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
}

impl SqliteDb {
    pub async fn operations_usage_domain_runs_on_connection(
        connection: &mut sqlx::SqliteConnection,
    ) -> crate::Result<Vec<OperationsUsageDomainRunRow>> {
        let rows = sqlx::query("SELECT id, status FROM execution")
            .fetch_all(connection)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| OperationsUsageDomainRunRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn embedded_workspace_recent_agent_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        workspace_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT agent_id FROM execution WHERE workspace_id = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(workspace_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_retired_review_rerun_error(
        &self,
        reason: &str,
        updated_at: String,
        execution_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE execution SET error = ?, updated_at = ? WHERE id = ? AND status <> 'running'",
        )
        .bind(reason)
        .bind(updated_at)
        .bind(execution_id)
        .execute(self.pool())
        .await?)
    }
}

pub struct ExecutionSetupTaskEvidenceRow {
    row: SqliteRow,
}
impl ExecutionSetupTaskEvidenceRow {
    pub fn execution_count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("execution_count")?)
    }
    pub fn attempt_count(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("attempt_count")?)
    }
    pub fn latest_commit_sha(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("latest_commit_sha")?)
    }
}

impl SqliteDb {
    pub async fn execution_setup_task_evidence_counts(
        &self,
        task_id: &str,
    ) -> crate::Result<ExecutionSetupTaskEvidenceRow> {
        let rows = sqlx::query(
            "SELECT
             COUNT(*) AS execution_count,
             SUM(CASE WHEN role != 'reviewer' THEN 1 ELSE 0 END) AS attempt_count,
             (SELECT after_sha FROM execution
                WHERE task_id = ?
                  AND role != 'reviewer'
                  AND after_sha IS NOT NULL
                  AND COALESCE(before_sha, '') != after_sha
                ORDER BY created_at DESC, id DESC LIMIT 1) AS latest_commit_sha
         FROM execution WHERE task_id = ?",
        )
        .bind(task_id)
        .bind(task_id)
        .fetch_one(self.pool())
        .await?;
        Ok(ExecutionSetupTaskEvidenceRow { row: rows })
    }
}

impl SqliteDb {
    pub async fn machine_precheck_task_holds_run_slot(
        &self,
        task_id: &str,
        role: &str,
        executor_role: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution WHERE task_id = ? AND status = 'running' AND role IN (?, ?))",
    ).bind(task_id).bind(role).bind(executor_role).fetch_one(self.pool()).await?)
    }
}

pub struct ShutdownExecutionSourceRow {
    row: SqliteRow,
}
impl ShutdownExecutionSourceRow {
    pub fn id(&self) -> String {
        self.row.get("id")
    }
}

impl SqliteDb {
    pub async fn shutdown_running_execution_sources(
        &self,
        task_id: &str,
    ) -> crate::Result<Vec<ShutdownExecutionSourceRow>> {
        let rows = sqlx::query("SELECT id FROM execution WHERE task_id = ? AND status = 'running'")
            .bind(task_id)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ShutdownExecutionSourceRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn agent_capacity_running_execution_count(
        &self,
        agent_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE agent_id = ? AND status = 'running'",
        )
        .bind(agent_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_running_execution_id(
        &self,
        workspace_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM execution
                 WHERE workspace_id = ? AND status = 'running'
                 ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(workspace_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_has_running_execution(&self, workspace_id: &str) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM execution WHERE workspace_id = ? AND status = 'running')",
        )
        .bind(workspace_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn execution_action_authority_ids(
        &self,
        task_id: &str,
        current_role: Option<&str>,
        blocked_execution_id: Option<&str>,
        coder_role: &str,
    ) -> crate::Result<Vec<String>> {
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT DISTINCT id FROM (");
        let mut first = true;

        macro_rules! candidate_static {
            ($predicate:expr) => {{
                if !first {
                    query.push(" UNION ALL ");
                }
                first = false;
                query
                    .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                    .push_bind(task_id)
                    .push(" AND ")
                    .push($predicate)
                    .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
            }};
        }

        macro_rules! candidate_role {
            ($predicate:expr, $role:expr) => {{
                if !first {
                    query.push(" UNION ALL ");
                }
                first = false;
                query
                    .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                    .push_bind(task_id)
                    .push(" AND ")
                    .push($predicate)
                    .push_bind($role)
                    .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
            }};
        }

        macro_rules! candidate_coder_roles {
            ($predicate:expr, $role:expr) => {{
                if !first {
                    query.push(" UNION ALL ");
                }
                first = false;
                query
                    .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                    .push_bind(task_id)
                    .push(" AND ")
                    .push($predicate)
                    .push_bind($role)
                    .push(", 'executor') ORDER BY created_at DESC, id DESC LIMIT 1)");
            }};
        }

        macro_rules! candidate_id {
            ($execution_id:expr) => {{
                if !first {
                    query.push(" UNION ALL ");
                }
                query
                    .push("SELECT id FROM (SELECT id FROM execution WHERE task_id = ")
                    .push_bind(task_id)
                    .push(" AND id = ")
                    .push_bind($execution_id)
                    .push(" ORDER BY created_at DESC, id DESC LIMIT 1)");
            }};
        }

        candidate_static!("status != 'running'");
        candidate_static!("status != 'running' AND agent_session_id IS NOT NULL");
        // `action_agent_id` can fall back to the newest execution with an agent
        // identity when there is no role assignment. It includes running rows;
        // preserve that authority class in the bounded projection as well.
        // `action_agent_id` falls back to the newest agent-backed execution even
        // when that row is still running. Keep that exact authority source in the
        // bounded projection so the `/actions` fingerprint observes the same row
        // the resolver can use.
        candidate_static!("agent_id IS NOT NULL");
        candidate_static!("status = 'running'");
        candidate_static!("status = 'running' AND role = 'interactive'");
        if let Some(role) = current_role {
            if role == coder_role {
                candidate_coder_roles!("status != 'running' AND role IN (", role);
                candidate_coder_roles!(
                    "status != 'running' AND agent_session_id IS NOT NULL AND role IN (",
                    role
                );
                candidate_coder_roles!(
                    "status != 'running' AND agent_id IS NOT NULL AND role IN (",
                    role
                );
                candidate_coder_roles!("status = 'running' AND role IN (", role);
            } else {
                candidate_role!("status != 'running' AND role = ", role);
                candidate_role!(
                    "status != 'running' AND agent_session_id IS NOT NULL AND role = ",
                    role
                );
                candidate_role!(
                    "status != 'running' AND agent_id IS NOT NULL AND role = ",
                    role
                );
                candidate_role!("status = 'running' AND role = ", role);
            }
        }
        if let Some(execution_id) = blocked_execution_id {
            candidate_id!(execution_id);
        }

        // The resolver consumes this as a set, but the `/actions` coherence
        // fingerprint compares the projected rows directly. Make the projection
        // order deterministic so an unchanged authority snapshot cannot look
        // different merely because SQLite chose another UNION scan order.
        query.push(") candidates ORDER BY id");
        let rows = query.build().fetch_all(self.pool()).await?;
        let execution_ids = rows
            .iter()
            .map(|row| row.try_get::<String, _>("id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(execution_ids)
    }
}

impl SqliteDb {
    pub async fn previous_sibling_execution_head(
        &self,
        root_id: &str,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT e.after_sha FROM execution e JOIN task sibling ON sibling.id = e.task_id
         WHERE sibling.parent_task_id = ?1 AND sibling.id != ?2
           AND e.after_sha IS NOT NULL AND e.after_sha != ''
           AND e.created_at <= COALESCE(
               (SELECT MIN(own.created_at) FROM execution own WHERE own.task_id = ?2),
               e.created_at)
         ORDER BY e.created_at DESC, e.rowid DESC LIMIT 1",
        )
        .bind(root_id)
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn retired_review_settlement_execution_ids(
        &self,
        task_id: &str,
        error: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT id FROM execution WHERE task_id = ? AND status <> 'running' AND error = ?",
        )
        .bind(task_id)
        .bind(error)
        .fetch_all(self.pool())
        .await?)
    }
}
