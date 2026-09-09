use super::*;
use crate::MarkUsageInvocationUnsettled;
use crate::{AgentExecutionStats, StartUsageInvocation};
use std::collections::HashSet;

#[async_trait]
impl ExecutionRepo for SqliteDb {
    async fn create(&self, input: CreateExecution) -> Result<Execution> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let execution = Self::create_execution_in_tx(&mut transaction, &input).await?;
        transaction.commit().await?;
        Ok(execution)
    }

    async fn create_with_lease(
        &self,
        input: CreateExecution,
        lease: ClaimExecutionLease,
    ) -> Result<Execution> {
        if input.status != ExecutionStatus::Running
            || lease.execution_id != input.id
            || lease.expected_version != 1
            || lease.owner.trim().is_empty()
            || lease.lease_expires_at > lease.hard_deadline_at
        {
            return Err(DbError::Check(
                "initial execution lease must match a running execution and bounded owner claim"
                    .to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        Self::create_execution_in_tx(&mut transaction, &input).await?;
        let result = sqlx::query(
            "UPDATE execution
             SET lease_owner = ?,
                 lease_expires_at = MIN(?, ?),
                 hard_deadline_at = ?,
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ? AND status = 'running'
               AND execution_version = 1 AND lease_owner IS NULL",
        )
        .bind(&lease.owner)
        .bind(&lease.lease_expires_at)
        .bind(&lease.hard_deadline_at)
        .bind(&lease.hard_deadline_at)
        .bind(&lease.now)
        .bind(&lease.now)
        .bind(&lease.execution_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let execution = execution_in_tx(&mut transaction, &input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        transaction.commit().await?;
        Ok(execution)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Execution>> {
        sqlx::query("SELECT * FROM execution WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_execution)
            .transpose()
    }

    async fn stats_by_agent(&self, agent_id: &str) -> Result<AgentExecutionStats> {
        let run_row = sqlx::query(
            "SELECT \
                COUNT(*) AS task_execution_count, \
                COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_runs, \
                AVG(CASE \
                    WHEN status != 'running' \
                    THEN (JULIANDAY(updated_at) - JULIANDAY(created_at)) * 86400000 \
                    ELSE NULL \
                END) AS avg_duration_ms \
             FROM execution \
             WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?;

        let task_execution_count: i64 = run_row.try_get("task_execution_count")?;
        let completed_runs: i64 = run_row.try_get("completed_runs")?;
        let avg_duration_ms = run_row
            .try_get::<Option<f64>, _>("avg_duration_ms")?
            .map(|duration| duration.round() as i64);
        let success_rate = if task_execution_count > 0 {
            Some(completed_runs as f64 / task_execution_count as f64)
        } else {
            None
        };

        Ok(AgentExecutionStats {
            avg_duration_ms,
            success_rate,
        })
    }

    async fn list_by_task(&self, task_id: &str, page: PageRequest) -> Result<Page<Execution>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM execution WHERE task_id = ? ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_execution)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution WHERE task_id = ?")
                    .bind(task_id)
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn list_latest_executions_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Execution>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM (
                SELECT execution.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY task_id
                           ORDER BY created_at DESC, id DESC
                       ) AS rn
                FROM execution
                WHERE task_id IN (",
        );
        let mut separated = query.separated(", ");
        for task_id in task_ids {
            separated.push_bind(*task_id);
        }
        separated.push_unseparated(
            ")
            ) ranked
            WHERE rn = 1
            ORDER BY task_id ASC",
        );
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_execution).collect()
    }

    async fn list_by_task_and_role(
        &self,
        task_id: &str,
        role: &str,
        page: PageRequest,
    ) -> Result<Page<Execution>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM execution WHERE task_id = ? AND role = ? ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .bind(role)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_execution)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = ?",
                )
                .bind(task_id)
                .bind(role)
                .fetch_one(&self.pool)
                .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn count_by_task_and_role(&self, task_id: &str, role: &str) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = ?",
        )
        .bind(task_id)
        .bind(role)
        .fetch_one(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn update(&self, input: UpdateExecution) -> Result<Execution> {
        // Terminal status changes must pass through the owner/version CAS
        // boundary below.  Leaving this field accepted would reintroduce the
        // stale read-then-update path that can overwrite a concurrent winner.
        if input.status.is_some() {
            return Err(DbError::InvalidTransition);
        }

        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE execution SET ");
        let mut needs_comma = false;
        macro_rules! push_assignment {
            ($column:literal, $value:expr) => {{
                if needs_comma {
                    query.push(", ");
                }
                needs_comma = true;
                query.push($column).push(" = ").push_bind($value);
            }};
        }
        if let Some(agent_session_id) = input.agent_session_id {
            push_assignment!("agent_session_id", agent_session_id);
        }
        if let Some(agent_message_id) = input.agent_message_id {
            push_assignment!("agent_message_id", agent_message_id);
        }
        if let Some(last_activity_at) = input.last_activity_at {
            push_assignment!("last_activity_at", last_activity_at);
        }
        if let Some(summary) = input.summary {
            push_assignment!("summary", summary);
        }
        if let Some(logs_path) = input.logs_path {
            push_assignment!("logs_path", logs_path);
        }
        if let Some(before_sha) = input.before_sha {
            push_assignment!("before_sha", before_sha);
        }
        if let Some(after_sha) = input.after_sha {
            push_assignment!("after_sha", after_sha);
        }
        if let Some(error) = input.error {
            push_assignment!("error", error);
        }
        if let Some(executor_config_snapshot_json) = input.executor_config_snapshot_json {
            push_assignment!(
                "executor_config_snapshot_json",
                executor_config_snapshot_json
            );
        }
        if let Some(stop_reason) = input.stop_reason {
            push_assignment!("stop_reason", stop_reason.map(|value| value.to_string()));
        }
        if let Some(stopped_by) = input.stopped_by {
            push_assignment!("stopped_by", stopped_by);
        }
        if let Some(resume_policy) = input.resume_policy {
            push_assignment!(
                "resume_policy",
                resume_policy.map(|value| value.to_string())
            );
        }
        if let Some(stopped_at) = input.stopped_at {
            push_assignment!("stopped_at", stopped_at);
        }
        if needs_comma {
            query.push(", ");
        }
        query.push("updated_at = ").push_bind(&input.updated_at);
        query.push(" WHERE id = ").push_bind(&input.id);
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        query.build().execute(&mut *transaction).await?;
        let updated = sqlx::query("SELECT * FROM execution WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_execution)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        transaction.commit().await?;
        Ok(updated)
    }

    async fn claim_lease(&self, input: ClaimExecutionLease) -> Result<ExecutionLeaseMutation> {
        if input.expected_version < 1 || input.owner.trim().is_empty() {
            return Err(DbError::Check(
                "execution lease claim requires a positive version and owner".to_owned(),
            ));
        }
        if input.lease_expires_at > input.hard_deadline_at {
            return Err(DbError::Check(
                "execution lease expiry cannot exceed its hard deadline".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE execution
             SET lease_owner = ?,
                 lease_expires_at = MIN(?, COALESCE(hard_deadline_at, ?)),
                 hard_deadline_at = COALESCE(hard_deadline_at, ?),
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ?
               AND status = 'running'
               AND execution_version = ?
               AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?)
               AND (hard_deadline_at IS NULL OR hard_deadline_at > ?)",
        )
        .bind(&input.owner)
        .bind(&input.lease_expires_at)
        .bind(&input.hard_deadline_at)
        .bind(&input.hard_deadline_at)
        .bind(&input.now)
        .bind(&input.now)
        .bind(&input.execution_id)
        .bind(input.expected_version)
        .bind(&input.now)
        .bind(&input.now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
            let outcome = if current.as_ref().is_some_and(|execution| {
                execution.hard_deadline_at.as_deref() <= Some(input.now.as_str())
            }) {
                ExecutionLeaseMutation::HardDeadline { current }
            } else {
                ExecutionLeaseMutation::Concurrent { current }
            };
            transaction.rollback().await?;
            return Ok(outcome);
        }
        let execution = execution_in_tx(&mut transaction, &input.execution_id)
            .await?
            .ok_or(DbError::NotFound)?;
        transaction.commit().await?;
        Ok(ExecutionLeaseMutation::Updated(execution))
    }

    async fn renew_lease(&self, input: RenewExecutionLease) -> Result<ExecutionLeaseMutation> {
        if input.expected_version < 1 || input.owner.trim().is_empty() {
            return Err(DbError::Check(
                "execution lease renewal requires a positive version and owner".to_owned(),
            ));
        }
        if input.lease_expires_at <= input.now {
            return Err(DbError::Check(
                "execution lease expiry must be after the heartbeat time".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query(
            "UPDATE execution
             SET lease_expires_at = MIN(?, COALESCE(hard_deadline_at, ?)),
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ?
               AND status = 'running'
               AND execution_version = ?
               AND lease_owner = ?
               AND lease_expires_at > ?
               AND hard_deadline_at > ?",
        )
        .bind(&input.lease_expires_at)
        .bind(&input.lease_expires_at)
        .bind(&input.now)
        .bind(&input.now)
        .bind(&input.execution_id)
        .bind(input.expected_version)
        .bind(&input.owner)
        .bind(&input.now)
        .bind(&input.now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
            let outcome = if current.as_ref().is_some_and(|execution| {
                execution.hard_deadline_at.as_deref() <= Some(input.now.as_str())
            }) {
                ExecutionLeaseMutation::HardDeadline { current }
            } else {
                ExecutionLeaseMutation::Concurrent { current }
            };
            transaction.rollback().await?;
            return Ok(outcome);
        }
        let execution = execution_in_tx(&mut transaction, &input.execution_id)
            .await?
            .ok_or(DbError::NotFound)?;
        transaction.commit().await?;
        Ok(ExecutionLeaseMutation::Updated(execution))
    }

    async fn record_progress(
        &self,
        input: RecordExecutionProgress,
    ) -> Result<ExecutionLeaseMutation> {
        if input.expected_version < 1 || input.owner.trim().is_empty() {
            return Err(DbError::Check(
                "execution progress requires a positive version and owner".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
        let eligible = current.as_ref().is_some_and(|execution| {
            execution.status == ExecutionStatus::Running
                && execution.execution_version == input.expected_version
                && execution.lease_owner.as_deref() == Some(input.owner.as_str())
                && execution
                    .lease_expires_at
                    .as_deref()
                    .is_some_and(|expires_at| expires_at > input.now.as_str())
                && execution
                    .hard_deadline_at
                    .as_deref()
                    .is_some_and(|deadline| deadline > input.now.as_str())
        });
        if !eligible {
            let outcome = if current.as_ref().is_some_and(|execution| {
                execution.hard_deadline_at.as_deref() <= Some(input.now.as_str())
            }) {
                ExecutionLeaseMutation::HardDeadline { current }
            } else {
                ExecutionLeaseMutation::Concurrent { current }
            };
            transaction.rollback().await?;
            return Ok(outcome);
        }

        let current = current.ok_or(DbError::NotFound)?;
        // Semantic progress is an ordered stream.  A delayed remote/log batch
        // must not move the liveness watermark backwards or create another
        // warning epoch; duplicate timestamps are no-ops as well.
        if current
            .last_progress_at
            .as_deref()
            .is_some_and(|last_progress_at| {
                progress_timestamp_is_not_newer(&input.progress_at, last_progress_at)
            })
        {
            transaction.rollback().await?;
            return Ok(ExecutionLeaseMutation::Updated(current));
        }

        let result = sqlx::query(
            "UPDATE execution
             SET last_progress_at = ?, execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ?
               AND status = 'running'
               AND execution_version = ?
               AND lease_owner = ?
               AND lease_expires_at > ?
               AND hard_deadline_at > ?",
        )
        .bind(&input.progress_at)
        .bind(&input.now)
        .bind(&input.execution_id)
        .bind(input.expected_version)
        .bind(&input.owner)
        .bind(&input.now)
        .bind(&input.now)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
            let outcome = if current.as_ref().is_some_and(|execution| {
                execution.hard_deadline_at.as_deref() <= Some(input.now.as_str())
            }) {
                ExecutionLeaseMutation::HardDeadline { current }
            } else {
                ExecutionLeaseMutation::Concurrent { current }
            };
            transaction.rollback().await?;
            return Ok(outcome);
        }
        let execution = execution_in_tx(&mut transaction, &input.execution_id)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                .bind(&execution.task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let event_id = new_uuid_v4();
        let progress_event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: "execution.progressed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: execution.task_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: Some(input.owner.clone()),
            scope_type: if project_id.is_some() {
                "project".to_owned()
            } else {
                "task".to_owned()
            },
            scope_id: project_id.unwrap_or_else(|| execution.task_id.clone()),
            correlation_id: event_id,
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "execution-progress:{}:{}",
                execution.id, input.progress_at
            )),
            payload_json: serde_json::json!({
                "execution_id": execution.id,
                "task_id": execution.task_id,
                "last_progress_at": execution.last_progress_at,
            })
            .to_string(),
            created_at: input.now.clone(),
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &progress_event).await?;
        transaction.commit().await?;
        Ok(ExecutionLeaseMutation::Updated(execution))
    }

    async fn record_progress_warning(
        &self,
        input: RecordExecutionProgressWarning,
    ) -> Result<ExecutionProgressWarningOutcome> {
        if input.expected_version < 1 || input.owner.trim().is_empty() {
            return Err(DbError::Check(
                "execution progress warning requires a positive version and owner".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        // BEGIN IMMEDIATE serializes this validation with heartbeat, semantic
        // progress, and terminal CAS writers.  Warning publication itself is
        // intentionally non-mutating: it is an Attention projection signal,
        // not another lease-version bump that would invalidate the owner's
        // cached heartbeat handle.
        let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
        let eligible = current.as_ref().is_some_and(|execution| {
            execution.status == ExecutionStatus::Running
                && execution.execution_version == input.expected_version
                && execution.lease_owner.as_deref() == Some(input.owner.as_str())
                && execution
                    .lease_expires_at
                    .as_deref()
                    .is_some_and(|expires_at| expires_at > input.now.as_str())
                && execution
                    .hard_deadline_at
                    .as_deref()
                    .is_some_and(|deadline| deadline > input.now.as_str())
                && match (
                    execution.last_progress_at.as_deref(),
                    input.expected_last_progress_at.as_deref(),
                ) {
                    (Some(actual), Some(expected)) => {
                        actual == expected
                            && progress_timestamp_is_before(actual, &input.stale_before)
                    }
                    (None, None) => {
                        progress_timestamp_is_before(&execution.created_at, &input.stale_before)
                    }
                    _ => false,
                }
        });
        if !eligible {
            transaction.rollback().await?;
            return Ok(ExecutionProgressWarningOutcome::Concurrent { current });
        }

        let execution = current.ok_or(DbError::NotFound)?;
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                .bind(&execution.task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let event_id = new_uuid_v4();
        let progress_warning_dedupe_key = format!(
            "execution-progress-warning:{}:{}",
            execution.id,
            input.expected_last_progress_at.as_deref().unwrap_or("none")
        );
        let progress_warning_event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: "execution.progress_warning".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: execution.task_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: Some(input.owner),
            scope_type: if project_id.is_some() {
                "project".to_owned()
            } else {
                "task".to_owned()
            },
            scope_id: project_id.unwrap_or_else(|| execution.task_id.clone()),
            // Keep event semantics stable across monitor scans and heartbeat
            // renewals.  The dedupe identity is the execution plus semantic
            // progress epoch, not the transient execution version.
            correlation_id: progress_warning_dedupe_key.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(progress_warning_dedupe_key),
            payload_json: serde_json::json!({
                "execution_id": execution.id,
                "task_id": execution.task_id,
                "last_progress_at": execution.last_progress_at,
            })
            .to_string(),
            created_at: input.now,
        };
        let event =
            DomainEventRepo::append_event_in_tx(self, &mut transaction, &progress_warning_event)
                .await?;
        transaction.commit().await?;
        if event.id == progress_warning_event.id {
            Ok(ExecutionProgressWarningOutcome::Committed { execution, event })
        } else {
            Ok(ExecutionProgressWarningOutcome::Replayed { execution, event })
        }
    }

    async fn terminalize(&self, input: TerminalizeExecution) -> Result<ExecutionTerminalOutcome> {
        if input.expected_version < 1 {
            return Err(DbError::Check(
                "execution terminalization requires a positive version".to_owned(),
            ));
        }
        if input.status == ExecutionStatus::Running {
            return Err(DbError::InvalidTransition);
        }
        if !(0..=16).contains(&input.causation_depth) {
            return Err(DbError::Check(
                "execution terminal event causation depth must be between 0 and 16".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE execution SET status = ");
        query.push_bind(input.status.to_string());
        if let Some(stop_reason) = input.stop_reason.as_ref() {
            query.push(", stop_reason = ");
            query.push_bind(stop_reason.as_ref().map(ToString::to_string));
        }
        if let Some(stopped_by) = input.stopped_by.as_ref() {
            query
                .push(", stopped_by = ")
                .push_bind(stopped_by.as_deref());
        }
        query.push(", stopped_at = ");
        match input.stopped_at.as_ref() {
            Some(Some(stopped_at)) => query.push_bind(stopped_at),
            Some(None) | None => query.push_bind(input.updated_at.as_str()),
        };
        if let Some(resume_policy) = input.resume_policy.as_ref() {
            query.push(", resume_policy = ");
            query.push_bind(resume_policy.as_ref().map(ToString::to_string));
        }
        if let Some(agent_session_id) = input.agent_session_id.as_ref() {
            query
                .push(", agent_session_id = ")
                .push_bind(agent_session_id.as_deref());
        }
        if let Some(agent_message_id) = input.agent_message_id.as_ref() {
            query
                .push(", agent_message_id = ")
                .push_bind(agent_message_id.as_deref());
        }
        if let Some(last_activity_at) = input.last_activity_at.as_ref() {
            query
                .push(", last_activity_at = ")
                .push_bind(last_activity_at.as_deref());
        }
        if let Some(last_progress_at) = input.last_progress_at.as_ref() {
            query
                .push(", last_progress_at = ")
                .push_bind(last_progress_at.as_deref());
        }
        if let Some(summary) = input.summary.as_ref() {
            query.push(", summary = ").push_bind(summary.as_deref());
        }
        if let Some(logs_path) = input.logs_path.as_ref() {
            query.push(", logs_path = ").push_bind(logs_path.as_deref());
        }
        if let Some(before_sha) = input.before_sha.as_ref() {
            query
                .push(", before_sha = ")
                .push_bind(before_sha.as_deref());
        }
        if let Some(after_sha) = input.after_sha.as_ref() {
            query.push(", after_sha = ").push_bind(after_sha.as_deref());
        }
        if let Some(error) = input.error.as_ref() {
            query.push(", error = ").push_bind(error.as_deref());
        }
        if let Some(snapshot) = input.executor_config_snapshot_json.as_ref() {
            query
                .push(", executor_config_snapshot_json = ")
                .push_bind(snapshot.as_deref());
        }
        query.push(
            ", lease_owner = NULL, lease_expires_at = NULL,
                 execution_version = execution_version + 1,
                 updated_at = ",
        );
        query.push_bind(&input.updated_at);
        query.push(" WHERE id = ");
        query.push_bind(&input.execution_id);
        query.push(" AND status = 'running' AND execution_version = ");
        query.push_bind(input.expected_version);
        if let Some(owner) = input.lease_owner.as_deref() {
            query.push(" AND lease_owner = ").push_bind(owner);
        }

        let result = query.build().execute(&mut *transaction).await?;
        if result.rows_affected() != 1 {
            let current = execution_in_tx(&mut transaction, &input.execution_id).await?;
            transaction.rollback().await?;
            return Ok(ExecutionTerminalOutcome::Concurrent { current });
        }

        // Capture and close the scheduler lease in this same transaction. A
        // task can have at most one active workspace lease, but the update is
        // intentionally keyed by execution as a defense against stale callers.
        let workspace_lease_id = sqlx::query_scalar::<_, String>(
            "SELECT id FROM workspace_lease
             WHERE execution_id = ? AND status = 'active'
             ORDER BY issued_at DESC, id DESC LIMIT 1",
        )
        .bind(&input.execution_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let workspace_lease_status =
            workspace_lease_id
                .as_ref()
                .map(|_| match input.lease_disposition {
                    ExecutionLeaseDisposition::Revoke => "revoked".to_owned(),
                    ExecutionLeaseDisposition::Expire => "expired".to_owned(),
                });
        if let Some(status) = workspace_lease_status.as_deref() {
            sqlx::query(
                "UPDATE workspace_lease
                 SET status = ?, revoked_at = ?, version = version + 1,
                     updated_at = ?
                 WHERE execution_id = ? AND status = 'active'",
            )
            .bind(status)
            .bind(&input.updated_at)
            .bind(&input.updated_at)
            .bind(&input.execution_id)
            .execute(&mut *transaction)
            .await?;
        }

        let updated = execution_in_tx(&mut transaction, &input.execution_id)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                .bind(&updated.task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let event_id = new_uuid_v4();
        let event_type = match updated.status {
            ExecutionStatus::Completed => "execution.completed",
            ExecutionStatus::Failed => "execution.failed",
            ExecutionStatus::Cancelled => "execution.cancelled",
            ExecutionStatus::Running => unreachable!("terminal CAS rejects running status"),
        };
        let event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: updated.task_id.clone(),
            actor_type: input.actor_type,
            actor_id: input.actor_id,
            scope_type: if project_id.is_some() {
                "project".to_owned()
            } else {
                "task".to_owned()
            },
            scope_id: project_id
                .clone()
                .unwrap_or_else(|| updated.task_id.clone()),
            correlation_id: input
                .correlation_id
                .unwrap_or_else(|| event_id.clone()),
            causation_id: input.causation_id,
            causation_depth: input.causation_depth,
            dedupe_key: Some(format!(
                "execution-terminal:{}:{}",
                updated.id, updated.status
            )),
            payload_json: serde_json::json!({
                "execution_id": updated.id,
                "task_id": updated.task_id,
                "project_id": project_id,
                "role": updated.role,
                "status": updated.status.to_string(),
                // Preserve the winning owner predicate for late-result
                // diagnostics after terminalization clears lease_owner.
                "previous_lease_owner": input.lease_owner,
                "stop_reason": updated.stop_reason.as_ref().map(ToString::to_string),
                "error": updated.error.as_deref().map(|value| value.chars().take(500).collect::<String>()),
                "workspace_lease_id": workspace_lease_id,
                "workspace_lease_status": workspace_lease_status,
            })
            .to_string(),
            created_at: input.updated_at,
        };
        let event = DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        Ok(ExecutionTerminalOutcome::Committed {
            execution: updated,
            event: Box::new(event),
            workspace_lease_id,
            workspace_lease_status,
            replayed: false,
        })
    }

    async fn terminalize_with_ledger(
        &self,
        input: TerminalizeExecutionWithLedger,
    ) -> Result<ExecutionTerminalOutcome> {
        self.terminalize_with_ledger_and_invocations(input, Vec::new())
            .await
    }

    async fn terminalize_with_ledger_and_invocations(
        &self,
        input: TerminalizeExecutionWithLedger,
        remote_invocations: Vec<CreateUsageInvocation>,
    ) -> Result<ExecutionTerminalOutcome> {
        let terminal = &input.terminal;
        if terminal.expected_version < 1 {
            return Err(DbError::Check(
                "execution terminalization requires a positive version".to_owned(),
            ));
        }
        if terminal.status == ExecutionStatus::Running {
            return Err(DbError::InvalidTransition);
        }
        if !(0..=16).contains(&terminal.causation_depth) {
            return Err(DbError::Check(
                "execution terminal event causation depth must be between 0 and 16".to_owned(),
            ));
        }
        if input.terminal_report_id.is_some() != input.terminal_report_digest.is_some() {
            return Err(DbError::Check(
                "terminal report identity requires a complete id and digest".to_owned(),
            ));
        }
        if input
            .terminal_report_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || input
                .terminal_report_digest
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(DbError::Check(
                "terminal report identity cannot be empty".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;

        // A remote daemon may replay a fully committed terminal payload after
        // a server restart. The receipt table is the durable idempotency
        // authority; compare both id and digest before attempting the
        // execution CAS. Search the whole database before attempting that
        // CAS: a terminal_report_id is globally unique, not merely unique
        // within one execution. Reusing it for a different execution or
        // payload is an idempotency conflict even when that execution is
        // still running.
        if let (Some(report_id), Some(report_digest)) = (
            input.terminal_report_id.as_deref(),
            input.terminal_report_digest.as_deref(),
        ) {
            if let Some(receipt) = terminal_receipt_in_tx(
                &mut transaction,
                report_id,
                report_digest,
                &terminal.execution_id,
                terminal.lease_owner.as_deref(),
            )
            .await?
            {
                transaction.commit().await?;
                return Ok(receipt);
            }
        }

        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE execution SET status = ");
        query.push_bind(terminal.status.to_string());
        if let Some(stop_reason) = terminal.stop_reason.as_ref() {
            query.push(", stop_reason = ");
            query.push_bind(stop_reason.as_ref().map(ToString::to_string));
        }
        if let Some(stopped_by) = terminal.stopped_by.as_ref() {
            query
                .push(", stopped_by = ")
                .push_bind(stopped_by.as_deref());
        }
        query.push(", stopped_at = ");
        match terminal.stopped_at.as_ref() {
            Some(Some(stopped_at)) => query.push_bind(stopped_at),
            Some(None) | None => query.push_bind(terminal.updated_at.as_str()),
        };
        if let Some(resume_policy) = terminal.resume_policy.as_ref() {
            query.push(", resume_policy = ");
            query.push_bind(resume_policy.as_ref().map(ToString::to_string));
        }
        if let Some(agent_session_id) = terminal.agent_session_id.as_ref() {
            query
                .push(", agent_session_id = ")
                .push_bind(agent_session_id.as_deref());
        }
        if let Some(agent_message_id) = terminal.agent_message_id.as_ref() {
            query
                .push(", agent_message_id = ")
                .push_bind(agent_message_id.as_deref());
        }
        if let Some(last_activity_at) = terminal.last_activity_at.as_ref() {
            query
                .push(", last_activity_at = ")
                .push_bind(last_activity_at.as_deref());
        }
        if let Some(last_progress_at) = terminal.last_progress_at.as_ref() {
            query
                .push(", last_progress_at = ")
                .push_bind(last_progress_at.as_deref());
        }
        if let Some(summary) = terminal.summary.as_ref() {
            query.push(", summary = ").push_bind(summary.as_deref());
        }
        if let Some(logs_path) = terminal.logs_path.as_ref() {
            query.push(", logs_path = ").push_bind(logs_path.as_deref());
        }
        if let Some(before_sha) = terminal.before_sha.as_ref() {
            query
                .push(", before_sha = ")
                .push_bind(before_sha.as_deref());
        }
        if let Some(after_sha) = terminal.after_sha.as_ref() {
            query.push(", after_sha = ").push_bind(after_sha.as_deref());
        }
        if let Some(error) = terminal.error.as_ref() {
            query.push(", error = ").push_bind(error.as_deref());
        }
        if let Some(snapshot) = terminal.executor_config_snapshot_json.as_ref() {
            query
                .push(", executor_config_snapshot_json = ")
                .push_bind(snapshot.as_deref());
        }
        query.push(
            ", lease_owner = NULL, lease_expires_at = NULL,
                 execution_version = execution_version + 1,
                 updated_at = ",
        );
        query.push_bind(&terminal.updated_at);
        query.push(" WHERE id = ");
        query.push_bind(&terminal.execution_id);
        query.push(" AND status = 'running' AND execution_version = ");
        query.push_bind(terminal.expected_version);
        if let Some(owner) = terminal.lease_owner.as_deref() {
            query.push(" AND lease_owner = ").push_bind(owner);
            // Only a remote owner reporting its own outcome proves liveness
            // here; see `require_live_owner_lease`. The lease token alone
            // cannot stand in for that intent, because a local caller carries
            // whatever owner the row already holds — including a daemon token.
            // Gating on the token instead left every expired lease
            // un-terminalizable by the two callers that exist to clear one: a
            // user cancel and the recovery reaper both lost the CAS forever,
            // and reported the untouched running row as success.
            if input.require_live_owner_lease {
                query.push(
                    " AND lease_expires_at IS NOT NULL AND julianday(lease_expires_at) > julianday(",
                );
                query.push_bind(&terminal.updated_at);
                query.push(")");
                query.push(
                    " AND hard_deadline_at IS NOT NULL AND julianday(hard_deadline_at) > julianday(",
                );
                query.push_bind(&terminal.updated_at);
                query.push(")");
            }
        }

        let result = query.build().execute(&mut *transaction).await?;
        let terminal_cas_won = result.rows_affected() == 1;
        let current_after_cas = if terminal_cas_won {
            None
        } else {
            let current = execution_in_tx(&mut transaction, &terminal.execution_id).await?;
            if !input.allow_late_settlement
                || current
                    .as_ref()
                    .is_none_or(|execution| execution.status == ExecutionStatus::Running)
            {
                transaction.rollback().await?;
                return Ok(ExecutionTerminalOutcome::Concurrent { current });
            }
            current
        };

        let (workspace_lease_id, workspace_lease_status) = if terminal_cas_won {
            let workspace_lease_id = sqlx::query_scalar::<_, String>(
                "SELECT id FROM workspace_lease
                 WHERE execution_id = ? AND status = 'active'
                 ORDER BY issued_at DESC, id DESC LIMIT 1",
            )
            .bind(&terminal.execution_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let workspace_lease_status =
                workspace_lease_id
                    .as_ref()
                    .map(|_| match terminal.lease_disposition {
                        ExecutionLeaseDisposition::Revoke => "revoked".to_owned(),
                        ExecutionLeaseDisposition::Expire => "expired".to_owned(),
                    });
            if let Some(status) = workspace_lease_status.as_deref() {
                sqlx::query(
                    "UPDATE workspace_lease
                     SET status = ?, revoked_at = ?, version = version + 1,
                         updated_at = ?
                     WHERE execution_id = ? AND status = 'active'",
                )
                .bind(status)
                .bind(&terminal.updated_at)
                .bind(&terminal.updated_at)
                .bind(&terminal.execution_id)
                .execute(&mut *transaction)
                .await?;
            }
            (workspace_lease_id, workspace_lease_status)
        } else {
            (None, None)
        };

        let updated = match current_after_cas {
            Some(current) => current,
            None => execution_in_tx(&mut transaction, &terminal.execution_id)
                .await?
                .ok_or(DbError::NotFound)?,
        };

        // A remote late-drain is allowed only when the terminal event proves
        // that this same daemon owned the lease which was displaced.  The
        // service performs the read-side gate before pricing work; repeat the
        // proof inside this transaction so an owner takeover between those
        // reads cannot materialize invocations or append a usage event to a
        // victim execution.
        if !terminal_cas_won && input.allow_late_settlement {
            if let Some(expected_owner) = terminal.lease_owner.as_deref() {
                let previous_owner = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT json_extract(payload_json, '$.previous_lease_owner')
                     FROM domain_event
                     WHERE entity_type = 'task'
                       AND entity_id = ?
                       AND event_type IN (
                           'execution.completed', 'execution.failed',
                           'execution.cancelled', 'execution.terminal_report.received'
                       )
                       AND json_extract(payload_json, '$.execution_id') = ?
                     ORDER BY sequence DESC
                     LIMIT 1",
                )
                .bind(&updated.task_id)
                .bind(&updated.id)
                .fetch_optional(&mut *transaction)
                .await?
                .flatten();
                if !previous_owner
                    .as_deref()
                    .is_some_and(|owner| same_remote_daemon_owner(owner, expected_owner))
                {
                    transaction.rollback().await?;
                    return Ok(ExecutionTerminalOutcome::Concurrent {
                        current: Some(updated),
                    });
                }
            }
        }

        // The service normally prepares these rows from the target
        // execution's selections, but keep the composite boundary defensive:
        // a malformed caller must not be able to terminalize one execution
        // while materializing an invocation belonging to another source. This
        // validation deliberately occurs only after the owner CAS (or the
        // durable late-owner proof above), while BEGIN IMMEDIATE is held. No
        // pricing-selection read is exposed to a caller that lost ownership
        // or was taken over concurrently.
        for invocation in &remote_invocations {
            if invocation.domain_kind != crate::PricingDomainKind::Execution
                || invocation.surface != UsageSurface::TaskExecution
                || invocation.source_id != terminal.execution_id
                || invocation.execution_id.as_deref() != Some(terminal.execution_id.as_str())
            {
                return Err(DbError::IdempotencyConflict);
            }
            let Some(selection) = sqlx::query(
                "SELECT source_id, execution_id, domain_kind, surface,
                        candidate_key, attempt_ordinal
                 FROM pricing_selection WHERE id = ?",
            )
            .bind(&invocation.pricing_selection_id)
            .fetch_optional(&mut *transaction)
            .await?
            else {
                return Err(DbError::NotFound);
            };
            let selection_source_id: String = selection.try_get("source_id")?;
            let selection_execution_id: Option<String> = selection.try_get("execution_id")?;
            let selection_domain_kind: String = selection.try_get("domain_kind")?;
            let selection_surface: String = selection.try_get("surface")?;
            let selection_candidate_key: Option<String> = selection.try_get("candidate_key")?;
            let selection_attempt_ordinal: i64 = selection.try_get("attempt_ordinal")?;
            if selection_source_id != terminal.execution_id
                || selection_execution_id.as_deref() != Some(terminal.execution_id.as_str())
                || selection_domain_kind != crate::PricingDomainKind::Execution.to_string()
                || selection_surface != UsageSurface::TaskExecution.to_string()
                || selection_candidate_key != invocation.candidate_key
                || selection_attempt_ordinal != invocation.attempt_ordinal
            {
                return Err(DbError::IdempotencyConflict);
            }
        }
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                .bind(&updated.task_id)
                .fetch_optional(&mut *transaction)
                .await?;
        if !terminal_cas_won && input.terminal_report_id.is_none() {
            transaction.rollback().await?;
            return Ok(ExecutionTerminalOutcome::Concurrent {
                current: Some(updated),
            });
        }
        let event_id = new_uuid_v4();
        let event_type = if terminal_cas_won {
            match updated.status {
                ExecutionStatus::Completed => "execution.completed",
                ExecutionStatus::Failed => "execution.failed",
                ExecutionStatus::Cancelled => "execution.cancelled",
                ExecutionStatus::Running => unreachable!("terminal CAS rejects running status"),
            }
        } else {
            "execution.terminal_report.received"
        };
        let event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: updated.task_id.clone(),
            actor_type: terminal.actor_type.clone(),
            actor_id: terminal.actor_id.clone(),
            scope_type: if project_id.is_some() {
                "project".to_owned()
            } else {
                "task".to_owned()
            },
            scope_id: project_id
                .clone()
                .unwrap_or_else(|| updated.task_id.clone()),
            correlation_id: terminal
                .correlation_id
                .clone()
                .unwrap_or_else(|| event_id.clone()),
            causation_id: terminal.causation_id.clone(),
            causation_depth: terminal.causation_depth,
            dedupe_key: if terminal_cas_won {
                Some(format!(
                    "execution-terminal:{}:{}",
                    updated.id, updated.status
                ))
            } else {
                Some(format!(
                    "execution-terminal-report:{}",
                    input.terminal_report_id.as_deref().ok_or_else(|| {
                        DbError::Check(
                            "late terminal report is missing its durable report id".to_owned(),
                        )
                    })?
                ))
            },
            payload_json: serde_json::json!({
                "execution_id": updated.id,
                "task_id": updated.task_id,
                "project_id": project_id,
                "role": updated.role,
                "status": updated.status.to_string(),
                "previous_lease_owner": terminal.lease_owner.clone(),
                "stop_reason": updated.stop_reason.as_ref().map(ToString::to_string),
                "error": updated.error.as_deref().map(|value| value.chars().take(500).collect::<String>()),
                "workspace_lease_id": workspace_lease_id,
                "workspace_lease_status": workspace_lease_status,
                "terminal_report_id": input.terminal_report_id.clone(),
                "terminal_report_digest": input.terminal_report_digest.clone(),
                "late_settlement": !terminal_cas_won,
            })
            .to_string(),
            created_at: terminal.updated_at.clone(),
        };

        // Remote terminal reports are evidence that the daemon crossed the
        // provider-call boundary.  Materialize and start those invocations
        // only after the ownership/CAS checks above, and keep the inserts in
        // this same transaction as terminalization and settlement.  A
        // conflict, receipt replay, or persistence failure therefore leaves
        // no newly observed invocation behind.
        for invocation_input in &remote_invocations {
            let invocation = self
                .create_usage_invocation_in_tx(&mut transaction, invocation_input.clone())
                .await?;
            if invocation.lifecycle == UsageInvocationLifecycle::Admitted {
                self.start_usage_invocation_in_tx(
                    &mut transaction,
                    StartUsageInvocation {
                        id: invocation.id,
                        expected_version: invocation.version,
                        started_at: terminal.updated_at.clone(),
                        updated_at: terminal.updated_at.clone(),
                    },
                )
                .await?;
            }
        }

        let mut settled_ids = HashSet::new();
        // A cancellation may carry reports observed before the cancellation
        // won the execution CAS. Settle those supplied invocations first and
        // only leave calls with no observed report pending for late drain.
        for settlement in &input.settlements {
            let belongs: Option<String> =
                sqlx::query_scalar("SELECT execution_id FROM usage_invocation WHERE id = ?")
                    .bind(&settlement.invocation_id)
                    .fetch_optional(&mut *transaction)
                    .await?;
            if belongs.as_deref() != Some(terminal.execution_id.as_str()) {
                return Err(DbError::IdempotencyConflict);
            }
            let mut expected_version = settlement.expected_version;
            let existing = crate::sqlite::pricing::select_invocation_in_tx(
                &mut transaction,
                &settlement.invocation_id,
            )
            .await?
            .ok_or(DbError::NotFound)?;
            if existing.lifecycle == UsageInvocationLifecycle::Settled {
                crate::sqlite::pricing::validate_settled_usage_invocation_in_tx(
                    &mut transaction,
                    &existing,
                    settlement,
                )
                .await?;
                settled_ids.insert(existing.id);
                continue;
            }
            if existing.lifecycle == UsageInvocationLifecycle::Unsettled {
                return Err(DbError::IdempotencyConflict);
            }
            if existing.lifecycle == UsageInvocationLifecycle::Admitted {
                self.start_usage_invocation_in_tx(
                    &mut transaction,
                    StartUsageInvocation {
                        id: existing.id,
                        expected_version: existing.version,
                        started_at: settlement.settled_at.clone(),
                        updated_at: settlement.updated_at.clone(),
                    },
                )
                .await?;
                expected_version = existing.version + 1;
            }
            let invocation = self
                .settle_usage_invocation_in_tx(
                    &mut transaction,
                    SettleUsageInvocation {
                        id: settlement.invocation_id.clone(),
                        expected_version,
                        telemetry_state: settlement.telemetry_state,
                        terminal_reason: settlement.terminal_reason.clone(),
                        settled_at: settlement.settled_at.clone(),
                        updated_at: settlement.updated_at.clone(),
                    },
                )
                .await?;
            settled_ids.insert(invocation.id);
            for usage_event in &settlement.events {
                self.append_usage_event_in_tx(&mut transaction, usage_event.clone())
                    .await?;
            }
        }

        if !terminal_cas_won {
            // The execution already has an authoritative terminal outcome.
            // Only supplied reports are drained here; untouched invocations
            // remain pending for a later replay and no execution/workspace
            // state is changed.
        } else if updated.status == ExecutionStatus::Cancelled {
            // Cancellation owns the domain outcome but leaves only calls
            // without an observed report pending so a late durable result can
            // settle their ledger later.
            sqlx::query(
                "UPDATE usage_invocation
                 SET lifecycle = 'pending_settlement', updated_at = ?,
                     version = version + 1
                 WHERE execution_id = ? AND lifecycle = 'started'",
            )
            .bind(&terminal.updated_at)
            .bind(&terminal.execution_id)
            .execute(&mut *transaction)
            .await?;
            if input.mark_unreplayable_pending_unsettled {
                // Recovery has already established that this execution was
                // owned by a local runtime and therefore cannot deliver a
                // replayable terminal result after a process crash.  Keep the
                // proof and the ledger disposition in this same transaction;
                // a crash after the terminal CAS can no longer strand a
                // pending invocation forever.
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        } else if input.preserve_pending_settlement {
            // Recovery of a daemon-owned execution can race the daemon's
            // replayable terminal notification. Keep calls without an
            // observed report pending so that late delivery can settle them;
            // never manufacture an unmetered result at this boundary.
            sqlx::query(
                "UPDATE usage_invocation
                 SET lifecycle = 'pending_settlement', updated_at = ?,
                     version = version + 1
                 WHERE execution_id = ? AND lifecycle = 'started'",
            )
            .bind(&terminal.updated_at)
            .bind(&terminal.execution_id)
            .execute(&mut *transaction)
            .await?;
            if input.mark_unreplayable_pending_unsettled {
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        } else {
            if input.mark_unreplayable_pending_unsettled {
                // A local runtime may have crossed the provider boundary but
                // lost its result before this recovery terminal CAS. Keep
                // that uncertainty explicit: move active attempts to
                // pending, then close them as `unsettled` in this same
                // transaction. This avoids manufacturing an unmetered result
                // and removes the crash window between terminalization and a
                // follow-up recovery sweep.
                sqlx::query(
                    "UPDATE usage_invocation
                     SET lifecycle = 'pending_settlement', updated_at = ?,
                         version = version + 1
                     WHERE execution_id = ? AND lifecycle = 'started'",
                )
                .bind(&terminal.updated_at)
                .bind(&terminal.execution_id)
                .execute(&mut *transaction)
                .await?;
                let pending_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle = 'pending_settlement'",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in pending_rows {
                    let id: String = row.try_get("id")?;
                    if settled_ids.contains(&id) {
                        continue;
                    }
                    let version: i64 = row.try_get("version")?;
                    self.mark_usage_invocation_unsettled_in_tx(
                        &mut transaction,
                        MarkUsageInvocationUnsettled {
                            id,
                            expected_version: version,
                            terminal_reason: "recovery_no_replayable_result".to_owned(),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            } else {
                // A provider may return no report at all. Still close every
                // started/pending invocation explicitly as unmetered so no
                // call remains in an ambiguous pending state after
                // terminalization.
                let active_rows = sqlx::query(
                    "SELECT id, version FROM usage_invocation
                     WHERE execution_id = ? AND lifecycle IN ('started', 'pending_settlement')",
                )
                .bind(&terminal.execution_id)
                .fetch_all(&mut *transaction)
                .await?;
                for row in active_rows {
                    let id: String = row.try_get("id")?;
                    if settled_ids.contains(&id) {
                        continue;
                    }
                    let version: i64 = row.try_get("version")?;
                    self.settle_usage_invocation_in_tx(
                        &mut transaction,
                        SettleUsageInvocation {
                            id,
                            expected_version: version,
                            telemetry_state: UsageTelemetryState::Unmetered,
                            terminal_reason: Some("missing_usage_report".to_owned()),
                            settled_at: terminal.updated_at.clone(),
                            updated_at: terminal.updated_at.clone(),
                        },
                    )
                    .await?;
                }
            }
        }

        let event = DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        if let (Some(terminal_report_id), Some(terminal_report_digest)) = (
            input.terminal_report_id.as_deref(),
            input.terminal_report_digest.as_deref(),
        ) {
            persist_terminal_receipt_in_tx(
                &mut transaction,
                &ExecutionTerminalReceipt {
                    terminal_report_id: terminal_report_id.to_owned(),
                    execution_id: terminal.execution_id.clone(),
                    payload_digest: terminal_report_digest.to_owned(),
                    event_id: event.id.clone(),
                    created_at: event.created_at.clone(),
                },
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(ExecutionTerminalOutcome::Committed {
            execution: updated,
            event: Box::new(event),
            workspace_lease_id,
            workspace_lease_status,
            replayed: false,
        })
    }

    async fn get_execution_terminal_receipt(
        &self,
        terminal_report_id: &str,
    ) -> Result<Option<ExecutionTerminalReceipt>> {
        sqlx::query(
            "SELECT terminal_report_id, execution_id, payload_digest, event_id, created_at
             FROM execution_terminal_receipt
             WHERE terminal_report_id = ?",
        )
        .bind(terminal_report_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_terminal_receipt)
        .transpose()
    }

    async fn list_expired_leases(&self, now: &str, limit: i64) -> Result<Vec<Execution>> {
        sqlx::query(
            "SELECT * FROM execution
             WHERE status = 'running'
               AND (
                    (lease_owner IS NOT NULL
                     AND (lease_expires_at IS NULL OR lease_expires_at <= ?))
                    OR (hard_deadline_at IS NOT NULL AND hard_deadline_at <= ?)
               )
             ORDER BY COALESCE(lease_expires_at, hard_deadline_at, created_at) ASC, id ASC
             LIMIT ?",
        )
        .bind(now)
        .bind(now)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_execution)
        .collect()
    }

    async fn list_stale_progress(
        &self,
        now: &str,
        stale_before: &str,
        limit: i64,
    ) -> Result<Vec<Execution>> {
        sqlx::query(
            "SELECT * FROM execution
             WHERE status = 'running'
               AND lease_owner IS NOT NULL
               AND lease_expires_at > ?
               AND hard_deadline_at > ?
               AND (
                    (last_progress_at IS NULL AND created_at < ?)
                    OR (last_progress_at IS NOT NULL AND last_progress_at < ?)
               )
             ORDER BY COALESCE(last_progress_at, created_at) ASC, id ASC
             LIMIT ?",
        )
        .bind(now)
        .bind(now)
        .bind(stale_before)
        .bind(stale_before)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_execution)
        .collect()
    }

    async fn list_running(&self) -> Result<Vec<Execution>> {
        let rows = sqlx::query(
            "SELECT * FROM execution
             WHERE status = 'running'
             ORDER BY created_at ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_execution).collect()
    }

    async fn list_running_for_daemon_not_in(
        &self,
        daemon_id: &str,
        created_before: &str,
        exclude_ids: &[String],
    ) -> Result<Vec<Execution>> {
        let rows = if exclude_ids.is_empty() {
            sqlx::query(
                "SELECT e.* FROM execution e
                 INNER JOIN agent_current a ON a.id = e.agent_id
                 WHERE e.status = 'running'
                   AND a.daemon_id = ?
                   AND e.created_at < ?
                 ORDER BY e.created_at ASC, e.id ASC",
            )
            .bind(daemon_id)
            .bind(created_before)
            .fetch_all(&self.pool)
            .await?
        } else {
            let placeholders = exclude_ids
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "SELECT e.* FROM execution e
                 INNER JOIN agent_current a ON a.id = e.agent_id
                 WHERE e.status = 'running'
                   AND a.daemon_id = ?
                   AND e.created_at < ?
                   AND e.id NOT IN ({placeholders})
                 ORDER BY e.created_at ASC, e.id ASC"
            );
            let mut query = sqlx::query(&query).bind(daemon_id).bind(created_before);
            for execution_id in exclude_ids {
                query = query.bind(execution_id);
            }
            query.fetch_all(&self.pool).await?
        };

        rows.into_iter().map(map_execution).collect()
    }

    async fn get_logs_path(&self, id: &str) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT logs_path FROM execution WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }
}

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
