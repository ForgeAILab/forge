use super::*;
use crate::AgentExecutionStats;

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
                COUNT(*) AS total_runs, \
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

        let total_runs: i64 = run_row.try_get("total_runs")?;
        let completed_runs: i64 = run_row.try_get("completed_runs")?;
        let avg_duration_ms = run_row
            .try_get::<Option<f64>, _>("avg_duration_ms")?
            .map(|duration| duration.round() as i64);
        let success_rate = if total_runs > 0 {
            Some(completed_runs as f64 / total_runs as f64)
        } else {
            None
        };

        let usage_row = sqlx::query(
            "SELECT \
                COALESCE(SUM(eu.input_tokens), 0) AS total_input_tokens, \
                COALESCE(SUM(eu.output_tokens), 0) AS total_output_tokens, \
                COALESCE(SUM(eu.cache_read_tokens), 0) AS total_cache_read_tokens, \
                COALESCE(SUM(eu.cache_write_tokens), 0) AS total_cache_write_tokens, \
                SUM(eu.cost_usd) AS total_cost_usd \
             FROM execution_usage eu \
             JOIN execution e ON eu.execution_id = e.id \
             WHERE e.agent_id = ?",
        )
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?;

        let total_input_tokens: i64 = usage_row.try_get("total_input_tokens")?;
        let total_output_tokens: i64 = usage_row.try_get("total_output_tokens")?;
        let total_cache_read_tokens: i64 = usage_row.try_get("total_cache_read_tokens")?;
        let total_cache_write_tokens: i64 = usage_row.try_get("total_cache_write_tokens")?;
        let total_cost_usd: Option<f64> = usage_row.try_get("total_cost_usd")?;

        Ok(AgentExecutionStats {
            total_runs,
            avg_duration_ms,
            success_rate,
            total_input_tokens,
            total_output_tokens,
            total_cache_read_tokens,
            total_cache_write_tokens,
            total_cost_usd,
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
        })
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

fn progress_timestamp_is_not_newer(candidate: &str, current: &str) -> bool {
    match (
        chrono::DateTime::parse_from_rfc3339(candidate),
        chrono::DateTime::parse_from_rfc3339(current),
    ) {
        (Ok(candidate), Ok(current)) => candidate <= current,
        _ => candidate <= current,
    }
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
