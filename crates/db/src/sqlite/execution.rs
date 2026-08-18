use super::*;
use crate::AgentExecutionStats;

#[async_trait]
impl ExecutionRepo for SqliteDb {
    async fn create(&self, input: CreateExecution) -> Result<Execution> {
        let mut transaction = self.pool.begin().await?;
        let execution = Self::create_execution_in_tx(&mut transaction, &input).await?;
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
        let execution = ExecutionRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if let Some(status) = input.status.as_ref() {
            if !execution_transition_allowed(&execution.status, status) {
                return Err(DbError::InvalidTransition);
            }
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
        if let Some(status) = input.status {
            push_assignment!("status", status.to_string());
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
        query.push("updated_at = ").push_bind(input.updated_at);
        query.push(" WHERE id = ").push_bind(&input.id);
        query.build().execute(&self.pool).await?;
        ExecutionRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn update_last_activity_at(&self, id: &str, timestamp: &str) -> Result<()> {
        sqlx::query("UPDATE execution SET last_activity_at = ?, updated_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(timestamp)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_stalled_running(&self, stale_before: &str) -> Result<Vec<Execution>> {
        let rows = sqlx::query(
            "SELECT * FROM execution
             WHERE status = 'running'
               AND (
                 (last_activity_at IS NULL AND created_at < ?)
                 OR (last_activity_at IS NOT NULL AND last_activity_at < ?)
               )
             ORDER BY COALESCE(last_activity_at, created_at) ASC, id ASC",
        )
        .bind(stale_before)
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_execution).collect()
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
