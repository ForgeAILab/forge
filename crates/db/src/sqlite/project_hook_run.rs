use super::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const PROJECT_HOOK_RUN_COLUMNS: &str = "id, project_id, rule_id, trigger_type, dedupe_key, status, source_task_id, source_execution_id, automation_task_id, execution_id, agent_id, reason, created_at, updated_at, completed_at";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectHookRunCursor {
    created_at: String,
    id: String,
}

#[async_trait]
impl ProjectHookRunRepo for SqliteDb {
    async fn try_claim(&self, input: CreateProjectHookRun) -> Result<Option<ProjectHookRun>> {
        let sql = format!(
            "INSERT INTO project_hook_run ({PROJECT_HOOK_RUN_COLUMNS}) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(project_id, rule_id, dedupe_key) DO NOTHING \
             RETURNING {PROJECT_HOOK_RUN_COLUMNS}"
        );
        sqlx::query(&sql)
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.rule_id)
            .bind(&input.trigger_type)
            .bind(&input.dedupe_key)
            .bind(input.status.to_string())
            .bind(input.source_task_id.as_deref())
            .bind(input.source_execution_id.as_deref())
            .bind(input.automation_task_id.as_deref())
            .bind(input.execution_id.as_deref())
            .bind(input.agent_id.as_deref())
            .bind(input.reason.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(input.completed_at.as_deref())
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_hook_run)
            .transpose()
    }

    async fn try_claim_or_skip_at_limit(
        &self,
        input: CreateProjectHookRun,
        max_active_runs: i64,
        skip_reason: &str,
    ) -> Result<Option<ProjectHookRun>> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let sql = format!(
            "INSERT INTO project_hook_run ({PROJECT_HOOK_RUN_COLUMNS}) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(project_id, rule_id, dedupe_key) DO NOTHING \
             RETURNING {PROJECT_HOOK_RUN_COLUMNS}"
        );
        let Some(run) = sqlx::query(&sql)
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.rule_id)
            .bind(&input.trigger_type)
            .bind(&input.dedupe_key)
            .bind(input.status.to_string())
            .bind(input.source_task_id.as_deref())
            .bind(input.source_execution_id.as_deref())
            .bind(input.automation_task_id.as_deref())
            .bind(input.execution_id.as_deref())
            .bind(input.agent_id.as_deref())
            .bind(input.reason.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(input.completed_at.as_deref())
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_project_hook_run)
            .transpose()?
        else {
            transaction.commit().await?;
            return Ok(None);
        };

        let active_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_hook_run \
             WHERE project_id = ? \
               AND rule_id = ? \
               AND status IN ('queued', 'running', 'dispatched') \
               AND completed_at IS NULL",
        )
        .bind(&input.project_id)
        .bind(&input.rule_id)
        .fetch_one(&mut *transaction)
        .await?;

        let run = if active_count > max_active_runs {
            let sql = format!(
                "UPDATE project_hook_run \
                 SET status = ?, reason = ?, updated_at = ?, completed_at = ? \
                 WHERE id = ? RETURNING {PROJECT_HOOK_RUN_COLUMNS}"
            );
            sqlx::query(&sql)
                .bind(ProjectHookRunStatus::Skipped.to_string())
                .bind(skip_reason)
                .bind(&input.updated_at)
                .bind(&input.updated_at)
                .bind(&run.id)
                .fetch_optional(&mut *transaction)
                .await?
                .map(map_project_hook_run)
                .transpose()?
                .ok_or(DbError::NotFound)?
        } else {
            run
        };

        transaction.commit().await?;
        Ok(Some(run))
    }

    async fn update_status(&self, input: UpdateProjectHookRun) -> Result<ProjectHookRun> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new("UPDATE project_hook_run SET status = ");
        query.push_bind(input.status.to_string());
        if let Some(automation_task_id) = input.automation_task_id {
            query
                .push(", automation_task_id = ")
                .push_bind(automation_task_id);
        }
        if let Some(execution_id) = input.execution_id {
            query.push(", execution_id = ").push_bind(execution_id);
        }
        if let Some(agent_id) = input.agent_id {
            query.push(", agent_id = ").push_bind(agent_id);
        }
        if let Some(reason) = input.reason {
            query.push(", reason = ").push_bind(reason);
        }
        if let Some(completed_at) = input.completed_at {
            query.push(", completed_at = ").push_bind(completed_at);
        }
        query
            .push(", updated_at = ")
            .push_bind(&input.updated_at)
            .push(" WHERE id = ")
            .push_bind(&input.id)
            .push(" RETURNING ")
            .push(PROJECT_HOOK_RUN_COLUMNS);

        query
            .build()
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_hook_run)
            .transpose()?
            .ok_or(DbError::NotFound)
    }

    async fn list_recent_for_project(
        &self,
        project_id: &str,
        limit: i64,
    ) -> Result<Vec<ProjectHookRun>> {
        let sql = format!(
            "SELECT {PROJECT_HOOK_RUN_COLUMNS} FROM project_hook_run \
             WHERE project_id = ? ORDER BY created_at DESC, id DESC LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(project_id)
            .bind(limit.clamp(1, 500))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_project_hook_run).collect()
    }

    async fn list_for_project(
        &self,
        project_id: &str,
        page: PageRequest,
    ) -> Result<Page<ProjectHookRun>> {
        let cursor = decode_project_hook_run_cursor(&page.cursor)?;
        let limit = limit(&page);
        let rows = if let Some(cursor) = cursor {
            let sql = format!(
                "SELECT {PROJECT_HOOK_RUN_COLUMNS} FROM project_hook_run \
                 WHERE project_id = ? \
                   AND (created_at < ? OR (created_at = ? AND id < ?)) \
                 ORDER BY created_at DESC, id DESC LIMIT ?"
            );
            sqlx::query(&sql)
                .bind(project_id)
                .bind(&cursor.created_at)
                .bind(&cursor.created_at)
                .bind(&cursor.id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await?
        } else {
            let sql = format!(
                "SELECT {PROJECT_HOOK_RUN_COLUMNS} FROM project_hook_run \
                 WHERE project_id = ? ORDER BY created_at DESC, id DESC LIMIT ?"
            );
            sqlx::query(&sql)
                .bind(project_id)
                .bind(limit + 1)
                .fetch_all(&self.pool)
                .await?
        };
        let mut items = rows
            .into_iter()
            .map(map_project_hook_run)
            .collect::<Result<Vec<_>>>()?;
        let has_next = items.len() > limit as usize;
        if has_next {
            items.truncate(limit as usize);
        }
        let next_cursor = if has_next {
            items
                .last()
                .map(|item| encode_project_hook_run_cursor(&item.created_at, &item.id))
                .transpose()?
        } else {
            None
        };
        Ok(Page {
            items,
            next_cursor,
            total_count: None,
        })
    }

    async fn count_active_for_rule(&self, project_id: &str, rule_id: &str) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_hook_run \
             WHERE project_id = ? \
               AND rule_id = ? \
               AND status IN ('queued', 'running', 'dispatched') \
               AND completed_at IS NULL",
        )
        .bind(project_id)
        .bind(rule_id)
        .fetch_one(&self.pool)
        .await?)
    }
}

fn map_project_hook_run(row: SqliteRow) -> Result<ProjectHookRun> {
    Ok(ProjectHookRun {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        rule_id: row.try_get("rule_id")?,
        trigger_type: row.try_get("trigger_type")?,
        dedupe_key: row.try_get("dedupe_key")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        source_task_id: row.try_get("source_task_id")?,
        source_execution_id: row.try_get("source_execution_id")?,
        automation_task_id: row.try_get("automation_task_id")?,
        execution_id: row.try_get("execution_id")?,
        agent_id: row.try_get("agent_id")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn decode_project_hook_run_cursor(cursor: &Option<String>) -> Result<Option<ProjectHookRunCursor>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let bytes =
        base64::Engine::decode(&URL_SAFE_NO_PAD, cursor).map_err(|_| DbError::InvalidCursor)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| DbError::InvalidCursor)
}

fn encode_project_hook_run_cursor(created_at: &str, id: &str) -> Result<String> {
    let bytes = serde_json::to_vec(&ProjectHookRunCursor {
        created_at: created_at.to_owned(),
        id: id.to_owned(),
    })
    .map_err(|_| DbError::InvalidCursor)?;
    Ok(base64::Engine::encode(&URL_SAFE_NO_PAD, bytes))
}
