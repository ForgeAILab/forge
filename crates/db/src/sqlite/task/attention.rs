//! Task attention projections.

use super::*;

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionTaskContextRow {
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub version: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_task_context(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<AttentionTaskContextRow>> {
        let rows = sqlx::query("SELECT title, status, version FROM task WHERE id = ?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?;
        rows.map(|row| {
            Ok(AttentionTaskContextRow {
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                version: row.try_get("version").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn attention_event_task_project_id(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_task_assignee_identity(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT assignee_id FROM task
                     WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
        )
        .bind(task_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_identity_assignment_count(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_agent_assignment_count(&self, task_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
        )
        .bind(task_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_task_identity_access_count(
        &self,
        task_id: &str,
        identity_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
                 WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
        )
        .bind(task_id)
        .bind(identity_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn attention_scope_task_project_id(
        &self,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(task_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn attention_agent_focus_task_id(
        &self,
        assignee_id: &str,
        commitment_owner_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT task_id
             FROM (
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 WHERE t.assignee_id = ?
                   AND t.status = 'in_progress'
                   AND t.deleted_at IS NULL
                 UNION
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 JOIN agent_commitment c ON c.originating_task_id = t.id
                 WHERE c.owner_identity_id = ?
                   AND c.status IN ('proposed', 'open', 'accepted', 'in_progress', 'blocked')
                   AND t.deleted_at IS NULL
             )
             ORDER BY updated_at DESC, task_id DESC
             LIMIT 1",
        )
        .bind(assignee_id)
        .bind(commitment_owner_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub(crate) fn attention_project_visibility_predicate(
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => ("t.project_id = ?".to_owned(), vec![project_id.to_owned()]),
            None => (
                "(p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_member pm
                    WHERE pm.project_id = p.id AND pm.user_id = ?
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleWorkItemsRow {
    pub id: crate::Result<String>,
    pub project_id: crate::Result<String>,
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub priority: crate::Result<i64>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    /// Read only visible workflow definitions before deriving review-state filters.
    pub async fn attention_visible_project_workflows(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<Vec<(String, String)>> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        let predicate = predicate.replace("t.project_id", "p.id");
        let sql = format!("SELECT p.id, p.workflow_definition FROM project p WHERE {predicate}");
        let mut query = sqlx::query_as(&sql);
        for value in values {
            query = query.bind(value);
        }
        Ok(query.fetch_all(self.pool()).await?)
    }

    pub async fn attention_visible_work_items(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        statuses: &[&str],
        review_states: &[(String, String)],
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleWorkItemsRow>> {
        let status_placeholders = vec!["?"; statuses.len()].join(", ");
        let review_predicate =
            vec!["(t.project_id = ? AND t.status = ?)"; review_states.len()].join(" OR ");
        let review_predicate = if review_predicate.is_empty() {
            "0"
        } else {
            &review_predicate
        };
        let (project_predicate, project_values) =
            Self::attention_project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.priority, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND (t.status IN ({status_placeholders}) OR {review_predicate})
               AND {project_predicate}
             ORDER BY t.priority DESC, t.updated_at ASC, t.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for status in statuses {
            query = query.bind(status);
        }
        for (project, state) in review_states {
            query = query.bind(project).bind(state);
        }
        for value in project_values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleWorkItemsRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                priority: row.try_get("priority").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleRecentOutcomesRow {
    pub id: crate::Result<String>,
    pub project_id: crate::Result<String>,
    pub title: crate::Result<String>,
    pub status: crate::Result<String>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_visible_recent_outcomes(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<AttentionVisibleRecentOutcomesRow>> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('done', 'cancelled', 'blocked')
               AND {predicate}
             ORDER BY t.updated_at DESC, t.id DESC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| AttentionVisibleRecentOutcomesRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                project_id: row.try_get("project_id").map_err(crate::DbError::from),
                title: row.try_get("title").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
            .collect())
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionVisibleQueuedTaskCountRow {
    pub count: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn attention_visible_queued_task_count(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> crate::Result<AttentionVisibleQueuedTaskCountRow> {
        let (predicate, values) = Self::attention_project_visibility_predicate(user_id, project_id);
        let queued_sql = format!(
            "SELECT COUNT(*) AS count FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('todo', 'backlog') AND {predicate}"
        );
        let mut query = sqlx::query(&queued_sql);
        for value in &values {
            query = query.bind(value);
        }
        let queued = query.fetch_one(self.pool()).await?;
        Ok(AttentionVisibleQueuedTaskCountRow {
            count: queued.try_get("count").map_err(crate::DbError::from),
        })
    }
}
