use super::*;

/// Explicit prerequisites and the hierarchy's completion/order waits form
/// one graph. The writer transaction also fences a concurrent reorder.
pub(super) async fn dependency_cycle_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    task_id: &str,
    candidate: Option<&str>,
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "WITH RECURSIVE live AS (
            SELECT id,parent_task_id,subtask_order,created_at FROM task
            WHERE deleted_at IS NULL AND json_extract(condition_json,'$.kind') != 'settled'
         ), ordered AS (
            SELECT *,ROW_NUMBER() OVER(PARTITION BY parent_task_id ORDER BY subtask_order,created_at,id) AS position
            FROM live WHERE parent_task_id IS NOT NULL
         ), edges(from_id,from_phase,to_id,to_phase) AS (
            SELECT d.task_id,'start',d.depends_on_id,'done' FROM task_dependency d
            JOIN live t ON t.id=d.task_id JOIN live p ON p.id=d.depends_on_id
            UNION SELECT id,'done',id,'start' FROM live
            UNION SELECT child.id,'start',parent.id,'start' FROM live parent JOIN live child ON child.parent_task_id=parent.id
            UNION SELECT parent.id,'done',child.id,'done' FROM live parent JOIN live child ON child.parent_task_id=parent.id
            UNION SELECT later.id,'start',earlier.id,'done' FROM ordered later JOIN ordered earlier
              ON later.parent_task_id=earlier.parent_task_id AND later.position=earlier.position+1
         ), reachable(id,phase) AS (
            SELECT to_id,to_phase FROM edges WHERE from_id=?1 AND from_phase='start'
            UNION SELECT id,'done' FROM live WHERE id=?2
            UNION SELECT e.to_id,e.to_phase FROM edges e JOIN reachable r ON e.from_id=r.id AND e.from_phase=r.phase
         ) SELECT EXISTS(SELECT 1 FROM reachable WHERE id=?1 AND phase='start')"
    ).bind(task_id).bind(candidate).fetch_one(&mut **tx).await?)
}

#[async_trait]
impl TaskDependencyRepo for SqliteDb {
    async fn add_dependency(&self, task_id: &str, depends_on_id: &str, now: &str) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        self.add_dependency_in_tx(&mut transaction, task_id, depends_on_id, now)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn add_dependency_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        depends_on_id: &str,
        now: &str,
    ) -> Result<()> {
        if task_id == depends_on_id {
            return Err(DbError::CycleDetected);
        }

        if dependency_cycle_in_tx(transaction, task_id, Some(depends_on_id)).await? {
            return Err(DbError::CycleDetected);
        }

        sqlx::query(
            "INSERT INTO task_dependency (task_id, depends_on_id, created_at) VALUES (?, ?, ?)",
        )
        .bind(task_id)
        .bind(depends_on_id)
        .bind(now)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    async fn remove_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM task_dependency WHERE task_id = ? AND depends_on_id = ?")
            .bind(task_id)
            .bind(depends_on_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_dependencies(&self, task_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn list_dependents(&self, depends_on_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT task_id FROM task_dependency WHERE depends_on_id = ?",
        )
        .bind(depends_on_id)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn list_dependency_tasks(&self, task_id: &str) -> Result<Vec<Task>> {
        let columns = task_columns_with_alias("t");
        let sql = format!(
            "SELECT {columns}
             FROM task_dependency AS td
             JOIN task AS t ON t.id = td.depends_on_id
             WHERE td.task_id = ? AND t.deleted_at IS NULL
             ORDER BY td.created_at ASC, t.id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(task_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn list_dependent_tasks(&self, depends_on_id: &str) -> Result<Vec<Task>> {
        let columns = task_columns_with_alias("t");
        let sql = format!(
            "SELECT {columns}
             FROM task_dependency AS td
             JOIN task AS t ON t.id = td.task_id
             WHERE td.depends_on_id = ? AND t.deleted_at IS NULL
             ORDER BY td.created_at ASC, t.id ASC"
        );
        let rows = sqlx::query(&sql)
            .bind(depends_on_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(map_task).collect()
    }

    async fn unsatisfied_dependencies(&self, task_id: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ? AND depends_on_id NOT IN (SELECT id FROM task WHERE status = 'done')",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?)
    }
}

fn task_columns_with_alias(alias: &str) -> String {
    TASK_COLUMNS
        .split(", ")
        .map(|column| format!("{alias}.{column}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeTaskDependencyRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeTaskDependencyRow {
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn depends_on_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("depends_on_id")?)
    }
}

impl SqliteDb {
    pub async fn native_work_task_dependencies(
        &self,
        ids: &[String],
    ) -> crate::Result<Vec<NativeTaskDependencyRow>> {
        let placeholders = vec!["?"; ids.len()].join(", ");
        let sql = format!(
            "SELECT task_id, depends_on_id FROM task_dependency
                 WHERE task_id IN ({placeholders}) ORDER BY task_id, depends_on_id"
        );
        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }

        let rows = query.fetch_all(self.pool()).await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeTaskDependencyRow { row })
            .collect())
    }
}
