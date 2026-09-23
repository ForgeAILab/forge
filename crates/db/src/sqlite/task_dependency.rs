use super::*;

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

        let cycle_count = sqlx::query_scalar::<_, i64>(
            "WITH RECURSIVE reachable(id) AS (
                SELECT depends_on_id FROM task_dependency WHERE task_id = ?
                UNION
                SELECT td.depends_on_id FROM task_dependency td JOIN reachable r ON td.task_id = r.id
            )
            SELECT COUNT(*) FROM reachable WHERE id = ?",
        )
        .bind(depends_on_id)
        .bind(task_id)
        .fetch_one(&mut **transaction)
        .await?;
        if cycle_count > 0 {
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
