use super::*;

#[async_trait]
impl TaskCommentRepo for SqliteDb {
    async fn create_comment(&self, input: CreateTaskComment) -> Result<TaskComment> {
        // A replayed worklog append must return the original entry rather than
        // appending a second one into the reviewer's context.
        if let Some(key) = input.idempotency_key.as_deref() {
            if let Some(existing) =
                sqlx::query("SELECT * FROM task_comment WHERE task_id = ? AND idempotency_key = ?")
                    .bind(&input.task_id)
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await?
                    .map(map_task_comment)
                    .transpose()?
            {
                return Ok(existing);
            }
        }
        sqlx::query("INSERT INTO task_comment (id, task_id, author_type, author_id, author_name, content, execution_id, role, worklog_kind, idempotency_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.task_id)
            .bind(input.author_type.to_string())
            .bind(input.author_id.as_deref())
            .bind(&input.author_name)
            .bind(&input.content)
            .bind(input.execution_id.as_deref())
            .bind(input.role.as_deref())
            .bind(input.worklog_kind.as_deref())
            .bind(input.idempotency_key.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&self.pool)
            .await?;
        Self::get_comment_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_comments(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskComment>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM task_comment WHERE task_id = ? ORDER BY {} LIMIT ? OFFSET ?",
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
            .map(map_task_comment)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_comment WHERE task_id = ?")
                    .bind(task_id)
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn get_comment_by_id(&self, id: &str) -> Result<Option<TaskComment>> {
        sqlx::query("SELECT * FROM task_comment WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_task_comment)
            .transpose()
    }

    async fn delete_comment(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM task_comment WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }
}
