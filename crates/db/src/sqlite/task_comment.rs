use super::*;

#[async_trait]
impl TaskCommentRepo for SqliteDb {
    async fn create_comment(&self, input: CreateTaskComment) -> Result<TaskComment> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let comment = self
            .create_task_comment_in_tx(&mut transaction, &input)
            .await?;
        transaction.commit().await?;
        Ok(comment)
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

impl SqliteDb {
    pub async fn create_task_comment_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateTaskComment,
    ) -> Result<TaskComment> {
        // A replayed worklog append must return the original entry rather than
        // appending a second one into the reviewer's context.
        if let Some(key) = input.idempotency_key.as_deref() {
            if let Some(existing) =
                sqlx::query("SELECT * FROM task_comment WHERE task_id = ? AND idempotency_key = ?")
                    .bind(&input.task_id)
                    .bind(key)
                    .fetch_optional(&mut **transaction)
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
            .execute(&mut **transaction)
            .await?;
        sqlx::query("SELECT * FROM task_comment WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(Into::into)
            .and_then(map_task_comment)
    }

    /// Put the wait of a lead's question on its root, inside the transaction
    /// that writes the question and its Attention item: all three exist or
    /// none does. `Ok(false)`: the root already waits for a person on another
    /// question (or another human boundary) and nothing was written; the
    /// caller drops the transaction. `Ok(true)`: the root waits on
    /// `comment_id` (now, or already: a retry of the same question).
    pub async fn open_lead_input_wait_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        root_id: &str,
        comment_id: &str,
        execution_id: &str,
        now: &str,
    ) -> Result<bool> {
        let raw: Option<String> = sqlx::query_scalar(
            "SELECT metadata_json FROM task WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(root_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let mut metadata = crate::TaskMetadata::parse(raw.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        if metadata.extra.get("awaiting_human") == Some(&serde_json::Value::Bool(true)) {
            return Ok(metadata
                .extra
                .get("awaiting_human_reason")
                .and_then(serde_json::Value::as_str)
                == Some(crate::LEAD_INPUT_WAIT)
                && metadata
                    .extra
                    .get("awaiting_human_marker_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(comment_id));
        }
        for (key, value) in [
            ("awaiting_human", serde_json::Value::Bool(true)),
            ("awaiting_human_reason", crate::LEAD_INPUT_WAIT.into()),
            ("awaiting_human_marker_id", comment_id.into()),
            ("lead_input_execution_id", execution_id.into()),
        ] {
            metadata.extra.insert(key.to_owned(), value);
        }
        let metadata_json = metadata.to_json();
        let condition = crate::task_condition::metadata_condition(
            transaction,
            root_id,
            metadata_json.as_deref(),
        )
        .await?;
        sqlx::query(
            "UPDATE task
             SET metadata_json = ?, condition_json = CASE WHEN ? THEN ? ELSE condition_json END, updated_at = ?
             WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(metadata_json.as_deref())
        .bind(condition.is_some())
        .bind(condition)
        .bind(now)
        .bind(root_id)
        .execute(&mut **transaction)
        .await?;
        Ok(true)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct NativeProjectWorklogRow {
    row: sqlx::sqlite::SqliteRow,
}
impl NativeProjectWorklogRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn task_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_id")?)
    }
    pub fn title(&self) -> crate::Result<String> {
        Ok(self.row.try_get("title")?)
    }
    pub fn worklog_kind(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("worklog_kind")?)
    }
    pub fn role_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("role")?)
    }
    pub fn execution_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("execution_id")?)
    }
    pub fn content(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn native_ingested_task_comment(
        &self,
        task_id: &str,
        idempotency_key: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task_comment WHERE task_id = ? AND idempotency_key = ? LIMIT 1",
        )
        .bind(task_id)
        .bind(idempotency_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn native_task_worklog_exists(
        &self,
        task_id: &str,
        idempotency_key: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM task_comment
                 WHERE task_id = ? AND idempotency_key = ?
             )",
        )
        .bind(task_id)
        .bind(idempotency_key)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn native_project_worklog(
        &self,
        project_id: &str,
        task_filter: Option<&str>,
        task_id: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<NativeProjectWorklogRow>> {
        let rows = sqlx::query(
            "SELECT c.id, c.task_id, c.worklog_kind, c.role, c.execution_id,
                    c.content, c.created_at, t.title
             FROM task_comment c
             JOIN task t ON t.id = c.task_id
             WHERE t.project_id = ? AND t.deleted_at IS NULL
               AND c.author_type = 'agent'
               AND (? IS NULL OR c.task_id = ?)
             ORDER BY c.created_at DESC, c.id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(task_filter)
        .bind(task_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| NativeProjectWorklogRow { row })
            .collect())
    }
}
