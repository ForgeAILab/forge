use super::*;

#[async_trait]
impl TaskMediaRepo for SqliteDb {
    async fn create_media(&self, input: CreateTaskMedia) -> Result<TaskMedia> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO task_media (id, task_id, display_filename, content_type, byte_size, storage_key, author_type, author_id, author_name, created_at, deleted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.display_filename)
        .bind(&input.content_type)
        .bind(input.byte_size)
        .bind(&input.storage_key)
        .bind(input.author_type.to_string())
        .bind(input.author_id.as_deref())
        .bind(&input.author_name)
        .bind(&input.created_at)
        .execute(&mut *transaction)
        .await?;
        super::logic_triggers::project_task_media_in_tx(&mut transaction, &input).await?;
        let media = sqlx::query("SELECT * FROM task_media WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_task_media)?;
        transaction.commit().await?;
        Ok(media)
    }

    async fn list_media(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskMedia>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM task_media WHERE task_id = ? AND deleted_at IS NULL ORDER BY {} LIMIT ? OFFSET ?",
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
            .map(map_task_media)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM task_media WHERE task_id = ? AND deleted_at IS NULL",
                )
                .bind(task_id)
                .fetch_one(&self.pool)
                .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn list_active_media_for_task(&self, task_id: &str) -> Result<Vec<TaskMedia>> {
        sqlx::query(
            "SELECT * FROM task_media WHERE task_id = ? AND deleted_at IS NULL ORDER BY created_at ASC, id ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_task_media)
        .collect()
    }

    async fn get_media_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<TaskMedia>> {
        let sql = if include_deleted {
            "SELECT * FROM task_media WHERE id = ?"
        } else {
            "SELECT * FROM task_media WHERE id = ? AND deleted_at IS NULL"
        };

        sqlx::query(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_task_media)
            .transpose()
    }

    async fn soft_delete_media(&self, id: &str, deleted_at: &str) -> Result<TaskMedia> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let asset_id: Option<String> = sqlx::query_scalar(
            "SELECT asset_id FROM task_media WHERE id = ? AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .flatten();
        let result =
            sqlx::query("UPDATE task_media SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL")
                .bind(deleted_at)
                .bind(id)
                .execute(&mut *transaction)
                .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }

        sqlx::query(
            "UPDATE project_media_attachment
             SET deleted_at = ?, availability = 'purged', updated_at = ?,
                 version = version + 1
             WHERE task_media_id = ? AND deleted_at IS NULL",
        )
        .bind(deleted_at)
        .bind(deleted_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if let Some(asset_id) = asset_id {
            super::shared_media::reconcile_legacy_task_media_delete_in_tx(
                &mut transaction,
                &asset_id,
                deleted_at,
            )
            .await?;
        }
        let media = sqlx::query("SELECT * FROM task_media WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_task_media)?;
        transaction.commit().await?;
        Ok(media)
    }
}
