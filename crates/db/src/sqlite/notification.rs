use super::*;

#[async_trait]
impl NotificationRepo for SqliteDb {
    async fn create(&self, input: CreateNotification) -> Result<Notification> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let notification = self
            .create_notification_in_tx(&mut transaction, &input)
            .await?;
        transaction.commit().await?;
        Ok(notification)
    }

    async fn list(&self, query: NotificationListQuery) -> Result<Page<Notification>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut where_parts = Vec::new();
        if query.project_id.is_some() {
            where_parts.push("project_id = ?");
        }
        if query.read.is_some() {
            where_parts.push("read = ?");
        }
        let where_sql = if where_parts.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_parts.join(" AND "))
        };

        let sql = format!(
            "SELECT * FROM notification {where_sql} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        );
        let mut q = sqlx::query(&sql);
        if let Some(project_id) = &query.project_id {
            q = q.bind(project_id);
        }
        if let Some(read) = query.read {
            q = q.bind(if read { 1 } else { 0 });
        }
        let rows = q
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_notification)
            .collect::<Result<Vec<_>>>()?;

        let total = if query.page.include_total {
            let count_sql = format!("SELECT COUNT(*) FROM notification {where_sql}");
            let mut cq = sqlx::query_scalar::<_, i64>(&count_sql);
            if let Some(project_id) = &query.project_id {
                cq = cq.bind(project_id);
            }
            if let Some(read) = query.read {
                cq = cq.bind(if read { 1 } else { 0 });
            }
            Some(cq.fetch_one(&self.pool).await?)
        } else {
            None
        };

        page_from_items(items, &query.page, offset, total)
    }

    async fn unread_count(&self, project_id: Option<&str>) -> Result<i64> {
        match project_id {
            Some(project_id) => sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM notification WHERE read = 0 AND project_id = ?",
            )
            .bind(project_id)
            .fetch_one(&self.pool)
            .await
            .map_err(Into::into),
            None => {
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM notification WHERE read = 0")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(Into::into)
            }
        }
    }

    async fn mark_read(&self, id: &str) -> Result<Notification> {
        let result = sqlx::query("UPDATE notification SET read = 1 WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }

        sqlx::query("SELECT * FROM notification WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_notification)
            .transpose()?
            .ok_or(DbError::NotFound)
    }

    async fn mark_all_read(&self, project_id: Option<&str>) -> Result<u64> {
        let result = match project_id {
            Some(project_id) => {
                sqlx::query("UPDATE notification SET read = 1 WHERE read = 0 AND project_id = ?")
                    .bind(project_id)
                    .execute(&self.pool)
                    .await?
            }
            None => {
                sqlx::query("UPDATE notification SET read = 1 WHERE read = 0")
                    .execute(&self.pool)
                    .await?
            }
        };
        Ok(result.rows_affected())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM notification WHERE id = ?")
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
    pub async fn create_notification_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateNotification,
    ) -> Result<Notification> {
        sqlx::query(
            "INSERT INTO notification (id, project_id, task_id, event_type, title, body, read, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(input.task_id.as_deref())
        .bind(&input.event_type)
        .bind(&input.title)
        .bind(input.body.as_deref())
        .bind(if input.read { 1 } else { 0 })
        .bind(&input.created_at)
        .execute(&mut **transaction)
        .await?;

        sqlx::query("SELECT * FROM notification WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut **transaction)
            .await?
            .map(map_notification)
            .transpose()?
            .ok_or(DbError::NotFound)
    }
}
