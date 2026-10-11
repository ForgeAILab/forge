use super::*;

#[async_trait]
impl RuntimeRepo for SqliteDb {
    async fn create(&self, input: CreateRuntime) -> Result<Runtime> {
        let result = sqlx::query("INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, labels_json, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS(SELECT 1 FROM daemon WHERE id = ? AND removed_at IS NULL)")
            .bind(&input.id)
            .bind(&input.daemon_id)
            .bind(&input.kind)
            .bind(&input.workspace_root)
            .bind(input.status.to_string())
            .bind(&input.labels_json)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(&input.daemon_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        RuntimeRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Runtime>> {
        sqlx::query("SELECT * FROM runtime WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_runtime)
            .transpose()
    }

    async fn get_by_daemon_id(&self, daemon_id: &str) -> Result<Option<Runtime>> {
        sqlx::query(
            "SELECT * FROM runtime WHERE daemon_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(daemon_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_runtime)
        .transpose()
    }

    async fn upsert_by_daemon_kind(&self, input: CreateRuntime) -> Result<Runtime> {
        let result = sqlx::query("INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, labels_json, created_at, updated_at) SELECT ?, ?, ?, ?, ?, ?, ?, ? WHERE EXISTS(SELECT 1 FROM daemon WHERE id = ? AND removed_at IS NULL) ON CONFLICT(daemon_id, kind) DO UPDATE SET workspace_root = excluded.workspace_root, status = excluded.status, labels_json = excluded.labels_json, updated_at = excluded.updated_at")
            .bind(&input.id)
            .bind(&input.daemon_id)
            .bind(&input.kind)
            .bind(&input.workspace_root)
            .bind(input.status.to_string())
            .bind(&input.labels_json)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(&input.daemon_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }

        sqlx::query("SELECT * FROM runtime WHERE daemon_id = ? AND kind = ?")
            .bind(&input.daemon_id)
            .bind(&input.kind)
            .fetch_optional(&self.pool)
            .await?
            .map(map_runtime)
            .transpose()?
            .ok_or(DbError::NotFound)
    }

    async fn list(&self, query: RuntimeListQuery) -> Result<Page<Runtime>> {
        let offset = decode_offset(&query.page.cursor)?;
        let where_sql = if query.daemon_id.is_some() {
            " WHERE daemon_id = ? AND EXISTS(SELECT 1 FROM daemon WHERE id=runtime.daemon_id AND removed_at IS NULL)"
        } else {
            " WHERE EXISTS(SELECT 1 FROM daemon WHERE id=runtime.daemon_id AND removed_at IS NULL)"
        };
        let sql = format!(
            "SELECT * FROM runtime{} ORDER BY {} LIMIT ? OFFSET ?",
            where_sql,
            order_clause_without_priority(&query.page)
        );
        let mut q = sqlx::query(&sql);
        if let Some(daemon_id) = &query.daemon_id {
            q = q.bind(daemon_id);
        }
        let rows = q
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_runtime)
            .collect::<Result<Vec<_>>>()?;
        let total = if query.page.include_total {
            let count_sql = format!("SELECT COUNT(*) FROM runtime{}", where_sql);
            let mut q = sqlx::query_scalar::<_, i64>(&count_sql);
            if let Some(daemon_id) = &query.daemon_id {
                q = q.bind(daemon_id);
            }
            Some(q.fetch_one(&self.pool).await?)
        } else {
            None
        };
        page_from_items(items, &query.page, offset, total)
    }
}
