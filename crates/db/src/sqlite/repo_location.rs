use super::{
    check_error, decode_offset, limit, order_clause_without_priority, page_from_items, parse_enum,
    workspace_placement::map_workspace_placement, SqliteDb,
};
use crate::{
    CreateRepoLocation, DbError, Page, PageRequest, RepoLocation, RepoLocationRepo, Result,
    UpdateRepoLocation, WorkspacePlacement,
};
use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row, Sqlite};

#[async_trait]
impl RepoLocationRepo for SqliteDb {
    async fn create(&self, input: CreateRepoLocation) -> Result<RepoLocation> {
        let row = sqlx::query(
            "INSERT INTO repo_location (
                id, repo_id, owner_kind, daemon_id, runtime_id, path, kind,
                is_default, status, last_verified_at, last_error, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING *",
        )
        .bind(&input.id)
        .bind(&input.repo_id)
        .bind(input.owner_kind.to_string())
        .bind(input.daemon_id.as_deref())
        .bind(input.runtime_id.as_deref())
        .bind(&input.path)
        .bind(input.kind.to_string())
        .bind(input.is_default)
        .bind(input.status.to_string())
        .bind(input.last_verified_at.as_deref())
        .bind(input.last_error.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .fetch_one(&self.pool)
        .await
        .map_err(check_error)?;
        map_repo_location(row)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<RepoLocation>> {
        sqlx::query("SELECT * FROM repo_location WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_repo_location)
            .transpose()
    }

    async fn list_by_repo(&self, repo_id: &str, page: PageRequest) -> Result<Page<RepoLocation>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM repo_location WHERE repo_id = ? ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(repo_id)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_repo_location)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM repo_location WHERE repo_id = ?",
                )
                .bind(repo_id)
                .fetch_one(&self.pool)
                .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn update(&self, input: UpdateRepoLocation) -> Result<RepoLocation> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "UPDATE repo_location SET version = version + 1, updated_at = ",
        );
        query.push_bind(&input.updated_at);
        if let Some(path) = input.path {
            query.push(", path = ").push_bind(path);
        }
        if let Some(kind) = input.kind {
            query.push(", kind = ").push_bind(kind.to_string());
        }
        if let Some(is_default) = input.is_default {
            query.push(", is_default = ").push_bind(is_default);
        }
        if let Some(status) = input.status {
            query.push(", status = ").push_bind(status.to_string());
        }
        if let Some(last_verified_at) = input.last_verified_at {
            query
                .push(", last_verified_at = ")
                .push_bind(last_verified_at);
        }
        if let Some(last_error) = input.last_error {
            query.push(", last_error = ").push_bind(last_error);
        }
        query
            .push(" WHERE id = ")
            .push_bind(&input.id)
            .push(" AND version = ")
            .push_bind(input.expected_version)
            .push(" RETURNING *");
        let row = query
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(check_error)?
            .ok_or(DbError::VersionConflict)?;
        map_repo_location(row)
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let in_use: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workspace_placement
                           WHERE repo_location_id = ? AND state <> 'cleaned')",
        )
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
        if in_use {
            return Err(DbError::VersionConflict);
        }
        // Passive integration queues never block this deletion; the queue
        // that targeted the location is left without one, suspended.
        crate::integration_queue::suspend_queues_for_deleted_location(&mut transaction, id).await?;
        let result = sqlx::query("DELETE FROM repo_location WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn get_blocking_placement(&self, id: &str) -> Result<Option<WorkspacePlacement>> {
        sqlx::query(
            "SELECT * FROM workspace_placement
             WHERE repo_location_id = ? AND state <> 'cleaned'
             ORDER BY created_at ASC, id ASC LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_workspace_placement)
        .transpose()
    }
}

fn map_repo_location(row: SqliteRow) -> Result<RepoLocation> {
    Ok(RepoLocation {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        owner_kind: parse_enum(row.try_get::<String, _>("owner_kind")?)?,
        daemon_id: row.try_get("daemon_id")?,
        runtime_id: row.try_get("runtime_id")?,
        path: row.try_get("path")?,
        kind: parse_enum(row.try_get::<String, _>("kind")?)?,
        is_default: row.try_get::<i64, _>("is_default")? != 0,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        last_verified_at: row.try_get("last_verified_at")?,
        last_error: row.try_get("last_error")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}
