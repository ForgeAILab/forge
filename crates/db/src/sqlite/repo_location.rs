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
        // Moving a repository's default checkout takes the target away from
        // its queues: refused while a merge into the repository has an
        // unknown result or an outstanding owner effect.
        if input.is_default.is_some() {
            let mut guard = crate::begin_immediate(&self.pool).await?;
            let changes: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM repo_location WHERE id=? AND is_default<>?)",
            )
            .bind(&input.id)
            .bind(input.is_default)
            .fetch_one(&mut *guard)
            .await?;
            if changes {
                crate::integration_queue::refuse_unresolved_integration(
                    &mut guard,
                    crate::integration_queue::IntegrationGuardScope::RepoOfLocation(&input.id),
                )
                .await?;
            }
            guard.commit().await?;
        }
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
        // A merge whose result is unknown, or whose owner effect is still
        // outstanding, into or from this location refuses the deletion.
        crate::integration_queue::refuse_unresolved_integration(
            &mut transaction,
            crate::integration_queue::IntegrationGuardScope::Location(id),
        )
        .await?;
        // A queue with nothing unresolved never blocks this deletion; the
        // one that targeted the location is left without one, suspended.
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

// Preserve the original tuple query's positional decoding and error indices.
impl<'r> sqlx::FromRow<'r, SqliteRow> for CloneVerificationLocationRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get(0)?,
            path: row.try_get(1)?,
            version: row.try_get(2)?,
            last_error: row.try_get(3)?,
        })
    }
}
#[derive(Debug)]
pub struct CloneVerificationLocationRow {
    pub id: String,
    pub path: String,
    pub version: i64,
    pub last_error: Option<String>,
}

impl SqliteDb {
    pub async fn workspace_admission_clone_verification_locations(
        &self,
        repo_id: &str,
        bound_location: &Option<String>,
        matching_location: &Option<String>,
    ) -> crate::Result<Vec<CloneVerificationLocationRow>> {
        Ok(sqlx::query_as::<_, CloneVerificationLocationRow>("SELECT id, path, version, last_error FROM repo_location WHERE repo_id = ?
                 AND owner_kind = 'server' AND daemon_id IS NULL AND runtime_id IS NULL
                 AND kind = 'managed_clone' AND (status = 'unverified' OR (status = 'unavailable' AND last_error LIKE '{%')
                      OR (status = 'invalid' AND last_error = 'managed_clone_is_worktree'))
                 AND (? IS NULL OR id = ?)")
.bind(repo_id)
.bind(bound_location)
.bind(matching_location)
.fetch_all(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn workspace_admission_server_location_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repo_location WHERE repo_id = ? AND owner_kind = 'server' AND daemon_id IS NULL")
.bind(repo_id)
.fetch_one(connection)
.await?)
    }
}

#[derive(Debug)]
pub struct ReadyServerLocationRow {
    id: std::result::Result<String, sqlx::Error>,
}

impl ReadyServerLocationRow {
    /// Preserve the original `Row::get` panic only when this fallback is used.
    pub fn id(self) -> String {
        self.id.unwrap()
    }
}

impl SqliteDb {
    pub async fn workspace_admission_ready_server_location_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
    ) -> crate::Result<Option<ReadyServerLocationRow>> {
        let rows = sqlx::query("SELECT id FROM repo_location WHERE repo_id = ? AND owner_kind = 'server' AND status = 'ready'
                AND daemon_id IS NULL ORDER BY is_default DESC, created_at, id LIMIT 1")
.bind(repo_id)
.fetch_optional(connection)
.await?;
        Ok(rows.map(|row| ReadyServerLocationRow {
            id: row.try_get("id"),
        }))
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct PreparedServerRepoLocationRow {
    pub id: crate::Result<String>,
    pub status: crate::Result<String>,
    pub version: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn prepared_server_repo_location_in_tx(
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        kind: String,
        source: &str,
    ) -> crate::Result<Option<PreparedServerRepoLocationRow>> {
        let rows = sqlx::query(
            "SELECT id, status, version FROM repo_location
         WHERE repo_id = ? AND owner_kind = 'server' AND daemon_id IS NULL
           AND runtime_id IS NULL AND kind = ? AND path = ?
         ORDER BY created_at ASC, id ASC LIMIT 1",
        )
        .bind(repo_id)
        .bind(kind)
        .bind(source)
        .fetch_optional(connection)
        .await?;
        rows.map(|row| {
            Ok(PreparedServerRepoLocationRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                version: row.try_get("version").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn mark_prepared_server_repo_location_ready_in_tx(
        connection: &mut sqlx::SqliteConnection,
        last_verified_at: &str,
        updated_at: &str,
        id: &str,
        expected_version: i64,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE repo_location SET status = 'ready', last_verified_at = ?,
                 last_error = NULL, version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?",
        )
        .bind(last_verified_at)
        .bind(updated_at)
        .bind(id)
        .bind(expected_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertPreparedServerRepoLocationInTxInput<'a> {
    pub id: &'a str,
    pub repo_id: &'a str,
    pub source: &'a str,
    pub kind: String,
    pub is_default: bool,
    pub last_verified_at: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_prepared_server_repo_location_in_tx(
        connection: &mut sqlx::SqliteConnection,
        input: InsertPreparedServerRepoLocationInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO repo_location (
            id, repo_id, owner_kind, path, kind, is_default, status,
            last_verified_at, created_at, updated_at
         ) VALUES (?, ?, 'server', ?, ?, ?, 'ready', ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.repo_id)
        .bind(input.source)
        .bind(input.kind)
        .bind(input.is_default)
        .bind(input.last_verified_at)
        .bind(input.created_at)
        .bind(input.updated_at)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct EnvironmentProvisionRetryDetailRow {
    pub started_at: Option<String>,
    pub last_error: Option<String>,
    pub attempts: i64,
    pub checks_digest: String,
    pub connection_id: i64,
}

// Preserve the original tuple query's positional decoding and error indices.
impl<'r> sqlx::FromRow<'r, SqliteRow> for EnvironmentProvisionRetryDetailRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            started_at: row.try_get(0)?,
            last_error: row.try_get(1)?,
            attempts: row.try_get(2)?,
            checks_digest: row.try_get(3)?,
            connection_id: row.try_get(4)?,
        })
    }
}

impl SqliteDb {
    pub async fn environment_probe_visible_daemon_locations(
        &self,
        project_id: &str,
        owner_id: Option<&str>,
    ) -> crate::Result<Vec<(String, String, String)>> {
        Ok(sqlx::query_as::<_, (String, String, String)>("SELECT l.id, l.daemon_id, l.runtime_id FROM repo_location l JOIN repo r ON r.id = l.repo_id JOIN daemon d ON d.id = l.daemon_id WHERE r.project_id = ? AND l.owner_kind = 'daemon' AND l.status = 'ready' AND (d.visibility = 'global' OR d.owner_id IS NULL OR d.owner_id = ?)")
            .bind(project_id)
            .bind(owner_id)
            .fetch_all(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn environment_probe_server_checkout_path(
        &self,
        project_id: &str,
        primary_repo_id: Option<&str>,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT l.path FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.owner_kind = 'server' AND l.status = 'ready' ORDER BY (r.id = ?) DESC, l.is_default DESC, l.created_at, l.id LIMIT 1")
            .bind(project_id)
            .bind(primary_repo_id)
            .fetch_optional(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn environment_probe_daemon_location(
        &self,
        project_id: &str,
        daemon_id: &str,
        runtime_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>("SELECT l.id FROM repo_location l JOIN repo r ON r.id = l.repo_id WHERE r.project_id = ? AND l.daemon_id = ? AND l.runtime_id = ? AND l.status = 'ready' ORDER BY l.is_default DESC, l.created_at, l.id LIMIT 1")
            .bind(project_id)
            .bind(daemon_id)
            .bind(runtime_id)
            .fetch_optional(self.pool())
            .await?)
    }
}

impl SqliteDb {
    pub async fn environment_provision_retry_detail(
        &self,
        repo_id: &str,
        runtime_id: &str,
    ) -> crate::Result<Option<EnvironmentProvisionRetryDetailRow>> {
        Ok(sqlx::query_as::<_, EnvironmentProvisionRetryDetailRow>("SELECT j.started_at, COALESCE(l.last_error, j.last_error), j.attempts, j.checks_digest, j.connection_id FROM repo_provision_retry j LEFT JOIN repo_location l ON l.id = j.location_id WHERE j.repo_id = ? AND j.runtime_id = ?")
            .bind(repo_id)
            .bind(runtime_id)
            .fetch_optional(self.pool())
            .await?)
    }
}
