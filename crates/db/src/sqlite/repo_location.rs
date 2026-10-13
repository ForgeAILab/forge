use super::{
    check_error, decode_offset, limit, order_clause_without_priority, page_from_items, parse_enum,
    workspace_placement::map_workspace_placement, SqliteDb,
};
use crate::{
    CreateRepoLocation, DbError, Page, PageRequest, RepoLocation, RepoLocationRepo, Result,
    SortOrder, UpdateRepoLocation, WorkspacePlacement,
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

pub struct ClaimRepositoryProvisionAttemptInTxInput<'a> {
    pub repo_id: &'a str,
    pub runtime_id: &'a str,
    pub next_attempt_at: String,
    pub checks_digest: &'a str,
    pub connection_id: i64,
    pub started_at: String,
    pub max_attempts: i64,
    pub now: String,
}

impl SqliteDb {
    pub async fn claim_repository_provision_attempt_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: ClaimRepositoryProvisionAttemptInTxInput<'_>,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>("INSERT INTO repo_provision_retry (repo_id, runtime_id, attempts, next_attempt_at, checks_digest, connection_id, started_at) VALUES (?, ?, 1, ?, ?, ?, ?) ON CONFLICT(repo_id, runtime_id) DO UPDATE SET attempts = CASE WHEN checks_digest <> excluded.checks_digest OR connection_id <> excluded.connection_id THEN 1 ELSE attempts + 1 END, next_attempt_at = excluded.next_attempt_at, checks_digest = excluded.checks_digest, connection_id = excluded.connection_id, started_at = excluded.started_at, last_error = CASE WHEN checks_digest <> excluded.checks_digest THEN NULL ELSE last_error END WHERE (checks_digest <> excluded.checks_digest OR connection_id <> excluded.connection_id OR (attempts < ? AND julianday(next_attempt_at) <= julianday(?))) RETURNING attempts",)
.bind(input.repo_id)
.bind(input.runtime_id)
.bind(input.next_attempt_at)
.bind(input.checks_digest)
.bind(input.connection_id)
.bind(input.started_at)
.bind(input.max_attempts)
.bind(input.now)
.fetch_optional(connection).await?)
    }
}

impl SqliteDb {
    pub async fn schedule_repository_provision_successor_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        next_attempt_at: String,
        repo_id: &str,
        runtime_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_provision_retry SET next_attempt_at = ? WHERE repo_id = ? AND runtime_id = ?",)
.bind(next_attempt_at)
.bind(repo_id)
.bind(runtime_id)
.execute(connection).await?)
    }
}

pub struct ReleaseStaleRepositoryProvisionAttemptInput<'a> {
    pub next_attempt_at: String,
    pub repo_id: &'a str,
    pub runtime_id: &'a Option<String>,
    pub attempts: i64,
    pub checks_digest: &'a str,
    pub connection_id: i64,
}

impl SqliteDb {
    pub async fn release_stale_repository_provision_attempt(
        &self,
        input: ReleaseStaleRepositoryProvisionAttemptInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_provision_retry SET next_attempt_at=? WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?",)
.bind(input.next_attempt_at)
.bind(input.repo_id)
.bind(input.runtime_id)
.bind(input.attempts)
.bind(input.checks_digest)
.bind(input.connection_id)
.execute(self.pool()).await?)
    }
}

pub struct RecordRepositoryProvisionFailureInTxInput<'a> {
    pub last_error: String,
    pub next_attempt_at: String,
    pub repo_id: &'a str,
    pub runtime_id: &'a Option<String>,
    pub attempts: i64,
    pub checks_digest: &'a str,
    pub connection_id: i64,
    pub project_id: &'a str,
    pub daemon_id: &'a Option<String>,
    pub readiness_runtime_id: &'a Option<String>,
    pub readiness_version: i64,
}

impl SqliteDb {
    pub async fn record_repository_provision_failure_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: RecordRepositoryProvisionFailureInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_provision_retry SET last_error = ?, next_attempt_at = ? WHERE repo_id = ? AND runtime_id = ? AND attempts = ? AND checks_digest = ? AND connection_id = ? AND COALESCE((SELECT version FROM project_machine_readiness WHERE project_id=? AND owner_kind='daemon' AND daemon_id=? AND runtime_id=?),-1)=?",)
.bind(input.last_error)
.bind(input.next_attempt_at)
.bind(input.repo_id)
.bind(input.runtime_id)
.bind(input.attempts)
.bind(input.checks_digest)
.bind(input.connection_id)
.bind(input.project_id)
.bind(input.daemon_id)
.bind(input.readiness_runtime_id)
.bind(input.readiness_version)
.execute(connection).await?)
    }
}

pub struct RepositoryProvisionAttemptFailureInput<'a> {
    pub repo_id: &'a str,
    pub runtime_id: &'a Option<String>,
    pub attempts: i64,
    pub checks_digest: &'a str,
    pub connection_id: i64,
}

impl SqliteDb {
    pub async fn repository_provision_attempt_failure(
        &self,
        input: RepositoryProvisionAttemptFailureInput<'_>,
    ) -> crate::Result<Option<(Option<String>, Option<String>)>> {
        Ok(sqlx::query_as::<_, (Option<String>, Option<String>)>("SELECT started_at, last_error FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?",)
.bind(input.repo_id)
.bind(input.runtime_id)
.bind(input.attempts)
.bind(input.checks_digest)
.bind(input.connection_id)
.fetch_optional(self.pool()).await?)
    }
}

pub struct RepositoryProvisionAttemptIsCurrentInput<'a> {
    pub repo_id: &'a str,
    pub runtime_id: &'a Option<String>,
    pub attempts: i64,
    pub checks_digest: &'a str,
    pub connection_id: i64,
}

impl SqliteDb {
    pub async fn repository_provision_attempt_is_current(
        &self,
        input: RepositoryProvisionAttemptIsCurrentInput<'_>,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND attempts=? AND checks_digest=? AND connection_id=?)",)
.bind(input.repo_id)
.bind(input.runtime_id)
.bind(input.attempts)
.bind(input.checks_digest)
.bind(input.connection_id)
.fetch_one(self.pool()).await?)
    }
}

pub struct ApplyRepositoryProvisionLocationInTxInput<'a> {
    pub path: &'a Option<String>,
    pub status: String,
    pub last_error: &'a Option<String>,
    pub verified: bool,
    pub verified_at: &'a str,
    pub updated_at: &'a str,
    pub location_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn apply_repository_provision_location_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: ApplyRepositoryProvisionLocationInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_location SET path=COALESCE(?,path),status=?,last_error=?,last_verified_at=CASE WHEN ? THEN ? ELSE last_verified_at END,updated_at=?,version=version+1 WHERE id=? AND version=?",)
.bind(input.path)
.bind(input.status)
.bind(input.last_error)
.bind(input.verified)
.bind(input.verified_at)
.bind(input.updated_at)
.bind(input.location_id)
.bind(input.version)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn delete_verified_repository_provision_retry_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        runtime_id: &Option<String>,
        checks_digest: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "DELETE FROM repo_provision_retry WHERE repo_id=? AND runtime_id=? AND checks_digest=?",
        )
        .bind(repo_id)
        .bind(runtime_id)
        .bind(checks_digest)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_provision_location_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        runtime_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT location_id FROM repo_provision_retry WHERE repo_id = ? AND runtime_id = ?",
        )
        .bind(repo_id)
        .bind(runtime_id)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_provision_location_exists_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        runtime_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM repo_location WHERE repo_id = ? AND runtime_id = ?)",
        )
        .bind(repo_id)
        .bind(runtime_id)
        .fetch_one(connection)
        .await?)
    }
}

pub struct InsertRepositoryProvisionLocationInTxInput<'a> {
    pub location_id: &'a str,
    pub repo_id: &'a str,
    pub daemon_id: &'a str,
    pub runtime_id: &'a str,
    pub path: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_repository_provision_location_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertRepositoryProvisionLocationInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO repo_location (id, repo_id, owner_kind, daemon_id, runtime_id, path, kind, status, created_at, updated_at) VALUES (?, ?, 'daemon', ?, ?, ?, 'managed_clone', 'unverified', ?, ?)",)
.bind(input.location_id)
.bind(input.repo_id)
.bind(input.daemon_id)
.bind(input.runtime_id)
.bind(input.path)
.bind(input.created_at)
.bind(input.updated_at)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn record_repository_provision_location_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        location_id: &str,
        repo_id: &str,
        runtime_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE repo_provision_retry SET location_id = ? WHERE repo_id = ? AND runtime_id = ?",
        )
        .bind(location_id)
        .bind(repo_id)
        .bind(runtime_id)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_location_default_changes_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        location_id: &str,
        repo_id: &str,
        is_default: bool,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM repo_location WHERE id = ? AND repo_id = ? AND is_default <> ?)",)
.bind(location_id)
.bind(repo_id)
.bind(is_default)
.fetch_one(connection).await?)
    }
}

pub struct SetRepositoryLocationDefaultInTxInput<'a> {
    pub is_default: bool,
    pub updated_at: &'a str,
    pub location_id: &'a str,
    pub repo_id: &'a str,
    pub version: i64,
}

impl SqliteDb {
    pub async fn set_repository_location_default_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: SetRepositoryLocationDefaultInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_location SET is_default = ?, version = version + 1, updated_at = ? WHERE id = ? AND repo_id = ? AND version = ?",)
.bind(input.is_default)
.bind(input.updated_at)
.bind(input.location_id)
.bind(input.repo_id)
.bind(input.version)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn other_repository_location_defaults_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        location_id: &str,
    ) -> crate::Result<Vec<(String, i64)>> {
        Ok(sqlx::query_as::<_, (String, i64)>("SELECT id, version FROM repo_location WHERE repo_id = ? AND id <> ? AND is_default = 1",)
.bind(repo_id)
.bind(location_id)
.fetch_all(connection).await?)
    }
}

impl SqliteDb {
    pub async fn clear_other_repository_location_default_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: &str,
        location_id: String,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE repo_location SET is_default = 0, version = version + 1, updated_at = ? WHERE id = ? AND version = ?",)
.bind(updated_at)
.bind(location_id)
.bind(version)
.execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn repository_location_provisioning_exists(
        &self,
        location_id: &str,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM repo_provision_retry WHERE location_id = ?)",
        )
        .bind(location_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn repository_location_reconnect_verification_ids(
        &self,
        daemon_id: &str,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM repo_location WHERE daemon_id = ?
             AND (owner_kind = 'daemon' OR kind = 'shared_mount')
             AND NOT EXISTS (SELECT 1 FROM repo_provision_retry j
                             WHERE j.location_id = repo_location.id)
             ORDER BY created_at, id",
        )
        .bind(daemon_id)
        .fetch_all(self.pool())
        .await?)
    }
}

#[derive(Clone, Copy)]
pub enum RepoLocationPageSort {
    CreatedAt,
    UpdatedAt,
    Id,
}
pub struct VisibleRepositoryLocationPageInput<'a> {
    pub repo_id: &'a str,
    pub user_id: &'a str,
    pub sort: RepoLocationPageSort,
    pub order: SortOrder,
    pub limit: i64,
    pub offset: i64,
}
const VISIBLE_REPOSITORY_LOCATION_PREDICATE: &str =
    "l.repo_id = ? AND (l.daemon_id IS NULL OR EXISTS (
            SELECT 1 FROM daemon d WHERE d.id = l.daemon_id
            AND (d.visibility = 'global' OR d.owner_id IS NULL OR d.owner_id = ?)))";
impl SqliteDb {
    pub async fn visible_repository_location_page_ids(
        &self,
        input: VisibleRepositoryLocationPageInput<'_>,
    ) -> crate::Result<Vec<String>> {
        let sort = match input.sort {
            RepoLocationPageSort::CreatedAt => "l.created_at",
            RepoLocationPageSort::UpdatedAt => "l.updated_at",
            RepoLocationPageSort::Id => "l.id",
        };
        let order = match input.order {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        };
        let predicate = VISIBLE_REPOSITORY_LOCATION_PREDICATE;
        Ok(sqlx::query_scalar::<_, String>(&format!(
            "SELECT l.id FROM repo_location l WHERE {predicate} ORDER BY {sort} {order}, l.id {order} LIMIT ? OFFSET ?"
        )).bind(input.repo_id).bind(input.user_id).bind(input.limit).bind(input.offset).fetch_all(self.pool()).await?)
    }
    pub async fn visible_repository_location_count(
        &self,
        repo_id: &str,
        user_id: &str,
    ) -> crate::Result<i64> {
        let predicate = VISIBLE_REPOSITORY_LOCATION_PREDICATE;
        Ok(sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM repo_location l WHERE {predicate}"
        ))
        .bind(repo_id)
        .bind(user_id)
        .fetch_one(self.pool())
        .await?)
    }
}

#[derive(Debug)]
pub struct PlacementEligibilityLocationRow {
    pub id: String,
    pub version: i64,
    pub status: String,
    pub runtime_status: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PlacementEligibilityLocationRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get(0)?,
            version: row.try_get(1)?,
            status: row.try_get(2)?,
            runtime_status: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_locations(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<PlacementEligibilityLocationRow>> {
        Ok(sqlx::query_as("SELECT l.id,l.version,l.status,COALESCE(r.status,'') FROM repo_location l JOIN repo ON repo.id=l.repo_id LEFT JOIN runtime r ON r.id=l.runtime_id WHERE repo.project_id=? ORDER BY l.id").bind(project_id).fetch_all(self.pool()).await?)
    }
}

#[derive(Debug)]
pub struct PlacementEligibilityProvisionRetryRow {
    pub runtime_id: String,
    pub attempts: i64,
    pub checks_digest: String,
    pub connection_id: i64,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for PlacementEligibilityProvisionRetryRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            runtime_id: row.try_get(0)?,
            attempts: row.try_get(1)?,
            checks_digest: row.try_get(2)?,
            connection_id: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn placement_eligibility_provision_retries(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<PlacementEligibilityProvisionRetryRow>> {
        Ok(sqlx::query_as("SELECT j.runtime_id,j.attempts,j.checks_digest,j.connection_id FROM repo_provision_retry j JOIN repo r ON r.id=j.repo_id WHERE r.project_id=? ORDER BY j.repo_id,j.runtime_id").bind(project_id).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn placement_refusal_provision_error(
        &self,
        repo_id: &str,
        runtime_id: &Option<String>,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar("SELECT COALESCE(l.last_error,j.last_error) FROM repo_provision_retry j LEFT JOIN repo_location l ON l.id=j.location_id WHERE j.repo_id=? AND j.runtime_id=?").bind(repo_id).bind(runtime_id).fetch_optional(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_server_clone_paths(&self) -> crate::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT path FROM repo_location WHERE owner_kind = 'server'")
                .fetch_all(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn project_environment_ready_location_owners(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<(String, Option<String>, Option<String>)>> {
        Ok(sqlx::query_as(
        "SELECT l.owner_kind,l.daemon_id,l.runtime_id FROM repo_location l JOIN repo r ON r.id=l.repo_id WHERE r.project_id=? AND l.status='ready'").bind(project_id).fetch_all(self.pool()).await?)
    }
}

pub struct PlacementRepoLocationSourceRow {
    row: SqliteRow,
}
impl PlacementRepoLocationSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn repo_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("repo_id")?)
    }
    pub fn owner_kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("owner_kind")?)
    }
    pub fn daemon_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("daemon_id")?)
    }
    pub fn runtime_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("runtime_id")?)
    }
    pub fn path(&self) -> crate::Result<String> {
        Ok(self.row.try_get("path")?)
    }
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn is_default(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("is_default")?)
    }
    pub fn status(&self) -> crate::Result<String> {
        Ok(self.row.try_get("status")?)
    }
    pub fn last_verified_at(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("last_verified_at")?)
    }
    pub fn last_error(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("last_error")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn placement_repository_location_sources_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
    ) -> crate::Result<Vec<PlacementRepoLocationSourceRow>> {
        let rows =
            sqlx::query("SELECT * FROM repo_location WHERE repo_id = ? ORDER BY created_at, id")
                .bind(repo_id)
                .fetch_all(connection)
                .await?;
        Ok(rows
            .into_iter()
            .map(|row| PlacementRepoLocationSourceRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn embedded_workspace_existing_repo_location_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        repo_id: &str,
        source: &str,
        kind: String,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM repo_location WHERE repo_id = ? AND owner_kind = 'server'
             AND daemon_id IS NULL AND runtime_id IS NULL AND path = ? AND kind = ?
             ORDER BY created_at, id LIMIT 1",
        )
        .bind(repo_id)
        .bind(source)
        .bind(kind)
        .fetch_optional(connection)
        .await?)
    }
}

pub struct InsertEmbeddedWorkspaceRepoLocationInTxInput<'a> {
    pub id: &'a str,
    pub repo_id: &'a str,
    pub source: &'a str,
    pub kind: String,
    pub is_default: bool,
    pub status: &'a str,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_embedded_workspace_repo_location_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertEmbeddedWorkspaceRepoLocationInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO repo_location (id, repo_id, owner_kind, path, kind,
                     is_default, status, created_at, updated_at)
                     VALUES (?, ?, 'server', ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.repo_id)
        .bind(input.source)
        .bind(input.kind)
        .bind(input.is_default)
        .bind(input.status)
        .bind(input.created_at)
        .bind(input.updated_at)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_transfer_ready_server_location(
        &self,
        location_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM repo_location WHERE id=? AND owner_kind='server' AND status='ready'",
        )
        .bind(location_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_transfer_ready_server_locations(
        &self,
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(
            "SELECT id,path FROM repo_location WHERE owner_kind='server' AND status='ready' ORDER BY id",
        ).fetch_all(self.pool()).await?)
    }
}

#[derive(Debug)]
pub struct IntegrationTransferOwedReleaseRow {
    pub attempt_id: String,
    pub repo_location_id: String,
    pub daemon_id: String,
    pub runtime_id: String,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for IntegrationTransferOwedReleaseRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            attempt_id: row.try_get(0)?,
            repo_location_id: row.try_get(1)?,
            daemon_id: row.try_get(2)?,
            runtime_id: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn integration_transfer_owed_release_ends(
        &self,
        since: &str,
        limit: u32,
    ) -> crate::Result<Vec<IntegrationTransferOwedReleaseRow>> {
        Ok(sqlx::query_as(
            "SELECT a.id,l.id,l.daemon_id,l.runtime_id FROM integration_attempt a \
             JOIN integration_queue q ON q.id=a.queue_id \
             JOIN repo_location l ON l.id IN (a.repo_location_id,q.target_location_id) \
             WHERE l.owner_kind='daemon' AND l.daemon_id IS NOT NULL AND l.runtime_id IS NOT NULL \
               AND a.repo_location_id IS NOT NULL AND q.target_location_id IS NOT NULL \
               AND a.repo_location_id<>q.target_location_id \
               AND (q.head_attempt_id IS NULL OR q.head_attempt_id<>a.id) \
               AND a.started_at IS NOT NULL AND a.updated_at>=? ORDER BY a.updated_at DESC,a.id LIMIT ?",
        ).bind(since).bind(limit).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn integration_owner_server_checkout_path(
        &self,
        location_id: Option<&str>,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT path FROM repo_location WHERE id=? AND owner_kind='server'")
                .bind(location_id)
                .fetch_optional(self.pool())
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct IntegrationTargetLocationRow {
    pub path: String,
    pub owner_kind: String,
    pub daemon_id: Option<String>,
    pub runtime_id: Option<String>,
}
impl<'r> sqlx::FromRow<'r, SqliteRow> for IntegrationTargetLocationRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            path: row.try_get(0)?,
            owner_kind: row.try_get(1)?,
            daemon_id: row.try_get(2)?,
            runtime_id: row.try_get(3)?,
        })
    }
}

impl SqliteDb {
    pub async fn integration_target_location(
        &self,
        location_id: &str,
    ) -> crate::Result<Option<IntegrationTargetLocationRow>> {
        Ok(sqlx::query_as(
            "SELECT path,owner_kind,daemon_id,runtime_id FROM repo_location WHERE id=?",
        )
        .bind(location_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn integration_target_disconnected_daemon_id(
        &self,
        location_id: &str,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar(
            "SELECT l.daemon_id FROM repo_location l JOIN daemon d ON d.id=l.daemon_id \
         WHERE l.id=? AND l.owner_kind='daemon' AND d.removed_at IS NULL",
        )
        .bind(location_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn machine_precheck_repository_location_statuses(
        &self,
        repo_id: &str,
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(
            sqlx::query_as("SELECT id, status FROM repo_location WHERE repo_id = ?")
                .bind(repo_id)
                .fetch_all(self.pool())
                .await?,
        )
    }
}
