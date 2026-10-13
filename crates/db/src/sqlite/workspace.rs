use super::workspace_root::under_sql;
use super::*;

#[async_trait]
impl WorkspaceRepo for SqliteDb {
    async fn create(&self, input: CreateWorkspace) -> Result<Workspace> {
        sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, before_sha, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.task_id)
            .bind(&input.repo_id)
            .bind(&input.worktree_path)
            .bind(&input.branch)
            .bind(input.status.to_string())
            .bind(input.before_sha.as_deref())
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&self.pool)
            .await?;
        WorkspaceRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Workspace>> {
        sqlx::query("SELECT * FROM workspace WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_workspace)
            .transpose()
    }

    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<Workspace>> {
        sqlx::query("SELECT * FROM workspace WHERE task_id = ?")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_workspace)
            .transpose()
    }

    async fn embedded_path_is_owned_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        path: &str,
    ) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM workspace WHERE worktree_path = ? LIMIT 1")
                .bind(path)
                .fetch_optional(&mut **transaction)
                .await?
                .is_some(),
        )
    }

    async fn task_owns_embedded_path(&self, task_id: &str, path: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM workspace
             LEFT JOIN workspace_placement AS placement
               ON placement.workspace_id = workspace.id
             WHERE workspace.task_id = ?
               AND workspace.status IN ('creating', 'ready', 'error')
               AND ((placement.owner_kind = 'server' AND placement.workspace_handle = ?)
                    OR (placement.id IS NULL AND workspace.worktree_path = ?))
             LIMIT 1",
        )
        .bind(task_id)
        .bind(path)
        .bind(path)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    async fn set_cleanup_after(
        &self,
        id: &str,
        cleanup_after: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace> {
        let result = sqlx::query(
            "UPDATE workspace
             SET cleanup_after = ?,
                 cleanup_attempts = CASE WHEN ? IS NULL THEN 0 ELSE cleanup_attempts END,
                 last_cleanup_error = CASE WHEN ? IS NULL THEN NULL ELSE last_cleanup_error END,
                 updated_at = ?
             WHERE id = ?",
        )
        .bind(cleanup_after.as_deref())
        .bind(cleanup_after.as_deref())
        .bind(cleanup_after.as_deref())
        .bind(updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        WorkspaceRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_cleaned(&self, id: &str, updated_at: &str) -> Result<Workspace> {
        let result = sqlx::query(
            "UPDATE workspace
             SET status = 'cleaned', cleanup_after = NULL, cleanup_attempts = 0,
                 last_cleanup_error = NULL, error = NULL, updated_at = ?
             WHERE id = ?",
        )
        .bind(updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        WorkspaceRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_pending_cleanup(&self, now: &str) -> Result<Vec<Workspace>> {
        let rows = sqlx::query(
            "SELECT * FROM workspace WHERE cleanup_after IS NOT NULL AND cleanup_after <= ? AND status != 'cleaned' ORDER BY cleanup_after ASC, id ASC",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_workspace).collect()
    }

    async fn update_status(
        &self,
        id: &str,
        status: WorkspaceStatus,
        error: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace> {
        let result =
            sqlx::query("UPDATE workspace SET status = ?, error = ?, updated_at = ? WHERE id = ?")
                .bind(status.to_string())
                .bind(error.as_deref())
                .bind(updated_at)
                .bind(id)
                .execute(&self.pool)
                .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        WorkspaceRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let mut transaction = crate::begin_immediate(self.pool()).await?;
        // A pending_remote_cancel record is daemon-scoped and never blocks
        // deletion; reconnect still sends workspace.cancel for it. A remote
        // operation whose step is still live does.
        let running: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_remote_operation r JOIN task_step s ON s.id=r.step_id WHERE r.workspace_id=? AND r.state='running' AND s.status IN ('pending','claimed'))")
            .bind(id).fetch_one(&mut *transaction).await?;
        if running {
            return Err(DbError::ResourceInUse {
                resource: format!("workspace {id}"),
                reason: "a remote workspace operation is still running".into(),
            });
        }
        // A cancellation fences Tasks through its workspace row. Read who it
        // fences before the row goes, and restate their conditions after.
        let fenced = crate::remote_cancel::fenced_tasks(&mut transaction, id).await?;
        let result = sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        for task_id in &fenced {
            crate::task_condition::produce(
                &mut transaction,
                task_id,
                crate::ConditionChange::Operations,
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

impl SqliteDb {
    pub async fn workspace_cleanup_attempts_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<i64>> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT cleanup_attempts FROM workspace
             WHERE task_id = ? AND status != 'cleaned'",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?;
        Ok(result)
    }
}

#[derive(Debug)]
pub struct RecordWorkspaceCleanupFailureInput<'a> {
    pub increment: i64,
    pub last_error: &'a str,
    pub cleanup_after: &'a str,
    pub updated_at: &'a str,
    pub task_id: &'a str,
}

impl SqliteDb {
    pub async fn record_workspace_cleanup_failure_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &RecordWorkspaceCleanupFailureInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE workspace
             SET cleanup_attempts = cleanup_attempts + ?,
                 last_cleanup_error = ?, cleanup_after = ?, updated_at = ?
             WHERE task_id = ? AND status != 'cleaned'",
        )
        .bind(input.increment)
        .bind(input.last_error)
        .bind(input.cleanup_after)
        .bind(input.updated_at)
        .bind(input.task_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug, Clone)]
pub struct WorkspaceCleanupAttentionRow {
    pub id: String,
    pub worktree_path: String,
    pub cleanup_attempts: i64,
    pub last_cleanup_error: Option<String>,
    pub project_id: String,
    pub title: String,
}

impl SqliteDb {
    pub async fn workspace_cleanup_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<WorkspaceCleanupAttentionRow>> {
        let row = sqlx::query(
            "SELECT w.id, w.worktree_path, w.cleanup_attempts, w.last_cleanup_error,
                    t.project_id, t.title
             FROM workspace w JOIN task t ON t.id = w.task_id
             WHERE w.task_id = ? AND w.status != 'cleaned'",
        )
        .bind(task_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(WorkspaceCleanupAttentionRow {
                id: row.try_get("id")?,
                worktree_path: row.try_get("worktree_path")?,
                cleanup_attempts: row.try_get("cleanup_attempts")?,
                last_cleanup_error: row.try_get("last_cleanup_error")?,
                project_id: row.try_get("project_id")?,
                title: row.try_get("title")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn clear_workspace_cleanup_retry(
        &self,
        updated_at: &str,
        task_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE workspace
             SET cleanup_after = NULL, cleanup_attempts = 0,
                 last_cleanup_error = NULL, updated_at = ?
             WHERE task_id = ?
               AND (cleanup_after IS NOT NULL OR cleanup_attempts != 0
                    OR last_cleanup_error IS NOT NULL)",
        )
        .bind(updated_at)
        .bind(task_id)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn due_workspace_cleanup_task_ids(
        &self,
        now: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        let result = sqlx::query_scalar::<_, String>("SELECT task_id FROM workspace
             WHERE status != 'cleaned' AND (cleanup_after <= ? OR (status = 'cleaning' AND cleanup_after IS NULL))
             ORDER BY cleanup_after, id LIMIT ?")
            .bind(now)
            .bind(limit)
            .fetch_all(self.pool()).await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn workspace_cleanup_has_active_execution_or_lease(
        &self,
        task_id: &str,
        workspace_id: Option<&str>,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                SELECT 1 FROM execution
                WHERE (task_id = ? OR workspace_id = ?) AND status = 'running'
             ) OR EXISTS (
                SELECT 1 FROM workspace_lease wl
                LEFT JOIN execution e ON e.id = wl.execution_id
                WHERE (wl.task_id = ? OR e.workspace_id = ?) AND wl.status = 'active'
             )",
        )
        .bind(task_id)
        .bind(workspace_id)
        .bind(task_id)
        .bind(workspace_id)
        .fetch_one(self.pool())
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn workspace_branch_is_shared(
        &self,
        repo_id: &str,
        branch: &str,
        workspace_id: &str,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (SELECT 1 FROM workspace
                            WHERE repo_id = ? AND branch = ? AND id != ? AND status != 'cleaned')",
        )
        .bind(repo_id)
        .bind(branch)
        .bind(workspace_id)
        .fetch_one(self.pool())
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn mark_workspace_cleaning_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        now: &str,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE workspace SET status = 'cleaning', cleanup_after = COALESCE(cleanup_after, ?), updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(now)
            .bind(workspace_id)
            .execute(connection).await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn mark_workspace_cleaned_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        now: &str,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE workspace
                         SET status = 'cleaned', cleanup_after = NULL,
                             cleanup_attempts = 0, last_cleanup_error = NULL,
                             error = NULL, updated_at = ?
                         WHERE id = ?",
        )
        .bind(now)
        .bind(workspace_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug, Clone)]
pub struct OverdueWorkspaceCleanupRow {
    pub id: String,
    pub task_id: String,
    pub cleanup_after: Option<String>,
}

impl SqliteDb {
    pub async fn overdue_workspace_cleanup_rows(
        &self,
        now: &str,
    ) -> crate::Result<Vec<OverdueWorkspaceCleanupRow>> {
        let rows = sqlx::query(
            "SELECT id, task_id, cleanup_after
             FROM workspace
             WHERE status IN ('ready', 'cleaning')
               AND cleanup_after IS NOT NULL
               AND cleanup_after < ?
             ORDER BY cleanup_after ASC, id ASC",
        )
        .bind(now)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(OverdueWorkspaceCleanupRow {
                    id: row.try_get("id")?,
                    task_id: row.try_get("task_id")?,
                    cleanup_after: row.try_get("cleanup_after")?,
                })
            })
            .collect()
    }
}

impl SqliteDb {
    pub async fn mark_owner_workspace_cleaned_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        updated_at: String,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE workspace SET status = 'cleaned', cleanup_after = NULL, cleanup_attempts = 0, last_cleanup_error = NULL, error = NULL, updated_at = ? WHERE id = ?")
.bind(updated_at)
.bind(workspace_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct RecordedOwnerWorkspaceHeadInput<'a> {
    pub placement_id: &'a str,
    pub generation: i64,
    pub workspace_id: &'a str,
    pub reset_placement_id: &'a str,
    pub reset_generation: i64,
}

impl SqliteDb {
    pub async fn recorded_owner_workspace_head(
        &self,
        input: RecordedOwnerWorkspaceHeadInput<'_>,
    ) -> crate::Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>("SELECT head_sha FROM (
         SELECT head_sha, recorded_at AS evidence_at, placement_id AS evidence_id FROM workspace_expected_head
         WHERE placement_id = ? AND generation = ?
         UNION ALL
         SELECT e.after_sha AS head_sha, (COALESCE(e.stopped_at, (SELECT MIN(receipt.created_at) FROM execution_terminal_receipt receipt WHERE receipt.execution_id = e.id), e.created_at)) AS evidence_at, e.id AS evidence_id FROM execution e WHERE e.workspace_id = ? AND e.after_sha IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM command_receipt reset
             WHERE reset.operation = 'daemon.workspace.reset'
               AND json_extract(reset.outcome_json, '$.metadata.placement_id') = ?
               AND json_extract(reset.outcome_json, '$.metadata.generation') = ?
               AND json_extract(reset.outcome_json, '$.metadata.status') = 'result'
               AND julianday(reset.committed_at) > julianday(e.created_at))
         ) ORDER BY julianday(evidence_at) DESC, evidence_id DESC LIMIT 1")
.bind(input.placement_id)
.bind(input.generation)
.bind(input.workspace_id)
.bind(input.reset_placement_id)
.bind(input.reset_generation)
.fetch_optional(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn workspace_admission_task_workspace_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT id FROM workspace WHERE task_id = ?")
                .bind(task_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_admission_workspace_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT id FROM workspace WHERE task_id = ?")
                .bind(task_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

#[derive(Debug)]
pub struct InsertCreatingAdmissionWorkspaceInTxInput<'a> {
    pub id: &'a str,
    pub task_id: &'a str,
    pub repo_id: &'a str,
    pub worktree_path: String,
    pub branch: String,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_creating_admission_workspace_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertCreatingAdmissionWorkspaceInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'creating', ?, ?)")
.bind(input.id)
.bind(input.task_id)
.bind(input.repo_id)
.bind(input.worktree_path)
.bind(input.branch)
.bind(input.created_at)
.bind(input.updated_at)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn workspace_admission_repo_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        workspace_id: &str,
    ) -> crate::Result<String> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT repo_id FROM workspace WHERE id = ?")
                .bind(workspace_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn mark_reserved_workspace_creating_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        worktree_path: String,
        updated_at: &str,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE workspace SET status = 'creating', worktree_path = ?, error = NULL, updated_at = ? WHERE id = ?")
.bind(worktree_path)
.bind(updated_at)
.bind(workspace_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct MarkPreparedAdmissionWorkspaceReadyInTxInput<'a> {
    pub branch: String,
    pub before_sha: String,
    pub worktree_path: String,
    pub updated_at: String,
    pub workspace_id: &'a str,
}

impl SqliteDb {
    pub async fn mark_prepared_admission_workspace_ready_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: MarkPreparedAdmissionWorkspaceReadyInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE workspace SET status = 'ready', branch = ?, before_sha = COALESCE(before_sha, ?),
                worktree_path = ?, error = NULL, cleanup_after = NULL, cleanup_attempts = 0,
                last_cleanup_error = NULL, updated_at = ? WHERE id = ?")
.bind(input.branch)
.bind(input.before_sha)
.bind(input.worktree_path)
.bind(input.updated_at)
.bind(input.workspace_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertPreparedServerWorkspaceInTxInput<'a> {
    pub id: &'a str,
    pub task_id: &'a str,
    pub repo_id: &'a str,
    pub worktree_path: &'a str,
    pub branch: &'a str,
    pub status: String,
    pub before_sha: Option<&'a str>,
    pub created_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_prepared_server_workspace_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertPreparedServerWorkspaceInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO workspace (
            id, task_id, repo_id, worktree_path, branch, status, before_sha, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.task_id)
        .bind(input.repo_id)
        .bind(input.worktree_path)
        .bind(input.branch)
        .bind(input.status)
        .bind(input.before_sha)
        .bind(input.created_at)
        .bind(input.updated_at)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn mark_server_workspace_ready_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        worktree_path: &str,
        updated_at: &str,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE workspace SET status = 'ready', worktree_path = ?, error = NULL, updated_at = ?
         WHERE id = ?",
        )
        .bind(worktree_path)
        .bind(updated_at)
        .bind(workspace_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn record_owner_workspace_reset_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        before_sha: String,
        branch: String,
        updated_at: String,
        workspace_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE workspace SET status = 'ready', before_sha = ?, branch = ?, cleanup_after = NULL,
            cleanup_attempts = 0, last_cleanup_error = NULL, error = NULL, updated_at = ? WHERE id = ?")
.bind(before_sha)
.bind(branch)
.bind(updated_at)
.bind(workspace_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn native_ready_workspace_path(
        &self,
        workspace_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT worktree_path FROM workspace WHERE id = ? AND status = 'ready'",
        )
        .bind(workspace_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_live_worktree_paths(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT worktree_path FROM workspace WHERE status != 'cleaned'",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_task_statuses(&self, task_id: &str) -> crate::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT status FROM workspace WHERE task_id = ?")
                .bind(task_id)
                .fetch_all(self.pool())
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn workspace_gc_path_prefix_statuses(
        &self,
        prefix: String,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT status FROM workspace
                     WHERE substr(worktree_path, 1, length(?1)) = ?1",
        )
        .bind(prefix)
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_build_eviction_candidates(
        &self,
        limit: i64,
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as::<_, (String, String)>(
            "SELECT task_id, worktree_path FROM workspace
             WHERE status = 'ready' ORDER BY updated_at, id LIMIT ?",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_gc_measurement_candidates(
        &self,
        prefix: String,
        limit: i64,
    ) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as::<_, (String, String)>(
            "SELECT id, worktree_path FROM workspace
                     WHERE status NOT IN ('cleaned', 'cleaning')
                       AND substr(worktree_path, 1, length(?1)) = ?1
                     ORDER BY disk_measured_at IS NOT NULL, disk_measured_at, id LIMIT ?2",
        )
        .bind(prefix)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_workspace_gc_measurement(
        &self,
        bytes: Option<i64>,
        measured_at: String,
        workspace_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE workspace SET disk_bytes = COALESCE(?, disk_bytes), disk_measured_at = ?
                 WHERE id = ? AND status NOT IN ('cleaned', 'cleaning')",
        )
        .bind(bytes)
        .bind(measured_at)
        .bind(workspace_id)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_server_worktrees(&self) -> crate::Result<Vec<(String, String)>> {
        Ok(sqlx::query_as(
            "SELECT w.worktree_path, w.task_id FROM workspace w
         LEFT JOIN workspace_placement p ON p.workspace_id = w.id
         WHERE w.status != 'cleaned' AND COALESCE(p.owner_kind, 'server') = 'server'",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn workspace_root_live_workspace_count(&self, spelling: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM workspace WHERE status != 'cleaned' AND {}",
            under_sql("worktree_path")
        ))
        .bind(spelling)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn delete_unacquired_task_workspace_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        workspace_id: &str,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "DELETE FROM workspace
                     WHERE id = ? AND task_id = ?
                       AND NOT EXISTS (
                           SELECT 1 FROM execution
                           WHERE execution.workspace_id = workspace.id
                       )",
        )
        .bind(workspace_id)
        .bind(task_id)
        .execute(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn record_review_rebase_expected_head(
        &self,
        head_sha: String,
        recorded_at: String,
        placement_id: &str,
        generation: i64,
        version: i64,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "INSERT INTO workspace_expected_head (placement_id, generation, head_sha, recorded_at)
             SELECT id, generation, ?, ? FROM workspace_placement
             WHERE id = ? AND generation = ? AND version = ?
             ON CONFLICT(placement_id) DO UPDATE SET generation = excluded.generation,
                 head_sha = excluded.head_sha, recorded_at = excluded.recorded_at",
        )
        .bind(head_sha)
        .bind(recorded_at)
        .bind(placement_id)
        .bind(generation)
        .bind(version)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn schedule_terminal_hook_workspace_cleanup_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        deadline: String,
        updated_at: String,
        task_id: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE workspace SET cleanup_after=COALESCE(cleanup_after,?),updated_at=? WHERE task_id=? AND status!='cleaned'")
            .bind(deadline)
            .bind(updated_at)
            .bind(task_id)
            .execute(connection).await?)
    }
}
