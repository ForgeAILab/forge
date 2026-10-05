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
        let result = sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        transaction.commit().await?;
        Ok(())
    }
}
