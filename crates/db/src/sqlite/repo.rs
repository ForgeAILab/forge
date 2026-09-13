use super::*;
use crate::now_rfc3339;

#[async_trait]
impl RepoRepo for SqliteDb {
    async fn create(&self, input: CreateRepo) -> Result<Repo> {
        sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.name)
            .bind(&input.remote_url)
            .bind(&input.local_path)
            .bind(input.work_mode.to_string())
            .bind(&input.default_branch)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&self.pool)
            .await
            .map_err(check_error)?;
        RepoRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_primary_for_project(
        &self,
        input: CreateRepo,
        provider_config: Option<CreatePrProviderConfig>,
        expected_project_version: i64,
        project_updated_at: String,
    ) -> Result<Repo> {
        if provider_config
            .as_ref()
            .is_some_and(|config| config.repo_id != input.id)
        {
            return Err(DbError::Check(
                "PR provider configuration must belong to the created repository".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let project_row = sqlx::query("SELECT version, primary_repo_id FROM project WHERE id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let project_version: i64 = project_row.try_get("version")?;
        let existing_primary_repo_id: Option<String> = project_row.try_get("primary_repo_id")?;
        if project_version != expected_project_version || existing_primary_repo_id.is_some() {
            return Err(DbError::VersionConflict);
        }

        sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.name)
            .bind(&input.remote_url)
            .bind(&input.local_path)
            .bind(input.work_mode.to_string())
            .bind(&input.default_branch)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;

        if let Some(config) = provider_config {
            sqlx::query(
                "INSERT INTO pr_provider_config (id, repo_id, provider_type, base_url, polling_interval_seconds, token_secret_ref, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&config.id)
            .bind(&config.repo_id)
            .bind(&config.provider_type)
            .bind(&config.base_url)
            .bind(config.polling_interval_seconds)
            .bind(&config.token_secret_ref)
            .bind(&config.created_at)
            .bind(&config.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
        }

        let result = sqlx::query(
            "UPDATE project
             SET primary_repo_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND primary_repo_id IS NULL",
        )
        .bind(&input.id)
        .bind(&project_updated_at)
        .bind(&input.project_id)
        .bind(expected_project_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        wake_dispatch_for_project_in_tx(&mut transaction, &input.project_id, &project_updated_at)
            .await?;
        let repo_row = sqlx::query("SELECT * FROM repo WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *transaction)
            .await?;
        let repo = map_repo(repo_row)?;
        transaction.commit().await?;
        Ok(repo)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Repo>> {
        sqlx::query("SELECT * FROM repo WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_repo)
            .transpose()
    }

    async fn list_by_project(&self, project_id: &str, page: PageRequest) -> Result<Page<Repo>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM repo WHERE project_id = ? ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(project_id)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows.into_iter().map(map_repo).collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            Some(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM repo WHERE project_id = ?")
                    .bind(project_id)
                    .fetch_one(&self.pool)
                    .await?,
            )
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn update(&self, input: UpdateRepo) -> Result<Repo> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let repo_row = sqlx::query("SELECT * FROM repo WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let mut repo = map_repo(repo_row)?;
        if let Some(name) = input.name {
            repo.name = name;
        }
        if let Some(local_path) = input.local_path {
            repo.local_path = local_path;
        }
        if let Some(remote_url) = input.remote_url {
            repo.remote_url = remote_url;
        }
        if let Some(work_mode) = input.work_mode {
            repo.work_mode = work_mode;
        }
        if let Some(default_branch) = input.default_branch {
            repo.default_branch = default_branch;
        }
        repo.updated_at = input.updated_at;
        sqlx::query(
            "UPDATE repo SET name = ?, remote_url = ?, local_path = ?, work_mode = ?, default_branch = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&repo.name)
        .bind(&repo.remote_url)
        .bind(&repo.local_path)
        .bind(repo.work_mode.to_string())
        .bind(&repo.default_branch)
        .bind(&repo.updated_at)
        .bind(&repo.id)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        wake_dispatch_for_project_in_tx(&mut transaction, &repo.project_id, &repo.updated_at)
            .await?;
        transaction.commit().await?;
        Ok(repo)
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let project_id: String = sqlx::query_scalar("SELECT project_id FROM repo WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let in_use: bool = sqlx::query_scalar(
            "SELECT
                EXISTS(
                    SELECT 1
                    FROM execution e
                    JOIN workspace w ON w.id = e.workspace_id
                    WHERE w.repo_id = ? AND e.status = 'running'
                )
                OR EXISTS(
                    SELECT 1
                    FROM workspace_lease wl
                    WHERE wl.repository_binding_id = ? AND wl.status = 'active'
                )",
        )
        .bind(id)
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
        if in_use {
            return Err(DbError::RepoInUse {
                repo_id: id.to_owned(),
            });
        }
        let project_updated_at = now_rfc3339();
        sqlx::query(
            "UPDATE project
             SET primary_repo_id = NULL, version = version + 1, updated_at = ?
             WHERE primary_repo_id = ?",
        )
        .bind(&project_updated_at)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        let result = sqlx::query("DELETE FROM repo WHERE id = ?")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        wake_dispatch_for_project_in_tx(&mut transaction, &project_id, &project_updated_at).await?;
        transaction.commit().await?;
        Ok(())
    }
}
