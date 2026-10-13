use super::*;
use crate::now_rfc3339;

#[async_trait]
impl RepoRepo for SqliteDb {
    async fn create(&self, input: CreateRepo) -> Result<Repo> {
        let remote_url = normalize_repo_remote_url(input.remote_url);
        sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.name)
            .bind(&remote_url)
            .bind(&input.local_path)
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
        expected_project_version: i64,
        project_updated_at: String,
    ) -> Result<Repo> {
        let remote_url = normalize_repo_remote_url(input.remote_url);
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

        sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.project_id)
            .bind(&input.name)
            .bind(&remote_url)
            .bind(&input.local_path)
            .bind(&input.default_branch)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;

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

        wake_dispatch_for_project_in_tx(
            self,
            &mut transaction,
            &input.project_id,
            &project_updated_at,
        )
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
            repo.remote_url = normalize_repo_remote_url(remote_url);
        }
        if let Some(default_branch) = input.default_branch {
            repo.default_branch = default_branch;
        }
        repo.updated_at = input.updated_at;
        sqlx::query(
            "UPDATE repo SET name = ?, remote_url = ?, local_path = ?, default_branch = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&repo.name)
        .bind(&repo.remote_url)
        .bind(&repo.local_path)
        .bind(&repo.default_branch)
        .bind(&repo.updated_at)
        .bind(&repo.id)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        wake_dispatch_for_project_in_tx(self, &mut transaction, &repo.project_id, &repo.updated_at)
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
        crate::integration_queue::refuse_unresolved_integration(
            &mut transaction,
            crate::integration_queue::IntegrationGuardScope::Repo(id),
        )
        .await?;
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
        sqlx::query("DELETE FROM pr_provider_config WHERE repo_id = ?")
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
        wake_dispatch_for_project_in_tx(self, &mut transaction, &project_id, &project_updated_at)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl SqliteDb {
    pub async fn workspace_cleanup_repo_ids_after(
        &self,
        after: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        let result = sqlx::query_scalar::<_, String>(
            "SELECT id FROM repo WHERE id > ?
             UNION SELECT repo_id AS id FROM workspace WHERE repo_id > ?
             ORDER BY id LIMIT ?",
        )
        .bind(after)
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneRepositoryContextRow {
    pub repo_id: String,
    pub repository_name: String,
    pub repository_kind: String,
    pub remote_url: Option<String>,
    pub default_branch: String,
    pub execution_id: Option<String>,
    pub execution_status: Option<String>,
    pub execution_role: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub execution_summary: Option<String>,
    pub execution_created_at: Option<String>,
    pub execution_updated_at: Option<String>,
    pub review_id: Option<String>,
    pub review_status: Option<String>,
    pub ci_results: Option<String>,
    pub audit_result: Option<String>,
    pub human_decision: Option<String>,
    pub review_created_at: Option<String>,
    pub review_updated_at: Option<String>,
}

impl SqliteDb {
    pub async fn milestone_repository_context_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        task_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneRepositoryContextRow>> {
        let row = sqlx::query("SELECT repo.id AS repo_id, repo.name AS repository_name, repo.work_mode AS repository_kind,
                        CASE WHEN trim(repo.remote_url) = '' THEN NULL ELSE repo.remote_url END AS remote_url,
                        repo.default_branch,
                        e.id AS execution_id, e.status AS execution_status, e.role AS execution_role,
                        e.before_sha, e.after_sha, e.summary AS execution_summary,
                        e.created_at AS execution_created_at, e.updated_at AS execution_updated_at,
                        r.id AS review_id, r.status AS review_status,
                        r.step_results_json AS ci_results, NULL AS audit_result,
                        NULL AS human_decision,
                        r.created_at AS review_created_at, r.updated_at AS review_updated_at
                 FROM task t
                 JOIN project p ON p.id = t.project_id
                 LEFT JOIN execution e ON e.id = (
                     SELECT e2.id FROM execution e2
                     WHERE e2.task_id = t.id
                     ORDER BY e2.updated_at DESC, e2.id DESC LIMIT 1
                 )
                 LEFT JOIN workspace w ON w.id = e.workspace_id
                 JOIN repo ON repo.id = CASE
                     WHEN e.id IS NULL THEN p.primary_repo_id
                     ELSE w.repo_id
                 END AND repo.project_id = t.project_id
                 LEFT JOIN review r ON r.id = (
                     SELECT r2.id FROM review r2
                     WHERE r2.execution_id = e.id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1
                 )
                 WHERE t.id = ? AND t.project_id = ?
                 LIMIT 1")
.bind(task_id)
.bind(project_id)
.fetch_optional(connection).await?;
        row.map(|row| {
            Ok(MilestoneRepositoryContextRow {
                repo_id: row.try_get("repo_id")?,
                repository_name: row.try_get("repository_name")?,
                repository_kind: row.try_get("repository_kind")?,
                remote_url: row.try_get("remote_url")?,
                default_branch: row.try_get("default_branch")?,
                execution_id: row.try_get("execution_id")?,
                execution_status: row.try_get("execution_status")?,
                execution_role: row.try_get("execution_role")?,
                before_sha: row.try_get("before_sha")?,
                after_sha: row.try_get("after_sha")?,
                execution_summary: row.try_get("execution_summary")?,
                execution_created_at: row.try_get("execution_created_at")?,
                execution_updated_at: row.try_get("execution_updated_at")?,
                review_id: row.try_get("review_id")?,
                review_status: row.try_get("review_status")?,
                ci_results: row.try_get("ci_results")?,
                audit_result: row.try_get("audit_result")?,
                human_decision: row.try_get("human_decision")?,
                review_created_at: row.try_get("review_created_at")?,
                review_updated_at: row.try_get("review_updated_at")?,
            })
        })
        .transpose()
    }
}
