use super::*;

fn map_external_link(row: &SqliteRow) -> Result<TaskExternalLink> {
    Ok(TaskExternalLink {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        integration_id: row.try_get("integration_id")?,
        platform: row.try_get("platform")?,
        remote_owner: row.try_get("remote_owner")?,
        remote_repo: row.try_get("remote_repo")?,
        remote_issue_number: row.try_get("remote_issue_number")?,
        remote_url: row.try_get("remote_url")?,
        global_id: row.try_get("global_id")?,
        synced_at: row.try_get("synced_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[async_trait]
impl ExternalLinkRepo for SqliteDb {
    async fn create_link(&self, input: CreateTaskExternalLink) -> Result<TaskExternalLink> {
        sqlx::query(
            "INSERT INTO task_external_link (id, task_id, integration_id, platform, remote_owner, remote_repo, remote_issue_number, remote_url, global_id, synced_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(&input.integration_id)
        .bind(&input.platform)
        .bind(&input.remote_owner)
        .bind(&input.remote_repo)
        .bind(input.remote_issue_number)
        .bind(&input.remote_url)
        .bind(&input.global_id)
        .bind(&input.synced_at)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(check_error)?;

        ExternalLinkRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<TaskExternalLink>> {
        sqlx::query("SELECT * FROM task_external_link WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_external_link(&row))
            .transpose()
    }

    async fn get_by_global_id(&self, global_id: &str) -> Result<Option<TaskExternalLink>> {
        sqlx::query("SELECT * FROM task_external_link WHERE global_id = ?")
            .bind(global_id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| map_external_link(&row))
            .transpose()
    }

    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<TaskExternalLink>> {
        sqlx::query(
            "SELECT * FROM task_external_link WHERE task_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?
            .map(|row| map_external_link(&row))
            .transpose()
    }

    async fn list_by_task_id(&self, task_id: &str) -> Result<Vec<TaskExternalLink>> {
        let rows = sqlx::query(
            "SELECT * FROM task_external_link WHERE task_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(map_external_link).collect()
    }

    async fn list_latest_links_for_tasks(
        &self,
        task_ids: &[&str],
    ) -> Result<Vec<TaskExternalLink>> {
        let mut connection = self.pool.acquire().await?;
        latest_links(&mut connection, task_ids).await
    }

    async fn list_by_integration(&self, integration_id: &str) -> Result<Vec<TaskExternalLink>> {
        let rows = sqlx::query(
            "SELECT * FROM task_external_link WHERE integration_id = ? ORDER BY created_at DESC, id DESC",
        )
        .bind(integration_id)
        .fetch_all(&self.pool)
        .await?;

        rows.iter().map(map_external_link).collect()
    }

    async fn delete_link(&self, id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM task_external_link WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }
}

pub(super) async fn latest_links(
    connection: &mut sqlx::SqliteConnection,
    task_ids: &[&str],
) -> Result<Vec<TaskExternalLink>> {
    if task_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT * FROM (
                 SELECT task_external_link.*, ROW_NUMBER() OVER (
                     PARTITION BY task_id ORDER BY created_at DESC, id DESC
                 ) AS rn FROM task_external_link WHERE task_id IN (",
    );
    let mut ids = query.separated(", ");
    for task_id in task_ids {
        ids.push_bind(*task_id);
    }
    ids.push_unseparated(") ) ranked WHERE rn = 1 ORDER BY task_id");
    let rows = query.build().fetch_all(&mut *connection).await?;
    rows.iter().map(map_external_link).collect()
}
