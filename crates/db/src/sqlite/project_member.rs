use super::*;
use crate::{CreateProjectMember, ProjectMember, ProjectMemberRepo};

#[async_trait]
impl ProjectMemberRepo for SqliteDb {
    async fn add_member(&self, input: CreateProjectMember) -> Result<ProjectMember> {
        sqlx::query(
            "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.user_id)
        .bind(&input.role)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if e.to_string().contains("UNIQUE") {
                DbError::Check("member already exists in project".into())
            } else {
                DbError::from(e)
            }
        })?;

        Ok(ProjectMember {
            id: input.id,
            project_id: input.project_id,
            user_id: input.user_id,
            role: input.role,
            created_at: input.created_at,
            updated_at: input.updated_at,
        })
    }

    async fn get_member(&self, project_id: &str, user_id: &str) -> Result<Option<ProjectMember>> {
        sqlx::query("SELECT * FROM project_member WHERE project_id = ? AND user_id = ?")
            .bind(project_id)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_member)
            .transpose()
    }

    async fn list_members(&self, project_id: &str) -> Result<Vec<ProjectMember>> {
        let rows = sqlx::query(
            "SELECT * FROM project_member WHERE project_id = ? ORDER BY created_at ASC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_project_member).collect()
    }

    async fn update_member_role(
        &self,
        project_id: &str,
        user_id: &str,
        role: &str,
        updated_at: &str,
    ) -> Result<ProjectMember> {
        let result = sqlx::query(
            "UPDATE project_member SET role = ?, updated_at = ? \
             WHERE project_id = ? AND user_id = ? \
             RETURNING *",
        )
        .bind(role)
        .bind(updated_at)
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        match result {
            Some(row) => map_project_member(row),
            None => Err(DbError::NotFound),
        }
    }

    async fn remove_member(&self, project_id: &str, user_id: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM project_member WHERE project_id = ? AND user_id = ?")
            .bind(project_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        Ok(())
    }
}

fn map_project_member(row: SqliteRow) -> Result<ProjectMember> {
    Ok(ProjectMember {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        user_id: row.try_get("user_id")?,
        role: row.try_get("role")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub async fn escalation_project_member_exists_on_connection(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
    user_id: &str,
) -> crate::Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM project_member WHERE project_id=? AND user_id=?)",
    )
    .bind(project_id)
    .bind(user_id)
    .fetch_one(connection)
    .await?)
}

impl SqliteDb {
    pub async fn bootstrap_project_member_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        user_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_member WHERE project_id = ? AND user_id = ?",
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_one(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn bootstrap_project_owner_member_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        id: String,
        project_id: &str,
        user_id: &str,
        created_at: &str,
        updated_at: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at) \
                     VALUES (?, ?, ?, 'owner', ?, ?)",
        )
        .bind(id)
        .bind(project_id)
        .bind(user_id)
        .bind(created_at)
        .bind(updated_at)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn task_proposal_project_member(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT 1 FROM project_member
                     WHERE project_id = ? AND user_id = ? LIMIT 1",
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn execution_setup_project_admin_member(
        &self,
        project_id: &str,
        user_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM project_member
                 WHERE project_id = ? AND user_id = ? AND role IN ('owner', 'admin')
             )",
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_one(self.pool())
        .await?)
    }
}
