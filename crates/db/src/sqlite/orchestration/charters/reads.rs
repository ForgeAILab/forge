use super::*;

impl SqliteDb {
    pub async fn current_project_operating_skill_revision_id(
        &self,
        skill_key: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
          AND revision.skill_key = skill.skill_key
         WHERE skill.skill_key = ?
           AND skill.lifecycle = 'active'
           AND skill.current_revision_id IS NOT NULL
         LIMIT 1",
        )
        .bind(skill_key)
        .fetch_optional(self.pool())
        .await?)
    }

    pub async fn latest_project_charter_approval_id(
        &self,
        charter_id: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_charter_approval
         WHERE charter_id = ? ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(charter_id)
        .fetch_optional(self.pool())
        .await?)
    }

    pub async fn latest_project_charter_id(
        &self,
        project_id: &str,
        account_id: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_charter
             WHERE project_id = ? AND account_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .bind(account_id)
        .fetch_optional(self.pool())
        .await?)
    }

    pub async fn project_charter_genesis_session_id(
        &self,
        charter_id: &str,
    ) -> Result<Option<Option<String>>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT genesis_session_id FROM project_charter WHERE id = ?",
        )
        .bind(charter_id)
        .fetch_optional(self.pool())
        .await?)
    }

    pub async fn project_charter_approval_id_by_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_charter_approval WHERE idempotency_key = ? LIMIT 1",
        )
        .bind(idempotency_key)
        .fetch_optional(self.pool())
        .await?)
    }
}
