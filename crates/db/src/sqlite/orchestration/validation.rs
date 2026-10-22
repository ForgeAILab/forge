use super::*;
/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ValidationCommandObservationSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ValidationCommandObservationSourceRow {
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn args_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("args_json")?)
    }
    pub fn program(&self) -> crate::Result<String> {
        Ok(self.row.try_get("program")?)
    }
    pub fn exit_code(&self) -> crate::Result<Option<i64>> {
        Ok(self.row.try_get("exit_code")?)
    }
    pub fn success(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("success")?)
    }
    pub fn output_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("output_digest")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ValidationAcceptanceCheckSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ValidationAcceptanceCheckSourceRow {
    pub fn source_kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_kind")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
}

impl SqliteDb {
    pub async fn validation_acceptance_check_source(
        &self,
        check_id: &str,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
    ) -> crate::Result<Option<ValidationAcceptanceCheckSourceRow>> {
        let rows = sqlx::query(
            "SELECT version, source_kind FROM project_milestone_check
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND definition_revision_id = ?",
        )
        .bind(check_id)
        .bind(project_id)
        .bind(milestone_id)
        .bind(definition_revision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ValidationAcceptanceCheckSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn validation_latest_delivered_task_at(
        &self,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT MAX(t.updated_at) FROM project_task_governance g
             JOIN task t ON t.id = g.task_id
             WHERE g.milestone_id = ? AND t.project_id = ? AND t.status = 'done'
               AND t.deleted_at IS NULL",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn validation_milestone_definition_created_at(
        &self,
        definition_revision_id: &str,
    ) -> crate::Result<String> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT created_at FROM project_milestone_revision WHERE id = ?",
        )
        .bind(definition_revision_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn validation_command_observation_source(
        &self,
        observation_id: &str,
        project_id: &str,
        actor_identity_id: &str,
    ) -> crate::Result<Option<ValidationCommandObservationSourceRow>> {
        let rows = sqlx::query(
            "SELECT program, args_json, exit_code, success, output_digest, created_at
                 FROM project_command_observation
                 WHERE id = ? AND project_id = ? AND actor_identity_id = ?",
        )
        .bind(observation_id)
        .bind(project_id)
        .bind(actor_identity_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ValidationCommandObservationSourceRow { row }))
    }
}
