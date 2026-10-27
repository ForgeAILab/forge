use super::*;

impl SqliteDb {
    pub async fn milestone_manual_result_replay(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<MilestoneManualResultRead>> {
        let row =
            sqlx::query("SELECT * FROM project_milestone_check_result WHERE idempotency_key = ?")
                .bind(idempotency_key)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|row| MilestoneManualResultRead { row }))
    }

    pub async fn milestone_manual_check_target_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        check_id: &str,
        project_id: &str,
        milestone_id: &str,
    ) -> Result<Option<MilestoneCheckTargetRead>> {
        let row = sqlx::query(
            "SELECT c.version, c.definition_revision_id, c.source_kind,
                m.current_definition_revision_id, r.author_id
         FROM project_milestone_check c
         JOIN project_milestone m ON m.id = c.milestone_id AND m.project_id = c.project_id
         LEFT JOIN project_milestone_revision r ON r.id = c.definition_revision_id
         WHERE c.id = ? AND c.project_id = ? AND c.milestone_id = ?",
        )
        .bind(check_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(connection)
        .await?;
        Ok(row.map(|row| MilestoneCheckTargetRead { row }))
    }

    pub async fn milestone_waiver_check_target_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        check_id: &str,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
        expected_version: i64,
    ) -> Result<Option<MilestoneCheckTargetRead>> {
        let row = sqlx::query(
            "SELECT c.version, m.current_definition_revision_id, r.author_id
         FROM project_milestone_check c
         JOIN project_milestone m
           ON m.id = c.milestone_id AND m.project_id = c.project_id
         JOIN project_milestone_revision r ON r.id = c.definition_revision_id
         WHERE c.id = ? AND c.project_id = ? AND c.milestone_id = ?
           AND c.definition_revision_id = ? AND c.version = ?",
        )
        .bind(check_id)
        .bind(project_id)
        .bind(milestone_id)
        .bind(definition_revision_id)
        .bind(expected_version)
        .fetch_optional(connection)
        .await?;
        Ok(row.map(|row| MilestoneCheckTargetRead { row }))
    }

    pub async fn milestone_check_charter_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> Result<Option<Option<String>>> {
        let row = sqlx::query(
            "SELECT p.current_charter_revision_id FROM project p WHERE p.id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| Ok(row.try_get("current_charter_revision_id")?))
            .transpose()
    }

    pub async fn record_manual_milestone_result_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        result: &CreateProjectMilestoneCheckResult,
    ) -> Result<()> {
        let inserted = sqlx::query(
            "INSERT INTO project_milestone_check_result (
            id, project_id, milestone_id, check_id, definition_revision_id,
            outcome, source_kind, source_manifest_json, input_digest,
            governing_charter_revision_id,
            principal_type, principal_id, authorization_basis, authorization_action,
            expected_version, explicit_event, authorization_occurred_at,
            idempotency_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, 'manual', ?, ?, ?, 'user', ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.project_id)
        .bind(&result.milestone_id)
        .bind(&result.check_id)
        .bind(&result.definition_revision_id)
        .bind(&result.outcome)
        .bind(&result.source_manifest_json)
        .bind(&result.input_digest)
        .bind(&result.governing_charter_revision_id)
        .bind(&result.principal_id)
        .bind(&result.authorization_basis)
        .bind(&result.authorization_action)
        .bind(result.expected_version)
        .bind(&result.explicit_event)
        .bind(&result.authorization_occurred_at)
        .bind(&result.idempotency_key)
        .bind(&result.created_at)
        .execute(&mut *connection)
        .await;
        if let Err(error) = inserted {
            if error.to_string().to_ascii_lowercase().contains("unique") {
                return Err(DbError::IdempotencyConflict);
            }
            return Err(error.into());
        }
        let updated = sqlx::query(
            "UPDATE project_milestone_check SET current_result_id = ?, version = version + 1,
         updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(&result.id)
        .bind(&result.created_at)
        .bind(&result.check_id)
        .bind(result.expected_version)
        .execute(connection)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }
}

pub struct MilestoneCheckTargetRead {
    row: SqliteRow,
}

impl MilestoneCheckTargetRead {
    pub fn version(&self) -> Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn definition_revision_id(&self) -> Result<String> {
        Ok(self.row.try_get("definition_revision_id")?)
    }
    pub fn source_kind(&self) -> Result<String> {
        Ok(self.row.try_get("source_kind")?)
    }
    pub fn current_definition_revision_id(&self) -> Result<Option<String>> {
        Ok(self.row.try_get("current_definition_revision_id")?)
    }
    pub fn author_id(&self) -> Result<Option<String>> {
        Ok(self.row.try_get("author_id")?)
    }
}

pub struct MilestoneManualResultRead {
    row: SqliteRow,
}

impl MilestoneManualResultRead {
    pub fn id(&self) -> Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn project_id(&self) -> Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn milestone_id(&self) -> Result<String> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn check_id(&self) -> Result<String> {
        Ok(self.row.try_get("check_id")?)
    }
    pub fn source_kind(&self) -> Result<String> {
        Ok(self.row.try_get("source_kind")?)
    }
    pub fn outcome(&self) -> Result<String> {
        Ok(self.row.try_get("outcome")?)
    }
    pub fn created_at(&self) -> Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn source_manifest_json(&self) -> Result<String> {
        Ok(self.row.try_get("source_manifest_json")?)
    }
    pub fn definition_revision_id(&self) -> Result<String> {
        Ok(self.row.try_get("definition_revision_id")?)
    }
    pub fn principal_type(&self) -> Result<String> {
        Ok(self.row.try_get("principal_type")?)
    }
    pub fn principal_id(&self) -> Result<String> {
        Ok(self.row.try_get("principal_id")?)
    }
    pub fn authorization_basis(&self) -> Result<String> {
        Ok(self.row.try_get("authorization_basis")?)
    }
    pub fn authorization_action(&self) -> Result<String> {
        Ok(self.row.try_get("authorization_action")?)
    }
    pub fn explicit_event(&self) -> Result<String> {
        Ok(self.row.try_get("explicit_event")?)
    }
    pub fn authorization_occurred_at(&self) -> Result<String> {
        Ok(self.row.try_get("authorization_occurred_at")?)
    }
    pub fn input_digest(&self) -> Result<String> {
        Ok(self.row.try_get("input_digest")?)
    }
    pub fn governing_charter_revision_id(&self) -> Result<Option<String>> {
        Ok(self.row.try_get("governing_charter_revision_id")?)
    }
    pub fn expected_version(&self) -> Result<i64> {
        Ok(self.row.try_get("expected_version")?)
    }
}
