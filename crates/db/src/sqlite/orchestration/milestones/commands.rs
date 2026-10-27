use super::*;

pub struct MaterializeMilestoneCheckInput<'a> {
    pub project_id: &'a str,
    pub milestone_id: &'a str,
    pub definition_revision_id: &'a str,
    pub check_id: &'a str,
    pub description: &'a str,
    pub required: bool,
    pub source_kind: &'a str,
    pub expected_result: &'a str,
    pub evidence_required: bool,
    pub updated_at: &'a str,
}

pub struct TransitionMilestoneLifecycleInput<'a> {
    pub project_id: &'a str,
    pub milestone_id: &'a str,
    pub expected_version: i64,
    pub from_lifecycle: &'a str,
    pub lifecycle: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn transition_milestone_lifecycle_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: TransitionMilestoneLifecycleInput<'_>,
    ) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE project_milestone SET lifecycle = ?, version = version + 1, updated_at = ?
         WHERE id = ? AND project_id = ? AND version = ? AND lifecycle = ?",
        )
        .bind(input.lifecycle)
        .bind(input.updated_at)
        .bind(input.milestone_id)
        .bind(input.project_id)
        .bind(input.expected_version)
        .bind(input.from_lifecycle)
        .execute(&mut *connection)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        self.repair_milestone_primary_pointer_in_tx(
            connection,
            input.project_id,
            input.milestone_id,
        )
        .await
    }

    pub async fn repair_milestone_primary_pointer_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        changed_milestone_id: &str,
    ) -> Result<()> {
        let primary = self
            .primary_milestone_id_in_tx(connection, project_id)
            .await?;
        if primary.as_deref() != Some(changed_milestone_id) {
            return Ok(());
        }
        let lifecycle = self
            .milestone_lifecycle_in_tx(connection, changed_milestone_id, project_id)
            .await?;
        if matches!(
            lifecycle.as_str(),
            "planned" | "active" | "ready_for_release" | "released"
        ) {
            return Ok(());
        }
        let replacement = self
            .replacement_active_milestone_id_in_tx(connection, project_id, changed_milestone_id)
            .await?;
        self.repair_primary_milestone_in_tx(
            connection,
            replacement,
            &now_rfc3339(),
            project_id,
            changed_milestone_id,
        )
        .await?;
        Ok(())
    }

    pub async fn transition_milestone_definition_lifecycle_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        milestone_id: &str,
        from_lifecycle: &str,
        lifecycle: &str,
    ) -> Result<u64> {
        let updated = sqlx::query(
            "UPDATE project_milestone_revision SET lifecycle = ?
         WHERE id = ? AND milestone_id = ? AND lifecycle = ?",
        )
        .bind(lifecycle)
        .bind(revision_id)
        .bind(milestone_id)
        .bind(from_lifecycle)
        .execute(connection)
        .await?;
        Ok(updated.rows_affected())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn advance_milestone_definition_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
        revision_id: &str,
        expected_version: i64,
        lifecycle: &str,
        updated_at: &str,
    ) -> Result<u64> {
        if lifecycle == "approved" {
            sqlx::query(
                "UPDATE project_milestone_revision
             SET lifecycle = 'superseded'
             WHERE milestone_id = ? AND id != ? AND lifecycle = 'approved'",
            )
            .bind(milestone_id)
            .bind(revision_id)
            .execute(&mut *connection)
            .await?;
        }
        let advanced = if matches!(lifecycle, "proposed" | "approved") {
            sqlx::query(
                "UPDATE project_milestone
             SET current_definition_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(revision_id)
            .bind(updated_at)
            .bind(milestone_id)
            .bind(project_id)
            .bind(expected_version)
            .execute(&mut *connection)
            .await?
            .rows_affected()
        } else {
            sqlx::query(
                "UPDATE project_milestone SET version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
            )
            .bind(updated_at)
            .bind(milestone_id)
            .bind(project_id)
            .bind(expected_version)
            .execute(&mut *connection)
            .await?
            .rows_affected()
        };
        Ok(advanced)
    }

    pub async fn materialize_milestone_definition_check_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: MaterializeMilestoneCheckInput<'_>,
    ) -> Result<()> {
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project_id, milestone_id, definition_revision_id
             FROM project_milestone_check WHERE id = ?",
        )
        .bind(input.check_id)
        .fetch_optional(&mut *connection)
        .await?;
        if let Some((existing_project, existing_milestone, existing_revision)) = existing {
            if existing_project != input.project_id || existing_milestone != input.milestone_id {
                return Err(DbError::VersionConflict);
            }
            if existing_revision == input.definition_revision_id {
                return Ok(());
            }
            sqlx::query(
                "UPDATE project_milestone_check
                 SET definition_revision_id = ?, check_key = ?, description = ?, required = ?,
                     source_kind = ?, expected_result = ?, evidence_required = ?,
                     version = version + 1, current_result_id = NULL, updated_at = ?
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
            )
            .bind(input.definition_revision_id)
            .bind(input.check_id)
            .bind(input.description)
            .bind(input.required)
            .bind(input.source_kind)
            .bind(input.expected_result)
            .bind(input.evidence_required)
            .bind(input.updated_at)
            .bind(input.check_id)
            .bind(input.project_id)
            .bind(input.milestone_id)
            .execute(connection)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO project_milestone_check (
                    id, project_id, milestone_id, definition_revision_id, check_key,
                    description, required, source_kind, expected_result,
                    evidence_required, version, current_result_id, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
            )
            .bind(input.check_id)
            .bind(input.project_id)
            .bind(input.milestone_id)
            .bind(input.definition_revision_id)
            .bind(input.check_id)
            .bind(input.description)
            .bind(input.required)
            .bind(input.source_kind)
            .bind(input.expected_result)
            .bind(input.evidence_required)
            .bind(input.updated_at)
            .bind(input.updated_at)
            .execute(connection)
            .await?;
        }
        Ok(())
    }
}
