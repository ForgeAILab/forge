use super::*;

pub struct ProjectMilestoneIdRead {
    row: SqliteRow,
}

impl ProjectMilestoneIdRead {
    pub fn id(&self) -> Result<String> {
        Ok(self.row.try_get("id")?)
    }
}

pub struct MilestoneDefinitionTransitionRead {
    row: SqliteRow,
}

impl MilestoneDefinitionTransitionRead {
    pub fn lifecycle(&self) -> Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }

    pub fn acceptance_checks_json(&self) -> Result<String> {
        Ok(self.row.try_get("acceptance_checks_json")?)
    }

    pub fn evidence_requirements_json(&self) -> Result<String> {
        Ok(self.row.try_get("evidence_requirements_json")?)
    }
}

impl SqliteDb {
    pub async fn project_milestone_id_page(
        &self,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectMilestoneIdRead>> {
        let rows = if let Some((sequence, id)) = cursor.as_ref() {
            sqlx::query(
                "SELECT id
             FROM project_milestone
             WHERE project_id = ?
               AND (milestone_sequence > ? OR (milestone_sequence = ? AND id > ?))
             ORDER BY milestone_sequence ASC, id ASC LIMIT ?",
            )
            .bind(project_id)
            .bind(sequence)
            .bind(sequence)
            .bind(id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT id FROM project_milestone
             WHERE project_id = ?
             ORDER BY milestone_sequence ASC, id ASC LIMIT ?",
            )
            .bind(project_id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|row| ProjectMilestoneIdRead { row })
            .collect())
    }

    pub async fn project_milestone_revision_id_page(
        &self,
        project_id: &str,
        milestone_id: &str,
        cursor: Option<(i64, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectMilestoneIdRead>> {
        let rows = if let Some((revision, id)) = cursor.as_ref() {
            sqlx::query(
                "SELECT r.id
             FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.milestone_id = ? AND m.project_id = ?
               AND (r.revision > ? OR (r.revision = ? AND r.id > ?))
             ORDER BY r.revision ASC, r.id ASC LIMIT ?",
            )
            .bind(milestone_id)
            .bind(project_id)
            .bind(revision)
            .bind(revision)
            .bind(id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT r.id
             FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.milestone_id = ? AND m.project_id = ?
             ORDER BY r.revision ASC, r.id ASC LIMIT ?",
            )
            .bind(milestone_id)
            .bind(project_id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|row| ProjectMilestoneIdRead { row })
            .collect())
    }

    pub async fn milestone_definition_transition_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        milestone_id: &str,
        project_id: &str,
    ) -> Result<Option<MilestoneDefinitionTransitionRead>> {
        let row = sqlx::query(
            "SELECT r.* FROM project_milestone_revision r
         JOIN project_milestone m ON m.id = r.milestone_id
         WHERE r.id = ? AND r.milestone_id = ? AND m.project_id = ?",
        )
        .bind(revision_id)
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        Ok(row.map(|row| MilestoneDefinitionTransitionRead { row }))
    }
}
