use super::*;
/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct ProjectAdoptionCharterSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl ProjectAdoptionCharterSourceRow {
    pub fn charter_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_id")?)
    }
    pub fn charter_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("charter_version")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn rendered_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
    pub fn content_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_json")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectGoverningCharterSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectGoverningCharterSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("revision")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn render_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn rendered_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectValidationOutcomeCountRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectValidationOutcomeCountRow {
    pub fn key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("key")?)
    }
    pub fn count(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("count")?)
    }
}

impl SqliteDb {
    pub async fn project_adoption_charter_source(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<ProjectAdoptionCharterSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS charter_id, c.version AS charter_version,
                r.id AS revision_id, r.revision, r.content_digest,
                r.rendered_digest, r.content_json
         FROM project AS p
         JOIN project_charter AS c ON c.project_id = p.id
         JOIN project_charter_revision AS r
           ON r.id = c.current_draft_revision_id
          AND r.charter_id = c.id
         WHERE p.id = ?
           AND p.charter_status = 'legacy_unverified'
           AND p.charter_setup_required = 1
           AND c.current_approved_revision_id IS NULL
           AND r.lifecycle IN ('draft', 'proposed')
         ORDER BY c.updated_at DESC, c.id DESC
         LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ProjectAdoptionCharterSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn effective_project_governing_charter_source(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<EffectiveProjectGoverningCharterSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id, c.version, r.id AS revision_id, r.revision,
                r.content_digest, r.render_version, r.rendered_digest
         FROM project_charter c
         JOIN project_charter_revision r
           ON r.id = c.current_approved_revision_id
          AND r.charter_id = c.id
         WHERE c.project_id = ?
           AND c.lifecycle = 'attached'
           AND r.lifecycle = 'approved'
         ORDER BY c.updated_at DESC, c.id DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| EffectiveProjectGoverningCharterSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn effective_project_validation_outcome_counts(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<EffectiveProjectValidationOutcomeCountRow>> {
        let rows = sqlx::query(
            "SELECT COALESCE(r.outcome, 'missing') AS key, COUNT(*) AS count
         FROM project_milestone_check c
         JOIN project_milestone m
           ON m.id = c.milestone_id AND m.project_id = c.project_id
          AND m.current_definition_revision_id = c.definition_revision_id
         LEFT JOIN project_milestone_check_result r
           ON r.id = c.current_result_id
          AND r.definition_revision_id = c.definition_revision_id
         WHERE c.project_id = ?
         GROUP BY COALESCE(r.outcome, 'missing') ORDER BY key ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectValidationOutcomeCountRow { row })
            .collect())
    }
}
