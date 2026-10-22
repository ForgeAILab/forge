use super::*;
pub struct DeliveryFollowupMilestoneRow {
    row: SqliteRow,
}
impl DeliveryFollowupMilestoneRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn milestone_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_key")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn definition_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("definition_revision_id")?)
    }
    pub fn task_selection_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("task_selection_json")?)
    }
}

impl SqliteDb {
    pub async fn delivery_followup_milestone_sources(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<DeliveryFollowupMilestoneRow>> {
        let rows = sqlx::query(
            "SELECT m.id, m.milestone_key, m.version,
                    m.current_definition_revision_id AS definition_revision_id,
                    r.task_selection_json
             FROM project_milestone m
             JOIN project_milestone_revision r
               ON r.id = m.current_definition_revision_id AND r.milestone_id = m.id
             WHERE m.project_id = ?
               AND m.lifecycle IN ('planned', 'active', 'ready_for_release')
             ORDER BY m.milestone_sequence ASC, m.id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| DeliveryFollowupMilestoneRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn delivery_followup_open_task_count(
        &self,
        project_id: &str,
        milestone_id: &str,
        task_selection_json: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task t
                 WHERE t.project_id = ?
                   AND t.deleted_at IS NULL AND t.archived_at IS NULL
                   AND t.status NOT IN ('done', 'cancelled')
                   AND (
                        t.id IN (SELECT g.task_id FROM project_task_governance g
                                 WHERE g.milestone_id = ? AND g.project_id = ?)
                     OR t.id IN (SELECT value FROM json_each(?))
                   )",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(project_id)
        .bind(task_selection_json)
        .fetch_optional(self.pool())
        .await?)
    }
}

pub struct DeliveryFollowupCheckRow {
    row: SqliteRow,
}
impl DeliveryFollowupCheckRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn source_kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_kind")?)
    }
    pub fn outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("outcome")?)
    }
}

impl SqliteDb {
    pub async fn delivery_followup_outstanding_check_sources(
        &self,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
    ) -> crate::Result<Vec<DeliveryFollowupCheckRow>> {
        let rows = sqlx::query(
            "SELECT c.id, c.source_kind, COALESCE(r.outcome, 'missing') AS outcome
                 FROM project_milestone_check c
                 LEFT JOIN project_milestone_check_result r
                   ON r.id = c.current_result_id
                  AND r.definition_revision_id = c.definition_revision_id
                 WHERE c.project_id = ? AND c.milestone_id = ?
                   AND c.definition_revision_id = ? AND c.required = 1
                   AND COALESCE(r.outcome, 'missing') NOT IN ('passed', 'waived')
                 ORDER BY c.check_key ASC
                 LIMIT 64",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(definition_revision_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| DeliveryFollowupCheckRow { row })
            .collect())
    }
}
