use super::*;

pub struct ProjectDecisionCandidateRead {
    row: SqliteRow,
}
impl ProjectDecisionCandidateRead {
    pub fn into_candidate(self) -> Result<ProjectDecisionCandidateRecord> {
        map_decision_candidate(self.row)
    }
}

pub struct ProjectDecisionRead {
    row: SqliteRow,
}
impl ProjectDecisionRead {
    pub fn into_decision(self) -> Result<ProjectDecisionRecord> {
        map_decision(self.row)
    }
}

impl SqliteDb {
    pub async fn project_decision_candidate_page(
        &self,
        project_id: &str,
        cursor: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectDecisionCandidateRead>> {
        let mut statement = String::from(
            "SELECT * FROM project_decision_candidate
         WHERE project_id = ?",
        );
        if cursor.is_some() {
            statement.push_str(" AND (created_at < ? OR (created_at = ? AND id < ?))");
        }
        statement.push_str(" ORDER BY created_at DESC, id DESC LIMIT ?");
        let mut candidate_query = sqlx::query(&statement).bind(project_id);
        if let Some((created_at, id)) = cursor {
            candidate_query = candidate_query
                .bind(created_at.clone())
                .bind(created_at)
                .bind(id);
        }
        let rows = candidate_query
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectDecisionCandidateRead { row })
            .collect())
    }
    pub async fn project_decision_page(
        &self,
        project_id: &str,
        cursor: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectDecisionRead>> {
        let mut statement = String::from(
            "SELECT * FROM project_decision
         WHERE project_id = ?",
        );
        if cursor.is_some() {
            statement.push_str(" AND (created_at < ? OR (created_at = ? AND id < ?))");
        }
        statement.push_str(" ORDER BY created_at DESC, id DESC LIMIT ?");
        let mut decision_query = sqlx::query(&statement).bind(project_id);
        if let Some((created_at, id)) = cursor {
            decision_query = decision_query
                .bind(created_at.clone())
                .bind(created_at)
                .bind(id);
        }
        let rows = decision_query
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectDecisionRead { row })
            .collect())
    }
    pub async fn project_decision_replacement_state(
        &self,
        project_id: &str,
        decision_id: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT state FROM project_decision
             WHERE project_id = ? AND supersedes_decision_id = ?
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .bind(decision_id)
        .fetch_optional(self.pool())
        .await?)
    }
    pub async fn project_decision_detail_source(
        &self,
        project_id: &str,
        decision_id: &str,
    ) -> Result<Option<(ProjectDecisionRecord, Option<String>)>> {
        let row = sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ?")
            .bind(decision_id)
            .bind(project_id)
            .fetch_optional(self.pool())
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = map_decision(row)?;
        let replacement_state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM project_decision
         WHERE project_id = ? AND supersedes_decision_id = ?
         ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .bind(decision_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(Some((record, replacement_state)))
    }
}
