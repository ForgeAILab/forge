use super::*;

pub struct ProjectDocumentIdRead {
    row: SqliteRow,
}
impl ProjectDocumentIdRead {
    pub fn id(&self) -> Result<String> {
        Ok(self.row.try_get("id")?)
    }
}

pub struct ProjectDocumentRevisionRead {
    row: SqliteRow,
}
impl ProjectDocumentRevisionRead {
    pub fn into_revision(self) -> Result<ProjectDocumentRevisionRecord> {
        map_document_revision(self.row)
    }
}

impl SqliteDb {
    pub async fn project_document_id_page(
        &self,
        project_id: &str,
        cursor: Option<(String, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectDocumentIdRead>> {
        let rows = if let Some((updated_at, id)) = cursor.as_ref() {
            sqlx::query(
                "SELECT id FROM project_document
             WHERE project_id = ?
               AND (updated_at < ? OR (updated_at = ? AND id < ?))
             ORDER BY updated_at DESC, id DESC LIMIT ?",
            )
            .bind(project_id)
            .bind(updated_at)
            .bind(updated_at)
            .bind(id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT id FROM project_document
             WHERE project_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT ?",
            )
            .bind(project_id)
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|row| ProjectDocumentIdRead { row })
            .collect())
    }
    pub async fn project_document_revision_page(
        &self,
        document_id: &str,
        cursor: Option<(i64, String)>,
        limit: i64,
    ) -> Result<Vec<ProjectDocumentRevisionRead>> {
        let mut statement = String::from(
            "SELECT * FROM project_document_revision
         WHERE document_id = ?",
        );
        if cursor.is_some() {
            statement.push_str(" AND (revision < ? OR (revision = ? AND id < ?))");
        }
        statement.push_str(" ORDER BY revision DESC, id DESC LIMIT ?");
        let mut revision_query = sqlx::query(&statement).bind(document_id);
        if let Some((revision, id)) = cursor {
            revision_query = revision_query.bind(revision).bind(revision).bind(id);
        }
        let rows = revision_query
            .bind(limit + 1)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectDocumentRevisionRead { row })
            .collect())
    }
    pub async fn project_document_count_before(
        &self,
        project_id: &str,
        updated_at: &str,
        id: &str,
    ) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_document
         WHERE project_id = ? AND (updated_at < ? OR (updated_at = ? AND id < ?))",
        )
        .bind(project_id)
        .bind(updated_at)
        .bind(updated_at)
        .bind(id)
        .fetch_one(self.pool())
        .await?;
        Ok(count)
    }
}
