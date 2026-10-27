//! Project-scoped media and evidence reads. Cursor encoding remains at the API boundary.
//! Snapshots defer decoding so each row retains its place among the caller's
//! checksum reconciliation and response validation steps.
use super::*;

pub struct ProjectMediaAssetIdRead {
    row: SqliteRow,
}

impl ProjectMediaAssetIdRead {
    pub fn asset_id(&self) -> Result<String> {
        Ok(self.row.try_get("id")?)
    }
}

pub struct ProjectEvidenceAttachmentRead {
    row: SqliteRow,
}

impl ProjectEvidenceAttachmentRead {
    pub fn into_attachment(self) -> Result<ProjectMediaAttachment> {
        map_project_media_attachment(&self.row)
    }
}

impl SqliteDb {
    pub async fn project_media_asset_id_page(
        &self,
        project_id: &str,
        cursor: Option<(String, String)>,
        limit: i64,
    ) -> Result<(Vec<ProjectMediaAssetIdRead>, bool)> {
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |(created_at, id)| {
            (Some(created_at), Some(id))
        });
        let mut statement = sqlx::query(
            "SELECT m.* FROM media_asset m
         WHERE m.project_id = ? AND m.deleted_at IS NULL
         ORDER BY m.created_at ASC, m.id ASC LIMIT ?",
        );
        if let (Some(created_at), Some(id)) = (cursor_created_at, cursor_id) {
            statement = sqlx::query(
                "SELECT m.* FROM media_asset m
             WHERE m.project_id = ? AND m.deleted_at IS NULL
               AND (m.created_at > ? OR (m.created_at = ? AND m.id > ?))
             ORDER BY m.created_at ASC, m.id ASC LIMIT ?",
            )
            .bind(project_id)
            .bind(created_at.clone())
            .bind(created_at)
            .bind(id)
            .bind(limit + 1);
        } else {
            statement = statement.bind(project_id).bind(limit + 1);
        }
        let mut rows = statement.fetch_all(self.pool()).await?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let ids = rows
            .into_iter()
            .map(|row| ProjectMediaAssetIdRead { row })
            .collect();
        Ok((ids, has_more))
    }

    pub async fn project_media_task_media_ids(
        &self,
        project_id: &str,
        asset_id: &str,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT task_media_id FROM project_media_attachment
             WHERE project_id = ? AND asset_id = ? AND task_media_id IS NOT NULL
               AND deleted_at IS NULL ORDER BY id ASC",
        )
        .bind(project_id)
        .bind(asset_id)
        .fetch_all(self.pool())
        .await?)
    }

    pub async fn project_evidence_page(
        &self,
        project_id: &str,
        milestone_id: &str,
        cursor: Option<(String, String)>,
        limit: i64,
    ) -> Result<(Vec<ProjectEvidenceAttachmentRead>, bool)> {
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |(created_at, id)| {
            (Some(created_at), Some(id))
        });
        let mut statement = sqlx::query(
            "SELECT * FROM project_media_attachment
         WHERE project_id = ? AND milestone_id = ?
           AND attachment_kind = 'evidence' AND deleted_at IS NULL
         ORDER BY created_at ASC, id ASC LIMIT ?",
        );
        if let (Some(created_at), Some(id)) = (cursor_created_at, cursor_id) {
            statement = sqlx::query(
                "SELECT * FROM project_media_attachment
             WHERE project_id = ? AND milestone_id = ?
               AND attachment_kind = 'evidence' AND deleted_at IS NULL
               AND (created_at > ? OR (created_at = ? AND id > ?))
             ORDER BY created_at ASC, id ASC LIMIT ?",
            )
            .bind(project_id)
            .bind(milestone_id)
            .bind(created_at.clone())
            .bind(created_at)
            .bind(id)
            .bind(limit + 1);
        } else {
            statement = statement
                .bind(project_id)
                .bind(milestone_id)
                .bind(limit + 1);
        }
        let mut rows = statement.fetch_all(self.pool()).await?;
        let has_more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let items = rows
            .into_iter()
            .map(|row| ProjectEvidenceAttachmentRead { row })
            .collect();
        Ok((items, has_more))
    }

    pub async fn project_evidence_by_id(
        &self,
        project_id: &str,
        milestone_id: &str,
        evidence_id: &str,
    ) -> Result<Option<ProjectMediaAttachment>> {
        let row = sqlx::query(
            "SELECT * FROM project_media_attachment
         WHERE id = ? AND project_id = ? AND milestone_id = ?
           AND attachment_kind = 'evidence' AND deleted_at IS NULL",
        )
        .bind(evidence_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(map_project_media_attachment).transpose()
    }
}
