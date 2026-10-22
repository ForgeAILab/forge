use super::*;
fn map_release(row: SqliteRow) -> Result<ProjectReleaseRecord> {
    Ok(ProjectReleaseRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        release_sequence: row.try_get("release_sequence")?,
        release_revision: row.try_get("release_revision")?,
        release_identifier: required_string(&row, "release_identifier")?,
        milestone_revision_id: required_string(&row, "milestone_revision_id")?,
        readiness_snapshot_id: required_string(&row, "readiness_snapshot_id")?,
        readiness_digest: required_string(&row, "readiness_digest")?,
        summary: required_string(&row, "summary")?,
        changelog: required_string(&row, "changelog")?,
        known_issues_json: required_string(&row, "known_issues_json")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        document_revisions_json: required_string(&row, "document_revisions_json")?,
        decision_ids_json: required_string(&row, "decision_ids_json")?,
        task_references_json: required_string(&row, "task_references_json")?,
        validation_references_json: required_string(&row, "validation_references_json")?,
        git_references_json: required_string(&row, "git_references_json")?,
        evidence_references_json: required_string(&row, "evidence_references_json")?,
        waivers_json: required_string(&row, "waivers_json")?,
        releasing_principal_type: required_string(&row, "releasing_principal_type")?,
        releasing_principal_id: required_string(&row, "releasing_principal_id")?,
        releasing_principal_display_name: optional_string(
            &row,
            "releasing_principal_display_name",
        )?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        explicit_event: required_string(&row, "explicit_event")?,
        schema_version: required_string(&row, "schema_version")?,
        snapshot_digest: required_string(&row, "snapshot_digest")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_release_reference(row: SqliteRow) -> Result<ProjectReleaseReferenceRecord> {
    Ok(ProjectReleaseReferenceRecord {
        release_id: required_string(&row, "release_id")?,
        ordinal: row.try_get("ordinal")?,
        reference_kind: required_string(&row, "reference_kind")?,
        record_id: required_string(&row, "record_id")?,
        record_version: optional_string(&row, "record_version")?,
        record_state: optional_string(&row, "record_state")?,
        record_digest: optional_string(&row, "record_digest")?,
        metadata_json: required_string(&row, "metadata_json")?,
    })
}

fn release_record_from_row(row: sqlx::sqlite::SqliteRow) -> crate::Result<ProjectReleaseRecord> {
    Ok(ProjectReleaseRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        milestone_id: row.try_get("milestone_id")?,
        release_sequence: row.try_get("release_sequence")?,
        release_revision: row.try_get("release_revision")?,
        release_identifier: row.try_get("release_identifier")?,
        milestone_revision_id: row.try_get("milestone_revision_id")?,
        readiness_snapshot_id: row.try_get("readiness_snapshot_id")?,
        readiness_digest: row.try_get("readiness_digest")?,
        summary: row.try_get("summary")?,
        changelog: row.try_get("changelog")?,
        known_issues_json: row.try_get("known_issues_json")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        document_revisions_json: row.try_get("document_revisions_json")?,
        decision_ids_json: row.try_get("decision_ids_json")?,
        task_references_json: row.try_get("task_references_json")?,
        validation_references_json: row.try_get("validation_references_json")?,
        git_references_json: row.try_get("git_references_json")?,
        evidence_references_json: row.try_get("evidence_references_json")?,
        waivers_json: row.try_get("waivers_json")?,
        releasing_principal_type: row.try_get("releasing_principal_type")?,
        releasing_principal_id: row.try_get("releasing_principal_id")?,
        releasing_principal_display_name: row.try_get("releasing_principal_display_name")?,
        authorization_basis: row.try_get("authorization_basis")?,
        authorization_action: row.try_get("authorization_action")?,
        explicit_event: row.try_get("explicit_event")?,
        authorization_occurred_at: row.try_get("authorization_occurred_at")?,
        schema_version: row.try_get("schema_version")?,
        snapshot_digest: row.try_get("snapshot_digest")?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
    })
}

impl SqliteDb {
    pub async fn release_by_idempotency_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        key: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(project_id)
        .bind(key)
        .fetch_optional(connection)
        .await?
        .map(release_record_from_row)
        .transpose()
    }
}

#[derive(Debug)]
pub struct InsertMilestoneReleaseInput<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub milestone_id: &'a str,
    pub release_revision: i64,
    pub release_identifier: &'a str,
    pub milestone_revision_id: &'a str,
    pub readiness_snapshot_id: &'a str,
    pub readiness_digest: &'a str,
    pub summary: &'a str,
    pub changelog: &'a str,
    pub known_issues_json: &'a str,
    pub charter_revision_id: Option<&'a str>,
    pub document_revisions_json: &'a str,
    pub decision_ids_json: &'a str,
    pub task_references_json: &'a str,
    pub validation_references_json: &'a str,
    pub git_references_json: &'a str,
    pub evidence_references_json: &'a str,
    pub waivers_json: &'a str,
    pub principal_type: &'a str,
    pub principal_id: &'a str,
    pub principal_display_name: Option<&'a str>,
    pub authorization_basis: &'a str,
    pub authorization_action: &'a str,
    pub explicit_event: &'a str,
    pub authorization_occurred_at: &'a str,
    pub schema_version: &'a str,
    pub snapshot_digest: &'a str,
    pub idempotency_key: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_milestone_release_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertMilestoneReleaseInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query("INSERT INTO project_release (
                id, project_id, milestone_id, release_sequence, release_revision,
                release_identifier, milestone_revision_id, readiness_snapshot_id,
                readiness_digest, summary, changelog,
                known_issues_json, charter_revision_id, document_revisions_json, decision_ids_json,
                task_references_json, validation_references_json, git_references_json,
                evidence_references_json, waivers_json, releasing_principal_type,
                releasing_principal_id, releasing_principal_display_name,
                authorization_basis, authorization_action,
                explicit_event, authorization_occurred_at, schema_version,
                snapshot_digest, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(input.id)
            .bind(input.project_id)
            .bind(input.milestone_id)
            .bind(input.release_revision)
            .bind(input.release_revision)
            .bind(input.release_identifier)
            .bind(input.milestone_revision_id)
            .bind(input.readiness_snapshot_id)
            .bind(input.readiness_digest)
            .bind(input.summary)
            .bind(input.changelog)
            .bind(input.known_issues_json)
            .bind(input.charter_revision_id)
            .bind(input.document_revisions_json)
            .bind(input.decision_ids_json)
            .bind(input.task_references_json)
            .bind(input.validation_references_json)
            .bind(input.git_references_json)
            .bind(input.evidence_references_json)
            .bind(input.waivers_json)
            .bind(input.principal_type)
            .bind(input.principal_id)
            .bind(input.principal_display_name)
            .bind(input.authorization_basis)
            .bind(input.authorization_action)
            .bind(input.explicit_event)
            .bind(input.authorization_occurred_at)
            .bind(input.schema_version)
            .bind(input.snapshot_digest)
            .bind(input.idempotency_key)
            .bind(input.created_at)
            .execute(connection).await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseTaskReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub record_version: &'a str,
    pub record_state: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_task_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseTaskReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'task', ?, ?, ?, NULL, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(input.record_version)
        .bind(input.record_state)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseValidationReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub record_version: &'a str,
    pub record_state: &'a str,
    pub record_digest: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_validation_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseValidationReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'validation', ?, ?, ?, ?, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(input.record_version)
        .bind(input.record_state)
        .bind(input.record_digest)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseDocumentReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub record_digest: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_document_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseDocumentReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'document', ?, NULL, 'approved', ?, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(input.record_digest)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseDecisionReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub record_state: &'a str,
    pub record_digest: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_decision_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseDecisionReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'decision', ?, NULL, ?, ?, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(input.record_state)
        .bind(input.record_digest)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseGitReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub record_version: &'a str,
    pub execution_status: Option<&'a str>,
    pub record_digest: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_git_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseGitReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'git_ref', ?, ?, ?, ?, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(input.record_version)
        .bind(input.execution_status)
        .bind(input.record_digest)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertReleaseReviewReferenceInput<'a> {
    pub release_id: &'a str,
    pub ordinal: i64,
    pub record_id: &'a str,
    pub review_version: Option<String>,
    pub record_state: Option<&'a str>,
    pub record_digest: &'a str,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_release_review_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertReleaseReviewReferenceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                        release_id, ordinal, reference_kind, record_id,
                        record_version, record_state, record_digest, metadata_json
                     ) VALUES (?, ?, 'review', ?, ?, ?, ?, ?)",
        )
        .bind(input.release_id)
        .bind(input.ordinal)
        .bind(input.record_id)
        .bind(&input.review_version)
        .bind(input.record_state)
        .bind(input.record_digest)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn insert_release_known_issue_reference_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        release_id: &str,
        ordinal: i64,
        record_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'known_issue', ?, NULL, NULL, NULL, '{}')",
        )
        .bind(release_id)
        .bind(ordinal)
        .bind(record_id)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn release_record_by_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        release_id: &str,
    ) -> crate::Result<ProjectReleaseRecord> {
        release_record_from_row(
            sqlx::query("SELECT * FROM project_release WHERE id = ?")
                .bind(release_id)
                .fetch_one(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn project_release_record_by_id(
        &self,
        release_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        sqlx::query("SELECT * FROM project_release WHERE id = ? AND project_id = ?")
            .bind(release_id)
            .bind(project_id)
            .fetch_optional(self.pool())
            .await?
            .map(release_record_from_row)
            .transpose()
    }
}

impl SqliteDb {
    pub async fn releases_pinning_readiness_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_release
             WHERE project_id = ? AND milestone_id = ? AND readiness_snapshot_id = ?",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(snapshot_id)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn milestone_release_records(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ProjectReleaseRecord>> {
        sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND milestone_id = ?
             ORDER BY release_revision ASC, id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(release_record_from_row)
        .collect()
    }
}

impl SqliteDb {
    pub async fn next_milestone_release_revision_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar(
            "SELECT COALESCE(MAX(release_revision), 0) + 1
             FROM project_release WHERE milestone_id = ?",
        )
        .bind(milestone_id)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn project_release_by_idempotency(
        &self,
        project_id: &str,
        key: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        sqlx::query("SELECT * FROM project_release WHERE project_id = ? AND idempotency_key = ?")
            .bind(project_id)
            .bind(key)
            .fetch_optional(self.pool())
            .await?
            .map(release_record_from_row)
            .transpose()
    }
}

impl SqliteDb {
    pub async fn project_release_by_readiness_target(
        &self,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
        digest: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND milestone_id = ?
               AND readiness_snapshot_id = ? AND readiness_digest = ?
             ORDER BY release_revision ASC, id ASC LIMIT 1",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(snapshot_id)
        .bind(digest)
        .fetch_optional(self.pool())
        .await?
        .map(release_record_from_row)
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseCharterRevisionSourceRow {
    pub charter_id: String,
    pub revision: i64,
    pub content_digest: String,
    pub render_version: String,
    pub rendered_digest: String,
}

impl SqliteDb {
    pub async fn release_charter_revision_source(
        &self,
        revision_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ReleaseCharterRevisionSourceRow>> {
        let row = sqlx::query(
            "SELECT cr.charter_id, cr.revision, cr.content_digest,
                            cr.render_version, cr.rendered_digest
                     FROM project_charter_revision cr
                     JOIN project_charter c ON c.id = cr.charter_id
                     WHERE cr.id = ? AND c.project_id = ?",
        )
        .bind(revision_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(|row| {
            Ok(ReleaseCharterRevisionSourceRow {
                charter_id: row.try_get("charter_id")?,
                revision: row.try_get("revision")?,
                content_digest: row.try_get("content_digest")?,
                render_version: row.try_get("render_version")?,
                rendered_digest: row.try_get("rendered_digest")?,
            })
        })
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseReferenceRow {
    pub reference_kind: String,
    pub record_id: String,
    pub record_version: Option<String>,
    pub record_state: Option<String>,
    pub record_digest: Option<String>,
    pub metadata_json: String,
}

impl SqliteDb {
    pub async fn release_reference_rows(
        &self,
        release_id: &str,
    ) -> crate::Result<Vec<ReleaseReferenceRow>> {
        let rows = sqlx::query(
            "SELECT reference_kind, record_id, record_version, record_state,
                    record_digest, metadata_json
             FROM project_release_reference
             WHERE release_id = ? ORDER BY ordinal ASC",
        )
        .bind(release_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ReleaseReferenceRow {
                    reference_kind: row.try_get("reference_kind")?,
                    record_id: row.try_get("record_id")?,
                    record_version: row.try_get("record_version")?,
                    record_state: row.try_get("record_state")?,
                    record_digest: row.try_get("record_digest")?,
                    metadata_json: row.try_get("metadata_json")?,
                })
            })
            .collect()
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectReleaseSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectReleaseSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn milestone_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn release_sequence(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("release_sequence")?)
    }
    pub fn release_revision(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("release_revision")?)
    }
    pub fn release_identifier(&self) -> crate::Result<String> {
        Ok(self.row.try_get("release_identifier")?)
    }
    pub fn readiness_snapshot_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("readiness_snapshot_id")?)
    }
    pub fn readiness_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("readiness_digest")?)
    }
    pub fn snapshot_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("snapshot_digest")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_release_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectReleaseSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, milestone_id, release_sequence, release_revision,
                release_identifier, readiness_snapshot_id, readiness_digest,
                snapshot_digest,
                created_at
         FROM project_release
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectReleaseSourceRow { row })
            .collect())
    }
}
pub(super) async fn create_project_release(
    db: &SqliteDb,
    input: CreateProjectRelease,
    references: Vec<CreateProjectReleaseReference>,
) -> Result<ProjectReleaseRecord> {
    if input.releasing_principal_type.trim().is_empty()
        || input.releasing_principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(existing) = sqlx::query("SELECT * FROM project_release WHERE idempotency_key = ?")
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .map(map_release)
        .transpose()?
    {
        if existing.id != input.id
            || existing.project_id != input.project_id
            || existing.milestone_id != input.milestone_id
            || existing.release_sequence != input.release_sequence
            || existing.release_revision != input.release_revision
            || existing.release_identifier != input.release_identifier
            || existing.milestone_revision_id != input.milestone_revision_id
            || existing.readiness_snapshot_id != input.readiness_snapshot_id
            || existing.readiness_digest != input.readiness_digest
            || existing.summary != input.summary
            || existing.changelog != input.changelog
            || existing.known_issues_json != input.known_issues_json
            || existing.charter_revision_id != input.charter_revision_id
            || existing.document_revisions_json != input.document_revisions_json
            || existing.decision_ids_json != input.decision_ids_json
            || existing.task_references_json != input.task_references_json
            || existing.validation_references_json != input.validation_references_json
            || existing.git_references_json != input.git_references_json
            || existing.evidence_references_json != input.evidence_references_json
            || existing.waivers_json != input.waivers_json
            || existing.releasing_principal_type != input.releasing_principal_type
            || existing.releasing_principal_id != input.releasing_principal_id
            || existing.releasing_principal_display_name != input.releasing_principal_display_name
            || existing.authorization_basis != input.authorization_basis
            || existing.authorization_action != input.authorization_action
            || existing.authorization_occurred_at != input.authorization_occurred_at
            || existing.explicit_event != input.explicit_event
            || existing.schema_version != input.schema_version
            || existing.snapshot_digest != input.snapshot_digest
        {
            return Err(DbError::VersionConflict);
        }
        let persisted_references = sqlx::query(
            "SELECT ordinal, reference_kind, record_id, record_version,
                        record_state, record_digest, metadata_json
                 FROM project_release_reference
                 WHERE release_id = ? ORDER BY ordinal ASC",
        )
        .bind(&existing.id)
        .fetch_all(&mut *tx)
        .await?;
        let same_references = persisted_references.len() == references.len()
            && persisted_references
                .iter()
                .zip(references.iter())
                .all(|(row, reference)| {
                    row.try_get::<i64, _>("ordinal").ok() == Some(reference.ordinal)
                        && row.try_get::<String, _>("reference_kind").ok()
                            == Some(reference.reference_kind.clone())
                        && row.try_get::<String, _>("record_id").ok()
                            == Some(reference.record_id.clone())
                        && row.try_get::<Option<String>, _>("record_version").ok()
                            == Some(reference.record_version.clone())
                        && row.try_get::<Option<String>, _>("record_state").ok()
                            == Some(reference.record_state.clone())
                        && row.try_get::<Option<String>, _>("record_digest").ok()
                            == Some(reference.record_digest.clone())
                        && row.try_get::<String, _>("metadata_json").ok()
                            == Some(reference.metadata_json.clone())
                });
        if !same_references {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let milestone = sqlx::query(
        "SELECT version, lifecycle, milestone_key FROM project_milestone
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    if milestone_version != input.expected_milestone_version {
        return Err(DbError::VersionConflict);
    }
    let readiness = sqlx::query(
        "SELECT definition_revision_id
             FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND readiness_digest = ? AND outcome = 'ready'",
    )
    .bind(&input.readiness_snapshot_id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.readiness_digest)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let definition_revision_id: String = readiness.try_get("definition_revision_id")?;
    if definition_revision_id != input.milestone_revision_id {
        return Err(DbError::VersionConflict);
    }
    if let Some(charter_revision_id) = input.charter_revision_id.as_deref() {
        let charter_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1
                 FROM project_charter c
                 JOIN project_charter_revision r ON r.id = ? AND r.charter_id = c.id
                 WHERE c.project_id = ?
                   AND c.current_approved_revision_id = r.id
                   AND r.lifecycle = 'approved'
                 LIMIT 1",
        )
        .bind(charter_revision_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if charter_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_release (
                id, project_id, milestone_id, release_sequence, release_revision,
                release_identifier, milestone_revision_id, readiness_snapshot_id,
                readiness_digest, summary, changelog,
                known_issues_json,
                charter_revision_id, document_revisions_json, decision_ids_json,
                task_references_json, validation_references_json, git_references_json,
                evidence_references_json, waivers_json, releasing_principal_type,
                releasing_principal_id, releasing_principal_display_name,
                authorization_basis, authorization_action,
                authorization_occurred_at, explicit_event, schema_version,
                snapshot_digest, idempotency_key, created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(input.release_sequence)
    .bind(input.release_revision)
    .bind(&input.release_identifier)
    .bind(&input.milestone_revision_id)
    .bind(&input.readiness_snapshot_id)
    .bind(&input.readiness_digest)
    .bind(&input.summary)
    .bind(&input.changelog)
    .bind(&input.known_issues_json)
    .bind(input.charter_revision_id.as_deref())
    .bind(&input.document_revisions_json)
    .bind(&input.decision_ids_json)
    .bind(&input.task_references_json)
    .bind(&input.validation_references_json)
    .bind(&input.git_references_json)
    .bind(&input.evidence_references_json)
    .bind(&input.waivers_json)
    .bind(&input.releasing_principal_type)
    .bind(&input.releasing_principal_id)
    .bind(input.releasing_principal_display_name.as_deref())
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.authorization_occurred_at)
    .bind(&input.explicit_event)
    .bind(&input.schema_version)
    .bind(&input.snapshot_digest)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        if error.to_string().to_ascii_lowercase().contains("unique") {
            DbError::VersionConflict
        } else {
            check_error(error)
        }
    })?;
    for reference in references {
        sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&reference.release_id)
        .bind(reference.ordinal)
        .bind(&reference.reference_kind)
        .bind(&reference.record_id)
        .bind(reference.record_version.as_deref())
        .bind(reference.record_state.as_deref())
        .bind(reference.record_digest.as_deref())
        .bind(&reference.metadata_json)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
    }
    let released = sqlx::query(
        "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('ready_for_release', 'released')",
    )
    .bind(&input.created_at)
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .bind(input.expected_milestone_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if released.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let row = sqlx::query("SELECT * FROM project_release WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_release(row)
}
pub(super) async fn create_project_release_command(
    db: &SqliteDb,
    input: CreateProjectReleaseCommand,
) -> Result<ProjectReleaseRecord> {
    let CreateProjectReleaseCommand {
        release: input,
        references,
        command_receipt,
        action_execution,
    } = input;
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[
                ("project_id", input.project_id.as_str()),
                ("milestone_id", input.milestone_id.as_str()),
            ],
        )?;
        let release_id = command_outcome_string(&receipt, "release_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT * FROM project_release
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
        )
        .bind(&release_id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
        let record = map_release(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    if input.releasing_principal_type.trim().is_empty()
        || input.releasing_principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
    if references
        .iter()
        .any(|reference| reference.release_id != input.id)
    {
        return Err(DbError::VersionConflict);
    }
    let milestone = sqlx::query(
        "SELECT version, lifecycle, milestone_key FROM project_milestone
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    if milestone_version != input.expected_milestone_version {
        return Err(DbError::VersionConflict);
    }
    let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
    if milestone_lifecycle != "ready_for_release" {
        return Err(DbError::VersionConflict);
    }
    let readiness = sqlx::query(
        "SELECT definition_revision_id
             FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND readiness_digest = ? AND outcome = 'ready'",
    )
    .bind(&input.readiness_snapshot_id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.readiness_digest)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let definition_revision_id: String = readiness.try_get("definition_revision_id")?;
    if definition_revision_id != input.milestone_revision_id {
        return Err(DbError::VersionConflict);
    }
    if let Some(charter_revision_id) = input.charter_revision_id.as_deref() {
        let charter_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1
                 FROM project_charter c
                 JOIN project_charter_revision r ON r.id = ? AND r.charter_id = c.id
                 WHERE c.project_id = ?
                   AND c.current_approved_revision_id = r.id
                   AND r.lifecycle = 'approved'
                 LIMIT 1",
        )
        .bind(charter_revision_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if charter_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_release (
                id, project_id, milestone_id, release_sequence, release_revision,
                release_identifier, milestone_revision_id, readiness_snapshot_id,
                readiness_digest, summary, changelog,
                known_issues_json, charter_revision_id, document_revisions_json,
                decision_ids_json, task_references_json, validation_references_json,
                git_references_json, evidence_references_json, waivers_json,
                releasing_principal_type, releasing_principal_id,
                releasing_principal_display_name, authorization_basis,
                authorization_action, authorization_occurred_at, explicit_event,
                schema_version, snapshot_digest, idempotency_key, created_at
             ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(input.release_sequence)
    .bind(input.release_revision)
    .bind(&input.release_identifier)
    .bind(&input.milestone_revision_id)
    .bind(&input.readiness_snapshot_id)
    .bind(&input.readiness_digest)
    .bind(&input.summary)
    .bind(&input.changelog)
    .bind(&input.known_issues_json)
    .bind(input.charter_revision_id.as_deref())
    .bind(&input.document_revisions_json)
    .bind(&input.decision_ids_json)
    .bind(&input.task_references_json)
    .bind(&input.validation_references_json)
    .bind(&input.git_references_json)
    .bind(&input.evidence_references_json)
    .bind(&input.waivers_json)
    .bind(&input.releasing_principal_type)
    .bind(&input.releasing_principal_id)
    .bind(input.releasing_principal_display_name.as_deref())
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.authorization_occurred_at)
    .bind(&input.explicit_event)
    .bind(&input.schema_version)
    .bind(&input.snapshot_digest)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    for reference in references {
        sqlx::query(
            "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&reference.release_id)
        .bind(reference.ordinal)
        .bind(&reference.reference_kind)
        .bind(&reference.record_id)
        .bind(reference.record_version.as_deref())
        .bind(reference.record_state.as_deref())
        .bind(reference.record_digest.as_deref())
        .bind(&reference.metadata_json)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    }
    let released = sqlx::query(
        "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle = 'ready_for_release'",
    )
    .bind(&input.created_at)
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .bind(input.expected_milestone_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if released.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            command_receipt.as_ref(),
            input.releasing_principal_type.clone(),
            Some(input.releasing_principal_id.clone()),
            input.id.clone(),
            Some(input.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.release.created".to_owned(),
        entity_type: "project_release".to_owned(),
        entity_id: input.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-release-created:{}", input.id)),
        payload_json: serde_json::json!({
            "operation": "project.release",
            "project_id": input.project_id.clone(),
            "milestone_id": input.milestone_id.clone(),
            "release_id": input.id.clone(),
            "release_identifier": input.release_identifier.clone(),
            "release_revision": input.release_revision,
            "readiness_snapshot_id": input.readiness_snapshot_id.clone(),
            "readiness_digest": input.readiness_digest.clone(),
            "snapshot_digest": input.snapshot_digest.clone(),
        })
        .to_string(),
        created_at: input.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(db, &mut tx, &event.id, command_receipt, action_execution).await?;
    let row = sqlx::query("SELECT * FROM project_release WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_release(row)
}
pub(super) async fn create_project_release_request_command(
    db: &SqliteDb,
    input: CreateProjectReleaseRequestCommand,
) -> Result<ProjectReleaseRequestRecord> {
    let CreateProjectReleaseRequestCommand {
        request,
        command_receipt,
        action_execution,
    } = input;
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[
                ("project_id", request.project_id.as_str()),
                ("milestone_id", request.milestone_id.as_str()),
                (
                    "readiness_snapshot_id",
                    request.readiness_snapshot_id.as_str(),
                ),
            ],
        )?;
        validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
        let event_id = serde_json::from_str::<serde_json::Value>(&receipt.outcome_json)
            .ok()
            .and_then(|outcome| {
                outcome
                    .get("candidate_event_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| receipt.event_id.clone());
        let event = sqlx::query(
            "SELECT * FROM domain_event
                 WHERE id = ? AND event_type = 'project_release.candidate_requested'
                   AND scope_type = 'project' AND scope_id = ?",
        )
        .bind(&event_id)
        .bind(&request.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(command_receipt.as_ref(), "project", &request.project_id)?;
        let payload: serde_json::Value =
            serde_json::from_str(&event.try_get::<String, _>("payload_json")?)
                .map_err(|_| DbError::IdempotencyConflict)?;
        let record = ProjectReleaseRequestRecord {
            event_id,
            project_id: payload
                .get("project_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::IdempotencyConflict)?
                .to_owned(),
            milestone_id: payload
                .get("milestone_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::IdempotencyConflict)?
                .to_owned(),
            expected_milestone_version: payload
                .get("milestone_version")
                .and_then(serde_json::Value::as_i64)
                .ok_or(DbError::IdempotencyConflict)?,
            readiness_snapshot_id: payload
                .get("readiness_snapshot_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::IdempotencyConflict)?
                .to_owned(),
            readiness_digest: payload
                .get("readiness_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::IdempotencyConflict)?
                .to_owned(),
            status: payload
                .get("status")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::IdempotencyConflict)?
                .to_owned(),
            idempotency_key: request.idempotency_key.clone(),
            created_at: event.try_get("created_at")?,
        };
        tx.commit().await?;
        return Ok(record);
    }

    validate_command_scope(command_receipt.as_ref(), "project", &request.project_id)?;
    if request.event_id.trim().is_empty()
        || request.project_id.trim().is_empty()
        || request.milestone_id.trim().is_empty()
        || request.readiness_snapshot_id.trim().is_empty()
        || request.readiness_digest.trim().is_empty()
        || request.status.trim().is_empty()
        || request.idempotency_key.trim().is_empty()
    {
        return Err(DbError::Check(
            "release request command input is incomplete".to_owned(),
        ));
    }
    let milestone = sqlx::query(
        "SELECT version, lifecycle FROM project_milestone
             WHERE id = ? AND project_id = ?",
    )
    .bind(&request.milestone_id)
    .bind(&request.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
    if milestone_version != request.expected_milestone_version
        || milestone_lifecycle != "ready_for_release"
    {
        return Err(DbError::VersionConflict);
    }
    let ready: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND readiness_digest = ? AND outcome = 'ready' LIMIT 1",
    )
    .bind(&request.readiness_snapshot_id)
    .bind(&request.project_id)
    .bind(&request.milestone_id)
    .bind(&request.readiness_digest)
    .fetch_optional(&mut *tx)
    .await?;
    if ready.is_none() {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            command_receipt.as_ref(),
            "agent".to_owned(),
            None,
            request.event_id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: request.event_id.clone(),
        event_type: "project_release.candidate_requested".to_owned(),
        entity_type: "project_readiness_snapshot".to_owned(),
        entity_id: request.readiness_snapshot_id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: request.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-release-request:{}",
            request.idempotency_key
        )),
        payload_json: serde_json::json!({
            "operation": "project.release",
            "project_id": request.project_id.clone(),
            "milestone_id": request.milestone_id.clone(),
            "milestone_version": request.expected_milestone_version,
            "readiness_snapshot_id": request.readiness_snapshot_id.clone(),
            "readiness_digest": request.readiness_digest.clone(),
            "status": request.status.clone(),
            "final_release_created": false,
        })
        .to_string(),
        created_at: request.created_at.clone(),
    };
    let event = DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(db, &mut tx, &event.id, command_receipt, action_execution).await?;
    let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
        .map_err(|_| DbError::Check("release request event payload is invalid".to_owned()))?;
    let record = ProjectReleaseRequestRecord {
        event_id: event.id.clone(),
        project_id: request.project_id,
        milestone_id: request.milestone_id,
        expected_milestone_version: request.expected_milestone_version,
        readiness_snapshot_id: request.readiness_snapshot_id,
        readiness_digest: request.readiness_digest,
        status: payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(request.status.as_str())
            .to_owned(),
        idempotency_key: request.idempotency_key,
        created_at: event.created_at.clone(),
    };
    tx.commit().await?;
    Ok(record)
}
pub(super) async fn list_project_release_references(
    db: &SqliteDb,
    release_id: &str,
) -> Result<Vec<ProjectReleaseReferenceRecord>> {
    sqlx::query(
        "SELECT * FROM project_release_reference
             WHERE release_id = ? ORDER BY ordinal ASC",
    )
    .bind(release_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_release_reference)
    .collect()
}
