use super::*;
fn map_document(row: SqliteRow) -> Result<ProjectDocumentRecord> {
    Ok(ProjectDocumentRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        kind: required_string(&row, "kind")?,
        title: required_string(&row, "title")?,
        lifecycle: required_string(&row, "lifecycle")?,
        approval_policy: required_string(&row, "approval_policy")?,
        current_draft_revision_id: optional_string(&row, "current_draft_revision_id")?,
        current_approved_revision_id: optional_string(&row, "current_approved_revision_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_document_revision(row: SqliteRow) -> Result<ProjectDocumentRevisionRecord> {
    Ok(ProjectDocumentRevisionRecord {
        id: required_string(&row, "id")?,
        document_id: required_string(&row, "document_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        content_json: required_string(&row, "content_json")?,
        rendered_view: required_string(&row, "rendered_view")?,
        change_summary: required_string(&row, "change_summary")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_document_approval(row: SqliteRow) -> Result<ProjectDocumentApprovalRecord> {
    Ok(ProjectDocumentApprovalRecord {
        id: required_string(&row, "id")?,
        document_id: required_string(&row, "document_id")?,
        revision_id: required_string(&row, "revision_id")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        lifecycle: required_string(&row, "lifecycle")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectApprovedDocumentSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectApprovedDocumentSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn kind(&self) -> crate::Result<String> {
        Ok(self.row.try_get("kind")?)
    }
    pub fn title(&self) -> crate::Result<String> {
        Ok(self.row.try_get("title")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
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
impl SqliteDb {
    pub async fn effective_project_approved_document_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectApprovedDocumentSourceRow>> {
        let rows = sqlx::query(
            "SELECT d.id, d.kind, d.title, d.current_approved_revision_id AS revision_id,
                d.version, d.lifecycle, r.revision, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_document d
         JOIN project_document_revision r
           ON r.id = d.current_approved_revision_id AND r.document_id = d.id
         WHERE d.project_id = ? AND r.lifecycle = 'approved'
         ORDER BY d.updated_at DESC, d.id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectApprovedDocumentSourceRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn effective_project_unreleased_document_ids(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_document
         WHERE project_id = ? AND current_draft_revision_id IS NOT NULL
           AND (current_approved_revision_id IS NULL
                OR current_draft_revision_id != current_approved_revision_id)
         ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ArtifactDocumentApprovalSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ArtifactDocumentApprovalSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn document_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("document_id")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn principal_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_type")?)
    }
    pub fn principal_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_id")?)
    }
    pub fn authorization_basis(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_basis")?)
    }
    pub fn authorization_action(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_action")?)
    }
    pub fn explicit_event(&self) -> crate::Result<String> {
        Ok(self.row.try_get("explicit_event")?)
    }
    pub fn authorization_occurred_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_occurred_at")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn rendered_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
    pub fn idempotency_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("idempotency_key")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn artifact_next_document_revision(&self, document_id: &str) -> crate::Result<i64> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
        )
        .bind(document_id)
        .fetch_one(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn artifact_document_approval_source(
        &self,
        approval_id: &str,
    ) -> crate::Result<Option<ArtifactDocumentApprovalSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, document_id, revision_id, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, version, created_at, updated_at
         FROM project_document_approval WHERE id = ?",
        )
        .bind(approval_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| ArtifactDocumentApprovalSourceRow { row }))
    }
}
pub(super) async fn create_project_document(
    db: &SqliteDb,
    input: CreateProjectDocument,
) -> Result<ProjectDocumentRecord> {
    sqlx::query(
        "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.kind)
    .bind(&input.title)
    .bind(&input.approval_policy)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(db.pool())
    .await
    .map_err(check_error)?;
    db.get_project_document(&input.id)
        .await?
        .ok_or(DbError::NotFound)
}
pub(super) async fn get_project_document(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectDocumentRecord>> {
    select_one(
        "SELECT * FROM project_document WHERE id = ?",
        db.pool(),
        id,
        map_document,
    )
    .await
}
pub(super) async fn create_project_document_revision(
    db: &SqliteDb,
    input: CreateProjectDocumentRevision,
) -> Result<ProjectDocumentRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let document = sqlx::query(
        "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
    )
    .bind(&input.document_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let document_version: i64 = document.try_get("version")?;
    if document_version != input.expected_document_version {
        return Err(DbError::VersionConflict);
    }
    let current_draft: Option<String> = document.try_get("current_draft_revision_id")?;
    if input.base_revision > 0 {
        let Some(current_draft) = current_draft else {
            return Err(DbError::VersionConflict);
        };
        let Some(base_revision_id) = input.base_revision_id.as_deref() else {
            return Err(DbError::VersionConflict);
        };
        if current_draft != base_revision_id {
            return Err(DbError::VersionConflict);
        }
        let base_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_document_revision
                 WHERE id = ? AND document_id = ? AND revision = ? LIMIT 1",
        )
        .bind(base_revision_id)
        .bind(&input.document_id)
        .bind(input.base_revision)
        .fetch_optional(&mut *tx)
        .await?;
        if base_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if input.base_revision_id.is_some() || current_draft.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
    )
    .bind(&input.document_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.document_id)
    .bind(revision)
    .bind(input.base_revision)
    .bind(input.base_revision_id.as_deref())
    .bind(&input.lifecycle)
    .bind(&input.schema_version)
    .bind(&input.render_version)
    .bind(&input.content_json)
    .bind(&input.rendered_view)
    .bind(&input.change_summary)
    .bind(&input.author_type)
    .bind(input.author_id.as_deref())
    .bind(&input.source_refs_json)
    .bind(&input.content_digest)
    .bind(&input.rendered_digest)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.id)
    .bind(&input.created_at)
    .bind(&input.document_id)
    .bind(input.expected_document_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let project_id: String = document.try_get("project_id")?;
    DomainEventRepo::append_event_in_tx(
        db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.revision_created".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.author_type.clone(),
            actor_id: input.author_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id: input.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("project-document-revision-created:{}", input.id)),
            payload_json: serde_json::json!({
                "document_id": input.document_id.clone(),
                "revision_id": input.id.clone(),
                "revision": revision,
                "lifecycle": input.lifecycle.clone(),
                "content_digest": input.content_digest.clone(),
                "rendered_digest": input.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        },
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_revision(row)
}
pub(super) async fn create_project_document_atomically(
    db: &SqliteDb,
    input: CreateProjectDocumentAtomically,
) -> Result<ProjectDocumentRevisionRecord> {
    if input.document.id != input.revision.document_id
        || input.revision.expected_document_version != 1
        || input.revision.base_revision != 0
        || input.revision.base_revision_id.is_some()
    {
        return Err(DbError::VersionConflict);
    }

    let mut tx = crate::begin_immediate(db.pool()).await?;
    sqlx::query(
        "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
    )
    .bind(&input.document.id)
    .bind(&input.document.project_id)
    .bind(&input.document.kind)
    .bind(&input.document.title)
    .bind(&input.document.approval_policy)
    .bind(&input.document.created_at)
    .bind(&input.document.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    sqlx::query(
        "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.document_id)
    .bind(&input.revision.lifecycle)
    .bind(&input.revision.schema_version)
    .bind(&input.revision.render_version)
    .bind(&input.revision.content_json)
    .bind(&input.revision.rendered_view)
    .bind(&input.revision.change_summary)
    .bind(&input.revision.author_type)
    .bind(input.revision.author_id.as_deref())
    .bind(&input.revision.source_refs_json)
    .bind(&input.revision.content_digest)
    .bind(&input.revision.rendered_digest)
    .bind(&input.revision.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = 1",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.created_at)
    .bind(&input.document.id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    DomainEventRepo::append_event_in_tx(
        db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.revision_created".to_owned(),
            entity_type: "project_document_revision".to_owned(),
            entity_id: input.revision.id.clone(),
            actor_type: input.revision.author_type.clone(),
            actor_id: input.revision.author_id.clone(),
            scope_type: "project".to_owned(),
            scope_id: input.document.project_id.clone(),
            correlation_id: input.revision.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "project-document-revision-created:{}",
                input.revision.id
            )),
            payload_json: serde_json::json!({
                "document_id": input.revision.document_id.clone(),
                "revision_id": input.revision.id.clone(),
                "revision": 1,
                "lifecycle": input.revision.lifecycle.clone(),
                "content_digest": input.revision.content_digest.clone(),
                "rendered_digest": input.revision.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.revision.created_at.clone(),
        },
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_revision(row)
}
pub(super) async fn create_project_document_shell_command(
    db: &SqliteDb,
    input: CreateProjectDocumentShellCommand,
) -> Result<ProjectDocumentRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            [("project_id", input.document.project_id.as_str())].as_ref(),
        )?;
        let document_id = command_outcome_string(&receipt, "document_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT * FROM project_document
                 WHERE id = ? AND project_id = ?",
        )
        .bind(&document_id)
        .bind(&input.document.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let document = map_document(row)?;
        tx.commit().await?;
        return Ok(document);
    }

    if input.expected_project_version < 1 {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &input.document.project_id,
    )?;
    let now = input.document.updated_at.clone();
    let updated = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&now)
    .bind(&input.document.project_id)
    .bind(input.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
    )
    .bind(&input.document.id)
    .bind(&input.document.project_id)
    .bind(&input.document.kind)
    .bind(&input.document.title)
    .bind(&input.document.approval_policy)
    .bind(&input.document.created_at)
    .bind(&input.document.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;

    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            "user".to_owned(),
            None,
            input.document.id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.document.created".to_owned(),
        entity_type: "project_document".to_owned(),
        entity_id: input.document.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.document.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-document-created:{}", input.document.id)),
        payload_json: serde_json::json!({
            "project_id": input.document.project_id.clone(),
            "document_id": input.document.id.clone(),
            "kind": input.document.kind.clone(),
            "title": input.document.title.clone(),
            "approval_policy": input.document.approval_policy.clone(),
            "expected_project_version": input.expected_project_version,
        })
        .to_string(),
        created_at: input.document.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(
        db,
        &mut tx,
        &event.id,
        input.command_receipt,
        input.action_execution,
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document WHERE id = ?")
        .bind(&input.document.id)
        .fetch_one(&mut *tx)
        .await?;
    let document = map_document(row)?;
    tx.commit().await?;
    Ok(document)
}
pub(super) async fn create_project_document_command(
    db: &SqliteDb,
    input: CreateProjectDocumentCommand,
) -> Result<ProjectDocumentRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("project_id", input.document.project_id.as_str())],
        )?;
        let document_id = command_outcome_string(&receipt, "document_id")?;
        let revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT r.*, d.project_id
                 FROM project_document_revision r
                 JOIN project_document d ON d.id = r.document_id
                 WHERE r.id = ? AND r.document_id = ?",
        )
        .bind(&revision_id)
        .bind(&document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: String = row.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let record = map_document_revision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    if input.document.id != input.revision.document_id
        || input.revision.expected_document_version != 1
        || input.revision.base_revision != 0
        || input.revision.base_revision_id.is_some()
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &input.document.project_id,
    )?;
    sqlx::query("SELECT id FROM project WHERE id = ?")
        .bind(&input.document.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    sqlx::query(
        "INSERT INTO project_document (
                id, project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'draft', ?, NULL, NULL, 1, ?, ?)",
    )
    .bind(&input.document.id)
    .bind(&input.document.project_id)
    .bind(&input.document.kind)
    .bind(&input.document.title)
    .bind(&input.document.approval_policy)
    .bind(&input.document.created_at)
    .bind(&input.document.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    sqlx::query(
        "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.document_id)
    .bind(&input.revision.lifecycle)
    .bind(&input.revision.schema_version)
    .bind(&input.revision.render_version)
    .bind(&input.revision.content_json)
    .bind(&input.revision.rendered_view)
    .bind(&input.revision.change_summary)
    .bind(&input.revision.author_type)
    .bind(input.revision.author_id.as_deref())
    .bind(&input.revision.source_refs_json)
    .bind(&input.revision.content_digest)
    .bind(&input.revision.rendered_digest)
    .bind(&input.revision.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = 1",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.created_at)
    .bind(&input.document.id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            input.revision.author_type.clone(),
            input.revision.author_id.clone(),
            input.revision.id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.document.revision_created".to_owned(),
        entity_type: "project_document_revision".to_owned(),
        entity_id: input.revision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.document.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-document-revision-created:{}",
            input.revision.id
        )),
        payload_json: serde_json::json!({
            "document_id": input.revision.document_id.clone(),
            "revision_id": input.revision.id.clone(),
            "revision": 1,
            "lifecycle": input.revision.lifecycle.clone(),
            "content_digest": input.revision.content_digest.clone(),
            "rendered_digest": input.revision.rendered_digest.clone(),
        })
        .to_string(),
        created_at: input.revision.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(
        db,
        &mut tx,
        &event.id,
        input.command_receipt,
        input.action_execution,
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_revision(row)
}
pub(super) async fn append_project_document_revision_command(
    db: &SqliteDb,
    input: AppendProjectDocumentRevisionCommand,
) -> Result<ProjectDocumentRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("document_id", input.revision.document_id.as_str())],
        )?;
        let revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT r.*, d.project_id
                 FROM project_document_revision r
                 JOIN project_document d ON d.id = r.document_id
                 WHERE r.id = ? AND r.document_id = ?",
        )
        .bind(&revision_id)
        .bind(&input.revision.document_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: String = row.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let record = map_document_revision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let document = sqlx::query(
        "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
    )
    .bind(&input.revision.document_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let project_id: String = document.try_get("project_id")?;
    validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
    let document_version: i64 = document.try_get("version")?;
    if document_version != input.revision.expected_document_version {
        return Err(DbError::VersionConflict);
    }
    let current_draft: Option<String> = document.try_get("current_draft_revision_id")?;
    if input.revision.base_revision > 0 {
        let Some(current_draft) = current_draft else {
            return Err(DbError::VersionConflict);
        };
        let Some(base_revision_id) = input.revision.base_revision_id.as_deref() else {
            return Err(DbError::VersionConflict);
        };
        if current_draft != base_revision_id {
            return Err(DbError::VersionConflict);
        }
        let base_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_document_revision
                 WHERE id = ? AND document_id = ? AND revision = ? LIMIT 1",
        )
        .bind(base_revision_id)
        .bind(&input.revision.document_id)
        .bind(input.revision.base_revision)
        .fetch_optional(&mut *tx)
        .await?;
        if base_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if input.revision.base_revision_id.is_some() || current_draft.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
    )
    .bind(&input.revision.document_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO project_document_revision (
                id, document_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_refs_json,
                content_digest, rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.document_id)
    .bind(revision)
    .bind(input.revision.base_revision)
    .bind(input.revision.base_revision_id.as_deref())
    .bind(&input.revision.lifecycle)
    .bind(&input.revision.schema_version)
    .bind(&input.revision.render_version)
    .bind(&input.revision.content_json)
    .bind(&input.revision.rendered_view)
    .bind(&input.revision.change_summary)
    .bind(&input.revision.author_type)
    .bind(input.revision.author_id.as_deref())
    .bind(&input.revision.source_refs_json)
    .bind(&input.revision.content_digest)
    .bind(&input.revision.rendered_digest)
    .bind(&input.revision.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_draft_revision_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.created_at)
    .bind(&input.revision.document_id)
    .bind(input.revision.expected_document_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            input.revision.author_type.clone(),
            input.revision.author_id.clone(),
            input.revision.id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.document.revision_created".to_owned(),
        entity_type: "project_document_revision".to_owned(),
        entity_id: input.revision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: project_id,
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-document-revision-created:{}",
            input.revision.id
        )),
        payload_json: serde_json::json!({
            "document_id": input.revision.document_id.clone(),
            "revision_id": input.revision.id.clone(),
            "revision": revision,
            "lifecycle": input.revision.lifecycle.clone(),
            "content_digest": input.revision.content_digest.clone(),
            "rendered_digest": input.revision.rendered_digest.clone(),
        })
        .to_string(),
        created_at: input.revision.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(
        db,
        &mut tx,
        &event.id,
        input.command_receipt,
        input.action_execution,
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_revision(row)
}
pub(super) async fn get_project_document_revision(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectDocumentRevisionRecord>> {
    select_one(
        "SELECT * FROM project_document_revision WHERE id = ?",
        db.pool(),
        id,
        map_document_revision,
    )
    .await
}
pub(super) async fn list_project_document_revisions(
    db: &SqliteDb,
    document_id: &str,
) -> Result<Vec<ProjectDocumentRevisionRecord>> {
    sqlx::query(
        "SELECT * FROM project_document_revision
             WHERE document_id = ? ORDER BY revision ASC, id ASC",
    )
    .bind(document_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_document_revision)
    .collect()
}
pub(super) async fn approve_project_document(
    db: &SqliteDb,
    input: ApproveProjectDocument,
) -> Result<ProjectDocumentApprovalRecord> {
    if input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(existing) =
        sqlx::query("SELECT * FROM project_document_approval WHERE idempotency_key = ?")
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(map_document_approval)
            .transpose()?
    {
        if existing.document_id != input.document_id
            || existing.revision_id != input.revision_id
            || existing.content_digest != input.content_digest
            || existing.rendered_digest != input.rendered_digest
            || existing.principal_type != input.principal_type
            || existing.principal_id != input.principal_id
            || existing.authorization_basis != input.authorization_basis
            || existing.authorization_action != input.authorization_action
            || existing.authorization_occurred_at != input.authorization_occurred_at
            || existing.explicit_event != input.explicit_event
        {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let document = sqlx::query(
        "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
    )
    .bind(&input.document_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let document_version: i64 = document.try_get("version")?;
    if document_version != input.expected_document_version {
        return Err(DbError::VersionConflict);
    }
    let target = sqlx::query(
        "SELECT lifecycle FROM project_document_revision
             WHERE id = ? AND document_id = ? AND content_digest = ?
               AND rendered_digest = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.document_id)
    .bind(&input.content_digest)
    .bind(&input.rendered_digest)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let target_lifecycle: String = target.try_get("lifecycle")?;
    if matches!(
        target_lifecycle.as_str(),
        "rejected" | "withdrawn" | "superseded"
    ) {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'superseded'
             WHERE document_id = ? AND lifecycle = 'approved' AND id != ?",
    )
    .bind(&input.document_id)
    .bind(&input.revision_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let approved = sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'approved'
             WHERE id = ? AND document_id = ? AND lifecycle != 'approved'",
    )
    .bind(&input.revision_id)
    .bind(&input.document_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if approved.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_approved_revision_id = ?, lifecycle = 'approved',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.updated_at)
    .bind(&input.document_id)
    .bind(input.expected_document_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_document_approval (
                id, document_id, revision_id, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.document_id)
    .bind(&input.revision_id)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(&input.authorization_occurred_at)
    .bind(&input.content_digest)
    .bind(&input.rendered_digest)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let project_id: String = document.try_get("project_id")?;
    DomainEventRepo::append_event_in_tx(
        db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.document.approved".to_owned(),
            entity_type: "project_document_approval".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.principal_type.clone(),
            actor_id: Some(input.principal_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id,
            correlation_id: input.id.clone(),
            causation_id: Some(input.explicit_event.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!("project-document-approved:{}", input.id)),
            payload_json: serde_json::json!({
                "document_id": input.document_id.clone(),
                "revision_id": input.revision_id.clone(),
                "approval_id": input.id.clone(),
                "content_digest": input.content_digest.clone(),
                "rendered_digest": input.rendered_digest.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        },
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_approval(row)
}
pub(super) async fn approve_project_document_command(
    db: &SqliteDb,
    input: ApproveProjectDocumentCommand,
) -> Result<ProjectDocumentApprovalRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[
                ("document_id", input.approval.document_id.as_str()),
                ("revision_id", input.approval.revision_id.as_str()),
            ],
        )?;
        let approval_id = command_outcome_string(&receipt, "approval_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT a.*, d.project_id
                 FROM project_document_approval a
                 JOIN project_document d ON d.id = a.document_id
                 WHERE a.id = ? AND a.document_id = ? AND a.revision_id = ?",
        )
        .bind(&approval_id)
        .bind(&input.approval.document_id)
        .bind(&input.approval.revision_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: String = row.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let record = map_document_approval(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let approval = &input.approval;
    if approval.principal_type.trim().is_empty()
        || approval.principal_id.trim().is_empty()
        || approval.authorization_basis.trim().is_empty()
        || approval.authorization_action.trim().is_empty()
        || approval.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&approval.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let document = sqlx::query(
        "SELECT project_id, version, current_draft_revision_id
             FROM project_document WHERE id = ?",
    )
    .bind(&approval.document_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let project_id: String = document.try_get("project_id")?;
    validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
    let document_version: i64 = document.try_get("version")?;
    let current_draft_revision_id: Option<String> =
        document.try_get("current_draft_revision_id")?;
    if document_version != approval.expected_document_version {
        return Err(DbError::VersionConflict);
    }
    if current_draft_revision_id.as_deref() != Some(approval.revision_id.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let target = sqlx::query(
        "SELECT lifecycle FROM project_document_revision
             WHERE id = ? AND document_id = ? AND content_digest = ?
               AND rendered_digest = ?",
    )
    .bind(&approval.revision_id)
    .bind(&approval.document_id)
    .bind(&approval.content_digest)
    .bind(&approval.rendered_digest)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let target_lifecycle: String = target.try_get("lifecycle")?;
    if matches!(
        target_lifecycle.as_str(),
        "rejected" | "withdrawn" | "superseded"
    ) {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'superseded'
             WHERE document_id = ? AND lifecycle = 'approved' AND id != ?",
    )
    .bind(&approval.document_id)
    .bind(&approval.revision_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let approved = sqlx::query(
        "UPDATE project_document_revision SET lifecycle = 'approved'
             WHERE id = ? AND document_id = ? AND lifecycle != 'approved'",
    )
    .bind(&approval.revision_id)
    .bind(&approval.document_id)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if approved.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let updated = sqlx::query(
        "UPDATE project_document
             SET current_approved_revision_id = ?, lifecycle = 'approved',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&approval.revision_id)
    .bind(&approval.updated_at)
    .bind(&approval.document_id)
    .bind(approval.expected_document_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_document_approval (
                id, document_id, revision_id, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)",
    )
    .bind(&approval.id)
    .bind(&approval.document_id)
    .bind(&approval.revision_id)
    .bind(&approval.principal_type)
    .bind(&approval.principal_id)
    .bind(&approval.authorization_basis)
    .bind(&approval.authorization_action)
    .bind(&approval.explicit_event)
    .bind(&approval.authorization_occurred_at)
    .bind(&approval.content_digest)
    .bind(&approval.rendered_digest)
    .bind(&approval.idempotency_key)
    .bind(&approval.created_at)
    .bind(&approval.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            approval.principal_type.clone(),
            Some(approval.principal_id.clone()),
            approval.id.clone(),
            Some(approval.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.document.approved".to_owned(),
        entity_type: "project_document_approval".to_owned(),
        entity_id: approval.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: project_id,
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-document-approved:{}", approval.id)),
        payload_json: serde_json::json!({
            "document_id": approval.document_id.clone(),
            "revision_id": approval.revision_id.clone(),
            "approval_id": approval.id.clone(),
            "content_digest": approval.content_digest.clone(),
            "rendered_digest": approval.rendered_digest.clone(),
        })
        .to_string(),
        created_at: approval.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(
        db,
        &mut tx,
        &event.id,
        input.command_receipt,
        input.action_execution,
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_document_approval WHERE id = ?")
        .bind(&approval.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_document_approval(row)
}

mod reads;
pub use reads::{ProjectDocumentIdRead, ProjectDocumentRevisionRead};
