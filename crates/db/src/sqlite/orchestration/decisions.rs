use super::*;
fn map_decision_candidate(row: SqliteRow) -> Result<ProjectDecisionCandidateRecord> {
    Ok(ProjectDecisionCandidateRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        question: required_string(&row, "question")?,
        context_json: required_string(&row, "context_json")?,
        options_json: required_string(&row, "options_json")?,
        selected_outcome: optional_string(&row, "selected_outcome")?,
        rationale: optional_string(&row, "rationale")?,
        principal_type: optional_string(&row, "principal_type")?,
        principal_id: optional_string(&row, "principal_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        expected_project_version: row.try_get("expected_project_version")?,
        effective_decision_id: optional_string(&row, "effective_decision_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_decision(row: SqliteRow) -> Result<ProjectDecisionRecord> {
    Ok(ProjectDecisionRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        state: required_string(&row, "state")?,
        decision_class: required_string(&row, "decision_class")?,
        question: required_string(&row, "question")?,
        context_json: required_string(&row, "context_json")?,
        options_json: required_string(&row, "options_json")?,
        selected_outcome: required_string(&row, "selected_outcome")?,
        rationale: required_string(&row, "rationale")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authority_basis: required_string(&row, "authority_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        affected_records_json: required_string(&row, "affected_records_json")?,
        supersedes_decision_id: optional_string(&row, "supersedes_decision_id")?,
        created_at: required_string(&row, "created_at")?,
    })
}

pub struct EffectiveProjectDecisionSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectDecisionSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("state")?)
    }
    pub fn decision_class(&self) -> crate::Result<String> {
        Ok(self.row.try_get("decision_class")?)
    }
    pub fn question(&self) -> crate::Result<String> {
        Ok(self.row.try_get("question")?)
    }
    pub fn selected_outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("selected_outcome")?)
    }
    pub fn rationale(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rationale")?)
    }
    pub fn principal_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_type")?)
    }
    pub fn principal_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_id")?)
    }
    pub fn authority_basis(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authority_basis")?)
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
    pub fn charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn source_refs_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_refs_json")?)
    }
    pub fn affected_records_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("affected_records_json")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_decision_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectDecisionSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, state, decision_class, question, selected_outcome,
                rationale, principal_type, principal_id, authority_basis,
                authorization_action, explicit_event, authorization_occurred_at,
                charter_revision_id, source_refs_json,
                affected_records_json, created_at
         FROM project_decision
         WHERE project_id = ? AND state IN ('active', 'invalidated')
         ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectDecisionSourceRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn effective_project_decision_candidate_ids(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_decision_candidate
         WHERE project_id = ? AND lifecycle IN ('draft', 'proposed')
         ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct ProjectDecisionCommandSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl ProjectDecisionCommandSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("state")?)
    }
    pub fn decision_class(&self) -> crate::Result<String> {
        Ok(self.row.try_get("decision_class")?)
    }
    pub fn question(&self) -> crate::Result<String> {
        Ok(self.row.try_get("question")?)
    }
    pub fn context_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("context_json")?)
    }
    pub fn options_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("options_json")?)
    }
    pub fn selected_outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("selected_outcome")?)
    }
    pub fn rationale(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rationale")?)
    }
    pub fn principal_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_type")?)
    }
    pub fn principal_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("principal_id")?)
    }
    pub fn authority_basis(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authority_basis")?)
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
    pub fn charter_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn required_charter_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("charter_revision_id")?)
    }
    pub fn source_refs_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("source_refs_json")?)
    }
    pub fn affected_records_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("affected_records_json")?)
    }
    pub fn supersedes_decision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("supersedes_decision_id")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct DecisionArtifactRenderSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl DecisionArtifactRenderSourceRow {
    pub fn render_version(&self) -> crate::Result<String> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn rendered_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct DecisionGoverningDocumentSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl DecisionGoverningDocumentSourceRow {
    pub fn artifact_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("artifact_id")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
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

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct DecisionGoverningCharterSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl DecisionGoverningCharterSourceRow {
    pub fn artifact_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("artifact_id")?)
    }
    pub fn current_charter_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("current_charter_revision_id")?)
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
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
}

impl SqliteDb {
    pub async fn decision_governing_charter_source(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<DecisionGoverningCharterSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS artifact_id, p.current_charter_revision_id,
                    r.content_digest, r.render_version, r.rendered_digest,
                    r.lifecycle
             FROM project AS p
             JOIN project_charter AS c ON c.project_id = p.id
             JOIN project_charter_revision AS r
               ON r.id = p.current_charter_revision_id AND r.charter_id = c.id
             WHERE p.id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| DecisionGoverningCharterSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn decision_governing_document_sources(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<DecisionGoverningDocumentSourceRow>> {
        let rows = sqlx::query(
            "SELECT d.id AS artifact_id, r.id AS revision_id, r.content_digest,
                    r.render_version, r.rendered_digest
             FROM project_document AS d
             JOIN project_document_revision AS r
               ON r.id = d.current_approved_revision_id AND r.document_id = d.id
             WHERE d.project_id = ? AND r.lifecycle = 'approved'",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| DecisionGoverningDocumentSourceRow { row })
            .collect())
    }
}

pub struct DecisionArtifactRenderSourceInput<'a> {
    pub document_project_id: &'a str,
    pub document_id: &'a str,
    pub document_revision_id: &'a str,
    pub document_digest: &'a str,
    pub charter_project_id: &'a str,
    pub charter_id: &'a str,
    pub charter_revision_id: &'a str,
    pub charter_digest: &'a str,
    pub milestone_project_id: &'a str,
    pub milestone_id: &'a str,
    pub milestone_revision_id: &'a str,
    pub milestone_digest: &'a str,
}

impl SqliteDb {
    pub async fn decision_artifact_render_source(
        &self,
        input: DecisionArtifactRenderSourceInput<'_>,
    ) -> crate::Result<Option<DecisionArtifactRenderSourceRow>> {
        let rows = sqlx::query(
            "SELECT render_version, rendered_digest
             FROM project_document_revision r JOIN project_document d ON d.id = r.document_id
             WHERE d.project_id = ? AND d.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_charter_revision r JOIN project_charter c ON c.id = r.charter_id
             WHERE c.project_id = ? AND c.id = ? AND r.id = ? AND r.content_digest = ?
             UNION ALL
             SELECT r.render_version, r.rendered_digest
             FROM project_milestone_revision r JOIN project_milestone m ON m.id = r.milestone_id
             WHERE m.project_id = ? AND m.id = ? AND r.id = ? AND r.content_digest = ? LIMIT 1",
        )
        .bind(input.document_project_id)
        .bind(input.document_id)
        .bind(input.document_revision_id)
        .bind(input.document_digest)
        .bind(input.charter_project_id)
        .bind(input.charter_id)
        .bind(input.charter_revision_id)
        .bind(input.charter_digest)
        .bind(input.milestone_project_id)
        .bind(input.milestone_id)
        .bind(input.milestone_revision_id)
        .bind(input.milestone_digest)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| DecisionArtifactRenderSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn decision_governing_charter_reference(
        &self,
        revision_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ? LIMIT 1",
        )
        .bind(revision_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn project_decision_command_source(
        &self,
        decision_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectDecisionCommandSourceRow>> {
        let rows =
            sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ? LIMIT 1")
                .bind(decision_id)
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(rows.map(|row| ProjectDecisionCommandSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn decision_milestone_reference(
        &self,
        id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM project_milestone WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?)
    }
}
pub(super) async fn create_project_decision_candidate(
    db: &SqliteDb,
    input: CreateProjectDecisionCandidate,
) -> Result<ProjectDecisionCandidateRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_decision_candidate (
                id, project_id, lifecycle, question, context_json, options_json,
                selected_outcome, rationale, principal_type, principal_id,
                source_refs_json, expected_project_version, effective_decision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.lifecycle)
    .bind(&input.question)
    .bind(&input.context_json)
    .bind(&input.options_json)
    .bind(input.selected_outcome.as_deref())
    .bind(input.rationale.as_deref())
    .bind(input.principal_type.as_deref())
    .bind(input.principal_id.as_deref())
    .bind(&input.source_refs_json)
    .bind(input.expected_project_version)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let advanced = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.updated_at)
    .bind(&input.project_id)
    .bind(input.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id) = match (input.principal_type.clone(), input.principal_id.clone()) {
        (Some(kind), Some(id)) => (kind, Some(id)),
        (Some(kind), None) => (kind, None),
        (None, Some(id)) => ("system".to_owned(), Some(id)),
        (None, None) => ("system".to_owned(), None),
    };
    DomainEventRepo::append_event_in_tx(
        db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.candidate_created".to_owned(),
            entity_type: "project_decision_candidate".to_owned(),
            entity_id: input.id.clone(),
            actor_type,
            actor_id,
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id: input.id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("project-decision-candidate-created:{}", input.id)),
            payload_json: serde_json::json!({
                "project_id": input.project_id.clone(),
                "candidate_id": input.id.clone(),
                "lifecycle": input.lifecycle.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        },
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision_candidate(row)
}
pub(super) async fn create_project_decision_candidate_command(
    db: &SqliteDb,
    input: CreateProjectDecisionCandidateCommand,
) -> Result<ProjectDecisionCandidateRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("project_id", input.candidate.project_id.as_str())],
        )?;
        let candidate_id = command_outcome_string(&receipt, "candidate_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT c.*, p.id AS project_exists
                 FROM project_decision_candidate c
                 JOIN project p ON p.id = c.project_id
                 WHERE c.id = ? AND c.project_id = ?",
        )
        .bind(&candidate_id)
        .bind(&input.candidate.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.candidate.project_id,
        )?;
        let record = map_decision_candidate(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let candidate = &input.candidate;
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &candidate.project_id,
    )?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&candidate.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != candidate.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_decision_candidate (
                id, project_id, lifecycle, question, context_json, options_json,
                selected_outcome, rationale, principal_type, principal_id,
                source_refs_json, expected_project_version, effective_decision_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 1, ?, ?)",
    )
    .bind(&candidate.id)
    .bind(&candidate.project_id)
    .bind(&candidate.lifecycle)
    .bind(&candidate.question)
    .bind(&candidate.context_json)
    .bind(&candidate.options_json)
    .bind(candidate.selected_outcome.as_deref())
    .bind(candidate.rationale.as_deref())
    .bind(candidate.principal_type.as_deref())
    .bind(candidate.principal_id.as_deref())
    .bind(&candidate.source_refs_json)
    .bind(candidate.expected_project_version)
    .bind(&candidate.created_at)
    .bind(&candidate.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let advanced = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&candidate.updated_at)
    .bind(&candidate.project_id)
    .bind(candidate.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let default_actor_type = candidate
        .principal_type
        .clone()
        .unwrap_or_else(|| "system".to_owned());
    let default_actor_id = candidate.principal_id.clone();
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            default_actor_type,
            default_actor_id,
            candidate.id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.decision.candidate_created".to_owned(),
        entity_type: "project_decision_candidate".to_owned(),
        entity_id: candidate.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: candidate.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-decision-candidate-created:{}",
            candidate.id
        )),
        payload_json: serde_json::json!({
            "project_id": candidate.project_id.clone(),
            "candidate_id": candidate.id.clone(),
            "lifecycle": candidate.lifecycle.clone(),
        })
        .to_string(),
        created_at: candidate.created_at.clone(),
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
    let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
        .bind(&candidate.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision_candidate(row)
}
pub(super) async fn get_project_decision_candidate(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectDecisionCandidateRecord>> {
    select_one(
        "SELECT * FROM project_decision_candidate WHERE id = ?",
        db.pool(),
        id,
        map_decision_candidate,
    )
    .await
}
pub(super) async fn list_project_decision_candidates(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ProjectDecisionCandidateRecord>> {
    sqlx::query(
        "SELECT * FROM project_decision_candidate
             WHERE project_id = ? ORDER BY created_at ASC, id ASC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_decision_candidate)
    .collect()
}
pub(super) async fn append_project_decision(
    db: &SqliteDb,
    input: CreateProjectDecision,
) -> Result<ProjectDecisionRecord> {
    if input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authority_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    if let Some(supersedes_id) = input.supersedes_decision_id.as_deref() {
        let belongs: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_decision
                 WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(supersedes_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if belongs.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_decision (
                id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.state)
    .bind(&input.decision_class)
    .bind(&input.question)
    .bind(&input.context_json)
    .bind(&input.options_json)
    .bind(&input.selected_outcome)
    .bind(&input.rationale)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authority_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(&input.authorization_occurred_at)
    .bind(input.charter_revision_id.as_deref())
    .bind(&input.source_refs_json)
    .bind(&input.affected_records_json)
    .bind(input.supersedes_decision_id.as_deref())
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let advanced = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.created_at)
    .bind(&input.project_id)
    .bind(input.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    DomainEventRepo::append_event_in_tx(
        db,
        &mut tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "project.decision.created".to_owned(),
            entity_type: "project_decision".to_owned(),
            entity_id: input.id.clone(),
            actor_type: input.principal_type.clone(),
            actor_id: Some(input.principal_id.clone()),
            scope_type: "project".to_owned(),
            scope_id: input.project_id.clone(),
            correlation_id: input.id.clone(),
            causation_id: Some(input.explicit_event.clone()),
            causation_depth: 0,
            dedupe_key: Some(format!("project-decision-created:{}", input.id)),
            payload_json: serde_json::json!({
                "project_id": input.project_id.clone(),
                "decision_id": input.id.clone(),
                "state": input.state.clone(),
                "decision_class": input.decision_class.clone(),
                "supersedes_decision_id": input.supersedes_decision_id.clone(),
                "charter_revision_id": input.charter_revision_id.clone(),
            })
            .to_string(),
            created_at: input.created_at.clone(),
        },
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision(row)
}
pub(super) async fn append_project_decision_command(
    db: &SqliteDb,
    input: AppendProjectDecisionCommand,
) -> Result<ProjectDecisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("project_id", input.decision.project_id.as_str())],
        )?;
        let decision_id = command_outcome_string(&receipt, "decision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT * FROM project_decision
                 WHERE id = ? AND project_id = ?",
        )
        .bind(&decision_id)
        .bind(&input.decision.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.decision.project_id,
        )?;
        let record = map_decision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let decision = &input.decision;
    if decision.principal_type.trim().is_empty()
        || decision.principal_id.trim().is_empty()
        || decision.authority_basis.trim().is_empty()
        || decision.authorization_action.trim().is_empty()
        || decision.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&decision.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &decision.project_id,
    )?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&decision.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != decision.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    if let Some(supersedes_id) = decision.supersedes_decision_id.as_deref() {
        let belongs: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_decision
                 WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(supersedes_id)
        .bind(&decision.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if belongs.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_decision (
                id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
    )
    .bind(&decision.id)
    .bind(&decision.project_id)
    .bind(&decision.state)
    .bind(&decision.decision_class)
    .bind(&decision.question)
    .bind(&decision.context_json)
    .bind(&decision.options_json)
    .bind(&decision.selected_outcome)
    .bind(&decision.rationale)
    .bind(&decision.principal_type)
    .bind(&decision.principal_id)
    .bind(&decision.authority_basis)
    .bind(&decision.authorization_action)
    .bind(&decision.explicit_event)
    .bind(&decision.authorization_occurred_at)
    .bind(decision.charter_revision_id.as_deref())
    .bind(&decision.source_refs_json)
    .bind(&decision.affected_records_json)
    .bind(decision.supersedes_decision_id.as_deref())
    .bind(&decision.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let advanced = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&decision.created_at)
    .bind(&decision.project_id)
    .bind(decision.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            decision.principal_type.clone(),
            Some(decision.principal_id.clone()),
            decision.id.clone(),
            Some(decision.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.decision.created".to_owned(),
        entity_type: "project_decision".to_owned(),
        entity_id: decision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: decision.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-decision-created:{}", decision.id)),
        payload_json: serde_json::json!({
            "project_id": decision.project_id.clone(),
            "decision_id": decision.id.clone(),
            "state": decision.state.clone(),
            "decision_class": decision.decision_class.clone(),
            "supersedes_decision_id": decision.supersedes_decision_id.clone(),
            "charter_revision_id": decision.charter_revision_id.clone(),
        })
        .to_string(),
        created_at: decision.created_at.clone(),
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
    let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
        .bind(&decision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision(row)
}
pub(super) async fn approve_project_decision_candidate_command(
    db: &SqliteDb,
    input: ApproveProjectDecisionCandidateCommand,
) -> Result<ProjectDecisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("candidate_id", input.candidate_id.as_str())],
        )?;
        let decision_id = command_outcome_string(&receipt, "decision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let candidate_exists: Option<String> = sqlx::query_scalar(
            "SELECT id FROM project_decision_candidate
                 WHERE id = ? AND project_id = ? AND effective_decision_id = ?",
        )
        .bind(&input.candidate_id)
        .bind(&input.decision.project_id)
        .bind(&decision_id)
        .fetch_optional(&mut *tx)
        .await?;
        if candidate_exists.is_none() {
            return Err(DbError::IdempotencyConflict);
        }
        let row = sqlx::query("SELECT * FROM project_decision WHERE id = ? AND project_id = ?")
            .bind(&decision_id)
            .bind(&input.decision.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(
            input.command_receipt.as_ref(),
            "project",
            &input.decision.project_id,
        )?;
        let record = map_decision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let decision = &input.decision;
    if decision.principal_type.trim().is_empty()
        || decision.principal_id.trim().is_empty()
        || decision.authority_basis.trim().is_empty()
        || decision.authorization_action.trim().is_empty()
        || decision.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&decision.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &decision.project_id,
    )?;
    let project =
        sqlx::query("SELECT version, current_charter_revision_id FROM project WHERE id = ?")
            .bind(&decision.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != decision.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    let candidate = sqlx::query(
        "SELECT * FROM project_decision_candidate
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.candidate_id)
    .bind(&decision.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let candidate_lifecycle: String = candidate.try_get("lifecycle")?;
    if !matches!(candidate_lifecycle.as_str(), "draft" | "proposed") {
        return Err(DbError::VersionConflict);
    }
    let candidate_version: i64 = candidate.try_get("version")?;
    if candidate_version != input.expected_candidate_version {
        return Err(DbError::VersionConflict);
    }
    let candidate_question: String = candidate.try_get("question")?;
    let candidate_context_json: String = candidate.try_get("context_json")?;
    let candidate_options_json: String = candidate.try_get("options_json")?;
    let candidate_selected_outcome: Option<String> = candidate.try_get("selected_outcome")?;
    let candidate_rationale: Option<String> = candidate.try_get("rationale")?;
    let candidate_source_refs_json: String = candidate.try_get("source_refs_json")?;
    if candidate_question != decision.question
        || candidate_context_json != decision.context_json
        || candidate_options_json != decision.options_json
        || candidate_selected_outcome.as_deref() != Some(decision.selected_outcome.as_str())
        || candidate_rationale.as_deref() != Some(decision.rationale.as_str())
        || candidate_source_refs_json != decision.source_refs_json
    {
        return Err(DbError::VersionConflict);
    }
    let context: serde_json::Value = serde_json::from_str(&candidate_context_json)
        .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
    if let Some(context_class) = context.get("decision_class").and_then(|v| v.as_str()) {
        if context_class != decision.decision_class {
            return Err(DbError::VersionConflict);
        }
    }
    let context_supersedes = context
        .get("supersedes_decision_id")
        .and_then(|v| v.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let context_invalidates = context
        .get("invalidates_decision_id")
        .and_then(|v| v.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    if context_supersedes.is_some() && context_invalidates.is_some() {
        return Err(DbError::VersionConflict);
    }
    let expected_state = if context_invalidates.is_some() {
        "invalidated"
    } else {
        "active"
    };
    if decision.state != expected_state
        || decision.supersedes_decision_id
            != context_supersedes.clone().or(context_invalidates.clone())
    {
        return Err(DbError::VersionConflict);
    }
    if let Some(target_id) = decision.supersedes_decision_id.as_deref() {
        let target_exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM project_decision WHERE id = ? AND project_id = ?")
                .bind(target_id)
                .bind(&decision.project_id)
                .fetch_optional(&mut *tx)
                .await?;
        if target_exists.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_decision (
                id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, authorization_action, explicit_event,
                authorization_occurred_at, charter_revision_id,
                source_refs_json, affected_records_json, supersedes_decision_id,
                created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
    )
    .bind(&decision.id)
    .bind(&decision.project_id)
    .bind(&decision.state)
    .bind(&decision.decision_class)
    .bind(&decision.question)
    .bind(&decision.context_json)
    .bind(&decision.options_json)
    .bind(&decision.selected_outcome)
    .bind(&decision.rationale)
    .bind(&decision.principal_type)
    .bind(&decision.principal_id)
    .bind(&decision.authority_basis)
    .bind(&decision.authorization_action)
    .bind(&decision.explicit_event)
    .bind(&decision.authorization_occurred_at)
    .bind(decision.charter_revision_id.as_deref())
    .bind(&decision.source_refs_json)
    .bind(&decision.affected_records_json)
    .bind(decision.supersedes_decision_id.as_deref())
    .bind(&decision.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let candidate_update = sqlx::query(
        "UPDATE project_decision_candidate
             SET lifecycle = 'approved', effective_decision_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('draft', 'proposed')",
    )
    .bind(&decision.id)
    .bind(&decision.created_at)
    .bind(&input.candidate_id)
    .bind(&decision.project_id)
    .bind(input.expected_candidate_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if candidate_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let project_update = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&decision.created_at)
    .bind(&decision.project_id)
    .bind(decision.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if project_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            decision.principal_type.clone(),
            Some(decision.principal_id.clone()),
            decision.id.clone(),
            Some(decision.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.decision.approved".to_owned(),
        entity_type: "project_decision".to_owned(),
        entity_id: decision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: decision.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-decision-approved:{}:{}",
            input.candidate_id, decision.id
        )),
        payload_json: serde_json::json!({
            "project_id": decision.project_id.clone(),
            "candidate_id": input.candidate_id.clone(),
            "decision_id": decision.id.clone(),
            "expected_project_version": decision.expected_project_version,
            "principal_id": decision.principal_id.clone(),
            "authorization_event_id": decision.explicit_event.clone(),
            "authorization_basis": decision.authority_basis.clone(),
        })
        .to_string(),
        created_at: decision.created_at.clone(),
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
    let row = sqlx::query("SELECT * FROM project_decision WHERE id = ?")
        .bind(&decision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision(row)
}
pub(super) async fn reject_project_decision_candidate_command(
    db: &SqliteDb,
    input: RejectProjectDecisionCandidateCommand,
) -> Result<ProjectDecisionCandidateRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("candidate_id", input.candidate_id.as_str())],
        )?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT * FROM project_decision_candidate
                 WHERE id = ? AND project_id = ? AND lifecycle = 'rejected'",
        )
        .bind(&input.candidate_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        let record = map_decision_candidate(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    if input.reason.trim().is_empty()
        || input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    let candidate = sqlx::query(
        "SELECT * FROM project_decision_candidate
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.candidate_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let lifecycle: String = candidate.try_get("lifecycle")?;
    if !matches!(lifecycle.as_str(), "draft" | "proposed") {
        return Err(DbError::VersionConflict);
    }
    let candidate_version: i64 = candidate.try_get("version")?;
    if candidate_version != input.expected_candidate_version {
        return Err(DbError::VersionConflict);
    }
    let mut context: serde_json::Value =
        serde_json::from_str(&candidate.try_get::<String, _>("context_json")?)
            .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
    if !context.is_object() {
        context = serde_json::json!({"summary": context});
    }
    context["rejection_reason"] = serde_json::Value::String(input.reason.clone());
    let context_json = serde_json::to_string(&context)
        .map_err(|_| DbError::Check("Decision candidate context is invalid".to_owned()))?;
    let updated = sqlx::query(
        "UPDATE project_decision_candidate
             SET lifecycle = 'rejected', context_json = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('draft', 'proposed')",
    )
    .bind(&context_json)
    .bind(&input.updated_at)
    .bind(&input.candidate_id)
    .bind(&input.project_id)
    .bind(input.expected_candidate_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let project_update = sqlx::query(
        "UPDATE project SET version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.updated_at)
    .bind(&input.project_id)
    .bind(input.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if project_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            input.principal_type.clone(),
            Some(input.principal_id.clone()),
            input.candidate_id.clone(),
            Some(input.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.decision.candidate_rejected".to_owned(),
        entity_type: "project_decision_candidate".to_owned(),
        entity_id: input.candidate_id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-decision-rejected:{}:{}",
            input.candidate_id,
            input.command_receipt.as_ref().map_or_else(
                || input.explicit_event.clone(),
                |receipt| receipt.idempotency_key.clone()
            )
        )),
        payload_json: serde_json::json!({
            "project_id": input.project_id.clone(),
            "candidate_id": input.candidate_id.clone(),
            "reason": input.reason.clone(),
            "expected_project_version": input.expected_project_version,
            "principal_id": input.principal_id.clone(),
            "authorization_event_id": input.explicit_event.clone(),
            "authorization_basis": input.authorization_basis.clone(),
        })
        .to_string(),
        created_at: input.updated_at.clone(),
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
    let row = sqlx::query("SELECT * FROM project_decision_candidate WHERE id = ?")
        .bind(&input.candidate_id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_decision_candidate(row)
}
pub(super) async fn list_effective_project_decisions(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ProjectDecisionRecord>> {
    sqlx::query(
        "SELECT * FROM project_decision
             WHERE project_id = ? AND state = 'active'
             ORDER BY created_at ASC, id ASC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_decision)
    .collect()
}

mod reads;
pub use reads::{ProjectDecisionCandidateRead, ProjectDecisionRead};

pub struct InsertMilestoneWaiverDecisionInput<'a> {
    pub waiver_id: &'a str,
    pub project_id: &'a str,
    pub check_id: &'a str,
    pub reason: &'a str,
    pub user_id: &'a str,
    pub authorization_basis: &'a str,
    pub authorization_action: &'a str,
    pub explicit_event: &'a str,
    pub authorization_occurred_at: &'a str,
    pub charter_revision_id: &'a str,
    pub affected_records_json: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_milestone_waiver_decision_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: InsertMilestoneWaiverDecisionInput<'_>,
    ) -> Result<()> {
        sqlx::query(
        "INSERT INTO project_decision (
            id, project_id, state, decision_class, question, context_json,
            selected_outcome, rationale, principal_type, principal_id,
            authority_basis, authorization_action, explicit_event,
            authorization_occurred_at, charter_revision_id,
            source_refs_json, affected_records_json, created_at
         ) VALUES (?, ?, 'active', 'waiver', ?, '{}', 'waived', ?, 'user', ?, ?, ?, ?, ?, ?, '[]', ?, ?)",
    )
    .bind(input.waiver_id)
    .bind(input.project_id)
    .bind(format!("Waive milestone check {}", input.check_id))
    .bind(input.reason)
    .bind(input.user_id)
    .bind(input.authorization_basis)
    .bind(input.authorization_action)
    .bind(input.explicit_event)
    .bind(input.authorization_occurred_at)
    .bind(input.charter_revision_id)
    .bind(input.affected_records_json)
    .bind(input.created_at)
    .execute(connection)
    .await?;
        Ok(())
    }
}
