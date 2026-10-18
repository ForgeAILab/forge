use super::*;
fn map_canonical_conflict(row: SqliteRow) -> Result<ProjectCanonicalConflictRecord> {
    Ok(ProjectCanonicalConflictRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        domain: required_string(&row, "domain")?,
        governing_record_type: required_string(&row, "governing_record_type")?,
        governing_record_id: required_string(&row, "governing_record_id")?,
        governing_record_revision: required_string(&row, "governing_record_revision")?,
        governing_record_digest: required_string(&row, "governing_record_digest")?,
        conflicting_record_type: required_string(&row, "conflicting_record_type")?,
        conflicting_record_id: required_string(&row, "conflicting_record_id")?,
        conflicting_record_revision: required_string(&row, "conflicting_record_revision")?,
        conflicting_record_digest: required_string(&row, "conflicting_record_digest")?,
        affected_paths_json: required_string(&row, "affected_paths_json")?,
        conflict_code: required_string(&row, "conflict_code")?,
        description: required_string(&row, "description")?,
        detected_by_type: required_string(&row, "detected_by_type")?,
        detected_by_id: optional_string(&row, "detected_by_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_reconciliation(row: SqliteRow) -> Result<ProjectReconciliationRecord> {
    Ok(ProjectReconciliationRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        conflict_id: required_string(&row, "conflict_id")?,
        record_type: required_string(&row, "record_type")?,
        record_id: required_string(&row, "record_id")?,
        record_revision: required_string(&row, "record_revision")?,
        record_digest: required_string(&row, "record_digest")?,
        governing_record_type: required_string(&row, "governing_record_type")?,
        governing_record_id: required_string(&row, "governing_record_id")?,
        governing_record_revision: required_string(&row, "governing_record_revision")?,
        governing_record_digest: required_string(&row, "governing_record_digest")?,
        state: required_string(&row, "state")?,
        current_resolution_id: optional_string(&row, "current_resolution_id")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_reconciliation_resolution(row: SqliteRow) -> Result<ProjectReconciliationResolutionRecord> {
    Ok(ProjectReconciliationResolutionRecord {
        id: required_string(&row, "id")?,
        reconciliation_id: required_string(&row, "reconciliation_id")?,
        action: required_string(&row, "action")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        explicit_event: required_string(&row, "explicit_event")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        reason: required_string(&row, "reason")?,
        occurred_at: required_string(&row, "occurred_at")?,
        replacement_ref_type: optional_string(&row, "replacement_ref_type")?,
        replacement_ref_id: optional_string(&row, "replacement_ref_id")?,
        replacement_ref_revision: optional_string(&row, "replacement_ref_revision")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectReconciliationSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectReconciliationSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn conflict_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflict_id")?)
    }
    pub fn record_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_type")?)
    }
    pub fn record_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_id")?)
    }
    pub fn record_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_revision")?)
    }
    pub fn record_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_digest")?)
    }
    pub fn state(&self) -> crate::Result<String> {
        Ok(self.row.try_get("state")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectCanonicalConflictSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectCanonicalConflictSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn domain(&self) -> crate::Result<String> {
        Ok(self.row.try_get("domain")?)
    }
    pub fn governing_record_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_type")?)
    }
    pub fn governing_record_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_id")?)
    }
    pub fn governing_record_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_revision")?)
    }
    pub fn governing_record_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_digest")?)
    }
    pub fn conflicting_record_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflicting_record_type")?)
    }
    pub fn conflicting_record_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflicting_record_id")?)
    }
    pub fn conflicting_record_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflicting_record_revision")?)
    }
    pub fn conflicting_record_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflicting_record_digest")?)
    }
    pub fn affected_paths_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("affected_paths_json")?)
    }
    pub fn conflict_code(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflict_code")?)
    }
    pub fn description(&self) -> crate::Result<String> {
        Ok(self.row.try_get("description")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_required_reconciliation_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectReconciliationSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, conflict_id, record_type, record_id, record_revision,
                record_digest, state, version, updated_at
         FROM project_reconciliation_record
         WHERE project_id = ? AND state = 'required'
         ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectReconciliationSourceRow { row })
            .collect())
    }
}

impl SqliteDb {
    pub async fn effective_project_canonical_conflict_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectCanonicalConflictSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, domain, governing_record_type, governing_record_id,
                governing_record_revision, governing_record_digest,
                conflicting_record_type, conflicting_record_id,
                conflicting_record_revision, conflicting_record_digest,
                affected_paths_json, conflict_code, description, created_at
         FROM project_canonical_conflict
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectCanonicalConflictSourceRow { row })
            .collect())
    }
}

pub struct ExecutionSetupReconciliationConflictRow {
    row: SqliteRow,
}
impl ExecutionSetupReconciliationConflictRow {
    pub fn reconciliation_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("reconciliation_id")?)
    }
    pub fn conflict_code(&self) -> crate::Result<String> {
        Ok(self.row.try_get("conflict_code")?)
    }
    pub fn description(&self) -> crate::Result<String> {
        Ok(self.row.try_get("description")?)
    }
    pub fn record_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_type")?)
    }
    pub fn record_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("record_id")?)
    }
    pub fn governing_record_type(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_type")?)
    }
    pub fn governing_record_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("governing_record_id")?)
    }
    pub fn affected_paths_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("affected_paths_json")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn execution_setup_reconciliation_conflict_sources(
        &self,
        project_id: &str,
    ) -> crate::Result<Vec<ExecutionSetupReconciliationConflictRow>> {
        let rows = sqlx::query(
            "SELECT r.id AS reconciliation_id, c.conflict_code, c.description,
                r.record_type, r.record_id,
                r.governing_record_type, r.governing_record_id,
                c.affected_paths_json, r.updated_at
         FROM project_reconciliation_record r
         JOIN project_canonical_conflict c ON c.id = r.conflict_id
         WHERE r.project_id = ? AND r.state = 'required'
         ORDER BY r.updated_at DESC, r.id DESC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| ExecutionSetupReconciliationConflictRow { row })
            .collect())
    }
}
pub(super) async fn create_project_canonical_conflict(
    db: &SqliteDb,
    input: CreateProjectCanonicalConflict,
) -> Result<ProjectCanonicalConflictRecord> {
    if input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(existing) =
        sqlx::query("SELECT * FROM project_canonical_conflict WHERE idempotency_key = ?")
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(map_canonical_conflict)
            .transpose()?
    {
        let same = existing.project_id == input.project_id
            && existing.domain == input.domain
            && existing.governing_record_type == input.governing_record_type
            && existing.governing_record_id == input.governing_record_id
            && existing.governing_record_revision == input.governing_record_revision
            && existing.governing_record_digest == input.governing_record_digest
            && existing.conflicting_record_type == input.conflicting_record_type
            && existing.conflicting_record_id == input.conflicting_record_id
            && existing.conflicting_record_revision == input.conflicting_record_revision
            && existing.conflicting_record_digest == input.conflicting_record_digest
            && existing.affected_paths_json == input.affected_paths_json
            && existing.conflict_code == input.conflict_code
            && existing.description == input.description
            && existing.detected_by_type == input.detected_by_type
            && existing.detected_by_id == input.detected_by_id
            && existing.authorization_basis == input.authorization_basis
            && existing.authorization_action == input.authorization_action
            && existing.explicit_event == input.explicit_event
            && existing.authorization_occurred_at == input.authorization_occurred_at;
        if !same {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let row = sqlx::query(
        "INSERT INTO project_canonical_conflict (
                id, project_id, domain, governing_record_type,
                governing_record_id, governing_record_revision,
                governing_record_digest, conflicting_record_type,
                conflicting_record_id, conflicting_record_revision,
                conflicting_record_digest, affected_paths_json, conflict_code,
                description, detected_by_type, detected_by_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING *",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.domain)
    .bind(&input.governing_record_type)
    .bind(&input.governing_record_id)
    .bind(&input.governing_record_revision)
    .bind(&input.governing_record_digest)
    .bind(&input.conflicting_record_type)
    .bind(&input.conflicting_record_id)
    .bind(&input.conflicting_record_revision)
    .bind(&input.conflicting_record_digest)
    .bind(&input.affected_paths_json)
    .bind(&input.conflict_code)
    .bind(&input.description)
    .bind(&input.detected_by_type)
    .bind(input.detected_by_id.as_deref())
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(&input.authorization_occurred_at)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let record = map_canonical_conflict(row)?;
    tx.commit().await?;
    Ok(record)
}
pub(super) async fn get_project_canonical_conflict(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectCanonicalConflictRecord>> {
    select_one(
        "SELECT * FROM project_canonical_conflict WHERE id = ?",
        db.pool(),
        id,
        map_canonical_conflict,
    )
    .await
}
pub(super) async fn list_project_canonical_conflicts(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ProjectCanonicalConflictRecord>> {
    sqlx::query(
        "SELECT * FROM project_canonical_conflict
             WHERE project_id = ? ORDER BY created_at DESC, id DESC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_canonical_conflict)
    .collect()
}
pub(super) async fn create_project_reconciliation(
    db: &SqliteDb,
    input: CreateProjectReconciliation,
) -> Result<ProjectReconciliationRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let row = sqlx::query(
        "INSERT INTO project_reconciliation_record (
                id, project_id, conflict_id, record_type, record_id,
                record_revision, record_digest, governing_record_type,
                governing_record_id, governing_record_revision,
                governing_record_digest, state, current_resolution_id, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'required', NULL, 1, ?, ?)
             RETURNING *",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.conflict_id)
    .bind(&input.record_type)
    .bind(&input.record_id)
    .bind(&input.record_revision)
    .bind(&input.record_digest)
    .bind(&input.governing_record_type)
    .bind(&input.governing_record_id)
    .bind(&input.governing_record_revision)
    .bind(&input.governing_record_digest)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let record = map_reconciliation(row)?;
    tx.commit().await?;
    Ok(record)
}
pub(super) async fn get_project_reconciliation(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectReconciliationRecord>> {
    select_one(
        "SELECT * FROM project_reconciliation_record WHERE id = ?",
        db.pool(),
        id,
        map_reconciliation,
    )
    .await
}
pub(super) async fn list_project_reconciliations(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ProjectReconciliationRecord>> {
    sqlx::query(
        "SELECT * FROM project_reconciliation_record
             WHERE project_id = ? ORDER BY updated_at DESC, id DESC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_reconciliation)
    .collect()
}
pub(super) async fn resolve_project_reconciliation(
    db: &SqliteDb,
    input: ResolveProjectReconciliation,
) -> Result<ProjectReconciliationRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replacement_required = matches!(input.action.as_str(), "revised" | "superseded");
    let replacement_present = input.replacement_ref_type.is_some()
        || input.replacement_ref_id.is_some()
        || input.replacement_ref_revision.is_some();
    if !matches!(
        input.action.as_str(),
        "retained" | "revised" | "cancelled" | "superseded" | "invalidated"
    ) || input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || input.authorization_occurred_at.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
        || input.reason.trim().is_empty()
        || input.occurred_at.trim().is_empty()
        || !valid_authorization_timestamp(&input.occurred_at)
        || (replacement_required
            && (input
                .replacement_ref_type
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
                || input
                    .replacement_ref_id
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .is_empty()))
        || (!replacement_required && replacement_present)
    {
        return Err(DbError::VersionConflict);
    }
    if let Some(existing) = sqlx::query(
        "SELECT r.* FROM project_reconciliation_record r
             JOIN project_reconciliation_resolution resolution
               ON resolution.id = r.current_resolution_id
             WHERE resolution.idempotency_key = ?",
    )
    .bind(&input.idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    .map(map_reconciliation)
    .transpose()?
    {
        let resolution = sqlx::query(
            "SELECT action, principal_type, principal_id,
                        authorization_basis, authorization_action, explicit_event,
                        authorization_occurred_at, reason, occurred_at,
                        replacement_ref_type, replacement_ref_id, replacement_ref_revision
                 FROM project_reconciliation_resolution
                 WHERE idempotency_key = ?",
        )
        .bind(&input.idempotency_key)
        .fetch_one(&mut *tx)
        .await?;
        let same = existing.id == input.id
            && existing.state == input.action
            && resolution.try_get::<String, _>("action")? == input.action
            && resolution.try_get::<String, _>("principal_type")? == input.principal_type
            && resolution.try_get::<String, _>("principal_id")? == input.principal_id
            && resolution.try_get::<String, _>("authorization_basis")? == input.authorization_basis
            && resolution.try_get::<String, _>("authorization_action")?
                == input.authorization_action
            && resolution.try_get::<String, _>("explicit_event")? == input.explicit_event
            && resolution.try_get::<String, _>("authorization_occurred_at")?
                == input.authorization_occurred_at
            && resolution.try_get::<String, _>("reason")? == input.reason
            && resolution.try_get::<String, _>("occurred_at")? == input.occurred_at
            && resolution.try_get::<Option<String>, _>("replacement_ref_type")?
                == input.replacement_ref_type
            && resolution.try_get::<Option<String>, _>("replacement_ref_id")?
                == input.replacement_ref_id
            && resolution.try_get::<Option<String>, _>("replacement_ref_revision")?
                == input.replacement_ref_revision;
        if !same {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let current = sqlx::query(
        "SELECT * FROM project_reconciliation_record
             WHERE id = ? AND state = 'required'",
    )
    .bind(&input.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let current_version: i64 = current.try_get("version")?;
    if current_version != input.expected_version {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_reconciliation_resolution (
                id, reconciliation_id, action, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, reason, occurred_at, idempotency_key,
                replacement_ref_type, replacement_ref_id, replacement_ref_revision,
                created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.resolution_id)
    .bind(&input.id)
    .bind(&input.action)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(&input.authorization_occurred_at)
    .bind(&input.reason)
    .bind(&input.occurred_at)
    .bind(&input.idempotency_key)
    .bind(&input.replacement_ref_type)
    .bind(&input.replacement_ref_id)
    .bind(&input.replacement_ref_revision)
    .bind(&input.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let updated = sqlx::query(
        "UPDATE project_reconciliation_record
             SET state = ?, current_resolution_id = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND state = 'required' AND version = ?",
    )
    .bind(&input.action)
    .bind(&input.resolution_id)
    .bind(&input.updated_at)
    .bind(&input.id)
    .bind(input.expected_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let event = db.append_event_in_tx(&mut tx, &input.domain_event).await?;
    finalize_command_in_tx(db, &mut tx, &event.id, Some(input.command_receipt), None).await?;
    let row = sqlx::query("SELECT * FROM project_reconciliation_record WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    let record = map_reconciliation(row)?;
    tx.commit().await?;
    Ok(record)
}
pub(super) async fn get_project_reconciliation_resolution(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectReconciliationResolutionRecord>> {
    select_one(
        "SELECT * FROM project_reconciliation_resolution WHERE id = ?",
        db.pool(),
        id,
        map_reconciliation_resolution,
    )
    .await
}
