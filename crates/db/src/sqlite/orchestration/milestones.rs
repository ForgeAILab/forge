use super::*;
use std::collections::BTreeSet;
async fn materialize_milestone_check_definitions_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project_id: &str,
    milestone_id: &str,
    revision: &CreateProjectMilestoneRevision,
    checks: &[CreateProjectMilestoneCheck],
) -> Result<()> {
    if revision.lifecycle == "draft" {
        if checks.is_empty() {
            return Ok(());
        }
        return Err(DbError::VersionConflict);
    }

    let declared: Vec<serde_json::Value> = serde_json::from_str(&revision.acceptance_checks_json)
        .map_err(|error| {
        DbError::Check(format!(
            "milestone acceptance-check definition JSON is invalid: {error}"
        ))
    })?;
    let declared_ids = declared
        .iter()
        .map(|value| {
            value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    DbError::Check(
                        "milestone acceptance checks require stable non-empty ids".to_owned(),
                    )
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let supplied_ids = checks
        .iter()
        .map(|check| check.id.clone())
        .collect::<BTreeSet<_>>();
    if declared_ids.len() != declared.len()
        || supplied_ids.len() != checks.len()
        || supplied_ids != declared_ids
    {
        return Err(DbError::VersionConflict);
    }

    let declared_evidence: Vec<serde_json::Value> =
        serde_json::from_str(&revision.evidence_requirements_json).map_err(|error| {
            DbError::Check(format!(
                "milestone evidence-requirement definition JSON is invalid: {error}"
            ))
        })?;
    let mut evidence_ids = BTreeSet::new();
    let mut required_evidence_ids = BTreeSet::new();
    for value in &declared_evidence {
        let id = value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                DbError::Check(
                    "milestone evidence requirements require stable non-empty ids".to_owned(),
                )
            })?;
        if !evidence_ids.insert(id.to_owned()) {
            return Err(DbError::VersionConflict);
        }
        if value
            .get("required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            required_evidence_ids.insert(id.to_owned());
        }
    }

    for check in checks {
        if check.id.trim().is_empty()
            || check.project_id != project_id
            || check.milestone_id != milestone_id
            || check.definition_revision_id != revision.id
            || check.expected_milestone_version != revision.expected_milestone_version
            || check.check_key != check.id
            || check.description.trim().is_empty()
            // `task_validation` joins the definable kinds now that
            // `project.validation` gives it a receipt-backed result path.
            || !matches!(
                check.source_kind.as_str(),
                "manual" | "policy_waiver" | "task_validation"
            )
            || check.evidence_required != required_evidence_ids.contains(&check.id)
            || check.updated_at != revision.created_at
            || check.created_at.trim().is_empty()
        {
            return Err(DbError::VersionConflict);
        }
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT project_id, milestone_id, definition_revision_id
             FROM project_milestone_check WHERE id = ?",
        )
        .bind(&check.id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some((existing_project, existing_milestone, existing_revision)) = existing {
            if existing_project != project_id || existing_milestone != milestone_id {
                return Err(DbError::VersionConflict);
            }
            if existing_revision == revision.id {
                return Err(DbError::VersionConflict);
            }
            let updated = sqlx::query(
                "UPDATE project_milestone_check
                 SET definition_revision_id = ?, check_key = ?, description = ?, required = ?,
                     source_kind = ?, expected_result = ?, evidence_required = ?,
                     version = version + 1, current_result_id = NULL, updated_at = ?
                 WHERE id = ? AND project_id = ? AND milestone_id = ?",
            )
            .bind(&revision.id)
            .bind(&check.check_key)
            .bind(&check.description)
            .bind(check.required)
            .bind(&check.source_kind)
            .bind(&check.expected_result)
            .bind(check.evidence_required)
            .bind(&check.updated_at)
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .execute(&mut **tx)
            .await
            .map_err(orchestration_write_error)?;
            if updated.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        } else {
            sqlx::query(
                "INSERT INTO project_milestone_check (
                    id, project_id, milestone_id, definition_revision_id, check_key,
                    description, required, source_kind, expected_result,
                    evidence_required, version, current_result_id, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
            )
            .bind(&check.id)
            .bind(project_id)
            .bind(milestone_id)
            .bind(&revision.id)
            .bind(&check.check_key)
            .bind(&check.description)
            .bind(check.required)
            .bind(&check.source_kind)
            .bind(&check.expected_result)
            .bind(check.evidence_required)
            .bind(&check.created_at)
            .bind(&check.updated_at)
            .execute(&mut **tx)
            .await
            .map_err(orchestration_write_error)?;
        }
    }
    Ok(())
}

fn map_milestone(row: SqliteRow) -> Result<ProjectMilestoneRecord> {
    Ok(ProjectMilestoneRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_sequence: row.try_get("milestone_sequence")?,
        milestone_key: required_string(&row, "milestone_key")?,
        display_label: optional_string(&row, "display_label")?,
        current_definition_revision_id: optional_string(&row, "current_definition_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        blocker_reason_json: required_string(&row, "blocker_reason_json")?,
        stale_reason_json: required_string(&row, "stale_reason_json")?,
        reconciliation_reason_json: required_string(&row, "reconciliation_reason_json")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_milestone_revision(row: SqliteRow) -> Result<ProjectMilestoneRevisionRecord> {
    Ok(ProjectMilestoneRevisionRecord {
        id: required_string(&row, "id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        display_label: optional_string(&row, "display_label")?,
        outcome: required_string(&row, "outcome")?,
        included_scope_json: required_string(&row, "included_scope_json")?,
        excluded_scope_json: required_string(&row, "excluded_scope_json")?,
        charter_revision_id: optional_string(&row, "charter_revision_id")?,
        document_revisions_json: required_string(&row, "document_revisions_json")?,
        task_selection_json: required_string(&row, "task_selection_json")?,
        dependencies_json: required_string(&row, "dependencies_json")?,
        risks_json: required_string(&row, "risks_json")?,
        acceptance_checks_json: required_string(&row, "acceptance_checks_json")?,
        evidence_requirements_json: required_string(&row, "evidence_requirements_json")?,
        known_issues_json: required_string(&row, "known_issues_json")?,
        change_summary: required_string(&row, "change_summary")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        rendered_view: required_string(&row, "rendered_view")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn map_milestone_check(row: SqliteRow) -> Result<ProjectMilestoneCheckRecord> {
    Ok(ProjectMilestoneCheckRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        check_key: required_string(&row, "check_key")?,
        description: required_string(&row, "description")?,
        required: row.try_get::<i64, _>("required")? != 0,
        source_kind: required_string(&row, "source_kind")?,
        expected_result: required_string(&row, "expected_result")?,
        evidence_required: row.try_get::<i64, _>("evidence_required")? != 0,
        version: row.try_get("version")?,
        current_result_id: optional_string(&row, "current_result_id")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_milestone_result(row: SqliteRow) -> Result<ProjectMilestoneCheckResultRecord> {
    Ok(ProjectMilestoneCheckResultRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        check_id: required_string(&row, "check_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        outcome: required_string(&row, "outcome")?,
        source_kind: required_string(&row, "source_kind")?,
        source_manifest_json: required_string(&row, "source_manifest_json")?,
        input_digest: required_string(&row, "input_digest")?,
        governing_charter_revision_id: optional_string(&row, "governing_charter_revision_id")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        expected_version: row.try_get("expected_version")?,
        explicit_event: required_string(&row, "explicit_event")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn milestone_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> crate::Result<ProjectMilestoneRecord> {
    Ok(ProjectMilestoneRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        milestone_sequence: row.try_get("milestone_sequence")?,
        milestone_key: row.try_get("milestone_key")?,
        display_label: row.try_get("display_label")?,
        current_definition_revision_id: row.try_get("current_definition_revision_id")?,
        lifecycle: row.try_get("lifecycle")?,
        blocker_reason_json: row.try_get("blocker_reason_json")?,
        stale_reason_json: row.try_get("stale_reason_json")?,
        reconciliation_reason_json: row.try_get("reconciliation_reason_json")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn milestone_revision_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> crate::Result<ProjectMilestoneRevisionRecord> {
    Ok(ProjectMilestoneRevisionRecord {
        id: row.try_get("id")?,
        milestone_id: row.try_get("milestone_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: row.try_get("base_revision_id")?,
        lifecycle: row.try_get("lifecycle")?,
        display_label: row.try_get("display_label")?,
        outcome: row.try_get("outcome")?,
        included_scope_json: row.try_get("included_scope_json")?,
        excluded_scope_json: row.try_get("excluded_scope_json")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        document_revisions_json: row.try_get("document_revisions_json")?,
        task_selection_json: row.try_get("task_selection_json")?,
        dependencies_json: row.try_get("dependencies_json")?,
        risks_json: row.try_get("risks_json")?,
        acceptance_checks_json: row.try_get("acceptance_checks_json")?,
        evidence_requirements_json: row.try_get("evidence_requirements_json")?,
        known_issues_json: row.try_get("known_issues_json")?,
        change_summary: row.try_get("change_summary")?,
        schema_version: row.try_get("schema_version")?,
        render_version: row.try_get("render_version")?,
        rendered_view: row.try_get("rendered_view")?,
        content_digest: row.try_get("content_digest")?,
        rendered_digest: row.try_get("rendered_digest")?,
        author_type: row.try_get("author_type")?,
        author_id: row.try_get("author_id")?,
        source_refs_json: row.try_get("source_refs_json")?,
        created_at: row.try_get("created_at")?,
    })
}

impl SqliteDb {
    pub async fn lock_milestone_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
        expected_milestone_version: i64,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project_milestone SET version = version
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn milestone_version_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        let result = sqlx::query_scalar(
            "SELECT version FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn mark_milestone_released_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        now: &str,
        milestone_id: &str,
        project_id: &str,
        expected_milestone_version: i64,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ? WHERE id = ? AND project_id = ? AND version = ?
                 AND lifecycle IN ('ready_for_release', 'released')",
        )
        .bind(now)
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn active_milestone_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active'",
        )
        .bind(project_id)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn active_milestone_exists_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<i64>> {
        let result = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone
                 WHERE id = ? AND project_id = ? AND lifecycle = 'active'",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn project_milestone_record_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRecord>> {
        sqlx::query(
            "SELECT * FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?
        .map(milestone_record_from_row)
        .transpose()
    }
}

impl SqliteDb {
    pub async fn milestone_definition_record_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRevisionRecord>> {
        sqlx::query(
            "SELECT r.* FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.id = ? AND r.milestone_id = ? AND m.project_id = ?",
        )
        .bind(revision_id)
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?
        .map(milestone_revision_record_from_row)
        .transpose()
    }
}

impl SqliteDb {
    pub async fn milestone_lifecycle_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<String> {
        let result = sqlx::query_scalar(
            "SELECT lifecycle FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn replacement_active_milestone_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<String>> {
        let result = sqlx::query_scalar(
            "SELECT id FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active' AND id != ?
             ORDER BY milestone_sequence ASC, id ASC LIMIT 1",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn governed_milestone_task_ids_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Vec<String>> {
        let result = sqlx::query_scalar(
            "SELECT g.task_id FROM project_task_governance g
             JOIN task t ON t.id = g.task_id
             WHERE g.milestone_id = ? AND t.project_id = ?
               AND t.deleted_at IS NULL AND t.archived_at IS NULL
               AND t.status != 'cancelled'
             ORDER BY g.task_id",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_all(connection)
        .await?;
        Ok(result)
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectMilestoneSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectMilestoneSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn milestone_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_key")?)
    }
    pub fn display_label(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("display_label")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
    pub fn current_definition_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_definition_revision_id")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn blocker_reason_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("blocker_reason_json")?)
    }
    pub fn stale_reason_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("stale_reason_json")?)
    }
    pub fn reconciliation_reason_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("reconciliation_reason_json")?)
    }
    pub fn definition_digest(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("definition_digest")?)
    }
    pub fn acceptance_checks_json(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("acceptance_checks_json")?)
    }
    pub fn evidence_requirements_json(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("evidence_requirements_json")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_milestone_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectMilestoneSourceRow>> {
        let rows = sqlx::query(
            "SELECT m.id, m.milestone_key, m.display_label, m.lifecycle,
                m.current_definition_revision_id, m.version,
                m.blocker_reason_json, m.stale_reason_json,
                m.reconciliation_reason_json,
                r.content_digest AS definition_digest,
                r.acceptance_checks_json, r.evidence_requirements_json
         FROM project_milestone m
         LEFT JOIN project_milestone_revision r
           ON r.id = m.current_definition_revision_id AND r.milestone_id = m.id
         WHERE m.project_id = ?
           AND (
                m.lifecycle IN ('planned', 'active', 'ready_for_release')
                OR m.id = (SELECT primary_milestone_id FROM project WHERE id = ?)
           )
         ORDER BY m.milestone_sequence ASC, m.id ASC LIMIT ?",
        )
        .bind(project_id)
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectMilestoneSourceRow { row })
            .collect())
    }
}
pub(super) async fn create_project_milestone(
    db: &SqliteDb,
    input: CreateProjectMilestone,
) -> Result<ProjectMilestoneRecord> {
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
        "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(input.milestone_sequence)
    .bind(&input.milestone_key)
    .bind(input.display_label.as_deref())
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
    let row = sqlx::query("SELECT * FROM project_milestone WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone(row)
}
pub(super) async fn list_project_milestones(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Vec<ProjectMilestoneRecord>> {
    sqlx::query(
        "SELECT * FROM project_milestone
             WHERE project_id = ? ORDER BY milestone_sequence ASC, id ASC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_milestone)
    .collect()
}
pub(super) async fn get_project_milestone(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectMilestoneRecord>> {
    select_one(
        "SELECT * FROM project_milestone WHERE id = ?",
        db.pool(),
        id,
        map_milestone,
    )
    .await
}
pub(super) async fn create_project_milestone_revision(
    db: &SqliteDb,
    input: CreateProjectMilestoneRevision,
) -> Result<ProjectMilestoneRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let milestone = sqlx::query(
        "SELECT version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
    )
    .bind(&input.milestone_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    if milestone_version != input.expected_milestone_version {
        return Err(DbError::VersionConflict);
    }
    let current_revision_id: Option<String> =
        milestone.try_get("current_definition_revision_id")?;
    if input.base_revision > 0 {
        let Some(current_revision_id) = current_revision_id else {
            return Err(DbError::VersionConflict);
        };
        let Some(base_revision_id) = input.base_revision_id.as_deref() else {
            return Err(DbError::VersionConflict);
        };
        if current_revision_id != base_revision_id {
            return Err(DbError::VersionConflict);
        }
        let base_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone_revision
                 WHERE id = ? AND milestone_id = ? AND revision = ? LIMIT 1",
        )
        .bind(base_revision_id)
        .bind(&input.milestone_id)
        .bind(input.base_revision)
        .fetch_optional(&mut *tx)
        .await?;
        if base_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if input.base_revision_id.is_some() || current_revision_id.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_milestone_revision WHERE milestone_id = ?",
    )
    .bind(&input.milestone_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest,
                author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.milestone_id)
        .bind(revision)
        .bind(input.base_revision)
        .bind(input.base_revision_id.as_deref())
        .bind(&input.lifecycle)
        .bind(input.display_label.as_deref())
        .bind(&input.outcome)
        .bind(&input.included_scope_json)
        .bind(&input.excluded_scope_json)
        .bind(input.charter_revision_id.as_deref())
        .bind(&input.document_revisions_json)
        .bind(&input.task_selection_json)
        .bind(&input.dependencies_json)
        .bind(&input.risks_json)
        .bind(&input.acceptance_checks_json)
        .bind(&input.evidence_requirements_json)
        .bind(&input.known_issues_json)
        .bind(&input.change_summary)
        .bind(&input.schema_version)
        .bind(&input.render_version)
        .bind(&input.rendered_view)
        .bind(&input.content_digest)
        .bind(&input.rendered_digest)
        .bind(&input.author_type)
        .bind(input.author_id.as_deref())
        .bind(&input.source_refs_json)
        .bind(&input.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
    let advanced = if input.lifecycle == "draft" {
        sqlx::query(
            "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?",
        )
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?
    } else {
        sqlx::query(
            // An approved definition is what makes a milestone active
            // work. Baseline activation used to perform this transition;
            // the approved Charter and its milestone definition are the
            // authority now, so the pointer advance carries it.
            "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = ?",
        )
        .bind(&input.id)
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?
    };
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_revision(row)
}
pub(super) async fn create_project_milestone_atomically(
    db: &SqliteDb,
    input: CreateProjectMilestoneAtomically,
) -> Result<ProjectMilestoneRevisionRecord> {
    if input.milestone.id != input.revision.milestone_id
        || input.revision.expected_milestone_version != 1
        || input.revision.base_revision != 0
        || input.revision.base_revision_id.is_some()
    {
        return Err(DbError::VersionConflict);
    }

    let mut tx = crate::begin_immediate(db.pool()).await?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.milestone.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.milestone.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(&input.milestone.id)
    .bind(&input.milestone.project_id)
    .bind(input.milestone.milestone_sequence)
    .bind(&input.milestone.milestone_key)
    .bind(input.milestone.display_label.as_deref())
    .bind(&input.milestone.created_at)
    .bind(&input.milestone.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    // A Project's first milestone becomes its primary outcome. Genesis
    // seeds that pointer for a Charter-created Project, but a `standard`
    // Project's Agent creates the milestone itself; leaving the pointer
    // null there produced a Project with an active milestone and no
    // primary, which is exactly the state turn admission rejects — and
    // because admission fails before the Agent can act, it could never
    // set the pointer itself. COALESCE keeps an existing choice intact.
    let advanced = sqlx::query(
        "UPDATE project
                SET primary_milestone_id = COALESCE(primary_milestone_id, ?),
                    version = version + 1, updated_at = ?
              WHERE id = ? AND version = ?",
    )
    .bind(&input.milestone.id)
    .bind(&input.milestone.updated_at)
    .bind(&input.milestone.project_id)
    .bind(input.milestone.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest,
                author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.milestone_id)
        .bind(&input.revision.lifecycle)
        .bind(input.revision.display_label.as_deref())
        .bind(&input.revision.outcome)
        .bind(&input.revision.included_scope_json)
        .bind(&input.revision.excluded_scope_json)
        .bind(input.revision.charter_revision_id.as_deref())
        .bind(&input.revision.document_revisions_json)
        .bind(&input.revision.task_selection_json)
        .bind(&input.revision.dependencies_json)
        .bind(&input.revision.risks_json)
        .bind(&input.revision.acceptance_checks_json)
        .bind(&input.revision.evidence_requirements_json)
        .bind(&input.revision.known_issues_json)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
    // The pointer trigger only accepts a 'proposed'/'approved' target, so
    // a 'draft' first revision must skip the pointer update here exactly
    // like `create_project_milestone_revision` does -- the pointer is
    // promoted by a later revision, not by this creation.
    let advanced = if input.revision.lifecycle == "draft" {
        sqlx::query(
            "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = 1",
        )
        .bind(&input.revision.created_at)
        .bind(&input.milestone.id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?
    } else {
        sqlx::query(
            // An approved definition is what makes a milestone active
            // work. Baseline activation used to perform this transition;
            // the approved Charter and its milestone definition are the
            // authority now, so the pointer advance carries it.
            "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = 1",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.created_at)
        .bind(&input.milestone.id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?
    };
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_revision(row)
}
pub(super) async fn create_project_milestone_command(
    db: &SqliteDb,
    input: CreateProjectMilestoneCommand,
) -> Result<ProjectMilestoneRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("project_id", input.milestone.project_id.as_str())],
        )?;
        let milestone_id = command_outcome_string(&receipt, "milestone_id")?;
        let revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT r.*, m.project_id
                 FROM project_milestone_revision r
                 JOIN project_milestone m ON m.id = r.milestone_id
                 WHERE r.id = ? AND r.milestone_id = ?",
        )
        .bind(&revision_id)
        .bind(&milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: String = row.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let record = map_milestone_revision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    if input.milestone.id != input.revision.milestone_id
        || input.revision.expected_milestone_version != 1
        || input.revision.base_revision != 0
        || input.revision.base_revision_id.is_some()
    {
        return Err(DbError::VersionConflict);
    }
    validate_command_scope(
        input.command_receipt.as_ref(),
        "project",
        &input.milestone.project_id,
    )?;
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.milestone.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.milestone.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    let (milestone_sequence, milestone_key) = if input.allocate_project_sequence {
        let sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(milestone_sequence), 0) + 1
                 FROM project_milestone WHERE project_id = ?",
        )
        .bind(&input.milestone.project_id)
        .fetch_one(&mut *tx)
        .await?;
        (sequence, format!("M{sequence:03}"))
    } else {
        (
            input.milestone.milestone_sequence,
            input.milestone.milestone_key.clone(),
        )
    };
    sqlx::query(
        "INSERT INTO project_milestone (
                id, project_id, milestone_sequence, milestone_key, display_label,
                lifecycle, blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'planned', '[]', '[]', '[]', 1, ?, ?)",
    )
    .bind(&input.milestone.id)
    .bind(&input.milestone.project_id)
    .bind(milestone_sequence)
    .bind(&milestone_key)
    .bind(input.milestone.display_label.as_deref())
    .bind(&input.milestone.created_at)
    .bind(&input.milestone.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    // See `create_project_milestone_atomically`: a Project's first
    // milestone becomes its primary outcome, or turn admission rejects the
    // Project the moment that milestone goes active.
    let project_updated = sqlx::query(
        "UPDATE project
                SET primary_milestone_id = COALESCE(primary_milestone_id, ?),
                    version = version + 1, updated_at = ?
              WHERE id = ? AND version = ?",
    )
    .bind(&input.milestone.id)
    .bind(&input.milestone.updated_at)
    .bind(&input.milestone.project_id)
    .bind(input.milestone.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if project_updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest, author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.milestone_id)
        .bind(&input.revision.lifecycle)
        .bind(input.revision.display_label.as_deref())
        .bind(&input.revision.outcome)
        .bind(&input.revision.included_scope_json)
        .bind(&input.revision.excluded_scope_json)
        .bind(input.revision.charter_revision_id.as_deref())
        .bind(&input.revision.document_revisions_json)
        .bind(&input.revision.task_selection_json)
        .bind(&input.revision.dependencies_json)
        .bind(&input.revision.risks_json)
        .bind(&input.revision.acceptance_checks_json)
        .bind(&input.revision.evidence_requirements_json)
        .bind(&input.revision.known_issues_json)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    materialize_milestone_check_definitions_in_tx(
        &mut tx,
        &input.milestone.project_id,
        &input.milestone.id,
        &input.revision,
        &input.check_definitions,
    )
    .await?;
    let milestone_updated = if input.revision.lifecycle == "draft" {
        sqlx::query(
            "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND version = 1",
        )
        .bind(&input.revision.created_at)
        .bind(&input.milestone.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    } else {
        sqlx::query(
            // An approved definition is what makes a milestone active
            // work. Baseline activation used to perform this transition;
            // the approved Charter and its milestone definition are the
            // authority now, so the pointer advance carries it.
            "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = 1",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.created_at)
        .bind(&input.milestone.id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    };
    if milestone_updated.rows_affected() != 1 {
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
        event_type: "milestone.definition.created".to_owned(),
        entity_type: "project_milestone_revision".to_owned(),
        entity_id: input.revision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.milestone.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-milestone-revision-created:{}",
            input.revision.id
        )),
        payload_json: serde_json::json!({
            "operation": "project.milestone",
            "project_id": input.milestone.project_id.clone(),
            "milestone_id": input.milestone.id.clone(),
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
    let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_revision(row)
}
pub(super) async fn append_project_milestone_revision_command(
    db: &SqliteDb,
    input: AppendProjectMilestoneRevisionCommand,
) -> Result<ProjectMilestoneRevisionRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(
            &receipt,
            &[("milestone_id", input.revision.milestone_id.as_str())],
        )?;
        let revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT r.*, m.project_id
                 FROM project_milestone_revision r
                 JOIN project_milestone m ON m.id = r.milestone_id
                 WHERE r.id = ? AND r.milestone_id = ?",
        )
        .bind(&revision_id)
        .bind(&input.revision.milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: String = row.try_get("project_id")?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        let record = map_milestone_revision(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    let milestone = sqlx::query(
        "SELECT project_id, version, current_definition_revision_id
             FROM project_milestone WHERE id = ?",
    )
    .bind(&input.revision.milestone_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let project_id: String = milestone.try_get("project_id")?;
    validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    if milestone_version != input.revision.expected_milestone_version {
        return Err(DbError::VersionConflict);
    }
    let current_revision_id: Option<String> =
        milestone.try_get("current_definition_revision_id")?;
    if input.revision.base_revision > 0 {
        let Some(base_revision_id) = input.revision.base_revision_id.as_deref() else {
            return Err(DbError::VersionConflict);
        };
        if let Some(current_revision_id) = current_revision_id.as_deref() {
            if current_revision_id != base_revision_id {
                return Err(DbError::VersionConflict);
            }
        } else {
            // A first draft intentionally leaves the current-definition
            // pointer NULL.  In that state the runtime projects the
            // latest immutable revision as the effective editing base.
            let latest_revision_id: Option<String> = sqlx::query_scalar(
                "SELECT id FROM project_milestone_revision
                     WHERE milestone_id = ? ORDER BY revision DESC, id DESC LIMIT 1",
            )
            .bind(&input.revision.milestone_id)
            .fetch_optional(&mut *tx)
            .await?;
            if latest_revision_id.as_deref() != Some(base_revision_id) {
                return Err(DbError::VersionConflict);
            }
        }
        let base_matches: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone_revision
                 WHERE id = ? AND milestone_id = ? AND revision = ? LIMIT 1",
        )
        .bind(base_revision_id)
        .bind(&input.revision.milestone_id)
        .bind(input.revision.base_revision)
        .fetch_optional(&mut *tx)
        .await?;
        if base_matches.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if input.revision.base_revision_id.is_some() || current_revision_id.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_milestone_revision WHERE milestone_id = ?",
    )
    .bind(&input.revision.milestone_id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
            "INSERT INTO project_milestone_revision (
                id, milestone_id, revision, base_revision, base_revision_id, lifecycle,
                display_label, outcome, included_scope_json, excluded_scope_json,
                charter_revision_id, document_revisions_json, task_selection_json,
                dependencies_json, risks_json, acceptance_checks_json,
                evidence_requirements_json, known_issues_json, change_summary,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest, author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.milestone_id)
        .bind(revision)
        .bind(input.revision.base_revision)
        .bind(input.revision.base_revision_id.as_deref())
        .bind(&input.revision.lifecycle)
        .bind(input.revision.display_label.as_deref())
        .bind(&input.revision.outcome)
        .bind(&input.revision.included_scope_json)
        .bind(&input.revision.excluded_scope_json)
        .bind(input.revision.charter_revision_id.as_deref())
        .bind(&input.revision.document_revisions_json)
        .bind(&input.revision.task_selection_json)
        .bind(&input.revision.dependencies_json)
        .bind(&input.revision.risks_json)
        .bind(&input.revision.acceptance_checks_json)
        .bind(&input.revision.evidence_requirements_json)
        .bind(&input.revision.known_issues_json)
        .bind(&input.revision.change_summary)
        .bind(&input.revision.schema_version)
        .bind(&input.revision.render_version)
        .bind(&input.revision.rendered_view)
        .bind(&input.revision.content_digest)
        .bind(&input.revision.rendered_digest)
        .bind(&input.revision.author_type)
        .bind(input.revision.author_id.as_deref())
        .bind(&input.revision.source_refs_json)
        .bind(&input.revision.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    materialize_milestone_check_definitions_in_tx(
        &mut tx,
        &project_id,
        &input.revision.milestone_id,
        &input.revision,
        &input.check_definitions,
    )
    .await?;
    let advanced = if input.revision.lifecycle == "draft" {
        sqlx::query(
            "UPDATE project_milestone
                 SET version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&input.revision.created_at)
        .bind(&input.revision.milestone_id)
        .bind(&project_id)
        .bind(input.revision.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    } else {
        sqlx::query(
            // An approved definition is what makes a milestone active
            // work. Baseline activation used to perform this transition;
            // the approved Charter and its milestone definition are the
            // authority now, so the pointer advance carries it.
            "UPDATE project_milestone
                 SET current_definition_revision_id = ?,
                     lifecycle = CASE WHEN lifecycle = 'planned'
                                      THEN 'active' ELSE lifecycle END,
                     version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.created_at)
        .bind(&input.revision.milestone_id)
        .bind(&project_id)
        .bind(input.revision.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    };
    if advanced.rows_affected() != 1 {
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
        event_type: "milestone.definition.revised".to_owned(),
        entity_type: "project_milestone_revision".to_owned(),
        entity_id: input.revision.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: project_id,
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-milestone-revision-created:{}",
            input.revision.id
        )),
        payload_json: serde_json::json!({
            "operation": "project.milestone",
            "milestone_id": input.revision.milestone_id.clone(),
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
    let row = sqlx::query("SELECT * FROM project_milestone_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_revision(row)
}
pub(super) async fn set_primary_project_milestone_command(
    db: &SqliteDb,
    input: SetPrimaryProjectMilestoneCommand,
) -> Result<Project> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let replay = resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?;
    if let Some(receipt) = replay {
        validate_command_outcome_identity(&receipt, &[("project_id", input.project_id.as_str())])?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
        ))
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        let project = map_project(row)?;
        tx.commit().await?;
        return Ok(project);
    }

    validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
    if input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let project = sqlx::query("SELECT version FROM project WHERE id = ?")
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.expected_project_version {
        return Err(DbError::VersionConflict);
    }
    let active_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active'",
    )
    .bind(&input.project_id)
    .fetch_one(&mut *tx)
    .await?;
    if active_count > 0 && input.primary_milestone_id.is_none() {
        return Err(DbError::VersionConflict);
    }
    if let Some(milestone_id) = input.primary_milestone_id.as_deref() {
        let valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone
                 WHERE id = ? AND project_id = ? AND lifecycle IN ('planned', 'active')",
        )
        .bind(milestone_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if valid.is_none() {
            return Err(DbError::VersionConflict);
        }
    }
    let updated = sqlx::query(
        "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                 updated_at = ? WHERE id = ? AND version = ?",
    )
    .bind(input.primary_milestone_id.as_deref())
    .bind(&input.updated_at)
    .bind(&input.project_id)
    .bind(input.expected_project_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if updated.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            input.principal_type.clone(),
            Some(input.principal_id.clone()),
            input.explicit_event.clone(),
            Some(input.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "milestone.primary.set".to_owned(),
        entity_type: "project".to_owned(),
        entity_id: input.project_id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!(
            "project-milestone-primary-set:{}",
            input.idempotency_key
        )),
        payload_json: serde_json::json!({
            "operation": "project.milestone",
            "project_id": input.project_id.clone(),
            "primary_milestone_id": input.primary_milestone_id.clone(),
            "expected_project_version": input.expected_project_version,
            "principal_id": input.principal_id.clone(),
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
    let row = sqlx::query(&format!(
        "SELECT {PROJECT_COLUMNS} FROM project WHERE id = ?"
    ))
    .bind(&input.project_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    map_project(row)
}
pub(super) async fn get_project_milestone_revision(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectMilestoneRevisionRecord>> {
    select_one(
        "SELECT * FROM project_milestone_revision WHERE id = ?",
        db.pool(),
        id,
        map_milestone_revision,
    )
    .await
}
pub(super) async fn list_project_milestone_revisions(
    db: &SqliteDb,
    milestone_id: &str,
) -> Result<Vec<ProjectMilestoneRevisionRecord>> {
    sqlx::query(
        "SELECT * FROM project_milestone_revision
             WHERE milestone_id = ? ORDER BY revision ASC, id ASC",
    )
    .bind(milestone_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_milestone_revision)
    .collect()
}
pub(super) async fn create_project_milestone_check(
    db: &SqliteDb,
    input: CreateProjectMilestoneCheck,
) -> Result<ProjectMilestoneCheckRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    let milestone = sqlx::query(
        "SELECT version FROM project_milestone
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
    let definition_matches: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ? LIMIT 1",
    )
    .bind(&input.definition_revision_id)
    .bind(&input.milestone_id)
    .fetch_optional(&mut *tx)
    .await?;
    if definition_matches.is_none() {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_milestone_check (
                id, project_id, milestone_id, definition_revision_id, check_key,
                description, required, source_kind, expected_result,
                evidence_required, version, current_result_id, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.definition_revision_id)
    .bind(&input.check_key)
    .bind(&input.description)
    .bind(input.required)
    .bind(&input.source_kind)
    .bind(&input.expected_result)
    .bind(input.evidence_required)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let advanced = sqlx::query(
        "UPDATE project_milestone SET version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&input.updated_at)
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .bind(input.expected_milestone_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let row = sqlx::query("SELECT * FROM project_milestone_check WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_check(row)
}
pub(super) async fn append_project_milestone_check_result(
    db: &SqliteDb,
    command: AppendProjectMilestoneCheckResultCommand,
) -> Result<ProjectMilestoneCheckResultRecord> {
    let AppendProjectMilestoneCheckResultCommand {
        result: input,
        command_receipt,
        action_execution,
    } = command;
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
    validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
    if let Some(receipt) = resolve_command_replay(db, &mut tx, command_receipt.as_ref()).await? {
        validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
        let result_id = command_outcome_string(&receipt, "result_id")?;
        let row = sqlx::query("SELECT * FROM project_milestone_check_result WHERE id = ?")
            .bind(&result_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
        tx.commit().await?;
        return map_milestone_result(row);
    }
    if let Some(existing) =
        sqlx::query("SELECT * FROM project_milestone_check_result WHERE idempotency_key = ?")
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(map_milestone_result)
            .transpose()?
    {
        if existing.id != input.id
            || existing.project_id != input.project_id
            || existing.milestone_id != input.milestone_id
            || existing.check_id != input.check_id
            || existing.definition_revision_id != input.definition_revision_id
            || existing.source_kind != input.source_kind
            || existing.source_manifest_json != input.source_manifest_json
            || existing.input_digest != input.input_digest
            || existing.outcome != input.outcome
            || existing.governing_charter_revision_id != input.governing_charter_revision_id
            || existing.principal_type != input.principal_type
            || existing.principal_id != input.principal_id
            || existing.authorization_basis != input.authorization_basis
            || existing.authorization_action != input.authorization_action
            || existing.authorization_occurred_at != input.authorization_occurred_at
            || existing.expected_version != input.expected_version
            || existing.explicit_event != input.explicit_event
        {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let check = sqlx::query(
        "SELECT version FROM project_milestone_check
             WHERE id = ? AND project_id = ? AND milestone_id = ?
               AND definition_revision_id = ? AND source_kind = ?",
    )
    .bind(&input.check_id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.definition_revision_id)
    .bind(&input.source_kind)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let check_version: i64 = check.try_get("version")?;
    if check_version != input.expected_version {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_milestone_check_result (
                id, project_id, milestone_id, check_id, definition_revision_id,
                outcome, source_kind, source_manifest_json, input_digest,
                governing_charter_revision_id,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at, expected_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.check_id)
    .bind(&input.definition_revision_id)
    .bind(&input.outcome)
    .bind(&input.source_kind)
    .bind(&input.source_manifest_json)
    .bind(&input.input_digest)
    .bind(input.governing_charter_revision_id.as_deref())
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.authorization_occurred_at)
    .bind(input.expected_version)
    .bind(&input.explicit_event)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    let advanced = sqlx::query(
        "UPDATE project_milestone_check
             SET current_result_id = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.id)
    .bind(&input.created_at)
    .bind(&input.check_id)
    .bind(input.expected_version)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if advanced.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            command_receipt.as_ref(),
            input.principal_type.clone(),
            Some(input.principal_id.clone()),
            input.id.clone(),
            None,
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.milestone.check.recorded".to_owned(),
        entity_type: "project_milestone_check_result".to_owned(),
        entity_id: input.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("milestone-check-recorded:{}", input.id)),
        payload_json: serde_json::json!({
            "project_id": input.project_id.clone(),
            "milestone_id": input.milestone_id.clone(),
            "check_id": input.check_id.clone(),
            "definition_revision_id": input.definition_revision_id.clone(),
            "source_kind": input.source_kind.clone(),
            "outcome": input.outcome.clone(),
        })
        .to_string(),
        created_at: input.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(db, &mut tx, &event.id, command_receipt, action_execution).await?;
    let row = sqlx::query("SELECT * FROM project_milestone_check_result WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_milestone_result(row)
}

mod commands;
mod reads;
pub use commands::{MaterializeMilestoneCheckInput, TransitionMilestoneLifecycleInput};
pub use reads::{MilestoneDefinitionTransitionRead, ProjectMilestoneIdRead};
