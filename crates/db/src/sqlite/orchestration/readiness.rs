use super::*;
fn map_readiness(row: SqliteRow) -> Result<ProjectReadinessSnapshotRecord> {
    Ok(ProjectReadinessSnapshotRecord {
        id: required_string(&row, "id")?,
        project_id: required_string(&row, "project_id")?,
        milestone_id: required_string(&row, "milestone_id")?,
        definition_revision_id: required_string(&row, "definition_revision_id")?,
        input_manifest_json: required_string(&row, "input_manifest_json")?,
        event_watermark: required_string(&row, "event_watermark")?,
        outcome: required_string(&row, "outcome")?,
        blocking_reasons_json: required_string(&row, "blocking_reasons_json")?,
        check_results_json: required_string(&row, "check_results_json")?,
        waiver_manifest_json: required_string(&row, "waiver_manifest_json")?,
        evidence_manifest_json: required_string(&row, "evidence_manifest_json")?,
        commit_context_json: required_string(&row, "commit_context_json")?,
        computing_policy_revision: required_string(&row, "computing_policy_revision")?,
        readiness_digest: required_string(&row, "readiness_digest")?,
        principal_type: required_string(&row, "principal_type")?,
        principal_id: required_string(&row, "principal_id")?,
        authorization_basis: required_string(&row, "authorization_basis")?,
        authorization_action: required_string(&row, "authorization_action")?,
        authorization_occurred_at: required_string(&row, "authorization_occurred_at")?,
        expected_milestone_version: row.try_get("expected_milestone_version")?,
        explicit_event: required_string(&row, "explicit_event")?,
        idempotency_key: required_string(&row, "idempotency_key")?,
        created_at: required_string(&row, "created_at")?,
    })
}

fn readiness_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> crate::Result<ProjectReadinessSnapshotRecord> {
    Ok(ProjectReadinessSnapshotRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        milestone_id: row.try_get("milestone_id")?,
        definition_revision_id: row.try_get("definition_revision_id")?,
        input_manifest_json: row.try_get("input_manifest_json")?,
        event_watermark: row.try_get("event_watermark")?,
        outcome: row.try_get("outcome")?,
        blocking_reasons_json: row.try_get("blocking_reasons_json")?,
        check_results_json: row.try_get("check_results_json")?,
        waiver_manifest_json: row.try_get("waiver_manifest_json")?,
        evidence_manifest_json: row.try_get("evidence_manifest_json")?,
        commit_context_json: row.try_get("commit_context_json")?,
        computing_policy_revision: row.try_get("computing_policy_revision")?,
        readiness_digest: row.try_get("readiness_digest")?,
        principal_type: row.try_get("principal_type")?,
        principal_id: row.try_get("principal_id")?,
        authorization_basis: row.try_get("authorization_basis")?,
        authorization_action: row.try_get("authorization_action")?,
        expected_milestone_version: row.try_get("expected_milestone_version")?,
        explicit_event: row.try_get("explicit_event")?,
        authorization_occurred_at: row.try_get("authorization_occurred_at")?,
        idempotency_key: row.try_get("idempotency_key")?,
        created_at: row.try_get("created_at")?,
    })
}

impl SqliteDb {
    pub async fn readiness_by_idempotency_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        key: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(project_id)
        .bind(key)
        .fetch_optional(connection)
        .await?
        .map(readiness_record_from_row)
        .transpose()
    }
}

#[derive(Debug)]
pub struct UpdateMilestoneReadinessProjectionInput<'a> {
    pub lifecycle: &'a str,
    pub blocker_json: &'a str,
    pub stale_json: &'a str,
    pub reconciliation_json: &'a str,
    pub now: &'a str,
    pub milestone_id: &'a str,
    pub project_id: &'a str,
    pub expected_milestone_version: i64,
}

impl SqliteDb {
    pub async fn update_milestone_readiness_projection_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &UpdateMilestoneReadinessProjectionInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE project_milestone
             SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                 reconciliation_reason_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('active', 'ready_for_release', 'released')",
        )
        .bind(input.lifecycle)
        .bind(input.blocker_json)
        .bind(input.stale_json)
        .bind(input.reconciliation_json)
        .bind(input.now)
        .bind(input.milestone_id)
        .bind(input.project_id)
        .bind(input.expected_milestone_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn milestone_readiness_by_id_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        snapshot_id: &str,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?",
        )
        .bind(snapshot_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(connection)
        .await?
        .map(readiness_record_from_row)
        .transpose()
    }
}

impl SqliteDb {
    pub async fn milestone_readiness_record_by_id(
        &self,
        snapshot_id: &str,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?",
        )
        .bind(snapshot_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(self.pool())
        .await?
        .map(readiness_record_from_row)
        .transpose()
    }
}

impl SqliteDb {
    pub async fn milestone_readiness_records(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND milestone_id = ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(readiness_record_from_row)
        .collect()
    }
}

impl SqliteDb {
    pub async fn project_readiness_record_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND id = ?",
        )
        .bind(project_id)
        .bind(snapshot_id)
        .fetch_optional(connection)
        .await?
        .map(readiness_record_from_row)
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneCharterAuthorityRow {
    pub charter_id: String,
    pub current_approved_revision_id: Option<String>,
    pub revision_id: String,
    pub lifecycle: String,
    pub content_digest: String,
    pub render_version: String,
    pub rendered_digest: String,
}

impl SqliteDb {
    pub async fn milestone_charter_authority_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        revision_id: &str,
    ) -> crate::Result<Option<MilestoneCharterAuthorityRow>> {
        let row = sqlx::query(
            "SELECT c.id AS charter_id,
                    c.current_approved_revision_id,
                    cr.id AS revision_id,
                    cr.lifecycle,
                    cr.content_digest,
                    cr.render_version,
                    cr.rendered_digest
             FROM project_charter c
             JOIN project_charter_revision cr ON cr.charter_id = c.id
             WHERE c.project_id = ? AND cr.id = ?",
        )
        .bind(project_id)
        .bind(revision_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(MilestoneCharterAuthorityRow {
                charter_id: row.try_get("charter_id")?,
                current_approved_revision_id: row.try_get("current_approved_revision_id")?,
                revision_id: row.try_get("revision_id")?,
                lifecycle: row.try_get("lifecycle")?,
                content_digest: row.try_get("content_digest")?,
                render_version: row.try_get("render_version")?,
                rendered_digest: row.try_get("rendered_digest")?,
            })
        })
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneCheckResultRow {
    pub check_id: String,
    pub source_kind: String,
    pub id: String,
    pub principal_type: String,
    pub principal_id: String,
    pub created_at: String,
    pub outcome: String,
    pub input_digest: String,
    pub source_manifest_json: String,
    pub definition_revision_id: String,
    pub governing_charter_revision_id: Option<String>,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub expected_version: i64,
}

impl SqliteDb {
    pub async fn milestone_check_result_rows_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<MilestoneCheckResultRow>> {
        let rows = sqlx::query(
            "SELECT r.* FROM project_milestone_check_result r
             JOIN project_milestone_check c ON c.id = r.check_id
             WHERE r.project_id = ? AND r.milestone_id = ?
               AND r.definition_revision_id = c.definition_revision_id
             ORDER BY r.check_id ASC, r.created_at DESC, r.id DESC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MilestoneCheckResultRow {
                    check_id: row.try_get("check_id")?,
                    source_kind: row.try_get("source_kind")?,
                    id: row.try_get("id")?,
                    principal_type: row.try_get("principal_type")?,
                    principal_id: row.try_get("principal_id")?,
                    created_at: row.try_get("created_at")?,
                    outcome: row.try_get("outcome")?,
                    input_digest: row.try_get("input_digest")?,
                    source_manifest_json: row.try_get("source_manifest_json")?,
                    definition_revision_id: row.try_get("definition_revision_id")?,
                    governing_charter_revision_id: row.try_get("governing_charter_revision_id")?,
                    authorization_basis: row.try_get("authorization_basis")?,
                    authorization_action: row.try_get("authorization_action")?,
                    explicit_event: row.try_get("explicit_event")?,
                    authorization_occurred_at: row.try_get("authorization_occurred_at")?,
                    expected_version: row.try_get("expected_version")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneWaiverAffectedRow {
    pub id: String,
    pub affected_records_json: String,
}

impl SqliteDb {
    pub async fn active_milestone_waiver_rows_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Vec<MilestoneWaiverAffectedRow>> {
        let rows = sqlx::query(
            "SELECT id, affected_records_json
             FROM project_decision
             WHERE project_id = ? AND decision_class = 'waiver' AND state = 'active'
               AND length(trim(authorization_action)) > 0
               AND length(trim(authorization_occurred_at)) > 0",
        )
        .bind(project_id)
        .fetch_all(connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MilestoneWaiverAffectedRow {
                    id: row.try_get("id")?,
                    affected_records_json: row.try_get("affected_records_json")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneDecisionRow {
    pub id: String,
    pub state: String,
    pub decision_class: String,
    pub question: String,
    pub context_json: String,
    pub options_json: String,
    pub selected_outcome: String,
    pub rationale: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authority_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub charter_revision_id: Option<String>,
    pub source_refs_json: String,
    pub affected_records_json: String,
    pub supersedes_decision_id: Option<String>,
    pub created_at: String,
}

impl SqliteDb {
    pub async fn effective_milestone_decision_rows_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
        charter_revision_id: Option<&str>,
    ) -> crate::Result<Vec<MilestoneDecisionRow>> {
        let rows = sqlx::query(
            "SELECT id, state, decision_class, question, context_json,
                    options_json, selected_outcome, rationale,
                    principal_type, principal_id, authority_basis,
                    authorization_action, explicit_event,
                    authorization_occurred_at, charter_revision_id,
                    source_refs_json,
                    affected_records_json, supersedes_decision_id, created_at
             FROM project_decision
             WHERE project_id = ? AND state = 'active'
               AND (
                    json_extract(affected_records_json, '$.milestone_id') = ?
                    OR (? IS NOT NULL AND charter_revision_id = ?)
               )
             ORDER BY id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(charter_revision_id)
        .bind(charter_revision_id)
        .fetch_all(connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MilestoneDecisionRow {
                    id: row.try_get("id")?,
                    state: row.try_get("state")?,
                    decision_class: row.try_get("decision_class")?,
                    question: row.try_get("question")?,
                    context_json: row.try_get("context_json")?,
                    options_json: row.try_get("options_json")?,
                    selected_outcome: row.try_get("selected_outcome")?,
                    rationale: row.try_get("rationale")?,
                    principal_type: row.try_get("principal_type")?,
                    principal_id: row.try_get("principal_id")?,
                    authority_basis: row.try_get("authority_basis")?,
                    authorization_action: row.try_get("authorization_action")?,
                    explicit_event: row.try_get("explicit_event")?,
                    authorization_occurred_at: row.try_get("authorization_occurred_at")?,
                    charter_revision_id: row.try_get("charter_revision_id")?,
                    source_refs_json: row.try_get("source_refs_json")?,
                    affected_records_json: row.try_get("affected_records_json")?,
                    supersedes_decision_id: row.try_get("supersedes_decision_id")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneReadinessSourceRow {
    pub version: i64,
    pub updated_at: String,
    pub milestone_key: String,
    pub display_label: Option<String>,
    pub lifecycle: String,
    pub current_definition_revision_id: Option<String>,
}

impl SqliteDb {
    pub async fn milestone_readiness_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<MilestoneReadinessSourceRow> {
        let row = sqlx::query(
            "SELECT version, updated_at, milestone_key, display_label, lifecycle,
                    current_definition_revision_id
             FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_one(connection)
        .await?;
        Ok(MilestoneReadinessSourceRow {
            version: row.try_get("version")?,
            updated_at: row.try_get("updated_at")?,
            milestone_key: row.try_get("milestone_key")?,
            display_label: row.try_get("display_label")?,
            lifecycle: row.try_get("lifecycle")?,
            current_definition_revision_id: row.try_get("current_definition_revision_id")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneDefinitionReadinessSourceRow {
    pub revision: i64,
    pub content_digest: String,
    pub rendered_digest: String,
    pub render_version: String,
    pub lifecycle: String,
    pub created_at: String,
}

impl SqliteDb {
    pub async fn milestone_definition_readiness_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        milestone_id: &str,
    ) -> crate::Result<MilestoneDefinitionReadinessSourceRow> {
        let row = sqlx::query(
            "SELECT revision, content_digest, rendered_digest, render_version,
                    lifecycle, created_at
             FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ?",
        )
        .bind(revision_id)
        .bind(milestone_id)
        .fetch_one(connection)
        .await?;
        Ok(MilestoneDefinitionReadinessSourceRow {
            revision: row.try_get("revision")?,
            content_digest: row.try_get("content_digest")?,
            rendered_digest: row.try_get("rendered_digest")?,
            render_version: row.try_get("render_version")?,
            lifecycle: row.try_get("lifecycle")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CharterReadinessSourceRow {
    pub revision: i64,
    pub created_at: String,
    pub content_digest: String,
    pub render_version: String,
    pub rendered_digest: String,
}

impl SqliteDb {
    pub async fn charter_readiness_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        charter_id: &str,
    ) -> crate::Result<CharterReadinessSourceRow> {
        let row = sqlx::query(
            "SELECT revision, created_at, content_digest, render_version, rendered_digest
                 FROM project_charter_revision
                 WHERE id = ? AND charter_id = ?",
        )
        .bind(revision_id)
        .bind(charter_id)
        .fetch_one(connection)
        .await?;
        Ok(CharterReadinessSourceRow {
            revision: row.try_get("revision")?,
            created_at: row.try_get("created_at")?,
            content_digest: row.try_get("content_digest")?,
            render_version: row.try_get("render_version")?,
            rendered_digest: row.try_get("rendered_digest")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ActiveCheckWaiverSourceRow {
    pub id: String,
    pub rationale: String,
    pub authority_basis: String,
    pub authorization_action: String,
    pub principal_type: String,
    pub principal_id: Option<String>,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub affected_records_json: String,
    pub created_at: String,
}

impl SqliteDb {
    pub async fn active_check_waiver_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
        milestone_id: &str,
        check_id: &str,
    ) -> crate::Result<Option<ActiveCheckWaiverSourceRow>> {
        let row = sqlx::query(
            "SELECT id, rationale, authority_basis, authorization_action,
                        principal_type, principal_id, explicit_event,
                        authorization_occurred_at,
                        affected_records_json, created_at
                 FROM project_decision
                 WHERE project_id = ? AND decision_class = 'waiver' AND state = 'active'
                   AND json_extract(affected_records_json, '$.milestone_id') = ?
                   AND json_extract(affected_records_json, '$.check_id') = ?
                 ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(check_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(ActiveCheckWaiverSourceRow {
                id: row.try_get("id")?,
                rationale: row.try_get("rationale")?,
                authority_basis: row.try_get("authority_basis")?,
                authorization_action: row.try_get("authorization_action")?,
                principal_type: row.try_get("principal_type")?,
                principal_id: row.try_get("principal_id")?,
                explicit_event: row.try_get("explicit_event")?,
                authorization_occurred_at: row.try_get("authorization_occurred_at")?,
                affected_records_json: row.try_get("affected_records_json")?,
                created_at: row.try_get("created_at")?,
            })
        })
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct MilestoneDocumentSourceRow {
    pub version: i64,
    pub updated_at: String,
    pub current_approved_revision_id: Option<String>,
    pub revision_id: String,
    pub lifecycle: String,
    pub content_digest: String,
}

impl SqliteDb {
    pub async fn milestone_document_source_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        revision_id: &str,
        document_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneDocumentSourceRow>> {
        let row = sqlx::query(
            "SELECT d.version, d.updated_at, d.current_approved_revision_id,
                        r.id AS revision_id, r.lifecycle, r.content_digest
                 FROM project_document d
                 JOIN project_document_revision r
                   ON r.id = ? AND r.document_id = d.id
                 WHERE d.id = ? AND d.project_id = ?",
        )
        .bind(revision_id)
        .bind(document_id)
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(MilestoneDocumentSourceRow {
                version: row.try_get("version")?,
                updated_at: row.try_get("updated_at")?,
                current_approved_revision_id: row.try_get("current_approved_revision_id")?,
                revision_id: row.try_get("revision_id")?,
                lifecycle: row.try_get("lifecycle")?,
                content_digest: row.try_get("content_digest")?,
            })
        })
        .transpose()
    }
}

#[derive(Debug, Clone)]
pub struct ProjectAgentIdentityRow {
    pub identity_id: String,
}

impl SqliteDb {
    pub async fn active_project_agent_identity_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<Option<ProjectAgentIdentityRow>> {
        let row = sqlx::query(
            "SELECT identity_id FROM project_agent_binding
             WHERE project_id = ? AND state = 'active'
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(connection)
        .await?;
        row.map(|row| {
            Ok(ProjectAgentIdentityRow {
                identity_id: row.try_get("identity_id")?,
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn project_readiness_record(
        &self,
        project_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query("SELECT * FROM project_readiness_snapshot WHERE project_id = ? AND id = ?")
            .bind(project_id)
            .bind(snapshot_id)
            .fetch_optional(self.pool())
            .await?
            .map(readiness_record_from_row)
            .transpose()
    }
}

#[derive(Debug)]
pub struct InsertMilestoneReadinessInput<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub milestone_id: &'a str,
    pub definition_revision_id: &'a str,
    pub input_manifest_json: &'a str,
    pub event_watermark: &'a str,
    pub outcome: &'a str,
    pub blocking_reasons_json: &'a str,
    pub check_results_json: &'a str,
    pub waiver_manifest_json: &'a str,
    pub evidence_manifest_json: &'a str,
    pub commit_context_json: &'a str,
    pub computing_policy_revision: &'a str,
    pub readiness_digest: &'a str,
    pub principal_type: &'a str,
    pub principal_id: &'a str,
    pub authorization_basis: &'a str,
    pub authorization_action: &'a str,
    pub expected_milestone_version: i64,
    pub explicit_event: &'a str,
    pub authorization_occurred_at: &'a str,
    pub idempotency_key: &'a str,
    pub created_at: &'a str,
}

impl SqliteDb {
    pub async fn insert_milestone_readiness_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertMilestoneReadinessInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_readiness_snapshot (
            id, project_id, milestone_id, definition_revision_id,
            input_manifest_json, event_watermark, outcome, blocking_reasons_json,
            check_results_json, waiver_manifest_json, evidence_manifest_json,
            commit_context_json, computing_policy_revision, readiness_digest,
            principal_type, principal_id, authorization_basis, authorization_action,
            expected_milestone_version, explicit_event, authorization_occurred_at,
            idempotency_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(input.id)
        .bind(input.project_id)
        .bind(input.milestone_id)
        .bind(input.definition_revision_id)
        .bind(input.input_manifest_json)
        .bind(input.event_watermark)
        .bind(input.outcome)
        .bind(input.blocking_reasons_json)
        .bind(input.check_results_json)
        .bind(input.waiver_manifest_json)
        .bind(input.evidence_manifest_json)
        .bind(input.commit_context_json)
        .bind(input.computing_policy_revision)
        .bind(input.readiness_digest)
        .bind(input.principal_type)
        .bind(input.principal_id)
        .bind(input.authorization_basis)
        .bind(input.authorization_action)
        .bind(input.expected_milestone_version)
        .bind(input.explicit_event)
        .bind(input.authorization_occurred_at)
        .bind(input.idempotency_key)
        .bind(input.created_at)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct InsertMilestoneReadinessSourceInput<'a> {
    pub snapshot_id: &'a str,
    pub ordinal: i64,
    pub source_kind: &'a str,
    pub source_id: &'a str,
    pub source_version: &'a str,
    pub source_digest: &'a str,
    pub availability: Option<&'a str>,
    pub metadata_json: &'a str,
}

impl SqliteDb {
    pub async fn insert_milestone_readiness_input_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: &InsertMilestoneReadinessSourceInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "INSERT INTO project_readiness_input (
                readiness_snapshot_id, ordinal, source_kind, source_id,
                source_version, source_digest, availability, disposition, metadata_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'included', ?)",
        )
        .bind(input.snapshot_id)
        .bind(input.ordinal)
        .bind(input.source_kind)
        .bind(input.source_id)
        .bind(input.source_version)
        .bind(input.source_digest)
        .bind(input.availability)
        .bind(input.metadata_json)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

/// Query-specific typed accessors preserve the caller's conditional decoding order.
pub struct EffectiveProjectReadinessSourceRow {
    row: sqlx::sqlite::SqliteRow,
}

impl EffectiveProjectReadinessSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn milestone_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn definition_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("definition_revision_id")?)
    }
    pub fn event_watermark(&self) -> crate::Result<String> {
        Ok(self.row.try_get("event_watermark")?)
    }
    pub fn outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("outcome")?)
    }
    pub fn blocking_reasons_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("blocking_reasons_json")?)
    }
    pub fn readiness_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("readiness_digest")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn effective_project_readiness_sources(
        &self,
        project_id: &str,
        limit: i64,
    ) -> crate::Result<Vec<EffectiveProjectReadinessSourceRow>> {
        let rows = sqlx::query(
            "SELECT id, milestone_id, definition_revision_id,
                event_watermark, outcome,
                blocking_reasons_json, readiness_digest, created_at
         FROM project_readiness_snapshot
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(project_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| EffectiveProjectReadinessSourceRow { row })
            .collect())
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MilestoneArtifactReferenceSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MilestoneArtifactReferenceSourceRow {
    pub fn artifact_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("artifact_id")?)
    }
    pub fn revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("revision_id")?)
    }
    pub fn content_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("content_digest")?)
    }
    pub fn render_version_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn render_version_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("render_version")?)
    }
    pub fn rendered_digest_required(&self) -> crate::Result<String> {
        Ok(self.row.try_get("rendered_digest")?)
    }
    pub fn rendered_digest_optional(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("rendered_digest")?)
    }
    pub fn current_approved_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_approved_revision_id")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MilestoneReleaseReadinessCandidateRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MilestoneReleaseReadinessCandidateRow {
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn milestone_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn readiness_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("readiness_digest")?)
    }
    pub fn outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("outcome")?)
    }
    pub fn blocking_reasons_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("blocking_reasons_json")?)
    }
}

/// Typed projection accessors retain the caller's decoding order and error handling.
pub struct MilestoneReplayReadinessSourceRow {
    row: sqlx::sqlite::SqliteRow,
}
impl MilestoneReplayReadinessSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn project_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_id")?)
    }
    pub fn milestone_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("milestone_id")?)
    }
    pub fn definition_revision_id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("definition_revision_id")?)
    }
    pub fn input_manifest_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("input_manifest_json")?)
    }
    pub fn event_watermark(&self) -> crate::Result<String> {
        Ok(self.row.try_get("event_watermark")?)
    }
    pub fn outcome(&self) -> crate::Result<String> {
        Ok(self.row.try_get("outcome")?)
    }
    pub fn blocking_reasons_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("blocking_reasons_json")?)
    }
    pub fn check_results_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("check_results_json")?)
    }
    pub fn waiver_manifest_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("waiver_manifest_json")?)
    }
    pub fn evidence_manifest_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("evidence_manifest_json")?)
    }
    pub fn commit_context_json(&self) -> crate::Result<String> {
        Ok(self.row.try_get("commit_context_json")?)
    }
    pub fn computing_policy_revision(&self) -> crate::Result<String> {
        Ok(self.row.try_get("computing_policy_revision")?)
    }
    pub fn readiness_digest(&self) -> crate::Result<String> {
        Ok(self.row.try_get("readiness_digest")?)
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
    pub fn authorization_occurred_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("authorization_occurred_at")?)
    }
    pub fn expected_milestone_version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("expected_milestone_version")?)
    }
    pub fn explicit_event(&self) -> crate::Result<String> {
        Ok(self.row.try_get("explicit_event")?)
    }
    pub fn idempotency_key(&self) -> crate::Result<String> {
        Ok(self.row.try_get("idempotency_key")?)
    }
    pub fn created_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("created_at")?)
    }
}

impl SqliteDb {
    pub async fn milestone_replay_readiness_source(
        &self,
        snapshot_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneReplayReadinessSourceRow>> {
        let rows =
            sqlx::query("SELECT * FROM project_readiness_snapshot WHERE id = ? AND project_id = ?")
                .bind(snapshot_id)
                .bind(project_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(rows.map(|row| MilestoneReplayReadinessSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_release_readiness_candidate(
        &self,
        snapshot_id: &str,
    ) -> crate::Result<Option<MilestoneReleaseReadinessCandidateRow>> {
        let rows = sqlx::query(
            "SELECT project_id, milestone_id, readiness_digest, outcome,
                blocking_reasons_json
         FROM project_readiness_snapshot WHERE id = ?",
        )
        .bind(snapshot_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MilestoneReleaseReadinessCandidateRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_inherited_charter_reference(
        &self,
        milestone_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneArtifactReferenceSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS artifact_id, r.id AS revision_id, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_milestone m
         JOIN project_milestone_revision d
           ON d.id = COALESCE(
                m.current_definition_revision_id,
                (SELECT id FROM project_milestone_revision
                 WHERE milestone_id = m.id ORDER BY revision DESC LIMIT 1)
              )
          AND d.milestone_id = m.id
         JOIN project_charter_revision r ON r.id = d.charter_revision_id
         JOIN project_charter c ON c.id = r.charter_id AND c.project_id = m.project_id
         WHERE m.id = ? AND m.project_id = ?
         LIMIT 1",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MilestoneArtifactReferenceSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_current_charter_reference(
        &self,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneArtifactReferenceSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS artifact_id, r.id AS revision_id, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project p
         JOIN project_charter_revision r ON r.id = p.current_charter_revision_id
         JOIN project_charter c ON c.id = r.charter_id AND c.project_id = p.id
         WHERE p.id = ?",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MilestoneArtifactReferenceSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_charter_reference_source(
        &self,
        revision_id: &str,
        charter_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneArtifactReferenceSourceRow>> {
        let rows = sqlx::query(
            "SELECT c.id AS artifact_id, c.current_approved_revision_id,
                r.id AS revision_id, r.lifecycle, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_charter_revision r
         JOIN project_charter c ON c.id = r.charter_id
         WHERE r.id = ? AND r.charter_id = ? AND c.project_id = ?
         LIMIT 1",
        )
        .bind(revision_id)
        .bind(charter_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MilestoneArtifactReferenceSourceRow { row }))
    }
}

impl SqliteDb {
    pub async fn milestone_document_reference_source(
        &self,
        revision_id: &str,
        document_id: &str,
        project_id: &str,
    ) -> crate::Result<Option<MilestoneArtifactReferenceSourceRow>> {
        let rows = sqlx::query(
            "SELECT d.id AS artifact_id, r.id AS revision_id, r.content_digest,
                r.render_version, r.rendered_digest
         FROM project_document_revision r
         JOIN project_document d ON d.id = r.document_id
         WHERE r.id = ? AND r.document_id = ? AND d.project_id = ?
         LIMIT 1",
        )
        .bind(revision_id)
        .bind(document_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(rows.map(|row| MilestoneArtifactReferenceSourceRow { row }))
    }
}
pub(super) async fn create_project_readiness_snapshot(
    db: &SqliteDb,
    input: CreateProjectReadinessSnapshot,
) -> Result<ProjectReadinessSnapshotRecord> {
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
        sqlx::query("SELECT * FROM project_readiness_snapshot WHERE idempotency_key = ?")
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
            .map(map_readiness)
            .transpose()?
    {
        if existing.id != input.id
            || existing.project_id != input.project_id
            || existing.milestone_id != input.milestone_id
            || existing.definition_revision_id != input.definition_revision_id
            || existing.input_manifest_json != input.input_manifest_json
            || existing.event_watermark != input.event_watermark
            || existing.outcome != input.outcome
            || existing.blocking_reasons_json != input.blocking_reasons_json
            || existing.check_results_json != input.check_results_json
            || existing.waiver_manifest_json != input.waiver_manifest_json
            || existing.evidence_manifest_json != input.evidence_manifest_json
            || existing.commit_context_json != input.commit_context_json
            || existing.computing_policy_revision != input.computing_policy_revision
            || existing.readiness_digest != input.readiness_digest
            || existing.principal_type != input.principal_type
            || existing.principal_id != input.principal_id
            || existing.authorization_basis != input.authorization_basis
            || existing.authorization_action != input.authorization_action
            || existing.authorization_occurred_at != input.authorization_occurred_at
            || existing.expected_milestone_version != input.expected_milestone_version
            || existing.explicit_event != input.explicit_event
        {
            return Err(DbError::VersionConflict);
        }
        tx.commit().await?;
        return Ok(existing);
    }
    let milestone = sqlx::query(
        "SELECT version, lifecycle FROM project_milestone
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let milestone_version: i64 = milestone.try_get("version")?;
    let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
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
        "INSERT INTO project_readiness_snapshot (
                id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json,
                waiver_manifest_json, evidence_manifest_json, commit_context_json,
                computing_policy_revision, readiness_digest, principal_type,
                principal_id, authorization_basis, authorization_action,
                authorization_occurred_at, expected_milestone_version,
                explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.definition_revision_id)
    .bind(&input.input_manifest_json)
    .bind(&input.event_watermark)
    .bind(&input.outcome)
    .bind(&input.blocking_reasons_json)
    .bind(&input.check_results_json)
    .bind(&input.waiver_manifest_json)
    .bind(&input.evidence_manifest_json)
    .bind(&input.commit_context_json)
    .bind(&input.computing_policy_revision)
    .bind(&input.readiness_digest)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.authorization_occurred_at)
    .bind(input.expected_milestone_version)
    .bind(&input.explicit_event)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if milestone_lifecycle != "released" {
        let lifecycle = if input.outcome == "ready" {
            "ready_for_release"
        } else {
            "active"
        };
        let updated = sqlx::query(
            "UPDATE project_milestone
                 SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                     reconciliation_reason_json = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(lifecycle)
        .bind(&input.blocker_projection_json)
        .bind(&input.stale_projection_json)
        .bind(&input.reconciliation_projection_json)
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }
    let row = sqlx::query("SELECT * FROM project_readiness_snapshot WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_readiness(row)
}
pub(super) async fn create_project_readiness_snapshot_command(
    db: &SqliteDb,
    input: CreateProjectReadinessSnapshotCommand,
) -> Result<ProjectReadinessSnapshotRecord> {
    let CreateProjectReadinessSnapshotCommand {
        snapshot: input,
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
        let snapshot_id = command_outcome_string(&receipt, "readiness_snapshot_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, action_execution.as_ref()).await?;
        let row = sqlx::query(
            "SELECT r.* FROM project_readiness_snapshot r
                 WHERE r.id = ? AND r.project_id = ? AND r.milestone_id = ?",
        )
        .bind(&snapshot_id)
        .bind(&input.project_id)
        .bind(&input.milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
        let record = map_readiness(row)?;
        tx.commit().await?;
        return Ok(record);
    }

    if input.principal_type.trim().is_empty()
        || input.principal_id.trim().is_empty()
        || input.authorization_basis.trim().is_empty()
        || input.authorization_action.trim().is_empty()
        || input.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&input.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    let milestone = sqlx::query(
        "SELECT version, lifecycle FROM project_milestone
             WHERE id = ? AND project_id = ?",
    )
    .bind(&input.milestone_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    validate_command_scope(command_receipt.as_ref(), "project", &input.project_id)?;
    // Readiness candidates are computed in a separate transaction from
    // this command persistence boundary. Re-check the source watermark
    // while holding the command's write lock so a Project mutation that
    // committed after candidate computation cannot be hidden by the
    // readiness snapshot. The readiness event itself is deliberately
    // excluded: it is the command's derived output, not a source input.
    let current_source_watermark: String = sqlx::query_scalar(
        "SELECT COALESCE(
                 (SELECT id FROM domain_event
                  WHERE scope_type = 'project' AND scope_id = ?
                    AND event_type NOT IN (
                        'milestone.readiness.evaluated',
                        'project_release.candidate_requested'
                    )
                  ORDER BY sequence DESC LIMIT 1),
                 'none'
             )",
    )
    .bind(&input.project_id)
    .fetch_one(&mut *tx)
    .await?;
    if current_source_watermark != input.event_watermark {
        return Err(DbError::VersionConflict);
    }
    let milestone_version: i64 = milestone.try_get("version")?;
    let milestone_lifecycle: String = milestone.try_get("lifecycle")?;
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
        "INSERT INTO project_readiness_snapshot (
                id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json, waiver_manifest_json,
                evidence_manifest_json, commit_context_json, computing_policy_revision,
                readiness_digest, principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at,
                expected_milestone_version, explicit_event, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.project_id)
    .bind(&input.milestone_id)
    .bind(&input.definition_revision_id)
    .bind(&input.input_manifest_json)
    .bind(&input.event_watermark)
    .bind(&input.outcome)
    .bind(&input.blocking_reasons_json)
    .bind(&input.check_results_json)
    .bind(&input.waiver_manifest_json)
    .bind(&input.evidence_manifest_json)
    .bind(&input.commit_context_json)
    .bind(&input.computing_policy_revision)
    .bind(&input.readiness_digest)
    .bind(&input.principal_type)
    .bind(&input.principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.authorization_occurred_at)
    .bind(input.expected_milestone_version)
    .bind(&input.explicit_event)
    .bind(&input.idempotency_key)
    .bind(&input.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if milestone_lifecycle != "released" {
        let lifecycle = if input.outcome == "ready" {
            "ready_for_release"
        } else {
            "active"
        };
        let updated = sqlx::query(
            "UPDATE project_milestone
                 SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                     reconciliation_reason_json = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(lifecycle)
        .bind(&input.blocker_projection_json)
        .bind(&input.stale_projection_json)
        .bind(&input.reconciliation_projection_json)
        .bind(&input.created_at)
        .bind(&input.milestone_id)
        .bind(&input.project_id)
        .bind(input.expected_milestone_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }
    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            command_receipt.as_ref(),
            input.principal_type.clone(),
            Some(input.principal_id.clone()),
            input.id.clone(),
            Some(input.explicit_event.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "milestone.readiness.evaluated".to_owned(),
        entity_type: "project_readiness_snapshot".to_owned(),
        entity_id: input.id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-readiness-snapshot-created:{}", input.id)),
        payload_json: serde_json::json!({
            "operation": "project.milestone.readiness",
            "project_id": input.project_id.clone(),
            "milestone_id": input.milestone_id.clone(),
            "readiness_snapshot_id": input.id.clone(),
            "readiness_digest": input.readiness_digest.clone(),
            "result": input.outcome.clone(),
        })
        .to_string(),
        created_at: input.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;
    finalize_command_in_tx(db, &mut tx, &event.id, command_receipt, action_execution).await?;
    let row = sqlx::query("SELECT * FROM project_readiness_snapshot WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_readiness(row)
}

mod checks;
pub use checks::{MilestoneCheckTargetRead, MilestoneManualResultRead};
