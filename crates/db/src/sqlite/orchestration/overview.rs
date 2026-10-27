//! Persistence sources for the Project Overview; response validation remains in the API.
use super::*;

// Decode on access: projection validation and skip branches determine which field is read next.
macro_rules! overview_read {
    ($name:ident { $($field:ident: $ty:ty),* $(,)? }) => {
        pub struct $name { row: SqliteRow }
        impl $name {
            $(pub fn $field(&self) -> Result<$ty> { Ok(self.row.try_get(stringify!($field))?) })*
        }
    };
}

overview_read!(OverviewCharterRead {
    author_type: String,
    author_id: Option<String>,
    content_digest: String,
    rendered_view: String,
    render_version: String,
    rendered_digest: String,
    approved_at: Option<String>,
    revision_id: String,
    charter_id: String,
    revision: i64,
    base_revision_id: Option<String>,
    project_mode: String,
    maturity: String,
    schema_version: String,
    change_summary: String,
    created_at: String,
    content_json: String,
    source_refs_json: String,
});

overview_read!(OverviewMilestoneRead {
    milestone_key: String,
    id: String,
    project_id: String,
    milestone_sequence: i64,
    display_label: Option<String>,
    lifecycle: String,
    version: i64,
    created_at: String,
    updated_at: String,
    current_definition_revision_id: Option<String>,
    blocker_reason_json: String,
    stale_reason_json: String,
    reconciliation_reason_json: String,
});

overview_read!(OverviewMilestoneRevisionRead {
    author_type: String,
    author_id: Option<String>,
    charter_revision_id: Option<String>,
    charter_id: Option<String>,
    charter_content_digest: Option<String>,
    charter_render_version: Option<String>,
    charter_render_digest: Option<String>,
    rendered_view: String,
    content_digest: String,
    rendered_digest: String,
    render_version: String,
    lifecycle: String,
    display_label: Option<String>,
    outcome: String,
    id: String,
    milestone_id: String,
    revision: i64,
    base_revision_id: Option<String>,
    schema_version: String,
    change_summary: String,
    created_at: String,
    included_scope_json: String,
    excluded_scope_json: String,
    document_revisions_json: String,
    task_selection_json: String,
    dependencies_json: String,
    risks_json: String,
    acceptance_checks_json: String,
    evidence_requirements_json: String,
    known_issues_json: String,
    source_refs_json: String,
});

pub struct OverviewAcceptanceCheckRead {
    pub id: String,
    pub description: String,
    pub required: i64,
    pub source_kind: String,
    pub expected_result: String,
    pub version: i64,
    pub result_id: Option<String>,
    pub input_digest: Option<String>,
    pub outcome: Option<String>,
}

impl OverviewAcceptanceCheckRead {
    fn from_row(row: SqliteRow) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            description: row.try_get("description")?,
            required: row.try_get("required")?,
            source_kind: row.try_get("source_kind")?,
            expected_result: row.try_get("expected_result")?,
            version: row.try_get("version")?,
            result_id: row.try_get("result_id")?,
            input_digest: row.try_get("input_digest")?,
            outcome: row.try_get("outcome")?,
        })
    }
}

pub struct OverviewTaskRead {
    pub status: String,
    pub condition_json: String,
}

impl OverviewTaskRead {
    fn from_row(row: SqliteRow) -> Result<Self> {
        Ok(Self {
            status: row.try_get("status")?,
            condition_json: row.try_get("condition_json")?,
        })
    }
}

overview_read!(OverviewCheckRead {
    required: i64,
    outcome: Option<String>,
});

overview_read!(OverviewDocumentRead {
    id: String,
    current_approved_revision_id: Option<String>,
    kind: String,
    document_lifecycle: String,
    current_draft_revision_id: Option<String>,
    approved_digest: Option<String>,
    approved_lifecycle: Option<String>,
    working_digest: Option<String>,
    working_lifecycle: Option<String>,
});

overview_read!(OverviewCandidateRead {
    id: String,
    question: String,
    options_json: String,
    selected_outcome: Option<String>,
    rationale: Option<String>,
    context_json: String,
    principal_type: Option<String>,
    principal_id: Option<String>,
    lifecycle: String,
    version: i64,
    created_at: String,
    updated_at: String,
});

overview_read!(OverviewDecisionRead {
    id: String,
    state: String,
    principal_type: String,
    principal_id: String,
    project_id: String,
    question: String,
    context_json: String,
    selected_outcome: String,
    rationale: String,
    decision_class: String,
    authority_basis: String,
    supersedes_decision_id: Option<String>,
    created_at: String,
    affected_records_json: String,
    options_json: String,
    source_refs_json: String,
});

overview_read!(OverviewReplacementRead {
    state: String,
    supersedes_decision_id: Option<String>,
});

overview_read!(OverviewEvidenceRead {
    availability: String,
    id: String,
    project_id: String,
    asset_id: String,
    task_id: Option<String>,
    source_task_id: Option<String>,
    source_execution_id: Option<String>,
    source_validation_id: Option<String>,
    source_task_version: Option<i64>,
    source_context_digest: Option<String>,
    source_definition_revision_id: Option<String>,
    milestone_id: Option<String>,
    caption: Option<String>,
    created_at: String,
    version: i64,
    deleted_at: Option<String>,
    evidence_kind: Option<String>,
    checksum: Option<String>,
    author_type: String,
    author_id: Option<String>,
    acceptance_check_ids_json: String,
});

overview_read!(OverviewReadinessRead {
    definition_revision_id: String,
    event_watermark: String,
    outcome: String,
    computing_policy_revision: String,
    readiness_digest: String,
    expected_milestone_version: i64,
    principal_type: String,
    principal_id: String,
    authorization_basis: String,
    authorization_action: String,
    explicit_event: String,
    authorization_occurred_at: String,
    id: String,
    project_id: String,
    milestone_id: String,
    created_at: String,
    input_manifest_json: String,
    blocking_reasons_json: String,
    check_results_json: String,
    waiver_manifest_json: String,
    evidence_manifest_json: String,
    commit_context_json: String,
});

overview_read!(OverviewReleaseRead {
    charter_revision_id: Option<String>,
    historic_charter_id: Option<String>,
    charter_content_digest: Option<String>,
    charter_render_version: Option<String>,
    charter_render_digest: Option<String>,
    decision_ids_json: String,
    task_references_json: String,
    validation_references_json: String,
    evidence_references_json: String,
    releasing_principal_type: String,
    releasing_principal_id: String,
    milestone_revision_id: String,
    milestone_definition_digest: String,
    readiness_snapshot_id: String,
    readiness_digest: String,
    snapshot_digest: String,
    release_identifier: String,
    expected_milestone_version: i64,
    source_event_watermark: String,
    authorization_basis: String,
    authorization_action: String,
    explicit_event: String,
    authorization_occurred_at: String,
    schema_version: String,
    milestone_id: String,
    milestone_key: String,
    release_revision: i64,
    display_label: Option<String>,
    summary: String,
    created_at: String,
    idempotency_key: String,
    id: String,
    release_sequence: i64,
    changelog: String,
    known_issues_json: String,
    document_revisions_json: String,
    git_references_json: String,
    waivers_json: String,
});

impl SqliteDb {
    pub async fn overview_current_charter(
        &self,
        revision_id: &str,
        charter_id: &str,
        project_id: &str,
    ) -> Result<Option<OverviewCharterRead>> {
        let row = sqlx::query(
            "SELECT c.id AS charter_id, c.project_id, c.project_mode, c.maturity,
                r.id AS revision_id,
                r.revision, r.base_revision, r.base_revision_id,
                r.lifecycle, r.schema_version,
                r.render_version, r.content_json, r.rendered_view,
                r.change_summary, r.author_type, r.author_id,
                r.source_refs_json, r.content_digest, r.rendered_digest,
                r.created_at,
                (SELECT ca.created_at
                   FROM project_charter_approval ca
                  WHERE ca.charter_id = c.id
                    AND ca.revision_id = r.id
                    AND ca.lifecycle IN ('active', 'consumed')
                  ORDER BY ca.created_at DESC, ca.id DESC
                  LIMIT 1) AS approved_at
         FROM project_charter c
         JOIN project_charter_revision r ON r.id = ? AND r.charter_id = c.id
         WHERE c.id = ? AND c.project_id = ?
           AND c.current_approved_revision_id = r.id
           AND c.lifecycle = 'attached'
           AND r.lifecycle = 'approved'",
        )
        .bind(revision_id)
        .bind(charter_id)
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|row| OverviewCharterRead { row }))
    }

    pub async fn overview_active_milestones(
        &self,
        project_id: &str,
    ) -> Result<Vec<OverviewMilestoneRead>> {
        let rows = sqlx::query(
            "SELECT id, project_id, milestone_sequence, milestone_key,
                display_label, current_definition_revision_id, lifecycle,
                blocker_reason_json, stale_reason_json,
                reconciliation_reason_json, version, created_at, updated_at
         FROM project_milestone
         WHERE project_id = ?
           AND (
                lifecycle IN ('active', 'ready_for_release')
                OR id = (SELECT primary_milestone_id FROM project WHERE id = ?)
           )
         ORDER BY milestone_sequence ASC, id ASC",
        )
        .bind(project_id)
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewMilestoneRead { row })
            .collect())
    }

    pub async fn overview_milestone_revision(
        &self,
        revision_id: &str,
        milestone_row_id: &str,
    ) -> Result<Option<OverviewMilestoneRevisionRead>> {
        let row = sqlx::query(
            "SELECT mr.id, mr.milestone_id, mr.revision, mr.base_revision, mr.lifecycle,
                    mr.display_label, mr.outcome, mr.included_scope_json,
                    mr.excluded_scope_json, mr.charter_revision_id,
                    mr.document_revisions_json, mr.task_selection_json,
                    mr.dependencies_json, mr.risks_json, mr.acceptance_checks_json,
                    mr.evidence_requirements_json, mr.known_issues_json,
                    mr.change_summary, mr.schema_version, mr.render_version, mr.rendered_view,
                    mr.content_digest, mr.rendered_digest, mr.author_type, mr.author_id,
                    mr.source_refs_json, mr.created_at,
                    mr.base_revision_id AS base_revision_id,
                    cr.charter_id AS charter_id,
                    cr.content_digest AS charter_content_digest,
                    cr.render_version AS charter_render_version,
                    cr.rendered_digest AS charter_render_digest
             FROM project_milestone_revision mr
             LEFT JOIN project_charter_revision cr
               ON cr.id = mr.charter_revision_id
             WHERE mr.id = ? AND mr.milestone_id = ?",
        )
        .bind(revision_id)
        .bind(milestone_row_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|row| OverviewMilestoneRevisionRead { row }))
    }

    pub async fn overview_acceptance_checks(
        &self,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
    ) -> Result<Vec<OverviewAcceptanceCheckRead>> {
        let rows = sqlx::query(
            "SELECT c.id, c.description, c.required, c.source_kind, c.expected_result,
                c.version, r.id AS result_id, r.outcome, r.input_digest
         FROM project_milestone_check c
         LEFT JOIN project_milestone_check_result r ON r.id = c.current_result_id
         WHERE c.project_id = ? AND c.milestone_id = ? AND c.definition_revision_id = ?
         ORDER BY c.check_key ASC, c.id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(definition_revision_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(OverviewAcceptanceCheckRead::from_row)
            .collect()
    }

    pub async fn overview_milestone_tasks(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> Result<Vec<OverviewTaskRead>> {
        let rows = sqlx::query(
            "SELECT t.status, t.condition_json
             FROM task t
             JOIN project_task_governance g ON g.task_id = t.id
             WHERE t.project_id = ? AND g.project_id = ? AND g.milestone_id = ?
               AND t.deleted_at IS NULL",
        )
        .bind(project_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(OverviewTaskRead::from_row).collect()
    }

    pub async fn overview_project_tasks(&self, project_id: &str) -> Result<Vec<OverviewTaskRead>> {
        let rows = sqlx::query(
            "SELECT status, condition_json FROM task
             WHERE project_id = ? AND deleted_at IS NULL",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(OverviewTaskRead::from_row).collect()
    }

    pub async fn overview_milestone_checks(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> Result<Vec<OverviewCheckRead>> {
        let rows = sqlx::query(
            "SELECT c.required, r.outcome
             FROM project_milestone_check c
             JOIN project_milestone m
               ON m.id = c.milestone_id AND m.project_id = c.project_id
              AND m.current_definition_revision_id = c.definition_revision_id
             LEFT JOIN project_milestone_check_result r
               ON r.id = COALESCE(
                    c.current_result_id,
                    (SELECT r2.id FROM project_milestone_check_result r2
                     WHERE r2.check_id = c.id
                       AND r2.definition_revision_id = c.definition_revision_id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1)
                  )
              AND r.definition_revision_id = c.definition_revision_id
             WHERE c.project_id = ? AND c.milestone_id = ?",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewCheckRead { row })
            .collect())
    }

    pub async fn overview_project_checks(
        &self,
        project_id: &str,
    ) -> Result<Vec<OverviewCheckRead>> {
        let rows = sqlx::query(
            "SELECT c.required, r.outcome
             FROM project_milestone_check c
             JOIN project_milestone m
               ON m.id = c.milestone_id AND m.project_id = c.project_id
              AND m.current_definition_revision_id = c.definition_revision_id
             LEFT JOIN project_milestone_check_result r
               ON r.id = COALESCE(
                    c.current_result_id,
                    (SELECT r2.id FROM project_milestone_check_result r2
                     WHERE r2.check_id = c.id
                       AND r2.definition_revision_id = c.definition_revision_id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1)
                  )
              AND r.definition_revision_id = c.definition_revision_id
             WHERE c.project_id = ?",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewCheckRead { row })
            .collect())
    }

    pub async fn overview_document_freshness(
        &self,
        project_id: &str,
    ) -> Result<Vec<OverviewDocumentRead>> {
        let rows = sqlx::query(
            "SELECT d.id, d.kind, d.lifecycle AS document_lifecycle,
                d.current_draft_revision_id,
                d.current_approved_revision_id,
                approved.content_digest AS approved_digest,
                approved.lifecycle AS approved_lifecycle,
                working.content_digest AS working_digest,
                working.lifecycle AS working_lifecycle
         FROM project_document d
         LEFT JOIN project_document_revision approved
           ON approved.id = d.current_approved_revision_id
          AND approved.document_id = d.id
         LEFT JOIN project_document_revision working
           ON working.id = d.current_draft_revision_id
          AND working.document_id = d.id
         WHERE d.project_id = ?
         ORDER BY d.updated_at DESC, d.id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewDocumentRead { row })
            .collect())
    }

    pub async fn overview_pending_decisions(
        &self,
        project_id: &str,
    ) -> Result<Vec<OverviewCandidateRead>> {
        let rows = sqlx::query(
            "SELECT id, lifecycle, question, context_json, options_json,
                selected_outcome, rationale, principal_type, principal_id,
                version, created_at, updated_at
         FROM project_decision_candidate
         WHERE project_id = ? AND lifecycle IN ('draft', 'proposed')
         ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewCandidateRead { row })
            .collect())
    }

    pub async fn overview_decisions(&self, project_id: &str) -> Result<Vec<OverviewDecisionRead>> {
        let rows = sqlx::query(
            "SELECT id, project_id, state, decision_class, question, context_json,
                options_json, selected_outcome, rationale, principal_type,
                principal_id, authority_basis, source_refs_json,
                affected_records_json, supersedes_decision_id, created_at
         FROM project_decision
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC
         LIMIT 64",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewDecisionRead { row })
            .collect())
    }

    pub async fn overview_decision_replacements(
        &self,
        project_id: &str,
    ) -> Result<Vec<OverviewReplacementRead>> {
        let rows = sqlx::query(
            "SELECT supersedes_decision_id, state FROM project_decision
         WHERE project_id = ? AND supersedes_decision_id IS NOT NULL
         ORDER BY created_at DESC, id DESC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewReplacementRead { row })
            .collect())
    }

    pub async fn overview_evidence(&self, project_id: &str) -> Result<Vec<OverviewEvidenceRead>> {
        let rows = sqlx::query(
            "SELECT a.id, a.project_id, a.asset_id, a.task_id,
                a.source_task_id, a.source_execution_id, a.source_validation_id,
                a.source_task_version, a.source_context_digest,
                a.source_definition_revision_id,
                a.milestone_id, a.acceptance_check_ids_json, a.caption,
                a.evidence_kind, COALESCE(a.checksum, m.checksum) AS checksum,
                CASE WHEN m.availability != 'available' THEN m.availability
                     ELSE a.availability END AS availability,
                a.author_type, a.author_id, a.created_at,
                a.updated_at, a.deleted_at, a.version
         FROM project_media_attachment a
         JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
         WHERE a.project_id = ? AND a.attachment_kind = 'evidence'
         ORDER BY a.created_at ASC, a.id ASC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewEvidenceRead { row })
            .collect())
    }

    pub async fn overview_latest_readiness(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> Result<Option<OverviewReadinessRead>> {
        let row = sqlx::query(
            "SELECT id, project_id, milestone_id, definition_revision_id,
                input_manifest_json, event_watermark, outcome,
                blocking_reasons_json, check_results_json, waiver_manifest_json,
                evidence_manifest_json, commit_context_json,
                computing_policy_revision, readiness_digest,
                principal_type, principal_id, authorization_basis,
                authorization_action, authorization_occurred_at,
                expected_milestone_version, explicit_event, created_at
         FROM project_readiness_snapshot
         WHERE project_id = ? AND milestone_id = ?
         ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|row| OverviewReadinessRead { row }))
    }

    pub async fn overview_releases(&self, project_id: &str) -> Result<Vec<OverviewReleaseRead>> {
        let rows = sqlx::query(
            "SELECT r.id, r.project_id, r.milestone_id, r.release_sequence,
                r.release_revision, r.release_identifier,
                r.milestone_revision_id, r.readiness_snapshot_id,
                r.readiness_digest,
                r.summary, r.changelog, r.known_issues_json,
                r.charter_revision_id, r.document_revisions_json,
                r.decision_ids_json, r.task_references_json,
                r.validation_references_json, r.git_references_json,
                r.evidence_references_json, r.waivers_json,
                r.releasing_principal_type, r.releasing_principal_id,
                r.authorization_basis, r.authorization_action,
                r.explicit_event, r.authorization_occurred_at,
                r.schema_version, r.snapshot_digest, r.idempotency_key,
                r.created_at, m.milestone_key, m.display_label,
                mr.content_digest AS milestone_definition_digest,
                rs.expected_milestone_version,
                rs.event_watermark AS source_event_watermark,
                cr.charter_id AS historic_charter_id,
                cr.content_digest AS charter_content_digest,
                cr.render_version AS charter_render_version,
                cr.rendered_digest AS charter_render_digest
         FROM project_release r
         JOIN project_milestone m ON m.id = r.milestone_id AND m.project_id = r.project_id
         JOIN project_milestone_revision mr
           ON mr.id = r.milestone_revision_id AND mr.milestone_id = r.milestone_id
         JOIN project_readiness_snapshot rs
           ON rs.id = r.readiness_snapshot_id
          AND rs.project_id = r.project_id
          AND rs.milestone_id = r.milestone_id
         LEFT JOIN project_charter_revision cr ON cr.id = r.charter_revision_id
         WHERE r.project_id = ?
         ORDER BY r.created_at DESC, r.id DESC",
        )
        .bind(project_id)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OverviewReleaseRead { row })
            .collect())
    }

    pub async fn overview_event_watermark(&self, project_id: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT id FROM domain_event
         WHERE scope_type = 'project' AND scope_id = ?
         ORDER BY sequence DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.pool())
        .await?;
        row.map(|row| row.try_get("id").map_err(DbError::from))
            .transpose()
    }

    pub async fn overview_failed_task_count(&self, project_id: &str) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task
         WHERE project_id = ? AND deleted_at IS NULL
           AND (lower(status) LIKE '%failed%'
                OR lower(status) LIKE '%error%'
                OR json_extract(condition_json, '$.evidence.presentation.hard_failure') = 1)",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await
        .map_err(DbError::from)
    }

    pub async fn overview_reconciliation_required(&self, project_id: &str) -> Result<bool> {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
             SELECT 1 FROM project_reconciliation_record
             WHERE project_id = ? AND state = 'required'
         )",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await
        .map(|value| value != 0)
        .map_err(DbError::from)
    }
}
