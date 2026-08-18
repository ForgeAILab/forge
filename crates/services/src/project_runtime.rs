//! Canonical, scope-bound Project runtime projection.
//!
//! The Project Agent prompt and the native `project.current_state` read must
//! consume the same projection.  This module is the single read path for the
//! effective Project state; it deliberately returns references, digests, and
//! bounded summaries rather than copying arbitrary artifact bodies into the
//! runtime context.

use std::collections::BTreeMap;

use db::SqliteDb;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::{Result, ServiceError};

const DEFAULT_LIMIT: i64 = 32;
const MAX_LIMIT: i64 = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectEffectiveStateProjection {
    pub project: ProjectIdentityProjection,
    pub governing_charter: Option<CharterReferenceProjection>,
    pub active_execution_baseline: Option<ExecutionBaselineReferenceProjection>,
    pub approved_documents: Vec<ApprovedDocumentProjection>,
    pub active_decisions: Vec<DecisionProjection>,
    pub invalidated_decisions: Vec<DecisionProjection>,
    pub reconciliation_required: Vec<ReconciliationProjection>,
    pub canonical_conflicts: Vec<CanonicalConflictProjection>,
    pub task_summary: TaskSummaryProjection,
    pub validation_summary: ValidationSummaryProjection,
    pub commitments: Vec<ProjectCommitmentProjection>,
    pub inbox: Vec<ProjectInboxProjection>,
    pub active_milestones: Vec<MilestoneProjection>,
    pub primary_milestone_id: Option<String>,
    pub readiness: ReadinessProjection,
    pub releases: Vec<ReleaseProjection>,
    pub unreleased_changes: UnreleasedChangesProjection,
    pub source_event_watermark: String,
    pub source_event_sequence: i64,
    pub source_project_version: i64,
    pub source_project_work_epoch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectCurrentStateResponse {
    pub scope: String,
    pub effective_state: ProjectEffectiveStateProjection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectIdentityProjection {
    pub id: String,
    pub name: String,
    pub paused: bool,
    pub charter_status: String,
    pub charter_setup_required: bool,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CharterReferenceProjection {
    pub id: String,
    pub revision_id: String,
    pub revision: i64,
    pub version: i64,
    pub content_digest: String,
    pub render_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBaselineReferenceProjection {
    pub id: String,
    pub revision_id: String,
    pub revision: i64,
    pub version: i64,
    pub lifecycle: String,
    pub charter_revision_id: String,
    pub content_digest: String,
    pub render_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ApprovedDocumentProjection {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub revision_id: String,
    pub revision: i64,
    pub version: i64,
    pub lifecycle: String,
    pub content_digest: String,
    pub render_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DecisionProjection {
    pub id: String,
    pub state: String,
    pub decision_class: String,
    pub question: String,
    pub selected_outcome: String,
    pub rationale: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authority_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub charter_revision_id: Option<String>,
    pub baseline_revision_id: Option<String>,
    pub source_refs: Vec<Value>,
    pub affected_records: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationProjection {
    pub id: String,
    pub conflict_id: String,
    pub record_type: String,
    pub record_id: String,
    pub record_revision: String,
    pub record_digest: String,
    pub state: String,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CanonicalConflictProjection {
    pub id: String,
    pub domain: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub governing_record_revision: String,
    pub governing_record_digest: String,
    pub conflicting_record_type: String,
    pub conflicting_record_id: String,
    pub conflicting_record_revision: String,
    pub conflicting_record_digest: String,
    pub affected_paths: Vec<String>,
    pub conflict_code: String,
    pub description: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CountProjection {
    pub key: String,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskSummaryProjection {
    pub total: i64,
    pub by_status: Vec<CountProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ValidationSummaryProjection {
    pub total: i64,
    pub by_outcome: Vec<CountProjection>,
}

/// Bounded, identity-bound Project commitments.  The runtime receives
/// lifecycle metadata and evidence counts, never the free-form description or
/// evidence body.  A completed commitment without an evidence row is not a
/// valid effective state and is rejected by `validate_effective_state`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectCommitmentProjection {
    pub id: String,
    pub status: String,
    pub due_at: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: bool,
    pub evidence_count: i64,
    pub blocked_reason: Option<String>,
    pub version: i64,
    pub updated_at: String,
}

/// Bounded, identity-bound Project inbox records.  Payload and body content
/// stay behind the explicit inbox read tool; the effective state carries only
/// provenance needed for reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectInboxProjection {
    pub id: String,
    pub kind: String,
    pub status: String,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub correlation_id: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MilestoneProjection {
    pub id: String,
    pub milestone_key: String,
    pub display_label: Option<String>,
    pub lifecycle: String,
    pub definition_revision_id: Option<String>,
    pub definition_digest: Option<String>,
    pub version: i64,
    pub blocker_reasons: Vec<Value>,
    pub stale_reasons: Vec<Value>,
    pub reconciliation_reasons: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadinessProjection {
    pub latest: Option<ReadinessSnapshotProjection>,
    pub by_milestone: Vec<ReadinessSnapshotProjection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReadinessSnapshotProjection {
    pub id: String,
    pub milestone_id: String,
    pub definition_revision_id: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub event_watermark: String,
    pub outcome: String,
    pub blocking_reasons: Vec<Value>,
    pub readiness_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseProjection {
    pub id: String,
    pub milestone_id: String,
    pub release_sequence: i64,
    pub release_revision: i64,
    pub release_identifier: String,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub snapshot_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UnreleasedChangesProjection {
    pub document_ids: Vec<String>,
    pub decision_candidate_ids: Vec<String>,
    pub baseline_revision_ids: Vec<String>,
    pub active_milestone_ids: Vec<String>,
    pub reconciliation_ids: Vec<String>,
}

/// Load the one canonical Project projection used by both the Project Agent
/// prompt and the native `project.current_state` tool. `limit` is a server
/// bounded presentation limit; it never changes scope or authority.
pub async fn load_effective_project_state(
    db: &SqliteDb,
    project_id: &str,
    limit: Option<i64>,
) -> Result<ProjectEffectiveStateProjection> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let project = sqlx::query(
        "SELECT id, name, paused_at, charter_status, charter_setup_required,
                version, created_at, updated_at, project_work_epoch,
                primary_milestone_id
         FROM project WHERE id = ?",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;

    let project_version: i64 = project.try_get("version")?;
    let project_work_epoch: i64 = project.try_get("project_work_epoch")?;
    let primary_milestone_id: Option<String> = project.try_get("primary_milestone_id")?;
    let identity = ProjectIdentityProjection {
        id: project.try_get("id")?,
        name: project.try_get("name")?,
        paused: project.try_get::<Option<String>, _>("paused_at")?.is_some(),
        charter_status: project.try_get("charter_status")?,
        charter_setup_required: project.try_get::<i64, _>("charter_setup_required")? != 0,
        version: project_version,
        created_at: project.try_get("created_at")?,
        updated_at: project.try_get("updated_at")?,
    };

    let governing_charter = sqlx::query(
        "SELECT c.id, c.version, r.id AS revision_id, r.revision,
                r.content_digest, r.rendered_digest
         FROM project_charter c
         JOIN project_charter_revision r
           ON r.id = c.current_approved_revision_id
          AND r.charter_id = c.id
         WHERE c.project_id = ?
           AND c.lifecycle = 'attached'
           AND r.lifecycle = 'approved'
         ORDER BY c.updated_at DESC, c.id DESC LIMIT 1",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .map(|row| {
        Ok::<_, sqlx::Error>(CharterReferenceProjection {
            id: row.try_get("id")?,
            revision_id: row.try_get("revision_id")?,
            revision: row.try_get("revision")?,
            version: row.try_get("version")?,
            content_digest: row.try_get("content_digest")?,
            render_digest: row.try_get("rendered_digest")?,
        })
    })
    .transpose()?;

    let active_execution_baseline = sqlx::query(
        "SELECT b.id, b.current_revision_id AS revision_id, b.version,
                b.lifecycle, r.revision, r.charter_revision_id,
                r.content_digest, r.rendered_digest,
                r.release_policy_revision, r.release_policy_digest
         FROM project_execution_baseline b
         JOIN project_execution_baseline_revision r
           ON r.id = b.current_revision_id AND r.baseline_id = b.id
         WHERE b.project_id = ? AND b.lifecycle = 'active'
           AND r.lifecycle = 'approved'
         ORDER BY b.updated_at DESC, b.id DESC LIMIT 1",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .map(|row| {
        Ok::<_, sqlx::Error>(ExecutionBaselineReferenceProjection {
            id: row.try_get("id")?,
            revision_id: row.try_get("revision_id")?,
            revision: row.try_get("revision")?,
            version: row.try_get("version")?,
            lifecycle: row.try_get("lifecycle")?,
            charter_revision_id: row.try_get("charter_revision_id")?,
            content_digest: row.try_get("content_digest")?,
            render_digest: row.try_get("rendered_digest")?,
            release_policy_revision: row.try_get("release_policy_revision")?,
            release_policy_digest: row.try_get("release_policy_digest")?,
        })
    })
    .transpose()?;

    let approved_documents = sqlx::query(
        "SELECT d.id, d.kind, d.title, d.current_approved_revision_id AS revision_id,
                d.version, d.lifecycle, r.revision, r.content_digest,
                r.rendered_digest
         FROM project_document d
         JOIN project_document_revision r
           ON r.id = d.current_approved_revision_id AND r.document_id = d.id
         WHERE d.project_id = ? AND r.lifecycle = 'approved'
         ORDER BY d.updated_at DESC, d.id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(ApprovedDocumentProjection {
            id: row.try_get("id")?,
            kind: row.try_get("kind")?,
            title: row.try_get("title")?,
            revision_id: row.try_get("revision_id")?,
            revision: row.try_get("revision")?,
            version: row.try_get("version")?,
            lifecycle: row.try_get("lifecycle")?,
            content_digest: row.try_get("content_digest")?,
            render_digest: row.try_get("rendered_digest")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let decision_rows = sqlx::query(
        "SELECT id, state, decision_class, question, selected_outcome,
                rationale, principal_type, principal_id, authority_basis,
                authorization_action, explicit_event, authorization_occurred_at,
                charter_revision_id, baseline_revision_id, source_refs_json,
                affected_records_json, created_at
         FROM project_decision
         WHERE project_id = ? AND state IN ('active', 'invalidated')
         ORDER BY created_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    let mut active_decisions = Vec::new();
    let mut invalidated_decisions = Vec::new();
    for row in decision_rows {
        let decision = DecisionProjection {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            decision_class: row.try_get("decision_class")?,
            question: row.try_get("question")?,
            selected_outcome: row.try_get("selected_outcome")?,
            rationale: row.try_get("rationale")?,
            principal_type: row.try_get("principal_type")?,
            principal_id: row.try_get("principal_id")?,
            authority_basis: row.try_get("authority_basis")?,
            authorization_action: row.try_get("authorization_action")?,
            explicit_event: row.try_get("explicit_event")?,
            authorization_occurred_at: row.try_get("authorization_occurred_at")?,
            charter_revision_id: row.try_get("charter_revision_id")?,
            baseline_revision_id: row.try_get("baseline_revision_id")?,
            source_refs: json_value_array(row.try_get("source_refs_json")?)?,
            affected_records: json_object_value(row.try_get("affected_records_json")?)?,
            created_at: row.try_get("created_at")?,
        };
        if decision.state == "invalidated" {
            invalidated_decisions.push(decision);
        } else {
            active_decisions.push(decision);
        }
    }

    let reconciliation_required = sqlx::query(
        "SELECT id, conflict_id, record_type, record_id, record_revision,
                record_digest, state, version, updated_at
         FROM project_reconciliation_record
         WHERE project_id = ? AND state = 'required'
         ORDER BY updated_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(ReconciliationProjection {
            id: row.try_get("id")?,
            conflict_id: row.try_get("conflict_id")?,
            record_type: row.try_get("record_type")?,
            record_id: row.try_get("record_id")?,
            record_revision: row.try_get("record_revision")?,
            record_digest: row.try_get("record_digest")?,
            state: row.try_get("state")?,
            version: row.try_get("version")?,
            updated_at: row.try_get("updated_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let canonical_conflicts = sqlx::query(
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
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(CanonicalConflictProjection {
            id: row.try_get("id")?,
            domain: row.try_get("domain")?,
            governing_record_type: row.try_get("governing_record_type")?,
            governing_record_id: row.try_get("governing_record_id")?,
            governing_record_revision: row.try_get("governing_record_revision")?,
            governing_record_digest: row.try_get("governing_record_digest")?,
            conflicting_record_type: row.try_get("conflicting_record_type")?,
            conflicting_record_id: row.try_get("conflicting_record_id")?,
            conflicting_record_revision: row.try_get("conflicting_record_revision")?,
            conflicting_record_digest: row.try_get("conflicting_record_digest")?,
            affected_paths: json_string_array(row.try_get("affected_paths_json")?)?,
            conflict_code: row.try_get("conflict_code")?,
            description: row.try_get("description")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let task_counts = sqlx::query(
        "SELECT status AS key, COUNT(*) AS count
         FROM task WHERE project_id = ? AND deleted_at IS NULL
         GROUP BY status ORDER BY status ASC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(CountProjection {
            key: row.try_get("key")?,
            count: row.try_get("count")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    let task_total = task_counts.iter().map(|item| item.count).sum();

    let validation_counts = sqlx::query(
        "SELECT COALESCE(r.outcome, 'missing') AS key, COUNT(*) AS count
         FROM project_milestone_check c
         LEFT JOIN project_milestone_check_result r ON r.id = c.current_result_id
         WHERE c.project_id = ?
         GROUP BY COALESCE(r.outcome, 'missing') ORDER BY key ASC",
    )
    .bind(project_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(CountProjection {
            key: row.try_get("key")?,
            count: row.try_get("count")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;
    let validation_total = validation_counts.iter().map(|item| item.count).sum();

    // Coordination state is scoped through the active Project Agent binding
    // before retrieval.  This prevents a Project projection from exposing a
    // different identity's private obligations or inbox while still making
    // open reconciliation work visible after a binding/session rotation.
    let commitments = sqlx::query(
        "SELECT c.id, c.status, c.due_at, c.originating_task_id,
                c.evidence_required, c.blocked_reason, c.version, c.updated_at,
                (SELECT COUNT(*) FROM agent_commitment_evidence e
                 WHERE e.commitment_id = c.id) AS evidence_count
         FROM agent_commitment c
         JOIN project_agent_binding b
           ON b.project_id = c.scope_id
          AND b.identity_id = c.owner_identity_id
          AND b.state = 'active'
         WHERE c.scope_type = 'project' AND c.scope_id = ?
         ORDER BY COALESCE(c.due_at, c.updated_at) ASC, c.updated_at ASC, c.id ASC
         LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(ProjectCommitmentProjection {
            id: row.try_get("id")?,
            status: row.try_get("status")?,
            due_at: row.try_get("due_at")?,
            originating_task_id: row.try_get("originating_task_id")?,
            evidence_required: row.try_get::<i64, _>("evidence_required")? != 0,
            evidence_count: row.try_get("evidence_count")?,
            blocked_reason: row.try_get("blocked_reason")?,
            version: row.try_get("version")?,
            updated_at: row.try_get("updated_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let inbox = sqlx::query(
        "SELECT i.id, i.kind, i.status, i.source_type, i.source_id,
                i.correlation_id, i.version, i.created_at, i.updated_at
         FROM agent_inbox_item i
         JOIN project_agent_binding b
           ON b.project_id = i.scope_id
          AND b.identity_id = i.recipient_identity_id
          AND b.state = 'active'
         WHERE i.scope_type = 'project' AND i.scope_id = ?
           AND i.status <> 'dismissed'
         ORDER BY i.updated_at DESC, i.id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(ProjectInboxProjection {
            id: row.try_get("id")?,
            kind: row.try_get("kind")?,
            status: row.try_get("status")?,
            source_type: row.try_get("source_type")?,
            source_id: row.try_get("source_id")?,
            correlation_id: row.try_get("correlation_id")?,
            version: row.try_get("version")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let milestone_rows = sqlx::query(
        "SELECT m.id, m.milestone_key, m.display_label, m.lifecycle,
                m.current_definition_revision_id, m.version,
                m.blocker_reason_json, m.stale_reason_json,
                m.reconciliation_reason_json,
                r.content_digest AS definition_digest
         FROM project_milestone m
         LEFT JOIN project_milestone_revision r
           ON r.id = m.current_definition_revision_id AND r.milestone_id = m.id
         WHERE m.project_id = ?
           AND m.lifecycle IN ('planned', 'active', 'ready_for_release')
         ORDER BY m.milestone_sequence ASC, m.id ASC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    let active_milestones = milestone_rows
        .into_iter()
        .map(|row| {
            Ok(MilestoneProjection {
                id: row.try_get("id")?,
                milestone_key: row.try_get("milestone_key")?,
                display_label: row.try_get("display_label")?,
                lifecycle: row.try_get("lifecycle")?,
                definition_revision_id: row.try_get("current_definition_revision_id")?,
                definition_digest: row.try_get("definition_digest")?,
                version: row.try_get("version")?,
                blocker_reasons: json_value_array(row.try_get("blocker_reason_json")?)?,
                stale_reasons: json_value_array(row.try_get("stale_reason_json")?)?,
                reconciliation_reasons: json_value_array(
                    row.try_get("reconciliation_reason_json")?,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    validate_primary_milestone_pointer(&active_milestones, primary_milestone_id.as_deref())?;

    let readiness_rows = sqlx::query(
        "SELECT id, milestone_id, definition_revision_id, baseline_id,
                baseline_revision_id, baseline_digest, release_policy_revision,
                release_policy_digest, event_watermark, outcome,
                blocking_reasons_json, readiness_digest, created_at
         FROM project_readiness_snapshot
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit.saturating_mul(4))
    .fetch_all(db.pool())
    .await?;
    let mut seen_milestones = BTreeMap::new();
    for row in readiness_rows {
        let milestone_id: String = row.try_get("milestone_id")?;
        if seen_milestones.contains_key(&milestone_id) {
            continue;
        }
        let snapshot = ReadinessSnapshotProjection {
            id: row.try_get("id")?,
            milestone_id: milestone_id.clone(),
            definition_revision_id: row.try_get("definition_revision_id")?,
            baseline_id: row.try_get("baseline_id")?,
            baseline_revision_id: row.try_get("baseline_revision_id")?,
            baseline_digest: row.try_get("baseline_digest")?,
            release_policy_revision: row.try_get("release_policy_revision")?,
            release_policy_digest: row.try_get("release_policy_digest")?,
            event_watermark: row.try_get("event_watermark")?,
            outcome: row.try_get("outcome")?,
            blocking_reasons: json_value_array(row.try_get("blocking_reasons_json")?)?,
            readiness_digest: row.try_get("readiness_digest")?,
            created_at: row.try_get("created_at")?,
        };
        seen_milestones.insert(milestone_id, snapshot);
    }
    let by_milestone = seen_milestones.into_values().collect::<Vec<_>>();
    let latest = by_milestone.first().cloned();

    let releases = sqlx::query(
        "SELECT id, milestone_id, release_sequence, release_revision,
                release_identifier, readiness_snapshot_id, readiness_digest,
                baseline_id, baseline_revision_id, baseline_digest, snapshot_digest,
                created_at
         FROM project_release
         WHERE project_id = ?
         ORDER BY created_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok(ReleaseProjection {
            id: row.try_get("id")?,
            milestone_id: row.try_get("milestone_id")?,
            release_sequence: row.try_get("release_sequence")?,
            release_revision: row.try_get("release_revision")?,
            release_identifier: row.try_get("release_identifier")?,
            readiness_snapshot_id: row.try_get("readiness_snapshot_id")?,
            readiness_digest: row.try_get("readiness_digest")?,
            baseline_id: row.try_get("baseline_id")?,
            baseline_revision_id: row.try_get("baseline_revision_id")?,
            baseline_digest: row.try_get("baseline_digest")?,
            snapshot_digest: row.try_get("snapshot_digest")?,
            created_at: row.try_get("created_at")?,
        })
    })
    .collect::<Result<Vec<_>>>()?;

    let document_ids = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_document
         WHERE project_id = ? AND current_draft_revision_id IS NOT NULL
           AND (current_approved_revision_id IS NULL
                OR current_draft_revision_id != current_approved_revision_id)
         ORDER BY updated_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    let decision_candidate_ids = sqlx::query_scalar::<_, String>(
        "SELECT id FROM project_decision_candidate
         WHERE project_id = ? AND lifecycle IN ('draft', 'proposed')
         ORDER BY updated_at DESC, id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    let baseline_revision_ids = sqlx::query_scalar::<_, String>(
        "SELECT r.id FROM project_execution_baseline_revision r
         JOIN project_execution_baseline b ON b.id = r.baseline_id
         WHERE b.project_id = ? AND r.lifecycle IN ('draft', 'proposed')
         ORDER BY r.created_at DESC, r.id DESC LIMIT ?",
    )
    .bind(project_id)
    .bind(limit)
    .fetch_all(db.pool())
    .await?;
    let active_milestone_ids = active_milestones
        .iter()
        .map(|milestone| milestone.id.clone())
        .collect();
    let reconciliation_ids = reconciliation_required
        .iter()
        .map(|record| record.id.clone())
        .collect();

    let watermark_row = sqlx::query(
        "SELECT id, sequence FROM domain_event
         WHERE scope_type = 'project' AND scope_id = ?
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?;
    let (source_event_watermark, source_event_sequence) = match watermark_row {
        Some(row) => (row.try_get("id")?, row.try_get("sequence")?),
        None => (format!("project-work-epoch:{project_work_epoch}"), 0),
    };

    let projection = ProjectEffectiveStateProjection {
        project: identity,
        governing_charter,
        active_execution_baseline,
        approved_documents,
        active_decisions,
        invalidated_decisions,
        reconciliation_required,
        canonical_conflicts,
        task_summary: TaskSummaryProjection {
            total: task_total,
            by_status: task_counts,
        },
        validation_summary: ValidationSummaryProjection {
            total: validation_total,
            by_outcome: validation_counts,
        },
        commitments,
        inbox,
        active_milestones,
        primary_milestone_id,
        readiness: ReadinessProjection {
            latest,
            by_milestone,
        },
        releases,
        unreleased_changes: UnreleasedChangesProjection {
            document_ids,
            decision_candidate_ids,
            baseline_revision_ids,
            active_milestone_ids,
            reconciliation_ids,
        },
        source_event_watermark,
        source_event_sequence,
        source_project_version: project_version,
        source_project_work_epoch: project_work_epoch,
    };
    validate_effective_state(&projection)?;
    Ok(projection)
}

fn validate_effective_state(projection: &ProjectEffectiveStateProjection) -> Result<()> {
    require_nonempty("Project identity id", &projection.project.id)?;
    require_nonempty("Project identity name", &projection.project.name)?;
    require_nonempty(
        "Project identity Charter status",
        &projection.project.charter_status,
    )?;
    require_nonempty(
        "Project identity created_at",
        &projection.project.created_at,
    )?;
    require_nonempty(
        "Project identity updated_at",
        &projection.project.updated_at,
    )?;
    if projection.project.version < 1 || projection.source_project_version < 1 {
        return Err(ServiceError::invalid_operation(
            "Project effective state has an invalid Project version",
        ));
    }
    if projection.source_project_work_epoch < 0 || projection.source_event_sequence < 0 {
        return Err(ServiceError::invalid_operation(
            "Project effective state has an invalid source watermark",
        ));
    }
    require_nonempty(
        "Project effective-state event watermark",
        &projection.source_event_watermark,
    )?;
    if projection.project.charter_status == "charter_backed"
        && projection.governing_charter.is_none()
    {
        return Err(ServiceError::conflict(
            "Charter-backed Project has no current approved Charter in effective state",
        ));
    }
    if let Some(charter) = projection.governing_charter.as_ref() {
        for (field, value) in [
            ("Charter id", charter.id.as_str()),
            ("Charter revision id", charter.revision_id.as_str()),
            ("Charter content digest", charter.content_digest.as_str()),
            ("Charter render digest", charter.render_digest.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        if charter.revision < 1 || charter.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid Charter revision",
            ));
        }
    }
    if let Some(baseline) = projection.active_execution_baseline.as_ref() {
        for (field, value) in [
            ("Execution baseline id", baseline.id.as_str()),
            (
                "Execution baseline revision id",
                baseline.revision_id.as_str(),
            ),
            (
                "Execution baseline Charter revision id",
                baseline.charter_revision_id.as_str(),
            ),
            (
                "Execution baseline content digest",
                baseline.content_digest.as_str(),
            ),
            (
                "Execution baseline render digest",
                baseline.render_digest.as_str(),
            ),
            (
                "Execution baseline release-policy revision",
                baseline.release_policy_revision.as_str(),
            ),
            (
                "Execution baseline release-policy digest",
                baseline.release_policy_digest.as_str(),
            ),
        ] {
            require_nonempty(field, value)?;
        }
        if baseline.revision < 1 || baseline.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid execution baseline revision",
            ));
        }
    }
    for document in &projection.approved_documents {
        for (field, value) in [
            ("Project Document id", document.id.as_str()),
            ("Project Document kind", document.kind.as_str()),
            (
                "Project Document revision id",
                document.revision_id.as_str(),
            ),
            (
                "Project Document content digest",
                document.content_digest.as_str(),
            ),
            (
                "Project Document render digest",
                document.render_digest.as_str(),
            ),
        ] {
            require_nonempty(field, value)?;
        }
        if document.revision < 1 || document.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid Project Document revision",
            ));
        }
    }
    for decision in projection
        .active_decisions
        .iter()
        .chain(projection.invalidated_decisions.iter())
    {
        for (field, value) in [
            ("Project Decision id", decision.id.as_str()),
            ("Project Decision state", decision.state.as_str()),
            ("Project Decision class", decision.decision_class.as_str()),
            ("Project Decision question", decision.question.as_str()),
            (
                "Project Decision outcome",
                decision.selected_outcome.as_str(),
            ),
            (
                "Project Decision principal type",
                decision.principal_type.as_str(),
            ),
            (
                "Project Decision principal id",
                decision.principal_id.as_str(),
            ),
            (
                "Project Decision authority basis",
                decision.authority_basis.as_str(),
            ),
            (
                "Project Decision authorization action",
                decision.authorization_action.as_str(),
            ),
            (
                "Project Decision explicit event",
                decision.explicit_event.as_str(),
            ),
            (
                "Project Decision authorization occurred_at",
                decision.authorization_occurred_at.as_str(),
            ),
            ("Project Decision created_at", decision.created_at.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        if decision.source_refs.len() > 64 {
            return Err(ServiceError::invalid_operation(
                "Project Decision source references exceed the runtime bound",
            ));
        }
        let affected_records_size = serde_json::to_string(&decision.affected_records)
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "Project Decision affected records cannot be serialized: {error}"
                ))
            })?
            .len();
        if affected_records_size > 16 * 1024 {
            return Err(ServiceError::invalid_operation(
                "Project Decision affected records exceed the runtime bound",
            ));
        }
        if let Some(revision_id) = decision.baseline_revision_id.as_deref() {
            require_nonempty("Project Decision baseline revision id", revision_id)?;
        }
        if let Some(revision_id) = decision.charter_revision_id.as_deref() {
            require_nonempty("Project Decision Charter revision id", revision_id)?;
        }
    }
    for record in &projection.reconciliation_required {
        for (field, value) in [
            ("Reconciliation id", record.id.as_str()),
            ("Reconciliation conflict id", record.conflict_id.as_str()),
            ("Reconciliation record type", record.record_type.as_str()),
            ("Reconciliation record id", record.record_id.as_str()),
            (
                "Reconciliation record revision",
                record.record_revision.as_str(),
            ),
            (
                "Reconciliation record digest",
                record.record_digest.as_str(),
            ),
            ("Reconciliation state", record.state.as_str()),
            ("Reconciliation updated_at", record.updated_at.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        if record.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid reconciliation version",
            ));
        }
    }
    for conflict in &projection.canonical_conflicts {
        for (field, value) in [
            ("Canonical conflict id", conflict.id.as_str()),
            ("Canonical conflict domain", conflict.domain.as_str()),
            (
                "Canonical conflict governing record id",
                conflict.governing_record_id.as_str(),
            ),
            (
                "Canonical conflict governing record revision",
                conflict.governing_record_revision.as_str(),
            ),
            (
                "Canonical conflict governing record digest",
                conflict.governing_record_digest.as_str(),
            ),
            (
                "Canonical conflict conflicting record id",
                conflict.conflicting_record_id.as_str(),
            ),
            (
                "Canonical conflict conflicting record revision",
                conflict.conflicting_record_revision.as_str(),
            ),
            (
                "Canonical conflict conflicting record digest",
                conflict.conflicting_record_digest.as_str(),
            ),
            ("Canonical conflict code", conflict.conflict_code.as_str()),
            (
                "Canonical conflict created_at",
                conflict.created_at.as_str(),
            ),
        ] {
            require_nonempty(field, value)?;
        }
    }
    for counts in [
        projection.task_summary.by_status.as_slice(),
        projection.validation_summary.by_outcome.as_slice(),
    ] {
        for count in counts {
            require_nonempty("Project effective-state count key", &count.key)?;
            if count.count < 0 {
                return Err(ServiceError::invalid_operation(
                    "Project effective state has a negative count",
                ));
            }
        }
    }
    let task_count_total = projection
        .task_summary
        .by_status
        .iter()
        .map(|count| count.count)
        .sum::<i64>();
    let validation_count_total = projection
        .validation_summary
        .by_outcome
        .iter()
        .map(|count| count.count)
        .sum::<i64>();
    if projection.task_summary.total < 0
        || projection.validation_summary.total < 0
        || task_count_total != projection.task_summary.total
        || validation_count_total != projection.validation_summary.total
    {
        return Err(ServiceError::invalid_operation(
            "Project effective state has inconsistent summary totals",
        ));
    }
    for commitment in &projection.commitments {
        for (field, value) in [
            ("Project commitment id", commitment.id.as_str()),
            ("Project commitment status", commitment.status.as_str()),
            (
                "Project commitment updated_at",
                commitment.updated_at.as_str(),
            ),
        ] {
            require_nonempty(field, value)?;
        }
        if commitment.version < 1 || commitment.evidence_count < 0 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid commitment version or evidence count",
            ));
        }
        if commitment.status == "completed" && commitment.evidence_count == 0 {
            return Err(ServiceError::conflict(
                "Project commitment cannot be projected as completed without authoritative evidence",
            ));
        }
        if let Some(task_id) = commitment.originating_task_id.as_deref() {
            require_nonempty("Project commitment originating task id", task_id)?;
        }
    }
    for item in &projection.inbox {
        for (field, value) in [
            ("Project inbox id", item.id.as_str()),
            ("Project inbox kind", item.kind.as_str()),
            ("Project inbox status", item.status.as_str()),
            ("Project inbox correlation id", item.correlation_id.as_str()),
            ("Project inbox created_at", item.created_at.as_str()),
            ("Project inbox updated_at", item.updated_at.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        if item.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid inbox version",
            ));
        }
        if let Some(source_id) = item.source_id.as_deref() {
            require_nonempty("Project inbox source id", source_id)?;
        }
    }
    for milestone in &projection.active_milestones {
        for (field, value) in [
            ("Project milestone id", milestone.id.as_str()),
            ("Project milestone key", milestone.milestone_key.as_str()),
            ("Project milestone lifecycle", milestone.lifecycle.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        let definition_revision_id =
            milestone.definition_revision_id.as_deref().ok_or_else(|| {
                ServiceError::conflict(
                    "Active Project milestone has no current definition revision",
                )
            })?;
        let definition_digest = milestone.definition_digest.as_deref().ok_or_else(|| {
            ServiceError::conflict("Active Project milestone has no definition digest")
        })?;
        require_nonempty(
            "Project milestone definition revision",
            definition_revision_id,
        )?;
        require_nonempty("Project milestone definition digest", definition_digest)?;
        if milestone.version < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid milestone version",
            ));
        }
    }
    if let Some(primary_id) = projection.primary_milestone_id.as_deref() {
        require_nonempty("Project primary milestone id", primary_id)?;
    }
    for readiness in projection
        .readiness
        .by_milestone
        .iter()
        .chain(projection.readiness.latest.iter())
    {
        for (field, value) in [
            ("Readiness snapshot id", readiness.id.as_str()),
            ("Readiness milestone id", readiness.milestone_id.as_str()),
            (
                "Readiness definition revision id",
                readiness.definition_revision_id.as_str(),
            ),
            ("Readiness baseline id", readiness.baseline_id.as_str()),
            (
                "Readiness baseline revision id",
                readiness.baseline_revision_id.as_str(),
            ),
            (
                "Readiness baseline digest",
                readiness.baseline_digest.as_str(),
            ),
            (
                "Readiness release-policy revision",
                readiness.release_policy_revision.as_str(),
            ),
            (
                "Readiness release-policy digest",
                readiness.release_policy_digest.as_str(),
            ),
            (
                "Readiness event watermark",
                readiness.event_watermark.as_str(),
            ),
            ("Readiness outcome", readiness.outcome.as_str()),
            ("Readiness digest", readiness.readiness_digest.as_str()),
            ("Readiness created_at", readiness.created_at.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
    }
    for release in &projection.releases {
        for (field, value) in [
            ("Project release id", release.id.as_str()),
            (
                "Project release milestone id",
                release.milestone_id.as_str(),
            ),
            (
                "Project release identifier",
                release.release_identifier.as_str(),
            ),
            (
                "Project release readiness snapshot id",
                release.readiness_snapshot_id.as_str(),
            ),
            (
                "Project release readiness digest",
                release.readiness_digest.as_str(),
            ),
            ("Project release baseline id", release.baseline_id.as_str()),
            (
                "Project release baseline revision id",
                release.baseline_revision_id.as_str(),
            ),
            (
                "Project release baseline digest",
                release.baseline_digest.as_str(),
            ),
            (
                "Project release snapshot digest",
                release.snapshot_digest.as_str(),
            ),
            ("Project release created_at", release.created_at.as_str()),
        ] {
            require_nonempty(field, value)?;
        }
        if release.release_sequence < 1 || release.release_revision < 1 {
            return Err(ServiceError::invalid_operation(
                "Project effective state has an invalid release revision",
            ));
        }
    }
    for id in projection
        .unreleased_changes
        .document_ids
        .iter()
        .chain(projection.unreleased_changes.decision_candidate_ids.iter())
        .chain(projection.unreleased_changes.baseline_revision_ids.iter())
        .chain(projection.unreleased_changes.active_milestone_ids.iter())
        .chain(projection.unreleased_changes.reconciliation_ids.iter())
    {
        require_nonempty("Project unreleased-change id", id)?;
    }
    Ok(())
}

fn require_nonempty<'a>(field: &str, value: &'a str) -> Result<&'a str> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "Project effective state is missing {field}"
        )));
    }
    Ok(value)
}

fn json_value_array(value: String) -> Result<Vec<Value>> {
    serde_json::from_str::<Vec<Value>>(&value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project runtime state contains invalid JSON array: {error}"
        ))
    })
}

fn json_object_value(value: String) -> Result<Value> {
    let parsed = serde_json::from_str::<Value>(&value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project runtime state contains invalid JSON object: {error}"
        ))
    })?;
    if !parsed.is_object() {
        return Err(ServiceError::invalid_operation(
            "Project runtime state contains a non-object decision record",
        ));
    }
    Ok(parsed)
}

fn json_string_array(value: String) -> Result<Vec<String>> {
    serde_json::from_str::<Vec<String>>(&value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project runtime state contains invalid string array JSON: {error}"
        ))
    })
}

fn validate_primary_milestone_pointer(
    milestones: &[MilestoneProjection],
    primary_milestone_id: Option<&str>,
) -> Result<()> {
    let active = milestones
        .iter()
        .filter(|milestone| milestone.lifecycle == "active")
        .collect::<Vec<_>>();
    // A Charter-created Project is born with its first milestone still
    // `planned` and already referenced as primary; the pointer becomes
    // active-only once any milestone activates.
    let pointer_eligible = |primary_id: &str| {
        milestones.iter().any(|milestone| {
            milestone.id == primary_id
                && matches!(milestone.lifecycle.as_str(), "active" | "planned")
        })
    };
    match (active.is_empty(), primary_milestone_id) {
        (true, None) => Ok(()),
        (true, Some(primary_id)) if pointer_eligible(primary_id) => Ok(()),
        (true, Some(_)) => Err(ServiceError::conflict(
            "Project primary milestone points at a Project with no active milestones",
        )),
        (false, None) => Err(ServiceError::conflict(
            "Project with active milestones has no explicit primary milestone",
        )),
        (false, Some(primary_id)) if active.iter().any(|milestone| milestone.id == primary_id) => {
            Ok(())
        }
        (false, Some(_)) => Err(ServiceError::conflict(
            "Project primary milestone is not one of the active milestones",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn milestone(id: &str, lifecycle: &str) -> MilestoneProjection {
        MilestoneProjection {
            id: id.to_owned(),
            milestone_key: "M001".to_owned(),
            display_label: None,
            lifecycle: lifecycle.to_owned(),
            definition_revision_id: None,
            definition_digest: None,
            version: 1,
            blocker_reasons: Vec::new(),
            stale_reasons: Vec::new(),
            reconciliation_reasons: Vec::new(),
        }
    }

    #[test]
    fn primary_pointer_is_required_only_for_active_milestones() {
        assert!(
            validate_primary_milestone_pointer(&[milestone("planned", "planned")], None).is_ok()
        );
        // A Charter-created Project starts with a planned primary milestone.
        assert!(validate_primary_milestone_pointer(
            &[milestone("planned", "planned")],
            Some("planned"),
        )
        .is_ok());
        assert!(validate_primary_milestone_pointer(
            &[milestone("planned", "planned")],
            Some("missing"),
        )
        .is_err());
        assert!(validate_primary_milestone_pointer(
            &[milestone("ready", "ready_for_release")],
            None,
        )
        .is_ok());
        assert!(
            validate_primary_milestone_pointer(&[milestone("active", "active")], None).is_err()
        );
        assert!(validate_primary_milestone_pointer(
            &[milestone("active", "active")],
            Some("active"),
        )
        .is_ok());
    }

    #[test]
    fn projection_is_closed_and_has_all_authority_domains() {
        let projection = ProjectEffectiveStateProjection {
            project: ProjectIdentityProjection {
                id: "p".to_owned(),
                name: "P".to_owned(),
                paused: false,
                charter_status: "legacy_unverified".to_owned(),
                charter_setup_required: true,
                version: 3,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
            active_execution_baseline: None,
            approved_documents: Vec::new(),
            active_decisions: Vec::new(),
            invalidated_decisions: Vec::new(),
            reconciliation_required: Vec::new(),
            canonical_conflicts: Vec::new(),
            task_summary: TaskSummaryProjection {
                total: 0,
                by_status: Vec::new(),
            },
            validation_summary: ValidationSummaryProjection {
                total: 0,
                by_outcome: Vec::new(),
            },
            commitments: Vec::new(),
            inbox: Vec::new(),
            active_milestones: Vec::new(),
            primary_milestone_id: None,
            readiness: ReadinessProjection {
                latest: None,
                by_milestone: Vec::new(),
            },
            releases: Vec::new(),
            unreleased_changes: UnreleasedChangesProjection {
                document_ids: Vec::new(),
                decision_candidate_ids: Vec::new(),
                baseline_revision_ids: Vec::new(),
                active_milestone_ids: Vec::new(),
                reconciliation_ids: Vec::new(),
            },
            source_event_watermark: "event".to_owned(),
            source_event_sequence: 4,
            source_project_version: 3,
            source_project_work_epoch: 1,
        };
        let value = serde_json::to_value(&projection).expect("projection serializes");
        assert!(value.get("governing_charter").is_some());
        assert!(value.get("active_execution_baseline").is_some());
        assert!(value.get("approved_documents").is_some());
        assert!(value.get("reconciliation_required").is_some());
        assert!(value.get("canonical_conflicts").is_some());
        assert!(value.get("unreleased_changes").is_some());
        assert!(serde_json::from_value::<ProjectEffectiveStateProjection>(value).is_ok());
        let validation = validate_effective_state(&projection);
        assert!(validation.is_ok(), "validation error: {validation:?}");

        let mut missing_watermark = projection.clone();
        missing_watermark.source_event_watermark.clear();
        assert!(validate_effective_state(&missing_watermark).is_err());

        let mut missing_charter = projection;
        missing_charter.project.charter_status = "charter_backed".to_owned();
        assert!(validate_effective_state(&missing_charter).is_err());
    }

    #[test]
    fn current_state_response_is_scope_tagged_and_closed() {
        let projection = ProjectEffectiveStateProjection {
            project: ProjectIdentityProjection {
                id: "project-1".to_owned(),
                name: "Project".to_owned(),
                paused: false,
                charter_status: "legacy_unverified".to_owned(),
                charter_setup_required: true,
                version: 1,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
            active_execution_baseline: None,
            approved_documents: Vec::new(),
            active_decisions: Vec::new(),
            invalidated_decisions: Vec::new(),
            reconciliation_required: Vec::new(),
            canonical_conflicts: Vec::new(),
            task_summary: TaskSummaryProjection {
                total: 0,
                by_status: Vec::new(),
            },
            validation_summary: ValidationSummaryProjection {
                total: 0,
                by_outcome: Vec::new(),
            },
            commitments: Vec::new(),
            inbox: Vec::new(),
            active_milestones: Vec::new(),
            primary_milestone_id: None,
            readiness: ReadinessProjection {
                latest: None,
                by_milestone: Vec::new(),
            },
            releases: Vec::new(),
            unreleased_changes: UnreleasedChangesProjection {
                document_ids: Vec::new(),
                decision_candidate_ids: Vec::new(),
                baseline_revision_ids: Vec::new(),
                active_milestone_ids: Vec::new(),
                reconciliation_ids: Vec::new(),
            },
            source_event_watermark: "project-work-epoch:1".to_owned(),
            source_event_sequence: 0,
            source_project_version: 1,
            source_project_work_epoch: 1,
        };
        let response = ProjectCurrentStateResponse {
            scope: "project".to_owned(),
            effective_state: projection,
        };
        let value = serde_json::to_value(&response).expect("response serializes");
        assert_eq!(value["scope"], "project");
        assert!(value["effective_state"]["source_project_version"].is_number());
        assert!(serde_json::from_value::<ProjectCurrentStateResponse>(value).is_ok());
    }

    #[test]
    fn persisted_projection_arrays_fail_closed_on_malformed_json() {
        assert!(json_value_array("not-json".to_owned()).is_err());
        assert!(json_string_array(r#"[1]"#.to_owned()).is_err());
        assert!(json_object_value(r#"[1]"#.to_owned()).is_err());
        assert_eq!(
            json_value_array(r#"[{"code":"blocked"}]"#.to_owned()).expect("valid JSON array"),
            vec![serde_json::json!({"code": "blocked"})]
        );
    }

    #[test]
    fn completed_commitments_require_authoritative_evidence() {
        let mut projection = ProjectEffectiveStateProjection {
            project: ProjectIdentityProjection {
                id: "project-1".to_owned(),
                name: "Project".to_owned(),
                paused: false,
                charter_status: "legacy_unverified".to_owned(),
                charter_setup_required: true,
                version: 1,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
            active_execution_baseline: None,
            approved_documents: Vec::new(),
            active_decisions: Vec::new(),
            invalidated_decisions: Vec::new(),
            reconciliation_required: Vec::new(),
            canonical_conflicts: Vec::new(),
            task_summary: TaskSummaryProjection {
                total: 0,
                by_status: Vec::new(),
            },
            validation_summary: ValidationSummaryProjection {
                total: 0,
                by_outcome: Vec::new(),
            },
            commitments: vec![ProjectCommitmentProjection {
                id: "commitment-1".to_owned(),
                status: "completed".to_owned(),
                due_at: None,
                originating_task_id: None,
                evidence_required: true,
                evidence_count: 0,
                blocked_reason: None,
                version: 1,
                updated_at: "now".to_owned(),
            }],
            inbox: Vec::new(),
            active_milestones: Vec::new(),
            primary_milestone_id: None,
            readiness: ReadinessProjection {
                latest: None,
                by_milestone: Vec::new(),
            },
            releases: Vec::new(),
            unreleased_changes: UnreleasedChangesProjection {
                document_ids: Vec::new(),
                decision_candidate_ids: Vec::new(),
                baseline_revision_ids: Vec::new(),
                active_milestone_ids: Vec::new(),
                reconciliation_ids: Vec::new(),
            },
            source_event_watermark: "event".to_owned(),
            source_event_sequence: 1,
            source_project_version: 1,
            source_project_work_epoch: 1,
        };
        assert!(validate_effective_state(&projection).is_err());
        projection.commitments[0].evidence_count = 1;
        assert!(validate_effective_state(&projection).is_ok());
    }
}
