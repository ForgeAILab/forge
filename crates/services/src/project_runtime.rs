//! Canonical, scope-bound Project runtime projection.
//!
//! The Project Agent prompt and the native `project.current_state` read must
//! consume the same projection.  This module is the single read path for the
//! effective Project state; it deliberately returns references, digests, and
//! bounded summaries rather than copying arbitrary artifact bodies into the
//! runtime context.

use std::collections::BTreeMap;

use api_types::{
    AcceptanceEvidenceRequirement, MilestoneAcceptanceCheck, ProjectCharterContent,
    ProjectExecutionSetupResponse,
};
use db::SqliteDb;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::{Result, ServiceError};

const DEFAULT_LIMIT: i64 = 32;
const MAX_LIMIT: i64 = 64;
const MAX_ADOPTION_CHARTER_CONTENT_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectEffectiveStateProjection {
    pub project: ProjectIdentityProjection,
    pub governing_charter: Option<CharterReferenceProjection>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectCurrentStateResponse {
    pub scope: String,
    pub effective_state: ProjectEffectiveStateProjection,
    /// Canonical setup/readiness dimensions are exposed beside the broader
    /// Project projection so current-state consumers cannot infer them from a
    /// single readiness boolean.
    #[serde(default)]
    pub execution_setup: Option<ProjectExecutionSetupResponse>,
    /// The unapproved adoption Charter is separate from effective state: it
    /// is useful while `charter_setup_required` is true, but it cannot become
    /// the governing Charter until the exact user approval flow consumes it.
    #[serde(default)]
    pub adoption_charter: Option<ProjectAdoptionCharterProjection>,
}

/// The current, unapproved adoption revision for a legacy Project.  This is
/// intentionally a bounded typed payload rather than a rendered or arbitrary
/// JSON body so a Project Agent can revise one field while preserving the
/// server-owned revision, version, and digest guards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectAdoptionCharterProjection {
    pub charter_id: String,
    pub revision_id: String,
    pub revision: i64,
    pub version: i64,
    pub content_digest: String,
    pub render_digest: String,
    pub content: ProjectCharterContent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectIdentityProjection {
    pub id: String,
    pub name: String,
    pub paused: bool,
    pub charter_status: String,
    pub charter_setup_required: bool,
    /// Independent commands inherited by a Task when it next enters review.
    /// The Project Agent can replace this list through
    /// `project.review_config` without receiving the rest of settings.
    #[serde(default)]
    pub default_review_ci_steps: Vec<String>,
    /// Commands that prepare the detached clean checkout before the default
    /// review CI commands run.
    #[serde(default)]
    pub default_review_setup_steps: Vec<String>,
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
    pub render_version: String,
    pub render_digest: String,
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
    pub render_version: String,
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
    /// Exact current-definition contract. Project Agents must reuse these
    /// stable IDs instead of inventing baseline acceptance-matrix aliases.
    #[serde(default)]
    pub acceptance_checks: Vec<MilestoneAcceptanceCheck>,
    #[serde(default)]
    pub evidence_requirements: Vec<AcceptanceEvidenceRequirement>,
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
    pub snapshot_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UnreleasedChangesProjection {
    pub document_ids: Vec<String>,
    pub decision_candidate_ids: Vec<String>,
    pub active_milestone_ids: Vec<String>,
    pub reconciliation_ids: Vec<String>,
}

/// Load the one canonical Project projection used by both the Project Agent
/// prompt and the native `project.current_state` tool. `limit` is a server
/// bounded presentation limit; it never changes scope or authority.
///
/// The adoption revision is read separately because it is not effective
/// Project state.  Keep this lookup on the same project-id boundary as the
/// effective projection so callers cannot select an arbitrary Charter.
pub async fn load_project_adoption_charter(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Option<ProjectAdoptionCharterProjection>> {
    let row = db.project_adoption_charter_source(project_id).await?;

    row.map(|row| {
        let content_json: String = row.content_json()?;
        if content_json.len() > MAX_ADOPTION_CHARTER_CONTENT_BYTES {
            return Err(ServiceError::conflict(
                "current adoption Charter content exceeds the bounded read limit",
            ));
        }
        let content = serde_json::from_str::<ProjectCharterContent>(&content_json)
            .map_err(|_| ServiceError::conflict("current adoption Charter content is invalid"))?;
        Ok(ProjectAdoptionCharterProjection {
            charter_id: row.charter_id()?,
            revision_id: row.revision_id()?,
            revision: row.revision()?,
            version: row.charter_version()?,
            content_digest: row.content_digest()?,
            render_digest: row.rendered_digest()?,
            content,
        })
    })
    .transpose()
}

pub async fn load_effective_project_state(
    db: &SqliteDb,
    project_id: &str,
    limit: Option<i64>,
) -> Result<ProjectEffectiveStateProjection> {
    let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let project = db
        .effective_project_identity_source(project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;

    let project_version: i64 = project.version()?;
    let project_work_epoch: i64 = project.project_work_epoch()?;
    let primary_milestone_id: Option<String> = project.primary_milestone_id()?;
    let (default_review_ci_steps, default_review_setup_steps) =
        default_review_steps(project.settings()?)?;
    let identity = ProjectIdentityProjection {
        id: project.id()?,
        name: project.name()?,
        paused: project.paused_at()?.is_some(),
        charter_status: project.charter_status()?,
        charter_setup_required: project.charter_setup_required()? != 0,
        default_review_ci_steps,
        default_review_setup_steps,
        version: project_version,
        created_at: project.created_at()?,
        updated_at: project.updated_at()?,
    };

    let governing_charter = db
        .effective_project_governing_charter_source(project_id)
        .await?
        .map(|row| {
            Ok::<_, db::DbError>(CharterReferenceProjection {
                id: row.id()?,
                revision_id: row.revision_id()?,
                revision: row.revision()?,
                version: row.version()?,
                content_digest: row.content_digest()?,
                render_version: row.render_version()?,
                render_digest: row.rendered_digest()?,
            })
        })
        .transpose()?;

    let approved_documents = db
        .effective_project_approved_document_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ApprovedDocumentProjection {
                id: row.id()?,
                kind: row.kind()?,
                title: row.title()?,
                revision_id: row.revision_id()?,
                revision: row.revision()?,
                version: row.version()?,
                lifecycle: row.lifecycle()?,
                content_digest: row.content_digest()?,
                render_version: row.render_version()?,
                render_digest: row.rendered_digest()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let decision_rows = db
        .effective_project_decision_sources(project_id, limit)
        .await?;
    let mut active_decisions = Vec::new();
    let mut invalidated_decisions = Vec::new();
    for row in decision_rows {
        let decision = DecisionProjection {
            id: row.id()?,
            state: row.state()?,
            decision_class: row.decision_class()?,
            question: row.question()?,
            selected_outcome: row.selected_outcome()?,
            rationale: row.rationale()?,
            principal_type: row.principal_type()?,
            principal_id: row.principal_id()?,
            authority_basis: row.authority_basis()?,
            authorization_action: row.authorization_action()?,
            explicit_event: row.explicit_event()?,
            authorization_occurred_at: row.authorization_occurred_at()?,
            charter_revision_id: row.charter_revision_id()?,
            source_refs: json_value_array(row.source_refs_json()?)?,
            affected_records: json_object_value(row.affected_records_json()?)?,
            created_at: row.created_at()?,
        };
        if decision.state == "invalidated" {
            invalidated_decisions.push(decision);
        } else {
            active_decisions.push(decision);
        }
    }

    let reconciliation_required = db
        .effective_project_required_reconciliation_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ReconciliationProjection {
                id: row.id()?,
                conflict_id: row.conflict_id()?,
                record_type: row.record_type()?,
                record_id: row.record_id()?,
                record_revision: row.record_revision()?,
                record_digest: row.record_digest()?,
                state: row.state()?,
                version: row.version()?,
                updated_at: row.updated_at()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let canonical_conflicts = db
        .effective_project_canonical_conflict_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(CanonicalConflictProjection {
                id: row.id()?,
                domain: row.domain()?,
                governing_record_type: row.governing_record_type()?,
                governing_record_id: row.governing_record_id()?,
                governing_record_revision: row.governing_record_revision()?,
                governing_record_digest: row.governing_record_digest()?,
                conflicting_record_type: row.conflicting_record_type()?,
                conflicting_record_id: row.conflicting_record_id()?,
                conflicting_record_revision: row.conflicting_record_revision()?,
                conflicting_record_digest: row.conflicting_record_digest()?,
                affected_paths: json_string_array(row.affected_paths_json()?)?,
                conflict_code: row.conflict_code()?,
                description: row.description()?,
                created_at: row.created_at()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let task_counts = db
        .effective_project_task_status_counts(project_id)
        .await?
        .into_iter()
        .map(|row| {
            Ok(CountProjection {
                key: row.key()?,
                count: row.count()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let task_total = task_counts.iter().map(|item| item.count).sum();

    let validation_counts = db
        .effective_project_validation_outcome_counts(project_id)
        .await?
        .into_iter()
        .map(|row| {
            Ok(CountProjection {
                key: row.key()?,
                count: row.count()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let validation_total = validation_counts.iter().map(|item| item.count).sum();

    // Coordination state is scoped through the active Project Agent binding
    // before retrieval.  This prevents a Project projection from exposing a
    // different identity's private obligations or inbox while still making
    // open reconciliation work visible after a binding/session rotation.
    let commitments = db
        .effective_project_commitment_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ProjectCommitmentProjection {
                id: row.id()?,
                status: row.status()?,
                due_at: row.due_at()?,
                originating_task_id: row.originating_task_id()?,
                evidence_required: row.evidence_required()? != 0,
                evidence_count: row.evidence_count()?,
                blocked_reason: row.blocked_reason()?,
                version: row.version()?,
                updated_at: row.updated_at()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let inbox = db
        .effective_project_inbox_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ProjectInboxProjection {
                id: row.id()?,
                kind: row.kind()?,
                status: row.status()?,
                source_type: row.source_type()?,
                source_id: row.source_id()?,
                correlation_id: row.correlation_id()?,
                version: row.version()?,
                created_at: row.created_at()?,
                updated_at: row.updated_at()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let milestone_rows = db
        .effective_project_milestone_sources(project_id, limit)
        .await?;
    let active_milestones = milestone_rows
        .into_iter()
        .map(|row| {
            Ok(MilestoneProjection {
                id: row.id()?,
                milestone_key: row.milestone_key()?,
                display_label: row.display_label()?,
                lifecycle: row.lifecycle()?,
                definition_revision_id: row.current_definition_revision_id()?,
                definition_digest: row.definition_digest()?,
                acceptance_checks: json_typed_array(row.acceptance_checks_json()?)?,
                evidence_requirements: json_typed_array(row.evidence_requirements_json()?)?,
                version: row.version()?,
                blocker_reasons: json_value_array(row.blocker_reason_json()?)?,
                stale_reasons: json_value_array(row.stale_reason_json()?)?,
                reconciliation_reasons: json_value_array(row.reconciliation_reason_json()?)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    validate_primary_milestone_pointer(&active_milestones, primary_milestone_id.as_deref())?;

    let readiness_rows = db
        .effective_project_readiness_sources(project_id, limit.saturating_mul(4))
        .await?;
    let mut seen_milestones = BTreeMap::new();
    for row in readiness_rows {
        let milestone_id: String = row.milestone_id()?;
        if seen_milestones.contains_key(&milestone_id) {
            continue;
        }
        let snapshot = ReadinessSnapshotProjection {
            id: row.id()?,
            milestone_id: milestone_id.clone(),
            definition_revision_id: row.definition_revision_id()?,
            event_watermark: row.event_watermark()?,
            outcome: row.outcome()?,
            blocking_reasons: json_value_array(row.blocking_reasons_json()?)?,
            readiness_digest: row.readiness_digest()?,
            created_at: row.created_at()?,
        };
        seen_milestones.insert(milestone_id, snapshot);
    }
    let by_milestone = seen_milestones.into_values().collect::<Vec<_>>();
    let latest = by_milestone.first().cloned();

    let releases = db
        .effective_project_release_sources(project_id, limit)
        .await?
        .into_iter()
        .map(|row| {
            Ok(ReleaseProjection {
                id: row.id()?,
                milestone_id: row.milestone_id()?,
                release_sequence: row.release_sequence()?,
                release_revision: row.release_revision()?,
                release_identifier: row.release_identifier()?,
                readiness_snapshot_id: row.readiness_snapshot_id()?,
                readiness_digest: row.readiness_digest()?,
                snapshot_digest: row.snapshot_digest()?,
                created_at: row.created_at()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let document_ids = db
        .effective_project_unreleased_document_ids(project_id, limit)
        .await?;
    let decision_candidate_ids = db
        .effective_project_decision_candidate_ids(project_id, limit)
        .await?;
    let active_milestone_ids = active_milestones
        .iter()
        .map(|milestone| milestone.id.clone())
        .collect();
    let reconciliation_ids = reconciliation_required
        .iter()
        .map(|record| record.id.clone())
        .collect();

    let watermark_row = db.effective_project_event_watermark(project_id).await?;
    let (source_event_watermark, source_event_sequence) = match watermark_row {
        Some(row) => (row.id()?, row.sequence()?),
        None => (format!("project-work-epoch:{project_work_epoch}"), 0),
    };

    let projection = ProjectEffectiveStateProjection {
        project: identity,
        governing_charter,
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
        // A milestone the Project Agent has just created carries only a draft
        // definition until that definition is proposed and approved, so an
        // absent current revision is a legal state the Project must be able to
        // report on rather than a projection failure. A present revision still
        // has to carry its digest.
        match (
            milestone.definition_revision_id.as_deref(),
            milestone.definition_digest.as_deref(),
        ) {
            (Some(definition_revision_id), Some(definition_digest)) => {
                require_nonempty(
                    "Project milestone definition revision",
                    definition_revision_id,
                )?;
                require_nonempty("Project milestone definition digest", definition_digest)?;
            }
            (None, None) => {}
            _ => {
                return Err(ServiceError::conflict(
                    "Project milestone definition revision and digest must be recorded together",
                ));
            }
        }
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

fn json_typed_array<T: DeserializeOwned>(value: Option<String>) -> Result<Vec<T>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<T>>(&value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project runtime state contains an invalid typed JSON array: {error}"
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

fn default_review_steps(settings: String) -> Result<(Vec<String>, Vec<String>)> {
    let settings = serde_json::from_str::<Value>(&settings).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Project runtime state contains invalid Project settings: {error}"
        ))
    })?;
    let Some(review_config) = settings.get("default_review_config") else {
        return Ok((Vec::new(), Vec::new()));
    };
    let review_config = review_config.as_object().ok_or_else(|| {
        ServiceError::invalid_operation(
            "Project runtime state contains a non-object default_review_config",
        )
    })?;
    let read = |field: &str| {
        review_config
            .get(field)
            .map(|value| {
                serde_json::from_value::<Vec<String>>(value.clone()).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "Project runtime state contains invalid default review {field}: {error}"
                    ))
                })
            })
            .transpose()
            .map(|value| value.unwrap_or_default())
    };
    Ok((read("ci_steps")?, read("setup_steps")?))
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
    // `planned` and already referenced as primary, and a Project whose only
    // milestone has reached `ready_for_release` or `released` has moved
    // forward, not broken its pointer. Only `cancelled` leaves the pointer
    // naming work the Project no longer intends to do. Rejecting the released
    // states wedged every turn admission the moment a Project delivered.
    let pointer_eligible = |primary_id: &str| {
        milestones.iter().any(|milestone| {
            milestone.id == primary_id
                && matches!(
                    milestone.lifecycle.as_str(),
                    "active" | "planned" | "ready_for_release" | "released"
                )
        })
    };
    match primary_milestone_id {
        None if active.is_empty() => Ok(()),
        None => Err(ServiceError::conflict(
            "Project with active milestones has no explicit primary milestone",
        )),
        Some(primary_id) if pointer_eligible(primary_id) => Ok(()),
        Some(_) => Err(ServiceError::conflict(
            "Project primary milestone is not an intended Project milestone",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, now_rfc3339, run_migrations, CreateProject, ProjectRepo, SqliteDb,
    };

    fn adoption_content() -> ProjectCharterContent {
        serde_json::from_value(serde_json::json!({
            "identity": {
                "working_name": "Pocket Tasks",
                "one_line_vision": "A small local task list",
                "maturity": "mvp"
            },
            "problem_and_people": {
                "problem_or_opportunity": "Keep a short list of local work",
                "target_users": ["one developer"]
            },
            "core_experience": {
                "primary_outcome": "Tasks can be tracked from a terminal"
            },
            "scope": {},
            "success": {},
            "constraints_and_risks": {},
            "knowledge_ledger": {}
        }))
        .expect("valid adoption Charter content")
    }

    async fn adoption_fixture() -> (SqliteDb, ProjectCharterContent, String, String) {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, created_at, updated_at)
             VALUES ('owner-1', 'owner@example.test', 'test', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("owner");
        ProjectRepo::create(
            &db,
            CreateProject {
                id: "project-adoption-read".to_owned(),
                name: "Pocket Tasks".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some("owner-1".to_owned()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");

        let content = adoption_content();
        let content_json = api_types::canonical_json(&content).expect("canonical content");
        let rendered = crate::render_project_charter(&content);
        let content_digest = crate::charter_content_digest(&content);
        let render_digest =
            crate::charter_render_digest(crate::PROJECT_CHARTER_RENDER_VERSION, &rendered);
        sqlx::query(
            "INSERT INTO project_charter
                (id, account_id, project_id, project_mode, maturity, lifecycle,
                 version, created_at, updated_at)
             VALUES ('adoption-charter-1', 'owner-1', 'project-adoption-read',
                     'compact', 'mvp', 'draft', 2, ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("Charter");
        sqlx::query(
            "INSERT INTO project_charter_revision
                (id, charter_id, revision, base_revision, lifecycle, schema_version,
                 render_version, content_json, rendered_view, change_summary,
                 author_type, source_refs_json, content_digest, rendered_digest, created_at)
             VALUES ('adoption-revision-1', 'adoption-charter-1', 1, 0, 'proposed',
                     'forge.project-charter/v1', ?, ?, ?, 'initial draft', 'agent',
                     '[]', ?, ?, ?)",
        )
        .bind(crate::PROJECT_CHARTER_RENDER_VERSION)
        .bind(&content_json)
        .bind(&rendered)
        .bind(&content_digest)
        .bind(&render_digest)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("Charter revision");
        sqlx::query(
            "UPDATE project_charter
             SET current_draft_revision_id = 'adoption-revision-1'
             WHERE id = 'adoption-charter-1'",
        )
        .execute(db.pool())
        .await
        .expect("draft pointer");
        (db, content, content_digest, render_digest)
    }

    #[tokio::test]
    async fn current_state_reads_the_pending_adoption_charter_content() {
        let (db, expected_content, expected_content_digest, expected_render_digest) =
            adoption_fixture().await;
        let projection = load_project_adoption_charter(&db, "project-adoption-read")
            .await
            .expect("adoption projection")
            .expect("pending adoption Charter");

        assert_eq!(projection.charter_id, "adoption-charter-1");
        assert_eq!(projection.revision_id, "adoption-revision-1");
        assert_eq!(projection.revision, 1);
        assert_eq!(projection.version, 2);
        assert_eq!(projection.content_digest, expected_content_digest);
        assert_eq!(projection.render_digest, expected_render_digest);
        assert_eq!(projection.content, expected_content);
    }

    #[tokio::test]
    async fn current_state_does_not_project_adoption_after_approval() {
        let (db, _, _, _) = adoption_fixture().await;
        sqlx::query(
            "UPDATE project_charter_revision
             SET lifecycle = 'approved'
             WHERE id = 'adoption-revision-1'",
        )
        .execute(db.pool())
        .await
        .expect("approve revision");
        sqlx::query(
            "UPDATE project_charter
             SET current_draft_revision_id = NULL,
                 current_approved_revision_id = 'adoption-revision-1',
                 lifecycle = 'attached'
             WHERE id = 'adoption-charter-1'",
        )
        .execute(db.pool())
        .await
        .expect("approve Charter");
        sqlx::query(
            "UPDATE project
             SET current_charter_id = 'adoption-charter-1',
                 current_charter_revision_id = 'adoption-revision-1',
                 current_charter_version = 3,
                 charter_status = 'charter_backed',
                 charter_setup_required = 0
             WHERE id = 'project-adoption-read'",
        )
        .execute(db.pool())
        .await
        .expect("back Project with approved Charter");

        assert!(load_project_adoption_charter(&db, "project-adoption-read")
            .await
            .expect("adoption projection")
            .is_none());

        let effective = load_effective_project_state(&db, "project-adoption-read", None)
            .await
            .expect("effective Project state");
        let governing = effective
            .governing_charter
            .expect("approved Charter is governing");
        assert_eq!(
            governing.render_version,
            crate::PROJECT_CHARTER_RENDER_VERSION
        );
        assert!(!governing.render_digest.is_empty());
    }

    fn milestone(id: &str, lifecycle: &str) -> MilestoneProjection {
        MilestoneProjection {
            id: id.to_owned(),
            milestone_key: "M001".to_owned(),
            display_label: None,
            lifecycle: lifecycle.to_owned(),
            definition_revision_id: None,
            definition_digest: None,
            acceptance_checks: Vec::new(),
            evidence_requirements: Vec::new(),
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
        // Delivering the Project's only milestone must not invalidate the
        // pointer that still names it — that wedged every turn admission.
        for delivered in ["ready_for_release", "released"] {
            assert!(
                validate_primary_milestone_pointer(
                    &[milestone("delivered", delivered)],
                    Some("delivered"),
                )
                .is_ok(),
                "a {delivered} primary milestone must stay a valid pointer"
            );
        }
        assert!(validate_primary_milestone_pointer(
            &[
                milestone("delivered", "released"),
                milestone("next", "active")
            ],
            Some("delivered"),
        )
        .is_ok());
        assert!(validate_primary_milestone_pointer(
            &[milestone("cancelled", "cancelled")],
            Some("cancelled"),
        )
        .is_err());
    }

    /// A minimal projection that validates, so a test can vary one domain.
    fn minimal_projection() -> ProjectEffectiveStateProjection {
        ProjectEffectiveStateProjection {
            project: ProjectIdentityProjection {
                id: "p".to_owned(),
                name: "P".to_owned(),
                paused: false,
                charter_status: "legacy_unverified".to_owned(),
                charter_setup_required: true,
                default_review_ci_steps: Vec::new(),
                default_review_setup_steps: Vec::new(),
                version: 3,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
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
                active_milestone_ids: Vec::new(),
                reconciliation_ids: Vec::new(),
            },
            source_event_watermark: "event".to_owned(),
            source_event_sequence: 4,
            source_project_version: 3,
            source_project_work_epoch: 1,
        }
    }

    /// A milestone the Project Agent just created has only a draft definition,
    /// so its current revision is absent until that draft is approved. Failing
    /// the projection there wedged every Project Agent turn: the Agent could
    /// not take the turn that would have proposed the definition.
    #[test]
    fn a_milestone_whose_definition_is_still_a_draft_projects_without_failing() {
        let mut pending = minimal_projection();
        pending.active_milestones = vec![milestone("m1", "planned")];
        pending.primary_milestone_id = Some("m1".to_owned());
        let validation = validate_effective_state(&pending);
        assert!(validation.is_ok(), "validation error: {validation:?}");

        let mut approved = minimal_projection();
        let mut defined = milestone("m1", "planned");
        defined.definition_revision_id = Some("rev-1".to_owned());
        defined.definition_digest = Some("digest-1".to_owned());
        approved.active_milestones = vec![defined];
        approved.primary_milestone_id = Some("m1".to_owned());
        assert!(validate_effective_state(&approved).is_ok());

        let mut half_recorded = minimal_projection();
        let mut partial = milestone("m1", "planned");
        partial.definition_revision_id = Some("rev-1".to_owned());
        half_recorded.active_milestones = vec![partial];
        half_recorded.primary_milestone_id = Some("m1".to_owned());
        assert!(validate_effective_state(&half_recorded).is_err());
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
                default_review_ci_steps: Vec::new(),
                default_review_setup_steps: Vec::new(),
                version: 3,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
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
                default_review_ci_steps: Vec::new(),
                default_review_setup_steps: Vec::new(),
                version: 1,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
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
            execution_setup: None,
            adoption_charter: None,
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
                default_review_ci_steps: Vec::new(),
                default_review_setup_steps: Vec::new(),
                version: 1,
                created_at: "now".to_owned(),
                updated_at: "now".to_owned(),
            },
            governing_charter: None,
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
