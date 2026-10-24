//! Project-local milestone, readiness, and release persistence.
//!
//! The database migration owns the durable shape and the pure
//! `milestone_orchestration` module owns deterministic policy.  This module
//! is the narrow transaction boundary between those two layers.  It keeps the
//! release path out of chat/runtime code and, importantly, never treats a
//! cached overview or a previous `ready_for_release` state as release
//! authority.

use std::{collections::HashMap, sync::Arc};

use api_types::{
    canonical_digest_with_schema, AcceptanceCheckResultStatus, AcceptanceEvidenceRequirement,
    ArtifactRef, AuthorizationProvenance, EvidenceAttachment, EvidenceAvailability, EvidenceKind,
    MilestoneAcceptanceCheck, MilestoneDefinitionContent, MilestoneDefinitionLifecycle,
    MilestoneDefinitionRevision, MilestoneLifecycle, MilestoneProjectionReason,
    MilestoneProjectionReasonKind, PrincipalKind, PrincipalRef, ProjectMilestone, ProjectRelease,
    ReadinessFreshness, ReadinessFreshnessStatus, ReadinessInput, ReadinessReason, ReadinessResult,
    ReadinessSnapshot, ReleaseDecisionReference, ReleaseSnapshot, ReleaseTaskReference,
    ReleaseValidationReference, RevisionProvenance, ValidationResult,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo, ProjectMilestoneRecord,
    ProjectMilestoneRevisionRecord, ProjectOrchestrationRepo, ProjectReadinessSnapshotRecord,
    ProjectReleaseRecord, SqliteDb,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

use crate::{
    evaluate_readiness, release_snapshot_digest, verify_release_candidate,
    MilestoneOrchestrationError, ReadinessDocumentState, ReadinessEvaluation,
    ReadinessEvaluationInput, ReadinessTaskState, ReleaseCandidateVerification,
    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION, MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
};

const COMPUTING_POLICY_REVISION: &str = "forge.readiness.compute/v1";
const RELEASE_SCHEMA_VERSION: &str = "forge.milestone-release/v1";
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;
const MAX_AUTHORIZATION_TIMESTAMP_LEN: usize = 64;
const EVIDENCE_ATTACH_AUTHORIZATION_ACTION: &str = "project.evidence.attach";
const CHECK_RESULT_AUTHORIZATION_ACTION: &str = "project.milestone.check.record";

/// The exact read-side recomputation shared by release and the Overview
/// freshness overlay.  The immutable readiness row is never rewritten; this
/// value is only a transaction-local comparison against current governed
/// inputs.
struct ReadinessRecheck {
    milestone: ProjectMilestoneRecord,
    definition: MilestoneDefinitionRevision,
    candidate_snapshot: ReadinessSnapshot,
    recomputed: ReadinessEvaluation,
    evidence: Vec<EvidenceAttachment>,
    task_states: Vec<ReadinessTaskState>,
}

/// Milestone-internal acceptance contract.
///
/// Readiness authority is the approved Charter plus the milestone's own
/// acceptance matrix. This check keeps that matrix self-consistent: a
/// required acceptance check must have a required evidence requirement under
/// the same stable id, so a milestone cannot demand proof it never asks
/// anyone to attach.
fn milestone_definition_contract_reasons(
    definition: &MilestoneDefinitionRevision,
) -> Vec<ReadinessReason> {
    let mut reasons = Vec::new();
    for check in definition
        .content
        .acceptance_checks
        .iter()
        .filter(|check| check.required)
    {
        if !definition
            .content
            .evidence_requirements
            .iter()
            .any(|requirement| requirement.required && requirement.id == check.id)
        {
            reasons.push(ReadinessReason {
                code: "milestone_evidence_contract_reconciliation_required".to_owned(),
                message: format!(
                    "reconciliation_required: required acceptance check '{}' has no required evidence requirement with the same stable id",
                    check.id
                ),
                blocking: true,
                check_id: Some(check.id.clone()),
                source_ids: vec![check.id.clone(), definition.id.clone()],
            });
        }
    }
    reasons
}

#[derive(Debug, Clone)]
pub struct MilestoneRuntime {
    db: Arc<SqliteDb>,
}

impl MilestoneRuntime {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct ProjectionReasons {
    pub(crate) blockers: Vec<MilestoneProjectionReason>,
    pub(crate) stale: Vec<MilestoneProjectionReason>,
    pub(crate) reconciliation: Vec<MilestoneProjectionReason>,
}

pub(crate) fn projection_reasons(reasons: &[ReadinessReason]) -> ProjectionReasons {
    let mut result = ProjectionReasons::default();
    for reason in reasons {
        let kind = if reason.code.contains("stale") || reason.code.contains("policy") {
            MilestoneProjectionReasonKind::Stale
        } else if reason.code.contains("reconcil") {
            MilestoneProjectionReasonKind::ReconciliationRequired
        } else if reason.code.contains("evidence") {
            MilestoneProjectionReasonKind::EvidenceUnavailable
        } else if reason.code.contains("check") {
            MilestoneProjectionReasonKind::CheckFailed
        } else {
            MilestoneProjectionReasonKind::Blocker
        };
        let projection = MilestoneProjectionReason {
            kind,
            code: reason.code.clone(),
            message: reason.message.clone(),
            source_ids: reason.source_ids.clone(),
        };
        match kind {
            MilestoneProjectionReasonKind::Stale => result.stale.push(projection),
            MilestoneProjectionReasonKind::ReconciliationRequired => {
                result.reconciliation.push(projection)
            }
            _ => result.blockers.push(projection),
        }
    }
    result
}

fn validation_results_from_rows(
    rows: Vec<db::MilestoneCheckResultRow>,
    project_id: &str,
    expected_definition_revision_id: &str,
    expected_charter_revision_id: Option<&str>,
) -> crate::Result<Vec<ValidationResult>> {
    let mut latest = std::collections::BTreeMap::<String, ValidationResult>::new();
    for row in rows {
        let check_id: String = row.check_id.clone();
        if latest.contains_key(&check_id) {
            continue;
        }
        let source_kind: String = row.source_kind.clone();
        match source_kind.as_str() {
            // Each of these has a server-owned admission path that supplies
            // exact provenance: `manual` through the user attestation route,
            // `policy_waiver` through the user waiver, and `task_validation`
            // through the receipt-backed `project.validation` command. The
            // remaining enum values stay schema-compatible for historical rows
            // but cannot become release authority until they have one too.
            "manual" | "policy_waiver" | "task_validation" => {}
            other => {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!("unknown persisted check result source kind {other}"),
                });
            }
        }
        let id: String = row.id.clone();
        if id.trim().is_empty() || check_id.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable validation result is missing identity fields".to_owned(),
            });
        }
        let principal = PrincipalRef {
            kind: principal_kind(&row.principal_type.clone())?,
            id: row.principal_id.clone(),
            display_name: None,
        };
        let evaluated_at: String = row.created_at.clone();
        let status = check_status(&row.outcome.clone())?;
        let input_digest: String = row.input_digest.clone();
        if evaluated_at.trim().is_empty() || input_digest.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("immutable validation {id} is missing target provenance"),
            });
        }
        let source_manifest: Value = parse_json_required(
            &row.source_manifest_json.clone(),
            "validation source_manifest",
        )?;
        let definition_revision_id: String = row.definition_revision_id.clone();
        let manifest_definition_revision_id = source_manifest
            .get("check_definition_revision_id")
            .and_then(Value::as_str);
        let manifest_governing_revision_ids: Option<Vec<String>> = source_manifest
            .get("governing_revision_ids")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| crate::ServiceError::InvalidOperation {
                message: format!("invalid validation governing revisions: {error}"),
            })?;
        let persisted_charter_revision_id: Option<String> =
            row.governing_charter_revision_id.clone();
        // The approved Charter is the whole governing authority for a result.
        let expected_revision_ids =
            expected_charter_revision_id.map(|charter| vec![charter.to_owned()]);
        if definition_revision_id != expected_definition_revision_id
            || manifest_definition_revision_id != Some(expected_definition_revision_id)
            || manifest_governing_revision_ids.as_ref() != expected_revision_ids.as_ref()
            || persisted_charter_revision_id.as_deref() != expected_charter_revision_id
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable validation result is stale for the active authority".to_owned(),
            });
        }
        let result = source_manifest
            .get("result")
            .and_then(Value::as_str)
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "immutable validation source_manifest.result is required".to_owned(),
            })?
            .to_owned();
        if result.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("immutable validation {id} has an empty result"),
            });
        }
        let governing: Vec<String> = source_manifest
            .get("governing_revision_ids")
            .cloned()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "immutable validation governing_revision_ids are required".to_owned(),
            })
            .and_then(|value| {
                serde_json::from_value(value).map_err(|error| {
                    crate::ServiceError::InvalidOperation {
                        message: format!("invalid validation governing_revision_ids: {error}"),
                    }
                })
            })?;
        if governing.is_empty() || governing.iter().any(|value| value.trim().is_empty()) {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("immutable validation {id} has incomplete governing revisions"),
            });
        }
        let authorization = AuthorizationProvenance {
            principal: principal.clone(),
            authorization_basis: row.authorization_basis.clone(),
            action: row.authorization_action.clone(),
            event_id: row.explicit_event.clone(),
            occurred_at: row.authorization_occurred_at.clone(),
        };
        if source_kind == "manual" {
            if principal.kind != PrincipalKind::User {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "manual check result must be authorized by a user".to_owned(),
                });
            }
            validate_persisted_authorization(&authorization, CHECK_RESULT_AUTHORIZATION_ACTION)?;
        } else {
            validate_persisted_authorization_receipt(&authorization, &format!("validation {id}"))?;
        }
        let result_digest = canonical_digest_with_schema(
            MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &json!({
                "id": id,
                "check_id": check_id,
                "status": status,
                "result": result.clone(),
                "input_digest": input_digest.clone(),
                "evaluated_at": evaluated_at.clone(),
                "governing_revision_ids": governing.clone(),
                "authorization": authorization.clone(),
                "source_manifest": source_manifest
            }),
        )
        .map_err(json_error)?;
        latest.insert(
            check_id.clone(),
            ValidationResult {
                id: id.clone(),
                project_id: project_id.to_owned(),
                check_id,
                status,
                result,
                principal: principal.clone(),
                authorization: authorization.clone(),
                input_digest,
                governing_revision_ids: governing,
                expected_version: {
                    let version: i64 = row.expected_version;
                    if version <= 0 {
                        return Err(crate::ServiceError::InvalidOperation {
                            message: format!(
                                "immutable validation {id} has invalid expected version"
                            ),
                        });
                    }
                    version
                },
                event_id: {
                    let event_id: String = row.explicit_event.clone();
                    if event_id.trim().is_empty() || event_id != authorization.event_id {
                        return Err(crate::ServiceError::InvalidOperation {
                            message: format!(
                                "immutable validation {id} has invalid event identity"
                            ),
                        });
                    }
                    event_id
                },
                evaluated_at,
                result_digest,
            },
        );
    }
    Ok(latest.into_values().collect())
}

/// Two principal references identify the same principal when their kind and id
/// agree. `display_name` is an optional presentation label: the immutable rows
/// persist only `*_principal_type`/`*_principal_id`, so a reference rebuilt from
/// those columns always carries `None` while the authorization the caller sent
/// may carry the label the UI showed. Comparing the whole struct therefore
/// rejects provenance that is in fact identical, and — because these rows are
/// immutable — permanently blocks readiness on evidence that can never be
/// corrected.
fn same_principal(left: &PrincipalRef, right: &PrincipalRef) -> bool {
    left.kind == right.kind && left.id == right.id
}

fn evidence_from_rows(
    rows: Vec<db::MilestoneEvidenceRow>,
    project_id: &str,
) -> crate::Result<Vec<EvidenceAttachment>> {
    rows.into_iter()
        .map(|row| {
            let author = PrincipalRef {
                kind: principal_kind(&row.author_type.clone())?,
                id: row
                    .author_id
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing author_id".to_owned(),
                    })?,
                display_name: None,
            };
            let attachment_authorization: AuthorizationProvenance =
                parse_json_required(&row.authorization_json.clone(), "evidence authorization")?;
            if !same_principal(&attachment_authorization.principal, &author) {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "immutable evidence authorization principal disagrees with author"
                        .to_owned(),
                });
            }
            validate_persisted_authorization(
                &attachment_authorization,
                EVIDENCE_ATTACH_AUTHORIZATION_ACTION,
            )?;
            Ok(EvidenceAttachment {
                id: row.id.clone(),
                project_id: project_id.to_owned(),
                asset_id: row.asset_id.clone(),
                task_id: row.task_id.clone(),
                source_task_id: row.source_task_id.clone(),
                source_run_id: row.source_execution_id.clone(),
                source_validation_id: row.source_validation_id.clone(),
                source_task_version: row.source_task_version,
                source_context_digest: row.source_context_digest.clone(),
                source_definition_revision_id: row.source_definition_revision_id.clone(),
                milestone_id: row.milestone_id.clone(),
                acceptance_check_ids: parse_json_required(
                    &row.acceptance_check_ids_json.clone(),
                    "evidence acceptance_check_ids",
                )?,
                caption: row
                    .caption
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing caption".to_owned(),
                    })?,
                kind: evidence_kind(row.evidence_kind.clone().as_deref())?,
                checksum: row
                    .checksum
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing checksum".to_owned(),
                    })?,
                availability: evidence_availability(&row.availability.clone())?,
                author,
                captured_at: row.created_at.clone(),
                version: row.version,
                created_at: row.created_at.clone(),
                removed_at: row.deleted_at.clone(),
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleasePinMetadata {
    attachment_id: String,
    asset_id: String,
    attachment_digest: String,
    asset_checksum: String,
    task_media_id: Option<String>,
    stable_project_url: Option<String>,
}

/// Canonical, bounded repository/build/check context.  The public fields stay
/// string-valued for compatibility with the existing API contract, but each
/// string is canonical JSON for this closed object rather than an opaque
/// digest.  It therefore preserves the exact repository identity, execution
/// SHAs, review/CI results, and observed timestamps in readiness and release
/// manifests.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
struct RepositoryContextReference {
    task_id: String,
    task_version: i64,
    repository_id: String,
    repository_name: String,
    repository_kind: String,
    remote_url: Option<String>,
    default_branch: String,
    execution_id: Option<String>,
    execution_status: Option<String>,
    execution_role: Option<String>,
    before_sha: Option<String>,
    after_sha: Option<String>,
    execution_summary: Option<String>,
    execution_created_at: Option<String>,
    execution_updated_at: Option<String>,
    review_id: Option<String>,
    review_status: Option<String>,
    ci_results: Option<String>,
    audit_result: Option<String>,
    human_decision: Option<String>,
    review_created_at: Option<String>,
    review_updated_at: Option<String>,
    observed_at: String,
}

fn validate_repository_context_reference(
    reference: &RepositoryContextReference,
) -> crate::Result<()> {
    if reference.task_id.trim().is_empty()
        || reference.task_version <= 0
        || reference.repository_id.trim().is_empty()
        || reference.repository_name.trim().is_empty()
        || reference.repository_kind.trim().is_empty()
        || reference.default_branch.trim().is_empty()
        || reference.observed_at.trim().is_empty()
        || reference
            .remote_url
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "repository context is missing required immutable metadata".to_owned(),
        });
    }
    if reference.execution_id.is_none()
        && (reference.execution_status.is_some()
            || reference.execution_role.is_some()
            || reference.before_sha.is_some()
            || reference.after_sha.is_some()
            || reference.execution_summary.is_some()
            || reference.execution_created_at.is_some()
            || reference.execution_updated_at.is_some())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "repository context has execution fields without an execution identity"
                .to_owned(),
        });
    }
    if let Some(execution_id) = reference.execution_id.as_deref() {
        if execution_id.trim().is_empty()
            || reference
                .execution_status
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || reference
                .execution_role
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || reference
                .execution_updated_at
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "repository context has incomplete execution metadata".to_owned(),
            });
        }
    }
    if reference.review_id.is_none()
        && (reference.review_status.is_some()
            || reference.ci_results.is_some()
            || reference.audit_result.is_some()
            || reference.human_decision.is_some()
            || reference.review_created_at.is_some()
            || reference.review_updated_at.is_some())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "repository context has review fields without a review identity".to_owned(),
        });
    }
    if let Some(review_id) = reference.review_id.as_deref() {
        if review_id.trim().is_empty()
            || reference
                .review_status
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || reference
                .review_created_at
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            || reference
                .review_updated_at
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "repository context has incomplete review metadata".to_owned(),
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn release_snapshot(
    project_id: &str,
    milestone: &ProjectMilestoneRecord,
    definition: &MilestoneDefinitionRevision,
    readiness: &ReadinessSnapshot,
    verified: &ReleaseCandidateVerification,
    release_id: &str,
    evidence: &[EvidenceAttachment],
    pin_metadata: &[ReleasePinMetadata],
    task_states: &[ReadinessTaskState],
    included_decisions: &[ReleaseDecisionReference],
    actor: &PrincipalRef,
    authorization: &AuthorizationProvenance,
    idempotency_key: &str,
    released_at: &str,
) -> crate::Result<ReleaseSnapshot> {
    let charter_revision = definition.content.charter_revision.clone().ok_or_else(|| {
        crate::ServiceError::InvalidOperation {
            message: "release requires an approved Project Charter revision".to_owned(),
        }
    })?;
    if definition.content.outcome.trim().is_empty()
        || charter_revision.artifact_id.trim().is_empty()
        || charter_revision.revision_id.trim().is_empty()
        || charter_revision.content_digest.trim().is_empty()
        || definition
            .content
            .known_issues
            .iter()
            .any(|issue| issue.trim().is_empty())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "release definition contains incomplete summary, Charter, or known-issue data"
                .to_owned(),
        });
    }
    for document in &definition.content.document_revisions {
        if document.artifact_id.trim().is_empty()
            || document.revision_id.trim().is_empty()
            || document.content_digest.trim().is_empty()
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "release definition contains an incomplete Document reference".to_owned(),
            });
        }
    }
    let included_tasks = definition
        .content
        .task_ids
        .iter()
        .map(|task_id| {
            let task = task_states
                .iter()
                .find(|task| task.task_id == *task_id)
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!("release references missing Task {task_id}"),
                })?;
            Ok(ReleaseTaskReference {
                task_id: task.task_id.clone(),
                task_version: task.version,
                task_type: task.task_type.clone(),
                task_state: task.state.clone(),
                acceptance_check_ids: readiness
                    .check_results
                    .iter()
                    .filter(|result| {
                        result
                            .governing_revision_ids
                            .iter()
                            .any(|reference| reference == task_id)
                    })
                    .map(|result| result.check_id.clone())
                    .collect(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let mut snapshot = ReleaseSnapshot {
        schema_version: RELEASE_SCHEMA_VERSION.to_owned(),
        project_id: project_id.to_owned(),
        milestone_id: milestone.id.clone(),
        milestone_canonical_id: milestone.milestone_key.clone(),
        release_revision: verified.release_revision,
        release_identity: verified.release_identity.clone(),
        milestone_definition_revision_id: definition.id.clone(),
        milestone_definition_digest: definition.content_digest.clone(),
        expected_milestone_version: readiness.expected_milestone_version,
        display_label: milestone.display_label.clone(),
        summary: definition.content.outcome.clone(),
        changelog: {
            let change_summary = definition.provenance.change_summary.trim();
            if change_summary.is_empty() {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "release requires a non-empty definition change summary".to_owned(),
                });
            }
            vec![change_summary.to_owned()]
        },
        known_issues: definition.content.known_issues.clone(),
        readiness_snapshot_id: readiness.id.clone(),
        readiness_digest: readiness.readiness_digest.clone(),
        source_event_watermark: readiness.source_event_watermark.clone(),
        charter_revision,
        document_revisions: definition.content.document_revisions.clone(),
        included_decisions: included_decisions.to_vec(),
        included_tasks,
        validation_results: readiness
            .check_results
            .iter()
            .map(|result| ReleaseValidationReference {
                validation_id: result.id.clone(),
                result_digest: result.result_digest.clone(),
                evaluated_at: result.evaluated_at.clone(),
                principal: result.principal.clone(),
                authorization: result.authorization.clone(),
                status: result.status,
                result: result.result.clone(),
                input_digest: result.input_digest.clone(),
                governing_revision_ids: result.governing_revision_ids.clone(),
            })
            .collect(),
        repository_references: readiness.commit_build_check_context.clone(),
        evidence_pins: evidence
            .iter()
            .map(|attachment| {
                let metadata = pin_metadata
                    .iter()
                    .find(|metadata| metadata.attachment_id == attachment.id)
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: format!(
                            "release evidence attachment {} is missing immutable pin metadata",
                            attachment.id
                        ),
                    })?;
                if metadata.asset_id != attachment.asset_id
                    || metadata.attachment_digest != attachment.checksum
                {
                    return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
                }
                Ok(api_types::EvidencePin {
                    id: new_uuid_v4(),
                    release_id: release_id.to_owned(),
                    attachment_id: attachment.id.clone(),
                    asset_id: metadata.asset_id.clone(),
                    attachment_digest: metadata.attachment_digest.clone(),
                    asset_checksum: metadata.asset_checksum.clone(),
                    availability: attachment.availability,
                    availability_projection: release_evidence_availability(attachment.availability),
                    task_media_id: metadata.task_media_id.clone(),
                    stable_project_url: metadata.stable_project_url.clone(),
                    pinned_at: released_at.to_owned(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?,
        waived_check_ids: readiness.waiver_ids.clone(),
        released_by: actor.clone(),
        authorization: authorization.clone(),
        released_at: released_at.to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        snapshot_digest: String::new(),
    };
    snapshot.snapshot_digest =
        release_snapshot_digest(&snapshot).map_err(map_orchestration_error)?;
    Ok(snapshot)
}

async fn insert_readiness(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &ReadinessSnapshot,
    authorization: &AuthorizationProvenance,
    idempotency_key: &str,
    expected_milestone_version: i64,
) -> crate::Result<()> {
    db.insert_milestone_readiness_in_tx(
        tx,
        &db::InsertMilestoneReadinessInput {
            id: &snapshot.id,
            project_id: &snapshot.project_id,
            milestone_id: &snapshot.milestone_id,
            definition_revision_id: &snapshot.milestone_definition_revision_id,
            input_manifest_json: &(serde_json::to_string(&snapshot.input_manifest)
                .map_err(json_error)?),
            event_watermark: &snapshot.source_event_watermark,
            outcome: readiness_result_name(snapshot.result),
            blocking_reasons_json: &(serde_json::to_string(&snapshot.reasons)
                .map_err(json_error)?),
            check_results_json: &(serde_json::to_string(&snapshot.check_results)
                .map_err(json_error)?),
            waiver_manifest_json: &(serde_json::to_string(&snapshot.waiver_ids)
                .map_err(json_error)?),
            evidence_manifest_json: &(serde_json::to_string(&PersistedEvidenceManifest {
                ids: snapshot.evidence_attachment_ids.clone(),
                digests: snapshot.evidence_digests.clone(),
                availability: snapshot.evidence_availability.clone(),
            })
            .map_err(json_error)?),
            commit_context_json: &(serde_json::to_string(&snapshot.commit_build_check_context)
                .map_err(json_error)?),
            computing_policy_revision: &snapshot.computing_policy_revision,
            readiness_digest: &snapshot.readiness_digest,
            principal_type: principal_kind_name(authorization.principal.kind),
            principal_id: &authorization.principal.id,
            authorization_basis: &authorization.authorization_basis,
            authorization_action: &authorization.action,
            expected_milestone_version,
            explicit_event: &authorization.event_id,
            authorization_occurred_at: &authorization.occurred_at,
            idempotency_key,
            created_at: &snapshot.computed_at,
        },
    )
    .await
    .map_err(map_db_conflict)?;
    for (ordinal, input) in snapshot.input_manifest.iter().enumerate() {
        let availability = if input.source_kind == "evidence" {
            match snapshot
                .evidence_attachment_ids
                .iter()
                .position(|id| id == &input.source_id)
            {
                Some(index) => Some(evidence_availability_name(
                    *snapshot.evidence_availability.get(index).ok_or_else(|| {
                        crate::ServiceError::InvalidOperation {
                            message: "readiness evidence availability index is out of bounds"
                                .to_owned(),
                        }
                    })?,
                )),
                None => {
                    return Err(crate::ServiceError::InvalidOperation {
                        message: format!(
                            "readiness evidence input {} is missing from evidence manifest",
                            input.source_id
                        ),
                    });
                }
            }
        } else {
            None
        };
        db.insert_milestone_readiness_input_in_tx(
            tx,
            &db::InsertMilestoneReadinessSourceInput {
                snapshot_id: &snapshot.id,
                ordinal: i64::try_from(ordinal).map_err(|_| {
                    crate::ServiceError::InvalidOperation {
                        message: "readiness input ordinal overflow".to_owned(),
                    }
                })?,
                source_kind: &input.source_kind,
                source_id: &input.source_id,
                source_version: &input.source_version.to_string(),
                source_digest: &input.source_digest,
                availability,
                metadata_json: &(serde_json::to_string(&json!({
                    "observed_at": input.observed_at,
                }))
                .map_err(json_error)?),
            },
        )
        .await
        .map_err(map_db_conflict)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn append_milestone_event(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    event_type: &str,
    project_id: &str,
    milestone_id: &str,
    actor: &PrincipalRef,
    idempotency_key: &str,
    payload: Value,
    created_at: &str,
) -> crate::Result<()> {
    DomainEventRepo::append_event_in_tx(
        db,
        tx,
        &CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: event_type.to_owned(),
            entity_type: "milestone".to_owned(),
            entity_id: milestone_id.to_owned(),
            actor_type: principal_kind_name(actor.kind).to_owned(),
            actor_id: Some(actor.id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: Uuid::new_v4().to_string(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("{event_type}:{project_id}:{idempotency_key}")),
            payload_json: payload.to_string(),
            created_at: created_at.to_owned(),
        },
    )
    .await?;
    Ok(())
}

fn project_milestone_from_record(
    record: ProjectMilestoneRecord,
) -> crate::Result<ProjectMilestone> {
    let milestone_id = record.id.clone();
    let definition_revision_id =
        record
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!(
                    "milestone {} has no current definition revision",
                    milestone_id
                ),
            })?;
    project_milestone_from_record_with_definition(record, definition_revision_id)
}

/// Shared projection body, taking the definition revision id explicitly so
/// callers can supply a fallback (see `MilestoneRuntime::list`) instead of
/// requiring `current_definition_revision_id` to be set.
fn project_milestone_from_record_with_definition(
    record: ProjectMilestoneRecord,
    definition_revision_id: String,
) -> crate::Result<ProjectMilestone> {
    if record.id.trim().is_empty()
        || record.project_id.trim().is_empty()
        || record.milestone_key.trim().is_empty()
        || record.milestone_sequence <= 0
        || record.version <= 0
        || record.created_at.trim().is_empty()
        || record.updated_at.trim().is_empty()
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable milestone record is missing required identity fields".to_owned(),
        });
    }
    let lifecycle = milestone_lifecycle(&record.lifecycle)?;
    let projection_reasons = projection_reasons_from_record(&record)?;
    Ok(ProjectMilestone {
        id: record.id,
        project_id: record.project_id,
        milestone_sequence: record.milestone_sequence,
        canonical_id: record.milestone_key,
        display_label: record.display_label.clone(),
        definition_revision_id,
        lifecycle,
        projection_reasons,
        version: record.version,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn projection_reasons_from_record(
    record: &ProjectMilestoneRecord,
) -> crate::Result<Vec<MilestoneProjectionReason>> {
    let mut result = Vec::new();
    for (json_value, kind) in [
        (
            &record.blocker_reason_json,
            MilestoneProjectionReasonKind::Blocker,
        ),
        (
            &record.stale_reason_json,
            MilestoneProjectionReasonKind::Stale,
        ),
        (
            &record.reconciliation_reason_json,
            MilestoneProjectionReasonKind::ReconciliationRequired,
        ),
    ] {
        let reasons: Vec<MilestoneProjectionReason> =
            parse_json_required(json_value, "milestone projection reasons")?;
        for reason in &reasons {
            let kind_matches_bucket = match kind {
                MilestoneProjectionReasonKind::Blocker => matches!(
                    reason.kind,
                    MilestoneProjectionReasonKind::Blocker
                        | MilestoneProjectionReasonKind::CheckFailed
                        | MilestoneProjectionReasonKind::EvidenceUnavailable
                ),
                _ => reason.kind == kind,
            };
            if !kind_matches_bucket {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "persisted milestone projection reason kind is inconsistent"
                        .to_owned(),
                });
            }
        }
        result.extend(reasons);
    }
    Ok(result)
}

fn definition_from_record(
    record: ProjectMilestoneRevisionRecord,
    project_id: &str,
) -> crate::Result<MilestoneDefinitionRevision> {
    let definition_id = record.id.clone();
    let acceptance_checks: Vec<MilestoneAcceptanceCheck> = parse_json_required(
        &record.acceptance_checks_json,
        "milestone definition acceptance_checks",
    )?;
    let evidence_requirements: Vec<AcceptanceEvidenceRequirement> = parse_json_required(
        &record.evidence_requirements_json,
        "milestone definition evidence_requirements",
    )?;
    let display_label = record
        .display_label
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| crate::ServiceError::InvalidOperation {
            message: format!("milestone definition {definition_id} has no display label"),
        })?;
    Ok(MilestoneDefinitionRevision {
        id: record.id,
        milestone_id: record.milestone_id,
        project_id: project_id.to_owned(),
        revision_number: record.revision,
        base_revision_id: record.base_revision_id,
        lifecycle: definition_lifecycle(&record.lifecycle)?,
        schema_version: record.schema_version,
        content: MilestoneDefinitionContent {
            name: display_label,
            outcome: record.outcome,
            included_scope: parse_json_required(
                &record.included_scope_json,
                "milestone definition included_scope",
            )?,
            excluded_scope: parse_json_required(
                &record.excluded_scope_json,
                "milestone definition excluded_scope",
            )?,
            charter_revision: record.charter_revision_id.map(|id| ArtifactRef {
                artifact_id: id.clone(),
                revision_id: id,
                content_digest: String::new(),
                render_version: None,
                render_digest: None,
            }),
            document_revisions: parse_json_required(
                &record.document_revisions_json,
                "milestone definition document_revisions",
            )?,
            task_ids: parse_json_required(
                &record.task_selection_json,
                "milestone definition task_selection",
            )?,
            dependencies: parse_json_required(
                &record.dependencies_json,
                "milestone definition dependencies",
            )?,
            risks: parse_json_required(&record.risks_json, "milestone definition risks")?,
            acceptance_checks,
            evidence_requirements,
            known_issues: parse_json_required(
                &record.known_issues_json,
                "milestone definition known_issues",
            )?,
            target_date: None,
        },
        rendered_view: record.rendered_view,
        render_version: record.render_version,
        content_digest: record.content_digest,
        render_digest: record.rendered_digest,
        provenance: RevisionProvenance {
            author: PrincipalRef {
                kind: principal_kind(&record.author_type)?,
                id: record
                    .author_id
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: format!(
                            "milestone definition {} has no authored-by principal",
                            definition_id
                        ),
                    })?,
                display_name: None,
            },
            profile_revision: None,
            operating_skill_revision: None,
            source_refs: parse_json_required(
                &record.source_refs_json,
                "milestone definition source_refs",
            )?,
            change_summary: record.change_summary,
            material_diff: None,
        },
        created_at: record.created_at,
    })
}

fn readiness_from_record(
    record: ProjectReadinessSnapshotRecord,
) -> crate::Result<ReadinessSnapshot> {
    if record.id.trim().is_empty()
        || record.project_id.trim().is_empty()
        || record.milestone_id.trim().is_empty()
        || record.definition_revision_id.trim().is_empty()
        || record.expected_milestone_version <= 0
        || record.event_watermark.trim().is_empty()
        || record.computing_policy_revision.trim().is_empty()
        || record.readiness_digest.trim().is_empty()
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable readiness snapshot is missing required authority references"
                .to_owned(),
        });
    }
    let result = readiness_result(&record.outcome)?;
    let evidence_manifest: PersistedEvidenceManifest = parse_json_required(
        &record.evidence_manifest_json,
        "readiness evidence_manifest",
    )?;
    if evidence_manifest.ids.len() != evidence_manifest.digests.len()
        || evidence_manifest.ids.len() != evidence_manifest.availability.len()
        || evidence_manifest
            .ids
            .iter()
            .any(|value| value.trim().is_empty())
        || evidence_manifest
            .digests
            .iter()
            .any(|value| value.trim().is_empty())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable readiness evidence manifest arrays must have equal length"
                .to_owned(),
        });
    }
    let requesting_principal = PrincipalRef {
        kind: principal_kind(&record.principal_type)?,
        id: record.principal_id.clone(),
        display_name: None,
    };
    let authorization = AuthorizationProvenance {
        principal: requesting_principal.clone(),
        authorization_basis: record.authorization_basis.clone(),
        action: record.authorization_action.clone(),
        event_id: record.explicit_event.clone(),
        occurred_at: record.authorization_occurred_at.clone(),
    };
    validate_persisted_authorization(&authorization, "project.milestone.readiness")?;
    let commit_build_check_context: Vec<String> =
        parse_json_required(&record.commit_context_json, "readiness commit_context")?;
    for context in &commit_build_check_context {
        let reference: RepositoryContextReference =
            parse_json_required(context, "readiness repository context")?;
        validate_repository_context_reference(&reference)?;
    }
    let input_manifest: Vec<ReadinessInput> =
        parse_json_required(&record.input_manifest_json, "readiness input_manifest")?;
    if input_manifest.is_empty()
        || input_manifest.iter().any(|input| {
            input.source_kind.trim().is_empty()
                || input.source_id.trim().is_empty()
                || input.source_version <= 0
                || input.source_digest.trim().is_empty()
                || input.observed_at.trim().is_empty()
        })
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable readiness input manifest is incomplete".to_owned(),
        });
    }
    let reasons: Vec<ReadinessReason> =
        parse_json_required(&record.blocking_reasons_json, "readiness reasons")?;
    if reasons
        .iter()
        .any(|reason| reason.code.trim().is_empty() || reason.message.trim().is_empty())
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable readiness reasons are incomplete".to_owned(),
        });
    }
    let check_results: Vec<ValidationResult> =
        parse_json_required(&record.check_results_json, "readiness check_results")?;
    for result in &check_results {
        if result.id.trim().is_empty()
            || result.check_id.trim().is_empty()
            || result.result.trim().is_empty()
            || result.input_digest.trim().is_empty()
            || result.governing_revision_ids.is_empty()
            || result
                .governing_revision_ids
                .iter()
                .any(|value| value.trim().is_empty())
            || result.expected_version <= 0
            || result.evaluated_at.trim().is_empty()
            || result.event_id.trim().is_empty()
            || result.event_id != result.authorization.event_id
            || !same_principal(&result.principal, &result.authorization.principal)
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable readiness check result is incomplete".to_owned(),
            });
        }
        validate_persisted_authorization_receipt(
            &result.authorization,
            &format!("readiness validation {}", result.id),
        )?;
    }
    let waiver_ids: Vec<String> =
        parse_json_required(&record.waiver_manifest_json, "readiness waivers")?;
    if waiver_ids.iter().any(|value| value.trim().is_empty()) {
        return Err(crate::ServiceError::InvalidOperation {
            message: "immutable readiness waivers are incomplete".to_owned(),
        });
    }
    Ok(ReadinessSnapshot {
        id: record.id,
        project_id: record.project_id,
        milestone_id: record.milestone_id,
        expected_milestone_version: record.expected_milestone_version,
        milestone_definition_revision_id: record.definition_revision_id,
        input_manifest,
        source_event_watermark: record.event_watermark,
        result,
        reasons,
        check_results,
        waiver_ids,
        evidence_attachment_ids: evidence_manifest.ids,
        evidence_digests: evidence_manifest.digests,
        evidence_availability: evidence_manifest.availability,
        commit_build_check_context,
        computing_policy_revision: record.computing_policy_revision,
        readiness_digest: record.readiness_digest,
        computed_at: record.created_at,
        requesting_principal,
        authorization,
    })
}

/// Compare only the governed readiness inputs and deterministic result.  The
/// source event watermark and computed digest are intentionally excluded: the
/// former is diagnostic metadata and the latter includes that metadata, so
/// either would make an unrelated Project event falsely invalidate a snapshot.
fn readiness_inputs_match(candidate: &ReadinessSnapshot, recomputed: &ReadinessEvaluation) -> bool {
    candidate.expected_milestone_version == recomputed.expected_milestone_version
        && candidate.milestone_definition_revision_id == recomputed.milestone_definition_revision_id
        && candidate.input_manifest == recomputed.ordered_input_manifest
        && candidate.result == recomputed.result
        && candidate.reasons == recomputed.reasons
        && candidate.check_results == recomputed.ordered_check_results
        && candidate.waiver_ids == recomputed.waiver_ids
        && candidate.evidence_attachment_ids == recomputed.evidence_attachment_ids
        && candidate.evidence_digests == recomputed.evidence_digests
        && candidate.evidence_availability == recomputed.evidence_availability
        && candidate.commit_build_check_context == recomputed.commit_build_check_context
        && candidate.computing_policy_revision == recomputed.computing_policy_revision
        && candidate.requesting_principal == recomputed.requesting_principal
        && candidate.authorization == recomputed.authorization
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct PersistedEvidenceManifest {
    ids: Vec<String>,
    digests: Vec<String>,
    availability: Vec<EvidenceAvailability>,
}

fn validate_authorization(
    authorization: &AuthorizationProvenance,
    actor: &PrincipalRef,
    action: &str,
) -> crate::Result<()> {
    validate_authorization_shape(authorization, actor, action)?;
    if !valid_authorization_timestamp(&authorization.occurred_at) {
        return Err(crate::ServiceError::AuthorizationDenied {
            message: "authorization provenance timestamp is outside the allowed clock skew"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_authorization_shape(
    authorization: &AuthorizationProvenance,
    actor: &PrincipalRef,
    action: &str,
) -> crate::Result<()> {
    if authorization.principal.kind != actor.kind
        || authorization.principal.id != actor.id
        || authorization.action != action
        || authorization.authorization_basis.trim().is_empty()
        || authorization.event_id.trim().is_empty()
        || authorization.occurred_at.trim().is_empty()
        || !well_formed_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(crate::ServiceError::AuthorizationDenied {
            message: "authorization provenance does not match the authenticated action".to_owned(),
        });
    }
    Ok(())
}

fn validate_persisted_authorization(
    authorization: &AuthorizationProvenance,
    expected_action: &str,
) -> crate::Result<()> {
    validate_persisted_authorization_receipt(authorization, expected_action)?;
    if authorization.action != expected_action {
        return Err(crate::ServiceError::InvalidOperation {
            message: "corrupt immutable authorization provenance".to_owned(),
        });
    }
    Ok(())
}

fn validate_persisted_authorization_receipt(
    authorization: &AuthorizationProvenance,
    field: &str,
) -> crate::Result<()> {
    if authorization.principal.id.trim().is_empty()
        || authorization.authorization_basis.trim().is_empty()
        || authorization.action.trim().is_empty()
        || authorization.event_id.trim().is_empty()
        || !well_formed_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: format!("{field} has corrupt immutable authorization provenance"),
        });
    }
    Ok(())
}

fn well_formed_authorization_timestamp(value: &str) -> bool {
    value.len() <= MAX_AUTHORIZATION_TIMESTAMP_LEN
        && value.trim() == value
        && DateTime::parse_from_rfc3339(value).is_ok()
}

fn valid_authorization_timestamp(value: &str) -> bool {
    if !well_formed_authorization_timestamp(value) {
        return false;
    }
    let Ok(timestamp) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    let elapsed = Utc::now().signed_duration_since(timestamp.with_timezone(&Utc));
    elapsed.num_seconds().abs() <= MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS
}

fn map_orchestration_error(error: MilestoneOrchestrationError) -> crate::ServiceError {
    crate::ServiceError::InvalidOperation {
        message: error.to_string(),
    }
}

fn map_db_conflict(error: db::DbError) -> crate::ServiceError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        crate::ServiceError::Db(db::DbError::VersionConflict)
    } else {
        crate::ServiceError::Db(error)
    }
}

fn json_error(error: impl std::fmt::Display) -> crate::ServiceError {
    crate::ServiceError::InvalidOperation {
        message: format!("invalid canonical JSON: {error}"),
    }
}

fn parse_json_required<T: DeserializeOwned>(value: &str, field: &str) -> crate::Result<T> {
    serde_json::from_str(value).map_err(|error| crate::ServiceError::InvalidOperation {
        message: format!("corrupt immutable {field} JSON: {error}"),
    })
}

fn principal_kind(value: &str) -> crate::Result<PrincipalKind> {
    match value {
        "user" => Ok(PrincipalKind::User),
        "agent" => Ok(PrincipalKind::Agent),
        "worker" => Ok(PrincipalKind::Worker),
        "reviewer" => Ok(PrincipalKind::Reviewer),
        "service" => Ok(PrincipalKind::Service),
        "system" => Ok(PrincipalKind::System),
        other => Err(crate::ServiceError::InvalidOperation {
            message: format!("unknown persisted principal kind {other}"),
        }),
    }
}

fn principal_kind_name(value: PrincipalKind) -> &'static str {
    match value {
        PrincipalKind::User => "user",
        PrincipalKind::Agent => "agent",
        PrincipalKind::Worker => "worker",
        PrincipalKind::Reviewer => "reviewer",
        PrincipalKind::Service => "service",
        PrincipalKind::System => "system",
    }
}

fn milestone_lifecycle(value: &str) -> crate::Result<MilestoneLifecycle> {
    serde_json::from_value(json!(value)).map_err(|_| crate::ServiceError::InvalidOperation {
        message: format!("invalid milestone lifecycle {value}"),
    })
}

fn definition_lifecycle(value: &str) -> crate::Result<MilestoneDefinitionLifecycle> {
    serde_json::from_value(json!(value)).map_err(|_| crate::ServiceError::InvalidOperation {
        message: format!("invalid milestone definition lifecycle {value}"),
    })
}

fn readiness_result(value: &str) -> crate::Result<ReadinessResult> {
    serde_json::from_value(json!(value)).map_err(|_| crate::ServiceError::InvalidOperation {
        message: format!("invalid readiness result {value}"),
    })
}

fn readiness_result_name(value: ReadinessResult) -> &'static str {
    match value {
        ReadinessResult::Ready => "ready",
        ReadinessResult::Blocked => "blocked",
        ReadinessResult::Failed => "failed",
        ReadinessResult::Stale => "stale",
    }
}

fn check_status(value: &str) -> crate::Result<AcceptanceCheckResultStatus> {
    match value {
        "passed" => Ok(AcceptanceCheckResultStatus::Pass),
        "failed" => Ok(AcceptanceCheckResultStatus::Fail),
        "stale" => Ok(AcceptanceCheckResultStatus::Stale),
        "waived" => Ok(AcceptanceCheckResultStatus::Waived),
        "missing" => Ok(AcceptanceCheckResultStatus::Blocked),
        "pending" => Ok(AcceptanceCheckResultStatus::Pending),
        "blocked" => Ok(AcceptanceCheckResultStatus::Blocked),
        "unavailable" => Ok(AcceptanceCheckResultStatus::Unavailable),
        other => Err(crate::ServiceError::InvalidOperation {
            message: format!("unknown persisted acceptance check outcome {other}"),
        }),
    }
}

fn evidence_kind(value: Option<&str>) -> crate::Result<EvidenceKind> {
    match value {
        Some("screenshot") => Ok(EvidenceKind::Screenshot),
        Some("walkthrough_video") => Ok(EvidenceKind::WalkthroughVideo),
        Some("log") => Ok(EvidenceKind::Log),
        Some("report") => Ok(EvidenceKind::Report),
        Some("other") => Ok(EvidenceKind::Other),
        None => Err(crate::ServiceError::InvalidOperation {
            message: "persisted evidence kind is missing".to_owned(),
        }),
        Some(other) => Err(crate::ServiceError::InvalidOperation {
            message: format!("unknown persisted evidence kind {other}"),
        }),
    }
}

fn evidence_availability(value: &str) -> crate::Result<EvidenceAvailability> {
    match value {
        "available" => Ok(EvidenceAvailability::Available),
        "quarantined" => Ok(EvidenceAvailability::Quarantined),
        "redacted" => Ok(EvidenceAvailability::Redacted),
        "purged" => Ok(EvidenceAvailability::Purged),
        other => Err(crate::ServiceError::InvalidOperation {
            message: format!("unknown persisted evidence availability {other}"),
        }),
    }
}

fn release_evidence_availability(
    value: EvidenceAvailability,
) -> api_types::ReleaseEvidenceAvailability {
    match value {
        EvidenceAvailability::Available => api_types::ReleaseEvidenceAvailability::Available,
        EvidenceAvailability::Quarantined => api_types::ReleaseEvidenceAvailability::Quarantined,
        EvidenceAvailability::Redacted => api_types::ReleaseEvidenceAvailability::Redacted,
        EvidenceAvailability::Purged => api_types::ReleaseEvidenceAvailability::Purged,
    }
}

fn release_evidence_tombstone(
    value: &str,
) -> crate::Result<api_types::ReleaseEvidenceAvailability> {
    match value {
        "redacted" => Ok(api_types::ReleaseEvidenceAvailability::Redacted),
        // A post-release purge is represented by the immutable historical
        // pin plus an audited read-time overlay.  Never rewrite the pin's
        // four-state asset availability; expose the distinct release view.
        "purged" => Ok(api_types::ReleaseEvidenceAvailability::EvidenceUnavailable),
        "evidence_unavailable" => Ok(api_types::ReleaseEvidenceAvailability::EvidenceUnavailable),
        other => Err(crate::ServiceError::InvalidOperation {
            message: format!("unknown persisted media tombstone availability {other}"),
        }),
    }
}

fn evidence_availability_name(value: EvidenceAvailability) -> &'static str {
    match value {
        EvidenceAvailability::Available => "available",
        EvidenceAvailability::Quarantined => "quarantined",
        EvidenceAvailability::Redacted => "redacted",
        EvidenceAvailability::Purged => "purged",
    }
}

mod definitions;

mod readiness;

mod release;

mod inputs;

#[cfg(test)]
mod tests;
