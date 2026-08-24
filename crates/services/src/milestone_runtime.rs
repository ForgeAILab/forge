//! Project-local milestone, readiness, and release persistence.
//!
//! The database migration owns the durable shape and the pure
//! `milestone_orchestration` module owns deterministic policy.  This module
//! is the narrow transaction boundary between those two layers.  It keeps the
//! release path out of chat/runtime code and, importantly, never treats a
//! cached overview or a previous `ready_for_release` state as release
//! authority.

use std::{collections::HashSet, sync::Arc};

use api_types::{
    canonical_digest_with_schema, AcceptanceCheckResultStatus, AcceptanceEvidenceRequirement,
    ArtifactRef, AuthorizationProvenance, EvidenceAttachment, EvidenceAvailability, EvidenceKind,
    ExecutionBaselineReleasePolicy, MilestoneAcceptanceCheck, MilestoneDefinitionContent,
    MilestoneDefinitionLifecycle, MilestoneDefinitionRevision, MilestoneLifecycle,
    MilestoneProjectionReason, MilestoneProjectionReasonKind, PrincipalKind, PrincipalRef,
    ProjectMilestone, ProjectRelease, ReadinessFreshness, ReadinessFreshnessStatus, ReadinessInput,
    ReadinessReason, ReadinessResult, ReadinessSnapshot, ReleaseDecisionReference, ReleaseSnapshot,
    ReleaseTaskReference, ReleaseValidationReference, RevisionProvenance, ValidationResult,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEventRepo, ProjectMilestoneRecord,
    ProjectMilestoneRevisionRecord, ProjectOrchestrationRepo, ProjectReadinessSnapshotRecord,
    ProjectReleaseRecord, SqliteDb,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};
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

/// Exact active-baseline inputs that govern one readiness evaluation.
///
/// The acceptance matrix is intentionally retained as typed data here rather
/// than treated as an opaque part of `baseline_digest`: readiness must fail
/// closed when the approved baseline names checks that the pinned milestone
/// definition does not actually contain.
struct ActiveBaselineInputs {
    baseline_digest: String,
    stored_policy_revision: String,
    release_policy_digest: String,
    required_check_definition_revisions: Vec<String>,
    milestone_ids: Vec<String>,
    milestone_definition_revision_ids: Vec<String>,
    acceptance_matrix: Vec<AcceptanceEvidenceRequirement>,
}

fn baseline_definition_contract_reasons(
    milestone_id: &str,
    definition: &MilestoneDefinitionRevision,
    baseline: &ActiveBaselineInputs,
) -> Vec<ReadinessReason> {
    let reason = |code: &str, message: String, source_ids: Vec<String>| ReadinessReason {
        code: code.to_owned(),
        message,
        blocking: true,
        check_id: None,
        source_ids,
    };

    if baseline.milestone_ids.len() != baseline.milestone_definition_revision_ids.len() {
        return vec![reason(
            "baseline_manifest_reconciliation_required",
            "reconciliation_required: the active baseline milestone manifest is malformed"
                .to_owned(),
            vec![milestone_id.to_owned()],
        )];
    }
    let matching_indices = baseline
        .milestone_ids
        .iter()
        .enumerate()
        .filter_map(|(index, candidate)| (candidate == milestone_id).then_some(index))
        .collect::<Vec<_>>();
    let [index] = matching_indices.as_slice() else {
        return vec![reason(
            "baseline_milestone_reconciliation_required",
            "reconciliation_required: the readiness milestone is not governed exactly once by the active baseline"
                .to_owned(),
            vec![milestone_id.to_owned()],
        )];
    };
    if baseline.milestone_definition_revision_ids[*index] != definition.id {
        return vec![reason(
            "baseline_definition_reconciliation_required",
            "reconciliation_required: the active baseline does not pin the milestone's current definition revision"
                .to_owned(),
            vec![
                milestone_id.to_owned(),
                baseline.milestone_definition_revision_ids[*index].clone(),
                definition.id.clone(),
            ],
        )];
    }

    let required_check_revisions = baseline
        .required_check_definition_revisions
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let acceptance_ids = definition
        .content
        .acceptance_checks
        .iter()
        .map(|check| check.id.as_str())
        .collect::<HashSet<_>>();
    let evidence_ids = definition
        .content
        .evidence_requirements
        .iter()
        .map(|requirement| requirement.id.as_str())
        .collect::<HashSet<_>>();
    let mut reasons = Vec::new();
    let mut matrix_ids = HashSet::<String>::new();
    for requirement in &baseline.acceptance_matrix {
        let requirement_id = requirement.id.trim();
        let check_definition_revision = requirement
            .check_definition_revision
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if requirement_id.is_empty()
            || requirement.description.trim().is_empty()
            || !matrix_ids.insert(requirement_id.to_owned())
        {
            reasons.push(reason(
                "baseline_matrix_reconciliation_required",
                "reconciliation_required: the active-baseline acceptance/evidence matrix contains an empty or duplicate requirement"
                    .to_owned(),
                vec![requirement.id.clone()],
            ));
        }
        match check_definition_revision {
            Some(revision) if required_check_revisions.contains(revision) => {}
            Some(revision) => reasons.push(reason(
                "baseline_check_definition_reconciliation_required",
                format!(
                    "reconciliation_required: active-baseline requirement '{}' references check definition '{}' outside the release policy",
                    requirement.id, revision
                ),
                vec![requirement.id.clone(), revision.to_owned()],
            )),
            None => reasons.push(reason(
                "baseline_check_definition_reconciliation_required",
                format!(
                    "reconciliation_required: active-baseline requirement '{}' does not name a check definition revision",
                    requirement.id
                ),
                vec![requirement.id.clone()],
            )),
        }
    }

    for check in definition
        .content
        .acceptance_checks
        .iter()
        .filter(|check| check.required)
    {
        let evidence_requirement = definition
            .content
            .evidence_requirements
            .iter()
            .find(|requirement| requirement.required && requirement.id == check.id);
        let Some(evidence_requirement) = evidence_requirement else {
            reasons.push(reason(
                "milestone_evidence_contract_reconciliation_required",
                format!(
                    "reconciliation_required: required acceptance check '{}' has no required evidence requirement with the same stable id",
                    check.id
                ),
                vec![check.id.clone(), definition.id.clone()],
            ));
            continue;
        };
        let matrix_requirement = baseline
            .acceptance_matrix
            .iter()
            .find(|requirement| requirement.id == check.id);
        let Some(matrix_requirement) = matrix_requirement else {
            continue;
        };
        if !matrix_requirement.required
            || matrix_requirement.description != check.description
            || matrix_requirement.evidence_kind != evidence_requirement.evidence_kind
            || matrix_requirement.check_definition_revision.as_deref()
                != Some(definition.id.as_str())
        {
            reasons.push(reason(
                "baseline_requirement_semantics_reconciliation_required",
                format!(
                    "reconciliation_required: active-baseline requirement '{}' does not exactly match the pinned milestone check, evidence kind, and definition revision",
                    check.id
                ),
                vec![check.id.clone(), definition.id.clone()],
            ));
        }
    }

    let mut definition_ids = definition
        .content
        .acceptance_checks
        .iter()
        .filter(|check| check.required)
        .map(|check| check.id.clone())
        .chain(
            definition
                .content
                .evidence_requirements
                .iter()
                .filter(|requirement| requirement.required)
                .map(|requirement| requirement.id.clone()),
        )
        .collect::<Vec<_>>();
    definition_ids.sort();
    definition_ids.dedup();
    let mut definition_only = definition_ids
        .iter()
        .filter(|id| !matrix_ids.contains(id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut matrix_only = if baseline.milestone_ids.len() == 1 {
        matrix_ids
            .iter()
            .filter(|id| {
                !acceptance_ids.contains(id.as_str()) && !evidence_ids.contains(id.as_str())
            })
            .cloned()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    definition_only.sort();
    matrix_only.sort();
    if !definition_only.is_empty() || !matrix_only.is_empty() {
        let mut source_ids = definition_only.clone();
        source_ids.extend(matrix_only.clone());
        reasons.push(reason(
            "baseline_requirement_reconciliation_required",
            format!(
                "reconciliation_required: active-baseline requirement IDs do not match the pinned milestone definition (definition-only: [{}]; matrix-only: [{}])",
                definition_only.join(", "),
                matrix_only.join(", ")
            ),
            source_ids,
        ));
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

    /// List only milestones belonging to the requested Project.
    ///
    /// A milestone can legitimately have `current_definition_revision_id ==
    /// NULL` mid-flow (a "define" proposal's first revision stays `draft`
    /// until promoted, and the pointer trigger only accepts a
    /// `proposed`/`approved` target).  Failing the whole list on one such row
    /// would hide every other milestone in the Project, so this falls back to
    /// each unset milestone's latest definition revision instead.
    pub async fn list(&self, project_id: &str) -> crate::Result<Vec<ProjectMilestone>> {
        let rows = ProjectOrchestrationRepo::list_project_milestones(&*self.db, project_id).await?;
        let mut milestones = Vec::with_capacity(rows.len());
        for record in rows {
            if let Some(milestone) = self.project_milestone_projection(record).await? {
                milestones.push(milestone);
            }
        }
        Ok(milestones)
    }

    /// Load one milestone after checking its Project scope at the query
    /// boundary.  A caller cannot use this method to probe another Project's
    /// milestone by identifier.
    pub async fn get(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestone>> {
        let Some(row) =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, milestone_id).await?
        else {
            return Ok(None);
        };
        if row.project_id != project_id {
            return Ok(None);
        }
        self.project_milestone_projection(row).await
    }

    /// Project a milestone record into its public API shape, tolerating an
    /// unset `current_definition_revision_id` pointer (see `list` above) by
    /// falling back to the latest definition revision of any lifecycle.
    /// Returns `None` only when the milestone has no definition revision at
    /// all -- which atomic milestone creation should make impossible -- so
    /// that one corrupt row is skipped instead of failing every caller.
    async fn project_milestone_projection(
        &self,
        record: ProjectMilestoneRecord,
    ) -> crate::Result<Option<ProjectMilestone>> {
        if record.current_definition_revision_id.is_some() {
            return project_milestone_from_record(record).map(Some);
        }
        let revisions =
            ProjectOrchestrationRepo::list_project_milestone_revisions(&*self.db, &record.id)
                .await?;
        let Some(latest_id) = revisions
            .into_iter()
            .max_by_key(|revision| revision.revision)
            .map(|revision| revision.id)
        else {
            tracing::warn!(
                milestone_id = %record.id,
                "milestone has no definition revision; skipping from projection"
            );
            return Ok(None);
        };
        project_milestone_from_record_with_definition(record, latest_id).map(Some)
    }

    /// Return a current definition revision only when it belongs to the
    /// requested Project/milestone pair.
    pub async fn definition(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<MilestoneDefinitionRevision>> {
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(None);
        };
        let Some(revision_id) = milestone.current_definition_revision_id else {
            return Ok(None);
        };
        let Some(revision) =
            ProjectOrchestrationRepo::get_project_milestone_revision(&*self.db, &revision_id)
                .await?
        else {
            return Ok(None);
        };
        if revision.milestone_id != milestone_id {
            return Ok(None);
        }
        let definition = definition_from_record(revision, project_id)?;
        Ok(Some(self.hydrate_definition(project_id, definition).await?))
    }

    /// Load one exact immutable definition revision within Project scope.
    pub async fn definition_revision(
        &self,
        project_id: &str,
        milestone_id: &str,
        revision_id: &str,
    ) -> crate::Result<Option<MilestoneDefinitionRevision>> {
        let Some(revision) =
            ProjectOrchestrationRepo::get_project_milestone_revision(&*self.db, revision_id)
                .await?
        else {
            return Ok(None);
        };
        if revision.milestone_id != milestone_id {
            return Ok(None);
        }
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(None);
        };
        if milestone.project_id != project_id {
            return Ok(None);
        }
        let definition = definition_from_record(revision, project_id)?;
        Ok(Some(self.hydrate_definition(project_id, definition).await?))
    }

    /// List immutable definition revisions for one Project-local milestone.
    pub async fn revisions(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<MilestoneDefinitionRevision>> {
        let Some(milestone) = self.get_record(project_id, milestone_id).await? else {
            return Ok(Vec::new());
        };
        if milestone.project_id != project_id {
            return Ok(Vec::new());
        }
        let definitions =
            ProjectOrchestrationRepo::list_project_milestone_revisions(&*self.db, milestone_id)
                .await?
                .into_iter()
                .map(|revision| definition_from_record(revision, project_id))
                .collect::<crate::Result<Vec<_>>>()?;
        let mut hydrated = Vec::with_capacity(definitions.len());
        for definition in definitions {
            hydrated.push(self.hydrate_definition(project_id, definition).await?);
        }
        Ok(hydrated)
    }

    /// Compute one readiness candidate without persisting it.  Command
    /// services use this read-locked computation to prepare the complete
    /// immutable snapshot, then hand that snapshot to the command-aware DB
    /// composite, which commits the snapshot, lifecycle projection, event,
    /// receipt, and optional action execution together.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn evaluate_candidate(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
        baseline_id: &str,
        baseline_revision_id: &str,
        release_policy_revision: &str,
    ) -> crate::Result<ReadinessSnapshot> {
        if expected_milestone_version < 1
            || project_id.trim().is_empty()
            || milestone_id.trim().is_empty()
            || baseline_id.trim().is_empty()
            || baseline_revision_id.trim().is_empty()
            || release_policy_revision.trim().is_empty()
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "readiness candidate references are incomplete".to_owned(),
            });
        }
        validate_authorization(authorization, actor, "project.milestone.readiness")?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if actor.kind == PrincipalKind::Agent {
            let bound_agent = self
                .project_agent_principal_in_tx(&mut tx, project_id)
                .await?;
            if bound_agent.id != actor.id {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "readiness may only be requested by the bound Project Agent"
                        .to_owned(),
                });
            }
        }
        let locked = sqlx::query(
            "UPDATE project_milestone SET version = version
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(&mut *tx)
        .await?;
        if locked.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let milestone = self
            .get_record_in_tx(&mut tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        let revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(&mut tx, project_id, milestone_id, &revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone_definition_revision", revision_id.clone())
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        let baseline = self
            .baseline_inputs_in_tx(
                &mut tx,
                project_id,
                baseline_id,
                baseline_revision_id,
                release_policy_revision,
            )
            .await?;
        let baseline_contract_reasons =
            baseline_definition_contract_reasons(milestone_id, &definition, &baseline);
        let baseline_digest = baseline.baseline_digest;
        let release_policy_digest = baseline.release_policy_digest;
        let check_results = self
            .check_results_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
                baseline_revision_id,
                release_policy_revision,
                &release_policy_digest,
            )
            .await?;
        let evidence = self
            .evidence_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let waiver_ids = self
            .waiver_ids_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let (task_states, document_states) = self
            .source_states_in_tx(&mut tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(&mut tx, project_id, &task_states)
            .await?;
        let input_manifest = self
            .input_manifest_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition.content.charter_revision.as_ref(),
                baseline_id,
                baseline_revision_id,
                &baseline_digest,
                release_policy_revision,
                &release_policy_digest,
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let source_event_watermark = self.source_watermark_in_tx(&mut tx, project_id).await?;
        let evaluation = evaluate_readiness(ReadinessEvaluationInput {
            milestone: project_milestone_from_record(milestone)?,
            definition,
            baseline_id: baseline_id.to_owned(),
            baseline_revision_id: baseline_revision_id.to_owned(),
            baseline_digest,
            release_policy_revision: release_policy_revision.to_owned(),
            release_policy_digest,
            source_event_watermark,
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence,
            waiver_ids,
            task_states,
            document_states,
            commit_build_check_context,
            baseline_contract_reasons,
            authorization: authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        let snapshot = evaluation.into_snapshot(new_uuid_v4(), now_rfc3339());
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Compute and persist one immutable readiness candidate.  The exact
    /// source rows are loaded before the insert and their versions/digests are
    /// included in the pure evaluator's ordered manifest.  A successful
    /// candidate compare-and-swaps `active` to `ready_for_release`; non-ready
    /// candidates remain active and persist typed projection reasons.
    #[allow(clippy::too_many_arguments)]
    pub async fn evaluate(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
        baseline_id: &str,
        baseline_revision_id: &str,
        release_policy_revision: &str,
        idempotency_key: &str,
    ) -> crate::Result<ReadinessSnapshot> {
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "readiness idempotency key is required".to_owned(),
            });
        }

        // Read and write the complete readiness candidate on one connection.
        // SQLite's write transaction is opened before any source reload; this
        // prevents a check/evidence/baseline mutation from being observed
        // after the candidate was computed but before the lifecycle CAS.
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        if let Some(existing) = sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(project_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .map(readiness_record_from_row)
        .transpose()?
        {
            if existing.milestone_id != milestone_id
                || existing.expected_milestone_version != expected_milestone_version
                || existing.baseline_id != baseline_id
                || existing.baseline_revision_id != baseline_revision_id
                || existing.release_policy_revision != release_policy_revision
                || existing.principal_type != principal_kind_name(actor.kind)
                || existing.principal_id != actor.id
                || existing.principal_type != principal_kind_name(authorization.principal.kind)
                || existing.principal_id != authorization.principal.id
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            tx.commit().await?;
            return readiness_from_record(existing);
        }
        validate_authorization(authorization, actor, "project.milestone.readiness")?;
        if baseline_id.trim().is_empty()
            || baseline_revision_id.trim().is_empty()
            || release_policy_revision.trim().is_empty()
        {
            return Err(crate::ServiceError::InvalidOperation {
                message:
                    "readiness requires explicit active baseline and release-policy references"
                        .to_owned(),
            });
        }
        if actor.kind == PrincipalKind::Agent {
            let bound_agent = self
                .project_agent_principal_in_tx(&mut tx, project_id)
                .await?;
            if bound_agent.id != actor.id {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "readiness may only be requested by the bound Project Agent"
                        .to_owned(),
                });
            }
        }

        // Acquire the milestone write lock before loading any source row. A
        // readiness candidate must observe one authoritative SQLite snapshot;
        // a later lifecycle CAS alone would still permit a source mutation to
        // race between the first read and the evaluation.
        let locked = sqlx::query(
            "UPDATE project_milestone SET version = version
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(&mut *tx)
        .await?;
        if locked.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let milestone = self
            .get_record_in_tx(&mut tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        if milestone.version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(&mut tx, project_id, milestone_id, &revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone_definition_revision", revision_id.clone())
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        let baseline = self
            .baseline_inputs_in_tx(
                &mut tx,
                project_id,
                baseline_id,
                baseline_revision_id,
                release_policy_revision,
            )
            .await?;
        let baseline_contract_reasons =
            baseline_definition_contract_reasons(milestone_id, &definition, &baseline);
        let baseline_digest = baseline.baseline_digest;
        let release_policy_digest = baseline.release_policy_digest;
        let check_results = self
            .check_results_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
                baseline_revision_id,
                release_policy_revision,
                &release_policy_digest,
            )
            .await?;
        let evidence = self
            .evidence_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let waiver_ids = self
            .waiver_ids_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        let (task_states, document_states) = self
            .source_states_in_tx(&mut tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(&mut tx, project_id, &task_states)
            .await?;
        let input_manifest = self
            .input_manifest_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &revision_id,
                definition.content.charter_revision.as_ref(),
                baseline_id,
                baseline_revision_id,
                &baseline_digest,
                release_policy_revision,
                &release_policy_digest,
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let source_event_watermark = self.source_watermark_in_tx(&mut tx, project_id).await?;
        let evaluation = evaluate_readiness(ReadinessEvaluationInput {
            milestone: project_milestone_from_record(milestone.clone())?,
            definition,
            baseline_id: baseline_id.to_owned(),
            baseline_revision_id: baseline_revision_id.to_owned(),
            baseline_digest,
            release_policy_revision: release_policy_revision.to_owned(),
            release_policy_digest,
            source_event_watermark,
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence,
            waiver_ids,
            task_states: task_states.clone(),
            document_states: document_states.clone(),
            commit_build_check_context,
            baseline_contract_reasons,
            authorization: authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        let snapshot = evaluation
            .clone()
            .into_snapshot(new_uuid_v4(), now_rfc3339());

        let current: i64 = sqlx::query_scalar(
            "SELECT version FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(db::DbError::NotFound)?;
        if current != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        insert_readiness(
            &mut tx,
            &snapshot,
            authorization,
            idempotency_key,
            expected_milestone_version,
        )
        .await?;
        let projection_reasons = projection_reasons(&evaluation.reasons);
        let (next_lifecycle, blocker, stale, reconciliation) = if milestone.lifecycle == "released"
        {
            // Correction readiness is observational only for an immutable
            // release. Never regress a released milestone to active, even
            // when the fresh candidate is blocked/stale/failed.
            (
                "released",
                serde_json::to_string(&projection_reasons.blockers).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.stale).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.reconciliation).map_err(json_error)?,
            )
        } else if evaluation.result == ReadinessResult::Ready {
            (
                if milestone.lifecycle == "released" {
                    "released"
                } else {
                    "ready_for_release"
                },
                "[]".to_owned(),
                "[]".to_owned(),
                "[]".to_owned(),
            )
        } else {
            (
                "active",
                serde_json::to_string(&projection_reasons.blockers).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.stale).map_err(json_error)?,
                serde_json::to_string(&projection_reasons.reconciliation).map_err(json_error)?,
            )
        };
        let changed = sqlx::query(
            "UPDATE project_milestone
             SET lifecycle = ?, blocker_reason_json = ?, stale_reason_json = ?,
                 reconciliation_reason_json = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?
               AND lifecycle IN ('active', 'ready_for_release', 'released')",
        )
        .bind(next_lifecycle)
        .bind(blocker)
        .bind(stale)
        .bind(reconciliation)
        .bind(&snapshot.computed_at)
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        self.repair_primary_pointer_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.readiness.evaluated",
            project_id,
            milestone_id,
            actor,
            idempotency_key,
            json!({
                "readiness_snapshot_id": snapshot.id,
                "readiness_digest": snapshot.readiness_digest,
                "result": snapshot.result,
                "projection_reasons": projection_reasons,
                "authorization": authorization,
            }),
            &snapshot.computed_at,
        )
        .await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Re-authorize the exact candidate, recompute its digest, and atomically
    /// write one immutable release revision plus evidence pins.  This method
    /// intentionally does not create a new readiness snapshot during release.
    #[allow(clippy::too_many_arguments)]
    pub async fn release(
        &self,
        project_id: &str,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        milestone_id: &str,
        expected_milestone_version: i64,
        readiness_snapshot_id: &str,
        readiness_digest: &str,
        idempotency_key: &str,
    ) -> crate::Result<ProjectRelease> {
        if actor.kind != PrincipalKind::User {
            return Err(crate::ServiceError::InvalidOperation {
                message: "only an authenticated user may release a milestone".to_owned(),
            });
        }
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "release idempotency key is required".to_owned(),
            });
        }
        if let Some(existing) = self
            .release_by_idempotency(project_id, idempotency_key)
            .await?
        {
            if existing.milestone_id != milestone_id
                || existing.readiness_snapshot_id != readiness_snapshot_id
                || existing.readiness_digest != readiness_digest
                || existing.releasing_principal_id != actor.id
                || existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.releasing_principal_id != authorization.principal.id
                || existing.releasing_principal_type
                    != principal_kind_name(authorization.principal.kind)
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            let replay = self.release_from_record(existing).await?;
            if replay.snapshot.expected_milestone_version.checked_add(1)
                != Some(expected_milestone_version)
            {
                return Err(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            return Ok(replay);
        }
        validate_authorization(authorization, actor, "project.milestone.release")?;
        // A second Confirm may carry a fresh UI key after the first request
        // committed but before the Overview refreshed. The immutable
        // readiness target is already released; return that exact record
        // instead of re-verifying it against the now-released lifecycle.
        if let Some(existing) = self
            .release_by_readiness_target(
                project_id,
                milestone_id,
                readiness_snapshot_id,
                readiness_digest,
            )
            .await?
        {
            if existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.releasing_principal_id != actor.id
            {
                return Err(crate::ServiceError::Conflict(
                    "this readiness snapshot is already released by another principal".to_owned(),
                ));
            }
            return self.release_from_record(existing).await;
        }

        // Acquire the milestone's write lock before loading any release-gating
        // inputs. SQLite has no FOR UPDATE; a guarded no-op update is the
        // compare-and-swap lock for this transaction. Every source reload and
        // the manifest/pins/lifecycle/event commit below happens while this
        // lock is held.
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let locked = sqlx::query(
            "UPDATE project_milestone SET version = version
             WHERE id = ? AND project_id = ? AND version = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(&mut *tx)
        .await?;
        if locked.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let candidate = self
            .readiness_by_id_in_tx(&mut tx, project_id, readiness_snapshot_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("readiness_snapshot", readiness_snapshot_id)
            })?;
        if candidate.milestone_id != milestone_id || candidate.project_id != project_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let recheck = self
            .recheck_readiness_in_tx(&mut tx, project_id, milestone_id, candidate)
            .await?;
        let ReadinessRecheck {
            milestone,
            definition,
            candidate_snapshot,
            recomputed,
            evidence,
            task_states,
        } = recheck;
        if !readiness_inputs_match(&candidate_snapshot, &recomputed) {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let included_decisions = self
            .effective_decision_references_in_tx(
                &mut tx,
                project_id,
                milestone_id,
                &candidate_snapshot.baseline_revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
            )
            .await?;
        let pin_metadata = self
            .release_pin_metadata_in_tx(&mut tx, project_id, milestone_id, &evidence)
            .await?;
        let release_revision = self.release_revision_in_tx(&mut tx, milestone_id).await?;
        let project_agent = self
            .project_agent_principal_in_tx(&mut tx, project_id)
            .await?;
        let verified = verify_release_candidate(
            &project_milestone_from_record(milestone.clone())?,
            &candidate_snapshot,
            &recomputed,
            readiness_snapshot_id,
            readiness_digest,
            release_revision,
            actor,
            &project_agent,
        )
        .map_err(map_orchestration_error)?;
        if milestone.version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        if milestone.version != candidate_snapshot.expected_milestone_version + 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let released_at = now_rfc3339();
        let release_id = new_uuid_v4();
        let snapshot = release_snapshot(
            project_id,
            &milestone,
            &definition,
            &candidate_snapshot,
            &verified,
            &release_id,
            &evidence,
            &pin_metadata,
            &task_states,
            &included_decisions,
            actor,
            authorization,
            idempotency_key,
            &released_at,
        )?;
        if let Some(existing) = sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(project_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing = release_record_from_row(existing)?;
            if existing.milestone_id != milestone_id
                || existing.readiness_snapshot_id != readiness_snapshot_id
                || existing.readiness_digest != readiness_digest
                || existing.releasing_principal_id != actor.id
                || existing.releasing_principal_type != principal_kind_name(actor.kind)
                || existing.authorization_basis != authorization.authorization_basis
                || existing.authorization_action != authorization.action
                || existing.explicit_event != authorization.event_id
                || existing.authorization_occurred_at != authorization.occurred_at
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            tx.commit().await?;
            return self.release_from_record(existing).await;
        }
        let current_version: i64 = sqlx::query_scalar(
            "SELECT version FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(db::DbError::NotFound)?;
        if current_version != expected_milestone_version {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let current_candidate = sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?",
        )
        .bind(readiness_snapshot_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(readiness_record_from_row)
        .transpose()?
        .ok_or(db::DbError::NotFound)?;
        if current_candidate.readiness_digest != readiness_digest
            || current_candidate.readiness_digest != verified.readiness_digest
            || current_candidate.definition_revision_id
                != candidate_snapshot.milestone_definition_revision_id
            || current_candidate.baseline_id != candidate_snapshot.baseline_id
            || current_candidate.baseline_revision_id != candidate_snapshot.baseline_revision_id
            || current_candidate.baseline_digest != candidate_snapshot.baseline_digest
            || current_candidate.release_policy_revision
                != candidate_snapshot.release_policy_revision
            || current_candidate.release_policy_digest != candidate_snapshot.release_policy_digest
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let inserted = sqlx::query(
            "INSERT INTO project_release (
                id, project_id, milestone_id, release_sequence, release_revision,
                release_identifier, milestone_revision_id, readiness_snapshot_id,
                readiness_digest, baseline_id, baseline_revision_id, baseline_digest,
                release_policy_revision, release_policy_digest, summary, changelog,
                known_issues_json, charter_revision_id, document_revisions_json, decision_ids_json,
                task_references_json, validation_references_json, git_references_json,
                evidence_references_json, waivers_json, releasing_principal_type,
                releasing_principal_id, authorization_basis, authorization_action,
                explicit_event, authorization_occurred_at, schema_version,
                snapshot_digest, idempotency_key, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&release_id)
        .bind(project_id)
        .bind(milestone_id)
        .bind(release_revision)
        .bind(release_revision)
        .bind(&snapshot.release_identity)
        .bind(&snapshot.milestone_definition_revision_id)
        .bind(readiness_snapshot_id)
        .bind(&snapshot.readiness_digest)
        .bind(&candidate_snapshot.baseline_id)
        .bind(&candidate_snapshot.baseline_revision_id)
        .bind(&candidate_snapshot.baseline_digest)
        .bind(&candidate_snapshot.release_policy_revision)
        .bind(&candidate_snapshot.release_policy_digest)
        .bind(&snapshot.summary)
        .bind(serde_json::to_string(&snapshot.changelog).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.known_issues).map_err(json_error)?)
        .bind((!snapshot.charter_revision.revision_id.is_empty())
            .then_some(snapshot.charter_revision.revision_id.as_str()))
        .bind(serde_json::to_string(&snapshot.document_revisions).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.included_decisions).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.included_tasks).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.validation_results).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.repository_references).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.evidence_pins).map_err(json_error)?)
        .bind(serde_json::to_string(&snapshot.waived_check_ids).map_err(json_error)?)
        .bind("user")
        .bind(&actor.id)
        .bind(&authorization.authorization_basis)
        .bind(&authorization.action)
        .bind(&authorization.event_id)
        .bind(&authorization.occurred_at)
        .bind(RELEASE_SCHEMA_VERSION)
        .bind(&snapshot.snapshot_digest)
        .bind(idempotency_key)
        .bind(&released_at)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_conflict)?;
        if inserted.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let mut reference_ordinal: i64 = 0;
        for task in &snapshot.included_tasks {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'task', ?, ?, ?, NULL, ?)",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(&task.task_id)
            .bind(task.task_version.to_string())
            .bind(&task.task_state)
            .bind(serde_json::to_string(task).map_err(json_error)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
        }
        for validation in &snapshot.validation_results {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'validation', ?, ?, ?, ?, ?)",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(&validation.validation_id)
            .bind(validation.evaluated_at.clone())
            .bind(format!("{:?}", validation.status).to_ascii_lowercase())
            .bind(&validation.result_digest)
            .bind(serde_json::to_string(validation).map_err(json_error)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
        }
        for document in &snapshot.document_revisions {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'document', ?, NULL, 'approved', ?, ?)",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(&document.revision_id)
            .bind(&document.content_digest)
            .bind(serde_json::to_string(document).map_err(json_error)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
        }
        for decision in &snapshot.included_decisions {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'decision', ?, NULL, ?, ?, ?)",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(&decision.decision_id)
            .bind(format!("{:?}", decision.state).to_ascii_lowercase())
            .bind(&decision.digest)
            .bind(serde_json::to_string(decision).map_err(json_error)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
        }
        for repository in &snapshot.repository_references {
            let reference: RepositoryContextReference =
                parse_json_required(repository, "release repository reference")?;
            validate_repository_context_reference(&reference)?;
            let record_id = reference
                .execution_id
                .as_deref()
                .unwrap_or(reference.task_id.as_str())
                .to_owned();
            let record_digest =
                canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &reference)
                    .map_err(json_error)?;
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'git_ref', ?, ?, ?, ?, ?)",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(record_id)
            .bind(reference.task_version.to_string())
            .bind(reference.execution_status.as_deref())
            .bind(record_digest)
            .bind(serde_json::to_string(&reference).map_err(json_error)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
            if let Some(review_id) = reference.review_id.as_deref() {
                let review_version = reference
                    .review_updated_at
                    .clone()
                    .or_else(|| reference.review_created_at.clone());
                let review_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "review_id": review_id,
                        "status": reference.review_status.clone(),
                        "ci_results": reference.ci_results.clone(),
                        "audit_result": reference.audit_result.clone(),
                        "human_decision": reference.human_decision.clone(),
                        "created_at": reference.review_created_at.clone(),
                        "updated_at": reference.review_updated_at.clone(),
                    }),
                )
                .map_err(json_error)?;
                sqlx::query(
                    "INSERT INTO project_release_reference (
                        release_id, ordinal, reference_kind, record_id,
                        record_version, record_state, record_digest, metadata_json
                     ) VALUES (?, ?, 'review', ?, ?, ?, ?, ?)",
                )
                .bind(&release_id)
                .bind(reference_ordinal)
                .bind(review_id)
                .bind(review_version)
                .bind(reference.review_status.as_deref())
                .bind(review_digest)
                .bind(serde_json::to_string(&reference).map_err(json_error)?)
                .execute(&mut *tx)
                .await
                .map_err(map_sqlx_conflict)?;
                reference_ordinal += 1;
            }
        }
        for issue in &snapshot.known_issues {
            sqlx::query(
                "INSERT INTO project_release_reference (
                    release_id, ordinal, reference_kind, record_id,
                    record_version, record_state, record_digest, metadata_json
                 ) VALUES (?, ?, 'known_issue', ?, NULL, NULL, NULL, '{}')",
            )
            .bind(&release_id)
            .bind(reference_ordinal)
            .bind(issue)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            reference_ordinal += 1;
        }
        for (ordinal, pin) in snapshot.evidence_pins.iter().enumerate() {
            let metadata = serde_json::to_string(pin).map_err(json_error)?;
            sqlx::query(
                "INSERT INTO project_release_media_pin (
                    id, project_id, release_id, asset_id, attachment_id,
                    legacy_task_media_id, asset_checksum, attachment_digest,
                    availability, pin_digest, created_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&pin.id)
            .bind(project_id)
            .bind(&pin.release_id)
            .bind(&pin.asset_id)
            .bind(&pin.attachment_id)
            .bind(pin.task_media_id.as_deref())
            .bind(&pin.asset_checksum)
            .bind(&pin.attachment_digest)
            .bind(evidence_availability_name(pin.availability))
            .bind(
                canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &metadata)
                    .map_err(json_error)?,
            )
            .bind(&released_at)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_conflict)?;
            let _ = ordinal;
        }
        let transitioned = sqlx::query(
            "UPDATE project_milestone SET lifecycle = 'released', version = version + 1,
                 updated_at = ? WHERE id = ? AND project_id = ? AND version = ?
                 AND lifecycle IN ('ready_for_release', 'released')",
        )
        .bind(&released_at)
        .bind(milestone_id)
        .bind(project_id)
        .bind(expected_milestone_version)
        .execute(&mut *tx)
        .await?;
        if transitioned.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        self.repair_primary_pointer_in_tx(&mut tx, project_id, milestone_id)
            .await?;
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.released",
            project_id,
            milestone_id,
            actor,
            idempotency_key,
            json!({
                "release_id": release_id,
                "release_identity": snapshot.release_identity,
                "readiness_snapshot_id": readiness_snapshot_id,
                "readiness_digest": readiness_digest,
                "snapshot_digest": snapshot.snapshot_digest,
                "authorization": authorization,
            }),
            &released_at,
        )
        .await?;
        let row = sqlx::query("SELECT * FROM project_release WHERE id = ?")
            .bind(&release_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        self.release_from_record(release_record_from_row(row)?)
            .await
    }

    pub async fn set_primary(
        &self,
        project_id: &str,
        expected_project_version: i64,
        primary_milestone_id: Option<&str>,
        actor: &PrincipalRef,
        authorization: &AuthorizationProvenance,
        idempotency_key: &str,
    ) -> crate::Result<()> {
        validate_authorization(authorization, actor, "project.milestone.primary.set")?;
        if idempotency_key.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "primary milestone idempotency key is required".to_owned(),
            });
        }
        let dedupe = format!("milestone.primary.set:{project_id}:{idempotency_key}");
        if let Some(event) = DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe).await? {
            let payload: Value =
                parse_json_required(&event.payload_json, "primary milestone event")?;
            let existing_primary = payload.get("primary_milestone_id").and_then(Value::as_str);
            if existing_primary != primary_milestone_id
                || payload
                    .get("expected_project_version")
                    .and_then(Value::as_i64)
                    != Some(expected_project_version)
                || payload.get("principal_id").and_then(Value::as_str) != Some(actor.id.as_str())
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            return Ok(());
        }

        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let locked = sqlx::query(
            "UPDATE project SET version = version
             WHERE id = ? AND version = ?",
        )
        .bind(project_id)
        .bind(expected_project_version)
        .execute(&mut *tx)
        .await?;
        if locked.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active'",
        )
        .bind(project_id)
        .fetch_one(&mut *tx)
        .await?;
        if active_count > 0 && primary_milestone_id.is_none() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "an active Project must retain an explicit primary milestone".to_owned(),
            });
        }
        if let Some(id) = primary_milestone_id {
            let valid: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_milestone
                 WHERE id = ? AND project_id = ? AND lifecycle = 'active'",
            )
            .bind(id)
            .bind(project_id)
            .fetch_optional(&mut *tx)
            .await?;
            if valid.is_none() {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
        }
        let occurred_at = now_rfc3339();
        let updated = sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                 updated_at = ? WHERE id = ? AND version = ?",
        )
        .bind(primary_milestone_id)
        .bind(&occurred_at)
        .bind(project_id)
        .bind(expected_project_version)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let entity_id = primary_milestone_id
            .map(str::to_owned)
            .unwrap_or_else(|| "project".to_owned());
        append_milestone_event(
            &self.db,
            &mut tx,
            "milestone.primary.set",
            project_id,
            &entity_id,
            actor,
            idempotency_key,
            json!({
                "primary_milestone_id": primary_milestone_id,
                "expected_project_version": expected_project_version,
                "principal_id": actor.id.clone(),
            }),
            &occurred_at,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Read one immutable release by Project-scoped identifier.
    pub async fn get_release(
        &self,
        project_id: &str,
        release_id: &str,
    ) -> crate::Result<Option<api_types::ProjectRelease>> {
        let Some(row) =
            sqlx::query("SELECT * FROM project_release WHERE id = ? AND project_id = ?")
                .bind(release_id)
                .bind(project_id)
                .fetch_optional(self.db.pool())
                .await?
        else {
            return Ok(None);
        };
        Ok(Some(
            self.release_from_record(release_record_from_row(row)?)
                .await?,
        ))
    }

    pub async fn get_readiness(
        &self,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<Option<ReadinessSnapshot>> {
        let Some(row) = sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ?",
        )
        .bind(snapshot_id)
        .bind(project_id)
        .bind(milestone_id)
        .fetch_optional(self.db.pool())
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(readiness_from_record(readiness_record_from_row(
            row,
        )?)?))
    }

    /// Recompute the current governed inputs for one immutable readiness
    /// snapshot without changing any persisted state.  This is deliberately
    /// the same recheck used by release: a live Overview must not infer
    /// freshness from the broad Project event stream, because attention
    /// events and readiness audit events do not change release authority.
    pub async fn readiness_freshness(
        &self,
        project_id: &str,
        milestone_id: &str,
        snapshot_id: &str,
    ) -> crate::Result<ReadinessFreshness> {
        let mut tx = self.db.pool().begin().await?;
        let Some(record) = self
            .readiness_by_id_in_tx(&mut tx, project_id, snapshot_id)
            .await?
        else {
            tx.commit().await?;
            return Ok(ReadinessFreshness {
                status: ReadinessFreshnessStatus::Unavailable,
                reason: Some("The immutable readiness snapshot is unavailable.".to_owned()),
                snapshot_source_event_watermark: None,
                current_source_event_watermark: None,
            });
        };
        if record.milestone_id != milestone_id {
            tx.commit().await?;
            return Ok(ReadinessFreshness {
                status: ReadinessFreshnessStatus::Unavailable,
                reason: Some(
                    "The readiness snapshot does not belong to this milestone.".to_owned(),
                ),
                snapshot_source_event_watermark: None,
                current_source_event_watermark: None,
            });
        }

        let candidate = readiness_from_record(record.clone())?;
        let snapshot_watermark = Some(candidate.source_event_watermark.clone());
        let current_watermark = Some(self.source_watermark_in_tx(&mut tx, project_id).await?);
        let recheck = self
            .recheck_readiness_in_tx(&mut tx, project_id, milestone_id, record)
            .await;

        let (status, reason) = match recheck {
            Ok(recheck) => {
                let expected_current_version = candidate.expected_milestone_version.checked_add(1);
                // The direct runtime evaluator records a released correction
                // with its normal one-step CAS, while the command composite
                // intentionally leaves a terminal released milestone version
                // unchanged for an observational correction. Accept exactly
                // those two representations; active/ready candidates still
                // require the one readiness transition.
                let version_matches = if recheck.milestone.lifecycle == "released" {
                    recheck.milestone.version == candidate.expected_milestone_version
                        || expected_current_version
                            .is_some_and(|version| recheck.milestone.version == version)
                } else {
                    expected_current_version
                        .is_some_and(|version| recheck.milestone.version == version)
                };
                let inputs_match = readiness_inputs_match(
                    &recheck.candidate_snapshot,
                    &recheck.recomputed,
                );
                if version_matches && inputs_match {
                    (ReadinessFreshnessStatus::Current, None)
                } else if !version_matches {
                    (
                        ReadinessFreshnessStatus::Stale,
                        Some(
                            "The milestone changed beyond the one readiness transition represented by this snapshot."
                                .to_owned(),
                        ),
                    )
                } else {
                    (
                        ReadinessFreshnessStatus::Stale,
                        Some(
                            "One or more governed readiness inputs changed after this immutable snapshot was computed."
                                .to_owned(),
                        ),
                    )
                }
            }
            Err(crate::ServiceError::Db(db::DbError::VersionConflict)) => (
                ReadinessFreshnessStatus::Stale,
                Some(
                    "One or more governed readiness inputs no longer match this immutable snapshot."
                        .to_owned(),
                ),
            ),
            Err(error) => {
                tracing::warn!(
                    project_id,
                    milestone_id,
                    snapshot_id,
                    error = %error,
                    "readiness freshness recomputation unavailable"
                );
                (
                    ReadinessFreshnessStatus::Unavailable,
                    Some("The current readiness inputs could not be rechecked.".to_owned()),
                )
            }
        };
        tx.commit().await?;
        Ok(ReadinessFreshness {
            status,
            reason,
            // These values are diagnostic only.  Freshness is determined by
            // the exact input comparison above, never by watermark equality.
            snapshot_source_event_watermark: snapshot_watermark,
            current_source_event_watermark: current_watermark,
        })
    }

    pub async fn list_readiness(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ReadinessSnapshot>> {
        let rows = sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND milestone_id = ?
             ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.db.pool())
        .await?;
        rows.into_iter()
            .map(|row| readiness_from_record(readiness_record_from_row(row)?))
            .collect()
    }

    pub async fn list_releases(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<ProjectRelease>> {
        let rows = sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND milestone_id = ?
             ORDER BY release_revision ASC, id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(self.db.pool())
        .await?;
        let mut releases = Vec::with_capacity(rows.len());
        for row in rows {
            releases.push(
                self.release_from_record(release_record_from_row(row)?)
                    .await?,
            );
        }
        Ok(releases)
    }

    async fn get_record(
        &self,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRecord>> {
        let row = ProjectOrchestrationRepo::get_project_milestone(&*self.db, milestone_id).await?;
        Ok(row.filter(|row| row.project_id == project_id))
    }

    /// Reload a milestone using the release transaction's connection.  The
    /// no-op version update in `release` is the SQLite write lock; every
    /// release-authoritative read must stay on this transaction afterwards.
    async fn get_record_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRecord>> {
        sqlx::query(
            "SELECT * FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .map(milestone_record_from_row)
        .transpose()
        .map_err(crate::ServiceError::from)
    }

    async fn readiness_by_id_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        sqlx::query(
            "SELECT * FROM project_readiness_snapshot
             WHERE project_id = ? AND id = ?",
        )
        .bind(project_id)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?
        .map(readiness_record_from_row)
        .transpose()
        .map_err(crate::ServiceError::from)
    }

    async fn definition_record_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        revision_id: &str,
    ) -> crate::Result<Option<ProjectMilestoneRevisionRecord>> {
        sqlx::query(
            "SELECT r.* FROM project_milestone_revision r
             JOIN project_milestone m ON m.id = r.milestone_id
             WHERE r.id = ? AND r.milestone_id = ? AND m.project_id = ?",
        )
        .bind(revision_id)
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .map(milestone_revision_record_from_row)
        .transpose()
        .map_err(crate::ServiceError::from)
    }

    /// Return the exact active/approved baseline and the policy revision it
    /// actually stores.  An empty baseline is retained as an input for a
    /// blocked candidate; a ready candidate is rejected later when the
    /// candidate and recomputation cannot agree on these exact values.
    async fn baseline_inputs_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        baseline_id: &str,
        baseline_revision_id: &str,
        release_policy_revision: &str,
    ) -> crate::Result<ActiveBaselineInputs> {
        if baseline_id.trim().is_empty() || baseline_revision_id.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "readiness requires explicit baseline and baseline revision references"
                    .to_owned(),
            });
        }
        let row = sqlx::query(
            "SELECT b.lifecycle AS baseline_lifecycle,
                    b.current_revision_id,
                    r.lifecycle AS revision_lifecycle,
                    r.content_digest,
                    r.release_policy_revision,
                    r.release_policy_digest,
                    r.release_policy_json,
                    r.milestone_ids_json,
                    r.milestone_definition_revision_ids_json,
                    r.acceptance_matrix_json
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r
               ON r.id = ? AND r.baseline_id = b.id
             WHERE b.id = ? AND b.project_id = ?",
        )
        .bind(baseline_revision_id)
        .bind(baseline_id)
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        };
        let baseline_lifecycle: String = row.try_get("baseline_lifecycle")?;
        let current_revision_id: String = row.try_get("current_revision_id")?;
        let revision_lifecycle: String = row.try_get("revision_lifecycle")?;
        let stored_policy_revision: String = row.try_get("release_policy_revision")?;
        let release_policy_json: String = row.try_get("release_policy_json")?;
        if baseline_lifecycle != "active"
            || revision_lifecycle != "approved"
            || current_revision_id != baseline_revision_id
            || stored_policy_revision != release_policy_revision
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let policy_envelope: Value =
            parse_json_required(&release_policy_json, "execution baseline release policy")?;
        let policy_value = policy_envelope.get("policy").cloned().ok_or_else(|| {
            crate::ServiceError::InvalidOperation {
                message: "execution baseline release policy payload is missing".to_owned(),
            }
        })?;
        let policy: ExecutionBaselineReleasePolicy =
            serde_json::from_value(policy_value).map_err(json_error)?;
        let computed_policy_digest =
            crate::execution_baseline::release_policy_digest(&policy).map_err(json_error)?;
        if policy.schema_version != crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA
            || policy.revision != stored_policy_revision
            || computed_policy_digest != row.try_get::<String, _>("release_policy_digest")?
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "active baseline contains a malformed release policy reference".to_owned(),
            });
        }
        validate_persisted_release_policy(&policy)?;
        Ok(ActiveBaselineInputs {
            baseline_digest: row.try_get("content_digest")?,
            stored_policy_revision,
            release_policy_digest: row.try_get("release_policy_digest")?,
            required_check_definition_revisions: policy.required_check_definition_revisions,
            milestone_ids: parse_json_required(
                &row.try_get::<String, _>("milestone_ids_json")?,
                "execution baseline milestone ids",
            )?,
            milestone_definition_revision_ids: parse_json_required(
                &row.try_get::<String, _>("milestone_definition_revision_ids_json")?,
                "execution baseline milestone definition revision ids",
            )?,
            acceptance_matrix: parse_json_required(
                &row.try_get::<String, _>("acceptance_matrix_json")?,
                "execution baseline acceptance/evidence matrix",
            )?,
        })
    }

    async fn hydrate_charter_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        definition: &mut MilestoneDefinitionRevision,
    ) -> crate::Result<()> {
        let Some(charter) = definition.content.charter_revision.as_mut() else {
            return Ok(());
        };
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
        .bind(&charter.revision_id)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(row) = row else {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        };
        let current_approved_revision_id: Option<String> =
            row.try_get("current_approved_revision_id")?;
        let lifecycle: String = row.try_get("lifecycle")?;
        let revision_id: String = row.try_get("revision_id")?;
        if current_approved_revision_id.as_deref() != Some(revision_id.as_str())
            || lifecycle != "approved"
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        charter.artifact_id = row.try_get("charter_id")?;
        charter.revision_id = revision_id;
        charter.content_digest = row.try_get("content_digest")?;
        charter.render_version = Some(row.try_get("render_version")?);
        charter.render_digest = Some(row.try_get("rendered_digest")?);
        Ok(())
    }

    /// Public definition reads must expose the exact approved Charter
    /// artifact rather than the identifier-only placeholder used while
    /// constructing a transaction-local definition. Missing Charter rows
    /// fail closed in `hydrate_charter_in_tx`.
    async fn hydrate_definition(
        &self,
        project_id: &str,
        mut definition: MilestoneDefinitionRevision,
    ) -> crate::Result<MilestoneDefinitionRevision> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.hydrate_charter_in_tx(&mut tx, project_id, &mut definition)
            .await?;
        tx.commit().await?;
        Ok(definition)
    }

    #[allow(clippy::too_many_arguments)]
    async fn check_results_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
        charter_revision_id: Option<&str>,
        baseline_revision_id: &str,
        release_policy_revision: &str,
        release_policy_digest: &str,
    ) -> crate::Result<Vec<ValidationResult>> {
        let rows = sqlx::query(
            "SELECT r.* FROM project_milestone_check_result r
             JOIN project_milestone_check c ON c.id = r.check_id
             WHERE r.project_id = ? AND r.milestone_id = ?
               AND r.definition_revision_id = c.definition_revision_id
             ORDER BY r.check_id ASC, r.created_at DESC, r.id DESC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(&mut **tx)
        .await?;
        validation_results_from_rows(
            rows,
            project_id,
            definition_revision_id,
            charter_revision_id,
            baseline_revision_id,
            release_policy_revision,
            release_policy_digest,
        )
    }

    async fn evidence_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<EvidenceAttachment>> {
        let rows = sqlx::query(
            "SELECT a.*, m.legacy_task_media_id, m.checksum AS asset_checksum
             FROM project_media_attachment a
             JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
             WHERE a.project_id = ? AND a.milestone_id = ?
               AND a.attachment_kind = 'evidence' AND a.deleted_at IS NULL
             ORDER BY a.id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(&mut **tx)
        .await?;
        evidence_from_rows(rows, project_id)
    }

    async fn release_pin_metadata_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        evidence: &[EvidenceAttachment],
    ) -> crate::Result<Vec<ReleasePinMetadata>> {
        if evidence.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT a.id, a.asset_id, a.task_media_id, a.project_url,
                    a.checksum AS attachment_digest, m.checksum AS asset_checksum
             FROM project_media_attachment a
             JOIN media_asset m ON m.id = a.asset_id AND m.project_id = a.project_id
             WHERE a.project_id = ? AND a.milestone_id = ?
               AND a.attachment_kind = 'evidence' AND a.deleted_at IS NULL
             ORDER BY a.id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .fetch_all(&mut **tx)
        .await?;
        let metadata: Vec<ReleasePinMetadata> = rows
            .into_iter()
            .map(|row| {
                let attachment_digest: String = row
                    .try_get::<Option<String>, _>("attachment_digest")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "release evidence attachment is missing its digest".to_owned(),
                    })?;
                let asset_checksum: String = row
                    .try_get::<Option<String>, _>("asset_checksum")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "release evidence asset is missing its checksum".to_owned(),
                    })?;
                Ok(ReleasePinMetadata {
                    attachment_id: row.try_get("id")?,
                    asset_id: row.try_get("asset_id")?,
                    attachment_digest,
                    asset_checksum,
                    task_media_id: row.try_get("task_media_id")?,
                    stable_project_url: row.try_get("project_url")?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        for attachment in evidence {
            let Some(pin) = metadata
                .iter()
                .find(|pin| pin.attachment_id == attachment.id)
            else {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "release evidence attachment {} is missing immutable pin metadata",
                        attachment.id
                    ),
                });
            };
            if pin.asset_id != attachment.asset_id || pin.attachment_digest != attachment.checksum {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
        }
        Ok(metadata)
    }

    async fn waiver_ids_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
    ) -> crate::Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT id, affected_records_json
             FROM project_decision
             WHERE project_id = ? AND decision_class = 'waiver' AND state = 'active'
               AND length(trim(authorization_action)) > 0
               AND length(trim(authorization_occurred_at)) > 0",
        )
        .bind(project_id)
        .fetch_all(&mut **tx)
        .await?;
        let mut ids = Vec::new();
        for row in rows {
            let affected: String = row.try_get("affected_records_json")?;
            let affected: Value = parse_json_required(&affected, "waiver affected_records")?;
            if affected.get("milestone_id").and_then(Value::as_str) == Some(milestone_id) {
                if let Some(check_id) = affected.get("check_id").and_then(Value::as_str) {
                    ids.push(check_id.to_owned());
                }
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    async fn effective_decision_references_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        baseline_revision_id: &str,
        charter_revision_id: Option<&str>,
    ) -> crate::Result<Vec<ReleaseDecisionReference>> {
        let rows = sqlx::query(
            "SELECT id, state, decision_class, question, context_json,
                    options_json, selected_outcome, rationale,
                    principal_type, principal_id, authority_basis,
                    authorization_action, explicit_event,
                    authorization_occurred_at, charter_revision_id,
                    baseline_revision_id, source_refs_json,
                    affected_records_json, supersedes_decision_id, created_at
             FROM project_decision
             WHERE project_id = ? AND state = 'active'
               AND (
                    json_extract(affected_records_json, '$.milestone_id') = ?
                    OR baseline_revision_id = ?
                    OR (? IS NOT NULL AND charter_revision_id = ?)
               )
             ORDER BY id ASC",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(baseline_revision_id)
        .bind(charter_revision_id)
        .bind(charter_revision_id)
        .fetch_all(&mut **tx)
        .await?;
        rows.into_iter()
            .map(|row| {
                let decision_id: String = row.try_get("id")?;
                let affected: Value = parse_json_required(
                    &row.try_get::<String, _>("affected_records_json")?,
                    "decision affected_records",
                )?;
                let affected_milestone_id = affected
                    .get("milestone_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let affected_check_id = affected
                    .get("check_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let principal = PrincipalRef {
                    kind: principal_kind(&row.try_get::<String, _>("principal_type")?)?,
                    id: row.try_get("principal_id")?,
                    display_name: None,
                };
                let authorization = AuthorizationProvenance {
                    principal,
                    authorization_basis: row.try_get("authority_basis")?,
                    action: row.try_get("authorization_action")?,
                    event_id: row.try_get("explicit_event")?,
                    occurred_at: row.try_get("authorization_occurred_at")?,
                };
                let decision_class: String = row.try_get("decision_class")?;
                let question: String = row.try_get("question")?;
                let context: Value = parse_json_required(
                    &row.try_get::<String, _>("context_json")?,
                    "decision context",
                )?;
                let options: Value = parse_json_required(
                    &row.try_get::<String, _>("options_json")?,
                    "decision options",
                )?;
                let selected_outcome: String = row.try_get("selected_outcome")?;
                let rationale: String = row.try_get("rationale")?;
                let state: String = row.try_get("state")?;
                let source_refs: Value = parse_json_required(
                    &row.try_get::<String, _>("source_refs_json")?,
                    "decision source_refs",
                )?;
                let charter_revision_id: Option<String> = row.try_get("charter_revision_id")?;
                let baseline_revision_id: Option<String> = row.try_get("baseline_revision_id")?;
                let supersedes_decision_id: Option<String> =
                    row.try_get("supersedes_decision_id")?;
                let created_at: String = row.try_get("created_at")?;
                validate_persisted_authorization_receipt(
                    &authorization,
                    &format!("decision {decision_id}"),
                )?;
                let digest = canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "decision_id": decision_id,
                        "state": state,
                        "decision_class": decision_class,
                        "question": question,
                        "context": context,
                        "options": options,
                        "selected_outcome": selected_outcome,
                        "rationale": rationale,
                        "charter_revision_id": charter_revision_id,
                        "baseline_revision_id": baseline_revision_id,
                        "source_refs": source_refs,
                        "affected_records": affected,
                        "supersedes_decision_id": supersedes_decision_id,
                        "created_at": created_at,
                        "authorization": authorization,
                    }),
                )
                .map_err(json_error)?;
                let state = match state.as_str() {
                    "active" => api_types::DecisionRecordState::Active,
                    "superseded" => api_types::DecisionRecordState::Superseded,
                    "invalidated" => api_types::DecisionRecordState::Invalidated,
                    other => {
                        return Err(crate::ServiceError::InvalidOperation {
                            message: format!("unknown persisted waiver state {other}"),
                        });
                    }
                };
                Ok(ReleaseDecisionReference {
                    decision_id,
                    state,
                    digest,
                    rationale,
                    authorization,
                    affected_milestone_id,
                    affected_check_id,
                })
            })
            .collect()
    }

    async fn repair_primary_pointer_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        changed_milestone_id: &str,
    ) -> crate::Result<()> {
        let primary: Option<String> =
            sqlx::query_scalar("SELECT primary_milestone_id FROM project WHERE id = ?")
                .bind(project_id)
                .fetch_one(&mut **tx)
                .await?;
        if primary.as_deref() != Some(changed_milestone_id) {
            return Ok(());
        }
        let changed_lifecycle: String = sqlx::query_scalar(
            "SELECT lifecycle FROM project_milestone WHERE id = ? AND project_id = ?",
        )
        .bind(changed_milestone_id)
        .bind(project_id)
        .fetch_one(&mut **tx)
        .await?;
        // Readiness and release advance the primary outcome; they do not make
        // the Project forget which milestone it delivered. Keep the pointer
        // until a user explicitly emphasizes another milestone. Only a
        // lifecycle that removes the milestone from the Project's intended
        // outcome set should require automatic repair.
        if matches!(
            changed_lifecycle.as_str(),
            "planned" | "active" | "ready_for_release" | "released"
        ) {
            return Ok(());
        }
        let replacement: Option<String> = sqlx::query_scalar(
            "SELECT id FROM project_milestone
             WHERE project_id = ? AND lifecycle = 'active' AND id != ?
             ORDER BY milestone_sequence ASC, id ASC LIMIT 1",
        )
        .bind(project_id)
        .bind(changed_milestone_id)
        .fetch_optional(&mut **tx)
        .await?;
        sqlx::query(
            "UPDATE project SET primary_milestone_id = ?, version = version + 1,
             updated_at = ? WHERE id = ? AND primary_milestone_id = ?",
        )
        .bind(replacement)
        .bind(now_rfc3339())
        .bind(project_id)
        .bind(changed_milestone_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn input_manifest_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition_revision_id: &str,
        charter: Option<&ArtifactRef>,
        baseline_id: &str,
        baseline_revision_id: &str,
        baseline_digest: &str,
        release_policy_revision: &str,
        release_policy_digest: &str,
        checks: &[ValidationResult],
        evidence: &[EvidenceAttachment],
        waiver_ids: &[String],
        task_states: &[ReadinessTaskState],
        document_states: &[ReadinessDocumentState],
        commit_build_check_context: &[String],
    ) -> crate::Result<Vec<ReadinessInput>> {
        let milestone_source = sqlx::query(
            "SELECT version, updated_at, milestone_key, display_label, lifecycle,
                    current_definition_revision_id
             FROM project_milestone
             WHERE id = ? AND project_id = ?",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_one(&mut **tx)
        .await?;
        let definition_source = sqlx::query(
            "SELECT revision, content_digest, rendered_digest, render_version,
                    lifecycle, created_at
             FROM project_milestone_revision
             WHERE id = ? AND milestone_id = ?",
        )
        .bind(definition_revision_id)
        .bind(milestone_id)
        .fetch_one(&mut **tx)
        .await?;
        let milestone_version: i64 = milestone_source.try_get("version")?;
        let milestone_observed_at: String = milestone_source.try_get("updated_at")?;
        if milestone_version <= 0 || milestone_observed_at.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "milestone source manifest has invalid version or timestamp".to_owned(),
            });
        }
        let current_definition_revision_id: Option<String> =
            milestone_source.try_get("current_definition_revision_id")?;
        if current_definition_revision_id.as_deref() != Some(definition_revision_id) {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let milestone_digest = canonical_digest_with_schema(
            MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &json!({
                "project_id": project_id,
                "milestone_id": milestone_id,
                "milestone_key": milestone_source.try_get::<String, _>("milestone_key")?,
                "display_label": milestone_source.try_get::<Option<String>, _>("display_label")?,
                "lifecycle": milestone_source.try_get::<String, _>("lifecycle")?,
                "current_definition_revision_id": milestone_source
                    .try_get::<Option<String>, _>("current_definition_revision_id")?,
            }),
        )
        .map_err(json_error)?;
        let definition_revision: i64 = definition_source.try_get("revision")?;
        let definition_digest: String = definition_source.try_get("content_digest")?;
        let definition_observed_at: String = definition_source.try_get("created_at")?;
        let definition_source_digest = canonical_digest_with_schema(
            MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
            &json!({
                "revision": definition_revision,
                "content_digest": definition_digest,
                "rendered_digest": definition_source.try_get::<String, _>("rendered_digest")?,
                "render_version": definition_source.try_get::<String, _>("render_version")?,
                "lifecycle": definition_source.try_get::<String, _>("lifecycle")?,
            }),
        )
        .map_err(json_error)?;
        let mut inputs = vec![
            ReadinessInput {
                source_kind: "milestone".to_owned(),
                source_id: milestone_id.to_owned(),
                source_version: milestone_version,
                source_digest: milestone_digest,
                observed_at: milestone_observed_at,
            },
            ReadinessInput {
                source_kind: "milestone_definition".to_owned(),
                source_id: definition_revision_id.to_owned(),
                source_version: definition_revision,
                source_digest: definition_source_digest,
                observed_at: definition_observed_at.clone(),
            },
        ];
        if let Some(charter) = charter {
            let charter_source = sqlx::query(
                "SELECT revision, created_at, content_digest, render_version, rendered_digest
                 FROM project_charter_revision
                 WHERE id = ? AND charter_id = ?",
            )
            .bind(&charter.revision_id)
            .bind(&charter.artifact_id)
            .fetch_one(&mut **tx)
            .await?;
            let charter_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "artifact_id": charter.artifact_id,
                    "revision_id": charter.revision_id,
                    "content_digest": charter_source.try_get::<String, _>("content_digest")?,
                    "render_version": charter_source.try_get::<String, _>("render_version")?,
                    "render_digest": charter_source.try_get::<String, _>("rendered_digest")?,
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "charter".to_owned(),
                source_id: charter.revision_id.clone(),
                source_version: charter_source.try_get("revision")?,
                source_digest: charter_digest,
                observed_at: charter_source.try_get("created_at")?,
            });
        }
        if !baseline_id.is_empty() {
            let baseline_source = sqlx::query(
                "SELECT b.version, r.created_at
                 FROM project_execution_baseline b
                 JOIN project_execution_baseline_revision r
                   ON r.id = ? AND r.baseline_id = b.id
                 WHERE b.id = ? AND b.project_id = ? AND b.current_revision_id = r.id",
            )
            .bind(baseline_revision_id)
            .bind(baseline_id)
            .bind(project_id)
            .fetch_one(&mut **tx)
            .await?;
            inputs.push(ReadinessInput {
                source_kind: "baseline".to_owned(),
                source_id: baseline_id.to_owned(),
                source_version: baseline_source.try_get("version")?,
                source_digest: baseline_digest.to_owned(),
                observed_at: baseline_source.try_get("created_at")?,
            });
        }
        let policy_observed_at = if !baseline_id.is_empty() {
            inputs
                .iter()
                .find(|input| input.source_kind == "baseline")
                .map(|input| input.observed_at.clone())
                .unwrap_or_else(|| definition_observed_at.clone())
        } else {
            definition_observed_at.clone()
        };
        inputs.push(ReadinessInput {
            source_kind: "release_policy".to_owned(),
            source_id: release_policy_revision.to_owned(),
            source_version: 1,
            source_digest: release_policy_digest.to_owned(),
            observed_at: policy_observed_at,
        });
        for result in checks {
            inputs.push(ReadinessInput {
                source_kind: "validation".to_owned(),
                source_id: result.id.clone(),
                source_version: result.expected_version,
                source_digest: result.result_digest.clone(),
                observed_at: result.evaluated_at.clone(),
            });
        }
        for attachment in evidence {
            let authorization: AuthorizationProvenance = parse_json_required(
                &sqlx::query_scalar::<_, String>(
                    "SELECT authorization_json FROM project_media_attachment
                     WHERE id = ? AND project_id = ?",
                )
                .bind(&attachment.id)
                .bind(project_id)
                .fetch_one(&mut **tx)
                .await?,
                "evidence authorization",
            )?;
            if !same_principal(&authorization.principal, &attachment.author) {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "evidence attachment {} authorization principal disagrees with its author",
                        attachment.id
                    ),
                });
            }
            validate_persisted_authorization(&authorization, EVIDENCE_ATTACH_AUTHORIZATION_ACTION)?;
            inputs.push(ReadinessInput {
                source_kind: "evidence".to_owned(),
                source_id: attachment.id.clone(),
                source_version: attachment.version,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "attachment": attachment,
                        "authorization": authorization,
                    }),
                )
                .map_err(json_error)?,
                observed_at: attachment.captured_at.clone(),
            });
        }
        for waiver_id in waiver_ids {
            let waiver = sqlx::query(
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
            .bind(waiver_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!("active waiver for check {waiver_id} is unavailable"),
            })?;
            let waiver_principal = PrincipalRef {
                kind: principal_kind(&waiver.try_get::<String, _>("principal_type")?)?,
                id: waiver
                    .try_get::<Option<String>, _>("principal_id")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: format!("active waiver {waiver_id} is missing its principal"),
                    })?,
                display_name: None,
            };
            let waiver_authorization = AuthorizationProvenance {
                principal: waiver_principal,
                authorization_basis: waiver.try_get("authority_basis")?,
                action: waiver.try_get("authorization_action")?,
                event_id: waiver.try_get("explicit_event")?,
                occurred_at: waiver.try_get("authorization_occurred_at")?,
            };
            validate_persisted_authorization_receipt(
                &waiver_authorization,
                &format!("waiver {waiver_id}"),
            )?;
            let waiver_payload = json!({
                "id": waiver.try_get::<String, _>("id")?,
                "rationale": waiver.try_get::<String, _>("rationale")?,
                "authorization": waiver_authorization,
                "affected_records": parse_json_required::<Value>(
                    &waiver.try_get::<String, _>("affected_records_json")?,
                    "waiver affected_records",
                )?,
            });
            inputs.push(ReadinessInput {
                source_kind: "waiver".to_owned(),
                source_id: waiver_id.clone(),
                source_version: 1,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &waiver_payload,
                )
                .map_err(json_error)?,
                observed_at: waiver.try_get("created_at")?,
            });
        }
        for task in task_states {
            let source_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "task_id": task.task_id,
                    "version": task.version,
                    "task_type": task.task_type,
                    "state": task.state,
                    "observed_at": task.observed_at,
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "task".to_owned(),
                source_id: task.task_id.clone(),
                source_version: task.version,
                source_digest,
                observed_at: task.observed_at.clone(),
            });
        }
        for document in document_states {
            let source_digest = canonical_digest_with_schema(
                MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                &json!({
                    "document_id": document.document_id,
                    "revision_id": document.revision_id,
                    "version": document.version,
                    "lifecycle": document.lifecycle,
                    "current_approved": document.current_approved,
                    "content_digest": document.content_digest,
                }),
            )
            .map_err(json_error)?;
            inputs.push(ReadinessInput {
                source_kind: "document".to_owned(),
                source_id: document.revision_id.clone(),
                source_version: document.version,
                source_digest,
                observed_at: document.observed_at.clone(),
            });
        }
        for context in commit_build_check_context {
            let reference: RepositoryContextReference =
                parse_json_required(context, "repository execution/review context")?;
            validate_repository_context_reference(&reference)?;
            inputs.push(ReadinessInput {
                source_kind: "repository_context".to_owned(),
                source_id: reference
                    .execution_id
                    .clone()
                    .unwrap_or_else(|| format!("task:{}", reference.task_id)),
                source_version: reference.task_version,
                source_digest: canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &reference,
                )
                .map_err(json_error)?,
                observed_at: reference.observed_at,
            });
        }
        Ok(inputs)
    }

    async fn source_states_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        definition: &MilestoneDefinitionRevision,
    ) -> crate::Result<(Vec<ReadinessTaskState>, Vec<ReadinessDocumentState>)> {
        // A milestone gates on every Task bound to it, not only the ones its
        // definition happens to list: governance binds Tasks to milestones at
        // creation, and a definition authored before (or without) that Task
        // selection must not compute "ready" while governed work is open.
        // Cancelled/archived/deleted governed Tasks no longer gate.
        let governed_task_ids: Vec<String> = sqlx::query_scalar(
            "SELECT g.task_id FROM project_task_governance g
             JOIN task t ON t.id = g.task_id
             WHERE g.milestone_id = ? AND t.project_id = ?
               AND t.deleted_at IS NULL AND t.archived_at IS NULL
               AND t.status != 'cancelled'
             ORDER BY g.task_id",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_all(&mut **tx)
        .await?;
        let mut selected_task_ids: Vec<&String> = definition.content.task_ids.iter().collect();
        for task_id in &governed_task_ids {
            if !selected_task_ids.contains(&task_id) {
                selected_task_ids.push(task_id);
            }
        }
        let mut tasks = Vec::with_capacity(selected_task_ids.len());
        for task_id in selected_task_ids {
            let row = sqlx::query(
                "SELECT version, task_type, status, deleted_at, updated_at
                 FROM task WHERE id = ? AND project_id = ?",
            )
            .bind(task_id)
            .bind(project_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!("milestone references missing Task {task_id}"),
            })?;
            let deleted_at: Option<String> = row.try_get("deleted_at")?;
            let version: i64 = row.try_get("version")?;
            let task_type: String = row.try_get("task_type")?;
            let task_status: String = row.try_get("status")?;
            let observed_at: String = row.try_get("updated_at")?;
            if version <= 0
                || task_type.trim().is_empty()
                || task_status.trim().is_empty()
                || observed_at.trim().is_empty()
            {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!("selected Task {task_id} has incomplete source metadata"),
                });
            }
            tasks.push(ReadinessTaskState {
                task_id: task_id.clone(),
                version,
                task_type,
                state: if deleted_at.is_some() {
                    "deleted".to_owned()
                } else {
                    task_status
                },
                observed_at,
            });
        }

        let mut documents = Vec::with_capacity(definition.content.document_revisions.len());
        for document in &definition.content.document_revisions {
            let row = sqlx::query(
                "SELECT d.version, d.updated_at, d.current_approved_revision_id,
                        r.id AS revision_id, r.lifecycle, r.content_digest
                 FROM project_document d
                 JOIN project_document_revision r
                   ON r.id = ? AND r.document_id = d.id
                 WHERE d.id = ? AND d.project_id = ?",
            )
            .bind(&document.revision_id)
            .bind(&document.artifact_id)
            .bind(project_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!(
                    "milestone references missing Document revision {}",
                    document.revision_id
                ),
            })?;
            let current_approved_revision_id: Option<String> =
                row.try_get("current_approved_revision_id")?;
            let content_digest: String = row.try_get("content_digest")?;
            if document.content_digest.trim().is_empty()
                || document.content_digest != content_digest
            {
                return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
            }
            let version: i64 = row.try_get("version")?;
            let lifecycle: String = row.try_get("lifecycle")?;
            let observed_at: String = row.try_get("updated_at")?;
            if version <= 0 || lifecycle.trim().is_empty() || observed_at.trim().is_empty() {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!(
                        "Document revision {} has incomplete source metadata",
                        document.revision_id
                    ),
                });
            }
            documents.push(ReadinessDocumentState {
                document_id: document.artifact_id.clone(),
                revision_id: row.try_get("revision_id")?,
                version,
                lifecycle,
                current_approved: current_approved_revision_id.as_deref()
                    == Some(document.revision_id.as_str()),
                content_digest,
                observed_at,
            });
        }
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        documents.sort_by(|left, right| {
            (left.document_id.as_str(), left.revision_id.as_str())
                .cmp(&(right.document_id.as_str(), right.revision_id.as_str()))
        });
        Ok((tasks, documents))
    }

    async fn commit_build_check_context_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        tasks: &[ReadinessTaskState],
    ) -> crate::Result<Vec<String>> {
        let mut context = Vec::new();
        for task in tasks {
            let rows = sqlx::query(
                "SELECT t.repo_id, repo.name AS repository_name, repo.work_mode AS repository_kind,
                        repo.remote_url, repo.default_branch,
                        e.id AS execution_id, e.status AS execution_status, e.role AS execution_role,
                        e.before_sha, e.after_sha, e.summary AS execution_summary,
                        e.created_at AS execution_created_at, e.updated_at AS execution_updated_at,
                        r.id AS review_id, r.status AS review_status,
                        r.step_results_json AS ci_results, NULL AS audit_result,
                        NULL AS human_decision,
                        r.created_at AS review_created_at, r.updated_at AS review_updated_at
                 FROM task t
                 JOIN repo ON repo.id = t.repo_id AND repo.project_id = t.project_id
                 LEFT JOIN execution e ON e.id = (
                     SELECT e2.id FROM execution e2
                     WHERE e2.task_id = t.id
                     ORDER BY e2.updated_at DESC, e2.id DESC LIMIT 1
                 )
                 LEFT JOIN review r ON r.id = (
                     SELECT r2.id FROM review r2
                     WHERE r2.execution_id = e.id
                     ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1
                 )
                 WHERE t.id = ? AND t.project_id = ?
                 LIMIT 1",
            )
            .bind(&task.task_id)
            .bind(project_id)
            .fetch_optional(&mut **tx)
            .await?;
            let row = rows.ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: format!("selected Task {} has no repository record", task.task_id),
            })?;
            let reference = RepositoryContextReference {
                task_id: task.task_id.clone(),
                task_version: task.version,
                repository_id: row.try_get("repo_id")?,
                repository_name: row.try_get("repository_name")?,
                repository_kind: row.try_get("repository_kind")?,
                remote_url: row.try_get("remote_url")?,
                default_branch: row.try_get("default_branch")?,
                execution_id: row.try_get("execution_id")?,
                execution_status: row.try_get("execution_status")?,
                execution_role: row.try_get("execution_role")?,
                before_sha: row.try_get("before_sha")?,
                after_sha: row.try_get("after_sha")?,
                execution_summary: row.try_get("execution_summary")?,
                execution_created_at: row.try_get("execution_created_at")?,
                execution_updated_at: row.try_get("execution_updated_at")?,
                review_id: row.try_get("review_id")?,
                review_status: row.try_get("review_status")?,
                ci_results: row.try_get("ci_results")?,
                audit_result: row.try_get("audit_result")?,
                human_decision: row.try_get("human_decision")?,
                review_created_at: row.try_get("review_created_at")?,
                review_updated_at: row.try_get("review_updated_at")?,
                observed_at: task.observed_at.clone(),
            };
            validate_repository_context_reference(&reference).map_err(|error| {
                crate::ServiceError::InvalidOperation {
                    message: format!(
                        "selected Task {} has incomplete repository context: {error}",
                        task.task_id
                    ),
                }
            })?;
            context.push(api_types::canonical_json(&reference).map_err(json_error)?);
        }
        context.sort();
        Ok(context)
    }

    /// Recompute the exact current readiness inputs for a persisted candidate.
    /// The candidate's expected milestone version is retained for the pure
    /// evaluator because recording readiness is the one permitted transition
    /// between candidate creation and the live milestone row.  This mirrors
    /// the release verification path; callers decide whether to persist or
    /// merely compare the result.
    async fn recheck_readiness_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        milestone_id: &str,
        candidate: ProjectReadinessSnapshotRecord,
    ) -> crate::Result<ReadinessRecheck> {
        let milestone = self
            .get_record_in_tx(tx, project_id, milestone_id)
            .await?
            .ok_or_else(|| crate::ServiceError::not_found("milestone", milestone_id))?;
        if candidate.milestone_id != milestone_id || candidate.project_id != project_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let expected_revision_id = milestone
            .current_definition_revision_id
            .clone()
            .ok_or_else(|| crate::ServiceError::InvalidOperation {
                message: "milestone has no current definition revision".to_owned(),
            })?;
        let definition_record = self
            .definition_record_in_tx(tx, project_id, milestone_id, &expected_revision_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found(
                    "milestone_definition_revision",
                    expected_revision_id.clone(),
                )
            })?;
        let mut definition = definition_from_record(definition_record, project_id)?;
        self.hydrate_charter_in_tx(tx, project_id, &mut definition)
            .await?;
        if candidate.definition_revision_id != expected_revision_id {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }

        let baseline_id = candidate.baseline_id.clone();
        let baseline_revision_id = candidate.baseline_revision_id.clone();
        let release_policy_revision = candidate.release_policy_revision.clone();
        let baseline = self
            .baseline_inputs_in_tx(
                tx,
                project_id,
                &baseline_id,
                &baseline_revision_id,
                &release_policy_revision,
            )
            .await?;
        let baseline_contract_reasons =
            baseline_definition_contract_reasons(milestone_id, &definition, &baseline);
        if baseline.stored_policy_revision != release_policy_revision
            || baseline.baseline_digest != candidate.baseline_digest
            || baseline.release_policy_digest != candidate.release_policy_digest
        {
            return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
        }
        let baseline_digest = baseline.baseline_digest;
        let release_policy_digest = baseline.release_policy_digest;

        let check_results = self
            .check_results_in_tx(
                tx,
                project_id,
                milestone_id,
                &expected_revision_id,
                definition
                    .content
                    .charter_revision
                    .as_ref()
                    .map(|charter| charter.revision_id.as_str()),
                &baseline_revision_id,
                &release_policy_revision,
                &release_policy_digest,
            )
            .await?;
        let evidence = self.evidence_in_tx(tx, project_id, milestone_id).await?;
        let waiver_ids = self.waiver_ids_in_tx(tx, project_id, milestone_id).await?;
        let (task_states, document_states) = self
            .source_states_in_tx(tx, project_id, milestone_id, &definition)
            .await?;
        let commit_build_check_context = self
            .commit_build_check_context_in_tx(tx, project_id, &task_states)
            .await?;
        let mut input_manifest = self
            .input_manifest_in_tx(
                tx,
                project_id,
                milestone_id,
                &expected_revision_id,
                definition.content.charter_revision.as_ref(),
                &baseline_id,
                &baseline_revision_id,
                &baseline_digest,
                &release_policy_revision,
                &release_policy_digest,
                &check_results,
                &evidence,
                &waiver_ids,
                &task_states,
                &document_states,
                &commit_build_check_context,
            )
            .await?;
        let candidate_expected_milestone_version = candidate.expected_milestone_version;
        let candidate_snapshot = readiness_from_record(candidate)?;

        // Evaluating readiness itself advances the mutable milestone one
        // version and (for a ready result) changes active -> ready_for_release.
        // Normalize only the self-authored milestone source entry back to the
        // immutable candidate identity.  This is also correct for a released
        // correction: released rows remain terminal while their observational
        // readiness candidate is recomputed against the same one-step version.
        if let Some(milestone_input) = input_manifest
            .iter_mut()
            .find(|input| input.source_kind == "milestone")
        {
            let candidate_milestone_input = candidate_snapshot
                .input_manifest
                .iter()
                .find(|input| input.source_kind == "milestone")
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: "readiness snapshot is missing its milestone source manifest"
                        .to_owned(),
                })?;
            milestone_input.source_version = candidate_expected_milestone_version;
            milestone_input.source_digest = candidate_milestone_input.source_digest.clone();
            milestone_input.observed_at = candidate_milestone_input.observed_at.clone();
        }
        let recomputed = evaluate_readiness(ReadinessEvaluationInput {
            milestone: ProjectMilestone {
                version: candidate_expected_milestone_version,
                ..project_milestone_from_record(milestone.clone())?
            },
            definition: definition.clone(),
            baseline_id,
            baseline_revision_id,
            baseline_digest,
            release_policy_revision,
            release_policy_digest,
            // Preserve the candidate's immutable diagnostic watermark while
            // recomputing its governed inputs.  The current watermark is
            // returned separately by `readiness_freshness`; using it here
            // would make the digest/release token depend on unrelated events.
            source_event_watermark: candidate_snapshot.source_event_watermark.clone(),
            computing_policy_revision: COMPUTING_POLICY_REVISION.to_owned(),
            input_manifest,
            check_results,
            evidence: evidence.clone(),
            waiver_ids,
            task_states: task_states.clone(),
            document_states: document_states.clone(),
            commit_build_check_context,
            baseline_contract_reasons,
            authorization: candidate_snapshot.authorization.clone(),
        })
        .map_err(map_orchestration_error)?;
        Ok(ReadinessRecheck {
            milestone,
            definition,
            candidate_snapshot,
            recomputed,
            evidence,
            task_states,
        })
    }

    async fn source_watermark_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
    ) -> crate::Result<String> {
        let event_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type NOT IN (
                   'milestone.readiness.evaluated',
                   'project_release.candidate_requested'
               )
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?;
        Ok(event_id.unwrap_or_else(|| "none".to_owned()))
    }

    async fn project_agent_principal_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
    ) -> crate::Result<PrincipalRef> {
        let row = sqlx::query(
            "SELECT identity_id FROM project_agent_binding
             WHERE project_id = ? AND state = 'active'
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| crate::ServiceError::not_found("project_agent_binding", project_id))?;
        Ok(PrincipalRef {
            kind: PrincipalKind::Agent,
            id: row.try_get("identity_id")?,
            display_name: None,
        })
    }

    async fn release_revision_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        milestone_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(MAX(release_revision), 0) + 1
             FROM project_release WHERE milestone_id = ?",
        )
        .bind(milestone_id)
        .fetch_one(&mut **tx)
        .await?)
    }

    async fn readiness_by_id(
        &self,
        project_id: &str,
        id: &str,
    ) -> crate::Result<Option<ProjectReadinessSnapshotRecord>> {
        Ok(
            sqlx::query("SELECT * FROM project_readiness_snapshot WHERE project_id = ? AND id = ?")
                .bind(project_id)
                .bind(id)
                .fetch_optional(self.db.pool())
                .await?
                .map(readiness_record_from_row)
                .transpose()?,
        )
    }

    async fn release_by_idempotency(
        &self,
        project_id: &str,
        key: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        Ok(sqlx::query(
            "SELECT * FROM project_release WHERE project_id = ? AND idempotency_key = ?",
        )
        .bind(project_id)
        .bind(key)
        .fetch_optional(self.db.pool())
        .await?
        .map(release_record_from_row)
        .transpose()?)
    }

    async fn release_by_readiness_target(
        &self,
        project_id: &str,
        milestone_id: &str,
        readiness_snapshot_id: &str,
        readiness_digest: &str,
    ) -> crate::Result<Option<ProjectReleaseRecord>> {
        Ok(sqlx::query(
            "SELECT * FROM project_release
             WHERE project_id = ? AND milestone_id = ?
               AND readiness_snapshot_id = ? AND readiness_digest = ?
             ORDER BY release_revision ASC, id ASC LIMIT 1",
        )
        .bind(project_id)
        .bind(milestone_id)
        .bind(readiness_snapshot_id)
        .bind(readiness_digest)
        .fetch_optional(self.db.pool())
        .await?
        .map(release_record_from_row)
        .transpose()?)
    }

    async fn release_from_record(
        &self,
        record: ProjectReleaseRecord,
    ) -> crate::Result<ProjectRelease> {
        if record.schema_version != RELEASE_SCHEMA_VERSION
            || record.snapshot_digest.trim().is_empty()
            || record.release_revision <= 0
            || record.release_sequence <= 0
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release record has invalid schema or identity fields"
                    .to_owned(),
            });
        }
        let milestone = self
            .get_record(&record.project_id, &record.milestone_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found("milestone", record.milestone_id.clone())
            })?;
        let definition_record = ProjectOrchestrationRepo::get_project_milestone_revision(
            &*self.db,
            &record.milestone_revision_id,
        )
        .await?
        .ok_or_else(|| {
            crate::ServiceError::not_found(
                "milestone_definition_revision",
                record.milestone_revision_id.clone(),
            )
        })?;
        if definition_record.milestone_id != record.milestone_id {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release definition belongs to another milestone".to_owned(),
            });
        }
        let definition = definition_from_record(definition_record, &record.project_id)?;
        let readiness = self
            .readiness_by_id(&record.project_id, &record.readiness_snapshot_id)
            .await?
            .ok_or_else(|| {
                crate::ServiceError::not_found(
                    "readiness_snapshot",
                    record.readiness_snapshot_id.clone(),
                )
            })?;
        let readiness = readiness_from_record(readiness)?;
        if record.baseline_id.trim().is_empty()
            || record.baseline_revision_id.trim().is_empty()
            || record.baseline_digest.trim().is_empty()
            || record.release_policy_revision.trim().is_empty()
            || record.release_policy_digest.trim().is_empty()
            || record.readiness_digest != readiness.readiness_digest
            || record.baseline_id != readiness.baseline_id
            || record.baseline_revision_id != readiness.baseline_revision_id
            || record.baseline_digest != readiness.baseline_digest
            || record.release_policy_revision != readiness.release_policy_revision
            || record.release_policy_digest != readiness.release_policy_digest
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release authority references disagree with readiness"
                    .to_owned(),
            });
        }
        let pins = self
            .release_pins(
                &record.project_id,
                &record.id,
                &record.evidence_references_json,
            )
            .await?;
        let charter_revision =
            if let Some(charter_revision_id) = record.charter_revision_id.as_deref() {
                let row = sqlx::query(
                    "SELECT cr.charter_id, cr.revision, cr.content_digest,
                            cr.render_version, cr.rendered_digest
                     FROM project_charter_revision cr
                     JOIN project_charter c ON c.id = cr.charter_id
                     WHERE cr.id = ? AND c.project_id = ?",
                )
                .bind(charter_revision_id)
                .bind(&record.project_id)
                .fetch_optional(self.db.pool())
                .await?;
                row.map(|row| {
                    Ok::<_, sqlx::Error>(ArtifactRef {
                        artifact_id: row.try_get("charter_id")?,
                        revision_id: charter_revision_id.to_owned(),
                        content_digest: row.try_get("content_digest")?,
                        render_version: Some(row.try_get("render_version")?),
                        render_digest: Some(row.try_get("rendered_digest")?),
                    })
                })
                .transpose()?
                .ok_or_else(|| crate::ServiceError::InvalidOperation {
                    message: format!(
                    "immutable release references missing Charter revision {charter_revision_id}"
                ),
                })?
            } else {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "immutable release is missing its Charter revision".to_owned(),
                });
            };
        let releasing_principal = PrincipalRef {
            kind: principal_kind(&record.releasing_principal_type)?,
            id: record.releasing_principal_id.clone(),
            display_name: None,
        };
        let release_authorization = AuthorizationProvenance {
            principal: releasing_principal.clone(),
            authorization_basis: record.authorization_basis.clone(),
            action: record.authorization_action.clone(),
            event_id: record.explicit_event.clone(),
            occurred_at: record.authorization_occurred_at.clone(),
        };
        validate_persisted_authorization(&release_authorization, "project.milestone.release")?;
        let snapshot = ReleaseSnapshot {
            schema_version: record.schema_version.clone(),
            project_id: record.project_id.clone(),
            milestone_id: record.milestone_id.clone(),
            milestone_canonical_id: milestone.milestone_key.clone(),
            release_revision: record.release_revision,
            release_identity: record.release_identifier.clone(),
            milestone_definition_revision_id: record.milestone_revision_id.clone(),
            milestone_definition_digest: definition.content_digest,
            expected_milestone_version: readiness.expected_milestone_version,
            display_label: milestone.display_label.clone(),
            summary: record.summary.clone(),
            changelog: parse_json_required(&record.changelog, "release changelog")?,
            known_issues: parse_json_required(&record.known_issues_json, "release known_issues")?,
            readiness_snapshot_id: readiness.id,
            readiness_digest: record.readiness_digest.clone(),
            source_event_watermark: readiness.source_event_watermark.clone(),
            baseline_id: record.baseline_id.clone(),
            baseline_revision_id: record.baseline_revision_id.clone(),
            baseline_digest: record.baseline_digest.clone(),
            charter_revision,
            document_revisions: parse_json_required(
                &record.document_revisions_json,
                "release document_revisions",
            )?,
            included_decisions: parse_json_required(
                &record.decision_ids_json,
                "release decision_ids",
            )?,
            included_tasks: parse_json_required(
                &record.task_references_json,
                "release task_references",
            )?,
            validation_results: parse_json_required(
                &record.validation_references_json,
                "release validation_references",
            )?,
            repository_references: parse_json_required(
                &record.git_references_json,
                "release git_references",
            )?,
            evidence_pins: pins,
            waived_check_ids: parse_json_required(&record.waivers_json, "release waivers")?,
            release_policy_revision: record.release_policy_revision,
            release_policy_digest: record.release_policy_digest,
            released_by: releasing_principal,
            authorization: release_authorization,
            released_at: record.created_at.clone(),
            idempotency_key: record.idempotency_key.clone(),
            snapshot_digest: record.snapshot_digest.clone(),
        };
        let computed_snapshot_digest =
            release_snapshot_digest(&snapshot).map_err(map_orchestration_error)?;
        if computed_snapshot_digest != record.snapshot_digest {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release snapshot digest does not match its contents".to_owned(),
            });
        }
        self.validate_release_references(&record.id, &snapshot)
            .await?;
        Ok(ProjectRelease {
            id: record.id,
            project_id: record.project_id,
            milestone_id: record.milestone_id,
            release_sequence: record.release_sequence,
            release_identity: record.release_identifier.clone(),
            snapshot,
            version: record.release_revision,
            created_at: record.created_at,
        })
    }

    async fn validate_release_references(
        &self,
        release_id: &str,
        snapshot: &ReleaseSnapshot,
    ) -> crate::Result<()> {
        type ExpectedReference = (
            &'static str,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        );
        let mut expected: Vec<ExpectedReference> = Vec::new();
        for task in &snapshot.included_tasks {
            expected.push((
                "task",
                task.task_id.clone(),
                Some(task.task_version.to_string()),
                Some(task.task_state.clone()),
                None,
                serde_json::to_string(task).map_err(json_error)?,
            ));
        }
        for validation in &snapshot.validation_results {
            expected.push((
                "validation",
                validation.validation_id.clone(),
                Some(validation.evaluated_at.clone()),
                Some(format!("{:?}", validation.status).to_ascii_lowercase()),
                Some(validation.result_digest.clone()),
                serde_json::to_string(validation).map_err(json_error)?,
            ));
        }
        for document in &snapshot.document_revisions {
            expected.push((
                "document",
                document.revision_id.clone(),
                None,
                Some("approved".to_owned()),
                Some(document.content_digest.clone()),
                serde_json::to_string(document).map_err(json_error)?,
            ));
        }
        for decision in &snapshot.included_decisions {
            expected.push((
                "decision",
                decision.decision_id.clone(),
                None,
                Some(format!("{:?}", decision.state).to_ascii_lowercase()),
                Some(decision.digest.clone()),
                serde_json::to_string(decision).map_err(json_error)?,
            ));
        }
        for repository in &snapshot.repository_references {
            let reference: RepositoryContextReference =
                parse_json_required(repository, "release repository reference")?;
            validate_repository_context_reference(&reference)?;
            let record_id = reference
                .execution_id
                .as_deref()
                .unwrap_or(reference.task_id.as_str())
                .to_owned();
            let record_digest =
                canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &reference)
                    .map_err(json_error)?;
            expected.push((
                "git_ref",
                record_id,
                Some(reference.task_version.to_string()),
                reference.execution_status.clone(),
                Some(record_digest),
                serde_json::to_string(&reference).map_err(json_error)?,
            ));
            if let Some(review_id) = reference.review_id.as_deref() {
                let review_version = reference
                    .review_updated_at
                    .clone()
                    .or_else(|| reference.review_created_at.clone());
                let review_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &json!({
                        "review_id": review_id,
                        "status": reference.review_status.clone(),
                        "ci_results": reference.ci_results.clone(),
                        "audit_result": reference.audit_result.clone(),
                        "human_decision": reference.human_decision.clone(),
                        "created_at": reference.review_created_at.clone(),
                        "updated_at": reference.review_updated_at.clone(),
                    }),
                )
                .map_err(json_error)?;
                expected.push((
                    "review",
                    review_id.to_owned(),
                    review_version,
                    reference.review_status.clone(),
                    Some(review_digest),
                    serde_json::to_string(&reference).map_err(json_error)?,
                ));
            }
        }
        for issue in &snapshot.known_issues {
            expected.push((
                "known_issue",
                issue.clone(),
                None,
                None,
                None,
                "{}".to_owned(),
            ));
        }
        let rows = sqlx::query(
            "SELECT reference_kind, record_id, record_version, record_state,
                    record_digest, metadata_json
             FROM project_release_reference
             WHERE release_id = ? ORDER BY ordinal ASC",
        )
        .bind(release_id)
        .fetch_all(self.db.pool())
        .await?;
        if rows.len() != expected.len() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable release references do not match its snapshot".to_owned(),
            });
        }
        for (row, (kind, id, version, state, digest, metadata)) in rows.into_iter().zip(expected) {
            if row.try_get::<String, _>("reference_kind")? != kind
                || row.try_get::<String, _>("record_id")? != id
                || row.try_get::<Option<String>, _>("record_version")? != version
                || row.try_get::<Option<String>, _>("record_state")? != state
                || row.try_get::<Option<String>, _>("record_digest")? != digest
                || row.try_get::<String, _>("metadata_json")? != metadata
            {
                return Err(crate::ServiceError::InvalidOperation {
                    message: "immutable release reference disagrees with its snapshot".to_owned(),
                });
            }
        }
        Ok(())
    }

    async fn release_pins(
        &self,
        project_id: &str,
        release_id: &str,
        frozen_evidence_json: &str,
    ) -> crate::Result<Vec<api_types::EvidencePin>> {
        let frozen: Vec<api_types::EvidencePin> =
            parse_json_required(frozen_evidence_json, "release evidence_references")?;
        let rows = sqlx::query(
            "SELECT p.id, p.release_id, p.attachment_id, p.asset_id,
                    p.asset_checksum, p.attachment_digest, p.availability,
                    p.pin_digest, p.created_at, m.checksum AS live_asset_checksum,
                    (SELECT t.availability FROM media_asset_tombstone t
                     WHERE t.asset_id = p.asset_id
                       AND (t.release_pin_id = p.id OR
                            (t.release_pin_id IS NULL AND t.release_id = p.release_id))
                     ORDER BY t.created_at DESC, t.id DESC LIMIT 1) AS tombstone_availability
             FROM project_release_media_pin p
             JOIN media_asset m ON m.id = p.asset_id
             WHERE p.project_id = ? AND p.release_id = ? ORDER BY p.id ASC",
        )
        .bind(project_id)
        .bind(release_id)
        .fetch_all(self.db.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                let attachment_id = row
                    .try_get::<Option<String>, _>("attachment_id")?
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing attachment_id".to_owned(),
                    })?;
                let attachment_digest = row
                    .try_get::<Option<String>, _>("attachment_digest")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing attachment digest".to_owned(),
                    })?;
                let asset_checksum = row
                    .try_get::<Option<String>, _>("asset_checksum")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing asset checksum".to_owned(),
                    })?;
                if let Some(live_asset_checksum) = row
                    .try_get::<Option<String>, _>("live_asset_checksum")?
                    .filter(|value| !value.trim().is_empty())
                {
                    if live_asset_checksum != asset_checksum {
                        return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
                    }
                }
                let availability =
                    evidence_availability(&row.try_get::<String, _>("availability")?)?;
                let pin_digest: String = row
                    .try_get::<Option<String>, _>("pin_digest")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable release pin is missing pin_digest".to_owned(),
                    })?;
                let availability_projection =
                    match row.try_get::<Option<String>, _>("tombstone_availability")? {
                        Some(value) => release_evidence_tombstone(&value)?,
                        None => release_evidence_availability(availability),
                    };
                let id: String = row.try_get("id")?;
                let frozen_pin = frozen.iter().find(|pin| pin.id == id).ok_or_else(|| {
                    crate::ServiceError::InvalidOperation {
                        message: format!("release pin {id} is missing frozen metadata"),
                    }
                })?;
                if frozen_pin.attachment_digest != attachment_digest
                    || frozen_pin.asset_checksum != asset_checksum
                    || frozen_pin.attachment_id != attachment_id
                    || frozen_pin.asset_id != row.try_get::<String, _>("asset_id")?
                    || frozen_pin.release_id != row.try_get::<String, _>("release_id")?
                    || frozen_pin.availability != availability
                    || frozen_pin.availability_projection
                        != release_evidence_availability(availability)
                    || frozen_pin.pinned_at != row.try_get::<String, _>("created_at")?
                {
                    return Err(crate::ServiceError::Db(db::DbError::VersionConflict));
                }
                let frozen_metadata = serde_json::to_string(frozen_pin).map_err(json_error)?;
                let expected_pin_digest = canonical_digest_with_schema(
                    MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION,
                    &frozen_metadata,
                )
                .map_err(json_error)?;
                if pin_digest != expected_pin_digest {
                    return Err(crate::ServiceError::InvalidOperation {
                        message: "immutable release pin digest does not match its metadata"
                            .to_owned(),
                    });
                }
                Ok(api_types::EvidencePin {
                    id,
                    release_id: row.try_get("release_id")?,
                    attachment_id,
                    asset_id: row.try_get("asset_id")?,
                    attachment_digest,
                    asset_checksum,
                    availability,
                    availability_projection,
                    task_media_id: frozen_pin.task_media_id.clone(),
                    stable_project_url: frozen_pin.stable_project_url.clone(),
                    pinned_at: row.try_get("created_at")?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()
            .and_then(|pins| {
                if pins.len() != frozen.len() {
                    return Err(crate::ServiceError::InvalidOperation {
                        message: "release evidence pin set does not match frozen metadata"
                            .to_owned(),
                    });
                }
                Ok(pins)
            })
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
    rows: Vec<sqlx::sqlite::SqliteRow>,
    project_id: &str,
    expected_definition_revision_id: &str,
    expected_charter_revision_id: Option<&str>,
    expected_baseline_revision_id: &str,
    expected_policy_revision: &str,
    expected_policy_digest: &str,
) -> crate::Result<Vec<ValidationResult>> {
    let mut latest = std::collections::BTreeMap::<String, ValidationResult>::new();
    for row in rows {
        let check_id: String = row.try_get("check_id")?;
        if latest.contains_key(&check_id) {
            continue;
        }
        let source_kind: String = row.try_get("source_kind")?;
        match source_kind.as_str() {
            // The current admission path materializes only these two closed
            // source kinds. Other enum values remain schema-compatible for
            // historical rows, but cannot become release authority until a
            // server-owned projection supplies their exact provenance.
            "manual" | "policy_waiver" => {}
            other => {
                return Err(crate::ServiceError::InvalidOperation {
                    message: format!("unknown persisted check result source kind {other}"),
                });
            }
        }
        let id: String = row.try_get("id")?;
        if id.trim().is_empty() || check_id.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: "immutable validation result is missing identity fields".to_owned(),
            });
        }
        let principal = PrincipalRef {
            kind: principal_kind(&row.try_get::<String, _>("principal_type")?)?,
            id: row.try_get("principal_id")?,
            display_name: None,
        };
        let evaluated_at: String = row.try_get("created_at")?;
        let status = check_status(&row.try_get::<String, _>("outcome")?)?;
        let input_digest: String = row.try_get("input_digest")?;
        if evaluated_at.trim().is_empty() || input_digest.trim().is_empty() {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("immutable validation {id} is missing target provenance"),
            });
        }
        let source_manifest: Value = parse_json_required(
            &row.try_get::<String, _>("source_manifest_json")?,
            "validation source_manifest",
        )?;
        let definition_revision_id: String = row.try_get("definition_revision_id")?;
        let manifest_definition_revision_id = source_manifest
            .get("check_definition_revision_id")
            .and_then(Value::as_str);
        let manifest_policy_revision = source_manifest
            .get("governing_policy_revision")
            .and_then(Value::as_str);
        let manifest_policy_digest = source_manifest
            .get("governing_policy_digest")
            .and_then(Value::as_str);
        let manifest_governing_revision_ids: Option<Vec<String>> = source_manifest
            .get("governing_revision_ids")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| crate::ServiceError::InvalidOperation {
                message: format!("invalid validation governing revisions: {error}"),
            })?;
        let expected_governing_revision_ids =
            expected_charter_revision_id.map(|charter_revision_id| {
                vec![
                    charter_revision_id.to_owned(),
                    expected_baseline_revision_id.to_owned(),
                ]
            });
        let persisted_charter_revision_id: Option<String> =
            row.try_get("governing_charter_revision_id")?;
        let persisted_baseline_revision_id: Option<String> =
            row.try_get("governing_baseline_revision_id")?;
        if definition_revision_id != expected_definition_revision_id
            || manifest_definition_revision_id != Some(expected_definition_revision_id)
            || manifest_policy_revision != Some(expected_policy_revision)
            || manifest_policy_digest != Some(expected_policy_digest)
            || manifest_governing_revision_ids.as_ref() != expected_governing_revision_ids.as_ref()
            || persisted_charter_revision_id.as_deref() != expected_charter_revision_id
            || persisted_baseline_revision_id.as_deref() != Some(expected_baseline_revision_id)
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
            authorization_basis: row.try_get("authorization_basis")?,
            action: row.try_get("authorization_action")?,
            event_id: row.try_get("explicit_event")?,
            occurred_at: row.try_get("authorization_occurred_at")?,
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
                    let version: i64 = row.try_get("expected_version")?;
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
                    let event_id: String = row.try_get("explicit_event")?;
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
    rows: Vec<sqlx::sqlite::SqliteRow>,
    project_id: &str,
) -> crate::Result<Vec<EvidenceAttachment>> {
    rows.into_iter()
        .map(|row| {
            let author = PrincipalRef {
                kind: principal_kind(&row.try_get::<String, _>("author_type")?)?,
                id: row
                    .try_get::<Option<String>, _>("author_id")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing author_id".to_owned(),
                    })?,
                display_name: None,
            };
            let attachment_authorization: AuthorizationProvenance = parse_json_required(
                &row.try_get::<String, _>("authorization_json")?,
                "evidence authorization",
            )?;
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
                id: row.try_get("id")?,
                project_id: project_id.to_owned(),
                asset_id: row.try_get("asset_id")?,
                task_id: row.try_get("task_id")?,
                source_task_id: row.try_get("source_task_id")?,
                source_run_id: row.try_get("source_execution_id")?,
                source_validation_id: row.try_get("source_validation_id")?,
                source_task_version: row.try_get("source_task_version")?,
                source_context_digest: row.try_get("source_context_digest")?,
                source_definition_revision_id: row.try_get("source_definition_revision_id")?,
                milestone_id: row.try_get("milestone_id")?,
                acceptance_check_ids: parse_json_required(
                    &row.try_get::<String, _>("acceptance_check_ids_json")?,
                    "evidence acceptance_check_ids",
                )?,
                caption: row
                    .try_get::<Option<String>, _>("caption")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing caption".to_owned(),
                    })?,
                kind: evidence_kind(
                    row.try_get::<Option<String>, _>("evidence_kind")?
                        .as_deref(),
                )?,
                checksum: row
                    .try_get::<Option<String>, _>("checksum")?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| crate::ServiceError::InvalidOperation {
                        message: "immutable evidence attachment is missing checksum".to_owned(),
                    })?,
                availability: evidence_availability(&row.try_get::<String, _>("availability")?)?,
                author,
                captured_at: row.try_get("created_at")?,
                version: row.try_get("version")?,
                created_at: row.try_get("created_at")?,
                removed_at: row.try_get("deleted_at")?,
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
        baseline_id: readiness.baseline_id.clone(),
        baseline_revision_id: readiness.baseline_revision_id.clone(),
        baseline_digest: readiness.baseline_digest.clone(),
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
        release_policy_revision: readiness.release_policy_revision.clone(),
        release_policy_digest: readiness.release_policy_digest.clone(),
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
    tx: &mut Transaction<'_, Sqlite>,
    snapshot: &ReadinessSnapshot,
    authorization: &AuthorizationProvenance,
    idempotency_key: &str,
    expected_milestone_version: i64,
) -> crate::Result<()> {
    sqlx::query(
        "INSERT INTO project_readiness_snapshot (
            id, project_id, milestone_id, definition_revision_id,
            baseline_id, baseline_revision_id, baseline_digest,
            release_policy_revision, release_policy_digest,
            input_manifest_json, event_watermark, outcome, blocking_reasons_json,
            check_results_json, waiver_manifest_json, evidence_manifest_json,
            commit_context_json, computing_policy_revision, readiness_digest,
            principal_type, principal_id, authorization_basis, authorization_action,
            expected_milestone_version, explicit_event, authorization_occurred_at,
            idempotency_key, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&snapshot.id)
    .bind(&snapshot.project_id)
    .bind(&snapshot.milestone_id)
    .bind(&snapshot.milestone_definition_revision_id)
    .bind(&snapshot.baseline_id)
    .bind(&snapshot.baseline_revision_id)
    .bind(&snapshot.baseline_digest)
    .bind(&snapshot.release_policy_revision)
    .bind(&snapshot.release_policy_digest)
    .bind(serde_json::to_string(&snapshot.input_manifest).map_err(json_error)?)
    .bind(&snapshot.source_event_watermark)
    .bind(readiness_result_name(snapshot.result))
    .bind(serde_json::to_string(&snapshot.reasons).map_err(json_error)?)
    .bind(serde_json::to_string(&snapshot.check_results).map_err(json_error)?)
    .bind(serde_json::to_string(&snapshot.waiver_ids).map_err(json_error)?)
    .bind(
        serde_json::to_string(&PersistedEvidenceManifest {
            ids: snapshot.evidence_attachment_ids.clone(),
            digests: snapshot.evidence_digests.clone(),
            availability: snapshot.evidence_availability.clone(),
        })
        .map_err(json_error)?,
    )
    .bind(serde_json::to_string(&snapshot.commit_build_check_context).map_err(json_error)?)
    .bind(&snapshot.computing_policy_revision)
    .bind(&snapshot.readiness_digest)
    .bind(principal_kind_name(authorization.principal.kind))
    .bind(&authorization.principal.id)
    .bind(&authorization.authorization_basis)
    .bind(&authorization.action)
    .bind(expected_milestone_version)
    .bind(&authorization.event_id)
    .bind(&authorization.occurred_at)
    .bind(idempotency_key)
    .bind(&snapshot.computed_at)
    .execute(&mut **tx)
    .await
    .map_err(map_sqlx_conflict)?;
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
        sqlx::query(
            "INSERT INTO project_readiness_input (
                readiness_snapshot_id, ordinal, source_kind, source_id,
                source_version, source_digest, availability, disposition, metadata_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'included', ?)",
        )
        .bind(&snapshot.id)
        .bind(
            i64::try_from(ordinal).map_err(|_| crate::ServiceError::InvalidOperation {
                message: "readiness input ordinal overflow".to_owned(),
            })?,
        )
        .bind(&input.source_kind)
        .bind(&input.source_id)
        .bind(input.source_version.to_string())
        .bind(&input.source_digest)
        .bind(availability)
        .bind(
            serde_json::to_string(&json!({
                "observed_at": input.observed_at,
            }))
            .map_err(json_error)?,
        )
        .execute(&mut **tx)
        .await
        .map_err(map_sqlx_conflict)?;
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
        || record.baseline_id.trim().is_empty()
        || record.baseline_revision_id.trim().is_empty()
        || record.baseline_digest.trim().is_empty()
        || record.release_policy_revision.trim().is_empty()
        || record.release_policy_digest.trim().is_empty()
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
        baseline_id: record.baseline_id,
        baseline_revision_id: record.baseline_revision_id,
        baseline_digest: record.baseline_digest,
        release_policy_revision: record.release_policy_revision,
        release_policy_digest: record.release_policy_digest,
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
        && candidate.baseline_id == recomputed.baseline_id
        && candidate.baseline_revision_id == recomputed.baseline_revision_id
        && candidate.baseline_digest == recomputed.baseline_digest
        && candidate.release_policy_revision == recomputed.release_policy_revision
        && candidate.release_policy_digest == recomputed.release_policy_digest
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

fn milestone_record_from_row(row: sqlx::sqlite::SqliteRow) -> db::Result<ProjectMilestoneRecord> {
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
) -> db::Result<ProjectMilestoneRevisionRecord> {
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

fn readiness_record_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> db::Result<ProjectReadinessSnapshotRecord> {
    Ok(ProjectReadinessSnapshotRecord {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        milestone_id: row.try_get("milestone_id")?,
        definition_revision_id: row.try_get("definition_revision_id")?,
        baseline_id: row.try_get("baseline_id")?,
        baseline_revision_id: row.try_get("baseline_revision_id")?,
        baseline_digest: row.try_get("baseline_digest")?,
        release_policy_revision: row.try_get("release_policy_revision")?,
        release_policy_digest: row.try_get("release_policy_digest")?,
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

fn release_record_from_row(row: sqlx::sqlite::SqliteRow) -> db::Result<ProjectReleaseRecord> {
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
        baseline_id: row.try_get("baseline_id")?,
        baseline_revision_id: row.try_get("baseline_revision_id")?,
        baseline_digest: row.try_get("baseline_digest")?,
        release_policy_revision: row.try_get("release_policy_revision")?,
        release_policy_digest: row.try_get("release_policy_digest")?,
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

/// The release policy is persisted as a closed typed payload, not as an
/// arbitrary bag of strings.  Re-validate every admission rule when loading
/// an active baseline so a corrupt row cannot become readiness authority just
/// because its digest column happens to match.
fn validate_persisted_release_policy(policy: &ExecutionBaselineReleasePolicy) -> crate::Result<()> {
    if policy.schema_version != crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA
        || policy.revision.trim().is_empty()
    {
        return Err(crate::ServiceError::InvalidOperation {
            message: "active baseline release policy has an invalid schema or revision".to_owned(),
        });
    }
    validate_policy_identifiers(
        "required_check_definition_revisions",
        &policy.required_check_definition_revisions,
        true,
    )?;
    validate_policy_literals(
        "reviewer_independence_rules",
        &policy.reviewer_independence_rules,
        &["independent-reviewer"],
        true,
    )?;
    validate_policy_literals(
        "manual_attestation_rules",
        &policy.manual_attestation_rules,
        &["manual-attestation"],
        false,
    )?;
    validate_policy_literals(
        "waiver_rules",
        &policy.waiver_rules,
        &["user-waiver"],
        false,
    )?;
    validate_policy_literals(
        "evidence_kinds",
        &policy.evidence_kinds,
        &[
            "artifact",
            "ci-log",
            "media",
            "review-report",
            "test-report",
        ],
        true,
    )?;
    validate_policy_literals(
        "evidence_contexts",
        &policy.evidence_contexts,
        &[
            "commit",
            "external",
            "milestone",
            "project",
            "repository",
            "task",
        ],
        true,
    )?;
    validate_policy_literals(
        "evidence_freshness_rules",
        &policy.evidence_freshness_rules,
        &[
            "current-baseline",
            "current-charter",
            "current-commit",
            "current-milestone",
        ],
        true,
    )?;
    validate_policy_literals(
        "dependency_rules",
        &policy.dependency_rules,
        &[
            "dependencies-green",
            "dependencies-reviewed",
            "no-blocked-dependencies",
        ],
        true,
    )?;
    validate_policy_literals(
        "stale_input_rules",
        &policy.stale_input_rules,
        &["stale-baseline-blocks", "stale-evidence-blocks"],
        true,
    )?;
    validate_policy_literals(
        "forbidden_side_effects",
        &policy.forbidden_side_effects,
        &[
            "credential-access",
            "cross-project-write",
            "force-push",
            "merge",
            "publish",
            "release",
        ],
        true,
    )?;
    validate_policy_literals(
        "known_issue_rules",
        &policy.known_issue_rules,
        &[
            "known-issue-blocks",
            "known-issue-waiver",
            "record-known-issue",
        ],
        true,
    )?;
    validate_policy_literals(
        "correction_rules",
        &policy.correction_rules,
        &[
            "correct-before-release",
            "correction-required",
            "rerun-failed-checks",
        ],
        true,
    )?;
    validate_policy_literals(
        "purge_rules",
        &policy.purge_rules,
        &[
            "purge-invalid-evidence",
            "purge-revoked-evidence",
            "purge-stale-evidence",
        ],
        true,
    )?;
    Ok(())
}

/// Validate the closed release-policy contract at API admission boundaries.
/// The same validator is used when a baseline is reloaded for readiness or
/// release, so manual checks and waivers cannot accept an opaque vector bag
/// merely because its digest column was recomputed.
pub fn validate_release_policy(policy: &ExecutionBaselineReleasePolicy) -> Result<(), String> {
    validate_persisted_release_policy(policy).map_err(|error| error.to_string())
}

fn validate_policy_identifiers(
    field: &str,
    values: &[String],
    required: bool,
) -> crate::Result<()> {
    if required && values.is_empty() {
        return Err(crate::ServiceError::InvalidOperation {
            message: format!("active baseline release policy field {field} is empty"),
        });
    }
    let mut seen = HashSet::new();
    let mut previous: Option<&str> = None;
    for value in values {
        if value.trim() != value
            || value.is_empty()
            || !value.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
            })
            || !seen.insert(value.as_str())
            || previous.is_some_and(|previous| previous >= value.as_str())
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("active baseline release policy field {field} is not canonical"),
            });
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_policy_literals(
    field: &str,
    values: &[String],
    supported: &[&str],
    required: bool,
) -> crate::Result<()> {
    if required && values.is_empty() {
        return Err(crate::ServiceError::InvalidOperation {
            message: format!("active baseline release policy field {field} is empty"),
        });
    }
    let mut seen = HashSet::new();
    let mut previous: Option<&str> = None;
    for value in values {
        if value.trim() != value
            || value.is_empty()
            || !supported.contains(&value.as_str())
            || !seen.insert(value.as_str())
            || previous.is_some_and(|previous| previous >= value.as_str())
        {
            return Err(crate::ServiceError::InvalidOperation {
                message: format!("active baseline release policy field {field} is not canonical"),
            });
        }
        previous = Some(value);
    }
    Ok(())
}

fn map_orchestration_error(error: MilestoneOrchestrationError) -> crate::ServiceError {
    crate::ServiceError::InvalidOperation {
        message: error.to_string(),
    }
}

fn map_sqlx_conflict(error: sqlx::Error) -> crate::ServiceError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        crate::ServiceError::Db(db::DbError::VersionConflict)
    } else {
        crate::ServiceError::Db(error.into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_identity_ignores_the_display_label() {
        let stored = PrincipalRef {
            kind: PrincipalKind::User,
            id: "user-1".to_owned(),
            display_name: None,
        };
        let sent = PrincipalRef {
            kind: PrincipalKind::User,
            id: "user-1".to_owned(),
            display_name: Some("E2E Default User".to_owned()),
        };
        // The row keeps only kind and id, so a caller that sent the label the
        // UI displayed must still match the author rebuilt from those columns.
        assert!(same_principal(&stored, &sent));

        let other = PrincipalRef {
            kind: PrincipalKind::User,
            id: "user-2".to_owned(),
            display_name: None,
        };
        assert!(!same_principal(&stored, &other));

        let agent = PrincipalRef {
            kind: PrincipalKind::Agent,
            id: "user-1".to_owned(),
            display_name: None,
        };
        assert!(!same_principal(&stored, &agent));
    }

    #[test]
    fn result_and_principal_names_are_closed() {
        assert_eq!(readiness_result_name(ReadinessResult::Ready), "ready");
        assert_eq!(principal_kind_name(PrincipalKind::Reviewer), "reviewer");
        assert_eq!(
            evidence_availability_name(EvidenceAvailability::Purged),
            "purged"
        );
    }

    #[test]
    fn corrupt_immutable_json_is_reported_instead_of_defaulted() {
        let error = parse_json_required::<Vec<String>>("{", "release changelog")
            .expect_err("corrupt immutable JSON must fail closed");
        match error {
            crate::ServiceError::InvalidOperation { message } => {
                assert!(message.contains("corrupt immutable release changelog JSON"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn authorization_timestamps_are_rfc3339_and_bounded() {
        let now = Utc::now().to_rfc3339();
        assert!(well_formed_authorization_timestamp(&now));
        assert!(valid_authorization_timestamp(&now));
        assert!(!well_formed_authorization_timestamp("not-a-timestamp"));
        assert!(!well_formed_authorization_timestamp(&format!(" {now}")));
        let old = (Utc::now() - chrono::Duration::hours(49)).to_rfc3339();
        assert!(well_formed_authorization_timestamp(&old));
        assert!(!valid_authorization_timestamp(&old));
    }

    #[test]
    fn persisted_enum_values_fail_closed() {
        assert!(principal_kind("unknown").is_err());
        assert!(check_status("unknown").is_err());
        assert!(evidence_kind(Some("unknown")).is_err());
        assert!(evidence_availability("unknown").is_err());
        assert!(release_evidence_tombstone("unknown").is_err());
    }

    #[test]
    fn purged_release_evidence_is_projected_unavailable() {
        assert_eq!(
            release_evidence_tombstone("purged").expect("purge overlay"),
            api_types::ReleaseEvidenceAvailability::EvidenceUnavailable
        );
    }

    #[test]
    fn blocker_bucket_accepts_typed_check_and_evidence_reasons() {
        let record = ProjectMilestoneRecord {
            id: "milestone-1".to_owned(),
            project_id: "project-1".to_owned(),
            milestone_sequence: 1,
            milestone_key: "M001".to_owned(),
            display_label: None,
            current_definition_revision_id: Some("revision-1".to_owned()),
            lifecycle: "active".to_owned(),
            blocker_reason_json: serde_json::to_string(&vec![
                MilestoneProjectionReason {
                    kind: MilestoneProjectionReasonKind::CheckFailed,
                    code: "check_missing".to_owned(),
                    message: "required check has no result".to_owned(),
                    source_ids: vec!["check-1".to_owned()],
                },
                MilestoneProjectionReason {
                    kind: MilestoneProjectionReasonKind::EvidenceUnavailable,
                    code: "evidence_missing".to_owned(),
                    message: "required evidence is unavailable".to_owned(),
                    source_ids: vec!["evidence-1".to_owned()],
                },
            ])
            .expect("reasons serialize"),
            stale_reason_json: "[]".to_owned(),
            reconciliation_reason_json: "[]".to_owned(),
            version: 1,
            created_at: "2026-08-14T00:00:00Z".to_owned(),
            updated_at: "2026-08-14T00:00:00Z".to_owned(),
        };
        let reasons = projection_reasons_from_record(&record).expect("typed blockers decode");
        assert_eq!(reasons.len(), 2);
    }
}
