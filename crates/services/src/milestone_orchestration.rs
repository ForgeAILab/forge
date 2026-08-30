//! Pure milestone, readiness, and release-candidate rules.
//!
//! The repository layer owns persistence and authorization lookups.  This
//! module owns the small, deterministic rules which must be shared by those
//! paths: lifecycle transitions, primary-milestone invariants, readiness
//! computation, principal separation, and immutable release identities.
//! Nothing in this module performs I/O or creates a release pin.

use api_types::{
    canonical_digest_with_schema, AcceptanceCheckResultStatus, AcceptanceEvidenceRequirement,
    AuthorizationProvenance, EvidenceAttachment, EvidenceAvailability, EvidenceKind,
    MilestoneAcceptanceCheck, MilestoneDefinitionLifecycle, MilestoneDefinitionRevision,
    MilestoneLifecycle, PrincipalKind, PrincipalRef, ProjectMilestone, ReadinessInput,
    ReadinessReason, ReadinessResult, ReadinessSnapshot, ReleaseSnapshot, ValidationResult,
};
use serde::Serialize;

/// Schema domain for readiness digests.  It is deliberately separate from
/// the generic canonical JSON schema so a change to the readiness input
/// contract cannot silently reuse a prior candidate identity.
pub const MILESTONE_READINESS_DIGEST_SCHEMA_VERSION: &str = "forge.milestone-readiness/v1";

/// Schema domain for immutable release snapshot digests.
pub const MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION: &str = "forge.milestone-release/v1";

/// Principal-bound action which must not be performed against the same work
/// authored by that principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalAction {
    Review,
    Attest,
    Waive,
    Release,
}

impl PrincipalAction {
    const fn label(self) -> &'static str {
        match self {
            Self::Review => "review",
            Self::Attest => "attest",
            Self::Waive => "waive",
            Self::Release => "release",
        }
    }
}

/// Pure errors produced by milestone and release-candidate validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MilestoneOrchestrationError {
    #[error("invalid milestone lifecycle transition: {from:?} -> {to:?}")]
    InvalidMilestoneTransition {
        from: MilestoneLifecycle,
        to: MilestoneLifecycle,
    },

    #[error("invalid milestone definition lifecycle transition: {from:?} -> {to:?}")]
    InvalidDefinitionTransition {
        from: MilestoneDefinitionLifecycle,
        to: MilestoneDefinitionLifecycle,
    },

    #[error("milestone sequence must be positive: {sequence}")]
    InvalidMilestoneSequence { sequence: i64 },

    #[error("release revision must be positive: {revision}")]
    InvalidReleaseRevision { revision: i64 },

    #[error("primary milestone is required for project {project_id} while milestones are active")]
    PrimaryMilestoneRequired { project_id: String },

    #[error("primary milestone {milestone_id} does not belong to project {project_id}")]
    PrimaryMilestoneWrongProject {
        project_id: String,
        milestone_id: String,
    },

    #[error("primary milestone {milestone_id} was not found in project {project_id}")]
    PrimaryMilestoneNotFound {
        project_id: String,
        milestone_id: String,
    },

    #[error("primary milestone {milestone_id} is not active")]
    PrimaryMilestoneNotActive { milestone_id: String },

    #[error("released milestones are terminal")]
    ReleasedTerminal,

    #[error("cancelled milestones are terminal")]
    CancelledTerminal,

    #[error("readiness target does not match its project or milestone")]
    ReadinessTargetMismatch,

    #[error("readiness is not allowed for milestone lifecycle {lifecycle:?}")]
    ReadinessLifecycleNotEligible { lifecycle: MilestoneLifecycle },

    #[error("release candidate field {field} does not match")]
    ReleaseCandidateMismatch { field: &'static str },

    #[error("release candidate is not ready")]
    ReleaseCandidateNotReady,

    #[error("release revision {revision} is not valid for milestone lifecycle {lifecycle:?}")]
    ReleaseRevisionNotAllowed {
        revision: i64,
        lifecycle: MilestoneLifecycle,
    },

    #[error("only an authenticated user may release a milestone")]
    ReleaseRequiresUser,

    #[error("project agent may not {action} its own project outcome")]
    ProjectAgentSelfAction { action: &'static str },

    #[error("principal {principal_id} may not {action} work authored by the same principal")]
    SelfAction {
        action: &'static str,
        principal_id: String,
    },

    #[error("canonical digest failed: {message}")]
    Digest { message: String },
}

/// Validate one milestone instance transition.
pub fn validate_milestone_transition(
    from: MilestoneLifecycle,
    to: MilestoneLifecycle,
) -> Result<(), MilestoneOrchestrationError> {
    if from == to {
        return Ok(());
    }

    let allowed = match from {
        MilestoneLifecycle::Planned => matches!(
            to,
            MilestoneLifecycle::Active | MilestoneLifecycle::Cancelled
        ),
        MilestoneLifecycle::Active => {
            matches!(
                to,
                MilestoneLifecycle::ReadyForRelease | MilestoneLifecycle::Cancelled
            )
        }
        MilestoneLifecycle::ReadyForRelease => {
            matches!(
                to,
                MilestoneLifecycle::Active | MilestoneLifecycle::Released
            )
        }
        MilestoneLifecycle::Released => false,
        MilestoneLifecycle::Cancelled => false,
    };

    if allowed {
        Ok(())
    } else if from == MilestoneLifecycle::Released {
        Err(MilestoneOrchestrationError::ReleasedTerminal)
    } else if from == MilestoneLifecycle::Cancelled {
        Err(MilestoneOrchestrationError::CancelledTerminal)
    } else {
        Err(MilestoneOrchestrationError::InvalidMilestoneTransition { from, to })
    }
}

/// Validate the independent lifecycle of an immutable milestone definition
/// revision.  Definition revision state is intentionally not the milestone
/// instance state.
pub fn validate_definition_transition(
    from: MilestoneDefinitionLifecycle,
    to: MilestoneDefinitionLifecycle,
) -> Result<(), MilestoneOrchestrationError> {
    if from == to {
        return Ok(());
    }

    let allowed = match from {
        MilestoneDefinitionLifecycle::Draft => matches!(to, MilestoneDefinitionLifecycle::Proposed),
        MilestoneDefinitionLifecycle::Proposed => {
            matches!(to, MilestoneDefinitionLifecycle::Approved)
        }
        MilestoneDefinitionLifecycle::Approved => {
            matches!(to, MilestoneDefinitionLifecycle::Superseded)
        }
        MilestoneDefinitionLifecycle::Superseded => false,
    };

    if allowed {
        Ok(())
    } else {
        Err(MilestoneOrchestrationError::InvalidDefinitionTransition { from, to })
    }
}

/// Validate the Project's explicit primary-milestone invariant.
///
/// A primary pointer is required when at least one milestone is active. Once
/// chosen, it may continue to identify that outcome as it advances through
/// ready-for-release and released; delivery must not erase the Project's
/// emphasized milestone. Cancelled milestones are never eligible.
pub fn validate_primary_milestone(
    project_id: &str,
    milestones: &[ProjectMilestone],
    primary_milestone_id: Option<&str>,
) -> Result<(), MilestoneOrchestrationError> {
    let active = milestones
        .iter()
        .filter(|milestone| milestone.lifecycle == MilestoneLifecycle::Active)
        .collect::<Vec<_>>();

    let Some(milestone_id) = primary_milestone_id else {
        return if active.is_empty() {
            Ok(())
        } else {
            Err(MilestoneOrchestrationError::PrimaryMilestoneRequired {
                project_id: project_id.to_owned(),
            })
        };
    };
    let Some(milestone) = milestones.iter().find(|item| item.id == milestone_id) else {
        return Err(MilestoneOrchestrationError::PrimaryMilestoneNotFound {
            project_id: project_id.to_owned(),
            milestone_id: milestone_id.to_owned(),
        });
    };
    if milestone.project_id != project_id {
        return Err(MilestoneOrchestrationError::PrimaryMilestoneWrongProject {
            project_id: project_id.to_owned(),
            milestone_id: milestone_id.to_owned(),
        });
    }
    if milestone.lifecycle == MilestoneLifecycle::Cancelled {
        return Err(MilestoneOrchestrationError::PrimaryMilestoneNotActive {
            milestone_id: milestone_id.to_owned(),
        });
    }
    Ok(())
}

/// Compare principals by authenticated identity, not display name.
pub fn principals_equal(left: &PrincipalRef, right: &PrincipalRef) -> bool {
    left.kind == right.kind && left.id == right.id
}

/// Deny a reviewer/attester/waiver actor from acting on work authored by the
/// same principal.  This helper intentionally has no role or persistence
/// lookups; callers must provide the authenticated and authored principals.
pub fn validate_independent_principal(
    action: PrincipalAction,
    actor: &PrincipalRef,
    authored_by: &PrincipalRef,
) -> Result<(), MilestoneOrchestrationError> {
    if principals_equal(actor, authored_by) {
        Err(MilestoneOrchestrationError::SelfAction {
            action: action.label(),
            principal_id: actor.id.clone(),
        })
    } else {
        Ok(())
    }
}

/// Enforce the Project Agent boundary for self-attestation, self-waiver, and
/// self-release.  The same generic principal comparison is used for all
/// actions so aliases or display-name changes cannot bypass it.
pub fn validate_project_agent_action(
    action: PrincipalAction,
    actor: &PrincipalRef,
    project_agent: &PrincipalRef,
) -> Result<(), MilestoneOrchestrationError> {
    if principals_equal(actor, project_agent) {
        Err(MilestoneOrchestrationError::ProjectAgentSelfAction {
            action: action.label(),
        })
    } else {
        Ok(())
    }
}

/// Validate the final release actor.  Release is an explicit user action;
/// readiness state and Task completion are never treated as authority.
pub fn validate_release_actor(
    actor: &PrincipalRef,
    project_agent: &PrincipalRef,
) -> Result<(), MilestoneOrchestrationError> {
    if actor.kind != PrincipalKind::User {
        return Err(MilestoneOrchestrationError::ReleaseRequiresUser);
    }
    validate_project_agent_action(PrincipalAction::Release, actor, project_agent)
}

/// Inputs used by the pure readiness evaluator.  The caller supplies all
/// exact source versions observed by its transaction; this type performs no
/// repository reads and therefore cannot silently refresh one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessEvaluationInput {
    pub milestone: ProjectMilestone,
    pub definition: MilestoneDefinitionRevision,
    pub source_event_watermark: String,
    pub computing_policy_revision: String,
    pub input_manifest: Vec<ReadinessInput>,
    pub check_results: Vec<ValidationResult>,
    pub evidence: Vec<EvidenceAttachment>,
    pub waiver_ids: Vec<String>,
    /// Exact Task rows referenced by the immutable definition.  A terminal
    /// Task is not, by itself, acceptance; the evaluator still requires every
    /// authoritative check and evidence requirement below.
    pub task_states: Vec<ReadinessTaskState>,
    /// Exact Document revision projections referenced by the immutable
    /// definition.  These are included in the digest and cannot be replaced
    /// by a cached overview projection.
    pub document_states: Vec<ReadinessDocumentState>,
    pub commit_build_check_context: Vec<String>,
    /// Reconciliation blockers discovered inside the milestone definition
    /// itself. These are governed readiness inputs, not transport errors:
    /// they must be digested and persisted in a non-ready snapshot so
    /// operators can reconcile the exact conflict.
    pub definition_contract_reasons: Vec<ReadinessReason>,
    /// The authority receipt is part of the candidate identity. A caller
    /// cannot replay the same idempotency key with different provenance.
    pub authorization: AuthorizationProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadinessTaskState {
    pub task_id: String,
    pub version: i64,
    pub task_type: String,
    pub state: String,
    pub observed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadinessDocumentState {
    pub document_id: String,
    pub revision_id: String,
    pub version: i64,
    pub lifecycle: String,
    pub current_approved: bool,
    pub content_digest: String,
    pub observed_at: String,
}

/// Deterministic readiness result.  `readiness_digest` is a candidate token,
/// not a capability; the release transaction must recompute it from fresh
/// inputs and compare it with the stored snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessEvaluation {
    pub project_id: String,
    pub milestone_id: String,
    pub milestone_definition_revision_id: String,
    pub ordered_input_manifest: Vec<ReadinessInput>,
    pub source_event_watermark: String,
    pub result: ReadinessResult,
    pub reasons: Vec<ReadinessReason>,
    pub ordered_check_results: Vec<ValidationResult>,
    pub waiver_ids: Vec<String>,
    pub evidence_attachment_ids: Vec<String>,
    pub evidence_digests: Vec<String>,
    pub evidence_availability: Vec<EvidenceAvailability>,
    pub commit_build_check_context: Vec<String>,
    pub computing_policy_revision: String,
    pub readiness_digest: String,
    pub expected_milestone_version: i64,
    pub requesting_principal: PrincipalRef,
    pub authorization: AuthorizationProvenance,
}

impl ReadinessEvaluation {
    /// Materialize the immutable candidate record.  This only creates a value;
    /// persistence and the active-milestone transition belong to the caller.
    #[must_use]
    pub fn into_snapshot(self, snapshot_id: String, computed_at: String) -> ReadinessSnapshot {
        ReadinessSnapshot {
            id: snapshot_id,
            project_id: self.project_id,
            milestone_id: self.milestone_id,
            expected_milestone_version: self.expected_milestone_version,
            milestone_definition_revision_id: self.milestone_definition_revision_id,
            input_manifest: self.ordered_input_manifest,
            source_event_watermark: self.source_event_watermark,
            result: self.result,
            reasons: self.reasons,
            check_results: self.ordered_check_results,
            waiver_ids: self.waiver_ids,
            evidence_attachment_ids: self.evidence_attachment_ids,
            evidence_digests: self.evidence_digests,
            evidence_availability: self.evidence_availability,
            commit_build_check_context: self.commit_build_check_context,
            computing_policy_revision: self.computing_policy_revision,
            readiness_digest: self.readiness_digest,
            computed_at,
            requesting_principal: self.requesting_principal,
            authorization: self.authorization,
        }
    }
}

/// Evaluate exact checks and evidence into a release candidate or typed
/// non-ready result.  No lifecycle mutation or release pin is performed.
pub fn evaluate_readiness(
    input: ReadinessEvaluationInput,
) -> Result<ReadinessEvaluation, MilestoneOrchestrationError> {
    if input.milestone.project_id != input.definition.project_id
        || input.milestone.id != input.definition.milestone_id
    {
        return Err(MilestoneOrchestrationError::ReadinessTargetMismatch);
    }

    if !matches!(
        input.milestone.lifecycle,
        MilestoneLifecycle::Active
            | MilestoneLifecycle::ReadyForRelease
            | MilestoneLifecycle::Released
    ) {
        return Err(MilestoneOrchestrationError::ReadinessLifecycleNotEligible {
            lifecycle: input.milestone.lifecycle,
        });
    }

    // The repository must authorize these records before constructing the
    // input.  Retain a second pure boundary here so a malformed caller cannot
    // accidentally digest or expose another Project's metadata.
    if input
        .check_results
        .iter()
        .any(|result| result.project_id != input.milestone.project_id)
        || input.evidence.iter().any(|attachment| {
            attachment.project_id != input.milestone.project_id
                || attachment
                    .milestone_id
                    .as_deref()
                    .is_some_and(|milestone_id| milestone_id != input.milestone.id)
        })
    {
        return Err(MilestoneOrchestrationError::ReadinessTargetMismatch);
    }

    let ordered_input_manifest = ordered_inputs(input.input_manifest);
    let ordered_check_results = ordered_validation_results(input.check_results);
    let ordered_evidence = ordered_evidence(input.evidence);
    let mut waiver_ids = input.waiver_ids;
    waiver_ids.sort();
    let mut commit_build_check_context = input.commit_build_check_context;
    commit_build_check_context.sort();

    let mut reasons = input.definition_contract_reasons;
    let mut has_failed = false;
    let mut has_stale = false;
    let mut has_blocked = !reasons.is_empty();

    for task in &input.task_states {
        if task.state != "done" {
            has_blocked = true;
            reasons.push(ReadinessReason {
                code: "task_not_done".to_owned(),
                message: format!("referenced Task {} is not done", task.task_id),
                blocking: true,
                check_id: None,
                source_ids: vec![task.task_id.clone()],
            });
        }
    }

    for document in &input.document_states {
        if document.lifecycle != "approved" || !document.current_approved {
            has_stale = true;
            reasons.push(ReadinessReason {
                code: "document_not_approved".to_owned(),
                message: format!(
                    "referenced Document revision {} is not the current approved revision",
                    document.revision_id
                ),
                blocking: true,
                check_id: None,
                source_ids: vec![document.document_id.clone(), document.revision_id.clone()],
            });
        }
    }

    if input.definition.lifecycle != MilestoneDefinitionLifecycle::Approved {
        has_blocked = true;
        reasons.push(ReadinessReason {
            code: "definition_not_approved".to_owned(),
            message: "the milestone definition revision is not approved".to_owned(),
            blocking: true,
            check_id: None,
            source_ids: vec![input.definition.id.clone()],
        });
    }

    if input.definition.content.charter_revision.is_none() {
        has_blocked = true;
        reasons.push(ReadinessReason {
            code: "charter_missing".to_owned(),
            message: "the milestone has no approved Project Charter revision".to_owned(),
            blocking: true,
            check_id: None,
            source_ids: vec![input.definition.id.clone()],
        });
    }

    if input.computing_policy_revision.is_empty() {
        has_stale = true;
        reasons.push(ReadinessReason {
            code: "policy_reference_missing".to_owned(),
            message: "the computing policy reference is required".to_owned(),
            blocking: true,
            check_id: None,
            source_ids: Vec::new(),
        });
    }

    evaluate_required_checks(
        &input.definition.content.acceptance_checks,
        &ordered_check_results,
        &waiver_ids,
        &mut reasons,
        &mut has_failed,
        &mut has_stale,
        &mut has_blocked,
    );
    let evidence_freshness = EvidenceFreshnessContext {
        current_definition_revision_id: &input.definition.id,
        task_states: &input.task_states,
        check_results: &ordered_check_results,
        commit_build_check_context: &commit_build_check_context,
    };
    evaluate_evidence_requirements(
        &input.definition.content.evidence_requirements,
        &ordered_evidence,
        &evidence_freshness,
        &mut reasons,
        &mut has_stale,
        &mut has_blocked,
    );

    reasons.sort_by(|left, right| {
        (
            left.code.as_str(),
            left.check_id.as_deref().unwrap_or_default(),
            left.source_ids.as_slice(),
            left.message.as_str(),
        )
            .cmp(&(
                right.code.as_str(),
                right.check_id.as_deref().unwrap_or_default(),
                right.source_ids.as_slice(),
                right.message.as_str(),
            ))
    });

    let result = if has_failed {
        ReadinessResult::Failed
    } else if has_stale {
        ReadinessResult::Stale
    } else if has_blocked {
        ReadinessResult::Blocked
    } else {
        ReadinessResult::Ready
    };

    let evidence_attachment_ids = ordered_evidence
        .iter()
        .map(|evidence| evidence.id.clone())
        .collect::<Vec<_>>();
    let evidence_digests = ordered_evidence
        .iter()
        .map(|evidence| evidence.checksum.clone())
        .collect::<Vec<_>>();
    let evidence_availability = ordered_evidence
        .iter()
        .map(|evidence| evidence.availability)
        .collect::<Vec<_>>();

    let digest_payload = ReadinessDigestPayload {
        project_id: &input.milestone.project_id,
        milestone_id: &input.milestone.id,
        milestone_version: input.milestone.version,
        milestone_definition_revision_id: &input.definition.id,
        milestone_definition_digest: &input.definition.content_digest,
        source_event_watermark: &input.source_event_watermark,
        computing_policy_revision: &input.computing_policy_revision,
        input_manifest: &ordered_input_manifest,
        check_results: &ordered_check_results,
        evidence: &ordered_evidence,
        waiver_ids: &waiver_ids,
        task_states: &input.task_states,
        document_states: &input.document_states,
        commit_build_check_context: &commit_build_check_context,
        result,
        reasons: &reasons,
        authorization: &input.authorization,
    };
    let readiness_digest =
        canonical_digest_with_schema(MILESTONE_READINESS_DIGEST_SCHEMA_VERSION, &digest_payload)
            .map_err(|error| MilestoneOrchestrationError::Digest {
                message: error.to_string(),
            })?;

    Ok(ReadinessEvaluation {
        project_id: input.milestone.project_id,
        milestone_id: input.milestone.id,
        milestone_definition_revision_id: input.definition.id,
        ordered_input_manifest,
        source_event_watermark: input.source_event_watermark,
        result,
        reasons,
        ordered_check_results,
        waiver_ids,
        evidence_attachment_ids,
        evidence_digests,
        evidence_availability,
        commit_build_check_context,
        computing_policy_revision: input.computing_policy_revision,
        readiness_digest,
        expected_milestone_version: input.milestone.version,
        requesting_principal: input.authorization.principal.clone(),
        authorization: input.authorization,
    })
}

fn evaluate_required_checks(
    definitions: &[MilestoneAcceptanceCheck],
    results: &[ValidationResult],
    waiver_ids: &[String],
    reasons: &mut Vec<ReadinessReason>,
    has_failed: &mut bool,
    has_stale: &mut bool,
    has_blocked: &mut bool,
) {
    for definition in definitions.iter().filter(|check| check.required) {
        if waiver_ids.iter().any(|id| id == &definition.id) {
            continue;
        }
        let latest = results
            .iter()
            .filter(|result| result.check_id == definition.id)
            .max_by_key(|result| (&result.evaluated_at, &result.id));

        let Some(result) = latest else {
            *has_blocked = true;
            reasons.push(ReadinessReason {
                code: "check_missing".to_owned(),
                message: format!(
                    "required check {} has no authoritative result",
                    definition.id
                ),
                blocking: true,
                check_id: Some(definition.id.clone()),
                source_ids: vec![definition.id.clone()],
            });
            continue;
        };

        match result.status {
            AcceptanceCheckResultStatus::Pass => {}
            AcceptanceCheckResultStatus::Waived
                if waiver_ids
                    .iter()
                    .any(|id| id == &result.id || id == &definition.id) => {}
            AcceptanceCheckResultStatus::Fail => {
                *has_failed = true;
                reasons.push(check_reason(
                    "check_failed",
                    format!("required check {} failed", definition.id),
                    definition,
                    result,
                ));
            }
            AcceptanceCheckResultStatus::Stale => {
                *has_stale = true;
                reasons.push(check_reason(
                    "check_stale",
                    format!("required check {} is stale", definition.id),
                    definition,
                    result,
                ));
            }
            AcceptanceCheckResultStatus::Pending | AcceptanceCheckResultStatus::Blocked => {
                *has_blocked = true;
                reasons.push(check_reason(
                    "check_blocked",
                    format!("required check {} is not complete", definition.id),
                    definition,
                    result,
                ));
            }
            AcceptanceCheckResultStatus::Unavailable => {
                *has_blocked = true;
                reasons.push(check_reason(
                    "check_unavailable",
                    format!("required check {} is unavailable", definition.id),
                    definition,
                    result,
                ));
            }
            AcceptanceCheckResultStatus::Waived => {
                *has_blocked = true;
                reasons.push(check_reason(
                    "waiver_missing",
                    format!("required check {} has no authorized waiver", definition.id),
                    definition,
                    result,
                ));
            }
        }
    }

    // Optional checks remain visible without making a candidate non-ready.
    for definition in definitions.iter().filter(|check| !check.required) {
        let Some(result) = results
            .iter()
            .filter(|result| result.check_id == definition.id)
            .max_by_key(|result| (&result.evaluated_at, &result.id))
        else {
            continue;
        };
        if !matches!(result.status, AcceptanceCheckResultStatus::Pass) {
            reasons.push(ReadinessReason {
                code: "optional_check_not_passing".to_owned(),
                message: format!("optional check {} is not passing", definition.id),
                blocking: false,
                check_id: Some(definition.id.clone()),
                source_ids: vec![result.id.clone()],
            });
        }
    }
}

fn check_reason(
    code: &'static str,
    message: String,
    definition: &MilestoneAcceptanceCheck,
    result: &ValidationResult,
) -> ReadinessReason {
    ReadinessReason {
        code: code.to_owned(),
        message,
        blocking: true,
        check_id: Some(definition.id.clone()),
        source_ids: vec![result.id.clone(), result.input_digest.clone()],
    }
}

struct EvidenceFreshnessContext<'a> {
    current_definition_revision_id: &'a str,
    task_states: &'a [ReadinessTaskState],
    check_results: &'a [ValidationResult],
    commit_build_check_context: &'a [String],
}

fn evaluate_evidence_requirements(
    requirements: &[AcceptanceEvidenceRequirement],
    evidence: &[EvidenceAttachment],
    freshness: &EvidenceFreshnessContext<'_>,
    reasons: &mut Vec<ReadinessReason>,
    has_stale: &mut bool,
    has_blocked: &mut bool,
) {
    for requirement in requirements
        .iter()
        .filter(|requirement| requirement.required)
    {
        let supporting = evidence.iter().filter(|attachment| {
            !attachment.caption.trim().is_empty()
                && attachment
                    .acceptance_check_ids
                    .iter()
                    .any(|check_id| check_id == &requirement.id)
                && requirement
                    .evidence_kind
                    .as_deref()
                    .is_none_or(|kind| evidence_kind_matches(attachment.kind, kind))
        });
        let supporting = supporting.collect::<Vec<_>>();

        if supporting.is_empty() {
            *has_blocked = true;
            reasons.push(ReadinessReason {
                code: "evidence_missing".to_owned(),
                message: format!(
                    "required evidence {} is missing or not relevant",
                    requirement.id
                ),
                blocking: true,
                check_id: Some(requirement.id.clone()),
                source_ids: Vec::new(),
            });
            continue;
        }

        let expected_definition_revision_id = requirement
            .check_definition_revision
            .as_deref()
            .unwrap_or(freshness.current_definition_revision_id);
        let (fresh, stale): (Vec<_>, Vec<_>) = supporting.into_iter().partition(|attachment| {
            evidence_context_state(
                attachment,
                expected_definition_revision_id,
                freshness.task_states,
                freshness.check_results,
                freshness.commit_build_check_context,
            ) == EvidenceContextState::Fresh
        });
        if fresh.is_empty() {
            *has_stale = true;
            let missing = stale
                .iter()
                .filter(|attachment| {
                    evidence_context_state(
                        attachment,
                        expected_definition_revision_id,
                        freshness.task_states,
                        freshness.check_results,
                        freshness.commit_build_check_context,
                    ) == EvidenceContextState::Missing
                })
                .map(|attachment| attachment.id.clone())
                .collect::<Vec<_>>();
            let mismatched = stale
                .iter()
                .filter(|attachment| {
                    evidence_context_state(
                        attachment,
                        expected_definition_revision_id,
                        freshness.task_states,
                        freshness.check_results,
                        freshness.commit_build_check_context,
                    ) == EvidenceContextState::Stale
                })
                .map(|attachment| attachment.id.clone())
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                reasons.push(ReadinessReason {
                    code: "evidence_context_missing".to_owned(),
                    message: format!(
                        "required evidence {} is missing its exact source context",
                        requirement.id
                    ),
                    blocking: true,
                    check_id: Some(requirement.id.clone()),
                    source_ids: missing,
                });
            }
            if !mismatched.is_empty() {
                reasons.push(ReadinessReason {
                    code: "evidence_context_stale".to_owned(),
                    message: format!(
                        "required evidence {} no longer matches its exact source context",
                        requirement.id
                    ),
                    blocking: true,
                    check_id: Some(requirement.id.clone()),
                    source_ids: mismatched,
                });
            }
            continue;
        }

        let unavailable = fresh
            .iter()
            .filter(|attachment| attachment.availability != EvidenceAvailability::Available)
            .collect::<Vec<_>>();
        if !unavailable.is_empty() {
            *has_blocked = true;
            reasons.push(ReadinessReason {
                code: "evidence_unavailable".to_owned(),
                message: format!("required evidence {} is not available", requirement.id),
                blocking: true,
                check_id: Some(requirement.id.clone()),
                source_ids: unavailable
                    .iter()
                    .map(|attachment| attachment.id.clone())
                    .collect(),
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceContextState {
    Fresh,
    Missing,
    Stale,
}

fn evidence_context_state(
    attachment: &EvidenceAttachment,
    expected_definition_revision_id: &str,
    task_states: &[ReadinessTaskState],
    check_results: &[ValidationResult],
    commit_build_check_context: &[String],
) -> EvidenceContextState {
    let Some(source_definition_revision_id) = attachment.source_definition_revision_id.as_deref()
    else {
        return EvidenceContextState::Missing;
    };
    if source_definition_revision_id != expected_definition_revision_id {
        return EvidenceContextState::Stale;
    }

    if let Some(source_validation_id) = attachment.source_validation_id.as_deref() {
        if !check_results
            .iter()
            .any(|result| result.id == source_validation_id)
        {
            return EvidenceContextState::Stale;
        }
    }

    let source_task_id = attachment
        .source_task_id
        .as_deref()
        .or(attachment.task_id.as_deref());
    let Some(source_task_id) = source_task_id else {
        return if attachment.source_task_version.is_none()
            && attachment.source_context_digest.is_none()
            && attachment.source_run_id.is_none()
            && attachment.source_validation_id.is_none()
        {
            EvidenceContextState::Fresh
        } else if attachment.source_context_digest.is_none() {
            EvidenceContextState::Missing
        } else {
            EvidenceContextState::Stale
        };
    };
    let Some(task) = task_states
        .iter()
        .find(|task| task.task_id == source_task_id)
    else {
        return EvidenceContextState::Stale;
    };
    if attachment.source_task_version.is_none() || attachment.source_context_digest.is_none() {
        return EvidenceContextState::Missing;
    }
    if attachment.source_task_version != Some(task.version) {
        return EvidenceContextState::Stale;
    }

    let Some(source_context_digest) = attachment.source_context_digest.as_deref() else {
        return EvidenceContextState::Missing;
    };
    let context = commit_build_check_context.iter().find(|context| {
        let parsed = serde_json::from_str::<serde_json::Value>(context).ok();
        let context_task_id = parsed
            .as_ref()
            .and_then(|value| value.get("task_id"))
            .and_then(serde_json::Value::as_str);
        let context_execution_id = parsed
            .as_ref()
            .and_then(|value| value.get("execution_id"))
            .and_then(serde_json::Value::as_str);
        if let Some(source_run_id) = attachment.source_run_id.as_deref() {
            context_execution_id == Some(source_run_id)
        } else {
            context_task_id == Some(source_task_id)
        }
    });
    let context = context.or_else(|| {
        (attachment.source_run_id.is_none() && commit_build_check_context.len() == 1)
            .then(|| &commit_build_check_context[0])
    });
    let Some(context) = context else {
        return EvidenceContextState::Stale;
    };
    let expected_digest = serde_json::from_str::<serde_json::Value>(context)
        .ok()
        .and_then(|value| {
            canonical_digest_with_schema(MILESTONE_READINESS_DIGEST_SCHEMA_VERSION, &value).ok()
        })
        .or_else(|| {
            canonical_digest_with_schema(MILESTONE_READINESS_DIGEST_SCHEMA_VERSION, context).ok()
        });
    if expected_digest.as_deref() == Some(source_context_digest) {
        EvidenceContextState::Fresh
    } else {
        EvidenceContextState::Stale
    }
}

fn evidence_kind_matches(kind: EvidenceKind, expected: &str) -> bool {
    let actual = match kind {
        EvidenceKind::Screenshot => "screenshot",
        EvidenceKind::WalkthroughVideo => "walkthrough_video",
        EvidenceKind::Log => "log",
        EvidenceKind::Report => "report",
        EvidenceKind::Other => "other",
    };
    actual == expected
}

fn ordered_inputs(mut inputs: Vec<ReadinessInput>) -> Vec<ReadinessInput> {
    inputs.sort_by(|left, right| {
        (
            left.source_kind.as_str(),
            left.source_id.as_str(),
            left.source_version,
            left.source_digest.as_str(),
            left.observed_at.as_str(),
        )
            .cmp(&(
                right.source_kind.as_str(),
                right.source_id.as_str(),
                right.source_version,
                right.source_digest.as_str(),
                right.observed_at.as_str(),
            ))
    });
    inputs
}

fn ordered_validation_results(mut results: Vec<ValidationResult>) -> Vec<ValidationResult> {
    results.sort_by(|left, right| {
        (
            left.check_id.as_str(),
            left.evaluated_at.as_str(),
            left.id.as_str(),
            left.result_digest.as_str(),
        )
            .cmp(&(
                right.check_id.as_str(),
                right.evaluated_at.as_str(),
                right.id.as_str(),
                right.result_digest.as_str(),
            ))
    });
    results
}

fn ordered_evidence(mut evidence: Vec<EvidenceAttachment>) -> Vec<EvidenceAttachment> {
    evidence.sort_by(|left, right| {
        (
            left.id.as_str(),
            left.version,
            left.checksum.as_str(),
            left.captured_at.as_str(),
        )
            .cmp(&(
                right.id.as_str(),
                right.version,
                right.checksum.as_str(),
                right.captured_at.as_str(),
            ))
    });
    evidence
}

#[derive(Serialize)]
struct ReadinessDigestPayload<'a> {
    project_id: &'a str,
    milestone_id: &'a str,
    milestone_version: i64,
    milestone_definition_revision_id: &'a str,
    milestone_definition_digest: &'a str,
    source_event_watermark: &'a str,
    computing_policy_revision: &'a str,
    input_manifest: &'a [ReadinessInput],
    check_results: &'a [ValidationResult],
    evidence: &'a [EvidenceAttachment],
    waiver_ids: &'a [String],
    task_states: &'a [ReadinessTaskState],
    document_states: &'a [ReadinessDocumentState],
    commit_build_check_context: &'a [String],
    result: ReadinessResult,
    reasons: &'a [ReadinessReason],
    authorization: &'a AuthorizationProvenance,
}

/// Verify a named readiness candidate against a freshly recomputed result and
/// return the immutable identity that the repository may write atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseCandidateVerification {
    pub release_identity: String,
    pub readiness_digest: String,
    pub release_revision: i64,
}

#[allow(clippy::too_many_arguments)]
pub fn verify_release_candidate(
    milestone: &ProjectMilestone,
    candidate: &ReadinessSnapshot,
    recomputed: &ReadinessEvaluation,
    requested_snapshot_id: &str,
    requested_readiness_digest: &str,
    release_revision: i64,
    actor: &PrincipalRef,
    project_agent: &PrincipalRef,
) -> Result<ReleaseCandidateVerification, MilestoneOrchestrationError> {
    validate_release_actor(actor, project_agent)?;

    if !matches!(
        milestone.lifecycle,
        MilestoneLifecycle::ReadyForRelease | MilestoneLifecycle::Released
    ) {
        return Err(MilestoneOrchestrationError::ReleaseRevisionNotAllowed {
            revision: release_revision,
            lifecycle: milestone.lifecycle,
        });
    }
    if release_revision <= 0 {
        return Err(MilestoneOrchestrationError::InvalidReleaseRevision {
            revision: release_revision,
        });
    }
    if milestone.lifecycle == MilestoneLifecycle::ReadyForRelease && release_revision != 1 {
        return Err(MilestoneOrchestrationError::ReleaseRevisionNotAllowed {
            revision: release_revision,
            lifecycle: milestone.lifecycle,
        });
    }
    if milestone.lifecycle == MilestoneLifecycle::Released && release_revision < 2 {
        return Err(MilestoneOrchestrationError::ReleaseRevisionNotAllowed {
            revision: release_revision,
            lifecycle: milestone.lifecycle,
        });
    }

    if candidate.result != ReadinessResult::Ready || recomputed.result != ReadinessResult::Ready {
        return Err(MilestoneOrchestrationError::ReleaseCandidateNotReady);
    }
    if candidate.id != requested_snapshot_id {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "readiness_snapshot_id",
        });
    }
    if candidate.readiness_digest != requested_readiness_digest {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "readiness_digest_request",
        });
    }
    if candidate.readiness_digest != recomputed.readiness_digest {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "readiness_digest_recomputed",
        });
    }
    if candidate.project_id != milestone.project_id || candidate.milestone_id != milestone.id {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "project_or_milestone",
        });
    }
    if candidate.expected_milestone_version != recomputed.expected_milestone_version
        || candidate.requesting_principal != recomputed.requesting_principal
        || candidate.authorization != recomputed.authorization
        || candidate.source_event_watermark != recomputed.source_event_watermark
        || candidate.input_manifest != recomputed.ordered_input_manifest
        || candidate.check_results != recomputed.ordered_check_results
        || candidate.waiver_ids != recomputed.waiver_ids
        || candidate.evidence_attachment_ids != recomputed.evidence_attachment_ids
        || candidate.evidence_digests != recomputed.evidence_digests
        || candidate.evidence_availability != recomputed.evidence_availability
        || candidate.commit_build_check_context != recomputed.commit_build_check_context
        || candidate.computing_policy_revision != recomputed.computing_policy_revision
    {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "readiness_snapshot_contents",
        });
    }
    if candidate.milestone_definition_revision_id != recomputed.milestone_definition_revision_id {
        return Err(MilestoneOrchestrationError::ReleaseCandidateMismatch {
            field: "readiness_source_references",
        });
    }

    Ok(ReleaseCandidateVerification {
        release_identity: release_identity(milestone.milestone_sequence, release_revision)?,
        readiness_digest: recomputed.readiness_digest.clone(),
        release_revision,
    })
}

/// Render the stable Project-local milestone identity (`M001`, `M002`, ...).
pub fn milestone_identity(sequence: i64) -> Result<String, MilestoneOrchestrationError> {
    if sequence <= 0 {
        return Err(MilestoneOrchestrationError::InvalidMilestoneSequence { sequence });
    }
    Ok(format!("M{sequence:03}"))
}

/// Render the stable immutable release identity (`M001-r1`, ...).
pub fn release_identity(
    sequence: i64,
    revision: i64,
) -> Result<String, MilestoneOrchestrationError> {
    if revision <= 0 {
        return Err(MilestoneOrchestrationError::InvalidReleaseRevision { revision });
    }
    Ok(format!("{}-r{revision}", milestone_identity(sequence)?))
}

/// Compute the immutable whole-snapshot digest.  The digest field itself is
/// excluded from the payload to avoid a recursive hash.
pub fn release_snapshot_digest(
    snapshot: &ReleaseSnapshot,
) -> Result<String, MilestoneOrchestrationError> {
    let mut value =
        serde_json::to_value(snapshot).map_err(|error| MilestoneOrchestrationError::Digest {
            message: error.to_string(),
        })?;
    if let serde_json::Value::Object(object) = &mut value {
        object.remove("snapshot_digest");
    }
    canonical_digest_with_schema(MILESTONE_RELEASE_DIGEST_SCHEMA_VERSION, &value).map_err(|error| {
        MilestoneOrchestrationError::Digest {
            message: error.to_string(),
        }
    })
}

/// Alias named for release-transaction call sites which intentionally
/// recompute the candidate digest immediately before writing a manifest.
pub fn recompute_readiness_digest(
    input: ReadinessEvaluationInput,
) -> Result<String, MilestoneOrchestrationError> {
    Ok(evaluate_readiness(input)?.readiness_digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{
        ArtifactRef, AuthorizationProvenance, CharterRisk, MilestoneDefinitionContent,
        PrincipalKind, ProvenanceRef, RevisionProvenance,
    };

    fn principal(kind: PrincipalKind, id: &str) -> PrincipalRef {
        PrincipalRef {
            kind,
            id: id.to_owned(),
            display_name: None,
        }
    }

    fn milestone(lifecycle: MilestoneLifecycle) -> ProjectMilestone {
        ProjectMilestone {
            id: "milestone-1".to_owned(),
            project_id: "project-1".to_owned(),
            milestone_sequence: 1,
            canonical_id: "M001".to_owned(),
            display_label: Some("M1 — Deliver outcome".to_owned()),
            definition_revision_id: "definition-1".to_owned(),
            lifecycle,
            projection_reasons: Vec::new(),
            version: 1,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            updated_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    fn check(id: &str, required: bool) -> MilestoneAcceptanceCheck {
        MilestoneAcceptanceCheck {
            id: id.to_owned(),
            description: format!("check {id}"),
            required,
            source_kind: api_types::AcceptanceCheckSourceKind::Manual,
            expected_result: "pass".to_owned(),
            latest_result: None,
            latest_result_id: None,
            latest_result_digest: None,
        }
    }

    fn definition(
        lifecycle: MilestoneDefinitionLifecycle,
        checks: Vec<MilestoneAcceptanceCheck>,
        evidence_requirements: Vec<AcceptanceEvidenceRequirement>,
    ) -> MilestoneDefinitionRevision {
        MilestoneDefinitionRevision {
            id: "definition-1".to_owned(),
            milestone_id: "milestone-1".to_owned(),
            project_id: "project-1".to_owned(),
            revision_number: 1,
            base_revision_id: None,
            lifecycle,
            schema_version: "forge.milestone/v1".to_owned(),
            content: MilestoneDefinitionContent {
                name: "Outcome".to_owned(),
                outcome: "A useful outcome".to_owned(),
                included_scope: vec!["in".to_owned()],
                excluded_scope: vec!["out".to_owned()],
                charter_revision: Some(ArtifactRef {
                    artifact_id: "charter".to_owned(),
                    revision_id: "charter-r1".to_owned(),
                    content_digest: "charter-digest".to_owned(),
                    render_version: None,
                    render_digest: None,
                }),
                document_revisions: Vec::new(),
                task_ids: vec!["task-1".to_owned()],
                dependencies: Vec::new(),
                risks: vec![CharterRisk {
                    id: "risk-1".to_owned(),
                    description: "risk".to_owned(),
                    impact: None,
                    treatment: None,
                    revisit_trigger: None,
                    owner: None,
                }],
                acceptance_checks: checks,
                evidence_requirements,
                known_issues: Vec::new(),
                target_date: None,
            },
            rendered_view: "Outcome".to_owned(),
            render_version: "v1".to_owned(),
            content_digest: "definition-digest".to_owned(),
            render_digest: "render-digest".to_owned(),
            provenance: RevisionProvenance {
                author: principal(PrincipalKind::User, "user-1"),
                profile_revision: None,
                operating_skill_revision: None,
                source_refs: Vec::<ProvenanceRef>::new(),
                change_summary: "initial".to_owned(),
                material_diff: None,
            },
            created_at: "2026-08-13T00:00:00Z".to_owned(),
        }
    }

    fn validation(
        id: &str,
        check_id: &str,
        status: AcceptanceCheckResultStatus,
        evaluated_at: &str,
    ) -> ValidationResult {
        let actor = principal(PrincipalKind::Reviewer, "reviewer-1");
        ValidationResult {
            id: id.to_owned(),
            project_id: "project-1".to_owned(),
            check_id: check_id.to_owned(),
            status,
            result: format!("{status:?}"),
            principal: actor.clone(),
            authorization: AuthorizationProvenance {
                principal: actor,
                authorization_basis: "assigned".to_owned(),
                action: "validate".to_owned(),
                event_id: format!("event-{id}"),
                occurred_at: evaluated_at.to_owned(),
            },
            input_digest: format!("input-{id}"),
            governing_revision_ids: vec!["baseline-r1".to_owned()],
            expected_version: 1,
            event_id: format!("event-{id}"),
            evaluated_at: evaluated_at.to_owned(),
            result_digest: format!("result-{id}"),
        }
    }

    fn evidence(id: &str, availability: EvidenceAvailability) -> EvidenceAttachment {
        EvidenceAttachment {
            id: id.to_owned(),
            project_id: "project-1".to_owned(),
            asset_id: format!("asset-{id}"),
            task_id: Some("task-1".to_owned()),
            source_task_id: Some("task-1".to_owned()),
            source_run_id: None,
            source_validation_id: None,
            source_task_version: Some(2),
            source_context_digest: Some(
                canonical_digest_with_schema(
                    MILESTONE_READINESS_DIGEST_SCHEMA_VERSION,
                    &"commit:abc",
                )
                .expect("evidence context digest"),
            ),
            source_definition_revision_id: Some("definition-1".to_owned()),
            milestone_id: Some("milestone-1".to_owned()),
            acceptance_check_ids: vec!["evidence-1".to_owned()],
            caption: "A useful proof".to_owned(),
            kind: EvidenceKind::Screenshot,
            checksum: format!("checksum-{id}"),
            availability,
            author: principal(PrincipalKind::Worker, "worker-1"),
            captured_at: "2026-08-13T00:00:00Z".to_owned(),
            version: 1,
            created_at: "2026-08-13T00:00:00Z".to_owned(),
            removed_at: None,
        }
    }

    fn readiness_input(
        checks: Vec<MilestoneAcceptanceCheck>,
        results: Vec<ValidationResult>,
        evidence: Vec<EvidenceAttachment>,
    ) -> ReadinessEvaluationInput {
        ReadinessEvaluationInput {
            milestone: milestone(MilestoneLifecycle::Active),
            definition: definition(
                MilestoneDefinitionLifecycle::Approved,
                checks,
                if evidence.is_empty() {
                    Vec::new()
                } else {
                    vec![AcceptanceEvidenceRequirement {
                        id: "evidence-1".to_owned(),
                        description: "proof".to_owned(),
                        required: true,
                        evidence_kind: Some("screenshot".to_owned()),
                        check_definition_revision: None,
                    }]
                },
            ),
            source_event_watermark: "event-10".to_owned(),
            computing_policy_revision: "compute-r1".to_owned(),
            input_manifest: vec![
                ReadinessInput {
                    source_kind: "task".to_owned(),
                    source_id: "task-1".to_owned(),
                    source_version: 2,
                    source_digest: "task-digest".to_owned(),
                    observed_at: "2026-08-13T00:00:00Z".to_owned(),
                },
                ReadinessInput {
                    source_kind: "document".to_owned(),
                    source_id: "doc-1".to_owned(),
                    source_version: 1,
                    source_digest: "doc-digest".to_owned(),
                    observed_at: "2026-08-13T00:00:00Z".to_owned(),
                },
            ],
            check_results: results,
            evidence,
            waiver_ids: Vec::new(),
            task_states: vec![ReadinessTaskState {
                task_id: "task-1".to_owned(),
                version: 2,
                task_type: "task".to_owned(),
                state: "done".to_owned(),
                observed_at: "2026-08-13T00:00:00Z".to_owned(),
            }],
            document_states: vec![ReadinessDocumentState {
                document_id: "doc-1".to_owned(),
                revision_id: "doc-r1".to_owned(),
                version: 1,
                lifecycle: "approved".to_owned(),
                current_approved: true,
                content_digest: "doc-digest".to_owned(),
                observed_at: "2026-08-13T00:00:00Z".to_owned(),
            }],
            commit_build_check_context: vec!["commit:abc".to_owned()],
            definition_contract_reasons: Vec::new(),
            authorization: AuthorizationProvenance {
                principal: principal(PrincipalKind::User, "user-1"),
                authorization_basis: "release-review".to_owned(),
                action: "project.milestone.readiness".to_owned(),
                event_id: "readiness-event".to_owned(),
                occurred_at: "2026-08-13T00:00:00Z".to_owned(),
            },
        }
    }

    #[test]
    fn milestone_lifecycle_has_explicit_edges_and_released_is_terminal() {
        assert!(validate_milestone_transition(
            MilestoneLifecycle::Planned,
            MilestoneLifecycle::Active
        )
        .is_ok());
        assert!(validate_milestone_transition(
            MilestoneLifecycle::Planned,
            MilestoneLifecycle::Cancelled
        )
        .is_ok());
        assert!(validate_milestone_transition(
            MilestoneLifecycle::Active,
            MilestoneLifecycle::ReadyForRelease
        )
        .is_ok());
        assert!(validate_milestone_transition(
            MilestoneLifecycle::ReadyForRelease,
            MilestoneLifecycle::Active
        )
        .is_ok());
        assert!(validate_milestone_transition(
            MilestoneLifecycle::ReadyForRelease,
            MilestoneLifecycle::Released
        )
        .is_ok());
        assert_eq!(
            validate_milestone_transition(MilestoneLifecycle::Released, MilestoneLifecycle::Active),
            Err(MilestoneOrchestrationError::ReleasedTerminal)
        );
        assert!(validate_milestone_transition(
            MilestoneLifecycle::Released,
            MilestoneLifecycle::Released
        )
        .is_ok());
    }

    #[test]
    fn definition_lifecycle_is_separate_and_append_only() {
        assert!(validate_definition_transition(
            MilestoneDefinitionLifecycle::Draft,
            MilestoneDefinitionLifecycle::Proposed
        )
        .is_ok());
        assert!(validate_definition_transition(
            MilestoneDefinitionLifecycle::Proposed,
            MilestoneDefinitionLifecycle::Approved
        )
        .is_ok());
        assert!(validate_definition_transition(
            MilestoneDefinitionLifecycle::Approved,
            MilestoneDefinitionLifecycle::Superseded
        )
        .is_ok());
        assert!(validate_definition_transition(
            MilestoneDefinitionLifecycle::Superseded,
            MilestoneDefinitionLifecycle::Approved
        )
        .is_err());
    }

    #[test]
    fn multiple_active_milestones_require_an_explicit_active_primary() {
        let mut second = milestone(MilestoneLifecycle::Active);
        second.id = "milestone-2".to_owned();
        second.milestone_sequence = 2;
        second.canonical_id = "M002".to_owned();
        assert_eq!(
            validate_primary_milestone(
                "project-1",
                &[milestone(MilestoneLifecycle::Active), second.clone()],
                None
            ),
            Err(MilestoneOrchestrationError::PrimaryMilestoneRequired {
                project_id: "project-1".to_owned()
            })
        );
        assert!(validate_primary_milestone(
            "project-1",
            &[milestone(MilestoneLifecycle::Active), second],
            Some("milestone-1")
        )
        .is_ok());
        assert!(validate_primary_milestone(
            "project-1",
            &[milestone(MilestoneLifecycle::Planned)],
            None
        )
        .is_ok());
        assert!(validate_primary_milestone(
            "project-1",
            &[milestone(MilestoneLifecycle::Released)],
            Some("milestone-1")
        )
        .is_ok());
        let mut next = milestone(MilestoneLifecycle::Active);
        next.id = "milestone-2".to_owned();
        assert!(validate_primary_milestone(
            "project-1",
            &[milestone(MilestoneLifecycle::Released), next],
            Some("milestone-1")
        )
        .is_ok());
        assert!(validate_primary_milestone(
            "project-1",
            &[milestone(MilestoneLifecycle::Cancelled)],
            Some("milestone-1")
        )
        .is_err());
    }

    #[test]
    fn readiness_is_ready_only_for_authoritative_passing_check_and_relevant_evidence() {
        let input = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        let result = evaluate_readiness(input).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Ready);
        assert!(result.reasons.is_empty());
        assert_eq!(result.evidence_attachment_ids, vec!["evidence-asset-1"]);
        assert_eq!(
            result.evidence_availability,
            vec![EvidenceAvailability::Available]
        );
        assert_eq!(result.readiness_digest.len(), 64);
    }

    #[test]
    fn required_evidence_without_exact_context_is_stale() {
        let mut input = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        input.evidence[0].source_context_digest = None;
        let result = evaluate_readiness(input).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Stale);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.code == "evidence_context_missing"));
    }

    #[test]
    fn required_evidence_becomes_stale_when_task_or_context_changes() {
        let mut task_changed = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        task_changed.task_states[0].version += 1;
        let result = evaluate_readiness(task_changed).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Stale);

        let mut context_changed = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        context_changed.commit_build_check_context = vec!["commit:def".to_owned()];
        let result = evaluate_readiness(context_changed).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Stale);
    }

    #[test]
    fn readiness_distinguishes_failed_stale_and_blocked_inputs() {
        let failed = evaluate_readiness(readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Fail,
                "2026-08-13T00:00:01Z",
            )],
            Vec::new(),
        ))
        .expect("readiness computes");
        assert_eq!(failed.result, ReadinessResult::Failed);

        let stale = evaluate_readiness(readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Stale,
                "2026-08-13T00:00:01Z",
            )],
            Vec::new(),
        ))
        .expect("readiness computes");
        assert_eq!(stale.result, ReadinessResult::Stale);

        let blocked = evaluate_readiness(readiness_input(
            vec![check("check-1", true)],
            Vec::new(),
            Vec::new(),
        ))
        .expect("readiness computes");
        assert_eq!(blocked.result, ReadinessResult::Blocked);
    }

    #[test]
    fn terminal_tasks_do_not_imply_readiness() {
        let mut input = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        input.task_states[0].state = "review".to_owned();
        let result = evaluate_readiness(input).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Blocked);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.code == "task_not_done"));
    }

    #[test]
    fn non_current_document_revision_is_stale() {
        let mut input = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        input.document_states[0].current_approved = false;
        let result = evaluate_readiness(input).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Stale);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.code == "document_not_approved"));
    }

    #[test]
    fn readiness_digest_is_stable_when_unordered_inputs_are_reordered() {
        let mut left = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        let mut right = left.clone();
        right.input_manifest.reverse();
        right.check_results.reverse();
        right.evidence.reverse();
        left.input_manifest
            .sort_by(|a, b| a.source_id.cmp(&b.source_id));
        assert_eq!(
            evaluate_readiness(left).unwrap().readiness_digest,
            evaluate_readiness(right).unwrap().readiness_digest
        );
    }

    #[test]
    fn evidence_must_have_caption_and_acceptance_linkage() {
        let mut input = readiness_input(
            Vec::new(),
            Vec::new(),
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        input.evidence[0].caption.clear();
        let result = evaluate_readiness(input).expect("readiness computes");
        assert_eq!(result.result, ReadinessResult::Blocked);
        assert!(result
            .reasons
            .iter()
            .any(|reason| reason.code == "evidence_missing"));
    }

    #[test]
    fn principal_separation_uses_kind_and_id_and_release_requires_user() {
        let worker = principal(PrincipalKind::Worker, "same");
        let reviewer = principal(PrincipalKind::Reviewer, "same");
        assert!(!principals_equal(&worker, &reviewer));
        assert!(validate_independent_principal(PrincipalAction::Review, &worker, &worker).is_err());
        assert!(
            validate_independent_principal(PrincipalAction::Review, &reviewer, &worker).is_ok()
        );
        let agent = principal(PrincipalKind::Agent, "agent");
        assert!(validate_project_agent_action(PrincipalAction::Waive, &agent, &agent).is_err());
        assert_eq!(
            validate_release_actor(&agent, &agent),
            Err(MilestoneOrchestrationError::ReleaseRequiresUser)
        );
    }

    #[test]
    fn release_candidate_requires_fresh_matching_digest_and_user() {
        let mut input = readiness_input(
            vec![check("check-1", true)],
            vec![validation(
                "result-1",
                "check-1",
                AcceptanceCheckResultStatus::Pass,
                "2026-08-13T00:00:01Z",
            )],
            vec![evidence(
                "evidence-asset-1",
                EvidenceAvailability::Available,
            )],
        );
        let recomputed = evaluate_readiness(input.clone()).unwrap();
        let candidate = recomputed
            .clone()
            .into_snapshot("snapshot-1".to_owned(), "now".to_owned());
        let ready_milestone = candidate_as_ready(&candidate);
        let user = principal(PrincipalKind::User, "user-1");
        let agent = principal(PrincipalKind::Agent, "agent-1");
        let verified = verify_release_candidate(
            &ready_milestone,
            &candidate,
            &recomputed,
            "snapshot-1",
            &candidate.readiness_digest,
            1,
            &user,
            &agent,
        )
        .expect("candidate verifies");
        assert_eq!(verified.release_identity, "M001-r1");

        input.check_results[0].status = AcceptanceCheckResultStatus::Fail;
        let changed = evaluate_readiness(input).unwrap();
        assert!(verify_release_candidate(
            &candidate_as_ready(&candidate),
            &candidate,
            &changed,
            "snapshot-1",
            &candidate.readiness_digest,
            1,
            &user,
            &agent,
        )
        .is_err());
    }

    fn candidate_as_ready(_candidate: &ReadinessSnapshot) -> ProjectMilestone {
        milestone(MilestoneLifecycle::ReadyForRelease)
    }

    #[test]
    fn release_identity_and_snapshot_digest_are_deterministic() {
        assert_eq!(milestone_identity(1).unwrap(), "M001");
        assert_eq!(release_identity(1, 2).unwrap(), "M001-r2");
        assert_eq!(
            milestone_identity(0),
            Err(MilestoneOrchestrationError::InvalidMilestoneSequence { sequence: 0 })
        );
    }
}
