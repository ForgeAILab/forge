//! Persistence contracts for Charter-backed Project orchestration.
//!
//! This module deliberately keeps the database crate independent from the API
//! crate.  The API owns its closed wire enums and JSON contracts; the database
//! exposes stable records and mutation inputs containing the exact revision and
//! digest values which are persisted by SQLite.  Services are responsible for
//! converting between the two layers and for policy authorization.

use async_trait::async_trait;

use crate::{CreateProject, Project, Result};

/// A durable Project Charter (the identity record, not a revision).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCharterRecord {
    pub id: String,
    pub account_id: String,
    pub genesis_session_id: Option<String>,
    pub project_id: Option<String>,
    pub current_draft_revision_id: Option<String>,
    pub current_approved_revision_id: Option<String>,
    pub project_mode: String,
    pub maturity: String,
    pub lifecycle: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectCharter {
    pub id: String,
    pub account_id: String,
    pub genesis_session_id: Option<String>,
    pub project_mode: String,
    pub maturity: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCharterRevisionRecord {
    pub id: String,
    pub charter_id: String,
    pub revision: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub schema_version: String,
    pub render_version: String,
    pub content_json: String,
    pub rendered_view: String,
    pub change_summary: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub source_refs_json: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectCharterRevision {
    pub id: String,
    pub charter_id: String,
    pub expected_charter_version: i64,
    pub project_mode: String,
    pub maturity: String,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub schema_version: String,
    pub render_version: String,
    pub content_json: String,
    pub rendered_view: String,
    pub change_summary: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub source_refs_json: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub created_at: String,
}

/// Atomically create (or claim) an owned Charter and append its first
/// revision.  The ownership claim and revision pointer must share a
/// transaction: a failed first revision must not leave an empty Charter
/// attached to a Project or Genesis session, and two callers racing for one
/// caller-supplied ID must have one serialized winner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectCharterRevisionAtomically {
    pub project_id: Option<String>,
    pub genesis_session_id: Option<String>,
    pub account_id: String,
    pub charter: CreateProjectCharter,
    pub revision: CreateProjectCharterRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCharterApprovalRecord {
    pub id: String,
    pub approval_type: String,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub expected_charter_version: i64,
    pub approved_name: Option<String>,
    pub approved_slug: Option<String>,
    pub approved_project_mode: String,
    pub selected_identity_id: Option<String>,
    pub selected_profile_id: Option<String>,
    pub selected_operating_skill_revision_id: Option<String>,
    pub selected_policy_revision: Option<String>,
    pub selected_policy_digest: Option<String>,
    pub approving_principal_type: String,
    pub approving_principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub source_action: String,
    pub approval_event_id: Option<String>,
    pub lifecycle: String,
    pub idempotency_key: String,
    pub consumed_project_id: Option<String>,
    pub consumed_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// An immutable, project-scoped record of two canonical claims which cannot
/// both be authoritative. The referenced records are typed IDs plus exact
/// revisions/digests; their bodies are intentionally not copied here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCanonicalConflictRecord {
    pub id: String,
    pub project_id: String,
    pub domain: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub governing_record_revision: String,
    pub governing_record_digest: String,
    pub conflicting_record_type: String,
    pub conflicting_record_id: String,
    pub conflicting_record_revision: String,
    pub conflicting_record_digest: String,
    pub affected_paths_json: String,
    pub conflict_code: String,
    pub description: String,
    pub detected_by_type: String,
    pub detected_by_id: Option<String>,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectCanonicalConflict {
    pub id: String,
    pub project_id: String,
    pub domain: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub governing_record_revision: String,
    pub governing_record_digest: String,
    pub conflicting_record_type: String,
    pub conflicting_record_id: String,
    pub conflicting_record_revision: String,
    pub conflicting_record_digest: String,
    pub affected_paths_json: String,
    pub conflict_code: String,
    pub description: String,
    pub detected_by_type: String,
    pub detected_by_id: Option<String>,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub idempotency_key: String,
    pub created_at: String,
}

/// A typed reconciliation projection attached to one affected record. Its
/// state is `required` until the explicit resolution operation inserts an
/// immutable resolution event and advances this row to one of the five
/// allowed retained/revised/cancelled/superseded/invalidated outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReconciliationRecord {
    pub id: String,
    pub project_id: String,
    pub conflict_id: String,
    pub record_type: String,
    pub record_id: String,
    pub record_revision: String,
    pub record_digest: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub governing_record_revision: String,
    pub governing_record_digest: String,
    pub state: String,
    pub current_resolution_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReconciliation {
    pub id: String,
    pub project_id: String,
    pub conflict_id: String,
    pub record_type: String,
    pub record_id: String,
    pub record_revision: String,
    pub record_digest: String,
    pub governing_record_type: String,
    pub governing_record_id: String,
    pub governing_record_revision: String,
    pub governing_record_digest: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveProjectReconciliation {
    pub id: String,
    pub expected_version: i64,
    pub resolution_id: String,
    pub action: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub reason: String,
    pub occurred_at: String,
    pub idempotency_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveProjectCharter {
    pub id: String,
    pub approval_type: String,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub expected_charter_version: i64,
    pub approved_name: Option<String>,
    pub approved_slug: Option<String>,
    pub approved_project_mode: String,
    pub selected_identity_id: Option<String>,
    pub selected_profile_id: Option<String>,
    pub selected_operating_skill_revision_id: Option<String>,
    pub selected_policy_revision: Option<String>,
    pub selected_policy_digest: Option<String>,
    pub approving_principal_type: String,
    pub approving_principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub source_action: String,
    pub idempotency_key: String,
    pub event_id: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDocumentRecord {
    pub id: String,
    pub project_id: String,
    pub kind: String,
    pub title: String,
    pub lifecycle: String,
    pub approval_policy: String,
    pub current_draft_revision_id: Option<String>,
    pub current_approved_revision_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDocument {
    pub id: String,
    pub project_id: String,
    pub kind: String,
    pub title: String,
    pub approval_policy: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDocumentRevisionRecord {
    pub id: String,
    pub document_id: String,
    pub revision: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub schema_version: String,
    pub render_version: String,
    pub content_json: String,
    pub rendered_view: String,
    pub change_summary: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_refs_json: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDocumentRevision {
    pub id: String,
    pub document_id: String,
    pub expected_document_version: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub schema_version: String,
    pub render_version: String,
    pub content_json: String,
    pub rendered_view: String,
    pub change_summary: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_refs_json: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub created_at: String,
}

/// Atomically create a Project Document shell and its first revision,
/// pointing `current_draft_revision_id` at that revision in the same
/// transaction.  A failed first revision must not leave an empty Document
/// shell behind, and the caller-supplied id on `document.id` is a
/// server-minted primary key, never a value trusted from an agent payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDocumentAtomically {
    pub document: CreateProjectDocument,
    pub revision: CreateProjectDocumentRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDocumentApprovalRecord {
    pub id: String,
    pub document_id: String,
    pub revision_id: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub lifecycle: String,
    pub idempotency_key: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveProjectDocument {
    pub id: String,
    pub document_id: String,
    pub revision_id: String,
    pub expected_document_version: i64,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub idempotency_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDecisionCandidateRecord {
    pub id: String,
    pub project_id: String,
    pub lifecycle: String,
    pub question: String,
    pub context_json: String,
    pub options_json: String,
    pub selected_outcome: Option<String>,
    pub rationale: Option<String>,
    pub principal_type: Option<String>,
    pub principal_id: Option<String>,
    pub source_refs_json: String,
    pub expected_project_version: i64,
    pub effective_decision_id: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDecisionCandidate {
    pub id: String,
    pub project_id: String,
    pub lifecycle: String,
    pub question: String,
    pub context_json: String,
    pub options_json: String,
    pub selected_outcome: Option<String>,
    pub rationale: Option<String>,
    pub principal_type: Option<String>,
    pub principal_id: Option<String>,
    pub source_refs_json: String,
    pub expected_project_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDecisionRecord {
    pub id: String,
    pub project_id: String,
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
    pub baseline_revision_id: Option<String>,
    pub source_refs_json: String,
    pub affected_records_json: String,
    pub supersedes_decision_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDecision {
    pub id: String,
    pub project_id: String,
    pub expected_project_version: i64,
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
    pub baseline_revision_id: Option<String>,
    pub source_refs_json: String,
    pub affected_records_json: String,
    pub supersedes_decision_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectExecutionBaselineRecord {
    pub id: String,
    pub project_id: String,
    pub current_revision_id: Option<String>,
    pub lifecycle: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// The baseline shell primary key is server-minted: agent/client payloads
/// routinely carry fabricated identifiers, so callers never supply one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectExecutionBaseline {
    pub project_id: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectExecutionBaselineRevisionRecord {
    pub id: String,
    pub baseline_id: String,
    pub revision: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub charter_revision_id: String,
    pub document_revisions_json: String,
    pub plan_items_json: String,
    pub milestone_id: Option<String>,
    pub milestone_ids_json: String,
    pub milestone_definition_revision_ids_json: String,
    pub primary_milestone_id: Option<String>,
    pub release_policy_json: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub acceptance_matrix_json: String,
    pub capability_classes_json: String,
    pub risk_classes_json: String,
    pub adaptive_envelope_json: String,
    pub elevated_operations_json: String,
    pub exclusions_json: String,
    pub rollback_recovery_json: String,
    pub schema_version: String,
    pub render_version: String,
    pub rendered_view: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub source_refs_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectExecutionBaselineRevision {
    pub id: String,
    pub baseline_id: String,
    pub expected_baseline_version: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub charter_revision_id: String,
    pub document_revisions_json: String,
    pub plan_items_json: String,
    pub milestone_id: Option<String>,
    pub milestone_ids_json: String,
    pub milestone_definition_revision_ids_json: String,
    pub primary_milestone_id: Option<String>,
    pub release_policy_json: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub acceptance_matrix_json: String,
    pub capability_classes_json: String,
    pub risk_classes_json: String,
    pub adaptive_envelope_json: String,
    pub elevated_operations_json: String,
    pub exclusions_json: String,
    pub rollback_recovery_json: String,
    pub schema_version: String,
    pub render_version: String,
    pub rendered_view: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub source_refs_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectExecutionBaselineApprovalRecord {
    pub id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub expected_project_version: i64,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub lifecycle: String,
    pub idempotency_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveProjectExecutionBaseline {
    pub id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub idempotency_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateProjectExecutionBaseline {
    pub approval_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMilestoneRecord {
    pub id: String,
    pub project_id: String,
    pub milestone_sequence: i64,
    pub milestone_key: String,
    pub display_label: Option<String>,
    pub current_definition_revision_id: Option<String>,
    pub lifecycle: String,
    pub blocker_reason_json: String,
    pub stale_reason_json: String,
    pub reconciliation_reason_json: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestone {
    pub id: String,
    pub project_id: String,
    pub expected_project_version: i64,
    pub milestone_sequence: i64,
    pub milestone_key: String,
    pub display_label: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMilestoneRevisionRecord {
    pub id: String,
    pub milestone_id: String,
    pub revision: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub display_label: Option<String>,
    pub outcome: String,
    pub included_scope_json: String,
    pub excluded_scope_json: String,
    pub charter_revision_id: Option<String>,
    pub document_revisions_json: String,
    pub task_selection_json: String,
    pub dependencies_json: String,
    pub risks_json: String,
    pub acceptance_checks_json: String,
    pub evidence_requirements_json: String,
    pub known_issues_json: String,
    pub change_summary: String,
    pub schema_version: String,
    pub render_version: String,
    pub rendered_view: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_refs_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestoneRevision {
    pub id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub base_revision: i64,
    pub base_revision_id: Option<String>,
    pub lifecycle: String,
    pub display_label: Option<String>,
    pub outcome: String,
    pub included_scope_json: String,
    pub excluded_scope_json: String,
    pub charter_revision_id: Option<String>,
    pub document_revisions_json: String,
    pub task_selection_json: String,
    pub dependencies_json: String,
    pub risks_json: String,
    pub acceptance_checks_json: String,
    pub evidence_requirements_json: String,
    pub known_issues_json: String,
    pub change_summary: String,
    pub schema_version: String,
    pub render_version: String,
    pub rendered_view: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub author_type: String,
    pub author_id: Option<String>,
    pub source_refs_json: String,
    pub created_at: String,
}

/// Atomically create a Project Milestone shell and its first definition
/// revision.  A failed first revision must not leave an empty Milestone
/// shell behind.  The pointer trigger only allows
/// `current_definition_revision_id` to target a `proposed`/`approved`
/// revision, so a `draft` first revision intentionally leaves the pointer
/// NULL until a later action promotes it -- callers projecting a milestone's
/// definition must fall back to its latest revision when the pointer is
/// unset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestoneAtomically {
    pub milestone: CreateProjectMilestone,
    pub revision: CreateProjectMilestoneRevision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMilestoneCheckRecord {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub definition_revision_id: String,
    pub check_key: String,
    pub description: String,
    pub required: bool,
    pub source_kind: String,
    pub expected_result: String,
    pub evidence_required: bool,
    pub version: i64,
    pub current_result_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestoneCheck {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub definition_revision_id: String,
    pub expected_milestone_version: i64,
    pub check_key: String,
    pub description: String,
    pub required: bool,
    pub source_kind: String,
    pub expected_result: String,
    pub evidence_required: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMilestoneCheckResultRecord {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub check_id: String,
    pub definition_revision_id: String,
    pub outcome: String,
    pub source_kind: String,
    pub source_manifest_json: String,
    pub input_digest: String,
    pub governing_charter_revision_id: Option<String>,
    pub governing_baseline_revision_id: Option<String>,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub expected_version: i64,
    pub explicit_event: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestoneCheckResult {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub check_id: String,
    pub definition_revision_id: String,
    pub outcome: String,
    pub source_kind: String,
    pub source_manifest_json: String,
    pub input_digest: String,
    pub governing_charter_revision_id: Option<String>,
    pub governing_baseline_revision_id: Option<String>,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub expected_version: i64,
    pub explicit_event: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReadinessSnapshotRecord {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub definition_revision_id: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub input_manifest_json: String,
    pub event_watermark: String,
    pub outcome: String,
    pub blocking_reasons_json: String,
    pub check_results_json: String,
    pub waiver_manifest_json: String,
    pub evidence_manifest_json: String,
    pub commit_context_json: String,
    pub computing_policy_revision: String,
    pub readiness_digest: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub expected_milestone_version: i64,
    pub explicit_event: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReadinessSnapshot {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub definition_revision_id: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub input_manifest_json: String,
    pub event_watermark: String,
    pub outcome: String,
    pub blocking_reasons_json: String,
    pub check_results_json: String,
    pub waiver_manifest_json: String,
    pub evidence_manifest_json: String,
    pub commit_context_json: String,
    pub computing_policy_revision: String,
    pub readiness_digest: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub expected_milestone_version: i64,
    pub explicit_event: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReleaseRecord {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub release_sequence: i64,
    pub release_revision: i64,
    pub release_identifier: String,
    pub milestone_revision_id: String,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub summary: String,
    pub changelog: String,
    pub known_issues_json: String,
    pub charter_revision_id: Option<String>,
    pub document_revisions_json: String,
    pub decision_ids_json: String,
    pub task_references_json: String,
    pub validation_references_json: String,
    pub git_references_json: String,
    pub evidence_references_json: String,
    pub waivers_json: String,
    pub releasing_principal_type: String,
    pub releasing_principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub schema_version: String,
    pub snapshot_digest: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectRelease {
    pub id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub release_sequence: i64,
    pub release_revision: i64,
    pub release_identifier: String,
    pub milestone_revision_id: String,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub baseline_id: String,
    pub baseline_revision_id: String,
    pub baseline_digest: String,
    pub release_policy_revision: String,
    pub release_policy_digest: String,
    pub summary: String,
    pub changelog: String,
    pub known_issues_json: String,
    pub charter_revision_id: Option<String>,
    pub document_revisions_json: String,
    pub decision_ids_json: String,
    pub task_references_json: String,
    pub validation_references_json: String,
    pub git_references_json: String,
    pub evidence_references_json: String,
    pub waivers_json: String,
    pub releasing_principal_type: String,
    pub releasing_principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub schema_version: String,
    pub snapshot_digest: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReleaseReferenceRecord {
    pub release_id: String,
    pub ordinal: i64,
    pub reference_kind: String,
    pub record_id: String,
    pub record_version: Option<String>,
    pub record_state: Option<String>,
    pub record_digest: Option<String>,
    pub metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseReference {
    pub release_id: String,
    pub ordinal: i64,
    pub reference_kind: String,
    pub record_id: String,
    pub record_version: Option<String>,
    pub record_state: Option<String>,
    pub record_digest: Option<String>,
    pub metadata_json: String,
}

/// Inputs for the one transaction which turns an exact Charter approval into
/// a Project, Project Agent binding, and typed handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectFromCharterApproval {
    pub approval_id: String,
    pub idempotency_key: String,
    pub account_id: String,
    pub project: CreateProject,
    pub project_agent_binding_id: String,
    pub handoff_id: String,
    pub target_message_id: String,
    pub target_turn_id: String,
    /// Historical Main identity that authored the Genesis source turn.  This
    /// must not be reconstructed from the current account Main binding after
    /// discovery has completed.
    pub source_identity_id: Option<String>,
    pub source_profile_id: Option<String>,
    pub source_instruction_revision_id: Option<String>,
    pub source_message_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub handoff_content: String,
    pub content_guard_json: String,
    pub source_revisions_json: String,
    /// The authenticated user authorization which consumes the approval and
    /// creates the Project. This is deliberately distinct from the Charter
    /// approval provenance: approving a Charter and materializing its Project
    /// are two separate user actions.
    pub create_principal_type: String,
    pub create_principal_id: String,
    pub create_authorization_basis: String,
    pub create_action: String,
    pub create_event_id: String,
    pub create_occurred_at: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub max_attempts: i64,
    pub policy_revision: String,
    pub policy_digest: String,
    pub member_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedProjectFromCharterApproval {
    pub project: Project,
    pub project_agent_binding_id: String,
    pub project_chat_id: String,
    pub charter_id: String,
    pub charter_revision_id: String,
    pub handoff_id: String,
    pub target_message_id: String,
    pub target_turn_id: String,
}

#[async_trait]
pub trait ProjectOrchestrationRepo: Send + Sync {
    async fn get_project_charter(&self, id: &str) -> Result<Option<ProjectCharterRecord>>;
    async fn get_project_charter_for_account(
        &self,
        id: &str,
        account_id: &str,
    ) -> Result<Option<ProjectCharterRecord>>;
    async fn create_project_charter(
        &self,
        input: CreateProjectCharter,
    ) -> Result<ProjectCharterRecord>;
    async fn get_project_charter_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterRevisionRecord>>;
    async fn list_project_charter_revisions(
        &self,
        charter_id: &str,
    ) -> Result<Vec<ProjectCharterRevisionRecord>>;
    async fn create_project_charter_revision(
        &self,
        input: CreateProjectCharterRevision,
    ) -> Result<ProjectCharterRevisionRecord>;
    async fn create_project_charter_revision_atomically(
        &self,
        input: CreateProjectCharterRevisionAtomically,
    ) -> Result<ProjectCharterRevisionRecord>;
    async fn get_project_charter_approval(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterApprovalRecord>>;
    async fn approve_project_charter(
        &self,
        input: ApproveProjectCharter,
    ) -> Result<ProjectCharterApprovalRecord>;
    async fn create_project_from_charter_approval(
        &self,
        input: CreateProjectFromCharterApproval,
    ) -> Result<CreatedProjectFromCharterApproval>;

    async fn create_project_canonical_conflict(
        &self,
        input: CreateProjectCanonicalConflict,
    ) -> Result<ProjectCanonicalConflictRecord>;
    async fn get_project_canonical_conflict(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCanonicalConflictRecord>>;
    async fn list_project_canonical_conflicts(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectCanonicalConflictRecord>>;
    async fn create_project_reconciliation(
        &self,
        input: CreateProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord>;
    async fn get_project_reconciliation(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationRecord>>;
    async fn list_project_reconciliations(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectReconciliationRecord>>;
    async fn resolve_project_reconciliation(
        &self,
        input: ResolveProjectReconciliation,
    ) -> Result<ProjectReconciliationRecord>;

    async fn create_project_document(
        &self,
        input: CreateProjectDocument,
    ) -> Result<ProjectDocumentRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_document(&self, id: &str) -> Result<Option<ProjectDocumentRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_document_revision(
        &self,
        input: CreateProjectDocumentRevision,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_document_atomically(
        &self,
        input: CreateProjectDocumentAtomically,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }
    async fn approve_project_document(
        &self,
        input: ApproveProjectDocument,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }

    async fn get_project_document_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDocumentRevisionRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_document_revisions(
        &self,
        document_id: &str,
    ) -> Result<Vec<ProjectDocumentRevisionRecord>> {
        let _ = document_id;
        Err(crate::DbError::Check(
            "Project document persistence is not wired".to_owned(),
        ))
    }

    async fn create_project_decision_candidate(
        &self,
        input: CreateProjectDecisionCandidate,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_decision_candidate(
        &self,
        id: &str,
    ) -> Result<Option<ProjectDecisionCandidateRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Project decision persistence is not wired".to_owned(),
        ))
    }
    async fn append_project_decision(
        &self,
        input: CreateProjectDecision,
    ) -> Result<ProjectDecisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_decision_candidates(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionCandidateRecord>> {
        let _ = project_id;
        Err(crate::DbError::Check(
            "Project decision persistence is not wired".to_owned(),
        ))
    }
    async fn list_effective_project_decisions(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectDecisionRecord>> {
        let _ = project_id;
        Err(crate::DbError::Check(
            "Project decision persistence is not wired".to_owned(),
        ))
    }

    async fn create_project_execution_baseline(
        &self,
        input: CreateProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_execution_baseline(
        &self,
        id: &str,
    ) -> Result<Option<ProjectExecutionBaselineRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_execution_baseline_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectExecutionBaselineRevisionRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn approve_project_execution_baseline(
        &self,
        input: ApproveProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineApprovalRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_execution_baseline_revision(
        &self,
        input: CreateProjectExecutionBaselineRevision,
    ) -> Result<ProjectExecutionBaselineRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn activate_project_execution_baseline(
        &self,
        input: ActivateProjectExecutionBaseline,
    ) -> Result<ProjectExecutionBaselineRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }

    async fn create_project_milestone(
        &self,
        input: CreateProjectMilestone,
    ) -> Result<ProjectMilestoneRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_milestone_atomically(
        &self,
        input: CreateProjectMilestoneAtomically,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_milestones(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectMilestoneRecord>> {
        let _ = project_id;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_milestone_revision(
        &self,
        id: &str,
    ) -> Result<Option<ProjectMilestoneRevisionRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_milestone_revisions(
        &self,
        milestone_id: &str,
    ) -> Result<Vec<ProjectMilestoneRevisionRecord>> {
        let _ = milestone_id;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_milestone_revision(
        &self,
        input: CreateProjectMilestoneRevision,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_milestone(&self, id: &str) -> Result<Option<ProjectMilestoneRecord>> {
        let _ = id;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_milestone_check(
        &self,
        input: CreateProjectMilestoneCheck,
    ) -> Result<ProjectMilestoneCheckRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn append_project_milestone_check_result(
        &self,
        input: CreateProjectMilestoneCheckResult,
    ) -> Result<ProjectMilestoneCheckResultRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_readiness_snapshot(
        &self,
        input: CreateProjectReadinessSnapshot,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Readiness persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_release(
        &self,
        input: CreateProjectRelease,
        references: Vec<CreateProjectReleaseReference>,
    ) -> Result<ProjectReleaseRecord> {
        let _ = (input, references);
        Err(crate::DbError::Check(
            "Release persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_release_references(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseReferenceRecord>> {
        let _ = release_id;
        Err(crate::DbError::Check(
            "Release persistence is not wired".to_owned(),
        ))
    }
}

/// Promote preplanned Task governance inside a baseline-activation
/// transaction.
///
/// This is the single shared implementation behind both activation surfaces
/// (the REST route and `ProjectOrchestrationRepo::activate_project_execution_baseline`).
/// It mirrors the server-side governance derivation used for new Task
/// proposals (`derive_active_baseline_governance` in the services crate):
/// a Task is bound to the exact activated baseline revision, its plan item,
/// and its milestone (falling back to the baseline's primary milestone).
///
/// Two classes of preplanned Tasks are promoted:
/// 1. Governance rows already bound to the activated `(baseline, revision)`
///    pair flip to `runnable = 1`.
/// 2. Non-terminal Tasks whose governance row names a plan item present in
///    the activated revision but is bound to no baseline (or a stale one)
///    are re-bound to the activated revision. Governance links are immutable
///    by trigger, so the stale row is replaced rather than updated; the
///    replacement becomes runnable only for repository-backed Tasks, exactly
///    like a fresh proposal would.
///
/// The database runnable-guard triggers re-validate every promotion against
/// the active baseline, current Charter, and the exact user approval receipt,
/// so this pass can never mint authority the triggers would refuse.
pub async fn promote_baseline_task_governance_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    project_id: &str,
    baseline_id: &str,
    revision_id: &str,
    now: &str,
) -> Result<u64> {
    use sqlx::Row as _;

    let Some(revision) = sqlx::query(
        "SELECT r.charter_revision_id, r.plan_items_json, r.milestone_id,
                r.milestone_ids_json, r.milestone_definition_revision_ids_json,
                r.primary_milestone_id, r.content_digest, r.rendered_digest,
                r.adaptive_envelope_json
         FROM project_execution_baseline_revision r
         JOIN project_execution_baseline b
           ON b.id = r.baseline_id AND b.project_id = ?
         WHERE r.id = ? AND r.baseline_id = ?
           AND b.lifecycle = 'active' AND b.current_revision_id = r.id
           AND r.lifecycle = 'approved'",
    )
    .bind(project_id)
    .bind(revision_id)
    .bind(baseline_id)
    .fetch_optional(&mut **tx)
    .await?
    else {
        return Ok(0);
    };
    let charter_revision_id: String = revision.try_get("charter_revision_id")?;
    let plan_items_json: String = revision.try_get("plan_items_json")?;
    let revision_milestone_id: Option<String> = revision.try_get("milestone_id")?;
    let milestone_ids_json: String = revision.try_get("milestone_ids_json")?;
    let milestone_definition_revision_ids_json: String =
        revision.try_get("milestone_definition_revision_ids_json")?;
    let primary_milestone_id: Option<String> = revision.try_get("primary_milestone_id")?;
    let content_digest: String = revision.try_get("content_digest")?;
    let rendered_digest: String = revision.try_get("rendered_digest")?;
    let adaptive_envelope_json: String = revision.try_get("adaptive_envelope_json")?;

    // Nothing is promotable without the current Charter and the exact user
    // approval receipt the runnable-guard triggers demand.
    let admitted: i64 = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0
               AND current_charter_revision_id = ?
         ) AND EXISTS (
             SELECT 1 FROM project_execution_baseline_approval
             WHERE baseline_id = ? AND revision_id = ?
               AND principal_type = 'user'
               AND authorization_action = 'project.execution_baseline.approve'
               AND length(trim(authorization_basis)) > 0
               AND length(trim(authorization_occurred_at)) > 0
               AND length(trim(explicit_event)) > 0
               AND content_digest = ? AND rendered_digest = ?
               AND lifecycle IN ('active', 'consumed')
         )",
    )
    .bind(project_id)
    .bind(&charter_revision_id)
    .bind(baseline_id)
    .bind(revision_id)
    .bind(&content_digest)
    .bind(&rendered_digest)
    .fetch_one(&mut **tx)
    .await?;
    if admitted != 1 {
        return Ok(0);
    }

    // 1) Preplanned Tasks already bound to this exact baseline revision.
    let flipped = sqlx::query(
        "UPDATE project_task_governance
         SET runnable = 1, version = version + 1, updated_at = ?
         WHERE project_id = ? AND baseline_id = ? AND baseline_revision_id = ?
           AND charter_revision_id = ? AND runnable = 0",
    )
    .bind(now)
    .bind(project_id)
    .bind(baseline_id)
    .bind(revision_id)
    .bind(&charter_revision_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    // 2) Non-terminal Tasks that reference a plan item the activated revision
    //    includes, but whose governance row predates the activation.
    let plan_item_ids = plan_item_identifiers(&plan_items_json);
    let mut rebound = 0_u64;
    if !plan_item_ids.is_empty() {
        let candidates = sqlx::query(
            "SELECT g.task_id, g.plan_item_id, g.milestone_id,
                    g.document_revisions_json, g.capability_class, g.risk_class,
                    g.replacement_of_task_id, g.provenance_json, g.created_at,
                    (t.repo_id IS NOT NULL) AS repository_capable
             FROM project_task_governance g
             JOIN task t ON t.id = g.task_id AND t.project_id = g.project_id
             WHERE g.project_id = ?
               AND g.plan_item_id IS NOT NULL
               AND NOT (g.baseline_id IS ? AND g.baseline_revision_id IS ?)
               AND t.deleted_at IS NULL
               AND t.status NOT IN ('done', 'cancelled')",
        )
        .bind(project_id)
        .bind(baseline_id)
        .bind(revision_id)
        .fetch_all(&mut **tx)
        .await?;
        let plan_items: serde_json::Value =
            serde_json::from_str(&plan_items_json).unwrap_or(serde_json::Value::Null);
        let milestone_ids: serde_json::Value =
            serde_json::from_str(&milestone_ids_json).unwrap_or(serde_json::Value::Null);
        for candidate in candidates {
            let plan_item_id: String = candidate.try_get("plan_item_id")?;
            if !plan_item_ids.iter().any(|id| id == &plan_item_id) {
                continue;
            }
            let task_id: String = candidate.try_get("task_id")?;
            let old_milestone_id: Option<String> = candidate.try_get("milestone_id")?;
            let milestone_id = old_milestone_id
                .filter(|id| {
                    revision_milestone_id.as_deref() == Some(id.as_str())
                        || primary_milestone_id.as_deref() == Some(id.as_str())
                        || json_names_identifier(&milestone_ids, id)
                        || json_names_identifier(&plan_items, id)
                })
                .or_else(|| primary_milestone_id.clone());
            let repository_capable: i64 = candidate.try_get("repository_capable")?;
            let provenance_json = rebound_provenance_json(
                &candidate.try_get::<String, _>("provenance_json")?,
                &plan_item_id,
                baseline_id,
                revision_id,
                &content_digest,
                &rendered_digest,
                &adaptive_envelope_json,
                &milestone_definition_revision_ids_json,
            )?;
            // Governance links are immutable by trigger: replace the stale
            // row inside the activation transaction instead of updating it.
            sqlx::query("DELETE FROM project_task_governance WHERE task_id = ? AND project_id = ?")
                .bind(&task_id)
                .bind(project_id)
                .execute(&mut **tx)
                .await?;
            sqlx::query(
                "INSERT INTO project_task_governance
                 (task_id, project_id, charter_revision_id, baseline_id,
                  baseline_revision_id, plan_item_id, milestone_id,
                  document_revisions_json, capability_class, risk_class,
                  runnable, replacement_of_task_id, provenance_json,
                  version, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(&task_id)
            .bind(project_id)
            .bind(&charter_revision_id)
            .bind(baseline_id)
            .bind(revision_id)
            .bind(&plan_item_id)
            .bind(milestone_id.as_deref())
            .bind(candidate.try_get::<String, _>("document_revisions_json")?)
            .bind(candidate.try_get::<Option<String>, _>("capability_class")?)
            .bind(candidate.try_get::<Option<String>, _>("risk_class")?)
            .bind(if repository_capable == 1 {
                1_i64
            } else {
                0_i64
            })
            .bind(candidate.try_get::<Option<String>, _>("replacement_of_task_id")?)
            .bind(&provenance_json)
            .bind(candidate.try_get::<String, _>("created_at")?)
            .bind(now)
            .execute(&mut **tx)
            .await?;
            rebound += 1;
        }
    }
    Ok(flipped + rebound)
}

/// The plan-item identifiers a baseline revision covers. The canonical shape
/// is `[{"id": "..."}]`; plain strings and `plan_item_id` keys are accepted
/// for parity with the proposal-side identifier matching.
fn plan_item_identifiers(plan_items_json: &str) -> Vec<String> {
    let Ok(serde_json::Value::Array(items)) =
        serde_json::from_str::<serde_json::Value>(plan_items_json)
    else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| match item {
            serde_json::Value::String(id) => Some(id.clone()),
            serde_json::Value::Object(map) => map
                .get("id")
                .or_else(|| map.get("plan_item_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .filter(|id| !id.trim().is_empty())
        .collect()
}

/// Recursive identifier containment matching the proposal-side
/// `json_contains_identifier` semantics.
fn json_names_identifier(value: &serde_json::Value, identifier: &str) -> bool {
    match value {
        serde_json::Value::String(value) => value == identifier,
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_names_identifier(value, identifier)),
        serde_json::Value::Object(values) => {
            ["id", "plan_item_id", "document_revision_id", "milestone_id"]
                .iter()
                .any(|key| values.get(*key).and_then(serde_json::Value::as_str) == Some(identifier))
                || values
                    .values()
                    .any(|value| json_names_identifier(value, identifier))
        }
        _ => false,
    }
}

/// The authoritative provenance envelope for a re-bound governance row,
/// mirroring the proposal-side `build_provenance` shape.
#[allow(clippy::too_many_arguments)]
fn rebound_provenance_json(
    existing: &str,
    plan_item_id: &str,
    baseline_id: &str,
    revision_id: &str,
    content_digest: &str,
    rendered_digest: &str,
    adaptive_envelope_json: &str,
    milestone_definition_revision_ids_json: &str,
) -> Result<String> {
    use sha2::Digest as _;

    let mut map = match serde_json::from_str::<serde_json::Value>(existing) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    map.remove("baseline_pending");
    for (key, value) in [
        ("origin_plan_item_id", plan_item_id),
        ("governing_baseline_id", baseline_id),
        ("governing_baseline_revision_id", revision_id),
        ("governing_baseline_content_digest", content_digest),
        ("governing_baseline_rendered_digest", rendered_digest),
    ] {
        map.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
    }
    map.insert(
        "adaptive_envelope_digest".to_owned(),
        serde_json::Value::String(hex::encode(sha2::Sha256::digest(
            adaptive_envelope_json.as_bytes(),
        ))),
    );
    map.insert(
        "governing_milestone_definition_revision_ids".to_owned(),
        serde_json::from_str(milestone_definition_revision_ids_json)
            .unwrap_or(serde_json::Value::Array(Vec::new())),
    );
    map.insert(
        "schema".to_owned(),
        serde_json::Value::String("forge.task-governance/v1".to_owned()),
    );
    serde_json::to_string(&serde_json::Value::Object(map))
        .map_err(|error| crate::DbError::Check(error.to_string()))
}
