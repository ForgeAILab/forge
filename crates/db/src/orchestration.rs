//! Persistence contracts for Charter-backed Project orchestration.
//!
//! This module deliberately keeps the database crate independent from the API
//! crate.  The API owns its closed wire enums and JSON contracts; the database
//! exposes stable records and mutation inputs containing the exact revision and
//! digest values which are persisted by SQLite.  Services are responsible for
//! converting between the two layers and for policy authorization.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::{
    CommandReceipt, CreateAgentActionExecution, CreateCommandReceipt, CreateDomainEvent,
    CreateProject, CreateTask, CreateTaskRoleAssignment, Project, Result, Task,
};

/// Recursively sort object keys while preserving array order for the
/// immutable Project handoff contract.  The handoff is assembled in two
/// places (the SQLite create transaction and the Project Agent turn worker),
/// so both sides must use the same canonical JSON projection.
pub fn canonicalize_project_handoff_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_project_handoff_json(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(values) => serde_json::Value::Array(
            values
                .iter()
                .map(canonicalize_project_handoff_json)
                .collect(),
        ),
        scalar => scalar.clone(),
    }
}

/// Remove only values allocated by the Project-create transaction from a
/// handoff packet.  Charter/approval/policy/authorization and semantic
/// content remain part of the immutable request identity; transport IDs and
/// delivery timestamps are filled by the transaction and therefore are not.
pub fn normalize_project_handoff_request(
    value: &serde_json::Value,
) -> std::result::Result<serde_json::Value, String> {
    let mut normalized = value.clone();
    let object = normalized
        .as_object_mut()
        .ok_or_else(|| "handoff source_revisions_json must be a JSON object".to_owned())?;
    object.remove("approval_id");
    object.remove("handoff_id");
    object.remove("correlation_id");
    object.remove("created_at");
    if let Some(project) = object
        .get_mut("project")
        .and_then(serde_json::Value::as_object_mut)
    {
        project.remove("id");
    }
    if let Some(request) = object
        .get_mut("request")
        .and_then(serde_json::Value::as_object_mut)
    {
        request.remove("policy_revision");
        request.remove("policy_digest");
        request.remove("source_revisions_digest");
        request.remove("authorization");
    }
    if let Some(target) = object
        .get_mut("target")
        .and_then(serde_json::Value::as_object_mut)
    {
        target.insert("chat_id".to_owned(), serde_json::Value::Null);
        target.remove("binding_id");
        target.remove("message_id");
        target.remove("turn_id");
    }
    if let Some(delivery) = object
        .get_mut("delivery")
        .and_then(serde_json::Value::as_object_mut)
    {
        delivery.insert("delivered_at".to_owned(), serde_json::Value::Null);
    }
    if let Some(source) = object
        .get_mut("source")
        .and_then(serde_json::Value::as_object_mut)
    {
        source.remove("message_id");
    }
    Ok(normalized)
}

/// Compute the immutable handoff request fingerprint used by both the
/// SQLite Project-create transaction and the Project Agent turn worker.
/// `source_revisions_json` is passed separately because the initial create
/// request does not yet contain the server-added `request` envelope.
pub fn project_handoff_request_fingerprint(
    value: &serde_json::Value,
    source_revisions_json: &str,
    authorization: &serde_json::Value,
) -> std::result::Result<String, String> {
    let mut normalized = normalize_project_handoff_request(value)?;
    let object = normalized
        .as_object_mut()
        .ok_or_else(|| "handoff source_revisions_json must be a JSON object".to_owned())?;
    let request = object
        .entry("request".to_owned())
        .or_insert_with(|| serde_json::json!({}));
    let request = request
        .as_object_mut()
        .ok_or_else(|| "handoff request must be a JSON object".to_owned())?;
    let source_value: serde_json::Value = serde_json::from_str(source_revisions_json)
        .map_err(|error| format!("handoff source_revisions_json is invalid: {error}"))?;
    let source_value = normalize_project_handoff_request(&source_value)?;
    let source_revisions_json =
        serde_json::to_string(&canonicalize_project_handoff_json(&source_value))
            .map_err(|error| format!("handoff source_revisions_json is invalid: {error}"))?;
    request.insert(
        "source_revisions_json".to_owned(),
        serde_json::Value::String(source_revisions_json),
    );
    request.insert(
        "authorization".to_owned(),
        canonicalize_project_handoff_json(authorization),
    );
    let canonical = canonicalize_project_handoff_json(&normalized);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| format!("handoff request is invalid: {error}"))?;
    Ok(hex::encode(sha2::Sha256::digest(&bytes)))
}

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
    /// Optional command finalization owned by the same SQLite transaction as
    /// the revision and its durable event.  Ordinary REST/Project-Agent
    /// callers leave this unset; the Main command boundary supplies it.
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
    /// Optional command finalization owned by the same SQLite transaction as
    /// the Charter shell, first revision, and durable event.
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Finalize a semantically equivalent first-Charter-revision retry without
/// inserting another immutable revision.  The command receipt and replay
/// event remain part of the same transaction as the current-draft checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeProjectCharterRevisionNoop {
    pub account_id: String,
    pub project_id: String,
    pub charter_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub command_receipt: CreateCommandReceipt,
    pub action_execution: Option<CreateAgentActionExecution>,
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

/// One immutable resolution event applied to a `ProjectReconciliationRecord`.
/// A reconciliation currently carries at most one resolution (the closed
/// state machine has no re-open path), but the row is kept separate from the
/// reconciliation projection so the exact principal, reason, and replacement
/// reference stay append-only evidence rather than columns rewritten in
/// place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReconciliationResolutionRecord {
    pub id: String,
    pub reconciliation_id: String,
    pub action: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub reason: String,
    pub occurred_at: String,
    pub replacement_ref_type: Option<String>,
    pub replacement_ref_id: Option<String>,
    pub replacement_ref_revision: Option<String>,
    pub idempotency_key: String,
    pub created_at: String,
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
    /// Exact successor artifact for a `revised`/`superseded` outcome.  All
    /// three fields are present together or absent together; the service
    /// layer enforces that shape and record-type consistency before this
    /// input is constructed.
    pub replacement_ref_type: Option<String>,
    pub replacement_ref_id: Option<String>,
    pub replacement_ref_revision: Option<String>,
    /// Present only for `invalid_active_baseline` + `revised`. The repository
    /// validates and activates this exact approved successor inside the same
    /// transaction as the reconciliation resolution.
    pub invalid_baseline_replacement: Option<ResolveInvalidActiveBaseline>,
    pub occurred_at: String,
    pub idempotency_key: String,
    pub updated_at: String,
    /// The durable domain event committed in the same transaction as the
    /// resolution.  A replay short-circuits before this input is used, so
    /// exactly one event is ever appended per genuinely new resolution.
    pub domain_event: CreateDomainEvent,
    /// The command receipt bound to `domain_event` by
    /// `finalize_command_in_tx` inside the same transaction.
    pub command_receipt: CreateCommandReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveInvalidActiveBaseline {
    pub project_id: String,
    pub baseline_id: String,
    pub invalid_revision_id: String,
    pub successor_revision_id: String,
    pub approval_id: String,
    /// `true` when the reconciliation click is also the user's exact approval
    /// of the server-generated correction revision. The repository creates
    /// and consumes that approval inside the same repair transaction.
    pub create_approval: bool,
    pub approval_principal_id: String,
    pub approval_authorization_basis: String,
    pub approval_authorization_action: String,
    pub approval_authorization_occurred_at: String,
    pub approval_explicit_event: String,
    pub approval_idempotency_key: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub charter_revision_id: String,
    pub milestone_ids: Vec<String>,
    pub milestone_definition_revision_ids: Vec<String>,
    pub primary_milestone_id: Option<String>,
    pub content_digest: String,
    pub rendered_digest: String,
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

/// Apply a user-approved Charter to an existing Project.  This command owns
/// the complete adoption/amendment boundary: the immutable approval target,
/// Project pointer/version CAS, Project Agent binding rotation, Project Chat
/// bootstrap (for legacy adoption), amendment provenance, and all command
/// finalization are committed by one SQLite transaction.
///
/// The service layer supplies server-minted IDs for the replacement binding,
/// bootstrap message, and amendment row before constructing the command so
/// those IDs can be part of the frozen command outcome.  `None` is accepted
/// for the optional rows only where the operation does not need that row (an
/// amendment has no bootstrap message; an adoption has no amendment row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyProjectCharterApprovalCommand {
    pub approval: ApproveProjectCharter,
    pub project_id: String,
    pub expected_project_version: i64,
    pub expected_current_charter_revision_id: Option<String>,
    pub existing_binding_id: String,
    pub replacement_binding_id: Option<String>,
    pub bootstrap_message_id: Option<String>,
    pub bootstrap_content: Option<String>,
    pub bootstrap_content_guard_json: Option<String>,
    pub bootstrap_author_id: Option<String>,
    pub bootstrap_correlation_id: Option<String>,
    pub bootstrap_source_metadata_json: Option<String>,
    pub amendment_id: Option<String>,
    pub amendment_rationale: Option<String>,
    pub amendment_material_diff_json: Option<String>,
    pub amendment_affected_records_json: Option<String>,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// The frozen domain identity returned by the Charter adoption/amendment
/// composite.  The full wire outcome is owned by the command service and is
/// persisted in `command_receipt.outcome_json`; this record lets repository
/// callers verify every row which the composite touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProjectCharterApprovalRecord {
    pub approval: ProjectCharterApprovalRecord,
    pub project_id: String,
    pub project_version: i64,
    pub project_charter_status: String,
    pub project_charter_setup_required: bool,
    pub project_charter_id: String,
    pub project_charter_revision_id: String,
    pub project_agent_binding_id: String,
    pub project_chat_id: String,
    pub bootstrap_message_id: Option<String>,
    pub amendment_id: Option<String>,
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

/// A command-aware Project Document shell creation.  The shell is kept as a
/// separate public operation because the REST contract creates the typed
/// Document before its first revision is submitted.  The command receipt and
/// domain event are finalized in the same transaction as the Project CAS and
/// shell row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDocumentShellCommand {
    pub document: CreateProjectDocument,
    pub expected_project_version: i64,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// A command-aware first Project Document revision.  The nested domain
/// inputs intentionally remain the same records used by the historical
/// repository methods; the command envelope is an additional transaction
/// boundary rather than a replacement for those low-level contracts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectDocumentCommand {
    pub document: CreateProjectDocument,
    pub revision: CreateProjectDocumentRevision,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendProjectDocumentRevisionCommand {
    pub revision: CreateProjectDocumentRevision,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
pub struct ApproveProjectDocumentCommand {
    pub approval: ApproveProjectDocument,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
pub struct CreateProjectDecisionCandidateCommand {
    pub candidate: CreateProjectDecisionCandidate,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
pub struct AppendProjectDecisionCommand {
    pub decision: CreateProjectDecision,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Approve one current Decision candidate and append its immutable effective
/// Decision in one command transaction.  The candidate id/version is kept
/// separate from the Decision input because candidate approval changes both
/// records and must compare-and-swap each one before the event/receipt is
/// finalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveProjectDecisionCandidateCommand {
    pub candidate_id: String,
    pub expected_candidate_version: i64,
    pub decision: CreateProjectDecision,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectProjectDecisionCandidateCommand {
    pub candidate_id: String,
    pub project_id: String,
    pub expected_project_version: i64,
    pub expected_candidate_version: i64,
    pub reason: String,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub explicit_event: String,
    pub authorization_occurred_at: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
    pub updated_at: String,
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

/// Transactional command input for saving or proposing an execution-baseline
/// revision.  The service owns the lifecycle/validation decision; SQLite
/// owns the shell, immutable revision, event, receipt, and optional action
/// execution composite.  `baseline_id` is optional because the first shell is
/// server-minted and a replay must resolve its authoritative id from the
/// frozen command receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaveProjectExecutionBaselineRevisionCommand {
    pub project_id: String,
    pub baseline_id: Option<String>,
    pub revision_id: String,
    pub expected_baseline_version: Option<i64>,
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
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Approval command composite.  The service has already validated the exact
/// persisted review target and interactive-user authorization; this record
/// keeps those claims in the same transaction as the baseline lifecycle and
/// command receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveProjectExecutionBaselineCommand {
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
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Activation command composite.  The paired milestone/definition manifests
/// are supplied by the service after it has revalidated the frozen manifest;
/// SQLite rechecks ownership/currentness while holding its write lock before
/// promoting milestones and Task governance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateProjectExecutionBaselineCommand {
    pub project_id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub approval_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub charter_revision_id: String,
    pub milestone_ids: Vec<String>,
    pub milestone_definition_revision_ids: Vec<String>,
    pub primary_milestone_id: Option<String>,
    pub content_digest: String,
    pub rendered_digest: String,
    pub idempotency_key: String,
    pub updated_at: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Atomic "approve plan and start work" command composite (D18, F13).  The
/// service has already validated the exact persisted proposed review target
/// and interactive-user authorization; this record commits the approval,
/// activation, milestone/Task-governance promotion, durable events, and one
/// command receipt in a single transaction. Only a freshly proposed baseline
/// (never a re-approval of an already-active one) may use this command; the
/// already-approved "Start approved work" gesture keeps using the separate
/// exact replay-safe `activate_project_execution_baseline_command`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApproveAndActivateProjectExecutionBaselineCommand {
    pub project_id: String,
    pub baseline_id: String,
    pub revision_id: String,
    pub approval_id: String,
    pub expected_baseline_version: i64,
    pub expected_project_version: i64,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub charter_revision_id: String,
    pub milestone_ids: Vec<String>,
    pub milestone_definition_revision_ids: Vec<String>,
    pub primary_milestone_id: Option<String>,
    pub idempotency_key: String,
    pub created_at: String,
    pub updated_at: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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

/// Command-aware first milestone definition.  The shell, first immutable
/// definition, domain event, and optional command finalization are committed
/// by one SQLite transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectMilestoneCommand {
    pub milestone: CreateProjectMilestone,
    pub revision: CreateProjectMilestoneRevision,
    /// Allocate the next Project-local milestone sequence while holding the
    /// command transaction's Project write lock.  Transport-neutral command
    /// services set this for new milestones so concurrent distinct commands
    /// cannot race on a pre-read `MAX(sequence) + 1` value.
    pub allocate_project_sequence: bool,
    /// Acceptance-check definitions materialized with a non-draft revision.
    /// These rows are part of the same receipt/event transaction.
    pub check_definitions: Vec<CreateProjectMilestoneCheck>,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Command-aware append of one immutable milestone definition revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendProjectMilestoneRevisionCommand {
    pub revision: CreateProjectMilestoneRevision,
    /// Acceptance-check definitions materialized with a non-draft revision.
    /// Draft revisions intentionally leave the current check projection
    /// unchanged until they are proposed.
    pub check_definitions: Vec<CreateProjectMilestoneCheck>,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Command-aware update of the Project's explicit primary milestone pointer.
/// `primary_milestone_id = None` is allowed only when no active milestone
/// remains, matching the authoritative Project lifecycle rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetPrimaryProjectMilestoneCommand {
    pub project_id: String,
    pub primary_milestone_id: Option<String>,
    pub expected_project_version: i64,
    pub principal_type: String,
    pub principal_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub explicit_event: String,
    pub idempotency_key: String,
    pub updated_at: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
    pub blocker_projection_json: String,
    pub stale_projection_json: String,
    pub reconciliation_projection_json: String,
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

/// Command-aware readiness evaluation result.  The pure readiness evaluator
/// lives above the database; this input contains its complete frozen output
/// and this wrapper gives the persistence layer one atomic command boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReadinessSnapshotCommand {
    pub snapshot: CreateProjectReadinessSnapshot,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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

/// Command-aware immutable release manifest creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseCommand {
    pub release: CreateProjectRelease,
    pub references: Vec<CreateProjectReleaseReference>,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// A Project Agent release-candidate request is not itself an authoritative
/// release manifest.  It is an immutable domain event which records the exact
/// ready snapshot presented for user approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseRequest {
    pub event_id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub status: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReleaseRequestRecord {
    pub event_id: String,
    pub project_id: String,
    pub milestone_id: String,
    pub expected_milestone_version: i64,
    pub readiness_snapshot_id: String,
    pub readiness_digest: String,
    pub status: String,
    pub idempotency_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectReleaseRequestCommand {
    pub request: CreateProjectReleaseRequest,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// The immutable governance projection written alongside a Task proposal.
/// Services derive these values from the current Project Charter/baseline;
/// SQLite owns the final scope, runnable, and uniqueness checks while holding
/// the command write lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectTaskGovernance {
    pub task_id: String,
    pub project_id: String,
    pub charter_revision_id: Option<String>,
    pub baseline_id: Option<String>,
    pub baseline_revision_id: Option<String>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub document_revisions_json: String,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    pub runnable: bool,
    pub replacement_of_task_id: Option<String>,
    pub provenance_json: String,
    pub created_at: String,
    pub updated_at: String,
}

/// Atomic Task proposal composite.  The Task row, optional immutable
/// governance projection, prerequisite dependencies, explicit/default role
/// assignments, durable `task.created` event, command receipt, and optional
/// AgentAction execution are committed by one SQLite transaction. The command
/// receipt is optional only for repository-level characterization callers;
/// the service command always supplies it.
#[derive(Debug, Clone)]
pub struct CreateTaskProposalCommand {
    pub task: CreateTask,
    pub governance: Option<CreateProjectTaskGovernance>,
    pub role_assignments: Vec<CreateTaskRoleAssignment>,
    pub depends_on_task_ids: Vec<String>,
    pub metadata_json: Option<String>,
    /// Present only for an approval/audit-backed execution. Directly allowed
    /// commands intentionally leave these unset and therefore cannot create
    /// an AgentActionExecution row.
    pub source_action_id: Option<String>,
    pub expected_action_version: Option<i64>,
    /// Frozen authorization provenance shared by action-backed and direct
    /// transports. SQLite rechecks the mutable binding/action facts under
    /// BEGIN IMMEDIATE before inserting the Task.
    pub source_actor_identity_id: String,
    pub source_scope_type: String,
    pub source_scope_id: String,
    pub source_target_type: Option<String>,
    pub source_target_id: Option<String>,
    pub source_operation: String,
    pub source_requested_permission: String,
    pub source_policy_result: String,
    pub source_policy_revision: Option<String>,
    pub source_policy_digest: Option<String>,
    pub source_payload_hash: String,
    pub executor_type: String,
    pub executor_id: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// One bounded child payload for an adaptive Task split.  The payload is
/// intentionally smaller than `CreateTask`: adaptive reshaping inherits the
/// source Task's repository, workflow, governance, capability, and risk
/// facts.  Callers can only supply the child text (and the historical
/// optional assignee hint).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveTaskChild {
    pub title: String,
    pub description: Option<String>,
    pub assignee_id: Option<String>,
}

/// The closed set of adaptive Task mutations.  Keeping split/sequence/
/// replace in one enum prevents transport adapters from reaching separate
/// persistence paths with subtly different governance checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptiveTaskOperation {
    Split {
        items: Vec<AdaptiveTaskChild>,
    },
    Sequence {
        ordered_task_ids: Vec<String>,
    },
    Replace {
        title: String,
        description: Option<String>,
    },
}

impl AdaptiveTaskOperation {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Split { .. } => "split",
            Self::Sequence { .. } => "sequence",
            Self::Replace { .. } => "replace",
        }
    }
}

/// Transactional command input for bounded Task reshaping.  The receipt is
/// the command identity and replay boundary; the database repeats every
/// mutable baseline/envelope/CAS check while holding `BEGIN IMMEDIATE`.
#[derive(Debug, Clone)]
pub struct ApplyAdaptiveTaskCommand {
    pub project_id: String,
    pub source_task_id: String,
    pub expected_task_version: i64,
    pub expected_board_revision: i64,
    pub operation: AdaptiveTaskOperation,
    pub rationale: String,
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
}

/// Frozen result of one adaptive command.  On replay, all Task snapshots and
/// the board revision come from the immutable receipt outcome rather than
/// from mutable live rows.
#[derive(Debug, Clone)]
pub struct AppliedAdaptiveTaskCommand {
    pub source_task: Task,
    pub tasks: Vec<Task>,
    pub board_revision: i64,
    pub receipt: CommandReceipt,
    pub replayed: bool,
}

/// The task snapshot returned by the command composite.  It is intentionally
/// separate from the live Task query: a replay can return the exact frozen
/// receipt snapshot even if a later workflow transition changed the row.
#[derive(Debug, Clone)]
pub struct CreatedTaskProposal {
    pub task: crate::Task,
    pub replayed: bool,
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
    /// Operation identity allocated with the Genesis Project-create
    /// composite so a post-commit process stop still leaves recoverable
    /// provisioning work under a stable row id.
    pub provisioning_operation_id: String,
    pub policy_revision: String,
    pub policy_digest: String,
    pub member_id: String,
    /// Optional Main command finalization. When present, the action outcome
    /// and command receipt are committed with the existing Project/Chat/
    /// binding/handoff transaction rather than in a post-commit path.
    pub command_receipt: Option<CreateCommandReceipt>,
    pub action_execution: Option<CreateAgentActionExecution>,
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
    /// Atomically apply one bounded split/sequence/replace Task command.
    /// Implementations must resolve the command receipt before mutable reads,
    /// then perform the exact adaptive-baseline gate, source Task/version CAS,
    /// board CAS, governance insertion, event, and receipt in one writer
    /// transaction.
    async fn apply_adaptive_task_command(
        &self,
        input: ApplyAdaptiveTaskCommand,
    ) -> Result<AppliedAdaptiveTaskCommand> {
        let _ = input;
        Err(crate::DbError::Check(
            "Adaptive Task command persistence is not wired".to_owned(),
        ))
    }
    /// Insert the immutable Task governance projection through the same
    /// scope/runnable/replacement checks used by adaptive materialization.
    /// Callers must already own the surrounding writer transaction.
    async fn insert_project_task_governance_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        governance: CreateProjectTaskGovernance,
    ) -> Result<()> {
        let _ = (transaction, governance);
        Err(crate::DbError::Check(
            "Task governance persistence is not wired".to_owned(),
        ))
    }
    /// Atomically materialize a `task.propose` command.  The implementation
    /// must resolve the command receipt before applying current authorization
    /// or lifecycle checks so a response-loss retry is replay-exact.
    async fn create_task_proposal_command(
        &self,
        input: CreateTaskProposalCommand,
    ) -> Result<CreatedTaskProposal> {
        let _ = input;
        Err(crate::DbError::Check(
            "Task proposal command persistence is not wired".to_owned(),
        ))
    }
    async fn get_project_charter(&self, id: &str) -> Result<Option<ProjectCharterRecord>>;
    /// The adoption Charter a Project already owns, if any. A Project holds at
    /// most one Charter (`idx_project_charter_project`), so this resolves the
    /// real primary key without trusting an id an agent proposed.
    async fn get_project_adoption_charter(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectCharterRecord>>;
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
    async fn finalize_project_charter_revision_noop(
        &self,
        input: FinalizeProjectCharterRevisionNoop,
    ) -> Result<ProjectCharterRevisionRecord>;
    async fn get_project_charter_approval(
        &self,
        id: &str,
    ) -> Result<Option<ProjectCharterApprovalRecord>>;
    async fn approve_project_charter(
        &self,
        input: ApproveProjectCharter,
    ) -> Result<ProjectCharterApprovalRecord>;
    /// Atomically apply a user-approved adoption or amendment to an existing
    /// Project.  Genesis `project_creation` approvals intentionally continue
    /// through the separate Project-create composite below.
    async fn apply_project_charter_approval_command(
        &self,
        input: ApplyProjectCharterApprovalCommand,
    ) -> Result<AppliedProjectCharterApprovalRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project Charter adoption/amendment command persistence is not wired".to_owned(),
        ))
    }
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
    async fn get_project_reconciliation_resolution(
        &self,
        id: &str,
    ) -> Result<Option<ProjectReconciliationResolutionRecord>>;

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
    /// Execute Project CAS, shell creation, the shell-created domain event,
    /// and optional command finalization in one transaction.
    async fn create_project_document_shell_command(
        &self,
        input: CreateProjectDocumentShellCommand,
    ) -> Result<ProjectDocumentRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document command persistence is not wired".to_owned(),
        ))
    }
    /// Execute first-Document creation, its first immutable revision, the
    /// domain event, and optional command finalization in one transaction.
    async fn create_project_document_command(
        &self,
        input: CreateProjectDocumentCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document command persistence is not wired".to_owned(),
        ))
    }
    /// Append one immutable Document revision and finalize its command in the
    /// same transaction as the Document pointer/version update and event.
    async fn append_project_document_revision_command(
        &self,
        input: AppendProjectDocumentRevisionCommand,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document command persistence is not wired".to_owned(),
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
    async fn approve_project_document_command(
        &self,
        input: ApproveProjectDocumentCommand,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project document command persistence is not wired".to_owned(),
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
    async fn create_project_decision_candidate_command(
        &self,
        input: CreateProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision command persistence is not wired".to_owned(),
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
    async fn append_project_decision_command(
        &self,
        input: AppendProjectDecisionCommand,
    ) -> Result<ProjectDecisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision command persistence is not wired".to_owned(),
        ))
    }
    async fn approve_project_decision_candidate_command(
        &self,
        input: ApproveProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision candidate command persistence is not wired".to_owned(),
        ))
    }
    async fn reject_project_decision_candidate_command(
        &self,
        input: RejectProjectDecisionCandidateCommand,
    ) -> Result<ProjectDecisionCandidateRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Project decision candidate command persistence is not wired".to_owned(),
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
    /// Resolve the Project's authoritative baseline projection target. The
    /// lifecycle ordering is part of the repository query contract so every
    /// transport observes the same current candidate.
    async fn get_project_execution_baseline_for_project(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectExecutionBaselineRecord>> {
        let _ = project_id;
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
    async fn list_project_execution_baseline_revisions(
        &self,
        baseline_id: &str,
    ) -> Result<Vec<ProjectExecutionBaselineRevisionRecord>> {
        let _ = baseline_id;
        Err(crate::DbError::Check(
            "Execution baseline persistence is not wired".to_owned(),
        ))
    }
    async fn list_project_execution_baseline_approvals(
        &self,
        baseline_id: &str,
    ) -> Result<Vec<ProjectExecutionBaselineApprovalRecord>> {
        let _ = baseline_id;
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
    /// Atomically append a draft baseline revision (and, for a first draft,
    /// its server-minted baseline shell) with its durable event/receipt.
    async fn save_project_execution_baseline_draft_command(
        &self,
        input: SaveProjectExecutionBaselineRevisionCommand,
    ) -> Result<ProjectExecutionBaselineRevisionRecord>;
    /// Atomically append a complete proposed baseline revision with its
    /// durable event/receipt. The service has already applied the stricter
    /// approval-target validation; the repository repeats CAS/scope guards.
    async fn propose_project_execution_baseline_command(
        &self,
        input: SaveProjectExecutionBaselineRevisionCommand,
    ) -> Result<ProjectExecutionBaselineRevisionRecord>;
    /// Atomically persist the exact user approval receipt and advance the
    /// baseline's approval projection.
    async fn approve_project_execution_baseline_command(
        &self,
        input: ApproveProjectExecutionBaselineCommand,
    ) -> Result<ProjectExecutionBaselineApprovalRecord>;
    /// Atomically consume the exact approval receipt, activate the baseline,
    /// promote milestones/Task governance, and append event/command receipt.
    async fn activate_project_execution_baseline_command(
        &self,
        input: ActivateProjectExecutionBaselineCommand,
    ) -> Result<ProjectExecutionBaselineRecord>;
    /// Atomically approve and activate the exact freshly proposed revision in
    /// one transaction/receipt: approval, activation, milestone/Task
    /// governance promotion, and both durable events commit or roll back
    /// together (D18, F13).
    async fn approve_and_activate_project_execution_baseline_command(
        &self,
        input: ApproveAndActivateProjectExecutionBaselineCommand,
    ) -> Result<ProjectExecutionBaselineRecord>;

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
    async fn create_project_milestone_command(
        &self,
        input: CreateProjectMilestoneCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone command persistence is not wired".to_owned(),
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
    async fn append_project_milestone_revision_command(
        &self,
        input: AppendProjectMilestoneRevisionCommand,
    ) -> Result<ProjectMilestoneRevisionRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone command persistence is not wired".to_owned(),
        ))
    }
    async fn set_primary_project_milestone_command(
        &self,
        input: SetPrimaryProjectMilestoneCommand,
    ) -> Result<Project> {
        let _ = input;
        Err(crate::DbError::Check(
            "Milestone command persistence is not wired".to_owned(),
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
    async fn create_project_readiness_snapshot_command(
        &self,
        input: CreateProjectReadinessSnapshotCommand,
    ) -> Result<ProjectReadinessSnapshotRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Readiness command persistence is not wired".to_owned(),
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
    async fn create_project_release_command(
        &self,
        input: CreateProjectReleaseCommand,
    ) -> Result<ProjectReleaseRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Release command persistence is not wired".to_owned(),
        ))
    }
    async fn create_project_release_request_command(
        &self,
        input: CreateProjectReleaseRequestCommand,
    ) -> Result<ProjectReleaseRequestRecord> {
        let _ = input;
        Err(crate::DbError::Check(
            "Release request command persistence is not wired".to_owned(),
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
