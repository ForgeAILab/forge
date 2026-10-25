//! Shared Project artifact command boundary.
//!
//! This module is intentionally transport neutral.  REST supplies an
//! authenticated user authorization and native Project Agent execution can
//! supply an `AgentAction` provenance, but both paths use the same canonical
//! Project-scoped command context and the same database composite.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{DocumentRevisionLifecycle, ProjectDocumentContent, RevisionProvenance};
use db::{
    new_uuid_v4, now_rfc3339, AgentActionExecutionStatus, AgentActionStatus,
    AppendProjectDocumentRevisionCommand, ApproveProjectDocument, ApproveProjectDocumentCommand,
    CommandReceipt, CommandReceiptRepo, CreateAgentActionExecution, CreateCommandReceipt,
    CreateProjectDocument, CreateProjectDocumentCommand, CreateProjectDocumentRevision,
    CreateProjectDocumentShellCommand, CreateProjectMediaAttachment,
    CreateProjectMediaAttachmentMutation, ProjectDocumentApprovalRecord, ProjectDocumentRecord,
    ProjectDocumentRevisionRecord, ProjectMediaAttachment, ProjectMemberRepo,
    ProjectOrchestrationRepo, ProjectRepo, SharedMediaRepo, SqliteDb,
};
use serde::{ser::SerializeStruct, Serialize};
use serde_json::{json, Value};

use crate::{
    AgentActionProvenance, CommandContext, CommandPrincipal, CommandScope, CommandScopeType,
    ExpectedCommandState, NewCommandContext, Result, ServiceError,
};

pub const PROJECT_EVIDENCE_COMMAND: &str = "project.evidence";
pub const PROJECT_VALIDATION_COMMAND: &str = "project.validation";

/// The authorization information that is common to all transports.  The
/// adapter is responsible for authenticating this data; this service owns its
/// canonical inclusion in the command digest and receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCommandAuthorization {
    pub principal_type: String,
    pub principal_id: String,
    pub policy_result: String,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub requested_permission: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub authorization_event_id: String,
    pub authorization_basis: String,
    pub authorization_action: String,
    pub authorization_occurred_at: String,
    pub authorization_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectEvidenceCommand {
    pub project_id: String,
    pub milestone_id: String,
    pub asset_id: String,
    pub task_id: Option<String>,
    pub source_run_id: Option<String>,
    pub source_validation_id: Option<String>,
    pub acceptance_check_ids: Vec<String>,
    pub caption: String,
    pub evidence_kind: String,
    pub checksum: String,
    pub expected_milestone_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

/// One agent-observed acceptance-check result. Integrated behaviour is what a
/// milestone check actually asserts, and that is wider than the Task under
/// review: a check can cover a feature delivered earlier that this Task's work
/// has to keep working. So the observation is recorded as its own authorized
/// command rather than derived from a single Task's review.
#[derive(Debug, Clone, Serialize)]
pub struct ProjectValidationCommand {
    pub project_id: String,
    pub milestone_id: String,
    pub check_id: String,
    pub definition_revision_id: String,
    pub status: String,
    pub result: String,
    pub input_digest: String,
    /// The Task whose run produced this observation, when a Task produced it.
    /// Absent when the Project Agent exercised the software itself in its own
    /// workspace checkout. When present it is verified, so a named Task is
    /// always a Task that really ran.
    pub observed_task_id: Option<String>,
    /// Observations of commands this Agent ran itself in its verification
    /// checkout (`forge_task_command` returns each id). A `task_validation`
    /// pass or fail recorded by an Agent must cite at least one that is newer
    /// than the delivered work; a Task's report never settles the check.
    pub observed_command_ids: Vec<String>,
    /// Optional artifact captured by that Task through `task.evidence`.
    pub evidence_asset_id: Option<String>,
    pub expected_milestone_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDocumentRevisionCommand {
    pub project_id: String,
    pub document_id: String,
    pub kind: Option<String>,
    pub title: Option<String>,
    pub approval_policy: Option<String>,
    pub base_revision_id: Option<String>,
    pub lifecycle: DocumentRevisionLifecycle,
    pub content: ProjectDocumentContent,
    pub change_summary: String,
    pub provenance: RevisionProvenance,
    pub expected_document_version: i64,
    pub expected_digest: Option<String>,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDocumentCreateCommand {
    pub project_id: String,
    pub kind: String,
    pub title: String,
    pub approval_policy: String,
    pub expected_project_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectDocumentApprovalCommand {
    pub project_id: String,
    pub document_id: String,
    pub revision_id: String,
    pub content_digest: String,
    pub rendered_digest: String,
    pub expected_document_version: i64,
    pub idempotency_key: String,
    pub authorization: ProjectCommandAuthorization,
}

impl Serialize for ProjectEvidenceCommand {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Authorization is part of the command identity.  Keep a dedicated
        // implementation so its shape cannot accidentally depend on a REST
        // request envelope or native tool payload.
        let mut state = serializer.serialize_struct("ProjectEvidenceCommand", 13)?;
        state.serialize_field("project_id", &self.project_id)?;
        state.serialize_field("milestone_id", &self.milestone_id)?;
        state.serialize_field("asset_id", &self.asset_id)?;
        state.serialize_field("task_id", &self.task_id)?;
        state.serialize_field("source_run_id", &self.source_run_id)?;
        state.serialize_field("source_validation_id", &self.source_validation_id)?;
        state.serialize_field("acceptance_check_ids", &self.acceptance_check_ids)?;
        state.serialize_field("caption", &self.caption)?;
        state.serialize_field("evidence_kind", &self.evidence_kind)?;
        state.serialize_field("checksum", &self.checksum)?;
        state.serialize_field(
            "expected_milestone_version",
            &self.expected_milestone_version,
        )?;
        state.serialize_field("idempotency_key", &self.idempotency_key)?;
        state.serialize_field("authorization", &self.authorization)?;
        state.end()
    }
}

impl Serialize for ProjectCommandAuthorization {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ProjectCommandAuthorization", 14)?;
        state.serialize_field("principal_type", &self.principal_type)?;
        state.serialize_field("principal_id", &self.principal_id)?;
        state.serialize_field("policy_result", &self.policy_result)?;
        state.serialize_field("policy_revision", &self.policy_revision)?;
        state.serialize_field("policy_digest", &self.policy_digest)?;
        state.serialize_field("requested_permission", &self.requested_permission)?;
        state.serialize_field("correlation_id", &self.correlation_id)?;
        state.serialize_field("causation_id", &self.causation_id)?;
        state.serialize_field("causation_depth", &self.causation_depth)?;
        state.serialize_field("authorization_event_id", &self.authorization_event_id)?;
        state.serialize_field("authorization_basis", &self.authorization_basis)?;
        state.serialize_field("authorization_action", &self.authorization_action)?;
        state.serialize_field("authorization_occurred_at", &self.authorization_occurred_at)?;
        state.serialize_field("authorization_json", &self.authorization_json)?;
        state.end()
    }
}

#[derive(Clone)]
pub struct ProjectArtifactCommandService {
    db: Arc<SqliteDb>,
}

fn validate_document_scope(document: &db::ProjectDocumentRecord, project_id: &str) -> Result<()> {
    if document.project_id != project_id {
        return Err(ServiceError::invalid_operation(
            "Project Document command crosses Project scope",
        ));
    }
    Ok(())
}

fn validate_document_create_command(command: &ProjectDocumentCreateCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.kind.trim().is_empty()
        || command.title.trim().is_empty()
        || command.approval_policy.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.correlation_id.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || command
            .authorization
            .authorization_occurred_at
            .trim()
            .is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Project Document create command has incomplete identity or authorization",
        ));
    }
    if command.authorization.authorization_action != "project.document.create" {
        return Err(ServiceError::invalid_operation(
            "Project Document create authorization action is invalid",
        ));
    }
    if command.expected_project_version < 1 {
        return Err(ServiceError::invalid_operation(
            "expected_project_version must be positive",
        ));
    }
    if crate::parse_document_kind(&command.kind).is_none() {
        return Err(ServiceError::invalid_operation(
            "Project Document kind is invalid",
        ));
    }
    if !matches!(
        command.approval_policy.as_str(),
        "none" | "project_agent" | "user" | "user_or_project_agent"
    ) {
        return Err(ServiceError::invalid_operation(
            "Project Document approval policy is invalid",
        ));
    }
    Ok(())
}

fn validate_document_context(
    context: &CommandContext,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    if !matches!(
        context.operation(),
        "project.document" | "project.document.create"
    ) || context.canonical_scope().scope_type() != CommandScopeType::Project
        || context.canonical_scope().scope_id() != project_id
        || context.principal().principal_type() != authorization.principal_type
        || context.principal().principal_id() != authorization.principal_id
    {
        return Err(ServiceError::invalid_operation(
            "Project Document command context does not match its authorization",
        ));
    }
    Ok(())
}

fn validate_document_revision_command(command: &ProjectDocumentRevisionCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.document_id.trim().is_empty()
        || command.change_summary.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.correlation_id.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || command
            .authorization
            .authorization_occurred_at
            .trim()
            .is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Project Document revision command has incomplete identity or authorization",
        ));
    }
    if command.authorization.authorization_action != "project.document.revision.save" {
        return Err(ServiceError::invalid_operation(
            "Project Document revision authorization action is invalid",
        ));
    }
    if command.expected_document_version < 1 {
        return Err(ServiceError::invalid_operation(
            "expected_document_version must be positive",
        ));
    }
    if !matches!(
        command.lifecycle,
        DocumentRevisionLifecycle::Draft | DocumentRevisionLifecycle::Proposed
    ) {
        return Err(ServiceError::invalid_operation(
            "a new Project Document revision must be draft or proposed",
        ));
    }
    Ok(())
}

fn validate_document_approval_command(command: &ProjectDocumentApprovalCommand) -> Result<()> {
    if command.project_id.trim().is_empty()
        || command.document_id.trim().is_empty()
        || command.revision_id.trim().is_empty()
        || command.content_digest.trim().is_empty()
        || command.rendered_digest.trim().is_empty()
        || command.idempotency_key.trim().is_empty()
        || command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.correlation_id.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || command
            .authorization
            .authorization_occurred_at
            .trim()
            .is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Project Document approval command has incomplete identity or authorization",
        ));
    }
    if command.authorization.authorization_action != "project.document.approve" {
        return Err(ServiceError::invalid_operation(
            "Project Document approval authorization action is invalid",
        ));
    }
    if command.expected_document_version < 1 {
        return Err(ServiceError::invalid_operation(
            "expected_document_version must be positive",
        ));
    }
    Ok(())
}

fn effective_document_lifecycle(
    document: &ProjectDocumentRecord,
    lifecycle: DocumentRevisionLifecycle,
) -> Result<&'static str> {
    if document.approval_policy == "none" {
        return Ok("approved");
    }
    match lifecycle {
        DocumentRevisionLifecycle::Draft => Ok("draft"),
        DocumentRevisionLifecycle::Proposed => Ok("proposed"),
        _ => Err(ServiceError::invalid_operation(
            "a new Project Document revision must be draft or proposed",
        )),
    }
}

fn validate_document_approval_policy(
    document: &ProjectDocumentRecord,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    match authorization.principal_type.as_str() {
        "user" if document.approval_policy == "none" => Err(ServiceError::conflict(
            "this Project Document does not have an approval policy",
        )),
        "user" if document.approval_policy == "project_agent" => {
            Err(ServiceError::AuthorizationDenied {
                message: "this Project Document requires the bound Project Agent".to_owned(),
            })
        }
        "agent"
            if matches!(
                document.approval_policy.as_str(),
                "project_agent" | "user_or_project_agent"
            ) =>
        {
            Ok(())
        }
        "agent" => Err(ServiceError::AuthorizationDenied {
            message: "Project Agent cannot approve this Project Document".to_owned(),
        }),
        "user" => Ok(()),
        _ => Err(ServiceError::AuthorizationDenied {
            message: "Project Document approvals accept only user or Project Agent principals"
                .to_owned(),
        }),
    }
}

fn command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let mut receipt = create_receipt(context, outcome_json);
    let execution = action_execution(context, outcome_json);
    if let Some(execution) = execution.as_ref() {
        receipt.agent_action_execution_id = Some(execution.id.clone());
    }
    (receipt, execution)
}

fn result_value(outcome_json: &str, field: &str) -> Result<String> {
    let outcome: Value = serde_json::from_str(outcome_json).map_err(|_| {
        ServiceError::Conflict("Project Document receipt outcome is invalid".to_owned())
    })?;
    outcome
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::Conflict(format!("Project Document receipt has no {field}")))
}

fn result_i64(outcome: &Value, field: &str) -> Result<i64> {
    outcome
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::Conflict(format!("Project Document receipt has no {field}")))
}

fn result_optional_string(outcome: &Value, field: &str) -> Result<Option<String>> {
    match outcome.get(field) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        _ => Err(ServiceError::Conflict(format!(
            "Project Document receipt has invalid {field}"
        ))),
    }
}

async fn authorize_project_principal(
    db: &SqliteDb,
    project_id: &str,
    authorization: &ProjectCommandAuthorization,
) -> Result<()> {
    match authorization.principal_type.as_str() {
        "user" => {
            let project = ProjectRepo::get_by_id(db, project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", project_id))?;
            if project.owner_id.as_deref() == Some(authorization.principal_id.as_str())
                || ProjectMemberRepo::get_member(db, project_id, &authorization.principal_id)
                    .await?
                    .is_some()
            {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "principal is not a member of the Project".to_owned(),
                })
            }
        }
        "agent" => {
            let bound: Option<String> = db
                .artifact_authorized_project_identity(project_id, &authorization.principal_id)
                .await?;
            if bound.is_some() {
                Ok(())
            } else {
                Err(ServiceError::AuthorizationDenied {
                    message: "agent principal is not actively bound to the Project".to_owned(),
                })
            }
        }
        _ => Err(ServiceError::AuthorizationDenied {
            message: "Project artifact commands accept only user or Project Agent principals"
                .to_owned(),
        }),
    }
}

async fn load_project_media_attachment(
    db: &SqliteDb,
    attachment_id: &str,
) -> Result<Option<ProjectMediaAttachment>> {
    let row = db.evidence_attachment_source(attachment_id).await?;
    let attachment = row
        .map(
            |row| -> std::result::Result<ProjectMediaAttachment, db::DbError> {
                Ok(ProjectMediaAttachment {
                    id: row.id()?,
                    project_id: row.project_id()?,
                    asset_id: row.asset_id()?,
                    attachment_kind: row.attachment_kind()?,
                    task_media_id: row.task_media_id()?,
                    task_id: row.task_id()?,
                    milestone_id: row.milestone_id()?,
                    milestone_check_id: row.milestone_check_id()?,
                    source_task_id: row.source_task_id()?,
                    source_execution_id: row.source_execution_id()?,
                    source_validation_id: row.source_validation_id()?,
                    source_task_version: row.source_task_version()?,
                    source_context_digest: row.source_context_digest()?,
                    source_definition_revision_id: row.source_definition_revision_id()?,
                    acceptance_check_ids_json: row.acceptance_check_ids_json()?,
                    caption: row.caption()?,
                    evidence_kind: row.evidence_kind()?,
                    checksum: row.checksum()?,
                    availability: row.availability()?,
                    project_url: row.project_url()?,
                    author_type: row.author_type()?,
                    author_id: row.author_id()?,
                    authorization_json: row.authorization_json()?,
                    version: row.version()?,
                    created_at: row.created_at()?,
                    deleted_at: row.deleted_at()?,
                    updated_at: row.updated_at()?,
                })
            },
        )
        .transpose()?;
    Ok(attachment)
}

async fn load_document_approval(
    db: &SqliteDb,
    approval_id: &str,
) -> Result<Option<ProjectDocumentApprovalRecord>> {
    let row = db.artifact_document_approval_source(approval_id).await?;
    let approval = row
        .map(
            |row| -> std::result::Result<ProjectDocumentApprovalRecord, db::DbError> {
                Ok(ProjectDocumentApprovalRecord {
                    id: row.id()?,
                    document_id: row.document_id()?,
                    revision_id: row.revision_id()?,
                    principal_type: row.principal_type()?,
                    principal_id: row.principal_id()?,
                    authorization_basis: row.authorization_basis()?,
                    authorization_action: row.authorization_action()?,
                    explicit_event: row.explicit_event()?,
                    authorization_occurred_at: row.authorization_occurred_at()?,
                    content_digest: row.content_digest()?,
                    rendered_digest: row.rendered_digest()?,
                    lifecycle: row.lifecycle()?,
                    idempotency_key: row.idempotency_key()?,
                    version: row.version()?,
                    created_at: row.created_at()?,
                    updated_at: row.updated_at()?,
                })
            },
        )
        .transpose()?;
    Ok(approval)
}

// The full command identity is intentionally explicit at this boundary.
#[allow(clippy::too_many_arguments)]
fn command_context_for<T: Serialize>(
    operation: &str,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
    input: &T,
    action: Option<AgentActionProvenance>,
    project_id: &str,
    expected_version: i64,
    digest_key: &str,
) -> Result<CommandContext> {
    if idempotency_key.trim().is_empty()
        || authorization.principal_type.trim().is_empty()
        || authorization.principal_id.trim().is_empty()
        || authorization.correlation_id.trim().is_empty()
    {
        return Err(ServiceError::invalid_operation(
            "Project command authorization provenance is incomplete",
        ));
    }
    let context = command_context_for_unvalidated(
        operation,
        idempotency_key,
        authorization,
        input,
        action,
        project_id,
        expected_version,
        digest_key,
    )?;
    Ok(context)
}

#[allow(clippy::too_many_arguments)]
fn command_context_for_unvalidated<T: Serialize>(
    operation: &str,
    idempotency_key: &str,
    authorization: &ProjectCommandAuthorization,
    input: &T,
    action: Option<AgentActionProvenance>,
    project_id: &str,
    expected_version: i64,
    digest_key: &str,
) -> Result<CommandContext> {
    let context = NewCommandContext {
        principal: CommandPrincipal {
            principal_type: authorization.principal_type.clone(),
            principal_id: authorization.principal_id.clone(),
        },
        canonical_scope: CommandScope {
            scope_type: CommandScopeType::Project,
            scope_id: project_id.to_owned(),
        },
        operation: operation.to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        expected_state: ExpectedCommandState {
            versions: BTreeMap::from([(digest_key.to_owned(), expected_version)]),
            digests: BTreeMap::new(),
        },
        authorization_provenance: Some(crate::AuthorizationProvenance {
            policy_result: authorization.policy_result.clone(),
            policy_revision: authorization.policy_revision.clone(),
            policy_digest: authorization.policy_digest.clone(),
            requested_permission: authorization.requested_permission.clone(),
        }),
        action_provenance: action,
        correlation_id: authorization.correlation_id.clone(),
        causation_id: authorization.causation_id.clone(),
        causation_depth: authorization.causation_depth,
    };
    CommandContext::from_authorized_input(context, input).map_err(|error| {
        ServiceError::invalid_operation(format!("Project command digest: {error}"))
    })
}

fn action_execution(
    context: &CommandContext,
    outcome_json: &str,
) -> Option<CreateAgentActionExecution> {
    context.action_provenance.as_ref().map(|provenance| {
        let now = now_rfc3339();
        CreateAgentActionExecution {
            id: new_uuid_v4(),
            action_id: provenance.action_id.clone(),
            expected_action_version: provenance.expected_action_version,
            attempt: provenance.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(outcome_json.to_owned()),
            error: None,
            executed_by_type: provenance.executed_by_type.clone(),
            executed_by_id: provenance.executed_by_id.clone(),
            idempotency_key: provenance.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(outcome_json.to_owned()),
            created_at: now.clone(),
            completed_at: Some(now.clone()),
            updated_at: now,
        }
    })
}

/// Translate the acceptance-check status vocabulary an Agent records into the
/// persisted outcome vocabulary readiness reads back. This mirrors the manual
/// attestation route's mapping so a human attestation and an agent-recorded
/// validation of the same observation persist identically. `pending` and
/// `waived` are absent on purpose: an Agent records what it observed, and a
/// waiver is the user's alone.
fn validation_outcome(status: &str) -> Option<&'static str> {
    match status {
        "pass" => Some("passed"),
        "fail" => Some("failed"),
        "blocked" | "unavailable" => Some("missing"),
        "stale" => Some("stale"),
        _ => None,
    }
}

/// Every field readiness reads back out of a validation result, in one place.
///
/// `milestone_runtime` re-derives the governing Charter, definition
/// revision, and release policy and rejects the result as "stale for the active
/// authority" when any of them is absent or different. That makes this manifest
/// a contract with the reader, not a free-form provenance note: a field the
/// user attestation route writes and this one omits does not merely lose
/// detail, it makes every result recorded here permanently unusable.
struct ValidationManifestInputs<'a> {
    result: &'a str,
    governing_charter_revision_id: &'a str,
    check_definition_revision_id: &'a str,
    principal_type: &'a str,
    principal_id: &'a str,
    observed_task_id: Option<&'a str>,
    evidence_asset_id: Option<&'a str>,
    command_observations: &'a [Value],
}

fn validation_source_manifest(inputs: ValidationManifestInputs<'_>) -> Value {
    json!({
        "result": inputs.result,
        // Readiness compares this against the approved Charter revision it
        // re-derives. The Charter is the whole governing authority, and the
        // Charter alone is what the observation is bound to.
        "governing_revision_ids": [inputs.governing_charter_revision_id],
        "check_definition_revision_id": inputs.check_definition_revision_id,
        "observed_by": {
            "kind": inputs.principal_type,
            "id": inputs.principal_id,
            "task_id": inputs.observed_task_id,
            "evidence_asset_id": inputs.evidence_asset_id,
            "command_observations": inputs.command_observations,
        },
    })
}

fn validate_evidence_command(command: &ProjectEvidenceCommand) -> Result<()> {
    for (field, value) in [
        ("project_id", command.project_id.as_str()),
        ("milestone_id", command.milestone_id.as_str()),
        ("asset_id", command.asset_id.as_str()),
        ("caption", command.caption.as_str()),
        ("evidence_kind", command.evidence_kind.as_str()),
        ("checksum", command.checksum.as_str()),
        ("idempotency_key", command.idempotency_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "{field} is required"
            )));
        }
    }
    if command.expected_milestone_version < 1 {
        return Err(ServiceError::invalid_operation(
            "expected_milestone_version must be a positive integer",
        ));
    }
    if command.authorization.principal_type.trim().is_empty()
        || command.authorization.principal_id.trim().is_empty()
        || command.authorization.correlation_id.trim().is_empty()
        || command
            .authorization
            .authorization_event_id
            .trim()
            .is_empty()
        || command.authorization.authorization_basis.trim().is_empty()
        || !matches!(
            command.authorization.authorization_action.as_str(),
            "project.evidence.attach" | PROJECT_EVIDENCE_COMMAND
        )
    {
        return Err(ServiceError::invalid_operation(
            "evidence authorization provenance is incomplete",
        ));
    }
    Ok(())
}

fn command_context(
    command: &ProjectEvidenceCommand,
    action: Option<AgentActionProvenance>,
) -> Result<CommandContext> {
    let mut versions = BTreeMap::new();
    versions.insert(
        "expected_milestone_version".to_owned(),
        command.expected_milestone_version,
    );
    let context = NewCommandContext {
        principal: CommandPrincipal {
            principal_type: command.authorization.principal_type.clone(),
            principal_id: command.authorization.principal_id.clone(),
        },
        canonical_scope: CommandScope {
            scope_type: CommandScopeType::Project,
            scope_id: command.project_id.clone(),
        },
        operation: PROJECT_EVIDENCE_COMMAND.to_owned(),
        idempotency_key: command.idempotency_key.clone(),
        expected_state: ExpectedCommandState {
            versions,
            digests: BTreeMap::new(),
        },
        authorization_provenance: Some(crate::AuthorizationProvenance {
            policy_result: command.authorization.policy_result.clone(),
            policy_revision: command.authorization.policy_revision.clone(),
            policy_digest: command.authorization.policy_digest.clone(),
            requested_permission: command.authorization.requested_permission.clone(),
        }),
        action_provenance: action,
        correlation_id: command.authorization.correlation_id.clone(),
        causation_id: command.authorization.causation_id.clone(),
        causation_depth: command.authorization.causation_depth,
    };
    CommandContext::from_authorized_input(context, command).map_err(|error| {
        ServiceError::invalid_operation(format!("evidence command digest: {error}"))
    })
}

/// Pair the receipt with the AgentAction execution record when the command
/// arrived through an admitted Action, so both land in the same transaction.
fn validation_command_bundle(
    context: &CommandContext,
    outcome_json: &str,
) -> (CreateCommandReceipt, Option<CreateAgentActionExecution>) {
    let receipt = create_receipt(context, outcome_json);
    let execution = context.action_provenance.as_ref().map(|provenance| {
        let committed_at = now_rfc3339();
        CreateAgentActionExecution {
            id: new_uuid_v4(),
            action_id: provenance.action_id.clone(),
            expected_action_version: provenance.expected_action_version,
            attempt: provenance.attempt,
            status: AgentActionExecutionStatus::Succeeded,
            result_json: Some(outcome_json.to_owned()),
            error: None,
            executed_by_type: provenance.executed_by_type.clone(),
            executed_by_id: provenance.executed_by_id.clone(),
            idempotency_key: provenance.execution_idempotency_key.clone(),
            action_status: AgentActionStatus::Executed,
            action_outcome_json: Some(outcome_json.to_owned()),
            created_at: committed_at.clone(),
            completed_at: Some(committed_at.clone()),
            updated_at: committed_at,
        }
    });
    (receipt, execution)
}

fn create_receipt(context: &CommandContext, outcome_json: &str) -> CreateCommandReceipt {
    CreateCommandReceipt {
        id: new_uuid_v4(),
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        scope_type: context.canonical_scope().scope_type().as_str().to_owned(),
        scope_id: context.canonical_scope().scope_id().to_owned(),
        operation: context.operation().to_owned(),
        idempotency_key: context.idempotency_key().to_owned(),
        input_digest: context.input_digest().to_owned(),
        policy_result: context
            .authorization_provenance
            .as_ref()
            .map_or_else(|| "allowed".to_owned(), |value| value.policy_result.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        event_id: String::new(),
        agent_action_execution_id: None,
        outcome_json: outcome_json.to_owned(),
        committed_at: now_rfc3339(),
    }
}

mod execution;

#[cfg(test)]
mod tests;
