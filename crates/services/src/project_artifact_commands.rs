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
use sqlx::Row;

use crate::{
    AgentActionProvenance, CommandContext, CommandPrincipal, CommandScope, CommandScopeType,
    ExpectedCommandState, NewCommandContext, Result, ServiceError,
};

pub const PROJECT_EVIDENCE_COMMAND: &str = "project.evidence";

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

impl ProjectArtifactCommandService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Attach one Project evidence reference.  The domain mutation, event,
    /// command receipt, and optional action execution are finalized by the DB
    /// composite in one SQLite transaction.
    pub async fn attach_evidence(
        &self,
        command: ProjectEvidenceCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectMediaAttachment> {
        validate_evidence_command(&command)?;
        if command.expected_milestone_version < 1 {
            return Err(ServiceError::invalid_operation(
                "expected_milestone_version must be positive for an authenticated user command",
            ));
        }
        let context = command_context(&command, action)?;
        self.attach_evidence_with_context(command, context).await
    }

    /// Native Project Agent execution supplies the already-authorized command
    /// context built from its admitted AgentAction.  Keeping this entry point
    /// separate prevents the service from reconstructing or weakening that
    /// provenance.
    pub(crate) async fn attach_evidence_with_context(
        &self,
        command: ProjectEvidenceCommand,
        context: CommandContext,
    ) -> Result<ProjectMediaAttachment> {
        validate_evidence_command(&command)?;
        if context.operation() != PROJECT_EVIDENCE_COMMAND
            || context.canonical_scope().scope_type() != CommandScopeType::Project
            || context.canonical_scope().scope_id() != command.project_id
            || context.principal().principal_type() != command.authorization.principal_type
            || context.principal().principal_id() != command.authorization.principal_id
        {
            return Err(ServiceError::invalid_operation(
                "evidence command context does not match its Project authorization",
            ));
        }

        // Resolve the receipt before membership, mutable asset/milestone
        // checks, or server identity allocation. A response-loss retry must
        // return the original attachment even after its current projections
        // have changed.
        if let Some(existing) = self.replay_evidence(&context).await? {
            return Ok(existing);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;

        let milestone =
            ProjectOrchestrationRepo::get_project_milestone(&*self.db, &command.milestone_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_milestone", &command.milestone_id)
                })?;
        if milestone.project_id != command.project_id {
            // Treat a milestone belonging to another Project as absent from
            // this scope. This keeps the public route from revealing that a
            // caller-supplied milestone exists elsewhere while retaining the
            // service's strict Project-scope rejection.
            return Err(ServiceError::NotFound {
                entity: "milestone",
                id: command.milestone_id,
            });
        }

        let asset = SharedMediaRepo::get_media_asset(&*self.db, &command.asset_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("media_asset", &command.asset_id))?;
        if asset.project_id != command.project_id
            || asset.deleted_at.is_some()
            || asset.availability != "available"
        {
            return Err(ServiceError::conflict(
                "evidence media is unavailable or cross-Project",
            ));
        }
        if asset.checksum.as_deref() != Some(command.checksum.as_str()) {
            return Err(ServiceError::conflict(
                "evidence checksum does not match the media asset",
            ));
        }

        let expected_milestone_version = command.expected_milestone_version;
        let attachment_id = new_uuid_v4();
        let now = now_rfc3339();
        let result = json!({
            "operation": PROJECT_EVIDENCE_COMMAND,
            "project_id": command.project_id,
            "milestone_id": command.milestone_id,
            "attachment_id": attachment_id,
            "asset_id": command.asset_id,
            "domain_committed": true,
        });
        let result_json = serde_json::to_string(&result).map_err(|error| {
            ServiceError::invalid_operation(format!("serialize evidence result: {error}"))
        })?;
        let attachment = CreateProjectMediaAttachment {
            id: attachment_id,
            project_id: command.project_id.clone(),
            asset_id: command.asset_id.clone(),
            attachment_kind: "evidence".to_owned(),
            task_media_id: None,
            task_id: command.task_id.clone(),
            milestone_id: Some(command.milestone_id.clone()),
            milestone_check_id: None,
            source_task_id: command.task_id.clone(),
            source_execution_id: command.source_run_id.clone(),
            source_validation_id: command.source_validation_id.clone(),
            acceptance_check_ids_json: serde_json::to_string(&command.acceptance_check_ids)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            caption: Some(command.caption.clone()),
            evidence_kind: Some(command.evidence_kind.clone()),
            checksum: Some(command.checksum.clone()),
            availability: "available".to_owned(),
            project_url: Some(format!(
                "/api/v1/projects/{}/media/{}",
                command.project_id, command.asset_id
            )),
            author_type: command.authorization.principal_type.clone(),
            author_id: Some(command.authorization.principal_id.clone()),
            authorization_json: command.authorization.authorization_json.clone(),
            created_at: now,
        };
        let receipt = create_receipt(&context, &result_json);
        let execution = context.action_provenance.as_ref().map(|provenance| {
            let committed_at = now_rfc3339();
            CreateAgentActionExecution {
                id: new_uuid_v4(),
                action_id: provenance.action_id.clone(),
                expected_action_version: provenance.expected_action_version,
                attempt: provenance.attempt,
                status: AgentActionExecutionStatus::Succeeded,
                result_json: Some(result_json.clone()),
                error: None,
                executed_by_type: provenance.executed_by_type.clone(),
                executed_by_id: provenance.executed_by_id.clone(),
                idempotency_key: provenance.execution_idempotency_key.clone(),
                action_status: AgentActionStatus::Executed,
                action_outcome_json: Some(result_json.clone()),
                created_at: committed_at.clone(),
                completed_at: Some(committed_at.clone()),
                updated_at: committed_at,
            }
        });
        let mut receipt = receipt;
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        SharedMediaRepo::create_project_media_attachment_mutation(
            &*self.db,
            CreateProjectMediaAttachmentMutation {
                attachment,
                expected_milestone_version,
                idempotency_key: command.idempotency_key,
                mutation_fingerprint: context.input_digest().to_owned(),
                authorization_event_id: command.authorization.authorization_event_id,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await
        .map_err(Into::into)
    }

    /// Create the typed Project Document shell through the command-aware
    /// repository method.  REST and native execution both use this boundary;
    /// the first revision remains a separate command because the public REST
    /// contract creates the shell before content is submitted.
    pub async fn create_document(
        &self,
        command: ProjectDocumentCreateCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentRecord> {
        validate_document_create_command(&command)?;
        let context = command_context_for(
            "project.document.create",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_project_version,
            "expected_project_version",
        )?;
        self.create_document_with_context(command, context).await
    }

    pub(crate) async fn create_document_with_context(
        &self,
        command: ProjectDocumentCreateCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRecord> {
        validate_document_create_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(document) = self.replay_document_shell(&context).await? {
            return Ok(document);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let document_id = new_uuid_v4();
        let now = now_rfc3339();
        let result_json = json!({
            "operation": "project.document.create",
            "project_id": command.project_id,
            "document_id": document_id,
            "kind": command.kind,
            "title": command.title,
            "approval_policy": command.approval_policy,
            "expected_project_version": command.expected_project_version,
            "lifecycle": "draft",
            "current_draft_revision_id": Value::Null,
            "current_approved_revision_id": Value::Null,
            "version": 1,
            "created_at": now.clone(),
            "updated_at": now.clone(),
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let document = ProjectOrchestrationRepo::create_project_document_shell_command(
            &*self.db,
            CreateProjectDocumentShellCommand {
                document: CreateProjectDocument {
                    id: result_value(&result_json, "document_id")?,
                    project_id: command.project_id,
                    kind: command.kind,
                    title: command.title,
                    approval_policy: command.approval_policy,
                    created_at: now.clone(),
                    updated_at: now,
                },
                expected_project_version: command.expected_project_version,
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if document.id != result_value(&result_json, "document_id")? || document.version != 1 {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical shell".to_owned(),
            ));
        }
        Ok(document)
    }

    /// Append one typed Project Document revision through the command-aware
    /// repository method.  The adapter supplies only typed content and
    /// authorization provenance; scope, base revision, and the frozen result
    /// are owned here.
    pub async fn save_document_revision(
        &self,
        command: ProjectDocumentRevisionCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentRevisionRecord> {
        validate_document_revision_command(&command)?;
        let context = command_context_for(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.save_document_revision_with_context(command, context)
            .await
    }

    pub(crate) async fn save_document_revision_with_context(
        &self,
        command: ProjectDocumentRevisionCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRevisionRecord> {
        validate_document_revision_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(revision) = self.replay_document_revision(&context).await? {
            return Ok(revision);
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        if command.provenance.author.id != command.authorization.principal_id {
            return Err(ServiceError::AuthorizationDenied {
                message: "Document revision provenance principal does not match command principal"
                    .to_owned(),
            });
        }

        let existing =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &command.document_id).await?;
        let Some(document) = existing else {
            return self.create_first_document_revision(command, context).await;
        };
        validate_document_scope(&document, &command.project_id)?;
        if let Some(kind) = command.kind.as_deref() {
            if kind != document.kind {
                return Err(ServiceError::conflict(
                    "Project Document kind does not match the existing Document",
                ));
            }
        }
        if let Some(title) = command.title.as_deref() {
            if title != document.title {
                return Err(ServiceError::conflict(
                    "Project Document title does not match the existing Document",
                ));
            }
        }
        let current_draft = match document.current_draft_revision_id.as_deref() {
            Some(id) => Some(
                ProjectOrchestrationRepo::get_project_document_revision(&*self.db, id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project_document_revision", id))?,
            ),
            None => None,
        };
        if let Some(expected_digest) = command.expected_digest.as_deref() {
            if current_draft
                .as_ref()
                .map(|revision| revision.content_digest.as_str())
                != Some(expected_digest)
            {
                return Err(ServiceError::conflict(
                    "the current draft digest changed before this revision was saved",
                ));
            }
        }
        let (base_revision, base_revision_id) = if let Some(base_id) =
            command.base_revision_id.clone()
        {
            let base = ProjectOrchestrationRepo::get_project_document_revision(&*self.db, &base_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_document_revision", &base_id))?;
            if base.document_id != command.document_id
                || document.current_draft_revision_id.as_deref() != Some(base_id.as_str())
            {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            (base.revision, Some(base_id))
        } else {
            if current_draft.is_some() {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            (0, None)
        };
        let kind = crate::parse_document_kind(&document.kind).ok_or_else(|| {
            ServiceError::invalid_operation("persisted Project Document kind is invalid")
        })?;
        let rendered_view = crate::render_project_document(&document.title, kind, &command.content);
        let content_json = crate::render_project_document_json(&command.content);
        let effective_lifecycle = effective_document_lifecycle(&document, command.lifecycle)?;
        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_document_revision WHERE document_id = ?",
        )
        .bind(&command.document_id)
        .fetch_one(self.db.pool())
        .await?;
        let revision_id = new_uuid_v4();
        let created_at = now_rfc3339();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": command.document_id,
            "revision_id": revision_id,
            "revision": revision_number,
            "lifecycle": effective_lifecycle,
            "domain_committed": true,
            "requires_user_authorization": effective_lifecycle == "proposed",
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let revision = ProjectOrchestrationRepo::append_project_document_revision_command(
            &*self.db,
            AppendProjectDocumentRevisionCommand {
                revision: CreateProjectDocumentRevision {
                    id: revision_id,
                    document_id: command.document_id,
                    expected_document_version: command.expected_document_version,
                    base_revision,
                    base_revision_id,
                    lifecycle: effective_lifecycle.to_owned(),
                    schema_version: crate::PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
                    render_version: crate::PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
                    content_json,
                    rendered_view: rendered_view.clone(),
                    change_summary: command.change_summary,
                    author_type: command.authorization.principal_type.clone(),
                    author_id: Some(command.authorization.principal_id.clone()),
                    source_refs_json: serde_json::to_string(&command.provenance.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    content_digest: crate::document_content_digest(&command.content),
                    rendered_digest: crate::document_render_digest(
                        crate::PROJECT_DOCUMENT_RENDER_VERSION,
                        &rendered_view,
                    ),
                    created_at,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if revision.revision != revision_number {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical revision".to_owned(),
            ));
        }
        Ok(revision)
    }

    async fn create_first_document_revision(
        &self,
        command: ProjectDocumentRevisionCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentRevisionRecord> {
        let kind_text = command.kind.clone().ok_or_else(|| {
            ServiceError::invalid_operation("first Project Document revision kind is required")
        })?;
        let title = command.title.clone().ok_or_else(|| {
            ServiceError::invalid_operation("first Project Document revision title is required")
        })?;
        let approval_policy = command
            .approval_policy
            .clone()
            .unwrap_or_else(|| "user_or_project_agent".to_owned());
        if command.expected_document_version != 1
            || command.base_revision_id.is_some()
            || command.lifecycle != DocumentRevisionLifecycle::Draft
        {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        crate::parse_document_kind(&kind_text)
            .ok_or_else(|| ServiceError::invalid_operation("Project Document kind is invalid"))?;
        if title.trim().is_empty() || approval_policy.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "first Project Document identity is incomplete",
            ));
        }
        let document_id = new_uuid_v4();
        let revision_id = new_uuid_v4();
        let kind = crate::parse_document_kind(&kind_text)
            .ok_or_else(|| ServiceError::invalid_operation("Project Document kind is invalid"))?;
        let rendered_view = crate::render_project_document(&title, kind, &command.content);
        let now = now_rfc3339();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": document_id,
            "revision_id": revision_id,
            "revision": 1,
            "lifecycle": "draft",
            "domain_committed": true,
            "requires_user_authorization": false,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let revision = ProjectOrchestrationRepo::create_project_document_command(
            &*self.db,
            CreateProjectDocumentCommand {
                document: CreateProjectDocument {
                    id: result_value(&result_json, "document_id")?,
                    project_id: command.project_id,
                    kind: kind_text,
                    title,
                    approval_policy,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                revision: CreateProjectDocumentRevision {
                    id: result_value(&result_json, "revision_id")?,
                    document_id: result_value(&result_json, "document_id")?,
                    expected_document_version: 1,
                    base_revision: 0,
                    base_revision_id: None,
                    lifecycle: "draft".to_owned(),
                    schema_version: crate::PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
                    render_version: crate::PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
                    content_json: crate::render_project_document_json(&command.content),
                    rendered_view: rendered_view.clone(),
                    change_summary: command.change_summary,
                    author_type: command.authorization.principal_type.clone(),
                    author_id: Some(command.authorization.principal_id.clone()),
                    source_refs_json: serde_json::to_string(&command.provenance.source_refs)
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                    content_digest: crate::document_content_digest(&command.content),
                    rendered_digest: crate::document_render_digest(
                        crate::PROJECT_DOCUMENT_RENDER_VERSION,
                        &rendered_view,
                    ),
                    created_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        if revision.id != result_value(&result_json, "revision_id")? || revision.revision != 1 {
            return Err(ServiceError::Conflict(
                "Project Document command returned a non-canonical first revision".to_owned(),
            ));
        }
        Ok(revision)
    }

    /// Approve one exact current Document revision through the shared command
    /// repository method.  No current revision or digest is inferred.
    pub async fn approve_document(
        &self,
        command: ProjectDocumentApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let (approval, _) = self.approve_document_with_status(command, action).await?;
        Ok(approval)
    }

    /// Approve one exact current Document revision and report whether the
    /// result came from an existing immutable command receipt.  HTTP adapters
    /// use this distinction to preserve the created-vs-replayed status while
    /// all transports continue to share the same receipt-first command path.
    pub async fn approve_document_with_status(
        &self,
        command: ProjectDocumentApprovalCommand,
        action: Option<AgentActionProvenance>,
    ) -> Result<(ProjectDocumentApprovalRecord, bool)> {
        validate_document_approval_command(&command)?;
        let context = command_context_for(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            &command,
            action,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.approve_document_with_context_with_status(command, context)
            .await
    }

    /// Resolve an approval receipt before adapter-level authorization checks.
    /// A replay must compare the complete frozen command identity first, so a
    /// changed authorization envelope receives `idempotency_conflict` instead
    /// of being treated as a new unauthorized request.
    pub async fn replay_document_approval_if_present(
        &self,
        command: &ProjectDocumentApprovalCommand,
    ) -> Result<Option<ProjectDocumentApprovalRecord>> {
        let context = command_context_for_unvalidated(
            "project.document",
            &command.idempotency_key,
            &command.authorization,
            command,
            None,
            &command.project_id,
            command.expected_document_version,
            "expected_document_version",
        )?;
        self.replay_document_approval(&context).await
    }

    pub(crate) async fn approve_document_with_context(
        &self,
        command: ProjectDocumentApprovalCommand,
        context: CommandContext,
    ) -> Result<ProjectDocumentApprovalRecord> {
        let (approval, _) = self
            .approve_document_with_context_with_status(command, context)
            .await?;
        Ok(approval)
    }

    pub(crate) async fn approve_document_with_context_with_status(
        &self,
        command: ProjectDocumentApprovalCommand,
        context: CommandContext,
    ) -> Result<(ProjectDocumentApprovalRecord, bool)> {
        validate_document_approval_command(&command)?;
        validate_document_context(&context, &command.project_id, &command.authorization)?;
        if let Some(approval) = self.replay_document_approval(&context).await? {
            return Ok((approval, true));
        }
        authorize_project_principal(&self.db, &command.project_id, &command.authorization).await?;
        let document =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &command.document_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_document", &command.document_id))?;
        validate_document_scope(&document, &command.project_id)?;
        validate_document_approval_policy(&document, &command.authorization)?;
        let revision = ProjectOrchestrationRepo::get_project_document_revision(
            &*self.db,
            &command.revision_id,
        )
        .await?
        .ok_or_else(|| {
            ServiceError::not_found("project_document_revision", &command.revision_id)
        })?;
        if revision.document_id != document.id
            || document.current_draft_revision_id.as_deref() != Some(revision.id.as_str())
            || revision.content_digest != command.content_digest
            || revision.rendered_digest != command.rendered_digest
            || !matches!(revision.lifecycle.as_str(), "draft" | "proposed")
        {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let approval_id = new_uuid_v4();
        let result_json = json!({
            "operation": "project.document",
            "project_id": command.project_id,
            "document_id": command.document_id,
            "revision_id": command.revision_id,
            "approval_id": approval_id,
            "content_digest": command.content_digest,
            "render_digest": command.rendered_digest,
            "lifecycle": "active",
            "domain_committed": true,
        })
        .to_string();
        let (mut receipt, execution) = command_bundle(&context, &result_json);
        if let Some(execution) = execution.as_ref() {
            receipt.agent_action_execution_id = Some(execution.id.clone());
        }
        let now = now_rfc3339();
        let approval = ProjectOrchestrationRepo::approve_project_document_command(
            &*self.db,
            ApproveProjectDocumentCommand {
                approval: ApproveProjectDocument {
                    id: approval_id,
                    document_id: document.id,
                    revision_id: revision.id,
                    expected_document_version: command.expected_document_version,
                    principal_type: command.authorization.principal_type,
                    principal_id: command.authorization.principal_id,
                    authorization_basis: command.authorization.authorization_basis,
                    authorization_action: command.authorization.authorization_action,
                    explicit_event: command.authorization.authorization_event_id,
                    authorization_occurred_at: command.authorization.authorization_occurred_at,
                    content_digest: command.content_digest,
                    rendered_digest: command.rendered_digest,
                    idempotency_key: command.idempotency_key,
                    created_at: now.clone(),
                    updated_at: now,
                },
                command_receipt: Some(receipt),
                action_execution: execution,
            },
        )
        .await?;
        Ok((approval, false))
    }

    async fn replay_document_shell(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
            ServiceError::Conflict("Project Document replay outcome is invalid".to_owned())
        })?;
        let project_id = result_value(&receipt.outcome_json, "project_id")?;
        if project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(ProjectDocumentRecord {
            id: result_value(&receipt.outcome_json, "document_id")?,
            project_id,
            kind: result_value(&receipt.outcome_json, "kind")?,
            title: result_value(&receipt.outcome_json, "title")?,
            lifecycle: result_value(&receipt.outcome_json, "lifecycle")?,
            approval_policy: result_value(&receipt.outcome_json, "approval_policy")?,
            current_draft_revision_id: result_optional_string(
                &outcome,
                "current_draft_revision_id",
            )?,
            current_approved_revision_id: result_optional_string(
                &outcome,
                "current_approved_revision_id",
            )?,
            version: result_i64(&outcome, "version")?,
            created_at: result_value(&receipt.outcome_json, "created_at")?,
            updated_at: result_value(&receipt.outcome_json, "updated_at")?,
        }))
    }

    async fn replay_document_revision(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentRevisionRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let document_id = result_value(&receipt.outcome_json, "document_id")?;
        let revision_id = result_value(&receipt.outcome_json, "revision_id")?;
        let revision =
            ProjectOrchestrationRepo::get_project_document_revision(&*self.db, &revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Project Document replay revision is missing".to_owned())
                })?;
        if revision.document_id != document_id {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let document = ProjectOrchestrationRepo::get_project_document(&*self.db, &document_id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("Project Document replay shell is missing".to_owned())
            })?;
        if document.project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(revision))
    }

    async fn replay_document_approval(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectDocumentApprovalRecord>> {
        let Some(receipt) = self.replay_receipt(context).await? else {
            return Ok(None);
        };
        let approval_id = result_value(&receipt.outcome_json, "approval_id")?;
        let approval = load_document_approval(&self.db, &approval_id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict("Project Document replay approval is missing".to_owned())
            })?;
        if approval.principal_id != context.principal().principal_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let document =
            ProjectOrchestrationRepo::get_project_document(&*self.db, &approval.document_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("Project Document replay shell is missing".to_owned())
                })?;
        if document.project_id != context.canonical_scope().scope_id() {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(approval))
    }

    async fn replay_receipt(&self, context: &CommandContext) -> Result<Option<CommandReceipt>> {
        let existing = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await?;
        if existing.is_some() {
            return Ok(existing);
        }
        let existing_digest: Option<String> = sqlx::query_scalar(
            "SELECT input_digest FROM command_receipt
             WHERE principal_type = ? AND principal_id = ? AND scope_type = ?
               AND scope_id = ? AND operation = ? AND idempotency_key = ?
             LIMIT 1",
        )
        .bind(context.principal().principal_type())
        .bind(context.principal().principal_id())
        .bind(context.canonical_scope().scope_type().as_str())
        .bind(context.canonical_scope().scope_id())
        .bind(context.operation())
        .bind(context.idempotency_key())
        .fetch_optional(self.db.pool())
        .await?;
        if existing_digest.is_some_and(|digest| digest != context.input_digest()) {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(None)
    }

    async fn replay_evidence(
        &self,
        context: &CommandContext,
    ) -> Result<Option<ProjectMediaAttachment>> {
        let existing = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            context.principal().principal_type(),
            context.principal().principal_id(),
            context.canonical_scope().scope_type().as_str(),
            context.canonical_scope().scope_id(),
            context.operation(),
            context.idempotency_key(),
            context.input_digest(),
        )
        .await?;
        if let Some(receipt) = existing {
            let outcome: Value = serde_json::from_str(&receipt.outcome_json).map_err(|_| {
                ServiceError::Conflict("evidence receipt outcome is invalid".to_owned())
            })?;
            let attachment_id = outcome
                .get("attachment_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ServiceError::Conflict("evidence receipt has no attachment".to_owned())
                })?;
            let attachment = load_project_media_attachment(&self.db, attachment_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict("evidence receipt attachment is missing".to_owned())
                })?;
            if attachment.project_id != context.canonical_scope().scope_id() {
                return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
            }
            return Ok(Some(attachment));
        }

        // Exact-digest lookup intentionally returns no row for altered input.
        // Detect that identity collision before any domain lookup so the
        // caller receives idempotency_conflict rather than a version error.
        let existing_digest: Option<String> = sqlx::query_scalar(
            "SELECT input_digest FROM command_receipt
             WHERE principal_type = ? AND principal_id = ? AND scope_type = ?
               AND scope_id = ? AND operation = ? AND idempotency_key = ?
             LIMIT 1",
        )
        .bind(context.principal().principal_type())
        .bind(context.principal().principal_id())
        .bind(context.canonical_scope().scope_type().as_str())
        .bind(context.canonical_scope().scope_id())
        .bind(context.operation())
        .bind(context.idempotency_key())
        .fetch_optional(self.db.pool())
        .await?;
        if existing_digest.is_some_and(|digest| digest != context.input_digest()) {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(None)
    }
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
            let bound: Option<String> = sqlx::query_scalar(
                "SELECT identity_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
            )
            .bind(project_id)
            .bind(&authorization.principal_id)
            .fetch_optional(db.pool())
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
    let row = sqlx::query(
        "SELECT id, project_id, asset_id, attachment_kind, task_media_id, task_id,
                milestone_id, milestone_check_id, source_task_id, source_execution_id,
                source_validation_id, acceptance_check_ids_json, caption, evidence_kind,
                checksum, availability, project_url, author_type, author_id,
                authorization_json, version, created_at, deleted_at, updated_at
         FROM project_media_attachment WHERE id = ? AND attachment_kind = 'evidence'",
    )
    .bind(attachment_id)
    .fetch_optional(db.pool())
    .await?;
    let attachment = row
        .map(
            |row| -> std::result::Result<ProjectMediaAttachment, sqlx::Error> {
                Ok(ProjectMediaAttachment {
                    id: row.try_get("id")?,
                    project_id: row.try_get("project_id")?,
                    asset_id: row.try_get("asset_id")?,
                    attachment_kind: row.try_get("attachment_kind")?,
                    task_media_id: row.try_get("task_media_id")?,
                    task_id: row.try_get("task_id")?,
                    milestone_id: row.try_get("milestone_id")?,
                    milestone_check_id: row.try_get("milestone_check_id")?,
                    source_task_id: row.try_get("source_task_id")?,
                    source_execution_id: row.try_get("source_execution_id")?,
                    source_validation_id: row.try_get("source_validation_id")?,
                    acceptance_check_ids_json: row.try_get("acceptance_check_ids_json")?,
                    caption: row.try_get("caption")?,
                    evidence_kind: row.try_get("evidence_kind")?,
                    checksum: row.try_get("checksum")?,
                    availability: row.try_get("availability")?,
                    project_url: row.try_get("project_url")?,
                    author_type: row.try_get("author_type")?,
                    author_id: row.try_get("author_id")?,
                    authorization_json: row.try_get("authorization_json")?,
                    version: row.try_get("version")?,
                    created_at: row.try_get("created_at")?,
                    deleted_at: row.try_get("deleted_at")?,
                    updated_at: row.try_get("updated_at")?,
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
    let row = sqlx::query(
        "SELECT id, document_id, revision_id, principal_type, principal_id,
                authorization_basis, authorization_action, explicit_event,
                authorization_occurred_at, content_digest, rendered_digest,
                lifecycle, idempotency_key, version, created_at, updated_at
         FROM project_document_approval WHERE id = ?",
    )
    .bind(approval_id)
    .fetch_optional(db.pool())
    .await?;
    let approval = row
        .map(
            |row| -> std::result::Result<ProjectDocumentApprovalRecord, sqlx::Error> {
                Ok(ProjectDocumentApprovalRecord {
                    id: row.try_get("id")?,
                    document_id: row.try_get("document_id")?,
                    revision_id: row.try_get("revision_id")?,
                    principal_type: row.try_get("principal_type")?,
                    principal_id: row.try_get("principal_id")?,
                    authorization_basis: row.try_get("authorization_basis")?,
                    authorization_action: row.try_get("authorization_action")?,
                    explicit_event: row.try_get("explicit_event")?,
                    authorization_occurred_at: row.try_get("authorization_occurred_at")?,
                    content_digest: row.try_get("content_digest")?,
                    rendered_digest: row.try_get("rendered_digest")?,
                    lifecycle: row.try_get("lifecycle")?,
                    idempotency_key: row.try_get("idempotency_key")?,
                    version: row.try_get("version")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
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
