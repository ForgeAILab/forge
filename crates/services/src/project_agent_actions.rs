//! Typed materialization for Project Agent orchestration proposals.
//!
//! Project native tools persist an `AgentAction` first.  This module is the
//! only path which may turn the safe Project-local proposal operations into
//! Charter/Document/Decision/Milestone/media domain records.  The generic
//! action executor deliberately rejects these operations, so an arbitrary
//! result can never masquerade as a domain mutation.

use std::sync::Arc;

use api_types::{
    canonical_digest_with_schema, canonical_json, AdaptiveEnvelope, ArtifactRef,
    AuthorizationProvenance, ExecutionBaselineContent, MilestoneDefinitionContent, PrincipalKind,
    PrincipalRef, ProjectCharterContent, ProjectDocumentContent, ProjectDocumentKind,
    RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecution, AgentActionExecutionStatus,
    AgentActionPolicyResult, AgentActionRepo, AgentActionStatus, ApproveProjectDocument,
    CreateAgentActionExecution, CreateDomainEvent, CreateProjectCharter,
    CreateProjectCharterRevision, CreateProjectCharterRevisionAtomically,
    CreateProjectDecisionCandidate, CreateProjectDocumentRevision, CreateProjectMediaAttachment,
    CreateProjectMilestone, CreateProjectMilestoneRevision, DomainEventRepo,
    ProjectOrchestrationRepo, SharedMediaRepo, SqliteDb,
};
use forge_agent_host::{
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
};
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    baseline_column_json, document_content_digest, document_render_digest, parse_document_kind,
    render_execution_baseline, render_project_document, validate_execution_baseline_policy,
    AgentActionService, MilestoneRuntime, Result, ServiceError, EXECUTION_BASELINE_RENDER_VERSION,
    EXECUTION_BASELINE_SCHEMA_VERSION, PROJECT_DOCUMENT_RENDER_VERSION,
    PROJECT_DOCUMENT_SCHEMA_VERSION,
};

const MILESTONE_DEFINITION_SCHEMA: &str = "forge.milestone-definition/v1";
const MILESTONE_RENDER_SCHEMA: &str = "forge.milestone-definition-render/v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteProjectOrchestrationActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

#[derive(Clone)]
pub struct ProjectOrchestrationActionService {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
}

impl ProjectOrchestrationActionService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            actions: AgentActionService::new(Arc::clone(&db)),
            db,
        }
    }

    /// Materialize one admitted Project Agent action through a typed domain
    /// operation.  A successful action replay is resolved before mutable
    /// Project state is loaded, making lost responses safe to retry.
    pub async fn execute(
        &self,
        input: ExecuteProjectOrchestrationActionInput,
    ) -> Result<AgentActionExecution> {
        let action = self.actions.get(&input.action_id).await?;
        if !is_project_orchestration_operation(&action.operation) {
            return Err(ServiceError::invalid_operation(
                "action is not a Project orchestration proposal",
            ));
        }
        let project_id = self.project_id_for_action(&action).await?;
        self.authorize_actor(&action, &project_id, &input).await?;

        if let Some(existing) =
            AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id).await?
        {
            if existing.idempotency_key != input.idempotency_key {
                return Err(ServiceError::conflict(
                    "Project orchestration action already has a successful execution with a different idempotency key",
                ));
            }
            return Ok(existing);
        }

        let admitted = matches!(
            (&action.policy_result, &action.status),
            (
                AgentActionPolicyResult::Allowed,
                AgentActionStatus::Proposed
            ) | (
                AgentActionPolicyResult::ApprovalRequired,
                AgentActionStatus::Approved,
            )
        );
        if !admitted {
            return Err(ServiceError::invalid_operation(
                "Project orchestration action requires an admitted policy result and status",
            ));
        }
        if action.version != input.expected_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }

        let payload: Value = serde_json::from_str(&action.payload_json)
            .map_err(|_| ServiceError::invalid_operation("Project action payload is invalid"))?;
        let result = match action.operation.as_str() {
            PROJECT_CHARTER_ADOPTION_OPERATION => {
                self.materialize_charter_adoption(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_DOCUMENT_OPERATION => {
                self.materialize_document(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_DECISION_OPERATION => {
                self.materialize_decision_checked(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_EXECUTION_BASELINE_OPERATION => {
                self.materialize_execution_baseline(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_MILESTONE_OPERATION => {
                self.materialize_milestone(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_EVIDENCE_OPERATION => {
                self.materialize_evidence(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_READINESS_OPERATION => {
                self.materialize_readiness_request(&action, &project_id, &payload)
                    .await?
            }
            PROJECT_RELEASE_OPERATION => {
                self.materialize_release_request(&action, &project_id, &payload)
                    .await?
            }
            _ => unreachable!("operation was validated above"),
        };

        let result_json = serde_json::to_string(&result).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Project orchestration execution result: {error}"
            ))
        })?;
        AgentActionRepo::record_action_execution(
            &*self.db,
            CreateAgentActionExecution {
                id: new_uuid_v4(),
                action_id: input.action_id,
                expected_action_version: input.expected_version,
                attempt: 1,
                status: AgentActionExecutionStatus::Succeeded,
                result_json: Some(result_json.clone()),
                error: None,
                executed_by_type: input.executed_by_type,
                executed_by_id: input.executed_by_id,
                idempotency_key: required("execution idempotency key", &input.idempotency_key)?,
                action_status: AgentActionStatus::Executed,
                action_outcome_json: Some(result_json),
                created_at: now_rfc3339(),
                completed_at: Some(now_rfc3339()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn project_id_for_action(&self, action: &AgentAction) -> Result<String> {
        if action.target_type.as_deref() == Some("project") {
            let project_id = action
                .target_id
                .clone()
                .ok_or_else(|| ServiceError::invalid_operation("Project action has no target"))?;
            let exists: Option<String> =
                sqlx::query_scalar("SELECT id FROM project WHERE id = ? LIMIT 1")
                    .bind(&project_id)
                    .fetch_optional(self.db.pool())
                    .await?;
            return exists.ok_or_else(|| ServiceError::not_found("project", project_id));
        }
        if action.scope_type == "project" {
            return Ok(action.scope_id.clone());
        }
        let project_id: Option<String> = sqlx::query_scalar(
            "SELECT project_id FROM agent_chat WHERE id = ? AND kind = 'project' LIMIT 1",
        )
        .bind(&action.scope_id)
        .fetch_optional(self.db.pool())
        .await?;
        project_id
            .ok_or_else(|| ServiceError::invalid_operation("Project action has no Project scope"))
    }

    async fn authorize_actor(
        &self,
        action: &AgentAction,
        project_id: &str,
        input: &ExecuteProjectOrchestrationActionInput,
    ) -> Result<()> {
        if input.executed_by_type == "agent" {
            if input.executed_by_id != action.actor_identity_id {
                return Err(ServiceError::invalid_operation(
                    "only the proposing Project Agent may execute this action",
                ));
            }
            let bound: Option<String> = sqlx::query_scalar(
                "SELECT project_id FROM project_agent_binding
                 WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
            )
            .bind(project_id)
            .bind(&action.actor_identity_id)
            .fetch_optional(self.db.pool())
            .await?;
            if bound.as_deref() != Some(project_id) {
                return Err(ServiceError::invalid_operation(
                    "Project Agent is not actively bound to this Project",
                ));
            }
        } else if input.executed_by_type == "user" {
            let owner: Option<String> =
                sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ? LIMIT 1")
                    .bind(project_id)
                    .fetch_optional(self.db.pool())
                    .await?;
            if owner.as_deref() != Some(input.executed_by_id.as_str()) {
                return Err(ServiceError::invalid_operation(
                    "only the Project owner may execute a Project action",
                ));
            }
        } else {
            return Err(ServiceError::invalid_operation(
                "Project action executor type is not admitted",
            ));
        }
        Ok(())
    }

    async fn materialize_charter_adoption(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let charter_id = string(payload, "charter_id")?;
        let project = sqlx::query(
            "SELECT owner_id, charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let project_owner_id: Option<String> = project.try_get("owner_id")?;
        let account_id = if let Some(owner_id) = project_owner_id {
            owner_id
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT owner_id FROM agent_identity WHERE id = ? LIMIT 1",
            )
            .bind(&action.actor_identity_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project adoption has no account owner for its Charter scope",
                )
            })?
        };
        let charter = ProjectOrchestrationRepo::get_project_charter(&*self.db, &charter_id).await?;
        if let Some(charter) = &charter {
            if charter.project_id.as_deref() != Some(project_id)
                || charter.genesis_session_id.is_some()
            {
                return Err(ServiceError::invalid_operation(
                    "Charter adoption crosses Project scope",
                ));
            }
        }
        let current_charter_id: Option<String> = project.try_get("current_charter_id")?;
        let current_charter_revision_id: Option<String> =
            project.try_get("current_charter_revision_id")?;
        let is_setup_project = project.try_get::<String, _>("charter_status")?
            == "legacy_unverified"
            && project.try_get::<i64, _>("charter_setup_required")? == 1
            && current_charter_id.is_none()
            && current_charter_revision_id.is_none();
        if charter.is_none() && !is_setup_project {
            return Err(ServiceError::invalid_operation(
                "Project Charter adoption requires a setup Project or an existing Project Charter",
            ));
        }
        if charter.is_none()
            && (current_charter_id.is_some() || current_charter_revision_id.is_some())
        {
            return Err(ServiceError::invalid_operation(
                "Project Charter adoption cannot replace an existing current Charter",
            ));
        }
        let content: ProjectCharterContent = from_value(payload, "content")?;
        let render = crate::render_and_digest_charter(&content);
        if string(payload, "rendered_view")? != render.rendered_view
            || string(payload, "render_version")? != render.render_version
        {
            return Err(ServiceError::conflict(
                "Charter adoption render does not match the server renderer",
            ));
        }
        let project_mode = string(payload, "project_mode")?;
        let maturity = string(payload, "maturity")?;
        let provenance: RevisionProvenance = from_value(payload, "provenance")?;
        let base_revision_id = payload
            .get("base_revision_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let base_revision = if let Some(base_id) = base_revision_id.as_deref() {
            let base = ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, base_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_charter_revision", base_id))?;
            if base.charter_id != charter_id || charter.is_none() {
                return Err(ServiceError::invalid_operation(
                    "Charter adoption base revision crosses Charter scope",
                ));
            }
            base.revision
        } else {
            0
        };
        let requested_charter_version = integer(payload, "expected_charter_version")?;
        let (expected_charter_version, charter_mode, charter_maturity) = match charter.as_ref() {
            Some(charter) => {
                let expected = if requested_charter_version == 0 {
                    if charter.version != 1 || charter.current_draft_revision_id.is_some() {
                        return Err(ServiceError::conflict(
                            "a Project Charter adoption draft already exists; send its current version",
                        ));
                    }
                    charter.version
                } else {
                    requested_charter_version
                };
                if charter.version != expected {
                    return Err(ServiceError::conflict(
                        "the Project Charter changed before adoption was materialized",
                    ));
                }
                if charter.project_mode != project_mode || charter.maturity != maturity {
                    return Err(ServiceError::conflict(
                        "Project Charter mode and maturity are immutable after draft creation",
                    ));
                }
                (
                    expected,
                    charter.project_mode.clone(),
                    charter.maturity.clone(),
                )
            }
            None => {
                if requested_charter_version != 0 || base_revision_id.is_some() {
                    return Err(ServiceError::conflict(
                        "a new Project adoption Charter begins at expected version 0 with no base revision",
                    ));
                }
                (1, project_mode.clone(), maturity.clone())
            }
        };
        let revision_input = CreateProjectCharterRevision {
            id: new_uuid_v4(),
            charter_id: charter_id.clone(),
            expected_charter_version,
            project_mode: charter_mode.clone(),
            maturity: charter_maturity.clone(),
            base_revision,
            base_revision_id: base_revision_id.clone(),
            lifecycle: "draft".to_owned(),
            schema_version: "forge.project-charter/v1".to_owned(),
            render_version: render.render_version,
            content_json: canonical_json(&content)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            rendered_view: render.rendered_view,
            change_summary: provenance.change_summary,
            author_type: "agent".to_owned(),
            author_id: Some(action.actor_identity_id.clone()),
            source_message_id: None,
            source_turn_job_id: None,
            source_refs_json: serde_json::to_string(&provenance.source_refs)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            content_digest: render.content_digest,
            rendered_digest: render.render_digest,
            created_at: now_rfc3339(),
        };
        let revision = if !is_setup_project {
            ProjectOrchestrationRepo::create_project_charter_revision(&*self.db, revision_input)
                .await?
        } else {
            let now = now_rfc3339();
            let charter_record = charter.as_ref();
            ProjectOrchestrationRepo::create_project_charter_revision_atomically(
                &*self.db,
                CreateProjectCharterRevisionAtomically {
                    project_id: Some(project_id.to_owned()),
                    genesis_session_id: None,
                    account_id: account_id.clone(),
                    charter: CreateProjectCharter {
                        id: charter_id.clone(),
                        account_id: account_id.clone(),
                        genesis_session_id: None,
                        project_mode: charter_record
                            .map(|charter| charter.project_mode.clone())
                            .unwrap_or_else(|| charter_mode.clone()),
                        maturity: charter_record
                            .map(|charter| charter.maturity.clone())
                            .unwrap_or_else(|| charter_maturity.clone()),
                        created_at: charter_record
                            .map(|charter| charter.created_at.clone())
                            .unwrap_or_else(|| now.clone()),
                        updated_at: charter_record
                            .map(|charter| charter.updated_at.clone())
                            .unwrap_or(now),
                    },
                    revision: revision_input,
                },
            )
            .await?
        };
        Ok(json!({
            "operation": PROJECT_CHARTER_ADOPTION_OPERATION,
            "project_id": project_id,
            "charter_id": charter_id,
            "revision_id": revision.id,
            "revision": revision.revision,
            "lifecycle": revision.lifecycle,
            "domain_committed": true,
            "requires_user_authorization": true,
        }))
    }

    async fn materialize_document(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let document_id = string(payload, "document_id")?;
        let kind_text = string(payload, "kind")?;
        let kind = parse_document_kind(&kind_text)
            .ok_or_else(|| ServiceError::invalid_operation("Project Document kind is invalid"))?;
        let title = string(payload, "title")?;
        let document_action = string(payload, "action")?;
        let document =
            match ProjectOrchestrationRepo::get_project_document(&*self.db, &document_id).await? {
                Some(document) => document,
                // A Charter-created Project starts with no Documents and the
                // Agent surface has no separate create operation, so the first
                // draft_revision of an unknown Document id creates the Document.
                None if document_action == "draft_revision"
                    && payload
                        .get("base_revision_id")
                        .map(Value::is_null)
                        .unwrap_or(true) =>
                {
                    if !db::validate_uuid_v4(&document_id) {
                        return Err(ServiceError::invalid_operation(
                            "Project Document id must be a new UUID v4",
                        ));
                    }
                    let now = now_rfc3339();
                    sqlx::query(
                        "INSERT INTO project_document (
                        id, project_id, kind, title, lifecycle, approval_policy,
                        current_draft_revision_id, current_approved_revision_id,
                        version, created_at, updated_at
                     ) VALUES (?, ?, ?, ?, 'draft', 'user_or_project_agent', NULL, NULL, 1, ?, ?)",
                    )
                    .bind(&document_id)
                    .bind(project_id)
                    .bind(&kind_text)
                    .bind(&title)
                    .bind(&now)
                    .bind(&now)
                    .execute(self.db.pool())
                    .await?;
                    ProjectOrchestrationRepo::get_project_document(&*self.db, &document_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project_document", document_id.clone())
                        })?
                }
                None => {
                    return Err(ServiceError::not_found(
                        "project_document",
                        document_id.clone(),
                    ));
                }
            };
        if document.project_id != project_id {
            return Err(ServiceError::invalid_operation(
                "Project Document action crosses Project scope",
            ));
        }
        if document.kind != kind_text || document.title != title {
            return Err(ServiceError::conflict(
                "Project Document identity does not match the proposal",
            ));
        }
        if document_action == "approve" {
            return self
                .materialize_document_approval(action, project_id, payload, &document)
                .await;
        }
        if !matches!(
            document_action.as_str(),
            "draft_revision" | "propose_approval"
        ) {
            return Err(ServiceError::invalid_operation(
                "Project Agent may draft or propose a Document revision only",
            ));
        }
        let content = parse_document_content(
            kind,
            payload.get("content").ok_or_else(|| {
                ServiceError::invalid_operation("Project Document content is required")
            })?,
        )?;
        let rendered_view = render_project_document(&document.title, kind, &content);
        let base_revision_id = payload
            .get("base_revision_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let base_revision = if let Some(base_id) = base_revision_id.as_deref() {
            let base = ProjectOrchestrationRepo::get_project_document_revision(&*self.db, base_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_document_revision", base_id))?;
            if base.document_id != document_id {
                return Err(ServiceError::invalid_operation(
                    "Project Document base revision crosses Document scope",
                ));
            }
            base.revision
        } else {
            0
        };
        let lifecycle = if document_action == "propose_approval" {
            "proposed"
        } else {
            "draft"
        };
        let revision = ProjectOrchestrationRepo::create_project_document_revision(
            &*self.db,
            CreateProjectDocumentRevision {
                id: new_uuid_v4(),
                document_id: document_id.clone(),
                expected_document_version: integer(payload, "expected_document_version")?,
                base_revision,
                base_revision_id,
                lifecycle: lifecycle.to_owned(),
                schema_version: PROJECT_DOCUMENT_SCHEMA_VERSION.to_owned(),
                render_version: PROJECT_DOCUMENT_RENDER_VERSION.to_owned(),
                content_json: crate::render_project_document_json(&content),
                rendered_view: rendered_view.clone(),
                change_summary: "Project Agent authored a typed document revision".to_owned(),
                author_type: "agent".to_owned(),
                author_id: Some(action.actor_identity_id.clone()),
                source_refs_json: "[]".to_owned(),
                content_digest: document_content_digest(&content),
                rendered_digest: document_render_digest(
                    PROJECT_DOCUMENT_RENDER_VERSION,
                    &rendered_view,
                ),
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_DOCUMENT_OPERATION,
            "project_id": project_id,
            "document_id": document_id,
            "revision_id": revision.id,
            "revision": revision.revision,
            "lifecycle": revision.lifecycle,
            "domain_committed": true,
            "requires_user_authorization": lifecycle == "proposed",
        }))
    }

    /// Materialize the Project Agent's narrow Document approval authority.
    ///
    /// This is intentionally separate from the user HTTP approval path.  The
    /// agent may approve only a Document whose policy explicitly admits the
    /// agent, and only while its authenticated Project binding is active.  An
    /// active execution baseline is used as a boundary, never as a grant: a
    /// Document already selected by that baseline, or required by an active
    /// release-gating check, is material scope and remains user-only.  The
    /// baseline/envelope fields are optional for pre-baseline planning
    /// Documents, but whenever supplied they must identify the exact active
    /// revision and envelope digest; stale or malformed persisted JSON fails
    /// closed.
    async fn materialize_document_approval(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        document: &db::ProjectDocumentRecord,
    ) -> Result<Value> {
        if !matches!(
            document.approval_policy.as_str(),
            "project_agent" | "user_or_project_agent"
        ) {
            return Err(ServiceError::invalid_operation(
                "Project Agent cannot approve a user-only or approval-free Document",
            ));
        }

        let revision_id = string(payload, "revision_id")?;
        let content_digest = string(payload, "content_digest")?;
        let rendered_digest = string(payload, "render_digest")?;
        let expected_document_version = integer(payload, "expected_document_version")?;
        let revision =
            ProjectOrchestrationRepo::get_project_document_revision(&*self.db, &revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("project_document_revision", revision_id.clone())
                })?;
        if revision.document_id != document.id {
            return Err(ServiceError::invalid_operation(
                "Project Document approval crosses Document scope",
            ));
        }
        if revision.content_digest != content_digest || revision.rendered_digest != rendered_digest
        {
            return Err(ServiceError::conflict(
                "Project Document approval digests do not match the exact revision",
            ));
        }
        if !matches!(revision.lifecycle.as_str(), "draft" | "proposed") {
            return Err(ServiceError::conflict(
                "Project Document approval target is not an approvable draft or proposal",
            ));
        }
        if document.current_draft_revision_id.as_deref() != Some(revision_id.as_str()) {
            return Err(ServiceError::conflict(
                "Project Document approval target is not the current draft revision",
            ));
        }

        // Re-read the binding at materialization time.  The action's actor
        // identity is not enough: a replaced/paused binding must not retain
        // authority over a Project action that was proposed earlier.
        let binding = sqlx::query(
            "SELECT b.id, b.identity_id, b.profile_id, b.policy_revision,
                    b.policy_digest, b.charter_revision_id,
                    p.identity_id AS profile_identity_id,
                    i.paused AS identity_paused
             FROM project_agent_binding AS b
             JOIN agent_profile AS p ON p.id = b.profile_id
             JOIN agent_identity AS i ON i.id = b.identity_id
             WHERE b.project_id = ? AND b.identity_id = ? AND b.state = 'active'
             LIMIT 1",
        )
        .bind(project_id)
        .bind(&action.actor_identity_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Document approval requires the proposing identity's active Project binding",
            )
        })?;
        let binding_id: String = binding.try_get("id")?;
        let binding_identity_id: String = binding.try_get("identity_id")?;
        let binding_profile_id: String = binding.try_get("profile_id")?;
        let binding_policy_revision: String = binding.try_get("policy_revision")?;
        let binding_policy_digest: String = binding.try_get("policy_digest")?;
        let binding_charter_revision_id: Option<String> = binding.try_get("charter_revision_id")?;
        let profile_identity_id: String = binding.try_get("profile_identity_id")?;
        let identity_paused: bool = binding.try_get::<i64, _>("identity_paused")? != 0;
        if binding_identity_id != action.actor_identity_id
            || profile_identity_id != action.actor_identity_id
            || binding_profile_id.trim().is_empty()
            || binding_policy_revision.trim().is_empty()
            || binding_policy_digest.trim().is_empty()
            || identity_paused
        {
            return Err(ServiceError::invalid_operation(
                "Project Document approval binding is stale, incomplete, or paused",
            ));
        }
        let current_charter_revision_id: Option<String> = sqlx::query_scalar(
            "SELECT current_charter_revision_id
             FROM project
             WHERE id = ? AND charter_status = 'charter_backed'
               AND charter_setup_required = 0
             LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .flatten();
        if current_charter_revision_id.is_some()
            && binding_charter_revision_id.as_deref() != current_charter_revision_id.as_deref()
        {
            return Err(ServiceError::invalid_operation(
                "Project Document approval binding is not attached to the current approved Charter",
            ));
        }

        let requested_baseline_id = optional_nonempty_string(payload, "baseline_id")?;
        let requested_baseline_revision_id =
            optional_nonempty_string(payload, "baseline_revision_id")?;
        let requested_envelope_digest = optional_nonempty_string(payload, "envelope_digest")?;
        if requested_baseline_id.is_some() != requested_baseline_revision_id.is_some()
            || requested_baseline_id.is_none() != requested_envelope_digest.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "Project Document approval baseline_id, baseline_revision_id, and envelope_digest must be supplied together",
            ));
        }

        let active_baseline = sqlx::query(
            "SELECT b.id, b.current_revision_id, r.content_digest,
                    r.adaptive_envelope_json, r.document_revisions_json
             FROM project_execution_baseline AS b
             JOIN project_execution_baseline_revision AS r
               ON r.id = b.current_revision_id AND r.baseline_id = b.id
             WHERE b.project_id = ? AND b.lifecycle = 'active'
               AND r.lifecycle = 'approved'
             ORDER BY b.updated_at DESC, b.id DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?;
        let mut active_baseline_document_ids = Vec::new();
        if let Some(baseline) = active_baseline {
            let active_baseline_id: String = baseline.try_get("id")?;
            let active_baseline_revision_id: String = baseline.try_get("current_revision_id")?;
            let adaptive_envelope_json: String = baseline.try_get("adaptive_envelope_json")?;
            let adaptive_envelope: Value =
                serde_json::from_str(&adaptive_envelope_json).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "active execution baseline adaptive envelope is invalid: {error}"
                    ))
                })?;
            validate_adaptive_envelope_value(&adaptive_envelope)?;
            let envelope_digest = adaptive_envelope_digest(&adaptive_envelope)?;
            if requested_baseline_id.as_deref() != Some(active_baseline_id.as_str())
                || requested_baseline_revision_id.as_deref()
                    != Some(active_baseline_revision_id.as_str())
                || requested_envelope_digest.as_deref() != Some(envelope_digest.as_str())
            {
                return Err(ServiceError::conflict(
                    "Project Document approval must bind the exact active baseline and adaptive envelope",
                ));
            }
            let baseline_documents_json: String = baseline.try_get("document_revisions_json")?;
            let baseline_documents: Vec<ArtifactRef> =
                serde_json::from_str(&baseline_documents_json).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "active execution baseline Document references are invalid: {error}"
                    ))
                })?;
            active_baseline_document_ids.extend(
                baseline_documents
                    .iter()
                    .map(|reference| reference.artifact_id.clone()),
            );
            if active_baseline_document_ids
                .iter()
                .any(|artifact_id| artifact_id == &document.id)
            {
                return Err(ServiceError::invalid_operation(
                    "Project Agent cannot approve a Document selected by the active execution baseline",
                ));
            }
        } else if requested_baseline_id.is_some() {
            return Err(ServiceError::conflict(
                "Project Document approval names a baseline but the Project has no active baseline",
            ));
        }

        // A required document-approval check is a release gate even when the
        // Document's own policy is agent-admissible.  The check target is
        // carried by the immutable milestone definition's ArtifactRefs; a
        // malformed definition cannot be treated as an empty gate.
        let gate_rows = sqlx::query(
            "SELECT r.document_revisions_json
             FROM project_milestone_check AS c
             JOIN project_milestone AS m
               ON m.id = c.milestone_id AND m.project_id = c.project_id
             JOIN project_milestone_revision AS r
               ON r.id = c.definition_revision_id AND r.milestone_id = m.id
             WHERE c.project_id = ? AND c.required = 1
               AND c.source_kind = 'document_approval'
               AND m.lifecycle IN ('planned', 'active', 'ready_for_release')",
        )
        .bind(project_id)
        .fetch_all(self.db.pool())
        .await?;
        for row in gate_rows {
            let document_revisions_json: String = row.try_get("document_revisions_json")?;
            let references: Vec<ArtifactRef> = serde_json::from_str(&document_revisions_json)
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "release-gating milestone Document references are invalid: {error}"
                    ))
                })?;
            if references.is_empty()
                || references
                    .iter()
                    .any(|reference| reference.artifact_id == document.id)
            {
                return Err(ServiceError::invalid_operation(
                    "Project Agent cannot approve a release-gating Document",
                ));
            }
        }

        // Execution Plans are the material execution contract by definition;
        // their approval must remain an explicit user decision even if a
        // caller labels the policy as agent-admissible.
        if document.kind == "execution_plan" {
            return Err(ServiceError::invalid_operation(
                "Project Agent cannot approve the material execution-plan Document",
            ));
        }

        let now = now_rfc3339();
        let approval = ProjectOrchestrationRepo::approve_project_document(
            &*self.db,
            ApproveProjectDocument {
                id: new_uuid_v4(),
                document_id: document.id.clone(),
                revision_id: revision.id.clone(),
                expected_document_version,
                principal_type: "agent".to_owned(),
                principal_id: action.actor_identity_id.clone(),
                authorization_basis: format!(
                    "project_agent_document_policy:{}:{}:{}",
                    document.approval_policy, binding_id, binding_policy_revision
                ),
                authorization_action: "project.document.approve".to_owned(),
                explicit_event: action.id.clone(),
                authorization_occurred_at: action.created_at.clone(),
                content_digest: revision.content_digest.clone(),
                rendered_digest: revision.rendered_digest.clone(),
                idempotency_key: format!("project.document.agent-approve:{}", action.dedupe_key),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_DOCUMENT_OPERATION,
            "project_id": project_id,
            "document_id": document.id,
            "revision_id": approval.revision_id,
            "approval_id": approval.id,
            "content_digest": approval.content_digest,
            "render_digest": approval.rendered_digest,
            "principal_type": approval.principal_type,
            "principal_id": approval.principal_id,
            "lifecycle": approval.lifecycle,
            "domain_committed": true,
            "requires_user_authorization": false,
        }))
    }

    async fn materialize_decision_checked(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let operation = string(payload, "action")?;
        if !matches!(operation.as_str(), "record_candidate" | "record_effective") {
            return Err(ServiceError::invalid_operation(
                "Project Agent may record implementation choices or propose candidates; supersession, invalidation, and user-scope decisions remain user-only",
            ));
        }
        if payload.get("decision_class").and_then(Value::as_str) != Some("project_implementation") {
            return Err(ServiceError::invalid_operation(
                "Project Agent decisions must use the project_implementation class",
            ));
        }
        let baseline_id = string(payload, "baseline_id")?;
        let baseline_revision_id = string(payload, "baseline_revision_id")?;
        let baseline = sqlx::query(
            "SELECT b.id, r.charter_revision_id, r.content_digest AS baseline_content_digest,
                    r.render_version AS baseline_render_version,
                    r.rendered_digest AS baseline_render_digest,
                    r.document_revisions_json,
                    r.milestone_id, r.milestone_ids_json, r.primary_milestone_id,
                    r.adaptive_envelope_json
             FROM project_execution_baseline AS b
             JOIN project_execution_baseline_revision AS r
               ON r.id = b.current_revision_id
             WHERE b.id = ? AND b.project_id = ? AND b.lifecycle = 'active'
               AND b.current_revision_id = ? AND r.lifecycle = 'approved'
             LIMIT 1",
        )
        .bind(&baseline_id)
        .bind(project_id)
        .bind(&baseline_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict(
                "Project decision must reference the exact approved revision of the active baseline",
            )
        })?;
        let baseline_charter_revision_id: String = baseline.try_get("charter_revision_id")?;
        let baseline_document_revisions_json: String =
            baseline.try_get("document_revisions_json")?;
        let baseline_milestone_id: Option<String> = baseline.try_get("milestone_id")?;
        let baseline_milestone_ids_json: String = baseline.try_get("milestone_ids_json")?;
        let baseline_primary_milestone_id: Option<String> =
            baseline.try_get("primary_milestone_id")?;
        let adaptive_envelope_json: String = baseline.try_get("adaptive_envelope_json")?;
        let adaptive_envelope = parse_adaptive_envelope(&adaptive_envelope_json)?;
        let baseline_document_revisions: Vec<ArtifactRef> =
            serde_json::from_str(&baseline_document_revisions_json).map_err(|_| {
                ServiceError::invalid_operation("active baseline Document references are invalid")
            })?;
        let affected_artifact_refs: Vec<ArtifactRef> = serde_json::from_value(
            payload
                .get("affected_artifact_refs")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|_| ServiceError::invalid_operation("affected artifact references are invalid"))?;
        let affected_task_ids: Vec<String> = serde_json::from_value(
            payload
                .get("affected_task_ids")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|_| ServiceError::invalid_operation("affected Task IDs are invalid"))?;
        let affected_milestone_ids: Vec<String> = serde_json::from_value(
            payload
                .get("affected_milestone_ids")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|_| ServiceError::invalid_operation("affected milestone IDs are invalid"))?;

        // Resolve references before deciding whether the choice is inside the
        // baseline.  A cross-Project or stale reference is a hard denial;
        // a same-Project reference not named by the baseline becomes a
        // reconciliation candidate below.
        for reference in &affected_artifact_refs {
            let row = sqlx::query(
                "SELECT r.render_version, r.rendered_digest
                 FROM project_document_revision AS r
                 JOIN project_document AS d ON d.id = r.document_id
                 WHERE d.project_id = ? AND d.id = ? AND r.id = ? AND r.content_digest = ?
                 UNION ALL
                 SELECT r.render_version, r.rendered_digest
                 FROM project_charter_revision AS r
                 JOIN project_charter AS c ON c.id = r.charter_id
                 WHERE c.project_id = ? AND c.id = ? AND r.id = ? AND r.content_digest = ?
                 UNION ALL
                 SELECT r.render_version, r.rendered_digest
                 FROM project_execution_baseline_revision AS r
                 JOIN project_execution_baseline AS b ON b.id = r.baseline_id
                 WHERE b.project_id = ? AND b.id = ? AND r.id = ? AND r.content_digest = ?
                 UNION ALL
                 SELECT r.render_version, r.rendered_digest
                 FROM project_milestone_revision AS r
                 JOIN project_milestone AS m ON m.id = r.milestone_id
                 WHERE m.project_id = ? AND m.id = ? AND r.id = ? AND r.content_digest = ?
                 LIMIT 1",
            )
            .bind(project_id)
            .bind(&reference.artifact_id)
            .bind(&reference.revision_id)
            .bind(&reference.content_digest)
            .bind(project_id)
            .bind(&reference.artifact_id)
            .bind(&reference.revision_id)
            .bind(&reference.content_digest)
            .bind(project_id)
            .bind(&reference.artifact_id)
            .bind(&reference.revision_id)
            .bind(&reference.content_digest)
            .bind(project_id)
            .bind(&reference.artifact_id)
            .bind(&reference.revision_id)
            .bind(&reference.content_digest)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "decision artifact reference is unknown, stale, or outside Project scope",
                )
            })?;
            let render_version: String = row.try_get("render_version")?;
            let render_digest: String = row.try_get("rendered_digest")?;
            if reference
                .render_version
                .as_deref()
                .is_some_and(|value| value != render_version)
                || reference
                    .render_digest
                    .as_deref()
                    .is_some_and(|value| value != render_digest)
            {
                return Err(ServiceError::conflict(
                    "decision artifact reference render digest is stale",
                ));
            }
        }
        for task_id in &affected_task_ids {
            let owned: Option<i64> = sqlx::query_scalar(
                "SELECT 1
                     FROM task AS t
                     JOIN project_task_governance AS g ON g.task_id = t.id
                     WHERE t.id = ? AND t.project_id = ?
                       AND g.baseline_id = ? AND g.baseline_revision_id = ?
                     LIMIT 1",
            )
            .bind(task_id)
            .bind(project_id)
            .bind(&baseline_id)
            .bind(&baseline_revision_id)
            .fetch_optional(self.db.pool())
            .await?;
            if owned.is_none() {
                return Err(ServiceError::invalid_operation(
                    "decision affected Task crosses Project scope",
                ));
            }
        }
        for milestone_id in &affected_milestone_ids {
            let owned: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_milestone WHERE id = ? AND project_id = ? LIMIT 1",
            )
            .bind(milestone_id)
            .bind(project_id)
            .fetch_optional(self.db.pool())
            .await?;
            if owned.is_none() {
                return Err(ServiceError::invalid_operation(
                    "decision affected milestone crosses Project scope",
                ));
            }
        }

        let baseline_charter = sqlx::query(
            "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_charter_revision AS r
             JOIN project_charter AS c ON c.id = r.charter_id
             WHERE r.id = ? AND c.project_id = ? LIMIT 1",
        )
        .bind(&baseline_charter_revision_id)
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict("active baseline Charter is outside Project scope")
        })?;
        let baseline_charter_id: String = baseline_charter.try_get("artifact_id")?;
        let baseline_charter_content_digest: String = baseline_charter.try_get("content_digest")?;
        let baseline_charter_render_version: String = baseline_charter.try_get("render_version")?;
        let baseline_charter_render_digest: String = baseline_charter.try_get("rendered_digest")?;
        let baseline_charter_lifecycle: String = baseline_charter.try_get("lifecycle")?;
        let baseline_content_digest: String = baseline.try_get("baseline_content_digest")?;
        let baseline_render_version: String = baseline.try_get("baseline_render_version")?;
        let baseline_render_digest: String = baseline.try_get("baseline_render_digest")?;
        if baseline_charter_content_digest.trim().is_empty()
            || baseline_charter_render_version.trim().is_empty()
            || baseline_charter_render_digest.trim().is_empty()
            || baseline_content_digest.trim().is_empty()
            || baseline_render_version.trim().is_empty()
            || baseline_render_digest.trim().is_empty()
            || baseline_charter_lifecycle != "approved"
        {
            return Err(ServiceError::invalid_operation(
                "active baseline references are missing exact immutable digests",
            ));
        }
        let mut baseline_artifacts = baseline_document_revisions;
        baseline_artifacts.push(ArtifactRef {
            artifact_id: baseline_charter_id,
            revision_id: baseline_charter_revision_id.clone(),
            content_digest: baseline_charter_content_digest,
            render_version: Some(baseline_charter_render_version),
            render_digest: Some(baseline_charter_render_digest),
        });
        baseline_artifacts.push(ArtifactRef {
            artifact_id: baseline_id.clone(),
            revision_id: baseline_revision_id.clone(),
            content_digest: baseline_content_digest,
            render_version: Some(baseline_render_version),
            render_digest: Some(baseline_render_digest),
        });
        let references_inside_baseline = affected_artifact_refs.iter().all(|reference| {
            baseline_artifacts.iter().any(|allowed| {
                reference.artifact_id == allowed.artifact_id
                    && reference.revision_id == allowed.revision_id
                    && reference.content_digest == allowed.content_digest
                    && reference.render_version == allowed.render_version
                    && reference.render_digest == allowed.render_digest
            })
        });
        let milestones_inside_baseline = affected_milestone_ids.iter().all(|milestone_id| {
            baseline_milestone_id.as_deref() == Some(milestone_id.as_str())
                || baseline_primary_milestone_id.as_deref() == Some(milestone_id.as_str())
                || json_contains_identifier(&baseline_milestone_ids_json, milestone_id)
        });
        let selected_outcome = optional_string(payload, "selected_outcome");
        let outcome_inside_envelope =
            selected_outcome_is_inside_envelope(&adaptive_envelope, selected_outcome.as_deref());
        let reconciliation_reason = if !references_inside_baseline {
            Some("affected artifact is outside the active baseline".to_owned())
        } else if !milestones_inside_baseline {
            Some("affected milestone is outside the active baseline".to_owned())
        } else if !outcome_inside_envelope {
            Some("selected outcome is outside the active adaptive envelope".to_owned())
        } else {
            None
        };
        let options = payload
            .get("options")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let expected_project_version = integer(payload, "expected_project_version")?;
        let current_project_version = project_version(&self.db, project_id).await?;
        if expected_project_version != current_project_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let question = string(payload, "question")?;
        let rationale = optional_string(payload, "rationale");
        let context_json = json!({
            "decision_class": "project_implementation",
            "summary": reconciliation_reason.as_ref().map_or_else(
                || "Implementation choice inside the active execution baseline".to_owned(),
                |reason| format!("reconciliation_required: {reason}"),
            ),
            "affected_artifact_refs": affected_artifact_refs,
            "affected_task_ids": affected_task_ids,
            "affected_milestone_ids": affected_milestone_ids,
            "governing_baseline_revision_id": baseline_revision_id.clone(),
        })
        .to_string();
        let context_value: Value = serde_json::from_str(&context_json).map_err(|_| {
            ServiceError::invalid_operation("Project decision context could not be encoded")
        })?;
        let affected_records_json = json!({
            "artifact_refs": context_value
                .get("affected_artifact_refs")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "task_ids": context_value
                .get("affected_task_ids")
                .cloned()
                .unwrap_or_else(|| json!([])),
            "milestone_ids": context_value
                .get("affected_milestone_ids")
                .cloned()
                .unwrap_or_else(|| json!([])),
        })
        .to_string();
        let source_refs_json = json!([{
            "source_kind": "project_chat",
            "source_id": action.id,
            "observed_at": now_rfc3339(),
        }])
        .to_string();
        if operation == "record_effective" && reconciliation_reason.is_none() {
            let selected_outcome = selected_outcome.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "an effective implementation decision requires selected_outcome",
                )
            })?;
            let rationale = rationale.ok_or_else(|| {
                ServiceError::invalid_operation(
                    "an effective implementation decision requires rationale",
                )
            })?;
            let decision_id = string(payload, "decision_id")?;
            let decision = ProjectOrchestrationRepo::append_project_decision(
                &*self.db,
                db::CreateProjectDecision {
                    id: decision_id,
                    project_id: project_id.to_owned(),
                    expected_project_version,
                    state: "active".to_owned(),
                    decision_class: "project_implementation".to_owned(),
                    question,
                    context_json,
                    options_json: options.to_string(),
                    selected_outcome,
                    rationale,
                    principal_type: "agent".to_owned(),
                    principal_id: action.actor_identity_id.clone(),
                    authority_basis: "active_execution_baseline_adaptive_envelope".to_owned(),
                    authorization_action: "project.decision.record_effective".to_owned(),
                    explicit_event: action.id.clone(),
                    authorization_occurred_at: action.created_at.clone(),
                    charter_revision_id: Some(baseline_charter_revision_id),
                    baseline_revision_id: Some(baseline_revision_id),
                    source_refs_json,
                    affected_records_json,
                    supersedes_decision_id: None,
                    created_at: now_rfc3339(),
                },
            )
            .await?;
            return Ok(json!({
                "operation": PROJECT_DECISION_OPERATION,
                "project_id": project_id,
                "decision_id": decision.id,
                "state": decision.state,
                "authority_basis": decision.authority_basis,
                "domain_committed": true,
                "requires_user_authorization": false,
            }));
        }
        let candidate = ProjectOrchestrationRepo::create_project_decision_candidate(
            &*self.db,
            CreateProjectDecisionCandidate {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                lifecycle: "proposed".to_owned(),
                question,
                context_json,
                options_json: options.to_string(),
                selected_outcome,
                rationale,
                principal_type: Some("agent".to_owned()),
                principal_id: Some(action.actor_identity_id.clone()),
                source_refs_json,
                expected_project_version,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_DECISION_OPERATION,
            "project_id": project_id,
            "candidate_id": candidate.id,
            "lifecycle": candidate.lifecycle,
            "reconciliation_required": reconciliation_reason.is_some(),
            "reconciliation_reason": reconciliation_reason,
            "domain_committed": true,
            "requires_user_authorization": true,
        }))
    }

    async fn materialize_execution_baseline(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let baseline_id = string(payload, "baseline_id")?;
        let operation = string(payload, "action")?;
        if !matches!(
            operation.as_str(),
            "draft_revision" | "revise" | "propose_approval"
        ) {
            return Err(ServiceError::invalid_operation(
                "Project Agent may draft or propose a baseline; approval and activation are user-only",
            ));
        }
        if operation == "propose_approval" {
            // The Project Agent can prepare an exact approval target, but the
            // approval itself remains an interactive-user-only mutation.
            if payload.get("approval_id").is_some() {
                return Err(ServiceError::invalid_operation(
                    "Project Agent cannot create or consume an execution-baseline approval receipt",
                ));
            }
        }

        let content = if let Some(content) = payload.get("content") {
            // Derive the release-policy digest from the caller's own frozen
            // policy when it is omitted from a full content object.
            let mut content_value = content.clone();
            // The Charter ArtifactRef digests are server truths: resolve them
            // from the referenced approved revision instead of requiring the
            // caller to reproduce them byte-for-byte.
            if let Some(revision_id) = content_value
                .get("charter_revision")
                .and_then(|reference| reference.get("revision_id"))
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                let charter = sqlx::query(
                    "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                            r.rendered_digest
                     FROM project_charter_revision r
                     JOIN project_charter c ON c.id = r.charter_id
                     WHERE r.id = ? AND c.project_id = ? AND r.lifecycle = 'approved'",
                )
                .bind(&revision_id)
                .bind(project_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| {
                    ServiceError::conflict(
                        "baseline Charter revision is not approved and Project-scoped",
                    )
                })?;
                content_value["charter_revision"] = json!({
                    "artifact_id": charter.try_get::<String, _>("artifact_id")?,
                    "revision_id": revision_id,
                    "content_digest": charter.try_get::<String, _>("content_digest")?,
                    "render_version": charter.try_get::<String, _>("render_version")?,
                    "render_digest": charter.try_get::<String, _>("rendered_digest")?,
                });
            }
            // Document ArtifactRef digests are likewise server truths keyed
            // by revision_id.
            let document_reference_ids: Vec<(usize, String)> = content_value
                .get("document_revisions")
                .and_then(Value::as_array)
                .map(|references| {
                    references
                        .iter()
                        .enumerate()
                        .filter_map(|(index, reference)| {
                            reference
                                .get("revision_id")
                                .and_then(Value::as_str)
                                .map(|revision_id| (index, revision_id.to_owned()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            for (index, revision_id) in document_reference_ids {
                let row = sqlx::query(
                    "SELECT d.id AS artifact_id, r.content_digest, r.render_version,
                            r.rendered_digest
                     FROM project_document_revision r
                     JOIN project_document d ON d.id = r.document_id
                     WHERE r.id = ? AND d.project_id = ?",
                )
                .bind(&revision_id)
                .bind(project_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| {
                    ServiceError::conflict("baseline Document revision is not Project-scoped")
                })?;
                content_value["document_revisions"][index] = json!({
                    "artifact_id": row.try_get::<String, _>("artifact_id")?,
                    "revision_id": revision_id,
                    "content_digest": row.try_get::<String, _>("content_digest")?,
                    "render_version": row.try_get::<String, _>("render_version")?,
                    "render_digest": row.try_get::<String, _>("rendered_digest")?,
                });
            }
            if let Some(object) = content_value.as_object_mut() {
                let digest_missing = object
                    .get("release_policy_digest")
                    .and_then(Value::as_str)
                    .map(|value| value.trim().is_empty())
                    .unwrap_or(true);
                if digest_missing {
                    if let Some(policy_value) = object.get("release_policy").cloned() {
                        let policy: api_types::ExecutionBaselineReleasePolicy =
                            serde_json::from_value(policy_value).map_err(|error| {
                                ServiceError::invalid_operation(format!(
                                    "invalid release policy: {error}"
                                ))
                            })?;
                        let digest = crate::execution_baseline::release_policy_digest(&policy)
                            .map_err(|error| {
                                ServiceError::invalid_operation(format!(
                                    "release policy digest: {error}"
                                ))
                            })?;
                        object.insert("release_policy_digest".to_owned(), json!(digest));
                    }
                }
            }
            from_value::<ExecutionBaselineContent>(&json!({ "content": content_value }), "content")?
        } else {
            let release_policy_value = payload.get("release_policy").cloned().ok_or_else(|| {
                ServiceError::invalid_operation(
                    "execution baseline proposals require a complete frozen release_policy",
                )
            })?;
            // The digest is derived from the caller's own frozen policy; the
            // server computes it when the caller omits it.
            let release_policy_digest_value = match payload
                .get("release_policy_digest")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                Some(digest) => digest.to_owned(),
                None => {
                    let policy: api_types::ExecutionBaselineReleasePolicy =
                        serde_json::from_value(release_policy_value.clone()).map_err(|error| {
                            ServiceError::invalid_operation(format!(
                                "invalid release policy: {error}"
                            ))
                        })?;
                    crate::execution_baseline::release_policy_digest(&policy).map_err(|error| {
                        ServiceError::invalid_operation(format!("release policy digest: {error}"))
                    })?
                }
            };
            let charter_revision_id = string(payload, "charter_revision_id")?;
            let charter = sqlx::query(
                "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                        r.rendered_digest
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND c.project_id = ? AND r.lifecycle = 'approved'",
            )
            .bind(&charter_revision_id)
            .bind(project_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::conflict(
                    "baseline Charter revision is not approved and Project-scoped",
                )
            })?;
            let charter_ref = json!({
                "artifact_id": charter.try_get::<String, _>("artifact_id")?,
                "revision_id": charter_revision_id,
                "content_digest": charter.try_get::<String, _>("content_digest")?,
                "render_version": charter.try_get::<String, _>("render_version")?,
                "render_digest": charter.try_get::<String, _>("rendered_digest")?,
            });
            serde_json::from_value(json!({
                "charter_revision": charter_ref,
                "document_revisions": payload.get("document_revisions").cloned().unwrap_or_else(|| json!([])),
                "plan_item_ids": payload.get("plan_item_ids").cloned().unwrap_or_else(|| json!([])),
                "milestone_ids": payload.get("milestone_ids").cloned().unwrap_or_else(|| json!([])),
                "milestone_definition_revision_ids": payload
                    .get("milestone_definition_revision_ids")
                    .cloned()
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "execution baseline proposals require exact milestone definition revisions",
                        )
                    })?,
                "primary_milestone_id": payload.get("primary_milestone_id").cloned().unwrap_or(Value::Null),
                "release_policy_revision": string(payload, "release_policy_revision")?,
                "release_policy_digest": release_policy_digest_value,
                "release_policy": release_policy_value,
                "acceptance_evidence_matrix": payload.get("acceptance_evidence_matrix").cloned().unwrap_or_else(|| json!([])),
                "capability_classes": payload.get("capability_classes").cloned().unwrap_or_else(|| json!([])),
                "risk_classes": payload.get("risk_classes").cloned().unwrap_or_else(|| json!([])),
                "reviewer_independence_rules": payload.get("reviewer_independence_rules").cloned().unwrap_or_else(|| json!([])),
                "elevated_operations": payload.get("elevated_operations").cloned().unwrap_or_else(|| json!([])),
                "adaptive_envelope": payload.get("adaptive_envelope").cloned().unwrap_or_else(|| json!({})),
                "rollback_and_recovery": payload.get("rollback_and_recovery").cloned().unwrap_or_else(|| json!([])),
                "exclusions": payload.get("exclusions").cloned().unwrap_or_else(|| json!([])),
            }))
            .map_err(|error| ServiceError::invalid_operation(format!("invalid execution baseline content: {error}")))?
        };

        validate_execution_baseline_policy(&content).map_err(ServiceError::conflict)?;
        let project_charter_revision: Option<String> = sqlx::query_scalar(
            "SELECT current_charter_revision_id FROM project
             WHERE id = ? AND charter_status = 'charter_backed' AND charter_setup_required = 0",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await?
        .flatten();
        if project_charter_revision.as_deref()
            != Some(content.charter_revision.revision_id.as_str())
        {
            return Err(ServiceError::conflict(
                "execution baseline must reference the current approved Project Charter revision",
            ));
        }
        validate_agent_baseline_artifacts(&self.db, project_id, &content).await?;

        let rendered = render_execution_baseline(&content).map_err(|error| {
            ServiceError::invalid_operation(format!("render execution baseline: {error}"))
        })?;
        for (field, supplied, computed) in [
            (
                "rendered_view",
                payload.get("rendered_view").and_then(Value::as_str),
                Some(rendered.rendered_view.as_str()),
            ),
            (
                "content_digest",
                payload.get("content_digest").and_then(Value::as_str),
                Some(rendered.content_digest.as_str()),
            ),
            (
                "render_digest",
                payload.get("render_digest").and_then(Value::as_str),
                Some(rendered.render_digest.as_str()),
            ),
        ] {
            if let Some(supplied) = supplied {
                if Some(supplied) != computed {
                    return Err(ServiceError::conflict(format!(
                        "execution baseline {field} does not match the server renderer"
                    )));
                }
            }
        }
        if let Some(schema_version) = payload.get("schema_version").and_then(Value::as_str) {
            if schema_version != EXECUTION_BASELINE_SCHEMA_VERSION {
                return Err(ServiceError::conflict(
                    "execution baseline schema version is stale",
                ));
            }
        }
        if let Some(render_version) = payload.get("render_version").and_then(Value::as_str) {
            if render_version != EXECUTION_BASELINE_RENDER_VERSION {
                return Err(ServiceError::conflict(
                    "execution baseline render version is stale",
                ));
            }
        }

        let columns = baseline_column_json(&content)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let manifest = json!({
            "schema": "forge.execution-baseline-manifest/v1",
            "content": content,
            "rendered_view": rendered.rendered_view,
            "provenance": payload.get("provenance").cloned().unwrap_or_else(|| json!({})),
            "source_action_id": action.id.clone(),
        });
        let now = now_rfc3339();
        let mut tx = self.db.pool().begin().await?;
        let baseline = sqlx::query(
            "SELECT project_id, lifecycle, version, current_revision_id
             FROM project_execution_baseline WHERE id = ?",
        )
        .bind(&baseline_id)
        .fetch_optional(&mut *tx)
        .await?;
        let (baseline_lifecycle, baseline_version, current_revision_id) =
            if let Some(row) = baseline {
                let owner: String = row.try_get("project_id")?;
                if owner != project_id {
                    return Err(ServiceError::invalid_operation(
                        "execution baseline crosses Project scope",
                    ));
                }
                (
                    row.try_get::<String, _>("lifecycle")?,
                    row.try_get::<i64, _>("version")?,
                    row.try_get::<Option<String>, _>("current_revision_id")?,
                )
            } else {
                sqlx::query(
                    "INSERT INTO project_execution_baseline
                 (id, project_id, lifecycle, version, created_at, updated_at)
                 VALUES (?, ?, 'draft', 1, ?, ?)",
                )
                .bind(&baseline_id)
                .bind(project_id)
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
                ("draft".to_owned(), 1, None)
            };
        let expected_baseline_version = payload
            .get("expected_baseline_version")
            .and_then(Value::as_i64)
            .unwrap_or(baseline_version);
        if expected_baseline_version != baseline_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let base_revision_id = payload
            .get("base_revision_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if current_revision_id.is_some() && base_revision_id.is_none() {
            return Err(ServiceError::conflict(
                "base_revision_id is required when revising an execution baseline",
            ));
        }
        if current_revision_id.is_none() && base_revision_id.is_some() {
            return Err(ServiceError::conflict(
                "a first execution baseline revision cannot name a base revision",
            ));
        }
        let base_revision = if let Some(base_revision_id) = base_revision_id.as_deref() {
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM project_execution_baseline_revision
                 WHERE id = ? AND baseline_id = ?",
            )
            .bind(base_revision_id)
            .bind(&baseline_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ServiceError::conflict("execution baseline base revision is stale"))?
        } else {
            0
        };
        if base_revision_id.is_some()
            && current_revision_id.as_deref() != base_revision_id.as_deref()
        {
            return Err(ServiceError::conflict(
                "execution baseline base revision is not current",
            ));
        }
        let revision_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_execution_baseline_revision WHERE baseline_id = ?",
        )
        .bind(&baseline_id)
        .fetch_one(&mut *tx)
        .await?;
        let revision_id = new_uuid_v4();
        let milestone_ids_json = serde_json::to_string(&content.milestone_ids)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        sqlx::query(
            "INSERT INTO project_execution_baseline_revision (
                id, baseline_id, revision, base_revision, base_revision_id, lifecycle,
                charter_revision_id, document_revisions_json, plan_items_json,
                milestone_id, milestone_ids_json, milestone_definition_revision_ids_json,
                primary_milestone_id, release_policy_json,
                release_policy_revision, release_policy_digest, acceptance_matrix_json,
                capability_classes_json, risk_classes_json, adaptive_envelope_json,
                elevated_operations_json, exclusions_json, rollback_recovery_json,
                schema_version, render_version, rendered_view, content_digest,
                rendered_digest, source_refs_json, created_at
             ) VALUES (
                 ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             )",
        )
        .bind(&revision_id)
        .bind(&baseline_id)
        .bind(revision_number)
        .bind(base_revision)
        .bind(base_revision_id.as_deref())
        .bind(&content.charter_revision.revision_id)
        .bind(&columns.document_revisions_json)
        .bind(&columns.plan_items_json)
        .bind(columns.milestone_id.as_deref())
        .bind(&milestone_ids_json)
        .bind(&columns.milestone_definition_revision_ids_json)
        .bind(columns.primary_milestone_id.as_deref())
        .bind(&columns.release_policy_json)
        .bind(&content.release_policy_revision)
        .bind(&content.release_policy_digest)
        .bind(&columns.acceptance_matrix_json)
        .bind(&columns.capability_classes_json)
        .bind(&columns.risk_classes_json)
        .bind(&columns.adaptive_envelope_json)
        .bind(&columns.elevated_operations_json)
        .bind(&columns.exclusions_json)
        .bind(&columns.rollback_recovery_json)
        .bind(EXECUTION_BASELINE_SCHEMA_VERSION)
        .bind(EXECUTION_BASELINE_RENDER_VERSION)
        .bind(&rendered.rendered_view)
        .bind(&rendered.content_digest)
        .bind(&rendered.render_digest)
        .bind(
            serde_json::to_string(&manifest)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        )
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        let advanced = sqlx::query(
            "UPDATE project_execution_baseline
             SET current_revision_id = CASE WHEN lifecycle IN ('draft', 'proposed') THEN ? ELSE current_revision_id END,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(&revision_id)
        .bind(&now)
        .bind(&baseline_id)
        .bind(baseline_version)
        .execute(&mut *tx)
        .await?;
        if advanced.rows_affected() != 1 {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        tx.commit().await?;
        Ok(json!({
            "operation": PROJECT_EXECUTION_BASELINE_OPERATION,
            "project_id": project_id,
            "baseline_id": baseline_id,
            "revision_id": revision_id,
            "revision": revision_number,
            "lifecycle": "proposed",
            "baseline_lifecycle": baseline_lifecycle,
            "content_digest": rendered.content_digest,
            "render_digest": rendered.render_digest,
            "domain_committed": true,
        }))
    }

    async fn materialize_milestone(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let operation = string(payload, "action")?;
        if operation == "set_primary" {
            let primary_id = string(payload, "primary_milestone_id")?;
            let milestone = ProjectOrchestrationRepo::get_project_milestone(&*self.db, &primary_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_milestone", primary_id.clone()))?;
            if milestone.project_id != project_id {
                return Err(ServiceError::invalid_operation(
                    "primary milestone crosses Project scope",
                ));
            }
            let expected = integer(payload, "expected_milestone_version")?;
            let now = now_rfc3339();
            let update = sqlx::query(
                "UPDATE project SET primary_milestone_id = ?, version = version + 1,
                 updated_at = ? WHERE id = ? AND version = ?",
            )
            .bind(&primary_id)
            .bind(&now)
            .bind(project_id)
            .bind(expected)
            .execute(self.db.pool())
            .await?;
            if update.rows_affected() != 1 {
                return Err(ServiceError::Db(db::DbError::VersionConflict));
            }
            return Ok(json!({
                "operation": PROJECT_MILESTONE_OPERATION,
                "project_id": project_id,
                "milestone_id": primary_id,
                "primary": true,
                "domain_committed": true,
                "requires_user_authorization": false,
            }));
        }

        let content: MilestoneDefinitionContent = from_value(payload, "content")?;
        let (milestone_id, expected_version, base_revision_id, base_revision) =
            if operation == "define" {
                let project_version = project_version(&self.db, project_id).await?;
                let sequence: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(milestone_sequence), 0) + 1
                     FROM project_milestone WHERE project_id = ?",
                )
                .bind(project_id)
                .fetch_one(self.db.pool())
                .await?;
                let milestone = ProjectOrchestrationRepo::create_project_milestone(
                    &*self.db,
                    CreateProjectMilestone {
                        id: new_uuid_v4(),
                        project_id: project_id.to_owned(),
                        expected_project_version: project_version,
                        milestone_sequence: sequence,
                        milestone_key: crate::milestone_identity(sequence)
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                        display_label: optional_string(payload, "display_label")
                            .or_else(|| Some(content.name.clone())),
                        created_at: now_rfc3339(),
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                (milestone.id, 1, None, 0)
            } else {
                let milestone_id = string(payload, "milestone_id")?;
                let milestone =
                    ProjectOrchestrationRepo::get_project_milestone(&*self.db, &milestone_id)
                        .await?
                        .ok_or_else(|| {
                            ServiceError::not_found("project_milestone", milestone_id.clone())
                        })?;
                if milestone.project_id != project_id {
                    return Err(ServiceError::invalid_operation(
                        "milestone revision crosses Project scope",
                    ));
                }
                let revisions = ProjectOrchestrationRepo::list_project_milestone_revisions(
                    &*self.db,
                    &milestone_id,
                )
                .await?;
                let base = revisions
                    .iter()
                    .find(|revision| {
                        Some(revision.id.as_str())
                            == milestone.current_definition_revision_id.as_deref()
                    })
                    .ok_or_else(|| ServiceError::conflict("milestone has no current definition"))?;
                (
                    milestone.id,
                    integer(payload, "expected_milestone_version")?,
                    Some(base.id.clone()),
                    base.revision,
                )
            };

        let canonical = canonical_json(&content)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let revision = ProjectOrchestrationRepo::create_project_milestone_revision(
            &*self.db,
            CreateProjectMilestoneRevision {
                id: new_uuid_v4(),
                milestone_id: milestone_id.clone(),
                expected_milestone_version: expected_version,
                base_revision,
                base_revision_id,
                lifecycle: if operation == "define" {
                    "draft".to_owned()
                } else {
                    "proposed".to_owned()
                },
                display_label: Some(content.name.clone()),
                outcome: content.outcome.clone(),
                included_scope_json: serde_json::to_string(&content.included_scope)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                excluded_scope_json: serde_json::to_string(&content.excluded_scope)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                charter_revision_id: content
                    .charter_revision
                    .as_ref()
                    .map(|reference| reference.revision_id.clone()),
                document_revisions_json: serde_json::to_string(&content.document_revisions)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                task_selection_json: serde_json::to_string(&content.task_ids)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                dependencies_json: serde_json::to_string(&content.dependencies)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                risks_json: serde_json::to_string(&content.risks)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                acceptance_checks_json: serde_json::to_string(&content.acceptance_checks)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                evidence_requirements_json: serde_json::to_string(&content.evidence_requirements)
                    .map_err(|error| {
                    ServiceError::invalid_operation(error.to_string())
                })?,
                known_issues_json: serde_json::to_string(&content.known_issues)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                change_summary: "Project Agent authored a typed milestone definition".to_owned(),
                schema_version: MILESTONE_DEFINITION_SCHEMA.to_owned(),
                render_version: MILESTONE_RENDER_SCHEMA.to_owned(),
                rendered_view: canonical.clone(),
                content_digest: canonical_digest_with_schema(MILESTONE_DEFINITION_SCHEMA, &content)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                rendered_digest: canonical_digest_with_schema(MILESTONE_RENDER_SCHEMA, &canonical)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                author_type: "agent".to_owned(),
                author_id: Some(action.actor_identity_id.clone()),
                source_refs_json: "[]".to_owned(),
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_MILESTONE_OPERATION,
            "project_id": project_id,
            "milestone_id": milestone_id,
            "revision_id": revision.id,
            "revision": revision.revision,
            "lifecycle": revision.lifecycle,
            "domain_committed": true,
            "requires_user_authorization": operation != "define",
        }))
    }

    async fn materialize_evidence(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let milestone_id = string(payload, "milestone_id")?;
        let milestone = ProjectOrchestrationRepo::get_project_milestone(&*self.db, &milestone_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project_milestone", milestone_id.clone()))?;
        if milestone.project_id != project_id {
            return Err(ServiceError::invalid_operation(
                "evidence milestone crosses Project scope",
            ));
        }
        let asset_id = string(payload, "asset_id")?;
        let asset = SharedMediaRepo::get_media_asset(&*self.db, &asset_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("media_asset", asset_id.clone()))?;
        if asset.project_id != project_id
            || asset.deleted_at.is_some()
            || asset.availability != "available"
        {
            return Err(ServiceError::conflict(
                "evidence media is unavailable or cross-Project",
            ));
        }
        let requested_checksum = string(payload, "checksum")?;
        if asset.checksum.as_deref() != Some(requested_checksum.as_str()) {
            return Err(ServiceError::conflict(
                "evidence checksum does not match the media asset",
            ));
        }
        let acceptance_check_ids = payload
            .get("acceptance_check_ids")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let attachment = SharedMediaRepo::create_project_media_attachment(
            &*self.db,
            CreateProjectMediaAttachment {
                id: new_uuid_v4(),
                project_id: project_id.to_owned(),
                asset_id: asset_id.clone(),
                attachment_kind: "evidence".to_owned(),
                // Project evidence is an independent reference to the shared
                // bytes. Keeping the legacy task_media_id here would collide
                // with the legacy one-row attachment uniqueness constraint
                // when an asset is cited by multiple milestones.
                task_media_id: None,
                task_id: optional_string(payload, "task_id"),
                milestone_id: Some(milestone_id.clone()),
                milestone_check_id: None,
                source_task_id: optional_string(payload, "task_id"),
                source_execution_id: None,
                source_validation_id: None,
                acceptance_check_ids_json: acceptance_check_ids.to_string(),
                caption: Some(string(payload, "caption")?),
                evidence_kind: Some(string(payload, "kind")?),
                checksum: Some(requested_checksum),
                availability: "available".to_owned(),
                project_url: Some(format!("/api/v1/projects/{project_id}/media/{asset_id}")),
                author_type: "agent".to_owned(),
                author_id: Some(action.actor_identity_id.clone()),
                authorization_json: json!({
                    "operation": PROJECT_EVIDENCE_OPERATION,
                    "action_id": action.id,
                })
                .to_string(),
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_EVIDENCE_OPERATION,
            "project_id": project_id,
            "milestone_id": milestone_id,
            "attachment_id": attachment.id,
            "asset_id": asset_id,
            "domain_committed": true,
        }))
    }

    async fn materialize_readiness_request(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let milestone_id = string(payload, "milestone_id")?;
        let milestone = ProjectOrchestrationRepo::get_project_milestone(&*self.db, &milestone_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project_milestone", milestone_id.clone()))?;
        if milestone.project_id != project_id {
            return Err(ServiceError::invalid_operation(
                "readiness request crosses Project scope",
            ));
        }
        let baseline_id = string(payload, "baseline_id")?;
        let baseline_revision_id = string(payload, "baseline_revision_id")?;
        let policy_revision = string(payload, "release_policy_revision")?;
        let baseline = sqlx::query(
            "SELECT b.id, b.current_revision_id, r.content_digest, r.release_policy_revision,
                    r.release_policy_digest
             FROM project_execution_baseline b
             JOIN project_execution_baseline_revision r ON r.id = b.current_revision_id
             WHERE b.id = ? AND b.project_id = ? AND b.lifecycle = 'active'
               AND b.current_revision_id = ? AND r.lifecycle = 'approved' LIMIT 1",
        )
        .bind(&baseline_id)
        .bind(project_id)
        .bind(&baseline_revision_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| ServiceError::conflict("readiness request baseline is not active"))?;
        let release_policy_revision: String = baseline.try_get("release_policy_revision")?;
        if release_policy_revision != policy_revision {
            return Err(ServiceError::conflict("readiness request policy is stale"));
        }
        let actor = PrincipalRef {
            kind: PrincipalKind::Agent,
            id: action.actor_identity_id.clone(),
            display_name: None,
        };
        let authorization = AuthorizationProvenance {
            principal: actor.clone(),
            authorization_basis: "bound_project_agent_action".to_owned(),
            action: "project.milestone.readiness".to_owned(),
            event_id: action.id.clone(),
            occurred_at: action.created_at.clone(),
        };
        let snapshot = MilestoneRuntime::new(Arc::clone(&self.db))
            .evaluate(
                project_id,
                &actor,
                &authorization,
                &milestone_id,
                integer(payload, "milestone_version")?,
                &baseline_id,
                &baseline_revision_id,
                &release_policy_revision,
                &format!("project-agent-readiness:{}", action.dedupe_key),
            )
            .await?;
        Ok(json!({
            "operation": PROJECT_READINESS_OPERATION,
            "project_id": project_id,
            "milestone_id": milestone_id,
            "readiness_snapshot_id": snapshot.id,
            "readiness_digest": snapshot.readiness_digest,
            "result": snapshot.result,
            "status": "computed",
            "domain_committed": true,
        }))
    }

    async fn materialize_release_request(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
    ) -> Result<Value> {
        let milestone_id = string(payload, "milestone_id")?;
        let milestone = ProjectOrchestrationRepo::get_project_milestone(&*self.db, &milestone_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project_milestone", milestone_id.clone()))?;
        if milestone.project_id != project_id {
            return Err(ServiceError::invalid_operation(
                "release request crosses Project scope",
            ));
        }
        let snapshot_id = string(payload, "readiness_snapshot_id")?;
        let snapshot_digest = string(payload, "readiness_digest")?;
        let snapshot = sqlx::query(
            "SELECT id, readiness_digest, outcome FROM project_readiness_snapshot
             WHERE id = ? AND project_id = ? AND milestone_id = ? LIMIT 1",
        )
        .bind(&snapshot_id)
        .bind(project_id)
        .bind(&milestone_id)
        .fetch_optional(self.db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::not_found("project_readiness_snapshot", snapshot_id.clone())
        })?;
        let stored_digest: String = snapshot.try_get("readiness_digest")?;
        if stored_digest != snapshot_digest {
            return Err(ServiceError::conflict(
                "release request readiness digest is stale",
            ));
        }
        if snapshot.try_get::<String, _>("outcome")? != "ready"
            || milestone.lifecycle != "ready_for_release"
            || milestone.version != integer(payload, "milestone_version")?
        {
            return Err(ServiceError::conflict(
                "release candidate requires the current ready-for-release milestone snapshot",
            ));
        }
        let event_id = new_uuid_v4();
        let dedupe = format!("project-release-request:{}", action.dedupe_key);
        if let Some(existing) = DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe).await? {
            return Ok(json!({
                "operation": PROJECT_RELEASE_OPERATION,
                "project_id": project_id,
                "candidate_event_id": existing.id,
                "status": "pending_user_release_approval",
                "domain_committed": true,
            }));
        }
        DomainEventRepo::append_event(
            &*self.db,
            CreateDomainEvent {
                id: event_id.clone(),
                event_type: "project_release.candidate_requested".to_owned(),
                entity_type: "project_readiness_snapshot".to_owned(),
                entity_id: snapshot_id.clone(),
                actor_type: "agent".to_owned(),
                actor_id: Some(action.actor_identity_id.clone()),
                scope_type: "project".to_owned(),
                scope_id: project_id.to_owned(),
                correlation_id: action.correlation_id.clone(),
                causation_id: Some(action.id.clone()),
                causation_depth: action.causation_depth + 1,
                dedupe_key: Some(dedupe),
                payload_json: json!({
                    "milestone_id": milestone_id,
                    "milestone_version": payload.get("milestone_version"),
                    "readiness_snapshot_id": snapshot_id,
                    "readiness_digest": snapshot_digest,
                    "status": "pending_user_release_approval",
                    "final_release_created": false,
                })
                .to_string(),
                created_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(json!({
            "operation": PROJECT_RELEASE_OPERATION,
            "project_id": project_id,
            "candidate_event_id": event_id,
            "status": "pending_user_release_approval",
            "domain_committed": true,
            "final_release_created": false,
        }))
    }
}

pub fn is_project_orchestration_operation(operation: &str) -> bool {
    matches!(
        operation,
        PROJECT_CHARTER_ADOPTION_OPERATION
            | PROJECT_DOCUMENT_OPERATION
            | PROJECT_DECISION_OPERATION
            | PROJECT_EXECUTION_BASELINE_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION
    )
}

async fn validate_agent_baseline_artifacts(
    db: &SqliteDb,
    project_id: &str,
    content: &ExecutionBaselineContent,
) -> Result<()> {
    let charter = sqlx::query(
        "SELECT c.id AS artifact_id, r.content_digest, r.render_version,
                r.rendered_digest, r.lifecycle
         FROM project_charter_revision r
         JOIN project_charter c ON c.id = r.charter_id
         WHERE r.id = ? AND c.project_id = ?
         LIMIT 1",
    )
    .bind(&content.charter_revision.revision_id)
    .bind(project_id)
    .fetch_optional(db.pool())
    .await?
    .ok_or_else(|| ServiceError::conflict("baseline Charter revision is not Project-scoped"))?;
    let charter_artifact_id: String = charter.try_get("artifact_id")?;
    let charter_content_digest: String = charter.try_get("content_digest")?;
    let charter_lifecycle: String = charter.try_get("lifecycle")?;
    let charter_render_version: String = charter.try_get("render_version")?;
    let charter_render_digest: String = charter.try_get("rendered_digest")?;
    if charter_artifact_id != content.charter_revision.artifact_id
        || charter_content_digest != content.charter_revision.content_digest
        || charter_lifecycle != "approved"
        || content.charter_revision.render_version.as_deref()
            != Some(charter_render_version.as_str())
        || content.charter_revision.render_digest.as_deref() != Some(charter_render_digest.as_str())
    {
        return Err(ServiceError::conflict(
            "baseline Charter ArtifactRef does not match its approved revision",
        ));
    }
    for reference in &content.document_revisions {
        let row = sqlx::query(
            "SELECT d.id AS artifact_id, r.content_digest, r.render_version,
                    r.rendered_digest, r.lifecycle
             FROM project_document_revision r
             JOIN project_document d ON d.id = r.document_id
             WHERE r.id = ? AND d.project_id = ?",
        )
        .bind(&reference.revision_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| {
            ServiceError::conflict("baseline Document revision is not Project-scoped")
        })?;
        let artifact_id: String = row.try_get("artifact_id")?;
        let content_digest: String = row.try_get("content_digest")?;
        let lifecycle: String = row.try_get("lifecycle")?;
        let render_version: String = row.try_get("render_version")?;
        let render_digest: String = row.try_get("rendered_digest")?;
        let exact = artifact_id == reference.artifact_id
            && content_digest == reference.content_digest
            && lifecycle == "approved"
            && reference.render_version.as_deref() == Some(render_version.as_str())
            && reference.render_digest.as_deref() == Some(render_digest.as_str());
        if !exact {
            return Err(ServiceError::conflict(
                "baseline Document ArtifactRef does not match its approved revision",
            ));
        }
    }
    for milestone_id in &content.milestone_ids {
        let owned: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_milestone WHERE id = ? AND project_id = ? LIMIT 1",
        )
        .bind(milestone_id)
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?;
        if owned.is_none() {
            return Err(ServiceError::conflict(
                "baseline milestone is not Project-scoped",
            ));
        }
    }
    Ok(())
}

async fn project_version(db: &SqliteDb, project_id: &str) -> Result<i64> {
    sqlx::query_scalar("SELECT version FROM project WHERE id = ? LIMIT 1")
        .bind(project_id)
        .fetch_optional(db.pool())
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))
}

fn json_contains_identifier(value: &str, identifier: &str) -> bool {
    serde_json::from_str::<Value>(value)
        .ok()
        .is_some_and(|value| json_contains_identifier_value(&value, identifier))
}

fn json_contains_identifier_value(value: &Value, identifier: &str) -> bool {
    match value {
        Value::String(value) => value == identifier,
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_identifier_value(value, identifier)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_identifier_value(value, identifier)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn parse_document_content(
    kind: ProjectDocumentKind,
    value: &Value,
) -> Result<ProjectDocumentContent> {
    macro_rules! parse {
        ($variant:ident) => {
            serde_json::from_value(value.clone())
                .map(ProjectDocumentContent::$variant)
                .map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "invalid Project Document content: {error}"
                    ))
                })
        };
    }
    match kind {
        ProjectDocumentKind::Research => parse!(Research),
        ProjectDocumentKind::DeliveryBrief => parse!(DeliveryBrief),
        ProjectDocumentKind::ProductSpec => parse!(ProductSpec),
        ProjectDocumentKind::Design => parse!(Design),
        ProjectDocumentKind::Architecture => parse!(Architecture),
        ProjectDocumentKind::ExecutionPlan => parse!(ExecutionPlan),
    }
}

fn string(payload: &Value, field: &str) -> Result<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn optional_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn optional_nonempty_string(payload: &Value, field: &str) -> Result<Option<String>> {
    match payload.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(ServiceError::invalid_operation(format!(
            "{field} must be a non-empty string when supplied"
        ))),
    }
}

fn parse_adaptive_envelope(value: &str) -> Result<AdaptiveEnvelope> {
    let value: Value = serde_json::from_str(value).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "active execution baseline adaptive envelope is invalid: {error}"
        ))
    })?;
    validate_adaptive_envelope_value(&value)
}

fn validate_adaptive_envelope_value(value: &Value) -> Result<AdaptiveEnvelope> {
    let object = value.as_object().ok_or_else(|| {
        ServiceError::invalid_operation(
            "active execution baseline adaptive envelope must be an object",
        )
    })?;
    const FIELDS: [&str; 6] = [
        "allowed_task_operations",
        "fixed_outcomes",
        "fixed_acceptance",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "elevated_operations",
    ];
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Err(ServiceError::invalid_operation(
            "active execution baseline adaptive envelope must contain exactly its required arrays",
        ));
    }
    for field in FIELDS {
        if !object.get(field).is_some_and(Value::is_array)
            || object
                .get(field)
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().any(|value| !value.is_string()))
        {
            return Err(ServiceError::invalid_operation(format!(
                "active execution baseline adaptive envelope field {field} must be a string array"
            )));
        }
    }
    serde_json::from_value(value.clone()).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "active execution baseline adaptive envelope is invalid: {error}"
        ))
    })
}

fn adaptive_envelope_digest(value: &Value) -> Result<String> {
    let envelope = validate_adaptive_envelope_value(value)?;
    canonical_digest_with_schema("forge.execution-baseline-adaptive-envelope/v1", &envelope)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

fn selected_outcome_is_inside_envelope(
    envelope: &AdaptiveEnvelope,
    selected_outcome: Option<&str>,
) -> bool {
    envelope.fixed_outcomes.is_empty()
        || selected_outcome.is_some_and(|outcome| {
            envelope
                .fixed_outcomes
                .iter()
                .any(|fixed| fixed.as_str() == outcome)
        })
}

fn integer(payload: &Value, field: &str) -> Result<i64> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn from_value<T: serde::de::DeserializeOwned>(payload: &Value, field: &str) -> Result<T> {
    serde_json::from_value(
        payload
            .get(field)
            .cloned()
            .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))?,
    )
    .map_err(|error| ServiceError::invalid_operation(format!("invalid {field}: {error}")))
}

fn required(field: &str, value: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} is required"
        )));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{
        AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
    };
    use db::{create_sqlite_pool, run_migrations, CreateProject, ProjectRepo};
    use std::sync::Arc;

    fn baseline_test_content(
        charter_id: &str,
        charter_revision_id: &str,
        charter_digest: &str,
        milestone_id: &str,
        milestone_definition_revision_id: &str,
    ) -> ExecutionBaselineContent {
        let release_policy = ExecutionBaselineReleasePolicy {
            schema_version: crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA.to_owned(),
            revision: "policy-r1".to_owned(),
            required_check_definition_revisions: vec!["check-r1".to_owned()],
            reviewer_independence_rules: vec!["independent-reviewer".to_owned()],
            manual_attestation_rules: vec!["manual-attestation".to_owned()],
            waiver_rules: vec!["user-waiver".to_owned()],
            evidence_kinds: vec!["test-report".to_owned()],
            evidence_contexts: vec!["repository".to_owned()],
            evidence_freshness_rules: vec!["current-commit".to_owned()],
            dependency_rules: vec!["dependencies-green".to_owned()],
            stale_input_rules: vec!["stale-baseline-blocks".to_owned()],
            forbidden_side_effects: vec!["publish".to_owned()],
            known_issue_rules: vec!["record-known-issue".to_owned()],
            correction_rules: vec!["correct-before-release".to_owned()],
            purge_rules: vec!["purge-invalid-evidence".to_owned()],
        };
        let release_policy_digest = canonical_digest_with_schema(
            crate::EXECUTION_BASELINE_RELEASE_POLICY_SCHEMA,
            &release_policy,
        )
        .expect("release policy digest");
        ExecutionBaselineContent {
            charter_revision: ArtifactRef {
                artifact_id: charter_id.to_owned(),
                revision_id: charter_revision_id.to_owned(),
                content_digest: charter_digest.to_owned(),
                render_version: Some("charter-render-v1".to_owned()),
                render_digest: Some("charter-render-digest".to_owned()),
            },
            document_revisions: Vec::new(),
            plan_item_ids: vec!["plan-1".to_owned()],
            milestone_ids: vec![milestone_id.to_owned()],
            milestone_definition_revision_ids: vec![milestone_definition_revision_id.to_owned()],
            primary_milestone_id: Some(milestone_id.to_owned()),
            release_policy_revision: release_policy.revision.clone(),
            release_policy_digest,
            release_policy,
            acceptance_evidence_matrix: Vec::new(),
            capability_classes: vec!["repository_write".to_owned()],
            risk_classes: vec!["low".to_owned()],
            reviewer_independence_rules: Vec::new(),
            elevated_operations: Vec::new(),
            adaptive_envelope: AdaptiveEnvelope {
                allowed_task_operations: vec!["split".to_owned()],
                fixed_outcomes: Vec::new(),
                fixed_acceptance: Vec::new(),
                fixed_risk_classes: vec!["low".to_owned()],
                forbidden_side_effects: Vec::new(),
                elevated_operations: Vec::new(),
            },
            rollback_and_recovery: Vec::new(),
            exclusions: Vec::new(),
        }
    }

    #[tokio::test]
    async fn agent_defined_milestone_uses_canonical_project_sequence_key() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("schema");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        let project_id = new_uuid_v4();
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "Milestone key project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        let payload = json!({
            "action": "define",
            "content": {
                "name": "Checkout flow",
                "outcome": "Customers can complete checkout"
            }
        });
        let action = AgentAction {
            id: new_uuid_v4(),
            actor_identity_id: new_uuid_v4(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            operation: PROJECT_MILESTONE_OPERATION.to_owned(),
            payload_json: payload.to_string(),
            payload_hash: "payload-hash".to_owned(),
            dedupe_key: "milestone-key-action".to_owned(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_project".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(project_id.clone()),
            outcome_json: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now,
        };
        ProjectOrchestrationActionService::new(Arc::new(db.clone()))
            .materialize_milestone(&action, &project_id, &payload)
            .await
            .expect("milestone definition materializes");
        let key: String =
            sqlx::query_scalar("SELECT milestone_key FROM project_milestone WHERE project_id = ?")
                .bind(&project_id)
                .fetch_one(db.pool())
                .await
                .expect("milestone key");
        assert_eq!(key, "M001");
    }

    #[tokio::test]
    async fn action_baseline_materializer_persists_ordered_milestone_definition_manifest_v076() {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("fresh V076 schema");
        let db = SqliteDb::new(pool);
        let now = now_rfc3339();
        let user_id = new_uuid_v4();
        let project_id = new_uuid_v4();
        let charter_id = new_uuid_v4();
        let charter_revision_id = new_uuid_v4();
        let milestone_id = new_uuid_v4();
        let milestone_definition_revision_id = new_uuid_v4();
        let baseline_id = new_uuid_v4();
        let action_id = new_uuid_v4();

        sqlx::query(
            "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
             VALUES (?, ?, 'test', 'Baseline Action User', ?, ?)",
        )
        .bind(&user_id)
        .bind(format!("{user_id}@example.test"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("user");
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.clone(),
                name: "Baseline Action Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: Some(user_id.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project");
        sqlx::query(
            "INSERT INTO project_charter (
                 id, account_id, project_id, project_mode, maturity, lifecycle,
                 version, created_at, updated_at
             ) VALUES (?, ?, ?, 'compact', 'prototype', 'attached', 1, ?, ?)",
        )
        .bind(&charter_id)
        .bind(&user_id)
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("charter");
        sqlx::query(
            "INSERT INTO project_charter_revision (
                 id, charter_id, revision, base_revision, lifecycle, schema_version,
                 render_version, content_json, rendered_view, change_summary,
                 author_type, author_id, source_refs_json, content_digest,
                 rendered_digest, created_at
             ) VALUES (?, ?, 1, 0, 'approved', 'charter-v1', 'charter-render-v1',
                       '{}', '{}', 'test', 'user', ?, '[]', ?, ?, ?)",
        )
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .bind(&user_id)
        .bind("charter-content-digest")
        .bind("charter-render-digest")
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("charter revision");
        sqlx::query(
            "UPDATE project_charter
             SET current_approved_revision_id = ?, current_draft_revision_id = ?, version = 2
             WHERE id = ?",
        )
        .bind(&charter_revision_id)
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .expect("approve charter fixture");
        sqlx::query(
            "UPDATE project
             SET current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = 1, charter_status = 'charter_backed',
                 charter_setup_required = 0
             WHERE id = ?",
        )
        .bind(&charter_id)
        .bind(&charter_revision_id)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("attach charter fixture");
        sqlx::query(
            "INSERT INTO project_milestone (
                 id, project_id, milestone_sequence, milestone_key, display_label,
                 lifecycle, blocker_reason_json, stale_reason_json,
                 reconciliation_reason_json, version, created_at, updated_at
             ) VALUES (?, ?, 1, 'M001', 'Deliver outcome', 'planned', '[]', '[]', '[]', 1, ?, ?)",
        )
        .bind(&milestone_id)
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("milestone");
        sqlx::query(
            "INSERT INTO project_milestone_revision (
                 id, milestone_id, revision, base_revision, lifecycle,
                 display_label, outcome, included_scope_json, excluded_scope_json,
                 charter_revision_id, document_revisions_json, task_selection_json,
                 dependencies_json, risks_json, acceptance_checks_json,
                 evidence_requirements_json, known_issues_json, change_summary,
                 schema_version, render_version, rendered_view, content_digest,
                 rendered_digest, author_type, author_id, source_refs_json, created_at
             ) VALUES (?, ?, 1, 0, 'proposed', 'Deliver outcome', 'Deliver outcome',
                       '[]', '[]', ?, '[]', '[]', '[]', '[]', '[]', '[]', '[]',
                       'test', ?, ?, '{}', ?, ?, 'agent', NULL, '[]', ?)",
        )
        .bind(&milestone_definition_revision_id)
        .bind(&milestone_id)
        .bind(&charter_revision_id)
        .bind(MILESTONE_DEFINITION_SCHEMA)
        .bind(MILESTONE_RENDER_SCHEMA)
        .bind("milestone-content-digest")
        .bind("milestone-render-digest")
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("milestone definition");
        sqlx::query(
            "UPDATE project_milestone
             SET current_definition_revision_id = ?
             WHERE id = ?",
        )
        .bind(&milestone_definition_revision_id)
        .bind(&milestone_id)
        .execute(db.pool())
        .await
        .expect("milestone definition pointer");

        let content = baseline_test_content(
            &charter_id,
            &charter_revision_id,
            "charter-content-digest",
            &milestone_id,
            &milestone_definition_revision_id,
        );
        let expected_release_policy_digest = content.release_policy_digest.clone();
        let expected_release_policy =
            serde_json::to_value(&content.release_policy).expect("release policy JSON");
        let expected_adaptive_envelope =
            serde_json::to_value(&content.adaptive_envelope).expect("adaptive envelope JSON");
        let payload = json!({
            "action": "draft_revision",
            "baseline_id": baseline_id,
            "content": content,
        });
        let action = AgentAction {
            id: action_id,
            actor_identity_id: new_uuid_v4(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            operation: PROJECT_EXECUTION_BASELINE_OPERATION.to_owned(),
            payload_json: payload.to_string(),
            payload_hash: "payload-hash".to_owned(),
            dedupe_key: "baseline-action-dedupe".to_owned(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            requested_permission: "propose_project".to_owned(),
            policy_result: AgentActionPolicyResult::Allowed,
            policy_reason: None,
            status: AgentActionStatus::Proposed,
            target_type: Some("project".to_owned()),
            target_id: Some(project_id.clone()),
            outcome_json: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let service = ProjectOrchestrationActionService::new(Arc::new(db.clone()));
        let result = service
            .materialize_execution_baseline(&action, &project_id, &payload)
            .await
            .expect("Project action baseline materializes on fresh V076 schema");
        let revision_id = result
            .get("revision_id")
            .and_then(Value::as_str)
            .expect("revision id")
            .to_owned();
        let row = sqlx::query(
            "SELECT milestone_ids_json, milestone_definition_revision_ids_json,
                    primary_milestone_id, release_policy_revision, release_policy_digest,
                    release_policy_json, adaptive_envelope_json
             FROM project_execution_baseline_revision WHERE id = ?",
        )
        .bind(revision_id)
        .fetch_one(db.pool())
        .await
        .expect("persisted baseline revision");
        assert_eq!(
            row.try_get::<String, _>("milestone_ids_json")
                .expect("milestone ids"),
            format!(r#"["{milestone_id}"]"#)
        );
        assert_eq!(
            row.try_get::<String, _>("milestone_definition_revision_ids_json")
                .expect("definition ids"),
            format!(r#"["{milestone_definition_revision_id}"]"#)
        );
        assert_eq!(
            row.try_get::<Option<String>, _>("primary_milestone_id")
                .expect("primary milestone"),
            Some(milestone_id)
        );
        assert_eq!(
            row.try_get::<String, _>("release_policy_revision")
                .expect("policy revision"),
            "policy-r1"
        );
        assert_eq!(
            row.try_get::<String, _>("release_policy_digest")
                .expect("policy digest"),
            expected_release_policy_digest
        );
        let release_policy_json: Value = serde_json::from_str(
            &row.try_get::<String, _>("release_policy_json")
                .expect("policy json"),
        )
        .expect("release policy projection");
        assert_eq!(
            release_policy_json.get("policy"),
            Some(&expected_release_policy)
        );
        assert_eq!(
            serde_json::from_str::<Value>(
                &row.try_get::<String, _>("adaptive_envelope_json")
                    .expect("adaptive envelope"),
            )
            .expect("adaptive envelope projection"),
            expected_adaptive_envelope
        );
    }

    #[test]
    fn baseline_identifier_matching_is_structural_and_bounded() {
        let baseline = r#"{"plan_items":[{"id":"plan-1"}],"milestones":["milestone-1"]}"#;
        assert!(json_contains_identifier(baseline, "plan-1"));
        assert!(json_contains_identifier(baseline, "milestone-1"));
        assert!(!json_contains_identifier(baseline, "plan-2"));
        assert!(!json_contains_identifier("not-json", "plan-1"));
    }

    #[test]
    fn adaptive_envelope_is_closed_and_rejects_out_of_envelope_outcomes() {
        let value = json!({
            "allowed_task_operations": ["task.read"],
            "fixed_outcomes": ["approved"],
            "fixed_acceptance": ["checks_pass"],
            "fixed_risk_classes": ["low"],
            "forbidden_side_effects": ["release"],
            "elevated_operations": []
        });
        let envelope = validate_adaptive_envelope_value(&value).expect("valid envelope");
        assert!(selected_outcome_is_inside_envelope(
            &envelope,
            Some("approved")
        ));
        assert!(!selected_outcome_is_inside_envelope(
            &envelope,
            Some("rejected")
        ));
        assert!(!selected_outcome_is_inside_envelope(&envelope, None));

        let mut with_unknown = value.clone();
        with_unknown["unexpected"] = json!([]);
        assert!(validate_adaptive_envelope_value(&with_unknown).is_err());
        let mut with_missing = value;
        with_missing
            .as_object_mut()
            .expect("object")
            .remove("fixed_acceptance");
        assert!(validate_adaptive_envelope_value(&with_missing).is_err());
    }

    #[test]
    fn adaptive_envelope_digest_binding_rejects_wrong_digest() {
        let value = json!({
            "allowed_task_operations": [],
            "fixed_outcomes": ["approved"],
            "fixed_acceptance": [],
            "fixed_risk_classes": [],
            "forbidden_side_effects": [],
            "elevated_operations": []
        });
        let digest = adaptive_envelope_digest(&value).expect("digest");
        assert_ne!(digest, "wrong-envelope-digest");
        let mut tampered = value;
        tampered["fixed_outcomes"] = json!(["rejected"]);
        assert_ne!(
            adaptive_envelope_digest(&tampered).expect("tampered digest"),
            digest
        );
    }
}
