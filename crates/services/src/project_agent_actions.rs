//! Typed materialization for Project Agent orchestration proposals.
//!
//! Project native tools persist an `AgentAction` first.  This module is the
//! only path which may turn the safe Project-local proposal operations into
//! Charter/Document/Decision/Milestone/media domain records.  The generic
//! action executor deliberately rejects these operations, so an arbitrary
//! result can never masquerade as a domain mutation.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{
    CurrentVersionOrRevision, PrincipalKind, ProjectCharterContent, ProjectDocumentContent,
    ProjectDocumentKind, RevisionProvenance,
};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecution, AgentActionExecutionStatus,
    AgentActionPolicyResult, AgentActionRepo, AgentActionStatus, CommandReceipt,
    CommandReceiptRepo, CreateAgentActionExecution, ProjectOrchestrationRepo, ProjectRepo,
    SqliteDb,
};
use forge_agent_host::{
    is_allowed_project_direct_payload, is_project_orchestration_operation,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION,
    PROJECT_RELEASE_OPERATION, PROJECT_VALIDATION_OPERATION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{
    parse_document_kind, AgentActionProvenance, AgentActionService,
    AuthorizationProvenance as CommandAuthorizationProvenance, CommandContext, CommandPrincipal,
    CommandScope, CommandScopeType, ExpectedCommandState, NewCommandContext,
    ProjectArtifactCommandService, ProjectCharterCommandService, ProjectCharterRevisionCommand,
    ProjectCommandAuthorization, ProjectDocumentApprovalCommand, ProjectDocumentRevisionCommand,
    ProjectEvidenceCommand, ProjectMilestoneCommandService, ProjectValidationCommand, Result,
    ServiceError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteProjectOrchestrationActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

/// Authenticated input for a directly admitted Project coordination command.
/// The native adapter supplies the canonical scope selected by the host and
/// the bound Project id; this service verifies both against the durable
/// Project-Agent binding before a receipt miss can mutate state.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteDirectProjectCommandInput {
    pub actor_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub project_id: String,
    pub operation: String,
    pub payload: Value,
    pub idempotency_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub requested_permission: String,
}

/// Frozen direct-command response.  `result` is copied from the durable
/// receipt, never rebuilt from current Project projections, so a response-loss
/// retry has exactly the same domain outcome and event identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectProjectCommandResult {
    pub receipt_id: String,
    pub event_id: String,
    pub operation: String,
    pub project_id: String,
    pub result: Value,
    pub agent_action_execution_id: Option<String>,
    /// True only when this response was reconstructed from an exact durable
    /// receipt match rather than produced by the fresh writer transaction.
    pub replayed: bool,
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

    /// Load the minimal current state needed to correct an authorized
    /// version/digest conflict.  The native adapter has already established
    /// the actor's active Project binding before calling this method; this
    /// service still verifies that the named domain record belongs to that
    /// Project before returning any state.  Repository getters are used here
    /// instead of allowing adapters to become alternate persistence paths.
    pub(crate) async fn authorized_current_version_or_revision(
        &self,
        project_id: &str,
        operation: &str,
        arguments: &Value,
    ) -> Result<Option<CurrentVersionOrRevision>> {
        let Some(project) = ProjectRepo::get_by_id(&*self.db, project_id).await? else {
            return Ok(None);
        };
        let payload = arguments.get("payload").unwrap_or(arguments);
        let Some(payload) = payload.as_object() else {
            return Ok(None);
        };

        match operation {
            PROJECT_DOCUMENT_OPERATION => {
                let Some(document_id) = payload.get("document_id").and_then(Value::as_str) else {
                    return Ok(None);
                };
                let Some(document) =
                    ProjectOrchestrationRepo::get_project_document(&*self.db, document_id).await?
                else {
                    return Ok(None);
                };
                if document.project_id != project.id {
                    return Ok(None);
                }
                let mut current =
                    CurrentVersionOrRevision::new("project_document", document.id.clone());
                current.version = Some(document.version);
                if let Some(revision_id) = document.current_draft_revision_id {
                    if let Some(revision) = ProjectOrchestrationRepo::get_project_document_revision(
                        &*self.db,
                        &revision_id,
                    )
                    .await?
                    {
                        if revision.document_id != document.id {
                            return Ok(None);
                        }
                        current.revision_id = Some(revision.id);
                        current.revision = Some(revision.revision);
                        current.content_digest = Some(revision.content_digest);
                        current.rendered_digest = Some(revision.rendered_digest);
                    }
                }
                Ok(Some(current))
            }
            PROJECT_MILESTONE_OPERATION
                if matches!(
                    payload.get("action").and_then(Value::as_str),
                    Some("define") | Some("set_primary")
                ) =>
            {
                let mut current = CurrentVersionOrRevision::new("project", project.id);
                current.version = Some(project.version);
                Ok(Some(current))
            }
            PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_VALIDATION_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION => {
                let milestone_id = payload
                    .get("milestone_id")
                    .or_else(|| payload.get("primary_milestone_id"))
                    .and_then(Value::as_str);
                let Some(milestone_id) = milestone_id else {
                    return Ok(None);
                };
                let Some(milestone) =
                    ProjectOrchestrationRepo::get_project_milestone(&*self.db, milestone_id)
                        .await?
                else {
                    return Ok(None);
                };
                if milestone.project_id != project.id {
                    return Ok(None);
                }

                let mut current =
                    CurrentVersionOrRevision::new("project_milestone", milestone.id.clone());
                current.version = Some(milestone.version);
                if let Some(revision_id) = milestone.current_definition_revision_id {
                    current.revision_id = Some(revision_id.clone());
                    if let Some(revision) =
                        ProjectOrchestrationRepo::get_project_milestone_revision(
                            &*self.db,
                            &revision_id,
                        )
                        .await?
                    {
                        if revision.milestone_id != milestone.id {
                            return Ok(None);
                        }
                        current.revision = Some(revision.revision);
                        current.content_digest = Some(revision.content_digest);
                        current.rendered_digest = Some(revision.rendered_digest);
                    }
                }
                Ok(Some(current))
            }
            _ => Ok(None),
        }
    }

    async fn replay_direct(&self, context: &CommandContext) -> Result<Option<CommandReceipt>> {
        direct_replay(&self.db, context).await
    }

    async fn authorize_direct_actor(&self, input: &ExecuteDirectProjectCommandInput) -> Result<()> {
        let actor =
            sqlx::query("SELECT paused, archived_at FROM agent_identity WHERE id = ? LIMIT 1")
                .bind(&input.actor_identity_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("agent_identity", input.actor_identity_id.clone())
                })?;
        if actor.try_get::<i64, _>("paused")? != 0
            || actor.try_get::<Option<String>, _>("archived_at")?.is_some()
        {
            return Err(ServiceError::AuthorizationDenied {
                message: "Project Agent is paused or archived".to_owned(),
            });
        }

        let project_exists: Option<String> =
            sqlx::query_scalar("SELECT id FROM project WHERE id = ? LIMIT 1")
                .bind(&input.project_id)
                .fetch_optional(self.db.pool())
                .await?;
        if project_exists.is_none() {
            return Err(ServiceError::not_found("project", input.project_id.clone()));
        }

        match input.scope_type.as_str() {
            "project" if input.scope_id == input.project_id => {}
            "project" => {
                return Err(ServiceError::AuthorizationDenied {
                    message: "direct Project command scope does not match its canonical Project"
                        .to_owned(),
                });
            }
            "agent_chat" => {
                let chat =
                    sqlx::query("SELECT kind, project_id FROM agent_chat WHERE id = ? LIMIT 1")
                        .bind(&input.scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                let Some(chat) = chat else {
                    return Err(ServiceError::not_found(
                        "agent_chat",
                        input.scope_id.clone(),
                    ));
                };
                if chat.try_get::<String, _>("kind")? != "project"
                    || chat.try_get::<Option<String>, _>("project_id")?.as_deref()
                        != Some(input.project_id.as_str())
                {
                    return Err(ServiceError::AuthorizationDenied {
                        message:
                            "direct command requires the Project Agent Chat bound to this Project"
                                .to_owned(),
                    });
                }
            }
            _ => unreachable!("direct input scope was validated before authorization"),
        }

        let bound: Option<String> = sqlx::query_scalar(
            "SELECT identity_id FROM project_agent_binding
             WHERE project_id = ? AND identity_id = ? AND state = 'active' LIMIT 1",
        )
        .bind(&input.project_id)
        .bind(&input.actor_identity_id)
        .fetch_optional(self.db.pool())
        .await?;
        if bound.is_none() {
            return Err(ServiceError::AuthorizationDenied {
                message: "Project Agent is not actively bound to this Project".to_owned(),
            });
        }
        Ok(())
    }

    /// Execute one closed, automatically-allowed Project coordination
    /// subaction directly.  The receipt lookup intentionally precedes the
    /// current binding/policy check: a response-loss retry returns its frozen
    /// result even if the Project Agent was subsequently paused or rebound.
    /// Fresh commands are re-authorized before entering the shared typed
    /// materializers.  `CommandContext.action_provenance` is always `None` in
    /// this path, so no AgentAction or AgentActionExecution row can be made.
    pub async fn execute_direct(
        &self,
        input: ExecuteDirectProjectCommandInput,
    ) -> Result<DirectProjectCommandResult> {
        validate_direct_input(&input)?;
        let payload = input.payload.clone();
        let receipt_operation = direct_receipt_operation(&input.operation, &payload)?;
        let context = direct_command_context(&input, &receipt_operation, &payload, "allowed")?;

        if let Some(receipt) = self.replay_direct(&context).await? {
            return direct_result_from_receipt(receipt, &input.project_id, true);
        }

        self.authorize_direct_actor(&input).await?;
        let (policy_result, reason) = self
            .actions
            .evaluate_direct_command_policy(
                &input.actor_identity_id,
                &input.scope_type,
                &input.scope_id,
                &input.requested_permission,
                &input.operation,
                Some(&serde_json::to_string(&payload).map_err(|error| {
                    ServiceError::invalid_operation(format!(
                        "serialize direct Project command payload: {error}"
                    ))
                })?),
            )
            .await?;
        if policy_result != AgentActionPolicyResult::Allowed {
            return Err(ServiceError::AuthorizationDenied {
                message: reason.unwrap_or_else(|| {
                    "direct Project command policy did not admit this operation".to_owned()
                }),
            });
        }

        let action = direct_materialization_action(&input, &payload);
        let result = match input.operation.as_str() {
            PROJECT_CHARTER_ADOPTION_OPERATION => {
                self.materialize_charter_adoption(&action, &input.project_id, &payload, &context)
                    .await?
            }
            PROJECT_DOCUMENT_OPERATION => {
                self.materialize_document_with_command(
                    &action,
                    &input.project_id,
                    &payload,
                    Some(&context),
                )
                .await?
            }
            PROJECT_DECISION_OPERATION => {
                self.materialize_decision_checked_with_command(
                    &action,
                    &input.project_id,
                    &payload,
                    Some(&context),
                )
                .await?
            }
            PROJECT_MILESTONE_OPERATION => {
                self.materialize_milestone(&action, &input.project_id, &payload, Some(&context))
                    .await?
            }
            PROJECT_EVIDENCE_OPERATION => {
                self.materialize_evidence_with_command(
                    &action,
                    &input.project_id,
                    &payload,
                    Some(&context),
                )
                .await?
            }
            PROJECT_VALIDATION_OPERATION => {
                self.materialize_validation(&action, &input.project_id, &payload, Some(&context))
                    .await?
            }
            PROJECT_READINESS_OPERATION => {
                self.materialize_readiness_request(
                    &action,
                    &input.project_id,
                    &payload,
                    Some(&context),
                )
                .await?
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Project release candidates remain approval-backed actions",
                ));
            }
        };

        // The typed command service owns the durable receipt.  Read that
        // frozen row only to assemble the transport-neutral direct result;
        // never rebuild the result from mutable domain projections.
        let receipt = self.replay_direct(&context).await?.ok_or_else(|| {
            ServiceError::Conflict(format!(
                "direct Project command {} committed without a command receipt",
                input.operation
            ))
        })?;
        if receipt.agent_action_execution_id.is_some() {
            return Err(ServiceError::Conflict(
                "direct Project command unexpectedly created an AgentAction execution".to_owned(),
            ));
        }
        // Keep the materializer result evaluated so malformed command-service
        // outcomes fail before the receipt is exposed, while the receipt
        // remains the authoritative replay payload.
        if !result.is_object() {
            return Err(ServiceError::Conflict(
                "direct Project command returned a non-object domain result".to_owned(),
            ));
        }
        direct_result_from_receipt(receipt, &input.project_id, false)
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
        let payload: Value = serde_json::from_str(&action.payload_json)
            .map_err(|_| ServiceError::invalid_operation("Project action payload is invalid"))?;

        // A receipt is the replay boundary for the migrated command families.
        // It is resolved before admission/lifecycle checks so a response lost
        // after commit remains replayable even after the Project has advanced.
        let command_context = if is_project_orchestration_operation(&action.operation) {
            Some(
                self.project_command_context(&action, &input, &project_id, &payload)
                    .await?,
            )
        } else {
            None
        };
        if let Some(context) = command_context.as_ref() {
            if let Some(existing) = self
                .resolve_project_command_replay(&action, context)
                .await?
            {
                return Ok(existing);
            }
        } else if let Some(existing) =
            AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id).await?
        {
            if existing.idempotency_key != input.idempotency_key {
                return Err(ServiceError::conflict(
                    "Project orchestration action already has a successful execution with a different idempotency key",
                ));
            }
            return Ok(existing);
        }

        // Resolve an exact command receipt before re-admitting the current
        // actor binding. A response-loss retry must replay after a binding is
        // paused or replaced; fresh commands are re-authorized below.
        self.authorize_actor(&action, &project_id, &input).await?;

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

        let result = match action.operation.as_str() {
            PROJECT_CHARTER_ADOPTION_OPERATION => {
                let context = command_context.as_ref().ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Charter execution requires a canonical command context",
                    )
                })?;
                self.materialize_charter_adoption(&action, &project_id, &payload, context)
                    .await?
            }
            PROJECT_DOCUMENT_OPERATION => {
                self.materialize_document_with_command(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            PROJECT_DECISION_OPERATION => {
                self.materialize_decision_checked_with_command(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            PROJECT_MILESTONE_OPERATION => {
                self.materialize_milestone(&action, &project_id, &payload, command_context.as_ref())
                    .await?
            }
            PROJECT_EVIDENCE_OPERATION => {
                self.materialize_evidence_with_command(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            PROJECT_VALIDATION_OPERATION => {
                self.materialize_validation(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            PROJECT_READINESS_OPERATION => {
                self.materialize_readiness_request(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            PROJECT_RELEASE_OPERATION => {
                self.materialize_release_request(
                    &action,
                    &project_id,
                    &payload,
                    command_context.as_ref(),
                )
                .await?
            }
            _ => unreachable!("operation was validated above"),
        };

        let result_json = serde_json::to_string(&result).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Project orchestration execution result: {error}"
            ))
        })?;
        if is_project_orchestration_operation(&action.operation) {
            #[cfg(test)]
            if crate::test_support::take_after_domain_commit(&action.id) {
                return Err(ServiceError::conflict(
                    "characterization failpoint: stopped after Project domain commit before AgentAction receipt",
                ));
            }
            return AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict(
                        "Project command committed without a successful AgentAction execution"
                            .to_owned(),
                    )
                });
        }
        #[cfg(test)]
        if crate::test_support::take_after_domain_commit(&action.id) {
            return Err(ServiceError::conflict(
                "characterization failpoint: stopped after Project domain commit before AgentAction receipt",
            ));
        }
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

    async fn project_command_context(
        &self,
        action: &AgentAction,
        input: &ExecuteProjectOrchestrationActionInput,
        project_id: &str,
        payload: &Value,
    ) -> Result<CommandContext> {
        if !matches!(
            action.scope_type.as_str(),
            "account" | "project" | "agent_chat" | "task"
        ) {
            return Err(ServiceError::invalid_operation(
                "Project orchestration action has an invalid canonical scope",
            ));
        }
        let principal_type = required("typed Project executor type", &input.executed_by_type)?;
        let principal_id = required("typed Project executor id", &input.executed_by_id)?;
        let idempotency_key = required("execution idempotency key", &input.idempotency_key)?;

        let mut versions = BTreeMap::from([(action.id.clone(), input.expected_version)]);
        for key in [
            "expected_project_version",
            "expected_charter_version",
            "expected_document_version",
            "expected_candidate_version",
        ] {
            if let Some(value) = payload.get(key).and_then(Value::as_i64) {
                versions.insert(key.to_owned(), value);
            }
        }
        if matches!(
            action.operation.as_str(),
            PROJECT_EVIDENCE_OPERATION | PROJECT_VALIDATION_OPERATION
        ) {
            let expected_milestone_version = payload
                .get("expected_milestone_version")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 1)
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "expected_milestone_version must be a positive integer",
                    )
                })?;
            versions.insert(
                "expected_milestone_version".to_owned(),
                expected_milestone_version,
            );
        } else if let Some(value) = payload
            .get("expected_milestone_version")
            .and_then(Value::as_i64)
        {
            versions.insert("expected_milestone_version".to_owned(), value);
        }
        if action.operation == PROJECT_DOCUMENT_OPERATION
            && payload.get("action").and_then(Value::as_str) == Some("draft_revision")
            && payload.get("base_revision_id").is_none_or(Value::is_null)
        {
            // The first Document shell has a fixed server-side CAS version;
            // native payloads intentionally omit it because the shell does
            // not exist yet.
            versions.insert("expected_document_version".to_owned(), 1);
        }
        let digests = BTreeMap::from([
            ("action_scope_type".to_owned(), action.scope_type.clone()),
            ("action_scope_id".to_owned(), action.scope_id.clone()),
            ("project_id".to_owned(), project_id.to_owned()),
        ]);

        CommandContext::from_authorized_input(
            NewCommandContext {
                principal: CommandPrincipal {
                    principal_type,
                    principal_id,
                },
                canonical_scope: CommandScope {
                    // Project domain command repositories require a canonical
                    // Project scope even when the proposal arrived through a
                    // Project Agent Chat.  The original action scope remains
                    // bound above in the digest.
                    scope_type: CommandScopeType::Project,
                    scope_id: project_id.to_owned(),
                },
                operation: action.operation.clone(),
                idempotency_key,
                expected_state: ExpectedCommandState { versions, digests },
                authorization_provenance: Some(CommandAuthorizationProvenance {
                    policy_result: action.policy_result.to_string(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(action.requested_permission.clone()),
                }),
                action_provenance: Some(AgentActionProvenance {
                    action_id: action.id.clone(),
                    expected_action_version: input.expected_version,
                    attempt: 1,
                    execution_idempotency_key: input.idempotency_key.clone(),
                    executed_by_type: input.executed_by_type.clone(),
                    executed_by_id: input.executed_by_id.clone(),
                }),
                correlation_id: action.correlation_id.clone(),
                causation_id: action.causation_id.clone(),
                causation_depth: action.causation_depth,
            },
            payload,
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize Project command input digest: {error}"
            ))
        })
    }

    async fn resolve_project_command_replay(
        &self,
        action: &AgentAction,
        context: &CommandContext,
    ) -> Result<Option<AgentActionExecution>> {
        let receipt = CommandReceiptRepo::get_command_receipt(
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
        if let Some(receipt) = receipt {
            let execution = AgentActionRepo::get_successful_action_execution(&*self.db, &action.id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict(
                        "Project command receipt has no successful AgentAction execution"
                            .to_owned(),
                    )
                })?;
            if receipt.agent_action_execution_id.as_deref() != Some(execution.id.as_str())
                || receipt.outcome_json != execution.result_json.clone().unwrap_or_default()
                || execution.idempotency_key != context.idempotency_key()
                || execution.executed_by_type != context.principal().principal_type()
                || execution.executed_by_id != context.principal().principal_id()
            {
                return Err(ServiceError::Conflict(
                    "Project command receipt provenance does not match its AgentAction execution"
                        .to_owned(),
                ));
            }
            return Ok(Some(execution));
        }

        // Do not fall back to the old action-execution-only replay path here.
        // If a receipt exists under this scope/key with a different digest,
        // the composite DB command will classify it as an idempotency
        // conflict.  If a pre-migration execution has no receipt, attempting
        // the new command is allowed to fail atomically rather than silently
        // treating an unbound legacy row as a frozen command result.
        Ok(None)
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
        command_context: &CommandContext,
    ) -> Result<Value> {
        let content = payload
            .get("content")
            .cloned()
            .ok_or_else(|| ServiceError::invalid_operation("Charter content is required"))?;
        let content: ProjectCharterContent = serde_json::from_value(content).map_err(|error| {
            ServiceError::invalid_operation(format!("Charter content is invalid: {error}"))
        })?;
        let provenance: RevisionProvenance = from_value(payload, "provenance")?;
        let authorization_event_id = command_context
            .action_provenance
            .as_ref()
            .map(|value| value.action_id.clone())
            .unwrap_or_else(|| command_context.correlation_id().to_owned());
        let authorization_basis = if command_context.action_provenance.is_some() {
            "agent_action"
        } else {
            "project_agent_binding_policy"
        };
        let authorization = ProjectCommandAuthorization {
            principal_type: command_context.principal().principal_type().to_owned(),
            principal_id: command_context.principal().principal_id().to_owned(),
            policy_result: command_context
                .authorization_provenance
                .as_ref()
                .map_or_else(
                    || action.policy_result.to_string(),
                    |value| value.policy_result.clone(),
                ),
            policy_revision: None,
            policy_digest: command_context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.policy_digest.clone()),
            requested_permission: command_context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.requested_permission.clone()),
            correlation_id: command_context.correlation_id().to_owned(),
            causation_id: command_context.causation_id.clone(),
            causation_depth: command_context.causation_depth,
            authorization_event_id: authorization_event_id.clone(),
            authorization_basis: authorization_basis.to_owned(),
            authorization_action: "project_charter.revision.save".to_owned(),
            authorization_occurred_at: action.created_at.clone(),
            authorization_json: json!({
                "principal": {
                    "kind": command_context.principal().principal_type(),
                    "id": command_context.principal().principal_id(),
                },
                "authorization_basis": authorization_basis,
                "action": "project_charter.revision.save",
                "event_id": authorization_event_id,
                "correlation_id": command_context.correlation_id(),
                "causation_id": command_context.causation_id,
            })
            .to_string(),
        };
        let command = ProjectCharterRevisionCommand {
            project_id: project_id.to_owned(),
            charter_id: optional_string(payload, "charter_id").unwrap_or_default(),
            base_revision_id: payload
                .get("base_revision_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            expected_digest: optional_string(payload, "expected_digest"),
            project_mode: string(payload, "project_mode")?,
            maturity: string(payload, "maturity")?,
            content,
            rendered_view: optional_string(payload, "rendered_view"),
            render_version: optional_string(payload, "render_version"),
            provenance,
            expected_charter_version: integer(payload, "expected_charter_version")?,
            idempotency_key: command_context.idempotency_key().to_owned(),
            authorization,
        };
        let outcome = ProjectCharterCommandService::new(Arc::clone(&self.db))
            .save_revision_with_context(command, command_context.clone())
            .await?;
        Ok(json!({
            "operation": PROJECT_CHARTER_ADOPTION_OPERATION,
            "project_id": project_id,
            "charter_id": outcome.revision.charter_id,
            "charter_version": outcome.charter_version,
            "revision_id": outcome.revision.id,
            "revision": outcome.revision.revision,
            "content_digest": outcome.revision.content_digest,
            "render_digest": outcome.revision.rendered_digest,
            "lifecycle": outcome.revision.lifecycle,
            "readiness": outcome.readiness,
            "domain_committed": true,
            "requires_user_authorization": true,
        }))
    }

    async fn materialize_document_with_command(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Document execution requires a canonical command context",
            )
        })?;
        let document_action = string(payload, "action")?;
        let requested_document_id = string(payload, "document_id")?;
        let service = ProjectArtifactCommandService::new(Arc::clone(&self.db));
        if matches!(
            document_action.as_str(),
            "draft_revision" | "propose_approval"
        ) {
            let kind_text = string(payload, "kind")?;
            let kind = parse_document_kind(&kind_text).ok_or_else(|| {
                ServiceError::invalid_operation("Project Document kind is invalid")
            })?;
            let title = string(payload, "title")?;
            let content = parse_document_content(
                kind,
                payload.get("content").ok_or_else(|| {
                    ServiceError::invalid_operation("Project Document content is required")
                })?,
            )?;
            let lifecycle = if document_action == "propose_approval" {
                api_types::DocumentRevisionLifecycle::Proposed
            } else {
                api_types::DocumentRevisionLifecycle::Draft
            };
            let base_revision_id = payload
                .get("base_revision_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let expected_document_version = payload
                .get("expected_document_version")
                .and_then(Value::as_i64)
                .unwrap_or_else(|| if base_revision_id.is_none() { 1 } else { 0 });
            let authorization =
                document_command_authorization(action, context, "project.document.revision.save");
            let provenance = api_types::RevisionProvenance {
                author: api_types::PrincipalRef {
                    kind: PrincipalKind::Agent,
                    id: action.actor_identity_id.clone(),
                    display_name: None,
                },
                profile_revision: None,
                operating_skill_revision: None,
                source_refs: Vec::new(),
                change_summary: "Project Agent authored a typed document revision".to_owned(),
                material_diff: None,
            };
            let revision = service
                .save_document_revision_with_context(
                    ProjectDocumentRevisionCommand {
                        project_id: project_id.to_owned(),
                        document_id: requested_document_id,
                        kind: Some(kind_text),
                        title: Some(title),
                        approval_policy: Some("user_or_project_agent".to_owned()),
                        base_revision_id,
                        lifecycle,
                        content,
                        change_summary: provenance.change_summary.clone(),
                        provenance,
                        expected_document_version,
                        expected_digest: optional_string(payload, "expected_digest"),
                        idempotency_key: context.idempotency_key().to_owned(),
                        authorization,
                    },
                    context.clone(),
                )
                .await?;
            return Ok(json!({
                "operation": PROJECT_DOCUMENT_OPERATION,
                "project_id": project_id,
                "document_id": revision.document_id,
                "revision_id": revision.id,
                "revision": revision.revision,
                "lifecycle": revision.lifecycle,
                "domain_committed": true,
                "requires_user_authorization": revision.lifecycle == "proposed",
            }));
        }
        if document_action != "approve" {
            return Err(ServiceError::invalid_operation(
                "Project Agent may draft, propose, or approve a Document revision only",
            ));
        }
        let authorization =
            document_command_authorization(action, context, "project.document.approve");
        let approval = service
            .approve_document_with_context(
                ProjectDocumentApprovalCommand {
                    project_id: project_id.to_owned(),
                    document_id: requested_document_id,
                    revision_id: string(payload, "revision_id")?,
                    content_digest: string(payload, "content_digest")?,
                    rendered_digest: string(payload, "render_digest")?,
                    expected_document_version: integer(payload, "expected_document_version")?,
                    idempotency_key: context.idempotency_key().to_owned(),
                    authorization,
                },
                context.clone(),
            )
            .await?;
        Ok(json!({
            "operation": PROJECT_DOCUMENT_OPERATION,
            "project_id": project_id,
            "document_id": approval.document_id,
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

    /// Translate an admitted native Decision action into the shared service.
    async fn materialize_decision_checked_with_command(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Decision execution requires a canonical command context",
            )
        })?;
        crate::ProjectDecisionCommandService::new(Arc::clone(&self.db))
            .execute_project_agent_command(action, project_id, payload, context)
            .await
    }

    async fn materialize_milestone(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project milestone execution requires a canonical command context",
            )
        })?;
        ProjectMilestoneCommandService::new(Arc::clone(&self.db))
            .execute_project_agent_command(action, project_id, payload, context)
            .await
    }

    async fn materialize_evidence_with_command(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project evidence execution requires a canonical command context",
            )
        })?;
        let milestone_id = string(payload, "milestone_id")?;
        let asset_id = string(payload, "asset_id")?;
        let requested_checksum = string(payload, "checksum")?;
        let acceptance_check_ids: Vec<String> = from_value(payload, "acceptance_check_ids")?;
        let expected_milestone_version = positive_integer(payload, "expected_milestone_version")?;
        let authorization_event_id = context
            .action_provenance
            .as_ref()
            .map(|provenance| provenance.action_id.clone())
            .unwrap_or_else(|| context.correlation_id().to_owned());
        let authorization = ProjectCommandAuthorization {
            principal_type: context.principal().principal_type().to_owned(),
            principal_id: context.principal().principal_id().to_owned(),
            policy_result: context.authorization_provenance.as_ref().map_or_else(
                || action.policy_result.to_string(),
                |value| value.policy_result.clone(),
            ),
            policy_revision: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.policy_revision.clone()),
            policy_digest: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.policy_digest.clone()),
            requested_permission: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.requested_permission.clone()),
            correlation_id: context.correlation_id().to_owned(),
            causation_id: context.causation_id.clone(),
            causation_depth: context.causation_depth,
            authorization_event_id: authorization_event_id.clone(),
            authorization_basis: "project_agent_binding_policy".to_owned(),
            authorization_action: "project.evidence.attach".to_owned(),
            authorization_occurred_at: action.created_at.clone(),
            authorization_json: json!({
                "principal": {
                    "kind": context.principal().principal_type(),
                    "id": context.principal().principal_id(),
                },
                "authorization_basis": "project_agent_binding_policy",
                "action": "project.evidence.attach",
                "event_id": authorization_event_id,
                "occurred_at": action.created_at,
            })
            .to_string(),
        };
        let attachment = ProjectArtifactCommandService::new(Arc::clone(&self.db))
            .attach_evidence_with_context(
                ProjectEvidenceCommand {
                    project_id: project_id.to_owned(),
                    milestone_id: milestone_id.clone(),
                    asset_id: asset_id.clone(),
                    task_id: optional_string(payload, "task_id"),
                    source_run_id: optional_string(payload, "source_run_id"),
                    source_validation_id: optional_string(payload, "source_validation_id"),
                    acceptance_check_ids,
                    caption: string(payload, "caption")?,
                    evidence_kind: string(payload, "kind")?,
                    checksum: requested_checksum,
                    expected_milestone_version,
                    idempotency_key: context.idempotency_key().to_owned(),
                    authorization,
                },
                context.clone(),
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

    /// Record one agent-observed acceptance-check result. The check's own
    /// source kind decides whether this path is legal at all, so the command
    /// service -- not this materializer -- owns that refusal.
    async fn materialize_validation(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project validation execution requires a canonical command context",
            )
        })?;
        let milestone_id = string(payload, "milestone_id")?;
        let check_id = string(payload, "check_id")?;
        let expected_milestone_version = payload
            .get("expected_milestone_version")
            .or_else(|| payload.get("milestone_version"))
            .and_then(Value::as_i64)
            .filter(|value| *value >= 1)
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "expected_milestone_version must be a positive integer",
                )
            })?;
        let authorization_event_id = format!("agent-action:{}", action.id);
        let authorization = ProjectCommandAuthorization {
            principal_type: context.principal().principal_type().to_owned(),
            principal_id: context.principal().principal_id().to_owned(),
            policy_result: context
                .authorization_provenance
                .as_ref()
                .map_or_else(|| "allowed".to_owned(), |value| value.policy_result.clone()),
            policy_revision: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.policy_revision.clone()),
            policy_digest: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.policy_digest.clone()),
            requested_permission: context
                .authorization_provenance
                .as_ref()
                .and_then(|value| value.requested_permission.clone()),
            correlation_id: context.correlation_id().to_owned(),
            causation_id: context.causation_id.clone(),
            causation_depth: context.causation_depth,
            authorization_event_id: authorization_event_id.clone(),
            authorization_basis: "project_agent_binding_policy".to_owned(),
            authorization_action: "project.validation.record".to_owned(),
            authorization_occurred_at: action.created_at.clone(),
            authorization_json: json!({
                "principal": {
                    "kind": context.principal().principal_type(),
                    "id": context.principal().principal_id(),
                },
                "authorization_basis": "project_agent_binding_policy",
                "action": "project.validation.record",
                "event_id": authorization_event_id,
                "occurred_at": action.created_at,
            })
            .to_string(),
        };
        let recorded = ProjectArtifactCommandService::new(Arc::clone(&self.db))
            .record_validation_with_context(
                ProjectValidationCommand {
                    project_id: project_id.to_owned(),
                    milestone_id: milestone_id.clone(),
                    check_id: check_id.clone(),
                    definition_revision_id: string(payload, "definition_revision_id")?,
                    status: string(payload, "status")?,
                    result: string(payload, "result")?,
                    input_digest: string(payload, "input_digest")?,
                    observed_task_id: optional_string(payload, "observed_task_id"),
                    evidence_asset_id: optional_string(payload, "evidence_asset_id"),
                    expected_milestone_version,
                    idempotency_key: context.idempotency_key().to_owned(),
                    authorization,
                },
                context.clone(),
            )
            .await?;
        Ok(json!({
            "operation": PROJECT_VALIDATION_OPERATION,
            "project_id": project_id,
            "milestone_id": milestone_id,
            "check_id": check_id,
            "result_id": recorded.id,
            "outcome": recorded.outcome,
            "domain_committed": true,
        }))
    }

    async fn materialize_readiness_request(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project readiness execution requires a canonical command context",
            )
        })?;
        ProjectMilestoneCommandService::new(Arc::clone(&self.db))
            .execute_project_agent_command(action, project_id, payload, context)
            .await
    }

    async fn materialize_release_request(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project release-request execution requires a canonical command context",
            )
        })?;
        ProjectMilestoneCommandService::new(Arc::clone(&self.db))
            .execute_project_agent_command(action, project_id, payload, context)
            .await
    }
}

fn validate_direct_input(input: &ExecuteDirectProjectCommandInput) -> Result<()> {
    for (field, value) in [
        ("actor_identity_id", input.actor_identity_id.as_str()),
        ("scope_type", input.scope_type.as_str()),
        ("scope_id", input.scope_id.as_str()),
        ("project_id", input.project_id.as_str()),
        ("operation", input.operation.as_str()),
        ("idempotency_key", input.idempotency_key.as_str()),
        ("correlation_id", input.correlation_id.as_str()),
        ("requested_permission", input.requested_permission.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "direct Project command {field} is required"
            )));
        }
    }
    if !matches!(input.scope_type.as_str(), "project" | "agent_chat") {
        return Err(ServiceError::invalid_operation(
            "direct Project commands require a Project or Project Agent Chat scope",
        ));
    }
    if input.requested_permission != "propose_project" {
        return Err(ServiceError::AuthorizationDenied {
            message: "direct Project commands require the propose_project permission".to_owned(),
        });
    }
    if !(0..=8).contains(&input.causation_depth) {
        return Err(ServiceError::invalid_operation(
            "direct Project command causation depth exceeds the reaction bound",
        ));
    }
    if input.payload.get("project_id").is_some() {
        return Err(ServiceError::invalid_operation(
            "Project command scope is server-derived; project_id must not be supplied in payload",
        ));
    }
    if !is_allowed_project_direct_payload(&input.operation, &input.payload) {
        return Err(ServiceError::invalid_operation(
            "Project operation is not an automatically allowed coordination subaction",
        ));
    }
    Ok(())
}

fn direct_receipt_operation(operation: &str, payload: &Value) -> Result<String> {
    if !is_allowed_project_direct_payload(operation, payload) {
        return Err(ServiceError::invalid_operation(
            "Project operation is not an automatically allowed coordination subaction",
        ));
    }
    Ok(operation.to_owned())
}

fn direct_command_context(
    input: &ExecuteDirectProjectCommandInput,
    operation: &str,
    payload: &Value,
    policy_result: &str,
) -> Result<CommandContext> {
    let mut versions = BTreeMap::new();
    for key in [
        "expected_project_version",
        "expected_charter_version",
        "expected_document_version",
        "expected_candidate_version",
        "expected_milestone_version",
        "milestone_version",
    ] {
        if let Some(value) = payload.get(key).and_then(Value::as_i64) {
            versions.insert(key.to_owned(), value);
        }
    }
    let digests = BTreeMap::from([
        ("action_scope_type".to_owned(), input.scope_type.clone()),
        ("action_scope_id".to_owned(), input.scope_id.clone()),
        ("project_id".to_owned(), input.project_id.clone()),
    ]);
    CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type: "agent".to_owned(),
                principal_id: input.actor_identity_id.clone(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: input.project_id.clone(),
            },
            operation: operation.to_owned(),
            idempotency_key: input.idempotency_key.clone(),
            expected_state: ExpectedCommandState { versions, digests },
            authorization_provenance: Some(CommandAuthorizationProvenance {
                policy_result: policy_result.to_owned(),
                policy_revision: None,
                policy_digest: None,
                requested_permission: Some(input.requested_permission.clone()),
            }),
            action_provenance: None,
            correlation_id: input.correlation_id.clone(),
            causation_id: input.causation_id.clone(),
            causation_depth: input.causation_depth,
        },
        payload,
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!("serialize direct Project command: {error}"))
    })
}

async fn direct_replay(db: &SqliteDb, context: &CommandContext) -> Result<Option<CommandReceipt>> {
    CommandReceiptRepo::get_command_receipt(
        db,
        context.principal().principal_type(),
        context.principal().principal_id(),
        context.canonical_scope().scope_type().as_str(),
        context.canonical_scope().scope_id(),
        context.operation(),
        context.idempotency_key(),
        context.input_digest(),
    )
    .await
    .map_err(Into::into)
}

fn direct_result_from_receipt(
    receipt: CommandReceipt,
    project_id: &str,
    replayed: bool,
) -> Result<DirectProjectCommandResult> {
    if receipt.agent_action_execution_id.is_some() {
        return Err(ServiceError::Conflict(
            "direct Project command receipt is unexpectedly linked to an AgentAction execution"
                .to_owned(),
        ));
    }
    let result: Value = serde_json::from_str(&receipt.outcome_json).map_err(|error| {
        ServiceError::Conflict(format!(
            "direct Project command receipt outcome is invalid: {error}"
        ))
    })?;
    Ok(DirectProjectCommandResult {
        receipt_id: receipt.id,
        event_id: receipt.event_id,
        operation: receipt.operation,
        project_id: project_id.to_owned(),
        result,
        agent_action_execution_id: None,
        replayed,
    })
}

fn direct_materialization_action(
    input: &ExecuteDirectProjectCommandInput,
    payload: &Value,
) -> AgentAction {
    let now = now_rfc3339();
    let payload_json = payload.to_string();
    let mut hasher = Sha256::new();
    hasher.update(payload_json.as_bytes());
    AgentAction {
        id: format!("direct-command:{}", input.correlation_id),
        actor_identity_id: input.actor_identity_id.clone(),
        scope_type: input.scope_type.clone(),
        scope_id: input.scope_id.clone(),
        operation: input.operation.clone(),
        payload_hash: hex::encode(hasher.finalize()),
        payload_json,
        dedupe_key: input.idempotency_key.clone(),
        correlation_id: input.correlation_id.clone(),
        causation_id: input.causation_id.clone(),
        causation_depth: input.causation_depth,
        requested_permission: input.requested_permission.clone(),
        policy_result: AgentActionPolicyResult::Allowed,
        policy_reason: None,
        status: AgentActionStatus::Proposed,
        target_type: None,
        target_id: None,
        outcome_json: None,
        version: 1,
        created_at: now.clone(),
        updated_at: now,
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

fn integer(payload: &Value, field: &str) -> Result<i64> {
    payload
        .get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| ServiceError::invalid_operation(format!("{field} is required")))
}

fn positive_integer(payload: &Value, field: &str) -> Result<i64> {
    let value = integer(payload, field)?;
    if value < 1 {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must be a positive integer"
        )));
    }
    Ok(value)
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

fn document_command_authorization(
    action: &AgentAction,
    context: &CommandContext,
    authorization_action: &str,
) -> ProjectCommandAuthorization {
    let provenance = context.authorization_provenance.as_ref();
    let authorization_event_id = context
        .action_provenance
        .as_ref()
        .map(|value| value.action_id.clone())
        .unwrap_or_else(|| context.correlation_id().to_owned());
    // Document commands historically record the binding-policy basis even
    // when an approval-backed AgentAction supplied the audit event.  Keep
    // that basis stable for action-backed replay; direct commands use the
    // same policy basis with a correlation-backed authorization event.
    let authorization_basis = "project_agent_binding_policy";
    ProjectCommandAuthorization {
        principal_type: context.principal().principal_type().to_owned(),
        principal_id: context.principal().principal_id().to_owned(),
        policy_result: provenance.map_or_else(
            || action.policy_result.to_string(),
            |value| value.policy_result.clone(),
        ),
        policy_revision: provenance.and_then(|value| value.policy_revision.clone()),
        policy_digest: provenance.and_then(|value| value.policy_digest.clone()),
        requested_permission: provenance.and_then(|value| value.requested_permission.clone()),
        correlation_id: context.correlation_id().to_owned(),
        causation_id: context.causation_id.clone(),
        causation_depth: context.causation_depth,
        authorization_event_id,
        authorization_basis: authorization_basis.to_owned(),
        authorization_action: authorization_action.to_owned(),
        authorization_occurred_at: action.created_at.clone(),
        authorization_json: json!({
            "principal": {
                "kind": context.principal().principal_type(),
                "id": context.principal().principal_id(),
            },
            "authorization_basis": authorization_basis,
            "action": authorization_action,
            "event_id": context
                .action_provenance
                .as_ref()
                .map(|value| value.action_id.as_str())
                .unwrap_or_else(|| context.correlation_id()),
            "correlation_id": context.correlation_id(),
            "causation_id": context.causation_id,
            "occurred_at": action.created_at,
        })
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_project_allowlist_excludes_approval_and_release_operations() {
        assert!(is_allowed_project_direct_payload(
            PROJECT_CHARTER_ADOPTION_OPERATION,
            &json!({"action": "draft_revision"}),
        ));
        assert!(is_allowed_project_direct_payload(
            PROJECT_DOCUMENT_OPERATION,
            &json!({"action": "propose_approval"}),
        ));
        assert!(is_allowed_project_direct_payload(
            PROJECT_DOCUMENT_OPERATION,
            &json!({"action": "approve"}),
        ));
        assert!(is_allowed_project_direct_payload(
            PROJECT_DECISION_OPERATION,
            &json!({
                "action": "record_effective",
                "decision_class": "project_implementation"
            }),
        ));
        assert!(!is_allowed_project_direct_payload(
            PROJECT_RELEASE_OPERATION,
            &json!({"action": "propose_candidate"}),
        ));
    }

    #[test]
    fn direct_context_has_agent_principal_and_no_action_provenance() {
        let input = ExecuteDirectProjectCommandInput {
            actor_identity_id: "agent-1".to_owned(),
            scope_type: "agent_chat".to_owned(),
            scope_id: "chat-1".to_owned(),
            project_id: "project-1".to_owned(),
            operation: PROJECT_MILESTONE_OPERATION.to_owned(),
            payload: json!({"action": "set_primary", "expected_milestone_version": 1}),
            idempotency_key: "direct-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            causation_id: Some("cause-1".to_owned()),
            causation_depth: 0,
            requested_permission: "propose_project".to_owned(),
        };
        let context = direct_command_context(
            &input,
            PROJECT_MILESTONE_OPERATION,
            &input.payload,
            "allowed",
        )
        .expect("direct command context");
        assert_eq!(context.principal().principal_type(), "agent");
        assert_eq!(context.principal().principal_id(), "agent-1");
        assert_eq!(context.canonical_scope().scope_id(), "project-1");
        assert!(context.action_provenance.is_none());
        assert_eq!(context.operation(), PROJECT_MILESTONE_OPERATION);
    }
}
