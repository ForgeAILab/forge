//! Typed materialization for Project Agent orchestration proposals.
//!
//! Project native tools persist an `AgentAction` first.  This module is the
//! only path which may turn the safe Project-local proposal operations into
//! Charter/Document/Decision/Milestone/media domain records.  The generic
//! action executor deliberately rejects these operations, so an arbitrary
//! result can never masquerade as a domain mutation.

use std::{collections::BTreeMap, sync::Arc};

#[cfg(test)]
use api_types::canonical_digest_with_schema;
use api_types::{
    ArtifactRef, CurrentVersionOrRevision, ExecutionBaselineContent, PrincipalKind,
    ProjectCharterContent, ProjectDocumentContent, ProjectDocumentKind, RevisionProvenance,
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
    PROJECT_EVIDENCE_OPERATION, PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_VALIDATION_OPERATION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{
    parse_document_kind, AgentActionProvenance, AgentActionService,
    AuthorizationProvenance as CommandAuthorizationProvenance, CommandContext, CommandPrincipal,
    CommandScope, CommandScopeType, ExecutionBaselineCommandService, ExpectedCommandState,
    NewCommandContext, ProjectArtifactCommandService, ProjectCharterCommandService,
    ProjectCharterRevisionCommand, ProjectCommandAuthorization, ProjectDocumentApprovalCommand,
    ProjectDocumentRevisionCommand, ProjectEvidenceCommand, ProjectMilestoneCommandService,
    ProjectValidationCommand, ProposeExecutionBaselineForApprovalCommand, Result,
    SaveExecutionBaselineDraftCommand, ServiceError, EXECUTION_BASELINE_PROPOSE_COMMAND,
    EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
};

#[cfg(test)]
const MILESTONE_DEFINITION_SCHEMA: &str = "forge.milestone-definition/v1";
#[cfg(test)]
const MILESTONE_RENDER_SCHEMA: &str = "forge.milestone-definition-render/v1";

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
            PROJECT_EXECUTION_BASELINE_OPERATION => {
                let Some(baseline_id) = payload.get("baseline_id").and_then(Value::as_str) else {
                    return Ok(None);
                };
                let Some(baseline) = ProjectOrchestrationRepo::get_project_execution_baseline(
                    &*self.db,
                    baseline_id,
                )
                .await?
                else {
                    return Ok(None);
                };
                if baseline.project_id != project.id {
                    return Ok(None);
                }

                let mut current = CurrentVersionOrRevision::new(
                    "project_execution_baseline",
                    baseline.id.clone(),
                );
                current.version = Some(baseline.version);
                if let Some(revision_id) = baseline.current_revision_id {
                    current.revision_id = Some(revision_id.clone());
                    if let Some(revision) =
                        ProjectOrchestrationRepo::get_project_execution_baseline_revision(
                            &*self.db,
                            &revision_id,
                        )
                        .await?
                    {
                        if revision.baseline_id != baseline.id {
                            return Ok(None);
                        }
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
            PROJECT_EXECUTION_BASELINE_OPERATION => {
                self.materialize_direct_execution_baseline(
                    &action,
                    &input.project_id,
                    &payload,
                    &context,
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
            PROJECT_EXECUTION_BASELINE_OPERATION => {
                self.materialize_execution_baseline(
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
        if action.operation == PROJECT_EXECUTION_BASELINE_OPERATION {
            return self
                .resolve_execution_baseline_replay(action, context)
                .await;
        }
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

    /// Baseline commands have lifecycle-specific receipt operations below the
    /// coarse native action operation.  Replay therefore follows the frozen
    /// action-execution link rather than asking the generic Project command
    /// lookup to guess a digest/operation that the adapter never owns.
    async fn resolve_execution_baseline_replay(
        &self,
        action: &AgentAction,
        context: &CommandContext,
    ) -> Result<Option<AgentActionExecution>> {
        let Some(execution) =
            AgentActionRepo::get_successful_action_execution(&*self.db, &action.id).await?
        else {
            return Ok(None);
        };
        if execution.idempotency_key != context.idempotency_key()
            || execution.executed_by_type != context.principal().principal_type()
            || execution.executed_by_id != context.principal().principal_id()
        {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        let receipt = CommandReceiptRepo::get_command_receipt_by_agent_action_execution(
            &*self.db,
            &execution.id,
        )
        .await?;
        let Some(receipt) = receipt else {
            return Err(ServiceError::Conflict(
                "execution baseline action execution has no linked command receipt".to_owned(),
            ));
        };
        if receipt.scope_type != CommandScopeType::Project.as_str()
            || receipt.scope_id != context.canonical_scope().scope_id()
            || !matches!(
                receipt.operation.as_str(),
                EXECUTION_BASELINE_SAVE_DRAFT_COMMAND | EXECUTION_BASELINE_PROPOSE_COMMAND
            )
            || receipt.agent_action_execution_id.as_deref() != Some(execution.id.as_str())
            || receipt.idempotency_key != execution.idempotency_key
            || receipt.principal_type != execution.executed_by_type
            || receipt.principal_id != execution.executed_by_id
            || receipt.outcome_json != execution.result_json.clone().unwrap_or_default()
        {
            return Err(ServiceError::Db(db::DbError::IdempotencyConflict));
        }
        Ok(Some(execution))
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

    async fn materialize_execution_baseline(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        command_context: Option<&CommandContext>,
    ) -> Result<Value> {
        let context = command_context.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project execution-baseline execution requires a canonical command context",
            )
        })?;
        let action_name = string(payload, "action")?;
        let content = self
            .native_execution_baseline_content(project_id, payload)
            .await?;
        let provenance: RevisionProvenance = from_value(payload, "provenance")?;
        // The review target is derived from `content`, never echoed by the
        // agent. Requiring the model to reproduce the server renderer
        // byte-for-byte and recompute both digests is a contract it cannot
        // satisfy, and it failed every baseline the Project Agent authored.
        // The REST route keeps its strict round-trip contract; here the
        // server renders and stamps its own canonical values.
        let render =
            crate::execution_baseline::render_execution_baseline(&content).map_err(|error| {
                ServiceError::invalid_operation(format!("render baseline: {error}"))
            })?;
        let rendered_view = render.rendered_view;
        let render_version =
            crate::execution_baseline::EXECUTION_BASELINE_RENDER_VERSION.to_owned();
        let content_digest = render.content_digest;
        let render_digest = render.render_digest;
        let authorization_action = match action_name.as_str() {
            "draft_revision" | "revise" => EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            "propose_approval" => EXECUTION_BASELINE_PROPOSE_COMMAND,
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Project Agent may draft or propose a baseline; approval and activation are user-only",
                ));
            }
        };
        let authorization = document_command_authorization(action, context, authorization_action);
        let baseline_id = optional_nonempty_string(payload, "baseline_id")?;
        let base_revision_id = optional_nonempty_string(payload, "base_revision_id")?;
        let expected_baseline_version = payload
            .get("expected_baseline_version")
            .map(|_| nonnegative_integer(payload, "expected_baseline_version"))
            .transpose()?;
        let service = ExecutionBaselineCommandService::new(Arc::clone(&self.db));
        let _outcome = match authorization_action {
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND => {
                service
                    .save_draft(SaveExecutionBaselineDraftCommand {
                        project_id: project_id.to_owned(),
                        baseline_id,
                        base_revision_id,
                        expected_baseline_version,
                        content,
                        rendered_view,
                        render_version,
                        content_digest,
                        render_digest,
                        provenance,
                        idempotency_key: context.idempotency_key().to_owned(),
                        authorization,
                        action: context.action_provenance.clone(),
                    })
                    .await?
            }
            EXECUTION_BASELINE_PROPOSE_COMMAND => {
                let baseline_id = baseline_id.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "baseline_id is required when proposing an execution baseline for approval",
                    )
                })?;
                let expected_baseline_version = expected_baseline_version.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "expected_baseline_version is required when proposing an execution baseline for approval",
                    )
                })?;
                service
                    .propose_for_approval(ProposeExecutionBaselineForApprovalCommand {
                        project_id: project_id.to_owned(),
                        baseline_id,
                        base_revision_id,
                        expected_baseline_version,
                        content,
                        rendered_view,
                        render_version,
                        content_digest,
                        render_digest,
                        provenance,
                        idempotency_key: context.idempotency_key().to_owned(),
                        authorization,
                        action: context.action_provenance.clone(),
                    })
                    .await?
            }
            _ => unreachable!(),
        };
        let execution = AgentActionRepo::get_successful_action_execution(&*self.db, &action.id)
            .await?
            .ok_or_else(|| {
                ServiceError::Conflict(
                    "execution baseline command committed without its AgentAction execution"
                        .to_owned(),
                )
            })?;
        let frozen = execution.result_json.ok_or_else(|| {
            ServiceError::Conflict(
                "execution baseline command AgentAction execution has no frozen outcome".to_owned(),
            )
        })?;
        serde_json::from_str(&frozen).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "deserialize frozen execution baseline outcome: {error}"
            ))
        })
    }

    async fn materialize_direct_execution_baseline(
        &self,
        action: &AgentAction,
        project_id: &str,
        payload: &Value,
        context: &CommandContext,
    ) -> Result<Value> {
        let action_name = string(payload, "action")?;
        let content = self
            .native_execution_baseline_content(project_id, payload)
            .await?;
        let provenance: RevisionProvenance = from_value(payload, "provenance")?;
        // The review target is derived from `content`, never echoed by the
        // agent. Requiring the model to reproduce the server renderer
        // byte-for-byte and recompute both digests is a contract it cannot
        // satisfy, and it failed every baseline the Project Agent authored.
        // The REST route keeps its strict round-trip contract; here the
        // server renders and stamps its own canonical values.
        let render =
            crate::execution_baseline::render_execution_baseline(&content).map_err(|error| {
                ServiceError::invalid_operation(format!("render baseline: {error}"))
            })?;
        let rendered_view = render.rendered_view;
        let render_version =
            crate::execution_baseline::EXECUTION_BASELINE_RENDER_VERSION.to_owned();
        let content_digest = render.content_digest;
        let render_digest = render.render_digest;
        let authorization_action = match action_name.as_str() {
            "draft_revision" | "revise" => EXECUTION_BASELINE_SAVE_DRAFT_COMMAND,
            "propose_approval" => EXECUTION_BASELINE_PROPOSE_COMMAND,
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Project Agent may draft or propose a baseline; approval and activation are user-only",
                ));
            }
        };
        let authorization = document_command_authorization(action, context, authorization_action);
        let baseline_id = optional_nonempty_string(payload, "baseline_id")?;
        let base_revision_id = optional_nonempty_string(payload, "base_revision_id")?;
        let expected_baseline_version = payload
            .get("expected_baseline_version")
            .map(|_| nonnegative_integer(payload, "expected_baseline_version"))
            .transpose()?;
        let service = ExecutionBaselineCommandService::new(Arc::clone(&self.db));
        let outcome = match authorization_action {
            EXECUTION_BASELINE_SAVE_DRAFT_COMMAND => {
                service
                    .save_draft_with_context(
                        SaveExecutionBaselineDraftCommand {
                            project_id: project_id.to_owned(),
                            baseline_id,
                            base_revision_id,
                            expected_baseline_version,
                            content,
                            rendered_view,
                            render_version,
                            content_digest,
                            render_digest,
                            provenance,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization,
                            action: None,
                        },
                        context.clone(),
                    )
                    .await?
            }
            EXECUTION_BASELINE_PROPOSE_COMMAND => {
                let baseline_id = baseline_id.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "baseline_id is required when proposing an execution baseline for approval",
                    )
                })?;
                let expected_baseline_version = expected_baseline_version.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "expected_baseline_version is required when proposing an execution baseline for approval",
                    )
                })?;
                service
                    .propose_for_approval_with_context(
                        ProposeExecutionBaselineForApprovalCommand {
                            project_id: project_id.to_owned(),
                            baseline_id,
                            base_revision_id,
                            expected_baseline_version,
                            content,
                            rendered_view,
                            render_version,
                            content_digest,
                            render_digest,
                            provenance,
                            idempotency_key: context.idempotency_key().to_owned(),
                            authorization,
                            action: None,
                        },
                        context.clone(),
                    )
                    .await?
            }
            _ => unreachable!(),
        };
        serde_json::to_value(outcome).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "serialize direct execution baseline outcome: {error}"
            ))
        })
    }

    /// Build the native baseline content from authoritative Project state.
    /// The model selects a Charter revision by id; Forge authorizes that id in
    /// the bound Project and supplies every digest/render echo from persistence
    /// before the baseline itself is rendered. REST callers retain the strict
    /// round-trip validation in `ExecutionBaselineCommandService`.
    async fn native_execution_baseline_content(
        &self,
        project_id: &str,
        payload: &Value,
    ) -> Result<ExecutionBaselineContent> {
        let mut content_value = payload
            .get("content")
            .cloned()
            .ok_or_else(|| ServiceError::invalid_operation("content is required"))?;
        let content_object = content_value.as_object_mut().ok_or_else(|| {
            ServiceError::invalid_operation("execution baseline content must be an object")
        })?;
        let charter_revision_id = content_object
            .get("charter_revision")
            .and_then(Value::as_object)
            .and_then(|reference| reference.get("revision_id"))
            .and_then(Value::as_str)
            .filter(|revision_id| !revision_id.trim().is_empty())
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "content.charter_revision.revision_id must be non-empty",
                )
            })?;
        let charter_revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, charter_revision_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::conflict("Charter revision is not owned by this Project")
                })?;
        let charter =
            ProjectOrchestrationRepo::get_project_charter(&*self.db, &charter_revision.charter_id)
                .await?
                .filter(|charter| charter.project_id.as_deref() == Some(project_id))
                .ok_or_else(|| {
                    ServiceError::conflict("Charter revision is not owned by this Project")
                })?;
        content_object.insert(
            "charter_revision".to_owned(),
            serde_json::to_value(ArtifactRef {
                artifact_id: charter.id,
                revision_id: charter_revision.id,
                content_digest: charter_revision.content_digest,
                render_version: Some(charter_revision.render_version),
                render_digest: Some(charter_revision.rendered_digest),
            })
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "serialize canonical Charter ArtifactRef: {error}"
                ))
            })?,
        );

        // The policy digest is a hash over the frozen release policy the caller
        // just supplied. A model cannot compute a digest, so demanding it echo
        // one made the baseline unauthorable. A placeholder keeps the typed
        // shape intact through deserialization; the real value is derived below
        // from the policy payload the server is about to validate.
        content_object.insert(
            "release_policy_digest".to_owned(),
            Value::String("pending-server-derived".to_owned()),
        );
        let mut content: ExecutionBaselineContent =
            serde_json::from_value(content_value).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid content: {error}"))
            })?;
        // Project Agents start with the complete safe adaptive vocabulary.
        // Fine-grained reductions belong to an explicit user setting; until
        // that surface exists, a model-authored baseline must not accidentally
        // remove its own ability to split, sequence, or replace in-scope Tasks.
        content.adaptive_envelope.allowed_task_operations =
            api_types::AdaptiveTaskOperation::ALL.to_vec();
        content.release_policy_digest =
            crate::execution_baseline::release_policy_digest(&content.release_policy).map_err(
                |error| ServiceError::invalid_operation(format!("release policy digest: {error}")),
            )?;
        Ok(content)
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
    if operation == PROJECT_EXECUTION_BASELINE_OPERATION {
        return match payload.get("action").and_then(Value::as_str) {
            Some("draft_revision") | Some("revise") => {
                Ok(EXECUTION_BASELINE_SAVE_DRAFT_COMMAND.to_owned())
            }
            Some("propose_approval") => Ok(EXECUTION_BASELINE_PROPOSE_COMMAND.to_owned()),
            _ => Err(ServiceError::invalid_operation(
                "Project Agent may draft or propose a baseline; approval and activation are user-only",
            )),
        };
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
    if operation == EXECUTION_BASELINE_SAVE_DRAFT_COMMAND
        || operation == EXECUTION_BASELINE_PROPOSE_COMMAND
    {
        versions.insert(
            "baseline_version".to_owned(),
            payload
                .get("expected_baseline_version")
                .and_then(Value::as_i64)
                .unwrap_or(1),
        );
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

fn optional_nonempty_string(payload: &Value, field: &str) -> Result<Option<String>> {
    match payload.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(ServiceError::invalid_operation(format!(
            "{field} must be a non-empty string when supplied"
        ))),
    }
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

fn nonnegative_integer(payload: &Value, field: &str) -> Result<i64> {
    let value = integer(payload, field)?;
    if value < 0 {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must be a non-negative integer"
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
    use api_types::{
        AdaptiveEnvelope, ArtifactRef, ExecutionBaselineContent, ExecutionBaselineReleasePolicy,
        PrincipalKind, PrincipalRef,
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
                allowed_task_operations: vec![api_types::AdaptiveTaskOperation::Split],
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
    async fn action_baseline_materializer_rehydrates_charter_ref_and_persists_manifest_v076() {
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

        let mut content = baseline_test_content(
            &charter_id,
            &charter_revision_id,
            "charter-content-digest",
            &milestone_id,
            &milestone_definition_revision_id,
        );
        content.charter_revision.artifact_id = "invented-charter".to_owned();
        content.charter_revision.content_digest = "invented-content-digest".to_owned();
        content.charter_revision.render_version = Some("forge.charter-render/v1".to_owned());
        content.charter_revision.render_digest = Some("invented-render-digest".to_owned());
        let mut native_payload = json!({"content": content});
        let action_service = ProjectOrchestrationActionService::new(Arc::new(db.clone()));
        let content = action_service
            .native_execution_baseline_content(&project_id, &native_payload)
            .await
            .expect("native content rehydrates the persisted Charter revision");
        assert_eq!(
            content.adaptive_envelope.allowed_task_operations,
            api_types::AdaptiveTaskOperation::ALL
        );
        assert_eq!(content.charter_revision.artifact_id, charter_id);
        assert_eq!(
            content.charter_revision.content_digest,
            "charter-content-digest"
        );
        assert_eq!(
            content.charter_revision.render_version.as_deref(),
            Some("charter-render-v1")
        );
        assert_eq!(
            content.charter_revision.render_digest.as_deref(),
            Some("charter-render-digest")
        );
        // The native provider adapter removes these redundant echoes entirely;
        // the service accepts that exact shape as long as revision_id remains.
        for field in [
            "artifact_id",
            "content_digest",
            "render_version",
            "render_digest",
        ] {
            native_payload["content"]["charter_revision"]
                .as_object_mut()
                .expect("Charter ref object")
                .remove(field);
        }
        let content_without_echoes = action_service
            .native_execution_baseline_content(&project_id, &native_payload)
            .await
            .expect("revision_id alone resolves the canonical Charter ref");
        assert_eq!(
            content_without_echoes.charter_revision,
            content.charter_revision
        );
        let expected_release_policy_digest = content.release_policy_digest.clone();
        let expected_release_policy =
            serde_json::to_value(&content.release_policy).expect("release policy JSON");
        let expected_adaptive_envelope =
            serde_json::to_value(&content.adaptive_envelope).expect("adaptive envelope JSON");
        // No baseline_id: drafting a new baseline must server-mint the shell
        // id. The native adapter uses this same command service below.
        let rendered = crate::render_execution_baseline(&content).expect("render baseline");
        let service = crate::ExecutionBaselineCommandService::new(Arc::new(db.clone()));
        let result = service
            .save_draft(crate::SaveExecutionBaselineDraftCommand {
                project_id: project_id.clone(),
                baseline_id: None,
                base_revision_id: None,
                expected_baseline_version: None,
                content,
                rendered_view: rendered.rendered_view,
                render_version: crate::EXECUTION_BASELINE_RENDER_VERSION.to_owned(),
                content_digest: rendered.content_digest,
                render_digest: rendered.render_digest,
                provenance: RevisionProvenance {
                    author: PrincipalRef {
                        kind: PrincipalKind::User,
                        id: user_id.clone(),
                        display_name: None,
                    },
                    profile_revision: None,
                    operating_skill_revision: None,
                    source_refs: Vec::new(),
                    change_summary: "test baseline draft".to_owned(),
                    material_diff: None,
                },
                idempotency_key: "baseline-action-dedupe".to_owned(),
                authorization: ProjectCommandAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user_id,
                    policy_result: "allowed".to_owned(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some("propose_project".to_owned()),
                    correlation_id: "baseline-action-correlation".to_owned(),
                    causation_id: None,
                    causation_depth: 0,
                    authorization_event_id: "baseline-action-authorization".to_owned(),
                    authorization_basis: "test".to_owned(),
                    authorization_action: crate::EXECUTION_BASELINE_SAVE_DRAFT_COMMAND.to_owned(),
                    authorization_occurred_at: now,
                    authorization_json: "{}".to_owned(),
                },
                action: None,
            })
            .await
            .expect("baseline command materializes on fresh V076 schema");
        let minted_baseline_id = result.baseline_id.as_str();
        uuid::Uuid::parse_str(minted_baseline_id).expect("baseline id is a server-minted UUID");
        let revision_id = result
            .revision_id
            .as_deref()
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
