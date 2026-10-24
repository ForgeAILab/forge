//! Forge service adapter for the host's scope-derived native tools.
//!
//! This adapter exposes read projections and policy-checked proposal commands.
//! Query operations use the read boundary; direct `task.propose`,
//! `task.adaptive`, Main Charter, and bounded Project orchestration commands
//! call their shared command services, while approval-required mutations
//! retain an `AgentAction` envelope.

#[cfg(test)]
use forge_agent_host::PROJECT_CURRENT_STATE_OPERATION;
use services::{
    bounded_file::read_bounded_regular_file,
    task_service::execution::{ExecutionOutboxInput, ExecutionOutboxReport},
};
mod project_proposals;
mod registered_proposals;
mod registered_reads;
mod task_lead;
mod task_proposals;
mod task_records;
#[cfg(test)]
use std::collections::BTreeSet;

use std::{
    collections::BTreeMap,
    fs,
    net::IpAddr,
    path::{Component, Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use sha2::{Digest, Sha256};

use api_types::{
    ApprovalTarget, CanonicalScopeRef as OutcomeScopeRef, CurrentVersionOrRevision, DeniedBy,
    OrchestrationOutcome, OutcomeCode, OutcomeScopeType, OutcomeStatus, RetryAction,
    RetryInstruction, SetupRequirement,
};
use async_trait::async_trait;
use chrono::Utc;
use config::{is_blocked_public_search_address, is_restricted_public_hostname, PublicSearchConfig};
use db::{
    AgentAction, AgentActionListQuery, AgentActionPolicyResult, AgentActionRepo,
    AgentCommitmentListQuery, AgentCommitmentRepo, AgentInboxListQuery, AgentInboxRepo,
    CommandReceiptRepo, MemoryScopeGrant, SqliteDb, TaskBoardRepo,
};
use forge_agent_host::outbound_http::{
    public_http_client_builder, ProxyEnvironment, PublicAddressPolicy, PublicHttpError,
};
use forge_agent_host::{
    contains_authority_override, operation_contract, operation_descriptor, operation_permission,
    AgentHostError, CanonicalScope, CanonicalScopeType, CommandObservation, ForgeToolProvider,
    OperationClassification, PublicSearchScope, WorkspaceAccess, MAIN_CHARTER_DRAFT_OPERATION,
    MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_START_OPERATION,
    MAIN_PROJECT_CREATE_OPERATION, PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_DECISION_OPERATION,
    PROJECT_DOCUMENT_OPERATION, PROJECT_ESCALATE_OPERATION, PROJECT_EVIDENCE_OPERATION,
    PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
    PROJECT_VALIDATION_OPERATION, TASK_ACTION_OPERATION, TASK_ADAPTIVE_OPERATION,
    TASK_DEPENDENCY_OPERATION, TASK_EVIDENCE_OPERATION, TASK_PLAN_OPERATION,
    TASK_PROPOSE_OPERATION, TASK_WORKLOG_OPERATION,
};
use reqwest::header::ACCEPT;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    agent_inquiry_runner::{InquiryRequest, InquiryRunner},
    coordination_service::{AgentActionService, ProposeActionInput},
    memory::{MemoryAccessContext, MemoryService},
    project_agent_actions::ExecuteDirectProjectCommandInput,
    project_runtime::{
        load_effective_project_state, load_project_adoption_charter, ProjectCurrentStateResponse,
    },
    task_service::{
        AdaptiveTaskChild, AdaptiveTaskCommand, AdaptiveTaskCommandResult, AdaptiveTaskOperation,
        DirectTaskProposalInput, TaskDependencyAction, TaskProposalCommandResult,
        TaskProposalPayload,
    },
    MainGenesisCharterDraftRequest, MainGenesisCommandService, MainGenesisDraftCommandInput,
    MainGenesisDraftPrincipal, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, MainOrchestrationQueryService, OrchestrationAuthorizationService,
    ProjectOrchestrationActionService, TaskService,
};

type SessionDenialRows = BTreeMap<(String, String), Vec<db::ChatSessionDenial>>;

/// Forge-owned provider injected into native Agent Runtime compositions.
#[derive(Clone)]
pub struct CoordinationToolProvider {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
    authorization: OrchestrationAuthorizationService,
    memory: MemoryService,
    main_queries: MainOrchestrationQueryService,
    project_actions: ProjectOrchestrationActionService,
    public_search: Arc<RwLock<Option<PublicSearchConfig>>>,
    /// Reminders seen by the resolved turn's state-card read. Success only
    /// deletes a durable reminder when that read actually found one.
    read_session_denials: Arc<RwLock<SessionDenialRows>>,
    /// Shared TaskService used to execute directly admitted `task.propose` and
    /// `task.adaptive` commands inline, so native proposals materialize
    /// through the durable command receipt path without a separate caller.
    task_service: Arc<RwLock<Option<Arc<TaskService>>>>,
    /// Root of the media store. Evidence capture writes the bytes it was given
    /// here before recording the Task media row that references them.
    media_root: Arc<RwLock<Option<PathBuf>>>,
    /// Dispatches ephemeral inquiry sub-agents. Attached after construction
    /// for the same reason `task_service` is: the runner reaches the native
    /// backend, which holds this provider, so wiring it at construction would
    /// close a cycle.
    inquiry_runner: Arc<RwLock<Option<Arc<dyn InquiryRunner>>>>,
}

impl std::fmt::Debug for CoordinationToolProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinationToolProvider")
            .finish_non_exhaustive()
    }
}

impl CoordinationToolProvider {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            actions: AgentActionService::new(Arc::clone(&db)),
            authorization: OrchestrationAuthorizationService::new(Arc::clone(&db)),
            memory: MemoryService::new(Arc::clone(&db)),
            main_queries: MainOrchestrationQueryService::new(Arc::clone(&db)),
            project_actions: ProjectOrchestrationActionService::new(Arc::clone(&db)),
            public_search: Arc::new(RwLock::new(None)),
            read_session_denials: Arc::new(RwLock::new(BTreeMap::new())),
            task_service: Arc::new(RwLock::new(None)),
            media_root: Arc::new(RwLock::new(None)),
            inquiry_runner: Arc::new(RwLock::new(None)),
            db,
        }
    }

    /// Recheck stored causes through the same policy used by native calls.
    /// Reminders are advisory: a read, recheck or deletion failure cannot fail
    /// turn admission or hide the agent's normal state card.
    pub async fn chat_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Vec<(String, DeniedBy)> {
        let key = (identity_id.to_owned(), chat_id.to_owned());
        if session_id.is_some() {
            if let Ok(mut read) = self.read_session_denials.write() {
                read.remove(&key);
            }
        }
        match self
            .recheck_session_denials(identity_id, profile_id, chat_id, session_id)
            .await
        {
            Ok((unavailable, rows)) => {
                if session_id.is_some() {
                    if let Ok(mut read) = self.read_session_denials.write() {
                        read.insert(key, rows);
                    }
                }
                unavailable
            }
            Err(error) => {
                tracing::warn!(identity_id, chat_id, error = %error, "could not read native denial reminders");
                Vec::new()
            }
        }
    }

    async fn recheck_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> crate::Result<(Vec<(String, DeniedBy)>, Vec<db::ChatSessionDenial>)> {
        use db::ChatSessionDenialRepo;
        let rows = self
            .db
            .chat_session_denials(identity_id, profile_id, chat_id, session_id)
            .await?;
        let mut unavailable = Vec::new();
        let mut holding_rows = Vec::new();
        for row in rows {
            let mut cause = row.denied_by.parse::<DeniedBy>().ok();
            let holds = match cause.as_ref() {
                Some(DeniedBy::OperationNotInScope) => true,
                Some(DeniedBy::ProjectPaused(_)) => {
                    let scope = CanonicalScope {
                        scope_type: CanonicalScopeType::AgentChat,
                        scope_id: chat_id.to_owned(),
                        workspace_access: WorkspaceAccess::Deny,
                    };
                    let project_id = self
                        .authorization
                        .project_orchestration_target(identity_id, &scope)
                        .await?;
                    let pause = self.db.native_paused_project_reason(&project_id).await?;
                    if let Some(reason) = pause {
                        cause = Some(db::project_pause_denial(reason.as_deref()));
                        true
                    } else {
                        false
                    }
                }
                Some(cause) if cause.withdraws_operation() && cause.clears() => {
                    // An operation the registry does not offer in an Agent
                    // Chat has no permission there. Its denial is then judged
                    // by the permission it was refused for, so the reminder
                    // holds for as long as the policy still refuses it.
                    let permission = operation_permission(
                        CanonicalScopeType::AgentChat,
                        &row.operation,
                    )
                    .or(match cause {
                        DeniedBy::PermissionMissing(permission) => Some(permission.as_str()),
                        _ => None,
                    });
                    if let Some(permission) = permission {
                        let (result, reason) = self
                            .actions
                            .evaluate_direct_command_policy(
                                identity_id,
                                "agent_chat",
                                chat_id,
                                permission,
                                &row.operation,
                                None,
                            )
                            .await?;
                        result == AgentActionPolicyResult::Denied
                            && reason
                                .as_deref()
                                .map(|reason| native_denial_cause(reason, Some(permission)))
                                .as_ref()
                                == Some(cause)
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if holds {
                let entry = (
                    row.operation.clone(),
                    cause.expect("holding cause is parsed"),
                );
                if !unavailable.contains(&entry) {
                    unavailable.push(entry);
                }
                holding_rows.push(row);
            } else {
                self.db.delete_chat_session_denial(&row).await?;
            }
        }
        Ok((unavailable, holding_rows))
    }

    /// Attach the shared TaskService so admitted Task commands execute inline
    /// through the shared receipt-backed command paths.
    /// Attach the media storage root so Task sessions can capture evidence.
    pub fn set_media_root(&self, media_root: PathBuf) {
        if let Ok(mut slot) = self.media_root.write() {
            *slot = Some(media_root);
        }
    }

    fn media_root_handle(&self) -> Option<PathBuf> {
        self.media_root.read().ok().and_then(|slot| slot.clone())
    }

    pub fn set_task_service(&self, task_service: Arc<TaskService>) {
        if let Ok(mut slot) = self.task_service.write() {
            *slot = Some(task_service);
        }
    }

    pub(crate) fn task_service_handle(&self) -> Option<Arc<TaskService>> {
        self.task_service.read().ok().and_then(|slot| slot.clone())
    }

    pub fn set_inquiry_runner(&self, runner: Arc<dyn InquiryRunner>) {
        if let Ok(mut slot) = self.inquiry_runner.write() {
            *slot = Some(runner);
        }
    }

    fn inquiry_runner_handle(&self) -> Option<Arc<dyn InquiryRunner>> {
        self.inquiry_runner
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Check the immutable command identity before evaluating mutable policy.
    /// An exact adaptive retry must reach the shared receipt replay path even
    /// if the Project's current governance has changed since the original
    /// commit.  The command service still performs the digest-aware lookup and
    /// returns an idempotency conflict for a changed payload.
    async fn adaptive_receipt_exists(
        &self,
        actor_identity_id: &str,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<bool, AgentHostError> {
        CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            "agent",
            actor_identity_id,
            "project",
            project_id,
            TASK_ADAPTIVE_OPERATION,
            idempotency_key,
        )
        .await
        .map(|receipt| receipt.is_some())
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    /// Configure the optional public search endpoint used by native Main and
    /// Project Agent Chat turns.  This is a runtime setting, not a credential
    /// store; the provider never accepts authentication headers or cookies.
    pub fn set_public_search_config(&self, config: Option<PublicSearchConfig>) {
        if let Ok(mut slot) = self.public_search.write() {
            *slot = config;
        }
    }

    fn public_search_config(&self) -> Option<PublicSearchConfig> {
        self.public_search.read().ok().and_then(|slot| slot.clone())
    }

    /// Convert a native service result into the stable model-facing envelope.
    /// The operation and scope passed here are always the host-derived values;
    /// neither is read from model payloads.  Approval rows are deliberately
    /// represented as `approval_required`, never as a committed domain
    /// success.
    fn structured_success(
        operation: &str,
        scope: &CanonicalScope,
        correlation_id: &str,
        mut result: Value,
        approval_required: bool,
    ) -> Result<Value, AgentHostError> {
        let replayed = result
            .get("replayed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // `replayed` is the envelope-level outcome field. Keep the adaptive
        // domain result itself frozen-identical across first commit and exact
        // replay so callers can compare the receipt-backed result directly.
        if operation == TASK_ADAPTIVE_OPERATION {
            if let Some(object) = result.as_object_mut() {
                object.remove("replayed");
            }
        }
        let receipt_id = result
            .get("receipt_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let event_id = result
            .get("event_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let requires_user_authorization = result
            .get("requires_user_authorization")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let approval_required = approval_required || requires_user_authorization;
        let scope = outcome_scope(scope);
        let mut outcome = if approval_required {
            let mut outcome = OrchestrationOutcome::new(
                OutcomeCode::ApprovalRequired,
                OutcomeStatus::ApprovalRequired,
                operation,
                scope.clone(),
                correlation_id,
            );
            outcome.safe_message =
                "approval is required before this operation is committed".to_owned();
            outcome.approval_target = Some(approval_target(operation, scope, &result));
            // Some direct commands (for example a baseline proposal) commit
            // an exact proposal receipt before user approval, while a pure
            // approval-backed Action has no domain commit. Preserve the
            // former's frozen result without making the latter look executed.
            if receipt_id.is_some() {
                outcome.result = Some(result);
            }
            outcome
        } else {
            OrchestrationOutcome::succeeded(operation, scope, correlation_id, Some(result))
        };
        outcome.replayed = replayed;
        outcome.receipt_id = receipt_id;
        outcome.event_id = event_id;
        serde_json::to_value(outcome).map_err(|_| AgentHostError::ProtectedPersistence)
    }

    async fn project_pause_cause(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        paused_project_id: Option<&str>,
    ) -> DeniedBy {
        let project_id = if scope.scope_type == CanonicalScopeType::Task {
            self.db
                .native_task_project_id(&scope.scope_id)
                .await
                .ok()
                .flatten()
        } else {
            self.authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .ok()
        };
        let Some(project_id) =
            project_id.filter(|id| paused_project_id.is_none_or(|paused| paused == id))
        else {
            return DeniedBy::Unspecified;
        };
        let reason = self
            .db
            .native_project_pause_reason(&project_id)
            .await
            .ok()
            .flatten()
            .flatten();
        db::project_pause_denial(reason.as_deref())
    }

    /// Build a structured failure after the command boundary has established
    /// the canonical actor/scope.  Current state is loaded only after that
    /// authorization check and only for the Project resource named by a
    /// typed command payload.
    async fn structured_boundary_error(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: &Value,
        error: AgentHostError,
    ) -> AgentHostError {
        let correlation_id = correlation_id(arguments, operation, scope);
        let error_kind = match &error {
            AgentHostError::StructuredOutcome(outcome) => outcome.code.as_str(),
            AgentHostError::Authority(_) => "authority",
            AgentHostError::AgentPaused { .. } => "agent_paused",
            AgentHostError::ProjectPaused { .. } => "project_paused",
            AgentHostError::Configuration(_) => "configuration",
            AgentHostError::SessionRetired => "session_retired",
            AgentHostError::SessionNotFound => "session_not_found",
            AgentHostError::CredentialNotFound => "credential_not_found",
            AgentHostError::VersionConflict => "version_conflict",
            AgentHostError::Unsupported(_) => "unsupported",
            AgentHostError::Runtime(_) => "runtime",
            AgentHostError::RuntimeWithUsage { .. } => "runtime",
            AgentHostError::TurnLimitReached { .. } => "turn_limit_reached",
            AgentHostError::ProtectedPersistence => "protected_persistence",
        };
        tracing::warn!(
            correlation_id = %correlation_id,
            operation,
            error_kind,
            "native Forge orchestration operation failed; cause redacted"
        );
        let mut outcome = match error {
            AgentHostError::StructuredOutcome(outcome) => *outcome,
            AgentHostError::SessionRetired => OrchestrationOutcome::failed(
                OutcomeCode::ValidationError,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "session history has been retired; start a new session",
            ),
            AgentHostError::SessionNotFound | AgentHostError::CredentialNotFound => {
                OrchestrationOutcome::failed(
                    OutcomeCode::NotFound,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    "the requested Forge resource is unavailable",
                )
            }
            AgentHostError::VersionConflict => OrchestrationOutcome::failed(
                OutcomeCode::VersionConflict,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the authorized resource changed; refresh current state and retry",
            ),
            AgentHostError::Configuration(_) => {
                let mut outcome = OrchestrationOutcome::failed(
                    OutcomeCode::SetupRequired,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    "required Forge setup is incomplete",
                );
                outcome.setup_requirements =
                    Some(vec![SetupRequirement::new("forge_configuration")]);
                outcome.retry = Some(RetryInstruction::new(RetryAction::CompleteSetup, false));
                outcome
            }
            AgentHostError::AgentPaused { agent_id } => OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &correlation_id,
                if agent_id == actor_identity_id {
                    DeniedBy::IdentityPaused
                } else {
                    DeniedBy::TargetAgentPaused
                },
            ),
            AgentHostError::ProjectPaused { project_id } => OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &correlation_id,
                self.project_pause_cause(actor_identity_id, scope, Some(&project_id))
                    .await,
            ),
            AgentHostError::Authority(detail) => {
                let mut cause =
                    native_denial_cause(&detail, operation_permission(scope.scope_type, operation));
                if matches!(cause, DeniedBy::ProjectPaused(_)) {
                    cause = self
                        .project_pause_cause(actor_identity_id, scope, None)
                        .await;
                }
                tracing::warn!(diagnostic = %detail, denied_by = %cause, operation,
                    "native policy refusal; diagnostic is server-only");
                OrchestrationOutcome::terminal_denial(
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    cause,
                )
            }
            AgentHostError::Unsupported(detail) => {
                let mut outcome = OrchestrationOutcome::failed(
                    OutcomeCode::ValidationError,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    format!(
                        "the operation or arguments are not valid for this Forge surface ({detail})"
                    ),
                );
                outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
                outcome
            }
            AgentHostError::Runtime(_) => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
            AgentHostError::RuntimeWithUsage { .. } => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
            AgentHostError::TurnLimitReached { .. } => OrchestrationOutcome::failed(
                OutcomeCode::TransientFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation reached a runtime limit; retry later",
            ),
            AgentHostError::ProtectedPersistence => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
        };
        outcome.operation = operation.to_owned();
        outcome.scope = outcome_scope(scope);
        outcome.correlation_id = correlation_id;

        if outcome.code == OutcomeCode::PolicyDenied && outcome.denied_by.is_none() {
            outcome = OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &outcome.correlation_id,
                DeniedBy::Unspecified,
            );
        }
        if outcome
            .denied_by
            .as_ref()
            .is_some_and(DeniedBy::withdraws_operation)
            && operation != "message.send"
            && self
                .authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .is_ok()
        {
            if let Some(permission) = operation_permission(scope.scope_type, "message.send") {
                if self
                    .actions
                    .evaluate_direct_command_policy(
                        actor_identity_id,
                        scope_type_name(scope.scope_type),
                        &scope.scope_id,
                        permission,
                        "message.send",
                        None,
                    )
                    .await
                    .is_ok_and(|(result, _)| {
                        matches!(
                            result,
                            AgentActionPolicyResult::Allowed
                                | AgentActionPolicyResult::ApprovalRequired
                        )
                    })
                {
                    outcome.alternatives = Some(vec!["message.send".to_owned()]);
                    outcome.safe_message.push_str(" Use message.send to escalate to the user only what your authority cannot cover.");
                }
            }
        }

        if matches!(outcome.code, OutcomeCode::VersionConflict) {
            let current = match self
                .authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .map_err(native_scope_error)
            {
                Ok(project_id) => self
                    .project_actions
                    .authorized_current_version_or_revision(&project_id, operation, arguments)
                    .await
                    .ok()
                    .flatten(),
                Err(_) => None,
            };
            if let Some(current) = current {
                outcome.current_version_or_revision = Some(current.clone());
                outcome.retry = Some(retry_for_current(operation, &current));
            } else if outcome.retry.is_none() {
                outcome.retry = Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true));
            }
        }
        if matches!(outcome.code, OutcomeCode::IdempotencyConflict) {
            // Never query current state for a conflicting key.  A new key is
            // the only safe corrective action when the receipt is bound to a
            // different command input.
            outcome.current_version_or_revision = None;
            outcome.retry = Some(RetryInstruction::new(
                RetryAction::UseNewIdempotencyKey,
                false,
            ));
        }
        AgentHostError::StructuredOutcome(Box::new(outcome))
    }
    async fn read_internal(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
        admitted: Option<&operation_registry::authority::EffectiveAuthority>,
    ) -> Result<Value, AgentHostError> {
        let resolved;
        let admitted = if let Some(admitted) = admitted {
            Some(admitted)
        } else if registered_reads::CATALOG.lookup(operation).is_some() {
            resolved = match self
                .resolve_registered_authority(actor_identity_id, scope)
                .await
            {
                Ok(authority) => authority,
                Err(error) => {
                    return Err(self
                        .structured_boundary_error(
                            actor_identity_id,
                            scope,
                            operation,
                            &arguments,
                            error,
                        )
                        .await);
                }
            };
            Some(&resolved)
        } else {
            None
        };
        if let (Some(authority), Some(spec)) =
            (admitted, registered_reads::CATALOG.lookup(operation))
        {
            if let Err(error) = self.evaluate_registered_authority(authority, scope, spec) {
                return Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        operation,
                        &arguments,
                        error,
                    )
                    .await);
            }
        }
        let boundary_arguments = arguments.clone();
        match operation_descriptor(scope.scope_type, operation, None).classification {
            OperationClassification::Query => {}
            OperationClassification::Denied => {
                return Err(AgentHostError::StructuredOutcome(Box::new(
                    OrchestrationOutcome::terminal_denial(
                        operation,
                        outcome_scope(scope),
                        correlation_id(&arguments, operation, scope),
                        DeniedBy::OperationNotInScope,
                    ),
                )));
            }
            OperationClassification::DirectCommand
            | OperationClassification::ApprovalRequiredAction => {
                return Err(AgentHostError::Unsupported(
                    "mutation operations execute through the proposal boundary".to_owned(),
                ));
            }
        }
        let result = if let Some(spec) = registered_reads::CATALOG.lookup(operation) {
            let context = registered_reads::Context {
                provider: self,
                actor_identity_id,
                scope,
                proposal_arguments: None,
                runtime_session_id: None,
                admitted_authority: admitted,
            };
            match spec.dispatch(&context, arguments).await {
                Ok(result) => Ok(result),
                Err(operation_registry::DispatchError::Handler(error)) => Err(error),
                Err(operation_registry::DispatchError::InvalidInput(message)) => {
                    Err(invalid_arguments(message))
                }
            }
        } else {
            Err(AgentHostError::Unsupported(
                "Forge read operation is not implemented".to_owned(),
            ))
        };
        if operation_contract(operation).is_some() {
            match result {
                Ok(result) => Self::structured_success(
                    operation,
                    scope,
                    &correlation_id(&boundary_arguments, operation, scope),
                    result,
                    false,
                ),
                Err(error) => Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        operation,
                        &boundary_arguments,
                        error,
                    )
                    .await),
            }
        } else {
            match result {
                Err(error)
                    if matches!(&error, AgentHostError::Authority(_))
                        || matches!(
                            &error,
                            AgentHostError::StructuredOutcome(_)
                                | AgentHostError::AgentPaused { .. }
                                | AgentHostError::ProjectPaused { .. }
                        ) =>
                {
                    Err(self
                        .structured_boundary_error(
                            actor_identity_id,
                            scope,
                            operation,
                            &boundary_arguments,
                            error,
                        )
                        .await)
                }
                other => other,
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn proposal_boundary(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
        prepared: bool,
        admitted_authority: Option<&operation_registry::authority::EffectiveAuthority>,
    ) -> Result<Value, AgentHostError> {
        let correlation = correlation_id(&arguments, operation, scope);
        let payload = arguments.get("payload");
        let approval_required = payload
            .map(|payload| {
                matches!(
                    operation_descriptor(scope.scope_type, operation, Some(payload)).classification,
                    OperationClassification::ApprovalRequiredAction
                )
            })
            .unwrap_or(false);
        let result = if registered_proposals::CATALOG.lookup(operation).is_some() {
            self.registered_proposal(
                actor_identity_id,
                scope,
                runtime_session_id,
                operation,
                arguments.clone(),
                prepared,
                admitted_authority,
            )
            .await
        } else {
            Err(AgentHostError::Unsupported(
                "Forge proposal operation is not implemented".to_owned(),
            ))
        };
        if operation_contract(operation).is_some() {
            match result {
                Ok(result) => Self::structured_success(
                    operation,
                    scope,
                    &correlation,
                    result,
                    approval_required,
                ),
                Err(error) => Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        operation,
                        &arguments,
                        error,
                    )
                    .await),
            }
        } else {
            match result {
                Err(error)
                    if matches!(&error, AgentHostError::Authority(_))
                        || matches!(
                            &error,
                            AgentHostError::StructuredOutcome(_)
                                | AgentHostError::AgentPaused { .. }
                                | AgentHostError::ProjectPaused { .. }
                        ) =>
                {
                    Err(self
                        .structured_boundary_error(
                            actor_identity_id,
                            scope,
                            operation,
                            &arguments,
                            error,
                        )
                        .await)
                }
                other => other,
            }
        }
    }
}

#[async_trait]
impl ForgeToolProvider for CoordinationToolProvider {
    async fn read_denial(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        // Only a typed denial stops preparation. A resolution failure is
        // reported by the read itself, which names the operation and scope.
        match self.registered_authority(actor, scope, operation).await {
            Err(error @ AgentHostError::StructuredOutcome(_)) => Err(error),
            _ => Ok(()),
        }
    }

    async fn record_terminal_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
        denied_by: &DeniedBy,
    ) -> Result<(), AgentHostError> {
        if matches!(denied_by, DeniedBy::PermissionMissing(name)
            if operation_permission(scope.scope_type, operation) != Some(name.as_str()))
        {
            return Ok(());
        }
        if scope.scope_type == CanonicalScopeType::AgentChat {
            db::ChatSessionDenialRepo::record_chat_session_denial(
                &*self.db,
                actor_identity_id,
                &scope.scope_id,
                session_token,
                operation,
                denied_by,
            )
            .await
            .map_err(|error| service_error(error.into()))?;
        }
        Ok(())
    }

    async fn clear_terminal_denials(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        if scope.scope_type == CanonicalScopeType::AgentChat {
            let rows = self
                .read_session_denials
                .read()
                .ok()
                .and_then(|read| {
                    read.get(&(actor_identity_id.to_owned(), scope.scope_id.clone()))
                        .map(|rows| {
                            rows.iter()
                                .filter(|row| {
                                    row.operation == operation
                                        && (row.session_id == session_token
                                            || row.runtime_session_id.as_deref()
                                                == Some(session_token))
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                })
                .unwrap_or_default();
            for row in rows {
                db::ChatSessionDenialRepo::delete_chat_session_denial(&*self.db, &row)
                    .await
                    .map_err(|error| service_error(error.into()))?;
                if let Ok(mut read) = self.read_session_denials.write() {
                    if let Some(rows) =
                        read.get_mut(&(actor_identity_id.to_owned(), scope.scope_id.clone()))
                    {
                        rows.retain(|stored| {
                            stored.session_id != row.session_id
                                || stored.operation != row.operation
                                || stored.denied_by != row.denied_by
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn observe_command(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        observation: CommandObservation,
    ) -> Result<Value, AgentHostError> {
        // The observation belongs to the Project this session is bound to;
        // the binding, not the payload, says which Project that is.
        let arguments =
            json!({"session_id": observation.session_id, "turn_id": observation.turn_id});
        let project_id = match self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
        {
            Ok(project_id) => project_id,
            Err(error) => {
                return Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        "observe_command",
                        &arguments,
                        service_error(error),
                    )
                    .await);
            }
        };
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        let saved = self
            .db
            .insert_native_project_command_observation(
                db::InsertNativeProjectCommandObservationInput {
                    id: &id,
                    project_id: &project_id,
                    actor_identity_id,
                    scope_type: scope_type_name(scope.scope_type),
                    scope_id: &scope.scope_id,
                    session_id: &observation.session_id,
                    turn_id: observation.turn_id.as_deref(),
                    program: &observation.program,
                    args_json: serde_json::to_string(&observation.args)
                        .unwrap_or_else(|_| "[]".to_owned()),
                    exit_code: observation.exit_code,
                    success: i64::from(observation.success),
                    output_digest: &observation.output_digest,
                    stdout_excerpt: &observation.stdout_excerpt,
                    stderr_excerpt: &observation.stderr_excerpt,
                    created_at: &now,
                },
            )
            .await;
        if let Err(error) = saved {
            return Err(self
                .structured_boundary_error(
                    actor_identity_id,
                    scope,
                    "observe_command",
                    &arguments,
                    AgentHostError::Runtime(match error {
                        db::DbError::Sqlx(error) => error.to_string(),
                        error => error.to_string(),
                    }),
                )
                .await);
        }
        Ok(json!({"observation_id": id, "recorded_at": now}))
    }

    fn public_search_configured(&self) -> bool {
        self.public_search_config()
            .is_some_and(|config| config.endpoint.is_some() && config.validate().is_ok())
    }

    async fn public_search(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        search_scope: PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        let result = self
            .run_public_search(actor_identity_id, scope, search_scope, query, limit)
            .await;
        match result {
            Err(error) => Err(self
                .structured_boundary_error(
                    actor_identity_id,
                    scope,
                    forge_agent_host::FORGE_PUBLIC_WEB_SEARCH_TOOL,
                    &json!({"query": query, "limit": limit}),
                    error,
                )
                .await),
            other => other,
        }
    }

    async fn read(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.read_internal(actor, scope, operation, arguments, None)
            .await
    }
    async fn read_admitted(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
        authority: &operation_registry::authority::EffectiveAuthority,
    ) -> Result<Value, AgentHostError> {
        self.read_internal(actor, scope, operation, arguments, Some(authority))
            .await
    }

    async fn replay_proposal(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: &Value,
    ) -> Result<Option<Value>, AgentHostError> {
        self.try_project_command_replay(actor, scope, operation, arguments)
            .await?
            .map(|result| {
                Self::structured_success(
                    operation,
                    scope,
                    &correlation_id(arguments, operation, scope),
                    result,
                    false,
                )
            })
            .transpose()
    }
    async fn proposal_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        self.proposal_admission(actor_identity_id, scope, operation)
            .await
    }
    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.proposal_boundary(
            actor_identity_id,
            scope,
            runtime_session_id,
            operation,
            arguments,
            false,
            None,
        )
        .await
    }
    async fn propose_prepared(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.proposal_boundary(
            actor_identity_id,
            scope,
            runtime_session_id,
            operation,
            arguments,
            true,
            None,
        )
        .await
    }
    async fn propose_admitted(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
        authority: &operation_registry::authority::EffectiveAuthority,
        prepared: bool,
    ) -> Result<Value, AgentHostError> {
        self.proposal_boundary(
            actor_identity_id,
            scope,
            runtime_session_id,
            operation,
            arguments,
            prepared,
            Some(authority),
        )
        .await
    }
}

fn bounded_untrusted_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn required_argument(arguments: &Value, field: &str) -> Result<String, AgentHostError> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentHostError::Unsupported(format!("{field} is required")))
}

fn validate_proposal_payload(operation: &str, payload: &Value) -> Result<(), AgentHostError> {
    if !payload.is_object() {
        return Err(AgentHostError::Authority(
            "Forge proposal payload must be an object".to_owned(),
        ));
    }
    if operation_registry::PROPOSAL_CATALOG
        .lookup(operation)
        .is_none()
        && serde_json::to_vec(payload)
            .map(|bytes| bytes.len() > 64 * 1024)
            .unwrap_or(true)
    {
        return Err(AgentHostError::Authority(
            "Forge proposal payload is too large".into(),
        ));
    }
    if operation == "session.action" {
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| AgentHostError::Authority("session action is required".to_owned()))?;
        if !matches!(action, "cancel" | "steer") {
            return Err(AgentHostError::Authority(
                "only bounded cancel or steer session actions are admitted".to_owned(),
            ));
        }
        if action == "steer"
            && payload
                .get("content")
                .and_then(Value::as_str)
                .is_none_or(|content| content.chars().count() > 4096)
        {
            return Err(AgentHostError::Authority(
                "session steer content must be at most 4096 characters".to_owned(),
            ));
        }
    }
    if operation_contract(operation).is_some() && contains_authority_override(payload) {
        return Err(AgentHostError::Authority(
            "Forge orchestration scope and authority are server-derived".to_owned(),
        ));
    }
    match operation {
        "project.lifecycle" => {
            let action = payload
                .get("action")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AgentHostError::Authority("Project lifecycle action is required".to_owned())
                })?;
            if !matches!(action, "organize" | "pause" | "resume" | "archive") {
                return Err(AgentHostError::Authority(
                    "Project lifecycle action is not admitted".to_owned(),
                ));
            }
            if payload
                .get("project_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(AgentHostError::Authority(
                    "project_id is required for this lifecycle action".to_owned(),
                ));
            }
        }
        "handoff.publish" => {
            let target = payload
                .get("target_project_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority("target_project_id is required".to_owned())
                })?;
            if target.chars().count() > 200 {
                return Err(AgentHostError::Authority(
                    "handoff target is invalid".to_owned(),
                ));
            }
            let content = payload
                .get("content")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority("handoff content is required".to_owned())
                })?;
            if content.chars().count() > 16_384 {
                return Err(AgentHostError::Authority(
                    "handoff content is too long".to_owned(),
                ));
            }
        }
        "decision.request" => {
            return Err(AgentHostError::Authority(
                "generic decision proposals are not admitted; use the typed Project orchestration contract".to_owned(),
            ));
        }
        "project.release" | "project.milestone.release" => {
            return Err(AgentHostError::Authority(
                "final release is user-only; Project Agent may submit only a typed release candidate request".to_owned(),
            ));
        }
        _ => {}
    }
    if matches!(operation, "project.lifecycle" | "handoff.publish") {
        // The provider persists only guarded action envelopes.  This catches
        // credential-shaped model output before it reaches the action ledger,
        // while retaining the actual content in the protected runtime only.
        let serialized = serde_json::to_string(payload).map_err(|_| {
            AgentHostError::Authority("proposal payload is not serializable".to_owned())
        })?;
        guard_agent_chat_content(&serialized).map_err(|_| {
            AgentHostError::Authority("protected values cannot be proposed".to_owned())
        })?;
    }
    Ok(())
}

fn scope_type_name(scope_type: CanonicalScopeType) -> &'static str {
    match scope_type {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

fn workspace_access_name(access: WorkspaceAccess) -> &'static str {
    match access {
        WorkspaceAccess::Deny => "deny",
        WorkspaceAccess::TaskRead => "task_read",
        WorkspaceAccess::TaskWrite => "task_write",
        WorkspaceAccess::ProjectVerify => "project_verify",
        WorkspaceAccess::AccountScratch => "account_scratch",
    }
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

/// A captured artifact is evidence, not a transfer channel: keep it small
/// enough that a runaway capture cannot fill the media store.
#[derive(Clone, Copy)]
enum EvidenceAuthor<'a> {
    Agent(&'a str),
    Verify,
}
impl<'a> EvidenceAuthor<'a> {
    fn kind(self) -> db::CommentAuthorType {
        match self {
            Self::Agent(_) => db::CommentAuthorType::Agent,
            Self::Verify => db::CommentAuthorType::System,
        }
    }
    fn id(self) -> Option<&'a str> {
        match self {
            Self::Agent(id) => Some(id),
            Self::Verify => None,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Agent(_) => "Agent",
            Self::Verify => "Forge Verify",
        }
    }
}

struct StoredEvidence {
    media_id: String,
    asset_id: String,
    checksum: String,
    byte_size: i64,
}

/// At most this many bytes of one outbox file are read.
const MAX_OUTBOX_FILE_BYTES: u64 = 1024 * 1024;
/// At most this many entries of one outbox file are ingested.
const MAX_OUTBOX_ENTRIES: usize = 200;

const MAX_CAPTURED_EVIDENCE_BYTES: i64 = 25 * 1024 * 1024;

/// A worklog entry is a summary the next role reads, not a transcript.
const MAX_WORKLOG_SUMMARY_CHARS: usize = 4_000;

/// Inline only what an Agent can read and act on; anything larger is named
/// rather than pasted into a turn.
const MAX_INLINE_ARTIFACT_BYTES: i64 = 256 * 1024;
const MAX_INLINE_ARTIFACT_CHARS: usize = 8_000;

mod commands;
mod evidence;
mod inquiry;
mod outbox;
mod outcomes;
mod project_reads;
mod public_search;
mod read_projections;

use evidence::*;
#[cfg(test)]
use outbox::*;
use outcomes::*;

#[cfg(test)]
mod tests;
