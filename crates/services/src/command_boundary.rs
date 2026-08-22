use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use api_types::{
    ApprovalTarget, CanonicalScopeRef, CurrentVersionOrRevision, OrchestrationOutcome, OutcomeCode,
    OutcomeScopeType, OutcomeStatus, RetryAction, RetryInstruction, SetupRequirement,
};

use crate::ServiceError;

pub use forge_agent_host::OperationClassification;

pub const COMMAND_INPUT_DIGEST_SCHEMA: &str = "forge.command-input/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandPrincipal {
    pub(crate) principal_type: String,
    pub(crate) principal_id: String,
}

impl CommandPrincipal {
    pub fn principal_type(&self) -> &str {
        &self.principal_type
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandScopeType {
    Account,
    Project,
    AgentChat,
    Task,
}

impl CommandScopeType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Project => "project",
            Self::AgentChat => "agent_chat",
            Self::Task => "task",
        }
    }
}

impl From<forge_agent_host::CanonicalScopeType> for CommandScopeType {
    fn from(scope_type: forge_agent_host::CanonicalScopeType) -> Self {
        match scope_type {
            forge_agent_host::CanonicalScopeType::Account => Self::Account,
            forge_agent_host::CanonicalScopeType::Project => Self::Project,
            forge_agent_host::CanonicalScopeType::AgentChat => Self::AgentChat,
            forge_agent_host::CanonicalScopeType::Task => Self::Task,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandScope {
    pub(crate) scope_type: CommandScopeType,
    pub(crate) scope_id: String,
}

impl From<&forge_agent_host::CanonicalScope> for CommandScope {
    fn from(scope: &forge_agent_host::CanonicalScope) -> Self {
        Self {
            scope_type: scope.scope_type.into(),
            scope_id: scope.scope_id.clone(),
        }
    }
}

impl CommandScope {
    pub fn scope_type(&self) -> CommandScopeType {
        self.scope_type
    }

    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOperationMetadata {
    pub operation: String,
    pub classification: OperationClassification,
    pub required_permission: Option<String>,
    pub allowed_scopes: Vec<CommandScopeType>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ExpectedCommandState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub versions: BTreeMap<String, i64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub digests: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthorizationProvenance {
    pub(crate) policy_result: String,
    pub(crate) policy_revision: Option<String>,
    pub(crate) policy_digest: Option<String>,
    pub(crate) requested_permission: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentActionProvenance {
    pub(crate) action_id: String,
    pub(crate) expected_action_version: i64,
    pub(crate) attempt: i64,
    pub(crate) execution_idempotency_key: String,
    pub(crate) executed_by_type: String,
    pub(crate) executed_by_id: String,
}

impl AgentActionProvenance {
    #[must_use]
    pub fn new(
        action_id: String,
        expected_action_version: i64,
        attempt: i64,
        execution_idempotency_key: String,
        executed_by_type: String,
        executed_by_id: String,
    ) -> Self {
        Self {
            action_id,
            expected_action_version,
            attempt,
            execution_idempotency_key,
            executed_by_type,
            executed_by_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandContext {
    pub(crate) principal: CommandPrincipal,
    pub(crate) canonical_scope: CommandScope,
    pub(crate) operation: String,
    pub(crate) idempotency_key: String,
    pub(crate) input_digest: String,
    #[serde(default)]
    pub(crate) expected_state: ExpectedCommandState,
    pub(crate) authorization_provenance: Option<AuthorizationProvenance>,
    pub(crate) action_provenance: Option<AgentActionProvenance>,
    pub(crate) correlation_id: String,
    pub(crate) causation_id: Option<String>,
    pub(crate) causation_depth: i64,
}

#[derive(Serialize)]
struct CommandDigestEnvelope<'a, T> {
    principal: &'a CommandPrincipal,
    canonical_scope: &'a CommandScope,
    operation: &'a str,
    expected_state: &'a ExpectedCommandState,
    authorization_provenance: &'a Option<AuthorizationProvenance>,
    action_provenance: &'a Option<AgentActionProvenance>,
    correlation_id: &'a str,
    causation_id: &'a Option<String>,
    causation_depth: i64,
    input: &'a T,
}

#[derive(Debug, Clone)]
pub(crate) struct NewCommandContext {
    pub principal: CommandPrincipal,
    pub canonical_scope: CommandScope,
    pub operation: String,
    pub idempotency_key: String,
    pub expected_state: ExpectedCommandState,
    pub authorization_provenance: Option<AuthorizationProvenance>,
    pub action_provenance: Option<AgentActionProvenance>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
}

impl CommandContext {
    pub(crate) fn from_authorized_input<T: Serialize>(
        context: NewCommandContext,
        input: &T,
    ) -> std::result::Result<Self, serde_json::Error> {
        let NewCommandContext {
            principal,
            canonical_scope,
            operation,
            idempotency_key,
            expected_state,
            authorization_provenance,
            action_provenance,
            correlation_id,
            causation_id,
            causation_depth,
        } = context;
        let input_digest = api_types::canonical_digest_with_schema(
            COMMAND_INPUT_DIGEST_SCHEMA,
            &CommandDigestEnvelope {
                principal: &principal,
                canonical_scope: &canonical_scope,
                operation: &operation,
                expected_state: &expected_state,
                authorization_provenance: &authorization_provenance,
                action_provenance: &action_provenance,
                correlation_id: &correlation_id,
                causation_id: &causation_id,
                causation_depth: causation_depth.max(0),
                input,
            },
        )?;
        Ok(Self {
            principal,
            canonical_scope,
            operation,
            idempotency_key,
            input_digest,
            expected_state,
            authorization_provenance,
            action_provenance,
            correlation_id,
            causation_id,
            causation_depth: causation_depth.max(0),
        })
    }

    pub fn principal(&self) -> &CommandPrincipal {
        &self.principal
    }

    pub fn canonical_scope(&self) -> &CommandScope {
        &self.canonical_scope
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn input_digest(&self) -> &str {
        &self.input_digest
    }

    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
}

/// Context required to render a safe, transport-neutral outcome.  The
/// operation, scope, and correlation identity are server-derived and are
/// therefore never accepted from a free-form error string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutcomeContext {
    pub operation: String,
    pub scope: CanonicalScopeRef,
    pub correlation_id: String,
}

impl CommandOutcomeContext {
    #[must_use]
    pub fn new(
        operation: impl Into<String>,
        scope: impl Into<CanonicalScopeRef>,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            operation: operation.into(),
            scope: scope.into(),
            correlation_id: correlation_id.into(),
        }
    }

    #[must_use]
    pub fn from_command_context(context: &CommandContext) -> Self {
        Self::new(
            context.operation().to_owned(),
            CanonicalScopeRef::from(&context.canonical_scope),
            context.correlation_id().to_owned(),
        )
    }
}

impl From<&CommandScope> for CanonicalScopeRef {
    fn from(scope: &CommandScope) -> Self {
        let scope_type = match scope.scope_type {
            CommandScopeType::Account => OutcomeScopeType::Account,
            CommandScopeType::Project => OutcomeScopeType::Project,
            CommandScopeType::AgentChat => OutcomeScopeType::AgentChat,
            CommandScopeType::Task => OutcomeScopeType::Task,
        };
        Self::new(scope_type, scope.scope_id.clone())
    }
}

impl From<CommandScope> for CanonicalScopeRef {
    fn from(scope: CommandScope) -> Self {
        Self::from(&scope)
    }
}

/// Render a service error with only a stable code and bounded, model-safe
/// text.  Current state and command-specific retry arguments must be supplied
/// separately by the authorized command path.
#[must_use]
pub fn outcome_for_service_error(
    error: &ServiceError,
    context: &CommandOutcomeContext,
) -> OrchestrationOutcome {
    outcome_for_service_error_with_correction(error, context, None, None)
}

/// Render a service error and attach corrective state only when the caller has
/// already loaded it under the same authorization/scope check.  Idempotency,
/// policy, not-found, and internal failures intentionally discard correction
/// data to avoid turning an error mapper into a cross-scope state oracle.
#[must_use]
pub fn outcome_for_service_error_with_correction(
    error: &ServiceError,
    context: &CommandOutcomeContext,
    current: Option<CurrentVersionOrRevision>,
    retry: Option<RetryInstruction>,
) -> OrchestrationOutcome {
    let (code, safe_message, default_retry, setup_requirements) = match error {
        ServiceError::Db(db::DbError::IdempotencyConflict) => (
            OutcomeCode::IdempotencyConflict,
            "the idempotency key is already bound to different command input",
            Some(RetryInstruction::new(
                RetryAction::UseNewIdempotencyKey,
                false,
            )),
            None,
        ),
        ServiceError::Db(
            db::DbError::VersionConflict
            | db::DbError::TaskVersionConflict { .. }
            | db::DbError::BoardRevisionConflict { .. },
        ) => (
            OutcomeCode::VersionConflict,
            "the command targeted stale authorized state; refresh and retry",
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
            None,
        ),
        ServiceError::Db(db::DbError::NotFound) | ServiceError::NotFound { .. } => (
            OutcomeCode::NotFound,
            "the requested resource was not found",
            None,
            None,
        ),
        ServiceError::ProductGenesisActiveSession { .. } => (
            OutcomeCode::ActiveSessionConflict,
            "a Product Genesis discovery session is already active",
            None,
            None,
        ),
        ServiceError::AuthorizationDenied { .. }
        | ServiceError::GuardRejection { .. }
        | ServiceError::AgentPaused { .. }
        | ServiceError::ProjectPaused { .. }
        | ServiceError::TerminalAttachTokenInvalid
        | ServiceError::TerminalPathGuardrail => (
            OutcomeCode::PolicyDenied,
            "the command is not permitted in the current scope",
            None,
            None,
        ),
        ServiceError::ExecutionSetupRequired { requirements, .. } => (
            OutcomeCode::SetupRequired,
            "additional Project execution setup is required before this command can run",
            Some(RetryInstruction::new(RetryAction::CompleteSetup, true)),
            Some(requirements.clone()),
        ),
        ServiceError::DependencyGate
        | ServiceError::MissingPrimaryRepo { .. }
        | ServiceError::RepoMismatch { .. }
        | ServiceError::PrProviderMissing { .. }
        | ServiceError::PrProviderTokenMissing { .. }
        | ServiceError::ParentWorkspaceRequired { .. }
        | ServiceError::TerminalDisabled
        | ServiceError::TerminalWorkspaceNotReady => (
            OutcomeCode::SetupRequired,
            "additional setup is required before this command can run",
            Some(RetryInstruction::new(RetryAction::CompleteSetup, true)),
            Some(vec![setup_requirement_for(error)]),
        ),
        ServiceError::RateLimited {
            retry_after_seconds,
        } => {
            let mut retry = RetryInstruction::new(RetryAction::RetryAfter, true);
            retry.after_seconds = Some(*retry_after_seconds);
            (
                OutcomeCode::TransientFailure,
                "the command is temporarily rate limited",
                Some(retry),
                None,
            )
        }
        ServiceError::DaemonUnavailable { .. }
        | ServiceError::DaemonTimeout { .. }
        | ServiceError::TerminalDaemonUnavailable { .. }
        | ServiceError::TerminalActiveExecution { .. }
        | ServiceError::PrSyncFailure { .. } => (
            OutcomeCode::TransientFailure,
            "the command could not complete right now; retry later",
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
            None,
        ),
        ServiceError::InvalidOperation { .. }
        | ServiceError::TerminalInvalidInput { .. }
        | ServiceError::TaskActionUnavailable { .. }
        | ServiceError::Conflict(_)
        | ServiceError::NestedSubtaskUnsupported
        | ServiceError::SubtaskAssigneeUnsupported { .. }
        | ServiceError::SubtaskSequenceStarted { .. }
        | ServiceError::SubtaskManagedByRoot { .. }
        | ServiceError::WorkspaceResetRequired { .. }
        | ServiceError::TaskSequenceAlreadyStarted { .. }
        | ServiceError::TerminalSessionLimit { .. }
        | ServiceError::TerminalNotFound => (
            OutcomeCode::ValidationError,
            "the command input or current state is invalid",
            None,
            None,
        ),
        ServiceError::Db(_)
        | ServiceError::Git(_)
        | ServiceError::Review(_)
        | ServiceError::Domain(_) => (
            OutcomeCode::InternalFailure,
            "the command could not be completed; contact support with the correlation id",
            None,
            None,
        ),
    };

    let mut outcome = OrchestrationOutcome::new(
        code,
        OrchestrationOutcome::status_for_code(code),
        context.operation.clone(),
        context.scope.clone(),
        context.correlation_id.clone(),
    );
    outcome.safe_message = safe_message.to_owned();
    outcome.retry = default_retry;
    outcome.setup_requirements = setup_requirements;

    let allows_correction = matches!(
        code,
        OutcomeCode::VersionConflict | OutcomeCode::DigestConflict | OutcomeCode::SetupRequired
    );
    if allows_correction {
        outcome.current_version_or_revision = current;
        if retry.is_some() {
            outcome.retry = retry;
        }
    }
    outcome
}

fn setup_requirement_for(error: &ServiceError) -> SetupRequirement {
    let mut requirement = match error {
        ServiceError::MissingPrimaryRepo { .. } => SetupRequirement::new("primary_repository"),
        ServiceError::RepoMismatch { .. } => SetupRequirement::new("repository_link"),
        ServiceError::PrProviderMissing { .. } => SetupRequirement::new("pull_request_provider"),
        ServiceError::PrProviderTokenMissing { .. } => {
            SetupRequirement::new("pull_request_provider_token")
        }
        ServiceError::ParentWorkspaceRequired { .. } => SetupRequirement::new("parent_workspace"),
        ServiceError::TerminalDisabled => SetupRequirement::new("terminal_enabled"),
        ServiceError::TerminalWorkspaceNotReady => SetupRequirement::new("terminal_workspace"),
        _ => SetupRequirement::new("execution_setup"),
    };
    requirement.action = Some(RetryAction::CompleteSetup);
    requirement
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome_context() -> CommandOutcomeContext {
        CommandOutcomeContext::new(
            "project.document.save",
            CanonicalScopeRef::new(OutcomeScopeType::Project, "project-1"),
            "correlation-1",
        )
    }

    #[test]
    fn conflict_is_redacted_and_classified_as_validation() {
        let error = ServiceError::Conflict("reload revision 9 and retry with version 4".to_owned());
        let outcome = outcome_for_service_error(&error, &outcome_context());
        assert_eq!(outcome.code, OutcomeCode::ValidationError);
        assert_eq!(outcome.status, OutcomeStatus::Failed);
        assert_eq!(
            outcome.safe_message,
            "the command input or current state is invalid"
        );
        assert!(!outcome.safe_message.contains("revision 9"));
        assert!(outcome.current_version_or_revision.is_none());
    }

    #[test]
    fn database_conflicts_have_stable_codes_and_safe_corrections() {
        let current = CurrentVersionOrRevision {
            resource_type: "document".to_owned(),
            resource_id: "document-1".to_owned(),
            version: Some(7),
            revision_id: Some("revision-7".to_owned()),
            revision: Some(7),
            content_digest: Some("content-digest".to_owned()),
            rendered_digest: Some("rendered-digest".to_owned()),
        };
        let version = outcome_for_service_error_with_correction(
            &ServiceError::Db(db::DbError::VersionConflict),
            &outcome_context(),
            Some(current.clone()),
            None,
        );
        let idempotency = outcome_for_service_error_with_correction(
            &ServiceError::Db(db::DbError::IdempotencyConflict),
            &outcome_context(),
            Some(current),
            None,
        );
        assert_eq!(version.code.as_str(), "version_conflict");
        assert!(version.retry.as_ref().is_some_and(|retry| retry.retryable));
        assert_eq!(
            version
                .current_version_or_revision
                .expect("current")
                .version,
            Some(7)
        );
        assert_eq!(idempotency.code.as_str(), "idempotency_conflict");
        assert!(idempotency.current_version_or_revision.is_none());
        assert!(idempotency
            .retry
            .as_ref()
            .is_some_and(|retry| !retry.retryable));
    }

    #[test]
    fn unknown_failures_are_redacted_and_correlation_bound() {
        let outcome = outcome_for_service_error(
            &ServiceError::Domain("secret database topology".to_owned()),
            &outcome_context(),
        );
        assert_eq!(outcome.code, OutcomeCode::InternalFailure);
        assert_eq!(outcome.correlation_id, "correlation-1");
        assert!(!outcome.safe_message.contains("secret"));
    }

    #[test]
    fn canonical_scope_serializes_to_the_receipt_vocabulary() {
        let scope = CommandScope {
            scope_type: CommandScopeType::AgentChat,
            scope_id: "chat-1".to_owned(),
        };
        assert_eq!(scope.scope_type.as_str(), "agent_chat");
        assert_eq!(
            serde_json::to_value(scope).expect("scope serializes"),
            serde_json::json!({"scope_type": "agent_chat", "scope_id": "chat-1"})
        );
    }

    #[test]
    fn digest_is_server_computed_and_binds_expected_state_and_principal() {
        let base = NewCommandContext {
            principal: CommandPrincipal {
                principal_type: "user".to_owned(),
                principal_id: "user-1".to_owned(),
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Project,
                scope_id: "project-1".to_owned(),
            },
            operation: "project.document.save".to_owned(),
            idempotency_key: "key-1".to_owned(),
            expected_state: ExpectedCommandState::default(),
            authorization_provenance: Some(AuthorizationProvenance {
                policy_result: "allowed".to_owned(),
                policy_revision: Some("policy-1".to_owned()),
                policy_digest: Some("digest-1".to_owned()),
                requested_permission: Some("propose_project".to_owned()),
            }),
            action_provenance: None,
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
        };
        let original = CommandContext::from_authorized_input(
            base.clone(),
            &serde_json::json!({
                "document_id": "document-1"
            }),
        )
        .expect("digest computes");
        let mut changed_state = base.clone();
        changed_state
            .expected_state
            .versions
            .insert("document".to_owned(), 2);
        let changed_state = CommandContext::from_authorized_input(
            changed_state,
            &serde_json::json!({"document_id": "document-1"}),
        )
        .expect("digest computes");
        let mut changed_principal = base.clone();
        changed_principal.principal.principal_id = "user-2".to_owned();
        let changed_principal = CommandContext::from_authorized_input(
            changed_principal,
            &serde_json::json!({"document_id": "document-1"}),
        )
        .expect("digest computes");
        let mut changed_permission = base.clone();
        changed_permission
            .authorization_provenance
            .as_mut()
            .expect("authorization provenance")
            .requested_permission = Some("read_project".to_owned());
        let changed_permission = CommandContext::from_authorized_input(
            changed_permission,
            &serde_json::json!({"document_id": "document-1"}),
        )
        .expect("digest computes");
        let mut changed_causation = base;
        changed_causation.correlation_id = "correlation-2".to_owned();
        changed_causation.causation_id = Some("event-2".to_owned());
        changed_causation.causation_depth = 3;
        let changed_causation = CommandContext::from_authorized_input(
            changed_causation,
            &serde_json::json!({"document_id": "document-1"}),
        )
        .expect("digest computes");
        assert_ne!(original.input_digest(), changed_state.input_digest());
        assert_ne!(original.input_digest(), changed_principal.input_digest());
        assert_ne!(original.input_digest(), changed_permission.input_digest());
        assert_ne!(original.input_digest(), changed_causation.input_digest());
    }
}
