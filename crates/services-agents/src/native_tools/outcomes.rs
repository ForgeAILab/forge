//! Native command result envelopes, denial causes, and error mapping.

use super::*;

pub(super) fn action_value(action: &AgentAction) -> Value {
    json!({
        "id": action.id,
        "operation": action.operation,
        "scope_type": action.scope_type,
        "scope_id": action.scope_id,
        "requested_permission": action.requested_permission,
        "policy_result": action.policy_result.to_string(),
        "status": action.status.to_string(),
        "target_type": action.target_type,
        "target_id": action.target_id,
        "version": action.version,
    })
}

/// Deserialize a model-authored command payload, reporting a schema mismatch
/// as a model-facing validation outcome.  The model authored this input, so
/// the offending field path and the expected shape are safe to hand back —
/// and without them the `CorrectInput` retry instruction names no correction,
/// which leaves a wrong payload unfixable and invites the model to narrate a
/// success it never got.
pub(super) fn direct_project_command_value(
    result: crate::project_agent_actions::DirectProjectCommandResult,
) -> Value {
    let requires_user_authorization = result
        .result
        .get("requires_user_authorization")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    json!({"operation":result.operation,"status":"succeeded","replayed":result.replayed,"materialized":true,"domain_committed":true,"receipt_id":result.receipt_id,"event_id":result.event_id,"domain_result":result.result,"agent_action_execution_id":result.agent_action_execution_id,"requires_user_authorization":requires_user_authorization})
}

pub(super) fn typed_command_payload<T: serde::de::DeserializeOwned>(
    operation: &str,
    scope: &CanonicalScope,
    correlation_id: &str,
    payload: Value,
) -> Result<T, AgentHostError> {
    serde_path_to_error::deserialize(payload).map_err(|error| {
        let path = error.path().to_string();
        let detail = if path.is_empty() {
            error.inner().to_string()
        } else {
            format!("{path}: {}", error.inner())
        };
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::ValidationError,
            operation,
            outcome_scope(scope),
            correlation_id,
            format!("the payload does not match the {operation} schema ({detail})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        AgentHostError::StructuredOutcome(Box::new(outcome))
    })
}

pub(super) fn outcome_scope(scope: &CanonicalScope) -> OutcomeScopeRef {
    let scope_type = match scope.scope_type {
        CanonicalScopeType::Account => OutcomeScopeType::Account,
        CanonicalScopeType::Project => OutcomeScopeType::Project,
        CanonicalScopeType::AgentChat => OutcomeScopeType::AgentChat,
        CanonicalScopeType::Task => OutcomeScopeType::Task,
    };
    OutcomeScopeRef::new(scope_type, scope.scope_id.clone())
}

pub(super) fn correlation_id(arguments: &Value, operation: &str, scope: &CanonicalScope) -> String {
    let supplied = arguments
        .get("correlation_id")
        .and_then(Value::as_str)
        .or_else(|| {
            arguments
                .get("payload")
                .and_then(|payload| payload.get("correlation_id"))
                .and_then(Value::as_str)
        });
    if let Some(value) = supplied
        .filter(|value| !value.trim().is_empty())
        .filter(|value| value.chars().count() <= 256)
        .filter(|value| !value.chars().any(char::is_control))
    {
        return value.to_owned();
    }
    // Read operations do not accept a model correlation id.  Mint a fresh
    // server-side join key rather than deriving one from model-controlled
    // operation text or a scope label.
    let _ = (operation, scope);
    Uuid::new_v4().to_string()
}

fn result_string(result: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| result.get(*name).and_then(Value::as_str).map(str::to_owned))
}

fn result_i64(result: &Value, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| result.get(*name).and_then(Value::as_i64))
}

pub(super) fn approval_target(
    operation: &str,
    scope: OutcomeScopeRef,
    result: &Value,
) -> ApprovalTarget {
    let nested_target = result
        .get("domain_result")
        .and_then(|domain_result| domain_result.get("approval_target"));
    let target_value = nested_target.unwrap_or(result);
    let target_type = result_string(target_value, &["target_type", "scope_type"])
        .or_else(|| {
            target_value
                .get("baseline_id")
                .map(|_| "execution_baseline".to_owned())
        })
        .unwrap_or_else(|| scope.scope_type.as_str().to_owned());
    let target_id = result_string(
        target_value,
        &["target_id", "baseline_id", "project_id", "id"],
    )
    .unwrap_or_else(|| scope.scope_id.clone());
    let mut target = ApprovalTarget::new(target_type, target_id);
    target.operation = Some(operation.to_owned());
    target.version = result_i64(
        target_value,
        &[
            "version",
            "baseline_version",
            "project_version",
            "charter_version",
            "document_version",
            "milestone_version",
        ],
    )
    .or_else(|| {
        result.get("domain_result").and_then(|domain_result| {
            result_i64(
                domain_result,
                &[
                    "version",
                    "baseline_version",
                    "project_version",
                    "charter_version",
                    "document_version",
                    "milestone_version",
                ],
            )
        })
    });
    target.revision_id = result_string(
        nested_target.unwrap_or(result),
        &["revision_id", "current_revision_id"],
    );
    target.revision = result_i64(nested_target.unwrap_or(result), &["revision"]);
    target.content_digest = result_string(nested_target.unwrap_or(result), &["content_digest"]);
    target.rendered_digest = result_string(
        nested_target.unwrap_or(result),
        &["render_digest", "rendered_digest"],
    );
    target.requires_user_authorization = true;
    target
}

pub(super) fn retry_for_current(
    operation: &str,
    current: &CurrentVersionOrRevision,
) -> RetryInstruction {
    let mut retry = RetryInstruction::new(RetryAction::RefreshAndRetry, true);
    if let Some(version) = current.version {
        let field = match operation {
            _ if current.resource_type == "project" => "expected_project_version",
            TASK_ADAPTIVE_OPERATION => "expected_task_version",
            TASK_ACTION_OPERATION => "version",
            PROJECT_DOCUMENT_OPERATION => "expected_document_version",
            PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_VALIDATION_OPERATION => "expected_milestone_version",
            PROJECT_READINESS_OPERATION | PROJECT_RELEASE_OPERATION => "milestone_version",
            _ => "expected_version",
        };
        retry.arguments.insert(field.to_owned(), json!(version));
    }
    if let Some(revision) = current.revision {
        if matches!(operation, TASK_ADAPTIVE_OPERATION) {
            retry
                .arguments
                .insert("expected_board_revision".to_owned(), json!(revision));
        }
    }
    if let Some(revision_id) = current.revision_id.as_deref() {
        if matches!(operation, PROJECT_DOCUMENT_OPERATION) {
            retry
                .arguments
                .insert("base_revision_id".to_owned(), json!(revision_id));
        }
    }
    if let Some(content_digest) = current.content_digest.as_deref() {
        retry
            .arguments
            .insert("content_digest".to_owned(), json!(content_digest));
    }
    if let Some(rendered_digest) = current.rendered_digest.as_deref() {
        retry
            .arguments
            .insert("render_digest".to_owned(), json!(rendered_digest));
    }
    retry
}

/// Translate only known server-authored reasons about the caller's own authority.
/// Unknown reasons may describe another scope and retain the generic redaction.
pub(super) fn native_denial_cause(reason: &str, permission: Option<&str>) -> DeniedBy {
    if let Ok(cause) = reason.parse::<DeniedBy>() {
        return match cause {
            DeniedBy::PermissionMissing(name) if name == "unknown" => permission
                .map(|name| DeniedBy::PermissionMissing(name.to_owned()))
                .unwrap_or(DeniedBy::Unspecified),
            cause => cause,
        };
    }
    if let Some(name) = reason
        .strip_prefix("permission ")
        .and_then(|rest| {
            rest.strip_suffix(" is outside the server-issued identity/profile/scope ceiling")
        })
        .filter(|name| {
            !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
        })
    {
        return DeniedBy::PermissionMissing(name.to_owned());
    }
    match reason {
        "requested permission is outside the Project binding ceiling"
        | "requested permission is outside the Agent Chat binding ceiling"
        | "direct Project command permission is outside the account ceiling"
        | "direct Project command permission is outside the selected profile ceiling"
        | "direct Project command permission is outside the active binding ceiling" => permission
            .map(|name| DeniedBy::PermissionMissing(name.to_owned()))
            .unwrap_or(DeniedBy::Unspecified),
        "actor identity has no active binding in the requested scope or is paused or archived" => {
            DeniedBy::AuthorityRevoked
        }
        "actor identity is paused or archived"
        | "Project Agent is paused or archived"
        | "direct Project command principal is paused or archived"
        | "agent principal is paused or archived" => DeniedBy::IdentityPaused,
        "direct Project command has no selected active profile" => DeniedBy::ProfileNotSelected,
        "the bound Project has no approved Charter"
        | "Project orchestration remains blocked until a user-approved Charter adoption is committed"
        | "direct Project command is blocked by current Charter state" => {
            DeniedBy::CharterNotAdopted
        }
        "the Task workflow no longer admits writes in its terminal state" => DeniedBy::TaskTerminal,
        "reviewer assignments cannot perform Task writes" => DeniedBy::ReviewerReadOnly,
        "protected mutation requires an independent approval" => {
            DeniedBy::IndependentApprovalRequired
        }
        "genesis.start requires an explicit user request to create or start a new Project" => {
            DeniedBy::UserRequestRequired
        }
        "genesis.start requires the currently leased Main Chat turn" => {
            DeniedBy::LeasedTurnRequired
        }
        "Project Charter adoption is not valid for the current Project state" => {
            DeniedBy::CharterAdoptionNotApplicable
        }
        "query operations execute through the read boundary" => DeniedBy::ReadBoundaryRequired,
        "direct command is not admitted for this permission or payload" => {
            DeniedBy::DirectCommandNotAdmitted
        }
        "the Task assignment does not admit review proposals" => DeniedBy::ReviewAssignmentRequired,
        "direct Project commands require the propose_project permission" => {
            DeniedBy::PermissionMissing("propose_project".to_owned())
        }
        "Product Genesis start requires propose_discovery"
        | "Main Charter draft requires the propose_discovery permission" => {
            DeniedBy::PermissionMissing("propose_discovery".to_owned())
        }
        "operation is denied by the canonical native operation catalog" => {
            DeniedBy::OperationNotInScope
        }
        _ => DeniedBy::Unspecified,
    }
}

pub(super) fn native_scope_error(error: crate::ServiceError) -> AgentHostError {
    match error {
        crate::ServiceError::AuthorizationDenied { message }
        | crate::ServiceError::InvalidOperation { message } => AgentHostError::Authority(message),
        crate::ServiceError::NotFound { .. } | crate::ServiceError::Db(db::DbError::NotFound) => {
            AgentHostError::Authority("the requested Forge scope is unavailable".to_owned())
        }
        crate::ServiceError::Db(_) => AgentHostError::ProtectedPersistence,
        _ => AgentHostError::ProtectedPersistence,
    }
}

/// An argument-shape rejection is the model's only path to a corrected call,
/// so it travels back verbatim as a structured validation outcome with a
/// `correct_input` retry. Raising it as a bare `Runtime` error renders as
/// "the Forge operation could not complete" — an internal failure the model
/// cannot act on, so it retries the same rejected shape or gives up on a
/// repair its doctrine requires. Only server-authored messages naming the
/// offending field belong here; never echo caller-supplied values.
pub(super) fn invalid_arguments(message: String) -> AgentHostError {
    let mut outcome = OrchestrationOutcome::failed(
        OutcomeCode::ValidationError,
        "unknown",
        OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
        "",
        format!("the operation or arguments are not valid for this Forge surface ({message})"),
    );
    outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
    AgentHostError::StructuredOutcome(Box::new(outcome))
}

pub(super) fn service_error(error: crate::ServiceError) -> AgentHostError {
    if let crate::ServiceError::TurnFailure { error, .. } = error {
        return service_error(*error);
    }
    if let crate::ServiceError::TaskActionUnavailable {
        available_actions,
        reason,
        wait_cause,
    } = &error
    {
        let mut outcome = if let Some(cause) = wait_cause {
            OrchestrationOutcome::terminal_denial(
                "task.action",
                OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
                "",
                cause.clone(),
            )
        } else {
            OrchestrationOutcome::failed(
                OutcomeCode::ActionUnavailable,
                "task.action",
                OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
                "",
                reason.clone(),
            )
        };
        outcome.details = Some(json!({ "available_actions": available_actions }));
        if wait_cause.is_none() {
            outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        }
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    let target_refusal = match &error {
        crate::ServiceError::PlacementUnavailable(error) if error.needs_daemon_upgrade() => {
            Some(DeniedBy::DaemonUpgradeRequired)
        }
        crate::ServiceError::PlacementUnavailable(_) => Some(DeniedBy::PlacementUnavailable),
        crate::ServiceError::DaemonUpgradeRequired { .. } => Some(DeniedBy::DaemonUpgradeRequired),
        crate::ServiceError::WorkspaceResetRequired { .. } => {
            Some(DeniedBy::WorkspaceResetRequired)
        }
        _ => None,
    };
    if let Some(cause) = target_refusal {
        return AgentHostError::StructuredOutcome(Box::new(OrchestrationOutcome::terminal_denial(
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            cause,
        )));
    }
    match &error {
        crate::ServiceError::AuthorizationDenied { message } => {
            return AgentHostError::Authority(message.clone());
        }
        crate::ServiceError::AgentPaused { agent_id }
        | crate::ServiceError::Db(db::DbError::AgentPaused { agent_id }) => {
            return AgentHostError::AgentPaused {
                agent_id: agent_id.clone(),
            };
        }
        crate::ServiceError::ProjectPaused { project_id }
        | crate::ServiceError::Db(db::DbError::ProjectPaused { project_id }) => {
            return AgentHostError::ProjectPaused {
                project_id: project_id.clone(),
            };
        }
        crate::ServiceError::Db(db::DbError::Check(message))
            if native_denial_cause(message, Some("propose_project")) != DeniedBy::Unspecified =>
        {
            return AgentHostError::Authority(message.clone());
        }
        _ => {}
    }
    // A validation reason from the command boundary names the offending
    // input, so it returns to the model verbatim. Collapsing it to a bare
    // "not valid" leaves the model retrying the same rejected shape with
    // nothing to correct.
    if let crate::ServiceError::InvalidOperation { message }
    | crate::ServiceError::TerminalInvalidInput { message } = &error
    {
        return invalid_arguments(message.clone());
    }
    // A conflict's prose is the only account of what the caller got wrong, so
    // it is shown. It stays unstructured on purpose: read it, never parse it
    // to infer version or digest semantics.
    if let crate::ServiceError::Conflict(message) = &error {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::ValidationError,
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            format!("the command could not be accepted; correct the typed input ({message})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    // A `not_found` that names nothing is uncorrectable: a caller that passed
    // several ids in one payload cannot tell which one Forge could not resolve,
    // and retries the same rejected shape. The entity is a server-authored
    // static name and the id is the one the caller just supplied, so neither
    // discloses anything the caller did not already hold — the HTTP surface
    // returns exactly this pair for the same failure.
    if let crate::ServiceError::NotFound { entity, id } = &error {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::NotFound,
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            format!("the requested Forge resource is unavailable ({entity} {id})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    let (code, safe_message, setup_requirement, retry) = match error {
        crate::ServiceError::NotFound { .. } | crate::ServiceError::Db(db::DbError::NotFound) => (
            OutcomeCode::NotFound,
            "the requested Forge resource is unavailable",
            None,
            None,
        ),
        crate::ServiceError::Db(db::DbError::TurnNotRetryable | db::DbError::ChatTurnLive) => (
            OutcomeCode::ValidationError,
            "Agent Chat turn cannot be retried in its current state",
            None,
            None,
        ),
        crate::ServiceError::Db(db::DbError::IdempotencyConflict) => (
            OutcomeCode::IdempotencyConflict,
            "the idempotency key is already bound to a different command",
            None,
            Some(RetryInstruction::new(
                RetryAction::UseNewIdempotencyKey,
                false,
            )),
        ),
        crate::ServiceError::Db(
            db::DbError::VersionConflict
            | db::DbError::TaskVersionConflict { .. }
            | db::DbError::BoardRevisionConflict { .. }
            | db::DbError::MoveOperationConflict { .. },
        ) => (
            OutcomeCode::VersionConflict,
            "the authorized resource changed; refresh current state and retry",
            None,
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
        ),
        // ServiceError::Conflict intentionally has no structured discriminator;
        // never parse its prose to guess version or digest semantics.
        crate::ServiceError::Conflict(_) => (
            OutcomeCode::ValidationError,
            "the command could not be accepted; correct the typed input",
            None,
            None,
        ),
        crate::ServiceError::ProductGenesisActiveSession { .. } => (
            OutcomeCode::ActiveSessionConflict,
            "a Product Genesis discovery session is already active",
            None,
            None,
        ),
        crate::ServiceError::AuthorizationDenied { .. } => unreachable!("handled above"),
        crate::ServiceError::InvalidOperation { .. }
        | crate::ServiceError::TerminalInvalidInput { .. } => (
            OutcomeCode::ValidationError,
            "the operation or arguments are not valid for this Forge surface",
            None,
            Some(RetryInstruction::new(RetryAction::CorrectInput, false)),
        ),
        crate::ServiceError::ExecutionSetupRequired { requirements, .. } => (
            OutcomeCode::SetupRequired,
            "required Forge setup is incomplete",
            requirements.first().cloned(),
            Some(RetryInstruction::new(RetryAction::CompleteSetup, true)),
        ),
        crate::ServiceError::DependencyGate
        | crate::ServiceError::MissingPrimaryRepo { .. }
        | crate::ServiceError::PrimaryRepoNotFound { .. }
        | crate::ServiceError::RepoMismatch { .. }
        | crate::ServiceError::TerminalWorkspaceNotReady
        | crate::ServiceError::TerminalDisabled => (
            OutcomeCode::SetupRequired,
            "required Forge setup is incomplete",
            Some(SetupRequirement::new("forge_setup")),
            Some(RetryInstruction::new(RetryAction::CompleteSetup, false)),
        ),
        crate::ServiceError::TaskActionUnavailable { .. } => (
            OutcomeCode::SetupRequired,
            "the requested Forge action is not currently available",
            Some(SetupRequirement::new("task_action")),
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
        ),
        crate::ServiceError::RateLimited {
            retry_after_seconds,
        } => {
            let mut retry = RetryInstruction::new(RetryAction::RetryAfter, true);
            retry.after_seconds = Some(retry_after_seconds);
            (
                OutcomeCode::TransientFailure,
                "the Forge operation is temporarily rate limited",
                None,
                Some(retry),
            )
        }
        crate::ServiceError::DaemonNotReady { .. }
        | crate::ServiceError::PrepareFailed { .. }
        | crate::ServiceError::DaemonUnavailable { .. }
        | crate::ServiceError::DaemonTimeout { .. }
        | crate::ServiceError::TerminalDaemonUnavailable { .. }
        | crate::ServiceError::TerminalSessionLimit { .. } => (
            OutcomeCode::TransientFailure,
            "the Forge operation is temporarily unavailable; retry later",
            None,
            Some(RetryInstruction::new(RetryAction::RetryAfter, true)),
        ),
        other => {
            // The outer boundary adds the authoritative correlation id before
            // recording an operator diagnostic.  Keep this mapper itself
            // free of model-visible implementation details.
            let _ = other;
            (
                OutcomeCode::InternalFailure,
                "the Forge operation could not complete",
                None,
                None,
            )
        }
    };
    let mut outcome = OrchestrationOutcome::failed(
        code,
        "unknown",
        OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
        "",
        safe_message,
    );
    outcome.setup_requirements = setup_requirement.map(|requirement| vec![requirement]);
    outcome.retry = retry;
    AgentHostError::StructuredOutcome(Box::new(outcome))
}
