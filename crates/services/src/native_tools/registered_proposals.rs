//! Main command bindings. Durable envelope and domain replay remain unchanged.
use super::*;
use async_trait::async_trait;
use operation_registry::main_proposals::{
    MainProposalContext, ProjectAgentSelection, ProjectCreate,
};
use std::sync::LazyLock;

pub(super) static CATALOG: LazyLock<operation_registry::OperationCatalog<AgentHostError>> =
    LazyLock::new(operation_registry::proposal_catalog);
impl CoordinationToolProvider {
    pub(super) async fn registered_authority(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let Some(spec) = registered_reads::CATALOG
            .lookup(operation)
            .or_else(|| CATALOG.lookup(operation))
        else {
            return Ok(());
        };
        let authority = self.resolve_registered_authority(actor, scope).await?;
        self.evaluate_registered_authority(&authority, scope, spec)
    }
    pub(super) async fn resolve_registered_authority(
        &self,
        actor: &str,
        scope: &CanonicalScope,
    ) -> Result<operation_registry::authority::EffectiveAuthority, AgentHostError> {
        self.db
            .resolve_effective_authority(
                actor,
                None,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                match scope.workspace_access {
                    WorkspaceAccess::AccountScratch => "account_scratch",
                    WorkspaceAccess::ProjectVerify => "project_verify",
                    _ => "deny",
                },
            )
            .await
            .map_err(|error| match error {
                db::DbError::NotFound => service_error(error.into()),
                // A persistence failure is not a denial: the boundary reports
                // it as an internal failure that names the operation.
                _ => AgentHostError::ProtectedPersistence,
            })
    }
    pub(super) fn evaluate_registered_authority(
        &self,
        authority: &operation_registry::authority::EffectiveAuthority,
        scope: &CanonicalScope,
        spec: &operation_registry::OperationSpec<AgentHostError>,
    ) -> Result<(), AgentHostError> {
        authority.evaluate(spec).map_err(|denial| {
            use operation_registry::authority::AuthorityDenial;
            let cause = match denial {
                AuthorityDenial::Revoked => DeniedBy::AuthorityRevoked,
                AuthorityDenial::PermissionMissing(permission) => {
                    DeniedBy::PermissionMissing(permission)
                }
                AuthorityDenial::PrincipalMismatch => DeniedBy::OperationNotInScope,
                AuthorityDenial::StateChanged => DeniedBy::CharterNotAdopted,
                AuthorityDenial::SetupCompleted => DeniedBy::CharterAdoptionNotApplicable,
            };
            AgentHostError::StructuredOutcome(Box::new(OrchestrationOutcome::terminal_denial(
                spec.id,
                outcome_scope(scope),
                "authority",
                cause,
            )))
        })
    }
    pub(super) async fn proposal_admission(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let Some(spec) = CATALOG.lookup(operation) else {
            return Ok(());
        };
        let _ = spec;
        self.main_proposal_binding(actor, scope, operation).await?;
        self.registered_authority(actor, scope, operation).await?;
        Ok(())
    }
    /// A native Main proposal still needs the caller's active Main binding.
    /// The shared evaluator admits an owned unbound identity for the
    /// AgentAction policy; the native boundary never did.
    async fn main_proposal_binding(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        if operation_registry::main_proposals::IDS.contains(&operation) {
            self.authorization
                .main_account_id(actor, scope)
                .await
                .map_err(native_scope_error)?;
        }
        Ok(())
    }
    /// A queued proposal (the six pending ones and the release candidate
    /// request) refused for a missing permission still leaves its denied
    /// ledger row, as it did before these operations were registered: the
    /// attempt stays visible and can never be approved. The denial the
    /// caller receives is unchanged; nothing is recorded unless the action
    /// policy itself denies the same request.
    async fn record_denied_queued_proposal(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: &Value,
        denial: &AgentHostError,
    ) {
        let release = operation == PROJECT_RELEASE_OPERATION;
        if !(release || operation_registry::legacy_proposals::IDS.contains(&operation))
            || !matches!(denial, AgentHostError::StructuredOutcome(outcome)
                if matches!(outcome.denied_by, Some(DeniedBy::PermissionMissing(_))))
            || contains_authority_override(arguments)
        {
            return;
        }
        let Some(permission) = operation_permission(scope.scope_type, operation) else {
            return;
        };
        let payload = match arguments.get("payload") {
            Some(payload) if payload.is_object() => payload.clone(),
            None | Some(Value::Null) => json!({}),
            Some(_) => return,
        };
        let (Ok(dedupe_key), Ok(correlation_id)) = (
            required_argument(arguments, "dedupe_key"),
            required_argument(arguments, "correlation_id"),
        ) else {
            return;
        };
        // The release request has a closed contract and a Project target:
        // a call that would have been refused before it was queued leaves
        // no row, exactly as before.
        let (payload, release_target) = if release {
            let spec = CATALOG.lookup(operation).expect("registered proposal");
            let (Ok(payload), Ok(target)) = (
                spec.normalize_arguments(&payload),
                self.authorization.direct_project_target(scope).await,
            ) else {
                return;
            };
            (payload, Some(target))
        } else {
            (payload, None)
        };
        if validate_proposal_payload(operation, &payload).is_err() {
            return;
        }
        let scope_type = scope_type_name(scope.scope_type);
        let payload_json = payload.to_string();
        if !matches!(
            self.actions
                .evaluate_direct_command_policy(
                    actor,
                    scope_type,
                    &scope.scope_id,
                    permission,
                    operation,
                    Some(&payload_json),
                )
                .await,
            Ok((AgentActionPolicyResult::Denied, _))
        ) {
            return;
        }
        let target_type = if release {
            "project"
        } else if operation == "session.action" {
            "scope"
        } else {
            scope_type
        };
        if let Err(error) = self
            .actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: actor.to_owned(),
                scope_type: scope_type.to_owned(),
                scope_id: scope.scope_id.clone(),
                operation: operation.to_owned(),
                payload_json,
                dedupe_key,
                correlation_id,
                causation_id: arguments
                    .get("causation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                causation_depth: arguments
                    .get("causation_depth")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                requested_permission: permission.to_owned(),
                policy_reason: None,
                target_type: Some(target_type.to_owned()),
                target_id: Some(release_target.unwrap_or_else(|| scope.scope_id.clone())),
            })
            .await
        {
            tracing::warn!(operation, %error, "denied proposal was not recorded");
        }
    }
    pub(super) async fn registered_proposal(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        mut arguments: Value,
        prepared: bool,
        admitted_authority: Option<&operation_registry::authority::EffectiveAuthority>,
    ) -> Result<Value, AgentHostError> {
        // Current authority precedes every payload diagnostic. Stored preparations
        // keep their exact arguments; only fresh calls consult the current spec.
        if let Some(admitted) = admitted_authority {
            self.main_proposal_binding(actor, scope, operation).await?;
            self.evaluate_registered_authority(
                admitted,
                scope,
                CATALOG.lookup(operation).expect("registered proposal"),
            )?;
        } else if let Err(denial) = self.proposal_admission(actor, scope, operation).await {
            if !prepared {
                self.record_denied_queued_proposal(actor, scope, operation, &arguments, &denial)
                    .await;
            }
            return Err(denial);
        }
        if contains_authority_override(&arguments) {
            return Err(AgentHostError::Authority(
                "Forge orchestration scope and authority are server-derived".into(),
            ));
        }
        let spec = CATALOG.lookup(operation).expect("registered proposal");
        if !prepared {
            const FIELDS: &[&str] = &[
                "operation",
                "payload",
                "dedupe_key",
                "correlation_id",
                "causation_id",
                "causation_depth",
            ];
            let object = arguments
                .as_object()
                .ok_or_else(|| invalid_arguments("proposal must be an object".into()))?;
            if let Some(field) = object
                .keys()
                .find(|field| !FIELDS.contains(&field.as_str()))
            {
                return Err(invalid_arguments(format!(
                    "{operation}: envelope field `{field}` is not admitted; expected {}",
                    spec.contract_line()
                )));
            }
            for field in ["dedupe_key", "correlation_id"] {
                required_argument(&arguments, field)?;
            }
            if let Some(depth) = object.get("causation_depth") {
                if !depth.as_i64().is_some_and(|depth| (0..=8).contains(&depth)) {
                    return Err(invalid_arguments(
                        "causation_depth must be an integer between 0 and 8".into(),
                    ));
                }
            }
            if object
                .get("causation_id")
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                return Err(invalid_arguments(
                    "causation_id must be a string or null".into(),
                ));
            }
        }
        let payload = if prepared {
            arguments["payload"].clone()
        } else {
            spec.normalize_arguments(&arguments["payload"])
                .map_err(invalid_arguments)?
        };
        // Preserve the existing serialized payload ceiling and its refusal: an
        // oversized payload is a malformed call, not a policy denial.
        validate_proposal_payload(operation, &payload).map_err(|_| {
            AgentHostError::Unsupported(
                "proposal payload does not match the typed operation schema".to_owned(),
            )
        })?;
        arguments["payload"] = payload.clone();
        let context = super::registered_reads::Context {
            provider: self,
            actor_identity_id: actor,
            scope,
            proposal_arguments: Some(&arguments),
            admitted_authority,
        };
        spec.dispatch_prepared(&context, payload)
            .await
            .map_err(|error| match error {
                operation_registry::DispatchError::Handler(error) => error,
                operation_registry::DispatchError::InvalidInput(message) => {
                    invalid_arguments(message)
                }
            })
    }
}
#[async_trait]
impl MainProposalContext<AgentHostError> for super::registered_reads::Context<'_> {
    async fn select_project_agent(
        &self,
        input: ProjectAgentSelection,
    ) -> Result<Value, AgentHostError> {
        let permission = CATALOG
            .lookup(MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION)
            .unwrap()
            .authority
            .permission(scope_type_name(self.scope.scope_type))
            .unwrap();
        let result = MainGenesisCommandService::new(self.provider.db.clone())
            .select_project_agent_for_admission(
                crate::MainGenesisProjectAgentSelectCommandInput {
                    principal: MainGenesisDraftPrincipal::MainAgent {
                        identity_id: self.actor_identity_id.to_owned(),
                        scope: self.scope.clone(),
                    },
                    request: crate::MainGenesisProjectAgentSelectRequest {
                        genesis_session_id: input.genesis_session_id,
                        expected_session_version: input.expected_session_version,
                        project_agent_identity_id: input.project_agent_identity_id,
                    },
                    idempotency_key: required_argument(
                        self.proposal_arguments.expect("proposal envelope"),
                        "dedupe_key",
                    )?,
                    correlation_id: required_argument(
                        self.proposal_arguments.expect("proposal envelope"),
                        "correlation_id",
                    )?,
                    causation_id: self
                        .proposal_arguments
                        .expect("proposal envelope")
                        .get("causation_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    causation_depth: self
                        .proposal_arguments
                        .expect("proposal envelope")
                        .get("causation_depth")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                    policy_result: AgentActionPolicyResult::Allowed.to_string(),
                    requested_permission: permission.to_owned(),
                },
                self.admitted_authority,
            )
            .await
            .map_err(service_error)?;
        Ok(
            json!({"operation":MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,"status":"succeeded","replayed":result.replayed,"materialized":true,"domain_committed":true,"receipt_id":result.receipt_id,"event_id":result.event_id,"domain_result":result.result}),
        )
    }
    async fn propose_project_create(&self, _input: ProjectCreate) -> Result<Value, AgentHostError> {
        let account = self
            .provider
            .authorization
            .main_account_target(self.scope)
            .await
            .map_err(native_scope_error)?;
        self.provider
            .enqueue_action(
                self.actor_identity_id,
                self.scope,
                MAIN_PROJECT_CREATE_OPERATION,
                self.proposal_arguments.expect("proposal envelope").clone(),
                self.proposal_arguments.expect("proposal envelope")["payload"].clone(),
                "propose_project",
                Some("account".into()),
                Some(account),
            )
            .await
    }
}

#[async_trait]
impl operation_registry::hand_proposals::HandProposalContext<AgentHostError>
    for super::registered_reads::Context<'_>
{
    async fn genesis_start(
        &self,
        _: operation_registry::hand_proposals::GenesisStart,
    ) -> Result<Value, AgentHostError> {
        let arguments = self.proposal_arguments.expect("proposal envelope");
        self.provider
            .execute_main_genesis_start(
                self.actor_identity_id,
                self.scope,
                arguments.clone(),
                arguments["payload"].clone(),
            )
            .await
    }
    async fn charter_draft(
        &self,
        _: operation_registry::hand_proposals::CharterDraft,
    ) -> Result<Value, AgentHostError> {
        let arguments = self.proposal_arguments.expect("proposal envelope");
        self.provider
            .execute_main_genesis_charter_draft(
                self.actor_identity_id,
                self.scope,
                arguments.clone(),
                arguments["payload"].clone(),
            )
            .await
    }
}
