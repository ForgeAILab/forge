//! Main command bindings. Durable envelope and domain replay remain unchanged.
use super::*;
use async_trait::async_trait;
use operation_registry::main_proposals::{
    self, MainProposalContext, ProjectAgentSelection, ProjectCreate,
};
use std::sync::LazyLock;

pub(super) static CATALOG: LazyLock<main_proposals::Catalog<AgentHostError>> =
    LazyLock::new(main_proposals::catalog);
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
        let authority = self
            .db
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
            .map_err(|error| service_error(error.into()))?;
        authority.evaluate(spec).map_err(|denial| {
            use operation_registry::authority::AuthorityDenial;
            let cause = match denial {
                AuthorityDenial::Revoked => DeniedBy::AuthorityRevoked,
                AuthorityDenial::PermissionMissing(permission) => {
                    DeniedBy::PermissionMissing(permission)
                }
                AuthorityDenial::PrincipalMismatch => DeniedBy::OperationNotInScope,
                AuthorityDenial::StateChanged => DeniedBy::CharterNotAdopted,
            };
            AgentHostError::StructuredOutcome(Box::new(OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                "authority",
                cause,
            )))
        })
    }
    pub(super) async fn main_proposal_admission(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let Some(spec) = CATALOG.lookup(operation) else {
            return Ok(());
        };
        self.authorization
            .main_account_id(actor, scope)
            .await
            .map_err(native_scope_error)?;
        let _ = spec;
        self.registered_authority(actor, scope, operation).await?;
        Ok(())
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
        self.main_proposal_admission(actor, scope, operation)
            .await?;
        if contains_authority_override(&arguments) {
            return Err(AgentHostError::Authority(
                "Forge orchestration scope and authority are server-derived".into(),
            ));
        }
        let spec = CATALOG.lookup(operation).expect("registered proposal");
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
            .main_account_id(self.actor_identity_id, self.scope)
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
