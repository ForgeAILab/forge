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
        let permission = spec
            .authority
            .permission(scope_type_name(scope.scope_type))
            .ok_or_else(|| {
                AgentHostError::Authority(
                    "proposal operation is not admitted for this scope".into(),
                )
            })?;
        let (policy, reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                permission,
                operation,
                None,
            )
            .await
            .map_err(service_error)?;
        if policy == AgentActionPolicyResult::Denied {
            return Err(AgentHostError::Authority(
                reason.unwrap_or_else(|| "Main proposal policy denied".into()),
            ));
        }
        Ok(())
    }
    pub(super) async fn registered_proposal(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        mut arguments: Value,
        prepared: bool,
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
        // Preserve the existing serialized payload ceiling and domain guards.
        validate_proposal_payload(operation, &payload)?;
        arguments["payload"] = payload.clone();
        let context = super::registered_reads::Context {
            provider: self,
            actor_identity_id: actor,
            scope,
            proposal_arguments: Some(&arguments),
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
            .select_project_agent(crate::MainGenesisProjectAgentSelectCommandInput {
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
            })
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
