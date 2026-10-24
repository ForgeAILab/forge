//! Native proposal envelopes, direct commands, and Main Genesis commands.

use super::*;

impl CoordinationToolProvider {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn enqueue_action(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
        payload: Value,
        requested_permission: &str,
        target_type: Option<String>,
        target_id: Option<String>,
    ) -> Result<Value, AgentHostError> {
        let dedupe_key = required_argument(&arguments, "dedupe_key")?;
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);

        let action = self
            .actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: actor_identity_id.to_owned(),
                scope_type: scope_type_name(scope.scope_type).to_owned(),
                scope_id: scope.scope_id.clone(),
                operation: operation.to_owned(),
                payload_json: payload.to_string(),
                dedupe_key,
                correlation_id,
                causation_id,
                causation_depth,
                requested_permission: requested_permission.to_owned(),
                policy_reason: None,
                target_type,
                target_id,
            })
            .await
            .map_err(service_error)?;
        if action.policy_result == AgentActionPolicyResult::Denied {
            return Err(AgentHostError::Authority(
                action
                    .policy_reason
                    .clone()
                    .unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
            ));
        }
        let mut response = action_value(&action);
        if operation_contract(operation).is_some() {
            // A proposal row is not a domain success. Protected Main
            // Project creation and all Project-local operations remain
            // explicitly pending until their typed executor/user transaction
            // runs.
            if let Some(object) = response.as_object_mut() {
                object.insert("materialized".to_owned(), Value::Bool(false));
                object.insert("domain_committed".to_owned(), Value::Bool(false));
                object.insert("domain_result".to_owned(), Value::Null);
                object.insert(
                    "requires_user_authorization".to_owned(),
                    Value::Bool(operation == MAIN_PROJECT_CREATE_OPERATION),
                );
            }
        }
        Ok(response)
    }

    /// Route an operation that the canonical host catalog has already
    /// classified as a direct command.  The adapter only supplies the
    /// server-derived source scope and policy envelope; Task/Project command
    /// services own authorization, validation, receipt replay, and domain
    /// persistence.  No branch here creates an `AgentAction` row.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_direct_command(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        payload: Value,
        requested_permission: &str,
        target_id: Option<String>,
        idempotency_key: String,
        correlation_id: String,
        causation_id: Option<String>,
        causation_depth: i64,
    ) -> Result<Value, AgentHostError> {
        if operation == PROJECT_ESCALATE_OPERATION {
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority("escalation requires a bound Project".to_owned())
            })?;
            let policy_payload = payload.to_string();
            let (policy, reason) = self
                .actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    Some(&policy_payload),
                )
                .await
                .map_err(service_error)?;
            if !matches!(policy, AgentActionPolicyResult::Allowed) {
                return Err(AgentHostError::Authority(
                    reason.unwrap_or_else(|| "escalation denied".to_owned()),
                ));
            }
            let request: api_types::ProjectEscalateRequest =
                serde_json::from_value(payload).map_err(|e| invalid_arguments(e.to_string()))?;
            let result =
                crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                    .escalate(
                        &project_id,
                        crate::project_escalation::EscalationAuthority::Agent(actor_identity_id),
                        request,
                        &idempotency_key,
                    )
                    .await
                    .map_err(service_error)?;
            return Ok(
                json!({"operation":operation,"status":"succeeded","materialized":true,"domain_committed":true,"domain_result":result,"correlation_id":correlation_id,"requires_user_authorization":false}),
            );
        }
        if forge_agent_host::is_project_orchestration_operation(operation) {
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority(
                    "direct Project command has no server-derived Project target".to_owned(),
                )
            })?;
            let result = self
                .project_actions
                .execute_direct(ExecuteDirectProjectCommandInput {
                    actor_identity_id: actor_identity_id.to_owned(),
                    scope_type: scope_type_name(scope.scope_type).to_owned(),
                    scope_id: scope.scope_id.clone(),
                    project_id,
                    operation: operation.to_owned(),
                    payload,
                    idempotency_key,
                    correlation_id,
                    causation_id,
                    causation_depth,
                    requested_permission: requested_permission.to_owned(),
                })
                .await
                .map_err(service_error)?;
            return Ok(direct_project_command_value(result));
        }

        Err(AgentHostError::Authority(
            "direct command has no shared service boundary".to_owned(),
        ))
    }

    /// Execute the directly admitted Main Charter draft without touching the
    /// AgentAction queue.  Policy is evaluated through the same service
    /// ceiling as proposals; the typed Main/Genesis command owns all domain
    /// validation, receipt creation, and replay behavior.
    pub(super) async fn execute_main_genesis_charter_draft(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        mut payload: Value,
    ) -> Result<Value, AgentHostError> {
        // `action` is the transport-level operation discriminator.  The
        // command service owns the typed Charter request and all domain
        // validation; do not duplicate an action/payload schema here.
        if let Some(object) = payload.as_object_mut() {
            object.remove("action");
        }
        let request: MainGenesisCharterDraftRequest = typed_command_payload(
            MAIN_CHARTER_DRAFT_OPERATION,
            scope,
            &correlation_id(&arguments, MAIN_CHARTER_DRAFT_OPERATION, scope),
            payload.clone(),
        )?;
        let dedupe_key = required_argument(&arguments, "dedupe_key")?;
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let requested_permission =
            operation_permission(scope.scope_type, MAIN_CHARTER_DRAFT_OPERATION).ok_or_else(
                || {
                    AgentHostError::Authority(
                        "charter draft has no canonical permission descriptor".to_owned(),
                    )
                },
            )?;
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                MAIN_CHARTER_DRAFT_OPERATION,
                Some(&payload.to_string()),
            )
            .await
            .map_err(service_error)?;
        if policy_result != AgentActionPolicyResult::Allowed {
            let reason = policy_reason.unwrap_or_else(|| {
                "Main Charter draft policy did not admit this command".to_owned()
            });
            tracing::warn!(diagnostic = %reason, "charter.draft policy denied");
            return Err(AgentHostError::Authority(reason));
        }
        let result = MainGenesisCommandService::new(self.db.clone())
            .execute(MainGenesisDraftCommandInput {
                principal: MainGenesisDraftPrincipal::MainAgent {
                    identity_id: actor_identity_id.to_owned(),
                    scope: scope.clone(),
                },
                request,
                idempotency_key: dedupe_key,
                correlation_id,
                causation_id,
                causation_depth,
                policy_result: policy_result.to_string(),
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": MAIN_CHARTER_DRAFT_OPERATION,
            "status": "succeeded",
            "materialized": true,
            "domain_committed": true,
            "receipt_id": result.receipt_id,
            "event_id": result.event_id,
            "domain_result": result.result,
        }))
    }

    /// Start Product Genesis from the currently leased Main baseline turn.
    /// The command service resolves the source message and turn from that
    /// lease; neither identifier is accepted from model-authored payload.
    pub(super) async fn execute_main_genesis_start(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        mut payload: Value,
    ) -> Result<Value, AgentHostError> {
        if let Some(object) = payload.as_object_mut() {
            object.remove("action");
        }
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let request: MainGenesisStartRequest = typed_command_payload(
            MAIN_GENESIS_START_OPERATION,
            scope,
            &correlation_id,
            payload.clone(),
        )?;
        let idempotency_key = required_argument(&arguments, "dedupe_key")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let requested_permission =
            operation_permission(scope.scope_type, MAIN_GENESIS_START_OPERATION).ok_or_else(
                || {
                    AgentHostError::Authority(
                        "Product Genesis start has no canonical permission descriptor".to_owned(),
                    )
                },
            )?;
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                MAIN_GENESIS_START_OPERATION,
                Some(&payload.to_string()),
            )
            .await
            .map_err(service_error)?;
        if policy_result != AgentActionPolicyResult::Allowed {
            return Err(AgentHostError::Authority(policy_reason.unwrap_or_else(
                || "Product Genesis start policy did not admit this command".to_owned(),
            )));
        }
        let result = MainGenesisCommandService::new(self.db.clone())
            .start(MainGenesisStartCommandInput {
                principal: MainGenesisStartPrincipal::MainAgent {
                    identity_id: actor_identity_id.to_owned(),
                    scope: scope.clone(),
                },
                request,
                idempotency_key,
                correlation_id,
                causation_id,
                causation_depth,
                policy_result: policy_result.to_string(),
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": MAIN_GENESIS_START_OPERATION,
            "status": "succeeded",
            "materialized": true,
            "domain_committed": true,
            "control_transfer": result.control_transfer,
            "receipt_id": result.receipt_id,
            "event_id": result.event_id,
            "domain_result": result.result,
        }))
    }
}
