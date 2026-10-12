//! Typed native Task command bindings. Domain commands own CAS, receipts and effects.
use super::*;
use operation_registry::task_proposals::{self as task, TaskProposalContext};
fn dependency_parts(input: task::Dependency) -> (String, String, TaskDependencyAction, String) {
    match input {
        task::Dependency::Add {
            task_id,
            depends_on_task_id,
            rationale,
        } => (
            task_id,
            depends_on_task_id,
            TaskDependencyAction::Add,
            rationale,
        ),
        task::Dependency::Remove {
            task_id,
            depends_on_task_id,
            rationale,
        } => (
            task_id,
            depends_on_task_id,
            TaskDependencyAction::Remove,
            rationale,
        ),
    }
}

fn adaptive_parts(input: task::Adaptive) -> (String, i64, i64, AdaptiveTaskOperation, String) {
    match input {
        task::Adaptive::Split {
            source_task_id,
            expected_task_version,
            expected_board_revision,
            rationale,
            items,
        } => (
            source_task_id,
            expected_task_version,
            expected_board_revision,
            AdaptiveTaskOperation::Split {
                items: items
                    .into_iter()
                    .map(|item| AdaptiveTaskChild {
                        title: item.title,
                        description: item.description,
                        assignee_id: item.assignee_id,
                    })
                    .collect(),
            },
            rationale,
        ),
        task::Adaptive::Sequence {
            source_task_id,
            expected_task_version,
            expected_board_revision,
            rationale,
            ordered_task_ids,
        } => (
            source_task_id,
            expected_task_version,
            expected_board_revision,
            AdaptiveTaskOperation::Sequence { ordered_task_ids },
            rationale,
        ),
        task::Adaptive::Replace {
            source_task_id,
            expected_task_version,
            expected_board_revision,
            rationale,
            title,
            description,
        } => (
            source_task_id,
            expected_task_version,
            expected_board_revision,
            AdaptiveTaskOperation::Replace { title, description },
            rationale,
        ),
    }
}

fn proposal_payload(input: task::Proposal) -> TaskProposalPayload {
    TaskProposalPayload {
        title: input.title,
        description: input.description,
        parent_task_id: input.parent_task_id,
        priority: input.priority,
        task_type: input.task_type,
        task_state_config: input.task_state_config,
        review_requirement_ids: input.review_requirement_ids,
        merge_config: input.merge_config,
        role_assignments: input.role_assignments,
        governance: input.governance,
        plan_item_id: input.plan_item_id,
        milestone_id: input.milestone_id,
        capability_class: input.capability_class,
        risk_class: input.risk_class,
        depends_on_task_ids: input.depends_on_task_ids,
    }
}
impl CoordinationToolProvider {
    async fn registered_task_action(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: task::Action,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let operation = TASK_ACTION_OPERATION;
        let requested_permission = registered_proposals::CATALOG
            .lookup(operation)
            .unwrap()
            .authority
            .permission(scope_type_name(scope.scope_type))
            .unwrap();
        let target_id = Some(
            self.authorization
                .direct_project_target(scope)
                .await
                .map_err(native_scope_error)?,
        );
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let task_service = self.task_service_handle().ok_or_else(|| {
            AgentHostError::Configuration("Task actions are not wired".to_owned())
        })?;
        let project_id = target_id.ok_or_else(|| {
            AgentHostError::Authority("Task action has no canonical Project".to_owned())
        })?;
        let task_id = input.task_id.as_str();
        let task = db::TaskRepo::get_by_id(&*self.db, task_id, false)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
            .filter(|task| task.project_id == project_id)
            .ok_or_else(|| {
                invalid_arguments("task_id must name a Task in this Project".to_owned())
            })?;
        let request = api_types::TaskActionRequest {
            action: input.action,
            version: input.version,
        };
        let (policy, reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                operation,
                None,
            )
            .await
            .map_err(service_error)?;
        if !matches!(policy, AgentActionPolicyResult::Allowed) {
            return Err(AgentHostError::Authority(
                reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
            ));
        }
        // Fresh unoffered Hold/Cancel calls must not reach the command's
        // late-integration no-op branch. Already admitted queued commands
        // retain that settlement behavior inside the Task writer.
        if matches!(
            request.action,
            api_types::TaskAction::Hold { .. } | api_types::TaskAction::Cancel { .. }
        ) {
            let offered = task_service
                .task_action_offers(&task.id, &api_types::Actor::agent(actor_identity_id))
                .await
                .map_err(service_error)?;
            if offered.version != request.version {
                return Err(service_error(
                    db::DbError::TaskVersionConflict {
                        expected: request.version,
                        actual: offered.version,
                    }
                    .into(),
                ));
            }
            if !offered.available_actions.iter().any(|offer| {
                offer.action.verb() == request.action.verb()
                    && task::action_parameters_allowed(offer, &request.action, false)
            }) {
                return Err(service_error(crate::ServiceError::TaskActionUnavailable {
                    available_actions: offered.available_actions,
                    reason: format!("action '{}' is unavailable", request.action.verb()),
                    wait_cause: None,
                }));
            }
        }
        let unblocking = crate::project_escalation::is_unblocking_verb(request.action.verb());
        let result = task_service
            .perform_task_action_as(
                task.id,
                request.action,
                request.version,
                api_types::Actor::agent(actor_identity_id),
            )
            .await
            .map_err(service_error)?;
        if unblocking {
            crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                .record_unblocking_action(&project_id, actor_identity_id, &result.task.id)
                .await
                .map_err(service_error)?;
        }
        let offers = task_service
            .task_action_offers(&result.task.id, &api_types::Actor::agent(actor_identity_id))
            .await
            .map_err(service_error)?;
        Ok(
            json!({ "operation": operation, "status": "succeeded", "replayed": false, "materialized": true, "domain_committed": true, "correlation_id": correlation_id, "task_id": result.task.id, "task_status": result.task.status, "task_version": result.task.version, "available_actions": offers.available_actions, "requires_user_authorization": false }),
        )
    }
    async fn registered_task_dependency(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: task::Dependency,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let operation = TASK_DEPENDENCY_OPERATION;
        let requested_permission = registered_proposals::CATALOG
            .lookup(operation)
            .unwrap()
            .authority
            .permission(scope_type_name(scope.scope_type))
            .unwrap();
        let target_id = Some(
            self.authorization
                .direct_project_target(scope)
                .await
                .map_err(native_scope_error)?,
        );
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let Some(task_service) = self.task_service_handle() else {
            return Err(AgentHostError::Configuration(
                "Task dependency execution is not wired to a TaskService".to_owned(),
            ));
        };
        let project_id = target_id.ok_or_else(|| {
            AgentHostError::Authority(
                "Task dependency command has no server-derived Project target".to_owned(),
            )
        })?;
        let (task_id, depends_on_task_id, action, _rationale) = dependency_parts(input);
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                operation,
                None,
            )
            .await
            .map_err(service_error)?;
        if !matches!(policy_result, AgentActionPolicyResult::Allowed) {
            tracing::warn!(
                operation,
                diagnostic = policy_reason.as_deref().unwrap_or("no reason recorded"),
                "Task dependency command policy denied"
            );
            return Err(AgentHostError::Authority(
                policy_reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
            ));
        }
        let task = task_service
            .perform_project_agent_dependency(&project_id, &task_id, &depends_on_task_id, action)
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": operation,
            "status": "succeeded",
            "replayed": false,
            "materialized": true,
            "domain_committed": true,
            "correlation_id": correlation_id,
            "task_id": task.id,
            "task_status": task.status,
            "task_version": task.version,
            "condition": task.condition.public(),
            "requires_user_authorization": false,
        }))
    }
    async fn registered_task_adaptive(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: task::Adaptive,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let operation = TASK_ADAPTIVE_OPERATION;
        let requested_permission = registered_proposals::CATALOG
            .lookup(operation)
            .unwrap()
            .authority
            .permission(scope_type_name(scope.scope_type))
            .unwrap();
        let target_id = Some(
            self.authorization
                .direct_project_target(scope)
                .await
                .map_err(native_scope_error)?,
        );
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let idempotency_key = required_argument(arguments, "dedupe_key")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let payload = arguments["payload"].clone();
        let Some(task_service) = self.task_service_handle() else {
            return Err(AgentHostError::Configuration(
                "Adaptive Task execution is not wired to a TaskService".to_owned(),
            ));
        };
        let project_id = target_id.ok_or_else(|| {
            AgentHostError::Authority(
                "adaptive Task command has no server-derived Project target".to_owned(),
            )
        })?;

        let (
            source_task_id,
            expected_task_version,
            expected_board_revision,
            adaptive_operation,
            rationale,
        ) = adaptive_parts(input);
        let receipt_exists = self
            .adaptive_receipt_exists(actor_identity_id, &project_id, &idempotency_key)
            .await?;
        let (policy_result, policy_reason) = if receipt_exists {
            (AgentActionPolicyResult::Allowed, None)
        } else {
            self.actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    Some(&payload.to_string()),
                )
                .await
                .map_err(service_error)?
        };
        if !matches!(policy_result, AgentActionPolicyResult::Allowed) {
            return Err(AgentHostError::Authority(
                policy_reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
            ));
        }
        let result: AdaptiveTaskCommandResult = task_service
            .execute_adaptive_task_command(AdaptiveTaskCommand {
                project_id,
                source_task_id,
                expected_task_version,
                expected_board_revision,
                operation: adaptive_operation,
                rationale,
                actor_type: "agent".to_owned(),
                actor_id: actor_identity_id.to_owned(),
                policy_result: "allowed".to_owned(),
                policy_revision: None,
                policy_digest: None,
                requested_permission: Some(requested_permission.to_owned()),
                idempotency_key,
                correlation_id,
                causation_id,
                causation_depth,
            })
            .await
            .map_err(service_error)?;
        let task_ids = result
            .tasks
            .iter()
            .map(|task| Value::String(task.id.clone()))
            .collect::<Vec<_>>();
        Ok(json!({
            "operation": operation,
            "status": "succeeded",
            "replayed": result.replayed,
            "materialized": true,
            "domain_committed": true,
            "receipt_id": result.receipt.id,
            "event_id": result.receipt.event_id,
            "input_digest": result.receipt.input_digest,
            "policy_result": result.receipt.policy_result,
            "correlation_id": result.receipt.correlation_id,
            "source_task_id": result.source_task.id,
            "task_ids": task_ids,
            "board_revision": result.board_revision,
            "domain_result": {
                "source_task_id": result.source_task.id,
                "task_ids": result.tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>(),
                "board_revision": result.board_revision,
            },
            "action_id": Value::Null,
            "agent_action_execution_id": Value::Null,
            "requires_user_authorization": false,
        }))
    }
    async fn registered_task_propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: task::Proposal,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let operation = TASK_PROPOSE_OPERATION;
        let requested_permission = registered_proposals::CATALOG
            .lookup(operation)
            .unwrap()
            .authority
            .permission(scope_type_name(scope.scope_type))
            .unwrap();
        let target_id = Some(
            self.authorization
                .direct_project_target(scope)
                .await
                .map_err(native_scope_error)?,
        );
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let idempotency_key = required_argument(arguments, "dedupe_key")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let Some(task_service) = self.task_service_handle() else {
            return Err(AgentHostError::Configuration(
                "Task proposal execution is not wired to a TaskService".to_owned(),
            ));
        };
        let project_id = target_id.ok_or_else(|| {
            AgentHostError::Authority(
                "direct task proposal has no server-derived Project target".to_owned(),
            )
        })?;
        let payload = proposal_payload(input);
        let payload_json = serde_json::to_string(&payload).map_err(|_| {
            AgentHostError::Unsupported("task proposal payload is invalid".to_owned())
        })?;
        let (policy_result, reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                operation,
                Some(&payload_json),
            )
            .await
            .map_err(service_error)?;
        let result: TaskProposalCommandResult = task_service
            .execute_task_proposal_direct(DirectTaskProposalInput {
                actor_identity_id: actor_identity_id.to_owned(),
                executor_type: "agent".to_owned(),
                executor_id: actor_identity_id.to_owned(),
                source_scope_type: scope_type_name(scope.scope_type).to_owned(),
                source_scope_id: scope.scope_id.clone(),
                project_id,
                payload,
                idempotency_key,
                correlation_id,
                causation_id,
                causation_depth,
                policy_result: "allowed".to_owned(),
                preflight_policy_result: Some(policy_result.to_string()),
                preflight_policy_reason: reason,
                policy_revision: None,
                policy_digest: None,
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": operation,
            "status": "succeeded",
            "replayed": result.replayed,
            "materialized": true,
            "domain_committed": true,
            "receipt_id": result.receipt.id,
            "event_id": result.receipt.event_id,
            "input_digest": result.receipt.input_digest,
            "policy_result": result.receipt.policy_result,
            "correlation_id": result.receipt.correlation_id,
            "domain_result": {
                "task_id": result.task.id,
                "task_status": result.task.status,
            },
            "agent_action_execution_id": Value::Null,
            "requires_user_authorization": false,
        }))
    }
}

#[async_trait]
impl TaskProposalContext<AgentHostError> for registered_reads::Context<'_> {
    async fn task_action(&self, input: task::Action) -> Result<Value, AgentHostError> {
        self.provider
            .registered_task_action(
                self.actor_identity_id,
                self.scope,
                input,
                self.proposal_arguments.expect("Task command envelope"),
            )
            .await
    }
    async fn task_dependency(&self, input: task::Dependency) -> Result<Value, AgentHostError> {
        self.provider
            .registered_task_dependency(
                self.actor_identity_id,
                self.scope,
                input,
                self.proposal_arguments.expect("Task command envelope"),
            )
            .await
    }
    async fn task_adaptive(&self, input: task::Adaptive) -> Result<Value, AgentHostError> {
        self.provider
            .registered_task_adaptive(
                self.actor_identity_id,
                self.scope,
                input,
                self.proposal_arguments.expect("Task command envelope"),
            )
            .await
    }
    async fn task_propose(&self, input: task::Proposal) -> Result<Value, AgentHostError> {
        self.provider
            .registered_task_propose(
                self.actor_identity_id,
                self.scope,
                input,
                self.proposal_arguments.expect("Task command envelope"),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proposal_conversion_keeps_the_hand_decoder_canonical_bytes() {
        // The complete field set/order captured from TaskProposalPayload on
        // 433a00ab. This pins bytes used by Task command receipt digesting.
        let raw = json!({"title":"Task","review_requirement_ids":[],"priority":3,"task_type":"planning_task"});
        let old: TaskProposalPayload = serde_json::from_value(raw.clone()).unwrap();
        let typed: task::Proposal = serde_json::from_value(raw).unwrap();
        let frozen = r#"{"title":"Task","description":null,"parent_task_id":null,"priority":3,"task_type":"planning_task","task_state_config":null,"review_requirement_ids":[],"merge_config":null,"role_assignments":null,"governance":null,"plan_item_id":null,"milestone_id":null,"capability_class":null,"risk_class":null,"depends_on_task_ids":[]}"#;
        assert_eq!(serde_json::to_string(&old).unwrap(), frozen);
        assert_eq!(
            serde_json::to_string(&proposal_payload(typed)).unwrap(),
            frozen
        );
    }
}
