//! Typed Project and pending-intent bindings. Command envelopes are forwarded
//! verbatim so receipt digests and prepared replay keep the hand-path bytes.
use super::registered_reads::Context;
use super::*;
use async_trait::async_trait;
use forge_agent_host::PROJECT_REVIEW_CONFIG_OPERATION;
use operation_registry::{legacy_proposals as legacy, project_proposals as project};

impl Context<'_> {
    async fn project_command(&self, operation: &str) -> Result<Value, AgentHostError> {
        let arguments = self.proposal_arguments.expect("proposal envelope");
        let spec = registered_proposals::CATALOG
            .lookup(operation)
            .expect("registered Project command");
        let permission = spec
            .authority
            .permission(scope_type_name(self.scope.scope_type))
            .expect("admitted permission");
        let target = self
            .provider
            .authorization
            .direct_project_target(self.scope)
            .await
            .map_err(native_scope_error)?;
        self.provider
            .execute_direct_command(
                self.actor_identity_id,
                self.scope,
                operation,
                arguments["payload"].clone(),
                permission,
                Some(target),
                required_argument(arguments, "dedupe_key")?,
                required_argument(arguments, "correlation_id")?,
                arguments
                    .get("causation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                arguments
                    .get("causation_depth")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )
            .await
    }
    async fn pending(&self, operation: &str) -> Result<Value, AgentHostError> {
        let arguments = self.proposal_arguments.expect("pending-proposal envelope");
        let spec = registered_proposals::CATALOG
            .lookup(operation)
            .expect("registered pending proposal");
        let permission = spec
            .authority
            .permission(scope_type_name(self.scope.scope_type))
            .expect("admitted permission");
        let target_type = if operation == "session.action" {
            "scope"
        } else {
            scope_type_name(self.scope.scope_type)
        };
        let result = self
            .provider
            .enqueue_action(
                self.actor_identity_id,
                self.scope,
                operation,
                arguments.clone(),
                arguments["payload"].clone(),
                permission,
                Some(target_type.into()),
                Some(self.scope.scope_id.clone()),
            )
            .await?;
        if matches!(result["status"].as_str(), Some("succeeded" | "executed")) {
            return Err(AgentHostError::Authority(
                "a legacy proposal has no materialized domain effect".into(),
            ));
        }
        Ok(result)
    }
}
#[async_trait]
impl project::ProjectProposalContext<AgentHostError> for Context<'_> {
    async fn review_config(&self, _: project::ReviewConfig) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_REVIEW_CONFIG_OPERATION).await
    }
    async fn document(&self, _: project::Document) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_DOCUMENT_OPERATION).await
    }
    async fn decision(&self, _: project::Decision) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_DECISION_OPERATION).await
    }
    async fn milestone(&self, _: project::Milestone) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_MILESTONE_OPERATION).await
    }
    async fn validation(&self, _: project::Validation) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_VALIDATION_OPERATION).await
    }
    async fn release_request(&self, _: project::ReleaseRequest) -> Result<Value, AgentHostError> {
        let arguments = self.proposal_arguments.expect("proposal envelope");
        let target = self
            .provider
            .authorization
            .direct_project_target(self.scope)
            .await
            .map_err(native_scope_error)?;
        self.provider
            .enqueue_action(
                self.actor_identity_id,
                self.scope,
                PROJECT_RELEASE_OPERATION,
                arguments.clone(),
                arguments["payload"].clone(),
                "propose_project",
                Some("project".into()),
                Some(target),
            )
            .await
    }
    async fn escalate(&self, _: project::Escalation) -> Result<Value, AgentHostError> {
        self.project_command(PROJECT_ESCALATE_OPERATION).await
    }
}
#[async_trait]
impl legacy::LegacyProposalContext<AgentHostError> for Context<'_> {
    async fn pending_message(&self, _: legacy::Message) -> Result<Value, AgentHostError> {
        self.pending("message.send").await
    }
    async fn pending_commitment(&self, _: legacy::Commitment) -> Result<Value, AgentHostError> {
        self.pending("commitment.update").await
    }
    async fn pending_memory_publish(&self, _: legacy::Memory) -> Result<Value, AgentHostError> {
        self.pending("memory.publish").await
    }
    async fn pending_memory_supersede(&self, _: legacy::Memory) -> Result<Value, AgentHostError> {
        self.pending("memory.supersede").await
    }
    async fn pending_review(&self, _: legacy::Review) -> Result<Value, AgentHostError> {
        self.pending("review.request").await
    }
    async fn pending_session(&self, _: legacy::Session) -> Result<Value, AgentHostError> {
        self.pending("session.action").await
    }
}
