//! Typed service bindings for the first shared read specifications.
use super::CoordinationToolProvider;
use async_trait::async_trait;
use forge_agent_host::{AgentHostError, CanonicalScope};
use operation_registry::{
    project_reads::SectionArguments, scope_reads::NoArguments, OperationCatalog,
};
use serde_json::Value;
use std::sync::LazyLock;

pub(super) static CATALOG: LazyLock<OperationCatalog<AgentHostError>> =
    LazyLock::new(operation_registry::read_catalog);

pub(super) struct Context<'a> {
    pub provider: &'a CoordinationToolProvider,
    pub actor_identity_id: &'a str,
    pub scope: &'a CanonicalScope,
    pub admitted_authority: Option<&'a operation_registry::authority::EffectiveAuthority>,
    pub proposal_arguments: Option<&'a Value>,
}
#[async_trait]
impl operation_registry::scope_reads::ScopeReadContext<AgentHostError> for Context<'_> {
    async fn account_summary(&self, _input: NoArguments) -> Result<Value, AgentHostError> {
        self.provider
            .summary(self.actor_identity_id, self.scope)
            .await
    }
    async fn chat_summary(&self, _input: NoArguments) -> Result<Value, AgentHostError> {
        self.provider
            .summary(self.actor_identity_id, self.scope)
            .await
    }
}
impl Context<'_> {
    fn project_target(&self) -> Result<&str, AgentHostError> {
        match self
            .admitted_authority
            .map(|authority| &authority.principal)
        {
            Some(operation_registry::authority::Principal::ProjectAgent { project_id, .. }) => {
                Ok(project_id)
            }
            _ => Err(AgentHostError::Authority(
                "Project read has no admitted target".into(),
            )),
        }
    }
}
#[async_trait]
impl operation_registry::project_reads::ProjectReadContext<AgentHostError> for Context<'_> {
    async fn current_state(
        &self,
        input: operation_registry::project_reads::CurrentStateArguments,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .project_current_state_read(
                self.actor_identity_id,
                self.scope,
                self.project_target()?,
                input,
            )
            .await
    }
    async fn observations(
        &self,
        input: operation_registry::project_reads::ObservationsArguments,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .project_observations_read(
                self.actor_identity_id,
                self.scope,
                self.project_target()?,
                input,
            )
            .await
    }
    async fn project_charter(&self, _input: NoArguments) -> Result<Value, AgentHostError> {
        self.provider
            .project_charter_read(self.actor_identity_id, self.scope, self.project_target()?)
            .await
    }
    async fn skill_section(&self, input: SectionArguments) -> Result<Value, AgentHostError> {
        self.provider
            .project_skill_section_read(
                self.actor_identity_id,
                self.scope,
                self.project_target()?,
                input,
            )
            .await
    }
}

#[async_trait]
impl operation_registry::main_reads::MainReadContext<AgentHostError> for Context<'_> {
    async fn genesis_project_agents(
        &self,
        input: operation_registry::main_reads::GenesisProjectAgentsQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .main_queries
            .for_admission(self.admitted_authority)
            .project_agents(self.actor_identity_id, self.scope, input)
            .await
            .map_err(super::service_error)
    }
    async fn charter_read(
        &self,
        input: operation_registry::main_reads::CharterReadQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .main_queries
            .for_admission(self.admitted_authority)
            .charter_read(self.actor_identity_id, self.scope, input)
            .await
            .map_err(super::native_scope_error)
    }
    async fn charter_readiness(
        &self,
        input: operation_registry::main_reads::CharterProjectionQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .main_queries
            .for_admission(self.admitted_authority)
            .charter_readiness(self.actor_identity_id, self.scope, input)
            .await
            .map_err(super::service_error)
    }
    async fn charter_diff(
        &self,
        input: operation_registry::main_reads::CharterDiffQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .main_queries
            .for_admission(self.admitted_authority)
            .charter_diff(self.actor_identity_id, self.scope, input)
            .await
            .map_err(super::service_error)
    }
    async fn charter_approval_target(
        &self,
        input: operation_registry::main_reads::CharterProjectionQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .main_queries
            .for_admission(self.admitted_authority)
            .charter_approval_target(self.actor_identity_id, self.scope, input)
            .await
            .map_err(super::service_error)
    }
    async fn discovery_read(
        &self,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .discovery_read(self.actor_identity_id, self.scope, input)
            .await
    }
    async fn portfolio_read(
        &self,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .portfolio_read(self.actor_identity_id, self.scope, input)
            .await
    }
    async fn inquiry_run(
        &self,
        input: operation_registry::main_reads::InquiryQuery,
    ) -> Result<Value, AgentHostError> {
        self.provider
            .inquiry_run(self.actor_identity_id, self.scope, input)
            .await
    }
}
