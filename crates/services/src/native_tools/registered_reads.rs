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
#[async_trait]
impl operation_registry::project_reads::ProjectReadContext<AgentHostError> for Context<'_> {
    async fn project_charter(&self, _input: NoArguments) -> Result<Value, AgentHostError> {
        self.provider
            .project_charter_read(self.actor_identity_id, self.scope)
            .await
    }
    async fn skill_section(&self, input: SectionArguments) -> Result<Value, AgentHostError> {
        self.provider
            .project_skill_section_read(self.actor_identity_id, self.scope, input)
            .await
    }
}
