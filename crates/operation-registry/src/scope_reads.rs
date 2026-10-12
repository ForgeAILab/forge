//! Scope identity projections. No caller-controlled selector is admitted.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[async_trait::async_trait]
pub trait ScopeReadContext<E>: Send + Sync {
    async fn account_summary(&self, input: NoArguments) -> Result<Value, E>;
    async fn project_summary(&self, input: ProjectSummary) -> Result<Value, E>;
    async fn chat_summary(&self, input: NoArguments) -> Result<Value, E>;
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoArguments {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectSummary {
    pub project_id: Option<String>,
}
pub const IDS: &[&str] = &["account.summary", "agent_chat.summary", "project.summary"];

pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed(
            "project.summary",
            AuthorityRule {
                principal: authority::PrincipalRule::MainOrProjectScope,
                permissions: &[
                    ("project", "read_project"),
                    ("agent_chat", "read_agent_chat"),
                ],
                binding: "admitted scope",
            },
            EffectClass::Query,
            AvailabilityRule::Always,
            &[SurfaceBinding {
                native_aggregate: "forge_scope_read",
                projection: FieldProjection::ReadArguments,
            }],
            "Read a bounded Project summary.",
            "Main names an owned project_id; Project scope uses its binding.",
            &[StructuralConstraint::ClosedObject],
            |c, i| Box::pin(c.project_summary(i)),
        ),
        OperationSpec::typed(
            "account.summary",
            AuthorityRule {
                principal: authority::PrincipalRule::BoundIdentity,
                permissions: &[("account", "read_account")],
                binding: "current identity",
            },
            EffectClass::Query,
            AvailabilityRule::Always,
            &[SurfaceBinding {
                native_aggregate: "forge_scope_read",
                projection: FieldProjection::ReadArguments,
            }],
            "Read the current account identity.",
            "",
            &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.account_summary(input)),
        ),
        OperationSpec::typed(
            "agent_chat.summary",
            AuthorityRule {
                principal: authority::PrincipalRule::BoundIdentity,
                permissions: &[("agent_chat", "read_agent_chat")],
                binding: "bound chat",
            },
            EffectClass::Query,
            AvailabilityRule::Always,
            &[SurfaceBinding {
                native_aggregate: "forge_scope_read",
                projection: FieldProjection::ReadArguments,
            }],
            "Read the bound Agent Chat identity.",
            "",
            &[StructuralConstraint::ClosedObject],
            |context, input| Box::pin(context.chat_summary(input)),
        ),
    ]
}
