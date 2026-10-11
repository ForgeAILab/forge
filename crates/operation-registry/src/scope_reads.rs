//! Scope identity projections. No caller-controlled selector is admitted.
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[async_trait::async_trait]
pub trait ScopeReadContext<E>: Send + Sync {
    async fn account_summary(&self, input: NoArguments) -> Result<Value, E>;
    async fn chat_summary(&self, input: NoArguments) -> Result<Value, E>;
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoArguments {}

pub const IDS: &[&str] = &["account.summary", "agent_chat.summary"];

pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
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
