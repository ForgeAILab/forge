#[cfg(test)]
mod descriptors;
mod handlers;
pub(crate) mod registry;

use serde_json::Value;

use crate::{error::McpToolError, protocol::McpContext, state::AppState};

// MCP exposes only assignment of an Agent to the Task's effective implementation
// role. Arbitrary role selection and Agent↔human routing remain REST/user actions.
/// Runs a moved tool's handler without the registry admission in front of
/// it, so tests can prove the handler's own effect-time re-checks.
#[cfg(test)]
pub(crate) async fn dispatch_handler_only(
    state: &AppState,
    name: &str,
    arguments: Value,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let spec = operation_registry::mcp::lookup(name).expect("moved tool");
    let input = spec.decode(arguments).map_err(registry::input_error)?;
    registry::dispatch(state, input, context).await
}

pub(crate) async fn dispatch_tool(
    state: &AppState,
    name: &str,
    arguments: Value,
    context: &McpContext,
) -> Result<Value, McpToolError> {
    let spec = operation_registry::mcp::lookup(name)
        .ok_or_else(|| McpToolError::protocol(-32601, "method not found"))?;
    registry::authorize_resource(state, spec, &arguments, context).await?;
    let input = spec.decode(arguments).map_err(registry::input_error)?;
    registry::dispatch(state, input, context).await
}
