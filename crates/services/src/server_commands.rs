//! Shared REST/MCP/domain validation of command-bearing configuration.
use crate::{Result, ServiceError};
use serde_json::Value;

/// Validate opaque Project settings, workflow defaults/bindings and Task
/// overrides alike, including commands inside retained unknown fields.
pub fn validate_configuration(value: &Value) -> Result<()> {
    if executors::server_commands::ensure_allowed().is_ok() {
        return Ok(());
    }
    fn commands(value: &Value) -> bool {
        match value {
            Value::Object(object) => {
                object.get("type").and_then(Value::as_str) == Some("script")
                    || object.iter().any(|(key, value)| {
                        (matches!(
                            key.as_str(),
                            "ci_steps" | "setup_steps" | "command_allowlist"
                        ) && value.as_array().is_none_or(|items| !items.is_empty())
                            && !value.is_null())
                            || (key == "command"
                                && value.as_str().is_some_and(|s| !s.trim().is_empty()))
                            || (key == "scaffold" && !value.is_null())
                            || commands(value)
                    })
            }
            Value::Array(items) => items.iter().any(commands),
            _ => false,
        }
    }
    if commands(value) {
        return Err(ServiceError::ServerCommandsDisabled);
    }
    Ok(())
}

impl From<executors::server_commands::ServerCommandsDisabled> for ServiceError {
    fn from(_: executors::server_commands::ServerCommandsDisabled) -> Self {
        Self::ServerCommandsDisabled
    }
}

/// Stored/domain Task inputs are JSON text. Normal mode preserves its existing
/// parsing behavior; demo mode validates before a creation path can retain it.
pub fn validate_serialized_configuration(value: Option<&str>) -> Result<()> {
    if executors::server_commands::ensure_allowed().is_ok() {
        return Ok(());
    }
    if let Some(value) = value {
        let value = serde_json::from_str(value).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid configuration: {error}"))
        })?;
        validate_configuration(&value)?;
    }
    Ok(())
}
