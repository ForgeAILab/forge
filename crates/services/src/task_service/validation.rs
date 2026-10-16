use super::*;

pub(super) fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

/// A merge target branch reaches Git; demo mode refuses one Git could read as
/// an option before it is stored.
pub(super) fn serialize_merge_config(config: Option<Value>) -> Result<Option<String>> {
    if let Some(branch) = config
        .as_ref()
        .and_then(|config| config.get("target_branch"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|branch| !branch.is_empty())
    {
        crate::server_commands::validate_branch_name(branch)?;
    }
    serialize_config(config)
}

pub(super) fn serialize_config(config: Option<Value>) -> Result<Option<String>> {
    config
        .map(|value| serde_json::to_string(&value))
        .transpose()
        .map_err(|error| ServiceError::invalid_operation(format!("invalid JSON config: {error}")))
}
