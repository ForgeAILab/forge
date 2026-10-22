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
                            || (matches!(key.as_str(), "scaffold" | "verify") && !value.is_null())
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
    // Environment assets are copied from absolute paths on the server.
    if value
        .get("environment")
        .and_then(|environment| environment.get("assets"))
        .is_some_and(|assets| assets.as_array().is_none_or(|items| !items.is_empty()))
        && !value["environment"]["assets"].is_null()
    {
        return Err(location_refused(
            "environment assets copy files from the server's filesystem",
        ));
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

impl From<executors::server_commands::ServerAgentsDisabled> for ServiceError {
    fn from(_: executors::server_commands::ServerAgentsDisabled) -> Self {
        Self::ServerAgentsDisabled
    }
}

pub fn location_refused(reason: &str) -> ServiceError {
    ServiceError::ServerLocationRefused {
        message: format!("{}: {reason}", executors::server_commands::LOCATION_MESSAGE),
    }
}

/// Demo rule for every repository input: no server filesystem path (local
/// path, `file://` or any non-https URL), and a remote that is `https://` to
/// a public address, checked with the provider-request address policy
/// (including DNS resolution). Normal mode accepts what it always accepted.
pub async fn validate_repository_input(
    remote_url: Option<&str>,
    local_path: Option<&str>,
    default_branch: Option<&str>,
) -> Result<()> {
    if !executors::server_commands::manager_only() {
        return Ok(());
    }
    if local_path.is_some_and(|path| !path.trim().is_empty()) {
        return Err(location_refused(
            "a path on the server's filesystem cannot be a repository",
        ));
    }
    if let Some(remote_url) = remote_url.map(str::trim).filter(|url| !url.is_empty()) {
        crate::provider_credentials::provider_url_addresses(remote_url)
            .await
            .map_err(|reason| location_refused(&reason.replace("base_url", "repository URL")))?;
    }
    if let Some(branch) = default_branch {
        validate_branch_name(branch)?;
    }
    Ok(())
}

/// Execution-time counterpart for stored rows (seed data, upgrades): the
/// server never runs Git against a refused repository.
pub async fn ensure_repository_allowed(repo: &db::Repo) -> Result<()> {
    validate_repository_input(
        repo.remote_url.as_deref(),
        repo.local_path.as_deref(),
        Some(&repo.default_branch),
    )
    .await
}

/// Demo rule for branch names Forge passes to Git: a name Git itself would
/// accept as a branch, never one that Git could read as an option.
pub fn validate_branch_name(branch: &str) -> Result<()> {
    if !executors::server_commands::manager_only() {
        return Ok(());
    }
    let name = branch.trim();
    let name = name.strip_prefix("refs/heads/").unwrap_or(name);
    if name.is_empty()
        || name == "@"
        || name.starts_with('-')
        || name.starts_with('/')
        || name.ends_with('/')
        || name.ends_with('.')
        || name.contains("..")
        || name.contains("@{")
        || name.contains("//")
        || name
            .chars()
            .any(|c| c.is_control() || " ~^:?*[\\".contains(c))
        || name
            .split('/')
            .any(|part| part.starts_with('.') || part.ends_with(".lock"))
    {
        return Err(location_refused(
            "the branch name is not a valid Git branch",
        ));
    }
    Ok(())
}

/// Demo rule for writes to files on the server's own filesystem.
pub fn ensure_server_files_allowed(reason: &str) -> Result<()> {
    if executors::server_commands::manager_only() {
        return Err(location_refused(reason));
    }
    Ok(())
}
