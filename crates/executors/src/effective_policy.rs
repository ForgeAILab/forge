use crate::ExecutorKind;
use api_types::EffectiveExecutionPolicy;
use serde_json::Value;
use std::path::{Component, Path, PathBuf};

pub fn resolve_effective_policy(
    executor_kind: &ExecutorKind,
    config_snapshot: &Value,
    workspace_path: Option<&str>,
    workspace_root: Option<&str>,
) -> EffectiveExecutionPolicy {
    let config = config_snapshot.get("config").unwrap_or(config_snapshot);
    let permission_policy = config_string(config, config_snapshot, "permission_policy")
        .unwrap_or_else(|| "unknown".to_owned());
    let isolation_posture = resolve_isolation_posture(executor_kind, config, config_snapshot);
    let yolo_high_risk = permission_policy == "yolo";
    let codex_high_risk =
        matches!(executor_kind, ExecutorKind::Codex) && isolation_posture == "danger-full-access";
    let claude_code_high_risk = matches!(executor_kind, ExecutorKind::ClaudeCode)
        && config_bool(config, config_snapshot, "dangerously_skip_permissions") == Some(true);
    let cursor_high_risk =
        matches!(executor_kind, ExecutorKind::Cursor) && isolation_posture == "force";
    let is_high_risk =
        yolo_high_risk || codex_high_risk || claude_code_high_risk || cursor_high_risk;

    EffectiveExecutionPolicy {
        executor_kind: executor_kind.to_string(),
        permission_policy,
        isolation_posture,
        is_high_risk,
        effective_cwd: workspace_path.map(str::to_owned),
        workspace_root: workspace_root.map(str::to_owned),
        environment_posture: "inherited".to_owned(),
        scoped_tools: collect_string_values(config, config_snapshot, "scoped_tools"),
        mcp_servers: collect_string_values(config, config_snapshot, "mcp_servers"),
    }
}

pub fn validate_workspace_policy(
    effective_cwd: Option<&str>,
    workspace_root: Option<&str>,
    isolation_posture: &str,
) -> Result<(), WorkspacePolicyError> {
    let cwd = effective_cwd
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| WorkspacePolicyError::PathResolutionFailed {
            path: effective_cwd.unwrap_or_default().to_owned(),
            reason: "effective cwd is required".to_owned(),
        })?;

    if isolation_posture.contains("workspace-write")
        && workspace_root
            .map(|path| path.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(WorkspacePolicyError::MissingWorkspaceRoot);
    }

    let canonical_cwd = resolve_path(cwd)?;
    let Some(root) = workspace_root.filter(|path| !path.trim().is_empty()) else {
        return Ok(());
    };
    let canonical_root = resolve_path(root)?;

    if !canonical_cwd.starts_with(&canonical_root) {
        return Err(WorkspacePolicyError::CwdOutsideWorkspace {
            cwd: canonical_cwd.display().to_string(),
            workspace_root: canonical_root.display().to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WorkspacePolicyError {
    #[error("cwd outside workspace: {cwd} is not under {workspace_root}")]
    CwdOutsideWorkspace { cwd: String, workspace_root: String },
    #[error("workspace-write isolation requires a workspace root")]
    MissingWorkspaceRoot,
    #[error("failed to resolve path {path}: {reason}")]
    PathResolutionFailed { path: String, reason: String },
}

fn resolve_isolation_posture(
    executor_kind: &ExecutorKind,
    config: &Value,
    config_snapshot: &Value,
) -> String {
    match executor_kind {
        ExecutorKind::Embedded => "task_workspace".to_owned(),
        ExecutorKind::Codex
            if config_string(config, config_snapshot, "permission_policy").as_deref()
                == Some("yolo") =>
        {
            "danger-full-access".to_owned()
        }
        ExecutorKind::Codex => config_string(config, config_snapshot, "sandbox")
            .unwrap_or_else(|| "not_applicable".to_owned()),
        ExecutorKind::ClaudeCode => {
            if config_bool(config, config_snapshot, "dangerously_skip_permissions") == Some(true)
                || config_string(config, config_snapshot, "permission_policy").as_deref()
                    == Some("yolo")
            {
                "dangerously_skip_permissions".to_owned()
            } else {
                "standard".to_owned()
            }
        }
        ExecutorKind::Cursor => {
            if config_string(config, config_snapshot, "permission_policy").as_deref()
                == Some("yolo")
                || config_bool(config, config_snapshot, "force").unwrap_or_else(|| {
                    config_string(config, config_snapshot, "permission_policy").as_deref()
                        != Some("plan")
                })
            {
                "force".to_owned()
            } else {
                "propose_only".to_owned()
            }
        }
        ExecutorKind::Opencode | ExecutorKind::Gemini | ExecutorKind::Smith
            if config_string(config, config_snapshot, "permission_policy").as_deref()
                == Some("yolo") =>
        {
            "yolo".to_owned()
        }
        ExecutorKind::Shell
        | ExecutorKind::Opencode
        | ExecutorKind::Gemini
        | ExecutorKind::Smith
        | ExecutorKind::Null => "not_applicable".to_owned(),
    }
}

fn config_string(config: &Value, config_snapshot: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .or_else(|| config_snapshot.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn config_bool(config: &Value, config_snapshot: &Value, key: &str) -> Option<bool> {
    config
        .get(key)
        .or_else(|| config_snapshot.get(key))
        .and_then(Value::as_bool)
}

fn collect_string_values(config: &Value, config_snapshot: &Value, key: &str) -> Vec<String> {
    let Some(value) = config.get(key).or_else(|| config_snapshot.get(key)) else {
        return Vec::new();
    };

    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        Value::Object(items) => items.keys().cloned().collect(),
        Value::String(item) => vec![item.clone()],
        _ => Vec::new(),
    }
}

fn resolve_path(path: &str) -> Result<PathBuf, WorkspacePolicyError> {
    let path_ref = Path::new(path);
    match std::fs::canonicalize(path_ref) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => absolute_lexical(path_ref)
            .map_err(|error| WorkspacePolicyError::PathResolutionFailed {
                path: path.to_owned(),
                reason: error.to_string(),
            }),
        Err(error) => Err(WorkspacePolicyError::PathResolutionFailed {
            path: path.to_owned(),
            reason: error.to_string(),
        }),
    }
}

fn absolute_lexical(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    Ok(normalize_lexical(&absolute))
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_codex_effective_policy() {
        let policy = resolve_effective_policy(
            &ExecutorKind::Codex,
            &json!({
                "permission_policy": "supervised",
                "sandbox": "workspace-write",
                "scoped_tools": ["apply_patch"],
                "mcp_servers": ["filesystem"]
            }),
            Some("/tmp/workspace/task/repo"),
            Some("/tmp/workspace"),
        );

        assert_eq!(policy.executor_kind, "codex");
        assert_eq!(policy.permission_policy, "supervised");
        assert_eq!(policy.isolation_posture, "workspace-write");
        assert!(!policy.is_high_risk);
        assert_eq!(
            policy.effective_cwd.as_deref(),
            Some("/tmp/workspace/task/repo")
        );
        assert_eq!(policy.workspace_root.as_deref(), Some("/tmp/workspace"));
        assert_eq!(policy.environment_posture, "inherited");
        assert_eq!(policy.scoped_tools, vec!["apply_patch"]);
        assert_eq!(policy.mcp_servers, vec!["filesystem"]);
    }

    #[test]
    fn resolves_claude_code_effective_policy() {
        let policy = resolve_effective_policy(
            &ExecutorKind::ClaudeCode,
            &json!({
                "config": {
                    "permission_policy": "auto",
                    "dangerously_skip_permissions": false
                }
            }),
            Some("/tmp/workspace/task/repo"),
            Some("/tmp/workspace"),
        );

        assert_eq!(policy.executor_kind, "claude_code");
        assert_eq!(policy.permission_policy, "auto");
        assert_eq!(policy.isolation_posture, "standard");
        assert!(!policy.is_high_risk);
    }

    #[test]
    fn resolves_shell_effective_policy() {
        let policy = resolve_effective_policy(
            &ExecutorKind::Shell,
            &json!({ "permission_policy": "plan" }),
            Some("/tmp/workspace/task/repo"),
            Some("/tmp/workspace"),
        );

        assert_eq!(policy.executor_kind, "shell");
        assert_eq!(policy.permission_policy, "plan");
        assert_eq!(policy.isolation_posture, "not_applicable");
        assert!(!policy.is_high_risk);
    }

    #[test]
    fn resolves_opencode_effective_policy() {
        let policy = resolve_effective_policy(
            &ExecutorKind::Opencode,
            &json!({ "permission_policy": "auto" }),
            Some("/tmp/workspace/task/repo"),
            Some("/tmp/workspace"),
        );

        assert_eq!(policy.executor_kind, "opencode");
        assert_eq!(policy.permission_policy, "auto");
        assert_eq!(policy.isolation_posture, "not_applicable");
        assert!(!policy.is_high_risk);
    }

    #[test]
    fn marks_codex_danger_full_access_high_risk() {
        let policy = resolve_effective_policy(
            &ExecutorKind::Codex,
            &json!({
                "permission_policy": "auto",
                "sandbox": "danger-full-access"
            }),
            None,
            None,
        );

        assert_eq!(policy.isolation_posture, "danger-full-access");
        assert!(policy.is_high_risk);
    }

    #[test]
    fn marks_claude_code_skip_permissions_high_risk() {
        let policy = resolve_effective_policy(
            &ExecutorKind::ClaudeCode,
            &json!({
                "permission_policy": "auto",
                "dangerously_skip_permissions": true
            }),
            None,
            None,
        );

        assert_eq!(policy.isolation_posture, "dangerously_skip_permissions");
        assert!(policy.is_high_risk);
    }

    #[test]
    fn yolo_policy_derives_full_access_and_high_risk() {
        let policy = resolve_effective_policy(
            &ExecutorKind::Codex,
            &json!({ "permission_policy": "yolo", "sandbox": "read-only" }),
            Some("/tmp/workspace/task/repo"),
            Some("/tmp/workspace"),
        );

        assert_eq!(policy.permission_policy, "yolo");
        assert_eq!(policy.isolation_posture, "danger-full-access");
        assert!(policy.is_high_risk);
    }

    #[test]
    fn validates_cwd_inside_workspace() {
        let workspace = tempfile::tempdir().expect("workspace root creates");
        let cwd = workspace.path().join("task").join("repo");
        std::fs::create_dir_all(&cwd).expect("cwd creates");

        let result =
            validate_workspace_policy(cwd.to_str(), workspace.path().to_str(), "workspace-write");

        assert!(result.is_ok());
    }

    #[test]
    fn rejects_cwd_outside_workspace() {
        let workspace = tempfile::tempdir().expect("workspace root creates");
        let outside = tempfile::tempdir().expect("outside root creates");

        let result = validate_workspace_policy(
            outside.path().to_str(),
            workspace.path().to_str(),
            "workspace-write",
        );

        assert!(matches!(
            result,
            Err(WorkspacePolicyError::CwdOutsideWorkspace { .. })
        ));
    }

    #[test]
    fn rejects_workspace_write_without_root() {
        let workspace = tempfile::tempdir().expect("workspace root creates");

        let result = validate_workspace_policy(workspace.path().to_str(), None, "workspace-write");

        assert_eq!(result, Err(WorkspacePolicyError::MissingWorkspaceRoot));
    }
}
