//! Applying a Project's declared host environment to Task executions.
//!
//! [`api_types::ProjectEnvironment`] names what every execution needs from
//! its host: environment variables, git-ignored assets copied into the
//! worktree, and cheap preflight checks. The server applies it immediately
//! before an execution launches so an agent never spends a run rediscovering
//! a missing toolchain.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use api_types::{EnvironmentAsset, EnvironmentCheck, ProjectEnvironment};

/// Snapshot key carrying the Project environment variables to an executor.
///
/// Like the Task role, this is runtime authority stamped after the immutable
/// execution snapshot is loaded, never authored profile configuration.
pub const TASK_ENVIRONMENT_CONFIG_KEY: &str = "_forge_task_environment";

/// Bytes of check output kept for the blocking annotation.
const CHECK_OUTPUT_TAIL_BYTES: usize = 4096;

/// Stamp the Project environment variables onto an in-memory executor config.
pub fn mark_task_environment(config: &mut serde_json::Value, env: &BTreeMap<String, String>) {
    let Some(object) = config.as_object_mut() else {
        return;
    };
    if env.is_empty() {
        object.remove(TASK_ENVIRONMENT_CONFIG_KEY);
        return;
    }
    object.insert(
        TASK_ENVIRONMENT_CONFIG_KEY.to_owned(),
        serde_json::to_value(env).unwrap_or_default(),
    );
}

/// The Project environment variables carried by an in-memory executor config.
#[must_use]
pub fn task_environment(config: &serde_json::Value) -> BTreeMap<String, String> {
    config
        .get(TASK_ENVIRONMENT_CONFIG_KEY)
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

/// Reject an environment Forge could not apply safely.
pub fn validate_project_environment(environment: &ProjectEnvironment) -> Result<(), String> {
    for key in environment.env.keys() {
        let valid = !key.is_empty()
            && !key.starts_with(|c: char| c.is_ascii_digit())
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            return Err(format!("environment variable name {key:?} is not valid"));
        }
        if key.starts_with("FORGE_") || key == "PWD" {
            return Err(format!("environment variable {key} is reserved by Forge"));
        }
    }
    for asset in &environment.assets {
        if !Path::new(&asset.source).is_absolute() {
            return Err(format!(
                "asset source {:?} must be an absolute host path",
                asset.source
            ));
        }
        worktree_relative(&asset.target)?;
    }
    let mut names = std::collections::BTreeSet::new();
    for check in &environment.checks {
        if check.name.trim().is_empty() {
            return Err("every environment check needs a name".to_owned());
        }
        if check.command.trim().is_empty() {
            return Err(format!("environment check {} has no command", check.name));
        }
        if check.timeout_seconds == 0 {
            return Err(format!(
                "environment check {} needs a positive timeout",
                check.name
            ));
        }
        if !names.insert(check.name.as_str()) {
            return Err(format!(
                "environment check {} is declared twice",
                check.name
            ));
        }
    }
    Ok(())
}

fn worktree_relative(target: &str) -> Result<PathBuf, String> {
    let path = Path::new(target);
    let escapes = path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    });
    if target.trim().is_empty() || escapes {
        return Err(format!(
            "asset target {target:?} must be a path inside the worktree"
        ));
    }
    Ok(path.to_path_buf())
}

/// Copy each asset whose target is absent into the worktree.
///
/// A present target is left alone: it is either tracked content or an earlier
/// copy, and overwriting it could dirty the worktree.
pub async fn materialize_assets(
    worktree: &Path,
    assets: &[EnvironmentAsset],
) -> Result<(), String> {
    let worktree = worktree.to_path_buf();
    let assets = assets.to_vec();
    tokio::task::spawn_blocking(move || {
        for asset in &assets {
            let target = worktree.join(worktree_relative(&asset.target)?);
            if target.symlink_metadata().is_ok() {
                continue;
            }
            let source = Path::new(&asset.source);
            if !source.exists() {
                return Err(format!(
                    "asset source {} does not exist on the Forge host",
                    asset.source
                ));
            }
            copy_recursively(source, &target).map_err(|error| {
                format!(
                    "failed to copy asset {} to {}: {error}",
                    asset.source, asset.target
                )
            })?;
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("asset copy task failed: {error}"))?
}

fn copy_recursively(source: &Path, target: &Path) -> std::io::Result<()> {
    if source.is_dir() {
        std::fs::create_dir_all(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_recursively(&entry.path(), &target.join(entry.file_name()))?;
        }
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(source, target).map(|_| ())
}

/// A preflight check that did not pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentCheckFailure {
    pub name: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub output_tail: String,
}

impl EnvironmentCheckFailure {
    #[must_use]
    pub fn message(&self) -> String {
        let outcome = if self.timed_out {
            "timed out".to_owned()
        } else {
            match self.exit_code {
                Some(code) => format!("exited {code}"),
                None => "was killed".to_owned(),
            }
        };
        format!(
            "environment check '{}' {outcome}: {}",
            self.name, self.command
        )
    }
}

/// Run the checks gating `role`, in order, stopping at the first failure.
pub async fn run_environment_checks(
    worktree: &Path,
    env: &BTreeMap<String, String>,
    checks: &[EnvironmentCheck],
    role: &str,
) -> Option<EnvironmentCheckFailure> {
    for check in checks.iter().filter(|check| check.applies_to(role)) {
        let mut command = tokio::process::Command::new("bash");
        command
            .args(["-lc", &check.command])
            .current_dir(worktree)
            .envs(env)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        let outcome =
            tokio::time::timeout(Duration::from_secs(check.timeout_seconds), command.output())
                .await;
        let failure = |exit_code, timed_out, output_tail| EnvironmentCheckFailure {
            name: check.name.clone(),
            command: check.command.clone(),
            exit_code,
            timed_out,
            output_tail,
        };
        match outcome {
            Err(_) => return Some(failure(None, true, String::new())),
            Ok(Err(error)) => return Some(failure(None, false, error.to_string())),
            Ok(Ok(output)) if !output.status.success() => {
                let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                text.push_str(&String::from_utf8_lossy(&output.stderr));
                return Some(failure(output.status.code(), false, output_tail(&text)));
            }
            Ok(Ok(_)) => {}
        }
    }
    None
}

fn output_tail(text: &str) -> String {
    if text.len() <= CHECK_OUTPUT_TAIL_BYTES {
        return text.to_owned();
    }
    let mut start = text.len() - CHECK_OUTPUT_TAIL_BYTES;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(name: &str, command: &str, roles: &[&str]) -> EnvironmentCheck {
        EnvironmentCheck {
            name: name.to_owned(),
            command: command.to_owned(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            timeout_seconds: 10,
        }
    }

    #[test]
    fn environment_marker_round_trips_and_clears() {
        let mut config = serde_json::json!({ "executor_type": "codex" });
        let env = BTreeMap::from([("GODOT_BIN".to_owned(), "/opt/godot".to_owned())]);
        mark_task_environment(&mut config, &env);
        assert_eq!(task_environment(&config), env);

        mark_task_environment(&mut config, &BTreeMap::new());
        assert!(config.get(TASK_ENVIRONMENT_CONFIG_KEY).is_none());
        assert!(task_environment(&config).is_empty());
    }

    #[test]
    fn validation_rejects_unsafe_declarations() {
        let asset = |source: &str, target: &str| ProjectEnvironment {
            assets: vec![EnvironmentAsset {
                source: source.to_owned(),
                target: target.to_owned(),
            }],
            ..ProjectEnvironment::default()
        };
        assert!(validate_project_environment(&asset("/srv/art", "assets/vendor")).is_ok());
        assert!(validate_project_environment(&asset("srv/art", "assets")).is_err());
        assert!(validate_project_environment(&asset("/srv/art", "../outside")).is_err());
        assert!(validate_project_environment(&asset("/srv/art", "/abs")).is_err());

        for key in ["FORGE_TASK_ID", "PWD", "1BAD", "BAD-NAME", ""] {
            let environment = ProjectEnvironment {
                env: BTreeMap::from([(key.to_owned(), "x".to_owned())]),
                ..ProjectEnvironment::default()
            };
            assert!(validate_project_environment(&environment).is_err(), "{key}");
        }

        let duplicate = ProjectEnvironment {
            checks: vec![check("godot", "true", &[]), check("godot", "true", &[])],
            ..ProjectEnvironment::default()
        };
        assert!(validate_project_environment(&duplicate).is_err());
    }

    #[tokio::test]
    async fn assets_are_copied_only_when_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("source");
        std::fs::create_dir_all(source.join("nested")).expect("source dir");
        std::fs::write(source.join("nested/a.png"), "art").expect("source file");
        let worktree = dir.path().join("worktree");
        std::fs::create_dir_all(worktree.join("kept")).expect("worktree");
        std::fs::write(worktree.join("kept/a.png"), "tracked").expect("tracked file");

        let assets = vec![
            EnvironmentAsset {
                source: source.to_string_lossy().into_owned(),
                target: "vendor/limezu".to_owned(),
            },
            EnvironmentAsset {
                source: source.to_string_lossy().into_owned(),
                target: "kept".to_owned(),
            },
        ];
        materialize_assets(&worktree, &assets)
            .await
            .expect("copies");

        assert_eq!(
            std::fs::read_to_string(worktree.join("vendor/limezu/nested/a.png")).expect("copied"),
            "art"
        );
        assert_eq!(
            std::fs::read_to_string(worktree.join("kept/a.png")).expect("kept"),
            "tracked"
        );
        assert!(!worktree.join("kept/nested").exists());

        let missing = vec![EnvironmentAsset {
            source: dir.path().join("absent").to_string_lossy().into_owned(),
            target: "absent".to_owned(),
        }];
        let error = materialize_assets(&worktree, &missing)
            .await
            .expect_err("missing source fails");
        assert!(error.contains("does not exist"), "{error}");
    }

    #[tokio::test]
    async fn checks_gate_their_roles_and_see_project_env() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = BTreeMap::from([("PROJECT_TOOL".to_owned(), "present".to_owned())]);
        let checks = vec![
            check("tool", "test \"$PROJECT_TOOL\" = present", &[]),
            check("browser", "echo no chromium >&2; exit 3", &["reviewer"]),
        ];

        assert_eq!(
            run_environment_checks(dir.path(), &env, &checks, "coder").await,
            None
        );
        let failure = run_environment_checks(dir.path(), &env, &checks, "reviewer")
            .await
            .expect("reviewer check fails");
        assert_eq!(failure.name, "browser");
        assert_eq!(failure.exit_code, Some(3));
        assert!(failure.output_tail.contains("no chromium"));
        assert!(failure.message().contains("'browser' exited 3"));

        let failure = run_environment_checks(dir.path(), &BTreeMap::new(), &checks[..1], "coder")
            .await
            .expect("missing env fails");
        assert_eq!(failure.name, "tool");
    }

    #[tokio::test]
    async fn checks_time_out() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut slow = check("slow", "sleep 5", &[]);
        slow.timeout_seconds = 1;
        let failure = run_environment_checks(dir.path(), &BTreeMap::new(), &[slow], "coder")
            .await
            .expect("times out");
        assert!(failure.timed_out);
        assert!(failure.message().contains("timed out"));
    }
}
