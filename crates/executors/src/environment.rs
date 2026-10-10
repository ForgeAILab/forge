//! Applying a Project's declared host environment to Task executions.
//!
//! [`api_types::ProjectEnvironment`] names what every execution needs from
//! its host: environment variables, git-ignored assets copied into the
//! worktree, and cheap preflight checks. The server applies it immediately
//! before an execution launches so an agent never spends a run rediscovering
//! a missing toolchain.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use api_types::{EnvironmentAsset, EnvironmentCheck, ProjectEnvironment};

/// Snapshot key carrying the Project environment variables to an executor.
///
/// Like the Task role, this is runtime authority stamped after the immutable
/// execution snapshot is loaded, never authored profile configuration.
pub const TASK_ENVIRONMENT_CONFIG_KEY: &str = "_forge_task_environment";

/// Bytes of check output retained from a preflight probe.
const CHECK_OUTPUT_TAIL_BYTES: usize = 4096;

/// Maximum runtime of a host environment check, including legacy settings.
pub const MAX_ENVIRONMENT_CHECK_TIMEOUT_SECONDS: u64 = 300;

fn check_timeout_seconds(check: &EnvironmentCheck) -> u64 {
    check
        .timeout_seconds
        .clamp(1, MAX_ENVIRONMENT_CHECK_TIMEOUT_SECONDS)
}

/// Makes sibling staging paths unique within this Forge process.
static ASSET_STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
    if !(60..=86400).contains(&environment.recheck_interval_seconds) {
        return Err("recheck_interval_seconds must be between 60 and 86400".to_owned());
    }
    for key in environment.env.keys() {
        let valid = !key.is_empty()
            && !key.starts_with(|c: char| c.is_ascii_digit())
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            return Err(format!("environment variable name {key:?} is not valid"));
        }
        let uppercase = key.to_ascii_uppercase();
        if uppercase.starts_with("FORGE_") || uppercase == "PWD" {
            return Err(format!("environment variable {key} is reserved by Forge"));
        }
    }
    let mut targets: Vec<(String, Vec<String>)> = Vec::new();
    for asset in &environment.assets {
        if !Path::new(&asset.source).is_absolute() {
            return Err(format!(
                "asset source {:?} must be an absolute host path",
                asset.source
            ));
        }
        let normalized = worktree_relative(&asset.target)?;
        let key = normalized
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_lowercase())
            .collect::<Vec<_>>();
        if let Some((other, _)) = targets
            .iter()
            .find(|(_, candidate)| key.starts_with(candidate) || candidate.starts_with(&key))
        {
            return Err(format!(
                "asset targets {:?} and {:?} overlap",
                other, asset.target
            ));
        }
        targets.push((asset.target.clone(), key));
    }
    let mut names = BTreeSet::new();
    for check in &environment.checks {
        if check.name.trim().is_empty() || check.name.trim() != check.name {
            return Err("every environment check needs a name".to_owned());
        }
        if check.command.trim().is_empty() {
            return Err(format!("environment check {} has no command", check.name));
        }
        if !(1..=MAX_ENVIRONMENT_CHECK_TIMEOUT_SECONDS).contains(&check.timeout_seconds) {
            return Err(format!(
                "environment check {} timeout_seconds must be between 1 and 300",
                check.name
            ));
        }
        if !names.insert(check.name.as_str()) {
            return Err(format!(
                "environment check {} is declared twice",
                check.name
            ));
        }
        let mut roles = BTreeSet::new();
        for role in &check.roles {
            if role.trim().is_empty() || role.trim() != role {
                return Err(format!(
                    "environment check {} has an invalid role",
                    check.name
                ));
            }
            if !roles.insert(role) {
                return Err(format!(
                    "environment check {} declares role {role} twice",
                    check.name
                ));
            }
        }
    }
    Ok(())
}

fn worktree_relative(target: &str) -> Result<PathBuf, String> {
    let path = Path::new(target);
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "asset target {target:?} must be a path inside the worktree"
                ));
            }
        }
    }
    if target.trim().is_empty() || normalized.as_os_str().is_empty() {
        return Err(format!(
            "asset target {target:?} must be a path inside the worktree"
        ));
    }
    Ok(normalized)
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
        let canonical_worktree = worktree.canonicalize().map_err(|error| {
            format!("failed to resolve worktree {}: {error}", worktree.display())
        })?;
        for asset in &assets {
            let source = Path::new(&asset.source);
            let source_metadata = source.symlink_metadata().map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    format!(
                        "asset source {} does not exist on the Forge host",
                        asset.source
                    )
                } else {
                    format!("failed to inspect asset source {}: {error}", asset.source)
                }
            })?;
            if source_metadata.file_type().is_symlink() {
                return Err(format!(
                    "asset source {} must not be a symbolic link",
                    asset.source
                ));
            }
            let canonical_source = source.canonicalize().map_err(|error| {
                format!("failed to resolve asset source {}: {error}", asset.source)
            })?;
            if canonical_source.starts_with(&canonical_worktree)
                || canonical_worktree.starts_with(&canonical_source)
            {
                return Err(format!(
                    "asset source {} must be outside the worktree and must not contain it",
                    asset.source
                ));
            }

            let relative_target = worktree_relative(&asset.target)?;
            let target = canonical_worktree.join(&relative_target);
            ensure_safe_target_parent(&canonical_worktree, &relative_target)?;
            match target.symlink_metadata() {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(format!("asset target {} is a symbolic link", asset.target));
                }
                Ok(_) => continue,
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to inspect asset target {}: {error}",
                        asset.target
                    ));
                }
            }

            let staging = staging_path(&target)?;
            let copy_result = copy_recursively(&canonical_source, &staging)
                .and_then(|()| std::fs::rename(&staging, &target));
            if let Err(error) = copy_result {
                remove_staging_path(&staging);
                return Err(format!(
                    "failed to copy asset {} to {}: {error}",
                    asset.source, asset.target
                ));
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("asset copy task failed: {error}"))?
}

fn copy_recursively(source: &Path, target: &Path) -> std::io::Result<()> {
    let metadata = source.symlink_metadata()?;
    if metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("symbolic link in asset source: {}", source.display()),
        ));
    }
    if metadata.is_dir() {
        std::fs::create_dir(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_recursively(&entry.path(), &target.join(entry.file_name()))?;
        }
        return Ok(());
    }
    if !metadata.is_file() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported asset source type: {}", source.display()),
        ));
    }
    std::fs::copy(source, target).map(|_| ())
}

fn ensure_safe_target_parent(worktree: &Path, target: &Path) -> Result<(), String> {
    let mut current = worktree.to_path_buf();
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    for component in parent.components() {
        let Component::Normal(part) = component else {
            return Err(format!(
                "asset target {:?} must be a path inside the worktree",
                target.display()
            ));
        };
        current.push(part);
        match current.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "asset target parent {} is a symbolic link",
                    current.display()
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "asset target parent {} is not a directory",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|error| {
                    format!(
                        "failed to create asset target parent {}: {error}",
                        current.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "failed to inspect asset target parent {}: {error}",
                    current.display()
                ));
            }
        }
    }
    Ok(())
}

fn staging_path(target: &Path) -> Result<PathBuf, String> {
    let parent = target
        .parent()
        .ok_or_else(|| format!("asset target {} has no parent", target.display()))?;
    let file_name = target
        .file_name()
        .ok_or_else(|| format!("asset target {} has no file name", target.display()))?
        .to_string_lossy();
    for _ in 0..100 {
        let nonce = ASSET_STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let staging = parent.join(format!(
            ".{file_name}.forge-asset-{}-{nonce}",
            std::process::id()
        ));
        match staging.symlink_metadata() {
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(staging),
            Ok(_) => {}
            Err(error) => {
                return Err(format!(
                    "failed to inspect asset staging path {}: {error}",
                    staging.display()
                ));
            }
        }
    }
    Err(format!(
        "could not allocate a staging path for asset target {}",
        target.display()
    ))
}

fn remove_staging_path(path: &Path) {
    let Ok(metadata) = path.symlink_metadata() else {
        return;
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        let _ = std::fs::remove_dir_all(path);
    } else {
        let _ = std::fs::remove_file(path);
    }
}

/// Redact declared environment values before command output is persisted.
///
/// Project settings are not a secret store, but this prevents an accidental
/// `echo "$TOKEN"` from copying a configured value into durable logs and
/// blocking annotations.
#[must_use]
pub fn redact_environment_values(text: &str, environment: &BTreeMap<String, String>) -> String {
    let mut values = environment
        .values()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    values.dedup();
    values.into_iter().fold(text.to_owned(), |redacted, value| {
        redacted.replace(value, "[REDACTED]")
    })
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

/// Outcome of a single check, including bounded, redacted output on success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentCheckResult {
    pub passed: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub output_tail: String,
}

/// Run one check independently of its role selector. Used by the owner's
/// on-demand re-check, which must report every configured check.
pub async fn run_environment_check(
    worktree: &Path,
    env: &BTreeMap<String, String>,
    check: &EnvironmentCheck,
) -> EnvironmentCheckResult {
    let run_scope =
        crate::sandbox::SandboxEnv::for_command(worktree, crate::sandbox::RunPurpose::Probe);
    let mut command = environment_check_command(worktree, env, check, run_scope.env());
    let outcome = tokio::time::timeout(
        Duration::from_secs(check_timeout_seconds(check)),
        command.output(),
    )
    .await;
    match outcome {
        Err(_) => EnvironmentCheckResult {
            passed: false,
            exit_code: None,
            timed_out: true,
            output_tail: String::new(),
        },
        Ok(Err(error)) => EnvironmentCheckResult {
            passed: false,
            exit_code: None,
            timed_out: false,
            output_tail: output_tail(&redact_environment_values(&error.to_string(), env)),
        },
        Ok(Ok(output)) => {
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            EnvironmentCheckResult {
                passed: output.status.success(),
                exit_code: output.status.code(),
                timed_out: false,
                output_tail: output_tail(&redact_environment_values(&text, env)),
            }
        }
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
        let result = run_environment_check(worktree, env, check).await;
        if !result.passed {
            return Some(EnvironmentCheckFailure {
                name: check.name.clone(),
                command: check.command.clone(),
                exit_code: result.exit_code,
                timed_out: result.timed_out,
                output_tail: result.output_tail,
            });
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

fn environment_check_command(
    worktree: &Path,
    env: &BTreeMap<String, String>,
    check: &EnvironmentCheck,
    sandbox: &crate::sandbox::SandboxEnv,
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("bash");
    command
        .args(["-lc", &check.command])
        .current_dir(worktree)
        .envs(env)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    crate::run_process::apply_sandboxed(&mut command, env, sandbox);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(name: &str, command: &str, roles: &[&str]) -> EnvironmentCheck {
        EnvironmentCheck {
            scope: Default::default(),
            name: name.to_owned(),
            command: command.to_owned(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            timeout_seconds: 10,
        }
    }

    #[test]
    fn environment_timeout_is_bounded_on_write_and_legacy_read() {
        let mut environment = ProjectEnvironment::default();
        let mut check = EnvironmentCheck {
            scope: Default::default(),
            name: "slow".to_owned(),
            command: "true".to_owned(),
            roles: Vec::new(),
            timeout_seconds: u64::MAX,
        };
        environment.checks.push(check.clone());
        assert!(validate_project_environment(&environment)
            .unwrap_err()
            .contains("timeout_seconds"));
        assert_eq!(check_timeout_seconds(&check), 300);
        check.timeout_seconds = 0;
        assert_eq!(check_timeout_seconds(&check), 1);
        environment.checks[0].timeout_seconds = 300;
        assert!(validate_project_environment(&environment).is_ok());
    }

    #[test]
    fn environment_recheck_interval_defaults_and_rejects_invalid_values() {
        let default: ProjectEnvironment = serde_json::from_str("{}").unwrap();
        assert_eq!(default.recheck_interval_seconds, 600);
        assert_eq!(ProjectEnvironment::default().recheck_interval_seconds, 600);
        for interval in [0, 5, 59, 86401, u64::MAX] {
            let environment = ProjectEnvironment {
                recheck_interval_seconds: interval,
                ..Default::default()
            };
            assert!(validate_project_environment(&environment)
                .unwrap_err()
                .contains("recheck_interval_seconds"));
        }
        for interval in [60, 600, 86400] {
            assert!(validate_project_environment(&ProjectEnvironment {
                recheck_interval_seconds: interval,
                ..Default::default()
            })
            .is_ok());
        }
        assert!(
            serde_json::from_str::<ProjectEnvironment>(r#"{"recheck_interval_seconds":-1}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<ProjectEnvironment>(r#"{"unknown":true}"#).is_err());
    }

    #[tokio::test]
    async fn individual_environment_check_keeps_success_output_and_ignores_roles() {
        let root = tempfile::TempDir::new().unwrap();
        let env = BTreeMap::from([("TOKEN".to_owned(), "private-value".to_owned())]);
        let result = run_environment_check(
            root.path(),
            &env,
            &check("browser", "printf '%s ready' \"$TOKEN\"", &["reviewer"]),
        )
        .await;
        assert!(result.passed);
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.output_tail, "[REDACTED] ready");
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

        let overlapping = ProjectEnvironment {
            assets: vec![
                EnvironmentAsset {
                    source: "/srv/art".to_owned(),
                    target: "vendor".to_owned(),
                },
                EnvironmentAsset {
                    source: "/srv/models".to_owned(),
                    target: "vendor/models".to_owned(),
                },
            ],
            ..ProjectEnvironment::default()
        };
        assert!(validate_project_environment(&overlapping)
            .expect_err("overlapping targets fail")
            .contains("overlap"));

        let duplicate_role = ProjectEnvironment {
            checks: vec![check("browser", "true", &["reviewer", "reviewer"])],
            ..ProjectEnvironment::default()
        };
        assert!(validate_project_environment(&duplicate_role).is_err());

        for key in ["forge_task_id", "pwd"] {
            let environment = ProjectEnvironment {
                env: BTreeMap::from([(key.to_owned(), "x".to_owned())]),
                ..ProjectEnvironment::default()
            };
            assert!(validate_project_environment(&environment).is_err(), "{key}");
        }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn assets_refuse_symlink_escape_and_remove_partial_copies() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let worktree = dir.path().join("worktree");
        let outside = dir.path().join("outside");
        let source = dir.path().join("source");
        std::fs::create_dir_all(&worktree).expect("worktree");
        std::fs::create_dir_all(&outside).expect("outside");
        std::fs::create_dir_all(&source).expect("source");
        std::fs::write(source.join("a.txt"), "copied first").expect("source file");
        symlink(&outside, worktree.join("escape")).expect("target symlink");

        let escape = [EnvironmentAsset {
            source: source.to_string_lossy().into_owned(),
            target: "escape/asset".to_owned(),
        }];
        let error = materialize_assets(&worktree, &escape)
            .await
            .expect_err("target symlink is refused");
        assert!(error.contains("symbolic link"), "{error}");
        assert!(!outside.join("asset").exists());

        std::fs::write(outside.join("secret.txt"), "secret").expect("outside file");
        symlink(outside.join("secret.txt"), source.join("z-link")).expect("source symlink");
        let partial = [EnvironmentAsset {
            source: source.to_string_lossy().into_owned(),
            target: "vendor".to_owned(),
        }];
        let error = materialize_assets(&worktree, &partial)
            .await
            .expect_err("source symlink is refused");
        assert!(error.contains("symbolic link"), "{error}");
        assert!(
            !worktree.join("vendor").exists(),
            "a failed copy must not leave a target that future runs skip"
        );
        assert!(
            std::fs::read_dir(&worktree)
                .expect("worktree reads")
                .all(|entry| !entry
                    .expect("entry reads")
                    .file_name()
                    .to_string_lossy()
                    .contains("forge-asset")),
            "staging paths are cleaned after failure"
        );
    }

    #[tokio::test]
    async fn asset_source_cannot_contain_or_live_in_the_worktree() {
        let dir = tempfile::tempdir().expect("tempdir");
        let worktree = dir.path().join("worktree");
        let source_inside = worktree.join("source");
        std::fs::create_dir_all(&source_inside).expect("worktree source");

        for source in [dir.path(), source_inside.as_path()] {
            let assets = [EnvironmentAsset {
                source: source.to_string_lossy().into_owned(),
                target: "vendor".to_owned(),
            }];
            let error = materialize_assets(&worktree, &assets)
                .await
                .expect_err("recursive source is refused");
            assert!(error.contains("outside the worktree"), "{error}");
        }
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

    #[tokio::test]
    async fn check_output_redacts_declared_environment_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let env = BTreeMap::from([("ACCESS_TOKEN".to_owned(), "very-secret".to_owned())]);
        let checks = [check("token", "printf '%s' \"$ACCESS_TOKEN\"; exit 1", &[])];
        let failure = run_environment_checks(dir.path(), &env, &checks, "coder")
            .await
            .expect("check fails");
        assert!(!failure.output_tail.contains("very-secret"));
        assert!(failure.output_tail.contains("[REDACTED]"));
    }
}

#[cfg(test)]
mod run_budget_tests {
    use super::*;
    #[test]
    fn launch_environment_preserves_project_and_fills_budget() {
        let temp = tempfile::tempdir().unwrap();
        let env =
            std::collections::BTreeMap::from([("CARGO_BUILD_JOBS".into(), "project-value".into())]);
        let check = EnvironmentCheck {
            name: "budget".into(),
            command: "cargo test".into(),
            scope: api_types::EnvironmentCheckScope::Workspace,
            roles: Vec::new(),
            timeout_seconds: 30,
        };
        let command = environment_check_command(
            temp.path(),
            &env,
            &check,
            &crate::sandbox::SandboxEnv::none(),
        );
        let envs: std::collections::BTreeMap<_, _> = command
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_owned(), v.to_owned())))
            .collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("CARGO_BUILD_JOBS")).unwrap(),
            "project-value"
        );
        let jobs = crate::run_process::machine_policy().get().build_jobs();
        for (key, default) in [
            ("RUST_TEST_THREADS", jobs.to_string()),
            ("MAKEFLAGS", format!("-j{jobs}")),
            ("CMAKE_BUILD_PARALLEL_LEVEL", jobs.to_string()),
            ("GOFLAGS", format!("-p={jobs}")),
        ] {
            assert_eq!(
                envs.get(std::ffi::OsStr::new(key)),
                Some(&std::env::var_os(key).unwrap_or(default.into()))
            );
        }
    }
}
