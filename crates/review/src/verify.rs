//! Review verify bundles use the unchanged canonical runner policy. The owner
//! supplies runtime inputs only after admission; this module never spawns.
use api_types::*;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

pub const ENV_KEYS: [&str; 5] = [
    "FORGE_VERIFY_PORT",
    "FORGE_VERIFY_URL",
    "FORGE_VERIFY_DIR",
    "FORGE_VERIFY_REQUEST",
    "FORGE_VERIFY_REPORT",
];

pub fn config(state: &Value) -> Result<Option<ReviewVerifyConfig>, String> {
    validate_review_verify_config(state)?;
    state
        .get("review")
        .unwrap_or(state)
        .get("verify")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value(v.clone()).map_err(|e| format!("verify: {e}")))
        .transpose()
}

/// The attempt and evaluation are in every command identity. A new evaluation
/// cannot join the completed uncacheable run of an earlier one.
pub fn build_bundle(
    config: &ReviewVerifyConfig,
    workspace_id: &str,
    generation: u64,
    attempt: i64,
    evaluation: u32,
    project_keys: impl Iterator<Item = String>,
) -> Result<CheckSpec, String> {
    config.validate()?;
    if attempt < 1 {
        return Err("verify needs a positive review attempt".into());
    }
    let mut keys: BTreeSet<_> = project_keys.collect();
    keys.extend(ENV_KEYS.map(str::to_owned));
    let mut commands = Vec::new();
    let prefix = format!("verify:{attempt}:{evaluation}:");
    let mut add = |name: &str, shell_text: String, timeout| {
        commands.push(CheckCommandSpec {
            id: format!("{prefix}{name}"),
            shell_text,
            shell: "bash -lc".into(),
            working_directory: CheckWorkingDirectory::TaskRoot,
            environment_keys: keys.clone(),
            timeout_seconds: Some(timeout),
            failure_policy: CheckFailurePolicy::StopBundle,
            cacheability: CheckCacheability::Uncacheable,
            requirement_ids: BTreeSet::new(),
        });
    };
    if let Some(start) = &config.start {
        add(
            "start",
            format!(
                r#"( bash -lc {}; code=$?; printf '%s\n' "$code" > "$FORGE_VERIFY_DIR/start.exit" ) > "$FORGE_VERIFY_DIR/app.log" 2>&1 < /dev/null &
printf '%s\n' "$!" > "$FORGE_VERIFY_DIR/start.pid"
sleep 0.05
[ ! -f "$FORGE_VERIFY_DIR/start.exit" ]"#,
                quote(start)
            ),
            10,
        );
    }
    if config.ready_path.is_some() || config.ready_command.is_some() {
        let probe = if let Some(command) = &config.ready_command {
            format!("bash -lc {}", quote(command))
        } else {
            format!(
                r#"( exec 3<>/dev/tcp/127.0.0.1/"$FORGE_VERIFY_PORT" || exit 1
printf 'GET %s HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' {} >&3
IFS=' ' read -r -t 2 protocol status rest <&3 || exit 1
[[ "$protocol" == HTTP/* && "$status" =~ ^[1-4][0-9][0-9]$ ]] ) 2>/dev/null"#,
                quote(config.ready_path.as_deref().unwrap_or("/"))
            )
        };
        add(
            "ready",
            format!(
                r#"while [ ! -f "$FORGE_VERIFY_DIR/start.exit" ]; do
if {probe}; then exit 0; fi
sleep 0.2
done
exit 1"#
            ),
            u64::from(config.ready_timeout_seconds.unwrap_or(120)),
        );
    }
    for check in &config.checks {
        add(
            &format!("check:{}", check.name),
            format!(
                r#"export FORGE_VERIFY_REPORT="$FORGE_VERIFY_DIR/{}.report.json"
[ ! -f "$FORGE_VERIFY_DIR/start.exit" ] || exit 1
bash -lc {}
code=$?
[ ! -f "$FORGE_VERIFY_DIR/start.exit" ] || exit 1
exit "$code""#,
                check.name,
                quote(&check.command)
            ),
            u64::from(check.timeout_seconds.unwrap_or(600)),
        );
    }
    let spec = CheckSpec {
        schema_revision: CHECK_SPEC_REVISION,
        scope: CheckScope::Workspace {
            workspace_id: workspace_id.into(),
            generation,
        },
        configured_commands: commands.len(),
        commands,
        blank_commands: vec![],
        declares_cleanup: false,
        execution_policy: CANONICAL_CI_POLICY.into(),
    };
    spec.validate()?;
    Ok(spec)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Only Forge's generated, uncacheable canonical bundle receives runtime
/// inputs. All commands must name the same positive attempt/evaluation.
pub fn bundle_attempt(spec: &CheckSpec) -> Result<Option<i64>, String> {
    let Some(first) = spec
        .commands
        .first()
        .filter(|c| c.id.starts_with("verify:"))
    else {
        return Ok(None);
    };
    let mut parts = first.id.splitn(4, ':');
    parts.next();
    let attempt: i64 = parts
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|_| "invalid verify attempt")?;
    let evaluation: u32 = parts
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|_| "invalid verify evaluation")?;
    let prefix = format!("verify:{attempt}:{evaluation}:");
    if attempt < 1
        || spec.execution_policy != CANONICAL_CI_POLICY
        || spec
            .commands
            .iter()
            .any(|c| !c.id.starts_with(&prefix) || c.cacheability != CheckCacheability::Uncacheable)
    {
        return Err("invalid verify bundle".into());
    }
    Ok(Some(attempt))
}

pub fn directory(worktree: &Path, attempt: i64) -> Result<PathBuf, String> {
    if attempt < 1 {
        return Err("invalid verify attempt".into());
    }
    let root =
        executors::sandbox::TaskRoot::of_worktree(worktree).ok_or("verify needs a Task root")?;
    Ok(root
        .path()
        .join(".forge-task/verify")
        .join(attempt.to_string()))
}

/// Called by the already-admitted server check owner, under its checkout lock.
/// This performs no command execution. Re-execution clears only known output
/// files so reports from an inconclusive evaluation cannot answer the next one.
pub fn prepare_environment(
    spec: &CheckSpec,
    worktree: &Path,
) -> Result<BTreeMap<String, String>, String> {
    let Some(attempt) = bundle_attempt(spec)? else {
        return Ok(BTreeMap::new());
    };
    let directory = directory(worktree, attempt)?;
    ensure_directory(&directory, worktree)?;
    for file in ["start.exit", "start.pid", "app.log"]
        .into_iter()
        .map(str::to_owned)
        .chain(spec.commands.iter().filter_map(|c| {
            c.id.split_once(":check:")
                .map(|(_, name)| format!("{name}.report.json"))
        }))
    {
        let path = directory.join(file);
        if path.exists() || path.symlink_metadata().is_ok() {
            std::fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    Ok(BTreeMap::from([
        ("FORGE_VERIFY_PORT".into(), port.to_string()),
        (
            "FORGE_VERIFY_URL".into(),
            format!("http://127.0.0.1:{port}"),
        ),
        (
            "FORGE_VERIFY_DIR".into(),
            directory.to_string_lossy().into_owned(),
        ),
        (
            "FORGE_VERIFY_REQUEST".into(),
            directory
                .join("request.json")
                .to_string_lossy()
                .into_owned(),
        ),
        (
            "FORGE_VERIFY_REPORT".into(),
            directory.join("report.json").to_string_lossy().into_owned(),
        ),
    ]))
}

/// Refuse symlinked reserved directories before writing or reading any inputs.
pub fn ensure_directory(directory: &Path, worktree: &Path) -> Result<(), String> {
    let root =
        executors::sandbox::TaskRoot::of_worktree(worktree).ok_or("verify needs a Task root")?;
    let relative = directory
        .strip_prefix(root.path())
        .map_err(|_| "verify directory escapes Task root")?;
    let mut path = root.path().to_owned();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err("invalid verify directory".into());
        }
        path.push(component);
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
            Ok(_) => return Err("verify directory is not a real directory".into()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&path).map_err(|e| e.to_string())?
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

pub fn prompt(details: &ReviewVerifyDetails) -> String {
    let mut out = format!("\n\nVerify results:\nOverall: {:?}\n", details.status);
    if let Some(reason) = &details.reason {
        out.push_str(&format!("Reason: {reason}\n"));
    }
    for check in &details.checks {
        out.push_str(&format!(
            "- {}: {:?} — {}\n",
            check.name, check.result, check.summary
        ));
        for path in &check.evidence_paths {
            out.push_str(&format!("  Evidence file: {path}\n"));
        }
        for id in &check.evidence {
            out.push_str(&format!("  Evidence media: {id}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn review_verify_bundle_is_uncacheable_canonical_and_ordered() {
        let config:ReviewVerifyConfig=serde_json::from_value(json!({"start":"run app","ready_command":"ready","checks":[{"name":"smoke","command":"verify"},{"name":"save","command":"save","timeout_seconds":8}]})).unwrap();
        let spec = build_bundle(
            &config,
            "workspace",
            3,
            7,
            0,
            vec!["PROJECT_KEY".to_owned()].into_iter(),
        )
        .unwrap();
        assert_eq!(spec.execution_policy, CANONICAL_CI_POLICY);
        assert!(!spec.cacheable());
        assert_eq!(bundle_attempt(&spec).unwrap(), Some(7));
        assert_eq!(
            spec.commands
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            [
                "verify:7:0:start",
                "verify:7:0:ready",
                "verify:7:0:check:smoke",
                "verify:7:0:check:save"
            ]
        );
        assert_eq!(
            spec.commands
                .iter()
                .map(|c| c.timeout_seconds)
                .collect::<Vec<_>>(),
            [Some(10), Some(120), Some(600), Some(8)]
        );
        assert!(spec
            .commands
            .iter()
            .all(|c| c.cacheability == CheckCacheability::Uncacheable
                && ENV_KEYS.iter().all(|key| c.environment_keys.contains(*key))));
        assert_ne!(
            spec,
            build_bundle(&config, "workspace", 3, 7, 1, std::iter::empty()).unwrap()
        );
    }
    #[test]
    fn review_verify_directory_refuses_symlinked_reserved_components() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("task/repo");
        std::fs::create_dir_all(&path).unwrap();
        executors::sandbox::TaskRoot::reserve(path.parent().unwrap()).unwrap();
        let dir = directory(&path, 1).unwrap();
        ensure_directory(&dir, &path).unwrap();
        #[cfg(unix)]
        {
            std::fs::remove_dir(&dir).unwrap();
            std::os::unix::fs::symlink(temp.path(), &dir).unwrap();
            assert!(ensure_directory(&dir, &path).is_err());
        }
    }
}
