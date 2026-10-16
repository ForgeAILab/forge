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
                r#"printf '%s\n' "$$" > "$FORGE_VERIFY_DIR/start.pgid"
( bash -lc {}; code=$?; printf '%s\n' "$code" > "$FORGE_VERIFY_DIR/start.exit" ) > "$FORGE_VERIFY_DIR/app.log" 2>&1 < /dev/null &
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
    if let Some(verify) = directory.parent() {
        stop_orphaned_starts(verify);
    }
    for file in ["start.exit", "start.pid", "start.pgid", "app.log"]
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
    let port = pick_port(|| {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .map_err(|e| e.to_string())?;
        Ok(listener.local_addr().map_err(|e| e.to_string())?.port())
    })?;
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

/// Ports handed to verify runs of this server, until the longest verify wall
/// limit has passed. The probe listener is closed before the app binds, so
/// the system can offer the same free port to a second Task's verify in the
/// meantime; two apps on one port would let one Task's checks (and its
/// readiness probe) reach the other Task's app.
static VERIFY_PORTS: std::sync::Mutex<Vec<(u16, std::time::Instant)>> =
    std::sync::Mutex::new(Vec::new());
const VERIFY_PORT_HOLD: std::time::Duration = std::time::Duration::from_secs(3600 + 300);

/// A free loopback port no other verify of this server holds, and that
/// nothing serves on the IPv6 loopback either (an app that binds `localhost`
/// may take `::1`, and a check that opens `localhost` may reach it there).
fn pick_port(mut candidate: impl FnMut() -> Result<u16, String>) -> Result<u16, String> {
    let now = std::time::Instant::now();
    let mut held = VERIFY_PORTS.lock().unwrap_or_else(|p| p.into_inner());
    held.retain(|(_, until)| *until > now);
    for _ in 0..64 {
        let port = candidate()?;
        if held.iter().any(|(taken, _)| *taken == port) {
            continue;
        }
        if std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, port))
            .is_err_and(|error| error.kind() == std::io::ErrorKind::AddrInUse)
        {
            continue;
        }
        held.push((port, now + VERIFY_PORT_HOLD));
        return Ok(port);
    }
    Err("no free verify port".into())
}

/// The start step records its process group (`start.pgid`, written before
/// the app is started). A run's end stops that group; an owner that died
/// mid-run (a server crash or restart) never did, and its app keeps running
/// with its port. Before a verify starts anything, every attempt directory of
/// this Task root is looked at, so neither the re-run of an attempt nor a
/// later attempt starts beside such an orphan. Only real directories and
/// regular files are read; a record whose group is gone is removed.
fn stop_orphaned_starts(verify: &Path) {
    let Ok(entries) = std::fs::read_dir(verify) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let record = entry.path().join("start.pgid");
        if !std::fs::symlink_metadata(&record)
            .is_ok_and(|m| m.file_type().is_file() && m.len() <= 32)
        {
            continue;
        }
        let Some(group) = std::fs::read_to_string(&record)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
        else {
            let _ = std::fs::remove_file(&record);
            continue;
        };
        if process_supervisor::stop_orphaned_group(group) {
            let _ = std::fs::remove_file(&record);
        }
    }
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
    #[cfg(unix)]
    fn alive(pid: &str) -> bool {
        std::process::Command::new("kill")
            .args(["-0", pid.trim()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    /// A Forge that dies mid-verify runs no stop: the app keeps running, and
    /// keeps its port. The restarted owner prepares the next evaluation (or
    /// attempt) and must stop that app before anything new starts.
    #[cfg(unix)]
    #[tokio::test]
    async fn review_verify_restart_stops_the_app_a_crashed_owner_left_running() {
        use std::time::{Duration, Instant};
        let temp = tempfile::tempdir().unwrap();
        let worktree = temp.path().join("task/repo");
        std::fs::create_dir_all(&worktree).unwrap();
        executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let config: ReviewVerifyConfig = serde_json::from_value(json!({
            "start": r#"sleep 300 & printf '%s\n' "$!" > "$FORGE_VERIFY_DIR/app.pidfile"; wait"#,
            "checks": [{"name": "smoke", "command": r#"sleep 300 & printf '%s\n%s\n' "$!" "$$" > "$FORGE_VERIFY_DIR/check.pids"; wait"#}]
        }))
        .unwrap();
        let spec = build_bundle(&config, "workspace", 1, 1, 0, std::iter::empty()).unwrap();
        let environment = prepare_environment(&spec, &worktree).unwrap();
        let dir = directory(&worktree, 1).unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let permit = check_executor::CheckPermit::already_admitted();
        let mut run = Box::pin(check_executor::execute(check_executor::CheckExecution {
            operation_id: "verify-crash",
            spec: &spec,
            target: check_executor::CheckoutTarget::Workspace(&worktree),
            owner: CheckOwnerIdentity {
                owner_kind: "server".into(),
                machine_id: None,
                runtime_id: "test".into(),
            },
            input_revisions: None,
            environment: &environment,
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            cancel: &cancel,
            permit: &permit,
            cleanup: check_executor::CleanupPlan {
                commands: &[],
                timeout: check_executor::CLEANUP_TIMEOUT,
            },
            output_limit: 512,
        }));
        let checking = async {
            let until = Instant::now() + Duration::from_secs(20);
            while !dir.join("check.pids").is_file() || !dir.join("app.pidfile").is_file() {
                assert!(Instant::now() < until, "verify did not reach its check");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        tokio::select! { result = &mut run => panic!("run ended: {result:?}"), _ = checking => {} }
        // The owner dies: nothing of the run is dropped, stopped or settled.
        std::mem::forget(run);
        let app = std::fs::read_to_string(dir.join("app.pidfile")).unwrap();
        let checks = std::fs::read_to_string(dir.join("check.pids")).unwrap();
        assert!(alive(&app));
        let next = build_bundle(&config, "workspace", 1, 1, 1, std::iter::empty()).unwrap();
        prepare_environment(&next, &worktree).unwrap();
        let survived = alive(&app);
        for pid in app.lines().chain(checks.lines()) {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", pid.trim()])
                .stderr(std::process::Stdio::null())
                .status();
        }
        assert!(
            !survived,
            "the restarted owner started beside the orphaned app"
        );
    }

    #[test]
    fn review_verify_port_is_not_handed_out_twice_or_taken_on_ipv6() {
        let first = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let reused = first.local_addr().unwrap().port();
        drop(first);
        let other = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let fresh = other.local_addr().unwrap().port();
        drop(other);
        // The system offers the same free port to two concurrent verifies.
        assert_eq!(pick_port(|| Ok(reused)).unwrap(), reused);
        let mut offers = [reused, fresh].into_iter();
        assert_eq!(pick_port(|| Ok(offers.next().unwrap())).unwrap(), fresh);
        // A port something serves on ::1 only is free on 127.0.0.1, not for verify.
        if let Ok(v6) = std::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0)) {
            let busy = v6.local_addr().unwrap().port();
            let spare = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
            let next = spare.local_addr().unwrap().port();
            drop(spare);
            if std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, busy)).is_ok() {
                let mut offers = [busy, next].into_iter();
                assert_eq!(pick_port(|| Ok(offers.next().unwrap())).unwrap(), next);
            }
        }
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
