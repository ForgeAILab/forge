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

/// The Review id and evaluation are in every command identity. A new evaluation
/// cannot join the completed uncacheable run of an earlier one.
pub fn build_bundle(
    config: &ReviewVerifyConfig,
    workspace_id: &str,
    generation: u64,
    review_id: &str,
    evaluation: u32,
    project_keys: impl Iterator<Item = String>,
) -> Result<CheckSpec, String> {
    config.validate()?;
    validate_review_id(review_id)?;
    let mut keys: BTreeSet<_> = project_keys.collect();
    keys.extend(ENV_KEYS.map(str::to_owned));
    let mut commands = Vec::new();
    let prefix = format!("verify:{review_id}:{evaluation}:");
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
( bash -lc {}; code=$?; printf '%s\n' "$code" > "$FORGE_VERIFY_DIR/start.exit" ) >> "$FORGE_VERIFY_DIR/app.log" 2>&1 < /dev/null &
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
/// inputs. All commands must name the same Review/evaluation.
pub fn bundle_review(spec: &CheckSpec) -> Result<Option<&str>, String> {
    let Some(first) = spec
        .commands
        .first()
        .filter(|c| c.id.starts_with("verify:"))
    else {
        return Ok(None);
    };
    let mut parts = first.id.splitn(4, ':');
    parts.next();
    let review_id = parts.next().unwrap_or_default();
    validate_review_id(review_id)?;
    let evaluation: u32 = parts
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|_| "invalid verify evaluation")?;
    let prefix = format!("verify:{review_id}:{evaluation}:");
    if spec.execution_policy != CANONICAL_CI_POLICY
        || spec
            .commands
            .iter()
            .any(|c| !c.id.starts_with(&prefix) || c.cacheability != CheckCacheability::Uncacheable)
    {
        return Err("invalid verify bundle".into());
    }
    Ok(Some(review_id))
}

fn validate_review_id(review_id: &str) -> Result<(), String> {
    if review_id.is_empty()
        || review_id.len() > 128
        || !review_id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err("invalid verify Review id".into());
    }
    Ok(())
}

pub fn directory(worktree: &Path, review_id: &str) -> Result<PathBuf, String> {
    validate_review_id(review_id)?;
    let root =
        executors::sandbox::TaskRoot::of_worktree(worktree).ok_or("verify needs a Task root")?;
    Ok(root.path().join(".forge-task/verify").join(review_id))
}

/// Called by the already-admitted server check owner, under its checkout lock.
/// This performs no command execution. Re-execution clears only known output
/// files so reports from an inconclusive evaluation cannot answer the next one.
pub fn prepare_environment(
    spec: &CheckSpec,
    worktree: &Path,
) -> Result<BTreeMap<String, String>, String> {
    let Some(review_id) = bundle_review(spec)? else {
        return Ok(BTreeMap::new());
    };
    let directory = directory(worktree, review_id)?;
    ensure_directory(&directory, worktree)?;
    if let Some(verify) = directory.parent() {
        // This Review's own record would be overwritten by the new start
        // step: one whose group could not be confirmed stopped is kept and
        // nothing is started beside it.
        if stop_orphaned_starts(verify)
            .iter()
            .any(|name| name.to_str() == Some(review_id))
        {
            return Err(
                "an earlier verify app of this Review could not be confirmed stopped".into(),
            );
        }
    }
    for file in ["start.exit", "start.pid", "start.pgid", "app.log", "port"]
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
    std::fs::write(directory.join("port"), port.to_string()).map_err(|e| e.to_string())?;
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

/// Review ids whose verify run this server is executing now. Once its start
/// step has exited, a live run's group has a gone leader and running members,
/// exactly like a group a crashed owner left behind: a sibling Review sharing
/// the Task root must not stop it from its own sweep.
static LIVE_RUNS: std::sync::Mutex<BTreeMap<String, usize>> =
    std::sync::Mutex::new(BTreeMap::new());

struct LiveRun(Option<String>);
impl LiveRun {
    fn enter(spec: &CheckSpec) -> Self {
        let Ok(Some(review_id)) = bundle_review(spec) else {
            return Self(None);
        };
        *LIVE_RUNS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(review_id.to_owned())
            .or_default() += 1;
        Self(Some(review_id.to_owned()))
    }
}
impl Drop for LiveRun {
    fn drop(&mut self) {
        let Some(review_id) = self.0.take() else {
            return;
        };
        let mut live = LIVE_RUNS.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = live.get_mut(&review_id) {
            *count -= 1;
            if *count == 0 {
                live.remove(&review_id);
            }
        }
    }
}
fn is_live(review_id: &str) -> bool {
    LIVE_RUNS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .contains_key(review_id)
}

/// On-disk bound of `app.log`: output past it is discarded.
pub const APP_LOG_CAP: u64 = 16 * 1024 * 1024;
/// While a run lasts, `app.log` exceeds [`APP_LOG_CAP`] by at most what the app
/// writes within one interval before it is cut back.
const APP_LOG_TRIM_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Run an admitted bundle after [`prepare_environment`]. A verify run is
/// marked live for its whole execution (a dropped execution unmarks it, as a
/// crash does). The app writes `app.log` itself, appending, so short lines
/// land at once; this owner cuts the file back to [`APP_LOG_CAP`] while the
/// run lasts and once more after settlement, before the receipt is recorded.
/// The group record is removed once settlement is confirmed.
pub async fn run_admitted<E>(
    spec: &CheckSpec,
    worktree: &Path,
    execution: impl std::future::Future<Output = Result<CheckReceipt, E>>,
) -> Result<CheckReceipt, E> {
    let live = LiveRun::enter(spec);
    let verify = live
        .0
        .as_deref()
        .and_then(|review_id| directory(worktree, review_id).ok());
    let mut execution = std::pin::pin!(execution);
    let receipt = match &verify {
        None => execution.await,
        Some(directory) => {
            let mut trim = tokio::time::interval(APP_LOG_TRIM_INTERVAL);
            let receipt = loop {
                tokio::select! {
                    receipt = &mut execution => break receipt,
                    _ = trim.tick() => cap_app_log(directory, worktree),
                }
            };
            cap_app_log(directory, worktree);
            receipt
        }
    }?;
    finish_run(spec, worktree, &receipt);
    Ok(receipt)
}

/// Keep the first [`APP_LOG_CAP`] bytes of a regular, singly linked `app.log`.
/// The candidate controls the directory's contents: a symlink, hard link,
/// FIFO or replaced file is left alone, and opening never blocks.
#[cfg(unix)]
fn cap_app_log(directory: &Path, worktree: &Path) {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let path = directory.join("app.log");
    let Ok(before) = std::fs::symlink_metadata(&path) else {
        return;
    };
    if !before.file_type().is_file()
        || before.nlink() != 1
        || before.len() <= APP_LOG_CAP
        || ensure_directory(directory, worktree).is_err()
    {
        return;
    }
    let Ok(file) = std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    else {
        return;
    };
    if file.metadata().is_ok_and(|opened| {
        opened.file_type().is_file()
            && opened.nlink() == 1
            && opened.dev() == before.dev()
            && opened.ino() == before.ino()
    }) {
        let _ = file.set_len(APP_LOG_CAP);
    }
}
#[cfg(not(unix))]
fn cap_app_log(_directory: &Path, _worktree: &Path) {}

/// A stopped group no longer needs a crash-recovery record. Retaining it could
/// later target an unrelated process group after the OS reuses its id.
fn finish_run(spec: &CheckSpec, worktree: &Path, receipt: &CheckReceipt) {
    if receipt.commands.is_empty() || !receipt.commands.iter().all(|c| c.process_tree_stopped) {
        return;
    }
    if let Ok(Some(review_id)) = bundle_review(spec) {
        if let Ok(directory) = directory(worktree, review_id) {
            let _ = std::fs::remove_file(directory.join("start.pgid"));
        }
    }
}

/// Group settlement cannot contain a daemon that created a new session. The
/// port probe is diagnostic only; never kill an unowned process by its port.
pub fn port_warning(port: u16) -> Option<String> {
    let busy = [
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    ]
    .into_iter()
    .any(|ip| {
        std::net::TcpListener::bind((ip, port))
            .is_err_and(|e| e.kind() == std::io::ErrorKind::AddrInUse)
    });
    busy.then(|| format!("Verify port {port} is still in use after process-group cleanup; a process may have escaped via setsid or daemonizing. Forge cannot stop processes outside its groups."))
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
/// with its port. Before a verify starts anything, every Review directory of
/// this Task root is looked at, so neither the re-run of an attempt nor a
/// later attempt starts beside such an orphan. Only real directories and
/// regular files are read; a record whose group is gone is removed. A Review
/// this server is running now is not an orphan and is left alone. Returns the
/// Review directories whose group could not be confirmed stopped.
fn stop_orphaned_starts(verify: &Path) -> Vec<std::ffi::OsString> {
    let mut unconfirmed = Vec::new();
    let Ok(entries) = std::fs::read_dir(verify) else {
        return unconfirmed;
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
        let text = std::fs::read_to_string(&record).ok();
        // Checked after the read: a run is marked live before its start step
        // can write the record, so a record written by a live run is skipped.
        if entry.file_name().to_str().is_some_and(is_live) {
            continue;
        }
        let Some(group) = text.and_then(|text| text.trim().parse::<u32>().ok()) else {
            let _ = std::fs::remove_file(&record);
            continue;
        };
        if process_supervisor::stop_orphaned_group(group) {
            let _ = std::fs::remove_file(&record);
        } else {
            unconfirmed.push(entry.file_name());
        }
    }
    unconfirmed
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
    for note in &details.notes {
        out.push_str(&format!("Note: {note}\n"));
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
            "review-7",
            0,
            vec!["PROJECT_KEY".to_owned()].into_iter(),
        )
        .unwrap();
        assert_eq!(spec.execution_policy, CANONICAL_CI_POLICY);
        assert!(!spec.cacheable());
        assert_eq!(bundle_review(&spec).unwrap(), Some("review-7"));
        assert_eq!(
            spec.commands
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>(),
            [
                "verify:review-7:0:start",
                "verify:review-7:0:ready",
                "verify:review-7:0:check:smoke",
                "verify:review-7:0:check:save"
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
            build_bundle(&config, "workspace", 3, "review-7", 1, std::iter::empty()).unwrap()
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
        let spec =
            build_bundle(&config, "workspace", 1, "review-1", 0, std::iter::empty()).unwrap();
        let environment = prepare_environment(&spec, &worktree).unwrap();
        let dir = directory(&worktree, "review-1").unwrap();
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
        let next =
            build_bundle(&config, "workspace", 1, "review-1", 1, std::iter::empty()).unwrap();
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

    /// Coordination children share a Task root. Once a running verify's
    /// start step has exited, its group looks like an orphan (leader gone,
    /// app running); a sibling Review's prepare must not stop it.
    #[cfg(unix)]
    #[tokio::test]
    async fn review_verify_sibling_prepare_leaves_a_live_run_running() {
        use std::time::{Duration, Instant};
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("coordination-root");
        let first = root.join("first");
        let second = root.join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        executors::sandbox::TaskRoot::reserve(&root).unwrap();
        let config: ReviewVerifyConfig = serde_json::from_value(json!({
            "start": r#"sleep 300 & printf '%s\n' "$!" > "$FORGE_VERIFY_DIR/app.pidfile"; wait"#,
            "checks": [{"name": "smoke", "command": r#"touch "$FORGE_VERIFY_DIR/checking"; while [ ! -f "$FORGE_VERIFY_DIR/release" ]; do sleep 0.05; done"#}]
        }))
        .unwrap();
        let a = build_bundle(&config, "first", 1, "review-a", 0, std::iter::empty()).unwrap();
        let b = build_bundle(&config, "second", 1, "review-b", 0, std::iter::empty()).unwrap();
        let environment = prepare_environment(&a, &first).unwrap();
        let dir = directory(&first, "review-a").unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let permit = check_executor::CheckPermit::already_admitted();
        let mut run = Box::pin(run_admitted(&a, &first, async {
            Ok::<_, std::convert::Infallible>(
                check_executor::execute(check_executor::CheckExecution {
                    operation_id: "verify-live",
                    spec: &a,
                    target: check_executor::CheckoutTarget::Workspace(&first),
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
                })
                .await,
            )
        }));
        let checking = async {
            let until = Instant::now() + Duration::from_secs(20);
            while !dir.join("checking").is_file() || !dir.join("app.pidfile").is_file() {
                assert!(Instant::now() < until, "verify did not reach its check");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! { result = &mut run => panic!("run ended: {result:?}"), _ = checking => {} }
        let app = std::fs::read_to_string(dir.join("app.pidfile")).unwrap();
        assert!(alive(&app));
        prepare_environment(&b, &second).unwrap();
        let survived = alive(&app);
        std::fs::write(dir.join("release"), "").unwrap();
        let receipt = run.await.unwrap();
        assert!(
            survived,
            "a sibling Review's prepare stopped a live verify app"
        );
        assert_eq!(
            receipt.outcome,
            CheckExecutionOutcome::Passed,
            "{receipt:?}"
        );
        assert!(receipt.commands.iter().all(|c| c.process_tree_stopped));
        assert!(!alive(&app));
        assert!(!dir.join("start.pgid").exists());
    }

    /// The new start step overwrites `start.pgid`. A record whose group the
    /// sweep could not confirm stopped (here group 1, which is never signalled)
    /// must survive, and nothing may start beside it.
    #[cfg(unix)]
    #[test]
    fn review_verify_prepare_keeps_an_unconfirmed_group_record_and_refuses() {
        let temp = tempfile::tempdir().unwrap();
        let worktree = temp.path().join("task/repo");
        std::fs::create_dir_all(&worktree).unwrap();
        executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        let config: ReviewVerifyConfig = serde_json::from_value(
            json!({"start":"sleep 300","checks":[{"name":"smoke","command":"true"}]}),
        )
        .unwrap();
        let rerun =
            build_bundle(&config, "workspace", 1, "review-1", 1, std::iter::empty()).unwrap();
        let dir = directory(&worktree, "review-1").unwrap();
        ensure_directory(&dir, &worktree).unwrap();
        std::fs::write(dir.join("start.pgid"), "1\n").unwrap();
        assert!(prepare_environment(&rerun, &worktree).is_err());
        assert_eq!(
            std::fs::read_to_string(dir.join("start.pgid")).unwrap(),
            "1\n"
        );
        // Another Review's unconfirmed record is kept for a later sweep but
        // does not block this one, whose own directory has no such record.
        let other =
            build_bundle(&config, "workspace", 1, "review-2", 0, std::iter::empty()).unwrap();
        prepare_environment(&other, &worktree).unwrap();
        assert!(dir.join("start.pgid").is_file());
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
    fn review_verify_reviews_sharing_a_task_root_keep_independent_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("coordination-root");
        let first = root.join("first");
        let second = root.join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        executors::sandbox::TaskRoot::reserve(&root).unwrap();
        let config: ReviewVerifyConfig =
            serde_json::from_value(json!({"checks":[{"name":"smoke","command":"true"}]})).unwrap();
        let a = build_bundle(&config, "first", 1, "review-a", 0, std::iter::empty()).unwrap();
        let b = build_bundle(&config, "second", 1, "review-b", 0, std::iter::empty()).unwrap();
        prepare_environment(&a, &first).unwrap();
        let a_dir = directory(&first, "review-a").unwrap();
        std::fs::write(a_dir.join("request.json"), "first request").unwrap();
        std::fs::write(a_dir.join("smoke.report.json"), "first report").unwrap();
        prepare_environment(&b, &second).unwrap();
        let b_dir = directory(&second, "review-b").unwrap();
        assert_ne!(a_dir, b_dir);
        assert_eq!(a_dir.parent(), b_dir.parent());
        std::fs::write(b_dir.join("request.json"), "second request").unwrap();
        assert_eq!(
            std::fs::read_to_string(a_dir.join("request.json")).unwrap(),
            "first request"
        );
        assert_eq!(
            std::fs::read_to_string(a_dir.join("smoke.report.json")).unwrap(),
            "first report"
        );
        assert!(directory(&first, "../escape").is_err());
    }

    #[test]
    fn review_verify_directory_refuses_symlinked_reserved_components() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("task/repo");
        std::fs::create_dir_all(&path).unwrap();
        executors::sandbox::TaskRoot::reserve(path.parent().unwrap()).unwrap();
        let dir = directory(&path, "review-1").unwrap();
        ensure_directory(&dir, &path).unwrap();
        #[cfg(unix)]
        {
            std::fs::remove_dir(&dir).unwrap();
            std::os::unix::fs::symlink(temp.path(), &dir).unwrap();
            assert!(ensure_directory(&dir, &path).is_err());
        }
    }
}
