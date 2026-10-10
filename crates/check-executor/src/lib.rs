//! Persistence-free, owner-local mechanical execution. Admission is the caller's.
use api_types::*;
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

pub const OUTPUT_TAIL_BYTES: usize = 4096;
pub const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
/// A piece of a secret cut by the tail boundary or by an interruption is
/// masked only from this length up. Shorter pieces disclose nothing useful,
/// and masking them would rewrite ordinary output that happens to start or end
/// with the same few bytes as some environment value.
const SECRET_FRAGMENT_BYTES: usize = 4;
const WITNESS_TIMEOUT: Duration = Duration::from_secs(2);
/// `git status` walks the worktree; a large one needs longer than a HEAD read.
const CLEAN_WITNESS_TIMEOUT: Duration = Duration::from_secs(20);
const LEGACY_SERVER: &str = "legacy-server/1";
const LEGACY_DAEMON: &str = "legacy-daemon/1";
fn legacy(policy: &str) -> bool {
    matches!(policy, LEGACY_SERVER | LEGACY_DAEMON)
}
fn canonical(policy: &str) -> bool {
    policy == CANONICAL_CI_POLICY
}

/// The environment a canonical step inherits on this owner, as a login shell
/// resolves it right now: the owner process's environment after the login
/// profile ran. Read by a probe built like a step (same shell, same inherited
/// environment, same removed Git variables), without the Project values and
/// without the build budget Forge adds per run.
pub struct InheritedEnvironment {
    pub values: BTreeMap<String, String>,
    pub shell_revision: String,
}
/// Variables left out of the environment identity: a login shell or the
/// session that started the owner sets them per process, and no build reads
/// its inputs from them. Everything else a step inherits is in the identity,
/// by name and value.
///
/// - `PWD`, `OLDPWD`: the shell's own directory bookkeeping; the step runs in
///   the checkout, the probe does not.
/// - `SHLVL`, `_`: set by every shell for itself.
/// - `COLUMNS`, `LINES`: the size of whatever terminal started the owner.
/// - `TERM_SESSION_ID`, `ITERM_SESSION_ID`, `SECURITYSESSIONID`,
///   `XDG_SESSION_ID`, `WINDOWID`, `TMUX`, `TMUX_PANE`, `STY`: the id of the
///   terminal, multiplexer or login session the owner was started from.
/// - `SSH_CLIENT`, `SSH_CONNECTION`, `SSH_TTY`: the address and port of the
///   SSH connection the owner was started over.
/// - `INVOCATION_ID`, `JOURNAL_STREAM`, `SYSTEMD_EXEC_PID`: per-start ids a
///   service manager gives the owner.
///
/// Not listed because the probe never has them: the build-budget variables
/// Forge adds per run when neither the operator nor the Project sets them
/// (`executors::run_process::BUILD_ENV_KEYS`; they only set parallelism), and
/// the Project values (in the identity through the request).
pub const ENVIRONMENT_IDENTITY_DENYLIST: [&str; 20] = [
    "COLUMNS",
    "INVOCATION_ID",
    "ITERM_SESSION_ID",
    "JOURNAL_STREAM",
    "LINES",
    "OLDPWD",
    "PWD",
    "SECURITYSESSIONID",
    "SHLVL",
    "SSH_CLIENT",
    "SSH_CONNECTION",
    "SSH_TTY",
    "STY",
    "SYSTEMD_EXEC_PID",
    "TERM_SESSION_ID",
    "TMUX",
    "TMUX_PANE",
    "WINDOWID",
    "XDG_SESSION_ID",
    "_",
];
impl InheritedEnvironment {
    /// What the environment identity is computed over.
    pub fn identity_values(&self) -> BTreeMap<&str, &str> {
        self.values
            .iter()
            .filter(|(key, _)| !ENVIRONMENT_IDENTITY_DENYLIST.contains(&key.as_str()))
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect()
    }
}
const PROBE_START: &str = "forge-inherited-environment-start";
const PROBE_END: &str = "forge-inherited-environment-end";
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
/// Read the inherited environment. One login shell per call, at most
/// [`PROBE_TIMEOUT`]; its process group is stopped when the bound passes.
///
/// `None` when the shell did not answer completely (a profile that hangs,
/// exits early or prints more than the bound). The caller then attests
/// nothing and the run is not reusable; steps run as they always did. The
/// shell's exit status is deliberately not read: a logout script may replace
/// it. Values are never logged.
pub async fn inherited_environment() -> Option<InheritedEnvironment> {
    probe_environment(None).await
}
/// `home` replaces the inherited `HOME`, so a test can supply a profile.
async fn probe_environment(home: Option<&Path>) -> Option<InheritedEnvironment> {
    // `compgen -e`: the exported names. Works on bash 3.2.
    let script = format!(
        "printf '%s\\0' {PROBE_START} \"$BASH_VERSION\"; for k in $(compgen -e); do printf '%s=%s\\0' \"$k\" \"${{!k}}\"; done; printf '%s\\0' {PROBE_END}"
    );
    let mut probe = Command::new("bash");
    probe
        .arg("-lc")
        .arg(script)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE");
    if let Some(home) = home {
        probe.env("HOME", home);
    }
    let output = process_supervisor::run(
        &mut probe,
        process_supervisor::Capture::Prefix(PROBE_OUTPUT_BYTES),
        Some(Instant::now() + PROBE_TIMEOUT),
        &CancellationToken::new(),
        process_supervisor::CompletionPolicy::StopDescendants,
    )
    .await
    .ok()?;
    if output.termination != process_supervisor::Termination::Exited || output.stdout_truncated {
        return None;
    }
    // A profile may print before the probe does, and a logout script after.
    let mut fields = output
        .stdout
        .split(|byte| *byte == 0)
        .skip_while(|field| !field.ends_with(PROBE_START.as_bytes()));
    fields.next()?;
    let shell_revision = String::from_utf8(fields.next()?.to_vec()).ok()?;
    let mut values = BTreeMap::new();
    let mut complete = false;
    for field in fields {
        if field == PROBE_END.as_bytes() {
            complete = true;
            break;
        }
        let field = String::from_utf8(field.to_vec()).ok()?;
        let (key, value) = field.split_once('=')?;
        values.insert(key.to_owned(), value.to_owned());
    }
    (complete && !shell_revision.is_empty()).then_some(InheritedEnvironment {
        values,
        shell_revision,
    })
}

/// What happens to a command's descendants once it exits normally.
///
/// Neither the server nor the daemon ever stopped a CI step's descendants: a
/// step may start a service that a later step uses. Both frozen policies keep
/// that, with the post-exit drain bounded (as the daemon's always was) so a
/// service that kept the output pipe cannot stall the step. The canonical CI
/// policy keeps it too, and stops every step's process group when the run
/// ends (see `execute`). Any other policy stops a step's group when it exits.
fn completion(policy: &str) -> process_supervisor::CompletionPolicy {
    if legacy(policy) || canonical(policy) {
        process_supervisor::CompletionPolicy::DrainFor(process_supervisor::POST_EXIT_DRAIN)
    } else {
        process_supervisor::CompletionPolicy::StopDescendants
    }
}

/// The process groups of a canonical run's finished steps. A run that is
/// dropped before its end (its owner was interrupted) still stops them, off
/// the runtime and unverified: no receipt is produced on that path.
struct RunTree(Vec<u32>);
impl Drop for RunTree {
    fn drop(&mut self) {
        for group in std::mem::take(&mut self.0) {
            std::thread::spawn(move || process_supervisor::stop_group(group));
        }
    }
}

/// A borrowed admission seam; stage C supplies machine occupancy before calling.
/// Legacy callers already hold their Task/execution admission. No slot acquired here.
pub struct CheckPermit {
    _private: (),
}
impl CheckPermit {
    pub fn already_admitted() -> Self {
        Self { _private: () }
    }
}

pub enum CheckoutTarget<'a> {
    Workspace(&'a Path),
    ExactCommit {
        repository: &'a Path,
        build_area: &'a Path,
        commit: &'a str,
    },
}
/// Declared cleanup is owner policy, pinned by the spec's execution_policy.
/// It never receives the already-cancelled run token or the expired wall deadline.
pub struct CleanupPlan<'a> {
    pub commands: &'a [CheckCommandSpec],
    pub timeout: Duration,
}
pub struct CheckExecution<'a> {
    pub operation_id: &'a str,
    pub spec: &'a CheckSpec,
    pub target: CheckoutTarget<'a>,
    pub owner: CheckOwnerIdentity,
    /// Explicit owner-maintained revisions, never secret bytes or a caller digest.
    /// A machine without revision evidence returns NotAttested.
    pub input_revisions: Option<&'a ServerCheckExecutionInputs>,
    pub environment: &'a BTreeMap<String, String>,
    pub deadline: Option<Instant>,
    pub cancel: &'a CancellationToken,
    pub permit: &'a CheckPermit,
    pub cleanup: CleanupPlan<'a>,
    pub output_limit: usize,
}
fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}
fn milliseconds(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}
fn tail(text: &str, limit: usize) -> String {
    let mut start = text.len().saturating_sub(limit);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
}
fn redacted(
    bytes: &[u8],
    truncated: bool,
    interrupted: bool,
    env: &BTreeMap<String, String>,
    limit: usize,
) -> (String, bool) {
    let mut text = String::from_utf8_lossy(bytes).into_owned();
    // A retained tail may begin halfway through a secret. Mask that fragment
    // too; otherwise truncation could disclose a secret suffix. Similarly the
    // final bytes of an interrupted command may be only a secret prefix.
    let mut values = env
        .values()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    values.dedup();
    for value in values {
        if truncated {
            for (offset, _) in value.char_indices() {
                if value.len() - offset < SECRET_FRAGMENT_BYTES {
                    break;
                }
                if text.starts_with(&value[offset..]) {
                    text.replace_range(..value.len() - offset, "[REDACTED]");
                    break;
                }
            }
        }
        if interrupted {
            if text.ends_with(value) {
                text.replace_range(text.len() - value.len().., "[REDACTED]");
                continue;
            }
            for (offset, _) in value
                .char_indices()
                .rev()
                .filter(|(offset, _)| *offset >= SECRET_FRAGMENT_BYTES)
            {
                if text.ends_with(&value[..offset]) {
                    text.replace_range(text.len() - offset.., "[REDACTED]");
                    break;
                }
            }
        }
    }
    let text = executors::environment::redact_environment_values(&text, env);
    let shortened = text.len() > limit;
    (tail(&text, limit), truncated || shortened)
}
/// Build a command. The two frozen legacy policies and the canonical CI
/// policy inherit the owner's environment through a login shell, with the
/// Project values on top; every other policy clears it and sees only the
/// spec's declared keys. All use the owner's current budget.
///
/// Legacy policies pass the declared keys and refuse when one is missing. The
/// canonical policy passes the Project environment in force now, whole: a
/// value that changed since the request makes the run a verdict that is not
/// reusable (its owner attests nothing), never a refusal.
pub fn command(
    path: &Path,
    spec: &CheckCommandSpec,
    environment: &BTreeMap<String, String>,
    policy: &str,
) -> Result<(Command, BTreeMap<String, String>), String> {
    if spec.shell != "bash -lc" {
        return Err("unsupported check shell".into());
    }
    let env: BTreeMap<_, _> = if canonical(policy) {
        environment.clone()
    } else {
        environment
            .iter()
            .filter(|(key, _)| spec.environment_keys.contains(*key))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    if spec
        .environment_keys
        .iter()
        .any(|key| !env.contains_key(key) && !canonical(policy))
    {
        return Err("missing declared check environment key".into());
    }
    let mut command = Command::new("bash");
    if !legacy(policy) && !canonical(policy) {
        command.env_clear().arg("--noprofile");
    }
    command
        .arg("-lc")
        .arg(&spec.shell_text)
        .current_dir(path)
        .envs(&env);
    executors::run_process::apply(&mut command, &env);
    // Legacy unbounded server CI inherited these; bounded CI and daemon did not.
    if policy != LEGACY_SERVER || spec.timeout_seconds.is_some() {
        command
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE");
    }
    if policy == LEGACY_DAEMON {
        command.env("PWD", path);
    }
    Ok((command, env))
}

pub async fn run_command(
    path: &Path,
    spec: &CheckCommandSpec,
    environment: &BTreeMap<String, String>,
    policy: &str,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    limit: usize,
) -> Result<CheckCommandReceipt, String> {
    run_command_group(path, spec, environment, policy, deadline, cancel, limit)
        .await
        .map(|(receipt, _)| receipt)
}
/// Also returns the command's process group, when one was started.
async fn run_command_group(
    path: &Path,
    spec: &CheckCommandSpec,
    environment: &BTreeMap<String, String>,
    policy: &str,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    limit: usize,
) -> Result<(CheckCommandReceipt, Option<u32>), String> {
    let started_at = now();
    let start = Instant::now();
    let (mut command, env) = command(path, spec, environment, policy)?;
    let per_command = spec
        .timeout_seconds
        .and_then(|s| start.checked_add(Duration::from_secs(s)));
    let deadline = match (deadline, per_command) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    if cancel.is_cancelled() || deadline.is_some_and(|d| d <= start) {
        let receipt = CheckCommandReceipt {
            id: spec.id.clone(),
            command: spec.shell_text.clone(),
            exit_code: None,
            outcome: if cancel.is_cancelled() {
                CheckExecutionOutcome::Cancelled
            } else {
                CheckExecutionOutcome::TimedOut
            },
            duration_ms: 0,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            stdout_drain_incomplete: false,
            stderr_drain_incomplete: false,
            process_tree_stopped: true,
            started_at,
            finished_at: now(),
        };
        return Ok((receipt, None));
    }
    let output = process_supervisor::run(
        &mut command,
        process_supervisor::Capture::Tail(limit),
        deadline,
        cancel,
        completion(policy),
    )
    .await
    .map_err(|e| e.to_string())?;
    let outcome = match output.termination {
        process_supervisor::Termination::TimedOut => CheckExecutionOutcome::TimedOut,
        process_supervisor::Termination::Cancelled => CheckExecutionOutcome::Cancelled,
        process_supervisor::Termination::Exited if output.status.success() => {
            CheckExecutionOutcome::Passed
        }
        process_supervisor::Termination::Exited => CheckExecutionOutcome::Failed,
    };
    let (stdout_tail, stdout_truncated) = redacted(
        &output.stdout,
        output.stdout_truncated,
        output.termination != process_supervisor::Termination::Exited,
        &env,
        limit,
    );
    let (stderr_tail, stderr_truncated) = redacted(
        &output.stderr,
        output.stderr_truncated,
        output.termination != process_supervisor::Termination::Exited,
        &env,
        limit,
    );
    let group = output.group;
    let receipt = CheckCommandReceipt {
        id: spec.id.clone(),
        command: spec.shell_text.clone(),
        // A command stopped by its limit or by cancellation has no verdict of
        // its own, even when its TERM trap exits 0.
        exit_code: output
            .status
            .code()
            .filter(|_| output.termination == process_supervisor::Termination::Exited),
        outcome,
        duration_ms: milliseconds(start.elapsed()),
        stdout_tail,
        stderr_tail,
        stdout_truncated,
        stderr_truncated,
        stdout_drain_incomplete: output.stdout_drain_incomplete,
        stderr_drain_incomplete: output.stderr_drain_incomplete,
        process_tree_stopped: output.descendants_stopped,
        started_at,
        finished_at: now(),
    };
    Ok((receipt, group))
}

/// The receipt of a run that never reached its checkout: nothing was spawned
/// and no cleanup ran. An owner settles an admitted key with this when it
/// cannot start (checkout busy until the deadline, cancelled while waiting,
/// workspace gone), so the key never stays without an outcome.
pub fn unstarted_receipt(
    operation_id: &str,
    owner: CheckOwnerIdentity,
    outcome: CheckExecutionOutcome,
    message: Option<String>,
) -> CheckReceipt {
    let at = now();
    CheckReceipt {
        operation_id: operation_id.to_owned(),
        owner,
        execution_inputs: CheckEnvironmentIdentity::NotAttested,
        commands: Vec::new(),
        outcome,
        cleanup: CheckCleanupReceipt {
            outcome: CheckCleanupOutcome::NotPerformed,
            commands: Vec::new(),
            checkout_removed: false,
            message: None,
        },
        prepared_head: None,
        finished_head: None,
        tracked_changes: None,
        started_at: at.clone(),
        finished_at: at,
        infrastructure_message: message,
    }
}

pub async fn execute(input: CheckExecution<'_>) -> CheckReceipt {
    let mut receipt = unstarted_receipt(
        input.operation_id,
        input.owner.clone(),
        CheckExecutionOutcome::Passed,
        None,
    );
    receipt.finished_at = String::new();
    // The frozen CI callers ran nothing but the step in the Task worktree and
    // read no receipt witness: no Git command is added beside their steps.
    // A managed checkout is always witnessed: its pass depends on it.
    let witnessed = !legacy(&input.spec.execution_policy)
        || matches!(input.target, CheckoutTarget::ExactCommit { .. });
    let mut scratch = None;
    let mut path = match input.target {
        CheckoutTarget::Workspace(path) => Some(path.to_owned()),
        _ => None,
    };
    let prepared = async {
        input.spec.validate()?;
        if input.deadline.is_none() && !legacy(&input.spec.execution_policy) {
            return Err("a canonical check requires a whole-run wall deadline".into());
        }
        if input.output_limit == 0 || input.cleanup.timeout.is_zero() {
            return Err("positive output and cleanup limits required".to_owned());
        }
        if input.spec.declares_cleanup == input.cleanup.commands.is_empty() {
            return Err("declared cleanup must have its pinned owner plan".into());
        }
        // Compute the attestation on this owner before preparing or spawning.
        receipt.execution_inputs = input.input_revisions.map_or(
            Ok(CheckEnvironmentIdentity::NotAttested),
            ServerCheckExecutionInputs::identity,
        )?;
        let checkout = match input.target {
            CheckoutTarget::Workspace(workspace) => workspace.to_owned(),
            CheckoutTarget::ExactCommit {
                repository,
                build_area,
                commit,
            } => {
                if !matches!(input.spec.scope, CheckScope::Commit) {
                    return Err("exact checkout requires commit scope".into());
                }
                if ![40, 64].contains(&commit.len())
                    || !commit
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                {
                    return Err("exact full commit object id required".into());
                }
                tokio::fs::create_dir_all(build_area)
                    .await
                    .map_err(|e| e.to_string())?;
                let directory = tempfile::Builder::new()
                    .prefix("check-")
                    .tempdir_in(build_area)
                    .map_err(|e| e.to_string())?;
                let checkout = directory.path().join("repo");
                scratch = Some(directory);
                path = Some(checkout.clone());
                git::command_output(repository, &["cat-file", "-t", commit])
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|o| {
                        if o.status.success() && o.stdout == b"commit\n" {
                            Ok(())
                        } else {
                            Err("target is not a commit".into())
                        }
                    })?;
                let target = checkout.to_str().ok_or("invalid build checkout path")?;
                let result = git::command_output(
                    repository,
                    &["clone", "--shared", "--no-checkout", "--", ".", target],
                )
                .await
                .map_err(|e| e.to_string())?;
                if !result.status.success() {
                    return Err("managed check clone failed".into());
                }
                let result = git::command_output(&checkout, &["checkout", "--detach", commit])
                    .await
                    .map_err(|e| e.to_string())?;
                if !result.status.success() {
                    return Err("managed exact checkout failed".into());
                }
                if git::get_current_sha(&checkout)
                    .await
                    .map_err(|e| e.to_string())?
                    != commit
                {
                    return Err("managed checkout witness mismatch".into());
                }
                checkout
            }
        };
        path = Some(checkout);
        Ok::<_, String>(())
    };
    let prepare_result = tokio::select! {
        biased;
        _ = input.cancel.cancelled() => { receipt.outcome = CheckExecutionOutcome::Cancelled; Ok(()) },
        _ = async { match input.deadline { Some(d) => tokio::time::sleep_until(d.into()).await, None => std::future::pending().await } } => { receipt.outcome = CheckExecutionOutcome::TimedOut; Ok(()) },
        result = prepared => result,
    };
    if let Err(error) = prepare_result {
        receipt.outcome = CheckExecutionOutcome::Infrastructure;
        receipt.infrastructure_message = Some(error);
    }
    // The canonical policy runs in the Task worktree, which is only a clean
    // checkout of the commit when nothing is modified, staged or untracked
    // there before the first command. `None`: the witness could not be taken.
    // Ignored files (build output, installed dependencies) are not looked at:
    // they are the worktree's own, which is why a canonical identity names
    // its worktree.
    let mut clean_before = None;
    let is_canonical = canonical(&input.spec.execution_policy);
    // Every canonical step's process group: a step may leave a service for
    // the next one, and the whole run's tree is stopped when the run ends.
    let mut groups = RunTree(Vec::new());
    if receipt.outcome == CheckExecutionOutcome::Passed {
        if let Some(path) = path.as_ref() {
            if witnessed {
                receipt.prepared_head =
                    tokio::time::timeout(WITNESS_TIMEOUT, git::get_current_sha(path))
                        .await
                        .ok()
                        .and_then(Result::ok);
            }
            if canonical(&input.spec.execution_policy) {
                clean_before = tokio::time::timeout(
                    CLEAN_WITNESS_TIMEOUT,
                    git::command_output(path, &["status", "--porcelain"]),
                )
                .await
                .ok()
                .and_then(Result::ok)
                .filter(|output| output.status.success())
                .map(|output| output.stdout.is_empty());
            }
            let mut sequence = CheckSequence::new(input.spec.clone());
            while let Some((_, spec)) = sequence.next_command() {
                match run_command_group(
                    path,
                    spec,
                    input.environment,
                    &input.spec.execution_policy,
                    input.deadline,
                    input.cancel,
                    input.output_limit,
                )
                .await
                {
                    Ok((command, group)) => {
                        if is_canonical {
                            groups.0.extend(group);
                        }
                        let outcome = command.outcome;
                        receipt.commands.push(command);
                        if outcome != CheckExecutionOutcome::Passed {
                            receipt.outcome = outcome;
                        }
                        sequence.completed(outcome);
                    }
                    Err(error) => {
                        receipt.outcome = CheckExecutionOutcome::Infrastructure;
                        receipt.infrastructure_message = Some(error);
                        break;
                    }
                }
            }
        }
    }
    if is_canonical {
        // Run end, whatever the outcome: stop what the steps left running,
        // before the last witness so nothing writes to the checkout after it.
        // `process_tree_stopped` then says what was verified, for every step:
        // no member of any step's process group is left.
        let groups = std::mem::take(&mut groups.0);
        let stopped = tokio::task::spawn_blocking(move || {
            groups
                .into_iter()
                .map(process_supervisor::stop_group)
                // Every group is stopped, also after one could not be.
                .collect::<Vec<_>>()
                .into_iter()
                .all(|stopped| stopped)
        })
        .await
        .unwrap_or(false);
        for command in &mut receipt.commands {
            command.process_tree_stopped = stopped;
        }
    }
    if let Some(path) = path.as_ref().filter(|_| witnessed) {
        let evidence = tokio::time::timeout(WITNESS_TIMEOUT, async {
            (
                git::get_current_sha(path).await.ok(),
                git::command_output(path, &["diff", "--quiet", "HEAD", "--"])
                    .await
                    .ok()
                    .map(|o| !o.status.success()),
            )
        })
        .await;
        if let Ok((head, tracked)) = evidence {
            receipt.finished_head = head;
            receipt.tracked_changes = tracked;
        }
        if canonical(&input.spec.execution_policy) {
            // One witness for the consumer: the commands ran on a clean
            // checkout and left no tracked change. A checkout that was not
            // clean before is reported as changed, and an unknown start as
            // unknown, so neither can certify a reusable result.
            receipt.tracked_changes = match (clean_before, receipt.tracked_changes) {
                (Some(true), after) => after,
                (Some(false), _) => Some(true),
                (None, _) => None,
            };
        }
    }
    if let Some(path) = path.as_ref() {
        if scratch.is_some()
            && receipt.outcome == CheckExecutionOutcome::Passed
            && (receipt.finished_head != receipt.prepared_head
                || receipt.tracked_changes != Some(false))
        {
            receipt.outcome = CheckExecutionOutcome::Failed;
        }
        if input.spec.declares_cleanup && !input.cleanup.commands.is_empty() {
            receipt.cleanup.outcome = CheckCleanupOutcome::Success;
            let deadline = Instant::now() + input.cleanup.timeout;
            // Cleanup always runs, including all declared commands after failure.
            for spec in input.cleanup.commands {
                match run_command(
                    path,
                    spec,
                    input.environment,
                    &input.spec.execution_policy,
                    Some(deadline),
                    &CancellationToken::new(),
                    input.output_limit,
                )
                .await
                {
                    Ok(command) => {
                        match command.outcome {
                            CheckExecutionOutcome::TimedOut => {
                                receipt.cleanup.outcome = CheckCleanupOutcome::TimedOut
                            }
                            CheckExecutionOutcome::Passed => {}
                            _ if receipt.cleanup.outcome != CheckCleanupOutcome::TimedOut => {
                                receipt.cleanup.outcome = CheckCleanupOutcome::Failed
                            }
                            _ => {}
                        }
                        receipt.cleanup.commands.push(command);
                    }
                    Err(error) => {
                        receipt.cleanup.outcome = CheckCleanupOutcome::Failed;
                        receipt.cleanup.message = Some(error);
                    }
                }
            }
        }
    } else if input.spec.declares_cleanup {
        receipt.cleanup.outcome = CheckCleanupOutcome::Failed;
        receipt.cleanup.message = Some("checkout unavailable for cleanup".into());
    }
    if let Some(directory) = scratch {
        // Removal has its own settlement bound; no pass is certified early.
        let removal = tokio::task::spawn_blocking(move || directory.close());
        match tokio::time::timeout(input.cleanup.timeout, removal).await {
            Ok(Ok(Ok(()))) => {
                receipt.cleanup.checkout_removed = true;
                if receipt.cleanup.outcome == CheckCleanupOutcome::NotPerformed {
                    receipt.cleanup.outcome = CheckCleanupOutcome::Success;
                }
            }
            _ => {
                receipt.cleanup.outcome = CheckCleanupOutcome::Uncertain;
                receipt.cleanup.message = Some("managed checkout removal not confirmed".into());
            }
        }
    }
    if receipt.outcome == CheckExecutionOutcome::Passed
        && !matches!(
            receipt.cleanup.outcome,
            CheckCleanupOutcome::Success | CheckCleanupOutcome::NotPerformed
        )
    {
        receipt.outcome = CheckExecutionOutcome::Infrastructure;
    }
    receipt.finished_at = now();
    receipt
}

#[cfg(test)]
mod tests;

/// Frozen legacy CI contract: current cwd, inherited shell environment and no
/// wall limit. Callers retain their existing per-command receipt/ack ordering.
pub fn legacy_ci_spec(
    text: &str,
    env: &BTreeMap<String, String>,
    seconds: u64,
    daemon: bool,
) -> CheckSpec {
    let blank = text.trim().is_empty();
    CheckSpec {
        schema_revision: CHECK_SPEC_REVISION,
        scope: CheckScope::Commit,
        commands: if blank {
            vec![]
        } else {
            vec![CheckCommandSpec {
                id: "ci:0".into(),
                shell_text: text.into(),
                shell: "bash -lc".into(),
                working_directory: CheckWorkingDirectory::TaskRoot,
                environment_keys: env.keys().cloned().collect(),
                timeout_seconds: (seconds > 0).then_some(seconds),
                failure_policy: CheckFailurePolicy::StopBundle,
                cacheability: CheckCacheability::Uncacheable,
                requirement_ids: Default::default(),
            }]
        },
        declares_cleanup: false,
        configured_commands: 1,
        blank_commands: if blank { vec![0] } else { vec![] },
        execution_policy: if daemon {
            "legacy-daemon/1"
        } else {
            "legacy-server/1"
        }
        .into(),
    }
}

/// Preserve the legacy consumer's configured indexes (including blank result
/// rows); execution of a blank command is dropped by legacy_ci_spec above.
pub fn legacy_ci_bundle(
    commands: &[String],
    env: &BTreeMap<String, String>,
    seconds: u64,
    daemon: bool,
) -> CheckSpec {
    let mut spec = legacy_ci_spec("template", env, seconds, daemon);
    let template = spec.commands[0].clone();
    spec.commands = commands
        .iter()
        .enumerate()
        .map(|(index, text)| {
            let mut command = template.clone();
            command.id = format!("ci:{index}");
            command.shell_text = text.clone();
            command
        })
        .collect();
    spec.configured_commands = spec.commands.len();
    spec
}

/// Ordered bundle policy, usable by phased legacy owners that retain/ack each
/// command before requesting the next. No execution or storage in this cursor.
pub struct CheckSequence {
    spec: CheckSpec,
    next: usize,
    stopped: bool,
}
impl CheckSequence {
    pub fn new(spec: CheckSpec) -> Self {
        Self {
            spec,
            next: 0,
            stopped: false,
        }
    }
    pub fn next_command(&self) -> Option<(usize, &CheckCommandSpec)> {
        if self.stopped {
            None
        } else {
            self.spec
                .commands
                .get(self.next)
                .map(|command| (self.next, command))
        }
    }
    pub fn completed(&mut self, outcome: CheckExecutionOutcome) {
        if let Some(command) = self.spec.commands.get(self.next) {
            self.stopped = outcome != CheckExecutionOutcome::Passed
                && (outcome != CheckExecutionOutcome::Failed
                    || command.failure_policy == CheckFailurePolicy::StopBundle);
            self.next += 1;
        }
    }
}
/// Legacy evidence projection shared by both CI consumers. Tails and the
/// signal-exit fallback are unchanged; output volume never selects the verdict.
pub struct LegacyCommandOutput<'a> {
    pub exit_code: Option<i32>,
    pub stdout: &'a str,
    pub stderr: &'a str,
}
pub fn command_outcome(
    index: usize,
    command: String,
    output: LegacyCommandOutput<'_>,
    env: &BTreeMap<String, String>,
    started_at: String,
    finished_at: String,
) -> CheckCommandOutcome {
    let stderr = executors::environment::redact_environment_values(output.stderr, env);
    let stdout = executors::environment::redact_environment_values(output.stdout, env);
    let combined = if stdout.is_empty() {
        stderr.clone()
    } else if stderr.is_empty() {
        stdout
    } else {
        format!("{stdout}\n{stderr}")
    };
    CheckCommandOutcome {
        index,
        command,
        exit_code: output.exit_code.filter(|code| *code >= 0).unwrap_or(1),
        stderr_tail: tail(&stderr, 4096),
        output_tail: tail(&combined, 4096),
        started_at,
        finished_at,
    }
}
