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
            for (offset, _) in value.char_indices().rev().filter(|(offset, _)| *offset > 0) {
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
/// Build a command using only the spec's declared keys. The two frozen legacy
/// policies retain ambient shell environment for equivalent caller migration.
/// All newly defined policies clear it. Both use the owner's current budget.
pub fn command(
    path: &Path,
    spec: &CheckCommandSpec,
    environment: &BTreeMap<String, String>,
    policy: &str,
) -> Result<(Command, BTreeMap<String, String>), String> {
    if spec.shell != "bash -lc" {
        return Err("unsupported check shell".into());
    }
    let env: BTreeMap<_, _> = environment
        .iter()
        .filter(|(key, _)| spec.environment_keys.contains(*key))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if spec
        .environment_keys
        .iter()
        .any(|key| !env.contains_key(key))
    {
        return Err("missing declared check environment key".into());
    }
    let mut command = Command::new("bash");
    if !matches!(policy, "legacy-server/1" | "legacy-daemon/1") {
        command.env_clear().arg("--noprofile");
    }
    command
        .arg("-lc")
        .arg(&spec.shell_text)
        .current_dir(path)
        .envs(&env);
    executors::run_process::apply(&mut command, &env);
    // Legacy unbounded server CI inherited these; bounded CI and daemon did not.
    if policy != "legacy-server/1" || spec.timeout_seconds.is_some() {
        command
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE");
    }
    if policy == "legacy-daemon/1" {
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
        return Ok(CheckCommandReceipt {
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
            process_tree_stopped: true,
            started_at,
            finished_at: now(),
        });
    }
    let output = process_supervisor::run(
        &mut command,
        process_supervisor::Capture::Tail(limit),
        deadline,
        cancel,
        process_supervisor::CompletionPolicy::StopDescendants,
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
    Ok(CheckCommandReceipt {
        id: spec.id.clone(),
        command: spec.shell_text.clone(),
        exit_code: output.status.code(),
        outcome,
        duration_ms: milliseconds(start.elapsed()),
        stdout_tail,
        stderr_tail,
        stdout_truncated,
        stderr_truncated,
        process_tree_stopped: output.descendants_stopped,
        started_at,
        finished_at: now(),
    })
}

pub async fn execute(input: CheckExecution<'_>) -> CheckReceipt {
    let mut receipt = CheckReceipt {
        operation_id: input.operation_id.to_owned(),
        owner: input.owner.clone(),
        execution_inputs: CheckEnvironmentIdentity::NotAttested,
        commands: Vec::new(),
        outcome: CheckExecutionOutcome::Passed,
        cleanup: CheckCleanupReceipt {
            outcome: CheckCleanupOutcome::NotPerformed,
            commands: Vec::new(),
            checkout_removed: false,
            message: None,
        },
        prepared_head: None,
        finished_head: None,
        tracked_changes: None,
        started_at: now(),
        finished_at: String::new(),
        infrastructure_message: None,
    };
    let mut scratch = None;
    let mut path = match input.target {
        CheckoutTarget::Workspace(path) => Some(path.to_owned()),
        _ => None,
    };
    let prepared = async {
        input.spec.validate()?;
        if input.deadline.is_none()
            && !matches!(
                input.spec.execution_policy.as_str(),
                "legacy-server/1" | "legacy-daemon/1"
            )
        {
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
    if receipt.outcome == CheckExecutionOutcome::Passed {
        if let Some(path) = path.as_ref() {
            receipt.prepared_head = git::get_current_sha(path).await.ok();
            let mut sequence = CheckSequence::new(input.spec.clone());
            while let Some((_, spec)) = sequence.next_command() {
                match run_command(
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
                    Ok(command) => {
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
    if let Some(path) = path.as_ref() {
        let evidence = tokio::time::timeout(Duration::from_secs(2), async {
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
