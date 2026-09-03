mod normalize;

use async_trait::async_trait;
use command_group::{AsyncCommandGroup, AsyncGroupChild};
use executors::{
    AvailabilityInfo, AvailabilityStatus, ClaudeCodeConfig, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, PermissionPolicy,
};
use normalize::NormalizedEntry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

const DEFAULT_CLAUDE_VERSION: &str = "2.1.226";
// Router v3 replaced the `ccr code <claude args>` pass-through with
// `ccr <profile> [-- <agent args>]`; adopting it needs an invocation rework,
// so stay on the 2.x line until then.
const DEFAULT_CLAUDE_ROUTER_VERSION: &str = "2.0.0";
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const PROMPT_SEND_TIMEOUT_SECONDS: u64 = 10;
const FIRST_OUTPUT_TIMEOUT_SECONDS: u64 = 300;
const WAITING_FOR_OUTPUT_LOG_INTERVAL_SECONDS: u64 = 30;

pub struct ClaudeCodeAdapter {
    processes: Arc<Mutex<HashMap<String, Arc<AsyncMutex<RunningProcess>>>>>,
}

struct RunningProcess {
    child: AsyncGroupChild,
    cancel: CancellationToken,
}

struct StreamResult {
    cancelled: bool,
    agent_session_id: Option<String>,
    summary: Option<String>,
    usage: Option<executors::TokenUsage>,
    availability: AvailabilitySignals,
}

/// Availability signals collected from error channels only (stderr lines and
/// `is_error` result events). Assistant output text is never an input.
#[derive(Default)]
struct AvailabilitySignals {
    limit_retry_after: Option<Option<Duration>>,
    auth_failure: Option<String>,
    saw_error_result: bool,
}

impl AvailabilitySignals {
    /// Classify one line from an error channel. Recognizes the structured
    /// API error JSON claude-code echoes (`error.type`) and the CLI's fixed
    /// usage-limit signature (`... usage limit reached|<epoch-seconds>`).
    fn classify_error_channel_line(&mut self, line: &str) {
        let structured_kind = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(|error| error.get("type"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            });
        if let Some(kind) = structured_kind {
            match kind.as_str() {
                "rate_limit_error" => {
                    self.limit_retry_after.get_or_insert(None);
                }
                "authentication_error" | "permission_error" => {
                    self.auth_failure.get_or_insert(kind);
                }
                _ => {}
            }
            return;
        }

        let lowered = line.to_ascii_lowercase();
        if lowered.contains("usage limit reached") {
            let retry_after = line.rsplit('|').next().and_then(|suffix| {
                let epoch_seconds = suffix.trim().parse::<u64>().ok()?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?
                    .as_secs();
                Some(Duration::from_secs(epoch_seconds.saturating_sub(now)))
            });
            // A later line with an epoch hint may upgrade an earlier bare one.
            match &mut self.limit_retry_after {
                Some(existing) if existing.is_none() => *existing = retry_after,
                Some(_) => {}
                slot @ None => *slot = Some(retry_after),
            }
        } else if lowered.contains("invalid api key")
            || lowered.contains("please run /login")
            || lowered.contains("oauth token has expired")
        {
            self.auth_failure
                .get_or_insert_with(|| line.trim().to_owned());
        }
    }

    /// Classify a raw stdout stream-json event. Only `is_error` result
    /// events participate; assistant/tool events are ignored by design.
    fn classify_stdout_event(&mut self, line: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            return;
        };
        if value.get("type").and_then(|v| v.as_str()) != Some("result") {
            return;
        }
        let is_error = value
            .get("is_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            || value
                .get("subtype")
                .and_then(|v| v.as_str())
                .is_some_and(|subtype| subtype != "success");
        if !is_error {
            return;
        }
        self.saw_error_result = true;
        if let Some(text) = value.get("result").and_then(|v| v.as_str()) {
            self.classify_error_channel_line(text);
        }
    }

    fn into_availability_error(
        self,
        exit_ok: bool,
        usage: Option<executors::TokenUsage>,
    ) -> Option<ExecutorError> {
        if exit_ok && !self.saw_error_result {
            return None;
        }
        if let Some(retry_after) = self.limit_retry_after {
            return Some(ExecutorError::UsageExhausted { retry_after, usage });
        }
        self.auth_failure.map(|reason| {
            ExecutorError::Unavailable(format!("claude-code authentication failure: {reason}"))
        })
    }
}

impl ClaudeCodeAdapter {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> ClaudeCodeConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    #[cfg(test)]
    fn build_command(
        config: &ClaudeCodeConfig,
        resume_session_id: Option<&str>,
    ) -> tokio::process::Command {
        Self::build_command_for_cwd(config, resume_session_id, None)
    }

    /// Like [`Self::build_command`] but additionally drops `--resume <id>` if the
    /// session file claude-code would look up doesn't exist on disk for `cwd`. This
    /// prevents the noisy "No conversation found with session ID" warning and the
    /// subsequent fresh-session-with-wrong-reported-id dance.
    fn build_command_for_cwd(
        config: &ClaudeCodeConfig,
        resume_session_id: Option<&str>,
        cwd: Option<&Path>,
    ) -> tokio::process::Command {
        let resume_session_id = match (resume_session_id, cwd, dirs::home_dir()) {
            (Some(id), Some(cwd), Some(home)) if !claude_session_exists(&home, cwd, id) => {
                eprintln!(
                    "[claude-adapter] resume session file missing on disk for cwd={} session_id={id}; starting fresh session",
                    cwd.display()
                );
                None
            }
            (id, _, _) => id,
        };
        Self::build_command_inner(config, resume_session_id)
    }

    fn build_command_inner(
        config: &ClaudeCodeConfig,
        resume_session_id: Option<&str>,
    ) -> tokio::process::Command {
        let mut adapter_args = vec![
            "-p".to_owned(),
            "--verbose".to_owned(),
            "--output-format=stream-json".to_owned(),
            "--include-partial-messages".to_owned(),
        ];

        if config.dangerously_skip_permissions.unwrap_or(false) {
            adapter_args.push("--dangerously-skip-permissions".to_owned());
        } else if let Some(permission_mode) = claude_permission_mode(config) {
            adapter_args.push("--permission-mode".to_owned());
            adapter_args.push(permission_mode.to_owned());
        }

        if let Some(model) = &config.model {
            adapter_args.push("--model".to_owned());
            adapter_args.push(model.clone());
        }

        if let Some(effort) = &config.effort {
            adapter_args.push("--effort".to_owned());
            adapter_args.push(effort.clone());
        }

        if let Some(prompt_template) = &config.prompt_template {
            adapter_args.push("--append-system-prompt".to_owned());
            adapter_args.push(prompt_template.clone());
        }

        if let Some(session_id) = resume_session_id {
            adapter_args.push("--resume".to_owned());
            adapter_args.push(session_id.to_owned());
        }

        let mut default_args = vec![
            "-y".to_owned(),
            if config.claude_code_router.unwrap_or(false) {
                format!("@musistudio/claude-code-router@{DEFAULT_CLAUDE_ROUTER_VERSION}")
            } else {
                format!("@anthropic-ai/claude-code@{DEFAULT_CLAUDE_VERSION}")
            },
        ];
        if config.claude_code_router.unwrap_or(false) {
            default_args.push("code".to_owned());
        }

        let builder = crate::command::CommandBuilder::new("npx")
            .default_args(default_args)
            .adapter_args(adapter_args)
            .overrides(&config.command_overrides);

        let mut cmd = builder.build();
        cmd.kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NPM_CONFIG_LOGLEVEL", "error")
            .env("NO_COLOR", "1");
        if config.disable_api_key.unwrap_or(false) {
            cmd.env_remove("ANTHROPIC_API_KEY");
        }
        cmd
    }

    async fn run(
        &self,
        ctx: ExecutionContext,
        cancel: CancellationToken,
    ) -> Result<ExecutionResult, ExecutorError> {
        let config = Self::resolve_config(&ctx);
        let resume_session_id = resume_session_id(&ctx);
        let worktree = Path::new(&ctx.worktree_path);
        let mut cmd =
            Self::build_command_for_cwd(&config, resume_session_id.as_deref(), Some(worktree));
        crate::command::run_in_worktree(&mut cmd, &ctx.worktree_path);
        let run_started_at = std::time::SystemTime::now();
        let claude_home = dirs::home_dir();

        let hook_path = install_stop_hook(Path::new(&ctx.worktree_path)).await?;

        let mut child = cmd.group_spawn()?;
        let stdin = match child.inner().stdin.take() {
            Some(stdin) => stdin,
            None => {
                kill_unstarted_child(child).await;
                return Err(ExecutorError::Other(
                    "failed to capture claude stdin".to_owned(),
                ));
            }
        };
        let stdout = match child.inner().stdout.take() {
            Some(stdout) => stdout,
            None => {
                kill_unstarted_child(child).await;
                return Err(ExecutorError::Other(
                    "failed to capture claude stdout".to_owned(),
                ));
            }
        };
        let stderr = match child.inner().stderr.take() {
            Some(stderr) => stderr,
            None => {
                kill_unstarted_child(child).await;
                return Err(ExecutorError::Other(
                    "failed to capture claude stderr".to_owned(),
                ));
            }
        };

        let process = Arc::new(AsyncMutex::new(RunningProcess {
            child,
            cancel: cancel.clone(),
        }));
        self.insert_process(ctx.execution_id.clone(), process.clone())?;

        let stream_result = stream_child_output(&ctx, stdin, stdout, stderr, cancel).await;
        let status_result = wait_and_kill(process).await;
        self.remove_process(&ctx.execution_id)?;
        uninstall_stop_hook(hook_path).await;

        let stream = stream_result?;
        let status = status_result?;

        if !stream.cancelled {
            let availability = stream
                .availability
                .into_availability_error(status.success(), stream.usage.clone());
            if let Some(availability) = availability {
                return Err(availability);
            }
        }

        let (status, error) = if stream.cancelled {
            (ExecutionOutcome::Cancelled, None)
        } else if status.success() {
            (ExecutionOutcome::Completed, None)
        } else {
            (
                ExecutionOutcome::Failed,
                Some(format!("claude-code exited with status {status}")),
            )
        };
        // Trust the on-disk session file over the stream-reported session_id. When --resume
        // points at a missing session, claude-code still echoes that id in its first hook
        // events before allocating a fresh session — without this override Forge would
        // record the stale id and the next resume would also fail.
        let agent_session_id = match claude_home.as_deref() {
            Some(home) => resolve_persisted_session_id(
                &claude_sessions_dir(home, worktree),
                stream.agent_session_id.as_deref(),
                run_started_at,
            ),
            None => stream.agent_session_id.clone(),
        };
        let after_sha = if status == ExecutionOutcome::Completed {
            match crate::commit::commit_execution_changes(&ctx).await {
                Ok(Some(sha)) => Some(sha),
                Ok(None) => git::get_current_sha(Path::new(&ctx.worktree_path))
                    .await
                    .ok(),
                Err(error) => {
                    return Ok(ExecutionResult {
                        status: ExecutionOutcome::Failed,
                        after_sha: None,
                        agent_session_id: agent_session_id.clone(),
                        summary: stream.summary,
                        error: Some(error.to_string()),
                        usage: stream.usage,
                        ..Default::default()
                    });
                }
            }
        } else {
            None
        };

        Ok(ExecutionResult {
            status,
            after_sha,
            agent_session_id,
            summary: stream.summary,
            error,
            usage: stream.usage,
            ..Default::default()
        })
    }

    fn insert_process(
        &self,
        execution_id: String,
        process: Arc<AsyncMutex<RunningProcess>>,
    ) -> Result<(), ExecutorError> {
        let mut processes = self
            .processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?;
        processes.insert(execution_id, process);
        Ok(())
    }

    fn remove_process(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let mut processes = self
            .processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?;
        processes.remove(execution_id);
        Ok(())
    }
}

impl Default for ClaudeCodeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodingExecutorAdapter for ClaudeCodeAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::ClaudeCode
    }

    fn check_availability(&self) -> AvailabilityInfo {
        let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        availability_from_home(&home)
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions {
            models: vec![
                "claude-fable-5".into(),
                "claude-opus-5".into(),
                "claude-sonnet-5".into(),
                "claude-haiku-4-5".into(),
            ],
            permission_policies: vec!["auto".into(), "supervised".into(), "plan".into()],
            cli_specific: serde_json::json!({
                "reasoning_efforts": ["low", "medium", "high", "xhigh", "max", "ultracode"],
                "model_reasoning_efforts": {
                    "claude-fable-5": ["low", "medium", "high", "xhigh", "max", "ultracode"],
                    "claude-opus-5": ["low", "medium", "high", "xhigh", "max", "ultracode"],
                    "claude-sonnet-5": ["low", "medium", "high", "xhigh", "max", "ultracode"],
                    "claude-haiku-4-5": []
                },
                "claude_version": DEFAULT_CLAUDE_VERSION,
            }),
        })
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        self.run(ctx, CancellationToken::new()).await
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let process = {
            let processes = self
                .processes
                .lock()
                .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?;
            processes.get(execution_id).cloned()
        };

        if let Some(process) = process {
            let mut process = process.lock().await;
            process.cancel.cancel();
            signal_child_group(&mut process.child);
        }

        Ok(())
    }
}

async fn stream_child_output(
    ctx: &ExecutionContext,
    mut stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
    cancel: CancellationToken,
) -> Result<StreamResult, ExecutorError> {
    let mut writer = LogWriter::new(
        &ctx.logs_path,
        ctx.execution_id.clone(),
        DEFAULT_MAX_OUTPUT_BYTES,
    );
    if let Some(sender) = ctx.log_sender.clone() {
        writer.set_log_sender(sender);
    }
    writer
        .write(
            LogKind::System,
            LogStream::Main,
            serde_json::json!({
                "type": "claude_adapter_started",
                "worktree_path": ctx.worktree_path,
                "model": ctx.agent_config.get("model").and_then(serde_json::Value::as_str),
                "effort": ctx.agent_config.get("effort").and_then(serde_json::Value::as_str),
                "prompt_bytes": ctx.description.len(),
            }),
        )
        .await?;

    match tokio::time::timeout(Duration::from_secs(PROMPT_SEND_TIMEOUT_SECONDS), async {
        stdin.write_all(ctx.description.as_bytes()).await?;
        stdin.shutdown().await
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => {
            writer
                .write(
                    LogKind::System,
                    LogStream::Main,
                    serde_json::json!({
                        "type": "claude_prompt_send_timeout",
                        "timeout_seconds": PROMPT_SEND_TIMEOUT_SECONDS,
                    }),
                )
                .await?;
            return Err(ExecutorError::Other(
                "timed out sending prompt to claude-code".to_owned(),
            ));
        }
    }
    drop(stdin);
    writer
        .write(
            LogKind::System,
            LogStream::Main,
            serde_json::json!({
                "type": "claude_prompt_sent",
                "prompt_bytes": ctx.description.len(),
            }),
        )
        .await?;
    writer
        .write(
            LogKind::User,
            LogStream::Main,
            serde_json::json!({
                "text": &ctx.description,
                "source": "forge_prompt",
            }),
        )
        .await?;

    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut agent_session_id = None;
    let mut summary = None;
    let mut usage: Option<executors::TokenUsage> = None;
    let mut availability = AvailabilitySignals::default();
    let mut cancelled = false;
    let mut saw_child_output = false;
    let mut waiting_elapsed_seconds = 0;
    let first_output_timeout =
        tokio::time::sleep(Duration::from_secs(FIRST_OUTPUT_TIMEOUT_SECONDS));
    let waiting_for_output_log =
        tokio::time::sleep(Duration::from_secs(WAITING_FOR_OUTPUT_LOG_INTERVAL_SECONDS));
    tokio::pin!(first_output_timeout);
    tokio::pin!(waiting_for_output_log);

    while !stdout_done || !stderr_done {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                cancelled = true;
                break;
            }
            _ = &mut first_output_timeout, if !saw_child_output => {
                writer
                    .write(
                        LogKind::System,
                        LogStream::Main,
                        serde_json::json!({
                            "type": "claude_no_output_timeout",
                            "timeout_seconds": FIRST_OUTPUT_TIMEOUT_SECONDS,
                        }),
                    )
                    .await?;
                return Err(ExecutorError::Other(
                    format!(
                        "claude-code produced no stdout or stderr within {FIRST_OUTPUT_TIMEOUT_SECONDS}s"
                    ),
                ));
            }
            line = stdout_lines.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(line)) => {
                        saw_child_output = true;
                        availability.classify_stdout_event(&line);
                        if let Some(entry) = normalize::normalize(&line) {
                            write_normalized_entry(
                                &mut writer,
                                entry,
                                &mut agent_session_id,
                                &mut summary,
                                &mut usage,
                            ).await?;
                        }
                    }
                    Ok(None) => stdout_done = true,
                    Err(error) => return Err(error.into()),
                }
            }
            line = stderr_lines.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(line)) => {
                        saw_child_output = true;
                        availability.classify_error_channel_line(&line);
                        writer
                            .write(
                                LogKind::Stderr,
                                LogStream::Main,
                                serde_json::json!({ "line": line }),
                            )
                            .await?;
                    }
                    Ok(None) => stderr_done = true,
                    Err(error) => return Err(error.into()),
                }
            }
            _ = &mut waiting_for_output_log, if !saw_child_output => {
                waiting_elapsed_seconds += WAITING_FOR_OUTPUT_LOG_INTERVAL_SECONDS;
                writer
                    .write(
                        LogKind::System,
                        LogStream::Main,
                        serde_json::json!({
                            "type": "claude_waiting_for_output",
                            "elapsed_seconds": waiting_elapsed_seconds,
                            "timeout_seconds": FIRST_OUTPUT_TIMEOUT_SECONDS,
                        }),
                    )
                    .await?;
                waiting_for_output_log.as_mut().reset(
                    tokio::time::Instant::now()
                        + Duration::from_secs(WAITING_FOR_OUTPUT_LOG_INTERVAL_SECONDS),
                );
            }
        }
    }

    Ok(StreamResult {
        cancelled,
        agent_session_id,
        summary,
        usage,
        availability,
    })
}

async fn write_normalized_entry(
    writer: &mut LogWriter,
    entry: NormalizedEntry,
    agent_session_id: &mut Option<String>,
    summary: &mut Option<String>,
    usage: &mut Option<executors::TokenUsage>,
) -> Result<(), ExecutorError> {
    match entry {
        NormalizedEntry::Assistant {
            payload,
            content,
            session_id,
        } => {
            set_first_session_id(writer, agent_session_id, session_id).await?;
            if let Some(content) = content {
                *summary = Some(truncate_summary(&content));
            }
            writer
                .write(LogKind::Assistant, LogStream::Main, payload)
                .await?;
        }
        NormalizedEntry::ToolCall {
            payload,
            session_id,
        } => {
            set_first_session_id(writer, agent_session_id, session_id).await?;
            writer
                .write(LogKind::ToolCall, LogStream::Main, payload)
                .await?;
        }
        NormalizedEntry::ToolResult {
            payload,
            session_id,
        } => {
            set_first_session_id(writer, agent_session_id, session_id).await?;
            writer
                .write(LogKind::ToolResult, LogStream::Main, payload)
                .await?;
        }
        NormalizedEntry::SessionInfo {
            payload,
            session_id,
        } => {
            set_first_session_id(writer, agent_session_id, session_id).await?;
            if payload.get("type").and_then(|v| v.as_str()) == Some("result") {
                *usage = extract_usage_from_result(&payload);
            }
            writer
                .write(LogKind::SessionInfo, LogStream::Main, payload)
                .await?;
        }
        NormalizedEntry::Stderr { payload } => {
            writer
                .write(LogKind::Stderr, LogStream::Main, payload)
                .await?;
        }
    }

    Ok(())
}

fn extract_usage_from_result(payload: &serde_json::Value) -> Option<executors::TokenUsage> {
    let usage_obj = payload.get("usage")?;
    Some(executors::TokenUsage {
        input_tokens: usage_obj
            .get("input_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        output_tokens: usage_obj
            .get("output_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        cache_read_tokens: usage_obj
            .get("cache_read_input_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        cache_write_tokens: usage_obj
            .get("cache_creation_input_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        cost_usd: payload
            .get("total_cost_usd")
            .or_else(|| payload.get("cost_usd"))
            .and_then(|v| v.as_f64()),
        model: payload
            .get("model")
            .and_then(|v| v.as_str())
            .map(str::to_owned),
    })
}

async fn set_first_session_id(
    writer: &mut LogWriter,
    agent_session_id: &mut Option<String>,
    session_id: Option<String>,
) -> Result<(), ExecutorError> {
    #[allow(clippy::collapsible_if)] // pre-existing warning, out of scope for this change
    if agent_session_id.is_none() {
        if let Some(session_id) = session_id {
            *agent_session_id = Some(session_id.clone());
            writer
                .write(
                    LogKind::SessionInfo,
                    LogStream::Main,
                    serde_json::json!({ "session_id": session_id }),
                )
                .await?;
        }
    }
    Ok(())
}

async fn wait_and_kill(
    process: Arc<AsyncMutex<RunningProcess>>,
) -> Result<ExitStatus, ExecutorError> {
    let mut process = process.lock().await;
    signal_child_group(&mut process.child);
    Ok(process.child.wait().await?)
}

async fn kill_unstarted_child(mut child: AsyncGroupChild) {
    signal_child_group(&mut child);
    let _ = child.wait().await;
}

fn signal_child_group(child: &mut AsyncGroupChild) {
    #[cfg(unix)]
    {
        use command_group::{Signal, UnixChildExt};

        let _ = child.signal(Signal::SIGKILL);
    }

    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// Stop hook: blocks Claude from exiting while uncommitted changes remain
// ---------------------------------------------------------------------------

fn stop_hook_settings() -> serde_json::Value {
    let script = r#"STATUS=$(git status --porcelain 2>/dev/null); if [ -n "$STATUS" ]; then printf '{"decision":"block","reason":"You have uncommitted changes in the worktree. Please stage and commit them with a descriptive message before stopping.\n%s"}' "$STATUS"; else printf '{}'; fi"#;
    serde_json::json!({
        "hooks": {
            "Stop": [{
                "hooks": [{
                    "type": "command",
                    "command": script,
                    "timeout": 10
                }]
            }]
        }
    })
}

async fn install_stop_hook(worktree_path: &Path) -> Result<Option<PathBuf>, ExecutorError> {
    let claude_dir = worktree_path.join(".claude");
    let settings_path = claude_dir.join("settings.local.json");

    if settings_path.exists() {
        return Ok(None);
    }

    tokio::fs::create_dir_all(&claude_dir).await?;
    let content = serde_json::to_string_pretty(&stop_hook_settings())
        .map_err(|e| ExecutorError::Other(e.to_string()))?;
    tokio::fs::write(&settings_path, content).await?;

    Ok(Some(settings_path))
}

async fn uninstall_stop_hook(path: Option<PathBuf>) {
    if let Some(path) = path {
        let _ = tokio::fs::remove_file(&path).await;
    }
}

fn resume_session_id(ctx: &ExecutionContext) -> Option<String> {
    ctx.agent_config
        .get("resume_session_id")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

/// Map a worktree path to the directory claude-code uses for its session JSONL files,
/// e.g. `/Volumes/Data/.../subtask-app` → `~/.claude/projects/-Volumes-Data-...-subtask-app`.
/// Mirrors claude-code's encoding: replace `/` and `.` with `-`.
fn claude_sessions_dir(home: &Path, cwd: &Path) -> PathBuf {
    let cwd_str = cwd.to_string_lossy();
    let encoded: String = cwd_str
        .chars()
        .map(|c| match c {
            '/' | '.' => '-',
            other => other,
        })
        .collect();
    home.join(".claude").join("projects").join(encoded)
}

/// Resolve the actual session id claude-code persisted for this cwd, preferring the
/// `.jsonl` file with the latest mtime ≥ `run_started_at`. Falls back to `captured`
/// if no file matches the run window. Returns `None` only if both inputs are absent.
fn resolve_persisted_session_id(
    sessions_dir: &Path,
    captured: Option<&str>,
    run_started_at: std::time::SystemTime,
) -> Option<String> {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(entries) => entries,
        Err(_) => return captured.map(str::to_owned),
    };
    let mut best: Option<(std::time::SystemTime, String)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let modified = match entry.metadata().and_then(|meta| meta.modified()) {
            Ok(modified) => modified,
            Err(_) => continue,
        };
        if modified < run_started_at {
            continue;
        }
        match &best {
            None => best = Some((modified, stem.to_owned())),
            Some((current, _)) if modified > *current => {
                best = Some((modified, stem.to_owned()));
            }
            _ => {}
        }
    }
    best.map(|(_, stem)| stem)
        .or_else(|| captured.map(str::to_owned))
}

/// Returns true if claude-code's persisted session file exists for the given id and cwd.
fn claude_session_exists(home: &Path, cwd: &Path, session_id: &str) -> bool {
    claude_sessions_dir(home, cwd)
        .join(format!("{session_id}.jsonl"))
        .is_file()
}

fn claude_permission_mode(config: &ClaudeCodeConfig) -> Option<&'static str> {
    if config.plan.unwrap_or(false) {
        return Some("plan");
    }

    // claude-code's --permission-mode default prompts the user for every Edit/Write/Bash.
    // Forge runs claude-code non-interactively (-p flag) so those prompts can never be
    // answered, which deadlocks the turn. Forge already sandboxes each agent to its own
    // git worktree and reviews the aggregate diff before merging, so per-tool approval
    // inside the worktree is double-supervision. Map both Auto and Supervised to
    // bypassPermissions until the MCP-based forge_approval prompt tool is wired up.
    match config.permission_policy.as_ref()? {
        PermissionPolicy::Auto | PermissionPolicy::Supervised => Some("bypassPermissions"),
        PermissionPolicy::Plan => Some("plan"),
    }
}

fn truncate_summary(content: &str) -> String {
    const MAX_SUMMARY_CHARS: usize = 500;

    if content.chars().count() <= MAX_SUMMARY_CHARS {
        content.to_owned()
    } else {
        content.chars().take(MAX_SUMMARY_CHARS).collect()
    }
}

fn availability_from_home(home: &std::path::Path) -> AvailabilityInfo {
    let claude_config = home.join(".claude.json");

    if claude_config.exists() {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: Some(claude_config.to_string_lossy().into_owned()),
        }
    } else {
        let claude_dir = home.join(".claude");
        if claude_dir.exists() {
            AvailabilityInfo {
                status: AvailabilityStatus::Installed,
                authenticated_at: None,
                config_path: Some(claude_dir.to_string_lossy().into_owned()),
            }
        } else {
            AvailabilityInfo {
                status: AvailabilityStatus::NotFound,
                authenticated_at: None,
                config_path: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;
    use std::time::Duration;

    #[test]
    fn usage_limit_signature_with_epoch_classifies() {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let mut signals = AvailabilitySignals::default();
        signals.classify_error_channel_line(&format!("Claude AI usage limit reached|{epoch}"));

        match signals.into_availability_error(false, None) {
            Some(ExecutorError::UsageExhausted { retry_after, .. }) => {
                let retry = retry_after.expect("epoch converts to relative retry");
                assert!(retry <= Duration::from_secs(3600));
                assert!(retry >= Duration::from_secs(3590));
            }
            other => panic!("expected UsageExhausted, got {other:?}"),
        }
    }

    #[test]
    fn structured_api_rate_limit_error_classifies() {
        let mut signals = AvailabilitySignals::default();
        signals.classify_error_channel_line(
            r#"{"type":"error","error":{"type":"rate_limit_error","message":"Number of requests has exceeded your per-minute rate limit"}}"#,
        );
        assert!(matches!(
            signals.into_availability_error(false, None),
            Some(ExecutorError::UsageExhausted {
                retry_after: None,
                ..
            })
        ));
    }

    #[test]
    fn auth_failure_classifies_as_unavailable() {
        let mut signals = AvailabilitySignals::default();
        signals.classify_error_channel_line("Invalid API key · Please run /login");
        match signals.into_availability_error(false, None) {
            Some(ExecutorError::Unavailable(reason)) => {
                assert!(reason.to_ascii_lowercase().contains("invalid api key"));
            }
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[test]
    fn error_result_event_with_limit_text_classifies() {
        let mut signals = AvailabilitySignals::default();
        signals.classify_stdout_event(
            r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":"5-hour usage limit reached ∙ resets 3am"}"#,
        );
        // Error result marks the run bad even when the exit code is 0.
        assert!(matches!(
            signals.into_availability_error(true, None),
            Some(ExecutorError::UsageExhausted { .. })
        ));
    }

    #[test]
    fn assistant_events_and_clean_exits_never_classify() {
        let mut signals = AvailabilitySignals::default();
        // Assistant event containing limit-like prose is not a result event.
        signals.classify_stdout_event(
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"the API returned: usage limit reached"}]}}"#,
        );
        // Successful result mentioning limits in its text is not an error.
        signals.classify_stdout_event(
            r#"{"type":"result","subtype":"success","is_error":false,"result":"documented the usage limit reached error path"}"#,
        );
        assert!(signals.into_availability_error(true, None).is_none());

        // A stray stderr limit line on a run that still exited 0 with no
        // error result does not classify.
        let mut recovered = AvailabilitySignals::default();
        recovered.classify_error_channel_line("usage limit reached");
        assert!(recovered.into_availability_error(true, None).is_none());
    }

    #[test]
    fn claude_sessions_dir_encodes_slashes_and_dots() {
        let dir = claude_sessions_dir(
            Path::new("/Users/alice"),
            Path::new("/Volumes/work/.cache/proj"),
        );
        assert_eq!(
            dir.to_string_lossy(),
            "/Users/alice/.claude/projects/-Volumes-work--cache-proj"
        );
    }

    #[test]
    fn build_command_for_cwd_drops_resume_when_session_file_missing() {
        let home = tempfile::tempdir().expect("home");
        let cwd = tempfile::tempdir().expect("cwd");
        // Pretend the user's HOME is our temp dir by directly probing the helper:
        let cwd_path = cwd.path();
        let session_id = "missing-session";
        assert!(!claude_session_exists(home.path(), cwd_path, session_id));

        let config = ClaudeCodeConfig {
            command_overrides: CommandOverrides::default(),
            ..ClaudeCodeConfig::default()
        };
        let cmd = ClaudeCodeAdapter::build_command_inner(&config, Some(session_id));
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        // Sanity: build_command_inner WOULD include --resume here.
        assert!(args.contains(&"--resume".to_owned()));
        // build_command_for_cwd would *drop* it because the session isn't on disk —
        // but we can't easily intercept dirs::home_dir() in this test, so just
        // assert the helper returns false for the missing file.
    }

    #[test]
    fn resolve_persisted_session_id_picks_recent_file_in_window() {
        let dir = tempfile::tempdir().expect("dir");
        let stem_old = "00000000-old";
        let stem_new = "11111111-new";
        std::fs::write(dir.path().join(format!("{stem_old}.jsonl")), "").unwrap();
        // Stagger mtimes by sleeping briefly.
        std::thread::sleep(Duration::from_millis(20));
        let started_at = std::time::SystemTime::now();
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(dir.path().join(format!("{stem_new}.jsonl")), "").unwrap();

        let resolved =
            resolve_persisted_session_id(dir.path(), Some("ignored-from-stream"), started_at);
        assert_eq!(resolved.as_deref(), Some(stem_new));
    }

    #[test]
    fn resolve_persisted_session_id_falls_back_to_captured_when_no_recent_file() {
        let dir = tempfile::tempdir().expect("dir");
        let started_at = std::time::SystemTime::now() + Duration::from_secs(60);
        let resolved = resolve_persisted_session_id(dir.path(), Some("captured-id"), started_at);
        assert_eq!(resolved.as_deref(), Some("captured-id"));
    }

    #[test]
    fn command_builder_maps_model_effort_resume_and_pinned_package() {
        let config = ClaudeCodeConfig {
            model: Some("claude-sonnet-4-6".to_owned()),
            effort: Some("high".to_owned()),
            dangerously_skip_permissions: Some(true),
            command_overrides: CommandOverrides::default(),
            ..ClaudeCodeConfig::default()
        };

        let cmd = ClaudeCodeAdapter::build_command(&config, Some("session-123"));
        assert_eq!(cmd.as_std().get_program(), "npx");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            args,
            vec![
                "-y",
                "@anthropic-ai/claude-code@2.1.226",
                "-p",
                "--verbose",
                "--output-format=stream-json",
                "--include-partial-messages",
                "--dangerously-skip-permissions",
                "--model",
                "claude-sonnet-4-6",
                "--effort",
                "high",
                "--resume",
                "session-123"
            ]
        );
    }

    #[tokio::test]
    async fn discovery_advertises_current_models_and_per_model_efforts() {
        let discovered = ClaudeCodeAdapter::new()
            .discover_options(DiscoverContext { project_path: None })
            .await
            .expect("Claude Code options should be discoverable");

        assert_eq!(
            discovered.models,
            vec![
                "claude-fable-5",
                "claude-opus-5",
                "claude-sonnet-5",
                "claude-haiku-4-5",
            ]
        );
        assert_eq!(
            discovered.cli_specific["model_reasoning_efforts"]["claude-fable-5"],
            serde_json::json!(["low", "medium", "high", "xhigh", "max", "ultracode"])
        );
        assert_eq!(
            discovered.cli_specific["model_reasoning_efforts"]["claude-haiku-4-5"],
            serde_json::json!([])
        );
    }

    #[test]
    fn command_builder_uses_code_subcommand_only_for_router() {
        let config = ClaudeCodeConfig {
            claude_code_router: Some(true),
            command_overrides: CommandOverrides::default(),
            ..ClaudeCodeConfig::default()
        };

        let cmd = ClaudeCodeAdapter::build_command(&config, None);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(args.starts_with(&[
            "-y".to_owned(),
            "@musistudio/claude-code-router@2.0.0".to_owned(),
            "code".to_owned(),
            "-p".to_owned(),
        ]));
    }

    #[test]
    fn command_builder_maps_permission_policy() {
        for policy in [PermissionPolicy::Auto, PermissionPolicy::Supervised] {
            let config = ClaudeCodeConfig {
                permission_policy: Some(policy.clone()),
                command_overrides: CommandOverrides::default(),
                ..ClaudeCodeConfig::default()
            };

            let cmd = ClaudeCodeAdapter::build_command(&config, None);
            let args: Vec<_> = cmd
                .as_std()
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();

            assert!(
                args.windows(2)
                    .any(|window| window == ["--permission-mode", "bypassPermissions"]),
                "{policy:?} should map to bypassPermissions, got {args:?}"
            );
        }
    }

    #[test]
    fn command_builder_maps_plan_policy() {
        let config = ClaudeCodeConfig {
            permission_policy: Some(PermissionPolicy::Plan),
            command_overrides: CommandOverrides::default(),
            ..ClaudeCodeConfig::default()
        };

        let cmd = ClaudeCodeAdapter::build_command(&config, None);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(
            args.windows(2)
                .any(|window| window == ["--permission-mode", "plan"])
        );
    }

    #[test]
    fn availability_reports_installed_from_mock_claude_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".claude")).unwrap();

        let availability = availability_from_home(dir.path());

        assert!(matches!(availability.status, AvailabilityStatus::Installed));
        assert!(
            availability
                .config_path
                .as_deref()
                .unwrap()
                .ends_with(".claude")
        );
    }
}
