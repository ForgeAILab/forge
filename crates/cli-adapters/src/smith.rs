use api_types::{OrchestrationOutcome, ToolResultSummary};
use async_trait::async_trait;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, PermissionPolicy, SmithConfig, TokenUsage,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_SUMMARY_CHARS: usize = 500;

/// Terminal `result` statuses that mean the provider's quota pool is
/// exhausted (Smith has already rotated through the provider's credentials).
const SMITH_LIMIT_STATUSES: &[&str] = &[
    "usage_limited",
    "budget_limited",
    "limit_reached",
    "limit_exhausted",
    "rate_limited",
];

/// Structured runtime-event error kinds that signal quota exhaustion.
const SMITH_LIMIT_ERROR_KINDS: &[&str] = &["rate_limited", "limit_exhausted"];

/// Structured runtime-event error kinds that signal an auth failure a wait
/// cannot cure.
const SMITH_AUTH_ERROR_KINDS: &[&str] = &[
    "unauthorized",
    "credential_expired",
    "credential_invalid",
    "credential_missing",
];

struct RunningExecution {
    child: Arc<AsyncMutex<Child>>,
    cancelled: Arc<AtomicBool>,
}

pub struct SmithAdapter {
    executions: Arc<Mutex<HashMap<String, RunningExecution>>>,
}

impl SmithAdapter {
    pub fn new() -> Self {
        Self {
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> SmithConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(config: &SmithConfig, prompt: &str) -> tokio::process::Command {
        let mut adapter_args = vec![
            "-p".to_owned(),
            prompt.to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
        ];

        if config.yolo.unwrap_or(false) {
            adapter_args.push("--yolo".to_owned());
        } else if let Some(ref approval) = config.approval {
            adapter_args.push("--approval".to_owned());
            adapter_args.push(approval.clone());
        } else if let Some(ref policy) = config.permission_policy {
            match policy {
                PermissionPolicy::Auto => {
                    adapter_args.push("--yolo".to_owned());
                }
                PermissionPolicy::Supervised | PermissionPolicy::Plan => {
                    adapter_args.push("--approval".to_owned());
                    adapter_args.push("ask".to_owned());
                }
            }
        }

        if let Some(ref profile) = config.profile {
            adapter_args.push("--profile".to_owned());
            adapter_args.push(profile.clone());
        }

        if let Some(ref provider) = config.provider {
            adapter_args.push("--provider".to_owned());
            adapter_args.push(provider.clone());
        }

        if let Some(ref model) = config.model {
            adapter_args.push("--model".to_owned());
            adapter_args.push(model.clone());
        }

        if let Some(ref effort) = config.effort {
            adapter_args.push("--effort".to_owned());
            adapter_args.push(effort.clone());
        }

        if let Some(ref resume) = config.resume_session_id {
            adapter_args.push("--resume".to_owned());
            adapter_args.push(resume.clone());
        }

        let builder = crate::command::CommandBuilder::new("smith")
            .adapter_args(adapter_args)
            .overrides(&config.command_overrides);

        let mut cmd = builder.build();
        cmd.kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NO_COLOR", "1");
        cmd
    }

    fn insert_execution(
        &self,
        execution_id: String,
        execution: RunningExecution,
    ) -> Result<(), ExecutorError> {
        self.executions
            .lock()
            .map_err(|_| ExecutorError::Other("execution map lock poisoned".into()))?
            .insert(execution_id, execution);
        Ok(())
    }

    fn remove_execution(&self, execution_id: &str) -> Result<(), ExecutorError> {
        self.executions
            .lock()
            .map_err(|_| ExecutorError::Other("execution map lock poisoned".into()))?
            .remove(execution_id);
        Ok(())
    }
}

impl Default for SmithAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodingExecutorAdapter for SmithAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Smith
    }

    fn check_availability(&self) -> AvailabilityInfo {
        detect_smith_availability()
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        // Smith models, providers, and profiles are user-configured in
        // `~/.smith/config.toml`, not a fixed vendor list — surface whatever
        // the user actually has. Missing or unparsable config yields empty
        // lists rather than an error so unconfigured hosts still discover.
        let surface = match smith_user_config_path() {
            Some(path) => match tokio::fs::read_to_string(&path).await {
                Ok(text) => parse_smith_config_surface(&text),
                Err(_) => SmithConfigSurface::default(),
            },
            None => SmithConfigSurface::default(),
        };

        Ok(DiscoveredOptions {
            models: surface.models,
            permission_policies: vec!["auto".into(), "supervised".into()],
            cli_specific: serde_json::json!({
                "profiles": surface.profiles,
                "providers": surface.providers,
                "model_providers": surface.model_providers,
            }),
        })
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let config = Self::resolve_config(&ctx);
        let prompt = if let Some(template) = &config.prompt_template {
            format!("{template}\n\n{}", ctx.description)
        } else {
            ctx.description.clone()
        };

        let mut cmd = Self::build_command(&config, &prompt);
        cmd.current_dir(&ctx.worktree_path);

        let mut child = cmd.spawn()?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture smith stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture smith stderr".into()))?;

        let child_arc = Arc::new(AsyncMutex::new(child));
        let cancelled = Arc::new(AtomicBool::new(false));

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
                LogKind::User,
                LogStream::Main,
                serde_json::json!({
                    "text": prompt.chars().take(200).collect::<String>(),
                    "source": "forge_prompt",
                    "mode": "cli",
                }),
            )
            .await?;

        self.insert_execution(
            ctx.execution_id.clone(),
            RunningExecution {
                child: child_arc.clone(),
                cancelled: cancelled.clone(),
            },
        )?;

        let stream_result = stream_run_output(stdout, stderr, &mut writer).await;
        let status = {
            let mut child = child_arc.lock().await;
            child.wait().await?
        };
        self.remove_execution(&ctx.execution_id)?;

        let stream = stream_result?;

        if let Some(session_id) = &stream.agent_session_id {
            writer
                .write(
                    LogKind::SessionInfo,
                    LogStream::Main,
                    serde_json::json!({
                        "session_id": session_id,
                        "source": "smith_cli",
                        "resumed": config.resume_session_id.is_some(),
                    }),
                )
                .await?;
        }

        if let Some(summary) = &stream.summary {
            writer
                .write(
                    LogKind::Assistant,
                    LogStream::Main,
                    serde_json::json!({
                        "text": summary,
                        "source": "smith_cli",
                        "session_id": stream.agent_session_id.as_deref(),
                    }),
                )
                .await?;
        }

        if cancelled.load(Ordering::SeqCst) {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Cancelled,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                assistant_output: stream.assistant_output,
                summary: stream.summary,
                error: None,
                usage: stream.usage,
                ..Default::default()
            });
        }

        if let Some(availability) = availability_error(&stream, status.success()) {
            return Err(availability);
        }

        if let Some(error) = stream.error {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                assistant_output: stream.assistant_output,
                summary: stream.summary,
                error: Some(error),
                usage: stream.usage,
                ..Default::default()
            });
        }

        if !status.success() {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                assistant_output: stream.assistant_output,
                summary: stream.summary,
                error: Some(smith_run_error(status, &stream.stderr_tail)),
                usage: stream.usage,
                ..Default::default()
            });
        }

        let after_sha = if let Ok(false) =
            git::is_worktree_clean(Path::new(&ctx.worktree_path)).await
        {
            let subject = crate::commit::build_commit_subject(Some(&ctx.description), &ctx.task_id);
            crate::commit::commit_worktree_changes(Path::new(&ctx.worktree_path), &subject)
                .await
                .map_err(|err| {
                    ExecutorError::Other(format!("failed to commit worktree changes: {err}"))
                })?
        } else {
            None
        };

        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha,
            agent_session_id: stream.agent_session_id,
            assistant_output: stream.assistant_output,
            summary: stream.summary,
            error: None,
            usage: stream.usage,
            ..Default::default()
        })
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let running = {
            let executions = self
                .executions
                .lock()
                .map_err(|_| ExecutorError::Other("execution map lock poisoned".into()))?;
            executions.get(execution_id).map(|item| RunningExecution {
                child: item.child.clone(),
                cancelled: item.cancelled.clone(),
            })
        };

        if let Some(running) = running {
            running.cancelled.store(true, Ordering::SeqCst);
            let mut child = running.child.lock().await;
            child.start_kill()?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct LimitSignal {
    retry_after: Option<std::time::Duration>,
}

#[derive(Default)]
struct StreamResult {
    agent_session_id: Option<String>,
    assistant_output: Option<String>,
    summary: Option<String>,
    error: Option<String>,
    stderr_tail: String,
    usage: Option<TokenUsage>,
    /// Limit signal from the terminal `result` line — always classifies.
    terminal_limit: Option<LimitSignal>,
    /// Limit signal from a mid-run runtime event — classifies only when the
    /// run also ends badly (Smith may have recovered internally).
    observed_limit: Option<LimitSignal>,
    /// Structured auth-failure kind observed in runtime events.
    auth_failure: Option<String>,
}

/// Availability classification from structured stream signals only.
/// Assistant output text is never an input to this decision.
fn availability_error(stream: &StreamResult, exit_ok: bool) -> Option<ExecutorError> {
    let ended_badly = stream.error.is_some() || !exit_ok;
    let limit = stream.terminal_limit.clone().or_else(|| {
        if ended_badly {
            stream.observed_limit.clone()
        } else {
            None
        }
    });
    if let Some(signal) = limit {
        return Some(ExecutorError::UsageExhausted {
            retry_after: signal.retry_after,
            usage: stream.usage.clone(),
        });
    }
    match &stream.auth_failure {
        Some(kind) if ended_badly => Some(ExecutorError::Unavailable(format!(
            "smith authentication failure: {kind}"
        ))),
        _ => None,
    }
}

/// Extract a retry hint from `retry_after_ms` (relative) or
/// `limit_resets_at_ms` (epoch), directly or one level down.
fn parse_retry_after(value: &serde_json::Value) -> Option<std::time::Duration> {
    fn direct(value: &serde_json::Value) -> Option<std::time::Duration> {
        if let Some(ms) = value.get("retry_after_ms").and_then(|v| v.as_u64()) {
            return Some(std::time::Duration::from_millis(ms));
        }
        if let Some(resets_at) = value.get("limit_resets_at_ms").and_then(|v| v.as_u64()) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_millis() as u64;
            return Some(std::time::Duration::from_millis(
                resets_at.saturating_sub(now_ms),
            ));
        }
        None
    }
    direct(value).or_else(|| {
        ["error", "rate_limit", "limits", "usage"]
            .iter()
            .find_map(|key| value.get(key).and_then(direct))
    })
}

/// Structured error kind carried by a runtime event payload, if any.
fn runtime_error_kind(payload: &serde_json::Value) -> Option<&str> {
    payload
        .get("error")
        .and_then(|error| error.get("kind"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            if payload.get("event").and_then(|v| v.as_str()) == Some("error") {
                payload.get("kind").and_then(|v| v.as_str())
            } else {
                None
            }
        })
}

/// Builds the bounded `ToolResultSummary` for a smith `tool_call_finished`
/// runtime event.
///
/// The smith CLI's own runtime-event boundary only guarantees `is_error`, so
/// a structured Forge command outcome the underlying tool produced is
/// opportunistically recovered when smith nests it under `value` (the same
/// convention the native runtime path uses); any other payload is not vetted
/// safe to echo verbatim and receives a fixed, generic summary instead.
fn tool_result_summary_from_smith_payload(
    payload: &serde_json::Value,
    call_id: &str,
    is_error: bool,
) -> ToolResultSummary {
    payload
        .get("value")
        .cloned()
        .and_then(|value| serde_json::from_value::<OrchestrationOutcome>(value).ok())
        .map(|outcome| ToolResultSummary::from_orchestration_outcome(&outcome))
        .unwrap_or_else(|| ToolResultSummary::unclassified(is_error, call_id))
}

async fn stream_run_output(
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    writer: &mut LogWriter,
) -> Result<StreamResult, ExecutorError> {
    let mut stdout_reader = BufReader::new(stdout).lines();
    let mut stderr_reader = BufReader::new(stderr).lines();

    let mut result = StreamResult::default();
    let mut assistant_chunks = Vec::new();
    let mut stderr_lines = Vec::new();

    loop {
        tokio::select! {
            line = stdout_reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        process_smith_stdout_line(&line, writer, &mut result, &mut assistant_chunks).await?;
                    }
                    Ok(None) => break,
                    Err(err) => return Err(ExecutorError::Other(format!("failed to read smith stdout: {err}"))),
                }
            }
            line = stderr_reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        writer.write(LogKind::Stderr, LogStream::Main, serde_json::json!({
                            "text": line,
                            "source": "smith_cli_stderr",
                        })).await?;
                        if stderr_lines.len() >= 20 {
                            stderr_lines.remove(0);
                        }
                        stderr_lines.push(line);
                    }
                    Ok(None) => {}
                    Err(err) => return Err(ExecutorError::Other(format!("failed to read smith stderr: {err}"))),
                }
            }
        }
    }

    // Drain remaining stderr if any
    while let Ok(Some(line)) = stderr_reader.next_line().await {
        writer
            .write(
                LogKind::Stderr,
                LogStream::Main,
                serde_json::json!({
                    "text": line,
                    "source": "smith_cli_stderr",
                }),
            )
            .await?;
        if stderr_lines.len() >= 20 {
            stderr_lines.remove(0);
        }
        stderr_lines.push(line);
    }

    if !assistant_chunks.is_empty() {
        let full = assistant_chunks.join("");
        if result.assistant_output.is_none() {
            result.assistant_output = Some(full.clone());
        }
        if result.summary.is_none() {
            result.summary = Some(full.chars().take(MAX_SUMMARY_CHARS).collect());
        }
    }

    result.stderr_tail = stderr_lines.join("\n");
    Ok(result)
}

async fn process_smith_stdout_line(
    line: &str,
    writer: &mut LogWriter,
    result: &mut StreamResult,
    assistant_chunks: &mut Vec<String>,
) -> Result<(), ExecutorError> {
    let parsed: serde_json::Value = match serde_json::from_str(line) {
        Ok(val) => val,
        Err(_) => {
            // Unstructured stdout line
            writer
                .write(
                    LogKind::Stdout,
                    LogStream::Main,
                    serde_json::json!({
                        "text": line,
                        "source": "smith_cli_stdout",
                    }),
                )
                .await?;
            return Ok(());
        }
    };

    let line_type = parsed.get("type").and_then(|v| v.as_str());

    match line_type {
        Some("runtime_event") => {
            if let Some(event) = parsed.get("event") {
                let payload = event.get("payload");
                if result.agent_session_id.is_none() {
                    result.agent_session_id = event
                        .get("session")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned);
                }

                if let Some(payload) = payload {
                    if let Some(kind) = runtime_error_kind(payload) {
                        if SMITH_LIMIT_ERROR_KINDS.contains(&kind) {
                            result.observed_limit = Some(LimitSignal {
                                retry_after: parse_retry_after(payload),
                            });
                        } else if SMITH_AUTH_ERROR_KINDS.contains(&kind) {
                            result.auth_failure = Some(kind.to_owned());
                        }
                    }
                    let event_kind = payload.get("event").and_then(|v| v.as_str());
                    match event_kind {
                        Some("text_delta") => {
                            if let Some(text) = payload.get("text").and_then(|v| v.as_str()) {
                                assistant_chunks.push(text.to_owned());
                                writer
                                    .write(
                                        LogKind::Assistant,
                                        LogStream::Main,
                                        serde_json::json!({
                                            "text": text,
                                            "source": "smith_event",
                                        }),
                                    )
                                    .await?;
                            }
                        }
                        Some("tool_call_started") => {
                            writer
                                .write(
                                    LogKind::System,
                                    LogStream::Main,
                                    serde_json::json!({
                                        "event": event_kind,
                                        "payload": payload,
                                        "source": "smith_tool_event",
                                    }),
                                )
                                .await?;
                        }
                        Some("tool_call_finished") => {
                            let call_id = payload
                                .get("call")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default();
                            let name = payload
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or_default();
                            let is_error = payload
                                .get("is_error")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let summary =
                                tool_result_summary_from_smith_payload(payload, call_id, is_error);
                            writer
                                .write(
                                    LogKind::ToolResult,
                                    LogStream::Main,
                                    serde_json::json!({
                                        "call_id": call_id,
                                        "name": name,
                                        "is_error": is_error,
                                        "success": !is_error,
                                        "summary": summary,
                                        "source": "smith_tool_event",
                                    }),
                                )
                                .await?;
                        }
                        _ => {
                            writer
                                .write(
                                    LogKind::System,
                                    LogStream::Main,
                                    serde_json::json!({
                                        "event": payload,
                                        "source": "smith_runtime_event",
                                    }),
                                )
                                .await?;
                        }
                    }
                }
            }
        }
        Some("result") => {
            if let Some(session_id) = parsed.get("session_id").and_then(|v| v.as_str()) {
                result.agent_session_id = Some(session_id.to_owned());
            }

            if let Some(output) = parsed.get("output").and_then(|v| v.as_str()) {
                result.assistant_output = Some(output.to_owned());
                result.summary = Some(output.chars().take(MAX_SUMMARY_CHARS).collect());
            }

            let status = parsed.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if SMITH_LIMIT_STATUSES.contains(&status) {
                result.terminal_limit = Some(LimitSignal {
                    retry_after: parse_retry_after(&parsed),
                });
            }
            if status == "approval_required" {
                let approval_details = parsed
                    .get("approval_required")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "Approval required for tool execution".to_string());
                result.error = Some(format!("smith execution halted: {approval_details}"));
            } else if status != "ok" {
                result.error = Some(format!("smith returned non-ok status: {status}"));
            }

            if let Some(usage_json) = parsed.get("usage") {
                let current_turn = usage_json.get("current_turn");
                if let Some(turn) = current_turn {
                    let input = turn
                        .get("input_uncached")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let output = turn.get("output").and_then(|v| v.as_i64()).unwrap_or(0);
                    result.usage = Some(TokenUsage {
                        input_tokens: input,
                        output_tokens: output,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        cost_usd: None,
                        model: parsed
                            .get("model")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                    });
                }
            }

            writer
                .write(
                    LogKind::System,
                    LogStream::Main,
                    serde_json::json!({
                        "result": parsed,
                        "source": "smith_result",
                    }),
                )
                .await?;
        }
        _ => {
            writer
                .write(
                    LogKind::Stdout,
                    LogStream::Main,
                    serde_json::json!({
                        "text": line,
                        "source": "smith_cli_stdout",
                    }),
                )
                .await?;
        }
    }

    Ok(())
}

fn executable_in_path(name: &str) -> bool {
    which::which(name).is_ok()
}

fn smith_config_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|dir| dir.join(".smith"))
}

fn smith_user_config_path() -> Option<PathBuf> {
    smith_config_dir().map(|dir| dir.join("config.toml"))
}

#[derive(Default)]
struct SmithConfigSurface {
    models: Vec<String>,
    providers: Vec<String>,
    profiles: Vec<serde_json::Value>,
    model_providers: std::collections::BTreeMap<String, String>,
}

/// Extract the user-facing selection surface from a Smith `config.toml`.
///
/// Smith validates `--profile` / `--provider` / `--model` against the user's
/// config: profiles must be main-enabled (no `use` list, or one containing
/// `"main"`), and models resolve as bare names against the selected provider's
/// `[models."provider/model"]` catalog entries. Anything unparsable degrades to
/// an empty surface.
fn parse_smith_config_surface(text: &str) -> SmithConfigSurface {
    let Ok(root) = text.parse::<toml::Table>() else {
        return SmithConfigSurface::default();
    };

    let mut surface = SmithConfigSurface::default();
    let mut seen_models = std::collections::HashSet::new();

    if let Some(profiles) = root.get("profiles").and_then(|v| v.as_table()) {
        for (name, entry) in profiles {
            let Some(entry) = entry.as_table() else {
                continue;
            };
            let main_enabled = match entry.get("use").and_then(|v| v.as_array()) {
                Some(uses) => uses.iter().any(|u| u.as_str() == Some("main")),
                None => true,
            };
            if !main_enabled {
                continue;
            }
            let provider = entry.get("provider").and_then(|v| v.as_str());
            let model = entry.get("model").and_then(|v| v.as_str());
            if let Some(model) = model.filter(|m| seen_models.insert((*m).to_owned())) {
                surface.models.push(model.to_owned());
            }
            if let (Some(provider), Some(model)) = (provider, model) {
                surface
                    .model_providers
                    .entry(model.to_owned())
                    .or_insert_with(|| provider.to_owned());
            }
            surface.profiles.push(serde_json::json!({
                "name": name,
                "provider": provider,
                "model": model,
            }));
        }
    }

    if let Some(models) = root.get("models").and_then(|v| v.as_table()) {
        for key in models.keys() {
            let (provider, bare) = key
                .split_once('/')
                .map_or((None, key.as_str()), |(provider, model)| {
                    (Some(provider), model)
                });
            if seen_models.insert(bare.to_owned()) {
                surface.models.push(bare.to_owned());
            }
            if let Some(provider) = provider {
                surface
                    .model_providers
                    .entry(bare.to_owned())
                    .or_insert_with(|| provider.to_owned());
            }
        }
    }

    if let Some(providers) = root.get("providers").and_then(|v| v.as_table()) {
        surface.providers = providers.keys().cloned().collect();
    }

    surface
}

fn smith_run_error(status: std::process::ExitStatus, stderr_tail: &str) -> String {
    let mut message = format!("smith run exited with status {status}");
    if !stderr_tail.trim().is_empty() {
        message.push_str("\nstderr tail:\n");
        message.push_str(stderr_tail.trim());
    }
    message
}

fn detect_smith_availability() -> AvailabilityInfo {
    let config_dir = smith_config_dir();

    if executable_in_path("smith") {
        let auth_or_config_exists = config_dir
            .as_ref()
            .map(|d| d.join("config.toml").exists() || d.join("auth.json").exists())
            .unwrap_or(false);

        let env_key_set = std::env::vars().any(|(k, _)| k.starts_with("SMITH_"));

        let status = if auth_or_config_exists || env_key_set {
            AvailabilityStatus::Authenticated
        } else {
            AvailabilityStatus::Installed
        };

        return AvailabilityInfo {
            status,
            authenticated_at: None,
            config_path: config_dir
                .filter(|p| p.exists())
                .map(|p| p.to_string_lossy().into_owned()),
        };
    }

    AvailabilityInfo {
        status: AvailabilityStatus::NotFound,
        authenticated_at: None,
        config_path: None,
    }
}

mod git {
    use std::path::Path;
    use tokio::process::Command;

    pub async fn is_worktree_clean(worktree: &Path) -> Result<bool, std::io::Error> {
        let output = Command::new("git")
            .arg("status")
            .arg("--porcelain")
            .current_dir(worktree)
            .output()
            .await?;

        Ok(output.stdout.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn stream_fixture(lines: &[serde_json::Value]) -> StreamResult {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = LogWriter::new(
            dir.path().join("fixture.jsonl"),
            "exec-fixture".to_owned(),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let mut result = StreamResult::default();
        let mut chunks = Vec::new();
        for line in lines {
            process_smith_stdout_line(&line.to_string(), &mut writer, &mut result, &mut chunks)
                .await
                .expect("fixture line processes");
        }
        result
    }

    #[tokio::test]
    async fn terminal_limit_status_classifies_as_usage_exhausted() {
        let stream = stream_fixture(&[serde_json::json!({
            "type": "result",
            "status": "usage_limited",
            "session_id": "session-1",
            "retry_after_ms": 90_000,
            "usage": {"current_turn": {"input_uncached": 100, "output": 40}},
        })])
        .await;

        let error = availability_error(&stream, false).expect("classifies");
        match error {
            ExecutorError::UsageExhausted { retry_after, usage } => {
                assert_eq!(retry_after, Some(std::time::Duration::from_millis(90_000)));
                assert_eq!(usage.expect("partial usage carried").output_tokens, 40);
            }
            other => panic!("expected UsageExhausted, got {other}"),
        }
    }

    #[tokio::test]
    async fn runtime_limit_error_classifies_only_when_run_ends_badly() {
        let limit_event = serde_json::json!({
            "type": "runtime_event",
            "event": {
                "session": "session-1",
                "payload": {
                    "event": "provider_attempt_finished",
                    "error": {"kind": "limit_exhausted", "retry_after_ms": 30_000},
                },
            },
        });

        // Non-zero exit after the structured limit event → classified.
        let bad = stream_fixture(std::slice::from_ref(&limit_event)).await;
        assert!(matches!(
            availability_error(&bad, false),
            Some(ExecutorError::UsageExhausted { .. })
        ));

        // Smith recovered (ok result, zero exit) → no classification.
        let recovered = stream_fixture(&[
            limit_event,
            serde_json::json!({"type": "result", "status": "ok", "output": "done"}),
        ])
        .await;
        assert!(availability_error(&recovered, true).is_none());
    }

    #[tokio::test]
    async fn auth_error_kind_classifies_as_unavailable() {
        let stream = stream_fixture(&[serde_json::json!({
            "type": "runtime_event",
            "event": {
                "payload": {
                    "event": "error",
                    "error": {"kind": "credential_expired"},
                },
            },
        })])
        .await;

        match availability_error(&stream, false) {
            Some(ExecutorError::Unavailable(reason)) => {
                assert!(reason.contains("credential_expired"));
            }
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assistant_text_mentioning_rate_limit_never_classifies() {
        let stream = stream_fixture(&[
            serde_json::json!({
                "type": "runtime_event",
                "event": {
                    "payload": {
                        "event": "text_delta",
                        "text": "we hit a rate limit in the API under test; usage limit reached",
                    },
                },
            }),
            serde_json::json!({"type": "result", "status": "error"}),
        ])
        .await;

        // Even though the run ended badly, prose is not a signal.
        assert!(availability_error(&stream, false).is_none());
    }

    #[tokio::test]
    async fn result_preserves_full_assistant_output_beside_bounded_task_summary() {
        let output = "x".repeat(MAX_SUMMARY_CHARS + 300);
        let stream = stream_fixture(&[serde_json::json!({
            "type": "result",
            "status": "ok",
            "output": output,
        })])
        .await;

        assert_eq!(
            stream.summary.as_deref().map(str::len),
            Some(MAX_SUMMARY_CHARS)
        );
        assert_eq!(stream.assistant_output.as_deref(), Some(output.as_str()));
    }

    #[tokio::test]
    async fn limit_resets_at_epoch_produces_relative_retry() {
        let resets_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let stream = stream_fixture(&[serde_json::json!({
            "type": "result",
            "status": "limit_reached",
            "rate_limit": {"limit_resets_at_ms": resets_at_ms},
        })])
        .await;

        match availability_error(&stream, false) {
            Some(ExecutorError::UsageExhausted { retry_after, .. }) => {
                let retry = retry_after.expect("relative retry derived");
                assert!(retry <= std::time::Duration::from_millis(60_000));
                assert!(retry >= std::time::Duration::from_millis(50_000));
            }
            other => panic!("expected UsageExhausted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tool_call_finished_persists_a_bounded_summary_without_the_raw_payload() {
        // Characterizes F14 for the smith adapter path: `tool_call_finished`
        // previously logged the entire foreign runtime-event payload
        // verbatim, so anything smith's own process happened to include
        // (here simulated as a leaked secret) reached the durable log
        // unbounded and unredacted.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("fixture.jsonl");
        let mut writer = LogWriter::new(
            log_path.clone(),
            "exec-fixture".to_owned(),
            DEFAULT_MAX_OUTPUT_BYTES,
        );
        let mut result = StreamResult::default();
        let mut chunks = Vec::new();
        let line = serde_json::json!({
            "type": "runtime_event",
            "event": {
                "payload": {
                    "event": "tool_call_finished",
                    "call": "call-1",
                    "name": "forge_task_command",
                    "is_error": true,
                    "raw_output": "fatal: uncommitted worktree changes SECRET_TOKEN=abc123",
                },
            },
        })
        .to_string();

        process_smith_stdout_line(&line, &mut writer, &mut result, &mut chunks)
            .await
            .expect("line processes");

        let raw = tokio::fs::read_to_string(&log_path)
            .await
            .expect("durable log reads");
        assert!(!raw.contains("SECRET_TOKEN"));
        assert!(!raw.contains("raw_output"));

        let entry: serde_json::Value =
            serde_json::from_str(raw.lines().next().expect("one log line writes"))
                .expect("log entry parses as JSON");
        assert_eq!(entry["kind"], "tool_result");
        let payload = &entry["payload"];
        assert_eq!(payload["call_id"], "call-1");
        assert_eq!(payload["name"], "forge_task_command");
        assert_eq!(payload["is_error"], true);
        assert_eq!(payload["success"], false);
        assert_eq!(payload["summary"]["status"], "failed");
        assert_eq!(payload["summary"]["code"], "internal_failure");
        assert_eq!(payload["summary"]["correlation_id"], "call-1");
    }

    #[test]
    fn test_build_command() {
        let config = SmithConfig {
            model: Some("gemini-3.6-flash".into()),
            profile: Some("work".into()),
            yolo: Some(true),
            ..SmithConfig::default()
        };

        let cmd = SmithAdapter::build_command(&config, "test prompt");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();

        assert_eq!(cmd.as_std().get_program(), "smith");
        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&"test prompt".to_string()));
        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--yolo".to_string()));
        assert!(args.contains(&"--profile".to_string()));
        assert!(args.contains(&"work".to_string()));
        assert!(args.contains(&"--model".to_string()));
        assert!(args.contains(&"gemini-3.6-flash".to_string()));
    }

    #[test]
    fn build_command_forwards_reasoning_effort_as_effort_flag() {
        let config = SmithConfig {
            profile: Some("forge-coder".to_owned()),
            effort: Some("max".to_owned()),
            ..SmithConfig::default()
        };

        let cmd = SmithAdapter::build_command(&config, "test prompt");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();

        let effort_at = args
            .iter()
            .position(|arg| arg == "--effort")
            .expect("--effort is forwarded");
        assert_eq!(args[effort_at + 1], "max");
    }

    #[test]
    fn build_command_omits_effort_flag_when_unset() {
        let config = SmithConfig {
            profile: Some("forge-coder".to_owned()),
            ..SmithConfig::default()
        };

        let cmd = SmithAdapter::build_command(&config, "test prompt");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();

        assert!(!args.contains(&"--effort".to_string()));
    }

    #[test]
    fn config_surface_extracts_profiles_providers_and_models() {
        let config = r#"
[providers.google]
api = "gemini"

[providers.zai]
api = "anthropic"

[profiles.code]
use = ["main", "child"]
provider = "chatgpt"
model = "gpt-5.6-terra"

[profiles.gemini]
use = ["main"]
provider = "google"
model = "gemini-3.6-flash"

[profiles.child-only]
use = ["child"]
provider = "zai"
model = "glm-4.7"

[profiles.no-use-list]
provider = "zai"
model = "glm-5.2"

[models."chatgpt/gpt-5.6-terra"]
context_tokens = 400000

[models."zai/glm-5.2"]
context_tokens = 200000
"#;

        let surface = parse_smith_config_surface(config);

        let profile_names: Vec<_> = surface
            .profiles
            .iter()
            .map(|p| p["name"].as_str().unwrap().to_owned())
            .collect();
        assert!(profile_names.contains(&"code".to_owned()));
        assert!(profile_names.contains(&"gemini".to_owned()));
        // Profiles without a `use` list are main-enabled by default.
        assert!(profile_names.contains(&"no-use-list".to_owned()));
        assert!(!profile_names.contains(&"child-only".to_owned()));

        assert!(surface.models.contains(&"gpt-5.6-terra".to_owned()));
        assert!(surface.models.contains(&"gemini-3.6-flash".to_owned()));
        assert!(surface.models.contains(&"glm-5.2".to_owned()));
        assert!(!surface.models.contains(&"glm-4.7".to_owned()));
        assert_eq!(
            surface.model_providers.get("gpt-5.6-terra"),
            Some(&"chatgpt".to_owned())
        );
        assert_eq!(
            surface.model_providers.get("gemini-3.6-flash"),
            Some(&"google".to_owned())
        );
        assert_eq!(
            surface.model_providers.get("glm-5.2"),
            Some(&"zai".to_owned())
        );
        // Catalog keys are deduplicated against profile models.
        assert_eq!(
            surface
                .models
                .iter()
                .filter(|m| *m == "gpt-5.6-terra")
                .count(),
            1
        );

        assert_eq!(surface.providers, vec!["google", "zai"]);
    }

    #[test]
    fn config_surface_degrades_to_empty_on_invalid_toml() {
        let surface = parse_smith_config_surface("not [valid toml");
        assert!(surface.models.is_empty());
        assert!(surface.providers.is_empty());
        assert!(surface.profiles.is_empty());
        assert!(surface.model_providers.is_empty());
    }
}
