pub mod client;
pub mod jsonrpc;
pub mod normalize;
pub mod protocol;

use async_trait::async_trait;
use client::CodexClient;
use command_group::{AsyncCommandGroup, AsyncGroupChild};
#[cfg(unix)]
use command_group::{Signal, UnixChildExt};
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodexConfig, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, PermissionPolicy,
};
use protocol::{
    AskForApproval, SandboxMode, ThreadForkParams, ThreadForkResponse, ThreadResumeParams,
    ThreadResumeResponse, ThreadStartParams, ThreadStartResponse, TurnHandle,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

const DEFAULT_CODEX_VERSION: &str = "0.147.0";
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const CODEX_SYSTEM_ERROR_FALLBACK: &str = "codex thread entered systemError status";

#[derive(Clone)]
struct RunningProcess {
    child: Arc<AsyncMutex<AsyncGroupChild>>,
    cancel: CancellationToken,
}

struct CleanupSignalGuard {
    child: Arc<AsyncMutex<AsyncGroupChild>>,
    armed: bool,
}

impl CleanupSignalGuard {
    fn new(child: Arc<AsyncMutex<AsyncGroupChild>>) -> Self {
        Self { child, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CleanupSignalGuard {
    fn drop(&mut self) {
        if self.armed
            && let Ok(mut child) = self.child.try_lock()
        {
            signal_child(&mut child);
        }
    }
}

pub struct CodexAdapter {
    processes: Arc<Mutex<HashMap<String, RunningProcess>>>,
}

impl CodexAdapter {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> CodexConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(config: &CodexConfig) -> tokio::process::Command {
        let overrides = &config.command_overrides;
        let builder = crate::command::CommandBuilder::new("npx")
            .default_args(vec![
                "-y".to_owned(),
                format!("@openai/codex@{DEFAULT_CODEX_VERSION}"),
            ])
            .adapter_args(vec!["app-server".to_owned()])
            .overrides(overrides);
        let mut cmd = builder.build();
        cmd.kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NPM_CONFIG_LOGLEVEL", "error")
            .env("NODE_NO_WARNINGS", "1")
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "error");
        cmd
    }

    fn thread_start_params(config: &CodexConfig, worktree_path: &str) -> ThreadStartParams {
        let permission = config.permission_policy.clone().unwrap_or_default();
        let fallback_sandbox = match permission {
            PermissionPolicy::Auto | PermissionPolicy::Supervised => SandboxMode::WorkspaceWrite,
            PermissionPolicy::Plan => SandboxMode::ReadOnly,
        };
        let fallback_approval = match permission {
            PermissionPolicy::Auto => AskForApproval::Never,
            PermissionPolicy::Supervised | PermissionPolicy::Plan => AskForApproval::OnRequest,
        };

        let mut config_overrides = HashMap::new();
        if let Some(effort) = &config.model_reasoning_effort {
            config_overrides.insert(
                "model_reasoning_effort".to_owned(),
                Value::String(effort.clone()),
            );
        }
        if let Some(summary) = &config.model_reasoning_summary {
            config_overrides.insert(
                "model_reasoning_summary".to_owned(),
                Value::String(summary.clone()),
            );
        }
        if let Some(profile) = &config.profile {
            config_overrides.insert("profile".to_owned(), Value::String(profile.clone()));
        }
        if let Some(include) = config.include_apply_patch_tool {
            config_overrides.insert("include_apply_patch_tool".to_owned(), Value::Bool(include));
        }

        ThreadStartParams {
            model: config.model.clone(),
            model_provider: None,
            cwd: Some(worktree_path.to_owned()),
            approval_policy: Some(AskForApproval::from_config(
                config.ask_for_approval.as_deref(),
                fallback_approval,
            )),
            sandbox: Some(SandboxMode::from_config(
                config.sandbox.as_deref(),
                fallback_sandbox,
            )),
            config: (!config_overrides.is_empty()).then_some(config_overrides),
            base_instructions: config
                .base_instructions
                .clone()
                .or_else(|| config.prompt_template.clone()),
            developer_instructions: config.developer_instructions.clone(),
            service_tier: None,
        }
    }

    fn insert_process(
        &self,
        execution_id: String,
        running: RunningProcess,
    ) -> Result<(), ExecutorError> {
        self.processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?
            .insert(execution_id, running);
        Ok(())
    }

    fn remove_process(&self, execution_id: &str) -> Result<(), ExecutorError> {
        self.processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?
            .remove(execution_id);
        Ok(())
    }
}

impl Default for CodexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
trait CodexSessionClient {
    async fn thread_start(
        &mut self,
        params: ThreadStartParams,
    ) -> Result<ThreadStartResponse, ExecutorError>;

    async fn thread_fork(
        &mut self,
        params: ThreadForkParams,
    ) -> Result<ThreadForkResponse, ExecutorError>;

    async fn thread_resume(
        &mut self,
        params: ThreadResumeParams,
    ) -> Result<ThreadResumeResponse, ExecutorError>;

    async fn turn_start(
        &mut self,
        thread_id: String,
        prompt: String,
    ) -> Result<TurnHandle, ExecutorError>;
}

#[async_trait]
impl CodexSessionClient for CodexClient {
    async fn thread_start(
        &mut self,
        params: ThreadStartParams,
    ) -> Result<ThreadStartResponse, ExecutorError> {
        CodexClient::thread_start(self, params).await
    }

    async fn thread_fork(
        &mut self,
        params: ThreadForkParams,
    ) -> Result<ThreadForkResponse, ExecutorError> {
        CodexClient::thread_fork(self, params).await
    }

    async fn thread_resume(
        &mut self,
        params: ThreadResumeParams,
    ) -> Result<ThreadResumeResponse, ExecutorError> {
        CodexClient::thread_resume(self, params).await
    }

    async fn turn_start(
        &mut self,
        thread_id: String,
        prompt: String,
    ) -> Result<TurnHandle, ExecutorError> {
        CodexClient::turn_start(self, thread_id, prompt).await
    }
}

#[async_trait]
impl CodingExecutorAdapter for CodexAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Codex
    }

    fn check_availability(&self) -> AvailabilityInfo {
        let codex_home = std::env::var("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| dirs_path("codex"));

        availability_from_codex_home(&codex_home)
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions {
            models: vec![
                "gpt-5.6-sol".into(),
                "gpt-5.6-terra".into(),
                "gpt-5.6-luna".into(),
                "gpt-5.5".into(),
                "gpt-5.4".into(),
                "gpt-5.4-mini".into(),
                "gpt-5.3-codex-spark".into(),
            ],
            permission_policies: vec!["auto".into(), "supervised".into(), "plan".into()],
            cli_specific: json!({
                "sandbox_modes": ["read-only", "workspace-write", "danger-full-access"],
                "approval_modes": ["never", "on-request", "on-failure", "unless-trusted"],
                "reasoning_efforts": ["low", "medium", "high", "xhigh", "max", "ultra"],
                "model_reasoning_efforts": {
                    "gpt-5.6-sol": ["low", "medium", "high", "xhigh", "max", "ultra"],
                    "gpt-5.6-terra": ["low", "medium", "high", "xhigh", "max", "ultra"],
                    "gpt-5.6-luna": ["low", "medium", "high", "xhigh", "max"],
                    "gpt-5.5": ["low", "medium", "high", "xhigh"],
                    "gpt-5.4": ["low", "medium", "high", "xhigh"],
                    "gpt-5.4-mini": ["low", "medium", "high", "xhigh"],
                    "gpt-5.3-codex-spark": ["low", "medium", "high", "xhigh"]
                },
                "codex_version": DEFAULT_CODEX_VERSION,
            }),
        })
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let config = Self::resolve_config(&ctx);
        let mut command = Self::build_command(&config);
        crate::command::run_in_worktree(&mut command, &ctx.worktree_path);
        let mut child = command.group_spawn()?;

        let stdout = match child.inner().stdout.take() {
            Some(stdout) => stdout,
            None => {
                cleanup_child(&mut child).await;
                return Err(ExecutorError::Other(
                    "failed to capture codex stdout".to_owned(),
                ));
            }
        };
        let stdin = match child.inner().stdin.take() {
            Some(stdin) => stdin,
            None => {
                cleanup_child(&mut child).await;
                return Err(ExecutorError::Other(
                    "failed to capture codex stdin".to_owned(),
                ));
            }
        };
        let stderr = match child.inner().stderr.take() {
            Some(stderr) => stderr,
            None => {
                cleanup_child(&mut child).await;
                return Err(ExecutorError::Other(
                    "failed to capture codex stderr".to_owned(),
                ));
            }
        };

        let cancel = CancellationToken::new();
        let child = Arc::new(AsyncMutex::new(child));
        let mut cleanup_guard = CleanupSignalGuard::new(child.clone());
        self.insert_process(
            ctx.execution_id.clone(),
            RunningProcess {
                child: child.clone(),
                cancel: cancel.clone(),
            },
        )?;

        let writer = Arc::new(AsyncMutex::new(LogWriter::new(
            &ctx.logs_path,
            ctx.execution_id.clone(),
            DEFAULT_MAX_OUTPUT_BYTES,
        )));
        {
            let mut w = writer.lock().await;
            if let Some(sender) = ctx.log_sender.clone() {
                w.set_log_sender(sender);
            }
        }
        let (stderr_tx, stderr_rx) = mpsc::channel(256);
        tokio::spawn(read_stderr(stderr, stderr_tx));

        let result = self
            .drive_codex(
                ctx.clone(),
                config,
                stdin,
                stdout,
                stderr_rx,
                writer,
                cancel,
            )
            .await;

        self.remove_process(&ctx.execution_id)?;
        {
            let mut guard = child.lock().await;
            cleanup_child(&mut guard).await;
        }
        cleanup_guard.disarm();

        result
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let running = self
            .processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?
            .get(execution_id)
            .cloned();

        if let Some(running) = running {
            running.cancel.cancel();
            let mut child = running.child.lock().await;
            signal_child(&mut child);
        }

        Ok(())
    }
}

impl CodexAdapter {
    async fn start_codex_session<C>(
        config: &CodexConfig,
        ctx: &ExecutionContext,
        client: &mut C,
    ) -> Result<(String, Option<String>), ExecutorError>
    where
        C: CodexSessionClient + Send,
    {
        if let Some(resume_thread_id) = config.resume_thread_id.clone() {
            if config.resume_thread_in_place.unwrap_or(false) {
                // Follow-up/chat resumes must continue the same Codex thread so
                // history and token cache are preserved. Each Forge turn starts a
                // fresh app-server process, so reload the saved thread before
                // sending the follow-up turn.
                let resumed_thread_id = match client
                    .thread_resume(ThreadResumeParams::from_start(
                        resume_thread_id,
                        Self::thread_start_params(config, &ctx.worktree_path),
                    ))
                    .await
                {
                    Ok(response) => {
                        response.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                            ExecutorError::Other(
                                "codex thread/resume response missing thread id".to_owned(),
                            )
                        })?
                    }
                    Err(error) if is_missing_codex_thread_error(&error) => {
                        let response = client
                            .thread_start(Self::thread_start_params(config, &ctx.worktree_path))
                            .await?;
                        let thread_id =
                            response.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                                ExecutorError::Other(
                                    "codex thread/start response missing thread id".to_owned(),
                                )
                            })?;
                        let prompt = config
                            .resume_fallback_prompt
                            .clone()
                            .unwrap_or_else(|| ctx.description.clone());
                        let turn = client.turn_start(thread_id.clone(), prompt).await?;
                        return Ok((thread_id, turn.turn_id));
                    }
                    Err(error) => return Err(error),
                };
                let turn = client
                    .turn_start(resumed_thread_id.clone(), ctx.description.clone())
                    .await?;
                return Ok((resumed_thread_id, turn.turn_id));
            }
            let thread_params = Self::thread_start_params(config, &ctx.worktree_path);
            let fork = client
                .thread_fork(ThreadForkParams::from_start(
                    resume_thread_id,
                    thread_params,
                ))
                .await?;
            let forked_thread_id = fork.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                ExecutorError::Other("codex thread/fork response missing thread id".to_owned())
            })?;
            let turn = client
                .turn_start(forked_thread_id.clone(), ctx.description.clone())
                .await?;
            Ok((forked_thread_id, turn.turn_id))
        } else {
            let thread_params = Self::thread_start_params(config, &ctx.worktree_path);
            let response = client.thread_start(thread_params).await?;
            let thread_id = response.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                ExecutorError::Other("codex thread/start response missing thread id".to_owned())
            })?;
            let turn = client
                .turn_start(thread_id.clone(), ctx.description.clone())
                .await?;
            Ok((thread_id, turn.turn_id))
        }
    }

    #[allow(clippy::too_many_arguments)] // pre-existing warning, out of scope for this change
    async fn drive_codex(
        &self,
        ctx: ExecutionContext,
        config: CodexConfig,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        stderr_rx: mpsc::Receiver<String>,
        writer: Arc<AsyncMutex<LogWriter>>,
        cancel: CancellationToken,
    ) -> Result<ExecutionResult, ExecutorError> {
        let mut client = CodexClient::spawn(stdin, stdout, &ctx.worktree_path, cancel.clone());

        client.initialize().await?;
        client.initialized().await?;

        let (thread_id, turn_id) = Self::start_codex_session(&config, &ctx, &mut client).await?;

        write_shared_log(
            &writer,
            LogKind::SessionInfo,
            json!({ "thread_id": thread_id }),
        )
        .await?;

        let run = client
            .run_until_turn_complete(writer.clone(), stderr_rx, ctx.heartbeat_interval_seconds)
            .await?;
        let mut outcome = run.outcome.unwrap_or(ExecutionOutcome::Failed);
        let mut summary = run.summary;
        let mut usage = run.usage;

        if outcome == ExecutionOutcome::Cancelled {
            let _ = client.cancel_turn(thread_id.clone(), turn_id).await;
        }

        let auto_commit = config.auto_commit.unwrap_or(true);
        // Commit reminder: if completed but worktree is dirty, nudge Codex once
        if auto_commit
            && outcome == ExecutionOutcome::Completed
            && let Ok(false) = git::is_worktree_clean(Path::new(&ctx.worktree_path)).await
        {
            let status_lines = git::status_porcelain(Path::new(&ctx.worktree_path))
                .await
                .unwrap_or_default()
                .join("\n");
            let reminder = format!(
                "You have uncommitted changes in the worktree. \
                 Please stage and commit them with a descriptive message before stopping.\n{status_lines}"
            );
            if let Ok(_handle) = client.turn_start(thread_id.clone(), reminder).await {
                let (_, empty_rx) = mpsc::channel(1);
                if let Ok(followup) = client
                    .run_until_turn_complete(
                        writer.clone(),
                        empty_rx,
                        ctx.heartbeat_interval_seconds,
                    )
                    .await
                {
                    outcome = followup.outcome.unwrap_or(outcome);
                    if let Some(s) = followup.summary {
                        summary = Some(s);
                    }
                    usage = merge_usage(usage, followup.usage);
                }
            }
        }

        if outcome != ExecutionOutcome::Completed {
            let error = match outcome {
                ExecutionOutcome::Completed => None,
                ExecutionOutcome::Cancelled => Some("codex execution cancelled".to_owned()),
                ExecutionOutcome::Failed => run
                    .error
                    .or_else(|| summary.clone())
                    .or_else(|| Some("codex turn failed".to_owned())),
            };
            return Ok(ExecutionResult {
                status: outcome.clone(),
                after_sha: None,
                agent_session_id: run.thread_id.or(Some(thread_id)),
                summary,
                error,
                usage: usage.map(|usage| usage_with_model(usage, &config)),
                ..Default::default()
            });
        }

        let agent_session_id = run.thread_id.or(Some(thread_id));
        if !auto_commit {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                after_sha: git::get_current_sha(Path::new(&ctx.worktree_path))
                    .await
                    .ok(),
                agent_session_id,
                summary,
                error: None,
                usage: usage.map(|usage| usage_with_model(usage, &config)),
                ..Default::default()
            });
        }
        let after_sha = match crate::commit::commit_execution_changes(&ctx).await {
            Ok(Some(sha)) => Some(sha),
            Ok(None) => git::get_current_sha(Path::new(&ctx.worktree_path))
                .await
                .ok(),
            Err(error) => {
                return Ok(ExecutionResult {
                    status: ExecutionOutcome::Failed,
                    after_sha: None,
                    agent_session_id,
                    summary,
                    error: Some(error.to_string()),
                    usage: usage.map(|usage| usage_with_model(usage, &config)),
                    ..Default::default()
                });
            }
        };

        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha,
            agent_session_id,
            summary,
            error: None,
            usage: usage.map(|usage| usage_with_model(usage, &config)),
            ..Default::default()
        })
    }
}

fn is_missing_codex_thread_error(error: &ExecutorError) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("thread not found")
}

fn merge_usage(
    current: Option<executors::TokenUsage>,
    next: Option<executors::TokenUsage>,
) -> Option<executors::TokenUsage> {
    match (current, next) {
        (None, next) => next,
        (current, None) => current,
        (Some(mut current), Some(next)) => {
            current.input_tokens += next.input_tokens;
            current.output_tokens += next.output_tokens;
            current.cache_read_tokens += next.cache_read_tokens;
            current.cache_write_tokens += next.cache_write_tokens;
            current.cost_usd = match (current.cost_usd, next.cost_usd) {
                (Some(left), Some(right)) => Some(left + right),
                (left, None) => left,
                (None, right) => right,
            };
            if current.model.is_none() {
                current.model = next.model;
            }
            Some(current)
        }
    }
}

fn usage_with_model(
    mut usage: executors::TokenUsage,
    config: &CodexConfig,
) -> executors::TokenUsage {
    if usage.model.is_none() {
        usage.model = config.model.clone();
    }
    usage
}

pub(crate) fn codex_event_error_message(raw: &Value) -> Option<String> {
    if is_system_error_status(raw) {
        return Some(
            extract_error_message(raw).unwrap_or_else(|| CODEX_SYSTEM_ERROR_FALLBACK.to_owned()),
        );
    }

    if is_error_notification(raw) {
        return extract_error_message(raw);
    }

    if normalize::is_turn_completed(raw) {
        return turn_completed_error_message(raw);
    }

    None
}

fn is_error_notification(raw: &Value) -> bool {
    raw.get("method").and_then(Value::as_str) == Some("error")
}

fn is_system_error_status(raw: &Value) -> bool {
    raw.get("method").and_then(Value::as_str) == Some("thread/status/changed")
        && value_at_path(raw, &["params", "status", "type"])
            .and_then(Value::as_str)
            .is_some_and(|status| status == "systemError")
}

fn turn_completed_error_message(raw: &Value) -> Option<String> {
    for path in [
        &["params", "turn", "error"][..],
        &["params", "turn", "errorMessage"],
        &["params", "error"],
        &["params", "errorMessage"],
    ] {
        let Some(error) = value_at_path(raw, path) else {
            continue;
        };
        if !error.is_null() {
            return Some(error_message_from_value(error));
        }
    }
    None
}

fn extract_error_message(raw: &Value) -> Option<String> {
    for path in [
        &["params", "status", "message"][..],
        &["params", "status", "error", "message"],
        &["params", "status", "error"],
        &["params", "status", "errorMessage"],
        &["params", "message"],
        &["params", "error", "message"],
        &["params", "error"],
        &["params", "errorMessage"],
    ] {
        if let Some(error) = value_at_path(raw, path) {
            let message = error_message_from_value(error);
            if !message.trim().is_empty() && message != "null" {
                return Some(message);
            }
        }
    }
    None
}

fn error_message_from_value(value: &Value) -> String {
    match value {
        Value::String(message) => error_message_from_str(message),
        Value::Object(_) => extract_nested_error_message(value)
            .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_else(|_| value.to_string())),
        _ => value.to_string(),
    }
}

fn error_message_from_str(message: &str) -> String {
    let trimmed = message.trim();
    if let Ok(parsed) = serde_json::from_str::<Value>(trimmed)
        && !parsed.is_string()
        && let Some(nested) = extract_nested_error_message(&parsed)
    {
        return nested;
    }
    message.to_owned()
}

fn extract_nested_error_message(value: &Value) -> Option<String> {
    for key in ["message", "errorMessage", "reason"] {
        if let Some(message) = value.get(key).and_then(Value::as_str)
            && !message.trim().is_empty()
        {
            return Some(error_message_from_str(message));
        }
    }

    for key in ["error", "data", "cause", "details"] {
        if let Some(nested) = value.get(key).and_then(extract_nested_error_message) {
            return Some(nested);
        }
    }

    None
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

async fn read_stderr(stderr: tokio::process::ChildStderr, tx: mpsc::Sender<String>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if tx.send(line).await.is_err() {
            break;
        }
    }
}

async fn write_shared_log(
    writer: &Arc<AsyncMutex<LogWriter>>,
    kind: LogKind,
    payload: Value,
) -> Result<(), ExecutorError> {
    writer
        .lock()
        .await
        .write(kind, LogStream::Main, payload)
        .await
        .map_err(ExecutorError::Io)
}

async fn cleanup_child(child: &mut AsyncGroupChild) {
    signal_child(child);
    let _ = child.wait().await;
}

fn signal_child(child: &mut AsyncGroupChild) {
    #[cfg(unix)]
    {
        let _ = child.signal(Signal::SIGKILL);
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
}

fn dirs_path(name: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(format!(".{name}"))
}

fn availability_from_codex_home(codex_home: &Path) -> AvailabilityInfo {
    let auth_path = codex_home.join("auth.json");
    if auth_path.exists() {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: Some(auth_path.to_string_lossy().into_owned()),
        }
    } else if codex_home.exists() {
        AvailabilityInfo {
            status: AvailabilityStatus::Installed,
            authenticated_at: None,
            config_path: Some(codex_home.to_string_lossy().into_owned()),
        }
    } else {
        AvailabilityInfo {
            status: AvailabilityStatus::NotFound,
            authenticated_at: None,
            config_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn command_builder_uses_codex_default_app_server() {
        let config = CodexConfig {
            permission_policy: Some(PermissionPolicy::Supervised),
            command_overrides: CommandOverrides {
                additional_params: Some(vec!["--verbose".to_owned()]),
                ..CommandOverrides::default()
            },
            ..CodexConfig::default()
        };

        let cmd = CodexAdapter::build_command(&config);
        assert_eq!(cmd.as_std().get_program(), "npx");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec!["-y", "@openai/codex@0.147.0", "app-server", "--verbose"]
        );
    }

    #[tokio::test]
    async fn discovery_advertises_current_models_and_per_model_efforts() {
        let discovered = CodexAdapter::new()
            .discover_options(DiscoverContext { project_path: None })
            .await
            .expect("Codex options should be discoverable");

        assert_eq!(
            discovered.models,
            vec![
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "gpt-5.5",
                "gpt-5.4",
                "gpt-5.4-mini",
                "gpt-5.3-codex-spark",
            ]
        );
        assert_eq!(
            discovered.cli_specific["model_reasoning_efforts"]["gpt-5.6-sol"],
            json!(["low", "medium", "high", "xhigh", "max", "ultra"])
        );
        assert_eq!(
            discovered.cli_specific["model_reasoning_efforts"]["gpt-5.6-luna"],
            json!(["low", "medium", "high", "xhigh", "max"])
        );
    }

    #[test]
    fn thread_start_params_maps_policy() {
        let config = CodexConfig {
            model: Some("gpt-5-codex".to_owned()),
            permission_policy: Some(PermissionPolicy::Plan),
            model_reasoning_effort: Some("high".to_owned()),
            ..CodexConfig::default()
        };

        let params = CodexAdapter::thread_start_params(&config, "/tmp/worktree");

        assert_eq!(params.model.as_deref(), Some("gpt-5-codex"));
        assert!(matches!(params.sandbox, Some(SandboxMode::ReadOnly)));
        assert!(matches!(
            params.approval_policy,
            Some(AskForApproval::OnRequest)
        ));
        assert_eq!(
            params
                .config
                .as_ref()
                .and_then(|config| config.get("model_reasoning_effort"))
                .and_then(Value::as_str),
            Some("high")
        );
    }

    #[test]
    fn codex_error_notification_extracts_nested_json_message() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "error",
            "params": {
                "error": {
                    "additionalDetails": null,
                    "codexErrorInfo": "other",
                    "message": "{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"The 'gpt-5.5' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again.\"}}"
                },
                "threadId": "thread-1",
                "turnId": "turn-1",
                "willRetry": false
            }
        });

        assert_eq!(
            codex_event_error_message(&raw).as_deref(),
            Some(
                "The 'gpt-5.5' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."
            )
        );
    }

    #[test]
    fn bare_system_error_status_uses_fallback_message() {
        let raw = json!({
            "jsonrpc": "2.0",
            "method": "thread/status/changed",
            "params": {
                "status": { "type": "systemError" },
                "threadId": "thread-1"
            }
        });

        assert_eq!(
            codex_event_error_message(&raw).as_deref(),
            Some(CODEX_SYSTEM_ERROR_FALLBACK)
        );
    }

    #[derive(Default)]
    struct StubCodexSessionClient {
        calls: Vec<&'static str>,
        fork_params: Option<ThreadForkParams>,
        resume_params: Option<ThreadResumeParams>,
        turn_thread_id: Option<String>,
        turn_prompt: Option<String>,
        fail_resume_with_missing_thread: bool,
        turn_attempts: usize,
    }

    #[async_trait]
    impl CodexSessionClient for StubCodexSessionClient {
        async fn thread_start(
            &mut self,
            _params: ThreadStartParams,
        ) -> Result<ThreadStartResponse, ExecutorError> {
            self.calls.push("thread_start");
            Ok(ThreadStartResponse {
                thread: Some(protocol::ThreadInfo {
                    id: "fresh-thread".to_owned(),
                }),
                thread_id: None,
                model: None,
            })
        }

        async fn thread_fork(
            &mut self,
            params: ThreadForkParams,
        ) -> Result<ThreadForkResponse, ExecutorError> {
            self.calls.push("thread_fork");
            self.fork_params = Some(params);
            Ok(ThreadForkResponse {
                thread: Some(protocol::ThreadInfo {
                    id: "forked-thread".to_owned(),
                }),
                thread_id: None,
                model: None,
            })
        }

        async fn thread_resume(
            &mut self,
            params: ThreadResumeParams,
        ) -> Result<ThreadResumeResponse, ExecutorError> {
            self.calls.push("thread_resume");
            self.resume_params = Some(params);
            if self.fail_resume_with_missing_thread {
                return Err(ExecutorError::Other(
                    "thread/resume failed: thread not found: source-thread (-32600)".to_owned(),
                ));
            }
            Ok(ThreadResumeResponse {
                thread: Some(protocol::ThreadInfo {
                    id: "source-thread".to_owned(),
                }),
                thread_id: None,
                model: None,
            })
        }

        async fn turn_start(
            &mut self,
            thread_id: String,
            prompt: String,
        ) -> Result<TurnHandle, ExecutorError> {
            self.calls.push("turn_start");
            self.turn_attempts += 1;
            self.turn_thread_id = Some(thread_id);
            self.turn_prompt = Some(prompt);
            Ok(TurnHandle {
                turn_id: Some("turn-1".to_owned()),
            })
        }
    }

    #[tokio::test]
    async fn resume_review_forks_thread_then_starts_turn() {
        let mut client = StubCodexSessionClient::default();
        let config = CodexConfig {
            resume_thread_id: Some("source-thread".to_owned()),
            model: Some("gpt-5-codex".to_owned()),
            permission_policy: Some(PermissionPolicy::Supervised),
            ..CodexConfig::default()
        };
        let ctx = ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            worktree_path: "/tmp/forge-codex-worktree".to_owned(),
            description: "Review the changes".to_owned(),
            agent_config: json!({}),
            logs_path: "/tmp/forge-codex.log".to_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };

        let (thread_id, turn_id) = CodexAdapter::start_codex_session(&config, &ctx, &mut client)
            .await
            .expect("session starts");

        assert_eq!(thread_id, "forked-thread");
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(client.calls, vec!["thread_fork", "turn_start"]);
        assert!(!client.calls.contains(&"start_review"));
        assert_eq!(client.turn_thread_id.as_deref(), Some("forked-thread"));
        assert_eq!(client.turn_prompt.as_deref(), Some("Review the changes"));

        let fork_params = client.fork_params.expect("fork params captured");
        assert_eq!(fork_params.thread_id, "source-thread");
        assert_eq!(fork_params.cwd.as_deref(), Some(ctx.worktree_path.as_str()));
        assert_eq!(fork_params.model.as_deref(), Some("gpt-5-codex"));
    }

    #[tokio::test]
    async fn resume_conversation_starts_turn_on_existing_thread() {
        let mut client = StubCodexSessionClient::default();
        let config = CodexConfig {
            resume_thread_id: Some("source-thread".to_owned()),
            resume_thread_in_place: Some(true),
            resume_fallback_prompt: Some("Full reconstructed prompt".to_owned()),
            model: Some("gpt-5-codex".to_owned()),
            permission_policy: Some(PermissionPolicy::Plan),
            ..CodexConfig::default()
        };
        let ctx = ExecutionContext {
            task_id: "conversation-1".to_owned(),
            execution_id: "message-1".to_owned(),
            worktree_path: "/tmp/forge-codex-worktree".to_owned(),
            description: "Continue the conversation".to_owned(),
            agent_config: json!({}),
            logs_path: "/tmp/forge-codex.log".to_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };

        let (thread_id, turn_id) = CodexAdapter::start_codex_session(&config, &ctx, &mut client)
            .await
            .expect("session starts");

        assert_eq!(thread_id, "source-thread");
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(client.calls, vec!["thread_resume", "turn_start"]);
        assert_eq!(client.turn_thread_id.as_deref(), Some("source-thread"));
        assert_eq!(
            client.turn_prompt.as_deref(),
            Some("Continue the conversation")
        );
        assert!(client.fork_params.is_none());

        let resume_params = client.resume_params.expect("resume params captured");
        assert_eq!(resume_params.thread_id, "source-thread");
        assert_eq!(
            resume_params.cwd.as_deref(),
            Some(ctx.worktree_path.as_str())
        );
        assert_eq!(resume_params.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(resume_params.exclude_turns, Some(true));
    }

    #[tokio::test]
    async fn resume_conversation_falls_back_to_new_thread_when_source_thread_is_missing() {
        let mut client = StubCodexSessionClient {
            fail_resume_with_missing_thread: true,
            ..StubCodexSessionClient::default()
        };
        let config = CodexConfig {
            resume_thread_id: Some("source-thread".to_owned()),
            resume_thread_in_place: Some(true),
            resume_fallback_prompt: Some("Full reconstructed prompt".to_owned()),
            model: Some("gpt-5-codex".to_owned()),
            permission_policy: Some(PermissionPolicy::Plan),
            ..CodexConfig::default()
        };
        let ctx = ExecutionContext {
            task_id: "conversation-1".to_owned(),
            execution_id: "message-1".to_owned(),
            worktree_path: "/tmp/forge-codex-worktree".to_owned(),
            description: "Continue the conversation".to_owned(),
            agent_config: json!({}),
            logs_path: "/tmp/forge-codex.log".to_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };

        let (thread_id, turn_id) = CodexAdapter::start_codex_session(&config, &ctx, &mut client)
            .await
            .expect("session starts");

        assert_eq!(thread_id, "fresh-thread");
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(
            client.calls,
            vec!["thread_resume", "thread_start", "turn_start"]
        );
        assert_eq!(client.turn_thread_id.as_deref(), Some("fresh-thread"));
        assert_eq!(
            client.turn_prompt.as_deref(),
            Some("Full reconstructed prompt")
        );
    }

    #[test]
    fn availability_reports_authenticated_from_mock_auth_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("auth.json"), "{}").unwrap();

        let availability = availability_from_codex_home(dir.path());

        assert!(matches!(
            availability.status,
            AvailabilityStatus::Authenticated
        ));
        assert!(
            availability
                .config_path
                .as_deref()
                .unwrap()
                .ends_with("auth.json")
        );
    }

    #[tokio::test]
    async fn turn_completed_error_returns_failed_without_commit_or_session() {
        let dir = tempfile::tempdir().expect("tempdir creates");
        let script_path = dir.path().join("fake-codex.sh");
        fs::write(
            &script_path,
            r#"#!/bin/sh
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{}}'
read line || exit 1
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1"}}}'
read line || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"turn-1"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"thread/status/changed","params":{"threadId":"thread-1","status":{"type":"systemError"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"error","params":{"threadId":"thread-1","turnId":"turn-1","willRetry":false,"error":{"message":"{\"type\":\"error\",\"status\":400,\"error\":{\"type\":\"invalid_request_error\",\"message\":\"model rejected\"}}"}}}'
printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","error":{"message":"model rejected"}}}}'
"#,
        )
        .expect("script writes");
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(&script_path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script_path, permissions).unwrap();
        }

        let adapter = CodexAdapter::new();
        let result = adapter
            .execute(ExecutionContext {
                task_id: "task-1".to_owned(),
                execution_id: "exec-1".to_owned(),
                worktree_path: dir.path().display().to_string(),
                description: "Do the task".to_owned(),
                agent_config: json!({
                    "base_command_override": script_path.display().to_string()
                }),
                logs_path: dir.path().join("codex.jsonl").display().to_string(),
                heartbeat_interval_seconds: 30,
                max_turns: None,
                log_sender: None,
            })
            .await
            .expect("adapter returns execution result");

        assert_eq!(result.status, ExecutionOutcome::Failed);
        assert_eq!(result.after_sha, None);
        assert_eq!(result.agent_session_id.as_deref(), Some("thread-1"));
        assert_eq!(result.error.as_deref(), Some("model rejected"));

        let logs = fs::read_to_string(dir.path().join("codex.jsonl")).expect("logs written");
        assert!(logs.contains("codex_protocol_error"));
        assert!(logs.contains("model rejected"));
    }
}
