use async_trait::async_trait;
use command_group::{AsyncCommandGroup, AsyncGroupChild};
#[cfg(unix)]
use command_group::{Signal, UnixChildExt};
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, CursorConfig, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, PermissionPolicy,
};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::process::{ExitStatus, Output, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_SUMMARY_CHARS: usize = 500;
const STATUS_TIMEOUT_SECONDS: u64 = 2;

pub struct CursorAdapter {
    processes: Arc<Mutex<HashMap<String, RunningProcess>>>,
}

#[derive(Clone)]
struct RunningProcess {
    child: Arc<AsyncMutex<AsyncGroupChild>>,
    cancel: CancellationToken,
}

struct CursorStreamResult {
    cancelled: bool,
    agent_session_id: Option<String>,
    summary: Option<String>,
    error: Option<String>,
    stderr_tail: String,
}

impl CursorAdapter {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> CursorConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(config: &CursorConfig, prompt: &str) -> tokio::process::Command {
        let mut adapter_args = vec![
            "-p".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
        ];

        if should_force(config) {
            adapter_args.push("--force".to_owned());
        }

        if let Some(model) = &config.model {
            adapter_args.push("--model".to_owned());
            adapter_args.push(model.clone());
        }

        if let Some(session_id) = &config.resume_session_id {
            adapter_args.push("--resume".to_owned());
            adapter_args.push(session_id.clone());
        }

        let builder = crate::command::CommandBuilder::new("cursor-agent")
            .adapter_args(adapter_args)
            .overrides(&config.command_overrides);

        let mut cmd = builder.build();
        cmd.arg(prompt)
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NO_COLOR", "1");
        cmd
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

impl Default for CursorAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodingExecutorAdapter for CursorAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Cursor
    }

    fn check_availability(&self) -> AvailabilityInfo {
        detect_cursor_availability()
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions {
            models: vec![],
            permission_policies: vec!["auto".into(), "supervised".into(), "plan".into()],
            cli_specific: serde_json::json!({
                "output_formats": ["text", "json", "stream-json"],
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

        let mut command = Self::build_command(&config, &prompt);
        command.current_dir(&ctx.worktree_path);
        let mut child = command.group_spawn()?;

        let stdout = match child.inner().stdout.take() {
            Some(stdout) => stdout,
            None => {
                signal_child(&mut child);
                let _ = child.wait().await;
                return Err(ExecutorError::Other(
                    "failed to capture cursor stdout".to_owned(),
                ));
            }
        };
        let stderr = match child.inner().stderr.take() {
            Some(stderr) => stderr,
            None => {
                signal_child(&mut child);
                let _ = child.wait().await;
                return Err(ExecutorError::Other(
                    "failed to capture cursor stderr".to_owned(),
                ));
            }
        };

        let child = Arc::new(AsyncMutex::new(child));
        let cancel = CancellationToken::new();
        self.insert_process(
            ctx.execution_id.clone(),
            RunningProcess {
                child: child.clone(),
                cancel: cancel.clone(),
            },
        )?;

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
                    "type": "cursor_adapter_started",
                    "worktree_path": ctx.worktree_path,
                    "model": config.model.as_deref(),
                    "prompt_bytes": prompt.len(),
                    "force": should_force(&config),
                }),
            )
            .await?;

        let stream_result = stream_child_output(stdout, stderr, &mut writer, cancel.clone()).await;
        let stream_failed = stream_result.is_err();
        let status = {
            let mut child = child.lock().await;
            if stream_failed {
                signal_child(&mut child);
            }
            child.wait().await?
        };
        self.remove_process(&ctx.execution_id)?;

        let stream = stream_result?;
        if stream.cancelled {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Cancelled,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                summary: stream.summary,
                error: None,
                usage: None,
                ..Default::default()
            });
        }

        if let Some(error) = stream.error {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                summary: stream.summary,
                error: Some(error),
                usage: None,
                ..Default::default()
            });
        }

        if !status.success() {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                summary: stream.summary,
                error: Some(cursor_run_error(status, &stream.stderr_tail)),
                usage: None,
                ..Default::default()
            });
        }

        let after_sha = if let Ok(false) =
            git::is_worktree_clean(Path::new(&ctx.worktree_path)).await
        {
            let subject = crate::commit::build_commit_subject(Some(&ctx.description), &ctx.task_id);
            crate::commit::commit_worktree_changes(Path::new(&ctx.worktree_path), &subject)
                .await
                .unwrap_or(None)
        } else {
            None
        };
        let after_sha = match after_sha {
            Some(sha) => Some(sha),
            None => git::get_current_sha(Path::new(&ctx.worktree_path))
                .await
                .ok(),
        };

        Ok(ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha,
            agent_session_id: stream.agent_session_id,
            summary: stream.summary,
            error: None,
            usage: None,
            ..Default::default()
        })
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let running = {
            self.processes
                .lock()
                .map_err(|_| ExecutorError::Other("process map lock poisoned".to_owned()))?
                .get(execution_id)
                .cloned()
        };

        if let Some(running) = running {
            running.cancel.cancel();
            let mut child = running.child.lock().await;
            signal_child(&mut child);
        }

        Ok(())
    }
}

async fn stream_child_output(
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    writer: &mut LogWriter,
    cancel: CancellationToken,
) -> Result<CursorStreamResult, ExecutorError> {
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut cancelled = false;
    let mut agent_session_id = None;
    let mut summary = None;
    let mut assistant_text = String::new();
    let mut error = None;
    let mut stderr_tail = String::new();

    while !stdout_done || !stderr_done {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                cancelled = true;
                break;
            }
            line = stdout_lines.next_line(), if !stdout_done => {
                match line? {
                    Some(line) => {
                        if let Ok(event) = serde_json::from_str::<Value>(&line) {
                            capture_cursor_event(
                                &event,
                                &mut agent_session_id,
                                &mut summary,
                                &mut assistant_text,
                                &mut error,
                            );
                            writer
                                .write(classify_cursor_event(&event), LogStream::Main, event)
                                .await?;
                        } else {
                            writer
                                .write(
                                    LogKind::Stdout,
                                    LogStream::Main,
                                    serde_json::json!({ "line": line }),
                                )
                                .await?;
                        }
                    }
                    None => stdout_done = true,
                }
            }
            line = stderr_lines.next_line(), if !stderr_done => {
                match line? {
                    Some(line) => {
                        push_tail(&mut stderr_tail, &line, 2000);
                        writer
                            .write(
                                LogKind::Stderr,
                                LogStream::Main,
                                serde_json::json!({ "line": line }),
                            )
                            .await?;
                    }
                    None => stderr_done = true,
                }
            }
        }
    }

    Ok(CursorStreamResult {
        cancelled,
        agent_session_id,
        summary,
        error,
        stderr_tail,
    })
}

fn capture_cursor_event(
    event: &Value,
    agent_session_id: &mut Option<String>,
    summary: &mut Option<String>,
    assistant_text: &mut String,
    error: &mut Option<String>,
) {
    if agent_session_id.is_none() {
        *agent_session_id = extract_session_id(event);
    }

    match event_type(event) {
        "assistant" => {
            if let Some(text) = extract_message_text(event) {
                assistant_text.push_str(&text);
                *summary = Some(truncate_summary(assistant_text));
            }
        }
        "result" => {
            if let Some(text) = event.get("result").and_then(Value::as_str) {
                *summary = Some(truncate_summary(text));
            }
            if event
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                *error = Some(
                    event
                        .get("result")
                        .and_then(Value::as_str)
                        .filter(|text| !text.trim().is_empty())
                        .unwrap_or("cursor-agent result marked error")
                        .to_owned(),
                );
            }
        }
        _ => {}
    }
}

fn classify_cursor_event(event: &Value) -> LogKind {
    match event_type(event) {
        "assistant" => LogKind::AssistantDelta,
        "tool_call" => {
            if event.get("subtype").and_then(Value::as_str) == Some("completed") {
                LogKind::ToolResult
            } else {
                LogKind::ToolCall
            }
        }
        "user" => LogKind::User,
        "system" | "result" => LogKind::SessionInfo,
        _ => LogKind::Stdout,
    }
}

fn should_force(config: &CursorConfig) -> bool {
    config.force.unwrap_or({
        !matches!(
            config.permission_policy.as_ref(),
            Some(PermissionPolicy::Plan)
        )
    })
}

fn cursor_run_error(status: ExitStatus, stderr_tail: &str) -> String {
    let mut message = format!("cursor-agent exited with status {status}");
    let trimmed = stderr_tail.trim();
    if !trimmed.is_empty() {
        message.push_str("\nstderr:\n");
        message.push_str(trimmed);
    }
    message
}

fn event_type(event: &Value) -> &str {
    event.get("type").and_then(Value::as_str).unwrap_or("")
}

fn extract_session_id(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            for key in ["session_id", "sessionId", "sessionID"] {
                if let Some(id) = map.get(key).and_then(Value::as_str)
                    && !id.trim().is_empty()
                {
                    return Some(id.to_owned());
                }
            }
            for key in ["message", "data", "result"] {
                if let Some(id) = map.get(key).and_then(extract_session_id) {
                    return Some(id);
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(extract_session_id),
        _ => None,
    }
}

fn extract_message_text(value: &Value) -> Option<String> {
    let content = value
        .get("message")
        .and_then(|message| message.get("content"))?;
    match content {
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("");
            if text.trim().is_empty() {
                None
            } else {
                Some(text)
            }
        }
        Value::String(text) if !text.trim().is_empty() => Some(text.to_owned()),
        _ => None,
    }
}

fn push_tail(buffer: &mut String, line: &str, max_chars: usize) {
    if !buffer.is_empty() {
        buffer.push('\n');
    }
    buffer.push_str(line);
    let len = buffer.chars().count();
    if len > max_chars {
        *buffer = buffer.chars().skip(len - max_chars).collect();
    }
}

fn truncate_summary(content: &str) -> String {
    if content.chars().count() <= MAX_SUMMARY_CHARS {
        content.to_owned()
    } else {
        content.chars().take(MAX_SUMMARY_CHARS).collect()
    }
}

fn detect_cursor_availability() -> AvailabilityInfo {
    if std::env::var("CURSOR_API_KEY")
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        };
    }

    if !executable_in_path("cursor-agent") {
        return AvailabilityInfo {
            status: AvailabilityStatus::NotFound,
            authenticated_at: None,
            config_path: None,
        };
    }

    if cursor_status_authenticated() {
        return AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        };
    }

    AvailabilityInfo {
        status: AvailabilityStatus::Installed,
        authenticated_at: None,
        config_path: None,
    }
}

fn cursor_status_authenticated() -> bool {
    let mut command = std::process::Command::new("cursor-agent");
    command.arg("status");
    let Some(output) = command_output_timeout(command, Duration::from_secs(STATUS_TIMEOUT_SECONDS))
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }

    let mut combined = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
    combined.push_str(&String::from_utf8_lossy(&output.stderr).to_ascii_lowercase());
    cursor_status_output_authenticated(&combined)
}

fn cursor_status_output_authenticated(combined: &str) -> bool {
    let combined = combined.to_ascii_lowercase();
    if combined.contains("not authenticated")
        || combined.contains("not logged in")
        || combined.contains("not signed in")
        || combined.contains("authentication required")
        || combined.contains("authenticated: false")
        || combined.contains("authenticated false")
    {
        return false;
    }
    combined.contains("authenticated: true")
        || combined.contains("authenticated true")
        || combined.contains("logged in")
        || combined.contains("signed in")
}

fn command_output_timeout(mut command: std::process::Command, timeout: Duration) -> Option<Output> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(command.output());
    });
    rx.recv_timeout(timeout).ok()?.ok()
}

fn executable_in_path(name: &str) -> bool {
    which::which(name).is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;

    #[test]
    fn command_builder_maps_cursor_args() {
        let config = CursorConfig {
            model: Some("gpt-5".to_owned()),
            resume_session_id: Some("session-123".to_owned()),
            permission_policy: Some(PermissionPolicy::Supervised),
            command_overrides: CommandOverrides::default(),
            ..CursorConfig::default()
        };

        let cmd = CursorAdapter::build_command(&config, "hello");
        assert_eq!(cmd.as_std().get_program(), "cursor-agent");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(
            args,
            vec![
                "-p",
                "--output-format",
                "stream-json",
                "--force",
                "--model",
                "gpt-5",
                "--resume",
                "session-123",
                "hello",
            ]
        );
    }

    #[test]
    fn plan_policy_omits_force_unless_explicit() {
        let config = CursorConfig {
            permission_policy: Some(PermissionPolicy::Plan),
            command_overrides: CommandOverrides::default(),
            ..CursorConfig::default()
        };
        let cmd = CursorAdapter::build_command(&config, "hello");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(!args.contains(&"--force".to_owned()));

        let config = CursorConfig {
            force: Some(true),
            permission_policy: Some(PermissionPolicy::Plan),
            command_overrides: CommandOverrides::default(),
            ..CursorConfig::default()
        };
        let cmd = CursorAdapter::build_command(&config, "hello");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--force".to_owned()));
    }

    #[test]
    fn captures_cursor_stream_fields() {
        let mut session_id = None;
        let mut summary = None;
        let mut assistant_text = String::new();
        let mut error = None;

        capture_cursor_event(
            &serde_json::json!({
                "type": "system",
                "session_id": "session-1"
            }),
            &mut session_id,
            &mut summary,
            &mut assistant_text,
            &mut error,
        );
        capture_cursor_event(
            &serde_json::json!({
                "type": "assistant",
                "message": {
                    "content": [
                        { "type": "text", "text": "hello" },
                        { "type": "text", "text": " world" }
                    ]
                },
                "session_id": "session-1"
            }),
            &mut session_id,
            &mut summary,
            &mut assistant_text,
            &mut error,
        );
        capture_cursor_event(
            &serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "result": "final text",
                "session_id": "session-1"
            }),
            &mut session_id,
            &mut summary,
            &mut assistant_text,
            &mut error,
        );

        assert_eq!(session_id.as_deref(), Some("session-1"));
        assert_eq!(summary.as_deref(), Some("final text"));
        assert!(error.is_none());
    }

    #[test]
    fn status_parser_does_not_treat_not_logged_in_as_logged_in() {
        assert!(!cursor_status_output_authenticated("Not logged in"));
        assert!(!cursor_status_output_authenticated(
            "Authentication required. Please run 'agent login' first"
        ));
        assert!(!cursor_status_output_authenticated("Authenticated: false"));
        assert!(cursor_status_output_authenticated("Logged in"));
        assert!(cursor_status_output_authenticated(
            "Signed in as user@example.test"
        ));
        assert!(cursor_status_output_authenticated("Authenticated: true"));
    }
}
