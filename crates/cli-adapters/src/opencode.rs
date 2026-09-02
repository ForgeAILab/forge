use async_trait::async_trait;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, OpencodeConfig, PermissionPolicy,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const MAX_SUMMARY_CHARS: usize = 500;

pub struct OpencodeAdapter {
    executions: Arc<Mutex<HashMap<String, RunningExecution>>>,
}

#[derive(Clone)]
struct RunningExecution {
    child: Arc<AsyncMutex<Child>>,
    cancelled: Arc<AtomicBool>,
}

impl OpencodeAdapter {
    pub fn new() -> Self {
        Self {
            executions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> OpencodeConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(config: &OpencodeConfig, prompt: &str) -> tokio::process::Command {
        let overrides = &config.command_overrides;

        let merged_overrides = overrides.clone();

        let mut adapter_args = vec![
            "run".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ];
        if let Some(ref model) = config.model {
            adapter_args.push("--model".into());
            adapter_args.push(normalize_opencode_model_id(model));
        }
        if let Some(ref agent) = config.agent {
            adapter_args.push("--agent".into());
            adapter_args.push(agent.clone());
        }
        if let Some(ref variant) = config.variant {
            adapter_args.push("--variant".into());
            adapter_args.push(variant.clone());
        }
        if let Some(ref session_id) = config.resume_session_id {
            adapter_args.push("--session".into());
            adapter_args.push(session_id.clone());
        }
        if should_skip_permissions(config) {
            adapter_args.push("--dangerously-skip-permissions".into());
        }

        let builder = crate::command::CommandBuilder::new("opencode")
            .adapter_args(adapter_args)
            .overrides(&merged_overrides);

        let mut cmd = builder.build();
        cmd.arg(prompt);
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
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

impl Default for OpencodeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CodingExecutorAdapter for OpencodeAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Opencode
    }

    fn check_availability(&self) -> AvailabilityInfo {
        detect_opencode_availability()
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions {
            models: vec![],
            permission_policies: vec!["auto".into(), "supervised".into()],
            cli_specific: serde_json::json!({}),
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
            .ok_or_else(|| ExecutorError::Other("failed to capture opencode stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture opencode stderr".into()))?;

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
                        "source": "opencode_cli",
                        "resumed": config.resume_session_id.is_some(),
                    }),
                )
                .await?;
        }

        let assistant_output = stream.assistant_output;
        let summary = assistant_output.as_deref().map(truncate_summary);

        if let Some(text) = &assistant_output {
            writer
                .write(
                    LogKind::Assistant,
                    LogStream::Main,
                    serde_json::json!({
                        "text": text,
                        "source": "opencode_cli",
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
                assistant_output,
                summary,
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
                assistant_output,
                summary,
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
                assistant_output,
                summary,
                error: Some(opencode_run_error(status, &stream.stderr_tail)),
                usage: None,
                ..Default::default()
            });
        }

        if assistant_output.is_none() {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Failed,
                after_sha: None,
                agent_session_id: stream.agent_session_id,
                assistant_output: None,
                summary: None,
                error: Some("opencode run completed without assistant text".to_owned()),
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
            assistant_output,
            summary,
            error: None,
            usage: None,
            ..Default::default()
        })
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let execution = {
            let execs = self
                .executions
                .lock()
                .map_err(|_| ExecutorError::Other("execution map lock poisoned".into()))?;
            execs.get(execution_id).cloned()
        };

        if let Some(exec) = execution {
            exec.cancelled.store(true, Ordering::SeqCst);
            let mut child = exec.child.lock().await;
            child.start_kill()?;
        }

        Ok(())
    }
}

struct RunStreamResult {
    agent_session_id: Option<String>,
    /// Complete final assistant text.  The bounded `summary` preview is
    /// derived from this at the end of the run; nothing truncates on the way
    /// in, because the assistant log entry is the transcript a reviewer's
    /// verdict is read back from.
    assistant_output: Option<String>,
    error: Option<String>,
    stderr_tail: String,
}

async fn stream_run_output(
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    writer: &mut LogWriter,
) -> Result<RunStreamResult, ExecutorError> {
    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut agent_session_id = None;
    let mut assistant_output = None;
    let mut error = None;
    let mut stderr_tail = String::new();

    while !stdout_done || !stderr_done {
        tokio::select! {
            line = stdout_lines.next_line(), if !stdout_done => {
                match line? {
                    Some(line) => {
                        let cleaned = strip_ansi_codes(&line);
                        if let Ok(event) = serde_json::from_str::<serde_json::Value>(&cleaned) {
                            capture_run_event(&event, &mut agent_session_id, &mut assistant_output, &mut error);
                            writer.write(classify_run_event(&event), LogStream::Main, event).await?;
                        } else {
                            if !cleaned.trim().is_empty() {
                                assistant_output = Some(cleaned.clone());
                            }
                            writer
                                .write(
                                    LogKind::Stdout,
                                    LogStream::Main,
                                    serde_json::json!({ "line": cleaned }),
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
                        let cleaned = strip_ansi_codes(&line);
                        push_tail(&mut stderr_tail, &cleaned, 2000);
                        writer
                            .write(
                                LogKind::Stderr,
                                LogStream::Main,
                                serde_json::json!({ "line": cleaned }),
                            )
                            .await?;
                    }
                    None => stderr_done = true,
                }
            }
        }
    }

    Ok(RunStreamResult {
        agent_session_id,
        assistant_output,
        error,
        stderr_tail,
    })
}

fn should_skip_permissions(config: &OpencodeConfig) -> bool {
    matches!(
        config.permission_policy.as_ref(),
        Some(PermissionPolicy::Auto)
    ) || config.auto_approve == Some(true)
}

fn normalize_opencode_model_id(model: &str) -> String {
    if model.contains('/') {
        return model.to_owned();
    }

    if model.starts_with("glm-") {
        return format!("zai-coding-plan/{model}");
    }

    if model.starts_with("mimo-") {
        return format!("xiaomi-token-plan-cn/{model}");
    }

    model.to_owned()
}

fn opencode_run_error(status: std::process::ExitStatus, stderr_tail: &str) -> String {
    let mut message = format!("opencode run exited with status {status}");
    let trimmed = stderr_tail.trim();
    if !trimmed.is_empty() {
        message.push_str("\nstderr:\n");
        message.push_str(trimmed);
    }
    message
}

fn classify_run_event(event: &serde_json::Value) -> LogKind {
    let event_type = event_type(event);
    let lower = event_type.to_ascii_lowercase();

    if lower == "error" || lower.contains("error") {
        LogKind::System
    } else if lower.contains("tool") && lower.contains("result") {
        LogKind::ToolResult
    } else if lower.contains("tool") || lower.contains("permission") {
        LogKind::ToolCall
    } else if lower.contains("session") {
        LogKind::SessionInfo
    } else if lower.contains("delta") || lower.contains("part") || lower.contains("message") {
        LogKind::AssistantDelta
    } else {
        LogKind::Stdout
    }
}

fn capture_run_event(
    event: &serde_json::Value,
    agent_session_id: &mut Option<String>,
    assistant_output: &mut Option<String>,
    error: &mut Option<String>,
) {
    if agent_session_id.is_none() {
        *agent_session_id = extract_session_id(event);
    }

    if error.is_none() && is_error_event(event) {
        *error = Some(
            extract_error_text(event)
                .unwrap_or_else(|| "opencode emitted an error event".to_owned()),
        );
        return;
    }

    if let Some(text) = extract_text(event)
        && !text.trim().is_empty()
    {
        *assistant_output = Some(text);
    }
}

fn is_error_event(event: &serde_json::Value) -> bool {
    event_type(event).eq_ignore_ascii_case("error")
}

fn extract_error_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in ["error", "data", "cause"] {
                if let Some(text) = map.get(key).and_then(extract_error_text) {
                    return Some(text);
                }
            }
            for key in ["message", "name", "code"] {
                if let Some(text) = map.get(key).and_then(|v| v.as_str())
                    && !text.trim().is_empty()
                {
                    return Some(text.to_owned());
                }
            }
            None
        }
        serde_json::Value::String(text) if !text.trim().is_empty() => Some(text.to_owned()),
        _ => None,
    }
}

fn event_type(event: &serde_json::Value) -> &str {
    event
        .get("type")
        .or_else(|| event.get("event"))
        .or_else(|| event.get("kind"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
}

fn extract_session_id(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in ["sessionID", "sessionId", "session_id"] {
                if let Some(id) = map.get(key).and_then(|v| v.as_str())
                    && !id.trim().is_empty()
                {
                    return Some(id.to_owned());
                }
            }
            for key in ["properties", "data", "session"] {
                if let Some(id) = map.get(key).and_then(extract_session_id) {
                    return Some(id);
                }
            }
            None
        }
        serde_json::Value::Array(values) => values.iter().find_map(extract_session_id),
        _ => None,
    }
}

fn extract_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            for key in ["text", "content", "summary"] {
                if let Some(text) = map.get(key).and_then(|v| v.as_str())
                    && !text.trim().is_empty()
                {
                    return Some(text.to_owned());
                }
            }
            for key in ["message", "part", "properties", "data"] {
                if let Some(text) = map.get(key).and_then(extract_text) {
                    return Some(text);
                }
            }
            if let Some(parts) = map.get("parts").and_then(|v| v.as_array()) {
                let text = parts
                    .iter()
                    .filter_map(extract_text)
                    .collect::<Vec<_>>()
                    .join("");
                if !text.trim().is_empty() {
                    return Some(text);
                }
            }
            None
        }
        serde_json::Value::Array(values) => {
            let text = values
                .iter()
                .filter_map(extract_text)
                .collect::<Vec<_>>()
                .join("");
            if text.trim().is_empty() {
                None
            } else {
                Some(text)
            }
        }
        serde_json::Value::String(text) if !text.trim().is_empty() => Some(text.to_owned()),
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

// ---------------------------------------------------------------------------
// Utility / availability detection (unchanged)
// ---------------------------------------------------------------------------

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn opencode_config_dir() -> Option<PathBuf> {
    env_path("XDG_CONFIG_HOME")
        .or_else(dirs::config_dir)
        .map(|dir| dir.join("opencode"))
}

fn opencode_data_dir() -> Option<PathBuf> {
    env_path("XDG_DATA_HOME")
        .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("share")))
        .map(|dir| dir.join("opencode"))
}

fn opencode_state_dir() -> Option<PathBuf> {
    env_path("XDG_STATE_HOME")
        .or_else(|| dirs::home_dir().map(|home| home.join(".local").join("state")))
        .map(|dir| dir.join("opencode"))
}

fn opencode_config_file_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(path) = env_path("OPENCODE_CONFIG") {
        candidates.push(path);
    }

    if let Some(dir) = opencode_config_dir() {
        candidates.push(dir.join("opencode.json"));
        candidates.push(dir.join("opencode.jsonc"));
    }

    if let Some(dir) = opencode_data_dir() {
        candidates.push(dir.join("opencode.json"));
        candidates.push(dir.join("opencode.jsonc"));
    }

    #[cfg(target_os = "macos")]
    {
        let managed = PathBuf::from("/Library/Application Support/opencode");
        candidates.push(managed.join("opencode.json"));
        candidates.push(managed.join("opencode.jsonc"));
    }

    #[cfg(target_os = "linux")]
    {
        let managed = PathBuf::from("/etc/opencode");
        candidates.push(managed.join("opencode.json"));
        candidates.push(managed.join("opencode.jsonc"));
    }

    #[cfg(windows)]
    if let Some(program_data) = env_path("ProgramData") {
        let managed = program_data.join("opencode");
        candidates.push(managed.join("opencode.json"));
        candidates.push(managed.join("opencode.jsonc"));
    }

    candidates
}

fn first_existing_path(paths: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    paths.into_iter().find(|path| path.exists())
}

fn opencode_auth_path() -> Option<PathBuf> {
    opencode_data_dir().map(|dir| dir.join("auth.json"))
}

fn path_modified_epoch_secs(path: &Path) -> Option<String> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    modified
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs().to_string())
}

fn executable_in_path(name: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&path_var).any(|dir| {
        #[cfg(windows)]
        {
            let pathext = std::env::var_os("PATHEXT")
                .map(|value| {
                    value
                        .to_string_lossy()
                        .split(';')
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| {
                    vec![
                        ".COM".to_string(),
                        ".EXE".to_string(),
                        ".BAT".to_string(),
                        ".CMD".to_string(),
                    ]
                });

            let direct = dir.join(name);
            direct.is_file()
                || pathext
                    .iter()
                    .any(|ext| dir.join(format!("{name}{ext}")).is_file())
        }

        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            let candidate = dir.join(name);
            candidate
                .metadata()
                .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        }
    })
}

fn detect_opencode_availability() -> AvailabilityInfo {
    if let Some(auth_path) = opencode_auth_path()
        && auth_path.exists()
    {
        return AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: path_modified_epoch_secs(&auth_path),
            config_path: Some(auth_path.to_string_lossy().into_owned()),
        };
    }

    let config_file = first_existing_path(opencode_config_file_candidates());

    let config_dir = opencode_config_dir().filter(|p| p.exists());
    let data_dir = opencode_data_dir().filter(|p| p.exists());
    let state_dir = opencode_state_dir().filter(|p| p.exists());
    let custom_config_dir = env_path("OPENCODE_CONFIG_DIR").filter(|p| p.exists());
    let home_opencode = dirs::home_dir()
        .map(|home| home.join(".opencode"))
        .filter(|p| p.exists());

    let installation_indicator = config_file
        .clone()
        .or(config_dir)
        .or(data_dir)
        .or(state_dir)
        .or(custom_config_dir)
        .or(home_opencode);

    if let Some(path) = installation_indicator {
        return AvailabilityInfo {
            status: AvailabilityStatus::Installed,
            authenticated_at: None,
            config_path: Some(path.to_string_lossy().into_owned()),
        };
    }

    if executable_in_path("opencode") {
        return AvailabilityInfo {
            status: AvailabilityStatus::Installed,
            authenticated_at: None,
            config_path: None,
        };
    }

    AvailabilityInfo {
        status: AvailabilityStatus::NotFound,
        authenticated_at: None,
        config_path: None,
    }
}

fn strip_ansi_codes(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_alphabetic() {
                        chars.next();
                        break;
                    }
                    chars.next();
                }
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;

    #[test]
    fn command_builder_uses_run_cli_and_permission_env() {
        let config = OpencodeConfig {
            model: Some("anthropic/claude-sonnet-4-6".to_owned()),
            permission_policy: Some(PermissionPolicy::Auto),
            command_overrides: CommandOverrides::default(),
            ..OpencodeConfig::default()
        };

        let cmd = OpencodeAdapter::build_command(&config, "hello");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(cmd.as_std().get_program(), "opencode");
        assert!(args.contains(&"run".to_owned()));
        assert!(args.windows(2).any(|pair| pair == ["--format", "json"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "anthropic/claude-sonnet-4-6"])
        );
        assert!(args.contains(&"--dangerously-skip-permissions".to_owned()));
        assert_eq!(args.last(), Some(&"hello".to_owned()));

        let envs: HashMap<_, _> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| {
                value.map(|value| {
                    (
                        key.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
            })
            .collect();
        assert_eq!(envs.get("OPENCODE_PERMISSION"), None);
    }

    #[test]
    fn detect_reports_not_found_when_no_indicators_present() {
        let info = detect_opencode_availability();
        match info.status {
            AvailabilityStatus::Authenticated
            | AvailabilityStatus::Installed
            | AvailabilityStatus::NotFound => {}
        }
    }

    #[test]
    fn extract_session_id_reads_opencode_run_event() {
        let event = serde_json::json!({
            "type": "step_start",
            "sessionID": "ses_123",
            "part": { "messageID": "msg_123" }
        });
        assert_eq!(extract_session_id(&event), Some("ses_123".to_owned()));
    }

    #[test]
    fn extract_text_reads_nested_parts() {
        let event = serde_json::json!({
            "type": "message",
            "parts": [
                { "type": "text", "text": "hello" },
                { "type": "text", "text": " world" }
            ]
        });
        assert_eq!(extract_text(&event), Some("hello world".to_owned()));
    }

    #[test]
    fn extract_error_text_reads_opencode_error_event() {
        let event = serde_json::json!({
            "type": "error",
            "sessionID": "ses_test",
            "error": {
                "name": "UnknownError",
                "data": { "message": "Model not found: mimo-v2.5-pro/." }
            }
        });
        assert_eq!(
            extract_error_text(&event),
            Some("Model not found: mimo-v2.5-pro/.".to_owned())
        );
    }

    #[test]
    fn normalizes_common_opencode_model_aliases() {
        assert_eq!(
            normalize_opencode_model_id("glm-5.1"),
            "zai-coding-plan/glm-5.1"
        );
        assert_eq!(
            normalize_opencode_model_id("mimo-v2.5"),
            "xiaomi-token-plan-cn/mimo-v2.5"
        );
        assert_eq!(
            normalize_opencode_model_id("openai/gpt-5.2"),
            "openai/gpt-5.2"
        );
    }

    #[tokio::test]
    async fn execute_drives_opencode_run_json_without_permission_env() {
        let dir = tempfile::tempdir().unwrap();
        let fake_opencode = dir.path().join("fake-opencode");
        std::fs::write(
            &fake_opencode,
            r#"#!/bin/sh
if [ "${OPENCODE_PERMISSION+x}" = "x" ]; then
  echo "unexpected OPENCODE_PERMISSION=$OPENCODE_PERMISSION" >&2
  exit 42
fi
printf '%s\n' '{"type":"step_start","sessionID":"ses_test","part":{"type":"step-start","sessionID":"ses_test"}}'
printf '%s\n' '{"type":"text","sessionID":"ses_test","part":{"type":"text","text":"forge fake ok"}}'
printf '%s\n' '{"type":"step_finish","sessionID":"ses_test","part":{"type":"step-finish","sessionID":"ses_test"}}'
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&fake_opencode).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&fake_opencode, permissions).unwrap();
        }

        let logs_path = dir.path().join("opencode.jsonl");
        let adapter = OpencodeAdapter::new();
        let result = adapter
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: dir.path().to_string_lossy().to_string(),
                description: "hello".to_owned(),
                agent_config: serde_json::json!({
                    "permission_policy": "auto",
                    "base_command_override": fake_opencode.to_string_lossy(),
                }),
                logs_path: logs_path.to_string_lossy().to_string(),
                heartbeat_interval_seconds: 1,
                max_turns: None,
                log_sender: None,
            })
            .await
            .unwrap();

        assert_eq!(result.status, ExecutionOutcome::Completed);
        assert_eq!(result.agent_session_id, Some("ses_test".to_owned()));
        assert_eq!(result.summary, Some("forge fake ok".to_owned()));
    }

    #[tokio::test]
    async fn assistant_log_entry_keeps_the_whole_final_message() {
        // A reviewer's verdict marker sits at the END of its final message, and
        // the review runner reads the verdict back out of the assistant log
        // entry. Truncating that entry to the bounded `summary` preview silently
        // discarded the marker and every long review failed as "verdict marker
        // missing", so the log entry must carry the complete text while
        // `summary` stays bounded for the Task projection.
        let dir = tempfile::tempdir().unwrap();
        let fake_opencode = dir.path().join("fake-opencode-long");
        let body = "x".repeat(2_000);
        std::fs::write(
            &fake_opencode,
            format!(
                r#"#!/bin/sh
printf '%s\n' '{{"type":"text","sessionID":"ses_test","part":{{"type":"text","text":"{body}\n===REVIEW: PASS==="}}}}'
"#
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&fake_opencode).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&fake_opencode, permissions).unwrap();
        }

        let logs_path = dir.path().join("opencode-long.jsonl");
        let adapter = OpencodeAdapter::new();
        let result = adapter
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: dir.path().to_string_lossy().to_string(),
                description: "hello".to_owned(),
                agent_config: serde_json::json!({
                    "base_command_override": fake_opencode.to_string_lossy(),
                }),
                logs_path: logs_path.to_string_lossy().to_string(),
                heartbeat_interval_seconds: 1,
                max_turns: None,
                log_sender: None,
            })
            .await
            .unwrap();

        let full = result.assistant_output.expect("full assistant output");
        assert!(
            full.ends_with("===REVIEW: PASS==="),
            "verdict marker survives"
        );
        assert!(full.chars().count() > 500);
        assert_eq!(
            result.summary.as_ref().map(|s| s.chars().count()),
            Some(500)
        );

        let logged = std::fs::read_to_string(&logs_path).unwrap();
        let assistant_text = logged
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|entry| entry.get("kind").and_then(|k| k.as_str()) == Some("assistant"))
            .and_then(|entry| {
                entry
                    .get("payload")
                    .and_then(|payload| payload.get("text"))
                    .and_then(|text| text.as_str())
                    .map(str::to_owned)
            })
            .expect("assistant log entry");
        assert!(
            assistant_text.ends_with("===REVIEW: PASS==="),
            "the assistant log entry must keep the verdict marker, got {} chars",
            assistant_text.chars().count()
        );
    }

    #[tokio::test]
    async fn execute_fails_when_opencode_emits_error_event() {
        let dir = tempfile::tempdir().unwrap();
        let fake_opencode = dir.path().join("fake-opencode-error");
        std::fs::write(
            &fake_opencode,
            r#"#!/bin/sh
printf '%s\n' '{"type":"error","sessionID":"ses_test","error":{"data":{"message":"Model not found: bad-model"}}}'
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&fake_opencode).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&fake_opencode, permissions).unwrap();
        }

        let logs_path = dir.path().join("opencode-error.jsonl");
        let adapter = OpencodeAdapter::new();
        let result = adapter
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: dir.path().to_string_lossy().to_string(),
                description: "hello".to_owned(),
                agent_config: serde_json::json!({
                    "base_command_override": fake_opencode.to_string_lossy(),
                }),
                logs_path: logs_path.to_string_lossy().to_string(),
                heartbeat_interval_seconds: 1,
                max_turns: None,
                log_sender: None,
            })
            .await
            .unwrap();

        assert_eq!(result.status, ExecutionOutcome::Failed);
        assert_eq!(result.agent_session_id, Some("ses_test".to_owned()));
        assert_eq!(result.error, Some("Model not found: bad-model".to_owned()));
    }

    #[tokio::test]
    async fn execute_fails_when_opencode_exits_without_text() {
        let dir = tempfile::tempdir().unwrap();
        let fake_opencode = dir.path().join("fake-opencode-empty");
        std::fs::write(
            &fake_opencode,
            r#"#!/bin/sh
printf '%s\n' '{"type":"step_start","sessionID":"ses_test","part":{"type":"step-start","sessionID":"ses_test"}}'
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&fake_opencode).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&fake_opencode, permissions).unwrap();
        }

        let logs_path = dir.path().join("opencode-empty.jsonl");
        let adapter = OpencodeAdapter::new();
        let result = adapter
            .execute(ExecutionContext {
                task_id: "task".to_owned(),
                execution_id: "execution".to_owned(),
                worktree_path: dir.path().to_string_lossy().to_string(),
                description: "hello".to_owned(),
                agent_config: serde_json::json!({
                    "base_command_override": fake_opencode.to_string_lossy(),
                }),
                logs_path: logs_path.to_string_lossy().to_string(),
                heartbeat_interval_seconds: 1,
                max_turns: None,
                log_sender: None,
            })
            .await
            .unwrap();

        assert_eq!(result.status, ExecutionOutcome::Failed);
        assert_eq!(result.agent_session_id, Some("ses_test".to_owned()));
        assert_eq!(
            result.error,
            Some("opencode run completed without assistant text".to_owned())
        );
    }
}
