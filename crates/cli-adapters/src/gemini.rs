use async_trait::async_trait;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, GeminiConfig, LogKind, LogStream, LogWriter, PermissionPolicy,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::Mutex as AsyncMutex;

const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
const FIRST_OUTPUT_TIMEOUT_SECONDS: u64 = 300;
const MAX_SUMMARY_CHARS: usize = 500;

pub struct GeminiAdapter {
    processes: Arc<Mutex<HashMap<String, Arc<AsyncMutex<Child>>>>>,
}

impl GeminiAdapter {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_config(ctx: &ExecutionContext) -> GeminiConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(config: &GeminiConfig, prompt: &str) -> tokio::process::Command {
        // Current Gemini CLI releases require the prompt as `-p`'s argument;
        // a bare `-p` with the prompt on stdin exits 1 with usage help.
        let mut adapter_args = vec![
            "-p".to_owned(),
            prompt.to_owned(),
            "--output-format=json".to_owned(),
        ];

        let policy_is_yolo = matches!(
            config.permission_policy.as_ref(),
            Some(PermissionPolicy::Yolo)
        );
        let unattended = policy_is_yolo
            || config.yolo.unwrap_or(false)
            || matches!(
                config.permission_policy.as_ref(),
                Some(PermissionPolicy::Auto) | Some(PermissionPolicy::Supervised)
            );

        if unattended {
            adapter_args.push("--yolo".to_owned());
        }

        if let Some(ref model) = config.model {
            adapter_args.push("--model".to_owned());
            adapter_args.push(model.clone());
        }

        if !policy_is_yolo && let Some(ref sandbox) = config.sandbox {
            adapter_args.push("--sandbox".to_owned());
            adapter_args.push(sandbox.clone());
        }

        if let Some(check_every_n) = config.check_every_n {
            adapter_args.push("--check_every_n".to_owned());
            adapter_args.push(check_every_n.to_string());
        }

        if let Some(ref resume_session_id) = config.resume_session_id {
            adapter_args.push("--resume".to_owned());
            adapter_args.push(resume_session_id.clone());
        }

        let builder = crate::command::CommandBuilder::new("gemini")
            .adapter_args(adapter_args)
            .overrides(&config.command_overrides);

        let mut cmd = builder.build();
        cmd.kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("NO_COLOR", "1")
            // Task worktrees are freshly created directories the user never
            // opened interactively; without this the CLI refuses headless
            // runs with an untrusted-directory error.
            .env("GEMINI_CLI_TRUST_WORKSPACE", "true");
        cmd
    }

    fn insert_process(
        &self,
        execution_id: String,
        child: Arc<AsyncMutex<Child>>,
    ) -> Result<(), ExecutorError> {
        self.processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".into()))?
            .insert(execution_id, child);
        Ok(())
    }

    fn remove_process(&self, execution_id: &str) -> Result<(), ExecutorError> {
        self.processes
            .lock()
            .map_err(|_| ExecutorError::Other("process map lock poisoned".into()))?
            .remove(execution_id);
        Ok(())
    }
}

impl Default for GeminiAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// The user-level `~/.gemini/settings.json` may pin OAuth auth, which the
/// CLI prefers over an injected `GEMINI_API_KEY` and which fails headless.
/// When Forge injects a provider API key, point the CLI at a Forge-owned
/// home whose settings select API-key auth so the key actually drives the
/// run.
///
/// The home belongs to the Task: `<task root>/.forge-task/home/gemini`,
/// removed with the Task root. A worktree whose Task root Forge did not
/// reserve keeps the shared directory in the system temp dir.
fn ensure_api_key_home(worktree: &Path) -> Option<PathBuf> {
    let home = executors::sandbox::TaskRoot::of_worktree(worktree).map_or_else(
        || std::env::temp_dir().join("forge-gemini-api-key-home"),
        |task_root| task_root.home("gemini"),
    );
    let settings_dir = home.join(".gemini");
    std::fs::create_dir_all(&settings_dir).ok()?;
    std::fs::write(
        settings_dir.join("settings.json"),
        r#"{"security":{"auth":{"selectedType":"gemini-api-key"}}}"#,
    )
    .ok()?;
    Some(home)
}

#[async_trait]
impl CodingExecutorAdapter for GeminiAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Gemini
    }

    fn check_availability(&self) -> AvailabilityInfo {
        detect_gemini_availability()
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions {
            models: vec![
                "auto".into(),
                "pro".into(),
                "flash".into(),
                "flash-lite".into(),
                "gemini-3.8-flash".into(),
                "gemini-3.5-flash".into(),
                "gemini-3.1-pro-preview".into(),
                "gemini-3.5-flash-lite".into(),
                "gemini-3.1-flash-lite".into(),
                "gemini-3-pro-preview".into(),
                "gemini-3-flash-preview".into(),
                "gemini-2.5-pro".into(),
                "gemini-2.5-flash".into(),
                "gemini-2.5-flash-lite".into(),
            ],
            permission_policies: vec!["auto".into(), "supervised".into(), "yolo".into()],
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
        let key_injected = config
            .command_overrides
            .env
            .as_ref()
            .is_some_and(|env| env.contains_key("GEMINI_API_KEY"));
        if key_injected && let Some(home) = ensure_api_key_home(Path::new(&ctx.worktree_path)) {
            cmd.env("GEMINI_CLI_HOME", home);
        }
        // Owns the execution's temp directory until this execution returns.
        let _run_scope = crate::command::run_in_task_worktree(&mut cmd, &ctx);

        let mut child = cmd.spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture gemini stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture gemini stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ExecutorError::Other("failed to capture gemini stderr".into()))?;

        let child_arc = Arc::new(AsyncMutex::new(child));
        self.insert_process(ctx.execution_id.clone(), child_arc.clone())?;

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
                    "type": "gemini_adapter_started",
                    "worktree_path": ctx.worktree_path,
                    "model": ctx.agent_config.get("model").and_then(serde_json::Value::as_str),
                }),
            )
            .await?;

        let stream_result =
            stream_child_output(&ctx, stdin, stdout, stderr, &prompt, &mut writer).await;

        let status = {
            let mut child = child_arc.lock().await;
            let _ = child.start_kill();
            child.wait().await?
        };
        self.remove_process(&ctx.execution_id)?;

        let stream = stream_result?;

        if !status.success()
            && let Some(retry_after) = stream.capacity.retry_after
        {
            return Err(ExecutorError::UsageExhausted {
                retry_after,
                usage_reports: Vec::new(),
            });
        }

        let (outcome, error) = if status.success() {
            (ExecutionOutcome::Completed, None)
        } else {
            (
                ExecutionOutcome::Failed,
                stream
                    .error
                    .or_else(|| Some(format!("gemini exited with status {status}"))),
            )
        };

        let after_sha =
            if outcome == ExecutionOutcome::Completed && crate::commit::auto_commit_enabled(&ctx) {
                match crate::commit::commit_execution_changes(&ctx).await {
                    Ok(Some(sha)) => Some(sha),
                    Ok(None) => git::get_current_sha(Path::new(&ctx.worktree_path))
                        .await
                        .ok(),
                    Err(e) => {
                        return Ok(ExecutionResult {
                            status: ExecutionOutcome::Failed,
                            after_sha: None,
                            agent_session_id: stream.agent_session_id,
                            summary: stream.summary,
                            error: Some(e.to_string()),
                            usage_reports: Vec::new(),
                            ..Default::default()
                        });
                    }
                }
            } else {
                None
            };

        Ok(ExecutionResult {
            status: outcome,
            after_sha,
            agent_session_id: stream.agent_session_id,
            summary: stream.summary,
            error,
            usage_reports: Vec::new(),
            ..Default::default()
        })
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        let process = {
            let procs = self
                .processes
                .lock()
                .map_err(|_| ExecutorError::Other("process map lock poisoned".into()))?;
            procs.get(execution_id).cloned()
        };

        if let Some(child_arc) = process {
            let mut child = child_arc.lock().await;
            child.start_kill()?;
        }

        Ok(())
    }
}

struct StreamResult {
    summary: Option<String>,
    agent_session_id: Option<String>,
    error: Option<String>,
    capacity: crate::capacity::CapacitySignal,
}

fn observe_gemini_error(value: &serde_json::Value, stream: &mut StreamResult) {
    let kind = value.get("type").and_then(serde_json::Value::as_str);
    if matches!(
        kind,
        Some("assistant" | "tool_call" | "tool_use" | "tool_result")
    ) {
        return;
    }
    let error = value
        .get("error")
        .filter(|error| !error.is_null() && error.as_bool() != Some(false))
        .or_else(|| {
            (kind == Some("error")
                || value.get("is_error").and_then(serde_json::Value::as_bool) == Some(true))
            .then_some(value)
        });
    if let Some(error) = error {
        let error = error
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| error.to_string());
        stream.capacity.observe(&error);
        stream.error = Some(error);
    }
}

/// Cap on the buffered stdout used to recover the final `--output-format=json`
/// document. The CLI prints that document pretty-printed across many lines, so
/// per-line JSON parsing never sees it.
const STDOUT_TAIL_BUFFER_BYTES: usize = 1024 * 1024;

async fn stream_child_output(
    _ctx: &ExecutionContext,
    mut stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    prompt: &str,
    writer: &mut LogWriter,
) -> Result<StreamResult, ExecutorError> {
    // The prompt travels as `-p`'s argument; close stdin immediately so the
    // CLI never waits on it.
    let _ = stdin.shutdown().await;
    drop(stdin);

    writer
        .write(
            LogKind::User,
            LogStream::Main,
            serde_json::json!({
                "text": prompt.chars().take(200).collect::<String>(),
                "source": "forge_prompt",
            }),
        )
        .await?;

    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();
    let mut stdout_done = false;
    let mut stderr_done = false;
    let mut summary = None;
    let mut agent_session_id = None;
    let mut stdout_tail = String::new();
    let mut stream = StreamResult {
        summary: None,
        agent_session_id: None,
        error: None,
        capacity: crate::capacity::CapacitySignal::default(),
    };
    let mut saw_output = false;
    let first_output_timeout =
        tokio::time::sleep(Duration::from_secs(FIRST_OUTPUT_TIMEOUT_SECONDS));
    tokio::pin!(first_output_timeout);

    while !stdout_done || !stderr_done {
        tokio::select! {
            _ = &mut first_output_timeout, if !saw_output => {
                return Err(ExecutorError::Other(
                    format!("gemini produced no output within {FIRST_OUTPUT_TIMEOUT_SECONDS}s"),
                ));
            }
            line = stdout_lines.next_line(), if !stdout_done => {
                match line {
                    Ok(Some(line)) => {
                        saw_output = true;
                        if stdout_tail.len() + line.len() < STDOUT_TAIL_BUFFER_BYTES {
                            stdout_tail.push_str(&line);
                            stdout_tail.push('\n');
                        }
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
                            observe_gemini_error(&json, &mut stream);
                            let kind = json
                                .get("type")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let log_kind = match kind {
                                "assistant" | "result" => LogKind::Assistant,
                                "tool_call" | "tool_use" => LogKind::ToolCall,
                                "tool_result" => LogKind::ToolResult,
                                _ => LogKind::Stdout,
                            };

                            if matches!(kind, "assistant" | "result")
                                && let Some(content) = json
                                    .get("content")
                                    .and_then(|v| v.as_str())
                                    .or_else(|| json.get("message").and_then(|v| v.as_str()))
                            {
                                summary = Some(truncate_summary(content));
                            }
                            if let Some(session) =
                                json.get("session_id").and_then(|v| v.as_str())
                            {
                                agent_session_id = Some(session.to_owned());
                            }

                            writer.write(log_kind, LogStream::Main, json).await?;
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
                    Ok(None) => stdout_done = true,
                    Err(e) => return Err(e.into()),
                }
            }
            line = stderr_lines.next_line(), if !stderr_done => {
                match line {
                    Ok(Some(line)) => {
                        saw_output = true;
                        stream.capacity.observe(&line);
                        writer
                            .write(
                                LogKind::Stderr,
                                LogStream::Main,
                                serde_json::json!({ "line": line }),
                            )
                            .await?;
                    }
                    Ok(None) => stderr_done = true,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }

    // `--output-format=json` prints the final document pretty-printed over many
    // lines, so the per-line parse above never sees it. Recover the session id
    // and response text from the buffered tail.
    if let Some(document) = parse_final_json_document(&stdout_tail) {
        observe_gemini_error(&document, &mut stream);
        if agent_session_id.is_none() {
            agent_session_id = document
                .get("session_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
        }
        if summary.is_none() {
            summary = document
                .get("response")
                .and_then(|v| v.as_str())
                .map(truncate_summary);
        }
    }

    stream.summary = summary;
    stream.agent_session_id = agent_session_id;
    Ok(stream)
}

/// Parse the last top-level JSON object out of buffered stdout. Scans forward
/// from each `{` at line start so leading non-JSON noise (startup banners,
/// npm output) doesn't break recovery.
fn parse_final_json_document(stdout_tail: &str) -> Option<serde_json::Value> {
    for (offset, _) in stdout_tail.rmatch_indices("\n{") {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&stdout_tail[offset + 1..]) {
            return Some(value);
        }
    }
    if stdout_tail.trim_start().starts_with('{')
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(stdout_tail.trim())
    {
        return Some(value);
    }
    None
}

fn truncate_summary(content: &str) -> String {
    if content.chars().count() <= MAX_SUMMARY_CHARS {
        content.to_owned()
    } else {
        content.chars().take(MAX_SUMMARY_CHARS).collect()
    }
}

// ---------------------------------------------------------------------------
// Availability detection
// ---------------------------------------------------------------------------

fn detect_gemini_availability() -> AvailabilityInfo {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));

    let gemini_config = home.join(".gemini");
    if gemini_config.join("settings.json").exists() {
        return AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: Some(gemini_config.to_string_lossy().into_owned()),
        };
    }

    if std::env::var("GEMINI_API_KEY").is_ok() || std::env::var("GOOGLE_API_KEY").is_ok() {
        return AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        };
    }

    if gemini_config.exists() {
        return AvailabilityInfo {
            status: AvailabilityStatus::Installed,
            authenticated_at: None,
            config_path: Some(gemini_config.to_string_lossy().into_owned()),
        };
    }

    if executable_in_path("gemini") {
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

fn executable_in_path(name: &str) -> bool {
    which::which(name).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;

    #[cfg(unix)]
    #[tokio::test]
    async fn gemini_capacity_preserves_exit_status_success_semantics() {
        use std::os::unix::fs::PermissionsExt;

        for (message, exit_code) in [
            ("model rejected", 0),
            ("HTTP 429; retry in 90s", 0),
            ("model rejected", 1),
            ("HTTP 429; retry in 90s", 1),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let script = dir.path().join("gemini.sh");
            let document = serde_json::json!({"error": {"message": message}});
            std::fs::write(
                &script,
                format!("#!/bin/sh\nprintf '%s\\n' '{document}'\nexit {exit_code}\n"),
            )
            .unwrap();
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
            let result = GeminiAdapter::new()
                .execute(ExecutionContext {
                    task_id: "task-1".to_owned(),
                    execution_id: "gemini-capacity".to_owned(),
                    worktree_path: dir.path().display().to_string(),
                    description: "do the task".to_owned(),
                    agent_config: serde_json::json!({
                        "base_command_override": script.display().to_string(),
                        "auto_commit": false,
                    }),
                    logs_path: dir.path().join("gemini.jsonl").display().to_string(),
                    heartbeat_interval_seconds: 30,
                    max_turns: None,
                    log_sender: None,
                })
                .await;
            match (exit_code, message.starts_with("HTTP 429")) {
                (0, _) => {
                    let result = result.unwrap();
                    assert_eq!(result.status, ExecutionOutcome::Completed);
                    assert!(result.error.is_none());
                }
                (_, true) => assert!(matches!(
                    result,
                    Err(ExecutorError::UsageExhausted { retry_after: Some(delay), .. })
                        if delay == Duration::from_secs(90)
                )),
                (_, false) => {
                    let result = result.unwrap();
                    assert_eq!(result.status, ExecutionOutcome::Failed);
                    assert!(result.error.unwrap().contains(message));
                }
            }
        }
    }

    #[test]
    fn gemini_capacity_error_document_preserves_retry_hint() {
        let document = parse_final_json_document(
            "{\n  \"error\": {\n    \"code\": 429,\n    \"status\": \"RESOURCE_EXHAUSTED\",\n    \"details\": [{\"retryDelay\": \"90s\"}]\n  }\n}\n",
        ).unwrap();
        let mut stream = StreamResult {
            summary: None,
            agent_session_id: None,
            error: None,
            capacity: crate::capacity::CapacitySignal::default(),
        };
        observe_gemini_error(&document, &mut stream);
        assert!(stream.error.is_some());
        assert_eq!(
            stream.capacity.retry_after,
            Some(Some(Duration::from_secs(90)))
        );
    }

    #[test]
    fn gemini_capacity_classification_ignores_assistant_and_tool_text() {
        let mut stream = StreamResult {
            summary: None,
            agent_session_id: None,
            error: None,
            capacity: crate::capacity::CapacitySignal::default(),
        };
        for value in [
            serde_json::json!({"type": "assistant", "message": "rate limit"}),
            serde_json::json!({"type": "tool_result", "error": {"code": 429}}),
            serde_json::json!({"response": "usage limit reached"}),
        ] {
            observe_gemini_error(&value, &mut stream);
        }
        assert!(stream.capacity.retry_after.is_none());
        assert!(stream.error.is_none());
    }

    #[tokio::test]
    async fn discovery_advertises_current_models_and_aliases() {
        let discovered = GeminiAdapter::new()
            .discover_options(DiscoverContext { project_path: None })
            .await
            .expect("discovery should succeed");

        assert!(discovered.models.contains(&"auto".to_owned()));
        assert!(discovered.models.contains(&"gemini-3.8-flash".to_owned()));
        assert!(
            discovered
                .models
                .contains(&"gemini-3.5-flash-lite".to_owned())
        );
        assert!(discovered.models.contains(&"gemini-3.5-flash".to_owned()));
        assert!(
            discovered
                .models
                .contains(&"gemini-3.1-pro-preview".to_owned())
        );
        assert!(!discovered.models.contains(&"gemini-2.0-flash".to_owned()));
    }

    #[test]
    fn command_builder_forwards_resume_session_id() {
        let config = GeminiConfig {
            resume_session_id: Some("4297e3d8-c4d5-4789-bf33-63336047a747".to_owned()),
            command_overrides: CommandOverrides::default(),
            ..GeminiConfig::default()
        };

        let cmd = GeminiAdapter::build_command(&config, "finish the checklist");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(args.windows(2).any(|pair| {
            pair[0] == "--resume" && pair[1] == "4297e3d8-c4d5-4789-bf33-63336047a747"
        }));
    }

    #[test]
    fn parses_session_id_from_pretty_printed_final_document() {
        let stdout_tail = "YOLO mode enabled\n{\n  \"session_id\": \"0d12b860-6661-4aa9-a885-3685b9f8bae5\",\n  \"response\": \"All done.\",\n  \"stats\": {\n    \"models\": {}\n  }\n}\n";
        let document = parse_final_json_document(stdout_tail).expect("document");
        assert_eq!(
            document.get("session_id").and_then(|v| v.as_str()),
            Some("0d12b860-6661-4aa9-a885-3685b9f8bae5")
        );
        assert_eq!(
            document.get("response").and_then(|v| v.as_str()),
            Some("All done.")
        );
    }

    #[test]
    fn command_builder_maps_model_and_yolo() {
        let config = GeminiConfig {
            model: Some("gemini-2.5-pro".to_owned()),
            yolo: Some(true),
            command_overrides: CommandOverrides::default(),
            ..GeminiConfig::default()
        };

        let cmd = GeminiAdapter::build_command(&config, "do the task");
        assert_eq!(cmd.as_std().get_program(), "gemini");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(args.contains(&"--yolo".to_owned()));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "gemini-2.5-pro"])
        );
    }

    #[test]
    fn command_builder_maps_unattended_policies_to_yolo() {
        for policy in [
            PermissionPolicy::Yolo,
            PermissionPolicy::Auto,
            PermissionPolicy::Supervised,
        ] {
            let config = GeminiConfig {
                permission_policy: Some(policy),
                command_overrides: CommandOverrides::default(),
                ..GeminiConfig::default()
            };

            let cmd = GeminiAdapter::build_command(&config, "do the task");
            let args: Vec<_> = cmd
                .as_std()
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();

            assert!(args.contains(&"--yolo".to_owned()));
        }
    }

    #[test]
    fn yolo_policy_ignores_legacy_sandbox_setting() {
        let config = GeminiConfig {
            permission_policy: Some(PermissionPolicy::Yolo),
            sandbox: Some("docker".to_owned()),
            ..GeminiConfig::default()
        };

        let cmd = GeminiAdapter::build_command(&config, "do the task");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(args.contains(&"--yolo".to_owned()));
        assert!(!args.contains(&"--sandbox".to_owned()));
    }

    #[test]
    fn command_builder_includes_sandbox_and_check_every_n() {
        let config = GeminiConfig {
            sandbox: Some("docker".to_owned()),
            check_every_n: Some(5),
            command_overrides: CommandOverrides::default(),
            ..GeminiConfig::default()
        };

        let cmd = GeminiAdapter::build_command(&config, "do the task");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(args.windows(2).any(|pair| pair == ["--sandbox", "docker"]));
        assert!(args.windows(2).any(|pair| pair == ["--check_every_n", "5"]));
    }

    #[test]
    fn command_builder_with_base_override() {
        let config = GeminiConfig {
            command_overrides: CommandOverrides {
                base_command_override: Some("/usr/local/bin/gemini".to_owned()),
                additional_params: Some(vec!["--verbose".to_owned()]),
                env: None,
            },
            ..GeminiConfig::default()
        };

        let cmd = GeminiAdapter::build_command(&config, "do the task");
        assert_eq!(cmd.as_std().get_program(), "/usr/local/bin/gemini");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--verbose".to_owned()));
    }
}
