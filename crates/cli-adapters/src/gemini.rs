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

        if config.yolo.unwrap_or(false) {
            adapter_args.push("--yolo".to_owned());
        } else if let Some(ref policy) = config.permission_policy {
            match policy {
                PermissionPolicy::Auto => {
                    adapter_args.push("--yolo".to_owned());
                }
                PermissionPolicy::Supervised | PermissionPolicy::Plan => {}
            }
        }

        if let Some(ref model) = config.model {
            adapter_args.push("--model".to_owned());
            adapter_args.push(model.clone());
        }

        if let Some(ref sandbox) = config.sandbox {
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
        let key_injected = config
            .command_overrides
            .env
            .as_ref()
            .is_some_and(|env| env.contains_key("GEMINI_API_KEY"));
        if key_injected && let Some(home) = ensure_api_key_home() {
            cmd.env("GEMINI_CLI_HOME", home);
        }
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
fn ensure_api_key_home() -> Option<PathBuf> {
    let home = std::env::temp_dir().join("forge-gemini-api-key-home");
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
                "gemini-3.5-flash".into(),
                "gemini-3.1-pro-preview".into(),
                "gemini-3.1-flash-lite".into(),
                "gemini-3-pro-preview".into(),
                "gemini-3-flash-preview".into(),
                "gemini-2.5-pro".into(),
                "gemini-2.5-flash".into(),
                "gemini-2.5-flash-lite".into(),
            ],
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
        crate::command::run_in_worktree(&mut cmd, &ctx.worktree_path);

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

        let (outcome, error) = if status.success() {
            (ExecutionOutcome::Completed, None)
        } else {
            (
                ExecutionOutcome::Failed,
                Some(format!("gemini exited with status {status}")),
            )
        };

        let after_sha = if outcome == ExecutionOutcome::Completed {
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
                        usage: None,
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
            usage: None,
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
    if (agent_session_id.is_none() || summary.is_none())
        && let Some(document) = parse_final_json_document(&stdout_tail)
    {
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

    Ok(StreamResult {
        summary,
        agent_session_id,
    })
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

    #[tokio::test]
    async fn discovery_advertises_current_models_and_aliases() {
        let discovered = GeminiAdapter::new()
            .discover_options(DiscoverContext { project_path: None })
            .await
            .expect("discovery should succeed");

        assert!(discovered.models.contains(&"auto".to_owned()));
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
    fn command_builder_maps_permission_policy_auto_to_yolo() {
        let config = GeminiConfig {
            permission_policy: Some(PermissionPolicy::Auto),
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
