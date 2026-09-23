pub mod client;
pub mod jsonrpc;
pub mod normalize;
pub mod protocol;

use async_trait::async_trait;
use client::{ChatToolHandler, CodexClient};
use command_group::{AsyncCommandGroup, AsyncGroupChild};
#[cfg(unix)]
use command_group::{Signal, UnixChildExt};
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodexConfig, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter, PermissionPolicy,
};
use protocol::{
    AskForApproval, DynamicToolSpec, SandboxMode, ThreadForkParams, ThreadForkResponse,
    ThreadResumeParams, ThreadResumeResponse, ThreadStartParams, ThreadStartResponse, TurnHandle,
};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;

const DEFAULT_CODEX_VERSION: &str = "0.154.0";
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const CODEX_SYSTEM_ERROR_FALLBACK: &str = "codex thread entered systemError status";
const MANAGED_CONFIG: &str = "suppress_unstable_features_warning = true\n";
const MANAGED_WRITE_DELIVERY_INSTRUCTIONS: &str = "Forge-managed Codex Task delivery (authoritative): do not stage or commit changes. Leave completed file changes in the Task worktree. Forge finalizes them into the Task-branch commit outside this sandbox. Never request broader filesystem access to reach linked Git metadata.";
const MANAGED_RULES_FILE: &str = "forge-task-boundary.rules";
const MANAGED_RULES: &str = include_str!("../tests/fixtures/forge-task-boundary.rules");

const CODEX_MODELS: &[&str] = &[
    "gpt-6-astra",
    "gpt-reserve",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex-spark",
    "codex-auto-review",
];

pub const CODEX_REASONING_EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max", "ultra"];

#[must_use]
pub fn codex_reasoning_efforts_for_model(model: &str) -> &'static [&'static str] {
    match model {
        "gpt-6-astra" | "gpt-5.6-sol" | "gpt-5.6-terra" => CODEX_REASONING_EFFORTS,
        "gpt-reserve" | "gpt-5.6-luna" | "codex-auto-review" => {
            &["low", "medium", "high", "xhigh", "max"]
        }
        "gpt-5.5" | "gpt-5.4" | "gpt-5.4-mini" | "gpt-5.3-codex-spark" => {
            &["low", "medium", "high", "xhigh"]
        }
        // Custom/newer models may use any effort currently advertised by the
        // Codex family. The provider remains authoritative for that model.
        _ => CODEX_REASONING_EFFORTS,
    }
}

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
    chat_tools: Option<Arc<dyn ChatToolHandler>>,
}

impl CodexAdapter {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
            chat_tools: None,
        }
    }

    /// Attach the host's per-turn tool registry to this adapter instance.
    ///
    /// Chat adapters are intentionally created per turn so a handler cannot
    /// accidentally be reused for another session or scope.
    #[must_use]
    pub fn with_chat_tools(mut self, handler: Arc<dyn ChatToolHandler>) -> Self {
        self.chat_tools = Some(handler);
        self
    }

    fn resolve_config(ctx: &ExecutionContext) -> CodexConfig {
        serde_json::from_value(ctx.agent_config.clone()).unwrap_or_default()
    }

    fn build_command(
        config: &CodexConfig,
        managed_codex_home: Option<&Path>,
    ) -> tokio::process::Command {
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
        // Workflow Tasks run with a Forge-owned Codex configuration root.
        // Apply this after profile command overrides so an authored profile
        // cannot reintroduce ambient rules, hooks, plugins, or connectors.
        if let Some(home) = managed_codex_home {
            let scratch = home.join("task-scratch");
            cmd.env("CODEX_HOME", home)
                .env("TMPDIR", &scratch)
                .env("TMP", &scratch)
                .env("TEMP", &scratch);
        }
        cmd
    }

    fn thread_start_params(
        config: &CodexConfig,
        worktree_path: &str,
        runtime_scope: &Value,
        mcp_server_names: &[String],
        managed_scratch_root: Option<&Path>,
    ) -> ThreadStartParams {
        let permission = config.permission_policy.clone().unwrap_or_default();
        let is_yolo = matches!(permission, PermissionPolicy::Yolo);
        let managed_task = executors::task_role(runtime_scope).is_some();
        let managed_read_only = executors::is_worktree_read_only(runtime_scope);
        let fallback_sandbox = match permission {
            PermissionPolicy::Yolo => SandboxMode::DangerFullAccess,
            PermissionPolicy::Auto | PermissionPolicy::Supervised => SandboxMode::WorkspaceWrite,
            PermissionPolicy::Plan => SandboxMode::ReadOnly,
        };
        let fallback_approval = match permission {
            PermissionPolicy::Yolo | PermissionPolicy::Auto => AskForApproval::Never,
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
        if let Some(profile) = &config.profile
            && !managed_task
        {
            config_overrides.insert("profile".to_owned(), Value::String(profile.clone()));
        }
        if let Some(include) = config.include_apply_patch_tool {
            config_overrides.insert("include_apply_patch_tool".to_owned(), Value::Bool(include));
        }
        if managed_task {
            // Task execution is a Forge-owned runtime boundary.  An isolated
            // CODEX_HOME removes filesystem-backed configuration, while these
            // per-thread overrides also suppress host-discovered skills and
            // account/plugin tools injected independently of that directory.
            config_overrides.insert(
                "features".to_owned(),
                json!({
                    "apps": false,
                    "plugins": false,
                    "remote_plugin": false,
                    "plugin_sharing": false,
                    "hooks": false,
                    "skill_search": false,
                    "skip_host_skill_discovery": true,
                    "tool_suggest": false,
                    "browser_use": false,
                    "computer_use": false,
                    "enable_mcp_apps": false,
                }),
            );
            config_overrides.insert(
                "skills".to_owned(),
                json!({
                    "include_instructions": false,
                    "config": [],
                }),
            );
            config_overrides.insert(
                "web_search".to_owned(),
                Value::String("disabled".to_owned()),
            );
            config_overrides.insert(
                "mcp_servers".to_owned(),
                Value::Object(
                    mcp_server_names
                        .iter()
                        .map(|name| (name.clone(), json!({ "enabled": false })))
                        .collect(),
                ),
            );
            if !managed_read_only {
                let writable_roots = managed_scratch_root
                    .map(|path| vec![Value::String(path.to_string_lossy().into_owned())])
                    .unwrap_or_default();
                config_overrides.insert(
                    "sandbox_workspace_write".to_owned(),
                    json!({
                        "network_access": false,
                        "exclude_slash_tmp": true,
                        "exclude_tmpdir_env_var": true,
                        "writable_roots": writable_roots,
                    }),
                );
            }
        }

        let developer_instructions = if managed_task && !managed_read_only {
            Some(match config.developer_instructions.as_deref() {
                Some(existing) if !existing.trim().is_empty() => {
                    format!("{existing}\n\n{MANAGED_WRITE_DELIVERY_INSTRUCTIONS}")
                }
                _ => MANAGED_WRITE_DELIVERY_INSTRUCTIONS.to_owned(),
            })
        } else {
            config.developer_instructions.clone()
        };

        ThreadStartParams {
            model: config.model.clone(),
            model_provider: None,
            cwd: Some(worktree_path.to_owned()),
            approval_policy: Some(if managed_task || is_yolo {
                AskForApproval::Never
            } else {
                AskForApproval::from_config(config.ask_for_approval.as_deref(), fallback_approval)
            }),
            sandbox: Some(if managed_read_only {
                SandboxMode::ReadOnly
            } else if managed_task {
                SandboxMode::WorkspaceWrite
            } else if is_yolo {
                SandboxMode::DangerFullAccess
            } else {
                SandboxMode::from_config(config.sandbox.as_deref(), fallback_sandbox)
            }),
            config: (!config_overrides.is_empty()).then_some(config_overrides),
            dynamic_tools: None,
            environments: None,
            base_instructions: config
                .base_instructions
                .clone()
                .or_else(|| config.prompt_template.clone()),
            developer_instructions,
            service_tier: None,
        }
    }

    fn chat_thread_start_params(
        config: &CodexConfig,
        worktree_path: &str,
        runtime_scope: &Value,
        dynamic_tools: Vec<DynamicToolSpec>,
        mcp_server_names: &[String],
    ) -> ThreadStartParams {
        let mut params = Self::thread_start_params(config, worktree_path, runtime_scope, &[], None);
        params.approval_policy = Some(AskForApproval::Never);
        params.sandbox = Some(SandboxMode::ReadOnly);
        params.dynamic_tools = Some(promote_payload_guidance(dynamic_tools));
        params.environments = Some(Vec::new());

        let mut overrides = params.config.take().unwrap_or_default();
        // These are all session config keys understood by Codex 0.154. Keep
        // Code Mode disabled for chat turns where the selected model honors
        // thread feature overrides; model metadata may still force a mode for
        // models that declare one in the Codex catalog. MCP server entries are
        // populated below from config/read and disabled individually because
        // an empty map may be merged with inherited configuration. The
        // adapter also rejects any elicitation that gets through.
        overrides.insert(
            "features".to_owned(),
            json!({
                "code_mode": false,
                "code_mode_only": false,
                "shell_tool": false,
                "apply_patch_freeform": false,
                "apps": false,
                "plugins": false,
                "remote_plugin": false,
                "plugin_sharing": false,
                "hooks": false,
                "skill_search": false,
                "skip_host_skill_discovery": true,
                "tool_suggest": false,
                "connectors": false,
                "browser_use": false,
                "computer_use": false,
                "enable_mcp_apps": false,
                "web_search": false,
            }),
        );
        overrides.insert(
            "web_search".to_owned(),
            Value::String("disabled".to_owned()),
        );
        overrides.insert(
            "skills".to_owned(),
            json!({
                "include_instructions": false,
                "config": [],
            }),
        );
        let mcp_servers = mcp_server_names
            .iter()
            .map(|name| (name.clone(), json!({ "enabled": false })))
            .collect::<serde_json::Map<_, _>>();
        overrides.insert("mcp_servers".to_owned(), Value::Object(mcp_servers));
        params.config = Some(overrides);
        params
    }

    fn managed_codex_home(ctx: &ExecutionContext) -> Result<Option<PathBuf>, ExecutorError> {
        if executors::task_role(&ctx.agent_config).is_none() {
            return Ok(None);
        }
        let logs_parent = Path::new(&ctx.logs_path).parent().ok_or_else(|| {
            ExecutorError::Other("managed Codex execution has no log directory".to_owned())
        })?;
        let managed_home = logs_parent.join(".codex-managed-home");
        prepare_managed_codex_home(&managed_home, &ambient_codex_home())?;
        Ok(Some(managed_home))
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

/// Preserve a typed orchestration envelope's operation guidance when Codex
/// compacts a large input schema for Code Mode.  The 0.154 schema compactor
/// removes nested `description` fields as its first size-reduction pass, while
/// the dynamic function description remains intact.  The host deliberately
/// keeps multi-operation payloads provider-portable as a plain object and puts
/// their per-operation shapes in `payload.description`, so hoist that one
/// description onto the function without changing validation or the schema
/// sent to direct-mode providers.
fn promote_payload_guidance(dynamic_tools: Vec<DynamicToolSpec>) -> Vec<DynamicToolSpec> {
    dynamic_tools
        .into_iter()
        .map(|mut tool| {
            let Some(payload_description) = tool
                .input_schema
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get("payload"))
                .and_then(|payload| payload.get("description"))
                .and_then(Value::as_str)
                .filter(|description| !description.trim().is_empty())
            else {
                return tool;
            };

            if tool.description.contains(payload_description) {
                return tool;
            }

            let function_description = tool.description.trim_end();
            tool.description = if function_description.is_empty() {
                payload_description.to_owned()
            } else {
                format!("{function_description}\n\n{payload_description}")
            };
            tool
        })
        .collect()
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
        let model_reasoning_efforts = CODEX_MODELS
            .iter()
            .map(|model| {
                (
                    (*model).to_owned(),
                    json!(codex_reasoning_efforts_for_model(model)),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        Ok(DiscoveredOptions {
            models: CODEX_MODELS
                .iter()
                .map(|model| (*model).to_owned())
                .collect(),
            permission_policies: vec![
                "auto".into(),
                "supervised".into(),
                "plan".into(),
                "yolo".into(),
            ],
            cli_specific: json!({
                "sandbox_modes": ["read-only", "workspace-write", "danger-full-access"],
                "approval_modes": ["never", "on-request", "on-failure", "unless-trusted"],
                "reasoning_efforts": CODEX_REASONING_EFFORTS,
                "model_reasoning_efforts": model_reasoning_efforts,
                "codex_version": DEFAULT_CODEX_VERSION,
            }),
        })
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        let config = Self::resolve_config(&ctx);
        let managed_codex_home = Self::managed_codex_home(&ctx)?;
        let managed_scratch_root = managed_codex_home
            .as_ref()
            .map(|home| home.join("task-scratch"));
        let mut command = Self::build_command(&config, managed_codex_home.as_deref());
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
                managed_scratch_root,
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
        mcp_server_names: &[String],
        managed_scratch_root: Option<&Path>,
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
                        Self::thread_start_params(
                            config,
                            &ctx.worktree_path,
                            &ctx.agent_config,
                            mcp_server_names,
                            managed_scratch_root,
                        ),
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
                            .thread_start(Self::thread_start_params(
                                config,
                                &ctx.worktree_path,
                                &ctx.agent_config,
                                mcp_server_names,
                                managed_scratch_root,
                            ))
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
            let thread_params = Self::thread_start_params(
                config,
                &ctx.worktree_path,
                &ctx.agent_config,
                mcp_server_names,
                managed_scratch_root,
            );
            let forked_thread_id = match client
                .thread_fork(ThreadForkParams::from_start(
                    resume_thread_id,
                    thread_params.clone(),
                ))
                .await
            {
                Ok(fork) => fork.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                    ExecutorError::Other("codex thread/fork response missing thread id".to_owned())
                })?,
                // A Task thread created before Forge isolated managed Codex
                // homes is not visible from the new home. Start clean inside
                // the managed boundary instead of importing ambient history
                // or making the Task permanently unrecoverable.
                Err(error) if is_missing_codex_thread_error(&error) => {
                    let response = client.thread_start(thread_params).await?;
                    response.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
                        ExecutorError::Other(
                            "codex thread/start response missing thread id".to_owned(),
                        )
                    })?
                }
                Err(error) => return Err(error),
            };
            let turn = client
                .turn_start(forked_thread_id.clone(), ctx.description.clone())
                .await?;
            Ok((forked_thread_id, turn.turn_id))
        } else {
            let thread_params = Self::thread_start_params(
                config,
                &ctx.worktree_path,
                &ctx.agent_config,
                mcp_server_names,
                managed_scratch_root,
            );
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

    async fn start_chat_session<C>(
        config: &CodexConfig,
        ctx: &ExecutionContext,
        client: &mut C,
        dynamic_tools: Vec<DynamicToolSpec>,
        mcp_server_names: &[String],
    ) -> Result<(String, Option<String>), ExecutorError>
    where
        C: CodexSessionClient + Send,
    {
        // A chat turn always gets a new thread. In particular, do not resume
        // or fork a task thread, since those operations could inherit tools
        // and permissions from the prior thread.
        let response = client
            .thread_start(Self::chat_thread_start_params(
                config,
                &ctx.worktree_path,
                &ctx.agent_config,
                dynamic_tools,
                mcp_server_names,
            ))
            .await?;
        let thread_id = response.thread_id().map(ToOwned::to_owned).ok_or_else(|| {
            ExecutorError::Other("codex thread/start response missing thread id".to_owned())
        })?;
        let turn = client
            .turn_start(thread_id.clone(), ctx.description.clone())
            .await?;
        Ok((thread_id, turn.turn_id))
    }

    #[allow(clippy::too_many_arguments)] // pre-existing warning, out of scope for this change
    async fn drive_codex(
        &self,
        ctx: ExecutionContext,
        config: CodexConfig,
        managed_scratch_root: Option<PathBuf>,
        stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        stderr_rx: mpsc::Receiver<String>,
        writer: Arc<AsyncMutex<LogWriter>>,
        cancel: CancellationToken,
    ) -> Result<ExecutionResult, ExecutorError> {
        let chat_tools = self.chat_tools.clone();
        let mut client = CodexClient::spawn_with_chat_tools(
            stdin,
            stdout,
            &ctx.worktree_path,
            cancel.clone(),
            chat_tools.clone(),
        );

        let managed_task = executors::task_role(&ctx.agent_config).is_some();
        if let Some(role) = executors::task_role(&ctx.agent_config) {
            write_shared_log(
                &writer,
                LogKind::SessionInfo,
                json!({
                    "type": "managed_execution_scope",
                    "task_role": role,
                    "sandbox": if executors::is_worktree_read_only(&ctx.agent_config) {
                        "read-only"
                    } else {
                        "workspace-write"
                    },
                    "approval_policy": "never",
                    "isolated_codex_home": true,
                    "ambient_temp_roots_writable": false,
                    "task_scratch_root": managed_scratch_root.as_ref().map(|path| path.display().to_string()),
                }),
            )
            .await?;
        }

        client.initialize().await?;
        client.initialized().await?;

        let (thread_id, turn_id) = if let Some(handler) = chat_tools.as_ref() {
            let effective_config = client.config_read(&ctx.worktree_path).await?;
            let mcp_server_names = configured_mcp_server_names(&effective_config);
            Self::start_chat_session(
                &config,
                &ctx,
                &mut client,
                handler.specs(),
                &mcp_server_names,
            )
            .await?
        } else {
            let mcp_server_names = if managed_task {
                let effective_config = client.config_read(&ctx.worktree_path).await?;
                configured_mcp_server_names(&effective_config)
            } else {
                Vec::new()
            };
            Self::start_codex_session(
                &config,
                &ctx,
                &mut client,
                &mcp_server_names,
                managed_scratch_root.as_deref(),
            )
            .await?
        };

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
        let mut usage_reports = run.usage_reports;
        for report in &mut usage_reports {
            report.fill_identity(None, config.model.as_deref());
        }

        if outcome == ExecutionOutcome::Cancelled {
            let _ = client.cancel_turn(thread_id.clone(), turn_id).await;
        }

        let force_managed_host_commit =
            managed_task && !executors::is_worktree_read_only(&ctx.agent_config);
        let auto_commit = chat_tools.is_none()
            && (force_managed_host_commit || crate::commit::auto_commit_enabled(&ctx));
        // Unmanaged Codex runs may still be configured to author their own
        // commit. Managed Task runs deliberately cannot write the linked Git
        // metadata outside their sandbox; Forge performs their host-side
        // finalization below, so a reminder would only provoke a doomed
        // approval/escalation attempt and a second provider turn.
        if auto_commit
            && !managed_task
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
                    for mut report in followup.usage_reports {
                        report.fill_identity(None, config.model.as_deref());
                        client::append_usage_report(&mut usage_reports, report);
                    }
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
                usage_reports,
                ..Default::default()
            });
        }

        let agent_session_id = run.thread_id.or(Some(thread_id));
        if !auto_commit {
            return Ok(ExecutionResult {
                status: ExecutionOutcome::Completed,
                // Chat sandboxes are intentionally not Git worktrees.  A
                // disabled auto-commit path must not probe Git either: even
                // `git rev-parse` is Task finalization in this context and
                // can turn a valid assistant reply into a failed turn.
                after_sha: None,
                agent_session_id,
                summary,
                error: None,
                usage_reports,
                ..Default::default()
            });
        }
        let after_sha = match crate::commit::commit_execution_changes_with_policy(
            &ctx,
            force_managed_host_commit,
        )
        .await
        {
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
                    usage_reports,
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
            usage_reports,
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

fn configured_mcp_server_names(config_read_response: &Value) -> Vec<String> {
    let Some(config) = config_read_response.get("config") else {
        return Vec::new();
    };
    let mut names = BTreeSet::new();
    collect_mcp_server_names(config.get("mcp_servers"), &mut names);
    if let Some(profiles) = config.get("profiles").and_then(Value::as_object) {
        for profile in profiles.values() {
            collect_mcp_server_names(profile.get("mcp_servers"), &mut names);
        }
    }
    names.into_iter().collect()
}

fn collect_mcp_server_names(value: Option<&Value>, names: &mut BTreeSet<String>) {
    let Some(servers) = value.and_then(Value::as_object) else {
        return;
    };
    names.extend(servers.keys().cloned());
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

fn ambient_codex_home() -> PathBuf {
    std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs_path("codex"))
}

/// Build the smallest Codex home a managed Task needs. Authentication is the
/// only shared state; personal configuration inputs are deliberately absent.
/// The home lives beside Task logs, outside the worktree sandbox, so a Worker
/// cannot seed rules for a later execution.
fn prepare_managed_codex_home(
    managed_home: &Path,
    ambient_home: &Path,
) -> Result<(), ExecutorError> {
    std::fs::create_dir_all(managed_home).map_err(|error| {
        ExecutorError::Other(format!(
            "failed to create managed Codex home {}: {error}",
            managed_home.display()
        ))
    })?;
    let home_metadata = managed_home.symlink_metadata().map_err(ExecutorError::Io)?;
    if home_metadata.file_type().is_symlink() || !home_metadata.is_dir() {
        return Err(ExecutorError::Other(format!(
            "managed Codex home is not a Forge-owned directory: {}",
            managed_home.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(managed_home, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                ExecutorError::Other(format!(
                    "failed to protect managed Codex home {}: {error}",
                    managed_home.display()
                ))
            },
        )?;
    }

    // Codex materializes config.toml (project trust), bundled system skills,
    // and a plugins directory in its home while it runs. They are runtime
    // cache, not durable authority: discard them before every execution and
    // recreate only Forge's canonical config. This also makes a second attempt
    // safe when it reuses the Task log directory. Files that Codex does not
    // create remain fail-closed.
    //
    // `plugins` used to sit on the fail-closed list below, which meant the
    // first Codex execution in a Task always succeeded and every later one in
    // the same log directory died deterministically with "forbidden
    // configuration input" — Codex had created the directory itself during the
    // first run. Every re-review and every retry after a review was therefore
    // unreachable; observed as three identical reviewer failures on one Task
    // whose first reviewer attempt had completed normally.
    reset_managed_runtime_path(&managed_home.join("config.toml"))?;
    reset_managed_runtime_path(&managed_home.join("skills"))?;
    reset_managed_runtime_path(&managed_home.join("task-scratch"))?;
    reset_managed_runtime_path(&managed_home.join("plugins"))?;
    // These two Codex never writes. A repository or a user planting one is the
    // case this guard exists for, so they stay fail-closed.
    for forbidden in ["AGENTS.md", "hooks.json"] {
        let path = managed_home.join(forbidden);
        if path.exists() {
            return Err(ExecutorError::Other(format!(
                "managed Codex home contains forbidden configuration input: {}",
                path.display()
            )));
        }
    }
    ensure_managed_config(managed_home)?;
    ensure_managed_rules(managed_home)?;
    let scratch = managed_home.join("task-scratch");
    std::fs::create_dir(&scratch).map_err(ExecutorError::Io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700))
            .map_err(ExecutorError::Io)?;
    }

    let source_auth = ambient_home.join("auth.json");
    if !source_auth.is_file() {
        // Environment- or keyring-backed authentication does not need a file.
        return Ok(());
    }
    let managed_auth = managed_home.join("auth.json");
    if managed_auth.symlink_metadata().is_ok() {
        let source = source_auth.canonicalize().map_err(ExecutorError::Io)?;
        let linked = managed_auth.canonicalize().map_err(ExecutorError::Io)?;
        if linked != source {
            return Err(ExecutorError::Other(format!(
                "managed Codex authentication link points at an unexpected file: {}",
                managed_auth.display()
            )));
        }
        return Ok(());
    }

    #[cfg(unix)]
    std::os::unix::fs::symlink(&source_auth, &managed_auth).map_err(ExecutorError::Io)?;
    #[cfg(windows)]
    std::fs::hard_link(&source_auth, &managed_auth).map_err(ExecutorError::Io)?;
    Ok(())
}

fn reset_managed_runtime_path(path: &Path) -> Result<(), ExecutorError> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            std::fs::remove_dir_all(path).map_err(ExecutorError::Io)
        }
        Ok(_) => std::fs::remove_file(path).map_err(ExecutorError::Io),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExecutorError::Io(error)),
    }
}

fn ensure_managed_config(managed_home: &Path) -> Result<(), ExecutorError> {
    let path = managed_home.join("config.toml");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path).map_err(ExecutorError::Io)?;
    file.write_all(MANAGED_CONFIG.as_bytes())
        .map_err(ExecutorError::Io)
}

fn ensure_managed_rules(managed_home: &Path) -> Result<(), ExecutorError> {
    let rules_dir = managed_home.join("rules");
    match rules_dir.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ExecutorError::Other(format!(
                "managed Codex rules path is not a Forge-owned directory: {}",
                rules_dir.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&rules_dir).map_err(ExecutorError::Io)?;
        }
        Err(error) => return Err(ExecutorError::Io(error)),
    }

    let expected = rules_dir.join(MANAGED_RULES_FILE);
    for entry in std::fs::read_dir(&rules_dir).map_err(ExecutorError::Io)? {
        let entry = entry.map_err(ExecutorError::Io)?;
        if entry.path() != expected {
            return Err(ExecutorError::Other(format!(
                "managed Codex home contains an unexpected rules file: {}",
                entry.path().display()
            )));
        }
    }
    match std::fs::read_to_string(&expected) {
        Ok(contents) if contents == MANAGED_RULES => return Ok(()),
        Ok(_) => {
            return Err(ExecutorError::Other(format!(
                "managed Codex Task boundary rules were modified: {}",
                expected.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ExecutorError::Io(error)),
    }

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(&expected).map_err(ExecutorError::Io)?;
    file.write_all(MANAGED_RULES.as_bytes())
        .map_err(ExecutorError::Io)
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

        let cmd = CodexAdapter::build_command(&config, None);
        assert_eq!(cmd.as_std().get_program(), "npx");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec!["-y", "@openai/codex@0.154.0", "app-server", "--verbose"]
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
                "gpt-6-astra",
                "gpt-reserve",
                "gpt-5.6-sol",
                "gpt-5.6-terra",
                "gpt-5.6-luna",
                "gpt-5.5",
                "gpt-5.4",
                "gpt-5.4-mini",
                "gpt-5.3-codex-spark",
                "codex-auto-review",
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
        assert_eq!(
            codex_reasoning_efforts_for_model("gpt-5.5"),
            ["low", "medium", "high", "xhigh"]
        );
        assert_eq!(
            codex_reasoning_efforts_for_model("gpt-reserve"),
            ["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(
            codex_reasoning_efforts_for_model("future-codex-model"),
            CODEX_REASONING_EFFORTS
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

        let params =
            CodexAdapter::thread_start_params(&config, "/tmp/worktree", &json!({}), &[], None);

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
    fn thread_start_params_maps_yolo_to_full_access_without_approval() {
        let config = CodexConfig {
            permission_policy: Some(PermissionPolicy::Yolo),
            sandbox: Some("read-only".to_owned()),
            ask_for_approval: Some("on-request".to_owned()),
            ..CodexConfig::default()
        };

        let params =
            CodexAdapter::thread_start_params(&config, "/tmp/worktree", &json!({}), &[], None);

        assert!(matches!(
            params.sandbox,
            Some(SandboxMode::DangerFullAccess)
        ));
        assert!(matches!(
            params.approval_policy,
            Some(AskForApproval::Never)
        ));
    }

    #[test]
    fn managed_task_scope_overrides_yolo_and_profile_approval_settings() {
        let config = CodexConfig {
            permission_policy: Some(PermissionPolicy::Yolo),
            sandbox: Some("danger-full-access".to_owned()),
            ask_for_approval: Some("on-request".to_owned()),
            profile: Some("ambient-profile".to_owned()),
            ..CodexConfig::default()
        };
        let mut runtime_scope = json!({});
        executors::mark_task_role(&mut runtime_scope, "worker");

        let params = CodexAdapter::thread_start_params(
            &config,
            "/tmp/worktree",
            &runtime_scope,
            &["project-local".to_owned()],
            Some(Path::new("/tmp/forge-task-scratch")),
        );

        assert!(matches!(params.sandbox, Some(SandboxMode::WorkspaceWrite)));
        assert!(matches!(
            params.approval_policy,
            Some(AskForApproval::Never)
        ));
        let overrides = params.config.expect("managed overrides are present");
        assert_eq!(overrides.get("profile"), None);
        assert_eq!(overrides["features"]["apps"], json!(false));
        assert_eq!(overrides["features"]["plugins"], json!(false));
        assert_eq!(overrides["features"]["hooks"], json!(false));
        assert_eq!(overrides["features"]["skill_search"], json!(false));
        assert_eq!(
            overrides["features"]["skip_host_skill_discovery"],
            json!(true)
        );
        assert_eq!(overrides["skills"]["include_instructions"], json!(false));
        assert_eq!(overrides["web_search"], json!("disabled"));
        assert_eq!(
            overrides["mcp_servers"]["project-local"]["enabled"],
            json!(false)
        );
        assert_eq!(
            overrides["sandbox_workspace_write"],
            json!({
                "network_access": false,
                "exclude_slash_tmp": true,
                "exclude_tmpdir_env_var": true,
                "writable_roots": ["/tmp/forge-task-scratch"],
            })
        );
        assert_eq!(
            params.developer_instructions.as_deref(),
            Some(MANAGED_WRITE_DELIVERY_INSTRUCTIONS)
        );
    }

    #[test]
    fn managed_read_only_role_stays_read_only() {
        let config = CodexConfig {
            permission_policy: Some(PermissionPolicy::Yolo),
            ..CodexConfig::default()
        };
        let mut runtime_scope = json!({});
        executors::mark_task_role(&mut runtime_scope, "reviewer");
        executors::mark_worktree_read_only(&mut runtime_scope);

        let params =
            CodexAdapter::thread_start_params(&config, "/tmp/worktree", &runtime_scope, &[], None);

        assert!(matches!(params.sandbox, Some(SandboxMode::ReadOnly)));
        assert!(matches!(
            params.approval_policy,
            Some(AskForApproval::Never)
        ));
    }

    #[test]
    fn managed_codex_home_shares_only_authentication() {
        let dir = tempfile::tempdir().expect("tempdir creates");
        let ambient = dir.path().join("ambient");
        let managed = dir.path().join("logs/task/.codex-managed-home");
        fs::create_dir_all(ambient.join("rules")).expect("ambient rules dir creates");
        fs::write(ambient.join("auth.json"), "{\"auth_mode\":\"chatgpt\"}")
            .expect("ambient auth writes");
        fs::write(ambient.join("config.toml"), "approval_policy = 'never'")
            .expect("ambient config writes");
        fs::write(
            ambient.join("rules/default.rules"),
            "prefix_rule(pattern=[\"git\",\"merge\"], decision=\"allow\")",
        )
        .expect("ambient rule writes");

        prepare_managed_codex_home(&managed, &ambient).expect("managed home prepares");

        assert_eq!(
            managed
                .join("auth.json")
                .canonicalize()
                .expect("managed auth resolves"),
            ambient
                .join("auth.json")
                .canonicalize()
                .expect("ambient auth resolves")
        );
        assert_eq!(
            fs::read_to_string(managed.join("config.toml")).expect("managed config reads"),
            MANAGED_CONFIG
        );
        let managed_rules = fs::read_to_string(managed.join("rules").join(MANAGED_RULES_FILE))
            .expect("managed deny rules read");
        assert!(managed_rules.contains("decision=\"forbidden\""));
        assert!(managed_rules.contains("[\"git\", \"merge\"]"));
        assert!(!managed_rules.contains("decision=\"allow\""));

        fs::write(
            managed.join("config.toml"),
            format!("{MANAGED_CONFIG}\n[projects.\"/tmp/generated\"]\ntrust_level = \"trusted\"\n"),
        )
        .expect("Codex-generated project trust writes");
        fs::create_dir_all(managed.join("skills/.system/generated"))
            .expect("Codex-generated skill directory writes");
        fs::write(
            managed.join("skills/.system/generated/SKILL.md"),
            "runtime-generated",
        )
        .expect("Codex-generated skill writes");
        fs::write(managed.join("task-scratch/transient"), "temporary")
            .expect("Task scratch writes");

        prepare_managed_codex_home(&managed, &ambient)
            .expect("managed home safely prepares for another execution");
        assert_eq!(
            fs::read_to_string(managed.join("config.toml")).expect("reset managed config reads"),
            MANAGED_CONFIG
        );
        assert!(!managed.join("skills").exists());
        assert!(managed.join("task-scratch").is_dir());
        assert!(
            fs::read_dir(managed.join("task-scratch"))
                .expect("Task scratch reads")
                .next()
                .is_none(),
            "Task scratch is reset between attempts"
        );

        fs::write(managed.join("rules/foreign.rules"), "# not Forge-owned")
            .expect("foreign rule writes");
        let error = prepare_managed_codex_home(&managed, &ambient)
            .expect_err("managed authority inputs fail closed");
        assert!(error.to_string().contains("unexpected rules file"));
    }

    #[test]
    fn a_plugins_directory_codex_created_does_not_block_the_next_execution() {
        // Codex writes `plugins/` into its home while it runs. Treating that
        // as a forbidden configuration input meant the first execution in a
        // Task's log directory always succeeded and every later one died
        // deterministically — so a re-review, or any retry after a review,
        // could not run at all. Observed as three identical reviewer failures
        // on a Task whose first reviewer attempt had completed normally.
        let root = std::env::temp_dir().join(format!(
            "forge-codex-plugins-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        let managed = root.join("managed");
        let ambient = root.join("ambient");
        fs::create_dir_all(&ambient).expect("ambient home writes");
        fs::write(ambient.join("auth.json"), "{}").expect("ambient auth writes");

        prepare_managed_codex_home(&managed, &ambient).expect("first execution prepares");

        // Exactly what Codex leaves behind: the directory plus its own
        // staging and cache children.
        fs::create_dir_all(managed.join("plugins/.remote-plugin-install-staging"))
            .expect("Codex plugin staging writes");
        fs::create_dir_all(managed.join("plugins/cache")).expect("Codex plugin cache writes");

        prepare_managed_codex_home(&managed, &ambient)
            .expect("a second execution must prepare over Codex's own plugins directory");
        assert!(
            !managed.join("plugins").exists(),
            "the plugins directory is runtime cache and is discarded, not inherited"
        );

        // The inputs Codex never writes stay fail-closed.
        for forbidden in ["AGENTS.md", "hooks.json"] {
            fs::write(managed.join(forbidden), "planted").expect("planted input writes");
            let error = prepare_managed_codex_home(&managed, &ambient)
                .expect_err("a planted configuration input must fail closed");
            assert!(
                error.to_string().contains("forbidden configuration input"),
                "{forbidden} must still be refused: {error}"
            );
            fs::remove_file(managed.join(forbidden)).expect("planted input clears");
        }

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn managed_codex_home_overrides_profile_environment() {
        let config = CodexConfig {
            command_overrides: CommandOverrides {
                env: Some(HashMap::from([(
                    "CODEX_HOME".to_owned(),
                    "/tmp/profile-home".to_owned(),
                )])),
                ..CommandOverrides::default()
            },
            ..CodexConfig::default()
        };
        let managed = Path::new("/tmp/forge-managed-home");

        let cmd = CodexAdapter::build_command(&config, Some(managed));
        let command = cmd.as_std();
        let actual = command
            .get_envs()
            .find_map(|(key, value)| {
                (key == "CODEX_HOME").then(|| value.expect("CODEX_HOME has value"))
            })
            .expect("CODEX_HOME is set");

        assert_eq!(actual, managed.as_os_str());
        let expected_scratch = managed.join("task-scratch");
        for key in ["TMPDIR", "TMP", "TEMP"] {
            let actual = command
                .get_envs()
                .find_map(|(candidate, value)| {
                    (candidate == key).then(|| value.expect("scratch variable has value"))
                })
                .unwrap_or_else(|| panic!("{key} is set"));
            assert_eq!(actual, expected_scratch.as_os_str());
        }
    }

    #[test]
    fn chat_thread_start_params_advertises_only_read_only_scoped_tools() {
        let config = CodexConfig {
            model: Some("gpt-5-codex".to_owned()),
            permission_policy: Some(PermissionPolicy::Auto),
            include_apply_patch_tool: Some(false),
            ..CodexConfig::default()
        };
        let tools = vec![DynamicToolSpec {
            name: "forge_echo".to_owned(),
            description: "Echo JSON".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": { "value": { "type": "string" } },
            }),
        }];
        let params = CodexAdapter::chat_thread_start_params(
            &config,
            "/tmp/worktree",
            &json!({}),
            tools,
            &["inherited-server".to_owned()],
        );
        let wire = serde_json::to_value(&params).expect("thread params serialize");

        assert_eq!(wire["approvalPolicy"], "never");
        assert_eq!(wire["sandbox"], "read-only");
        assert_eq!(wire["environments"], json!([]));
        assert_eq!(wire["dynamicTools"][0]["type"], "function");
        assert_eq!(wire["dynamicTools"][0]["name"], "forge_echo");
        assert_eq!(wire["dynamicTools"][0]["inputSchema"]["type"], "object");
        assert_eq!(wire["config"]["features"]["code_mode"], false);
        assert_eq!(wire["config"]["features"]["code_mode_only"], false);
        assert_eq!(wire["config"]["features"]["shell_tool"], false);
        assert_eq!(wire["config"]["features"]["apply_patch_freeform"], false);
        assert_eq!(wire["config"]["features"]["apps"], false);
        assert_eq!(wire["config"]["features"]["plugins"], false);
        assert_eq!(wire["config"]["features"]["hooks"], false);
        assert_eq!(wire["config"]["features"]["skill_search"], false);
        assert_eq!(
            wire["config"]["features"]["skip_host_skill_discovery"],
            true
        );
        assert_eq!(wire["config"]["features"]["connectors"], false);
        assert_eq!(wire["config"]["features"]["browser_use"], false);
        assert_eq!(wire["config"]["features"]["computer_use"], false);
        assert_eq!(wire["config"]["features"]["enable_mcp_apps"], false);
        assert_eq!(wire["config"]["features"]["web_search"], false);
        assert_eq!(wire["config"]["web_search"], "disabled");
        assert_eq!(wire["config"]["skills"]["include_instructions"], false);
        assert_eq!(
            wire["config"]["mcp_servers"]["inherited-server"]["enabled"],
            false
        );
    }

    #[test]
    fn chat_dynamic_tool_promotes_nested_payload_guidance() {
        let guidance = "project.review_config requires action=set_ci_steps and string ci_steps";
        let params = CodexAdapter::chat_thread_start_params(
            &CodexConfig::default(),
            "/tmp/worktree",
            &json!({}),
            vec![DynamicToolSpec {
                name: "forge_project_orchestration_propose".to_owned(),
                description: "Submit a typed Forge proposal.".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "payload": {
                            "type": "object",
                            "description": guidance,
                        },
                    },
                }),
            }],
            &[],
        );
        let wire = serde_json::to_value(params).expect("thread params serialize");

        assert_eq!(
            wire["dynamicTools"][0]["description"],
            format!("Submit a typed Forge proposal.\n\n{guidance}")
        );
        assert_eq!(
            wire["dynamicTools"][0]["inputSchema"]["properties"]["payload"]["description"],
            guidance
        );
    }

    #[test]
    fn config_read_mcp_names_include_top_level_and_profile_servers() {
        let response = json!({
            "config": {
                "mcp_servers": { "root-server": {} },
                "profiles": {
                    "chat": { "mcp_servers": { "profile-server": {} } },
                    "review": { "mcp_servers": { "root-server": {} } },
                },
            },
        });

        assert_eq!(
            configured_mcp_server_names(&response),
            vec!["profile-server", "root-server"]
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
        start_params: Option<ThreadStartParams>,
        fork_params: Option<ThreadForkParams>,
        resume_params: Option<ThreadResumeParams>,
        turn_thread_id: Option<String>,
        turn_prompt: Option<String>,
        fail_resume_with_missing_thread: bool,
        fail_fork_with_missing_thread: bool,
        turn_attempts: usize,
    }

    #[async_trait]
    impl CodexSessionClient for StubCodexSessionClient {
        async fn thread_start(
            &mut self,
            params: ThreadStartParams,
        ) -> Result<ThreadStartResponse, ExecutorError> {
            self.calls.push("thread_start");
            self.start_params = Some(params);
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
            if self.fail_fork_with_missing_thread {
                return Err(ExecutorError::Other(
                    "thread/fork failed: thread not found: source-thread (-32600)".to_owned(),
                ));
            }
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

        let (thread_id, turn_id) =
            CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], None)
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
    async fn resume_review_starts_fresh_when_legacy_source_thread_is_missing() {
        let mut client = StubCodexSessionClient {
            fail_fork_with_missing_thread: true,
            ..StubCodexSessionClient::default()
        };
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

        let (thread_id, turn_id) =
            CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], None)
                .await
                .expect("missing legacy session starts fresh");

        assert_eq!(thread_id, "fresh-thread");
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(
            client.calls,
            vec!["thread_fork", "thread_start", "turn_start"]
        );
        assert_eq!(client.turn_thread_id.as_deref(), Some("fresh-thread"));
        assert_eq!(client.turn_prompt.as_deref(), Some("Review the changes"));
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

        let (thread_id, turn_id) =
            CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], None)
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

        let (thread_id, turn_id) =
            CodexAdapter::start_codex_session(&config, &ctx, &mut client, &[], None)
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

    #[tokio::test]
    async fn chat_session_always_starts_fresh_thread_even_when_resume_is_configured() {
        let mut client = StubCodexSessionClient::default();
        let config = CodexConfig {
            resume_thread_id: Some("old-thread".to_owned()),
            resume_thread_in_place: Some(true),
            model: Some("gpt-5-codex".to_owned()),
            ..CodexConfig::default()
        };
        let ctx = ExecutionContext {
            task_id: "chat-1".to_owned(),
            execution_id: "turn-1".to_owned(),
            worktree_path: "/tmp/forge-chat-worktree".to_owned(),
            description: "Answer the user".to_owned(),
            agent_config: json!({}),
            logs_path: "/tmp/forge-chat.log".to_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };

        let (thread_id, turn_id) = CodexAdapter::start_chat_session(
            &config,
            &ctx,
            &mut client,
            vec![DynamicToolSpec {
                name: "forge_echo".to_owned(),
                description: "Echo JSON".to_owned(),
                input_schema: json!({"type": "object"}),
            }],
            &["inherited-server".to_owned()],
        )
        .await
        .expect("chat session starts");

        assert_eq!(thread_id, "fresh-thread");
        assert_eq!(turn_id.as_deref(), Some("turn-1"));
        assert_eq!(client.calls, vec!["thread_start", "turn_start"]);
        assert!(client.resume_params.is_none());
        let start_params = client.start_params.expect("chat start params captured");
        assert_eq!(start_params.dynamic_tools.as_ref().map(Vec::len), Some(1));
        assert_eq!(start_params.environments, Some(Vec::new()));
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
