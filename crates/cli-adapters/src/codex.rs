pub mod client;
pub mod jsonrpc;
pub mod normalize;
pub mod protocol;

mod managed_home;

use managed_home::{
    adopt_legacy_managed_home, ambient_codex_home, codex_admitted_sandbox, package_cache_roots,
    prepare_managed_codex_home, resolve_path_for_overlap, safe_writable_root,
};

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

const DEFAULT_CODEX_VERSION: &str = "0.159.1";
const DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const CODEX_SYSTEM_ERROR_FALLBACK: &str = "codex thread entered systemError status";
const MANAGED_CONFIG: &str = "suppress_unstable_features_warning = true\n";
const MANAGED_WRITE_DELIVERY_INSTRUCTIONS: &str = "Forge-managed Codex Task delivery (authoritative): do not stage or commit changes. Leave completed file changes in the Task worktree. Forge finalizes them into the Task-branch commit outside this sandbox. Never request broader filesystem access to reach linked Git metadata.";
const MANAGED_RULES_FILE: &str = "forge-task-boundary.rules";
const MANAGED_RULES: &str = include_str!("../tests/fixtures/forge-task-boundary.rules");

const CODEX_MODELS: &[&str] = &[
    "gpt-6.1-sol",
    "gpt-6-astra",
    "gpt-6-sol",
    "gpt-6-luna",
    "gpt-reserve",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-5.5",
    "codex-auto-review",
];

pub const CODEX_REASONING_EFFORTS: &[&str] = &["low", "medium", "high", "xhigh", "max", "ultra"];

#[must_use]
pub fn codex_reasoning_efforts_for_model(model: &str) -> &'static [&'static str] {
    match model {
        "gpt-6.1-sol" | "gpt-6-astra" | "gpt-6-sol" | "gpt-5.6-sol" | "gpt-5.6-terra" => {
            CODEX_REASONING_EFFORTS
        }
        "gpt-6-luna" | "gpt-reserve" | "gpt-5.6-luna" | "codex-auto-review" => {
            &["low", "medium", "high", "xhigh", "max"]
        }
        "gpt-5.5" => &["low", "medium", "high", "xhigh"],
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
        managed_writable_roots: &[PathBuf],
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
            // Both roles get the same workspace-write sandbox: verification
            // (installing dependencies, building, running tests) needs network
            // access and writable package caches. A read-only role still cannot
            // deliver code — Forge fails any execution that leaves authored
            // changes or moves HEAD, then resets the worktree (see the
            // read-only gate in the task execution runner).
            let writable_roots = managed_writable_roots
                .iter()
                .map(|path| Value::String(path.to_string_lossy().into_owned()))
                .collect::<Vec<_>>();
            config_overrides.insert(
                "sandbox_workspace_write".to_owned(),
                json!({
                    "network_access": true,
                    "exclude_slash_tmp": true,
                    "exclude_tmpdir_env_var": true,
                    "writable_roots": writable_roots,
                }),
            );
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
            // `yolo` is the owner's explicit opt-out of the sandbox and wins for
            // managed Tasks too: real-browser QA, GUI toolchains, and servers
            // probed across commands cannot run under workspace-write. The
            // read-only gate in the task runner still discards a read-only
            // role's authored changes.
            sandbox: Some(if is_yolo {
                SandboxMode::DangerFullAccess
            } else if managed_task {
                SandboxMode::WorkspaceWrite
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
        let mut params = Self::thread_start_params(config, worktree_path, runtime_scope, &[], &[]);
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
        let legacy_home = Path::new(&ctx.logs_path)
            .parent()
            .map(|logs| logs.join(".codex-managed-home"));
        // The home belongs to the Task root, so two executions of different
        // Tasks never share or reset one home, wherever their logs go.
        let managed_home =
            match executors::sandbox::TaskRoot::of_worktree(Path::new(&ctx.worktree_path)) {
                Some(task_root) => {
                    adopt_legacy_managed_home(
                        legacy_home.as_deref(),
                        &task_root.home("codex"),
                        &ctx.task_id,
                    );
                    // Refuses a `home` that is a link or a file.
                    task_root.prepare_home("codex")
                }
                None => None,
            };
        let managed_home = match managed_home {
            Some(home) => home,
            // A Task root Forge did not reserve (or cannot use) keeps the
            // home beside its logs.
            None => legacy_home.ok_or_else(|| {
                ExecutorError::Other("managed Codex execution has no log directory".to_owned())
            })?,
        };
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
        let mut command = Self::build_command(&config, managed_codex_home.as_deref());
        // Owns the execution's temp directory until this execution returns.
        // Only what this execution's Codex sandbox will be able to write is
        // kept; the rest stays as `build_command` and the server left it.
        let is_yolo = matches!(
            config.permission_policy.clone().unwrap_or_default(),
            PermissionPolicy::Yolo
        );
        let managed_task = managed_codex_home.is_some();
        executors::sandbox::start_compiler_cache(std::path::Path::new(&ctx.worktree_path)).await;
        let run_scope = crate::command::run_in_task_worktree_with(&mut command, &ctx, |sandbox| {
            codex_admitted_sandbox(
                sandbox,
                Path::new(&ctx.worktree_path),
                managed_task,
                is_yolo,
            )
        });
        // The build directory is a writable root only when it is the one the
        // child will really use (the Project, the profile or the operator may
        // have chosen another).
        let build_in_use = run_scope
            .env()
            .build_dir("CARGO_TARGET_DIR")
            .filter(|build| {
                command.as_std().get_envs().any(|(key, value)| {
                    key == "CARGO_TARGET_DIR" && value == Some(build.as_os_str())
                })
            });
        // The Worker's TMPDIR: the per-run directory of the Task root, or the
        // home's own scratch when the Task root is not Forge's.
        let managed = ManagedRoots {
            scratch: managed_codex_home.as_ref().map(|home| {
                run_scope
                    .env()
                    .tmp_dir()
                    .map_or_else(|| home.join("task-scratch"), Path::to_path_buf)
            }),
            build: managed_codex_home
                .as_ref()
                .and(build_in_use)
                .and_then(Path::parent)
                .map(Path::to_path_buf),
            compiler_cache: managed_codex_home
                .as_ref()
                .and(run_scope.env().compiler_cache_dir_in_use(command.as_std()))
                .map(Path::to_path_buf),
            home: managed_codex_home,
        };
        executors::server_commands::ensure_allowed()
            .map_err(|e| ExecutorError::Other(e.to_string()))?;
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
                managed,
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
        managed_writable_roots: &[PathBuf],
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
                            managed_writable_roots,
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
                    Err(error) if is_unresumable_codex_thread_error(&error) => {
                        let response = client
                            .thread_start(Self::thread_start_params(
                                config,
                                &ctx.worktree_path,
                                &ctx.agent_config,
                                mcp_server_names,
                                managed_writable_roots,
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
                managed_writable_roots,
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
                Err(error) if is_unresumable_codex_thread_error(&error) => {
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
                managed_writable_roots,
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
        managed: ManagedRoots,
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
                    "sandbox": "workspace-write",
                    "network_access": true,
                    "worktree_read_only": executors::is_worktree_read_only(&ctx.agent_config),
                    "approval_policy": "never",
                    "isolated_codex_home": true,
                    "ambient_temp_roots_writable": false,
                    "task_scratch_root": managed.scratch.as_ref().map(|path| path.display().to_string()),
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
            // Outside the worktree a managed Task may write only its scratch
            // root, its outbox (worklog and evidence delivery), and the host's
            // package-manager caches (so dependency installs and builds work).
            let managed_writable_roots = if managed_task {
                let home = dirs::home_dir();
                let daemon_root = ctx.agent_config["_forge_workspace_root"]
                    .as_str()
                    .map(Path::new);
                let managed_home = managed.home.as_deref();
                let cache_roots = home.as_deref().map_or_else(Vec::new, |home| {
                    package_cache_roots(home, |key| std::env::var_os(key))
                        .into_iter()
                        .zip(package_cache_roots(home, |_| None))
                        .filter_map(|(root, default)| {
                            safe_writable_root(
                                &root,
                                &default,
                                Some(home),
                                Path::new(&ctx.worktree_path),
                                daemon_root,
                                managed_home,
                            )
                        })
                        .collect::<Vec<_>>()
                });
                // Forge creates these exact delivery/scratch directories. Cache
                // configuration must never grant additional Task or daemon paths.
                let scratch = managed.scratch.as_deref().map(resolve_path_for_overlap);
                // The per-Task build directory sits beside the worktree, so
                // a build that follows CARGO_TARGET_DIR must be able to write it.
                let build = managed.build.as_deref().and_then(|build| {
                    std::fs::create_dir_all(build).ok()?;
                    Some(resolve_path_for_overlap(build))
                });
                let outbox = Path::new(&ctx.worktree_path).parent().map(|task| {
                    resolve_path_for_overlap(task)
                        .join(".forge-outbox")
                        .join(&ctx.execution_id)
                });
                managed.scratch.iter().cloned()
                    .chain(managed.build.iter().cloned())
                    .chain(executors::execution_outbox_path(Path::new(&ctx.worktree_path), &ctx.execution_id))
                    .filter_map(|root| {
                        let resolved = resolve_path_for_overlap(&root);
                        if scratch.as_ref() == Some(&resolved)
                            || build.as_ref() == Some(&resolved)
                            || outbox.as_ref() == Some(&resolved)
                        {
                            Some(resolved)
                        } else {
                            tracing::warn!(root = %root.display(), resolved = %resolved.display(), "dropped redirected Codex scratch or outbox root");
                            None
                        }
                    })
                    .chain(cache_roots)
                    // The shared compiler cache's store for this repository:
                    // a root only while the child really carries its wrapper.
                    .chain(managed.compiler_cache.as_deref().map(resolve_path_for_overlap))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            Self::start_codex_session(
                &config,
                &ctx,
                &mut client,
                &mcp_server_names,
                &managed_writable_roots,
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
            .run_until_turn_complete(
                writer.clone(),
                stderr_rx,
                ctx.heartbeat_interval_seconds,
                &thread_id,
            )
            .await?;
        let mut outcome = run.outcome.unwrap_or(ExecutionOutcome::Failed);
        let mut summary = run.summary;
        let mut usage_reports = run.usage_reports;
        let mut provider_error = run.error;
        let mut capacity = run.capacity;
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
                        &thread_id,
                    )
                    .await
                {
                    outcome = followup.outcome.unwrap_or(outcome);
                    if followup.error.is_some() {
                        provider_error = followup.error;
                    }
                    if followup.capacity.retry_after.is_some() {
                        capacity = followup.capacity;
                    }
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
            if outcome == ExecutionOutcome::Failed
                && let Some(retry_after) = capacity.retry_after
            {
                return Err(ExecutorError::UsageExhausted {
                    retry_after,
                    usage_reports,
                });
            }
            let error = match outcome {
                ExecutionOutcome::Completed => None,
                ExecutionOutcome::Cancelled => Some("codex execution cancelled".to_owned()),
                ExecutionOutcome::Failed => provider_error
                    .or_else(|| summary.clone())
                    .or_else(|| Some("codex turn failed".to_owned())),
            };
            return Ok(ExecutionResult {
                status: outcome.clone(),
                after_sha: None,
                agent_session_id: Some(thread_id),
                summary,
                error,
                usage_reports,
                ..Default::default()
            });
        }

        let agent_session_id = Some(thread_id);
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

/// Whether Codex rejected a resume/fork of a thread Forge can never resume,
/// so the run must start fresh instead. Older Codex builds say "thread not
/// found"; current ones say "no rollout found for thread id". Before 0.13.6
/// Forge could also record a multi-agent sub-agent's thread as the session,
/// which Codex refuses to resume except through its parent.
fn is_unresumable_codex_thread_error(error: &ExecutorError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("thread not found")
        || message.contains("no rollout found")
        || message.contains("sub-agent through its parent")
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

/// The directories outside the worktree a managed Codex execution uses.
#[derive(Debug, Default)]
struct ManagedRoots {
    home: Option<PathBuf>,
    /// The Worker's `TMPDIR`.
    scratch: Option<PathBuf>,
    /// `<task root>/.forge-task/build`, when the Task root is Forge's.
    build: Option<PathBuf>,
    /// The repository store of the shared compiler cache, when this
    /// execution was really handed the wrapper that writes it.
    compiler_cache: Option<PathBuf>,
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
mod tests;
