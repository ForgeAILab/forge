use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use serde_json::{json, Value};
use tokio::{process::Command, time::timeout};
use tracing::{error, info, warn};

use crate::lifecycle::{LifecycleHookContext, PluginRegistry, PluginResult};
use crate::workspace_backend::{
    ResolvedWorkspace, RunSpec, WorkspaceBackendError, WorkspaceRunPurpose,
};

pub struct LifecycleHookRunner;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LifecycleHookRun {
    pub index: usize,
    pub entry: Value,
    pub status: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub stdout: String,
    pub stderr: String,
    pub error: Option<String>,
    pub command: Option<String>,
    pub working_dir: String,
    pub duration_ms: u64,
    pub log_path: Option<String>,
    pub environment_preview: BTreeMap<String, String>,
}

impl LifecycleHookRunner {
    pub async fn run_hooks(
        ctx: LifecycleHookContext,
        hooks: &[api_types::LifecycleHookDef],
        plugin_registry: Arc<PluginRegistry>,
    ) {
        Self::run_hooks_inner(ctx, hooks, plugin_registry, None).await;
    }

    pub async fn run_workspace_hooks(
        ctx: LifecycleHookContext,
        hooks: &[api_types::LifecycleHookDef],
        plugin_registry: Arc<PluginRegistry>,
        workspace: &ResolvedWorkspace,
    ) {
        Self::run_hooks_inner(ctx, hooks, plugin_registry, Some(workspace)).await;
    }

    async fn run_hooks_inner(
        ctx: LifecycleHookContext,
        hooks: &[api_types::LifecycleHookDef],
        plugin_registry: Arc<PluginRegistry>,
        workspace: Option<&ResolvedWorkspace>,
    ) {
        for (index, hook) in hooks.iter().enumerate() {
            match hook {
                api_types::LifecycleHookDef::Script {
                    command,
                    timeout_seconds,
                    blocking,
                } => {
                    if ctx.event == api_types::LifecycleEvent::BeforeWork && *blocking {
                        info!(
                            event = %event_name(&ctx.event),
                            task_id = %ctx.task_id,
                            hook_index = index,
                            "blocking before_work script hook skipped by async lifecycle runner"
                        );
                        continue;
                    }
                    match Self::run_script(&ctx, index, command, *timeout_seconds, workspace).await
                    {
                        Ok(mut run) => {
                            run.log_path = Self::write_log_entry(&ctx, index, &run.entry)
                        }
                        Err(error) => warn!(
                            event = %event_name(&ctx.event), task_id = %ctx.task_id,
                            hook_index = index, %error, "lifecycle workspace hook unavailable"
                        ),
                    }
                }
                api_types::LifecycleHookDef::Plugin {
                    name,
                    enabled,
                    config: _,
                } => {
                    if !enabled {
                        info!(
                            event = %event_name(&ctx.event),
                            task_id = %ctx.task_id,
                            plugin_name = %name,
                            "lifecycle plugin hook disabled"
                        );
                        continue;
                    }

                    let entry = Self::run_plugin_hook(
                        &ctx,
                        index,
                        name,
                        plugin_registry.as_ref(),
                        workspace,
                    )
                    .await;
                    Self::write_log_entry(&ctx, index, &entry);
                }
            }
        }
    }

    pub async fn run_blocking_before_work_hooks(
        ctx: LifecycleHookContext,
        hooks: &[api_types::LifecycleHookDef],
    ) -> Option<LifecycleHookRun> {
        if ctx.event != api_types::LifecycleEvent::BeforeWork {
            return None;
        }

        for (index, hook) in hooks.iter().enumerate() {
            let api_types::LifecycleHookDef::Script {
                command,
                timeout_seconds,
                blocking,
            } = hook
            else {
                continue;
            };
            if !blocking {
                continue;
            }
            let mut run = Self::run_script_hook(&ctx, index, command, *timeout_seconds).await;
            run.log_path = Self::write_log_entry(&ctx, index, &run.entry);
            if run.status != "success" {
                return Some(run);
            }
        }
        None
    }

    pub async fn run_blocking_workspace_hooks(
        ctx: LifecycleHookContext,
        hooks: &[api_types::LifecycleHookDef],
        workspace: &ResolvedWorkspace,
    ) -> crate::Result<Option<LifecycleHookRun>> {
        if ctx.event != api_types::LifecycleEvent::BeforeWork {
            return Ok(None);
        }

        for (index, hook) in hooks.iter().enumerate() {
            let api_types::LifecycleHookDef::Script {
                command,
                timeout_seconds,
                blocking,
            } = hook
            else {
                continue;
            };
            if !blocking {
                continue;
            }
            let attempt = crate::workflow::engine::durable::current_hook(&ctx.task_id);
            let (recorded, interrupted) = match &attempt {
                Some(attempt) => {
                    attempt
                        .db
                        .start_hook_script(&attempt.step, attempt.index, index as i64)
                        .await?
                }
                None => (None, false),
            };
            let mut run = if let Some(recorded) = recorded {
                serde_json::from_str::<LifecycleHookRun>(&recorded)
                    .map_err(|e| crate::ServiceError::invalid_operation(e.to_string()))?
            } else {
                let mut run = Self::run_workspace_script_hook(
                    &ctx,
                    index,
                    command,
                    *timeout_seconds,
                    workspace,
                )
                .await?;
                if let Some(attempt) = &attempt {
                    run.entry["step_id"] = json!(attempt.step.id);
                    if interrupted {
                        run.entry["rerun_after_interruption"] = json!(true);
                    }
                    attempt
                        .db
                        .finish_hook_script(
                            &attempt.step,
                            attempt.index,
                            index as i64,
                            &serde_json::to_string(&run).map_err(|e| {
                                crate::ServiceError::invalid_operation(e.to_string())
                            })?,
                        )
                        .await?;
                }
                run
            };
            run.log_path = Self::write_log_entry(&ctx, index, &run.entry);
            if run.status != "success" {
                return Ok(Some(run));
            }
        }
        Ok(None)
    }

    pub async fn test_script_hook(
        ctx: &LifecycleHookContext,
        index: usize,
        command: &str,
        timeout_seconds: u64,
    ) -> LifecycleHookRun {
        let mut run = Self::run_script_hook(ctx, index, command, timeout_seconds).await;
        run.log_path = Self::write_log_entry(ctx, index, &run.entry);
        run
    }

    pub async fn test_workspace_script_hook(
        ctx: &LifecycleHookContext,
        index: usize,
        command: &str,
        timeout_seconds: u64,
        workspace: &ResolvedWorkspace,
    ) -> crate::Result<LifecycleHookRun> {
        let mut run =
            Self::run_workspace_script_hook(ctx, index, command, timeout_seconds, workspace)
                .await?;
        run.log_path = Self::write_log_entry(ctx, index, &run.entry);
        Ok(run)
    }

    async fn run_script(
        ctx: &LifecycleHookContext,
        index: usize,
        command: &str,
        timeout_seconds: u64,
        workspace: Option<&ResolvedWorkspace>,
    ) -> crate::Result<LifecycleHookRun> {
        match workspace {
            Some(workspace) => {
                Self::run_workspace_script_hook(ctx, index, command, timeout_seconds, workspace)
                    .await
            }
            None => Ok(Self::run_script_hook(ctx, index, command, timeout_seconds).await),
        }
    }

    async fn run_workspace_script_hook(
        ctx: &LifecycleHookContext,
        index: usize,
        command: &str,
        timeout_seconds: u64,
        workspace: &ResolvedWorkspace,
    ) -> crate::Result<LifecycleHookRun> {
        let start = Instant::now();
        let working_dir = working_dir(ctx);
        let environment_preview = environment_preview(ctx);
        let mut env = ctx.env.clone();
        env.extend(environment_preview.clone());
        let result = workspace
            .backend
            .run(
                &workspace.placement,
                &RunSpec {
                    purpose: WorkspaceRunPurpose::Hook,
                    command: command.to_owned(),
                    env,
                    timeout_secs: if timeout_seconds == 0 {
                        30
                    } else {
                        timeout_seconds
                    },
                    // Hook output is collected before its historical 10 KiB prefix
                    // is retained; conformance commands have separate size budgets.
                    max_output_bytes: isize::MAX as usize,
                },
            )
            .await;
        let duration_ms = start.elapsed().as_millis() as u64;
        let returned_output = result.is_ok();
        let (exit_code, timed_out, stdout, stderr, error) = match result {
            Ok(result) => {
                let exit_code = (result.exit_code >= 0).then_some(result.exit_code);
                let error = match exit_code {
                    Some(0) => None,
                    Some(code) => Some(format!("exit code {code}")),
                    None => Some("terminated by signal".to_owned()),
                };
                (
                    exit_code,
                    false,
                    truncate_output(&executors::environment::redact_environment_values(
                        &result.stdout_tail,
                        &ctx.env,
                    )),
                    truncate_output(&executors::environment::redact_environment_values(
                        &result.stderr_tail,
                        &ctx.env,
                    )),
                    error,
                )
            }
            Err(WorkspaceBackendError::Other(error)) => {
                let error = match *error {
                    crate::ServiceError::InvalidOperation { message } => message,
                    error => return Err(error),
                };
                let timed_out = error == "review command timed out";
                let error = if timed_out {
                    "timeout".to_owned()
                } else {
                    error
                };
                (None, timed_out, String::new(), String::new(), Some(error))
            }
            Err(error) => return Err(error.into()),
        };
        let status = if error.is_none() { "success" } else { "failed" };
        let mut entry = json!({
            "event": event_name(&ctx.event),
            "hook_type": "script",
            "command": command,
            "duration_ms": duration_ms,
            "status": status,
            "working_dir": working_dir,
            "timeout": timed_out,
            "environment": environment_preview.clone(),
        });
        if let Some(error) = error.as_ref() {
            entry["error"] = json!(error);
            warn!(event = %event_name(&ctx.event), task_id = %ctx.task_id, hook_index = index,
                %command, duration_ms, %error, "lifecycle script hook failed");
        } else {
            info!(event = %event_name(&ctx.event), task_id = %ctx.task_id, hook_index = index,
                %command, duration_ms, "lifecycle script hook completed");
        }
        if returned_output {
            entry["exit_code"] = json!(exit_code);
            entry["stdout"] = json!(stdout);
            entry["stderr"] = json!(stderr);
        }
        Ok(LifecycleHookRun {
            index,
            entry,
            status: status.to_owned(),
            exit_code,
            timed_out,
            stdout,
            stderr,
            error,
            command: Some(command.to_owned()),
            working_dir,
            duration_ms,
            log_path: None,
            environment_preview,
        })
    }

    async fn run_script_hook(
        ctx: &LifecycleHookContext,
        index: usize,
        command: &str,
        timeout_seconds: u64,
    ) -> LifecycleHookRun {
        let start = Instant::now();
        let working_dir = working_dir(ctx);
        let timeout_seconds = if timeout_seconds == 0 {
            30
        } else {
            timeout_seconds
        };
        let environment_preview = environment_preview(ctx);

        let mut child_command = Command::new("bash");
        executors::run_process::apply(&mut child_command, &ctx.env);
        let child = match child_command
            .arg("-lc")
            .arg(command)
            .current_dir(&working_dir)
            .envs(&ctx.env)
            .env("FORGE_EVENT", event_name(&ctx.event))
            .env("FORGE_TASK_ID", &ctx.task_id)
            .env("FORGE_TASK_TITLE", &ctx.task_title)
            .env("FORGE_TASK_STATUS", &ctx.task_status)
            .env("FORGE_TASK_PREVIOUS_STATUS", &ctx.previous_status)
            .env("FORGE_PROJECT_ID", &ctx.project_id)
            .env("FORGE_PROJECT_NAME", &ctx.project_name)
            .env("FORGE_REPO_PATH", &ctx.repo_path)
            .env(
                "FORGE_WORKTREE_PATH",
                ctx.worktree_path.as_deref().unwrap_or_default(),
            )
            .env(
                "FORGE_AGENT_ID",
                ctx.agent_id.as_deref().unwrap_or_default(),
            )
            .env(
                "FORGE_EXECUTION_ID",
                ctx.execution_id.as_deref().unwrap_or_default(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(err) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let error = err.to_string();
                warn!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    %command,
                    %err,
                    "failed to start lifecycle script hook"
                );
                let entry = json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "script",
                    "command": command,
                    "duration_ms": duration_ms,
                    "status": "failed",
                    "error": error,
                    "working_dir": working_dir,
                    "timeout": false,
                    "environment": environment_preview.clone(),
                });
                return LifecycleHookRun {
                    index,
                    entry,
                    status: "failed".to_owned(),
                    exit_code: None,
                    timed_out: false,
                    stdout: String::new(),
                    stderr: String::new(),
                    error: Some(error),
                    command: Some(command.to_owned()),
                    working_dir,
                    duration_ms,
                    log_path: None,
                    environment_preview,
                };
            }
        };

        match timeout(
            Duration::from_secs(timeout_seconds),
            child.wait_with_output(),
        )
        .await
        {
            Ok(Ok(output)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let stdout = truncate_output(&executors::environment::redact_environment_values(
                    &String::from_utf8_lossy(&output.stdout),
                    &ctx.env,
                ));
                let stderr = truncate_output(&executors::environment::redact_environment_values(
                    &String::from_utf8_lossy(&output.stderr),
                    &ctx.env,
                ));
                let exit_code = output.status.code();
                let success = exit_code == Some(0);

                if success {
                    info!(
                        event = %event_name(&ctx.event),
                        task_id = %ctx.task_id,
                        hook_index = index,
                        %command,
                        duration_ms,
                        "lifecycle script hook completed"
                    );
                    let entry = json!({
                        "event": event_name(&ctx.event),
                        "hook_type": "script",
                        "command": command,
                        "duration_ms": duration_ms,
                        "status": "success",
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                        "working_dir": working_dir,
                        "timeout": false,
                        "environment": environment_preview.clone(),
                    });
                    LifecycleHookRun {
                        index,
                        entry,
                        status: "success".to_owned(),
                        exit_code,
                        timed_out: false,
                        stdout,
                        stderr,
                        error: None,
                        command: Some(command.to_owned()),
                        working_dir,
                        duration_ms,
                        log_path: None,
                        environment_preview,
                    }
                } else {
                    let error_message = match output.status.code() {
                        Some(code) => format!("exit code {code}"),
                        None => "terminated by signal".to_owned(),
                    };
                    warn!(
                        event = %event_name(&ctx.event),
                        task_id = %ctx.task_id,
                        hook_index = index,
                        %command,
                        duration_ms,
                        error = %error_message,
                        "lifecycle script hook failed"
                    );
                    let entry = json!({
                        "event": event_name(&ctx.event),
                        "hook_type": "script",
                        "command": command,
                        "duration_ms": duration_ms,
                        "status": "failed",
                        "error": error_message,
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                        "working_dir": working_dir,
                        "timeout": false,
                        "environment": environment_preview.clone(),
                    });
                    LifecycleHookRun {
                        index,
                        entry,
                        status: "failed".to_owned(),
                        exit_code,
                        timed_out: false,
                        stdout,
                        stderr,
                        error: Some(error_message),
                        command: Some(command.to_owned()),
                        working_dir,
                        duration_ms,
                        log_path: None,
                        environment_preview,
                    }
                }
            }
            Ok(Err(err)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let error = err.to_string();
                warn!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    %command,
                    duration_ms,
                    %err,
                    "lifecycle script hook failed to collect output"
                );
                let entry = json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "script",
                    "command": command,
                    "duration_ms": duration_ms,
                    "status": "failed",
                    "error": error,
                    "working_dir": working_dir,
                    "timeout": false,
                    "environment": environment_preview.clone(),
                });
                LifecycleHookRun {
                    index,
                    entry,
                    status: "failed".to_owned(),
                    exit_code: None,
                    timed_out: false,
                    stdout: String::new(),
                    stderr: String::new(),
                    error: Some(error),
                    command: Some(command.to_owned()),
                    working_dir,
                    duration_ms,
                    log_path: None,
                    environment_preview,
                }
            }
            Err(_) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                warn!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    %command,
                    duration_ms,
                    timeout_seconds,
                    "lifecycle script hook timed out"
                );
                let entry = json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "script",
                    "command": command,
                    "duration_ms": duration_ms,
                    "status": "failed",
                    "error": "timeout",
                    "working_dir": working_dir,
                    "timeout": true,
                    "environment": environment_preview.clone(),
                });
                LifecycleHookRun {
                    index,
                    entry,
                    status: "failed".to_owned(),
                    exit_code: None,
                    timed_out: true,
                    stdout: String::new(),
                    stderr: String::new(),
                    error: Some("timeout".to_owned()),
                    command: Some(command.to_owned()),
                    working_dir,
                    duration_ms,
                    log_path: None,
                    environment_preview,
                }
            }
        }
    }

    async fn run_plugin_hook(
        ctx: &LifecycleHookContext,
        index: usize,
        name: &str,
        plugin_registry: &PluginRegistry,
        workspace: Option<&ResolvedWorkspace>,
    ) -> Value {
        let start = Instant::now();
        let Some(plugin) = plugin_registry.get(name) else {
            let duration_ms = start.elapsed().as_millis() as u64;
            warn!(
                event = %event_name(&ctx.event),
                task_id = %ctx.task_id,
                hook_index = index,
                plugin_name = %name,
                "lifecycle plugin hook not found"
            );
            return json!({
                "event": event_name(&ctx.event),
                "hook_type": "plugin",
                "plugin_name": name,
                "duration_ms": duration_ms,
                "status": "failed",
                "error": format!("plugin '{name}' not found"),
            });
        };

        if !plugin.supported_events().contains(&ctx.event) {
            let duration_ms = start.elapsed().as_millis() as u64;
            warn!(
                event = %event_name(&ctx.event),
                task_id = %ctx.task_id,
                hook_index = index,
                plugin_name = %name,
                "lifecycle plugin hook does not support event"
            );
            return json!({
                "event": event_name(&ctx.event),
                "hook_type": "plugin",
                "plugin_name": name,
                "duration_ms": duration_ms,
                "status": "skipped",
                "reason": "unsupported_event",
            });
        }

        let result = match workspace {
            Some(workspace) => plugin.execute_in_workspace(ctx, workspace).await,
            None => plugin.execute(ctx).await,
        };
        match result {
            Ok(PluginResult::Success) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                info!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    plugin_name = %name,
                    duration_ms,
                    "lifecycle plugin hook completed"
                );
                json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "plugin",
                    "plugin_name": name,
                    "duration_ms": duration_ms,
                    "status": "success",
                })
            }
            Ok(PluginResult::Skipped { reason }) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                warn!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    plugin_name = %name,
                    %reason,
                    "lifecycle plugin hook skipped"
                );
                json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "plugin",
                    "plugin_name": name,
                    "duration_ms": duration_ms,
                    "status": "skipped",
                    "reason": reason,
                })
            }
            Err(err) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                warn!(
                    event = %event_name(&ctx.event),
                    task_id = %ctx.task_id,
                    hook_index = index,
                    plugin_name = %name,
                    error = %err.message,
                    "lifecycle plugin hook failed"
                );
                json!({
                    "event": event_name(&ctx.event),
                    "hook_type": "plugin",
                    "plugin_name": name,
                    "duration_ms": duration_ms,
                    "status": "failed",
                    "error": err.message,
                })
            }
        }
    }

    pub(crate) fn write_log_entry(
        ctx: &LifecycleHookContext,
        index: usize,
        entry: &Value,
    ) -> Option<String> {
        let log_dir = ctx.log_dir.as_ref()?;

        if let Err(err) = std::fs::create_dir_all(log_dir) {
            error!(
                event = %event_name(&ctx.event),
                task_id = %ctx.task_id,
                hook_index = index,
                path = %log_dir.display(),
                %err,
                "failed to create lifecycle hook log directory"
            );
            return None;
        }

        let file_name = format!(
            "hook-{}-{}-{}.jsonl",
            event_name(&ctx.event),
            index,
            Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
        );
        let path = log_dir.join(file_name);

        let serialized = match serde_json::to_string(entry) {
            Ok(serialized) => serialized,
            Err(err) => {
                error!(
                        event = %event_name(&ctx.event),
                        task_id = %ctx.task_id,
                        hook_index = index,
                        %err,
                    "failed to serialize lifecycle hook log entry"
                );
                return None;
            }
        };

        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => file,
            Err(err) => {
                error!(
                        event = %event_name(&ctx.event),
                        task_id = %ctx.task_id,
                        hook_index = index,
                        path = %path.display(),
                        %err,
                    "failed to open lifecycle hook log file"
                );
                return None;
            }
        };

        if let Err(err) = writeln!(file, "{serialized}") {
            error!(
                event = %event_name(&ctx.event),
                task_id = %ctx.task_id,
                hook_index = index,
                path = %path.display(),
                %err,
                "failed to write lifecycle hook log entry"
            );
            return None;
        }

        Some(path.display().to_string())
    }
}

fn environment_preview(ctx: &LifecycleHookContext) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("FORGE_EVENT".to_owned(), event_name(&ctx.event).to_owned());
    env.insert("FORGE_TASK_ID".to_owned(), ctx.task_id.clone());
    env.insert("FORGE_TASK_TITLE".to_owned(), ctx.task_title.clone());
    env.insert("FORGE_TASK_STATUS".to_owned(), ctx.task_status.clone());
    env.insert(
        "FORGE_TASK_PREVIOUS_STATUS".to_owned(),
        ctx.previous_status.clone(),
    );
    env.insert("FORGE_PROJECT_ID".to_owned(), ctx.project_id.clone());
    env.insert("FORGE_PROJECT_NAME".to_owned(), ctx.project_name.clone());
    env.insert("FORGE_REPO_PATH".to_owned(), ctx.repo_path.clone());
    env.insert(
        "FORGE_WORKTREE_PATH".to_owned(),
        ctx.worktree_path.clone().unwrap_or_default(),
    );
    env.insert(
        "FORGE_AGENT_ID".to_owned(),
        ctx.agent_id.clone().unwrap_or_default(),
    );
    env.insert(
        "FORGE_EXECUTION_ID".to_owned(),
        ctx.execution_id.clone().unwrap_or_default(),
    );
    env
}

fn truncate_output(output: &str) -> String {
    const LIMIT: usize = 10 * 1024;

    if output.len() <= LIMIT {
        return output.to_owned();
    }

    let mut end = LIMIT;
    while !output.is_char_boundary(end) {
        end -= 1;
    }
    output[..end].to_owned()
}

fn working_dir(ctx: &LifecycleHookContext) -> String {
    if let Some(worktree_path) = ctx.worktree_path.as_deref() {
        return worktree_path.to_owned();
    }
    if !ctx.repo_path.is_empty() {
        return ctx.repo_path.clone();
    }
    ".".to_owned()
}

fn event_name(event: &api_types::LifecycleEvent) -> &'static str {
    match event {
        api_types::LifecycleEvent::BeforeWork => "before_work",
        api_types::LifecycleEvent::OnWorkStart => "on_work_start",
        api_types::LifecycleEvent::OnWorkStop => "on_work_stop",
        api_types::LifecycleEvent::OnTaskDone => "on_task_done",
        api_types::LifecycleEvent::OnTaskCancel => "on_task_cancel",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(worktree_path: &std::path::Path) -> LifecycleHookContext {
        LifecycleHookContext {
            env: std::collections::BTreeMap::new(),
            event: api_types::LifecycleEvent::BeforeWork,
            task_id: "task-1".to_owned(),
            task_title: "Test task".to_owned(),
            task_status: "in_progress".to_owned(),
            previous_status: "todo".to_owned(),
            project_id: "project-1".to_owned(),
            project_name: "Project".to_owned(),
            repo_path: worktree_path.display().to_string(),
            worktree_path: Some(worktree_path.display().to_string()),
            agent_id: Some("agent-1".to_owned()),
            execution_id: None,
            log_dir: Some(worktree_path.join(".forge-test-hook-logs")),
        }
    }

    #[tokio::test]
    async fn blocking_before_work_requires_exit_zero_for_success() {
        let temp = tempfile::tempdir().expect("temp dir");
        let hooks = vec![api_types::LifecycleHookDef::Script {
            command: "echo ok; exit 42".to_owned(),
            timeout_seconds: 5,
            blocking: true,
        }];

        let failure =
            LifecycleHookRunner::run_blocking_before_work_hooks(ctx(temp.path()), &hooks).await;

        assert!(failure.is_some());
    }

    #[tokio::test]
    async fn blocking_before_work_returns_failed_script_result() {
        let temp = tempfile::tempdir().expect("temp dir");
        let hooks = vec![api_types::LifecycleHookDef::Script {
            command: "echo out; echo err >&2; exit 7".to_owned(),
            timeout_seconds: 5,
            blocking: true,
        }];

        let failure = LifecycleHookRunner::run_blocking_before_work_hooks(ctx(temp.path()), &hooks)
            .await
            .expect("blocking hook fails");

        assert_eq!(failure.exit_code, Some(7));
        assert_eq!(failure.status, "failed");
        assert!(failure.stdout.contains("out"));
        assert!(failure.stderr.contains("err"));
        assert!(failure.log_path.is_some());
    }

    #[tokio::test]
    async fn async_before_work_skips_blocking_script_hooks() {
        let temp = tempfile::tempdir().expect("temp dir");
        let marker = temp.path().join("async-blocking-hook-ran");
        let hooks = vec![api_types::LifecycleHookDef::Script {
            command: format!("touch {}", marker.display()),
            timeout_seconds: 5,
            blocking: true,
        }];

        LifecycleHookRunner::run_hooks(ctx(temp.path()), &hooks, Arc::new(PluginRegistry::new()))
            .await;

        assert!(
            !marker.exists(),
            "blocking before_work scripts are owned by the before_enter barrier"
        );
    }

    #[tokio::test]
    async fn test_script_hook_returns_stdout_stderr_and_exit_zero() {
        let temp = tempfile::tempdir().expect("temp dir");
        let run = LifecycleHookRunner::test_script_hook(
            &ctx(temp.path()),
            0,
            "echo test-out; echo test-err >&2; exit 0",
            5,
        )
        .await;

        assert_eq!(run.status, "success");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(run.stdout, "test-out\n");
        assert_eq!(run.stderr, "test-err\n");
        assert!(!run.timed_out);
        assert!(!run.working_dir.is_empty());
        assert!(!run.environment_preview.is_empty());
        let log_path = run.log_path.as_deref().expect("log path");
        assert!(std::path::Path::new(log_path).exists());
    }

    #[tokio::test]
    async fn script_hook_output_redacts_project_environment_values_before_truncating() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut context = ctx(temp.path());
        context
            .env
            .insert("ACCESS_TOKEN".to_owned(), "very-secret".to_owned());
        let run = LifecycleHookRunner::test_script_hook(
            &context,
            0,
            "printf '%010235d%s' 0 \"$ACCESS_TOKEN\"",
            5,
        )
        .await;

        assert_eq!(run.status, "success");
        assert!(!run.stdout.contains("very-"), "{}", run.stdout);
        assert!(run.stdout.contains("[REDA"), "{}", run.stdout);
    }

    #[tokio::test]
    async fn test_script_hook_returns_non_zero_exit_code_for_failure() {
        let temp = tempfile::tempdir().expect("temp dir");
        let run = LifecycleHookRunner::test_script_hook(
            &ctx(temp.path()),
            0,
            "echo fail-out; echo fail-err >&2; exit 19",
            5,
        )
        .await;

        assert_eq!(run.status, "failed");
        assert_eq!(run.exit_code, Some(19));
        assert_eq!(run.stdout, "fail-out\n");
        assert_eq!(run.stderr, "fail-err\n");
        assert!(!run.timed_out);
        let log_path = run.log_path.as_deref().expect("log path");
        assert!(std::path::Path::new(log_path).exists());
    }
}
