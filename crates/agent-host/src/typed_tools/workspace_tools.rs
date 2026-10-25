//! Bounded workspace file, directory, command, and validation tools.

use super::*;

const MAX_FILE_READ_BYTES: usize = 128 * 1024;

const MAX_FILE_WRITE_BYTES: usize = 1024 * 1024;

const MAX_COMMAND_OUTPUT_BYTES: usize = 128 * 1024;

/// A Task command that runs longer is stopped, so a watch mode, a server, or
/// a hung test suite cannot hold the execution until its hard deadline.
const TASK_COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

fn string_array(arguments: &Value, field: &str) -> Result<Vec<String>, RuntimeError> {
    let Some(value) = arguments.get(field) else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .ok_or_else(|| RuntimeError::tool(format!("{field} must be an array")))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| RuntimeError::tool(format!("{field} must contain strings")))
        })
        .collect()
}

fn directory_glob(arguments: &Value) -> Result<Option<&str>, RuntimeError> {
    let Some(value) = arguments.get("glob") else {
        return Ok(None);
    };
    let glob = value
        .as_str()
        .ok_or_else(|| RuntimeError::tool("Task Workspace glob must be a string"))?;
    if glob.is_empty() || glob.chars().count() > MAX_DIRECTORY_GLOB_CHARS {
        return Err(RuntimeError::tool(format!(
            "Task Workspace glob must contain 1 to {MAX_DIRECTORY_GLOB_CHARS} characters"
        )));
    }
    if glob.contains('/') || glob.contains('\\') {
        return Err(RuntimeError::tool(
            "Task Workspace glob filters direct entry names and cannot contain path separators",
        ));
    }
    Ok(Some(glob))
}

fn directory_entries(
    workspace_root: &str,
    directory: &str,
    glob: Option<&str>,
) -> Result<(Vec<Value>, bool), RuntimeError> {
    let read_dir = std::fs::read_dir(directory).map_err(|error| match error.kind() {
        ErrorKind::NotFound => RuntimeError::not_found(format!(
            "Task Workspace directory not found: {}",
            workspace_relative_path(workspace_root, directory)
                .unwrap_or_else(|_| directory.to_owned())
        )),
        _ => RuntimeError::tool(format!(
            "Task Workspace directory could not be listed ({:?})",
            error.kind()
        )),
    })?;
    let mut entries = BTreeMap::new();
    let mut truncated = false;
    for entry in read_dir {
        let entry = entry.map_err(|error| {
            RuntimeError::tool(format!(
                "Task Workspace directory entry could not be read ({:?})",
                error.kind()
            ))
        })?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| RuntimeError::workspace("Task Workspace entry is not valid UTF-8"))?;
        if glob.is_some_and(|pattern| !glob_matches(pattern, &name)) {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            RuntimeError::tool(format!(
                "Task Workspace entry type could not be read ({:?})",
                error.kind()
            ))
        })?;
        let kind = if file_type.is_dir() {
            "directory"
        } else if file_type.is_file() {
            "file"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "other"
        };
        let entry_path = entry.path();
        let entry_path = entry_path
            .to_str()
            .ok_or_else(|| RuntimeError::workspace("Task Workspace entry is not valid UTF-8"))?;
        entries.insert(
            name.clone(),
            json!({
                "name": name,
                "path": workspace_relative_path(workspace_root, entry_path)?,
                "kind": kind,
            }),
        );
        if entries.len() > MAX_DIRECTORY_ENTRIES {
            entries.pop_last();
            truncated = true;
        }
    }
    Ok((entries.into_values().collect(), truncated))
}

fn bounded_workspace_path(workspace: &dyn Workspace, raw: &str) -> Result<String, RuntimeError> {
    if raw.trim().is_empty() {
        return Err(RuntimeError::workspace(
            "Task Workspace path cannot be empty",
        ));
    }
    let raw_path = Path::new(raw);
    if raw_path
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(RuntimeError::workspace(
            "Task Workspace path contains a forbidden traversal component",
        ));
    }
    let root = Path::new(workspace.root());
    let candidate = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        root.join(raw_path)
    };
    let candidate = candidate
        .to_str()
        .ok_or_else(|| RuntimeError::workspace("Task Workspace path is not valid UTF-8"))?;
    if !workspace.contains(candidate) {
        return Err(RuntimeError::workspace(
            "Task Workspace path resolves outside its root",
        ));
    }
    workspace.resolve(candidate)
}

fn filesystem_resource(root: &str, path: &str) -> Result<SecurityResource, RuntimeError> {
    let root_path = Path::new(root);
    let path = Path::new(path);
    let relative = path
        .strip_prefix(root_path)
        .map_err(|_| RuntimeError::workspace("Task Workspace path is outside its root"))?;
    let segments = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str().map(str::to_owned),
            Component::CurDir => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(RuntimeError::workspace(
            "Task Workspace resource contains an invalid component",
        ));
    }
    Ok(SecurityResource::filesystem(root.to_owned(), segments))
}

fn validate_command_args(args: &[String]) -> Result<(), RuntimeError> {
    if args.len() > 128 {
        return Err(RuntimeError::tool("Task command has too many arguments"));
    }
    for arg in args {
        let path = Path::new(arg);
        if path.is_absolute()
            || path
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return Err(RuntimeError::tool(
                "Task command arguments cannot escape the admitted workspace",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct TaskReadTool;

#[async_trait]
impl Tool for TaskReadTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "forge_task_read",
            "Read a UTF-8 file inside the admitted Task Workspace.",
            json!({
                "type":"object",
                "required":["path"],
                "properties":{"path":{"type":"string","minLength":1}},
                "additionalProperties":false
            }),
            ToolEffects::read_only(),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let path =
            bounded_workspace_path(ctx.workspace.as_ref(), required_string(&arguments, "path")?)?;
        let resource = filesystem_resource(ctx.workspace.root(), &path)?;
        let arguments = json!({"path": path});
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "forge_task_read",
            arguments,
            PermissionSet::single(Permission::FsRead),
            resource,
            ToolEffects::read_only(),
            ToolCallDisplay::new("Read Task Workspace file"),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let path = required_string(prepared.arguments(), "path")?;
        let path = bounded_workspace_path(ctx.workspace.as_ref(), path)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(tool_error_outcome(json!({
                    "code": "not_found",
                    "message": "Task Workspace path was not found",
                    "path": workspace_relative_path(ctx.workspace.root(), &path)?,
                })));
            }
            Err(_) if Path::new(&path).is_dir() => {
                let (entries, truncated) = directory_entries(ctx.workspace.root(), &path, None)?;
                return Ok(tool_error_outcome(json!({
                    "code": "is_directory",
                    "message": "Task Workspace path is a directory; use forge_task_list to enumerate it",
                    "path": workspace_relative_path(ctx.workspace.root(), &path)?,
                    "entries": entries,
                    "truncated": truncated,
                })));
            }
            Err(error) => {
                return Err(RuntimeError::tool(format!(
                    "Task Workspace file could not be read ({:?})",
                    error.kind()
                )));
            }
        };
        let truncated = bytes.len() > MAX_FILE_READ_BYTES;
        let bounded = bytes
            .into_iter()
            .take(MAX_FILE_READ_BYTES)
            .collect::<Vec<_>>();
        let text = String::from_utf8_lossy(&bounded).into_owned();
        Ok(ToolOutcome::json(json!({
            "path": path,
            "content": text,
            "truncated": truncated
        })))
    }
}

#[derive(Debug)]
pub(super) struct TaskListTool;

#[async_trait]
impl Tool for TaskListTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            FORGE_TASK_LIST_TOOL,
            "List direct children of a directory inside the admitted Task Workspace. An optional glob filters entry names with * and ? wildcards.",
            json!({
                "type":"object",
                "required":["path"],
                "properties":{
                    "path":{"type":"string","minLength":1},
                    "glob":{"type":"string","minLength":1,"maxLength":MAX_DIRECTORY_GLOB_CHARS}
                },
                "additionalProperties":false
            }),
            ToolEffects::read_only(),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let path =
            bounded_workspace_path(ctx.workspace.as_ref(), required_string(&arguments, "path")?)?;
        let glob = directory_glob(&arguments)?;
        let resource = filesystem_resource(ctx.workspace.root(), &path)?;
        let mut prepared_arguments = json!({"path": path});
        if let Some(glob) = glob {
            prepared_arguments["glob"] = Value::String(glob.to_owned());
        }
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            FORGE_TASK_LIST_TOOL,
            prepared_arguments,
            PermissionSet::single(Permission::FsRead),
            resource,
            ToolEffects::read_only(),
            ToolCallDisplay::new("List Task Workspace directory"),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let path = required_string(prepared.arguments(), "path")?;
        let path = bounded_workspace_path(ctx.workspace.as_ref(), path)?;
        let glob = directory_glob(prepared.arguments())?;
        let (entries, truncated) = match directory_entries(ctx.workspace.root(), &path, glob) {
            Ok(listing) => listing,
            Err(error) if error.kind == agent_runtime::core::error::ErrorKind::NotFound => {
                return Ok(tool_error_outcome(json!({
                    "code": "not_found",
                    "message": "Task Workspace directory was not found",
                    "path": workspace_relative_path(ctx.workspace.root(), &path)?,
                })));
            }
            Err(_error) if !Path::new(&path).is_dir() => {
                return Ok(tool_error_outcome(json!({
                    "code": "not_a_directory",
                    "message": "Task Workspace list path must be a directory",
                    "path": workspace_relative_path(ctx.workspace.root(), &path)?,
                })));
            }
            Err(error) => return Err(error),
        };
        Ok(ToolOutcome::json(json!({
            "path": workspace_relative_path(ctx.workspace.root(), &path)?,
            "glob": glob,
            "entries": entries,
            "truncated": truncated,
            "limit": MAX_DIRECTORY_ENTRIES,
        })))
    }
}

#[derive(Debug)]
pub(super) struct TaskWriteTool;

#[async_trait]
impl Tool for TaskWriteTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "forge_task_write",
            "Write UTF-8 content inside the admitted Task Workspace.",
            json!({
                "type":"object",
                "required":["path","content"],
                "properties":{
                    "path":{"type":"string","minLength":1},
                    "content":{"type":"string","maxLength":MAX_FILE_WRITE_BYTES}
                },
                "additionalProperties":false
            }),
            // The concrete write path is derived during `prepare`; this
            // static scope supplies the typed FsWrite upper bound without
            // granting a literal path outside the host workspace.
            ToolEffects::new(Vec::new()).with_write("<task-workspace>"),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let path =
            bounded_workspace_path(ctx.workspace.as_ref(), required_string(&arguments, "path")?)?;
        let content = required_string(&arguments, "content")?;
        if content.len() > MAX_FILE_WRITE_BYTES {
            return Err(RuntimeError::tool("Task Workspace write is too large"));
        }
        let resource = filesystem_resource(ctx.workspace.root(), &path)?;
        let arguments = json!({"path": path, "content": content});
        let effects = ToolEffects::new(Vec::new()).with_write(path.clone());
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "forge_task_write",
            arguments,
            PermissionSet::single(Permission::FsWrite),
            resource,
            effects,
            ToolCallDisplay::new("Write Task Workspace file"),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let path = required_string(prepared.arguments(), "path")?;
        let content = required_string(prepared.arguments(), "content")?;
        let path = bounded_workspace_path(ctx.workspace.as_ref(), path)?;
        if let Some(parent) = Path::new(&path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| RuntimeError::tool(error.to_string()))?;
        }
        let path = bounded_workspace_path(ctx.workspace.as_ref(), &path)?;
        std::fs::write(&path, content).map_err(|error| RuntimeError::tool(error.to_string()))?;
        Ok(ToolOutcome::json(json!({"path": path, "written": true})))
    }
}

/// The subdirectory of a Project Agent verification workspace that holds the
/// repository checkout. The workspace root itself is the *parent* of the
/// checkout (durable `forge/` docs beside a disposable `checkout/`), so the
/// checkout is where verification commands must run — see `TaskCommandTool`.
pub const PROJECT_VERIFICATION_CHECKOUT_DIR: &str = "checkout";

/// Where a verification command's observation is recorded: the provider that
/// persists it for the Project this session is bound to.
#[derive(Debug)]
pub(super) struct CommandObserver {
    pub(super) actor_identity_id: String,
    pub(super) scope: CanonicalScope,
    pub(super) provider: Arc<dyn ForgeToolProvider>,
}

#[derive(Debug)]
pub(super) struct TaskCommandTool {
    pub(super) environment: std::collections::BTreeMap<String, String>,
    /// Programs this composition may spawn. Resolved by the host from owner
    /// configuration and the owning Project, never from model input.
    pub(super) allowlist: Arc<CommandAllowlist>,
    /// Present only for a Project Agent verification session: every command
    /// it runs is recorded as an observation the Agent cites in
    /// `project.validation`. A Task worker's commands are its own business.
    pub(super) observer: Option<CommandObserver>,
    /// Directory the command runs in, relative to the workspace root.
    ///
    /// `None` runs at the root: a Task worktree is the repository. A Project
    /// Agent verification workspace is the parent of its checkout, and build
    /// tools walk *up* from the current directory for a manifest, so a
    /// `cargo test` at that parent would run whatever repository the data
    /// directory happens to sit in — for a `--data-dir ./test` server, Forge
    /// itself. Verification commands therefore run inside the checkout.
    pub(super) command_dir: Option<&'static str>,
}

#[async_trait]
impl Tool for TaskCommandTool {
    fn spec(&self) -> ToolSpec {
        let description = if self.command_dir.is_some() {
            "Run one allowlisted command inside the Project's repository checkout, which is \
             its current directory."
        } else {
            "Run one allowlisted command with the Task Workspace as its current directory."
        };
        ToolSpec::new(
            "forge_task_command",
            description,
            json!({
                "type":"object",
                "required":["program"],
                "properties":{
                    "program":{"type":"string","minLength":1},
                    "args":{"type":"array","items":{"type":"string"},"maxItems":128}
                },
                "additionalProperties":false
            }),
            ToolEffects::new(Vec::new()).with_spawn(),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        let program = required_string(&arguments, "program")?;
        validate_command_program(&self.allowlist, program)?;
        let args = string_array(&arguments, "args")?;
        validate_command_args(&args)?;
        if ctx.workspace.root() == "<none>" {
            return Err(RuntimeError::workspace("Task command requires a workspace"));
        }
        if let Some(command_dir) = self.command_dir {
            if !Path::new(ctx.workspace.root()).join(command_dir).is_dir() {
                return Err(RuntimeError::workspace(
                    "this Project has no repository checkout to run commands in",
                ));
            }
        }
        let arguments = json!({"program": program, "args": args});
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "forge_task_command",
            arguments,
            PermissionSet::single(Permission::ProcessSpawn),
            SecurityResource::other("process", "forge_task_command"),
            ToolEffects::new(Vec::new()).with_spawn(),
            ToolCallDisplay::new("Run Task Workspace command"),
        ))
    }

    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let program = required_string(prepared.arguments(), "program")?;
        let args = string_array(prepared.arguments(), "args")?;
        let run =
            execute_workspace_command(program, &args, self.command_dir, &self.environment, ctx)
                .await?;
        let Some(observer) = &self.observer else {
            return Ok(command_outcome(&run, None));
        };
        // A verification command that ran but left no observation would let
        // the Agent settle a check on nothing; the run is read-and-run, so
        // failing the call is the safe way to surface a recording fault.
        let recorded = observer
            .provider
            .observe_command(
                &observer.actor_identity_id,
                &observer.scope,
                CommandObservation {
                    session_id: ctx.session.to_string(),
                    turn_id: ctx.turn.as_ref().map(ToString::to_string),
                    program: run.program.clone(),
                    args: run.args.clone(),
                    exit_code: run.status_code,
                    success: run.success,
                    output_digest: run.output_digest(),
                    stdout_excerpt: bounded_text(&run.stdout, MAX_OBSERVATION_EXCERPT_BYTES).0,
                    stderr_excerpt: bounded_text(&run.stderr, MAX_OBSERVATION_EXCERPT_BYTES).0,
                },
            )
            .await
            .map_err(|error| match error {
                AgentHostError::StructuredOutcome(outcome) => RuntimeError::tool(format!(
                    "the command ran but Forge could not record the observation: {}",
                    outcome.safe_message
                )),
                other => host_error_to_runtime(other),
            })?;
        let observation_id = recorded
            .get("observation_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RuntimeError::tool("Forge recorded the observation without an observation id")
            })?;
        Ok(command_outcome(&run, Some(observation_id)))
    }
}

#[derive(Debug)]
pub(super) struct TaskValidateTool {
    pub(super) environment: std::collections::BTreeMap<String, String>,
}

#[async_trait]
impl Tool for TaskValidateTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec::new(
            "forge_task_validate",
            "Run the fixed read-only git whitespace validation for the Task Workspace.",
            json!({"type":"object","additionalProperties":false}),
            ToolEffects::new(Vec::new()).with_spawn(),
        )
    }

    async fn prepare(
        &self,
        arguments: Value,
        ctx: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        if !arguments.is_object() {
            return Err(RuntimeError::tool("validation arguments must be an object"));
        }
        if ctx.workspace.root() == "<none>" {
            return Err(RuntimeError::workspace(
                "Task validation requires a workspace",
            ));
        }
        Ok(PreparedToolCall::new(
            ctx.call_id.clone(),
            "forge_task_validate",
            json!({}),
            PermissionSet::single(Permission::ProcessSpawn),
            SecurityResource::other("process", "forge_task_validate"),
            ToolEffects::new(Vec::new()).with_spawn(),
            ToolCallDisplay::new("Validate Task Workspace"),
        ))
    }

    async fn invoke(
        &self,
        _prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        let run = execute_workspace_command(
            "git",
            &["diff".to_owned(), "--check".to_owned()],
            None,
            &self.environment,
            ctx,
        )
        .await?;
        Ok(command_outcome(&run, None))
    }
}

/// Bytes of stdout/stderr kept verbatim in a recorded observation.
const MAX_OBSERVATION_EXCERPT_BYTES: usize = 4_000;

/// A workspace command after it ran: the exact bytes it produced, kept whole
/// until the outcome is rendered so an observation digest covers all of it.
pub(super) struct ExecutedCommand {
    program: String,
    args: Vec<String>,
    status_code: Option<i32>,
    pub(super) success: bool,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
}

impl ExecutedCommand {
    fn output_digest(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&self.stdout);
        hasher.update(b"\n--stderr--\n");
        hasher.update(&self.stderr);
        hex::encode(hasher.finalize())
    }
}

pub(super) async fn execute_workspace_command(
    program: &str,
    args: &[String],
    command_dir: Option<&str>,
    environment: &std::collections::BTreeMap<String, String>,
    ctx: &InvocationContext,
) -> Result<ExecutedCommand, RuntimeError> {
    executors::server_commands::ensure_allowed()
        .map_err(|error| RuntimeError::tool(error.to_string()))?;
    if ctx.should_stop() {
        return Err(RuntimeError::cancelled("Task command cancelled"));
    }
    let root = Path::new(ctx.workspace.root());
    let current_dir = match command_dir {
        Some(command_dir) => root.join(command_dir),
        None => root.to_path_buf(),
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&current_dir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default());
    executors::sandbox::start_compiler_cache(root).await;
    // One temp directory per command, inside the Task root, removed when
    // the command returns. The environment is cleared above, so without it a
    // child falls back to the shared platform temp directory.
    let run_scope =
        executors::sandbox::SandboxEnv::for_command(root, executors::sandbox::RunPurpose::Command);
    // Cleared above: the server's own `CARGO_TARGET_DIR` or compiler wrapper
    // is not inherited, so it must not hold back the Task root's.
    executors::run_process::apply_sandboxed_cleared(&mut command, environment, run_scope.env());
    let output = run_bounded_command(command, TASK_COMMAND_TIMEOUT)
        .await
        .map_err(|error| RuntimeError::tool(format!("Task command failed: {error}")))?;
    Ok(ExecutedCommand {
        program: program.to_owned(),
        args: args.to_vec(),
        status_code: output.status_code,
        success: output.success,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

struct BoundedOutput {
    status_code: Option<i32>,
    success: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Run `command` in its own process group and never leave it behind.
///
/// The group is killed when the command exits (reaping anything it
/// backgrounded), when it outlives `timeout`, and when this future is dropped
/// because the turn was cancelled or hit its deadline. Before this, a command
/// that never exited held the execution until its hard deadline and then
/// survived it: one Project leaked a hung test runner's whole worker tree on
/// every attempt. Exit is judged by the command's own status, not by its pipes
/// closing, so a backgrounded process holding stdout cannot stall the result.
/// A timeout is reported as a failed run with the reason appended to stderr,
/// so the agent can change course instead of losing the execution.
async fn run_bounded_command(
    mut command: Command,
    timeout: std::time::Duration,
) -> std::io::Result<BoundedOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let group = ProcessGroupGuard::new(child.id());
    let stdout = child
        .stdout
        .take()
        .map(|pipe| tokio::spawn(read_capped(pipe)));
    let stderr = child
        .stderr
        .take()
        .map(|pipe| tokio::spawn(read_capped(pipe)));
    let status = tokio::time::timeout(timeout, child.wait()).await;
    // Killing the group closes every pipe a leftover process still holds.
    drop(group);
    let collect = |reader: Option<tokio::task::JoinHandle<Vec<u8>>>| async move {
        match reader {
            Some(reader) => reader.await.unwrap_or_default(),
            None => Vec::new(),
        }
    };
    let stdout = collect(stdout).await;
    let mut stderr = collect(stderr).await;
    match status {
        Ok(status) => {
            let status = status?;
            Ok(BoundedOutput {
                status_code: status.code(),
                success: status.success(),
                stdout,
                stderr,
            })
        }
        Err(_) => {
            // Lead with the reason: the tail of a full stderr is what gets cut.
            let mut reason = format!(
                "Forge stopped this command after {} seconds. Commands must exit on their \
                 own: run tests once instead of in watch mode, and do not start servers or \
                 other long-running processes in the foreground.\n",
                timeout.as_secs()
            )
            .into_bytes();
            reason.append(&mut stderr);
            let stderr = reason;
            Ok(BoundedOutput {
                status_code: None,
                success: false,
                stdout,
                stderr,
            })
        }
    }
}

/// Drain `pipe` to its end, keeping only what a tool result can show. The
/// rest is read and discarded so a chatty command never blocks on a full pipe.
async fn read_capped(mut pipe: impl tokio::io::AsyncRead + Unpin) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let cap = MAX_COMMAND_OUTPUT_BYTES + 1;
    let mut kept = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                let room = cap.saturating_sub(kept.len());
                kept.extend_from_slice(&chunk[..read.min(room)]);
            }
        }
    }
    kept
}

/// Kills a spawned command's whole process group when dropped.
struct ProcessGroupGuard {
    #[cfg(unix)]
    pgid: Option<i32>,
}

impl ProcessGroupGuard {
    fn new(pid: Option<u32>) -> Self {
        #[cfg(unix)]
        {
            Self {
                pgid: pid.and_then(|pid| i32::try_from(pid).ok()),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = pid;
            Self {}
        }
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            // The group was created for this command by `process_group(0)`,
            // so its id is the child's pid and names no unrelated group. An
            // already-empty group is not an error worth reporting.
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pgid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

fn command_outcome(run: &ExecutedCommand, observation_id: Option<&str>) -> ToolOutcome {
    let stdout = bounded_text(&run.stdout, MAX_COMMAND_OUTPUT_BYTES);
    let stderr = bounded_text(&run.stderr, MAX_COMMAND_OUTPUT_BYTES);
    let mut value = json!({
        "program": run.program,
        "args": run.args,
        "status": run.status_code,
        "success": run.success,
        "stdout": stdout.0,
        "stderr": stderr.0,
        "truncated": stdout.1 || stderr.1
    });
    if let Some(observation_id) = observation_id {
        value["observation_id"] = json!(observation_id);
    }
    ToolOutcome::json(value)
}

fn bounded_text(bytes: &[u8], limit: usize) -> (String, bool) {
    let truncated = bytes.len() > limit;
    let bytes = &bytes[..bytes.len().min(limit)];
    (String::from_utf8_lossy(bytes).into_owned(), truncated)
}

#[cfg(all(test, unix))]
mod bounded_command_tests;
