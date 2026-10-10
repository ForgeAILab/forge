use executors::CommandOverrides;
use std::collections::HashMap;
use std::ffi::OsString;
use std::process::Stdio;
use tokio::process::Command;

/// Builds a tokio Command from adapter defaults + user overrides.
pub struct CommandBuilder {
    default_program: String,
    default_args: Vec<String>,
    adapter_args: Vec<String>,
    overrides: CommandOverrides,
}

impl CommandBuilder {
    pub fn new(default_program: impl Into<String>) -> Self {
        Self {
            default_program: default_program.into(),
            default_args: Vec::new(),
            adapter_args: Vec::new(),
            overrides: CommandOverrides::default(),
        }
    }

    pub fn default_args(mut self, args: Vec<String>) -> Self {
        self.default_args = args;
        self
    }

    pub fn adapter_args(mut self, args: Vec<String>) -> Self {
        self.adapter_args = args;
        self
    }

    pub fn overrides(mut self, overrides: &CommandOverrides) -> Self {
        self.overrides = overrides.clone();
        self
    }

    /// Resolve the program to use (override or default).
    fn resolve_program(&self) -> String {
        if let Some(ref base) = self.overrides.base_command_override {
            base.clone()
        } else {
            self.default_program.clone()
        }
    }

    /// Build the full argument list: default_args + adapter_args + additional_params.
    fn resolve_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        // If using base_command_override, skip default_args (user controls everything)
        if self.overrides.base_command_override.is_none() {
            args.extend(self.default_args.iter().cloned());
        }

        args.extend(self.adapter_args.iter().cloned());

        if let Some(ref additional) = self.overrides.additional_params {
            args.extend(additional.iter().cloned());
        }

        args
    }

    /// Merge profile env into the system environment (profile wins on conflict).
    fn resolve_env(&self) -> HashMap<OsString, OsString> {
        let mut env: HashMap<OsString, OsString> = std::env::vars_os().collect();

        if let Some(ref profile_env) = self.overrides.env {
            for (k, v) in profile_env {
                env.insert(OsString::from(k), OsString::from(v));
            }
        }

        env
    }

    /// Build the tokio Command ready to spawn.
    pub fn build(&self) -> Command {
        let program = self.resolve_program();
        let args = self.resolve_args();
        let env = self.resolve_env();

        let mut cmd = Command::new(&program);
        cmd.args(&args);
        cmd.env_clear();
        for (k, v) in &env {
            cmd.env(k, v);
        }
        // Adapter commands are non-interactive by contract.  Keep them away
        // from the caller's controlling terminal; adapters that need a
        // protocol stream explicitly replace this with piped stdin.
        cmd.stdin(Stdio::null());

        cmd
    }

    /// Resolve the full executable path using `which`.
    pub fn resolve_executable(&self) -> Option<std::path::PathBuf> {
        let program = self.resolve_program();
        which::which(&program).ok()
    }
}

/// Point a child process at the Task worktree.
///
/// `current_dir` alone is not enough: a tool that trusts `$PWD` over
/// `getcwd()` — OpenCode resolves its project directory from it — would
/// otherwise inherit the Forge server's own working directory and run the
/// Task against the wrong checkout.
///
/// The child also learns its Task, execution, and outbox (see
/// [`executors::execution_outbox_path`]); the outbox is created here so the
/// harness can write to it without first discovering that it is missing.
///
/// The returned scope owns the execution's temp directory (`TMPDIR`, `TMP`,
/// `TEMP` inside the Task root): hold it until the child has exited.
#[must_use = "dropping the scope removes the execution's temp directory"]
pub fn run_in_task_worktree(
    command: &mut Command,
    ctx: &executors::ExecutionContext,
) -> executors::sandbox::RunScope {
    run_in_task_worktree_with(command, ctx, |sandbox| sandbox)
}

/// [`run_in_task_worktree`] for an adapter whose CLI confines its own writes.
/// `admit` sees the environment the Task root offers and returns the part the
/// CLI's sandbox can write; what it drops the run keeps as inherited, so a run
/// is never handed a temp or build directory it cannot write.
#[must_use = "dropping the scope removes the execution's temp directory"]
pub fn run_in_task_worktree_with(
    command: &mut Command,
    ctx: &executors::ExecutionContext,
    admit: impl FnOnce(executors::sandbox::SandboxEnv) -> executors::sandbox::SandboxEnv,
) -> executors::sandbox::RunScope {
    // The Project environment goes first so Forge's own variables below
    // always win.
    let environment = executors::environment::task_environment(&ctx.agent_config);
    command.envs(&environment);
    let run_scope = admit(executors::sandbox::SandboxEnv::for_run(
        std::path::Path::new(&ctx.worktree_path),
        &ctx.execution_id,
        executors::sandbox::RunPurpose::Execution,
    ))
    .scoped();
    executors::run_process::apply_sandboxed(command, &environment, run_scope.env());
    command
        .current_dir(&ctx.worktree_path)
        .env("PWD", &ctx.worktree_path)
        .env("FORGE_TASK_ID", &ctx.task_id)
        .env("FORGE_EXECUTION_ID", &ctx.execution_id);
    let Some(outbox) = executors::execution_outbox_path(
        std::path::Path::new(&ctx.worktree_path),
        &ctx.execution_id,
    ) else {
        return run_scope;
    };
    match std::fs::create_dir_all(&outbox) {
        Ok(()) => {
            command.env(executors::FORGE_OUTBOX_ENV, &outbox);
            if executors::task_role_can_write_plan(executors::task_role(&ctx.agent_config)) {
                command.env(
                    executors::FORGE_PLAN_PATH_ENV,
                    outbox.join(executors::OUTBOX_PLAN_FILE),
                );
            }
        }
        Err(error) => tracing::warn!(
            execution_id = %ctx.execution_id,
            outbox = %outbox.display(),
            %error,
            "execution outbox could not be created; worklog and evidence will not be delivered"
        ),
    }
    run_scope
}

#[cfg(test)]
mod tests {
    use super::*;
    use executors::CommandOverrides;

    #[test]
    fn default_command_no_overrides() {
        let builder = CommandBuilder::new("codex")
            .default_args(vec!["-y".into(), "@openai/codex@0.1".into()])
            .adapter_args(vec!["app-server".into()]);

        let cmd = builder.build();
        let prog = cmd.as_std().get_program();
        assert_eq!(prog, "codex");

        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, vec!["-y", "@openai/codex@0.1", "app-server"]);
    }

    #[test]
    fn base_command_override_skips_default_args() {
        let overrides = CommandOverrides {
            base_command_override: Some("/usr/local/bin/my-codex".into()),
            additional_params: Some(vec!["--verbose".into()]),
            env: None,
        };
        let builder = CommandBuilder::new("npx")
            .default_args(vec!["-y".into(), "@openai/codex@0.1".into()])
            .adapter_args(vec!["app-server".into()])
            .overrides(&overrides);

        let cmd = builder.build();
        let prog = cmd.as_std().get_program();
        assert_eq!(prog, "/usr/local/bin/my-codex");

        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, vec!["app-server", "--verbose"]);
    }

    #[test]
    fn env_merge_profile_wins() {
        let overrides = CommandOverrides {
            base_command_override: None,
            additional_params: None,
            env: Some(HashMap::from([("MY_VAR".into(), "profile_val".into())])),
        };
        let builder = CommandBuilder::new("echo").overrides(&overrides);
        let cmd = builder.build();

        let envs: HashMap<_, _> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_owned(), v.to_owned())))
            .collect();
        assert_eq!(
            envs.get(&OsString::from("MY_VAR")),
            Some(&OsString::from("profile_val"))
        );
    }
}

#[cfg(test)]
mod worktree_tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn worktree_children_see_the_worktree_as_pwd_and_their_outbox() {
        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join("task-1").join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree dir");
        let mut agent_config = serde_json::json!({
            executors::environment::TASK_ENVIRONMENT_CONFIG_KEY: {
                "GODOT_BIN": "/opt/godot",
            },
        });
        executors::mark_task_role(&mut agent_config, "planner");
        let ctx = executors::ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            worktree_path: worktree.to_string_lossy().into_owned(),
            description: String::new(),
            agent_config,
            logs_path: String::new(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };
        let mut command = CommandBuilder::new("true").build();
        let _run_scope = run_in_task_worktree(&mut command, &ctx);

        let std_command = command.as_std();
        assert_eq!(std_command.get_current_dir(), Some(worktree.as_path()));
        let env = |name: &str| {
            std_command
                .get_envs()
                .find(|(key, _)| *key == OsStr::new(name))
                .and_then(|(_, value)| value)
                .map(|value| value.to_owned())
        };
        assert_eq!(env("PWD"), Some(worktree.clone().into_os_string()));
        assert_eq!(env("FORGE_EXECUTION_ID"), Some("exec-1".into()));
        assert_eq!(env("GODOT_BIN"), Some("/opt/godot".into()));
        let outbox = temp
            .path()
            .join("task-1")
            .join(".forge-outbox")
            .join("exec-1");
        assert_eq!(env("FORGE_OUTBOX"), Some(outbox.clone().into_os_string()));
        assert_eq!(
            env("FORGE_PLAN_PATH"),
            Some(outbox.join("plan.md").into_os_string())
        );
        assert!(
            outbox.is_dir(),
            "the outbox exists before the harness starts"
        );
    }

    #[test]
    fn reviewer_children_do_not_receive_a_plan_write_path() {
        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join("task-1").join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree dir");
        let mut agent_config = serde_json::json!({});
        executors::mark_task_role(&mut agent_config, "reviewer");
        let ctx = executors::ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-review".to_owned(),
            worktree_path: worktree.to_string_lossy().into_owned(),
            description: String::new(),
            agent_config,
            logs_path: String::new(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };
        let mut command = CommandBuilder::new("true").build();

        let _run_scope = run_in_task_worktree(&mut command, &ctx);

        assert!(
            command
                .as_std()
                .get_envs()
                .all(|(key, _)| key != OsStr::new(executors::FORGE_PLAN_PATH_ENV))
        );
    }
}

#[cfg(test)]
mod run_budget_tests {
    use super::*;
    #[test]
    fn launch_environment_preserves_project_and_fills_budget() {
        let temp = tempfile::tempdir().unwrap();
        let env =
            std::collections::BTreeMap::from([("CARGO_BUILD_JOBS".into(), "project-value".into())]);
        let mut config = serde_json::json!({});
        executors::environment::mark_task_environment(&mut config, &env);
        let ctx = executors::ExecutionContext {
            task_id: "task".into(),
            execution_id: "exec".into(),
            worktree_path: temp.path().to_string_lossy().into_owned(),
            description: String::new(),
            agent_config: config,
            logs_path: String::new(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };
        let mut command = CommandBuilder::new("codex").build();
        let _run_scope = run_in_task_worktree(&mut command, &ctx);
        let envs: std::collections::BTreeMap<_, _> = command
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_owned(), v.to_owned())))
            .collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("CARGO_BUILD_JOBS")).unwrap(),
            "project-value"
        );
        let jobs = executors::run_process::machine_policy().get().build_jobs();
        for (key, default) in [
            ("RUST_TEST_THREADS", jobs.to_string()),
            ("MAKEFLAGS", format!("-j{jobs}")),
            ("CMAKE_BUILD_PARALLEL_LEVEL", jobs.to_string()),
            ("GOFLAGS", format!("-p={jobs}")),
        ] {
            assert_eq!(
                envs.get(std::ffi::OsStr::new(key)),
                Some(&std::env::var_os(key).unwrap_or(default.into()))
            );
        }
    }

    fn launch_ctx(worktree: &std::path::Path, execution_id: &str) -> executors::ExecutionContext {
        executors::ExecutionContext {
            task_id: "t".to_owned(),
            execution_id: execution_id.to_owned(),
            worktree_path: worktree.to_string_lossy().into_owned(),
            description: String::new(),
            agent_config: serde_json::json!({}),
            logs_path: String::new(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        }
    }

    /// Every CLI adapter launches through `CommandBuilder::build` (which
    /// copies the server environment, `TMPDIR` included) and then
    /// `run_in_task_worktree`.
    #[tokio::test]
    async fn adapter_launch_gets_a_task_root_tmpdir_that_goes_when_the_execution_returns() {
        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join("t").join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree dir");
        executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).expect("reserved");
        for (script, code) in [("exit 0", 0), ("exit 3", 3)] {
            let mut command = CommandBuilder::new("sh")
                .adapter_args(vec![
                    "-c".into(),
                    format!("touch \"$TMPDIR/made\"; printf '%s' \"$TMPDIR\"; {script}"),
                ])
                .build();
            command.env("TMPDIR", "/server/tmp");
            let run_scope = run_in_task_worktree(&mut command, &launch_ctx(&worktree, "exec-1"));
            let output = command.output().await.expect("child runs");
            assert_eq!(output.status.code(), Some(code));
            let seen = std::path::PathBuf::from(String::from_utf8(output.stdout).unwrap());
            assert_eq!(
                seen,
                worktree.parent().unwrap().join(".forge-task/tmp/exec1")
            );
            assert!(seen.join("made").exists());
            // Success, failure and cancellation all end the adapter's
            // `execute`, which drops the scope.
            drop(run_scope);
            assert!(!seen.exists());
        }
    }

    #[tokio::test]
    async fn adapter_launch_in_an_unreserved_task_root_keeps_the_inherited_tmpdir() {
        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join("legacy").join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree dir");
        let mut command = CommandBuilder::new("sh")
            .adapter_args(vec!["-c".into(), "printf '%s' \"$TMPDIR\"".into()])
            .build();
        command.env("TMPDIR", "/server/tmp");
        let _run_scope = run_in_task_worktree(&mut command, &launch_ctx(&worktree, "exec-1"));
        let output = command.output().await.expect("child runs");
        assert_eq!(String::from_utf8(output.stdout).unwrap(), "/server/tmp");
        assert!(!worktree.parent().unwrap().join(".forge-task").exists());
    }

    /// What a run printed for `$RUSTC_WRAPPER|$KACHE_CACHE_DIR` when Forge
    /// offered `wrapper` and `store`. The operator's own environment wins
    /// over Forge's, so on a machine whose environment names a wrapper (or a
    /// kache directory) this asserts that rule instead.
    #[cfg(unix)]
    fn assert_saw_compiler_cache(seen: &str, wrapper: &std::path::Path, store: &std::path::Path) {
        let operator = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
        let (wrapper, store) = (wrapper.to_str().unwrap(), store.to_str().unwrap());
        if operator("RUSTC_WRAPPER") {
            assert!(!seen.starts_with(wrapper), "{seen}");
        } else if operator("KACHE_CACHE_DIR") {
            assert!(seen.starts_with(&format!("{wrapper}|")), "{seen}");
        } else {
            assert_eq!(seen, format!("{wrapper}|{store}"));
        }
    }

    /// Plan 3.4 F: every CLI adapter launch in a Task worktree is handed
    /// the machine's shared compiler cache; an adapter whose CLI confines
    /// its own writes can drop it.
    #[cfg(unix)]
    #[tokio::test]
    async fn adapter_launch_gets_the_shared_compiler_cache() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("ws");
        let worktree = root.join("t").join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
        // A linked worktree of the repository `r1`, as the server lays it out.
        std::fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", root.join(".repos/r1/worktrees/t").display()),
        )
        .unwrap();
        // The repository names the worktree back, as Git does; without it the
        // worktree's own `.git` file claims nothing.
        std::fs::create_dir_all(root.join(".repos/r1/worktrees/t")).unwrap();
        std::fs::write(
            root.join(".repos/r1/worktrees/t/gitdir"),
            format!("{}\n", worktree.join(".git").display()),
        )
        .unwrap();
        let wrapper = temp.path().join("kache");
        std::fs::write(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        executors::compiler_cache::install(
            &root,
            Some(executors::compiler_cache::CompilerCache {
                kind: executors::compiler_cache::WrapperKind::of(&wrapper),
                wrapper: wrapper.clone(),
                dir: root.join(executors::compiler_cache::CACHE_DIR),
                max_bytes: 1 << 30,
            }),
        );
        let store = root.join(executors::compiler_cache::CACHE_DIR).join("r1");
        let seen_file = temp.path().join("seen");
        let script = format!(
            "printf '%s|%s' \"$RUSTC_WRAPPER\" \"$KACHE_CACHE_DIR\" > '{}'",
            seen_file.display()
        );
        let launch = |args: Vec<String>| CommandBuilder::new("sh").adapter_args(args).build();
        let mut command = launch(vec!["-c".into(), script.clone()]);
        let run_scope = run_in_task_worktree(&mut command, &launch_ctx(&worktree, "exec-1"));
        assert!(command.output().await.expect("child runs").status.success());
        drop(run_scope);
        assert_saw_compiler_cache(
            &std::fs::read_to_string(&seen_file).unwrap(),
            &wrapper,
            &store,
        );

        let mut command = launch(vec!["-c".into(), script]);
        let run_scope =
            run_in_task_worktree_with(&mut command, &launch_ctx(&worktree, "exec-2"), |sandbox| {
                sandbox.without_compiler_cache()
            });
        assert!(command.output().await.expect("child runs").status.success());
        drop(run_scope);
        executors::compiler_cache::install(&root, None);
        let seen = std::fs::read_to_string(&seen_file).unwrap();
        assert!(!seen.starts_with(wrapper.to_str().unwrap()), "{seen}");
    }
}
