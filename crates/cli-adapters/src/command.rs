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
pub fn run_in_task_worktree(command: &mut Command, ctx: &executors::ExecutionContext) {
    // The Project environment goes first so Forge's own variables below
    // always win.
    command.envs(executors::environment::task_environment(&ctx.agent_config));
    command
        .current_dir(&ctx.worktree_path)
        .env("PWD", &ctx.worktree_path)
        .env("FORGE_TASK_ID", &ctx.task_id)
        .env("FORGE_EXECUTION_ID", &ctx.execution_id);
    let Some(outbox) = executors::execution_outbox_path(
        std::path::Path::new(&ctx.worktree_path),
        &ctx.execution_id,
    ) else {
        return;
    };
    match std::fs::create_dir_all(&outbox) {
        Ok(()) => {
            command.env(executors::FORGE_OUTBOX_ENV, &outbox);
        }
        Err(error) => tracing::warn!(
            execution_id = %ctx.execution_id,
            outbox = %outbox.display(),
            %error,
            "execution outbox could not be created; worklog and evidence will not be delivered"
        ),
    }
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
        let ctx = executors::ExecutionContext {
            task_id: "task-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            worktree_path: worktree.to_string_lossy().into_owned(),
            description: String::new(),
            agent_config: serde_json::json!({
                executors::environment::TASK_ENVIRONMENT_CONFIG_KEY: {
                    "GODOT_BIN": "/opt/godot",
                },
            }),
            logs_path: String::new(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        };
        let mut command = CommandBuilder::new("true").build();
        run_in_task_worktree(&mut command, &ctx);

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
        assert!(
            outbox.is_dir(),
            "the outbox exists before the harness starts"
        );
    }
}
