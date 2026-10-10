use std::{fs, path::Path};

use anyhow::{Context, Result};
use api_types::{WorkspaceRunPolicy, WorkspaceRunPurpose};
use serde::Deserialize;

/// Local daemon configuration, read from `daemon.yaml` beside its credentials.
/// Requests cannot replace the effective policy read from this local file.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// Unset: automatic; zero: unlimited. Computed on this daemon machine.
    pub max_concurrent_runs: Option<u32>,
    pub build_jobs_per_run: Option<u32>,
    pub run_nice: u32,
    pub workspace: DaemonWorkspaceConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonWorkspaceConfig {
    pub run: DaemonWorkspaceRunConfig,
    /// `workspace.compiler_cache`: this machine's opt-in shared compiler
    /// cache. Never taken from the server: a daemon is another machine.
    pub compiler_cache: config::CompilerCacheConfig,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonWorkspaceRunConfig {
    /// `workspace.run.allow`: locally permitted purposes. Defaults to
    /// `[ci_step]`; hook and environment_setup require an explicit opt-in.
    pub allow: Vec<WorkspaceRunPurpose>,
}

impl Default for DaemonWorkspaceRunConfig {
    fn default() -> Self {
        Self {
            allow: vec![WorkspaceRunPurpose::CiStep],
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            max_concurrent_runs: None,
            build_jobs_per_run: None,
            run_nice: config::default_run_nice(),
            workspace: Default::default(),
        }
    }
}
impl DaemonConfig {
    pub fn run_budget(
        &self,
        cap: Option<u32>,
        jobs: Option<u32>,
        nice: Option<u32>,
    ) -> Result<config::RunBudget> {
        let run_nice = nice.unwrap_or(self.run_nice);
        anyhow::ensure!(run_nice <= 19, "run_nice must be between 0 and 19");
        Ok(config::RunBudget {
            max_concurrent_runs: cap.or(self.max_concurrent_runs),
            build_jobs_per_run: jobs.or(self.build_jobs_per_run),
            run_nice,
        })
    }
    /// Resolve this machine's compiler cache (flags over `daemon.yaml`) and
    /// install it for `workspace_root`. Off unless a wrapper is configured.
    pub fn install_compiler_cache(
        &self,
        workspace_root: &Path,
        wrapper: Option<String>,
        max_bytes: Option<u64>,
        dir: Option<std::path::PathBuf>,
    ) {
        let config = self
            .workspace
            .compiler_cache
            .clone()
            .overridden(wrapper, max_bytes, dir);
        executors::compiler_cache::install_configured(workspace_root, &config);
    }
    pub fn load(credentials_path: &Path) -> Result<Self> {
        let path = credentials_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("daemon.yaml");
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
        };
        let config: Self =
            serde_yaml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        config.run_budget(None, None, None)?;
        Ok(config)
    }

    pub fn run_cap(&self, flag: Option<u32>) -> u32 {
        config::resolved_run_cap(flag.or(self.max_concurrent_runs))
    }

    pub fn run_policy(&self) -> WorkspaceRunPolicy {
        let mut allowed_purposes = Vec::new();
        for purpose in &self.workspace.run.allow {
            if !allowed_purposes.contains(purpose) {
                allowed_purposes.push(*purpose);
            }
        }
        WorkspaceRunPolicy { allowed_purposes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_capacity_local_configuration_and_flag() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        assert!(DaemonConfig::load(&credentials).unwrap().run_cap(None) >= 2);
        fs::write(dir.path().join("daemon.yaml"), "max_concurrent_runs: 3\n").unwrap();
        let config = DaemonConfig::load(&credentials).unwrap();
        assert_eq!(config.run_cap(None), 3);
        assert_eq!(config.run_cap(Some(0)), 0);
        assert_eq!(config.run_cap(Some(2)), 2);
    }

    #[test]
    fn local_run_policy_defaults_and_explicit_allow_list() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        assert_eq!(
            DaemonConfig::load(&credentials)
                .unwrap()
                .run_policy()
                .allowed_purposes,
            [WorkspaceRunPurpose::CiStep]
        );
        fs::write(
            dir.path().join("daemon.yaml"),
            "workspace:\n  run:\n    allow: [hook, ci_step, hook]\n",
        )
        .unwrap();
        assert_eq!(
            DaemonConfig::load(&credentials)
                .unwrap()
                .run_policy()
                .allowed_purposes,
            [WorkspaceRunPurpose::Hook, WorkspaceRunPurpose::CiStep]
        );
        fs::write(
            dir.path().join("daemon.yaml"),
            "workspace:\n  run:\n    allow: []\n",
        )
        .unwrap();
        assert!(DaemonConfig::load(&credentials)
            .unwrap()
            .run_policy()
            .allowed_purposes
            .is_empty());
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    #[cfg(unix)]
    use std::collections::BTreeMap;
    #[cfg(unix)]
    #[test]
    fn daemon_compiler_cache_is_its_own_file_key_and_flags_and_off_by_default() {
        use std::os::unix::fs::PermissionsExt;
        // The shared cache is off for a process whose own environment names
        // a wrapper (it wins for every run), which this test cannot undo.
        if executors::compiler_cache::WRAPPER_CONFIG_KEYS
            .iter()
            .any(|key| std::env::var_os(key).is_some_and(|value| !value.is_empty()))
        {
            eprintln!("skipped: this environment names a compiler wrapper (RUSTC_WRAPPER)");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let creds = dir.path().join("credentials.json");
        let base = dir.path().canonicalize().unwrap();
        let root = base.join("root");
        fs::create_dir_all(&root).unwrap();
        let wrapper = base.join("kache");
        // Answers `--version`, as the real one does when it is resolved.
        fs::write(
            &wrapper,
            "#!/bin/sh\n[ \"$1\" = --version ] && exit 0\nexec \"$@\"\n",
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();

        DaemonConfig::load(&creds)
            .unwrap()
            .install_compiler_cache(&root, None, None, None);
        assert!(executors::compiler_cache::installed(&root).is_none());

        fs::write(
            dir.path().join("daemon.yaml"),
            format!(
                "workspace:\n  compiler_cache:\n    wrapper: {}\n    max_bytes: 5000\n",
                wrapper.display()
            ),
        )
        .unwrap();
        let config = DaemonConfig::load(&creds).unwrap();
        config.install_compiler_cache(&root, None, None, None);
        let cache = executors::compiler_cache::installed(&root).unwrap();
        assert_eq!(cache.wrapper, wrapper);
        assert_eq!(cache.max_bytes, 5000);
        assert_eq!(cache.dir, root.join(".forge/build/cache"));

        // Flags win over the file; an empty wrapper flag turns it off.
        config.install_compiler_cache(&root, None, Some(9000), Some(base.join("c")));
        let cache = executors::compiler_cache::installed(&root).unwrap();
        assert_eq!((cache.max_bytes, cache.dir.clone()), (9000, base.join("c")));
        config.install_compiler_cache(&root, Some(String::new()), None, None);
        assert!(executors::compiler_cache::installed(&root).is_none());
    }

    #[test]
    fn daemon_run_budget_file_flag_and_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let creds = dir.path().join("credentials.json");
        let default = DaemonConfig::load(&creds)
            .unwrap()
            .run_budget(None, None, None)
            .unwrap();
        assert_eq!(default, config::RunBudget::default());
        fs::write(
            dir.path().join("daemon.yaml"),
            "max_concurrent_runs: 2\nbuild_jobs_per_run: 3\nrun_nice: 7\n",
        )
        .unwrap();
        let config = DaemonConfig::load(&creds).unwrap();
        assert_eq!(config.run_budget(None, None, None).unwrap().build_jobs(), 3);
        let off = config.run_budget(Some(0), Some(0), Some(0)).unwrap();
        assert_eq!(off.build_jobs(), 0);
        assert_eq!(off.run_nice, 0);
        assert!(config.run_budget(None, None, Some(20)).is_err());
        fs::write(dir.path().join("daemon.yaml"), "run_nice: 20\n").unwrap();
        assert!(DaemonConfig::load(&creds).is_err());
    }
    #[cfg(unix)]
    #[test]
    fn daemon_priority_probe() {
        println!("FORGE_NICE={}", executors::run_process::current_niceness());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_commands_receive_local_env_and_niceness() {
        let config: DaemonConfig =
            serde_yaml::from_str("build_jobs_per_run: 3\nrun_nice: 5\n").unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "daemon_config::budget_tests::daemon_priority_probe",
            "--nocapture",
        ]);
        let project = BTreeMap::from([("CARGO_BUILD_JOBS".into(), "project".into())]);
        let budget = config.run_budget(None, None, None).unwrap();
        executors::run_process::apply_budget(&mut command, budget, &project);
        let envs: BTreeMap<_, _> = command
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| v.map(|v| (k.to_owned(), v.to_owned())))
            .collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("CARGO_BUILD_JOBS")).unwrap(),
            "project"
        );
        assert_eq!(
            envs.get(std::ffi::OsStr::new("MAKEFLAGS")).unwrap(),
            &std::env::var_os("MAKEFLAGS").unwrap_or_else(|| "-j3".into())
        );
        let out = command.output().await.unwrap();
        assert!(out.status.success());
        let text = String::from_utf8(out.stdout).unwrap();
        let increased: i32 = text
            .lines()
            .find_map(|line| line.split_once("FORGE_NICE=").map(|(_, value)| value))
            .unwrap()
            .parse()
            .unwrap();
        let original = executors::run_process::current_niceness();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "daemon_config::budget_tests::daemon_priority_probe",
            "--nocapture",
        ]);
        executors::run_process::apply_budget(
            &mut command,
            config.run_budget(None, Some(0), Some(0)).unwrap(),
            &BTreeMap::new(),
        );
        let out = command.output().await.unwrap();
        let text = String::from_utf8(out.stdout).unwrap();
        let nice: i32 = text
            .lines()
            .find_map(|line| line.split_once("FORGE_NICE=").map(|(_, value)| value))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(nice, original);
        assert_eq!(increased, (original + 5).min(19));
    }
}
