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
