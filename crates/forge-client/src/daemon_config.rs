use std::{fs, path::Path};

use anyhow::{Context, Result};
use api_types::{WorkspaceRunPolicy, WorkspaceRunPurpose};
use serde::Deserialize;

/// Local daemon configuration, read from `daemon.yaml` beside its credentials.
/// Requests cannot replace the effective policy read from this local file.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    /// Unset: automatic; zero: unlimited. Computed on this daemon machine.
    pub max_concurrent_runs: Option<u32>,
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

impl DaemonConfig {
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
        serde_yaml::from_str(&text).with_context(|| format!("parse {}", path.display()))
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
