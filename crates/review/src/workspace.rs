use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use api_types::ProjectEnvironment;
use async_trait::async_trait;

use crate::ReviewError;

#[derive(Clone, Copy)]
pub struct CommandLimits {
    pub timeout_secs: u64,
    pub max_output_bytes: usize,
}

pub struct CommandOutput {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl From<std::process::Output> for CommandOutput {
    fn from(output: std::process::Output) -> Self {
        Self {
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}

/// Review I/O is supplied by the workspace owner; the review crate has no
/// dependency on the service layer that resolves placement.
#[async_trait]
pub trait ReviewWorkspace: Send + Sync {
    /// None preserves the unbounded CI runner. Conformance commands are bounded.
    async fn run(
        &self,
        command: &str,
        env: &BTreeMap<String, String>,
        limits: Option<CommandLimits>,
    ) -> Result<CommandOutput, ReviewError>;
    async fn git_read(&self, args: &[&str], optional: bool) -> Result<Option<String>, String>;
    async fn diff(&self, default_branch: &str) -> Result<String, ReviewError>;
    async fn clean_checkout(
        &self,
        commit_sha: &str,
        environment: &ProjectEnvironment,
        prepare: bool,
    ) -> Result<Box<dyn ReviewWorkspace>, String>;
    async fn materialize_assets(&self, environment: &ProjectEnvironment) -> Result<(), String> {
        executors::environment::materialize_assets(&self.embedded_path()?, &environment.assets)
            .await
    }
    async fn restore(&self, commit_sha: &str) -> Result<(), String>;
    /// Only the execution provider on the placement owner may consume this path.
    async fn execution_path(&self) -> Result<String, String> {
        self.embedded_path()
            .map(|path| path.to_string_lossy().into_owned())
    }
    fn placement_id(&self) -> Option<&str> {
        None
    }
    /// Preserve owner failures across the string-returning evidence boundary.
    fn infrastructure_error(&self, _reason: &str) -> Option<ReviewError> {
        None
    }
    /// Release an owner-local detached checkout after its checks settle.
    async fn close(&self) -> Result<(), String> {
        Ok(())
    }
    fn embedded_path(&self) -> Result<PathBuf, String>;
}

struct LocalReviewWorkspace {
    path: PathBuf,
    // Keep the detached checkout alive until all conformance checks finish.
    _scratch: tempfile::TempDir,
}

impl AsRef<Path> for LocalReviewWorkspace {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl<T: AsRef<Path> + Send + Sync + ?Sized> ReviewWorkspace for T {
    async fn run(
        &self,
        command: &str,
        env: &BTreeMap<String, String>,
        limits: Option<CommandLimits>,
    ) -> Result<CommandOutput, ReviewError> {
        let output = match limits {
            Some(limits) => crate::run_workspace_command(
                self.as_ref(),
                command,
                env,
                limits.timeout_secs,
                limits.max_output_bytes,
            )
            .await
            .map_err(ReviewError::Workspace)?,
            None => {
                let receipt = check_executor::execute(check_executor::CheckExecution {
                    operation_id: "legacy-review-ci",
                    spec: &check_executor::legacy_ci_spec(command, env, 0, false),
                    target: check_executor::CheckoutTarget::Workspace(self.as_ref()),
                    owner: api_types::CheckOwnerIdentity {
                        owner_kind: "server".into(),
                        machine_id: None,
                        runtime_id: "server".into(),
                    },
                    input_revisions: None,
                    environment: env,
                    deadline: None,
                    cancel: &tokio_util::sync::CancellationToken::new(),
                    permit: &check_executor::CheckPermit::already_admitted(),
                    cleanup: check_executor::CleanupPlan {
                        commands: &[],
                        timeout: check_executor::CLEANUP_TIMEOUT,
                    },
                    output_limit: 1024 * 1024,
                })
                .await;
                if let Some(message) = receipt.infrastructure_message {
                    return Err(std::io::Error::other(message).into());
                }
                let Some(output) = receipt.commands.into_iter().next() else {
                    return Ok(CommandOutput {
                        exit_code: Some(0),
                        stdout: String::new(),
                        stderr: String::new(),
                    });
                };
                return Ok(CommandOutput {
                    exit_code: output.exit_code,
                    stdout: output.stdout_tail,
                    stderr: output.stderr_tail,
                });
            }
        };
        Ok(output.into())
    }

    async fn git_read(&self, args: &[&str], optional: bool) -> Result<Option<String>, String> {
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            git::command_output_bounded(self.as_ref(), args, crate::contract::MAX_EVIDENCE_BYTES),
        )
        .await
        .map_err(|_| "review command timed out".to_owned())?
        .map_err(|error| match error {
            git::GitError::Io(error) => error.to_string(),
            error => error.to_string(),
        })?;
        if !output.status.success() {
            return if optional {
                Ok(None)
            } else {
                Err(format!(
                    "git evidence unavailable: {}",
                    String::from_utf8_lossy(&output.stderr)
                ))
            };
        }
        String::from_utf8(output.stdout)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    async fn diff(&self, default_branch: &str) -> Result<String, ReviewError> {
        crate::read_git_diff(self.as_ref(), default_branch).await
    }

    async fn clean_checkout(
        &self,
        commit_sha: &str,
        environment: &ProjectEnvironment,
        prepare: bool,
    ) -> Result<Box<dyn ReviewWorkspace>, String> {
        let scratch = tempfile::tempdir().map_err(|error| error.to_string())?;
        let path = scratch.path().join("candidate");
        if prepare {
            self.git_read(
                &[
                    "clone",
                    "--shared",
                    "--no-checkout",
                    "--",
                    ".",
                    path.to_str().ok_or("invalid check path")?,
                ],
                false,
            )
            .await?;
            path.git_read(&["checkout", "--detach", commit_sha], false)
                .await?;
            executors::environment::materialize_assets(&path, &environment.assets).await?;
        }
        Ok(Box::new(LocalReviewWorkspace {
            path,
            _scratch: scratch,
        }))
    }

    async fn restore(&self, commit_sha: &str) -> Result<(), String> {
        git::restore_worktree(self.as_ref(), commit_sha)
            .await
            .map_err(|error| error.to_string())
    }

    fn embedded_path(&self) -> Result<PathBuf, String> {
        Ok(self.as_ref().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_ci_spawn_failure_keeps_its_io_error_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");
        assert!(matches!(
            missing.run("true", &BTreeMap::new(), None).await,
            Err(ReviewError::Io(_))
        ));
    }

    #[tokio::test]
    async fn unbounded_ci_steps_keep_output_that_bounded_conformance_rejects() {
        let workspace = tempfile::tempdir().unwrap();
        let command = "printf '%8192s' x";
        let output = workspace
            .path()
            .run(command, &BTreeMap::new(), None)
            .await
            .unwrap();
        assert_eq!(output.exit_code, Some(0));
        assert_eq!(output.stdout.len(), 8192);
        let bounded = workspace
            .path()
            .run(
                command,
                &BTreeMap::new(),
                Some(CommandLimits {
                    timeout_secs: 5,
                    max_output_bytes: 4096,
                }),
            )
            .await;
        assert!(
            matches!(bounded, Err(ReviewError::Workspace(reason)) if reason == "review command output exceeds size budget")
        );
    }
}
