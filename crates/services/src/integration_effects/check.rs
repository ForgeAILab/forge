//! Command effects and the CI sequence; all storage belongs to the consumer.
use super::{EffectWorkspace, RunResult, RunSpec};
use crate::{workspace_backend::Result, ServiceError};
use api_types::CheckCommandOutcome;
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
};

/// Embedded commands share timeout, stdin, Git environment and output semantics.
pub async fn run_at(path: &Path, spec: &RunSpec) -> Result<RunResult> {
    if spec.max_output_bytes == 0
        || (spec.timeout_secs == 0 && spec.purpose != api_types::WorkspaceRunPurpose::CiStep)
    {
        return Err(ServiceError::invalid_operation(
            "workspace run requires a positive timeout and output bound",
        )
        .into());
    }
    if spec.purpose == api_types::WorkspaceRunPurpose::EnvironmentSetup
        && spec.max_output_bytes < isize::MAX as usize
    {
        return run_bounded_environment(path, spec).await;
    }
    let started = Instant::now();
    let output = match spec.purpose {
        api_types::WorkspaceRunPurpose::EnvironmentProbe
        | api_types::WorkspaceRunPurpose::RepoProvision => {
            return Err(ServiceError::invalid_operation(
                "machine operations cannot run through workspace.run",
            )
            .into());
        }
        api_types::WorkspaceRunPurpose::Hook | api_types::WorkspaceRunPurpose::EnvironmentSetup => {
            // Hooks and environment checks inherit Git variables, close
            // stdin, and collect output before their callers redact it.
            let mut command = review::workspace_command(path, &spec.command, &spec.env);
            command
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true);
            tokio::time::timeout(
                std::time::Duration::from_secs(spec.timeout_secs),
                command.output(),
            )
            .await
            .map_err(|_| ServiceError::invalid_operation("review command timed out"))?
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
        }
        api_types::WorkspaceRunPurpose::CiStep
            if spec.timeout_secs == 0 && spec.max_output_bytes == usize::MAX =>
        {
            review::workspace_command(path, &spec.command, &spec.env)
                .output()
                .await?
        }
        api_types::WorkspaceRunPurpose::CiStep => review::run_workspace_command(
            path,
            &spec.command,
            &spec.env,
            spec.timeout_secs,
            spec.max_output_bytes,
        )
        .await
        .map_err(ServiceError::invalid_operation)?,
    };
    let mut stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if spec.purpose == api_types::WorkspaceRunPurpose::CiStep {
        stdout = executors::environment::redact_environment_values(&stdout, &spec.env);
        stderr = executors::environment::redact_environment_values(&stderr, &spec.env);
    }
    Ok(RunResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout_tail: stdout,
        stderr_tail: stderr,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

/// Drain both pipes continuously but retain only their bounded tails. Checks
/// still run in the checkout; callers must configure read-only commands.
async fn run_bounded_environment(path: &Path, spec: &RunSpec) -> Result<RunResult> {
    use tokio::io::AsyncReadExt;
    async fn tail(
        mut pipe: impl tokio::io::AsyncRead + Unpin,
        limit: usize,
    ) -> std::io::Result<Vec<u8>> {
        let mut retained = Vec::with_capacity(limit);
        let mut chunk = [0u8; 4096];
        loop {
            let size = pipe.read(&mut chunk).await?;
            if size == 0 {
                break;
            }
            retained.extend_from_slice(&chunk[..size]);
            if retained.len() > limit {
                retained.drain(..retained.len() - limit);
            }
        }
        Ok(retained)
    }
    let started = Instant::now();
    let mut command = review::workspace_command(path, &spec.command, &spec.env);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (stdout, stderr, status) =
        tokio::time::timeout(std::time::Duration::from_secs(spec.timeout_secs), async {
            tokio::join!(
                tail(stdout, spec.max_output_bytes),
                tail(stderr, spec.max_output_bytes),
                child.wait()
            )
        })
        .await
        .map_err(|_| ServiceError::invalid_operation("review command timed out"))?;
    Ok(RunResult {
        exit_code: status?.code().unwrap_or(-1),
        stdout_tail: String::from_utf8_lossy(&stdout?).into_owned(),
        stderr_tail: String::from_utf8_lossy(&stderr?).into_owned(),
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

pub struct CheckRunInput<'a> {
    pub workspace: &'a EffectWorkspace,
    pub commands: &'a [String],
    pub purpose: api_types::WorkspaceRunPurpose,
    pub environment: &'a BTreeMap<String, String>,
    pub deadline: Option<Duration>,
    pub max_output_bytes: usize,
}

pub struct CheckCommand {
    pub index: usize,
    pub spec: RunSpec,
    pub started_at: String,
}

pub struct CheckRunFailure {
    pub error: crate::workspace_backend::WorkspaceBackendError,
    pub completed_steps: usize,
    pub commands: Vec<CheckCommandOutcome>,
}

pub struct CheckRunOutcome {
    pub commands: Vec<CheckCommandOutcome>,
    pub failed_step_index: Option<usize>,
}

/// A sequence yields one effect at a time so an owner reply can be recorded
/// and acknowledged at the old site before the next command is issued.
pub struct CheckRun<'a> {
    input: CheckRunInput<'a>,
    outcome: CheckRunOutcome,
}

impl<'a> CheckRun<'a> {
    pub fn new(input: CheckRunInput<'a>) -> Self {
        let capacity = input.commands.len();
        Self {
            input,
            outcome: CheckRunOutcome {
                commands: Vec::with_capacity(capacity),
                failed_step_index: None,
            },
        }
    }
    pub fn next_command(&self) -> Option<CheckCommand> {
        if self.outcome.failed_step_index.is_some() {
            return None;
        }
        let index = self.outcome.commands.len();
        let step = self.input.commands.get(index)?;
        Some(CheckCommand {
            index,
            started_at: chrono::Utc::now().to_rfc3339(),
            spec: RunSpec {
                purpose: self.input.purpose,
                command: step.clone(),
                env: self.input.environment.clone(),
                timeout_secs: self.input.deadline.map_or(0, |deadline| deadline.as_secs()),
                max_output_bytes: self.input.max_output_bytes,
            },
        })
    }
    pub fn completed(&mut self, command: CheckCommand, output: RunResult) {
        let finished_at = chrono::Utc::now().to_rfc3339();
        let env = self.input.environment;
        let stderr = executors::environment::redact_environment_values(&output.stderr_tail, env);
        let stdout = executors::environment::redact_environment_values(&output.stdout_tail, env);
        let output_tail = if stdout.is_empty() {
            stderr.clone()
        } else if stderr.is_empty() {
            stdout.clone()
        } else {
            format!("{stdout}\n{stderr}")
        };
        let exit_code = if output.exit_code < 0 {
            1
        } else {
            output.exit_code
        };
        self.outcome.commands.push(CheckCommandOutcome {
            index: command.index,
            command: command.spec.command,
            exit_code,
            stderr_tail: tail_bytes(&stderr, 4096),
            output_tail: tail_bytes(&output_tail, 4096),
            started_at: command.started_at,
            finished_at,
        });
        if exit_code != 0 {
            self.outcome.failed_step_index = Some(command.index);
        }
    }
    pub fn infrastructure_failed(
        self,
        error: crate::workspace_backend::WorkspaceBackendError,
    ) -> CheckRunFailure {
        CheckRunFailure {
            error,
            completed_steps: self.outcome.commands.len(),
            commands: self.outcome.commands,
        }
    }
    pub fn outcome(self) -> CheckRunOutcome {
        self.outcome
    }
}

pub fn tail_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut start = text.len().saturating_sub(max_bytes);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

#[cfg(test)]
mod environment_tests {
    use super::*;
    #[tokio::test]
    async fn environment_output_is_bounded_while_draining_both_streams() {
        let dir = tempfile::TempDir::new().unwrap();
        let spec=RunSpec{purpose:api_types::WorkspaceRunPurpose::EnvironmentSetup,
            command:"i=0; while [ $i -lt 10000 ]; do printf 'abcdefghij'; printf 'ABCDEFGHIJ' >&2; i=$((i+1)); done; printf 'tail'; printf 'TAIL' >&2".into(),
            env:Default::default(),timeout_secs:5,max_output_bytes:64};
        let result = run_bounded_environment(dir.path(), &spec).await.unwrap();
        assert_eq!(result.exit_code, 0);
        assert!(result.stdout_tail.len() <= 64 && result.stderr_tail.len() <= 64);
        assert!(result.stdout_tail.ends_with("tail"));
        assert!(result.stderr_tail.ends_with("TAIL"));
    }
}
