//! Command effects; all storage belongs to the consumer.
use super::{RunResult, RunSpec};
use crate::{workspace_backend::Result, ServiceError};
use std::{path::Path, time::Instant};

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
    // These commands have no run scope; while one runs in a Task worktree
    // the collector leaves that repository's compiler-cache store alone.
    let _cache_hold = executors::sandbox::hold_compiler_cache(path);
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
        api_types::WorkspaceRunPurpose::CiStep => review::run_workspace_command(
            path,
            &spec.command,
            &spec.env,
            spec.timeout_secs,
            spec.max_output_bytes.min(1024 * 1024),
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
