use std::{collections::BTreeMap, path::Path, time::Duration};

use tokio::process::Command;

/// The shell invocation shared by review steps and owner-local workspace runs.
pub fn workspace_command(path: &Path, step: &str, env: &BTreeMap<String, String>) -> Command {
    let mut command = Command::new("bash");
    command.arg("-lc").arg(step).envs(env).current_dir(path);
    command
}

pub async fn run_workspace_command(
    path: &Path,
    step: &str,
    env: &BTreeMap<String, String>,
    seconds: u64,
    limit: usize,
) -> Result<std::process::Output, String> {
    let mut command = workspace_command(path, step, env);
    command
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .kill_on_drop(true);
    bounded_output(&mut command, seconds, limit).await
}

pub(crate) async fn bounded_output(
    command: &mut Command,
    seconds: u64,
    limit: usize,
) -> Result<std::process::Output, String> {
    use std::process::Stdio;
    use tokio::io::AsyncReadExt;
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| e.to_string())?;
    let stdout = child.stdout.take().ok_or("missing stdout pipe")?;
    let stderr = child.stderr.take().ok_or("missing stderr pipe")?;
    let read = |pipe: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>| async move {
        let mut bytes = Vec::new();
        pipe.take((limit + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| e.to_string())?;
        if bytes.len() > limit {
            return Err("review command output exceeds size budget".to_owned());
        }
        Ok(bytes)
    };
    tokio::time::timeout(Duration::from_secs(seconds), async {
        let (stdout, stderr) = tokio::try_join!(read(Box::pin(stdout)), read(Box::pin(stderr)))?;
        let status = child.wait().await.map_err(|e| e.to_string())?;
        Ok(std::process::Output {
            status,
            stdout,
            stderr,
        })
    })
    .await
    .map_err(|_| "review command timed out".to_owned())?
}
