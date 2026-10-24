//! Workspace command timeout, completion, and cancellation tests.

use std::time::Duration;

use tokio::process::Command;

use super::run_bounded_command;

/// A shell that backgrounds `sleep 60`, records its pid, then runs `tail`.
fn backgrounding_shell(pid_file: &std::path::Path, tail: &str) -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg(format!(
        "sleep 60 & echo $! > '{}'; {tail}",
        pid_file.display()
    ));
    command
}

async fn background_pid(pid_file: &std::path::Path) -> i32 {
    for _ in 0..50 {
        if let Ok(text) = std::fs::read_to_string(pid_file) {
            if let Ok(pid) = text.trim().parse() {
                return pid;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("background pid was never recorded");
}

async fn assert_gone(pid: i32) {
    let pid = nix::unistd::Pid::from_raw(pid);
    for _ in 0..50 {
        if nix::sys::signal::kill(pid, None).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
    panic!("process {pid} outlived its Task command");
}

#[tokio::test]
async fn a_command_that_never_exits_is_stopped_with_its_whole_group() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let output = run_bounded_command(
        backgrounding_shell(&pid_file, "wait"),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    assert!(!output.success);
    assert_eq!(output.status_code, None);
    assert!(String::from_utf8_lossy(&output.stderr).contains("Forge stopped this command"));
    assert_gone(background_pid(&pid_file).await).await;
}

#[tokio::test]
async fn a_finished_command_leaves_nothing_running() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let output = run_bounded_command(
        backgrounding_shell(&pid_file, "echo done"),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(output.success);
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "done");
    assert_gone(background_pid(&pid_file).await).await;
}

#[tokio::test]
async fn a_cancelled_turn_kills_the_running_command() {
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    // The runtime cancels a tool by dropping its future.
    let dropped = tokio::time::timeout(
        Duration::from_millis(500),
        run_bounded_command(
            backgrounding_shell(&pid_file, "wait"),
            Duration::from_secs(60),
        ),
    )
    .await;
    assert!(
        dropped.is_err(),
        "the command should still have been running"
    );
    assert_gone(background_pid(&pid_file).await).await;
}
