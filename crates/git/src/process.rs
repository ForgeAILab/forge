//! Cancellation owns the complete Git process group, including hooks.
//!
//! Every Git command runs in its own process group. Finishing normally leaves
//! the group alone. Dropping the future (cancel or timeout) stops the group:
//! SIGTERM first, so Git removes its own lock files (`index.lock`, ref locks)
//! and leaves a rebase in the stopped state that interrupted-rebase recovery
//! expects, then SIGKILL for anything that ignored it. The group is the
//! child's own, never the caller's.
use std::process::{Output, Stdio};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

/// How long a cancelled Git gets to remove its lock files before SIGKILL.
#[cfg(unix)]
const TERMINATE_GRACE: std::time::Duration = std::time::Duration::from_millis(500);

/// Owns the spawned Git until its exit status has been collected.
struct ProcessGroup {
    child: Child,
    finished: bool,
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // `None` once the child was reaped: its id may have been reused, so
        // the group is no longer addressed.
        let Some(id) = self.child.id() else {
            return;
        };
        #[cfg(unix)]
        self.stop_group(id);
        #[cfg(windows)]
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &id.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(unix)]
impl ProcessGroup {
    /// Blocks for at most [`TERMINATE_GRACE`] plus two `kill` invocations;
    /// Git exits within a few milliseconds of SIGTERM. The crate forbids
    /// `unsafe`, so the group is signalled through `kill(1)`.
    fn stop_group(&mut self, leader: u32) {
        // 0 and 1 would address the caller's own group or every process.
        if leader <= 1 {
            return;
        }
        if signal_group("TERM", leader) {
            let deadline = std::time::Instant::now() + TERMINATE_GRACE;
            // An exit or a wait error both end the grace period.
            while matches!(self.child.try_wait(), Ok(None)) && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        }
        // Whatever ignored SIGTERM (a hook, or Git past the grace period).
        signal_group("KILL", leader);
        // Backstop for Git itself should `kill(1)` be unavailable.
        let _ = self.child.start_kill();
    }
}

#[cfg(unix)]
fn signal_group(signal: &str, leader: u32) -> bool {
    std::process::Command::new("kill")
        .args([&format!("-{signal}"), "--", &format!("-{leader}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

async fn read_all(pipe: Option<impl tokio::io::AsyncRead + Unpin>) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if let Some(mut pipe) = pipe {
        pipe.read_to_end(&mut bytes).await?;
    }
    Ok(bytes)
}

pub(crate) async fn output(command: &mut Command) -> std::io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The group guard stops Unix children itself, SIGTERM first. Letting the
    // runtime SIGKILL Git on drop would leave its lock files behind.
    #[cfg(unix)]
    command.process_group(0).kill_on_drop(false);
    #[cfg(not(unix))]
    command.kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let mut group = ProcessGroup {
        child,
        finished: false,
    };
    let (status, stdout, stderr) =
        tokio::try_join!(group.child.wait(), read_all(stdout), read_all(stderr))?;
    // Hooks that outlive a finished Git are not ours to stop.
    group.finished = true;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;

    /// An orphaned hook can linger as a zombie where nothing reaps orphans
    /// (a container without an init); a zombie has stopped.
    fn alive(pid: &str) -> bool {
        let pid = pid.trim();
        let exists = std::process::Command::new("kill")
            .args(["-0", pid])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success();
        let zombie = std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, tail)| tail.starts_with('Z'))
        });
        exists && !zombie
    }

    async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    fn install_hook(repo: &Path, name: &str, body: &str) {
        let hook = repo.join(".git/hooks").join(name);
        std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
        std::fs::write(&hook, body).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// `git commit -a` holds `index.lock` while its pre-commit hook runs, so
    /// cancelling there is the case a plain SIGKILL gets wrong.
    #[tokio::test]
    async fn cancelling_git_stops_its_hook_and_leaves_no_index_lock() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        crate::init(repo).await.unwrap();
        std::fs::write(repo.join("file"), "one\n").unwrap();
        crate::commit_all(repo, "initial").await.unwrap();
        let head = crate::get_current_sha(repo).await.unwrap();
        std::fs::write(repo.join("file"), "two\n").unwrap();
        install_hook(
            repo,
            "pre-commit",
            "#!/bin/sh\necho $$ > hook.pid\necho $PPID > git.pid\nexec sleep 60\n",
        );

        let lock = repo.join(".git/index.lock");
        {
            let commit = crate::run_git(repo, &["commit", "-am", "cancelled"]);
            tokio::pin!(commit);
            tokio::select! {
                result = &mut commit => panic!("commit finished: {result:?}"),
                _ = wait_for("the hook to start", || repo.join("git.pid").exists()) => {}
            }
            assert!(lock.exists(), "the hook runs under the index lock");
        }

        // Dropping the future returned only after the group was stopped.
        assert!(!lock.exists(), "cancelled Git left its index lock behind");
        for file in ["git.pid", "hook.pid"] {
            let pid = std::fs::read_to_string(repo.join(file)).unwrap();
            wait_for(file, || !alive(&pid)).await;
        }
        assert_eq!(crate::get_current_sha(repo).await.unwrap(), head);

        // The checkout is usable at once: no stale lock to clear by hand.
        std::fs::remove_file(repo.join(".git/hooks/pre-commit")).unwrap();
        crate::run_git(repo, &["commit", "-am", "after cancel"])
            .await
            .unwrap();
        assert_ne!(crate::get_current_sha(repo).await.unwrap(), head);
    }

    /// A hook that ignores SIGTERM is still gone once the future is dropped.
    #[tokio::test]
    async fn a_hook_that_ignores_sigterm_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        crate::init(repo).await.unwrap();
        std::fs::write(repo.join("file"), "one\n").unwrap();
        crate::commit_all(repo, "initial").await.unwrap();
        std::fs::write(repo.join("file"), "two\n").unwrap();
        install_hook(
            repo,
            "pre-commit",
            "#!/bin/sh\ntrap '' TERM\necho $$ > hook.pid\nwhile :; do sleep 1; done\n",
        );
        {
            let commit = crate::run_git(repo, &["commit", "-am", "cancelled"]);
            tokio::pin!(commit);
            tokio::select! {
                result = &mut commit => panic!("commit finished: {result:?}"),
                _ = wait_for("the hook to start", || repo.join("hook.pid").exists()) => {}
            }
        }
        let pid = std::fs::read_to_string(repo.join("hook.pid")).unwrap();
        wait_for("the hook to die", || !alive(&pid)).await;
    }

    /// An uncancelled command is untouched: output, exit status and cwd.
    #[tokio::test]
    async fn an_uncancelled_command_keeps_its_output_and_exit_status() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        crate::init(repo).await.unwrap();
        let top = crate::run_git(repo, &["rev-parse", "--show-toplevel"])
            .await
            .unwrap();
        assert_eq!(
            std::fs::canonicalize(top.trim()).unwrap(),
            std::fs::canonicalize(repo).unwrap()
        );
        let error = crate::run_git(repo, &["rev-parse", "--verify", "refs/heads/absent"])
            .await
            .unwrap_err();
        assert!(matches!(error, crate::GitError::CommandFailed { .. }));
    }
}
