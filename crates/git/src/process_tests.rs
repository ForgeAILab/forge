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
