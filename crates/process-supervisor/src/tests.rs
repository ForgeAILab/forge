use super::*;

fn shell(text: &str) -> Command {
    let mut command = Command::new("bash");
    command.arg("-c").arg(text);
    command
}

fn alive(pid: &str) -> bool {
    std::process::Command::new("kill")
        .args(["-0", pid.trim()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

/// A descendant that left the group (`setsid`) cannot be signalled through it.
/// It must neither stall a finished command nor turn its exit into a timeout.
#[tokio::test]
async fn a_finished_command_is_not_held_by_a_descendant_outside_its_group() {
    if !std::process::Command::new("perl")
        .args(["-MPOSIX", "-e", "1"])
        .status()
        .is_ok_and(|status| status.success())
    {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut command = shell(
        "perl -MPOSIX -e 'POSIX::setsid(); open(F, \">escaped.pid\"); print F $$; close(F); sleep 30' & \
         while [ ! -s escaped.pid ]; do sleep 0.01; done; echo done; exit 7",
    );
    command.current_dir(dir.path());
    let started = Instant::now();
    let output = run(
        &mut command,
        Capture::Tail(4096),
        // Shorter than the post-exit drain: the exit status still stands.
        Some(Instant::now() + Duration::from_secs(1)),
        &CancellationToken::new(),
        CompletionPolicy::StopDescendants,
    )
    .await
    .unwrap();
    let pid = std::fs::read_to_string(dir.path().join("escaped.pid")).unwrap();
    let escaped = alive(&pid);
    let _ = std::process::Command::new("kill")
        .args(["-KILL", pid.trim()])
        .status();
    assert_eq!(output.termination, Termination::Exited);
    assert_eq!(output.status.code(), Some(7));
    assert_eq!(output.stdout, b"done\n");
    assert!(output.stdout_drain_incomplete && output.stderr_drain_incomplete);
    assert!(started.elapsed() < Duration::from_secs(10));
    // The platform limit: a process outside the group is not ours to signal.
    assert!(escaped);
}

/// `DrainFor` leaves the group alone after a normal exit; `StopDescendants`
/// stops it, including a member that ignores SIGTERM.
#[tokio::test]
async fn completion_policy_decides_what_outlives_a_normal_exit() {
    for (policy, survives) in [
        (CompletionPolicy::DrainFor(Duration::from_millis(200)), true),
        (CompletionPolicy::StopDescendants, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("child.sh"),
            "trap '' TERM\necho $$ > child.pid\nwhile :; do sleep 1; done\n",
        )
        .unwrap();
        let mut command =
            shell("bash child.sh >/dev/null 2>&1 & while [ ! -s child.pid ]; do sleep 0.01; done");
        command.current_dir(dir.path());
        let output = run(
            &mut command,
            Capture::All,
            None,
            &CancellationToken::new(),
            policy,
        )
        .await
        .unwrap();
        assert!(output.status.success());
        let pid = std::fs::read_to_string(dir.path().join("child.pid")).unwrap();
        let survived = alive(&pid);
        let _ = std::process::Command::new("kill")
            .args(["-KILL", pid.trim()])
            .status();
        assert_eq!(survived, survives, "{policy:?}");
        assert_eq!(output.descendants_stopped, !survives);
    }
}
