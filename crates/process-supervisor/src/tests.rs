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

/// A drain bound is wall-clock. Bytes the finished command wrote before the
/// bound must survive a reader that was not scheduled in time to read them.
#[tokio::test]
async fn a_slow_reader_still_takes_what_the_finished_command_wrote() {
    for _ in 0..20 {
        // More than a pipe buffer, so the tail is still in the pipe when the
        // leader exits, and a zero bound that ends the wait at once.
        let mut command = shell("head -c 200000 /dev/zero | tr '\\0' x; printf END");
        let output = run(
            &mut command,
            Capture::All,
            None,
            &CancellationToken::new(),
            CompletionPolicy::DrainFor(Duration::ZERO),
        )
        .await
        .unwrap();
        assert_eq!(output.termination, Termination::Exited);
        assert_eq!(output.stdout.len(), 200_003);
        assert!(output.stdout.ends_with(b"END"));
        assert!(!output.stdout_drain_incomplete);
    }
}

/// The reader is starved past the bound while a descendant still holds the
/// pipe: what was written before the bound is kept, and the command is not
/// held until the descendant closes it.
#[tokio::test]
async fn a_starved_reader_keeps_bytes_written_before_the_bound() {
    let mut command = shell("(sleep 0.2; printf late; sleep 5) & printf early");
    let started = Instant::now();
    let cancel = CancellationToken::new();
    let (output, ()) = tokio::join!(
        run(
            &mut command,
            Capture::All,
            None,
            &cancel,
            CompletionPolicy::DrainFor(Duration::from_millis(600)),
        ),
        async {
            // Hold the only runtime thread across the write and the bound.
            tokio::time::sleep(Duration::from_millis(100)).await;
            std::thread::sleep(Duration::from_millis(900));
        }
    );
    let output = output.unwrap();
    assert_eq!(output.termination, Termination::Exited);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "earlylate");
    assert!(output.stdout_drain_incomplete);
    assert!(started.elapsed() < Duration::from_secs(4));
}
