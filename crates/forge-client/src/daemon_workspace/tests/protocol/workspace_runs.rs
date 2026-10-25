//! Workspace run budgets, output, preemption cleanup, and compiler caches.

use super::*;

#[tokio::test]
async fn ci_unbounded_sentinels_do_not_remove_other_purpose_budgets() {
    let fixture = Fixture::new().await;
    let backend = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        WorkspaceRunPolicy {
            allowed_purposes: vec![
                WorkspaceRunPurpose::CiStep,
                WorkspaceRunPurpose::Hook,
                WorkspaceRunPurpose::EnvironmentSetup,
            ],
        },
        Arc::clone(&fixture.journal),
    )
    .unwrap();
    for (index, (timeout_secs, max_output_bytes)) in
        [(0, 4096), (2, u64::MAX)].into_iter().enumerate()
    {
        let mut params = fixture.run(
            &format!("hook-budget-{index}"),
            WorkspaceRunPurpose::Hook,
            "printf unexpected > forbidden-hook",
        );
        params.timeout_secs = timeout_secs;
        params.max_output_bytes = max_output_bytes;
        assert_eq!(
            backend
                .handle(
                    METHOD_WORKSPACE_RUN,
                    serde_json::to_value(params).unwrap(),
                    Vec::new
                )
                .await
                .unwrap_err()
                .code,
            INVALID_INPUT
        );
    }
    assert!(!fixture.path().join("forbidden-hook").exists());
}

#[tokio::test]
async fn ci_output_is_bounded_redacted_and_replayed_only_before_ack() {
    let fixture = Fixture::new().await;
    let journal = Arc::new(DaemonJournal::with_limits(fixture.dir.path(), 1024, 8192));
    let backend = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        Arc::clone(&journal),
    )
    .unwrap();
    let mut params = fixture.run("unbounded-ci", WorkspaceRunPurpose::CiStep, "head -c 1100000 /dev/zero | tr '\\000' x; printf '%s' \"$FORGE_TEST_SECRET\"; printf stderr >&2");
    params.timeout_secs = 0;
    params.max_output_bytes = u64::MAX;
    params.env = vec![("FORGE_TEST_SECRET".into(), "secret-value".into())];
    let request = serde_json::to_value(params).unwrap();
    let result = backend
        .handle(METHOD_WORKSPACE_RUN, request.clone(), Vec::new)
        .await
        .unwrap();
    let stored_bytes: u64 = std::fs::read_dir(journal.directory())
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    assert!(
        stored_bytes <= 8192,
        "receipt and workspace registry share the byte cap: {stored_bytes}, files: {:?}",
        std::fs::read_dir(journal.directory())
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), entry.metadata().unwrap().len())
            })
            .collect::<Vec<_>>()
    );
    let output: WorkspaceRunResult = decode(result.clone()).unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert!(output.stdout.len() <= crate::daemon_persistence::MAX_CI_LOG_BYTES);
    assert!(output
        .stdout
        .starts_with(crate::daemon_persistence::CI_LOG_TRUNCATION_MARKER));
    assert!(!output.stdout.contains("secret-value"));
    assert_eq!(output.stderr, "stderr");
    assert!(!output.timed_out && output.stdout_truncated && !output.stderr_truncated);
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().to_owned(),
        "daemon-1".into(),
        backend.policy.clone(),
        Arc::clone(&journal),
    )
    .unwrap();
    assert_eq!(
        restarted
            .handle(METHOD_WORKSPACE_RUN, request, Vec::new)
            .await
            .unwrap(),
        result
    );
    journal
        .acknowledge(&JournalAckParams {
            entry_id: output.entry_id,
        })
        .unwrap();
    assert!(journal.operation("unbounded-ci").unwrap().is_none());
}

/// Plan 3.4 F: `workspace.run` in a daemon workspace is handed this
/// machine's shared compiler cache, with the store of the worktree's
/// repository. The operator's own environment wins over Forge's, so on a
/// machine whose environment names a wrapper this asserts that rule instead.
/// The decision for a process that outlives a preempted `workspace.run`:
/// the command's temp directory lives exactly as long as the command, and a
/// process that left the command's process group (its own session: nothing
/// Forge started can signal it, and nothing can know when it ends) must not
/// depend on it. So after the preemption the directory is gone and stays
/// gone: the survivor's writes there fail, they neither bring the directory
/// back nor land anywhere else, and the Task root holds no live run, so the
/// collector and the next command are not held up by a process nobody owns.
/// This is what a normal return already does to a descendant it leaves
/// running.
#[cfg(unix)]
#[tokio::test]
async fn a_process_that_outlives_a_preempted_workspace_run_loses_its_temp_dir() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path().canonicalize().unwrap();
    let pid_file = fixture.dir.path().join("survivor.pid");
    let status_file = fixture.dir.path().join("survivor.status");
    // A descendant in its own session that keeps writing into `$TMPDIR`.
    let survivor = format!(
        "perl -e 'use POSIX; exit if fork; POSIX::setsid(); \
           open(P, \">\", $ARGV[0]); print P \"$$ $ENV{{TMPDIR}}\"; close P; \
           while (1) {{ my $ok = open(A, \">>\", \"$ENV{{TMPDIR}}/alive\"); close A if $ok; \
             open(S, \">\", \"$ARGV[1].new\"); print S ($ok ? \"wrote\" : \"failed\"); close S; \
             rename(\"$ARGV[1].new\", $ARGV[1]); select(undef, undef, undef, 0.02); }}' '{}' '{}'; sleep 60",
        pid_file.display(),
        status_file.display()
    );
    let mut params = fixture.run("outlive-run", WorkspaceRunPurpose::CiStep, &survivor);
    params.timeout_secs = 60;
    let operation = fixture.backend.handle(
        METHOD_WORKSPACE_RUN,
        serde_json::to_value(params).unwrap(),
        Vec::new,
    );
    let status = || std::fs::read_to_string(&status_file).unwrap_or_default();
    let preempt = async {
        let wrote = tokio::time::timeout(Duration::from_secs(20), async {
            while status() != "wrote" {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let cancel = fixture
            .backend
            .cancel_command(WorkspaceCancelParams {
                operation_id: "outlive-run".into(),
            })
            .await
            .unwrap();
        (wrote, cancel)
    };
    let (result, (wrote, cancel)) = tokio::join!(operation, preempt);
    let Ok(recorded) = std::fs::read_to_string(&pid_file) else {
        panic!("the survivor never started: {result:?}");
    };
    let (pid, tmp) = recorded.split_once(' ').unwrap();
    let tmp = PathBuf::from(tmp);
    let kill = |signal: &str| {
        std::process::Command::new("kill")
            .args([signal, pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    };
    let checked = async {
        if !tmp.canonicalize().map_or_else(
            |_| tmp.starts_with(&root) || tmp.starts_with(fixture.dir.path()),
            |tmp| tmp.starts_with(&root),
        ) {
            return Err(format!(
                "not the command's own directory: {}",
                tmp.display()
            ));
        }
        if !kill("-0") {
            return Err("the process in its own session did not survive".to_owned());
        }
        let removed = tokio::time::timeout(Duration::from_secs(10), async {
            while tmp.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if removed.is_err() {
            return Err("the temp directory outlived the preempted command".to_owned());
        }
        Ok(())
    }
    .await;
    // The survivor runs on: its writes now fail and bring nothing back.
    let failed = tokio::time::timeout(Duration::from_secs(10), async {
        while status() != "failed" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let gone = !tmp.exists();
    let task_root = fixture.path().parent().unwrap().to_path_buf();
    let no_live_run = executors::sandbox::unless_live_run_in(&task_root, || ()).is_some();
    kill("-9");
    assert!(
        wrote,
        "the survivor wrote into the command's temp directory"
    );
    assert!(result.is_err(), "the command was preempted");
    assert_eq!(cancel.state, WorkspaceCancelState::Killed);
    checked.unwrap();
    assert!(
        failed.is_ok(),
        "the survivor's writes fail once the directory is gone"
    );
    assert!(gone, "nothing brought the temp directory back");
    assert!(
        no_live_run,
        "the Task root holds no live run after the preemption"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_run_gets_this_machines_shared_compiler_cache() {
    let fixture = Fixture::new().await;
    let root = fixture.dir.path();
    let worktree = root
        .join(WORKTREE_DIRECTORY)
        .join(&fixture.prepared.workspace.workspace_handle)
        .join("repo");
    let repository =
        executors::compiler_cache::repository_id(&worktree).expect("a linked worktree");
    let wrapper = root.join("kache");
    process_supervisor::executable::write_executable(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
    let cache_dir = root.join(executors::compiler_cache::CACHE_DIR);
    executors::compiler_cache::install(
        root,
        Some(executors::compiler_cache::CompilerCache {
            kind: executors::compiler_cache::WrapperKind::of(&wrapper),
            wrapper: wrapper.clone(),
            dir: cache_dir.clone(),
            max_bytes: 1 << 30,
        }),
    );
    let params = fixture.run(
        "cache-run",
        WorkspaceRunPurpose::CiStep,
        "printf '%s|%s' \"$RUSTC_WRAPPER\" \"$KACHE_CACHE_DIR\"",
    );
    let result = fixture
        .backend
        .handle(
            METHOD_WORKSPACE_RUN,
            serde_json::to_value(params).unwrap(),
            Vec::new,
        )
        .await
        .unwrap();
    executors::compiler_cache::install(root, None);
    let output: WorkspaceRunResult = decode(result).unwrap();
    assert_eq!(output.exit_code, Some(0));
    let operator = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
    let (seen, wrapper) = (output.stdout.as_str(), wrapper.to_str().unwrap());
    if operator("RUSTC_WRAPPER") {
        assert!(!seen.starts_with(wrapper), "{seen}");
    } else if operator("KACHE_CACHE_DIR") {
        assert!(seen.starts_with(&format!("{wrapper}|")), "{seen}");
    } else {
        assert_eq!(
            seen,
            format!("{wrapper}|{}", cache_dir.join(repository).display())
        );
    }
}
