use super::*;
fn step(text: &str) -> CheckCommandSpec {
    CheckCommandSpec {
        id: "check".into(),
        shell_text: text.into(),
        shell: "bash -lc".into(),
        working_directory: CheckWorkingDirectory::TaskRoot,
        environment_keys: Default::default(),
        timeout_seconds: None,
        failure_policy: CheckFailurePolicy::StopBundle,
        cacheability: CheckCacheability::Uncacheable,
        requirement_ids: Default::default(),
    }
}
fn spec(text: &str) -> CheckSpec {
    CheckSpec {
        schema_revision: CHECK_SPEC_REVISION,
        scope: CheckScope::Commit,
        commands: vec![step(text)],
        declares_cleanup: false,
        configured_commands: 1,
        blank_commands: vec![],
        execution_policy: "owner-check/1".into(),
    }
}
fn owner() -> CheckOwnerIdentity {
    CheckOwnerIdentity {
        owner_kind: "server".into(),
        machine_id: None,
        runtime_id: "test".into(),
    }
}
async fn run(
    path: &Path,
    spec: &CheckSpec,
    cleanup: &[CheckCommandSpec],
    timeout: Duration,
    cancel: &CancellationToken,
) -> CheckReceipt {
    execute(CheckExecution {
        operation_id: "test",
        spec,
        target: CheckoutTarget::Workspace(path),
        owner: owner(),
        input_revisions: None,
        environment: &BTreeMap::new(),
        deadline: Some(Instant::now() + timeout),
        cancel,
        permit: &CheckPermit::already_admitted(),
        cleanup: CleanupPlan {
            commands: cleanup,
            timeout: Duration::from_millis(250),
        },
        output_limit: 512,
    })
    .await
}
#[tokio::test]
async fn fifty_megabytes_per_stream_are_drained_and_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = run(temp.path(),&spec("head -c 52428800 /dev/zero; head -c 52428800 /dev/zero >&2; printf tail; printf TAIL >&2"),&[],Duration::from_secs(30),&CancellationToken::new()).await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    let command = &receipt.commands[0];
    assert!(command.stdout_truncated && command.stderr_truncated);
    assert!(command.stdout_tail.len() <= 512 && command.stderr_tail.len() <= 512);
    assert!(command.stdout_tail.ends_with("tail") && command.stderr_tail.ends_with("TAIL"));
}
#[tokio::test]
async fn cleanup_runs_after_pass_fail_timeout_and_cancel_and_is_bounded() {
    let temp = tempfile::tempdir().unwrap();
    for (text, cancelled, expected) in [
        ("true", false, CheckExecutionOutcome::Passed),
        ("exit 7", false, CheckExecutionOutcome::Failed),
        ("sleep 60", false, CheckExecutionOutcome::TimedOut),
        ("sleep 60", true, CheckExecutionOutcome::Cancelled),
    ] {
        let cancel = CancellationToken::new();
        if cancelled {
            let signal = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                signal.cancel();
            });
        }
        let mut bundle = spec(text);
        bundle.declares_cleanup = true;
        let receipt = run(
            temp.path(),
            &bundle,
            &[step("printf cleaned > marker")],
            Duration::from_millis(200),
            &cancel,
        )
        .await;
        assert_eq!(receipt.outcome, expected, "{text}");
        assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::Success);
        assert_eq!(
            std::fs::read_to_string(temp.path().join("marker")).unwrap(),
            "cleaned"
        );
        std::fs::remove_file(temp.path().join("marker")).unwrap();
    }
    let mut bundle = spec("true");
    bundle.declares_cleanup = true;
    let start = Instant::now();
    let receipt = run(
        temp.path(),
        &bundle,
        &[step("sleep 60")],
        Duration::from_secs(5),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::TimedOut);
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Infrastructure);
    assert!(start.elapsed() < Duration::from_secs(3));
}
#[tokio::test]
async fn command_limit_is_clamped_to_remaining_wall_time() {
    let temp = tempfile::tempdir().unwrap();
    let mut bundle = spec("sleep 0.15");
    let mut second = step("sleep 60");
    second.id = "second".into();
    second.timeout_seconds = Some(60);
    bundle.commands.push(second);
    bundle.configured_commands = 2;
    let receipt = run(
        temp.path(),
        &bundle,
        &[],
        Duration::from_millis(400),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::TimedOut);
    assert_eq!(receipt.commands.len(), 2);
    assert_eq!(receipt.commands[1].outcome, CheckExecutionOutcome::TimedOut);
    assert!(receipt.commands[1].duration_ms < 1500);
}
#[tokio::test]
async fn declared_environment_and_local_budget_only_and_redacted_tail() {
    let temp = tempfile::tempdir().unwrap();
    let mut bundle=spec("test -z \"${UNDECLARED+x}\" && test -z \"${FORGE_CHECK_INHERITED_SENTINEL+x}\" && printf '%s' \"$SECRET\" && printf '|%s|%s' \"$CARGO_BUILD_JOBS\" \"$MAKEFLAGS\"");
    bundle.commands[0].environment_keys.insert("SECRET".into());
    let env = BTreeMap::from([
        ("SECRET".into(), "super-secret-value".into()),
        ("UNDECLARED".into(), "wrong".into()),
    ]);
    let (command, filtered) = command(
        temp.path(),
        &bundle.commands[0],
        &env,
        &bundle.execution_policy,
    )
    .unwrap();
    assert_eq!(filtered.len(), 1);
    assert!(!command.as_std().get_envs().any(|(k, _)| k == "UNDECLARED"));
    for key in executors::run_process::BUILD_ENV_KEYS {
        assert!(command.as_std().get_envs().any(|(k, _)| k == key));
    }
    std::env::set_var("FORGE_CHECK_INHERITED_SENTINEL", "must-not-leak");
    let receipt = run_command(
        temp.path(),
        &bundle.commands[0],
        &env,
        &bundle.execution_policy,
        Some(Instant::now() + Duration::from_secs(5)),
        &CancellationToken::new(),
        512,
    )
    .await
    .unwrap();
    std::env::remove_var("FORGE_CHECK_INHERITED_SENTINEL");
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert!(receipt.stdout_tail.contains("[REDACTED]"));
    assert!(!receipt.stdout_tail.contains("super-secret"));
    assert_eq!(
        redacted(b"ret-value", true, false, &env, 512).0,
        "[REDACTED]"
    );
    let overlapping = BTreeMap::from([
        ("A_SHORT".into(), "abc".into()),
        ("Z_LONG".into(), "abcXYZ".into()),
    ]);
    assert_eq!(
        redacted(b"abcXYZ", true, false, &overlapping, 512).0,
        "[REDACTED]"
    );
    let repeated = BTreeMap::from([("ONLY".into(), "abcabc".into())]);
    assert_eq!(
        redacted(b"abcabc", false, true, &repeated, 512).0,
        "[REDACTED]"
    );
    let partial = BTreeMap::from([
        ("A_SHORT".into(), "sup".into()),
        ("Z_LONG".into(), "super-secret".into()),
    ]);
    assert_eq!(
        redacted(b"super-", false, true, &partial, 512).0,
        "[REDACTED]"
    );
}
#[tokio::test]
async fn managed_checkout_is_exact_root_scoped_and_removed_without_touching_source() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    std::fs::create_dir(&source).unwrap();
    git::init(&source).await.unwrap();
    std::fs::write(source.join("file"), "first").unwrap();
    git::commit_all(&source, "first").await.unwrap();
    let commit = git::get_current_sha(&source).await.unwrap();
    std::fs::write(source.join("file"), "second").unwrap();
    git::commit_all(&source, "second").await.unwrap();
    let head = git::get_current_sha(&source).await.unwrap();
    std::fs::write(source.join("untracked"), "keep").unwrap();
    let task = temp.path().join("task");
    assert!(git::command_output(
        &source,
        &[
            "worktree",
            "add",
            "--detach",
            task.to_str().unwrap(),
            &commit
        ]
    )
    .await
    .unwrap()
    .status
    .success());
    std::fs::write(task.join("file"), "task-dirt").unwrap();
    let build = temp.path().join("build/checks");
    let mut bundle=spec("test \"$(cat file)\" = first && test ! -f untracked && test \"$(git rev-parse --show-toplevel)\" = \"$PWD\" && printf checked");
    // Canonicalize PWD on macOS where the OS temp directory may be a symlink.
    bundle.commands[0].shell_text="test \"$(cat file)\" = first && test ! -f untracked && test \"$(cd \"$(git rev-parse --show-toplevel)\" && pwd -P)\" = \"$(pwd -P)\" && printf checked".into();
    let revisions = ServerCheckExecutionInputs {
        toolchain_revision: "test-tools".into(),
        environment_revision: "empty".into(),
        asset_revisions: Default::default(),
        secret_revisions: Default::default(),
        shell_revision: "bash".into(),
        runner_revision: "owner-check/1".into(),
    };
    let receipt = execute(CheckExecution {
        operation_id: "managed",
        spec: &bundle,
        target: CheckoutTarget::ExactCommit {
            repository: &source,
            build_area: &build,
            commit: &commit,
        },
        owner: owner(),
        input_revisions: Some(&revisions),
        environment: &BTreeMap::new(),
        deadline: Some(Instant::now() + Duration::from_secs(10)),
        cancel: &CancellationToken::new(),
        permit: &CheckPermit::already_admitted(),
        cleanup: CleanupPlan {
            commands: &[],
            timeout: CLEANUP_TIMEOUT,
        },
        output_limit: 512,
    })
    .await;
    assert_eq!(
        receipt.outcome,
        CheckExecutionOutcome::Passed,
        "{receipt:?}"
    );
    assert_eq!(receipt.prepared_head.as_deref(), Some(commit.as_str()));
    assert_eq!(receipt.execution_inputs, revisions.identity().unwrap());
    assert!(receipt.cleanup.checkout_removed);
    assert_eq!(std::fs::read_dir(build).unwrap().count(), 0);
    assert_eq!(git::get_current_sha(&source).await.unwrap(), head);
    assert_eq!(
        std::fs::read_to_string(source.join("file")).unwrap(),
        "second"
    );
    assert_eq!(
        std::fs::read_to_string(source.join("untracked")).unwrap(),
        "keep"
    );
    assert_eq!(git::get_current_sha(&task).await.unwrap(), commit);
    assert_eq!(
        std::fs::read_to_string(task.join("file")).unwrap(),
        "task-dirt"
    );
}
#[cfg(unix)]
fn alive(pid: &str) -> bool {
    let exists = std::process::Command::new("kill")
        .args(["-0", pid.trim()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap()
        .success();
    let zombie = std::fs::read_to_string(format!("/proc/{}/stat", pid.trim())).is_ok_and(|s| {
        s.rsplit_once(") ")
            .is_some_and(|(_, tail)| tail.starts_with('Z'))
    });
    exists && !zombie
}
#[cfg(unix)]
#[tokio::test]
async fn grandchild_ignoring_term_is_gone_after_timeout_and_cancel() {
    for cancelled in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();
        std::fs::write(
            temp.path().join("grandchild.sh"),
            "trap '' TERM\necho $$ > grandchild.pid\nwhile :; do sleep 1; done\n",
        )
        .unwrap();
        let bundle = spec("bash -c 'bash grandchild.sh & wait' & wait");
        let timer = async {
            if !cancelled {
                return;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while !temp.path().join("grandchild.pid").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            cancel.cancel();
        };
        let (receipt, ()) = tokio::join!(
            run(temp.path(), &bundle, &[], Duration::from_secs(2), &cancel),
            timer
        );
        assert_eq!(
            receipt.outcome,
            if cancelled {
                CheckExecutionOutcome::Cancelled
            } else {
                CheckExecutionOutcome::TimedOut
            },
            "{receipt:?}"
        );
        let pid = std::fs::read_to_string(temp.path().join("grandchild.pid")).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while alive(&pid) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(receipt.commands[0].process_tree_stopped);
    }
}

#[tokio::test]
async fn cleanup_runs_after_command_preparation_failure_and_cancellation_before_start() {
    let temp = tempfile::tempdir().unwrap();
    let mut bundle = spec("never");
    bundle.declares_cleanup = true;
    bundle.commands[0].shell = "missing shell".into();
    let receipt = run(
        temp.path(),
        &bundle,
        &[step("printf cleanup > marker")],
        Duration::from_secs(2),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Infrastructure);
    assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::Success);
    std::fs::remove_file(temp.path().join("marker")).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let receipt = run(
        temp.path(),
        &bundle,
        &[step("printf cleanup > marker")],
        Duration::from_secs(2),
        &cancel,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Cancelled);
    assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::Success);
    assert!(receipt.commands.is_empty());
    assert!(temp.path().join("marker").exists());
}

#[tokio::test]
async fn failed_continue_policy_preserves_failure_and_runs_later_commands() {
    let temp = tempfile::tempdir().unwrap();
    let mut bundle = spec("exit 3");
    bundle.commands[0].failure_policy = CheckFailurePolicy::Continue;
    let mut second = step("printf later > marker");
    second.id = "later".into();
    bundle.commands.push(second);
    bundle.configured_commands = 2;
    let receipt = run(
        temp.path(),
        &bundle,
        &[],
        Duration::from_secs(2),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Failed);
    assert_eq!(receipt.commands.len(), 2);
    assert_eq!(receipt.commands[0].exit_code, Some(3));
    assert_eq!(receipt.commands[1].outcome, CheckExecutionOutcome::Passed);
}

#[cfg(unix)]
#[tokio::test]
async fn descendants_are_stopped_even_when_the_leader_exits_successfully() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("child.sh"),
        "trap '' TERM\necho $$ > child.pid\nwhile :; do sleep 1; done\n",
    )
    .unwrap();
    let receipt = run(
        temp.path(),
        &spec("bash child.sh & while [ ! -f child.pid ]; do sleep 0.01; done; exit 0"),
        &[],
        Duration::from_secs(3),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    let pid = std::fs::read_to_string(temp.path().join("child.pid")).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while alive(&pid) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn timeout_and_cancel_retain_the_term_trap_output() {
    for cancelled in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let token = CancellationToken::new();
        let bundle =
            spec("trap 'printf final; exit 0' TERM; printf started; touch ready; sleep 60 & wait");
        let cancel = async {
            if !cancelled {
                return;
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while !temp.path().join("ready").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            token.cancel();
        };
        let (receipt, ()) = tokio::join!(
            run(
                temp.path(),
                &bundle,
                &[],
                Duration::from_millis(500),
                &token
            ),
            cancel
        );
        assert_eq!(
            receipt.outcome,
            if cancelled {
                CheckExecutionOutcome::Cancelled
            } else {
                CheckExecutionOutcome::TimedOut
            }
        );
        assert!(
            receipt.commands[0].stdout_tail.ends_with("final"),
            "{receipt:?}"
        );
    }
}

#[tokio::test]
async fn an_undeclared_cleanup_plan_never_executes_a_command() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = run(
        temp.path(),
        &spec("touch ran"),
        &[step("touch cleaned")],
        Duration::from_secs(2),
        &CancellationToken::new(),
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Infrastructure);
    assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::NotPerformed);
    assert!(receipt.commands.is_empty());
    assert!(!temp.path().join("ran").exists() && !temp.path().join("cleaned").exists());
}

/// Neither owner ever stopped what a CI step left running: a later step may
/// depend on it. A service that kept the step's output pipe open does not
/// stall the step either.
#[cfg(unix)]
#[tokio::test]
async fn frozen_ci_policies_keep_a_background_service_for_the_next_step() {
    for daemon in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let env = BTreeMap::new();
        let run = |text: &'static str| {
            let path = temp.path().to_owned();
            let env = env.clone();
            async move {
                execute(CheckExecution {
                    operation_id: "legacy",
                    spec: &legacy_ci_spec(text, &env, 0, daemon),
                    target: CheckoutTarget::Workspace(&path),
                    owner: owner(),
                    input_revisions: None,
                    environment: &env,
                    deadline: None,
                    cancel: &CancellationToken::new(),
                    permit: &CheckPermit::already_admitted(),
                    cleanup: CleanupPlan {
                        commands: &[],
                        timeout: CLEANUP_TIMEOUT,
                    },
                    output_limit: 4096,
                })
                .await
            }
        };
        let started = Instant::now();
        // The service inherits stdout and stderr and outlives the step.
        let first = run("sleep 30 & echo $! > service.pid; echo started").await;
        assert_eq!(first.outcome, CheckExecutionOutcome::Passed, "{first:?}");
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_eq!(first.commands[0].stdout_tail, "started\n");
        assert!(first.commands[0].stdout_drain_incomplete);
        assert!(!first.commands[0].process_tree_stopped);
        // No witness Git command runs beside a frozen CI step.
        assert!(first.prepared_head.is_none() && first.tracked_changes.is_none());
        let second = run("kill -0 \"$(cat service.pid)\"").await;
        let pid = std::fs::read_to_string(temp.path().join("service.pid")).unwrap();
        let _ = std::process::Command::new("kill")
            .args(["-KILL", pid.trim()])
            .status();
        assert_eq!(second.outcome, CheckExecutionOutcome::Passed, "{second:?}");
    }
}

#[test]
fn redaction_leaves_short_edge_matches_alone_and_masks_cut_secrets() {
    let env = BTreeMap::from([
        ("MODE".into(), "production".into()),
        ("FLAG".into(), "x1".into()),
        ("TOKEN".into(), "0123456789abcdef".into()),
    ]);
    // Truncated and interrupted output that merely starts or ends with a few
    // bytes of some value is not rewritten.
    assert_eq!(
        redacted(b"1 test failed: pro", true, true, &env, 512).0,
        "1 test failed: pro"
    );
    assert_eq!(
        redacted(b"cdef failed 0123", true, true, &env, 512).0,
        "[REDACTED] failed [REDACTED]"
    );
}

async fn git(path: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "fixture Git command failed");
}
async fn candidate() -> (tempfile::TempDir, String) {
    let temp = tempfile::tempdir().unwrap();
    git(temp.path(), &["init", "-q"]).await;
    git(temp.path(), &["config", "user.name", "Check test"]).await;
    git(
        temp.path(),
        &["config", "user.email", "check@example.invalid"],
    )
    .await;
    std::fs::write(temp.path().join("tracked"), "one\n").unwrap();
    std::fs::write(temp.path().join(".gitignore"), "ignored\n").unwrap();
    git(temp.path(), &["add", "."]).await;
    git(temp.path(), &["commit", "-q", "-m", "candidate"]).await;
    let head = git::get_current_sha(temp.path()).await.unwrap();
    (temp, head)
}
fn canonical_spec(steps: &[&str]) -> CheckSpec {
    let mut spec = legacy_ci_bundle(
        &steps.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(),
        &BTreeMap::new(),
        0,
        false,
    );
    spec.execution_policy = CANONICAL_CI_POLICY.into();
    for command in &mut spec.commands {
        command.cacheability = CheckCacheability::DeclaredControlledInputs;
    }
    spec
}
async fn canonical_run(
    path: &Path,
    spec: &CheckSpec,
    env: &BTreeMap<String, String>,
    seconds: u64,
) -> CheckReceipt {
    let inputs = ServerCheckExecutionInputs {
        toolchain_revision: "t".into(),
        environment_revision: "e".into(),
        asset_revisions: BTreeMap::new(),
        secret_revisions: BTreeMap::new(),
        shell_revision: "s".into(),
        runner_revision: "r".into(),
    };
    execute(CheckExecution {
        operation_id: "canonical",
        spec,
        target: CheckoutTarget::Workspace(path),
        owner: owner(),
        input_revisions: Some(&inputs),
        environment: env,
        deadline: Some(Instant::now() + Duration::from_secs(seconds)),
        cancel: &CancellationToken::new(),
        permit: &CheckPermit::already_admitted(),
        cleanup: CleanupPlan {
            commands: &[],
            timeout: Duration::from_millis(250),
        },
        output_limit: 4096,
    })
    .await
}
#[tokio::test]
async fn a_canonical_run_on_a_clean_checkout_returns_the_full_witness_and_attestation() {
    let (temp, head) = candidate().await;
    // An ignored build product is not a change to the checkout.
    let receipt = canonical_run(
        temp.path(),
        &canonical_spec(&["echo built > ignored"]),
        &BTreeMap::new(),
        30,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(receipt.prepared_head.as_deref(), Some(head.as_str()));
    assert_eq!(receipt.finished_head, receipt.prepared_head);
    assert_eq!(receipt.tracked_changes, Some(false));
    assert!(receipt.commands.iter().all(|c| c.process_tree_stopped));
    assert!(matches!(
        receipt.execution_inputs,
        CheckEnvironmentIdentity::Attested { .. }
    ));
}
/// Decision A: a canonical step inherits the owner's environment, exactly as
/// a frozen-policy step does. Nothing is cleared.
#[tokio::test]
async fn a_canonical_step_inherits_the_owner_environment_and_the_project_values() {
    let (temp, _) = candidate().await;
    // Set by the test harness for this process and declared by nobody.
    let ambient = ["CARGO_MANIFEST_DIR", "CARGO_PKG_NAME"]
        .into_iter()
        .find(|key| std::env::var_os(key).is_some())
        .expect("cargo sets its package variables for a test process");
    let env = BTreeMap::from([("DECLARED".to_owned(), "project-value".to_owned())]);
    let script = format!(
        "test -n \"${{{ambient}+x}}\" && test \"$DECLARED\" = project-value && command -v git >/dev/null"
    );
    let receipt = canonical_run(temp.path(), &canonical_spec(&[&script]), &env, 30).await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    // The same variable is in what the owner attests, by name and value...
    let inherited = inherited_environment().await.expect("login shell answers");
    assert!(inherited.values.get(ambient) == std::env::var(ambient).ok().as_ref());
    assert!(inherited.identity_values().contains_key(ambient));
    assert!(!inherited.shell_revision.is_empty());
    // ...and the per-process shell bookkeeping is not.
    assert!(inherited.values.contains_key("PWD") && inherited.values.contains_key("SHLVL"));
    for key in ENVIRONMENT_IDENTITY_DENYLIST {
        assert!(!inherited.identity_values().contains_key(key));
    }
    // A Project value the request did not declare still reaches the step: the
    // run uses the environment in force, and its owner decides reusability.
    let mut spec = canonical_spec(&["test \"$LATER\" = added"]);
    spec.commands[0].environment_keys = ["GONE".to_owned()].into();
    let env = BTreeMap::from([("LATER".to_owned(), "added".to_owned())]);
    let receipt = canonical_run(temp.path(), &spec, &env, 30).await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
}
/// The Forge host's profile is known to replace the login shell's exit code
/// from its logout script, and a profile may print. Neither fails the probe;
/// a profile that never lets the shell answer does, without hanging.
#[tokio::test]
async fn the_environment_probe_survives_a_noisy_profile_and_a_clobbered_exit_code() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join(".bash_profile"),
        "echo profile noise\nexport FORGE_PROBE_FROM_PROFILE='two words'\n",
    )
    .unwrap();
    std::fs::write(home.path().join(".bash_logout"), "echo bye\nexit 7\n").unwrap();
    let inherited = probe_environment(Some(home.path()))
        .await
        .expect("a clobbered exit code is not a failed probe");
    assert_eq!(
        inherited
            .values
            .get("FORGE_PROBE_FROM_PROFILE")
            .map(String::as_str),
        Some("two words")
    );
    // A profile that ends the shell before it answers: nothing is attested.
    std::fs::write(home.path().join(".bash_profile"), "exit 0\n").unwrap();
    assert!(probe_environment(Some(home.path())).await.is_none());
}
#[tokio::test]
async fn a_canonical_witness_reports_a_dirty_start_a_tracked_change_and_a_moved_head() {
    // Untracked work the commit does not contain: not a clean checkout.
    let (temp, _) = candidate().await;
    std::fs::write(temp.path().join("uncommitted"), "x").unwrap();
    let receipt = canonical_run(
        temp.path(),
        &canonical_spec(&["true"]),
        &BTreeMap::new(),
        30,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(receipt.tracked_changes, Some(true));
    // A step that rewrites a tracked file still passes; the witness says so.
    let (temp, _) = candidate().await;
    let receipt = canonical_run(
        temp.path(),
        &canonical_spec(&["echo two >> tracked"]),
        &BTreeMap::new(),
        30,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(receipt.tracked_changes, Some(true));
    // A step that commits moves HEAD.
    let (temp, head) = candidate().await;
    let receipt = canonical_run(
        temp.path(),
        &canonical_spec(&["git commit -q --allow-empty -m moved"]),
        &BTreeMap::new(),
        30,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(receipt.prepared_head.as_deref(), Some(head.as_str()));
    assert_ne!(receipt.finished_head, receipt.prepared_head);
}
/// Decision B: a service one step starts is there for the next step, and the
/// whole run's process tree is stopped, and verified stopped, when the run
/// ends.
#[tokio::test]
async fn a_canonical_service_survives_to_the_next_step_and_is_stopped_at_run_end() {
    let (temp, _) = candidate().await;
    let scratch = tempfile::tempdir().unwrap();
    let pid = scratch.path().join("service.pid");
    let start = format!("sleep 300 >/dev/null 2>&1 & echo $! > '{}'", pid.display());
    // Step 2 runs well after step 1's shell exited and needs the service.
    let uses = format!("sleep 1; kill -0 \"$(cat '{}')\"", pid.display());
    let receipt = canonical_run(
        temp.path(),
        &canonical_spec(&[&start, &uses]),
        &BTreeMap::new(),
        30,
    )
    .await;
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(receipt.commands.len(), 2);
    assert!(receipt.commands.iter().all(|c| c.process_tree_stopped));
    let service = std::fs::read_to_string(&pid).unwrap();
    assert!(!alive(service.trim()), "the run ended: its tree is stopped");
    assert_eq!(receipt.tracked_changes, Some(false));

    // The same at a failed and at a timed-out end.
    for (last, seconds, outcome) in [
        ("false", 30, CheckExecutionOutcome::Failed),
        ("sleep 30", 2, CheckExecutionOutcome::TimedOut),
    ] {
        let _ = std::fs::remove_file(&pid);
        let receipt = canonical_run(
            temp.path(),
            &canonical_spec(&[&start, last]),
            &BTreeMap::new(),
            seconds,
        )
        .await;
        assert_eq!(receipt.outcome, outcome);
        assert!(receipt.commands.iter().all(|c| c.process_tree_stopped));
        let service = std::fs::read_to_string(&pid).unwrap();
        assert!(!alive(service.trim()));
    }
}
#[tokio::test]
async fn check_in_a_task_worktree_gets_a_task_root_tmpdir_removed_after_pass_fail_and_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let worktree = temp.path().join("t").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let tmp_root = worktree.parent().unwrap().join(".forge-task/tmp");
    let seen_file = temp.path().join("seen");
    let leftovers = || std::fs::read_dir(&tmp_root).map_or(0, Iterator::count);
    for (tail, outcome) in [
        ("exit 0", CheckExecutionOutcome::Passed),
        ("exit 9", CheckExecutionOutcome::Failed),
    ] {
        let bundle = spec(&format!(
            "touch \"$TMPDIR/made\" && printf '%s' \"$TMPDIR\" > {}; {tail}",
            seen_file.display()
        ));
        let receipt = run_command(
            &worktree,
            &bundle.commands[0],
            &BTreeMap::new(),
            &bundle.execution_policy,
            Some(Instant::now() + Duration::from_secs(10)),
            &CancellationToken::new(),
            512,
        )
        .await
        .unwrap();
        assert_eq!(receipt.outcome, outcome);
        let seen = std::path::PathBuf::from(std::fs::read_to_string(&seen_file).unwrap());
        assert_eq!(seen.parent(), Some(tmp_root.as_path()));
        assert_eq!(leftovers(), 0);
    }
    let cancel = CancellationToken::new();
    let bundle = spec("touch \"$TMPDIR/made\"; sleep 30");
    let stop = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.cancel();
    });
    let receipt = run_command(
        &worktree,
        &bundle.commands[0],
        &BTreeMap::new(),
        &bundle.execution_policy,
        Some(Instant::now() + Duration::from_secs(20)),
        &cancel,
        512,
    )
    .await
    .unwrap();
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Cancelled);
    assert_eq!(leftovers(), 0);

    // A checkout that is not a Task root gets nothing and nothing beside it.
    let plain = tempfile::tempdir().unwrap();
    let (command, _) = command(
        plain.path(),
        &spec("true").commands[0],
        &BTreeMap::new(),
        &spec("true").execution_policy,
    )
    .unwrap();
    assert!(!command
        .as_std()
        .get_envs()
        .any(|(key, _)| key == "TMPDIR" || key == "CARGO_TARGET_DIR"));
}

/// What a run printed for `$RUSTC_WRAPPER|$KACHE_CACHE_DIR` when Forge
/// offered `wrapper` and `store`. The operator's own environment wins
/// over Forge's, so on a machine whose environment names a wrapper (or a
/// kache directory) this asserts that rule instead.
#[cfg(unix)]
fn assert_saw_compiler_cache(seen: &str, wrapper: &std::path::Path, store: &std::path::Path) {
    let operator = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
    let (wrapper, store) = (wrapper.to_str().unwrap(), store.to_str().unwrap());
    if operator("RUSTC_WRAPPER") {
        assert!(!seen.starts_with(wrapper), "{seen}");
    } else if operator("KACHE_CACHE_DIR") {
        assert!(seen.starts_with(&format!("{wrapper}|")), "{seen}");
    } else {
        assert_eq!(seen, format!("{wrapper}|{store}"));
    }
}

/// Plan 3.4 F: a check in a Task worktree is handed the machine's shared
/// compiler cache, with the store of the worktree's repository.
#[cfg(unix)]
#[tokio::test]
async fn check_in_a_task_worktree_gets_the_shared_compiler_cache() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("ws");
    let worktree = root.join("t").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    // A linked worktree of the repository `r1`, as the server lays it out.
    std::fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", root.join(".repos/r1/worktrees/t").display()),
    )
    .unwrap();
    let wrapper = temp.path().join("kache");
    std::fs::write(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    executors::compiler_cache::install(
        &root,
        Some(executors::compiler_cache::CompilerCache {
            kind: executors::compiler_cache::WrapperKind::of(&wrapper),
            wrapper: wrapper.clone(),
            dir: root.join(executors::compiler_cache::CACHE_DIR),
            max_bytes: 1 << 30,
        }),
    );
    let store = root.join(executors::compiler_cache::CACHE_DIR).join("r1");
    let seen_file = temp.path().join("seen");
    let script = format!(
        "printf '%s|%s' \"$RUSTC_WRAPPER\" \"$KACHE_CACHE_DIR\" > '{}'",
        seen_file.display()
    );
    let bundle = spec(&script);
    let receipt = run_command(
        &worktree,
        &bundle.commands[0],
        &BTreeMap::new(),
        &bundle.execution_policy,
        Some(Instant::now() + Duration::from_secs(10)),
        &CancellationToken::new(),
        512,
    )
    .await
    .unwrap();
    executors::compiler_cache::install(&root, None);
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_saw_compiler_cache(
        &std::fs::read_to_string(&seen_file).unwrap(),
        &wrapper,
        &store,
    );
}
