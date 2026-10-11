use super::*;
fn params(fixture: &Fixture, id: &str, text: &str) -> DaemonCheckRunParams {
    DaemonCheckRunParams {
        operation_id: id.into(),
        target: DaemonCheckTarget::Workspace {
            workspace: fixture.reference(),
        },
        purpose: WorkspaceRunPurpose::CiStep,
        spec: check_executor::legacy_ci_spec(text, &BTreeMap::new(), 5, true),
        env: vec![],
        cleanup_commands: vec![],
        cleanup_timeout_ms: 1000,
        deadline: (chrono::Utc::now() + chrono::Duration::seconds(10)).to_rfc3339(),
    }
}
async fn call(
    backend: &DaemonWorkspaceBackend,
    params: &DaemonCheckRunParams,
) -> DaemonCheckResult {
    decode(
        backend
            .handle(
                METHOD_CHECK_RUN,
                serde_json::to_value(params).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap()
}
#[tokio::test]
async fn duplicate_check_key_replays_receipt_without_starting_a_second_tree() {
    let fixture = Fixture::new().await;
    let request = params(
        &fixture,
        "check-once",
        "printf run >> runs; printf evidence",
    );
    let first = call(&fixture.backend, &request).await;
    assert_eq!(first, call(&fixture.backend, &request).await);
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("runs")).unwrap(),
        "run"
    );
    assert_eq!(
        fixture
            .backend
            .check_cancel(DaemonCheckOperationParams {
                daemon_id: "daemon-1".into(),
                operation_id: request.operation_id.clone()
            })
            .await
            .unwrap(),
        first
    );
    let DaemonCheckResult::Completed { receipt } = first else {
        panic!("missing receipt")
    };
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert_eq!(
        receipt.execution_inputs,
        CheckEnvironmentIdentity::NotAttested
    );
    assert_eq!(receipt.owner.machine_id.as_deref(), Some("daemon-1"));
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().into(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        fixture.journal.clone(),
    )
    .unwrap();
    assert_eq!(
        call(&restarted, &request).await,
        DaemonCheckResult::Completed { receipt }
    );
}
#[tokio::test]
async fn running_lookup_duplicate_cancel_and_restart_never_repeat_an_intent() {
    let fixture = Fixture::new().await;
    let request = params(&fixture, "check-live", "printf run >> runs; sleep 60");
    let operation = DaemonCheckOperationParams {
        daemon_id: "daemon-1".into(),
        operation_id: request.operation_id.clone(),
    };
    let run = call(&fixture.backend, &request);
    tokio::pin!(run);
    tokio::select! {
        result=&mut run=>panic!("early result {result:?}"),
        _=async {while !fixture.path().join("runs").exists(){tokio::time::sleep(Duration::from_millis(10)).await;}}=>{}
    }
    assert_eq!(
        call(&fixture.backend, &request).await,
        DaemonCheckResult::Running {
            operation_id: request.operation_id.clone()
        }
    );
    assert_eq!(
        fixture
            .backend
            .check_lookup(operation.clone())
            .await
            .unwrap(),
        DaemonCheckResult::Running {
            operation_id: request.operation_id.clone()
        }
    );
    let (result, cancelled) = tokio::join!(run, fixture.backend.check_cancel(operation.clone()));
    assert_eq!(result, cancelled.unwrap());
    let DaemonCheckResult::Completed { receipt } = result else {
        panic!("missing cancellation receipt")
    };
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Cancelled);
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("runs")).unwrap(),
        "run"
    );

    let interrupted = params(&fixture, "check-crash", "printf crash >> runs; sleep 60");
    {
        let lost = call(&fixture.backend, &interrupted);
        tokio::pin!(lost);
        tokio::select! {
            result=&mut lost=>panic!("early result {result:?}"),
            _=async {while std::fs::read_to_string(fixture.path().join("runs")).unwrap()!="runcrash" {tokio::time::sleep(Duration::from_millis(10)).await;}}=>{}
        }
        // Drop a real running owner handler before receipt retention.
    }
    let restarted = DaemonWorkspaceBackend::new(
        fixture.dir.path().into(),
        "daemon-1".into(),
        fixture.backend.policy.clone(),
        fixture.journal.clone(),
    )
    .unwrap();
    assert_eq!(
        call(&restarted, &interrupted).await,
        DaemonCheckResult::Interrupted {
            operation_id: interrupted.operation_id.clone()
        }
    );
    assert_eq!(
        restarted
            .check_cancel(DaemonCheckOperationParams {
                daemon_id: "daemon-1".into(),
                operation_id: interrupted.operation_id.clone()
            })
            .await
            .unwrap(),
        DaemonCheckResult::Interrupted {
            operation_id: interrupted.operation_id.clone()
        }
    );
    assert_eq!(
        std::fs::read_to_string(fixture.path().join("runs")).unwrap(),
        "runcrash"
    );
}

#[tokio::test]
async fn check_receipt_tail_stays_bounded_after_journal_redaction() {
    let fixture = Fixture::new().await;
    let mut request = params(&fixture, "check-redaction", "printf '%4096s' E");
    request.env = vec![("SECRET".into(), "E".into())];
    request.spec.commands[0]
        .environment_keys
        .insert("SECRET".into());
    let DaemonCheckResult::Completed { receipt } = call(&fixture.backend, &request).await else {
        panic!("missing receipt")
    };
    assert_eq!(receipt.outcome, CheckExecutionOutcome::Passed);
    assert!(receipt.commands[0].stdout_truncated);
    assert!(receipt.commands[0].stdout_tail.len() <= check_executor::OUTPUT_TAIL_BYTES);
    assert_eq!(
        call(&fixture.backend, &request).await,
        DaemonCheckResult::Completed { receipt }
    );
}

/// Eight real threads race one key against a managed exact-commit checkout:
/// whoever arrives while the first is still cloning sees `running`, never a
/// second process tree, and every later answer is the one retained receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_of_one_key_start_one_tree_while_the_checkout_is_prepared() {
    let fixture = Arc::new(Fixture::new().await);
    let commit = git::get_current_sha(&fixture.repo).await.unwrap();
    let runs = fixture.dir.path().join("exact-runs");
    let mut request = params(
        &fixture,
        "check-race",
        &format!("printf run >> '{}'; sleep 0.3", runs.display()),
    );
    request.target = DaemonCheckTarget::ExactCommit {
        daemon_id: "daemon-1".into(),
        runtime_id: "runtime-1".into(),
        repo_location_id: "location-1".into(),
        commit_sha: commit,
    };
    let start = Arc::new(tokio::sync::Barrier::new(8));
    let mut racers = Vec::new();
    for _ in 0..8 {
        let (fixture, request, start) = (fixture.clone(), request.clone(), start.clone());
        racers.push(tokio::spawn(async move {
            start.wait().await;
            call(&fixture.backend, &request).await
        }));
    }
    let mut receipts = Vec::new();
    for racer in racers {
        match racer.await.unwrap() {
            DaemonCheckResult::Completed { receipt } => receipts.push(receipt),
            DaemonCheckResult::Running { operation_id } => assert_eq!(operation_id, "check-race"),
            other => panic!("unexpected duplicate answer {other:?}"),
        }
    }
    assert!(!receipts.is_empty());
    assert_eq!(
        receipts[0].outcome,
        CheckExecutionOutcome::Passed,
        "{:?}",
        receipts[0]
    );
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    assert_eq!(std::fs::read_to_string(&runs).unwrap(), "run");
    assert_eq!(
        call(&fixture.backend, &request).await,
        DaemonCheckResult::Completed {
            receipt: receipts.remove(0)
        }
    );
    assert_eq!(std::fs::read_to_string(&runs).unwrap(), "run");
    // The managed checkout is gone and nothing else was left in the build area.
    let build = fixture.dir.path().join(".forge/build/checks");
    assert!(!build.exists() || std::fs::read_dir(&build).unwrap().next().is_none());
}

/// A checkout held by another owner operation bounds the wait by the check's
/// own deadline: the key settles as timed out, nothing was spawned, and the
/// duplicate gets that receipt.
#[tokio::test]
async fn a_busy_checkout_settles_the_key_at_its_deadline_without_spawning() {
    let fixture = Fixture::new().await;
    let mut request = params(&fixture, "check-busy", "printf run >> runs");
    request.deadline = (chrono::Utc::now() + chrono::Duration::milliseconds(300)).to_rfc3339();
    let busy = fixture.backend.owner_lock(&format!(
        "workspace:{}",
        fixture.reference().workspace_handle
    ));
    let held = busy.lock().await;
    let result = tokio::time::timeout(Duration::from_secs(5), call(&fixture.backend, &request))
        .await
        .expect("the wait for the checkout is bounded");
    drop(held);
    let DaemonCheckResult::Completed { receipt } = &result else {
        panic!("missing receipt {result:?}")
    };
    assert_eq!(receipt.outcome, CheckExecutionOutcome::TimedOut);
    assert!(receipt.commands.is_empty());
    assert_eq!(receipt.cleanup.outcome, CheckCleanupOutcome::NotPerformed);
    assert!(!fixture.path().join("runs").exists());
    assert_eq!(call(&fixture.backend, &request).await, result);
    assert!(!fixture.path().join("runs").exists());
}

/// A deadline is also the key's retention bound, so it cannot be arbitrary.
#[tokio::test]
async fn a_deadline_more_than_a_day_ahead_is_refused_before_any_intent() {
    let fixture = Fixture::new().await;
    let mut request = params(&fixture, "check-far", "printf run >> runs");
    request.deadline = (chrono::Utc::now() + chrono::Duration::hours(25)).to_rfc3339();
    let refused = fixture
        .backend
        .handle(
            METHOD_CHECK_RUN,
            serde_json::to_value(&request).unwrap(),
            Vec::new,
        )
        .await
        .unwrap_err();
    assert_eq!(refused.code, INVALID_INPUT);
    assert!(fixture
        .journal
        .check_operation("check-far")
        .unwrap()
        .is_none());
    assert!(!fixture.path().join("runs").exists());
}

/// Retention: an acknowledged receipt leaves once its deadline has passed, an
/// unacknowledged or interrupted key a day later. Nothing leaves while a
/// duplicate could still start a command.
#[tokio::test]
async fn check_keys_are_pruned_once_a_duplicate_can_no_longer_run() {
    let fixture = Fixture::new().await;
    let now = chrono::Utc::now();
    let journal = &fixture.journal;
    let entries = || {
        std::fs::read_dir(journal.directory())
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("entry-")
            })
            .count()
    };
    let before = entries();
    let acknowledged = params(&fixture, "check-acked", "true");
    let unacknowledged = params(&fixture, "check-unacked", "true");
    for request in [&acknowledged, &unacknowledged] {
        let DaemonCheckResult::Completed { .. } = call(&fixture.backend, request).await else {
            panic!("missing receipt")
        };
    }
    // An interrupted intent: retained, never finished.
    let interrupted = params(&fixture, "check-interrupted", "true");
    journal
        .retain_entry(&JournalEntry::Check {
            operation: crate::daemon_persistence::JournalCheckOperation {
                entry_id: operation_entry_id("check-interrupted"),
                operation_id: "check-interrupted".into(),
                request: serde_json::to_value(&interrupted).unwrap(),
                receipt: None,
                acknowledged: false,
            },
        })
        .unwrap();
    assert_eq!(entries(), before + 3);
    // Acknowledged before the deadline: the key stays, and a duplicate still
    // returns the receipt without running.
    journal
        .acknowledge(&JournalAckParams {
            entry_id: operation_entry_id("check-acked"),
        })
        .unwrap();
    assert_eq!(journal.prune_checks(now).unwrap(), 0);
    assert!(matches!(
        call(&fixture.backend, &acknowledged).await,
        DaemonCheckResult::Completed { .. }
    ));
    assert!(journal
        .pending()
        .unwrap()
        .iter()
        .all(|entry| entry.entry_id() != operation_entry_id("check-acked")));
    // Past the ten-second deadline only the acknowledged key leaves.
    assert_eq!(
        journal
            .prune_checks(now + chrono::Duration::seconds(60))
            .unwrap(),
        1
    );
    assert!(journal.check_operation("check-acked").unwrap().is_none());
    assert!(journal.check_operation("check-unacked").unwrap().is_some());
    // A day past the deadline the unacknowledged and interrupted keys leave.
    assert_eq!(
        journal
            .prune_checks(now + chrono::Duration::hours(25))
            .unwrap(),
        2
    );
    assert_eq!(entries(), before);
}
