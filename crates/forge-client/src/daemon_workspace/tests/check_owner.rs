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
