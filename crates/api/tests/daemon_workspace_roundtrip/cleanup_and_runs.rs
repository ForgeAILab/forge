//! Remote workspace cleanup, execution recovery and daemon reports.

use super::*;

#[tokio::test]
async fn remote_cleanup_stays_cleaning_offline_until_owner_acknowledges() {
    let mut fixture = Fixture::new("forge-workspace-cleanup").await;
    fixture.mark_task_done().await;
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&fixture.resolved.placement.workspace_id)
        .await
        .unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(
        &*fixture.harness.state.db,
        &fixture.resolved.placement.id,
    )
    .await
    .unwrap()
    .unwrap();
    let workspace = WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let offline_diagnostic = format!(
        "placement_state={:?}, workspace_status={:?}, pending_journal_count={}, last_server_frame={:?}",
        placement.state,
        workspace.status,
        fixture.runtime.journal().pending().unwrap().len(),
        fixture.link.as_ref().and_then(DaemonLink::last_request),
    );
    assert_eq!(
        placement.state,
        PlacementState::Cleaning,
        "offline cleanup remains pending: {offline_diagnostic}"
    );
    assert_eq!(
        workspace.status,
        WorkspaceStatus::Cleaning,
        "offline cleanup preserves workspace state: {offline_diagnostic}"
    );
    fixture.reconnect().await;
    // The offline attempt left the scheduler's one-tick retry deadline on the
    // workspace, and cleanup retains that backoff. Elapse it as the next
    // scheduler tick would see it, so this retry reaches the returned owner.
    assert!(workspace
        .cleanup_after
        .as_deref()
        .and_then(|deadline| chrono::DateTime::parse_from_rfc3339(deadline).ok())
        .is_some_and(|deadline| deadline > chrono::Utc::now()));
    fixture
        .harness
        .state
        .cleanup_scheduler
        .schedule(&placement.workspace_id, Duration::ZERO)
        .await
        .unwrap();
    let cleanup_result = fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&placement.workspace_id)
        .await;
    if let Err(error) = cleanup_result {
        let observed_placement =
            WorkspacePlacementRepo::get_by_id(&*fixture.harness.state.db, &placement.id)
                .await
                .unwrap();
        let observed_workspace =
            WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
                .await
                .unwrap();
        panic!(
            "owner cleanup acknowledgement commits: error={error:?}, placement={observed_placement:?}, workspace={observed_workspace:?}, pending_journal_count={}, last_server_frame={:?}",
            fixture.runtime.journal().pending().unwrap().len(),
            fixture.link.as_ref().and_then(DaemonLink::last_request),
        );
    }
    let placement = WorkspacePlacementRepo::get_by_id(&*fixture.harness.state.db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    let workspace = WorkspaceRepo::get_by_id(&*fixture.harness.state.db, &placement.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let settled_diagnostic = format!(
        "placement_state={:?}, workspace_status={:?}, pending_journal_count={}, last_server_frame={:?}",
        placement.state,
        workspace.status,
        fixture.runtime.journal().pending().unwrap().len(),
        fixture.link.as_ref().and_then(DaemonLink::last_request),
    );
    assert_eq!(
        placement.state,
        PlacementState::Cleaned,
        "owner acknowledgement cleans placement: {settled_diagnostic}"
    );
    assert_eq!(
        workspace.status,
        WorkspaceStatus::Cleaned,
        "owner acknowledgement cleans workspace: {settled_diagnostic}"
    );
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_CLEANUP).await["owner_result"]["cleaned"],
        true
    );
    fixture.wait_until_journal_empty().await;
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_cleanup_retry_acknowledges_after_the_cleaned_state_commits() {
    let fixture = Fixture::new("forge-workspace-cleanup-retained-ack").await;
    fixture.mark_task_done().await;
    // Lose the unsolicited cleanup notification while retaining RPC replies,
    // so the test can stop between the state commit and journal acknowledgement.
    let (outbound, _notifications) = mpsc::unbounded_channel();
    fixture.runtime.attach(outbound);
    let mut transaction = db::begin_immediate(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar("SELECT version FROM workspace_placement WHERE id = ?")
        .bind(&fixture.resolved.placement.id)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    let updated = sqlx::query("UPDATE workspace_placement SET state = 'cleaning', version = version + 1 WHERE id = ? AND version = ? AND state = 'ready'")
        .bind(&fixture.resolved.placement.id)
        .bind(version)
        .execute(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    transaction.commit().await.unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(
        &*fixture.harness.state.db,
        &fixture.resolved.placement.id,
    )
    .await
    .unwrap()
    .unwrap();
    // The backend retains removal without acknowledging it until the server
    // commits the cleaned state. Stop at that boundary to model interruption.
    fixture.resolved.backend.cleanup(&placement).await.unwrap();
    assert!(!fixture.runtime.journal().pending().unwrap().is_empty());
    let updated = sqlx::query("UPDATE workspace_placement SET state = 'cleaned', version = version + 1 WHERE id = ? AND version = ? AND state = 'cleaning'")
        .bind(&placement.id)
        .bind(placement.version)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    WorkspaceRepo::update_status(
        &*fixture.harness.state.db,
        &placement.workspace_id,
        WorkspaceStatus::Cleaned,
        None,
        &db::now_rfc3339(),
    )
    .await
    .unwrap();
    fixture
        .harness
        .state
        .cleanup_scheduler
        .cleanup_now(&placement.workspace_id)
        .await
        .unwrap();
    fixture.wait_until_journal_empty().await;
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_CLEANUP)
            .len(),
        1,
        "acknowledgement retry does not remove the workspace again"
    );
}

#[tokio::test]
async fn remote_interrupted_run_is_settled_and_error_acknowledged_without_rerun() {
    let fixture = Fixture::new("forge-workspace-interrupted-run").await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .interrupt
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RUN.into());
    let result = fixture
        .resolved
        .backend
        .run(
            &fixture.resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: "printf duplicate >> never-run".into(),
                env: BTreeMap::new(),
                timeout_secs: 0,
                max_output_bytes: usize::MAX,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(WorkspaceBackendError::OwnerUnreachable { .. })
    ));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert_eq!(receipt["metadata"]["status"], "error");
    assert_eq!(receipt["owner_result"]["error"]["code"], DAEMON_UNAVAILABLE);
    fixture.wait_until_journal_empty().await;
    assert_eq!(fixture.run("test ! -e never-run").await.exit_code, 0);
    let requests = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_WORKSPACE_RUN);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["command"] == "printf duplicate >> never-run")
            .count(),
        1
    );
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_run_policy_error_is_durably_recorded_and_acknowledged() {
    let fixture = Fixture::with_policy(
        "forge-workspace-run-policy",
        WorkspaceRunPolicy {
            allowed_purposes: vec![WorkspaceRunPurpose::CiStep],
        },
    )
    .await;
    let result = fixture
        .resolved
        .backend
        .run(
            &fixture.resolved.placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::Hook,
                command: "printf should-not-run".into(),
                env: BTreeMap::new(),
                timeout_secs: 2,
                max_output_bytes: 1024,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(WorkspaceBackendError::PurposeDenied {
            purpose: WorkspaceRunPurpose::Hook
        })
    ));
    let receipt = fixture.receipt(METHOD_WORKSPACE_RUN).await;
    assert_eq!(receipt["metadata"]["status"], "error");
    assert_eq!(receipt["owner_result"]["error"]["code"], PURPOSE_DENIED);
    assert_eq!(
        fixture
            .link
            .as_ref()
            .unwrap()
            .requests(METHOD_WORKSPACE_RUN)
            .len(),
        1
    );
    fixture.wait_until_journal_empty().await;
}

#[tokio::test]
async fn remote_review_disconnect_does_not_freeze_a_failed_assessment() {
    use db::ReviewConformanceRepo;
    let mut fixture = Fixture::new("forge-workspace-review-disconnect").await;
    fixture.candidate().await;
    let (_review, auditor) = fixture.reviewer_attempt().await;
    let auditor_id = auditor.id;
    review::contract::admit(
        &fixture.harness.state.db,
        &auditor_id,
        &fixture.resolved.placement.task_id,
        &fixture.resolved,
    )
    .await
    .unwrap();
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    let result = review::contract::evaluate(
        &fixture.harness.state.db,
        &auditor_id,
        &fixture.resolved,
        "The candidate is implemented.\n\n{\"result\":\"pass\",\"reason\":\"verified\"}",
    )
    .await;
    let reason = result.err().unwrap();
    assert!(matches!(
        fixture.resolved.infrastructure_error(&reason),
        Some(review::ReviewError::OwnerUnavailable { .. })
    ));
    assert!(fixture
        .harness
        .state
        .db
        .review_conformance(&auditor_id)
        .await
        .unwrap()
        .is_none());
    fixture.assert_server_cannot_resolve_workspace();
}

#[tokio::test]
async fn remote_lost_run_result_reconciles_from_the_real_daemon_journal() {
    let mut fixture = Fixture::new("forge-workspace-lost-reply").await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RUN.into());
    let resolved = fixture.resolved.clone();
    let running = tokio::spawn(async move {
        resolved
            .backend
            .run(
                &resolved.placement,
                &RunSpec {
                    purpose: WorkspaceRunPurpose::CiStep,
                    command: "printf once >> run-count; printf retained".into(),
                    env: BTreeMap::new(),
                    timeout_secs: 0,
                    max_output_bytes: usize::MAX,
                },
            )
            .await
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if fixture.runtime.journal().pending().unwrap().iter().any(|entry| matches!(entry,
            JournalEntry::Operation { operation } if operation.method == METHOD_WORKSPACE_RUN && operation.outcome.is_some())) { break }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run result reaches the owner journal"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    assert!(matches!(
        running.await.unwrap(),
        Err(WorkspaceBackendError::OwnerUnreachable { .. })
    ));
    fixture.reconnect().await;
    let client = fixture.resolved.backend.daemon_client().unwrap();
    client
        .reconcile_pending_operations(&fixture.daemon_id)
        .await
        .unwrap();
    client
        .retry_acknowledgements(&fixture.daemon_id)
        .await
        .unwrap();
    assert_eq!(
        fixture.receipt(METHOD_WORKSPACE_RUN).await["owner_result"]["stdout"],
        "retained"
    );
    fixture.wait_until_journal_empty().await;
    assert_eq!(fixture.run("cat run-count").await.stdout_tail, "once");
    fixture.assert_server_cannot_resolve_workspace();
}

/// A daemon's report carries the sizes its collector measured for its Task
/// roots; the server stores them on the workspace rows (only for handles
/// that daemon owns) and the reply carries the server's check-run limit
/// beside the floor.
#[tokio::test]
async fn remote_daemon_report_stores_task_root_sizes_and_returns_the_check_limit() {
    let fixture = Fixture::new("forge-owner-disk-sizes").await;
    let placement = &fixture.resolved.placement;
    let db = &fixture.harness.state.db;
    let row = || async {
        WorkspaceRepo::get_by_id(&**db, &placement.workspace_id)
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        row().await.disk_bytes,
        None,
        "a daemon's root was never sized"
    );
    db.disk_admission.set_check_run_timeout_seconds(5400);
    let report_url = format!("/api/v1/daemons/{}/report", fixture.daemon_id);
    let size = |handle: &str, bytes: u64| json!({"workspace_handle": handle, "disk_bytes": bytes, "measured_at": "2026-10-10T00:00:00Z"});
    let reply: DaemonResponse = common::json_request_with_bearer(
        &fixture.harness.app,
        Method::POST,
        &report_url,
        &fixture.token,
        json!({
            "detected_clis": [],
            "workspace_disk": [
                size(placement.workspace_handle.as_deref().unwrap(), 4096),
                size("workspace-00000000-0000-4000-8000-000000000000", 1),
            ],
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(reply.check_run_timeout_seconds, Some(5400));
    let sized = row().await;
    assert_eq!(sized.disk_bytes, Some(4096));
    assert!(sized.disk_measured_at.is_some());
    // A report without sizes (an older daemon) leaves the row as it is.
    let _: DaemonResponse = common::json_request_with_bearer(
        &fixture.harness.app,
        Method::POST,
        &report_url,
        &fixture.token,
        json!({"detected_clis": []}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(row().await.disk_bytes, Some(4096));
}
