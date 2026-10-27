//! Execution leases repository tests.
use super::*;

#[tokio::test]
async fn latest_non_running_finds_terminal_after_100_newer_running_rows() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "review".to_owned(), "history task").await;
    let old_now = "2026-09-11T00:00:00Z";
    let now = "2026-09-12T00:00:00Z";

    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: "recovery-history-000".to_owned(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("[Forge automatic review recovery] older attempt".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: old_now.to_owned(),
            updated_at: old_now.to_owned(),
        },
    )
    .await
    .expect("automatic recovery execution creates");

    for index in 1..=100 {
        ExecutionRepo::create(
            &db,
            CreateExecution {
                id: format!("history-execution-{index:03}"),
                task_id: task_id.clone(),
                agent_id: None,
                role: "executor".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: Some(format!("unrelated execution {index}")),
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("history execution creates");
    }

    let latest_terminal =
        ExecutionRepo::latest_non_running_by_task_and_role(&db, &task_id, "executor")
            .await
            .expect("latest terminal execution loads")
            .expect("older terminal execution remains discoverable");
    assert_eq!(latest_terminal.id, "recovery-history-000");
    assert!(latest_terminal
        .summary
        .as_deref()
        .is_some_and(|summary| summary.starts_with("[Forge automatic review recovery]")));
}

#[tokio::test]
async fn execution_lease_and_terminal_cas_are_single_winner_and_preserve_deadline() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Execution liveness CAS",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let execution_id = new_uuid_v4();
    let now = "2026-08-21T00:00:00Z";
    let claimed = ExecutionRepo::create_with_lease(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "embedded:test-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:30Z".to_owned(),
            hard_deadline_at: Some("2026-08-21T01:00:00Z".to_owned()),
            now: now.to_owned(),
        },
    )
    .await
    .expect("initial lease claim commits");
    assert_eq!(claimed.execution_version, 2);
    assert_eq!(claimed.lease_owner.as_deref(), Some("embedded:test-owner"));
    assert_eq!(
        claimed.hard_deadline_at.as_deref(),
        Some("2026-08-21T01:00:00Z")
    );
    let competing_claim = ExecutionRepo::claim_lease(
        &db,
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: "remote:competing-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:40Z".to_owned(),
            hard_deadline_at: Some("2026-08-21T01:00:00Z".to_owned()),
            now: "2026-08-21T00:00:10Z".to_owned(),
        },
    )
    .await
    .expect("live owner rejects competing claim");
    assert!(matches!(
        competing_claim,
        ExecutionLeaseMutation::Concurrent { .. }
    ));
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task lookup before workspace lease succeeds")
        .expect("task exists before workspace lease");
    let workspace_lease =
        WorkspaceLeaseRepo::issue(
            &db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                task_id: task_id.clone(),
                task_version: task.version,
                execution_id: execution_id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repo_id.clone(),
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent_id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: now.to_owned(),
                expires_at: "2026-08-21T00:00:45Z".to_owned(),
                created_at: now.to_owned(),
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("workspace lease issues");
    assert_eq!(workspace_lease.status, "active");

    let progressed = ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: "embedded:test-owner".to_owned(),
            progress_at: "2026-08-21T00:00:05Z".to_owned(),
            now: "2026-08-21T00:00:05Z".to_owned(),
        },
    )
    .await
    .expect("semantic progress commits");
    let progressed = match progressed {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected semantic progress result: {other:?}"),
    };
    assert_eq!(progressed.execution_version, claimed.execution_version + 1);
    let progress_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'execution.progressed' AND entity_id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("progress event count reads");
    assert_eq!(
        progress_event_count, 0,
        "progress outside a warned stall only moves the watermark"
    );

    let out_of_order_progress = ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: progressed.execution_version,
            owner: "embedded:test-owner".to_owned(),
            progress_at: "2026-08-21T00:00:04Z".to_owned(),
            now: "2026-08-21T00:00:06Z".to_owned(),
        },
    )
    .await
    .expect("out-of-order progress is classified as a no-op");
    let out_of_order_execution = match out_of_order_progress {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected out-of-order progress result: {other:?}"),
    };
    assert_eq!(
        out_of_order_execution.last_progress_at.as_deref(),
        Some("2026-08-21T00:00:05Z")
    );
    let duplicate_progress = ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: progressed.execution_version,
            owner: "embedded:test-owner".to_owned(),
            progress_at: "2026-08-21T00:00:05Z".to_owned(),
            now: "2026-08-21T00:00:07Z".to_owned(),
        },
    )
    .await
    .expect("duplicate progress is classified as a no-op");
    let duplicate_progress = match duplicate_progress {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected duplicate progress result: {other:?}"),
    };
    assert_eq!(
        duplicate_progress.execution_version,
        progressed.execution_version
    );
    let progress_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'execution.progressed' AND entity_id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("progress event count after duplicate reads");
    assert_eq!(progress_event_count, 0);

    let renewed = ExecutionRepo::renew_lease(
        &db,
        RenewExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: progressed.execution_version,
            owner: "embedded:test-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:45Z".to_owned(),
            now: "2026-08-21T00:00:10Z".to_owned(),
        },
    )
    .await
    .expect("owner heartbeat commits");
    let renewed = match renewed {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected heartbeat result: {other:?}"),
    };
    assert_eq!(renewed.execution_version, progressed.execution_version + 1);
    assert_eq!(
        renewed.last_progress_at.as_deref(),
        Some("2026-08-21T00:00:05Z")
    );

    let warning = ExecutionRepo::record_progress_warning(
        &db,
        crate::RecordExecutionProgressWarning {
            execution_id: execution_id.clone(),
            expected_version: renewed.execution_version,
            owner: "embedded:test-owner".to_owned(),
            expected_last_progress_at: renewed.last_progress_at.clone(),
            stale_before: "2026-08-21T00:00:06Z".to_owned(),
            now: "2026-08-21T00:00:10Z".to_owned(),
        },
    )
    .await
    .expect("progress warning commits");
    let warned = match warning {
        ExecutionProgressWarningOutcome::Committed { execution, event } => {
            assert_eq!(event.event_type, "execution.progress_warning");
            execution
        }
        other => panic!("unexpected progress warning result: {other:?}"),
    };
    assert_eq!(
        warned.execution_version, renewed.execution_version,
        "warning projection must not invalidate the owner's heartbeat CAS"
    );

    let renewed_again = match ExecutionRepo::renew_lease(
        &db,
        RenewExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: warned.execution_version,
            owner: "embedded:test-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:55Z".to_owned(),
            now: "2026-08-21T00:00:20Z".to_owned(),
        },
    )
    .await
    .expect("heartbeat after warning commits")
    {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected heartbeat-after-warning result: {other:?}"),
    };
    let replayed_warning = ExecutionRepo::record_progress_warning(
        &db,
        crate::RecordExecutionProgressWarning {
            execution_id: execution_id.clone(),
            expected_version: renewed_again.execution_version,
            owner: "embedded:test-owner".to_owned(),
            expected_last_progress_at: renewed_again.last_progress_at.clone(),
            stale_before: "2026-08-21T00:00:21Z".to_owned(),
            now: "2026-08-21T00:00:20Z".to_owned(),
        },
    )
    .await
    .expect("repeated progress warning is classified");
    assert!(matches!(
        replayed_warning,
        ExecutionProgressWarningOutcome::Replayed { .. }
    ));
    let stale_progress =
        ExecutionRepo::list_stale_progress(&db, "2026-08-21T00:00:20Z", "2026-08-21T00:00:06Z", 10)
            .await
            .expect("stale live progress query succeeds");
    assert_eq!(stale_progress.len(), 1);
    assert_eq!(stale_progress[0].id, execution_id);

    // Progress that ends the warned stall epoch is the one durable event:
    // it resolves the warning's Attention item.
    let renewed_again = match ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: renewed_again.execution_version,
            owner: "embedded:test-owner".to_owned(),
            progress_at: "2026-08-21T00:00:21Z".to_owned(),
            now: "2026-08-21T00:00:21Z".to_owned(),
        },
    )
    .await
    .expect("progress after a warning commits")
    {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("unexpected progress-after-warning result: {other:?}"),
    };
    let progress_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'execution.progressed' AND entity_id = ?",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("progress event count after warning reads");
    assert_eq!(progress_event_count, 1);
    let hard_deadline_renewal = ExecutionRepo::renew_lease(
        &db,
        RenewExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: renewed_again.execution_version,
            owner: "embedded:test-owner".to_owned(),
            lease_expires_at: "2026-08-21T01:01:00Z".to_owned(),
            now: "2026-08-21T01:00:00Z".to_owned(),
        },
    )
    .await
    .expect("hard deadline renewal is classified");
    assert!(matches!(
        hard_deadline_renewal,
        ExecutionLeaseMutation::HardDeadline { .. }
    ));

    let terminal_input = TerminalizeExecution {
        execution_id: execution_id.clone(),
        expected_version: renewed_again.execution_version,
        lease_owner: renewed_again.lease_owner.clone(),
        status: ExecutionStatus::Completed,
        stop_reason: Some(None),
        stopped_by: Some(Some("embedded:test-owner".to_owned())),
        stopped_at: Some(Some("2026-08-21T00:00:11Z".to_owned())),
        resume_policy: Some(Some(crate::ResumePolicy::None)),
        agent_session_id: Some(None),
        agent_message_id: Some(None),
        last_activity_at: Some(None),
        last_progress_at: Some(Some("2026-08-21T00:00:05Z".to_owned())),
        summary: Some(Some("complete".to_owned())),
        logs_path: Some(None),
        before_sha: Some(None),
        after_sha: Some(Some("after-sha".to_owned())),
        error: Some(None),
        executor_config_snapshot_json: Some(None),
        updated_at: "2026-08-21T00:00:11Z".to_owned(),
        actor_type: "system".to_owned(),
        actor_id: None,
        correlation_id: None,
        causation_id: None,
        causation_depth: 0,
        lease_disposition: ExecutionLeaseDisposition::Expire,
    };

    // A conflicting terminal dedupe key makes the event append fail after the
    // execution UPDATE.  The repository must roll the UPDATE back, proving
    // that terminal status and its durable event are one transaction.
    let conflict_event_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: conflict_event_id.clone(),
            event_type: "execution.conflict".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task_id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: conflict_event_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("execution-terminal:{}:completed", execution_id)),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-21T00:00:10Z".to_owned(),
        },
    )
    .await
    .expect("conflicting terminal event inserts");
    let failed_terminal = ExecutionRepo::terminalize(&db, terminal_input.clone()).await;
    assert!(
        failed_terminal.is_err(),
        "event conflict must abort terminal CAS"
    );
    let rolled_back = ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("execution lookup after rollback succeeds")
        .expect("execution remains present after rollback");
    assert_eq!(rolled_back.status, ExecutionStatus::Running);
    assert_eq!(
        rolled_back.execution_version,
        terminal_input.expected_version
    );
    assert_eq!(rolled_back.lease_owner, terminal_input.lease_owner);
    assert_eq!(
        rolled_back.lease_expires_at.as_deref(),
        Some("2026-08-21T00:00:55Z")
    );
    assert_eq!(
        rolled_back.hard_deadline_at.as_deref(),
        Some("2026-08-21T01:00:00Z")
    );
    let rolled_back_workspace_lease = WorkspaceLeaseRepo::get_by_id(&db, &workspace_lease.id)
        .await
        .expect("workspace lease lookup after rollback succeeds")
        .expect("workspace lease remains after rollback");
    assert_eq!(rolled_back_workspace_lease.status, "active");
    assert_eq!(rolled_back_workspace_lease.version, workspace_lease.version);
    sqlx::query("DELETE FROM domain_event WHERE id = ?")
        .bind(conflict_event_id)
        .execute(db.pool())
        .await
        .expect("conflicting event removes for successful retry");

    let committed = ExecutionRepo::terminalize(&db, terminal_input)
        .await
        .expect("terminal completion commits");
    let committed_execution = match committed {
        crate::ExecutionTerminalOutcome::Committed {
            execution,
            event,
            workspace_lease_id,
            workspace_lease_status,
            replayed: _,
        } => {
            assert_eq!(event.event_type, "execution.completed");
            assert_eq!(
                workspace_lease_id.as_deref(),
                Some(workspace_lease.id.as_str())
            );
            assert_eq!(workspace_lease_status.as_deref(), Some("expired"));
            let payload: serde_json::Value =
                serde_json::from_str(&event.payload_json).expect("terminal payload is JSON");
            assert_eq!(
                payload["previous_lease_owner"].as_str(),
                Some("embedded:test-owner")
            );
            execution
        }
        other => panic!("unexpected terminal result: {other:?}"),
    };
    assert_eq!(committed_execution.status, ExecutionStatus::Completed);
    assert!(committed_execution.lease_owner.is_none());
    assert!(committed_execution.lease_expires_at.is_none());
    assert_eq!(
        committed_execution.hard_deadline_at.as_deref(),
        Some("2026-08-21T01:00:00Z")
    );
    let expired_workspace_lease = WorkspaceLeaseRepo::get_by_id(&db, &workspace_lease.id)
        .await
        .expect("expired workspace lease loads")
        .expect("workspace lease remains historical");
    assert_eq!(expired_workspace_lease.status, "expired");
    assert_eq!(
        expired_workspace_lease.revoked_at.as_deref(),
        Some("2026-08-21T00:00:11Z")
    );

    let stale_renewal = ExecutionRepo::renew_lease(
        &db,
        RenewExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: renewed_again.execution_version,
            owner: "embedded:test-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:55Z".to_owned(),
            now: "2026-08-21T00:00:12Z".to_owned(),
        },
    )
    .await
    .expect("stale renewal is classified");
    assert!(matches!(
        stale_renewal,
        ExecutionLeaseMutation::Concurrent { .. }
    ));
    let stale_terminal = ExecutionRepo::terminalize(
        &db,
        TerminalizeExecution {
            execution_id: execution_id.clone(),
            expected_version: renewed_again.execution_version,
            lease_owner: Some("embedded:test-owner".to_owned()),
            status: ExecutionStatus::Failed,
            stop_reason: Some(Some(crate::StopReason::ExecutionStalled)),
            stopped_by: Some(Some("monitor".to_owned())),
            stopped_at: Some(Some("2026-08-21T00:00:12Z".to_owned())),
            resume_policy: Some(Some(crate::ResumePolicy::Manual)),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some("late monitor".to_owned())),
            executor_config_snapshot_json: None,
            updated_at: "2026-08-21T00:00:12Z".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Expire,
        },
    )
    .await
    .expect("stale terminal is classified");
    assert!(matches!(
        stale_terminal,
        crate::ExecutionTerminalOutcome::Concurrent { .. }
    ));
    let terminal_event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE dedupe_key LIKE ? AND entity_id = ?",
    )
    .bind(format!("execution-terminal:{execution_id}:%"))
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("terminal event count reads");
    assert_eq!(terminal_event_count, 1);
}

/// Unbounded executions (the default since Task runs lost their implicit wall
/// clock) carry no `hard_deadline_at`. Progress must still commit — a NULL
/// deadline failed the SQL guard and `None <= Some(now)` then misreported it
/// as a reached deadline, so every remote log/progress update was rejected —
/// and a stalled unbounded run must still be able to raise a warning.
#[tokio::test]
async fn unbounded_execution_records_progress_and_progress_warnings() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Unbounded execution progress",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let execution_id = new_uuid_v4();
    let now = "2026-08-21T00:00:00Z";
    let claimed = ExecutionRepo::create_with_lease(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
        ClaimExecutionLease {
            execution_id: execution_id.clone(),
            expected_version: 1,
            owner: "remote:unbounded-owner".to_owned(),
            lease_expires_at: "2026-08-21T00:00:30Z".to_owned(),
            hard_deadline_at: None,
            now: now.to_owned(),
        },
    )
    .await
    .expect("unbounded lease claim commits");
    assert_eq!(claimed.hard_deadline_at, None);

    let progressed = match ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: "remote:unbounded-owner".to_owned(),
            progress_at: "2026-08-21T00:00:05Z".to_owned(),
            now: "2026-08-21T00:00:05Z".to_owned(),
        },
    )
    .await
    .expect("progress call succeeds")
    {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("an unbounded execution must accept progress: {other:?}"),
    };
    assert_eq!(
        progressed.last_progress_at.as_deref(),
        Some("2026-08-21T00:00:05Z")
    );

    let stale = ExecutionRepo::record_progress(
        &db,
        RecordExecutionProgress {
            execution_id: execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: "remote:unbounded-owner".to_owned(),
            progress_at: "2026-08-21T00:00:06Z".to_owned(),
            now: "2026-08-21T00:00:06Z".to_owned(),
        },
    )
    .await
    .expect("stale progress call succeeds");
    assert!(
        matches!(stale, ExecutionLeaseMutation::Concurrent { .. }),
        "a stale version is a concurrent write, not a reached deadline: {stale:?}"
    );

    let warning = ExecutionRepo::record_progress_warning(
        &db,
        crate::RecordExecutionProgressWarning {
            execution_id: execution_id.clone(),
            expected_version: progressed.execution_version,
            owner: "remote:unbounded-owner".to_owned(),
            expected_last_progress_at: progressed.last_progress_at.clone(),
            stale_before: "2026-08-21T00:00:10Z".to_owned(),
            now: "2026-08-21T00:00:20Z".to_owned(),
        },
    )
    .await
    .expect("progress warning call succeeds");
    assert!(
        matches!(warning, ExecutionProgressWarningOutcome::Committed { .. }),
        "a stalled unbounded execution must raise a warning: {warning:?}"
    );
}
