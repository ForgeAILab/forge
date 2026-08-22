//! Gate-D acceptance fixtures for execution liveness and terminal races.
//!
//! These tests exercise the owner/version repository boundary directly. They
//! use deterministic RFC3339 timestamps so a silent provider wait, an expired
//! owner, and two concurrent terminal callers can be tested without sleeping
//! or coupling assertions to the wall clock.

use std::sync::Arc;

use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    ClaimExecutionLease, CreateAgent, CreateExecution, CreateProject, CreateRepo, CreateTask,
    CreateWorkspaceLease, ExecutionLeaseDisposition, ExecutionLeaseMutation, ExecutionRepo,
    ExecutionStatus, ExecutionTerminalOutcome, ProjectRepo, RepoRepo, SqliteDb, TaskRepo,
    TerminalizeExecution, WorkMode, WorkspaceLeaseRepo,
};

const STALE_PROGRESS: &str = "2020-01-01T00:00:00+00:00";
const T0: &str = "2025-01-01T00:00:00+00:00";
const T1: &str = "2025-01-01T00:00:01+00:00";
const T4: &str = "2025-01-01T00:00:04+00:00";
const T5: &str = "2025-01-01T00:00:05+00:00";
const T6: &str = "2025-01-01T00:00:06+00:00";
const T7: &str = "2025-01-01T00:00:07+00:00";
const T8: &str = "2025-01-01T00:00:08+00:00";
const T10: &str = "2025-01-01T00:00:10+00:00";
const T19: &str = "2025-01-01T00:00:19+00:00";
const T20: &str = "2025-01-01T00:00:20+00:00";

struct ExecutionFixture {
    db: Arc<SqliteDb>,
    task_id: String,
    execution_id: String,
    workspace_lease_id: String,
}

/// Build a repository-backed execution and a scheduler lease. The project is
/// intentionally a legacy-unverified/setup-required project so the fixture
/// does not need to manufacture a Charter or execution baseline merely to
/// exercise the execution CAS. The workspace-lease trigger still verifies
/// that the task, assigned executor, repository, and execution agree.
async fn fixture() -> ExecutionFixture {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("SQLite pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = now_rfc3339();

    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Execution liveness acceptance".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project fixture creates");

    let repo_id = new_uuid_v4();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "execution-liveness".to_owned(),
            remote_url: "https://example.invalid/execution-liveness.git".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repository fixture creates");

    let agent_id = new_uuid_v4();
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: agent_id.clone(),
            name: "execution-liveness-worker".to_owned(),
            description: None,
            executor_type: "embedded".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 2,
            heartbeat_interval_seconds: 1,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("worker fixture creates");

    let task_id = new_uuid_v4();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            assignee_type: Some("agent".to_owned()),
            assignee_id: Some(agent_id.clone()),
            title: "Execution liveness fixture".to_owned(),
            description: Some("silent provider/tool execution".to_owned()),
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task fixture creates");

    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
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
            summary: Some("provider request is in flight".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution fixture creates");

    let workspace_lease_id = new_uuid_v4();
    WorkspaceLeaseRepo::issue(
        &*db,
        CreateWorkspaceLease {
            id: workspace_lease_id.clone(),
            project_id,
            task_id: task_id.clone(),
            task_version: 1,
            execution_id: execution_id.clone(),
            operation_idempotency_key: format!("execution-liveness:{execution_id}"),
            repository_binding_id: repo_id,
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
            issued_at: T0.to_owned(),
            expires_at: "9999-01-01T00:00:00+00:00".to_owned(),
            created_at: T0.to_owned(),
            updated_at: T0.to_owned(),
        },
    )
    .await
    .expect("workspace lease fixture creates");

    ExecutionFixture {
        db,
        task_id,
        execution_id,
        workspace_lease_id,
    }
}

async fn terminal_event_count(db: &SqliteDb, task_id: &str, execution_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event
         WHERE event_type IN ('execution.completed', 'execution.failed', 'execution.cancelled')
           AND entity_id = ?
           AND json_extract(payload_json, '$.execution_id') = ?",
    )
    .bind(task_id)
    .bind(execution_id)
    .fetch_one(db.pool())
    .await
    .expect("terminal event count query succeeds")
}

fn updated(mutation: ExecutionLeaseMutation) -> db::Execution {
    match mutation {
        ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("expected updated lease mutation, got {other:?}"),
    }
}

fn terminal_input(
    execution_id: &str,
    expected_version: i64,
    owner: Option<&str>,
    terminal: (ExecutionStatus, db::StopReason),
    actor_type: &str,
    lease_disposition: ExecutionLeaseDisposition,
    updated_at: &str,
) -> TerminalizeExecution {
    let (status, stop_reason) = terminal;
    TerminalizeExecution {
        execution_id: execution_id.to_owned(),
        expected_version,
        lease_owner: owner.map(str::to_owned),
        status,
        stop_reason: Some(Some(stop_reason)),
        stopped_by: Some(Some(actor_type.to_owned())),
        stopped_at: Some(Some(updated_at.to_owned())),
        resume_policy: None,
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        last_progress_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: Some(Some(format!("terminalized by {actor_type}"))),
        executor_config_snapshot_json: None,
        updated_at: updated_at.to_owned(),
        actor_type: actor_type.to_owned(),
        actor_id: None,
        correlation_id: Some(format!("gate-d:{execution_id}:{actor_type}")),
        causation_id: None,
        causation_depth: 0,
        lease_disposition,
    }
}

fn committed_status(outcomes: &[ExecutionTerminalOutcome]) -> ExecutionStatus {
    let committed = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            ExecutionTerminalOutcome::Committed { execution, .. } => Some(execution),
            ExecutionTerminalOutcome::Concurrent { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(committed.len(), 1, "exactly one terminal CAS must win");
    committed[0].status.clone()
}

#[tokio::test]
async fn d1_silent_provider_wait_renews_until_hard_deadline_without_progress() {
    let fixture = fixture().await;
    let owner = "embedded-owner";

    let claimed = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: 1,
                owner: owner.to_owned(),
                lease_expires_at: T5.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("owner claim succeeds"),
    );

    // Semantic progress is deliberately stale. It is recorded through its
    // own owner/version check and must not act as the heartbeat.
    let progressed = updated(
        ExecutionRepo::record_progress(
            &*fixture.db,
            db::RecordExecutionProgress {
                execution_id: fixture.execution_id.clone(),
                expected_version: claimed.execution_version,
                owner: owner.to_owned(),
                progress_at: STALE_PROGRESS.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("stale semantic progress records"),
    );
    assert_eq!(progressed.last_progress_at.as_deref(), Some(STALE_PROGRESS));
    assert_eq!(progressed.execution_version, claimed.execution_version + 1);

    assert!(
        ExecutionRepo::list_expired_leases(&*fixture.db, T1, 100)
            .await
            .expect("lease expiry scan succeeds")
            .is_empty(),
        "a current owner lease remains live even when semantic progress is stale"
    );

    let renewed = updated(
        ExecutionRepo::renew_lease(
            &*fixture.db,
            db::RenewExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: progressed.execution_version,
                owner: owner.to_owned(),
                lease_expires_at: T8.to_owned(),
                now: T4.to_owned(),
            },
        )
        .await
        .expect("owner heartbeat renews"),
    );
    assert_eq!(renewed.last_heartbeat_at.as_deref(), Some(T4));
    assert_eq!(renewed.last_progress_at.as_deref(), Some(STALE_PROGRESS));
    assert_eq!(renewed.lease_expires_at.as_deref(), Some(T8));
    let stale_progress = ExecutionRepo::list_stale_progress(&*fixture.db, T4, T10, 100)
        .await
        .expect("semantic progress warning scan succeeds");
    assert_eq!(
        stale_progress.iter().map(|row| &row.id).collect::<Vec<_>>(),
        [&fixture.execution_id]
    );
    assert!(
        ExecutionRepo::list_expired_leases(&*fixture.db, T7, 100)
            .await
            .expect("lease expiry scan succeeds")
            .is_empty(),
        "generic recovery does not equate stale progress with owner death"
    );

    let renewed_again = updated(
        ExecutionRepo::renew_lease(
            &*fixture.db,
            db::RenewExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: renewed.execution_version,
                owner: owner.to_owned(),
                lease_expires_at: T19.to_owned(),
                now: T7.to_owned(),
            },
        )
        .await
        .expect("heartbeat renews before hard deadline"),
    );
    assert_eq!(renewed_again.lease_expires_at.as_deref(), Some(T19));
    assert_eq!(renewed_again.hard_deadline_at.as_deref(), Some(T20));

    let at_deadline = ExecutionRepo::renew_lease(
        &*fixture.db,
        db::RenewExecutionLease {
            execution_id: fixture.execution_id.clone(),
            expected_version: renewed_again.execution_version,
            owner: owner.to_owned(),
            lease_expires_at: "9999-01-01T00:00:00+00:00".to_owned(),
            now: T20.to_owned(),
        },
    )
    .await
    .expect("hard deadline renewal returns a typed outcome");
    assert!(
        matches!(at_deadline, ExecutionLeaseMutation::HardDeadline { .. }),
        "heartbeat cannot extend a running execution beyond its hard deadline"
    );
    let execution = ExecutionRepo::get_by_id(&*fixture.db, &fixture.execution_id)
        .await
        .expect("execution lookup succeeds")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    assert_eq!(execution.last_progress_at.as_deref(), Some(STALE_PROGRESS));
}

#[tokio::test]
async fn d2_dead_owner_expires_once_and_stale_owner_cannot_renew_or_progress() {
    let fixture = fixture().await;
    let owner = "dead-owner";
    let claimed = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: 1,
                owner: owner.to_owned(),
                lease_expires_at: T5.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("owner claim succeeds"),
    );

    let wrong_owner_claim = ExecutionRepo::claim_lease(
        &*fixture.db,
        ClaimExecutionLease {
            execution_id: fixture.execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: "wrong-daemon:stale-connection".to_owned(),
            lease_expires_at: T8.to_owned(),
            hard_deadline_at: T20.to_owned(),
            now: T1.to_owned(),
        },
    )
    .await
    .expect("wrong owner claim returns a typed outcome");
    assert!(matches!(
        wrong_owner_claim,
        ExecutionLeaseMutation::Concurrent { .. }
    ));

    let expired = ExecutionRepo::list_expired_leases(&*fixture.db, T6, 100)
        .await
        .expect("expired owner scan succeeds");
    assert_eq!(
        expired.iter().map(|row| &row.id).collect::<Vec<_>>(),
        [&fixture.execution_id]
    );

    // A replacement owner may take over only after expiry. The old owner is
    // still holding the pre-takeover version and must lose every write race.
    let replacement_owner = "replacement-owner";
    let replacement = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: claimed.execution_version,
                owner: replacement_owner.to_owned(),
                lease_expires_at: T8.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T6.to_owned(),
            },
        )
        .await
        .expect("replacement owner claim succeeds"),
    );
    let healthy_renewal = updated(
        ExecutionRepo::renew_lease(
            &*fixture.db,
            db::RenewExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: replacement.execution_version,
                owner: replacement_owner.to_owned(),
                lease_expires_at: T10.to_owned(),
                now: T7.to_owned(),
            },
        )
        .await
        .expect("replacement owner heartbeat renews"),
    );
    let stale_renewal_after_takeover = ExecutionRepo::renew_lease(
        &*fixture.db,
        db::RenewExecutionLease {
            execution_id: fixture.execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: owner.to_owned(),
            lease_expires_at: T8.to_owned(),
            now: T7.to_owned(),
        },
    )
    .await
    .expect("stale owner renewal returns a typed outcome");
    assert!(matches!(
        stale_renewal_after_takeover,
        ExecutionLeaseMutation::Concurrent { .. }
    ));

    let terminal = ExecutionRepo::terminalize(
        &*fixture.db,
        terminal_input(
            &fixture.execution_id,
            healthy_renewal.execution_version,
            Some(replacement_owner),
            (ExecutionStatus::Failed, db::StopReason::CrashRecovery),
            "monitor",
            ExecutionLeaseDisposition::Expire,
            T8,
        ),
    )
    .await
    .expect("monitor terminalization succeeds");
    let (terminal_execution, lease_status) = match terminal {
        ExecutionTerminalOutcome::Committed {
            execution,
            workspace_lease_status,
            ..
        } => (execution, workspace_lease_status),
        other => panic!("expired owner must terminalize once, got {other:?}"),
    };
    assert_eq!(terminal_execution.status, ExecutionStatus::Failed);
    assert_eq!(terminal_execution.lease_owner, None);
    assert_eq!(lease_status.as_deref(), Some("expired"));
    assert_eq!(
        terminal_event_count(&fixture.db, &fixture.task_id, &fixture.execution_id).await,
        1
    );

    assert!(ExecutionRepo::list_expired_leases(&*fixture.db, T8, 100)
        .await
        .expect("post-terminal expiry scan succeeds")
        .is_empty());

    let stale_renew = ExecutionRepo::renew_lease(
        &*fixture.db,
        db::RenewExecutionLease {
            execution_id: fixture.execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: owner.to_owned(),
            lease_expires_at: T10.to_owned(),
            now: T8.to_owned(),
        },
    )
    .await
    .expect("stale renewal returns a typed outcome");
    assert!(
        matches!(stale_renew, ExecutionLeaseMutation::Concurrent { .. }),
        "a terminalized execution rejects the old owner heartbeat"
    );

    let stale_progress = ExecutionRepo::record_progress(
        &*fixture.db,
        db::RecordExecutionProgress {
            execution_id: fixture.execution_id.clone(),
            expected_version: claimed.execution_version,
            owner: owner.to_owned(),
            progress_at: T8.to_owned(),
            now: T8.to_owned(),
        },
    )
    .await
    .expect("stale progress returns a typed outcome");
    assert!(matches!(
        stale_progress,
        ExecutionLeaseMutation::Concurrent { .. }
    ));

    let duplicate_terminal = ExecutionRepo::terminalize(
        &*fixture.db,
        terminal_input(
            &fixture.execution_id,
            healthy_renewal.execution_version,
            Some(replacement_owner),
            (ExecutionStatus::Failed, db::StopReason::CrashRecovery),
            "monitor-retry",
            ExecutionLeaseDisposition::Expire,
            T10,
        ),
    )
    .await
    .expect("duplicate monitor terminalization returns a typed outcome");
    assert!(matches!(
        duplicate_terminal,
        ExecutionTerminalOutcome::Concurrent { .. }
    ));
    assert_eq!(
        terminal_event_count(&fixture.db, &fixture.task_id, &fixture.execution_id).await,
        1
    );
    let lease = WorkspaceLeaseRepo::get_by_id(&*fixture.db, &fixture.workspace_lease_id)
        .await
        .expect("workspace lease lookup succeeds")
        .expect("workspace lease exists");
    assert_eq!(lease.status, "expired");
}

#[tokio::test]
async fn d3_completion_and_monitor_race_has_one_terminal_event_and_lease_disposition() {
    let fixture = fixture().await;
    let owner = "race-owner";
    let claimed = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: 1,
                owner: owner.to_owned(),
                lease_expires_at: T5.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("owner claim succeeds"),
    );
    let completion = terminal_input(
        &fixture.execution_id,
        claimed.execution_version,
        Some(owner),
        (ExecutionStatus::Completed, db::StopReason::LegacyUnknown),
        "runner",
        ExecutionLeaseDisposition::Revoke,
        T4,
    );
    let monitor = terminal_input(
        &fixture.execution_id,
        claimed.execution_version,
        Some(owner),
        (ExecutionStatus::Failed, db::StopReason::CrashRecovery),
        "monitor",
        ExecutionLeaseDisposition::Expire,
        T4,
    );
    let db_for_runner = Arc::clone(&fixture.db);
    let db_for_monitor = Arc::clone(&fixture.db);
    let (runner_result, monitor_result) = tokio::join!(
        async move { ExecutionRepo::terminalize(&*db_for_runner, completion).await },
        async move { ExecutionRepo::terminalize(&*db_for_monitor, monitor).await },
    );
    let outcomes = vec![
        runner_result.expect("runner race participant succeeds"),
        monitor_result.expect("monitor race participant succeeds"),
    ];
    let status = committed_status(&outcomes);
    assert!(matches!(
        status,
        ExecutionStatus::Completed | ExecutionStatus::Failed
    ));
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExecutionTerminalOutcome::Concurrent { .. }))
            .count(),
        1
    );
    assert_eq!(
        terminal_event_count(&fixture.db, &fixture.task_id, &fixture.execution_id).await,
        1
    );
    let lease = WorkspaceLeaseRepo::get_by_id(&*fixture.db, &fixture.workspace_lease_id)
        .await
        .expect("workspace lease lookup succeeds")
        .expect("workspace lease exists");
    assert!(matches!(lease.status.as_str(), "revoked" | "expired"));
    assert_eq!(lease.version, 2);
}

#[tokio::test]
async fn d3_cancellation_and_completion_race_has_one_terminal_event_and_lease_disposition() {
    let fixture = fixture().await;
    let owner = "cancel-race-owner";
    let claimed = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: 1,
                owner: owner.to_owned(),
                lease_expires_at: T5.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("owner claim succeeds"),
    );
    let completion = terminal_input(
        &fixture.execution_id,
        claimed.execution_version,
        Some(owner),
        (ExecutionStatus::Completed, db::StopReason::LegacyUnknown),
        "runner",
        ExecutionLeaseDisposition::Revoke,
        T4,
    );
    let cancellation = terminal_input(
        &fixture.execution_id,
        claimed.execution_version,
        Some(owner),
        (ExecutionStatus::Cancelled, db::StopReason::UserCancelled),
        "user",
        ExecutionLeaseDisposition::Revoke,
        T4,
    );
    let db_for_runner = Arc::clone(&fixture.db);
    let db_for_user = Arc::clone(&fixture.db);
    let (runner_result, cancellation_result) = tokio::join!(
        async move { ExecutionRepo::terminalize(&*db_for_runner, completion).await },
        async move { ExecutionRepo::terminalize(&*db_for_user, cancellation).await },
    );
    let outcomes = vec![
        runner_result.expect("runner race participant succeeds"),
        cancellation_result.expect("cancellation race participant succeeds"),
    ];
    let status = committed_status(&outcomes);
    assert!(matches!(
        status,
        ExecutionStatus::Completed | ExecutionStatus::Cancelled
    ));
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExecutionTerminalOutcome::Concurrent { .. }))
            .count(),
        1
    );
    assert_eq!(
        terminal_event_count(&fixture.db, &fixture.task_id, &fixture.execution_id).await,
        1
    );
    let lease = WorkspaceLeaseRepo::get_by_id(&*fixture.db, &fixture.workspace_lease_id)
        .await
        .expect("workspace lease lookup succeeds")
        .expect("workspace lease exists");
    assert_eq!(lease.status, "revoked");
    assert_eq!(lease.version, 2);
}

#[tokio::test]
async fn d3_late_runner_result_cannot_overwrite_monitor_winner() {
    let fixture = fixture().await;
    let owner = "late-runner-owner";
    let claimed = updated(
        ExecutionRepo::claim_lease(
            &*fixture.db,
            ClaimExecutionLease {
                execution_id: fixture.execution_id.clone(),
                expected_version: 1,
                owner: owner.to_owned(),
                lease_expires_at: T5.to_owned(),
                hard_deadline_at: T20.to_owned(),
                now: T0.to_owned(),
            },
        )
        .await
        .expect("owner claim succeeds"),
    );
    let monitor = ExecutionRepo::terminalize(
        &*fixture.db,
        terminal_input(
            &fixture.execution_id,
            claimed.execution_version,
            Some(owner),
            (ExecutionStatus::Failed, db::StopReason::CrashRecovery),
            "monitor",
            ExecutionLeaseDisposition::Expire,
            T6,
        ),
    )
    .await
    .expect("monitor terminalization succeeds");
    assert!(matches!(
        monitor,
        ExecutionTerminalOutcome::Committed { .. }
    ));

    let late_runner = ExecutionRepo::terminalize(
        &*fixture.db,
        terminal_input(
            &fixture.execution_id,
            claimed.execution_version,
            Some(owner),
            (ExecutionStatus::Completed, db::StopReason::LegacyUnknown),
            "late-runner",
            ExecutionLeaseDisposition::Revoke,
            T7,
        ),
    )
    .await
    .expect("late terminal result returns a typed concurrent outcome");
    assert!(matches!(
        late_runner,
        ExecutionTerminalOutcome::Concurrent { .. }
    ));
    let persisted = ExecutionRepo::get_by_id(&*fixture.db, &fixture.execution_id)
        .await
        .expect("execution lookup succeeds")
        .expect("execution exists");
    assert_eq!(persisted.status, ExecutionStatus::Failed);
    assert_eq!(persisted.error.as_deref(), Some("terminalized by monitor"));
    assert_eq!(
        terminal_event_count(&fixture.db, &fixture.task_id, &fixture.execution_id).await,
        1
    );
    let lease = WorkspaceLeaseRepo::get_by_id(&*fixture.db, &fixture.workspace_lease_id)
        .await
        .expect("workspace lease lookup succeeds")
        .expect("workspace lease exists");
    assert_eq!(lease.status, "expired");
    assert_eq!(lease.version, 2);
}
