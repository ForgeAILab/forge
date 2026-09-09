use db::{
    create_sqlite_pool, run_migrations, CostCoverageReasonCode, CreateExecution,
    CreatePricingSelection, CreateProject, CreateRepo, CreateTask, CreateUsageEvent,
    CreateUsageInvocation, DbError, ExecutionLeaseDisposition, ExecutionRepo, ExecutionStatus,
    ExecutionTerminalOutcome, PricingAdmissionProvenanceKind, PricingDomainKind,
    PricingSelectionStatus, ProjectRepo, RepoRepo, ResumePolicy, SqliteDb, StartUsageInvocation,
    TaskRepo, TerminalizeExecution, TerminalizeExecutionWithLedger, UsageEventProvenanceKind,
    UsageEventReportMode, UsageInvocationLifecycle, UsageLedgerSettlement, UsageSurface,
    UsageTelemetryState,
};

async fn db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations apply");
    SqliteDb::new(pool)
}

async fn seed_base(db: &SqliteDb) {
    let now = "2026-09-08T00:00:00Z";
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('receipt-owner', 'receipt-owner@example.test', 'test', 'Receipt owner', ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .expect("owner creates");
    ProjectRepo::create(
        db,
        CreateProject {
            id: "receipt-project".to_owned(),
            name: "Receipt project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("receipt-owner".to_owned()),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: "receipt-repo".to_owned(),
            project_id: "receipt-project".to_owned(),
            name: "receipt".to_owned(),
            remote_url: "https://example.test/receipt.git".to_owned(),
            local_path: Some("/tmp/receipt".to_owned()),
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("repo creates");
    TaskRepo::create(
        db,
        CreateTask {
            id: "receipt-task".to_owned(),
            project_id: "receipt-project".to_owned(),
            repo_id: Some("receipt-repo".to_owned()),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "receipt task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("task creates");
}

async fn seed_started_invocation(db: &SqliteDb, execution_id: &str, name: &str) -> String {
    let now = "2026-09-08T00:00:00Z";
    let selection_id = format!("selection-{name}");
    let invocation_id = format!("invocation-{name}");
    db::UsageLedgerRepo::create_pricing_selection(
        db,
        CreatePricingSelection {
            id: selection_id.clone(),
            owner_user_id: Some("receipt-owner".to_owned()),
            project_id: Some("receipt-project".to_owned()),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: execution_id.to_owned(),
            execution_id: Some(execution_id.to_owned()),
            task_id: Some("receipt-task".to_owned()),
            candidate_key: Some(name.to_owned()),
            attempt_ordinal: 0,
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: Some("receipt-model".to_owned()),
            admitted_provider_id: Some("receipt-provider".to_owned()),
            admitted_model_id: Some("receipt-model".to_owned()),
            source_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status: PricingSelectionStatus::Unpriced,
            selection_reason: Some(CostCoverageReasonCode::MissingBinding),
            selection_digest: format!("selection-digest-{name}"),
            selected_at: now.to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("pricing selection creates");
    let invocation = db::UsageLedgerRepo::create_usage_invocation(
        db,
        CreateUsageInvocation {
            id: invocation_id.clone(),
            owner_user_id: Some("receipt-owner".to_owned()),
            project_id: Some("receipt-project".to_owned()),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: execution_id.to_owned(),
            execution_id: Some(execution_id.to_owned()),
            task_id: Some("receipt-task".to_owned()),
            domain_idempotency_key: format!("receipt-call-{name}"),
            candidate_key: Some(name.to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: selection_id,
            admitted_provider_id: Some("receipt-provider".to_owned()),
            admitted_model_id: Some("receipt-model".to_owned()),
            admitted_runtime_model: Some("receipt-model".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("receipt".to_owned()),
            backend_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: now.to_owned(),
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("usage invocation creates");
    let started = db::UsageLedgerRepo::start_usage_invocation(
        db,
        StartUsageInvocation {
            id: invocation.id,
            expected_version: invocation.version,
            started_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("usage invocation starts");
    assert_eq!(started.lifecycle, UsageInvocationLifecycle::Started);
    started.id
}

fn usage_event_fixture(invocation_id: &str) -> CreateUsageEvent {
    CreateUsageEvent {
        id: "replay-event".to_owned(),
        invocation_id: invocation_id.to_owned(),
        owner_user_id: Some("receipt-owner".to_owned()),
        project_id: Some("receipt-project".to_owned()),
        surface: UsageSurface::TaskExecution,
        source_id: "settle-execution".to_owned(),
        execution_id: Some("settle-execution".to_owned()),
        task_id: Some("receipt-task".to_owned()),
        event_idempotency_key: "replay-event-key".to_owned(),
        source_report_id: "replay-report".to_owned(),
        report_sequence: 0,
        report_mode: UsageEventReportMode::FinalSnapshot,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        provider_id: Some("receipt-provider".to_owned()),
        model_id: Some("receipt-model".to_owned()),
        runtime_model: Some("receipt-model".to_owned()),
        candidate_key: Some("observed".to_owned()),
        attempt_ordinal: 0,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("receipt".to_owned()),
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        telemetry_state: UsageTelemetryState::Metered,
        input_tokens: Some(1),
        output_tokens: Some(2),
        cache_read_tokens: Some(0),
        cache_write_tokens: Some(0),
        context_tokens: None,
        selected_tier: None,
        provider_reported_nano_usd: None,
        legacy_reported_cost_usd: None,
        estimated_nano_usd: None,
        cost_kind: db::UsageCostKind::None,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: Some(CostCoverageReasonCode::MissingBinding),
        occurred_at: "2026-09-08T00:00:01Z".to_owned(),
        created_at: "2026-09-08T00:00:01Z".to_owned(),
    }
}

async fn seed_execution(db: &SqliteDb, execution_id: &str) {
    let now = "2026-09-08T00:00:00Z";
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.to_owned(),
            task_id: "receipt-task".to_owned(),
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
            last_activity_at: Some(now.to_owned()),
            summary: None,
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
    .expect("execution creates");
}

fn terminal_input(
    execution_id: &str,
    report_id: &str,
    digest: &str,
) -> TerminalizeExecutionWithLedger {
    TerminalizeExecutionWithLedger {
        terminal: TerminalizeExecution {
            execution_id: execution_id.to_owned(),
            expected_version: 1,
            lease_owner: None,
            status: ExecutionStatus::Completed,
            stop_reason: Some(None),
            stopped_by: Some(None),
            stopped_at: Some(Some("2026-09-08T00:00:01Z".to_owned())),
            resume_policy: Some(Some(ResumePolicy::None)),
            agent_session_id: Some(None),
            agent_message_id: Some(None),
            last_activity_at: Some(None),
            last_progress_at: Some(None),
            summary: Some(Some("done".to_owned())),
            logs_path: Some(None),
            before_sha: None,
            after_sha: None,
            error: Some(None),
            executor_config_snapshot_json: Some(None),
            updated_at: "2026-09-08T00:00:01Z".to_owned(),
            actor_type: "daemon".to_owned(),
            actor_id: Some("daemon:receipt".to_owned()),
            correlation_id: Some("receipt-correlation".to_owned()),
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
        settlements: Vec::new(),
        terminal_report_id: Some(report_id.to_owned()),
        terminal_report_digest: Some(digest.to_owned()),
        mark_unreplayable_pending_unsettled: false,
        allow_late_settlement: false,
        preserve_pending_settlement: false,
        require_live_owner_lease: false,
    }
}

#[tokio::test]
async fn terminal_receipt_replays_empty_usage_after_restart_and_is_globally_unique() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "receipt-execution-1").await;
    let report_id = "terminal-report-empty";
    let digest = "a".repeat(64);

    let first = ExecutionRepo::terminalize_with_ledger(
        &db,
        terminal_input("receipt-execution-1", report_id, &digest),
    )
    .await
    .expect("first terminal report commits");
    assert!(matches!(
        first,
        ExecutionTerminalOutcome::Committed {
            replayed: false,
            ..
        }
    ));

    // A fresh repository handle models a server restart. The report has no
    // usage rows, but its durable receipt still makes an exact replay a
    // committed acknowledgement rather than an endless transport retry.
    let restarted = SqliteDb::new(db.pool().clone());
    let replay = ExecutionRepo::terminalize_with_ledger(
        &restarted,
        terminal_input("receipt-execution-1", report_id, &digest),
    )
    .await
    .expect("exact replay acknowledges");
    assert!(matches!(
        replay,
        ExecutionTerminalOutcome::Committed { replayed: true, .. }
    ));
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type = 'execution.completed'",
    )
    .fetch_one(restarted.pool())
    .await
    .expect("terminal event count reads");
    assert_eq!(event_count, 1);

    // Reusing the id for a different payload conflicts before a second
    // terminal mutation can occur.
    let conflicting_payload = ExecutionRepo::terminalize_with_ledger(
        &restarted,
        terminal_input("receipt-execution-1", report_id, &"b".repeat(64)),
    )
    .await;
    assert!(matches!(
        conflicting_payload,
        Err(DbError::IdempotencyConflict)
    ));

    // The same report id is also reserved across executions, even when the
    // payload digest is byte-for-byte identical.
    seed_execution(&restarted, "receipt-execution-2").await;
    let conflicting_execution = ExecutionRepo::terminalize_with_ledger(
        &restarted,
        terminal_input("receipt-execution-2", report_id, &digest),
    )
    .await;
    assert!(matches!(
        conflicting_execution,
        Err(DbError::IdempotencyConflict)
    ));
    let second = ExecutionRepo::get_by_id(&restarted, "receipt-execution-2")
        .await
        .expect("second execution reads")
        .expect("second execution exists");
    assert_eq!(second.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn expired_remote_lease_cannot_terminalize_or_create_ledger_rows() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "expired-remote-execution").await;
    sqlx::query(
        "UPDATE execution
         SET lease_owner = ?, lease_expires_at = ?, hard_deadline_at = ?
         WHERE id = ?",
    )
    .bind("daemon:expired-daemon:connection:7")
    .bind("2026-09-08T00:00:00Z")
    .bind("2026-09-08T01:00:00Z")
    .bind("expired-remote-execution")
    .execute(db.pool())
    .await
    .expect("expired lease seeds");

    let mut terminal = terminal_input(
        "expired-remote-execution",
        "expired-remote-report",
        &"e".repeat(64),
    );
    terminal.terminal.lease_owner = Some("daemon:expired-daemon:connection:7".to_owned());
    terminal.require_live_owner_lease = true;
    terminal.terminal.updated_at = "2026-09-08T00:00:01Z".to_owned();
    terminal.terminal_report_id = None;
    terminal.terminal_report_digest = None;
    let outcome = ExecutionRepo::terminalize_with_ledger(&db, terminal)
        .await
        .expect("expired lease returns typed concurrency");
    assert!(matches!(
        outcome,
        ExecutionTerminalOutcome::Concurrent { current: Some(_) }
    ));
    assert_eq!(
        ExecutionRepo::get_by_id(&db, "expired-remote-execution")
            .await
            .expect("execution reads")
            .expect("execution exists")
            .status,
        ExecutionStatus::Running
    );
    let ledger_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_invocation WHERE source_id = 'expired-remote-execution'",
    )
    .fetch_one(db.pool())
    .await
    .expect("ledger rows count");
    assert_eq!(ledger_rows, 0);
    let receipt_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_terminal_receipt
         WHERE terminal_report_id = 'expired-remote-report'",
    )
    .fetch_one(db.pool())
    .await
    .expect("receipt rows count");
    assert_eq!(receipt_rows, 0);
}

#[tokio::test]
async fn expired_local_lease_still_terminalizes() {
    // The mirror of the remote case above. A local caller — a user cancel, or
    // the recovery reaper — is not a lease holder proving ownership; it is the
    // authority that revokes one, and an expired lease is exactly the state it
    // exists to clear. Holding it to the daemon's liveness proof made a
    // stranded execution impossible to stop.
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "expired-local-execution").await;
    sqlx::query(
        "UPDATE execution
         SET lease_owner = ?, lease_expires_at = ?, hard_deadline_at = ?
         WHERE id = ?",
    )
    .bind("daemon:abandoned-daemon:connection:3")
    .bind("2026-09-08T00:00:00Z")
    .bind("2026-09-08T00:00:00Z")
    .bind("expired-local-execution")
    .execute(db.pool())
    .await
    .expect("expired lease seeds");

    let mut terminal = terminal_input(
        "expired-local-execution",
        "expired-local-report",
        &"f".repeat(64),
    );
    // A daemon-owned row on purpose: the recovery reaper carries whatever owner
    // the execution already holds, so gating liveness on the token would strand
    // exactly the abandoned daemon executions it exists to reap.
    terminal.terminal.lease_owner = Some("daemon:abandoned-daemon:connection:3".to_owned());
    terminal.terminal.status = ExecutionStatus::Cancelled;
    terminal.terminal.updated_at = "2026-09-08T00:00:01Z".to_owned();
    terminal.terminal_report_id = None;
    terminal.terminal_report_digest = None;
    let outcome = ExecutionRepo::terminalize_with_ledger(&db, terminal)
        .await
        .expect("local cancellation terminalizes");
    assert!(matches!(
        outcome,
        ExecutionTerminalOutcome::Committed { .. }
    ));
    assert_eq!(
        ExecutionRepo::get_by_id(&db, "expired-local-execution")
            .await
            .expect("execution reads")
            .expect("execution exists")
            .status,
        ExecutionStatus::Cancelled
    );
}

#[tokio::test]
async fn remote_invocation_materialization_rolls_back_with_terminal_cas() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "remote-rollback-execution").await;

    let terminal = terminal_input(
        "remote-rollback-execution",
        "remote-rollback-report",
        &"d".repeat(64),
    );
    let invalid_invocation = CreateUsageInvocation {
        id: "remote-rollback-invocation".to_owned(),
        owner_user_id: Some("receipt-owner".to_owned()),
        project_id: Some("receipt-project".to_owned()),
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: "remote-rollback-execution".to_owned(),
        execution_id: Some("remote-rollback-execution".to_owned()),
        task_id: Some("receipt-task".to_owned()),
        domain_idempotency_key: "remote-rollback-call".to_owned(),
        candidate_key: Some("remote".to_owned()),
        attempt_ordinal: 0,
        pricing_selection_id: "selection-does-not-exist".to_owned(),
        admitted_provider_id: Some("provider".to_owned()),
        admitted_model_id: Some("model".to_owned()),
        admitted_runtime_model: Some("model".to_owned()),
        pricing_subject_id: None,
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        agent_id: None,
        profile_id: None,
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("remote".to_owned()),
        backend_kind: None,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        admitted_at: "2026-09-08T00:00:01Z".to_owned(),
        created_at: "2026-09-08T00:00:01Z".to_owned(),
        updated_at: "2026-09-08T00:00:01Z".to_owned(),
    };
    let result = ExecutionRepo::terminalize_with_ledger_and_invocations(
        &db,
        terminal.clone(),
        vec![invalid_invocation],
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        ExecutionRepo::get_by_id(&db, "remote-rollback-execution")
            .await
            .expect("execution reads")
            .expect("execution exists")
            .status,
        ExecutionStatus::Running
    );
    let invocation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM usage_invocation WHERE id = 'remote-rollback-invocation'",
    )
    .fetch_one(db.pool())
    .await
    .expect("invocation count reads");
    assert_eq!(invocation_count, 0);
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_terminal_receipt
         WHERE terminal_report_id = 'remote-rollback-report'",
    )
    .fetch_one(db.pool())
    .await
    .expect("receipt count reads");
    assert_eq!(receipt_count, 0);
}

#[tokio::test]
async fn cancelled_terminal_settles_observed_usage_and_leaves_only_unresolved_pending() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "cancel-execution").await;
    let observed = seed_started_invocation(&db, "cancel-execution", "observed").await;
    let unresolved = seed_started_invocation(&db, "cancel-execution", "unresolved").await;

    let mut cancellation = terminal_input("cancel-execution", "", "");
    cancellation.terminal.status = ExecutionStatus::Cancelled;
    cancellation.terminal_report_id = None;
    cancellation.terminal_report_digest = None;
    cancellation.settlements = vec![UsageLedgerSettlement {
        invocation_id: observed.clone(),
        expected_version: 2,
        telemetry_state: UsageTelemetryState::Unmetered,
        terminal_reason: Some("observed_before_cancel".to_owned()),
        settled_at: "2026-09-08T00:00:01Z".to_owned(),
        updated_at: "2026-09-08T00:00:01Z".to_owned(),
        events: Vec::new(),
    }];
    let committed = ExecutionRepo::terminalize_with_ledger(&db, cancellation)
        .await
        .expect("cancellation commits");
    let cancelled = match committed {
        ExecutionTerminalOutcome::Committed { execution, .. } => execution,
        other => panic!("unexpected cancellation result: {other:?}"),
    };
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);

    let observed_row = db::UsageLedgerRepo::get_usage_invocation(&db, &observed)
        .await
        .expect("observed invocation reads")
        .expect("observed invocation exists");
    assert_eq!(observed_row.lifecycle, UsageInvocationLifecycle::Settled);
    assert_eq!(
        observed_row.terminal_reason.as_deref(),
        Some("observed_before_cancel")
    );
    let unresolved_row = db::UsageLedgerRepo::get_usage_invocation(&db, &unresolved)
        .await
        .expect("unresolved invocation reads")
        .expect("unresolved invocation exists");
    assert_eq!(
        unresolved_row.lifecycle,
        UsageInvocationLifecycle::PendingSettlement
    );

    // A late daemon report settles the pending call, but its CAS loser path
    // never changes the already-cancelled execution outcome.
    let mut late = terminal_input("cancel-execution", "late-cancel-report", &"c".repeat(64));
    late.terminal.expected_version = cancelled.execution_version;
    late.allow_late_settlement = true;
    late.terminal.status = ExecutionStatus::Completed;
    late.settlements = vec![UsageLedgerSettlement {
        invocation_id: unresolved,
        expected_version: unresolved_row.version,
        telemetry_state: UsageTelemetryState::Unmetered,
        terminal_reason: Some("late_observed".to_owned()),
        settled_at: "2026-09-08T00:00:02Z".to_owned(),
        updated_at: "2026-09-08T00:00:02Z".to_owned(),
        events: Vec::new(),
    }];
    let late_outcome = ExecutionRepo::terminalize_with_ledger(&db, late)
        .await
        .expect("late settlement commits");
    let late_execution = match late_outcome {
        ExecutionTerminalOutcome::Committed { execution, .. } => execution,
        other => panic!("unexpected late settlement result: {other:?}"),
    };
    assert_eq!(late_execution.status, ExecutionStatus::Cancelled);
    let settled_late = db::UsageLedgerRepo::get_usage_invocation(&db, "invocation-unresolved")
        .await
        .expect("late invocation reads")
        .expect("late invocation exists");
    assert_eq!(settled_late.lifecycle, UsageInvocationLifecycle::Settled);
}

#[tokio::test]
async fn unreplayable_recovery_terminal_marks_local_attempt_unsettled_atomically() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "unreplayable-execution").await;
    let invocation_id = seed_started_invocation(&db, "unreplayable-execution", "local").await;

    let mut terminal = terminal_input("unreplayable-execution", "", "");
    terminal.terminal.status = ExecutionStatus::Failed;
    terminal.terminal_report_id = None;
    terminal.terminal_report_digest = None;
    terminal.mark_unreplayable_pending_unsettled = true;
    let committed = ExecutionRepo::terminalize_with_ledger(&db, terminal)
        .await
        .expect("unreplayable terminal commits");
    assert!(matches!(
        committed,
        ExecutionTerminalOutcome::Committed { .. }
    ));

    let invocation = db::UsageLedgerRepo::get_usage_invocation(&db, &invocation_id)
        .await
        .expect("invocation reads")
        .expect("invocation exists");
    assert_eq!(invocation.lifecycle, UsageInvocationLifecycle::Unsettled);
    assert_eq!(
        invocation.terminal_reason.as_deref(),
        Some("recovery_no_replayable_result")
    );
}

#[tokio::test]
async fn settled_invocation_replay_compares_terminal_fields_and_all_event_fields() {
    let db = db().await;
    seed_base(&db).await;
    seed_execution(&db, "settle-execution").await;
    let invocation_id = seed_started_invocation(&db, "settle-execution", "observed").await;
    let event = usage_event_fixture(&invocation_id);
    let settlement = UsageLedgerSettlement {
        invocation_id: invocation_id.clone(),
        expected_version: 2,
        telemetry_state: UsageTelemetryState::Metered,
        terminal_reason: None,
        settled_at: "2026-09-08T00:00:01Z".to_owned(),
        updated_at: "2026-09-08T00:00:01Z".to_owned(),
        events: vec![event.clone()],
    };
    db::UsageLedgerRepo::settle_usage_invocations_with_events(&db, vec![settlement.clone()])
        .await
        .expect("settlement commits");
    db::UsageLedgerRepo::settle_usage_invocations_with_events(&db, vec![settlement.clone()])
        .await
        .expect("identical settlement replays");

    let mut changed_terminal = settlement.clone();
    changed_terminal.terminal_reason = Some("different reason".to_owned());
    assert!(matches!(
        db::UsageLedgerRepo::settle_usage_invocations_with_events(&db, vec![changed_terminal])
            .await,
        Err(DbError::IdempotencyConflict)
    ));

    let mut changed_event = settlement;
    changed_event.events[0].input_tokens = Some(99);
    assert!(matches!(
        db::UsageLedgerRepo::settle_usage_invocations_with_events(&db, vec![changed_event]).await,
        Err(DbError::IdempotencyConflict)
    ));
}
