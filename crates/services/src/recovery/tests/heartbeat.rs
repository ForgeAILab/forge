//! Heartbeat recovery cases.

use super::*;

#[tokio::test]
async fn heartbeat_monitor_times_out_busy_agents_and_recovers_tasks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let old_heartbeat = "1970-01-01T00:00:00+00:00".to_owned();
    let agent = seed_agent(&db, AgentStatus::Busy, Some(old_heartbeat.clone())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let _execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;

    let relay = crate::DomainEventBroadcastConsumer::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Some(db.domain_event_head().await.unwrap()),
    );
    let monitor =
        HeartbeatMonitor::with_check_interval(Arc::clone(&db), event_bus, Duration::from_millis(5));
    let timed_out = monitor.check_once().await.expect("monitor runs");
    assert_eq!(timed_out, 1);
    relay.broadcast_once(100).await.unwrap();

    let updated_agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    assert_eq!(updated_agent.status, AgentStatus::Error);

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let recovered_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(recovered_task.status, "in_progress".to_owned());
    assert_eq!(recovered_task.assignee_id, task.assignee_id);
    let annotation: Value =
        serde_json::from_str(recovered_task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "recovery_required");
    assert_eq!(annotation["blocking_reason"], "agent_timeout");
    assert_eq!(annotation["blocked_by"], "system:heartbeat_monitor");
    assert_eq!(
        annotation["message"],
        "Recovered after agent heartbeat timeout"
    );
    assert!(annotation.get("recovery_actions").is_none());

    let mut event_types = Vec::new();
    let mut recovered_event_id = None;
    while let Ok(event) = rx.try_recv() {
        if event.event_type == "task.recovered" {
            recovered_event_id = Some(event.entity_id);
        }
        event_types.push(event.event_type);
    }
    assert!(event_types.iter().any(|event| event == "agent.timeout"));
    assert!(event_types
        .iter()
        .any(|event| event == "domain_event.committed"));
    assert!(event_types.iter().any(|event| event == "task.recovered"));
    assert_eq!(recovered_event_id.as_deref(), Some(task.id.as_str()));
}

#[tokio::test]
async fn heartbeat_monitor_stale_busy_agent_does_not_cancel_healthy_quiet_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(
        &db,
        AgentStatus::Busy,
        Some("1970-01-01T00:00:00+00:00".to_owned()),
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let claimed = match ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: "quiet-owner".to_owned(),
            lease_expires_at: (now + ChronoDuration::minutes(5)).to_rfc3339(),
            hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
            now: now_text.clone(),
        },
    )
    .await
    .expect("execution lease claims")
    {
        db::ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("execution lease is not claimed: {other:?}"),
    };
    let renewed = match ExecutionRepo::renew_lease(
        &*db,
        db::RenewExecutionLease {
            execution_id: claimed.id.clone(),
            expected_version: claimed.execution_version,
            owner: "quiet-owner".to_owned(),
            lease_expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
            now: now_text,
        },
    )
    .await
    .expect("execution lease renews")
    {
        db::ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("execution lease is not renewed: {other:?}"),
    };

    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus));
    let timed_out = monitor.check_once().await.expect("monitor runs");
    assert_eq!(timed_out, 1, "only the legacy agent timeout is observed");

    let updated_agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .expect("agent loads")
        .expect("agent exists");
    assert_eq!(updated_agent.status, AgentStatus::Error);

    let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated_execution.status, ExecutionStatus::Running);
    assert_eq!(
        updated_execution.lease_owner.as_deref(),
        Some("quiet-owner")
    );
    assert_eq!(
        updated_execution.execution_version, renewed.execution_version,
        "the stale agent path did not terminalize or mutate the healthy owner lease"
    );
    assert!(updated_execution
        .lease_expires_at
        .as_deref()
        .is_some_and(|expires_at| rfc3339_is_after(expires_at, &now_rfc3339())));

    let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(updated_task.error_annotation.is_none());
}

#[tokio::test]
async fn heartbeat_monitor_marks_stalled_executions_and_schedules_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(
        &db,
        &crate::embedded_daemon::embedded_machine_id(),
        AgentStatus::Idle,
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
    ExecutionRepo::update(
        &*db,
        UpdateExecution {
            id: execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("execution activity updates");

    let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    let now = now_rfc3339();
    assert!(matches!(
        ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution_for_claim.execution_version,
                owner: "expired-owner".to_owned(),
                lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                now,
            },
        )
        .await
        .expect("execution lease claims"),
        db::ExecutionLeaseMutation::Updated(_)
    ));

    let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let executor = Arc::new(RecordingCancelExecutor::default());
    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_service(task_service)
        .with_task_executor(executor.clone())
        .with_execution_stall_timeout(Duration::from_secs(1));

    let stalled = monitor.check_once().await.expect("monitor checks");

    assert_eq!(stalled, 1);
    assert_eq!(
        executor
            .cancelled
            .lock()
            .expect("cancel log lock")
            .as_slice(),
        std::slice::from_ref(&execution.id)
    );
    let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated_execution.status, ExecutionStatus::Failed);
    assert_eq!(
        updated_execution.stop_reason,
        Some(StopReason::ExecutionStalled)
    );
    assert_eq!(updated_execution.resume_policy, Some(ResumePolicy::Auto));

    let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata: Value = serde_json::from_str(updated_task.metadata_json.as_deref().unwrap())
        .expect("metadata parses");
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        1
    );
    assert_eq!(
        metadata["deferred_dispatch"]["reason"],
        "execution retry (attempt 1)"
    );

    let mut event_types = Vec::new();
    for _ in 0..3 {
        event_types.push(rx.recv().await.expect("event receives").event_type);
    }
    assert!(event_types.iter().any(|event| event == "execution.stalled"));
    assert!(event_types
        .iter()
        .any(|event| event == "task.execution_retry"));
}

#[tokio::test]
async fn heartbeat_monitor_routes_expired_reviewer_through_review_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(
        &db,
        &crate::embedded_daemon::embedded_machine_id(),
        AgentStatus::Idle,
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        default_states::REVIEW.to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let assignment_now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: default_roles::REVIEWER.to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: assignment_now.clone(),
            updated_at: assignment_now,
        },
    )
    .await
    .expect("reviewer role assignment creates");
    sqlx::query("UPDATE task SET task_state_config = ?, metadata_json = ? WHERE id = ?")
        .bind(r#"{"retry_budgets":{"execution":3}}"#)
        .bind("{}")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("retry policy updates");

    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
    sqlx::query("UPDATE execution SET role = ? WHERE id = ?")
        .bind(default_roles::REVIEWER)
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .expect("reviewer role updates");
    let now = now_rfc3339();
    ReviewRepo::create(
        &*db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("running review creates");

    let current = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert!(matches!(
        ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: current.id.clone(),
                expected_version: current.execution_version,
                owner: "expired-review-owner".to_owned(),
                lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                now,
            },
        )
        .await
        .expect("execution lease claims"),
        db::ExecutionLeaseMutation::Updated(_)
    ));

    let task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let monitor = HeartbeatMonitor::new(Arc::clone(&db), event_bus)
        .with_task_service(task_service)
        .with_execution_stall_timeout(Duration::from_secs(1));
    assert_eq!(monitor.check_once().await.expect("monitor checks"), 1);

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    assert!(reviews[0].finished_at.is_none());
    let details: Value =
        serde_json::from_str(&reviews[0].step_results_json).expect("review details parse");
    assert_eq!(details["execution_retry"]["execution_id"], execution.id);
    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(current_task.blocked_json.is_none());
    let metadata: Value =
        serde_json::from_str(current_task.metadata_json.as_deref().unwrap_or("{}"))
            .expect("metadata parses");
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        1
    );
    assert!(metadata.get("deferred_dispatch").is_some());
}

#[tokio::test]
async fn heartbeat_monitor_warns_on_live_stale_progress_without_terminalizing() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(
        &db,
        &crate::embedded_daemon::embedded_machine_id(),
        AgentStatus::Idle,
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let claimed = match ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: "live-owner".to_owned(),
            lease_expires_at: (now + ChronoDuration::minutes(5)).to_rfc3339(),
            hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
            now: now_text.clone(),
        },
    )
    .await
    .expect("execution lease claims")
    {
        db::ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("execution lease is not claimed: {other:?}"),
    };
    sqlx::query("UPDATE execution SET last_progress_at = ? WHERE id = ?")
        .bind("1970-01-01T00:00:00+00:00")
        .bind(&claimed.id)
        .execute(db.pool())
        .await
        .expect("stale progress updates");

    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_execution_stall_timeout(Duration::from_secs(1));
    let first = monitor.check_once().await.expect("first monitor pass");
    let second = monitor.check_once().await.expect("second monitor pass");

    assert_eq!(first, 1, "one bounded warning is emitted for the episode");
    assert_eq!(
        second, 0,
        "the warning is deduplicated on subsequent passes"
    );
    let persisted = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(persisted.status, ExecutionStatus::Running);
    let persisted_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert!(persisted_task.error_annotation.is_none());
    assert!(persisted_task.blocked_json.is_none());
    let events = DomainEventRepo::list_events_after(&*db, 0, 100)
        .await
        .expect("domain events load");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == "execution.progress_warning" && event.entity_id == task.id
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn heartbeat_monitor_terminal_cas_race_emits_one_terminal_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(
        &db,
        &crate::embedded_daemon::embedded_machine_id(),
        AgentStatus::Idle,
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
    let now = Utc::now();
    let now_text = now.to_rfc3339();
    let claimed = match ExecutionRepo::claim_lease(
        &*db,
        db::ClaimExecutionLease {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            owner: "race-owner".to_owned(),
            lease_expires_at: (now - ChronoDuration::minutes(1)).to_rfc3339(),
            hard_deadline_at: Some((now + ChronoDuration::hours(1)).to_rfc3339()),
            now: now_text.clone(),
        },
    )
    .await
    .expect("execution lease claims")
    {
        db::ExecutionLeaseMutation::Updated(execution) => execution,
        other => panic!("execution lease is not claimed: {other:?}"),
    };

    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus));
    let db_for_completion = Arc::clone(&db);
    let completion = db::TerminalizeExecution {
        execution_id: claimed.id.clone(),
        expected_version: claimed.execution_version,
        lease_owner: claimed.lease_owner.clone(),
        status: ExecutionStatus::Completed,
        stop_reason: Some(Some(StopReason::LegacyUnknown)),
        stopped_by: Some(Some("runner".to_owned())),
        stopped_at: Some(Some(now_text.clone())),
        resume_policy: Some(Some(ResumePolicy::None)),
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        last_progress_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        updated_at: now_text,
        actor_type: "runner".to_owned(),
        actor_id: None,
        correlation_id: None,
        causation_id: None,
        causation_depth: 0,
        lease_disposition: ExecutionLeaseDisposition::Revoke,
    };
    let (monitor_result, completion_result) = tokio::join!(monitor.check_once(), async move {
        ExecutionRepo::terminalize(&*db_for_completion, completion).await
    });
    monitor_result.expect("monitor race pass");
    completion_result.expect("completion race pass");

    let persisted = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert!(matches!(
        persisted.status,
        ExecutionStatus::Completed | ExecutionStatus::Failed
    ));
    let events = DomainEventRepo::list_events_after(&*db, 0, 100)
        .await
        .expect("domain events load");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type.starts_with("execution.")
                    && event.entity_id == task.id
                    && event.payload_json.contains(&execution.id)
            })
            .count(),
        1,
        "the monitor and completion race has one durable terminal event"
    );
}

#[tokio::test]
async fn supervised_heartbeat_recovers_panic_reports_budget_without_cancelling_and_stops() {
    use std::sync::atomic::AtomicUsize;
    struct InFlight(Arc<AtomicBool>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let db = Arc::new(sqlite_db().await);
    let workers = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
    let monitor = Arc::new(HeartbeatMonitor::with_check_interval(
        db,
        Arc::new(EventBus::new(16)),
        Duration::from_secs(300),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let in_flight = Arc::new(AtomicBool::new(false));
    let resume = Arc::new(tokio::sync::Notify::new());
    let complete = Arc::new(tokio::sync::Notify::new());
    let handle = Arc::clone(&monitor).start_with_check(&workers, Duration::from_millis(100), {
        let calls = Arc::clone(&calls);
        let in_flight = Arc::clone(&in_flight);
        let resume = Arc::clone(&resume);
        let complete = Arc::clone(&complete);
        move |_| {
            let calls = Arc::clone(&calls);
            let in_flight = Arc::clone(&in_flight);
            let resume = Arc::clone(&resume);
            let complete = Arc::clone(&complete);
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("heartbeat tick panic");
                }
                in_flight.store(true, Ordering::SeqCst);
                let _in_flight = InFlight(in_flight);
                resume.notified().await;
                complete.notify_one();
                Ok(())
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !workers.status().await.unwrap().iter().any(|row| {
            row.worker_name == "heartbeat-monitor"
                && row
                    .last_error
                    .as_deref()
                    .is_some_and(|error| error.contains("tick running longer than 100ms"))
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(in_flight.load(Ordering::SeqCst));
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), complete.notified())
        .await
        .unwrap();
    monitor.stop();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    let row = workers.status().await.unwrap().remove(0);
    assert!(!row.running);
    assert_eq!(row.restart_count, 1);
    assert!(row.last_error.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!in_flight.load(Ordering::SeqCst));
}

#[tokio::test]
async fn heartbeat_monitor_start_stops() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let monitor = Arc::new(HeartbeatMonitor::with_check_interval(
        Arc::clone(&db),
        event_bus,
        Duration::from_millis(1),
    ));

    let handle = Arc::clone(&monitor).start(&crate::worker_runtime::PeriodicWorkers::new(
        Arc::clone(&db),
    ));
    monitor.stop();
    handle.await.expect("monitor task joins");
    assert!(monitor.is_stopped());
}

#[tokio::test]
async fn heartbeat_monitor_skips_embedded_daemon_executions_for_disconnect_check() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(
        &db,
        &crate::embedded_daemon::embedded_machine_id(),
        AgentStatus::Idle,
    )
    .await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;

    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_daemon_connections(Arc::clone(&registry));

    monitor.check_once().await.expect("first check");
    tokio::time::sleep(Duration::from_millis(5)).await;
    let interrupted = monitor.check_once().await.expect("second check");
    assert_eq!(interrupted, 0);

    let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn heartbeat_monitor_does_not_cancel_remote_stalled_executions_via_embedded_executor() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let (agent_id, _) = seed_agent_with_daemon(&db, "remote-machine-d", AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent_id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent_id, None).await;
    ExecutionRepo::update(
        &*db,
        UpdateExecution {
            id: execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("execution activity updates");

    let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    let now = now_rfc3339();
    assert!(matches!(
        ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution_for_claim.execution_version,
                owner: "expired-owner".to_owned(),
                lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                now,
            },
        )
        .await
        .expect("execution lease claims"),
        db::ExecutionLeaseMutation::Updated(_)
    ));

    let executor = Arc::new(RecordingCancelExecutor::default());
    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(executor.clone())
        .with_execution_stall_timeout(Duration::from_secs(1));

    let stalled = monitor.check_once().await.expect("monitor checks");
    assert_eq!(stalled, 1);
    assert!(executor
        .cancelled
        .lock()
        .expect("cancel log lock")
        .is_empty());
}

#[tokio::test]
async fn heartbeat_monitor_cancels_stalled_executions_of_daemonless_agents() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let now = now_rfc3339();
    let agent = AgentRepo::create(
        &*db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "embedded-shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 1,
            max_missed_heartbeats: 1,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("daemonless agent creates");
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id.clone(), None).await;
    ExecutionRepo::update(
        &*db,
        UpdateExecution {
            id: execution.id.clone(),
            status: None,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(Some("1970-01-01T00:00:00+00:00".to_owned())),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("execution activity updates");

    let execution_for_claim = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    let now = now_rfc3339();
    assert!(matches!(
        ExecutionRepo::claim_lease(
            &*db,
            db::ClaimExecutionLease {
                execution_id: execution.id.clone(),
                expected_version: execution_for_claim.execution_version,
                owner: "expired-owner".to_owned(),
                lease_expires_at: "1970-01-01T00:00:00+00:00".to_owned(),
                hard_deadline_at: Some((Utc::now() + ChronoDuration::hours(1)).to_rfc3339(),),
                now,
            },
        )
        .await
        .expect("execution lease claims"),
        db::ExecutionLeaseMutation::Updated(_)
    ));

    let executor = Arc::new(RecordingCancelExecutor::default());
    let monitor = HeartbeatMonitor::new(Arc::clone(&db), Arc::clone(&event_bus))
        .with_task_executor(executor.clone())
        .with_execution_stall_timeout(Duration::from_secs(1));

    let stalled = monitor.check_once().await.expect("monitor checks");
    assert_eq!(stalled, 1);
    assert_eq!(
        executor
            .cancelled
            .lock()
            .expect("cancel log lock")
            .as_slice(),
        std::slice::from_ref(&execution.id)
    );
}

#[test]
fn a_fresh_dispatch_marker_is_not_treated_as_a_dead_owner() {
    // The reviewer execution that regressed was killed 8ms after creation.
    let execution = dispatch_marker_execution(
        Some("dispatch-pending:execution-1"),
        "2026-09-03T08:33:22.584249+00:00",
        Some("2026-09-03T09:03:22.584249+00:00"),
    );
    assert!(is_unclaimed_dispatch_marker(
        &execution,
        "2026-09-03T08:33:22.592175+00:00"
    ));
}

#[test]
fn a_stale_dispatch_marker_is_reclaimed_once_the_grace_elapses() {
    let execution = dispatch_marker_execution(
        Some("dispatch-pending:execution-1"),
        "2026-09-03T08:33:22.584249+00:00",
        Some("2026-09-03T09:03:22.584249+00:00"),
    );
    assert!(!is_unclaimed_dispatch_marker(
        &execution,
        "2026-09-03T08:34:22.584249+00:00"
    ));
}

#[test]
fn a_real_owner_lease_is_never_given_the_dispatch_grace() {
    let execution = dispatch_marker_execution(
        Some("daemon-7"),
        "2026-09-03T08:33:22.584249+00:00",
        Some("2026-09-03T09:03:22.584249+00:00"),
    );
    assert!(!is_unclaimed_dispatch_marker(
        &execution,
        "2026-09-03T08:33:22.592175+00:00"
    ));
}

#[test]
fn a_dispatch_marker_past_its_hard_deadline_is_still_eligible() {
    let execution = dispatch_marker_execution(
        Some("dispatch-pending:execution-1"),
        "2026-09-03T08:33:22.584249+00:00",
        Some("2026-09-03T08:33:22.584249+00:00"),
    );
    assert!(!is_unclaimed_dispatch_marker(
        &execution,
        "2026-09-03T08:33:22.592175+00:00"
    ));
}
