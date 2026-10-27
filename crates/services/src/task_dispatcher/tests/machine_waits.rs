//! Tests for machine waits in the task dispatcher.

use super::*;
#[tokio::test]
async fn machine_capacity_waits_in_initial_state_then_starts_after_run_ends() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    db.server_run_cap.set(
        Some(1),
        config::resolved_run_cap(Some(1)),
        &config::embedded_machine_id(),
    );
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, queued.status);
    assert_eq!(waiting.version, queued.version);
    let disposition = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(
        disposition.capability, "machine_capacity",
        "{}",
        disposition.safe_message
    );
    let health = crate::task_diagnostics::derive_workflow_health(
        &waiting,
        &workflow,
        &[],
        None,
        None,
        false,
        None,
    );
    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.severity, api_types::HealthSeverity::Info);
    assert_eq!(health.stale_reason.as_deref(), Some("machine_capacity"));
    assert!(waiting.error_annotation.is_none());
    assert!(waiting.failed_json.is_none());
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE scope_type = 'task' AND scope_id = ?",
    )
    .bind(&queued.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 0);
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let started = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::current_dispatch_disposition(&admitted).is_none());
}

/// One slot, one Task, and that Task's own run holds the slot: it waits for
/// nothing. The precheck used to count the Task's execution against the Task,
/// so a machine capped at one run showed every running Task as waiting for a
/// slot, and the wait outlived the run.
#[tokio::test]
async fn machine_capacity_task_does_not_wait_for_its_own_slot() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    assign_role(&db, &running.id, "coder", &agent_id).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    db.server_run_cap.set(
        Some(1),
        config::resolved_run_cap(Some(1)),
        &config::embedded_machine_id(),
    );
    let (dispatcher, _rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .unwrap()
        .unwrap();
    let task = machine_capacity_task(&db, &running.id).await;
    assert!(dispatcher
        .task_service
        .capacity_wait_for(&task, &agent, Some("coder"))
        .await
        .unwrap()
        .is_none());
    dispatcher.check_once_and_drain().await.unwrap();
    let task = machine_capacity_task(&db, &running.id).await;
    assert!(deferred_dispatch::dispatch_disposition(&task).is_none());

    // A wait left on a Task that then got the slot is retired by the same
    // observation, not kept until some other Task is dispatched.
    deferred_dispatch::record_dispatch_disposition(
        &db,
        &task,
        "machine_capacity",
        "machine_capacity: waiting for a machine run slot",
    )
    .await
    .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    let task = machine_capacity_task(&db, &running.id).await;
    assert!(deferred_dispatch::dispatch_disposition(&task).is_none());

    // A running execution that is not the Task's run of the role asked for
    // (an interactive session) holds a slot, and a coder dispatch needs a
    // second one: the Task does wait, and the wait is stable. Answering "not
    // waiting" had every pass retire the wait, try, be refused by the
    // reserve transaction and record the wait again.
    let session = seed_task(&db, &project_id, "SESSION", "in_progress", 0).await;
    assign_role(&db, &session.id, "coder", &agent_id).await;
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    seed_running_execution(&db, &session.id, &agent_id, "interactive").await;
    let task = machine_capacity_task(&db, &session.id).await;
    assert!(dispatcher
        .task_service
        .capacity_wait_for(&task, &agent, Some("coder"))
        .await
        .unwrap()
        .is_some());
    dispatcher.check_once_and_drain().await.unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    let settled = machine_capacity_task(&db, &session.id).await;
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&settled)
            .map(|wait| wait.capability)
            .as_deref(),
        Some("machine_capacity")
    );
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
        let again = machine_capacity_task(&db, &session.id).await;
        assert_eq!(again.version, settled.version, "the wait is rewritten");
    }
    let runs: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
    )
    .bind(&session.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(runs, 1, "a second run was admitted over the cap");
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&session.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status = 'done' WHERE id = ?")
        .bind(&session.id)
        .execute(db.pool())
        .await
        .unwrap();
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;

    // Another Task on the same machine does wait for it.
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    assert!(dispatcher
        .task_service
        .capacity_wait_for(&queued, &agent, Some("coder"))
        .await
        .unwrap()
        .is_some());
}

/// The backstop under every run-slot wait. The slot is freed by something
/// that marks nobody (here: the marks are thrown away, as a release that
/// raced the wait or a future writer that forgets its trigger would leave
/// it). Nothing is dirty, nothing is asked: the wait row's own deadline has
/// the Task read again within one period, and it is dispatched.
#[tokio::test]
async fn machine_capacity_wait_is_read_again_at_its_deadline_without_any_mark() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    assign_role(&db, &running.id, "coder", &agent_id).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    db.server_run_cap.set(
        Some(1),
        config::resolved_run_cap(Some(1)),
        &config::embedded_machine_id(),
    );
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    // The new waiter is read once more with its wait row in place.
    assert_eq!(dispatcher.tick(false).await.unwrap(), 0);
    dispatcher.drain_steps().await.unwrap();
    let row: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT daemon_id, deadline FROM task_schedule_wait WHERE task_id = ?")
            .bind(&queued.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(row.0.as_deref(), Some("*"));
    let armed = row.1.expect("a run-slot wait has a recheck deadline");
    assert!(
        dispatcher.schedule_sleep() <= super::reconciliation::SLOT_WAIT_RECHECK,
        "the loop wakes for it"
    );
    // Read again before the deadline: the timer is kept, not pushed out.
    dispatcher.check_once_and_drain().await.unwrap();
    let kept: Option<String> =
        sqlx::query_scalar("SELECT deadline FROM task_schedule_wait WHERE task_id = ?")
            .bind(&queued.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(kept.as_deref(), Some(armed.as_str()));

    // The slot is freed and every mark is lost.
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status = 'done' WHERE id = ?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher.tick(false).await.unwrap(),
        0,
        "nothing marked it"
    );
    tokio::time::sleep(super::reconciliation::SLOT_WAIT_RECHECK + Duration::from_millis(300)).await;
    dispatcher.tick(false).await.unwrap();
    dispatcher.drain_steps().await.unwrap();
    let started = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("dispatched by the timer alone")
        .unwrap();
    assert_eq!(started.task_id, queued.id);
}

/// Every machine under its free-space floor: the Task waits where it is with
/// the machine-capacity wait naming the disk, raises nothing, and is
/// dispatched by the next scan after the reading recovers. Nobody acts.
#[tokio::test]
async fn disk_pressure_waits_in_initial_state_then_starts_when_the_reading_recovers() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let free = Arc::new(std::sync::atomic::AtomicU64::new(10));
    let reading = Arc::clone(&free);
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: db::now_rfc3339(),
                gc_state: Some("owned".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, queued.status);
    let disposition = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(
        (
            disposition.capability.as_str(),
            disposition.capacity_scope.as_deref()
        ),
        ("machine_capacity", Some("disk")),
        "{}",
        disposition.safe_message
    );
    assert!(
        disposition
            .safe_message
            .starts_with("disk_pressure: waiting for free space"),
        "{}",
        disposition.safe_message
    );
    // The collector runs on this root, so the wait does not say otherwise.
    assert!(!disposition.safe_message.contains("garbage collection"));
    assert!(format!("{:?}", waiting.condition).contains("Disk"));
    let health = crate::task_diagnostics::derive_workflow_health(
        &waiting,
        &workflow,
        &[],
        None,
        None,
        false,
        None,
    );
    assert_eq!(health.kind, api_types::WorkflowHealthKind::WaitingForAgent);
    assert_eq!(health.severity, api_types::HealthSeverity::Info);
    assert_eq!(health.label, "Waiting for Disk Space");
    assert_eq!(health.stale_reason.as_deref(), Some("disk_pressure"));
    assert!(waiting.error_annotation.is_none() && waiting.failed_json.is_none());
    let attention: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE scope_type = 'task' AND scope_id = ?",
    )
    .bind(&queued.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(attention, 0);
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert!(project.system_pause_reason.is_none());
    // Still short on the next scan: still waiting, nothing rewritten.
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap()
            .version,
        waiting.version
    );

    // The reading recovers; the next scan dispatches the Task.
    free.store(900, std::sync::atomic::Ordering::SeqCst);
    db.disk_admission.refresh();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let started = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::current_dispatch_disposition(&admitted).is_none());
    assert!(!format!("{:?}", admitted.condition).contains("Disk"));
}

/// The disk wait is level-triggered by a timer, not by a Task event: free
/// disk comes back without any commit. The supervised loop's own tick
/// (`tick(false)`, which only looks at what is dirty, due or kicked) reads a
/// disk waiter again at its durable deadline, keeps the wait while the disk
/// is short (a freed run slot does not clear it), and dispatches the Task
/// after the reading recovers, across a server restart, with nothing else
/// happening.
#[tokio::test]
async fn disk_wait_is_reread_by_the_loop_tick_on_a_timer_and_survives_a_restart() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let free = Arc::new(std::sync::atomic::AtomicU64::new(10));
    let reading = Arc::clone(&free);
    db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: db::now_rfc3339(),
                gc_state: Some("owned".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let deadline = || async {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT deadline FROM task_schedule_wait WHERE task_id = ?",
        )
        .bind(&queued.id)
        .fetch_optional(db.pool())
        .await
        .unwrap()
        .flatten()
    };
    let disk_wait = || async {
        let task = TaskRepo::get_by_id(&*db, &queued.id, false)
            .await
            .unwrap()
            .unwrap();
        deferred_dispatch::current_dispatch_disposition(&task)
            .and_then(|wait| wait.capacity_scope)
            .as_deref()
            == Some("disk")
    };
    let recheck = super::reconciliation::DISK_WAIT_RECHECK + Duration::from_millis(300);

    let (dispatcher, _rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert!(disk_wait().await);
    // Recording the wait dirtied the Task; the loop's next tick settles it.
    assert_eq!(dispatcher.tick(false).await.unwrap(), 0);
    dispatcher.drain_steps().await.unwrap();
    assert!(disk_wait().await);
    let rows: Vec<(Option<String>, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT agent_id, daemon_id, deadline FROM task_schedule_wait")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let first = deadline()
        .await
        .unwrap_or_else(|| panic!("a disk wait has a timer: {rows:?}"));
    assert!(
        dispatcher.schedule_sleep() <= super::reconciliation::DISK_WAIT_RECHECK,
        "the loop wakes for it"
    );

    // Still short at the deadline. A run slot is freed at the same time
    // (the cap changes, which kicks every machine waiter): the Task still
    // waits for disk, and the timer is armed again.
    tokio::time::sleep(recheck).await;
    db.server_run_cap
        .set(Some(8), 8, &config::embedded_machine_id());
    assert_eq!(dispatcher.tick(false).await.unwrap(), 0);
    dispatcher.drain_steps().await.unwrap();
    assert!(disk_wait().await, "a freed slot does not clear a disk wait");
    let second = deadline().await.expect("the timer is armed again");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&second).unwrap()
            > chrono::DateTime::parse_from_rfc3339(&first).unwrap()
    );

    // The reading recovers. No Task changes, nothing is asked. The server
    // restarts: a new dispatcher, whose loop ticks on its own.
    free.store(900, std::sync::atomic::Ordering::SeqCst);
    db.disk_admission.refresh();
    drop(dispatcher);
    let (restarted, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    tokio::time::sleep(recheck).await;
    restarted.startup_reconcile().await.unwrap();
    restarted.drain_steps().await.unwrap();
    restarted.tick(false).await.unwrap();
    restarted.drain_steps().await.unwrap();
    let started = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("dispatched by the timer alone")
        .unwrap();
    assert_eq!(started.task_id, queued.id);
    assert!(!disk_wait().await);
}
