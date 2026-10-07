//! The reconciling dispatcher's own contract: failures stay with their Task,
//! a wait writes nothing while it waits, the sweep finds what kicks missed and
//! replays nothing else, new owner parks are visible, and the stop fence holds.
use super::*;

async fn fixture() -> (Arc<db::SqliteDb>, String, String, TempDir, TempDir) {
    let db = Arc::new(sqlite_db().await);
    let (repo, workspace) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    (db, project, agent, repo, workspace)
}

async fn count(db: &db::SqliteDb, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(db.pool()).await.unwrap()
}

async fn executions(db: &db::SqliteDb, task: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn role_commands(db: &db::SqliteDb, task: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='command' AND json_extract(payload_json,'$.operation')='reconcile_role'")
        .bind(task)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn reload(db: &db::SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

/// Run one complete lap of the sweep now.
async fn sweep_now(dispatcher: &TaskDispatcher) {
    dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
    dispatcher.reconcile_all().await.unwrap();
    dispatcher.drain_steps().await.unwrap();
}

/// One Task whose stored condition cannot be decoded neither fails the pass
/// nor leaves it marked idle: every other Project still dispatches, the row is
/// recomputed from its legacy fields and the repair is reported.
#[tokio::test]
async fn one_undecodable_condition_does_not_stall_any_project() {
    let db = Arc::new(sqlite_db().await);
    let (ra, rb, ws) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let (pa, _) = seed_project_repo(&db, ra.path()).await;
    let (pb, _) = seed_project_repo(&db, rb.path()).await;
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let good = seed_task(&db, &pa, "good", "todo", 0).await;
    assign_role(&db, &good.id, "coder", &agent).await;
    let bad = seed_task(&db, &pb, "bad", "todo", 0).await;
    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"from_a_newer_build\"}' WHERE id=?")
        .bind(&bad.id)
        .execute(db.pool())
        .await
        .unwrap();
    let (dispatcher, _rx) = build_dispatcher(db.clone(), ws.path()).await;
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_ne!(
        reload(&db, &good.id).await.status,
        "todo",
        "a healthy Project's Task must still dispatch"
    );
    assert_eq!(executions(&db, &good.id).await, 1);
    // The unreadable row was recomputed, and the lap that saw it reports it
    // in the invariant report.
    assert!(db.task_condition(&bad.id).await.is_ok());
    assert!(db
        .condition_check_status()
        .last_pass
        .is_some_and(|pass| pass.repaired >= 1));
    assert!(db.task_schedule_violations().await.unwrap().is_empty());
}

/// A Task refused for the Project limit or a machine run slot is parked once.
/// While nothing frees capacity, thirty more ticks add no step, no event and
/// no Task write.
#[tokio::test]
async fn a_capacity_wait_writes_nothing_while_it_waits() {
    for cap in ["project", "machine"] {
        let (db, project, agent, _repo, ws) = fixture().await;
        let running = seed_task(&db, &project, "running", "in_progress", 0).await;
        assign_role(&db, &running.id, "coder", &agent).await;
        seed_running_execution(&db, &running.id, &agent, "coder").await;
        let waiting = seed_task(&db, &project, "waiting", "todo", 0).await;
        assign_role(&db, &waiting.id, "coder", &agent).await;
        let active = seed_task(&db, &project, "active", "in_progress", 0).await;
        assign_role(&db, &active.id, "coder", &agent).await;
        if cap == "project" {
            set_project_active_task_limit(&db, &project, 1).await;
        } else {
            db.server_run_cap.set(
                Some(1),
                config::resolved_run_cap(Some(1)),
                &config::embedded_machine_id(),
            );
        }
        let (dispatcher, _rx) = build_dispatcher_runtime(db.clone(), ws.path(), true).await;
        for _ in 0..6 {
            dispatcher.check_once_and_drain().await.unwrap();
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let waiting_now = reload(&db, &waiting.id).await;
        assert_eq!(waiting_now.status, "todo", "{cap}");
        assert_eq!(
            deferred_dispatch::current_dispatch_disposition(&waiting_now).map(|d| d.capability),
            Some(format!("{cap}_capacity")),
            "the wait is visible exactly as before"
        );
        let observe = || async {
            (
                count(&db, "SELECT COUNT(*) FROM task_step").await,
                count(&db, "SELECT COUNT(*) FROM domain_event").await,
                count(&db, "SELECT COALESCE(SUM(version),0) FROM task").await,
                count(&db, "SELECT COUNT(*) FROM execution").await,
            )
        };
        let before = observe().await;
        let metadata = reload(&db, &waiting.id).await.metadata_json;
        for _ in 0..30 {
            dispatcher.check_once_and_drain().await.unwrap();
            tokio::time::sleep(Duration::from_millis(12)).await;
        }
        assert_eq!(observe().await, before, "{cap}: idle ticks wrote nothing");
        assert_eq!(reload(&db, &waiting.id).await.metadata_json, metadata);
    }
}

/// Freed capacity kicks the waiters it can admit: the Task that waited on the
/// Project limit is admitted on the pass after the slot frees.
#[tokio::test]
async fn a_freed_project_slot_admits_the_waiter_at_once() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let running = seed_task(&db, &project, "running", "in_progress", 0).await;
    assign_role(&db, &running.id, "coder", &agent).await;
    seed_running_execution(&db, &running.id, &agent, "coder").await;
    let waiting = seed_task(&db, &project, "waiting", "todo", 0).await;
    assign_role(&db, &waiting.id, "coder", &agent).await;
    set_project_active_task_limit(&db, &project, 1).await;
    let (dispatcher, _rx) = build_dispatcher(db.clone(), ws.path()).await;
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_eq!(reload(&db, &waiting.id).await.status, "todo");
    // The running Task leaves its active slot.
    sqlx::query("UPDATE execution SET status='cancelled' WHERE task_id=?")
        .bind(&running.id)
        .execute(db.pool())
        .await
        .unwrap();
    let current = reload(&db, &running.id).await;
    TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: current.id.clone(),
            expected_version: current.version,
            status: "done".into(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(reload(&db, &waiting.id).await.status, "in_progress");
}

/// Role commands of different Tasks are independent. Four busy long-lane
/// steps and a queued role command in one Project do not delay a Task of
/// another Project by a single pass.
#[tokio::test]
async fn busy_long_steps_never_stall_another_projects_dispatch() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let (ra, rb, ws) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let (pa, _) = seed_project_repo(&db, ra.path()).await;
    let (pb, _) = seed_project_repo(&db, rb.path()).await;
    let agent_a = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let agent_b =
        seed_agent_with_executor(&db, 3, DaemonStatus::Online, AgentStatus::Idle, "shell").await;
    // Four long-lane steps claimed and still running, as CI or merge steps are.
    for i in 0..4 {
        let busy = seed_task(&db, &pa, &format!("busy {i}"), "merging", 0).await;
        let id = new_uuid_v4();
        db.enqueue_step(&db::EnqueueTaskStep {
            id: id.clone(),
            task_id: busy.id.clone(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: busy.status.clone(),
            expected_version: busy.version,
            expected_epoch: None,
            lane: "long".into(),
            available_at: "2026-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
        db.claim_step(
            "busy-worker",
            Some(&busy.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    }
    // A fifth Task of the same Project has its role command queued behind them.
    let queued = seed_task(&db, &pa, "queued", "in_progress", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_a).await;
    let other = seed_task(&db, &pb, "other", "in_progress", 0).await;
    assign_role(&db, &other.id, "coder", &agent_b).await;
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    dispatcher.check_once().await.unwrap();
    assert_eq!(role_commands(&db, &queued.id).await, 1);
    // Only the other Project's Task is driven: Project A's command stays queued.
    dispatcher.task_service.drain(&other.id).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        other.id,
        "another Project dispatches at once"
    );
    assert_eq!(executions(&db, &queued.id).await, 0);
}

/// A recorded refusal holds until the Task changes or is woken, as before.
/// The sweep leaves it alone: no command, no step, no event and no write, lap
/// after lap. A change to the Task re-opens it on the next pass.
#[tokio::test]
async fn the_sweep_replays_no_cached_refusal_until_a_fact_changes() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let task = seed_task(&db, &project, "refused", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    deferred_dispatch::record_dispatch_disposition(&db, &task, "in_progress", "setup refused")
        .await
        .unwrap();
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let parked = reload(&db, &task.id).await;
    assert!(deferred_dispatch::current_dispatch_disposition(&parked).is_some());
    let observe = || async {
        let t = reload(&db, &task.id).await;
        (
            count(&db, "SELECT COUNT(*) FROM task_step").await,
            count(&db, "SELECT COUNT(*) FROM domain_event").await,
            t.version,
            t.updated_at,
            t.metadata_json,
            t.error_annotation,
        )
    };
    let before = observe().await;
    for _ in 0..3 {
        sweep_now(&dispatcher).await;
        for _ in 0..3 {
            dispatcher.check_once_and_drain().await.unwrap();
        }
    }
    assert_eq!(observe().await, before, "the sweep replayed nothing");
    assert_eq!(executions(&db, &task.id).await, 0);
    assert_eq!(role_commands(&db, &task.id).await, 0);
    // A restart holds it too: the refusal is stored on the Task.
    let (restarted, _rx) = build_dispatcher(db.clone(), ws.path()).await;
    restarted.startup_reconcile().await.unwrap();
    sweep_now(&restarted).await;
    assert_eq!(observe().await, before, "startup replayed nothing");
    // The Task changes: its version no longer matches the refusal.
    let current = reload(&db, &task.id).await;
    TaskRepo::update(
        &*db,
        UpdateTask {
            id: current.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: Some(5),
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
}

/// A queued role command re-resolves the Task under its lease. A refusal
/// recorded after it was queued holds the Task, as it held it before.
#[tokio::test]
async fn a_queued_role_command_honours_a_refusal_recorded_after_it_was_queued() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let task = seed_task(&db, &project, "late refusal", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher_runtime(db.clone(), ws.path(), false).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    assert_eq!(role_commands(&db, &task.id).await, 1);
    // The refusal lands while the command waits in the queue.
    let current = reload(&db, &task.id).await;
    sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.dispatch_disposition',json(?)) WHERE id=?")
        .bind(
            serde_json::json!({
                "task_version": current.version,
                "capability": "in_progress",
                "blocker_digest": "sha256:late",
                "recorded_at": now_rfc3339(),
                "safe_message": "late refusal",
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.drain_steps().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(executions(&db, &task.id).await, 0);
}

/// The stop fence belongs to the one dispatcher instance. A role command it
/// queued finds that instance, not a fresh one, and dispatches nothing once
/// it stopped.
#[tokio::test]
async fn a_queued_role_command_observes_the_dispatchers_stop_fence() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let task = seed_task(&db, &project, "fenced", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher_runtime(db.clone(), ws.path(), false).await;
    assert_eq!(dispatcher.check_once().await.unwrap(), 1);
    assert_eq!(role_commands(&db, &task.id).await, 1);
    dispatcher.stop();
    dispatcher.drain_steps().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(executions(&db, &task.id).await, 0, "the fence held");
}

/// Kicks racing the sweep, and a kick racing a leased role command, never
/// produce a second command or a second execution.
#[tokio::test]
async fn kicks_racing_the_sweep_or_a_leased_step_dispatch_once() {
    use db::TaskStepRepo;
    let (db, project, agent, _repo, ws) = fixture().await;
    let (dispatcher, mut rx) = build_dispatcher_runtime(db.clone(), ws.path(), false).await;
    dispatcher.check_once().await.unwrap();
    // Two kicks while a sweep lap and a pass are running.
    let raced = seed_task(&db, &project, "raced", "in_progress", 0).await;
    assign_role(&db, &raced.id, "coder", &agent).await;
    dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
    let kicks = async {
        for _ in 0..2 {
            db.kick_schedule(&raced.id).await.unwrap();
            tokio::task::yield_now().await;
        }
    };
    let (swept, passed, ()) =
        tokio::join!(dispatcher.reconcile_all(), dispatcher.check_once(), kicks);
    swept.unwrap();
    passed.unwrap();
    for _ in 0..3 {
        dispatcher.check_once().await.unwrap();
    }
    assert_eq!(role_commands(&db, &raced.id).await, 1, "one role command");
    // A kick while that command is leased by a worker.
    let leased = db
        .claim_step(
            "worker",
            Some(&raced.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .expect("the role command is claimable");
    for _ in 0..3 {
        db.kick_schedule(&raced.id).await.unwrap();
        dispatcher.check_once().await.unwrap();
    }
    sweep_now_without_drain(&dispatcher).await;
    assert_eq!(
        role_commands(&db, &raced.id).await,
        1,
        "the lease owns the Task"
    );
    // The worker gives the step back; it runs once.
    db.retry_step(&leased, "worker restarted", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    dispatcher.drain_steps().await.unwrap();
    for _ in 0..3 {
        db.kick_schedule(&raced.id).await.unwrap();
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        raced.id
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(executions(&db, &raced.id).await, 1, "one execution");
    assert_eq!(role_commands(&db, &raced.id).await, 1);
}

async fn sweep_now_without_drain(dispatcher: &TaskDispatcher) {
    dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
    dispatcher.reconcile_all().await.unwrap();
}

/// `sweep_and_assert` reads the durable state before anything repairs it, so
/// a Task a kick missed fails the assertion. The sweep then repairs it, at
/// startup and in steady state, and the assertion holds.
#[tokio::test]
async fn a_missed_kick_fails_the_assertion_and_is_repaired_by_the_sweep() {
    for startup in [false, true] {
        let (db, project, agent, _repo, ws) = fixture().await;
        let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
        if !startup {
            dispatcher.check_once_and_drain().await.unwrap();
            sweep_now(&dispatcher).await;
        }
        let task = seed_task(&db, &project, "missed", "in_progress", 0).await;
        assign_role(&db, &task.id, "coder", &agent).await;
        // The commit happened; its kick never reached the dispatcher.
        sqlx::query("DELETE FROM task_schedule_dirty")
            .execute(db.pool())
            .await
            .unwrap();
        let unowned = dispatcher.sweep_and_assert().await.unwrap_err();
        assert!(unowned.to_string().contains(&task.id), "{unowned}");
        assert_eq!(executions(&db, &task.id).await, 0);
        if startup {
            // Startup does not wait for the sweep: the first ticks run it in
            // slices behind dispatch.
            let (restarted, mut restarted_rx) = build_dispatcher(db.clone(), ws.path()).await;
            assert_eq!(restarted.startup_reconcile().await.unwrap(), 0);
            for _ in 0..5 {
                restarted.check_once_and_drain().await.unwrap();
            }
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), restarted_rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .task_id,
                task.id
            );
            restarted.sweep_and_assert().await.unwrap();
        } else {
            sweep_now(&dispatcher).await;
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .task_id,
                task.id
            );
            dispatcher.sweep_and_assert().await.unwrap();
        }
        assert_eq!(executions(&db, &task.id).await, 1);
    }
}

/// The operator refresh reconciles every Task now, as it scanned every Task
/// before: it does not wait for the next lap of the sweep.
#[tokio::test]
async fn a_forced_reconcile_finds_work_no_kick_announced() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    dispatcher.reconcile_all().await.unwrap();
    let task = seed_task(&db, &project, "unannounced", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(dispatcher.check_once().await.unwrap(), 0, "no lap is due");
    assert_eq!(dispatcher.reconcile_all().await.unwrap(), 1);
    dispatcher.drain_steps().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
}

/// Agent availability also turns on facts no commit announces: credentials,
/// provider health and backoff, connection health and CLI policy all feed the
/// same availability read. A Task held for an unavailable Agent is re-read at
/// the scan interval, so it recovers no later than the scanning dispatcher
/// recovered it, with no kick at all.
#[tokio::test]
async fn an_unavailable_agent_hold_is_rechecked_at_the_scan_interval() {
    let (db, project, agent, _repo, ws) = fixture().await;
    // CLI policy has no scheduler trigger. This row disables the Agent's
    // executor on its machine.
    let daemon = AgentRepo::get_by_id(&*db, &agent)
        .await
        .unwrap()
        .unwrap()
        .daemon_id
        .expect("the fixture Agent is pinned to a machine");
    let owner = new_uuid_v4();
    sqlx::query(
        "INSERT INTO user(id,email,password_hash,created_at,updated_at) VALUES (?,?,'test',?,?)",
    )
    .bind(&owner)
    .bind(format!("{owner}@example.test"))
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO cli_runtime_policy(owner_user_id,daemon_id,executor_type,enabled,created_at,updated_at) VALUES (?,?,'shell',0,?,?)")
        .bind(&owner)
        .bind(&daemon)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task(&db, &project, "held", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_eq!(executions(&db, &task.id).await, 0, "held while unavailable");
    // Not a lap of the sweep: the next one is two minutes away.
    dispatcher.schedule_state.lock().unwrap().sweep.due =
        std::time::Instant::now() + Duration::from_secs(120);
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE cli_runtime_policy SET enabled=1 WHERE daemon_id=?")
        .bind(&daemon)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM task_schedule_dirty WHERE dirty=1"
        )
        .await,
        0,
        "no commit kicked the Task"
    );
    // The supervised loop's own tick, once the scan interval (10 ms in this
    // fixture) has passed.
    tokio::time::sleep(Duration::from_millis(30)).await;
    dispatcher.tick(false).await.unwrap();
    dispatcher.drain_steps().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
}

/// A Task nothing will ever continue is parked for its owner, and the park is
/// visible today: an annotation of an existing kind whose message names the
/// owner and the action, shown by the exception and intervention readers. It
/// changes neither the Task's slot nor its dispatch, and the dispatcher
/// removes it on the pass that sees the Task repaired.
#[tokio::test]
async fn an_owner_park_is_visible_in_the_condition_and_clears_itself() {
    let (db, project, agent, _repo, ws) = fixture().await;
    set_project_active_task_limit(&db, &project, 5).await;
    // A state the Project workflow does not define.
    let stranded = seed_task(&db, &project, "stranded", "retired_state", 0).await;
    assign_role(&db, &stranded.id, "coder", &agent).await;
    // An active state nobody was given: human work, exactly as before.
    let unassigned = seed_task(&db, &project, "nobody", "in_progress", 0).await;
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    let slots_before = slots::load_project_slots(
        &db,
        &ProjectRepo::get_by_id(&*db, &project)
            .await
            .unwrap()
            .unwrap(),
    )
    .await
    .unwrap();
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let parked = reload(&db, &stranded.id).await;
    assert!(
        parked.error_annotation.is_none(),
        "the stage-three bridge is removed"
    );
    let annotation = parked
        .condition
        .read()
        .diagnostic
        .expect("visible condition park");
    assert_eq!(
        annotation.annotation_type,
        api_types::FailureKind::WorkflowGuardRejected
    );
    let message = annotation.message.unwrap();
    assert!(
        message.contains("retired_state")
            && message.contains("Owner: the Project Agent")
            && message.contains("Action: edit the Project workflow"),
        "{message}"
    );
    assert!(parked.blocked_json.is_none() && parked.failed_json.is_none());
    assert_eq!(
        parked.version, stranded.version,
        "condition-only diagnostics use the list revision"
    );
    // Public readers show it today.
    let workflow = WorkflowEngine::resolve_workflow("{}");
    let exception =
        crate::task_diagnostics::task_exception_projection(&parked, &workflow, &[], None, vec![])
            .expect("the exception reader shows the park");
    assert_eq!(exception.message, message);
    assert!(db::material_blocker(&parked.condition).requires_intervention);
    let public = parked.condition.public();
    let public = serde_json::to_value(public).unwrap();
    assert_eq!(public["details"]["owner"], "project_agent");
    assert_eq!(public["details"]["recovery"], "edit_workflow");
    // It neither blocks dispatch nor moves the Task to a parked slot.
    assert!(!helpers::has_blocking_annotation(&parked));
    assert_eq!(
        slots::load_project_slots(
            &db,
            &ProjectRepo::get_by_id(&*db, &project)
                .await
                .unwrap()
                .unwrap()
        )
        .await
        .unwrap(),
        slots_before
    );
    // The unassigned Task is untouched in every legacy field.
    let nobody = reload(&db, &unassigned.id).await;
    assert_eq!(
        (
            nobody.version,
            &nobody.error_annotation,
            &nobody.blocked_json,
            &nobody.metadata_json
        ),
        (unassigned.version, &None, &None, &None)
    );
    let park: String =
        sqlx::query_scalar("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
            .bind(&unassigned.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(
        park.contains("HumanWork") && park.contains("AssignRole"),
        "{park}"
    );
    // Nothing more is written while it stays parked.
    for _ in 0..5 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert_eq!(reload(&db, &stranded.id).await.version, parked.version);
    // The owner moves the Task to a state the workflow defines: the park
    // goes and the Task is dispatched on the same pass.
    sqlx::query("UPDATE task SET status='in_progress', status_epoch=status_epoch+1, version=version+1 WHERE id=?")
        .bind(&stranded.id)
        .execute(db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &stranded.id)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        stranded.id
    );
    assert!(reload(&db, &stranded.id).await.error_annotation.is_none());
}

/// Wall time of one lap of the sweep, and of the startup pass, for Tasks that
/// are mostly settled. Release build:
/// `cargo test -p services --release --lib sweep_wall_time -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "measurement"]
async fn sweep_wall_time() {
    let open: i64 = std::env::var("SWEEP_OPEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let settled: i64 = std::env::var("SWEEP_SETTLED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(45000);
    let path = std::env::temp_dir().join(format!("forge-sweep-{}.db", std::process::id()));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    let pool = create_sqlite_pool(&format!("sqlite:{}?mode=rwc", path.display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let (repo, ws) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let seed = seed_task(&db, &project, "parked", "in_progress", 0).await;
    let finished = seed_task(&db, &project, "finished", "done", 0).await;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query(
        "UPDATE task SET blocked_json='{\"kind\":\"manual_stop\",\"reason\":\"held\"}' WHERE id=?",
    )
    .bind(&seed.id)
    .execute(&mut *tx)
    .await
    .unwrap();
    db.sync_condition_in_tx(&mut tx, &seed.id).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &finished.id)
        .await
        .unwrap();
    for (prefix, n, source) in [
        ("held-", open - 1, &seed.id),
        ("done-", settled - 1, &finished.id),
    ] {
        if n > 0 {
            sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO task(id,project_id,title,task_type,status,priority,created_at,updated_at,blocked_json,condition_json) SELECT ?||i,project_id,title,task_type,status,priority,created_at,updated_at,blocked_json,json_set(condition_json,'$.evidence.witnesses[0].task_id',?||i) FROM task JOIN n WHERE id=?")
                .bind(n).bind(prefix).bind(prefix).bind(source).execute(&mut *tx).await.unwrap();
        }
    }
    // A mid-life history: 20 transitions per Task.
    sqlx::query("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20) INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,status_epoch) SELECT t.id||'-l'||i,t.id,'todo','in_progress','system','h','2026-01-01T00:00:00Z',-i FROM task t JOIN n")
        .execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("DELETE FROM task_schedule_dirty")
        .execute(db.pool())
        .await
        .unwrap();
    let (dispatcher, _rx) = build_dispatcher_runtime(db.clone(), ws.path(), false).await;
    let started = std::time::Instant::now();
    dispatcher.startup_reconcile().await.unwrap();
    println!(
        "SWEEP open={open} settled={settled} startup_reconcile wall={:?} (dispatch starts here)",
        started.elapsed()
    );
    for lap in ["startup lap (installs parks)", "steady lap", "steady lap"] {
        dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
        let started = std::time::Instant::now();
        let (mut slices, mut longest, mut writer) = (0, Duration::ZERO, Duration::ZERO);
        loop {
            let slice = std::time::Instant::now();
            // A writer that commits while the sweep reads.
            let write = async {
                let at = std::time::Instant::now();
                let mut c = db.pool().acquire().await.unwrap();
                sqlx::query("BEGIN IMMEDIATE")
                    .execute(&mut *c)
                    .await
                    .unwrap();
                sqlx::query("UPDATE project SET name=name WHERE id=?")
                    .bind(&project)
                    .execute(&mut *c)
                    .await
                    .unwrap();
                sqlx::query("COMMIT").execute(&mut *c).await.unwrap();
                at.elapsed()
            };
            let (tick, wrote) = tokio::join!(dispatcher.check_once(), write);
            tick.unwrap();
            writer = writer.max(wrote);
            slices += 1;
            longest = longest.max(slice.elapsed());
            let state = dispatcher.schedule_state.lock().unwrap();
            if !state.sweep.active() && !state.sweep.has_pending() {
                break;
            }
        }
        println!(
            "SWEEP open={open} settled={settled} {lap}: wall={:?} ticks={slices} longest_tick={longest:?} slowest_concurrent_writer={writer:?}",
            started.elapsed()
        );
    }
    println!(
        "SWEEP parks={} last_pass={:?}",
        count(&db, "SELECT COUNT(*) FROM task_schedule_park").await,
        db.condition_check_status().last_pass
    );
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// A subtask whose coordination root is blocked, failed or annotated is held
/// by an internal park only: no step, no refusal, no Task write.
#[tokio::test]
async fn a_subtask_of_a_held_coordination_root_is_parked_without_a_refusal() {
    for variant in ["clean", "blocked", "failed", "annotation", "stale"] {
        let (db, project, agent, _repo, workspace) = fixture().await;
        let root = seed_task(&db, &project, "root", "todo", 1).await;
        let child = seed_subtask(&db, &root, "child", "todo", 0).await;
        assign_role(&db, &root.id, crate::workflow::default_roles::CODER, &agent).await;
        let root = reload(&db, &root.id).await;
        let interruption = serde_json::json!({"kind":"workspace_error","reason":"root is held","created_at":now_rfc3339()}).to_string();
        let update = |annotation, blocked, failed| db::UpdateTaskStatus {
            id: root.id.clone(),
            expected_version: root.version,
            status: root.status.clone(),
            assignee_id: None,
            error_annotation: annotation,
            blocked_json: blocked,
            failed_json: failed,
            updated_at: now_rfc3339(),
        };
        // The real writer: the root's condition is produced with its fields.
        let held = match variant {
            "blocked" => Some(update(None, Some(Some(interruption)), None)),
            "failed" => Some(update(None, None, Some(Some(interruption)))),
            "annotation" => Some(update(
                Some(Some(
                    r#"{"type":"workspace_error","blocking_reason":"root annotated"}"#.to_owned(),
                )),
                None,
                None,
            )),
            _ => None,
        };
        if let Some(held) = held {
            TaskRepo::update_status(&*db, held).await.unwrap();
        }
        if variant == "stale" {
            // A writer that missed its condition sync: the fields hold, the
            // stored copy does not say so yet.
            sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
                .bind(r#"{"kind":"workspace_error","reason":"root is held"}"#)
                .bind(&root.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        assert_eq!(
            crate::task_hierarchy::root_blocked(&reload(&db, &root.id).await),
            !matches!(variant, "clean" | "stale"),
            "{variant}"
        );
        let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
        for _ in 0..3 {
            dispatcher.check_once_and_drain().await.unwrap();
        }
        let after = reload(&db, &child.id).await;
        let steps: Vec<(String, String)> =
            sqlx::query_as("SELECT kind, status FROM task_step WHERE task_id=? ORDER BY rowid")
                .bind(&child.id)
                .fetch_all(db.pool())
                .await
                .unwrap();
        let executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
            .bind(&child.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        if variant == "clean" {
            assert_eq!((after.status.as_str(), executions), ("in_progress", 1));
            continue;
        }
        assert_eq!(
            (
                after.status.as_str(),
                after.version,
                &after.metadata_json,
                &after.error_annotation,
                &after.blocked_json,
                executions,
                &steps
            ),
            ("todo", child.version, &None, &None, &None, 0, &Vec::new()),
            "{variant}"
        );
        let park: String =
            sqlx::query_scalar("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
                .bind(&child.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(
            park.contains("Children") && park.contains("SettleChildren"),
            "{variant}: {park}"
        );
    }
}

/// A park migrated from its visible annotation carries that diagnostic. The
/// dispatcher sees the same park and leaves it alone, so the message and its
/// `blocked_at` stay as they were shown; a different diagnosis replaces it.
#[tokio::test]
async fn a_migrated_owner_park_keeps_its_saved_diagnostic_across_passes() {
    let (db, project, agent, _repo, ws) = fixture().await;
    let stranded = seed_task(&db, &project, "stranded", "retired_state", 0).await;
    assign_role(&db, &stranded.id, "coder", &agent).await;
    let (dispatcher, _rx) = build_dispatcher(db.clone(), ws.path()).await;
    dispatcher.check_once_and_drain().await.unwrap();
    let park = |db: Arc<db::SqliteDb>, id: String| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT reason_json FROM task_schedule_park WHERE task_id=?",
        )
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
    };
    let own = park(db.clone(), stranded.id.clone()).await;
    // What the reader migration leaves: the dispatcher's park plus the
    // diagnostic the old annotation showed.
    let saved = serde_json::json!({"type":"workflow_guard_rejected","blocking_reason":"workflow_invalid","blocked_by":"system:task_dispatcher","blocked_at":"2026-10-06T01:00:00Z","blocked_execution_id":null,"artifact":null,"message":"as it was shown"});
    sqlx::query("UPDATE task_schedule_park SET reason_json=json_set(reason_json,'$.diagnostic',json(?)) WHERE task_id=?")
        .bind(saved.to_string())
        .bind(&stranded.id)
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(std::slice::from_ref(&stranded.id))
        .await
        .unwrap();
    let migrated = park(db.clone(), stranded.id.clone()).await;
    assert_ne!(migrated, own);
    assert!(super::reconciliation::same_park(Some(&migrated), &own));
    assert!(!super::reconciliation::same_park(
        Some(&migrated),
        &own.replace("retired_state", "another_state")
    ));
    assert!(!super::reconciliation::same_park(None, &own));
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    dispatcher.schedule_state.lock().unwrap().sweep.due = std::time::Instant::now();
    dispatcher.reconcile_all().await.unwrap();
    assert_eq!(park(db.clone(), stranded.id.clone()).await, migrated);
    let diagnostic = reload(&db, &stranded.id)
        .await
        .condition
        .read()
        .diagnostic
        .unwrap();
    assert_eq!(diagnostic.message.as_deref(), Some("as it was shown"));
    assert_eq!(
        diagnostic.blocked_at.as_deref(),
        Some("2026-10-06T01:00:00Z")
    );
}
