//! Tests for machine admission in the task dispatcher.

use super::*;
#[tokio::test]
async fn machine_capacity_six_todo_waiters_take_one_freed_slot_per_tick() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 16, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let mut waiting = Vec::new();
    for i in 0..6 {
        let task = seed_task(&db, &project_id, &format!("wait {i}"), "todo", 0).await;
        assign_role(&db, &task.id, "coder", &agent).await;
        waiting.push(task);
    }
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    for _ in 0..3 {
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    }
    for task in &waiting {
        let current = machine_capacity_task(&db, &task.id).await;
        assert_eq!(current.status, "todo");
        assert_eq!(current.version, task.version);
    }
    let mut active = busy.id;
    for admitted_count in 1..=6 {
        sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
            .bind(&active)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task SET status = 'done' WHERE id = ?")
            .bind(&active)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
        active = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id;
        let mut still_todo = 0;
        for task in &waiting {
            let current = machine_capacity_task(&db, &task.id).await;
            if current.status == "todo" {
                still_todo += 1;
                assert_eq!(current.version, task.version);
            }
        }
        assert_eq!(still_todo, 6 - admitted_count);
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    }
}

#[tokio::test]
async fn machine_capacity_start_refusal_parks_ready_workspace_then_retries() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task(&db, &project_id, "start loses slot", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    sqlx::query(&format!("CREATE TRIGGER fill_at_ready AFTER UPDATE OF state ON workspace_placement WHEN NEW.task_id = '{}' AND NEW.state = 'ready' BEGIN UPDATE execution SET status = 'running' WHERE task_id = '{}'; END", task.id, busy.id)).execute(db.pool()).await.unwrap();
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    dispatcher.check_once_and_drain().await.unwrap();
    let waiting = machine_capacity_task(&db, &task.id).await;
    assert_eq!(waiting.status, "in_progress");
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .capability,
        "machine_capacity"
    );
    assert!(waiting.error_annotation.is_none());
    assert!(waiting.blocked_json.is_none());
    assert!(waiting.failed_json.is_none());
    let executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id = ?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(executions, 0);
    let attention: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE scope_id = ?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(attention, 0);
    let placement = db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(placement.state, db::PlacementState::Ready);
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .parked,
        1
    );
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
        assert_eq!(
            machine_capacity_task(&db, &task.id).await.version,
            waiting.version
        );
    }
    sqlx::query("DROP TRIGGER fill_at_ready")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    let after = db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.workspace_id, placement.workspace_id);
    assert!(deferred_dispatch::current_dispatch_disposition(
        &machine_capacity_task(&db, &task.id).await
    )
    .is_none());
}

#[tokio::test]
async fn machine_capacity_title_edit_keeps_waiter_parked_until_recheck() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    let task = seed_task(&db, &project_id, "wait", "todo", 0).await;
    assign_role(&db, &task.id, "coder", &agent).await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let (dispatcher, _) = build_dispatcher(db.clone(), workspaces.path()).await;
    dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "in_progress".into(),
            crate::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                reason: None,
                triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    dispatcher.drain_steps().await.unwrap();
    let before = machine_capacity_task(&db, &task.id).await;
    let edited = TaskRepo::update(
        &*db,
        UpdateTask {
            id: before.id.clone(),
            expected_version: before.version,
            title: Some("renamed".into()),
            description: None,
            priority: None,
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
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .parked,
        1
    );
    assert!(deferred_dispatch::current_dispatch_disposition(&edited).is_some());
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        machine_capacity_task(&db, &task.id).await.version,
        edited.version
    );
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .parked,
        1
    );
}

#[tokio::test]
async fn machine_capacity_unparking_respects_project_limit() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    sqlx::query(
        "UPDATE project SET settings = json_set(settings, '$.max_active_tasks', 1) WHERE id = ?",
    )
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    let agent = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let t1 = seed_task(&db, &project_id, "parked", "in_progress", 0).await;
    assign_role(&db, &t1.id, "coder", &agent).await;
    deferred_dispatch::record_dispatch_disposition(
        &db,
        &t1,
        "machine_capacity",
        "machine_capacity: waiting for a machine run slot",
    )
    .await
    .unwrap();
    let t2 = seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    seed_running_execution(&db, &t2.id, &agent, "coder").await;
    db.server_run_cap
        .set(Some(4), 4, &config::embedded_machine_id());
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    for _ in 0..3 {
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    }
    let current = machine_capacity_task(&db, &t1.id).await;
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&current)
            .unwrap()
            .capability,
        "project_capacity"
    );
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (1, 1));
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&t2.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status = 'done' WHERE id = ?")
        .bind(&t2.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        t1.id
    );
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        1
    );
}

#[tokio::test]
async fn machine_capacity_fifty_full_waiters_use_zero_write_transactions() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 100, DaemonStatus::Online, AgentStatus::Idle).await;
    let busy = seed_task(&db, &project_id, "busy", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    for i in 0..50 {
        let task = seed_task(&db, &project_id, &format!("wait {i}"), "todo", 0).await;
        assign_role(&db, &task.id, "coder", &agent).await;
    }
    let (dispatcher, _) = build_dispatcher(db.clone(), workspaces.path()).await;
    dispatcher.check_once_and_drain().await.unwrap(); // Establish visible waits and Project readiness caches.
                                                      // A Task that starts waiting for a run slot is read once more after its
                                                      // wait row exists; that look is part of establishing the wait.
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let _trace = tracing::subscriber::set_default(MachineCapacityWriteCounter(writes.clone()));
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
    assert_eq!(writes.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[tokio::test]
async fn machine_capacity_restart_between_prepare_and_start_relaunches_ready() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let id = seed_agent(&db, 8, DaemonStatus::Online, AgentStatus::Idle).await;
    let agent = AgentRepo::get_by_id(&*db, &id).await.unwrap().unwrap();
    let task = seed_task(&db, &project_id, "restart", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &id).await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let (before_restart, _) = build_dispatcher(db.clone(), workspaces.path()).await;
    let placement_id = before_restart
        .task_service
        .ready_workspace_for_capacity_test(&task, &agent)
        .await
        .unwrap();
    drop(before_restart);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .task_id,
        task.id
    );
    assert_eq!(
        db::WorkspacePlacementRepo::get_for_task(&*db, &task.id)
            .await
            .unwrap()
            .unwrap()
            .id,
        placement_id
    );
}
