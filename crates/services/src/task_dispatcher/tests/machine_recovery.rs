//! Tests for machine recovery in the task dispatcher.

use super::*;
#[tokio::test]
async fn machine_capacity_active_waiter_is_parked_at_final_version() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let revision_before: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    dispatcher
        .task_service
        .transition(
            queued.id.clone(),
            "in_progress".to_owned(),
            crate::task_service::TransitionOptions {
                bridge: Default::default(),
                version: queued.version,
                reason: Some("manual start".to_owned()),
                triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    dispatcher.drain_steps().await.unwrap();
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    let marker = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(marker.capability, "machine_capacity");
    assert_eq!(marker.task_version, waiting.version);
    let health = crate::task_diagnostics::derive_workflow_health(
        &waiting,
        &workflow,
        &[],
        None,
        None,
        false,
        None,
    );
    assert_eq!(health.stale_reason.as_deref(), Some("machine_capacity"));
    let current = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let revision_after: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(revision_after > revision_before);
    let slots = super::slots::load_project_slots(&db, &current)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (1, 1));
    let batch = super::slots::load_projects_slots(&db, std::slice::from_ref(&current))
        .await
        .unwrap();
    assert_eq!(batch[&project_id].slots, slots);
    assert_eq!(
        batch[&project_id].revision,
        Some((revision_after, current.version))
    );
    assert!(waiting.error_annotation.is_none());
    for _ in 0..4 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let stable = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stable.version, waiting.version);
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&running.id)
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
        queued.id
    );
}
#[tokio::test]
async fn machine_capacity_queued_recovery_is_quiet_until_slot_frees() {
    let action = api_types::TaskAction::Retry {
        reason: Some("operator retry".to_owned()),
        fresh_session: Some(true),
        refresh_workspace: None,
        reset_budget: None,
        guidance: None,
    };
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id =
        seed_agent_with_executor(&db, 4, DaemonStatus::Online, AgentStatus::Idle, "codex").await;
    let busy = seed_task(&db, &project_id, "occupies machine", "in_progress", 0).await;
    assign_role(&db, &busy.id, "coder", &agent_id).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());

    let task = seed_task(&db, &project_id, "recover", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let stopped = seed_cancelled_execution(
        &db,
        &task.id,
        &agent_id,
        "coder",
        Some(StopReason::UserCancelled),
        Some(ResumePolicy::Manual),
    )
    .await;
    let annotation = serde_json::json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_execution_id": serde_json::Value::Null,
        "recovery_actions": ["reexecute"],
    });
    let _ = stopped;
    let task = TaskRepo::update(
        &*db,
        UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation.to_string())),
            blocked_json: Some(Some(
                serde_json::json!({"kind": "recovery_required", "reason": "crash_recovery"})
                    .to_string(),
            )),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("interruption persists");
    set_prompt_execution_snapshots(&db, &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    let queued = dispatcher
        .task_service
        .perform_task_action(&task.id, action, task.version)
        .await
        .unwrap()
        .task;
    let marker = deferred_dispatch::queued_recovery(&queued).unwrap();
    while events.try_recv().is_ok() {}
    for _ in 0..4 {
        dispatcher.check_once_and_drain().await.unwrap();
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.version, queued.version);
        assert_eq!(
            deferred_dispatch::queued_recovery(&current).unwrap().id,
            marker.id
        );
        assert_eq!(current.metadata_json, queued.metadata_json);
        while let Ok(event) = events.try_recv() {
            assert_ne!(event.entity_id, task.id, "{}", event.event_type);
        }
    }
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
    let started = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::queued_recovery(&started).is_none());
}

#[tokio::test]
async fn machine_capacity_queued_recovery_lost_race_is_quiet() {
    let action = api_types::TaskAction::Retry {
        reason: Some("operator retry".to_owned()),
        fresh_session: Some(true),
        refresh_workspace: None,
        reset_budget: None,
        guidance: None,
    };
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id =
        seed_agent_with_executor(&db, 8, DaemonStatus::Online, AgentStatus::Idle, "codex").await;
    let busy = seed_task(&db, &project_id, "occupies machine", "in_progress", 0).await;
    assign_role(&db, &busy.id, "coder", &agent_id).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let task = machine_capacity_interrupted_task(&db, &project_id, &agent_id, "R").await;
    set_prompt_execution_snapshots(&db, &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();
    let queued = dispatcher
        .task_service
        .perform_task_action(&task.id, action, task.version)
        .await
        .unwrap()
        .task;
    let marker = deferred_dispatch::queued_recovery(&queued).unwrap();

    while events.try_recv().is_ok() {}
    // Slot free at precheck time; taken again the moment the replay claims the Task.
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER machine_capacity_race AFTER UPDATE OF metadata_json ON task WHEN NEW.id = '{}' AND json_extract(NEW.metadata_json, '$.deferred_dispatch.reason') = 'recovery replay in flight' BEGIN UPDATE execution SET status = 'running' WHERE task_id = '{}'; END",
        task.id, busy.id
    ))
    .execute(db.pool())
    .await
    .unwrap();
    for _ in 0..3 {
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
        let current = machine_capacity_task(&db, &task.id).await;
        assert_eq!(current.version, queued.version);
        assert_eq!(
            deferred_dispatch::queued_recovery(&current).unwrap().id,
            marker.id
        );
        assert!(current.error_annotation.is_none());
        assert!(current.blocked_json.is_none());
        assert!(!deferred_dispatch::is_pending(&current, chrono::Utc::now()));
        while let Ok(event) = events.try_recv() {
            assert_ne!(event.entity_id, task.id, "{}", event.event_type);
        }
    }
    sqlx::query("DROP TRIGGER machine_capacity_race")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    let started = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.task_id, task.id);
    assert!(
        deferred_dispatch::queued_recovery(&machine_capacity_task(&db, &task.id).await).is_none()
    );
}

#[tokio::test]
async fn machine_capacity_stale_marker_clears_on_agent_skip() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let agent_id = seed_agent(&db, 0, DaemonStatus::Online, AgentStatus::Idle).await; // max 1
    let running = seed_task(&db, &project_id, "RUN", "in_progress", 0).await;
    seed_running_execution(&db, &running.id, &agent_id, "coder").await;
    let queued = seed_task(&db, &project_id, "WAIT", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    // The Task waited for the machine earlier; the machine now has room (unlimited).
    deferred_dispatch::record_dispatch_disposition(
        &db,
        &queued,
        "machine_capacity",
        "machine_capacity: waiting for a machine run slot",
    )
    .await
    .unwrap();
    db.server_run_cap
        .set(Some(0), 0, &config::embedded_machine_id());
    let (dispatcher, _rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    let queued = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    dispatcher
        .task_service
        .transition(
            queued.id.clone(),
            "in_progress".to_owned(),
            crate::task_service::TransitionOptions {
                bridge: Default::default(),
                version: queued.version,
                reason: Some("manual start".to_owned()),
                triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();

    let after = machine_capacity_task(&db, &queued.id).await;
    assert!(deferred_dispatch::current_dispatch_disposition(&after).is_none());
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (2, 0));
    for _ in 0..2 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    assert!(deferred_dispatch::current_dispatch_disposition(
        &machine_capacity_task(&db, &queued.id).await
    )
    .is_none());
}

#[tokio::test]
async fn machine_capacity_review_recovery_is_quiet_and_resumes() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "REV", "in_progress", 0).await;
    assign_role(&db, &task.id, "coder", &agent_id).await;
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), workspaces.path()).await;
    // A real coder run with a real workspace, then it completes and review fails.
    dispatcher.check_once_and_drain().await.unwrap();
    let ctx = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, task.id);
    let candidate: String =
        sqlx::query_scalar("SELECT id FROM execution WHERE task_id = ? AND status = 'running'")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let failed_at = (chrono::Utc::now() - chrono::Duration::minutes(3)).to_rfc3339();
    let entered_at =
        (chrono::Utc::now() - chrono::Duration::minutes(3) - chrono::Duration::seconds(1))
            .to_rfc3339();
    sqlx::query(
        "UPDATE execution SET status = 'completed', stopped_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(&failed_at)
    .bind(&failed_at)
    .bind(&candidate)
    .execute(db.pool())
    .await
    .unwrap();
    // RecordingExecutor does not settle leases. Model the normal coder
    // completion cleanup before putting its candidate into failed review.
    let lease = db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .unwrap()
        .unwrap();
    db::WorkspaceLeaseRepo::revoke(&*db, &lease.id, lease.version, &now_rfc3339())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status = 'review', version = version + 1, task_state_config = ? WHERE id = ?")
        .bind(serde_json::json!({"retry_budgets":{"review":1}}).to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate,
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: r#"{"ci_steps":[{"index":0,"command":"false","exit_code":1,"stderr_tail":"CI failed"}]}"#
                .to_owned(),
            started_at: failed_at.clone(),
            created_at: failed_at.clone(),
            updated_at: failed_at,
        },
    )
    .await
    .unwrap();
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: "in_progress".to_owned(),
            to_state: "review".to_owned(),
            trigger_name: None,
            triggered_by: Actor::system(SystemComponent::Workflow).display(),
            bridge: Default::default(),
            trigger_reason: "coder completed".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: entered_at,
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE project SET settings = json_set(settings, '$.automatic_recovery', json_object('enabled', json('true'), 'agent_id', ?, 'max_attempts', 1)) WHERE id = ?")
        .bind(&agent_id).bind(&project_id).execute(db.pool()).await.unwrap();
    let busy = seed_task(&db, &project_id, "BUSY", "in_progress", 0).await;
    seed_running_execution(&db, &busy.id, &agent_id, "coder").await;
    db.server_run_cap
        .set(Some(1), 1, &config::embedded_machine_id());
    let before = machine_capacity_task(&db, &task.id).await;
    let mut events = dispatcher.event_bus.subscribe();
    for _ in 0..4 {
        assert_eq!(dispatcher.check_once_and_drain().await.unwrap(), 0);
        let current = machine_capacity_task(&db, &task.id).await;
        assert_eq!(current.version, before.version);
        assert!(current.error_annotation.is_none());
        assert!(current.blocked_json.is_none());
        while let Ok(event) = events.try_recv() {
            assert_ne!(event.entity_id, task.id, "{}", event.event_type);
        }
    }
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&busy.id)
        .execute(db.pool())
        .await
        .unwrap();
    dispatcher.check_once_and_drain().await.unwrap();
    let after = machine_capacity_task(&db, &task.id).await;
    let runs = ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert!(
        !runs.is_empty(),
        "status={} version={} annotation={:?} blocked={:?} marker={:?}",
        after.status,
        after.version,
        after.error_annotation,
        after.blocked_json,
        deferred_dispatch::current_dispatch_disposition(&after)
    );
    assert_eq!(
        runs.len(),
        1,
        "the ordinary entry hook must not start a second coder"
    );
    let started = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(started.task_id, task.id);
    let execution = ExecutionRepo::get_by_id(&*db, &started.execution_id)
        .await
        .unwrap()
        .unwrap();
    let purpose: Option<String> = sqlx::query_scalar("SELECT purpose FROM execution WHERE id=?")
        .bind(&execution.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(purpose.as_deref(), Some("automatic_review_recovery"));
}
