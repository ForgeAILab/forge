//! Tests for project capacity in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_project_capacity_disposition_refreshes_only_on_change() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    let queued = seed_task(&db, &project_id, "queued", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let mut events = dispatcher.event_bus.subscribe();

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let recorded = events.try_recv().unwrap();
    assert_eq!(recorded.event_type, "task.updated");
    assert_eq!(recorded.entity_id, queued.id);
    assert!(matches!(
        recorded.context,
        events::EventContext::TaskUpdated { project_id: id } if id == project_id
    ));
    assert!(
        events.try_recv().is_err(),
        "first record emits exactly once"
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .capability,
        "project_capacity"
    );

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    assert!(events.try_recv().is_err(), "unchanged tick emits nothing");
    assert!(rx.try_recv().is_err());

    dispatcher
        .clear_dispatch_disposition(&waiting)
        .await
        .unwrap();
    let cleared = events.try_recv().unwrap();
    assert_eq!(cleared.event_type, "task.updated");
    assert_eq!(cleared.entity_id, queued.id);
    assert!(matches!(
        cleared.context,
        events::EventContext::TaskUpdated { project_id: id } if id == project_id
    ));
    assert!(events.try_recv().is_err(), "clear emits exactly once");
    let after_clear = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::dispatch_disposition(&after_clear).is_none());

    // Even a stale clearer still carrying the marker must not emit again.
    dispatcher
        .clear_dispatch_disposition(&waiting)
        .await
        .unwrap();
    dispatcher
        .clear_dispatch_disposition(&after_clear)
        .await
        .unwrap();
    assert!(events.try_recv().is_err(), "no-op clears emit nothing");
}

#[tokio::test]
async fn dispatcher_full_project_rechecks_capacity_without_task_version_change() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let mut active_tasks = Vec::new();
    for status in [
        "in_progress",
        "in_progress",
        "review",
        "review",
        "merge_failed",
    ] {
        active_tasks.push(seed_task(&db, &project_id, "admitted", status, 0).await);
    }
    let queued = seed_task(&db, &project_id, "NK-50", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

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
    assert_eq!(waiting.status, "todo");
    assert_eq!(waiting.version, queued.version);
    let disposition = deferred_dispatch::current_dispatch_disposition(&waiting).unwrap();
    assert_eq!(disposition.capability, "project_capacity");
    assert_eq!(
        disposition.safe_message,
        "project_at_capacity: waiting for a slot (5/5 active)"
    );
    assert!(!deferred_dispatch::dispatch_disposition_is_current(
        &waiting, "coder"
    ));
    assert!(rx.try_recv().is_err());

    // A sentinel timestamp makes even a same-clock repeated write observable.
    sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.dispatch_disposition.recorded_at', '2026-01-01T00:00:00Z'), updated_at = '2026-01-01T00:00:00Z' WHERE id = ?")
        .bind(&queued.id).execute(db.pool()).await.unwrap();
    let unchanged = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let repeated = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated.metadata_json, unchanged.metadata_json);
    assert_eq!(repeated.updated_at, unchanged.updated_at);

    TaskRepo::update_status(
        &*db,
        db::UpdateTaskStatus {
            id: active_tasks[0].id.clone(),
            expected_version: active_tasks[0].version,
            status: "done".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.status, "in_progress");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&admitted).is_none());
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        5
    );
}

#[tokio::test]
async fn dispatcher_project_limit_preserves_ready_queue_order_and_counts_each_admission() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    let low = seed_task(&db, &project_id, "low", "todo", 1).await;
    let high = seed_task(&db, &project_id, "high", "todo", 10).await;
    assign_role(&db, &low.id, "coder", &agent_id).await;
    assign_role(&db, &high.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, high.id);
    let waiting = TaskRepo::get_by_id(&*db, &low.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "todo");
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_at_capacity: waiting for a slot (1/1 active)"
    );
}

#[tokio::test]
async fn dispatcher_parked_review_frees_a_project_slot() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..4 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let parked = seed_task(&db, &project_id, "owner review", "review", 0).await;
    set_task_error_annotation(
        &db,
        &parked,
        r#"{"type":"review_needs_owner","message":"needs macOS","recovery_actions":["reexecute"]}"#,
    )
    .await;
    let queued = seed_task(&db, &project_id, "ready", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked, slots.queued), (5, 1, 0));
}

#[tokio::test]
async fn dispatcher_parked_guard_rechecks_when_owner_clears_a_park() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..2 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let mut parked_tasks = Vec::new();
    for _ in 0..10 {
        let task = seed_task(&db, &project_id, "owner review", "review", 0).await;
        set_task_error_annotation(&db, &task, r#"{"type":"review_needs_owner"}"#).await;
        parked_tasks.push(task);
    }
    let queued = seed_task(&db, &project_id, "ready", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
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
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_waiting_on_owner: 10 parked tasks waiting on the owner"
    );

    sqlx::query("UPDATE task SET error_annotation = NULL, version = version + 1 WHERE id = ?")
        .bind(&parked_tasks[0].id)
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(std::slice::from_ref(&parked_tasks[0].id))
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let admitted = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(deferred_dispatch::dispatch_disposition_for_test(&admitted).is_none());
}

#[tokio::test]
async fn dispatcher_owner_reexecute_resumes_over_project_limit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let parked = seed_task(&db, &project_id, "interrupted", "in_progress", 0).await;
    assign_role(&db, &parked.id, "coder", &agent_id).await;
    set_task_error_annotation(&db, &parked,
        r#"{"type":"recovery_required","blocking_reason":"crash_recovery","recovery_actions":["reexecute"]}"#).await;
    let queued = seed_task(&db, &project_id, "new work", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        5
    );

    let recovered = dispatcher
        .task_service
        .test_apply_action(
            &parked.id,
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            Some("host is ready".to_owned()),
            None,
        )
        .await
        .expect("owner recovery is admitted over the limit");
    assert!(recovered.error_annotation.is_none());
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, parked.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.active, slots.parked), (6, 0));
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let waiting = TaskRepo::get_by_id(&*db, &queued.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deferred_dispatch::current_dispatch_disposition(&waiting)
            .unwrap()
            .safe_message,
        "project_at_capacity: waiting for a slot (6/5 active)"
    );
}

#[tokio::test]
async fn dispatcher_project_limit_skips_projection_without_admission_candidate() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo.path()).await;
    let mut project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    project.settings = "invalid".to_owned();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, _) = build_dispatcher(Arc::clone(&db), workspace.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
    let task = seed_task(&db, &project_id, "blocked initial Task", "todo", 0).await;
    sqlx::query("UPDATE task SET blocked_json = '{}' WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn dispatcher_zero_project_limit_disables_capacity_and_parked_guard() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    for _ in 0..10 {
        let parked = seed_task(&db, &project_id, "owner review", "review", 0).await;
        set_task_error_annotation(&db, &parked, r#"{"type":"review_needs_owner"}"#).await;
    }
    let queued = seed_task(&db, &project_id, "new work", "todo", 0).await;
    assign_role(&db, &queued.id, "coder", &agent_id).await;
    let project = set_project_active_task_limit(&db, &project_id, 0).await;
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks_and_drain(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, queued.id);
    let slots = super::slots::load_project_slots(&db, &project)
        .await
        .unwrap();
    assert_eq!((slots.limit, slots.active, slots.parked), (0, 0, 0));
}

#[tokio::test]
async fn dispatcher_active_recovery_runs_over_project_limit() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 4, DaemonStatus::Online, AgentStatus::Idle).await;
    for _ in 0..5 {
        seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    }
    let recovering = seed_task(&db, &project_id, "undispatched", "in_progress", 0).await;
    assign_role(&db, &recovering.id, "coder", &agent_id).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        6
    );
    assert_eq!(
        dispatcher
            .recover_active_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx.task_id, recovering.id);
}

#[tokio::test]
async fn dispatcher_coordination_root_advance_is_not_capacity_gated() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    seed_task(&db, &project_id, "admitted", "in_progress", 0).await;
    let root = seed_task(&db, &project_id, "coordination", "todo", 0).await;
    let child = seed_task(&db, &project_id, "completed child", "done", 0).await;
    sqlx::query(
        "UPDATE task SET parent_task_id = ?, subtask_order = 0, version = version + 1 WHERE id = ?",
    )
    .bind(&root.id)
    .bind(&child.id)
    .execute(db.pool())
    .await
    .unwrap();
    // Exercise the coordination transitions without starting aggregate
    // review execution, which has its own independent admission checks.
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        state.hooks = api_types::StateHooks::default();
    }
    sqlx::query("UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let project = set_project_active_task_limit(&db, &project_id, 1).await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        1
    );
    assert_eq!(
        dispatcher
            .dispatch_initial_tasks(&project, &workflow)
            .await
            .unwrap(),
        1
    );
    let advanced = TaskRepo::get_by_id(&*db, &root.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advanced.status, "review");
    assert!(deferred_dispatch::dispatch_disposition_for_test(&advanced).is_none());
    // The advance itself was not gated; the root's own aggregate review now
    // holds a slot because no child does.
    assert_eq!(
        super::slots::load_project_slots(&db, &project)
            .await
            .unwrap()
            .active,
        2
    );
    assert!(rx.try_recv().is_err());
}
