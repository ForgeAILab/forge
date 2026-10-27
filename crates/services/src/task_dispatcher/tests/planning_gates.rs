//! Tests for planning gates in the task dispatcher.

use super::*;
#[tokio::test]
async fn dispatcher_check_once_does_not_dispatch_after_stop() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "high", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    dispatcher.stop();

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "todo");
    assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn dispatcher_skips_unassigned_planning_gate_before_coder_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "high", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "in_progress");
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_waits_for_deferred_dispatch_cooldown() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "deferred",
        crate::workflow::default_states::IN_PROGRESS,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    deferred_dispatch::set(
        &db,
        &task,
        crate::workflow::default_states::IN_PROGRESS,
        &(chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
        "test cooldown",
    )
    .await
    .expect("deferred dispatch metadata writes");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        0
    );

    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    deferred_dispatch::set(
        &db,
        &task,
        crate::workflow::default_states::IN_PROGRESS,
        &(chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339(),
        "test cooldown expired",
    )
    .await
    .expect("deferred dispatch metadata updates");
    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let execution_ctx = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("executor spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(deferred_dispatch::pending_until(&task).is_none());
}

#[tokio::test]
async fn dispatcher_enters_unassigned_auto_planning_gate_before_coder_dispatch() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_auto_approval(&db, &project_id).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project_id, "auto plan", "todo", 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, "in_progress");
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    let executions = ExecutionRepo::list_by_task_and_role(
        &*db,
        &task.id,
        crate::workflow::default_roles::CODER,
        PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].agent_id.as_deref(),
        Some(agent_id.as_str())
    );
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(transitions
        .iter()
        .any(|entry| entry.from_state == "todo" && entry.to_state == "planning"));
    assert!(transitions
        .iter()
        .any(|entry| entry.from_state == "planning" && entry.to_state == "in_progress"));
}

#[tokio::test]
async fn dispatcher_recovers_task_stuck_in_unassigned_optional_planning_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "stuck planning",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let dispatched = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");

    assert_eq!(dispatched, 1);
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.status, crate::workflow::default_states::IN_PROGRESS);
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("execution spawned in time")
        .expect("execution context received");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER
        )
        .await
        .expect("execution count loads"),
        1
    );
}

#[tokio::test]
async fn dispatcher_does_not_relaunch_planner_awaiting_plan_review() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_user_approval(&db, &project_id, true).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "planned",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;

    let task_workspace_dir = workspace_dir.path().join(&task.id);
    let worktree_path = task_workspace_dir.join("forge");
    std::fs::create_dir_all(&worktree_path).expect("worktree creates");
    let now = now_rfc3339();
    let workspace = WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree_path.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    std::fs::write(
        task_workspace_dir.join("plan.md"),
        "- [ ] implement the planned change\n",
    )
    .expect("plan artifact writes");
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::PLANNER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("plan ready for review".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: Some(workspace.id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed planner execution creates");
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": now,
                "planning_execution_id": execution.id,
                "planning_state_entry_token": null,
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("metadata update");
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first_scan = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    let second_scan = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs again");

    assert_eq!((first_scan, second_scan), (0, 0));
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::PLANNER
        )
        .await
        .expect("execution count loads"),
        1
    );
    let still_waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(
        still_waiting.status,
        crate::workflow::default_states::PLANNING
    );
    assert_eq!(still_waiting.version, waiting.version);
    let metadata: serde_json::Value = serde_json::from_str(
        still_waiting
            .metadata_json
            .as_deref()
            .expect("waiting metadata exists"),
    )
    .expect("waiting metadata parses");
    assert_eq!(
        metadata
            .get("planning_execution_id")
            .and_then(serde_json::Value::as_str),
        Some(execution.id.as_str())
    );
    assert_eq!(
        metadata
            .get("awaiting_human")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[tokio::test]
async fn dispatcher_clears_plan_review_wait_from_an_older_planning_entry() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    set_planning_gate_user_approval(&db, &project_id, true).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "re-entered planning",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;

    let old_entry_id = new_uuid_v4();
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: old_entry_id.clone(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::BACKLOG.to_owned(),
            to_state: crate::workflow::default_states::PLANNING.to_owned(),
            trigger_name: Some("plan".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            bridge: Default::default(),
            trigger_reason: "first planning entry".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-01-01T00:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("old planning entry records");
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::PLANNER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some("2026-01-01T00:00:01+00:00".to_owned()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("old plan ready".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "task_state": crate::workflow::default_states::PLANNING,
                    "state_entry_token": old_entry_id.clone(),
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: "2026-01-01T00:00:01+00:00".to_owned(),
            updated_at: "2026-01-01T00:00:01+00:00".to_owned(),
        },
    )
    .await
    .expect("old planner execution creates");
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": "2026-01-01T00:00:01+00:00",
                "planning_execution_id": execution_id,
                "planning_state_entry_token": old_entry_id,
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("old wait metadata writes");
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            to_state: crate::workflow::default_states::PLANNING.to_owned(),
            trigger_name: Some("replan".to_owned()),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow).display(),
            bridge: Default::default(),
            trigger_reason: "second planning entry".to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-01-01T00:00:02+00:00".to_owned(),
        },
    )
    .await
    .expect("new planning entry records");

    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let replacement = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement planner dispatches")
        .expect("replacement execution context arrives");
    assert_eq!(replacement.task_id, task.id);
    let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata = recovered.metadata().expect("metadata parses");
    assert!(metadata.extra.get("awaiting_human").is_none());
    assert!(metadata.extra.get("planning_state_entry_token").is_none());
}

#[tokio::test]
async fn project_wide_sweep_cleans_plan_files_for_paused_removed_state() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "retired gate", "removed_gate", 1).await;
    let task_root = workspace_dir.path().join(&task.id);
    let worktree = task_root.join("forge");
    std::fs::create_dir_all(&worktree).expect("worktree creates");
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: worktree.to_string_lossy().into_owned(),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("workspace creates");
    let execution_id = new_uuid_v4();
    crate::plan_artifact::prepare_execution_plan_outbox(&worktree, &execution_id, "planner", None)
        .expect("outbox prepares");
    crate::plan_artifact::write_execution_outbox_plan(
        &worktree,
        &execution_id,
        "- [ ] remove private bytes\n",
    )
    .expect("candidate writes");
    let outbox = executors::existing_execution_outbox(&worktree, &execution_id)
        .expect("outbox resolves")
        .expect("prepared outbox exists");
    crate::plan_artifact::stage_execution_outbox_plan(&outbox, &worktree, &execution_id)
        .expect("candidate stages");
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let marker = serde_json::json!({
        "execution_id": execution_id.clone(),
        "state": "removed_gate",
        "state_entry_token": null,
        "project_version": project.version,
    });
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "terminal_execution_settlement": marker.clone(),
                "plan_publication_cleanup": marker,
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("cleanup marker writes");
    sqlx::query("UPDATE project SET paused_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project pauses");

    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let paused_project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    let stage_path = task_root
        .join(".forge-plan-staging")
        .join(format!("{execution_id}.md"));
    std::fs::remove_file(&stage_path).expect("staged file removes for failure injection");
    std::fs::create_dir(&stage_path).expect("stage-path directory injects cleanup failure");
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("failed private cleanup is contained"),
        0
    );
    let still_pending = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(still_pending
        .metadata()
        .expect("metadata parses")
        .extra
        .get("plan_publication_cleanup")
        .is_some());
    std::fs::remove_dir(&stage_path).expect("failure injection removes");
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("publication cleanup reconciles"),
        1
    );
    assert!(!stage_path.exists());
    assert!(!executors::execution_outbox_path(&worktree, &execution_id)
        .expect("outbox path is contained")
        .exists());
    let cleaned = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata = cleaned.metadata().expect("metadata parses");
    assert!(metadata.extra.get("plan_publication_cleanup").is_none());
    assert!(metadata
        .extra
        .get("terminal_execution_settlement")
        .is_some());
    assert_eq!(
        dispatcher
            .reconcile_plan_publication_claims(&paused_project)
            .await
            .expect("completed cleanup is quiescent"),
        0
    );
}

#[tokio::test]
async fn dispatcher_clears_stale_plan_review_wait_on_default_auto_approval_gate() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(
        &db,
        &project_id,
        "legacy planning wait",
        crate::workflow::default_states::PLANNING,
        1,
    )
    .await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        &agent_id,
    )
    .await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "awaiting_human": true,
                "awaiting_human_reason": "plan_review",
                "planning_completed_at": now_rfc3339(),
                "awaiting_human_marker_id": new_uuid_v4(),
            })
            .to_string(),
        )
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("legacy wait metadata writes");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    assert_eq!(
        dispatcher
            .check_once_and_drain()
            .await
            .expect("dispatcher runs"),
        1
    );
    let execution_ctx = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("planner dispatches in time")
        .expect("planner execution context arrives");
    assert_eq!(execution_ctx.task_id, task.id);
    assert_eq!(
        executors::task_role(&execution_ctx.agent_config),
        Some(crate::workflow::default_roles::PLANNER)
    );
    let recovered = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value = recovered
        .metadata_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .expect("task metadata parses")
        .unwrap_or_else(|| serde_json::json!({}));
    assert!(metadata.get("awaiting_human").is_none());
    assert!(metadata.get("awaiting_human_reason").is_none());
    assert!(metadata.get("planning_completed_at").is_none());
    assert!(metadata.get("awaiting_human_marker_id").is_none());
}

#[tokio::test]
async fn dispatcher_repeated_scans_do_not_churn_or_redispatch_completed_custom_gate() {
    const CUSTOM_GATE: &str = "manual_qa";

    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().expect("repo dir creates");
    let workspace_dir = TempDir::new().expect("workspace dir creates");
    let (project_id, _repo_id) = seed_project_repo(&db, repo_dir.path()).await;
    let agent_id = seed_agent(&db, 1, DaemonStatus::Online, AgentStatus::Idle).await;

    let mut workflow = crate::workflow::default_workflow::default_workflow();
    let mut custom_gate = workflow
        .states
        .iter()
        .find(|state| state.name == crate::workflow::default_states::PLANNING)
        .expect("default planning gate exists")
        .clone();
    custom_gate.name = CUSTOM_GATE.to_owned();
    custom_gate.display_name = "Manual QA".to_owned();
    custom_gate.role = Some(crate::workflow::default_roles::CODER.to_owned());
    let gate_config = custom_gate
        .gate_config
        .as_mut()
        .expect("cloned gate has config");
    gate_config.requires_user_approval = Some(true);
    gate_config.optional_when_unassigned = Some(false);
    workflow.states.push(custom_gate);
    sqlx::query(
        "UPDATE project SET workflow_definition = ?, workflow_template_name = ?, updated_at = ? WHERE id = ?",
    )
    .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
    .bind("custom")
    .bind(now_rfc3339())
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("custom workflow updates");

    let task = seed_task(&db, &project_id, "manual QA", CUSTOM_GATE, 1).await;
    assign_role(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        &agent_id,
    )
    .await;
    let project_version = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .expect("Project loads")
        .expect("Project exists")
        .version;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("custom gate work complete".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                serde_json::json!({
                    "executor_type": "shell",
                    "config": {},
                    "project_version": project_version,
                    "task_state": CUSTOM_GATE,
                    "state_entry_token": null,
                })
                .to_string(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("completed custom gate execution creates");
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let (dispatcher, mut rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let first_scan = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs");
    let second_scan = dispatcher
        .check_once_and_drain()
        .await
        .expect("dispatcher runs again");

    assert_eq!((first_scan, second_scan), (0, 0));
    assert!(rx.try_recv().is_err());
    assert_eq!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::CODER,
        )
        .await
        .expect("execution count loads"),
        1
    );
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(after.status, CUSTOM_GATE);
    assert_eq!(after.version, before.version);
}
