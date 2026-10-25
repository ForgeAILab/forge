//! Startup recovery cases.

use super::*;

#[tokio::test]
async fn crash_recovery_suspends_stale_native_sessions() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let agent = seed_agent(&db, AgentStatus::Idle, None).await;
    let now = now_rfc3339();
    let native_profile = AgentProfileRepo::create_profile(
        &*db,
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: agent.id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("native profile creates");

    let stale_ready = seed_agent_session(
        &db,
        &agent.id,
        &native_profile.id,
        "native",
        "scope-ready",
        "ready",
        "healthy",
    )
    .await;
    let stale_running = seed_agent_session(
        &db,
        &agent.id,
        &native_profile.id,
        "native",
        "scope-running",
        "running",
        "healthy",
    )
    .await;
    let cancelled = seed_agent_session(
        &db,
        &agent.id,
        &native_profile.id,
        "native",
        "scope-cancelled",
        "cancelled",
        "healthy",
    )
    .await;
    let replaced = seed_agent_session(
        &db,
        &agent.id,
        &native_profile.id,
        "native",
        "scope-replaced",
        "replaced",
        "healthy",
    )
    .await;
    let cli_ready = seed_agent_session(
        &db,
        &agent.id,
        &agent.profile_id,
        "cli",
        "scope-cli",
        "ready",
        "healthy",
    )
    .await;

    CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");

    // Stale native sessions in non-terminal statuses are suspended with
    // an unverified connection, and the version bump preserves the
    // optimistic-concurrency contract.
    let ready_after = load_session(&db, &stale_ready.id).await;
    assert_eq!(ready_after.status, "suspended");
    assert_eq!(ready_after.connection_status, "unknown");
    assert_eq!(ready_after.version, stale_ready.version + 1);
    let running_after = load_session(&db, &stale_running.id).await;
    assert_eq!(running_after.status, "suspended");
    assert_eq!(running_after.connection_status, "unknown");
    assert_eq!(running_after.version, stale_running.version + 1);

    // The suspended session vacates the active-scope slot so the reuse
    // path re-establishes a fresh session on next use.
    assert!(AgentSessionRepo::get_active_agent_session(
        &*db,
        &agent.id,
        &ready_after.context_scope_id
    )
    .await
    .expect("active session query runs")
    .is_none());

    // Terminal native sessions and external-backend sessions are left
    // untouched.
    let cancelled_after = load_session(&db, &cancelled.id).await;
    assert_eq!(cancelled_after.status, "cancelled");
    assert_eq!(cancelled_after.connection_status, "healthy");
    assert_eq!(cancelled_after.version, cancelled.version);
    let replaced_after = load_session(&db, &replaced.id).await;
    assert_eq!(replaced_after.status, "replaced");
    assert_eq!(replaced_after.version, replaced.version);
    let cli_after = load_session(&db, &cli_ready.id).await;
    assert_eq!(cli_after.status, "ready");
    assert_eq!(cli_after.connection_status, "healthy");
    assert_eq!(cli_after.version, cli_ready.version);
}

#[tokio::test]
async fn crash_recovery_ignores_idle_in_progress_tasks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let in_progress = seed_task(
        &db,
        project_id.clone(),
        "in_progress".to_owned(),
        Some(agent.id),
    )
    .await;
    let todo = seed_task(&db, project_id, "todo".to_owned(), None).await;

    let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
    let recovered = recovery.run_recovery().await.expect("recovery runs");
    assert_eq!(recovered, 0);

    let recovered_task = TaskRepo::get_by_id(&*db, &in_progress.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(recovered_task.status, "in_progress".to_owned());
    assert_eq!(recovered_task.assignee_id, in_progress.assignee_id);
    assert_eq!(recovered_task.error_annotation, None);

    let unchanged = TaskRepo::get_by_id(&*db, &todo.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(unchanged.error_annotation, None);

    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn crash_recovery_skips_user_assigned_in_progress_tasks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let user_task = seed_task_with_assignee(
        &db,
        project_id,
        "in_progress".to_owned(),
        "user",
        Some("human-user".to_owned()),
    )
    .await;

    let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
    let recovered = recovery.run_recovery().await.expect("recovery runs");
    assert_eq!(recovered, 0);

    let unchanged = TaskRepo::get_by_id(&*db, &user_task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(unchanged.status, "in_progress");
    assert_eq!(unchanged.error_annotation, None);

    assert!(rx.try_recv().is_err());
}

/// The start-up sweep removes what a dead process left in a server-owned
/// Task root and nothing else: not the directory of a hook, check or tool
/// command running in this process (they are in no table), not one made
/// since this process started, and nothing beside a legacy-shaped path.
#[cfg(unix)]
#[tokio::test]
async fn recovery_sweeps_temp_dirs_of_dead_runs_and_never_one_of_a_live_run() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id) = seed_project_repo(&db).await;
    let root = tempfile::tempdir().expect("workspace root");
    let legacy = tempfile::tempdir().expect("user directory");
    let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
    let backdate = |dir: &std::path::Path| {
        std::fs::File::open(dir)
            .and_then(|file| file.set_modified(old))
            .expect("directory is back-dated");
    };
    let mut worktrees = Vec::new();
    for shape in ["managed", "legacy"] {
        let task = seed_task(&db, project_id.clone(), "todo".to_owned(), None).await;
        let worktree = if shape == "managed" {
            root.path().join(&task.id).join("repo")
        } else {
            legacy.path().join("projects").join("repo")
        };
        std::fs::create_dir_all(&worktree).expect("worktree creates");
        let now = now_rfc3339();
        let workspace = db::WorkspaceRepo::create(
            &*db,
            db::CreateWorkspace {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: worktree.to_string_lossy().into_owned(),
                branch: format!("task/{shape}"),
                status: db::WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        let location = db::RepoLocationRepo::create(
            &*db,
            db::CreateRepoLocation {
                id: new_uuid_v4(),
                repo_id: repo_id.clone(),
                owner_kind: db::RepoLocationOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                path: format!("checkout-{shape}"),
                kind: db::RepoLocationKind::SharedMount,
                is_default: shape == "managed",
                status: db::RepoLocationStatus::Ready,
                last_verified_at: Some(now.clone()),
                last_error: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("location creates");
        WorkspacePlacementRepo::create(
            &*db,
            db::CreateWorkspacePlacement {
                id: new_uuid_v4(),
                workspace_id: workspace.id,
                task_id: task.id,
                agent_id: None,
                owner_kind: PlacementOwnerKind::Server,
                daemon_id: None,
                runtime_id: None,
                repo_location_id: location.id,
                execution_daemon_id: None,
                workspace_handle: Some(worktree.to_string_lossy().into_owned()),
                generation: 1,
                state: PlacementState::Ready,
                selected_by: db::PlacementSelectedBy::Scheduler,
                selection_reason: "{}".to_owned(),
                reserved_until: None,
                disconnected_at: None,
                failure_cause: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("placement creates");
        worktrees.push(worktree);
    }
    let task_root = worktrees[0].parent().unwrap();
    executors::sandbox::TaskRoot::reserve(task_root).expect("Task root reserves");
    // A hook running right now, in this process, however old its directory looks.
    let hook = executors::sandbox::SandboxEnv::for_command(
        &worktrees[0],
        executors::sandbox::RunPurpose::Hook,
    );
    let hook_tmp = hook
        .env()
        .tmp_dir()
        .expect("hook has a temp dir")
        .to_path_buf();
    // In the Task root, or in the short directory beside it when the
    // test's temp path is long: the sweep covers both.
    let tmp = hook_tmp.parent().expect("temp parent").to_path_buf();
    assert!(tmp.starts_with(root.path()));
    backdate(&hook_tmp);
    // What the previous process left, and what something made since.
    let (dead, recent) = (tmp.join("deadrun"), tmp.join("recentrun"));
    std::fs::create_dir_all(dead.join("leftover")).unwrap();
    std::fs::create_dir(&recent).unwrap();
    backdate(&dead);
    // A run the previous process left detached (a protected push, a
    // check still settling): made before this process, in nobody's
    // table, and possibly still alive.
    let detached = tmp.join("detachedrun");
    std::fs::create_dir(&detached).unwrap();
    std::fs::File::open(&detached)
        .and_then(|file| {
            file.set_modified(
                std::time::SystemTime::now() - std::time::Duration::from_secs(3 * 60 * 60),
            )
        })
        .expect("directory is back-dated");
    // Beside the legacy path: a user's directories, old or not.
    let user_short = legacy.path().join(".forge-tmp/olduser");
    let user_task = legacy.path().join("projects/.forge-task/tmp/olduser");
    for dir in [&user_short, &user_task] {
        std::fs::create_dir_all(dir).unwrap();
        backdate(dir);
    }

    CrashRecovery::new(Arc::clone(&db), Arc::new(EventBus::new(16)))
        .run_recovery()
        .await
        .expect("recovery runs");

    assert!(!dead.exists(), "the dead run's directory is removed");
    assert!(hook_tmp.is_dir(), "a live hook keeps its directory");
    assert!(recent.is_dir(), "a directory made since start is kept");
    assert!(
        detached.is_dir(),
        "a previous process's run that may still be alive keeps its directory"
    );
    assert!(user_short.is_dir() && user_task.is_dir());
    drop(hook);
    assert!(!hook_tmp.exists());
}

#[tokio::test]
async fn recovery_monitors_all_active_states() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(
            serde_json::to_string(&crate::workflow::default_workflow::default_workflow()).unwrap(),
        )
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent = seed_agent(
        &db,
        AgentStatus::Busy,
        Some("1970-01-01T00:00:00+00:00".to_owned()),
    )
    .await;

    let task_in_progress = seed_task(
        &db,
        project_id.clone(),
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let task_merge_failed =
        seed_task(&db, project_id, "merge_failed".to_owned(), Some(agent.id)).await;

    let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
    let recovered = recovery.run_recovery().await.expect("recovery runs");
    assert_eq!(recovered, 0);

    let t1 = TaskRepo::get_by_id(&*db, &task_in_progress.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(t1.status, "in_progress");

    let t2 = TaskRepo::get_by_id(&*db, &task_merge_failed.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(t2.status, "merge_failed");
}

#[tokio::test]
async fn crash_recovery_auto_redispatches_running_executions_in_any_active_state() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let in_progress_agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let merge_failed_agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let in_progress = seed_task(
        &db,
        project_id.clone(),
        "in_progress".to_owned(),
        Some(in_progress_agent.id.clone()),
    )
    .await;
    let merge_failed = seed_task(
        &db,
        project_id,
        "merge_failed".to_owned(),
        Some(merge_failed_agent.id.clone()),
    )
    .await;
    let in_progress_execution =
        seed_running_execution(&db, in_progress.id.clone(), in_progress_agent.id, None).await;
    let merge_failed_execution =
        seed_running_execution(&db, merge_failed.id.clone(), merge_failed_agent.id, None).await;

    let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
    let recovered = recovery.run_recovery().await.expect("recovery runs");
    assert_eq!(recovered, 0);

    for (task, execution, expected_status) in [
        (in_progress, in_progress_execution, "in_progress"),
        (merge_failed, merge_failed_execution, "merge_failed"),
    ] {
        for task_id in sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
        )
        .fetch_all(db.pool())
        .await
        .unwrap()
        {
            crate::test_support::drain_task_steps(&db, &task_id).await;
        }
        let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        assert_eq!(updated_task.status, expected_status);
        assert!(updated_task.error_annotation.is_none());

        let updated = ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        assert_eq!(updated.status, ExecutionStatus::Cancelled);
        assert!(updated.error.as_deref().unwrap().contains("Recovered"));
        assert_eq!(updated.resume_policy, Some(ResumePolicy::Auto));
    }
}

#[tokio::test]
async fn repeated_restart_queues_one_recovery_and_publishes_task_recovered_when_it_settles() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
    let recovery = CrashRecovery::new(Arc::clone(&db), event_bus);
    // Two restarts before the queue drains: one recovery command for the
    // Task and its dead execution.
    recovery.run_recovery().await.expect("first restart");
    recovery.run_recovery().await.expect("second restart");
    let commands = db
        .task_steps(&task.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|step| {
            step.kind == "command" && step.payload_json.contains("recover_task_after_restart")
        })
        .count();
    assert_eq!(commands, 1);

    let bus = Arc::new(EventBus::new(64));
    let mut rx = bus.subscribe();
    crate::TaskService::new(Arc::clone(&db), Arc::clone(&bus))
        .drain(&task.id)
        .await
        .expect("recovery step settles");
    assert_eq!(
        ExecutionRepo::get_by_id(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Cancelled
    );
    let mut recovered = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if event.event_type == "task.recovered" {
            recovered.push(event);
        }
    }
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].entity_id, task.id);
    match &recovered[0].context {
        EventContext::TaskRecovered { reason, .. } => assert_eq!(reason, "crash_recovery"),
        other => panic!("unexpected task.recovered context: {other:?}"),
    }
}

#[tokio::test]
async fn cancel_running_executions_returns_cancelled_execution_metadata() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(
        &db,
        task.id.clone(),
        agent.id.clone(),
        Some("session-789".to_owned()),
    )
    .await;

    let cancelled = cancel_running_executions(
        &db,
        &task.id,
        StopReason::CrashRecovery,
        &api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
        ResumePolicy::Manual,
    )
    .await
    .expect("cancellation succeeds");

    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].execution_id, execution.id);
    assert_eq!(
        cancelled[0].agent_session_id.as_deref(),
        Some("session-789")
    );
}

#[tokio::test]
async fn recover_task_cas_loss_does_not_revoke_successor_workspace_lease() {
    let db = Arc::new(sqlite_db().await);
    let (project_id, repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id.clone(),
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;

    let workspace_id = new_uuid_v4();
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task.id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: format!("/tmp/forge-recovery-{workspace_id}"),
            branch: ::workspace::task_branch_name(&task.id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("successor workspace creates");

    // Leave an active lease for a newer successor attempt. The successor
    // is made terminal through a direct fixture update so the stale
    // recovery calls below only race on the older running attempt; the
    // lease remains the protected task-scoped grant.
    let successor = seed_running_execution_in_workspace(
        &db,
        task.id.clone(),
        agent.id.clone(),
        None,
        Some(workspace_id.clone()),
    )
    .await;
    let now = Utc::now();
    let successor_lease =
        WorkspaceLeaseRepo::issue(
            &*db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id,
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: successor.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent.id.clone(),
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: (now - ChronoDuration::minutes(1)).to_rfc3339(),
                expires_at: (now + ChronoDuration::hours(1)).to_rfc3339(),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .expect("successor lease issues");
    sqlx::query("UPDATE execution SET status = 'completed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&successor.id)
        .execute(db.pool())
        .await
        .expect("successor fixture terminalizes");

    let stale_execution = seed_running_execution_in_workspace(
        &db,
        task.id.clone(),
        agent.id.clone(),
        None,
        Some(workspace_id),
    )
    .await;
    let stale_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("stale task loads")
        .expect("stale task exists");
    let stopped_by = api_types::Actor::system(api_types::SystemComponent::CrashRecovery);
    let stale_task_for_race = stale_task.clone();
    let (first, second) = tokio::join!(
        recover_task(&db, stale_task, StopReason::CrashRecovery, &stopped_by),
        recover_task(
            &db,
            stale_task_for_race,
            StopReason::CrashRecovery,
            &stopped_by,
        ),
    );
    let first = first.expect("first recovery race call");
    let second = second.expect("second recovery race call");
    // An interrupted implementation execution is now left for automatic
    // redispatch, so neither caller annotates the Task. The race invariant
    // this test exists for is the one below: the loser must not touch the
    // successor's lease.
    assert!(
        !first.annotated && !second.annotated,
        "an auto-resumable recovery does not park the Task behind an annotation"
    );

    let cancelled = ExecutionRepo::get_by_id(&*db, &stale_execution.id)
        .await
        .expect("raced execution loads")
        .expect("raced execution exists");
    assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    let persisted_lease = WorkspaceLeaseRepo::get_by_id(&*db, &successor_lease.id)
        .await
        .expect("successor lease loads")
        .expect("successor lease exists");
    assert_eq!(persisted_lease.status, "active");
    assert_eq!(
        persisted_lease.version, successor_lease.version,
        "a recovery caller that lost the terminal CAS is inert for successor leases"
    );
}

#[tokio::test]
async fn crash_recovery_keeps_active_task_with_resumable_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(1024));
    let _task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(
        &db,
        task.id.clone(),
        agent.id,
        Some("session-123".to_owned()),
    )
    .await;

    CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated_task.status, "in_progress");

    let updated_execution = ExecutionRepo::get_by_id(&*db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(updated_execution.status, ExecutionStatus::Cancelled);
    assert_eq!(
        updated_execution.agent_session_id.as_deref(),
        Some("session-123")
    );
    assert_eq!(updated_execution.resume_policy, Some(ResumePolicy::Auto));
}

#[tokio::test]
async fn crash_recovery_does_not_follow_up_resumable_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(1024));
    let _task_service = Arc::new(TaskService::new(Arc::clone(&db), Arc::clone(&event_bus)));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(
        &db,
        task.id.clone(),
        agent.id,
        Some("session-456".to_owned()),
    )
    .await;

    CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let updated_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated_task.status, "in_progress");

    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(executions.items[0].id, execution.id);
    assert_eq!(executions.items[0].status, ExecutionStatus::Cancelled);
}

#[tokio::test]
async fn crash_recovery_clears_stale_recovery_annotations_without_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let task = seed_task(&db, project_id, "in_progress".to_owned(), None).await;
    stamp_recovery_annotation(&db, &task, None).await;

    let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");
    assert_eq!(recovered, 1);

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.error_annotation, None);
}

#[tokio::test]
async fn crash_recovery_preserves_legitimate_pending_recovery_annotations() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
    let terminalized_at = now_rfc3339();
    ExecutionRepo::terminalize(
        &*db,
        TerminalizeExecution {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            lease_owner: execution.lease_owner.clone(),
            status: ExecutionStatus::Cancelled,
            stop_reason: Some(Some(StopReason::CrashRecovery)),
            stopped_by: Some(Some("system:crash_recovery".to_owned())),
            resume_policy: Some(Some(ResumePolicy::Manual)),
            stopped_at: Some(Some(terminalized_at.clone())),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(Some("Recovered".to_owned())),
            executor_config_snapshot_json: None,
            updated_at: terminalized_at,
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .expect("execution terminalizes");
    let annotated = stamp_recovery_annotation(&db, &task, Some(&execution.id)).await;

    let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");
    assert_eq!(recovered, 0);

    let updated = TaskRepo::get_by_id(&*db, &annotated.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(
        updated.error_annotation.as_deref(),
        annotated.error_annotation.as_deref()
    );
    let annotation: Value =
        serde_json::from_str(updated.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocked_execution_id"], execution.id);
}

#[tokio::test]
async fn crash_recovery_clears_stale_recovery_annotations_for_missing_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let task = seed_task(&db, project_id, "in_progress".to_owned(), None).await;
    stamp_recovery_annotation(&db, &task, Some("missing-execution-id")).await;

    let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");
    assert_eq!(recovered, 1);

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.error_annotation, None);
}

#[tokio::test]
async fn crash_recovery_clears_stale_recovery_annotations_for_completed_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let (project_id, _repo_id) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".to_owned(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
    let terminalized_at = now_rfc3339();
    ExecutionRepo::terminalize(
        &*db,
        TerminalizeExecution {
            execution_id: execution.id.clone(),
            expected_version: execution.execution_version,
            lease_owner: execution.lease_owner.clone(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(Some(terminalized_at.clone())),
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
            updated_at: terminalized_at,
            actor_type: "system".to_owned(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .expect("execution terminalizes");
    stamp_recovery_annotation(&db, &task, Some(&execution.id)).await;

    let recovered = CrashRecovery::new(Arc::clone(&db), event_bus)
        .run_recovery()
        .await
        .expect("recovery runs");
    assert_eq!(recovered, 1);

    for task_id in sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT task_id FROM task_step WHERE status IN ('pending','claimed')",
    )
    .fetch_all(db.pool())
    .await
    .unwrap()
    {
        crate::test_support::drain_task_steps(&db, &task_id).await;
    }
    let updated = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(updated.error_annotation, None);
}

#[tokio::test]
async fn restart_recovery_queues_dead_execution_settlement_before_worker_runs() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(32));
    let (project_id, _) = seed_project_repo(&db).await;
    let agent = seed_agent(&db, AgentStatus::Busy, Some(now_rfc3339())).await;
    let task = seed_task(
        &db,
        project_id,
        "in_progress".into(),
        Some(agent.id.clone()),
    )
    .await;
    let execution = seed_running_execution(&db, task.id.clone(), agent.id, None).await;
    let restarted = Arc::new(SqliteDb::new(db.pool().clone()));
    CrashRecovery::new(restarted.clone(), bus.clone())
        .run_recovery()
        .await
        .unwrap();
    assert_eq!(
        ExecutionRepo::get_by_id(&*restarted, &execution.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ExecutionStatus::Running
    );
    let queued = restarted.task_steps(&task.id).await.unwrap();
    assert!(queued.iter().any(|s| s.kind == "command"
        && s.status == "pending"
        && s.payload_json.contains("recover_task_after_restart")));
    let service = TaskService::new(restarted.clone(), bus);
    service.drain(&task.id).await.unwrap();
    let settled = ExecutionRepo::get_by_id(&*restarted, &execution.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.status, ExecutionStatus::Cancelled);
    assert_eq!(settled.resume_policy, Some(ResumePolicy::Auto));
    assert!(restarted
        .task_steps(&task.id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "command" && s.status == "done"));
}
