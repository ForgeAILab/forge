//! Cleanup scheduling, tick batching and backoff.

use super::*;

#[tokio::test]
async fn schedule_writes_cleanup_after() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

    scheduler
        .schedule(&workspace_id, Duration::from_secs(60))
        .await
        .expect("cleanup schedules");

    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert!(workspace.cleanup_after.is_some());
}

#[tokio::test]
async fn loop_replays_past_due_workspaces() {
    let db = sqlite_db().await;
    let event_bus = Arc::new(EventBus::new(16));
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, _) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let cleanup_after = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    WorkspaceRepo::set_cleanup_after(&*db, &workspace_id, Some(cleanup_after), &now_rfc3339())
        .await
        .expect("cleanup deadline sets");
    let scheduler =
        WorkspaceCleanupScheduler::new(Arc::clone(&db), event_bus, temp.path().to_path_buf());

    scheduler.tick().await.expect("tick succeeds");

    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .expect("workspace loads")
        .expect("workspace exists");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
}

#[tokio::test]
async fn terminal_transitions_schedule_cleanup_and_preserve_cancelled_grace() {
    for (terminal, with_hooks) in [
        ("done", false),
        ("done", true),
        ("cancelled", false),
        ("cancelled", true),
    ] {
        let db = sqlite_db().await;
        let bus = Arc::new(EventBus::new(16));
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, "todo").await;
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        if !with_hooks {
            for state in &mut workflow.states {
                state.hooks = api_types::StateHooks::default();
            }
        }
        let project = ProjectRepo::get_by_id(&*db, &task.project_id)
            .await
            .expect("project loads")
            .expect("project exists");
        ProjectRepo::update_workflow(
            &*db,
            &project.id,
            &serde_json::to_string(&workflow).expect("workflow serializes"),
            None,
            project.version,
            &now_rfc3339(),
        )
        .await
        .expect("workflow updates");
        let scheduler = Arc::new(WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::clone(&bus),
            temp.path().to_path_buf(),
        ));
        let service = crate::TaskService::new(Arc::clone(&db), bus)
            .with_cleanup_scheduler(Arc::clone(&scheduler))
            .with_workspace_root(temp.path().to_path_buf());
        let updated = service
            .transition(
                task.id.clone(),
                terminal.to_owned(),
                (task.version, Some("finish cleanup fixture".to_owned())),
            )
            .await
            .expect("terminal transition succeeds");
        assert_eq!(updated.task.status, terminal);
        service.drain(&task.id).await.unwrap();
        assert!(
            worktree_path.exists(),
            "deletion must run outside the transition"
        );
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .expect("workspace loads")
            .expect("workspace exists");
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        let deadline = chrono::DateTime::parse_from_rfc3339(
            workspace
                .cleanup_after
                .as_deref()
                .expect("cleanup schedules"),
        )
        .unwrap();
        if terminal == "cancelled" {
            assert!(deadline > chrono::Utc::now() + chrono::Duration::hours(23));
            scheduler.tick().await.expect("tick respects grace");
            scheduler.sweep().await.expect("sweep respects grace");
            assert!(worktree_path.exists());
            assert!(logs_dir(temp.path(), &task)
                .join(".codex-managed-home")
                .exists());
            // Advance the persisted terminal timestamp and deadline past grace.
            sqlx::query("UPDATE task SET updated_at = ?, version = version + 1 WHERE id = ?")
                .bind((chrono::Utc::now() - chrono::Duration::hours(25)).to_rfc3339())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            scheduler
                .schedule(&workspace_id, Duration::ZERO)
                .await
                .unwrap();
        }
        scheduler.tick().await.expect("worker cleans due workspace");
        assert!(!worktree_path.exists());
        assert_retained_logs(temp.path(), &task);
        assert_eq!(
            WorkspaceRepo::get_by_id(&*db, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkspaceStatus::Cleaned
        );
        let source = temp.path().join(".repos").join(&workspace.repo_id);
        assert!(git::branch_exists(&source, &workspace.branch)
            .await
            .expect("branch lookup"));
    }
}

#[tokio::test]
async fn nonterminal_tasks_are_untouched_even_with_completed_executions_and_stale_deadlines() {
    for status in ["review", "merge_failed"] {
        let db = sqlite_db().await;
        let temp = TempDir::new().expect("temp dir creates");
        let (workspace_id, worktree_path) =
            seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
        let task = fixture_task(&db, &workspace_id).await;
        let task = set_fixture_status(&db, &task, status).await;
        fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Completed).await;
        let scheduler = WorkspaceCleanupScheduler::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(16)),
            temp.path().to_path_buf(),
        );
        scheduler
            .schedule(&workspace_id, Duration::ZERO)
            .await
            .expect("deadline sets");
        scheduler
            .cleanup_now(workspace_id.clone())
            .await
            .expect("cleanup skips");
        scheduler.tick().await.expect("tick succeeds");
        scheduler.sweep().await.expect("sweep succeeds");
        assert!(worktree_path.join("node_modules/build-output").exists());
        assert!(logs_dir(temp.path(), &task)
            .join(".codex-managed-home")
            .exists());
        let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        assert!(workspace.cleanup_after.is_none());
    }
}

#[tokio::test]
async fn worker_robustness_cleanup_backs_off_failed_head_and_continues() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (failed_id, failed_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let (healthy_id, healthy_path) = seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    WorkspaceRepo::set_cleanup_after(
        &*db,
        &failed_id,
        Some("2000-01-01T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    WorkspaceRepo::set_cleanup_after(
        &*db,
        &healthy_id,
        Some("2001-01-01T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler.set_terminal_cleanup_handler(Arc::new(FailingWorkspaceCleanup {
        workspace_id: failed_id.clone(),
    }));

    scheduler.tick().await.unwrap();

    let failed = WorkspaceRepo::get_by_id(&*db, &failed_id)
        .await
        .unwrap()
        .unwrap();
    let healthy = WorkspaceRepo::get_by_id(&*db, &healthy_id)
        .await
        .unwrap()
        .unwrap();
    let first_retry = chrono::DateTime::parse_from_rfc3339(
        failed.cleanup_after.as_deref().expect("retry is scheduled"),
    )
    .unwrap();
    assert!(failed_path.exists());
    assert_eq!(failed.cleanup_attempts, 1);
    assert!(failed.last_cleanup_error.is_some());
    assert!(first_retry > chrono::Utc::now());
    assert_eq!(healthy.status, WorkspaceStatus::Cleaned);
    assert!(!healthy_path.exists());

    WorkspaceRepo::set_cleanup_after(
        &*db,
        &failed_id,
        Some("2002-01-01T00:00:00Z".to_owned()),
        &now_rfc3339(),
    )
    .await
    .unwrap();
    scheduler.tick().await.unwrap();

    let failed = WorkspaceRepo::get_by_id(&*db, &failed_id)
        .await
        .unwrap()
        .unwrap();
    let second_retry = chrono::DateTime::parse_from_rfc3339(
        failed
            .cleanup_after
            .as_deref()
            .expect("retry is rescheduled"),
    )
    .unwrap();
    assert_eq!(failed.cleanup_attempts, 2);
    assert!(second_retry > first_retry);
}

#[tokio::test]
async fn tick_clears_ineligible_batch_and_reaches_later_terminal_task() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let repo_id = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap()
        .repo_id;
    let mut stale_ids = Vec::new();
    for index in 0..SWEEP_LIMIT {
        let stale_task = seed_log_only_task(
            &db,
            temp.path(),
            &task,
            new_uuid_v4(),
            None,
            if index < 2 { "done" } else { "review" },
        )
        .await;
        let id = new_uuid_v4();
        WorkspaceRepo::create(
            &*db,
            CreateWorkspace {
                id: id.clone(),
                task_id: stale_task.id.clone(),
                repo_id: repo_id.clone(),
                worktree_path: temp
                    .path()
                    .join(&stale_task.id)
                    .join("repo")
                    .to_string_lossy()
                    .into_owned(),
                branch: workspace::task_branch_name(&stale_task.id),
                status: WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        WorkspaceRepo::set_cleanup_after(
            &*db,
            &id,
            Some((chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339()),
            &now_rfc3339(),
        )
        .await
        .unwrap();
        if index < 2 {
            // Reproduce historical dangling references without changing
            // the schema's normal foreign-key guarantees.
            let mut connection = db.pool().acquire().await.unwrap();
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *connection)
                .await
                .unwrap();
            if index == 0 {
                sqlx::query("UPDATE task SET project_id = ?, version = version + 1 WHERE id = ?")
                    .bind(new_uuid_v4())
                    .bind(&stale_task.id)
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            } else {
                sqlx::query("UPDATE workspace SET task_id = ? WHERE id = ?")
                    .bind(new_uuid_v4())
                    .bind(&id)
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            }
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        stale_ids.push(id);
    }
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler
        .schedule(&workspace_id, Duration::ZERO)
        .await
        .unwrap();
    scheduler.tick().await.unwrap();
    assert!(
        worktree_path.exists(),
        "first batch contains only ineligible rows"
    );
    for id in stale_ids {
        let workspace = WorkspaceRepo::get_by_id(&*db, &id).await.unwrap().unwrap();
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        assert!(workspace.cleanup_after.is_none());
    }
    scheduler.tick().await.unwrap();
    assert!(
        !worktree_path.exists(),
        "ineligible rows must not starve later cleanup"
    );
}

#[tokio::test]
async fn tick_backs_off_errors_before_workspace_is_loaded() {
    let db = sqlite_db().await;
    let temp = TempDir::new().unwrap();
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler
        .schedule(&workspace_id, Duration::ZERO)
        .await
        .unwrap();
    let mut connection = db.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET status = 'invalid' WHERE id = ?")
        .bind(&workspace_id)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    scheduler.tick().await.unwrap();
    let deadline =
        sqlx::query_scalar::<_, String>("SELECT cleanup_after FROM workspace WHERE id = ?")
            .bind(&workspace_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(chrono::DateTime::parse_from_rfc3339(&deadline).unwrap() > chrono::Utc::now());
    assert!(worktree_path.exists());
    sqlx::query("UPDATE workspace SET status = 'ready' WHERE id = ?")
        .bind(&workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    scheduler.tick().await.unwrap();
    assert!(worktree_path.exists(), "failed row respects retry backoff");
    scheduler
        .schedule(&workspace_id, Duration::ZERO)
        .await
        .unwrap();
    scheduler.tick().await.unwrap();
    assert!(!worktree_path.exists());
}

#[tokio::test]
async fn running_execution_blocks_terminal_cleanup_until_settled() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler
        .cleanup_now(workspace_id.clone())
        .await
        .expect("cleanup defers");
    scheduler.sweep().await.expect("sweep succeeds");
    assert!(worktree_path.exists());
    assert!(logs_dir(temp.path(), &task)
        .join(".codex-managed-home")
        .exists());
    sqlx::query("UPDATE execution SET status = 'completed' WHERE task_id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("execution settles");
    scheduler
        .schedule(&workspace_id, Duration::ZERO)
        .await
        .unwrap();
    scheduler.sweep().await.expect("backfill retries");
    assert!(!worktree_path.exists());
    assert_retained_logs(temp.path(), &task);
}

#[tokio::test]
async fn active_workspace_lease_blocks_cleanup_even_after_execution_stops() {
    let db = sqlite_db().await;
    let temp = TempDir::new().expect("temp dir creates");
    let (workspace_id, worktree_path) =
        seed_workspace(&db, temp.path(), WorkspaceStatus::Ready).await;
    let task = fixture_task(&db, &workspace_id).await;
    let now = now_rfc3339();
    let agent = db::AgentRepo::create(
        &*db,
        db::CreateAgent {
            id: new_uuid_v4(),
            name: "Cleanup lease worker".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");
    let execution =
        fixture_execution(&db, &task, &workspace_id, db::ExecutionStatus::Running).await;
    sqlx::query("UPDATE execution SET agent_id = ? WHERE id = ?")
        .bind(&agent.id)
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .expect("execution agent binds");
    let workspace = WorkspaceRepo::get_by_id(&*db, &workspace_id)
        .await
        .unwrap()
        .unwrap();
    let lease =
        db::WorkspaceLeaseRepo::issue(
            &*db,
            db::CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id: task.project_id.clone(),
                task_id: task.id.clone(),
                task_version: task.version,
                execution_id: execution.id.clone(),
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: workspace.repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: "[\"repository_write\"]".to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent.id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: now.clone(),
                expires_at: (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("lease issues");
    // Simulate interrupted historical settlement that left the lease active.
    sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .expect("execution stops");
    let scheduler = WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::new(EventBus::new(16)),
        temp.path().to_path_buf(),
    );
    scheduler
        .cleanup_now(workspace_id.clone())
        .await
        .expect("cleanup defers");
    scheduler.sweep().await.expect("sweep defers");
    assert!(worktree_path.exists());
    assert!(logs_dir(temp.path(), &task)
        .join(".codex-managed-home")
        .exists());
    db::WorkspaceLeaseRepo::revoke(&*db, &lease.id, lease.version, &now_rfc3339())
        .await
        .expect("lease revokes");
    scheduler
        .schedule(&workspace_id, Duration::ZERO)
        .await
        .unwrap();
    scheduler.sweep().await.expect("cleanup retries");
    assert!(!worktree_path.exists());
    assert_retained_logs(temp.path(), &task);
}
