//! Execution recovery, resumption and submission eligibility.
use super::*;

#[tokio::test]
async fn re_execute_cancelled_execution_dispatches_fresh() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service
        .re_execute_execution(parent_execution.id)
        .await
        .expect("re-execute succeeds");

    assert_eq!(result.execution.role, "coder".to_owned());
    assert_eq!(result.execution.status, ExecutionStatus::Running);
    assert_eq!(result.execution.parent_execution_id, None);
    assert_eq!(result.execution.agent_session_id, None);
}

/// Re-execute used to carry the parent execution's Agent over, so a role
/// reassigned after that execution failed the assignment CAS inside the
/// execution INSERT and surfaced as a bare version conflict -- which made
/// recovery impossible in exactly the case it exists for: swapping a broken
/// principal for a working one.
#[tokio::test]
async fn re_execute_follows_a_reassigned_role_principal() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let original_agent_id = seed_agent(&db).await;
    let replacement_agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(original_agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("workflow role assignment creates");
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(original_agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("failed parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some("executor died".to_owned()),
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");
    service
        .reassign_role(
            role_assignment_input(&task.id, "coder", Some(replacement_agent_id.clone()), None),
            false,
            false,
        )
        .await
        .expect("role reassignment applies");

    let result = service
        .re_execute_execution(parent_execution.id)
        .await
        .expect("re-execute succeeds after the role was reassigned");

    assert_eq!(result.execution.agent_id, Some(replacement_agent_id));
    assert_eq!(result.execution.status, ExecutionStatus::Running);
}

#[tokio::test]
async fn re_execute_rejects_running_parent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("parent execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(matches!(
        result,
        Err(ServiceError::InvalidOperation { message })
            if message.contains("re-execute requires")
    ));
}

#[tokio::test]
async fn re_execute_rejects_concurrent_running_execution() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::CODER,
        Some(&agent_id),
    )
    .await;
    // The concurrency guard is workspace-scoped: a second worker is only a
    // conflict when it would enter the same worktree.
    let workspace_id = seed_workspace_for_task(&db, &task, &repo_id, workspace_root.path()).await;
    let now = now_rfc3339();
    let parent_execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Cancelled,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("test-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("cancelled parent".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("parent execution creates");
    ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("running sibling".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                r#"{"executor_type":"shell","config":{}}"#.to_owned(),
            ),
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running execution creates");

    let result = service.re_execute_execution(parent_execution.id).await;

    assert!(
        matches!(result, Err(ServiceError::ExecutionAlreadyRunning { .. })),
        "a running sibling must reject re-execution, got {:?}",
        result.as_ref().err()
    );
}

#[tokio::test]
async fn interactive_execution_completion_does_not_trigger_review_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;
    let task = service
        .create_task(
            project_id,
            "Interactive no cascade",
            Some("printf no-cascade".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let launched = service
        .launch_execution(task.id.clone(), agent_id, None, None)
        .await
        .expect("launch succeeds");

    let registry = Arc::new(cli_adapters::test_support::test_registry());
    let executor = executors::AdapterExecutor::new(registry);
    let execution = service
        .run_execution(launched.execution.id.clone(), &executor)
        .await
        .expect("interactive execution runs");
    assert_eq!(execution.status, ExecutionStatus::Completed);

    service
        .maybe_cascade_executor_completion(&launched.execution.id)
        .await
        .expect("cascade check succeeds");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress".to_owned());
}

#[tokio::test]
async fn recover_reexecute_without_blocked_execution_dispatches_current_state_role() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(PendingExecutor))
        .with_repo_cache_locks(Arc::new(RepoCacheLockManager::default()))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment created");
    let annotation = json!({
        "type": "recovery_required",
        "blocking_reason": "crash_recovery",
        "blocked_by": "system:crash_recovery",
        "blocked_at": now_rfc3339(),
        "message": "Recovered after server restart",
        "recovery_actions": ["reexecute", "reset_to_initial", "cancel_task"],
    })
    .to_string();
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task recovery annotation saved");

    assert_eq!(
        service
            .test_action_values(task.id.clone())
            .await
            .expect("recovery actions resolve"),
        vec![
            api_types::TaskAction::Cancel { reason: None },
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None
            },
            api_types::TaskAction::Restart { reason: None },
            api_types::TaskAction::Approve {
                override_checks: Some(true),
                reason: None
            },
        ]
    );

    for unadvertised in [
        api_types::TaskAction::Retry {
            fresh_session: Some(false),
            refresh_workspace: None,
            reset_budget: None,
            guidance: None,
            reason: None,
        },
        api_types::TaskAction::Retry {
            reason: None,
            fresh_session: None,
            refresh_workspace: None,
            reset_budget: Some(true),
            guidance: None,
        },
    ] {
        let error = service
            .test_apply_action(
                task.id.clone(),
                unadvertised,
                Some("must not widen recovery contract".to_owned()),
                None,
            )
            .await
            .expect_err("unadvertised recovery action is rejected");
        assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
        let still_blocked = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .expect("task reloads")
            .expect("task exists");
        assert!(still_blocked.error_annotation.is_some());
        assert_eq!(
            ExecutionRepo::count_by_task_and_role(
                &*db,
                &task.id,
                crate::workflow::default_roles::CODER,
            )
            .await
            .expect("execution count loads"),
            0
        );
    }

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            Some("test".to_owned()),
            Some("resume current work".to_owned()),
        )
        .await
        .expect("reexecute recovers");

    assert_eq!(recovered.status, "in_progress");
    assert_eq!(recovered.error_annotation, None);
    let executions = ExecutionRepo::list_by_task(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .expect("executions load");
    assert_eq!(executions.items.len(), 1);
    assert_eq!(
        executions.items[0].role,
        crate::workflow::default_roles::CODER
    );
    assert_eq!(executions.items[0].status, ExecutionStatus::Running);
    assert!(executions.items[0]
        .summary
        .as_deref()
        .unwrap_or_default()
        .contains("resume current work"));
}

#[tokio::test]
async fn submit_is_not_available_while_agent_work_has_not_completed() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment created");
    let never_run_actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("never-run actions resolve");
    assert!(
        !never_run_actions.contains(&api_types::TaskAction::Approve {
            reason: None,
            override_checks: Some(false)
        }),
        "an assigned Task must not skip its first coder execution"
    );
    let running = seed_running_coder_execution(&db, &task.id, Some(agent_id), None).await;

    let actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Approve {
        reason: None,
        override_checks: Some(false)
    }));

    let error = service
        .test_apply_intent(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            None,
            Some(task.version),
        )
        .await
        .expect_err("submit cannot bypass unfinished agent work");
    assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
    let execution = ExecutionRepo::get_by_id(&*db, &running.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    assert_eq!(execution.status, ExecutionStatus::Running);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "in_progress");
}

#[tokio::test]
async fn submit_does_not_reuse_a_completed_attempt_from_before_review_remediation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    TransitionLogRepo::insert(
        &*db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            from_state: crate::workflow::default_states::REVIEW.to_owned(),
            to_state: crate::workflow::default_states::IN_PROGRESS.to_owned(),
            trigger_name: Some("reject".to_owned()),
            triggered_by: "user:api".to_owned(),
            bridge: Default::default(),
            trigger_reason: "review remediation".to_owned(),
            hook_results_json: None,
            rejection: true,
            // Keep the boundary unambiguously newer than the seeded attempt.
            created_at: "2099-01-01T00:00:00Z".to_owned(),
        },
    )
    .await
    .expect("review remediation boundary records");

    let actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Approve {
        reason: None,
        override_checks: Some(false)
    }));
}

#[tokio::test]
async fn resume_is_not_offered_for_unrelated_role_history() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        Some("coder-session"),
        "2026-01-01T00:00:00Z",
    )
    .await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::INTERACTIVE,
        ExecutionStatus::Completed,
        Some("interactive-session"),
        "2026-01-02T00:00:00Z",
    )
    .await;

    let actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(
        !actions.iter().any(|action| matches!(
            action,
            api_types::TaskAction::Retry {
                fresh_session: Some(false),
                ..
            }
        )),
        "unrelated role history must never authorize a reviewer session resume"
    );
    let offers = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .all(|offer| offer.target_execution_id.is_none()));
}

#[tokio::test]
async fn submit_uses_latest_current_role_execution_not_later_interactive_history() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        crate::workflow::default_roles::INTERACTIVE,
        ExecutionStatus::Completed,
        Some("interactive-session"),
        "2099-01-01T00:00:00Z",
    )
    .await;

    let actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(actions.contains(&api_types::TaskAction::Approve {
        reason: None,
        override_checks: Some(false)
    }));
}

#[tokio::test]
async fn resume_without_session_clears_manual_stop_before_reexecute() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workspace_root = TempDir::new().expect("workspace temp dir creates");
    let service = TaskService::new(Arc::clone(&db), event_bus)
        .with_task_executor(Arc::new(NoDiffExecutor))
        .with_workspace_root(workspace_root.path().to_path_buf());
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        role_assignment_input(
            &task.id,
            crate::workflow::default_roles::CODER,
            Some(agent_id.clone()),
            None,
        ),
    )
    .await
    .expect("coder assignment creates");
    let parent = seed_execution(
        &db,
        &task.id,
        Some(&agent_id),
        "executor",
        ExecutionStatus::Completed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    let annotation = api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ManualStop,
        blocking_reason: "user_paused".to_owned(),
        blocked_by: Some("user:api".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: Some(parent.id.clone()),
        artifact: None,
        message: Some("paused".to_owned()),
        hook: None,
    };
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(
                serde_json::to_string(&annotation).expect("annotation serializes"),
            )),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("manual-stop annotation saves");

    let result = service
        .test_apply_intent(
            task.id.clone(),
            api_types::TaskAction::Release { reason: None },
            Some("continue after pause".to_owned()),
            Some(task.version),
        )
        .await
        .expect("resume re-executes the no-session parent");
    assert_eq!(result.task.error_annotation, None);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(current.error_annotation.is_none());
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .expect("running executions load")
        .iter()
        .any(|execution| execution.id != parent.id));
}

/// A reviewer execution binds the current Review attempt and that binding
/// only accepts a `Running` attempt. Recovery re-executes inside `review`,
/// where no transition hook runs, so a settled attempt -- the normal shape
/// after a reviewer execution dies -- made `reexecute` reject every attempt
/// as a bare `version_conflict` with no other advertised way out.
#[tokio::test]
async fn reexecute_opens_a_fresh_review_attempt_when_the_last_one_settled() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service =
        TaskService::new(Arc::clone(&db), event_bus).with_task_executor(Arc::new(NoDiffExecutor));
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let coder_agent_id = seed_agent(&db).await;
    let reviewer_agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    for (role, agent_id) in [
        (crate::workflow::default_roles::CODER, &coder_agent_id),
        (crate::workflow::default_roles::REVIEWER, &reviewer_agent_id),
    ] {
        TaskRoleAssignmentRepo::assign(
            &*db,
            role_assignment_input(&task.id, role, Some(agent_id.clone()), None),
        )
        .await
        .expect("role assignment creates");
    }
    let candidate = seed_execution(
        &db,
        &task.id,
        Some(&coder_agent_id),
        crate::workflow::default_roles::CODER,
        ExecutionStatus::Completed,
        None,
        "2026-01-01T00:00:00Z",
    )
    .await;
    seed_execution(
        &db,
        &task.id,
        Some(&reviewer_agent_id),
        crate::workflow::default_roles::REVIEWER,
        ExecutionStatus::Failed,
        None,
        "2026-01-01T00:01:00Z",
    )
    .await;
    crate::task_service::tests::helpers::seed_failed_review(
        &db,
        &task.id,
        &candidate.id,
        1,
        serde_json::json!({}),
    )
    .await;
    let annotation = serde_json::to_string(&api_types::TaskBlockingAnnotation {
        annotation_type: api_types::FailureKind::ExecutorFailed,
        blocking_reason: "executor_error".to_owned(),
        blocked_by: Some("system:executor".to_owned()),
        blocked_at: Some(now_rfc3339()),
        blocked_execution_id: None,
        artifact: None,
        message: Some("the reviewer executor died".to_owned()),
        hook: None,
    })
    .expect("annotation serializes");
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(Some(annotation)),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("blocking annotation saves");

    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            Some("test".to_owned()),
            None,
        )
        .await
        .expect("reexecute recovers a task whose review attempt already settled");

    assert_eq!(recovered.status, "review");
    assert_eq!(recovered.error_annotation, None);
    // Recovery returning at all is the regression: the reviewer execution
    // could only be created because a fresh attempt was open for it to bind.
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    let latest = reviews
        .iter()
        .max_by_key(|review| review.attempt_number)
        .expect("a review attempt exists");
    assert_eq!(latest.attempt_number, 2);
    assert_eq!(latest.execution_id, candidate.id);
    assert!(
        ExecutionRepo::count_by_task_and_role(
            &*db,
            &task.id,
            crate::workflow::default_roles::REVIEWER,
        )
        .await
        .expect("reviewer executions count")
            >= 2
    );
}

#[tokio::test]
async fn hard_failed_active_task_cannot_resume_or_submit() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "in_progress".to_owned()).await;
    let task = TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: Some(Some(json!({ "reason": "executor failed" }).to_string())),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("hard failure records");

    let actions = service
        .test_action_values(task.id.clone())
        .await
        .expect("actions resolve");
    assert!(!actions.contains(&api_types::TaskAction::Release { reason: None }));
    assert!(!actions.contains(&api_types::TaskAction::Approve {
        reason: None,
        override_checks: Some(false)
    }));
    for action in [
        api_types::TaskAction::Release { reason: None },
        api_types::TaskAction::Approve {
            reason: None,
            override_checks: Some(false),
        },
    ] {
        let error = service
            .test_apply_intent(task.id.clone(), action, None, Some(task.version))
            .await
            .expect_err("hard failure blocks generic task action");
        assert!(matches!(error, ServiceError::TaskActionUnavailable { .. }));
    }
}
