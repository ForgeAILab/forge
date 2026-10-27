//! Workflow engine lifecycle tests.

use super::*;

#[tokio::test]
async fn lifecycle_ordering() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-lifecycle-ordering";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert_phases_are_ordered(&results);
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "auto_cascade_on_unassigned_role"
                && entry.phase == "after_enter"),
        "default todo -> planning should cascade to in_progress when no planner is assigned"
    );
}

#[tokio::test]
async fn transition_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-project-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let workflow = default_workflow::default_workflow();
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition_with_authority(
            task_id,
            default_states::IN_PROGRESS,
            task_before.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            false,
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.version, task_before.version);
    assert!(
        TransitionLogRepo::list_by_task(&*db, task_id)
            .await
            .expect("transition logs load")
            .is_empty(),
        "stale transition must not append a state-change log"
    );
    let running_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .expect("execution count loads");
    assert_eq!(running_count, 0, "stale transition must not dispatch work");
}

#[tokio::test]
async fn retry_entry_barrier_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-barrier-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let blocked = TaskRepo::set_entry_barrier(
        &*db,
        task_id,
        task_before.version,
        Some(
            serde_json::json!({
                "state": default_states::TODO,
                "status": "blocked",
                "started_at": now_rfc3339(),
            })
            .to_string(),
        ),
        &now_rfc3339(),
    )
    .await
    .expect("entry barrier is seeded");
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let workflow = default_workflow::default_workflow();
    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            task_id,
            blocked.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.version, blocked.version);
    assert_eq!(task_after.entry_barrier_json, blocked.entry_barrier_json);
}

#[tokio::test]
async fn reset_to_initial_rejects_stale_project_workflow_authority_without_mutation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-stale-reset-workflow";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let task_before = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let project = ProjectRepo::get_by_id(&*db, &task_before.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    ProjectRepo::update_workflow(
        &*db,
        &project.id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .restart_with_authority(
            task_id,
            default_states::TODO,
            task_before.version,
            &default_workflow::default_workflow(),
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale workflow authority",
            super::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition.clone(),
                clear_review_passed_at_on_commit: false,
            },
        )
        .await;
    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected stale workflow authority conflict"
    );

    let task_after = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.version, task_before.version);
    assert!(
        TransitionLogRepo::list_by_task(&*db, task_id)
            .await
            .expect("transition logs load")
            .is_empty(),
        "stale reset must not append a state-change log"
    );
}

#[tokio::test]
async fn default_workflow_allows_user_to_leave_planning() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-manual-exit";
    seed_project_repo_and_task(&db, task_id, default_states::PLANNING).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work after planning",
            false,
            Default::default(),
        )
        .await
        .expect("planning can be advanced by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
}

#[tokio::test]
async fn default_workflow_allows_user_to_start_work_from_todo() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-human-start-work";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start human work",
            false,
            Default::default(),
        )
        .await
        .expect("todo can be moved directly into active work by a user");

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::TODO
            && log.to_state == default_states::IN_PROGRESS
            && log.triggered_by == "user:test"
    }));
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.to_state == default_states::PLANNING),
        "dragging to the main In Progress column should not implicitly enter planning"
    );
}

#[tokio::test]
async fn default_workflow_skips_planning_when_no_planner_is_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-no-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("planning gate is skipped");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::PLANNING
            && log.to_state == default_states::IN_PROGRESS
            && log.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::GateSkipped)
            && !log.rejection
    }));
}

#[tokio::test]
async fn default_workflow_keeps_planning_when_planner_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-planning-human-planner";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_user_role(&db, task_id, default_roles::PLANNER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("human planning gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::PLANNING);
}

#[tokio::test]
async fn default_workflow_skips_system_review_when_no_checks_or_reviewer_are_assigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-no-checks";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "request review after agent completion",
            false,
            Default::default(),
        )
        .await
        .expect("system-entered unconfigured review skips to merge gate");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status.to_string(), default_states::MERGING);
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(transition_logs.iter().any(|log| {
        log.from_state == default_states::REVIEW
            && log.to_state == default_states::MERGING
            && log.trigger_reason == "review skipped: no checks or reviewer assigned"
    }));
}

#[tokio::test]
async fn default_workflow_keeps_user_requested_review_when_unassigned() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-requested";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request human review",
            false,
            Default::default(),
        )
        .await
        .expect("user-requested review gate is entered");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
    let stored = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let metadata = stored.metadata().expect("metadata parses");
    assert_eq!(metadata.extra.get("awaiting_human"), Some(&json!(true)));
    assert_eq!(
        metadata.extra.get("awaiting_human_reason"),
        Some(&json!("manual_review"))
    );
    let transition_logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs load");
    assert!(
        !transition_logs
            .iter()
            .any(|log| log.from_state == default_states::REVIEW
                && log.to_state == default_states::MERGING),
        "user-requested review should not immediately cascade into merge"
    );
}

#[tokio::test]
async fn default_workflow_keeps_review_when_reviewer_is_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-review-human-reviewer";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;
    assign_user_role(&db, task_id, default_roles::REVIEWER).await;
    let workflow = default_workflow::default_workflow();
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::REVIEW,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await
        .expect("human review gate is entered");

    assert_eq!(result.task.status.to_string(), default_states::REVIEW);
}

#[tokio::test]
async fn guard_rejection_returns_412_error() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut rx = event_bus.subscribe();
    let task_id = "task-guard-rejection";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::PLANNER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks {
                        before_exit: vec![hook(
                            "require_upstream_roles_completed",
                            FailurePolicy::Block,
                        )],
                        ..StateHooks::default()
                    },
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
            assert!(reason.contains(default_roles::PLANNER));
        }
        Err(error) => panic!("expected guard rejection, got {error:?}"),
        Ok(_) => panic!("expected guard rejection, got successful transition"),
    }

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);

    let events = drain_events(&mut rx);
    assert!(
        events
            .iter()
            .all(|event| event.event_type != "task.status_changed"),
        "guard rejection must not publish task.status_changed: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_type == "transition.guard_rejected"),
        "guard rejection event should be published"
    );
}

#[tokio::test]
async fn effect_failure_log_policy_continues() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-effect-failure-log";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    default_states::TODO,
                    StateKind::Initial,
                    None,
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                default_states::IN_PROGRESS,
            ),
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                Some(default_roles::CODER),
                StateHooks {
                    // A non-dispatch effect: Log-policy failures never move
                    // the task. (A failed *dispatch* entering an active state
                    // rolls the task back — covered separately below.)
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds despite logged effect failure");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let results = hook_results(&db, task_id).await;
    assert!(results.iter().any(|entry| {
        entry.action == "notify_role_holder"
            && entry.phase == "on_enter"
            && entry.outcome == "failed"
    }));
    assert!(result.task.error_annotation.is_none());
}

#[test]
fn validate_claimable_backlog_rejection() {
    let workflow = default_workflow::default_workflow();

    let result = WorkflowEngine::validate_claimable(&workflow, default_states::BACKLOG);
    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains(default_states::BACKLOG) || message.contains("not claimable"),
                "unexpected validation message: {message}"
            );
        }
        other => panic!("expected invalid operation, got {other:?}"),
    }

    WorkflowEngine::validate_claimable(&workflow, default_states::TODO)
        .expect("todo should be claimable");
}

#[tokio::test]
async fn cancellation_implicit_edge() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-cancellation-implicit-edge";
    seed_project_repo_and_task(&db, task_id, default_states::IN_PROGRESS).await;

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks {
                    before_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Block)],
                    on_exit: vec![hook("dispatch_fix_agent", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
            state(
                default_states::CANCELLED,
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some(default_states::CANCELLED.to_owned()),
    };
    let current_task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            task_id,
            default_states::CANCELLED,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "cancel",
            false,
            Default::default(),
        )
        .await
        .expect("implicit cancellation transition succeeds");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::CANCELLED);
    let results = hook_results(&db, task_id).await;
    assert!(
        results
            .iter()
            .filter(|entry| entry.phase == "before_exit")
            .all(|entry| entry.outcome == "skipped"),
        "implicit cancellation should skip before_exit hooks: {results:?}"
    );
    assert!(
        results
            .iter()
            .any(|entry| entry.action == "dispatch_fix_agent" && entry.phase == "on_exit"),
        "implicit cancellation should still run on_exit hooks"
    );
}

#[tokio::test]
async fn before_work_workspace_reset_required_keeps_typed_recovery_annotation() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind(
            json!({
                "lifecycle_hooks": {
                    "before_work": [{
                        "type": "script",
                        "command": "true",
                        "timeout_seconds": 5,
                        "blocking": true
                    }]
                }
            })
            .to_string(),
        )
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .expect("blocking before-work hook config writes");
    let worktree_path =
        std::path::PathBuf::from(fixture.workspace.embedded_worktree_path_for_backend());
    std::fs::remove_dir_all(&worktree_path).expect("worktree directory removes");
    run_git(fixture._repo_dir.path(), &["worktree", "prune"]);
    run_git(
        fixture._repo_dir.path(),
        &["branch", "-D", &fixture.workspace.branch],
    );

    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .expect("typed workspace blocker settles the transition");

    let result = drain_result(fixture.engine.clone(), result).await;

    assert_eq!(result.task.status, "in_progress");
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let annotation: serde_json::Value = serde_json::from_str(
        task.error_annotation
            .as_deref()
            .expect("workspace reset annotation exists"),
    )
    .expect("workspace reset annotation is JSON");
    assert_eq!(annotation["type"], "workspace_reset_required");
    assert!(annotation["message"]
        .as_str()
        .expect("annotation message is text")
        .contains("workspace reset required"));
    assert!(annotation.get("recovery_actions").is_none());
    assert!(
        WorkspaceRepo::get_by_id(&*fixture.db, &fixture.workspace.id)
            .await
            .expect("workspace reloads")
            .is_some()
    );
    assert!(!worktree_path.exists());
}
