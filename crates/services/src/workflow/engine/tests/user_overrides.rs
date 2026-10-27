//! Workflow engine user overrides tests.

use super::*;

#[tokio::test]
async fn user_override_succeeds_across_missing_edge() {
    // Delta: User moves a task across a missing edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
            Default::default(),
        )
        .await
        .expect("user override across missing edge succeeds");

    assert_eq!(result.task.status, "done");
}

#[tokio::test]
async fn user_override_does_not_reopen_terminal_state() {
    // Terminal reopen must not use user routing override (missing edge from terminal -> non-terminal).
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "done", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override",
            false,
            Default::default(),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ServiceError::Db(db::DbError::InvalidTransition))
        ),
        "user override must not reopen a terminal state"
    );
}

#[tokio::test]
async fn user_override_succeeds_along_system_only_edge() {
    // Delta: User moves a task along a system-only edge
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = system_only_fail_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let user_result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "failed",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "user override along fail edge",
            false,
            Default::default(),
        )
        .await
        .expect("user override along system-only edge succeeds");
    assert_eq!(user_result.task.status, "failed");

    let task_id_system = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_system, "working", &workflow).await;
    let system_result = eng
        .workflow_execution()
        .transition(
            &task_id_system,
            "failed",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "system fail transition",
            false,
            Default::default(),
        )
        .await
        .expect("system actor may use system-only edge");
    assert_eq!(system_result.task.status, "failed");

    let task_id_agent = new_uuid_v4();
    seed_custom_workflow_task(&db, &task_id_agent, "working", &workflow).await;
    let agent_result = eng
        .workflow_execution()
        .transition(
            &task_id_agent,
            "failed",
            1,
            &workflow,
            &api_types::Actor::agent("unit"),
            "agent attempt",
            false,
            Default::default(),
        )
        .await;
    match agent_result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("system-only"),
                "expected system-only rejection, got: {message}"
            );
        }
        Ok(_) => panic!("expected system-only rejection for agent, got Ok"),
        Err(other) => panic!("expected system-only rejection for agent, got {other:?}"),
    }
}

#[tokio::test]
async fn agent_can_use_a_system_trigger_only_for_the_cancellation_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut workflow = system_only_fail_edge_workflow();
    workflow.cancellation_state = Some("failed".to_owned());
    workflow.states[1].kind = StateKind::Terminal;
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "failed",
            1,
            &workflow,
            &api_types::Actor::agent("project-agent"),
            "cancelled through the scoped Task command",
            false,
            Default::default(),
        )
        .await
        .expect("an authorized Agent cancellation can reach the cancellation target");

    assert_eq!(result.task.status, "failed");
    let triggered_by: String = sqlx::query_scalar(
        "SELECT triggered_by FROM transition_log WHERE task_id = ? ORDER BY rowid DESC LIMIT 1",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("transition audit row");
    assert_eq!(triggered_by, "agent:project-agent");
}

#[tokio::test]
async fn override_not_granted_to_agents_or_system() {
    // Delta: Override authority is not granted to agents or system
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let workflow = missing_edge_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    for (actor, label) in [
        (
            api_types::Actor::system(api_types::SystemComponent::General),
            "system",
        ),
        (api_types::Actor::agent("unit"), "agent"),
    ] {
        let task_id = new_uuid_v4();
        seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
        let result = eng
            .workflow_execution()
            .transition(
                &task_id,
                "done",
                1,
                &workflow,
                &actor,
                "should not override",
                false,
                Default::default(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(ServiceError::Db(db::DbError::InvalidTransition))
            ),
            "{label} actor should be rejected on missing edge without override"
        );
    }
}

#[tokio::test]
async fn override_to_undefined_target_rejected_with_enumerated_states() {
    // Delta: Target state not in workflow
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "nonexistent",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "invalid target",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            assert!(
                message.contains("working"),
                "expected defined state 'working' in message, got: {message}"
            );
            assert!(
                message.contains("done"),
                "expected defined state 'done' in message, got: {message}"
            );
        }
        Ok(_) => panic!("expected undefined target rejection, got Ok"),
        Err(other) => panic!("expected undefined target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn custom_workflow_lacking_review_rejects_with_enumerated_states() {
    // Delta: Custom project workflow lacks the requested target
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = workflow_without_review_state();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::InvalidOperation { message }) => {
            assert!(
                message.contains("not defined in workflow"),
                "expected undefined-state message, got: {message}"
            );
            for state_name in ["pending", "working", "shipped"] {
                assert!(
                    message.contains(state_name),
                    "expected defined state '{state_name}' in message, got: {message}"
                );
            }
            assert!(
                !message
                    .split("defined states are: ")
                    .nth(1)
                    .unwrap_or("")
                    .contains("review"),
                "review must not appear among defined states, got: {message}"
            );
        }
        Ok(_) => panic!("expected review target rejection, got Ok"),
        Err(other) => panic!("expected review target rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn content_guard_blocks_user_override() {
    // Delta: A content guard may still block a user override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state(
                    "working",
                    StateKind::Active,
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
                "paused",
            ),
            state("paused", StateKind::Custom, None, StateHooks::default()),
            with_trigger(
                state(
                    default_states::PLANNING,
                    StateKind::Gate,
                    Some(default_roles::PLANNER),
                    StateHooks::default(),
                ),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    assign_agent_role_without_agent(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "override blocked by guard",
            false,
            Default::default(),
        )
        .await;

    match result {
        Err(ServiceError::GuardRejection { guard, reason: _ }) => {
            assert_eq!(guard, "require_upstream_roles_completed");
        }
        Ok(_) => panic!("expected guard rejection, got Ok"),
        Err(other) => panic!("expected guard rejection, got {other:?}"),
    }

    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status, "working");
}

#[tokio::test]
async fn version_conflict_still_applies_to_override() {
    // Delta: Version conflict still applies to override
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "done",
            2,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "stale version",
            false,
            Default::default(),
        )
        .await;

    assert!(
        matches!(result, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "expected version conflict"
    );
}

#[tokio::test]
async fn override_is_auditable() {
    // Delta: Override is auditable
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = missing_edge_workflow();
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    let reason = "audit this override";
    let eng = engine(Arc::clone(&db), event_bus);

    eng.workflow_execution()
        .transition(
            &task_id,
            "done",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            reason,
            false,
            Default::default(),
        )
        .await
        .expect("override transition succeeds");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let latest = logs
        .iter()
        .find(|entry| entry.to_state == "done" && !entry.rejection)
        .expect("successful override transition log exists");
    assert_eq!(latest.triggered_by, "user:override:test");
    assert_eq!(latest.trigger_reason, reason);
}

#[tokio::test]
async fn subtask_user_override_into_review_no_workspace_no_reviewer_completes() {
    // Task 6.1: no workspace, no reviewer — subtask user override into review must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into review",
            false,
            Default::default(),
        )
        .await
        .expect("subtask user override into review completes without panic");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_review_with_ci_steps_completes_or_fails_gracefully() {
    // Task 6.1: CI step configured — must not panic; may fail with ServiceError only
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        Some(r#"{"review":{"ci_steps":["test -d ."]}}"#.to_owned()),
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review with ci",
            false,
            Default::default(),
        )
        .await;

    match result {
        Ok(transition) => assert_eq!(transition.task.status, default_states::REVIEW),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "CI-configured subtask review must fail via ServiceError, not panic: {error:?}"
        ),
    }
}

#[tokio::test]
async fn subtask_user_override_into_review_empty_ci_auto_passes() {
    // Task 6.1: empty CI auto-passes — system unconfigured review skips; user stays in review
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask review no ci",
            false,
            Default::default(),
        )
        .await
        .expect("subtask user override into review with empty CI completes");

    assert_eq!(result.task.status, default_states::REVIEW);
}

#[tokio::test]
async fn subtask_user_override_into_merging_without_merge_service_completes() {
    // Task 6.1: merge service absent — subtask user override into merging must not panic
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let parent_id = new_uuid_v4();
    let subtask_id = new_uuid_v4();
    seed_parent_and_subtask(
        &db,
        &parent_id,
        &subtask_id,
        default_states::IN_PROGRESS,
        None,
    )
    .await;
    let workflow = default_workflow::default_workflow();
    let eng = engine(Arc::clone(&db), event_bus);

    let result = eng
        .workflow_execution()
        .transition(
            &subtask_id,
            default_states::MERGING,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "subtask into merging",
            false,
            Default::default(),
        )
        .await;

    match result {
        Ok(transition) => assert!(
            transition.task.status == default_states::MERGING
                || transition.task.status == default_states::DONE,
            "merge hook may cascade when merge service is absent, got {}",
            transition.task.status
        ),
        Err(error) => assert!(
            matches!(
                error,
                ServiceError::GuardRejection { .. }
                    | ServiceError::InvalidOperation { .. }
                    | ServiceError::Db(_)
            ),
            "subtask merge override must fail via ServiceError, not panic: {error:?}"
        ),
    }
}
