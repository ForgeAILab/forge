//! Workflow engine dispatch failures tests.

use super::*;

#[tokio::test]
async fn dispatch_failure_entering_active_state_rolls_task_back_to_initial() {
    // Incident repro: a task without an approved execution baseline was
    // scheduled by the dispatcher, planning was skipped (no planner), and the
    // coder dispatch failed on entering in_progress — yet the task stayed in
    // in_progress with zero executions, looking in-flight forever.
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-failure-rollback";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A coder assignment whose agent is gone makes dispatch_role_agent FAIL
    // (not skip) when the task enters in_progress.
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
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
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "scheduled by task dispatcher",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    assert!(db
        .task_steps(task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "cascade"));
    assert_eq!(
        result.task.status.to_string(),
        default_states::TODO,
        "a failed dispatch must not leave the task in the working state"
    );

    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::TODO);
    let annotation = task
        .error_annotation
        .expect("dispatch failure is annotated on the task");
    let annotation: serde_json::Value =
        serde_json::from_str(&annotation).expect("annotation is json");
    assert_eq!(annotation["type"], "dispatch_failed");
    assert!(
        annotation["message"]
            .as_str()
            .expect("annotation message is a string")
            .contains("invalid coder role assignment"),
        "annotation carries the dispatch error: {annotation}"
    );

    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .expect("transition logs list");
    let enter = logs
        .iter()
        .find(|entry| entry.to_state == default_states::IN_PROGRESS)
        .expect("planning -> in_progress transition is logged");
    let entries: Vec<HookResultEntry> = serde_json::from_str(
        enter
            .hook_results_json
            .as_deref()
            .expect("hook results are written"),
    )
    .expect("hook results deserialize");
    assert!(
        entries.iter().any(|entry| {
            entry.action == "dispatch_role_agent"
                && entry.phase == "on_enter"
                && entry.outcome == "failed"
        }),
        "failed dispatch hook is recorded: {entries:?}"
    );
    let rollback = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::IN_PROGRESS
                && entry.to_state == default_states::TODO
        })
        .expect("in_progress -> todo rollback transition is logged");
    assert!(
        rollback
            .trigger_reason
            .starts_with("dispatch failed entering in_progress"),
        "rollback records the dispatch failure: {}",
        rollback.trigger_reason
    );
    assert_eq!(
        rollback.triggered_by,
        api_types::Actor::system(api_types::SystemComponent::Workflow).display()
    );
}

#[tokio::test]
async fn successful_dispatch_clears_stale_dispatch_failure_annotation() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = "task-dispatch-annotation-clears";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    // A user-held coder role makes dispatch_role_agent return Ok
    // (task.awaiting_human) without needing a task executor.
    assign_user_role(&db, task_id, default_roles::CODER).await;
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(
            json!({
                "type": "dispatch_failed",
                "message": "repository Task is not runnable",
            })
            .to_string(),
        )
        .bind(task_id)
        .execute(db.pool())
        .await
        .expect("stale dispatch failure annotation seeds");
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
            "restart work after baseline approval",
            false,
            Default::default(),
        )
        .await
        .expect("transition succeeds");

    let result = drain_result(engine(db.clone(), Arc::new(EventBus::new(32))), result).await;

    assert_eq!(result.task.status.to_string(), default_states::IN_PROGRESS);
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(task.status.to_string(), default_states::IN_PROGRESS);
    assert_eq!(
        task.error_annotation, None,
        "successful dispatch clears the stale dispatch_failed annotation"
    );
}

#[tokio::test]
async fn dispatch_failure_preserves_other_annotations_and_wakes_only_its_upgrade_owner() {
    let db = sqlite_db().await;
    for kind in [
        "manual_stop",
        "workspace_error",
        "agent_timeout",
        "recovery_required",
        "workspace_reset_required",
        "max_turns_exceeded",
        "before_work_hook_failed",
        "before_work_hook_timeout",
    ] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        let original = json!({"type":kind,"message":"keep"}).to_string();
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(&original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        super::annotate_dispatch_failure(&db, &task_id, "in_progress", "upgrade", None)
            .await
            .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&db, &task_id, false)
                .await
                .unwrap()
                .unwrap()
                .error_annotation
                .as_deref(),
            Some(original.as_str())
        );
    }
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "todo").await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            json!({"owner_wait":{"daemon_id":"owner","started_at":"1970-01-01T00:00:00Z"}})
                .to_string(),
        )
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let error = ServiceError::DaemonUpgradeRequired {
        daemon_id: "owner".into(),
    };
    super::annotate_upgrade_dispatch_refusal(&db, &task_id, "in_progress", &error)
        .await
        .unwrap();
    super::annotate_dispatch_failure(&db, &task_id, "in_progress", &error.to_string(), None)
        .await
        .unwrap();
    let observed = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let metadata: serde_json::Value =
        serde_json::from_str(observed.metadata_json.as_deref().unwrap()).unwrap();
    assert!(metadata.get("owner_wait").is_none());
    super::wake_upgraded_daemon_tasks(&db, &["other-owner".into()])
        .await
        .unwrap();
    assert!(TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_some());
    super::wake_upgraded_daemon_tasks(&db, &["owner".into()])
        .await
        .unwrap();
    assert!(TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_none());
}

#[tokio::test]
async fn dispatch_failure_overwrites_nonblocking_and_malformed_annotations() {
    let db = sqlite_db().await;
    for original in [
        r#"{"type":"old_notice"}"#,
        r#"{"message":"old"}"#,
        "{broken",
    ] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        super::annotate_dispatch_failure(&db, &task_id, "in_progress", "new failure", None)
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let annotation: serde_json::Value =
            serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "dispatch_failed");
        assert_eq!(annotation["message"], "new failure");
    }
}

#[tokio::test]
async fn dispatch_failure_upgrade_metadata_wakes_with_blocking_annotation_and_skips_deleted_tasks()
{
    let db = sqlite_db().await;
    for deleted in [false, true] {
        let task_id = new_uuid_v4();
        seed_project_repo_and_task(&db, &task_id, "todo").await;
        let original = json!({"type":"manual_stop","message":"keep"}).to_string();
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(&original)
            .bind(&task_id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        crate::deferred_dispatch::record_dispatch_disposition(&db, &task, "coder", "upgrade")
            .await
            .unwrap();
        super::annotate_upgrade_dispatch_refusal(
            &db,
            &task_id,
            "in_progress",
            &ServiceError::DaemonUpgradeRequired {
                daemon_id: "owner".into(),
            },
        )
        .await
        .unwrap();
        if deleted {
            sqlx::query("UPDATE task SET deleted_at = ? WHERE id = ?")
                .bind(now_rfc3339())
                .bind(&task_id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        super::wake_upgraded_daemon_tasks(&db, &["owner".into(), "other-owner".into()])
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &task_id, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.error_annotation.as_deref(), Some(original.as_str()));
        let metadata: serde_json::Value = task
            .metadata_json
            .as_deref()
            .map(|raw| serde_json::from_str(raw).unwrap())
            .unwrap_or(json!({}));
        assert_eq!(metadata.get("daemon_upgrade_refusal").is_some(), deleted);
        assert_eq!(metadata.get("dispatch_disposition").is_some(), deleted);
    }
}

#[tokio::test]
async fn dispatch_failure_upgrade_wake_preserves_a_concurrent_manual_deferral() {
    let db = sqlite_db().await;
    let task_id = new_uuid_v4();
    seed_project_repo_and_task(&db, &task_id, "todo").await;
    super::annotate_upgrade_dispatch_refusal(
        &db,
        &task_id,
        "in_progress",
        &ServiceError::DaemonUpgradeRequired {
            daemon_id: "owner".into(),
        },
    )
    .await
    .unwrap();
    let observed = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    // Metadata writes need not bump Task.version. The wake fences those too.
    crate::deferred_dispatch::set(
        &db,
        &observed,
        "in_progress",
        "2099-01-01T00:00:00Z",
        "manual action",
    )
    .await
    .unwrap();
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &observed)
        .await
        .unwrap());
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        crate::deferred_dispatch::pending_until(&current)
            .unwrap()
            .reason,
        "manual action"
    );
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    super::wake_upgraded_daemon_tasks(&db, &["owner".into()])
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(crate::deferred_dispatch::pending_until(&current).is_some());
    crate::deferred_dispatch::clear(&db, &current)
        .await
        .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    let cleared = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    crate::deferred_dispatch::set(
        &db,
        &cleared,
        "in_progress",
        "2099-01-01T00:00:00Z",
        "new manual action",
    )
    .await
    .unwrap();
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!super::clear_upgrade_dispatch_refusal(&db, &current)
        .await
        .unwrap());
    assert!(crate::deferred_dispatch::pending_until(
        &TaskRepo::get_by_id(&db, &task_id, false)
            .await
            .unwrap()
            .unwrap()
    )
    .is_some());
}

/// Failure memory keeps its coverage from typed signals once reason and hook
/// message words stopped counting: a failed hook on an ordinary move, and the
/// dispatch-failure rollback whose only base signal was the word "failed".
#[tokio::test]
async fn failure_memory_records_failed_hook_and_dispatch_rollback_from_typed_evidence() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));

    // A Log-policy hook failure leaves the Task in an ordinary state.
    let task_id = "task-memory-failed-hook";
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
                    on_enter: vec![hook("notify_role_holder", FailurePolicy::Log)],
                    ..StateHooks::default()
                },
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::IN_PROGRESS,
            task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .unwrap();
    let entered = logs
        .iter()
        .find(|entry| entry.to_state == default_states::IN_PROGRESS)
        .unwrap();
    assert!(!entered.rejection);
    assert!(hook_results(&db, task_id)
        .await
        .iter()
        .any(|entry| entry.action == "notify_role_holder" && entry.outcome == "failed"));
    assert_eq!(transition_memory_count(&db, &entered.id).await, 1);

    // The rollback cascade's reason is the only base signal; its typed signal
    // is the DispatchFailed interruption the failed dispatch recorded.
    let task_id = "task-memory-dispatch-rollback";
    seed_project_repo_and_task(&db, task_id, default_states::TODO).await;
    assign_agent_role_without_agent(&db, task_id, default_roles::CODER).await;
    // A benign entry hook on the initial state gives the rollback its own
    // hooks step, which is where failure memory is recorded.
    let mut workflow = default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == default_states::TODO)
        .unwrap()
        .hooks
        .on_enter = vec![hook("satisfy_dependents", FailurePolicy::Log)];
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await
        .unwrap()
        .unwrap();
    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            task_id,
            default_states::PLANNING,
            task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "scheduled by task dispatcher",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
    assert_eq!(settled.task.status, default_states::TODO);
    let logs = TransitionLogRepo::list_by_task(&*db, task_id)
        .await
        .unwrap();
    let rollback = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::IN_PROGRESS
                && entry.to_state == default_states::TODO
        })
        .unwrap();
    assert!(!rollback.rejection);
    let rollback_hooks: Vec<HookResultEntry> =
        serde_json::from_str(rollback.hook_results_json.as_deref().unwrap()).unwrap();
    assert!(!rollback_hooks.is_empty());
    assert!(rollback_hooks.iter().all(|entry| entry.outcome != "failed"));
    assert!(settled
        .task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("dispatch_failed")));
    assert_eq!(transition_memory_count(&db, &rollback.id).await, 1);
}
