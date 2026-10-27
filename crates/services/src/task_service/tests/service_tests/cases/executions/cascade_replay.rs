//! Execution completion races and pre-upgrade cascade replay.
use super::*;

#[tokio::test]
async fn execution_completion_racing_queued_cascade_transitions_exactly_once() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
    let (project_id, _, _repo) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .retain(|s| matches!(s.name.as_str(), "in_progress" | "done"));
    for state in &mut workflow.states {
        state.hooks = api_types::StateHooks::default();
        state.triggers.clear();
        if state.name == "in_progress" {
            state.triggers.insert(
                api_types::WorkflowTrigger::Accept,
                api_types::WorkflowTriggerDefinition {
                    to: "done".into(),
                    dispatch: None,
                },
            );
        }
    }
    workflow.cancellation_state = None;
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task_with_status(&db, &project_id, "in_progress".into()).await;
    seed_role_assignment(&db, &task.id, "coder", Some(&agent)).await;
    let execution = seed_completed_coder_execution(&db, &task, &agent, None).await;
    let cascade = service
        .workflow_execution()
        .cascade_step_input(
            &task,
            &workflow,
            "done".into(),
            "racing cascade".into(),
            Default::default(),
            false,
            false,
            None,
            None,
            db::new_uuid_v4(),
            None,
        )
        .await
        .unwrap();
    db.enqueue_step(&cascade).await.unwrap();
    let (completion, drained) = tokio::join!(
        service.maybe_cascade_executor_completion(&execution.id),
        service.drain(&task.id)
    );
    completion.unwrap();
    drained.unwrap();
    assert_eq!(service.drain(&task.id).await.unwrap().status, "done");
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transition_log WHERE task_id=? AND to_state='done'",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(db.pending_steps(&task.id).await.unwrap(), 0);
}

#[tokio::test]
async fn busy_claim_still_starts_its_execution_after_the_fast_head_releases() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let (project, _, _repo) = seed_project_repo(&db).await;
    let agent = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project, "todo".into()).await;
    let root = TempDir::new().unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf())
        .with_task_executor(Arc::new(StartedClaimExecutor(started.clone())));
    db.enqueue_task_mutation(
        &task.id,
        db::TaskMutation::TaskSetEntryBarrier {
            id: task.id.clone(),
            expected_version: task.version,
            entry_barrier_json: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let head = db
        .claim_step(
            "fast-owner",
            Some(&task.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let result = service
        .claim_and_start_task(&task.id, Assignee::Agent(agent), None)
        .await;
    assert!(matches!(result, Err(ServiceError::TaskBusy { .. })));
    assert!(ExecutionRepo::list_running_by_task(&*db, &task.id)
        .await
        .unwrap()
        .is_empty());
    db::task_writer::in_task_step(head.clone(), db.execute_task_mutation(&head))
        .await
        .unwrap();
    db.release_step(&head.id, "fast-owner").await.unwrap();
    service.drain(&task.id).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    assert_eq!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A finished execution's completion cascade is queued behind busy Task work
/// when a Hold arrives. The Hold must not lose the completion: once released,
/// the Task ends exactly where it would have ended without the Hold.
#[tokio::test]
async fn hold_before_a_queued_completion_cascade_never_loses_the_completion() {
    use db::TaskStepRepo;
    async fn fixture() -> (Arc<SqliteDb>, TaskService, Task, Execution) {
        let db = Arc::new(sqlite_db().await);
        let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
        let (project_id, _, _repo) = seed_project_repo(&db).await;
        let agent = seed_agent(&db).await;
        let task = seed_task_with_status(&db, &project_id, "in_progress".into()).await;
        seed_role_assignment(&db, &task.id, "coder", Some(&agent)).await;
        let execution = seed_completed_coder_execution(&db, &task, &agent, None).await;
        (db, service, task, execution)
    }

    // Without a Hold, the completion moves the Task on.
    let (_db, service, task, execution) = fixture().await;
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    let expected = service.drain(&task.id).await.unwrap().status;
    assert_ne!(expected, "in_progress", "the completion moves the Task on");

    // The completion is queued (as a busy completion request leaves it), and
    // a Hold arrives before it runs.
    let (db, service, task, execution) = fixture().await;
    service
        .enqueue_task_command(
            &task.id,
            "maybe_cascade_executor_completion",
            serde_json::json!([execution.id]),
            false,
        )
        .await
        .unwrap();
    let hold = service
        .perform_task_action(
            &task.id,
            api_types::TaskAction::Hold { reason: None },
            task.version,
        )
        .await;
    assert!(
        matches!(
            hold,
            Ok(_)
                | Err(ServiceError::TaskActionUnavailable { .. })
                | Err(ServiceError::Db(db::DbError::TaskVersionConflict { .. }))
        ),
        "{hold:?}"
    );
    let held = service.drain(&task.id).await.unwrap();
    let release = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
        .available_actions
        .into_iter()
        .find(|offer| offer.action.verb() == "release");
    if let Some(release) = release {
        service
            .perform_task_action(&task.id, release.action, held.version)
            .await
            .unwrap();
    }
    let after = service.drain(&task.id).await.unwrap();
    assert_eq!(
        after.status, expected,
        "a refused Hold before the queued completion stranded the Task (hold={hold:?})"
    );
    assert_eq!(db.pending_steps(&task.id).await.unwrap(), 0);

    // An accepted Hold: entry hooks are still queued, so the owner's Hold
    // applies; the queued completion is then released with the Task.
    let (db, service, task, execution) = fixture().await;
    db.enqueue_step(&db::EnqueueTaskStep {
        id: new_uuid_v4(),
        task_id: task.id.clone(),
        kind: "hooks".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: "entry-hooks".into(),
        chain_id: "entry-hooks".into(),
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: now_rfc3339(),
    })
    .await
    .unwrap();
    service
        .enqueue_task_command(
            &task.id,
            "maybe_cascade_executor_completion",
            serde_json::json!([execution.id]),
            false,
        )
        .await
        .unwrap();
    let held = service
        .perform_task_action(
            &task.id,
            api_types::TaskAction::Hold { reason: None },
            task.version,
        )
        .await
        .expect("Hold applies while entry hooks are queued");
    let held = service.drain(&held.task.id).await.unwrap();
    if let Some(release) = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
        .available_actions
        .into_iter()
        .find(|offer| offer.action.verb() == "release")
    {
        service
            .perform_task_action(&task.id, release.action, held.version)
            .await
            .unwrap();
    }
    let after = service.drain(&task.id).await.unwrap();
    assert_eq!(
        after.status, expected,
        "an accepted Hold before the queued completion stranded the Task"
    );
    assert_eq!(db.pending_steps(&task.id).await.unwrap(), 0);
}

#[tokio::test]
async fn pre_upgrade_conflict_handoff_cascade_executes_typed() {
    pre_upgrade_cascade(
        "[conflict-handoff] rebased onto main; conflicts were committed with markers in: a.rs, b c.rs; paths_json=[\"a.rs\",\"b c.rs\"]",
        Some(api_types::TransitionBridgeKind::ConflictHandoff),
        Some(serde_json::json!({"paths":["a.rs","b c.rs"]})),
    )
    .await;
}

#[tokio::test]
async fn pre_upgrade_target_moved_cascade_executes_typed() {
    pre_upgrade_cascade(
        "[review-refresh] [target-moved-rebase] main advanced; rebased onto main, re-review required",
        Some(api_types::TransitionBridgeKind::TargetMovedRebase),
        None,
    )
    .await;
}

#[tokio::test]
async fn pre_upgrade_conformance_refresh_cascade_executes_typed() {
    pre_upgrade_cascade(
        "[review-refresh] conformance review required: scope drift",
        Some(api_types::TransitionBridgeKind::ReviewRefresh),
        None,
    )
    .await;
}

#[tokio::test]
async fn pre_upgrade_hooks_step_replays_with_backfilled_bridge() {
    let refresh = pre_upgrade_hooks_step_replays(None).await;
    // The dispatch hook ran live on the backfilled committed row.
    assert_eq!(
        refresh.trigger_reason,
        "Mechanical merge contention resolved; fresh review required"
    );
}

#[tokio::test]
async fn pre_upgrade_hook_checkpoint_replays_with_backfilled_bridge() {
    let saved = "[review-refresh] mechanical merge contention resolved; fresh review required";
    let refresh = pre_upgrade_hooks_step_replays(Some(saved)).await;
    // The saved result replayed instead of re-running the hook.
    assert_eq!(refresh.trigger_reason, saved);
}
