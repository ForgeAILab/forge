use super::super::*;
use api_types::MoveTaskRequest;
use db::TaskBoardRepo;

fn move_request(
    task: &Task,
    board_revision: i64,
    target_status: &str,
    before_id: Option<String>,
    after_id: Option<String>,
) -> MoveTaskRequest {
    MoveTaskRequest {
        operation_id: new_uuid_v4(),
        task_version: task.version,
        board_revision,
        target_status: target_status.to_owned(),
        before_id,
        after_id,
    }
}

#[tokio::test]
async fn board_reorder_is_idempotent_and_emits_one_move_event() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let mut events = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let first = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let moved = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let revision = TaskBoardRepo::board_revision(&*db, &project_id)
        .await
        .expect("revision loads");
    let request = move_request(&moved, revision, "todo", None, Some(first.id.clone()));

    let committed = service
        .move_task(moved.id.clone(), request.clone())
        .await
        .expect("same-column move commits");
    assert_eq!(committed.task.version, moved.version + 1);
    assert!(committed.task.board_position < first.board_position);

    let replayed = service
        .move_task(moved.id.clone(), request.clone())
        .await
        .expect("same operation replays");
    assert_eq!(replayed, committed);
    let stored = TaskRepo::get_by_id(&*db, &moved.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(stored.version, committed.task.version);

    let drained = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        drained
            .iter()
            .filter(|event| event.event_type == events::TASK_MOVED_EVENT)
            .count(),
        1
    );
    assert!(drained
        .iter()
        .all(|event| event.event_type != "task.status_changed"));

    let mut conflicting = request;
    conflicting.after_id = None;
    let conflict = service.move_task(moved.id, conflicting).await;
    assert!(matches!(
        conflict,
        Err(ServiceError::Db(DbError::MoveOperationConflict { .. }))
    ));
}

#[tokio::test]
async fn cross_column_move_preserves_workflow_cascade_and_event_contract() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(64));
    let mut events = event_bus.subscribe();
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let revision = TaskBoardRepo::board_revision(&*db, &project_id)
        .await
        .expect("revision loads");

    let result = service
        .move_task(
            task.id.clone(),
            move_request(&task, revision, default_states::PLANNING, None, None),
        )
        .await
        .expect("cross-column move commits and cascades");
    assert_eq!(result.old_status, default_states::TODO);
    assert_eq!(result.task.status, default_states::IN_PROGRESS);
    assert!(result.board_revision > revision);

    let logs = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .expect("transition logs load");
    assert!(logs.iter().any(|log| {
        log.from_state == default_states::TODO && log.to_state == default_states::PLANNING
    }));
    assert!(logs.iter().any(|log| {
        log.from_state == default_states::PLANNING && log.to_state == default_states::IN_PROGRESS
    }));

    let drained = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    let moved_events = drained
        .iter()
        .filter(|event| event.event_type == events::TASK_MOVED_EVENT)
        .collect::<Vec<_>>();
    assert_eq!(moved_events.len(), 1);
    match &moved_events[0].context {
        EventContext::TaskMoved(payload) => {
            assert_eq!(payload.old_status, default_states::TODO);
            assert_eq!(payload.new_status, default_states::PLANNING);
            assert_eq!(payload.task_version, task.version + 1);
        }
        context => panic!("expected task moved context, got {context:?}"),
    }
    assert!(drained.iter().any(|event| {
        event.event_type == "task.status_changed"
            && matches!(
                &event.context,
                EventContext::TaskStatusChanged { old_status, new_status, .. }
                    if old_status == default_states::PLANNING
                        && new_status == default_states::IN_PROGRESS
            )
    }));
}

#[tokio::test]
async fn board_move_conflicts_and_guard_rejection_write_nothing() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(32));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let stale_revision = TaskBoardRepo::board_revision(&*db, &project_id)
        .await
        .expect("revision loads");
    let _other = seed_task_with_status(&db, &project_id, "todo".to_owned()).await;
    let stale = service
        .move_task(
            task.id.clone(),
            move_request(&task, stale_revision, default_states::BACKLOG, None, None),
        )
        .await;
    assert!(matches!(
        stale,
        Err(ServiceError::Db(DbError::BoardRevisionConflict { .. }))
    ));
    let unchanged = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(unchanged.status, task.status);
    assert_eq!(unchanged.version, task.version);

    let mut guarded_todo = workflow_state(
        default_states::TODO,
        StateKind::Initial,
        None,
        StateHooks {
            before_exit: vec![HookSpec {
                action: "require_upstream_roles_completed".to_owned(),
                params: json!({}),
                applies_to: HookAudience::All,
                on_failure: FailurePolicy::Block,
            }],
            ..StateHooks::default()
        },
    );
    guarded_todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: default_states::IN_PROGRESS.to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            guarded_todo,
            workflow_state(
                default_states::PLANNING,
                StateKind::Gate,
                Some(default_roles::PLANNER),
                StateHooks::default(),
            ),
            workflow_state(
                default_states::IN_PROGRESS,
                StateKind::Active,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    update_project_workflow(&db, &project_id, &workflow).await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        db::CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: default_roles::PLANNER.to_owned(),
            assignee_type: None,
            assignee_id: None,
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("incomplete planner role assigns");
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let revision = TaskBoardRepo::board_revision(&*db, &project_id)
        .await
        .expect("revision loads");
    let operation_id = new_uuid_v4();
    let rejected = service
        .move_task(
            task.id.clone(),
            MoveTaskRequest {
                operation_id: operation_id.clone(),
                task_version: current.version,
                board_revision: revision,
                target_status: default_states::IN_PROGRESS.to_owned(),
                before_id: None,
                after_id: None,
            },
        )
        .await;
    assert!(matches!(rejected, Err(ServiceError::GuardRejection { .. })));
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(after.status, current.status);
    assert_eq!(after.version, current.version);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_move_operation WHERE operation_id = ?",
        )
        .bind(operation_id)
        .fetch_one(db.pool())
        .await
        .expect("operation count loads"),
        0
    );
}
