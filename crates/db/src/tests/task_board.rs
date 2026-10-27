//! Task board repository tests.
use super::*;

#[tokio::test]
async fn task_board_revision_migration_preserves_tasks_and_tracks_board_changes() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    sqlx::raw_sql(
        r#"
        CREATE TABLE project (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE task (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            status TEXT NOT NULL,
            board_position REAL NOT NULL,
            deleted_at TEXT,
            archived_at TEXT
        );
        INSERT INTO project (id, name) VALUES ('project-1', 'Forge');
        INSERT INTO task (id, project_id, status, board_position)
        VALUES ('task-1', 'project-1', 'todo', 1.0);
        "#,
    )
    .execute(&pool)
    .await
    .expect("pre-v57 schema seeds");

    sqlx::raw_sql(include_str!(
        "../../migrations/V057__task_board_revision.sql"
    ))
    .execute(&pool)
    .await
    .expect("v57 applies");

    let preserved = sqlx::query_as::<_, (String, String, f64)>(
        "SELECT id, status, board_position FROM task WHERE id = 'task-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing task remains");
    assert_eq!(preserved, ("task-1".to_owned(), "todo".to_owned(), 1.0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = 'project-1'",)
            .fetch_one(&pool)
            .await
            .expect("revision loads"),
        0
    );

    sqlx::query("UPDATE task SET status = 'review' WHERE id = 'task-1'")
        .execute(&pool)
        .await
        .expect("status updates");
    sqlx::query(
        "INSERT INTO task (id, project_id, status, board_position) VALUES ('task-2', 'project-1', 'todo', 2.0)",
    )
    .execute(&pool)
    .await
    .expect("task inserts");
    sqlx::query("UPDATE task SET archived_at = '2026-07-22T00:00:00Z' WHERE id = 'task-2'")
        .execute(&pool)
        .await
        .expect("task archives");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = 'project-1'",)
            .fetch_one(&pool)
            .await
            .expect("revision loads"),
        3
    );
}

#[tokio::test]
async fn compare_and_move_is_atomic_versioned_and_idempotent() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let first_id = seed_task(&db, &project_id, None, "todo".to_owned(), "first").await;
    let moved_id = seed_task(&db, &project_id, None, "todo".to_owned(), "moved").await;
    let moved = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let operation_id = new_uuid_v4();
    let board_revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let input = CompareAndMoveTask {
        post_commit_step: None,
        integration_cancel: false,
        operation_id: operation_id.clone(),
        project_id: project_id.clone(),
        task_id: moved_id.clone(),
        task_version: moved.version,
        board_revision,
        target_status: "todo".to_owned(),
        target_column_statuses: vec!["todo".to_owned()],
        before_id: None,
        after_id: Some(first_id),
        entry_barrier_json: None,
        transition_log_id: new_uuid_v4(),
        workflow_snapshot: serde_json::Value::Null,
        trigger_name: None,
        triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
        bridge: Default::default(),
        trigger_reason: "board reorder".to_owned(),
        rejection: false,
        expected_project_version: None,
        expected_workflow_definition: None,
        updated_at: now_rfc3339(),
    };

    let committed = TaskBoardRepo::compare_and_move_task(&db, input.clone())
        .await
        .expect("move commits");
    let result = match committed {
        MoveTaskPersistence::Committed { result, .. } => result,
        MoveTaskPersistence::Replayed(_) => panic!("first move must commit"),
    };
    assert_eq!(result.task.version, moved.version + 1);
    assert!(result.task.board_position < moved.board_position);
    assert!(result.board_revision > board_revision);
    TaskBoardRepo::complete_move_operation(&db, &operation_id, &result, &now_rfc3339())
        .await
        .expect("operation completes");

    let replayed = TaskBoardRepo::replay_move_task(&db, &operation_id, &input.identity())
        .await
        .expect("replay loads")
        .expect("completed result exists");
    assert_eq!(replayed, *result);
    let loaded = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(loaded.version, result.task.version);

    let conflicting_identity = MoveTaskIdentity {
        before_id: Some(new_uuid_v4()),
        ..input.identity()
    };
    assert!(matches!(
        TaskBoardRepo::replay_move_task(&db, &operation_id, &conflicting_identity).await,
        Err(DbError::MoveOperationConflict { .. })
    ));

    let stale_task = CompareAndMoveTask {
        post_commit_step: None,
        integration_cancel: false,
        operation_id: new_uuid_v4(),
        task_version: moved.version,
        board_revision: result.board_revision,
        before_id: None,
        after_id: None,
        ..input.clone()
    };
    assert!(matches!(
        TaskBoardRepo::compare_and_move_task(&db, stale_task).await,
        Err(DbError::TaskVersionConflict { .. })
    ));

    let stale_board = CompareAndMoveTask {
        post_commit_step: None,
        integration_cancel: false,
        operation_id: new_uuid_v4(),
        task_version: loaded.version,
        board_revision,
        before_id: None,
        after_id: None,
        ..input
    };
    assert!(matches!(
        TaskBoardRepo::compare_and_move_task(&db, stale_board).await,
        Err(DbError::BoardRevisionConflict { .. })
    ));
}

#[tokio::test]
async fn compare_and_move_rejects_stale_project_workflow_authority_without_mutation() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "stale workflow move",
    )
    .await;
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let task_before = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let board_revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("board revision loads");
    let input = CompareAndMoveTask {
        post_commit_step: None,
        integration_cancel: false,
        operation_id: new_uuid_v4(),
        project_id: project_id.clone(),
        task_id: task_id.clone(),
        task_version: task_before.version,
        board_revision,
        target_status: "todo".to_owned(),
        target_column_statuses: vec!["todo".to_owned()],
        before_id: None,
        after_id: None,
        entry_barrier_json: None,
        transition_log_id: new_uuid_v4(),
        workflow_snapshot: serde_json::Value::Null,
        trigger_name: None,
        triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
        bridge: Default::default(),
        trigger_reason: "stale workflow authority".to_owned(),
        rejection: false,
        expected_project_version: Some(project.version),
        expected_workflow_definition: Some(project.workflow_definition.clone()),
        updated_at: now_rfc3339(),
    };

    ProjectRepo::update_workflow(
        &db,
        &project_id,
        "{\"workflow\":\"w2\"}",
        None,
        project.version,
        &now_rfc3339(),
    )
    .await
    .expect("workflow update wins the race");

    assert!(matches!(
        TaskBoardRepo::compare_and_move_task(&db, input).await,
        Err(DbError::VersionConflict)
    ));
    let task_after = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.version, task_before.version);
    assert_eq!(task_after.board_position, task_before.board_position);
    assert_eq!(
        TaskBoardRepo::board_revision(&db, &project_id)
            .await
            .expect("board revision reloads"),
        board_revision
    );
}

#[tokio::test]
async fn compare_and_move_emits_interruption_resolution_with_the_task_update() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "todo".to_owned(), "blocked move").await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let blocked = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: Some(Some(r#"{"reason":"needs a decision"}"#.to_owned())),
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("task becomes blocked");
    let board_revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("board revision loads");

    let committed = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            post_commit_step: None,
            integration_cancel: false,
            operation_id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: task_id.clone(),
            task_version: blocked.version,
            board_revision,
            target_status: "todo".to_owned(),
            target_column_statuses: vec!["todo".to_owned()],
            before_id: None,
            after_id: None,
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            workflow_snapshot: serde_json::Value::Null,
            trigger_name: None,
            triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
            bridge: Default::default(),
            trigger_reason: "resolve the block by moving the task".to_owned(),
            rejection: false,
            expected_project_version: None,
            expected_workflow_definition: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("move commits");
    let moved = match committed {
        MoveTaskPersistence::Committed { result, .. } => result.task,
        MoveTaskPersistence::Replayed(_) => panic!("first move must commit"),
    };
    assert!(moved.blocked_json.is_none());
    assert!(
        !crate::material_blocker(&moved.condition).requires_intervention,
        "the returned Task states the condition the move produced"
    );

    let events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("events list");
    let interruption_payloads = events
        .iter()
        .filter(|event| {
            event.event_type == "task.interruption_changed" && event.entity_id == task_id
        })
        .map(|event| {
            serde_json::from_str::<serde_json::Value>(&event.payload_json)
                .expect("interruption payload parses")
        })
        .collect::<Vec<_>>();
    assert_eq!(interruption_payloads.len(), 2);
    assert_eq!(
        interruption_payloads[0]["material_blocker"]["requires_intervention"],
        true
    );
    assert_eq!(
        interruption_payloads[1]["material_blocker"]["requires_intervention"],
        false
    );
    assert_eq!(interruption_payloads[1]["task_version"], moved.version);
}

#[tokio::test]
async fn compare_and_move_validates_empty_columns_neighbors_and_renormalizes() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _) = seed_project_repo_agent(&db).await;
    let before_id = seed_task(&db, &project_id, None, "todo".to_owned(), "before").await;
    let after_id = seed_task(&db, &project_id, None, "todo".to_owned(), "after").await;
    let moved_id = seed_task(&db, &project_id, None, "todo".to_owned(), "moved").await;
    sqlx::query("UPDATE task SET board_position = 1.0 WHERE id = ?")
        .bind(&before_id)
        .execute(db.pool())
        .await
        .expect("before position sets");
    sqlx::query("UPDATE task SET board_position = 1.000000000001 WHERE id = ?")
        .bind(&after_id)
        .execute(db.pool())
        .await
        .expect("after position sets");
    sqlx::query("UPDATE task SET board_position = 3.0 WHERE id = ?")
        .bind(&moved_id)
        .execute(db.pool())
        .await
        .expect("moved position sets");
    let moved = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let renormalized = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            post_commit_step: None,
            integration_cancel: false,
            operation_id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: moved_id.clone(),
            task_version: moved.version,
            board_revision: revision,
            target_status: "todo".to_owned(),
            target_column_statuses: vec!["todo".to_owned()],
            before_id: Some(before_id.clone()),
            after_id: Some(after_id.clone()),
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            workflow_snapshot: serde_json::Value::Null,
            trigger_name: None,
            triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
            bridge: Default::default(),
            trigger_reason: "board reorder".to_owned(),
            rejection: false,
            expected_project_version: None,
            expected_workflow_definition: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("tight gap move commits");
    let result = match renormalized {
        MoveTaskPersistence::Committed { result, .. } => result,
        MoveTaskPersistence::Replayed(_) => panic!("move must commit"),
    };
    assert_eq!(result.task.board_position, 1.5);
    assert!(result.board_revision > revision + 1);

    let source_id = seed_task(&db, &project_id, None, "todo".to_owned(), "source").await;
    let source = TaskRepo::get_by_id(&db, &source_id, false)
        .await
        .expect("source loads")
        .expect("source exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let empty_move = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            post_commit_step: None,
            integration_cancel: false,
            operation_id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: source_id,
            task_version: source.version,
            board_revision: revision,
            target_status: "review".to_owned(),
            target_column_statuses: vec!["review".to_owned()],
            before_id: None,
            after_id: None,
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            workflow_snapshot: serde_json::Value::Null,
            trigger_name: None,
            triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
            bridge: Default::default(),
            trigger_reason: "board move".to_owned(),
            rejection: false,
            expected_project_version: None,
            expected_workflow_definition: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("empty destination accepts null neighbors");
    assert!(matches!(empty_move, MoveTaskPersistence::Committed { .. }));

    let another_id = seed_task(&db, &project_id, None, "todo".to_owned(), "another").await;
    let another = TaskRepo::get_by_id(&db, &another_id, false)
        .await
        .expect("another loads")
        .expect("another exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let nonempty = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            post_commit_step: None,
            integration_cancel: false,
            operation_id: new_uuid_v4(),
            project_id,
            task_id: another_id,
            task_version: another.version,
            board_revision: revision,
            target_status: "review".to_owned(),
            target_column_statuses: vec!["review".to_owned()],
            before_id: None,
            after_id: None,
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            workflow_snapshot: serde_json::Value::Null,
            trigger_name: None,
            triggered_by: api_types::Actor::user(api_types::UserActionSource::BoardDrag),
            bridge: Default::default(),
            trigger_reason: "board move".to_owned(),
            rejection: false,
            expected_project_version: None,
            expected_workflow_definition: None,
            updated_at: now_rfc3339(),
        },
    )
    .await;
    assert!(matches!(nonempty, Err(DbError::InvalidTaskMove(_))));
}
