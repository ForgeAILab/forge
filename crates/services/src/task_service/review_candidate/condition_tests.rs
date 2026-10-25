use super::*;
use crate::recovery::test_support::{conditions::*, seed_project_repo, seed_task, sqlite_db};

#[tokio::test]
async fn missing_candidate_real_writer_equals_legacy_oracle() {
    let db = Arc::new(sqlite_db().await);
    let (project, _) = seed_project_repo(&db).await;
    let service = TaskService::new_for_test(db.clone(), Arc::new(EventBus::new(32)));
    for shape in SHAPES {
        let task = seed_task(&db, project.clone(), "review".into(), None).await;
        let step = claim(&db, &task).await;
        db::task_writer::in_task_step(step, async {
            seed_shape(&db, &task, shape).await;
            let before = read(&db, &task.id).await;
            let result = service.missing_review_candidate(&before).await.unwrap();
            assert!(result.target.is_none());
            let row = read(&db, &task.id).await;
            let expected = candidate_columns(&before, &row);
            assert_columns(&row, &expected);
            let identity = db.task_condition_entry_identity(&row.id, row.version).await.unwrap();
            assert_write_and_next(&db, &row, identity, shape, "candidate").await;
            let message = "nothing to review: no implementation run or branch commits were delivered; send back to add work, or cancel the Task";
            assert_full_text(&db, &task.id, message, "diagnostic").await;
            if *shape == "long_reason" { assert_full_text(&db, &task.id, &"h".repeat(200_000), "hold").await; }
            clear_own_columns(&db, &row.id, true, shape).await;
            println!("S2A candidate {shape}: legacy oracle, two writes, check, dispatcher, clear, text PASS");
        }).await;
    }
}

#[tokio::test]
async fn healthy_service_retry_hold_release_and_board_move_never_park_unreadable() {
    use api_types::TaskAction;
    use db::{
        AgentRepo, AssigneeKind, CreateAgent, CreateTaskRoleAssignment, TaskBoardRepo,
        TaskRoleAssignmentRepo,
    };
    let db = Arc::new(sqlite_db().await);
    let (project, _) = seed_project_repo(&db).await;
    let at = now_rfc3339();
    let daemon_id = new_uuid_v4();
    db::DaemonRepo::upsert_by_machine_id(
        &*db,
        db::UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("condition-{daemon_id}"),
            hostname: "fixture".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            agent_version: None,
            labels_json: "{}".into(),
            status: db::DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".into(),
            created_at: at.clone(),
            updated_at: at.clone(),
        },
    )
    .await
    .unwrap();
    db::DaemonRepo::update_report(
        &*db,
        db::UpdateDaemonReport {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            detected_clis_json: r#"[{"kind":"shell","availability":"authenticated"}]"#.into(),
            labels_json: None,
            status: db::DaemonStatus::Online,
            last_report_at: at.clone(),
            updated_at: at.clone(),
        },
    )
    .await
    .unwrap();
    let agent_id = new_uuid_v4();
    AgentRepo::create(
        &*db,
        CreateAgent {
            id: agent_id.clone(),
            name: "fixture".into(),
            description: None,
            executor_type: "shell".into(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".into(),
            config_json: "{}".into(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".into(),
            created_at: at.clone(),
            updated_at: at.clone(),
        },
    )
    .await
    .unwrap();
    let service = TaskService::new_for_test(db.clone(), Arc::new(EventBus::new(64)));
    let task = seed_task(
        &db,
        project.clone(),
        "in_progress".into(),
        Some(agent_id.clone()),
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            role_name: "coder".into(),
            assignee_type: Some(AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: at.clone(),
            updated_at: at,
        },
    )
    .await
    .unwrap();
    crate::deferred_dispatch::record_dispatch_disposition(
        &db,
        &task,
        "machine_capacity",
        "machine_capacity: waiting for a machine run slot",
    )
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let held = service
        .perform_task_action(
            &task.id,
            TaskAction::Hold {
                reason: Some("wait".into()),
            },
            task.version,
        )
        .await
        .unwrap()
        .task;
    assert_readable(&held);
    let released = service
        .perform_task_action(&task.id, TaskAction::Release { reason: None }, held.version)
        .await
        .unwrap()
        .task;
    assert_readable(&released);
    let retry_task = seed_task(
        &db,
        project.clone(),
        "in_progress".into(),
        Some(agent_id.clone()),
    )
    .await;
    db::task_condition::test_support::set_task_condition(
        &db,
        &retry_task.id,
        db::ConditionStatement::Edit {
            edit: Box::new(db::ConditionOwnerEdit {
                owner: db::ConditionReasonOwner::Diagnostic,
                expected: None,
                replacement: Some(db::ConditionReplacement {
                    identity: db::ConditionIdentity::Entry {
                        epoch: 0,
                        transition_id: None,
                    },
                    condition: db::StatedCondition::Parked {
                        reason: db::ParkReason::Failure {
                            failure_kind: api_types::FailureKind::ExecutorFailed,
                        },
                        resume: db::ConditionContinuation::Reconcile,
                        since: None,
                        presentation: Default::default(),
                    },
                }),
            }),
        },
    )
    .await
    .unwrap();
    let retry_task = TaskRepo::get_by_id(&*db, &retry_task.id, false)
        .await
        .unwrap()
        .unwrap();
    let retried = service
        .perform_task_action(&retry_task.id, TaskAction::retry(), retry_task.version)
        .await
        .unwrap()
        .task;
    assert_readable(&retried);
    let move_task = seed_task(&db, project.clone(), "todo".into(), None).await;
    let moved = service
        .move_task(
            &move_task.id,
            api_types::MoveTaskRequest {
                operation_id: new_uuid_v4(),
                task_version: move_task.version,
                board_revision: TaskBoardRepo::board_revision(&*db, &project).await.unwrap(),
                target_status: "backlog".into(),
                before_id: None,
                after_id: None,
            },
        )
        .await
        .unwrap();
    assert_readable(&moved.task);
}

fn assert_readable(task: &Task) {
    assert!(!task.condition.reasons().any(|r| matches!(r, db::ParkReason::UnknownCondition { source, .. } if source.field == db::LegacyConditionField::ConditionJson)), "{:?}", task.condition);
    assert!(db::task_condition::decode(&serde_json::to_string(&task.condition).unwrap()).is_ok());
}
