use super::*;
use crate::recovery::test_support::{seed_project_repo, seed_task, sqlite_db};
use db::{EnqueueTaskStep, TaskStepRepo};

#[tokio::test]
async fn missing_candidate_real_writer_equals_legacy_oracle() {
    let db = Arc::new(sqlite_db().await);
    let (project, _) = seed_project_repo(&db).await;
    let task = seed_task(&db, project, "review".into(), None).await;
    let service = TaskService::new_for_test(db.clone(), Arc::new(EventBus::new(32)));
    let id = new_uuid_v4();
    db.enqueue_step(&EnqueueTaskStep {
        id: id.clone(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id,
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: now_rfc3339(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step(
            "candidate-condition-test",
            Some(&task.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let result = db::task_writer::in_task_step(step.clone(), async {
        let at = now_rfc3339();
        TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*db, &task.id, task.version,
            Some(db::ConditionStatement::hold_operator_text("user", "owner measurements", &at)),
            Some(serde_json::json!({"kind":"manual_stop", "reason":"owner measurements", "created_at":at}).to_string()),
            None, &at, None, vec![], vec![],
            Some(db::ConditionStatement::Hold { actor: "user".into(), reason: "owner measurements".into(), at: at.clone() }),
        ).await.unwrap();
        let held = TaskRepo::mutate_metadata(&*db, &task.id, None, vec![
            db::TaskMetadataMutation::Set { key: "owner_wait".into(), value: serde_json::json!({"daemon_id":"waiting-daemon", "started_at":at}) },
            db::TaskMetadataMutation::Set { key: "queued_recovery".into(), value: serde_json::json!({"id":"queued-review-command"}) },
        ], &at).await.unwrap();
        service.missing_review_candidate(&held).await.unwrap()
    }).await;
    assert!(result.target.is_none());
    let row = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(row.condition.evidence().stated);
    assert!(!row.condition.evidence().ownership.is_empty());
    let mut connection = db.pool().acquire().await.unwrap();
    let facts = db::ConditionFacts::load(&mut connection, &task.id)
        .await
        .unwrap();
    let mut legacy = db::LegacyConditionInput::from(&row);
    drop(connection);
    legacy.facts = Some(facts);
    let mapped = db::map_legacy_condition(&legacy);
    db::task_writer::in_task_step(step, async {
        let mut tx = db.pool().begin().await.unwrap();
        db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Legacy)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    })
    .await;
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.condition.typed(),
        row.condition.typed(),
        "two-write condition stability"
    );
    assert_eq!(row.condition.typed(), mapped.typed());
    assert_eq!(row.condition.public(), mapped.public());
    assert_eq!(
        db::material_blocker(&row.condition),
        db::material_blocker(&mapped)
    );
    assert_eq!(after.condition.typed(), mapped.typed());
    assert_eq!(after.condition.public(), mapped.public());
    assert_eq!(
        db::material_blocker(&after.condition),
        db::material_blocker(&mapped)
    );
    assert_eq!(
        db::CreateDomainEvent::task_interruption_changed(&row).dedupe_key,
        db::CreateDomainEvent::task_interruption_changed(&after).dedupe_key
    );
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
