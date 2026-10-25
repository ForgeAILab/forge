use super::*;
use crate::recovery::test_support::{conditions::*, seed_project_repo, seed_task, sqlite_db};
use db::{EnqueueTaskStep, TaskStepRepo};

#[tokio::test]
async fn owner_reset_real_writer_equals_legacy_oracle_and_retains_long_text() {
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    for shape in SHAPES {
        let task = seed_task(&db, project.clone(), "in_progress".into(), None).await;
        let step = claim(&db, &task).await;
        db::task_writer::in_task_step(step, async {
            seed_shape(&db, &task, shape).await;
            let before = read(&db, &task.id).await;
            let placement = placement(&before);
            let message = if *shape == "long_reason" {
                "z".repeat(200_000)
            } else {
                format!("owner reset: {shape}")
            };
            annotate_owner_workspace_reset(&db, &placement, &message)
                .await
                .unwrap();
            let row = read(&db, &task.id).await;
            let expected = reset_columns(&before, &row, &message);
            assert_columns(&row, &expected);
            if *shape == "annotation_hold" {
                assert!(matches!(
                    row.condition,
                    db::TaskCondition::Parked {
                        primary: db::ParkReason::Failure {
                            failure_kind: api_types::FailureKind::WorkspaceResetRequired
                        },
                        ..
                    }
                ));
                assert!(db::material_blocker(&row.condition).requires_intervention);
            }
            let identity = db::ConditionIdentity::Operation {
                epoch: db
                    .task_condition_entry_identity(&row.id, row.version)
                    .await
                    .unwrap()
                    .epoch(),
                operation_id: placement.id,
                generation: placement.generation,
            };
            assert_write_and_next(&db, &row, identity, shape, "reset").await;
            assert_full_text(&db, &task.id, &message, "diagnostic").await;
            clear_own_columns(&db, &row.id, false, shape).await;
            println!(
                "S2A reset {shape}: legacy oracle, two writes, check, dispatcher, clear, text PASS"
            );
        })
        .await;
    }
}

#[tokio::test]
async fn large_root_and_200kb_reason_store_main_encoder_bytes_and_full_diagnostic_text() {
    async fn assert_main_bytes(db: &SqliteDb, task_id: &str, held: bool, reset: bool) {
        let row = TaskRepo::get_by_id(db, task_id, false)
            .await
            .unwrap()
            .unwrap();
        let mut connection = db.pool().acquire().await.unwrap();
        let facts = db::ConditionFacts::load(&mut connection, task_id)
            .await
            .unwrap();
        drop(connection);
        let mut legacy = db::LegacyConditionInput::from(&row);
        legacy.facts = Some(facts);
        // MAIN serializes the legacy mapper directly, with no payload preparation.
        // The oracle never calls this branch's producer or condition setter.
        let mut mapped = db::map_legacy_condition(&legacy);
        if held {
            // MAIN's Hold omits legacy copies and marks its typed statement.
            mapped = mapped.typed();
            let db::TaskCondition::Parked { evidence, .. } = &mut mapped else {
                panic!("the legacy hold must park");
            };
            evidence.stated = true;
        }
        if reset && CONVERTED {
            mapped = ownership_encoding(
                &mapped,
                db::ConditionIdentity::Operation {
                    epoch: db
                        .task_condition_entry_identity(task_id, row.version)
                        .await
                        .unwrap()
                        .epoch(),
                    operation_id: "placement".into(),
                    generation: 1,
                },
            );
        }
        let expected = serde_json::to_string(&mapped).unwrap();
        let stored: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(task_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(stored.as_bytes(), expected.as_bytes());
        assert_eq!(
            row.condition
                .evidence()
                .witnesses
                .iter()
                .filter(|witness| matches!(witness, db::ConditionWitness::Child { .. }))
                .count(),
            400
        );
    }
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    let task = seed_task(&db, project, "in_progress".into(), None).await;
    let at = now_rfc3339();
    let mut tx = db.pool().begin().await.unwrap();
    for i in 0..400 {
        sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,subtask_order,created_at,updated_at) VALUES (?,?,?,'child','todo',?,?,?)")
            .bind(db::new_uuid_v4()).bind(&task.project_id).bind(&task.id).bind(i).bind(&at).bind(&at)
            .execute(&mut *tx).await.unwrap();
    }
    sqlx::query(
        "UPDATE task SET metadata_json='{\"coordination_review_pending\":true}' WHERE id=?",
    )
    .bind(&task.id)
    .execute(&mut *tx)
    .await
    .unwrap();
    db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_main_bytes(&db, &task.id, false, false).await;
    let placement = WorkspacePlacement {
        id: "placement".into(),
        workspace_id: "workspace".into(),
        task_id: task.id.clone(),
        agent_id: None,
        owner_kind: db::PlacementOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: "location".into(),
        execution_daemon_id: None,
        workspace_handle: None,
        generation: 1,
        state: db::PlacementState::Failed,
        selected_by: db::PlacementSelectedBy::Scheduler,
        selection_reason: "{}".into(),
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        version: 1,
        created_at: at.clone(),
        updated_at: at.clone(),
    };

    let id = db::new_uuid_v4();
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
        available_at: at,
    })
    .await
    .unwrap();
    let step = db
        .claim_step(
            "byte-parity-test",
            Some(&task.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let reason = "z".repeat(200_000);
    db::task_writer::in_task_step(
        step.clone(),
        annotate_owner_workspace_reset(&db, &placement, &reason),
    )
    .await
    .unwrap();
    assert_main_bytes(&db, &task.id, false, true).await;
    let texts = db.task_diagnostic_texts(&task.id, None, 100).await.unwrap();
    let text = texts
        .items
        .iter()
        .find(|text| text.source == "diagnostic")
        .unwrap();
    assert_eq!(text.text.as_bytes(), reason.as_bytes());
    assert_eq!(text.byte_length, 200_000);
    assert_eq!(text.occurrence_count, 1);

    let row = TaskRepo::get_by_id(&db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let at = now_rfc3339();
    db::task_writer::in_task_step(
        step,
        TaskRepo::update_recovery_metadata_if_no_running_execution(
            &db,
            &row.id,
            row.version,
            Some(db::ConditionStatement::hold_operator_text(
                "user", &reason, &at,
            )),
            Some(
                serde_json::json!({"kind":"manual_stop", "reason":reason, "created_at":at})
                    .to_string(),
            ),
            None,
            &at,
            None,
            vec![],
            vec![],
            Some(db::ConditionStatement::Hold {
                actor: "user".into(),
                reason: reason.clone(),
                at: at.clone(),
            }),
        ),
    )
    .await
    .unwrap();
    assert_main_bytes(&db, &task.id, true, false).await;
    let texts = db.task_diagnostic_texts(&task.id, None, 100).await.unwrap();
    let text = texts
        .items
        .iter()
        .find(|text| text.source == "hold")
        .unwrap();
    assert_eq!(text.text.as_bytes(), reason.as_bytes());
    assert_eq!(text.byte_length, 200_000);
    assert_eq!(text.occurrence_count, 1);
}
