// Unit A conversion target: keep this real-writer, two-write legacy oracle active.
use super::*;
use crate::recovery::test_support::{seed_project_repo, seed_task, sqlite_db};
use db::{EnqueueTaskStep, TaskStepRepo};

#[tokio::test]
async fn owner_reset_real_writer_equals_legacy_oracle_and_retains_long_text() {
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    let mut writer_marks = Vec::new();
    for (message, held, blocked_kind) in [
        ("owner reset details".to_owned(), None, None),
        ("z".repeat(200_000), None, None),
        ("late owner reset".to_owned(), Some(true), None),
        ("annotation-only owner reset".to_owned(), Some(false), None),
        ("reset with other owners".to_owned(), None, None),
        (
            "retry exhausted reset".into(),
            None,
            Some("retry_exhausted"),
        ),
        (
            "review exhausted reset".into(),
            None,
            Some("review_budget_exhausted"),
        ),
        (
            "merge exhausted reset".into(),
            None,
            Some("merge_fix_budget_exhausted"),
        ),
        ("gate failed reset".into(), None, Some("review_gate_failed")),
        ("timeout reset".into(), None, Some("agent_timeout")),
        ("loop reset".into(), None, Some("workflow_loop")),
        ("deferred reset".into(), None, Some("deferred")),
    ] {
        let task = seed_task(&db, project.clone(), "in_progress".into(), None).await;
        let at = now_rfc3339();
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
            available_at: at.clone(),
        })
        .await
        .unwrap();
        let step = db
            .claim_step(
                "owner-condition-test",
                Some(&task.id),
                &db::task_writer::lease_deadline(),
            )
            .await
            .unwrap()
            .unwrap();
        db::task_writer::in_task_step(step.clone(), async {
            if let Some(with_interruption) = held {
                TaskRepo::update_recovery_metadata_if_no_running_execution(
                    &db, &task.id, task.version,
                    Some(db::ConditionStatement::hold_operator_text("user", "owner measurements", &at)),
                    with_interruption.then(|| serde_json::json!({"kind":"manual_stop","reason":"owner measurements","created_at":at}).to_string()),
                    None, &at, None, vec![], vec![], with_interruption.then(|| db::ConditionStatement::Hold { actor: "user".into(), reason: "owner measurements".into(), at: at.clone() })
                ).await.unwrap();
            }
            if message == "reset with other owners" {
                TaskRepo::mutate_metadata(&db, &task.id, None, vec![
                    db::TaskMetadataMutation::Set { key: "owner_wait".into(), value: serde_json::json!({"daemon_id":"waiting-daemon", "started_at":at}) },
                    db::TaskMetadataMutation::Set { key: "queued_recovery".into(), value: serde_json::json!({"id":"queued-owner-command"}) },
                ], &at).await.unwrap();
            }
            if let Some(kind) = blocked_kind {
                let mut tx = db.pool().begin().await.unwrap();
                if kind == "deferred" {
                    sqlx::query("UPDATE task SET metadata_json=? WHERE id=?")
                        .bind(serde_json::json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z", "reason":"retry later", "target_state":"in_progress"}}).to_string())
                        .bind(&task.id).execute(&mut *tx).await.unwrap();
                } else {
                    let blocked = if kind == "workflow_loop" {
                        serde_json::json!({"reason":"loop detected", "blocked_at":at, "source":"workflow_loop"})
                    } else {
                        serde_json::json!({"kind":kind, "reason":"interrupted", "created_at":at, "source":"replan"})
                    };
                    sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
                        .bind(blocked.to_string()).bind(&task.id).execute(&mut *tx).await.unwrap();
                }
                db.produce_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Legacy).await.unwrap();
                tx.commit().await.unwrap();
            }
            annotate_owner_workspace_reset(&db, &placement, &message).await.unwrap();
        }).await;
        let row = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();

        writer_marks.push((
            row.condition.evidence().stated,
            row.condition.evidence().ownership.len(),
        ));
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
        let after = TaskRepo::get_by_id(&db, &task.id, false)
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
        crate::task_actions::test_support::assert_legacy_offers(&db, &row, &mapped).await;
        assert_eq!(
            db::material_blocker(&row.condition),
            db::material_blocker(&mapped)
        );
        assert_eq!(after.condition.typed(), mapped.typed());
        assert_eq!(after.condition.public(), mapped.public());
        crate::task_actions::test_support::assert_legacy_offers(&db, &after, &mapped).await;
        assert_eq!(
            db::material_blocker(&after.condition),
            db::material_blocker(&mapped)
        );
        assert_eq!(
            db::CreateDomainEvent::task_interruption_changed(&row).dedupe_key,
            db::CreateDomainEvent::task_interruption_changed(&after).dedupe_key
        );
        if held == Some(false) {
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
        let texts = db
            .task_diagnostic_texts(&task.id, None, 100)
            .await
            .unwrap()
            .items;
        assert!(texts
            .iter()
            .any(|d| d.text == message && d.source == "diagnostic"));
    }
    // The unit-A target currently runs the base writer; no typed ownership is introduced.
    assert!(writer_marks
        .iter()
        .all(|(stated, count)| !stated && *count == 0));
}

#[tokio::test]
async fn large_root_and_200kb_reason_store_main_encoder_bytes_and_full_diagnostic_text() {
    async fn assert_main_bytes(db: &SqliteDb, task_id: &str, held: bool) {
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
    assert_main_bytes(&db, &task.id, false).await;
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
    assert_main_bytes(&db, &task.id, false).await;
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
    assert_main_bytes(&db, &task.id, true).await;
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

#[tokio::test]
async fn typed_reset_matches_archived_main_for_every_shape_and_second_write() {
    use crate::recovery::test_support::conditions::*;
    let mut visited = 0;
    for shape in SHAPES {
        let db = sqlite_db().await;
        let _clock = freeze_clock();
        let projector = attention_projector(&db);
        let (project, _) = seed_project_repo(&db).await;
        let task = seed_task(&db, project, "in_progress".into(), None).await;
        let step = claim(&db, &task).await;
        db::task_writer::in_task_step(step.clone(), async {
            seed_shape(&db, &task, shape).await;
            let before = read(&db, &task.id).await;
            let message = match *shape {
                "long_reason" => "z".repeat(200_000),
                "clear" => ESCAPED_REASON.into(),
                _ => format!("owner reset: {shape}"),
            };
            annotate_owner_workspace_reset(&db, &placement(&before), &message).await.unwrap();
            let row = read(&db, &task.id).await;
            // Independent main writer literal, including alphabetical key order.
            let at = serde_json::from_str::<serde_json::Value>(row.error_annotation.as_ref().unwrap()).unwrap()["blocked_at"].clone();
            let expected = serde_json::json!({"type":"workspace_reset_required", "blocking_reason":"owner_workspace_reset_required", "blocked_by":"system:heartbeat_monitor", "blocked_at":at, "message":message}).to_string();
            assert_eq!(row.error_annotation.as_deref(), Some(expected.as_str()));
            assert_eq!(row.blocked_json, before.blocked_json);
            assert_eq!(row.failed_json, before.failed_json);
            assert_main_snapshot(&db, &projector, &row, "reset", shape, "first").await;
            assert_next_write(&db, &projector, &row, "reset", shape, &step).await;
        }).await;
        visited += 3;
    }
    assert_writer_cases("reset", visited);
}
