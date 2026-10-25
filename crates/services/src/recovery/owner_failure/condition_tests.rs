use super::*;
use crate::recovery::test_support::{seed_project_repo, seed_task, sqlite_db};
use db::{EnqueueTaskStep, TaskStepRepo};

#[tokio::test]
async fn owner_reset_real_writer_equals_legacy_oracle_and_retains_long_text() {
    let db = sqlite_db().await;
    let (project, _) = seed_project_repo(&db).await;
    for (message, held) in [
        ("owner reset details".to_owned(), None),
        ("z".repeat(200_000), None),
        ("late owner reset".to_owned(), Some(true)),
        ("annotation-only owner reset".to_owned(), Some(false)),
        ("reset with other owners".to_owned(), None),
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
            annotate_owner_workspace_reset(&db, &placement, &message).await.unwrap();
        }).await;
        let row = TaskRepo::get_by_id(&db, &task.id, false)
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
        let encoded: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert!(encoded.len() <= db::CONDITION_JSON_LIMIT);
        let texts = db
            .task_diagnostic_texts(&task.id, None, 100)
            .await
            .unwrap()
            .items;
        assert!(texts
            .iter()
            .any(|d| d.text == message && d.source == "diagnostic"));
    }
}
