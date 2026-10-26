use super::tests::{claim, db, task};
use super::*;
use crate::{Conditioned, ProjectRepo, TaskMutation, TaskRepo};
use serde_json::json;

const AT: &str = "2026-10-25T07:00:00Z";

#[tokio::test]
async fn diagnostic_dedup_cap_rollback_and_project_cascade() {
    let db = db().await;
    task(&db, "text").await;
    let owner = DiagnosticOwner::Reason {
        reason: ParkReason::Failure {
            failure_kind: api_types::FailureKind::ExecutorFailed,
        },
    };
    let original = "é".repeat(DIAGNOSTIC_TEXT_LIMIT);
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let id = db
        .record_task_diagnostic_text_in_tx(
            &mut tx,
            "text",
            RecordTaskDiagnosticText {
                kind: "failure",
                source: "executor",
                owner: &owner,
                text: &original,
            },
        )
        .await
        .unwrap();
    let again = db
        .record_task_diagnostic_text_in_tx(
            &mut tx,
            "text",
            RecordTaskDiagnosticText {
                kind: "failure",
                source: "executor",
                owner: &owner,
                text: &original,
            },
        )
        .await
        .unwrap();
    assert_eq!(id, again);
    tx.commit().await.unwrap();
    let row = db.task_diagnostic_text("text", &id).await.unwrap().unwrap();
    assert_eq!(row.byte_length, original.len() as i64);
    assert_eq!(row.digest, diagnostics::digest(original.as_bytes()));
    assert_eq!(row.occurrence_count, 2);
    assert!(row.text.len() <= DIAGNOSTIC_TEXT_LIMIT);
    assert!(row.text.ends_with(PRESENTATION_TRUNCATION_MARKER));
    assert_eq!(row.owner, owner);
    assert!(row.last_seen >= row.created_at);
    assert!(db
        .task_diagnostic_text("other", &id)
        .await
        .unwrap()
        .is_none());
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.record_task_diagnostic_text_in_tx(
        &mut tx,
        "text",
        RecordTaskDiagnosticText {
            kind: "failure",
            source: "executor",
            owner: &owner,
            text: "rolled back",
        },
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        db.task_diagnostic_texts("text", None, 100)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    ProjectRepo::delete(&db, "p").await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_text")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn real_hold_retains_200kb_text() {
    let db = db().await;
    task(&db, "long").await;
    let text = "é".repeat(100_000);
    super::statement_tests::hold(&db, "long", &text).await;
    let condition = db.task_condition("long").await.unwrap();
    assert!(condition.read().interruption.unwrap().reason.len() <= PRESENTATION_TEXT_LIMIT);
    let texts = db.task_diagnostic_texts("long", None, 100).await.unwrap();
    let detail = texts
        .items
        .iter()
        .find(|text| text.source == "hold")
        .unwrap();
    assert_eq!(detail.text, text);
    assert_eq!(detail.byte_length, 200_000);
    assert_eq!(detail.source, "hold");
}

#[tokio::test]
async fn deployed_sql_and_update_payloads_still_decode_and_replay() {
    // Verbatim field shapes from 1991b306: task_mutation.rs and repository.rs.
    const SQL: &str = r#"{"Sql":{"task_id":"historic","query":"UPDATE task SET blocked_json=?, updated_at=? WHERE id=?","arguments":["{\"kind\":\"executor_failed\",\"reason\":\"historic full reason\",\"created_at\":\"2026-10-01T00:00:00Z\"}","2026-10-01T00:00:00Z","historic"]}}"#;
    const UPDATE: &str = r#"{"TaskUpdate":{"input":{"id":"historic","expected_version":1,"title":"accepted before upgrade","description":{"present":false,"value":null},"priority":null,"merge_config":{"present":false,"value":null},"plan":{"present":false,"value":null},"error_annotation":{"present":false,"value":null},"blocked_json":{"present":true,"value":null},"failed_json":{"present":false,"value":null},"task_state_config":{"present":false,"value":null},"parent_task_id":{"present":false,"value":null},"updated_at":"2026-10-01T00:00:00Z"}}}"#;
    let db = db().await;
    task(&db, "historic").await;
    let step = claim(&db, "historic").await;
    crate::task_writer::in_task_step(step, async {
        let sql: TaskMutation = serde_json::from_str(SQL).unwrap();
        assert_eq!(sql.apply(&db).await.unwrap(), json!(1));
        let row = TaskRepo::get_by_id(&db, "historic", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.condition.read().blocked_message.as_deref(),
            Some("historic full reason")
        );
        let update: TaskMutation = serde_json::from_str(UPDATE).unwrap();
        let result: Task = serde_json::from_value(update.apply(&db).await.unwrap()).unwrap();
        assert_eq!(result.title, "accepted before upgrade");
        assert!(result.blocked_json.is_none());
        assert!(matches!(result.condition, TaskCondition::Clear { .. }));
        // TaskArchive receipts and queued rollback commands from the same deployed build.
        const ARCHIVE: &str = r#"{"id":"historic","project_id":"p","parent_task_id":null,"assignee_type":null,"assignee_id":null,"title":"old result","description":null,"task_type":"task","status":"todo","is_automation":false,"priority":0,"board_position":0.0,"subtask_order":null,"task_state_config":null,"merge_config":null,"metadata_json":null,"plan":null,"error_annotation":null,"blocked_json":"{\"kind\":\"workflow_loop\",\"reason\":\"old rollback block\",\"created_at\":\"2026-10-01T00:00:00Z\"}","failed_json":null,"entry_barrier_json":null,"review_passed_at":null,"archived_at":null,"deleted_at":null,"version":1,"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-01T00:00:00Z"}"#;
        const RESTORE: &str = r#"{"TaskRestoreQueuedRecovery":{"input":{"task_id":"historic","expected_version":1,"queued_recovery_id":"restore-history","failed_json":null,"error_annotation":null,"blocked_json":"{\"kind\":\"workflow_loop\",\"reason\":\"old rollback block\",\"created_at\":\"2026-10-01T00:00:00Z\"}","updated_at":"2026-10-01T00:00:00Z"}}}"#;
        let archived: Task = serde_json::from_str(ARCHIVE).unwrap();
        assert_eq!(archived.condition.read().blocked_message.as_deref(), Some("old rollback block"));
        TaskRepo::mutate_metadata(&db, "historic", None, vec![crate::TaskMetadataMutation::Set {
            key: "queued_recovery".into(), value: json!({"id":"restore-history"}),
        }], AT).await.unwrap();
        let restore: TaskMutation = serde_json::from_str(RESTORE).unwrap();
        let restored: Task = serde_json::from_value(restore.apply(&db).await.unwrap()).unwrap();
        assert_eq!(restored.blocked_json, archived.blocked_json);
        assert_eq!(restored.condition.read().blocked_message.as_deref(), Some("old rollback block"));
        let envelope = Conditioned {
            input: sql,
            condition: ConditionStatement::Preserve,
        };
        assert_eq!(
            serde_json::from_value::<Conditioned<TaskMutation>>(
                serde_json::to_value(envelope).unwrap()
            )
            .unwrap()
            .condition,
            ConditionStatement::Preserve
        );
    })
    .await;
}

#[tokio::test]
async fn corrupt_idle_rebuilds_with_raw_text_and_live_step_self_heals() {
    let db = db().await;
    task(&db, "broken-idle").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='broken-idle'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(&["broken-idle".into()])
        .await
        .unwrap();
    let parked = db.task_condition("broken-idle").await.unwrap();
    assert!(matches!(parked, TaskCondition::Clear { .. }));
    let details = db
        .task_diagnostic_texts("broken-idle", None, 100)
        .await
        .unwrap()
        .items;
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].text, "{}");
    assert_eq!(
        db.check_task_conditions_of(&["broken-idle".into()])
            .await
            .unwrap(),
        0
    );
    super::statement_tests::hold(&db, "broken-idle", "new explicit intent").await;
    assert!(!recovery::unreadable(
        &db.task_condition("broken-idle").await.unwrap()
    ));
    task(&db, "broken-step").await;
    let step = super::producer_tests::enqueue(&db, "broken-step", "hooks", "todo", "{}").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='broken-step'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(&["broken-step".into()])
        .await
        .unwrap();
    assert!(
        matches!(db.task_condition("broken-step").await.unwrap(), TaskCondition::Entering { step_id, .. } if step_id == step)
    );
    assert_eq!(
        db.task_diagnostic_texts("broken-step", None, 100)
            .await
            .unwrap()
            .items[0]
            .text,
        "{}"
    );
}

#[tokio::test]
async fn stated_sql_queue_and_inline_paths_use_intent_not_sql_classification() {
    use crate::{
        task_writer::{TaskQuery, TaskQueryResult},
        TaskStepRepo,
    };
    let db = db().await;
    for queued in [false, true] {
        let id = if queued {
            "typed-queued"
        } else {
            "typed-inline"
        };
        task(&db, id).await;
        let statement = ConditionStatement::Hold {
            actor: "operator".into(),
            reason: "hold".into(),
            at: AT.into(),
        };
        if queued {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let result = TaskQuery::stated(
                &db,
                id,
                "UPDATE task SET title=? WHERE id=?",
                ConditionChange::Legacy,
                statement,
            )
            .bind("typed title")
            .bind(id)
            .execute_in_tx(&mut tx)
            .await
            .unwrap();
            assert!(matches!(result, TaskQueryResult::Queued { .. }));
            tx.commit().await.unwrap();
            let step = db
                .claim_step("typed-sql", Some(id), &crate::task_writer::lease_deadline())
                .await
                .unwrap()
                .unwrap();
            let payload: TaskMutation = serde_json::from_str(&step.payload_json).unwrap();
            assert!(matches!(payload, TaskMutation::StatedSql { .. }));
            crate::task_writer::in_task_step(step, async {
                assert_eq!(payload.apply(&db).await.unwrap(), json!(1));
            })
            .await;
        } else {
            let step = claim(&db, id).await;
            crate::task_writer::in_task_step(step, async {
                assert_eq!(
                    TaskQuery::stated(
                        &db,
                        id,
                        "UPDATE task SET title=? WHERE id=?",
                        ConditionChange::Legacy,
                        statement
                    )
                    .bind("typed title")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap(),
                    1
                );
            })
            .await;
        }
        let row = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        assert_eq!(row.title, "typed title");
        assert!(row
            .condition
            .reasons()
            .any(|r| matches!(r, ParkReason::Held { actor } if actor == "operator")));
        assert!(
            row.error_annotation.is_none(),
            "the typed intent was never inferred from a legacy field"
        );
    }
}

#[tokio::test]
async fn inline_cas_rejection_rolls_back_condition_legacy_and_text() {
    let db = db().await;
    let t = task(&db, "atomic").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        {
            let version = t.version - 1;
            let statement = ConditionStatement::Hold {
                actor: "operator".into(),
                reason: "full diagnostic that must roll back".into(),
                at: AT.into(),
            };
            let input = crate::UpdateTaskStatus {
                id: t.id.clone(),
                expected_version: version,
                status: t.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some("new legacy value".into())),
                blocked_json: None,
                failed_json: None,
                updated_at: AT.into(),
            };
            let result: Result<Task> = db
                .apply_conditioned_task_mutation(
                    &t.id,
                    Conditioned {
                        input: TaskMutation::TaskUpdateStatus { input },
                        condition: statement,
                    },
                )
                .await;
            assert!(matches!(result, Err(DbError::VersionConflict)));
            let row = TaskRepo::get_by_id(&db, &t.id, false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.version, t.version);
            assert!(row.error_annotation.is_none());
            assert_eq!(row.condition, t.condition);
            assert!(db
                .task_diagnostic_texts(&t.id, None, 100)
                .await
                .unwrap()
                .items
                .is_empty());
        }
    })
    .await;
}

#[tokio::test]
async fn unicode_attention_material_keeps_its_identity() {
    let db = db().await;
    task(&db, "unicode-material").await;
    let message = "😀".repeat(512);
    let material = MaterialBlocker {
        requires_intervention: true,
        interruption: Some(json!({"source":"failed","kind":"executor_failed","reason":message})),
    };
    let condition = TaskCondition::Failed {
        failure: ParkReason::Failure {
            failure_kind: api_types::FailureKind::ExecutorFailed,
        },
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since: None,
        evidence: ConditionEvidence {
            material: Some(material.clone()),
            presentation: Some(ConditionRead {
                failure_message: Some(message),
                hard_failure: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    };
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    set_condition(&mut tx, "unicode-material", &encode(&condition), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let stored = db.task_condition("unicode-material").await.unwrap();
    assert_eq!(
        material_blocker(&stored),
        material,
        "private Attention digest input retains its historical 512-character bound"
    );
}
