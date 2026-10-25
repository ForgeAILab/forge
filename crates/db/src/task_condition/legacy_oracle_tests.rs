use super::tests::{claim, db, task};
use super::*;
use crate::TaskRepo;
use serde_json::json;

#[tokio::test]
async fn corrupt_statements_keep_legacy_reasons_and_base_write_outcomes() {
    let db = db().await;
    let at = "2026-10-25T00:00:00Z";
    let replacement = ConditionReplacement {
        identity: ConditionIdentity::Entry {
            epoch: 0,
            transition_id: None,
        },
        condition: StatedCondition::Parked {
            reason: ParkReason::Failure {
                failure_kind: api_types::FailureKind::WorkspaceResetRequired,
            },
            resume: ConditionContinuation::Reconcile,
            since: None,
            presentation: ConditionPresentation::default(),
        },
    };
    let statements = vec![
        ConditionStatement::Hold {
            actor: "user".into(),
            reason: "hold".into(),
            at: at.into(),
        },
        ConditionStatement::Preserve,
        ConditionStatement::Release,
        ConditionStatement::Restore {
            saved: Box::new(SavedTaskCondition::capture(&TaskCondition::default())),
            expected_epoch: 0,
            expected_condition_digest: diagnostics::digest(b"{}"),
        },
        ConditionStatement::ReplaceDiagnostic {
            replacement: Box::new(replacement.clone()),
            preserve_interruption: true,
        },
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Diagnostic,
                expected: None,
                replacement: Some(replacement),
            }),
        },
        ConditionStatement::Integration {
            reason: IntegrationReason::Waiting {
                attempt_id: IntegrationAttemptId::new("attempt"),
            },
        },
        ConditionStatement::Check {
            wait: CheckWait {
                consumer_id: "consumer".into(),
                origin: "entry".into(),
                phase: CheckWaitPhase::Result,
            },
            epoch: 0,
        },
        ConditionStatement::CheckCleared {
            consumer_id: "consumer".into(),
        },
        ConditionStatement::IntegrationCleared {
            attempt_id: IntegrationAttemptId::new("attempt"),
        },
        ConditionStatement::IntegrationHandedOff {
            attempt_id: IntegrationAttemptId::new("attempt"),
        },
    ];
    for (n, statement) in statements.into_iter().enumerate() {
        let id = format!("corrupt-statement-{n}");
        task(&db, &id).await;
        sqlx::query("UPDATE task SET error_annotation=?,entry_barrier_json=?,metadata_json=?,condition_json='{}' WHERE id=?")
            .bind(json!({"type":"workspace_error", "blocking_reason":"old failure"}).to_string())
            .bind(json!({"state":"todo", "status":"blocked", "reason":"old barrier"}).to_string())
            .bind(json!({"awaiting_human":true,"paused_integration":{"reason":"paused"},"daemon_upgrade_refusal":{"message":"upgrade"},"owner_wait":{"daemon_id":"machine", "started_at":at}}).to_string())
            .bind(&id).execute(db.pool()).await.unwrap();
        let step = claim(&db, &id).await;
        let row = TaskRepo::get_by_id(&db, &id, false).await.unwrap().unwrap();
        let mut c = db.pool().acquire().await.unwrap();
        let mut facts = ConditionFacts::load(&mut c, &id).await.unwrap();
        drop(c);
        match &statement {
            ConditionStatement::Integration { reason } => facts.integration = Some(reason.clone()),
            ConditionStatement::Check { wait, epoch } => facts.check = Some((wait.clone(), *epoch)),
            _ => {}
        }
        let mut input = LegacyConditionInput::from(&row);
        input.facts = Some(facts);
        let expected = map_legacy_condition(&input);
        let refused = matches!(
            statement,
            ConditionStatement::IntegrationCleared { .. }
                | ConditionStatement::IntegrationHandedOff { .. }
        );
        crate::task_writer::in_task_step(step, async {
            let mut tx = db.pool().begin().await.unwrap();
            let result = if statement.owner_stated() {
                db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                    .await
            } else {
                db.state_task_condition_in_tx(&mut tx, &id, ConditionChange::Legacy, &statement)
                    .await
            };
            if refused {
                assert!(
                    matches!(result, Err(DbError::VersionConflict)),
                    "{statement:?}: {result:?}"
                );
                tx.rollback().await.unwrap();
                let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
                    .bind(&id)
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                assert_eq!(
                    raw, "{}",
                    "{statement:?}: refused write leaves storage intact"
                );
            } else {
                result.unwrap_or_else(|e| panic!("{statement:?}: {e:?}"));
                tx.commit().await.unwrap();
                let raw = raw_condition(&db, &id).await;
                assert_eq!(raw, encode(&expected), "{statement:?}: MAIN repair bytes");
                let actual = db.task_condition(&id).await.unwrap();
                assert_eq!(actual.typed(), expected.typed(), "{statement:?}");
                assert_eq!(actual.public(), expected.public(), "{statement:?}");
                assert_eq!(material_blocker(&actual), material_blocker(&expected));
                assert!(db
                    .task_diagnostic_texts(&id, None, 100)
                    .await
                    .unwrap()
                    .items
                    .iter()
                    .any(|t| t.text == "{}"));
            }
            let after = TaskRepo::get_by_id(&db, &id, false).await.unwrap().unwrap();
            assert_eq!(after.version, row.version, "{statement:?}");
            assert_eq!(
                LegacyConditionInput::from(&after),
                LegacyConditionInput::from(&row),
                "{statement:?}"
            );
        })
        .await;
    }
}

async fn raw_condition(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn legacy_oracle(db: &SqliteDb, id: &str) -> TaskCondition {
    let row = TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap();
    let mut c = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut c, id).await.unwrap();
    let mut input = LegacyConditionInput::from(&row);
    input.facts = Some(facts);
    map_legacy_condition(&input)
}

#[tokio::test]
async fn corrupt_background_check_stores_main_repair_bytes() {
    let db = db().await;
    task(&db, "corrupt-check").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='corrupt-check'")
        .execute(db.pool())
        .await
        .unwrap();
    let expected = encode(&legacy_oracle(&db, "corrupt-check").await);
    let status = db
        .check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    assert_eq!(status.repaired, 1);
    assert_eq!(raw_condition(&db, "corrupt-check").await, expected);
    let texts = db
        .task_diagnostic_texts("corrupt-check", None, 100)
        .await
        .unwrap();
    assert_eq!(texts.items.len(), 1);
    assert_eq!(texts.items[0].text, "{}");
    assert_eq!(texts.items[0].occurrence_count, 1);
}

#[tokio::test]
async fn once_corrupt_legacy_copy_is_repaired_on_first_targeted_check() {
    let db = db().await;
    task(&db, "stale-copy").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='stale-copy'")
        .execute(db.pool())
        .await
        .unwrap();
    let ids = ["stale-copy".to_owned()];
    assert_eq!(db.check_task_conditions_of(&ids).await.unwrap(), 1);
    let before = db.task_condition("stale-copy").await.unwrap();
    sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.awaiting_human',json('false')) WHERE id='stale-copy'")
        .execute(db.pool()).await.unwrap();
    let expected = legacy_oracle(&db, "stale-copy").await;
    assert_eq!(before.typed(), expected.typed());
    assert_ne!(raw_condition(&db, "stale-copy").await, encode(&expected));
    assert_eq!(db.check_task_conditions_of(&ids).await.unwrap(), 1);
    assert_eq!(raw_condition(&db, "stale-copy").await, encode(&expected));
    assert_eq!(db.check_task_conditions_of(&ids).await.unwrap(), 0);
}

async fn coordination_statement_matches_legacy(block: Option<&str>, visible_children: bool) {
    for integration in [false, true] {
        let db = db().await;
        task(&db, "root").await;
        if visible_children {
            task(&db, "child").await;
            super::producer_tests::set_parent(&db, "root", &["child"]).await;
        }
        let annotation =
            block.map(|kind| json!({"type":kind,"blocking_reason":"blocked root"}).to_string());
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=?,metadata_json='{\"coordination_review_pending\":true}' WHERE id='root'")
            .bind(annotation).execute(&mut *tx).await.unwrap();
        db.sync_condition_in_tx(&mut tx, "root").await.unwrap();
        tx.commit().await.unwrap();
        let step = claim(&db, "root").await;
        let before = TaskRepo::get_by_id(&db, "root", false)
            .await
            .unwrap()
            .unwrap();
        let statement = if integration {
            ConditionStatement::Integration {
                reason: IntegrationReason::Waiting {
                    attempt_id: IntegrationAttemptId::new("attempt"),
                },
            }
        } else {
            ConditionStatement::Check {
                wait: CheckWait {
                    consumer_id: "consumer".into(),
                    origin: "entry".into(),
                    phase: CheckWaitPhase::Result,
                },
                epoch: 0,
            }
        };
        let mut c = db.pool().acquire().await.unwrap();
        let mut facts = ConditionFacts::load(&mut c, "root").await.unwrap();
        drop(c);
        match &statement {
            ConditionStatement::Integration { reason } => facts.integration = Some(reason.clone()),
            ConditionStatement::Check { wait, epoch } => facts.check = Some((wait.clone(), *epoch)),
            _ => unreachable!(),
        }
        let mut input = LegacyConditionInput::from(&before);
        input.facts = Some(facts);
        let expected = map_legacy_condition(&input);
        crate::task_writer::in_task_step(step, async {
            let mut previous_raw = None;
            for _ in 0..2 {
                let mut tx = db.pool().begin().await.unwrap();
                db.state_integration_condition_in_tx(&mut tx, "root", &statement)
                    .await
                    .unwrap_or_else(|error| panic!("{statement:?}: {error:?}"));
                tx.commit().await.unwrap();
                let actual = db.task_condition("root").await.unwrap();
                assert_eq!(actual.typed(), expected.typed(), "{statement:?}");
                assert_eq!(actual.public(), expected.public(), "{statement:?}");
                let raw = raw_condition(&db, "root").await;
                if let Some(previous) = previous_raw.replace(raw.clone()) {
                    assert_eq!(raw, previous, "repeated {statement:?} must be idempotent");
                }
                let after = TaskRepo::get_by_id(&db, "root", false)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(after.version, before.version);
                assert_eq!(
                    LegacyConditionInput::from(&after),
                    LegacyConditionInput::from(&before)
                );
            }
        })
        .await;
    }
}

#[tokio::test]
async fn integration_and_check_match_legacy_on_held_root_with_children() {
    coordination_statement_matches_legacy(Some("manual_stop"), true).await;
}

#[tokio::test]
async fn integration_and_check_match_legacy_on_failed_root_with_children() {
    coordination_statement_matches_legacy(Some("workspace_error"), true).await;
}

#[tokio::test]
async fn integration_and_check_match_legacy_on_flagged_root_without_children() {
    coordination_statement_matches_legacy(None, false).await;
}

#[tokio::test]
async fn integration_and_check_match_legacy_on_unblocked_root_with_children() {
    coordination_statement_matches_legacy(None, true).await;
}

#[tokio::test]
async fn legacy_check_repairs_divergent_stated_ownership_in_one_sweep() {
    let db = db().await;
    task(&db, "stated-divergence").await;
    let row = TaskRepo::get_by_id(&db, "stated-divergence", false)
        .await
        .unwrap()
        .unwrap();
    let mut c = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut c, &row.id).await.unwrap();
    drop(c);
    let mut input = LegacyConditionInput::from(&row);
    input.facts = Some(facts);
    let expected = map_legacy_condition(&input);
    let statement = ConditionStatement::Edit {
        edit: Box::new(ConditionOwnerEdit {
            owner: ConditionReasonOwner::Diagnostic,
            expected: None,
            replacement: Some(ConditionReplacement {
                identity: ConditionIdentity::Entry {
                    epoch: 0,
                    transition_id: None,
                },
                condition: StatedCondition::Parked {
                    reason: ParkReason::Failure {
                        failure_kind: api_types::FailureKind::WorkspaceResetRequired,
                    },
                    resume: ConditionContinuation::Reconcile,
                    since: None,
                    presentation: Default::default(),
                },
            }),
        }),
    };
    super::test_support::set_task_condition(&db, &row.id, statement)
        .await
        .unwrap();
    let report = db
        .check_task_conditions_of(std::slice::from_ref(&row.id))
        .await
        .unwrap();
    assert_eq!(report, 1);
    assert_eq!(
        db.task_condition(&row.id).await.unwrap().typed(),
        expected.typed()
    );
}

#[tokio::test]
async fn maximal_free_text_is_stored_without_refusing_the_legacy_write() {
    let db = db().await;
    task(&db, "maximum-text").await;
    let text = "\0".repeat(PRESENTATION_TEXT_LIMIT);
    let big = (0..32)
        .map(|n| (format!("field-{n}"), json!(text)))
        .collect::<serde_json::Map<_, _>>();
    let annotation = json!({"type":"workspace_error", "blocking_reason":text, "blocked_by":text,
        "message":text, "hook":big, "artifact":{"kind":"log", "id":"artifact", "log_path":text}})
    .to_string();
    let entry =
        json!({"state":"todo", "status":"blocked", "message":text, "diagnostic":big}).to_string();
    let interrupted =
        json!({"kind":"executor_failed", "reason":text, "created_at":"2026-10-25T00:00:00Z"})
            .to_string();
    let metadata = json!({
        "environment_wait":big,
        "placement_refusal":{"annotation":serde_json::from_str::<serde_json::Value>(&annotation).unwrap(),"message":text},
        "deferred_dispatch":{"kind":"execution_retry", "reason":text, "not_before":"2099-01-01T00:00:00Z", "target_state":"blocked"},
        "dispatch_disposition":{"task_version":1, "capability":"machine_capacity", "blocker_digest":"fixture", "recorded_at":"2026-10-25T00:00:00Z", "safe_message":text},
    }).to_string();
    let step = claim(&db, "maximum-text").await;
    crate::task_writer::in_task_step(step.clone(), async {
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("UPDATE task SET status='blocked',error_annotation=?,entry_barrier_json=?,metadata_json=?,blocked_json=?,failed_json=? WHERE id='maximum-text'")
            .bind(&annotation).bind(&entry).bind(&metadata).bind(&interrupted).bind(&interrupted).execute(&mut *tx).await.unwrap();
        db.produce_condition_in_tx(&mut tx, "maximum-text", ConditionChange::Full).await.unwrap();
        tx.commit().await.unwrap();
    }).await;
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='maximum-text'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let stored = decode(&raw).unwrap();
    let row = TaskRepo::get_by_id(&db, "maximum-text", false)
        .await
        .unwrap()
        .unwrap();
    let mut c = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut c, &row.id).await.unwrap();
    drop(c);
    let mut input = LegacyConditionInput::from(&row);
    input.facts = Some(facts);
    let expected = map_legacy_condition(&input);
    assert_eq!(raw, serde_json::to_string(&expected).unwrap());
    assert_eq!(row.condition.public(), expected.public());
    assert_eq!(row.condition.read(), expected.read());
    assert_eq!(material_blocker(&stored), material_blocker(&expected));
    let operator = db.blocked_task_operator_rows().await.unwrap();
    // The Operations query includes interrupted or explicitly blocked Tasks.
    assert_eq!(operator.len(), 1);
    assert_eq!(
        decode(&operator[0].condition_json)
            .unwrap()
            .read()
            .operator_reason,
        expected.read().operator_reason
    );
    assert_eq!(
        db.check_task_conditions_of(std::slice::from_ref(&row.id))
            .await
            .unwrap(),
        0
    );
    let released =
        crate::task_writer::in_task_step(step, super::statement_tests::release(&db, &row.id)).await;
    let mut c = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut c, &row.id).await.unwrap();
    drop(c);
    let mut input = LegacyConditionInput::from(&released);
    input.facts = Some(facts);
    let mapped = map_legacy_condition(&input);
    assert_eq!(
        released.condition.read(),
        mapped.read(),
        "a following owner write preserves MAIN presentation"
    );
    assert_eq!(released.condition.public(), mapped.public());
    assert_eq!(
        material_blocker(&released.condition),
        material_blocker(&mapped)
    );
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&row.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(raw, serde_json::to_string(&released.condition).unwrap());
}
