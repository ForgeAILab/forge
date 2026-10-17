//! A writer that states its condition must store what the legacy mapping of
//! the fields it just wrote would have stored. The legacy fields are still
//! written, so the old mapping is the oracle: every case drives the real
//! repository writer, then compares the stored condition with
//! `map_legacy_condition` under the Task's durable facts. The only named
//! differences are the ones [`TaskCondition::typed`] removes: the six legacy
//! copies in `evidence` (absent from a stated condition) and `evidence.stated`.
use super::tests::{db, task};
use super::*;
use crate::{TaskMetadataMutation, TaskRepo};
use serde_json::{json, Value};

const AT: &str = "2026-10-07T12:00:00Z";

/// The stored condition and what the old mapping says of the row as it stands.
async fn stored_and_mapped(db: &SqliteDb, id: &str) -> (TaskCondition, TaskCondition) {
    let row = TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap();
    let mut connection = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut connection, id).await.unwrap();
    let mut input = LegacyConditionInput::from(&row);
    input.facts = Some(facts);
    (row.condition, map_legacy_condition(&input))
}

/// Give a Task the state other writers leave behind, through the mapping
/// those writers still use.
async fn given(
    db: &SqliteDb,
    id: &str,
    status: &str,
    columns: [Option<Value>; 4],
    metadata: Value,
) {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status=?,error_annotation=?,blocked_json=?,failed_json=?,entry_barrier_json=?,metadata_json=? WHERE id=?")
        .bind(status)
        .bind(columns[0].as_ref().map(Value::to_string))
        .bind(columns[1].as_ref().map(Value::to_string))
        .bind(columns[2].as_ref().map(Value::to_string))
        .bind(columns[3].as_ref().map(Value::to_string))
        .bind(metadata.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, id).await.unwrap();
    tx.commit().await.unwrap();
}

/// The owner's hold, exactly as `TaskService::hold_waiting_task` writes it.
pub(super) async fn hold(db: &SqliteDb, id: &str, reason: &str) -> Task {
    let version = TaskRepo::get_by_id(db, id, false)
        .await
        .unwrap()
        .unwrap()
        .version;
    TaskRepo::update_recovery_metadata_if_no_running_execution(
        db,
        id,
        version,
        Some(ConditionStatement::hold_operator_text("user", reason, AT)),
        Some(json!({"kind":"manual_stop","reason":reason,"created_at":AT}).to_string()),
        None,
        AT,
        None,
        vec!["coder".into()],
        [
            "queued_recovery",
            "deferred_dispatch",
            "dispatch_disposition",
            "environment_wait",
            "owner_wait",
        ]
        .into_iter()
        .map(|key| TaskMetadataMutation::Remove { key: key.into() })
        .collect(),
        Some(ConditionStatement::Hold {
            actor: "user".into(),
            reason: reason.into(),
            at: AT.into(),
        }),
    )
    .await
    .unwrap()
}

/// The owner's release, exactly as `TaskService::release_to_dispatch_queue`
/// writes it.
pub(super) async fn release(db: &SqliteDb, id: &str) -> Task {
    let version = TaskRepo::get_by_id(db, id, false)
        .await
        .unwrap()
        .unwrap()
        .version;
    TaskRepo::update_recovery_metadata_if_no_running_execution(
        db,
        id,
        version,
        None,
        None,
        None,
        AT,
        None,
        vec!["coder".into()],
        Vec::new(),
        Some(ConditionStatement::Release),
    )
    .await
    .unwrap()
}

async fn assert_stated_as_mapped(db: &SqliteDb, id: &str, context: &str) -> TaskCondition {
    let (stored, mapped) = stored_and_mapped(db, id).await;
    let evidence = stored.evidence();
    assert!(
        evidence.stated,
        "{context}: the writer stated the condition"
    );
    assert!(
        evidence.error_annotation.is_none()
            && evidence.blocked_json.is_none()
            && evidence.failed_json.is_none()
            && evidence.entry_barrier_json.is_none()
            && evidence.metadata.is_empty()
            && evidence.unparsed_metadata.is_none(),
        "{context}: no legacy copy in a stated condition"
    );
    assert_eq!(stored.typed(), mapped.typed(), "{context}: typed condition");
    assert_eq!(
        stored.public(),
        mapped.public(),
        "{context}: public reading"
    );
    assert_eq!(stored.read(), mapped.read(), "{context}: reader projection");
    assert_eq!(
        material_blocker(&stored),
        material_blocker(&mapped),
        "{context}: Attention blocker"
    );
    // The background check finds nothing to repair, so the stated condition
    // is not mapped back from the legacy fields behind the writer.
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new(),
        "{context}"
    );
    let before = db.condition_check_status().repaired;
    db.check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    assert_eq!(db.condition_check_status().repaired, before, "{context}");
    let (after, _) = stored_and_mapped(db, id).await;
    assert_eq!(after, stored, "{context}: the check left the statement");
    stored
}

fn cases() -> Vec<(&'static str, &'static str, [Option<Value>; 4], Value)> {
    let barrier = json!({"state":"review","status":"blocked","started_at":"2026-10-07T10:00:00Z","blocking_reason":"hook failed"});
    let retry =
        json!({"not_before":"2026-10-07T13:00:00Z","reason":"retry","target_state":"in_progress"});
    vec![
        (
            "nothing else",
            "in_progress",
            [None, None, None, None],
            json!({}),
        ),
        ("initial state", "todo", [None, None, None, None], json!({})),
        (
            "blocked entry",
            "in_progress",
            [None, None, None, Some(barrier.clone())],
            json!({}),
        ),
        (
            "blocked entry in an initial state",
            "todo",
            [None, None, None, Some(barrier.clone())],
            json!({}),
        ),
        (
            "exhausted review entry",
            "in_progress",
            [
                None,
                None,
                None,
                Some(
                    json!({"state":"review","status":"blocked","blocking_reason":"review retry budget exhausted"}),
                ),
            ],
            json!({}),
        ),
        (
            "other owners' waits",
            "in_progress",
            [None, None, None, None],
            json!({
                "placement_refusal":{"state":"in_progress","annotation":{"type":"dispatch_failed"}},
                "daemon_upgrade_refusal":{"code":"upgrade"},
                "paused_integration":{"state":"merging","deferred_at":AT},
                "awaiting_human":true,
                "awaiting_human_reason":"plan_review",
                "retry_budgets":{"execution":3}
            }),
        ),
        (
            "waits a hold drops",
            "in_progress",
            [
                Some(
                    json!({"type":"dispatch_failed","blocking_reason":"no machine","message":"none"}),
                ),
                None,
                None,
                None,
            ],
            json!({
                "deferred_dispatch":retry,
                "owner_wait":{"daemon_id":"d1","started_at":"2026-10-07T09:00:00Z"},
                "environment_wait":{"kind":"environment_not_ready"},
                "dispatch_disposition":{"task_version":1,"capability":"machine_capacity","blocker_digest":"x","recorded_at":AT,"safe_message":"busy"},
                "queued_recovery":{"id":"q1"}
            }),
        ),
        (
            "retry timer",
            "in_progress",
            [None, None, None, None],
            json!({"deferred_dispatch":retry,"last_execution_failure_execution_id":"e1"}),
        ),
        (
            "plan settlement behind a retry timer",
            "in_progress",
            [None, None, None, None],
            json!({"deferred_dispatch":retry,"plan_settlement_wait":{"execution_id":"e7"}}),
        ),
        (
            "review CI retry standing in for a blocked entry",
            "in_progress",
            [
                Some(json!({"type":"ci_failed","blocking_reason":"review_ci_infrastructure"})),
                None,
                None,
                Some(barrier),
            ],
            json!({"deferred_dispatch":retry}),
        ),
        (
            "failure and interruption",
            "in_progress",
            [
                Some(
                    json!({"type":"executor_failed","blocking_reason":"crashed","task_step_id":"s1"}),
                ),
                Some(json!({"kind":"retry_exhausted","reason":"out of retries","created_at":AT})),
                Some(
                    json!({"kind":"executor_failed","reason":"boom","created_at":AT,"execution_id":"e2"}),
                ),
                None,
            ],
            json!({}),
        ),
        (
            "malformed waits",
            "in_progress",
            [None, None, None, None],
            json!({"owner_wait":7,"placement_refusal":"no","awaiting_human":"yes"}),
        ),
    ]
}

#[tokio::test]
async fn hold_and_release_state_what_the_legacy_mapping_of_their_writes_says() {
    for (index, (name, status, columns, metadata)) in cases().into_iter().enumerate() {
        let db = db().await;
        let id = format!("t{index}");
        task(&db, &id).await;
        given(&db, &id, status, columns, metadata).await;

        hold(&db, &id, "Wait for the owner measurements").await;
        let held = assert_stated_as_mapped(&db, &id, &format!("hold over {name}")).await;
        assert!(
            matches!(&held, TaskCondition::Parked { primary: ParkReason::Held { actor }, .. } if actor == "user"),
            "hold over {name}: {held:?}"
        );

        release(&db, &id).await;
        let released = assert_stated_as_mapped(&db, &id, &format!("release after {name}")).await;
        assert!(
            !released
                .reasons()
                .any(|reason| matches!(reason, ParkReason::Held { .. })),
            "release after {name}: {released:?}"
        );
    }
}

/// A release is also offered on a Task that was never held by this writer
/// (a stopped run, a restored row): it states its condition from whatever is
/// stored, and the mapping of what it wrote must agree.
#[tokio::test]
async fn release_states_the_legacy_mapping_without_a_prior_hold() {
    for (index, (name, status, columns, metadata)) in cases().into_iter().enumerate() {
        let db = db().await;
        let id = format!("r{index}");
        task(&db, &id).await;
        given(&db, &id, status, columns, metadata).await;
        release(&db, &id).await;
        assert_stated_as_mapped(&db, &id, &format!("release over {name}")).await;
    }
}

/// Scope item 1: a 200,000-byte reason does not grow the stored condition.
/// Ceilings: 8 KiB where the writer states the condition; 24 KiB where the
/// condition is still mapped from the legacy fields, which copies each of
/// them into `evidence` cut at [`EVIDENCE_VALUE_LIMIT`].
#[tokio::test]
async fn a_200_000_byte_reason_keeps_the_stored_condition_small() {
    const STATED_CEILING: usize = 8 * 1024;
    const MAPPED_CEILING: usize = 24 * 1024;
    let reason = "é".repeat(100_000);
    assert_eq!(reason.len(), 200_000);
    let db = db().await;
    task(&db, "big").await;
    let size = |db: &SqliteDb| {
        let pool = db.pool().clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT length(CAST(condition_json AS BLOB)) FROM task WHERE id='big'",
            )
            .fetch_one(&pool)
            .await
            .unwrap() as usize
        }
    };

    hold(&db, "big", &reason).await;
    let stated = size(&db).await;
    assert!(stated <= STATED_CEILING, "held: {stated} bytes");
    let held = assert_stated_as_mapped(&db, "big", "hold with a long reason").await;
    let read = held.read();
    for text in [
        read.blocked_message.as_deref().unwrap(),
        read.diagnostic.as_ref().unwrap().blocking_reason.as_str(),
        read.interruption.as_ref().unwrap().reason.as_str(),
    ] {
        assert!(text.len() <= PRESENTATION_TEXT_LIMIT);
        assert!(text.ends_with(PRESENTATION_TRUNCATION_MARKER));
    }
    release(&db, "big").await;
    assert!(size(&db).await <= STATED_CEILING);

    // The execution-failure writer is not converted: its condition is mapped.
    let version = TaskRepo::get_by_id(&db, "big", false)
        .await
        .unwrap()
        .unwrap()
        .version;
    TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        "big",
        version,
        Some(
            json!({"type":"executor_failed","blocking_reason":"failed","message":reason})
                .to_string(),
        ),
        None,
        Some(json!({"kind":"executor_failed","reason":reason,"created_at":AT}).to_string()),
        AT,
        None,
        vec!["coder".into()],
        Vec::new(),
        None,
    )
    .await
    .unwrap();
    let mapped = size(&db).await;
    assert!(mapped <= MAPPED_CEILING, "failed: {mapped} bytes");
    println!("condition_json bytes: stated hold {stated}, mapped failure {mapped}");
}

/// Item 6: a stored condition that cannot be read is not edited by a
/// statement. The writer's write still lands and the condition is restated
/// whole, so an ordinary hold or release never leaves the Task unreadable.
/// This is the corrupt-or-empty class: nothing in it is a newer encoding.
#[tokio::test]
async fn a_statement_over_an_unreadable_condition_restates_it() {
    let db = db().await;
    task(&db, "broken").await;
    // The column only admits JSON, so "unreadable" is JSON no decoder accepts:
    // empty, a scalar, an array, and a known tag with a broken body.
    for corrupt in ["{}", "[]", "7", "{\"kind\":\"clear\"}", "{\"kind\":7}"] {
        assert!(matches!(classify(corrupt.as_bytes()), Stored::Corrupt(_)));
        sqlx::query("UPDATE task SET condition_json=? WHERE id='broken'")
            .bind(corrupt)
            .execute(db.pool())
            .await
            .unwrap();
        let held = hold(&db, "broken", "hold").await;
        assert!(
            matches!(
                &held.condition,
                TaskCondition::Parked {
                    primary: ParkReason::Held { .. },
                    ..
                }
            ),
            "{corrupt:?}: {:?}",
            held.condition
        );
        assert_eq!(
            db.task_condition_violations().await.unwrap(),
            Vec::<String>::new()
        );
        sqlx::query("UPDATE task SET condition_json=? WHERE id='broken'")
            .bind(corrupt)
            .execute(db.pool())
            .await
            .unwrap();
        let released = release(&db, "broken").await;
        assert!(
            matches!(released.condition, TaskCondition::Clear { .. }),
            "{corrupt:?}"
        );
        assert_eq!(
            db.task_condition_violations().await.unwrap(),
            Vec::<String>::new()
        );
    }
}

/// The classes, pinned: a serde message change must fail here, not silently
/// move a newer encoding into the class that is overwritten.
#[test]
fn stored_conditions_classify_as_readable_corrupt_or_newer() {
    use UnknownConditionProblem as Problem;
    for (raw, expected) in [
        ("not json", Some(Problem::MalformedJson)),
        ("7", Some(Problem::NonObject)),
        ("[]", Some(Problem::NonObject)),
        ("{}", Some(Problem::InvalidShape)),
        (r#"{"kind":7}"#, Some(Problem::InvalidShape)),
        (r#"{"kind":"clear"}"#, Some(Problem::InvalidShape)),
        (
            r#"{"kind":"parked","primary":{"kind":"held"},"additional":[],"resume":{"kind":"reconcile"},"since":null,"evidence":{}}"#,
            Some(Problem::InvalidShape),
        ),
        // A well-formed tagged value naming a variant this build lacks.
        (r#"{"kind":"nope","evidence":{}}"#, None),
        (
            r#"{"kind":"parked","primary":{"kind":"future_integration"},"additional":[],"resume":{"kind":"reconcile"},"since":null,"evidence":{}}"#,
            None,
        ),
        (
            r#"{"kind":"parked","primary":{"kind":"integration","reason":{"kind":"future_reason","attempt_id":"a"}},"additional":[],"resume":{"kind":"reconcile"},"since":null,"evidence":{}}"#,
            None,
        ),
        (
            r#"{"kind":"deferred","until":null,"reason":"legacy","resume":{"kind":"future_resume"},"evidence":{}}"#,
            None,
        ),
        (
            r#"{"kind":"clear","evidence":{"witnesses":[{"kind":"future_witness"}]}}"#,
            None,
        ),
    ] {
        match (classify(raw.as_bytes()), expected.clone()) {
            (Stored::Corrupt(problem), Some(expected)) => assert_eq!(problem, expected, "{raw}"),
            (Stored::Newer, None) => {}
            (other, _) => panic!("{raw}: {other:?}"),
        }
        let unknown_kind = matches!(
            decode_or_unknown(raw),
            TaskCondition::Parked {
                primary: ParkReason::UnknownCondition {
                    problem: Problem::UnknownKind,
                    ..
                },
                ..
            }
        );
        assert_eq!(unknown_kind, expected.is_none(), "{raw}");
    }
    assert!(matches!(
        classify(encode(&TaskCondition::default()).as_bytes()),
        Stored::Readable(_)
    ));
}

/// A recognisably newer encoding is quarantined. A hold or a release over it
/// is an error that writes nothing: no annotation, no version, no timestamp,
/// no event, and the stored bytes stay as they were. No check repairs it; the
/// pass counts it and names the Task.
#[tokio::test]
async fn a_statement_over_a_newer_encoding_is_refused_and_writes_nothing() {
    let db = db().await;
    task(&db, "newer").await;
    let row = |db: &SqliteDb| {
        let pool = db.pool().clone();
        async move {
            sqlx::query_as::<_, (i64, Option<String>, Option<String>, String, String, i64)>(
                "SELECT version,error_annotation,blocked_json,updated_at,condition_json,(SELECT count(*) FROM domain_event) FROM task WHERE id='newer'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    for newer in [
        r#"{"kind":"nope","evidence":{}}"#,
        r#"{"kind":"parked","primary":{"kind":"future_integration"},"additional":[],"resume":{"kind":"reconcile"},"since":null,"evidence":{}}"#,
    ] {
        sqlx::query("UPDATE task SET condition_json=? WHERE id='newer'")
            .bind(newer)
            .execute(db.pool())
            .await
            .unwrap();
        let before = row(&db).await;
        for statement in [
            ConditionStatement::Hold {
                actor: "user".into(),
                reason: "hold".into(),
                at: AT.into(),
            },
            ConditionStatement::Release,
        ] {
            let held = matches!(statement, ConditionStatement::Hold { .. });
            let refused = TaskRepo::update_recovery_metadata_if_no_running_execution(
                &db,
                "newer",
                before.0,
                held.then(|| ConditionStatement::hold_operator_text("user", "hold", AT)),
                held.then(|| {
                    json!({"kind":"manual_stop","reason":"hold","created_at":AT}).to_string()
                }),
                None,
                AT,
                None,
                vec!["coder".into()],
                Vec::new(),
                Some(statement),
            )
            .await;
            assert!(
                matches!(&refused, Err(DbError::TaskConditionQuarantined { task_id }) if task_id == "newer"),
                "{newer}: {refused:?}"
            );
            assert!(!refused.unwrap_err().is_transient());
            assert_eq!(row(&db).await, before, "{newer}: nothing was written");
        }
        // Readers keep the typed unknown park.
        assert!(matches!(
            TaskRepo::get_by_id(&db, "newer", false)
                .await
                .unwrap()
                .unwrap()
                .condition,
            TaskCondition::Parked {
                primary: ParkReason::UnknownCondition {
                    problem: UnknownConditionProblem::UnknownKind,
                    ..
                },
                ..
            }
        ));
        // No check, sync or backfill rewrites it.
        assert_eq!(
            db.check_task_conditions_of(&["newer".to_owned()])
                .await
                .unwrap(),
            0
        );
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.sync_condition_in_tx(&mut tx, "newer").await.unwrap();
        backfill(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(row(&db).await, before, "{newer}");
    }
    // A whole pass reports the quarantined Task by id.
    while db
        .check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap()
        .last_pass
        .is_none_or(|pass| pass.quarantined == 0)
    {}
    let pass = db.condition_check_status().last_pass.unwrap();
    assert_eq!(
        (pass.quarantined, pass.quarantined_ids, pass.repaired),
        (1, vec!["newer".to_owned()], 0)
    );
}

/// The other newer class: a database a newer build has recorded its mapping
/// revision in. Every undecodable value there is that build's encoding, even
/// one this build would otherwise call empty, and the marker is not lowered.
#[tokio::test]
async fn an_undecodable_condition_under_a_newer_recorded_revision_is_quarantined() {
    let db = db().await;
    task(&db, "ahead").await;
    let newer = (MAPPING_REVISION + 1).to_string();
    crate::SystemSettingRepo::set_setting(&db, MAPPING_REVISION_KEY, &newer, AT)
        .await
        .unwrap();
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='ahead'")
        .execute(db.pool())
        .await
        .unwrap();
    let version = TaskRepo::get_by_id(&db, "ahead", false)
        .await
        .unwrap()
        .unwrap()
        .version;
    let refused = TaskRepo::update_recovery_metadata_if_no_running_execution(
        &db,
        "ahead",
        version,
        None,
        None,
        None,
        AT,
        None,
        vec!["coder".into()],
        Vec::new(),
        Some(ConditionStatement::Release),
    )
    .await;
    assert!(matches!(
        refused,
        Err(DbError::TaskConditionQuarantined { .. })
    ));
    assert_eq!(
        db.check_task_conditions_of(&["ahead".to_owned()])
            .await
            .unwrap(),
        0
    );
    assert!(!db.backfill_task_conditions_if_stale().await.unwrap());
    assert_eq!(
        crate::SystemSettingRepo::get_setting(&db, MAPPING_REVISION_KEY)
            .await
            .unwrap(),
        Some(newer)
    );
    let stored: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id='ahead'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(stored, "{}");
}

/// A queued hold written before writers stated conditions still applies.
#[test]
fn a_queued_mutation_without_a_statement_still_decodes() {
    let stored = json!({"TaskUpdateRecoveryMetadataIfNoRunningExecution":{
        "id":"t","expected_version":1,"error_annotation":null,"blocked_json":null,
        "failed_json":null,"updated_at":AT,"workspace_id":null,"overlapping_roles":[],
        "metadata_mutations":[]}});
    let decoded: crate::TaskMutation = serde_json::from_value(stored).unwrap();
    assert!(matches!(
        decoded,
        crate::TaskMutation::TaskUpdateRecoveryMetadataIfNoRunningExecution {
            condition: None,
            ..
        }
    ));
}
