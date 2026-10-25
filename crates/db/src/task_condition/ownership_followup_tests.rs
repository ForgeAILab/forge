use super::tests::{claim, db, task};
use super::*;
use crate::TaskRepo;
use serde_json::json;

const EPOCH: i64 = 7;
const AT: &str = "2026-10-25T00:00:00Z";

async fn read(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}

async fn replace_stored(db: &SqliteDb, id: &str, condition: &TaskCondition) {
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(encode(condition))
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
}

async fn state(
    db: &SqliteDb,
    id: &str,
    statement: &ConditionStatement,
    context: &str,
) -> TaskCondition {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.state_integration_condition_in_tx(&mut tx, id, statement)
        .await
        .unwrap_or_else(|error| {
            panic!("{context} {id} {statement:?}: statement refused: {error:?}")
        });
    tx.commit().await.unwrap();
    db.task_condition(id).await.unwrap()
}

fn assert_projections(actual: &TaskCondition, expected: &TaskCondition, context: &str) {
    assert_eq!(actual.typed(), expected.typed(), "{context}: typed");
    assert_eq!(actual.public(), expected.public(), "{context}: public");
    assert_eq!(
        material_blocker(actual),
        material_blocker(expected),
        "{context}: material"
    );
}

async fn assert_statement_matches_main(
    db: &SqliteDb,
    id: &str,
    identity: &ConditionIdentity,
    statement: &ConditionStatement,
) {
    // Reproduce the writers' stored Ownership row, including any integration
    // or check witness already live in this epoch, immediately before each seam.
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.state_task_condition_in_tx(
        &mut tx,
        id,
        ConditionChange::Legacy,
        &ConditionStatement::Ownership {
            owner: ConditionReasonOwner::Diagnostic,
            identity: identity.clone(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let before = read(db, id).await;
    let owned = before.condition.clone();
    assert!(owned.evidence().stated);
    assert_eq!(
        owned.evidence().ownership,
        vec![ConditionOwnership {
            owner: ConditionReasonOwner::Diagnostic,
            identity: identity.clone(),
            reason: None,
            state: None,
            material: None,
        }]
    );
    assert_eq!(owned, {
        let mut typed = owned.typed();
        typed.evidence_mut().stated = true;
        typed.evidence_mut().ownership = owned.evidence().ownership.clone();
        typed
    });
    let mut c = db.pool().acquire().await.unwrap();
    let facts = ConditionFacts::load(&mut c, id).await.unwrap();
    drop(c);
    assert_eq!(facts.epoch, EPOCH);
    // Main's writers leave the full, unstated legacy mapping. Replay the same
    // statement over that row on the same Task, keeping lease/facts/columns equal.
    let main_row = facts.condition(&LegacyConditionInput::from(&before));
    assert!(!main_row.evidence().stated);
    assert!(main_row.evidence().ownership.is_empty());
    assert_projections(&owned, &main_row, "writer row");
    replace_stored(db, id, &main_row).await;
    let expected = state(db, id, statement, "unstated main row").await;
    replace_stored(db, id, &owned).await;
    let actual = state(db, id, statement, "Ownership row").await;
    assert_projections(&actual, &expected, &format!("{id} {statement:?}"));
    let after = read(db, id).await;
    assert_eq!(
        LegacyConditionInput::from(&after),
        LegacyConditionInput::from(&before),
        "{statement:?}: no legacy column changes"
    );
    assert_eq!(
        (after.version, after.updated_at),
        (before.version, before.updated_at),
        "{statement:?}: no Task version or timestamp changes"
    );
    assert_eq!(
        db.task_condition_entry_identity(id, after.version)
            .await
            .unwrap()
            .epoch(),
        EPOCH,
        "{statement:?}: same status epoch"
    );
}

async fn ownership_statements_match_main(operation: bool) {
    let db = db().await;
    let id = if operation { "reset" } else { "candidate" };
    task(&db, id).await;
    let identity = if operation {
        ConditionIdentity::Operation {
            epoch: EPOCH,
            operation_id: "placement".into(),
            generation: 1,
        }
    } else {
        ConditionIdentity::Entry {
            epoch: EPOCH,
            transition_id: None,
        }
    };
    let (status, annotation, blocked) = if operation {
        (
            "in_progress",
            json!({"type":"workspace_reset_required", "blocking_reason":"owner_workspace_reset_required",
                "blocked_by":"system:heartbeat_monitor", "blocked_at":AT, "message":"owner reset"}),
            None,
        )
    } else {
        let reason = "nothing to review: no implementation run or branch commits were delivered";
        (
            "review",
            json!({"type":"review_blocked", "blocking_reason":"review_candidate_missing",
                "message":format!("{reason}; send back to add work, or cancel the Task"), "blocked_at":AT}),
            Some(json!({"kind":"review_blocked", "reason":reason, "created_at":AT, "execution_id":null}).to_string()),
        )
    };
    if operation {
        // Keep this acceptance fixture outside main's existing refusal for
        // recovery guidance on an in-progress Task with no coder assignment.
        crate::AgentRepo::create(
            &db,
            crate::CreateAgent {
                id: "coder".into(),
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
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: crate::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".into(),
                created_at: AT.into(),
                updated_at: AT.into(),
            },
        )
        .await
        .unwrap();
    }
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    if operation {
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('coder-role',?,'coder','agent','coder',?,?)")
            .bind(id).bind(AT).bind(AT).execute(&mut *tx).await.unwrap();
    }
    sqlx::query(
        "UPDATE task SET status=?,error_annotation=?,blocked_json=?,failed_json=NULL WHERE id=?",
    )
    .bind(status)
    .bind(annotation.to_string())
    .bind(blocked)
    .bind(id)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE task SET status_epoch=? WHERE id=?")
        .bind(EPOCH)
        .bind(id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, id).await.unwrap();
    tx.commit().await.unwrap();
    let step = claim(&db, id).await;
    crate::task_writer::in_task_step(step, async {
        // All production owner-stated kinds: Integration, IntegrationHandedOff,
        // IntegrationCleared, Check and CheckCleared. Each starts with Ownership.
        for reason in super::integration_tests::reasons() {
            assert_statement_matches_main(
                &db,
                id,
                &identity,
                &ConditionStatement::Integration {
                    reason: reason.clone(),
                },
            )
            .await;
            assert_eq!(
                db.task_condition(id).await.unwrap().integration_reason(),
                Some(&reason)
            );
            if reason.hands_off() {
                assert_statement_matches_main(
                    &db,
                    id,
                    &identity,
                    &ConditionStatement::IntegrationHandedOff {
                        attempt_id: reason.attempt_id().clone(),
                    },
                )
                .await;
                assert!(db
                    .task_condition(id)
                    .await
                    .unwrap()
                    .integration_handoff_ready());
            }
            assert_statement_matches_main(
                &db,
                id,
                &identity,
                &ConditionStatement::IntegrationCleared {
                    attempt_id: reason.attempt_id().clone(),
                },
            )
            .await;
            assert!(db
                .task_condition(id)
                .await
                .unwrap()
                .integration_reason()
                .is_none());
        }
        for phase in [
            CheckWaitPhase::Result,
            CheckWaitPhase::Slot,
            CheckWaitPhase::InfrastructureExhausted,
        ] {
            let wait = CheckWait {
                consumer_id: "consumer".into(),
                origin: "entry".into(),
                phase,
            };
            assert_statement_matches_main(
                &db,
                id,
                &identity,
                &ConditionStatement::Check {
                    wait: wait.clone(),
                    epoch: EPOCH,
                },
            )
            .await;
            assert_eq!(
                db.task_condition(id).await.unwrap().check_wait(),
                Some(&wait)
            );
            assert_statement_matches_main(
                &db,
                id,
                &identity,
                &ConditionStatement::CheckCleared {
                    consumer_id: wait.consumer_id,
                },
            )
            .await;
            assert!(db
                .task_condition(id)
                .await
                .unwrap()
                .check_witness()
                .is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn operation_ownership_then_integration_and_check_match_main() {
    ownership_statements_match_main(true).await;
}

#[tokio::test]
async fn entry_ownership_then_integration_and_check_match_main() {
    ownership_statements_match_main(false).await;
}
