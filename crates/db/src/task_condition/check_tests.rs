//! The durable check runner's typed waits: stated under the Task's claimed
//! step, visible, preserved across other writers, and ended by their owner.
use super::tests::{claim, db, task};
use super::*;
use crate::TaskRepo;
use serde_json::json;

fn wait(phase: CheckWaitPhase, consumer: &str) -> CheckWait {
    CheckWait {
        phase,
        consumer_id: consumer.into(),
        origin: "entry".into(),
    }
}
async fn read(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}
async fn try_state(db: &SqliteDb, id: &str, statement: &ConditionStatement) -> Result<()> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    db.state_integration_condition_in_tx(&mut tx, id, statement)
        .await?;
    tx.commit().await?;
    Ok(())
}
async fn state(db: &SqliteDb, id: &str, phase: CheckWaitPhase, consumer: &str) {
    try_state(
        db,
        id,
        &ConditionStatement::Check {
            wait: wait(phase, consumer),
            epoch: 0,
        },
    )
    .await
    .unwrap();
}
async fn clear(db: &SqliteDb, id: &str, consumer: &str) {
    try_state(
        db,
        id,
        &ConditionStatement::CheckCleared {
            consumer_id: consumer.into(),
        },
    )
    .await
    .unwrap();
}
const PHASES: [CheckWaitPhase; 3] = [
    CheckWaitPhase::Result,
    CheckWaitPhase::Slot,
    CheckWaitPhase::InfrastructureExhausted,
];

#[tokio::test]
async fn each_check_wait_is_stated_under_the_step_is_public_and_ends_with_its_owner() {
    let db = db().await;
    for (index, phase) in PHASES.into_iter().enumerate() {
        let id = format!("check-{index}");
        task(&db, &id).await;
        let statement = ConditionStatement::Check {
            wait: wait(phase, "consumer"),
            epoch: 0,
        };
        // Not without the Task's claimed step, and not through the legacy seam.
        assert!(matches!(
            try_state(&db, &id, &statement).await,
            Err(DbError::Check(_))
        ));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            producers::state(&mut tx, &id, &statement).await,
            Err(DbError::Check(_))
        ));
        tx.rollback().await.unwrap();
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            // Only the status entry that asked may state its wait.
            assert!(matches!(
                try_state(
                    &db,
                    &id,
                    &ConditionStatement::Check {
                        wait: wait(phase, "consumer"),
                        epoch: 7,
                    },
                )
                .await,
                Err(DbError::VersionConflict)
            ));
            let before = read(&db, &id).await;
            state(&db, &id, phase, "consumer").await;
            let stated = read(&db, &id).await;
            assert_eq!(stated.version, before.version, "a statement bumps no version");
            assert_eq!(
                (&stated.error_annotation, &stated.metadata_json),
                (&before.error_annotation, &before.metadata_json),
                "no legacy marker field is written"
            );
            assert_eq!(stated.condition.check_wait(), Some(&wait(phase, "consumer")));
            let exhausted = phase == CheckWaitPhase::InfrastructureExhausted;
            // A result or slot wait is owned work, not a blocker.
            assert_eq!(stated.condition.is_blocked(), exhausted);
            assert!(!material_blocker(&stated.condition).requires_intervention || exhausted);
            // Visible through the public condition, with its owner and recovery.
            let public = serde_json::to_value(stated.condition.public()).unwrap();
            assert_eq!(public["kind"], "parked");
            assert_eq!(
                public["primary"],
                json!({"kind":"check","wait":{"phase":phase,"consumer_id":"consumer","origin":"entry"}})
            );
            assert_eq!(
                json!([public["details"]["owner"], public["details"]["recovery"]]),
                if exhausted {
                    json!(["user", "retry_check"])
                } else {
                    json!(["check_runner", "wait_for_check"])
                }
            );
            assert_eq!(public["details"]["failed"], false);
            db.check_task_condition_invariant(&stated).await.unwrap();

            // Another consumer cannot end this wait; its own consumer does.
            clear(&db, &id, "someone-else").await;
            assert_eq!(
                read(&db, &id).await.condition.check_wait(),
                Some(&wait(phase, "consumer"))
            );
            clear(&db, &id, "consumer").await;
            let cleared = read(&db, &id).await;
            assert!(cleared.condition.check_witness().is_none());
            assert!(matches!(cleared.condition, TaskCondition::Clear { .. }));
            db.check_task_condition_invariant(&cleared).await.unwrap();
        })
        .await;
    }
}

#[tokio::test]
async fn a_check_wait_moves_between_phases_and_survives_every_unrelated_write() {
    let db = db().await;
    for (index, phase) in PHASES.into_iter().enumerate() {
        let id = format!("carry-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            // Slot wait -> slot freed -> (exhausted) -> retry: one witness,
            // restated in place by the same consumer.
            state(&db, &id, CheckWaitPhase::Slot, "consumer").await;
            state(&db, &id, CheckWaitPhase::Result, "consumer").await;
            state(&db, &id, phase, "consumer").await;
            let expected = wait(phase, "consumer");
            // Every other producer family restates the row around it.
            for change in [
                ConditionChange::Legacy,
                ConditionChange::Human,
                ConditionChange::Hooks,
                ConditionChange::Execution,
                ConditionChange::Budget,
                ConditionChange::Operations,
                ConditionChange::Children,
                ConditionChange::Entry,
                ConditionChange::Full,
            ] {
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                db.produce_condition_in_tx(&mut tx, &id, change)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let current = read(&db, &id).await;
                assert_eq!(current.condition.check_wait(), Some(&expected), "{change:?}");
                db.check_task_condition_invariant(&current).await.unwrap();
            }
            // An ordinary Task write (a title edit) bumps the version only.
            let current = read(&db, &id).await;
            let edited = TaskRepo::update(
                &db,
                crate::UpdateTask {
                    id: id.clone(),
                    expected_version: current.version,
                    title: Some("renamed".into()),
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: None,
                    blocked_json: None,
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: crate::now_rfc3339(),
                },
            )
            .await
            .unwrap();
            assert_eq!(edited.condition.check_wait(), Some(&expected));
            // An owner hold stands in front of it; releasing shows it again.
            super::statement_tests::hold(&db, &id, "owner hold").await;
            let held = read(&db, &id).await;
            assert!(matches!(
                &held.condition,
                TaskCondition::Parked {
                    primary: ParkReason::Held { .. },
                    ..
                }
            ));
            assert_eq!(held.condition.check_wait(), Some(&expected));
            super::statement_tests::release(&db, &id).await;
            let released = read(&db, &id).await;
            assert!(matches!(
                &released.condition,
                TaskCondition::Parked {
                    primary: ParkReason::Check { .. },
                    ..
                }
            ));
            // A missed producer is repaired without losing the wait.
            sqlx::query("UPDATE task SET condition_json=json_set(condition_json,'$.since','tampered') WHERE id=?")
                .bind(&id)
                .execute(db.pool())
                .await
                .unwrap();
            db.check_task_conditions_of(std::slice::from_ref(&id))
                .await
                .unwrap();
            let repaired = read(&db, &id).await;
            assert_eq!(repaired.condition.check_wait(), Some(&expected));
            db.check_task_condition_invariant(&repaired).await.unwrap();
        })
        .await;
    }
}

#[tokio::test]
async fn a_check_wait_ends_when_the_task_leaves_the_status_entry_that_asked() {
    let db = db().await;
    for (index, (status, settled)) in [("working", false), ("cancelled", true)]
        .into_iter()
        .enumerate()
    {
        let id = format!("leave-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            state(&db, &id, CheckWaitPhase::Result, "consumer").await;
        })
        .await;
        // Sent back or cancelled while the check runs: a new status entry.
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET status=?,status_epoch=status_epoch+1 WHERE id=?")
            .bind(status)
            .bind(&id)
            .execute(&mut *tx)
            .await
            .unwrap();
        db.produce_condition_in_tx(&mut tx, &id, ConditionChange::Entry)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let moved = read(&db, &id).await;
        assert!(moved.condition.check_wait().is_none(), "{status}");
        assert!(moved.condition.check_witness().is_none(), "{status}");
        assert_eq!(
            matches!(moved.condition, TaskCondition::Settled { .. }),
            settled && moved.condition.evidence().witnesses.len() == 1,
            "{status}: {:?}",
            moved.condition
        );
        db.check_task_condition_invariant(&moved).await.unwrap();
    }
}

#[tokio::test]
async fn a_check_wait_is_durable_across_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", temp.path().join("forge.sqlite").display());
    let expected = wait(CheckWaitPhase::Slot, "consumer");
    {
        let pool = crate::create_sqlite_pool(&url).await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        crate::ProjectRepo::create(
            &db,
            crate::CreateProject {
                id: "p".into(),
                owner_id: None,
                name: "Conditions".into(),
                primary_repo_id: None,
                updated_at: crate::now_rfc3339(),
                settings: "{}".into(),
                workflow_definition: "{}".into(),
                created_at: crate::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        task(&db, "restart").await;
        let step = claim(&db, "restart").await;
        crate::task_writer::in_task_step(step, async {
            state(&db, "restart", CheckWaitPhase::Slot, "consumer").await;
        })
        .await;
        db.pool().close().await;
    }
    // The server stopped with the step still claimed. A new process reads
    // the same wait, and the startup recompute of every row keeps it.
    let pool = crate::create_sqlite_pool(&url).await.unwrap();
    crate::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    assert_eq!(
        read(&db, "restart").await.condition.check_wait(),
        Some(&expected)
    );
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, "restart").await.unwrap();
    tx.commit().await.unwrap();
    let recomputed = read(&db, "restart").await;
    assert_eq!(recomputed.condition.check_wait(), Some(&expected));
    db.check_task_condition_invariant(&recomputed)
        .await
        .unwrap();
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}
