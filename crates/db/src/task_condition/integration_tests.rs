use super::tests::{claim, db, task};
use super::*;
use crate::{TaskRepo, TaskStepRepo};
use serde_json::json;

pub(super) fn reasons() -> Vec<IntegrationReason> {
    let id = || IntegrationAttemptId::new("attempt");
    let mut reasons = vec![
        IntegrationReason::Waiting {
            attempt_id: id(),
            blocked_by: vec![IntegrationAttemptId::new("earlier")],
        },
        IntegrationReason::Owned {
            attempt_id: id(),
            phase: IntegrationPhase::Checking,
        },
        IntegrationReason::Repair {
            attempt_id: id(),
            conflict_paths: Some(vec!["src/a.rs".into()]),
            repair_paths: vec!["Cargo.lock".into()],
            predecessor_attempt_id: Some(IntegrationAttemptId::new("predecessor")),
        },
        IntegrationReason::ReviewRequired {
            attempt_id: id(),
            authority_reason: "outside reviewed paths".into(),
        },
        IntegrationReason::CandidateCheckFailed {
            attempt_id: id(),
            check: "unit".into(),
            message: "candidate exit 1".into(),
        },
        IntegrationReason::Deferred {
            attempt_id: id(),
            cause: IntegrationDeferralCause::Infrastructure,
            owner_id: Some("machine".into()),
            message: "transport unavailable".into(),
            retry_at: Some("2099-01-01T00:00:00Z".into()),
        },
        IntegrationReason::Applied { attempt_id: id() },
    ];
    for phase in [
        IntegrationPhase::Validating,
        IntegrationPhase::Rebasing,
        IntegrationPhase::AwaitingCarry,
        IntegrationPhase::AwaitingAuthorization,
        IntegrationPhase::FastForwarding,
        IntegrationPhase::Reconciling,
    ] {
        reasons.push(IntegrationReason::Owned {
            attempt_id: id(),
            phase,
        });
    }
    for cause in [
        IntegrationDeferralCause::OwnerOffline,
        IntegrationDeferralCause::TargetDirty,
        IntegrationDeferralCause::BudgetExhausted,
        IntegrationDeferralCause::OwnerRequired,
        IntegrationDeferralCause::UnresolvedResult,
    ] {
        reasons.push(IntegrationReason::Deferred {
            attempt_id: id(),
            cause,
            owner_id: Some("machine".into()),
            message: "owner condition".into(),
            retry_at: None,
        });
    }
    reasons
}
async fn read(db: &SqliteDb, id: &str) -> Task {
    TaskRepo::get_by_id(db, id, false).await.unwrap().unwrap()
}
async fn state(db: &SqliteDb, id: &str, reason: &IntegrationReason) {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.state_integration_condition_in_tx(
        &mut tx,
        id,
        &ConditionStatement::Integration {
            reason: reason.clone(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
async fn raw(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn every_statement_requires_a_live_step_and_round_trips_without_legacy_writes() {
    let db = db().await;
    for (index, reason) in reasons().iter().enumerate() {
        let id = format!("statement-{index}");
        let before = task(&db, &id).await;
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_event")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let statement = ConditionStatement::Integration {
            reason: reason.clone(),
        };
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                .await,
            Err(DbError::Check(_))
        ));
        assert!(
            matches!(
                producers::state(&mut tx, &id, &statement).await,
                Err(DbError::Check(_))
            ),
            "the legacy statement seam cannot introduce integration ownership"
        );
        tx.rollback().await.unwrap();
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step.clone(), async {
            state(&db, &id, reason).await;
            let stored = db.task_condition(&id).await.unwrap();
            assert_eq!(stored.integration_reason(), Some(reason));
            assert!(stored.evidence().stated);
            assert!(
                stored.evidence().error_annotation.is_none()
                    && stored.evidence().metadata.is_empty()
            );
            assert_eq!(
                stored.public().details().blocked,
                reason.requires_intervention()
            );
            assert_eq!(stored.integration_wait(), Some(reason));
            assert_eq!(decode(&encode(&stored)).unwrap(), stored);
            assert_eq!(
                serde_json::from_value::<api_types::TaskCondition>(
                    serde_json::to_value(stored.public()).unwrap()
                )
                .unwrap(),
                stored.public()
            );
            let after = read(&db, &id).await;
            assert_eq!(
                LegacyConditionInput::from(&before),
                LegacyConditionInput::from(&after)
            );
            assert_eq!(
                (before.version, &before.updated_at),
                (after.version, &after.updated_at)
            );
            db.check_task_condition_invariant(&after).await.unwrap();
            // Clearing has the same lease requirement and an attempt fence.
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            assert!(matches!(
                db.state_integration_condition_in_tx(
                    &mut tx,
                    &id,
                    &ConditionStatement::IntegrationCleared {
                        attempt_id: IntegrationAttemptId::new("obsolete")
                    }
                )
                .await,
                Err(DbError::VersionConflict)
            ));
            tx.rollback().await.unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            db.state_integration_condition_in_tx(
                &mut tx,
                &id,
                &ConditionStatement::IntegrationCleared {
                    attempt_id: reason.attempt_id().clone(),
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            assert!(read(&db, &id)
                .await
                .condition
                .integration_reason()
                .is_none());
            // A task-local context with an expired/settled DB lease is refused.
            state(&db, &id, reason).await;
            let mut done = crate::begin_immediate(db.pool()).await.unwrap();
            db.finish_step_in_tx(&mut done, &step, "done", None)
                .await
                .unwrap();
            done.commit().await.unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            assert!(matches!(
                db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                    .await,
                Err(DbError::VersionConflict)
            ));
            let input = LegacyConditionInput::from(&after);
            let mut facts = ConditionFacts::load(&mut tx, &id).await.unwrap();
            facts.integration = Some(IntegrationReason::Applied {
                attempt_id: IntegrationAttemptId::new("replacement"),
            });
            assert!(
                matches!(
                    db.set_condition(
                        &mut tx,
                        &id,
                        after.version,
                        &input,
                        &facts.condition(&input)
                    )
                    .await,
                    Err(DbError::VersionConflict)
                ),
                "set_condition cannot bypass the claimed integration step fence"
            );
            tx.rollback().await.unwrap();
        })
        .await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.state_integration_condition_in_tx(
                &mut tx,
                &id,
                &ConditionStatement::IntegrationCleared {
                    attempt_id: reason.attempt_id().clone()
                }
            )
            .await,
            Err(DbError::Check(_))
        ));
        tx.rollback().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_event")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            events
        );
    }
}

#[tokio::test]
async fn survival_matrix_preserves_other_owners_primary_and_integration_lineage() {
    let db = db().await;
    let changes = [
        ConditionChange::Legacy,
        ConditionChange::Human,
        ConditionChange::Hooks,
        ConditionChange::Budget,
        ConditionChange::Entry,
        ConditionChange::Execution,
        ConditionChange::Operations,
        ConditionChange::Children,
        ConditionChange::Full,
    ];
    let reasons = reasons();
    for (index, (reason, blocked)) in reasons
        .iter()
        .flat_map(|reason| [(reason, false), (reason, true)])
        .enumerate()
    {
        let id = format!("survival-{index}");
        task(&db, &id).await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET status='merging',entry_barrier_json=? WHERE id=?")
            .bind(blocked.then(|| json!({"state":"merging","status":"blocked","blocking_reason":"other owner","started_at":"2026-10-07T00:00:00Z"}).to_string()))
            .bind(&id).execute(&mut *tx).await.unwrap();
        db.sync_condition_in_tx(&mut tx, &id).await.unwrap();
        tx.commit().await.unwrap();
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            state(&db, &id, reason).await;
            for change in changes {
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                db.produce_condition_in_tx(&mut tx, &id, change).await.unwrap();
                tx.commit().await.unwrap();
                let current = read(&db, &id).await;
                assert_eq!(current.condition.integration_reason(), Some(reason), "{index} {change:?}");
                if !blocked { assert_eq!(current.condition.public().details().blocked, reason.requires_intervention(), "{index} {change:?}: typed primary flags"); }
                assert!(matches!(&current.condition, TaskCondition::Parked { primary, .. }
                    if matches!(primary, ParkReason::EntryBlocked { .. }) == blocked
                    && (blocked || matches!(primary, ParkReason::Integration { .. }))), "{index} {change:?}");
                db.check_task_condition_invariant(&current).await.unwrap();
            }
            super::statement_tests::hold(&db, &id, "user hold").await;
            let held = read(&db, &id).await;
            assert!(matches!(held.condition, TaskCondition::Parked { primary: ParkReason::Held { .. }, .. }));
            assert_eq!(held.condition.integration_reason(), Some(reason));
            // Re-stating integration while held must leave the user's primary.
            state(&db, &id, reason).await;
            assert!(matches!(read(&db, &id).await.condition, TaskCondition::Parked { primary: ParkReason::Held { .. }, .. }));
            super::statement_tests::release(&db, &id).await;
            let released = read(&db, &id).await;
            assert!(matches!(&released.condition, TaskCondition::Parked { primary, .. }
                if matches!(primary, ParkReason::EntryBlocked { .. }) == blocked
                && (blocked || matches!(primary, ParkReason::Integration { .. }))));
            assert_eq!(released.condition.integration_reason(), Some(reason));
            // A missed legacy sync must be repaired without replacing the owner witness.
            sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.paused_integration',json(?)) WHERE id=?")
                .bind(json!({"state":"merging"}).to_string()).bind(&id).execute(db.pool()).await.unwrap();
            assert_eq!(db.check_task_conditions_of(std::slice::from_ref(&id)).await.unwrap(), 1);
            let repaired = read(&db, &id).await;
            assert_eq!(repaired.condition.integration_reason(), Some(reason));
            assert!(repaired.condition.reasons().any(|r| matches!(r, ParkReason::ProjectPaused { .. })));
            // Settlement removes scheduling parks, retaining the typed lineage.
            for terminal in ["done", "cancelled"] {
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                sqlx::query("UPDATE task SET status=?,status_epoch=status_epoch+1 WHERE id=?").bind(terminal).bind(&id).execute(&mut *tx).await.unwrap();
                db.produce_condition_in_tx(&mut tx, &id, ConditionChange::Entry).await.unwrap();
                tx.commit().await.unwrap();
                let settled = read(&db, &id).await;
                assert!(matches!(settled.condition, TaskCondition::Settled { .. }));
                assert_eq!(settled.condition.integration_reason(), Some(reason));
                assert_eq!(settled.condition.read().entry_recorded, blocked);
                db.check_task_condition_invariant(&settled).await.unwrap();
                assert_eq!(db.check_task_conditions_of(std::slice::from_ref(&id)).await.unwrap(), 0);
            }
        }).await;
    }
}

#[test]
fn real_handoff_owns_execution_while_integration_lineage_survives() {
    for reason in reasons() {
        let facts = ConditionFacts {
            task_id: "task".into(),
            state: "repair".into(),
            integration: Some(reason.clone()),
            integration_handoff_ready: reason.hands_off(),
            execution: Some(("run".into(), "coder".into())),
            ..Default::default()
        };
        let condition = facts.condition(&LegacyConditionInput::default());
        assert_eq!(condition.integration_reason(), Some(&reason));
        assert_eq!(
            matches!(condition, TaskCondition::Running { .. }),
            reason.hands_off()
        );
        assert_eq!(condition.integration_wait().is_none(), reason.hands_off());
    }
}

#[tokio::test]
async fn upgrade_and_unsupported_conditions_preserve_owner_statements() {
    let db = db().await;
    task(&db, "known").await;
    let step = claim(&db, "known").await;
    let reason = reasons().remove(0);
    crate::task_writer::in_task_step(step, state(&db, "known", &reason)).await;
    task(&db, "future").await;
    let unknown = r#"{"kind":"parked","primary":{"kind":"future_integration"},"additional":[],"resume":{"kind":"future"},"since":null,"evidence":{}}"#;
    sqlx::query("UPDATE task SET condition_json=? WHERE id='future'")
        .bind(unknown)
        .execute(db.pool())
        .await
        .unwrap();
    for change in [
        ConditionChange::Legacy,
        ConditionChange::Hooks,
        ConditionChange::Budget,
        ConditionChange::Entry,
        ConditionChange::Execution,
        ConditionChange::Full,
    ] {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.produce_condition_in_tx(&mut tx, "future", change)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(raw(&db, "future").await, unknown);
    }
    let step = claim(&db, "future").await;
    crate::task_writer::in_task_step(step, async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.state_integration_condition_in_tx(
                &mut tx,
                "future",
                &ConditionStatement::Integration {
                    reason: reason.clone()
                }
            )
            .await,
            Err(DbError::Check(_))
        ));
        tx.rollback().await.unwrap();
        super::statement_tests::hold(&db, "future", "hold").await;
        super::statement_tests::release(&db, "future").await;
    })
    .await;
    crate::SystemSettingRepo::set_setting(&db, MAPPING_REVISION_KEY, "4", &crate::now_rfc3339())
        .await
        .unwrap();
    while db.backfill_task_conditions_if_stale().await.unwrap() {}
    assert_eq!(raw(&db, "future").await, unknown);
    assert_eq!(
        read(&db, "known").await.condition.integration_reason(),
        Some(&reason)
    );
    assert!(matches!(
        read(&db, "future").await.condition,
        TaskCondition::Parked {
            primary: ParkReason::UnknownCondition { .. },
            ..
        }
    ));
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        vec!["future"]
    );
}

#[tokio::test]
async fn wait_and_owner_blocker_slots_are_active_and_parked_respectively() {
    let db = db().await;
    task(&db, "slots").await;
    sqlx::query("UPDATE task SET status='merging' WHERE id='slots'")
        .execute(db.pool())
        .await
        .unwrap();
    let step = claim(&db, "slots").await;
    crate::task_writer::in_task_step(step, async {
        let mut owner = reasons().remove(5);
        if let IntegrationReason::Deferred { cause, .. } = &mut owner {
            *cause = IntegrationDeferralCause::TargetDirty;
        }
        for (reason, counts) in [(reasons().remove(0), (1, 0, 0)), (owner, (0, 1, 0))] {
            state(&db, "slots", &reason).await;
            assert_eq!(
                db.count_project_slots(
                    "p",
                    r#"{"merging":{"kind":"gate","owns_work":true}}"#,
                    "{}"
                )
                .await
                .unwrap(),
                counts
            );
            let condition = read(&db, "slots").await.condition;
            assert_eq!(condition.is_blocked(), reason.requires_intervention());
            assert_eq!(
                material_blocker(&condition).requires_intervention,
                reason.requires_intervention()
            );
        }
    })
    .await;
}

#[test]
fn progress_digest_excludes_operational_state_and_rejects_operational_fields() {
    let mut baseline = None;
    for reason in reasons()
        .into_iter()
        .filter(|reason| !reason.requires_intervention())
    {
        let condition = ConditionFacts {
            task_id: "task".into(),
            state: "merging".into(),
            integration: Some(reason),
            ..Default::default()
        }
        .condition(&LegacyConditionInput::default());
        let material = material_blocker(&condition);
        assert!(!material.requires_intervention);
        assert!(material.interruption.is_none());
        assert_eq!(baseline.get_or_insert_with(|| material.clone()), &material);
        for field in [
            "queue_position",
            "queue_revision",
            "lease_until",
            "worker_token",
            "phase_started_at",
            "retry_tick",
        ] {
            let mut value = serde_json::to_value(&condition).unwrap();
            value["primary"]["reason"][field] = json!(42);
            assert!(
                decode(&value.to_string()).is_err(),
                "{field}: unsupported state must quarantine"
            );
        }
    }
}

#[test]
fn malformed_integration_witnesses_are_not_interpreted() {
    for reason in [
        IntegrationReason::Applied {
            attempt_id: IntegrationAttemptId::new(""),
        },
        IntegrationReason::Waiting {
            attempt_id: IntegrationAttemptId::new("self"),
            blocked_by: vec![IntegrationAttemptId::new("self")],
        },
        IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new("attempt"),
            conflict_paths: Some(vec!["../outside".into()]),
            repair_paths: vec![],
            predecessor_attempt_id: None,
        },
    ] {
        let condition = ConditionFacts {
            task_id: "task".into(),
            integration: Some(reason),
            ..Default::default()
        }
        .condition(&LegacyConditionInput::default());
        assert!(decode(&encode(&condition)).is_err());
    }
}

#[test]
fn unsupported_owner_witness_fields_are_quarantined() {
    let condition = ConditionFacts {
        task_id: "task".into(),
        integration: Some(IntegrationReason::Applied {
            attempt_id: IntegrationAttemptId::new("attempt"),
        }),
        ..Default::default()
    }
    .condition(&LegacyConditionInput::default());
    let mut raw = serde_json::to_value(&condition).unwrap();
    raw["evidence"]["witnesses"][1]["future_guard"] = json!("unknown ownership");
    assert!(decode(&raw.to_string()).is_err());
}

#[tokio::test]
async fn clearing_integration_restores_another_owners_deferred_continuation() {
    let db = db().await;
    task(&db, "deferred-owner").await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET metadata_json=? WHERE id='deferred-owner'")
        .bind(json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"other owner retry","target_state":"in_progress"}}).to_string())
        .execute(&mut *tx).await.unwrap();
    db.sync_condition_in_tx(&mut tx, "deferred-owner")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let baseline = read(&db, "deferred-owner").await.condition;
    assert!(matches!(baseline, TaskCondition::Deferred { .. }));
    let step = claim(&db, "deferred-owner").await;
    crate::task_writer::in_task_step(step, async {
        state(&db, "deferred-owner", &reasons().remove(0)).await;
        assert_eq!(
            read(&db, "deferred-owner").await.condition.read().retry,
            baseline.read().retry
        );
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.state_integration_condition_in_tx(
            &mut tx,
            "deferred-owner",
            &ConditionStatement::IntegrationCleared {
                attempt_id: IntegrationAttemptId::new("attempt"),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            read(&db, "deferred-owner").await.condition.typed(),
            baseline.typed()
        );
    })
    .await;
}

#[tokio::test]
async fn handed_off_repair_and_review_are_ready_without_a_fake_running_execution() {
    let db = db().await;
    for (i, reason) in reasons()
        .into_iter()
        .filter(IntegrationReason::hands_off)
        .enumerate()
    {
        let id = format!("handoff-{i}");
        task(&db, &id).await;
        let statement = ConditionStatement::IntegrationHandedOff {
            attempt_id: reason.attempt_id().clone(),
        };
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        assert!(matches!(
            db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                .await,
            Err(DbError::Check(_))
        ));
        tx.rollback().await.unwrap();
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step.clone(), async {
            state(&db, &id, &reason).await;
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            assert!(matches!(
                db.state_integration_condition_in_tx(
                    &mut tx,
                    &id,
                    &ConditionStatement::IntegrationHandedOff {
                        attempt_id: IntegrationAttemptId::new("obsolete")
                    }
                )
                .await,
                Err(DbError::VersionConflict)
            ));
            tx.rollback().await.unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            sqlx::query(
                "UPDATE task SET status='merge_failed',status_epoch=status_epoch+1 WHERE id=?",
            )
            .bind(&id)
            .execute(&mut *tx)
            .await
            .unwrap();
            db.produce_condition_in_tx(&mut tx, &id, ConditionChange::Entry)
                .await
                .unwrap();
            db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            for change in [
                ConditionChange::Legacy,
                ConditionChange::Human,
                ConditionChange::Hooks,
                ConditionChange::Budget,
                ConditionChange::Entry,
                ConditionChange::Execution,
                ConditionChange::Operations,
                ConditionChange::Children,
                ConditionChange::Full,
            ] {
                let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
                db.produce_condition_in_tx(&mut tx, &id, change)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                let condition = read(&db, &id).await.condition;
                assert!(
                    matches!(condition, TaskCondition::Clear { .. }),
                    "{change:?}: {condition:?}"
                );
                assert!(condition.integration_handoff_ready());
                assert_eq!(condition.integration_reason(), Some(&reason));
                assert!(condition.integration_wait().is_none());
                db.check_task_condition_invariant(&read(&db, &id).await)
                    .await
                    .unwrap();
            }
            super::statement_tests::hold(&db, &id, "hold delegated work").await;
            assert!(read(&db, &id).await.condition.integration_handoff_ready());
            super::statement_tests::release(&db, &id).await;
            assert!(matches!(
                read(&db, &id).await.condition,
                TaskCondition::Clear { .. }
            ));
            let mut refreshed = reason.clone();
            if let IntegrationReason::Repair { repair_paths, .. } = &mut refreshed {
                repair_paths.push("src/touched.rs".into());
            }
            state(&db, &id, &refreshed).await;
            assert!(
                read(&db, &id).await.condition.integration_handoff_ready(),
                "path updates keep delegation"
            );
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            sqlx::query("UPDATE task SET status='done',status_epoch=status_epoch+1 WHERE id=?")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .unwrap();
            db.produce_condition_in_tx(&mut tx, &id, ConditionChange::Entry)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let settled = read(&db, &id).await;
            assert!(matches!(settled.condition, TaskCondition::Settled { .. }));
            assert!(settled.condition.integration_handoff_ready());
            assert_eq!(
                db.check_task_conditions_of(std::slice::from_ref(&id))
                    .await
                    .unwrap(),
                0
            );
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            db.finish_step_in_tx(&mut tx, &step, "done", None)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            assert!(matches!(
                db.state_integration_condition_in_tx(&mut tx, &id, &statement)
                    .await,
                Err(DbError::VersionConflict)
            ));
            tx.rollback().await.unwrap();
        })
        .await;
    }
}
