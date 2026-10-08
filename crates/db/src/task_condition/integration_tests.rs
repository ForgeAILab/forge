use super::tests::{claim, db, task};
use super::*;
use crate::{TaskRepo, TaskStepRepo};
use serde_json::json;

pub(super) fn reasons() -> Vec<IntegrationReason> {
    let id = || IntegrationAttemptId::new("attempt");
    let mut reasons = vec![
        IntegrationReason::Waiting { attempt_id: id() },
        IntegrationReason::Owned {
            attempt_id: id(),
            phase: IntegrationPhase::Checking,
        },
        IntegrationReason::Repair {
            attempt_id: id(),
            predecessor_attempt_id: Some(IntegrationAttemptId::new("predecessor")),
            conflict_paths: Some(IntegrationPaths::bounded(["src/a.rs".to_owned()])),
            repair_paths: IntegrationPaths::bounded(["Cargo.lock".to_owned()]),
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
            // A task-local context whose step has settled is refused. (An
            // expired lease is `an_expired_lease_is_refused_unless_...`.)
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
                    Err(DbError::Check(_))
                ),
                "set_condition cannot bypass the claimed integration step fence, and refuses with a check failure so no caller answers it by writing unfenced"
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
                // A settled Task keeps the attempt identity and nothing else.
                assert_eq!(settled.condition.integration_reason(), None);
                assert_eq!(settled.condition.integration_attempt(), Some(reason.attempt_id()));
                assert!(!raw(&db, &id).await.contains("paths"));
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
        // Handed off or not, a live execution reads as Running: the Task is
        // never presented as parked on integration while it runs.
        assert!(matches!(condition, TaskCondition::Running { .. }));
        assert!(condition.integration_wait().is_none());
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
            Err(DbError::TaskConditionQuarantined { .. })
        ));
        tx.rollback().await.unwrap();
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
        IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new("self"),
            predecessor_attempt_id: Some(IntegrationAttemptId::new("self")),
            conflict_paths: None,
            repair_paths: IntegrationPaths::default(),
        },
        IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new("attempt"),
            predecessor_attempt_id: None,
            conflict_paths: Some(IntegrationPaths::bounded(["../outside".to_owned()])),
            repair_paths: IntegrationPaths::default(),
        },
        // More paths than the sample holds, and a count that disagrees.
        IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new("attempt"),
            predecessor_attempt_id: None,
            conflict_paths: None,
            repair_paths: IntegrationPaths {
                count: 40,
                paths: (0..40).map(|i| format!("src/{i}.rs")).collect(),
                truncated: false,
            },
        },
        IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new("attempt"),
            predecessor_attempt_id: None,
            conflict_paths: None,
            repair_paths: IntegrationPaths {
                count: 1,
                paths: vec!["src/a.rs".into(), "src/b.rs".into()],
                truncated: false,
            },
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
                *repair_paths = IntegrationPaths::bounded(
                    repair_paths
                        .paths
                        .iter()
                        .cloned()
                        .chain(["src/touched.rs".to_owned()]),
                );
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
            assert!(!settled.condition.integration_handoff_ready());
            assert_eq!(
                settled.condition.integration_attempt(),
                Some(reason.attempt_id())
            );
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

/// One reason of each behaviour: an ordinary wait, a repair that carries
/// lineage, and an owner blocker.
fn sample_reasons() -> Vec<IntegrationReason> {
    let mut reasons = reasons();
    let blocker = reasons
        .iter()
        .find(|reason| reason.requires_intervention())
        .unwrap()
        .clone();
    vec![reasons.remove(0), reasons.remove(1), blocker]
}
async fn produce(db: &SqliteDb, id: &str, change: ConditionChange) -> TaskCondition {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.produce_condition_in_tx(&mut tx, id, change)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let current = read(db, id).await;
    db.check_task_condition_invariant(&current).await.unwrap();
    current.condition
}
fn has_witness(condition: &TaskCondition, found: impl Fn(&ConditionWitness) -> bool) -> bool {
    condition.evidence().witnesses.iter().any(found)
}

/// The Hooks, Execution, Budget, Operations and Children producers each
/// state a real fact of their own beside an integration reason. Every case
/// asserts both: the integration reason, and the other owner's fact. Dropping
/// either fails it.
#[tokio::test]
async fn each_producer_states_its_own_fact_beside_the_integration_reason() {
    use super::producer_tests::{enqueue, pending_cancel, set_parent, workspace};
    let db = db().await;
    for (index, reason) in sample_reasons().iter().enumerate() {
        let waits = Some(reason);
        // Hooks: a pending entry hooks step. Integration stays the park; the
        // step keeps its witness and reads as itself once integration clears.
        let id = format!("hooks-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            state(&db, &id, reason).await;
            let hook = enqueue(&db, &id, "hooks", "todo", "{}").await;
            let condition = produce(&db, &id, ConditionChange::Hooks).await;
            assert_eq!(condition.integration_wait(), waits, "hooks {index}");
            assert!(
                has_witness(
                    &condition,
                    |w| matches!(w, ConditionWitness::Step { step_id, .. } if *step_id == hook)
                ),
                "hooks {index}: {condition:?}"
            );
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
            assert!(
                matches!(read(&db, &id).await.condition, TaskCondition::Entering { step_id, .. } if step_id == hook),
                "hooks {index}: the entry step is the condition again"
            );
        })
        .await;

        // Execution: a live run. The Task reads as Running, never as parked
        // on integration, and is parked on integration again when it ends.
        let id = format!("execution-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            state(&db, &id, reason).await;
            sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','running',?,?)")
                .bind(format!("run-{index}")).bind(&id).bind(crate::now_rfc3339()).bind(crate::now_rfc3339())
                .execute(db.pool()).await.unwrap();
            let condition = produce(&db, &id, ConditionChange::Execution).await;
            assert!(
                matches!(&condition, TaskCondition::Running { execution_id, .. } if *execution_id == format!("run-{index}")),
                "execution {index}: {condition:?}"
            );
            assert_eq!(condition.integration_reason(), waits, "execution {index}");
            assert_eq!(condition.integration_wait(), None);
            assert_eq!(
                serde_json::to_value(condition.public()).unwrap()["kind"],
                "running"
            );
            sqlx::query("UPDATE execution SET status='completed' WHERE task_id=?")
                .bind(&id)
                .execute(db.pool())
                .await
                .unwrap();
            let condition = produce(&db, &id, ConditionChange::Execution).await;
            assert_eq!(condition.integration_wait(), waits, "execution {index}");
        })
        .await;

        // Budget: an exhausted ledger. The exhaustion parks first; the
        // integration wait is retained behind it.
        let id = format!("budget-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step.clone(), async {
            state(&db, &id, reason).await;
            crate::task_writer::TaskQuery::new(
                &db,
                &id,
                "UPDATE task SET blocked_json=? WHERE id=?",
            )
            .bind(json!({"kind":"retry_exhausted"}).to_string())
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            crate::budget::charge(&mut tx, &id, "execution", 3, &step.id)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            let condition = produce(&db, &id, ConditionChange::Budget).await;
            assert!(
                condition.budget_exhausted(),
                "budget {index}: {condition:?}"
            );
            assert!(
                has_witness(
                    &condition,
                    |w| matches!(w, ConditionWitness::Budget { key, .. } if key == "execution")
                ),
                "budget {index}"
            );
            assert!(
                !matches!(
                    &condition,
                    TaskCondition::Parked {
                        primary: ParkReason::Integration { .. },
                        ..
                    }
                ),
                "budget {index}: the exhaustion is primary"
            );
            assert_eq!(condition.integration_wait(), waits, "budget {index}");
        })
        .await;

        // Operations: an unconfirmed remote cancellation.
        let id = format!("operations-{index}");
        task(&db, &id).await;
        let step = claim(&db, &id).await;
        let _directory = workspace(&db, &format!("w-{index}"), &id).await;
        crate::task_writer::in_task_step(step.clone(), async {
            state(&db, &id, reason).await;
            let operation =
                pending_cancel(&db, &format!("op-{index}"), &step.id, &format!("w-{index}")).await;
            let condition = produce(&db, &id, ConditionChange::Operations).await;
            assert!(
                condition
                    .reasons()
                    .any(|r| matches!(r, ParkReason::RemoteCancelPending { .. })),
                "operations {index}: {condition:?}"
            );
            assert!(has_witness(
                &condition,
                |w| matches!(w, ConditionWitness::Operation { operation_id, .. } if *operation_id == format!("op-{index}"))
            ));
            assert_eq!(condition.integration_wait(), waits, "operations {index}");
            db.acknowledge_remote_cancel(&operation).await.unwrap();
            let condition = read(&db, &id).await.condition;
            assert!(!condition
                .reasons()
                .any(|r| matches!(r, ParkReason::RemoteCancelPending { .. })));
            assert_eq!(condition.integration_wait(), waits, "operations {index}");
        })
        .await;

        // Children: a coordination root waiting on an unsettled child.
        let (id, child) = (format!("children-{index}"), format!("child-{index}"));
        task(&db, &id).await;
        task(&db, &child).await;
        set_parent(&db, &id, &[&child]).await;
        let step = claim(&db, &id).await;
        crate::task_writer::in_task_step(step, async {
            state(&db, &id, reason).await;
            TaskRepo::mutate_metadata(
                &db,
                &id,
                None,
                vec![crate::TaskMetadataMutation::Set {
                    key: "coordination_review_pending".into(),
                    value: json!(true),
                }],
                &crate::now_rfc3339(),
            )
            .await
            .unwrap();
            let condition = produce(&db, &id, ConditionChange::Children).await;
            assert!(
                has_witness(
                    &condition,
                    |w| matches!(w, ConditionWitness::Child { task_id, settled: false, .. } if *task_id == child)
                ),
                "children {index}: {condition:?}"
            );
            assert!(
                condition
                    .reasons()
                    .any(|r| matches!(r, ParkReason::Children { remaining, .. } if remaining.contains(&child))),
                "children {index}: {condition:?}"
            );
            assert_eq!(condition.integration_wait(), waits, "children {index}");
        })
        .await;
    }
    assert!(db.task_condition_violations().await.unwrap().is_empty());
}

/// How integration lies over each lifecycle condition. A live execution
/// keeps its Running presentation. An entry hooks step and a retry timer are
/// the Task step's own bookkeeping with nothing running: integration parks
/// over them, and each returns when integration is cleared.
#[test]
fn overlay_of_running_entering_and_deferred() {
    let wait = reasons().remove(0);
    let facts = |hooks: bool, execution: bool| ConditionFacts {
        task_id: "task".into(),
        state: "merging".into(),
        integration: Some(wait.clone()),
        hooks: hooks.then(|| "hook".to_owned()),
        execution: execution.then(|| ("run".to_owned(), "coder".to_owned())),
        ..Default::default()
    };
    let none = LegacyConditionInput::default();
    let timer = super::tests::input(
        None,
        None,
        None,
        None,
        Some(
            json!({"deferred_dispatch":{"not_before":"2099-01-01T00:00:00Z","reason":"retry","target_state":"merging"}}),
        ),
    );
    let without = |mut facts: ConditionFacts, input: &LegacyConditionInput| {
        facts.integration = None;
        facts.condition(input)
    };
    // Running: untouched, with the reason kept on the evidence.
    let running = facts(false, true).condition(&none);
    assert!(matches!(running, TaskCondition::Running { .. }));
    assert_eq!(running.integration_reason(), Some(&wait));
    assert_eq!(running.integration_wait(), None);
    assert_eq!(
        serde_json::to_value(running.public()).unwrap()["kind"],
        "running"
    );
    integration::validate(&running).unwrap();
    // Entering: parked on integration; the step witness is retained.
    assert!(matches!(
        without(facts(true, false), &none),
        TaskCondition::Entering { .. }
    ));
    let entering = facts(true, false).condition(&none);
    assert!(matches!(
        entering,
        TaskCondition::Parked {
            primary: ParkReason::Integration { .. },
            ..
        }
    ));
    assert!(has_witness(
        &entering,
        |w| matches!(w, ConditionWitness::Step { step_id, .. } if step_id == "hook")
    ));
    // Deferred: parked on integration; the timer is still read from the
    // legacy column and is the condition again without integration.
    assert!(matches!(
        without(facts(false, false), &timer),
        TaskCondition::Deferred { .. }
    ));
    let deferred = facts(false, false).condition(&timer);
    assert!(matches!(
        deferred,
        TaskCondition::Parked {
            primary: ParkReason::Integration { .. },
            ..
        }
    ));
    assert_eq!(
        deferred.read().retry,
        without(facts(false, false), &timer).read().retry
    );
}

/// The attempt fence. An attempt restates its own reason; another attempt
/// takes the row only when none owns it, after a clear or after a handoff.
#[tokio::test]
async fn an_integration_statement_is_fenced_on_its_attempt() {
    let db = db().await;
    task(&db, "fenced").await;
    let step = claim(&db, "fenced").await;
    let reason = |attempt: &str, phase| IntegrationReason::Owned {
        attempt_id: IntegrationAttemptId::new(attempt),
        phase,
    };
    let try_state = |reason: IntegrationReason| {
        let db = &db;
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let result = db
                .state_integration_condition_in_tx(
                    &mut tx,
                    "fenced",
                    &ConditionStatement::Integration { reason },
                )
                .await;
            if result.is_ok() {
                tx.commit().await.unwrap();
            }
            result
        }
    };
    crate::task_writer::in_task_step(step, async {
        try_state(reason("first", IntegrationPhase::Validating))
            .await
            .unwrap();
        // The owning attempt restates freely.
        try_state(reason("first", IntegrationPhase::Checking))
            .await
            .unwrap();
        // A different attempt, stale or new, cannot replace it.
        let before = raw(&db, "fenced").await;
        assert!(matches!(
            try_state(reason("second", IntegrationPhase::Validating)).await,
            Err(DbError::Check(_))
        ));
        assert_eq!(raw(&db, "fenced").await, before);
        // Once the first is cleared, the second may take the row.
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.state_integration_condition_in_tx(
            &mut tx,
            "fenced",
            &ConditionStatement::IntegrationCleared {
                attempt_id: IntegrationAttemptId::new("first"),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        try_state(reason("second", IntegrationPhase::Validating))
            .await
            .unwrap();
        // And the stale first attempt cannot come back over it.
        assert!(matches!(
            try_state(reason("first", IntegrationPhase::Checking)).await,
            Err(DbError::Check(_))
        ));
        // A handed-off attempt no longer owns the row either.
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.state_integration_condition_in_tx(
            &mut tx,
            "fenced",
            &ConditionStatement::IntegrationCleared {
                attempt_id: IntegrationAttemptId::new("second"),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let repair = |attempt: &str| IntegrationReason::Repair {
            attempt_id: IntegrationAttemptId::new(attempt),
            predecessor_attempt_id: None,
            conflict_paths: None,
            repair_paths: IntegrationPaths::default(),
        };
        try_state(repair("third")).await.unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.state_integration_condition_in_tx(
            &mut tx,
            "fenced",
            &ConditionStatement::IntegrationHandedOff {
                attempt_id: IntegrationAttemptId::new("third"),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        try_state(reason("fourth", IntegrationPhase::Validating))
            .await
            .unwrap();
        assert_eq!(
            read(&db, "fenced").await.condition.integration_attempt(),
            Some(&IntegrationAttemptId::new("fourth"))
        );
    })
    .await;
}

/// The lease check is the generic fence's (`fence_hook_in_tx`): a claimed
/// step whose stored lease has expired is refused, unless this process still
/// holds the step as active, which stands in for the lease it is renewing.
/// A step claimed for another Task is refused outright.
#[tokio::test]
async fn an_expired_lease_is_refused_unless_this_process_still_holds_the_step() {
    let db = db().await;
    task(&db, "lease").await;
    task(&db, "other").await;
    let step = claim(&db, "lease").await;
    let statement = ConditionStatement::Integration {
        reason: reasons().remove(0),
    };
    let stated = |task_id: &'static str| {
        let (db, statement) = (&db, &statement);
        async move {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let result = db
                .state_integration_condition_in_tx(&mut tx, task_id, statement)
                .await;
            tx.rollback().await.unwrap();
            result
        }
    };
    crate::task_writer::in_task_step(step.clone(), async {
        stated("lease").await.unwrap();
        // The step of one Task states nothing about another.
        assert!(matches!(stated("other").await, Err(DbError::Check(_))));
        sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
            .bind(&step.id)
            .execute(db.pool())
            .await
            .unwrap();
        // Expired, and nothing in this process holds the step: refused.
        assert!(matches!(
            stated("lease").await,
            Err(DbError::VersionConflict)
        ));
        // Expired in the row, but held as active here: accepted, exactly as
        // the generic fence accepts it.
        let held = db.hold_task_step(&step);
        stated("lease").await.unwrap();
        drop(held);
        assert!(matches!(
            stated("lease").await,
            Err(DbError::VersionConflict)
        ));
    })
    .await;
}

/// Lineage is bounded in the condition: a count, at most
/// `INTEGRATION_PATH_SAMPLE` paths and a truncation mark per path set.
#[tokio::test]
async fn repair_lineage_is_bounded_in_the_condition() {
    let db = db().await;
    task(&db, "lineage").await;
    let paths = IntegrationPaths::bounded((0..500).map(|i| format!("src/generated/{i}.rs")));
    assert_eq!(
        (paths.count, paths.paths.len(), paths.truncated),
        (500, INTEGRATION_PATH_SAMPLE, true)
    );
    let small = IntegrationPaths::bounded(["a".to_owned()]);
    assert_eq!((small.count, small.truncated), (1, false));
    let reason = IntegrationReason::Repair {
        attempt_id: IntegrationAttemptId::new("attempt"),
        predecessor_attempt_id: Some(IntegrationAttemptId::new("predecessor")),
        conflict_paths: Some(paths.clone()),
        repair_paths: paths,
    };
    let step = claim(&db, "lineage").await;
    crate::task_writer::in_task_step(step, state(&db, "lineage", &reason)).await;
    let stored = raw(&db, "lineage").await;
    assert_eq!(
        read(&db, "lineage").await.condition.integration_reason(),
        Some(&reason)
    );
    // Reason and witness each carry two samples of 32 short paths.
    assert!(stored.len() < 8 * 1024, "{} bytes", stored.len());
}
