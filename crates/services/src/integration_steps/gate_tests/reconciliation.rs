//! Integration admission reconciliation and continuation ownership.
use super::*;

/// A Task in its merge state with no attempt and no entry step (the entry
/// hook returned `Skipped`, the import did not take it, a manual move
/// entered the state): the dispatcher's sweep admits it through the hook's
/// own admission function and it merges. Nothing else is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_dispatcher_admits_a_merging_task_that_nothing_drives() {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    world.touch("t").await;
    assert!(world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .is_none());
    let resolved = dispatcher(&world)
        .await
        .resolved_for_test("t")
        .await
        .unwrap();
    assert!(
        resolved.starts_with("Step(AdmitIntegration)"),
        "the scheduler resolves the Task to the reconciler's step: {resolved}"
    );
    let attempt = match world.reconcile_until_admitted("t").await {
        Some(attempt) => attempt,
        None => panic!(
            "not admitted: {}\n{}",
            dispatcher(&world)
                .await
                .resolved_for_test("t")
                .await
                .unwrap(),
            world.dump("t").await
        ),
    };
    assert_eq!(attempt.admission_key, "admit:t:0");
    assert!(integration(&world.task("t").await)
        .to_string()
        .contains("waiting"));
    // A second lap admits nothing more: the same row.
    dispatcher(&world).await.reconcile_all().await.unwrap();
    let again = world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.id, attempt.id);
    let production = Production::new(&world);
    merged(&world, "t", &production).await;
    assert_eq!(world.merges("t").await, 1);
    // While the import is pending nothing is admitted.
    world.legacy_task("late", "late.txt", "merging").await;
    world.seed_execution("late", "executor", None).await;
    world.touch("late").await;
    world
        .service
        .integration_import_pending()
        .store(true, std::sync::atomic::Ordering::Release);
    dispatcher(&world).await.reconcile_all().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(world
        .db
        .current_integration_attempt("late")
        .await
        .unwrap()
        .is_none());
}

/// Importer rows that wait for the Task's owner (P13, P15, P16, P17), after
/// the owner's action lifted the block IN PLACE (the Task never left its
/// merge state, so no entry hook runs again): while the block stands the
/// reconciler admits nothing; once it is lifted the next sweep admits the
/// Task and it merges, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_owner_waits_are_admitted_by_the_sweep_once_their_block_is_lifted_in_place() {
    let rows: [(u64, &str, &str); 4] = [
        (
            13,
            "UPDATE task SET blocked_json='{\"reason\":\"held by the owner\"}' WHERE id='t'",
            "UPDATE task SET blocked_json=NULL,version=version+1 WHERE id='t'",
        ),
        (
            15,
            "UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"gone\"}}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL,version=version+1 WHERE id='t'",
        ),
        (
            16,
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
            "UPDATE workspace SET error=NULL WHERE task_id='t'",
        ),
        (
            17,
            "UPDATE task SET metadata_json='{\"dispatch_disposition\":{\"capability\":\"x\",\"version\":0}}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL,version=version+1 WHERE id='t'",
        ),
    ];
    for (priority, seed, lift) in rows {
        let mut world = World::new().await;
        world.legacy_task("t", "t.txt", "merging").await;
        world.seed_execution("t", "executor", None).await;
        sqlx::query(seed).execute(world.db.pool()).await.unwrap();
        world.touch("t").await;
        let (imported, row) = world.imported("t").await;
        assert_eq!(row, priority);
        let production = Production::new(&world);
        let running = production.run();
        world
            .until(
                "t",
                &format!("P{priority}: the continuation settles"),
                || async {
                    world
                        .db
                        .task_steps("t")
                        .await
                        .unwrap()
                        .iter()
                        .all(|step| !matches!(step.status.as_str(), "pending" | "claimed"))
                },
            )
            .await;
        if priority == 13 {
            // The block stands: the sweep admits nothing.
            let dispatcher = dispatcher(&world).await;
            for _ in 0..3 {
                dispatcher.reconcile_all().await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let current = world
                .db
                .current_integration_attempt("t")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.id, imported.id, "P13 stays put while blocked");
            assert!(!world.on_main("t.txt").await);
        }
        sqlx::query(lift).execute(world.db.pool()).await.unwrap();
        world.touch("t").await;
        let dispatcher = dispatcher(&world).await;
        world
            .until(
                "t",
                &format!("P{priority}: merged after the lift"),
                || async {
                    dispatcher.reconcile_all().await.unwrap();
                    world.task("t").await.status == "done"
                },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "P{priority}");
        assert_eq!(
            world.attempt(&imported.id).await.state,
            S::Superseded,
            "P{priority}: the imported row is history"
        );
    }
}

/// Importer rows P16 (the workspace's owner is lost) and P17 (an accepted
/// owner command or a sticky disposition was still to run): each ends in a
/// typed condition its owner sees (`deferred` / `owner_required`), nothing
/// merges by itself, and the action the Task offers for it (Retry) works:
/// the Task is admitted again and merges, once. The legacy state is left as
/// the old build wrote it; no sweep of the dispatcher runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_lost_owner_and_accepted_command_rows_state_owner_required_and_retry_works() {
    let rows: [(u64, &str); 2] = [
        (
            16,
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
        ),
        (
            17,
            "UPDATE task SET metadata_json='{\"queued_recovery\":{\"id\":\"accepted\"}}' WHERE id='t'",
        ),
    ];
    for (priority, seed) in rows {
        let mut world = World::new().await;
        world.legacy_task("t", "t.txt", "merging").await;
        world.seed_execution("t", "executor", None).await;
        sqlx::query(seed).execute(world.db.pool()).await.unwrap();
        // The stored condition follows the row, as its writer left it.
        let mut tx = db::begin_immediate(world.db.pool()).await.unwrap();
        world
            .db
            .produce_condition_in_tx(&mut tx, "t", db::ConditionChange::Full)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let (imported, row) = world.imported("t").await;
        assert_eq!(row, priority);
        let production = Production::new(&world);
        let running = production.run();
        world
            .until("t", &format!("P{priority}: the owner is told"), || async {
                integration(&world.task("t").await)["kind"] == "deferred"
            })
            .await;
        let stated = integration(&world.task("t").await);
        assert_eq!(stated["cause"], "owner_required", "P{priority}: {stated}");
        // Nothing moves it but its owner.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(world.task("t").await.status, "merging", "P{priority}");
        assert_eq!(world.merges("t").await, 0, "P{priority}");
        assert_eq!(world.attempt(&imported.id).await.state, S::Parked);
        let offers = world.offers("t").await;
        assert!(
            offers.iter().any(|(verb, _)| verb == "retry"),
            "P{priority}: the Task offers Retry: {offers:?}"
        );
        world
            .act(
                "t",
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
            )
            .await;
        world
            .until(
                "t",
                &format!("P{priority}: the retried merge lands"),
                || async { world.task("t").await.status == "done" },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "P{priority}");
        assert_eq!(
            world.attempt(&imported.id).await.state,
            S::Superseded,
            "P{priority}: the imported row is history"
        );
    }
}

/// A merge that stopped for its owner (`retries_exhausted`, a dirty checkout,
/// ...) states `deferred` on the Task and offers Retry; the Retry puts the
/// same attempt back in line with its counted retries started again, and it
/// merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stopped_merge_offers_retry_and_the_retry_gets_it_merged() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    // As the worker leaves it when its counted retries are spent.
    world
        .set(
            &attempt.id,
            "state='parked',failure_kind='owner_required',failure_message='retries_exhausted: infrastructure: the owner did not answer',operation_receipts_json='[{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0},{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0},{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0}]'",
        )
        .await;
    let parked = world.attempt(&attempt.id).await;
    world.ask(&parked, IntegrationStepAction::Park).await;
    world
        .until("t", "the Task says why its merge stopped", || async {
            integration(&world.task("t").await)
                .to_string()
                .contains("owner_required")
        })
        .await;
    let offers = world.offers("t").await;
    assert!(
        offers.contains(&("retry".into(), "integration_retry".into())),
        "{offers:?}"
    );
    let production = Production::new(&world);
    let running = production.run();
    // Nothing moves it but the owner.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(world.attempt(&attempt.id).await.state, S::Parked);
    world
        .act(
            "t",
            TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
        )
        .await;
    world
        .until("t", "the retried merge lands", || async {
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.merges("t").await, 1);
    let done = world
        .db
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        done.operation_receipts_json
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["kind"] == "owner_retry"),
        "the same attempt, retried by its owner"
    );
}

/// A durable attempt, a lost/wrong condition, then a parked cause behind a
/// real Hold. Reconciliation restores ownership without a new admission,
/// hook replay, or loss of the Hold's primary action.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dispatcher_restates_stale_integration_from_the_current_attempt() {
    let world = World::new().await;
    world.legacy_task("stale", "stale.txt", "merging").await;
    world.seed_execution("stale", "executor", None).await;
    let current = world
        .service
        .admit_to_integration_queue("stale")
        .await
        .unwrap();
    let dispatcher = dispatcher(&world).await;
    world
        .until("stale", "missing ownership is restated", || async {
            dispatcher.reconcile_all().await.unwrap();
            integration(&world.task("stale").await)["attempt_id"] == current.id
        })
        .await;
    let raw: String = world
        .scalar(
            "SELECT CAST(condition_json AS TEXT) FROM task WHERE id=?",
            "stale",
        )
        .await;
    sqlx::query("UPDATE task SET condition_json=? WHERE id='stale'")
        .bind(raw.replace(&current.id, "stale-attempt"))
        .execute(world.db.pool())
        .await
        .unwrap();
    world
        .until("stale", "old ownership is replaced", || async {
            dispatcher.reconcile_all().await.unwrap();
            integration(&world.task("stale").await)["attempt_id"] == current.id
        })
        .await;
    world.act("stale", TaskAction::Hold { reason: None }).await;
    world.set(&current.id, "state='parked',failure_kind='owner_required',failure_message='target_dirty: local edit',available_at=NULL").await;
    world
        .until(
            "stale",
            "the current parked cause is restated behind the hold",
            || async {
                dispatcher.reconcile_all().await.unwrap();
                integration(&world.task("stale").await)["cause"] == "target_dirty"
            },
        )
        .await;
    assert!(matches!(
        world.task("stale").await.condition,
        db::TaskCondition::Parked {
            primary: db::ParkReason::Held { .. },
            ..
        }
    ));
    assert_eq!(
        world
            .db
            .current_integration_attempt("stale")
            .await
            .unwrap()
            .unwrap()
            .id,
        current.id
    );
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?",
                "stale"
            )
            .await,
        1
    );
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='hooks'",
                "stale"
            )
            .await,
        0
    );
}

/// The pre-queue rejection rule charges one merge fix and one rejection at
/// the merging gate for a red candidate / dirty Task. Dirty target is an
/// owner stop, with no charge. The queue must preserve those outcomes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn red_head_dirty_task_and_dirty_target_preserve_pre_queue_budget_outcomes() {
    for case in ["red", "dirty-task", "dirty-target"] {
        let world = World::new().await;
        let a = world.reconciled("budget", "budget.txt").await;
        match case {
            "red" => {
                review_ci(&world, "budget", &["exit 7"]).await;
                world.move_target("outside").await;
            }
            "dirty-task" => {
                std::fs::write(world.tree("budget").join("local"), "dirty\n").unwrap();
            }
            _ => {
                std::fs::write(world.repo().join("local"), "dirty\n").unwrap();
            }
        }
        let production = Production::new(&world);
        let running = production.run();
        world
            .until("budget", "the budget outcome is settled", || async {
                if case == "dirty-target" {
                    integration(&world.task("budget").await)["cause"] == "target_dirty"
                } else {
                    world.task("budget").await.status == "merge_failed"
                }
            })
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        let charged: Vec<(String, i64)> = sqlx::query_as(
            "SELECT kind,spent FROM task_budget WHERE task_id='budget' AND spent>0 ORDER BY kind",
        )
        .fetch_all(world.db.pool())
        .await
        .unwrap();
        let expected = if case == "dirty-target" {
            vec![]
        } else {
            vec![("gate:merging".to_owned(), 1), ("merge_fix".to_owned(), 1)]
        };
        assert_eq!(charged, expected, "{case}");
        assert_eq!(world.merges("budget").await, 0, "{case}");
        if case == "dirty-target" {
            assert_eq!(world.attempt(&a.id).await.state, S::Parked);
            assert_eq!(world.task("budget").await.status, "merging");
        } else {
            assert_eq!(world.attempt(&a.id).await.state, S::Ejected);
        }
    }
}

/// 1a, the other half: a binding is replaced only when it is provably gone.
/// A placement that is not `ready` (its machine is away) proves nothing: the
/// parked attempt keeps its binding, and the owner's Retry puts that same
/// attempt back in line. Once recovery has verified the placement `ready`
/// under another generation, the attempt is superseded by one bound to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_attempt_is_rebound_only_when_its_ready_placement_names_another_binding() {
    let world = World::new().await;
    world.legacy_task("bind", "bind.txt", "merging").await;
    world.seed_execution("bind", "executor", None).await;
    let first = world
        .service
        .admit_to_integration_queue("bind")
        .await
        .unwrap();
    let placement = first.placement_id.clone().unwrap();
    world.set(&first.id, "state='parked',placement_generation=placement_generation+7,failure_kind='infrastructure',failure_message='owner_offline: owner absent',available_at=NULL").await;
    sqlx::query("UPDATE workspace_placement SET state='disconnected' WHERE id=?")
        .bind(&placement)
        .execute(world.db.pool())
        .await
        .unwrap();
    let attempts = || async {
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?",
                "bind",
            )
            .await
    };
    world.service.integration_owner_ready("bind").await.unwrap();
    assert_eq!(attempts().await, 1, "an away machine's binding was stolen");
    let task = world.task("bind").await;
    world.service.retry_integration(&task).await.unwrap();
    let current = world
        .db
        .current_integration_attempt("bind")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (attempts().await, current.id.as_str()),
        (1, first.id.as_str()),
        "Retry replaced the attempt of a machine that is only away"
    );
    assert!(
        current.available_at.is_some(),
        "Retry did not put the parked attempt back in line"
    );
    world
        .set(&first.id, "state='parked',available_at=NULL")
        .await;
    sqlx::query("UPDATE workspace_placement SET state='ready' WHERE id=?")
        .bind(&placement)
        .execute(world.db.pool())
        .await
        .unwrap();
    world.service.integration_owner_ready("bind").await.unwrap();
    let rebound = world
        .db
        .current_integration_attempt("bind")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rebound.predecessor_attempt_id.as_deref(),
        Some(first.id.as_str())
    );
    assert_eq!(rebound.placement_generation, first.placement_generation);
    assert_eq!(world.attempt(&first.id).await.state, S::Superseded);
    assert_eq!(rebound.queue_seq, first.queue_seq);
}

/// Two writers on one row state one cause. The reconciler's restatement
/// (from the scheduler's snapshot) writes first on an imported row nothing
/// has stated yet; the import continuation writes the same row afterwards;
/// the reconciler looks again. All three decide with `deferral::deferral`:
/// a quarantined imported row that names no operation is its owner's
/// (`owner_required`, not a result that could still arrive), and a row that
/// waited for a machine before the queue (P15) names that machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reconciler_and_the_import_continuation_state_one_cause_for_one_row() {
    for (case, assignments, cause, owner) in [
        (
            "quarantined",
            "state='quarantined',import_source_json='{\"priority\":20}',failure_kind='infrastructure',failure_message='contradictory evidence',available_at=NULL",
            "owner_required",
            Value::Null,
        ),
        (
            "offline",
            "state='parked',import_source_json='{\"priority\":15}',failure_kind='infrastructure',failure_message='waiting for the checkout owner',available_at='2099-01-01T00:00:00Z'",
            "owner_offline",
            json!("gone-machine"),
        ),
    ] {
        let world = World::new().await;
        world.legacy_task("two", "two.txt", "merging").await;
        world.seed_execution("two", "executor", None).await;
        if case == "offline" {
            sqlx::query("UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.owner_wait',json_object('daemon_id','gone-machine')) WHERE id='two'")
                .execute(world.db.pool())
                .await
                .unwrap();
        }
        let current = world
            .service
            .admit_to_integration_queue("two")
            .await
            .unwrap();
        world.set(&current.id, assignments).await;
        let stated = || async {
            let reason = integration(&world.task("two").await);
            (reason["cause"].clone(), reason["owner_id"].clone())
        };
        let dispatcher = dispatcher(&world).await;
        // Writer one: the reconciler, on a row whose deferral nobody stated.
        world
            .until("two", "the reconciler states the deferral", || async {
                dispatcher.reconcile_all().await.unwrap();
                integration(&world.task("two").await)["kind"] == "deferred"
            })
            .await;
        assert_eq!(stated().await, (json!(cause), owner.clone()), "{case}: reconciler");
        // Writer two: the import continuation of the same row.
        let report = super::super::import::import_at_start(&world.db)
            .await
            .unwrap();
        assert_eq!(report.continuations, 1, "{case}");
        world
            .until("two", "the import continuation settles", || async {
                world
                    .scalar::<i64>(
                        "SELECT COUNT(*) FROM task_step WHERE task_id=? AND causation_key LIKE 'integration-import:%' AND status='done'",
                        "two",
                    )
                    .await
                    == 1
            })
            .await;
        assert_eq!(
            stated().await,
            (json!(cause), owner.clone()),
            "{case}: import continuation"
        );
        // And the reconciler finds nothing to restate.
        let before = world.task("two").await.version;
        dispatcher.reconcile_all().await.unwrap();
        assert_eq!(stated().await, (json!(cause), owner), "{case}: second look");
        assert_eq!(world.task("two").await.version, before, "{case}");
        assert_eq!(
            world.attempt(&current.id).await.revision,
            world
                .db
                .current_integration_attempt("two")
                .await
                .unwrap()
                .unwrap()
                .revision,
            "{case}: the row was not replaced"
        );
    }
}

/// A stale-bound attempt that is still its queue's head (claimed, not yet
/// started; or reconciled back to `queued` on the slot). Supersede refuses a
/// head, so the reconciler used to get the same row back from admission and
/// restate `waiting` at every pass. It now writes nothing while the attempt
/// holds the slot, and rebinds it through admission once it is off the slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_bound_head_is_left_to_its_driver_and_rebound_once_it_is_off_the_slot() {
    let world = World::new().await;
    world.legacy_task("head", "head.txt", "merging").await;
    world.seed_execution("head", "executor", None).await;
    let first = world
        .service
        .admit_to_integration_queue("head")
        .await
        .unwrap();
    let queue = first.queue_id.clone().unwrap();
    // The placement is `ready` under another generation than the attempt's.
    world
        .set(&first.id, "placement_generation=placement_generation+7")
        .await;
    sqlx::query("UPDATE integration_queue SET head_attempt_id=?,revision=revision+1 WHERE id=?")
        .bind(&first.id)
        .bind(&queue)
        .execute(world.db.pool())
        .await
        .unwrap();
    let before = world.task("head").await;
    let rows = || async {
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?",
                "head",
            )
            .await
    };
    for _ in 0..3 {
        assert!(
            !world
                .service
                .reconcile_integration_admission(&before)
                .await
                .unwrap(),
            "a head was handed to admission"
        );
    }
    let after = world.task("head").await;
    assert_eq!(
        (after.version, json!(after.condition)),
        (before.version, json!(before.condition)),
        "the reconciler wrote on a Task whose attempt holds the slot"
    );
    assert_eq!(rows().await, 1);
    assert_eq!(world.attempt(&first.id).await.state, S::Queued);
    // Off the slot: the binding is replaced, the place in line kept.
    sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL,revision=revision+1 WHERE id=?")
        .bind(&queue)
        .execute(world.db.pool())
        .await
        .unwrap();
    assert!(world
        .service
        .reconcile_integration_admission(&after)
        .await
        .unwrap());
    let rebound = world
        .db
        .current_integration_attempt("head")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rebound.predecessor_attempt_id.as_deref(),
        Some(first.id.as_str())
    );
    assert_eq!(rebound.queue_seq, first.queue_seq);
    assert_eq!(world.attempt(&first.id).await.state, S::Superseded);
}

/// A pre-queue `queued` attempt is claimable only once its import
/// continuation has settled. When that settled step is gone (step retention
/// pruned it) the row used to wait for the next start to enqueue the
/// continuation again. The queue worker's sweep now enqueues it, so the Task
/// merges without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_pre_queue_queued_row_without_its_continuation_step_is_continued_by_the_worker() {
    let world = World::new().await;
    let a = world.reconciled("pruned", "pruned.txt").await;
    // The row as the importer left it: bound, `queued`, and with no
    // continuation step in the Task's step queue any more.
    world
        .set(
            &a.id,
            "import_source_json='{\"version\":1,\"priority\":10}'",
        )
        .await;
    let key = format!("integration-import:{}", a.id);
    let continuations = || async {
        world
            .db
            .task_steps("pruned")
            .await
            .unwrap()
            .into_iter()
            .filter(|step| step.causation_key == key)
            .count()
    };
    assert_eq!(continuations().await, 0);
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("pruned", "the Task merges without a restart", || async {
            world.task("pruned").await.status == "done"
        })
        .await;
    assert_eq!(continuations().await, 1, "one continuation, enqueued once");
    assert_eq!(world.merges("pruned").await, 1);
    // The same attempt merged; its driver completes it after the Task is
    // reported `done`.
    world
        .until("pruned", "the attempt is completed", || async {
            world.attempt(&a.id).await.state == S::Completed
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// Two sweeps at once, and a sweep beside the start pass, enqueue one
/// continuation for a pre-queue `queued` row and count it once: the step's
/// key is unique per Task. A history row and a row that is not `queued` get
/// no step from the sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweeps_and_the_start_pass_enqueue_one_continuation_for_one_row() {
    let world = World::new().await;
    let world = &world;
    let a = world.reconciled("twice", "twice.txt").await;
    let parked = world.reconciled("resting", "resting.txt").await;
    for id in [&a.id, &parked.id] {
        world
            .set(id, "import_source_json='{\"version\":1,\"priority\":10}'")
            .await;
    }
    world
        .set(
            &parked.id,
            "state='parked',failure_kind='owner_required',failure_message='held'",
        )
        .await;
    let steps = |task: &'static str, id: String| async move {
        let key = format!("integration-import:{id}");
        world
            .db
            .task_steps(task)
            .await
            .unwrap()
            .into_iter()
            .filter(|step| step.causation_key == key)
            .count()
    };
    let (one, two, start) = tokio::join!(
        import::enqueue_missing_continuations(&world.db, true),
        import::enqueue_missing_continuations(&world.db, true),
        import::enqueue_missing_continuations(&world.db, true),
    );
    assert_eq!(
        one.unwrap() + two.unwrap() + start.unwrap(),
        1,
        "one enqueue is counted"
    );
    assert_eq!(steps("twice", a.id.clone()).await, 1);
    assert_eq!(
        steps("resting", parked.id.clone()).await,
        0,
        "the sweep continues `queued` rows only"
    );
    // The start pass lists every current pre-queue row, once.
    let (sweep, start) = tokio::join!(
        import::enqueue_missing_continuations(&world.db, true),
        import::enqueue_missing_continuations(&world.db, false),
    );
    assert_eq!(sweep.unwrap() + start.unwrap(), 1);
    assert_eq!(steps("twice", a.id.clone()).await, 1);
    assert_eq!(steps("resting", parked.id.clone()).await, 1);
    assert_eq!(
        import::enqueue_missing_continuations(&world.db, false)
            .await
            .unwrap(),
        0
    );
}
