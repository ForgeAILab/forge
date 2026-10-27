//! Quarantined import retries, missing branches and successor checks.
use super::*;

/// A parked quarantine is level-triggered, but a level that is already
/// stated writes nothing: later sweeps and later starts add no Task step, no
/// scheduler generation and no attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_quarantined_import_writes_nothing_on_later_sweeps_and_starts() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    world.parked_quarantine("orphan").await;
    let dispatcher = dispatcher(&world).await;
    // The first pass after the park may write the scheduler's own park row.
    for _ in 0..3 {
        dispatcher.reconcile_all().await.unwrap();
        world.service.drain("orphan").await.unwrap();
    }
    let parked = world.task("orphan").await;
    let before = world.bookkeeping("orphan").await;
    assert!(
        before.1.iter().all(|row| row.1 == 0),
        "still dirty: {before:?}"
    );
    for _ in 0..3 {
        let again = import::import_at_start(&world.db).await.unwrap();
        assert_eq!((again.imported, again.continuations), (0, 0));
        dispatcher.reconcile_all().await.unwrap();
        world.service.drain("orphan").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(world.bookkeeping("orphan").await, before);
    let after = world.task("orphan").await;
    assert_eq!(after.condition, parked.condition);
    assert_eq!(after.version, parked.version);
    assert_eq!(integration(&after)["cause"], "owner_required");
}

/// The old server finished the merge in Git and died before recording it:
/// the import finds no execution and quarantines. The owner's Retry must
/// find the commit in the target and report the Task as landed: no second
/// merge, no new commit on the target, no park for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_of_a_quarantined_import_whose_commit_already_landed_settles_without_a_second_merge()
{
    for target_moved_after in [false, true] {
        let world = World::new().await;
        let candidate = world.legacy_task("orphan", "orphan.txt", "merging").await;
        git_out(&world.repo(), &["merge", "--ff-only", "task-orphan"])
            .await
            .expect("the old merge landed");
        if target_moved_after {
            world.move_target("later.txt").await;
        }
        let target = world.main_tip().await;
        let commits = world.main_commits().await;
        world.parked_quarantine("orphan").await;
        review_ci(&world, "orphan", &["test -f orphan.txt"]).await;
        let production = Production::new(&world);
        let running = production.run();
        world.act("orphan", retry_action()).await;
        world
            .until("orphan", "the landed Task is reported", || async {
                world.task("orphan").await.status == "done"
            })
            .await;
        assert_eq!(world.main_tip().await, target, "moved={target_moved_after}");
        assert_eq!(world.main_commits().await, commits);
        assert!(world.on_main("orphan.txt").await);
        assert!(git_out(
            &world.repo(),
            &["merge-base", "--is-ancestor", &candidate, "refs/heads/main"]
        )
        .await
        .is_some());
        assert!(world.merges("orphan").await <= 1);
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    }
}

/// Retry twice, and the importer and the sweep beside it: one admitted
/// successor, which the quarantine never competes with again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_retry_and_later_passes_leave_one_successor_of_a_quarantined_import() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    let quarantine = world.parked_quarantine("orphan").await;
    let head = git::get_current_sha(&world.tree("orphan")).await.unwrap();
    world.act("orphan", retry_action()).await;
    let admitted = world
        .db
        .current_integration_attempt("orphan")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.original_candidate_sha.as_deref(), Some(&*head));
    assert_eq!(
        admitted.predecessor_attempt_id.as_deref(),
        Some(&*quarantine)
    );
    // The second Retry is refused or returns the same row: never a third.
    let version = world.task("orphan").await.version;
    let _ = world
        .service
        .perform_task_action("orphan", retry_action(), version)
        .await;
    let stated = integration(&world.task("orphan").await);
    assert_eq!(stated["attempt_id"], admitted.id, "{stated}");
    assert_ne!(stated["cause"], "owner_required", "{stated}");
    assert!(world
        .db
        .unowned_quarantined_import("orphan")
        .await
        .unwrap()
        .is_none());
    let dispatcher = dispatcher(&world).await;
    let again = import::import_at_start(&world.db).await.unwrap();
    assert_eq!((again.imported, again.continuations), (0, 0));
    dispatcher.reconcile_all().await.unwrap();
    let rows = || async {
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?",
                "orphan",
            )
            .await
    };
    assert_eq!(rows().await, 2);
    // The successor merges once, with its head check.
    review_ci(&world, "orphan", &["test -f orphan.txt"]).await;
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("orphan", "the successor merges", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("orphan").await.status == "done"
        })
        .await;
    assert_eq!(world.merges("orphan").await, 1);
    assert_eq!(rows().await, 2);
    world
        .until("orphan", "the successor is completed", || async {
            world
                .scalar::<String>(
                    "SELECT state FROM integration_attempt WHERE id=?",
                    &admitted.id,
                )
                .await
                == "completed"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// Hold keeps the park behind the hold, Release brings Retry back, and
/// Cancel ends the Task. A Task that left the entry, or was deleted, is not
/// the quarantine's any more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hold_release_and_cancel_of_a_parked_quarantined_import() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    world.parked_quarantine("orphan").await;
    let dispatcher = dispatcher(&world).await;
    world.act("orphan", TaskAction::Hold { reason: None }).await;
    for _ in 0..2 {
        dispatcher.reconcile_all().await.unwrap();
        world.service.drain("orphan").await.unwrap();
    }
    let held = world.task("orphan").await;
    assert_eq!(held.status, "merging");
    assert!(
        matches!(
            held.condition,
            db::TaskCondition::Parked {
                primary: db::ParkReason::Held { .. },
                ..
            }
        ),
        "{:?}",
        held.condition
    );
    assert_eq!(integration(&held)["cause"], "owner_required");
    let offers = world.offers("orphan").await;
    assert!(
        offers.iter().any(|(verb, _)| verb == "release"),
        "{offers:?}"
    );
    world
        .act("orphan", TaskAction::Release { reason: None })
        .await;
    world
        .until("orphan", "Retry is offered again after Release", || async {
            dispatcher.reconcile_all().await.unwrap();
            world
                .offers("orphan")
                .await
                .contains(&("retry".into(), "integration_retry".into()))
        })
        .await;
    assert!(world
        .db
        .current_integration_attempt("orphan")
        .await
        .unwrap()
        .is_none());
    assert!(world
        .db
        .unowned_quarantined_import("orphan")
        .await
        .unwrap()
        .is_some());
    let cancelled = world
        .act("orphan", TaskAction::Cancel { reason: None })
        .await;
    assert_eq!(cancelled.status, "cancelled");
    assert!(world
        .db
        .unowned_quarantined_import("orphan")
        .await
        .unwrap()
        .is_none());
    let report = import::import_at_start(&world.db).await.unwrap();
    assert_eq!(report.continuations, 0);
}

/// Every other `corrupt_import` cause keeps the policy it had: the row is
/// history, and the sweep admits the Task through the normal admission from
/// its present rows. A Task that admission cannot name a candidate for is
/// blocked, typed, with Retry and Cancel; it is never left `clear`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_corrupt_import_cause_is_admitted_by_the_sweep_or_blocked_typed() {
    let world = World::new().await;
    for task in ["noexec", "exec"] {
        world
            .legacy_task(task, &format!("{task}.txt"), "merging")
            .await;
        // The old entry's checkpoint is not an object: `corrupt_import`,
        // "hook effects are not an object", with no queue.
        world.old_merge_step(task, json!([])).await;
        world.touch(task).await;
    }
    world.seed_execution("exec", "executor", None).await;
    let report = import::import_at_start(&world.db).await.unwrap();
    assert_eq!(
        (report.quarantined, report.continuations),
        (2, 0),
        "{report:?}"
    );
    for task in ["noexec", "exec"] {
        assert!(world
            .db
            .unowned_quarantined_import(task)
            .await
            .unwrap()
            .is_none());
    }
    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until("exec", "the Task with a candidate merges", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("exec").await.status == "done"
        })
        .await;
    world
        .until("noexec", "the Task without one is blocked", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("noexec").await.blocked_json.is_some()
        })
        .await;
    let blocked = world.task("noexec").await;
    assert_eq!(blocked.status, "merging");
    assert!(
        blocked
            .blocked_json
            .as_deref()
            .unwrap()
            .contains("integration_admission_failed"),
        "{:?}",
        blocked.blocked_json
    );
    let offers = world.offers("noexec").await;
    for verb in ["retry", "cancel"] {
        assert!(
            offers.iter().any(|(offered, _)| offered == verb),
            "{offers:?}"
        );
    }
    assert!(!world.on_main("noexec.txt").await);
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// The Task's worktree and branch are gone: Retry is refused with the cause,
/// nothing is admitted or merged, and the park keeps Retry and Cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_of_a_quarantined_import_whose_branch_is_gone_is_refused_and_the_park_stays() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    world.parked_quarantine("orphan").await;
    git_out(
        &world.repo(),
        &[
            "worktree",
            "remove",
            "--force",
            world.tree("orphan").to_str().unwrap(),
        ],
    )
    .await
    .expect("worktree removed");
    git_out(&world.repo(), &["branch", "-D", "task-orphan"])
        .await
        .expect("branch deleted");
    let tip = world.main_tip().await;
    let version = world.task("orphan").await.version;
    let refused = world
        .service
        .perform_task_action("orphan", retry_action(), version)
        .await;
    assert!(
        refused.is_err(),
        "a Retry with no branch admitted something"
    );
    let parked = world.task("orphan").await;
    assert_eq!(parked.status, "merging");
    assert_eq!(integration(&parked)["cause"], "owner_required");
    assert!(world
        .db
        .current_integration_attempt("orphan")
        .await
        .unwrap()
        .is_none());
    let offers = world.offers("orphan").await;
    assert!(
        offers.contains(&("retry".into(), "integration_retry".into())),
        "{offers:?}"
    );
    assert!(offers.iter().any(|(verb, _)| verb == "cancel"));
    assert_eq!(world.main_tip().await, tip);
}

/// The legacy execution exists but names no workspace: nothing can prove
/// what its review covered. Retry admits the present branch, the head's
/// review authority refuses it, and the Task leaves the queue for its repair
/// state with the cause. It is never merged unreviewed and never `clear`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_with_an_unpinned_legacy_execution_never_merges_without_review() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    let execution = world.seed_execution("orphan", "executor", None).await;
    sqlx::query("UPDATE execution SET workspace_id=NULL WHERE id=?")
        .bind(execution)
        .execute(world.db.pool())
        .await
        .unwrap();
    world.parked_quarantine("orphan").await;
    let tip = world.main_tip().await;
    let production = Production::new(&world);
    let running = production.run();
    world.act("orphan", retry_action()).await;
    // The send-back commits `merge_failed` first; the hop on to review is
    // the next step of the same chain, and only its refusal states the
    // failure Restart is offered for. Reading the offers between the two
    // sees a Task that is still moving (Cancel, and no Restart).
    world
        .until(
            "orphan",
            "the unreviewable candidate is sent back and parked",
            || async {
                let task = world.task("orphan").await;
                task.status == "merge_failed"
                    && task.condition.read().failure_kind
                        == Some(api_types::FailureKind::CascadeFailed)
            },
        )
        .await;
    assert_eq!(world.main_tip().await, tip);
    assert!(!world.on_main("orphan.txt").await);
    assert_eq!(world.merges("orphan").await, 0);
    let offers = world.offers("orphan").await;
    for verb in ["restart", "cancel"] {
        assert!(
            offers.iter().any(|(offered, _)| offered == verb),
            "{offers:?}"
        );
    }
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let dispatcher = dispatcher(&world).await;
    dispatcher.reconcile_all().await.unwrap();
    world.service.drain("orphan").await.unwrap();
    let before = world.bookkeeping("orphan").await;
    for _ in 0..20 {
        dispatcher.reconcile_all().await.unwrap();
        world.service.drain("orphan").await.unwrap();
        assert_eq!(world.bookkeeping("orphan").await, before);
    }
    let parked = world.task("orphan").await;
    assert_eq!(
        parked.condition.read().failure_kind,
        Some(api_types::FailureKind::CascadeFailed)
    );
    assert!(db::material_blocker(&parked.condition).requires_intervention);
}

/// A paused Project withholds Retry, and the summary must not hint an action
/// the action system refuses: it names the Project's resume, and Retry is
/// hinted and offered once the Project runs again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quarantined_import_of_a_paused_project_hints_the_resume_and_then_retry() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    sqlx::query("UPDATE project SET paused_at=? WHERE id='p'")
        .bind(db::now_rfc3339())
        .execute(world.db.pool())
        .await
        .unwrap();
    world.parked_quarantine("orphan").await;
    let hint = || async {
        crate::integration_reads::task_integration(&world.db, &world.task("orphan").await)
            .await
            .unwrap()
            .unwrap()
            .action_hint
    };
    let retry = ("retry".to_owned(), "integration_retry".to_owned());
    let offers = world.offers("orphan").await;
    assert!(!offers.contains(&retry), "{offers:?}");
    assert!(offers.iter().any(|(verb, _)| verb == "cancel"));
    assert_eq!(
        hint().await,
        Some(api_types::IntegrationActionHint::ResumeProject)
    );
    sqlx::query("UPDATE project SET paused_at=NULL WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    assert!(world.offers("orphan").await.contains(&retry));
    assert_eq!(hint().await, Some(api_types::IntegrationActionHint::Retry));
}

/// The head check a retried quarantined import owes is owed by every
/// successor of that Retry in the same entry: an attempt that replaces the
/// retried one (its placement was verified again) must not fast-forward an
/// unchanged target unchecked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_of_a_retried_quarantined_import_still_owes_the_head_check() {
    let world = World::new().await;
    world.legacy_task("orphan", "orphan.txt", "merging").await;
    let quarantine = world.parked_quarantine("orphan").await;
    review_ci(&world, "orphan", &["test -f orphan.txt"]).await;
    world.act("orphan", retry_action()).await;
    let first = world
        .db
        .current_integration_attempt("orphan")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.predecessor_attempt_id.as_deref(), Some(&*quarantine));
    // Recovery verified the placement again: the binding is stale and the
    // sweep replaces the attempt through admission.
    sqlx::query("UPDATE workspace_placement SET generation=generation+1 WHERE task_id='orphan'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let row = world.task("orphan").await;
    assert!(world
        .service
        .reconcile_integration_admission(&row)
        .await
        .unwrap());
    let second = world
        .db
        .current_integration_attempt("orphan")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(second.id, first.id);
    assert_eq!(second.predecessor_attempt_id.as_deref(), Some(&*first.id));
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("orphan", "the successor merges", || async {
            world.task("orphan").await.status == "done"
        })
        .await;
    let checks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE id IN (SELECT run_id FROM check_consumer WHERE origin='integration' AND task_id='orphan')")
        .fetch_one(world.db.pool()).await.unwrap();
    assert_eq!(checks, 1, "the successor merged without the head check");
    assert_eq!(world.merges("orphan").await, 1);
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
