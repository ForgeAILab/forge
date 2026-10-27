//! Durable workflow disk waits tests.

use super::*;

// The review entry's check needs the Task worktree, which is gone, and the
// disk is under its floor. That is a wait, never a failed review: the hooks
// step suspends on the disk wait, the Task shows the disk capacity wait, no
// review attempt and no check exist. When the disk recovers the step's
// recheck runs the hook again and the entry settles with its verdict.
#[tokio::test]
async fn a_review_entry_whose_worktree_cannot_be_recreated_under_the_disk_floor_waits_for_disk() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    let entered = enter_review(&fixture, "CI entry").await;
    assert_eq!(entered.pending_steps, 1);
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(
        waits_for_disk(&fixture).await,
        "the Task states the disk wait"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        0
    );
    let attempts: i64 =
        sqlx::query_scalar("SELECT attempts FROM task_step WHERE kind='hooks' AND task_id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(attempts, 0, "the wait is not charged to the step");

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?} {:?}", task.error_annotation);
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[0].1.contains("entry-commands"));
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&task).is_none(),
        "the disk wait is over"
    );
}

// A blocking before-work hook needs the worktree too. It used to record the
// disk wait and then report the hook as failed, which parked the Task as a
// failed transition hook for its owner. Now the hook waits: nothing ran,
// nothing failed, and the script runs once the disk recovers.
#[tokio::test]
async fn a_before_work_hook_whose_worktree_cannot_be_recreated_waits_for_disk_and_does_not_fail() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let marker = TempDir::new().unwrap();
    let ran = marker.path().join("ran");
    let scripts = json!([{"type":"script","command":format!("echo run >> '{}'", ran.display()),"timeout_seconds":30,"blocking":true}]);
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(json!({"lifecycle_hooks":{"before_work":scripts}}).to_string())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "entry with a before-work hook").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(!ran.exists(), "the script ran without its worktree");
    assert!(
        waits_for_disk(&fixture).await,
        "the Task states the disk wait"
    );
    // The built-in no-change guard records its decision at its reserved
    // index before any configured hook; that is not a hook result.
    assert_eq!(
        count(
            &fixture,
            &format!(
                "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json IS NOT NULL AND hook_index<>{}",
                db::budget::REPAIR_NO_CHANGE_CHECKPOINT
            )
        )
        .await,
        0,
        "no hook result (and no failure) was recorded"
    );

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(std::fs::read_to_string(&ran).unwrap(), "run\n");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
}

// A held Task is its owner's decision to stop. A system re-entry of its
// review (recovery settling a retired inline re-run, a queued `rerun_review`
// command) does nothing while the hold stands: no attempt is superseded, no
// status entry, hooks step, check or review attempt is created. The release
// runs the review entry by itself.
#[tokio::test]
async fn a_system_review_re_entry_on_a_held_task_does_nothing_until_release() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    // Held while its entry check is queued: the wait is abandoned.
    suspend_on_entry_check(&fixture).await;
    let entered = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let held = fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Hold { reason: None },
            entered.version,
        )
        .await
        .expect("the Task is held")
        .task;
    assert_eq!(held.status, "review");
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let snapshot = || async {
        let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        (
            epoch,
            review_summaries(&fixture).await,
            count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
            count(
                &fixture,
                "SELECT COUNT(*) FROM task_step WHERE kind='hooks'",
            )
            .await,
            count(&fixture, "SELECT COUNT(*) FROM transition_log").await,
        )
    };
    let before = snapshot().await;
    let id = uuid::Uuid::parse_str(&fixture.task.id).unwrap();
    let recovery = api_types::Actor::system(api_types::SystemComponent::CrashRecovery);
    let refused = fixture.engine.rerun_review(id, recovery).await;
    assert!(
        matches!(&refused, Err(ServiceError::Conflict(_))),
        "a system re-entry ran on a held Task: {:?}",
        refused.map(|task| task.status)
    );
    drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(
        snapshot().await,
        before,
        "the refused re-entry changed the Task"
    );
    // The owner is not offered the re-run either while the hold stands.
    assert!(matches!(
        fixture
            .engine
            .perform_task_action(
                &fixture.task.id,
                api_types::TaskAction::retry(),
                held.version
            )
            .await,
        Err(ServiceError::TaskActionUnavailable { .. })
    ));

    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Release { reason: None },
            current.version,
        )
        .await
        .expect("the Task is released");
    // Nobody asks again: the release itself runs the entry the hold stopped
    // (the hold reached the Task while its entry check was pending and
    // superseded the hooks step). Without that the released Task sat in
    // `review` with no check and no attempt, its approval on offer.
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(task.status, "review");
    let after = snapshot().await;
    assert!(after.0 > before.0, "a new review entry ran after release");
    assert_eq!(after.2, before.2 + 1, "the entry asked for its check once");
    assert!(after
        .1
        .iter()
        .any(|review| review.0 == db::ReviewStatus::AwaitingHuman
            && review.1.contains("entry-commands")));
    assert_eq!(
        after
            .1
            .iter()
            .filter(|review| matches!(
                review.0,
                db::ReviewStatus::Running | db::ReviewStatus::AwaitingHuman
            ))
            .count(),
        1,
        "one open attempt: {:?}",
        after.1
    );
    // A second release has nothing to resume.
    assert!(!fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
}

// While its entry waits for disk the Task offers `cancel` and no `hold`. A
// hold used to be on offer (the disk wait is a capacity wait like a dispatch
// wait): it superseded the hooks step, and its release left the Task in
// `review` with its entry never run: no check, no attempt, and the gate's
// approval on offer. Cancel works: the step is superseded, nothing failed.
#[tokio::test]
async fn a_task_whose_entry_waits_for_disk_offers_cancel_and_no_hold() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let _free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "CI entry").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    assert!(waits_for_disk(&fixture).await);
    let offers = fixture
        .engine
        .task_action_offers(
            &fixture.task.id,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let verbs: Vec<&str> = offers
        .available_actions
        .iter()
        .map(|offer| offer.action.verb())
        .collect();
    assert_eq!(verbs, ["cancel"]);
    // Two more rechecks: the wait is one step that is suspended again, not
    // a new step (or a review attempt) per recheck.
    let rows = |fixture: &FailedCiFixture| {
        let pool = fixture.db.pool().clone();
        async move {
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT (SELECT COUNT(*) FROM task_step),(SELECT COUNT(*) FROM review),(SELECT COALESCE(SUM(attempts),0) FROM task_step WHERE kind='hooks')",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let before = rows(&fixture).await;
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    stop.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(rows(&fixture).await, before, "a recheck added rows");
    assert_eq!(awaited_consumer(&fixture).await.as_deref(), Some("disk"));

    let cancelled = fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Cancel { reason: None },
            TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
                .await
                .unwrap()
                .unwrap()
                .version,
        )
        .await
        .expect("cancel is accepted while the entry waits for disk");
    assert_eq!(cancelled.task.status, "cancelled");
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    assert_eq!(task.status, "cancelled");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE status IN ('pending','claimed','suspended')"
        )
        .await,
        0
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);
}

// A hold command reaches the Task while its entry waits for disk (the offer
// list does not have it; the command is accepted all the same, because a
// hold may stop hooks in flight). It supersedes the hooks step. The release
// runs the entry again by itself: the Task does not sit in `review` with its
// checks unrun.
#[tokio::test]
async fn a_hold_while_the_entry_waits_for_disk_is_resumed_by_its_release() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    let free = worktree_gone_under_the_disk_floor(&fixture).await;
    enter_review(&fixture, "CI entry").await;
    let _ = fixture.engine.check_worker_or_embedded();
    suspend_on_disk(&fixture).await;
    let version = |fixture: &FailedCiFixture| {
        let (db, id) = (fixture.db.clone(), fixture.task.id.clone());
        async move {
            TaskRepo::get_by_id(&*db, &id, false)
                .await
                .unwrap()
                .unwrap()
                .version
        }
    };
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Hold { reason: None },
            version(&fixture).await,
        )
        .await
        .expect("the hold command is accepted");
    drain(fixture.engine.clone(), &fixture.task.id).await;
    assert!(fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 0);

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    fixture
        .engine
        .perform_task_action(
            &fixture.task.id,
            api_types::TaskAction::Release { reason: None },
            version(&fixture).await,
        )
        .await
        .expect("the Task is released");
    // The release ran the entry again: a new hooks step of a new status
    // entry, which reads the disk again at its recheck and asks for its
    // check.
    assert!(!fixture
        .db
        .entry_hooks_preempted(&fixture.task.id)
        .await
        .unwrap());
    resume_after_the_disk_recheck(&fixture).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?} {:?}", task.error_annotation);
    assert_eq!(reviews[0].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[0].1.contains("entry-commands"));
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1
    );
}
