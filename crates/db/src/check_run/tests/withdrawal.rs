//! Withdrawal repository tests.
use super::*;

/// A queued run whose last consumer is withdrawn is cancelled in the
/// withdrawal, with no check worker and no free slot, and the operator's
/// backlog counts drop it at once. A run another consumer still waits for
/// stays queued.
#[tokio::test]
async fn withdrawing_the_last_consumer_of_a_queued_run_cancels_it_at_once() {
    let db = fixture().await;
    seed_second_task(&db).await;
    let first = db.request_check_run(request("one")).await.unwrap();
    let mut other = request("two");
    other.task_id = Some("t2".into());
    let second = db.request_check_run(other).await.unwrap();
    let run_id = first.consumer.run_id.clone().unwrap();
    assert_eq!(second.consumer.run_id.as_deref(), Some(run_id.as_str()));
    // The run already told its consumers it waits for a slot.
    sqlx::query("UPDATE check_run SET capacity_wait_since=? WHERE id=?")
        .bind(NOW)
        .bind(&run_id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db
        .cancel_abandoned_check_consumer(&first.consumer.id)
        .await
        .unwrap());
    assert_eq!(run_state(&db, &run_id).await, "queued");
    assert_eq!(
        db.check_operator_summary()
            .await
            .unwrap()
            .waiting_for_slot
            .0,
        1
    );
    assert!(db
        .cancel_abandoned_check_consumer(&second.consumer.id)
        .await
        .unwrap());
    assert_eq!(run_state(&db, &run_id).await, "cancelled");
    let summary = db.check_operator_summary().await.unwrap();
    assert_eq!((summary.queued.0, summary.waiting_for_slot.0), (0, 0));
    assert!(db.find_live_check_run(&identity()).await.unwrap().is_none());
    // A second withdrawal of the same consumer changes nothing.
    assert!(!db
        .cancel_abandoned_check_consumer(&second.consumer.id)
        .await
        .unwrap());
}

/// Project deletion withdraws every consumer: a queued run is cancelled in
/// the same write, a claimed one is left to the worker that holds its lease.
#[tokio::test]
async fn withdrawing_a_projects_consumers_cancels_its_queued_runs_and_leaves_claimed_ones() {
    let db = fixture().await;
    seed_second_task(&db).await;
    let queued = db.request_check_run(request("one")).await.unwrap();
    let queued_id = queued.consumer.run_id.clone().unwrap();
    let claimed = db
        .request_check_run(before_work("two", "t2", "w2"))
        .await
        .unwrap();
    let claimed_id = claimed.consumer.run_id.clone().unwrap();
    assert_ne!(queued_id, claimed_id);
    let version: i64 = sqlx::query_scalar("SELECT version FROM check_run WHERE id=?")
        .bind(&claimed_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    db.claim_check_run(&claimed_id, version, "worker", NOW, UNTIL)
        .await
        .unwrap();
    assert_eq!(db.cancel_project_check_consumers("p").await.unwrap(), 2);
    assert_eq!(run_state(&db, &queued_id).await, "cancelled");
    assert_eq!(run_state(&db, &claimed_id).await, "running");
}

/// A queued run whose only consumer is stale by its Task's status entry
/// (nobody withdrew it: the Task moved on) is not kept waiting for a slot:
/// the worker's next admission look cancels it, withdrawing the consumer,
/// and the operator's backlog drops it. A run a live consumer waits for is
/// admitted as before.
#[tokio::test]
async fn a_queued_run_whose_consumers_are_stale_by_epoch_is_cancelled_at_the_next_admission_look() {
    let db = fixture().await;
    seed_second_task(&db).await;
    let stale = db.request_check_run(request("one")).await.unwrap();
    let stale_id = stale.consumer.run_id.clone().unwrap();
    let live = db
        .request_check_run(before_work("two", "t2", "w2"))
        .await
        .unwrap();
    let live_id = live.consumer.run_id.clone().unwrap();
    // The run waits for a slot, and its Task leaves the entry that asked.
    sqlx::query("UPDATE check_run SET capacity_wait_since=? WHERE id=?")
        .bind(NOW)
        .bind(&stale_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.check_operator_summary()
            .await
            .unwrap()
            .waiting_for_slot
            .0,
        1
    );
    let now = now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
    let run = db.check_run(&stale_id).await.unwrap().unwrap();
    assert!(matches!(
        db.admit_check_run(&run, "worker", &now, &until).await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(run_state(&db, &stale_id).await, "cancelled");
    let withdrawn: Option<String> =
        sqlx::query_scalar("SELECT cancelled_at FROM check_consumer WHERE id=?")
            .bind(&stale.consumer.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(withdrawn.is_some());
    let summary = db.check_operator_summary().await.unwrap();
    assert_eq!(summary.waiting_for_slot.0, 0);
    assert_eq!(rows(&db, "check_result").await, 0);
    let run = db.check_run(&live_id).await.unwrap().unwrap();
    assert!(matches!(
        db.admit_check_run(&run, "worker", &now, &until)
            .await
            .unwrap(),
        CheckAdmission::Admitted(_)
    ));
}

/// The races of a withdrawal, in the order SQLite serializes them (every
/// writer here takes the write lock first).
/// - A consumer that attaches before the withdrawal keeps the run queued.
/// - One that asks after it never gets the cancelled run: it schedules a
///   new one.
/// - A worker that read the run before the withdrawal cannot lease it
///   afterwards, by claim or by admission.
/// - A run a worker leased first is not cancelled by the withdrawal.
#[tokio::test]
async fn a_withdrawn_run_is_never_joined_or_leased_and_a_leased_run_is_never_cancelled_by_a_withdrawal(
) {
    let db = fixture().await;
    seed_second_task(&db).await;
    let first = db.request_check_run(request("one")).await.unwrap();
    let run_id = first.consumer.run_id.clone().unwrap();
    let read_before = db.check_run(&run_id).await.unwrap().unwrap();
    assert!(db
        .cancel_abandoned_check_consumer(&first.consumer.id)
        .await
        .unwrap());
    assert_eq!(run_state(&db, &run_id).await, "cancelled");
    let now = now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
    assert!(matches!(
        db.claim_check_run(&run_id, read_before.version, "worker", &now, &until)
            .await,
        Err(DbError::VersionConflict)
    ));
    assert!(matches!(
        db.admit_check_run(&read_before, "worker", &now, &until)
            .await,
        Err(DbError::VersionConflict)
    ));
    let current = db.check_run(&run_id).await.unwrap().unwrap();
    assert!(db
        .claim_check_run(&run_id, current.version, "worker", &now, &until)
        .await
        .is_err());
    assert_eq!(run_state(&db, &run_id).await, "cancelled");
    // The same identity asked for again, in the same instant.
    let mut other = request("two");
    other.task_id = Some("t2".into());
    let second = db.request_check_run(other).await.unwrap();
    assert!(matches!(
        second.disposition,
        CheckRequestDisposition::Scheduled
    ));
    let fresh = second.consumer.run_id.clone().unwrap();
    assert_ne!(fresh, run_id);
    assert_eq!(run_state(&db, &fresh).await, "queued");
    // Leased first, withdrawn second: the worker that holds it stops it.
    let run = db.check_run(&fresh).await.unwrap().unwrap();
    db.claim_check_run(&fresh, run.version, "worker", &now, &until)
        .await
        .unwrap();
    assert!(db
        .cancel_abandoned_check_consumer(&second.consumer.id)
        .await
        .unwrap());
    assert_eq!(run_state(&db, &fresh).await, "running");
    // Nothing reads a result of the cancelled run: no delivery, no retry,
    // no acknowledgement, and retention takes it by its state alone.
    assert_eq!(db.enqueue_check_result_steps(100).await.unwrap(), 0);
    assert!(db.retryable_check_runs(100).await.unwrap().is_empty());
    assert!(db
        .unacknowledged_checks(&now, 100)
        .await
        .unwrap()
        .is_empty());
    sqlx::query("UPDATE check_consumer SET created_at='2020-01-01T00:00:00Z'; UPDATE check_run SET finished_at='2020-01-01T00:00:00Z',updated_at='2020-01-01T00:00:00Z' WHERE state='cancelled'")
        .execute(db.pool())
        .await
        .unwrap();
    let pruned = db.prune_check_history(&now, 100).await.unwrap();
    assert_eq!((pruned.consumers, pruned.runs), (1, 1));
    assert!(db.check_run(&run_id).await.unwrap().is_none());
}

/// An integration head asks under one key for as long as its attempt and
/// candidate stand. When its consumer was withdrawn under it (a forced
/// Project deletion that was then refused) the next ask must wait again,
/// on a fresh run: the stored withdrawn row would answer every later ask
/// with nothing, and the head would time out after each Retry.
#[tokio::test]
async fn an_integration_consumer_withdrawn_under_its_head_waits_again_on_a_fresh_run_when_it_asks_again(
) {
    let db = fixture().await;
    let ask = || {
        let mut req = request("head");
        req.origin = CheckConsumerOrigin::Integration;
        req.purpose = CheckPurpose::QueueHeadCi;
        req
    };
    let first = db.request_check_run(ask()).await.unwrap();
    let first_run = first.consumer.run_id.clone().unwrap();
    assert_eq!(db.cancel_project_check_consumers("p").await.unwrap(), 1);
    assert_eq!(run_state(&db, &first_run).await, "cancelled");
    let again = db.request_check_run(ask()).await.unwrap();
    assert_eq!(again.consumer.id, first.consumer.id, "the same consumer");
    assert!(matches!(
        again.disposition,
        CheckRequestDisposition::Scheduled
    ));
    let second_run = again.consumer.run_id.clone().unwrap();
    assert_ne!(second_run, first_run);
    assert_eq!(run_state(&db, &second_run).await, "queued");
    let delivery = db
        .check_consumer_delivery(&first.consumer.id)
        .await
        .unwrap()
        .unwrap();
    assert!(delivery.cancelled_at.is_none());
    // Asked once more while that run is queued: the plain idempotent repeat.
    let repeat = db.request_check_run(ask()).await.unwrap();
    assert!(matches!(
        repeat.disposition,
        CheckRequestDisposition::Idempotent
    ));
    assert_eq!(repeat.consumer.run_id.as_deref(), Some(second_run.as_str()));
    assert_eq!(rows(&db, "check_run").await, 2);
    // While the withdrawn consumer's run is still being stopped the ask
    // changes nothing: joining it would hand over its cancellation.
    let run = db.check_run(&second_run).await.unwrap().unwrap();
    let now = now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
    let claimed = db
        .claim_check_run(&second_run, run.version, "worker", &now, &until)
        .await
        .unwrap();
    assert_eq!(db.cancel_project_check_consumers("p").await.unwrap(), 1);
    db.transition_check_run(&fence(&claimed), CheckRunState::Cancelling, &now)
        .await
        .unwrap();
    let stopping = db.request_check_run(ask()).await.unwrap();
    assert!(matches!(
        stopping.disposition,
        CheckRequestDisposition::Idempotent
    ));
    assert_eq!(rows(&db, "check_run").await, 2);
    assert!(db
        .check_consumer_delivery(&first.consumer.id)
        .await
        .unwrap()
        .unwrap()
        .cancelled_at
        .is_some());
    // A review entry's withdrawn consumer is not revived: its step asks
    // under a new Review.
    let entry = db.request_check_run(request("entry")).await.unwrap();
    assert!(db
        .cancel_abandoned_check_consumer(&entry.consumer.id)
        .await
        .unwrap());
    let entry_again = db.request_check_run(request("entry")).await.unwrap();
    assert!(matches!(
        entry_again.disposition,
        CheckRequestDisposition::Idempotent
    ));
    assert!(db
        .check_consumer_delivery(&entry.consumer.id)
        .await
        .unwrap()
        .unwrap()
        .cancelled_at
        .is_some());
}

async fn run_state(db: &SqliteDb, id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM check_run WHERE id=?")
        .bind(id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}
