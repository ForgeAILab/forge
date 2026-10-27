//! Durable workflow check reconfiguration tests.

use super::*;

// A wait nothing will answer does not last: when the consumer is cancelled
// under a suspended step (and nobody superseded the step), the step's
// deadline wakes it and it finds the consumer lost. It closes the attempt
// that asked and asks again for its check instead of waiting on the lost
// consumer (the bound on asking again is tested by
// `an_entry_whose_check_keeps_being_withdrawn_stops_asking_after_three_times`,
// where the hook then fails with "review check was lost").
#[tokio::test]
async fn a_suspended_step_whose_consumer_is_lost_asks_again_instead_of_waiting_forever() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    enter_review(&fixture, "CI entry").await;
    // Composed, but no check worker runs: the run stays queued.
    let _ = fixture.engine.check_worker_or_embedded();
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    let suspended = "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'";
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(&fixture, suspended).await != 1 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on its check");
    let until: String =
        sqlx::query_scalar("SELECT suspended_until FROM task_step WHERE status='suspended'")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert!(
        until > (chrono::Utc::now() + chrono::Duration::seconds(3600)).to_rfc3339(),
        "the deadline lies past the run's wall limit: {until}"
    );
    sqlx::query("UPDATE check_consumer SET cancelled_at=?")
        .bind(now_rfc3339())
        .execute(fixture.db.pool())
        .await
        .unwrap();
    // The deadline passes.
    sqlx::query(
        "UPDATE task_step SET suspended_until='2000-01-01T00:00:00Z' WHERE status='suspended'",
    )
    .execute(fixture.db.pool())
    .await
    .unwrap();
    fixture.db.domain_event_notify().notify_waiters();
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
                .await
                .unwrap();
            if reviews.len() == 2
                && count(
                    &fixture,
                    "SELECT COUNT(*) FROM task_step s JOIN check_consumer c ON c.id=s.awaited_consumer_id WHERE s.status='suspended' AND c.cancelled_at IS NULL",
                )
                .await
                    == 1
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the woken step asks again for the lost check");
    let mut reviews = db::ReviewRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert!(reviews[0]
        .step_results_json
        .contains("superseded_by_withdrawn_check"));
    assert_eq!(reviews[1].status, db::ReviewStatus::Running);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
    stop.send(true).unwrap();
    worker.await.unwrap();
}

// The cross-family fence, at the delivery: review entry and the integration
// queue share the one check-witness slot of a Task status entry. A delivery
// is applied only through its own consumer's family and step, under that
// family's current authority. An integration delivery that arrives while
// review entry waits is stale and leaves the entry untouched; neither
// envelope can be re-labelled for, or carried by the step of, the other.
#[tokio::test]
async fn a_check_delivery_is_never_consumed_by_the_other_family() {
    use crate::check_runner::consumer::{CheckApplyOutcome, CheckStaleReason, TaskCheckRequest};
    let fixture = approval_ci_fixture(json!(["echo reviewed"])).await;
    enter_review(&fixture, "CI entry").await;
    let checks = fixture.engine.check_worker_or_embedded();
    let consumers = fixture
        .engine
        .check_consumers()
        .expect("the check runtime is composed");
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    let suspended = "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'";
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while count(&fixture, suspended).await != 1 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hooks step suspends on its check");
    stop.send(true).unwrap();
    worker.await.unwrap();

    let entry_id: String = sqlx::query_scalar("SELECT id FROM check_consumer WHERE origin='entry'")
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let (entry_step, entry_delivery) = delivery_of(&fixture, &checks, &entry_id).await;
    assert_eq!(entry_delivery.origin, db::CheckConsumerOrigin::Entry);

    // The integration family asks for the same commit under its own
    // authority, from a claimed step of the Task, in the same status entry.
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let epoch = db::CheckDeliveryRepo::live_task_epoch(&*fixture.db, &task.id)
        .await
        .unwrap()
        .unwrap();
    let run = db::CheckWorkerRepo::check_worker_record(&*fixture.db, &entry_delivery.run_id)
        .await
        .unwrap()
        .run;
    let asking = new_uuid_v4();
    db::TaskStepRepo::enqueue_step(
        &*fixture.db,
        &db::EnqueueTaskStep {
            kind: "command".to_owned(),
            id: asking.clone(),
            task_id: task.id.clone(),
            payload_json: "{}".to_owned(),
            causation_step_id: None,
            causation_key: asking.clone(),
            chain_id: asking.clone(),
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".to_owned(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task_step SET status='claimed',claimed_by='integration-asker',lease_until=? WHERE id=?")
        .bind(db::task_writer::lease_deadline())
        .bind(&asking)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let step = fixture
        .db
        .task_steps(&task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.id == asking)
        .unwrap();
    let requested = db::task_writer::in_task_step(
        step,
        consumers.request(TaskCheckRequest {
            task_id: task.id.clone(),
            status_epoch: epoch,
            authority: "integration-attempt-1".to_owned(),
            origin: db::CheckConsumerOrigin::Integration,
            purpose: api_types::CheckPurpose::QueueHeadCi,
            identity: run.identity.clone(),
            workspace_id: run.workspace_id.clone(),
            machine_id: None,
            wall_timeout_seconds: 60,
        }),
    )
    .await
    .expect("the integration family asks");
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&asking)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let integration_id = requested.consumer.id.clone();
    assert_ne!(
        integration_id, entry_id,
        "one consumer per family and authority"
    );
    let (integration_step, integration_delivery) =
        delivery_of(&fixture, &checks, &integration_id).await;
    assert_ne!(integration_step, entry_step);

    // Neither envelope can be re-labelled for the other family ...
    assert!(consumers
        .apply(
            &entry_step,
            &db::CheckResultDelivery {
                origin: db::CheckConsumerOrigin::Integration,
                ..entry_delivery.clone()
            },
        )
        .await
        .is_err());
    assert!(consumers
        .apply(
            &integration_step,
            &db::CheckResultDelivery {
                origin: db::CheckConsumerOrigin::Entry,
                ..integration_delivery.clone()
            },
        )
        .await
        .is_err());
    // ... nor carried by the other consumer's delivery step.
    assert!(consumers
        .apply(&integration_step, &entry_delivery)
        .await
        .is_err());
    assert!(consumers
        .apply(&entry_step, &integration_delivery)
        .await
        .is_err());

    // The integration delivery arrives while review entry waits: the Task is
    // in `review`, the integration family names no authority, so it is stale
    // and the waiting entry is not touched by it.
    assert_eq!(
        consumers
            .apply(&integration_step, &integration_delivery)
            .await
            .unwrap(),
        CheckApplyOutcome::Stale(CheckStaleReason::Authority)
    );
    assert_eq!(count(&fixture, suspended).await, 1, "the entry still waits");
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::Running);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE applied_at IS NOT NULL"
        )
        .await,
        0
    );

    // The entry's own delivery wakes the entry's step, and only that.
    assert_eq!(
        consumers.apply(&entry_step, &entry_delivery).await.unwrap(),
        CheckApplyOutcome::Applied
    );
    assert_eq!(count(&fixture, suspended).await, 0);
    assert_eq!(
        consumers.apply(&entry_step, &entry_delivery).await.unwrap(),
        CheckApplyOutcome::AlreadyApplied
    );
    let settled = drain(fixture.engine.clone(), &task.id).await;
    assert_eq!(settled.status, "review");
    let reviews = db::ReviewRepo::list_by_task(&*fixture.db, &task.id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE origin='integration' AND applied_at IS NOT NULL"
        )
        .await,
        0,
        "the integration answer was applied to nothing"
    );
    assert!(
        settled.condition.check_witness().is_none(),
        "the shared witness slot is free once both deliveries are handled: {:?}",
        settled.condition
    );
}

// The Project's review commands change while the entry check of the old
// commands is pending. The finished check answers a question nobody asks any
// more: its verdict is applied to nothing and its attempt is closed. The
// woken step asks again for the current commands inside the same review
// entry, without the owner and without spending review budget.
#[tokio::test]
async fn a_review_config_change_while_the_entry_check_is_pending_asks_again_for_the_new_commands() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo first-commands").await;
    suspend_on_entry_check(&fixture).await;
    let entered: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo other-commands").await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(reviews[0].1.contains("superseded_by_config_change"));
    assert!(
        !reviews[0].1.contains("first-commands"),
        "the old commands' verdict was written to the attempt: {reviews:?}"
    );
    assert_eq!(
        reviews[1].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[1].1.contains("other-commands"), "{reviews:?}");
    assert!(!reviews[1].1.contains("first-commands"), "{reviews:?}");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.condition.check_witness().is_none());
    // One new consumer and one new run, in the same status entry.
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(DISTINCT status_epoch) FROM check_consumer"
        )
        .await,
        1
    );
    let now: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(now, entered, "the Task did not leave and re-enter review");
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks'"
        )
        .await,
        1
    );
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0,
        "asking again spends no review budget"
    );
}

// The Task's own review commands change while the entry check is pending. No
// Project version moves, so nothing but the comparison of what the check was
// asked under notices. The stale run is still queued: its consumer is
// cancelled (the run is stopped by the worker) and the entry asks again.
#[tokio::test]
async fn a_task_review_config_change_while_the_entry_check_is_pending_asks_again() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({"retry_budgets":{"review":3},"review":{"ci_steps":["echo other-commands"]}})
                .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let second = wake_and_resuspend(&fixture, &first).await;
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NOT NULL"
        )
        .await,
        1,
        "the stale consumer is cancelled"
    );
    let waiting = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        waiting
            .condition
            .check_witness()
            .map(|(wait, _)| wait.consumer_id.clone()),
        Some(second),
        "the Task states the wait for the check it asked for now"
    );
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(!reviews[0].1.contains("first-commands"), "{reviews:?}");
    assert_eq!(reviews[1].0, db::ReviewStatus::AwaitingHuman, "{reviews:?}");
    assert!(reviews[1].1.contains("other-commands"), "{reviews:?}");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run WHERE state='succeeded'"
        )
        .await,
        1,
        "only the current commands ran to a verdict"
    );
}

// A configuration that keeps changing does not make the entry ask forever:
// after three re-requests the next change ends the step as before. The
// verdict is applied to nothing and the Task parks typed, with its actions.
#[tokio::test]
async fn an_entry_asks_again_at_most_three_times_then_parks_for_its_owner() {
    let fixture = approval_ci_fixture(json!(["echo commands-0"])).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":3}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    set_project_review_commands(&fixture, "echo commands-0").await;
    suspend_on_entry_check(&fixture).await;
    let mut awaited = awaited_consumer(&fixture).await.unwrap();
    for round in 1..=3 {
        set_project_review_commands(&fixture, &format!("echo commands-{round}")).await;
        awaited = wake_and_resuspend(&fixture, &awaited).await;
    }
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4
    );
    // The count is durable and belongs to this entry: it is an effect of
    // the entry's own hooks step (every round above ran on a new worker and
    // read it back from there), so the step of a later entry starts at zero.
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint k JOIN task_step s ON s.id=k.step_id WHERE s.status='suspended' AND s.kind='hooks' AND CAST(json_extract(k.effects_json,'$.ci_rerequests') AS INTEGER)=3"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE json_extract(effects_json,'$.ci_rerequests') IS NOT NULL"
        )
        .await,
        1
    );
    set_project_review_commands(&fixture, "echo commands-4").await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 4, "{reviews:?}");
    assert!(
        reviews
            .iter()
            .all(|review| review.0 == db::ReviewStatus::Cancelled),
        "{reviews:?}"
    );
    assert!(reviews.iter().all(|review| !review.1.contains("commands-")));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4,
        "no fifth request"
    );
    assert_eq!(task.status, "review");
    assert!(task.review_passed_at.is_none());
    assert!(task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("cascade_failed")));
    match fixture
        .engine
        .perform_task_action(&task.id, api_types::TaskAction::retry(), task.version)
        .await
    {
        Err(ServiceError::TaskActionUnavailable {
            available_actions, ..
        }) => {
            let offered: Vec<String> = available_actions
                .iter()
                .map(|offer| offer.label.clone())
                .collect();
            for label in ["Restart Task", "Request changes", "Cancel Task"] {
                assert!(offered.iter().any(|o| o == label), "{label}: {offered:?}");
            }
        }
        other => panic!("retry on a parked review entry: {:?}", other.map(|_| ())),
    }
}

// A forced Project deletion cancels the Project's check consumers and is
// then refused (a run did not settle in time). From the Task's point of view
// nothing happened: the review entry that lost its check is woken and asks
// again, instead of failing with "review check was lost".
#[tokio::test]
async fn a_refused_project_deletion_re_requests_the_entry_checks_it_cancelled() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    // Phase two of the forced delete, then the refusal.
    assert_eq!(
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 2, "{reviews:?}");
    assert_eq!(reviews[0].0, db::ReviewStatus::Cancelled, "{reviews:?}");
    assert!(reviews[0].1.contains("superseded_by_withdrawn_check"));
    assert_eq!(
        reviews[1].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[1].1.contains("entry-commands"), "{reviews:?}");
    assert_eq!(task.status, "review");
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_consumer WHERE cancelled_at IS NULL"
        )
        .await,
        1
    );
    let second: String =
        sqlx::query_scalar("SELECT id FROM check_consumer WHERE cancelled_at IS NULL")
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_ne!(second, first);
    // The re-ask got a fresh run. The withdrawn run was cancelled in the
    // withdrawal (it never ran and has no result row); the run the second
    // consumer waited for is another one, and it ran.
    let runs: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT c.id,r.state,(SELECT COUNT(*) FROM check_result x WHERE x.run_id=r.id) FROM check_consumer c JOIN check_run r ON r.id=c.run_id ORDER BY c.created_at,c.rowid",
    )
    .fetch_all(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(
        runs,
        vec![
            (first.clone(), "cancelled".to_owned(), 0),
            (second.clone(), "succeeded".to_owned(), 1),
        ]
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0
    );
    // Nothing is left to wake.
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        0
    );
}

// The usual refusal: the forced delete cancelled the consumers, the run did
// not stop within the bound, and the run is still being stopped when the
// entries are woken. A request made now would join that run and be handed
// its cancellation (a second re-request for one refusal, and after two
// refused deletes a parked Task). The entry waits for the run to end and
// asks once: one new consumer, one new run, one re-request counted.
#[tokio::test]
async fn a_refused_deletion_whose_run_is_still_stopping_asks_once_when_the_run_has_ended() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let first = awaited_consumer(&fixture).await.unwrap();
    assert_eq!(
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    sqlx::query("UPDATE check_run SET state='cancelling'")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .engine
            .resume_project_check_waits(&fixture.task.project_id)
            .await
            .unwrap(),
        1
    );
    let (stop, signal) = tokio::sync::watch::channel(false);
    let worker = Arc::new(TaskStepWorker::new(fixture.engine.clone())).start(signal);
    // Three rechecks pass while the run is being stopped: nothing is asked.
    tokio::time::sleep(std::time::Duration::from_millis(3500)).await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1,
        "the entry joined a run that is being stopped"
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM review").await, 1);
    assert_eq!(awaited_consumer(&fixture).await.as_deref(), Some(&*first));
    let steps = count(&fixture, "SELECT COUNT(*) FROM task_step").await;
    // The run ends: the next recheck asks again.
    sqlx::query("UPDATE check_run SET state='cancelled', finished_at=updated_at, lease_owner=NULL, lease_until=NULL")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let awaited = awaited_consumer(&fixture).await;
            if awaited.as_deref().is_some_and(|id| id != first) {
                break awaited.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the entry asks again once the run has ended");
    stop.send(true).unwrap();
    worker.await.unwrap();
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        2
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 2);
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM check_run r JOIN check_consumer c ON c.run_id=r.id WHERE c.id<>'' AND r.state='queued' AND c.cancelled_at IS NULL"
        )
        .await,
        1,
        "{second}"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT CAST(json_extract(effects_json,'$.ci_rerequests') AS INTEGER) FROM task_hook_checkpoint WHERE json_extract(effects_json,'$.ci_rerequests') IS NOT NULL"
        )
        .await,
        1,
        "one refusal used one re-request"
    );
    // Waiting wrote no new step: the same step was suspended again.
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM task_step").await,
        steps
    );
    assert_eq!(
        db::budget::spent(fixture.db.pool(), &fixture.task.id, "review")
            .await
            .unwrap(),
        0
    );
}

// A check that keeps being withdrawn does not make the entry ask forever:
// after three re-requests the hook fails as an unanswered check always did.
#[tokio::test]
async fn an_entry_whose_check_keeps_being_withdrawn_stops_asking_after_three_times() {
    let fixture = approval_ci_fixture(json!(["echo entry-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut awaited = awaited_consumer(&fixture).await.unwrap();
    for _ in 0..3 {
        fixture
            .db
            .cancel_project_check_consumers(&fixture.task.project_id)
            .await
            .unwrap();
        awaited = wake_and_resuspend(&fixture, &awaited).await;
    }
    fixture
        .db
        .cancel_project_check_consumers(&fixture.task.project_id)
        .await
        .unwrap();
    fixture
        .db
        .wake_suspended_hooks(&fixture.task.id, &awaited)
        .await
        .unwrap();
    drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 4, "{reviews:?}");
    assert!(reviews
        .iter()
        .all(|review| review.0 == db::ReviewStatus::Cancelled));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        4
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_step WHERE kind='hooks' AND status='suspended'"
        )
        .await,
        0,
        "the entry does not wait for a check nobody will answer"
    );
    assert_eq!(
        count(
            &fixture,
            "SELECT COUNT(*) FROM task_hook_checkpoint WHERE result_json LIKE '%review check was lost%'"
        )
        .await,
        1,
        "the failure is on the Task's history"
    );
}

// The Project environment changes while the entry check is pending (with
// the pause, the rename or the limit change that moves the Project version).
// The run that ran is the run that was asked for: its verdict is applied.
// Its result answers the old environment only: the next entry runs again.
#[tokio::test]
async fn a_project_environment_change_while_the_entry_check_is_pending_applies_the_verdict_once() {
    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    sqlx::query("UPDATE project SET settings=json_set(CASE WHEN json_valid(settings) THEN settings ELSE '{}' END,'$.environment',json('{\"env\":{\"REVIEW_ENTRY_FLAG\":\"changed\"}}')), version=version+1 WHERE id=?")
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1);
    assert_eq!(
        reviews[0].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[0].1.contains("first-commands"));
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);
    re_enter_review(&fixture, "after the environment change").await;
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_run").await,
        2,
        "the old environment's result was reused for the new one"
    );
}

// The Project workflow is edited while the entry check is pending. An edit
// that leaves the entered state as it was does not concern the entry: the
// check that ran is applied, nothing is asked again and nobody is needed.
// An edit of the entered state itself (here: its hooks) still ends the
// step: the hook list the step froze is no longer that state's, the verdict
// is applied to nothing and the Task parks for its owner.
#[tokio::test]
async fn a_workflow_edit_that_leaves_the_entered_state_alone_keeps_the_entry_check() {
    let store = |fixture: &FailedCiFixture, workflow: &api_types::WorkflowDefinition| {
        let (pool, id) = (fixture.db.pool().clone(), fixture.task.project_id.clone());
        let workflow = serde_json::to_string(workflow).unwrap();
        async move {
            sqlx::query(
                "UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?",
            )
            .bind(workflow)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut edited = fixture.workflow.clone();
    let other = edited
        .states
        .iter_mut()
        .find(|state| state.name != "review" && state.name != "in_progress")
        .expect("a state the entry does not touch");
    other.config = json!({ "edited_while_the_check_ran": true });
    store(&fixture, &edited).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(
        reviews[0].0,
        db::ReviewStatus::AwaitingHuman,
        "{reviews:?} {:?}",
        task.error_annotation
    );
    assert!(reviews[0].1.contains("first-commands"));
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1
    );
    assert_eq!(count(&fixture, "SELECT COUNT(*) FROM check_run").await, 1);

    let fixture = approval_ci_fixture(json!(["echo first-commands"])).await;
    suspend_on_entry_check(&fixture).await;
    let mut edited = fixture.workflow.clone();
    let review = edited
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap();
    let hook = review
        .hooks
        .on_enter
        .first()
        .cloned()
        .expect("the review state has entry hooks");
    review.hooks.on_enter.push(hook);
    store(&fixture, &edited).await;
    let task = drain(fixture.engine.clone(), &fixture.task.id).await;
    let reviews = review_summaries(&fixture).await;
    assert!(
        reviews
            .iter()
            .all(|review| review.0 == db::ReviewStatus::Cancelled),
        "a verdict was applied under an edited entry: {reviews:?}"
    );
    assert_eq!(task.status, "review");
    assert!(task
        .error_annotation
        .as_deref()
        .is_some_and(|annotation| annotation.contains("cascade_failed")));
    assert_eq!(
        count(&fixture, "SELECT COUNT(*) FROM check_consumer").await,
        1,
        "an entry whose state was edited asked again"
    );
}
