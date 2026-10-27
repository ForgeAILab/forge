//! Reuse repository tests.
use super::*;

#[tokio::test]
async fn reusable_result_requires_exact_identity_certified_pass_and_successful_cleanup() {
    let db = fixture().await;
    let result = complete(
        &db,
        request("one"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    assert!(result.certified && result.cacheable);
    assert_eq!(
        db.reusable_check_result(&identity())
            .await
            .unwrap()
            .unwrap()
            .id,
        result.id
    );
    let reused = db.request_check_run(request("two")).await.unwrap();
    assert!(matches!(
        reused.disposition,
        CheckRequestDisposition::Reused
    ));
    assert_eq!(
        reused.consumer.result_id.as_deref(),
        Some(result.id.as_str())
    );
    for change in [
        |i: &mut CheckRunIdentity| {
            i.inputs.spec.commands[0].cacheability = CheckCacheability::Uncacheable
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.execution_revision = CheckExecutionRevision {
                number: 1,
                audit_ref: Some("force-action".into()),
            }
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.environment_identity = CheckEnvironmentIdentity::Attested {
                input_digest: "b".repeat(64),
            }
        },
        |i: &mut CheckRunIdentity| {
            i.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested
        },
        |i: &mut CheckRunIdentity| i.commit_sha = "b".repeat(40),
        |i: &mut CheckRunIdentity| i.repo_id = "different".into(),
        |i: &mut CheckRunIdentity| i.project_id = "different".into(),
    ] {
        let mut changed = identity();
        change(&mut changed);
        assert!(db.reusable_check_result(&changed).await.unwrap().is_none());
    }
    let counts = db.check_run_counts().await.unwrap();
    assert_eq!(counts.reusable_results, 1);
    assert_eq!(counts.by_state["succeeded"], 1);
}

#[tokio::test]
async fn uncacheable_and_unattested_results_always_miss_and_repeat_after_completion() {
    for unattested in [false, true] {
        let db = fixture().await;
        let mut req = request("one");
        if unattested {
            req.identity.inputs.environment_identity = CheckEnvironmentIdentity::NotAttested;
        } else {
            req.identity.inputs.spec.commands[0].cacheability = CheckCacheability::Uncacheable;
        }
        let input = req.identity.clone();
        let result = complete(&db, req, CheckResultOutcome::Pass, CheckCleanup::Success).await;
        assert!(!result.cacheable);
        assert!(db.reusable_check_result(&input).await.unwrap().is_none());
        let mut again = request("two");
        again.identity = input;
        assert!(matches!(
            db.request_check_run(again).await.unwrap().disposition,
            CheckRequestDisposition::Scheduled
        ));
    }
}

#[tokio::test]
async fn failed_timeout_cancelled_and_cleanup_failure_results_never_reuse() {
    for (outcome, cleanup) in [
        (CheckResultOutcome::Fail, CheckCleanup::Success),
        (CheckResultOutcome::TimedOut, CheckCleanup::Success),
        (CheckResultOutcome::Cancelled, CheckCleanup::Success),
        (
            CheckResultOutcome::InfrastructureFailed,
            CheckCleanup::Success,
        ),
        (CheckResultOutcome::Pass, CheckCleanup::Failed),
        (CheckResultOutcome::Pass, CheckCleanup::NotPerformed),
        (CheckResultOutcome::Pass, CheckCleanup::Uncertain),
    ] {
        let db = fixture().await;
        // The bundle declares a cleanup step, so an unperformed one is incomplete.
        let mut req = request("one");
        req.identity.inputs.spec.declares_cleanup = true;
        let declared = req.identity.clone();
        let result = complete(&db, req, outcome, cleanup).await;
        assert!(!result.certified);
        assert!(db.reusable_check_result(&declared).await.unwrap().is_none());
        assert!(db
            .reusable_check_result(&identity())
            .await
            .unwrap()
            .is_none());
        assert_eq!(db.check_run_counts().await.unwrap().reusable_results, 0);
        assert_eq!(
            db.check_result(&result.id).await.unwrap().unwrap().cleanup,
            cleanup
        );
    }
}

#[tokio::test]
async fn immutable_uncertain_receipt_can_be_followed_by_reconciled_cleanup_receipt() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let uncertain = db
        .finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Uncertain),
            NOW,
        )
        .await
        .unwrap();
    let run = db.check_run(&run.id).await.unwrap().unwrap();
    assert_eq!(run.state, CheckRunState::Uncertain);
    let run = db
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, NOW)
        .await
        .unwrap();
    let reconciled = db
        .finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW,
        )
        .await
        .unwrap();
    assert_ne!(uncertain.id, reconciled.id);
    assert_eq!(
        db.check_result(&uncertain.id)
            .await
            .unwrap()
            .unwrap()
            .cleanup,
        CheckCleanup::Uncertain
    );
    assert_eq!(
        db.reusable_check_result(&identity())
            .await
            .unwrap()
            .unwrap()
            .id,
        reconciled.id
    );
    assert_eq!(
        db.request_check_run(request("one"))
            .await
            .unwrap()
            .consumer
            .result_id
            .as_deref(),
        Some(reconciled.id.as_str())
    );
}

#[tokio::test]
async fn reuse_only_miss_creates_neither_run_nor_consumer() {
    let db = fixture().await;
    assert!(db
        .reuse_check_run(request("reuse-only-miss"))
        .await
        .unwrap()
        .is_none());
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let consumers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!((runs, consumers), (0, 0));
}

/// The integration head's reuse is applied by the step that records it: its
/// consumer is created answered and the check worker enqueues no delivery
/// step for it. An ordinary request answered by the same stored result is
/// still delivered.
#[tokio::test]
async fn a_reuse_only_hit_is_created_answered_and_gets_no_delivery_step() {
    let db = fixture().await;
    complete(
        &db,
        request("entry"),
        CheckResultOutcome::Pass,
        CheckCleanup::Success,
    )
    .await;
    assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 1);

    let mut head = request("head");
    head.origin = CheckConsumerOrigin::Integration;
    head.purpose = CheckPurpose::QueueHeadCi;
    let reused = db.reuse_check_run(head.clone()).await.unwrap().unwrap();
    assert!(matches!(
        reused.disposition,
        CheckRequestDisposition::Reused
    ));
    let state = db
        .check_consumer_delivery(&reused.consumer.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.applied_at.is_some(), "created answered");
    assert_eq!(state.delivery_step_id, None);
    assert_eq!(
        db.enqueue_check_result_steps(10).await.unwrap(),
        0,
        "no delivery step for the reuse"
    );
    let undelivered: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_consumer INDEXED BY check_consumer_undelivered WHERE result_id IS NOT NULL AND delivery_step_id IS NULL AND cancelled_at IS NULL AND applied_at IS NULL",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(undelivered, 0, "the idle probe's index is empty");
    // Asking again is the same consumer, still without a delivery.
    let again = db.reuse_check_run(head).await.unwrap().unwrap();
    assert_eq!(again.consumer.id, reused.consumer.id);
    assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 0);

    let plain = db.request_check_run(request("plain")).await.unwrap();
    assert!(matches!(plain.disposition, CheckRequestDisposition::Reused));
    assert_eq!(db.enqueue_check_result_steps(10).await.unwrap(), 1);
    // Retention: an answered reuse is finished history, not an undelivered
    // result that must be kept.
    sqlx::query("UPDATE check_consumer SET created_at='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&reused.consumer.id)
        .execute(db.pool())
        .await
        .unwrap();
    let pruned = db
        .prune_check_history("2001-01-01T00:00:00Z", 10)
        .await
        .unwrap();
    assert_eq!(pruned.consumers, 1);
}
