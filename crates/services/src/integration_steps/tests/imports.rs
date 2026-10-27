//! Imported rebases, carried reviews and durable merge evidence.
use super::*;

/// Importer row P6, Task still in `merging`: the old path rebased the
/// reviewed commit cleanly and the server was upgraded before it reported.
/// The queue checks the rebased commit and carries the passed review: one
/// check run, one carry, no reviewer, no coder, and the Task never leaves
/// `merging` before it is `done`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_imported_clean_rebase_is_checked_and_its_review_carried_without_a_new_review() {
    let mut world = World::new().await;
    let reviewed = world.legacy_task("six", "six.txt", "merging").await;
    review_ci(&world, "six", &["test -f moved"]).await;
    let contract = world.seed_passed_review("six", &["six.txt"]).await;
    let rebased = world.old_path_rebase("six").await;
    assert_ne!(reviewed, rebased);
    world
        .old_merge_step(
            "six",
            json!({"rebase_target": json!("main").to_string(), "rebase_outcome": json!({"kind":"rebased"}).to_string()}),
        )
        .await;
    let (imported, priority) = world.imported("six").await;
    assert_eq!(priority, 6);
    assert_eq!(
        (imported.state, imported.resume_state),
        (S::Parked, Some(S::Checking))
    );
    let production = Production::new(&world);
    let running = production.run();
    eventually("the rebased commit merges", || async {
        world.task("six").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    assert_eq!(tip, rebased, "the commit the old path rebased, as it is");
    let merged: String = world
        .scalar(
            "SELECT id FROM integration_attempt WHERE task_ref=? AND integrated_sha IS NOT NULL",
            "six",
        )
        .await;
    let merged = world.attempt(&merged).await;
    assert_eq!(
        merged.original_candidate_sha.as_deref(),
        Some(reviewed.as_str())
    );
    // Checked once, carried once against the reviewer's contract.
    assert_eq!(
        world
            .step("six", IntegrationStepAction::RequestCheck)
            .await
            .len(),
        1
    );
    assert_eq!(world.carries("six").await.len(), 1);
    let carried: String = world
        .scalar(
            "SELECT contract_execution_id FROM review_authority_carry WHERE task_id=?",
            "six",
        )
        .await;
    assert_eq!(carried, contract);
    // No bounce, no review, no repair.
    let transitions = world.transitions("six").await;
    assert_eq!(transitions.len(), 1, "{transitions:?}");
    assert_eq!(
        (transitions[0].0.as_str(), transitions[0].1.as_str()),
        ("merging", "done")
    );
    assert!(world
        .step("six", IntegrationStepAction::SendBack)
        .await
        .is_empty());
}

/// Importer row P9: the old path rebased, bounced and carried the review
/// (a stored carry row), and the Task was back in `merging` when the server
/// was upgraded. The carried commit is the reviewed object: it merges as it
/// is, with no new check, no second carry and no review.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_imported_stored_carry_merges_the_carried_commit_as_it_is() {
    let mut world = World::new().await;
    world.legacy_task("nine", "nine.txt", "merging").await;
    review_ci(&world, "nine", &["test -f moved"]).await;
    let contract = world.seed_passed_review("nine", &["nine.txt"]).await;
    let rebased = world.old_path_rebase("nine").await;
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    sqlx::query("INSERT INTO review_authority_carry(id,task_id,contract_execution_id,commit_sha,base_sha,kind,changed_paths_json,created_at) VALUES('carry','nine',?,?,?,'clean_rebase','[\"nine.txt\"]',?)")
        .bind(&contract).bind(&rebased).bind(&tip).bind(db::now_rfc3339())
        .execute(world.db.pool()).await.unwrap();
    let (imported, priority) = world.imported("nine").await;
    assert_eq!(priority, 9, "{:?}", imported.failure_message);
    let production = Production::new(&world);
    let running = production.run();
    eventually("the carried commit merges", || async {
        world.task("nine").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap(),
        rebased
    );
    assert!(world
        .step("nine", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert_eq!(world.carries("nine").await, vec![(rebased, tip)]);
    let transitions = world.transitions("nine").await;
    assert_eq!(transitions.len(), 1, "{transitions:?}");
}

/// Importer row P6, the `merge_failed` half of the old bounce: the Task was
/// between `merging` and its re-review on the `target_moved_rebase` bridge.
/// The continuation writes nothing: the Task's own flow continues as before
/// the upgrade (the dispatcher's mechanical hop to `review`, whose entry
/// checks the rebased commit and carries the review), no coder is asked to
/// "fix" a clean rebase by this step, and nothing can claim the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_imported_clean_rebase_parked_in_merge_failed_is_left_to_its_own_flow() {
    let mut world = World::new().await;
    world.legacy_task("pair", "pair.txt", "merge_failed").await;
    world.seed_passed_review("pair", &["pair.txt"]).await;
    world.old_path_rebase("pair").await;
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,bridge_kind,created_at) VALUES('bounce','pair','merging','merge_failed','system:workflow','target moved; rebased','target_moved_rebase',?)")
        .bind(db::now_rfc3339()).execute(world.db.pool()).await.unwrap();
    let (imported, priority) = world.imported("pair").await;
    assert_eq!(priority, 6);
    assert_eq!(imported.state, S::Parked, "not claimable");
    let production = Production::new(&world);
    let running = production.run();
    eventually("the continuation settles", || async {
        world
            .db
            .task_steps("pair")
            .await
            .unwrap()
            .iter()
            .any(|step| step.kind == INTEGRATION_STEP_KIND && step.status == "done")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.task("pair").await.status, "merge_failed");
    assert_eq!(world.transitions("pair").await.len(), 1);
    assert_eq!(world.attempt(&imported.id).await.state, S::Parked);
    assert!(world.carries("pair").await.is_empty());
    // What continues it is the dispatcher's review-refresh hop, unchanged.
    let last = db::TransitionLogRepo::list_by_task(&*world.db, "pair")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(last.bridge.is_review_refresh());
}

// ----- no wedge in `merging` (plan 3.2 stage D2b review) ---------------------

/// An actual pre-queue fast-forward survives a later Cancel and hook pruning.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_legacy_merge_that_landed_keeps_durable_evidence_after_hook_pruning() {
    let world = World::new().await;
    let candidate = world
        .legacy_task("cancelled", "landed.txt", "merging")
        .await;
    let execution = world.seed_execution("cancelled", "executor", None).await;
    world
        .old_merge_step(
            "cancelled",
            json!({"merge_intent": {
                "execution_id": execution, "workspace_id": "w-cancelled",
                "candidate_sha": candidate, "target_branch": "main"
            }}),
        )
        .await;
    git_out(&world.repo(), &["merge", "--ff-only", &candidate])
        .await
        .unwrap();
    let queue = world
        .db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let mut old = db::IntegrationAttempt::new(
        Some(queue.id),
        "cancelled".into(),
        "p".into(),
        "shadow:cancelled:0".into(),
        "merging".into(),
        0,
        1,
    );
    old.state = S::Cancelled;
    world.db.admit_integration_attempt(old).await.unwrap();
    sqlx::query("UPDATE task SET status='cancelled',status_epoch=1 WHERE id='cancelled'")
        .execute(world.db.pool())
        .await
        .unwrap();
    super::import::import_at_start(&world.db).await.unwrap();
    sqlx::query("DELETE FROM task_step WHERE id='s-cancelled'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let source: String = sqlx::query_scalar("SELECT import_source_json FROM integration_attempt WHERE task_ref='cancelled' AND import_source_json IS NOT NULL")
        .fetch_one(world.db.pool()).await.unwrap();
    assert!(
        source.contains(&candidate),
        "the real landed commit remains in durable evidence"
    );
    assert!(git::command_output(
        &world.repo(),
        &["merge-base", "--is-ancestor", &candidate, "main"]
    )
    .await
    .unwrap()
    .status
    .success());
    assert_eq!(world.task("cancelled").await.status, "cancelled");
}
