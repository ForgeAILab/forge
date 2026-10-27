//! Queue admission, checks, rebases and daemon reconnect reconciliation.

use super::*;

/// Admission (the write of the `merging` entry hook) binds the attempt to
/// the placement the Task merges from. Those fields are the authority for
/// the owner that may rebase the candidate, so they are asserted for a Task
/// placed off the default checkout's owner in both directions: a Task on a
/// daemon, and a Task on the server whose repository's default checkout is
/// a daemon's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_binds_the_attempt_to_the_placement_the_task_merges_from() {
    let fixture = Fixture::file_backed("forge-queue-admission").await;
    let state = &fixture.harness.state;
    let database = state.db.clone();
    let merging = |task_id: String| {
        let database = database.clone();
        async move {
            let now = db::now_rfc3339();
            sqlx::query(
                "UPDATE task SET status='merging',review_passed_at=?,updated_at=? WHERE id=?",
            )
            .bind(&now)
            .bind(&now)
            .bind(&task_id)
            .execute(database.pool())
            .await
            .unwrap();
        }
    };

    // A Task placed on a daemon.
    let placement = fixture.resolved.placement.clone();
    let candidate = fixture.candidate().await;
    merging(placement.task_id.clone()).await;
    let attempt = state
        .task_service
        .admit_to_integration_queue(&placement.task_id)
        .await
        .expect("a daemon-placed Task is admitted");
    assert_eq!(attempt.state, db::IntegrationAttemptState::Queued);
    assert_eq!(
        attempt.workspace_id.as_deref(),
        Some(placement.workspace_id.as_str())
    );
    assert_eq!(attempt.placement_id.as_deref(), Some(placement.id.as_str()));
    assert_eq!(
        attempt.repo_location_id.as_deref(),
        Some(placement.repo_location_id.as_str())
    );
    assert_eq!(attempt.owner_kind, Some(db::IntegrationOwnerKind::Daemon));
    assert_eq!(attempt.daemon_id, placement.daemon_id);
    assert!(attempt.daemon_id.is_some());
    assert_eq!(attempt.runtime_id, placement.runtime_id);
    assert_eq!(attempt.placement_generation, Some(placement.generation));
    assert_eq!(
        attempt.original_candidate_sha.as_deref(),
        Some(candidate.as_str())
    );
    // The same entry, placement and candidate: the same row.
    let again = state
        .task_service
        .admit_to_integration_queue(&placement.task_id)
        .await
        .unwrap();
    assert_eq!(again.id, attempt.id);

    // A Task placed on the server, in another clone than the daemon-owned
    // default checkout.
    let (placement, _clone, candidate) = server_placed_task(&fixture).await;
    ExecutionRepo::create(
        &*database,
        execution_input(
            &db::new_uuid_v4(),
            &placement.task_id,
            &placement.workspace_id,
            "executor",
            ExecutionStatus::Completed,
        ),
    )
    .await
    .expect("implementation execution creates");
    merging(placement.task_id.clone()).await;
    let attempt = state
        .task_service
        .admit_to_integration_queue(&placement.task_id)
        .await
        .expect("a server-placed Task is admitted");
    assert_eq!(
        attempt.workspace_id.as_deref(),
        Some(placement.workspace_id.as_str())
    );
    assert_eq!(attempt.placement_id.as_deref(), Some(placement.id.as_str()));
    assert_eq!(
        attempt.repo_location_id.as_deref(),
        Some(placement.repo_location_id.as_str())
    );
    assert_eq!(attempt.owner_kind, Some(db::IntegrationOwnerKind::Server));
    assert_eq!(attempt.daemon_id, None);
    assert_eq!(attempt.runtime_id, None);
    assert_eq!(attempt.placement_generation, Some(placement.generation));
    assert_eq!(
        attempt.original_candidate_sha.as_deref(),
        Some(candidate.as_str())
    );
    // Both Tasks merge into the one queue of the repository's target, whose
    // owner is the daemon that holds the default checkout.
    use db::IntegrationQueueRepo;
    let queue = database
        .integration_queue(attempt.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Some(queue.id.as_str()), again.queue_id.as_deref());
    assert_eq!(
        queue
            .target_owner_json
            .as_ref()
            .map(|owner| owner["owner_kind"].clone()),
        Some(json!("daemon"))
    );
}

/// A head on a daemon-owned target no longer parks: the queue worker drives
/// the rebase, the check hand-off and the fast-forward through the daemon
/// that holds the default checkout, with a real `ci_steps` command run by the
/// real check runner on that daemon.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_rebases_checks_and_fast_forwards_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-owned").await;
    // Passes only on the rebased commit: `outside.txt` arrives with the target.
    let head = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    // The check worker, as the server runtime starts it.
    let state = fixture.harness.state.clone();
    let periodic = services::worker_runtime::PeriodicWorkers::new(state.db.clone());
    let (stop_checks, checks_stopped) = tokio::sync::watch::channel(false);
    let _checks = state
        .task_service
        .compose_checks(
            state.daemon_connections.clone(),
            Duration::from_secs(60),
            &services::integration_worker::IntegrationWorkerConfig::default(),
        )
        .worker
        .start(&periodic, checks_stopped);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(moved_tip.as_str())
    );
    for file in ["feature.txt", "outside.txt"] {
        git_sync(
            &fixture.checkout,
            &["cat-file", "-e", &format!("{tip}:{file}")],
        );
    }
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    // One rebase and one fast-forward, each an attempt effect on the owner.
    let link = fixture.link.as_ref().unwrap();
    let rebases = attempt_requests(link, METHOD_WORKSPACE_RESET);
    assert_eq!(rebases.len(), 1, "{rebases:?}");
    assert_eq!(rebases[0]["operation"]["kind"], "rebase_target");
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(fast_forward_receipts(&attempt), 1);
    // The claim generation was announced with the live queues before its
    // first effect.
    let announced = link.requests(METHOD_INTEGRATION_ANNOUNCE);
    assert!(
        announced.iter().any(|params| {
            params["live_queue_ids"]
                .as_array()
                .is_some_and(|live| live.iter().any(|id| id == head.queue_id.as_str()))
        }),
        "{announced:?}"
    );
    // The check ran on the rebased commit, on the daemon, and passed.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(state.db.pool())
    .await
    .unwrap();
    assert_eq!(checks, vec![(tip.clone(), "pass".to_owned())]);
}

/// The red twin of the test above: the rebased commit's real `ci_steps`
/// command fails on the daemon. The head is ejected with the command's own
/// failure, the default checkout is not moved, and the Task is sent back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_ejects_a_red_check_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-red").await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt", "exit 3"]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let state = fixture.harness.state.clone();
    let periodic = services::worker_runtime::PeriodicWorkers::new(state.db.clone());
    let (stop_checks, checks_stopped) = tokio::sync::watch::channel(false);
    let _checks = state
        .task_service
        .compose_checks(
            state.daemon_connections.clone(),
            Duration::from_secs(60),
            &services::integration_worker::IntegrationWorkerConfig::default(),
        )
        .worker
        .start(&periodic, checks_stopped);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be ejected", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert!(
        attempt
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("exited 3")),
        "{:?}",
        attempt.failure_message
    );
    // Rebased on the daemon, checked there, never fast-forwarded.
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert_eq!(fast_forward_receipts(&attempt), 0);
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(state.db.pool())
    .await
    .unwrap();
    // The red head's check and its default-full triage rerun.
    assert_eq!(
        checks,
        vec![
            (attempt.candidate_sha.clone().unwrap(), "fail".to_owned()),
            (attempt.candidate_sha.clone().unwrap(), "fail".to_owned())
        ]
    );
    {
        use db::IntegrationQueueRepo;
        let queue = state
            .db
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert!(queue.head_attempt_id.is_none(), "the slot is free");
    }
}

/// A daemon-owned default checkout with uncommitted changes: the worker
/// reads that on the daemon and parks the head `target_dirty` at once, before
/// any effect, instead of spending rounds on the owner's refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_parks_target_dirty_at_once_on_a_daemon_owned_target() {
    let fixture = Fixture::file_backed("forge-queue-daemon-dirty").await;
    let head = admit_queue_head(&fixture, &[]).await;
    std::fs::write(fixture.checkout.join("uncommitted.txt"), "dirty\n").unwrap();
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to park", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Parked
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert!(
        attempt
            .failure_message
            .as_deref()
            .is_some_and(|message| message.starts_with("target_dirty")),
        "{:?}",
        attempt.failure_message
    );
    assert_eq!(attempt.available_at, None, "the owner decides, no retry");
    assert_eq!(
        attempt.phase_timings.as_ref().map(|timings| timings.rounds),
        Some(1)
    );
    let link = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(link, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(attempt.effect_receipts_json, json!([]));
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
}

/// The daemon finishes the queue's rebase but the reply is lost with the
/// connection. After the reconnect the worker reads the owner's receipt and
/// continues from it: the rebase is never sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_disconnect_in_rebasing_reconciles_by_receipt_without_a_second_rebase() {
    let mut fixture = Fixture::file_backed("forge-queue-daemon-rebasing").await;
    let head = admit_queue_head(&fixture, &[]).await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RESET.into());
    // A lease ten times longer than any wait of this test (each is bounded
    // at a minute): a head that merges here was not retried by its lease
    // running out. No wall-clock bound is asserted; the machine may be busy.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    // The owner rebased; its reply never arrives.
    let rebased = async {
        loop {
            let sha = fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            if !sha.trim().is_empty() && sha.trim() != head.candidate {
                return sha.trim().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let rebased = tokio::time::timeout(Duration::from_secs(30), rebased)
        .await
        .expect("the owner rebases");
    let sent_before =
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).len();
    assert_eq!(sent_before, 1);
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    // Unknown result: the attempt keeps its intent and nothing merges.
    queue_eventually!(&fixture, &head, "the head to hold an unknown rebase", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.effect_intent_json.is_some()
            && matches!(
                attempt.state,
                db::IntegrationAttemptState::Reconciling | db::IntegrationAttemptState::Quarantined
            )
    });
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");

    fixture.reconnect().await;
    // The head goes on once the Task's placement is `ready` again (the
    // server runtime's owner reconciliation after a reconnect).
    fixture
        .reconcile_reconnected_placement(&fixture.resolved.placement.id)
        .await;
    // The owner's receipt is read once and the head continues from it: the
    // rebased commit becomes the candidate without another rebase.
    let rebase_receipts = |attempt: &db::IntegrationAttempt| {
        serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(
            attempt.effect_receipts_json.clone(),
        )
        .unwrap()
        .into_iter()
        .filter(|receipt| receipt.request.kind == db::IntegrationOperationKind::Rebase)
        .collect::<Vec<_>>()
    };
    queue_eventually!(
        &fixture,
        &head,
        "the head to continue from the owner's receipt",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.candidate_sha.as_deref() == Some(rebased.as_str())
                && !matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                        | db::IntegrationAttemptState::Rebasing
                )
        }
    );
    // And the Task merges: the fast-forward that follows the reconnect is
    // admitted by the owner (its record of the default checkout is the one
    // the server stored with the reconnect verification).
    queue_eventually!(&fixture, &head, "the Task to merge after the reconnect", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    let receipts = rebase_receipts(&attempt);
    assert_eq!(receipts.len(), 1, "one rebase receipt: {receipts:?}");
    assert_eq!(
        receipts[0].operation_state,
        db::IntegrationOperationState::Succeeded
    );
    assert_eq!(receipts[0].result["outcome"]["kind"], "rebased");
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(
        attempt_requests(link, METHOD_WORKSPACE_RESET).len(),
        0,
        "the rebase was not repeated after the reconnect"
    );
    // Every effect ran once: one rebase receipt, one fast-forward receipt,
    // and no refusal in between.
    let all: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(fast_forward_receipts(&attempt), 1);
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(
        attempt.candidate_sha.as_deref(),
        Some(rebased.as_str()),
        "the one rebase is what merged"
    );
    assert_eq!(attempt.integrated_sha.as_deref(), Some(rebased.as_str()));
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        rebased
    );
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
}

/// The server changed its record of the default checkout after the owner last
/// verified it (a default toggle bumps the location version; the owner is not
/// told). The owner refuses the claim `foreign_owner`. The worker asks for the
/// location to be verified again and the head merges in a later round: it
/// does not park.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_reverifies_a_default_checkout_its_owner_does_not_recognise() {
    let fixture = Fixture::file_backed("forge-queue-daemon-location").await;
    let head = admit_queue_head(&fixture, &[]).await;
    let location_id = fixture.resolved.placement.repo_location_id.clone();
    sqlx::query("UPDATE repo_location SET version=version+1 WHERE id=?")
        .bind(&location_id)
        .execute(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let verified_before = fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_REPO_LOCATION_VERIFY)
        .len();
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    let receipts: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    let refused: Vec<_> = receipts
        .iter()
        .filter(|receipt| receipt.result["reason"] == "foreign_owner")
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "one refusal, then a verification: {receipts:?}"
    );
    assert_eq!(fast_forward_receipts(&attempt), 1);
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(
        link.requests(METHOD_REPO_LOCATION_VERIFY).len(),
        verified_before + 1,
        "the worker asked for one verification"
    );
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        head.candidate
    );
    // The owner's record and the server's agree again.
    let server: i64 = sqlx::query_scalar("SELECT version FROM repo_location WHERE id=?")
        .bind(&location_id)
        .fetch_one(fixture.harness.state.db.pool())
        .await
        .unwrap();
    let claimed: Vec<i64> = receipts
        .iter()
        .filter_map(|receipt| receipt.request.fence.target_owner["generation"].as_i64())
        .collect();
    assert_eq!(claimed.last(), Some(&server), "{claimed:?}");
}

/// The daemon fast-forwards the default checkout but the reply is lost with
/// the connection. The merge has landed; after the reconnect the worker
/// learns that from the owner's receipt and never fast-forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn daemon_disconnect_in_ff_inflight_reconciles_by_receipt_without_a_second_merge() {
    let mut fixture = Fixture::file_backed("forge-queue-daemon-ff").await;
    let head = admit_queue_head(&fixture, &[]).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    // A lease ten times longer than any wait of this test (each is bounded
    // at a minute): a head that merges here was not retried by its lease
    // running out. No wall-clock bound is asserted; the machine may be busy.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(30), async {
        while git::get_current_sha(&fixture.checkout).await.unwrap() != head.candidate {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the owner fast-forwards");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(
        &fixture,
        &head,
        "the head to hold an unknown fast-forward",
        {
            let attempt = queue_attempt(&fixture, &head).await;
            attempt.effect_intent_json.is_some()
                && matches!(
                    attempt.state,
                    db::IntegrationAttemptState::Reconciling
                        | db::IntegrationAttemptState::Quarantined
                )
        }
    );
    // The Task is not advanced on a guess, and not failed either.
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);

    // While its result is unknown the head keeps the slot under one claim.
    let held = {
        use db::IntegrationQueueRepo;
        let database = &fixture.harness.state.db;
        let queue = database
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            queue.head_attempt_id.as_deref(),
            Some(head.attempt_id.as_str())
        );
        queue.fence_generation
    };
    fixture.reconnect().await;
    queue_eventually!(
        &fixture,
        &head,
        "the Task to be done after the reconnect",
        {
            task_status(&fixture, &head.task_id).await == "done"
                && queue_attempt(&fixture, &head).await.state
                    == db::IntegrationAttemptState::Completed
        }
    );
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.integrated_sha.as_deref(),
        Some(head.candidate.as_str())
    );
    assert_eq!(fast_forward_receipts(&attempt), 1);
    // The same claim finished the head: no takeover was needed.
    let receipts: Vec<db::IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap();
    assert_eq!(receipts.len(), 1, "{receipts:?}");
    assert_eq!(receipts[0].request.fence.generation, held);
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).len(),
        0,
        "the fast-forward was not repeated after the reconnect"
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    {
        use db::IntegrationQueueRepo;
        let queue = fixture
            .harness
            .state
            .db
            .integration_queue(&head.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queue.state, db::IntegrationQueueState::Open);
        assert!(queue.head_attempt_id.is_none());
    }
}

/// Two Tasks in ONE queue on a daemon-owned default checkout, both in
/// worktrees of it. They are taken in order: the first is rebased, checked
/// green and merged; the second is then rebased onto the first one's merge,
/// its check is red, and it is sent back. The default checkout holds the
/// first Task only.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_tasks_in_one_queue_on_a_daemon_owned_target_merge_green_then_eject_red_in_order() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-daemon-two-tasks").await;
    let runtime_id = fixture.resolved.placement.runtime_id.clone().unwrap();
    let first = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    let (second_workspace, _, second_candidate) = daemon_task(
        &fixture,
        &fixture.daemon_id,
        &runtime_id,
        None,
        "second.txt",
    )
    .await;
    // Red only once it sits on top of the first Task's merge.
    let second = admit_placed_head(
        &fixture,
        &second_workspace.placement,
        second_candidate,
        &["test -f second.txt", "! test -f feature.txt"],
    )
    .await;
    assert_eq!(first.queue_id, second.queue_id, "one queue");
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &first, "the first Task to merge", {
        task_status(&fixture, &first.task_id).await == "done"
            && queue_attempt(&fixture, &first).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &second, "the second Task to be sent back", {
        queue_attempt(&fixture, &second).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &second.task_id).await != "merging"
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let merged = queue_attempt(&fixture, &first).await;
    let ejected = queue_attempt(&fixture, &second).await;
    assert!(merged.queue_seq < ejected.queue_seq);
    let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(merged.integrated_sha.as_deref(), Some(tip.as_str()));
    assert!(fixture.checkout.join("feature.txt").exists());
    assert!(!fixture.checkout.join("second.txt").exists());
    // The second head was rebased onto the first one's merge, then checked.
    assert_eq!(ejected.target_tip_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        ejected.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_ne!(task_status(&fixture, &second.task_id).await, "done");
    assert_eq!(succeeded(&merged, Kind::Rebase).len(), 1);
    assert_eq!(succeeded(&merged, Kind::FastForward).len(), 1);
    assert_eq!(succeeded(&ejected, Kind::Rebase).len(), 1);
    assert!(receipt_owners(&ejected, Kind::FastForward).is_empty());
    // Green first, red second, each on its own rebased commit; the red one
    // also ran its default-full triage rerun.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at, r.id",
    )
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(
        checks,
        vec![
            (tip, "pass".to_owned()),
            (ejected.candidate_sha.clone().unwrap(), "fail".to_owned()),
            (ejected.candidate_sha.clone().unwrap(), "fail".to_owned())
        ]
    );
    {
        use db::IntegrationQueueRepo;
        let queue = fixture
            .harness
            .state
            .db
            .integration_queue(&first.queue_id)
            .await
            .unwrap()
            .unwrap();
        assert!(queue.head_attempt_id.is_none(), "the slot is free");
    }
}
