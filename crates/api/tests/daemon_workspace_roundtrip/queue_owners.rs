//! Queue integration across server checkouts and daemon owners.

use super::*;

/// Task on a daemon, default checkout on the server. The target moved, so
/// the head needs the whole path: the target tip goes to the daemon, the
/// rebase and the real head check run there, the exact rebased commit goes
/// to the server and is fast-forwarded there.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_a_daemon_placed_task_into_a_server_owned_default_checkout() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-daemon-server").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f feature.txt", "test -f outside.txt"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    let task_location_tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    // The attempt left the slot: its transfer refs go on both ends.
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&default_checkout).is_empty()
            && integration_refs(&fixture.checkout).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert!(
        attempt.failure_message.is_none(),
        "never parked: {:?}",
        attempt.failure_message
    );
    // The default checkout holds exactly the checked candidate.
    let tip = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.candidate_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(moved_tip.as_str())
    );
    assert_ne!(tip, head.candidate, "the queue rebased the candidate");
    assert_eq!(git_sync(&default_checkout, &["rev-parse", "HEAD"]), tip);
    for file in ["feature.txt", "outside.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    // The Task's own location was neither merged nor advanced.
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        task_location_tip
    );
    // One rebase, by the daemon; one fast-forward, by the server.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, None)]
    );
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(link, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(
        !link.requests(METHOD_INTEGRATION_ANNOUNCE).is_empty(),
        "the Task's owner was told the claim generation"
    );
    // The check ran on the rebased commit, on the daemon, and passed.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id",
    )
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(checks, vec![(tip, "pass".to_owned())]);
}

/// The reverse: Task on the server, default checkout on a daemon. The
/// rebase runs on the server against the transferred target tip; the daemon
/// fast-forwards its checkout from the transferred candidate, without a
/// worktree of the Task.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_a_server_placed_task_into_a_daemon_owned_default_checkout() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-server-daemon").await;
    let (placement, task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(
        &fixture,
        &placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own location was not advanced"
    );
    // One rebase, by the server; one fast-forward, by the daemon.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, None)]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    let link = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(link, METHOD_WORKSPACE_RESET).is_empty());
    let merges = attempt_requests(link, METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert!(merges[0]["candidate_transfer_key"].is_string());
    assert_eq!(merges[0]["workspace_handle"], "");
}

/// Two daemons: the Task on one, the default checkout on the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_across_two_daemons() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-two-daemons").await;
    let second = extra_daemon(&fixture, "forge-queue-cross-two-daemons").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own location was not advanced"
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, Some(second.daemon_id.clone()))]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, Some(fixture.daemon_id.clone()))]
    );
    // Each owner was asked for its own effect only, after it was told the
    // claim generation.
    let target = fixture.link.as_ref().unwrap();
    let tasks = second.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(attempt_requests(target, METHOD_WORKSPACE_MERGE).len(), 1);
    assert_eq!(attempt_requests(tasks, METHOD_WORKSPACE_RESET).len(), 1);
    assert!(attempt_requests(tasks, METHOD_WORKSPACE_MERGE).is_empty());
    for link in [target, tasks] {
        assert!(!link.requests(METHOD_INTEGRATION_ANNOUNCE).is_empty());
    }
    // The check ran where the Task is.
    assert!(!tasks.requests(METHOD_CHECK_RUN).is_empty());
    assert!(target.requests(METHOD_CHECK_RUN).is_empty());
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, red head check: the Task's daemon rebases and runs the
/// check, which fails. The Task is sent back; the other daemon's default
/// checkout is never asked to fast-forward and holds no transfer ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_red_head_check_sends_the_task_back_without_a_merge() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-two-daemons-red").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-red").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f outside.txt", "exit 3"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be sent back", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        attempt.failure_kind,
        Some(db::IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(second.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    let target = fixture.link.as_ref().unwrap();
    let tasks = second.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert!(!tasks.requests(METHOD_CHECK_RUN).is_empty());
    assert!(target.requests(METHOD_CHECK_RUN).is_empty());
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert!(
        git_sync(&fixture.checkout, &["status", "--porcelain"]).is_empty(),
        "the default checkout is untouched"
    );
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, conflict on the rebase: the Task's daemon rebases onto the
/// tip the other daemon's default checkout sent, commits the conflict for
/// hand-off, and the Task goes back to its coder with the conflicted path.
/// The default checkout's daemon performs no effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_rebase_conflict_sends_the_task_back_with_the_conflict_handoff() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-two-daemons-conflict").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-conflict").await;
    let (resolved, _task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(&fixture, &resolved.placement, candidate, &[]).await;
    // The same file the candidate adds, with other content.
    std::fs::write(fixture.checkout.join("feature.txt"), "theirs\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the conflict to be handed off", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await == "merge_failed"
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(attempt.conflict_paths_json, Some(json!(["feature.txt"])));
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(second.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    let target = fixture.link.as_ref().unwrap();
    assert!(attempt_requests(target, METHOD_WORKSPACE_MERGE).is_empty());
    assert!(attempt_requests(target, METHOD_WORKSPACE_RESET).is_empty());
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_eq!(
        std::fs::read_to_string(fixture.checkout.join("feature.txt")).unwrap(),
        "theirs\n"
    );
    let _ = (&second.runtime, &second.token);
}

/// Two daemons, lost fast-forward reply: the Task is on one daemon, the
/// default checkout on the other, whose reply is lost with its connection.
/// The Task's daemon stays connected and is never asked about that effect;
/// the receipt comes from the daemon that performed it when it is back, and
/// the branch is moved once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_daemons_lost_fast_forward_reply_is_settled_by_the_target_daemon_alone() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-two-daemons-ff-lost").await;
    let second = extra_daemon(&fixture, "forge-queue-two-daemons-ff-lost").await;
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &second.daemon_id,
        &second.runtime_id,
        second.root.path(),
    )
    .await;
    let head = admit_placed_head(&fixture, &resolved.placement, candidate, &[]).await;
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_MERGE.into());
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    tokio::time::timeout(Duration::from_secs(30), async {
        while git::get_current_sha(&fixture.checkout).await.unwrap() != head.candidate {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the target's daemon fast-forwards the transferred candidate");
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
    {
        // The unknown effect belongs to the default checkout's daemon. The
        // Task's daemon is connected and has nothing to say about it: the
        // head stays unknown until the performing owner is back.
        let attempt = queue_attempt(&fixture, &head).await;
        let intent = attempt.effect_intent_json.clone().unwrap();
        assert!(intent["request"]["witness"].get("effect_owner").is_none());
        assert_eq!(
            intent["request"]["fence"]["target_owner"]["daemon_id"],
            json!(fixture.daemon_id)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(queue_attempt(&fixture, &head)
            .await
            .effect_intent_json
            .is_some());
        assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
    }
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);
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
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(
            db::IntegrationOperationState::Succeeded,
            Some(fixture.daemon_id.clone())
        )]
    );
    assert!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty(),
        "the fast-forward was not repeated after the reconnect"
    );
    assert!(
        attempt_requests(second.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty(),
        "the Task's daemon was never asked to fast-forward"
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = (&second.runtime, &second.token);
}

/// Two clones on ONE daemon: the Task in one, the default checkout in the
/// other. One owner performs both effects; the fast-forward takes the
/// candidate from the transferred ref (the clones share no Git common dir).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queue_worker_merges_between_two_clones_on_one_daemon() {
    use db::{IntegrationOperationKind as Kind, IntegrationOperationState as State};
    let fixture = Fixture::file_backed("forge-queue-cross-clone-daemon").await;
    let runtime_id = fixture.resolved.placement.runtime_id.clone().unwrap();
    let (resolved, task_clone, candidate) = daemon_clone_task(
        &fixture,
        &fixture.daemon_id,
        &runtime_id,
        fixture._daemon_root.path(),
    )
    .await;
    let head = admit_placed_head(
        &fixture,
        &resolved.placement,
        candidate,
        &["test -f feature.txt", "test -f outside.txt"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
    let task_location_tip = git_sync(&task_clone, &["rev-parse", "refs/heads/main"]);
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&fixture.checkout).is_empty() && integration_refs(&task_clone).is_empty()
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &fixture.checkout, &moved_tip).await;
    assert_eq!(
        git_sync(&task_clone, &["rev-parse", "refs/heads/main"]),
        task_location_tip,
        "the Task's own clone was not advanced"
    );
    let owner = Some(fixture.daemon_id.clone());
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase),
        vec![(State::Succeeded, owner.clone())]
    );
    assert_eq!(
        receipt_owners(&attempt, Kind::FastForward),
        vec![(State::Succeeded, owner)]
    );
    let link = fixture.link.as_ref().unwrap();
    assert_eq!(attempt_requests(link, METHOD_WORKSPACE_RESET).len(), 1);
    let merges = attempt_requests(link, METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert!(merges[0]["candidate_transfer_key"].is_string());
}
