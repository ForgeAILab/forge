//! Cross-owner failures, reconciliation and candidate fencing.

use super::*;

/// A red head check across owners: the rebased commit fails its real
/// `ci_steps` command on the Task's daemon. The head is sent back; the
/// server's default checkout is never touched and nothing is transferred
/// to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_red_head_check_sends_the_task_back_without_a_merge() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-red").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt", "exit 3"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    start_check_worker!(fixture, stop_checks);

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the head to be sent back", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await != "merging"
    });
    queue_eventually!(&fixture, &head, "the transfer refs to be released", {
        integration_refs(&default_checkout).is_empty()
            && integration_refs(&fixture.checkout).is_empty()
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
    assert_ne!(task_status(&fixture, &head.task_id).await, "done");
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert!(
        git_sync(&default_checkout, &["status", "--porcelain"]).is_empty(),
        "the default checkout is untouched"
    );
}

/// A conflict on the cross-owner rebase: the Task's daemon rebases onto the
/// transferred tip, commits the conflict for hand-off, and the Task is sent
/// back to its coder with the conflicted paths. Nothing reaches the server's
/// default checkout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_rebase_conflict_sends_the_task_back_with_the_conflict_handoff() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-conflict").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &[]).await;
    // The same file the candidate adds, with other content.
    std::fs::write(default_checkout.join("feature.txt"), "theirs\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();

    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the conflict to be handed off", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Ejected
            && task_status(&fixture, &head.task_id).await == "merge_failed"
    });
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(attempt.conflict_paths_json, Some(json!(["feature.txt"])));
    let bridges: Vec<(String, String)> = sqlx::query_as(
        "SELECT bridge_kind, bridge_payload FROM transition_log WHERE task_id=? AND bridge_kind IS NOT NULL",
    )
    .bind(&head.task_id)
    .fetch_all(fixture.harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(bridges.len(), 1, "{bridges:?}");
    assert_eq!(bridges[0].0, "conflict_handoff");
    assert!(bridges[0].1.contains("feature.txt"), "{bridges:?}");
    // The rebase ran on the Task's daemon; no fast-forward was asked for.
    assert_eq!(
        succeeded(&attempt, Kind::Rebase),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert!(receipt_owners(&attempt, Kind::FastForward).is_empty());
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        moved_tip
    );
    assert_eq!(
        std::fs::read_to_string(default_checkout.join("feature.txt")).unwrap(),
        "theirs\n"
    );
}

/// The Task's daemon rebases, and its reply is lost with the connection.
/// The default checkout's owner (the server) has nothing to say about that
/// effect: the receipt is asked of the Task's daemon when it is back, the
/// rebase is not repeated, and the head merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_task_daemon_disconnect_in_rebasing_is_settled_by_that_daemon() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-cross-rebase-lost").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let moved_tip = git::get_current_sha(&default_checkout).await.unwrap();
    fixture
        .link
        .as_ref()
        .unwrap()
        .drop_replies
        .lock()
        .unwrap()
        .insert(METHOD_WORKSPACE_RESET.into());
    start_check_worker!(fixture, stop_checks);
    // A lease far longer than any wait here: nothing below is a lease retry.
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(600));
    // The owner rebases: the Task's HEAD leaves the admitted candidate.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let moved = fixture
                .resolved
                .git_query(WorkspaceGitQuery::Head, false)
                .await
                .ok()
                .flatten()
                .is_some_and(|now| now.trim() != head.candidate);
            if moved {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Task's daemon rebases");
    assert_eq!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).len(),
        1
    );
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    queue_eventually!(&fixture, &head, "the head to hold an unknown rebase", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.effect_intent_json.is_some()
            && matches!(
                attempt.state,
                db::IntegrationAttemptState::Reconciling | db::IntegrationAttemptState::Quarantined
            )
    });
    {
        // The intent names the Task's daemon as the owner to ask.
        let attempt = queue_attempt(&fixture, &head).await;
        let intent = attempt.effect_intent_json.clone().unwrap();
        assert_eq!(
            intent["request"]["witness"]["effect_owner"]["daemon_id"],
            json!(fixture.daemon_id)
        );
        assert_eq!(
            intent["request"]["fence"]["target_owner"]["owner_kind"],
            "server"
        );
        assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
        assert_eq!(
            git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
            moved_tip
        );
    }
    fixture.reconnect().await;
    // The receipt is back with the connection, but the placement is still
    // `disconnected`: the head does not ask for its check yet.
    queue_eventually!(&fixture, &head, "the rebase receipt after the reconnect", {
        succeeded(&queue_attempt(&fixture, &head).await, Kind::Rebase).len() == 1
    });
    // Bounded by progress, not by a fixed sleep: the head reconciles the
    // receipt and goes back to `rebasing`, where it waits for the placement.
    queue_eventually!(&fixture, &head, "the head to wait in rebasing", {
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Rebasing
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        queue_attempt(&fixture, &head).await.state,
        db::IntegrationAttemptState::Rebasing
    );
    assert!(fixture
        .link
        .as_ref()
        .unwrap()
        .requests(METHOD_CHECK_RUN)
        .is_empty());
    // The owner reconciliation of the server runtime makes it ready again.
    fixture
        .reconcile_reconnected_placement(&fixture.resolved.placement.id)
        .await;
    queue_eventually!(&fixture, &head, "the Task to merge after the reconnect", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = assert_landed_exactly(&fixture, &head, &default_checkout, &moved_tip).await;
    // One rebase, by the daemon, never asked for again; one fast-forward.
    assert_eq!(
        receipt_owners(&attempt, Kind::Rebase)
            .into_iter()
            .map(|(_, owner)| owner)
            .collect::<Vec<_>>(),
        vec![Some(fixture.daemon_id.clone())]
    );
    assert_eq!(succeeded(&attempt, Kind::Rebase).len(), 1);
    assert!(
        attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_RESET).is_empty(),
        "the rebase was not repeated after the reconnect"
    );
    assert_eq!(succeeded(&attempt, Kind::FastForward), vec![None]);
}

/// The target's daemon fast-forwards a transferred candidate (the Task is
/// on the server) and its reply is lost with the connection. The receipt is
/// asked of that daemon when it is back; the fast-forward is not repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_target_daemon_disconnect_in_ff_inflight_is_settled_by_that_daemon() {
    use db::IntegrationOperationKind as Kind;
    let mut fixture = Fixture::file_backed("forge-queue-cross-ff-lost").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(&fixture, &placement, candidate, &[]).await;
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
    assert_eq!(task_status(&fixture, &head.task_id).await, "merging");
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
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
}

/// The default checkout moves after the cross-owner rebase and before the
/// fast-forward (a writer outside Forge, while the head check runs). The
/// server refuses to fast-forward onto a tip the candidate was not rebased
/// on; the head goes through a new round and lands on top of the new tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_target_moved_between_rebase_and_fast_forward_starts_a_new_round() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-moved").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &["test -f outside.txt && sleep 2"]).await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    // The first round's check is running on the rebased commit.
    queue_eventually!(&fixture, &head, "the first head check", {
        let attempt = queue_attempt(&fixture, &head).await;
        attempt.state == db::IntegrationAttemptState::Checking
            && !fixture
                .link
                .as_ref()
                .unwrap()
                .requests(METHOD_CHECK_RUN)
                .is_empty()
    });
    let first_candidate = queue_attempt(&fixture, &head).await.candidate_sha.unwrap();
    std::fs::write(default_checkout.join("late.txt"), "late\n").unwrap();
    git::commit_all(&default_checkout, "late").await.unwrap();
    let late_tip = git::get_current_sha(&default_checkout).await.unwrap();

    queue_eventually!(&fixture, &head, "the Task to merge", {
        task_status(&fixture, &head.task_id).await == "done"
            && queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Completed
    });
    let _ = stop.send(true);
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let attempt = queue_attempt(&fixture, &head).await;
    let tip = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(attempt.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(late_tip.as_str())
    );
    assert_ne!(
        tip, first_candidate,
        "the first round's commit never landed"
    );
    // Linear on top of the late commit: not a merge of a stale candidate.
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", &format!("{tip}^")]),
        late_tip
    );
    for file in ["feature.txt", "outside.txt", "late.txt"] {
        assert!(default_checkout.join(file).exists(), "{file}");
    }
    // Two rebases on the Task's daemon, one landed fast-forward on the server.
    assert_eq!(succeeded(&attempt, Kind::Rebase).len(), 2);
    assert_eq!(succeeded(&attempt, Kind::FastForward), vec![None]);
    assert!(
        attempt
            .phase_timings
            .as_ref()
            .is_some_and(|timings| timings.rounds >= 2),
        "{:?}",
        attempt.phase_timings
    );
}

/// The worker's cross-owner capabilities (facts, both transfers, the rebase
/// on the Task's daemon, the fast-forward on the server, the release) write
/// nothing outside the queue, the attempt and the Task-step queue. They are
/// driven directly, so no Task step runs in the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_ports_write_only_queue_attempt_and_task_step() {
    use db::IntegrationQueueRepo;
    use services::{
        daemon_transport::workspace_client::DaemonWorkspaceClient,
        integration_owner::{OwnerMergeReceipt, OwnerRebaseReceipt, ServerIntegrationOwner},
        integration_ports::{
            DaemonFences, OwnerObjectTransfer, RoutingIntegrationOwner, WorkspaceHeadFacts,
        },
        integration_worker::{
            IntegrationFactsPort, IntegrationOwnerPort, ObjectTransferEndpoint,
            ObjectTransferOutcome, ObjectTransferPort, ObjectTransferRelease,
            ObjectTransferRequest, OwnerFastForwardRequest, OwnerRebaseRequest,
        },
    };
    let fixture = Fixture::file_backed("forge-queue-cross-writes").await;
    let default_checkout = server_owned_default(&fixture).await;
    let candidate = fixture.candidate().await;
    std::fs::write(default_checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&default_checkout, "outside").await.unwrap();
    let (fence, attempt_id) = claimed_attempt(&fixture, "cross-owner-writes").await;
    let state = fixture.harness.state.clone();
    let database = state.db.clone();
    let server = Arc::new(ServerIntegrationOwner::new(database.clone()));
    let client = DaemonWorkspaceClient::new(state.daemon_connections.clone())
        .with_receipts(database.clone());
    let fences = Arc::new(DaemonFences::new(database.clone(), client.clone()));
    let owner = RoutingIntegrationOwner::new(server.clone(), client.clone(), fences.clone());
    let transfer = OwnerObjectTransfer::new(
        database.clone(),
        server,
        client.clone(),
        fences,
        &fixture.server_root.path().join("transfer-staging"),
    );
    let facts_port =
        WorkspaceHeadFacts::new(database.clone(), state.workspace_backend_router.clone())
            .with_daemons(client);
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    let queue = database
        .integration_queue(&fence.queue_id)
        .await
        .unwrap()
        .unwrap();
    async fn foreign(database: &db::SqliteDb) -> BTreeMap<String, String> {
        let mut digests = table_digests(database).await;
        // `daemon` carries the connected link's heartbeat (`last_report_at`,
        // `updated_at`, `version`), written by the connection, not by a port.
        digests.retain(|table, _| {
            ![
                "integration_queue",
                "integration_attempt",
                "task_step",
                "task_schedule_dirty",
                "daemon",
                // Written by the harness's event consumers only, and only
                // to move over events that exist. A consumer that first runs
                // after the wait below (seen under load: `notifications`
                // going from 0 to the newest setup event) is not a port
                // write. That the ports emit no event is asserted on
                // `domain_event` itself, by digest and by newest sequence.
                "event_consumer_cursor",
            ]
            .iter()
            .any(|own| table == own || table.starts_with(&format!("{own}::")))
        });
        let daemons: Vec<String> = sqlx::query_scalar(
            "SELECT id || '|' || machine_id || '|' || status || '|' || labels_json FROM daemon ORDER BY id",
        )
        .fetch_all(database.pool())
        .await
        .unwrap();
        digests.insert("daemon::stable".into(), daemons.join("\n"));
        digests
    }
    // The harness's own background consumers (event cursors, projections)
    // may still be working through the fixture's setup when the machine is
    // busy. Wait until the foreign tables stand still, so that only a port's
    // write can make them differ below.
    let mut before = foreign(&database).await;
    let mut still = 0;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = foreign(&database).await;
        if now == before {
            still += 1;
            if still >= 10 {
                break;
            }
        } else {
            still = 0;
            before = now;
        }
    }
    assert!(still >= 10, "the fixture's background writers settle");
    let cursors = |database: std::sync::Arc<db::SqliteDb>| async move {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT consumer_name,last_sequence,version FROM event_consumer_cursor ORDER BY consumer_name",
        )
        .fetch_all(database.pool())
        .await
        .unwrap();
        let events: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence,event_type FROM domain_event ORDER BY sequence DESC LIMIT 6",
        )
        .fetch_all(database.pool())
        .await
        .unwrap_or_default();
        format!("{rows:?} newest events {events:?}")
    };
    let cursors_before = cursors(database.clone()).await;
    let newest_event = |database: std::sync::Arc<db::SqliteDb>| async move {
        sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(sequence) FROM domain_event")
            .fetch_one(database.pool())
            .await
            .unwrap()
    };
    let events_before = newest_event(database.clone()).await;

    let facts = facts_port.head_facts(&attempt, &queue).await.unwrap();
    assert_eq!(facts.candidate_head, candidate);
    assert!(!facts.shared_object_store);
    let target = ObjectTransferEndpoint {
        repo_location_id: queue.target_location_id.clone().unwrap(),
        owner: services::integration_effects::EffectOwner::Server,
    };
    let request = |direction, have: &str, want: &str| ObjectTransferRequest {
        fence: fence.clone(),
        direction,
        task: facts.task_location.clone(),
        target: target.clone(),
        target_branch: "main".into(),
        have: vec![have.to_owned()],
        want: want.to_owned(),
        max_bytes: 256 * 1024 * 1024,
    };
    assert!(matches!(
        transfer
            .transfer(request(
                ObjectTransferDirection::Inbound,
                &candidate,
                &facts.target_tip
            ))
            .await
            .unwrap(),
        ObjectTransferOutcome::Transferred { .. }
    ));
    let rebased = owner
        .rebase(OwnerRebaseRequest {
            fence: fence.clone(),
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            expected_head_sha: candidate.clone(),
            expected_target_sha: facts.target_tip.clone(),
            handoff_conflicts: true,
            deadline: Duration::from_secs(60),
            cancel: tokio_util::sync::CancellationToken::new(),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            rebased,
            OwnerRebaseReceipt::Completed {
                outcome: WorkspaceOwnerOperationOutcome::Rebased
            }
        ),
        "{rebased:?}"
    );
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    let after_rebase = facts_port.head_facts(&attempt, &queue).await.unwrap();
    assert_ne!(after_rebase.candidate_head, candidate);
    assert!(after_rebase.target_in_candidate);
    assert!(matches!(
        transfer
            .transfer(request(
                ObjectTransferDirection::Outbound,
                &facts.target_tip,
                &after_rebase.candidate_head
            ))
            .await
            .unwrap(),
        ObjectTransferOutcome::Transferred { .. }
    ));
    let merged = owner
        .fast_forward(OwnerFastForwardRequest {
            fence: fence.clone(),
            workspace: after_rebase.workspace.clone(),
            target_branch: "main".into(),
            task_branch: after_rebase.task_branch.clone(),
            candidate_sha: after_rebase.candidate_head.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    assert!(
        matches!(merged, OwnerMergeReceipt::Completed { .. }),
        "{merged:?}"
    );
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        after_rebase.candidate_head
    );
    transfer
        .release(ObjectTransferRelease {
            attempt_id: attempt_id.clone(),
            task: facts.task_location.clone(),
            target,
        })
        .await
        .unwrap();
    assert!(integration_refs(&default_checkout).is_empty());
    assert!(integration_refs(&fixture.checkout).is_empty());

    let after = foreign(&database).await;
    let cursors_after = cursors(database.clone()).await;
    assert_eq!(
        newest_event(database.clone()).await,
        events_before,
        "a port emitted a domain event"
    );
    assert!(before.contains_key("domain_event"), "events are digested");
    for (table, digest) in &before {
        assert_eq!(
            after.get(table),
            Some(digest),
            "table `{table}` changed\ncursors before {cursors_before}\ncursors after  {cursors_after}"
        );
    }
    assert_eq!(after.len(), before.len());
    assert!(before.len() > 50, "the digest covers the schema");
    // Both effects left their receipt on the attempt, and nowhere else.
    let attempt = database
        .integration_attempt(&attempt_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt_receipts(&attempt).len(), 2);
}

/// R5, daemon side: a daemon that holds the default checkout fast-forwards a
/// candidate from another machine only from the ref this claim generation's
/// outbound transfer bound. Asked without that transfer, it refuses before
/// any Git effect, with a receipt, and its checkout does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_daemon_refuses_to_fast_forward_a_candidate_it_was_not_sent() {
    use services::{
        integration_owner::{OwnerEffectRefusal, OwnerMergeReceipt},
        integration_worker::{IntegrationOwnerPort, OwnerFastForwardRequest},
    };
    let fixture = Fixture::file_backed("forge-queue-cross-ff-unsent").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(&fixture, &placement, candidate.clone(), &[]).await;
    let (fence, owner, facts) = claimed_head(&fixture, &head).await;
    assert_eq!(facts.candidate_head, candidate);
    assert!(!facts.candidate_in_target, "read on the target's daemon");
    let before = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(facts.target_tip, before);
    let reflog = git_sync(&fixture.checkout, &["reflog", "--all"]);

    let refused = owner
        .fast_forward(OwnerFastForwardRequest {
            fence,
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            task_branch: facts.task_branch.clone(),
            candidate_sha: candidate.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    // The owner's gate refused before the effect started: its receipt says
    // the fast-forward was not performed.
    assert!(
        matches!(
            refused,
            OwnerMergeReceipt::NotPerformed {}
                | OwnerMergeReceipt::Refused {
                    reason: OwnerEffectRefusal::WitnessMismatch
                }
        ),
        "{refused:?}"
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        before
    );
    assert_eq!(git_sync(&fixture.checkout, &["reflog", "--all"]), reflog);
    // It was asked as a transferred fast-forward, and answered by receipt.
    let merges = attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE);
    assert_eq!(merges.len(), 1);
    assert!(merges[0]["candidate_transfer_key"].is_string());
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        receipt_owners(&attempt, db::IntegrationOperationKind::FastForward),
        vec![(
            db::IntegrationOperationState::Failed,
            Some(fixture.daemon_id.clone())
        )]
    );
    assert!(attempt.effect_intent_json.is_none(), "settled, not unknown");
}

/// R5, server side: the server fast-forwards a daemon-placed Task's
/// candidate only from the outbound transfer ref of the claim generation.
/// Without it nothing is merged, and the refusal is a settled receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_server_refuses_to_fast_forward_a_candidate_it_was_not_sent() {
    use services::{
        integration_owner::{OwnerEffectRefusal, OwnerMergeReceipt},
        integration_worker::{IntegrationOwnerPort, OwnerFastForwardRequest},
    };
    let fixture = Fixture::file_backed("forge-queue-cross-ff-unsent-server").await;
    let default_checkout = server_owned_default(&fixture).await;
    let head = admit_queue_head(&fixture, &[]).await;
    let (fence, owner, facts) = claimed_head(&fixture, &head).await;
    let before = git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]);
    assert_eq!(facts.target_tip, before);

    let refused = owner
        .fast_forward(OwnerFastForwardRequest {
            fence,
            workspace: facts.workspace.clone(),
            target_branch: "main".into(),
            task_branch: facts.task_branch.clone(),
            candidate_sha: head.candidate.clone(),
            target_sha: facts.target_tip.clone(),
            deadline: Duration::from_secs(30),
        })
        .await
        .unwrap();
    assert!(
        matches!(
            refused,
            OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch
            }
        ),
        "{refused:?}"
    );
    assert_eq!(
        git_sync(&default_checkout, &["rev-parse", "refs/heads/main"]),
        before
    );
    assert!(git_sync(&default_checkout, &["status", "--porcelain"]).is_empty());
    // The Task's daemon was not asked to merge anything.
    assert!(attempt_requests(fixture.link.as_ref().unwrap(), METHOD_WORKSPACE_MERGE).is_empty());
    let attempt = queue_attempt(&fixture, &head).await;
    assert_eq!(
        receipt_owners(&attempt, db::IntegrationOperationKind::FastForward),
        vec![(db::IntegrationOperationState::Failed, None)]
    );
    assert!(attempt.effect_intent_json.is_none(), "settled, not unknown");
}

/// The same race with the owners reversed: Task on the server, default
/// checkout on a daemon, and the daemon's checkout moves while the head
/// check runs on the server. The daemon refuses the transferred candidate
/// (its target witness no longer holds) before any effect; the head goes
/// through a new round and lands on top of the new tip.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cross_owner_daemon_target_moved_between_rebase_and_fast_forward_starts_a_new_round() {
    use db::IntegrationOperationKind as Kind;
    let fixture = Fixture::file_backed("forge-queue-cross-moved-daemon").await;
    let (placement, _task_clone, candidate) = server_placed_task(&fixture).await;
    let head = admit_placed_head(
        &fixture,
        &placement,
        candidate,
        &["test -f outside.txt && sleep 2"],
    )
    .await;
    std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&fixture.checkout, "outside").await.unwrap();
    start_check_worker!(fixture, stop_checks);
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    queue_eventually!(&fixture, &head, "the first head check", {
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .unwrap();
        queue_attempt(&fixture, &head).await.state == db::IntegrationAttemptState::Checking
            && runs > 0
    });
    let first_candidate = queue_attempt(&fixture, &head).await.candidate_sha.unwrap();
    std::fs::write(fixture.checkout.join("late.txt"), "late\n").unwrap();
    git::commit_all(&fixture.checkout, "late").await.unwrap();
    let late_tip = git::get_current_sha(&fixture.checkout).await.unwrap();

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
    assert_eq!(
        attempt.integrated_before_sha.as_deref(),
        Some(late_tip.as_str())
    );
    assert_ne!(
        tip, first_candidate,
        "the first round's commit never landed"
    );
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", &format!("{tip}^")]),
        late_tip
    );
    for file in ["feature.txt", "outside.txt", "late.txt"] {
        assert!(fixture.checkout.join(file).exists(), "{file}");
    }
    // Two rebases on the server; the daemon refused once and landed once.
    assert_eq!(succeeded(&attempt, Kind::Rebase), vec![None, None]);
    let fast_forwards = receipt_owners(&attempt, Kind::FastForward);
    assert_eq!(
        fast_forwards,
        vec![
            (
                db::IntegrationOperationState::Failed,
                Some(fixture.daemon_id.clone())
            ),
            (
                db::IntegrationOperationState::Succeeded,
                Some(fixture.daemon_id.clone())
            )
        ],
        "{:?}",
        attempt_receipts(&attempt)
    );
}
