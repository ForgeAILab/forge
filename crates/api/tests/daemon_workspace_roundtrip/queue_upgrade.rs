//! Pre-queue imports and recovered daemon queue placements.

use super::*;

/// Importer row P15 on a real daemon-placed Task: the old build was merging
/// when the daemon that holds the Task's checkout dropped off. It left the
/// Task in `merging` with its owner wait (`owner_wait`, an expired
/// `deferred_dispatch`), the placement `disconnected`, and no attempt. The
/// upgraded server starts with the daemon still away: the Task says, typed,
/// which machine it waits for and nothing merges. When the daemon reconnects
/// and its placement is `ready` again, the Task merges. No owner action is
/// taken anywhere in this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_pre_queue_merge_waiting_for_its_disconnected_daemon_merges_on_reconnect() {
    use db::{IntegrationQueueRepo, TaskStepRepo};
    let mut fixture = Fixture::file_backed("forge-queue-p15-reconnect").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let placement_id = fixture.resolved.placement.id.clone();

    // The daemon goes away while the old build is in `merging`.
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    let now = db::now_rfc3339();
    sqlx::query(
        "UPDATE task SET status='merging',review_passed_at=?1,updated_at=?1,version=version+1,\
           metadata_json=json_set(COALESCE(metadata_json,'{}'),\
             '$.owner_wait',json(?2),'$.deferred_dispatch',json(?3)) WHERE id=?4",
    )
    .bind(&now)
    .bind(json!({"daemon_id": fixture.daemon_id, "started_at": now}).to_string())
    .bind(
        json!({"target_state":"merging","reason":"daemon unavailable",
               "not_before":"2026-01-01T00:00:00+00:00"})
        .to_string(),
    )
    .bind(&task_id)
    .execute(database.pool())
    .await
    .unwrap();
    // What the old build's heartbeat monitor did for the daemon it lost.
    db::DaemonRepo::mark_offline(&*database, &fixture.daemon_id, &now)
        .await
        .unwrap();
    let monitor =
        services::HeartbeatMonitor::new(database.clone(), fixture.harness.state.event_bus.clone())
            .with_daemon_connections(fixture.harness.state.daemon_connections.clone())
            .with_task_service(service.clone());
    monitor.check_once().await.unwrap();
    let placement = WorkspacePlacementRepo::get_by_id(&*database, &placement_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(placement.state, PlacementState::Disconnected);
    // The Task's stored condition follows its row, as every writer leaves it.
    let mut tx = db::begin_immediate(database.pool()).await.unwrap();
    database
        .produce_condition_in_tx(&mut tx, &task_id, db::ConditionChange::Full)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(database
        .current_integration_attempt(&task_id)
        .await
        .unwrap()
        .is_none());

    // The upgraded server starts: the import, then the workers.
    let report = services::integration_steps::import::import_at_start(&database)
        .await
        .unwrap();
    assert_eq!(
        (report.imported, report.continuations),
        (1, 1),
        "{report:?}"
    );
    let imported = database
        .current_integration_attempt(&task_id)
        .await
        .unwrap()
        .expect("imported");
    assert_eq!(
        imported.import_source_json.as_ref().unwrap()["priority"],
        15,
        "{:?}",
        imported.import_source_json
    );
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    let condition = |task: &db::Task| json!(task.condition.integration_reason());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let task = TaskRepo::get_by_id(&*database, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let stated = condition(&task);
        if stated["kind"] == "deferred" {
            assert_eq!(stated["cause"], "owner_offline", "{stated}");
            assert_eq!(stated["owner_id"], fixture.daemon_id.as_str(), "{stated}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the Task never stated its owner wait: {stated}, condition {}, steps {:#?}",
            json!(task.condition)
                .to_string()
                .chars()
                .take(600)
                .collect::<String>(),
            database
                .task_steps(&task_id)
                .await
                .unwrap()
                .iter()
                .map(|s| (
                    s.kind.clone(),
                    s.causation_key.clone(),
                    s.status.clone(),
                    s.last_error.clone()
                ))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    // While the daemon is away nothing merges and the Task is not blocked.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waiting = TaskRepo::get_by_id(&*database, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "merging");
    assert!(waiting.blocked_json.is_none(), "{:?}", waiting.blocked_json);
    assert_ne!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        candidate
    );

    // The daemon is back; the runtime's owner reconciliation makes the
    // placement `ready`, which wakes the merge.
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            Arc::clone(&fixture.runtime),
        )
        .await,
    );
    fixture.reconcile_reconnected_placement(&placement_id).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let task = TaskRepo::get_by_id(&*database, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        if task.status == "done" {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let attempt = database
                .current_integration_attempt(&task_id)
                .await
                .unwrap();
            panic!(
                "the Task did not merge after the reconnect: status {}, condition {}, blocked {:?}, attempt {:?}, steps {:#?}",
                task.status,
                condition(&task),
                task.blocked_json,
                attempt.map(|a| (a.state, a.failure_message, a.available_at)),
                database
                    .task_steps(&task_id)
                    .await
                    .unwrap()
                    .iter()
                    .map(|s| (s.kind.clone(), s.causation_key.clone(), s.status.clone(), s.last_error.clone()))
                    .collect::<Vec<_>>()
            );
        }
        // The dispatcher's sweep is not run: the reconnect alone wakes it.
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        candidate,
        "the reviewed commit landed as it was"
    );
    let _ = service;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn open_item_2_retry_of_a_quarantined_import_waits_for_its_offline_daemon() {
    let mut fixture = Fixture::file_backed("forge-quarantine-offline-retry").await;
    let candidate = fixture.candidate().await;
    let database = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let placement_id = fixture.resolved.placement.id.clone();
    fixture.link.take();
    common::fake_daemon::wait_until_disconnected(&fixture.harness.state, &fixture.daemon_id).await;
    sqlx::query("UPDATE task SET status='merging' WHERE id=?")
        .bind(&task_id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution WHERE task_id=?")
        .bind(&task_id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state='disconnected' WHERE id=?")
        .bind(&placement_id)
        .execute(database.pool())
        .await
        .unwrap();
    let report = services::integration_steps::import::import_at_start(&database)
        .await
        .unwrap();
    assert_eq!(report.quarantined, 1);
    service.drain(&task_id).await.unwrap();
    let parked = TaskRepo::get_by_id(&*database, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let actor = Actor::user(UserActionSource::Api);
    let offers = service.task_action_offers(&task_id, &actor).await.unwrap();
    assert!(
        offers
            .available_actions
            .iter()
            .any(|offer| offer.reason == "integration_retry"),
        "{offers:?}"
    );
    let accepted = service
        .perform_task_action_as(&task_id, TaskAction::retry(), parked.version, actor)
        .await
        .expect("an offered Retry is accepted while the checkout owner is offline");
    let reason = json!(accepted.task.condition.integration_reason());
    assert_eq!(reason["cause"], "owner_offline", "{reason}");
    assert_eq!(reason["owner_id"], fixture.daemon_id);
    assert_ne!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        candidate
    );
    // This successor has no pinned legacy execution, so it owes a fresh
    // head check. The test app started before drain composed that runtime.
    let periodic = services::worker_runtime::PeriodicWorkers::new(database.clone());
    let (stop_checks, checks_stopped) = tokio::sync::watch::channel(false);
    let checks = service
        .compose_checks(
            fixture.harness.state.daemon_connections.clone(),
            Duration::from_secs(60),
            &services::integration_worker::IntegrationWorkerConfig::default(),
        )
        .worker
        .start(&periodic, checks_stopped);
    let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
    fixture.link = Some(
        DaemonLink::connect(
            &fixture.server,
            &fixture.daemon_id,
            &fixture.token,
            fixture.runtime.clone(),
        )
        .await,
    );
    fixture.reconcile_reconnected_placement(&placement_id).await;
    service.integration_owner_ready(&task_id).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if TaskRepo::get_by_id(&*database, &task_id, false)
                .await
                .unwrap()
                .unwrap()
                .status
                == "done"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let _ = stop_checks.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), checks).await;
    if result.is_err() {
        use db::{IntegrationQueueRepo, TaskStepRepo};
        let task = TaskRepo::get_by_id(&*database, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        panic!("the accepted Retry did not continue: status {}, condition {}, attempt {:?}, steps {:?}", task.status,
            json!(task.condition.public()), database.current_integration_attempt(&task_id).await.unwrap().map(|a| (a.state, a.failure_message)),
            database.task_steps(&task_id).await.unwrap().into_iter().map(|s| (s.kind, s.status, s.last_error)).collect::<Vec<_>>());
    }
    assert_eq!(
        git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]),
        candidate
    );
}

/// A head that was admitted before its daemon came back under another
/// runtime id (or with another placement generation) and was never parked:
/// the queue worker claims it with the binding it has. Through the real
/// owner it merges from the workspace the placement names now: every effect
/// is addressed from the current placement row, so the stale binding of the
/// attempt reaches no daemon and nothing parks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_queued_head_bound_to_the_daemons_previous_runtime_merges_from_its_current_placement() {
    for changed in ["runtime", "generation"] {
        let fixture = Fixture::file_backed("forge-queue-stale-head").await;
        let head = admit_queue_head(&fixture, &[]).await;
        let database = fixture.harness.state.db.clone();
        let sql = if changed == "runtime" {
            "UPDATE integration_attempt SET runtime_id='gone-runtime' WHERE id=?"
        } else {
            "UPDATE integration_attempt SET placement_generation=0 WHERE id=?"
        };
        sqlx::query(sql)
            .bind(&head.attempt_id)
            .execute(database.pool())
            .await
            .unwrap();
        // A rebase is the first effect that names the owner.
        std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
        git::commit_all(&fixture.checkout, "target advanced")
            .await
            .unwrap();
        let moved_tip = git::get_current_sha(&fixture.checkout).await.unwrap();
        let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut seen = Vec::new();
        let outcome = loop {
            let status = task_status(&fixture, &head.task_id).await;
            let attempts: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT id,state,failure_kind,failure_message FROM integration_attempt WHERE task_ref=? ORDER BY created_at,id",
            )
            .bind(&head.task_id)
            .fetch_all(database.pool())
            .await
            .unwrap();
            let line = format!("{status} {attempts:?}");
            if seen.last() != Some(&line) {
                seen.push(line);
            }
            if status == "done" {
                break "merged";
            }
            if attempts.iter().any(|(_, state, _, _)| state == "parked") {
                break "parked";
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{changed}: the stale-bound head neither merged nor parked: {seen:#?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        let _ = stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        // What the product does: the driver takes the Task's workspace owner
        // from the placement as it is now, not from the binding the attempt
        // was admitted with, so the head merges and nothing parks.
        assert_eq!(
            outcome, "merged",
            "{changed}: the stale-bound head parked: {seen:#?}"
        );
        let tip = git_sync(&fixture.checkout, &["rev-parse", "refs/heads/main"]);
        assert_ne!(tip, moved_tip, "{changed}: nothing landed: {seen:#?}");
        for file in ["feature.txt", "outside.txt"] {
            git_sync(
                &fixture.checkout,
                &["cat-file", "-e", &format!("{tip}:{file}")],
            );
        }
        // One rebase and one fast-forward reached the daemon, each naming
        // the placement's runtime and generation, never the stale binding.
        let placement = &fixture.resolved.placement;
        let link = fixture.link.as_ref().unwrap();
        for method in [METHOD_WORKSPACE_RESET, METHOD_WORKSPACE_MERGE] {
            let requests = attempt_requests(link, method);
            assert_eq!(requests.len(), 1, "{changed}: {method}: {requests:?}");
            let params = &requests[0];
            assert_eq!(
                params["runtime_id"].as_str(),
                placement.runtime_id.as_deref(),
                "{changed}: {method}"
            );
            assert_eq!(params["generation"], placement.generation, "{changed}");
            assert_eq!(params["placement_id"], placement.id.as_str(), "{changed}");
            let owner = &params["integration"]["request"]["witness"]["workspace"];
            assert_eq!(
                owner["owner"]["runtime_id"].as_str(),
                placement.runtime_id.as_deref(),
                "{changed}: {method} witness"
            );
            assert_eq!(owner["generation"], placement.generation, "{changed}");
            assert!(
                !params.to_string().contains("gone-runtime"),
                "{changed}: {method} was sent under the stale runtime"
            );
        }
        // The same attempt merged: no successor was needed.
        let attempts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?")
                .bind(&head.task_id)
                .fetch_one(database.pool())
                .await
                .unwrap();
        assert_eq!(attempts, 1, "{changed}: {seen:#?}");
    }
}

/// A parked attempt still names the runtime/generation that admitted it,
/// while reconnect recovery has verified its placement on the current owner.
/// Recovery must replace that binding before the next rebase is requested.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_parked_daemon_attempt_rebinds_after_runtime_or_placement_recovery() {
    use db::IntegrationQueueRepo;
    for changed in ["runtime", "generation"] {
        let fixture = Fixture::file_backed("forge-queue-rebind").await;
        let head = admit_queue_head(&fixture, &[]).await;
        let database = fixture.harness.state.db.clone();
        // Keep the ready, verified fake daemon placement, but give the
        // parked attempt the obsolete binding it had before recovery.
        let sql = if changed == "runtime" {
            "UPDATE integration_attempt SET state='parked',runtime_id='gone-runtime',failure_kind='infrastructure',failure_message='owner_offline: owner absent',available_at=NULL WHERE id=?"
        } else {
            "UPDATE integration_attempt SET state='parked',placement_generation=0,failure_kind='infrastructure',failure_message='owner_offline: owner absent',available_at=NULL WHERE id=?"
        };
        sqlx::query(sql)
            .bind(&head.attempt_id)
            .execute(database.pool())
            .await
            .unwrap();
        // Force a rebase, the effect that used to reject the old owner.
        std::fs::write(fixture.checkout.join("outside.txt"), "outside\n").unwrap();
        git::commit_all(&fixture.checkout, "target advanced")
            .await
            .unwrap();
        fixture
            .harness
            .state
            .task_service
            .integration_owner_ready(&head.task_id)
            .await
            .unwrap();
        fixture
            .harness
            .state
            .task_service
            .task_step_worker()
            .drain(&head.task_id)
            .await
            .unwrap();
        let successor: Option<String> =
            sqlx::query_scalar("SELECT id FROM integration_attempt WHERE predecessor_attempt_id=?")
                .bind(&head.attempt_id)
                .fetch_optional(database.pool())
                .await
                .unwrap();
        let rebound = database
            .integration_attempt(&successor.expect("recovery admitted a successor"))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            rebound.id, head.attempt_id,
            "{changed}: the obsolete binding must be released"
        );
        assert_eq!(rebound.runtime_id, fixture.resolved.placement.runtime_id);
        assert_eq!(
            rebound.placement_generation,
            Some(fixture.resolved.placement.generation)
        );
        assert_eq!(
            rebound.queue_seq,
            queue_attempt(&fixture, &head).await.queue_seq
        );
        assert_eq!(
            queue_attempt(&fixture, &head).await.state,
            db::IntegrationAttemptState::Superseded
        );
        assert_ne!(
            rebound.candidate_sha.as_deref(),
            Some(head.candidate.as_str())
        );
        assert!(
            rebound.integrated_sha.is_some(),
            "the replacement actually rebased and merged"
        );
        let (stop, running) = run_queue_worker(&fixture, Duration::from_secs(60));
        queue_eventually!(&fixture, &head, "the rebound Task to merge", {
            task_status(&fixture, &head.task_id).await == "done"
        });
        let _ = stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    }
}
