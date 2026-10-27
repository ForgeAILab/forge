//! Interrupted imports, stopped rebases and unavailable integration owners.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quarantined_imports_without_execution_or_pinned_workspace_park_and_retry_current_head() {
    for missing_execution in [true, false] {
        let world = World::new().await;
        let old_head = world.legacy_task("orphan", "orphan.txt", "merging").await;
        if !missing_execution {
            let execution = world.seed_execution("orphan", "executor", None).await;
            sqlx::query("UPDATE execution SET workspace_id=NULL WHERE id=?")
                .bind(execution)
                .execute(world.db.pool())
                .await
                .unwrap();
        }
        // Reproduce a previous build's start: it committed the import but
        // never enqueued a continuation for its non-current quarantine.
        let imported = world.db.import_integration_pass(100).await.unwrap();
        assert_eq!(imported.quarantined, 1);
        let id: String =
            sqlx::query_scalar("SELECT id FROM integration_attempt WHERE task_ref='orphan'")
                .fetch_one(world.db.pool())
                .await
                .unwrap();
        let orphan = world.attempt(&id).await;
        assert!(!orphan.current);
        assert_eq!(orphan.state, S::Quarantined);
        assert_eq!(
            orphan.failure_kind,
            Some(db::IntegrationFailureKind::CorruptImport)
        );
        assert_eq!(
            orphan.failure_message.as_deref(),
            Some("missing execution or pinned workspace")
        );
        let report = import::import_at_start(&world.db).await.unwrap();
        assert_eq!(report.imported, 0);
        assert_eq!(
            report.continuations, 1,
            "a previous start's quarantine needs its park step"
        );
        world
            .until("orphan", "the quarantine is an owner park", || async {
                integration(&world.task("orphan").await)["cause"] == "owner_required"
            })
            .await;
        let parked = world.task("orphan").await;
        assert_eq!(parked.status, "merging");
        assert!(matches!(parked.condition, db::TaskCondition::Parked { .. }));
        assert_eq!(
            integration(&parked)["message"],
            "missing execution or pinned workspace"
        );
        let summary = crate::integration_reads::task_integration(&world.db, &parked)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(summary.state, api_types::IntegrationMemberState::Deferred);
        assert_eq!(
            summary.wait.unwrap().code,
            api_types::IntegrationWaitCode::OwnerRequired
        );
        assert_eq!(
            summary.action_hint,
            Some(api_types::IntegrationActionHint::Retry)
        );
        let offers = world.offers("orphan").await;
        assert!(
            offers.contains(&("retry".into(), "integration_retry".into())),
            "{offers:?}"
        );
        assert!(offers.iter().any(|(verb, _)| verb == "cancel"));
        crate::AttentionService::new(world.db.clone(), crate::test_support::noop_wake_turns())
            .project_once(100)
            .await
            .unwrap();
        let attention: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE status='open' AND json_extract(details_json,'$.entity_id')='orphan'")
            .fetch_one(world.db.pool()).await.unwrap();
        assert_eq!(attention, 1);
        let operations = crate::OperatorStatusService::new_for_test(world.db.clone())
            .compute_status()
            .await
            .unwrap();
        assert_eq!(operations.integration_queues.quarantined_imports, 1);
        let unchanged = import::import_at_start(&world.db).await.unwrap();
        assert_eq!((unchanged.imported, unchanged.continuations), (0, 0));

        // An unpinned historical execution cannot prove review provenance.
        // Supply a present implementation run rather than rewriting history.
        // The no-execution case also proves Retry can use the current branch
        // without inventing a historical run.
        if !missing_execution {
            world.seed_execution("orphan", "executor", None).await;
        }
        // Retry must bind and check the present HEAD, not the import's candidate.
        std::fs::write(world.tree("orphan").join("current.txt"), "current\n").unwrap();
        git::commit_all(&world.tree("orphan"), "current candidate")
            .await
            .unwrap();
        let current_head = git::get_current_sha(&world.tree("orphan")).await.unwrap();
        assert_ne!(old_head, current_head);
        review_ci(&world, "orphan", &["test -f current.txt"]).await;
        let production = Production::new(&world);
        world
            .act(
                "orphan",
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
            )
            .await;
        let admitted = world
            .db
            .current_integration_attempt("orphan")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admitted.state, S::Queued);
        assert_ne!(admitted.id, orphan.id);
        assert_eq!(
            admitted.original_candidate_sha.as_deref(),
            Some(current_head.as_str())
        );
        let running = production.run();
        world
            .until(
                "orphan",
                "Retry validates and merges the present branch",
                || async { world.task("orphan").await.status == "done" },
            )
            .await;
        assert!(world.on_main("current.txt").await);
        assert_eq!(world.merges("orphan").await, 1);
        let checks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE id IN (SELECT run_id FROM check_consumer WHERE origin='integration' AND task_id='orphan')")
            .fetch_one(world.db.pool()).await.unwrap();
        assert_eq!(
            checks, 1,
            "the quarantined import cannot skip the head check"
        );
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    }
}

/// Importer row P5 without an open hook: a rebase is stopped in the Task's
/// checkout and nothing will finish it. The head does not park on it: the
/// owner's rebase continues it by the interrupted-rebase rule, and the
/// conflict it stopped on is handed to the Worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stopped_rebase_found_by_the_head_is_finished_and_its_conflict_handed_off() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    std::fs::write(world.repo().join("t.txt"), "target\n").unwrap();
    git::commit_all(&world.repo(), "target changes the same file")
        .await
        .unwrap();
    assert!(
        git_out(&world.tree("t"), &["rebase", "main"])
            .await
            .is_none(),
        "the rebase stops on the conflict"
    );
    assert!(git::detect_rebase_in_progress(&world.tree("t"))
        .await
        .unwrap());
    let tip = world.main_tip().await;
    let production = Production::new(&world);
    let running = production.run();
    world
        .until(
            "t",
            "the stopped rebase is finished by the head",
            || async {
                !matches!(
                    world.attempt(&attempt.id).await.state,
                    S::Queued | S::Validating | S::Rebasing
                )
            },
        )
        .await;
    let after = world.attempt(&attempt.id).await;
    assert_eq!(after.state, S::Ejected, "{}", world.dump("t").await);
    assert!(
        !git::detect_rebase_in_progress(&world.tree("t"))
            .await
            .unwrap(),
        "no rebase is left stopped in the checkout"
    );
    world
        .until("t", "the conflict goes to the Worker", || async {
            world.task("t").await.status != "merging"
                || world.task("t").await.blocked_json.is_some()
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.main_tip().await, tip, "nothing landed");
    assert_ne!(
        world.task("t").await.status,
        "done",
        "a rebase stopped on the target tip is not a landed merge"
    );
}

/// Importer row P10 at runtime: a merge that was deferred under a Project
/// pause (the legacy `paused_integration` marker). The marker is cleared,
/// the Task waits typed while the Project is paused, and it merges when the
/// Project is resumed, with nothing but the worker's sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_imported_paused_integration_merges_when_the_project_is_resumed() {
    let mut world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    crate::deferred_dispatch::defer_integration_for_pause(&world.db, &world.task("t").await)
        .await
        .unwrap();
    assert!(crate::deferred_dispatch::paused_integration(&world.task("t").await).is_some());
    sqlx::query("UPDATE project SET paused_at='2026-10-10T00:00:00Z' WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let (_, priority) = world.imported("t").await;
    assert_eq!(priority, 10);
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("t", "the Task waits under the Project pause", || async {
            world
                .db
                .current_integration_attempt("t")
                .await
                .unwrap()
                .is_some_and(|attempt| {
                    attempt.state == S::Parked && gate_code(&attempt) == "project_paused"
                })
        })
        .await;
    assert!(
        crate::deferred_dispatch::paused_integration(&world.task("t").await).is_none(),
        "the legacy marker is cleared"
    );
    assert!(!world.on_main("t.txt").await);
    sqlx::query("UPDATE project SET paused_at=NULL WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world
        .until("t", "merged after the resume", || async {
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.merges("t").await, 1);
}

/// Disk pressure at admission. The Task's worktree is gone and the disk is
/// under its floor: making it again is refused. That is a wait, typed on the
/// Task as its disk capacity wait: the Task is not blocked, not failed and
/// not sent to repair, no attempt exists and nothing is charged. When the
/// disk recovers the same sweep admits the Task and it merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn admission_under_the_disk_floor_waits_for_disk_and_merges_when_it_recovers() {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    world.touch("t").await;
    std::fs::remove_dir_all(world.tree("t")).unwrap();
    let free = Arc::new(std::sync::atomic::AtomicU64::new(10));
    let reading = Arc::clone(&free);
    world.db.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(move || {
            Some(api_types::MachineDiskFacts {
                free_bytes: reading.load(std::sync::atomic::Ordering::SeqCst),
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: db::now_rfc3339(),
                gc_state: Some("unclaimed".to_owned()),
                compiler_cache_bytes: None,
            })
        }),
    );
    let admitted = world
        .service
        .reconcile_integration_admission(&world.task("t").await)
        .await
        .unwrap();
    assert!(!admitted, "nothing is admitted under the disk floor");
    let waiting = world.task("t").await;
    assert_eq!(waiting.status, "merging");
    assert!(
        waiting.blocked_json.is_none() && waiting.failed_json.is_none(),
        "a wait, not a block: {:?}",
        waiting.blocked_json
    );
    assert!(
        crate::deferred_dispatch::dispatch_disposition_for_test(&waiting).is_some_and(|wait| {
            wait.capability == "machine_capacity"
                && wait.capacity_scope.as_deref() == Some(api_types::CAPACITY_SCOPE_DISK)
        }),
        "the Task states its disk wait: {:?}",
        waiting.metadata_json
    );
    assert!(world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .is_none());
    assert!(!world.tree("t").exists(), "no worktree under the floor");

    free.store(900, std::sync::atomic::Ordering::SeqCst);
    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until("t", "the Task merges once the disk recovered", || async {
            dispatcher.reconcile_all().await.unwrap();
            let _ = world
                .service
                .reconcile_integration_admission(&world.task("t").await)
                .await;
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.merges("t").await, 1);
}

/// The server's login shell gives no environment when a Task is the head:
/// its check cannot be identified. On an unchanged target that is the
/// `settle` step's reuse, on a rebased commit the `request_check` step. The
/// step asks a few times on its own, then answers "no verdict" with the
/// cause. The attempt parks naming `check environment unavailable` (never
/// `task_step_failed`, which names the step), the Task states it, no check,
/// consumer or Review is created, and once the environment answers the
/// owner's Retry merges the Task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_head_without_an_environment_answer_parks_naming_the_cause_and_retry_merges() {
    for moved in [false, true] {
        let world = World::new().await;
        sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
            .execute(world.db.pool())
            .await
            .unwrap();
        let a = world.reconciled("env", "env.txt").await;
        review_ci(&world, "env", &["true"]).await;
        if moved {
            world.move_target("outside").await;
        }
        crate::check_runner::policy::silence_login_shell_for_test(&world.db, true)
            .await
            .unwrap();
        let reviews = world
            .scalar::<i64>("SELECT COUNT(*) FROM review WHERE task_id=?", "env")
            .await;
        // No counted retry: the first park is the owner's.
        let production = Production::with_ports(
            &world,
            production_ports_config(
                &world,
                crate::integration_worker::IntegrationWorkerConfig {
                    poll: Duration::from_millis(20),
                    sweep_interval: Duration::from_millis(100),
                    conflict_backoff: Duration::from_millis(5),
                    infra_retry: Vec::new(),
                    ..Default::default()
                },
            ),
        );
        let running = production.run();
        world
            .until("env", "the head parks for its environment", || async {
                world.attempt(&a.id).await.state == S::Parked
                    && integration(&world.task("env").await)["kind"] == "deferred"
            })
            .await;
        let parked = world.attempt(&a.id).await;
        let message = parked.failure_message.clone().unwrap_or_default();
        assert!(
            message.contains(super::super::CHECK_ENVIRONMENT_UNAVAILABLE)
                && !message.contains("task_step_failed"),
            "moved={moved}: {message}"
        );
        let stated = integration(&world.task("env").await);
        assert!(
            stated["message"]
                .as_str()
                .unwrap()
                .contains(super::super::CHECK_ENVIRONMENT_UNAVAILABLE),
            "moved={moved}: {stated}"
        );
        let failed_steps = world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM task_step WHERE task_id=? AND status='failed'",
                "env",
            )
            .await;
        assert_eq!(failed_steps, 0, "moved={moved}: no step failed for it");
        let checks: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM check_run)+(SELECT COUNT(*) FROM check_consumer)",
        )
        .fetch_one(world.db.pool())
        .await
        .unwrap();
        assert_eq!(checks, 0, "moved={moved}: nothing was asked");
        assert_eq!(
            world
                .scalar::<i64>("SELECT COUNT(*) FROM review WHERE task_id=?", "env")
                .await,
            reviews,
            "moved={moved}: no Review recorded"
        );
        assert_eq!(world.merges("env").await, 0);
        assert_eq!(world.task("env").await.status, "merging");
        let offers = world.offers("env").await;
        assert!(
            offers.iter().any(|(verb, _)| verb == "retry"),
            "moved={moved}: the Task offers Retry: {offers:?}"
        );
        // The environment answers; the owner's Retry puts the Task back in
        // line and it merges.
        crate::check_runner::policy::silence_login_shell_for_test(&world.db, false)
            .await
            .unwrap();
        world
            .act(
                "env",
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
            )
            .await;
        world
            .until("env", "the retried merge lands", || async {
                world.task("env").await.status == "done"
            })
            .await;
        assert_eq!(world.merges("env").await, 1, "moved={moved}");
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    }
}

/// A merge the importer took over as landed, whose commit the target's owner
/// says the target does not contain. The continuation asks a bounded number
/// of times, then parks the attempt for its owner: typed
/// (`deferred/owner_required`), with an Attention item, and with a Retry
/// that puts the Task back in line. It does not ask for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_imported_landed_row_whose_proof_fails_parks_for_its_owner_after_a_bounded_number_of_asks(
) {
    let world = World::new().await;
    world.legacy_task("gone", "gone.txt", "merging").await;
    world.seed_execution("gone", "executor", None).await;
    // Startup classifies every row before any continuation may run.
    world.db.import_integration_pass(100).await.unwrap();
    let a = world
        .db
        .current_integration_attempt("gone")
        .await
        .unwrap()
        .unwrap();
    assert!(a.import_source_json.is_some());
    // As the importer leaves a merge the old path recorded as landed, here
    // of a commit the target never got.
    let never_merged = "d".repeat(40);
    world
        .set(
            &a.id,
            &format!("state='applied',integrated_sha='{never_merged}',failure_kind=NULL,failure_message=NULL,available_at=NULL"),
        )
        .await;
    let report = import::import_at_start(&world.db).await.unwrap();
    assert_eq!(report.continuations, 1, "{report:?}");
    let key = format!("integration-import:{}", a.id);
    let continuation = || async {
        world
            .db
            .task_steps("gone")
            .await
            .unwrap()
            .into_iter()
            .find(|step| step.causation_key == key)
            .expect("the continuation step")
    };
    let interruptions = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id='gone'",
        )
        .fetch_one(world.db.pool())
        .await
        .unwrap()
    };
    let mut asks = 0;
    let mut told_while_asking = 0;
    loop {
        eventually("the continuation asked or ended", || async {
            let step = continuation().await;
            step.status == "done"
                || (step.status == "pending"
                    && step.attempts > asks
                    && step.available_at > db::now_rfc3339())
        })
        .await;
        let step = continuation().await;
        if step.status == "done" {
            break;
        }
        asks = step.attempts;
        assert!(
            asks < IMPORT_ABSENT_ASKS,
            "the owner answered: asked {asks} times and still asks"
        );
        // While it asks, the Task says the result is unresolved.
        let task = world.task("gone").await;
        assert_eq!(task.status, "merging");
        assert_eq!(integration(&task)["cause"], "unresolved_result");
        told_while_asking = interruptions().await;
        sqlx::query("UPDATE task_step SET available_at=? WHERE id=?")
            .bind(db::now_rfc3339())
            .bind(&step.id)
            .execute(world.db.pool())
            .await
            .unwrap();
        world.db.domain_event_notify().notify_waiters();
    }
    assert_eq!(continuation().await.attempts, IMPORT_ABSENT_ASKS);
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(db::IntegrationFailureKind::OwnerRequired)
    );
    assert_eq!(parked.available_at, None, "no deadline: the owner decides");
    assert_eq!(parked.integrated_sha, None, "the unproven claim is dropped");
    let task = world.task("gone").await;
    assert_eq!(task.status, "merging", "never done without the proof");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "owner_required");
    assert!(
        integration(&task)["message"]
            .as_str()
            .unwrap()
            .contains(&never_merged),
        "{}",
        integration(&task)
    );
    assert!(db::material_blocker(&task.condition).requires_intervention);
    // The park is a new interruption of the Task (what Attention projects),
    // raised once.
    assert_eq!(interruptions().await, told_while_asking + 1);
    // The owner's Retry is offered, and it puts the Task back in line: the
    // queue reads the target as it is and merges the Task's real commit.
    assert!(world
        .offers("gone")
        .await
        .contains(&("retry".into(), "integration_retry".into())));
    let production = Production::new(&world);
    let running = production.run();
    world
        .act(
            "gone",
            TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
        )
        .await;
    world
        .until(
            "gone",
            "the Task merges after the owner's Retry",
            || async { world.task("gone").await.status == "done" },
        )
        .await;
    assert_eq!(world.merges("gone").await, 1);
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// The same landed row, when the machine that holds the target's default
/// checkout is merely not connected (a daemon that is offline at start):
/// nobody answered, so nothing is known about the commit and there is no
/// decision for the Task's owner to take. The continuation keeps asking,
/// however many times it has run, and the attempt stays `applied`. Once the
/// machine is removed nobody will ever answer: then the asks are counted and
/// the attempt parks for its owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_imported_landed_row_waits_for_an_offline_target_owner_and_parks_only_when_it_is_removed(
) {
    let world = World::new().await;
    let world = &world;
    world.legacy_task("away", "away.txt", "merging").await;
    world.seed_execution("away", "executor", None).await;
    world.db.import_integration_pass(100).await.unwrap();
    let a = world
        .db
        .current_integration_attempt("away")
        .await
        .unwrap()
        .unwrap();
    let landed = "e".repeat(40);
    world
        .set(
            &a.id,
            &format!("state='applied',integrated_sha='{landed}',failure_kind=NULL,failure_message=NULL,available_at=NULL"),
        )
        .await;
    // The target's default checkout lives on a daemon that is not connected.
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES('d-away','m-away','away-host','linux','aarch64','offline',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(world.db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES('rt-away','d-away','codex','root','offline','{}',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(world.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repo_location SET owner_kind='daemon',daemon_id='d-away',runtime_id='rt-away' WHERE id=(SELECT target_location_id FROM integration_queue WHERE id=?)")
        .bind(a.queue_id.as_deref().unwrap())
        .execute(world.db.pool())
        .await
        .unwrap();
    import::import_at_start(&world.db).await.unwrap();
    let key = format!("integration-import:{}", a.id);
    let key = &key;
    let continuation = || async move {
        world
            .db
            .task_steps("away")
            .await
            .unwrap()
            .into_iter()
            .find(|step| &step.causation_key == key)
            .expect("the continuation step")
    };
    let ask_again = |asks: i64| async move {
        eventually("the continuation asked or ended", || async move {
            let step = continuation().await;
            step.status == "done"
                || (step.status == "pending"
                    && step.attempts > asks
                    && step.available_at > db::now_rfc3339())
        })
        .await;
        continuation().await
    };
    let ready = |id: String| async move {
        sqlx::query("UPDATE task_step SET available_at=? WHERE id=?")
            .bind(db::now_rfc3339())
            .bind(&id)
            .execute(world.db.pool())
            .await
            .unwrap();
        world.db.domain_event_notify().notify_waiters();
    };
    // Well past every bound: it still asks, and the row is still `applied`.
    let mut asks = 0;
    let mut waiting = String::new();
    for round in 0..IMPORT_PROOF_ASKS + 3 {
        if round > 0 {
            ready(waiting.clone()).await;
        }
        let step = ask_again(asks).await;
        assert_eq!(step.status, "pending", "{}", world.dump("away").await);
        asks = step.attempts;
        let task = world.task("away").await;
        assert_eq!(integration(&task)["cause"], "unresolved_result");
        assert_eq!(world.attempt(&a.id).await.state, S::Applied);
        waiting = step.id;
    }
    assert_eq!(
        world.attempt(&a.id).await.integrated_sha.as_deref(),
        Some(landed.as_str()),
        "the claim is kept while nobody answered"
    );
    // The machine is removed: nobody will answer. Counted from here.
    sqlx::query("UPDATE daemon SET removed_at=? WHERE id='d-away'")
        .bind(db::now_rfc3339())
        .execute(world.db.pool())
        .await
        .unwrap();
    ready(waiting).await;
    let mut counted = 0;
    loop {
        let step = ask_again(asks).await;
        if step.status == "done" {
            break;
        }
        asks = step.attempts;
        counted += 1;
        assert!(
            counted < IMPORT_PROOF_ASKS,
            "asked {counted} times and still asks"
        );
        ready(step.id).await;
    }
    assert_eq!(counted, IMPORT_PROOF_ASKS - 1);
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(db::IntegrationFailureKind::OwnerRequired)
    );
    assert_eq!(
        integration(&world.task("away").await)["cause"],
        "owner_required"
    );
}

/// The row names a commit the target does not contain, but the Task's own
/// commit did land (the old path merged it and recorded another id). The
/// owner answered, so the bound is the short one. After the park the
/// owner's Retry puts the Task back in line: the queue reads the target as
/// it is, finds the Task's commit on it, and reports the Task merged once,
/// without a second merge of a landed commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unproven_import_whose_commit_did_land_is_reported_once_after_retry_and_not_merged_again(
) {
    let world = World::new().await;
    let candidate = world.legacy_task("landed", "landed.txt", "merging").await;
    world.seed_execution("landed", "executor", None).await;
    // The old path's merge: the target contains the Task's commit.
    git_out(&world.repo(), &["merge", "--ff-only", "task-landed"])
        .await
        .expect("the Task's commit lands on the target");
    let target = git::get_current_sha(&world.repo()).await.unwrap();
    assert_eq!(target, candidate);
    world.db.import_integration_pass(100).await.unwrap();
    let a = world
        .db
        .current_integration_attempt("landed")
        .await
        .unwrap()
        .unwrap();
    let wrong = "c".repeat(40);
    world
        .set(
            &a.id,
            &format!("state='applied',integrated_sha='{wrong}',failure_kind=NULL,failure_message=NULL,available_at=NULL"),
        )
        .await;
    import::import_at_start(&world.db).await.unwrap();
    let key = format!("integration-import:{}", a.id);
    let continuation = || async {
        world
            .db
            .task_steps("landed")
            .await
            .unwrap()
            .into_iter()
            .find(|step| step.causation_key == key)
            .expect("the continuation step")
    };
    let mut asks = 0;
    loop {
        eventually("the continuation asked or ended", || async {
            let step = continuation().await;
            step.status == "done"
                || (step.status == "pending"
                    && step.attempts > asks
                    && step.available_at > db::now_rfc3339())
        })
        .await;
        let step = continuation().await;
        if step.status == "done" {
            break;
        }
        asks = step.attempts;
        assert!(
            asks < IMPORT_ABSENT_ASKS,
            "the owner answered: asked {asks} times and still asks"
        );
        sqlx::query("UPDATE task_step SET available_at=? WHERE id=?")
            .bind(db::now_rfc3339())
            .bind(&step.id)
            .execute(world.db.pool())
            .await
            .unwrap();
        world.db.domain_event_notify().notify_waiters();
    }
    assert_eq!(continuation().await.attempts, IMPORT_ABSENT_ASKS);
    assert_eq!(world.attempt(&a.id).await.state, S::Parked);
    assert_eq!(world.task("landed").await.status, "merging");
    let production = Production::new(&world);
    let running = production.run();
    world
        .act(
            "landed",
            TaskAction::Retry {
                reason: None,
                fresh_session: None,
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
        )
        .await;
    world
        .until(
            "landed",
            "the landed Task is reported after Retry",
            || async { world.task("landed").await.status == "done" },
        )
        .await;
    assert_eq!(world.merges("landed").await, 1);
    // Nothing was merged a second time: the target is where the old merge
    // left it.
    assert_eq!(git::get_current_sha(&world.repo()).await.unwrap(), target);
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
