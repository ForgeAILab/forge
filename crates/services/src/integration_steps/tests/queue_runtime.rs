//! Queue admission, production ports and concurrent runtime recovery.
use super::*;

/// The real `IntegrationQueueWorker`, the real server owner and real Git over
/// this consumer and the real step worker. Two Tasks enter the queue: the
/// first merges the reviewed commit as it is; the second lost the race, is
/// rebased by the queue, settled, fast-forwarded, and both end `done`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_tasks_enter_the_queue_and_merge_in_order_through_the_real_worker() {
    use crate::integration_worker::{IntegrationWorkerConfig, SystemClock};
    let world = World::new().await;
    let first = world.add_task("one", "one.txt", "one\n").await;
    let second = world.add_task("two", "two.txt", "two\n").await;
    let worker = Arc::new(IntegrationQueueWorker::new(
        world.db.clone(),
        Arc::new(crate::integration_owner::ServerIntegrationOwner::new(
            world.db.clone(),
        )),
        world.port.clone(),
        Arc::new(GitFacts {
            db: world.db.clone(),
        }),
        Arc::new(NoTransfer),
        Arc::new(SystemClock),
        IntegrationWorkerConfig {
            poll: Duration::from_millis(20),
            sweep_interval: Duration::from_millis(100),
            conflict_backoff: Duration::from_millis(5),
            ..Default::default()
        },
    ));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(worker.clone().run(stopped));
    eventually("both Tasks are done", || async {
        world.task("one").await.status == "done" && world.task("two").await.status == "done"
    })
    .await;
    eventually("both attempts completed", || async {
        world.attempt(&first.id).await.state == S::Completed
            && world.attempt(&second.id).await.state == S::Completed
    })
    .await;
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    // Git: the target holds both commits, the second on top of the first.
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    let (one, two) = (
        world.attempt(&first.id).await,
        world.attempt(&second.id).await,
    );
    assert_eq!(two.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(
        one.integrated_sha, one.original_candidate_sha,
        "the reviewed commit, as it was"
    );
    assert_ne!(
        two.integrated_sha, two.original_candidate_sha,
        "rebased by the queue"
    );
    for file in ["one.txt", "two.txt"] {
        assert!(
            git_out(&world.repo(), &["cat-file", "-e", &format!("{tip}:{file}")])
                .await
                .is_some(),
            "{file} is on main"
        );
    }
    // Tasks: exactly today's success trail, and no bounce through merge_failed.
    for (task, attempt) in [("one", &one), ("two", &two)] {
        assert_eq!(
            world.comments(task).await,
            vec![format!(
                "Changes merged to main (SHA: {})",
                attempt.integrated_sha.clone().unwrap()
            )]
        );
        let transitions = world.transitions(task).await;
        assert_eq!(transitions.len(), 1, "{transitions:?}");
        assert_eq!(
            (
                transitions[0].0.as_str(),
                transitions[0].1.as_str(),
                transitions[0].2.as_str()
            ),
            ("merging", "done", "merge succeeded")
        );
        // `done` is visible from the commit of the transition, and that
        // commit queues the cascade's step for the entry into `done` (the
        // completion passes `cascade_step_input`). A step of the Task may
        // therefore still be live at that instant. It has to settle: the
        // wait is bounded, and the rows that do not settle are printed.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
        let mut waited = false;
        loop {
            let live: Vec<String> = world
                .db
                .task_steps(task)
                .await
                .unwrap()
                .iter()
                .filter(|step| !matches!(step.status.as_str(), "done" | "superseded"))
                .map(|step| {
                    format!(
                        "#{} {} {} attempts={} lease_until={:?} last_error={:?} payload={}",
                        step.seq,
                        step.kind,
                        step.status,
                        step.attempts,
                        step.lease_until,
                        step.last_error,
                        step.payload_json
                    )
                })
                .collect();
            if live.is_empty() {
                break;
            }
            if !waited {
                println!("Task {task} is done with steps still settling: {live:#?}");
                waited = true;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "steps of Task {task} still live after it is done: {live:#?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    // The first asked for no check; the second was rebased and had none configured.
    assert!(world
        .step("one", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert_eq!(
        world
            .step("two", IntegrationStepAction::RequestCheck)
            .await
            .len(),
        1
    );
}

// ----- production ports (plan 3.2 stage D2a) ------------------------------

/// Parity with today's merge path, which merges a Task placed in another
/// clone than the default checkout (into that clone). Through the queue such
/// a Task lands in the DEFAULT checkout: the target tip is brought to the
/// Task's clone and bound to a ref, the rebase is onto that commit, the real
/// check runs on the rebased commit, and the exact commit is sent to the
/// default checkout and fast-forwarded there. Three Tasks in the clone: the
/// first needs no rebase, the second is rebased and green, the third is
/// rebased and red.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tasks_in_another_clone_merge_into_the_default_checkout_through_the_queue() {
    let world = World::new().await;
    world.add_clone().await;
    let clone_main = git_out(&world.clone_path(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    let first = world.add_task_in_clone("one", "one.txt", "one\n").await;
    let green = world.add_task_in_clone("two", "two.txt", "two\n").await;
    let red = world
        .add_task_in_clone("three", "three.txt", "three\n")
        .await;
    review_ci(&world, "one", &["test -f one.txt"]).await;
    review_ci(&world, "two", &["test -f one.txt", "test -f two.txt"]).await;
    review_ci(&world, "three", &["test -f two.txt", "exit 3"]).await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the first two merge and the third is sent back", || async {
        world.task("one").await.status == "done"
            && world.task("two").await.status == "done"
            && world.attempt(&red.id).await.state == S::Ejected
            && world.task("three").await.status != "merging"
    })
    .await;
    eventually("the merged attempts complete", || async {
        world.attempt(&first.id).await.state == S::Completed
            && world.attempt(&green.id).await.state == S::Completed
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let (one, two, three) = (
        world.attempt(&first.id).await,
        world.attempt(&green.id).await,
        world.attempt(&red.id).await,
    );
    assert!(one.failure_message.is_none(), "{:?}", one.failure_message);
    assert!(two.failure_message.is_none(), "{:?}", two.failure_message);
    // The DEFAULT checkout holds the first two, the second rebased.
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    assert_eq!(two.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(one.integrated_sha, one.original_candidate_sha);
    assert_ne!(two.integrated_sha, two.original_candidate_sha);
    for (file, present) in [("one.txt", true), ("two.txt", true), ("three.txt", false)] {
        assert_eq!(
            git_out(&world.repo(), &["cat-file", "-e", &format!("{tip}:{file}")])
                .await
                .is_some(),
            present,
            "{file}"
        );
    }
    // The exact commit the Task's clone holds is what landed.
    assert_eq!(git::get_current_sha(&world.tree("two")).await.unwrap(), tip);
    // The clone's own target branch was never written.
    assert_eq!(
        git_out(&world.clone_path(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap(),
        clone_main
    );
    // Commits moved through the owners, and each merged attempt's transfer
    // refs are gone from both checkouts.
    for attempt in [&one, &two] {
        assert!(
            attempt
                .phase_timings
                .as_ref()
                .is_some_and(|timings| timings.transfer_ms.is_some()),
            "{:?}",
            attempt.phase_timings
        );
        for repo in [world.repo(), world.clone_path()] {
            let refs = git_out(
                &repo,
                &[
                    "for-each-ref",
                    "--format=%(refname)",
                    &format!("refs/forge/integration/{}-*", attempt.id),
                ],
            )
            .await
            .unwrap_or_default();
            assert!(refs.trim().is_empty(), "{refs}");
        }
    }
    // The checks ran on the rebased commits in the clone, plus the
    // default-full rerun of the red step.
    let runs: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at",
    )
    .fetch_all(world.db.pool())
    .await
    .unwrap();
    assert_eq!(
        runs,
        vec![
            (two.candidate_sha.clone().unwrap(), "pass".to_owned()),
            (three.candidate_sha.clone().unwrap(), "fail".to_owned()),
            (three.candidate_sha.clone().unwrap(), "fail".to_owned()),
        ],
        "{runs:?}"
    );
    assert_eq!(
        three.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
}

/// `carry_for` against a real review contract. Two Tasks were reviewed and
/// passed by an agent reviewer; a third Task lands first and moves the
/// target, so the queue rebases both. The first keeps its review: the
/// rebased commit changes exactly what the reviewer saw, its check passes,
/// the carry is recorded against the contract and charged, and it merges
/// without a new review. The second loses it: its contract froze another
/// change set than the candidate has, so the rebased commit is outside what
/// was reviewed; it is sent back for review and never merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_real_review_contract_is_carried_over_the_queue_rebase_or_lost_with_its_change_set() {
    let world = World::new().await;
    let first = world.add_task("one", "one.txt", "one\n").await;
    let kept = world.add_task("two", "two.txt", "two\n").await;
    let lost = world.add_task("three", "three.txt", "three\n").await;
    for task in ["one", "two", "three"] {
        review_ci(&world, task, &["test -f base"]).await;
    }
    let kept_contract = world.seed_passed_review("two", &["two.txt"]).await;
    world.seed_passed_review("three", &["elsewhere.txt"]).await;
    let production = Production::new(&world);
    let running = production.run();
    eventually(
        "one and two merge and three is sent back for review",
        || async {
            world.task("one").await.status == "done"
                && world.task("two").await.status == "done"
                && world.attempt(&lost.id).await.state == S::NeedsReview
        },
    )
    .await;
    eventually("the merged attempts complete", || async {
        world.attempt(&first.id).await.state == S::Completed
            && world.attempt(&kept.id).await.state == S::Completed
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let (two, three) = (world.attempt(&kept.id).await, world.attempt(&lost.id).await);
    // Kept: one carry, of the rebased commit onto the tip it was rebased on,
    // against the contract the reviewer froze; the allowance was charged.
    assert_ne!(two.integrated_sha, two.original_candidate_sha);
    let carries: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT task_id,contract_execution_id,commit_sha,base_sha FROM review_authority_carry ORDER BY created_at",
    )
    .fetch_all(world.db.pool())
    .await
    .unwrap();
    assert_eq!(
        carries,
        vec![(
            "two".to_owned(),
            kept_contract,
            two.integrated_sha.clone().unwrap(),
            two.integrated_before_sha.clone().unwrap(),
        )],
        "{carries:?}"
    );
    let spent: i64 = world
        .scalar(
            "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind='review_carry'),0)",
            "two",
        )
        .await;
    assert_eq!(spent, 1);
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    assert_eq!(two.integrated_sha.as_deref(), Some(tip.as_str()));
    // Lost: no carry, the reason names the path, and nothing of it landed.
    assert!(
        three
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("outside the reviewed change set")),
        "{:?}",
        three.failure_message
    );
    assert!(git_out(
        &world.repo(),
        &["cat-file", "-e", &format!("{tip}:three.txt")]
    )
    .await
    .is_none());
    assert_ne!(world.task("three").await.status, "done");
    assert_eq!(
        world
            .step("three", IntegrationStepAction::SendBack)
            .await
            .len(),
        1
    );
}

/// The worker over its production ports writes nothing outside the queue,
/// the attempt and the Task-step queue. The Task-step worker is stopped, so
/// every write in the window is the queue worker's own: a head whose real
/// rebase conflicts, to its ejection; then a head that is rebased cleanly, to
/// its request for a check; then idle sweeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_worker_over_production_ports_writes_only_queue_attempt_and_task_step() {
    let world = World::new().await;
    // No Task step runs in this test: only the queue worker writes.
    let _ = world.stop.send(true);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let conflicted = world.add_task("conflict", "base", "theirs\n").await;
    let clean = world.add_task("clean", "clean.txt", "clean\n").await;
    // The target moves under both, on the file the first one changed.
    std::fs::write(world.repo().join("base"), "ours\n").unwrap();
    git::commit_all(&world.repo(), "outside").await.unwrap();
    let (worker, _daemons, _config) = production_ports(&world);
    let before = foreign_digest(&world.db).await;
    let steps_before: i64 = world
        .scalar("SELECT COUNT(*) FROM task_step WHERE task_id!=?", "")
        .await;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(worker.clone().run(stopped));
    eventually(
        "the conflict is ejected and the clean head asks for its check",
        || async {
            world.attempt(&conflicted.id).await.state == S::Ejected
                && world.attempt(&clean.id).await.state == S::Checking
        },
    )
    .await;
    // A few sweeps and polls on top.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let after = foreign_digest(&world.db).await;
    for (table, digest) in &before {
        assert_eq!(after.get(table), Some(digest), "table `{table}` changed");
    }
    assert_eq!(after.len(), before.len());
    assert!(before.len() > 50, "the digest covers the schema");
    let steps_after: i64 = world
        .scalar("SELECT COUNT(*) FROM task_step WHERE task_id!=?", "")
        .await;
    assert!(
        steps_after > steps_before,
        "the worker did ask for Task steps"
    );
    let ejected = world.attempt(&conflicted.id).await;
    assert_eq!(ejected.conflict_paths_json, Some(json!(["base"])));
    let rebased = world.attempt(&clean.id).await;
    assert_ne!(rebased.candidate_sha, rebased.original_candidate_sha);
    assert!(
        git_out(
            &world.repo(),
            &["cat-file", "-e", "refs/heads/main:clean.txt"]
        )
        .await
        .is_none(),
        "nothing merged without a Task step"
    );
}

/// A Task re-enters `merging` while the attempt of its previous entry is the
/// head of the queue, in the middle of its check. The admission flags that
/// attempt, the worker releases it at once, and the new entry is admitted
/// without failing or retrying the caller's step. The commit the old attempt
/// rebased was never checked: the new attempt keeps the reviewed commit, so
/// the rebased commit is checked before it may merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_reentering_merging_takes_over_from_its_old_head_and_the_rebased_commit_is_still_checked(
) {
    let world = World::new().await;
    let old = world.add_task("again", "again.txt", "again\n").await;
    world.seed_execution("again", "executor", None).await;
    // The target moved, so the head rebases and must check.
    std::fs::write(world.repo().join("moved"), "moved\n").unwrap();
    git::commit_all(&world.repo(), "target moved")
        .await
        .unwrap();
    let gate = world.temp.path().join("go");
    review_ci(
        &world,
        "again",
        &[&format!(
            "while [ ! -f {} ]; do sleep 0.1; done",
            gate.display()
        )],
    )
    .await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the head waits for its check", || async {
        world.attempt(&old.id).await.state == S::Checking
    })
    .await;
    let reviewed = world.attempt(&old.id).await.original_candidate_sha;
    let rebased = git::get_current_sha(&world.tree("again")).await.unwrap();
    assert_ne!(reviewed.as_deref(), Some(rebased.as_str()));
    // The Task leaves and re-enters `merging`: a new status entry.
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='again'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let releases_before = production.worker.releases();
    let started = std::time::Instant::now();
    let admitted = world
        .service
        .admit_to_integration_queue("again")
        .await
        .expect("admitted without a conflict for the caller");
    let elapsed = started.elapsed();
    assert_ne!(admitted.id, old.id);
    assert_eq!(admitted.expected_epoch, 1);
    let released = world.attempt(&old.id).await;
    // The order of events, whatever the load: the old head's driver
    // released it (the signal the admission waits on) before the new entry
    // was admitted, and the admission did not run out its wait.
    assert_eq!(
        production.worker.releases(),
        releases_before + 1,
        "the old head was released by its driver, once"
    );
    assert!(
        released.updated_at <= admitted.enqueued_at,
        "released {} before admitted {}",
        released.updated_at,
        admitted.enqueued_at
    );
    // The takeover is one driver pass after the flag. The bound is half of
    // the admission's own wait (`RELEASE_WAIT`), so it tells a takeover on
    // the signal from an admission that waited its wait out.
    assert!(
        elapsed < Duration::from_secs(10),
        "re-admission finishes within 10 s: {elapsed:?}"
    );
    // Released by the admission's cancel flag, or by the head's own gate
    // read finding the entry gone, whichever came first.
    assert_eq!(released.state, S::Cancelled);
    assert_eq!(
        admitted.original_candidate_sha, reviewed,
        "the reviewed commit, not the unchecked rebase"
    );
    assert_eq!(admitted.candidate_sha.as_deref(), Some(rebased.as_str()));
    // Nothing merges before the check of the rebased commit has passed.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(world.task("again").await.status, "merging");
    assert_ne!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap(),
        rebased
    );
    std::fs::write(&gate, "go").unwrap();
    eventually("the re-admitted Task merges", || async {
        world.task("again").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let merged = world.attempt(&admitted.id).await;
    assert_eq!(merged.integrated_sha.as_deref(), Some(rebased.as_str()));
    assert_eq!(merged.checks_commit_sha.as_deref(), Some(rebased.as_str()));
    let steps = world.db.task_steps("again").await.unwrap();
    assert!(
        steps.iter().all(|step| step.status != "failed"),
        "{steps:?}"
    );
    assert!(
        steps.iter().all(|step| step.attempts <= 2),
        "no step needs more than two attempts: {steps:?}; old {:?}; new {:?}",
        world.attempt(&old.id).await.phase_timings,
        merged.phase_timings
    );
}

/// The order of events of a re-entry, with the admission's poll taken away:
/// the admission flags the old head and wakes the worker, the head's driver
/// releases it and signals the release, and that signal (nothing else) lets
/// the admission look again. Without the signal the admission would return
/// the conflict after `RELEASE_WAIT` and its step would spend an attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_reentry_admission_is_woken_by_the_release_of_its_old_head_not_by_its_poll() {
    let world = World::new().await;
    let old = world.add_task("signal", "signal.txt", "signal\n").await;
    world.seed_execution("signal", "executor", None).await;
    std::fs::write(world.repo().join("moved"), "moved\n").unwrap();
    git::commit_all(&world.repo(), "target moved")
        .await
        .unwrap();
    let gate = world.temp.path().join("go");
    review_ci(
        &world,
        "signal",
        &[&format!(
            "while [ ! -f {} ]; do sleep 0.1; done",
            gate.display()
        )],
    )
    .await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the head waits for its check", || async {
        world.attempt(&old.id).await.state == S::Checking
    })
    .await;
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='signal'")
        .execute(world.db.pool())
        .await
        .unwrap();
    super::admission::test_poll::disable("signal");
    let releases_before = production.worker.releases();
    let task = world.task("signal").await;
    let admitted = super::admission::admit_released(
        &world.db,
        world.service.merge_service.as_ref().unwrap(),
        &task,
        Some(&production.worker),
    )
    .await
    .expect("the release of the old head woke the admission");
    assert_ne!(admitted.id, old.id);
    assert_eq!(production.worker.releases(), releases_before + 1);
    let released = world.attempt(&old.id).await;
    // By the admission's cancel flag or by the head's own gate read finding
    // the entry gone, whichever came first: either way its driver's release.
    assert_eq!(released.state, S::Cancelled);
    assert!(released.updated_at <= admitted.enqueued_at);
    std::fs::write(&gate, "go").unwrap();
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// The same, through the composition the server runtime uses: the worker
/// `TaskService::compose_integration` registers on the service (the
/// runtime's call, `runtime/mod.rs`) is the worker every admission is handed
/// (`TaskService::integration_worker`), and it is that worker's drivers that
/// signal a release. With the admission's poll taken away,
/// `admit_to_integration_queue` returns only on that signal: a service whose
/// admissions were handed no worker, or another worker than the one that
/// runs, would wait `RELEASE_WAIT` out and return the conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_composed_service_hands_its_admissions_the_worker_whose_release_wakes_them() {
    let world = World::new().await;
    assert!(
        world.service.integration_worker().is_none(),
        "nothing is registered before the composition"
    );
    let worker = world.service.integration_worker_or_embedded();
    assert!(Arc::ptr_eq(
        &worker,
        &world.service.integration_worker().unwrap()
    ));
    // Every clone of the service (the one hooks and steps hold) sees it.
    assert!(Arc::ptr_eq(
        &worker,
        &world.service.clone().integration_worker().unwrap()
    ));
    let old = world.add_task("composed", "composed.txt", "x\n").await;
    world.seed_execution("composed", "executor", None).await;
    std::fs::write(world.repo().join("moved"), "moved\n").unwrap();
    git::commit_all(&world.repo(), "target moved")
        .await
        .unwrap();
    let gate = world.temp.path().join("go-composed");
    review_ci(
        &world,
        "composed",
        &[&format!(
            "while [ ! -f {} ]; do sleep 0.1; done",
            gate.display()
        )],
    )
    .await;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let running = tokio::spawn(worker.clone().run(stopped));
    eventually("the head holds the slot", || async {
        matches!(
            world.attempt(&old.id).await.state,
            S::Checking | S::Rebasing | S::Validating
        ) && world
            .db
            .integration_queue(old.queue_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap()
            .head_attempt_id
            .as_deref()
            == Some(old.id.as_str())
    })
    .await;
    eventually("the head waits for its check", || async {
        world.attempt(&old.id).await.state == S::Checking
    })
    .await;
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='composed'")
        .execute(world.db.pool())
        .await
        .unwrap();
    super::admission::test_poll::disable("composed");
    let releases_before = worker.releases();
    let started = std::time::Instant::now();
    let admitted = world
        .service
        .admit_to_integration_queue("composed")
        .await
        .expect("the release of the old head woke the admission");
    assert_ne!(admitted.id, old.id);
    assert!(worker.releases() > releases_before);
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "woken by the release, not by the end of the wait: {:?}",
        started.elapsed()
    );
    assert_eq!(world.attempt(&old.id).await.state, S::Cancelled);
    std::fs::write(&gate, "go").unwrap();
    let _ = stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// The Task's target branch changed between two entries of `merging`: the
/// attempt in the old target's queue is flagged and cancelled, and the Task
/// is admitted to the new target's queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_target_cancels_the_old_queue_membership_and_admits_to_the_new_queue() {
    let world = World::new().await;
    let old = world.add_task("moved", "moved.txt", "moved\n").await;
    world.seed_execution("moved", "executor", None).await;
    git_out(&world.repo(), &["branch", "release"])
        .await
        .unwrap();
    sqlx::query("UPDATE task SET merge_config=?,status_epoch=status_epoch+1 WHERE id='moved'")
        .bind(json!({"target_branch":"release"}).to_string())
        .execute(world.db.pool())
        .await
        .unwrap();
    let admitted = world
        .service
        .admit_to_integration_queue("moved")
        .await
        .unwrap();
    let before = world.attempt(&old.id).await;
    assert_eq!(before.state, S::Cancelled);
    assert!(before.cancel_requested_at.is_some());
    assert!(!before.current);
    assert_ne!(admitted.queue_id, old.queue_id);
    let queue = world
        .db
        .integration_queue(admitted.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.target_branch, "release");
    assert_eq!(admitted.state, S::Queued);
    assert!(admitted.current);
    assert!(!super::admission::unbound(&admitted));
    // The same entry again is the same row.
    let again = world
        .service
        .admit_to_integration_queue("moved")
        .await
        .unwrap();
    assert_eq!(again.id, admitted.id);
    // And the queue of the new target merges it.
    let production = Production::new(&world);
    let running = production.run();
    eventually("merged into the new target", || async {
        world.task("moved").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/release"]).await,
        admitted.original_candidate_sha
    );
}

/// A Task admitted while its repository has no default checkout: the queue
/// is created suspended and the Task is told why. When the repository gets
/// its default checkout the queue reopens and the Task merges on the
/// worker's sweep alone: nothing notifies the worker and no Task event is
/// needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_queue_suspended_for_a_missing_default_checkout_reopens_on_the_sweep_alone() {
    let world = World::new().await;
    sqlx::query("UPDATE repo_location SET is_default=0 WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let waiting = world.add_task("late", "late.txt", "late\n").await;
    let queue_id = waiting.queue_id.clone().unwrap();
    let state = |world: &World| {
        let (db, id) = (world.db.clone(), queue_id.clone());
        async move { db.integration_queue(&id).await.unwrap().unwrap().state }
    };
    assert_eq!(state(&world).await, db::IntegrationQueueState::Suspended);
    let production = Production::new(&world);
    let running = production.run();
    eventually("the Task is told its queue is suspended", || async {
        !world
            .step("late", IntegrationStepAction::Park)
            .await
            .is_empty()
    })
    .await;
    assert_eq!(world.attempt(&waiting.id).await.state, S::Queued);
    assert_eq!(world.task("late").await.status, "merging");
    // The default checkout appears. A plain row write: no notify, no event.
    sqlx::query("UPDATE repo_location SET is_default=1 WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    eventually("the queue reopens and the Task merges", || async {
        world.task("late").await.status == "done"
            && world.attempt(&waiting.id).await.state == S::Completed
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(state(&world).await, db::IntegrationQueueState::Open);
    assert_eq!(world.attempt(&waiting.id).await.state, S::Completed);
}

/// A Task its owner blocked while it waits in the queue is not merged: it
/// keeps its `merging` entry, so only the `settle` step can refuse. No
/// permit is written, nothing lands, and the head leaves the slot parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_blocked_while_it_is_queued_gets_no_permit_and_does_not_merge() {
    let world = World::new().await;
    let held = world.add_task("held", "held.txt", "held\n").await;
    sqlx::query(
        "UPDATE task SET blocked_json='{\"reason\":\"stopped by the owner\"}' WHERE id='held'",
    )
    .execute(world.db.pool())
    .await
    .unwrap();
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    let production = Production::new(&world);
    let running = production.run();
    eventually("the head leaves the slot", || async {
        world.attempt(&held.id).await.state == S::Parked
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let parked = world.attempt(&held.id).await;
    assert!(parked.permit_json.is_none());
    assert!(
        parked
            .failure_message
            .as_deref()
            .is_some_and(|message| message.contains("blocked")),
        "{:?}",
        parked.failure_message
    );
    assert_eq!(world.task("held").await.status, "merging");
    assert_eq!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap(),
        tip
    );
    assert!(world
        .step("held", IntegrationStepAction::Result)
        .await
        .is_empty());
}

/// Admission remains pinned to the implementation execution's repository,
/// even after the Project selects another primary repository.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_admission_uses_the_execution_repository_after_project_reselection() {
    let world = World::new().await;
    let sha = world.legacy_task("pinned", "pinned.txt", "merging").await;
    world.seed_execution("pinned", "executor", None).await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('other','p','other','release',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    sqlx::query("UPDATE project SET primary_repo_id='other',version=version+1 WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let admitted = world
        .service
        .admit_to_integration_queue("pinned")
        .await
        .unwrap();
    let queue = world
        .db
        .integration_queue(admitted.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.repo_id, "r");
    assert_eq!(queue.target_branch, "main");
    let production = Production::new(&world);
    let running = production.run();
    eventually("the pinned repository receives the candidate", || async {
        world.task("pinned").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/main"]).await,
        Some(sha)
    );
}

/// File-backed DB, real owner/facts/step/check ports: parallel sweep callers
/// acquire one head, and all siblings finish without external target moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn runtime_parallel_claimers_hold_one_head_and_queue_writers_never_move_its_target() {
    let world = World::new().await;
    let mut attempts = Vec::new();
    for i in 0..6 {
        attempts.push(
            world
                .add_task(
                    &format!("parallel-{i}"),
                    &format!("parallel-{i}.txt"),
                    "x\n",
                )
                .await,
        );
    }
    let production = Production::new(&world);
    let mut claims = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let worker = production.worker.clone();
        claims.spawn(async move { worker.sweep_once().await.unwrap() });
    }
    let mut drivers = Vec::new();
    while let Some(claim) = claims.join_next().await {
        drivers.extend(claim.unwrap());
    }
    assert_eq!(drivers.len(), 1, "one reserved head across parallel claims");
    drivers
        .pop()
        .unwrap()
        .run(tokio_util::sync::CancellationToken::new())
        .await;
    let running = production.run();
    // The result step reports the Task `done`; the worker then closes the
    // attempt on its next pass. Wait for both before the worker is stopped.
    eventually("all parallel siblings merge", || async {
        let mut done = true;
        for i in 0..6 {
            done &= world.task(&format!("parallel-{i}")).await.status == "done";
        }
        for attempt in &attempts {
            done &= world.attempt(&attempt.id).await.state == S::Completed;
        }
        done
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    for attempt in attempts {
        let after = world.attempt(&attempt.id).await;
        assert_eq!(after.state, S::Completed);
        assert_eq!(after.phase_timings.unwrap().external_target_moves(), 0);
        let sha = after.integrated_sha.unwrap();
        assert!(git::command_output(
            &world.repo(),
            &["merge-base", "--is-ancestor", &sha, "main"]
        )
        .await
        .unwrap()
        .status
        .success());
    }
}

/// A head's slow real CI does not occupy another target's slot. Both targets
/// use the same physical default checkout, with independent queue authority.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn runtime_two_targets_make_progress_while_one_head_waits_for_real_ci() {
    let world = World::new().await;
    git_out(&world.repo(), &["branch", "release"])
        .await
        .unwrap();
    let slow = world.add_task("slow-target", "slow.txt", "slow\n").await;
    let fast_sha = world
        .legacy_task("fast-target", "fast.txt", "merging")
        .await;
    world.seed_execution("fast-target", "executor", None).await;
    sqlx::query(
        "UPDATE task SET merge_config='{\"target_branch\":\"release\"}' WHERE id='fast-target'",
    )
    .execute(world.db.pool())
    .await
    .unwrap();
    let fast = world
        .service
        .admit_to_integration_queue("fast-target")
        .await
        .unwrap();
    let gate = world.temp.path().join("release-slow-check");
    review_ci(
        &world,
        "slow-target",
        &[&format!(
            "while [ ! -f '{}' ]; do sleep 0.1; done",
            gate.display()
        )],
    )
    .await;
    std::fs::write(world.repo().join("outside"), "outside\n").unwrap();
    git::commit_all(&world.repo(), "move main only")
        .await
        .unwrap();
    let production = Production::new(&world);
    let running = production.run();
    eventually("slow head waits and fast target finishes", || async {
        world.attempt(&slow.id).await.state == S::Checking
            && world.task("fast-target").await.status == "done"
    })
    .await;
    assert!(!gate.exists());
    assert_eq!(
        git_out(&world.repo(), &["rev-parse", "refs/heads/release"]).await,
        Some(fast_sha)
    );
    std::fs::write(&gate, "go").unwrap();
    eventually("the slow target finishes after its check", || async {
        world.task("slow-target").await.status == "done"
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    for id in [&slow.id, &fast.id] {
        assert_eq!(
            world
                .attempt(id)
                .await
                .phase_timings
                .unwrap()
                .external_target_moves(),
            0
        );
    }
}

/// Every production pipeline boundary from the worker's crash matrix is
/// replayed over real Git, file-backed storage, Task steps and check runner.
/// A fault drops the driver at the boundary; a new claim takes it over after
/// the lease. An admitted effect is adopted from its owner's receipt.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn runtime_crash_boundaries_recover_through_real_ports_without_duplicate_fast_forwards() {
    use crate::integration_worker::{CrashPoint as C, IntegrationWorkerConfig};
    use std::sync::atomic::{AtomicBool, Ordering};
    let cases = [
        (false, S::Queued, C::BeforeTransition),
        (false, S::Queued, C::AfterTransition),
        (false, S::Validating, C::BeforeTransition),
        (false, S::Validating, C::AfterStepEnqueue),
        (false, S::Validating, C::AfterTransition),
        (false, S::AwaitingTaskStep, C::BeforeTransition),
        (false, S::AwaitingTaskStep, C::AfterTransition),
        (false, S::ReadyFf, C::BeforeTransition),
        (false, S::ReadyFf, C::AfterTransition),
        (false, S::FfInflight, C::BeforeEffect),
        (false, S::FfInflight, C::AfterEffectReceipt),
        (false, S::FfInflight, C::BeforeTransition),
        (false, S::FfInflight, C::AfterTransition),
        (false, S::Applied, C::AfterStepEnqueue),
        (false, S::Applied, C::BeforeTransition),
        (false, S::Applied, C::AfterTransition),
        (true, S::Rebasing, C::BeforeEffect),
        (true, S::Rebasing, C::AfterEffectReceipt),
        (true, S::Rebasing, C::BeforeTransition),
        (true, S::Rebasing, C::AfterStepEnqueue),
        (true, S::Rebasing, C::AfterTransition),
        (true, S::Checking, C::BeforeTransition),
        (true, S::Checking, C::AfterTransition),
    ];
    for (moved, state, point) in cases {
        let world = World::new().await;
        let a = world.add_task("crash", "crash.txt", "crash\n").await;
        if moved {
            std::fs::write(world.repo().join("outside"), "outside\n").unwrap();
            git::commit_all(&world.repo(), "target moved")
                .await
                .unwrap();
            review_ci(&world, "crash", &["true"]).await;
        }
        let (worker, daemons, config) = production_ports_config(
            &world,
            IntegrationWorkerConfig {
                lease: Duration::from_secs(2),
                renew_every: Duration::from_millis(250),
                poll: Duration::from_millis(20),
                sweep_interval: Duration::from_millis(100),
                reconcile_interval: Duration::from_millis(100),
                ..Default::default()
            },
        );
        let composed = world
            .service
            .compose_checks(daemons, Duration::from_secs(60), &config);
        let periodic = crate::worker_runtime::PeriodicWorkers::new(world.db.clone());
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let _checks = composed.worker.start(&periodic, stopped.clone());
        // Witnesses for a boundary that was passed without its fault (seen
        // once: `applied/BeforeTransition`). Every state write on the row is
        // recorded by the database itself, whoever made it, with the queue's
        // lease holder and the Task steps open at that instant; the hook
        // records every boundary THIS worker passed.
        sqlx::raw_sql(
            "CREATE TABLE crash_witness(seq INTEGER PRIMARY KEY, line TEXT NOT NULL);
             CREATE TRIGGER crash_witness_state AFTER UPDATE OF state ON integration_attempt
             WHEN OLD.state<>NEW.state BEGIN
               INSERT INTO crash_witness(line) SELECT
                 strftime('%H:%M:%f','now')||' '||OLD.state||' -> '||NEW.state
                 ||' revision='||NEW.revision
                 ||' lease_owner='||COALESCE((SELECT q.lease_owner FROM integration_queue q WHERE q.id=NEW.queue_id),'-')
                 ||' fence='||COALESCE((SELECT q.fence_generation FROM integration_queue q WHERE q.id=NEW.queue_id),-1)
                 ||' open_steps=['||COALESCE((SELECT group_concat(s.id||' '||s.kind||' '||s.causation_key||' '||s.status||' by '||COALESCE(s.claimed_by,'-'),'; ')
                    FROM task_step s WHERE s.task_id=NEW.task_ref AND s.status IN ('pending','claimed','suspended')),'')||']';
             END;",
        )
        .execute(world.db.pool())
        .await
        .unwrap();
        let instance = worker.instance.clone();
        let passed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fired = Arc::new(AtomicBool::new(false));
        let (flag, trace) = (fired.clone(), passed.clone());
        worker.set_fault(Some(Arc::new(move |at, here| {
            trace.lock().unwrap().push(format!("{at}/{here:?}"));
            at == state && here == point && !flag.swap(true, Ordering::SeqCst)
        })));
        let running = tokio::spawn(worker.run(stopped));
        eventually(&format!("{state}/{point:?} recovers"), || async {
            world.task("crash").await.status == "done"
                && world.attempt(&a.id).await.state == S::Completed
        })
        .await;
        if !fired.load(Ordering::SeqCst) {
            let writes: Vec<String> =
                sqlx::query_scalar("SELECT line FROM crash_witness ORDER BY seq")
                    .fetch_all(world.db.pool())
                    .await
                    .unwrap();
            let steps: Vec<String> = sqlx::query_scalar(
                "SELECT id||' '||kind||' '||causation_key||' '||status||' by '||COALESCE(claimed_by,'-')||' attempts='||attempts FROM task_step WHERE task_id='crash' ORDER BY seq",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            let after = world.attempt(&a.id).await;
            panic!(
                "{state}/{point:?}: boundary reached; the attempt is {} without the fault.\n\
                 the faulted worker is {instance}\n\
                 boundaries it passed: {:?}\n\
                 state writes on the attempt (any writer):\n  {}\n\
                 Task steps:\n  {}\n\
                 acknowledgment: {:?}\njournal: {}",
                after.state,
                passed.lock().unwrap(),
                writes.join("\n  "),
                steps.join("\n  "),
                after.effect_ack_json,
                after.operation_receipts_json,
            );
        }
        let _ = stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        let after = world.attempt(&a.id).await;
        let receipts: Vec<db::IntegrationEffectReceipt> =
            serde_json::from_value(after.effect_receipts_json).unwrap();
        let ff = receipts
            .iter()
            .filter(|receipt| {
                receipt.request.kind == db::IntegrationOperationKind::FastForward
                    && receipt.operation_state == db::IntegrationOperationState::Succeeded
            })
            .count();
        assert_eq!(ff, 1, "{state}/{point:?}: one fast-forward");
        assert_eq!(
            world
                .comments("crash")
                .await
                .iter()
                .filter(|comment| comment.starts_with("Changes merged"))
                .count(),
            1
        );
        let sha = after.integrated_sha.unwrap();
        assert!(git::command_output(
            &world.repo(),
            &["merge-base", "--is-ancestor", &sha, "main"]
        )
        .await
        .unwrap()
        .status
        .success());
    }
}
