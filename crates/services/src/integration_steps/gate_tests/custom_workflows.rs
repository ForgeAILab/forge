//! Merge outcomes and imported tasks under custom workflow states.
use super::*;

/// The targets of a merge outcome are found by role when a workflow renamed
/// its states, and by name when it kept them.
#[test]
fn merge_outcome_targets_follow_the_workflows_own_state_names() {
    use super::super::reconcile::role_target;
    let default = crate::workflow::engine::WorkflowEngine::resolve_workflow("{}");
    for name in ["done", "merge_failed", "review"] {
        assert_eq!(role_target(&default, "merging", name), name);
    }
    let mut renamed = default.clone();
    let rename = |name: &mut String| match name.as_str() {
        "merging" => *name = "landing".into(),
        "merge_failed" => *name = "fixing".into(),
        "done" => *name = "shipped".into(),
        "review" => *name = "inspection".into(),
        _ => {}
    };
    for state in &mut renamed.states {
        rename(&mut state.name);
        for trigger in state.triggers.values_mut() {
            rename(&mut trigger.to);
        }
    }
    assert!(super::super::reconcile::merge_state(&renamed, "landing"));
    assert!(!super::super::reconcile::merge_state(&renamed, "merging"));
    assert_eq!(role_target(&renamed, "landing", "done"), "shipped");
    assert_eq!(role_target(&renamed, "landing", "merge_failed"), "fixing");
    assert_eq!(role_target(&renamed, "landing", "review"), "inspection");
}

/// A workflow with TWO merge states: each reports a landed merge to its own
/// `accept` target, also when the workflow still has a state named `done`,
/// and each sends a repair to its own repair state.
#[test]
fn each_of_two_merge_states_has_its_own_outcome_targets() {
    use super::super::reconcile::{merge_state, role_target};
    let default = crate::workflow::engine::WorkflowEngine::resolve_workflow("{}");
    let mut two = default.clone();
    let copy = |name: &str, as_name: &str| {
        let mut state = default
            .states
            .iter()
            .find(|state| state.name == name)
            .unwrap_or_else(|| panic!("the default workflow has `{name}`"))
            .clone();
        state.name = as_name.into();
        state
    };
    // A second line: `hotfix_merging` lands in `released` and is repaired in
    // `hotfix_failed`.
    let mut merging = copy("merging", "hotfix_merging");
    for trigger in merging.triggers.values_mut() {
        match trigger.to.as_str() {
            "done" => trigger.to = "released".into(),
            "merge_failed" => trigger.to = "hotfix_failed".into(),
            _ => {}
        }
    }
    two.states.push(merging);
    two.states.push(copy("done", "released"));
    two.states.push(copy("merge_failed", "hotfix_failed"));
    assert!(merge_state(&two, "merging") && merge_state(&two, "hotfix_merging"));
    assert_eq!(role_target(&two, "merging", "done"), "done");
    assert_eq!(role_target(&two, "hotfix_merging", "done"), "released");
    assert_eq!(role_target(&two, "merging", "merge_failed"), "merge_failed");
    // Only where the default workflow's merge state names its repair by a
    // `reject` trigger is the second line's own repair state found by it.
    let own_reject = default
        .outgoing_trigger_targets("merging")
        .any(|(trigger, to)| trigger == api_types::WorkflowTrigger::Reject && to == "merge_failed");
    if own_reject {
        assert_eq!(
            role_target(&two, "hotfix_merging", "merge_failed"),
            "hotfix_failed"
        );
    }
}

/// A workflow of the Project's own in which NO state merges (`run_merge` is
/// on no entry): a Task in a state that happens to be called `merging` was
/// never merging, and the importer does not take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_importer_takes_nothing_from_a_workflow_with_no_merge_state() {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    let mut workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow("{}");
    for state in &mut workflow.states {
        state
            .hooks
            .on_enter
            .retain(|hook| hook.action != "run_merge");
    }
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow).unwrap())
        .execute(world.db.pool())
        .await
        .unwrap();
    let pass = world.db.import_integration_pass(100).await.unwrap();
    assert_eq!((pass.examined, pass.imported), (0, 0));
    assert!(world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .is_none());
}

/// An imported merge with an unknown result that nobody will ever report
/// (its old operation is recorded `uncertain`, with no fenced intent) used
/// to refuse its owner's Retry for good ("reconnect the machine or remove
/// it") and, with it, every deletion of its repository. The owner's Retry
/// now settles it explicitly: Git decides, and the Task merges once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_owners_retry_settles_an_imported_unknown_operation_and_the_task_merges_once() {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    // Startup classifies every row before any continuation may run.
    // Enqueuing first and then rewriting the row allowed the live step
    // worker to finish a different disposition before this fixture's write.
    world.db.import_integration_pass(100).await.unwrap();
    let imported = world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .unwrap();
    // As the importer and the worker leave a merge whose old operation never
    // reported and whose checkouts could not be read then.
    sqlx::query("UPDATE integration_attempt SET state='quarantined',current_operation_state='uncertain',operation_id='old-merge',failure_kind='needs_fact',failure_message='integration result unknown',available_at=NULL,revision=revision+1 WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    world
        .until("t", "the owner is told the result is unknown", || async {
            integration(&world.task("t").await)["cause"] == "unresolved_result"
        })
        .await;
    let refused = db::RepoRepo::delete(&*world.db, "r").await;
    assert!(
        matches!(&refused, Err(db::DbError::IntegrationUnresolved { task_id, .. }) if task_id == "t"),
        "{refused:?}"
    );
    let production = Production::new(&world);
    let running = production.run();
    // The public offer must exist and its action must end the wait.
    assert!(world
        .offers("t")
        .await
        .contains(&("retry".into(), "integration_retry".into())));
    world
        .act(
            "t",
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
            "t",
            "the Task merges after the explicit settlement",
            || async { world.task("t").await.status == "done" },
        )
        .await;
    assert_eq!(world.merges("t").await, 1);
    let old = world.attempt(&imported.id).await;
    assert_eq!(
        old.current_operation_state,
        Some(db::IntegrationOperationState::Failed)
    );
    assert_eq!(
        old.operation_receipts_json[0]["kind"],
        "imported_operation_settled"
    );
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

/// A workflow whose merge and repair states have other names, and whose
/// merge state has another entry hook: a Task in that merge state is
/// admitted by role, merges through the queue and ends done. A subtask in
/// the merge state is not admitted (subtasks do not merge); the root it
/// belongs to is.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_custom_workflow_with_renamed_states_merges_through_the_queue() {
    let world = World::new().await;
    world.rename_merge_states().await;
    world.legacy_task("t", "t.txt", "landing").await;
    world.seed_execution("t", "executor", None).await;
    world.touch("t").await;
    // A coordination root in the merge state, and a subtask under it.
    world.legacy_task("root", "root.txt", "landing").await;
    world.seed_execution("root", "executor", None).await;
    world.touch("root").await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,review_passed_at,created_at,updated_at) VALUES('child','p','root','child','landing',?,?,?)")
        .bind(&now).bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until(
            "t",
            "the Task merges from the renamed merge state",
            || async {
                dispatcher.reconcile_all().await.unwrap();
                world.task("t").await.status == "done"
            },
        )
        .await;
    // The root is taken by role too: it has an attempt of its own (or the
    // typed block that says why it cannot join the queue). Never neither.
    world
        .until("root", "the root is admitted or says why not", || async {
            dispatcher.reconcile_all().await.unwrap();
            world
                .db
                .current_integration_attempt("root")
                .await
                .unwrap()
                .is_some()
                || world.task("root").await.status == "done"
                || world
                    .task("root")
                    .await
                    .blocked_json
                    .is_some_and(|blocked| blocked.contains("integration_admission_failed"))
        })
        .await;
    println!("root: {}", world.dump("root").await);
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert!(world.on_main("t.txt").await);
    let transitions = world.transitions("t").await;
    assert_eq!(
        (transitions[0].0.as_str(), transitions[0].1.as_str()),
        ("landing", "done"),
        "{transitions:?}"
    );
    assert!(
        world
            .db
            .current_integration_attempt("child")
            .await
            .unwrap()
            .is_none(),
        "a subtask is not admitted"
    );
}

/// The importer selects and classifies by role: a Task that was merging in
/// a renamed merge state before the queue is imported (row P18) and merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_importer_takes_a_task_in_a_renamed_merge_state() {
    let mut world = World::new().await;
    world.rename_merge_states().await;
    world.legacy_task("t", "t.txt", "landing").await;
    world.seed_execution("t", "executor", None).await;
    // A Task of a default-workflow name in this Project is not a merge.
    world.legacy_task("other", "other.txt", "merging").await;
    world.touch("t").await;
    // The importer runs before any worker, as at a start: the continuation
    // admits this row again, so the read below must come before it.
    world.stop_steps().await;
    let report = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!(report.imported, 1, "{report:?}");
    let imported = world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        imported.import_source_json.as_ref().unwrap()["priority"],
        18
    );
    world.start_steps();
    let production = Production::new(&world);
    merged_as(&world, "t", "done", &production).await;
    assert_eq!(world.merges("t").await, 1);
}

/// A drain starts the head driver itself and stops it when it returns. One
/// stopped right after its claim leaves the attempt in the slot; when the
/// Task has left the entry by then, only a head driver ends that attempt.
/// The next drain is not done while it holds the slot: it pumps the worker,
/// whose driver cancels the attempt of a Task that left. (The model's
/// `pre_queue_bare_p14_merge_failed_on_a_locked_index` waited here forever:
/// the Task's next entry into the queue retried against the stale attempt
/// with nothing sweeping the queue.)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_drain_ends_the_slot_of_a_task_that_left_its_entry() {
    let world = World::new().await;
    let attempt = world.reconciled("left", "left.txt").await;
    let worker = world.service.integration_worker_or_embedded();
    // The drain's driver, stopped before its first pass.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let driver = loop {
        if let Some(driver) = worker.sweep_once().await.unwrap().pop() {
            break driver;
        }
        assert!(tokio::time::Instant::now() < deadline, "no queue to claim");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let stop = tokio_util::sync::CancellationToken::new();
    stop.cancel();
    driver.run(stop).await;
    let held = world.attempt(&attempt.id).await;
    assert_eq!(held.state, S::Queued, "{}", world.dump("left").await);
    let queue = held.queue_id.clone().expect("in a queue");
    let head: Option<String> =
        sqlx::query_scalar("SELECT head_attempt_id FROM integration_queue WHERE id=?")
            .bind(&queue)
            .fetch_one(world.db.pool())
            .await
            .unwrap();
    assert_eq!(
        head.as_deref(),
        Some(attempt.id.as_str()),
        "it holds the slot"
    );
    // The Task leaves the entry (as the old build's queued cascade moved a
    // Task to `merge_failed` under the imported attempt), and the stopped
    // driver's lease lapses.
    sqlx::query("UPDATE task SET status='merge_failed',status_epoch=status_epoch+1,version=version+1 WHERE id='left'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.touch("left").await;
    sqlx::query("UPDATE integration_queue SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?")
        .bind(&queue)
        .execute(world.db.pool())
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(60), world.service.drain("left"))
        .await
        .expect("the drain returns")
        .unwrap();
    let ended = world.attempt(&attempt.id).await;
    assert_eq!(
        ended.state,
        S::Cancelled,
        "the drain left the attempt of a Task that left its entry in the slot\n{}",
        world.dump("left").await
    );
    let head: Option<String> =
        sqlx::query_scalar("SELECT head_attempt_id FROM integration_queue WHERE id=?")
            .bind(&queue)
            .fetch_one(world.db.pool())
            .await
            .unwrap();
    assert_eq!(head, None, "the slot is free");
}

/// Every merge role renamed, at runtime: a Task in the renamed merge state
/// merges through the queue and ends in the renamed done state; a Task
/// cancelled out of the renamed merge state the old way is read by the
/// start-time import by role (its state is called `dropped`) and gets its
/// history row; a second start imports nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn renamed_done_cancelled_and_review_states_are_followed_by_role() {
    let world = World::new().await;
    world.rename_states(true).await;
    world.legacy_task("t", "t.txt", "landing").await;
    world.seed_execution("t", "executor", None).await;
    world.touch("t").await;
    // Cancelled out of the merge state before the queue existed.
    world.legacy_task("c", "c.txt", "dropped").await;
    world.seed_execution("c", "executor", None).await;
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at) VALUES('c-left','c','landing','dropped','user:api','cancelled',?)")
        .bind(db::now_rfc3339())
        .execute(world.db.pool())
        .await
        .unwrap();
    world.touch("c").await;
    let report = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert!(
        report.imported >= 2,
        "both Tasks are read by role: {report:?}"
    );
    let history = world
        .db
        .integration_attempt(
            &sqlx::query_scalar::<_, String>(
                "SELECT id FROM integration_attempt WHERE task_ref='c' ORDER BY created_at DESC LIMIT 1",
            )
            .fetch_one(world.db.pool())
            .await
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.state, S::Cancelled, "a history row: {history:?}");
    assert!(!history.current);
    let again = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!(again.imported, 0, "{again:?}");

    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until("t", "the Task ends in the renamed done state", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("t").await.status == "shipped"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert!(world.on_main("t.txt").await);
    assert!(!world.on_main("c.txt").await);
    assert_eq!(world.task("c").await.status, "dropped");
}
