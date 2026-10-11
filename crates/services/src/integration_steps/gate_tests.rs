//! The Task gate and the reconciler against the real worker over its
//! production ports, the real step worker and real Git (plan 3.2 stage D2b,
//! round 2). A head is stepped by hand where a test needs the gate to land at
//! one exact point; otherwise the worker runs as the server runs it.
use super::*;
use crate::integration_worker::{HeadDriver, Pass};
use api_types::TaskAction;

impl World {
    /// A Task in `merging` that nothing has admitted (no attempt, no entry
    /// step), with its implementation run, then admitted by the reconciler's
    /// step: the attempt is bound and `waiting` is stated.
    async fn reconciled(&self, task: &str, file: &str) -> IntegrationAttempt {
        self.legacy_task(task, file, "merging").await;
        self.seed_execution(task, "executor", None).await;
        let row = self.task(task).await;
        assert!(
            self.service
                .reconcile_integration_admission(&row)
                .await
                .unwrap(),
            "the reconciler admits a merging Task nothing owns"
        );
        let attempt = self
            .db
            .current_integration_attempt(task)
            .await
            .unwrap()
            .expect("admitted");
        assert_eq!(attempt.state, S::Queued);
        assert!(attempt.workspace_id.is_some(), "bound to the placement");
        attempt
    }
    /// A fixture wrote the Task's legacy columns with plain SQL: derive its
    /// stored condition again, as every real write does.
    async fn touch(&self, task: &str) {
        db::TaskRepo::mutate_metadata(&*self.db, task, None, Vec::new(), &db::now_rfc3339())
            .await
            .unwrap();
    }
    async fn move_target(&self, file: &str) {
        std::fs::write(self.repo().join(file), "moved\n").unwrap();
        git::commit_all(&self.repo(), "target moved").await.unwrap();
    }
    async fn main_tip(&self) -> String {
        git_out(&self.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap()
    }
    async fn on_main(&self, file: &str) -> bool {
        let tip = self.main_tip().await;
        git_out(&self.repo(), &["cat-file", "-e", &format!("{tip}:{file}")])
            .await
            .is_some()
    }
    async fn act(&self, task: &str, action: TaskAction) -> db::Task {
        let version = self.task(task).await.version;
        self.service
            .perform_task_action(task, action.clone(), version)
            .await
            .unwrap_or_else(|error| panic!("{action:?} on {task}: {error}"))
            .task
    }
    async fn offers(&self, task: &str) -> Vec<(String, String)> {
        self.service
            .task_action_offers(
                task,
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await
            .unwrap()
            .available_actions
            .into_iter()
            .map(|offer| (offer.action.verb().to_owned(), offer.reason))
            .collect()
    }
    /// Rebase receipts on every attempt of the Task: Git rebases performed.
    async fn rebases(&self, task: &str) -> usize {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT effect_receipts_json FROM integration_attempt WHERE task_ref=?",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap();
        rows.iter()
            .flat_map(|raw| serde_json::from_str::<Vec<Value>>(raw).unwrap_or_default())
            .filter(|receipt| {
                receipt["request"]["kind"] == "rebase" && receipt["operation_state"] == "succeeded"
            })
            .count()
    }
    async fn merges(&self, task: &str) -> usize {
        self.comments(task)
            .await
            .iter()
            .filter(|comment| comment.starts_with("Changes merged to main"))
            .count()
    }
}

/// `(admission key, state, failure message, available_at, effect_seq, journal)`.
type AttemptRow = (String, String, Option<String>, Option<String>, i64, String);

impl World {
    /// Everything a failed wait needs to be read: the Task's condition, its
    /// attempts and its steps.
    async fn dump(&self, task: &str) -> String {
        let row = self.task(task).await;
        let attempts: Vec<AttemptRow> = sqlx::query_as(
            "SELECT admission_key,state,failure_message,available_at,effect_seq,operation_receipts_json FROM integration_attempt WHERE task_ref=? ORDER BY created_at,id",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap();
        let steps: Vec<String> = self
            .db
            .task_steps(task)
            .await
            .unwrap()
            .iter()
            .map(|step| {
                format!(
                    "#{} {} {} {} attempts={} error={:?}",
                    step.seq,
                    step.kind,
                    step.causation_key,
                    step.status,
                    step.attempts,
                    step.last_error
                )
            })
            .collect();
        format!(
            "status={} version={} blocked={:?} annotation={:?} condition={}\nattempts={attempts:#?}\nsteps={steps:#?}",
            row.status,
            row.version,
            row.blocked_json,
            row.error_annotation,
            json!(row.condition)
        )
    }
    /// `eventually` with a short bound and the Task's rows in the failure.
    async fn until<F, Fut>(&self, task: &str, what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            if check().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for: {what}\n{}", self.dump(task).await);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn claim(production: &Production) -> HeadDriver {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(driver) = production.worker.sweep_once().await.unwrap().pop() {
            return driver;
        }
        assert!(tokio::time::Instant::now() < deadline, "no queue to claim");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Step the head until `attempt` is in `stop` (that state's own pass has not
/// run) or it left the slot. Returns the attempt's state.
async fn step_to(world: &World, driver: &mut HeadDriver, attempt: &str, stop: S) -> S {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state = world.attempt(attempt).await.state;
        if state == stop {
            return state;
        }
        match driver.pass().await.unwrap() {
            Pass::Released | Pass::Lost => return world.attempt(attempt).await.state,
            Pass::Wait(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            Pass::Progress => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "head never reached {stop}"
        );
    }
}

async fn merged(world: &World, task: &str, production: &Production) {
    let running = production.run();
    world
        .until(task, &format!("{task} is done"), || async {
            world.task(task).await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

fn gate_code(attempt: &IntegrationAttempt) -> String {
    attempt
        .failure_message
        .clone()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// `settle`'s own gate read. The head driver read the Task gate, found it
/// open and asked for the decision; the owner's Hold lands before the step
/// runs. The step is held here by the test (nothing runs it until it is
/// asked), which is the seam between the driver's read and the step. The
/// step reads the gate itself: no permit is written, no protected `result`
/// step exists, the answer names the gate, and the Task did not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_between_the_drivers_gate_read_and_settle_writes_no_permit() {
    let world = World::new().await;
    let a = world.reconciled("t", "t.txt").await;
    // The driver's side: gate read open, the head waits for the decision.
    assert_eq!(
        crate::integration_steps::gate::task_gate(&world.db, "t")
            .await
            .unwrap(),
        None
    );
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    // The Hold lands now.
    let held = world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(held.status, "merging");
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;

    let after = world.attempt(&a.id).await;
    assert!(after.permit_json.is_none(), "no permit under a Hold");
    let answer = ack(&after, IntegrationStepAction::Settle).expect("settle answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Infrastructure);
    assert!(
        answer
            .message
            .as_deref()
            .is_some_and(crate::integration_worker::GateCause::parked_under),
        "the answer names the gate: {:?}",
        answer.message
    );
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    assert_eq!(world.task("t").await.status, "merging");
    assert_eq!(world.merges("t").await, 0);
}

/// An attempt was sent back (`ejected`, `needs_review`) but its `send_back`
/// step never moved the Task: the Task is still in the `merging` entry the
/// attempt was admitted in, with an attempt nothing drives. The reconciler
/// gives the Task a successor (same place in line), which merges; the
/// sent-back row is history. Nothing is merged twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_sent_back_attempt_whose_task_stayed_in_the_entry_gets_a_successor() {
    for state in ["ejected", "needs_review"] {
        let world = World::new().await;
        let attempt = world.reconciled("t", "t.txt").await;
        world
            .set(
                &attempt.id,
                &format!("state='{state}',failure_kind='infrastructure',failure_message='task_step_failed: send_back'"),
            )
            .await;
        assert_eq!(world.task("t").await.status, "merging");
        let production = Production::new(&world);
        let running = production.run();
        // The worker alone does not take it up: the row left the queue.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(world.task("t").await.status, "merging", "{state}");
        assert_eq!(world.merges("t").await, 0, "{state}");
        let dispatcher = dispatcher(&world).await;
        world
            .until("t", &format!("{state}: the successor merges"), || async {
                dispatcher.reconcile_all().await.unwrap();
                world.task("t").await.status == "done"
            })
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "{state}");
        let old = world.attempt(&attempt.id).await;
        assert_eq!(old.state, S::Superseded, "{state}");
        let successor = world
            .db
            .current_integration_attempt("t")
            .await
            .unwrap()
            .map(|current| (current.id != attempt.id, current.queue_seq == old.queue_seq));
        // The successor is the Task's record now, in the same place in line.
        if let Some((different, same_seq)) = successor {
            assert!(different && same_seq, "{state}");
        }
    }
}

/// Hold while queued, then Release. The held member is skipped (it leaves
/// the slot at once, with no Git effect, no check and no counted retry), the
/// member behind it merges, and the Release alone puts it back: it merges
/// with one rebase.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_held_while_queued_is_skipped_and_merges_after_its_release() {
    let world = World::new().await;
    let held = world.reconciled("held", "held.txt").await;
    let other = world.reconciled("other", "other.txt").await;
    assert!(
        world
            .offers("held")
            .await
            .contains(&("hold".into(), "integration_wait".into())),
        "{:?}",
        world.offers("held").await
    );
    let task = world.act("held", TaskAction::Hold { reason: None }).await;
    assert_eq!(task.status, "merging");
    assert_eq!(
        crate::integration_steps::gate::task_row_gate(&world.task("held").await),
        Some(crate::integration_worker::GateCause::Held),
        "the Hold is recorded on the Task"
    );
    let production = Production::new(&world);
    let running = production.run();
    eventually("the Task behind the held one merges", || async {
        world.task("other").await.status == "done"
    })
    .await;
    eventually("the held Task left the slot", || async {
        world.attempt(&held.id).await.state == S::Parked
    })
    .await;
    let parked = world.attempt(&held.id).await;
    assert_eq!(gate_code(&parked), "task_held", "{parked:?}");
    println!(
        "held: integration={} offers={:?}",
        integration(&world.task("held").await),
        world.offers("held").await
    );
    assert!(parked.available_at.is_some(), "the sweep looks again");
    assert_eq!(world.rebases("held").await, 0);
    assert!(world
        .step("held", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert!(world
        .step("held", IntegrationStepAction::Settle)
        .await
        .is_empty());
    assert!(!world.on_main("held.txt").await);
    let journal = parked.operation_receipts_json.as_array().unwrap().clone();
    assert!(
        journal.iter().all(|entry| entry["counted"] != true),
        "no retry is spent on a gate: {journal:?}"
    );
    // Held for many sweeps: still one journal entry, no round counted.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let later = world.attempt(&held.id).await;
    assert_eq!(
        later.operation_receipts_json.as_array().map_or(0, Vec::len),
        1,
        "{}",
        world.dump("held").await
    );
    assert_eq!(later.phase_timings.map_or(0, |timings| timings.rounds), 0);
    assert_eq!(world.task("held").await.status, "merging");
    // The owner's Release is the only action needed.
    let offers = world.offers("held").await;
    assert!(
        offers.iter().any(|(verb, _)| verb == "release"),
        "{offers:?}"
    );
    world
        .act("held", TaskAction::Release { reason: None })
        .await;
    world
        .until("held", "the released Task merges", || async {
            world.task("held").await.status == "done"
                && world.attempt(&held.id).await.state == S::Completed
                && world.attempt(&other.id).await.state == S::Completed
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert!(world.on_main("held.txt").await && world.on_main("other.txt").await);
    assert_eq!(
        world.rebases("held").await,
        1,
        "rebased once, after release"
    );
    assert_eq!(world.merges("held").await, 1);
    assert_eq!(world.attempt(&held.id).await.state, S::Completed);
    assert_eq!(world.attempt(&other.id).await.state, S::Completed);
}

/// Hold while the Task is the head, before its rebase: the rebase is never
/// asked for. After the Release it is rebased once, checked once and merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_before_its_rebase_runs_no_git_effect_and_merges_after_release() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.move_target("moved").await;
    let head = git::get_current_sha(&world.tree("t")).await.unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Rebasing).await,
        S::Rebasing
    );
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Checking).await,
        S::Parked,
        "the head leaves the slot instead of rebasing"
    );
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(gate_code(&parked), "task_held");
    assert_eq!(world.rebases("t").await, 0);
    assert_eq!(git::get_current_sha(&world.tree("t")).await.unwrap(), head);
    assert!(world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 1);
    assert_eq!(world.merges("t").await, 1);
    assert!(world.on_main("t.txt").await);
}

/// Hold while the head's check runs. The head leaves the slot; the verdict
/// of the running check arrives for a parked attempt and is applied to
/// nothing (no permit, no merge, no Review of a held Task's merge). After
/// the Release the rebase is not repeated; the check is asked again and the
/// Task merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_during_its_check_discards_the_verdict_and_does_not_rebase_twice() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    review_ci(&world, "t", &["sleep 1 && test -f t.txt"]).await;
    world.move_target("moved").await;
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Checking).await,
        S::Checking
    );
    assert_eq!(world.rebases("t").await, 1);
    let rebased = git::get_current_sha(&world.tree("t")).await.unwrap();
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::AwaitingTaskStep).await,
        S::Parked
    );
    let tip = world.main_tip().await;
    // The check that was running ends; its verdict finds nothing to apply to.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(gate_code(&parked), "task_held");
    assert!(parked.permit_json.is_none());
    assert_eq!(world.main_tip().await, tip);
    assert_eq!(world.task("t").await.status, "merging");
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    let asked = world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .len();
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 1, "the rebase is not repeated");
    assert_eq!(
        git::get_current_sha(&world.tree("t")).await.unwrap(),
        rebased
    );
    assert!(
        world
            .step("t", IntegrationStepAction::RequestCheck)
            .await
            .len()
            > asked,
        "the check is asked again after the gate lifted"
    );
    assert_eq!(world.merges("t").await, 1);
    assert_eq!(world.main_tip().await, rebased);
}

/// Hold while the Task is the head on an unchanged target, before the
/// permit: no `settle` step is asked for and nothing lands. After the
/// Release the reviewed commit merges as it is, with no rebase and no check.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_head_held_before_the_permit_gets_none_and_merges_as_reviewed_after_release() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    let reviewed = git::get_current_sha(&world.tree("t")).await.unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Validating).await,
        S::Validating
    );
    world.act("t", TaskAction::Hold { reason: None }).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::ReadyFf).await,
        S::Parked
    );
    assert!(world
        .step("t", IntegrationStepAction::Settle)
        .await
        .is_empty());
    assert!(world.attempt(&attempt.id).await.permit_json.is_none());
    assert!(!world.on_main("t.txt").await);
    world.act("t", TaskAction::Release { reason: None }).await;
    merged(&world, "t", &production).await;
    assert_eq!(world.rebases("t").await, 0);
    assert!(world
        .step("t", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    assert_eq!(world.main_tip().await, reviewed);
    assert_eq!(world.merges("t").await, 1);
}

/// Each gate that is not a Hold, set and lifted by a plain row write with
/// nothing notifying the worker: the head leaves the slot under the typed
/// cause, and the worker's sweep alone re-queues the Task when the gate has
/// lifted. One merge, no check, no counted retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_gate_parks_typed_and_the_sweep_alone_requeues_when_it_lifts() {
    let cases: [(&str, &str, &str); 5] = [
        (
            "task_blocked",
            "UPDATE task SET blocked_json='{\"reason\":\"stopped by the owner\"}' WHERE id='t'",
            "UPDATE task SET blocked_json=NULL WHERE id='t'",
        ),
        (
            "task_failed",
            "UPDATE task SET failed_json='{\"reason\":\"failed\"}' WHERE id='t'",
            "UPDATE task SET failed_json=NULL WHERE id='t'",
        ),
        (
            "task_entry_blocked",
            "UPDATE task SET entry_barrier_json='{\"state\":\"merging\",\"status\":\"blocked\"}' WHERE id='t'",
            "UPDATE task SET entry_barrier_json=NULL WHERE id='t'",
        ),
        (
            "task_awaiting_human",
            "UPDATE task SET metadata_json='{\"awaiting_human\":true}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL WHERE id='t'",
        ),
        (
            "project_paused",
            "UPDATE project SET paused_at='2026-10-10T00:00:00Z' WHERE id='p'",
            "UPDATE project SET paused_at=NULL WHERE id='p'",
        ),
    ];
    for (code, set, lift) in cases {
        let world = World::new().await;
        let attempt = world.reconciled("t", "t.txt").await;
        sqlx::query(set).execute(world.db.pool()).await.unwrap();
        let tip = world.main_tip().await;
        let production = Production::new(&world);
        let running = production.run();
        eventually(&format!("{code}: the head leaves the slot"), || async {
            world.attempt(&attempt.id).await.state == S::Parked
        })
        .await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let parked = world.attempt(&attempt.id).await;
        assert_eq!(gate_code(&parked), code, "{parked:?}");
        println!(
            "{code}: integration={} offers={:?}",
            integration(&world.task("t").await),
            world.offers("t").await
        );
        assert!(parked.permit_json.is_none(), "{code}");
        assert_eq!(world.main_tip().await, tip, "{code}: nothing landed");
        assert_eq!(world.task("t").await.status, "merging", "{code}");
        assert!(
            world
                .step("t", IntegrationStepAction::Settle)
                .await
                .is_empty(),
            "{code}: no decision is asked for under a gate"
        );
        let journal = parked.operation_receipts_json.as_array().unwrap().clone();
        assert!(
            journal.iter().all(|entry| entry["counted"] != true),
            "{code}: {journal:?}"
        );
        sqlx::query(lift).execute(world.db.pool()).await.unwrap();
        world
            .until(
                "t",
                &format!("{code}: the Task merges after the gate"),
                || async { world.task("t").await.status == "done" },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "{code}");
        assert_eq!(world.rebases("t").await, 0, "{code}");
        assert!(world.on_main("t.txt").await, "{code}");
    }
}

/// A paused Agent does not gate a merge: the Task its paused Agent is assigned
/// to is rebased, checked and fast-forwarded like any other (machine work, no
/// Agent turn), the attempt is never parked for it, and it merges once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_task_whose_agent_is_paused_still_merges() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.legacy_task("agent-home", "home.txt", "done").await;
    let agent = world.seed_reviewer("agent-home").await;
    sqlx::query("UPDATE task SET assignee_type='agent',assignee_id=? WHERE id='t'")
        .bind(&agent)
        .execute(world.db.pool())
        .await
        .unwrap();
    let paused = sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(&agent)
        .execute(world.db.pool())
        .await
        .unwrap();
    assert_eq!(paused.rows_affected(), 1, "the assigned Agent is paused");
    assert_eq!(
        crate::integration_steps::gate::task_gate(&world.db, "t")
            .await
            .unwrap(),
        None,
        "a paused Agent is no gate"
    );
    // The target moved: the queue has to rebase and check, all under the pause.
    world.move_target("other.txt").await;
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("t", "the Task merges under its paused Agent", || async {
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let journal: String = world
        .scalar(
            "SELECT operation_receipts_json FROM integration_attempt WHERE id=?",
            &attempt.id,
        )
        .await;
    assert!(!journal.contains("agent_paused"), "{journal}");
    assert_eq!(world.merges("t").await, 1);
    assert_eq!(world.rebases("t").await, 1);
    assert!(world.on_main("t.txt").await);
}

/// Project pause and resume through the service call the resume route
/// makes: the waiting attempt is due at once, not at its next look.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_project_resume_puts_its_waiting_merges_back_in_line_at_once() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    db::ProjectRepo::set_paused_at(&*world.db, "p", Some(db::now_rfc3339()))
        .await
        .unwrap();
    let production = Production::new(&world);
    let mut driver = claim(&production).await;
    assert_eq!(
        step_to(&world, &mut driver, &attempt.id, S::Validating).await,
        S::Parked
    );
    let parked = world.attempt(&attempt.id).await;
    assert_eq!(gate_code(&parked), "project_paused");
    // Far in the future: only the resume can make it due.
    world
        .set(&attempt.id, "available_at='2099-01-01T00:00:00Z'")
        .await;
    db::ProjectRepo::set_paused_at(&*world.db, "p", None)
        .await
        .unwrap();
    world
        .service
        .integration_project_resumed("p")
        .await
        .unwrap();
    merged(&world, "t", &production).await;
    assert_eq!(world.merges("t").await, 1);
}

// ----- the reconciler (B) -------------------------------------------------

/// The dispatcher over this World. Its repository sync pauses a Project
/// that has no primary repository, so the World's Project gets its one.
async fn dispatcher(world: &World) -> crate::TaskDispatcher {
    sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p' AND primary_repo_id IS NULL")
        .execute(world.db.pool())
        .await
        .unwrap();
    crate::TaskDispatcher::new(
        world.db.clone(),
        world.bus.clone(),
        Arc::new(world.service.clone()),
    )
}

impl World {
    /// Run the dispatcher's reconciliation until the Task has a current
    /// attempt that is `queued` (or give up after a few laps).
    async fn reconcile_until_admitted(&self, task: &str) -> Option<IntegrationAttempt> {
        let dispatcher = dispatcher(self).await;
        for _ in 0..40 {
            dispatcher.reconcile_all().await.unwrap();
            if let Some(attempt) = self.db.current_integration_attempt(task).await.unwrap() {
                if attempt.state == S::Queued
                    && attempt.original_candidate_sha.is_some()
                    && attempt.import_source_json.is_none()
                {
                    return Some(attempt);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }
}

/// A Task in its merge state with no attempt and no entry step (the entry
/// hook returned `Skipped`, the import did not take it, a manual move
/// entered the state): the dispatcher's sweep admits it through the hook's
/// own admission function and it merges. Nothing else is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_dispatcher_admits_a_merging_task_that_nothing_drives() {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", "merging").await;
    world.seed_execution("t", "executor", None).await;
    world.touch("t").await;
    assert!(world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .is_none());
    let resolved = dispatcher(&world)
        .await
        .resolved_for_test("t")
        .await
        .unwrap();
    assert!(
        resolved.starts_with("Step(AdmitIntegration)"),
        "the scheduler resolves the Task to the reconciler's step: {resolved}"
    );
    let attempt = match world.reconcile_until_admitted("t").await {
        Some(attempt) => attempt,
        None => panic!(
            "not admitted: {}\n{}",
            dispatcher(&world)
                .await
                .resolved_for_test("t")
                .await
                .unwrap(),
            world.dump("t").await
        ),
    };
    assert_eq!(attempt.admission_key, "admit:t:0");
    assert!(integration(&world.task("t").await)
        .to_string()
        .contains("waiting"));
    // A second lap admits nothing more: the same row.
    dispatcher(&world).await.reconcile_all().await.unwrap();
    let again = world
        .db
        .current_integration_attempt("t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(again.id, attempt.id);
    let production = Production::new(&world);
    merged(&world, "t", &production).await;
    assert_eq!(world.merges("t").await, 1);
    // While the import is pending nothing is admitted.
    world.legacy_task("late", "late.txt", "merging").await;
    world.seed_execution("late", "executor", None).await;
    world.touch("late").await;
    world
        .service
        .integration_import_pending()
        .store(true, std::sync::atomic::Ordering::Release);
    dispatcher(&world).await.reconcile_all().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(world
        .db
        .current_integration_attempt("late")
        .await
        .unwrap()
        .is_none());
}

/// Importer rows that wait for the Task's owner (P13, P15, P16, P17), after
/// the owner's action lifted the block IN PLACE (the Task never left its
/// merge state, so no entry hook runs again): while the block stands the
/// reconciler admits nothing; once it is lifted the next sweep admits the
/// Task and it merges, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_owner_waits_are_admitted_by_the_sweep_once_their_block_is_lifted_in_place() {
    let rows: [(u64, &str, &str); 4] = [
        (
            13,
            "UPDATE task SET blocked_json='{\"reason\":\"held by the owner\"}' WHERE id='t'",
            "UPDATE task SET blocked_json=NULL,version=version+1 WHERE id='t'",
        ),
        (
            15,
            "UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"gone\"}}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL,version=version+1 WHERE id='t'",
        ),
        (
            16,
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
            "UPDATE workspace SET error=NULL WHERE task_id='t'",
        ),
        (
            17,
            "UPDATE task SET metadata_json='{\"dispatch_disposition\":{\"capability\":\"x\",\"version\":0}}' WHERE id='t'",
            "UPDATE task SET metadata_json=NULL,version=version+1 WHERE id='t'",
        ),
    ];
    for (priority, seed, lift) in rows {
        let world = World::new().await;
        world.legacy_task("t", "t.txt", "merging").await;
        world.seed_execution("t", "executor", None).await;
        sqlx::query(seed).execute(world.db.pool()).await.unwrap();
        world.touch("t").await;
        let (imported, row) = world.imported("t").await;
        assert_eq!(row, priority);
        let production = Production::new(&world);
        let running = production.run();
        world
            .until(
                "t",
                &format!("P{priority}: the continuation settles"),
                || async {
                    world
                        .db
                        .task_steps("t")
                        .await
                        .unwrap()
                        .iter()
                        .all(|step| !matches!(step.status.as_str(), "pending" | "claimed"))
                },
            )
            .await;
        if priority == 13 {
            // The block stands: the sweep admits nothing.
            let dispatcher = dispatcher(&world).await;
            for _ in 0..3 {
                dispatcher.reconcile_all().await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let current = world
                .db
                .current_integration_attempt("t")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.id, imported.id, "P13 stays put while blocked");
            assert!(!world.on_main("t.txt").await);
        }
        sqlx::query(lift).execute(world.db.pool()).await.unwrap();
        world.touch("t").await;
        let dispatcher = dispatcher(&world).await;
        world
            .until(
                "t",
                &format!("P{priority}: merged after the lift"),
                || async {
                    dispatcher.reconcile_all().await.unwrap();
                    world.task("t").await.status == "done"
                },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "P{priority}");
        assert_eq!(
            world.attempt(&imported.id).await.state,
            S::Superseded,
            "P{priority}: the imported row is history"
        );
    }
}

/// Importer rows P16 (the workspace's owner is lost) and P17 (an accepted
/// owner command or a sticky disposition was still to run): each ends in a
/// typed condition its owner sees (`deferred` / `owner_required`), nothing
/// merges by itself, and the action the Task offers for it (Retry) works:
/// the Task is admitted again and merges, once. The legacy state is left as
/// the old build wrote it; no sweep of the dispatcher runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_lost_owner_and_accepted_command_rows_state_owner_required_and_retry_works() {
    let rows: [(u64, &str); 2] = [
        (
            16,
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
        ),
        (
            17,
            "UPDATE task SET metadata_json='{\"queued_recovery\":{\"id\":\"accepted\"}}' WHERE id='t'",
        ),
    ];
    for (priority, seed) in rows {
        let world = World::new().await;
        world.legacy_task("t", "t.txt", "merging").await;
        world.seed_execution("t", "executor", None).await;
        sqlx::query(seed).execute(world.db.pool()).await.unwrap();
        // The stored condition follows the row, as its writer left it.
        let mut tx = db::begin_immediate(world.db.pool()).await.unwrap();
        world
            .db
            .produce_condition_in_tx(&mut tx, "t", db::ConditionChange::Full)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let (imported, row) = world.imported("t").await;
        assert_eq!(row, priority);
        let production = Production::new(&world);
        let running = production.run();
        world
            .until("t", &format!("P{priority}: the owner is told"), || async {
                integration(&world.task("t").await)["kind"] == "deferred"
            })
            .await;
        let stated = integration(&world.task("t").await);
        assert_eq!(stated["cause"], "owner_required", "P{priority}: {stated}");
        // Nothing moves it but its owner.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(world.task("t").await.status, "merging", "P{priority}");
        assert_eq!(world.merges("t").await, 0, "P{priority}");
        assert_eq!(world.attempt(&imported.id).await.state, S::Parked);
        let offers = world.offers("t").await;
        assert!(
            offers.iter().any(|(verb, _)| verb == "retry"),
            "P{priority}: the Task offers Retry: {offers:?}"
        );
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
                &format!("P{priority}: the retried merge lands"),
                || async { world.task("t").await.status == "done" },
            )
            .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        assert_eq!(world.merges("t").await, 1, "P{priority}");
        assert_eq!(
            world.attempt(&imported.id).await.state,
            S::Superseded,
            "P{priority}: the imported row is history"
        );
    }
}

/// A merge that stopped for its owner (`retries_exhausted`, a dirty checkout,
/// ...) states `deferred` on the Task and offers Retry; the Retry puts the
/// same attempt back in line with its counted retries started again, and it
/// merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stopped_merge_offers_retry_and_the_retry_gets_it_merged() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    // As the worker leaves it when its counted retries are spent.
    world
        .set(
            &attempt.id,
            "state='parked',failure_kind='owner_required',failure_message='retries_exhausted: infrastructure: the owner did not answer',operation_receipts_json='[{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0},{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0},{\"kind\":\"worker_park\",\"reason\":\"infrastructure\",\"counted\":true,\"effect_seq\":0}]'",
        )
        .await;
    let parked = world.attempt(&attempt.id).await;
    world.ask(&parked, IntegrationStepAction::Park).await;
    world
        .until("t", "the Task says why its merge stopped", || async {
            integration(&world.task("t").await)
                .to_string()
                .contains("owner_required")
        })
        .await;
    let offers = world.offers("t").await;
    assert!(
        offers.contains(&("retry".into(), "integration_retry".into())),
        "{offers:?}"
    );
    let production = Production::new(&world);
    let running = production.run();
    // Nothing moves it but the owner.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(world.attempt(&attempt.id).await.state, S::Parked);
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
        .until("t", "the retried merge lands", || async {
            world.task("t").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(world.merges("t").await, 1);
    let done = world
        .db
        .integration_attempt(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        done.operation_receipts_json
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["kind"] == "owner_retry"),
        "the same attempt, retried by its owner"
    );
}

/// One Task the import cannot read does not stop the pass: it is
/// quarantined with the error, the others are imported, and the unreadable
/// Task is still admitted by the reconciler from its present rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unreadable_task_is_quarantined_and_the_import_continues() {
    let world = World::new().await;
    for task in ["bad", "good"] {
        world
            .legacy_task(task, &format!("{task}.txt"), "merging")
            .await;
        world.seed_execution(task, "executor", None).await;
        world.touch(task).await;
    }
    sqlx::query(
        "CREATE TRIGGER unreadable BEFORE INSERT ON integration_attempt \
         WHEN NEW.task_ref='bad' AND NEW.state<>'quarantined' \
         BEGIN SELECT RAISE(ABORT,'legacy rows of this Task cannot be read'); END",
    )
    .execute(world.db.pool())
    .await
    .unwrap();
    let report = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!(
        (report.imported, report.unreadable, report.quarantined),
        (2, 1, 1),
        "{report:?}"
    );
    let again = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!((again.imported, again.unreadable), (0, 0), "run twice");
    let (state, current, message): (String, bool, String) = sqlx::query_as(
        "SELECT state,current,failure_message FROM integration_attempt WHERE task_ref='bad'",
    )
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    assert_eq!((state.as_str(), current), ("quarantined", false));
    assert!(message.contains("cannot be read"), "{message}");
    assert!(world
        .db
        .current_integration_attempt("good")
        .await
        .unwrap()
        .is_some());
    sqlx::query("DROP TRIGGER unreadable")
        .execute(world.db.pool())
        .await
        .unwrap();
    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until("bad", "both Tasks merge", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("bad").await.status == "done" && world.task("good").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

// ----- custom workflows (D) -----------------------------------------------

impl World {
    /// The Project gets a workflow of its own: the default one with its
    /// merge, repair and done states renamed, and one more hook in front of
    /// `run_merge` on the merge state's entry.
    async fn rename_merge_states(&self) {
        self.rename_states(false).await
    }
    /// The same, and with `every_role` the done, cancellation and review
    /// states renamed too (`shipped`, `dropped`, `inspection`).
    async fn rename_states(&self, every_role: bool) {
        let mut workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow("{}");
        let rename = |name: &mut String| match name.as_str() {
            "merging" => *name = "landing".into(),
            "merge_failed" => *name = "fixing".into(),
            "done" if every_role => *name = "shipped".into(),
            "cancelled" if every_role => *name = "dropped".into(),
            "review" if every_role => *name = "inspection".into(),
            _ => {}
        };
        for state in &mut workflow.states {
            rename(&mut state.name);
            for trigger in state.triggers.values_mut() {
                rename(&mut trigger.to);
            }
            if state.name == "landing" {
                let mut extra = state.hooks.on_enter[0].clone();
                extra.action = "notify_role_holder".into();
                state.hooks.on_enter.insert(0, extra);
            }
        }
        let definition = serde_json::to_string(&workflow).unwrap();
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(&definition)
            .execute(self.db.pool())
            .await
            .unwrap();
        let resolved = crate::workflow::engine::WorkflowEngine::resolve_workflow(&definition);
        assert!(
            super::super::reconcile::merge_state(&resolved, "landing")
                && !resolved.states.iter().any(|state| state.name == "merging"),
            "the Project's own workflow is in force"
        );
    }
}

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
    let (imported, _) = world.imported("t").await;
    // As the importer and the worker leave a merge whose old operation never
    // reported and whose checkouts could not be read then.
    sqlx::query("UPDATE integration_attempt SET state='quarantined',current_operation_state='uncertain',operation_id='old-merge',failure_kind='needs_fact',failure_message='integration result unknown',available_at=NULL,revision=revision+1 WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    let refused = db::RepoRepo::delete(&*world.db, "r").await;
    assert!(
        matches!(&refused, Err(db::DbError::IntegrationUnresolved { task_id, .. }) if task_id == "t"),
        "{refused:?}"
    );
    let production = Production::new(&world);
    let running = production.run();
    world
        .until("t", "the owner is told the result is unknown", || async {
            integration(&world.task("t").await)["kind"] == "deferred"
        })
        .await;
    assert_eq!(
        integration(&world.task("t").await)["cause"],
        "unresolved_result"
    );
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
    let world = World::new().await;
    world.rename_merge_states().await;
    world.legacy_task("t", "t.txt", "landing").await;
    world.seed_execution("t", "executor", None).await;
    // A Task of a default-workflow name in this Project is not a merge.
    world.legacy_task("other", "other.txt", "merging").await;
    world.touch("t").await;
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
    let production = Production::new(&world);
    merged_as(&world, "t", "done", &production).await;
    assert_eq!(world.merges("t").await, 1);
}

async fn merged_as(world: &World, task: &str, status: &str, production: &Production) {
    let running = production.run();
    world
        .until(task, &format!("{task} is {status}"), || async {
            world.task(task).await.status == status
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

// ----- a stopped rebase, and the paused-integration marker (E) -------------

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
    let world = World::new().await;
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

/// Manual approve authority in the merge state. The owner approves a Task
/// that is held in `merging`: where the Task offers `approve`, performing it
/// declares the Task done with nothing merged (the owner's override, as
/// before the queue), and the Task's attempt is settled: it ends terminal
/// and is not left parked or current behind a done Task. Where `approve` is
/// not offered the Task stays put with its attempt parked under the Hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn approving_a_held_merging_task_settles_its_attempt() {
    let world = World::new().await;
    let attempt = world.reconciled("t", "t.txt").await;
    world.act("t", TaskAction::Hold { reason: None }).await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the held Task left the slot", || async {
        world.attempt(&attempt.id).await.state == S::Parked
    })
    .await;
    let offers = world.offers("t").await;
    println!("held merging Task offers: {offers:?}");
    if offers.iter().any(|(verb, _)| verb == "approve") {
        let approved = world
            .act(
                "t",
                TaskAction::Approve {
                    reason: Some("landed by hand".into()),
                    override_checks: None,
                },
            )
            .await;
        assert_eq!(approved.status, "done", "the owner's override");
        assert_eq!(world.merges("t").await, 0, "nothing was merged");
        assert!(!world.on_main("t.txt").await);
        world
            .until("t", "the attempt is settled", || async {
                let settled = world.attempt(&attempt.id).await;
                settled.state.terminal() && !settled.current
            })
            .await;
        assert_eq!(world.attempt(&attempt.id).await.state, S::Cancelled);
    } else {
        assert_eq!(world.task("t").await.status, "merging");
        assert_eq!(world.attempt(&attempt.id).await.state, S::Parked);
    }
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
