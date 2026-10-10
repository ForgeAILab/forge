//! Every action against a real attempt row and the real step worker, with the
//! queue-worker side scripted (it only ever enqueues a step and moves its own
//! attempt row); one test wires the real `IntegrationQueueWorker`.
use super::*;
use crate::integration_worker::{IntegrationQueueWorker, IntegrationStepPort};
use db::{IntegrationActivationRepo, IntegrationAttempt};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

async fn git_out(path: &Path, args: &[&str]) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .await
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub(super) struct World {
    pub temp: tempfile::TempDir,
    pub db: Arc<SqliteDb>,
    pub bus: Arc<events::EventBus>,
    pub service: crate::TaskService,
    pub port: Arc<TaskStepIntegrationPort>,
    stop: tokio::sync::watch::Sender<bool>,
}
impl Drop for World {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
impl World {
    pub async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("steps.sqlite").display()
        );
        let pool = db::create_sqlite_pool(&url).await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let bus = Arc::new(events::EventBus::default());
        let merge = Arc::new(crate::MergeService::new_for_test(
            db.clone(),
            bus.clone(),
            temp.path().join("workspaces"),
        ));
        let service = crate::TaskService::new(db.clone(), bus.clone()).with_merge_service(merge);
        // The real step worker, as the server runs it.
        let (stop, stopped) = tokio::sync::watch::channel(false);
        service.task_step_worker().start(stopped);
        let world = Self {
            port: Arc::new(TaskStepIntegrationPort::new(db.clone())),
            temp,
            db,
            bus,
            service,
            stop,
        };
        let repo = world.repo();
        std::fs::create_dir(&repo).unwrap();
        git::init(&repo).await.unwrap();
        std::fs::write(repo.join("base"), "base\n").unwrap();
        git::commit_all(&repo, "initial").await.unwrap();
        git::checkout_branch(&repo, "main").await.unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','r',?,'main',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l-r','r','server',?,'primary_checkout',1,'ready',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
        world
    }
    pub fn repo(&self) -> PathBuf {
        self.temp.path().join("repo-r")
    }
    pub fn tree(&self, task: &str) -> PathBuf {
        self.temp.path().join(format!("tree-{task}"))
    }
    /// A Task in `merging` with one commit writing `file`, admitted to the
    /// `main` queue.
    pub async fn add_task(&self, task: &str, file: &str, content: &str) -> IntegrationAttempt {
        let (repo, tree) = (self.repo(), self.tree(task));
        let branch = format!("task-{task}");
        git_out(
            &repo,
            &["worktree", "add", "-b", &branch, tree.to_str().unwrap()],
        )
        .await
        .expect("worktree");
        std::fs::write(tree.join(file), content).unwrap();
        git::commit_all(&tree, task).await.unwrap();
        let head = git::get_current_sha(&tree).await.unwrap();
        let now = db::now_rfc3339();
        let pool = self.db.pool();
        sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,'merging',?,?,?)").bind(task).bind(task).bind(&now).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,?,'ready',?,?)").bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&branch).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l-r',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
        let queue = self
            .db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let mut attempt = IntegrationAttempt::new(
            Some(queue.id),
            task.into(),
            "p".into(),
            format!("admit-{task}"),
            "merging".into(),
            0,
            1,
        );
        attempt.original_candidate_sha = Some(head);
        self.db.admit_integration_attempt(attempt).await.unwrap()
    }
    pub async fn attempt(&self, id: &str) -> IntegrationAttempt {
        self.db.integration_attempt(id).await.unwrap().unwrap()
    }
    pub async fn task(&self, id: &str) -> db::Task {
        TaskRepo::get_by_id(&*self.db, id, false)
            .await
            .unwrap()
            .unwrap()
    }
    /// The scripted worker: put the attempt in `state` at an `effect_seq`,
    /// as the head would have, and return it.
    pub async fn place(&self, id: &str, state: &str, effect_seq: i64) -> IntegrationAttempt {
        let a = self.attempt(id).await;
        let head = git::get_current_sha(&self.tree(&a.task_ref)).await.unwrap();
        let tip = git_out(&self.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap();
        sqlx::query("UPDATE integration_attempt SET state=?,effect_seq=?,slot_generation=1,candidate_sha=?,target_tip_sha=?,revision=revision+1 WHERE id=?")
            .bind(state).bind(effect_seq).bind(head).bind(tip).bind(id)
            .execute(self.db.pool()).await.unwrap();
        self.attempt(id).await
    }
    pub async fn set(&self, id: &str, assignments: &str) {
        sqlx::query(&format!(
            "UPDATE integration_attempt SET {assignments},revision=revision+1 WHERE id=?"
        ))
        .bind(id)
        .execute(self.db.pool())
        .await
        .unwrap();
    }
    pub async fn ask(&self, attempt: &IntegrationAttempt, action: IntegrationStepAction) {
        self.port
            .enqueue_step(&IntegrationQueueWorker::step_request(attempt, action))
            .await
            .unwrap();
    }
    pub async fn step(&self, task: &str, action: IntegrationStepAction) -> Vec<TaskStep> {
        self.db
            .task_steps(task)
            .await
            .unwrap()
            .into_iter()
            .filter(|step| {
                step.kind == INTEGRATION_STEP_KIND
                    && step
                        .causation_key
                        .ends_with(&format!(":{}", action.as_str()))
            })
            .collect()
    }
    pub async fn scalar<T>(&self, sql: &str, bind: &str) -> T
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite> + Send + Unpin,
    {
        sqlx::query_scalar(sql)
            .bind(bind)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }
    pub async fn comments(&self, task: &str) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT content FROM task_comment WHERE task_id=? ORDER BY created_at,rowid",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap()
    }
    /// `(from, to, reason, bridge_kind, triggered_by, rejection)` rows.
    pub async fn transitions(
        &self,
        task: &str,
    ) -> Vec<(String, String, String, Option<String>, String, i64)> {
        sqlx::query_as("SELECT from_state,to_state,trigger_reason,bridge_kind,triggered_by,rejection FROM transition_log WHERE task_id=? ORDER BY created_at,rowid")
            .bind(task)
            .fetch_all(self.db.pool())
            .await
            .unwrap()
    }
}

/// Poll until `check` holds; the real step worker runs in the background.
pub(super) async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn settled(world: &World, task: &str, action: IntegrationStepAction, status: &str) {
    eventually(&format!("{} step {status}", action.as_str()), || async {
        world
            .step(task, action)
            .await
            .last()
            .is_some_and(|step| step.status == status)
    })
    .await;
}
fn ack(attempt: &IntegrationAttempt, action: IntegrationStepAction) -> Option<IntegrationStepAck> {
    IntegrationStepAck::current(attempt, action)
}
fn integration(task: &db::Task) -> Value {
    json!(task.condition.integration_reason())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_on_an_unchanged_target_writes_a_bound_permit_and_protects_the_result() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    // The step is enqueued before the attempt transition it belongs to.
    let asked = world.place(&a.id, "validating", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    eventually("the settle step retried", || async {
        world.step("t", IntegrationStepAction::Settle).await[0].attempts >= 2
    })
    .await;
    let waiting = world.attempt(&a.id).await;
    assert!(waiting.acknowledged_at.is_none() && waiting.permit_json.is_none());
    assert_eq!(waiting.revision, asked.revision, "a retry writes nothing");
    world.set(&a.id, "state='awaiting_task_step'").await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;

    let a = world.attempt(&a.id).await;
    let answer = ack(&a, IntegrationStepAction::Settle).expect("settle answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Permit);
    assert_eq!(
        (answer.effect_seq, answer.generation),
        (1, a.slot_generation)
    );
    let permit = a.permit_json.clone().unwrap();
    assert_eq!(permit["candidate_sha"], json!(a.candidate_sha));
    assert_eq!(permit["target_tip_sha"], json!(a.target_tip_sha));
    assert_eq!(permit["task_ref"], "t");
    assert_eq!(permit["expected_epoch"], a.expected_epoch);
    assert_eq!(permit["slot_generation"], a.slot_generation);
    // Storage accepts exactly this permit for `ready_ff`.
    let mut ready = a.clone();
    ready.state = S::ReadyFf;
    let ready = world
        .db
        .transition_integration_attempt(ready)
        .await
        .unwrap();

    // The protected result step exists from the permit's own commit.
    let result = world.step("t", IntegrationStepAction::Result).await;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].status, "pending");
    assert!(!result[0].entry_fenced);
    assert!(result[0].available_at > db::now_rfc3339());
    let protected: bool = world
        .scalar(
            "SELECT integration_started_at IS NOT NULL FROM task_step WHERE id=?",
            &result[0].id,
        )
        .await;
    assert!(protected);
    assert_eq!(
        integration(&world.task("t").await),
        json!({"kind":"owned","attempt_id":a.id,"phase":"fast_forwarding"})
    );
    // The Task itself did not move.
    assert_eq!(world.task("t").await.status, "merging");

    // Duplicate delivery of the same request: no second step, one answer.
    world.ask(&ready, IntegrationStepAction::Settle).await;
    assert_eq!(
        world.step("t", IntegrationStepAction::Settle).await.len(),
        1
    );
    assert_eq!(world.attempt(&a.id).await.revision, ready.revision);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_for_a_past_effect_seq_or_generation_finishes_without_a_write() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 4).await;
    // The worker moved on (a takeover asks again under a new effect_seq).
    let mut old_seq = IntegrationQueueWorker::step_request(&asked, IntegrationStepAction::Settle);
    old_seq.effect_seq = 3;
    world.port.enqueue_step(&old_seq).await.unwrap();
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    let mut old_generation =
        IntegrationQueueWorker::step_request(&asked, IntegrationStepAction::SendBack);
    old_generation.generation = 0;
    world.set(&a.id, "slot_generation=2").await;
    let mut settle = old_generation.clone();
    settle.action = IntegrationStepAction::Settle;
    settle.effect_seq = 4;
    world.port.enqueue_step(&settle).await.unwrap();
    eventually("both stale settles finished", || async {
        world
            .step("t", IntegrationStepAction::Settle)
            .await
            .iter()
            .filter(|step| step.status == "done")
            .count()
            == 2
    })
    .await;
    let after = world.attempt(&a.id).await;
    assert!(after.acknowledged_at.is_none() && after.permit_json.is_none());
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());
    assert!(world
        .task("t")
        .await
        .condition
        .integration_reason()
        .is_none());
}

/// Settle with a permit, then let the scripted worker record the
/// fast-forward and ready the result step.
async fn land(world: &World, task: &str, attempt_id: &str) -> IntegrationAttempt {
    let asked = world.place(attempt_id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(world, task, IntegrationStepAction::Settle, "done").await;
    world
        .set(attempt_id, "state='applied',integrated_sha=candidate_sha")
        .await;
    world.attempt(attempt_id).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_applies_a_landed_merge_exactly_as_the_merge_hook_does() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let mut events = world.bus.subscribe();
    let applied = land(&world, "t", &a.id).await;
    // The worker asks for the result it already has a protected step for.
    world.ask(&applied, IntegrationStepAction::Result).await;
    assert_eq!(
        world.step("t", IntegrationStepAction::Result).await.len(),
        1
    );
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the Task is done", || async {
        world.task("t").await.status == "done"
    })
    .await;

    let sha = applied.candidate_sha.clone().unwrap();
    // Today's merge success: this comment, then `merging -> done` by the
    // workflow with this reason and no bridge.
    assert_eq!(
        world.comments("t").await,
        vec![format!("Changes merged to main (SHA: {sha})")]
    );
    assert_eq!(
        world.transitions("t").await,
        vec![(
            "merging".to_owned(),
            "done".to_owned(),
            "merge succeeded".to_owned(),
            None,
            "system:workflow".to_owned(),
            0
        )]
    );
    let a = world.attempt(&a.id).await;
    let answer = ack(&a, IntegrationStepAction::Result).expect("result answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Done);
    let result = &world.step("t", IntegrationStepAction::Result).await[0];
    assert_eq!(result.status, "done");
    // The landed result outranks a waiting owner command, as the merge
    // hook's own follow-up does.
    let priority: i64 = world
        .scalar(
            "SELECT priority FROM task_step WHERE causation_step_id=? AND kind='cascade'",
            &result.id,
        )
        .await;
    assert_eq!(priority, 2);
    let mut seen = Vec::new();
    while let Ok(event) = events.try_recv() {
        seen.push(event.event_type);
    }
    assert!(seen.contains(&"comment.created".to_owned()), "{seen:?}");
    // A redelivered request changes nothing.
    world.ask(&a, IntegrationStepAction::Result).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(world.comments("t").await.len(), 1);
    assert_eq!(world.attempt(&a.id).await.revision, a.revision);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_that_cannot_apply_never_dead_letters_and_is_rearmed() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    test_faults::set("t", true);
    land(&world, "t", &a.id).await;
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    // More deliveries than any step's retry allowance.
    for round in 1..=10 {
        eventually("the result step ran again", || async {
            world.step("t", IntegrationStepAction::Result).await[0].attempts >= round
        })
        .await;
        let step = &world.step("t", IntegrationStepAction::Result).await[0];
        assert_ne!(step.status, "failed");
        assert_ne!(step.status, "parked");
        // Parked far out; `ready_result_step` is what re-arms it.
        eventually("the result step is parked for later", || async {
            let step = &world.step("t", IntegrationStepAction::Result).await[0];
            step.status == "pending" && step.available_at > db::now_rfc3339()
        })
        .await;
        world.port.ready_result_step(&a.id, 1).await.unwrap();
    }
    let task = world.task("t").await;
    assert_eq!(task.status, "merging");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "unresolved_result");
    assert!(task.error_annotation.is_none(), "no failure annotation");
    assert!(
        world.attempt(&a.id).await.acknowledged_at.is_some(),
        "settle ack stays"
    );
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_none());

    test_faults::set("t", false);
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the Task is done", || async {
        world.task("t").await.status == "done"
    })
    .await;
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unused_permit_is_taken_back_when_the_protected_step_wakes() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    assert!(world.attempt(&a.id).await.permit_json.is_some());
    // No worker used the permit (it died): the step's own deadline passes.
    let result = world.step("t", IntegrationStepAction::Result).await;
    sqlx::query("UPDATE task_step SET available_at=? WHERE id=?")
        .bind(db::now_rfc3339())
        .bind(&result[0].id)
        .execute(world.db.pool())
        .await
        .unwrap();
    settled(&world, "t", IntegrationStepAction::Result, "done").await;
    let a = world.attempt(&a.id).await;
    assert!(a.permit_json.is_none() && a.acknowledged_at.is_none());
    assert_eq!(world.task("t").await.status, "merging");
    assert!(world.comments("t").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_hands_a_conflict_to_the_worker_as_the_merge_hook_does() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let mut events = world.bus.subscribe();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\",\"dir/g\"]',failure_message='conflict in f'",
        )
        .await;
    eventually("the Task was handed back", || async {
        world.task("t").await.status == "merge_failed"
    })
    .await;
    let details = "rebased onto main; conflicts were committed with markers in: f, dir/g";
    assert_eq!(
        world.comments("t").await,
        vec![format!(
            "Merge conflict handed back to the Worker: {details}"
        )]
    );
    let transitions = world.transitions("t").await;
    assert_eq!(
        transitions[0],
        (
            "merging".to_owned(),
            "merge_failed".to_owned(),
            details.to_owned(),
            Some("conflict_handoff".to_owned()),
            "system:workflow".to_owned(),
            0
        )
    );
    let payload: String = world
        .scalar(
            "SELECT bridge_payload FROM transition_log WHERE task_id=? AND bridge_kind='conflict_handoff'",
            "t",
        )
        .await;
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap()["paths"],
        json!(["f", "dir/g"])
    );
    let task = world.task("t").await;
    assert!(task.review_passed_at.is_none(), "the approval is void");
    let annotation: Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "merge_conflict");
    let mut failed = Vec::new();
    while let Ok(event) = events.try_recv() {
        if event.event_type == "merge.failed" {
            failed.push(event);
        }
    }
    assert_eq!(failed.len(), 1);
    let charged: i64 = world
        .scalar(
            "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind='conflict_handoff'),0)",
            "t",
        )
        .await;
    assert_eq!(charged, 1);
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::SendBack).is_some());
    assert_eq!(
        world.step("t", IntegrationStepAction::SendBack).await[0].status,
        "done"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_routes_a_red_check_and_a_lost_review_through_merge_failed() {
    let world = World::new().await;
    // A red check on the rebased commit: the coder's, under the merge-fix
    // budget, as any failed integration.
    let red = world.add_task("red", "f", "one\n").await;
    let asked = world.place(&red.id, "checking", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &red.id,
            "state='ejected',failure_kind='candidate_check_failed',failure_message='`make test` exited 2: boom'",
        )
        .await;
    eventually("the red Task went to merge_failed", || async {
        world.task("red").await.status == "merge_failed"
    })
    .await;
    let transitions = world.transitions("red").await;
    assert_eq!(
        (
            transitions[0].1.as_str(),
            &transitions[0].3,
            transitions[0].5
        ),
        ("merge_failed", &None, 1),
        "a plain rejection out of the merge gate"
    );
    assert!(transitions[0]
        .2
        .contains("checks failed after the rebase onto main"));
    let annotation: Value = serde_json::from_str(
        world
            .task("red")
            .await
            .error_annotation
            .as_deref()
            .unwrap_or("{}"),
    )
    .unwrap_or_default();
    let comments = world.comments("red").await;
    assert!(
        comments[0].starts_with("Merge checks failed:"),
        "{comments:?} {annotation}"
    );

    // Review authority lost: today's review refresh.
    let stale = world.add_task("stale", "g", "two\n").await;
    let asked = world.place(&stale.id, "awaiting_task_step", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &stale.id,
            "state='needs_review',failure_message='review acceptance is stale; fresh review required'",
        )
        .await;
    eventually("the stale Task left merging", || async {
        !world.transitions("stale").await.is_empty()
    })
    .await;
    let first = world.transitions("stale").await[0].clone();
    assert_eq!(
        (
            first.0.as_str(),
            first.1.as_str(),
            first.3.as_deref(),
            first.5
        ),
        ("merging", "merge_failed", Some("review_refresh"), 0)
    );
    assert_eq!(
        first.2,
        "conformance review required: review acceptance is stale; fresh review required"
    );
    eventually("review authority was cleared", || async {
        world.task("stale").await.review_passed_at.is_none()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_states_the_wait_once_and_waiting_is_restored_when_the_queue_reopens() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let interruptions = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id='t'",
        )
        .fetch_one(world.db.pool())
        .await
        .unwrap()
    };
    let before = interruptions().await;
    let asked = world.place(&a.id, "validating", 2).await;
    world.ask(&asked, IntegrationStepAction::Park).await;
    world
        .set(
            &a.id,
            "state='parked',failure_kind='owner_required',failure_message='target_dirty: notes.txt'",
        )
        .await;
    settled(&world, "t", IntegrationStepAction::Park, "done").await;
    let task = world.task("t").await;
    assert_eq!(
        integration(&task),
        json!({"kind":"deferred","attempt_id":a.id,"cause":"target_dirty","owner_id":null,"message":"target_dirty: notes.txt"})
    );
    assert_eq!(task.status, "merging");
    assert_eq!(interruptions().await, before + 1, "one incident per wait");
    // The same request again is the same step: nothing more is written.
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    assert_eq!(world.step("t", IntegrationStepAction::Park).await.len(), 1);
    assert_eq!(interruptions().await, before + 1);

    // A wait that ends on its own raises no incident.
    world
        .set(
            &a.id,
            "effect_seq=3,failure_kind='infrastructure',failure_message='infrastructure: owner busy'",
        )
        .await;
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("the infrastructure wait was stated", || async {
        integration(&world.task("t").await)["cause"] == "infrastructure"
    })
    .await;
    assert_eq!(interruptions().await, before + 1);

    // A member of a suspended queue reads the queue's reason ...
    let queue = a.queue_id.clone().unwrap();
    world
        .set(
            &a.id,
            "state='queued',effect_seq=4,failure_kind=NULL,failure_message=NULL",
        )
        .await;
    sqlx::query("UPDATE integration_queue SET state='suspended',last_error_kind='target_unavailable',last_error='the default checkout is offline' WHERE id=?")
        .bind(&queue).execute(world.db.pool()).await.unwrap();
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("the suspended queue was stated", || async {
        integration(&world.task("t").await)["cause"] == "owner_offline"
    })
    .await;
    // ... and once the queue is open again the same request restores `waiting`.
    sqlx::query(
        "UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL WHERE id=?",
    )
    .bind(&queue)
    .execute(world.db.pool())
    .await
    .unwrap();
    world.set(&a.id, "effect_seq=5").await;
    world
        .ask(&world.attempt(&a.id).await, IntegrationStepAction::Park)
        .await;
    eventually("waiting was restored", || async {
        integration(&world.task("t").await) == json!({"kind":"waiting","attempt_id":a.id})
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_cancel_flags_the_attempt_in_its_own_transaction_and_clear_drops_the_statement() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    // A queued member that was told something, then cancelled by its owner.
    let asked = world.place(&a.id, "parked", 1).await;
    world
        .set(
            &a.id,
            "failure_kind='owner_required',failure_message='transfer_too_large: 300 MiB'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::Park).await;
    settled(&world, "t", IntegrationStepAction::Park, "done").await;
    assert_eq!(integration(&world.task("t").await)["kind"], "deferred");
    // A settle the worker asked for that has not run yet (held back so the
    // Cancel finds it pending).
    let pending = world.place(&a.id, "awaiting_task_step", 2).await;
    let request = IntegrationQueueWorker::step_request(&pending, IntegrationStepAction::Settle);
    let held = TaskStepIntegrationPort::step_input(
        &request,
        "merging",
        world.task("t").await.version,
        None,
        "2099-01-01T00:00:00Z".into(),
    )
    .unwrap();
    world.db.enqueue_step(&held).await.unwrap();

    let cancelled = world.service.cancel_task("t").await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    let flagged = world.attempt(&a.id).await;
    assert!(
        flagged.cancel_requested_at.is_some(),
        "flagged with the Task write"
    );
    assert!(flagged.permit_json.is_none());
    // The unprotected step was dropped at once, not run to a refusal.
    let settle = &world.step("t", IntegrationStepAction::Settle).await[0];
    assert_eq!(settle.status, "superseded", "{:?}", settle.last_error);
    assert!(world
        .step("t", IntegrationStepAction::Result)
        .await
        .is_empty());

    // The worker releases the attempt and asks for the clear. (Until the
    // cutover the stage B observation also ends an attempt that is not a
    // queue head when its Task settles: either way it is cancelled.)
    world.ask(&flagged, IntegrationStepAction::Clear).await;
    if world.attempt(&a.id).await.state != S::Cancelled {
        let mut a = world.attempt(&a.id).await;
        a.state = S::Cancelled;
        a.current = false;
        a.completed_at = Some(db::now_rfc3339());
        world.db.transition_integration_attempt(a).await.unwrap();
    }
    settled(&world, "t", IntegrationStepAction::Clear, "done").await;
    let task = world.task("t").await;
    assert_eq!(task.status, "cancelled");
    assert!(task.condition.integration_wait().is_none());
    assert!(!world.step("t", IntegrationStepAction::Clear).await[0].entry_fenced);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_waits_behind_the_protected_result_step_and_the_merge_wins() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(&world, "t", IntegrationStepAction::Settle, "done").await;
    // The worker committed the fast-forward: storage refuses a cancel flag.
    world.set(&a.id, "state='ff_inflight'").await;
    let flagged = world
        .db
        .request_integration_cancel(
            &a.id,
            world.attempt(&a.id).await.revision,
            &db::now_rfc3339(),
        )
        .await;
    assert!(matches!(flagged, Err(db::DbError::InvalidTransition)));

    let service = world.service.clone();
    let cancel = tokio::spawn(async move { service.cancel_task("t").await });
    // The Cancel is queued behind the protected step and cannot be claimed.
    eventually("the cancel command is queued", || async {
        world
            .db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .any(|step| step.kind == "command" && step.status == "pending")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(world.task("t").await.status, "merging");
    assert!(!cancel.is_finished());
    assert!(world.attempt(&a.id).await.cancel_requested_at.is_none());

    // The fast-forward lands; the result runs first.
    world
        .set(&a.id, "state='applied',integrated_sha=candidate_sha")
        .await;
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the merge was applied", || async {
        !world.transitions("t").await.is_empty()
    })
    .await;
    // The result ran first: the Task's first move is the merge, and the
    // attempt was answered without ever carrying a cancel flag.
    let first = world.transitions("t").await[0].clone();
    assert_eq!(
        (first.0.as_str(), first.1.as_str(), first.2.as_str()),
        ("merging", "done", "merge succeeded")
    );
    let _ = tokio::time::timeout(Duration::from_secs(20), cancel).await;
    let a = world.attempt(&a.id).await;
    assert!(a.cancel_requested_at.is_none());
    let answer: IntegrationStepAck = serde_json::from_value(a.effect_ack_json.unwrap()).unwrap();
    assert_eq!(
        (answer.action, answer.outcome),
        (IntegrationStepAction::Result, IntegrationStepOutcome::Done)
    );
    assert_eq!(
        world.comments("t").await[0],
        format!("Changes merged to main (SHA: {})", a.candidate_sha.unwrap())
    );
}

/// One round of the race: a `settle` step and an owner Cancel start together.
/// Returns whether the permit won.
async fn race_once(world: &World, task: &'static str) -> bool {
    let a = world.add_task(task, task, "one\n").await;
    let asked = world.place(&a.id, "awaiting_task_step", 1).await;
    let service = world.service.clone();
    let port = world.port.clone();
    let request = IntegrationQueueWorker::step_request(&asked, IntegrationStepAction::Settle);
    let (cancel, _) = tokio::join!(
        tokio::spawn(async move { service.cancel_task(task).await }),
        tokio::spawn(async move { port.enqueue_step(&request).await })
    );
    let _ = cancel;
    eventually("the race settled", || async {
        let a = world.attempt(&a.id).await;
        a.permit_json.is_some() || world.task(task).await.status == "cancelled"
    })
    .await;
    let after = world.attempt(&a.id).await;
    let status = world.task(task).await.status;
    let permit = after.permit_json.is_some();
    // Exactly one of the two: a permit on a Task still merging, with its
    // protected step and no cancel flag; or a cancelled Task whose attempt
    // carries the flag and no permit.
    if permit {
        assert_eq!(status, "merging");
        assert!(after.cancel_requested_at.is_none());
        assert_eq!(
            world.step(task, IntegrationStepAction::Result).await.len(),
            1
        );
        // Finish the merge so the waiting Cancel (if any) can settle.
        world
            .set(&a.id, "state='applied',integrated_sha=candidate_sha")
            .await;
        world.port.ready_result_step(&a.id, 1).await.unwrap();
        eventually("the permitted Task is done", || async {
            world.task(task).await.status == "done"
        })
        .await;
    } else {
        assert_eq!(status, "cancelled");
        assert!(after.cancel_requested_at.is_some());
        assert!(world
            .step(task, IntegrationStepAction::Result)
            .await
            .is_empty());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(world.attempt(&a.id).await.permit_json.is_none());
    }
    permit
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_racing_the_permit_has_exactly_one_winner_under_real_concurrency() {
    let world = World::new().await;
    let mut permits = 0;
    for task in ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"] {
        permits += usize::from(race_once(&world, task).await);
    }
    // Either order is legal; each round asserted exactly one winner.
    println!("permit won {permits} of 8 rounds");
}

fn application(
    attempt: &IntegrationAttempt,
    outcome: db::CheckResultOutcome,
    exit_code: i32,
) -> crate::check_runner::consumer::CheckApplication {
    use api_types::{CheckDigestInput, CheckEnvironmentIdentity, CheckExecutionRevision};
    let at = |seconds: i64| {
        (chrono::DateTime::parse_from_rfc3339("2026-10-10T00:00:00Z").unwrap()
            + chrono::Duration::seconds(seconds))
        .to_rfc3339()
    };
    let result = db::StoredCheckResult {
        id: "result".into(),
        run_id: "run".into(),
        identity_key: "identity".into(),
        outcome,
        cleanup: db::CheckCleanup::NotPerformed,
        certified: false,
        cacheable: false,
        commands: vec![api_types::CheckCommandOutcome {
            index: 0,
            command: "make test".into(),
            exit_code,
            stderr_tail: if exit_code == 0 { "" } else { "boom" }.into(),
            output_tail: String::new(),
            started_at: at(3),
            finished_at: at(10),
        }],
        output_truncated: false,
        created_at: at(10),
    };
    crate::check_runner::consumer::CheckApplication {
        task_id: attempt.task_ref.clone(),
        status_epoch: attempt.expected_epoch,
        authority: settle::check_authority(attempt).unwrap(),
        consumer_id: "consumer".into(),
        run: db::StoredCheckRun {
            id: "run".into(),
            identity: db::CheckRunIdentity {
                project_id: "p".into(),
                repo_id: "r".into(),
                commit_sha: attempt.candidate_sha.clone().unwrap(),
                inputs: CheckDigestInput {
                    spec: check_executor::legacy_ci_spec(
                        "make test",
                        &Default::default(),
                        0,
                        false,
                    ),
                    environment: Default::default(),
                    environment_identity: CheckEnvironmentIdentity::NotAttested,
                    execution_revision: CheckExecutionRevision {
                        number: 0,
                        audit_ref: None,
                    },
                },
            },
            identity_key: "identity".into(),
            spec_digest: "digest".into(),
            cacheable: false,
            state: db::CheckRunState::Succeeded,
            operation_id: "operation".into(),
            workspace_id: None,
            machine_id: None,
            applied_timeout_seconds: Some(1800),
            lease_owner: None,
            lease_generation: 1,
            lease_until: None,
            version: 1,
            created_at: at(0),
            updated_at: at(10),
            finished_at: Some(at(10)),
        },
        verdict: if outcome == db::CheckResultOutcome::InfrastructureFailed {
            crate::check_runner::consumer::CheckVerdict::InfrastructureExhausted(result)
        } else {
            crate::check_runner::consumer::CheckVerdict::Result(result)
        },
    }
}

/// The merge-path consumer family, applied as the check's own delivery step
/// applies it (inside a claimed step of the Task).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_check_verdict_is_the_settle_answer_with_timing_for_the_waiting_worker() {
    use crate::check_runner::consumer::CheckConsumerFamily;
    let world = World::new().await;
    let family =
        IntegrationCheckFamily::new(Arc::new(IntegrationSteps::new(world.service.clone())));
    for (task, outcome, exit_code, expected) in [
        (
            "pass",
            db::CheckResultOutcome::Pass,
            0,
            IntegrationStepOutcome::Permit,
        ),
        (
            "red",
            db::CheckResultOutcome::Fail,
            2,
            IntegrationStepOutcome::CandidateCheckFailed,
        ),
        (
            "late",
            db::CheckResultOutcome::TimedOut,
            124,
            IntegrationStepOutcome::CandidateCheckFailed,
        ),
        (
            "lost",
            db::CheckResultOutcome::InfrastructureFailed,
            1,
            IntegrationStepOutcome::Infrastructure,
        ),
    ] {
        let a = world.add_task(task, task, "one\n").await;
        // The worker asked under effect_seq 2 and, after a takeover, waits
        // under effect_seq 3: the answer is for whoever waits now.
        let checking = world.place(&a.id, "checking", 3).await;
        assert_eq!(
            family.current_authority(task, 0).await.unwrap(),
            settle::check_authority(&checking)
        );
        assert_eq!(
            family.current_authority(task, 1).await.unwrap(),
            None,
            "another entry"
        );
        let delivery = application(&checking, outcome, exit_code);
        let lease = crate::test_support::TestTaskLease::claim(&world.db, task).await;
        lease.run(family.apply(&delivery)).await.unwrap();
        // Redelivery after a crash: the same answer, written once.
        let answered = world.attempt(&a.id).await;
        lease.run(family.apply(&delivery)).await.unwrap();
        assert_eq!(world.attempt(&a.id).await.revision, answered.revision);
        lease.release().await;

        let answer = ack(&answered, IntegrationStepAction::Settle).expect("settle answered");
        assert_eq!(answer.outcome, expected, "{task}");
        assert_eq!(
            (answer.effect_seq, answer.generation),
            (3, checking.slot_generation)
        );
        let permitted = expected == IntegrationStepOutcome::Permit;
        assert_eq!(answered.permit_json.is_some(), permitted, "{task}");
        assert_eq!(
            world.step(task, IntegrationStepAction::Result).await.len(),
            usize::from(permitted),
            "{task}"
        );
        match expected {
            IntegrationStepOutcome::Infrastructure => assert!(answer.check.is_none()),
            _ => assert_eq!(
                answer.check,
                Some(db::IntegrationCheckTiming::Ran {
                    slot_wait_ms: 3000,
                    run_ms: 7000
                })
            ),
        }
        if expected == IntegrationStepOutcome::CandidateCheckFailed {
            assert!(answer.message.unwrap().contains("`make test` exited"));
        }
        assert_eq!(world.task(task).await.status, "merging");
    }
    // An attempt that is no longer checking has no authority: nothing applies.
    let a = world.add_task("gone", "gone", "one\n").await;
    let checking = world.place(&a.id, "checking", 1).await;
    let delivery = application(&checking, db::CheckResultOutcome::Pass, 0);
    world.set(&a.id, "state='parked'").await;
    assert_eq!(family.current_authority("gone", 0).await.unwrap(), None);
    let lease = crate::test_support::TestTaskLease::claim(&world.db, "gone").await;
    lease.run(family.apply(&delivery)).await.unwrap();
    lease.release().await;
    assert!(world.attempt(&a.id).await.acknowledged_at.is_none());
}

#[test]
fn the_shared_carry_rule_bounds_carries_and_keeps_the_reviewed_change_set() {
    use crate::workflow::actions::carry::{
        carry_budget_refusal, carry_path_refusal, gate_refuses_carry,
    };
    let limit = i64::from(db::budget::Kind::ReviewCarry.default_limit());
    assert!(carry_budget_refusal(limit - 1).is_none());
    assert!(carry_budget_refusal(limit)
        .unwrap()
        .contains("integrations were already carried under this review"));
    let reviewed = vec!["src/a.rs".to_owned(), "src/b.rs".to_owned()];
    assert!(carry_path_refusal(&["src/a.rs".to_owned()], &reviewed).is_none());
    assert_eq!(
        carry_path_refusal(&["src/a.rs".to_owned(), "src/c.rs".to_owned()], &reviewed).unwrap(),
        "`src/c.rs` is outside the reviewed change set"
    );
    assert!(gate_refuses_carry(None).is_none());
}

// ----- the real queue worker over this consumer --------------------------

struct GitFacts {
    db: Arc<SqliteDb>,
}
#[async_trait::async_trait]
impl crate::integration_worker::IntegrationFactsPort for GitFacts {
    async fn head_facts(
        &self,
        attempt: &IntegrationAttempt,
        queue: &db::IntegrationQueue,
    ) -> Result<crate::integration_worker::HeadFacts> {
        use crate::integration_effects::{EffectOwner, EffectWorkspace};
        use crate::integration_worker::{HeadFacts, ObjectTransferEndpoint, TaskGate};
        let unreadable = || ServiceError::invalid_operation("facts unreadable");
        let (workspace_id, tree, branch, placement_id, generation): (String, String, String, String, i64) =
            sqlx::query_as("SELECT w.id,w.worktree_path,w.branch,p.id,p.generation FROM workspace w JOIN workspace_placement p ON p.workspace_id=w.id WHERE w.task_id=?")
                .bind(&attempt.task_ref)
                .fetch_one(self.db.pool())
                .await?;
        let repo: String = sqlx::query_scalar("SELECT path FROM repo_location WHERE id=?")
            .bind(&queue.target_location_id)
            .fetch_one(self.db.pool())
            .await?;
        let live: bool = sqlx::query_scalar(db::STEP_FENCE)
            .bind(&attempt.task_ref)
            .bind(&attempt.expected_status)
            .bind(attempt.expected_epoch)
            .fetch_one(self.db.pool())
            .await?;
        let (tree, repo) = (PathBuf::from(tree), PathBuf::from(repo));
        let candidate_head = git_out(&tree, &["rev-parse", "HEAD"])
            .await
            .ok_or_else(unreadable)?;
        let target_tip = git_out(
            &repo,
            &["rev-parse", &format!("refs/heads/{}", queue.target_branch)],
        )
        .await
        .ok_or_else(unreadable)?;
        let target_in_candidate = git_out(
            &repo,
            &["merge-base", "--is-ancestor", &target_tip, &candidate_head],
        )
        .await
        .is_some();
        let candidate_in_target = git_out(
            &repo,
            &["merge-base", "--is-ancestor", &candidate_head, &target_tip],
        )
        .await
        .is_some();
        Ok(HeadFacts {
            gate: if live { TaskGate::Live } else { TaskGate::Left },
            task_location: ObjectTransferEndpoint {
                repo_location_id: "l-r".into(),
                owner: EffectOwner::Server,
            },
            target_in_candidate,
            candidate_in_target,
            worktree_dirty: !git::is_worktree_clean(&tree).await?,
            target_dirty: !git::is_worktree_clean(&repo).await?,
            rebase_in_progress: git::detect_rebase_in_progress(&tree).await?,
            shared_object_store: true,
            workspace: EffectWorkspace {
                workspace_id,
                placement_id,
                generation,
                owner: EffectOwner::Server,
                handle: tree.to_string_lossy().into_owned(),
            },
            task_branch: branch,
            candidate_head,
            target_tip,
        })
    }
}
struct NoTransfer;
#[async_trait::async_trait]
impl crate::integration_worker::ObjectTransferPort for NoTransfer {
    async fn transfer(
        &self,
        _request: crate::integration_worker::ObjectTransferRequest,
    ) -> Result<crate::integration_worker::ObjectTransferOutcome> {
        Ok(crate::integration_worker::ObjectTransferOutcome::Transferred { bytes: 0 })
    }
    async fn release(
        &self,
        _release: crate::integration_worker::ObjectTransferRelease,
    ) -> Result<()> {
        Ok(())
    }
}

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
        assert!(world
            .db
            .task_steps(task)
            .await
            .unwrap()
            .iter()
            .all(|step| matches!(step.status.as_str(), "done" | "superseded")));
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
