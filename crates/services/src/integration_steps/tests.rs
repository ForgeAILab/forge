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
    step_worker: Option<tokio::task::JoinHandle<()>>,
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
            temp.path().join("workspaces"),
        ));
        let service = crate::TaskService::new(db.clone(), bus.clone()).with_merge_service(merge);
        // The real step worker, as the server runs it.
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let step_worker = service.task_step_worker().start(stopped);
        let world = Self {
            port: Arc::new(TaskStepIntegrationPort::new(db.clone())),
            temp,
            db,
            bus,
            service,
            stop,
            step_worker: Some(step_worker),
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
        attempt.execution_id = Some(self.seed_execution(task, "executor", None).await);
        attempt.execution_ref = attempt.execution_id.clone();
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
    /// The scripted worker's fast-forward: the target really takes the
    /// attempt's candidate, and the attempt records it as `applied`.
    pub async fn apply(&self, id: &str) {
        let sha = self.attempt(id).await.candidate_sha.expect("candidate");
        let repo = self.repo();
        if git_out(
            &repo,
            &[
                "-c",
                "user.name=forge",
                "-c",
                "user.email=forge@example.invalid",
                "merge",
                "--no-edit",
                &sha,
            ],
        )
        .await
        .is_none()
        {
            let _ = git_out(&repo, &["merge", "--abort"]).await;
            git_out(&repo, &["update-ref", "refs/heads/main", &sha])
                .await
                .expect("target moves to the candidate");
        }
        self.set(id, "state='applied',integrated_sha=candidate_sha")
            .await;
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
    /// What a Task that did not move is waiting on: its row, its attempts
    /// and every step with its wake time and last error.
    pub async fn stuck(&self, task: &str) -> String {
        let now = db::now_rfc3339();
        let writers = db::open_write_transactions();
        let row: (String, i64, Option<String>) =
            sqlx::query_as("SELECT status,version,condition_json FROM task WHERE id=?")
                .bind(task)
                .fetch_one(self.db.pool())
                .await
                .unwrap();
        let attempts: Vec<StuckAttempt> =
            sqlx::query_as("SELECT state,effect_seq,permit_json,effect_ack_json,cancel_requested_at FROM integration_attempt WHERE task_ref=? ORDER BY rowid")
                .bind(task)
                .fetch_all(self.db.pool())
                .await
                .unwrap();
        let steps: Vec<StuckStep> =
            sqlx::query_as("SELECT seq,kind,causation_key,status,attempts,available_at,last_error,payload_json FROM task_step WHERE task_id=? ORDER BY seq")
                .bind(task)
                .fetch_all(self.db.pool())
                .await
                .unwrap();
        let mut out = format!("now {now}\nwrite transactions {writers:#?}\ntask {row:?}\n");
        for attempt in attempts {
            out.push_str(&format!("attempt {attempt:?}\n"));
        }
        for step in steps {
            out.push_str(&format!("step {step:?}\n"));
        }
        for step in self.db.task_steps(task).await.unwrap() {
            out.push_str(&format!(
                "ownership {} active={} owner={:?} lease={:?} preempt={:?}\n",
                step.id,
                self.db.step_is_active(&step),
                step.claimed_by,
                step.lease_until,
                crate::worker_runtime::queue::preemption_diagnostic(&step.id),
            ));
        }
        out
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

/// `state, effect_seq, permit, ack, cancel_requested_at` of an attempt.
type StuckAttempt = (String, i64, Option<String>, Option<String>, Option<String>);
/// `seq, kind, causation_key, status, attempts, available_at, last_error,
/// payload` of a Task step.
type StuckStep = (
    i64,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
);

/// Poll until `check` holds; the real step worker runs in the background.
pub(super) async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // Generous: the focused suite runs hundreds of tests beside this one.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
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
    world.apply(attempt_id).await;
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
    // The fault is keyed by Task id in a process-wide set: an id no other
    // test uses (it made the plain `result` test time out under `"t"`).
    let world = World::new().await;
    let a = world.add_task("unapplied", "f", "one\n").await;
    test_faults::set("unapplied", true);
    land(&world, "unapplied", &a.id).await;
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    // More deliveries than any step's retry allowance.
    for round in 1..=10 {
        eventually("the result step ran again", || async {
            world.step("unapplied", IntegrationStepAction::Result).await[0].attempts >= round
        })
        .await;
        let step = &world.step("unapplied", IntegrationStepAction::Result).await[0];
        assert_ne!(step.status, "failed");
        assert_ne!(step.status, "parked");
        // Parked far out; `ready_result_step` is what re-arms it.
        eventually("the result step is parked for later", || async {
            let step = &world.step("unapplied", IntegrationStepAction::Result).await[0];
            step.status == "pending" && step.available_at > db::now_rfc3339()
        })
        .await;
        world.port.ready_result_step(&a.id, 1).await.unwrap();
    }
    let task = world.task("unapplied").await;
    assert_eq!(task.status, "merging");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "unresolved_result");
    assert!(task.error_annotation.is_none(), "no failure annotation");
    assert!(
        world.attempt(&a.id).await.acknowledged_at.is_some(),
        "settle ack stays"
    );
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_none());

    test_faults::set("unapplied", false);
    world.port.ready_result_step(&a.id, 1).await.unwrap();
    eventually("the Task is done", || async {
        world.task("unapplied").await.status == "done"
    })
    .await;
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_some());
}

/// One `result` delivery by the real step worker at the attempt's present
/// `effect_seq`: returns once the step answered or parked itself to ask
/// again (`true`: answered).
async fn deliver_result(world: &World, attempt: &IntegrationAttempt) -> bool {
    world.ask(attempt, IntegrationStepAction::Result).await;
    world
        .port
        .ready_result_step(&attempt.id, attempt.effect_seq)
        .await
        .unwrap();
    let key = IntegrationQueueWorker::step_request(attempt, IntegrationStepAction::Result)
        .causation_key();
    let key = &key;
    eventually(
        "the result step answered or asks again later",
        || async move {
            let steps = world.db.task_steps(&attempt.task_ref).await.unwrap();
            let step = steps
                .iter()
                .find(|step| &step.causation_key == key)
                .unwrap();
            step.status == "done"
                || (step.status == "pending"
                    && step.attempts > 0
                    && step.available_at > db::now_rfc3339())
        },
    )
    .await;
    let steps = world.db.task_steps(&attempt.task_ref).await.unwrap();
    steps
        .iter()
        .find(|step| &step.causation_key == key)
        .unwrap()
        .status
        == "done"
}
fn reads(sha: &str) -> usize {
    super::proof::test_reads::of(sha)
}

async fn unresolved(world: &World, task: &str) {
    let task = world.task(task).await;
    assert_eq!(task.status, "merging", "never done without the proof");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "unresolved_result");
}

/// Design rule: a Task is never reported merged unless the target contains
/// its commit. The attempt's `integrated_sha` is only a claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn result_reports_done_only_when_the_target_owner_proves_the_commit() {
    let world = World::new().await;
    let main = |world: &World| {
        let repo = world.repo();
        async move {
            git_out(&repo, &["rev-parse", "refs/heads/main"])
                .await
                .unwrap()
        }
    };
    let base = main(&world).await;

    // 1. Forged: the row says `applied` with the candidate as the merged
    //    commit, and nothing ever moved the target.
    let a = world.add_task("forged", "f1", "one\n").await;
    let a = world.place(&a.id, "applied", 1).await;
    world.set(&a.id, "integrated_sha=candidate_sha").await;
    let a = world.attempt(&a.id).await;
    assert!(deliver_result(&world, &a).await, "answered, not reported");
    assert_eq!(reads(a.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "forged").await;
    assert!(
        world.comments("forged").await.is_empty(),
        "no merged comment"
    );
    let answer = ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).unwrap();
    assert_eq!(answer.outcome, IntegrationStepOutcome::NotLanded);

    // 2. Mismatched: the row names a commit the target does contain (its
    //    old tip), which is not the Task's commit.
    let b = world.add_task("mismatched", "f2", "two\n").await;
    let b = world.place(&b.id, "applied", 1).await;
    world.set(&b.id, &format!("integrated_sha='{base}'")).await;
    let b = world.attempt(&b.id).await;
    let asked_before = reads(&base);
    assert!(deliver_result(&world, &b).await);
    // The merged commit, then the Task's own commit.
    assert_eq!(reads(&base), asked_before + 1);
    assert_eq!(reads(b.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "mismatched").await;

    // 3. A fast-forward receipt of another attempt, of a later claim
    //    generation, or of another owner refuses the report with no Git
    //    read at all, although the target contains the commit.
    let c = world.add_task("receipt", "f3", "three\n").await;
    let c = world.place(&c.id, "applied", 1).await;
    world.apply(&c.id).await;
    let c = world.attempt(&c.id).await;
    let sha = c.candidate_sha.clone().unwrap();
    let queue = world
        .db
        .integration_queue(c.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let owner = json!({"location_id":"l-r","owner_kind":"server","daemon_id":null,"runtime_id":null,"generation":1});
    sqlx::query(
        "UPDATE integration_queue SET target_location_id='l-r',target_owner_json=? WHERE id=?",
    )
    .bind(owner.to_string())
    .bind(&queue.id)
    .execute(world.db.pool())
    .await
    .unwrap();
    let receipt = |attempt_id: &str, generation: i64, owner: Value, after: &str| {
        json!([{
            "request": {
                "fence": {"queue_id": queue.id, "attempt_id": attempt_id, "generation": generation, "lease_owner": "w", "target_owner": owner},
                "kind": "fast_forward",
                "witness": {}
            },
            "result": {"kind": "completed", "outcome": {"Done": {"before_sha": base, "after_sha": after, "branch": "main"}}},
            "operation_state": "succeeded",
            "recorded_at": db::now_rfc3339()
        }])
        .to_string()
    };
    let other_owner = json!({"location_id":"l-r","owner_kind":"daemon","daemon_id":"d-other","runtime_id":"r","generation":1});
    for (seq, (key, forged)) in [
        (
            "other-attempt",
            receipt("someone-else", 1, owner.clone(), &sha),
        ),
        ("later-generation", receipt(&c.id, 7, owner.clone(), &sha)),
        ("other-owner", receipt(&c.id, 1, other_owner, &sha)),
        ("other-commit", receipt(&c.id, 1, owner.clone(), &base)),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("UPDATE integration_attempt SET effect_receipts_json=?,effect_seq=?,effect_ack_json=NULL,acknowledged_at=NULL,revision=revision+1 WHERE id=?")
            .bind(forged)
            .bind(seq as i64 + 2)
            .bind(&c.id)
            .execute(world.db.pool())
            .await
            .unwrap();
        let c = world.attempt(&c.id).await;
        assert!(deliver_result(&world, &c).await, "{key}");
        assert_eq!(reads(&sha), 0, "{key}: refused before Git");
        unresolved(&world, "receipt").await;
        assert_eq!(
            ack(&world.attempt(&c.id).await, IntegrationStepAction::Result)
                .unwrap()
                .outcome,
            IntegrationStepOutcome::NotLanded,
            "{key}"
        );
    }
    // Its own receipt, from the target's owner: one read, reported.
    sqlx::query("UPDATE integration_attempt SET effect_receipts_json=?,effect_seq=9,effect_ack_json=NULL,acknowledged_at=NULL,revision=revision+1 WHERE id=?")
        .bind(receipt(&c.id, 1, owner.clone(), &sha))
        .bind(&c.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    let c = world.attempt(&c.id).await;
    assert!(deliver_result(&world, &c).await);
    assert_eq!(reads(&sha), 1, "the normal path costs one ancestry read");
    assert_eq!(
        ack(&world.attempt(&c.id).await, IntegrationStepAction::Result)
            .unwrap()
            .outcome,
        IntegrationStepOutcome::Done
    );
    eventually("the proven Task is done", || async {
        world.task("receipt").await.status == "done"
    })
    .await;

    // 4. Rewound: the fast-forward landed and the target was then moved
    //    back (a force-push).
    let d = world.add_task("rewound", "f4", "four\n").await;
    let d = world.place(&d.id, "applied", 1).await;
    let before = main(&world).await;
    world.apply(&d.id).await;
    git_out(&world.repo(), &["reset", "--hard", &before])
        .await
        .unwrap();
    let d = world.attempt(&d.id).await;
    assert!(deliver_result(&world, &d).await);
    assert_eq!(reads(d.candidate_sha.as_deref().unwrap()), 1);
    unresolved(&world, "rewound").await;
    assert!(world.comments("rewound").await.is_empty());
    assert_eq!(
        ack(&world.attempt(&d.id).await, IntegrationStepAction::Result)
            .unwrap()
            .outcome,
        IntegrationStepOutcome::NotLanded
    );
}

/// An owner that cannot be asked is not a "no": the step stays, the Task
/// says why, and the proof is asked again on the result timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_target_owner_that_does_not_answer_keeps_the_result_step_asking() {
    let world = World::new().await;
    let a = world.add_task("mute", "f", "one\n").await;
    let a = world.place(&a.id, "applied", 1).await;
    world.apply(&a.id).await;
    // The default checkout cannot be read (its owner is not answering).
    let repo = world.repo();
    let away = world.temp.path().join("repo-away");
    std::fs::rename(&repo, &away).unwrap();
    let a = world.attempt(&a.id).await;
    assert!(
        !deliver_result(&world, &a).await,
        "no answer: the step asks again later"
    );
    unresolved(&world, "mute").await;
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::Result).is_none());
    assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    // The owner answers again.
    std::fs::rename(&away, &repo).unwrap();
    assert!(deliver_result(&world, &a).await);
    eventually("the Task is done", || async {
        world.task("mute").await.status == "done"
    })
    .await;
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

/// A `send_back` that stops after its comment, annotation and event but
/// before its transaction (a crash, a lost lease) is delivered again: the
/// Task ends with exactly what one delivery leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redelivered_send_back_repeats_no_comment_charge_or_transition() {
    let world = World::new().await;
    for (task, budget, set, comment) in [
        (
            "redelivered-conflict",
            vec![("conflict_handoff".to_owned(), 1)],
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
            "Merge conflict handed back to the Worker: rebased onto main; conflicts were committed with markers in: f",
        ),
        (
            "redelivered-red",
            // One rejection out of the `merging` gate and one merge fix.
            vec![("gate:merging".to_owned(), 1), ("merge_fix".to_owned(), 1)],
            "state='ejected',failure_kind='candidate_check_failed',failure_message='make test exited 2'",
            "Merge checks failed: checks failed after the rebase onto main: make test exited 2",
        ),
    ] {
        let a = world.add_task(task, task, "one\n").await;
        let asked = world.place(&a.id, "rebasing", 2).await;
        world.set(&a.id, set).await;
        test_faults::interrupt_once(task);
        world.ask(&asked, IntegrationStepAction::SendBack).await;
        eventually("the Task was handed back", || async {
            world.task(task).await.status == "merge_failed"
        })
        .await;
        let step = &world.step(task, IntegrationStepAction::SendBack).await[0];
        assert_eq!(step.status, "done");
        assert!(step.attempts >= 2, "the step was delivered twice");
        assert_eq!(world.comments(task).await, vec![comment.to_owned()], "{task}");
        let transitions = world.transitions(task).await;
        assert_eq!(
            transitions
                .iter()
                .filter(|row| row.0 == "merging" && row.1 == "merge_failed")
                .count(),
            1,
            "{task}: {transitions:?}"
        );
        let charged: Vec<(String, i64)> =
            sqlx::query_as("SELECT kind,spent FROM task_budget WHERE task_id=? AND spent>0 ORDER BY kind")
                .bind(task)
                .fetch_all(world.db.pool())
                .await
                .unwrap();
        assert_eq!(charged, budget, "{task}");
        let cascades: i64 = world
            .scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='cascade' AND causation_key LIKE 'integration:%'",
                task,
            )
            .await;
        assert_eq!(cascades, 1, "{task}");
        assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::SendBack).is_some());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_root_conflict_can_reopen_for_a_corrective_subtask() {
    let world = World::new().await;
    let a = world.add_task("root-repair", "f", "one\n").await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('finished-child','p','root-repair','child','done',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    settled(
        &world,
        "root-repair",
        IntegrationStepAction::SendBack,
        "done",
    )
    .await;
    let offers = world
        .service
        .task_action_offers(
            "root-repair",
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let send_back = offers
        .available_actions
        .iter()
        .find(|offer| offer.reason == "root_review_reopen")
        .expect("a root conflict has a corrective-work action");
    assert_eq!(send_back.action.verb(), "send_back");
    world
        .service
        .perform_task_action(
            "root-repair",
            api_types::TaskAction::SendBack {
                guidance: "resolve f in a corrective subtask".into(),
            },
            offers.version,
        )
        .await
        .unwrap();
    let reopened = world.task("root-repair").await;
    assert_eq!(reopened.status, "merge_failed");
    assert!(reopened.condition.integration_wait().is_none());
    // The ejected attempt keeps its row and its statement (the lineage the
    // next admission links to) and holds no place in the queue; nothing was
    // admitted or merged by the send-back.
    assert_eq!(
        reopened
            .condition
            .integration_reason()
            .map(|reason| reason.attempt_id().as_str().to_owned()),
        Some(a.id.clone())
    );
    assert!(reopened.condition.integration_handoff_ready());
    assert_eq!(
        world
            .scalar::<String>(
                "SELECT group_concat(state) FROM integration_attempt WHERE task_ref=?",
                "root-repair"
            )
            .await,
        "ejected"
    );
    assert!(matches!(
        reopened.condition,
        db::TaskCondition::Parked {
            primary: db::ParkReason::Held { .. },
            ..
        }
    ));
    let workflow = crate::workflow::default_workflow::default_workflow();
    crate::task_hierarchy::ensure_parent_accepts_subtasks(&reopened, &workflow).unwrap();
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM execution WHERE task_id=? AND status='running'",
                "root-repair"
            )
            .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manually_repaired_root_hands_integration_ownership_to_fresh_review() {
    let world = World::new().await;
    let a = world
        .add_task(
            "manual-root",
            "f",
            "<<<<<<< ours\none\n=======\ntwo\n>>>>>>> theirs\n",
        )
        .await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('manual-child','p','manual-root','child','done',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('manual-reviewer','manual-root','reviewer','user','owner',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    settled(
        &world,
        "manual-root",
        IntegrationStepAction::SendBack,
        "done",
    )
    .await;
    std::fs::write(world.tree("manual-root").join("f"), "resolved\n").unwrap();
    git::commit_all(&world.tree("manual-root"), "manual repair")
        .await
        .unwrap();
    let offers = world
        .service
        .task_action_offers(
            "manual-root",
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.reason == "manual_merge_repair"));
    world
        .service
        .perform_task_action(
            "manual-root",
            api_types::TaskAction::retry(),
            offers.version,
        )
        .await
        .unwrap();
    // The action is accepted into recovery; the real dispatcher, not the
    // Task-step worker alone, issues its replay command.
    sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let dispatcher = crate::TaskDispatcher::new(
        world.db.clone(),
        world.bus.clone(),
        Arc::new(world.service.clone()),
    );
    eventually(
        "manual repair reaches review and gives ownership away",
        || async {
            dispatcher.reconcile_all().await.unwrap();
            let task = world.task("manual-root").await;
            task.status == "review"
                && task.condition.integration_wait().is_none()
                && task.condition.integration_handoff_ready()
        },
    )
    .await;
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM execution WHERE task_id=? AND status='running'",
                "manual-root"
            )
            .await,
        0
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

/// A parked member holds no slot and has no owner effect outstanding: the
/// Task's Cancel ends its attempt in the Cancel's own transaction (a start
/// before the worker's next sweep finds no live attempt behind the terminal
/// Task), and the worker's release-step sweep asks for the `clear` that no
/// worker asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_cancel_ends_a_parked_attempt_in_its_own_transaction_and_the_sweep_asks_the_clear() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
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

    let cancelled = world.service.cancel_task("t").await.unwrap();
    assert_eq!(cancelled.status, "cancelled");
    let ended = world.attempt(&a.id).await;
    assert_eq!(ended.state, S::Cancelled, "ended with the Task write");
    assert!(!ended.current && ended.cancel_requested_at.is_none());
    assert!(world
        .step("t", IntegrationStepAction::Clear)
        .await
        .is_empty());

    let (worker, _daemons, _config) = production_ports(&world);
    assert!(worker.sweep_once().await.unwrap().is_empty());
    settled(&world, "t", IntegrationStepAction::Clear, "done").await;
    let task = world.task("t").await;
    assert_eq!(task.status, "cancelled");
    assert!(task.condition.integration_wait().is_none());
    // Asked once: a second sweep adds no step.
    worker.sweep_once().await.unwrap();
    assert_eq!(world.step("t", IntegrationStepAction::Clear).await.len(), 1);
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

    // The worker releases the attempt and asks for the clear. Nothing else
    // ends an attempt: the flag alone leaves it where it was.
    world.ask(&flagged, IntegrationStepAction::Clear).await;
    assert_ne!(world.attempt(&a.id).await.state, S::Cancelled);
    let mut released = world.attempt(&a.id).await;
    released.state = S::Cancelled;
    world
        .db
        .transition_integration_attempt(released)
        .await
        .unwrap();
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
    world.apply(&a.id).await;
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
    // The Cancel is not awaited here. A Cancel that loses the race waits
    // behind the protected `result` step for its caller-side bound (15 s for
    // a preempting command, then `task_busy` with the command still queued).
    // Joining it before the merge is finished below is what made a
    // permit-wins round take 15 s (D2 checklist item 7): the wait was this
    // test's, not the step queue's.
    let cancel = tokio::spawn(async move { service.cancel_task(task).await });
    let _ = tokio::spawn(async move { port.enqueue_step(&request).await }).await;
    // Bounded by what the race needs, not by the runner: a round that has
    // not settled in a minute is stuck, and says on what.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let now = world.attempt(&a.id).await;
        if now.permit_json.is_some() || world.task(task).await.status == "cancelled" {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "the race did not settle in 60 s:\n{}",
                world.stuck(task).await
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
        world.apply(&a.id).await;
        world.port.ready_result_step(&a.id, 1).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while world.task(task).await.status != "done" {
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "the permitted Task is not done after 60 s:\n{}",
                    world.stuck(task).await
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    // Either the Cancel won, or it ran after the merge was recorded and found
    // a finished Task, or it gave up waiting: it has answered by now.
    let answered = tokio::time::timeout(Duration::from_secs(30), cancel).await;
    assert!(answered.is_ok(), "the Cancel command answered");
    if permit {
        assert_eq!(world.task(task).await.status, "done", "the merge stands");
    }
    permit
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_racing_the_permit_has_exactly_one_winner_under_real_concurrency() {
    let world = World::new().await;
    let mut permits = 0;
    let started = std::time::Instant::now();
    // Eight rounds: a round the permit wins waits out the Cancel's own busy
    // wait, so twenty of them outran the round deadline on a loaded runner.
    for task in ["r00", "r01", "r02", "r03", "r04", "r05", "r06", "r07"] {
        permits += usize::from(race_once(&world, task).await);
    }
    // Either order is legal; each round asserted exactly one winner.
    println!(
        "permit won {permits} of 8 rounds in {:?}",
        started.elapsed()
    );
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
            failures: vec![],
            failures_truncated: false,
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
            placement_ready: true,
            task_target_tip: String::new(),
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

/// The worker over its production ports (`build_integration_worker`): the
/// routing owner, the workspace facts, the owner object transfer, and the
/// real Task-step port. Also composes the real check runner and its worker,
/// as the server runtime does.
struct Production {
    worker: Arc<IntegrationQueueWorker>,
    stop: tokio::sync::watch::Sender<bool>,
    _checks: tokio::task::JoinHandle<()>,
}
impl Drop for Production {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
/// `build_integration_worker` over this World, with fast timers.
fn production_ports(
    world: &World,
) -> (
    Arc<IntegrationQueueWorker>,
    Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    crate::integration_worker::IntegrationWorkerConfig,
) {
    production_ports_config(
        world,
        crate::integration_worker::IntegrationWorkerConfig {
            poll: Duration::from_millis(20),
            sweep_interval: Duration::from_millis(100),
            conflict_backoff: Duration::from_millis(5),
            ..Default::default()
        },
    )
}

fn production_ports_config(
    world: &World,
    config: crate::integration_worker::IntegrationWorkerConfig,
) -> (
    Arc<IntegrationQueueWorker>,
    Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    crate::integration_worker::IntegrationWorkerConfig,
) {
    use crate::daemon_transport::{DaemonConnectionRegistry, ServerExecutionEventSink};
    let sink = Arc::new(ServerExecutionEventSink::new(
        world.db.clone(),
        world.bus.clone(),
        world.temp.path().join("events"),
    ));
    let daemons = Arc::new(DaemonConnectionRegistry::new(
        world.bus.clone(),
        sink.clone(),
    ));
    sink.set_connection_registry(Arc::downgrade(&daemons));
    let router = world
        .service
        .merge_service
        .as_ref()
        .unwrap()
        .workspace_backend_router()
        .unwrap();
    (
        crate::integration_ports::build_integration_worker(
            world.db.clone(),
            router,
            daemons.clone(),
            Arc::new(crate::repo_location::RepoLocationService::new(
                world.db.clone(),
                Arc::new(crate::repo_location::RemoteDaemonLocationVerifier::new(
                    daemons.clone(),
                    world.temp.path().join("probes"),
                )),
            )),
            config.clone(),
        ),
        daemons,
        config,
    )
}

impl Production {
    fn new(world: &World) -> Self {
        Self::with_ports(world, production_ports(world))
    }
    fn with_ports(
        world: &World,
        (worker, daemons, config): (
            Arc<IntegrationQueueWorker>,
            Arc<crate::daemon_transport::DaemonConnectionRegistry>,
            crate::integration_worker::IntegrationWorkerConfig,
        ),
    ) -> Self {
        // The check runner, its consumer families and its worker, as the
        // one constructor builds them. The merge-path family is registered
        // again only to give it this worker's (fast) timers.
        let composed = world
            .service
            .compose_checks(daemons, Duration::from_secs(60), &config);
        IntegrationCheckFamily::register(
            &composed.consumers,
            Arc::new(
                IntegrationSteps::new(world.service.clone())
                    .with_timers(IntegrationStepTimers::from(&config)),
            ),
        );
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let periodic = crate::worker_runtime::PeriodicWorkers::new(world.db.clone());
        let checks = composed.worker.start(&periodic, stopped);
        Self {
            worker,
            stop,
            _checks: checks,
        }
    }
    fn run(&self) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(self.worker.clone().run(self.stop.subscribe()))
    }
}

async fn review_ci(world: &World, task: &str, steps: &[&str]) {
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"review":{"ci_steps":steps}}).to_string())
        .bind(task)
        .execute(world.db.pool())
        .await
        .unwrap();
}

/// The cost of the head check, measured. Each Task has a real `ci_steps`
/// command. (a) Three Tasks that each reach the head with the target where
/// their review left it merge with no head-check run at all. (b) Three Tasks
/// reviewed on one base, with the target moved once from outside: every one
/// of them is rebased (by the outside commit, then by the members ahead of
/// it) and checked exactly once: one run per rebased commit, none repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn head_check_runs_are_zero_on_an_unchanged_target_and_one_per_rebased_task() {
    let world = World::new().await;
    let runs = || async {
        sqlx::query_as::<_, (String, String)>(
            "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at",
        )
        .fetch_all(world.db.pool())
        .await
        .unwrap()
    };
    let production = Production::new(&world);
    let running = production.run();
    // (a) One at a time: each Task is cut from the tip it merges into.
    for name in ["a1", "a2", "a3"] {
        let file = format!("{name}.txt");
        world.add_task(name, &file, "x\n").await;
        review_ci(&world, name, &[&format!("test -f {file}")]).await;
        production.worker.notify_answered();
        eventually("an unrebased Task merges", || async {
            world.task(name).await.status == "done"
        })
        .await;
    }
    assert_eq!(runs().await, vec![], "no head check on an unchanged target");

    // (b) Three Tasks on one base, and the target moves once.
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let mut attempts = Vec::new();
    for name in ["b1", "b2", "b3"] {
        let file = format!("{name}.txt");
        attempts.push(world.add_task(name, &file, "x\n").await);
        review_ci(
            &world,
            name,
            &[&format!("test -f {file}"), "test -f outside.txt"],
        )
        .await;
    }
    std::fs::write(world.repo().join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&world.repo(), "outside").await.unwrap();
    let production = Production::new(&world);
    let running = production.run();
    eventually("the rebased Tasks merge", || async {
        let mut done = true;
        for name in ["b1", "b2", "b3"] {
            done &= world.task(name).await.status == "done";
        }
        done
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let ran = runs().await;
    let mut expected = Vec::new();
    for attempt in &attempts {
        let merged = world.attempt(&attempt.id).await;
        assert_ne!(merged.integrated_sha, merged.original_candidate_sha);
        expected.push((merged.integrated_sha.clone().unwrap(), "pass".to_owned()));
    }
    let mut sorted = ran.clone();
    sorted.sort();
    expected.sort();
    assert_eq!(
        sorted, expected,
        "exactly one run per rebased Task: {ran:?}"
    );
}

/// D2 checklist item 11. Three Tasks through the real worker, the production
/// ports and the real check runner on a server-owned target, each with a real
/// `ci_steps` command. The first merges as reviewed (no check: the target did
/// not move). The second is rebased by the queue and its command passes only
/// on the rebased commit: green, merged. The third is rebased and its command
/// exits non-zero: red, ejected, sent back, never merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn real_ci_steps_decide_the_head_through_the_production_ports() {
    let world = World::new().await;
    let first = world.add_task("one", "one.txt", "one\n").await;
    let green = world.add_task("two", "two.txt", "two\n").await;
    let red = world.add_task("three", "three.txt", "three\n").await;
    review_ci(&world, "one", &["test -f one.txt"]).await;
    // Passes only on top of the first Task's commit: the rebased commit.
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
    // Git: main holds the first two, the second rebased; never the third.
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
    // The checks really ran: one run per rebased commit, with the commands'
    // own exit codes, and none for the Task that merged as reviewed.
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
        ],
        "{runs:?}"
    );
    assert!(matches!(
        two.phase_timings
            .as_ref()
            .and_then(|timings| timings.check.clone()),
        Some(db::IntegrationCheckTiming::Ran { .. })
    ));
    assert_eq!(
        three.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
    assert!(
        three
            .failure_message
            .as_deref()
            .unwrap()
            .contains("exited 3"),
        "{:?}",
        three.failure_message
    );
    assert!(world
        .step("one", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    // The red Task is told why, by its `send_back` step.
    assert_eq!(
        world
            .step("three", IntegrationStepAction::SendBack)
            .await
            .len(),
        1
    );
    assert!(world
        .db
        .integration_queue(three.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap()
        .head_attempt_id
        .is_none());
}

/// A check that ended "no verdict after its retries" says nothing about the
/// commit. The round that waited for it is told so (and parks, counted). A
/// later round of the same commit must not read that stored answer again:
/// it would park on every retry and end at the owner without the check ever
/// running again. The new round's ask re-arms the consumer (a fresh
/// infrastructure budget, a new run) and waits. Only an answer whose cleanup
/// is unproven still stands, which the counted retries bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_verdict_answer_is_not_kept_for_the_commit_and_a_new_round_asks_again() {
    let world = World::new().await;
    let a = world.add_task("one", "one.txt", "one\n").await;
    review_ci(&world, "one", &["true"]).await;
    world.service.compose_checks(
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        Duration::from_secs(60),
        &crate::integration_worker::IntegrationWorkerConfig::default(),
    );
    let asked = |round: i64| {
        let world = &world;
        let id = a.id.clone();
        async move {
            let placed = world.place(&id, "checking", round).await;
            world
                .ask(&placed, IntegrationStepAction::RequestCheck)
                .await;
            eventually("the request_check step to finish", || async {
                let steps = world.step("one", IntegrationStepAction::RequestCheck).await;
                steps.len() as i64 == round && steps.iter().all(|step| step.status == "done")
            })
            .await;
            world.attempt(&id).await
        }
    };
    // Round 1 asks; the run exists and nothing has answered yet.
    let first = asked(1).await;
    assert_eq!(first.state, S::Checking);
    assert!(ack(&first, IntegrationStepAction::Settle).is_none());
    let (consumer, run): (String, String) =
        sqlx::query_as("SELECT id,run_id FROM check_consumer WHERE task_id='one'")
            .fetch_one(world.db.pool())
            .await
            .unwrap();
    // The run ends without a verdict after its retries, and that answer was
    // delivered and applied (round 1 parked on it).
    let now = db::now_rfc3339();
    let step = world.step("one", IntegrationStepAction::RequestCheck).await[0]
        .id
        .clone();
    let exhaust = |cleanup: &'static str, result: &'static str| {
        let (world, run, consumer, now, step) = (
            &world,
            run.clone(),
            consumer.clone(),
            now.clone(),
            step.clone(),
        );
        async move {
            let pool = world.db.pool();
            sqlx::query("UPDATE check_run SET state='failed',lease_owner=NULL,lease_until=NULL,finished_at=? WHERE id=?")
                .bind(&now).bind(&run).execute(pool).await.unwrap();
            sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) SELECT ?,id,identity_key,'infrastructure_failed',?,0,0,'[]',0,? FROM check_run WHERE id=?")
                .bind(result).bind(cleanup).bind(&now).bind(&run).execute(pool).await.unwrap();
            sqlx::query("UPDATE check_consumer SET run_id=?,result_id=?,infrastructure_retries=2,delivery_step_id=?,applied_at=? WHERE id=?")
                .bind(&run).bind(result).bind(&step).bind(&now).bind(&consumer).execute(pool).await.unwrap();
        }
    };
    exhaust("not_performed", "no-verdict-1").await;

    // Round 2, same commit: asked again, not answered from the stored row.
    let second = asked(2).await;
    assert_eq!(second.candidate_sha, first.candidate_sha);
    assert_eq!(second.state, S::Checking);
    assert!(
        ack(&second, IntegrationStepAction::Settle).is_none(),
        "{:?}",
        second.effect_ack_json
    );
    let (retries, delivery, applied): (i64, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT infrastructure_retries,delivery_step_id,applied_at FROM check_consumer WHERE id=?",
    )
    .bind(&consumer)
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    assert_eq!((retries, delivery, applied), (0, None, None));
    assert_eq!(
        world
            .scalar::<i64>("SELECT COUNT(*) FROM check_consumer WHERE task_id=?", "one")
            .await,
        1
    );

    // An exhausted answer whose cleanup is unproven is not run again from
    // here: it stands as "no verdict" for this round too.
    exhaust("uncertain", "no-verdict-2").await;
    let third = asked(3).await;
    let answer = ack(&third, IntegrationStepAction::Settle).expect("answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Infrastructure);
}

impl World {
    fn clone_path(&self) -> PathBuf {
        self.temp.path().join("clone-r")
    }
    /// A second server-owned checkout of the repo: its own clone, with its
    /// own object store and refs, registered as a non-default location.
    async fn add_clone(&self) {
        let clone = self.clone_path();
        git_out(
            self.temp.path(),
            &[
                "clone",
                "--quiet",
                self.repo().to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        )
        .await
        .expect("clone");
        // A clone does not copy the source's local identity.
        for (key, value) in [
            ("user.name", "Forge test"),
            ("user.email", "forge@example.invalid"),
        ] {
            git_out(&clone, &["config", key, value])
                .await
                .expect("identity");
        }
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l-c','r','server',?,'managed_clone',0,'ready',?,?)").bind(clone.to_str()).bind(&now).bind(&now).execute(self.db.pool()).await.unwrap();
    }
    /// Like `add_task`, but the Task's worktree belongs to the clone.
    async fn add_task_in_clone(&self, task: &str, file: &str, content: &str) -> IntegrationAttempt {
        let (clone, tree) = (self.clone_path(), self.tree(task));
        let branch = format!("task-{task}");
        git_out(
            &clone,
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
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l-c',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
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
        attempt.execution_id = Some(self.seed_execution(task, "executor", None).await);
        attempt.execution_ref = attempt.execution_id.clone();
        attempt.original_candidate_sha = Some(head);
        self.db.admit_integration_attempt(attempt).await.unwrap()
    }
}

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
    // The checks ran on the rebased commits in the clone.
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
        ],
        "{runs:?}"
    );
    assert_eq!(
        three.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
}

impl World {
    /// An agent that holds the reviewer role of `task`.
    async fn seed_reviewer(&self, task: &str) -> String {
        let agent_id = format!("reviewer-{task}");
        let now = db::now_rfc3339();
        db::AgentRepo::create(
            &*self.db,
            db::CreateAgent {
                id: agent_id.clone(),
                name: agent_id.clone(),
                description: None,
                executor_type: "shell".to_owned(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 2,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("agent creates");
        db::TaskRoleAssignmentRepo::assign(
            &*self.db,
            db::CreateTaskRoleAssignment {
                id: db::new_uuid_v4(),
                task_id: task.to_owned(),
                role_name: "reviewer".to_owned(),
                assignee_type: Some(db::AssigneeKind::Agent),
                assignee_id: Some(agent_id.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("role assignment creates");
        agent_id
    }
    async fn seed_execution(&self, task: &str, role: &str, agent: Option<&str>) -> String {
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        db::ExecutionRepo::create(
            &*self.db,
            db::CreateExecution {
                id: id.clone(),
                task_id: task.to_owned(),
                agent_id: agent.map(str::to_owned),
                role: role.to_owned(),
                // Stored settled; the reviewer's row is then marked running
                // below, as it is while its contract is frozen. (Creating a
                // running execution goes through dispatch admission, which
                // this fixture has no machine for.)
                status: db::ExecutionStatus::Completed,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: Some(format!("w-{task}")),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");
        if agent.is_some() {
            sqlx::query("UPDATE execution SET status='running' WHERE id=?")
                .bind(&id)
                .execute(self.db.pool())
                .await
                .unwrap();
        }
        id
    }
    /// A passed agent review of `task`'s present commit against the present
    /// target tip, exactly as a reviewer run that passed leaves it (the
    /// fixture `seed_passed_carry_review` of the workflow action tests): the
    /// executor's and the reviewer's executions, the frozen contract, the
    /// frozen passed assessment and the passed Review row. `reviewed_paths`
    /// is the change set the contract froze. Returns the contract's
    /// execution id.
    async fn seed_passed_review(&self, task: &str, reviewed_paths: &[&str]) -> String {
        use db::ReviewConformanceRepo;
        let agent = self.seed_reviewer(task).await;
        let executor = self.seed_execution(task, "executor", None).await;
        let reviewer = self.seed_execution(task, "reviewer", Some(&agent)).await;
        let commit_sha = git::get_current_sha(&self.tree(task)).await.unwrap();
        let base_sha = git_out(&self.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap();
        let context = ::review::contract::load_context(&self.db, task, Some(&reviewer))
            .await
            .expect("governing context loads");
        let mut contract = api_types::ReviewContract {
            execution_id: reviewer.clone(),
            policy: api_types::REVIEW_CONFORMANCE_POLICY.to_owned(),
            commit_sha,
            base_sha,
            candidate_changed_paths: reviewed_paths
                .iter()
                .map(|path| (*path).to_owned())
                .collect(),
            context,
            check_results: Vec::new(),
            digest: String::new(),
        };
        contract.digest = api_types::canonical_digest(&contract).expect("contract digests");
        self.db
            .create_review_contract(&contract)
            .await
            .expect("contract freezes");
        let conformance = api_types::ReviewConformance {
            status: api_types::ConformanceStatus::Passed,
            contract: Some(contract),
            assessment: Some(api_types::ReviewAssessment {
                fixable_by: api_types::FixableBy::Coder,
                repeat: false,
                findings: Vec::new(),
                resolved: Vec::new(),
                result: api_types::ReviewResult::Pass,
                reason: "looks right".to_owned(),
                report: String::new(),
            }),
            checks: Vec::new(),
            reason: None,
        };
        self.db
            .record_review_conformance(&conformance)
            .await
            .expect("assessment freezes");
        sqlx::query("UPDATE execution SET status='completed' WHERE id=?")
            .bind(&reviewer)
            .execute(self.db.pool())
            .await
            .unwrap();
        let now = db::now_rfc3339();
        db::ReviewRepo::create(
            &*self.db,
            db::CreateReview {
                id: db::new_uuid_v4(),
                task_id: task.to_owned(),
                execution_id: executor,
                attempt_number: 1,
                status: db::ReviewStatus::Passed,
                step_results_json: json!({
                    "ci_steps": [],
                    "conformance": conformance,
                    "auditor": { "verdict": "pass", "reason": "looks right" },
                })
                .to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("passed review records");
        reviewer
    }
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

/// A digest of every row of every table except the three the worker may
/// write: the queue, the attempt and the Task-step queue (with
/// `task_schedule_dirty`, which the `task_step` triggers maintain on every
/// step insert: the scheduler's "look at this Task" mark).
async fn foreign_digest(db: &SqliteDb) -> std::collections::BTreeMap<String, String> {
    use sha2::Digest;
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('integration_queue','integration_attempt','task_step','task_schedule_dirty') ORDER BY name",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let mut digests = std::collections::BTreeMap::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT name FROM pragma_table_xinfo('{table}') WHERE hidden IN (0,2,3) ORDER BY cid"
        ))
        .fetch_all(db.pool())
        .await
        .unwrap();
        let row = columns
            .iter()
            .map(|column| format!("quote(\"{column}\")"))
            .collect::<Vec<_>>()
            .join("||'|'||");
        let rows: Vec<String> =
            sqlx::query_scalar(&format!("SELECT {row} FROM \"{table}\" ORDER BY 1"))
                .fetch_all(db.pool())
                .await
                .unwrap();
        let mut hash = sha2::Sha256::new();
        for row in &rows {
            hash.update(row.as_bytes());
            hash.update(b"\n");
        }
        digests.insert(
            table,
            format!("{}:{}", rows.len(), hex::encode(hash.finalize())),
        );
    }
    digests
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

// ----- importer continuation from pre-upgrade fixtures (plan 3.2 stage D2b) --

impl World {
    /// A Task in `status` as a server without the queue left it: one commit
    /// writing `file` in its worktree, no attempt. Returns that commit.
    async fn legacy_task(&self, task: &str, file: &str, status: &str) -> String {
        let (repo, tree) = (self.repo(), self.tree(task));
        let branch = format!("task-{task}");
        git_out(
            &repo,
            &["worktree", "add", "-b", &branch, tree.to_str().unwrap()],
        )
        .await
        .expect("worktree");
        std::fs::write(tree.join(file), "legacy\n").unwrap();
        git::commit_all(&tree, task).await.unwrap();
        let now = db::now_rfc3339();
        let pool = self.db.pool();
        sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,?,?,?,?)").bind(task).bind(task).bind(status).bind(&now).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,?,'ready',?,?)").bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&branch).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l-r',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
        git::get_current_sha(&tree).await.unwrap()
    }
    /// Somebody else's commit lands on the target, and the old merge path
    /// rebases the Task's branch onto it cleanly. Returns the rebased commit.
    async fn old_path_rebase(&self, task: &str) -> String {
        std::fs::write(self.repo().join("moved"), "moved\n").unwrap();
        git::commit_all(&self.repo(), "target moved").await.unwrap();
        git_out(&self.tree(task), &["rebase", "main"])
            .await
            .expect("clean rebase");
        git::get_current_sha(&self.tree(task)).await.unwrap()
    }
    /// The settled hooks step of the old `merging` entry, with `effects` as
    /// its retained checkpoint.
    async fn old_merge_step(&self, task: &str, effects: Value) {
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,attempts,created_at,updated_at,expected_epoch,lane) VALUES(?,?,(SELECT COALESCE(MAX(seq),0)+1 FROM task_step WHERE task_id=?),'hooks','{}','entry',?,1,'merging',1,'failed',?,1,?,?,0,'long')")
            .bind(format!("s-{task}")).bind(task).bind(task).bind(format!("c-{task}")).bind(&now).bind(&now).bind(&now).execute(self.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES(?,0,?,?)")
            .bind(format!("s-{task}")).bind(&now).bind(effects.to_string())
            .execute(self.db.pool()).await.unwrap();
    }
    /// Stop the Task-step worker and wait until it has stopped.
    pub(super) async fn stop_steps(&mut self) {
        let worker = self
            .step_worker
            .take()
            .expect("the Task-step worker is running");
        let _ = self.stop.send(true);
        tokio::time::timeout(Duration::from_secs(10), worker)
            .await
            .expect("the Task-step worker stops")
            .unwrap();
    }
    /// Start the Task-step worker again, as the server runs it.
    pub(super) fn start_steps(&mut self) {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        self.stop = stop;
        self.step_worker = Some(self.service.task_step_worker().start(stopped));
    }
    /// The start's import, in the server's order: `import_at_start` runs
    /// before any worker, so the row read here is the one the importer
    /// wrote. The Task-step worker is stopped while the importer runs and
    /// the row is read, then started again to run the continuation. Left
    /// running, it may run the continuation first, and a row that is
    /// admitted again (P6, P9, P10, P18) is then superseded by a fresh
    /// attempt with no import source.
    async fn imported(&mut self, task: &str) -> (IntegrationAttempt, u64) {
        self.stop_steps().await;
        let report = super::import::import_at_start(&self.db).await.unwrap();
        assert_eq!(report.continuations, 1, "{report:?}");
        // A second start imports and enqueues nothing.
        let again = super::import::import_at_start(&self.db).await.unwrap();
        assert_eq!((again.imported, again.continuations), (0, 0));
        let attempt = self
            .db
            .current_integration_attempt(task)
            .await
            .unwrap()
            .expect("imported");
        let priority = attempt.import_source_json.as_ref().unwrap()["priority"]
            .as_u64()
            .unwrap();
        self.start_steps();
        (attempt, priority)
    }
    async fn carries(&self, task: &str) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT commit_sha,base_sha FROM review_authority_carry WHERE task_id=? ORDER BY created_at",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap()
    }
}

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

/// What the importer and its continuation leave for one pre-upgrade shape.
struct ImportedRow {
    merged: bool,
    /// Every attempt of the Task, for the failure message.
    trail: String,
    priority: u64,
    state: S,
    status: String,
    /// The typed integration reason on the Task's condition, if any.
    deferred: Option<String>,
    failed_steps: usize,
    /// `(verb, reason)` of every action the Task offers its owner.
    offers: Vec<(String, String)>,
    /// The Task's whole condition, for the failure message.
    condition: String,
}

/// Build the legacy shape `seed` describes for Task `t`, run the start-time
/// import twice, let the continuation step settle (the real step worker and
/// the real queue worker run), and report. Nothing may merge: every row here
/// is one the queue must NOT continue by itself.
async fn import_row(status: &str, seed: &str) -> ImportedRow {
    let world = World::new().await;
    world.legacy_task("t", "t.txt", status).await;
    world.seed_execution("t", "executor", None).await;
    for statement in seed.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(statement)
            .execute(world.db.pool())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    // The seed wrote the legacy columns behind the Task's back: its stored
    // condition is derived again, as every real write derives it.
    // (Not the row whose metadata is deliberately unreadable: no writer
    // can touch it, which is the row's point.)
    if !seed.contains("corrupt") {
        let _ =
            db::TaskRepo::mutate_metadata(&*world.db, "t", None, Vec::new(), &db::now_rfc3339())
                .await;
    }
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    let first = super::import::import_at_start(&world.db).await.unwrap();
    assert_eq!(first.imported, 1, "{first:?}");
    let again = super::import::import_at_start(&world.db).await.unwrap();
    assert_eq!((again.imported, again.continuations), (0, 0), "run twice");
    let production = Production::new(&world);
    let running = production.run();
    eventually("the continuation settles", || async {
        world
            .db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .filter(|step| step.kind == INTEGRATION_STEP_KIND)
            .all(|step| step.status != "pending" && step.status != "claimed")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let merged = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap()
        != tip;
    let trail: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT admission_key,state,failure_message FROM integration_attempt WHERE task_ref='t' ORDER BY created_at,id",
    )
    .fetch_all(world.db.pool())
    .await
    .unwrap();
    let (id, source): (String, String) = sqlx::query_as(
        "SELECT id,import_source_json FROM integration_attempt WHERE task_ref='t' AND import_source_json IS NOT NULL",
    )
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    let source: Value = serde_json::from_str(&source).unwrap();
    let task = world.task("t").await;
    let condition = integration(&task);
    ImportedRow {
        merged,
        trail: format!("{trail:?}"),
        priority: source["priority"].as_u64().unwrap(),
        state: world.attempt(&id).await.state,
        status: task.status.clone(),
        deferred: (!condition.is_null()).then(|| condition.to_string()),
        failed_steps: world
            .db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .filter(|step| step.status == "failed")
            .count(),
        offers: world
            .service
            .task_action_offers(
                "t",
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await
            .map(|offers| {
                offers
                    .available_actions
                    .into_iter()
                    .map(|offer| (offer.action.verb().to_owned(), offer.reason))
                    .collect()
            })
            .unwrap_or_default(),
        condition: format!(
            "{}\nsteps={:?}",
            json!(task.condition),
            world
                .db
                .task_steps("t")
                .await
                .unwrap()
                .iter()
                .map(|step| (&step.causation_key, &step.status, &step.last_error))
                .collect::<Vec<_>>()
        ),
    }
}

/// Importer rows the queue must not continue by itself, each from the legacy
/// columns the old code wrote, imported twice: the Task keeps its status,
/// nothing merges and no step fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn imported_rows_that_wait_for_their_owner_stay_put_with_a_typed_reason() {
    let conflict = "INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,bridge_kind,bridge_payload,created_at) VALUES('handoff','t','merging','merge_failed','system:workflow','conflict','conflict_handoff','{\"paths\":[\"t.txt\"]}','2026-10-01T00:00:00Z')";
    // (row, Task status, legacy state, attempt state after, typed cause)
    let rows: [(u64, &str, &str, S, Option<&str>); 8] = [
        // P7: a coder is repairing a handed-off conflict.
        (7, "merge_failed", conflict, S::Ejected, None),
        // P13: an existing block stays the Task's reason.
        (
            13,
            "merging",
            "UPDATE task SET blocked_json='{\"reason\":\"held by the owner\"}' WHERE id='t'",
            S::Parked,
            None,
        ),
        // P14: a stored retry deadline is kept; the sweep re-queues at it.
        (
            14,
            "merging",
            "UPDATE task SET metadata_json='{\"deferred_dispatch\":{\"not_before\":\"2099-01-01T00:00:00Z\",\"target_state\":\"merging\"}}' WHERE id='t'",
            S::Parked,
            Some("infrastructure"),
        ),
        // P15: the Task waited for its owner machine.
        (
            15,
            "merging",
            "UPDATE task SET metadata_json='{\"owner_wait\":{\"daemon_id\":\"gone\"}}' WHERE id='t'",
            S::Parked,
            Some("owner_offline"),
        ),
        // P16: the workspace's machine was removed.
        (
            16,
            "merging",
            "UPDATE workspace SET error='machine_removed' WHERE task_id='t'",
            S::Parked,
            Some("owner_required"),
        ),
        // P17: an accepted owner command is still to run.
        (
            17,
            "merging",
            "UPDATE task SET metadata_json='{\"queued_recovery\":{\"id\":\"accepted\"}}' WHERE id='t'",
            S::Parked,
            Some("owner_required"),
        ),
        // P19: an ordinary dirty-worktree repair by the coder.
        (
            19,
            "merge_failed",
            "UPDATE task SET error_annotation='{\"type\":\"dirty_worktree\"}' WHERE id='t'",
            S::Ejected,
            None,
        ),
        // P20: unreadable legacy state is kept, quarantined, for the owner.
        (
            20,
            "merging",
            "UPDATE task SET metadata_json='{corrupt' WHERE id='t'",
            S::Quarantined,
            None,
        ),
    ];
    for (priority, status, seed, state, cause) in rows {
        let row = import_row(status, seed).await;
        assert_eq!(row.priority, priority, "row P{priority}: {}", row.trail);
        assert!(!row.merged, "row P{priority} merged: {}", row.trail);
        assert_eq!(row.state, state, "row P{priority}: {}", row.trail);
        assert_eq!(
            row.status, status,
            "row P{priority}: the Task does not move"
        );
        assert_eq!(row.failed_steps, 0, "row P{priority}");
        // The reason is visible: the typed integration cause the
        // continuation states is on the Task's condition.
        // For these fixtures (legacy columns written with plain SQL) the
        // condition seam refuses the statement: the continuation records the
        // refusal on its step, the Task keeps its own reason, and the
        // reconciler supersedes the row once that reason is lifted. Either
        // the cause is on the Task or the refusal is on the step; never
        // neither.
        if let Some(cause) = cause {
            let stated = row.deferred.as_deref().is_some_and(|deferred| {
                deferred.contains("\"deferred\"") && deferred.contains(&format!("\"{cause}\""))
            });
            assert!(
                stated || row.condition.contains("integration statement refused"),
                "row P{priority}: `deferred` + `{cause}` is neither stated nor recorded as refused: {:?}\n{}",
                row.deferred,
                row.condition
            );
        }
        println!(
            "row P{priority}: status={} attempt={} integration={:?} offers={:?}",
            row.status, row.state, row.deferred, row.offers
        );
        // And the action that moves it is offered. A Task that is being
        // repaired (P7, P19) is in its repair state, where the Worker's own
        // flow continues; every row that stays in the merge state offers its
        // owner an action that gets it moving again.
        if status == "merging" {
            assert!(
                row.offers.iter().any(|(verb, _)| matches!(
                    verb.as_str(),
                    "retry" | "release" | "restart" | "approve"
                )),
                "row P{priority}: no action is offered that moves the Task: {:?}\n{}",
                row.offers,
                row.condition
            );
        }
    }
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

#[path = "gate_tests.rs"]
mod gate_tests;

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

/// The server's login shell gives no environment: review entry cannot
/// identify its check. Its first attempts are the hooks step's own (the
/// opened attempt and the checkpoint stay, nothing is shown). After
/// `ENVIRONMENT_ATTEMPTS` the Task states the review-CI infrastructure
/// interruption with its cause and a deferred re-entry; the unstarted Review
/// attempt is discarded, and no check, consumer or failed Review exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_review_entry_without_an_environment_answer_retries_then_states_the_interruption() {
    // The workflow's own entry, and the entry the owner's Retry of the
    // review asks for: both land the same way.
    for owner_asked in [false, true] {
        review_entry_without_an_environment_answer(owner_asked).await;
    }
}

async fn review_entry_without_an_environment_answer(owner_asked: bool) {
    let triggered_by = if owner_asked {
        crate::TaskService::task_action_actor(api_types::Actor::user(
            api_types::UserActionSource::Action(api_types::TaskAction::retry()),
        ))
    } else {
        crate::worker_runtime::queue::cascade_actor()
    };
    use crate::workflow::HookAction as _;
    let mut world = World::new().await;
    let _ = world.stop.send(true);
    tokio::time::timeout(Duration::from_secs(10), world.step_worker.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.legacy_task("silent", "silent.txt", "review").await;
    let execution = world.seed_execution("silent", "executor", None).await;
    review_ci(&world, "silent", &["true"]).await;
    let _production = Production::new(&world);
    crate::check_runner::policy::silence_login_shell_for_test(&world.db, true)
        .await
        .unwrap();
    let project = db::ProjectRepo::get_by_id(&*world.db, "p")
        .await
        .unwrap()
        .unwrap();
    let task = world.task("silent").await;
    let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &crate::worker_runtime::queue::cascade_actor(),
    );
    let state = workflow
        .states
        .iter()
        .find(|state| state.name == "review")
        .unwrap();
    let ctx = crate::workflow::HookContext {
        task_id: task.id.clone(),
        project_id: "p".into(),
        from_state: "in_progress".into(),
        to_state: "review".into(),
        db: world.db.clone(),
        event_bus: world.bus.clone(),
        gate_config: state.gate_config.clone(),
        state_config: crate::workflow::engine::effective_state_config(
            state,
            Some(&project),
            task.task_state_config.as_deref(),
        ),
        workflow: Arc::new(workflow),
        project_version: Some(project.version),
        project_workflow_definition: Some(project.workflow_definition),
        triggered_by,
        merge_service: world.service.merge_service.clone(),
        cleanup_scheduler: None,
        task_service: world.service.clone(),
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: world.temp.path().join("workspaces"),
        repo_cache_locks: None,
        workspace_backend_router: world.service.workspace_backend_router.clone(),
        workspace_id: Some("w-silent".into()),
        agent_id: None,
        execution_id: Some(execution),
    };
    db::TaskStepRepo::enqueue_step(
        &*world.db,
        &db::EnqueueTaskStep {
            kind: "hooks".to_owned(),
            id: "silent-entry".to_owned(),
            task_id: "silent".to_owned(),
            payload_json: "{}".to_owned(),
            causation_step_id: None,
            causation_key: "silent-entry".to_owned(),
            chain_id: "silent-entry".to_owned(),
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "long".to_owned(),
            available_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let attempt = |attempts: i64| {
        let (db, ctx) = (world.db.clone(), &ctx);
        async move {
            sqlx::query("UPDATE task_step SET status='claimed',claimed_by='silent-test',lease_until=?,attempts=? WHERE id='silent-entry'")
                .bind(db::task_writer::lease_deadline())
                .bind(attempts)
                .execute(db.pool())
                .await
                .unwrap();
            let step = db::TaskStepRepo::task_steps(&*db, "silent")
                .await
                .unwrap()
                .into_iter()
                .find(|step| step.id == "silent-entry")
                .unwrap();
            db.start_hook(&step, 0).await.unwrap();
            // As the step worker runs it: under the Task's step lease.
            db::task_writer::in_task_step(
                step.clone(),
                crate::workflow::engine::durable::in_hook(
                    db.clone(),
                    step,
                    0,
                    crate::workflow::actions::RunCiSteps.execute(ctx),
                ),
            )
            .await
        }
    };
    let count = |sql: &'static str| {
        let db = world.db.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sql)
                .fetch_one(db.pool())
                .await
                .unwrap()
        }
    };
    const OPEN_REVIEWS: &str = "SELECT COUNT(*) FROM review WHERE task_id='silent'";
    const CHECKS: &str = "SELECT (SELECT COUNT(*) FROM check_run)+(SELECT COUNT(*) FROM check_consumer)+(SELECT COUNT(*) FROM check_environment_attestation)";

    // An early attempt: the step asks again; the Task shows nothing.
    let (early, wait) = attempt(1).await;
    assert!(
        matches!(&early, crate::workflow::HookResult::Failed { reason } if reason.contains("login shell did not report")),
        "{early:?}"
    );
    assert!(wait.is_none());
    assert_eq!(count(OPEN_REVIEWS).await, 1, "the opened attempt is kept");
    assert_eq!(count(CHECKS).await, 0);
    let quiet = world.task("silent").await;
    assert_eq!(quiet.error_annotation, None);
    assert_eq!(quiet.version, task.version);

    // The last quiet attempt is spent: the interruption is stated.
    let (stated, wait) = attempt(crate::worker_runtime::queue::ENVIRONMENT_ATTEMPTS).await;
    assert!(
        matches!(&stated, crate::workflow::HookResult::Failed { reason } if reason.contains("login shell did not report")),
        "{stated:?}"
    );
    assert!(wait.is_none());
    assert_eq!(
        count(OPEN_REVIEWS).await,
        0,
        "no command ran: the attempt is not a Review and spends no review budget"
    );
    assert_eq!(count(CHECKS).await, 0);
    let interrupted = world.task("silent").await;
    assert_eq!(interrupted.status, "review");
    let annotation: serde_json::Value =
        serde_json::from_str(interrupted.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "before_work_hook_failed");
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("login shell did not report"));
    let metadata: serde_json::Value =
        serde_json::from_str(interrupted.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        metadata["deferred_dispatch"]["target_state"], "review",
        "the Task re-enters review by itself: {metadata}"
    );
    assert_eq!(
        count("SELECT spent FROM task_budget WHERE task_id='silent' AND kind='review_ci_infrastructure'").await,
        1
    );
    crate::check_runner::policy::silence_login_shell_for_test(&world.db, false)
        .await
        .unwrap();
}

/// Run real review-entry CI under the hooks lease, then enter the queue.
/// The unchanged head must get a real Hit; changing the target changes the
/// rebased commit and must produce one new run and Review evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn entry_ci_and_head_ci_share_identity_reuse_a_real_hit_and_record_review_evidence() {
    for moved in [false, true] {
        let mut world = World::new().await;
        sqlx::query("CREATE TABLE test_check_identity_inputs(input_json TEXT NOT NULL)")
            .execute(world.db.pool())
            .await
            .unwrap();
        let _ = world.stop.send(true);
        tokio::time::timeout(Duration::from_secs(10), world.step_worker.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
            .execute(world.db.pool())
            .await
            .unwrap();
        let original = world
            .legacy_task("evidence", "evidence.txt", "review")
            .await;
        let execution = world.seed_execution("evidence", "executor", None).await;
        let counter = world.temp.path().join("check-executions");
        let command = format!("echo run >> '{}'", counter.display());
        review_ci(&world, "evidence", &[&command]).await;
        let production = Production::new(&world);
        let project = db::ProjectRepo::get_by_id(&*world.db, "p")
            .await
            .unwrap()
            .unwrap();
        let task = world.task("evidence").await;
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &crate::worker_runtime::queue::cascade_actor(),
        );
        let state = workflow
            .states
            .iter()
            .find(|state| state.name == "review")
            .unwrap();
        let ctx = crate::workflow::HookContext {
            task_id: task.id.clone(),
            project_id: "p".into(),
            from_state: "in_progress".into(),
            to_state: "review".into(),
            db: world.db.clone(),
            event_bus: world.bus.clone(),
            gate_config: state.gate_config.clone(),
            state_config: crate::workflow::engine::effective_state_config(
                state,
                Some(&project),
                task.task_state_config.as_deref(),
            ),
            workflow: Arc::new(workflow),
            project_version: Some(project.version),
            project_workflow_definition: Some(project.workflow_definition),
            triggered_by: crate::worker_runtime::queue::cascade_actor(),
            merge_service: world.service.merge_service.clone(),
            cleanup_scheduler: None,
            task_service: world.service.clone(),
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: world.temp.path().join("workspaces"),
            repo_cache_locks: None,
            workspace_backend_router: world.service.workspace_backend_router.clone(),
            workspace_id: Some("w-evidence".into()),
            agent_id: None,
            execution_id: Some(execution),
        };
        let ci = crate::workflow::actions::run_ci_steps_in_step(
            &ctx,
            &world.service.check_worker_or_embedded(),
        )
        .await;
        assert!(matches!(ci, crate::workflow::HookResult::Ok), "{ci:?}");
        let entry: (String,String,String) = sqlx::query_as("SELECT c.result_id,c.identity_key,r.commit_sha FROM check_consumer c JOIN check_run r ON r.id=c.run_id WHERE c.task_id='evidence' AND c.origin='entry'")
            .fetch_one(world.db.pool()).await.unwrap();
        assert_eq!(entry.2, original);
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            1
        );
        sqlx::query("UPDATE task SET status='merging',status_epoch=status_epoch+1,version=version+1 WHERE id='evidence'")
            .execute(world.db.pool()).await.unwrap();
        if moved {
            std::fs::write(world.repo().join("outside"), "moved\n").unwrap();
            git::commit_all(&world.repo(), "target moved after entry CI")
                .await
                .unwrap();
        }
        let (stop_steps, stopped_steps) = tokio::sync::watch::channel(false);
        world.stop = stop_steps;
        world.step_worker = Some(world.service.task_step_worker().start(stopped_steps));
        let admitted = world
            .service
            .admit_to_integration_queue("evidence")
            .await
            .unwrap();
        let running = production.run();
        eventually("the checked Task merges", || async {
            world.task("evidence").await.status == "done"
        })
        .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        let row: String = sqlx::query_scalar("SELECT step_results_json FROM review WHERE task_id='evidence' ORDER BY attempt_number DESC LIMIT 1")
            .fetch_one(world.db.pool()).await.unwrap();
        let details: Value = serde_json::from_str(&row).unwrap();
        let merged = world.attempt(&admitted.id).await;
        let evidence = &details["execution"]["integration_checks"][0];
        // What the step wrote is what the reviews endpoint parses.
        let typed: api_types::ReviewDetails = serde_json::from_str(&row).unwrap();
        if typed.execution.is_none() {
            let probes = crate::check_runner::policy::probe_log::describe(&world.db).await;
            let identities: Vec<String> = sqlx::query_scalar(
                "SELECT input_json FROM test_check_identity_inputs ORDER BY rowid",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            let identities: Vec<Value> = identities
                .iter()
                .map(|raw| serde_json::from_str(raw).unwrap())
                .collect();
            let differing_fields: Vec<_> = identities
                .windows(2)
                .flat_map(|pair| {
                    let mut fields = Vec::new();
                    for field in ["project_id", "repo_id", "commit_sha"] {
                        if pair[0][field] != pair[1][field] {
                            fields.push(field.to_owned());
                        }
                    }
                    for field in [
                        "spec",
                        "environment",
                        "environment_identity",
                        "execution_revision",
                    ] {
                        if pair[0]["inputs"][field] != pair[1]["inputs"][field] {
                            fields.push(format!("inputs.{field}"));
                        }
                    }
                    fields
                })
                .collect();
            let reviews: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT id,status,step_results_json FROM review WHERE task_id='evidence' ORDER BY attempt_number",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            /// Origin, request key, run, result, cancelled at.
            type Consumer = (
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
            );
            let consumers: Vec<Consumer> = sqlx::query_as(
                "SELECT origin,request_key,run_id,result_id,cancelled_at FROM check_consumer WHERE task_id='evidence' ORDER BY created_at",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            let steps: Vec<(String, String, Option<String>, i64)> = sqlx::query_as(
                "SELECT kind,status,last_error,attempts FROM task_step WHERE task_id='evidence' ORDER BY created_at",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            panic!(
                "moved={moved}: no head-check evidence\ndiffering fields {differing_fields:?}\nidentity inputs {identities:#?}\nprobes {probes}\nreviews {reviews:?}\nconsumers {consumers:?}\nsteps {steps:?}\nattempt ack {:?} timings {:?}",
                merged.effect_ack_json, merged.phase_timings
            );
        }
        assert_eq!(typed.execution.unwrap().integration_checks.len(), 1);
        // No wait was stated for the synchronous reuse and none is left.
        let reviews: i64 = world
            .scalar("SELECT COUNT(*) FROM review WHERE task_id=?", "evidence")
            .await;
        assert_eq!(reviews, 1, "a head check never creates a Review");
        assert_eq!(evidence["attempt_id"], admitted.id);
        assert!(!evidence["ci_steps"].as_array().unwrap().is_empty());
        assert_eq!(
            evidence["commit_sha"].as_str(),
            merged.integrated_sha.as_deref()
        );
        if moved {
            assert_ne!(evidence["commit_sha"], original);
            assert_ne!(evidence["result_id"], entry.0);
            assert_eq!(evidence["source"], "executed");
        } else {
            assert_eq!(evidence["identity_key"], entry.1);
            assert_eq!(evidence["result_id"], entry.0);
            assert_eq!(evidence["source"], "cache_hit", "{details}");
            // The reuse was applied by the `settle` step that recorded it:
            // its consumer is answered and no delivery step is spent on it,
            // now or at a later look of the check worker.
            let _ = db::CheckDeliveryRepo::enqueue_check_result_steps(&*world.db, 100)
                .await
                .unwrap();
            let head: (Option<String>, Option<String>, i64) = sqlx::query_as(
                "SELECT c.applied_at,c.delivery_step_id,(SELECT COUNT(*) FROM task_step s WHERE s.task_id=c.task_id AND s.causation_key LIKE 'check-result:'||c.id||':%') FROM check_consumer c WHERE c.task_id='evidence' AND c.origin='integration'",
            )
            .fetch_one(world.db.pool())
            .await
            .unwrap();
            assert!(head.0.is_some(), "the head's reuse is created answered");
            assert_eq!((head.1, head.2), (None, 0), "no delivery step for a reuse");
        }
        let runs: i64 = world.scalar("SELECT COUNT(*) FROM check_run WHERE id IN(SELECT run_id FROM check_consumer WHERE task_id=?)", "evidence").await;
        // Both consumers of a hit reference the one entry run. A rebased
        // commit has the entry run and exactly one new head run.
        assert_eq!(runs, if moved { 2 } else { 1 });
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            if moved { 2 } else { 1 }
        );
        let probes = crate::check_runner::policy::probe_log::count(&world.db).await;
        assert_eq!(probes, 1, "moved={moved}: review entry + merge probe once");
        eprintln!("moved={moved}: review entry + merge probes={probes}, check runs={runs}");
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_merge_fix_brief_carries_command_and_output() {
    use crate::workflow::dispatch::{
        loader, resolve_prompt_builder, BUILDER_ID_CODER_MERGE_FIX_V2,
        BUILDER_ID_WORKER_MERGE_FIX_V1,
    };
    let world = World::new().await;
    let attempt = world.add_task("brief-red", "f", "one\n").await;
    let asked = world.place(&attempt.id, "checking", 2).await;
    world.set(&attempt.id, "state='ejected',failure_kind='candidate_check_failed',failure_message='`cargo test` exited 101: parser::empty FAILED'").await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    eventually("red check reaches repair", || async {
        world.task("brief-red").await.status == "merge_failed"
    })
    .await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    for (builder, role) in [
        (BUILDER_ID_CODER_MERGE_FIX_V2, "coder"),
        (BUILDER_ID_WORKER_MERGE_FIX_V1, "worker"),
    ] {
        let context = loader::load_agent_dispatch_context(loader::DispatchContextParams {
            db: world.db.clone(),
            router: &world.service.workspace_backend_router,
            task_id: "brief-red",
            role,
            state_name: "merge_failed",
            state_config: json!({}),
            execution_policy: None,
            workflow: &workflow,
        })
        .await
        .unwrap();
        let text = resolve_prompt_builder(builder).build(&context).user;
        for expected in [
            "MERGE-CI",
            "main",
            "cargo test",
            "parser::empty FAILED",
            "merge_fix budget: 1 of 1 spent; 0 remaining",
        ] {
            assert!(text.contains(expected), "{text}");
        }
        assert!(!text.contains("manual task-worktree repair"), "{text}");
        let window = world.db.review_window("brief-red").await.unwrap();
        assert!(::review::finding_ledger(&window.reviews)
            .unwrap()
            .is_empty());
    }
}
