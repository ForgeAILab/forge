//! Real SQLite, the real server owner and real Git in temp dirs; fake Task
//! steps, fake check verdicts, fake object transfer, a clock the test moves.
use super::*;
use crate::integration_effects::{EffectOwner, EffectWorkspace};
use crate::integration_owner::ServerIntegrationOwner;
use db::{
    IntegrationAttemptState as S, IntegrationCheckTiming, IntegrationCiSkipReason,
    IntegrationEffectAdmission, IntegrationEffectRequest, IntegrationFailureKind,
    IntegrationLostRaceKind, IntegrationOperationKind,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

macro_rules! wait_for {
    ($what:literal, $done:expr) => {{
        let mut waited = 0;
        while !$done {
            waited += 1;
            assert!(waited < 6000, "timed out waiting for {}", $what);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }};
}

/// The wall clock plus an offset that only grows. It cannot run behind or
/// stand still: the storage fence (`fencing::verify`) compares a lease with
/// the real wall clock, so a lease written under this clock is live for the
/// fence for at least as long as it is for the worker. Expiry is therefore
/// always simulated by moving this clock forward past `lease_until`, and the
/// takeover is what fences the old holder (a new generation), not the fence's
/// own clock. `a_holder_that_lost_its_lease_is_fenced_by_generation` pins it.
struct TestClock(Mutex<chrono::Duration>);
impl TestClock {
    fn advance(&self, seconds: i64) {
        assert!(seconds >= 0, "the test clock only moves forward");
        *self.0.lock().unwrap() += chrono::Duration::seconds(seconds);
    }
}
#[async_trait]
impl WorkerClock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now() + *self.0.lock().unwrap()
    }
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration.min(Duration::from_millis(2))).await;
    }
}

async fn git_out(path: &Path, args: &[&str]) -> Option<String> {
    let out = git::command_output(path, args).await.ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// The Task-step side: records every step, and `answer` plays the steps the
/// way the real consumer would, writing only the attempt's acknowledgment
/// fields (and the fixture Task's status for `result`).
#[derive(Default)]
struct FakeSteps {
    requests: Mutex<Vec<IntegrationStepRequest>>,
    answered: Mutex<HashSet<String>>,
    held: Mutex<HashSet<(String, IntegrationStepAction)>>,
    red: Mutex<HashSet<String>>,
    /// Permits name another candidate (a consumer bug the worker must bound).
    bad_permit: AtomicBool,
    readied: AtomicUsize,
    /// Steps the step worker settled without applying (`task|causation key`).
    dead: Mutex<HashSet<String>>,
    /// Every deciding step of these Tasks dies instead of answering.
    dying: Mutex<HashSet<String>>,
}
impl FakeSteps {
    fn hold(&self, task: &str, action: IntegrationStepAction) {
        self.held.lock().unwrap().insert((task.into(), action));
    }
    fn unhold(&self, task: &str, action: IntegrationStepAction) {
        self.held.lock().unwrap().remove(&(task.to_owned(), action));
    }
    fn actions(&self, task: &str) -> Vec<IntegrationStepAction> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.task_id == task)
            .map(|request| request.action)
            .collect()
    }
    async fn answer(&self, db: &SqliteDb) -> bool {
        let requests = self.requests.lock().unwrap().clone();
        let mut progressed = false;
        for request in requests {
            let key = format!("{}|{}", request.task_id, request.causation_key());
            if self.answered.lock().unwrap().contains(&key)
                || self
                    .held
                    .lock()
                    .unwrap()
                    .contains(&(request.task_id.clone(), request.action))
            {
                continue;
            }
            let Some(mut a) = db.integration_attempt(&request.attempt_id).await.unwrap() else {
                continue;
            };
            let current =
                a.effect_seq == request.effect_seq && a.slot_generation == request.generation;
            let done = |progressed: &mut bool| {
                self.answered.lock().unwrap().insert(key.clone());
                *progressed = true;
            };
            let ack = match request.action {
                IntegrationStepAction::RequestCheck | IntegrationStepAction::Settle => {
                    let checked = request.action == IntegrationStepAction::RequestCheck;
                    // A step is enqueued before the transition it belongs
                    // to: it retries until its attempt waits for it, and
                    // finishes without a write once the attempt moved on.
                    let (before, waiting) = if checked {
                        (S::Rebasing, S::Checking)
                    } else {
                        (S::Validating, S::AwaitingTaskStep)
                    };
                    if !current || !(a.state == before || a.state == waiting) {
                        done(&mut progressed);
                        continue;
                    }
                    if a.state != waiting {
                        continue;
                    }
                    if self.dying.lock().unwrap().contains(&request.task_id) {
                        // The step worker settled it `failed`: no answer.
                        self.dead.lock().unwrap().insert(key.clone());
                        done(&mut progressed);
                        continue;
                    }
                    if IntegrationStepAck::current(&a, IntegrationStepAction::Settle).is_some() {
                        done(&mut progressed);
                        continue;
                    }
                    let red = checked && self.red.lock().unwrap().contains(&request.task_id);
                    if !red {
                        let candidate = if self.bad_permit.load(Ordering::SeqCst) {
                            Some("0".repeat(40))
                        } else {
                            a.candidate_sha.clone()
                        };
                        a.permit_json = Some(
                            json!({"candidate_sha":candidate,"target_tip_sha":a.target_tip_sha,"task_ref":a.task_ref,"expected_epoch":a.expected_epoch,"slot_generation":a.slot_generation}),
                        );
                    }
                    IntegrationStepAck {
                        effect_seq: request.effect_seq,
                        generation: request.generation,
                        action: IntegrationStepAction::Settle,
                        outcome: if red {
                            IntegrationStepOutcome::CandidateCheckFailed
                        } else {
                            IntegrationStepOutcome::Permit
                        },
                        check: checked.then_some(IntegrationCheckTiming::Ran {
                            slot_wait_ms: 1,
                            run_ms: 2,
                        }),
                        message: red.then(|| "tests failed".to_owned()),
                    }
                }
                IntegrationStepAction::Result => {
                    if a.state.terminal() {
                        done(&mut progressed);
                        continue;
                    }
                    if a.state != S::Applied {
                        continue;
                    }
                    sqlx::query("UPDATE task SET status='done' WHERE id=?")
                        .bind(&request.task_id)
                        .execute(db.pool())
                        .await
                        .unwrap();
                    IntegrationStepAck {
                        effect_seq: a.effect_seq,
                        generation: a.slot_generation,
                        action: IntegrationStepAction::Result,
                        outcome: IntegrationStepOutcome::Done,
                        check: None,
                        message: None,
                    }
                }
                action => {
                    let applies = match action {
                        IntegrationStepAction::SendBack => {
                            matches!(a.state, S::Ejected | S::NeedsReview)
                        }
                        IntegrationStepAction::Park => {
                            matches!(a.state, S::Parked | S::Quarantined)
                        }
                        _ => a.state == S::Cancelled,
                    };
                    if applies || a.state.terminal() {
                        done(&mut progressed);
                    }
                    continue;
                }
            };
            a.effect_ack_json = Some(json!(ack));
            a.acknowledged_at = Some(db::now_rfc3339());
            // A lost compare-and-set is a step retry.
            if db.transition_integration_attempt(a).await.is_ok() {
                done(&mut progressed);
            }
        }
        progressed
    }
}
#[async_trait]
impl IntegrationStepPort for FakeSteps {
    async fn enqueue_step(&self, request: &IntegrationStepRequest) -> Result<()> {
        let mut requests = self.requests.lock().unwrap();
        if !requests.iter().any(|existing| {
            existing.task_id == request.task_id
                && existing.causation_key() == request.causation_key()
        }) {
            requests.push(request.clone());
        }
        Ok(())
    }
    async fn ready_result_step(&self, _attempt_id: &str, _effect_seq: i64) -> Result<()> {
        self.readied.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn step_state(&self, request: &IntegrationStepRequest) -> Result<IntegrationStepState> {
        let key = format!("{}|{}", request.task_id, request.causation_key());
        Ok(if self.dead.lock().unwrap().contains(&key) {
            IntegrationStepState::Dead
        } else if self.answered.lock().unwrap().contains(&key) {
            IntegrationStepState::Done
        } else if self.requests.lock().unwrap().iter().any(|existing| {
            existing.task_id == request.task_id
                && existing.causation_key() == request.causation_key()
        }) {
            IntegrationStepState::Live
        } else {
            IntegrationStepState::Missing
        })
    }
}

#[derive(Default)]
struct FakeTransfer {
    remote: Mutex<HashSet<String>>,
    too_large: AtomicBool,
    calls: Mutex<Vec<ObjectTransferDirection>>,
    keys: Mutex<Vec<String>>,
    released: Mutex<Vec<ObjectTransferRelease>>,
}
#[async_trait]
impl ObjectTransferPort for FakeTransfer {
    async fn transfer(&self, request: ObjectTransferRequest) -> Result<ObjectTransferOutcome> {
        self.calls.lock().unwrap().push(request.direction);
        self.keys
            .lock()
            .unwrap()
            .push(api_types::object_transfer_key(
                &request.fence.attempt_id,
                request.fence.generation,
                request.direction,
            ));
        assert_eq!(request.target.repo_location_id, "l-r");
        assert_eq!(request.task.owner, EffectOwner::Server);
        assert_eq!(request.have.len(), 1);
        Ok(if self.too_large.load(Ordering::SeqCst) {
            ObjectTransferOutcome::TooLarge {
                bytes: request.max_bytes + 1,
            }
        } else {
            ObjectTransferOutcome::Transferred { bytes: 1024 }
        })
    }
    async fn release(&self, release: ObjectTransferRelease) -> Result<()> {
        self.released.lock().unwrap().push(release);
        Ok(())
    }
}

struct GitFactsFake {
    db: Arc<SqliteDb>,
    transfer: Arc<FakeTransfer>,
    fail: AtomicBool,
    paused: AtomicBool,
}
#[async_trait]
impl IntegrationFactsPort for GitFactsFake {
    async fn head_facts(
        &self,
        attempt: &IntegrationAttempt,
        queue: &IntegrationQueue,
    ) -> Result<HeadFacts> {
        let unreadable = || ServiceError::invalid_operation("facts unreadable");
        if self.fail.load(Ordering::SeqCst) {
            return Err(unreadable());
        }
        let (workspace_id, tree, branch, placement_id, generation): (String, String, String, String, i64) =
            sqlx::query_as("SELECT w.id,w.worktree_path,w.branch,p.id,p.generation FROM workspace w JOIN workspace_placement p ON p.workspace_id=w.id WHERE w.task_id=?")
                .bind(&attempt.task_ref)
                .fetch_one(self.db.pool())
                .await?;
        let repo: String = sqlx::query_scalar("SELECT path FROM repo_location WHERE id=?")
            .bind(&queue.target_location_id)
            .fetch_one(self.db.pool())
            .await?;
        let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id=?")
            .bind(&attempt.task_ref)
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
        let ancestor = |a: String, b: String| {
            let repo = repo.clone();
            async move {
                git_out(&repo, &["merge-base", "--is-ancestor", &a, &b])
                    .await
                    .is_some()
            }
        };
        Ok(HeadFacts {
            gate: if status != "merging" {
                TaskGate::Left
            } else if self.paused.load(Ordering::SeqCst) {
                TaskGate::ProjectPaused
            } else {
                TaskGate::Live
            },
            task_location: ObjectTransferEndpoint {
                repo_location_id: format!("l-{}", queue.repo_id),
                owner: EffectOwner::Server,
            },
            target_in_candidate: ancestor(target_tip.clone(), candidate_head.clone()).await,
            candidate_in_target: ancestor(candidate_head.clone(), target_tip.clone()).await,
            worktree_dirty: !git::is_worktree_clean(&tree).await?,
            target_dirty: !git::is_worktree_clean(&repo).await?,
            rebase_in_progress: git::detect_rebase_in_progress(&tree).await?,
            shared_object_store: !self
                .transfer
                .remote
                .lock()
                .unwrap()
                .contains(&attempt.task_ref),
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

struct World {
    temp: tempfile::TempDir,
    db: Arc<SqliteDb>,
    clock: Arc<TestClock>,
    steps: Arc<FakeSteps>,
    transfer: Arc<FakeTransfer>,
    facts: Arc<GitFactsFake>,
}
fn config() -> IntegrationWorkerConfig {
    IntegrationWorkerConfig {
        poll: Duration::from_millis(5),
        conflict_backoff: Duration::from_millis(1),
        ..Default::default()
    }
}
impl World {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("queue.sqlite").display()
        );
        let pool = db::create_sqlite_pool(&url).await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let transfer = Arc::new(FakeTransfer::default());
        let world = Self {
            facts: Arc::new(GitFactsFake {
                db: db.clone(),
                transfer: transfer.clone(),
                fail: AtomicBool::new(false),
                paused: AtomicBool::new(false),
            }),
            temp,
            db,
            clock: Arc::new(TestClock(Mutex::new(chrono::Duration::zero()))),
            steps: Arc::new(FakeSteps::default()),
            transfer,
        };
        world.add_repo("r").await;
        world
    }
    fn repo(&self, id: &str) -> PathBuf {
        self.temp.path().join(format!("repo-{id}"))
    }
    fn tree(&self, task: &str) -> PathBuf {
        self.temp.path().join(format!("tree-{task}"))
    }
    async fn add_repo(&self, id: &str) {
        let repo = self.repo(id);
        std::fs::create_dir(&repo).unwrap();
        git::init(&repo).await.unwrap();
        std::fs::write(repo.join("base"), "base\n").unwrap();
        git::commit_all(&repo, "initial").await.unwrap();
        git::checkout_branch(&repo, "main").await.unwrap();
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES(?,'p',?,?,'main',?,?)").bind(id).bind(id).bind(repo.to_str()).bind(&now).bind(&now).execute(self.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES(?,?,'server',?,'primary_checkout',1,'ready',?,?)").bind(format!("l-{id}")).bind(id).bind(repo.to_str()).bind(&now).bind(&now).execute(self.db.pool()).await.unwrap();
    }
    /// A Task in `merging` with one commit writing `file`, admitted to the
    /// `main` queue of `repo`.
    async fn add_task(
        &self,
        repo_id: &str,
        task: &str,
        file: &str,
        content: &str,
    ) -> IntegrationAttempt {
        let (repo, tree) = (self.repo(repo_id), self.tree(task));
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
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p',?,'merging',?,?)").bind(task).bind(task).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?)").bind(format!("w-{task}")).bind(task).bind(repo_id).bind(tree.to_str()).bind(&branch).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server',?,?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(format!("l-{repo_id}")).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
        let queue = self
            .db
            .create_or_get_integration_queue(repo_id, "main")
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
    fn worker_with(&self, config: IntegrationWorkerConfig) -> Arc<IntegrationQueueWorker> {
        Arc::new(IntegrationQueueWorker::new(
            self.db.clone(),
            Arc::new(ServerIntegrationOwner::new(self.db.clone())),
            self.steps.clone(),
            self.facts.clone(),
            self.transfer.clone(),
            self.clock.clone(),
            config,
        ))
    }
    fn worker(&self) -> Arc<IntegrationQueueWorker> {
        self.worker_with(config())
    }
    async fn attempt(&self, id: &str) -> IntegrationAttempt {
        self.db.integration_attempt(id).await.unwrap().unwrap()
    }
    async fn tip(&self, repo: &str) -> String {
        git_out(&self.repo(repo), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap()
    }
    async fn external_commit(&self, repo: &str, file: &str) {
        std::fs::write(self.repo(repo).join(file), file).unwrap();
        git::commit_all(&self.repo(repo), file).await.unwrap();
    }
    async fn queue_of(&self, attempt: &IntegrationAttempt) -> IntegrationQueue {
        self.db
            .integration_queue(attempt.queue_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
    /// Sweep, step every driver and play the Task steps until nothing moves.
    /// `Some(error)` is a driver stopped by an injected fault (a "crash").
    async fn pump(&self, pump: &mut Pump) -> Option<ServiceError> {
        for _ in 0..400 {
            let mut progressed = false;
            match pump.worker.sweep_once().await {
                Ok(claimed) => {
                    progressed |= !claimed.is_empty();
                    pump.drivers.extend(claimed);
                }
                Err(error) => return Some(error),
            }
            let mut index = 0;
            while index < pump.drivers.len() {
                let mut released = false;
                for _ in 0..60 {
                    match pump.drivers[index].pass().await {
                        Ok(Pass::Progress) => progressed = true,
                        Ok(Pass::Wait(_)) => break,
                        Ok(Pass::Released | Pass::Lost) => {
                            released = true;
                            progressed = true;
                            break;
                        }
                        // The stopped driver stays: it still holds the lease.
                        Err(error) => return Some(error),
                    }
                }
                if released {
                    pump.drivers.remove(index);
                } else {
                    index += 1;
                }
            }
            progressed |= self.steps.answer(&self.db).await;
            if !progressed {
                return None;
            }
        }
        panic!("the worker did not settle");
    }
    async fn settle(&self, pump: &mut Pump) {
        if let Some(error) = self.pump(pump).await {
            panic!("unexpected driver error: {error}");
        }
    }
}
struct Pump {
    worker: Arc<IntegrationQueueWorker>,
    drivers: Vec<HeadDriver>,
}
impl Pump {
    fn new(worker: Arc<IntegrationQueueWorker>) -> Self {
        Self {
            worker,
            drivers: Vec::new(),
        }
    }
}
fn arm(worker: &IntegrationQueueWorker, state: S, point: CrashPoint) -> Arc<AtomicBool> {
    let fired = Arc::new(AtomicBool::new(false));
    let flag = fired.clone();
    worker.set_fault(Some(Arc::new(move |at, here| {
        at == state && here == point && !flag.swap(true, Ordering::SeqCst)
    })));
    fired
}
async fn ff_successes(world: &World, attempt: &str) -> usize {
    let a = world.attempt(attempt).await;
    serde_json::from_value::<Vec<db::IntegrationEffectReceipt>>(a.effect_receipts_json)
        .unwrap()
        .into_iter()
        .filter(|receipt| {
            receipt.request.kind == IntegrationOperationKind::FastForward
                && receipt.operation_state == db::IntegrationOperationState::Succeeded
        })
        .count()
}

#[test]
fn head_table_is_total_and_inside_the_storage_graph() {
    for state in S::ALL {
        let row = head_row(*state);
        for to in row.success.iter().chain(row.failure).chain(&row.on_timeout) {
            assert!(
                state.exits().contains(to),
                "{state} -> {to} is not a storage transition"
            );
        }
        assert_eq!(
            row.timeout == HeadTimeout::None,
            row.on_timeout.is_none(),
            "{state}: a timeout names its edge"
        );
        // Every non-terminal state has a way out that the worker performs.
        assert!(
            state.terminal() || !row.success.is_empty() || !row.failure.is_empty(),
            "{state} has no exit"
        );
        // The cancel column agrees with what storage lets a Task step ask.
        assert_eq!(
            row.allows(S::Cancelled),
            matches!(row.cancel, CancelRule::Now | CancelRule::AfterReceipt),
            "{state}"
        );
        assert!(
            !row.allows(S::Cancelled) || state.cancel_requestable(),
            "{state}"
        );
    }
    assert_eq!(HEAD_TABLE.len(), S::ALL.len());
}

#[test]
fn worker_sources_stay_inside_the_boundary() {
    for (name, source) in [
        ("mod", include_str!("mod.rs")),
        ("driver", include_str!("driver.rs")),
        ("ports", include_str!("ports.rs")),
        ("table", include_str!("table.rs")),
    ] {
        let code = source
            .lines()
            .map(|line| line.split("//").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            "git::",
            "sqlx::",
            ".pool()",
            "Command::",
            "events::",
            "EventBus",
            "TaskRepo",
            "TaskStepRepo",
            "TaskService",
            "ReviewRepo",
            "MergeService",
            "ExecutionRepo",
            "task_service::",
            "task_writer",
            "workflow::",
            "check_runner::",
            "state_integration_condition",
            "record_integration_observation",
            "observe_integration_",
            "admit_integration_attempt",
            "supersede_integration_attempt",
            "request_integration_cancel",
            "UPDATE ",
            "INSERT ",
            "DELETE ",
        ] {
            assert!(
                !code.contains(forbidden),
                "integration_worker/{name}.rs acquired forbidden capability {forbidden}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_target_merges_the_reviewed_commit_without_a_check() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let candidate = a.original_candidate_sha.clone().unwrap();
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let done = world.attempt(&a.id).await;
    assert_eq!(done.state, S::Completed);
    assert_eq!(world.tip("r").await, candidate);
    assert_eq!(done.integrated_sha.as_deref(), Some(candidate.as_str()));
    assert!(done.started_at.is_some());
    assert_eq!(
        world.steps.actions("a"),
        vec![IntegrationStepAction::Settle, IntegrationStepAction::Result]
    );
    let timings = done.phase_timings.unwrap();
    assert_eq!(
        timings.check,
        Some(IntegrationCheckTiming::Skipped {
            reason: IntegrationCiSkipReason::TargetUnchanged
        })
    );
    assert_eq!(timings.rounds, 1);
    assert!(timings.queued_ms.is_some() && timings.ff_ms.is_some());
    assert!(timings.head_total_ms.is_some() && timings.lost_races.is_empty());
    let queue = world.queue_of(&a).await;
    assert!(queue.head_attempt_id.is_none() && queue.lease_owner.is_none());
    assert!(world.steps.readied.load(Ordering::SeqCst) >= 1);
    // Snapshot and enqueue ports.
    let snapshot = pump
        .worker
        .integration_queue_snapshot(&queue.id, 10)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.members.is_empty() && !snapshot.driven_here);
    pump.worker.notify_enqueued(&queue.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moved_target_is_rebased_checked_and_fast_forwarded_in_one_pass_per_receipt() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    let mut pump = Pump::new(world.worker());
    // Members behind the head are shown in order.
    world.steps.hold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    let snapshot = pump
        .worker
        .integration_queue_snapshot(a.queue_id.as_deref().unwrap(), 10)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.driven_here);
    assert_eq!(
        snapshot
            .members
            .iter()
            .map(|member| (member.task_id.as_str(), member.position))
            .collect::<Vec<_>>(),
        vec![("a", Some(0)), ("b", Some(1))]
    );
    world.steps.unhold("a", IntegrationStepAction::Settle);
    // Stop b before its rebase, then run exactly one pass: receipt and
    // transition happen in it although the receipt bumped the revision (R13).
    arm(&pump.worker, S::Rebasing, CrashPoint::BeforeEffect);
    assert!(world.pump(&mut pump).await.is_some());
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(world.attempt(&b.id).await.state, S::Rebasing);
    pump.worker.set_fault(None);
    let mut driver = pump.drivers.pop().expect("the stopped driver");
    assert_eq!(driver.pass().await.unwrap(), Pass::Progress);
    let checking = world.attempt(&b.id).await;
    assert_eq!(checking.state, S::Checking);
    assert_ne!(checking.candidate_sha, checking.original_candidate_sha);
    pump.drivers.push(driver);
    world.settle(&mut pump).await;
    let done = world.attempt(&b.id).await;
    assert_eq!(done.state, S::Completed);
    assert_eq!(Some(world.tip("r").await), done.candidate_sha);
    assert_eq!(
        world.steps.actions("b"),
        vec![
            IntegrationStepAction::RequestCheck,
            IntegrationStepAction::Result
        ]
    );
    let timings = done.phase_timings.unwrap();
    assert!(matches!(
        timings.check,
        Some(IntegrationCheckTiming::Ran { .. })
    ));
    assert!(timings.rebase_ms.is_some());
    // Caught up with an earlier queue member: recorded, and free.
    assert_eq!(timings.lost_races.len(), 1);
    assert_eq!(
        timings.lost_races[0].kind,
        IntegrationLostRaceKind::QueueMember
    );
    assert_eq!(timings.external_target_moves(), 0);
    assert_eq!(ff_successes(&world, &b.id).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_and_red_check_eject_with_handoff_data_and_free_the_slot() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    let d = world.add_task("r", "d", "d.txt", "d\n").await;
    world.steps.red.lock().unwrap().insert("c".into());
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let conflicted = world.attempt(&b.id).await;
    assert_eq!(conflicted.state, S::Ejected);
    assert_eq!(conflicted.conflict_paths_json, Some(json!(["base"])));
    assert!(conflicted.current);
    assert_eq!(
        world.steps.actions("b"),
        vec![IntegrationStepAction::SendBack]
    );
    let red = world.attempt(&c.id).await;
    assert_eq!(red.state, S::Ejected);
    assert_eq!(
        red.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
    assert_eq!(red.failure_message.as_deref(), Some("tests failed"));
    assert_eq!(
        world.steps.actions("c"),
        vec![
            IntegrationStepAction::RequestCheck,
            IntegrationStepAction::SendBack
        ]
    );
    // The member behind both ejections still merged.
    let last = world.attempt(&d.id).await;
    assert_eq!(last.state, S::Completed);
    assert_eq!(Some(world.tip("r").await), last.candidate_sha);
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_target_moves_spend_the_allowance_and_the_sixth_parks() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        max_rounds: 50,
        ..config()
    }));
    let deciding = [
        IntegrationStepAction::Settle,
        IntegrationStepAction::RequestCheck,
    ];
    deciding
        .iter()
        .for_each(|action| world.steps.hold("a", *action));
    for round in 1..=6 {
        // The head holds the slot and waits for its permit; a writer outside
        // Forge moves the target under it; then the permit arrives and the
        // fast-forward finds the moved target.
        world.settle(&mut pump).await;
        let waiting = world.attempt(&a.id).await;
        assert!(
            matches!(waiting.state, S::AwaitingTaskStep | S::Checking),
            "round {round}: {}",
            waiting.state
        );
        world
            .external_commit("r", &format!("outside-{round}"))
            .await;
        deciding
            .iter()
            .for_each(|action| world.steps.unhold("a", *action));
        assert!(world.steps.answer(&world.db).await);
        deciding
            .iter()
            .for_each(|action| world.steps.hold("a", *action));
        world.settle(&mut pump).await;
        if round < 6 {
            let moved = world.attempt(&a.id).await.phase_timings.unwrap();
            assert_eq!(moved.external_target_moves(), round, "round {round}");
        }
    }
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(parked
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("external_target_moves_exhausted"));
    assert!(parked.available_at.is_none());
    let timings = parked.phase_timings.unwrap();
    assert_eq!(timings.external_target_moves(), 6);
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

/// Bring one head to `state` and stop there.
async fn hold_in(world: &World, pump: &mut Pump, state: S) -> IntegrationAttempt {
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    if matches!(state, S::Rebasing | S::Checking) {
        world.external_commit("r", "outside").await;
    }
    match state {
        S::Validating => {
            arm(&pump.worker, S::Queued, CrashPoint::AfterTransition);
        }
        S::Rebasing => {
            arm(&pump.worker, S::Rebasing, CrashPoint::BeforeEffect);
        }
        S::Checking => world.steps.hold("a", IntegrationStepAction::RequestCheck),
        S::AwaitingTaskStep => world.steps.hold("a", IntegrationStepAction::Settle),
        S::ReadyFf => {
            arm(&pump.worker, S::ReadyFf, CrashPoint::BeforeTransition);
        }
        S::FfInflight => {
            arm(&pump.worker, S::FfInflight, CrashPoint::BeforeEffect);
        }
        S::Applied => world.steps.hold("a", IntegrationStepAction::Result),
        _ => unreachable!(),
    }
    let _ = world.pump(pump).await;
    pump.worker.set_fault(None);
    let held = world.attempt(&a.id).await;
    assert_eq!(held.state, state);
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_in_each_cancellable_head_state_releases_without_a_merge() {
    for state in [
        S::Validating,
        S::Rebasing,
        S::Checking,
        S::AwaitingTaskStep,
        S::ReadyFf,
    ] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let held = hold_in(&world, &mut pump, state).await;
        let tip = world.tip("r").await;
        world
            .db
            .request_integration_cancel(&held.id, held.revision, &db::now_rfc3339())
            .await
            .unwrap_or_else(|error| panic!("{state}: {error}"));
        world.settle(&mut pump).await;
        let cancelled = world.attempt(&held.id).await;
        assert_eq!(cancelled.state, S::Cancelled, "{state}");
        assert!(cancelled.effect_intent_json.is_none(), "{state}");
        assert_eq!(world.tip("r").await, tip, "{state}");
        assert!(
            world
                .steps
                .actions("a")
                .contains(&IntegrationStepAction::Clear),
            "{state}"
        );
        assert!(
            world.queue_of(&held).await.head_attempt_id.is_none(),
            "{state}"
        );
    }
    // Critical states refuse the request; the merge runs to its result.
    for state in [S::FfInflight, S::Applied] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let held = hold_in(&world, &mut pump, state).await;
        assert!(matches!(
            world
                .db
                .request_integration_cancel(&held.id, held.revision, &db::now_rfc3339())
                .await,
            Err(db::DbError::InvalidTransition)
        ));
        world.steps.unhold("a", IntegrationStepAction::Result);
        world.settle(&mut pump).await;
        assert_eq!(world.attempt(&held.id).await.state, S::Completed, "{state}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_of_released_and_waiting_members_is_applied_by_the_sweep() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "base", "from a\n").await;
    let b = world.add_task("r", "b", "base", "from b\n").await;
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    let mut pump = Pump::new(world.worker());
    // c waits behind a held head; b will be ejected by its conflict.
    world.steps.hold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    let queued = world.attempt(&c.id).await;
    assert_eq!(queued.state, S::Queued);
    world
        .db
        .request_integration_cancel(&queued.id, queued.revision, &db::now_rfc3339())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&c.id).await.state, S::Cancelled);
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    let ejected = world.attempt(&b.id).await;
    assert_eq!(ejected.state, S::Ejected);
    world
        .db
        .request_integration_cancel(&ejected.id, ejected.revision, &db::now_rfc3339())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&b.id).await.state, S::Cancelled);
    for task in ["b", "c"] {
        assert!(world
            .steps
            .actions(task)
            .contains(&IntegrationStepAction::Clear));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_against_the_permit_has_exactly_one_winner() {
    // Both orders; the real race is the next test.
    for order in ["cancel_first", "commit_first"] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let ready = hold_in(&world, &mut pump, S::ReadyFf).await;
        assert!(ready.permit_json.is_some());
        let tip = world.tip("r").await;
        let mut driver = pump.drivers.pop().unwrap();
        let cancel = || async {
            world
                .db
                .request_integration_cancel(&ready.id, ready.revision, &db::now_rfc3339())
                .await
        };
        let cancelled = match order {
            "cancel_first" => {
                let cancelled = cancel().await;
                driver.pass().await.unwrap();
                cancelled
            }
            "commit_first" => {
                assert_eq!(driver.pass().await.unwrap(), Pass::Progress);
                assert_eq!(world.attempt(&ready.id).await.state, S::FfInflight);
                cancel().await
            }
            _ => unreachable!(),
        };
        pump.drivers.push(driver);
        world.settle(&mut pump).await;
        let end = world.attempt(&ready.id).await;
        match cancelled {
            Ok(_) => {
                assert_eq!(end.state, S::Cancelled, "{order}");
                assert_eq!(world.tip("r").await, tip, "{order}");
                assert_eq!(ff_successes(&world, &ready.id).await, 0, "{order}");
            }
            Err(_) => {
                assert_eq!(end.state, S::Completed, "{order}");
                assert_eq!(Some(world.tip("r").await), end.candidate_sha, "{order}");
            }
        }
        match order {
            "cancel_first" => assert_eq!(end.state, S::Cancelled),
            "commit_first" => assert_eq!(end.state, S::Completed),
            _ => {}
        }
    }
}

/// The cancel request runs on another runtime thread, released together with
/// the driver's cancel-or-commit pass. Whatever the interleaving: a granted
/// cancel means no fast-forward, a refused one means the merge completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_racing_the_permit_on_another_thread_has_exactly_one_winner() {
    let (mut cancels, mut merges) = (0, 0);
    for round in 0..10 {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let ready = hold_in(&world, &mut pump, S::ReadyFf).await;
        let tip = world.tip("r").await;
        let mut driver = pump.drivers.pop().unwrap();
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let cancel = tokio::spawn({
            let (db, gate, ready) = (world.db.clone(), gate.clone(), ready.clone());
            async move {
                gate.wait().await;
                // Vary who gets to the row first.
                for _ in 0..round % 4 {
                    tokio::task::yield_now().await;
                }
                db.request_integration_cancel(&ready.id, ready.revision, &db::now_rfc3339())
                    .await
            }
        });
        let pass = tokio::spawn(async move {
            gate.wait().await;
            let pass = driver.pass().await;
            (driver, pass)
        });
        let cancelled = cancel.await.unwrap();
        let (driver, pass) = pass.await.unwrap();
        pass.unwrap();
        pump.drivers.push(driver);
        world.settle(&mut pump).await;
        let end = world.attempt(&ready.id).await;
        match cancelled {
            Ok(_) => {
                cancels += 1;
                assert_eq!(end.state, S::Cancelled, "round {round}");
                assert_eq!(world.tip("r").await, tip, "round {round}");
                assert!(receipts_of(&world, &ready.id).await.is_empty());
            }
            // Lost the compare-and-set to the commit, or refused after it.
            Err(db::DbError::VersionConflict | db::DbError::InvalidTransition) => {
                merges += 1;
                assert_eq!(end.state, S::Completed, "round {round}");
                assert_eq!(Some(world.tip("r").await), end.candidate_sha);
                assert_eq!(ff_successes(&world, &ready.id).await, 1);
            }
            Err(error) => panic!("round {round}: {error}"),
        }
    }
    assert_eq!(cancels + merges, 10);
}

/// A real `git rebase` is running (held by a `pre-rebase` hook) when the
/// cancel request arrives: the driver stops the effect, the owner records
/// the cancelled receipt, and only then is the head released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_a_running_rebase_stops_git_and_releases_after_the_receipt() {
    use std::os::unix::fs::PermissionsExt;
    let world = World::new().await;
    let mut pump = Pump::new(world.worker());
    let held = hold_in(&world, &mut pump, S::Rebasing).await;
    let candidate = held.candidate_sha.clone().unwrap();
    let tip = world.tip("r").await;
    let (running, hook) = (
        world.temp.path().join("rebase-running"),
        world.repo("r").join(".git/hooks/pre-rebase"),
    );
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    std::fs::write(
        &hook,
        format!(
            "#!/bin/sh\ntouch '{}'\nwhile :; do sleep 0.05; done\n",
            running.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut driver = pump.drivers.pop().unwrap();
    let pass = tokio::spawn(async move {
        let pass = driver.pass().await;
        (driver, pass)
    });
    wait_for!("git rebase to run", running.exists());
    let running_attempt = world.attempt(&held.id).await;
    assert_eq!(running_attempt.state, S::Rebasing);
    assert_eq!(running_attempt.effect_intent_json.unwrap()["started"], true);
    assert!(!pass.is_finished());
    world
        .db
        .request_integration_cancel(&held.id, running_attempt.revision, &db::now_rfc3339())
        .await
        .unwrap();
    let (driver, pass) = tokio::time::timeout(Duration::from_secs(20), pass)
        .await
        .expect("the cancel reaches the running rebase")
        .unwrap();
    assert_eq!(pass.unwrap(), Pass::Released);
    drop(driver);
    let end = world.attempt(&held.id).await;
    assert_eq!(end.state, S::Cancelled);
    assert!(end.effect_intent_json.is_none());
    let receipts = receipts_of(&world, &held.id).await;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].request.kind, IntegrationOperationKind::Rebase);
    assert_eq!(receipts[0].result["kind"], "cancelled");
    assert_eq!(
        receipts[0].operation_state,
        db::IntegrationOperationState::Failed
    );
    // Git was stopped before it changed anything.
    assert_eq!(
        git_out(&world.tree("a"), &["rev-parse", "HEAD"]).await,
        Some(candidate)
    );
    assert!(!git::detect_rebase_in_progress(&world.tree("a"))
        .await
        .unwrap());
    assert_eq!(world.tip("r").await, tip);
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Clear));
    assert!(world.queue_of(&held).await.head_attempt_id.is_none());
    std::fs::remove_file(&hook).unwrap();
}

/// The process died inside the rebase: the intent is started and has no
/// receipt. The owner settles it from Git under its own lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_of_a_started_rebase_is_settled_by_the_owner_from_git() {
    for ran in [false, true] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = hold_in(&world, &mut pump, S::Rebasing).await;
        started_intent(&world, &a, IntegrationOperationKind::Rebase).await;
        if ran {
            // Git finished; the receipt was never written.
            git_out(
                &world.tree("a"),
                &["rebase", a.target_tip_sha.as_deref().unwrap()],
            )
            .await
            .unwrap();
        }
        drop(pump);
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        let mut drivers = pump.worker.sweep_once().await.unwrap();
        assert_eq!(world.attempt(&a.id).await.state, S::Reconciling);
        pump.drivers.append(&mut drivers);
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, S::Completed, "ran={ran}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha);
        let receipts = receipts_of(&world, &a.id).await;
        let kinds: Vec<_> = receipts
            .iter()
            .map(|receipt| (receipt.request.kind, receipt.result["kind"].to_string()))
            .collect();
        use IntegrationOperationKind::{FastForward, Rebase};
        let expected = if ran {
            // The finished rebase is proven and adopted: no second rebase.
            vec![(Rebase, "\"completed\""), (FastForward, "\"completed\"")]
        } else {
            vec![
                (Rebase, "\"not_performed\""),
                (Rebase, "\"completed\""),
                (FastForward, "\"completed\""),
            ]
        };
        assert_eq!(
            kinds,
            expected
                .into_iter()
                .map(|(kind, result)| (kind, result.to_owned()))
                .collect::<Vec<_>>(),
            "ran={ran}"
        );
        // The rebased commit was checked before it merged.
        assert!(world
            .steps
            .actions("a")
            .contains(&IntegrationStepAction::RequestCheck));
        assert_eq!(
            world.queue_of(&a).await.state,
            db::IntegrationQueueState::Open
        );
    }
}

/// A permit that does not bind the head is asked for again, but not for
/// ever: each ask is a Task step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permit_that_never_binds_parks_after_a_bounded_number_of_asks() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.bad_permit.store(true, Ordering::SeqCst);
    let mut pump = Pump::new(world.worker());
    // A refused permit ends a pump round (the driver waits one poll).
    for _ in 0..6 {
        world.settle(&mut pump).await;
    }
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    let asks = world
        .steps
        .actions("a")
        .iter()
        .filter(|action| **action == IntegrationStepAction::Settle)
        .count();
    assert_eq!(asks, 3);
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_that_lost_its_lease_is_fenced_by_generation() {
    let world = World::new().await;
    world.steps.hold("a", IntegrationStepAction::Settle);
    let mut old = Pump::new(world.worker());
    let a = hold_in(&world, &mut old, S::AwaitingTaskStep).await;
    let old_fence = world
        .db
        .integration_owner_fence(&a.id)
        .await
        .unwrap()
        .unwrap();
    // The old lease runs out on the worker clock and another worker claims.
    world.clock.advance(120);
    let mut new = Pump::new(world.worker());
    let mut drivers = new.worker.sweep_once().await.unwrap();
    assert_eq!(drivers.len(), 1);
    assert_eq!(drivers[0].generation(), old_fence.generation + 1);
    new.drivers.append(&mut drivers);
    // The old driver stops without a write, and the storage fence refuses
    // its generation whatever the wall clock says about the old lease.
    let before = world.attempt(&a.id).await.revision;
    assert_eq!(old.drivers[0].pass().await.unwrap(), Pass::Lost);
    assert_eq!(world.attempt(&a.id).await.revision, before);
    let stale = IntegrationEffectRequest {
        fence: old_fence,
        kind: IntegrationOperationKind::Rebase,
        witness: json!({"workspace":{"handle":"x"}}),
    };
    assert!(matches!(
        world.db.begin_integration_effect(stale).await.unwrap(),
        IntegrationEffectAdmission::Refused(db::IntegrationEffectRefusal::StaleFence)
    ));
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.settle(&mut new).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_suspended_queue_tells_its_members_once_and_reopens_when_the_target_is_ready() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    for _ in 0..3 {
        world.settle(&mut pump).await;
        world.clock.advance(30);
    }
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.state, db::IntegrationQueueState::Suspended);
    assert_eq!(
        queue.last_error_kind,
        Some(IntegrationFailureKind::TargetUnavailable)
    );
    for task in ["a", "b"] {
        assert_eq!(
            world.steps.actions(task),
            vec![IntegrationStepAction::Park],
            "{task}"
        );
    }
    assert_eq!(world.attempt(&a.id).await.state, S::Queued);
    sqlx::query("UPDATE repo_location SET status='ready' WHERE id='l-r'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.settle(&mut pump).await;
    assert_eq!(
        world.queue_of(&a).await.state,
        db::IntegrationQueueState::Open
    );
    for attempt in [&a, &b] {
        assert_eq!(world.attempt(&attempt.id).await.state, S::Completed);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quarantined_queue_without_a_head_reopens_only_when_nothing_ran() {
    let world = World::new().await;
    let imported = world.add_task("r", "imported", "i.txt", "i\n").await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    // What the importer leaves: a quarantined member, a quarantined queue,
    // no head and no lease.
    sqlx::query("UPDATE integration_attempt SET state='quarantined',current_operation_state='uncertain' WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE integration_queue SET state='quarantined'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let mut pump = Pump::new(world.worker());
    // An unknown result in the queue: it stays shut, without a write loop.
    world.settle(&mut pump).await;
    let shut = world.queue_of(&b).await;
    assert_eq!(shut.state, db::IntegrationQueueState::Quarantined);
    world.settle(&mut pump).await;
    assert_eq!(world.queue_of(&b).await.revision, shut.revision);
    assert_eq!(world.attempt(&b.id).await.state, S::Queued);
    // The lookup resolved it: nothing was ever admitted for that member.
    sqlx::query("UPDATE integration_attempt SET current_operation_state=NULL WHERE id=?")
        .bind(&imported.id)
        .execute(world.db.pool())
        .await
        .unwrap();
    world.clock.advance(61);
    world.settle(&mut pump).await;
    assert_eq!(
        world.queue_of(&b).await.state,
        db::IntegrationQueueState::Open
    );
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    // The pinned attempt is its owner's to retry.
    assert_eq!(world.attempt(&imported.id).await.state, S::Quarantined);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_project_parks_with_backoff_and_one_task_step() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.facts.paused.store(true, Ordering::SeqCst);
    let mut pump = Pump::new(world.worker());
    let mut gaps = Vec::new();
    for _ in 0..4 {
        world.settle(&mut pump).await;
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked);
        assert!(parked
            .failure_message
            .as_deref()
            .unwrap()
            .starts_with("project_paused"));
        let due = parse_time(parked.available_at.as_deref()).unwrap();
        gaps.push((due - world.clock.now()).num_seconds());
        world.clock.advance(gaps.last().unwrap() + 1);
    }
    assert!(
        gaps.windows(2).all(|pair| pair[1] > pair[0]),
        "the retry backs off: {gaps:?}"
    );
    // A wait with no end of its own tells the Task once, not on each retry.
    assert_eq!(world.steps.actions("a"), vec![IntegrationStepAction::Park]);
    world.facts.paused.store(false, Ordering::SeqCst);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

/// state x crash point -> expected recovery. `moved`: the target moved
/// before the head got the slot, so it has to rebase and check.
const CRASH_MATRIX: &[(bool, S, CrashPoint, S)] = &[
    (false, S::Queued, CrashPoint::BeforeTransition, S::Completed),
    (false, S::Queued, CrashPoint::AfterTransition, S::Completed),
    (
        false,
        S::Validating,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (
        false,
        S::Validating,
        CrashPoint::AfterStepEnqueue,
        S::Completed,
    ),
    (
        false,
        S::Validating,
        CrashPoint::AfterTransition,
        S::Completed,
    ),
    (
        false,
        S::AwaitingTaskStep,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (
        false,
        S::AwaitingTaskStep,
        CrashPoint::AfterTransition,
        S::Completed,
    ),
    (
        false,
        S::ReadyFf,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (false, S::ReadyFf, CrashPoint::AfterTransition, S::Completed),
    (false, S::FfInflight, CrashPoint::BeforeEffect, S::Completed),
    (
        false,
        S::FfInflight,
        CrashPoint::AfterEffectReceipt,
        S::Completed,
    ),
    (
        false,
        S::FfInflight,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (
        false,
        S::FfInflight,
        CrashPoint::AfterTransition,
        S::Completed,
    ),
    (
        false,
        S::Applied,
        CrashPoint::AfterStepEnqueue,
        S::Completed,
    ),
    (
        false,
        S::Applied,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (false, S::Applied, CrashPoint::AfterTransition, S::Completed),
    (true, S::Rebasing, CrashPoint::BeforeEffect, S::Completed),
    (
        true,
        S::Rebasing,
        CrashPoint::AfterEffectReceipt,
        S::Completed,
    ),
    (
        true,
        S::Rebasing,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (
        true,
        S::Rebasing,
        CrashPoint::AfterStepEnqueue,
        S::Completed,
    ),
    (true, S::Rebasing, CrashPoint::AfterTransition, S::Completed),
    (
        true,
        S::Checking,
        CrashPoint::BeforeTransition,
        S::Completed,
    ),
    (true, S::Checking, CrashPoint::AfterTransition, S::Completed),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_matrix_every_state_and_await_point_recovers_after_takeover() {
    for (moved, state, point, expected) in CRASH_MATRIX {
        let label = format!("moved={moved} {state}/{point:?}");
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = world.add_task("r", "a", "a.txt", "a\n").await;
        if *moved {
            world.external_commit("r", "outside").await;
        }
        let fired = arm(&pump.worker, *state, *point);
        let crashed = world.pump(&mut pump).await;
        assert!(fired.load(Ordering::SeqCst), "{label}: never reached");
        assert!(crashed.is_some(), "{label}: no crash");
        drop(pump);
        // The process is gone; its lease runs out; another one takes over.
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, *expected, "{label}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha, "{label}");
        assert!(end.effect_intent_json.is_none(), "{label}");
        assert!(ff_successes(&world, &a.id).await <= 1, "{label}");
        let commits = git_out(&world.repo("r"), &["rev-list", "--count", "main"])
            .await
            .unwrap();
        assert_eq!(commits, if *moved { "3" } else { "2" }, "{label}");
        let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id='a'")
            .fetch_one(world.db.pool())
            .await
            .unwrap();
        assert_eq!(status, "done", "{label}");
        assert!(
            world.queue_of(&a).await.head_attempt_id.is_none(),
            "{label}"
        );
    }
}

/// An intent admitted and started, as the owner leaves it when the process
/// dies inside the effect.
async fn started_intent(world: &World, a: &IntegrationAttempt, kind: IntegrationOperationKind) {
    let a = world.attempt(&a.id).await;
    let queue = world.queue_of(&a).await;
    let facts = world.facts.head_facts(&a, &queue).await.unwrap();
    let (head, target) = (
        a.candidate_sha.clone().unwrap(),
        a.target_tip_sha.clone().unwrap(),
    );
    let witness = if kind == IntegrationOperationKind::Rebase {
        json!({"workspace":facts.workspace,"target_branch":"main","expected_head_sha":head,"expected_target_sha":target,"handoff_conflicts":true,"deadline_nanos":"120000000000"})
    } else {
        json!({"workspace":facts.workspace,"target_branch":"main","task_branch":facts.task_branch,"expected_head_sha":head,"expected_target_sha":target,"reviewed":{"commit_sha":head,"base_sha":target},"deadline_nanos":null})
    };
    let request = IntegrationEffectRequest {
        fence: world
            .db
            .integration_owner_fence(&a.id)
            .await
            .unwrap()
            .unwrap(),
        kind,
        witness,
    };
    let IntegrationEffectAdmission::Started(mut guard) =
        world.db.begin_integration_effect(request).await.unwrap()
    else {
        panic!("not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
}
async fn started_ff_intent(world: &World, a: &IntegrationAttempt) {
    started_intent(world, a, IntegrationOperationKind::FastForward).await;
}
async fn receipts_of(world: &World, attempt: &str) -> Vec<db::IntegrationEffectReceipt> {
    serde_json::from_value(world.attempt(attempt).await.effect_receipts_json).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_of_an_inflight_fast_forward_reconciles_applied_or_not_performed() {
    for landed in [false, true] {
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = hold_in(&world, &mut pump, S::FfInflight).await;
        started_ff_intent(&world, &a).await;
        if landed {
            // Git ran; the receipt was never written.
            git_out(
                &world.repo("r"),
                &["merge", "--ff-only", a.candidate_sha.as_deref().unwrap()],
            )
            .await
            .unwrap();
        }
        drop(pump);
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        // The claim alone forces reconciliation.
        let mut drivers = pump.worker.sweep_once().await.unwrap();
        assert_eq!(world.attempt(&a.id).await.state, S::Reconciling);
        pump.drivers.append(&mut drivers);
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, S::Completed, "landed={landed}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha);
        // Proven applied: no second fast-forward. Proven not performed: one.
        assert_eq!(ff_successes(&world, &a.id).await, 1, "landed={landed}");
        let receipts: Vec<db::IntegrationEffectReceipt> =
            serde_json::from_value(end.effect_receipts_json).unwrap();
        assert_eq!(
            receipts.len(),
            if landed { 1 } else { 2 },
            "landed={landed}"
        );
        assert_eq!(
            world.queue_of(&a).await.state,
            db::IntegrationQueueState::Open
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_fast_forward_quarantines_and_reopens_only_with_a_witness() {
    let world = World::new().await;
    let mut pump = Pump::new(world.worker());
    let a = hold_in(&world, &mut pump, S::FfInflight).await;
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    started_ff_intent(&world, &a).await;
    git_out(
        &world.repo("r"),
        &["merge", "--ff-only", a.candidate_sha.as_deref().unwrap()],
    )
    .await
    .unwrap();
    // The checkout cannot be read: the owner has no proof either way.
    let (git_dir, hidden) = (world.repo("r").join(".git"), world.repo("r").join("hidden"));
    std::fs::rename(&git_dir, &hidden).unwrap();
    drop(pump);
    world.clock.advance(120);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let unknown = world.attempt(&a.id).await;
    assert_eq!(unknown.state, S::Quarantined);
    assert_eq!(
        unknown.failure_kind,
        Some(IntegrationFailureKind::NeedsFact)
    );
    assert!(unknown.effect_intent_json.is_some());
    let queue = world.queue_of(&a).await;
    assert_eq!(queue.state, db::IntegrationQueueState::Quarantined);
    assert_eq!(
        queue.last_error_kind,
        Some(IntegrationFailureKind::NeedsFact)
    );
    assert!(world
        .steps
        .actions("a")
        .contains(&IntegrationStepAction::Park));
    // Nothing behind it is started, and waiting does not change that.
    world.clock.advance(600);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Quarantined);
    assert_eq!(world.attempt(&b.id).await.state, S::Queued);
    // No witness, no re-open.
    let queue = world.queue_of(&a).await;
    assert!(world
        .db
        .reopen_integration_queue(
            &queue.id,
            queue.revision,
            &db::IntegrationQueueReopenWitness::NoEffect {
                attempt_id: a.id.clone()
            }
        )
        .await
        .is_err());
    // The checkout is readable again: the owner proves the exact merge, and
    // that settled receipt is the witness.
    std::fs::rename(&hidden, &git_dir).unwrap();
    world.clock.advance(600);
    world.settle(&mut pump).await;
    let end = world.attempt(&a.id).await;
    assert_eq!(end.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
    assert_eq!(
        world.queue_of(&a).await.state,
        db::IntegrationQueueState::Open
    );
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parked_infrastructure_retries_then_needs_the_owner_and_transfer_cap_parks_typed() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker());
    world.facts.fail.store(true, Ordering::SeqCst);
    for retry in 0..3 {
        world.settle(&mut pump).await;
        let parked = world.attempt(&a.id).await;
        assert_eq!(parked.state, S::Parked, "retry {retry}");
        assert_eq!(
            parked.failure_kind,
            Some(IntegrationFailureKind::Infrastructure)
        );
        assert!(parked.available_at.is_some());
        assert!(world.queue_of(&a).await.head_attempt_id.is_none());
        // Not due yet: the sweep leaves it alone.
        world.settle(&mut pump).await;
        assert_eq!(world.attempt(&a.id).await.revision, parked.revision);
        world.clock.advance(601);
    }
    world.settle(&mut pump).await;
    let exhausted = world.attempt(&a.id).await;
    assert_eq!(exhausted.state, S::Parked);
    assert_eq!(
        exhausted.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(exhausted.available_at.is_none());
    assert!(exhausted
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("retries_exhausted"));

    // A Task on a non-default location: commits are moved in and out.
    world.facts.fail.store(false, Ordering::SeqCst);
    let b = world.add_task("r", "b", "b.txt", "b\n").await;
    world.transfer.remote.lock().unwrap().insert("b".into());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&b.id).await.state, S::Completed);
    assert_eq!(
        *world.transfer.calls.lock().unwrap(),
        vec![
            ObjectTransferDirection::Inbound,
            ObjectTransferDirection::Outbound
        ]
    );
    // One key per attempt, claim generation and direction; the refs of the
    // finished attempt are released on both ends.
    let keys = world.transfer.keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 2);
    assert!(keys[0].starts_with(&format!("{}-", b.id)) && keys[0].ends_with("-in"));
    assert!(keys[1].ends_with("-out"));
    let released = world.transfer.released.lock().unwrap().clone();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].attempt_id, b.id);
    assert_eq!(released[0].target.repo_location_id, "l-r");
    let c = world.add_task("r", "c", "c.txt", "c\n").await;
    world.transfer.remote.lock().unwrap().insert("c".into());
    world.transfer.too_large.store(true, Ordering::SeqCst);
    world.settle(&mut pump).await;
    let capped = world.attempt(&c.id).await;
    assert_eq!(capped.state, S::Parked);
    assert_eq!(
        capped.failure_kind,
        Some(IntegrationFailureKind::OwnerRequired)
    );
    assert!(capped.available_at.is_none());
    assert!(capped
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("transfer_too_large"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queues_progress_independently_and_no_queue_starves_under_the_head_cap() {
    let world = World::new().await;
    world.add_repo("s").await;
    let slow = world.add_task("r", "slow", "slow.txt", "slow\n").await;
    let fast = world.add_task("s", "fast", "fast.txt", "fast\n").await;
    world.steps.hold("slow", IntegrationStepAction::Settle);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&slow.id).await.state, S::AwaitingTaskStep);
    assert_eq!(world.attempt(&fast.id).await.state, S::Completed);
    assert_eq!(pump.worker.active_heads(), 1);
    world.steps.unhold("slow", IntegrationStepAction::Settle);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&slow.id).await.state, S::Completed);

    // One head at a time over three queues with two members each: the
    // rotation serves every queue before any queue is served twice.
    let world = World::new().await;
    world.add_repo("s").await;
    world.add_repo("t").await;
    for repo in ["r", "s", "t"] {
        for n in 1..=2 {
            let task = format!("{repo}{n}");
            world
                .add_task(repo, &task, &format!("{task}.txt"), "x\n")
                .await;
        }
    }
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        max_heads: 1,
        ..config()
    }));
    world.settle(&mut pump).await;
    let order: Vec<String> = world
        .steps
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.action == IntegrationStepAction::Result)
        .map(|request| request.task_id.clone())
        .collect();
    assert_eq!(order.len(), 6);
    let firsts: HashSet<char> = order[..3]
        .iter()
        .map(|task| task.chars().next().unwrap())
        .collect();
    assert_eq!(firsts.len(), 3, "{order:?}");
    assert!(
        order[..3].iter().all(|task| task.ends_with('1')),
        "{order:?}"
    );
}

async fn digest(db: &SqliteDb) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT COALESCE(SUM(revision),0) FROM integration_queue),(SELECT COALESCE(SUM(revision),0) FROM integration_attempt),(SELECT COUNT(*) FROM task_step),(SELECT COALESCE(SUM(version),0) FROM task)")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_worker_writes_nothing() {
    let world = World::new().await;
    let done = world.add_task("r", "done", "done.txt", "x\n").await;
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&done.id).await.state, S::Completed);
    // A parked member that is not due, and an open queue with no member.
    world.add_repo("s").await;
    world
        .db
        .create_or_get_integration_queue("s", "main")
        .await
        .unwrap();
    std::fs::write(world.repo("r").join("dirty"), "dirty").unwrap();
    let parked = world.add_task("r", "parked", "parked.txt", "x\n").await;
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&parked.id).await.state, S::Parked);
    let before = digest(&world.db).await;
    let steps = world.steps.requests.lock().unwrap().len();
    for _ in 0..5 {
        assert!(pump.worker.sweep_once().await.unwrap().is_empty());
        world.clock.advance(30);
    }
    assert_eq!(digest(&world.db).await, before);
    assert_eq!(world.steps.requests.lock().unwrap().len(), steps);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_members_drain_in_order_under_the_supervised_loop() {
    let world = World::new().await;
    let mut attempts = Vec::new();
    for n in 0..20 {
        let task = format!("t{n:02}");
        attempts.push(
            world
                .add_task("r", &task, &format!("{task}.txt"), "x\n")
                .await,
        );
    }
    let worker = world.worker();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(worker.clone().run(shutdown));
    wait_for!("the queue to drain", {
        world.steps.answer(&world.db).await;
        world.attempt(&attempts[19].id).await.state == S::Completed
    });
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("bounded shutdown")
        .unwrap()
        .unwrap();
    let mut completed = Vec::new();
    for attempt in &attempts {
        let end = world.attempt(&attempt.id).await;
        assert_eq!(end.state, S::Completed);
        completed.push((end.completed_at.unwrap(), end.queue_seq));
    }
    let mut by_time = completed.clone();
    by_time.sort();
    assert_eq!(by_time, completed, "members left in queue order");
    assert_eq!(
        git_out(&world.repo("r"), &["rev-list", "--count", "main"])
            .await
            .unwrap(),
        "21"
    );
    assert_eq!(worker.active_heads(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_mid_head_is_clean_and_the_next_worker_finishes() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.hold("a", IntegrationStepAction::Settle);
    let worker = world.worker();
    let (stop, shutdown) = watch::channel(false);
    let running = tokio::spawn(worker.clone().run(shutdown));
    wait_for!(
        "the head to wait for its permit",
        world.attempt(&a.id).await.state == S::AwaitingTaskStep
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("bounded shutdown")
        .unwrap()
        .unwrap();
    assert_eq!(worker.active_heads(), 0);
    let left = world.attempt(&a.id).await;
    assert_eq!(left.state, S::AwaitingTaskStep);
    assert!(left.effect_intent_json.is_none());
    let before = digest(&world.db).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        digest(&world.db).await,
        before,
        "nothing runs after shutdown"
    );
    world.steps.unhold("a", IntegrationStepAction::Settle);
    world.clock.advance(120);
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}

fn park_seqs(world: &World, task: &str) -> Vec<i64> {
    world
        .steps
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.task_id == task && request.action == IntegrationStepAction::Park)
        .map(|request| request.effect_seq)
        .collect()
}

/// D2 checklist item 1. A counted retry that is re-queued, and a waiting
/// member of a queue that re-opens, each get one `park` step under a fresh
/// `effect_seq`: the level-triggered handler then restates `waiting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeued_retry_and_a_reopened_queue_tell_the_task_it_waits_again() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    let mut pump = Pump::new(world.worker());
    world.facts.fail.store(true, Ordering::SeqCst);
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(park_seqs(&world, "a"), vec![parked.effect_seq]);
    // Due: re-queued with `waiting` restated, claimed, and parked again.
    world.clock.advance(31);
    world.settle(&mut pump).await;
    let again = world.attempt(&a.id).await;
    assert_eq!(again.state, S::Parked);
    assert_eq!(
        park_seqs(&world, "a"),
        vec![parked.effect_seq, parked.effect_seq + 1, again.effect_seq],
        "deferred, waiting again, deferred again"
    );
    assert_eq!(again.effect_seq, parked.effect_seq + 2);
    world.facts.fail.store(false, Ordering::SeqCst);
    world.clock.advance(121);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(park_seqs(&world, "a").len(), 4, "waiting once more");

    // A suspended queue: both members are told once. A new process re-opens
    // it: the member that is not claimed is told it waits in line again.
    world.add_repo("s").await;
    let b = world.add_task("s", "b", "b.txt", "b\n").await;
    let c = world.add_task("s", "c", "c.txt", "c\n").await;
    sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l-s'")
        .execute(world.db.pool())
        .await
        .unwrap();
    for _ in 0..3 {
        world.settle(&mut pump).await;
        world.clock.advance(30);
    }
    assert_eq!(
        world.queue_of(&b).await.state,
        db::IntegrationQueueState::Suspended
    );
    let told = world.attempt(&c.id).await;
    assert_eq!(park_seqs(&world, "b").len(), 1);
    assert_eq!(park_seqs(&world, "c"), vec![told.effect_seq]);
    sqlx::query("UPDATE repo_location SET status='ready' WHERE id='l-s'")
        .execute(world.db.pool())
        .await
        .unwrap();
    world.steps.hold("b", IntegrationStepAction::Settle);
    let mut restarted = Pump::new(world.worker());
    world.settle(&mut restarted).await;
    assert_eq!(world.attempt(&c.id).await.state, S::Queued);
    assert_eq!(
        park_seqs(&world, "c"),
        vec![told.effect_seq, told.effect_seq + 1]
    );
    assert_eq!(
        park_seqs(&world, "b").len(),
        1,
        "the head is told by its steps"
    );
    // Not told a third time by later claims.
    world.steps.unhold("b", IntegrationStepAction::Settle);
    world.settle(&mut restarted).await;
    for attempt in [&b, &c] {
        assert_eq!(world.attempt(&attempt.id).await.state, S::Completed);
    }
    assert_eq!(park_seqs(&world, "c").len(), 2);
}

/// D2 checklist item 8. A deciding step that settled without answering is
/// asked again under a new `effect_seq`, a bounded number of times; then the
/// head parks with a typed cause and a counted retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_deciding_step_is_asked_again_then_the_head_parks_typed() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    world.steps.dying.lock().unwrap().insert("a".into());
    let mut pump = Pump::new(world.worker());
    world.settle(&mut pump).await;
    let parked = world.attempt(&a.id).await;
    assert_eq!(parked.state, S::Parked);
    assert_eq!(
        parked.failure_kind,
        Some(IntegrationFailureKind::Infrastructure)
    );
    assert!(parked
        .failure_message
        .as_deref()
        .unwrap()
        .starts_with("task_step_failed"));
    assert!(parked.available_at.is_some(), "a counted retry");
    let settles: Vec<i64> = world
        .steps
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.action == IntegrationStepAction::Settle)
        .map(|request| request.effect_seq)
        .collect();
    assert_eq!(
        settles.len(),
        3,
        "the first ask and two re-asks: {settles:?}"
    );
    assert!(settles.windows(2).all(|pair| pair[1] == pair[0] + 1));
    assert!(world.queue_of(&a).await.head_attempt_id.is_none());
    // The step side recovers: the retry merges.
    world.steps.dying.lock().unwrap().clear();
    world.clock.advance(31);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
    assert_eq!(ff_successes(&world, &a.id).await, 1);
}

/// D2 checklist items 9 and 6. A head waiting for its check asks again on a
/// timer (the ask applies a verdict whose delivery failed) instead of waiting
/// the check timeout out; an applied head re-arms its `result` step on every
/// sweep interval, not once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_check_is_asked_again_and_an_applied_head_re_arms_its_result() {
    let world = World::new().await;
    let a = world.add_task("r", "a", "a.txt", "a\n").await;
    // The target moves after the Task forked: the head is rebased and checked.
    world.external_commit("r", "outside").await;
    world.steps.hold("a", IntegrationStepAction::RequestCheck);
    let mut pump = Pump::new(world.worker_with(IntegrationWorkerConfig {
        check_reask: Duration::from_secs(20),
        ..config()
    }));
    world.settle(&mut pump).await;
    let checking = world.attempt(&a.id).await;
    assert_eq!(checking.state, S::Checking);
    let asks = |world: &World| {
        world
            .steps
            .actions("a")
            .into_iter()
            .filter(|action| *action == IntegrationStepAction::RequestCheck)
            .count()
    };
    assert_eq!(asks(&world), 1);
    world.clock.advance(10);
    world.settle(&mut pump).await;
    assert_eq!(asks(&world), 1, "not due yet");
    world.clock.advance(11);
    world.settle(&mut pump).await;
    let asked = world.attempt(&a.id).await;
    assert_eq!(asked.state, S::Checking);
    assert_eq!(asks(&world), 2);
    assert_eq!(asked.effect_seq, checking.effect_seq + 1);
    assert_eq!(
        asked.slot_generation, checking.slot_generation,
        "same claim"
    );

    world.steps.hold("a", IntegrationStepAction::Result);
    world.steps.unhold("a", IntegrationStepAction::RequestCheck);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    let mut readied = world.steps.readied.load(Ordering::SeqCst);
    assert!(readied >= 1);
    for sweep in 0..3 {
        world.clock.advance(31);
        world.settle(&mut pump).await;
        let now = world.steps.readied.load(Ordering::SeqCst);
        assert!(now > readied, "re-armed on sweep {sweep}");
        readied = now;
        assert_eq!(world.attempt(&a.id).await.state, S::Applied);
    }
    world.steps.unhold("a", IntegrationStepAction::Result);
    world.settle(&mut pump).await;
    assert_eq!(world.attempt(&a.id).await.state, S::Completed);
}
