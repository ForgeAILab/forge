//! Shared fixtures and assertions for the integration queue worker.
use super::*;

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
pub(super) struct TestClock(pub(super) Mutex<chrono::Duration>);
impl TestClock {
    pub(super) fn advance(&self, seconds: i64) {
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

pub(super) async fn git_out(path: &Path, args: &[&str]) -> Option<String> {
    let out = git::command_output(path, args).await.ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// The Task-step side: records every step, and `answer` plays the steps the
/// way the real consumer would, writing only the attempt's acknowledgment
/// fields (and the fixture Task's status for `result`).
#[derive(Default)]
pub(super) struct FakeSteps {
    pub(super) requests: Mutex<Vec<IntegrationStepRequest>>,
    pub(super) answered: Mutex<HashSet<String>>,
    pub(super) held: Mutex<HashSet<(String, IntegrationStepAction)>>,
    pub(super) red: Mutex<HashSet<String>>,
    pub(super) red_message: Mutex<Option<String>>,
    /// Permits name another candidate (a consumer bug the worker must bound).
    pub(super) bad_permit: AtomicBool,
    pub(super) readied: AtomicUsize,
    /// Steps the step worker settled without applying (`task|causation key`).
    pub(super) dead: Mutex<HashSet<String>>,
    /// Every deciding step of these Tasks dies instead of answering.
    pub(super) dying: Mutex<HashSet<String>>,
    /// Steps that settled without applying because their Task had left the
    /// status entry of the attempt (`task|causation key`).
    pub(super) left: Mutex<HashSet<String>>,
}
impl FakeSteps {
    pub(super) fn hold(&self, task: &str, action: IntegrationStepAction) {
        self.held.lock().unwrap().insert((task.into(), action));
    }
    pub(super) fn unhold(&self, task: &str, action: IntegrationStepAction) {
        self.held.lock().unwrap().remove(&(task.to_owned(), action));
    }
    pub(super) fn actions(&self, task: &str) -> Vec<IntegrationStepAction> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.task_id == task)
            .map(|request| request.action)
            .collect()
    }
    pub(super) async fn answer(&self, db: &SqliteDb) -> bool {
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
                        message: red.then(|| {
                            self.red_message
                                .lock()
                                .unwrap()
                                .clone()
                                .unwrap_or_else(|| "tests failed".to_owned())
                        }),
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
        Ok(if self.left.lock().unwrap().contains(&key) {
            IntegrationStepState::TaskLeft
        } else if self.dead.lock().unwrap().contains(&key) {
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
pub(super) struct FakeTransfer {
    pub(super) remote: Mutex<HashSet<String>>,
    pub(super) too_large: AtomicBool,
    /// The next transfer's reply is lost: the port answers with an error
    /// although the objects may have arrived.
    pub(super) lose_reply: AtomicBool,
    pub(super) calls: Mutex<Vec<ObjectTransferDirection>>,
    pub(super) keys: Mutex<Vec<String>>,
    pub(super) released: Mutex<Vec<ObjectTransferRelease>>,
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
        assert!(!request.have.is_empty() && !request.have.contains(&request.want));
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(ServiceError::invalid_operation(
                "daemon request timed out: integration.import_objects",
            ));
        }
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

pub(super) struct GitFactsFake {
    pub(super) db: Arc<SqliteDb>,
    pub(super) transfer: Arc<FakeTransfer>,
    pub(super) fail: AtomicBool,
    pub(super) paused: AtomicBool,
    /// The Task's placement is not `ready` (its owner just reconnected).
    pub(super) placement_down: AtomicBool,
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
                TaskGate::Gated(crate::integration_worker::GateCause::ProjectPaused)
            } else {
                TaskGate::Live
            },
            task_location: ObjectTransferEndpoint {
                repo_location_id: format!("l-{}", queue.repo_id),
                owner: EffectOwner::Server,
            },
            task_target_tip: String::new(),
            target_in_candidate: ancestor(target_tip.clone(), candidate_head.clone()).await,
            candidate_in_target: ancestor(candidate_head.clone(), target_tip.clone()).await,
            worktree_dirty: !git::is_worktree_clean(&tree).await?,
            target_dirty: !git::is_worktree_clean(&repo).await?,
            rebase_in_progress: git::detect_rebase_in_progress(&tree).await?,
            placement_ready: !self.placement_down.load(Ordering::SeqCst),
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

pub(super) struct World {
    pub(super) temp: tempfile::TempDir,
    pub(super) db: Arc<SqliteDb>,
    pub(super) clock: Arc<TestClock>,
    pub(super) steps: Arc<FakeSteps>,
    pub(super) transfer: Arc<FakeTransfer>,
    pub(super) facts: Arc<GitFactsFake>,
}
pub(super) fn config() -> IntegrationWorkerConfig {
    IntegrationWorkerConfig {
        poll: Duration::from_millis(5),
        conflict_backoff: Duration::from_millis(1),
        ..Default::default()
    }
}
impl World {
    pub(super) async fn new() -> Self {
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
                placement_down: AtomicBool::new(false),
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
    pub(super) fn repo(&self, id: &str) -> PathBuf {
        self.temp.path().join(format!("repo-{id}"))
    }
    pub(super) fn tree(&self, task: &str) -> PathBuf {
        self.temp.path().join(format!("tree-{task}"))
    }
    pub(super) async fn add_repo(&self, id: &str) {
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
    pub(super) async fn add_task(
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
    pub(super) fn worker_with(
        &self,
        config: IntegrationWorkerConfig,
    ) -> Arc<IntegrationQueueWorker> {
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
    pub(super) fn worker(&self) -> Arc<IntegrationQueueWorker> {
        self.worker_with(config())
    }
    pub(super) async fn attempt(&self, id: &str) -> IntegrationAttempt {
        self.db.integration_attempt(id).await.unwrap().unwrap()
    }
    pub(super) async fn tip(&self, repo: &str) -> String {
        git_out(&self.repo(repo), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap()
    }
    pub(super) async fn external_commit(&self, repo: &str, file: &str) {
        std::fs::write(self.repo(repo).join(file), file).unwrap();
        git::commit_all(&self.repo(repo), file).await.unwrap();
    }
    pub(super) async fn queue_of(&self, attempt: &IntegrationAttempt) -> IntegrationQueue {
        self.db
            .integration_queue(attempt.queue_id.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
    /// Sweep, step every driver and play the Task steps until nothing moves.
    /// `Some(error)` is a driver stopped by an injected fault (a "crash").
    pub(super) async fn pump(&self, pump: &mut Pump) -> Option<ServiceError> {
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
    pub(super) async fn settle(&self, pump: &mut Pump) {
        if let Some(error) = self.pump(pump).await {
            panic!("unexpected driver error: {error}");
        }
    }
}
pub(super) struct Pump {
    pub(super) worker: Arc<IntegrationQueueWorker>,
    pub(super) drivers: Vec<HeadDriver>,
}
impl Pump {
    pub(super) fn new(worker: Arc<IntegrationQueueWorker>) -> Self {
        Self {
            worker,
            drivers: Vec::new(),
        }
    }
}
pub(super) fn arm(worker: &IntegrationQueueWorker, state: S, point: CrashPoint) -> Arc<AtomicBool> {
    let fired = Arc::new(AtomicBool::new(false));
    let flag = fired.clone();
    worker.set_fault(Some(Arc::new(move |at, here| {
        at == state && here == point && !flag.swap(true, Ordering::SeqCst)
    })));
    fired
}
pub(super) async fn ff_successes(world: &World, attempt: &str) -> usize {
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

/// Bring one head to `state` and stop there.
pub(super) async fn hold_in(world: &World, pump: &mut Pump, state: S) -> IntegrationAttempt {
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

/// state x crash point -> expected recovery. `moved`: the target moved
/// before the head got the slot, so it has to rebase and check.
pub(super) const CRASH_MATRIX: &[(bool, S, CrashPoint, S)] = &[
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

/// An intent admitted and started, as the owner leaves it when the process
/// dies inside the effect.
pub(super) async fn started_intent(
    world: &World,
    a: &IntegrationAttempt,
    kind: IntegrationOperationKind,
) {
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
pub(super) async fn started_ff_intent(world: &World, a: &IntegrationAttempt) {
    started_intent(world, a, IntegrationOperationKind::FastForward).await;
}
pub(super) async fn receipts_of(world: &World, attempt: &str) -> Vec<db::IntegrationEffectReceipt> {
    serde_json::from_value(world.attempt(attempt).await.effect_receipts_json).unwrap()
}

/// The server owner, behind an owner that cannot be asked for a rebase (the
/// request fails before any intent: the owner could not be told the claim).
pub(super) struct UnreachableOwner {
    pub(super) inner: ServerIntegrationOwner,
    pub(super) down: AtomicBool,
}
#[async_trait]
impl IntegrationOwnerPort for UnreachableOwner {
    async fn rebase(
        &self,
        request: OwnerRebaseRequest,
    ) -> Result<crate::integration_owner::OwnerRebaseReceipt> {
        if self.down.load(Ordering::SeqCst) {
            return Err(ServiceError::invalid_operation(
                "daemon is not connected: integration.announce",
            ));
        }
        IntegrationOwnerPort::rebase(&self.inner, request).await
    }
    async fn fast_forward(
        &self,
        request: OwnerFastForwardRequest,
    ) -> Result<crate::integration_owner::OwnerMergeReceipt> {
        IntegrationOwnerPort::fast_forward(&self.inner, request).await
    }
    async fn reconcile_effect(&self, request: &db::IntegrationEffectRequest) -> Result<()> {
        IntegrationOwnerPort::reconcile_effect(&self.inner, request).await
    }
    async fn reconcile_outstanding(&self) -> Result<()> {
        IntegrationOwnerPort::reconcile_outstanding(&self.inner).await
    }
}

pub(super) async fn digest(db: &SqliteDb) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT COALESCE(SUM(revision),0) FROM integration_queue),(SELECT COALESCE(SUM(revision),0) FROM integration_attempt),(SELECT COUNT(*) FROM task_step),(SELECT COALESCE(SUM(version),0) FROM task)")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

pub(super) fn park_seqs(world: &World, task: &str) -> Vec<i64> {
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

/// What the old merge path had written for a Task that was merging when the
/// server was upgraded.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) enum Legacy {
    /// A merge sent to its owner whose reply never arrived (importer row P2).
    RunningRemoteOperation,
    /// A recorded `merge_intent` without an outcome (importer row P1).
    IntentWithoutOutcome,
}

impl World {
    /// A Task in `merging` the way a pre-queue server left it: a reviewed
    /// commit in its worktree, the legacy evidence of `shape`, no attempt.
    /// Returns the candidate commit.
    pub(super) async fn legacy_task(&self, task: &str, file: &str, shape: Legacy) -> String {
        let (repo, tree) = (self.repo("r"), self.tree(task));
        let branch = format!("task-{task}");
        git_out(
            &repo,
            &["worktree", "add", "-b", &branch, tree.to_str().unwrap()],
        )
        .await
        .expect("worktree");
        std::fs::write(tree.join(file), "legacy\n").unwrap();
        git::commit_all(&tree, task).await.unwrap();
        let head = git::get_current_sha(&tree).await.unwrap();
        let now = db::now_rfc3339();
        let pool = self.db.pool();
        sqlx::query("INSERT INTO task(id,project_id,title,status,review_passed_at,created_at,updated_at) VALUES(?,'p',?,'merging',?,?,?)").bind(task).bind(task).bind(&now).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,?,'ready',?,?)").bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&branch).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l-r',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(tree.to_str()).bind(&now).bind(&now).execute(pool).await.unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES(?,?,'executor','completed',?,?,?)").bind(format!("e-{task}")).bind(task).bind(format!("w-{task}")).bind(&now).bind(&now).execute(pool).await.unwrap();
        // The hooks step that was merging: settled by the old recovery, its
        // effects retained.
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,attempts,created_at,updated_at,expected_epoch,lane) VALUES(?,?,1,'hooks','{}','entry',?,1,'merging',1,'failed',?,1,?,?,0,'long')")
            .bind(format!("s-{task}")).bind(task).bind(format!("c-{task}")).bind(&now).bind(&now).bind(&now).execute(pool).await.unwrap();
        match shape {
            Legacy::RunningRemoteOperation => {
                sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES(?,?,?,?,'old-daemon','old-runtime',1,0,'running',?)")
                    .bind(format!("legacy-op-{task}")).bind(format!("s-{task}")).bind(format!("w-{task}")).bind(format!("pl-{task}")).bind(&now).execute(pool).await.unwrap();
            }
            Legacy::IntentWithoutOutcome => {
                let intent =
                    serde_json::json!({"target_branch":"main","candidate_sha":head}).to_string();
                sqlx::query("INSERT INTO task_hook_checkpoint(step_id,hook_index,started_at,effects_json) VALUES(?,0,?,?)")
                    .bind(format!("s-{task}")).bind(&now)
                    .bind(serde_json::json!({"merge_intent":intent}).to_string())
                    .execute(pool).await.unwrap();
            }
        }
        head
    }
    /// The storage importer, as the start runs it before any worker.
    pub(super) async fn import(&self, task: &str) -> IntegrationAttempt {
        while self
            .db
            .import_integration_pass(db::INTEGRATION_IMPORT_PAGE)
            .await
            .unwrap()
            .remaining
        {}
        self.db
            .current_integration_attempt(task)
            .await
            .unwrap()
            .expect("imported as the Task's current attempt")
    }
}
