//! Shared check runner fixtures, owners, capacity streams, and consumer helpers.

use super::*;

pub(super) async fn fixture() -> (tempfile::TempDir, Arc<SqliteDb>, Arc<CheckRunner>) {
    let temp = tempfile::tempdir().unwrap();
    let pool = db::create_sqlite_pool(&format!(
        "sqlite://{}",
        temp.path().join("checks.sqlite").display()
    ))
    .await
    .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let store = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','project',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','task','review',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let runner = Arc::new(CheckRunner::new(store.clone()));
    (temp, store, runner)
}
pub(super) fn request(key: &str) -> CheckRunRequest {
    CheckRunRequest {
        identity: CheckRunIdentity {
            project_id: "p".into(),
            repo_id: "r".into(),
            commit_sha: "a".repeat(40),
            inputs: CheckDigestInput {
                spec: check_executor::legacy_ci_spec("true", &Default::default(), 0, false),
                environment: Default::default(),
                environment_identity: CheckEnvironmentIdentity::Attested {
                    input_digest: "a".repeat(64),
                },
                execution_revision: CheckExecutionRevision {
                    number: 0,
                    audit_ref: None,
                },
            },
        },
        request_key: key.into(),
        task_id: Some("t".into()),
        status_epoch: 0,
        origin: CheckConsumerOrigin::Entry,
        purpose: CheckPurpose::EntryCi,
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    }
}
pub(super) fn cacheable_request(key: &str) -> CheckRunRequest {
    let mut req = request(key);
    req.identity.inputs.spec.commands[0].cacheability = CheckCacheability::DeclaredControlledInputs;
    req
}
pub(super) fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_owner: run.lease_owner.clone(),
        lease_generation: run.lease_generation,
    }
}
pub(super) async fn settle(
    store: &SqliteDb,
    run: &StoredCheckRun,
    outcome: CheckResultOutcome,
    cleanup: CheckCleanup,
) -> StoredCheckResult {
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    let run = store
        .claim_check_run(&run.id, run.version, "test", &now, &until)
        .await
        .unwrap();
    let run = store
        .transition_check_run(&fence(&run), CheckRunState::Cleaning, &now)
        .await
        .unwrap();
    store
        .finish_check_run(
            &fence(&run),
            CheckResultEvidence {
                outcome,
                cleanup,
                commands: vec![CheckCommandOutcome {
                    failures: vec![],
                    failures_truncated: false,
                    index: 0,
                    command: "true".into(),
                    exit_code: if outcome == CheckResultOutcome::Pass {
                        0
                    } else {
                        1
                    },
                    stderr_tail: String::new(),
                    output_tail: String::new(),
                    started_at: now.clone(),
                    finished_at: now.clone(),
                }],
                output_truncated: false,
                redaction_values: vec![],
                reusable: true,
            },
            &now,
        )
        .await
        .unwrap()
}
pub(super) fn scheduled(reply: RequestedCheck) -> StoredCheckRun {
    match reply.outcome {
        CheckRequestOutcome::Scheduled(run) => run,
        other => panic!("expected scheduled, got {other:?}"),
    }
}

#[derive(Default)]
pub(super) struct TestOwner {
    pub(super) runs: AtomicUsize,
    pub(super) lookups: AtomicUsize,
    pub(super) cancels: AtomicUsize,
    pub(super) acknowledgments: AtomicUsize,
    pub(super) interrupted: bool,
    pub(super) gone: bool,
    /// The owner keeps reporting the operation as running.
    pub(super) running: bool,
    pub(super) refuse_ack: bool,
}
pub(super) fn intent() -> CheckDispatchIntent {
    CheckDispatchIntent {
        target: CheckDispatchTarget::Server {
            path: "owner-local-handle".into(),
            workspace_id: "w".into(),
            placement_id: "pl".into(),
            generation: 1,
        },
        owner: CheckOwnerIdentity {
            owner_kind: "server".into(),
            machine_id: None,
            runtime_id: "test-runtime".into(),
        },
        environment_task_id: "t".into(),
    }
}
pub(super) fn receipt(record: &CheckWorkerRecord) -> CheckReceipt {
    let now = db::now_rfc3339();
    CheckReceipt {
        operation_id: record.run.operation_id.clone(),
        owner: record.dispatch.as_ref().unwrap().owner.clone(),
        execution_inputs: record.run.identity.inputs.environment_identity.clone(),
        commands: record
            .run
            .identity
            .inputs
            .spec
            .commands
            .iter()
            .map(|command| CheckCommandReceipt {
                failures: vec![],
                failures_truncated: false,
                id: command.id.clone(),
                command: command.shell_text.clone(),
                exit_code: Some(0),
                outcome: CheckExecutionOutcome::Passed,
                duration_ms: 0,
                stdout_tail: "passed".into(),
                stderr_tail: String::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                stdout_drain_incomplete: false,
                stderr_drain_incomplete: false,
                process_tree_stopped: true,
                started_at: now.clone(),
                finished_at: now.clone(),
            })
            .collect(),
        outcome: CheckExecutionOutcome::Passed,
        cleanup: CheckCleanupReceipt {
            outcome: CheckCleanupOutcome::NotPerformed,
            commands: vec![],
            checkout_removed: false,
            message: None,
        },
        prepared_head: Some(record.run.identity.commit_sha.clone()),
        finished_head: Some(record.run.identity.commit_sha.clone()),
        tracked_changes: Some(false),
        started_at: now.clone(),
        finished_at: now,
        infrastructure_message: None,
    }
}
#[async_trait::async_trait]
impl CheckOwnerPort for TestOwner {
    async fn prepare(&self, _run: &StoredCheckRun) -> Result<CheckDispatchIntent> {
        Ok(intent())
    }
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        _cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        assert!(record.dispatch.is_some() && record.deadline_at.is_some());
        if self.interrupted {
            Err(ServiceError::invalid_operation("transport uncertain"))
        } else {
            Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt(record)),
            })
        }
    }
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        if self.running {
            return Ok(DaemonCheckResult::Running {
                operation_id: record.run.operation_id.clone(),
            });
        }
        if self.interrupted {
            Ok(DaemonCheckResult::Unknown {
                operation_id: record.run.operation_id.clone(),
            })
        } else {
            Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt(record)),
            })
        }
    }
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.cancels.fetch_add(1, Ordering::SeqCst);
        if self.running {
            return Ok(DaemonCheckResult::Running {
                operation_id: record.run.operation_id.clone(),
            });
        }
        Ok(DaemonCheckResult::Unknown {
            operation_id: record.run.operation_id.clone(),
        })
    }
    async fn owner_gone(&self, _record: &CheckWorkerRecord) -> Result<bool> {
        Ok(self.gone)
    }
    async fn acknowledge(&self, _record: &CheckWorkerRecord) -> Result<()> {
        self.acknowledgments.fetch_add(1, Ordering::SeqCst);
        if self.refuse_ack {
            return Err(ServiceError::invalid_operation("owner unreachable"));
        }
        Ok(())
    }
}
pub(super) async fn admit(store: &SqliteDb, run: &StoredCheckRun) -> StoredCheckRun {
    let now = db::now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
    match store
        .admit_check_run(run, "test-worker", &now, &until)
        .await
        .unwrap()
    {
        CheckAdmission::Admitted(run) => *run,
        other => panic!("not admitted {other:?}"),
    }
}

/// An owner that, like the server owner recording its receipt, is inside a
/// write transaction of its own when the worker's one-second look at the
/// run's consumers comes due.
pub(super) struct WritesWhileRunning {
    pub(super) store: Arc<SqliteDb>,
}
#[async_trait::async_trait]
impl CheckOwnerPort for WritesWhileRunning {
    async fn prepare(&self, _run: &StoredCheckRun) -> Result<CheckDispatchIntent> {
        Ok(intent())
    }
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        _cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        let mut transaction = db::begin_immediate(self.store.pool()).await?;
        sqlx::query("UPDATE check_run SET updated_at=updated_at WHERE id=?")
            .bind(&record.run.id)
            .execute(&mut *transaction)
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        transaction.commit().await?;
        Ok(DaemonCheckResult::Completed {
            receipt: Box::new(receipt(record)),
        })
    }
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        Ok(DaemonCheckResult::Completed {
            receipt: Box::new(receipt(record)),
        })
    }
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        Ok(DaemonCheckResult::Unknown {
            operation_id: record.run.operation_id.clone(),
        })
    }
    async fn owner_gone(&self, _record: &CheckWorkerRecord) -> Result<bool> {
        Ok(false)
    }
    async fn acknowledge(&self, _record: &CheckWorkerRecord) -> Result<()> {
        Ok(())
    }
}

/// The slot rules under a steady stream of new Tasks, on a machine with two
/// slots. A stream Task is an execution that wants a slot at every turn; a
/// queued check is a Task in `review`. Every turn releases one slot (the
/// oldest run ends), then the check runner and the dispatcher each ask.
/// Checks go first and are admitted in queue order; executions keep a slot.
pub(super) struct SlotStream {
    pub(super) store: Arc<SqliteDb>,
    pub(super) started: usize,
}
impl SlotStream {
    pub(super) async fn capacity(&self) -> db::machine_capacity::MachineCapacity {
        let mut tx = db::begin_immediate(self.store.pool()).await.unwrap();
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(2), "server-machine")
            .await
            .unwrap()
    }
    /// A new Task's execution asks for a slot, as the reserve and start
    /// transactions ask. A refused one waits for a run slot.
    pub(super) async fn dispatch(&mut self) -> bool {
        let id = format!("stream-{}", self.started);
        let now = db::now_rfc3339();
        sqlx::query("INSERT OR IGNORE INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p','stream','in_progress',?,?)").bind(&id).bind(&now).bind(&now).execute(self.store.pool()).await.unwrap();
        if !self.capacity().await.admits_execution() {
            self.store
                .schedule_wait(&id, ("p", false), None, Some("*"), None)
                .await
                .unwrap();
            return false;
        }
        self.store
            .schedule_wait(&id, ("p", false), None, None, None)
            .await
            .unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','running',?,?)").bind(&id).bind(&id).bind(&now).bind(&now).execute(self.store.pool()).await.unwrap();
        self.started += 1;
        true
    }
    pub(super) async fn finish_oldest_execution(&self) -> bool {
        sqlx::query("UPDATE execution SET status='completed' WHERE id=(SELECT id FROM execution WHERE status='running' ORDER BY created_at,rowid LIMIT 1)")
            .execute(self.store.pool()).await.unwrap().rows_affected() == 1
    }
    pub(super) async fn try_admit(&self, run: &StoredCheckRun) -> Option<StoredCheckRun> {
        let run = self.store.check_run(&run.id).await.unwrap().unwrap();
        let now = db::now_rfc3339();
        let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
        match self
            .store
            .admit_check_run(&run, "stream", &now, &until)
            .await
            .unwrap()
        {
            CheckAdmission::Admitted(run) => Some(*run),
            CheckAdmission::Waiting => None,
        }
    }
}

/// An admitted check ends with a passing result and frees its slot.
pub(super) async fn finish_check(store: &SqliteDb, run: &StoredCheckRun) {
    let now = db::now_rfc3339();
    sqlx::query("UPDATE check_run SET state='succeeded',lease_owner=NULL,lease_until=NULL,finished_at=?,updated_at=?,version=version+1 WHERE id=? AND state='running'")
        .bind(&now)
        .bind(&now)
        .bind(&run.id)
        .execute(store.pool())
        .await
        .unwrap();
}

pub(super) async fn queue_check(
    runner: &CheckRunner,
    key: &str,
    origin: CheckConsumerOrigin,
) -> StoredCheckRun {
    let mut req = request(key);
    req.identity.commit_sha = format!(
        "{:0>40}",
        key.len() * 7919 + key.bytes().map(usize::from).sum::<usize>()
    );
    req.origin = origin;
    scheduled(runner.request(req).await.unwrap())
}

/// A Task that holds one slot (an interactive session) and needs a second
/// one for its coder run, on a machine with two slots where checks keep
/// queuing. `holder` has its worktree on the server.
pub(super) struct SecondSlotTask {
    pub(super) store: Arc<SqliteDb>,
}
impl SecondSlotTask {
    pub(super) async fn new(store: &Arc<SqliteDb>, temp: &tempfile::TempDir) -> Self {
        let now = db::now_rfc3339();
        let path = temp.path().to_str();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('holder','p','holder','in_progress',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('session','holder','interactive','running',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('hw','holder','r',?,'task/holder','ready',?,?)").bind(path).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('hl','r','server',?,'primary_checkout',1,'ready',?,?)").bind(path).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('hp','hw','holder','server','hl',?,1,'ready','scheduler','{}',?,?)").bind(path).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
        Self {
            store: store.clone(),
        }
    }
    pub(super) async fn aged(&self) -> bool {
        db::machine_capacity::aged_slot_wait(self.store.pool(), "holder")
            .await
            .unwrap()
            .is_some()
    }
    /// The coder dispatch asks as the reserve and start transactions ask.
    pub(super) async fn dispatch(&self) -> bool {
        let admitted = {
            let mut tx = db::begin_immediate(self.store.pool()).await.unwrap();
            let capacity = db::machine_capacity::count_machine_capacity(
                &mut tx,
                None,
                Some(2),
                "server-machine",
            )
            .await
            .unwrap();
            assert!(capacity.active_runs() <= 2, "the cap was exceeded");
            db::machine_capacity::capacity_for_task(
                &mut tx,
                capacity,
                None,
                "holder",
                "server-machine",
            )
            .await
            .unwrap()
            .admits_execution()
        };
        if !admitted {
            self.store
                .schedule_wait("holder", ("p", false), None, Some("*"), None)
                .await
                .unwrap();
            return false;
        }
        // The dispatch attempt drops the wait row before the run starts.
        self.store
            .schedule_wait("holder", ("p", false), None, None, None)
            .await
            .unwrap();
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('coder-run','holder','coder','running',?,?)").bind(&now).bind(&now).execute(self.store.pool()).await.unwrap();
        true
    }
}

/// An owner whose `prepare` is the window in question: the consumer's Task
/// is cancelled while the owner prepares, before any dispatch record exists.
pub(super) struct CancelledWhilePreparing {
    pub(super) store: Arc<SqliteDb>,
    pub(super) inner: TestOwner,
}
#[async_trait::async_trait]
impl CheckOwnerPort for CancelledWhilePreparing {
    async fn prepare(&self, run: &StoredCheckRun) -> Result<CheckDispatchIntent> {
        // The canceller looked for a dispatch to fence and found none.
        assert!(self
            .store
            .check_worker_record(&run.id)
            .await
            .unwrap()
            .dispatch
            .is_none());
        sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE run_id=?")
            .bind(db::now_rfc3339())
            .bind(&run.id)
            .execute(self.store.pool())
            .await
            .unwrap();
        self.inner.prepare(run).await
    }
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        self.inner.run(record, cancel).await
    }
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.inner.lookup(record).await
    }
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        self.inner.cancel(record).await
    }
    async fn owner_gone(&self, record: &CheckWorkerRecord) -> Result<bool> {
        self.inner.owner_gone(record).await
    }
    async fn acknowledge(&self, record: &CheckWorkerRecord) -> Result<()> {
        self.inner.acknowledge(record).await
    }
}

pub(super) struct TestFamily {
    pub(super) authority: std::sync::Mutex<Option<String>>,
    pub(super) applied: std::sync::Mutex<Vec<consumer::CheckApplication>>,
}
#[async_trait::async_trait]
impl consumer::CheckConsumerFamily for TestFamily {
    async fn current_authority(&self, _task: &str, _epoch: i64) -> Result<Option<String>> {
        Ok(self.authority.lock().unwrap().clone())
    }
    async fn apply(&self, application: &consumer::CheckApplication) -> Result<()> {
        self.applied.lock().unwrap().push(application.clone());
        Ok(())
    }
}
pub(super) fn task_request(authority: &str, commit: char) -> consumer::TaskCheckRequest {
    let base = request("unused");
    let mut identity = base.identity;
    identity.commit_sha = commit.to_string().repeat(40);
    consumer::TaskCheckRequest {
        task_id: "t".into(),
        status_epoch: 0,
        authority: authority.into(),
        origin: CheckConsumerOrigin::Integration,
        purpose: base.purpose,
        identity,
        workspace_id: None,
        machine_id: None,
        wall_timeout_seconds: 1800,
    }
}
pub(super) async fn consumers(
    store: &Arc<SqliteDb>,
    runner: &Arc<CheckRunner>,
    authority: &str,
) -> (consumer::TaskCheckConsumers, Arc<TestFamily>) {
    let family = Arc::new(TestFamily {
        authority: std::sync::Mutex::new(Some(authority.into())),
        applied: Default::default(),
    });
    let consumers = consumer::TaskCheckConsumers::new(store.clone(), runner.clone());
    consumers.register(CheckConsumerOrigin::Integration, family.clone());
    (consumers, family)
}
/// Run `future` under a claimed step of Task `t`, as the hook or command
/// that asks for a check runs. The step is this harness's own and is removed
/// afterwards, so the Task's queue holds only what the code under test put there.
pub(super) async fn in_step<T>(
    store: &SqliteDb,
    future: impl std::future::Future<Output = T>,
) -> T {
    let id = db::new_uuid_v4();
    store
        .enqueue_step(&db::EnqueueTaskStep {
            id: id.clone(),
            task_id: "t".into(),
            kind: "command".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: "review".into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: db::now_rfc3339(),
        })
        .await
        .unwrap();
    let out = in_claimed(store, &id, future).await;
    sqlx::query("DELETE FROM task_step WHERE id=?")
        .bind(&id)
        .execute(store.pool())
        .await
        .unwrap();
    out
}
/// Claim exactly `step_id` (whatever else is queued) and run `future` as it.
pub(super) async fn in_claimed<T>(
    store: &SqliteDb,
    step_id: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    sqlx::query(
        "UPDATE task_step SET status='claimed',claimed_by='check-tests',lease_until=? WHERE id=?",
    )
    .bind(db::task_writer::lease_deadline())
    .bind(step_id)
    .execute(store.pool())
    .await
    .unwrap();
    let step = store
        .task_steps("t")
        .await
        .unwrap()
        .into_iter()
        .find(|step| step.id == step_id)
        .unwrap();
    db::task_writer::in_task_step(step, future).await
}
/// A request made the way production makes it: from inside the Task's step.
pub(super) async fn ask(
    store: &SqliteDb,
    consumers: &consumer::TaskCheckConsumers,
    request: consumer::TaskCheckRequest,
) -> Result<RequestedCheck> {
    in_step(store, consumers.request(request)).await
}
/// The delivery step of the Task's only undelivered-then-delivered consumer.
pub(super) async fn delivery(
    store: &SqliteDb,
    consumer_id: &str,
) -> (String, db::CheckResultDelivery) {
    for step in store.task_steps("t").await.unwrap() {
        let payload: serde_json::Value = serde_json::from_str(&step.payload_json).unwrap();
        if payload["operation"] == "apply_check_result"
            && payload["arguments"]["consumer_id"] == consumer_id
        {
            return (
                step.id,
                serde_json::from_value(payload["arguments"].clone()).unwrap(),
            );
        }
    }
    panic!("no delivery step for {consumer_id}");
}

pub(super) async fn condition(store: &SqliteDb) -> db::TaskCondition {
    store.task_condition("t").await.unwrap()
}
pub(super) fn phase(condition: &db::TaskCondition) -> Option<CheckWaitPhase> {
    condition.check_wait().map(|wait| wait.phase)
}
/// The Task's progress notes, oldest first.
pub(super) async fn progress_notes(store: &SqliteDb) -> Vec<(String, db::CheckProgressDelivery)> {
    let mut notes = Vec::new();
    for step in store.task_steps("t").await.unwrap() {
        let payload: serde_json::Value = serde_json::from_str(&step.payload_json).unwrap();
        if payload["operation"] == "apply_check_progress" {
            notes.push((
                step.id,
                serde_json::from_value(payload["arguments"].clone()).unwrap(),
            ));
        }
    }
    notes
}

// ---- canonical CI policy: reuse through the real server owner ----

/// The inherited variable these tests read and change. The owner's
/// environment is the test process's, so every canonical test holds this lock
/// for its whole life: no other one sees the variable move under it.
pub(super) const INHERITED: &str = "FORGE_CHECK_TEST_INHERITED";
pub(super) static OWNER_ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) struct Canonical {
    pub(super) temp: tempfile::TempDir,
    pub(super) store: Arc<SqliteDb>,
    pub(super) runner: Arc<CheckRunner>,
    pub(super) worker: CheckRunWorker,
    pub(super) checkout: std::path::PathBuf,
    pub(super) head: String,
    pub(super) _environment: tokio::sync::MutexGuard<'static, ()>,
}
pub(super) async fn fixture_git(path: &std::path::Path, args: &[&str]) {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "fixture Git command failed");
}
pub(super) async fn canonical_fixture() -> Canonical {
    let environment = OWNER_ENVIRONMENT.lock().await;
    std::env::set_var(INHERITED, "one");
    let (temp, store, runner) = fixture().await;
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).unwrap();
    fixture_git(&checkout, &["init", "-q"]).await;
    fixture_git(&checkout, &["config", "user.name", "Check test"]).await;
    fixture_git(
        &checkout,
        &["config", "user.email", "check@example.invalid"],
    )
    .await;
    std::fs::write(checkout.join("tracked"), "one\n").unwrap();
    std::fs::write(checkout.join(".gitignore"), "build/\n").unwrap();
    fixture_git(&checkout, &["add", "."]).await;
    fixture_git(&checkout, &["commit", "-q", "-m", "candidate"]).await;
    let head = git::get_current_sha(&checkout).await.unwrap();
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t2','p','second','review',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    // Another Task's worktree of the same repository.
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w2','t2','r',?,'task/second','ready',?,?)").bind(temp.path().join("second").to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'ready','scheduler','{}',?,?)").bind(checkout.to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let owners = Arc::new(super::owners::WorkspaceCheckOwners::new(
        store.clone(),
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        std::time::Duration::from_secs(60),
    ));
    let worker = CheckRunWorker::new(store.clone(), owners);
    Canonical {
        temp,
        store,
        runner,
        worker,
        checkout,
        head,
        _environment: environment,
    }
}
impl Canonical {
    /// The step appends one line per execution, outside the checkout: the
    /// number of lines is the number of times the executor ran it.
    pub(super) fn counting_step(&self) -> String {
        format!(
            "echo run >> '{}'",
            self.temp.path().join("executions").display()
        )
    }
    pub(super) fn executions(&self) -> usize {
        std::fs::read_to_string(self.temp.path().join("executions"))
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }
    pub(super) async fn project_environment(&self) -> std::collections::BTreeMap<String, String> {
        review::contract::project_environment(&self.store, "t")
            .await
            .unwrap()
            .env
    }
    pub(super) async fn set_project_value(&self, value: &str) {
        sqlx::query("UPDATE project SET settings=? WHERE id='p'")
            .bind(serde_json::json!({"environment":{"env":{"FLAVOR":value}}}).to_string())
            .execute(self.store.pool())
            .await
            .unwrap();
    }
    /// What the review-entry family will state: the canonical spec of the
    /// steps, scoped to the Task worktree, in the environment in force now.
    pub(super) async fn request_in(
        &self,
        key: &str,
        workspace: &str,
        steps: &[&str],
    ) -> CheckRunRequest {
        let project = self.project_environment().await;
        let steps = steps.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        let mut spec = check_executor::legacy_ci_spec("template", &project, 0, false);
        let template = spec.commands[0].clone();
        spec.commands = steps
            .iter()
            .enumerate()
            .map(|(index, text)| {
                let mut command = template.clone();
                command.id = format!("ci:{index}");
                command.shell_text = text.clone();
                command
            })
            .collect();
        spec.configured_commands = spec.commands.len();
        spec.execution_policy = CANONICAL_CI_POLICY.into();
        spec.scope = CheckScope::Workspace {
            workspace_id: workspace.into(),
            generation: 1,
        };
        for command in &mut spec.commands {
            command.cacheability = CheckCacheability::DeclaredControlledInputs;
        }
        let mut req = request(key);
        req.task_id = Some(if workspace == "w" { "t" } else { "t2" }.into());
        req.workspace_id = Some(workspace.into());
        req.identity.commit_sha = self.head.clone();
        req.identity.inputs = super::policy::digest_input(&self.store, spec, &project)
            .await
            .unwrap();
        req
    }
    pub(super) async fn request(&self, key: &str, step: &str) -> CheckRunRequest {
        self.request_in(key, "w", &[step]).await
    }
    pub(super) async fn ask(&self, req: CheckRunRequest) -> RequestedCheck {
        self.runner.request(req).await.unwrap()
    }
    pub(super) async fn drive(&self, run: &StoredCheckRun) -> StoredCheckResult {
        self.worker
            .drive(admit(&self.store, run).await)
            .await
            .unwrap();
        let id: String = sqlx::query_scalar(
            "SELECT id FROM check_result WHERE run_id=? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&run.id)
        .fetch_one(self.store.pool())
        .await
        .unwrap();
        self.store.check_result(&id).await.unwrap().unwrap()
    }
    pub(super) async fn runs(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
            .fetch_one(self.store.pool())
            .await
            .unwrap()
    }
}
/// A second fixture in one test: the first one's lock is released first.
pub(super) async fn canonical_fixture_after(first: Canonical) -> Canonical {
    drop(first);
    canonical_fixture().await
}
