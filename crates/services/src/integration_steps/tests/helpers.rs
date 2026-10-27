//! Shared fixtures and assertions for integration steps.
use super::*;

pub(super) async fn git_out(path: &Path, args: &[&str]) -> Option<String> {
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

pub(in crate::integration_steps) struct World {
    pub temp: tempfile::TempDir,
    pub db: Arc<SqliteDb>,
    pub bus: Arc<events::EventBus>,
    pub service: crate::TaskService,
    pub port: Arc<TaskStepIntegrationPort>,
    pub(super) stop: tokio::sync::watch::Sender<bool>,
    pub(super) step_worker: Option<tokio::task::JoinHandle<()>>,
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
pub(super) type StuckAttempt = (String, i64, Option<String>, Option<String>, Option<String>);
/// `seq, kind, causation_key, status, attempts, available_at, last_error,
/// payload` of a Task step.
pub(super) type StuckStep = (
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
pub(in crate::integration_steps) async fn eventually<F, Fut>(what: &str, mut check: F)
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
pub(super) async fn settled(
    world: &World,
    task: &str,
    action: IntegrationStepAction,
    status: &str,
) {
    eventually(&format!("{} step {status}", action.as_str()), || async {
        world
            .step(task, action)
            .await
            .last()
            .is_some_and(|step| step.status == status)
    })
    .await;
}
pub(super) fn ack(
    attempt: &IntegrationAttempt,
    action: IntegrationStepAction,
) -> Option<IntegrationStepAck> {
    IntegrationStepAck::current(attempt, action)
}
pub(super) fn integration(task: &db::Task) -> Value {
    json!(task.condition.integration_reason())
}

/// Settle with a permit, then let the scripted worker record the
/// fast-forward and ready the result step.
pub(super) async fn land(world: &World, task: &str, attempt_id: &str) -> IntegrationAttempt {
    let asked = world.place(attempt_id, "awaiting_task_step", 1).await;
    world.ask(&asked, IntegrationStepAction::Settle).await;
    settled(world, task, IntegrationStepAction::Settle, "done").await;
    world.apply(attempt_id).await;
    world.attempt(attempt_id).await
}

/// One `result` delivery by the real step worker at the attempt's present
/// `effect_seq`: returns once the step answered or parked itself to ask
/// again (`true`: answered).
pub(super) async fn deliver_result(world: &World, attempt: &IntegrationAttempt) -> bool {
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
pub(super) fn reads(sha: &str) -> usize {
    super::proof::test_reads::of(sha)
}

pub(super) async fn unresolved(world: &World, task: &str) {
    let task = world.task(task).await;
    assert_eq!(task.status, "merging", "never done without the proof");
    assert_eq!(integration(&task)["kind"], "deferred");
    assert_eq!(integration(&task)["cause"], "unresolved_result");
}

/// One round of the race: a `settle` step and an owner Cancel start together.
/// Returns whether the permit won.
pub(super) async fn race_once(world: &World, task: &'static str) -> bool {
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

pub(super) fn application(
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

pub(super) struct GitFacts {
    pub(super) db: Arc<SqliteDb>,
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
pub(super) struct NoTransfer;
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

/// The worker over its production ports (`build_integration_worker`): the
/// routing owner, the workspace facts, the owner object transfer, and the
/// real Task-step port. Also composes the real check runner and its worker,
/// as the server runtime does.
pub(super) struct Production {
    pub(super) worker: Arc<IntegrationQueueWorker>,
    pub(super) stop: tokio::sync::watch::Sender<bool>,
    pub(super) _checks: tokio::task::JoinHandle<()>,
}
impl Drop for Production {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
/// `build_integration_worker` over this World, with fast timers.
pub(super) fn production_ports(
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

pub(super) fn production_ports_config(
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
    pub(super) fn new(world: &World) -> Self {
        Self::with_ports(world, production_ports(world))
    }
    pub(super) fn with_ports(
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
    pub(super) fn run(&self) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(self.worker.clone().run(self.stop.subscribe()))
    }
}

pub(super) async fn review_ci(world: &World, task: &str, steps: &[&str]) {
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"review":{"ci_steps":steps}}).to_string())
        .bind(task)
        .execute(world.db.pool())
        .await
        .unwrap();
}

impl World {
    pub(super) fn clone_path(&self) -> PathBuf {
        self.temp.path().join("clone-r")
    }
    /// A second server-owned checkout of the repo: its own clone, with its
    /// own object store and refs, registered as a non-default location.
    pub(super) async fn add_clone(&self) {
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
    pub(super) async fn add_task_in_clone(
        &self,
        task: &str,
        file: &str,
        content: &str,
    ) -> IntegrationAttempt {
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

impl World {
    /// An agent that holds the reviewer role of `task`.
    pub(super) async fn seed_reviewer(&self, task: &str) -> String {
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
    pub(super) async fn seed_execution(
        &self,
        task: &str,
        role: &str,
        agent: Option<&str>,
    ) -> String {
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
    pub(super) async fn seed_passed_review(&self, task: &str, reviewed_paths: &[&str]) -> String {
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

/// A digest of every row of every table except the three the worker may
/// write: the queue, the attempt and the Task-step queue (with
/// `task_schedule_dirty`, which the `task_step` triggers maintain on every
/// step insert: the scheduler's "look at this Task" mark).
pub(super) async fn foreign_digest(db: &SqliteDb) -> std::collections::BTreeMap<String, String> {
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

impl World {
    /// A Task in `status` as a server without the queue left it: one commit
    /// writing `file` in its worktree, no attempt. Returns that commit.
    pub(super) async fn legacy_task(&self, task: &str, file: &str, status: &str) -> String {
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
    pub(super) async fn old_path_rebase(&self, task: &str) -> String {
        std::fs::write(self.repo().join("moved"), "moved\n").unwrap();
        git::commit_all(&self.repo(), "target moved").await.unwrap();
        git_out(&self.tree(task), &["rebase", "main"])
            .await
            .expect("clean rebase");
        git::get_current_sha(&self.tree(task)).await.unwrap()
    }
    /// The settled hooks step of the old `merging` entry, with `effects` as
    /// its retained checkpoint.
    pub(super) async fn old_merge_step(&self, task: &str, effects: Value) {
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
    pub(super) async fn imported(&mut self, task: &str) -> (IntegrationAttempt, u64) {
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
    pub(super) async fn carries(&self, task: &str) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT commit_sha,base_sha FROM review_authority_carry WHERE task_id=? ORDER BY created_at",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap()
    }
}

/// What the importer and its continuation leave for one pre-upgrade shape.
pub(super) struct ImportedRow {
    pub(super) merged: bool,
    /// Every attempt of the Task, for the failure message.
    pub(super) trail: String,
    pub(super) priority: u64,
    pub(super) state: S,
    pub(super) status: String,
    /// The typed integration reason on the Task's condition, if any.
    pub(super) deferred: Option<String>,
    pub(super) failed_steps: usize,
    /// `(verb, reason)` of every action the Task offers its owner.
    pub(super) offers: Vec<(String, String)>,
    /// The Task's whole condition, for the failure message.
    pub(super) condition: String,
}

/// Build the legacy shape `seed` describes for Task `t`, run the start-time
/// import twice, let the continuation step settle (the real step worker and
/// the real queue worker run), and report. Nothing may merge: every row here
/// is one the queue must NOT continue by itself.
pub(super) async fn import_row(status: &str, seed: &str) -> ImportedRow {
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

pub(super) async fn review_entry_without_an_environment_answer(owner_asked: bool) {
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
