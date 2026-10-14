use super::*;
use api_types::{
    StateDefinition, StateHooks, StateKind, WorkflowDefinition, WorkflowTrigger,
    WorkflowTriggerDefinition,
};
use db::{EnqueueTaskStep, TaskStepRepo};

#[derive(Clone, Copy, PartialEq)]
pub(super) enum CommandPoint {
    AfterClaim,
    BeforeCommand,
    BeforeSettle,
    BeforeReply,
    BeforeRelease,
    BeforeSend,
}

#[derive(Clone, Copy, PartialEq)]
enum CommandFault {
    Pause,
    Panic,
    Retry,
    /// The database is busy once at the point.
    Busy,
    /// The claimer is slow at the point.
    Delay,
}

/// A real SQLITE_BUSY as the worker classifies it.
pub(super) fn busy() -> ServiceError {
    #[derive(Debug)]
    struct Busy;
    impl std::fmt::Display for Busy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("database is locked")
        }
    }
    impl std::error::Error for Busy {}
    impl sqlx::error::DatabaseError for Busy {
        fn message(&self) -> &str {
            "database is locked"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some("5".into())
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
    ServiceError::Db(db::DbError::Sqlx(sqlx::Error::Database(Box::new(Busy))))
}

/// A per-worker barrier: no global timing or interference between tests.
pub(super) struct CommandGate {
    point: CommandPoint,
    first: std::sync::atomic::AtomicBool,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    fault: CommandFault,
    job: std::sync::Mutex<Option<JoinHandle<()>>>,
}
impl CommandGate {
    fn new(point: CommandPoint, fault: CommandFault) -> Arc<Self> {
        Arc::new(Self {
            point,
            first: std::sync::atomic::AtomicBool::new(true),
            arrived: Default::default(),
            resume: Default::default(),
            fault,
            job: Default::default(),
        })
    }
    pub(super) fn keep_job(&self, job: JoinHandle<()>) {
        let mut kept = self.job.lock().unwrap();
        if kept.is_none() {
            *kept = Some(job);
        }
    }
    pub(super) async fn pause(
        &self,
        db: &db::SqliteDb,
        step: &TaskStep,
        point: CommandPoint,
    ) -> bool {
        if step.kind != "command"
            || step.attempts != 1
            || point != self.point
            || !self.first.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return false;
        }
        // A short real lease exercises the wall-clock lease read without
        // waiting on the production 60-second lease or simulating SQLite.
        let until = (chrono::Utc::now() + chrono::Duration::milliseconds(500)).to_rfc3339();
        sqlx::query("UPDATE task_step SET lease_until=? WHERE id=? AND claimed_by=?")
            .bind(until)
            .bind(&step.id)
            .bind(&step.claimed_by)
            .execute(db.pool())
            .await
            .unwrap();
        db.domain_event_notify().notify_waiters();
        self.arrived.notify_one();
        self.resume.notified().await;
        assert!(
            self.fault != CommandFault::Panic,
            "injected claimer panic after claim"
        );
        self.fault == CommandFault::Retry
    }
    /// A fault that needs no test-side resume. True: fail busy here.
    pub(super) async fn inject(&self, step: &TaskStep, point: CommandPoint) -> bool {
        if step.kind != "command"
            || point != self.point
            || !self.first.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return false;
        }
        if self.fault == CommandFault::Delay {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.fault == CommandFault::Busy
    }
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(3), self.arrived.notified())
            .await
            .expect("the claimer reached the barrier");
    }
    fn take_job(&self) -> JoinHandle<()> {
        self.job.lock().unwrap().take().unwrap()
    }
}

fn transition_command() -> crate::task_service::commands::TaskCommand {
    crate::task_service::commands::TaskCommand {
        operation: "engine_transition".into(),
        preempt: false,
        arguments: serde_json::json!({"task_id":"t","target_state":"done","version":1,"workflow":workflow(),"actor":cascade_actor(),"reason":"bounded command waiter","rejection":false,"skip_before_exit":false,"defer_dispatch_until":null,"board_move":null,"authority":null,"entry_retry":false}),
    }
}

#[tokio::test]
async fn open_item_4_durable_command_errors_preserve_inline_kinds_and_fields() {
    let errors: Vec<Box<dyn Fn() -> ServiceError>> = vec![
        Box::new(|| db::DbError::VersionConflict.into()),
        Box::new(|| {
            db::DbError::TaskVersionConflict {
                expected: 2,
                actual: 3,
            }
            .into()
        }),
        Box::new(|| ServiceError::not_found("execution", "missing")),
        Box::new(|| ServiceError::invalid_operation("deterministic refusal")),
        Box::new(|| ServiceError::AuthorizationDenied {
            message: "policy refused the command".into(),
        }),
        Box::new(|| ServiceError::GuardRejection {
            guard: "dependency_gate".into(),
            reason: "unfinished dependency".into(),
        }),
        Box::new(|| ServiceError::TaskBusy {
            pending_steps: 3,
            retry_after_ms: 250,
        }),
    ];
    for make_error in errors {
        let mut answers = Vec::new();
        for durable in [false, true] {
            let (db, worker) = fixture().await;
            test_hooks::inject_command_error(&worker.task_service, "t", make_error(), durable);
            let result = Arc::new(worker)
                .request_command::<serde_json::Value>("t", transition_command())
                .await;
            answers.push(format!("{:?}", result.unwrap_err()));
            let rows = db.task_steps("t").await.unwrap();
            assert_eq!(rows[0].status, "failed");
            assert_eq!(rows[0].attempts, 1, "a completed command is never repeated");
        }
        assert_eq!(
            answers[0], answers[1],
            "durable reply changed the error category or fields"
        );
    }
}

#[tokio::test]
async fn open_item_4_a_command_failure_before_its_handler_has_a_typed_durable_reply() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.kind = "command".into();
    input.payload_json = transition_command().payload_json().unwrap();
    db.enqueue_step(&input).await.unwrap();
    let mut claimed = db
        .claim_step("before-handler", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    claimed.attempts = MAX_STEP_ATTEMPTS;
    sqlx::query("UPDATE task_step SET attempts=? WHERE id=?")
        .bind(MAX_STEP_ATTEMPTS)
        .bind(&claimed.id)
        .execute(db.pool())
        .await
        .unwrap();
    let error = ServiceError::Db(db::DbError::VersionConflict);
    assert!(!db::task_writer::in_task_step(
        claimed.clone(),
        worker.settle_failed_attempt(&claimed, &error)
    )
    .await
    .unwrap());
    let stored = db.task_steps("t").await.unwrap().remove(0);
    assert_eq!(stored.status, "failed");
    let reply: command_reply::CommandReply = serde_json::from_str(
        &stored
            .result_json
            .expect("terminal command errors have a durable reply"),
    )
    .unwrap();
    assert!(matches!(
        reply.decode::<serde_json::Value>(),
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
}

/// Forced order for the durable-reply hook: the waiter is parked in its
/// select when the claimer releases the settled step and loses the live
/// reply. It answers from the durable row; it never polls a spent receiver.
#[tokio::test]
async fn lost_live_reply_while_the_waiter_is_parked_is_answered_from_the_durable_row() {
    let (db, mut worker) = fixture().await;
    test_hooks::inject_command_error(
        &worker.task_service,
        "t",
        db::DbError::VersionConflict.into(),
        true,
    );
    let gate = CommandGate::new(CommandPoint::BeforeReply, CommandFault::Pause);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<serde_json::Value>("t", transition_command())
                .await
        }
    });
    // The claimer has settled the step and holds it, before its release.
    gate.wait().await;
    // The waiter reads the settled, still-leased row and parks in its select.
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.resume.notify_one();
    let answer = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("the waiter answers")
        .expect("the waiter does not panic on a lost live reply");
    assert!(
        matches!(answer, Err(ServiceError::Db(db::DbError::VersionConflict))),
        "{answer:?}"
    );
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "failed");
    assert_eq!(rows[0].attempts, 1, "a completed command is never repeated");
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn command_waiter_reclaims_a_claimer_that_panics_after_claim() {
    let (db, mut worker) = fixture().await;
    let gate = CommandGate::new(CommandPoint::AfterClaim, CommandFault::Panic);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let mut waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<crate::workflow::engine::TransitionResult>(
                    "t",
                    transition_command(),
                )
                .await
        }
    });
    gate.wait().await;
    // The claimant is still alive here and its step is claimed. The
    // channel sender remains registered across its panic.
    gate.resume.notify_one();
    assert!(gate.take_job().await.unwrap_err().is_panic());
    let answer = tokio::time::timeout(Duration::from_secs(3), &mut waiter).await;
    if answer.is_err() {
        waiter.abort();
        let _ = waiter.await;
        panic!("command waiter hung after the claimer panicked and its lease lapsed");
    }
    assert_eq!(answer.unwrap().unwrap().unwrap().task.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    let command = rows.iter().find(|step| step.kind == "command").unwrap();
    assert_eq!(command.status, "done");
    assert_eq!(command.attempts, 2);
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn command_waiter_recovers_committed_reply_after_claimer_is_cancelled() {
    let (db, mut worker) = fixture().await;
    let gate = CommandGate::new(CommandPoint::BeforeReply, CommandFault::Pause);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<crate::workflow::engine::TransitionResult>(
                    "t",
                    transition_command(),
                )
                .await
        }
    });
    gate.wait().await;
    let job = gate.take_job();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let answer = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("a committed reply is recovered at lease expiry")
        .unwrap()
        .unwrap();
    assert_eq!(answer.task.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    let command = rows.iter().find(|step| step.kind == "command").unwrap();
    assert_eq!(command.status, "done");
    assert_eq!(command.attempts, 1, "the effect is never repeated");
    assert!(command.result_json.is_some());
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

async fn transition_under(
    point: CommandPoint,
    fault: CommandFault,
    command: crate::task_service::commands::TaskCommand,
) -> (
    Arc<db::SqliteDb>,
    Arc<TaskStepWorker>,
    Result<crate::workflow::engine::TransitionResult>,
) {
    let (db, mut worker) = fixture().await;
    worker.command_test_gate = Some(CommandGate::new(point, fault));
    let worker = Arc::new(worker);
    let answer = tokio::time::timeout(
        Duration::from_secs(10),
        worker.request_command::<crate::workflow::engine::TransitionResult>("t", command),
    )
    .await
    .expect("the command waiter is bounded");
    // The inline job releases after it answers.
    for _ in 0..200 {
        if db.active_step_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (db, worker, answer)
}

/// The command ran and committed its effect; SQLite is busy when the step
/// is settled. Re-queueing the step would run the command a second time.
#[tokio::test]
async fn command_that_ran_is_not_run_again_when_its_settlement_finds_the_database_busy() {
    let (db, worker, answer) = transition_under(
        CommandPoint::BeforeSettle,
        CommandFault::Busy,
        transition_command(),
    )
    .await;
    assert_eq!(
        answer.expect("the command's own answer").task.status,
        "done"
    );
    let rows = db.task_steps("t").await.unwrap();
    let command = rows.iter().find(|step| step.kind == "command").unwrap();
    assert_eq!(command.attempts, 1, "the effect is never repeated");
    assert_eq!(command.status, "done");
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

/// The command is settled `done`; only the release of its lease finds the
/// database busy. The caller gets the committed result, not that error.
#[tokio::test]
async fn committed_command_answers_its_result_when_the_release_finds_the_database_busy() {
    let (db, _worker, answer) = transition_under(
        CommandPoint::BeforeRelease,
        CommandFault::Busy,
        transition_command(),
    )
    .await;
    assert_eq!(answer.expect("the committed result").task.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    let command = rows.iter().find(|step| step.kind == "command").unwrap();
    assert_eq!((command.status.as_str(), command.attempts), ("done", 1));
    assert_eq!(command.lease_until, None, "the release is repeated");
}

/// A waiter that reads the released step before the claimer has sent the
/// reply must still return the command's typed error, not the stored
/// summary of it.
#[tokio::test]
async fn released_command_error_keeps_its_type_when_the_waiter_reads_before_the_reply() {
    let mut command = transition_command();
    command.arguments["target_state"] = serde_json::json!("no_such_state");
    let (_db, _worker, typed) = transition_under(
        CommandPoint::BeforeSettle,
        CommandFault::Delay,
        command.clone(),
    )
    .await;
    let typed = format!("{:?}", typed.err().expect("an unknown state is refused"));
    let (_db, _worker, raced) =
        transition_under(CommandPoint::BeforeSend, CommandFault::Delay, command).await;
    assert_eq!(format!("{:?}", raced.err().expect("refused")), typed);
}

fn read_current_task_command() -> crate::task_service::commands::TaskCommand {
    crate::task_service::commands::TaskCommand {
        operation: "block_cancelled_dependencies".into(),
        arguments: serde_json::json!(["t", []]),
        preempt: false,
    }
}

#[tokio::test]
async fn command_waiter_dropped_while_claimed_does_not_wedge_the_next_command() {
    let (db, mut worker) = fixture().await;
    let gate = CommandGate::new(CommandPoint::AfterClaim, CommandFault::Pause);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<crate::workflow::engine::TransitionResult>(
                    "t",
                    transition_command(),
                )
                .await
        }
    });
    gate.wait().await;
    waiter.abort();
    assert!(waiter
        .await
        .err()
        .expect("waiter was aborted")
        .is_cancelled());
    assert!(
        worker
            .task_service
            .task_step_replies
            .lock()
            .unwrap()
            .is_empty(),
        "a dropped request removes its sender"
    );
    // Also lose the claimer: the next command's predecessor has no owner or
    // waiter left to announce that the lease lapsed.
    let job = gate.take_job();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let next = tokio::time::timeout(
        Duration::from_secs(3),
        worker.request_command::<db::Task>("t", read_current_task_command()),
    )
    .await
    .expect("the next command takes over its abandoned predecessor")
    .unwrap();
    assert_eq!(next.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    let commands: Vec<_> = rows.iter().filter(|step| step.kind == "command").collect();
    assert_eq!(commands.len(), 2);
    assert!(commands.iter().all(|step| step.status == "done"));
    assert_eq!(commands[0].attempts, 2);
    assert_eq!(commands[1].attempts, 1);
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

/// A request dropped while the commit of its inline claim is in flight (an
/// aborted HTTP request) must not leave the step claimed for a whole lease
/// with no runner: the next command of the Task would answer `task_busy`.
/// The waiter is polled by hand and dropped after one more poll each round,
/// so every point where it can be suspended is covered.
#[tokio::test]
async fn command_waiter_dropped_at_any_await_leaves_no_claim_without_a_runner() {
    use std::{future::Future, task::Poll};
    let (db, worker) = fixture().await;
    let worker = Arc::new(worker);
    for round in 1..400usize {
        let mut waiter =
            Box::pin(worker.request_command::<db::Task>("t", read_current_task_command()));
        let mut finished = false;
        for _ in 0..round {
            finished =
                std::future::poll_fn(|cx| Poll::Ready(waiter.as_mut().poll(cx).is_ready())).await;
            if finished {
                break;
            }
            // Let SQLite finish what the waiter asked for before the waiter
            // is polled again or dropped.
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        drop(waiter);
        let next = tokio::time::timeout(
            Duration::from_secs(20),
            worker.request_command::<db::Task>("t", read_current_task_command()),
        )
        .await
        .expect("the next command is bounded");
        if let Err(error) = next {
            let queue: Vec<_> = db
                .task_steps("t")
                .await
                .unwrap()
                .iter()
                .filter(|step| step.status != "done")
                .map(|step| {
                    format!(
                        "{} attempts={} lease={:?} runner={}",
                        step.status,
                        step.attempts,
                        step.lease_until,
                        db.step_is_active(step)
                    )
                })
                .collect();
            panic!("round {round}: {error:?}; queue: {queue:?}");
        }
        assert!(
            worker
                .task_service
                .task_step_replies
                .lock()
                .unwrap()
                .is_empty(),
            "round {round}: no reply registration is left"
        );
        if finished {
            return;
        }
    }
    panic!("the waiter never finished");
}

#[tokio::test]
async fn command_waiter_returns_typed_busy_under_a_live_owner_that_completes_later() {
    let (db, mut worker) = fixture().await;
    db.set_claimed_task_step_wait(Duration::from_secs(1));
    let gate = CommandGate::new(CommandPoint::AfterClaim, CommandFault::Pause);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<crate::workflow::engine::TransitionResult>(
                    "t",
                    transition_command(),
                )
                .await
        }
    });
    gate.wait().await;
    let answer = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("a live claimer's waiter has a ceiling")
        .unwrap();
    assert!(
        matches!(
            answer,
            Err(ServiceError::TaskBusy {
                pending_steps: 1,
                retry_after_ms: 250
            })
        ),
        "expected typed busy, got {:?}",
        answer.err()
    );
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "claimed");
    assert_eq!(
        rows[0].attempts, 1,
        "a live owner is never stolen at lease expiry"
    );
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
    gate.resume.notify_one();
    gate.take_job().await.unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "done");
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A command whose attempt failed before it ran and was re-queued is still
/// accepted: it runs on its retry. Its caller is not answered with the
/// attempt's transient error (a caller told "error" asks again, and both
/// commands then run); it waits, within its bound, for the retry's answer.
#[tokio::test]
async fn command_waiter_gets_the_answer_of_its_requeued_command_not_the_attempt_error() {
    let (db, mut worker) = fixture().await;
    let gate = CommandGate::new(CommandPoint::BeforeCommand, CommandFault::Retry);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<db::Task>("t", read_current_task_command())
                .await
        }
    });
    gate.wait().await;
    gate.resume.notify_one();
    let answer = tokio::time::timeout(Duration::from_secs(4), waiter)
        .await
        .expect("the retry is driven at its back-off, inside the waiter's bound")
        .unwrap();
    assert_eq!(
        answer.expect("the re-queued command's own answer").status,
        "todo"
    );
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].status.as_str(), rows[0].attempts), ("done", 2));
    assert!(worker
        .task_service
        .task_step_replies
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn command_waiter_re_registers_a_lost_sender_for_its_live_step() {
    let (db, mut worker) = fixture().await;
    let gate = CommandGate::new(CommandPoint::AfterClaim, CommandFault::Pause);
    worker.command_test_gate = Some(gate.clone());
    let worker = Arc::new(worker);
    let waiter = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .request_command::<db::Task>("t", read_current_task_command())
                .await
        }
    });
    gate.wait().await;
    let replies = &worker.task_service.task_step_replies;
    let id = db.task_steps("t").await.unwrap()[0].id.clone();
    // Lose the sender after removal, before sending the attempt's answer.
    drop(replies.lock().unwrap().remove(&id).unwrap());
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if replies.lock().unwrap().contains_key(&id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a live step gets a replacement reply sender");
    assert_eq!(replies.lock().unwrap().len(), 1);
    gate.resume.notify_one();
    let answer = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    gate.take_job().await.unwrap();
    assert_eq!(answer.status, "todo");
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "done");
    assert_eq!(rows[0].attempts, 1);
    assert!(replies.lock().unwrap().is_empty());
}

async fn fixture() -> (Arc<db::SqliteDb>, TaskStepWorker) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    db.set_claimed_task_step_wait(Duration::from_secs(4));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','task','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    sqlx::query(
        "INSERT INTO task_step_workflow(id,definition_json,last_used_at) VALUES ('fixture',?,?)",
    )
    .bind(serde_json::to_string(&workflow()).unwrap())
    .bind(db::now_rfc3339())
    .execute(db.pool())
    .await
    .unwrap();
    let worker = TaskStepWorker::new(service.clone());
    (db, worker)
}

#[tokio::test]
async fn open_item_1_transient_repair_guard_uses_the_step_attempt_bound() {
    let (db, worker) = fixture().await;
    let service =
        worker
            .task_service
            .clone()
            .with_merge_service(Arc::new(crate::MergeService::new(
                db.clone(),
                std::path::PathBuf::from("unused"),
            )));
    let worker = TaskStepWorker::new(service);
    let mut definition = workflow();
    definition.states[0].name = "merge_failed".into();
    definition.states[0]
        .triggers
        .get_mut(&WorkflowTrigger::Accept)
        .unwrap()
        .to = "review".into();
    definition.states[0]
        .hooks
        .before_exit
        .push(api_types::HookSpec {
            action: "require_conflict_markers_resolved".into(),
            params: serde_json::json!({}),
            on_failure: api_types::FailurePolicy::Block,
            applies_to: api_types::HookAudience::All,
        });
    definition.states[1].name = "review".into();
    sqlx::query("UPDATE task SET status='merge_failed' WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task_step_workflow SET definition_json=? WHERE id='fixture'")
        .bind(serde_json::to_string(&definition).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let mut input = step("review");
    input.expected_status = "merge_failed".into();
    db.enqueue_step(&input).await.unwrap();
    for attempt in 1..=MAX_STEP_ATTEMPTS {
        db.ready_step(&input.id).await.unwrap();
        let claimed = db
            .claim_step("guard-test", Some("t"), &lease_deadline())
            .await
            .unwrap()
            .unwrap();
        let error = match attempt % 4 {
            0 => busy(),
            1 => ServiceError::Db(db::DbError::VersionConflict),
            2 => ServiceError::Git(git::GitError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "checkout read interrupted",
            ))),
            _ => ServiceError::Db(db::DbError::Sqlx(sqlx::Error::Io(std::io::Error::other(
                "checkout read interrupted",
            )))),
        };
        crate::workflow::actions::guard_test_faults::inject(&input.id, error);
        worker.execute(claimed).await.unwrap();
        let stored = db.task_steps("t").await.unwrap().remove(0);
        assert_eq!(stored.attempts, attempt);
        let task = TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap();
        if attempt < MAX_STEP_ATTEMPTS {
            assert_eq!(
                stored.status, "pending",
                "a transient guard error must retry"
            );
            assert!(task.error_annotation.is_none(), "no park before the bound");
        } else {
            assert_eq!(stored.status, "failed");
            assert_eq!(
                task.condition.read().failure_kind,
                Some(api_types::FailureKind::CascadeFailed)
            );
        }
    }
}

#[tokio::test]
async fn open_item_1_refresh_precondition_retries_without_checkpointing_the_failure() {
    let (db, worker) = fixture().await;
    let mut definition = workflow();
    definition.states[1].name = "merge_failed".into();
    definition.states[1].kind = StateKind::Active;
    definition.states[1]
        .hooks
        .on_enter
        .push(api_types::HookSpec {
            action: "dispatch_role_agent".into(),
            params: serde_json::json!({}),
            on_failure: api_types::FailurePolicy::Log,
            applies_to: api_types::HookAudience::All,
        });
    definition.states[0]
        .triggers
        .get_mut(&WorkflowTrigger::Accept)
        .unwrap()
        .to = "merge_failed".into();
    let mut command = transition_command();
    command.arguments["workflow"] = serde_json::json!(definition);
    command.arguments["target_state"] = serde_json::json!("merge_failed");
    let worker = Arc::new(worker);
    let entered = worker
        .request_command::<crate::workflow::engine::TransitionResult>("t", command)
        .await
        .unwrap();
    let id = entered.queued_step_id.unwrap();
    crate::workflow::refresh_read_faults::inject(&id, db::DbError::VersionConflict);
    let claimed = db
        .claim_step("refresh-test", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, id);
    worker.execute(claimed).await.unwrap();
    let failed_read = db
        .task_steps("t")
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    assert_eq!(failed_read.status, "pending");
    assert_eq!(failed_read.attempts, 1);
    assert!(TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap()
        .error_annotation
        .is_none());
    db.ready_step(&id).await.unwrap();
    let claimed = db
        .claim_step("refresh-test", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    worker.execute(claimed).await.unwrap();
    let retried = db
        .task_steps("t")
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    assert_eq!(retried.status, "done");
    assert_eq!(retried.attempts, 2);
}
fn workflow() -> WorkflowDefinition {
    let mut from = StateDefinition {
        name: "todo".into(),
        column: "todo".into(),
        display_name: "Todo".into(),
        kind: StateKind::Initial,
        role: None,
        hooks: StateHooks::default(),
        triggers: Default::default(),
        gate_config: None,
        config: serde_json::json!({}),
        dispatch: None,
        cleanup: None,
        canonical_phase: None,
    };
    from.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".into(),
            dispatch: None,
        },
    );
    let mut done = from.clone();
    done.name = "done".into();
    done.kind = StateKind::Terminal;
    done.triggers.clear();
    WorkflowDefinition {
        roles: vec![],
        states: vec![from, done],
        configuration: vec![],
        cancellation_state: None,
    }
}
fn step(to: &str) -> EnqueueTaskStep {
    EnqueueTaskStep {
        kind: "cascade".into(),
        id: db::new_uuid_v4(),
        task_id: "t".into(),
        payload_json: serde_json::to_string(&CascadePayload {
            bridge: Default::default(),
            to: to.into(),
            reason: "automatic".into(),
            rejection: false,
            skip_before_exit: false,
            workflow_ref: WorkflowReference::Snapshot("fixture".into()),
            clear_review_passed_at_on_commit: false,
            admission_agent_id: None,
            evidence: None,
        })
        .unwrap(),
        causation_step_id: None,
        causation_key: "commit".into(),
        chain_id: "chain".into(),
        chain_position: 1,
        expected_status: "todo".into(),
        expected_version: 1,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: db::now_rfc3339(),
    }
}
#[tokio::test]
async fn deterministic_refusal_fails_and_records_recovery_annotation() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("undefined")).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "todo");
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "cascade_failed");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("undefined"));
    let steps = db.task_steps("t").await.unwrap();
    assert_eq!(steps[0].status, "failed");
    assert_eq!(steps[0].attempts, 1);
    assert!(steps[0].completed_at.is_some());
    assert_eq!(db.pending_steps("t").await.unwrap(), 0);
}
#[tokio::test]
async fn reclaimed_step_drains_after_worker_restart_and_completes_atomically() {
    let (db, _worker) = fixture().await;
    let input = step("done");
    db.enqueue_step(&input).await.unwrap();
    db.claim_step("dead-process", Some("t"), "2000-01-01T00:00:00+00:00")
        .await
        .unwrap()
        .unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    let restarted = TaskStepWorker::new(service.clone());
    assert_eq!(restarted.drain("t").await.unwrap().status, "done");
    let completed = &db.task_steps("t").await.unwrap()[0];
    assert_eq!(completed.status, "done");
    assert_eq!(completed.attempts, 2);
    assert!(completed.claimed_by.is_none());
    assert_eq!(
        db::TransitionLogRepo::list_by_task(&*db, "t")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn drain_waits_for_status_step_lease_handover() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    let claimed = db
        .claim_step("owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    let payload = serde_json::from_str(&claimed.payload_json).unwrap();
    worker
        .engine
        .bind(&worker.task_service)
        .transition_step(&claimed, &payload, &workflow(), None)
        .await
        .unwrap();
    assert_eq!(db.pending_steps("t").await.unwrap(), 0);
    // Keep SQLx acquisition polling on its own task while the test waits;
    // cancelling it could recycle the sole in-memory database connection.
    let mut drain = tokio::spawn(async move { worker.drain("t").await });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut drain)
        .await
        .is_err());
    db.release_step(&claimed.id, "owner").await.unwrap();
    assert_eq!(drain.await.unwrap().unwrap().status, "done");
}
#[test]
fn retry_policy_distinguishes_availability_and_deterministic_refusals() {
    assert!(retryable(&ServiceError::RateLimited {
        retry_after_seconds: 1
    }));
    assert!(retryable(&ServiceError::Db(db::DbError::VersionConflict)));
    assert!(!retryable(&ServiceError::invalid_operation(
        "undefined state"
    )));
    assert!(!retryable(&ServiceError::GuardRejection {
        guard: "check".into(),
        reason: "failed".into()
    }));
}

#[tokio::test]
async fn failed_done_write_rolls_back_engine_status_cas_and_transition_log() {
    let (db, worker) = fixture().await;
    db.enqueue_step(&step("done")).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_step_done BEFORE UPDATE OF status ON task_step WHEN NEW.status='done' BEGIN SELECT RAISE(ABORT,'injected completion failure'); END")
        .execute(db.pool()).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "todo");
    assert!(db::TransitionLogRepo::list_by_task(&*db, "t")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "failed");
    assert!(task
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("injected completion failure"));
}

#[tokio::test]
async fn queued_step_resolves_current_project_authority_at_its_own_entry() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    let definition = serde_json::to_string(&workflow()).unwrap();
    payload.workflow_ref = WorkflowReference::Project;
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    sqlx::query("UPDATE project SET workflow_definition=?,version=version+1 WHERE id='p'")
        .bind(definition)
        .execute(db.pool())
        .await
        .unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "done");
    assert!(task.error_annotation.is_none());
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "done");
}

#[tokio::test]
async fn supervised_worker_consumes_commit_kick_and_stops_on_shutdown() {
    let (db, worker) = fixture().await;
    let (shutdown, receiver) = watch::channel(false);
    let handle = Arc::new(worker).start(receiver);
    tokio::task::yield_now().await;
    db.enqueue_step(&step("done")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if db.task_steps("t").await.unwrap()[0].status == "done" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background worker consumes committed cascade");
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("supervisor stops")
        .unwrap();
}

#[tokio::test]
async fn durable_hook_failure_keeps_the_committed_status_step() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    let mut definition = workflow();
    definition.states[1]
        .hooks
        .on_enter
        .push(api_types::HookSpec {
            action: "undefined_action".into(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Log,
        });
    payload.workflow_ref = WorkflowReference::Snapshot(
        db.store_step_workflow(&serde_json::to_string(&definition).unwrap())
            .await
            .unwrap(),
    );
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert_eq!(task.status, "done");
    assert!(task
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("Transition committed; hook failed"));
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "done");
    let failed = rows.last().unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.attempts, 1);
    assert!(failed
        .last_error
        .as_deref()
        .unwrap()
        .contains("undefined_action"));
    let logs = db::TransitionLogRepo::list_by_task(&*db, "t")
        .await
        .unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].id, input.id);
}

async fn claim_committed_hooks(db: &Arc<db::SqliteDb>, worker: &TaskStepWorker) -> TaskStep {
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    let mut definition = workflow();
    definition.states[1]
        .hooks
        .on_enter
        .push(api_types::HookSpec {
            action: "undefined_action".into(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Log,
        });
    payload.workflow_ref = WorkflowReference::Snapshot(
        db.store_step_workflow(&serde_json::to_string(&definition).unwrap())
            .await
            .unwrap(),
    );
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    let status = db
        .claim_step("status-owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    worker.execute_inner(&status).await.unwrap();
    db.release_step(&status.id, "status-owner").await.unwrap();
    db.claim_step("hook-owner", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn interrupted_committed_hook_does_not_annotate_later_task_entry() {
    let (db, worker) = fixture().await;
    let claimed = claim_committed_hooks(&db, &worker).await;
    let now = db::now_rfc3339();
    sqlx::query("UPDATE task SET status='todo',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,rejection,created_at) VALUES (?,'t','done','todo','user:api','restart',0,?)")
        .bind(db::new_uuid_v4()).bind(now).execute(db.pool()).await.unwrap();
    worker
        .fail_committed_hook_phase(&claimed, "predecessor was interrupted")
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "todo");
    assert!(task.error_annotation.is_none());
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].status, "done");
    let predecessor = rows.last().unwrap();
    assert_eq!(predecessor.status, "superseded");
    assert!(predecessor
        .last_error
        .as_deref()
        .unwrap()
        .contains("predecessor was interrupted"));
}

/// R5: a hook error after a sleep that expired the wall-clock lease is still
/// recorded while the local owner is alive.
#[tokio::test]
async fn committed_hook_failure_after_sleep_is_recorded_for_live_owner() {
    let (db, worker) = fixture().await;
    let claimed = claim_committed_hooks(&db, &worker).await;
    let _activity = db.hold_task_step(&claimed);
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?")
        .bind(&claimed.id)
        .execute(db.pool())
        .await
        .unwrap();
    worker
        .fail_committed_hook_phase(&claimed, "hook failed after wake")
        .await
        .unwrap();
    let rows = db.task_steps("t").await.unwrap();
    let row = rows.last().unwrap();
    assert_eq!(row.status, "failed");
    let task = TaskRepo::get_by_id(&*db, "t", false)
        .await
        .unwrap()
        .unwrap();
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "cascade_failed");
    assert_eq!(annotation["task_step_id"], claimed.id.as_str());
}

#[tokio::test]
async fn delayed_head_and_metadata_edit_preserve_the_cascade() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.available_at = lease_deadline();
    db.enqueue_step(&input).await.unwrap();
    sqlx::query("UPDATE task SET title='edited',version=version+1 WHERE id='t'")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(db
        .claim_step("early", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .is_none());
    sqlx::query("UPDATE task_step SET available_at='2000-01-01T00:00:00Z'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "done");
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "done");
}

#[tokio::test]
async fn background_worker_retries_version_conflict_with_backoff() {
    let (db, worker) = fixture().await;
    sqlx::query("CREATE TRIGGER temporarily_refuse BEFORE UPDATE OF status ON task WHEN NEW.status='done' BEGIN SELECT RAISE(IGNORE); END").execute(db.pool()).await.unwrap();
    let (stop, rx) = watch::channel(false);
    let handle = Arc::new(worker).start(rx);
    db.enqueue_step(&step("done")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = &db.task_steps("t").await.unwrap()[0];
            if row.status == "pending" && row.attempts == 1 {
                assert!(row.last_error.is_some());
                assert!(row.available_at > db::now_rfc3339());
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("DROP TRIGGER temporarily_refuse")
        .execute(db.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(db.task_steps("t").await.unwrap()[0].attempts, 1);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if db.task_steps("t").await.unwrap()[0].status == "done" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(db.task_steps("t").await.unwrap()[0].attempts, 2);
    stop.send(true).unwrap();
    handle.await.unwrap();
}

#[tokio::test]
async fn changed_gate_approval_supersedes_with_durable_diagnostic() {
    let (db, worker) = fixture().await;
    let mut wf = workflow();
    wf.states[0].kind = StateKind::Gate;
    let mut gate = crate::workflow::default_workflow::default_workflow()
        .states
        .into_iter()
        .find(|s| s.name == "review")
        .unwrap()
        .gate_config
        .unwrap();
    gate.requires_user_approval = Some(true);
    wf.states[0].gate_config = Some(gate);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&wf).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    payload.workflow_ref = WorkflowReference::Project;
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "todo");
    assert_eq!(db.task_steps("t").await.unwrap()[0].status, "superseded");
    let events:Vec<(String,String)>=sqlx::query_as("SELECT event_type,payload_json FROM domain_event WHERE event_type='transition.step_superseded'").fetch_all(db.pool()).await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].1.contains("requires approval"));
}

#[tokio::test]
async fn queued_hop_propagates_producer_review_clear_flag() {
    let (db, worker) = fixture().await;
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
        .bind(serde_json::to_string(&workflow()).unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET review_passed_at=? WHERE id='t'")
        .bind(db::now_rfc3339())
        .execute(db.pool())
        .await
        .unwrap();
    let mut input = step("done");
    let mut payload: CascadePayload = serde_json::from_str(&input.payload_json).unwrap();
    payload.workflow_ref = WorkflowReference::Project;
    payload.clear_review_passed_at_on_commit = true;
    input.payload_json = serde_json::to_string(&payload).unwrap();
    db.enqueue_step(&input).await.unwrap();
    let result = worker.drain("t").await.unwrap();
    assert_eq!(result.status, "done");
    assert!(result.review_passed_at.is_none());
}

#[tokio::test]
async fn loop_event_is_broadcast_once_with_durable_audit_envelope() {
    let (db, worker) = fixture().await;
    let mut events = worker.engine.event_bus.subscribe();
    let relay = crate::domain_event_broadcast::DomainEventBroadcastConsumer::new(
        db.clone(),
        worker.engine.event_bus.clone(),
        Some(db.domain_event_head().await.unwrap()),
    );
    let mut input = step("done");
    input.chain_position = CHAIN_LIMIT + 1;
    db.enqueue_step(&input).await.unwrap();
    let task = worker.drain("t").await.unwrap();
    assert!(task.blocked_json.is_some());
    relay.broadcast_once(100).await.unwrap();
    relay.broadcast_once(100).await.unwrap();
    let collected = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(
        collected
            .iter()
            .filter(|event| event.event_type == "transition.loop_detected")
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='transition.loop_detected'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(task.error_annotation.as_deref().unwrap())
            .unwrap()["type"],
        "workflow_loop"
    );
    let snapshot = worker
        .task_service
        .task_action_snapshot("t", &Actor::user(api_types::UserActionSource::Test))
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|offer| matches!(
        offer.action,
        api_types::TaskAction::Restart { .. } | api_types::TaskAction::Approve { .. }
    )));
}

#[tokio::test]
async fn owner_command_drives_fast_predecessors_before_returning_its_task() {
    let (db, worker) = fixture().await;
    let mutation = db::TaskMutation::TaskSetEntryBarrier {
        id: "t".into(),
        expected_version: 1,
        entry_barrier_json: None,
        updated_at: db::now_rfc3339(),
    };
    db.enqueue_task_mutation("t", mutation).await.unwrap();
    let command = crate::task_service::commands::TaskCommand {
        operation: "engine_transition".into(),
        preempt: false,
        arguments: serde_json::json!({"task_id":"t","target_state":"done","version":1,"workflow":workflow(),"actor":cascade_actor(),"reason":"owner queued behind fast work","rejection":false,"skip_before_exit":false,"defer_dispatch_until":null,"board_move":null,"authority":null,"entry_retry":false}),
    };
    let result: crate::workflow::engine::TransitionResult = Arc::new(worker)
        .request_command("t", command)
        .await
        .unwrap();
    assert_eq!(result.task.status, "done");
    let rows = db.task_steps("t").await.unwrap();
    assert_eq!(rows[0].kind, "mutation");
    assert_eq!(rows[0].status, "done");
    assert_eq!(rows[1].kind, "command");
    assert_eq!(rows[1].status, "done");
}

async fn remote_hook_fixture() -> (
    Arc<db::SqliteDb>,
    TaskStepWorker,
    db::Task,
    db::WorkspacePlacement,
    db::TaskStep,
) {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    let (task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let mut input = step("done");
    input.task_id = task.id.clone();
    input.expected_status = task.status.clone();
    input.expected_version = task.version;
    input.kind = "hooks".into();
    input.lane = "long".into();
    db.enqueue_step(&input).await.unwrap();
    let claimed = db
        .claim_step("remote-ci", Some(&task.id), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    db.register_remote_task_operation(&claimed, &placement, "remote-ci-operation")
        .await
        .unwrap();
    let service = crate::TaskService::new(db.clone(), Arc::new(events::EventBus::new(32)));
    let worker = TaskStepWorker::new(service.clone());
    (db, worker, task, placement, claimed)
}

async fn cancel_remote_task(worker: &TaskStepWorker, task: &db::Task) -> db::Task {
    let mut definition = workflow();
    definition.states[0].name = task.status.clone();
    definition.states[0].kind = StateKind::Active;
    definition.states[1].name = "cancelled".into();
    definition.cancellation_state = Some("cancelled".into());
    let current = TaskRepo::get_by_id(&*worker.db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    worker
        .engine
        .bind(&worker.task_service)
        .transition(
            &task.id,
            "cancelled",
            current.version,
            &definition,
            &Actor::user(api_types::UserActionSource::Test),
            "Cancel remote CI",
            false,
            Default::default(),
        )
        .await
        .unwrap()
        .task
}

#[tokio::test]
async fn connected_remote_cancel_ack_supersedes_hook_then_cancels_task() {
    let (db, mut worker, task, placement, claimed) = remote_hook_fixture().await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    worker.task_service.daemon_connections = Some(registry.clone());
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
        else {
            panic!("cancel request")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        assert_eq!(params["operation_id"], "remote-ci-operation");
        registry.dispatch_incoming_for_connection(
            &daemon_id,
            connection,
            api_types::DaemonFrame::Response {
                id,
                result: serde_json::json!({"operation_id":params["operation_id"],"state":"killed"}),
            },
        );
    });
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    responder.await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        db.task_steps(&task.id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .status,
        "superseded"
    );
    assert!(matches!(
        db.finish_remote_task_operation(&claimed, "remote-ci-operation")
            .await,
        Err(db::DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn disconnected_remote_cancel_fences_workspace_until_reconnect_ack() {
    let (db, worker, task, placement, claimed) = remote_hook_fixture().await;
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    let rows = db.task_steps(&task.id).await.unwrap();
    assert_eq!(
        rows.iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .last_error
            .as_deref(),
        Some("remote_operation_unconfirmed")
    );
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let backend =
        crate::workspace_backend::DaemonWorkspaceBackend::new(db.clone(), registry.clone());
    use crate::workspace_backend::WorkspaceBackend;
    let spec = crate::workspace_backend::RunSpec {
        purpose: crate::workspace_backend::WorkspaceRunPurpose::CiStep,
        command: "restart CI".into(),
        env: Default::default(),
        timeout_secs: 0,
        max_output_bytes: 4096,
    };
    assert!(backend
        .run(&placement, &spec)
        .await
        .unwrap_err()
        .to_string()
        .contains("pending_remote_cancel"));
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let responses = registry.clone();
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
        else {
            panic!("reconnect cancel")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        responses.dispatch_incoming_for_connection(&daemon_id,connection,api_types::DaemonFrame::Response{id,result:serde_json::json!({"operation_id":params["operation_id"],"state":"unknown"})});
    });
    crate::remote_cancel::reconcile(&db, registry, &placement.daemon_id.clone().unwrap())
        .await
        .unwrap();
    responder.await.unwrap();
    assert!(!db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    assert!(db
        .task_steps(&task.id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "mutation" && s.status == "pending"));
    assert!(matches!(
        db.finish_remote_task_operation(&claimed, "remote-ci-operation")
            .await,
        Err(db::DbError::VersionConflict)
    ));
}

/// The `done` cascade of a landed merge is put ahead of a waiting owner
/// command (`priority` 2). A Cancel that asks for preemption while that
/// cascade runs does not stop it: the Task whose change is on the target is
/// reported done, not cancelled. A step the command outranks is still told.
#[tokio::test]
async fn a_preempting_cancel_does_not_stop_the_cascade_of_a_landed_merge() {
    let (db, _worker) = fixture().await;
    for (key, priority) in [("landed", 2), ("ordinary", 0)] {
        sqlx::query("DELETE FROM task_step WHERE task_id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        let mut cascade = step("done");
        cascade.kind = "cascade".into();
        cascade.causation_key = key.into();
        let id = db.enqueue_step(&cascade).await.unwrap();
        sqlx::query("UPDATE task_step SET priority=? WHERE id=?")
            .bind(priority)
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let claimed = db
            .claim_step("worker", Some("t"), &lease_deadline())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, id);
        let control = db.register_step_control(&claimed);
        db.request_task_preemption("t").await.unwrap();
        assert_eq!(
            *control.preempt.borrow(),
            priority == 0,
            "{key}: preemption of a claimed step of priority {priority}"
        );
        db.release_step_control(&claimed);
    }
}

/// An earlier pending step, then a protected integration step, then a
/// priority-2 step: the first is claimed. (Without the rule the first waits
/// for the higher priority, which waits for the protected step, which waits
/// for the first.)
#[tokio::test]
async fn a_step_behind_a_protected_step_does_not_wedge_the_steps_before_it() {
    let (db, _worker) = fixture().await;
    let mut ids = Vec::new();
    for (key, priority, protected) in [
        ("first", 0, false),
        ("result", 0, true),
        ("landed", 2, false),
    ] {
        let mut input = step("done");
        input.causation_key = key.into();
        let id = db.enqueue_step(&input).await.unwrap();
        sqlx::query("UPDATE task_step SET priority=?,integration_started_at=? WHERE id=?")
            .bind(priority)
            .bind(protected.then(db::now_rfc3339))
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        ids.push(id);
    }
    for expected in &ids {
        let claimed = db
            .claim_step("worker", Some("t"), &lease_deadline())
            .await
            .unwrap()
            .expect("a step of the Task is claimable");
        assert_eq!(&claimed.id, expected);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &claimed, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.release_step(&claimed.id, "worker").await.unwrap();
    }
}

#[tokio::test]
async fn protected_merge_finishes_before_preempting_cancel_can_claim() {
    let (db, worker) = fixture().await;
    let mut input = step("done");
    input.kind = "hooks".into();
    db.enqueue_step(&input).await.unwrap();
    let merge = db
        .claim_step("push", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    db::task_writer::in_task_step(merge.clone(), db.protect_step_integration())
        .await
        .unwrap();
    let mut cancel = step("done");
    cancel.kind = "command".into();
    cancel.causation_key = "cancel".into();
    cancel.payload_json =
        serde_json::json!({"operation":"perform_task_action_as","arguments":[],"preempt":true})
            .to_string();
    db.enqueue_step(&cancel).await.unwrap();
    db.request_task_preemption("t").await.unwrap();
    assert!(db
        .claim_step("cancel", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .is_none());
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id='t'")
        .execute(&mut *tx)
        .await
        .unwrap();
    db.finish_step_in_tx(&mut tx, &merge, "done", None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.release_step(&merge.id, "push").await.unwrap();
    let cancel = db
        .claim_step("cancel", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancel.kind, "command");
    assert_eq!(
        TaskRepo::get_by_id(&*worker.db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "done"
    );
}

#[tokio::test]
async fn older_daemon_rejection_still_cancels_task_and_fences_workspace() {
    let (db, mut worker, task, placement, claimed) = remote_hook_fixture().await;
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    worker.task_service.daemon_connections = Some(registry.clone());
    let responder = tokio::spawn(async move {
        let api_types::DaemonFrame::Request { id, method, .. } = outbound.recv().await.unwrap()
        else {
            panic!("cancel request")
        };
        assert_eq!(method, api_types::METHOD_WORKSPACE_CANCEL);
        registry.dispatch_incoming_for_connection(
            &daemon_id,
            connection,
            api_types::DaemonFrame::Error {
                id: Some(id),
                error: api_types::DaemonErrorPayload {
                    code: api_types::UNSUPPORTED_METHOD.into(),
                    message: "older daemon".into(),
                    details: None,
                },
            },
        );
    });
    worker.preempt_step(&claimed).await.unwrap();
    db.release_step(&claimed.id, "remote-ci").await.unwrap();
    responder.await.unwrap();
    assert_eq!(cancel_remote_task(&worker, &task).await.status, "cancelled");
    assert!(db.task_has_pending_remote_cancel(&task.id).await.unwrap());
    assert_eq!(
        db.task_steps(&task.id)
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.id == claimed.id)
            .unwrap()
            .last_error
            .as_deref(),
        Some("remote_operation_unconfirmed")
    );
}

#[tokio::test]
async fn owner_command_busy_retains_its_write_until_predecessor_releases() {
    let (db, worker) = fixture().await;
    let predecessor = db
        .enqueue_task_mutation(
            "t",
            db::TaskMutation::TaskSetEntryBarrier {
                id: "t".into(),
                expected_version: 1,
                entry_barrier_json: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    let claimed = db
        .claim_step("other-worker", Some("t"), &lease_deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, predecessor);
    let worker = Arc::new(worker);
    let command = crate::task_service::commands::TaskCommand {
        operation: "engine_transition".into(),
        preempt: false,
        arguments: serde_json::json!({"task_id":"t","target_state":"done","version":1,"workflow":workflow(),"actor":cascade_actor(),"reason":"busy owner write","rejection":false,"skip_before_exit":false,"defer_dispatch_until":null,"board_move":null,"authority":null,"entry_retry":false}),
    };
    let result = worker
        .request_command::<crate::workflow::engine::TransitionResult>("t", command)
        .await;
    assert!(matches!(
        result,
        Err(ServiceError::TaskBusy {
            pending_steps: 2,
            ..
        })
    ));
    assert_eq!(
        TaskRepo::get_by_id(&*db, "t", false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "todo"
    );
    db::task_writer::in_task_step(claimed.clone(), db.execute_task_mutation(&claimed))
        .await
        .unwrap();
    db.release_step(&claimed.id, "other-worker").await.unwrap();
    assert_eq!(worker.drain("t").await.unwrap().status, "done");
    assert!(db
        .task_steps("t")
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
}

/// Only an I/O failure that can clear by itself is retried by the step
/// bound. A missing checkout or a refused permission is deterministic: it
/// parks at once instead of spending eight attempts first.
#[test]
fn open_items_fix_only_transient_io_errors_are_retryable() {
    use std::io::{Error, ErrorKind};
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::InvalidInput,
        ErrorKind::InvalidData,
        ErrorKind::Other,
    ] {
        assert!(
            !retryable(&ServiceError::Git(git::GitError::Io(Error::from(kind)))),
            "git {kind:?}"
        );
        assert!(
            !retryable(&ServiceError::Review(::review::ReviewError::Io(
                Error::from(kind)
            ))),
            "review {kind:?}"
        );
    }
    for kind in [
        ErrorKind::Interrupted,
        ErrorKind::WouldBlock,
        ErrorKind::TimedOut,
        ErrorKind::ResourceBusy,
    ] {
        assert!(
            retryable(&ServiceError::Git(git::GitError::Io(Error::from(kind)))),
            "git {kind:?}"
        );
        assert!(
            retryable(&ServiceError::Review(::review::ReviewError::Io(
                Error::from(kind)
            ))),
            "review {kind:?}"
        );
    }
}
