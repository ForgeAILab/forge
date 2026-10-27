//! Leased per-Task FIFO cascade execution. Event cursors and dead letters are
//! deliberately separate from this queue's per-Task ordering protocol.
use super::{
    consumer_error_kind, SupervisorPolicy, WorkerErrorKind, WorkerHealth, WorkerSupervisor,
};
use crate::{
    workflow::engine::{WorkflowAuthority, WorkflowEngine},
    Result, ServiceError,
};
use api_types::{Actor, SystemComponent};
use db::task_condition::channels;
use db::{TaskRepo, TaskStep, TaskStepRepo, WorkflowReference};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};

pub(crate) mod command_reply;

pub const CHAIN_LIMIT: i64 = 64;

/// One-shot per-Task command faults for deterministic transport regressions.
#[cfg(any(test, feature = "test-support"))]
pub mod test_hooks {
    use crate::ServiceError;
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };

    type CommandFaults = Mutex<HashMap<(usize, String), (ServiceError, bool)>>;
    fn faults() -> &'static CommandFaults {
        static FAULTS: OnceLock<CommandFaults> = OnceLock::new();
        FAULTS.get_or_init(Default::default)
    }
    pub fn inject_command_error(
        service: &crate::TaskService,
        task_id: &str,
        error: ServiceError,
        durable_reply: bool,
    ) {
        faults().lock().unwrap().insert(
            (
                std::sync::Arc::as_ptr(&service.task_step_replies) as usize,
                task_id.to_owned(),
            ),
            (error, durable_reply),
        );
    }
    pub(super) fn take(
        service: &crate::TaskService,
        task_id: &str,
    ) -> Option<(ServiceError, bool)> {
        faults().lock().unwrap().remove(&(
            std::sync::Arc::as_ptr(&service.task_step_replies) as usize,
            task_id.to_owned(),
        ))
    }
}
// Long steps (merge, CI review checks) can wait on one repository's lock for
// the whole hook. They get their own four slots so the eight fast slots keep
// every other Project's cascades moving. Per-repository head-of-line inside
// the long lane is a 3.2 follow-up.
const FAST_CONCURRENCY: usize = 8;
const LONG_CONCURRENCY: usize = 4;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(8);
const LEASE_SECONDS: i64 = 60;
const LAPSED_CLAIM_POLL: Duration = Duration::from_millis(250);

/// The waiter owns only its reply registration, not the durable step or
/// its worker. Cancellation and every error path remove the registration.
struct CommandReplyRegistration<'a> {
    service: &'a crate::TaskService,
    step_id: &'a str,
}
impl CommandReplyRegistration<'_> {
    fn register(&self) -> tokio::sync::oneshot::Receiver<Result<serde_json::Value>> {
        let (reply, received) = tokio::sync::oneshot::channel();
        self.service
            .task_step_replies
            .lock()
            .expect("Task step replies")
            .insert(self.step_id.to_owned(), reply);
        received
    }
}
impl Drop for CommandReplyRegistration<'_> {
    fn drop(&mut self) {
        self.service
            .task_step_replies
            .lock()
            .expect("Task step replies")
            .remove(self.step_id);
    }
}

fn decode_command_reply<T: serde::de::DeserializeOwned>(
    reply: Result<serde_json::Value>,
) -> Result<T> {
    reply.and_then(|value| {
        serde_json::from_value(value).map_err(|e| ServiceError::invalid_operation(e.to_string()))
    })
}

#[cfg(test)]
fn preemption_diagnostics() -> &'static std::sync::Mutex<std::collections::HashMap<String, String>>
{
    static STATES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, String>>,
    > = std::sync::OnceLock::new();
    STATES.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn preemption_diagnostic(step_id: &str) -> Option<String> {
    preemption_diagnostics()
        .lock()
        .unwrap()
        .get(step_id)
        .cloned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CascadePayload {
    #[serde(flatten)]
    pub bridge: api_types::TransitionBridge,
    pub to: String,
    pub reason: String,
    pub rejection: bool,
    pub skip_before_exit: bool,
    pub workflow_ref: WorkflowReference,
    pub clear_review_passed_at_on_commit: bool,
    #[serde(default)]
    pub admission_agent_id: Option<String>,
    /// A new completed CI/review result is an execution-result chain boundary.
    pub evidence: Option<String>,
}

/// A step is long only when its target state runs a merge or CI review
/// checks. Dispatch hooks only launch an execution and stay fast.
pub(crate) fn cascade_lane(workflow: &api_types::WorkflowDefinition, to: &str) -> &'static str {
    let long = workflow
        .states
        .iter()
        .find(|state| state.name == to)
        .is_some_and(|state| {
            state
                .hooks
                .before_enter
                .iter()
                .chain(&state.hooks.on_enter)
                .chain(&state.hooks.after_enter)
                .any(|hook| matches!(hook.action.as_str(), "run_merge" | "run_ci_steps"))
        });
    if long {
        "long"
    } else {
        "fast"
    }
}

pub struct TaskStepWorker {
    engine: Arc<WorkflowEngine>,
    task_service: crate::TaskService,
    db: Arc<db::SqliteDb>,
    pub(crate) renew_interval: Duration,
    #[cfg(test)]
    command_test_gate: Option<Arc<tests::CommandGate>>,
}

#[async_trait::async_trait]
impl db::task_writer::TaskStepExecutor for TaskStepWorker {
    async fn drive_inline(&self, task_id: &str) -> db::Result<()> {
        Arc::new(Self::new(self.task_service.clone()))
            .start_inline_head(task_id)
            .await
            .map_err(|error| match error {
                ServiceError::Db(error) => error,
                error => db::DbError::Check(error.to_string()),
            })
    }
}
/// How long `drain` waits for the head drivers it stopped.
#[cfg(any(test, feature = "test-support"))]
const DRAIN_DRIVER_JOIN: Duration = Duration::from_secs(30);

impl TaskStepWorker {
    pub fn new(task_service: crate::TaskService) -> Self {
        let engine = task_service.workflow_engine();
        Self {
            task_service,
            db: Arc::clone(&engine.db),
            engine,
            renew_interval: Duration::from_secs(15),
            #[cfg(test)]
            command_test_gate: None,
        }
    }
    pub(crate) async fn request_command<T: serde::de::DeserializeOwned>(
        self: &Arc<Self>,
        task_id: &str,
        command: crate::task_service::commands::TaskCommand,
    ) -> Result<T> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let requested_version = match command.operation.as_str() {
            "cancel_task_with_options" => command.arguments[1].as_i64(),
            "perform_task_action_as" => command.arguments[2].as_i64(),
            "transition" | "transition_with_plan_publication" => {
                command.arguments[2]["version"].as_i64()
            }
            "update_task" => command.arguments[1]["version"].as_i64(),
            "engine_transition" => command.arguments["version"].as_i64(),
            _ => None,
        };
        let landed = if command.preempt {
            let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or(db::DbError::NotFound)?;
            let workflow = WorkflowEngine::resolve_workflow_for_task(
                &task,
                &project.workflow_definition,
                &Actor::system(SystemComponent::Workflow),
            );
            workflow.state_kind(&task.status) == Some(api_types::StateKind::Terminal)
                && workflow.cancellation_state.as_deref() != Some(task.status.as_str())
                && self.db.task_has_completed_integration_step(task_id).await?
        } else {
            false
        };
        if !landed && requested_version.is_some_and(|version| version != task.version) {
            if command.operation == "engine_transition" {
                return Err(db::DbError::VersionConflict.into());
            }
            return Err(db::DbError::TaskVersionConflict {
                expected: requested_version.unwrap(),
                actual: task.version,
            }
            .into());
        }
        let startup_checks = if matches!(
            command.operation.as_str(),
            "start_execution"
                | "claim_and_start_task"
                | "launch_execution"
                | "follow_up_execution"
                | "follow_up_interactive_execution"
                | "dispatch_queued_recovery"
                | "reconcile_role"
                | "dispatch_recovery_role"
                | "dispatch_initial_role_execution_with_metadata_and_admission"
                | "dispatch_initial_role_execution_with_optional_admission"
                | "dispatch_role_follow_up"
                | "dispatch_role_follow_up_with_agent"
                | "dispatch_role_follow_up_with_admission"
                | "re_execute_execution_with_context"
        ) {
            let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or(db::DbError::NotFound)?;
            let settings: api_types::ProjectSettings = serde_json::from_str(&project.settings)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            !settings.environment.checks.is_empty()
        } else {
            false
        };
        let mut payload: serde_json::Value = serde_json::from_str(&command.payload_json()?)
            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        if command.operation == "dispatch_queued_recovery" {
            if let Some(queued) = crate::deferred_dispatch::queued_recovery(&task) {
                if queued.request.role_name.is_some() {
                    payload["admission_agent_id"] = serde_json::json!(queued.request.agent_id);
                }
            }
        }
        if command.preempt {
            let hooks: bool = self.db.task_has_preemptible_work(task_id).await?;
            payload["preempting_hooks"] = serde_json::json!(hooks);
        }
        let id = db::new_uuid_v4();
        let registration = CommandReplyRegistration {
            service: &self.task_service,
            step_id: &id,
        };
        let mut received = registration.register();
        let fence = crate::task_service::commands::TaskCommand::fence(&command.operation);
        let enqueue = async {
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            let queued = self
                .db
                .enqueue_step_in_tx(
                    &mut tx,
                    &db::EnqueueTaskStep {
                        id: id.clone(),
                        task_id: task_id.to_owned(),
                        kind: "command".to_owned(),
                        payload_json: payload.to_string(),
                        causation_step_id: db::task_writer::current_task_step().map(|step| step.id),
                        causation_key: id.clone(),
                        chain_id: id.clone(),
                        chain_position: 1,
                        expected_status: task.status.clone(),
                        expected_version: task.version,
                        expected_epoch: None,
                        lane: if startup_checks { "long" } else { "fast" }.to_owned(),
                        available_at: db::now_rfc3339(),
                    },
                )
                .await?;
            // A command left queued by a busy wait must not be lost to a
            // later preempting Cancel/Hold when it is identity-fenced.
            if fence == db::task_writer::EffectFence::Identity {
                self.db
                    .mark_step_identity_fenced_in_tx(&mut tx, &queued)
                    .await?;
            }
            tx.commit().await?;
            self.db.domain_event_notify().notify_waiters();
            Ok::<_, db::DbError>(queued)
        }
        .await;
        if let Err(error) = enqueue {
            return Err(error.into());
        }
        if command.preempt {
            self.db.request_task_preemption(task_id).await?;
        }
        let notify = self.db.domain_event_notify();
        let deadline = tokio::time::Instant::now()
            + if command.preempt {
                Duration::from_secs(15)
            } else {
                Duration::from_secs(5)
            };
        let mut claimed_until = None;
        loop {
            let changed = notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Prefer the channel's typed result, including a retryable
            // attempt error. A pending retry is not a terminal failure.
            let closed = match received.try_recv() {
                Ok(reply) => return decode_command_reply(reply),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => false,
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => true,
            };
            let steps = self.db.task_steps(task_id).await?;
            let own = steps
                .iter()
                .find(|step| step.id == id)
                .ok_or(db::DbError::NotFound)?;
            let lease = own
                .lease_until
                .as_deref()
                .and_then(db::task_writer::lease_remaining);
            if !matches!(
                own.status.as_str(),
                "pending" | "claimed" | "done" | "failed"
            ) {
                return Err(db::DbError::VersionConflict.into());
            }
            if matches!(own.status.as_str(), "done" | "failed") && lease.is_none() {
                // A live claimer releases the step and then sends its typed
                // reply. A waiter that reads the row between the two waits
                // for that reply, briefly. The durable answer carries the
                // same error kind and fields if the channel was lost. A
                // spent (closed) receiver has no reply left; never poll it.
                if own.claimed_by.is_none() && !closed {
                    if let Ok(Ok(reply)) =
                        tokio::time::timeout(LAPSED_CLAIM_POLL, &mut received).await
                    {
                        return decode_command_reply(reply);
                    }
                } else if let Ok(reply) = received.try_recv() {
                    return decode_command_reply(reply);
                }
                // A claimer may have committed and stopped before release
                // or reply. Its durable answer is safe once the lease lapses;
                // executing the command again would repeat its effect.
                if let Some(reply) = &own.result_json {
                    let reply: command_reply::CommandReply = serde_json::from_str(reply)
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    return reply.decode();
                }
                return Err(ServiceError::invalid_operation(
                    own.last_error
                        .as_deref()
                        .unwrap_or("Task step settled without a reply"),
                ));
            }
            if closed {
                // A sender can be lost between its removal and send while a
                // failed attempt is re-queued. Keep listening to this step;
                // a future attempt removes and sends this new registration.
                received = registration.register();
            }
            let now = tokio::time::Instant::now();
            let wake = if own.status == "pending" {
                if now >= deadline {
                    return Err(db::DbError::TaskBusy {
                        pending_steps: self.db.pending_steps(task_id).await?,
                        retry_after_ms: 250,
                    }
                    .into());
                }
                // A dropped waiter's predecessor may have no caller left
                // to wake on its lease. The next command drives it too.
                let predecessor_lease = steps
                    .iter()
                    .filter(|step| step.id != id)
                    .filter_map(|step| {
                        step.lease_until
                            .as_deref()
                            .and_then(db::task_writer::lease_remaining)
                    })
                    .min();
                // Its own retry waits out a back-off: drive it when due.
                let backoff = chrono::DateTime::parse_from_rfc3339(&own.available_at)
                    .ok()
                    .and_then(|due| {
                        (due.with_timezone(&chrono::Utc) - chrono::Utc::now())
                            .to_std()
                            .ok()
                    })
                    .map(|left| left + Duration::from_millis(5));
                deadline
                    .min(now + predecessor_lease.unwrap_or(deadline - now))
                    .min(now + backoff.unwrap_or(deadline - now))
            } else {
                let ceiling = *claimed_until.get_or_insert(now + self.db.claimed_task_step_wait());
                if now >= ceiling {
                    return Err(db::DbError::TaskBusy {
                        pending_steps: self.db.pending_steps(task_id).await?,
                        retry_after_ms: 250,
                    }
                    .into());
                }
                ceiling.min(now + lease.unwrap_or(LAPSED_CLAIM_POLL))
            };
            if own.status == "pending" || (own.status == "claimed" && lease.is_none()) {
                // Normal claims retain the live-owner witness and all
                // owner-token fences. Never cancel a claim's SQLite write.
                self.start_inline_head(task_id).await?;
            }
            tokio::select! {
                biased;
                reply=&mut received=>if let Ok(reply) = reply { return decode_command_reply(reply); },
                _=changed=>{},
                _=tokio::time::sleep_until(wake)=>{},
            }
        }
    }

    fn start_inline_head<'a>(
        self: &'a Arc<Self>,
        task_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // The claim, its local witness and its runner belong to one
            // spawned job, never to the waiter: a request dropped while the
            // claim's commit is in flight would otherwise leave the step
            // claimed for a whole lease with nobody running it, and every
            // later command of the Task would answer `task_busy`.
            let worker = self.clone();
            let task_id = task_id.to_owned();
            let (claimed, claim) = tokio::sync::oneshot::channel();
            let _job = tokio::spawn(async move {
                let step = match worker.claim_inline_head(&task_id).await {
                    Ok(Some(step)) => step,
                    Ok(None) => {
                        let _ = claimed.send(Ok(false));
                        return;
                    }
                    Err(error) => {
                        let _ = claimed.send(Err(error));
                        return;
                    }
                };
                let _activity = worker.db.hold_task_step(&step);
                let _ = claimed.send(Ok(true));
                #[cfg(test)]
                if let Some(gate) = &worker.command_test_gate {
                    gate.pause(&worker.db, &step, tests::CommandPoint::AfterClaim)
                        .await;
                }
                if let Err(error) = worker.execute(step).await {
                    tracing::warn!(%error,"inline Task step failed");
                }
            });
            // A job that stopped before it answered holds no claim it still
            // runs; the waiter's lease-bounded wait covers it.
            let _claimed = claim.await.unwrap_or(Ok(false))?;
            #[cfg(test)]
            if let Some(gate) = self.command_test_gate.as_ref().filter(|_| _claimed) {
                gate.keep_job(_job);
            }
            Ok(())
        })
    }
    async fn claim_inline_head(&self, task_id: &str) -> Result<Option<TaskStep>> {
        let lane = self
            .db
            .task_step_head_lane(task_id)
            .await?
            .unwrap_or_else(|| "fast".to_owned());
        Ok(self
            .db
            .claim_step_lane(
                &db::new_uuid_v4(),
                Some(task_id),
                Some(&lane),
                &lease_deadline(),
            )
            .await?)
    }
    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        WorkerSupervisor::new(
            WorkerHealth::new(Arc::clone(&self.db), "task_steps"),
            SupervisorPolicy::default(),
        )
        .with_shutdown_grace(Duration::from_secs(10))
        .start(move |shutdown| Arc::clone(&self).run(shutdown), shutdown)
    }
    async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let notify = self.db.domain_event_notify();
        let mut fast = JoinSet::new();
        let mut long = JoinSet::new();
        loop {
            if *shutdown.borrow_and_update() {
                break;
            }
            // Wall time can jump while the laptop's monotonic timer pauses.
            // Restore live owners before any claim on wake; local witnesses
            // also exclude their Tasks while a hook future remains alive.
            if let Err(error) = self.db.renew_active_steps(&lease_deadline()).await {
                tracing::warn!(%error,"active task step renewal failed; retaining executions");
            }
            for (lane, capacity, jobs) in [
                ("fast", FAST_CONCURRENCY, &mut fast),
                ("long", LONG_CONCURRENCY, &mut long),
            ] {
                while jobs.len() < capacity {
                    let owner = db::new_uuid_v4();
                    match self
                        .db
                        .claim_step_lane(&owner, None, Some(lane), &lease_deadline())
                        .await
                    {
                        Ok(Some(step)) => {
                            let activity = self.db.hold_task_step(&step);
                            let worker = Arc::clone(&self);
                            jobs.spawn(async move {
                                let _activity = activity;
                                worker.execute(step).await
                            });
                        }
                        Ok(None) => break,
                        Err(error) => {
                            tracing::warn!(%error, lane, "task step claim failed; preserving in-flight transitions");
                            break;
                        }
                    }
                }
            }
            tokio::select! {
                _ = super::supervisor::shutdown_signal(&mut shutdown) => break,
                result = fast.join_next(), if !fast.is_empty() => log_job(result),
                result = long.join_next(), if !long.is_empty() => log_job(result),
                _ = notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
        let drain = async {
            while let Some(result) = fast.join_next().await {
                log_job(Some(result));
            }
            while let Some(result) = long.join_next().await {
                log_job(Some(result));
            }
        };
        if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
            tracing::warn!("task step shutdown grace elapsed; leases will recover unfinished work");
            fast.abort_all();
            long.abort_all();
            while fast.join_next().await.is_some() {}
            while long.join_next().await.is_some() {}
        }
        Ok(())
    }
    /// Deterministic test/service utility. Does not spawn background work and
    /// waits for existing owners/backoff exactly as the production worker does.
    ///
    /// A hooks step suspended on a check is not pending, and nothing in the
    /// step queue advances it: drain runs the check worker's sweep itself
    /// until that check has an answer, then applies its delivery and the
    /// woken step like any other. A wait with no verdict to come (the
    /// consumer was cancelled, or infrastructure is exhausted and the Task
    /// is parked) ends the drain. So does a check queued behind a full
    /// machine, once the Task states that it waits for a slot: only a run
    /// ending elsewhere frees one, and the check worker admits it then.
    ///
    /// A Task in the integration queue has no pending step while the queue
    /// worker owns it: drain runs that worker's sweep itself and starts the
    /// head drivers it returns, runs the check worker for a head's check,
    /// and executes the steps of the head ahead of this Task in its queue,
    /// until the Task leaves the queue or its attempt waits on something
    /// only time or its owner ends (parked, quarantined, a suspended queue).
    ///
    /// Where the runtime's own queue worker is running, drain only waits for
    /// it. Head drivers drain started itself stop when it returns; a head
    /// they leave mid-way is taken over at its lease expiry.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn drain(&self, task_id: &str) -> Result<db::Task> {
        let stop = tokio_util::sync::CancellationToken::new();
        let mut drivers = JoinSet::new();
        let drained = self.drain_with(task_id, &mut drivers, &stop).await;
        stop.cancel();
        // A stopped driver ends at its next await; one that does not (a Git
        // child or an owner call that never returns) is aborted instead of
        // holding the drain open for ever.
        let joined = tokio::time::timeout(DRAIN_DRIVER_JOIN, async {
            while drivers.join_next().await.is_some() {}
        })
        .await;
        if joined.is_err() {
            tracing::warn!(target: "services::worker_runtime", task_id, left = drivers.len(), "integration head drivers did not stop within the drain's deadline; aborted");
            drivers.abort_all();
            while drivers.join_next().await.is_some() {}
        }
        drained
    }
    #[cfg(any(test, feature = "test-support"))]
    async fn drain_with(
        &self,
        task_id: &str,
        drivers: &mut JoinSet<()>,
        stop: &tokio_util::sync::CancellationToken,
    ) -> Result<db::Task> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        let checks = self.task_service.check_worker_or_embedded();
        let integration = self.task_service.integration_worker_or_embedded();
        loop {
            while drivers.try_join_next().is_some() {}
            // Whatever the step queue shows, a queue with work gets a head.
            // A claim also re-opens a suspended queue whose target became
            // ready, so a member of one is read again after the sweep.
            let (mut queued_head, suspended) = self.integration_head_task(task_id).await?;
            let pump = !integration.is_running();
            // A parked attempt whose wait is over (its retry time came, or
            // an owner action or a resume made it due) is taken back into
            // line by the worker's sweep, whether or not the Task is still
            // in the entry it was admitted in: one that left is cancelled
            // and its statement cleared.
            let due: bool = self.db.task_has_due_parked_integration(task_id).await?;
            if pump && (queued_head.is_some() || suspended || due) {
                for driver in integration.sweep_once().await? {
                    drivers.spawn(driver.run(stop.child_token()));
                }
                if suspended || due {
                    queued_head = self.integration_head_task(task_id).await?.0;
                }
            }
            if self.db.pending_steps(task_id).await? == 0
                && !self.db.task_step_is_running(task_id)
                && !self.db.task_steps(task_id).await?.iter().any(|step| {
                    step.lease_until
                        .as_deref()
                        .is_some_and(|until| until > db::now_rfc3339().as_str())
                })
            {
                if self.db.awaited_check_is_open(task_id).await? {
                    // A busy database is not a failed drain here either: the
                    // sweep wrote nothing, and a run whose job gave up keeps
                    // its lease and is taken up again when that lapses, as
                    // the production check worker does it. Both are waited
                    // out within the drain's own deadline.
                    let swept = async {
                        let mut jobs = JoinSet::new();
                        checks.sweep(&mut jobs).await?;
                        let mut first = Ok(());
                        // Every job is joined: one that failed must not
                        // abort the others in the middle of their effects.
                        while let Some(job) = jobs.join_next().await {
                            let job = job
                                .map_err(|_| {
                                    ServiceError::invalid_operation(
                                        "check job stopped during drain",
                                    )
                                })
                                .and_then(|result| result);
                            if first.is_ok() {
                                first = job;
                            }
                        }
                        first
                    }
                    .await;
                    match swept {
                        Ok(()) => {}
                        Err(error)
                            if database_busy(&error) && tokio::time::Instant::now() < deadline =>
                        {
                            tracing::warn!(task_id, %error, "check sweep in a task step drain found the database busy; trying again");
                            tokio::time::sleep(BUSY_BACKOFF_START).await;
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                    // The sweep just tried to admit the run. One still queued
                    // behind a full machine is answered only when a run ends
                    // elsewhere: once the Task's step has stated that wait,
                    // nothing here can advance it.
                    if self.db.pending_steps(task_id).await? == 0
                        && self.db.awaited_check_waits_for_slot(task_id).await?
                    {
                        return TaskRepo::get_by_id(&*self.db, task_id, false)
                            .await?
                            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()));
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ServiceError::invalid_operation(format!(
                            "task step drain timed out waiting for a check: {}",
                            self.db.awaited_check_summary(task_id).await?
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                if let Some(head_task) = queued_head {
                    // The head's check, if it asked for one.
                    let mut jobs = JoinSet::new();
                    checks.sweep(&mut jobs).await?;
                    while let Some(job) = jobs.join_next().await {
                        job.map_err(|_| {
                            ServiceError::invalid_operation("check job stopped during drain")
                        })??;
                    }
                    // The sweep just tried to admit the head's check. One
                    // still queued behind a full machine is answered only
                    // when a run ends elsewhere: nothing here can advance it.
                    let waits_for_slot: bool = self
                        .db
                        .integration_head_check_waits_for_slot(&head_task)
                        .await?;
                    if waits_for_slot && self.db.pending_steps(task_id).await? == 0 {
                        return TaskRepo::get_by_id(&*self.db, task_id, false)
                            .await?
                            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()));
                    }
                    // Another Task holds the slot: its steps are what this
                    // Task waits for.
                    if pump && head_task != task_id {
                        let owner = db::new_uuid_v4();
                        if let Some(step) = self
                            .db
                            .claim_step(&owner, Some(&head_task), &lease_deadline())
                            .await?
                        {
                            let _activity = self.db.hold_task_step(&step);
                            self.execute(step).await?;
                            continue;
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(ServiceError::invalid_operation(
                            "task step drain timed out waiting for the integration queue",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                return TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ServiceError::invalid_operation("task step drain timed out"));
            }
            let owner = db::new_uuid_v4();
            // A busy database is not a failed drain. The claim wrote nothing;
            // a step whose bookkeeping gave up keeps its lease and is claimed
            // again when that lapses, as the production worker does it. Both
            // are waited out within the drain's own deadline.
            let round = async {
                if let Some(step) = self
                    .db
                    .claim_step(&owner, Some(task_id), &lease_deadline())
                    .await?
                {
                    let _activity = self.db.hold_task_step(&step);
                    self.execute(step).await?;
                    return Ok(true);
                }
                Ok::<_, ServiceError>(false)
            }
            .await;
            match round {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) if database_busy(&error) => {
                    tracing::warn!(task_id, %error, "task step drain found the database busy; trying again");
                    tokio::time::sleep(BUSY_BACKOFF_START).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
    /// `Some` while `task_id` is in the integration queue with work the
    /// queue worker can do now: the Task whose attempt holds (or will take)
    /// the slot of its queue. `None` once the Task left the entry it was
    /// admitted in and its attempt holds no slot, or its attempt waits on
    /// time or its owner.
    ///
    /// An attempt that holds the slot is the worker's work whether or not its
    /// Task is still in that entry, and whether or not its cancel was asked
    /// for: only a head driver releases it (it cancels the attempt of a Task
    /// that left). A drain that returned there would stop the driver it
    /// started itself and never pump again; the Task's next entry into the
    /// queue would then retry against that attempt until its lease lapsed
    /// and something else swept the queue.
    ///
    /// The flag: the Task is a queued member of a queue that is not open.
    #[cfg(any(test, feature = "test-support"))]
    async fn integration_head_task(&self, task_id: &str) -> Result<(Option<String>, bool)> {
        let row: Option<db::IntegrationDrainMemberRow> =
            self.db.task_integration_drain_member(task_id).await?;
        let Some(db::IntegrationDrainMemberRow {
            state,
            queue_state,
            head_task,
            claimable,
            holds_slot,
            in_entry,
        }) = row
        else {
            return Ok((None, false));
        };
        if !in_entry && !holds_slot {
            return Ok((None, false));
        }
        let open = queue_state.as_deref() == Some("open");
        let live = match state.as_str() {
            "queued" => holds_slot || (open && claimable),
            "validating" | "rebasing" | "checking" | "awaiting_task_step" | "ready_ff"
            | "ff_inflight" | "applied" | "reconciling" => true,
            _ => false,
        };
        Ok((
            live.then(|| head_task.unwrap_or_else(|| task_id.to_owned())),
            in_entry && state == "queued" && !open && claimable,
        ))
    }
    async fn execute(&self, step: TaskStep) -> Result<()> {
        let id = step.id.clone();
        match self.execute_attempt(step).await {
            Err(error) => {
                let reply = self
                    .task_service
                    .task_step_replies
                    .lock()
                    .expect("Task step replies")
                    .remove(&id);
                if let Some(reply) = reply {
                    tracing::warn!(step_id=%id,%error,"Task step settlement failed; preserving durable intent");
                    let _ = reply.send(Err(error));
                    Ok(())
                } else {
                    Err(error)
                }
            }
            result => result,
        }
    }
    async fn execute_attempt(&self, step: TaskStep) -> Result<()> {
        let owner = step.claimed_by.as_deref().expect("claimed step owner");
        // Keep ownership until this row settles. A cascade's CAS inserts a
        // separate hooks row; that row owns and checkpoints its effects.
        // All commits fence this token.
        // Renewal runs independently: awaiting the pool from a select branch
        // must not stop polling a transition that currently owns a transaction.
        let (stop, mut stopped) = watch::channel(false);
        let db = self.db.clone();
        let id = step.id.clone();
        let token = owner.to_owned();
        let interval = self.renew_interval;
        let renewer = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _=super::supervisor::shutdown_signal(&mut stopped)=>return,
                    _=tokio::time::sleep(interval)=> {
                        match db.renew_step(&id,&token,&lease_deadline()).await {
                            Ok(true)=>{},
                            Ok(false)=>tracing::warn!(step_id=%id,"task step lost ownership; awaiting started transition"),
                            Err(error)=>tracing::warn!(step_id=%id,%error,"task step renewal failed; awaiting started transition"),
                        }
                    }
                }
            }
        });
        let control = self.db.register_step_control(&step);
        #[cfg(test)]
        crate::integration_steps::preemption_tests::capture_preempt(
            &step.task_id,
            control.preempt.clone(),
        );
        let integration: bool = self.db.task_step_integration_started(&step.id).await?;
        control
            .critical
            .store(integration, std::sync::atomic::Ordering::SeqCst);
        let mut preempt = control.preempt.subscribe();
        let requested: bool = self.db.task_step_preemption_requested(&step.id).await?;
        if requested {
            control.preempt.send_replace(true);
        }
        let mut work = Box::pin(db::task_writer::in_task_step(
            step.clone(),
            self.execute_inner(&step),
        ));
        let result = loop {
            // Integration, local or remote, is protected once it starts: it
            // runs to its own result, which settles normally. A remote merge
            // is never settled because a cancel acknowledgment is late; a
            // daemon disconnect settles it through workspace containment and
            // reconnect, never through a timeout. The waiting Cancel/Hold
            // stays queued behind it.
            if *preempt.borrow_and_update()
                && !control.critical.load(std::sync::atomic::Ordering::SeqCst)
            {
                break None;
            }
            tokio::select! {
                result=&mut work=>break Some(result),
                _=preempt.changed()=>{},
            }
        };
        // Drop the hook future before cancellation/lease settlement;
        // embedded CI, scripts and startup children are kill_on_drop.
        drop(work);
        let result = match result {
            Some(result) => result,
            None => {
                #[cfg(test)]
                preemption_diagnostics()
                    .lock()
                    .unwrap()
                    .insert(step.id.clone(), "preempt_step entered".into());
                let result = self.preempt_step(&step).await;
                #[cfg(test)]
                preemption_diagnostics().lock().unwrap().insert(
                    step.id.clone(),
                    format!("preempt_step returned: {result:?}"),
                );
                result
            }
        };
        self.db.release_step_control(&step);
        let _ = stop.send(true);
        if let Err(error) = renewer.await {
            tracing::warn!(%error,"task step renewal task stopped");
        }
        if let Err(error) = result {
            // The work is over; what is left records how it ended. A busy
            // database there is waited out: giving up would leave the step
            // claimed until its lease lapses and cost it an attempt.
            let requeued = wait_out_busy(|| self.settle_failed_attempt(&step, &error)).await?;
            // A re-queued command is still accepted and runs on its retry:
            // its waiter is not answered with this attempt's error (a caller
            // told "error" asks again, and both commands would run). It
            // keeps waiting, within its bound, for the retry's answer.
            if step.kind == "command" && !requeued {
                if let Some(reply) = self
                    .task_service
                    .task_step_replies
                    .lock()
                    .expect("Task step replies")
                    .remove(&step.id)
                {
                    let _ = reply.send(Err(error));
                }
            }
        }
        wait_out_busy(|| async {
            self.db
                .release_step(&step.id, owner)
                .await
                .map_err(ServiceError::from)
        })
        .await?;
        Ok(())
    }
    /// Record a failed attempt of a step this worker still owns. Every write
    /// is fenced by the claim, so repeating the whole of it is safe. `true`
    /// when the step was re-queued for another attempt.
    async fn settle_failed_attempt(&self, step: &TaskStep, error: &ServiceError) -> Result<bool> {
        let current = self
            .db
            .task_steps(&step.task_id)
            .await?
            .into_iter()
            .find(|s| s.id == step.id);
        if !current
            .as_ref()
            .is_some_and(|s| s.status == "claimed" && s.claimed_by == step.claimed_by)
        {
            tracing::warn!(step_id = %step.id, %error, "task step attempt lost ownership or was already settled");
            return Ok(false);
        }
        if !self.db.step_entry_matches(step).await? {
            self.settle(step, "superseded", Some(&error.to_string()), false, None)
                .await?;
        } else if step.attempts < MAX_STEP_ATTEMPTS
            && (database_busy(error)
                || matches!(
                    error,
                    ServiceError::Review(::review::ReviewError::CheckEnvironmentUnavailable)
                )
                || retryable(error))
        {
            // A hook that found the database busy wrote nothing in that
            // statement: it runs again from its checkpoints, as it does
            // after a lapsed lease, instead of failing its phase.
            self.db
                .retry_step(step, &error.to_string(), &retry_due(step.attempts))
                .await?;
            return Ok(true);
        } else if step.kind == "hooks" {
            self.fail_committed_hook_phase(step, &error.to_string())
                .await?;
        } else {
            self.settle(
                step,
                "failed",
                Some(&error.to_string()),
                true,
                (step.kind == "command").then_some(error),
            )
            .await?;
        }
        Ok(false)
    }
    async fn preempt_step(&self, step: &TaskStep) -> Result<()> {
        let operations = self.db.running_remote_task_operations(&step.id).await?;
        let unconfirmed = crate::remote_cancel::cancel_operations(
            &self.db,
            self.task_service.daemon_connections.clone(),
            &operations,
        )
        .await?;
        self.settle_preempted_step(step, unconfirmed).await
    }
    async fn settle_preempted_step(&self, step: &TaskStep, unconfirmed: bool) -> Result<()> {
        self.settle(
            step,
            "superseded",
            Some(if unconfirmed {
                "remote_operation_unconfirmed"
            } else {
                "preempted by owner command"
            }),
            false,
            None,
        )
        .await?;
        if let Some(reply) = self
            .task_service
            .task_step_replies
            .lock()
            .expect("Task step replies")
            .remove(&step.id)
        {
            let _ = reply.send(Err(db::DbError::VersionConflict.into()));
        }
        Ok(())
    }
    async fn execute_inner(&self, step: &TaskStep) -> Result<()> {
        if step.kind == "mutation" {
            let before = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
            let roles_before = db::TaskRoleAssignmentRepo::list_by_task(&*self.db, &step.task_id)
                .await?
                .into_iter()
                .map(|r| (r.role_name, r.assignee_type, r.assignee_id))
                .collect::<Vec<_>>();
            self.db.execute_task_mutation(step).await?;
            if let Some(task) = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await? {
                let roles_after =
                    db::TaskRoleAssignmentRepo::list_by_task(&*self.db, &step.task_id)
                        .await?
                        .into_iter()
                        .map(|r| (r.role_name, r.assignee_type, r.assignee_id))
                        .collect::<Vec<_>>();
                // Temporary dispatch/replay metadata deliberately leaves the
                // public Task version unchanged. It must not turn quiet
                // capacity probes into visible task.updated hints.
                if before
                    .as_ref()
                    .is_some_and(|old| old.version == task.version)
                    && roles_before == roles_after
                {
                    return Ok(());
                }
                self.engine.event_bus.publish(events::ForgeEvent {
                    event_type: "task.updated".into(),
                    entity_id: task.id,
                    timestamp: events::event_timestamp(),
                    context: events::EventContext::TaskUpdated {
                        project_id: task.project_id,
                    },
                });
            }
            return Ok(());
        }
        if step.kind == "command" {
            #[cfg(test)]
            if let Some(gate) = &self.command_test_gate {
                if gate
                    .pause(&self.db, step, tests::CommandPoint::BeforeCommand)
                    .await
                {
                    return Err(ServiceError::RateLimited {
                        retry_after_seconds: 1,
                    });
                }
            }
            let command: crate::task_service::commands::TaskCommand =
                serde_json::from_str(&step.payload_json)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            if !command.preempt
                && matches!(
                    command.operation.as_str(),
                    "reconcile_role"
                        | "transition"
                        | "transition_with_plan_publication"
                        | "engine_transition"
                        | "perform_task_action_as"
                )
                && !self.db.step_entry_matches(step).await?
            {
                self.settle(
                    step,
                    "superseded",
                    Some("Task left the command's accepted status entry"),
                    false,
                    None,
                )
                .await?;
                if let Some(reply) = self
                    .task_service
                    .task_step_replies
                    .lock()
                    .expect("Task step replies")
                    .remove(&step.id)
                {
                    let _ = reply.send(Err(db::DbError::VersionConflict.into()));
                }
                return Ok(());
            }
            if command.preempt {
                // A hooks step superseded while it waited for review-entry
                // CI: its remote run is cancelled or fenced, its consumer
                // cancelled and its review attempt closed, here in the
                // preempting command's own step, before the command acts.
                crate::check_runner::review_entry::abandon_superseded_waits(
                    &self.db,
                    self.task_service.daemon_connections.clone(),
                    &step.task_id,
                )
                .await?;
                // A disconnect may have failed the server-side hook before
                // Cancel arrived, although its owner command is still alive.
                let marked = self.db.pending_remote_cancels(None, None).await?;
                let operations = self
                    .db
                    .running_remote_operations_for_task(&step.task_id)
                    .await?
                    .into_iter()
                    .filter(|operation| {
                        !marked
                            .iter()
                            .any(|marker| marker.operation_id == operation.operation_id)
                    })
                    .collect::<Vec<_>>();
                let unconfirmed = crate::remote_cancel::cancel_operations(
                    &self.db,
                    self.task_service.daemon_connections.clone(),
                    &operations,
                )
                .await?;
                for operation in operations {
                    self.db
                        .supersede_failed_remote_task_step(
                            if unconfirmed {
                                "remote_operation_unconfirmed"
                            } else {
                                "preempted by owner command"
                            },
                            db::now_rfc3339(),
                            operation.step_id,
                            &step.task_id,
                        )
                        .await?;
                }
            }
            #[cfg(any(test, feature = "test-support"))]
            let fault = test_hooks::take(&self.task_service, &step.task_id);
            #[cfg(any(test, feature = "test-support"))]
            let durable_reply = fault.as_ref().is_some_and(|(_, durable)| *durable);
            let result = async {
                #[cfg(any(test, feature = "test-support"))]
                if let Some((error, _)) = fault {
                    return Err(error);
                }
                if command.operation == "engine_transition" {
                    #[derive(Deserialize)]
                    struct Input {
                        #[serde(flatten)]
                        pub bridge: api_types::TransitionBridge,
                        task_id: String,
                        target_state: String,
                        workflow: api_types::WorkflowDefinition,
                        actor: Actor,
                        reason: String,
                        rejection: bool,
                        skip_before_exit: bool,
                        defer_dispatch_until: Option<String>,
                        board_move: Option<crate::workflow::engine::BoardMoveRequest>,
                        authority: Option<WorkflowAuthority>,
                        entry_retry: bool,
                    }
                    let input: Input = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let current = TaskRepo::get_by_id(&*self.db, &input.task_id, false)
                        .await?
                        .ok_or(db::DbError::NotFound)?;
                    self.engine
                        .bind(&self.task_service)
                        .transition_inner(
                            input.task_id,
                            input.target_state,
                            current.version,
                            &input.workflow,
                            input.actor,
                            input.reason,
                            input.rejection,
                            input.skip_before_exit,
                            input.defer_dispatch_until,
                            input.board_move,
                            None,
                            input.authority,
                            input.entry_retry,
                            input.bridge,
                        )
                        .await
                        .and_then(|value| {
                            serde_json::to_value(value)
                                .map_err(|e| ServiceError::invalid_operation(e.to_string()))
                        })
                } else {
                    crate::TaskService::with_recovery_command_context(
                        &step.payload_json,
                        self.task_service.execute_task_command(&command),
                    )
                    .await
                }
            }
            .await;
            // This command only reads candidate facts and makes idempotent
            // queue writes. A reconnect failure goes through the normal step
            // retry bound; deterministic/exhausted failures park for the owner.
            if command.operation == "retry_integration" && result.is_err() {
                return result.map(|_| ());
            }
            let error = result.as_ref().err().map(ToString::to_string);
            let stored = command_reply::CommandReply::encode(&result)?;
            // The command has run. A busy database here must not send the
            // step back through `retry_step`: the next attempt would run the
            // command a second time. Wait the lock out; if it never clears,
            // the caller still gets the command's own answer and the step
            // is failed, not re-queued.
            let status = if result.is_ok() { "done" } else { "failed" };
            let settled = wait_out_busy(|| async {
                #[cfg(test)]
                if let Some(gate) = &self.command_test_gate {
                    if gate.inject(step, tests::CommandPoint::BeforeSettle).await {
                        return Err(tests::busy());
                    }
                }
                let mut tx = db::begin_immediate(self.db.pool()).await?;
                self.db
                    .finish_step_in_tx(&mut tx, step, status, error.as_deref())
                    .await?;
                self.db
                    .store_task_step_command_result_in_tx(
                        &mut tx,
                        &stored,
                        &step.id,
                        &step.claimed_by,
                    )
                    .await?;
                tx.commit().await?;
                Ok(())
            })
            .await;
            match settled {
                Ok(()) => {}
                Err(unsettled) if database_busy(&unsettled) => {
                    tracing::error!(step_id=%step.id,error=%unsettled,"Task command ran but its result could not be recorded; the step is failed, not run again");
                    if let Some(reply) = self
                        .task_service
                        .task_step_replies
                        .lock()
                        .expect("Task step replies")
                        .remove(&step.id)
                    {
                        let _ = reply.send(result);
                    }
                    return Err(ServiceError::invalid_operation(format!(
                        "Task command ran but its result could not be recorded: {unsettled}"
                    )));
                }
                Err(unsettled) => return Err(unsettled),
            }
            #[cfg(test)]
            if let Some(gate) = &self.command_test_gate {
                gate.pause(&self.db, step, tests::CommandPoint::BeforeReply)
                    .await;
            }
            // The result is durable: a release that fails here is repeated
            // by `execute_attempt`, and never replaces the caller's answer.
            let released = async {
                #[cfg(test)]
                if let Some(gate) = &self.command_test_gate {
                    if gate.inject(step, tests::CommandPoint::BeforeRelease).await {
                        return Err(tests::busy());
                    }
                }
                self.db
                    .release_step(&step.id, step.claimed_by.as_deref().expect("command owner"))
                    .await
                    .map_err(ServiceError::from)
            }
            .await;
            if let Err(error) = &released {
                tracing::warn!(step_id=%step.id,%error,"settled Task command was not released yet");
            }
            self.db.domain_event_notify().notify_waiters();
            #[cfg(test)]
            if let Some(gate) = &self.command_test_gate {
                gate.inject(step, tests::CommandPoint::BeforeSend).await;
                if gate.drops_reply() {
                    self.task_service
                        .task_step_replies
                        .lock()
                        .expect("Task step replies")
                        .remove(&step.id);
                }
            }
            // A lost live reply stays registered and unsent, as it does when
            // a claimer stops before its send: the waiter answers from the
            // durable row. Production never drops a sender without sending.
            #[cfg(any(test, feature = "test-support"))]
            if durable_reply {
                return Ok(());
            }
            if let Some(reply) = self
                .task_service
                .task_step_replies
                .lock()
                .expect("Task step replies")
                .remove(&step.id)
            {
                let _ = reply.send(result);
            }
            return Ok(());
        }
        if step.kind == crate::integration_steps::INTEGRATION_STEP_KIND {
            // The integration queue's requests (not produced until the queue
            // is activated). Each action fences itself on its attempt.
            return crate::integration_steps::IntegrationSteps::new(self.task_service.clone())
                .execute(step)
                .await;
        }
        let task = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
        if !self.db.step_entry_matches(step).await? {
            self.settle(
                step,
                "superseded",
                Some("Task left the producing status entry"),
                false,
                None,
            )
            .await?;
            // A step that left its entry while it waited for a check never
            // reads the result: close the review attempt it opened.
            return crate::check_runner::review_entry::abandon_superseded_waits(
                &self.db,
                self.task_service.daemon_connections.clone(),
                &step.task_id,
            )
            .await;
        }
        if step.kind == "hooks" {
            let payload: crate::workflow::engine::durable::HookPayload =
                serde_json::from_str(&step.payload_json)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            let result = self
                .engine
                .bind(&self.task_service)
                .execute_hook_step(step, &payload)
                .await?;
            if let Some(wait) = &result.suspend {
                // A hook asked for a check: the step waits, suspended and
                // uncharged, for the result's delivery to wake it.
                self.db
                    .suspend_hook_step(step, &wait.until, &wait.consumer_id)
                    .await?;
                return Ok(());
            }
            if let Some(reason) = result.retry.as_deref() {
                // A transient `run_merge` failure takes the cascade retry
                // budget and back-off; once spent it settles as a merge failure.
                if step.attempts < MAX_STEP_ATTEMPTS {
                    tracing::info!(step_id = %step.id, task_id = %step.task_id, attempts = step.attempts, error = %reason, "transient merge failure; retrying hook step");
                    self.db
                        .retry_step(step, reason, &retry_due(step.attempts))
                        .await?;
                    return Ok(());
                }
            }
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            self.db.fence_hook_in_tx(&mut tx, step).await?;
            let merged: bool = self
                .db
                .task_hook_step_merge_done_in_tx(&mut tx, &step.id)
                .await?;
            if result.failure.is_none() && merged {
                let task = self
                    .db
                    .get_task_in_tx(&mut tx, &step.task_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                if crate::deferred_dispatch::paused_integration(&task)
                    .is_some_and(|m| m.state == step.expected_status)
                {
                    self.db
                        .consume_paused_integration_after_merge_in_tx(
                            &mut tx,
                            db::now_rfc3339(),
                            &task.id,
                            task.version,
                        )
                        .await?;
                    self.db
                        .state_condition_in_tx(&mut tx, &task.id, db::ConditionChange::Legacy)
                        .await?;
                }
            }
            let status = if result.failure.is_some() {
                "failed"
            } else {
                "done"
            };
            self.db
                .finish_step_in_tx(&mut tx, step, status, result.failure.as_deref())
                .await?;
            if let Some(mut follow_up) = result.follow_up {
                follow_up.available_at = db::now_rfc3339();
                self.db.enqueue_step_in_tx(&mut tx, &follow_up).await?;
                if merged {
                    self.db
                        .prioritize_merged_follow_up_step_in_tx(&mut tx, &follow_up.id)
                        .await?;
                }
            } else if let Some(error) = result.merge_failure {
                // Whatever its policy, a merge that cannot complete must not
                // sit silently in its merge state: annotate it as a failed
                // merge, which the owner's Retry re-runs.
                let mut task = self
                    .db
                    .get_task_in_tx(&mut tx, &step.task_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                self.annotate_failure_in_tx(
                    &mut tx,
                    step,
                    &mut task,
                    api_types::FailureKind::WorkspaceError,
                    &format!("Merge failed: {error}"),
                )
                .await?;
            } else if let Some(error) = result.blocking_failure {
                // Any other `Log`-policy failure only settles the step
                // `failed` and is logged; it never blocks the Task.
                let mut task = self
                    .db
                    .get_task_in_tx(&mut tx, &step.task_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                self.annotate_in_tx(
                    &mut tx,
                    step,
                    &mut task,
                    "failed",
                    Some(&format!("Transition committed; hook failed: {error}")),
                )
                .await?;
            }
            tx.commit().await?;
            self.db.domain_event_notify().notify_waiters();
            // Entry hooks fence dispatch while they run. A continuation the
            // dispatcher skipped meanwhile (a queued role retry) becomes
            // dispatchable only now, so wake the dispatcher.
            self.task_service.dispatch_wake.notify_one();
            return Ok(());
        }
        let payload: CascadePayload =
            serde_json::from_str(&step.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid cascade payload: {error}"))
            })?;
        let task = task.as_ref().expect("matching Task");
        let (workflow, authority) = match &payload.workflow_ref {
            WorkflowReference::Project => {
                let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                (
                    WorkflowEngine::resolve_workflow_for_task(
                        task,
                        &project.workflow_definition,
                        &cascade_actor(),
                    ),
                    Some(WorkflowAuthority {
                        project_version: project.version,
                        workflow_definition: project.workflow_definition,
                        clear_review_passed_at_on_commit: payload.clear_review_passed_at_on_commit,
                    }),
                )
            }
            WorkflowReference::Snapshot(id) => (
                serde_json::from_str(&self.db.step_workflow(id).await?)
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                None,
            ),
            WorkflowReference::Definition(_) => {
                return Err(ServiceError::invalid_operation(
                    "queued workflow definition was not frozen at enqueue",
                ))
            }
        };
        let current_lane = cascade_lane(&workflow, &payload.to);
        if current_lane != step.lane {
            self.db.reroute_step(step, current_lane).await?;
            return Ok(());
        }
        if matches!(payload.workflow_ref, WorkflowReference::Project) {
            let source = workflow
                .states
                .iter()
                .find(|state| state.name == step.expected_status);
            let target = workflow
                .states
                .iter()
                .find(|state| state.name == payload.to);
            let edge = workflow
                .outgoing_trigger_targets(&step.expected_status)
                .any(|(_, to)| to == payload.to);
            let routing_exception = payload.skip_before_exit
                && (target.is_some_and(|state| state.kind == api_types::StateKind::Initial)
                    || payload.to == step.expected_status)
                || workflow.cancellation_state.as_deref() == Some(payload.to.as_str())
                || crate::task_hierarchy::replan_edge(&workflow, &payload.bridge, &payload.to);
            if source.is_none() || target.is_none() || (!edge && !routing_exception) {
                return self
                    .settle(
                        step,
                        "superseded",
                        Some("Cascade edge is no longer allowed by the current workflow"),
                        false,
                        None,
                    )
                    .await;
            }
        }
        if let Some(source) = workflow
            .states
            .iter()
            .find(|state| state.name == step.expected_status)
        {
            // A lead root's rejection re-plans: its target is the planning
            // state, over the re-plan edge (`effective_reject_target`).
            let rejection_to_target = payload.rejection
                && (source.gate_config.as_ref().is_some_and(|gate| {
                    gate.reject_target.as_deref() == Some(payload.to.as_str())
                }) || crate::task_hierarchy::replan_edge(
                    &workflow,
                    &payload.bridge,
                    &payload.to,
                ));
            if !WorkflowEngine::cascade_allowed(source, &payload.bridge, rejection_to_target) {
                return self
                    .settle(
                        step,
                        "superseded",
                        Some("Cascade requires approval in the current workflow"),
                        false,
                        None,
                    )
                    .await;
            }
        }
        let repeated = self
            .db
            .chain_steps(&step.chain_id)
            .await?
            .iter()
            .any(|prior| {
                prior.kind == "cascade"
                    && prior.chain_id == step.chain_id
                    && prior.chain_position < step.chain_position
                    && prior.status == "done"
                    && prior.expected_status == step.expected_status
                    && serde_json::from_str::<CascadePayload>(&prior.payload_json)
                        .is_ok_and(|p| p.to == payload.to)
            });
        if repeated || step.chain_position > CHAIN_LIMIT {
            let reason = format!("Automatic workflow loop detected: {} → {}; chain {}, step {} (limit {CHAIN_LIMIT})", step.expected_status, payload.to, step.chain_id, step.chain_position);
            self.settle(step, "parked", Some(&reason), true, None)
                .await?;
            return Ok(());
        }
        let result = self
            .engine
            .bind(&self.task_service)
            .transition_step(step, &payload, &workflow, authority)
            .await?;
        // These used to observe the final recursive result in TaskService's
        // wrapper. The queued terminal hop now owns that same settlement.
        if workflow.state_kind(&result.task.status) == Some(api_types::StateKind::Terminal) {
            if workflow.cancellation_state.as_deref() != Some(result.task.status.as_str()) {
                if let Err(error) = self
                    .task_service
                    .wake_dependents_of_completed_task(&result.task)
                    .await
                {
                    tracing::warn!(task_id=%result.task.id, %error, "failed to wake dependents after committed cascade");
                }
            }
            self.task_service
                .reconcile_terminal_subtask(&result.task)
                .await;
        }
        if workflow.state_kind(&result.task.status) == Some(api_types::StateKind::Initial) {
            if let Err(error) = self
                .task_service
                .finish_initial_unverified_refusal(&result.task)
                .await
            {
                tracing::warn!(task_id=%result.task.id,%error,"initial cascade refusal bookkeeping remains pending");
            }
        }
        Ok(())
    }
    async fn settle(
        &self,
        step: &TaskStep,
        status: &str,
        error: Option<&str>,
        annotate: bool,
        reply_error: Option<&ServiceError>,
    ) -> Result<()> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let mut task = self
            .db
            .get_task_in_tx(&mut tx, &step.task_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", step.task_id.clone()))?;
        let observed_epoch: i64 = self
            .db
            .task_step_settlement_epoch_in_tx(&mut tx, &step.task_id)
            .await?;
        let matches = task.deleted_at.is_none()
            && task.status == step.expected_status
            && observed_epoch == step.expected_epoch;
        let status = if annotate && !matches {
            "superseded"
        } else {
            status
        };
        self.db
            .finish_step_in_tx(&mut tx, step, status, error)
            .await?;
        if let Some(error) = reply_error {
            self.db
                .store_task_step_command_result_in_tx(
                    &mut tx,
                    &command_reply::CommandReply::encode_error(error)?,
                    &step.id,
                    &step.claimed_by,
                )
                .await?;
        }
        if status == "superseded" {
            let mut event = db::CreateDomainEvent::task_interruption_changed(&task);
            event.id = db::new_uuid_v4();
            event.event_type = "transition.step_superseded".into();
            event.entity_id = step.task_id.clone();
            event.scope_id = step.task_id.clone();
            event.dedupe_key = Some(format!("task-step-superseded:{}", step.id));
            event.payload_json = serde_json::json!({"task_id":step.task_id,"step_id":step.id,"expected_status":step.expected_status,"expected_epoch":step.expected_epoch,"observed_status":task.status,"observed_epoch":observed_epoch,"reason":error}).to_string();
            tracing::debug!(step_id=%step.id, expected_status=%step.expected_status, expected_epoch=step.expected_epoch, observed_status=%task.status, observed_epoch, "cascade superseded");
            db::DomainEventRepo::append_event_in_tx(&*self.db, &mut tx, &event).await?;
        }
        if annotate && matches {
            self.annotate_in_tx(&mut tx, step, &mut task, status, error)
                .await?;
        }
        tx.commit().await?;
        // A concurrent direct writer may supersede the step while parking
        // takes its transaction. Publish only the outcome that committed.
        if status == "parked" {
            self.engine.event_bus.publish(events::ForgeEvent {
                event_type: "transition.loop_detected".into(),
                entity_id: step.task_id.clone(),
                timestamp: events::event_timestamp(),
                context: events::EventContext::TransitionLoopDetected {
                    task_id: step.task_id.clone(),
                    state: step.expected_status.clone(),
                    chain_id: step.chain_id.clone(),
                    chain_position: step.chain_position,
                    reason: error.unwrap_or_default().to_owned(),
                },
            });
        }
        Ok(())
    }
    /// `task` must have been read inside `tx`; the write lock then guarantees
    /// its version is current. The row count is still checked so an event
    /// never claims an annotation that was not written.
    async fn annotate_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step: &TaskStep,
        task: &mut db::Task,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let kind = if status == "parked" {
            api_types::FailureKind::WorkflowLoop
        } else {
            api_types::FailureKind::CascadeFailed
        };
        self.write_annotation_in_tx(tx, step, task, status, kind, error)
            .await
    }

    /// A failed hook phase's own failure kind (a merge failure).
    async fn annotate_failure_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step: &TaskStep,
        task: &mut db::Task,
        kind: api_types::FailureKind,
        message: &str,
    ) -> Result<()> {
        self.write_annotation_in_tx(tx, step, task, "failed", kind, Some(message))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn write_annotation_for_test_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step: &TaskStep,
        task: &mut db::Task,
        status: &str,
        kind: api_types::FailureKind,
        error: Option<&str>,
    ) -> Result<()> {
        self.write_annotation_in_tx(tx, step, task, status, kind, error)
            .await
    }

    async fn write_annotation_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step: &TaskStep,
        task: &mut db::Task,
        status: &str,
        kind: api_types::FailureKind,
        error: Option<&str>,
    ) -> Result<()> {
        let now = db::now_rfc3339();
        let mut annotation = serde_json::to_string(&channels::ErrorAnnotation::new(
            channels::AnnotationShape::StepFailure(channels::StepFailureAnnotation {
                kind,
                state: step.expected_status.clone(),
                message: error.map(str::to_owned),
                detected_at: now.clone(),
                task_step_id: step.id.clone(),
                blocking_reason: (kind == api_types::FailureKind::WorkspaceError)
                    .then(|| error.map(str::to_owned)),
            }),
        ))
        .expect("condition channel serializes");
        if status != "parked" {
            if let Some(mut existing) = task
                .error_annotation
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            {
                if task.blocked_json.is_some()
                    || task.failed_json.is_some()
                    || existing["type"]
                        .as_str()
                        .is_some_and(crate::task_dispatcher::is_blocking_annotation_type)
                {
                    if let Some(mut typed) = task
                        .error_annotation
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<channels::ErrorAnnotation>(raw).ok())
                    {
                        typed.set_step_failure(channels::StepFailureOverlay {
                            task_step_id: step.id.clone(),
                            task_step_error: error.map(str::to_owned),
                        });
                        annotation =
                            serde_json::to_string(&typed).expect("condition channel serializes");
                    } else {
                        // Historical shapes retain the original merge, including unknown kinds.
                        existing["task_step_id"] = serde_json::json!(step.id);
                        existing["task_step_error"] = serde_json::json!(error);
                        annotation = existing.to_string();
                    }
                }
            }
        }
        let blocked = (status == "parked").then(|| {
            serde_json::to_string(&channels::Interruption {
                reason: error.map(str::to_owned),
                blocked_at: Some(Some(now.clone())),
                source: Some(Some("workflow_loop".into())),
                ..Default::default()
            })
            .expect("condition channel serializes")
        });
        let written = self
            .db
            .annotate_task_step_failure_in_tx(
                tx,
                annotation.clone(),
                &blocked,
                &now,
                &step.task_id,
                task.version,
            )
            .await?
            .rows_affected();
        if written != 1 {
            return Err(db::DbError::VersionConflict.into());
        }
        self.db
            .state_condition_in_tx(tx, &step.task_id, db::ConditionChange::Legacy)
            .await?;
        task.error_annotation = Some(annotation);
        if blocked.is_some() {
            task.blocked_json = blocked;
        }
        task.version += 1;
        task.updated_at = now;
        task.condition = self
            .db
            .get_task_in_tx(tx, &task.id)
            .await?
            .ok_or(db::DbError::NotFound)?
            .condition;
        let event = db::CreateDomainEvent::task_interruption_changed(task);
        db::DomainEventRepo::append_event_in_tx(&*self.db, tx, &event).await?;
        if status == "parked" {
            let mut loop_event = event;
            loop_event.id = db::new_uuid_v4();
            loop_event.event_type = "transition.loop_detected".into();
            loop_event.dedupe_key = Some(format!("task-step-loop:{}", step.id));
            loop_event.payload_json = serde_json::json!({"task_id":step.task_id,"state":step.expected_status,"chain_id":step.chain_id,"chain_position":step.chain_position,"reason":error}).to_string();
            db::DomainEventRepo::append_event_in_tx(&*self.db, tx, &loop_event).await?;
        }
        Ok(())
    }

    async fn fail_committed_hook_phase(&self, step: &TaskStep, error: &str) -> Result<()> {
        let message = format!("Transition committed; hook failed: {error}");
        self.settle(step, "failed", Some(&message), true, None)
            .await
    }
}
fn log_job(result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>) {
    match result {
        Some(Ok(Err(error))) => {
            tracing::warn!(%error,"task step settlement failed; lease will recover")
        }
        Some(Err(error)) => tracing::warn!(%error,"task step job panicked; lease will recover"),
        _ => {}
    }
}
fn lease_deadline() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(LEASE_SECONDS)).to_rfc3339()
}
/// Attempts (including the first) a step gets for retryable failures.
pub(crate) const MAX_STEP_ATTEMPTS: i64 = 8;
/// Attempts (including the first) a review entry asks the owner's login
/// shell for its environment before the Task shows the interruption: about
/// seven seconds of back-off on top of each probe's own bound.
pub(crate) const ENVIRONMENT_ATTEMPTS: i64 = 4;
const _: () = assert!(ENVIRONMENT_ATTEMPTS < MAX_STEP_ATTEMPTS);

/// Exponential back-off for the next attempt: 1 s doubling to 64 s.
fn retry_due(attempts: i64) -> String {
    let delay = 1_i64 << (attempts - 1).clamp(0, 6);
    (chrono::Utc::now() + chrono::Duration::seconds(delay)).to_rfc3339()
}

/// SQLite refused or gave up on a lock: nothing was written.
fn database_busy(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Db(error) if error.is_busy())
}

const BUSY_BACKOFF_START: Duration = Duration::from_millis(50);
const BUSY_BACKOFF_CAP: Duration = Duration::from_secs(2);
/// Tries after the first: about ten seconds of waiting on top of each
/// statement's own busy timeout.
const BUSY_RETRIES: usize = 8;

/// Run `operation` again, with backoff, while the database is busy.
async fn wait_out_busy<T, F, Fut>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = BUSY_BACKOFF_START;
    for _ in 0..BUSY_RETRIES {
        match operation().await {
            Err(error) if database_busy(&error) => {
                tracing::warn!(%error, "task step bookkeeping found the database busy; trying again");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(BUSY_BACKOFF_CAP);
            }
            result => return result,
        }
    }
    operation().await
}

pub(crate) fn retryable(error: &ServiceError) -> bool {
    match error {
        ServiceError::Db(db::DbError::VersionConflict) => true,
        // A checkout read that can clear by itself. A missing path or a
        // refused permission stays the same on every attempt: it parks.
        ServiceError::Git(git::GitError::Io(io))
        | ServiceError::Review(::review::ReviewError::Io(io)) => {
            matches!(
                io.kind(),
                std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::ResourceBusy
            )
        }
        _ => consumer_error_kind(error) == WorkerErrorKind::Transient,
    }
}

// The step uses the same actor as the former recursive hop.
pub(crate) fn cascade_actor() -> Actor {
    Actor::system(SystemComponent::Workflow)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod condition_tests;

#[cfg(test)]
mod workflow_loop_tests;
