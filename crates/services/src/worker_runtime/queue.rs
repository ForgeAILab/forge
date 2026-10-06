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
use db::{TaskRepo, TaskStep, TaskStepRepo};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};

pub const CHAIN_LIMIT: i64 = 64;
// Long steps (merge, CI review checks) can wait on one repository's lock for
// the whole hook. They get their own four slots so the eight fast slots keep
// every other Project's cascades moving. Per-repository head-of-line inside
// the long lane is a 3.2 follow-up.
const FAST_CONCURRENCY: usize = 8;
const LONG_CONCURRENCY: usize = 4;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(8);
const LEASE_SECONDS: i64 = 60;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub(crate) enum WorkflowReference {
    Project,
    Snapshot(String),
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
impl TaskStepWorker {
    pub fn new(task_service: crate::TaskService) -> Self {
        let engine = task_service.workflow_engine();
        Self {
            task_service,
            db: Arc::clone(&engine.db),
            engine,
            renew_interval: Duration::from_secs(15),
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
                && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND integration_started_at IS NOT NULL AND status='done')").bind(task_id).fetch_one(self.db.pool()).await?
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
            let hooks: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_step WHERE task_id=? AND status IN ('pending','claimed') AND (kind='hooks' OR (kind='command' AND json_extract(payload_json,'$.operation') IN ('start_execution','claim_task','claim_and_start_task','launch_execution','rerun_review','dispatch_initial_role_execution_with_metadata_and_admission','dispatch_initial_role_execution_with_optional_admission','dispatch_recovery_role','follow_up_interactive_execution','re_execute_execution_with_context'))))")
                .bind(task_id).fetch_one(self.db.pool()).await?;
            payload["preempting_hooks"] = serde_json::json!(hooks);
        }
        let id = db::new_uuid_v4();
        let (reply, mut received) = tokio::sync::oneshot::channel();
        self.task_service
            .task_step_replies
            .lock()
            .expect("Task step replies")
            .insert(id.clone(), reply);
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
                        lane: if startup_checks
                            || command.operation == "rerun_review"
                            || (command.operation == "dispatch_queued_recovery"
                                && crate::deferred_dispatch::queued_recovery(&task).is_some_and(
                                    |q| q.request.offer.reason == "review_checks_retry",
                                ))
                        {
                            "long"
                        } else {
                            "fast"
                        }
                        .to_owned(),
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
            self.task_service
                .task_step_replies
                .lock()
                .expect("Task step replies")
                .remove(&id);
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
        let result = loop {
            let changed = notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let own = self
                .db
                .task_steps(task_id)
                .await?
                .into_iter()
                .find(|step| step.id == id)
                .ok_or(db::DbError::NotFound)?;
            if own.status == "superseded" {
                break Err(db::DbError::VersionConflict.into());
            }
            if own.status == "claimed" || own.status == "done" || own.status == "failed" {
                break received
                    .await
                    .map_err(|_| {
                        ServiceError::invalid_operation("Task step reply was interrupted")
                    })?
                    .and_then(|value| {
                        serde_json::from_value(value)
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))
                    });
            }
            self.start_inline_head(task_id).await?;
            // One deadline bounds the whole predecessor wait, including a
            // protected merge ahead of a Cancel/Hold. The accepted command
            // stays queued and runs after the predecessor settles.
            tokio::select! {
                biased;
                reply=&mut received=>break reply.map_err(|_|ServiceError::invalid_operation("Task step reply was interrupted"))?
                    .and_then(|value|serde_json::from_value(value).map_err(|e|ServiceError::invalid_operation(e.to_string()))),
                _=changed=>{},
                _=tokio::time::sleep_until(deadline)=>break Err(db::DbError::TaskBusy {pending_steps:self.db.pending_steps(task_id).await?,retry_after_ms:250}.into()),
            }
        };
        self.task_service
            .task_step_replies
            .lock()
            .expect("Task step replies")
            .remove(&id);
        result
    }

    fn start_inline_head<'a>(
        self: &'a Arc<Self>,
        task_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let lane = sqlx::query_scalar::<_, String>("SELECT lane FROM task_step WHERE task_id=? AND status IN ('pending','claimed') ORDER BY CASE WHEN integration_started_at IS NOT NULL THEN 2 ELSE priority END DESC,seq LIMIT 1")
                .bind(task_id).fetch_optional(self.db.pool()).await?.unwrap_or_else(||"fast".to_owned());
            if let Some(step) = self
                .db
                .claim_step_lane(
                    &db::new_uuid_v4(),
                    Some(task_id),
                    Some(&lane),
                    &lease_deadline(),
                )
                .await?
            {
                let activity = self.db.hold_task_step(&step);
                let worker = self.clone();
                tokio::spawn(async move {
                    let _activity = activity;
                    if let Err(error) = worker.execute(step).await {
                        tracing::warn!(%error,"inline Task step failed");
                    }
                });
            }
            Ok(())
        })
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
    pub async fn drain(&self, task_id: &str) -> Result<db::Task> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        loop {
            if self.db.pending_steps(task_id).await? == 0
                && !self.db.task_step_is_running(task_id)
                && !self.db.task_steps(task_id).await?.iter().any(|step| {
                    step.lease_until
                        .as_deref()
                        .is_some_and(|until| until > db::now_rfc3339().as_str())
                })
            {
                return TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ServiceError::invalid_operation("task step drain timed out"));
            }
            let owner = db::new_uuid_v4();
            if let Some(step) = self
                .db
                .claim_step(&owner, Some(task_id), &lease_deadline())
                .await?
            {
                let _activity = self.db.hold_task_step(&step);
                self.execute(step).await?;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
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
        let integration: bool = sqlx::query_scalar(
            "SELECT integration_started_at IS NOT NULL FROM task_step WHERE id=?",
        )
        .bind(&step.id)
        .fetch_one(self.db.pool())
        .await?;
        control
            .critical
            .store(integration, std::sync::atomic::Ordering::SeqCst);
        let mut preempt = control.preempt.subscribe();
        let requested: bool =
            sqlx::query_scalar("SELECT preempt_requested_at IS NOT NULL FROM task_step WHERE id=?")
                .bind(&step.id)
                .fetch_one(self.db.pool())
                .await?;
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
            None => self.preempt_step(&step).await,
        };
        self.db.release_step_control(&step);
        let _ = stop.send(true);
        if let Err(error) = renewer.await {
            tracing::warn!(%error,"task step renewal task stopped");
        }
        if let Err(error) = result {
            let current = self
                .db
                .task_steps(&step.task_id)
                .await?
                .into_iter()
                .find(|s| s.id == step.id);
            if current
                .as_ref()
                .is_some_and(|s| s.status == "claimed" && s.claimed_by == step.claimed_by)
            {
                if !self.db.step_entry_matches(&step).await? {
                    self.settle(&step, "superseded", Some(&error.to_string()), false)
                        .await?;
                } else if step.kind == "hooks" {
                    self.fail_committed_hook_phase(&step, &error.to_string())
                        .await?;
                } else if retryable(&error) && step.attempts < MAX_STEP_ATTEMPTS {
                    self.db
                        .retry_step(&step, &error.to_string(), &retry_due(step.attempts))
                        .await?;
                } else {
                    self.settle(&step, "failed", Some(&error.to_string()), true)
                        .await?;
                }
            } else {
                tracing::warn!(step_id = %step.id, %error, "task step attempt lost ownership or was already settled");
            }
            if step.kind == "command" {
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
        self.db.release_step(&step.id, owner).await?;
        Ok(())
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
            let command: crate::task_service::commands::TaskCommand =
                serde_json::from_str(&step.payload_json)
                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
            if !command.preempt
                && matches!(
                    command.operation.as_str(),
                    "transition"
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
                    sqlx::query("UPDATE task_step SET status='superseded',last_error=?,updated_at=? WHERE id=? AND task_id=? AND status IN ('failed','parked')")
                        .bind(if unconfirmed {"remote_operation_unconfirmed"}else{"preempted by owner command"}).bind(db::now_rfc3339()).bind(operation.step_id).bind(&step.task_id).execute(self.db.pool()).await?;
                }
            }
            let result = if command.operation == "engine_transition" {
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
            };
            let error = result.as_ref().err().map(ToString::to_string);
            let stored = serde_json::json!({"result":result.as_ref().ok(),"error":error.as_ref().map(|message|serde_json::json!({"code":"check","message":message}))});
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            self.db
                .finish_step_in_tx(
                    &mut tx,
                    step,
                    if result.is_ok() { "done" } else { "failed" },
                    error.as_deref(),
                )
                .await?;
            sqlx::query("UPDATE task_step SET result_json=? WHERE id=? AND claimed_by=?")
                .bind(stored.to_string())
                .bind(&step.id)
                .bind(&step.claimed_by)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            self.db
                .release_step(&step.id, step.claimed_by.as_deref().expect("command owner"))
                .await?;
            self.db.domain_event_notify().notify_waiters();
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
        let task = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
        if !self.db.step_entry_matches(step).await? {
            return self
                .settle(
                    step,
                    "superseded",
                    Some("Task left the producing status entry"),
                    false,
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
            let merged:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_hook_checkpoint WHERE step_id=? AND json_type(json_extract(effects_json,'$.merge_outcome'),'$.Done') IS NOT NULL)").bind(&step.id).fetch_one(&mut *tx).await?;
            if result.failure.is_none() && merged {
                let task = self
                    .db
                    .get_task_in_tx(&mut tx, &step.task_id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                if crate::deferred_dispatch::paused_integration(&task)
                    .is_some_and(|m| m.state == step.expected_status)
                {
                    sqlx::query("UPDATE task SET metadata_json=json_set(json_remove(COALESCE(metadata_json,'{}'),'$.paused_integration'),'$.paused_integration_generation',COALESCE(json_extract(metadata_json,'$.paused_integration_generation'),0)+1),updated_at=? WHERE id=? AND version=?")
                        .bind(db::now_rfc3339()).bind(&task.id).bind(task.version).execute(&mut *tx).await?;
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
                    sqlx::query("UPDATE task_step SET priority=2 WHERE id=?")
                        .bind(&follow_up.id)
                        .execute(&mut *tx)
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
                || workflow.cancellation_state.as_deref() == Some(payload.to.as_str());
            if source.is_none() || target.is_none() || (!edge && !routing_exception) {
                return self
                    .settle(
                        step,
                        "superseded",
                        Some("Cascade edge is no longer allowed by the current workflow"),
                        false,
                    )
                    .await;
            }
        }
        if let Some(source) = workflow
            .states
            .iter()
            .find(|state| state.name == step.expected_status)
        {
            let rejection_to_target = payload.rejection
                && source
                    .gate_config
                    .as_ref()
                    .is_some_and(|gate| gate.reject_target.as_deref() == Some(payload.to.as_str()));
            if !WorkflowEngine::cascade_allowed(source, &payload.bridge, rejection_to_target) {
                return self
                    .settle(
                        step,
                        "superseded",
                        Some("Cascade requires approval in the current workflow"),
                        false,
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
            self.settle(step, "parked", Some(&reason), true).await?;
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
    ) -> Result<()> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let mut task = self
            .db
            .get_task_in_tx(&mut tx, &step.task_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", step.task_id.clone()))?;
        let observed_epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
            .bind(&step.task_id)
            .fetch_one(&mut *tx)
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
        let mut annotation = serde_json::json!({"type":kind, "state":step.expected_status, "message":error, "detected_at":now, "task_step_id":step.id});
        if kind == api_types::FailureKind::WorkspaceError {
            annotation["blocking_reason"] = serde_json::json!(error);
        }
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
                    existing["task_step_id"] = serde_json::json!(step.id);
                    existing["task_step_error"] = serde_json::json!(error);
                    annotation = existing;
                }
            }
        }
        let blocked = (status == "parked").then(|| {
            serde_json::json!({"reason":error, "blocked_at":now, "source":"workflow_loop"})
                .to_string()
        });
        let written = sqlx::query("UPDATE task SET error_annotation=?,blocked_json=COALESCE(?,blocked_json),version=version+1,updated_at=? WHERE id=? AND version=? AND deleted_at IS NULL")
            .bind(annotation.to_string()).bind(&blocked).bind(&now).bind(&step.task_id).bind(task.version)
            .execute(&mut **tx).await?.rows_affected();
        if written != 1 {
            return Err(db::DbError::VersionConflict.into());
        }
        self.db
            .state_condition_in_tx(tx, &step.task_id, db::ConditionChange::Legacy)
            .await?;
        task.error_annotation = Some(annotation.to_string());
        if blocked.is_some() {
            task.blocked_json = blocked;
        }
        task.version += 1;
        task.updated_at = now;
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
        self.settle(step, "failed", Some(&message), true).await
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
const MAX_STEP_ATTEMPTS: i64 = 8;

/// Exponential back-off for the next attempt: 1 s doubling to 64 s.
fn retry_due(attempts: i64) -> String {
    let delay = 1_i64 << (attempts - 1).clamp(0, 6);
    (chrono::Utc::now() + chrono::Duration::seconds(delay)).to_rfc3339()
}

pub(crate) fn retryable(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Db(db::DbError::VersionConflict))
        || consumer_error_kind(error) == WorkerErrorKind::Transient
}

// The step uses the same actor as the former recursive hop.
pub(crate) fn cascade_actor() -> Actor {
    Actor::system(SystemComponent::Workflow)
}

#[cfg(test)]
mod tests;
