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

tokio::task_local! { pub(crate) static PRODUCER_TASK: String; }
pub(crate) fn producer_deferred(task_id: &str) -> bool {
    PRODUCER_TASK.try_with(|id| id == task_id).unwrap_or(false)
}

/// Holds a pre-CAS enqueue through long inline producer hooks. Dropping a
/// cancelled producer stops renewal; its reservation expires crash-safely.
pub(crate) struct ProducerReservation(JoinHandle<()>);
impl ProducerReservation {
    pub(crate) fn hold(db: Arc<db::SqliteDb>, id: Option<&str>) -> Option<Self> {
        let id = id?.to_owned();
        Some(Self(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                match db.renew_step_reservation(&id, &lease_deadline()).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        tracing::warn!(%error, "task step producer reservation renewal failed")
                    }
                }
            }
        })))
    }
}
impl Drop for ProducerReservation {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CascadePayload {
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
    engine: WorkflowEngine,
    db: Arc<db::SqliteDb>,
    pub(crate) renew_interval: Duration,
}
impl TaskStepWorker {
    pub fn new(engine: WorkflowEngine) -> Self {
        Self {
            db: Arc::clone(&engine.db),
            engine,
            renew_interval: Duration::from_secs(15),
        }
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
        let owner = step.claimed_by.as_deref().expect("claimed step owner");
        // The status CAS marks done before inline hooks settle. Keep the lease
        // through that phase so another process cannot start this Task's next
        // step during its predecessor's hooks. All commits fence this token.
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
        let result = self.execute_inner(&step).await;
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
                } else if retryable(&error) && step.attempts < 8 {
                    let delay = 1_i64 << (step.attempts - 1).clamp(0, 6);
                    let due = (chrono::Utc::now() + chrono::Duration::seconds(delay)).to_rfc3339();
                    self.db.retry_step(&step, &error.to_string(), &due).await?;
                } else {
                    self.settle(&step, "failed", Some(&error.to_string()), true)
                        .await?;
                }
            } else if current
                .as_ref()
                .is_some_and(|s| s.status == "done" && s.claimed_by == step.claimed_by)
            {
                self.fail_committed_hook_phase(&step, &error.to_string())
                    .await?;
            } else {
                tracing::warn!(step_id = %step.id, %error, "task step attempt lost ownership or was already settled");
            }
        }
        self.db.release_step(&step.id, owner).await?;
        Ok(())
    }
    async fn execute_inner(&self, step: &TaskStep) -> Result<()> {
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
            if !WorkflowEngine::cascade_allowed(source, &payload.reason, rejection_to_target) {
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
                prior.chain_id == step.chain_id
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
            .transition_step(step, &payload, &workflow, authority)
            .await?;
        // These used to observe the final recursive result in TaskService's
        // wrapper. The queued terminal hop now owns that same settlement.
        if workflow.state_kind(&result.task.status) == Some(api_types::StateKind::Terminal) {
            if workflow.cancellation_state.as_deref() != Some(result.task.status.as_str()) {
                if let Err(error) = self
                    .engine
                    .task_service
                    .wake_dependents_of_completed_task(&result.task)
                    .await
                {
                    tracing::warn!(task_id=%result.task.id, %error, "failed to wake dependents after committed cascade");
                }
            }
            self.engine
                .task_service
                .reconcile_terminal_subtask(&result.task)
                .await;
        }
        if workflow.state_kind(&result.task.status) == Some(api_types::StateKind::Initial) {
            if let Err(error) = self
                .engine
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
        let now = db::now_rfc3339();
        let mut annotation = serde_json::json!({"type":if status=="parked" {"workflow_loop"} else {"cascade_failed"}, "state":step.expected_status, "message":error, "detected_at":now, "task_step_id":step.id});
        if status != "parked" {
            if let Some(mut existing) = task
                .error_annotation
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            {
                if existing["type"]
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
        let message = format!("Transition committed; inline hook failed: {error}");
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let mut task = self.db.get_task_in_tx(&mut tx, &step.task_id).await?;
        // The step's own CAS wrote the log row with the step's id.
        let own_epoch: Option<i64> =
            sqlx::query_scalar("SELECT status_epoch FROM transition_log WHERE id=? AND task_id=?")
                .bind(&step.id)
                .bind(&step.task_id)
                .fetch_optional(&mut *tx)
                .await?
                .flatten();
        let current_epoch: Option<i64> =
            sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                .bind(&step.task_id)
                .fetch_optional(&mut *tx)
                .await?;
        let target = serde_json::from_str::<CascadePayload>(&step.payload_json)
            .ok()
            .map(|p| p.to);
        let own_entry = own_epoch.is_some()
            && own_epoch == current_epoch
            && task
                .as_ref()
                .is_some_and(|t| t.deleted_at.is_none() && Some(&t.status) == target.as_ref());
        // A later action can legitimately interrupt inline hooks. Keep its
        // successful predecessor done and retain the diagnostic, without
        // annotating the later Task entry. Never replay an applied CAS.
        let status = if own_entry { "failed" } else { "done" };
        // Same ownership rule as finish_step_in_tx: a live local hook owns
        // its step after a sleep expired the wall-clock lease.
        let now = db::now_rfc3339();
        let changed = sqlx::query("UPDATE task_step SET status=?,last_error=?,updated_at=? WHERE id=? AND status='done' AND claimed_by=? AND (lease_until > ? OR ?)")
            .bind(status).bind(&message).bind(&now).bind(&step.id).bind(&step.claimed_by).bind(&now).bind(self.db.step_is_active(step))
            .execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            tracing::warn!(step_id=%step.id, %error, "task step lost ownership before its hook failure was recorded");
        }
        if own_entry && changed == 1 {
            if let Some(task) = task.as_mut() {
                let mut context = step.clone();
                context.expected_status = task.status.clone();
                context.expected_version = task.version;
                self.annotate_in_tx(&mut tx, &context, task, "failed", Some(&message))
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(())
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
fn retryable(error: &ServiceError) -> bool {
    matches!(error, ServiceError::Db(db::DbError::VersionConflict))
        || consumer_error_kind(error) == WorkerErrorKind::Transient
}

// The step uses the same actor as the former recursive hop.
pub(crate) fn cascade_actor() -> Actor {
    Actor::system(SystemComponent::Workflow)
}

#[cfg(test)]
mod tests;
