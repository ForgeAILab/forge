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
const CONCURRENCY: usize = 8;
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
    pub workflow: api_types::WorkflowDefinition,
    pub authority: Option<WorkflowAuthority>,
    /// A new completed CI/review result is an execution-result chain boundary.
    pub evidence: Option<String>,
}

pub struct TaskStepWorker {
    engine: WorkflowEngine,
    db: Arc<db::SqliteDb>,
}
impl TaskStepWorker {
    pub fn new(engine: WorkflowEngine) -> Self {
        Self {
            db: Arc::clone(&engine.db),
            engine,
        }
    }
    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        WorkerSupervisor::new(
            WorkerHealth::new(Arc::clone(&self.db), "task_steps"),
            SupervisorPolicy::default(),
        )
        .start(move |shutdown| Arc::clone(&self).run(shutdown), shutdown)
    }
    async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let notify = self.db.domain_event_notify();
        let mut jobs = JoinSet::new();
        loop {
            if *shutdown.borrow_and_update() {
                return Ok(());
            }
            while jobs.len() < CONCURRENCY {
                let owner = db::new_uuid_v4();
                let Some(step) = self.db.claim_step(&owner, None, &lease_deadline()).await? else {
                    break;
                };
                let worker = Arc::clone(&self);
                jobs.spawn(async move { worker.execute(step).await });
            }
            tokio::select! {
                _ = super::supervisor::shutdown_signal(&mut shutdown) => return Ok(()),
                result = jobs.join_next(), if !jobs.is_empty() => {
                    match result {
                        Some(Ok(Ok(()))) => {},
                        Some(Ok(Err(error))) => tracing::warn!(%error, "task step settlement failed; lease will recover"),
                        Some(Err(error)) => tracing::warn!(%error, "task step panicked; lease will recover"),
                        None => {},
                    }
                }
                _ = notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }
    /// Deterministic test/service utility. Does not spawn background work and
    /// waits for existing owners/backoff exactly as the production worker does.
    pub async fn drain(&self, task_id: &str) -> Result<db::Task> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
        loop {
            if self.db.pending_steps(task_id).await? == 0
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
        let execution = self.execute_inner(&step);
        tokio::pin!(execution);
        let result = loop {
            tokio::select! {
                result = &mut execution => break result,
                _ = tokio::time::sleep(Duration::from_secs(15)) => {
                    if !self.db.renew_step(&step.id, owner, &lease_deadline()).await? {
                        return Err(db::DbError::VersionConflict.into());
                    }
                }
            }
        };
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
                let task = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
                if task.as_ref().is_none_or(|t| {
                    t.status != step.expected_status || t.version != step.expected_version
                }) {
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
        if task
            .as_ref()
            .is_none_or(|t| t.status != step.expected_status || t.version != step.expected_version)
        {
            return self
                .settle(
                    step,
                    "superseded",
                    Some("Task left the producing status/version"),
                    false,
                )
                .await;
        }
        let mut payload: CascadePayload =
            serde_json::from_str(&step.payload_json).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid cascade payload: {error}"))
            })?;
        if payload.authority.is_some() {
            let task = task.as_ref().expect("matching Task");
            let project = db::ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            // This is a new normal system transition entry, including the
            // inherited-subtask selection. Pin this entry's authority rather
            // than treating unrelated Project settings edits as Task moves.
            payload.workflow = WorkflowEngine::resolve_workflow_for_task(
                task,
                &project.workflow_definition,
                &cascade_actor(),
            );
            payload.authority = Some(WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
            });
        }
        let repeated = self
            .db
            .task_steps(&step.task_id)
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
        let result = self.engine.transition_step(step, &payload).await?;
        // These used to observe the final recursive result in TaskService's
        // wrapper. The queued terminal hop now owns that same settlement.
        if payload.workflow.state_kind(&result.task.status) == Some(api_types::StateKind::Terminal)
        {
            if payload.workflow.cancellation_state.as_deref() != Some(result.task.status.as_str()) {
                self.engine
                    .task_service
                    .wake_dependents_of_completed_task(&result.task)
                    .await?;
            }
            self.engine
                .task_service
                .reconcile_terminal_subtask(&result.task)
                .await;
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
        let mut snapshot = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let matches: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND status=? AND version=? AND deleted_at IS NULL)")
            .bind(&step.task_id).bind(&step.expected_status).bind(step.expected_version).fetch_one(&mut *tx).await?;
        let status = if annotate && !matches {
            "superseded"
        } else {
            status
        };
        self.db
            .finish_step_in_tx(&mut tx, step, status, error)
            .await?;
        if annotate && matches {
            self.annotate_in_tx(
                &mut tx,
                step,
                snapshot.as_mut().expect("matching Task"),
                status,
                error,
            )
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
    async fn annotate_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step: &TaskStep,
        snapshot: &mut db::Task,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let now = db::now_rfc3339();
        let mut annotation = serde_json::json!({"type":"dispatch_failed", "state":step.expected_status, "message":error, "detected_at":now, "task_step_id":step.id});
        if status != "parked" {
            if let Some(mut existing) = snapshot
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
        sqlx::query("UPDATE task SET error_annotation=?,blocked_json=COALESCE(?,blocked_json),version=version+1,updated_at=? WHERE id=? AND version=?")
                .bind(annotation.to_string()).bind(blocked).bind(&now).bind(&step.task_id).bind(step.expected_version).execute(&mut **tx).await?;
        snapshot.error_annotation = Some(annotation.to_string());
        if status == "parked" {
            snapshot.blocked_json = Some(
                serde_json::json!({"reason":error,"blocked_at":now,"source":"workflow_loop"})
                    .to_string(),
            );
        }
        snapshot.version += 1;
        snapshot.updated_at = now;
        let event = db::CreateDomainEvent::task_interruption_changed(snapshot);
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
        let mut snapshot = TaskRepo::get_by_id(&*self.db, &step.task_id, false).await?;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let latest: Option<String> = sqlx::query_scalar("SELECT id FROM transition_log WHERE task_id=? ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(&step.task_id).fetch_optional(&mut *tx).await?;
        let target = serde_json::from_str::<CascadePayload>(&step.payload_json)
            .ok()
            .map(|p| p.to);
        let own_entry = latest.as_deref() == Some(step.id.as_str())
            && snapshot
                .as_ref()
                .is_some_and(|t| Some(&t.status) == target.as_ref());
        // A later action can legitimately interrupt inline hooks. Keep its
        // successful predecessor done and retain the diagnostic, without
        // annotating the later Task entry. Never replay an applied CAS.
        let status = if own_entry { "failed" } else { "done" };
        let changed = sqlx::query("UPDATE task_step SET status=?,last_error=?,updated_at=? WHERE id=? AND status='done' AND claimed_by=? AND lease_until > ?")
            .bind(status).bind(&message).bind(db::now_rfc3339()).bind(&step.id).bind(&step.claimed_by).bind(db::now_rfc3339())
            .execute(&mut *tx).await?.rows_affected();
        if own_entry && changed == 1 {
            if let Some(task) = snapshot.as_mut() {
                let current: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND status=? AND version=? AND deleted_at IS NULL)")
                    .bind(&task.id).bind(&task.status).bind(task.version).fetch_one(&mut *tx).await?;
                if current {
                    let mut context = step.clone();
                    context.expected_status = task.status.clone();
                    context.expected_version = task.version;
                    self.annotate_in_tx(&mut tx, &context, task, "failed", Some(&message))
                        .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }
}
fn lease_deadline() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(LEASE_SECONDS)).to_rfc3339()
}
fn retryable(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::Db(
            db::DbError::VersionConflict
                | db::DbError::AgentAtCapacity
                | db::DbError::MachineAtCapacity
        )
    ) || consumer_error_kind(error) == WorkerErrorKind::Transient
}

// The step uses the same actor as the former recursive hop.
pub(crate) fn cascade_actor() -> Actor {
    Actor::system(SystemComponent::Workflow)
}

#[cfg(test)]
mod tests;
