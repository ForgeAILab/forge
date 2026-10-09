//! The Task-step side of a check: how a step asks, and how its result is
//! applied exactly once by the step / attempt that asked.
//!
//! A consumer never reads a verdict from [`CheckRunner::request`]'s reply.
//! Hit, joined or scheduled, the result always arrives as one
//! `apply_check_result` Task step, and [`TaskCheckConsumers::apply`] is the
//! only door to the family's writer. The worker writes no Task state: every
//! Task, Review, condition or event write belongs to the family's `apply`.
use super::*;
use async_trait::async_trait;
use db::{
    CheckConsumerOrigin, CheckResultDelivery, CheckResultOutcome, CheckRunIdentity, CheckWorkerRepo,
};
use std::{collections::HashMap, sync::RwLock};

const REQUEST_KEY_SCHEMA: &str = "task-step/1";

/// What a Task step states when it asks for a check.
#[derive(Debug, Clone)]
pub struct TaskCheckRequest {
    pub task_id: String,
    /// The status entry the asking step runs under.
    pub status_epoch: i64,
    /// The step or attempt that asks, as its family names it (a review
    /// attempt, an integration attempt). Only this authority may apply the
    /// result; it is also what makes the request idempotent.
    pub authority: String,
    pub origin: CheckConsumerOrigin,
    pub purpose: api_types::CheckPurpose,
    pub identity: CheckRunIdentity,
    pub workspace_id: Option<String>,
    pub machine_id: Option<String>,
    pub wall_timeout_seconds: u64,
}
impl TaskCheckRequest {
    /// One consumer per (family, Task, status entry, authority).
    pub fn request_key(&self) -> String {
        format!(
            "{REQUEST_KEY_SCHEMA}|{}|{}|{}|{}",
            self.origin, self.task_id, self.status_epoch, self.authority
        )
    }
}
fn authority_of(consumer: &db::CheckConsumer) -> Option<&str> {
    let mut parts = consumer.request_key.splitn(5, '|');
    let schema = parts.next()?;
    let origin = parts.next()?;
    let task = parts.next()?;
    let epoch = parts.next()?;
    let authority = parts.next()?;
    (schema == REQUEST_KEY_SCHEMA
        && origin == consumer.origin.to_string()
        && Some(task) == consumer.task_id.as_deref()
        && epoch == consumer.status_epoch.to_string()
        && !authority.is_empty())
    .then_some(authority)
}

/// What the family applies. A red check is a result; exhausted
/// infrastructure retries are not a verdict on the candidate.
#[derive(Debug, Clone)]
pub enum CheckVerdict {
    /// Pass, fail, timed out or cancelled, produced by exactly the commit
    /// and spec the consumer asked for.
    Result(db::StoredCheckResult),
    /// No verdict could be produced after the automatic retries. The family
    /// parks its Task with a retryable reason; it must not record a failure
    /// of the candidate.
    InfrastructureExhausted(db::StoredCheckResult),
}
#[derive(Debug, Clone)]
pub struct CheckApplication {
    pub task_id: String,
    pub status_epoch: i64,
    pub authority: String,
    /// Stable across redelivery: the family's idempotency key.
    pub consumer_id: String,
    pub run: StoredCheckRun,
    pub verdict: CheckVerdict,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStaleReason {
    ConsumerCancelled,
    TaskEpoch,
    Authority,
}
/// `Stale` leaves the Task untouched: the step that asked is no longer the
/// one in charge, and whoever is asks again under its own authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "outcome", content = "reason", rename_all = "snake_case")]
pub enum CheckApplyOutcome {
    Applied,
    AlreadyApplied,
    Stale(CheckStaleReason),
}

/// One per consumer origin (review entry, merge path, ...). Called only from
/// the delivery's own Task step, under that step's lease.
#[async_trait]
pub trait CheckConsumerFamily: Send + Sync {
    /// The authority that may apply a check result to this Task right now,
    /// re-read from the family's own state (current attempt, candidate and
    /// check definition). `None` when nothing is waiting for a check.
    async fn current_authority(&self, task_id: &str, status_epoch: i64) -> Result<Option<String>>;
    /// Apply the verdict. A step can be redelivered after a crash, so this
    /// must be idempotent on `application.consumer_id`.
    async fn apply(&self, application: &CheckApplication) -> Result<()>;
}

pub struct TaskCheckConsumers {
    store: Arc<dyn CheckWorkerRepo>,
    runner: Arc<CheckRunner>,
    families: RwLock<HashMap<String, Arc<dyn CheckConsumerFamily>>>,
}
impl TaskCheckConsumers {
    pub fn new(store: Arc<dyn CheckWorkerRepo>, runner: Arc<CheckRunner>) -> Self {
        Self {
            store,
            runner,
            families: RwLock::default(),
        }
    }
    pub fn register(&self, origin: CheckConsumerOrigin, family: Arc<dyn CheckConsumerFamily>) {
        self.families
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(origin.to_string(), family);
    }
    fn family(&self, origin: CheckConsumerOrigin) -> Result<Arc<dyn CheckConsumerFamily>> {
        self.families
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&origin.to_string())
            .cloned()
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "no check consumer family is registered for {origin}"
                ))
            })
    }

    /// Ask for a check from inside the Task's step. Idempotent on
    /// (origin, Task, status entry, authority): asking again returns the
    /// same consumer and never a second run or a second delivery. The reply
    /// is information only; the verdict arrives as the delivery step.
    pub async fn request(&self, request: TaskCheckRequest) -> Result<RequestedCheck> {
        if request.authority.is_empty() || request.authority.contains('|') {
            return Err(ServiceError::invalid_operation(
                "check consumer authority must be a non-empty reference without '|'",
            ));
        }
        // A family nobody can deliver to would wait forever.
        self.family(request.origin)?;
        if self.store.live_task_epoch(&request.task_id).await? != Some(request.status_epoch) {
            return Err(ServiceError::invalid_operation(
                "the Task left the status entry that asks for this check",
            ));
        }
        let request_key = request.request_key();
        self.runner
            .request(db::CheckRunRequest {
                identity: request.identity,
                request_key,
                task_id: Some(request.task_id),
                status_epoch: request.status_epoch,
                origin: request.origin,
                purpose: request.purpose,
                workspace_id: request.workspace_id,
                machine_id: request.machine_id,
                wall_timeout_seconds: request.wall_timeout_seconds,
            })
            .await
    }

    /// The `apply_check_result` Task step. `step_id` is the claimed step:
    /// only the step recorded as the consumer's delivery may apply, once.
    /// An error means the envelope is not this consumer's delivery (the step
    /// fails visibly); `Stale` means it is, but its authority has passed.
    pub async fn apply(
        &self,
        step_id: &str,
        delivery: &CheckResultDelivery,
    ) -> Result<CheckApplyOutcome> {
        let refuse = |why: &str| ServiceError::invalid_operation(format!("check delivery {why}"));
        let state = self
            .store
            .check_consumer_delivery(&delivery.consumer_id)
            .await?
            .ok_or_else(|| refuse("names an unknown consumer"))?;
        let consumer = &state.consumer;
        if consumer.request_key != delivery.request_key
            || consumer.identity_key != delivery.identity_key
            || consumer.origin != delivery.origin
            || consumer.result_id.as_deref() != Some(&delivery.result_id)
            || consumer.run_id.as_deref() != Some(&delivery.run_id)
        {
            return Err(refuse("does not match its consumer"));
        }
        if state.delivery_step_id.as_deref() != Some(step_id) {
            return Err(refuse("is not carried by its consumer's delivery step"));
        }
        if state.applied_at.is_some() {
            return Ok(CheckApplyOutcome::AlreadyApplied);
        }
        if state.cancelled_at.is_some() {
            return Ok(CheckApplyOutcome::Stale(
                CheckStaleReason::ConsumerCancelled,
            ));
        }
        let run = self
            .store
            .check_run(&delivery.run_id)
            .await?
            .ok_or_else(|| refuse("names an unknown run"))?;
        let result = self
            .store
            .check_result(&delivery.result_id)
            .await?
            .ok_or_else(|| refuse("names an unknown result"))?;
        // The result must be the one produced for exactly this identity.
        if run.identity_key != consumer.identity_key
            || run.identity.commit_sha != delivery.commit_sha
            || run.spec_digest != delivery.spec_digest
            || result.run_id != run.id
            || result.identity_key != consumer.identity_key
            || !run.state.terminal()
        {
            return Err(refuse("carries a result for another identity"));
        }
        let task_id = consumer
            .task_id
            .clone()
            .ok_or_else(|| refuse("has no Task"))?;
        if state.live_task_epoch != Some(consumer.status_epoch) {
            return Ok(CheckApplyOutcome::Stale(CheckStaleReason::TaskEpoch));
        }
        let authority = authority_of(consumer)
            .ok_or_else(|| refuse("was not requested by a Task step"))?
            .to_owned();
        let family = self.family(consumer.origin)?;
        if family
            .current_authority(&task_id, consumer.status_epoch)
            .await?
            .as_deref()
            != Some(authority.as_str())
        {
            return Ok(CheckApplyOutcome::Stale(CheckStaleReason::Authority));
        }
        let verdict = if result.outcome == CheckResultOutcome::InfrastructureFailed {
            CheckVerdict::InfrastructureExhausted(result)
        } else {
            CheckVerdict::Result(result)
        };
        family
            .apply(&CheckApplication {
                task_id,
                status_epoch: consumer.status_epoch,
                authority,
                consumer_id: consumer.id.clone(),
                run,
                verdict,
            })
            .await?;
        self.store
            .mark_check_result_applied(&consumer.id, step_id, &db::now_rfc3339())
            .await?;
        Ok(CheckApplyOutcome::Applied)
    }
}
