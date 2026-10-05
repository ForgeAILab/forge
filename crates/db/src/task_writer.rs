//! Shared lease context and bounded synchronous execution of durable writes.
use crate::{
    DbError, EnqueueTaskStep, Result, SqliteDb, TaskMutation, TaskRepo, TaskStep, TaskStepRepo,
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

#[derive(Debug)]
pub struct TaskStepControl {
    pub preempt: tokio::sync::watch::Sender<bool>,
    pub critical: AtomicBool,
}
impl TaskStepControl {
    pub fn new() -> Arc<Self> {
        let (preempt, _) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            preempt,
            critical: AtomicBool::new(false),
        })
    }
}

tokio::task_local! { static CURRENT_STEP: TaskStep; }
pub fn current_task_step() -> Option<TaskStep> {
    CURRENT_STEP.try_with(Clone::clone).ok()
}
pub fn owns_task(task_id: &str) -> bool {
    CURRENT_STEP
        .try_with(|step| step.task_id == task_id)
        .unwrap_or(false)
}
pub async fn in_task_step<T>(step: TaskStep, future: impl std::future::Future<Output = T>) -> T {
    CURRENT_STEP.scope(step, future).await
}

/// Central guard for in-transaction helpers that write a Task's workflow
/// state directly. Under single-writer they must run in that Task's step.
#[track_caller]
pub fn debug_assert_task_lease(task_id: &str, writer: &str) {
    debug_assert!(
        owns_task(task_id),
        "{writer} wrote Task {task_id} workflow state outside its step lease"
    );
}

/// How a queued per-Task effect is fenced when it finally runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EffectFence {
    /// Applies only while the Task is still in the status entry it was queued
    /// in; a preempting Cancel/Hold supersedes it. For effects that are moot
    /// once the Task has moved on (status writes, entry barriers, deferrals
    /// for one status entry).
    #[default]
    Entry,
    /// Fenced by its own identity (its SQL predicate or idempotent content):
    /// it applies after a status change and survives a preempting Cancel/Hold.
    /// For effects that must never be lost (wakes, clears of its own marker,
    /// blocking annotations, dependency blocks, role clears).
    Identity,
}

#[async_trait]
pub trait TaskStepExecutor: Send + Sync {
    /// Start an eligible fast head through the normal worker code path.
    /// The worker owns the future even if the waiting request is dropped.
    async fn drive_inline(&self, task_id: &str) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
pub struct TaskMutationReply {
    pub result: Option<Value>,
    pub error: Option<Value>,
}
impl TaskMutationReply {
    fn from_result(result: &Result<Value>) -> Self {
        match result {
            Ok(value) => Self {
                result: Some(value.clone()),
                error: None,
            },
            Err(error) => Self {
                result: None,
                error: Some(match error {
                    DbError::TaskBusy {
                        pending_steps,
                        retry_after_ms,
                    } => {
                        serde_json::json!({"code":"task_busy","pending_steps":pending_steps,"retry_after_ms":retry_after_ms})
                    }
                    DbError::BoardRevisionConflict { expected, actual } => {
                        serde_json::json!({"code":"board_revision_conflict","expected":expected,"actual":actual})
                    }
                    DbError::MoveOperationConflict { operation_id } => {
                        serde_json::json!({"code":"move_operation_conflict","operation_id":operation_id})
                    }
                    DbError::MoveOperationIncomplete { operation_id } => {
                        serde_json::json!({"code":"move_operation_incomplete","operation_id":operation_id})
                    }
                    DbError::InvalidTaskMove(message) => {
                        serde_json::json!({"code":"invalid_task_move","message":message})
                    }
                    DbError::InvalidSoftDelete => serde_json::json!({"code":"invalid_soft_delete"}),
                    DbError::ReviewDetailsCorrupt { review_id, reason } => {
                        serde_json::json!({"code":"review_details_corrupt","review_id":review_id,"reason":reason})
                    }
                    DbError::TransitionBridgeCorrupt {
                        transition_log_id,
                        reason,
                    } => {
                        serde_json::json!({"code":"transition_bridge_corrupt","transition_log_id":transition_log_id,"reason":reason})
                    }
                    DbError::RepoInUse { repo_id } => {
                        serde_json::json!({"code":"repo_in_use","repo_id":repo_id})
                    }
                    DbError::ResourceInUse { resource, reason } => {
                        serde_json::json!({"code":"resource_in_use","resource":resource,"reason":reason})
                    }
                    DbError::ProjectInUse {
                        project_id,
                        running_executions,
                        active_leases,
                    } => {
                        serde_json::json!({"code":"project_in_use","project_id":project_id,"running_executions":running_executions,"active_leases":active_leases})
                    }
                    DbError::TurnNotRetryable => serde_json::json!({"code":"turn_not_retryable"}),
                    DbError::ChatTurnLive => serde_json::json!({"code":"chat_turn_live"}),
                    DbError::DeadLetterNotReplayable => {
                        serde_json::json!({"code":"dead_letter_not_replayable"})
                    }
                    DbError::InvalidCursor => serde_json::json!({"code":"invalid_cursor"}),
                    DbError::Sqlx(source) => {
                        serde_json::json!({"code":"database_error","message":source.to_string(),"transient":error.is_transient()})
                    }
                    DbError::VersionConflict => serde_json::json!({"code":"version_conflict"}),
                    DbError::NotFound => serde_json::json!({"code":"not_found"}),
                    DbError::TaskVersionConflict { expected, actual } => {
                        serde_json::json!({"code":"task_version_conflict","expected":expected,"actual":actual})
                    }
                    DbError::AgentPaused { agent_id } => {
                        serde_json::json!({"code":"agent_paused","agent_id":agent_id})
                    }
                    DbError::ProjectPaused { project_id } => {
                        serde_json::json!({"code":"project_paused","project_id":project_id})
                    }
                    DbError::InvalidTransition => serde_json::json!({"code":"invalid_transition"}),
                    DbError::IdempotencyConflict => {
                        serde_json::json!({"code":"idempotency_conflict"})
                    }
                    DbError::DependencyGate => serde_json::json!({"code":"dependency_gate"}),
                    DbError::CycleDetected => serde_json::json!({"code":"cycle_detected"}),
                    DbError::Check(message) => {
                        serde_json::json!({"code":"check","message":message})
                    }
                    DbError::AgentAtCapacity => serde_json::json!({"code":"agent_at_capacity"}),
                    DbError::MachineAtCapacity => serde_json::json!({"code":"machine_at_capacity"}),
                    DbError::ExecutionAlreadyRunning {
                        scope,
                        execution_id,
                    } => {
                        serde_json::json!({"code":"execution_already_running","scope":scope,"execution_id":execution_id})
                    }
                    _ => {
                        serde_json::json!({"code":"database_error","message":error.to_string(),"transient":false})
                    }
                }),
            },
        }
    }
    pub fn decode<T: DeserializeOwned>(self) -> Result<T> {
        if let Some(error) = self.error {
            let value = |key: &str| error[key].as_str().unwrap_or_default().to_owned();
            return Err(match error["code"].as_str() {
                Some("task_busy") => DbError::TaskBusy {
                    pending_steps: error["pending_steps"].as_i64().unwrap_or_default(),
                    retry_after_ms: error["retry_after_ms"].as_u64().unwrap_or_default(),
                },
                Some("board_revision_conflict") => DbError::BoardRevisionConflict {
                    expected: error["expected"].as_i64().unwrap_or_default(),
                    actual: error["actual"].as_i64().unwrap_or_default(),
                },
                Some("move_operation_conflict") => DbError::MoveOperationConflict {
                    operation_id: value("operation_id"),
                },
                Some("move_operation_incomplete") => DbError::MoveOperationIncomplete {
                    operation_id: value("operation_id"),
                },
                Some("invalid_task_move") => DbError::InvalidTaskMove(value("message")),
                Some("invalid_soft_delete") => DbError::InvalidSoftDelete,
                Some("review_details_corrupt") => DbError::ReviewDetailsCorrupt {
                    review_id: value("review_id"),
                    reason: value("reason"),
                },
                Some("transition_bridge_corrupt") => DbError::TransitionBridgeCorrupt {
                    transition_log_id: value("transition_log_id"),
                    reason: value("reason"),
                },
                Some("repo_in_use") => DbError::RepoInUse {
                    repo_id: value("repo_id"),
                },
                Some("resource_in_use") => DbError::ResourceInUse {
                    resource: value("resource"),
                    reason: value("reason"),
                },
                Some("project_in_use") => DbError::ProjectInUse {
                    project_id: value("project_id"),
                    running_executions: error["running_executions"].as_i64().unwrap_or_default(),
                    active_leases: error["active_leases"].as_i64().unwrap_or_default(),
                },
                Some("turn_not_retryable") => DbError::TurnNotRetryable,
                Some("chat_turn_live") => DbError::ChatTurnLive,
                Some("dead_letter_not_replayable") => DbError::DeadLetterNotReplayable,
                Some("invalid_cursor") => DbError::InvalidCursor,
                Some("database_error") => DbError::Sqlx(if error["transient"] == true {
                    sqlx::Error::Io(std::io::Error::other(value("message")))
                } else {
                    sqlx::Error::Protocol(value("message"))
                }),
                Some("version_conflict") => DbError::VersionConflict,
                Some("not_found") => DbError::NotFound,
                Some("task_version_conflict") => DbError::TaskVersionConflict {
                    expected: error["expected"].as_i64().unwrap_or_default(),
                    actual: error["actual"].as_i64().unwrap_or_default(),
                },
                Some("agent_paused") => DbError::AgentPaused {
                    agent_id: value("agent_id"),
                },
                Some("project_paused") => DbError::ProjectPaused {
                    project_id: value("project_id"),
                },
                Some("invalid_transition") => DbError::InvalidTransition,
                Some("idempotency_conflict") => DbError::IdempotencyConflict,
                Some("dependency_gate") => DbError::DependencyGate,
                Some("cycle_detected") => DbError::CycleDetected,
                Some("agent_at_capacity") => DbError::AgentAtCapacity,
                Some("machine_at_capacity") => DbError::MachineAtCapacity,
                Some("execution_already_running") => DbError::ExecutionAlreadyRunning {
                    scope: value("scope"),
                    execution_id: value("execution_id"),
                },
                _ => DbError::Check(value("message")),
            });
        }
        serde_json::from_value(self.result.unwrap_or(Value::Null))
            .map_err(|e| DbError::Check(e.to_string()))
    }
}

pub fn lease_deadline() -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339()
}

impl SqliteDb {
    pub async fn record_mutation_reply_in_tx<T: Serialize>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        result: &T,
    ) -> Result<()> {
        let Some(step) = current_task_step().filter(|step| step.kind == "mutation") else {
            return Ok(());
        };
        let mut value = serde_json::to_value(result).map_err(|e| DbError::Check(e.to_string()))?;
        let mutation: Value =
            serde_json::from_str(&step.payload_json).map_err(|e| DbError::Check(e.to_string()))?;
        if mutation.get("TaskMutateMetadata").is_some() && value.is_array() {
            value = value[0].clone();
        }
        if mutation.get("TaskUpdateIfAnnotation").is_some() {
            value = Value::Null;
        }
        let reply = TaskMutationReply {
            result: Some(value),
            error: None,
        };
        self.fence_hook_in_tx(tx, &step).await?;
        sqlx::query(
            "UPDATE task_step SET result_json=? WHERE id=? AND claimed_by=? AND status='claimed'",
        )
        .bind(serde_json::to_string(&reply).map_err(|e| DbError::Check(e.to_string()))?)
        .bind(&step.id)
        .bind(&step.claimed_by)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    /// Fence for in-transaction helpers that write `task_id`'s workflow
    /// state (status, blocking annotations, recovery metadata). Their public
    /// wrappers route callers without the lease through a mutation step, so
    /// the helper itself must only ever run in that Task's step.
    pub async fn fence_task_lease_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        writer: &str,
    ) -> Result<()> {
        debug_assert_task_lease(task_id, writer);
        self.fence_current_step_in_tx(tx).await
    }
    pub async fn fence_current_step_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<()> {
        if let Some(step) = current_task_step() {
            self.fence_hook_in_tx(tx, &step).await?;
        }
        Ok(())
    }
    pub async fn enqueue_task_mutation_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        mutation: TaskMutation,
    ) -> Result<String> {
        self.enqueue_fenced_task_mutation_in_tx(tx, task_id, mutation, EffectFence::Entry)
            .await
    }
    pub async fn enqueue_fenced_task_mutation_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task_id: &str,
        mutation: TaskMutation,
        fence: EffectFence,
    ) -> Result<String> {
        let task = TaskRepo::get_by_id_in_tx(self, tx, task_id, false)
            .await?
            .ok_or(DbError::NotFound)?;
        let id = crate::new_uuid_v4();
        self.enqueue_step_in_tx(
            tx,
            &EnqueueTaskStep {
                id: id.clone(),
                task_id: task_id.to_owned(),
                kind: "mutation".to_owned(),
                payload_json: serde_json::to_string(&mutation)
                    .map_err(|e| DbError::Check(e.to_string()))?,
                causation_step_id: current_task_step().map(|step| step.id),
                causation_key: id.clone(),
                chain_id: id.clone(),
                chain_position: 1,
                expected_status: task.status,
                expected_version: task.version,
                expected_epoch: None,
                lane: "fast".to_owned(),
                available_at: crate::now_rfc3339(),
            },
        )
        .await?;
        if fence == EffectFence::Identity {
            self.mark_step_identity_fenced_in_tx(tx, &id).await?;
        }
        Ok(id)
    }
    pub async fn mark_step_identity_fenced_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        step_id: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE task_step SET entry_fenced=0 WHERE id=? AND status='pending'")
            .bind(step_id)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
    pub async fn apply_task_sql(
        &self,
        task_id: &str,
        query: &str,
        mut arguments: Vec<Value>,
    ) -> Result<u64> {
        if !owns_task(task_id) {
            return Err(DbError::Check(
                "Task SQL requires its claimed step".to_owned(),
            ));
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.fence_current_step_in_tx(&mut tx).await?;
        // In-lease version override. Private Task effects already passed
        // their status/epoch fence (or are identity-fenced), and the lease
        // makes this step the Task's only workflow writer. A content edit or
        // an earlier fast effect may have advanced version, so every other
        // SQL predicate is kept and `AND version = ?` is rebound to the
        // version read in this same transaction. This deliberately disables
        // the version guard: an in-lease read-modify-write that spans an
        // `.await` must not rely on it to detect a concurrent writer. Only a
        // non-step writer (content edits) can interleave, and only with
        // content fields.
        let upper = query.to_ascii_uppercase();
        if let Some(position) = upper.find("AND VERSION = ?") {
            let parameter = query[..position]
                .bytes()
                .filter(|byte| *byte == b'?')
                .count();
            let version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(&mut *tx)
                .await?;
            let argument = arguments
                .get_mut(parameter)
                .ok_or_else(|| DbError::Check("Task version parameter missing".to_owned()))?;
            *argument = serde_json::json!(version);
        }
        let result = bind_query(query, arguments)?
            .execute(&mut *tx)
            .await?
            .rows_affected();
        self.record_mutation_reply_in_tx(&mut tx, &result).await?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn protect_step_integration(&self) -> Result<()> {
        if let Some(step) = current_task_step() {
            sqlx::query("UPDATE task_step SET integration_started_at=COALESCE(integration_started_at,?) WHERE id=? AND status='claimed' AND claimed_by=?")
                .bind(crate::now_rfc3339()).bind(&step.id).bind(&step.claimed_by).execute(self.pool()).await?;
            if let Some(control) = self
                .task_step_controls
                .lock()
                .expect("step controls")
                .get(&step.id)
            {
                control.critical.store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }
    pub async fn request_task_preemption(&self, task_id: &str) -> Result<()> {
        sqlx::query("UPDATE task_step SET preempt_requested_at=? WHERE task_id=? AND status='claimed' AND kind IN ('hooks','command') AND priority=0")
            .bind(crate::now_rfc3339()).bind(task_id).execute(self.pool()).await?;
        let steps = self.task_steps(task_id).await?;
        let controls = self.task_step_controls.lock().expect("step controls");
        for step in steps.iter().filter(|step| {
            step.status == "claimed"
                && !serde_json::from_str::<Value>(&step.payload_json)
                    .ok()
                    .is_some_and(|p| p["preempt"] == true)
        }) {
            if let Some(control) = controls.get(&step.id) {
                control.preempt.send_replace(true);
            }
        }
        self.domain_event_notify().notify_waiters();
        Ok(())
    }
    pub fn set_task_step_executor(&self, executor: std::sync::Weak<dyn TaskStepExecutor>) {
        *self.task_step_executor.lock().expect("step executor") = Some(executor);
    }
    pub async fn enqueue_task_mutation(
        &self,
        task_id: &str,
        mutation: TaskMutation,
    ) -> Result<String> {
        self.enqueue_fenced_task_mutation(task_id, mutation, EffectFence::Entry)
            .await
    }
    pub async fn enqueue_fenced_task_mutation(
        &self,
        task_id: &str,
        mutation: TaskMutation,
        fence: EffectFence,
    ) -> Result<String> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let id = self
            .enqueue_fenced_task_mutation_in_tx(&mut tx, task_id, mutation, fence)
            .await?;
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(id)
    }
    pub async fn run_task_mutation<T: DeserializeOwned>(
        &self,
        task_id: &str,
        mutation: TaskMutation,
    ) -> Result<T> {
        let task = TaskRepo::get_by_id(self, task_id, false)
            .await?
            .ok_or(DbError::NotFound)?;
        if mutation
            .expected_task_version()
            .is_some_and(|expected| expected != task.version)
        {
            return Err(DbError::VersionConflict);
        }
        let id = crate::new_uuid_v4();
        self.enqueue_step(&EnqueueTaskStep {
            id: id.clone(),
            task_id: task_id.to_owned(),
            kind: "mutation".to_owned(),
            payload_json: serde_json::to_string(&mutation)
                .map_err(|e| DbError::Check(e.to_string()))?,
            causation_step_id: current_task_step().map(|step| step.id),
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: task.status,
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".to_owned(),
            available_at: crate::now_rfc3339(),
        })
        .await?;
        self.wait_task_mutation(task_id, &id).await
    }

    async fn wait_task_mutation<T: DeserializeOwned>(&self, task_id: &str, id: &str) -> Result<T> {
        let notify = self.domain_event_notify();
        let wait = async {
            loop {
                let changed = notify.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let step = self
                    .task_steps(task_id)
                    .await?
                    .into_iter()
                    .find(|s| s.id == id)
                    .ok_or(DbError::NotFound)?;
                if let Some(reply) = step.result_json.filter(|_| {
                    (step.status == "done" || step.status == "failed") && step.lease_until.is_none()
                }) {
                    let reply: TaskMutationReply =
                        serde_json::from_str(&reply).map_err(|e| DbError::Check(e.to_string()))?;
                    return reply.decode();
                }
                if !matches!(
                    step.status.as_str(),
                    "pending" | "claimed" | "done" | "failed"
                ) {
                    return Err(DbError::VersionConflict);
                }
                let executor = self
                    .task_step_executor
                    .lock()
                    .expect("step executor")
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade);
                if let Some(executor) = executor {
                    executor.drive_inline(task_id).await?;
                } else {
                    // Repository-only users can execute persistence heads.
                    // An engine head remains owned by the installed worker.
                    self.drive_mutation_head(task_id).await?;
                }
                changed.await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(5), wait).await {
            Ok(result) => result,
            Err(_) => Err(DbError::TaskBusy {
                pending_steps: self.pending_steps(task_id).await?,
                retry_after_ms: 250,
            }),
        }
    }

    async fn drive_mutation_head(&self, task_id: &str) -> Result<()> {
        let head = self
            .task_steps(task_id)
            .await?
            .into_iter()
            .find(|s| matches!(s.status.as_str(), "pending" | "claimed"));
        if head.is_none_or(|s| s.kind != "mutation") {
            return Ok(());
        }
        if let Some(step) = self
            .claim_step(&crate::new_uuid_v4(), Some(task_id), &lease_deadline())
            .await?
        {
            let db = self.clone();
            let activity = db.hold_task_step(&step);
            tokio::spawn(async move {
                let _activity = activity;
                let owned = step.clone();
                let result = in_task_step(step, db.execute_task_mutation(&owned)).await;
                if let Err(error) = result {
                    tracing::warn!(%error,"inline Task persistence failed");
                }
                let _ = db
                    .release_step(&owned.id, owned.claimed_by.as_deref().unwrap_or_default())
                    .await;
            });
        }
        Ok(())
    }

    pub async fn execute_task_mutation(&self, step: &TaskStep) -> Result<()> {
        let mutation: TaskMutation =
            serde_json::from_str(&step.payload_json).map_err(|e| DbError::Check(e.to_string()))?;
        let result = if let Some(reply) = &step.result_json {
            serde_json::from_str::<TaskMutationReply>(reply)
                .map_err(|e| DbError::Check(e.to_string()))?
                .decode::<Value>()
        } else if !step.entry_fenced || self.step_entry_matches(step).await? {
            mutation.apply(self).await
        } else {
            Err(DbError::VersionConflict)
        };
        let reply = TaskMutationReply::from_result(&result);
        let mut tx = crate::begin_immediate(self.pool()).await?;
        self.finish_step_in_tx(
            &mut tx,
            step,
            if result.is_ok() { "done" } else { "failed" },
            result.as_ref().err().map(ToString::to_string).as_deref(),
        )
        .await?;
        sqlx::query("UPDATE task_step SET result_json=? WHERE id=? AND claimed_by=?")
            .bind(serde_json::to_string(&reply).map_err(|e| DbError::Check(e.to_string()))?)
            .bind(&step.id)
            .bind(&step.claimed_by)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(())
    }
}

fn bind_query(
    query: &str,
    arguments: Vec<Value>,
) -> Result<sqlx::query::Query<'_, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'_>>> {
    let mut query = sqlx::query(query);
    for value in arguments {
        query = match value {
            Value::Null => query.bind(Option::<String>::None),
            Value::Bool(value) => query.bind(value),
            Value::Number(value) if value.is_i64() => query.bind(value.as_i64().unwrap()),
            Value::Number(value) => query.bind(
                value
                    .as_f64()
                    .ok_or_else(|| DbError::Check("invalid Task SQL number".to_owned()))?,
            ),
            Value::String(value) => query.bind(value),
            value => query.bind(value.to_string()),
        };
    }
    Ok(query)
}

pub struct TaskQuery {
    db: SqliteDb,
    task_id: String,
    query: String,
    arguments: Vec<Value>,
    fence: EffectFence,
}
/// Outcome of a Task write. `Queued` is never "applied": the write became its
/// own step on the Task's queue and may still fail its fence there.
#[must_use = "a Task write may only have been queued; handle TaskQueryResult::Queued"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskQueryResult {
    /// Written under the caller's lease; the affected row count.
    Applied(u64),
    /// Enqueued in the caller's transaction as the Task's step `step_id`.
    Queued { step_id: String },
}
impl TaskQueryResult {
    /// Rows written under the lease; `None` when the write was only queued.
    pub fn applied(&self) -> Option<u64> {
        match self {
            Self::Applied(rows) => Some(*rows),
            Self::Queued { .. } => None,
        }
    }
    /// For writers that must hold the Task lease: a queued write is an error,
    /// never success. The error rolls back the caller's transaction, and with
    /// it the queued step.
    pub fn require_applied(self) -> Result<u64> {
        match self {
            Self::Applied(rows) => Ok(rows),
            Self::Queued { .. } => Err(DbError::Check(
                "Task write was queued, not applied: the writer does not hold the Task lease"
                    .to_owned(),
            )),
        }
    }
}
impl TaskQuery {
    pub fn new(db: &SqliteDb, task_id: &str, query: impl Into<String>) -> Self {
        Self {
            db: db.clone(),
            task_id: task_id.to_owned(),
            query: query.into(),
            arguments: Vec::new(),
            fence: EffectFence::Entry,
        }
    }
    pub fn bind<T: Serialize>(mut self, value: T) -> Self {
        self.arguments
            .push(serde_json::to_value(value).expect("Task SQL parameter serializes"));
        self
    }
    /// When queued, fence this effect by its own SQL predicate rather than
    /// the producing status entry (see [`EffectFence::Identity`]).
    pub fn identity_fenced(mut self) -> Self {
        self.fence = EffectFence::Identity;
        self
    }
    /// Applies the write and returns the affected row count: inline under
    /// the caller's lease, or as a mutation step that this call waits for
    /// (bounded; `task_busy` on timeout, `version_conflict` when fenced).
    pub async fn execute(self, _pool: &sqlx::SqlitePool) -> Result<u64> {
        let result = if owns_task(&self.task_id) {
            self.db
                .apply_task_sql(&self.task_id, &self.query, self.arguments)
                .await?
        } else {
            self.db
                .run_task_mutation(
                    &self.task_id,
                    TaskMutation::Sql {
                        task_id: self.task_id.clone(),
                        query: self.query,
                        arguments: self.arguments,
                    },
                )
                .await?
        };
        Ok(result)
    }
    /// Applies the write in `tx` under the caller's lease, or enqueues it in
    /// `tx` as the Task's own step and returns `Queued`.
    pub async fn execute_in_tx(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<TaskQueryResult> {
        if owns_task(&self.task_id) {
            self.db.fence_current_step_in_tx(tx).await?;
            return Ok(TaskQueryResult::Applied(
                bind_query(&self.query, self.arguments)?
                    .execute(&mut **tx)
                    .await?
                    .rows_affected(),
            ));
        }
        let step_id = self
            .db
            .enqueue_fenced_task_mutation_in_tx(
                tx,
                &self.task_id,
                TaskMutation::Sql {
                    task_id: self.task_id.clone(),
                    query: self.query,
                    arguments: self.arguments,
                },
                self.fence,
            )
            .await?;
        Ok(TaskQueryResult::Queued { step_id })
    }
}

/// Per-Task outcome counts of a bulk authority change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BulkTaskQueryResult {
    /// Tasks written under the caller's own lease.
    pub applied: u64,
    /// Tasks that received the effect as their own queued step.
    pub queued: u64,
}
impl BulkTaskQueryResult {
    pub fn tasks(&self) -> u64 {
        self.applied + self.queued
    }
}

/// Split a bulk authority change into one durable effect per affected Task,
/// in the authoritative transaction, without acquiring another Task lease.
/// Each queued effect is identity-fenced by the bulk predicate, so a wake or
/// clear is never dropped because the Task changed status first.
pub struct BulkTaskQuery {
    db: SqliteDb,
    query: String,
    arguments: Vec<Value>,
}
impl BulkTaskQuery {
    pub fn new(db: &SqliteDb, query: impl Into<String>) -> Self {
        Self {
            db: db.clone(),
            query: query.into(),
            arguments: Vec::new(),
        }
    }
    pub fn bind<T: Serialize>(mut self, value: T) -> Self {
        self.arguments
            .push(serde_json::to_value(value).expect("Task SQL parameter serializes"));
        self
    }
    pub async fn execute_in_tx(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<BulkTaskQueryResult> {
        let upper = self.query.to_ascii_uppercase();
        let at = upper
            .find("WHERE ")
            .ok_or_else(|| DbError::Check("bulk Task effect requires a predicate".to_owned()))?;
        let set_arguments = self.query[..at].bytes().filter(|b| *b == b'?').count();
        let predicate = &self.query[at + 6..];
        let select = format!("SELECT id FROM task WHERE deleted_at IS NULL AND ({predicate})");
        let ids = bind_query(&select, self.arguments[set_arguments..].to_vec())?
            .fetch_all(&mut **tx)
            .await?;
        use sqlx::Row;
        let mut result = BulkTaskQueryResult::default();
        for row in ids {
            let task_id: String = row.get("id");
            let query = format!("{}WHERE id=? AND ({predicate})", &self.query[..at]);
            let mut arguments = self.arguments.clone();
            arguments.insert(set_arguments, serde_json::json!(task_id));
            if owns_task(&task_id) {
                self.db.fence_current_step_in_tx(tx).await?;
                bind_query(&query, arguments)?.execute(&mut **tx).await?;
                result.applied += 1;
            } else {
                self.db
                    .enqueue_fenced_task_mutation_in_tx(
                        tx,
                        &task_id,
                        TaskMutation::Sql {
                            task_id: task_id.clone(),
                            query,
                            arguments,
                        },
                        EffectFence::Identity,
                    )
                    .await?;
                result.queued += 1;
            }
        }
        Ok(result)
    }
}

/// Preserve the two independent Option bits in command payloads.
pub mod nested_option {
    use super::*;
    #[derive(Serialize, Deserialize)]
    struct Change<T> {
        present: bool,
        value: Option<T>,
    }
    pub fn serialize<T: Serialize, S: serde::Serializer>(
        value: &Option<Option<T>>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Change {
            present: value.is_some(),
            value: value.as_ref().and_then(Option::as_ref),
        }
        .serialize(serializer)
    }
    pub fn deserialize<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<Option<T>>, D::Error> {
        let change = Change::<T>::deserialize(deserializer)?;
        Ok(change.present.then_some(change.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture() -> SqliteDb {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let now = crate::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','todo',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        db
    }
    #[tokio::test]
    async fn nullable_command_fields_preserve_clear_and_unchanged() {
        let db = fixture().await;
        let mut task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        task = TaskRepo::update(
            &db,
            crate::UpdateTask {
                id: "t".into(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some("blocked".into())),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: crate::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let clear = crate::UpdateTask {
            id: "t".into(),
            expected_version: task.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: crate::now_rfc3339(),
        };
        let round: crate::UpdateTask =
            serde_json::from_str(&serde_json::to_string(&clear).unwrap()).unwrap();
        assert_eq!(round.error_annotation, Some(None));
        assert_eq!(round.blocked_json, None);
        let task = TaskRepo::update(&db, round).await.unwrap();
        assert_eq!(task.error_annotation, None);
        assert!(db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .all(|step| step.kind == "mutation" && step.status == "done"));
    }
    #[tokio::test]
    async fn busy_wait_retains_the_accepted_mutation() {
        let db = fixture().await;
        let blocker = EnqueueTaskStep {
            id: crate::new_uuid_v4(),
            task_id: "t".into(),
            kind: "cascade".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: "block".into(),
            chain_id: "block".into(),
            chain_position: 1,
            expected_status: "todo".into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: crate::now_rfc3339(),
        };
        db.enqueue_step(&blocker).await.unwrap();
        let result =
            TaskRepo::set_entry_barrier(&db, "t", 1, Some("blocked".into()), &crate::now_rfc3339())
                .await;
        assert!(matches!(
            result,
            Err(DbError::TaskBusy {
                pending_steps: 2,
                ..
            })
        ));
        let head = db
            .claim_step("owner", Some("t"), &lease_deadline())
            .await
            .unwrap()
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &head, "superseded", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.release_step(&head.id, "owner").await.unwrap();
        let accepted = db
            .claim_step("writer", Some("t"), &lease_deadline())
            .await
            .unwrap()
            .unwrap();
        in_task_step(accepted.clone(), db.execute_task_mutation(&accepted))
            .await
            .unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&db, "t", false)
                .await
                .unwrap()
                .unwrap()
                .entry_barrier_json
                .as_deref(),
            Some("blocked")
        );
    }
    #[tokio::test]
    async fn accepted_annotation_rebases_after_fast_metadata_write_without_cas_retry() {
        let db = fixture().await;
        db.enqueue_task_mutation(
            "t",
            TaskMutation::Sql {
                task_id: "t".into(),
                query: "UPDATE task SET title='edited',version=version+1 WHERE id=?".into(),
                arguments: vec![serde_json::json!("t")],
            },
        )
        .await
        .unwrap();
        db.enqueue_task_mutation(
            "t",
            TaskMutation::TaskUpdate {
                input: crate::UpdateTask {
                    id: "t".into(),
                    expected_version: 1,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(Some("dependency_cancelled".into())),
                    blocked_json: Some(Some("{\"reason\":\"dependency cancelled\"}".into())),
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: crate::now_rfc3339(),
                },
            },
        )
        .await
        .unwrap();
        for _ in 0..2 {
            let step = db
                .claim_step("writer", Some("t"), &lease_deadline())
                .await
                .unwrap()
                .unwrap();
            in_task_step(step.clone(), db.execute_task_mutation(&step))
                .await
                .unwrap();
            db.release_step(&step.id, "writer").await.unwrap();
        }
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        assert_eq!(task.title, "edited");
        assert_eq!(
            task.error_annotation.as_deref(),
            Some("dependency_cancelled")
        );
        assert!(task.blocked_json.is_some());
        assert_eq!(task.version, 3);
        assert!(db
            .task_steps("t")
            .await
            .unwrap()
            .iter()
            .all(|s| s.status == "done" && s.attempts == 1));
    }
    #[tokio::test]
    async fn queued_sql_effect_rebases_content_version_and_rejects_a_released_owner() {
        let db = fixture().await;
        for (query, arguments) in [
            (
                "UPDATE task SET title='edited',version=version+1 WHERE id=?",
                vec![serde_json::json!("t")],
            ),
            (
                "UPDATE task SET error_annotation=?,version=version+1 WHERE id=? AND version = ?",
                vec![
                    serde_json::json!("parked"),
                    serde_json::json!("t"),
                    serde_json::json!(1),
                ],
            ),
        ] {
            db.enqueue_task_mutation(
                "t",
                TaskMutation::Sql {
                    task_id: "t".into(),
                    query: query.into(),
                    arguments,
                },
            )
            .await
            .unwrap();
        }
        let mut last = None;
        for _ in 0..2 {
            let step = db
                .claim_step("writer", Some("t"), &lease_deadline())
                .await
                .unwrap()
                .unwrap();
            in_task_step(step.clone(), db.execute_task_mutation(&step))
                .await
                .unwrap();
            db.release_step(&step.id, "writer").await.unwrap();
            last = Some(step);
        }
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        assert_eq!(task.title, "edited");
        assert_eq!(task.error_annotation.as_deref(), Some("parked"));
        assert_eq!(task.version, 3);
        let result = in_task_step(
            last.unwrap(),
            db.apply_task_sql(
                "t",
                "UPDATE task SET error_annotation=NULL WHERE id=?",
                vec![serde_json::json!("t")],
            ),
        )
        .await;
        assert!(matches!(result, Err(DbError::VersionConflict)));
        assert_eq!(
            TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap(),
            task
        );
    }
    #[tokio::test]
    async fn pending_remote_cancel_survives_workspace_delete_until_owner_ack() {
        use crate::WorkspaceRepo;
        let db = fixture().await;
        let root = tempfile::tempdir().unwrap();
        let now = crate::now_rfc3339();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/t','ready',?,?)").bind(root.path().to_string_lossy().as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout','ready',?,?)").bind(root.path().to_string_lossy().as_ref()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,state,selected_by,selection_reason,created_at,updated_at) VALUES('placement','w','t','server','l','handle','ready','backfill','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let step_id = db
            .enqueue_task_mutation(
                "t",
                TaskMutation::TaskSetEntryBarrier {
                    id: "t".into(),
                    expected_version: 1,
                    entry_barrier_json: None,
                    updated_at: now.clone(),
                },
            )
            .await
            .unwrap();
        sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES('operation',?,'w','placement','owner','runtime',1,0,'running',?)").bind(&step_id).bind(&now).execute(db.pool()).await.unwrap();
        let operation = db
            .running_remote_task_operations(&step_id)
            .await
            .unwrap()
            .remove(0);
        db.mark_pending_remote_cancel(&operation).await.unwrap();
        // While the operation's step is live, deletion is a typed conflict.
        assert!(matches!(
            WorkspaceRepo::delete(&db, "w").await,
            Err(DbError::ResourceInUse { .. })
        ));
        sqlx::query("UPDATE task_step SET status='superseded',completed_at=? WHERE id=?")
            .bind(&now)
            .bind(&step_id)
            .execute(db.pool())
            .await
            .unwrap();
        // The marker never blocks deletion; it stays as a daemon-scoped
        // cleanup record that owner reconnect still acknowledges.
        WorkspaceRepo::delete(&db, "w").await.unwrap();
        assert!(WorkspaceRepo::get_by_id(&db, "w").await.unwrap().is_none());
        assert_eq!(
            db.pending_remote_cancels(Some("owner"), None)
                .await
                .unwrap()
                .len(),
            1
        );
        db.acknowledge_remote_cancel(&operation).await.unwrap();
        assert!(db
            .pending_remote_cancels(None, None)
            .await
            .unwrap()
            .is_empty());
    }
    #[tokio::test]
    async fn identity_fenced_effect_survives_a_status_change_and_a_preempting_cancel() {
        let db = fixture().await;
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        // An entry-fenced deferral and an identity-fenced wake queued in the
        // same status entry ...
        let deferral = TaskQuery::new(
            &db,
            "t",
            "UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.deferred_dispatch','entry') WHERE id=?",
        )
        .bind("t")
        .execute_in_tx(&mut tx)
        .await
        .unwrap();
        let wake = TaskQuery::new(
            &db,
            "t",
            "UPDATE task SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.woken',1) WHERE id=?",
        )
        .bind("t")
        .identity_fenced()
        .execute_in_tx(&mut tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(deferral, TaskQueryResult::Queued { .. }));
        assert!(matches!(wake, TaskQueryResult::Queued { .. }));
        // ... then the Task changes status before either runs.
        sqlx::query("UPDATE task SET status='backlog',version=version+1 WHERE id='t'")
            .execute(db.pool())
            .await
            .unwrap();
        for _ in 0..2 {
            let step = db
                .claim_step("writer", Some("t"), &lease_deadline())
                .await
                .unwrap()
                .unwrap();
            in_task_step(step.clone(), db.execute_task_mutation(&step))
                .await
                .unwrap();
            db.release_step(&step.id, "writer").await.unwrap();
        }
        let task = TaskRepo::get_by_id(&db, "t", false).await.unwrap().unwrap();
        let metadata: Value = serde_json::from_str(task.metadata_json.as_deref().unwrap()).unwrap();
        assert_eq!(
            metadata["woken"], 1,
            "a wake is never dropped by a status change"
        );
        assert!(
            metadata.get("deferred_dispatch").is_none(),
            "the entry-fenced deferral is moot"
        );
        // A preempting Cancel supersedes pending entry-fenced work only.
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let _ = TaskQuery::new(&db, "t", "UPDATE task SET title='entry' WHERE id=?")
            .bind("t")
            .execute_in_tx(&mut tx)
            .await
            .unwrap();
        let _ = TaskQuery::new(
            &db,
            "t",
            "UPDATE task SET description='identity' WHERE id=?",
        )
        .bind("t")
        .identity_fenced()
        .execute_in_tx(&mut tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        db.enqueue_step(&EnqueueTaskStep {
            id: crate::new_uuid_v4(),
            task_id: "t".into(),
            kind: "command".into(),
            payload_json: serde_json::json!({"operation":"cancel_task_with_options","arguments":[],"preempt":true}).to_string(),
            causation_step_id: None,
            causation_key: "cancel".into(),
            chain_id: "cancel".into(),
            chain_position: 1,
            expected_status: "backlog".into(),
            expected_version: 2,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: crate::now_rfc3339(),
        })
        .await
        .unwrap();
        let live = db
            .task_steps("t")
            .await
            .unwrap()
            .into_iter()
            .filter(|step| step.status == "pending" && step.kind == "mutation")
            .collect::<Vec<_>>();
        assert_eq!(live.len(), 1);
        assert!(!live[0].entry_fenced);
    }
    #[test]
    fn stored_mutation_errors_preserve_conflict_and_internal_error_categories() {
        let reply = TaskMutationReply::from_result(&Err(DbError::ReviewDetailsCorrupt {
            review_id: "review".into(),
            reason: "invalid JSON".into(),
        }));
        assert!(matches!(
            reply.decode::<Value>(),
            Err(DbError::ReviewDetailsCorrupt { .. })
        ));
        let reply = TaskMutationReply::from_result(&Err(DbError::BoardRevisionConflict {
            expected: 1,
            actual: 2,
        }));
        assert!(matches!(
            reply.decode::<Value>(),
            Err(DbError::BoardRevisionConflict {
                expected: 1,
                actual: 2
            })
        ));
        let reply = TaskMutationReply::from_result(&Err(DbError::Sqlx(sqlx::Error::PoolTimedOut)));
        let error = reply.decode::<Value>().unwrap_err();
        assert!(matches!(error, DbError::Sqlx(_)));
        assert!(error.is_transient());
    }
}
