//! Durable reconciliation of Task outcomes into Agent coordination state.
//!
//! Task transitions are authoritative in the Task/domain-event ledger. The
//! worker prepares scope-validated outcomes, then commits idempotent commitment
//! and inbox effects with its retained checkpoint in one runtime transaction.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use db::{
    now_rfc3339, AgentCommitmentStatus, AgentInboxKind, CreateAgentCommitmentEvidence, DomainEvent,
    DomainEventRepo, SqliteDb, Task, TaskRepo,
};
use serde_json::{json, Value};
use sqlx::{Acquire, Row, Sqlite, Transaction};

use tokio::{sync::watch, task::JoinHandle};

use crate::{
    worker_runtime::{Outcome, Subscription, Worker, WorkerError, WorkerRuntime},
    CommitmentService, Result, ServiceError,
};

const CONSUMER_NAME: &str = "agent-coordination-outcomes";
const EVENT_TYPES: &[&str] = &[
    "task.transitioned",
    "task.done",
    "task.completed",
    "task.blocked",
    "task.failed",
    "task.cancelled",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationOutcomeRun {
    pub claimed_events: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Clone)]
pub struct CoordinationOutcomeConsumer {
    db: Arc<SqliteDb>,
    commitments: CommitmentService,
    consumer_name: String,
}

impl CoordinationOutcomeConsumer {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            commitments: CommitmentService::new(Arc::clone(&db)),
            db,
            consumer_name: CONSUMER_NAME.to_owned(),
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = consumer_name.into();
        self
    }

    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self)).start(shutdown)
    }
    pub async fn run_once(&self, limit: i64) -> Result<CoordinationOutcomeRun> {
        let processed_events = WorkerRuntime::new(Arc::clone(&self.db), Arc::new(self.clone()))
            .run_once(limit.clamp(1, 100) as usize)
            .await?;
        let last_sequence = self
            .db
            .get_consumer_cursor(&self.consumer_name)
            .await?
            .map_or(0, |c| c.last_sequence);
        Ok(CoordinationOutcomeRun {
            claimed_events: processed_events,
            processed_events,
            last_sequence,
        })
    }

    async fn prepare_event(&self, event: &DomainEvent) -> Result<Option<PreparedCoordination>> {
        if event.entity_type != "task" || !is_task_outcome_event(event) {
            return Ok(None);
        }
        let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await? else {
            // A task can be removed by a forward migration after its event
            // was committed.  The event remains checkpointable; there is no
            // safe scope to which a synthetic outcome could be delivered.
            return Ok(None);
        };
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(outcome) = task_outcome(event, &task, &payload) else {
            return Ok(None);
        };

        let commitment_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_commitment
             WHERE originating_task_id = ? ORDER BY id ASC",
        )
        .bind(&task.id)
        .fetch_all(self.db.pool())
        .await?;
        let action_origins = self.task_action_origins(&task.id).await?;

        let mut recipients = BTreeMap::<RecipientKey, RecipientSource>::new();
        let mut commitments = Vec::new();
        for commitment_id in commitment_ids {
            let commitment = self.commitments.get(&commitment_id).await?;
            if commitment.status == AgentCommitmentStatus::Cancelled {
                continue;
            }
            if !self
                .commitment_scope_matches_task(&commitment.scope_type, &commitment.scope_id, &task)
                .await?
            {
                tracing::warn!(
                    commitment_id = %commitment.id,
                    task_id = %task.id,
                    "skipping Task outcome for a commitment with an unrelated scope"
                );
                continue;
            }
            let key = RecipientKey {
                identity_id: commitment.owner_identity_id.clone(),
                scope_type: commitment.scope_type.clone(),
                scope_id: commitment.scope_id.clone(),
            };
            recipients
                .entry(key)
                .or_insert_with(|| RecipientSource::Commitment(commitment.id.clone()));
            commitments.push(commitment);
        }
        for action in &action_origins {
            if !self
                .action_scope_matches_task(&action.scope_type, &action.scope_id, &task)
                .await?
            {
                tracing::warn!(
                    action_id = %action.action_id,
                    task_id = %task.id,
                    "skipping Task outcome for an action with an unrelated scope"
                );
                continue;
            }
            let key = RecipientKey {
                identity_id: action.actor_identity_id.clone(),
                scope_type: action.scope_type.clone(),
                scope_id: action.scope_id.clone(),
            };
            recipients
                .entry(key)
                .or_insert_with(|| RecipientSource::Action(action.action_id.clone()));
        }

        Ok(Some(PreparedCoordination {
            task,
            outcome,
            actions: action_origins,
            commitments,
            recipients,
        }))
    }

    async fn task_action_origins(&self, task_id: &str) -> Result<Vec<ActionOrigin>> {
        let rows = sqlx::query(
            "SELECT a.id, a.actor_identity_id, a.scope_type, a.scope_id,
                    e.result_json
             FROM agent_action AS a
             JOIN agent_action_execution AS e ON e.action_id = a.id
             WHERE a.operation = 'task.propose'
               AND e.status = 'succeeded'
               AND e.result_json IS NOT NULL
             ORDER BY a.id ASC, e.created_at ASC",
        )
        .fetch_all(self.db.pool())
        .await?;
        let mut origins = Vec::new();
        let mut seen = BTreeSet::new();
        for row in rows {
            let result_json: String = row.try_get("result_json")?;
            let result = serde_json::from_str::<Value>(&result_json).unwrap_or(Value::Null);
            if result.get("task_id").and_then(Value::as_str) != Some(task_id) {
                continue;
            }
            let action_id: String = row.try_get("id")?;
            if !seen.insert(action_id.clone()) {
                continue;
            }
            origins.push(ActionOrigin {
                action_id,
                actor_identity_id: row.try_get("actor_identity_id")?,
                scope_type: row.try_get("scope_type")?,
                scope_id: row.try_get("scope_id")?,
            });
        }
        Ok(origins)
    }

    async fn acknowledge_originating_inbox_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        actions: &[ActionOrigin],
    ) -> Result<()> {
        let mut source_ids = Vec::with_capacity(actions.len() + 1);
        source_ids.push(task_id.to_owned());
        source_ids.extend(actions.iter().map(|action| action.action_id.clone()));
        let now = now_rfc3339();
        for source_id in source_ids {
            let ids = sqlx::query_scalar::<_, String>(
                "SELECT id FROM agent_inbox_item
                 WHERE source_id = ? AND COALESCE(source_type, '') <> 'task_outcome'",
            )
            .bind(&source_id)
            .fetch_all(&mut **tx)
            .await?;
            for id in ids {
                sqlx::query(
                    "UPDATE agent_inbox_item SET
                        status = 'acknowledged',
                        read_at = COALESCE(read_at, ?),
                        acknowledged_at = COALESCE(acknowledged_at, ?),
                        version = version + 1,
                        updated_at = ?
                     WHERE id = ? AND status IN ('unread', 'read')",
                )
                .bind(&now)
                .bind(&now)
                .bind(&now)
                .bind(id)
                .execute(&mut **tx)
                .await?;
            }
        }
        Ok(())
    }

    async fn commitment_scope_matches_task(
        &self,
        scope_type: &str,
        scope_id: &str,
        task: &Task,
    ) -> Result<bool> {
        match scope_type {
            "task" => Ok(scope_id == task.id),
            "project" => Ok(scope_id == task.project_id),
            "account" => Ok(sqlx::query_scalar::<_, Option<String>>(
                "SELECT owner_id FROM project WHERE id = ?",
            )
            .bind(&task.project_id)
            .fetch_optional(self.db.pool())
            .await?
            .flatten()
            .as_deref()
                == Some(scope_id)),
            "agent_chat" => Ok(sqlx::query_scalar::<_, Option<String>>(
                "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project'",
            )
            .bind(scope_id)
            .fetch_optional(self.db.pool())
            .await?
            .flatten()
            .as_deref()
                == Some(task.project_id.as_str())),
            // An Agent-scoped commitment does not carry a canonical Task
            // authority relation, so fail closed rather than cross-deliver.
            _ => Ok(false),
        }
    }

    async fn reconcile_commitment_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        commitment: &db::AgentCommitment,
        task: &Task,
        event: &DomainEvent,
        outcome: &TaskOutcome,
    ) -> Result<()> {
        let current = self
            .db
            .get_commitment_in_tx(tx, &commitment.id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if current.status == AgentCommitmentStatus::Cancelled {
            return Ok(());
        }
        if current.version != commitment.version {
            return Err(db::DbError::VersionConflict.into());
        }
        let dedupe_key = format!("task-outcome:{}:{}:commitment", task.id, event.id);
        match outcome {
            TaskOutcome::Delivered => {
                let evidence = CreateAgentCommitmentEvidence {
                    // Evidence IDs are globally unique, while one Task may
                    // satisfy several independently owned commitments.  The
                    // commitment suffix prevents one outcome from colliding
                    // with another commitment's evidence row during the same
                    // reconciliation transaction.
                    id: format!("task-delivery-evidence:{}:{}", task.id, commitment.id),
                    commitment_id: commitment.id.clone(),
                    evidence_type: "task_delivery".to_owned(),
                    evidence_id: task.id.clone(),
                    scope_type: "task".to_owned(),
                    scope_id: task.id.clone(),
                    description: Some("Task delivery reached the done state".to_owned()),
                    metadata_json: json!({
                        "task_id": &task.id,
                        "task_version": task.version,
                        "event_id": &event.id,
                        "event_type": &event.event_type,
                        "correlation_id": &event.correlation_id,
                    })
                    .to_string(),
                    authorized_by_type: "forge".to_owned(),
                    authorized_by_id: CONSUMER_NAME.to_owned(),
                    dedupe_key: dedupe_key.clone(),
                    created_at: now_rfc3339(),
                };
                if commitment.status == AgentCommitmentStatus::Completed {
                    return Ok(());
                }
                crate::coordination_service::validate_commitment_transition(
                    &current.status,
                    &AgentCommitmentStatus::Completed,
                    Some("Task delivery reconciled from the durable outcome event"),
                )?;
                let result = self
                    .db
                    .complete_commitment_in_tx(
                        tx,
                        db::CompleteAgentCommitment {
                            id: commitment.id.clone(),
                            expected_version: commitment.version,
                            evidence,
                            actor_type: "forge".to_owned(),
                            actor_id: CONSUMER_NAME.to_owned(),
                            reason: Some(
                                "Task delivery reconciled from the durable outcome event"
                                    .to_owned(),
                            ),
                            dedupe_key,
                            completed_at: now_rfc3339(),
                            updated_at: now_rfc3339(),
                        },
                    )
                    .await;
                match result {
                    Ok(_) => Ok(()),
                    Err(db::DbError::VersionConflict) => {
                        let evidence: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_commitment_evidence WHERE commitment_id = ? AND evidence_type = 'task_delivery' AND evidence_id = ?")
                            .bind(&commitment.id).bind(&task.id).fetch_one(&mut **tx).await?;
                        if current.status == AgentCommitmentStatus::Completed && evidence > 0 {
                            Ok(())
                        } else {
                            Err(db::DbError::VersionConflict.into())
                        }
                    }
                    Err(error) => Err(error.into()),
                }
            }
            TaskOutcome::Blocked { reason } | TaskOutcome::Cancelled { reason } => {
                let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_commitment_lifecycle WHERE commitment_id = ? AND dedupe_key = ?")
                    .bind(&commitment.id).bind(&dedupe_key).fetch_one(&mut **tx).await?;
                if exists > 0 {
                    return Ok(());
                }
                let (status, blocked_reason) = match &commitment.status {
                    AgentCommitmentStatus::Proposed => (AgentCommitmentStatus::Open, None),
                    AgentCommitmentStatus::Open
                    | AgentCommitmentStatus::Accepted
                    | AgentCommitmentStatus::InProgress
                    | AgentCommitmentStatus::Blocked => {
                        (AgentCommitmentStatus::Blocked, Some(Some(reason.clone())))
                    }
                    AgentCommitmentStatus::Completed | AgentCommitmentStatus::Cancelled => {
                        return Ok(());
                    }
                };
                crate::coordination_service::validate_commitment_transition(
                    &current.status,
                    &status,
                    Some(reason),
                )?;
                self.db
                    .update_commitment_in_tx(
                        tx,
                        db::UpdateAgentCommitment {
                            id: commitment.id.clone(),
                            expected_version: commitment.version,
                            status: Some(status),
                            due_at: None,
                            description: None,
                            blocked_reason,
                            cancellation_reason: None,
                            actor_type: "forge".to_owned(),
                            actor_id: CONSUMER_NAME.to_owned(),
                            reason: Some(reason.clone()),
                            evidence_id: None,
                            dedupe_key,
                            updated_at: now_rfc3339(),
                        },
                    )
                    .await
                    .map(|_| ())
                    .or_else(|error| match error {
                        db::DbError::VersionConflict => Ok(()),
                        other => Err(other.into()),
                    })
            }
        }
    }

    async fn action_scope_matches_task(
        &self,
        scope_type: &str,
        scope_id: &str,
        task: &Task,
    ) -> Result<bool> {
        Ok(match scope_type {
            "project" => scope_id == task.project_id,
            "task" => scope_id == task.id,
            "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
                "SELECT project_id FROM agent_chat
                 WHERE id = ? AND kind = 'project'",
            )
            .bind(scope_id)
            .fetch_optional(self.db.pool())
            .await?
            .flatten()
            .is_some_and(|project_id| project_id == task.project_id),
            _ => false,
        })
    }

    async fn deliver_outcome_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        recipient: &RecipientKey,
        source: &RecipientSource,
        task: &Task,
        event: &DomainEvent,
        outcome: &TaskOutcome,
    ) -> Result<()> {
        let (status, reason) = outcome.fields();
        let source_id = match source {
            RecipientSource::Commitment(id) | RecipientSource::Action(id) => id,
        };
        let payload_json = json!({
            "task_id": &task.id,
            "task_version": task.version,
            "event_id": &event.id,
            "event_type": &event.event_type,
            "status": status,
            "reason": reason,
            "source_id": source_id,
        })
        .to_string();
        let title = match status {
            "delivered" => "Task delivered",
            "blocked" => "Task delivery blocked",
            "cancelled" => "Task delivery cancelled",
            _ => "Task outcome",
        };
        self.db
            .create_inbox_item_in_tx(
                tx,
                db::CreateAgentInboxItem {
                    id: db::new_uuid_v4(),
                    recipient_identity_id: recipient.identity_id.clone(),
                    scope_type: recipient.scope_type.clone(),
                    scope_id: recipient.scope_id.clone(),
                    kind: AgentInboxKind::TaskOutcome,
                    status: db::AgentInboxStatus::Unread,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                    title: title.to_owned(),
                    body: payload_json.clone(),
                    payload_json,
                    source_type: Some("task_outcome".to_owned()),
                    source_id: Some(task.id.clone()),
                    correlation_id: event.correlation_id.clone(),
                    causation_id: Some(event.id.clone()),
                    dedupe_key: format!(
                        "task-outcome:{}:{}:inbox:{}:{}:{}:{}",
                        task.id,
                        event.id,
                        recipient.identity_id,
                        recipient.scope_type,
                        recipient.scope_id,
                        source_id
                    ),
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                tracing::warn!(
                    task_id = %task.id,
                    recipient_identity_id = %recipient.identity_id,
                    source_id = %source_id,
                    %error,
                    "Task outcome inbox delivery failed and will retry"
                );
                error.into()
            })
    }
}

pub struct PreparedCoordination {
    task: Task,
    outcome: TaskOutcome,
    actions: Vec<ActionOrigin>,
    commitments: Vec<db::AgentCommitment>,
    recipients: BTreeMap<RecipientKey, RecipientSource>,
}
#[async_trait]
impl Worker for CoordinationOutcomeConsumer {
    type Prepared = PreparedCoordination;
    fn name(&self) -> &str {
        &self.consumer_name
    }
    fn subscription(&self) -> Subscription {
        Subscription::Exact(EVENT_TYPES.iter().map(|s| s.to_string()).collect())
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
        self.prepare_event(event)
            .await
            .map(|p| p.map_or(Outcome::Skip, Outcome::Done))
            .map_err(crate::worker_runtime::consumer_error)
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        p: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        async {
            self.acknowledge_originating_inbox_in_tx(tx, &p.task.id, &p.actions)
                .await?;
            let health =
                crate::worker_runtime::WorkerHealth::new(Arc::clone(&self.db), &self.consumer_name);
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id = ?)")
                .bind(&p.task.id)
                .fetch_one(&mut **tx)
                .await?;
            if !exists {
                return Err(db::DbError::NotFound.into());
            }
            for commitment in &p.commitments {
                if self
                    .db
                    .get_commitment_in_tx(tx, &commitment.id)
                    .await?
                    .is_none()
                {
                    continue;
                }
                let mut item_tx = tx.begin().await?;
                match self
                    .reconcile_commitment_in_tx(
                        &mut item_tx,
                        commitment,
                        &p.task,
                        event,
                        &p.outcome,
                    )
                    .await
                {
                    Ok(()) => item_tx.commit().await?,
                    Err(error) => {
                        item_tx.rollback().await?;
                        let kind = crate::worker_runtime::consumer_error_kind(&error);
                        if kind != crate::worker_runtime::WorkerErrorKind::Terminal {
                            return Err(error);
                        }
                        let key = format!("event:{}:commitment:{}", event.sequence, commitment.id);
                        health
                            .isolated_item_failed_in_tx(
                                tx,
                                crate::worker_runtime::WorkItem {
                                    source_key: &key,
                                    item_type: "coordination_commitment",
                                },
                                db::RetryPolicy::default(),
                                "terminal",
                                &format!("commitment {}: {}", commitment.id, error),
                            )
                            .await?;
                    }
                }
            }
            for (recipient, source) in &p.recipients {
                if let RecipientSource::Commitment(id) = source {
                    if self
                        .db
                        .get_commitment_in_tx(tx, id)
                        .await?
                        .is_none_or(|current| current.status == AgentCommitmentStatus::Cancelled)
                    {
                        continue;
                    }
                }
                let mut item_tx = tx.begin().await?;
                match self
                    .deliver_outcome_in_tx(
                        &mut item_tx,
                        recipient,
                        source,
                        &p.task,
                        event,
                        &p.outcome,
                    )
                    .await
                {
                    Ok(()) => item_tx.commit().await?,
                    Err(error) => {
                        item_tx.rollback().await?;
                        let kind = crate::worker_runtime::consumer_error_kind(&error);
                        if kind != crate::worker_runtime::WorkerErrorKind::Terminal {
                            return Err(error);
                        }
                        let key = format!(
                            "event:{}:inbox:{}:{}:{}:{:?}",
                            event.sequence,
                            recipient.identity_id,
                            recipient.scope_type,
                            recipient.scope_id,
                            source
                        );
                        health
                            .isolated_item_failed_in_tx(
                                tx,
                                crate::worker_runtime::WorkItem {
                                    source_key: &key,
                                    item_type: "coordination_inbox",
                                },
                                db::RetryPolicy::default(),
                                "terminal",
                                &error.to_string(),
                            )
                            .await?;
                    }
                }
            }
            Ok::<_, ServiceError>(())
        }
        .await
        .map_err(crate::worker_runtime::consumer_error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RecipientKey {
    identity_id: String,
    scope_type: String,
    scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecipientSource {
    Commitment(String),
    Action(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionOrigin {
    action_id: String,
    actor_identity_id: String,
    scope_type: String,
    scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskOutcome {
    Delivered,
    Blocked { reason: String },
    Cancelled { reason: String },
}

impl TaskOutcome {
    fn fields(&self) -> (&'static str, Option<&str>) {
        match self {
            Self::Delivered => ("delivered", None),
            Self::Blocked { reason } => ("blocked", Some(reason)),
            Self::Cancelled { reason } => ("cancelled", Some(reason)),
        }
    }
}

fn is_task_outcome_event(event: &DomainEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        "task.transitioned"
            | "task.done"
            | "task.completed"
            | "task.blocked"
            | "task.failed"
            | "task.cancelled"
    )
}

fn task_outcome(event: &DomainEvent, task: &Task, payload: &Value) -> Option<TaskOutcome> {
    if event.event_type == "task.transitioned" {
        let transition =
            serde_json::from_value::<api_types::TaskTransitionEventPayload>(payload.clone())
                .ok()?;
        let snapshot = transition.known_workflow_snapshot()?;
        if snapshot.from_state.name == snapshot.to_state.name
            || snapshot.to_state.kind != api_types::StateKind::Terminal
        {
            return None;
        }
        return if snapshot.to_state.is_cancellation {
            Some(TaskOutcome::Cancelled {
                reason: transition.trigger_reason,
            })
        } else {
            Some(TaskOutcome::Delivered)
        };
    }
    let state = payload
        .get("to_state")
        .and_then(Value::as_str)
        .or_else(|| payload.get("status").and_then(Value::as_str))
        .unwrap_or(task.status.as_str());
    let reason = payload
        .get("trigger_reason")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| task_reason(task))
        .unwrap_or_else(|| format!("Task reached terminal outcome: {state}"));

    if state == "done" || state == "completed" || event.event_type == "task.done" {
        return Some(TaskOutcome::Delivered);
    }
    if state == "cancelled" || event.event_type == "task.cancelled" {
        return Some(TaskOutcome::Cancelled { reason });
    }
    if state == "blocked"
        || state.ends_with("_failed")
        || task.condition.read().hard_failure
        || event.event_type == "task.blocked"
        || event.event_type == "task.failed"
    {
        return Some(TaskOutcome::Blocked { reason });
    }
    None
}

fn task_reason(task: &Task) -> Option<String> {
    task.condition.read().interruption.map(|i| i.reason)
}

pub fn coordination_consumer_name() -> &'static str {
    CONSUMER_NAME
}
