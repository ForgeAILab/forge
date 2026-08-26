//! Durable reconciliation of Task outcomes into Agent coordination state.
//!
//! Task transitions are authoritative in the Task/domain-event ledger.  This
//! consumer is deliberately a projection: it claims the same leased event
//! sequence used by the other durable consumers, applies only idempotent
//! commitment/inbox writes, and records the receipt after those writes commit.
//! A process crash therefore replays the event without creating a second
//! evidence row, lifecycle transition, or outcome message.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use db::{
    now_rfc3339, AgentCommitmentRepo, AgentCommitmentStatus, AgentInboxKind, ClaimDomainEvents,
    CompleteDomainEvent, CreateAgentCommitmentEvidence, DomainEvent, DomainEventRepo, SqliteDb,
    Task, TaskRepo,
};
use serde_json::{json, Value};
use sqlx::Row;
use tokio::{sync::watch, task::JoinHandle, time::Duration as TokioDuration};
use uuid::Uuid;

use crate::{
    AgentInboxService, CommitmentService, CompleteCommitmentInput, DeliverInboxInput, Result,
    ServiceError, UpdateCommitmentInput,
};

const CONSUMER_NAME: &str = "agent-coordination-outcomes";
const LEASE_SECONDS: i64 = 60;
const POLL_INTERVAL: TokioDuration = TokioDuration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinationOutcomeRun {
    pub claimed_events: usize,
    pub reconciled_events: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Clone)]
pub struct CoordinationOutcomeConsumer {
    db: Arc<SqliteDb>,
    commitments: CommitmentService,
    inbox: AgentInboxService,
    consumer_name: String,
    lease_owner: String,
}

impl CoordinationOutcomeConsumer {
    pub fn new(db: Arc<SqliteDb>, lease_owner: impl Into<String>) -> Self {
        Self {
            commitments: CommitmentService::new(Arc::clone(&db)),
            inbox: AgentInboxService::new(Arc::clone(&db)),
            db,
            consumer_name: CONSUMER_NAME.to_owned(),
            lease_owner: lease_owner.into(),
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = consumer_name.into();
        self
    }

    /// Start the restart-safe outcome projector.  The in-memory lease owner
    /// is only a holder identity; the cursor and receipts live in SQLite.
    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Err(error) = self.run_once(100).await {
                            tracing::warn!(
                                consumer = %self.consumer_name,
                                %error,
                                "Task outcome reconciliation poll failed"
                            );
                        }
                    }
                }
            }
        })
    }

    /// Claim and reconcile a bounded batch.  A failed projection is not
    /// acknowledged, so the lease expires and a subsequent process retries
    /// the same event.  Every mutation below carries an event-derived dedupe
    /// key, making that replay safe even after a crash between projection and
    /// receipt checkpoint.
    pub async fn run_once(&self, limit: i64) -> Result<CoordinationOutcomeRun> {
        let now = now_rfc3339();
        let leased_until = lease_until(&now);
        let events = DomainEventRepo::claim_event_batch(
            &*self.db,
            ClaimDomainEvents {
                consumer_name: self.consumer_name.clone(),
                lease_owner: self.lease_owner.clone(),
                now,
                leased_until,
                limit: limit.clamp(1, 100),
            },
        )
        .await?;
        let claimed_events = events.len();
        let mut reconciled_events = 0;
        let mut processed_events = 0;
        let mut last_sequence =
            DomainEventRepo::get_consumer_cursor(&*self.db, &self.consumer_name)
                .await?
                .map(|cursor| cursor.last_sequence)
                .unwrap_or(0);

        for event in events {
            if self.reconcile_event(&event).await? {
                reconciled_events += 1;
            }
            let dedupe_key = crate::domain_event_service::event_completion_dedupe_key(&event);
            DomainEventRepo::complete_claimed_event(
                &*self.db,
                CompleteDomainEvent {
                    consumer_name: self.consumer_name.clone(),
                    lease_owner: self.lease_owner.clone(),
                    event_sequence: event.sequence,
                    event_id: event.id,
                    dedupe_key,
                    completed_at: now_rfc3339(),
                },
            )
            .await?;
            processed_events += 1;
            last_sequence = event.sequence;
        }

        Ok(CoordinationOutcomeRun {
            claimed_events,
            reconciled_events,
            processed_events,
            last_sequence,
        })
    }

    async fn reconcile_event(&self, event: &DomainEvent) -> Result<bool> {
        if event.entity_type != "task" || !is_task_outcome_event(event) {
            return Ok(false);
        }
        let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await? else {
            // A task can be removed by a forward migration after its event
            // was committed.  The event remains checkpointable; there is no
            // safe scope to which a synthetic outcome could be delivered.
            return Ok(false);
        };
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(outcome) = task_outcome(event, &task, &payload) else {
            return Ok(false);
        };

        let commitment_ids = sqlx::query_scalar::<_, String>(
            "SELECT id FROM agent_commitment
             WHERE originating_task_id = ? ORDER BY id ASC",
        )
        .bind(&task.id)
        .fetch_all(self.db.pool())
        .await?;
        let action_origins = self.task_action_origins(&task.id).await?;

        // A proposal inbox is acknowledged before the new outcome item is
        // inserted.  This prevents a replay from acknowledging the outcome
        // message that this same projector just created.
        self.acknowledge_originating_inbox(&task.id, &action_origins)
            .await?;

        let mut recipients = BTreeMap::<RecipientKey, RecipientSource>::new();
        for commitment_id in commitment_ids {
            let commitment = self.commitments.get(&commitment_id).await?;
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
            self.reconcile_commitment(&commitment, &task, event, &outcome)
                .await?;
            let key = RecipientKey {
                identity_id: commitment.owner_identity_id.clone(),
                scope_type: commitment.scope_type.clone(),
                scope_id: commitment.scope_id.clone(),
            };
            recipients
                .entry(key)
                .or_insert_with(|| RecipientSource::Commitment(commitment.id.clone()));
        }
        for action in action_origins {
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
                identity_id: action.actor_identity_id,
                scope_type: action.scope_type,
                scope_id: action.scope_id,
            };
            recipients
                .entry(key)
                .or_insert_with(|| RecipientSource::Action(action.action_id));
        }

        for (recipient, source) in recipients {
            self.deliver_outcome(&recipient, &source, &task, event, &outcome)
                .await?;
        }
        Ok(true)
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

    async fn acknowledge_originating_inbox(
        &self,
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
            .fetch_all(self.db.pool())
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
                .execute(self.db.pool())
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

    async fn reconcile_commitment(
        &self,
        commitment: &db::AgentCommitment,
        task: &Task,
        event: &DomainEvent,
        outcome: &TaskOutcome,
    ) -> Result<()> {
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
                    let evidence_rows = self.commitments.evidence(&commitment.id).await?;
                    if evidence_rows.iter().any(|row| {
                        row.evidence_type == "task_delivery" && row.evidence_id == task.id
                    }) {
                        return Ok(());
                    }
                    // Never rewrite a completed obligation with a second
                    // source of evidence; a human completion remains final.
                    return Ok(());
                }
                let result = self
                    .commitments
                    .complete(CompleteCommitmentInput {
                        id: commitment.id.clone(),
                        expected_version: commitment.version,
                        evidence: commitment_evidence_input(&evidence),
                        actor_type: "forge".to_owned(),
                        actor_id: CONSUMER_NAME.to_owned(),
                        reason: Some(
                            "Task delivery reconciled from the durable outcome event".to_owned(),
                        ),
                        dedupe_key,
                    })
                    .await;
                match result {
                    Ok(_) => Ok(()),
                    Err(ServiceError::Db(db::DbError::VersionConflict)) => {
                        let current = self.commitments.get(&commitment.id).await?;
                        let evidence_rows = self.commitments.evidence(&commitment.id).await?;
                        if current.status == AgentCommitmentStatus::Completed
                            && evidence_rows.iter().any(|row| {
                                row.evidence_type == "task_delivery" && row.evidence_id == task.id
                            })
                        {
                            Ok(())
                        } else {
                            Err(ServiceError::Db(db::DbError::VersionConflict))
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            TaskOutcome::Blocked { reason } | TaskOutcome::Cancelled { reason } => {
                let lifecycle =
                    AgentCommitmentRepo::list_commitment_lifecycle(&*self.db, &commitment.id)
                        .await?;
                if lifecycle.iter().any(|row| row.dedupe_key == dedupe_key) {
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
                self.commitments
                    .update(UpdateCommitmentInput {
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
                    })
                    .await
                    .map(|_| ())
                    .or_else(|error| match error {
                        ServiceError::Db(db::DbError::VersionConflict) => Ok(()),
                        other => Err(other),
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

    async fn deliver_outcome(
        &self,
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
        self.inbox
            .deliver(DeliverInboxInput {
                id: None,
                recipient_identity_id: recipient.identity_id.clone(),
                scope_type: recipient.scope_type.clone(),
                scope_id: recipient.scope_id.clone(),
                kind: AgentInboxKind::TaskOutcome,
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
            })
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
                error
            })
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
        || task.failed_json.is_some()
        || event.event_type == "task.blocked"
        || event.event_type == "task.failed"
    {
        return Some(TaskOutcome::Blocked { reason });
    }
    None
}

fn task_reason(task: &Task) -> Option<String> {
    [task.failed_json.as_deref(), task.blocked_json.as_deref()]
        .into_iter()
        .flatten()
        .find_map(|raw| {
            serde_json::from_str::<Value>(raw).ok().and_then(|value| {
                ["reason", "message", "blocking_reason"]
                    .into_iter()
                    .find_map(|key| value.get(key).and_then(Value::as_str).map(str::to_owned))
            })
        })
}

fn commitment_evidence_input(
    evidence: &CreateAgentCommitmentEvidence,
) -> crate::CommitmentEvidenceInput {
    crate::CommitmentEvidenceInput {
        id: Some(evidence.id.clone()),
        commitment_id: evidence.commitment_id.clone(),
        evidence_type: evidence.evidence_type.clone(),
        evidence_id: evidence.evidence_id.clone(),
        scope_type: evidence.scope_type.clone(),
        scope_id: evidence.scope_id.clone(),
        description: evidence.description.clone(),
        metadata_json: evidence.metadata_json.clone(),
        authorized_by_type: evidence.authorized_by_type.clone(),
        authorized_by_id: evidence.authorized_by_id.clone(),
        dedupe_key: evidence.dedupe_key.clone(),
    }
}

fn lease_until(now: &str) -> String {
    DateTime::parse_from_rfc3339(now)
        .map(|value| (value.with_timezone(&Utc) + Duration::seconds(LEASE_SECONDS)).to_rfc3339())
        .unwrap_or_else(|_| now.to_owned())
}

pub fn coordination_consumer_name() -> &'static str {
    CONSUMER_NAME
}

pub fn coordination_consumer_lease_owner() -> String {
    format!("coordination-consumer-{}", Uuid::new_v4())
}
