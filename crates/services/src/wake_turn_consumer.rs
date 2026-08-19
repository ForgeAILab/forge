//! Durable delivery of admitted agent wakes into Agent Chat turns.
//!
//! `AttentionService` admits wakes deterministically (budget, cooldown,
//! dedupe, self-event suppression) and records `agent.wake.admitted` in the
//! durable ledger.  This consumer is the delivery half: it turns each
//! admitted wake into one system-authored Agent Chat message plus a queued
//! turn job for the woken identity's chat, so the agent actually runs a turn
//! and can act on the incident.  It also delivers the user-driven
//! continuation `project.execution_baseline.activated` straight to the
//! Project Agent chat — a user approval is its own deterministic admission
//! and consumes no wake budget.
//!
//! Every admission is idempotent: the turn job dedupe key is derived from the
//! source event, so crash-replay after projection never queues a second turn.

use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, AdmitAgentChatTurn, AgentChat, AgentChatMessageAuthorType,
    AgentChatMessageStatus, AgentChatRepo, AgentChatTransactionRepo, ClaimDomainEvents,
    CompleteDomainEvent, CreateAgentChatMessage, CreateAgentChatTurnJob, DomainEvent,
    DomainEventRepo, ProjectAgentBindingRepo, SqliteDb,
};
use serde_json::Value;
use sqlx::Row;
use tokio::{sync::watch, task::JoinHandle, time::Duration as TokioDuration};
use uuid::Uuid;

use crate::Result;

const CONSUMER_NAME: &str = "agent-wake-turns";
const LEASE_SECONDS: i64 = 60;
const POLL_INTERVAL: TokioDuration = TokioDuration::from_secs(1);
const MAX_TURN_ATTEMPTS: i64 = 3;
const MAX_DETAIL_CHARS: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeTurnRun {
    pub claimed_events: usize,
    pub delivered_turns: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Clone)]
pub struct WakeTurnConsumer {
    db: Arc<SqliteDb>,
    consumer_name: String,
    lease_owner: String,
}

impl WakeTurnConsumer {
    pub fn new(db: Arc<SqliteDb>, lease_owner: impl Into<String>) -> Self {
        Self {
            db,
            consumer_name: CONSUMER_NAME.to_owned(),
            lease_owner: lease_owner.into(),
        }
    }

    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = consumer_name.into();
        self
    }

    /// Start the restart-safe wake deliverer.  The cursor and receipts live
    /// in SQLite; the in-memory lease owner is only a holder identity.
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
                                "wake turn delivery poll failed"
                            );
                        }
                    }
                }
            }
        })
    }

    /// Claim and deliver a bounded batch.  Delivery that cannot ever succeed
    /// (missing chat, replaced binding, chat not ready) is skipped with a
    /// receipt instead of erroring, so one dead wake cannot wedge the cursor.
    pub async fn run_once(&self, limit: i64) -> Result<WakeTurnRun> {
        // Wakes are timely signals, not history to reconcile: on the very
        // first run (no cursor yet) fast-forward past the existing ledger so
        // upgrading a deployment does not flood chats with stale wakes.
        let fast_forwarded = sqlx::query(
            "INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
             SELECT ?, COALESCE((SELECT MAX(sequence) FROM domain_event), 0), 1, ?
             WHERE NOT EXISTS (
                 SELECT 1 FROM event_consumer_cursor WHERE consumer_name = ?
             )",
        )
        .bind(&self.consumer_name)
        .bind(now_rfc3339())
        .bind(&self.consumer_name)
        .execute(self.db.pool())
        .await?;
        if fast_forwarded.rows_affected() > 0 {
            tracing::info!(
                consumer = %self.consumer_name,
                "initialized wake turn cursor past the existing event ledger"
            );
        }
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
        let mut delivered_turns = 0;
        let mut processed_events = 0;
        let mut last_sequence =
            DomainEventRepo::get_consumer_cursor(&*self.db, &self.consumer_name)
                .await?
                .map(|cursor| cursor.last_sequence)
                .unwrap_or(0);

        for event in events {
            if self.deliver_event(&event).await? {
                delivered_turns += 1;
            }
            let dedupe_key = event
                .dedupe_key
                .clone()
                .unwrap_or_else(|| format!("event:{}", event.id));
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

        Ok(WakeTurnRun {
            claimed_events,
            delivered_turns,
            processed_events,
            last_sequence,
        })
    }

    async fn deliver_event(&self, event: &DomainEvent) -> Result<bool> {
        match event.event_type.as_str() {
            "agent.wake.admitted" => self.deliver_wake(event).await,
            "project.execution_baseline.activated" => self.deliver_baseline_activation(event).await,
            _ => Ok(false),
        }
    }

    async fn deliver_wake(&self, event: &DomainEvent) -> Result<bool> {
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(identity_id) = payload.get("identity_id").and_then(Value::as_str) else {
            return Ok(false);
        };
        let scope_type = payload
            .get("scope_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(scope_id) = payload.get("scope_id").and_then(Value::as_str) else {
            return Ok(false);
        };
        let incident_key = payload
            .get("incident_key")
            .and_then(Value::as_str)
            .unwrap_or(event.entity_id.as_str());
        // A retry_exhausted incident about a chat's own turns cannot be fixed
        // by queueing another turn on that same chat — that is a feedback
        // loop. Leave it to the human attention surface.
        if scope_type == "agent_chat" && incident_key.contains(":retry_exhausted:") {
            return Ok(false);
        }

        let Some(chat) = self.chat_for_scope(scope_type, scope_id).await? else {
            tracing::debug!(%scope_type, %scope_id, "admitted wake has no deliverable chat");
            return Ok(false);
        };
        let Some((responder_identity_id, responder_profile_id)) =
            self.responder_for_chat(&chat).await?
        else {
            return Ok(false);
        };
        if responder_identity_id != identity_id {
            // The binding changed between admission and delivery; the new
            // responder did not receive this wake authority.
            tracing::debug!(
                chat_id = %chat.id,
                "admitted wake identity no longer holds the chat binding"
            );
            return Ok(false);
        }

        let content = self.wake_content(incident_key).await?;
        self.admit_turn(
            &chat,
            &responder_identity_id,
            &responder_profile_id,
            &content,
            &format!(
                "wake-turn:{}",
                event.dedupe_key.as_deref().unwrap_or(&event.id)
            ),
            event,
        )
        .await
    }

    async fn deliver_baseline_activation(&self, event: &DomainEvent) -> Result<bool> {
        if event.scope_type != "project" {
            return Ok(false);
        }
        let Some(chat) = self.chat_for_scope("project", &event.scope_id).await? else {
            return Ok(false);
        };
        let Some((responder_identity_id, responder_profile_id)) =
            self.responder_for_chat(&chat).await?
        else {
            return Ok(false);
        };
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let baseline_id = payload
            .pointer("/result/baseline_id")
            .and_then(Value::as_str)
            .unwrap_or(event.entity_id.as_str());
        let revision_id = payload
            .pointer("/result/revision_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let content = format!(
            "### Execution baseline activated\n\n\
             The user approved and activated Execution Baseline `{baseline_id}` \
             (revision `{revision_id}`). Begin execution now:\n\
             1. Ensure every baseline plan item has its Milestone task — create \
             any missing task via your task proposal operation, naming the \
             plan_item_id.\n\
             2. Confirm each task has runnable role assignments.\n\
             3. Start the highest-priority runnable task and keep work moving \
             through review without waiting for further prompts.\n\n\
             Only ask the user when a decision or missing prerequisite \
             genuinely requires them; otherwise decide and proceed."
        );
        self.admit_turn(
            &chat,
            &responder_identity_id,
            &responder_profile_id,
            &content,
            &format!(
                "baseline-turn:{}",
                event.dedupe_key.as_deref().unwrap_or(&event.id)
            ),
            event,
        )
        .await
    }

    async fn chat_for_scope(&self, scope_type: &str, scope_id: &str) -> Result<Option<AgentChat>> {
        let chat = match scope_type {
            "project" => AgentChatRepo::get_project_chat(&*self.db, scope_id).await?,
            "agent_chat" => AgentChatRepo::get_agent_chat(&*self.db, scope_id).await?,
            "account" => AgentChatRepo::get_main_chat(&*self.db, scope_id).await?,
            "task" => {
                let project_id: Option<String> =
                    sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
                        .bind(scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                match project_id {
                    Some(project_id) => {
                        AgentChatRepo::get_project_chat(&*self.db, &project_id).await?
                    }
                    None => None,
                }
            }
            _ => None,
        };
        Ok(chat.filter(|chat| chat.status == "ready"))
    }

    async fn responder_for_chat(&self, chat: &AgentChat) -> Result<Option<(String, String)>> {
        if chat.kind == "project" {
            let Some(project_id) = chat.project_id.as_deref() else {
                return Ok(None);
            };
            let Some(binding) =
                ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id).await?
            else {
                return Ok(None);
            };
            if binding.state != "active" {
                return Ok(None);
            }
            return Ok(binding.identity_id.zip(binding.profile_id));
        }
        if chat.kind == "account_main" {
            let Some(account_id) = chat.account_id.as_deref() else {
                return Ok(None);
            };
            let row = sqlx::query(
                "SELECT identity_id, profile_id FROM account_main_agent_binding
                 WHERE account_id = ? AND state = 'active'",
            )
            .bind(account_id)
            .fetch_optional(self.db.pool())
            .await?;
            return Ok(row.map(|row| {
                (
                    row.get::<String, _>("identity_id"),
                    row.get::<String, _>("profile_id"),
                )
            }));
        }
        Ok(None)
    }

    /// Compose the wake message from the durable Attention incident so the
    /// agent sees the same summary and recommended action the user would.
    async fn wake_content(&self, incident_key: &str) -> Result<String> {
        let incident = sqlx::query(
            "SELECT attention_type, summary, recommended_action, details_json
             FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(incident_key)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(incident) = incident else {
            return Ok(format!(
                "### Attention wake\n\nIncident `{incident_key}` requires your \
                 attention. Assess the current Project state with your tools \
                 and take the action it requires. If a decision belongs to the \
                 user, ask for it in this chat; otherwise proceed."
            ));
        };
        let attention_type: String = incident.get("attention_type");
        let summary: String = incident.get("summary");
        let recommended_action: String = incident.get("recommended_action");
        let details_json: String = incident.get("details_json");
        let details = if details_json.trim().is_empty()
            || details_json.trim() == "{}"
            || details_json.chars().count() > MAX_DETAIL_CHARS
        {
            String::new()
        } else {
            format!("\nDetails: `{details_json}`\n")
        };
        Ok(format!(
            "### Attention wake: {summary}\n\n\
             Category: `{attention_type}` — recommended action: \
             `{recommended_action}`.\nIncident: `{incident_key}`\n{details}\n\
             Assess the current state with your tools and take the action this \
             incident requires — retry or repair failed work, review \
             review-ready work, and keep tasks moving. If a decision genuinely \
             belongs to the user, ask for it in this chat; otherwise decide and \
             proceed."
        ))
    }

    async fn admit_turn(
        &self,
        chat: &AgentChat,
        responder_identity_id: &str,
        responder_profile_id: &str,
        content: &str,
        turn_dedupe_key: &str,
        event: &DomainEvent,
    ) -> Result<bool> {
        let now = now_rfc3339();
        // Deterministic ids keyed by the turn dedupe: replay after a crash
        // between admission and receipt reuses the identical row.
        let message_id = deterministic_uuid(&format!("{turn_dedupe_key}:message"));
        let turn_id = deterministic_uuid(&format!("{turn_dedupe_key}:turn"));
        let admitted = AgentChatTransactionRepo::admit_agent_chat_turn(
            &*self.db,
            AdmitAgentChatTurn {
                message: CreateAgentChatMessage {
                    id: message_id.clone(),
                    chat_id: chat.id.clone(),
                    // The store allocates the real sequence.
                    sequence: 0,
                    author_type: AgentChatMessageAuthorType::System,
                    author_id: None,
                    content: content.to_owned(),
                    content_guard_json: "{}".to_owned(),
                    sensitivity: "internal".to_owned(),
                    status: AgentChatMessageStatus::Complete,
                    outcome: None,
                    model: None,
                    profile_id: None,
                    session_id: None,
                    context_manifest_id: None,
                    token_usage_json: None,
                    duration_ms: None,
                    error: None,
                    correlation_id: event.correlation_id.clone(),
                    causation_id: Some(event.id.clone()),
                    handoff_id: None,
                    source_type: "native".to_owned(),
                    source_id: None,
                    source_message_id: None,
                    source_room_id: None,
                    source_conversation_id: None,
                    source_sequence: None,
                    source_metadata_json: "{}".to_owned(),
                    created_at: now.clone(),
                },
                turn: CreateAgentChatTurnJob {
                    id: turn_id,
                    chat_id: chat.id.clone(),
                    triggering_message_id: message_id,
                    responder_identity_id: responder_identity_id.to_owned(),
                    profile_id: responder_profile_id.to_owned(),
                    canonical_scope_type: "agent_chat".to_owned(),
                    canonical_scope_id: chat.id.clone(),
                    dedupe_key: turn_dedupe_key.to_owned(),
                    max_attempts: MAX_TURN_ATTEMPTS,
                    correlation_id: event.correlation_id.clone(),
                    causation_id: Some(event.id.clone()),
                    causation_depth: event.causation_depth + 1,
                    created_at: now.clone(),
                    updated_at: now,
                },
            },
        )
        .await?;
        tracing::info!(
            chat_id = %chat.id,
            turn_job_id = %admitted.turn.id,
            dedupe = %turn_dedupe_key,
            "delivered wake as agent chat turn"
        );
        Ok(true)
    }
}

/// Stable UUID derived from a dedupe key so replays reuse identical row ids.
fn deterministic_uuid(seed: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes()).to_string()
}

fn lease_until(now: &str) -> String {
    use chrono::{DateTime, Duration, Utc};
    DateTime::parse_from_rfc3339(now)
        .map(|now| now.with_timezone(&Utc) + Duration::seconds(LEASE_SECONDS))
        .map(|until| until.to_rfc3339())
        .unwrap_or_else(|_| now.to_owned())
}

pub fn wake_turn_consumer_name() -> &'static str {
    CONSUMER_NAME
}

pub fn wake_turn_consumer_lease_owner() -> String {
    format!("wake-turn-consumer-{}", new_uuid_v4())
}
