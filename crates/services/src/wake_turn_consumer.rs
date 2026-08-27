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
    AgentChatMessageStatus, AgentChatRepo, AgentWakeDisposition, AgentWakeDispositionKind,
    AgentWakeDispositionRepo, AttentionRepo, ClaimDomainEvents, CompleteClaimedWake,
    CompleteDomainEvent, CreateAgentChatMessage, CreateAgentChatTurnJob,
    CreateAgentWakeDisposition, CreateAttentionProjection, DomainEvent, DomainEventRepo,
    RetryAgentWakeDisposition, SqliteDb,
};
use serde_json::{json, Value};
use sqlx::Row;
use tokio::{sync::watch, task::JoinHandle, time::Duration as TokioDuration};
use uuid::Uuid;

use crate::{
    agent_turn_admission::{
        content_digest, AgentTurnAdmissionService, AgentTurnPrepareInput, AgentTurnReadiness,
        AgentTurnTrigger, PreparedAgentTurnAdmission, ResolvedAgentResponder,
    },
    attention_service::{wake_attention_incident_digest, MAX_WAKE_REACTION_DEPTH},
    Result, ServiceError,
};

const CONSUMER_NAME: &str = "agent-wake-turns";
const LEASE_SECONDS: i64 = 60;
const POLL_INTERVAL: TokioDuration = TokioDuration::from_secs(1);
const MAX_TURN_ATTEMPTS: i64 = 3;
const MAX_DETAIL_CHARS: usize = 2_000;
pub(crate) const DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA: &str =
    "forge.delivery-followup-postcondition/v1";
pub(crate) const DELIVERY_FOLLOWUP_READINESS_EVENT: &str = "milestone.readiness.evaluated";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeTurnRun {
    pub claimed_events: usize,
    pub delivered_turns: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Debug)]
enum WakeDeliveryPlan {
    Admitted {
        disposition: CreateAgentWakeDisposition,
        admission: Box<AdmitAgentChatTurn>,
        expected_attention: Option<db::ExpectedAttentionSnapshot>,
    },
    Suppressed(CreateAgentWakeDisposition),
    Deferred(CreateAgentWakeDisposition),
    SetupRequired(CreateAgentWakeDisposition),
}

impl WakeDeliveryPlan {
    fn disposition(&self) -> &CreateAgentWakeDisposition {
        match self {
            Self::Admitted { disposition, .. }
            | Self::Suppressed(disposition)
            | Self::Deferred(disposition)
            | Self::SetupRequired(disposition) => disposition,
        }
    }
}

struct DispositionSpec<'a> {
    event: &'a DomainEvent,
    attempt: i64,
    max_attempts: i64,
    kind: AgentWakeDispositionKind,
    reason: &'a str,
    incident_key: Option<String>,
    incident_digest: Option<String>,
    attention_id: Option<String>,
    responder: Option<&'a ResolvedAgentResponder>,
    parent_disposition_id: Option<String>,
}

struct DeferredPlanSpec<'a> {
    event: &'a DomainEvent,
    attempt: i64,
    max_attempts: i64,
    parent_id: Option<String>,
    reason: &'a str,
    incident_key: Option<String>,
    incident_digest: Option<String>,
    responder: Option<&'a ResolvedAgentResponder>,
    attention_id: Option<String>,
}

struct DeferredDispositionSpec<'a> {
    consumer_name: &'a str,
    event: &'a DomainEvent,
    attempt: i64,
    max_attempts: i64,
    incident_key: Option<String>,
    incident_digest: Option<String>,
    reason: &'a str,
    retry_at_value: String,
    attention_id: Option<String>,
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

    /// Claim and deliver a bounded batch. Every wake decision is checkpointed
    /// with exactly one durable disposition. Deferred/setup rows are retried
    /// from their immutable lineage on later polls.
    pub async fn run_once(&self, limit: i64) -> Result<WakeTurnRun> {
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
        let mut delivered_turns = 0;
        let mut processed_events = 0;
        for event in &events {
            if is_delivery_event(event) {
                if self.process_claimed_event(event).await? {
                    delivered_turns += 1;
                }
            } else {
                DomainEventRepo::complete_claimed_event(
                    &*self.db,
                    completion_for(&self.consumer_name, &self.lease_owner, event),
                )
                .await?;
            }
            processed_events += 1;
        }

        let retry_rows = AgentWakeDispositionRepo::list_reconsiderable_agent_wake_dispositions(
            &*self.db,
            &self.consumer_name,
            &now_rfc3339(),
            limit.clamp(1, 100),
        )
        .await?;
        for current in retry_rows {
            if self.process_retry(&current).await? {
                delivered_turns += 1;
            }
        }

        let last_sequence = DomainEventRepo::get_consumer_cursor(&*self.db, &self.consumer_name)
            .await?
            .map(|cursor| cursor.last_sequence)
            .unwrap_or(0);

        Ok(WakeTurnRun {
            claimed_events: events.len(),
            delivered_turns,
            processed_events,
            last_sequence,
        })
    }

    async fn process_claimed_event(&self, event: &DomainEvent) -> Result<bool> {
        let plan = match self.plan_event(event, 1, None).await {
            Ok(plan) => plan,
            Err(error) if transient_service_error(&error) => {
                tracing::debug!(event_id = %event.id, %error, "wake evaluation deferred");
                WakeDeliveryPlan::Deferred(deferred_disposition(DeferredDispositionSpec {
                    consumer_name: &self.consumer_name,
                    event,
                    attempt: 1,
                    max_attempts: MAX_TURN_ATTEMPTS,
                    incident_key: None,
                    incident_digest: None,
                    reason: "wake_evaluation_unavailable",
                    retry_at_value: retry_at(&event.created_at, 1),
                    attention_id: None,
                }))
            }
            Err(error) => {
                tracing::debug!(event_id = %event.id, %error, "wake evaluation suppressed");
                WakeDeliveryPlan::Suppressed(self.disposition(DispositionSpec {
                    event,
                    attempt: 1,
                    max_attempts: MAX_TURN_ATTEMPTS,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "wake_evaluation_invalid",
                    incident_key: None,
                    incident_digest: None,
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: None,
                }))
            }
        };
        let delivered = matches!(plan, WakeDeliveryPlan::Admitted { .. });
        let disposition = plan.disposition().clone();
        let (admission, expected_attention) = match plan {
            WakeDeliveryPlan::Admitted {
                admission,
                expected_attention,
                ..
            } => (Some(*admission), expected_attention),
            WakeDeliveryPlan::Suppressed(_)
            | WakeDeliveryPlan::Deferred(_)
            | WakeDeliveryPlan::SetupRequired(_) => (None, None),
        };
        let completion = completion_for(&self.consumer_name, &self.lease_owner, event);
        if let Some(admission) = admission {
            match AgentWakeDispositionRepo::complete_claimed_agent_wake(
                &*self.db,
                CompleteClaimedWake {
                    disposition: disposition.clone(),
                    completion: completion.clone(),
                    admission: Some(admission),
                    expected_attention: expected_attention.clone(),
                },
            )
            .await
            {
                Ok(_) => return Ok(delivered),
                Err(error) if transient_db_error(&error) => {
                    tracing::debug!(event_id = %event.id, %error,
                        "wake admission raced current authority; deferring");
                    let deferred = deferred_disposition(DeferredDispositionSpec {
                        consumer_name: &self.consumer_name,
                        event,
                        attempt: 1,
                        max_attempts: MAX_TURN_ATTEMPTS,
                        incident_key: disposition.incident_key.clone(),
                        incident_digest: disposition.incident_digest.clone(),
                        reason: "turn_admission_unavailable",
                        retry_at_value: retry_at(&event.created_at, 1),
                        attention_id: attention_id_from_provenance(
                            disposition.provenance_json.as_deref(),
                        ),
                    });
                    AgentWakeDispositionRepo::complete_claimed_agent_wake(
                        &*self.db,
                        CompleteClaimedWake {
                            disposition: deferred,
                            completion,
                            admission: None,
                            expected_attention: None,
                        },
                    )
                    .await?;
                    return Ok(false);
                }
                Err(error) => {
                    tracing::debug!(event_id = %event.id, %error,
                        "wake admission rejected deterministically");
                    let suppressed = self.disposition(DispositionSpec {
                        event,
                        attempt: 1,
                        max_attempts: MAX_TURN_ATTEMPTS,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason: "turn_admission_rejected",
                        incident_key: disposition.incident_key.clone(),
                        incident_digest: disposition.incident_digest.clone(),
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: None,
                    });
                    AgentWakeDispositionRepo::complete_claimed_agent_wake(
                        &*self.db,
                        CompleteClaimedWake {
                            disposition: suppressed,
                            completion,
                            admission: None,
                            expected_attention: None,
                        },
                    )
                    .await?;
                    return Ok(false);
                }
            }
        }
        AgentWakeDispositionRepo::complete_claimed_agent_wake(
            &*self.db,
            CompleteClaimedWake {
                disposition,
                completion,
                admission: None,
                expected_attention: None,
            },
        )
        .await?;
        Ok(false)
    }

    async fn process_retry(&self, current: &AgentWakeDisposition) -> Result<bool> {
        let Some(event) = DomainEventRepo::get_event(&*self.db, &current.source_event_id).await?
        else {
            return Err(ServiceError::not_found(
                "domain_event",
                current.source_event_id.clone(),
            ));
        };
        let attempt = current.attempt_number + 1;
        let plan = match self.plan_event(&event, attempt, Some(current)).await {
            Ok(plan) => plan,
            Err(error) if transient_service_error(&error) => {
                tracing::debug!(event_id = %event.id, attempt, %error, "wake retry evaluation deferred");
                retry_failure_plan(&self.consumer_name, &event, current, attempt)
            }
            Err(error) => {
                tracing::debug!(event_id = %event.id, attempt, %error, "wake retry evaluation suppressed");
                WakeDeliveryPlan::Suppressed(self.disposition(DispositionSpec {
                    event: &event,
                    attempt,
                    max_attempts: current.max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "wake_evaluation_invalid",
                    incident_key: current.incident_key.clone(),
                    incident_digest: current.incident_digest.clone(),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: Some(current.id.clone()),
                }))
            }
        };
        let delivered = matches!(plan, WakeDeliveryPlan::Admitted { .. });
        let disposition = plan.disposition().clone();
        let (admission, expected_attention) = match plan {
            WakeDeliveryPlan::Admitted {
                admission,
                expected_attention,
                ..
            } => (Some(*admission), expected_attention),
            WakeDeliveryPlan::Suppressed(_)
            | WakeDeliveryPlan::Deferred(_)
            | WakeDeliveryPlan::SetupRequired(_) => (None, None),
        };
        let now = now_rfc3339();
        if let Some(admission) = admission {
            match AgentWakeDispositionRepo::retry_agent_wake(
                &*self.db,
                RetryAgentWakeDisposition {
                    disposition: disposition.clone(),
                    expected_parent_id: current.id.clone(),
                    now: now.clone(),
                    admission: Some(admission),
                    expected_attention: expected_attention.clone(),
                },
            )
            .await
            {
                Ok(_) => return Ok(delivered),
                Err(error) if transient_db_error(&error) => {
                    tracing::debug!(event_id = %event.id, attempt, %error,
                        "wake retry admission raced current authority");
                    let fallback = retry_failure_disposition(
                        &self.consumer_name,
                        &event,
                        current,
                        attempt,
                        &now,
                    );
                    AgentWakeDispositionRepo::retry_agent_wake(
                        &*self.db,
                        RetryAgentWakeDisposition {
                            disposition: fallback,
                            expected_parent_id: current.id.clone(),
                            now,
                            admission: None,
                            expected_attention: None,
                        },
                    )
                    .await?;
                    return Ok(false);
                }
                Err(error) => {
                    tracing::debug!(event_id = %event.id, attempt, %error,
                        "wake retry admission rejected deterministically");
                    let suppressed = self.disposition(DispositionSpec {
                        event: &event,
                        attempt,
                        max_attempts: current.max_attempts,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason: "turn_admission_rejected",
                        incident_key: current.incident_key.clone(),
                        incident_digest: current.incident_digest.clone(),
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: Some(current.id.clone()),
                    });
                    match AgentWakeDispositionRepo::retry_agent_wake(
                        &*self.db,
                        RetryAgentWakeDisposition {
                            disposition: suppressed,
                            expected_parent_id: current.id.clone(),
                            now,
                            admission: None,
                            expected_attention: None,
                        },
                    )
                    .await
                    {
                        Ok(_) | Err(db::DbError::VersionConflict) => return Ok(false),
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        AgentWakeDispositionRepo::retry_agent_wake(
            &*self.db,
            RetryAgentWakeDisposition {
                disposition,
                expected_parent_id: current.id.clone(),
                now,
                admission: None,
                expected_attention: None,
            },
        )
        .await?;
        Ok(false)
    }

    async fn plan_event(
        &self,
        event: &DomainEvent,
        attempt: i64,
        parent: Option<&AgentWakeDisposition>,
    ) -> Result<WakeDeliveryPlan> {
        let max_attempts = parent.map_or(MAX_TURN_ATTEMPTS, |value| value.max_attempts);
        let parent_id = parent.map(|value| value.id.clone());
        let plan = match event.event_type.as_str() {
            "agent.wake.admitted" | "agent.wake.setup_required" => {
                self.plan_wake(event, attempt, max_attempts, parent_id)
                    .await?
            }
            "agent.wake.suppressed" => {
                let payload =
                    serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
                let reason = payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("attention_policy_suppressed");
                WakeDeliveryPlan::Suppressed(
                    self.disposition(DispositionSpec {
                        event,
                        attempt,
                        max_attempts,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason,
                        incident_key: payload
                            .get("incident_key")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        incident_digest: payload
                            .get("incident_digest")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: parent_id,
                    }),
                )
            }
            "project.execution_baseline.activated" => {
                self.plan_baseline(event, attempt, max_attempts, parent_id)
                    .await?
            }
            _ => WakeDeliveryPlan::Suppressed(self.disposition(DispositionSpec {
                event,
                attempt,
                max_attempts,
                kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                reason: "malformed_wake_decision",
                incident_key: None,
                incident_digest: None,
                attention_id: None,
                responder: None,
                parent_disposition_id: parent_id,
            })),
        };
        if attempt >= max_attempts
            && matches!(
                plan,
                WakeDeliveryPlan::Deferred(_) | WakeDeliveryPlan::SetupRequired(_)
            )
        {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "wake_retry_exhausted",
                    incident_key: plan.disposition().incident_key.clone(),
                    incident_digest: plan.disposition().incident_digest.clone(),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: plan.disposition().parent_disposition_id.clone(),
                },
            )));
        }
        Ok(plan)
    }

    async fn plan_wake(
        &self,
        event: &DomainEvent,
        attempt: i64,
        max_attempts: i64,
        parent_id: Option<String>,
    ) -> Result<WakeDeliveryPlan> {
        let payload = match serde_json::from_str::<Value>(&event.payload_json) {
            Ok(Value::Object(payload)) => Value::Object(payload),
            _ => {
                return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                    DispositionSpec {
                        event,
                        attempt,
                        max_attempts,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason: "malformed_wake_payload",
                        incident_key: None,
                        incident_digest: None,
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: parent_id,
                    },
                )))
            }
        };
        let fields = match parse_wake_fields(event, &payload) {
            Ok(fields) => fields,
            Err(reason) => {
                return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                    DispositionSpec {
                        event,
                        attempt,
                        max_attempts,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason,
                        incident_key: None,
                        incident_digest: None,
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: parent_id,
                    },
                )))
            }
        };
        if fields.reaction_depth < 0
            || fields.reaction_depth >= MAX_WAKE_REACTION_DEPTH
            || event.causation_depth < 0
            || event.causation_depth > MAX_WAKE_REACTION_DEPTH
        {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "reaction_depth_exceeded",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    attention_id: fields.attention_id,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        if is_self_event(event, &payload, fields.identity_id.as_deref()) {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "self_event",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    attention_id: fields.attention_id,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        if fields.scope_type == "agent_chat" && fields.incident_key.contains(":retry_exhausted:") {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "recursive_agent_response",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    attention_id: fields.attention_id,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        let Some(attention_id) = fields.attention_id.clone() else {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "attention_reference_missing",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        };
        let attention = match AttentionRepo::get_attention(&*self.db, &attention_id).await {
            Ok(Some(attention)) => attention,
            Ok(None) => {
                return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                    DispositionSpec {
                        event,
                        attempt,
                        max_attempts,
                        kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                        reason: "attention_missing",
                        incident_key: Some(fields.incident_key),
                        incident_digest: Some(fields.incident_digest),
                        attention_id: None,
                        responder: None,
                        parent_disposition_id: parent_id,
                    },
                )))
            }
            Err(_) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "attention_unavailable",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    responder: None,
                    attention_id: None,
                }))
            }
        };
        let scope_matches = match self.attention_matches_scope(&attention, &fields).await {
            Ok(matches) => matches,
            Err(_) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "attention_scope_unavailable",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(fields.incident_digest),
                    responder: None,
                    attention_id: Some(attention.id),
                }))
            }
        };
        if !scope_matches || attention.dedupe_key != fields.incident_key {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "cross_scope_incident",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(wake_attention_incident_digest(&attention)),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        let incident_digest = wake_attention_incident_digest(&attention);
        if attention.status == "resolved" {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "resolved_incident",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(incident_digest),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        if fields.incident_digest != incident_digest {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "attention_changed",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(incident_digest),
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        let Some(chat) = self
            .chat_for_scope(&fields.scope_type, &fields.scope_id)
            .await?
        else {
            return Ok(self.deferred_plan(DeferredPlanSpec {
                event,
                attempt,
                max_attempts,
                parent_id,
                reason: "chat_unavailable",
                incident_key: Some(fields.incident_key),
                incident_digest: Some(incident_digest),
                responder: None,
                attention_id: Some(attention.id),
            }));
        };
        let service = AgentTurnAdmissionService::new(Arc::clone(&self.db));
        let responder = match service.resolve(&chat).await {
            Ok(responder) => responder,
            Err(_) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "responder_resolution_unavailable",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(incident_digest),
                    responder: None,
                    attention_id: Some(attention.id),
                }))
            }
        };
        match responder.readiness {
            AgentTurnReadiness::SetupRequired => {
                return Ok(WakeDeliveryPlan::SetupRequired(self.disposition(
                    DispositionSpec {
                        event,
                        attempt,
                        max_attempts,
                        kind: AgentWakeDispositionKind::SetupRequired,
                        reason: "responder_binding_missing",
                        incident_key: Some(fields.incident_key),
                        incident_digest: Some(incident_digest),
                        attention_id: Some(attention.id),
                        responder: Some(&responder),
                        parent_disposition_id: parent_id,
                    },
                )))
            }
            AgentTurnReadiness::Unavailable => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "responder_unavailable",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(incident_digest),
                    responder: Some(&responder),
                    attention_id: Some(attention.id),
                }))
            }
            AgentTurnReadiness::Ready => {}
        }
        let content = wake_content(&attention);
        let digest = content_digest(&content)
            .map_err(|_| ServiceError::invalid_operation("wake content digest failed"))?;
        let dedupe_key = format!("wake-turn:{}", event.id);
        let prepared = match service
            .prepare(AgentTurnPrepareInput {
                chat: &chat,
                trigger: AgentTurnTrigger::AutonomousWake,
                dedupe_key: &dedupe_key,
                content_digest: &digest,
                causation_id: Some(&event.id),
                causation_depth: event
                    .causation_depth
                    .saturating_add(1)
                    .min(MAX_WAKE_REACTION_DEPTH),
                source_responder: None,
            })
            .await
        {
            Ok(prepared) => prepared,
            Err(_error) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "turn_admission_unavailable",
                    incident_key: Some(fields.incident_key),
                    incident_digest: Some(incident_digest),
                    responder: None,
                    attention_id: Some(attention.id),
                }));
            }
        };
        let admission =
            self.build_turn_admission(event, chat, content, prepared.clone(), Some(&attention))?;
        let mut disposition = self.disposition(DispositionSpec {
            event,
            attempt,
            max_attempts,
            kind: AgentWakeDispositionKind::TurnAdmitted,
            reason: "turn_admitted",
            incident_key: Some(fields.incident_key),
            incident_digest: Some(incident_digest),
            attention_id: None,
            responder: None,
            parent_disposition_id: parent_id,
        });
        disposition.turn_job_id = Some(admission.turn.id.clone());
        disposition.binding_id = admission.turn.responder_binding_id.clone();
        disposition.binding_version = admission.turn.responder_binding_version;
        disposition.profile_id = Some(admission.turn.profile_id.clone());
        disposition.profile_version = admission.turn.profile_version;
        disposition.provenance_json = Some(prepared.canonical_scope_provenance_json);
        Ok(WakeDeliveryPlan::Admitted {
            disposition,
            admission: Box::new(admission),
            expected_attention: Some(expected_attention_snapshot(&attention)),
        })
    }

    async fn plan_baseline(
        &self,
        event: &DomainEvent,
        attempt: i64,
        max_attempts: i64,
        parent_id: Option<String>,
    ) -> Result<WakeDeliveryPlan> {
        if event.scope_type != "project" {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "ineligible_scope",
                    incident_key: None,
                    incident_digest: None,
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        }
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(baseline_id) = payload
            .pointer("/result/baseline_id")
            .and_then(Value::as_str)
        else {
            return Ok(WakeDeliveryPlan::Suppressed(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::DeterministicallySuppressed,
                    reason: "malformed_baseline_activation",
                    incident_key: None,
                    incident_digest: None,
                    attention_id: None,
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        };
        let revision_id = payload
            .pointer("/result/revision_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let Some(chat) = self.chat_for_scope("project", &event.scope_id).await? else {
            let attention = self
                .ensure_setup_attention(event, None, "project_chat_missing")
                .await?;
            return Ok(WakeDeliveryPlan::SetupRequired(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::SetupRequired,
                    reason: "project_chat_setup_required",
                    incident_key: Some(format!("baseline_activation:{}", event.id)),
                    incident_digest: None,
                    attention_id: Some(attention.id),
                    responder: None,
                    parent_disposition_id: parent_id,
                },
            )));
        };
        let content = format!(
            "Traceability plan {baseline_id} (revision {revision_id}) is now active. The approved Charter already authorizes implementation. Refresh optional plan-item links, confirm Task assignments and workflows, then continue the highest-priority runnable Task."
        );
        let digest = content_digest(&content)
            .map_err(|_| ServiceError::invalid_operation("baseline content digest failed"))?;
        let service = AgentTurnAdmissionService::new(Arc::clone(&self.db));
        let responder = match service.resolve(&chat).await {
            Ok(responder) => responder,
            Err(_) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "responder_resolution_unavailable",
                    incident_key: Some(format!("baseline_activation:{}", event.id)),
                    incident_digest: None,
                    responder: None,
                    attention_id: None,
                }))
            }
        };
        if responder.readiness == AgentTurnReadiness::SetupRequired {
            let attention = self
                .ensure_setup_attention(
                    event,
                    responder.identity_id.clone(),
                    "project_responder_setup_required",
                )
                .await?;
            return Ok(WakeDeliveryPlan::SetupRequired(self.disposition(
                DispositionSpec {
                    event,
                    attempt,
                    max_attempts,
                    kind: AgentWakeDispositionKind::SetupRequired,
                    reason: "responder_setup_required",
                    incident_key: Some(format!("baseline_activation:{}", event.id)),
                    incident_digest: None,
                    attention_id: Some(attention.id),
                    responder: Some(&responder),
                    parent_disposition_id: parent_id,
                },
            )));
        }
        if responder.readiness == AgentTurnReadiness::Unavailable {
            return Ok(self.deferred_plan(DeferredPlanSpec {
                event,
                attempt,
                max_attempts,
                parent_id,
                reason: "responder_unavailable",
                incident_key: Some(format!("baseline_activation:{}", event.id)),
                incident_digest: None,
                responder: Some(&responder),
                attention_id: None,
            }));
        }
        let dedupe_key = format!("baseline-turn:{}", event.id);
        let prepared = match service
            .prepare(AgentTurnPrepareInput {
                chat: &chat,
                trigger: AgentTurnTrigger::BaselineActivation,
                dedupe_key: &dedupe_key,
                content_digest: &digest,
                causation_id: Some(&event.id),
                causation_depth: event
                    .causation_depth
                    .saturating_add(1)
                    .min(MAX_WAKE_REACTION_DEPTH),
                source_responder: None,
            })
            .await
        {
            Ok(prepared) => prepared,
            Err(_) => {
                return Ok(self.deferred_plan(DeferredPlanSpec {
                    event,
                    attempt,
                    max_attempts,
                    parent_id,
                    reason: "turn_admission_unavailable",
                    incident_key: Some(format!("baseline_activation:{}", event.id)),
                    incident_digest: None,
                    responder: Some(&responder),
                    attention_id: None,
                }))
            }
        };
        let admission = self.build_turn_admission(event, chat, content, prepared.clone(), None)?;
        let mut disposition = self.disposition(DispositionSpec {
            event,
            attempt,
            max_attempts,
            kind: AgentWakeDispositionKind::TurnAdmitted,
            reason: "turn_admitted",
            incident_key: Some(format!("baseline_activation:{}", event.id)),
            incident_digest: None,
            attention_id: None,
            responder: None,
            parent_disposition_id: parent_id,
        });
        disposition.turn_job_id = Some(admission.turn.id.clone());
        disposition.binding_id = admission.turn.responder_binding_id.clone();
        disposition.binding_version = admission.turn.responder_binding_version;
        disposition.profile_id = Some(admission.turn.profile_id.clone());
        disposition.profile_version = admission.turn.profile_version;
        disposition.provenance_json = Some(prepared.canonical_scope_provenance_json);
        Ok(WakeDeliveryPlan::Admitted {
            disposition,
            admission: Box::new(admission),
            expected_attention: None,
        })
    }

    fn build_turn_admission(
        &self,
        event: &DomainEvent,
        chat: AgentChat,
        content: String,
        prepared: PreparedAgentTurnAdmission,
        attention: Option<&db::AttentionProjection>,
    ) -> Result<AdmitAgentChatTurn> {
        let message_id = deterministic_uuid(&format!("{}:message", prepared.dedupe_key));
        let turn_id = deterministic_uuid(&format!("{}:turn", prepared.dedupe_key));
        let now = now_rfc3339();
        let base_turn = CreateAgentChatTurnJob {
            id: turn_id,
            chat_id: chat.id.clone(),
            triggering_message_id: message_id.clone(),
            responder_identity_id: prepared.responder.identity_id()?.to_owned(),
            profile_id: prepared.responder.profile_id()?.to_owned(),
            responder_binding_id: None,
            responder_binding_version: None,
            responder_identity_version: None,
            profile_version: None,
            operating_skill_revision_id: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
            admission_digest: None,
            canonical_scope_provenance_json: None,
            canonical_scope_type: "agent_chat".to_owned(),
            canonical_scope_id: chat.id.clone(),
            dedupe_key: prepared.dedupe_key.clone(),
            max_attempts: MAX_TURN_ATTEMPTS,
            correlation_id: event.correlation_id.clone(),
            causation_id: Some(event.id.clone()),
            causation_depth: event.causation_depth.saturating_add(1),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let turn = prepared.apply_to_turn(base_turn)?;
        let source_metadata_json =
            match attention.filter(|attention| attention.attention_type == "delivery_followup") {
                Some(attention) => json!({
                    "wake_event_id": event.id,
                    "wake_event_type": event.event_type,
                    "turn_postcondition": {
                        "schema_version": DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA,
                        "attention_id": attention.id,
                        "required_event_type": DELIVERY_FOLLOWUP_READINESS_EVENT,
                        "required_scope_type": attention.scope_type,
                        "required_scope_id": attention.scope_id,
                        "after_event_sequence": event.sequence,
                    }
                }),
                None => json!({
                    "wake_event_id": event.id,
                    "wake_event_type": event.event_type
                }),
            };
        let message = CreateAgentChatMessage {
            id: message_id,
            chat_id: chat.id,
            sequence: 0,
            author_type: AgentChatMessageAuthorType::System,
            author_id: None,
            content,
            content_guard_json: "{}".to_owned(),
            sensitivity: "internal".to_owned(),
            status: AgentChatMessageStatus::Complete,
            outcome: None,
            model: None,
            profile_id: Some(turn.profile_id.clone()),
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: event.correlation_id.clone(),
            causation_id: Some(event.id.clone()),
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: Some(event.id.clone()),
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: Some(event.sequence),
            source_metadata_json: source_metadata_json.to_string(),
            created_at: now,
        };
        Ok(AdmitAgentChatTurn { message, turn })
    }

    fn disposition(&self, spec: DispositionSpec<'_>) -> CreateAgentWakeDisposition {
        let DispositionSpec {
            event,
            attempt,
            max_attempts,
            kind,
            reason,
            incident_key,
            incident_digest,
            attention_id,
            responder,
            parent_disposition_id,
        } = spec;
        let now = now_rfc3339();
        CreateAgentWakeDisposition {
            id: deterministic_uuid(&format!(
                "wake-disposition:{}:{}:{}",
                self.consumer_name, event.id, attempt
            )),
            consumer_name: self.consumer_name.clone(),
            source_event_id: event.id.clone(),
            source_event_sequence: event.sequence,
            attempt_number: attempt,
            max_attempts,
            disposition: kind,
            reason: bounded_reason(reason),
            turn_job_id: None,
            attention_id: (kind == AgentWakeDispositionKind::SetupRequired)
                .then_some(attention_id)
                .flatten(),
            retry_at: None,
            incident_key: incident_key.map(|value| bounded_value(&value, 256)),
            incident_digest: incident_digest.map(|value| bounded_value(&value, 256)),
            binding_id: responder.and_then(|value| value.binding_id.clone()),
            binding_version: responder.and_then(|value| value.binding_version),
            profile_id: responder.and_then(|value| value.profile_id.clone()),
            profile_version: responder.and_then(|value| value.profile_version),
            provenance_json: None,
            parent_disposition_id,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    async fn attention_matches_scope(
        &self,
        attention: &db::AttentionProjection,
        fields: &WakeFields,
    ) -> Result<bool> {
        let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
        if details.get("scope_type").and_then(Value::as_str) != Some(fields.scope_type.as_str())
            || details.get("scope_id").and_then(Value::as_str) != Some(fields.scope_id.as_str())
        {
            return Ok(false);
        }
        match fields.scope_type.as_str() {
            "project" => {
                Ok(attention.scope_type == "project" && attention.scope_id == fields.scope_id)
            }
            "account" => {
                Ok(attention.scope_type == "account" && attention.scope_id == fields.scope_id)
            }
            "task" => {
                let project_id =
                    sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                        .bind(&fields.scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                Ok(project_id.is_some_and(|project_id| {
                    attention.scope_type == "project" && attention.scope_id == project_id
                }))
            }
            "agent_chat" => {
                let row =
                    sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                        .bind(&fields.scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                let Some(row) = row else {
                    return Ok(false);
                };
                let kind: String = row.try_get("kind")?;
                let account_id: Option<String> = row.try_get("account_id")?;
                let project_id: Option<String> = row.try_get("project_id")?;
                Ok(match kind.as_str() {
                    "account_main" => {
                        attention.scope_type == "account"
                            && attention.scope_id == account_id.unwrap_or_default()
                    }
                    "project" => {
                        attention.scope_type == "project"
                            && attention.scope_id == project_id.unwrap_or_default()
                    }
                    _ => false,
                })
            }
            _ => Ok(false),
        }
    }

    async fn chat_for_scope(&self, scope_type: &str, scope_id: &str) -> Result<Option<AgentChat>> {
        let chat = match scope_type {
            "project" => AgentChatRepo::get_project_chat(&*self.db, scope_id).await?,
            "agent_chat" => AgentChatRepo::get_agent_chat(&*self.db, scope_id).await?,
            "account" => AgentChatRepo::get_main_chat(&*self.db, scope_id).await?,
            "task" => {
                let project_id =
                    sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
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
        Ok(chat)
    }

    async fn ensure_setup_attention(
        &self,
        event: &DomainEvent,
        identity_id: Option<String>,
        reason: &str,
    ) -> Result<db::AttentionProjection> {
        let dedupe_key = format!(
            "attention:agent_setup:project:{}:baseline_activation:{}",
            event.scope_id, event.id
        );
        AttentionRepo::insert_attention(
            &*self.db,
            CreateAttentionProjection {
                id: deterministic_uuid(&dedupe_key),
                attention_type: "agent_setup_required".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: event.scope_id.clone(),
                identity_id,
                source_event_id: event.id.clone(),
                priority: 100,
                status: "open".to_owned(),
                summary: "Project Agent setup is required before execution can start".to_owned(),
                details_json: json!({
                    "source_event_id": event.id,
                    "source_sequence": event.sequence,
                    "scope_type": "project",
                    "scope_id": event.scope_id,
                    "reason": reason,
                })
                .to_string(),
                dedupe_key,
                occurred_at: event.created_at.clone(),
                updated_at: now_rfc3339(),
                acknowledged_at: None,
                snoozed_until: None,
                resolved_at: None,
                updated_by_user_id: None,
                recommended_action: "Configure the active Project Agent binding and Profile."
                    .to_owned(),
                source_sequence: Some(event.sequence),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    fn deferred_plan(&self, spec: DeferredPlanSpec<'_>) -> WakeDeliveryPlan {
        let DeferredPlanSpec {
            event,
            attempt,
            max_attempts,
            parent_id,
            reason,
            incident_key,
            incident_digest,
            responder,
            attention_id,
        } = spec;
        let mut disposition = self.disposition(DispositionSpec {
            event,
            attempt,
            max_attempts,
            kind: AgentWakeDispositionKind::Deferred,
            reason,
            incident_key,
            incident_digest,
            attention_id: None,
            responder,
            parent_disposition_id: parent_id,
        });
        if let Some(attention_id) = attention_id {
            disposition.provenance_json = Some(json!({ "attention_id": attention_id }).to_string());
        }
        disposition.retry_at = Some(retry_at(&event.created_at, attempt));
        WakeDeliveryPlan::Deferred(disposition)
    }
}

#[derive(Debug)]
struct WakeFields {
    scope_type: String,
    scope_id: String,
    incident_key: String,
    incident_digest: String,
    attention_id: Option<String>,
    identity_id: Option<String>,
    reaction_depth: i64,
}

fn is_delivery_event(event: &DomainEvent) -> bool {
    event.event_type.starts_with("agent.wake.")
        || event.event_type == "project.execution_baseline.activated"
}

fn completion_for(
    consumer_name: &str,
    lease_owner: &str,
    event: &DomainEvent,
) -> CompleteDomainEvent {
    CompleteDomainEvent {
        consumer_name: consumer_name.to_owned(),
        lease_owner: lease_owner.to_owned(),
        event_sequence: event.sequence,
        event_id: event.id.clone(),
        dedupe_key: crate::domain_event_service::event_completion_dedupe_key(event),
        completed_at: now_rfc3339(),
    }
}

fn expected_attention_snapshot(
    attention: &db::AttentionProjection,
) -> db::ExpectedAttentionSnapshot {
    db::ExpectedAttentionSnapshot {
        id: attention.id.clone(),
        version: attention.version,
        digest: Some(wake_attention_incident_digest(attention)),
        status: attention.status.clone(),
        canonical_scope_type: attention.scope_type.clone(),
        canonical_scope_id: attention.scope_id.clone(),
        source_event_id: attention.source_event_id.clone(),
        source_sequence: attention.source_sequence,
        dedupe_key: attention.dedupe_key.clone(),
    }
}

fn parse_wake_fields(
    event: &DomainEvent,
    payload: &Value,
) -> std::result::Result<WakeFields, &'static str> {
    let required = |key: &'static str| {
        payload
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or(key)
    };
    let scope_type = required("scope_type").map_err(|_| "scope_type_missing")?;
    let scope_id = required("scope_id").map_err(|_| "scope_id_missing")?;
    if scope_type != event.scope_type || scope_id != event.scope_id {
        return Err("cross_scope_event");
    }
    let incident_key = required("incident_key").map_err(|_| "incident_key_missing")?;
    let incident_digest = required("incident_digest").map_err(|_| "incident_digest_missing")?;
    let reaction_depth = payload
        .get("reaction_depth")
        .and_then(Value::as_i64)
        .ok_or("reaction_depth_missing")?;
    Ok(WakeFields {
        scope_type,
        scope_id,
        incident_key,
        incident_digest,
        reaction_depth,
        attention_id: payload
            .get("attention_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned),
        identity_id: payload
            .get("identity_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned),
    })
}

fn is_self_event(event: &DomainEvent, payload: &Value, identity_id: Option<&str>) -> bool {
    let Some(identity_id) = identity_id else {
        return false;
    };
    payload
        .get("caused_by_identity_id")
        .or_else(|| payload.get("actor_identity_id"))
        .and_then(Value::as_str)
        == Some(identity_id)
        || payload.get("source_event_id").and_then(Value::as_str) == Some(event.id.as_str())
        || event.causation_id.as_deref() == Some(event.id.as_str())
}

fn wake_content(attention: &db::AttentionProjection) -> String {
    let details = if attention.details_json.trim().is_empty()
        || attention.details_json.trim() == "{}"
        || attention.details_json.chars().count() > MAX_DETAIL_CHARS
    {
        String::new()
    } else {
        format!("\nDetails: {}\n", attention.details_json)
    };
    let delivery_requirement = if attention.attention_type == "delivery_followup" {
        "\nThis delivery follow-up cannot complete from narration. Before replying, invoke `project.readiness` for the applicable milestone even when the canonical result will be blocked, failed, or stale. Do not infer validation, evidence, readiness, or release from Task completion alone.\n"
    } else {
        ""
    };
    format!(
        "### Attention wake: {}\n\nCategory: {} — recommended action: {}.\nIncident: {}{}\nAssess the current state with your tools and take the action this incident requires. If a decision genuinely belongs to the user, ask for it; otherwise proceed.",
        attention.summary,
        attention.attention_type,
        attention.recommended_action,
        attention.dedupe_key,
        format_args!("{details}{delivery_requirement}")
    )
}

fn retry_at(base: &str, attempt: i64) -> String {
    use chrono::{DateTime, Duration, Utc};
    let shift = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let seconds = 5_i64.saturating_mul(1_i64.checked_shl(shift).unwrap_or(64));
    DateTime::parse_from_rfc3339(base)
        .map(|value| value.with_timezone(&Utc) + Duration::seconds(seconds))
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|_| base.to_owned())
}

fn bounded_reason(reason: &str) -> String {
    reason.chars().take(512).collect()
}

fn bounded_value(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn attention_id_from_provenance(provenance: Option<&str>) -> Option<String> {
    provenance
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| {
            value
                .get("attention_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn deferred_disposition(spec: DeferredDispositionSpec<'_>) -> CreateAgentWakeDisposition {
    let DeferredDispositionSpec {
        consumer_name,
        event,
        attempt,
        max_attempts,
        incident_key,
        incident_digest,
        reason,
        retry_at_value,
        attention_id,
    } = spec;
    CreateAgentWakeDisposition {
        id: deterministic_uuid(&format!(
            "wake-disposition:{}:{}:{}",
            consumer_name, event.id, attempt
        )),
        consumer_name: consumer_name.to_owned(),
        source_event_id: event.id.clone(),
        source_event_sequence: event.sequence,
        attempt_number: attempt,
        max_attempts,
        disposition: AgentWakeDispositionKind::Deferred,
        reason: bounded_reason(reason),
        turn_job_id: None,
        attention_id: None,
        retry_at: Some(retry_at_value),
        incident_key: incident_key.map(|value| bounded_value(&value, 256)),
        incident_digest: incident_digest.map(|value| bounded_value(&value, 256)),
        binding_id: None,
        binding_version: None,
        profile_id: None,
        profile_version: None,
        provenance_json: attention_id.map(|value| json!({ "attention_id": value }).to_string()),
        parent_disposition_id: None,
        created_at: event.created_at.clone(),
        updated_at: event.created_at.clone(),
    }
}

fn retry_failure_disposition(
    consumer_name: &str,
    event: &DomainEvent,
    current: &AgentWakeDisposition,
    attempt: i64,
    _now: &str,
) -> CreateAgentWakeDisposition {
    if attempt >= current.max_attempts {
        return CreateAgentWakeDisposition {
            id: deterministic_uuid(&format!(
                "wake-disposition:{}:{}:{}",
                consumer_name, event.id, attempt
            )),
            consumer_name: consumer_name.to_owned(),
            source_event_id: event.id.clone(),
            source_event_sequence: event.sequence,
            attempt_number: attempt,
            max_attempts: current.max_attempts,
            disposition: AgentWakeDispositionKind::DeterministicallySuppressed,
            reason: "wake_retry_exhausted".to_owned(),
            turn_job_id: None,
            attention_id: None,
            retry_at: None,
            incident_key: current.incident_key.clone(),
            incident_digest: current.incident_digest.clone(),
            binding_id: None,
            binding_version: None,
            profile_id: None,
            profile_version: None,
            provenance_json: current.provenance_json.clone(),
            parent_disposition_id: Some(current.id.clone()),
            created_at: event.created_at.clone(),
            updated_at: event.created_at.clone(),
        };
    }
    let mut result = deferred_disposition(DeferredDispositionSpec {
        consumer_name,
        event,
        attempt,
        max_attempts: current.max_attempts,
        incident_key: current.incident_key.clone(),
        incident_digest: current.incident_digest.clone(),
        reason: "turn_admission_unavailable",
        retry_at_value: retry_at(&event.created_at, attempt),
        attention_id: attention_id_from_provenance(current.provenance_json.as_deref()),
    });
    result.parent_disposition_id = Some(current.id.clone());
    result
}

fn retry_failure_plan(
    consumer_name: &str,
    event: &DomainEvent,
    current: &AgentWakeDisposition,
    attempt: i64,
) -> WakeDeliveryPlan {
    let disposition = retry_failure_disposition(consumer_name, event, current, attempt, "");
    match disposition.disposition {
        AgentWakeDispositionKind::Deferred => WakeDeliveryPlan::Deferred(disposition),
        AgentWakeDispositionKind::DeterministicallySuppressed => {
            WakeDeliveryPlan::Suppressed(disposition)
        }
        AgentWakeDispositionKind::TurnAdmitted | AgentWakeDispositionKind::SetupRequired => {
            WakeDeliveryPlan::Suppressed(CreateAgentWakeDisposition {
                disposition: AgentWakeDispositionKind::DeterministicallySuppressed,
                reason: "wake_retry_invalid".to_owned(),
                ..disposition
            })
        }
    }
}

fn transient_service_error(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::DependencyGate
            | ServiceError::Db(
                db::DbError::Sqlx(_) | db::DbError::VersionConflict | db::DbError::NotFound
            )
    )
}

fn transient_db_error(error: &db::DbError) -> bool {
    matches!(
        error,
        db::DbError::Sqlx(_)
            | db::DbError::VersionConflict
            | db::DbError::NotFound
            | db::DbError::DependencyGate
    )
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
