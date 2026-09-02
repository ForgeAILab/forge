use std::time::Duration as StdDuration;
use std::{collections::BTreeSet, sync::Arc};

use api_types::{
    AgentBindingSummary, AgentContinuityHealth, AgentDetailResponse, AgentScopeSummary,
    AgentSessionSummary, AgentUsageSummary, AttentionCategory, AttentionConsumerHealthResponse,
    AttentionItem, AttentionLifecycle, MissionControlAgentHealth, MissionControlCapacity,
    MissionControlCoordinationActivity, MissionControlHomeResponse, MissionControlRecentOutcome,
    MissionControlWorkItem,
};
use chrono::{DateTime, Duration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AgentContextScopeRepo, AgentRepo, AttentionListQuery,
    AttentionProjection, AttentionRepo, ClaimDomainEvents, CompleteDomainEvent,
    CreateAttentionProjection, CreateDomainEvent, DomainEvent, DomainEventRepo,
    EventConsumerCursor, Page, PageRequest, ProjectMemberRepo, ProjectRepo, SqliteDb,
    UpdateAttentionLifecycle, UpsertAttentionConsumerHealth,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteRow, Row, Sqlite, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{Result, ServiceError};

const CONSUMER_NAME: &str = "attention_projection";
const CONSUMER_LEASE_SECONDS: i64 = 30;
const CONSUMER_STALE_SECONDS: i64 = 90;
const MAX_ATTENTION_SUMMARY_LEN: usize = 160;
const PROJECTION_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
const WAKE_LEASE_SECONDS: i64 = 60;
const WAKE_COOLDOWN_SECONDS: i64 = 300;
/// Maximum causal hop count for an admitted autonomous wake.  A wake decision
/// consumes the next hop, so depth 8 is terminally suppressed rather than
/// admitted with a lease depth the next turn would exceed.
pub const MAX_WAKE_REACTION_DEPTH: i64 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionProjectionRun {
    pub claimed_events: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeAdmissionRequest {
    pub identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub incident_key: String,
    pub lease_owner: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub caused_by_identity_id: Option<String>,
    pub reaction_depth: i64,
    pub now: String,
    pub lease_seconds: i64,
    pub cooldown_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeAdmissionResult {
    Admitted {
        leased_until: String,
        cooldown_until: String,
        budget_remaining: Option<i64>,
    },
    Suppressed {
        reason: WakeSuppressionReason,
    },
    SetupRequired {
        reason: WakeSetupReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeSuppressionReason {
    DuplicateIncident,
    Cooldown,
    BudgetExhausted,
    ReactionDepthExceeded,
    SelfEvent,
    RecursiveAgentResponse,
    IneligibleScope,
    ResolvedIncident,
}

impl WakeSuppressionReason {
    fn code(&self) -> &'static str {
        match self {
            Self::DuplicateIncident => "duplicate_incident",
            Self::Cooldown => "cooldown",
            Self::BudgetExhausted => "budget_exhausted",
            Self::ReactionDepthExceeded => "reaction_depth_exceeded",
            Self::SelfEvent => "self_event",
            Self::RecursiveAgentResponse => "retry_exhausted_same_chat",
            Self::IneligibleScope => "ineligible_scope",
            Self::ResolvedIncident => "resolved_incident",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeSetupReason {
    ResponderBindingMissing,
}

impl WakeSetupReason {
    fn code(&self) -> &'static str {
        match self {
            Self::ResponderBindingMissing => "responder_binding_missing",
        }
    }
}

/// Context carried from the Attention projection into a wake decision.  The
/// decision event is the wake consumer's source event; these references point
/// back to the incident/source that caused it and never contain incident
/// details or model content.
#[derive(Debug, Clone, Default)]
struct WakeDecisionContext {
    attention_id: Option<String>,
    source_event_id: Option<String>,
    incident_digest: Option<String>,
    attention_status: Option<String>,
    attention_version: Option<i64>,
}

#[derive(Debug, Clone)]
enum WakeDecisionEvent {
    Admitted {
        leased_until: String,
        cooldown_until: String,
    },
    Suppressed(WakeSuppressionReason),
    SetupRequired(WakeSetupReason),
}

#[derive(Clone)]
pub struct AttentionService {
    db: Arc<SqliteDb>,
    event_bus: Option<Arc<events::EventBus>>,
}

impl AttentionService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            db,
            event_bus: None,
        }
    }

    /// Attach the event bus so an autonomy stall reaches the user. Without it
    /// the suppression is still recorded in the wake ledger; it just stays
    /// invisible, which is the failure this exists to prevent.
    #[must_use]
    pub fn with_event_bus(mut self, event_bus: Arc<events::EventBus>) -> Self {
        self.event_bus = Some(event_bus);
        self
    }

    /// Announce that autonomy has halted while incidents remain unanswered.
    async fn publish_autonomy_stall(&self, scope_type: &str, scope_id: &str, reason: &str) {
        if scope_type != "project" {
            return;
        }
        let Some(event_bus) = self.event_bus.as_ref() else {
            return;
        };
        // Only a stall with outstanding work is worth interrupting a user for.
        let open_incidents: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM attention_projection
             WHERE scope_type = 'project' AND scope_id = ? AND status = 'open'",
        )
        .bind(scope_id)
        .fetch_one(self.db.pool())
        .await
        .unwrap_or(0);
        if open_incidents == 0 {
            return;
        }
        event_bus.publish(events::ForgeEvent {
            event_type: "project.autonomy_stalled".to_owned(),
            entity_id: scope_id.to_owned(),
            timestamp: events::event_timestamp(),
            context: events::EventContext::ProjectAutonomyStalled {
                project_id: scope_id.to_owned(),
                open_incidents,
                reason: reason.to_owned(),
            },
        });
    }

    /// Start the durable Attention projection consumer.  The worker owns no
    /// mutable in-memory cursor: each tick claims from the database ledger,
    /// records bounded health, and exits promptly when the server requests
    /// shutdown.
    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(PROJECTION_POLL_INTERVAL);
            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow_and_update() {
                            break;
                        }
                    }
                    _ = poll.tick() => {
                        if let Err(error) = self.project_once(100).await {
                            tracing::warn!(error = %error, "Attention projection poll failed");
                        }
                    }
                }
            }
        })
    }

    /// Claim and project a bounded batch from the authoritative event ledger.
    /// Every event is checkpointed only after its projection has committed, so
    /// a crash leaves the event eligible for an idempotent replay.
    pub async fn project_once(&self, limit: i64) -> Result<AttentionProjectionRun> {
        let owner = new_uuid_v4();
        let started_at = now_rfc3339();
        let leased_until = (Utc::now() + Duration::seconds(CONSUMER_LEASE_SECONDS)).to_rfc3339();
        self.record_health(UpsertAttentionConsumerHealth {
            consumer_name: CONSUMER_NAME.to_owned(),
            last_sequence: 0,
            last_started_at: Some(started_at.clone()),
            last_success_at: None,
            last_error_at: None,
            last_error_code: None,
            last_error_message: None,
            lease_owner: Some(owner.clone()),
            lease_until: Some(leased_until.clone()),
            processed_events_delta: 0,
            updated_at: started_at.clone(),
        })
        .await?;

        let events = DomainEventRepo::claim_event_batch(
            &*self.db,
            ClaimDomainEvents {
                consumer_name: CONSUMER_NAME.to_owned(),
                lease_owner: owner.clone(),
                now: started_at.clone(),
                leased_until,
                limit: limit.clamp(1, 100),
            },
        )
        .await?;
        let claimed_events = events.len();
        let mut processed_events = 0;
        let mut last_sequence = self
            .consumer_cursor()
            .await?
            .map(|cursor| cursor.last_sequence)
            .unwrap_or(0);

        for event in events {
            if let Err(error) = self.project_event(&event).await {
                // A semantic dedupe/check conflict can never succeed on
                // retry: retrying the same event forever wedges the cursor
                // and silences the entire wake pipeline. Quarantine the
                // event (complete it without projection) and keep going;
                // transient errors still abort the poll and retry.
                if !matches!(&error, ServiceError::Db(db::DbError::Check(_))) {
                    let error_code = error_code(&error);
                    let error_message = bounded_error_message(&error);
                    let _ = self
                        .record_health(UpsertAttentionConsumerHealth {
                            consumer_name: CONSUMER_NAME.to_owned(),
                            last_sequence,
                            last_started_at: None,
                            last_success_at: None,
                            last_error_at: Some(now_rfc3339()),
                            last_error_code: Some(error_code.to_owned()),
                            last_error_message: Some(error_message),
                            lease_owner: Some(owner.clone()),
                            lease_until: None,
                            processed_events_delta: 0,
                            updated_at: now_rfc3339(),
                        })
                        .await;
                    return Err(error);
                }
                tracing::error!(
                    event_sequence = event.sequence,
                    event_id = %event.id,
                    event_type = %event.event_type,
                    %error,
                    "attention projection quarantined a poison event"
                );
            }

            let completed_at = now_rfc3339();
            let dedupe_key = crate::domain_event_service::event_completion_dedupe_key(&event);
            DomainEventRepo::complete_claimed_event(
                &*self.db,
                CompleteDomainEvent {
                    consumer_name: CONSUMER_NAME.to_owned(),
                    lease_owner: owner.clone(),
                    event_sequence: event.sequence,
                    event_id: event.id.clone(),
                    dedupe_key,
                    completed_at: completed_at.clone(),
                },
            )
            .await?;
            processed_events += 1;
            last_sequence = event.sequence;
            self.record_health(UpsertAttentionConsumerHealth {
                consumer_name: CONSUMER_NAME.to_owned(),
                last_sequence,
                last_started_at: None,
                last_success_at: Some(completed_at.clone()),
                last_error_at: None,
                last_error_code: None,
                last_error_message: None,
                lease_owner: Some(owner.clone()),
                lease_until: None,
                processed_events_delta: 1,
                updated_at: completed_at,
            })
            .await?;
        }

        self.record_health(UpsertAttentionConsumerHealth {
            consumer_name: CONSUMER_NAME.to_owned(),
            last_sequence,
            last_started_at: None,
            last_success_at: if claimed_events == 0 {
                None
            } else {
                Some(now_rfc3339())
            },
            last_error_at: None,
            last_error_code: None,
            last_error_message: None,
            lease_owner: None,
            lease_until: None,
            processed_events_delta: 0,
            updated_at: now_rfc3339(),
        })
        .await?;

        Ok(AttentionProjectionRun {
            claimed_events,
            processed_events,
            last_sequence,
        })
    }

    /// Apply deterministic wake admission after an Attention projection.
    /// This is a *pre-admission* decision: an `agent.wake.admitted` event
    /// reserves one analysis lease and consumes one budget unit, but creates
    /// no model job.  The wake consumer records the final `turn_admitted`,
    /// `deterministically_suppressed`, `deferred`, or `setup_required`
    /// disposition for that decision event before advancing its cursor.
    pub async fn admit_wake(&self, request: WakeAdmissionRequest) -> Result<WakeAdmissionResult> {
        let context = self.wake_decision_context_for_incident(&request).await?;
        // Projection requests with no current binding carry an empty identity
        // deliberately.  Preserve that configuration failure as a durable
        // setup decision rather than classifying it as an ineligible identity
        // (and never consult or consume a budget for it).
        if request.identity_id.trim().is_empty() {
            if let Some(reason) = wake_pre_admission_suppression_reason(&request) {
                return self
                    .persist_suppressed_wake(&request, &context, reason)
                    .await;
            }
            if context.attention_id.is_some() {
                return self.persist_setup_required_wake(&request, &context).await;
            }
        }
        self.admit_wake_with_context(request, context).await
    }

    async fn wake_decision_context_for_incident(
        &self,
        request: &WakeAdmissionRequest,
    ) -> Result<WakeDecisionContext> {
        let row = sqlx::query(
            "SELECT id, attention_type, scope_type, scope_id, status,
                    source_event_id, source_sequence, details_json,
                    recommended_action, version
             FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(&request.incident_key)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(WakeDecisionContext::default());
        };
        let id: String = row.try_get("id")?;
        let attention_type: String = row.try_get("attention_type")?;
        let scope_type: String = row.try_get("scope_type")?;
        let scope_id: String = row.try_get("scope_id")?;
        let status: String = row.try_get("status")?;
        let source_event_id: String = row.try_get("source_event_id")?;
        let source_sequence: Option<i64> = row.try_get("source_sequence")?;
        let details_json: String = row.try_get("details_json")?;
        let recommended_action: String = row.try_get("recommended_action")?;
        let version: i64 = row.try_get("version")?;
        Ok(WakeDecisionContext {
            attention_id: Some(id),
            source_event_id: Some(source_event_id.clone()),
            incident_digest: Some(wake_attention_state_digest(
                &attention_type,
                &scope_type,
                &scope_id,
                &status,
                &source_event_id,
                source_sequence,
                &details_json,
                &recommended_action,
                version,
            )),
            attention_status: Some(status),
            attention_version: Some(version),
        })
    }

    async fn admit_wake_with_context(
        &self,
        request: WakeAdmissionRequest,
        context: WakeDecisionContext,
    ) -> Result<WakeAdmissionResult> {
        if let Some(reason) = wake_pre_admission_suppression_reason(&request) {
            return self
                .persist_suppressed_wake(&request, &context, reason)
                .await;
        }
        // A projection crash can replay the same source event after the
        // lease/event transaction committed. Context-bearing projection calls
        // are idempotent at the pre-admission boundary: return the original
        // lease metadata instead of consuming budget again or turning one
        // source event into an admitted event followed by a duplicate
        // suppression.
        if context.source_event_id.is_some() {
            let dedupe_key = wake_admitted_dedupe_key(&request, &context);
            if let Some(event) =
                DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe_key).await?
            {
                let payload =
                    serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
                let leased_until = payload
                    .get("leased_until")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let cooldown_until = payload
                    .get("cooldown_until")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let budget_remaining = payload.get("budget_remaining").and_then(Value::as_i64);
                return Ok(WakeAdmissionResult::Admitted {
                    leased_until,
                    cooldown_until,
                    budget_remaining,
                });
            }
        }
        if matches!(request.scope_type.as_str(), "project" | "agent_chat")
            && !self
                .wake_responder_is_configured(&request.scope_type, &request.scope_id, None)
                .await?
        {
            if context.attention_id.is_some() {
                return self.persist_setup_required_wake(&request, &context).await;
            }
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }
        if !matches!(
            request.scope_type.as_str(),
            "account" | "project" | "agent_chat" | "task"
        ) {
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }
        if !self
            .wake_identity_is_eligible(&request.identity_id, &request.scope_type, &request.scope_id)
            .await?
        {
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }

        let now = parse_rfc3339(&request.now).unwrap_or_else(Utc::now);
        let lease_seconds = request.lease_seconds.clamp(1, 300);
        let cooldown_seconds = request.cooldown_seconds.clamp(1, 86_400);
        let leased_until = (now + Duration::seconds(lease_seconds)).to_rfc3339();
        let cooldown_until = (now + Duration::seconds(cooldown_seconds)).to_rfc3339();
        let now = now.to_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;

        // Attention is authoritative at the admission boundary.  A source
        // event can be delivered after an operator resolves its incident;
        // that event receives a terminal suppression rather than waking a
        // stale responder.
        let attention_status = if let Some(attention_id) = context.attention_id.as_deref() {
            sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
                .bind(attention_id)
                .fetch_optional(&mut *transaction)
                .await?
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM attention_projection WHERE dedupe_key = ?",
            )
            .bind(&request.incident_key)
            .fetch_optional(&mut *transaction)
            .await?
        };
        if attention_status.as_deref() == Some("resolved") {
            self.append_wake_decision_in_tx(
                &mut transaction,
                &request,
                &context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                None,
                None,
                &now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ResolvedIncident,
            });
        }

        let (budget, budget_scope_type, budget_scope_id) = self
            .wake_budget_in_tx(
                &mut transaction,
                &request.identity_id,
                &request.scope_type,
                &request.scope_id,
            )
            .await?;

        // The historical lease primary key includes identity_id.  Keep that
        // immutable schema, but make the active policy incident-global by
        // rejecting any live lease/cooldown for the canonical scope before
        // budget accounting.  This closes the binding-replacement race where
        // a new identity could otherwise analyse the same incident in
        // parallel with the old identity.
        let existing_lease = sqlx::query(
            "SELECT identity_id, leased_until, cooldown_until
             FROM agent_wake_lease
             WHERE scope_type = ? AND scope_id = ? AND incident_key = ?
               AND (leased_until > ? OR cooldown_until > ?)
             ORDER BY leased_until DESC, identity_id ASC
             LIMIT 1",
        )
        .bind(&budget_scope_type)
        .bind(&budget_scope_id)
        .bind(&request.incident_key)
        .bind(&now)
        .bind(&now)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing_lease {
            let leased_until_existing: String = row.try_get("leased_until")?;
            let cooldown_until_existing: Option<String> = row.try_get("cooldown_until")?;
            let reason = if leased_until_existing > now {
                WakeSuppressionReason::DuplicateIncident
            } else if cooldown_until_existing
                .as_deref()
                .is_some_and(|value| value > now.as_str())
            {
                WakeSuppressionReason::Cooldown
            } else {
                WakeSuppressionReason::DuplicateIncident
            };
            self.append_wake_decision_in_tx(
                &mut transaction,
                &request,
                &context,
                WakeDecisionEvent::Suppressed(reason.clone()),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            transaction.commit().await?;
            return Ok(WakeAdmissionResult::Suppressed { reason });
        }

        // Direct callers may not carry an Attention row (and therefore miss
        // the context-bearing replay check above).  Once an existing lease or
        // cooldown is ruled out, a matching admitted decision still means the
        // source was already pre-admitted; return its metadata rather than
        // incrementing the budget a second time before the domain-event
        // dedupe turns the append into a no-op.
        let dedupe_key = wake_admitted_dedupe_key(&request, &context);
        let admitted_payload = sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&dedupe_key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(payload_json) = admitted_payload {
            let payload = serde_json::from_str::<Value>(&payload_json).unwrap_or(Value::Null);
            let leased_until = payload
                .get("leased_until")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let cooldown_until = payload
                .get("cooldown_until")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let budget_remaining = payload.get("budget_remaining").and_then(Value::as_i64);
            transaction.rollback().await?;
            return Ok(WakeAdmissionResult::Admitted {
                leased_until,
                cooldown_until,
                budget_remaining,
            });
        }

        if budget == Some(0) {
            self.append_wake_decision_in_tx(
                &mut transaction,
                &request,
                &context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            transaction.commit().await?;
            self.publish_autonomy_stall(
                &budget_scope_type,
                &budget_scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::BudgetExhausted,
            });
        }

        let window_started =
            (parse_rfc3339(&now).unwrap_or_else(Utc::now) - Duration::hours(1)).to_rfc3339();
        let budget_remaining = if let Some(budget) = budget {
            let current = sqlx::query(
                "SELECT window_started_at, admitted_count
                 FROM agent_wake_budget_window
                 WHERE identity_id = ? AND scope_type = ? AND scope_id = ?",
            )
            .bind(&request.identity_id)
            .bind(&budget_scope_type)
            .bind(&budget_scope_id)
            .fetch_optional(&mut *transaction)
            .await?;
            let (admitted_count, in_window) = current
                .map(|row| {
                    let started: String = row.try_get("window_started_at")?;
                    let count: i64 = row.try_get("admitted_count")?;
                    Ok::<_, sqlx::Error>((count, started > window_started))
                })
                .transpose()?
                .unwrap_or((0, false));
            if in_window && admitted_count >= budget {
                self.append_wake_decision_in_tx(
                    &mut transaction,
                    &request,
                    &context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                    Some(0),
                    Some((&budget_scope_type, &budget_scope_id)),
                    &now,
                )
                .await?;
                transaction.commit().await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::BudgetExhausted,
                });
            }
            if in_window {
                sqlx::query(
                    "UPDATE agent_wake_budget_window
                     SET admitted_count = admitted_count + 1,
                         version = version + 1, updated_at = ?
                     WHERE identity_id = ? AND scope_type = ? AND scope_id = ?",
                )
                .bind(&now)
                .bind(&request.identity_id)
                .bind(&budget_scope_type)
                .bind(&budget_scope_id)
                .execute(&mut *transaction)
                .await?;
            } else {
                sqlx::query(
                    "INSERT INTO agent_wake_budget_window (
                        identity_id, scope_type, scope_id, window_started_at,
                        window_seconds, admitted_count, version, updated_at
                     ) VALUES (?, ?, ?, ?, 3600, 1, 1, ?)
                     ON CONFLICT(identity_id, scope_type, scope_id) DO UPDATE SET
                        window_started_at = excluded.window_started_at,
                        admitted_count = 1,
                        version = agent_wake_budget_window.version + 1,
                        updated_at = excluded.updated_at",
                )
                .bind(&request.identity_id)
                .bind(&budget_scope_type)
                .bind(&budget_scope_id)
                .bind(&now)
                .bind(&now)
                .execute(&mut *transaction)
                .await?;
            }
            Some((budget - admitted_count - 1).max(0))
        } else {
            None
        };

        let lease_result = sqlx::query(
            "INSERT INTO agent_wake_lease (
                identity_id, scope_type, scope_id, incident_key, lease_owner,
                leased_until, reaction_depth, updated_at, cooldown_until,
                last_admitted_at, admission_count, correlation_id, causation_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
             ON CONFLICT(identity_id, scope_type, scope_id, incident_key) DO UPDATE SET
                lease_owner = excluded.lease_owner,
                leased_until = excluded.leased_until,
                reaction_depth = excluded.reaction_depth,
                updated_at = excluded.updated_at,
                cooldown_until = excluded.cooldown_until,
                last_admitted_at = excluded.last_admitted_at,
                admission_count = agent_wake_lease.admission_count + 1,
                correlation_id = excluded.correlation_id,
                causation_id = excluded.causation_id
             WHERE agent_wake_lease.leased_until <= excluded.updated_at
               AND (agent_wake_lease.cooldown_until IS NULL
                    OR agent_wake_lease.cooldown_until <= excluded.updated_at)",
        )
        .bind(&request.identity_id)
        .bind(&budget_scope_type)
        .bind(&budget_scope_id)
        .bind(&request.incident_key)
        .bind(&request.lease_owner)
        .bind(&leased_until)
        .bind(request.reaction_depth.clamp(0, 8))
        .bind(&now)
        .bind(&cooldown_until)
        .bind(&now)
        .bind(&request.correlation_id)
        .bind(request.causation_id.as_deref())
        .execute(&mut *transaction)
        .await?;
        if lease_result.rows_affected() == 0 {
            // This is normally covered by the global pre-check.  Keep the
            // compare-and-swap result authoritative for same-transaction
            // replays and classify an expired lease/cooldown conservatively.
            transaction.rollback().await?;
            return self
                .persist_suppressed_wake(
                    &request,
                    &context,
                    WakeSuppressionReason::DuplicateIncident,
                )
                .await;
        }

        // The lease, budget increment, and pre-admitted wake event share one
        // transaction.  No budget is consumed for suppressed/setup-required
        // decision events.
        self.append_wake_decision_in_tx(
            &mut transaction,
            &request,
            &context,
            WakeDecisionEvent::Admitted {
                leased_until: leased_until.clone(),
                cooldown_until: cooldown_until.clone(),
            },
            budget_remaining,
            Some((&budget_scope_type, &budget_scope_id)),
            &now,
        )
        .await?;
        transaction.commit().await?;
        Ok(WakeAdmissionResult::Admitted {
            leased_until,
            cooldown_until,
            budget_remaining,
        })
    }

    async fn persist_suppressed_wake(
        &self,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        reason: WakeSuppressionReason,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now)
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        self.append_wake_decision_in_tx(
            &mut transaction,
            request,
            context,
            WakeDecisionEvent::Suppressed(reason.clone()),
            None,
            None,
            &now,
        )
        .await?;
        transaction.commit().await?;
        Ok(WakeAdmissionResult::Suppressed { reason })
    }

    async fn persist_setup_required_wake(
        &self,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now)
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        self.append_wake_decision_in_tx(
            &mut transaction,
            request,
            context,
            WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
            None,
            None,
            &now,
        )
        .await?;
        transaction.commit().await?;
        Ok(WakeAdmissionResult::SetupRequired {
            reason: WakeSetupReason::ResponderBindingMissing,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn append_wake_decision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        decision: WakeDecisionEvent,
        budget_remaining: Option<i64>,
        lease_scope: Option<(&str, &str)>,
        now: &str,
    ) -> Result<DomainEvent> {
        let source_event_id = context
            .source_event_id
            .as_deref()
            .or(request.causation_id.as_deref());
        let incident_digest = context
            .incident_digest
            .clone()
            .unwrap_or_else(|| wake_incident_digest(&request.incident_key, source_event_id));
        let attention_id = context.attention_id.clone();
        let safe_incident_key = bounded_wake_ref(&request.incident_key);
        let safe_source_event_id = source_event_id.map(bounded_wake_ref);
        let identity_id = (!request.identity_id.trim().is_empty())
            .then(|| bounded_wake_ref(&request.identity_id));
        let (
            event_type,
            decision_code,
            reason_code,
            admission_phase,
            lease_owner,
            leased_until,
            cooldown_until,
        ) = match &decision {
            WakeDecisionEvent::Admitted {
                leased_until,
                cooldown_until,
            } => (
                "agent.wake.admitted",
                "turn_admitted",
                "pre_admitted",
                "pre_admitted",
                Some(bounded_wake_ref(&request.lease_owner)),
                Some(leased_until.clone()),
                Some(cooldown_until.clone()),
            ),
            WakeDecisionEvent::Suppressed(reason) => (
                "agent.wake.suppressed",
                "deterministically_suppressed",
                reason.code(),
                "policy",
                None,
                None,
                None,
            ),
            WakeDecisionEvent::SetupRequired(reason) => (
                "agent.wake.setup_required",
                "setup_required",
                reason.code(),
                "configuration",
                None,
                None,
                None,
            ),
        };
        let source_key = source_event_id.unwrap_or(request.correlation_id.as_str());
        let decision_dedupe = if event_type == "agent.wake.admitted" {
            wake_admitted_dedupe_key(request, context)
        } else {
            format!(
                "agent-wake-decision:{}:{}",
                event_type.replace('.', "-"),
                wake_incident_digest(
                    &format!(
                        "{}:{}:{}:{}:{}:{}:{}",
                        request.scope_type,
                        request.scope_id,
                        request.incident_key,
                        source_key,
                        reason_code,
                        event_type,
                        incident_digest,
                    ),
                    None,
                )
            )
        };
        let causation_depth = (request.reaction_depth.max(0) + 1).min(16);
        let payload_json = json!({
            "decision": decision_code,
            // Keep the historical admitted action token while the consumer
            // migrates to the explicit decision/phase contract.
            "action": (event_type == "agent.wake.admitted").then_some("wake_admitted"),
            "reason": reason_code,
            "admission_phase": admission_phase,
            "identity_id": identity_id,
            "scope_type": bounded_wake_ref(&request.scope_type),
            "scope_id": bounded_wake_ref(&request.scope_id),
            "incident_key": safe_incident_key,
            "incident_digest": incident_digest,
            "attention_id": attention_id,
            "attention_status": context.attention_status.as_deref(),
            "attention_version": context.attention_version,
            "source_event_id": safe_source_event_id,
            "correlation_id": bounded_wake_ref(&request.correlation_id),
            "causation_id": request.causation_id.as_deref().map(bounded_wake_ref),
            "causation_depth": causation_depth,
            "reaction_depth": request.reaction_depth.clamp(0, 16),
            "budget_remaining": budget_remaining,
            "lease_scope_type": lease_scope.map(|value| value.0),
            "lease_scope_id": lease_scope.map(|value| value.1),
            "lease_owner": lease_owner,
            "leased_until": leased_until,
            "cooldown_until": cooldown_until,
        })
        .to_string();
        let event_scope_type = if matches!(
            request.scope_type.as_str(),
            "account" | "project" | "room" | "task" | "system" | "agent_chat"
        ) {
            request.scope_type.clone()
        } else {
            "system".to_owned()
        };
        let event_scope_id = if event_scope_type == request.scope_type {
            request.scope_id.clone()
        } else {
            wake_incident_digest(&request.scope_id, None)
        };
        DomainEventRepo::append_event_in_tx(
            &*self.db,
            transaction,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: event_type.to_owned(),
                entity_type: "agent_wake".to_owned(),
                entity_id: safe_incident_key,
                actor_type: "attention_projection".to_owned(),
                actor_id: None,
                scope_type: event_scope_type,
                scope_id: event_scope_id,
                correlation_id: request.correlation_id.clone(),
                causation_id: request.causation_id.clone(),
                causation_depth,
                dedupe_key: Some(decision_dedupe),
                payload_json,
                created_at: now.to_owned(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    async fn wake_budget_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<(Option<i64>, String, String)> {
        let mut budget_scope_type = scope_type.to_owned();
        let mut budget_scope_id = scope_id.to_owned();
        let project_id = match scope_type {
            "project" => Some(scope_id.to_owned()),
            "task" => {
                sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                    .bind(scope_id)
                    .fetch_optional(&mut **transaction)
                    .await?
            }
            "agent_chat" => sqlx::query_scalar::<_, Option<String>>(
                "SELECT project_id FROM agent_chat WHERE id = ?",
            )
            .bind(scope_id)
            .fetch_optional(&mut **transaction)
            .await?
            .flatten(),
            "account" => None,
            _ => return Ok((Some(0), budget_scope_type, budget_scope_id)),
        };

        if let Some(project_id) = project_id {
            budget_scope_type = "project".to_owned();
            budget_scope_id = project_id.clone();
            let budget = sqlx::query_scalar::<_, i64>(
                "SELECT wake_budget
                 FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
            )
            .bind(identity_id)
            .bind(&project_id)
            .fetch_optional(&mut **transaction)
            .await?;
            return Ok((budget.or(Some(0)), budget_scope_type, budget_scope_id));
        }

        if scope_type == "agent_chat" {
            if let Some(account_id) = sqlx::query_scalar::<_, String>(
                "SELECT account_id FROM agent_chat
                 WHERE id = ? AND kind = 'account_main'",
            )
            .bind(scope_id)
            .fetch_optional(&mut **transaction)
            .await?
            {
                return Ok((Some(10), "account".to_owned(), account_id));
            }
        }

        // Direct account agents have a bounded default window.  Project
        // memberships always use their explicit wake_budget above; this
        // fallback does not grant membership or any data authority.
        if scope_type == "account" {
            return Ok((Some(10), budget_scope_type, budget_scope_id));
        }
        Ok((Some(0), budget_scope_type, budget_scope_id))
    }

    pub async fn list_for_user(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        status: Option<&str>,
        include_snoozed: bool,
        page: PageRequest,
    ) -> Result<(
        Page<AttentionProjection>,
        Option<AttentionConsumerHealthResponse>,
    )> {
        if let Some(project_id) = project_id {
            self.require_project_access(user_id, project_id).await?;
        }
        let items = AttentionRepo::list_attention(
            &*self.db,
            AttentionListQuery {
                account_id: Some(user_id.to_owned()),
                project_id: project_id.map(str::to_owned),
                scope_type: None,
                status: status.map(str::to_owned),
                include_snoozed,
                page,
            },
        )
        .await?;
        let health = self.consumer_health().await?;
        Ok((items, health))
    }

    pub async fn acknowledge(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        if current.status == "resolved" {
            return Err(ServiceError::conflict(
                "resolved attention cannot be acknowledged",
            ));
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "acknowledged".to_owned(),
                acknowledged_at: Some(Some(now_rfc3339())),
                snoozed_until: Some(None),
                resolved_at: Some(None),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    pub async fn snooze(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
        snoozed_until: &str,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        if current.status == "resolved" {
            return Err(ServiceError::conflict(
                "resolved attention cannot be snoozed",
            ));
        }
        let until = DateTime::parse_from_rfc3339(snoozed_until)
            .map_err(|_| ServiceError::invalid_operation("snoozed_until must be RFC3339"))?
            .with_timezone(&Utc);
        let now = Utc::now();
        if until <= now || until > now + Duration::days(30) {
            return Err(ServiceError::invalid_operation(
                "snoozed_until must be within the next 30 days",
            ));
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "acknowledged".to_owned(),
                acknowledged_at: Some(Some(now_rfc3339())),
                snoozed_until: Some(Some(until.to_rfc3339())),
                resolved_at: Some(None),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    pub async fn resolve(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        if current.status == "resolved" {
            if current.version != expected_version {
                return Err(ServiceError::from(db::DbError::VersionConflict));
            }
            return Ok(current);
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "resolved".to_owned(),
                acknowledged_at: None,
                snoozed_until: Some(None),
                resolved_at: Some(Some(now_rfc3339())),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    pub async fn mission_control_home(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<MissionControlHomeResponse> {
        if let Some(project_id) = project_id {
            self.require_project_access(user_id, project_id).await?;
        }
        let limit = limit.clamp(1, 50);
        let (attention, health) = self
            .list_for_user(
                user_id,
                project_id,
                None,
                false,
                PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: db::SortBy::Priority,
                    sort_order: db::SortOrder::Desc,
                },
            )
            .await?;
        let coordination_activity = self
            .coordination_activity(user_id, project_id, limit)
            .await?;
        let review_ready = self
            .work_items(user_id, project_id, &["review"], limit)
            .await?;
        let active_work = self
            .work_items(user_id, project_id, &["in_progress", "merging"], limit)
            .await?;
        let agent_health = self.agent_health(user_id, project_id, limit).await?;
        let recent_outcomes = self.recent_outcomes(user_id, project_id, limit).await?;
        let capacity = self.capacity(user_id, project_id).await?;
        Ok(MissionControlHomeResponse {
            needs_attention: attention
                .items
                .into_iter()
                .map(attention_item)
                .collect::<Result<Vec<_>>>()?,
            coordination_activity,
            review_ready,
            active_work,
            agent_health,
            recent_outcomes,
            capacity,
            consumer_health: health,
            computed_at: now_rfc3339(),
        })
    }

    /// Project the two operator-visible coordination histories into one
    /// bounded feed. Direct command receipts are the committed history; only
    /// pending/approved approval actions are included from the action queue.
    /// The queries deliberately select digests and typed outcomes, never the
    /// action payload body. Visibility is enforced in SQL so an unauthorized
    /// project row cannot be loaded and filtered after the fact.
    async fn coordination_activity(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlCoordinationActivity>> {
        let (predicate, values) =
            self.coordination_scope_visibility_predicate(user_id, project_id, "r");

        let direct_sql = format!(
            "SELECT r.id, r.principal_type, r.principal_id, r.scope_type, r.scope_id,
                    r.operation, r.input_digest, r.policy_result, r.correlation_id,
                    r.outcome_json, r.committed_at
             FROM command_receipt r
             WHERE r.agent_action_execution_id IS NULL
               AND {predicate}
             ORDER BY r.committed_at DESC, r.id DESC
             LIMIT ?"
        );
        let mut direct_query = sqlx::query(&direct_sql);
        for value in &values {
            direct_query = direct_query.bind(value);
        }
        let direct_rows = direct_query.bind(limit).fetch_all(self.db.pool()).await?;

        let mut activities = direct_rows
            .into_iter()
            .map(|row| {
                let outcome_json: Option<String> = row.try_get("outcome_json")?;
                Ok(MissionControlCoordinationActivity {
                    id: row.try_get("id")?,
                    activity_kind: "direct_command".to_owned(),
                    actor_type: row.try_get("principal_type")?,
                    actor_id: row.try_get("principal_id")?,
                    scope_type: row.try_get("scope_type")?,
                    scope_id: row.try_get("scope_id")?,
                    operation: row.try_get("operation")?,
                    input_digest: row.try_get("input_digest")?,
                    policy_result: row.try_get("policy_result")?,
                    status: "committed".to_owned(),
                    correlation_id: row.try_get("correlation_id")?,
                    outcome: outcome_json.and_then(|value| serde_json::from_str(&value).ok()),
                    occurred_at: row.try_get("committed_at")?,
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(ServiceError::from)?;

        let (action_predicate, action_values) =
            self.coordination_scope_visibility_predicate(user_id, project_id, "a");
        let action_sql = format!(
            "SELECT a.id, a.actor_identity_id, a.scope_type, a.scope_id, a.operation,
                    a.payload_hash, a.policy_result, a.status, a.correlation_id,
                    a.outcome_json, a.updated_at
             FROM agent_action a
             WHERE a.policy_result = 'approval_required'
               AND a.status IN ('pending_approval', 'approved')
               AND {action_predicate}
             ORDER BY a.updated_at DESC, a.id DESC
             LIMIT ?"
        );
        let mut action_query = sqlx::query(&action_sql);
        for value in &action_values {
            action_query = action_query.bind(value);
        }
        let action_rows = action_query.bind(limit).fetch_all(self.db.pool()).await?;
        activities.extend(
            action_rows
                .into_iter()
                .map(|row| {
                    let outcome_json: Option<String> = row.try_get("outcome_json")?;
                    Ok(MissionControlCoordinationActivity {
                        id: row.try_get("id")?,
                        activity_kind: "approval_action".to_owned(),
                        actor_type: "agent".to_owned(),
                        actor_id: row.try_get("actor_identity_id")?,
                        scope_type: row.try_get("scope_type")?,
                        scope_id: row.try_get("scope_id")?,
                        operation: row.try_get("operation")?,
                        input_digest: row.try_get("payload_hash")?,
                        policy_result: row.try_get("policy_result")?,
                        status: row.try_get("status")?,
                        correlation_id: row.try_get("correlation_id")?,
                        outcome: outcome_json.and_then(|value| serde_json::from_str(&value).ok()),
                        occurred_at: row.try_get("updated_at")?,
                    })
                })
                .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?,
        );

        activities.sort_by(|left, right| {
            parse_rfc3339(&right.occurred_at)
                .cmp(&parse_rfc3339(&left.occurred_at))
                .then_with(|| right.occurred_at.cmp(&left.occurred_at))
                .then_with(|| right.id.cmp(&left.id))
        });
        activities.truncate(limit as usize);
        Ok(activities)
    }

    pub async fn agent_detail(
        &self,
        user_id: &str,
        identity_id: &str,
        limit: i64,
    ) -> Result<AgentDetailResponse> {
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))?;
        self.require_agent_access(user_id, identity_id).await?;
        let bindings = sqlx::query(
            "SELECT binding_id, binding_type, project_id, chat_id, state,
                    subscriptions_json, wake_budget
             FROM (
                 SELECT b.id AS binding_id,
                        'main' AS binding_type,
                        NULL AS project_id,
                        c.id AS chat_id,
                        b.state,
                        '[]' AS subscriptions_json,
                        0 AS wake_budget
                 FROM account_main_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'account_main' AND c.account_id = b.account_id
                 WHERE b.identity_id = ? AND b.account_id = ?
                   AND b.state <> 'revoked'
                 UNION ALL
                 SELECT b.id AS binding_id,
                        'project' AS binding_type,
                        b.project_id,
                        c.id AS chat_id,
                        b.state,
                        b.subscriptions_json,
                        b.wake_budget
                 FROM project_agent_binding b
                 JOIN agent_chat c
                   ON c.kind = 'project' AND c.project_id = b.project_id
                 JOIN project p ON p.id = b.project_id
                 LEFT JOIN project_member pm
                   ON pm.project_id = b.project_id AND pm.user_id = ?
                 WHERE b.identity_id = ? AND b.state <> 'revoked'
                   AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
             )
             ORDER BY project_id ASC, binding_type ASC, binding_id ASC LIMIT ?",
        )
        .bind(identity_id)
        .bind(user_id)
        .bind(user_id)
        .bind(identity_id)
        .bind(user_id)
        .bind(limit.clamp(1, 50))
        .fetch_all(self.db.pool())
        .await?
        .into_iter()
        .map(|row| {
            Ok(AgentBindingSummary {
                binding_id: row.try_get("binding_id")?,
                binding_type: row.try_get("binding_type")?,
                project_id: row.try_get("project_id")?,
                chat_id: row.try_get("chat_id")?,
                state: row.try_get("state")?,
                subscription_count: subscription_count(
                    &row.try_get::<String, _>("subscriptions_json")?,
                ),
                wake_budget: row.try_get("wake_budget")?,
            })
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;

        let mut scopes = Vec::new();
        for scope in AgentContextScopeRepo::list_context_scopes(&*self.db, identity_id).await? {
            if let Err(error) = self
                .require_scope_access(user_id, &scope.scope_type, &scope.scope_id)
                .await
            {
                if !is_visibility_miss(&error) {
                    return Err(error);
                }
                continue;
            }
            scopes.push(AgentScopeSummary {
                scope_type: scope.scope_type,
                scope_id: scope.scope_id,
                task_role: scope.task_role,
                workspace_access: scope.workspace_access,
                updated_at: scope.updated_at,
            });
            if scopes.len() >= limit.clamp(1, 50) as usize {
                break;
            }
        }

        let all_sessions = sqlx::query(
            "SELECT s.id AS session_id, c.scope_type, c.scope_id,
                    s.backend_kind, s.status, s.connection_status,
                    s.last_activity_at, s.updated_at
             FROM agent_session s
             JOIN agent_context_scope c ON c.id = s.context_scope_id
             WHERE s.identity_id = ?
             ORDER BY s.updated_at DESC, s.id DESC",
        )
        .bind(identity_id)
        .fetch_all(self.db.pool())
        .await?
        .into_iter()
        .map(|row| {
            Ok(AgentSessionSummary {
                session_id: row.try_get("session_id")?,
                scope_type: row.try_get("scope_type")?,
                scope_id: row.try_get("scope_id")?,
                backend_kind: row.try_get("backend_kind")?,
                status: row.try_get("status")?,
                connection_status: row.try_get("connection_status")?,
                last_activity_at: row.try_get("last_activity_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()?;
        let mut sessions = Vec::new();
        for session in all_sessions {
            if let Err(error) = self
                .require_scope_access(user_id, &session.scope_type, &session.scope_id)
                .await
            {
                if !is_visibility_miss(&error) {
                    return Err(error);
                }
                continue;
            }
            sessions.push(session);
            if sessions.len() >= limit.clamp(1, 50) as usize {
                break;
            }
        }

        let current_focus = match self.focus_for_agent(identity_id).await? {
            Some(item)
                if self
                    .require_project_access(user_id, &item.project_id)
                    .await
                    .is_ok() =>
            {
                Some(mission_work_item(item))
            }
            _ => None,
        };
        let mut checkpoint_present = false;
        for session in &sessions {
            let has_checkpoint = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM protected_agent_session_state
                 WHERE session_id = ? AND checkpoint_turn_id IS NOT NULL",
            )
            .bind(&session.session_id)
            .fetch_one(self.db.pool())
            .await?
                > 0;
            checkpoint_present |= has_checkpoint;
        }
        let continuity_status = if sessions
            .iter()
            .any(|session| session.status == "failed" || session.connection_status == "unavailable")
        {
            "degraded"
        } else if checkpoint_present || !sessions.is_empty() {
            "healthy"
        } else {
            "unknown"
        };
        let last_activity_at = sessions
            .iter()
            .find_map(|session| session.last_activity_at.clone());

        Ok(AgentDetailResponse {
            identity_id: agent.id,
            name: agent.name,
            description: agent.description,
            backend_kind: Some(agent.backend_kind),
            provider: agent.provider,
            model: agent.model,
            identity_status: agent.status.to_string(),
            paused: agent.paused,
            bindings,
            scopes,
            sessions,
            current_focus,
            open_commitment_count: self
                .visible_coordination_count(
                    "agent_commitment",
                    "owner_identity_id",
                    &["proposed", "open", "accepted", "in_progress", "blocked"],
                    user_id,
                    identity_id,
                )
                .await?,
            open_inbox_count: self
                .visible_coordination_count(
                    "agent_inbox_item",
                    "recipient_identity_id",
                    &["unread", "read", "acknowledged"],
                    user_id,
                    identity_id,
                )
                .await?,
            memory_namespace_count: self
                .visible_memory_namespace_count(user_id, identity_id)
                .await?,
            usage: self.agent_usage_summary(identity_id).await?,
            continuity: AgentContinuityHealth {
                status: continuity_status.to_owned(),
                checkpoint_present,
                last_activity_at,
            },
        })
    }

    pub async fn consumer_health(&self) -> Result<Option<AttentionConsumerHealthResponse>> {
        let Some(health) =
            AttentionRepo::get_attention_consumer_health(&*self.db, CONSUMER_NAME).await?
        else {
            return Ok(None);
        };
        let stale = health
            .last_success_at
            .as_deref()
            .and_then(parse_rfc3339)
            .map(|timestamp| Utc::now() - timestamp > Duration::seconds(CONSUMER_STALE_SECONDS))
            .unwrap_or(true);
        Ok(Some(AttentionConsumerHealthResponse {
            consumer_name: health.consumer_name,
            last_sequence: health.last_sequence,
            last_success_at: health.last_success_at,
            last_error_code: health.last_error_code,
            stale,
            processed_events: health.processed_events,
            updated_at: health.updated_at,
        }))
    }

    /// A Task entering `review` is only attention when a person must decide
    /// the gate. A review run by the workflow's reviewer Agent settles itself;
    /// waking the Project Agent for it sends the Agent to a `task.review`
    /// action the gate rejects, which wastes the turn and reports a blocker
    /// that does not exist. A Task that no longer exists is nobody's review.
    async fn review_needs_a_person(&self, event: &DomainEvent) -> Result<bool> {
        if event.entity_type != "task" {
            return Ok(true);
        }
        let Some(task) = db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await? else {
            return Ok(false);
        };
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(false);
        };
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(crate::task_service::task_review_requires_user_decision(
            &task, &workflow,
        ))
    }

    async fn project_event(&self, event: &DomainEvent) -> Result<()> {
        if let Some(category) = classify_event(event) {
            if category == "review_ready" && !self.review_needs_a_person(event).await? {
                return Ok(());
            }
            let (scope_type, scope_id) = self.event_scope(event).await?;
            // Attention's historical materialization table accepts the
            // account/project scope vocabulary, while Agent Chat events use
            // the chat as their canonical wake scope.  Keep the raw chat
            // scope in the incident key and wake payload (so delivery can
            // route to that chat), but materialize the Attention row under
            // its owning account/project scope.
            let (attention_scope_type, attention_scope_id) = self
                .attention_projection_scope(&scope_type, &scope_id)
                .await?;
            let identity_id = self.wake_identity_for_event(event).await?;
            let incident_key = attention_incident_key(category, event, &scope_type, &scope_id);
            let (priority, summary, recommended_action) = category_metadata(category);
            let task_context = if event.entity_type == "task" {
                sqlx::query("SELECT title, status FROM task WHERE id = ?")
                    .bind(&event.entity_id)
                    .fetch_optional(self.db.pool())
                    .await?
                    .map(|row| {
                        json!({
                            "task_title": bounded_text(row.try_get::<String, _>("title").unwrap_or_default()),
                            "task_status": row.try_get::<String, _>("status").unwrap_or_default(),
                        })
                    })
            } else {
                None
            };
            let event_payload =
                serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
            let details_json = serde_json::to_string(&json!({
                "source_event_id": event.id,
                "source_sequence": event.sequence,
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "task": task_context,
                "role": event_payload.get("role").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "stop_reason": event_payload.get("stop_reason").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "error": event_payload.get("error").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "recovery": ["inspect_task", "assign_or_confirm_role", "retry_once"],
            }))
            .map_err(|error| ServiceError::Domain(error.to_string()))?;
            let attention = AttentionRepo::insert_attention(
                &*self.db,
                CreateAttentionProjection {
                    id: new_uuid_v4(),
                    attention_type: category.to_owned(),
                    scope_type: attention_scope_type,
                    scope_id: attention_scope_id,
                    identity_id: identity_id.clone(),
                    source_event_id: event.id.clone(),
                    priority,
                    status: "open".to_owned(),
                    summary: bounded_summary(summary),
                    details_json,
                    dedupe_key: incident_key.clone(),
                    occurred_at: event.created_at.clone(),
                    updated_at: now_rfc3339(),
                    acknowledged_at: None,
                    snoozed_until: None,
                    resolved_at: None,
                    updated_by_user_id: None,
                    recommended_action: recommended_action.to_owned(),
                    source_sequence: Some(event.sequence),
                },
            )
            .await?;

            let decision_context = WakeDecisionContext {
                attention_id: Some(attention.id.clone()),
                source_event_id: Some(event.id.clone()),
                incident_digest: Some(wake_attention_incident_digest(&attention)),
                attention_status: Some(attention.status.clone()),
                attention_version: Some(attention.version),
            };

            // Wake admission happens only after the rebuildable Attention row
            // is durable.  Eligibility is checked independently of the
            // projection so a visible incident cannot grant an identity a
            // Project/Agent Chat/Task wake authority it does not already possess.
            let request = WakeAdmissionRequest {
                identity_id: identity_id.clone().unwrap_or_default(),
                scope_type: scope_type.clone(),
                scope_id: scope_id.clone(),
                incident_key: incident_key.clone(),
                lease_owner: new_uuid_v4(),
                correlation_id: event.correlation_id.clone(),
                causation_id: Some(event.id.clone()),
                caused_by_identity_id: (event.actor_type == "agent")
                    .then(|| event.actor_id.clone())
                    .flatten(),
                reaction_depth: event.causation_depth,
                now: now_rfc3339(),
                lease_seconds: WAKE_LEASE_SECONDS,
                cooldown_seconds: WAKE_COOLDOWN_SECONDS,
            };
            // Policy terminal states are checked before responder setup.  In
            // particular, a retry-exhausted Agent Chat event must be recorded
            // as recursive suppression even when its binding has already
            // disappeared; setup-required would otherwise hide a recursion
            // decision behind a missing responder.
            if let Some(reason) = wake_pre_admission_suppression_reason(&request) {
                let _ = self
                    .persist_suppressed_wake(&request, &decision_context, reason)
                    .await?;
            } else {
                let responder_configured = self
                    .wake_responder_is_configured(&scope_type, &scope_id, None)
                    .await?;
                let identity_eligible = match identity_id.as_deref() {
                    Some(identity_id) => {
                        self.wake_responder_is_configured(&scope_type, &scope_id, Some(identity_id))
                            .await?
                            && self
                                .wake_identity_is_eligible(identity_id, &scope_type, &scope_id)
                                .await?
                    }
                    None => false,
                };
                match identity_id {
                    Some(_) if responder_configured && identity_eligible => {
                        let _ = self
                            .admit_wake_with_context(request, decision_context)
                            .await?;
                    }
                    Some(_) if responder_configured => {
                        let _ = self
                            .persist_suppressed_wake(
                                &request,
                                &decision_context,
                                WakeSuppressionReason::IneligibleScope,
                            )
                            .await?;
                    }
                    _ => {
                        let _ = self
                            .persist_setup_required_wake(&request, &decision_context)
                            .await?;
                    }
                }
            }
        }

        for category in resolution_categories(event) {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            let incident_key = attention_incident_key(category, event, &scope_type, &scope_id);
            AttentionRepo::resolve_attention_by_dedupe(
                &*self.db,
                &incident_key,
                &event.id,
                &now_rfc3339(),
            )
            .await?;
        }
        if event.event_type == "milestone.readiness.evaluated" {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            let (scope_type, scope_id) = self
                .attention_projection_scope(&scope_type, &scope_id)
                .await?;
            let resolved_at = now_rfc3339();
            sqlx::query(
                "UPDATE attention_projection
                 SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                     source_event_id = ?, updated_at = ?, version = version + 1
                 WHERE attention_type = 'delivery_followup'
                   AND scope_type = ? AND scope_id = ? AND status <> 'resolved'",
            )
            .bind(&resolved_at)
            .bind(&event.id)
            .bind(&resolved_at)
            .bind(scope_type)
            .bind(scope_id)
            .execute(self.db.pool())
            .await?;
        }
        Ok(())
    }

    async fn attention_projection_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<(String, String)> {
        if scope_type != "agent_chat" {
            return Ok((scope_type.to_owned(), scope_id.to_owned()));
        }
        let Some(row) =
            sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                .bind(scope_id)
                .fetch_optional(self.db.pool())
                .await?
        else {
            // Keep the raw scope for the existing projection error path when
            // an event references a chat that no longer exists.  Valid Chat
            // events always resolve to one of the accepted material scopes.
            return Ok((scope_type.to_owned(), scope_id.to_owned()));
        };
        let kind: String = row.try_get("kind")?;
        match kind.as_str() {
            "account_main" => row
                .try_get::<Option<String>, _>("account_id")
                .map(|account_id| ("account".to_owned(), account_id.unwrap_or_default()))
                .map_err(ServiceError::from),
            "project" => row
                .try_get::<Option<String>, _>("project_id")
                .map(|project_id| ("project".to_owned(), project_id.unwrap_or_default()))
                .map_err(ServiceError::from),
            _ => Ok((scope_type.to_owned(), scope_id.to_owned())),
        }
    }

    async fn event_scope(&self, event: &DomainEvent) -> Result<(String, String)> {
        if event.scope_type == "task" || event.entity_type == "task" {
            if let Some(project_id) =
                sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                    .bind(&event.entity_id)
                    .fetch_optional(self.db.pool())
                    .await?
            {
                return Ok(("project".to_owned(), project_id));
            }
        }
        let is_agent_chat_event = event.scope_type == "agent_chat"
            || event.entity_type == "agent_chat"
            || event.entity_type == "agent_chat_turn_job";
        if is_agent_chat_event
            && sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_chat WHERE id = ?")
                .bind(&event.scope_id)
                .fetch_one(self.db.pool())
                .await?
                > 0
        {
            return Ok(("agent_chat".to_owned(), event.scope_id.clone()));
        }
        Ok((event.scope_type.clone(), event.scope_id.clone()))
    }

    async fn event_identity(&self, event: &DomainEvent) -> Result<Option<String>> {
        if event.actor_type != "agent" {
            return Ok(None);
        }
        let Some(actor_id) = event.actor_id.as_deref() else {
            return Ok(None);
        };
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                .bind(actor_id)
                .fetch_one(self.db.pool())
                .await?
                > 0;
        Ok(exists.then(|| actor_id.to_owned()))
    }

    async fn wake_identity_for_event(&self, event: &DomainEvent) -> Result<Option<String>> {
        // Resolve the current owner before looking at the source actor.  An
        // unresolved incident survives binding replacement and must wake the
        // replacement identity, never the stale identity that authored the
        // original event.
        let (scope_type, scope_id) = self.event_scope(event).await?;
        match scope_type.as_str() {
            "project" => {
                return Ok(sqlx::query_scalar::<_, String>(
                    "SELECT identity_id FROM project_agent_binding
                     WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
                )
                .bind(&scope_id)
                .fetch_optional(self.db.pool())
                .await?);
            }
            "account" => {
                return Ok(sqlx::query_scalar::<_, String>(
                    "SELECT identity_id FROM account_main_agent_binding
                     WHERE account_id = ? AND state = 'active'",
                )
                .bind(&scope_id)
                .fetch_optional(self.db.pool())
                .await?);
            }
            "agent_chat" => {
                let chat =
                    sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                        .bind(&scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                if let Some(chat) = chat {
                    let kind: String = chat.try_get("kind")?;
                    if kind == "account_main" {
                        return Ok(sqlx::query_scalar::<_, String>(
                            "SELECT identity_id FROM account_main_agent_binding
                             WHERE account_id = ? AND state = 'active'",
                        )
                        .bind(chat.try_get::<Option<String>, _>("account_id")?)
                        .fetch_optional(self.db.pool())
                        .await?);
                    }
                    if kind == "project" {
                        return Ok(sqlx::query_scalar::<_, String>(
                            "SELECT identity_id FROM project_agent_binding
                             WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
                        )
                        .bind(chat.try_get::<Option<String>, _>("project_id")?)
                        .fetch_optional(self.db.pool())
                        .await?);
                    }
                }
                return Ok(None);
            }
            "task" => {
                return Ok(sqlx::query_scalar::<_, String>(
                    "SELECT assignee_id FROM task
                     WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
                )
                .bind(&scope_id)
                .fetch_optional(self.db.pool())
                .await?);
            }
            _ => {}
        }

        // Non-canonical/legacy scopes have no current Main/Project responder
        // to resolve.  Keep the source actor as provenance only for these
        // scopes; callers will record a setup-required decision when it is
        // not eligible.
        if let Some(identity_id) = self.event_identity(event).await? {
            return Ok(Some(identity_id));
        }
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        for key in [
            "identity_id",
            "agent_id",
            "responder_identity_id",
            "assignee_id",
        ] {
            let Some(identity_id) = payload.get(key).and_then(Value::as_str) else {
                continue;
            };
            let exists =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_identity WHERE id = ?")
                    .bind(identity_id)
                    .fetch_one(self.db.pool())
                    .await?
                    > 0;
            if exists {
                return Ok(Some(identity_id.to_owned()));
            }
        }
        Ok(None)
    }

    /// Check whether a current Main/Project binding exists for this canonical
    /// scope.  `identity_id` is optional so projection can distinguish a
    /// missing responder (setup-required) from a configured responder that is
    /// ineligible for the particular event (deterministic suppression).
    async fn wake_responder_is_configured(
        &self,
        scope_type: &str,
        scope_id: &str,
        identity_id: Option<&str>,
    ) -> Result<bool> {
        let count = match scope_type {
            "account" => {
                if let Some(identity_id) = identity_id {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND identity_id = ? AND state = 'active'",
                    )
                    .bind(scope_id)
                    .bind(identity_id)
                    .fetch_one(self.db.pool())
                    .await?
                } else {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM account_main_agent_binding
                         WHERE account_id = ? AND state = 'active'",
                    )
                    .bind(scope_id)
                    .fetch_one(self.db.pool())
                    .await?
                }
            }
            "project" => {
                if let Some(identity_id) = identity_id {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND identity_id = ? AND state = 'active'",
                    )
                    .bind(scope_id)
                    .bind(identity_id)
                    .fetch_one(self.db.pool())
                    .await?
                } else {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM project_agent_binding
                         WHERE project_id = ? AND state = 'active' AND identity_id IS NOT NULL",
                    )
                    .bind(scope_id)
                    .fetch_one(self.db.pool())
                    .await?
                }
            }
            "agent_chat" => {
                let chat =
                    sqlx::query("SELECT kind, account_id, project_id FROM agent_chat WHERE id = ?")
                        .bind(scope_id)
                        .fetch_optional(self.db.pool())
                        .await?;
                let Some(chat) = chat else {
                    return Ok(false);
                };
                let kind: String = chat.try_get("kind")?;
                let account_id: Option<String> = chat.try_get("account_id")?;
                let project_id: Option<String> = chat.try_get("project_id")?;
                let configured = match kind.as_str() {
                    "account_main" => {
                        let Some(account_id) = account_id else {
                            return Ok(false);
                        };
                        if let Some(identity_id) = identity_id {
                            sqlx::query_scalar::<_, i64>(
                                "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND identity_id = ? AND state = 'active'",
                            )
                            .bind(account_id)
                            .bind(identity_id)
                            .fetch_one(self.db.pool())
                            .await?
                        } else {
                            sqlx::query_scalar::<_, i64>(
                                "SELECT COUNT(*) FROM account_main_agent_binding
                                 WHERE account_id = ? AND state = 'active'",
                            )
                            .bind(account_id)
                            .fetch_one(self.db.pool())
                            .await?
                        }
                    }
                    "project" => {
                        let Some(project_id) = project_id else {
                            return Ok(false);
                        };
                        if let Some(identity_id) = identity_id {
                            sqlx::query_scalar::<_, i64>(
                                "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND identity_id = ? AND state = 'active'",
                            )
                            .bind(project_id)
                            .bind(identity_id)
                            .fetch_one(self.db.pool())
                            .await?
                        } else {
                            sqlx::query_scalar::<_, i64>(
                                "SELECT COUNT(*) FROM project_agent_binding
                                 WHERE project_id = ? AND state = 'active'
                                   AND identity_id IS NOT NULL",
                            )
                            .bind(project_id)
                            .fetch_one(self.db.pool())
                            .await?
                        }
                    }
                    _ => 0,
                };
                configured
            }
            "task" => {
                if let Some(identity_id) = identity_id {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
                    )
                    .bind(scope_id)
                    .bind(identity_id)
                    .fetch_one(self.db.pool())
                    .await?
                } else {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM task
                         WHERE id = ? AND assignee_type = 'agent' AND assignee_id IS NOT NULL",
                    )
                    .bind(scope_id)
                    .fetch_one(self.db.pool())
                    .await?
                }
            }
            _ => 0,
        };
        Ok(count > 0)
    }

    async fn wake_identity_is_eligible(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<bool> {
        let eligible = match scope_type {
            "account" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM agent_identity
                 WHERE id = ? AND (owner_id IS NULL OR owner_id = ?)",
                )
                .bind(identity_id)
                .bind(scope_id)
                .fetch_one(self.db.pool())
                .await?
                    > 0
            }
            "project" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM project_agent_binding
                 WHERE identity_id = ? AND project_id = ? AND state = 'active'",
                )
                .bind(identity_id)
                .bind(scope_id)
                .fetch_one(self.db.pool())
                .await?
                    > 0
            }
            "agent_chat" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM agent_chat c
                 WHERE c.id = ? AND (
                    (c.kind = 'account_main' AND EXISTS (
                        SELECT 1 FROM account_main_agent_binding b
                        WHERE b.account_id = c.account_id
                          AND b.identity_id = ? AND b.state = 'active'
                    )) OR (c.kind = 'project' AND EXISTS (
                        SELECT 1 FROM project_agent_binding b
                        WHERE b.project_id = c.project_id
                          AND b.identity_id = ? AND b.state = 'active'
                    ))
                 )",
                )
                .bind(scope_id)
                .bind(identity_id)
                .bind(identity_id)
                .fetch_one(self.db.pool())
                .await?
                    > 0
            }
            "task" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM task
                 WHERE id = ? AND assignee_type = 'agent' AND assignee_id = ?",
                )
                .bind(scope_id)
                .bind(identity_id)
                .fetch_one(self.db.pool())
                .await?
                    > 0
            }
            _ => false,
        };
        Ok(eligible)
    }

    async fn record_health(&self, input: UpsertAttentionConsumerHealth) -> Result<()> {
        AttentionRepo::upsert_attention_consumer_health(&*self.db, input).await?;
        Ok(())
    }

    async fn consumer_cursor(&self) -> Result<Option<EventConsumerCursor>> {
        Ok(DomainEventRepo::get_consumer_cursor(&*self.db, CONSUMER_NAME).await?)
    }

    async fn authorized_attention(&self, user_id: &str, id: &str) -> Result<AttentionProjection> {
        let item = AttentionRepo::get_attention(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("attention", id.to_owned()))?;
        self.require_scope_access(user_id, &item.scope_type, &item.scope_id)
            .await?;
        Ok(item)
    }

    async fn require_scope_access(
        &self,
        user_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<()> {
        match scope_type {
            "account" => {
                if scope_id != user_id {
                    return Err(ServiceError::not_found("attention", scope_id.to_owned()));
                }
            }
            "project" => self.require_project_access(user_id, scope_id).await?,
            "agent_chat" => {
                let scope = sqlx::query_as::<_, (Option<String>, Option<String>)>(
                    "SELECT account_id, project_id FROM agent_chat WHERE id = ?",
                )
                .bind(scope_id)
                .fetch_optional(self.db.pool())
                .await?
                .ok_or_else(|| ServiceError::not_found("attention", scope_id.to_owned()))?;
                if let Some(project_id) = scope.1 {
                    self.require_project_access(user_id, &project_id).await?;
                } else if scope.0.as_deref() != Some(user_id) {
                    return Err(ServiceError::not_found("attention", scope_id.to_owned()));
                }
            }
            "task" => {
                let project_id =
                    sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                        .bind(scope_id)
                        .fetch_optional(self.db.pool())
                        .await?
                        .ok_or_else(|| ServiceError::not_found("task", scope_id.to_owned()))?;
                self.require_project_access(user_id, &project_id).await?;
            }
            "agent" => self.require_agent_access(user_id, scope_id).await?,
            _ => return Err(ServiceError::not_found("attention", scope_id.to_owned())),
        }
        Ok(())
    }

    async fn require_project_access(&self, user_id: &str, project_id: &str) -> Result<()> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        if project.owner_id.is_none() || project.owner_id.as_deref() == Some(user_id) {
            return Ok(());
        }
        ProjectMemberRepo::get_member(&*self.db, project_id, user_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        Ok(())
    }

    async fn require_agent_access(&self, user_id: &str, identity_id: &str) -> Result<()> {
        let row = sqlx::query("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(identity_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))?;
        let owner_id: Option<String> = row.try_get("owner_id")?;
        if owner_id.is_none() || owner_id.as_deref() == Some(user_id) {
            return Ok(());
        }
        let visible = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM project_agent_binding m
             JOIN project p ON p.id = m.project_id
             LEFT JOIN project_member pm ON pm.project_id = m.project_id AND pm.user_id = ?
             WHERE m.identity_id = ?
               AND m.state = 'active'
               AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)",
        )
        .bind(user_id)
        .bind(identity_id)
        .bind(user_id)
        .fetch_one(self.db.pool())
        .await?;
        if visible == 0 {
            return Err(ServiceError::not_found("agent", identity_id.to_owned()));
        }
        Ok(())
    }

    async fn work_items(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        statuses: &[&str],
        limit: i64,
    ) -> Result<Vec<MissionControlWorkItem>> {
        let status_placeholders = vec!["?"; statuses.len()].join(", ");
        let status_values = statuses.to_vec();
        let (project_predicate, project_values) =
            self.project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.priority, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ({status_placeholders})
               AND {project_predicate}
             ORDER BY t.priority DESC, t.updated_at ASC, t.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for status in status_values {
            query = query.bind(status);
        }
        for value in project_values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.db.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlWorkItem {
                    task_id: row.try_get("id")?,
                    project_id: row.try_get("project_id")?,
                    title: bounded_text(row.try_get::<String, _>("title")?),
                    status: row.try_get("status")?,
                    priority: row.try_get("priority")?,
                    updated_at: row.try_get("updated_at")?,
                    primary_action: if statuses.contains(&"review") {
                        "review".to_owned()
                    } else {
                        "inspect".to_owned()
                    },
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(ServiceError::from)
    }

    async fn agent_health(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlAgentHealth>> {
        let (predicate, values) = self.agent_visibility_predicate(user_id, project_id);
        let (project_count, count_values) = if let Some(project_id) = project_id {
            (
                "COUNT(DISTINCT CASE WHEN m.project_id = ? THEN m.project_id END)".to_owned(),
                vec![project_id.to_owned()],
            )
        } else {
            (
                "COUNT(DISTINCT CASE
                    WHEN p2.owner_id IS NULL OR p2.owner_id = ? OR pm2.user_id IS NOT NULL
                    THEN m.project_id END)"
                    .to_owned(),
                vec![user_id.to_owned()],
            )
        };
        let (active_session_count, session_count_values) = match project_id {
            Some(project_id) => (
                "COUNT(DISTINCT CASE WHEN session_scope.project_id = ? THEN s.id END)".to_owned(),
                vec![project_id.to_owned()],
            ),
            None => ("COUNT(DISTINCT s.id)".to_owned(), Vec::new()),
        };
        let sql = format!(
            "SELECT a.id, a.name, a.backend_kind, a.provider, a.model,
                    a.status, a.paused, a.last_heartbeat_at,
                    h.status AS connection_status,
                    {active_session_count} AS active_session_count,
                    {project_count} AS project_count
             FROM agent_current a
             LEFT JOIN agent_connection_health h ON h.profile_id = a.profile_id
             LEFT JOIN agent_session s ON s.identity_id = a.id
                 AND s.status IN ('starting', 'ready', 'running', 'degraded')
             LEFT JOIN agent_context_scope session_scope
                 ON session_scope.id = s.context_scope_id
             LEFT JOIN project_agent_binding m ON m.identity_id = a.id AND m.state = 'active'
             LEFT JOIN project p2 ON p2.id = m.project_id
             LEFT JOIN project_member pm2 ON pm2.project_id = m.project_id AND pm2.user_id = ?
             WHERE {predicate}
             GROUP BY a.id, a.name, a.backend_kind, a.provider, a.model,
                      a.status, a.paused, a.last_heartbeat_at, h.status
             ORDER BY a.name ASC, a.id ASC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in session_count_values {
            query = query.bind(value);
        }
        for value in count_values {
            query = query.bind(value);
        }
        query = query.bind(user_id);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.db.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlAgentHealth {
                    identity_id: row.try_get("id")?,
                    name: bounded_text(row.try_get::<String, _>("name")?),
                    backend_kind: row.try_get("backend_kind")?,
                    provider: row.try_get("provider")?,
                    model: row.try_get("model")?,
                    identity_status: row.try_get("status")?,
                    paused: row.try_get::<i64, _>("paused")? != 0,
                    connection_status: row.try_get("connection_status")?,
                    last_activity_at: row.try_get("last_heartbeat_at")?,
                    active_session_count: row.try_get("active_session_count")?,
                    project_count: row.try_get("project_count")?,
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(ServiceError::from)
    }

    async fn recent_outcomes(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlRecentOutcome>> {
        let (predicate, values) = self.project_visibility_predicate(user_id, project_id);
        let sql = format!(
            "SELECT t.id, t.project_id, t.title, t.status, t.updated_at
             FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('done', 'cancelled', 'blocked')
               AND {predicate}
             ORDER BY t.updated_at DESC, t.id DESC LIMIT ?"
        );
        let mut query = sqlx::query(&sql);
        for value in values {
            query = query.bind(value);
        }
        let rows = query.bind(limit).fetch_all(self.db.pool()).await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlRecentOutcome {
                    task_id: row.try_get("id")?,
                    project_id: row.try_get("project_id")?,
                    title: bounded_text(row.try_get::<String, _>("title")?),
                    outcome: row.try_get("status")?,
                    occurred_at: row.try_get("updated_at")?,
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(ServiceError::from)
    }

    async fn capacity(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> Result<MissionControlCapacity> {
        let (predicate, values) = self.project_visibility_predicate(user_id, project_id);
        // Keep the query construction below explicit; it avoids interpolating
        // authorization values while allowing account and project scopes.
        let active_sql = format!(
            "SELECT COUNT(*) AS count FROM execution e
             JOIN task t ON t.id = e.task_id JOIN project p ON p.id = t.project_id
             WHERE e.status = 'running' AND t.deleted_at IS NULL AND {predicate}"
        );
        let mut active_query = sqlx::query(&active_sql);
        for value in &values {
            active_query = active_query.bind(value);
        }
        let active = active_query
            .fetch_optional(self.db.pool())
            .await?
            .and_then(|row| row.try_get::<i64, _>("count").ok())
            .unwrap_or(0);

        let queued_sql = format!(
            "SELECT COUNT(*) AS count FROM task t JOIN project p ON p.id = t.project_id
             WHERE t.deleted_at IS NULL AND t.status IN ('todo', 'backlog') AND {predicate}"
        );
        let mut query = sqlx::query(&queued_sql);
        for value in &values {
            query = query.bind(value);
        }
        let queued = query
            .fetch_one(self.db.pool())
            .await?
            .try_get::<i64, _>("count")?;
        let (agent_predicate, agent_values) = self.agent_visibility_predicate(user_id, project_id);
        let session_sql = format!(
            "SELECT COUNT(*) FROM agent_session s
             JOIN agent_current a ON a.id = s.identity_id
             WHERE s.status IN ('starting', 'ready', 'running', 'degraded')
               AND {agent_predicate}"
        );
        let mut session_query = sqlx::query_scalar::<_, i64>(&session_sql);
        for value in agent_values {
            session_query = session_query.bind(value);
        }
        let active_sessions = session_query.fetch_one(self.db.pool()).await?;
        Ok(MissionControlCapacity {
            active_executions: active,
            queued_tasks: queued,
            active_sessions,
            healthy: self
                .consumer_health()
                .await?
                .map(|health| !health.stale && health.last_error_code.is_none())
                .unwrap_or(false),
        })
    }

    async fn focus_for_agent(&self, identity_id: &str) -> Result<Option<db::Task>> {
        // A Project/Main binding owns the obligation, not a Task Worker
        // assignment.  Keep the worker path for identities assigned directly
        // to a Task, then include the current Task attached to an unfinished
        // commitment owned by this identity.  The identity predicate is
        // deliberately applied in both branches so replacing a binding never
        // leaks the previous owner's focus into the replacement detail.
        let task_id = sqlx::query_scalar::<_, String>(
            "SELECT task_id
             FROM (
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 WHERE t.assignee_id = ?
                   AND t.status = 'in_progress'
                   AND t.deleted_at IS NULL
                 UNION
                 SELECT t.id AS task_id, t.updated_at
                 FROM task t
                 JOIN agent_commitment c ON c.originating_task_id = t.id
                 WHERE c.owner_identity_id = ?
                   AND c.status IN ('proposed', 'open', 'accepted', 'in_progress', 'blocked')
                   AND t.deleted_at IS NULL
             )
             ORDER BY updated_at DESC, task_id DESC
             LIMIT 1",
        )
        .bind(identity_id)
        .bind(identity_id)
        .fetch_optional(self.db.pool())
        .await?;
        match task_id {
            Some(task_id) => Ok(db::TaskRepo::get_by_id(&*self.db, &task_id, false).await?),
            None => Ok(None),
        }
    }

    /// Count only immutable memory bindings whose canonical scope is visible
    /// to the requesting user.  The query never reads memory bodies or FTS
    /// rows, and inaccessible scope existence is discarded before counting.
    async fn visible_memory_namespace_count(
        &self,
        user_id: &str,
        identity_id: &str,
    ) -> Result<i64> {
        let rows = sqlx::query(
            "SELECT DISTINCT scope_type, scope_id
             FROM forge_memory_source_binding
             WHERE identity_id = ?
             ORDER BY scope_type ASC, scope_id ASC",
        )
        .bind(identity_id)
        .fetch_all(self.db.pool())
        .await?;
        let mut visible = BTreeSet::new();
        for row in rows {
            let scope_type: String = row.try_get("scope_type")?;
            let scope_id: String = row.try_get("scope_id")?;
            match self
                .require_scope_access(user_id, &scope_type, &scope_id)
                .await
            {
                Ok(()) => {
                    visible.insert((scope_type, scope_id));
                }
                Err(error) if is_visibility_miss(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(visible.len() as i64)
    }

    async fn agent_usage_summary(&self, identity_id: &str) -> Result<AgentUsageSummary> {
        let execution = sqlx::query(
            "SELECT COUNT(DISTINCT eu.execution_id) AS execution_count,
                    SUM(eu.input_tokens) AS input_tokens,
                    SUM(eu.output_tokens) AS output_tokens,
                    SUM(eu.cache_read_tokens) AS cache_read_tokens,
                    SUM(eu.cache_write_tokens) AS cache_write_tokens,
                    SUM(eu.cost_usd) AS cost_usd
             FROM execution_usage eu
             JOIN execution e ON e.id = eu.execution_id
             WHERE e.agent_id = ?",
        )
        .bind(identity_id)
        .fetch_one(self.db.pool())
        .await?;
        let chat = sqlx::query(
            "SELECT COUNT(token_usage_json) AS execution_count,
                    SUM(CASE WHEN json_valid(token_usage_json)
                             THEN CAST(json_extract(token_usage_json, '$.input') AS INTEGER)
                             ELSE 0 END) AS input_tokens,
                    SUM(CASE WHEN json_valid(token_usage_json)
                             THEN CAST(json_extract(token_usage_json, '$.output') AS INTEGER)
                             ELSE 0 END) AS output_tokens,
                    SUM(CASE WHEN json_valid(token_usage_json)
                             THEN CAST(json_extract(token_usage_json, '$.cache_read') AS INTEGER)
                             ELSE 0 END) AS cache_read_tokens,
                    SUM(CASE WHEN json_valid(token_usage_json)
                             THEN CAST(json_extract(token_usage_json, '$.cache_write') AS INTEGER)
                             ELSE 0 END) AS cache_write_tokens,
                    SUM(CASE WHEN json_valid(token_usage_json)
                             THEN CAST(json_extract(token_usage_json, '$.cost_usd') AS REAL)
                             ELSE 0 END) AS cost_usd
             FROM agent_chat_message
             WHERE author_type = 'agent' AND author_id = ? AND token_usage_json IS NOT NULL",
        )
        .bind(identity_id)
        .fetch_one(self.db.pool())
        .await?;

        let execution_count = execution.try_get::<i64, _>("execution_count")?
            + chat.try_get::<i64, _>("execution_count")?;
        let input_tokens =
            optional_i64(&execution, "input_tokens")? + optional_i64(&chat, "input_tokens")?;
        let output_tokens =
            optional_i64(&execution, "output_tokens")? + optional_i64(&chat, "output_tokens")?;
        let cache_read_tokens = optional_i64(&execution, "cache_read_tokens")?
            + optional_i64(&chat, "cache_read_tokens")?;
        let cache_write_tokens = optional_i64(&execution, "cache_write_tokens")?
            + optional_i64(&chat, "cache_write_tokens")?;
        let execution_cost = optional_f64(&execution, "cost_usd")?;
        let chat_cost = optional_f64(&chat, "cost_usd")?;
        let cost_usd = execution_cost
            .into_iter()
            .chain(chat_cost)
            .reduce(|left, right| left + right);
        Ok(AgentUsageSummary {
            execution_count,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            cost_usd,
        })
    }

    async fn visible_coordination_count(
        &self,
        table: &str,
        identity_column: &str,
        statuses: &[&str],
        user_id: &str,
        identity_id: &str,
    ) -> Result<i64> {
        let (table, identity_column) = match (table, identity_column) {
            ("agent_commitment", "owner_identity_id") => (table, identity_column),
            ("agent_inbox_item", "recipient_identity_id") => (table, identity_column),
            _ => return Ok(0),
        };
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(self.db.pool())
        .await?;
        if exists == 0 {
            return Ok(0);
        }
        let placeholders = vec!["?"; statuses.len()].join(", ");
        let sql = format!(
            "SELECT scope_type, scope_id FROM {table}
             WHERE {identity_column} = ? AND status IN ({placeholders})"
        );
        let mut query = sqlx::query(&sql).bind(identity_id);
        for status in statuses {
            query = query.bind(status);
        }
        let rows = query.fetch_all(self.db.pool()).await?;
        let mut visible = 0;
        for row in rows {
            let scope_type: String = row.try_get("scope_type")?;
            let scope_id: String = row.try_get("scope_id")?;
            match self
                .require_scope_access(user_id, &scope_type, &scope_id)
                .await
            {
                Ok(()) => visible += 1,
                Err(error) if is_visibility_miss(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(visible)
    }

    fn project_visibility_predicate(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => ("t.project_id = ?".to_owned(), vec![project_id.to_owned()]),
            None => (
                "(p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_member pm
                    WHERE pm.project_id = p.id AND pm.user_id = ?
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }

    fn coordination_scope_visibility_predicate(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        scope_alias: &str,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => (
                format!(
                    "(({scope_alias}.scope_type = 'project' AND {scope_alias}.scope_id = ?
                       AND EXISTS (
                           SELECT 1 FROM project p
                           WHERE p.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'agent_chat' AND EXISTS (
                           SELECT 1 FROM agent_chat c
                           JOIN project p ON p.id = c.project_id
                           WHERE c.id = {scope_alias}.scope_id
                             AND c.kind = 'project'
                             AND c.project_id = ?
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'task' AND EXISTS (
                           SELECT 1 FROM task t
                           JOIN project p ON p.id = t.project_id
                           WHERE t.id = {scope_alias}.scope_id
                             AND t.project_id = ?
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       )))"
                ),
                vec![
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    project_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                ],
            ),
            None => (
                format!(
                    "(({scope_alias}.scope_type = 'account' AND {scope_alias}.scope_id = ?)
                      OR ({scope_alias}.scope_type = 'project' AND EXISTS (
                           SELECT 1 FROM project p
                           WHERE p.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       ))
                      OR ({scope_alias}.scope_type = 'agent_chat' AND EXISTS (
                           SELECT 1 FROM agent_chat c
                           LEFT JOIN project p ON p.id = c.project_id
                           WHERE c.id = {scope_alias}.scope_id
                             AND ((c.kind = 'account_main' AND c.account_id = ?)
                               OR (c.kind = 'project' AND p.id IS NOT NULL AND
                                   (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                       SELECT 1 FROM project_member pm
                                       WHERE pm.project_id = p.id AND pm.user_id = ?
                                   ))))
                       ))
                      OR ({scope_alias}.scope_type = 'task' AND EXISTS (
                           SELECT 1 FROM task t
                           JOIN project p ON p.id = t.project_id
                           WHERE t.id = {scope_alias}.scope_id
                             AND (p.owner_id IS NULL OR p.owner_id = ? OR EXISTS (
                                 SELECT 1 FROM project_member pm
                                 WHERE pm.project_id = p.id AND pm.user_id = ?
                             ))
                       )))"
                ),
                vec![
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                    user_id.to_owned(),
                ],
            ),
        }
    }

    fn agent_visibility_predicate(
        &self,
        user_id: &str,
        project_id: Option<&str>,
    ) -> (String, Vec<String>) {
        match project_id {
            Some(project_id) => (
                "EXISTS (
                    SELECT 1 FROM project_agent_binding pm
                    WHERE pm.identity_id = a.id AND pm.project_id = ? AND pm.state = 'active'
                )"
                .to_owned(),
                vec![project_id.to_owned()],
            ),
            None => (
                "(a.owner_id IS NULL OR a.owner_id = ? OR EXISTS (
                    SELECT 1 FROM project_agent_binding pam
                    JOIN project p ON p.id = pam.project_id
                    LEFT JOIN project_member pm ON pm.project_id = p.id AND pm.user_id = ?
                    WHERE pam.identity_id = a.id AND pam.state = 'active'
                      AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
                ))"
                .to_owned(),
                vec![user_id.to_owned(), user_id.to_owned(), user_id.to_owned()],
            ),
        }
    }
}

/// Return the stable incident identity for one Attention category.
///
/// Most Attention categories are scoped to the source entity.  Progress
/// warnings are different: a healthy executor may publish more than one
/// warning event while it waits, and those events must update one durable
/// incident rather than create a new item per heartbeat.  Execution rows are
/// one attempt/episode, so the execution id is the authoritative dedupe
/// identity for this category.  Terminal and semantic-progress events carry
/// the same id and therefore resolve the warning even when their source event
/// ids differ.
fn attention_incident_key(
    category: &str,
    event: &DomainEvent,
    scope_type: &str,
    scope_id: &str,
) -> String {
    if category == "progress_warning" {
        let execution_id = serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|payload| {
                payload
                    .get("execution_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| event.entity_id.clone());
        return format!("attention:{category}:{scope_type}:{scope_id}:execution:{execution_id}");
    }

    format!(
        "attention:{category}:{scope_type}:{scope_id}:{}:{}",
        event.entity_type, event.entity_id
    )
}

fn is_execution_progress_warning_event(event_type: &str) -> bool {
    event_type == "execution.progress_warning"
}

fn is_execution_semantic_progress_event(event_type: &str) -> bool {
    event_type == "execution.progressed"
}

fn classify_event(event: &DomainEvent) -> Option<&'static str> {
    let event_type = event.event_type.to_ascii_lowercase();
    if event_type == "project_release.candidate_requested" {
        return Some("human_input_required");
    }
    if event_type == "agent.question.created" || event_type == "agent.interaction.required" {
        return Some("human_input_required");
    }
    if event_type == "agent_chat.turn.failed" {
        let status = serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|payload| {
                payload
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        return (status.as_deref() == Some("failed")).then_some("retry_exhausted");
    }
    if event_type.contains("validation")
        && (event_type.contains("fail") || event_type.contains("error"))
    {
        return Some("validation_failed");
    }
    // A stale semantic-progress warning is deliberately checked before the
    // generic stall classifier.  It describes a live owner waiting on model
    // or tool work and must never become a `run_stalled` incident.
    if is_execution_progress_warning_event(&event_type) {
        return Some("progress_warning");
    }
    if event_type.contains("stalled") || event_type.contains("stall") {
        return Some("run_stalled");
    }
    if event_type.contains("retry")
        && (event_type.contains("exhaust") || event_type.contains("limit"))
    {
        return Some("retry_exhausted");
    }
    if event_type.contains("review")
        && (event_type.contains("ready") || event_type.contains("await"))
    {
        return Some("review_ready");
    }
    if event_type.contains("review")
        && (event_type.contains("risk")
            || event_type.contains("fail")
            || event_type.contains("reject"))
    {
        return Some("review_risk");
    }
    if (event_type.contains("runtime")
        || event_type.contains("connection")
        || event_type.contains("session"))
        && (event_type.contains("offline")
            || event_type.contains("unavailable")
            || event_type.contains("degraded")
            || event_type.contains("disconnect")
            || event_type.contains("failed"))
    {
        return Some("runtime_offline");
    }
    if (event_type.contains("runtime")
        || event_type.contains("connection")
        || event_type.contains("session"))
        && matches!(
            payload_status(event).as_deref(),
            Some("offline" | "unavailable" | "degraded" | "failed")
        )
    {
        return Some("runtime_offline");
    }
    if event_type.contains("budget")
        && (event_type.contains("threshold") || event_type.contains("low"))
    {
        return Some("budget_threshold");
    }
    if event_type.contains("commitment") && event_type.contains("overdue") {
        return Some("commitment_overdue");
    }
    if event_type == "task.transitioned" {
        let state = serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|value| {
                value
                    .get("to_state")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        return match state.as_deref() {
            Some("blocked") => Some("validation_failed"),
            Some("review") => Some("review_ready"),
            Some("failed") => Some("run_stalled"),
            Some("done") => Some("delivery_followup"),
            _ => None,
        };
    }
    if matches!(event_type.as_str(), "task.done" | "task.completed") {
        return Some("delivery_followup");
    }
    if matches!(
        event_type.as_str(),
        "execution.failed" | "execution.cancelled"
    ) {
        return Some("execution_failed");
    }
    None
}

fn payload_status(event: &DomainEvent) -> Option<String> {
    serde_json::from_str::<Value>(&event.payload_json)
        .ok()
        .and_then(|value| {
            value
                .get("status")
                .or_else(|| value.get("connection_status"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn resolution_categories(event: &DomainEvent) -> Vec<&'static str> {
    let event_type = event.event_type.to_ascii_lowercase();
    let mut categories = Vec::new();
    if is_execution_semantic_progress_event(&event_type)
        || matches!(
            event_type.as_str(),
            "execution.completed"
                | "execution.failed"
                | "execution.cancelled"
                | "execution.stalled"
                | "execution.lease_expired"
                | "execution.hard_deadline_exceeded"
        )
    {
        categories.push("progress_warning");
    }
    if event_type == "task.transitioned" {
        let state = serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|value| {
                value
                    .get("to_state")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        if matches!(state.as_deref(), Some("done" | "cancelled")) {
            categories.extend([
                "validation_failed",
                "run_stalled",
                "progress_warning",
                "retry_exhausted",
                "review_ready",
                "review_risk",
                "execution_failed",
            ]);
        }
    }
    if event_type == "execution.completed" {
        categories.push("execution_failed");
    }
    if event_type.contains("review")
        && (event_type.contains("passed") || event_type.contains("approved"))
    {
        categories.extend(["review_ready", "review_risk"]);
    }
    if (event_type.contains("runtime") || event_type.contains("connection"))
        && (event_type.contains("healthy")
            || event_type.contains("online")
            || event_type.contains("restored"))
    {
        categories.push("runtime_offline");
    }
    if event_type.contains("commitment")
        && (event_type.contains("completed") || event_type.contains("cancelled"))
    {
        categories.push("commitment_overdue");
    }
    if event_type.contains("answer") || event_type.contains("response") {
        categories.push("human_input_required");
    }
    categories
}

fn category_metadata(category: &str) -> (i64, &'static str, &'static str) {
    match category {
        "human_input_required" => (95, "Human input is required", "answer"),
        "validation_failed" => (80, "Validation failed", "inspect_validation"),
        "run_stalled" => (85, "Run appears stalled", "inspect_run"),
        "progress_warning" => (
            65,
            "Execution is waiting for semantic progress",
            "inspect_run",
        ),
        "retry_exhausted" => (90, "Retry budget exhausted", "review_retry"),
        "review_ready" => (55, "Work is ready for review", "review"),
        "review_risk" => (85, "Review reported a risk", "inspect_review"),
        "execution_failed" => (85, "Task execution stopped", "inspect_run"),
        "delivery_followup" => (
            70,
            "Task completed; reconcile validation, evidence, and readiness",
            "reconcile_delivery",
        ),
        "runtime_offline" => (95, "Agent runtime is unavailable", "restore_runtime"),
        "budget_threshold" => (60, "Agent budget threshold reached", "review_budget"),
        "commitment_overdue" => (75, "Commitment is overdue", "review_commitment"),
        _ => (50, "Attention required", "inspect"),
    }
}

pub fn attention_item(item: AttentionProjection) -> Result<AttentionItem> {
    let category = match item.attention_type.as_str() {
        "human_input_required" => AttentionCategory::HumanInputRequired,
        "validation_failed" => AttentionCategory::ValidationFailed,
        "run_stalled" => AttentionCategory::RunStalled,
        "progress_warning" => AttentionCategory::ProgressWarning,
        "retry_exhausted" => AttentionCategory::RetryExhausted,
        "review_ready" => AttentionCategory::ReviewReady,
        "review_risk" => AttentionCategory::ReviewRisk,
        "execution_failed" => AttentionCategory::ExecutionFailed,
        "delivery_followup" => AttentionCategory::DeliveryFollowup,
        "runtime_offline" => AttentionCategory::RuntimeOffline,
        "budget_threshold" => AttentionCategory::BudgetThreshold,
        "commitment_overdue" => AttentionCategory::CommitmentOverdue,
        other => {
            return Err(ServiceError::Domain(format!(
                "unknown attention category: {other}"
            )));
        }
    };
    let lifecycle = match item.status.as_str() {
        "open" => AttentionLifecycle::Open,
        "acknowledged" => AttentionLifecycle::Acknowledged,
        "resolved" => AttentionLifecycle::Resolved,
        other => {
            return Err(ServiceError::Domain(format!(
                "unknown attention lifecycle: {other}"
            )));
        }
    };
    let details = serde_json::from_str(&item.details_json).unwrap_or_else(|_| json!({}));
    Ok(AttentionItem {
        id: item.id,
        category,
        scope_type: item.scope_type,
        scope_id: item.scope_id,
        identity_id: item.identity_id,
        source_event_id: item.source_event_id,
        priority: item.priority,
        lifecycle,
        summary: item.summary,
        details,
        dedupe_key: item.dedupe_key,
        occurred_at: item.occurred_at,
        updated_at: item.updated_at,
        version: item.version,
        acknowledged_at: item.acknowledged_at,
        snoozed_until: item.snoozed_until,
        resolved_at: item.resolved_at,
        recommended_action: item.recommended_action,
    })
}

/// One projection row the reader cannot map (e.g. a category added on the
/// write side first) must not take down the whole Mission Control feed.
pub fn attention_item_lenient(item: AttentionProjection) -> Option<AttentionItem> {
    match attention_item(item) {
        Ok(item) => Some(item),
        Err(error) => {
            tracing::warn!(%error, "skipping unmappable attention projection row");
            None
        }
    }
}

fn mission_work_item(task: db::Task) -> MissionControlWorkItem {
    MissionControlWorkItem {
        task_id: task.id,
        project_id: task.project_id,
        title: bounded_text(task.title),
        status: task.status,
        priority: task.priority,
        updated_at: task.updated_at,
        primary_action: "inspect".to_owned(),
    }
}

fn bounded_summary(summary: &str) -> String {
    bounded_text(summary.to_owned())
}

fn optional_i64(row: &SqliteRow, column: &str) -> std::result::Result<i64, sqlx::Error> {
    Ok(row.try_get::<Option<i64>, _>(column)?.unwrap_or(0))
}

fn optional_f64(row: &SqliteRow, column: &str) -> std::result::Result<Option<f64>, sqlx::Error> {
    row.try_get::<Option<f64>, _>(column)
}

fn subscription_count(serialized: &str) -> i64 {
    serde_json::from_str::<Value>(serialized)
        .ok()
        .and_then(|value| match value {
            Value::Array(values) => Some(values.len()),
            Value::Object(values) => values
                .get("subscriptions")
                .and_then(Value::as_array)
                .map(Vec::len),
            _ => None,
        })
        .unwrap_or(0)
        .min(64) as i64
}

fn bounded_text(value: String) -> String {
    if value.len() <= MAX_ATTENTION_SUMMARY_LEN {
        return value;
    }
    value
        .chars()
        .take(MAX_ATTENTION_SUMMARY_LEN)
        .collect::<String>()
}

const MAX_WAKE_REF_CHARS: usize = 256;

fn bounded_wake_ref(value: &str) -> String {
    value.chars().take(MAX_WAKE_REF_CHARS).collect()
}

/// Return terminal policy reasons that must be decided before responder
/// configuration or budget lookup.  Keeping these checks in one helper makes
/// the projection path and direct admission path agree, including the
/// system-authored `agent_chat.turn.failed` retry-exhausted event that has no
/// actor identity for the ordinary self-event check.
fn wake_pre_admission_suppression_reason(
    request: &WakeAdmissionRequest,
) -> Option<WakeSuppressionReason> {
    if request.reaction_depth >= MAX_WAKE_REACTION_DEPTH {
        return Some(WakeSuppressionReason::ReactionDepthExceeded);
    }
    if request.reaction_depth > 0
        && request.caused_by_identity_id.as_deref() == Some(request.identity_id.as_str())
    {
        return Some(WakeSuppressionReason::SelfEvent);
    }
    if request.scope_type == "agent_chat" && request.incident_key.contains(":retry_exhausted:") {
        return Some(WakeSuppressionReason::RecursiveAgentResponse);
    }
    None
}

/// Digest only the canonical, bounded Attention projection state.  This is
/// deliberately metadata-only: a wake consumer can compare it with a fresh
/// projection before reconsidering an incident without receiving details from
/// an inaccessible scope.
/// Return the canonical redaction-safe Attention digest used by wake decision
/// events.  Wake delivery/reconsideration can call this helper on the freshly
/// loaded projection instead of reimplementing the material-state contract.
pub fn wake_attention_incident_digest(attention: &AttentionProjection) -> String {
    wake_attention_state_digest(
        &attention.attention_type,
        &attention.scope_type,
        &attention.scope_id,
        &attention.status,
        &attention.source_event_id,
        attention.source_sequence,
        &attention.details_json,
        &attention.recommended_action,
        attention.version,
    )
}

#[allow(clippy::too_many_arguments)]
fn wake_attention_state_digest(
    attention_type: &str,
    scope_type: &str,
    scope_id: &str,
    status: &str,
    source_event_id: &str,
    source_sequence: Option<i64>,
    details_json: &str,
    recommended_action: &str,
    version: i64,
) -> String {
    let details_digest = wake_incident_digest(&canonical_attention_details(details_json), None);
    let canonical = format!(
        "type={};scope_type={};scope_id={};status={};source_event_id={};source_sequence={source_sequence:?};details_digest={};recommended_action={};version={version}",
        bounded_wake_ref(attention_type),
        bounded_wake_ref(scope_type),
        bounded_wake_ref(scope_id),
        bounded_wake_ref(status),
        bounded_wake_ref(source_event_id),
        details_digest,
        bounded_wake_ref(recommended_action),
    );
    wake_incident_digest(&canonical, None)
}

/// Canonicalize only the bounded Attention details used for the digest.  The
/// details themselves never enter the wake event payload; this helper hashes
/// a normalized JSON representation so semantically identical object key
/// orderings cannot create a new incident admission.  Invalid legacy JSON is
/// retained as bounded text, preserving a deterministic digest while keeping
/// the projection observable without leaking its contents.
fn canonical_attention_details(details_json: &str) -> String {
    let bounded = details_json.chars().take(8_192).collect::<String>();
    serde_json::from_str::<Value>(&bounded)
        .map(|value| value.to_string())
        .unwrap_or(bounded)
}

fn wake_admitted_dedupe_key(
    request: &WakeAdmissionRequest,
    context: &WakeDecisionContext,
) -> String {
    let source_key = context
        .source_event_id
        .as_deref()
        .or(request.causation_id.as_deref())
        .unwrap_or(request.correlation_id.as_str());
    format!(
        "agent-wake-admitted:{}:{}:{}:{}:{}:{}",
        request.identity_id,
        request.scope_type,
        request.scope_id,
        request.incident_key,
        source_key,
        context.incident_digest.as_deref().unwrap_or("none")
    )
}

fn wake_incident_digest(incident_key: &str, source_event_id: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(incident_key.as_bytes());
    hasher.update([0]);
    if let Some(source_event_id) = source_event_id {
        hasher.update(source_event_id.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn is_visibility_miss(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::NotFound { .. } | ServiceError::Db(db::DbError::NotFound)
    )
}

fn error_code(error: &ServiceError) -> &'static str {
    match error {
        ServiceError::Db(db::DbError::VersionConflict) => "version_conflict",
        ServiceError::Db(db::DbError::NotFound) => "not_found",
        ServiceError::Db(_) => "database_error",
        ServiceError::Domain(_) => "projection_error",
        _ => "projection_error",
    }
}

fn bounded_error_message(error: &ServiceError) -> String {
    // Errors are operational diagnostics, not event payloads.  Keep them
    // bounded and strip likely credential-bearing query fragments.
    let mut message = error.to_string();
    for marker in ["api_key", "authorization", "bearer", "token", "secret"] {
        if message.to_ascii_lowercase().contains(marker) {
            message = "projection failed with a redacted dependency error".to_owned();
            break;
        }
    }
    bounded_text(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(event_type: &str, payload_json: &str) -> DomainEvent {
        DomainEvent {
            sequence: 1,
            id: "event-1".to_owned(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-1".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: payload_json.to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn task_transition_rules_are_deterministic() {
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"blocked"}"#)),
            Some("validation_failed")
        );
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"review"}"#)),
            Some("review_ready")
        );
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"done"}"#)),
            Some("delivery_followup")
        );
        assert_eq!(
            resolution_categories(&event("task.transitioned", r#"{"to_state":"done"}"#)),
            vec![
                "validation_failed",
                "run_stalled",
                "progress_warning",
                "retry_exhausted",
                "review_ready",
                "review_risk",
                "execution_failed"
            ]
        );
        assert_eq!(
            classify_event(&event("project_release.candidate_requested", "{}")),
            Some("human_input_required")
        );
        assert_eq!(
            classify_event(&event("execution.failed", r#"{"error":"boom"}"#)),
            Some("execution_failed")
        );
        assert_eq!(
            classify_event(&event(
                "execution.cancelled",
                r#"{"stop_reason":"user_requested"}"#
            )),
            Some("execution_failed")
        );
        assert_eq!(
            classify_event(&event(
                "execution.progress_warning",
                r#"{"execution_id":"execution-1"}"#
            )),
            Some("progress_warning")
        );
        assert_eq!(
            classify_event(&event(
                "execution.stalled",
                r#"{"execution_id":"execution-1"}"#
            )),
            Some("run_stalled")
        );
        assert_eq!(classify_event(&event("execution.completed", "{}")), None);
        assert_eq!(
            resolution_categories(&event("execution.completed", "{}")),
            vec!["progress_warning", "execution_failed"]
        );
        assert_eq!(
            resolution_categories(&event(
                "execution.failed",
                r#"{"execution_id":"execution-1"}"#
            )),
            vec!["progress_warning"]
        );
        assert_eq!(
            resolution_categories(&event(
                "execution.progressed",
                r#"{"execution_id":"execution-1"}"#
            )),
            vec!["progress_warning"]
        );
    }

    #[test]
    fn progress_warning_incident_is_deduped_by_execution_episode() {
        let first = event(
            "execution.progress_warning",
            r#"{"execution_id":"execution-1","episode_id":"episode-1"}"#,
        );
        let mut replay = first.clone();
        replay.id = "event-2".to_owned();
        replay.sequence = 2;
        assert_eq!(
            attention_incident_key("progress_warning", &first, "project", "project-1"),
            attention_incident_key("progress_warning", &replay, "project", "project-1")
        );

        let mut next_execution = replay;
        next_execution.id = "event-3".to_owned();
        next_execution.sequence = 3;
        next_execution.payload_json =
            r#"{"execution_id":"execution-2","episode_id":"episode-2"}"#.to_owned();
        assert_ne!(
            attention_incident_key("progress_warning", &first, "project", "project-1"),
            attention_incident_key("progress_warning", &next_execution, "project", "project-1")
        );
    }

    #[test]
    fn progress_warning_projection_maps_to_typed_attention_category() {
        let item = AttentionProjection {
            id: "attention-1".to_owned(),
            attention_type: "progress_warning".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            identity_id: None,
            source_event_id: "event-1".to_owned(),
            priority: 65,
            status: "open".to_owned(),
            summary: "Execution is waiting for semantic progress".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: "attention:progress_warning:project:project-1:execution:execution-1"
                .to_owned(),
            occurred_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            version: 1,
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_run".to_owned(),
            source_sequence: Some(1),
        };
        assert_eq!(
            attention_item(item).unwrap().category,
            AttentionCategory::ProgressWarning
        );
    }

    #[test]
    fn summaries_and_diagnostics_are_bounded_and_redacted() {
        let unicode = "é".repeat(200);
        assert!(bounded_text(unicode).chars().count() <= MAX_ATTENTION_SUMMARY_LEN);
        let error = ServiceError::Domain("authorization token=secret-value".to_owned());
        let message = bounded_error_message(&error);
        assert!(!message.contains("secret-value"));
        assert!(message.len() <= MAX_ATTENTION_SUMMARY_LEN);
    }
}
