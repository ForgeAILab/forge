use std::time::Duration as StdDuration;
use std::{collections::BTreeSet, sync::Arc};

use api_types::{
    AgentBindingSummary, AgentContinuityHealth, AgentDetailResponse, AgentScopeSummary,
    AgentSessionSummary, AttentionCategory, AttentionConsumerHealthResponse, AttentionItem,
    AttentionLifecycle, MissionControlAgentHealth, MissionControlCapacity,
    MissionControlCoordinationActivity, MissionControlHomeResponse, MissionControlRecentOutcome,
    MissionControlWorkItem, UsageAggregate,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use db::{
    new_uuid_v4, now_rfc3339,
    task_interruption_requires_intervention as interruption_fields_require_intervention,
    AgentContextScopeRepo, AgentRepo, AttentionListQuery, AttentionProjection, AttentionRepo,
    CreateAttentionProjection, CreateDomainEvent, DomainEvent, DomainEventRepo,
    EventConsumerCursor, Page, PageRequest, ProjectMemberRepo, ProjectRepo, SqliteDb,
    UpdateAttentionLifecycle,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Acquire, Row, Sqlite, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{
    wake_blocker::BlockerTurnState,
    worker_runtime::{Outcome, Subscription, Worker, WorkerError, WorkerRuntime},
    Result, ServiceError,
};

const CONSUMER_NAME: &str = "attention_projection";

pub(crate) fn attention_consumer_name() -> &'static str {
    CONSUMER_NAME
}

const CONSUMER_STALE_SECONDS: i64 = 90;
const MAX_ATTENTION_SUMMARY_LEN: usize = 160;
/// Terminal execution truth is committed before Task recovery disposition.
/// Give that short saga time to settle before treating a still-manual,
/// undisposed terminal attempt as an actionable orphan.
const TERMINAL_DISPOSITION_GRACE_SECONDS: i64 = 30;
const WAKE_LEASE_SECONDS: i64 = 60;
const WAKE_COOLDOWN_SECONDS: i64 = 300;
/// Below this hourly Project budget every wake category draws on one pool.
const MIN_SPLIT_WAKE_BUDGET: i64 = 5;
/// The sweep runs at most this often; the projection tick runs every loop.
const SWEEP_INTERVAL: StdDuration = StdDuration::from_secs(60);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventCategoryDecision {
    Ready(Option<&'static str>),
    Deferred,
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
    RepeatedFailure,
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
            Self::RepeatedFailure => "repeated_failure",
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
    task_id: Option<String>,
    requires_current_task_intervention: bool,
    orphan_execution_id: Option<String>,
    turn: Option<Arc<crate::wake_turn_consumer::PreparedWakeTurn>>,
    batch: Vec<AttentionProjection>,
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
    action_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    event_bus: Option<Arc<events::EventBus>>,
    last_sweep: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl AttentionService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            db,
            event_bus: None,
            action_connections: None,
            last_sweep: Arc::default(),
        }
    }

    /// Offer construction uses the same live owner resume facts as Task reads.
    #[must_use]
    pub fn with_action_connections(
        mut self,
        connections: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    ) -> Self {
        self.action_connections = Some(connections);
        self
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

    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self)).start(shutdown)
    }
    pub async fn project_once(&self, limit: i64) -> Result<AttentionProjectionRun> {
        let processed_events = WorkerRuntime::new(Arc::clone(&self.db), Arc::new(self.clone()))
            .run_once(limit.clamp(1, 100) as usize)
            .await?;
        self.sweep_once_at(&now_rfc3339()).await?;
        let last_sequence = self.consumer_cursor().await?.map_or(0, |c| c.last_sequence);
        Ok(AttentionProjectionRun {
            claimed_events: processed_events,
            processed_events,
            last_sequence,
        })
    }

    /// Atomically admit the current incident, its audit decision and turn.
    pub async fn admit_wake(&self, request: WakeAdmissionRequest) -> Result<WakeAdmissionResult> {
        let mut context = self.wake_decision_context_for_incident(&request).await?;
        if let Some(id) = context.attention_id.as_deref() {
            if let Some(attention) = self.db.get_attention(id).await? {
                let dedupe = wake_admitted_dedupe_key(&request, &context);
                context.turn = crate::WakeTurnConsumer::new(Arc::clone(&self.db))
                    .prepare_attention(
                        &attention,
                        &dedupe,
                        request.causation_id.as_deref(),
                        request.reaction_depth,
                        &[],
                    )
                    .await?
                    .map(Arc::new);
                context.batch = vec![attention];
            }
        }
        // Projection requests with no current binding carry an empty identity
        // deliberately.  Preserve that configuration failure as a durable
        // setup decision rather than classifying it as an ineligible identity
        // (and never consult or consume a budget for it).
        if request.identity_id.trim().is_empty() {
            if let Some(reason) = wake_policy_suppression_reason(&request) {
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

    /// Level-triggered reconsideration of open Attention. Each scope is
    /// isolated: a failing Project is logged and skipped, never the tick.
    /// Within a Project, blockers are batched first, then budget-suppressed
    /// decisions, then items waiting on setup.
    pub async fn sweep_once_at(&self, now: &str) -> Result<usize> {
        let rows = sqlx::query(
            "SELECT a.id, a.scope_type, a.scope_id, l.incident_digest AS latest_digest,
                    l.disposition AS latest_disposition, l.reason AS latest_reason,
                    l.identity_id AS latest_identity
             FROM attention_projection a
             LEFT JOIN agent_wake_attention_latest l ON l.attention_id = a.id
             WHERE a.status = 'open' AND a.recommended_action <> 'answer_escalation'
               AND (a.snoozed_until IS NULL OR a.snoozed_until <= ?)
             ORDER BY a.scope_type, a.scope_id, a.occurred_at, a.id",
        )
        .bind(now)
        .fetch_all(self.db.pool())
        .await?;
        let mut scopes = std::collections::BTreeMap::<(String, String), Vec<SweepCandidate>>::new();
        for row in rows {
            let disposition: Option<String> = row.try_get("latest_disposition")?;
            let latest = match disposition {
                Some(disposition) => Some(LatestWakeDecision {
                    digest: row.try_get("latest_digest")?,
                    disposition,
                    reason: row.try_get("latest_reason")?,
                    identity_id: row.try_get("latest_identity")?,
                }),
                None => None,
            };
            scopes
                .entry((row.try_get("scope_type")?, row.try_get("scope_id")?))
                .or_default()
                .push(SweepCandidate {
                    id: row.try_get("id")?,
                    latest,
                });
        }
        let mut admitted = 0;
        for ((scope_type, scope_id), candidates) in scopes {
            match self
                .sweep_scope(&scope_type, &scope_id, candidates, now)
                .await
            {
                Ok(count) => admitted += count,
                Err(error) => {
                    tracing::warn!(%scope_type, %scope_id, %error, "wake sweep skipped a scope after an error");
                }
            }
        }
        Ok(admitted)
    }

    async fn sweep_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
        candidates: Vec<SweepCandidate>,
        now: &str,
    ) -> Result<usize> {
        let at = parse_rfc3339(now).unwrap_or_else(Utc::now);
        let responder = if scope_type == "project" {
            let mut conn = self.db.pool().acquire().await?;
            crate::wake_blocker::current_project_responder(&mut conn, scope_id).await?
        } else {
            None
        };
        let mut blockers = Vec::new();
        let mut decisions = Vec::new();
        let mut others = Vec::new();
        for candidate in candidates {
            let Some(attention) = self.db.get_attention(&candidate.id).await? else {
                continue;
            };
            if attention.scope_type == "project" && blocker_category(&attention.attention_type) {
                let digest = wake_attention_incident_digest(&attention);
                let state = {
                    let mut conn = self.db.pool().acquire().await?;
                    crate::wake_blocker::blocker_turn_state(
                        &mut conn,
                        &attention.id,
                        &digest,
                        responder.as_ref(),
                        at,
                    )
                    .await?
                };
                match state {
                    BlockerTurnState::InFlight
                    | BlockerTurnState::Held
                    | BlockerTurnState::Completed { escalated: true } => {}
                    BlockerTurnState::Completed { escalated: false } => {
                        crate::project_escalation::ProjectEscalationService::new(Arc::clone(
                            &self.db,
                        ))
                        .escalate_blocker(&attention)
                        .await?;
                    }
                    BlockerTurnState::Eligible => {
                        let suppressed = candidate.latest.as_ref().is_some_and(|latest| {
                            crate::wake_blocker::is_policy_suppression(&latest.reason)
                                && latest.digest.as_deref() == Some(digest.as_str())
                                && latest.identity_id.as_deref()
                                    == responder.as_ref().map(|r| r.identity_id.as_str())
                        });
                        if !suppressed {
                            blockers.push(attention);
                        }
                    }
                }
            } else if let Some(latest) = &candidate.latest {
                if matches!(latest.disposition.as_str(), "setup_required" | "deferred") {
                    others.push(attention);
                } else if attention.attention_type == DECISION_RECORDED_CATEGORY
                    && matches!(
                        latest.reason.as_str(),
                        "budget_exhausted" | "cooldown" | "duplicate_incident"
                    )
                {
                    // An unanswered decision is level state: retry it once the
                    // bucket or cooldown allows.
                    decisions.push(attention);
                }
            }
        }
        let mut admitted = 0;
        if !blockers.is_empty() && self.admit_blocker_batch(blockers, now).await? {
            admitted += 1;
        }
        for attention in decisions.into_iter().chain(others) {
            let result: Result<WakeAdmissionResult> = async {
                let request = self.request_for_attention(&attention, now).await?;
                self.admit_wake(request).await
            }
            .await;
            match result {
                Ok(WakeAdmissionResult::Admitted { .. }) => admitted += 1,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(attention_id=%attention.id,%error,"wake reconsideration failed");
                }
            }
        }
        Ok(admitted)
    }

    /// One wake per Project window for every eligible blocker, with one directive.
    async fn admit_blocker_batch(
        &self,
        batch: Vec<AttentionProjection>,
        now: &str,
    ) -> Result<bool> {
        let mut members = Vec::with_capacity(batch.len());
        for attention in batch {
            let request = self.request_for_attention(&attention, now).await?;
            if let Some(reason) = wake_policy_suppression_reason(&request) {
                let context = self.wake_decision_context_for_incident(&request).await?;
                self.persist_suppressed_wake(&request, &context, reason)
                    .await?;
                continue;
            }
            members.push((attention, request));
        }
        let Some((first, request)) = members.first().cloned() else {
            return Ok(false);
        };
        let batch = members.into_iter().map(|(a, _)| a).collect::<Vec<_>>();
        let mut context = self.wake_decision_context_for_incident(&request).await?;
        // A re-admission (after a failed or imported turn) must not replay the
        // earlier admission of the same digest: name the turns it replaces.
        let mut parts = Vec::with_capacity(batch.len());
        for attention in &batch {
            let prior: Vec<String> = sqlx::query_scalar(
                "SELECT turn_job_id FROM agent_wake_blocker WHERE attention_id = ? ORDER BY turn_job_id",
            )
            .bind(&attention.id)
            .fetch_all(self.db.pool())
            .await?;
            let digest = wake_attention_incident_digest(attention);
            parts.push(if prior.is_empty() {
                digest
            } else {
                format!("{digest}@{}", prior.join(","))
            });
        }
        let combined = parts.join(":");
        context.incident_digest = Some(wake_incident_digest(&combined, None));
        context.turn = crate::WakeTurnConsumer::new(Arc::clone(&self.db))
            .prepare_attention(
                &first,
                &wake_admitted_dedupe_key(&request, &context),
                request.causation_id.as_deref(),
                request.reaction_depth,
                &batch,
            )
            .await?
            .map(Arc::new);
        context.batch = batch;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let outcome = self.admit_wake_in_tx(&mut tx, &request, &context).await?;
        tx.commit().await?;
        if let Some((scope_type, scope_id)) = outcome.stall_scope {
            self.publish_autonomy_stall(
                &scope_type,
                &scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
        }
        Ok(matches!(
            outcome.result,
            WakeAdmissionResult::Admitted { .. }
        ))
    }

    async fn request_for_attention(
        &self,
        attention: &AttentionProjection,
        now: &str,
    ) -> Result<WakeAdmissionRequest> {
        let event = self.db.get_event(&attention.source_event_id).await?;
        let identity = if attention.scope_type == "project" {
            db::ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &attention.scope_id)
                .await?
                .and_then(|b| b.identity_id)
        } else {
            attention.identity_id.clone()
        };
        Ok(WakeAdmissionRequest {
            identity_id: identity.unwrap_or_default(),
            scope_type: attention.scope_type.clone(),
            scope_id: attention.scope_id.clone(),
            incident_key: attention.dedupe_key.clone(),
            lease_owner: new_uuid_v4(),
            correlation_id: event
                .as_ref()
                .map(|e| e.correlation_id.clone())
                .unwrap_or_else(|| attention.id.clone()),
            causation_id: Some(attention.source_event_id.clone()),
            caused_by_identity_id: event
                .as_ref()
                .filter(|e| e.actor_type == "agent")
                .and_then(|e| e.actor_id.clone()),
            reaction_depth: event.as_ref().map_or(0, |e| e.causation_depth),
            now: now.to_owned(),
            lease_seconds: WAKE_LEASE_SECONDS,
            cooldown_seconds: WAKE_COOLDOWN_SECONDS,
        })
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
        let details = serde_json::from_str::<Value>(&details_json).unwrap_or(Value::Null);
        let requires_current_task_intervention = attention_type == "execution_failed"
            && details.get("source_event_type").and_then(Value::as_str)
                == Some("task.interruption_changed")
            && details
                .pointer("/recovery/requires_intervention")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        let task_id = requires_current_task_intervention
            .then(|| {
                details
                    .get("entity_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        let orphan_execution_id = if attention_type == "execution_failed"
            && matches!(
                details.get("source_event_type").and_then(Value::as_str),
                Some("execution.failed" | "execution.cancelled")
            ) {
            DomainEventRepo::get_event(&*self.db, &source_event_id)
                .await?
                .and_then(|event| serde_json::from_str::<Value>(&event.payload_json).ok())
                .and_then(|payload| {
                    payload
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
        } else {
            None
        };
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
            task_id,
            requires_current_task_intervention,
            orphan_execution_id,
            turn: None,
            batch: Vec::new(),
        })
    }

    async fn admit_wake_with_context(
        &self,
        request: WakeAdmissionRequest,
        context: WakeDecisionContext,
    ) -> Result<WakeAdmissionResult> {
        if let Some(reason) = wake_policy_suppression_reason(&request) {
            return self
                .persist_suppressed_wake(&request, &context, reason)
                .await;
        }
        // A projection crash can replay the same source event after the
        // lease/event transaction committed. Context-bearing projection calls
        // are idempotent at the admission boundary: return the original
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

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let committed = self
            .admit_wake_in_tx(&mut transaction, &request, &context)
            .await?;
        transaction.commit().await?;
        if let Some((scope_type, scope_id)) = committed.stall_scope {
            self.publish_autonomy_stall(
                &scope_type,
                &scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
        }
        Ok(committed.result)
    }

    async fn admit_wake_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
    ) -> Result<CommittedWakeAdmission> {
        let mut stall_scope = None;
        let result = self
            .admit_wake_result_in_tx(transaction, request, context, &mut stall_scope)
            .await?;
        Ok(CommittedWakeAdmission {
            result,
            stall_scope,
        })
    }
    async fn admit_wake_result_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        stall_scope: &mut Option<(String, String)>,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now).unwrap_or_else(Utc::now);
        let lease_seconds = request.lease_seconds.clamp(1, 300);
        let cooldown_seconds = request.cooldown_seconds.clamp(1, 86_400);
        let leased_until = (now + Duration::seconds(lease_seconds)).to_rfc3339();
        let cooldown_until = (now + Duration::seconds(cooldown_seconds)).to_rfc3339();
        let now = now.to_rfc3339();

        // Attention is authoritative at the admission boundary.  A source
        // event can be delivered after an operator resolves its incident;
        // that event receives a terminal suppression rather than waking a
        // stale responder.
        let attention_status = if let Some(attention_id) = context.attention_id.as_deref() {
            sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
                .bind(attention_id)
                .fetch_optional(&mut **transaction)
                .await?
        } else {
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM attention_projection WHERE dedupe_key = ?",
            )
            .bind(&request.incident_key)
            .fetch_optional(&mut **transaction)
            .await?
        };
        if attention_status.as_deref() == Some("resolved") {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ResolvedIncident,
            });
        }

        // The Task row is the action authority. A recovery that wins after
        // projection but before admission must suppress the stale wake in the
        // same write transaction that would otherwise consume budget.
        if context.requires_current_task_intervention || context.orphan_execution_id.is_some() {
            let active = if let Some(execution_id) = context.orphan_execution_id.as_deref() {
                // A successor can commit between projection and admission.
                // Check again under the same writer lock as wake budgeting.
                orphan_attempt_is_current(&self.db, transaction, execution_id).await?
            } else if let Some(task_id) = context.task_id.as_deref() {
                let row = sqlx::query(
                    "SELECT error_annotation, blocked_json, failed_json
                     FROM task WHERE id = ? AND deleted_at IS NULL AND status NOT IN ('done','cancelled')",
                )
                .bind(task_id)
                .fetch_optional(&mut **transaction)
                .await?;
                row.is_some_and(|row| {
                    let error_annotation = row
                        .try_get::<Option<String>, _>("error_annotation")
                        .ok()
                        .flatten();
                    let blocked_json = row
                        .try_get::<Option<String>, _>("blocked_json")
                        .ok()
                        .flatten();
                    let failed_json = row
                        .try_get::<Option<String>, _>("failed_json")
                        .ok()
                        .flatten();
                    interruption_fields_require_intervention(
                        error_annotation.as_deref(),
                        blocked_json.as_deref(),
                        failed_json.as_deref(),
                    )
                })
            } else {
                false
            };
            if !active {
                if let Some(attention_id) = context.attention_id.as_deref() {
                    sqlx::query(
                        "UPDATE attention_projection
                         SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                             updated_at = ?, version = version + 1
                         WHERE id = ? AND version = ? AND status <> 'resolved'",
                    )
                    .bind(&now)
                    .bind(&now)
                    .bind(attention_id)
                    .bind(context.attention_version)
                    .execute(&mut **transaction)
                    .await?;
                }
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::ResolvedIncident,
                });
            }
        }

        if context.attention_id.is_none() {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ResolvedIncident,
            });
        }
        if let Some(id) = context.attention_id.as_deref() {
            let actual = self
                .db
                .get_attention_in_tx(transaction, id)
                .await?
                .ok_or(db::DbError::VersionConflict)?;
            if context
                .attention_version
                .is_some_and(|version| version != actual.version)
                || context.source_event_id.as_deref() != Some(actual.source_event_id.as_str())
            {
                return Err(db::DbError::VersionConflict.into());
            }
            let canonical = match request.scope_type.as_str() {
                "project" | "account" => {
                    Some((request.scope_type.clone(), request.scope_id.clone()))
                }
                "task" => sqlx::query_scalar::<_, String>(
                    "SELECT project_id FROM task WHERE id=? AND deleted_at IS NULL",
                )
                .bind(&request.scope_id)
                .fetch_optional(&mut **transaction)
                .await?
                .map(|id| ("project".to_owned(), id)),
                "agent_chat" => {
                    let chat =
                        sqlx::query("SELECT project_id,account_id FROM agent_chat WHERE id=?")
                            .bind(&request.scope_id)
                            .fetch_optional(&mut **transaction)
                            .await?;
                    chat.and_then(|row| {
                        row.try_get::<Option<String>, _>("project_id")
                            .ok()
                            .flatten()
                            .map(|id| ("project".to_owned(), id))
                            .or_else(|| {
                                row.try_get::<Option<String>, _>("account_id")
                                    .ok()
                                    .flatten()
                                    .map(|id| ("account".to_owned(), id))
                            })
                    })
                }
                _ => None,
            };
            if canonical.as_ref() != Some(&(actual.scope_type.clone(), actual.scope_id.clone())) {
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::IneligibleScope),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::IneligibleScope,
                });
            }
        }
        if context.turn.is_none() {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::SetupRequired {
                reason: WakeSetupReason::ResponderBindingMissing,
            });
        }
        let category = context
            .batch
            .first()
            .map(wake_budget_category)
            .unwrap_or("delivery");
        if category == "delivery" {
            let repeated: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_wake_disposition d JOIN agent_chat_turn_job j ON j.id=d.turn_job_id JOIN agent_identity i ON i.id=j.responder_identity_id JOIN agent_profile p ON p.id=i.selected_profile_id WHERE d.disposition='turn_admitted' AND d.incident_key=? AND d.incident_digest=? AND j.status='failed' AND j.profile_id=p.id AND j.profile_version=p.version)")
                .bind(&request.incident_key).bind(context.incident_digest.as_deref()).fetch_one(&mut **transaction).await?;
            if repeated {
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::RepeatedFailure),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::RepeatedFailure,
                });
            }
        }
        // A blocker digest gets one completed turn, and no member gets a second
        // turn while any linked turn is queued or running. Owner escalation
        // owns any later response.
        if category == "blocker" {
            let at = parse_rfc3339(&now).unwrap_or_else(Utc::now);
            for attention in &context.batch {
                let state = crate::wake_blocker::blocker_turn_state(
                    transaction,
                    &attention.id,
                    &wake_attention_incident_digest(attention),
                    None,
                    at,
                )
                .await?;
                if matches!(
                    state,
                    BlockerTurnState::InFlight | BlockerTurnState::Completed { .. }
                ) {
                    self.append_wake_decision_in_tx(
                        transaction,
                        request,
                        context,
                        WakeDecisionEvent::Suppressed(WakeSuppressionReason::DuplicateIncident),
                        None,
                        None,
                        &now,
                    )
                    .await?;
                    return Ok(WakeAdmissionResult::Suppressed {
                        reason: WakeSuppressionReason::DuplicateIncident,
                    });
                }
            }
            if let Some(project_id) = context
                .batch
                .first()
                .filter(|a| a.scope_type == "project")
                .map(|a| &a.scope_id)
            {
                let cooling: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_wake_batch WHERE project_id = ? AND cooldown_until > ?)")
                    .bind(project_id).bind(&now).fetch_one(&mut **transaction).await?;
                if cooling {
                    self.append_wake_decision_in_tx(
                        transaction,
                        request,
                        context,
                        WakeDecisionEvent::Suppressed(WakeSuppressionReason::Cooldown),
                        None,
                        None,
                        &now,
                    )
                    .await?;
                    return Ok(WakeAdmissionResult::Suppressed {
                        reason: WakeSuppressionReason::Cooldown,
                    });
                }
            }
        }
        let (total_budget, budget_scope_type, budget_scope_id) = self
            .wake_budget_in_tx(
                transaction,
                &request.identity_id,
                &request.scope_type,
                &request.scope_id,
            )
            .await?;
        // The owner initiated an answer wake; it never spends autonomy budget.
        let total_budget = if context.batch.first().is_some_and(is_owner_answer) {
            None
        } else {
            total_budget
        };
        // Small budgets share one pool rather than starving a bucket.
        let shared_pool = budget_scope_type == "project"
            && total_budget.is_some_and(|total| total < MIN_SPLIT_WAKE_BUDGET);
        let budget = total_budget.map(|total| {
            if budget_scope_type == "project" && !shared_pool {
                category_budget(total, category)
            } else {
                total
            }
        });
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
        .fetch_optional(&mut **transaction)
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
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(reason.clone()),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed { reason });
        }

        // Direct callers may not carry an Attention row (and therefore miss
        // the context-bearing replay check above).  Once an existing lease or
        // cooldown is ruled out, a matching admitted decision still means the
        // source already admitted its turn; return its metadata rather than
        // incrementing the budget a second time before the domain-event
        // dedupe turns the append into a no-op.
        let dedupe_key = wake_admitted_dedupe_key(request, context);
        let admitted_payload = sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&dedupe_key)
        .fetch_optional(&mut **transaction)
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
            return Ok(WakeAdmissionResult::Admitted {
                leased_until,
                cooldown_until,
                budget_remaining,
            });
        }

        if budget == Some(0) {
            *stall_scope = Some((budget_scope_type.clone(), budget_scope_id.clone()));
            if budget_scope_type == "project" {
                let open_incidents: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM attention_projection WHERE scope_type = 'project' AND scope_id = ? AND status = 'open'"
                ).bind(&budget_scope_id).fetch_one(&mut **transaction).await?;
                if open_incidents > 0 {
                    DomainEventRepo::append_event_in_tx(&*self.db, transaction, &CreateDomainEvent {
                        id: new_uuid_v4(), event_type: "notification.requested".to_owned(),
                        entity_type: "project".to_owned(), entity_id: budget_scope_id.clone(),
                        actor_type: "system".to_owned(), actor_id: None,
                        scope_type: "project".to_owned(), scope_id: budget_scope_id.clone(),
                        correlation_id: request.correlation_id.clone(), causation_id: request.causation_id.clone(),
                        causation_depth: request.reaction_depth,
                        dedupe_key: Some(format!("autonomy-stalled:{}:{}:{}:{}", budget_scope_type, budget_scope_id,
                            context.turn.as_ref().and_then(|p|p.prepared.responder.binding_id.as_deref()).unwrap_or_default(),
                            context.turn.as_ref().and_then(|p|p.prepared.responder.binding_version).unwrap_or_default())),
                        payload_json: json!({
                            "project_id": budget_scope_id, "task_id": null,
                            "event_type": "project.autonomy_stalled",
                            "title": format!("Project Agent stopped: {open_incidents} open incident(s) unanswered"),
                            "body": "the Project Agent's hourly wake budget is exhausted"
                        }).to_string(), created_at: now.clone(),
                    }).await?;
                }
            }
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::BudgetExhausted,
            });
        }

        let mut budget_attempt = transaction.begin().await?;
        let window_started =
            (parse_rfc3339(&now).unwrap_or_else(Utc::now) - Duration::hours(1)).to_rfc3339();
        let budget_remaining = if let Some(budget) = budget {
            let current = sqlx::query(
                "SELECT window_started_at, admitted_count
                 FROM agent_wake_budget_window
                 WHERE scope_type = ? AND scope_id = ? AND category = ?",
            )
            .bind(&budget_scope_type)
            .bind(&budget_scope_id)
            .bind(category)
            .fetch_optional(&mut *budget_attempt)
            .await?;
            let (admitted_count, in_window) = current
                .map(|row| {
                    let started: String = row.try_get("window_started_at")?;
                    let count: i64 = row.try_get("admitted_count")?;
                    Ok::<_, sqlx::Error>((count, started > window_started))
                })
                .transpose()?
                .unwrap_or((0, false));
            let used = if shared_pool {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COALESCE(SUM(admitted_count), 0) FROM agent_wake_budget_window
                     WHERE scope_type = ? AND scope_id = ? AND window_started_at > ?",
                )
                .bind(&budget_scope_type)
                .bind(&budget_scope_id)
                .bind(&window_started)
                .fetch_one(&mut *budget_attempt)
                .await?
            } else if in_window {
                admitted_count
            } else {
                0
            };
            if used >= budget {
                self.append_wake_decision_in_tx(
                    &mut budget_attempt,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                    Some(0),
                    Some((&budget_scope_type, &budget_scope_id)),
                    &now,
                )
                .await?;
                budget_attempt.commit().await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::BudgetExhausted,
                });
            }
            if in_window {
                sqlx::query(
                    "UPDATE agent_wake_budget_window
                     SET admitted_count = admitted_count + 1, identity_id = ?,
                         version = version + 1, updated_at = ?
                     WHERE scope_type = ? AND scope_id = ? AND category = ?",
                )
                .bind(&request.identity_id)
                .bind(&now)
                .bind(&budget_scope_type)
                .bind(&budget_scope_id)
                .bind(category)
                .execute(&mut *budget_attempt)
                .await?;
            } else {
                sqlx::query(
                    "INSERT INTO agent_wake_budget_window (
                        identity_id, scope_type, scope_id, category, window_started_at,
                        window_seconds, admitted_count, version, updated_at
                     ) VALUES (?, ?, ?, ?, ?, 3600, 1, 1, ?)
                     ON CONFLICT(scope_type, scope_id, category) DO UPDATE SET
                        window_started_at = excluded.window_started_at,
                        identity_id = excluded.identity_id,
                        admitted_count = 1,
                        version = agent_wake_budget_window.version + 1,
                        updated_at = excluded.updated_at",
                )
                .bind(&request.identity_id)
                .bind(&budget_scope_type)
                .bind(&budget_scope_id)
                .bind(category)
                .bind(&now)
                .bind(&now)
                .execute(&mut *budget_attempt)
                .await?;
            }
            Some((budget - used - 1).max(0))
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
        .execute(&mut *budget_attempt)
        .await?;
        if lease_result.rows_affected() == 0 {
            // This is normally covered by the global pre-check.  Keep the
            // compare-and-swap result authoritative for same-transaction
            // replays and classify an expired lease/cooldown conservatively.
            budget_attempt.rollback().await?;
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::DuplicateIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::DuplicateIncident,
            });
        }

        budget_attempt.commit().await?;
        // The lease, budget increment, and admitted wake audit share one
        // transaction.  No budget is consumed for suppressed/setup-required
        // decision events.
        self.append_wake_decision_in_tx(
            transaction,
            request,
            context,
            WakeDecisionEvent::Admitted {
                leased_until: leased_until.clone(),
                cooldown_until: cooldown_until.clone(),
            },
            budget_remaining,
            Some((&budget_scope_type, &budget_scope_id)),
            &now,
        )
        .await?;
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
                "turn_admitted",
                "turn",
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
            "turn_job_id": (event_type == "agent.wake.admitted").then(|| context.turn.as_ref().map(|p|
                Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{}:turn", p.prepared.dedupe_key).as_bytes()).to_string())).flatten(),
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
        let event = DomainEventRepo::append_event_in_tx(
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
        .await?;
        let (kind, admission, expected) = match &decision {
            WakeDecisionEvent::Admitted { .. } => {
                let prepared = context
                    .turn
                    .as_ref()
                    .ok_or_else(|| ServiceError::invalid_operation("wake has no prepared turn"))?;
                let mut input = (**prepared).clone();
                let id = context
                    .attention_id
                    .as_deref()
                    .ok_or_else(|| ServiceError::invalid_operation("wake has no Attention"))?;
                let actual = self
                    .db
                    .get_attention_in_tx(transaction, id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                input.attention = actual.clone();
                let admitted = crate::WakeTurnConsumer::new(Arc::clone(&self.db))
                    .build_prepared_turn(&event, &input)?;
                (
                    db::AgentWakeDispositionKind::TurnAdmitted,
                    Some(admitted),
                    Some(expected_attention(&actual)),
                )
            }
            WakeDecisionEvent::Suppressed(_) => (
                db::AgentWakeDispositionKind::DeterministicallySuppressed,
                None,
                None,
            ),
            WakeDecisionEvent::SetupRequired(_) if context.attention_id.is_some() => {
                (db::AgentWakeDispositionKind::SetupRequired, None, None)
            }
            WakeDecisionEvent::SetupRequired(_) => (
                db::AgentWakeDispositionKind::DeterministicallySuppressed,
                None,
                None,
            ),
        };
        // The latest decision per Attention item (each batch member, with its
        // own digest) is what the sweep reads.
        let disposition_name = kind.to_string();
        let members: Vec<(String, String)> = if context.batch.is_empty() {
            context
                .attention_id
                .iter()
                .map(|id| (id.clone(), incident_digest.clone()))
                .collect()
        } else {
            context
                .batch
                .iter()
                .map(|a| (a.id.clone(), wake_attention_incident_digest(a)))
                .collect()
        };
        for (id, digest) in members {
            sqlx::query(
                "INSERT INTO agent_wake_attention_latest(attention_id,incident_digest,disposition,reason,identity_id,updated_at)
                 SELECT ?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM attention_projection WHERE id=?)
                 ON CONFLICT(attention_id) DO UPDATE SET incident_digest=excluded.incident_digest,
                    disposition=excluded.disposition, reason=excluded.reason,
                    identity_id=excluded.identity_id, updated_at=excluded.updated_at",
            )
            .bind(&id)
            .bind(&digest)
            .bind(&disposition_name)
            .bind(reason_code)
            .bind(identity_id.as_deref())
            .bind(now)
            .bind(&id)
            .execute(&mut **transaction)
            .await?;
        }
        // Audit-event dedupe is checked before persistence, making a replay a no-op.
        if self
            .db
            .get_agent_wake_disposition_in_tx(
                transaction,
                crate::wake_turn_consumer_name(),
                &event.id,
            )
            .await?
            .is_some()
        {
            return Ok(event);
        }
        let turn_job_id = admission.as_ref().map(|a| a.turn.id.clone());
        let disposition = db::CreateAgentWakeDisposition {
            id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("wake-disposition:{}", event.id).as_bytes(),
            )
            .to_string(),
            consumer_name: crate::wake_turn_consumer_name().to_owned(),
            source_event_id: event.id.clone(),
            source_event_sequence: event.sequence,
            attempt_number: 1,
            max_attempts: 3,
            disposition: kind,
            reason: reason_code.to_owned(),
            turn_job_id: turn_job_id.clone(),
            attention_id: (kind == db::AgentWakeDispositionKind::SetupRequired)
                .then(|| context.attention_id.clone())
                .flatten(),
            retry_at: None,
            incident_key: Some(bounded_wake_ref(&request.incident_key)),
            incident_digest: Some(incident_digest),
            binding_id: admission
                .as_ref()
                .and_then(|a| a.turn.responder_binding_id.clone()),
            binding_version: admission
                .as_ref()
                .and_then(|a| a.turn.responder_binding_version),
            profile_id: admission.as_ref().map(|a| a.turn.profile_id.clone()),
            profile_version: admission.as_ref().and_then(|a| a.turn.profile_version),
            provenance_json: Some(json!({"attention_id": context.attention_id}).to_string()),
            parent_disposition_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        };
        self.db
            .persist_agent_wake_in_tx(
                transaction,
                &event,
                db::PersistAgentWake {
                    disposition,
                    admission,
                    expected_attention: expected,
                },
            )
            .await?;
        if let Some(turn_id) = turn_job_id {
            if context
                .batch
                .first()
                .is_some_and(|a| wake_budget_category(a) == "blocker")
            {
                for item in &context.batch {
                    let current = self
                        .db
                        .get_attention_in_tx(transaction, &item.id)
                        .await?
                        .ok_or(db::DbError::VersionConflict)?;
                    if current.status != "open"
                        || current.version != item.version
                        || wake_attention_incident_digest(&current)
                            != wake_attention_incident_digest(item)
                    {
                        return Err(db::DbError::VersionConflict.into());
                    }
                    // A re-admission replaces a failed or imported row for the digest.
                    sqlx::query(
                        "INSERT INTO agent_wake_blocker(attention_id,incident_digest,turn_job_id,admitted_at) VALUES(?,?,?,?)
                         ON CONFLICT(attention_id,incident_digest) DO UPDATE SET turn_job_id=excluded.turn_job_id,
                            admitted_at=excluded.admitted_at, escalation_id=NULL, recorded_outcome=NULL,
                            legacy_source_event_id=NULL",
                    )
                    .bind(&item.id)
                    .bind(wake_attention_incident_digest(item))
                    .bind(&turn_id)
                    .bind(now)
                    .execute(&mut **transaction)
                    .await?;
                }
                sqlx::query("INSERT INTO agent_wake_batch(project_id,admitted_at,cooldown_until,turn_job_id) VALUES(?,?,?,?) ON CONFLICT(project_id) DO UPDATE SET admitted_at=excluded.admitted_at,cooldown_until=excluded.cooldown_until,turn_job_id=excluded.turn_job_id")
                    .bind(&context.batch[0].scope_id).bind(now).bind(cooldown_until).bind(&turn_id).execute(&mut **transaction).await?;
            }
            // The owner's answer turn becomes the escalated blockers' turn, so
            // they are re-armed: if they persist after it, they escalate again.
            if let Some(escalation_id) = context.batch.first().and_then(answered_escalation_id) {
                sqlx::query(
                    "UPDATE agent_wake_blocker SET turn_job_id = ?, escalation_id = NULL,
                        recorded_outcome = NULL, admitted_at = ?
                     WHERE escalation_id = ?",
                )
                .bind(&turn_id)
                .bind(now)
                .bind(&escalation_id)
                .execute(&mut **transaction)
                .await?;
            }
        }
        Ok(event)
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
        // Resolving an owner escalation is the owner's answer: it closes the
        // escalation and wakes the Agent, so no blocker stays un-wakeable.
        if current.recommended_action == "answer_escalation" {
            if current.version != expected_version {
                return Err(ServiceError::from(db::DbError::VersionConflict));
            }
            crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                .resolve_by_owner(&current, user_id)
                .await?;
            return self
                .db
                .get_attention(id)
                .await?
                .ok_or_else(|| ServiceError::not_found("attention", id.to_owned()));
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
        let capacity = self
            .capacity(
                user_id,
                project_id,
                !health
                    .as_ref()
                    .is_some_and(|health| health.stale || health.last_error_code.is_some()),
            )
            .await?;
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
        let row = sqlx::query("SELECT h.*, COALESCE(c.last_sequence, 0) AS last_sequence
            FROM worker_health h LEFT JOIN event_consumer_cursor c ON c.consumer_name = h.worker_name WHERE h.worker_name = ?")
            .bind(CONSUMER_NAME).fetch_optional(self.db.pool()).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let lag = self.db.domain_event_consumer_lag(&[CONSUMER_NAME]).await?;
        let stale = lag
            .first()
            .is_some_and(|lag| lag.stalled(Utc::now(), CONSUMER_STALE_SECONDS));
        let mut last_error_code: Option<String> = row.try_get("last_error_kind")?;
        let mut last_error_message: Option<String> = row.try_get("last_error")?;
        if last_error_message.is_none() {
            let recent: Option<(String, String)> = sqlx::query_as("SELECT error_kind, last_error FROM worker_dead_letter WHERE worker_name = ? AND resolved_at IS NULL AND dead_lettered_at >= ? ORDER BY dead_lettered_at DESC LIMIT 1")
                .bind(CONSUMER_NAME).bind((Utc::now() - Duration::hours(1)).to_rfc3339()).fetch_optional(self.db.pool()).await?;
            if let Some((kind, message)) = recent {
                last_error_code = Some(kind);
                last_error_message = Some(message);
            }
        }
        Ok(Some(AttentionConsumerHealthResponse {
            consumer_name: CONSUMER_NAME.to_owned(),
            last_sequence: row.try_get("last_sequence")?,
            last_success_at: row.try_get("last_success_at")?,
            last_error_code,
            last_error_message,
            stale,
            updated_at: row.try_get("updated_at")?,
        }))
    }

    /// A Task entering `review` is only attention when a person must decide
    /// the gate. A review run by the workflow's reviewer Agent settles itself;
    /// waking the Project Agent for it sends the Agent to a `task.action`
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

    async fn resolve_superseded_turn_incidents(&self) -> Result<()> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT a.dedupe_key, e.entity_id, e.id, e.payload_json FROM attention_projection a
             JOIN domain_event e ON e.id = a.source_event_id
             WHERE a.status <> 'resolved' AND e.event_type = 'agent_chat.turn.failed'",
        )
        .fetch_all(self.db.pool())
        .await?;
        for (key, turn_id, event_id, payload) in rows {
            if let Some(turn) =
                db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &turn_id).await?
            {
                let version = serde_json::from_str::<Value>(&payload)
                    .ok()
                    .and_then(|value| value.get("version").and_then(Value::as_i64));
                if turn.retry_action().is_none() || version.is_some_and(|v| v != turn.version) {
                    AttentionRepo::resolve_attention_by_dedupe(
                        &*self.db,
                        &key,
                        &event_id,
                        &now_rfc3339(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn prepare_projection(&self, event: &DomainEvent) -> Result<Outcome<PreparedAttention>> {
        let transition_attention = if event.event_type.eq_ignore_ascii_case("task.transitioned") {
            Some(task_transition_attention(event))
        } else {
            None
        };
        let category = match transition_attention {
            Some(transition) => transition.category,
            None => match self.event_category(event).await? {
                EventCategoryDecision::Ready(category) => category,
                EventCategoryDecision::Deferred => {
                    return Ok(Outcome::Defer {
                        after: StdDuration::from_secs(1),
                        reason: "terminal Task disposition has not settled".to_owned(),
                    })
                }
            },
        };
        let mut incident = None;
        if let Some(category) = category {
            let review_is_current =
                category != "review_ready" || self.review_needs_a_person(event).await?;
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
            let failed_turn = if event.event_type == "agent_chat.turn.failed" {
                db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &event.entity_id)
                    .await?
            } else {
                None
            };
            // Re-drive or completion may precede this consumer's projection.
            if failed_turn.as_ref().is_some_and(|turn| {
                turn.retry_action().is_none()
                    || serde_json::from_str::<Value>(&event.payload_json)
                        .ok()
                        .and_then(|value| value.get("version").and_then(Value::as_i64))
                        .is_some_and(|v| v != turn.version)
            }) {
                return Ok(Outcome::Skip);
            }
            let retry_action = failed_turn
                .as_ref()
                .and_then(db::AgentChatTurnJob::retry_action);
            let cause = failed_turn
                .as_ref()
                .and_then(|turn| turn.failure_class.as_ref());
            let summary = if category == "conflict_hotspot" {
                let payload =
                    serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
                format!(
                    "{} conflicted in {} Tasks this week",
                    payload
                        .get("path")
                        .and_then(Value::as_str)
                        .unwrap_or("Shared file"),
                    payload
                        .get("handoff_count")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                )
            } else if retry_action.is_some() {
                format!(
                    "Agent Chat turn failed: {}",
                    cause
                        .map(api_types::TurnFailure::code)
                        .unwrap_or("backend_failed")
                )
            } else {
                summary.to_owned()
            };
            let recommended_action = if retry_action.is_some() {
                "retry_turn"
            } else {
                recommended_action
            };
            let task_context = if event.entity_type == "task" {
                sqlx::query("SELECT title, status, version FROM task WHERE id = ?")
                    .bind(&event.entity_id)
                    .fetch_optional(self.db.pool())
                    .await?
                    .map(|row| {
                        json!({
                            "task_title": bounded_text(row.try_get::<String, _>("title").unwrap_or_default()),
                            "task_status": row.try_get::<String, _>("status").unwrap_or_default(),
                            "task_version": row.try_get::<i64, _>("version").unwrap_or_default(),
                        })
                    })
            } else {
                None
            };
            let event_payload =
                serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
            // What the user decided, so the wake prompt can say it back to
            // the Agent instead of making it rediscover the decision.
            let decision_context = user_decision_outcome(event).map(|outcome| {
                let payload_text = |key: &str| {
                    event_payload
                        .get(key)
                        .and_then(Value::as_str)
                        .map(|value| bounded_text(value.to_owned()))
                };
                json!({
                    "outcome": outcome,
                    "target_type": event.entity_type,
                    "target_id": event.entity_id,
                    "revision_id": payload_text("revision_id"),
                    "lifecycle": payload_text("lifecycle"),
                    "event_type": event.event_type,
                    "decided_by": "user",
                })
            });
            let interruption = event_payload
                .get("interruption")
                .cloned()
                .unwrap_or(Value::Null);
            let available_actions = if event.entity_type == "task" {
                if let Some(task) =
                    db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await?
                {
                    if let Some(project) =
                        db::ProjectRepo::get_by_id(&*self.db, &task.project_id).await?
                    {
                        let actor = match db::ProjectAgentBindingRepo::get_active_project_binding(
                            &*self.db,
                            &task.project_id,
                        )
                        .await?
                        .filter(|binding| binding.state == "active")
                        .and_then(|binding| binding.identity_id)
                        {
                            Some(agent_id) => api_types::Actor::agent(agent_id),
                            None => api_types::Actor::system(api_types::SystemComponent::Workflow),
                        };
                        let workflow =
                            crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
                                &task,
                                &project.workflow_definition,
                                &actor,
                            );
                        let snapshot = crate::task_actions::load_snapshot(
                            &self.db,
                            task,
                            workflow,
                            &actor,
                            self.action_connections.as_deref(),
                        )
                        .await?;
                        crate::available_actions(&snapshot)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let requires_intervention = event_payload
                .get("requires_intervention")
                .and_then(Value::as_bool)
                .unwrap_or(category == "execution_failed");
            let mut details = json!({
                "source_event_id": event.id,
                "source_event_type": event.event_type,
                "source_sequence": event.sequence,
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "task": task_context,
                "failure_class": cause,
                "retry_decision": failed_turn.as_ref().and_then(|turn| turn.retry_decision.as_ref()),
                "retry_action": retry_action,
                "decision": decision_context,
                "role": event_payload.get("role").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "stop_reason": event_payload.get("stop_reason").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "error": event_payload.get("error").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "interruption": interruption,
                "recovery": {
                    "requires_intervention": requires_intervention,
                    "actions": available_actions,
                    "automatic_retry": false,
                },
            });
            if event.event_type == "project.escalation.answered" {
                details["answer"] = event_payload.get("answer").cloned().unwrap_or(Value::Null);
                details["task_ids"] = event_payload
                    .get("task_ids")
                    .cloned()
                    .unwrap_or(Value::Null);
            }
            if category == "conflict_hotspot" {
                details["conflict_hotspot"] = event_payload.clone();
            }
            let details_json = serde_json::to_string(&details)
                .map_err(|error| ServiceError::Domain(error.to_string()))?;
            let projection = CreateAttentionProjection {
                id: new_uuid_v4(),
                attention_type: category.to_owned(),
                scope_type: attention_scope_type,
                scope_id: attention_scope_id,
                identity_id: identity_id.clone(),
                source_event_id: event.id.clone(),
                priority,
                status: "open".to_owned(),
                summary: bounded_summary(&summary),
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
            };
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
            let decision = if let Some(reason) = wake_policy_suppression_reason(&request) {
                PreparedWakeDecision::Suppressed(reason)
            } else {
                let configured = self
                    .wake_responder_is_configured(&scope_type, &scope_id, None)
                    .await?;
                let eligible = match identity_id.as_deref() {
                    Some(id) => {
                        self.wake_responder_is_configured(&scope_type, &scope_id, Some(id))
                            .await?
                            && self
                                .wake_identity_is_eligible(id, &scope_type, &scope_id)
                                .await?
                    }
                    None => false,
                };
                match identity_id {
                    Some(_) if configured && eligible => PreparedWakeDecision::Admit,
                    Some(_) if configured => {
                        PreparedWakeDecision::Suppressed(WakeSuppressionReason::IneligibleScope)
                    }
                    _ => PreparedWakeDecision::SetupRequired,
                }
            };
            let turn = if matches!(decision, PreparedWakeDecision::Admit)
                && !(projection.scope_type == "project" && blocker_category(category))
            {
                let candidate = projection_snapshot(&projection);
                let context = WakeDecisionContext {
                    source_event_id: Some(event.id.clone()),
                    incident_digest: Some(wake_attention_incident_digest(&candidate)),
                    ..Default::default()
                };
                crate::WakeTurnConsumer::new(Arc::clone(&self.db))
                    .prepare_attention(
                        &candidate,
                        &wake_admitted_dedupe_key(&request, &context),
                        request.causation_id.as_deref(),
                        request.reaction_depth,
                        &[],
                    )
                    .await?
                    .map(Arc::new)
            } else {
                None
            };
            incident = Some(PreparedAttentionIncident {
                projection,
                turn,
                request,
                decision,
                failed_turn,
                review_is_current,
                requires_intervention,
                orphan_execution_id: (category == "execution_failed"
                    && matches!(
                        event.event_type.as_str(),
                        "execution.failed" | "execution.cancelled"
                    ))
                .then(|| {
                    event_payload
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten(),
            });
        }

        let mut resolved_categories = resolution_categories(event);
        if transition_attention.is_some_and(|transition| transition.terminal) {
            resolved_categories.extend([
                "validation_failed",
                "run_stalled",
                "progress_warning",
                "retry_exhausted",
                "review_ready",
                "review_risk",
                "execution_failed",
            ]);
        }
        let mut resolutions = Vec::new();
        for category in resolved_categories {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            resolutions.push(attention_incident_key(
                category,
                event,
                &scope_type,
                &scope_id,
            ));
        }
        let followup_scope = if event.event_type == "milestone.readiness.evaluated" {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            Some(
                self.attention_projection_scope(&scope_type, &scope_id)
                    .await?,
            )
        } else {
            None
        };
        if incident.is_none() && resolutions.is_empty() && followup_scope.is_none() {
            return Ok(Outcome::Skip);
        }
        Ok(Outcome::Done(PreparedAttention {
            incident,
            resolutions,
            followup_scope,
        }))
    }

    async fn commit_projection(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &PreparedAttention,
    ) -> Result<Option<(String, String)>> {
        let mut stall = None;
        if let Some(p) = &prepared.incident {
            let admitted: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM domain_event WHERE causation_id = ? AND event_type = 'agent.wake.admitted')")
                .bind(&event.id).fetch_one(&mut **tx).await?;
            if admitted != 0 {
                return Ok(None);
            }
            let attention = self
                .db
                .insert_attention_in_tx(tx, p.projection.clone())
                .await?;
            let stale = if let Some(turn) = &p.failed_turn {
                self.db
                    .get_agent_chat_turn_job_in_tx(tx, &turn.id)
                    .await?
                    .is_some_and(|current| {
                        current.retry_action().is_none() || current.version != turn.version
                    })
            } else {
                false
            };
            if stale || !p.review_is_current {
                self.db
                    .resolve_attention_by_dedupe_in_tx(
                        tx,
                        &p.projection.dedupe_key,
                        &event.id,
                        &now_rfc3339(),
                    )
                    .await?;
                return Ok(None);
            }
            let context = WakeDecisionContext {
                attention_id: Some(attention.id.clone()),
                source_event_id: Some(event.id.clone()),
                incident_digest: Some(wake_attention_incident_digest(&attention)),
                attention_status: Some(attention.status.clone()),
                attention_version: Some(attention.version),
                task_id: (event.event_type == "task.interruption_changed")
                    .then(|| event.entity_id.clone()),
                requires_current_task_intervention: event.event_type == "task.interruption_changed"
                    && p.requires_intervention,
                orphan_execution_id: p.orphan_execution_id.clone(),
                turn: p.turn.clone(),
                batch: vec![attention.clone()],
            };
            match &p.decision {
                PreparedWakeDecision::Admit
                    if attention.scope_type == "project"
                        && blocker_category(&attention.attention_type) =>
                {
                    // A single Project wake is assembled after the projection window.
                }
                PreparedWakeDecision::Admit => {
                    let committed = self.admit_wake_in_tx(tx, &p.request, &context).await?;
                    stall = committed.stall_scope;
                }
                PreparedWakeDecision::Suppressed(reason) => {
                    self.append_wake_decision_in_tx(
                        tx,
                        &p.request,
                        &context,
                        WakeDecisionEvent::Suppressed(reason.clone()),
                        None,
                        None,
                        &p.request.now,
                    )
                    .await?;
                }
                PreparedWakeDecision::SetupRequired => {
                    self.append_wake_decision_in_tx(
                        tx,
                        &p.request,
                        &context,
                        WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
                        None,
                        None,
                        &p.request.now,
                    )
                    .await?;
                }
            }
        }
        for key in &prepared.resolutions {
            self.db
                .resolve_attention_by_dedupe_in_tx(tx, key, &event.id, &now_rfc3339())
                .await?;
        }
        if let Some((scope_type, scope_id)) = &prepared.followup_scope {
            let at = now_rfc3339();
            sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                source_event_id = ?, updated_at = ?, version = version + 1
                WHERE attention_type = 'delivery_followup' AND scope_type = ? AND scope_id = ? AND status <> 'resolved'")
                .bind(&at).bind(&event.id).bind(&at).bind(scope_type).bind(scope_id).execute(&mut **tx).await?;
        }
        Ok(stall)
    }

    /// Decide whether a durable event represents an actionable incident.
    /// Raw terminal execution events are audit facts, not recovery commands:
    /// the atomic `task.interruption_changed` event normally owns Attention.
    /// A manual terminal attempt that never receives any Task disposition is
    /// retained as a bounded-grace orphan safety net.
    async fn event_category(&self, event: &DomainEvent) -> Result<EventCategoryDecision> {
        if event.event_type == "project.escalation.answered" {
            return Ok(EventCategoryDecision::Ready(Some(
                DECISION_RECORDED_CATEGORY,
            )));
        }
        if event
            .event_type
            .eq_ignore_ascii_case("task.interruption_changed")
            && classify_event(event).is_some()
        {
            let current = db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await?;
            if !current.as_ref().is_some_and(task_requires_intervention) {
                return Ok(EventCategoryDecision::Ready(None));
            }
        }
        if !matches!(
            event.event_type.to_ascii_lowercase().as_str(),
            "execution.failed" | "execution.cancelled"
        ) {
            return Ok(EventCategoryDecision::Ready(classify_event(event)));
        }

        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(execution_id) = payload.get("execution_id").and_then(Value::as_str) else {
            return Ok(EventCategoryDecision::Ready(None));
        };
        let Some(execution) = db::ExecutionRepo::get_by_id(&*self.db, execution_id).await? else {
            return Ok(EventCategoryDecision::Ready(None));
        };
        if matches!(
            execution.resume_policy,
            Some(db::ResumePolicy::Auto | db::ResumePolicy::None)
        ) || matches!(
            execution.stop_reason,
            Some(db::StopReason::UserCancelled | db::StopReason::RoleReassigned)
        ) {
            return Ok(EventCategoryDecision::Ready(None));
        }
        // Recovery leaves the stopped attempt immutable. Its event may be
        // replayed after a successor has started or even finished, so the old
        // attempt's Manual policy alone cannot establish an orphan.
        let mut transaction = self.db.pool().begin().await?;
        let current = orphan_attempt_is_current(&self.db, &mut transaction, &execution.id).await?;
        transaction.commit().await?;
        if !current {
            return Ok(EventCategoryDecision::Ready(None));
        }

        let settled_before = Utc::now() - Duration::seconds(TERMINAL_DISPOSITION_GRACE_SECONDS);
        let terminal_at = parse_rfc3339(&event.created_at).unwrap_or(settled_before);
        if terminal_at > settled_before {
            return Ok(EventCategoryDecision::Deferred);
        }

        tracing::warn!(
            task_id = %execution.task_id,
            execution_id = %execution.id,
            event_id = %event.id,
            "terminal execution remained manually resumable without a Task disposition"
        );
        Ok(EventCategoryDecision::Ready(Some("execution_failed")))
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
        if event.event_type == "task.transitioned" {
            if let Ok(payload) =
                serde_json::from_str::<api_types::TaskTransitionEventPayload>(&event.payload_json)
            {
                if payload.known_workflow_snapshot().is_some() {
                    return Ok(("project".to_owned(), payload.project_id));
                }
            }
        }
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
        healthy: bool,
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
            healthy,
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

    async fn agent_usage_summary(&self, identity_id: &str) -> Result<UsageAggregate> {
        crate::usage_projection::usage_aggregate_for_agent(&self.db, identity_id).await
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
    if category == "conflict_hotspot" {
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let path = payload
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        return crate::worker_runtime::conflict_hotspot::incident_key(scope_id, path);
    }
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

fn task_requires_intervention(task: &db::Task) -> bool {
    interruption_fields_require_intervention(
        task.error_annotation.as_deref(),
        task.blocked_json.as_deref(),
        task.failed_json.as_deref(),
    )
}

async fn orphan_attempt_is_current(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    execution_id: &str,
) -> Result<bool> {
    let task_id = sqlx::query_scalar::<_, String>(
        "SELECT candidate.task_id FROM execution candidate
            WHERE candidate.id = ? AND candidate.status IN ('failed', 'cancelled')
              AND (candidate.resume_policy IS NULL OR candidate.resume_policy = 'manual')
              AND (candidate.stop_reason IS NULL OR candidate.stop_reason NOT IN ('user_cancelled', 'role_reassigned'))
              AND NOT EXISTS (
                  SELECT 1 FROM execution successor
                  WHERE successor.task_id = candidate.task_id AND successor.id <> candidate.id
                    AND (successor.status = 'running'
                         OR successor.parent_execution_id = candidate.id
                         OR successor.created_at > candidate.created_at)
              )",
    )
    .bind(execution_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(task_id) = task_id else {
        return Ok(false);
    };
    let Some(task) = db::TaskRepo::get_by_id_in_tx(db, transaction, &task_id, false).await? else {
        return Ok(false);
    };
    // A committed disposition owns its own incident, including an intentional
    // manual stop. Raw attempt events must not override those recovery choices.
    if task.error_annotation.is_some()
        || task_requires_intervention(&task)
        || crate::deferred_dispatch::is_pending(&task, Utc::now())
    {
        return Ok(false);
    }
    let workflow_json =
        sqlx::query_scalar::<_, String>("SELECT workflow_definition FROM project WHERE id = ?")
            .bind(&task.project_id)
            .fetch_optional(&mut **transaction)
            .await?;
    let Some(workflow_json) = workflow_json else {
        return Ok(false);
    };
    let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
        &task,
        &workflow_json,
        &api_types::Actor::system(api_types::SystemComponent::Workflow),
    );
    Ok(workflow.state_kind(&task.status) != Some(api_types::StateKind::Terminal))
}

/// Attention category for a decision the user recorded on something the
/// Project Agent was waiting for. The wake consumer resolves the incident
/// once the wake turn is admitted; it is a hand-off, not a lasting incident.
pub const DECISION_RECORDED_CATEGORY: &str = "decision_recorded";

/// The category an incident key was minted for (`attention:<category>:…`).
pub fn incident_key_category(incident_key: &str) -> Option<&str> {
    incident_key
        .strip_prefix("attention:")
        .and_then(|rest| rest.split(':').next())
        .filter(|category| !category.is_empty())
}

/// `approved` / `rejected` when `event` is a decision the *user* recorded on
/// a pending Agent proposal. Only user-authored events qualify: an Agent's
/// own approval must never wake the Agent that made it.
fn user_decision_outcome(event: &DomainEvent) -> Option<&'static str> {
    if event.actor_type != "user" {
        return None;
    }
    match event.event_type.to_ascii_lowercase().as_str() {
        "milestone.definition.transitioned" => {
            let lifecycle = serde_json::from_str::<Value>(&event.payload_json)
                .ok()
                .and_then(|payload| {
                    payload
                        .get("lifecycle")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
            match lifecycle.as_deref() {
                Some("approved") => Some("approved"),
                Some("rejected") => Some("rejected"),
                _ => None,
            }
        }
        "project.decision.approved" | "project.document.approved" => Some("approved"),
        "project.decision.candidate_rejected" => Some("rejected"),
        _ => None,
    }
}

#[derive(Clone, Copy, Default)]
struct TaskTransitionAttention {
    category: Option<&'static str>,
    terminal: bool,
}

fn task_transition_attention(event: &DomainEvent) -> TaskTransitionAttention {
    let payload =
        serde_json::from_str::<api_types::TaskTransitionEventPayload>(&event.payload_json);
    let Some(payload) = payload.ok() else {
        return TaskTransitionAttention::default();
    };
    let Some(snapshot) = payload.known_workflow_snapshot() else {
        tracing::debug!(event_id = %event.id, "Task transition has unknown historical workflow semantics");
        return TaskTransitionAttention::default();
    };
    if snapshot.from_state.name == snapshot.to_state.name {
        return TaskTransitionAttention::default();
    }
    let target = &snapshot.to_state;
    let terminal = target.kind == api_types::StateKind::Terminal;
    let category = if terminal && !target.is_cancellation {
        Some("delivery_followup")
    } else if target.kind == api_types::StateKind::Gate
        && target.canonical_phase == api_types::CanonicalPhase::Review
        && target.requires_user_approval
    {
        Some("review_ready")
    } else {
        None
    };
    TaskTransitionAttention { category, terminal }
}

fn classify_event(event: &DomainEvent) -> Option<&'static str> {
    let event_type = event.event_type.to_ascii_lowercase();
    if user_decision_outcome(event).is_some() {
        return Some(DECISION_RECORDED_CATEGORY);
    }
    if event_type == crate::worker_runtime::conflict_hotspot::DETECTED_EVENT {
        return Some("conflict_hotspot");
    }
    if event_type == "project_release.candidate_requested" {
        return Some("human_input_required");
    }
    if event_type == "agent.question.created" || event_type == "agent.interaction.required" {
        return Some("human_input_required");
    }
    if event_type == "agent_chat.turn.failed" {
        let payload = serde_json::from_str::<Value>(&event.payload_json).ok()?;
        let failure: api_types::TurnFailure =
            serde_json::from_value(payload.get("failure_class")?.clone()).ok()?;
        return (payload.get("status").and_then(Value::as_str) == Some("failed")
            && failure.requires_attention())
        .then_some("retry_exhausted");
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
    if matches!(event_type.as_str(), "task.done" | "task.completed") {
        return Some("delivery_followup");
    }
    if event_type == "task.interruption_changed" {
        return serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|payload| {
                payload
                    .get("requires_intervention")
                    .and_then(Value::as_bool)
            })
            .unwrap_or(false)
            .then_some("execution_failed");
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
    if event_type == "execution.completed" {
        categories.push("execution_failed");
    }
    if event_type == "task.interruption_changed"
        && !serde_json::from_str::<Value>(&event.payload_json)
            .ok()
            .and_then(|payload| {
                payload
                    .get("requires_intervention")
                    .and_then(Value::as_bool)
            })
            .unwrap_or(false)
    {
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
        "execution_failed" => (85, "Task needs recovery", "inspect_task"),
        "delivery_followup" => (
            70,
            "Task completed; reconcile validation, evidence, and readiness",
            "reconcile_delivery",
        ),
        "decision_recorded" => (
            65,
            "The user recorded a decision; continue from it",
            "continue_from_decision",
        ),
        "runtime_offline" => (95, "Agent runtime is unavailable", "restore_runtime"),
        "budget_threshold" => (60, "Agent budget threshold reached", "review_budget"),
        "commitment_overdue" => (75, "Commitment is overdue", "review_commitment"),
        "conflict_hotspot" => (60, "Shared file repeatedly conflicted", "split_hotspot"),
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
        "decision_recorded" => AttentionCategory::DecisionRecorded,
        "runtime_offline" | "environment_not_ready" => AttentionCategory::RuntimeOffline,
        "budget_threshold" => AttentionCategory::BudgetThreshold,
        "commitment_overdue" => AttentionCategory::CommitmentOverdue,
        "conflict_hotspot" => AttentionCategory::ConflictHotspot,
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

struct CommittedWakeAdmission {
    result: WakeAdmissionResult,
    stall_scope: Option<(String, String)>,
}
pub struct PreparedAttention {
    incident: Option<PreparedAttentionIncident>,
    resolutions: Vec<String>,
    followup_scope: Option<(String, String)>,
}
struct PreparedAttentionIncident {
    projection: CreateAttentionProjection,
    turn: Option<Arc<crate::wake_turn_consumer::PreparedWakeTurn>>,
    request: WakeAdmissionRequest,
    decision: PreparedWakeDecision,
    failed_turn: Option<db::AgentChatTurnJob>,
    review_is_current: bool,
    requires_intervention: bool,
    orphan_execution_id: Option<String>,
}
enum PreparedWakeDecision {
    Admit,
    Suppressed(WakeSuppressionReason),
    SetupRequired,
}
fn projection_worker_error(error: ServiceError) -> WorkerError {
    let message = bounded_error_message(&error);
    let kind = crate::worker_runtime::consumer_error(error).kind;
    match kind {
        crate::worker_runtime::WorkerErrorKind::Failure => WorkerError::new(message),
        crate::worker_runtime::WorkerErrorKind::Transient => WorkerError::transient(message),
        crate::worker_runtime::WorkerErrorKind::Terminal => WorkerError::terminal(message),
    }
}
#[async_trait]
impl Worker<Option<(String, String)>> for AttentionService {
    type Prepared = PreparedAttention;
    fn name(&self) -> &str {
        CONSUMER_NAME
    }
    fn subscription(&self) -> Subscription {
        Subscription::All
    }
    /// Superseded turn incidents resolve every loop, as before the sweep; the
    /// sweep itself runs at most once per [`SWEEP_INTERVAL`].
    async fn tick(&self) -> std::result::Result<(), WorkerError> {
        self.resolve_superseded_turn_incidents()
            .await
            .map_err(projection_worker_error)?;
        let due = {
            let mut last = self.last_sweep.lock().expect("sweep clock lock");
            let due = last.is_none_or(|at| at.elapsed() >= SWEEP_INTERVAL);
            if due {
                *last = Some(std::time::Instant::now());
            }
            due
        };
        if due {
            self.sweep_once_at(&now_rfc3339())
                .await
                .map_err(projection_worker_error)?;
        }
        Ok(())
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
        match self.prepare_projection(event).await {
            Err(ServiceError::Db(db::DbError::Check(reason))) => Ok(Outcome::DeadLetter { reason }),
            other => other.map_err(projection_worker_error),
        }
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<Option<(String, String)>, WorkerError> {
        match self.commit_projection(tx, event, prepared).await {
            Err(ServiceError::Db(db::DbError::Check(reason))) => Err(WorkerError::terminal(reason)),
            other => other.map_err(projection_worker_error),
        }
    }
    async fn after_commit(
        &self,
        _: &DomainEvent,
        _: &Self::Prepared,
        stall: &Option<(String, String)>,
    ) -> std::result::Result<(), WorkerError> {
        if let Some((scope_type, scope_id)) = stall {
            self.publish_autonomy_stall(
                scope_type,
                scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
        }
        Ok(())
    }
}

/// Return terminal policy reasons that must be decided before responder
/// configuration or budget lookup.  Keeping these checks in one helper makes
/// the projection path and direct admission path agree, including the
/// system-authored `agent_chat.turn.failed` retry-exhausted event that has no
/// actor identity for the ordinary self-event check.
fn wake_policy_suppression_reason(request: &WakeAdmissionRequest) -> Option<WakeSuppressionReason> {
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
    db::canonical_attention_incident_digest(attention)
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
    db::canonical_attention_incident_digest(&AttentionProjection {
        id: String::new(),
        attention_type: attention_type.to_owned(),
        scope_type: scope_type.to_owned(),
        scope_id: scope_id.to_owned(),
        identity_id: None,
        source_event_id: source_event_id.to_owned(),
        priority: 0,
        status: status.to_owned(),
        summary: String::new(),
        details_json: details_json.to_owned(),
        dedupe_key: String::new(),
        occurred_at: String::new(),
        updated_at: String::new(),
        version,
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: None,
        updated_by_user_id: None,
        recommended_action: recommended_action.to_owned(),
        source_sequence,
    })
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

/// Close a Chat turn incident through Attention's resolver so snoozes are cleared.
pub(crate) async fn resolve_turn_incident<D: db::AttentionRepo>(
    db: &D,
    chat_id: &str,
    turn_id: &str,
    event_id: &str,
) -> Result<()> {
    db::AttentionRepo::resolve_attention_by_dedupe(
        db,
        &format!("attention:retry_exhausted:agent_chat:{chat_id}:agent_chat_turn_job:{turn_id}"),
        event_id,
        &now_rfc3339(),
    )
    .await?;
    Ok(())
}

pub(crate) fn blocker_category(category: &str) -> bool {
    matches!(
        category,
        "execution_failed" | "environment_not_ready" | "review_risk" | "human_input_required"
    )
}
pub(crate) fn wake_budget_category(attention: &AttentionProjection) -> &'static str {
    if attention.scope_type != "project" {
        "delivery"
    } else if blocker_category(&attention.attention_type) {
        "blocker"
    } else if attention.attention_type == "decision_recorded" {
        "decision"
    } else {
        "delivery"
    }
}
/// The 4/4/2 split of a Project budget of at least [`MIN_SPLIT_WAKE_BUDGET`],
/// with every bucket holding at least one wake.
fn category_budget(total: i64, category: &str) -> i64 {
    let blocker = ((total / 10) * 4 + ((total % 10) * 4 + 9) / 10).max(1);
    let delivery = ((total / 10) * 4 + ((total % 10) * 4) / 10).max(1);
    match category {
        "blocker" => blocker,
        "decision" => (total - blocker - delivery).max(1),
        _ => delivery,
    }
}
/// An owner's escalation answer, which wakes the Agent without budget.
fn is_owner_answer(attention: &AttentionProjection) -> bool {
    answered_escalation_id(attention).is_some()
}
fn answered_escalation_id(attention: &AttentionProjection) -> Option<String> {
    if attention.attention_type != DECISION_RECORDED_CATEGORY {
        return None;
    }
    let details = serde_json::from_str::<Value>(&attention.details_json).ok()?;
    (details.get("source_event_type").and_then(Value::as_str)
        == Some("project.escalation.answered"))
    .then(|| {
        details
            .get("entity_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
    .flatten()
}
struct SweepCandidate {
    id: String,
    latest: Option<LatestWakeDecision>,
}
struct LatestWakeDecision {
    digest: Option<String>,
    disposition: String,
    reason: String,
    identity_id: Option<String>,
}
fn expected_attention(a: &AttentionProjection) -> db::ExpectedAttentionSnapshot {
    db::ExpectedAttentionSnapshot {
        id: a.id.clone(),
        version: a.version,
        digest: Some(wake_attention_incident_digest(a)),
        status: a.status.clone(),
        canonical_scope_type: a.scope_type.clone(),
        canonical_scope_id: a.scope_id.clone(),
        source_event_id: a.source_event_id.clone(),
        source_sequence: a.source_sequence,
        dedupe_key: a.dedupe_key.clone(),
    }
}
fn projection_snapshot(a: &CreateAttentionProjection) -> AttentionProjection {
    AttentionProjection {
        id: a.id.clone(),
        attention_type: a.attention_type.clone(),
        scope_type: a.scope_type.clone(),
        scope_id: a.scope_id.clone(),
        identity_id: a.identity_id.clone(),
        source_event_id: a.source_event_id.clone(),
        priority: a.priority,
        status: a.status.clone(),
        summary: a.summary.clone(),
        details_json: a.details_json.clone(),
        dedupe_key: a.dedupe_key.clone(),
        occurred_at: a.occurred_at.clone(),
        updated_at: a.updated_at.clone(),
        version: 1,
        acknowledged_at: a.acknowledged_at.clone(),
        snoozed_until: a.snoozed_until.clone(),
        resolved_at: a.resolved_at.clone(),
        updated_by_user_id: a.updated_by_user_id.clone(),
        recommended_action: a.recommended_action.clone(),
        source_sequence: a.source_sequence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn health_service() -> AttentionService {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        AttentionService::new(Arc::new(SqliteDb::new(pool)))
    }

    async fn append_projection_event(
        service: &AttentionService,
        id: &str,
        kind: &str,
    ) -> DomainEvent {
        service
            .db
            .append_event(CreateDomainEvent {
                id: id.into(),
                event_type: kind.into(),
                entity_type: "task".into(),
                entity_id: id.into(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "project".into(),
                scope_id: "projection-test".into(),
                correlation_id: id.into(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(id.into()),
                payload_json: "{}".into(),
                created_at: now_rfc3339(),
            })
            .await
            .unwrap()
    }
    /// m2: the Task condition's `material_blocker` against the real incident
    /// digest inputs. Each blocker shape is written to a Task, its
    /// `task.interruption_changed` event is projected by the Attention
    /// service, and the incident it materializes (Task context, failure
    /// class, action offers and all) must keep its canonical digest when the
    /// interruption and intervention flag are replaced by the condition's
    /// material blocker. Stage 4 hashes through this digest, not the struct.
    #[tokio::test]
    async fn material_blocker_keeps_the_real_incident_digest() {
        let service = health_service().await;
        let db = &service.db;
        let now = now_rfc3339();
        db::ProjectRepo::create(
            &**db,
            db::CreateProject {
                id: "material".into(),
                owner_id: None,
                name: "Material".into(),
                primary_repo_id: None,
                updated_at: now.clone(),
                settings: "{}".into(),
                workflow_definition: "{}".into(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
        service.project_once(1).await.unwrap(); // one-time worker initialization
        let text = |value: Value| Some(value.to_string());
        let mut shapes = vec![
            (
                text(json!({"type":"manual_stop","blocking_reason":"held"})),
                None,
                None,
            ),
            (
                text(
                    json!({"type":"review_needs_owner","blocking_reason":"finding","blocked_execution_id":"e"}),
                ),
                text(json!({"kind":"review_needs_owner","reason":"owner","execution_id":"e"})),
                None,
            ),
            (
                text(json!({"type":"merge_conflict","message":"advisory"})),
                None,
                None,
            ),
            (
                text(json!({"type":"retry_exhausted","message":"spent"})),
                text(json!({"kind":"retry_exhausted","reason":"spent"})),
                None,
            ),
            (
                None,
                None,
                text(json!({"kind":"executor_failed","reason":"failure","execution_id":"e"})),
            ),
            (
                text(json!({"type":"unknown","message":"retained"})),
                None,
                None,
            ),
            (
                None,
                text(json!({"kind":"manual_stop","reason":"manual"})),
                None,
            ),
            (
                text(json!({"type":"workspace_error","message":"m".repeat(10000)})),
                None,
                None,
            ),
            // Malformed and non-object values in each column.
            (Some("not json".into()), None, None),
            (Some("[1,2]".into()), None, None),
            (None, Some("not json".into()), None),
            (None, Some("42".into()), None),
            (None, None, Some("{".into())),
            (None, None, Some("[]".into())),
            (
                Some("not json".into()),
                text(json!({"kind":"workspace_error","reason":"real","execution_id":"e"})),
                None,
            ),
        ];
        for kind in db::LEGACY_BLOCKING_ANNOTATION_KINDS {
            let annotation = json!({"type":kind,"message":"detail","blocking_reason":"reason","blocked_by":"owner","blocked_execution_id":"execution"});
            let interruption = json!({"kind":kind,"reason":"reason","execution_id":"execution","details":{"key":"detail"}});
            shapes.push((text(annotation.clone()), None, None));
            shapes.push((text(annotation.clone()), text(interruption.clone()), None));
            shapes.push((
                text(annotation),
                text(interruption.clone()),
                text(interruption),
            ));
        }
        let mut incidents = 0;
        for (index, (annotation, blocked, failed)) in shapes.into_iter().enumerate() {
            let id = format!("material-{index}");
            let mut task = db::TaskRepo::create(
                &**db,
                db::CreateTask {
                    id: id.clone(),
                    project_id: "material".into(),
                    parent_task_id: None,
                    assignee_type: None,
                    assignee_id: None,
                    title: id.clone(),
                    description: None,
                    task_type: "task".into(),
                    status: "in_progress".into(),
                    is_automation: false,
                    priority: 0,
                    task_state_config: None,
                    merge_config: None,
                    subtask_order: None,
                    plan: None,
                    updated_at: now.clone(),
                    created_at: now.clone(),
                },
            )
            .await
            .unwrap();
            // The writer's own event, appended with the legacy columns.
            let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
            sqlx::query(
                "UPDATE task SET error_annotation=?,blocked_json=?,failed_json=? WHERE id=?",
            )
            .bind(&annotation)
            .bind(&blocked)
            .bind(&failed)
            .bind(&id)
            .execute(&mut *transaction)
            .await
            .unwrap();
            db.sync_condition_in_tx(&mut transaction, &id)
                .await
                .unwrap();
            task.error_annotation = annotation;
            task.blocked_json = blocked;
            task.failed_json = failed;
            let event = CreateDomainEvent::task_interruption_changed(&task);
            db::DomainEventRepo::append_event_in_tx(&**db, &mut transaction, &event)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            service.project_once(100).await.unwrap();

            let material = db::material_blocker(&db.task_condition(&id).await.unwrap());
            let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
            assert_eq!(
                json!(material.requires_intervention),
                payload["requires_intervention"],
                "{id}"
            );
            let incident: Option<String> = sqlx::query_scalar(
                "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=?",
            )
            .bind(&id)
            .fetch_optional(db.pool())
            .await
            .unwrap();
            let Some(incident) = incident else {
                assert!(
                    !material.requires_intervention,
                    "{id}: a blocker that needs intervention has an incident"
                );
                continue;
            };
            incidents += 1;
            let mut attention = db::AttentionRepo::get_attention(&**db, &incident)
                .await
                .unwrap()
                .unwrap();
            let real = db::canonical_attention_incident_digest(&attention);
            let mut details: Value = serde_json::from_str(&attention.details_json).unwrap();
            // The real details carry far more than the interruption.
            assert!(details["task"].is_object(), "{id}: {details}");
            assert!(details.get("failure_class").is_some());
            let mut stated = details["interruption"].clone();
            db::strip_attention_delivery_metadata(&mut stated);
            assert_eq!(json!(material.interruption), stated, "{id}");
            details["interruption"] = json!(material.interruption);
            details["recovery"]["requires_intervention"] = json!(material.requires_intervention);
            attention.details_json = details.to_string();
            assert_eq!(
                real,
                db::canonical_attention_incident_digest(&attention),
                "{id}: the material blocker re-arms a consumed incident"
            );
            // A different blocker does change it: the digest is not vacuous.
            details["interruption"] = json!({"source":"blocked","kind":"another","reason":"cause"});
            attention.details_json = details.to_string();
            assert_ne!(real, db::canonical_attention_incident_digest(&attention));
        }
        assert!(
            incidents >= 20,
            "only {incidents} shapes raised an incident"
        );
    }

    #[tokio::test]
    async fn runtime_upgrade_preserves_cursor_ignores_legacy_lease_and_projects_once() {
        let service = health_service().await;
        let old = append_projection_event(&service, "old-incident", "validation.failed").await;
        let next = append_projection_event(&service, "new-incident", "validation.failed").await;
        sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, ?, ?)")
            .bind(CONSUMER_NAME).bind(old.sequence).bind(now_rfc3339()).execute(service.db.pool()).await.unwrap();
        sqlx::raw_sql(include_str!("../../db/tests/fixtures/event_delivery.sql"))
            .execute(service.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) VALUES (?, ?, 'legacy', '2999-01-01T00:00:00Z', 1, ?)")
            .bind(CONSUMER_NAME).bind(next.sequence).bind(now_rfc3339()).execute(service.db.pool()).await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../db/migrations/V202610020700__retire_event_delivery_leases.sql"
        ))
        .execute(service.db.pool())
        .await
        .unwrap();
        sqlx::raw_sql("CREATE TRIGGER fail_after_projection BEFORE INSERT ON domain_event WHEN NEW.event_type LIKE 'agent.wake.%' BEGIN SELECT RAISE(ABORT, 'test failure after projection write'); END;")
            .execute(service.db.pool()).await.unwrap();
        assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
        assert_eq!(
            service
                .consumer_cursor()
                .await
                .unwrap()
                .unwrap()
                .last_sequence,
            old.sequence
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection")
            .fetch_one(service.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        sqlx::query("DROP TRIGGER fail_after_projection")
            .execute(service.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE worker_health SET retry_not_before = '2000-01-01T00:00:00Z'")
            .execute(service.db.pool())
            .await
            .unwrap();
        assert_eq!(service.project_once(100).await.unwrap().processed_events, 1);
        assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
        let keys: Vec<String> =
            sqlx::query_scalar("SELECT source_event_id FROM attention_projection")
                .fetch_all(service.db.pool())
                .await
                .unwrap();
        assert_eq!(keys, vec![next.id]);
        assert_eq!(
            service
                .consumer_cursor()
                .await
                .unwrap()
                .unwrap()
                .last_sequence,
            next.sequence
        );
        let health = service.consumer_health().await.unwrap().unwrap();
        assert!(health.last_success_at.is_some());
        assert!(health.last_error_code.is_none());
    }
    #[tokio::test]
    async fn runtime_unprojected_event_and_idle_cycle_write_nothing() {
        let service = health_service().await;
        service.project_once(1).await.unwrap(); // one-time worker initialization
        sqlx::raw_sql("CREATE TABLE writes (name TEXT); CREATE TRIGGER observed_health AFTER UPDATE ON worker_health BEGIN INSERT INTO writes VALUES ('health'); END;
            CREATE TRIGGER observed_cursor AFTER UPDATE ON event_consumer_cursor BEGIN INSERT INTO writes VALUES ('cursor'); END;")
            .execute(service.db.pool()).await.unwrap();
        append_projection_event(&service, "irrelevant", "ignored").await;
        for _ in 0..4 {
            assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM writes")
            .fetch_one(service.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    #[tokio::test]
    async fn runtime_commit_check_quarantines_once_and_next_incident_proceeds() {
        let service = health_service().await;
        let poison = append_projection_event(&service, "poison", "validation.failed").await;
        let good = append_projection_event(&service, "good", "validation.failed").await;
        let Outcome::Done(prepared) = service.prepare_projection(&poison).await.unwrap() else {
            panic!("projected");
        };
        let mut tx = db::begin_immediate(service.db.pool()).await.unwrap();
        service
            .commit_projection(&mut tx, &poison, &prepared)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        // Force exact semantic dedupe conflict at commit, after the first write.
        sqlx::query(
            "UPDATE domain_event SET payload_json = '{}' WHERE event_type LIKE 'agent.wake.%'",
        )
        .execute(service.db.pool())
        .await
        .unwrap();
        // Remove the fixture's linked setup disposition before corrupting its source.
        sqlx::query("DELETE FROM agent_wake_disposition_current")
            .execute(service.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM agent_wake_disposition")
            .execute(service.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM attention_projection")
            .execute(service.db.pool())
            .await
            .unwrap();
        let run = service.project_once(100).await.unwrap();
        assert_eq!(run.processed_events, 2); // terminal acknowledgement and good projection
        assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter WHERE worker_name = ?")
                .bind(CONSUMER_NAME)
                .fetch_one(service.db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
        let sources: Vec<String> =
            sqlx::query_scalar("SELECT source_event_id FROM attention_projection")
                .fetch_all(service.db.pool())
                .await
                .unwrap();
        assert_eq!(sources, vec![good.id]);
    }

    #[tokio::test]
    async fn consumer_health_ignores_resolved_quarantines() {
        let service = health_service().await;
        service.project_once(1).await.unwrap();
        let mut tx = db::begin_immediate(service.db.pool()).await.unwrap();
        db::WorkerHealth::new(service.db.clone(), CONSUMER_NAME)
            .dead_letter_in_tx(
                &mut tx,
                db::WorkItem {
                    source_key: "12",
                    item_type: "validation.failed",
                },
                db::FailureState {
                    attempts: 8,
                    first_failed_at: &now_rfc3339(),
                },
                "projection rejection",
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let health = service.consumer_health().await.unwrap().unwrap();
        assert_eq!(
            health.last_error_message.as_deref(),
            Some("projection rejection")
        );
        let id: String = sqlx::query_scalar("SELECT id FROM worker_dead_letter")
            .fetch_one(service.db.pool())
            .await
            .unwrap();
        crate::dead_letter_service::DeadLetterService::new(service.db.clone())
            .dismiss(
                crate::dead_letter_service::DeadLetterActor {
                    user_id: "admin",
                    is_admin: true,
                },
                &id,
                None,
            )
            .await
            .unwrap();
        let health = service.consumer_health().await.unwrap().unwrap();
        assert!(health.last_error_code.is_none());
        assert!(health.last_error_message.is_none());
    }

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

    fn user_event(event_type: &str, entity_type: &str, payload_json: &str) -> DomainEvent {
        DomainEvent {
            entity_type: entity_type.to_owned(),
            entity_id: "entity-1".to_owned(),
            actor_type: "user".to_owned(),
            actor_id: Some("user-1".to_owned()),
            ..event(event_type, payload_json)
        }
    }

    #[test]
    fn user_decisions_wake_the_agent_as_decision_recorded() {
        assert_eq!(
            classify_event(&user_event(
                "milestone.definition.transitioned",
                "milestone",
                r#"{"revision_id":"rev-1","lifecycle":"approved"}"#
            )),
            Some("decision_recorded")
        );
        assert_eq!(
            classify_event(&user_event(
                "milestone.definition.transitioned",
                "milestone",
                r#"{"revision_id":"rev-1","lifecycle":"rejected"}"#
            )),
            Some("decision_recorded")
        );
        // Proposing is the Agent asking, not the user deciding.
        assert_eq!(
            classify_event(&user_event(
                "milestone.definition.transitioned",
                "milestone",
                r#"{"revision_id":"rev-1","lifecycle":"proposed"}"#
            )),
            None
        );
        assert_eq!(
            classify_event(&user_event(
                "project.decision.approved",
                "project_decision",
                "{}"
            )),
            Some("decision_recorded")
        );
        assert_eq!(
            classify_event(&user_event(
                "project.decision.candidate_rejected",
                "project_decision_candidate",
                "{}"
            )),
            Some("decision_recorded")
        );
        assert_eq!(
            classify_event(&user_event(
                "project.document.approved",
                "project_document_approval",
                "{}"
            )),
            Some("decision_recorded")
        );
        assert_eq!(
            category_metadata("decision_recorded").2,
            "continue_from_decision"
        );
    }

    #[test]
    fn an_agents_own_approval_is_not_a_user_decision() {
        let agent_event = DomainEvent {
            actor_type: "agent".to_owned(),
            actor_id: Some("identity-1".to_owned()),
            ..user_event(
                "milestone.definition.transitioned",
                "milestone",
                r#"{"revision_id":"rev-1","lifecycle":"approved"}"#,
            )
        };
        assert_eq!(classify_event(&agent_event), None);
        assert_eq!(
            classify_event(&event("project.decision.approved", "{}")),
            None
        );
    }

    #[test]
    fn category_split_keeps_every_bucket_and_matches_ten() {
        let split = |total| {
            (
                category_budget(total, "blocker"),
                category_budget(total, "delivery"),
                category_budget(total, "decision"),
            )
        };
        assert_eq!(split(10), (4, 4, 2));
        assert_eq!(split(5), (2, 2, 1));
        assert_eq!(split(7), (3, 2, 2));
        for total in MIN_SPLIT_WAKE_BUDGET..=40 {
            let (blocker, delivery, decision) = split(total);
            assert!(blocker >= 1 && delivery >= 1 && decision >= 1, "{total}");
            assert_eq!(blocker + delivery + decision, total, "{total}");
        }
    }

    #[test]
    fn incident_keys_name_their_category() {
        assert_eq!(
            incident_key_category("attention:decision_recorded:project:p1:milestone:m1"),
            Some("decision_recorded")
        );
        assert_eq!(incident_key_category("attention::project:p1"), None);
        assert_eq!(incident_key_category("wake:decision_recorded"), None);
    }

    #[test]
    fn task_transition_rules_are_deterministic() {
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"blocked"}"#)),
            None
        );
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"review"}"#)),
            None
        );
        assert_eq!(
            classify_event(&event("task.transitioned", r#"{"to_state":"done"}"#)),
            None
        );
        assert!(
            resolution_categories(&event("task.transitioned", r#"{"to_state":"done"}"#)).is_empty()
        );
        assert_eq!(
            classify_event(&event("project_release.candidate_requested", "{}")),
            Some("human_input_required")
        );
        assert_eq!(
            classify_event(&event("execution.failed", r#"{"error":"boom"}"#)),
            None
        );
        assert_eq!(
            classify_event(&event(
                "execution.cancelled",
                r#"{"stop_reason":"user_cancelled"}"#
            )),
            None
        );
        assert_eq!(
            classify_event(&event(
                "task.interruption_changed",
                r#"{"requires_intervention":true}"#
            )),
            Some("execution_failed")
        );
        assert_eq!(
            classify_event(&event(
                "task.interruption_changed",
                r#"{"requires_intervention":false}"#
            )),
            None
        );
        assert_eq!(
            resolution_categories(&event(
                "task.interruption_changed",
                r#"{"requires_intervention":false}"#
            )),
            vec!["execution_failed"]
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
