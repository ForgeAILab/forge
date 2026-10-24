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
    new_uuid_v4, now_rfc3339, AgentContextScopeRepo, AgentRepo, AttentionListQuery,
    AttentionProjection, AttentionRepo, CreateAttentionProjection, CreateDomainEvent, DomainEvent,
    DomainEventRepo, EventConsumerCursor, Page, PageRequest, ProjectMemberRepo, ProjectRepo,
    SqliteDb, UpdateAttentionLifecycle,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Acquire, Sqlite, Transaction};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{
    wake_blocker::BlockerTurnState,
    wake_turns::{PreparedWakeTurn, WakeTurns},
    worker_runtime::{Outcome, Subscription, Worker, WorkerError, WorkerRuntime},
    Result, ServiceError,
};

/// `recommended_action` of the Attention item that carries a lead's question
/// to the owner. Resolving it takes the answer; it never wakes an Agent.
pub const ANSWER_LEAD_INPUT: &str = "answer_lead_input";
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
    turn: Option<Arc<PreparedWakeTurn>>,
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
    wake_turns: Arc<dyn WakeTurns>,
    action_connections: Option<Arc<crate::daemon_transport::DaemonConnectionRegistry>>,
    event_bus: Option<Arc<events::EventBus>>,
    last_sweep: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl AttentionService {
    pub fn new(db: Arc<SqliteDb>, wake_turns: Arc<dyn WakeTurns>) -> Self {
        Self {
            db,
            wake_turns,
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
        let open_incidents: i64 = self
            .db
            .open_project_autonomy_incident_count(scope_id)
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
                context.turn = self
                    .wake_turns
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
    db::task_condition::material_blocker(&task.condition).requires_intervention
}

async fn orphan_attempt_is_current(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    execution_id: &str,
) -> Result<bool> {
    let task_id = db
        .current_orphan_attempt_task_id_in_tx(transaction, execution_id)
        .await?;
    let Some(task_id) = task_id else {
        return Ok(false);
    };
    let Some(task) = db::TaskRepo::get_by_id_in_tx(db, transaction, &task_id, false).await? else {
        return Ok(false);
    };
    // A committed disposition owns its own incident, including an intentional
    // manual stop. Raw attempt events must not override those recovery choices.
    if task.condition.read().diagnostic_present
        || task_requires_intervention(&task)
        || task.condition.read().retry.is_some_and(|r| {
            DateTime::parse_from_rfc3339(&r.not_before).is_ok_and(|at| at > Utc::now())
        })
    {
        return Ok(false);
    }
    let workflow_json = db
        .orphan_attempt_workflow_definition_in_tx(transaction, &task.project_id)
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
        let material = event.task_material_blocker()?;
        return material.requires_intervention.then(|| {
            if material
                .interruption
                .as_ref()
                .and_then(|i| i.get("kind"))
                .and_then(Value::as_str)
                == Some("review_needs_owner")
                && material
                    .interruption
                    .as_ref()
                    .and_then(|i| i.get("source"))
                    .and_then(Value::as_str)
                    != Some("integration")
            {
                "review_needs_owner"
            } else {
                "execution_failed"
            }
        });
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
    if event_type == "task.interruption_changed" {
        // A changed cause replaces the previous kind of Task incident. A
        // cleared condition closes either kind; neither park can linger.
        let current = classify_event(event);
        if current != Some("execution_failed") {
            categories.push("execution_failed");
        }
        if current != Some("review_needs_owner") {
            categories.push("review_needs_owner");
        }
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
        "review_needs_owner" => (85, "Review needs the owner", "inspect_review"),
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
        "flaky_check" => (60, "CI check is flaky", "inspect_ci"),
        "base_check_failing" => (60, "Base CI needs attention", "inspect_ci"),
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
        "review_needs_owner" => AttentionCategory::ReviewNeedsOwner,
        "execution_failed" => AttentionCategory::ExecutionFailed,
        "delivery_followup" => AttentionCategory::DeliveryFollowup,
        "decision_recorded" => AttentionCategory::DecisionRecorded,
        "runtime_offline" | "environment_not_ready" => AttentionCategory::RuntimeOffline,
        "budget_threshold" => AttentionCategory::BudgetThreshold,
        "commitment_overdue" => AttentionCategory::CommitmentOverdue,
        "conflict_hotspot" => AttentionCategory::ConflictHotspot,
        "flaky_check" => AttentionCategory::FlakyCheck,
        "base_check_failing" => AttentionCategory::BaseCheckFailing,
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
    turn: Option<Arc<PreparedWakeTurn>>,
    request: WakeAdmissionRequest,
    decision: PreparedWakeDecision,
    failed_turn: Option<db::AgentChatTurnJob>,
    review_is_current: bool,
    requires_intervention: bool,
    orphan_execution_id: Option<String>,
}
enum PreparedWakeDecision {
    OwnerOnly,
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
pub async fn resolve_turn_incident<D: db::AttentionRepo>(
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
        "execution_failed"
            | "review_needs_owner"
            | "environment_not_ready"
            | "review_risk"
            | "human_input_required"
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

mod wake_sweep;

mod wake_admission;

mod wake_decisions;

mod actions;

mod mission_control;

mod projection;

mod wake_responder;

#[cfg(test)]
mod tests;
