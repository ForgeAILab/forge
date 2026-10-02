//! Durable delivery of admitted agent wakes into Agent Chat turns.
//!
//! `AttentionService` admits wakes deterministically (budget, cooldown,
//! dedupe, self-event suppression) and records `agent.wake.admitted` in the
//! durable ledger.  This consumer is the delivery half: it turns each
//! admitted wake into one system-authored Agent Chat message plus a queued
//! turn job for the woken identity's chat, so the agent actually runs a turn
//! and can act on the incident.  It also delivers the user-driven
//! continuation straight to the
//! Project Agent chat — a user approval is its own deterministic admission
//! and consumes no wake budget.
//!
//! Every admission is idempotent: the turn job dedupe key is derived from the
//! source event, so crash-replay after projection never queues a second turn.

use async_trait::async_trait;
use std::sync::Arc;

use db::{
    now_rfc3339, AdmitAgentChatTurn, AgentChat, AgentChatMessageAuthorType, AgentChatMessageStatus,
    AgentChatRepo, AgentWakeDisposition, AgentWakeDispositionKind, AgentWakeDispositionRepo,
    AttentionRepo, CreateAgentChatMessage, CreateAgentChatTurnJob, CreateAgentWakeDisposition,
    DomainEvent, DomainEventRepo, PersistAgentWake, RetryAgentWakeDisposition, SqliteDb,
};
use serde_json::{json, Value};
use sqlx::{Acquire, Row, Sqlite, Transaction};
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;

use crate::{
    agent_turn_admission::{
        content_digest, AgentTurnAdmissionService, AgentTurnPrepareInput, AgentTurnReadiness,
        AgentTurnTrigger, PreparedAgentTurnAdmission, ResolvedAgentResponder,
    },
    attention_service::{wake_attention_incident_digest, MAX_WAKE_REACTION_DEPTH},
    worker_runtime::{Outcome, Subscription, Worker, WorkerError, WorkerRuntime},
    Result, ServiceError,
};

const CONSUMER_NAME: &str = "agent-wake-turns";
const MAX_TURN_ATTEMPTS: i64 = 3;
const MAX_DETAIL_CHARS: usize = 2_000;

/// `outcome` of the system message that carries a wake prompt. The chat
/// timeline keys on it to collapse the work order to its summary line; the
/// wording of the prompt itself is not a contract.
pub const ATTENTION_WAKE_MESSAGE_OUTCOME: &str = "attention_wake";
pub(crate) const DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA: &str =
    "forge.delivery-followup-postcondition/v1";
pub(crate) const DELIVERY_FOLLOWUP_READINESS_EVENT: &str = "milestone.readiness.evaluated";
pub(crate) const DELIVERY_FOLLOWUP_VALIDATION_EVENT: &str = "project.milestone.check.recorded";
/// A wake message is a work order, not a report; keep the named work bounded
/// so a milestone with a long check matrix cannot crowd out the instruction.
const MAX_DELIVERY_FOLLOWUP_MILESTONES: usize = 3;
const MAX_DELIVERY_FOLLOWUP_CHECKS: usize = 8;

/// What a delivery follow-up wake actually has to settle, resolved from server
/// state at admission time.
///
/// The wake fires when a Task reaches `done`, and the work it implies is not
/// "describe the completion" — it is "settle the acceptance checks this
/// delivery was supposed to satisfy". Resolving that here means the work order
/// can name the exact milestone, version, definition revision, and check ids
/// the Agent must pass to `project.validation`, instead of asking it to
/// rediscover them and hope it decides to act.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DeliveryFollowupState {
    pub(crate) milestones: Vec<DeliveryFollowupMilestone>,
    /// Open milestones the work order could not carry. Reported rather than
    /// dropped, so a bounded message never reads as full coverage.
    pub(crate) skipped_milestones: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryFollowupMilestone {
    pub(crate) milestone_id: String,
    pub(crate) milestone_key: String,
    pub(crate) version: i64,
    pub(crate) definition_revision_id: String,
    pub(crate) open_task_count: i64,
    /// Required checks the Agent may settle itself, in stable check order.
    pub(crate) agent_check_ids: Vec<String>,
    /// Required checks only an authorized user can attest.
    pub(crate) manual_check_ids: Vec<String>,
}

impl DeliveryFollowupState {
    fn agent_settleable(&self) -> bool {
        self.milestones
            .iter()
            .any(|milestone| !milestone.agent_check_ids.is_empty())
    }

    /// The event the turn must actually commit. Outstanding validation comes
    /// first: readiness computed before the results exist can only re-report
    /// the same missing checks, which is exactly the loop this wake used to
    /// produce.
    pub(crate) fn required_event_type(&self) -> Option<&'static str> {
        if self.agent_settleable() {
            Some(DELIVERY_FOLLOWUP_VALIDATION_EVENT)
        } else if self.milestones.is_empty() {
            // Nothing to settle and nothing to evaluate.
            None
        } else {
            Some(DELIVERY_FOLLOWUP_READINESS_EVENT)
        }
    }
}

/// Name the checks the work order can carry. A milestone with more required
/// checks than fit says so rather than presenting a truncated list as complete.
fn named_checks(check_ids: &[String]) -> String {
    if check_ids.len() <= MAX_DELIVERY_FOLLOWUP_CHECKS {
        return check_ids.join(", ");
    }
    format!(
        "{}, and {} more (read `project.current_state` for the rest)",
        check_ids[..MAX_DELIVERY_FOLLOWUP_CHECKS].join(", "),
        check_ids.len() - MAX_DELIVERY_FOLLOWUP_CHECKS,
    )
}

/// Render the ordered work order for a delivery follow-up wake.
pub(crate) fn delivery_followup_directive(state: &DeliveryFollowupState) -> String {
    let mut lines = String::from("\nDELIVERY FOLLOW-UP WORK ORDER\n");
    let mut has_work = false;
    for milestone in &state.milestones {
        if milestone.agent_check_ids.is_empty() && milestone.manual_check_ids.is_empty() {
            continue;
        }
        has_work = true;
        let progress = if milestone.open_task_count == 0 {
            "every Task bound to it is done".to_owned()
        } else {
            format!("{} Task(s) still open", milestone.open_task_count)
        };
        lines.push_str(&format!(
            "\nMilestone {} ({}): {}.\n  milestone_id={} milestone_version={} definition_revision_id={}\n",
            milestone.milestone_key,
            milestone.milestone_id,
            progress,
            milestone.milestone_id,
            milestone.version,
            milestone.definition_revision_id,
        ));
        if !milestone.agent_check_ids.is_empty() {
            lines.push_str(&format!(
                "  Settle yourself, in this turn: {}. Run the delivered software in your checkout with forge_task_command against each check's expected result, then record what you observed with `project.validation` (action `record`) using the exact milestone_id, milestone_version, definition_revision_id, and check_id above, citing the observation_id values those commands returned in observed_command_ids. One call per check. A Task's or reviewer's report settles nothing.\n",
                named_checks(&milestone.agent_check_ids),
            ));
        }
        if !milestone.manual_check_ids.is_empty() {
            lines.push_str(&format!(
                "  User-attested only: {}. Ask the user for the observation; you may never record one yourself.\n",
                named_checks(&milestone.manual_check_ids),
            ));
        }
    }
    if !has_work {
        lines.push_str(
            "\nEvery required acceptance check already has a current authoritative result. Evaluate readiness for the applicable milestone with `project.readiness` and report the committed canonical result, even when it is blocked, failed, or stale.\n",
        );
        return lines;
    }
    lines.push_str(
        "\nRecord every check you can settle before evaluating readiness: readiness computed first can only re-report the same missing results. Narration does not complete this turn, and Task completion or a passing review is not validation.\n",
    );
    if state.skipped_milestones > 0 {
        lines.push_str(&format!(
            "{} further open milestone(s) are not named here; read `project.current_state` for them.\n",
            state.skipped_milestones,
        ));
    }
    lines
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeTurnRun {
    pub claimed_events: usize,
    pub processed_events: usize,
    pub last_sequence: i64,
}

#[derive(Debug)]
pub enum WakeDeliveryPlan {
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
}

impl WakeTurnConsumer {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
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

    pub async fn run_once(&self, limit: i64) -> Result<WakeTurnRun> {
        let processed_events = WorkerRuntime::new(Arc::clone(&self.db), Arc::new(self.clone()))
            .run_once(limit.clamp(1, 100) as usize)
            .await?;
        let last_sequence = self
            .db
            .get_consumer_cursor(&self.consumer_name)
            .await?
            .map_or(0, |cursor| cursor.last_sequence);
        Ok(WakeTurnRun {
            claimed_events: processed_events,
            processed_events,
            last_sequence,
        })
    }

    async fn prepare_event(&self, event: &DomainEvent) -> Result<WakeDeliveryPlan> {
        self.evaluation_result(event, None, self.plan_event(event, 1, None).await)
    }
    fn evaluation_result(
        &self,
        event: &DomainEvent,
        parent: Option<&AgentWakeDisposition>,
        result: Result<WakeDeliveryPlan>,
    ) -> Result<WakeDeliveryPlan> {
        match result {
            Ok(plan) => Ok(plan),
            Err(error) if transient_evaluation_error(&error) && parent.is_some() => Err(error),
            Err(error) if transient_evaluation_error(&error) => Ok(WakeDeliveryPlan::Deferred(
                deferred_disposition(DeferredDispositionSpec {
                    consumer_name: &self.consumer_name,
                    event,
                    attempt: 1,
                    max_attempts: MAX_TURN_ATTEMPTS,
                    incident_key: None,
                    incident_digest: None,
                    reason: "wake_evaluation_unavailable",
                    retry_at_value: retry_at(&event.created_at, 1),
                    attention_id: None,
                }),
            )),
            Err(_) => Ok(WakeDeliveryPlan::Suppressed(self.evaluation_suppression(
                event,
                parent,
                "wake_evaluation_invalid",
            ))),
        }
    }
    fn evaluation_suppression(
        &self,
        event: &DomainEvent,
        parent: Option<&AgentWakeDisposition>,
        reason: &str,
    ) -> CreateAgentWakeDisposition {
        self.disposition(DispositionSpec {
            event,
            attempt: parent.map_or(1, |p| p.attempt_number + 1),
            max_attempts: parent.map_or(MAX_TURN_ATTEMPTS, |p| p.max_attempts),
            kind: AgentWakeDispositionKind::DeterministicallySuppressed,
            reason,
            incident_key: parent.and_then(|p| p.incident_key.clone()),
            incident_digest: parent.and_then(|p| p.incident_digest.clone()),
            attention_id: None,
            responder: None,
            parent_disposition_id: parent.map(|p| p.id.clone()),
        })
    }
    async fn record_retry_error(
        &self,
        row: &AgentWakeDisposition,
        error: &WorkerError,
    ) -> Result<()> {
        let health =
            crate::worker_runtime::WorkerHealth::new(Arc::clone(&self.db), &self.consumer_name);
        let key = format!("wake-retry:{}", row.id);
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let current: Option<String> = sqlx::query_scalar("SELECT disposition_id FROM agent_wake_disposition_current WHERE consumer_name = ? AND source_event_id = ?")
            .bind(&self.consumer_name).bind(&row.source_event_id).fetch_optional(&mut *tx).await?;
        if current.as_deref() != Some(&row.id) {
            return Ok(());
        }
        let decision = health
            .isolated_item_failed_in_tx(
                &mut tx,
                crate::worker_runtime::WorkItem {
                    source_key: &key,
                    item_type: "agent_wake_retry",
                },
                db::RetryPolicy::default(),
                error.kind.as_str(),
                error.message(),
            )
            .await?;
        if matches!(decision, db::PoisonDecision::DeadLettered) {
            let now = now_rfc3339();
            let terminal = CreateAgentWakeDisposition {
                id: deterministic_uuid(&format!("wake-retry-failed:{}", row.id)),
                consumer_name: row.consumer_name.clone(),
                source_event_id: row.source_event_id.clone(),
                source_event_sequence: row.source_event_sequence,
                attempt_number: row.attempt_number + 1,
                max_attempts: row.max_attempts,
                disposition: AgentWakeDispositionKind::DeterministicallySuppressed,
                reason: "wake_retry_failed".into(),
                turn_job_id: None,
                attention_id: None,
                retry_at: None,
                incident_key: row.incident_key.clone(),
                incident_digest: row.incident_digest.clone(),
                binding_id: None,
                binding_version: None,
                profile_id: None,
                profile_version: None,
                provenance_json: Some(
                    json!({"failure": error.message(), "error_kind": error.kind.as_str()})
                        .to_string(),
                ),
                parent_disposition_id: Some(row.id.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            };
            self.db
                .retry_agent_wake_in_tx(
                    &mut tx,
                    RetryAgentWakeDisposition {
                        disposition: terminal,
                        expected_parent_id: row.id.clone(),
                        now,
                        admission: None,
                        expected_attention: None,
                    },
                )
                .await?;
        }
        tx.commit().await?;
        Ok(())
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
        let plan = self.evaluation_result(
            &event,
            Some(current),
            self.plan_event(&event, attempt, Some(current)).await,
        )?;
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
                Ok(_) => {
                    return Ok(delivered);
                }
                Err(error) if error.is_transient() => return Err(error.into()),
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
                Err(_) => {
                    let suppressed = self.evaluation_suppression(
                        &event,
                        Some(current),
                        "turn_admission_rejected",
                    );
                    self.db
                        .retry_agent_wake(RetryAgentWakeDisposition {
                            disposition: suppressed,
                            expected_parent_id: current.id.clone(),
                            now,
                            admission: None,
                            expected_attention: None,
                        })
                        .await?;
                    return Ok(false);
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
                )));
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
                )));
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
                )));
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
                }));
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
                }));
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
                }));
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
                )));
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
                }));
            }
            AgentTurnReadiness::Ready => {}
        }
        // A delivery follow-up's work order is resolved from server state, not
        // from the incident text: the Agent is told which milestone, which
        // checks, and which exact ids to record against.
        let delivery = if attention.attention_type == "delivery_followup" {
            match self
                .delivery_followup_state(
                    &attention.scope_id,
                    delivery_followup_task_id(&attention).as_deref(),
                )
                .await
            {
                Ok(state) => Some(state),
                Err(_) => {
                    return Ok(self.deferred_plan(DeferredPlanSpec {
                        event,
                        attempt,
                        max_attempts,
                        parent_id,
                        reason: "delivery_state_unavailable",
                        incident_key: Some(fields.incident_key),
                        incident_digest: Some(incident_digest),
                        responder: None,
                        attention_id: Some(attention.id),
                    }));
                }
            }
        } else {
            None
        };
        let content = wake_content(&attention, delivery.as_ref());
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
        let admission = self.build_turn_admission(
            event,
            chat,
            content,
            prepared.clone(),
            Some(&attention),
            delivery.as_ref(),
        )?;
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

    /// Resolve the delivery follow-up work order from server state.
    ///
    /// The milestone set is the one the completed Task is governed by; a Task
    /// bound to no milestone falls back to the Project's open milestones, so a
    /// delivery still reaches the outcome it was meant to satisfy.
    async fn delivery_followup_state(
        &self,
        project_id: &str,
        task_id: Option<&str>,
    ) -> Result<DeliveryFollowupState> {
        let governed_milestone_ids: Vec<String> = match task_id {
            Some(task_id) => {
                sqlx::query_scalar(
                    "SELECT DISTINCT g.milestone_id FROM project_task_governance g
                 WHERE g.task_id = ? AND g.project_id = ? AND g.milestone_id IS NOT NULL",
                )
                .bind(task_id)
                .bind(project_id)
                .fetch_all(self.db.pool())
                .await?
            }
            None => Vec::new(),
        };

        let candidates = sqlx::query(
            "SELECT m.id, m.milestone_key, m.version,
                    m.current_definition_revision_id AS definition_revision_id,
                    r.task_selection_json
             FROM project_milestone m
             JOIN project_milestone_revision r
               ON r.id = m.current_definition_revision_id AND r.milestone_id = m.id
             WHERE m.project_id = ?
               AND m.lifecycle IN ('planned', 'active', 'ready_for_release')
             ORDER BY m.milestone_sequence ASC, m.id ASC",
        )
        .bind(project_id)
        .fetch_all(self.db.pool())
        .await?;

        let mut milestones = Vec::new();
        let mut skipped_milestones = 0usize;
        for row in candidates {
            let milestone_id: String = row.try_get("id")?;
            if !governed_milestone_ids.is_empty() && !governed_milestone_ids.contains(&milestone_id)
            {
                continue;
            }
            if milestones.len() >= MAX_DELIVERY_FOLLOWUP_MILESTONES {
                skipped_milestones += 1;
                continue;
            }
            let definition_revision_id: String = row.try_get("definition_revision_id")?;
            let task_selection_json: String = row.try_get("task_selection_json")?;
            // Mirror readiness: a milestone gates on every governed Task plus
            // every Task its definition selected, so the wake never claims the
            // work is finished while readiness still sees an open Task.
            let open_task_count = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM task t
                 WHERE t.project_id = ?
                   AND t.deleted_at IS NULL AND t.archived_at IS NULL
                   AND t.status NOT IN ('done', 'cancelled')
                   AND (
                        t.id IN (SELECT g.task_id FROM project_task_governance g
                                 WHERE g.milestone_id = ? AND g.project_id = ?)
                     OR t.id IN (SELECT value FROM json_each(?))
                   )",
            )
            .bind(project_id)
            .bind(&milestone_id)
            .bind(project_id)
            .bind(&task_selection_json)
            .fetch_optional(self.db.pool())
            .await?
            .unwrap_or_default();

            let check_rows = sqlx::query(
                "SELECT c.id, c.source_kind, COALESCE(r.outcome, 'missing') AS outcome
                 FROM project_milestone_check c
                 LEFT JOIN project_milestone_check_result r
                   ON r.id = c.current_result_id
                  AND r.definition_revision_id = c.definition_revision_id
                 WHERE c.project_id = ? AND c.milestone_id = ?
                   AND c.definition_revision_id = ? AND c.required = 1
                   AND COALESCE(r.outcome, 'missing') NOT IN ('passed', 'waived')
                 ORDER BY c.check_key ASC
                 LIMIT 64",
            )
            .bind(project_id)
            .bind(&milestone_id)
            .bind(&definition_revision_id)
            .fetch_all(self.db.pool())
            .await?;
            let mut agent_check_ids = Vec::new();
            let mut manual_check_ids = Vec::new();
            for check in check_rows {
                let check_id: String = check.try_get("id")?;
                let source_kind: String = check.try_get("source_kind")?;
                let outcome: String = check.try_get("outcome")?;
                // A check that already failed or went stale is still
                // outstanding, but it is not unobserved: say which it is so the
                // Agent re-runs it rather than reporting it as never attempted.
                let named = if outcome == "missing" {
                    check_id
                } else {
                    format!("{check_id} (currently {outcome})")
                };
                if source_kind == "manual" {
                    manual_check_ids.push(named);
                } else {
                    agent_check_ids.push(named);
                }
            }
            milestones.push(DeliveryFollowupMilestone {
                milestone_id,
                milestone_key: row.try_get("milestone_key")?,
                version: row.try_get("version")?,
                definition_revision_id,
                open_task_count,
                agent_check_ids,
                manual_check_ids,
            });
        }
        Ok(DeliveryFollowupState {
            milestones,
            skipped_milestones,
        })
    }

    fn build_turn_admission(
        &self,
        event: &DomainEvent,
        chat: AgentChat,
        content: String,
        prepared: PreparedAgentTurnAdmission,
        attention: Option<&db::AttentionProjection>,
        delivery: Option<&DeliveryFollowupState>,
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
        // The postcondition names the one server record this turn must
        // produce. Outstanding validation asks for the validation record;
        // only once the checks are settled does readiness become the thing
        // the turn owes. When no approved baseline exists neither is
        // possible, and the turn must be free to say so.
        let required_event_type = attention
            .filter(|attention| attention.attention_type == "delivery_followup")
            .and_then(|attention| Some((attention, delivery?)))
            .and_then(|(attention, delivery)| Some((attention, delivery.required_event_type()?)));
        let source_metadata_json = match required_event_type {
            Some((attention, required_event_type)) => json!({
                "wake_event_id": event.id,
                "wake_event_type": event.event_type,
                "turn_postcondition": {
                    "schema_version": DELIVERY_FOLLOWUP_POSTCONDITION_SCHEMA,
                    "attention_id": attention.id,
                    "required_event_type": required_event_type,
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
            outcome: Some(ATTENTION_WAKE_MESSAGE_OUTCOME.to_owned()),
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

#[async_trait]
impl Worker for WakeTurnConsumer {
    type Prepared = WakeDeliveryPlan;
    fn name(&self) -> &str {
        &self.consumer_name
    }
    fn subscription(&self) -> Subscription {
        Subscription::Prefix(vec!["agent.wake.".to_owned()])
    }
    async fn tick(&self) -> std::result::Result<(), WorkerError> {
        let rows = self
            .db
            .list_reconsiderable_agent_wake_dispositions(&self.consumer_name, &now_rfc3339(), 100)
            .await
            .map_err(|e| crate::worker_runtime::consumer_error(e.into()))?;
        let health =
            crate::worker_runtime::WorkerHealth::new(Arc::clone(&self.db), &self.consumer_name);
        let mut last_error = None;
        for row in rows {
            let key = format!("wake-retry:{}", row.id);
            match self.process_retry(&row).await {
                Ok(_) => {
                    if let Err(error) = health.isolated_item_succeeded(&key).await {
                        last_error = Some(crate::worker_runtime::consumer_error(error.into()));
                    }
                }
                Err(error) => {
                    let error = crate::worker_runtime::consumer_error(error);
                    tracing::warn!(disposition_id = %row.id, %error, "wake retry row failed");
                    if let Err(report) = self.record_retry_error(&row, &error).await {
                        tracing::warn!(disposition_id = %row.id, %report, "failed to record wake retry error");
                        last_error = Some(crate::worker_runtime::consumer_error(report));
                    } else {
                        last_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = last_error {
            Err(error)
        } else {
            Ok(())
        }
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
        self.prepare_event(event)
            .await
            .map(Outcome::Done)
            .map_err(crate::worker_runtime::consumer_error)
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        plan: &Self::Prepared,
    ) -> std::result::Result<(), WorkerError> {
        let disposition = plan.disposition().clone();
        if let WakeDeliveryPlan::Admitted {
            admission,
            expected_attention,
            ..
        } = plan
        {
            // Admission can write before its final authority check rejects. Undo
            // that attempt before persisting the fallback in the outer transaction.
            let mut savepoint = tx
                .begin()
                .await
                .map_err(|e| crate::worker_runtime::consumer_error(db::DbError::Sqlx(e).into()))?;
            let result = self
                .db
                .persist_agent_wake_in_tx(
                    &mut savepoint,
                    event,
                    PersistAgentWake {
                        disposition: disposition.clone(),
                        admission: Some(*admission.clone()),
                        expected_attention: expected_attention.clone(),
                    },
                )
                .await;
            match result {
                Ok(_) => {
                    savepoint.commit().await.map_err(|e| {
                        crate::worker_runtime::consumer_error(db::DbError::Sqlx(e).into())
                    })?;
                    return Ok(());
                }
                Err(error) => {
                    savepoint.rollback().await.map_err(|e| {
                        crate::worker_runtime::consumer_error(db::DbError::Sqlx(e).into())
                    })?;
                    let fallback = if transient_db_error(&error) {
                        deferred_disposition(DeferredDispositionSpec {
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
                        })
                    } else {
                        let mut suppressed =
                            self.evaluation_suppression(event, None, "turn_admission_rejected");
                        suppressed.incident_key = disposition.incident_key.clone();
                        suppressed.incident_digest = disposition.incident_digest.clone();
                        suppressed
                    };
                    self.db
                        .persist_agent_wake_in_tx(
                            tx,
                            event,
                            PersistAgentWake {
                                disposition: fallback,
                                admission: None,
                                expected_attention: None,
                            },
                        )
                        .await
                        .map_err(|e| crate::worker_runtime::consumer_error(e.into()))?;
                    return Ok(());
                }
            }
        }
        self.db
            .persist_agent_wake_in_tx(
                tx,
                event,
                PersistAgentWake {
                    disposition,
                    admission: None,
                    expected_attention: None,
                },
            )
            .await
            .map_err(|e| crate::worker_runtime::consumer_error(e.into()))?;
        Ok(())
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

/// The completed Task behind a delivery follow-up, when the incident has one.
fn delivery_followup_task_id(attention: &db::AttentionProjection) -> Option<String> {
    let details = serde_json::from_str::<Value>(&attention.details_json).ok()?;
    if details.get("entity_type").and_then(Value::as_str) != Some("task") {
        return None;
    }
    details
        .get("entity_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

/// Recover the bounded recovery snapshot attached to an execution failure.
///
/// New Attention rows carry an object so that a scheduled retry can be
/// distinguished from a failure that needs intervention. Older rows used the
/// recovery action array directly; keep those rows readable, but treat their
/// actions as a context snapshot rather than authority.
fn execution_recovery_snapshot(attention: &db::AttentionProjection) -> (Vec<String>, bool) {
    let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
    let Some(recovery) = details.get("recovery") else {
        return (Vec::new(), false);
    };

    match recovery {
        Value::Object(recovery) => {
            let actions = recovery
                .get("actions")
                .and_then(Value::as_array)
                .map(|actions| {
                    actions
                        .iter()
                        .filter_map(Value::as_str)
                        .filter(|action| !action.trim().is_empty())
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let automatic_retry = recovery
                .get("automatic_retry")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            (actions, automatic_retry)
        }
        Value::Array(actions) => (
            actions
                .iter()
                .filter_map(Value::as_str)
                .filter(|action| !action.trim().is_empty())
                .map(str::to_owned)
                .collect(),
            false,
        ),
        _ => (Vec::new(), false),
    }
}

/// Give execution-failure wakes a constrained work order. The action list is
/// only the Attention's snapshot; `task.recover` remains server-authorized
/// against the current Task state.
fn execution_failed_directive(attention: &db::AttentionProjection) -> String {
    let (actions, automatic_retry) = execution_recovery_snapshot(attention);
    let action_snapshot = if actions.is_empty() {
        "No recovery action is advertised by this Attention snapshot.".to_owned()
    } else {
        format!(
            "Attention snapshot actions (context only): {}.",
            actions.join(", ")
        )
    };

    if automatic_retry {
        format!(
            "EXECUTION FAILURE RECOVERY\nAutomatic retry is scheduled or in progress. This wake is observation-only: refresh the current Task state and inspect/diagnose the execution, but do not manually retry, reexecute, resume, or invoke `task.recover` while that retry is pending. {action_snapshot} After it settles, use only a recovery action currently advertised by the current Task; the server remains authoritative and may reject stale or unsupported actions."
        )
    } else if actions.is_empty() {
        format!(
            "EXECUTION FAILURE RECOVERY\nRefresh the current Task state and inspect/diagnose the failed execution. {action_snapshot} Do not invoke `task.recover` unless the current Task advertises a recovery action; this wake grants no recovery action."
        )
    } else {
        format!(
            "EXECUTION FAILURE RECOVERY\nRefresh the current Task state before acting. {action_snapshot} Use only a recovery action currently advertised by the current Task; this list is a context snapshot, and the server remains authoritative and may reject stale or unsupported actions."
        )
    }
}

fn wake_content(
    attention: &db::AttentionProjection,
    delivery: Option<&DeliveryFollowupState>,
) -> String {
    let details = if attention.details_json.trim().is_empty()
        || attention.details_json.trim() == "{}"
        || attention.details_json.chars().count() > MAX_DETAIL_CHARS
    {
        String::new()
    } else {
        format!("\nDetails: {}\n", attention.details_json)
    };
    let delivery_requirement =
        match delivery.filter(|_| attention.attention_type == "delivery_followup") {
            Some(state) => delivery_followup_directive(state),
            None => String::new(),
        };
    let decision_requirement = decision_directive(attention);
    let final_instruction = if attention.attention_type == "execution_failed" {
        execution_failed_directive(attention)
    } else {
        "Assess the current state with your tools and take the action this incident requires. If a decision genuinely belongs to the user, ask for it; otherwise proceed.".to_owned()
    };
    format!(
        "### Attention wake: {}\n\nCategory: {} — recommended action: {}.\nIncident: {}{}\n{}",
        attention.summary,
        attention.attention_type,
        attention.recommended_action,
        attention.dedupe_key,
        format_args!("{details}{delivery_requirement}{decision_requirement}"),
        final_instruction,
    )
}

/// The work order behind a `decision_recorded` wake: say what the user
/// decided and tell the Agent to continue from it rather than ask again.
fn decision_directive(attention: &db::AttentionProjection) -> String {
    if attention.attention_type != crate::attention_service::DECISION_RECORDED_CATEGORY {
        return String::new();
    }
    let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
    let decision = details.get("decision");
    let field = |key: &str| {
        decision
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
    };
    let outcome = field("outcome").unwrap_or_else(|| "decided on".to_owned());
    let target = match field("target_type").as_deref() {
        Some("milestone") => "the milestone definition revision of milestone",
        Some("project_decision") | Some("project_decision_candidate") => "the proposed Decision",
        Some("project_document_approval") => "the Document revision approval",
        _ => "the pending item",
    };
    let target_id = field("target_id").unwrap_or_default();
    let revision = field("revision_id")
        .map(|revision_id| format!(" (revision {revision_id})"))
        .unwrap_or_default();
    format!(
        "\nUSER DECISION\nThe user {outcome} {target} {target_id}{revision}. That decision is recorded and authoritative: do not ask the user to confirm it again and do not re-propose it. Read `project.current_state`, then continue from the decision in this turn — plan, queue, or dispatch the work it unblocks, or state precisely what still blocks it.\n"
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

fn transient_evaluation_error(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::DependencyGate
            | ServiceError::Db(
                db::DbError::Sqlx(_) | db::DbError::NotFound | db::DbError::VersionConflict
            )
    )
}
fn transient_db_error(error: &db::DbError) -> bool {
    error.is_transient() || matches!(error, db::DbError::NotFound | db::DbError::DependencyGate)
}

/// Stable UUID derived from a dedupe key so replays reuse identical row ids.
fn deterministic_uuid(seed: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes()).to_string()
}

pub fn wake_turn_consumer_name() -> &'static str {
    CONSUMER_NAME
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attention(attention_type: &str, details_json: &str) -> db::AttentionProjection {
        db::AttentionProjection {
            id: "attention-1".to_owned(),
            attention_type: attention_type.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            identity_id: Some("identity-1".to_owned()),
            source_event_id: "event-1".to_owned(),
            priority: 65,
            status: "open".to_owned(),
            summary: "The user recorded a decision; continue from it".to_owned(),
            details_json: details_json.to_owned(),
            dedupe_key: format!("attention:{attention_type}:project:project-1:milestone:m-1"),
            occurred_at: "2026-09-02T00:00:00Z".to_owned(),
            updated_at: "2026-09-02T00:00:00Z".to_owned(),
            version: 1,
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "continue_from_decision".to_owned(),
            source_sequence: Some(1),
        }
    }

    #[test]
    fn decision_wakes_say_what_the_user_decided() {
        let content = wake_content(
            &attention(
                "decision_recorded",
                r#"{"decision":{"outcome":"approved","target_type":"milestone","target_id":"m-1","revision_id":"rev-2","decided_by":"user"}}"#,
            ),
            None,
        );
        assert!(content
            .starts_with("### Attention wake: The user recorded a decision; continue from it"));
        assert!(content.contains("USER DECISION"));
        assert!(content.contains(
            "The user approved the milestone definition revision of milestone m-1 (revision rev-2)."
        ));
        assert!(content.contains("do not ask the user to confirm it again"));
    }

    #[test]
    fn other_wakes_carry_no_decision_directive() {
        let content = wake_content(&attention("run_stalled", "{}"), None);
        assert!(!content.contains("USER DECISION"));
        assert!(content.starts_with("### Attention wake: "));
        assert!(content.contains("otherwise proceed."));
    }

    #[test]
    fn a_decision_without_details_still_directs_the_agent() {
        let directive = decision_directive(&attention("decision_recorded", "{}"));
        assert!(directive.contains("The user decided on the pending item"));
        assert!(directive.contains("continue from the decision"));
    }

    #[test]
    fn execution_failure_wake_uses_only_currently_advertised_actions() {
        let content = wake_content(
            &attention(
                "execution_failed",
                r#"{"recovery":{"requires_intervention":true,"actions":["reexecute","cancel_task"],"automatic_retry":false}}"#,
            ),
            None,
        );

        assert!(content.contains("EXECUTION FAILURE RECOVERY"));
        assert!(content.contains("Refresh the current Task state before acting"));
        assert!(content.contains("reexecute, cancel_task"));
        assert!(content.contains("Use only a recovery action currently advertised"));
        assert!(content.contains("server remains authoritative"));
        assert!(!content.contains("otherwise proceed."));
    }

    #[test]
    fn execution_failure_with_automatic_retry_is_observation_only() {
        let content = wake_content(
            &attention(
                "execution_failed",
                r#"{"recovery":{"requires_intervention":false,"actions":["reexecute"],"automatic_retry":true}}"#,
            ),
            None,
        );

        assert!(content.contains("Automatic retry is scheduled or in progress"));
        assert!(content.contains("observation-only"));
        assert!(content.contains("do not manually retry, reexecute, resume"));
        assert!(content.contains("or invoke `task.recover`"));
        assert!(content.contains("reexecute"));
        assert!(content.contains("use only a recovery action currently advertised"));
        assert!(!content.contains("otherwise proceed."));
    }

    #[test]
    fn execution_failure_without_actions_does_not_grant_recovery() {
        let content = wake_content(
            &attention(
                "execution_failed",
                r#"{"recovery":{"requires_intervention":false,"actions":[],"automatic_retry":false}}"#,
            ),
            None,
        );

        assert!(content.contains("inspect/diagnose the failed execution"));
        assert!(content.contains("No recovery action is advertised"));
        assert!(content.contains(
            "Do not invoke `task.recover` unless the current Task advertises a recovery action"
        ));
        assert!(!content.contains("otherwise proceed."));
    }

    #[test]
    fn execution_failure_legacy_recovery_array_remains_context_only() {
        let content = wake_content(
            &attention("execution_failed", r#"{"recovery":["reexecute"]}"#),
            None,
        );

        assert!(content.contains("Attention snapshot actions (context only): reexecute"));
        assert!(content.contains("Use only a recovery action currently advertised"));
    }
    #[tokio::test]
    async fn audit2_evaluation_failures_commit_typed_outcomes_without_quarantine() {
        struct EvaluationFault(WakeTurnConsumer, bool);
        #[async_trait]
        impl Worker for EvaluationFault {
            type Prepared = WakeDeliveryPlan;
            fn name(&self) -> &str {
                self.0.name()
            }
            fn subscription(&self) -> Subscription {
                self.0.subscription()
            }
            async fn handle(
                &self,
                event: &DomainEvent,
            ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
                let error = if self.1 {
                    ServiceError::Db(db::DbError::NotFound)
                } else {
                    ServiceError::invalid_operation("invalid evaluation")
                };
                self.0
                    .evaluation_result(event, None, Err(error))
                    .map(Outcome::Done)
                    .map_err(crate::worker_runtime::consumer_error)
            }
            async fn commit(
                &self,
                tx: &mut Transaction<'_, Sqlite>,
                event: &DomainEvent,
                p: &Self::Prepared,
            ) -> std::result::Result<(), WorkerError> {
                self.0.commit(tx, event, p).await
            }
        }
        for unavailable in [false, true] {
            let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
            db::run_migrations(&pool).await.unwrap();
            let db = Arc::new(SqliteDb::new(pool));
            let now = now_rfc3339();
            let event = db
                .append_event(db::CreateDomainEvent {
                    id: db::new_uuid_v4(),
                    event_type: "agent.wake.admitted".into(),
                    entity_type: "project".into(),
                    entity_id: "p".into(),
                    actor_type: "system".into(),
                    actor_id: None,
                    scope_type: "project".into(),
                    scope_id: "p".into(),
                    correlation_id: "test".into(),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: "{}".into(),
                    created_at: now,
                })
                .await
                .unwrap();
            let worker = Arc::new(EvaluationFault(
                WakeTurnConsumer::new(Arc::clone(&db)),
                unavailable,
            ));
            let runtime = WorkerRuntime::new(Arc::clone(&db), worker);
            runtime.run_once(100).await.unwrap();
            let row = db
                .get_current_agent_wake_disposition(CONSUMER_NAME, &event.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                row.reason,
                if unavailable {
                    "wake_evaluation_unavailable"
                } else {
                    "wake_evaluation_invalid"
                }
            );
            assert_eq!(
                row.disposition,
                if unavailable {
                    AgentWakeDispositionKind::Deferred
                } else {
                    AgentWakeDispositionKind::DeterministicallySuppressed
                }
            );
            assert_eq!(
                db.get_consumer_cursor(CONSUMER_NAME)
                    .await
                    .unwrap()
                    .unwrap()
                    .last_sequence,
                event.sequence
            );
            let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter")
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(dead, 0);
        }
    }
}
