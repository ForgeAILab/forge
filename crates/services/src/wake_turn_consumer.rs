//! Atomic wake turn preparation owned by Attention. Wake events are audit records.
use crate::{
    agent_turn_admission::{
        content_digest, AgentTurnAdmissionService, AgentTurnPrepareInput, AgentTurnReadiness,
        AgentTurnTrigger, PreparedAgentTurnAdmission,
    },
    worker_runtime::{Outcome, Subscription, Worker, WorkerError, WorkerRuntime},
    Result,
};
use async_trait::async_trait;
use db::{
    now_rfc3339, AdmitAgentChatTurn, AgentChat, AgentChatMessageAuthorType, AgentChatMessageStatus,
    AgentChatRepo, CreateAgentChatMessage, CreateAgentChatTurnJob, DomainEvent, DomainEventRepo,
    SqliteDb,
};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};
use uuid::Uuid;
const CONSUMER_NAME: &str = "agent-wake-turns";
const MAX_TURN_ATTEMPTS: i64 = 3;
const MAX_DETAIL_CHARS: usize = 2_000;
const DEFAULT_WAKE_DIRECTIVE: &str = "Assess the current state with your tools and take the action this incident requires. If a decision genuinely belongs to the user, ask for it; otherwise proceed.";
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

#[derive(Debug, Clone)]
pub(crate) struct PreparedWakeTurn {
    pub chat: AgentChat,
    pub content: String,
    pub prepared: PreparedAgentTurnAdmission,
    pub attention: db::AttentionProjection,
    pub delivery: Option<DeliveryFollowupState>,
}
#[derive(Clone)]
pub struct WakeTurnConsumer {
    db: Arc<SqliteDb>,
}
impl WakeTurnConsumer {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
    pub fn start(self: Arc<Self>, shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self)).start(shutdown)
    }
    /// Audit checkpoint only. Events never create or retry turn jobs.
    pub async fn run_once(&self, limit: i64) -> Result<WakeTurnRun> {
        let processed_events = WorkerRuntime::new(Arc::clone(&self.db), Arc::new(self.clone()))
            .run_once(limit.clamp(1, 100) as usize)
            .await?;
        let last_sequence = self
            .db
            .get_consumer_cursor(CONSUMER_NAME)
            .await?
            .map_or(0, |c| c.last_sequence);
        Ok(WakeTurnRun {
            claimed_events: processed_events,
            processed_events,
            last_sequence,
        })
    }
    pub(crate) async fn prepare_attention(
        &self,
        attention: &db::AttentionProjection,
        dedupe_key: &str,
        causation_id: Option<&str>,
        depth: i64,
        batch: &[db::AttentionProjection],
    ) -> Result<Option<PreparedWakeTurn>> {
        let chat = match attention.scope_type.as_str() {
            "project" => AgentChatRepo::get_project_chat(&*self.db, &attention.scope_id).await?,
            "account" => AgentChatRepo::get_main_chat(&*self.db, &attention.scope_id).await?,
            _ => None,
        };
        let Some(chat) = chat else {
            return Ok(None);
        };
        let service = AgentTurnAdmissionService::new(Arc::clone(&self.db));
        if service.resolve(&chat).await?.readiness != AgentTurnReadiness::Ready {
            return Ok(None);
        }
        let delivery = if attention.attention_type == "delivery_followup" {
            Some(
                self.delivery_followup_state(
                    &attention.scope_id,
                    delivery_followup_task_id(attention).as_deref(),
                )
                .await?,
            )
        } else {
            None
        };
        let mut content = wake_content(attention, delivery.as_ref());
        if batch.len() > 1 {
            // One existing directive for the whole batch; other incidents are data only.
            let details = batch.iter().map(|a| json!({"incident": a.dedupe_key, "summary": a.summary, "details": serde_json::from_str::<Value>(&a.details_json).unwrap_or(Value::Null)})).collect::<Vec<_>>();
            let first_line = content.lines().next().unwrap_or_default().to_owned();
            let directive = DEFAULT_WAKE_DIRECTIVE;
            content = format!(
                "{first_line}\n\nDetails: {}\n\n{directive}",
                Value::Array(details)
            );
        }
        let digest = content_digest(&content)?;
        let prepared = service
            .prepare(AgentTurnPrepareInput {
                chat: &chat,
                trigger: AgentTurnTrigger::AutonomousWake,
                dedupe_key,
                content_digest: &digest,
                causation_id,
                causation_depth: depth.saturating_add(1).min(8),
                source_responder: None,
            })
            .await?;
        Ok(Some(PreparedWakeTurn {
            chat,
            content,
            prepared,
            attention: attention.clone(),
            delivery,
        }))
    }
    pub(crate) fn build_prepared_turn(
        &self,
        event: &DomainEvent,
        input: &PreparedWakeTurn,
    ) -> Result<AdmitAgentChatTurn> {
        self.build_turn_admission(
            event,
            input.chat.clone(),
            input.content.clone(),
            input.prepared.clone(),
            Some(&input.attention),
            input.delivery.as_ref(),
        )
    }
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
}
#[async_trait]
impl Worker for WakeTurnConsumer {
    type Prepared = ();
    fn name(&self) -> &str {
        CONSUMER_NAME
    }
    fn subscription(&self) -> Subscription {
        Subscription::Prefix(vec!["agent.wake.".to_owned()])
    }
    async fn handle(&self, _: &DomainEvent) -> std::result::Result<Outcome<()>, WorkerError> {
        Ok(Outcome::Skip)
    }
    async fn commit(
        &self,
        _: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        _: &(),
    ) -> std::result::Result<(), WorkerError> {
        Ok(())
    }
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
                        .filter_map(|value| {
                            value
                                .get("action")
                                .and_then(|action| action.get("verb"))
                                .and_then(Value::as_str)
                                .or_else(|| value.as_str())
                        })
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
                .filter_map(|value| {
                    value
                        .get("action")
                        .and_then(|action| action.get("verb"))
                        .and_then(Value::as_str)
                        .or_else(|| value.as_str())
                })
                .filter(|action| !action.trim().is_empty())
                .map(str::to_owned)
                .collect(),
            false,
        ),
        _ => (Vec::new(), false),
    }
}

/// Give execution-failure wakes a constrained work order. The action list is
/// only the Attention's snapshot; `task.action` remains server-authorized
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
            "EXECUTION FAILURE RECOVERY\nAutomatic retry is scheduled or in progress. This wake is observation-only: refresh the current Task state and inspect/diagnose the execution, but do not manually retry, release, or invoke `task.action` while that retry is pending. {action_snapshot} After it settles, use only a recovery action currently advertised by the current Task; the server remains authoritative and may reject stale or unsupported actions."
        )
    } else if actions.is_empty() {
        format!(
            "EXECUTION FAILURE RECOVERY\nRefresh the current Task state and inspect/diagnose the failed execution. {action_snapshot} Do not invoke `task.action` unless the current Task advertises a recovery action; this wake grants no recovery action."
        )
    } else {
        format!(
            "EXECUTION FAILURE RECOVERY\nRefresh the current Task state before acting. {action_snapshot} Use only a recovery action currently advertised by the current Task; this list is a context snapshot, and the server remains authoritative and may reject stale or unsupported actions."
        )
    }
}

fn conflict_hotspot_directive() -> &'static str {
    "Conflict hot spot (path and Tasks in Details). Propose one Task via `task.propose` that splits this file into modules with clear owners so parallel Tasks edit disjoint files, unless an open Task already does."
}

fn wake_content(
    attention: &db::AttentionProjection,
    delivery: Option<&DeliveryFollowupState>,
) -> String {
    let details = if attention.attention_type == "conflict_hotspot" {
        // Keep the bounded event payload's path/Tasks once, including long paths,
        // without spending tokens on unrelated projection/recovery metadata.
        let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
        format!("\nDetails: {}\n", details["conflict_hotspot"])
    } else if attention.details_json.trim().is_empty()
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
    } else if attention.attention_type == "conflict_hotspot" {
        conflict_hotspot_directive().to_owned()
    } else {
        DEFAULT_WAKE_DIRECTIVE.to_owned()
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
    fn conflict_hotspot_directive_is_short_and_references_details_once() {
        let tasks: Vec<_> = (0..10).map(|_| db::new_uuid_v4()).collect();
        // A path larger than the generic Details cutoff still reaches this
        // wake, and cannot increase the directive's length or duplicate UUIDs.
        let path = format!("src/{}.rs", "長い名前".repeat(1000));
        let details = json!({"conflict_hotspot": {"path": path, "task_ids": tasks}}).to_string();
        let item = attention("conflict_hotspot", &details);
        let directive = conflict_hotspot_directive();
        assert_eq!(directive, "Conflict hot spot (path and Tasks in Details). Propose one Task via `task.propose` that splits this file into modules with clear owners so parallel Tasks edit disjoint files, unless an open Task already does.");
        assert!(directive.split_whitespace().count() <= 40);
        let content = wake_content(&item, None);
        assert!(content.ends_with(directive));
        assert_eq!(
            content.lines().last().unwrap().split_whitespace().count(),
            34
        );
        assert_eq!(content.matches(&path).count(), 1);
        for id in &tasks {
            assert_eq!(content.matches(id).count(), 1);
        }
        assert!(!directive.contains(&path));
        assert!(!directive.contains("Resolve"));
        for category in [
            "execution_failed",
            "delivery_followup",
            "decision_recorded",
            "run_stalled",
            "human_input_required",
            "review_risk",
            "budget_threshold",
            "commitment_overdue",
        ] {
            assert!(
                !wake_content(&attention(category, &details), None)
                    .contains("Conflict hot spot (path and Tasks in Details)"),
                "{category}"
            );
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
        assert!(content.contains("do not manually retry, release"));
        assert!(content.contains("or invoke `task.action`"));
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
            "Do not invoke `task.action` unless the current Task advertises a recovery action"
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
}
