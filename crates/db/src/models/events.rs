//! Domain events, wake dispositions, and attention projections.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainEvent {
    pub sequence: i64,
    pub id: String,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: String,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub scope_type: String,
    pub scope_id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub dedupe_key: Option<String>,
    pub payload_json: String,
    pub created_at: String,
}

/// The only terminal/processable outcomes a wake consumer may persist for a
/// claimed source event.  Deferred and setup-required outcomes are durable
/// retry points, not cursor-completion shortcuts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentWakeDispositionKind {
    TurnAdmitted,
    DeterministicallySuppressed,
    Deferred,
    SetupRequired,
}

impl fmt::Display for AgentWakeDispositionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TurnAdmitted => "turn_admitted",
            Self::DeterministicallySuppressed => "deterministically_suppressed",
            Self::Deferred => "deferred",
            Self::SetupRequired => "setup_required",
        })
    }
}

impl FromStr for AgentWakeDispositionKind {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "turn_admitted" => Ok(Self::TurnAdmitted),
            "deterministically_suppressed" => Ok(Self::DeterministicallySuppressed),
            "deferred" => Ok(Self::Deferred),
            "setup_required" => Ok(Self::SetupRequired),
            other => Err(format!("unknown wake disposition: {other}")),
        }
    }
}

/// One immutable wake-disposition attempt.  Retries append a new row and
/// advance `AgentWakeDispositionCurrent`; no prior identity/profile or
/// incident provenance is rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeDisposition {
    pub id: String,
    pub consumer_name: String,
    pub source_event_id: String,
    pub source_event_sequence: i64,
    pub attempt_number: i64,
    pub max_attempts: i64,
    pub disposition: AgentWakeDispositionKind,
    pub reason: String,
    pub turn_job_id: Option<String>,
    pub attention_id: Option<String>,
    pub retry_at: Option<String>,
    pub incident_key: Option<String>,
    pub incident_digest: Option<String>,
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
    pub provenance_json: Option<String>,
    pub parent_disposition_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentWakeDisposition {
    pub id: String,
    pub consumer_name: String,
    pub source_event_id: String,
    pub source_event_sequence: i64,
    pub attempt_number: i64,
    pub max_attempts: i64,
    pub disposition: AgentWakeDispositionKind,
    pub reason: String,
    pub turn_job_id: Option<String>,
    pub attention_id: Option<String>,
    pub retry_at: Option<String>,
    pub incident_key: Option<String>,
    pub incident_digest: Option<String>,
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
    pub provenance_json: Option<String>,
    pub parent_disposition_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Transaction-owned first wake disposition, current pointer and optional admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistAgentWake {
    pub disposition: CreateAgentWakeDisposition,
    /// Required for `turn_admitted`.  The DB inserts/replays this message and
    /// turn inside the same transaction as the disposition and source-event
    /// checkpoint, closing the crash seam between admission and delivery.
    pub admission: Option<AdmitAgentChatTurn>,
    /// Snapshot of the Attention that was used to prepare `admission`.  The
    /// repository re-reads this row under the same IMMEDIATE transaction
    /// before inserting the turn, so a material projection update cannot be
    /// admitted from stale resolver input.
    pub expected_attention: Option<ExpectedAttentionSnapshot>,
}

/// Append a later deferred/setup-required disposition attempt after the
/// authoritative source event has already been checkpointed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryAgentWakeDisposition {
    pub disposition: CreateAgentWakeDisposition,
    pub expected_parent_id: String,
    pub now: String,
    /// Required when the retry advances from deferred/setup-required to
    /// `turn_admitted`; it is committed with the new immutable attempt.
    pub admission: Option<AdmitAgentChatTurn>,
    /// Optional Attention snapshot used when a retry advances to a turn.
    pub expected_attention: Option<ExpectedAttentionSnapshot>,
}

/// Immutable Attention evidence captured while preparing one wake admission.
/// The scope/source fields are intentionally explicit: checking only a
/// projection version is unsafe because legacy upserts did not always bump
/// that version for material source changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedAttentionSnapshot {
    pub id: String,
    pub version: i64,
    pub digest: Option<String>,
    pub status: String,
    pub canonical_scope_type: String,
    pub canonical_scope_id: String,
    pub source_event_id: String,
    pub source_sequence: Option<i64>,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeDispositionCurrent {
    pub consumer_name: String,
    pub source_event_id: String,
    pub disposition_id: String,
    pub attempt_number: i64,
    pub updated_at: String,
    pub version: i64,
}

/// Rebuildable, durable Attention materialization.  The source event and
/// dedupe key keep this row derived; the lifecycle columns are the only
/// operator-owned state and use optimistic versions for concurrent clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionProjection {
    pub id: String,
    pub attention_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub identity_id: Option<String>,
    pub source_event_id: String,
    pub priority: i64,
    pub status: String,
    pub summary: String,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: String,
    pub updated_at: String,
    pub version: i64,
    pub acknowledged_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub resolved_at: Option<String>,
    pub updated_by_user_id: Option<String>,
    pub recommended_action: String,
    pub source_sequence: Option<i64>,
}

/// Remove, at any depth, what identifies one delivery of a blocker rather
/// than the blocker: event identity, Task version, bookkeeping times and the
/// execution that happened to report it. The incident digest and the Task
/// condition's material blocker share this one definition.
pub fn strip_attention_delivery_metadata(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for key in [
                "source_event_id",
                "source_event_type",
                "source_sequence",
                "task_version",
                "created_at",
                "updated_at",
                "detected_at",
                "last_checked_at",
                "next_check_at",
                "execution_id",
            ] {
                object.remove(key);
            }
            for child in object.values_mut() {
                strip_attention_delivery_metadata(child);
            }
        }
        serde_json::Value::Array(array) => {
            for child in array {
                strip_attention_delivery_metadata(child);
            }
        }
        _ => {}
    }
}

/// Canonical, redaction-safe digest used to bind a wake decision to the exact
/// Attention materialization it observed.  Keep this in `db` so the atomic
/// admission transaction can validate a snapshot without depending on the
/// services crate.
pub fn canonical_attention_incident_digest(attention: &AttentionProjection) -> String {
    let bounded_details = attention
        .details_json
        .chars()
        .take(8_192)
        .collect::<String>();
    let canonical_details = serde_json::from_str::<serde_json::Value>(&bounded_details)
        .map(|mut value| {
            strip_attention_delivery_metadata(&mut value);
            // A renamed Task or a changed action offer is not a new blocker.
            if let Some(task) = value.get_mut("task").and_then(|v| v.as_object_mut()) {
                task.remove("task_title");
            }
            if let Some(recovery) = value.get_mut("recovery").and_then(|v| v.as_object_mut()) {
                recovery.remove("actions");
            }
            value.to_string()
        })
        .unwrap_or(bounded_details);
    let details_digest = wake_incident_digest(&canonical_details);
    let canonical = format!(
        "type={};scope_type={};scope_id={};status={};details_digest={};recommended_action={}",
        bounded_wake_ref(&attention.attention_type),
        bounded_wake_ref(&attention.scope_type),
        bounded_wake_ref(&attention.scope_id),
        bounded_wake_ref(&attention.status),
        details_digest,
        bounded_wake_ref(&attention.recommended_action),
    );
    wake_incident_digest(&canonical)
}

fn bounded_wake_ref(value: &str) -> String {
    value.chars().take(256).collect()
}

fn wake_incident_digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hasher.update([0]);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAttentionProjection {
    pub id: String,
    pub attention_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub identity_id: Option<String>,
    pub source_event_id: String,
    pub priority: i64,
    pub status: String,
    pub summary: String,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: String,
    pub updated_at: String,
    pub acknowledged_at: Option<String>,
    pub snoozed_until: Option<String>,
    pub resolved_at: Option<String>,
    pub updated_by_user_id: Option<String>,
    pub recommended_action: String,
    pub source_sequence: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAttentionLifecycle {
    pub id: String,
    pub expected_version: i64,
    pub status: String,
    pub acknowledged_at: Option<Option<String>>,
    pub snoozed_until: Option<Option<String>>,
    pub resolved_at: Option<Option<String>>,
    pub updated_by_user_id: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttentionListQuery {
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub scope_type: Option<String>,
    pub status: Option<String>,
    pub include_snoozed: bool,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventConsumerCursor {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventConsumerCutover {
    pub consumer_name: String,
    pub cutover_sequence: i64,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainEventConsumerLag {
    pub consumer_name: String,
    pub last_sequence: i64,
    pub lag: i64,
    pub last_advanced_at: Option<String>,
    pub oldest_unprocessed_at: Option<String>,
    pub initialized_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteStorageStatus {
    pub incremental_vacuum: bool,
    pub free_pages: i64,
}

impl DomainEventConsumerLag {
    pub fn stalled(&self, now: chrono::DateTime<chrono::Utc>, seconds: i64) -> bool {
        let old = |date: &str| {
            chrono::DateTime::parse_from_rfc3339(date)
                .ok()
                .is_some_and(|date| {
                    (now - date.with_timezone(&chrono::Utc)).num_seconds() > seconds
                })
        };
        self.lag > 0
            && self.oldest_unprocessed_at.as_deref().is_some_and(old)
            && self.last_advanced_at.as_deref().is_none_or(old)
            && self.initialized_at.as_deref().is_none_or(old)
    }
}

impl DomainEvent {
    /// Decode immutable stored interruption events. The old payload is accepted
    /// only here at the event archive boundary; new writers emit the typed form.
    pub fn task_material_blocker(&self) -> Option<crate::MaterialBlocker> {
        if self.event_type != "task.interruption_changed" {
            return None;
        }
        let payload: serde_json::Value = serde_json::from_str(&self.payload_json).ok()?;
        if let Some(material) = payload.get("material_blocker") {
            return serde_json::from_value(material.clone()).ok();
        }
        let mut interruption = payload
            .get("interruption")
            .filter(|v| !v.is_null())
            .cloned();
        if let Some(value) = &mut interruption {
            crate::strip_attention_delivery_metadata(value);
        }
        Some(crate::MaterialBlocker {
            requires_intervention: payload.get("requires_intervention")?.as_bool()?,
            interruption,
        })
    }
}
