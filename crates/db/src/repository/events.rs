//! Domain event, wake disposition, and attention repositories.

use super::*;

#[async_trait]
pub trait DomainEventRepo: Send + Sync {
    async fn append_event(&self, input: CreateDomainEvent) -> Result<DomainEvent>;
    async fn append_event_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateDomainEvent,
    ) -> Result<DomainEvent>;
    async fn get_event(&self, id: &str) -> Result<Option<DomainEvent>>;
    async fn get_event_by_dedupe(&self, dedupe_key: &str) -> Result<Option<DomainEvent>>;
    async fn list_events_after(&self, sequence: i64, limit: i64) -> Result<Vec<DomainEvent>>;
    async fn get_consumer_cursor(&self, consumer_name: &str)
        -> Result<Option<EventConsumerCursor>>;
    async fn get_consumer_cutover(
        &self,
        consumer_name: &str,
    ) -> Result<Option<EventConsumerCutover>>;
}

/// Durable wake dispositions and semantic retry lineage. Initial persistence
/// uses SqliteDb::persist_agent_wake_in_tx inside the runtime checkpoint boundary.
/// Disposition attempts are immutable; deferred/setup-required retry paths
/// append a later attempt and move the current pointer.
#[async_trait]
pub trait AgentWakeDispositionRepo: Send + Sync {
    async fn get_agent_wake_disposition(
        &self,
        consumer_name: &str,
        source_event_id: &str,
        attempt_number: i64,
    ) -> Result<Option<AgentWakeDisposition>>;
    async fn get_current_agent_wake_disposition(
        &self,
        consumer_name: &str,
        source_event_id: &str,
    ) -> Result<Option<AgentWakeDisposition>>;
    async fn list_due_agent_wake_dispositions(
        &self,
        consumer_name: &str,
        now: &str,
        limit: i64,
    ) -> Result<Vec<AgentWakeDisposition>>;
    /// Return deferred attempts whose retry clock elapsed and setup-required
    /// attempts whose linked Attention incident or authoritative scope setup
    /// (binding/chat) changed after admission.  The latter is intentionally
    /// change-driven rather than an endless polling loop over an unchanged
    /// setup blocker.
    async fn list_reconsiderable_agent_wake_dispositions(
        &self,
        consumer_name: &str,
        now: &str,
        limit: i64,
    ) -> Result<Vec<AgentWakeDisposition>>;
    /// Append a due deferred/setup-required retry attempt and move the
    /// current pointer without rewriting the prior immutable attempt.
    async fn retry_agent_wake(
        &self,
        input: RetryAgentWakeDisposition,
    ) -> Result<AgentWakeDisposition>;
}

#[async_trait]
pub trait AttentionRepo: Send + Sync {
    async fn list_attention(&self, query: AttentionListQuery) -> Result<Page<AttentionProjection>>;
    async fn get_attention(&self, id: &str) -> Result<Option<AttentionProjection>>;
    async fn insert_attention(
        &self,
        input: CreateAttentionProjection,
    ) -> Result<AttentionProjection>;
    async fn update_attention_lifecycle(
        &self,
        input: UpdateAttentionLifecycle,
    ) -> Result<AttentionProjection>;
    async fn resolve_attention_by_dedupe(
        &self,
        dedupe_key: &str,
        source_event_id: &str,
        updated_at: &str,
    ) -> Result<Option<AttentionProjection>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDomainEvent {
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

impl CreateDomainEvent {
    /// Build the bounded recovery-disposition event for a Task interruption
    /// mutation. Writers append this in the same transaction as the Task row
    /// so attempt-level failures can remain audit-only without losing the
    /// action-required boundary.
    pub fn task_interruption_changed(task: &Task) -> Self {
        let material = crate::task_condition::material_blocker(&task.condition);
        let condition = task.condition.public();
        let material_digest =
            sha2::Sha256::digest(serde_json::to_vec(&material).expect("material serializes"))
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
        let event_id = crate::new_uuid_v4();

        Self {
            id: event_id.clone(),
            event_type: "task.interruption_changed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: event_id,
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "task-interruption-update:{}:{}:{}",
                task.id, task.version, material_digest
            )),
            payload_json: serde_json::json!({
                "task_id": interruption_bounded_text(&task.id, 128),
                "task_version": task.version,
                "task_status": interruption_bounded_text(&task.status, 128),
                "condition": condition,
                "material_blocker": material,
            })
            .to_string(),
            created_at: task.updated_at.clone(),
        }
    }

    /// Build the bounded, replayable ledger record for a task status
    /// transition. The transition-log id is deliberately reused as the event
    /// id/correlation key so retries cannot create a second source of truth.
    #[allow(clippy::too_many_arguments)]
    pub fn task_transition(
        transition_log_id: impl Into<String>,
        task_id: impl Into<String>,
        project_id: impl Into<String>,
        from_state: impl AsRef<str>,
        to_state: impl AsRef<str>,
        trigger_name: Option<&str>,
        triggered_by: impl AsRef<str>,
        trigger_reason: impl AsRef<str>,
        rejection: bool,
        created_at: impl Into<String>,
        workflow_snapshot: serde_json::Value,
    ) -> Self {
        let transition_log_id = transition_log_id.into();
        let task_id = task_id.into();
        let project_id = project_id.into();
        // State names are workflow identifiers; truncating them would detach
        // the ledger record from its immutable semantic snapshot.
        let from_state = from_state.as_ref().to_owned();
        let to_state = to_state.as_ref().to_owned();
        let trigger_name = trigger_name.map(|value| bounded_event_text(value, 128));
        let triggered_by = bounded_event_text(triggered_by.as_ref(), 256);
        let trigger_reason = bounded_event_text(trigger_reason.as_ref(), 512);
        let created_at = created_at.into();
        let (actor_type, actor_id) = triggered_by.split_once(':').map_or_else(
            || (triggered_by.clone(), None),
            |(kind, id)| (kind.to_owned(), Some(id.to_owned())),
        );
        let payload_json = serde_json::json!({
            "transition_log_id": transition_log_id,
            "project_id": project_id,
            "from_state": from_state,
            "to_state": to_state,
            "trigger_name": trigger_name,
            "trigger_reason": trigger_reason,
            "rejection": rejection,
            "workflow_snapshot": workflow_snapshot,
        })
        .to_string();

        Self {
            id: transition_log_id.clone(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task_id.clone(),
            actor_type,
            actor_id,
            scope_type: "task".to_owned(),
            scope_id: task_id,
            correlation_id: transition_log_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("task-transition:{transition_log_id}")),
            payload_json,
            created_at,
        }
    }
}

pub(crate) fn event_interruption_details(
    source: &str,
    value: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "source": source,
        "kind": value
            .get("kind")
            .or_else(|| value.get("type"))
            .and_then(serde_json::Value::as_str)
            .map(|text| interruption_bounded_text(text, 128)),
        "reason": value
            .get("reason")
            .or_else(|| value.get("blocking_reason"))
            .or_else(|| value.get("message"))
            .and_then(serde_json::Value::as_str)
            .map(|text| interruption_bounded_text(text, 512)),
        "execution_id": value
            .get("execution_id")
            .or_else(|| value.get("blocked_execution_id"))
            .and_then(serde_json::Value::as_str)
            .map(|text| interruption_bounded_text(text, 128)),
    })
}

/// Whether a Task interruption calls for autonomous recovery attention.
/// Manual stops retain their user-facing recovery controls but do not invite
/// an Agent to undo the user's stop. Use this for both event creation and
/// current-state wake admission so the two boundaries cannot disagree.
pub fn task_interruption_requires_intervention(
    error_annotation: Option<&str>,
    blocked_json: Option<&str>,
    failed_json: Option<&str>,
) -> bool {
    if failed_json.is_some() || blocked_json.is_some() {
        return true;
    }
    let annotation = error_annotation
        .map(parse_event_json_object)
        .unwrap_or_default();
    annotation
        .get("type")
        .cloned()
        .and_then(|kind| serde_json::from_value::<api_types::FailureKind>(kind).ok())
        .is_some_and(|kind| {
            !matches!(
                kind,
                api_types::FailureKind::ManualStop | api_types::FailureKind::Unknown
            )
        })
}

pub(crate) fn parse_event_json_object(raw: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()))
}

fn interruption_bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn bounded_event_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = value[..end].to_owned();
    output.push_str("[truncated]");
    output
}
