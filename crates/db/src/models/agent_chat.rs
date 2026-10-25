//! Agent bindings, chats, turns, and handoffs.

/// Versioned account-level binding for the singular Main Agent Chat.  Binding
/// rows are retained when replaced so historical messages keep their original
/// identity/profile attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountMainAgentBinding {
    pub id: String,
    pub account_id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub state: String,
    pub autonomy_policy_json: String,
    pub tool_policy_revision: String,
    pub version: i64,
    pub replaced_by_binding_id: Option<String>,
    pub replacement_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Versioned Project Agent binding.  A setup-required row intentionally has
/// no identity/profile; this is an explicit state for migrated Projects and
/// cannot be used to admit a model turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectAgentBinding {
    pub id: String,
    pub project_id: String,
    pub identity_id: Option<String>,
    pub profile_id: Option<String>,
    pub state: String,
    pub autonomy_policy_json: String,
    pub permission_ceiling_json: String,
    pub subscriptions_json: String,
    pub wake_budget: i64,
    pub operating_skill_revision_id: Option<String>,
    pub policy_revision: String,
    pub policy_digest: String,
    pub charter_id: Option<String>,
    pub charter_revision_id: Option<String>,
    pub charter_setup_required: bool,
    pub admission_receipt_id: Option<String>,
    pub charter_approval_id: Option<String>,
    pub version: i64,
    pub replaced_by_binding_id: Option<String>,
    pub replacement_reason: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Immutable proof that Forge admitted a Project through one exact Genesis
/// handoff or legacy-Charter adoption. Current Charter and binding authority
/// deliberately live elsewhere and may advance without replacing this row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectAdmissionReceipt {
    pub id: String,
    pub project_id: String,
    pub source_kind: String,
    pub handoff_id: Option<String>,
    pub initial_charter_approval_id: String,
    pub initial_charter_id: String,
    pub initial_charter_revision_id: String,
    pub payload_digest: String,
    pub validation_schema_version: String,
    pub validated_at: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChat {
    pub id: String,
    pub kind: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub status: String,
    pub instruction_revision: i64,
    pub message_count: i64,
    pub last_message_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatSourceRef {
    pub chat_id: String,
    pub source_type: String,
    pub source_id: String,
    pub source_scope_type: Option<String>,
    pub source_scope_id: Option<String>,
    pub source_revision: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatInstructionRevision {
    pub id: String,
    pub chat_id: String,
    pub source_type: String,
    pub source_id: Option<String>,
    pub revision: i64,
    pub body: String,
    pub content_guard_json: String,
    pub sensitivity: String,
    pub created_by_type: String,
    pub created_by_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatMessageAuthorType {
    User,
    Agent,
    System,
    Handoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatMessageStatus {
    Complete,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatMessage {
    pub id: String,
    pub chat_id: String,
    pub sequence: i64,
    pub author_type: AgentChatMessageAuthorType,
    pub author_id: Option<String>,
    pub content: String,
    pub content_guard_json: String,
    pub sensitivity: String,
    pub status: AgentChatMessageStatus,
    pub outcome: Option<String>,
    pub model: Option<String>,
    pub profile_id: Option<String>,
    pub session_id: Option<String>,
    pub context_manifest_id: Option<String>,
    pub token_usage_json: Option<String>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub handoff_id: Option<String>,
    pub source_type: String,
    pub source_id: Option<String>,
    pub source_message_id: Option<String>,
    pub source_room_id: Option<String>,
    pub source_conversation_id: Option<String>,
    pub source_sequence: Option<i64>,
    pub source_metadata_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatTurnState {
    Queued,
    Leased,
    AwaitingInput,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatTurnJob {
    pub id: String,
    pub chat_id: String,
    pub triggering_message_id: String,
    pub responder_identity_id: Option<String>,
    pub profile_id: Option<String>,
    /// Immutable admission provenance.  Legacy queued rows are nullable and
    /// are handled conservatively until re-admitted by the current resolver.
    pub responder_binding_id: Option<String>,
    pub responder_binding_version: Option<i64>,
    pub responder_identity_version: Option<i64>,
    pub profile_version: Option<i64>,
    pub operating_skill_revision_id: Option<String>,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub permission_policy_digest: Option<String>,
    pub tool_policy_digest: Option<String>,
    pub admission_digest: Option<String>,
    pub canonical_scope_provenance_json: Option<String>,
    pub canonical_scope_type: String,
    pub canonical_scope_id: String,
    pub status: AgentChatTurnState,
    pub pending_interaction_id: Option<String>,
    pub dedupe_key: String,
    pub lease_owner: Option<String>,
    pub leased_until: Option<String>,
    pub attempt_count: i64,
    pub max_attempts: i64,
    pub next_attempt_at: Option<String>,
    pub response_message_id: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failure_class: Option<api_types::TurnFailure>,
    pub retry_decision: Option<api_types::TurnRetryDecision>,
    pub pre_provider_failure_count: i64,
    pub invocation_count: i64,
    pub usage_limit_deferral_count: i64,
    pub usage_limit_first_deferred_at: Option<String>,
    /// Derived from later turns for this message or any later chat message.
    /// Computed only for failed or cancelled turns, the ones `retry_action`
    /// consults; `false` for every other status.
    pub retry_superseded: bool,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

impl AgentChatTurnJob {
    pub fn retry_action(&self) -> Option<api_types::RetryTurnAction> {
        (matches!(
            self.status,
            AgentChatTurnState::Failed | AgentChatTurnState::Cancelled
        ) && !self.retry_superseded)
            .then(|| api_types::RetryTurnAction {
                kind: api_types::RetryTurnActionKind::RetryTurn,
                chat_id: self.chat_id.clone(),
                turn_id: self.id.clone(),
                expected_version: self.version,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentHandoffStatus {
    Pending,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentHandoff {
    pub id: String,
    pub source_chat_id: String,
    pub target_chat_id: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub target_message_id: Option<String>,
    pub target_turn_job_id: Option<String>,
    pub author_identity_id: Option<String>,
    pub content: String,
    pub content_guard_json: String,
    pub source_revisions_json: String,
    pub status: AgentHandoffStatus,
    pub error_code: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
    pub updated_at: String,
}
