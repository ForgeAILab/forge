//! Agent binding, chat, message, turn, and handoff repositories.

use super::*;

#[async_trait]
pub trait AccountMainAgentBindingRepo: Send + Sync {
    async fn get_active_main_binding(
        &self,
        account_id: &str,
    ) -> Result<Option<AccountMainAgentBinding>>;
    async fn get_main_binding(&self, id: &str) -> Result<Option<AccountMainAgentBinding>>;
    async fn list_main_binding_history(
        &self,
        account_id: &str,
    ) -> Result<Vec<AccountMainAgentBinding>>;
    async fn create_main_binding(
        &self,
        input: CreateAccountMainAgentBinding,
    ) -> Result<AccountMainAgentBinding>;
    async fn replace_main_binding(
        &self,
        input: ReplaceAccountMainAgentBinding,
    ) -> Result<AccountMainAgentBinding>;
}

#[async_trait]
pub trait ProjectAgentBindingRepo: Send + Sync {
    async fn get_active_project_binding(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectAgentBinding>>;
    async fn get_project_binding(&self, id: &str) -> Result<Option<ProjectAgentBinding>>;
    async fn list_project_binding_history(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectAgentBinding>>;
    async fn create_project_binding(
        &self,
        input: CreateProjectAgentBinding,
    ) -> Result<ProjectAgentBinding>;
    async fn replace_project_binding(
        &self,
        input: ReplaceProjectAgentBinding,
    ) -> Result<ProjectAgentBinding>;
}

#[async_trait]
pub trait ProjectBindingCommandRepo: Send + Sync {
    async fn set_project_binding_command(
        &self,
        input: SetProjectAgentBindingCommand,
    ) -> Result<ProjectAgentBinding>;
}

#[async_trait]
pub trait ProjectAdmissionReceiptRepo: Send + Sync {
    async fn get_project_admission_receipt(
        &self,
        id: &str,
    ) -> Result<Option<ProjectAdmissionReceipt>>;
    async fn get_project_admission_receipt_for_project(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectAdmissionReceipt>>;
    async fn create_project_admission_receipt(
        &self,
        input: CreateProjectAdmissionReceipt,
    ) -> Result<ProjectAdmissionReceipt>;
    async fn resolve_current_project_binding_authority(
        &self,
        project_id: &str,
    ) -> Result<Option<CurrentProjectBindingAuthority>>;
    async fn get_current_project_operating_skill_revision(&self) -> Result<Option<String>>;
}

#[async_trait]
pub trait AgentChatRepo: Send + Sync {
    async fn get_agent_chat(&self, id: &str) -> Result<Option<AgentChat>>;
    async fn get_main_chat(&self, account_id: &str) -> Result<Option<AgentChat>>;
    async fn get_project_chat(&self, project_id: &str) -> Result<Option<AgentChat>>;
    async fn list_agent_chats(&self, account_id: &str) -> Result<Vec<AgentChat>>;
    async fn create_agent_chat(&self, input: CreateAgentChat) -> Result<AgentChat>;
    async fn update_agent_chat(&self, input: UpdateAgentChat) -> Result<AgentChat>;
    async fn list_chat_source_refs(&self, chat_id: &str) -> Result<Vec<AgentChatSourceRef>>;
    async fn list_chat_instructions(
        &self,
        chat_id: &str,
    ) -> Result<Vec<AgentChatInstructionRevision>>;
}

#[async_trait]
pub trait AgentChatMessageRepo: Send + Sync {
    async fn get_agent_chat_message(&self, id: &str) -> Result<Option<AgentChatMessage>>;
    async fn list_agent_chat_messages(
        &self,
        query: AgentChatMessageListQuery,
    ) -> Result<Page<AgentChatMessage>>;
    /// Append one standalone message (no turn admission). The store allocates
    /// the sequence; `input.sequence` is only a hint for fakes. The message id
    /// is the idempotency key — a same-id replay returns the stored row.
    async fn append_agent_chat_message(
        &self,
        input: CreateAgentChatMessage,
    ) -> Result<AgentChatMessage>;
}

#[async_trait]
pub trait AgentChatTurnJobRepo: Send + Sync {
    async fn get_agent_chat_turn_job(&self, id: &str) -> Result<Option<AgentChatTurnJob>>;
    async fn list_agent_chat_turn_jobs(&self, chat_id: &str) -> Result<Vec<AgentChatTurnJob>>;
    /// Turns of one chat that still owe work: `queued`, `leased` or `retry_wait`.
    async fn count_pending_agent_chat_turn_jobs(&self, chat_id: &str) -> Result<i64>;
    async fn create_agent_chat_turn_job(
        &self,
        input: CreateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob>;
    async fn update_agent_chat_turn_job(
        &self,
        input: UpdateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob>;
}

#[async_trait]
pub trait AgentHandoffRepo: Send + Sync {
    async fn get_agent_handoff(&self, id: &str) -> Result<Option<AgentHandoff>>;
    async fn list_agent_handoffs(&self, target_chat_id: &str) -> Result<Vec<AgentHandoff>>;
    async fn create_agent_handoff(&self, input: CreateAgentHandoff) -> Result<AgentHandoff>;
}

/// Short transactional composites used by the singular chat service.  The
/// individual repositories remain useful for inspection, while these methods
/// make message/turn and handoff delivery idempotent at the database boundary.
#[async_trait]
pub trait AgentChatTransactionRepo: Send + Sync {
    async fn admit_agent_chat_turn(
        &self,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn>;
    async fn admit_agent_chat_turn_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn>;
    /// Queue a causally linked continuation against an existing visible
    /// trigger message. The caller owns the surrounding domain transaction.
    async fn admit_agent_chat_continuation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob>;
    async fn complete_agent_chat_turn(
        &self,
        input: CompleteAgentChatTurn,
    ) -> Result<CompletedAgentChatTurn>;
    /// Complete a turn and settle the provider-attempt ledger in one SQLite
    /// composite.  Repository fakes that predate the ledger may use the
    /// default implementation; the SQLite adapter overrides it with the
    /// atomic CAS/settlement boundary.
    async fn complete_agent_chat_turn_with_usage(
        &self,
        input: CompleteAgentChatTurnWithUsage,
    ) -> Result<CompletedAgentChatTurn> {
        self.complete_agent_chat_turn(input.terminal).await
    }
    async fn complete_agent_chat_control_transfer(
        &self,
        input: CompleteAgentChatControlTransfer,
    ) -> Result<AgentChatTurnJob>;
    /// Complete the Genesis source turn's control-transfer boundary and
    /// settle the provider attempt in one SQLite composite. `provider_finished`
    /// is false when the baseline provider was stopped before it returned;
    /// in that case the invocation remains pending-settlement instead of
    /// being misclassified as unmetered.
    async fn complete_agent_chat_control_transfer_with_usage(
        &self,
        input: CompleteAgentChatControlTransferWithUsage,
    ) -> Result<AgentChatTurnJob> {
        self.complete_agent_chat_control_transfer(input.terminal)
            .await
    }
    async fn fail_agent_chat_turn(&self, input: FailAgentChatTurn) -> Result<AgentChatTurnJob>;
    /// Retry/terminal failure plus usage settlement.  The default preserves
    /// old in-memory repository behavior while the SQLite implementation
    /// composes the turn CAS, ledger rows, and durable event.
    async fn fail_agent_chat_turn_with_usage(
        &self,
        input: FailAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        self.fail_agent_chat_turn(input.terminal).await
    }
    async fn park_agent_chat_turn(&self, input: ParkAgentChatTurn) -> Result<AgentChatTurnJob>;
    async fn park_agent_chat_turn_with_usage(
        &self,
        input: ParkAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        self.park_agent_chat_turn(input.terminal).await
    }
    /// Fence the terminal source and atomically admit a fresh retry with a
    /// durable idempotency event. Authority is prepared by the admission service.
    async fn retry_agent_chat_turn(&self, input: RetryAgentChatTurn) -> Result<AgentChatTurnJob>;

    /// Cancel a queued/leased/retry-wait turn and append the cancellation
    /// event in the same transaction. The idempotency key is represented by
    /// the event dedupe key so retries do not require a second turn-job store.
    async fn cancel_agent_chat_turn(&self, input: CancelAgentChatTurn) -> Result<AgentChatTurnJob>;
    async fn cancel_agent_chat_turn_with_usage(
        &self,
        input: CancelAgentChatTurnWithUsage,
    ) -> Result<AgentChatTurnJob> {
        self.cancel_agent_chat_turn(input.terminal).await
    }
    async fn admit_agent_handoff(&self, input: AdmitAgentHandoff) -> Result<AdmittedAgentHandoff>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAccountMainAgentBinding {
    pub id: String,
    pub account_id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub autonomy_policy_json: String,
    pub tool_policy_revision: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceAccountMainAgentBinding {
    pub account_id: String,
    pub expected_version: i64,
    pub replacement: CreateAccountMainAgentBinding,
    pub replacement_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectAgentBinding {
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
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProjectAdmissionReceipt {
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
pub struct CurrentProjectBindingAuthority {
    pub project_id: String,
    pub admission_receipt_id: String,
    pub charter_approval_id: String,
    pub charter_id: String,
    pub charter_revision_id: String,
    pub operating_skill_revision_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceProjectAgentBinding {
    pub project_id: String,
    pub expected_version: i64,
    pub replacement: CreateProjectAgentBinding,
    pub replacement_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetProjectAgentBindingCommand {
    pub actor_user_id: String,
    pub expected_version: Option<i64>,
    pub replacement: CreateProjectAgentBinding,
    pub replacement_reason: Option<String>,
    pub event_id: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentChat {
    pub id: String,
    pub kind: String,
    pub account_id: Option<String>,
    pub project_id: Option<String>,
    pub status: String,
    pub instruction_revision: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentChat {
    pub id: String,
    pub expected_version: i64,
    pub status: Option<String>,
    pub instruction_revision: Option<i64>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentChatMessage {
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
pub struct AgentChatMessageListQuery {
    pub chat_id: String,
    pub before_sequence: Option<i64>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentChatTurnJob {
    pub id: String,
    pub chat_id: String,
    pub triggering_message_id: String,
    pub responder_identity_id: String,
    pub profile_id: String,
    /// Immutable admission provenance.  Legacy producers may leave these
    /// nullable until they are migrated to the shared admission service.
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
    pub dedupe_key: String,
    pub max_attempts: i64,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentChatTurnJob {
    pub id: String,
    pub expected_version: i64,
    pub status: AgentChatTurnState,
    pub pending_interaction_id: Option<Option<String>>,
    pub lease_owner: Option<Option<String>>,
    pub leased_until: Option<Option<String>>,
    pub attempt_count: Option<i64>,
    pub next_attempt_at: Option<Option<String>>,
    pub response_message_id: Option<Option<String>>,
    pub error_code: Option<Option<String>>,
    pub error_message: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentHandoff {
    pub id: String,
    pub source_chat_id: String,
    pub target_chat_id: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub author_identity_id: Option<String>,
    pub content: String,
    pub content_guard_json: String,
    pub source_revisions_json: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitAgentChatTurn {
    pub message: CreateAgentChatMessage,
    pub turn: CreateAgentChatTurnJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedAgentChatTurn {
    pub message: AgentChatMessage,
    pub turn: AgentChatTurnJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteAgentChatTurn {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub response: CreateAgentChatMessage,
    pub updated_at: String,
}

/// A chat completion input whose terminal CAS and observed usage reports are
/// committed together.  `settlements` is prepared before entering the DB
/// boundary so a persistence retry reuses the exact invocation/event IDs.
#[derive(Debug, Clone, PartialEq)]
pub struct CompleteAgentChatTurnWithUsage {
    pub terminal: CompleteAgentChatTurn,
    pub settlements: Vec<UsageLedgerSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteAgentChatControlTransfer {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub command_receipt_id: String,
    pub continuation_turn_id: String,
    pub genesis_session_id: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteAgentChatControlTransferWithUsage {
    pub terminal: CompleteAgentChatControlTransfer,
    pub settlements: Vec<UsageLedgerSettlement>,
    pub provider_finished: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailAgentChatTurn {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub status: AgentChatTurnState,
    pub attempt_count: i64,
    pub next_attempt_at: Option<String>,
    pub error_code: String,
    pub error_message: String,
    pub failure_class: api_types::TurnFailure,
    pub retry_decision: api_types::TurnRetryDecision,
    pub pre_provider_failure_count: i64,
    pub usage_limit_deferral_count: i64,
    pub usage_limit_first_deferred_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FailAgentChatTurnWithUsage {
    pub terminal: FailAgentChatTurn,
    pub settlements: Vec<UsageLedgerSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkAgentChatTurn {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub pending_interaction_id: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParkAgentChatTurnWithUsage {
    pub terminal: ParkAgentChatTurn,
    pub settlements: Vec<UsageLedgerSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAgentChatTurn {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub actor_user_id: String,
    pub idempotency_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryAgentChatTurn {
    pub new_turn: CreateAgentChatTurnJob,
    pub turn_job_id: String,
    pub expected_version: i64,
    pub actor_user_id: String,
    pub idempotency_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CancelAgentChatTurnWithUsage {
    pub terminal: CancelAgentChatTurn,
    pub settlements: Vec<UsageLedgerSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedAgentChatTurn {
    pub response: AgentChatMessage,
    pub turn: AgentChatTurnJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitAgentHandoff {
    pub handoff: CreateAgentHandoff,
    pub target_message: CreateAgentChatMessage,
    pub target_turn: CreateAgentChatTurnJob,
    /// Redaction-safe source responder provenance prepared by the shared
    /// resolver.  The SQLite composite revalidates it against the current
    /// Main binding/Profile before publishing a handoff.
    pub source_responder_provenance_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedAgentHandoff {
    pub handoff: AgentHandoff,
    pub message: AgentChatMessage,
    pub turn: AgentChatTurnJob,
}
