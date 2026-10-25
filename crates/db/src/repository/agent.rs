//! Agent identity, sessions, commitments, actions, LCM, and inquiry repositories.

use super::*;

#[async_trait]
pub trait AgentRepo: Send + Sync {
    async fn create(&self, input: CreateAgent) -> Result<Agent>;
    async fn create_identity_with_profile(
        &self,
        identity: CreateAgentIdentity,
        profile: CreateAgentProfile,
    ) -> Result<Agent>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Agent>>;
    async fn list(&self, query: AgentListQuery) -> Result<Page<Agent>>;
    async fn list_visible(&self, user_id: &str, query: AgentListQuery) -> Result<Page<Agent>>;
    async fn update(&self, input: UpdateAgent) -> Result<Agent>;
    async fn set_paused(&self, id: &str, paused: bool) -> Result<()>;
    async fn duplicate_agent(
        &self,
        source_id: &str,
        new_id: String,
        new_name: String,
        now: String,
    ) -> Result<Agent>;
    async fn archive(&self, id: &str, archived_at: &str) -> Result<()>;
    /// Tasks assigned to this identity that sit in an active or gate workflow
    /// state, plus its in-flight chat turns. This is assigned workload, not
    /// live concurrency, and is deliberately *not* what
    /// `max_concurrent_tasks` gates — see [`Self::count_running_executions`].
    async fn count_active_assigned_tasks(&self, agent_id: &str) -> Result<i64>;
    /// Executions currently running for this identity. This is the quantity
    /// `max_concurrent_tasks` bounds, and the one a capacity ratio should use.
    async fn count_running_executions(&self, agent_id: &str) -> Result<i64>;
}

#[async_trait]
pub trait AgentProfileRepo: Send + Sync {
    async fn create_profile(&self, input: CreateAgentProfile) -> Result<AgentProfile>;
    async fn create_and_select_profile(
        &self,
        profile: CreateAgentProfile,
        selection: SelectAgentProfile,
    ) -> Result<(AgentProfile, Agent)>;
    async fn get_profile(&self, id: &str) -> Result<Option<AgentProfile>>;
    async fn list_profiles(&self, identity_id: &str) -> Result<Vec<AgentProfile>>;
    async fn select_profile(&self, input: SelectAgentProfile) -> Result<Agent>;
}

#[async_trait]
pub trait CredentialHandleRepo: Send + Sync {
    async fn get_credential_handle(&self, id: &str) -> Result<Option<CredentialHandle>>;
    async fn get_credential_handle_for_owner(
        &self,
        id: &str,
        owner_user_id: &str,
    ) -> Result<Option<CredentialHandle>>;
    async fn list_credential_handles(&self, owner_user_id: &str) -> Result<Vec<CredentialHandle>>;
    async fn rename_credential_handle(
        &self,
        id: &str,
        owner_user_id: &str,
        label: &str,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<CredentialHandle>;
    async fn set_credential_handle_enabled(
        &self,
        id: &str,
        owner_user_id: &str,
        enabled: bool,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<CredentialHandle>;
    /// Agents whose active profile references each of the owner's provider
    /// entries.
    async fn list_credential_usage(&self, owner_user_id: &str) -> Result<Vec<CredentialUsage>>;
    /// Most recent admitted invocation per provider entry, keyed by
    /// credential id. Attributed through the invoking profile, so usage by
    /// agents that have since switched entries still counts.
    async fn get_provider_entry_health(
        &self,
        credential_id: &str,
    ) -> Result<Option<ProviderEntryHealth>>;
    async fn list_provider_entry_health(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<ProviderEntryHealth>>;
    /// Writes the whole health row only if the row seen by the caller is still
    /// current. A failed compare lets concurrent outcomes recompute backoff.
    async fn upsert_provider_entry_health(
        &self,
        health: ProviderEntryHealth,
        expected_version: Option<i64>,
    ) -> Result<bool>;
    /// Clears an unhealthy entry back to healthy. Auth failures clear only
    /// after a successful manual test. A no-op when absent or already healthy.
    async fn mark_provider_entry_healthy(
        &self,
        credential_id: &str,
        now: &str,
        manual_test: bool,
    ) -> Result<bool>;
    async fn list_credential_last_used(
        &self,
        owner_user_id: &str,
    ) -> Result<std::collections::HashMap<String, String>>;
    async fn revoke_credential_handle(
        &self,
        id: &str,
        owner_user_id: &str,
        updated_at: &str,
    ) -> Result<CredentialHandle>;
}

#[async_trait]
pub trait ProviderAuthorizationRepo: Send + Sync {
    async fn create_provider_authorization(
        &self,
        input: CreateProviderAuthorizationOperation,
    ) -> Result<ProviderAuthorizationOperation>;
    async fn get_provider_authorization(
        &self,
        id: &str,
        owner_user_id: &str,
    ) -> Result<Option<ProviderAuthorizationOperation>>;
    async fn get_provider_authorization_by_state_hash(
        &self,
        callback_state_hash: &str,
    ) -> Result<Option<ProviderAuthorizationOperation>>;
    async fn update_provider_authorization(
        &self,
        input: UpdateProviderAuthorizationOperation,
    ) -> Result<ProviderAuthorizationOperation>;
}

#[async_trait]
pub trait AgentContextScopeRepo: Send + Sync {
    async fn create_context_scope(
        &self,
        input: CreateAgentContextScope,
    ) -> Result<AgentContextScope>;
    async fn get_context_scope(&self, id: &str) -> Result<Option<AgentContextScope>>;
    async fn get_context_scope_for_identity(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
        task_role: Option<&str>,
    ) -> Result<Option<AgentContextScope>>;
    async fn list_context_scopes(&self, identity_id: &str) -> Result<Vec<AgentContextScope>>;
}

#[async_trait]
pub trait AgentSessionRepo: Send + Sync {
    async fn create_agent_session(&self, input: CreateAgentSession) -> Result<AgentSession>;
    async fn get_agent_session(&self, id: &str) -> Result<Option<AgentSession>>;
    async fn list_agent_sessions(&self, identity_id: &str) -> Result<Vec<AgentSession>>;
    async fn get_active_agent_session(
        &self,
        identity_id: &str,
        context_scope_id: &str,
    ) -> Result<Option<AgentSession>>;
    async fn update_agent_session(&self, input: UpdateAgentSession) -> Result<AgentSession>;
    async fn rotate_agent_session(&self, input: RotateAgentSession) -> Result<AgentSession>;
    /// Startup crash recovery: suspends every native-backend session left in
    /// a non-terminal status by a previous process. Native runtimes live
    /// inside the server process, so after a restart those rows cannot
    /// describe a live runtime. Suspension removes them from the active-scope
    /// set; the next create-or-resume in the same scope starts a fresh
    /// runtime session, which retires the old session's LCM timeline when it
    /// first binds one (`create_or_get_lcm_timeline`).
    /// Returns the number of sessions suspended.
    async fn suspend_stale_native_sessions(&self, updated_at: &str) -> Result<u64>;
}

#[async_trait]
pub trait AgentConnectionHealthRepo: Send + Sync {
    async fn upsert_connection_health(
        &self,
        input: UpsertAgentConnectionHealth,
    ) -> Result<AgentConnectionHealth>;
    async fn get_connection_health(
        &self,
        profile_id: &str,
    ) -> Result<Option<AgentConnectionHealth>>;
}

#[async_trait]
pub trait AgentCommitmentRepo: Send + Sync {
    async fn create_commitment(&self, input: CreateAgentCommitment) -> Result<AgentCommitment>;
    async fn get_commitment(&self, id: &str) -> Result<Option<AgentCommitment>>;
    async fn list_commitments(
        &self,
        query: AgentCommitmentListQuery,
    ) -> Result<Vec<AgentCommitment>>;
    async fn update_commitment(&self, input: UpdateAgentCommitment) -> Result<AgentCommitment>;
    async fn complete_commitment(&self, input: CompleteAgentCommitment) -> Result<AgentCommitment>;
    async fn transfer_commitment(&self, input: TransferAgentCommitment) -> Result<AgentCommitment>;
    async fn add_commitment_evidence(
        &self,
        input: CreateAgentCommitmentEvidence,
    ) -> Result<AgentCommitmentEvidence>;
    async fn list_commitment_evidence(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentEvidence>>;
    async fn list_commitment_transfers(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentTransfer>>;
    async fn list_commitment_lifecycle(
        &self,
        commitment_id: &str,
    ) -> Result<Vec<AgentCommitmentLifecycle>>;
}

#[async_trait]
pub trait AgentInboxRepo: Send + Sync {
    async fn create_inbox_item(&self, input: CreateAgentInboxItem) -> Result<AgentInboxItem>;
    async fn get_inbox_item(&self, id: &str) -> Result<Option<AgentInboxItem>>;
    async fn list_inbox_items(&self, query: AgentInboxListQuery) -> Result<Vec<AgentInboxItem>>;
    async fn update_inbox_item(&self, input: UpdateAgentInboxItem) -> Result<AgentInboxItem>;
    async fn create_question_with_inbox(
        &self,
        inbox: CreateAgentInboxItem,
        question: CreateAgentQuestion,
    ) -> Result<AgentQuestion>;
    async fn create_question(&self, input: CreateAgentQuestion) -> Result<AgentQuestion>;
    async fn get_question(&self, id: &str) -> Result<Option<AgentQuestion>>;
    async fn list_questions(&self, query: AgentQuestionListQuery) -> Result<Vec<AgentQuestion>>;
    async fn answer_question(&self, input: AnswerAgentQuestion) -> Result<AgentQuestion>;
}

#[async_trait]
pub trait AgentActionRepo: Send + Sync {
    async fn create_action(&self, input: CreateAgentAction) -> Result<AgentAction>;
    async fn get_action(&self, id: &str) -> Result<Option<AgentAction>>;
    async fn list_actions(&self, query: AgentActionListQuery) -> Result<Vec<AgentAction>>;
    async fn update_action(&self, input: UpdateAgentAction) -> Result<AgentAction>;
    async fn record_action_approval(
        &self,
        input: CreateAgentActionApproval,
    ) -> Result<AgentActionApproval>;
    async fn list_action_approvals(&self, action_id: &str) -> Result<Vec<AgentActionApproval>>;
    async fn record_action_execution(
        &self,
        input: CreateAgentActionExecution,
    ) -> Result<AgentActionExecution>;
    /// Record the execution row and CAS the action's terminal status/outcome
    /// inside a caller-owned transaction.  The caller commits this together
    /// with the command's domain rows, receipt, and event.
    async fn record_action_execution_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateAgentActionExecution,
    ) -> Result<AgentActionExecution>;
    async fn get_successful_action_execution(
        &self,
        action_id: &str,
    ) -> Result<Option<AgentActionExecution>>;
    async fn list_action_executions(&self, action_id: &str) -> Result<Vec<AgentActionExecution>>;
}

/// Durable replay records for shared mutating commands.
// The lookup deliberately spells out the complete canonical receipt identity
// at the repository boundary so callers cannot accidentally omit one of its
// authorization or replay components.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait CommandReceiptRepo: Send + Sync {
    /// Look up a receipt by its canonical storage identity without requiring
    /// the caller to know the input digest. Command services still use the
    /// digest-aware lookup to enforce replay equality.
    #[allow(clippy::too_many_arguments)]
    async fn get_command_receipt_by_identity(
        &self,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<Option<CommandReceipt>>;
    /// Look up the receipt by its complete canonical identity.  If the key is
    /// already committed with a different input digest, return
    /// `DbError::IdempotencyConflict` rather than exposing or rerunning it.
    async fn get_command_receipt(
        &self,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
        input_digest: &str,
    ) -> Result<Option<CommandReceipt>>;
    /// Look up the single frozen receipt linked to an action execution.
    ///
    /// This is used by adapters whose coarse action operation differs from
    /// the lifecycle-specific command operation stored in the receipt.
    async fn get_command_receipt_by_agent_action_execution(
        &self,
        agent_action_execution_id: &str,
    ) -> Result<Option<CommandReceipt>>;
    async fn create_command_receipt(&self, input: CreateCommandReceipt) -> Result<CommandReceipt>;
    async fn get_command_receipt_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        principal_type: &str,
        principal_id: &str,
        scope_type: &str,
        scope_id: &str,
        operation: &str,
        idempotency_key: &str,
        input_digest: &str,
    ) -> Result<Option<CommandReceipt>>;
    async fn create_command_receipt_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateCommandReceipt,
    ) -> Result<CommandReceipt>;
}

#[async_trait]
pub trait AgentLcmRepo: Send + Sync {
    async fn create_or_get_lcm_timeline(
        &self,
        input: CreateAgentLcmTimeline,
    ) -> Result<AgentLcmTimeline>;
    async fn get_lcm_timeline(&self, id: &str) -> Result<Option<AgentLcmTimeline>>;
    async fn get_lcm_timeline_for_binding(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<Option<AgentLcmTimeline>>;
    async fn list_lcm_entries(
        &self,
        timeline_id: &str,
        start: i64,
        end: i64,
        limit: i64,
    ) -> Result<Vec<AgentLcmEntryRecord>>;
    async fn list_lcm_nodes(
        &self,
        timeline_id: &str,
        active_only: bool,
    ) -> Result<Vec<AgentLcmNodeRecord>>;
    async fn get_lcm_node(
        &self,
        timeline_id: &str,
        node_id: &str,
    ) -> Result<Option<AgentLcmNodeRecord>>;
    async fn get_lcm_operation(
        &self,
        timeline_id: &str,
        operation_id: &str,
    ) -> Result<Option<AgentLcmOperation>>;
    async fn append_lcm_entries(
        &self,
        input: AppendAgentLcmEntries,
    ) -> Result<AgentLcmMutationResult>;
    /// Removes the provisional tail of a timeline's immutable sequence,
    /// starting at `from_sequence` (inclusive). Fails when any summary
    /// node's source range reaches into the truncated span.
    async fn truncate_lcm_entries_from(
        &self,
        timeline_id: &str,
        from_sequence: i64,
        updated_at: &str,
        claim: Option<&AgentLcmClaimFence>,
    ) -> Result<AgentLcmTruncation>;
    async fn commit_lcm_leaf(&self, input: CommitAgentLcmLeaf) -> Result<AgentLcmMutationResult>;
    async fn commit_lcm_condensation(
        &self,
        input: CommitAgentLcmCondensation,
    ) -> Result<AgentLcmMutationResult>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentLcmTimeline {
    pub id: String,
    pub identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub authorization_revision: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmClaimFence {
    pub owner: String,
    pub generation: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendAgentLcmEntries {
    pub claim: Option<AgentLcmClaimFence>,
    pub timeline_id: String,
    pub expected_revision: i64,
    pub operation_id: String,
    pub operation_fingerprint: String,
    pub entries: Vec<AgentLcmEntryRecord>,
    pub expected_sequence: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAgentLcmLeaf {
    pub claim: Option<AgentLcmClaimFence>,
    pub timeline_id: String,
    pub expected_revision: i64,
    pub operation_id: String,
    pub operation_fingerprint: String,
    pub node: AgentLcmNodeRecord,
    pub entry_ids: Vec<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAgentLcmCondensation {
    pub claim: Option<AgentLcmClaimFence>,
    pub timeline_id: String,
    pub expected_revision: i64,
    pub operation_id: String,
    pub operation_fingerprint: String,
    pub node: AgentLcmNodeRecord,
    pub child_node_ids: Vec<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentLcmMutationResult {
    pub revision: i64,
    pub already_committed: bool,
    pub entries: i64,
    pub node_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLcmTruncation {
    pub revision: i64,
    pub removed: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentListQuery {
    pub status: Option<AgentStatus>,
    pub executor_type: Option<String>,
    pub capabilities: Vec<String>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgent {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub executor_type: String,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities_json: String,
    pub config_json: String,
    pub credential_ref: Option<String>,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: i64,
    pub heartbeat_interval_seconds: i64,
    pub max_missed_heartbeats: i64,
    pub status: AgentStatus,
    pub last_heartbeat_at: Option<String>,
    pub is_default: bool,
    pub paused: bool,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentIdentity {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub max_concurrent_tasks: i64,
    pub heartbeat_interval_seconds: i64,
    pub max_missed_heartbeats: i64,
    pub status: AgentStatus,
    pub last_heartbeat_at: Option<String>,
    pub is_default: bool,
    pub paused: bool,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub account_permission_ceiling: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgent {
    pub id: String,
    pub expected_version: i64,
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub model: Option<Option<String>>,
    pub reasoning_effort: Option<Option<String>>,
    pub permission_policy: Option<Option<String>>,
    pub prompt_template: Option<Option<String>>,
    pub capabilities_json: Option<String>,
    pub config_json: Option<String>,
    pub daemon_id: Option<Option<String>>,
    pub max_concurrent_tasks: Option<i64>,
    pub heartbeat_interval_seconds: Option<i64>,
    pub max_missed_heartbeats: Option<i64>,
    pub status: Option<AgentStatus>,
    pub last_heartbeat_at: Option<Option<String>>,
    pub is_default: Option<bool>,
    pub paused: Option<bool>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentProfile {
    pub id: String,
    pub identity_id: String,
    pub backend_kind: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities_json: String,
    pub tool_policy_json: String,
    pub config_json: String,
    pub credential_ref: Option<String>,
    pub daemon_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectAgentProfile {
    pub identity_id: String,
    pub profile_id: String,
    pub expected_version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentContextScope {
    pub id: String,
    pub identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub task_role: Option<String>,
    pub workspace_access: String,
    pub authority_json: String,
    pub created_at: String,
    pub updated_at: String,
    /// Set only for a `project_verify` scope, which owns its workspace
    /// directly rather than through the Task `workspace` table.
    pub workspace_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentSession {
    pub id: String,
    pub identity_id: String,
    pub profile_id: String,
    pub context_scope_id: String,
    pub backend_kind: String,
    pub runtime_session_id: Option<String>,
    pub status: String,
    pub capabilities_json: String,
    pub connection_status: String,
    pub predecessor_session_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentSession {
    pub id: String,
    pub expected_version: i64,
    pub runtime_session_id: Option<Option<String>>,
    pub status: Option<String>,
    pub connection_status: Option<String>,
    pub last_activity_at: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotateAgentSession {
    pub previous_session_id: String,
    pub expected_version: i64,
    pub replacement: CreateAgentSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertAgentConnectionHealth {
    pub profile_id: String,
    pub status: String,
    pub capability_status_json: String,
    pub checked_at: Option<String>,
    pub error_code: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentCommitment {
    pub id: String,
    pub owner_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: AgentCommitmentStatus,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub originating_action_id: Option<String>,
    pub originating_task_id: Option<String>,
    pub evidence_required: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommitmentListQuery {
    pub visible_to_user_id: Option<String>,
    pub owner_identity_id: Option<String>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub status: Option<AgentCommitmentStatus>,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentCommitment {
    pub id: String,
    pub expected_version: i64,
    pub status: Option<AgentCommitmentStatus>,
    pub due_at: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub blocked_reason: Option<Option<String>>,
    pub cancellation_reason: Option<Option<String>>,
    pub actor_type: String,
    pub actor_id: String,
    pub reason: Option<String>,
    pub evidence_id: Option<String>,
    pub dedupe_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteAgentCommitment {
    pub id: String,
    pub expected_version: i64,
    pub evidence: CreateAgentCommitmentEvidence,
    pub actor_type: String,
    pub actor_id: String,
    pub reason: Option<String>,
    pub dedupe_key: String,
    pub completed_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferAgentCommitment {
    pub id: String,
    pub expected_version: i64,
    pub to_identity_id: String,
    pub reason: String,
    pub actor_type: String,
    pub actor_id: String,
    pub dedupe_key: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentCommitmentEvidence {
    pub id: String,
    pub commitment_id: String,
    pub evidence_type: String,
    pub evidence_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub description: Option<String>,
    pub metadata_json: String,
    pub authorized_by_type: String,
    pub authorized_by_id: String,
    pub dedupe_key: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentInboxItem {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub kind: AgentInboxKind,
    pub status: AgentInboxStatus,
    pub title: String,
    pub body: String,
    pub payload_json: String,
    pub source_type: Option<String>,
    pub source_id: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub dedupe_key: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInboxListQuery {
    pub visible_to_user_id: Option<String>,
    pub recipient_identity_id: String,
    pub status: Option<AgentInboxStatus>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentInboxItem {
    pub id: String,
    pub expected_version: i64,
    pub status: AgentInboxStatus,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentQuestion {
    pub id: String,
    pub recipient_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub question: String,
    pub context_json: String,
    pub asked_by_type: String,
    pub asked_by_id: String,
    pub inbox_item_id: Option<String>,
    pub due_at: Option<String>,
    pub correlation_id: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentQuestionListQuery {
    pub visible_to_user_id: Option<String>,
    pub recipient_identity_id: String,
    pub status: Option<AgentQuestionStatus>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnswerAgentQuestion {
    pub id: String,
    pub expected_version: i64,
    pub answer: String,
    pub answered_by_type: String,
    pub answered_by_id: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentAction {
    pub id: String,
    pub actor_identity_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub payload_json: String,
    pub payload_hash: String,
    pub dedupe_key: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub requested_permission: String,
    pub policy_result: AgentActionPolicyResult,
    pub policy_reason: Option<String>,
    pub status: AgentActionStatus,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionListQuery {
    pub visible_to_user_id: Option<String>,
    pub actor_identity_id: Option<String>,
    pub scope_type: Option<String>,
    pub scope_id: Option<String>,
    pub status: Option<AgentActionStatus>,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAgentAction {
    pub id: String,
    pub expected_version: i64,
    pub policy_result: Option<AgentActionPolicyResult>,
    pub policy_reason: Option<Option<String>>,
    pub status: Option<AgentActionStatus>,
    pub outcome_json: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentActionApproval {
    pub id: String,
    pub action_id: String,
    pub expected_action_version: i64,
    pub approver_identity_id: String,
    pub decision: AgentActionApprovalDecision,
    pub reason: Option<String>,
    pub resulting_status: AgentActionStatus,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentActionExecution {
    pub id: String,
    pub action_id: String,
    pub expected_action_version: i64,
    pub attempt: i64,
    pub status: AgentActionExecutionStatus,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
    pub action_status: AgentActionStatus,
    pub action_outcome_json: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCommandReceipt {
    pub id: String,
    pub principal_type: String,
    pub principal_id: String,
    pub scope_type: String,
    pub scope_id: String,
    pub operation: String,
    pub idempotency_key: String,
    pub input_digest: String,
    pub policy_result: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub event_id: String,
    pub agent_action_execution_id: Option<String>,
    pub outcome_json: String,
    pub committed_at: String,
}

/// Main-agent sub-agent runs ("inquiries"). This is a run log the Main
/// session can inspect and cancel, not a work item -- no repo, no task
/// flow, no state machine, and no retry/assignment/dependency/milestone/
/// review concept. `id`/timestamps are generated by the store, matching the
/// narrow set of other mutation methods in this crate whose input carries
/// no timestamp field.
#[async_trait]
pub trait AgentInquiryRepo: Send + Sync {
    async fn create_agent_inquiry(&self, input: CreateAgentInquiry) -> Result<AgentInquiry>;
    async fn get_agent_inquiry(&self, id: &str) -> Result<Option<AgentInquiry>>;
    /// Keyset-paginated, newest first. Mirrors `ProjectHookRunRepo::list_for_project`'s
    /// cursor idiom: an opaque `(created_at, id)` cursor, decoded/encoded internally.
    async fn list_agent_inquiries(
        &self,
        chat_id: &str,
        limit: i64,
        cursor: Option<&str>,
    ) -> Result<Page<AgentInquiry>>;
    async fn complete_agent_inquiry(&self, input: CompleteAgentInquiry) -> Result<AgentInquiry>;
    /// Complete an inquiry and settle its provider-attempt ledger in one
    /// SQLite composite. Adapters that predate the typed ledger may retain
    /// their old completion behavior through this default.
    async fn complete_agent_inquiry_with_usage(
        &self,
        input: CompleteAgentInquiryWithUsage,
    ) -> Result<AgentInquiry> {
        self.complete_agent_inquiry(input.terminal).await
    }
    async fn cancel_agent_inquiry(&self, id: &str, expected_version: i64) -> Result<AgentInquiry>;
    /// Cancel an inquiry and atomically move any started invocation to
    /// pending-settlement so a late provider report can be drained safely.
    async fn cancel_agent_inquiry_with_usage(
        &self,
        input: CancelAgentInquiryWithUsage,
    ) -> Result<AgentInquiry> {
        self.cancel_agent_inquiry(&input.id, input.expected_version)
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAgentInquiry {
    pub id: String,
    pub chat_id: String,
    pub turn_job_id: Option<String>,
    pub identity_id: String,
    pub owner_user_id: String,
    pub title: String,
    pub question: String,
    pub workspace_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteAgentInquiry {
    pub id: String,
    pub expected_version: i64,
    pub status: AgentInquiryStatus,
    pub findings: Option<String>,
    pub findings_path: Option<String>,
    pub error: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteAgentInquiryWithUsage {
    pub terminal: CompleteAgentInquiry,
    pub settlements: Vec<UsageLedgerSettlement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CancelAgentInquiryWithUsage {
    pub id: String,
    pub expected_version: i64,
    pub settlements: Vec<UsageLedgerSettlement>,
}
