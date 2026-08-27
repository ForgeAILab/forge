use crate::{models::*, pagination::*, DbError, Result};
use async_trait::async_trait;
use sqlx::{Sqlite, Transaction};

#[async_trait]
pub trait TaskRepo: Send + Sync {
    async fn create(&self, input: CreateTask) -> Result<Task>;
    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateTask,
    ) -> Result<Task>;
    async fn get_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<Task>>;
    async fn list(&self, query: TaskListQuery) -> Result<Page<Task>>;
    async fn list_by_executing_agent(&self, query: AgentTaskListQuery) -> Result<Page<Task>>;
    async fn list_subtasks_ordered(&self, parent_task_id: &str) -> Result<Vec<Task>>;
    async fn next_subtask_order(&self, parent_task_id: &str) -> Result<i64>;
    async fn reorder_subtasks(
        &self,
        parent_task_id: &str,
        ordered_ids: &[String],
        updated_at: &str,
    ) -> Result<()>;
    async fn update(&self, input: UpdateTask) -> Result<Task>;
    async fn archive(&self, input: ArchiveTask) -> Result<Task>;
    async fn soft_delete(&self, input: SoftDeleteTask) -> Result<Task>;
    async fn set_review_passed_at(
        &self,
        id: &str,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task>;
    async fn set_metadata_json(
        &self,
        id: &str,
        metadata_json: Option<String>,
        updated_at: &str,
    ) -> Result<()>;
    async fn set_entry_barrier(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
    ) -> Result<Task>;
    async fn claim(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ClaimTask,
    ) -> Result<ClaimedTask>;
    async fn update_status(&self, input: UpdateTaskStatus) -> Result<Task>;
}

#[async_trait]
pub trait TaskBoardRepo: Send + Sync {
    async fn board_revision(&self, project_id: &str) -> Result<i64>;
    async fn replay_move_task(
        &self,
        operation_id: &str,
        identity: &MoveTaskIdentity,
    ) -> Result<Option<MoveTaskResult>>;
    async fn compare_and_move_task(&self, input: CompareAndMoveTask)
        -> Result<MoveTaskPersistence>;
    async fn complete_move_operation(
        &self,
        operation_id: &str,
        result: &MoveTaskResult,
        updated_at: &str,
    ) -> Result<()>;
}

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
    async fn count_active_tasks(&self, agent_id: &str) -> Result<i64>;
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
    async fn list_credential_handles(&self, owner_user_id: &str) -> Result<Vec<CredentialHandle>>;
    async fn rename_credential_handle(
        &self,
        id: &str,
        owner_user_id: &str,
        label: &str,
        expected_version: i64,
        updated_at: &str,
    ) -> Result<CredentialHandle>;
    /// Agents whose active profile references each of the owner's provider
    /// entries, with the identity's most recent session activity.
    async fn list_credential_usage(&self, owner_user_id: &str) -> Result<Vec<CredentialUsage>>;
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
    /// set without losing continuity — the LCM timeline is keyed by identity
    /// and scope, not by the runtime session id, so the next
    /// create-or-resume in the same scope resumes the same timeline.
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
pub struct AppendAgentLcmEntries {
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
    async fn claim_event_batch(&self, input: ClaimDomainEvents) -> Result<Vec<DomainEvent>>;
    async fn complete_claimed_event(&self, input: CompleteDomainEvent) -> Result<bool>;
}

/// Durable wake dispositions and their atomic event checkpoint boundary.
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
    /// Persist the first disposition and complete the claimed source event
    /// in one transaction.  A replay of the same attempt is exact and does
    /// not create a second row or advance the cursor twice.
    async fn complete_claimed_agent_wake(
        &self,
        input: CompleteClaimedWake,
    ) -> Result<AgentWakeDisposition>;
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
    async fn get_attention_consumer_health(
        &self,
        consumer_name: &str,
    ) -> Result<Option<AttentionConsumerHealth>>;
    async fn upsert_attention_consumer_health(
        &self,
        input: UpsertAttentionConsumerHealth,
    ) -> Result<AttentionConsumerHealth>;
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
    ) -> Self {
        let transition_log_id = transition_log_id.into();
        let task_id = task_id.into();
        let project_id = project_id.into();
        let from_state = bounded_event_text(from_state.as_ref(), 128);
        let to_state = bounded_event_text(to_state.as_ref(), 128);
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDomainEvents {
    pub consumer_name: String,
    pub lease_owner: String,
    pub now: String,
    pub leased_until: String,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteDomainEvent {
    pub consumer_name: String,
    pub lease_owner: String,
    pub event_sequence: i64,
    pub event_id: String,
    pub dedupe_key: String,
    pub completed_at: String,
}

#[async_trait]
pub trait WorkspaceRepo: Send + Sync {
    async fn create(&self, input: CreateWorkspace) -> Result<Workspace>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Workspace>>;
    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<Workspace>>;
    async fn set_cleanup_after(
        &self,
        id: &str,
        cleanup_after: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace>;
    async fn mark_cleaned(&self, id: &str, updated_at: &str) -> Result<Workspace>;
    async fn list_pending_cleanup(&self, now: &str) -> Result<Vec<Workspace>>;
    async fn update_status(
        &self,
        id: &str,
        status: WorkspaceStatus,
        error: Option<String>,
        updated_at: &str,
    ) -> Result<Workspace>;
    async fn delete(&self, id: &str) -> Result<()>;
}

/// Internal scheduler authority for a Task workspace.  A lease is deliberately
/// separate from the filesystem-backed `Workspace` row: chat agents never
/// receive this record, a path, or a bearer token.  The scheduler persists only
/// the opaque logical repository binding and a short-lived capability grant.
#[async_trait]
pub trait WorkspaceLeaseRepo: Send + Sync {
    async fn issue(&self, input: CreateWorkspaceLease) -> Result<WorkspaceLease>;
    async fn get_by_id(&self, id: &str) -> Result<Option<WorkspaceLease>>;
    async fn get_active_for_task(&self, task_id: &str) -> Result<Option<WorkspaceLease>>;
    async fn revoke(
        &self,
        id: &str,
        expected_version: i64,
        revoked_at: &str,
    ) -> Result<WorkspaceLease>;
    /// Extend scheduler-owned leases that are approaching expiry while their
    /// exact Task, execution, assignment, and governance bindings remain
    /// current. Invalid or concurrently changed candidates are skipped.
    async fn renew_active(
        &self,
        now: &str,
        renew_before: &str,
        expires_at: &str,
        limit: i64,
    ) -> Result<Vec<WorkspaceLease>>;
    async fn expire(&self, now: &str, limit: i64) -> Result<Vec<WorkspaceLease>>;
}

#[async_trait]
pub trait DaemonRepo: Send + Sync {
    async fn upsert_by_machine_id(&self, input: UpsertDaemon) -> Result<Daemon>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Daemon>>;
    async fn get_by_machine_id(&self, machine_id: &str) -> Result<Option<Daemon>>;
    async fn list(&self, page: PageRequest) -> Result<Page<Daemon>>;
    async fn list_visible(&self, user_id: Option<&str>, page: PageRequest) -> Result<Page<Daemon>>;
    async fn get_visible(&self, id: &str, user_id: Option<&str>) -> Result<Option<Daemon>>;
    async fn update_report(&self, input: UpdateDaemonReport) -> Result<Daemon>;
    async fn mark_online(&self, id: &str, last_report_at: &str) -> Result<Daemon>;
    async fn mark_offline(&self, id: &str, updated_at: &str) -> Result<Daemon>;
    async fn list_available_for_executor(&self, executor_type: &str) -> Result<Vec<Daemon>>;
}

#[async_trait]
pub trait RuntimeRepo: Send + Sync {
    async fn create(&self, input: CreateRuntime) -> Result<Runtime>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Runtime>>;
    async fn get_by_daemon_id(&self, daemon_id: &str) -> Result<Option<Runtime>>;
    async fn upsert_by_daemon_kind(&self, input: CreateRuntime) -> Result<Runtime>;
    async fn list(&self, query: RuntimeListQuery) -> Result<Page<Runtime>>;
}

#[async_trait]
pub trait ExecutionRepo: Send + Sync {
    async fn create(&self, input: CreateExecution) -> Result<Execution>;
    /// Atomically insert a running execution and establish its first owner
    /// lease.  Dispatchers should prefer this boundary so a process crash
    /// cannot leave a newly-created running row ownerless.
    async fn create_with_lease(
        &self,
        input: CreateExecution,
        lease: ClaimExecutionLease,
    ) -> Result<Execution>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Execution>>;
    async fn stats_by_agent(&self, agent_id: &str) -> Result<AgentExecutionStats>;
    async fn list_by_task(&self, task_id: &str, page: PageRequest) -> Result<Page<Execution>>;
    async fn list_latest_executions_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Execution>>;
    async fn list_by_task_and_role(
        &self,
        task_id: &str,
        role: &str,
        page: PageRequest,
    ) -> Result<Page<Execution>>;
    async fn count_by_task_and_role(&self, task_id: &str, role: &str) -> Result<i64>;
    /// Metadata-only patch.  `status: Some(_)` is rejected; all terminal
    /// transitions must use `terminalize` so the owner/version CAS also owns
    /// terminal event and Workspace-lease side effects.
    async fn update(&self, input: UpdateExecution) -> Result<Execution>;
    /// Claim or take over an execution lease through the execution-version
    /// compare-and-swap boundary.  A takeover is allowed only after the prior
    /// lease has expired; a first claim fixes the hard deadline.
    async fn claim_lease(&self, input: ClaimExecutionLease) -> Result<ExecutionLeaseMutation>;
    /// Renew an owner-bound lease without consulting streamed output.  The
    /// returned mutation is typed so a caller can distinguish a concurrent
    /// owner/version loss from a hard-deadline refusal.
    async fn renew_lease(&self, input: RenewExecutionLease) -> Result<ExecutionLeaseMutation>;
    /// Record semantic progress separately from the owner heartbeat.
    async fn record_progress(
        &self,
        input: RecordExecutionProgress,
    ) -> Result<ExecutionLeaseMutation>;
    /// Atomically publish one typed progress-warning event for a still-live
    /// owner.  A semantic progress update or terminal CAS that wins first
    /// makes this return `Concurrent` and prevents a stale Attention item.
    async fn record_progress_warning(
        &self,
        input: RecordExecutionProgressWarning,
    ) -> Result<ExecutionProgressWarningOutcome>;
    /// Complete, fail, cancel, or expire an execution using one terminal CAS.
    /// The winning transaction appends the terminal domain event and revokes
    /// or expires the matching active Workspace lease before commit.
    async fn terminalize(&self, input: TerminalizeExecution) -> Result<ExecutionTerminalOutcome>;
    /// Return running rows whose owner lease or hard deadline is no longer
    /// valid.  A live lease is never considered stalled merely because its
    /// semantic progress timestamp is old.
    async fn list_expired_leases(&self, now: &str, limit: i64) -> Result<Vec<Execution>>;
    /// Return executions whose owner lease is still live but semantic output
    /// is stale.  This powers a distinct progress-warning policy and must not
    /// be used as owner-death evidence.
    async fn list_stale_progress(
        &self,
        now: &str,
        stale_before: &str,
        limit: i64,
    ) -> Result<Vec<Execution>>;
    async fn list_running(&self) -> Result<Vec<Execution>>;
    async fn list_running_for_daemon_not_in(
        &self,
        daemon_id: &str,
        created_before: &str,
        exclude_ids: &[String],
    ) -> Result<Vec<Execution>>;
    async fn get_logs_path(&self, id: &str) -> Result<Option<String>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentExecutionStats {
    pub total_runs: i64,
    pub avg_duration_ms: Option<i64>,
    pub success_rate: Option<f64>,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_cost_usd: Option<f64>,
}

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
    async fn complete_agent_chat_control_transfer(
        &self,
        input: CompleteAgentChatControlTransfer,
    ) -> Result<AgentChatTurnJob>;
    async fn fail_agent_chat_turn(&self, input: FailAgentChatTurn) -> Result<AgentChatTurnJob>;
    async fn park_agent_chat_turn(&self, input: ParkAgentChatTurn) -> Result<AgentChatTurnJob>;
    /// Cancel a queued/leased/retry-wait turn and append the cancellation
    /// event in the same transaction.  The idempotency key is represented by
    /// the event dedupe key so retries do not require a second turn-job store.
    async fn cancel_agent_chat_turn(&self, input: CancelAgentChatTurn) -> Result<AgentChatTurnJob>;
    async fn admit_agent_handoff(&self, input: AdmitAgentHandoff) -> Result<AdmittedAgentHandoff>;
}

#[async_trait]
pub trait MemoryRepository: Send + Sync {
    async fn insert_memory_item(&self, item: &MemoryItem) -> std::result::Result<(), DbError>;
    async fn get_memory_item(&self, id: &str) -> std::result::Result<Option<MemoryItem>, DbError>;
    async fn memory_source_exists(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
    ) -> std::result::Result<bool, DbError>;
    async fn memory_source_exists_with_confidence(
        &self,
        project_id: &str,
        source_type: &str,
        source_ref: &str,
        confidence: &str,
    ) -> std::result::Result<bool, DbError>;
    async fn search_memory_items(
        &self,
        project_id: &str,
        query: &str,
        limit: usize,
        cursor: Option<String>,
    ) -> std::result::Result<(Vec<MemoryItem>, bool), DbError>;
    async fn list_memory_items_by_source(
        &self,
        project_id: &str,
        source_type: &str,
        source_id: &str,
    ) -> std::result::Result<Vec<MemoryItem>, DbError>;
}

/// ACL-first semantic-memory operations.  These methods are intentionally a
/// separate trait so legacy project-indexing code cannot accidentally omit the
/// authorization context when it is migrated to scoped retrieval.
#[async_trait]
pub trait ScopedMemoryRepository: Send + Sync {
    async fn insert_memory_item_if_source_absent(
        &self,
        item: &MemoryItem,
        source_type: &str,
        source_ref: &str,
    ) -> std::result::Result<(MemoryItem, bool), DbError>;
    async fn get_memory_item_scoped(
        &self,
        query: MemoryGetQuery,
    ) -> std::result::Result<Option<MemoryItem>, DbError>;
    async fn search_memory_items_scoped(
        &self,
        query: MemoryAccessQuery,
    ) -> std::result::Result<(Vec<MemoryItem>, bool), DbError>;
    async fn insert_memory_lifecycle_assertion(
        &self,
        input: CreateMemoryLifecycleAssertion,
    ) -> std::result::Result<MemoryLifecycleAssertion, DbError>;
    async fn list_memory_lifecycle_assertions(
        &self,
        memory_item_id: &str,
    ) -> std::result::Result<Vec<MemoryLifecycleAssertion>, DbError>;
    async fn create_memory_source_binding(
        &self,
        input: CreateForgeMemorySourceBinding,
    ) -> std::result::Result<ForgeMemorySourceBinding, DbError>;
    async fn get_memory_source_binding(
        &self,
        identity_id: &str,
        context_scope_id: &str,
    ) -> std::result::Result<Option<ForgeMemorySourceBinding>, DbError>;
    async fn create_context_manifest(
        &self,
        input: CreateContextManifest,
    ) -> std::result::Result<ContextManifest, DbError>;
    async fn append_context_manifest_source(
        &self,
        input: CreateContextManifestSource,
    ) -> std::result::Result<ContextManifestSource, DbError>;
    async fn get_context_manifest(
        &self,
        id: &str,
    ) -> std::result::Result<Option<ContextManifest>, DbError>;
    async fn get_context_manifest_scoped(
        &self,
        id: &str,
        identity_id: &str,
        context_scope_id: &str,
    ) -> std::result::Result<Option<ContextManifest>, DbError>;
    async fn list_context_manifests_scoped(
        &self,
        identity_id: &str,
        context_scope_id: Option<&str>,
        limit: i64,
    ) -> std::result::Result<Vec<ContextManifest>, DbError>;
    async fn list_context_manifest_sources(
        &self,
        manifest_id: &str,
    ) -> std::result::Result<Vec<ContextManifestSource>, DbError>;
}

#[async_trait]
pub trait ReviewRepo: Send + Sync {
    async fn create(&self, input: CreateReview) -> Result<Review>;
    async fn update_status(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
    ) -> Result<Review>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Review>>;
    async fn list_by_task(&self, task_id: &str) -> Result<Vec<Review>>;
    async fn list_latest_reviews_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Review>>;
    async fn next_attempt_number(&self, task_id: &str) -> Result<i64>;
}

#[async_trait]
pub trait TaskCommentRepo: Send + Sync {
    async fn create_comment(&self, input: CreateTaskComment) -> Result<TaskComment>;
    async fn list_comments(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskComment>>;
    async fn get_comment_by_id(&self, id: &str) -> Result<Option<TaskComment>>;
    async fn delete_comment(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait TaskMediaRepo: Send + Sync {
    async fn create_media(&self, input: CreateTaskMedia) -> Result<TaskMedia>;
    async fn list_media(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskMedia>>;
    async fn list_active_media_for_task(&self, task_id: &str) -> Result<Vec<TaskMedia>>;
    async fn get_media_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<TaskMedia>>;
    async fn soft_delete_media(&self, id: &str, deleted_at: &str) -> Result<TaskMedia>;
}

/// Repository boundary for the additive Project-owned media metadata layer.
///
/// Implementations must keep reference changes transactional.  In
/// particular, a cleanup worker may only claim an asset after checking active
/// attachments and release pins in the same write transaction.  Claims carry
/// a persisted owner/expiry lease and asset version; reset/finalize operations
/// must present both exact values.  Every attachment/pin insertion must reject
/// an asset already queued for deletion or marked as deleted.
#[async_trait]
pub trait SharedMediaRepo: Send + Sync {
    async fn get_media_asset(&self, asset_id: &str) -> Result<Option<MediaAsset>>;
    async fn get_media_asset_for_task_media(
        &self,
        task_media_id: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn begin_project_media_upload(
        &self,
        input: BeginProjectMediaUpload,
    ) -> Result<ProjectMediaUpload>;
    async fn list_pending_project_media_uploads(
        &self,
        limit: i64,
    ) -> Result<Vec<ProjectMediaUpload>>;
    async fn delete_pending_project_media_upload(
        &self,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<bool>;
    async fn create_project_media_asset(
        &self,
        input: CreateProjectMediaAsset,
    ) -> Result<MediaAsset>;
    async fn finalize_project_media_upload(
        &self,
        project_id: &str,
        asset_id: &str,
        now: &str,
    ) -> Result<MediaAsset>;
    /// Persist a checksum discovered from the unchanged on-disk bytes.  The
    /// compare-and-set is intentionally narrow so concurrent reconciliation
    /// cannot overwrite an existing digest.
    async fn set_media_asset_checksum(
        &self,
        asset_id: &str,
        expected_byte_size: i64,
        checksum: &str,
        now: &str,
    ) -> Result<MediaAsset>;
    /// Return purged assets whose bytes still need idempotent physical
    /// cleanup after a crash between the tombstone transaction and unlink.
    async fn list_purged_media_assets(&self, limit: i64) -> Result<Vec<MediaAsset>>;
    async fn mark_purged_media_asset_reconciled(
        &self,
        asset_id: &str,
        reconciled_at: &str,
    ) -> Result<()>;
    /// Resolve an already committed Project media tombstone without applying
    /// a new mutation.  API routes use this before validating the current
    /// authorization so an immutable receipt can be replayed exactly; a
    /// changed receipt returns `IdempotencyConflict`.
    async fn replay_project_media_tombstone(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<Option<MediaAsset>>;
    async fn tombstone_project_media_asset(
        &self,
        input: ProjectMediaTombstone,
    ) -> Result<MediaAsset>;
    async fn create_project_media_attachment_mutation(
        &self,
        input: CreateProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment>;
    async fn soft_delete_project_media_attachment_mutation(
        &self,
        input: SoftDeleteProjectMediaAttachmentMutation,
    ) -> Result<ProjectMediaAttachment>;
    async fn create_project_media_attachment(
        &self,
        input: CreateProjectMediaAttachment,
    ) -> Result<ProjectMediaAttachment>;
    async fn soft_delete_project_media_attachment(
        &self,
        id: &str,
        deleted_at: &str,
    ) -> Result<ProjectMediaAttachment>;
    async fn create_project_release_media_pin(
        &self,
        input: CreateProjectReleaseMediaPin,
    ) -> Result<ProjectReleaseMediaPin>;
    async fn list_project_release_media_pins(
        &self,
        release_id: &str,
    ) -> Result<Vec<ProjectReleaseMediaPin>>;
    async fn reconcile_media_asset(&self, asset_id: &str, now: &str) -> Result<Option<MediaAsset>>;
    async fn claim_media_gc_candidates(
        &self,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
        limit: i64,
    ) -> Result<Vec<MediaAsset>>;
    async fn claim_media_gc_candidate(
        &self,
        asset_id: &str,
        now: &str,
        lease_owner: &str,
        lease_expires_at: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn reset_media_gc_candidate(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        now: &str,
    ) -> Result<Option<MediaAsset>>;
    async fn complete_media_gc(
        &self,
        asset_id: &str,
        lease_owner: &str,
        expected_version: i64,
        deleted_at: &str,
    ) -> Result<Option<MediaAsset>>;
}

#[async_trait]
pub trait TerminalSessionRepo: Send + Sync {
    async fn create_terminal_session(
        &self,
        input: CreateTerminalSession,
    ) -> Result<TerminalSession>;
    async fn get_terminal_session(&self, id: &str) -> Result<Option<TerminalSession>>;
    async fn list_terminal_sessions_for_task(
        &self,
        task_id: &str,
        include_ended: bool,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_running_terminal_sessions_for_workspace(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<TerminalSession>>;
    async fn list_all_running_terminal_sessions(&self) -> Result<Vec<TerminalSession>>;
    async fn update_terminal_session_status(
        &self,
        id: &str,
        expected_version: i64,
        update: UpdateTerminalSessionStatus,
    ) -> Result<TerminalSession>;
    async fn update_terminal_session_size(
        &self,
        id: &str,
        rows: i64,
        cols: i64,
        last_activity_at: &str,
    ) -> Result<TerminalSession>;
    async fn touch_terminal_session_activity(&self, id: &str, last_activity_at: &str)
        -> Result<()>;
    async fn delete_terminal_sessions_for_workspace(&self, workspace_id: &str) -> Result<u64>;
}

#[async_trait]
pub trait ProjectRepo: Send + Sync {
    async fn create(&self, input: CreateProject) -> Result<Project>;
    /// Create a Project together with its singular Agent Chat and binding.
    /// Passing no identity/profile preserves explicit setup-required state;
    /// passing both values makes the binding active in the same transaction.
    async fn create_with_agent_binding(
        &self,
        input: CreateProject,
        identity_id: Option<String>,
        profile_id: Option<String>,
    ) -> Result<Project>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Project>>;
    async fn list(&self, page: PageRequest) -> Result<Page<Project>>;
    async fn update(&self, input: UpdateProject) -> Result<Project>;
    async fn update_at_version(
        &self,
        input: UpdateProject,
        expected_version: i64,
        project_hooks_json: Option<String>,
    ) -> Result<Project>;
    async fn set_project_hooks_json(
        &self,
        id: &str,
        project_hooks_json: &str,
        updated_at: &str,
    ) -> Result<()>;
    async fn increment_project_work_epoch(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        by: i64,
    ) -> Result<i64>;
    async fn set_paused_at(&self, id: &str, paused_at: Option<String>) -> Result<()>;
    async fn delete(&self, id: &str) -> Result<()>;
}

/// Atomic Project execution-setup mutation boundary. The command receipt is
/// committed in the same SQLite transaction as the Project CAS update, so a
/// response-loss replay cannot apply the setup mutation twice.
#[async_trait]
pub trait ProjectExecutionSetupCommandRepo: Send + Sync {
    async fn apply_project_execution_setup_command(
        &self,
        input: ApplyProjectExecutionSetupCommand,
    ) -> Result<AppliedProjectExecutionSetupCommand>;
}

/// Durable reconciliation state for the cross-filesystem Project setup
/// operation.  These methods are intentionally separate from `ProjectRepo`:
/// setup readiness is not a Project version or a chat-binding lifecycle.
#[async_trait]
pub trait ProjectProvisioningRepo: Send + Sync {
    async fn get_provisioning_operation(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>>;
    async fn get_provisioning_operation_by_id(
        &self,
        id: &str,
    ) -> Result<Option<ProjectProvisioningOperation>>;
    async fn create_provisioning_operation(
        &self,
        input: CreateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation>;
    async fn update_provisioning_operation(
        &self,
        input: UpdateProjectProvisioningOperation,
    ) -> Result<ProjectProvisioningOperation>;
    async fn list_provisioning_checkpoints(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningCheckpoint>>;
    async fn get_provisioning_checkpoint(
        &self,
        operation_id: &str,
        checkpoint: &str,
    ) -> Result<Option<ProjectProvisioningCheckpoint>>;
    async fn upsert_provisioning_checkpoint(
        &self,
        input: UpsertProjectProvisioningCheckpoint,
    ) -> Result<ProjectProvisioningCheckpoint>;
    async fn list_provisioning_errors(
        &self,
        operation_id: &str,
    ) -> Result<Vec<ProjectProvisioningError>>;
    async fn record_provisioning_error(
        &self,
        input: CreateProjectProvisioningError,
    ) -> Result<ProjectProvisioningError>;
}

#[async_trait]
pub trait ProjectHookRunRepo: Send + Sync {
    async fn try_claim(&self, input: CreateProjectHookRun) -> Result<Option<ProjectHookRun>>;
    async fn try_claim_or_skip_at_limit(
        &self,
        input: CreateProjectHookRun,
        max_active_runs: i64,
        skip_reason: &str,
    ) -> Result<Option<ProjectHookRun>>;
    async fn update_status(&self, input: UpdateProjectHookRun) -> Result<ProjectHookRun>;
    async fn list_recent_for_project(
        &self,
        project_id: &str,
        limit: i64,
    ) -> Result<Vec<ProjectHookRun>>;
    async fn list_for_project(
        &self,
        project_id: &str,
        page: PageRequest,
    ) -> Result<Page<ProjectHookRun>>;
    async fn count_active_for_rule(&self, project_id: &str, rule_id: &str) -> Result<i64>;
}

#[async_trait]
pub trait RepoRepo: Send + Sync {
    async fn create(&self, input: CreateRepo) -> Result<Repo>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Repo>>;
    async fn list_by_project(&self, project_id: &str, page: PageRequest) -> Result<Page<Repo>>;
    async fn update(&self, input: UpdateRepo) -> Result<Repo>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait PrProviderConfigRepo: Send + Sync {
    async fn create(&self, input: CreatePrProviderConfig) -> Result<PrProviderConfig>;
    async fn get_by_repo_id(&self, repo_id: &str) -> Result<Option<PrProviderConfig>>;
    async fn update(&self, input: UpdatePrProviderConfig) -> Result<PrProviderConfig>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait PrMetadataRepo: Send + Sync {
    async fn create(&self, input: CreatePrMetadata) -> Result<PrMetadata>;
    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<PrMetadata>>;
    async fn update(&self, input: UpdatePrMetadata) -> Result<PrMetadata>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait IntegrationRepo: Send + Sync {
    async fn create_integration(
        &self,
        input: CreateProjectIntegration,
    ) -> Result<ProjectIntegration>;
    async fn get_by_id(&self, id: &str) -> Result<Option<ProjectIntegration>>;
    async fn get_by_project_id(&self, project_id: &str) -> Result<Option<ProjectIntegration>>;
    async fn update_integration(
        &self,
        input: UpdateProjectIntegration,
    ) -> Result<ProjectIntegration>;
    async fn update_last_polled_at(
        &self,
        id: &str,
        last_polled_at: &str,
        updated_at: &str,
    ) -> Result<()>;
    async fn list_enabled(&self) -> Result<Vec<ProjectIntegration>>;
    async fn delete_integration(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ExternalLinkRepo: Send + Sync {
    async fn create_link(&self, input: CreateTaskExternalLink) -> Result<TaskExternalLink>;
    async fn get_by_id(&self, id: &str) -> Result<Option<TaskExternalLink>>;
    async fn get_by_global_id(&self, global_id: &str) -> Result<Option<TaskExternalLink>>;
    async fn get_by_task_id(&self, task_id: &str) -> Result<Option<TaskExternalLink>>;
    async fn list_by_task_id(&self, task_id: &str) -> Result<Vec<TaskExternalLink>>;
    async fn list_by_integration(&self, integration_id: &str) -> Result<Vec<TaskExternalLink>>;
    async fn delete_link(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait SkillRepo: Send + Sync {
    async fn create(&self, input: CreateSkill) -> Result<Skill>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Skill>>;
    async fn list_by_project(&self, project_id: &str, page: PageRequest) -> Result<Page<Skill>>;
    async fn update(&self, input: UpdateSkill) -> Result<Skill>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait NotificationRepo: Send + Sync {
    async fn create(&self, input: CreateNotification) -> Result<Notification>;
    async fn list(&self, query: NotificationListQuery) -> Result<Page<Notification>>;
    async fn unread_count(&self, project_id: Option<&str>) -> Result<i64>;
    async fn mark_read(&self, id: &str) -> Result<Notification>;
    async fn mark_all_read(&self, project_id: Option<&str>) -> Result<u64>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait TaskDependencyRepo: Send + Sync {
    async fn add_dependency(&self, task_id: &str, depends_on_id: &str, now: &str) -> Result<()>;
    async fn remove_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()>;
    async fn list_dependencies(&self, task_id: &str) -> Result<Vec<String>>;
    async fn list_dependents(&self, depends_on_id: &str) -> Result<Vec<String>>;
    async fn unsatisfied_dependencies(&self, task_id: &str) -> Result<Vec<String>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListQuery {
    pub project_id: String,
    pub q: Option<String>,
    pub statuses: Vec<String>,
    pub agent_ids: Vec<String>,
    pub assignee_types: Vec<String>,
    pub assignee_ids: Vec<String>,
    pub priority: Option<i64>,
    pub include_archived: bool,
    pub include_cancelled: bool,
    pub include_deleted: bool,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTaskListQuery {
    pub agent_id: String,
    pub include_archived: bool,
    pub include_cancelled: bool,
    pub include_deleted: bool,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentListQuery {
    pub status: Option<AgentStatus>,
    pub executor_type: Option<String>,
    pub capabilities: Vec<String>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationListQuery {
    pub project_id: Option<String>,
    pub read: Option<bool>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProject {
    pub id: String,
    pub name: String,
    pub settings: String,
    pub workflow_definition: String,
    pub primary_repo_id: Option<String>,
    pub owner_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateProject {
    pub id: String,
    pub name: Option<String>,
    pub settings: Option<String>,
    pub primary_repo_id: Option<Option<String>>,
    pub paused_at: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRepo {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub remote_url: String,
    pub local_path: Option<String>,
    pub work_mode: WorkMode,
    pub default_branch: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRepo {
    pub id: String,
    pub name: Option<String>,
    pub local_path: Option<Option<String>>,
    pub remote_url: Option<String>,
    pub work_mode: Option<WorkMode>,
    pub default_branch: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePrProviderConfig {
    pub id: String,
    pub repo_id: String,
    pub provider_type: String,
    pub base_url: Option<String>,
    pub polling_interval_seconds: i64,
    pub token_secret_ref: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePrProviderConfig {
    pub id: String,
    pub provider_type: Option<String>,
    pub base_url: Option<Option<String>>,
    pub polling_interval_seconds: Option<i64>,
    pub token_secret_ref: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePrMetadata {
    pub id: String,
    pub task_id: String,
    pub provider_type: String,
    pub provider_pr_id: Option<String>,
    pub pr_url: Option<String>,
    pub source_branch: String,
    pub target_branch: String,
    pub pr_state: String,
    pub merge_status: String,
    pub last_synced_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatePrMetadata {
    pub id: String,
    pub provider_type: Option<String>,
    pub provider_pr_id: Option<Option<String>>,
    pub pr_url: Option<Option<String>>,
    pub source_branch: Option<String>,
    pub target_branch: Option<String>,
    pub pr_state: Option<String>,
    pub merge_status: Option<String>,
    pub last_synced_at: Option<Option<String>>,
    pub updated_at: String,
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
pub struct CreateWorkspace {
    pub id: String,
    pub task_id: String,
    pub repo_id: String,
    pub worktree_path: String,
    pub branch: String,
    pub status: WorkspaceStatus,
    pub before_sha: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateWorkspaceLease {
    pub id: String,
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub execution_id: String,
    pub operation_idempotency_key: String,
    pub repository_binding_id: String,
    pub base_ref: String,
    pub role: String,
    pub capabilities_json: String,
    pub assigned_principal_type: String,
    pub assigned_principal_id: String,
    pub capability_profile_revision: String,
    pub capability_profile_digest: String,
    pub issuing_principal_type: String,
    pub issuing_principal_id: String,
    pub issued_at: String,
    pub expires_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertDaemon {
    pub id: String,
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels_json: String,
    pub status: DaemonStatus,
    pub registration_token_hash: Option<String>,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateDaemonReport {
    pub id: String,
    pub last_report_at: String,
    pub status: DaemonStatus,
    pub detected_clis_json: String,
    pub labels_json: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRuntime {
    pub id: String,
    pub daemon_id: String,
    pub kind: String,
    pub workspace_root: String,
    pub status: RuntimeStatus,
    pub labels_json: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeListQuery {
    pub daemon_id: Option<String>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSkill {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSkill {
    pub id: String,
    pub name: Option<String>,
    pub content: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTask {
    pub id: String,
    pub project_id: String,
    pub repo_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub task_type: String,
    pub status: String,
    pub is_automation: bool,
    pub priority: i64,
    pub subtask_order: Option<i64>,
    pub task_state_config: Option<String>,
    pub merge_config: Option<String>,
    pub plan: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTask {
    pub id: String,
    pub expected_version: i64,
    pub title: Option<String>,
    pub description: Option<Option<String>>,
    pub priority: Option<i64>,
    pub merge_config: Option<Option<String>>,
    pub plan: Option<Option<String>>,
    pub error_annotation: Option<Option<String>>,
    pub blocked_json: Option<Option<String>>,
    pub failed_json: Option<Option<String>>,
    pub task_state_config: Option<Option<String>>,
    pub parent_task_id: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftDeleteTask {
    pub id: String,
    pub expected_version: i64,
    pub deleted_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveTask {
    pub id: String,
    pub expected_version: i64,
    pub archived_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimTask {
    pub task_id: String,
    pub assignee_type: String,
    pub assignee_id: Option<String>,
    pub expected_version: i64,
    pub source_status: String,
    pub target_status: String,
    pub capacity_statuses: Vec<String>,
    pub execution: CreateExecution,
    /// The initial owner lease is part of the claim transaction.  A running
    /// execution may never be inserted first and claimed later, since that
    /// would leave an unrecoverable ownerless window.
    pub execution_lease: ClaimExecutionLease,
    pub max_concurrent_tasks: i64,
    pub claimed_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedTask {
    pub task: Task,
    pub execution: Execution,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateTaskStatus {
    pub id: String,
    pub expected_version: i64,
    pub status: String,
    pub assignee_id: Option<Option<String>>,
    pub error_annotation: Option<Option<String>>,
    pub blocked_json: Option<Option<String>>,
    pub failed_json: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExecution {
    pub id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub role: String,
    pub status: ExecutionStatus,
    pub stop_reason: Option<StopReason>,
    pub stopped_by: Option<String>,
    pub resume_policy: Option<ResumePolicy>,
    pub stopped_at: Option<String>,
    pub parent_execution_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub agent_message_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub summary: Option<String>,
    pub logs_path: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub error: Option<String>,
    pub executor_config_snapshot_json: Option<String>,
    pub workspace_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateExecution {
    pub id: String,
    /// Must remain `None`; terminal status belongs to the CAS API.
    pub status: Option<ExecutionStatus>,
    pub stop_reason: Option<Option<StopReason>>,
    pub stopped_by: Option<Option<String>>,
    pub resume_policy: Option<Option<ResumePolicy>>,
    pub stopped_at: Option<Option<String>>,
    pub agent_session_id: Option<Option<String>>,
    pub agent_message_id: Option<Option<String>>,
    pub last_activity_at: Option<Option<String>>,
    pub summary: Option<Option<String>>,
    pub logs_path: Option<Option<String>>,
    pub before_sha: Option<Option<String>>,
    pub after_sha: Option<Option<String>>,
    pub error: Option<Option<String>>,
    pub executor_config_snapshot_json: Option<Option<String>>,
    pub updated_at: String,
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
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceProjectAgentBinding {
    pub project_id: String,
    pub expected_version: i64,
    pub replacement: CreateProjectAgentBinding,
    pub replacement_reason: Option<String>,
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
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkAgentChatTurn {
    pub turn_job_id: String,
    pub expected_version: i64,
    pub lease_owner: String,
    pub pending_interaction_id: String,
    pub updated_at: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateReview {
    pub id: String,
    pub task_id: String,
    pub execution_id: String,
    pub attempt_number: i64,
    pub status: ReviewStatus,
    pub step_results_json: String,
    pub started_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskComment {
    pub id: String,
    pub task_id: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[async_trait]
pub trait TaskRoleAssignmentRepo: Send + Sync {
    async fn assign(
        &self,
        input: CreateTaskRoleAssignment,
    ) -> std::result::Result<TaskRoleAssignment, crate::DbError>;
    async fn get_by_task_and_role(
        &self,
        task_id: &str,
        role_name: &str,
    ) -> std::result::Result<Option<TaskRoleAssignment>, crate::DbError>;
    async fn list_by_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<Vec<TaskRoleAssignment>, crate::DbError>;
    async fn remove(
        &self,
        task_id: &str,
        role_name: &str,
    ) -> std::result::Result<(), crate::DbError>;
}

#[async_trait]
pub trait TransitionLogRepo: Send + Sync {
    async fn insert(
        &self,
        input: CreateTransitionLog,
    ) -> std::result::Result<TransitionLog, crate::DbError>;
    async fn insert_recovery_marker(
        &self,
        task_id: &str,
        current_state: &str,
        action_kind: &str,
        triggered_by: &str,
        reason: &str,
    ) -> std::result::Result<TransitionLog, crate::DbError>;
    async fn list_by_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<Vec<TransitionLog>, crate::DbError>;
    async fn count_gate_rejections(
        &self,
        task_id: &str,
        gate_state: &str,
    ) -> std::result::Result<i64, crate::DbError>;
    async fn count_to_state_since(
        &self,
        task_id: &str,
        to_state: &str,
        since: Option<&str>,
    ) -> std::result::Result<i64, crate::DbError>;
    async fn update_hook_results(
        &self,
        id: &str,
        hook_results_json: &str,
    ) -> std::result::Result<(), crate::DbError>;
}

#[derive(Debug, Clone)]
pub struct UpsertExecutionUsage {
    pub execution_id: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskUsageSummary {
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_cost_usd: Option<f64>,
    pub execution_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CiStepStats {
    pub command: String,
    pub total_runs: i64,
    pub pass_count: i64,
    pub fail_count: i64,
    pub avg_duration_ms: Option<i64>,
    pub p50_duration_ms: Option<i64>,
    pub p95_duration_ms: Option<i64>,
    pub last_run_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelTokenBreakdown {
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub execution_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentTokenBreakdown {
    pub agent_id: String,
    pub agent_name: String,
    pub executor_type: String,
    pub model: Option<String>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub execution_count: i64,
    pub success_rate: Option<f64>,
    pub avg_duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectTokenStats {
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_cost_usd: Option<f64>,
    pub execution_count: i64,
    pub by_model: Vec<ModelTokenBreakdown>,
    pub by_agent: Vec<AgentTokenBreakdown>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectReviewSummary {
    pub total_reviews: i64,
    pub passed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub avg_duration_ms: Option<i64>,
    pub pass_rate: f64,
}

#[async_trait]
pub trait ProjectAnalyticsRepo: Send + Sync {
    async fn get_project_ci_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<CiStepStats>>;
    async fn get_project_token_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectTokenStats>;
    async fn get_project_review_summary(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectReviewSummary>;
}

#[async_trait]
pub trait ExecutionUsageRepo: Send + Sync {
    async fn upsert(&self, input: UpsertExecutionUsage) -> Result<ExecutionUsage>;
    async fn list_by_execution(&self, execution_id: &str) -> Result<Vec<ExecutionUsage>>;
    async fn get_task_usage_summary(&self, task_id: &str) -> Result<TaskUsageSummary>;
}

#[async_trait]
pub trait UserRepo: Send + Sync {
    async fn create_user(&self, user: &User) -> Result<()>;
    async fn get_user_by_id(&self, id: &str) -> Result<Option<User>>;
    async fn get_user_by_email(&self, email: &str) -> Result<Option<User>>;
    async fn search_users(&self, query: &str, limit: i64) -> Result<Vec<User>>;
    async fn list_users(&self, page: PageRequest) -> Result<Page<User>>;
    async fn set_admin(&self, id: &str, is_admin: bool) -> Result<()>;
    async fn update_profile(
        &self,
        id: &str,
        email: &str,
        display_name: Option<&str>,
        updated_at: &str,
    ) -> Result<()>;
    async fn count_admins(&self) -> Result<i64>;
    async fn delete_user(&self, id: &str) -> Result<bool>;
}

#[async_trait]
pub trait RefreshTokenRepo: Send + Sync {
    async fn create_refresh_token(&self, token: &RefreshToken) -> Result<()>;
    async fn delete_refresh_token_by_hash(&self, token_hash: &str) -> Result<Option<RefreshToken>>;
    async fn delete_refresh_tokens_by_family(&self, family_id: &str) -> Result<u64>;
    async fn delete_expired_refresh_tokens(&self) -> Result<u64>;
    async fn get_refresh_tokens_by_user(&self, user_id: &str) -> Result<Vec<RefreshToken>>;
}

#[async_trait]
pub trait PersonalAccessTokenRepo: Send + Sync {
    async fn create_pat(&self, input: CreatePersonalAccessToken) -> Result<PersonalAccessToken>;
    async fn get_pat_by_token_hash(&self, token_hash: &str) -> Result<Option<PersonalAccessToken>>;
    async fn list_pats_by_user(&self, user_id: &str) -> Result<Vec<PersonalAccessToken>>;
    async fn delete_pat(&self, id: &str, user_id: &str) -> Result<()>;
    async fn update_last_used(&self, id: &str, last_used_at: &str) -> Result<()>;
}

#[async_trait]
pub trait OAuthClientRepo: Send + Sync {
    async fn create_client(&self, input: CreateOAuthClient) -> Result<OAuthClient>;
    async fn get_client(&self, client_id: &str) -> Result<Option<OAuthClient>>;
    async fn touch_last_used(&self, client_id: &str, last_used_at: &str) -> Result<()>;
    async fn count_clients_created_since(&self, created_after_rfc3339: &str) -> Result<i64>;
}

#[async_trait]
pub trait OAuthAuthorizationCodeRepo: Send + Sync {
    async fn create_code(
        &self,
        input: CreateOAuthAuthorizationCode,
    ) -> Result<OAuthAuthorizationCode>;
    /// Returns the code row regardless of consumed_at / expires_at; service enforces semantics.
    async fn get_code_by_hash(&self, code_hash: &str) -> Result<Option<OAuthAuthorizationCode>>;
    /// Atomic single-use claim: UPDATE ... WHERE id = ? AND consumed_at IS NULL. Returns true if this caller won.
    async fn mark_code_consumed(&self, id: &str, consumed_at: &str) -> Result<bool>;
    async fn delete_expired_codes(&self, now_rfc3339: &str) -> Result<u64>;
}

#[async_trait]
pub trait OAuthRefreshTokenRepo: Send + Sync {
    async fn create_refresh_token(
        &self,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken>;
    async fn create_refresh_token_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken>;
    async fn get_refresh_token_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<OAuthRefreshToken>>;
    /// Atomic single-use claim for rotation. Returns true if this caller revoked the active row.
    async fn claim_refresh_token_for_rotation(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
        revoked_at: &str,
    ) -> Result<bool>;
    /// Set revoked_at on a single row by id. Idempotent.
    async fn revoke_refresh_token(&self, id: &str, revoked_at: &str) -> Result<()>;
    /// Set revoked_at on every non-revoked row in the family. Returns count of newly-revoked rows.
    async fn revoke_refresh_token_family(&self, family_id: &str, revoked_at: &str) -> Result<u64>;
    async fn revoke_refresh_token_family_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        family_id: &str,
        revoked_at: &str,
    ) -> Result<u64>;
    async fn delete_expired_refresh_tokens(&self, now_rfc3339: &str) -> Result<u64>;
}

#[async_trait]
pub trait ProjectMemberRepo: Send + Sync {
    async fn add_member(&self, input: CreateProjectMember) -> Result<ProjectMember>;
    async fn get_member(&self, project_id: &str, user_id: &str) -> Result<Option<ProjectMember>>;
    async fn list_members(&self, project_id: &str) -> Result<Vec<ProjectMember>>;
    async fn update_member_role(
        &self,
        project_id: &str,
        user_id: &str,
        role: &str,
        updated_at: &str,
    ) -> Result<ProjectMember>;
    async fn remove_member(&self, project_id: &str, user_id: &str) -> Result<()>;
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

#[async_trait]
pub trait SystemSettingRepo: Send + Sync {
    async fn get_setting(&self, key: &str) -> Result<Option<String>>;
    async fn set_setting(&self, key: &str, value: &str, updated_at: &str) -> Result<()>;
    async fn list_settings(&self) -> Result<Vec<(String, String)>>;
    async fn delete_setting(&self, key: &str) -> Result<()>;
}
