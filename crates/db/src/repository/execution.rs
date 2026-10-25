//! Execution repositories, admission, and update requests.

use super::*;

#[derive(Debug, Clone)]
pub struct TaskExecutionProjectionQuery {
    pub task_id: String,
    pub current_role: Option<String>,
    pub blocked_execution_id: Option<String>,
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
    /// Atomically insert a running execution after re-checking the Task
    /// snapshot that the dispatcher used to choose its role.  The admission
    /// snapshot is optional for lower-level callers that do not have a
    /// versioned Task decision; service dispatch paths should always supply it.
    async fn create_with_lease_and_admission(
        &self,
        input: CreateExecution,
        lease: ClaimExecutionLease,
        admission: Option<ExecutionAdmission>,
    ) -> Result<Execution>;
    /// Share the caller's writer transaction with placement start fencing.
    async fn create_with_lease_and_admission_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateExecution,
        lease: ClaimExecutionLease,
        admission: Option<ExecutionAdmission>,
    ) -> Result<Execution>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Execution>>;
    async fn get_by_ids(&self, ids: &[&str]) -> Result<Vec<Execution>>;
    async fn stats_by_agent(&self, agent_id: &str) -> Result<AgentExecutionStats>;
    async fn list_by_task(&self, task_id: &str, page: PageRequest) -> Result<Page<Execution>>;
    /// Return the running executions for one Task with one SQL predicate.
    /// Unlike offset pagination this is safe for admission/stop checks: a
    /// concurrent insert cannot shift an older running row to another page.
    async fn list_running_by_task(&self, task_id: &str) -> Result<Vec<Execution>>;
    async fn list_latest_executions_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Execution>>;
    /// Latest history, running occupancy, and bounded action authority for a page.
    async fn list_task_projection_executions(
        &self,
        tasks: &[TaskExecutionProjectionQuery],
    ) -> Result<Vec<Execution>>;
    async fn list_by_task_and_role(
        &self,
        task_id: &str,
        role: &str,
        page: PageRequest,
    ) -> Result<Page<Execution>>;
    /// Return the newest terminal execution for one Task/role pair. The
    /// terminal predicate belongs in SQL so a newer running row does not make
    /// a bounded first page hide an older resumable row.
    async fn latest_non_running_by_task_and_role(
        &self,
        task_id: &str,
        role: &str,
    ) -> Result<Option<Execution>>;
    /// Return the newest terminal execution that still carries a resumable
    /// agent session for one Task/role pair.
    async fn latest_resumable_by_task_and_role(
        &self,
        task_id: &str,
        role: &str,
    ) -> Result<Option<Execution>>;
    /// Return the newest execution with an assigned agent identity for one
    /// Task. Used only as a fallback when no role assignment identifies an
    /// agent.
    async fn latest_agent_execution_by_task(&self, task_id: &str) -> Result<Option<Execution>>;
    /// Return the newest execution whose role is one of `roles`, with the
    /// role predicate evaluated in SQL rather than after a capped page.
    async fn latest_execution_by_task_and_roles(
        &self,
        task_id: &str,
        roles: &[&str],
    ) -> Result<Option<Execution>>;
    /// Return the newest implementation execution still relevant to review
    /// admission. Failed/cancelled attempts are terminal remediation records,
    /// not review candidates; a newer running row remains visible so callers
    /// can fail closed until it settles.
    async fn latest_review_candidate_by_task_and_roles(
        &self,
        task_id: &str,
        roles: &[&str],
    ) -> Result<Option<Execution>>;
    /// Check whether one agent has ever supplied the requested role context
    /// for a dependency Task. This exact EXISTS query is used by the
    /// dependency gate and must not be approximated by a capped history page.
    async fn has_execution_by_task_role_and_agent(
        &self,
        task_id: &str,
        role: &str,
        agent_id: &str,
    ) -> Result<bool>;
    /// Find the first execution for a role created at or after a review
    /// attempt's start boundary.
    async fn first_by_task_and_role_at_or_after(
        &self,
        task_id: &str,
        role: &str,
        started_at: &str,
    ) -> Result<Option<Execution>>;
    /// Count and test in-flight automatic recovery executions without loading
    /// the complete execution history into the service process.
    async fn has_running_by_task_and_purpose(
        &self,
        task_id: &str,
        purpose: api_types::ExecutionPurpose,
    ) -> Result<bool>;
    /// Duplicate-follow-up detection is an exact parent/status lookup, not a
    /// first-page history scan.
    async fn has_active_or_completed_child(
        &self,
        task_id: &str,
        parent_execution_id: &str,
        role: &str,
    ) -> Result<bool>;
    async fn count_by_task_and_role(&self, task_id: &str, role: &str) -> Result<i64>;
    /// Metadata-only patch.  `status: Some(_)` is rejected; all terminal
    /// transitions must use `terminalize` so the owner/version CAS also owns
    /// terminal event and Workspace-lease side effects.
    async fn update(&self, input: UpdateExecution) -> Result<Execution>;
    /// Claim or take over an execution lease through the execution-version
    /// compare-and-swap boundary.  A takeover is allowed only after the prior
    /// lease has expired. A first claim fixes an explicitly configured hard
    /// deadline; without one, the renewable owner lease is the only clock.
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
    /// Task-execution terminalization with the immutable usage ledger and,
    /// for remote delivery, an optional durable terminal-report receipt. The
    /// execution CAS, workspace lease closure, invocation settlement/events,
    /// terminal domain event, and receipt all commit in one transaction.
    async fn terminalize_with_ledger(
        &self,
        input: TerminalizeExecutionWithLedger,
    ) -> Result<ExecutionTerminalOutcome>;
    /// Task-execution terminalization with remote invocation rows supplied by
    /// the service.  The rows are inserted and started inside the same
    /// BEGIN IMMEDIATE transaction as the terminal CAS, workspace closure,
    /// settlement/events, and durable terminal receipt.  Existing callers use
    /// `terminalize_with_ledger` when no remote materialization is needed.
    async fn terminalize_with_ledger_and_invocations(
        &self,
        input: TerminalizeExecutionWithLedger,
        invocations: Vec<CreateUsageInvocation>,
    ) -> Result<ExecutionTerminalOutcome>;
    /// Look up the immutable receipt for a remote terminal report. The
    /// terminalization boundary compares the digest; this accessor lets
    /// reconnect/recovery code inspect the durable receipt directly.
    async fn get_execution_terminal_receipt(
        &self,
        terminal_report_id: &str,
    ) -> Result<Option<ExecutionTerminalReceipt>>;
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
    /// Return only running executions whose Task belongs to one Project.
    /// Project-scoped administrative operations must not enumerate unrelated
    /// execution rows just to filter them in memory.
    async fn list_running_for_project(&self, project_id: &str) -> Result<Vec<Execution>>;
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
    pub avg_duration_ms: Option<i64>,
    pub success_rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

/// The Task facts used to choose a role execution.  These are checked again
/// in the same SQLite transaction as the execution INSERT so a transition
/// between the dispatcher's final read and admission fails closed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionAdmission {
    #[serde(default)]
    pub purpose: Option<api_types::ExecutionPurpose>,
    /// Only the replay owning this exact intent may consume it at admission.
    pub expected_queued_recovery_id: Option<String>,
    /// Project revision selected with the Task/workflow snapshot. Project
    /// settings, repository, pause, and workflow edits all advance this
    /// revision; role launches must not admit a stale prepared execution even
    /// when the workflow JSON itself is unchanged.
    pub expected_project_version: Option<i64>,
    pub expected_task_version: i64,
    pub expected_task_status: String,
    pub expected_effective_role: Option<String>,
    /// Capacity snapshot for the selected agent.  A role dispatch must carry
    /// the identity version and max observed while it selected the agent; the
    /// INSERT transaction compares both with the current identity and counts
    /// running executions under the same writer lock before admitting the new
    /// row. The identity version covers selected-profile/provider/daemon/config
    /// reassignment even when the concurrency max is unchanged.
    pub expected_agent_version: Option<i64>,
    pub expected_agent_max_concurrent_tasks: Option<i64>,
    /// For reviewer dispatch, the latest Review row selected as the candidate
    /// parent for the new execution. `None` is meaningful: it asserts that no
    /// candidate existed at the dispatch snapshot and is rechecked before
    /// INSERT.
    pub expected_reviewer_parent_execution_id: Option<String>,
    /// Candidate execution recorded by the latest Review snapshot. This is
    /// intentionally separate from the new execution's parent: a rerun may
    /// review a fresh candidate while the prior Review still names the old
    /// candidate.
    pub expected_latest_review_candidate_execution_id: Option<String>,
    /// Bind the complete Review snapshot that supplied the candidate. Review
    /// rows do not advance Task.version, so execution_id alone is not enough
    /// to detect a changed review authority or conformance payload.
    pub expected_reviewer_id: Option<String>,
    pub expected_reviewer_attempt_number: Option<i64>,
    pub expected_reviewer_status: Option<String>,
    pub expected_reviewer_updated_at: Option<String>,
    /// Exact durable execution bindings on the selected Review snapshot.
    /// These are compared independently of Review.updated_at because a
    /// binding repair/replacement may intentionally leave the Review payload
    /// timestamp unchanged.
    pub expected_reviewer_execution_id: Option<String>,
    pub expected_auditor_execution_id: Option<String>,
    /// Identity and update timestamp of the role assignment selected by the
    /// dispatcher.  The insert transaction compares both so an assignment
    /// that is removed and recreated (or changed and changed back) cannot
    /// silently authorize a stale launch.
    pub expected_assignment_id: Option<String>,
    pub expected_assignment_updated_at: Option<String>,
    /// Exact project workflow JSON selected by the service, or `None` when
    /// the task is governed by the built-in inherited subtask workflow. This
    /// lets the transaction detect a workflow edit that did not bump Task.
    pub expected_workflow_definition: Option<String>,
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
