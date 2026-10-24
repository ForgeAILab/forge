//! Task repositories, mutations, queries, roles, and transition logs.

use super::*;

#[async_trait]
pub trait TaskRepo: Send + Sync {
    async fn create(&self, input: CreateTask) -> Result<Task>;
    async fn create_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateTask,
    ) -> Result<Task>;
    async fn get_by_id(&self, id: &str, include_deleted: bool) -> Result<Option<Task>>;
    async fn get_by_id_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
        include_deleted: bool,
    ) -> Result<Option<Task>>;
    async fn list(&self, query: TaskListQuery) -> Result<Page<Task>>;
    /// One aggregate over visible Tasks and their latest Reviews. Workflow maps
    /// carry effective state kinds and whether the state holds root-owned work.
    /// Returns (active, parked, queued), without loading Task or Review bodies.
    async fn count_project_slots(
        &self,
        project_id: &str,
        project_states_json: &str,
        subtask_states_json: &str,
    ) -> Result<(i64, i64, i64)>;
    /// One grouped statement for a JSON object mapping Project IDs to state maps.
    /// Revision fences come from the same SQLite snapshot as the counts.
    async fn count_projects_slots(
        &self,
        project_states_json: &str,
        subtask_states_json: &str,
    ) -> Result<Vec<ProjectSlotCounts>>;
    /// List non-deleted Tasks in a Project whose metadata contains `key`.
    /// Recovery uses this narrow query for durable claims that must be found
    /// independently of the current workflow's state classification.
    async fn list_by_project_with_metadata_key(
        &self,
        project_id: &str,
        key: &str,
    ) -> Result<Vec<Task>>;
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
    /// Update Task fields and append a recovery boundary in the same write
    /// transaction. The marker must not become visible unless the Task CAS
    /// commits, and a concurrent Task writer must invalidate both together.
    async fn update_with_recovery_marker(
        &self,
        input: UpdateTask,
        marker: CreateTransitionLog,
    ) -> Result<Task>;
    /// Update Task fields only when the exact Project workflow authority used
    /// to prepare the mutation is still current. The Project check and Task
    /// CAS share one SQLite writer transaction.
    async fn update_with_workflow_authority(
        &self,
        input: UpdateTask,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task>;
    /// Recovery-marker variant of [`Self::update_with_workflow_authority`].
    /// The Task update, marker, Project authority check, and Task CAS commit
    /// as one writer transaction.
    async fn update_with_workflow_authority_and_recovery_marker(
        &self,
        input: UpdateTask,
        marker: CreateTransitionLog,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task>;
    /// Set a manual-stop annotation only when no newer running execution for
    /// the same Task (in any workspace) or the same workspace (including a
    /// shared-root sibling Task) is present. The execution check and Task
    /// version CAS share one SQLite write transaction, so a cross-role
    /// replacement cannot start between the check and the annotation write.
    /// The id of the latest transition log that entered `expected_status`.
    /// `None` is the explicit initial/no-log state and is checked as such.
    #[allow(clippy::too_many_arguments)]
    async fn set_error_annotation_if_no_running_execution(
        &self,
        id: &str,
        expected_version: i64,
        expected_status: &str,
        expected_state_entry_token: Option<&str>,
        expected_workflow_definition: &str,
        expected_assignment_role: Option<&str>,
        expected_assignment: Option<TaskRoleAssignment>,
        annotation: &str,
        updated_at: &str,
        stopped_execution_id: &str,
        workspace_id: Option<&str>,
        overlapping_roles: Vec<String>,
    ) -> Result<Task>;
    /// Update interruption and dispatch metadata only when no newer running
    /// execution for the same Task (in any workspace) or the same workspace (including a
    /// shared-root sibling Task) is present. Recovery failure paths use this
    /// boundary so they cannot resurrect a manual-stop annotation over a live
    /// cross-role replacement admitted after the clear.
    #[allow(clippy::too_many_arguments)]
    async fn update_recovery_metadata_if_no_running_execution(
        &self,
        id: &str,
        expected_version: i64,
        error_annotation: Option<String>,
        blocked_json: Option<String>,
        failed_json: Option<String>,
        updated_at: &str,
        workspace_id: Option<&str>,
        overlapping_roles: Vec<String>,
        metadata_mutations: Vec<TaskMetadataMutation>,
        condition: Option<crate::ConditionStatement>,
    ) -> Result<Task>;
    /// Restore a queued recovery's interruption and remove its intent in one
    /// versioned write, including when an unrelated execution superseded it.
    async fn restore_queued_recovery(&self, input: RestoreQueuedRecovery) -> Result<Task>;
    async fn archive(&self, input: ArchiveTask) -> Result<Task>;
    async fn soft_delete(&self, input: SoftDeleteTask) -> Result<Task>;
    async fn set_review_passed_at(
        &self,
        id: &str,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Compare-and-set the review authority projection against the Task
    /// revision that produced the review result.  Successful authority writes
    /// increment the Task version, so a late reviewer completion cannot
    /// resurrect the flag after a newer transition has won.
    async fn set_review_passed_at_cas(
        &self,
        id: &str,
        expected_version: i64,
        review_passed_at: Option<String>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Compare-and-set a review authority projection while binding it to the
    /// exact persisted Review update that produced the pass. `updated_at` is
    /// the Task write timestamp; the separate review timestamp lets recovery
    /// repair a missing projection without moving the Task clock backwards.
    async fn set_review_passed_at_cas_for_review(
        &self,
        id: &str,
        expected_version: i64,
        review_passed_at: Option<String>,
        expected_review_updated_at: &str,
        updated_at: &str,
    ) -> Result<Task>;
    /// Apply independent key-level metadata mutations while holding the
    /// SQLite write lock.  When supplied, `expected_version` rejects a stale
    /// Task snapshot before applying the mutation. Callers must use this
    /// boundary for marker updates; a stale snapshot must never replace the
    /// whole document and erase a marker written by another actor.
    async fn mutate_metadata(
        &self,
        id: &str,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Also report whether the mutations changed stored metadata, under the
    /// same write lock. Conditional no-ops must not trigger refresh events.
    async fn mutate_metadata_with_change(
        &self,
        id: &str,
        expected_version: Option<i64>,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<(Task, bool)>;
    /// Apply key-level metadata mutations under a Task version CAS and bump
    /// that version when anything changes. Use this for metadata that grants
    /// exclusive authority over a subsequent side effect: an already-started
    /// Task transition must lose its stale version after the claim commits.
    async fn mutate_metadata_and_bump_version(
        &self,
        id: &str,
        expected_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Version-bumping metadata mutation that also pins the Project revision.
    /// Used when metadata settlement authorizes an external side effect that
    /// was prepared under a particular workflow/governance snapshot.
    async fn mutate_metadata_and_bump_version_with_project_authority(
        &self,
        id: &str,
        expected_version: i64,
        expected_project_version: i64,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Apply a version-bumping metadata effect only while the named terminal
    /// execution is still the newest canonical-role attempt under the same
    /// Project and assignment authority.
    async fn mutate_metadata_and_bump_version_for_latest_execution(
        &self,
        id: &str,
        expected_version: i64,
        authority: LatestExecutionAuthority,
        mutations: Vec<TaskMetadataMutation>,
        updated_at: &str,
    ) -> Result<Option<Task>>;
    /// Install an exclusive Task metadata claim only while the named
    /// execution is still the newest attempt for its canonical role. The
    /// latest-attempt check, Project revision check, metadata write, and Task
    /// version bump share one SQLite writer transaction.
    async fn claim_metadata_for_latest_execution(
        &self,
        input: LatestExecutionMetadataClaim,
    ) -> Result<Task>;
    /// Clear a Task's dispatch disposition/deferred marker and advance its
    /// version in one write transaction. The version bump fences off a stale
    /// dispatcher snapshot that could otherwise re-record the marker.
    async fn wake_dispatch_for_task(&self, id: &str, updated_at: &str) -> Result<Task>;
    /// Wake every parked dispatch marker belonging to a Project in one
    /// transaction. This bumps each affected Task version as a stale-writer
    /// fence, and intentionally leaves `paused_integration` intact: resume
    /// handling consumes that marker after the Project pause boundary.
    async fn wake_dispatch_for_project(&self, project_id: &str, updated_at: &str) -> Result<u64>;
    async fn set_entry_barrier(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
    ) -> Result<Task>;
    /// Set or clear an entry barrier only when the exact Project workflow
    /// authority used to prepare the barrier is still current. The authority
    /// check and Task CAS share one SQLite writer transaction.
    async fn set_entry_barrier_with_workflow_authority(
        &self,
        id: &str,
        expected_version: i64,
        entry_barrier_json: Option<String>,
        updated_at: &str,
        expected_project_version: i64,
        expected_workflow_definition: String,
    ) -> Result<Task>;
    async fn claim(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: ClaimTask,
    ) -> Result<ClaimedTask>;
    async fn update_status(&self, input: UpdateTaskStatus) -> Result<Task>;
    /// Status/blocking projection guarded by the same latest-attempt boundary
    /// as terminal metadata effects. `None` means a newer attempt or changed
    /// governance won and the stale effect was intentionally ignored.
    async fn update_status_for_latest_execution(
        &self,
        input: UpdateTaskStatus,
        authority: LatestExecutionAuthority,
    ) -> Result<Option<Task>>;
    /// Update a Task's status and append a recovery boundary in the same
    /// write transaction. This closes the reset-to-initial marker race where
    /// a later transition could commit between two independent writes.
    async fn update_status_with_recovery_marker(
        &self,
        input: UpdateTaskStatus,
        marker: CreateTransitionLog,
    ) -> Result<Task>;
}

/// One atomic key-level mutation against a Task's metadata document.
///
/// Metadata is a shared extension point.  A whole-document read/modify/write
/// is not safe when independent dispatcher, recovery, and workflow paths each
/// own a different key, so those paths use this operation instead.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LatestExecutionAuthority {
    pub execution_id: String,
    pub role: String,
    pub agent_id: Option<String>,
    pub execution_updated_at: String,
    pub expected_project_version: i64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LatestExecutionMetadataClaim {
    pub task_id: String,
    pub expected_task_version: i64,
    pub authority: LatestExecutionAuthority,
    pub key: String,
    pub value: serde_json::Value,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TaskMetadataMutation {
    Budget(crate::budget::Mutation),
    BudgetIfSpent {
        key: String,
        expected: i64,
        window_id: String,
        mutations: Vec<Self>,
    },
    Set {
        key: String,
        value: serde_json::Value,
    },
    /// Set `key` only when its current JSON value still equals `expected`.
    /// This is the write-side counterpart to `RemoveIf`: a stale recovery
    /// worker must not recreate a marker after another worker cleared it.
    SetIf {
        key: String,
        expected: serde_json::Value,
        value: serde_json::Value,
    },
    /// Set `key` only when it is currently absent. This is useful for a
    /// first-writer marker whose stale producer must not resurrect a marker
    /// that a newer consumer already cleared.
    SetIfAbsent {
        key: String,
        value: serde_json::Value,
    },
    /// Add `by` to an integer metadata counter while holding the same write
    /// transaction as the read.  Unlike a caller-side read followed by `Set`,
    /// concurrent retry workers cannot overwrite one another's increments.
    Increment {
        key: String,
        by: i64,
    },
    Remove {
        key: String,
    },
    /// Remove `key` only when its current JSON value still equals `expected`.
    /// This lets a stale consumer clear the marker it observed without
    /// deleting a newer marker written by a concurrent producer.
    RemoveIf {
        key: String,
        expected: serde_json::Value,
    },
    /// Apply nested mutations only when `key` still contains `expected`.
    /// This gives grouped marker cleanup one identity precondition: a stale
    /// clearer cannot remove a newer marker merely because its reason string
    /// is unchanged.
    CompareAndMutate {
        key: String,
        expected: serde_json::Value,
        mutations: Vec<Self>,
    },
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
    /// Compare-and-move with the exact Project workflow authority that was
    /// used to resolve the target state. The authority is checked while the
    /// same writer transaction holds SQLite's lock, so a workflow edit that
    /// wins between the service read and the board mutation rejects the move
    /// without changing Task or board state.
    async fn compare_and_move_task_with_workflow_authority(
        &self,
        input: CompareAndMoveTask,
        expected_project_version: i64,
        expected_workflow_definition: &str,
    ) -> Result<MoveTaskPersistence>;
    async fn complete_move_operation(
        &self,
        operation_id: &str,
        result: &MoveTaskResult,
        updated_at: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait TaskCommentRepo: Send + Sync {
    async fn create_comment(&self, input: CreateTaskComment) -> Result<TaskComment>;
    async fn list_comments(&self, task_id: &str, page: PageRequest) -> Result<Page<TaskComment>>;
    async fn get_comment_by_id(&self, id: &str) -> Result<Option<TaskComment>>;
    async fn delete_comment(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait TaskDependencyRepo: Send + Sync {
    async fn add_dependency(&self, task_id: &str, depends_on_id: &str, now: &str) -> Result<()>;
    async fn add_dependency_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        depends_on_id: &str,
        now: &str,
    ) -> Result<()>;
    async fn remove_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()>;
    async fn list_dependencies(&self, task_id: &str) -> Result<Vec<String>>;
    async fn list_dependents(&self, depends_on_id: &str) -> Result<Vec<String>>;
    async fn list_dependency_tasks(&self, task_id: &str) -> Result<Vec<Task>>;
    async fn list_dependent_tasks(&self, depends_on_id: &str) -> Result<Vec<Task>>;
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
    pub user_id: String,
    pub include_archived: bool,
    pub include_cancelled: bool,
    pub include_deleted: bool,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTask {
    pub id: String,
    pub project_id: String,
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdateTask {
    pub id: String,
    pub expected_version: i64,
    pub title: Option<String>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub description: Option<Option<String>>,
    pub priority: Option<i64>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub merge_config: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub plan: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub error_annotation: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub blocked_json: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub failed_json: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub task_state_config: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
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
    pub execution: CreateExecution,
    /// The same immutable role/task/Project facts used by scheduler launches.
    /// Claims create the Task mutation and Running execution together, so the
    /// admission must be checked before either row is committed.
    pub execution_admission: Option<ExecutionAdmission>,
    /// Project workflow authority for user claims, which do not necessarily
    /// carry an agent execution admission.
    pub expected_project_version: Option<i64>,
    pub expected_workflow_definition: Option<String>,
    /// The initial owner lease is part of the claim transaction.  A running
    /// execution may never be inserted first and claimed later, since that
    /// would leave an unrecoverable ownerless window.
    pub execution_lease: ClaimExecutionLease,
    pub claimed_at: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClaimedTask {
    pub task: Task,
    pub execution: Execution,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdateTaskStatus {
    pub id: String,
    pub expected_version: i64,
    pub status: String,
    #[serde(with = "crate::task_writer::nested_option")]
    pub assignee_id: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub error_annotation: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub blocked_json: Option<Option<String>>,
    #[serde(with = "crate::task_writer::nested_option")]
    pub failed_json: Option<Option<String>>,
    pub updated_at: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RestoreQueuedRecovery {
    pub task_id: String,
    pub expected_version: i64,
    pub queued_recovery_id: String,
    pub failed_json: Option<String>,
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
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
    /// Execution that produced this entry, when an agent worklog append wrote
    /// it. User-authored comments carry no execution provenance.
    pub execution_id: Option<String>,
    /// Workflow role holding the workspace when the entry was written.
    pub role: Option<String>,
    /// `progress`, `decision`, `validation`, or `blocker` for a worklog entry.
    pub worklog_kind: Option<String>,
    /// Server-derived key so a retried turn appends once.
    pub idempotency_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[async_trait]
pub trait TaskRoleAssignmentRepo: Send + Sync {
    async fn assign(
        &self,
        input: CreateTaskRoleAssignment,
    ) -> std::result::Result<TaskRoleAssignment, crate::DbError>;
    /// Confirm an assignment against the exact row observed by the caller (or
    /// confirm that the row was absent), then upsert it under the SQLite write
    /// lock. This is used by idempotent/initial assignment paths that must not
    /// overwrite a newer reassignment which did not itself advance the Task
    /// version.
    async fn assign_if_unchanged(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
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
    async fn list_by_tasks(&self, task_ids: &[&str]) -> Result<Vec<TaskRoleAssignment>>;
    async fn remove(
        &self,
        task_id: &str,
        role_name: &str,
    ) -> std::result::Result<(), crate::DbError>;
    /// Replace a role assignment and invalidate the Task's review authority
    /// projection under one SQLite write transaction.  The expected Task
    /// revision binds the role change to the caller's snapshot: a concurrent
    /// status/authority transition rejects the whole operation instead of
    /// leaving a new assignee next to stale review authority.
    async fn assign_and_clear_review_authority(
        &self,
        input: CreateTaskRoleAssignment,
        expected_previous: Option<&TaskRoleAssignment>,
        expected_task_version: i64,
        updated_at: &str,
    ) -> std::result::Result<(TaskRoleAssignment, Task), crate::DbError>;
    /// Remove a role assignment and invalidate the Task's review authority
    /// projection under one SQLite write transaction.  The expected Task
    /// revision prevents a stale remover from committing a role change after
    /// a newer Task transition won.
    async fn remove_and_clear_review_authority(
        &self,
        expected_assignment: &TaskRoleAssignment,
        expected_task_version: i64,
        updated_at: &str,
    ) -> std::result::Result<Task, crate::DbError>;
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
    async fn list_by_tasks(&self, task_ids: &[&str]) -> Result<Vec<TransitionLog>>;
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
