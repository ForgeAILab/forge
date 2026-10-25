//! Tasks, reviews, media links, roles, and transition records.

use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "TaskArchive")]
pub struct Task {
    pub id: String,
    pub project_id: String,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub task_type: String,
    pub status: TaskStatus,
    pub is_automation: bool,
    pub priority: i64,
    pub board_position: f64,
    pub subtask_order: Option<i64>,
    pub task_state_config: Option<String>,
    pub merge_config: Option<String>,
    pub metadata_json: Option<String>,
    pub plan: Option<String>,
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub review_passed_at: Option<String>,
    pub archived_at: Option<String>,
    pub deleted_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub condition: crate::TaskCondition,
}
/// Decoder for private stored Task receipts written before condition became public.
/// Live REST/MCP serializers always use their dedicated DTOs.
#[derive(Deserialize)]
struct TaskArchive {
    pub id: String,
    pub project_id: String,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub task_type: String,
    pub status: TaskStatus,
    pub is_automation: bool,
    pub priority: i64,
    pub board_position: f64,
    pub subtask_order: Option<i64>,
    pub task_state_config: Option<String>,
    pub merge_config: Option<String>,
    pub metadata_json: Option<String>,
    pub plan: Option<String>,
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub review_passed_at: Option<String>,
    pub archived_at: Option<String>,
    pub deleted_at: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub condition: Option<crate::TaskCondition>,
}
impl From<TaskArchive> for Task {
    fn from(archive: TaskArchive) -> Self {
        let condition = archive.condition;
        let mut task = Self {
            id: archive.id,
            project_id: archive.project_id,
            parent_task_id: archive.parent_task_id,
            assignee_type: archive.assignee_type,
            assignee_id: archive.assignee_id,
            title: archive.title,
            description: archive.description,
            task_type: archive.task_type,
            status: archive.status,
            is_automation: archive.is_automation,
            priority: archive.priority,
            board_position: archive.board_position,
            subtask_order: archive.subtask_order,
            task_state_config: archive.task_state_config,
            merge_config: archive.merge_config,
            metadata_json: archive.metadata_json,
            plan: archive.plan,
            error_annotation: archive.error_annotation,
            blocked_json: archive.blocked_json,
            failed_json: archive.failed_json,
            entry_barrier_json: archive.entry_barrier_json,
            review_passed_at: archive.review_passed_at,
            archived_at: archive.archived_at,
            deleted_at: archive.deleted_at,
            version: archive.version,
            created_at: archive.created_at,
            updated_at: archive.updated_at,
            condition: crate::TaskCondition::default(),
        };
        task.condition = condition.unwrap_or_else(|| {
            crate::map_legacy_condition(&crate::LegacyConditionInput::from(&task))
        });
        task
    }
}

pub type TaskStatus = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveTaskIdentity {
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub board_revision: i64,
    pub target_status: String,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompareAndMoveTask {
    pub post_commit_step: Option<crate::EnqueueTaskStep>,
    /// The move enters the workflow's cancellation state: end the Task's
    /// attempt, or ask the integration queue to release it, in the move's own
    /// transaction, as the cancel transition does
    /// (`cancel_task_integration_in_tx`).
    pub integration_cancel: bool,
    pub operation_id: String,
    pub project_id: String,
    pub task_id: String,
    pub task_version: i64,
    pub board_revision: i64,
    pub target_status: String,
    pub target_column_statuses: Vec<String>,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub transition_log_id: String,
    /// Server-derived effective workflow semantics, pinned to task_version.
    pub workflow_snapshot: serde_json::Value,
    pub trigger_name: Option<String>,
    pub triggered_by: api_types::Actor,
    pub bridge: api_types::TransitionBridge,
    pub trigger_reason: String,
    pub rejection: bool,
    /// Exact Project workflow authority observed before resolving this move.
    /// Both values are checked inside the board-mutation transaction.
    pub expected_project_version: Option<i64>,
    pub expected_workflow_definition: Option<String>,
    pub updated_at: String,
}

impl CompareAndMoveTask {
    pub fn identity(&self) -> MoveTaskIdentity {
        MoveTaskIdentity {
            project_id: self.project_id.clone(),
            task_id: self.task_id.clone(),
            task_version: self.task_version,
            board_revision: self.board_revision,
            target_status: self.target_status.clone(),
            before_id: self.before_id.clone(),
            after_id: self.after_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MoveTaskResult {
    pub task: Task,
    pub board_revision: i64,
    pub operation_id: String,
    pub old_status: String,
    pub old_board_position: f64,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MoveTaskPersistence {
    Committed {
        result: Box<MoveTaskResult>,
        transition_log: Box<TransitionLog>,
    },
    Replayed(Box<MoveTaskResult>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Review {
    pub id: String,
    pub task_id: String,
    pub execution_id: String,
    /// Reviewer execution durably reserved for this Review attempt.
    pub reviewer_execution_id: Option<String>,
    /// Optional conformance auditor execution durably bound to this attempt.
    pub auditor_execution_id: Option<String>,
    pub attempt_number: i64,
    pub status: ReviewStatus,
    pub step_results_json: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReviewStatus {
    Running,
    AwaitingHuman,
    Passed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentAuthorType {
    User,
    Agent,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskComment {
    pub id: String,
    pub task_id: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub content: String,
    pub execution_id: Option<String>,
    pub role: Option<String>,
    pub worklog_kind: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMedia {
    pub id: String,
    pub task_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub created_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskMedia {
    pub id: String,
    pub task_id: String,
    pub display_filename: String,
    pub content_type: String,
    pub byte_size: i64,
    pub storage_key: String,
    pub author_type: CommentAuthorType,
    pub author_id: Option<String>,
    pub author_name: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssigneeKind {
    Agent,
    User,
}

enum_strings!(AssigneeKind {
    Agent => "agent",
    User => "user",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRoleAssignment {
    pub id: String,
    pub task_id: String,
    pub role_name: String,
    pub assignee_type: Option<AssigneeKind>,
    pub assignee_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionLog {
    pub id: String,
    pub task_id: String,
    pub from_state: String,
    pub to_state: String,
    pub trigger_name: Option<String>,
    pub triggered_by: String,
    #[serde(flatten)]
    pub bridge: api_types::TransitionBridge,
    pub trigger_reason: String,
    pub hook_results_json: Option<String>,
    pub rejection: bool,
    pub created_at: String,
}

/// Decode the stored `bridge_kind` / `bridge_payload` columns. The schema keeps
/// both open; an unknown kind or a payload that is not JSON is a typed
/// [`crate::DbError::TransitionBridgeCorrupt`], never a panic or a silent NULL.
pub fn decode_transition_bridge(
    transition_log_id: &str,
    kind: Option<&str>,
    payload: Option<&str>,
) -> crate::Result<api_types::TransitionBridge> {
    let corrupt = |reason: String| crate::DbError::TransitionBridgeCorrupt {
        transition_log_id: transition_log_id.to_owned(),
        reason,
    };
    Ok(api_types::TransitionBridge {
        bridge_kind: kind
            .map(str::parse::<api_types::TransitionBridgeKind>)
            .transpose()
            .map_err(corrupt)?,
        bridge_payload: payload
            .map(serde_json::from_str::<serde_json::Value>)
            .transpose()
            .map_err(|error| corrupt(format!("bridge_payload is not JSON: {error}")))?,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRoleAssignment {
    pub id: String,
    pub task_id: String,
    pub role_name: String,
    pub assignee_type: Option<AssigneeKind>,
    pub assignee_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTransitionLog {
    pub id: String,
    pub task_id: String,
    pub from_state: String,
    pub to_state: String,
    pub trigger_name: Option<String>,
    pub triggered_by: String,
    #[serde(flatten)]
    pub bridge: api_types::TransitionBridge,
    pub trigger_reason: String,
    pub hook_results_json: Option<String>,
    pub rejection: bool,
    pub created_at: String,
}

/// Shared write envelope. New aggregate inputs require a statement, including
/// explicit Preserve or an owner-scoped clear; there is no optional intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conditioned<T> {
    pub input: T,
    pub condition: crate::ConditionStatement,
}
