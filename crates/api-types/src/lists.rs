use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::{InterruptionMetadata, TaskResponse, TaskStatus, TaskType};

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Task {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: TaskStatus,
    pub task_type: TaskType,
    pub priority: i32,
    pub board_position: f64,
    pub blocked: Option<InterruptionMetadata>,
    pub failed: Option<InterruptionMetadata>,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub external_issue_number: Option<i64>,
    pub external_issue_url: Option<String>,
    pub version: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TasksResponse {
    pub items: Vec<TaskResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub total_count: Option<u64>,
    pub board_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub status: String,
    pub executor_type: String,
    pub daemon_id: Option<String>,
    /// Tasks assigned to this identity that sit in an active or gate workflow
    /// state. This is assigned workload, not live concurrency: it is not
    /// bounded by `max_concurrent_tasks` and routinely exceeds it.
    pub active_assigned_task_count: i32,
    /// Executions currently running for this identity. This is the number
    /// `max_concurrent_tasks` actually gates.
    pub running_execution_count: i32,
    pub max_concurrent_tasks: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentsResponse {
    pub items: Vec<Agent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Project {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectsResponse {
    pub items: Vec<Project>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MoveTaskRequest {
    pub operation_id: String,
    pub task_version: i64,
    pub board_revision: i64,
    pub target_status: String,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MoveTaskResponse {
    pub task: TaskResponse,
    pub board_revision: i64,
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct OperationConflictDetails {
    pub operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskVersionConflictDetails {
    pub expected_task_version: i64,
    pub actual_task_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct BoardRevisionConflictDetails {
    pub expected_board_revision: i64,
    pub actual_board_revision: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskMovedEventPayload {
    pub project_id: String,
    pub operation_id: String,
    pub old_status: String,
    pub new_status: String,
    pub old_board_position: f64,
    pub new_board_position: f64,
    pub task_version: i64,
    pub board_revision: i64,
    pub before_id: Option<String>,
    pub after_id: Option<String>,
}
