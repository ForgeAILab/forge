use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::{
    AgentStatus, CanonicalPhase, ExecutionAction, ExecutionBehavior, ExecutionRole,
    ExecutionStatus, InterruptionMetadata, PlanArtifactDetail, PlanProgressSummary, ResumePolicy,
    StopReason, TaskAnnotation, TaskRoleAssignmentResponse, TaskStatus, TaskType,
    WorkflowExceptionSummary, WorkflowHealthSummary, WorkspaceResponse,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum TaskAction {
    Start,
    Pause,
    Resume,
    Submit,
    RequestChanges,
    Approve,
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskActionsResponse {
    pub available_actions: Vec<TaskAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskResponse {
    pub id: String,
    pub project_id: String,
    pub repo_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub task_type: TaskType,
    pub status: TaskStatus,
    pub canonical_phase: CanonicalPhase,
    #[serde(default)]
    pub awaiting_human: bool,
    pub priority: i64,
    pub board_position: f64,
    pub subtask_order: Option<i64>,
    #[serde(default)]
    pub role_assignments: Vec<TaskRoleAssignmentResponse>,
    #[serde(default)]
    #[ts(type = "Record<string, number>")]
    pub remaining_retries: std::collections::HashMap<String, i64>,
    #[serde(default)]
    pub execution_actions: Vec<ExecutionAction>,
    pub error_annotation: Option<TaskAnnotation>,
    pub blocked: Option<InterruptionMetadata>,
    pub failed: Option<InterruptionMetadata>,
    pub workflow_health: Option<WorkflowHealthSummary>,
    pub workflow_exception: Option<WorkflowExceptionSummary>,
    pub execution_observability: TaskExecutionObservability,
    #[ts(type = "Record<string, unknown> | null")]
    pub task_state_config: Option<Value>,
    pub review_passed_at: Option<String>,
    pub archived_at: Option<String>,
    pub workspace: Option<WorkspaceResponse>,
    pub plan_progress: Option<PlanProgressSummary>,
    pub plan_artifact: Option<PlanArtifactDetail>,
    pub external_issue_number: Option<i64>,
    pub external_issue_url: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskExecutionObservability {
    pub execution_count: i64,
    pub active_execution_id: Option<String>,
    pub active_role: Option<String>,
    pub active_started_at: Option<String>,
    pub active_elapsed_seconds: Option<f64>,
    pub latest_execution_id: Option<String>,
    pub latest_execution_status: Option<String>,
    pub latest_role: Option<String>,
    pub latest_started_at: Option<String>,
    pub latest_stopped_at: Option<String>,
    pub latest_runtime_seconds: Option<f64>,
    pub total_runtime_seconds: f64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_tokens: i64,
    pub total_cost_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AgentResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub profile_id: String,
    pub backend_kind: String,
    pub executor_type: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub prompt_template: Option<String>,
    pub capabilities: Vec<String>,
    #[ts(type = "Record<string, unknown>")]
    pub config_json: Value,
    pub credential_handle_id: Option<String>,
    pub daemon_id: Option<String>,
    pub max_concurrent_tasks: i64,
    pub status: AgentStatus,
    pub active_task_count: Option<i64>,
    pub effective_status: Option<String>,
    pub total_runs: i64,
    pub avg_duration_ms: Option<i64>,
    pub success_rate: Option<f64>,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_tokens: i64,
    pub total_cost_usd: Option<f64>,
    pub is_default: bool,
    pub paused: bool,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub id: String,
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub status: String,
    pub last_report_at: Option<String>,
    pub detected_clis: Value,
    pub labels: Value,
    pub owner_id: Option<String>,
    pub visibility: String,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectedCli {
    pub kind: String,
    pub availability: String,
    pub config_path: Option<String>,
    pub version: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonRegisterRequest {
    pub machine_id: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub agent_version: Option<String>,
    pub labels: Option<Value>,
    pub runtimes: Option<Vec<RuntimeReport>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonRegisterResponse {
    pub daemon_id: String,
    pub registration_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonReportRequest {
    pub detected_clis: Vec<DetectedCli>,
    pub runtimes: Option<Vec<RuntimeReport>>,
    pub labels: Option<Value>,
    pub active_execution_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeReport {
    pub kind: String,
    pub workspace_root: String,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProjectionAgent {
    pub id: String,
    pub name: String,
    pub executor_type: String,
    pub effective_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProjectionItem {
    pub daemon_id: String,
    pub daemon_hostname: String,
    pub daemon_status: String,
    pub kind: String,
    pub availability: String,
    pub config_path: Option<String>,
    pub version: Option<String>,
    pub path: Option<String>,
    pub agents: Vec<CliProjectionAgent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliProjectionResponse {
    pub items: Vec<CliProjectionItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionUsageResponse {
    pub id: String,
    pub execution_id: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub cost_usd: Option<f64>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskUsageSummaryResponse {
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub total_cache_read_tokens: i64,
    pub total_cache_write_tokens: i64,
    pub total_cost_usd: Option<f64>,
    pub execution_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResponse {
    pub id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub role: ExecutionRole,
    pub status: ExecutionStatus,
    pub parent_execution_id: Option<String>,
    pub agent_session_id: Option<String>,
    pub prompt: Option<String>,
    pub summary: Option<String>,
    pub logs_path: Option<String>,
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
    pub error: Option<String>,
    pub stop_reason: Option<StopReason>,
    pub stopped_by: Option<String>,
    pub resume_policy: Option<ResumePolicy>,
    pub stopped_at: Option<String>,
    pub executor_config_snapshot: Option<Value>,
    pub workspace_id: Option<String>,
    pub plan_progress: Option<PlanProgressSummary>,
    pub plan_artifact: Option<PlanArtifactDetail>,
    pub usage: Option<Vec<ExecutionUsageResponse>>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchExecutionData {
    pub task: TaskResponse,
    pub execution: ExecutionResponse,
    pub workspace: WorkspaceResponse,
    pub execution_behavior: Option<ExecutionBehavior>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchExecutionResponse {
    pub data: LaunchExecutionData,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PromptPreviewResponse {
    pub system: String,
    pub user: String,
    pub tools: Option<Vec<String>>,
}
