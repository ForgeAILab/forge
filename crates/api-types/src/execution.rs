use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::{
    AgentStatus, CanonicalPhase, ExecutionBehavior, ExecutionBlockerProjection,
    ExecutionEvidenceSummary, ExecutionRole, ExecutionStatus, InterruptionMetadata,
    PlanArtifactDetail, PlanProgressSummary, ResumePolicy, StopReason, TaskAnnotation,
    TaskRoleAssignmentResponse, TaskStatus, TaskType, UsageAggregate, UsageBreakdown,
    WorkflowDefinition, WorkflowExceptionSummary, WorkflowHealthSummary,
    WorkspacePlacementResponse, WorkspaceResponse,
};

/// Public owner state for a running execution.  This is deliberately
/// separate from semantic progress: a healthy owner may be quiet while a
/// provider or tool call is in flight.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ExecutionOwnerHealth {
    /// The execution is terminal and has no live owner lease.
    Unowned,
    /// The running execution has a current owner lease.
    Healthy,
    /// The owner lease has expired (or was observed expired) while the
    /// execution is still represented as running.
    Expired,
    /// The owner/lease state is not available in this response.
    Unknown,
}

/// Bounded public metadata for a terminal interruption. Rejected late results
/// are retained as durable diagnostics instead of mutating this projection.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ExecutionInterruptionResponse {
    pub reason: String,
    pub kind: Option<String>,
    pub created_at: String,
}

/// The closed set of Task condition commands. Session launches use the session API.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(tag = "verb", rename_all = "snake_case", deny_unknown_fields)]
#[ts(export, tag = "verb", rename_all = "snake_case")]
pub enum TaskAction {
    Start,
    Hold {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
    },
    Release {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
    },
    Retry {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        fresh_session: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        refresh_workspace: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reset_budget: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        guidance: Option<String>,
    },
    SendBack {
        guidance: String,
    },
    Approve {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
        #[serde(rename = "override", default, skip_serializing_if = "Option::is_none")]
        #[ts(optional, rename = "override")]
        override_checks: Option<bool>,
    },
    Restart {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
    },
    Cancel {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        reason: Option<String>,
    },
}

impl TaskAction {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Hold { .. } => "hold",
            Self::Release { .. } => "release",
            Self::Retry { .. } => "retry",
            Self::SendBack { .. } => "send_back",
            Self::Approve { .. } => "approve",
            Self::Restart { .. } => "restart",
            Self::Cancel { .. } => "cancel",
        }
    }

    pub fn retry() -> Self {
        Self::Retry {
            reason: None,
            fresh_session: None,
            refresh_workspace: None,
            reset_budget: None,
            guidance: None,
        }
    }
}

impl std::fmt::Display for TaskAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.verb())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ActionAuthority {
    Owner,
    AssignedAgent,
    ProjectAgent,
    Reviewer,
}

/// A parameter is required when another offered boolean has this value.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ActionParameterRequirement {
    pub parameter: String,
    pub value: bool,
}

/// One executable offer. Parameters name the inputs meaningful in this snapshot;
/// `action` supplies defaults. Authority is filtered before this value is exposed.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ActionParameter {
    pub name: String,
    pub required: bool,
    pub boolean_values: Option<Vec<bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub required_when: Option<ActionParameterRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct Offer {
    pub action: TaskAction,
    pub parameters: Vec<ActionParameter>,
    pub authority: Vec<ActionAuthority>,
    pub reason: String,
    pub label: String,
    pub target_execution_id: Option<String>,
    #[serde(default)]
    pub propagates: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskActionsResponse {
    pub available_actions: Vec<Offer>,
    #[ts(type = "number")]
    pub version: i64,
}

/// Identifies where a Task's effective coder assignment is stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum EffectiveCoderSource {
    Own,
    InheritedFromRoot,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskResponse {
    #[serde(default)]
    pub placement_diagnostics: Vec<TaskPlacementDiagnostic>,
    pub id: String,
    pub project_id: String,
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
    pub effective_coder: Option<TaskRoleAssignmentResponse>,
    pub effective_coder_source: Option<EffectiveCoderSource>,
    #[serde(default)]
    #[ts(type = "Record<string, number>")]
    /// Gate state keys plus non-gate budget kinds; all values are authoritative remaining allowances.
    pub remaining_retries: std::collections::HashMap<String, i64>,
    #[serde(default)]
    #[ts(type = "Record<string, number>")]
    pub retry_limits: std::collections::HashMap<String, i64>,
    #[serde(default)]
    pub available_actions: Vec<Offer>,
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
    pub placement: Option<WorkspacePlacementResponse>,
    pub plan_progress: Option<PlanProgressSummary>,
    pub plan_artifact: Option<PlanArtifactDetail>,
    pub external_issue_number: Option<i64>,
    pub external_issue_url: Option<String>,
    /// Canonical attempt/execution/commit evidence for this Task (D17, F12).
    /// Progress language everywhere else must derive from this value; it can
    /// never be overridden to show "not started" once evidence exists.
    pub execution_evidence: ExecutionEvidenceSummary,
    /// The one canonical execution blocker for this Task (D16/D17), or
    /// `None` when nothing blocks this Task's execution right now. A
    /// Task-scoped blocker here never implies the whole Project is blocked.
    pub execution_blocker: Option<ExecutionBlockerProjection>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Board/list projection. Full content and accounting are loaded from task detail.
/// The collection supports ETag/If-None-Match with Cache-Control: private, no-cache.
/// A 304 has no JSON body; list validators are independent of board_revision.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskListItemResponse {
    pub id: String,
    pub project_id: String,
    pub parent_task_id: Option<String>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub title: String,
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
    /// Gate state keys plus non-gate budget kinds; all values are authoritative remaining allowances.
    pub remaining_retries: std::collections::HashMap<String, i64>,
    #[serde(default)]
    #[ts(type = "Record<string, number>")]
    pub retry_limits: std::collections::HashMap<String, i64>,
    pub error_annotation: Option<TaskAnnotation>,
    pub blocked: Option<InterruptionMetadata>,
    pub failed: Option<InterruptionMetadata>,
    pub workflow_health: Option<WorkflowHealthSummary>,
    pub workflow_exception: Option<WorkflowExceptionSummary>,
    pub review_passed_at: Option<String>,
    pub archived_at: Option<String>,
    pub external_issue_number: Option<i64>,
    pub external_issue_url: Option<String>,
    pub execution_observability: TaskListExecutionObservability,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskListExecutionObservability {
    pub latest_execution_id: Option<String>,
}

impl From<TaskResponse> for TaskListItemResponse {
    fn from(task: TaskResponse) -> Self {
        Self {
            id: task.id,
            project_id: task.project_id,
            parent_task_id: task.parent_task_id,
            assignee_type: task.assignee_type,
            assignee_id: task.assignee_id,
            title: task.title,
            task_type: task.task_type,
            status: task.status,
            canonical_phase: task.canonical_phase,
            awaiting_human: task.awaiting_human,
            priority: task.priority,
            board_position: task.board_position,
            subtask_order: task.subtask_order,
            role_assignments: task.role_assignments,
            remaining_retries: task.remaining_retries,
            retry_limits: task.retry_limits,
            error_annotation: task.error_annotation,
            blocked: task.blocked,
            failed: task.failed,
            workflow_health: task.workflow_health,
            workflow_exception: task.workflow_exception,
            review_passed_at: task.review_passed_at,
            archived_at: task.archived_at,
            external_issue_number: task.external_issue_number,
            external_issue_url: task.external_issue_url,
            version: task.version,
            created_at: task.created_at,
            updated_at: task.updated_at,
            execution_observability: TaskListExecutionObservability {
                latest_execution_id: task.execution_observability.latest_execution_id,
            },
        }
    }
}

/// Compact execution projection used by collection and task-bootstrap
/// endpoints. Large diagnostics remain available from `GET /executions/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ExecutionSummaryResponse {
    pub id: String,
    pub task_id: String,
    pub agent_id: Option<String>,
    pub role: ExecutionRole,
    pub status: ExecutionStatus,
    pub parent_execution_id: Option<String>,
    pub agent_session_id: Option<String>,
    /// A bounded preview of the execution summary (at most 500 characters).
    pub summary: Option<String>,
    /// Whether this execution resumed an existing provider session.
    pub is_resume: bool,
    pub workspace_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskDetailExecutionsPage {
    pub items: Vec<ExecutionSummaryResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub total_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskDetailResponse {
    pub task: TaskResponse,
    pub workflow: WorkflowDefinition,
    pub executions: TaskDetailExecutionsPage,
}

/// Small task projection for a direct parent, child, or dependency relation.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TaskRelationSummary {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    pub parent_task_id: Option<String>,
    pub subtask_order: Option<i64>,
    pub created_at: String,
}

/// Direct relations for a task, loaded without scanning its whole project.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskRelationsResponse {
    pub parent: Option<TaskRelationSummary>,
    pub subtasks: Vec<TaskRelationSummary>,
    pub dependencies: Vec<TaskRelationSummary>,
    pub missing_dependency_ids: Vec<String>,
    pub dependents: Vec<TaskRelationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskExecutionObservability {
    pub counts: crate::ActivityCounts,
    pub tokens: crate::TokenCounters,
    pub cost: crate::CostSummary,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct AgentRunnableOn {
    pub count: u32,
    /// Present only for admins.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub machines: Option<Vec<crate::MachineIdentity>>,
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
    #[serde(default)]
    pub runnable_on: AgentRunnableOn,
    pub max_concurrent_tasks: i64,
    pub status: AgentStatus,
    /// Assigned workload; see `Agent::active_assigned_task_count`.
    pub active_assigned_task_count: Option<i64>,
    /// Live concurrency; this is what `max_concurrent_tasks` gates.
    pub running_execution_count: Option<i64>,
    pub effective_status: Option<String>,
    pub avg_duration_ms: Option<i64>,
    pub success_rate: Option<f64>,
    pub usage: UsageAggregate,
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
    pub max_concurrent_runs: Option<i64>,
    pub run_limit: Option<u32>,
    pub effective_max_concurrent_runs: Option<i64>,
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
    #[serde(default)]
    pub max_concurrent_runs: Option<u32>,
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
    #[serde(default)]
    pub max_concurrent_runs: Option<u32>,
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
    #[ts(type = "Record<string, unknown> | null")]
    pub executor_config_snapshot: Option<Value>,
    pub workspace_id: Option<String>,
    pub plan_progress: Option<PlanProgressSummary>,
    pub plan_artifact: Option<PlanArtifactDetail>,
    pub usage: Option<Vec<UsageBreakdown>>,
    /// Optimistic version used by owner renewal and terminal CAS operations.
    pub execution_version: i64,
    /// Stable server-owned reference for the current owner, never a secret.
    pub lease_owner: Option<String>,
    pub owner_health: ExecutionOwnerHealth,
    pub lease_expires_at: Option<String>,
    pub hard_deadline_at: Option<String>,
    pub last_heartbeat_at: Option<String>,
    /// Semantic progress is intentionally distinct from the owner heartbeat.
    pub last_progress_at: Option<String>,
    pub liveness_warning: Option<String>,
    pub interruption: Option<ExecutionInterruptionResponse>,
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

/// Transport schemas share this closed command shape (REST serde, MCP, typed tools).
pub fn task_action_schema() -> serde_json::Value {
    use serde_json::json;
    let mut variants = Vec::new();
    variants.push(json!({"type":"object", "properties":{"verb":{"const":"start"}}, "required":["verb"], "additionalProperties":false}));
    for verb in ["hold", "release", "restart"] {
        variants.push(json!({"type":"object", "properties":{"verb":{"const":verb},"reason":{"type":"string","minLength":1}}, "required":["verb"], "additionalProperties":false}));
    }
    variants.push(json!({"type":"object", "properties":{"verb":{"const":"cancel"},"reason":{"type":"string","minLength":1}}, "required":["verb"], "additionalProperties":false}));
    variants.push(json!({"type":"object", "properties":{"verb":{"const":"retry"},"reason":{"type":"string","minLength":1}, "fresh_session":{"type":"boolean"}, "refresh_workspace":{"type":"boolean"}, "reset_budget":{"type":"boolean"}, "guidance":{"type":"string"}}, "required":["verb"], "additionalProperties":false}));
    variants.push(json!({"type":"object", "properties":{"verb":{"const":"send_back"}, "guidance":{"type":"string","minLength":1}}, "required":["verb","guidance"], "additionalProperties":false}));
    variants.push(json!({"type":"object", "properties":{"verb":{"const":"approve"}, "override":{"type":"boolean"},"reason":{"type":"string","minLength":1}}, "required":["verb"], "additionalProperties":false}));
    json!({"oneOf":variants})
}

#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct UpdateDaemonRequest {
    #[ts(type = "number")]
    pub version: i64,
    #[serde(deserialize_with = "deserialize_nullable_run_limit")]
    pub run_limit: Option<u32>,
}

fn deserialize_nullable_run_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u32>, D::Error> {
    Option::<u32>::deserialize(deserializer)
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskPlacementDiagnostic {
    pub machine: Option<crate::MachineIdentity>,
    pub filter_codes: Vec<String>,
    pub failing_checks: Vec<String>,
}

/// Removal commits revocation and queues Task settlement; observe Tasks for completion.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoveDaemonResponse {
    pub id: String,
    pub hostname: String,
    #[ts(type = "number")]
    pub pending_remote_cancels_cleared: u64,
    #[ts(type = "number")]
    pub cleanup_records_cleared: u64,
    #[ts(type = "number")]
    pub provisioning_attempts_cleared: u64,
    #[ts(type = "number")]
    pub readiness_records_cleared: u64,
    /// Workspaces the machine owned, released as lost.
    #[ts(type = "number")]
    pub placements_failed: u64,
    /// Tasks whose workspace was on the machine; each re-places on another one.
    #[ts(type = "number")]
    pub tasks_to_replace: u64,
    /// Agents pinned to the machine, now archived.
    #[ts(type = "number")]
    pub agents_retired: u64,
    #[ts(type = "number")]
    pub tasks_queued: u64,
}

/// What removing a machine would change. Read before confirming removal.
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RemoveDaemonPreview {
    pub id: String,
    #[ts(type = "number")]
    pub tasks_to_replace: u64,
    #[ts(type = "number")]
    pub agents_to_retire: u64,
}
