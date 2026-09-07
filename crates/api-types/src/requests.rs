use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use ts_rs::TS;

use crate::{
    project_hooks::{parse_project_hooks_json, ProjectHookRule},
    LifecycleEvent,
};
use crate::{
    InitialRoleAssignment, RecoveryAction, ReviewConfig, TaskGovernanceRequest, TaskStatus,
    TaskType, WorkMode,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTaskRequest {
    pub title: String,
    pub description: Option<String>,
    #[serde(default)]
    pub parent_task_id: Option<String>,
    pub task_type: Option<TaskType>,
    pub priority: Option<i64>,
    pub review_config: Option<ReviewConfig>,
    pub merge_config: Option<Value>,
    #[serde(default)]
    pub role_assignments: Option<Vec<InitialRoleAssignment>>,
    /// Immutable Charter/baseline/milestone provenance for Charter-backed
    /// implementation Tasks. Discovery/planning Tasks may omit it.
    #[serde(default)]
    pub governance: Option<TaskGovernanceRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ts_rs::TS)]
#[ts(export)]
pub struct ReorderSubtasksRequest {
    pub ordered_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTaskRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub priority: Option<i64>,
    pub merge_config: Option<Value>,
    pub plan: Option<String>,
    pub task_state_config: Option<Value>,
    /// Replace the exact non-universal Charter requirements owned by this
    /// Task. `None` preserves the current scope; an explicit empty list clears
    /// it. The server validates every ID against the current approved Charter.
    #[serde(default)]
    pub review_requirement_ids: Option<Vec<String>>,
    #[serde(default)]
    pub parent_task_id: Option<Option<String>>,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct ClaimOverrides {
    pub model_id: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimTaskRequest {
    pub agent_id: String,
    pub overrides: Option<ClaimOverrides>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchExecutionRequest {
    pub agent_id: String,
    pub summary: Option<String>,
    pub overrides: Option<ClaimOverrides>,
}

pub type ExecutionOverridesRequest = ClaimOverrides;

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct FollowUpRequest {
    pub message: String,
    pub agent_id: Option<String>,
    pub overrides: Option<ExecutionOverridesRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    pub task_id: String,
    pub depends_on_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddDependencyRequest {
    pub depends_on_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TransitionTaskRequest {
    pub status: TaskStatus,
    pub version: i64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub source: Option<TransitionSource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum TransitionSource {
    BoardDrag,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RecoverTaskRequest {
    pub action: RecoveryAction,
    pub reason: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TaskActionRequest {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub version: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApproveGateRequest {
    pub reason: Option<String>,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectGateRequest {
    pub reason: String,
    pub version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateProjectRequest {
    pub name: String,
    pub settings: Option<Value>,
    pub default_review_config: Option<ReviewConfig>,
    pub paused: Option<bool>,
    /// Optional initial Project Agent selection.  Omitting either value
    /// leaves the new Project in explicit `agent_setup_required` state; the
    /// server must never fabricate an identity or profile.
    #[serde(default)]
    pub project_agent_identity_id: Option<String>,
    #[serde(default)]
    pub project_agent_profile_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProjectRequest {
    pub version: i64,
    pub name: Option<String>,
    pub settings: Option<Value>,
    pub default_review_config: Option<ReviewConfig>,
    pub primary_repo_id: Option<String>,
    pub paused: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_project_hooks")]
    pub project_hooks: Option<Vec<ProjectHookRule>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TestLifecycleHookRequest {
    pub task_id: String,
    pub event: LifecycleEvent,
    pub hook_index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateRepoRequest {
    pub remote_url: String,
    pub local_path: Option<String>,
    pub name: Option<String>,
    pub default_branch: Option<String>,
    pub work_mode: Option<WorkMode>,
    pub pr_provider: Option<String>,
    pub pr_provider_config: Option<PrProviderConfigRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UpdateRepoRequest {
    #[ts(optional = nullable)]
    pub name: Option<String>,
    #[ts(optional = nullable)]
    pub remote_url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_update_field")]
    #[ts(type = "string | null")]
    #[ts(optional)]
    pub local_path: Option<Option<String>>,
    #[ts(optional = nullable)]
    pub default_branch: Option<String>,
    #[ts(optional = nullable)]
    pub work_mode: Option<WorkMode>,
}

fn deserialize_optional_update_field<'de, D, T>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn deserialize_project_hooks<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ProjectHookRule>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<Value>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let json = serde_json::to_string(&value).map_err(serde::de::Error::custom)?;
    parse_project_hooks_json(&json)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PrProviderConfigRequest {
    pub base_url: Option<String>,
    pub polling_interval_seconds: Option<i64>,
    pub token: Option<String>,
}
