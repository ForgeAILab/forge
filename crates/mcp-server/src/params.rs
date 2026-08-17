use std::str::FromStr;

use api_types::{LifecycleHooks, WorkflowTrigger};
use db::{AgentStatus, PageRequest, SortBy, SortOrder, TaskStatus};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::McpToolError;

#[derive(Debug, Deserialize)]
pub(crate) struct ToolCallParams {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateTaskParams {
    pub(crate) project_id: String,
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) parent_task_id: Option<String>,
    #[serde(default, rename = "type")]
    pub(crate) task_type: Option<String>,
    pub(crate) priority: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListTasksParams {
    pub(crate) project_id: String,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
    #[serde(default)]
    pub(crate) status: StatusFilter,
    pub(crate) sort_by: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetTaskParams {
    pub(crate) task_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PreviewPromptParams {
    pub(crate) task_id: String,
    pub(crate) role: String,
    pub(crate) trigger: Option<WorkflowTrigger>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySearchParams {
    pub(crate) project_id: String,
    pub(crate) query: String,
    pub(crate) layer: Option<u8>,
    pub(crate) token_budget: Option<u32>,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryGetParams {
    pub(crate) id: String,
    pub(crate) layer: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AssignAgentParams {
    pub(crate) task_id: String,
    pub(crate) agent_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListExecutionsParams {
    pub(crate) task_id: String,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateTaskParams {
    pub(crate) task_id: String,
    pub(crate) title: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) priority: Option<i64>,
    pub(crate) plan: Option<String>,
    pub(crate) version: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TransitionTaskParams {
    pub(crate) task_id: String,
    pub(crate) status: TaskStatusParam,
    pub(crate) version: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RegisterAgentParams {
    pub(crate) name: String,
    pub(crate) executor_type: String,
    pub(crate) daemon_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentsParams {
    pub(crate) status: Option<AgentStatusParam>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListProjectsParams {
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateProjectParams {
    pub(crate) name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetProjectParams {
    pub(crate) project_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateProjectParams {
    pub(crate) project_id: String,
    pub(crate) name: Option<String>,
    pub(crate) settings: Option<Value>,
    pub(crate) paused: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateProjectLifecycleHooksParams {
    pub(crate) project_id: String,
    pub(crate) lifecycle_hooks: LifecycleHooks,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateSubTasksParams {
    pub(crate) parent_task_id: String,
    pub(crate) subtasks: Vec<SubTaskInput>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AddTaskDependencyParams {
    pub(crate) task_id: String,
    pub(crate) depends_on_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RemoveTaskDependencyParams {
    pub(crate) task_id: String,
    pub(crate) depends_on_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListTaskDependenciesParams {
    pub(crate) task_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentProfilesParams {
    pub(crate) identity_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentSessionsParams {
    pub(crate) identity_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetAgentSessionParams {
    pub(crate) session_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BindMainAgentParams {
    pub(crate) identity_id: String,
    pub(crate) expected_version: i64,
    #[serde(default)]
    pub(crate) autonomy_policy: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BindProjectAgentParams {
    pub(crate) project_id: String,
    pub(crate) identity_id: String,
    pub(crate) expected_version: i64,
    #[serde(default)]
    pub(crate) permission_ceiling: Value,
    #[serde(default)]
    pub(crate) autonomy_policy: Value,
    #[serde(default)]
    pub(crate) subscriptions: Vec<String>,
    #[serde(default)]
    pub(crate) wake_budget: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetProjectAgentParams {
    pub(crate) project_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentChatsParams {
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetAgentChatParams {
    pub(crate) chat_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentChatMessagesParams {
    pub(crate) chat_id: String,
    pub(crate) before_sequence: Option<i64>,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SendAgentChatMessageParams {
    pub(crate) chat_id: String,
    pub(crate) content: String,
    pub(crate) dedupe_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListAgentHandoffsParams {
    pub(crate) project_id: String,
    pub(crate) cursor: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetAgentHandoffParams {
    pub(crate) project_id: String,
    pub(crate) handoff_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateAgentHandoffParams {
    pub(crate) project_id: String,
    pub(crate) content: String,
    pub(crate) source_message_id: Option<String>,
    pub(crate) source_turn_job_id: Option<String>,
    pub(crate) dedupe_key: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubTaskInput {
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) assignee_id: Option<String>,
}

#[derive(Debug)]
pub(crate) struct TaskStatusParam(TaskStatus);

impl<'de> Deserialize<'de> for TaskStatusParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        TaskStatus::from_str(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl From<TaskStatusParam> for TaskStatus {
    fn from(value: TaskStatusParam) -> Self {
        value.0
    }
}

#[derive(Debug)]
pub(crate) struct AgentStatusParam(AgentStatus);

impl<'de> Deserialize<'de> for AgentStatusParam {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        AgentStatus::from_str(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl From<AgentStatusParam> for AgentStatus {
    fn from(value: AgentStatusParam) -> Self {
        value.0
    }
}

#[derive(Debug, Default)]
pub(crate) struct StatusFilter(Vec<TaskStatus>);

impl StatusFilter {
    pub(crate) fn into_vec(self) -> Vec<TaskStatus> {
        self.0
    }
}

impl<'de> Deserialize<'de> for StatusFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let statuses = match value {
            Value::Null => Vec::new(),
            Value::String(value) => parse_status_list(&value).map_err(serde::de::Error::custom)?,
            Value::Array(values) => values
                .into_iter()
                .map(|value| match value {
                    Value::String(value) => {
                        TaskStatus::from_str(&value).map_err(|_| format!("invalid status: {value}"))
                    }
                    _ => Err("status array must contain strings".to_owned()),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(serde::de::Error::custom)?,
            _ => return Err(serde::de::Error::custom("status must be a string or array")),
        };
        Ok(Self(statuses))
    }
}

pub(crate) fn parse_params<T>(params: Value) -> Result<T, McpToolError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(params).map_err(|error| {
        McpToolError::new(-32602, "invalid params").with_data(json!({
            "details": error.to_string()
        }))
    })
}

fn parse_status_list(value: &str) -> Result<Vec<TaskStatus>, String> {
    value
        .split(',')
        .filter(|status| !status.trim().is_empty())
        .map(|status| {
            TaskStatus::from_str(status.trim())
                .map_err(|_| format!("invalid status: {}", status.trim()))
        })
        .collect()
}

pub(crate) fn page_request(
    cursor: Option<String>,
    limit: Option<i64>,
    sort_by: Option<String>,
) -> Result<PageRequest, McpToolError> {
    Ok(PageRequest {
        cursor,
        limit: limit.unwrap_or(20).clamp(1, 100),
        include_total: false,
        sort_by: parse_sort_by(sort_by.as_deref())?,
        sort_order: SortOrder::Desc,
    })
}

pub(crate) fn task_page_request(
    cursor: Option<String>,
    limit: Option<i64>,
    sort_by: Option<String>,
) -> Result<PageRequest, McpToolError> {
    if sort_by.is_none() {
        return Ok(PageRequest {
            cursor,
            limit: limit.unwrap_or(20).clamp(1, 100),
            include_total: false,
            sort_by: SortBy::BoardPosition,
            sort_order: SortOrder::Asc,
        });
    }

    page_request(cursor, limit, sort_by)
}

fn parse_sort_by(value: Option<&str>) -> Result<SortBy, McpToolError> {
    match value.unwrap_or("created_at") {
        "created_at" => Ok(SortBy::CreatedAt),
        "updated_at" => Ok(SortBy::UpdatedAt),
        "priority" => Ok(SortBy::Priority),
        "board_position" => Ok(SortBy::BoardPosition),
        "id" => Ok(SortBy::Id),
        value => Err(McpToolError::new(
            -32602,
            format!("invalid sort_by: {value}"),
        )),
    }
}
