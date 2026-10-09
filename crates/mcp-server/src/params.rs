use std::str::FromStr;

use api_types::{LifecycleHooks, WorkflowTrigger};
use db::{PageRequest, SortOrder, TaskStatus};
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
    #[serde(default)]
    pub(crate) depends_on_ids: Vec<String>,
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
pub(crate) struct GetProjectParams {
    pub(crate) project_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateProjectParams {
    pub(crate) project_id: String,
    pub(crate) version: i64,
    pub(crate) name: Option<String>,
    pub(crate) settings: Option<Value>,
    pub(crate) paused: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateProjectLifecycleHooksParams {
    pub(crate) project_id: String,
    pub(crate) version: i64,
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
pub(crate) struct ListSubTasksParams {
    pub(crate) parent_task_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReorderSubTasksParams {
    pub(crate) parent_task_id: String,
    pub(crate) ordered_ids: Vec<String>,
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
        limit: db::clamp_page_limit(limit),
        include_total: false,
        sort_by: db::sort_by_from_name(sort_by.as_deref())
            .map_err(|error| McpToolError::new(-32602, error.to_string()))?,
        sort_order: SortOrder::Desc,
    })
}

pub(crate) fn task_page_request(
    cursor: Option<String>,
    limit: Option<i64>,
    sort_by: Option<String>,
) -> Result<PageRequest, McpToolError> {
    if sort_by.is_none() {
        let (sort_by, sort_order) = db::task_sort_defaults();
        return Ok(PageRequest {
            cursor,
            limit: db::clamp_page_limit(limit),
            include_total: false,
            sort_by,
            sort_order,
        });
    }

    page_request(cursor, limit, sort_by)
}
