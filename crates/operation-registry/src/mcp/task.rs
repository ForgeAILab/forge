//! Direct MCP Task and Project input contracts; no service or DB dependencies.
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Default)]
pub struct StatusFilter(Vec<String>);
impl StatusFilter {
    pub fn into_vec(self) -> Vec<String> {
        self.0
    }
}
impl<'de> Deserialize<'de> for StatusFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let values = match value {
            Value::Null => vec![],
            Value::String(text) => text
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            Value::Array(values) => values
                .into_iter()
                .map(|v| {
                    v.as_str().map(str::to_owned).ok_or_else(|| {
                        serde::de::Error::custom("status array must contain strings")
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(serde::de::Error::custom("status must be a string or array")),
        };
        Ok(Self(values))
    }
}
impl JsonSchema for StatusFilter {
    fn schema_name() -> String {
        "StatusFilter".into()
    }
    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(
            serde_json::json!({"type":["string","array","null"],"items":{"type":"string"}}),
        )
        .unwrap()
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowTrigger {
    Accept,
    Reject,
    Fail,
    Retry,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListTasksParams {
    pub project_id: String,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    #[serde(default)]
    pub status: StatusFilter,
    pub sort_by: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetTaskParams {
    pub task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PreviewPromptParams {
    pub task_id: String,
    pub role: String,
    pub trigger: Option<WorkflowTrigger>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemorySearchParams {
    pub project_id: String,
    pub query: String,
    pub layer: Option<u8>,
    pub token_budget: Option<u32>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryGetParams {
    pub id: String,
    pub layer: Option<u8>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListExecutionsParams {
    pub task_id: String,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListTaskDependenciesParams {
    pub task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListSubTasksParams {
    pub parent_task_id: String,
}

fn non_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateTaskParams {
    pub project_id: String,
    pub title: String,
    pub description: Option<String>,
    #[serde(default, deserialize_with = "non_null")]
    #[schemars(with = "String")]
    pub parent_task_id: Option<String>,
    #[serde(default)]
    pub depends_on_ids: Vec<String>,
    #[serde(default, rename = "type", deserialize_with = "non_null")]
    #[schemars(with = "String")]
    pub task_type: Option<String>,
    #[serde(default, deserialize_with = "non_null")]
    #[schemars(with = "i64")]
    pub priority: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssignAgentParams {
    pub task_id: String,
    pub agent_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateTaskParams {
    pub task_id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub priority: Option<i64>,
    pub plan: Option<String>,
    pub version: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransitionTaskParams {
    pub task_id: String,
    pub status: String,
    pub version: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AddTaskDependencyParams {
    pub task_id: String,
    pub depends_on_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RemoveTaskDependencyParams {
    pub task_id: String,
    pub depends_on_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateSubTasksParams {
    pub parent_task_id: String,
    pub subtasks: Vec<SubTaskInput>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SubTaskInput {
    pub title: String,
    pub description: Option<String>,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReorderSubTasksParams {
    pub parent_task_id: String,
    pub ordered_ids: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FollowUpExecutionParams {
    pub execution_id: String,
    pub message: String,
    pub agent_id: Option<String>,
    pub overrides: Option<ExecutionOverrides>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOverrides {
    pub model_id: Option<String>,
    pub reasoning_effort: Option<String>,
    pub permission_policy: Option<String>,
    pub hard_deadline_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskActionParams {
    pub task_id: String,
    pub action: TaskAction,
    pub version: i64,
}

// The eight landed condition commands, mirrored at the dependency-free wire
// boundary and converted exhaustively by MCP. Server contracts keep the tagged
// variants; portable advertisement gives one field union with a verb enum.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "verb", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskAction {
    Start {},
    Hold {
        reason: Option<String>,
    },
    Release {
        reason: Option<String>,
    },
    Retry {
        reason: Option<String>,
        fresh_session: Option<bool>,
        refresh_workspace: Option<bool>,
        reset_budget: Option<bool>,
        guidance: Option<String>,
    },
    SendBack {
        guidance: String,
    },
    Approve {
        reason: Option<String>,
        #[serde(rename = "override")]
        override_checks: Option<bool>,
    },
    Restart {
        reason: Option<String>,
    },
    Cancel {
        reason: Option<String>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetProjectParams {
    pub project_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectParams {
    pub project_id: String,
    pub version: i64,
    pub name: Option<String>,
    pub settings: Option<Value>,
    pub paused: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectLifecycleHooksParams {
    pub project_id: String,
    pub version: i64,
    pub lifecycle_hooks: LifecycleHooks,
}

pub type LifecycleHooks = std::collections::BTreeMap<LifecycleEvent, Vec<LifecycleHookDef>>;
#[derive(Debug, Deserialize, JsonSchema, Ord, PartialOrd, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEvent {
    BeforeWork,
    OnWorkStart,
    OnWorkStop,
    OnTaskDone,
    OnTaskCancel,
}

fn hook_timeout() -> std::num::NonZeroU64 {
    std::num::NonZeroU64::new(30).unwrap()
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LifecycleHookDef {
    Script {
        command: String,
        #[serde(default = "hook_timeout")]
        timeout_seconds: std::num::NonZeroU64,
        #[serde(default)]
        blocking: bool,
    },
    Plugin {
        name: String,
        #[serde(default)]
        enabled: bool,
        config: Option<Value>,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectEscalateParams {
    pub need: String,
    #[serde(default)]
    pub task_ids: Vec<String>,
    pub dedupe_key: String,
}
