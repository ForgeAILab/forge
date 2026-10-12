//! Direct MCP contracts, including the Task/Project inputs in `task`. They are separate
//! from the native orchestration operations and retain their wire names.
use crate::{
    authority::{AuthorityRequirement, McpScopeRule, PrincipalRule, RequiredPermission},
    AvailabilityRule, StructuralConstraint, TypedInputContract,
};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::sync::LazyLock;
mod task;
pub use task::*;
#[cfg(test)]
mod task_tests;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetMainAgentParams {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RegisterAgentParams {
    pub name: String,
    pub executor_type: String,
    pub daemon_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentsParams {
    pub status: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListProjectsParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateProjectParams {
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentProfilesParams {
    pub identity_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentSessionsParams {
    pub identity_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAgentSessionParams {
    pub session_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindMainAgentParams {
    pub identity_id: String,
    pub expected_version: i64,
    #[serde(default)]
    pub autonomy_policy: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BindProjectAgentParams {
    pub project_id: String,
    pub identity_id: String,
    pub expected_version: i64,
    #[serde(default)]
    pub permission_ceiling: Value,
    #[serde(default)]
    pub autonomy_policy: Value,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub wake_budget: i64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetProjectAgentParams {
    pub project_id: String,
}

// The merge queues of one Project's repository: the arguments of the native
// `repo.integration_queue` read, plus the Project an MCP caller names. (A doc
// comment here would be published as the schema's description.)
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetIntegrationQueueParams {
    pub project_id: String,
    pub target_branch: Option<String>,
    // The handler clamps to 1..=100, including zero and larger inputs.
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentChatsParams {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAgentChatParams {
    pub chat_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentChatMessagesParams {
    pub chat_id: String,
    pub before_sequence: Option<i64>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendAgentChatMessageParams {
    pub chat_id: String,
    pub content: String,
    pub dedupe_key: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListAgentHandoffsParams {
    pub project_id: String,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetAgentHandoffParams {
    pub project_id: String,
    pub handoff_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateAgentHandoffParams {
    pub project_id: String,
    pub content: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub dedupe_key: String,
}

/// Tool-level role, evaluated with server-loaded facts. Object ownership is
/// resolved separately because IDs are references, never caller authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredRole {
    Account,
    Project,
    Member,
    Admin,
    Owner,
    BoundProject,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceRule {
    None,
    OwnedIdentity,
    OwnedSession,
    Project,
    Chat,
    Handoff,
    Task,
    ParentTask,
    CreateTask,
    DependencyMutation,
    Dependencies,
    Dependents,
    SubTasks,
    ReorderSubTasks,
    Execution,
}

#[derive(Debug, Clone, Copy)]
pub enum AgentReference {
    Task {
        task: &'static str,
        agent: &'static str,
    },
    SubTasks {
        parent: &'static str,
        items: &'static str,
        agent: &'static str,
    },
    Execution {
        execution: &'static str,
        agent: &'static str,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldCondition {
    NonNull,
    ScriptHooks,
}
pub fn contains_script_hooks(settings: &Value) -> bool {
    settings
        .get("lifecycle_hooks")
        .and_then(Value::as_object)
        .is_some_and(|events| {
            events
                .values()
                .filter_map(Value::as_array)
                .flatten()
                .any(|hook| {
                    hook.get("type").is_some_and(|kind| kind == "script")
                        || hook.get("command").is_some()
                })
        })
}

/// A conditional field is advertised and admitted by the same evaluator.
pub struct FieldAuthority {
    pub condition: FieldCondition,
    pub field: &'static str,
    pub permission: &'static str,
    pub denial_code: &'static str,
    pub denial_message: &'static str,
}
impl FieldAuthority {
    pub fn applies(&self, arguments: &Value) -> bool {
        match self.condition {
            FieldCondition::NonNull => arguments
                .get(self.field)
                .is_some_and(|value| !value.is_null()),
            FieldCondition::ScriptHooks => {
                arguments.get(self.field).is_some_and(contains_script_hooks)
            }
        }
    }
}
impl AuthorityRequirement for &FieldAuthority {
    fn principal_rule(&self) -> PrincipalRule {
        PrincipalRule::DelegatedUser
    }
    fn permission(&self, _: &str) -> RequiredPermission<'_> {
        RequiredPermission::Named(self.permission)
    }
    fn availability(&self) -> AvailabilityRule {
        AvailabilityRule::Always
    }
}

#[derive(Debug, Clone, Copy)]
pub enum VersionTarget {
    Project(&'static str),
    Task(&'static str),
}

pub struct McpOperation {
    pub name: &'static str,
    pub summary: &'static str,
    pub scope: McpScopeRule,
    pub role: RequiredRole,
    pub resource: ResourceRule,
    pub input: TypedInputContract,
    pub field_authority: Option<FieldAuthority>,
    pub version_target: Option<VersionTarget>,
    pub agent_reference: Option<AgentReference>,
    decode: Box<dyn Fn(Value) -> Result<McpInput, String> + Send + Sync>,
}
impl McpOperation {
    fn with_agent_reference(mut self, reference: AgentReference) -> Self {
        self.agent_reference = Some(reference);
        self
    }
    pub fn descriptor(&self, constrained: bool, account_admin: bool) -> Value {
        let mut schema = self.input.schema.clone();
        portable_projection(&mut schema);
        if constrained {
            schema["required"]
                .as_array_mut()
                .unwrap()
                .retain(|field| field != "project_id");
        }
        if let Some(rule) = &self.field_authority {
            if rule.condition == FieldCondition::ScriptHooks {
                schema["properties"][rule.field]["description"] = json!(rule.denial_message);
            } else if !account_admin {
                // Explicit null is a base-accepted no-op. Advertise exactly
                // that input to users who cannot exercise the field's power.
                schema["properties"][rule.field] = json!({"type":"null"});
            }
        }
        json!({"name": self.name, "description": format!("{} {}", self.summary, projected_fields(&schema)), "inputSchema": schema})
    }
    /// Strict Serde decode keeps MCP integer spelling unchanged; the shared
    /// contract checker then enforces the portable schema and closed fields.
    pub fn decode(&self, arguments: Value) -> Result<McpInput, McpInputError> {
        let decode = || {
            // Serde would also decode a struct from a positional array, which
            // bypasses the named fields that authority was checked against.
            if !arguments.is_object() {
                return Err("arguments must be an object".to_owned());
            }
            let input = (self.decode)(arguments.clone()).map_err(|detail| {
                // Keep strict MCP Serde diagnostics, adding the contract's
                // field path where its structural check can supply one.
                if let Err(constraint) = self.input.normalize(&arguments) {
                    if let Some((_, rest)) = constraint.split_once("argument `") {
                        if let Some((field, _)) = rest.split_once('`') {
                            return format!("{detail}; argument `{field}`");
                        }
                    }
                }
                detail
            })?;
            self.input.normalize(&arguments)?;
            Ok(input)
        };
        decode().map_err(|detail| McpInputError {
            operation: self.name,
            detail,
            expected: self.input.contract_line(),
        })
    }
}
#[derive(Debug)]
pub struct McpInputError {
    pub operation: &'static str,
    pub detail: String,
    pub expected: String,
}
impl std::fmt::Display for McpInputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}; expected {}",
            self.operation, self.detail, self.expected
        )
    }
}
impl std::error::Error for McpInputError {}

impl AuthorityRequirement for &McpOperation {
    fn principal_rule(&self) -> PrincipalRule {
        self.scope.principal_rule()
    }
    fn permission(&self, scope: &str) -> RequiredPermission<'_> {
        match self.role {
            RequiredRole::Owner if scope == "project" => {
                RequiredPermission::Named("mcp_project_owner")
            }
            RequiredRole::Owner | RequiredRole::BoundProject if scope != "project" => {
                RequiredPermission::Unavailable
            }
            RequiredRole::BoundProject => RequiredPermission::Named("mcp_project_visible"),
            RequiredRole::Admin if scope == "project" => {
                RequiredPermission::Named("mcp_project_admin")
            }
            RequiredRole::Member if scope == "project" => {
                RequiredPermission::Named("mcp_project_member")
            }
            RequiredRole::Project if scope == "project" => {
                RequiredPermission::Named("mcp_project_visible")
            }
            _ => RequiredPermission::None,
        }
    }
    fn availability(&self) -> AvailabilityRule {
        AvailabilityRule::Always
    }
}

fn projected_fields(schema: &Value) -> String {
    let properties = schema["properties"].as_object().expect("typed fields");
    if properties.is_empty() {
        return "no arguments".into();
    }
    let required = schema["required"]
        .as_array()
        .expect("typed required fields");
    format!(
        "{{{}}}",
        properties
            .keys()
            .map(|field| format!(
                "{field}{}",
                if required.contains(&json!(field)) {
                    ""
                } else {
                    "?"
                }
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn typed<I: DeserializeOwned + JsonSchema + 'static>(
    name: &'static str,
    summary: &'static str,
    role: RequiredRole,
    resource: ResourceRule,
    wrap: fn(I) -> McpInput,
) -> McpOperation {
    let mut schema = serde_json::to_value(
        schemars::gen::SchemaSettings::draft07()
            .with(|settings| settings.inline_subschemas = true)
            .into_generator()
            .into_root_schema_for::<I>(),
    )
    .unwrap();
    fn portable(schema: &mut Value) {
        if *schema == true {
            *schema = json!({});
        }
        if let Some(object) = schema.as_object_mut() {
            for key in ["$schema", "title", "default", "format"] {
                object.remove(key);
            }
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                for property in properties.values_mut() {
                    portable(property);
                }
            }
            if let Some(items) = object.get_mut("items") {
                portable(items);
            }
            // schemars marks an optional enum nullable in `type` but leaves
            // null out of its enumeration. Serde accepts that omission value.
            if object
                .get("type")
                .and_then(Value::as_array)
                .is_some_and(|types| types.contains(&Value::String("null".into())))
            {
                if let Some(values) = object.get_mut("enum").and_then(Value::as_array_mut) {
                    if !values.contains(&Value::Null) {
                        values.push(Value::Null);
                    }
                }
            }
        }
    }
    portable(&mut schema);
    schema["properties"] = schema.get("properties").cloned().unwrap_or(json!({}));
    schema["required"] = schema.get("required").cloned().unwrap_or(json!([]));
    schema["additionalProperties"] = json!(false);
    // These were object-only advertisements for intentionally opaque JSON inputs.
    // Value's empty schema advertises exactly the input the existing handler accepts.
    if name == "forge_create_task" {
        schema["properties"]["type"]["enum"] =
            json!(["task", "planning_task", "sub_task", "discovery"]);
        schema["properties"]["depends_on_ids"]["uniqueItems"] = json!(true);
    }
    if name == "forge_reorder_sub_tasks" {
        schema["properties"]["ordered_ids"]["uniqueItems"] = json!(true);
    }
    if matches!(name, "forge_memory_search" | "forge_memory_get") {
        schema["properties"]["layer"]["enum"] = json!([1, 2, 3, null]);
        schema["properties"]["layer"]["maximum"] = json!(255);
    }
    if name == "forge_memory_search" {
        for field in ["limit", "token_budget"] {
            schema["properties"][field]["maximum"] = json!(u32::MAX);
        }
    }
    if name == "forge_follow_up_execution" {
        // Option of an object is a nullable alternative in the derived schema.
        fn deadline_bounds(schema: &mut Value) {
            if schema["properties"].get("hard_deadline_seconds").is_some() {
                schema["properties"]["hard_deadline_seconds"]["minimum"] = json!(1);
                schema["properties"]["hard_deadline_seconds"]["maximum"] = json!(u32::MAX);
            }
            if let Some(variants) = schema.get_mut("anyOf").and_then(Value::as_array_mut) {
                for variant in variants {
                    deadline_bounds(variant);
                }
            }
        }
        deadline_bounds(&mut schema["properties"]["overrides"]);
    }
    if name == "forge_project_escalate" {
        schema["properties"]["need"]["minLength"] = json!(1);
        schema["properties"]["need"]["maxLength"] = json!(4096);
        schema["properties"]["task_ids"]["maxItems"] = json!(100);
        schema["properties"]["dedupe_key"]["minLength"] = json!(1);
    }
    if name == "forge_list_tasks" {
        schema["properties"]["sort_by"]["enum"] = json!([
            "created_at",
            "updated_at",
            "priority",
            "board_position",
            "id",
            null
        ]);
    }
    if name == "forge_list_agents" {
        schema["properties"]["status"]["enum"] = json!(["idle", "busy", "error", "offline", null]);
    }
    if name == "forge_set_project_agent" {
        schema["properties"]["wake_budget"]["minimum"] = json!(0);
    }
    McpOperation {
        version_target: match name {
            "forge_update_project" | "forge_update_project_lifecycle_hooks" => {
                Some(VersionTarget::Project("project_id"))
            }
            "forge_update_task" | "forge_transition_task" => Some(VersionTarget::Task("task_id")),
            _ => None,
        },
        agent_reference: None,
        name,
        summary,
        field_authority: match name {
            "forge_register_agent" => Some(FieldAuthority {
                condition: FieldCondition::NonNull,
                field: "daemon_id",
                permission: "mcp_account_admin",
                denial_code: "admin_required",
                denial_message: "Admin access required to pin an agent to a daemon",
            }),
            "forge_update_project" => Some(FieldAuthority {
                condition: FieldCondition::ScriptHooks,
                field: "settings",
                permission: "mcp_project_admin",
                denial_code: "insufficient_role",
                denial_message: "Script lifecycle hooks require the Project owner or admin role.",
            }),
            _ => None,
        },
        role,
        resource,
        scope: if role == RequiredRole::Account {
            McpScopeRule::AccountInspection
        } else {
            McpScopeRule::BoundProject
        },
        input: TypedInputContract {
            rust_type: std::any::type_name::<I>(),
            schema,
            constraints: &[StructuralConstraint::ClosedObject],
            decode: |value| {
                serde_json::from_value::<I>(value)
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            },
        },
        // Use the concrete typed variant constructor of this registration.
        decode: Box::new(move |value| {
            serde_json::from_value::<I>(value)
                .map(wrap)
                .map_err(|e| e.to_string())
        }),
    }
}

// One declaration generates typed inputs and their handler interface/dispatch.
// The domain specs select these typed variants; consumers bind concrete effects.
macro_rules! mcp_inputs {
    ($($variant:ident($input:ty) => $method:ident),* $(,)?) => {
        #[derive(Debug)]
        pub enum McpInput { $($variant($input)),* }
        #[async_trait::async_trait]
        pub trait McpHandlerContext<E>: Send + Sync {
            $(async fn $method(&self, input: $input) -> Result<Value, E>;)*
        }
        impl McpInput {
            pub async fn dispatch<E>(self, context: &dyn McpHandlerContext<E>) -> Result<Value, E> {
                match self { $(Self::$variant(input) => context.$method(input).await),* }
            }
        }
    };
}
mcp_inputs! {
    ProjectEscalate(ProjectEscalateParams) => project_escalate,
    UpdateProjectLifecycleHooks(UpdateProjectLifecycleHooksParams) => update_project_lifecycle_hooks,
    UpdateProject(UpdateProjectParams) => update_project,
    GetProject(GetProjectParams) => get_project,
    FollowUpExecution(FollowUpExecutionParams) => follow_up_execution,
    ReorderSubTasks(ReorderSubTasksParams) => reorder_sub_tasks,
    CreateSubTasks(CreateSubTasksParams) => create_sub_tasks,
    RemoveTaskDependency(RemoveTaskDependencyParams) => remove_task_dependency,
    AddTaskDependency(AddTaskDependencyParams) => add_task_dependency,
    TransitionTask(TransitionTaskParams) => transition_task,
    UpdateTask(UpdateTaskParams) => update_task,
    TaskAction(TaskActionParams) => task_action,
    AssignAgent(AssignAgentParams) => assign_agent,
    CreateTask(CreateTaskParams) => create_task,
    ListSubTasks(ListSubTasksParams) => list_sub_tasks,
    ListTaskDependents(ListTaskDependenciesParams) => list_task_dependents,
    ListTaskDependencies(ListTaskDependenciesParams) => list_task_dependencies,
    ListExecutions(ListExecutionsParams) => list_executions,
    GetTaskDiff(GetTaskParams) => get_task_diff,
    MemoryGet(MemoryGetParams) => memory_get,
    MemorySearch(MemorySearchParams) => memory_search,
    PreviewPrompt(PreviewPromptParams) => preview_prompt,
    GetTask(GetTaskParams) => get_task,
    ListTasks(ListTasksParams) => list_tasks,
    RegisterAgent(RegisterAgentParams) => register_agent,
    ListAgents(ListAgentsParams) => list_agents,
    ListProjects(ListProjectsParams) => list_projects,
    CreateProject(CreateProjectParams) => create_project,
    ListAgentProfiles(ListAgentProfilesParams) => list_agent_profiles,
    ListAgentSessions(ListAgentSessionsParams) => list_agent_sessions,
    GetAgentSession(GetAgentSessionParams) => get_agent_session,
    GetMainAgent(GetMainAgentParams) => get_main_agent,
    BindMainAgent(BindMainAgentParams) => bind_main_agent,
    GetProjectAgent(GetProjectAgentParams) => get_project_agent,
    BindProjectAgent(BindProjectAgentParams) => bind_project_agent,
    GetIntegrationQueue(GetIntegrationQueueParams) => get_integration_queue,
    ListAgentChats(ListAgentChatsParams) => list_agent_chats,
    GetAgentChat(GetAgentChatParams) => get_agent_chat,
    ListAgentChatMessages(ListAgentChatMessagesParams) => list_agent_chat_messages,
    SendAgentChatMessage(SendAgentChatMessageParams) => send_agent_chat_message,
    ListAgentHandoffs(ListAgentHandoffsParams) => list_agent_handoffs,
    GetAgentHandoff(GetAgentHandoffParams) => get_agent_handoff,
    CreateAgentHandoff(CreateAgentHandoffParams) => create_agent_handoff,
}

pub static CATALOG: LazyLock<Vec<McpOperation>> = LazyLock::new(|| {
    vec![
        typed::<ProjectEscalateParams>(
            "forge_project_escalate",
            "Escalate a bounded blocker to the exact Project owner.",
            RequiredRole::Owner,
            ResourceRule::None,
            McpInput::ProjectEscalate,
        ),
        typed::<CreateTaskParams>(
            "forge_create_task",
            "Create a Task; parent shares a workspace, dependencies gate execution.",
            RequiredRole::Project,
            ResourceRule::CreateTask,
            McpInput::CreateTask,
        ),
        typed::<ListTasksParams>(
            "forge_list_tasks",
            "List Tasks with integration and retry summaries.",
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::ListTasks,
        ),
        typed::<GetTaskParams>(
            "forge_get_task",
            "Read a Task and its current available_actions.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::GetTask,
        ),
        typed::<PreviewPromptParams>(
            "forge_preview_prompt",
            "Preview a role prompt without starting execution.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::PreviewPrompt,
        ),
        typed::<MemorySearchParams>(
            "forge_memory_search",
            "Search Project memory as retrieved context.",
            RequiredRole::BoundProject,
            ResourceRule::None,
            McpInput::MemorySearch,
        ),
        typed::<MemoryGetParams>(
            "forge_memory_get",
            "Read a Project memory item as context.",
            RequiredRole::BoundProject,
            ResourceRule::None,
            McpInput::MemoryGet,
        ),
        typed::<AssignAgentParams>(
            "forge_assign_agent",
            "Assign the implementation worker without starting execution.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::AssignAgent,
        )
        .with_agent_reference(AgentReference::Task {
            task: "task_id",
            agent: "agent_id",
        }),
        typed::<TaskActionParams>(
            "forge_task_action",
            "Apply a current available_actions command at an exact version.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::TaskAction,
        ),
        typed::<GetTaskParams>(
            "forge_get_task_diff",
            "Read the latest Task diff.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::GetTaskDiff,
        ),
        typed::<ListExecutionsParams>(
            "forge_list_executions",
            "List Task executions.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::ListExecutions,
        ),
        typed::<UpdateTaskParams>(
            "forge_update_task",
            "Edit mutable Task fields at an exact version.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::UpdateTask,
        ),
        typed::<TransitionTaskParams>(
            "forge_transition_task",
            "Transition a Task; pending_steps counts asynchronous follow-ups.",
            RequiredRole::Project,
            ResourceRule::Task,
            McpInput::TransitionTask,
        ),
        typed::<RegisterAgentParams>(
            "forge_register_agent",
            "Register an executor.",
            RequiredRole::Account,
            ResourceRule::None,
            McpInput::RegisterAgent,
        ),
        typed::<ListAgentsParams>(
            "forge_list_agents",
            "List executors.",
            RequiredRole::Account,
            ResourceRule::None,
            McpInput::ListAgents,
        ),
        typed::<ListProjectsParams>(
            "forge_list_projects",
            "List Projects.",
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::ListProjects,
        ),
        typed::<CreateProjectParams>(
            "forge_create_project",
            "Create an owned Project.",
            RequiredRole::Account,
            ResourceRule::None,
            McpInput::CreateProject,
        ),
        typed::<GetProjectParams>(
            "forge_get_project",
            "Read Project settings, hooks, and version.",
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::GetProject,
        ),
        typed::<UpdateProjectParams>(
            "forge_update_project",
            "Edit Project fields and validated settings at an exact version.",
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::UpdateProject,
        ),
        typed::<UpdateProjectLifecycleHooksParams>(
            "forge_update_project_lifecycle_hooks",
            "Replace lifecycle hooks at an exact Project version.",
            RequiredRole::Admin,
            ResourceRule::Project,
            McpInput::UpdateProjectLifecycleHooks,
        ),
        typed::<FollowUpExecutionParams>(
            "forge_follow_up_execution",
            "Resume a terminal interactive execution with a follow-up message.",
            RequiredRole::Project,
            ResourceRule::Execution,
            McpInput::FollowUpExecution,
        )
        .with_agent_reference(AgentReference::Execution {
            execution: "execution_id",
            agent: "agent_id",
        }),
        typed::<AddTaskDependencyParams>(
            "forge_add_task_dependency",
            "Add a prerequisite gate without sharing a workspace.",
            RequiredRole::Project,
            ResourceRule::DependencyMutation,
            McpInput::AddTaskDependency,
        ),
        typed::<RemoveTaskDependencyParams>(
            "forge_remove_task_dependency",
            "Remove a prerequisite gate without changing hierarchy.",
            RequiredRole::Project,
            ResourceRule::DependencyMutation,
            McpInput::RemoveTaskDependency,
        ),
        typed::<ListTaskDependenciesParams>(
            "forge_list_task_dependencies",
            "List prerequisite Task IDs.",
            RequiredRole::Project,
            ResourceRule::Dependencies,
            McpInput::ListTaskDependencies,
        ),
        typed::<ListTaskDependenciesParams>(
            "forge_list_task_dependents",
            "List dependent Task IDs.",
            RequiredRole::Project,
            ResourceRule::Dependents,
            McpInput::ListTaskDependents,
        ),
        typed::<CreateSubTasksParams>(
            "forge_create_sub_tasks",
            "Create ordered children; they share the root workspace and run serially.",
            RequiredRole::Project,
            ResourceRule::ParentTask,
            McpInput::CreateSubTasks,
        )
        .with_agent_reference(AgentReference::SubTasks {
            parent: "parent_task_id",
            items: "subtasks",
            agent: "assignee_id",
        }),
        typed::<ListSubTasksParams>(
            "forge_list_sub_tasks",
            "List ordered children sharing the root workspace.",
            RequiredRole::Project,
            ResourceRule::SubTasks,
            McpInput::ListSubTasks,
        ),
        typed::<ReorderSubTasksParams>(
            "forge_reorder_sub_tasks",
            "Reorder direct children; preserve the terminal prefix and active child.",
            RequiredRole::Project,
            ResourceRule::ReorderSubTasks,
            McpInput::ReorderSubTasks,
        ),
        typed::<ListAgentProfilesParams>(
            "forge_list_agent_profiles",
            "List owned profiles.",
            RequiredRole::Account,
            ResourceRule::OwnedIdentity,
            McpInput::ListAgentProfiles,
        ),
        typed::<ListAgentSessionsParams>(
            "forge_list_agent_sessions",
            "List owned sessions.",
            RequiredRole::Account,
            ResourceRule::OwnedIdentity,
            McpInput::ListAgentSessions,
        ),
        typed::<GetAgentSessionParams>(
            "forge_get_agent_session",
            "Get an owned session.",
            RequiredRole::Account,
            ResourceRule::OwnedSession,
            McpInput::GetAgentSession,
        ),
        typed::<GetMainAgentParams>(
            "forge_get_main_agent",
            "Get the Main binding.",
            RequiredRole::Account,
            ResourceRule::None,
            McpInput::GetMainAgent,
        ),
        typed::<BindMainAgentParams>(
            "forge_set_main_agent",
            "Replace the Main binding.",
            RequiredRole::Account,
            ResourceRule::OwnedIdentity,
            McpInput::BindMainAgent,
        ),
        typed::<GetProjectAgentParams>(
            "forge_get_project_agent",
            "Read binding.",
            RequiredRole::Member,
            ResourceRule::Project,
            McpInput::GetProjectAgent,
        ),
        typed::<BindProjectAgentParams>(
            "forge_set_project_agent",
            "Replace binding.",
            RequiredRole::Admin,
            ResourceRule::Project,
            McpInput::BindProjectAgent,
        ),
        typed::<GetIntegrationQueueParams>(
            "forge_get_integration_queue",
            "Read the merge queues of a Project's repository: who is merging, who waits, and why.",
            // The rule of `GET /repos/{id}/integration-queues`: the Project
            // is visible to the caller. The handler reads it by that rule.
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::GetIntegrationQueue,
        ),
        typed::<ListAgentChatsParams>(
            "forge_list_agent_chats",
            "List all.",
            RequiredRole::Project,
            ResourceRule::None,
            McpInput::ListAgentChats,
        ),
        typed::<GetAgentChatParams>(
            "forge_get_agent_chat",
            "Read Chat.",
            RequiredRole::Member,
            ResourceRule::Chat,
            McpInput::GetAgentChat,
        ),
        typed::<ListAgentChatMessagesParams>(
            "forge_list_agent_chat_messages",
            "Read messages.",
            RequiredRole::Member,
            ResourceRule::Chat,
            McpInput::ListAgentChatMessages,
        ),
        typed::<SendAgentChatMessageParams>(
            "forge_send_agent_chat_message",
            "Send message.",
            RequiredRole::Member,
            ResourceRule::Chat,
            McpInput::SendAgentChatMessage,
        ),
        typed::<ListAgentHandoffsParams>(
            "forge_list_agent_handoffs",
            "List all.",
            RequiredRole::Member,
            ResourceRule::Project,
            McpInput::ListAgentHandoffs,
        ),
        typed::<GetAgentHandoffParams>(
            "forge_get_agent_handoff",
            "Read handoff.",
            RequiredRole::Member,
            ResourceRule::Handoff,
            McpInput::GetAgentHandoff,
        ),
        typed::<CreateAgentHandoffParams>(
            "forge_create_agent_handoff",
            "Publish handoff.",
            RequiredRole::Member,
            ResourceRule::Project,
            McpInput::CreateAgentHandoff,
        ),
    ]
});
pub fn lookup(name: &str) -> Option<&'static McpOperation> {
    CATALOG.iter().find(|spec| spec.name == name)
}

#[cfg(test)]
mod tests;

/// Flatten typed alternatives only for provider advertisement. Serde retains
/// the exact tagged contract, including per-variant required/closed fields.
fn portable_projection(schema: &mut Value) {
    if *schema == true {
        *schema = json!({});
    }
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    for key in ["$schema", "title", "default", "format", "propertyNames"] {
        object.remove(key);
    }
    for alternative in ["oneOf", "anyOf"] {
        if let Some(Value::Array(mut variants)) = object.remove(alternative) {
            for variant in &mut variants {
                portable_projection(variant);
            }
            let mut kinds = Vec::<Value>::new();
            let mut values = Vec::<Value>::new();
            let mut properties = serde_json::Map::new();
            let mut required: Option<Vec<Value>> = None;
            for variant in &variants {
                let types = match &variant["type"] {
                    Value::Array(types) => types.clone(),
                    Value::String(kind) => vec![json!(kind)],
                    _ => vec![],
                };
                for kind in types {
                    if !kinds.contains(&kind) {
                        kinds.push(kind);
                    }
                }
                if let Some(enumeration) = variant["enum"].as_array() {
                    for value in enumeration {
                        if !values.contains(value) {
                            values.push(value.clone());
                        }
                    }
                } else if variant["type"] == "null" {
                    values.push(Value::Null);
                }
                if let Some(fields) = variant["properties"].as_object() {
                    for (field, property) in fields {
                        properties
                            .entry(field.clone())
                            .and_modify(|old: &mut Value| {
                                if let (Some(old), Some(new)) = (
                                    old.get_mut("enum").and_then(Value::as_array_mut),
                                    property["enum"].as_array(),
                                ) {
                                    for value in new {
                                        if !old.contains(value) {
                                            old.push(value.clone());
                                        }
                                    }
                                }
                            })
                            .or_insert_with(|| property.clone());
                    }
                    let fields = variant["required"].as_array().cloned().unwrap_or_default();
                    match &mut required {
                        None => required = Some(fields),
                        Some(current) => current.retain(|field| fields.contains(field)),
                    }
                }
            }
            if !kinds.is_empty() {
                object.insert(
                    "type".into(),
                    if kinds.len() == 1 {
                        kinds.remove(0)
                    } else {
                        json!(kinds)
                    },
                );
            }
            if !properties.is_empty() {
                object.insert("properties".into(), json!(properties));
                object.insert("required".into(), json!(required.unwrap_or_default()));
                object.insert("additionalProperties".into(), json!(false));
            } else if !values.is_empty() {
                object.insert("enum".into(), json!(values));
            }
        }
    }
    if let Some(value) = object.remove("const") {
        object.insert("enum".into(), json!([value]));
    }
    if let Some(fields) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for value in fields.values_mut() {
            portable_projection(value);
        }
    }
    if let Some(items) = object.get_mut("items") {
        portable_projection(items);
    }
    if let Some(additional) = object.get_mut("additionalProperties") {
        if additional.is_object() {
            portable_projection(additional);
        }
    }
}
