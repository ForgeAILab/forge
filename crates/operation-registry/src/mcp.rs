//! MCP account, identity and Chat contracts. These direct APIs are separate
//! from the native orchestration operations and retain their wire names.
use crate::{
    authority::{AuthorityRequirement, McpScopeRule, PrincipalRule, RequiredPermission},
    AvailabilityRule, StructuralConstraint, TypedInputContract,
};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::sync::LazyLock;

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
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceRule {
    None,
    OwnedIdentity,
    OwnedSession,
    Project,
    Chat,
    Handoff,
}

/// A conditional field is advertised and admitted by the same evaluator.
pub struct FieldAuthority {
    pub field: &'static str,
    pub permission: &'static str,
    pub denial_code: &'static str,
    pub denial_message: &'static str,
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

pub struct McpOperation {
    pub name: &'static str,
    pub summary: &'static str,
    pub scope: McpScopeRule,
    pub role: RequiredRole,
    pub resource: ResourceRule,
    pub input: TypedInputContract,
    pub field_authority: Option<FieldAuthority>,
    decode: Box<dyn Fn(Value) -> Result<McpInput, String> + Send + Sync>,
}
impl McpOperation {
    pub fn descriptor(&self, constrained: bool, account_admin: bool) -> Value {
        let mut schema = self.input.schema.clone();
        if constrained {
            schema["required"]
                .as_array_mut()
                .unwrap()
                .retain(|field| field != "project_id");
        }
        if let Some(rule) = &self.field_authority {
            if !account_admin {
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
            let input = (self.decode)(arguments.clone())?;
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
        }
    }
    portable(&mut schema);
    schema["properties"] = schema.get("properties").cloned().unwrap_or(json!({}));
    schema["required"] = schema.get("required").cloned().unwrap_or(json!([]));
    schema["additionalProperties"] = json!(false);
    // These were object-only advertisements for intentionally opaque JSON inputs.
    // Value's empty schema advertises exactly the input the existing handler accepts.
    if name == "forge_list_agents" {
        schema["properties"]["status"]["enum"] = json!(["idle", "busy", "error", "offline", null]);
    }
    if name == "forge_set_project_agent" {
        schema["properties"]["wake_budget"]["minimum"] = json!(0);
    }
    McpOperation {
        name,
        summary,
        field_authority: (name == "forge_register_agent").then_some(FieldAuthority {
            field: "daemon_id",
            permission: "mcp_account_admin",
            denial_code: "admin_required",
            denial_message: "Admin access required to pin an agent to a daemon",
        }),
        role,
        resource,
        scope: crate::authority::mcp_scope_rule(name).expect("scope-classified MCP operation"),
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
        // Use the concrete function generated by the registration macro below.
        decode: Box::new(move |value| {
            serde_json::from_value::<I>(value)
                .map(wrap)
                .map_err(|e| e.to_string())
        }),
    }
}

#[derive(Debug)]
pub enum McpInput {
    RegisterAgent(RegisterAgentParams),
    ListAgents(ListAgentsParams),
    ListProjects(ListProjectsParams),
    CreateProject(CreateProjectParams),
    ListAgentProfiles(ListAgentProfilesParams),
    ListAgentSessions(ListAgentSessionsParams),
    GetAgentSession(GetAgentSessionParams),
    GetMainAgent(GetMainAgentParams),
    BindMainAgent(BindMainAgentParams),
    GetProjectAgent(GetProjectAgentParams),
    BindProjectAgent(BindProjectAgentParams),
    ListAgentChats(ListAgentChatsParams),
    GetAgentChat(GetAgentChatParams),
    ListAgentChatMessages(ListAgentChatMessagesParams),
    SendAgentChatMessage(SendAgentChatMessageParams),
    ListAgentHandoffs(ListAgentHandoffsParams),
    GetAgentHandoff(GetAgentHandoffParams),
    CreateAgentHandoff(CreateAgentHandoffParams),
}

pub static CATALOG: LazyLock<Vec<McpOperation>> = LazyLock::new(|| {
    vec![
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
