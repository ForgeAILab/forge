use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(u64),
    String(String),
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self::Number(value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientRequest<P> {
    pub jsonrpc: &'static str,
    pub id: RequestId,
    pub method: &'static str,
    pub params: P,
}

impl<P> ClientRequest<P> {
    pub fn new(id: RequestId, method: &'static str, params: P) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method,
            params,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientNotification<P> {
    pub jsonrpc: &'static str,
    pub method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<P>,
}

impl<P> ClientNotification<P> {
    pub fn new(method: &'static str, params: Option<P>) -> Self {
        Self {
            jsonrpc: "2.0",
            method,
            params,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientResponse<R> {
    pub jsonrpc: &'static str,
    pub id: RequestId,
    pub result: R,
}

impl<R> ClientResponse<R> {
    pub fn new(id: RequestId, result: R) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerRequestMessage {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    pub id: RequestId,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerNotificationMessage {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub client_info: ClientInfo,
    pub capabilities: InitializeCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientInfo {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub version: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeCapabilities {
    pub experimental_api: bool,
}

pub type InitializeResponse = Value;

/// A function exposed to Codex for a single chat thread.
///
/// The adapter keeps this type neutral so the host does not need to depend on
/// Codex's tagged `DynamicToolSpec` enum. `ThreadStartParams` adds the
/// protocol-required `type: "function"` discriminator when it serializes the
/// list for `thread/start`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<AskForApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, Value>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_dynamic_tools"
    )]
    pub dynamic_tools: Option<Vec<DynamicToolSpec>>,
    /// An empty list explicitly disables Codex environment access for this
    /// thread. Omitting the field selects the default environment when
    /// environment access is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environments: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<Value>,
}

fn serialize_dynamic_tools<S>(
    tools: &Option<Vec<DynamicToolSpec>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(tools) = tools else {
        return serializer.serialize_none();
    };

    let wire_tools = tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            })
        })
        .collect::<Vec<_>>();
    wire_tools.serialize(serializer)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadForkParams {
    pub thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<AskForApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<Value>,
}

impl ThreadForkParams {
    pub fn from_start(thread_id: impl Into<String>, params: ThreadStartParams) -> Self {
        Self {
            thread_id: thread_id.into(),
            model: params.model,
            model_provider: params.model_provider,
            cwd: params.cwd,
            approval_policy: params.approval_policy,
            sandbox: params.sandbox,
            config: params.config,
            base_instructions: params.base_instructions,
            developer_instructions: params.developer_instructions,
            service_tier: params.service_tier,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeParams {
    pub thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<AskForApproval>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_turns: Option<bool>,
}

impl ThreadResumeParams {
    pub fn from_start(thread_id: impl Into<String>, params: ThreadStartParams) -> Self {
        Self {
            thread_id: thread_id.into(),
            model: params.model,
            model_provider: params.model_provider,
            cwd: params.cwd,
            approval_policy: params.approval_policy,
            sandbox: params.sandbox,
            config: params.config,
            base_instructions: params.base_instructions,
            developer_instructions: params.developer_instructions,
            service_tier: params.service_tier,
            exclude_turns: Some(true),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AskForApproval {
    UnlessTrusted,
    OnFailure,
    OnRequest,
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl SandboxMode {
    pub fn from_config(value: Option<&str>, fallback: Self) -> Self {
        match value {
            Some("read-only" | "locked-down") => Self::ReadOnly,
            Some("workspace-write" | "networking") => Self::WorkspaceWrite,
            Some("danger-full-access") => Self::DangerFullAccess,
            _ => fallback,
        }
    }
}

impl AskForApproval {
    pub fn from_config(value: Option<&str>, fallback: Self) -> Self {
        match value {
            Some("unless-trusted" | "unless-allow-listed") => Self::UnlessTrusted,
            Some("on-failure") => Self::OnFailure,
            Some("on-request" | "always") => Self::OnRequest,
            Some("never") => Self::Never,
            _ => fallback,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartResponse {
    #[serde(default)]
    pub thread: Option<ThreadInfo>,
    #[serde(default, alias = "thread_id")]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

impl ThreadStartResponse {
    pub fn thread_id(&self) -> Option<&str> {
        self.thread
            .as_ref()
            .map(|thread| thread.id.as_str())
            .or(self.thread_id.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadForkResponse {
    #[serde(default)]
    pub thread: Option<ThreadInfo>,
    #[serde(default, alias = "thread_id")]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

impl ThreadForkResponse {
    pub fn thread_id(&self) -> Option<&str> {
        self.thread
            .as_ref()
            .map(|thread| thread.id.as_str())
            .or(self.thread_id.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeResponse {
    #[serde(default)]
    pub thread: Option<ThreadInfo>,
    #[serde(default, alias = "thread_id")]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

impl ThreadResumeResponse {
    pub fn thread_id(&self) -> Option<&str> {
        self.thread
            .as_ref()
            .map(|thread| thread.id.as_str())
            .or(self.thread_id.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThreadInfo {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartParams {
    pub thread_id: String,
    pub input: Vec<UserInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collaboration_mode: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserInput {
    Text {
        text: String,
        #[serde(default)]
        text_elements: Vec<Value>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartResponse {
    #[serde(default, alias = "turn_id")]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub turn: Option<TurnInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TurnInfo {
    pub id: String,
}

#[derive(Debug, Clone)]
pub struct TurnHandle {
    pub turn_id: Option<String>,
}

impl From<TurnStartResponse> for TurnHandle {
    fn from(value: TurnStartResponse) -> Self {
        Self {
            turn_id: value.turn.map(|turn| turn.id).or(value.turn_id),
        }
    }
}

/// Hint key consumed from resolved Codex config snapshots when a Codex auditor
/// should review inside an executor's existing thread.
pub const RESUME_THREAD_ID_CONFIG_KEY: &str = "resume_thread_id";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewStartParams {
    pub thread_id: String,
    pub target: ReviewTarget,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReviewTarget {
    Custom { instructions: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewStartResponse {
    #[serde(default)]
    pub thread: Option<ThreadInfo>,
    #[serde(default, alias = "thread_id")]
    pub thread_id: Option<String>,
}

impl ReviewStartResponse {
    pub fn thread_id(&self) -> Option<&str> {
        self.thread
            .as_ref()
            .map(|thread| thread.id.as_str())
            .or(self.thread_id.as_deref())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelTurnParams {
    pub thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

pub type CancelTurnResponse = Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandExecutionApprovalResponse {
    pub decision: ApprovalDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileChangeApprovalResponse {
    pub decision: ApprovalDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpElicitationResponse {
    pub action: McpElicitationAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum McpElicitationAction {
    Accept,
    Decline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ApprovalDecision {
    Accept,
    Decline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolCallResponse {
    pub content_items: Vec<DynamicToolCallOutputContentItem>,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DynamicToolCallOutputContentItem {
    InputText { text: String },
}
