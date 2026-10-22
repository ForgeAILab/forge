use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAgent {
    Claude,
    Cursor,
    Codex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpScope {
    Project,
    Local,
    User,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAction {
    Install,
    Uninstall,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpEndpointQuery {
    pub scope: Option<String>,
    pub project_id: Option<String>,
}

/// Public connection information; never includes server configuration or credentials.
#[derive(Debug, Clone, Serialize, ts_rs::TS)]
pub struct McpEndpointResponse {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfigQuery {
    pub agent: String,
    pub scope: Option<String>,
    pub project_id: Option<String>,
    pub public_base_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfigActionRequest {
    pub agent: String,
    pub scope: Option<String>,
    pub project_id: Option<String>,
    pub public_base_url: Option<String>,
    pub action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpConfigResponse {
    pub installed: bool,
    pub url: Option<String>,
    pub expected_url: String,
    pub config_path: String,
    pub agents: Vec<String>,
}
