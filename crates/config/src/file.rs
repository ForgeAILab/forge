use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub(crate) struct FileConfig {
    pub forge: Option<FileForgePaths>,
    pub server: Option<FileServerConfig>,
    pub workspace: Option<FileWorkspaceConfig>,
    pub agent: Option<FileAgentDefaults>,
    pub public_search: Option<FilePublicSearchConfig>,
    pub terminal: Option<FileTerminalConfig>,
    pub scaffold: Option<FileScaffoldConfig>,
    pub project: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileScaffoldConfig {
    pub command: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileForgePaths {
    pub data_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileServerConfig {
    pub bind: Option<String>,
    pub public_base_url: Option<String>,
    pub mcp_enabled: Option<bool>,
    pub jwt_secret: Option<String>,
    pub bcrypt_cost: Option<u32>,
    pub cors_origins: Option<Vec<String>>,
    pub media_upload_limit_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileWorkspaceConfig {
    pub root: Option<String>,
    pub cleanup_delay_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileAgentDefaults {
    pub max_concurrent_tasks: Option<u32>,
    pub heartbeat_interval_seconds: Option<u64>,
    pub max_missed_heartbeats: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FilePublicSearchConfig {
    pub endpoint: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_response_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FileTerminalConfig {
    pub enabled: Option<bool>,
    pub max_sessions_per_task: Option<u32>,
    pub max_sessions_per_user: Option<u32>,
    pub idle_timeout_secs: Option<u64>,
    pub max_lifetime_secs: Option<u64>,
    pub attach_token_ttl_secs: Option<u64>,
    pub reconnect_scrollback_bytes: Option<usize>,
}
