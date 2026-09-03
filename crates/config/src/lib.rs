#![forbid(unsafe_code)]

mod defaults;
mod error;
mod file;
mod jwt_secret;
mod loader;
mod path;
mod runtime;
#[cfg(test)]
mod tests;
mod types;

pub use defaults::{
    DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_AGENT_MAX_CONCURRENT_TASKS,
    DEFAULT_AGENT_MAX_MISSED_HEARTBEATS, DEFAULT_BCRYPT_COST, DEFAULT_CORS_ORIGIN,
    DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES, DEFAULT_SCAFFOLD_COMMAND, DEFAULT_SERVER_BIND,
    DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
};
pub use error::ConfigError;
pub use path::{data_dir_from_env, default_config_path, default_data_dir, default_workspace_root};
pub use runtime::{
    read_server_state, server_state_path, write_server_state, ServerState, SERVER_STATE_FILE,
};
pub use types::{
    AgentDefaults, ConfigOverrides, ForgeConfig, ForgePaths, ProjectSettings, PublicSearchConfig,
    ScaffoldConfig, ServerConfig, TerminalConfig, WorkspaceConfig,
};
