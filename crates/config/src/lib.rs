#![forbid(unsafe_code)]

mod defaults;
mod error;
mod file;
mod jwt_secret;
mod loader;
mod machine_capacity;
mod run_budget;
pub use machine_capacity::{automatic_run_cap_for_cores, embedded_machine_id, resolved_run_cap};
pub use run_budget::{default_run_nice, logical_cores, resolved_build_jobs_for_cores, RunBudget};
mod path;
mod runtime;
#[cfg(test)]
mod tests;
mod types;

pub use defaults::{
    DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_AGENT_MAX_CONCURRENT_TASKS,
    DEFAULT_AGENT_MAX_MISSED_HEARTBEATS, DEFAULT_BCRYPT_COST, DEFAULT_CHECK_RUN_TIMEOUT_SECONDS,
    DEFAULT_CORS_ORIGIN, DEFAULT_EVENT_CONSUMER_STALL_SECONDS, DEFAULT_LOG_RETENTION_DAYS,
    DEFAULT_MAX_DISCONNECT_SECONDS, DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES, DEFAULT_MIN_FREE_BYTES,
    DEFAULT_MIN_FREE_INODE_PERCENT, DEFAULT_MIN_FREE_PERCENT, DEFAULT_SCAFFOLD_COMMAND,
    DEFAULT_SERVER_BIND, DEFAULT_USAGE_INDEX_BUDGET_MB, DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
};
pub use error::ConfigError;
pub use path::{data_dir_from_env, default_config_path, default_data_dir, default_workspace_root};
pub use runtime::{
    read_server_state, server_state_path, write_server_state, ServerState, SERVER_STATE_FILE,
};
pub use types::{
    AgentDefaults, CommandPolicyConfig, ConfigOverrides, ForgeConfig, ForgePaths, ProjectSettings,
    ProviderDeclaration, ProviderDeclarations, ProviderModelDeclaration, PublicSearchConfig,
    ScaffoldConfig, ServerConfig, TerminalConfig, WorkspaceConfig,
};
