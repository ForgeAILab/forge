use crate::{
    default_config_path, error::ConfigError, file::FileConfig, path::expand_path, ConfigOverrides,
    ForgeConfig,
};
use std::{env, fs, path::Path};

impl ForgeConfig {
    pub fn load(
        config_path: Option<&Path>,
        overrides: ConfigOverrides,
    ) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        let path = config_path.map_or_else(default_config_path, Path::to_path_buf);

        if path.exists() {
            let text = fs::read_to_string(&path).map_err(|source| ConfigError::Read {
                path: path.clone(),
                source,
            })?;
            let file_config =
                serde_yaml::from_str::<FileConfig>(&text).map_err(|source| ConfigError::Parse {
                    path: path.clone(),
                    source,
                })?;
            config.apply_file(file_config);
        }

        config.apply_env()?;
        config.apply_overrides(overrides);
        config.validate()?;
        Ok(config)
    }

    fn apply_file(&mut self, file: FileConfig) {
        if let Some(forge) = file.forge {
            if let Some(data_dir) = forge.data_dir {
                self.forge.data_dir = expand_path(&data_dir);
            }
        }

        if let Some(server) = file.server {
            if let Some(bind) = server.bind {
                self.server.bind = bind;
            }
            if let Some(public_base_url) = server.public_base_url {
                self.server.public_base_url = Some(public_base_url);
            }
            if let Some(mcp_enabled) = server.mcp_enabled {
                self.server.mcp_enabled = mcp_enabled;
            }
            if let Some(jwt_secret) = server.jwt_secret {
                self.server.jwt_secret = Some(jwt_secret);
            }
            if let Some(bcrypt_cost) = server.bcrypt_cost {
                self.server.bcrypt_cost = bcrypt_cost;
            }
            if let Some(cors_origins) = server.cors_origins {
                self.server.cors_origins = cors_origins;
            }
            if let Some(media_upload_limit_bytes) = server.media_upload_limit_bytes {
                self.server.media_upload_limit_bytes = media_upload_limit_bytes;
            }
        }

        if let Some(workspace) = file.workspace {
            if let Some(root) = workspace.root {
                self.workspace.root = expand_path(&root);
            }
            if let Some(cleanup_delay_seconds) = workspace.cleanup_delay_seconds {
                self.workspace.cleanup_delay_seconds = cleanup_delay_seconds;
            }
        }

        if let Some(agent) = file.agent {
            if let Some(max_concurrent_tasks) = agent.max_concurrent_tasks {
                self.agent.max_concurrent_tasks = max_concurrent_tasks;
            }
            if let Some(heartbeat_interval_seconds) = agent.heartbeat_interval_seconds {
                self.agent.heartbeat_interval_seconds = heartbeat_interval_seconds;
            }
            if let Some(max_missed_heartbeats) = agent.max_missed_heartbeats {
                self.agent.max_missed_heartbeats = max_missed_heartbeats;
            }
        }

        if let Some(public_search) = file.public_search {
            if let Some(endpoint) = public_search.endpoint {
                self.public_search.endpoint = Some(endpoint);
            }
            if let Some(timeout_ms) = public_search.timeout_ms {
                self.public_search.timeout_ms = timeout_ms;
            }
            if let Some(max_response_bytes) = public_search.max_response_bytes {
                self.public_search.max_response_bytes = max_response_bytes;
            }
        }

        if let Some(terminal) = file.terminal {
            if let Some(enabled) = terminal.enabled {
                self.terminal.enabled = enabled;
            }
            if let Some(max_sessions_per_task) = terminal.max_sessions_per_task {
                self.terminal.max_sessions_per_task = max_sessions_per_task;
            }
            if let Some(max_sessions_per_user) = terminal.max_sessions_per_user {
                self.terminal.max_sessions_per_user = max_sessions_per_user;
            }
            if let Some(idle_timeout_secs) = terminal.idle_timeout_secs {
                self.terminal.idle_timeout_secs = idle_timeout_secs;
            }
            if let Some(max_lifetime_secs) = terminal.max_lifetime_secs {
                self.terminal.max_lifetime_secs = max_lifetime_secs;
            }
            if let Some(attach_token_ttl_secs) = terminal.attach_token_ttl_secs {
                self.terminal.attach_token_ttl_secs = attach_token_ttl_secs;
            }
            if let Some(reconnect_scrollback_bytes) = terminal.reconnect_scrollback_bytes {
                self.terminal.reconnect_scrollback_bytes = reconnect_scrollback_bytes;
            }
        }

        if let Some(scaffold) = file.scaffold {
            if let Some(command) = scaffold.command {
                self.scaffold.command = command;
            }
        }

        if let Some(project) = file.project {
            self.project.values.extend(project);
        }
    }

    fn apply_env(&mut self) -> Result<(), ConfigError> {
        if let Some(value) = env_value("FORGE_SERVER_BIND") {
            self.server.bind = value;
        }
        if let Some(value) = env_value("FORGE_PUBLIC_BASE_URL") {
            self.server.public_base_url = Some(value);
        }
        if let Some(value) = env_value("FORGE_PUBLIC_SEARCH_ENDPOINT") {
            self.public_search.endpoint = Some(value);
        }
        if let Some(value) = env_value("FORGE_PUBLIC_SEARCH_TIMEOUT_MS") {
            self.public_search.timeout_ms =
                parse_env_u64("FORGE_PUBLIC_SEARCH_TIMEOUT_MS", &value)?;
        }
        if let Some(value) = env_value("FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES") {
            self.public_search.max_response_bytes =
                parse_env_u64("FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES", &value)?;
        }
        if let Some(value) = env_value("FORGE_DATA_DIR") {
            self.forge.data_dir = expand_path(&value);
        }
        if let Some(value) = env_value("FORGE_WORKSPACE_ROOT") {
            self.workspace.root = expand_path(&value);
        }
        if let Some(value) = env_value("FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS") {
            self.workspace.cleanup_delay_seconds =
                parse_env_u64("FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS", &value)?;
        }
        if let Some(value) = env_value("FORGE_AGENT_MAX_CONCURRENT_TASKS") {
            self.agent.max_concurrent_tasks =
                parse_env_u32("FORGE_AGENT_MAX_CONCURRENT_TASKS", &value)?;
        }
        if let Some(value) = env_value("FORGE_AGENT_HEARTBEAT_INTERVAL_SECONDS") {
            self.agent.heartbeat_interval_seconds =
                parse_env_u64("FORGE_AGENT_HEARTBEAT_INTERVAL_SECONDS", &value)?;
        }
        if let Some(value) = env_value("FORGE_AGENT_MAX_MISSED_HEARTBEATS") {
            self.agent.max_missed_heartbeats =
                parse_env_u32("FORGE_AGENT_MAX_MISSED_HEARTBEATS", &value)?;
        }
        if let Some(value) = env_value("FORGE_JWT_SECRET") {
            self.server.jwt_secret = Some(value);
        }
        if let Some(value) = env_value("FORGE_BCRYPT_COST") {
            self.server.bcrypt_cost = parse_env_u32("FORGE_BCRYPT_COST", &value)?;
        }
        if let Some(value) = env_value("FORGE_CORS_ORIGINS") {
            self.server.cors_origins = value
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Some(value) = env_value("FORGE_MEDIA_UPLOAD_LIMIT_BYTES") {
            self.server.media_upload_limit_bytes =
                parse_env_u64("FORGE_MEDIA_UPLOAD_LIMIT_BYTES", &value)?;
        }
        if let Some(value) = env_value("FORGE_SCAFFOLD_COMMAND") {
            self.scaffold.command = value;
        }
        Ok(())
    }

    fn apply_overrides(&mut self, overrides: ConfigOverrides) {
        if let Some(server_bind) = overrides.server_bind {
            self.server.bind = server_bind;
        }
        if let Some(public_base_url) = overrides.server_public_base_url {
            self.server.public_base_url = Some(public_base_url);
        }
        if let Some(mcp_enabled) = overrides.mcp_enabled {
            self.server.mcp_enabled = mcp_enabled;
        }
        if let Some(data_dir) = overrides.data_dir {
            self.forge.data_dir = data_dir;
        }
        if let Some(workspace_root) = overrides.workspace_root {
            self.workspace.root = workspace_root;
        }
        if let Some(cleanup_delay_seconds) = overrides.workspace_cleanup_delay_seconds {
            self.workspace.cleanup_delay_seconds = cleanup_delay_seconds;
        }
        if let Some(max_concurrent_tasks) = overrides.agent_max_concurrent_tasks {
            self.agent.max_concurrent_tasks = max_concurrent_tasks;
        }
        if let Some(heartbeat_interval_seconds) = overrides.agent_heartbeat_interval_seconds {
            self.agent.heartbeat_interval_seconds = heartbeat_interval_seconds;
        }
        if let Some(max_missed_heartbeats) = overrides.agent_max_missed_heartbeats {
            self.agent.max_missed_heartbeats = max_missed_heartbeats;
        }
        if let Some(jwt_secret) = overrides.jwt_secret {
            self.server.jwt_secret = Some(jwt_secret);
        }
        if let Some(bcrypt_cost) = overrides.bcrypt_cost {
            self.server.bcrypt_cost = bcrypt_cost;
        }
        if let Some(cors_origins) = overrides.cors_origins {
            self.server.cors_origins = cors_origins;
        }
        if let Some(media_upload_limit_bytes) = overrides.media_upload_limit_bytes {
            self.server.media_upload_limit_bytes = media_upload_limit_bytes;
        }
    }
}

fn env_value(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn parse_env_u64(key: &'static str, value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .map_err(|error| ConfigError::InvalidEnv {
            key,
            value: value.to_owned(),
            message: error.to_string(),
        })
}

fn parse_env_u32(key: &'static str, value: &str) -> Result<u32, ConfigError> {
    value
        .parse::<u32>()
        .map_err(|error| ConfigError::InvalidEnv {
            key,
            value: value.to_owned(),
            message: error.to_string(),
        })
}
