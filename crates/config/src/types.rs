use crate::{
    default_data_dir, default_workspace_root, error::ConfigError,
    DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_AGENT_MAX_CONCURRENT_TASKS,
    DEFAULT_AGENT_MAX_MISSED_HEARTBEATS, DEFAULT_BCRYPT_COST, DEFAULT_CORS_ORIGIN,
    DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES, DEFAULT_SCAFFOLD_COMMAND, DEFAULT_SERVER_BIND,
    DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv6Addr},
    path::PathBuf,
};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeConfig {
    pub forge: ForgePaths,
    pub server: ServerConfig,
    pub workspace: WorkspaceConfig,
    pub agent: AgentDefaults,
    #[serde(default)]
    pub public_search: PublicSearchConfig,
    #[serde(default)]
    pub terminal: TerminalConfig,
    #[serde(default)]
    pub scaffold: ScaffoldConfig,
    pub project: ProjectSettings,
    #[serde(default)]
    pub providers: ProviderDeclarations,
    #[serde(default)]
    pub commands: CommandPolicyConfig,
}

/// Which programs an Agent may spawn inside its own workspace.
///
/// Forge ships a built-in allowlist covering build and test tooling. This is
/// how an owner widens it for a stack Forge does not know about, or narrows
/// it to exactly what a deployment permits. A Project can layer its own
/// `command_allowlist` over the result through its settings.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CommandPolicyConfig {
    /// Programs added to the built-in allowlist (or to `only`, when set).
    pub allow: Vec<String>,
    /// Replaces the built-in allowlist outright. An empty list denies every
    /// command, which is a legitimate choice for a read-only deployment.
    pub only: Option<Vec<String>>,
}

/// Model providers declared in the user-owned Forge config file. Solo uses
/// these declarations to create protected provider entries and direct
/// embedded Agents, so a repository-scoped Solo session does not depend on a
/// local CLI harness login. The config file is the onboarding surface: the
/// TUI never prompts for provider credentials.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderDeclarations {
    pub entries: BTreeMap<String, ProviderDeclaration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderDeclaration {
    /// Provider transport kind. One of the embedded runtime's supported
    /// kinds: `openai`, `openai_compatible`, `openrouter`, `xai`, `gemini`.
    pub kind: String,
    /// API root for `openai_compatible`/`openrouter`/`xai` endpoints.
    /// Defaults to the kind's canonical base URL when omitted.
    pub base_url: Option<String>,
    /// Inline API key. The config file is owner-owned host configuration;
    /// prefer `api_key_env` when the deployment cannot protect the file.
    pub api_key: Option<String>,
    /// Environment variable that holds the API key at launch time.
    pub api_key_env: Option<String>,
    pub models: Vec<ProviderModelDeclaration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderModelDeclaration {
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub context_tokens: Option<u32>,
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

/// Optional least-privilege endpoint used by Main and Project Agent Chat
/// web-search tools.  The endpoint is intentionally unauthenticated: Forge
/// never sends cookies, browser state, credentials, or agent/profile secrets
/// to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PublicSearchConfig {
    /// A public HTTPS endpoint implementing Forge's bounded JSON search
    /// contract.  `None` leaves the native tool out of the catalog.
    pub endpoint: Option<String>,
    /// Whole request/response deadline in milliseconds.
    pub timeout_ms: u64,
    /// Maximum response body size in bytes.
    pub max_response_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgePaths {
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default)]
    pub public_base_url: Option<String>,
    #[serde(default = "default_mcp_enabled")]
    pub mcp_enabled: bool,
    pub jwt_secret: Option<String>,
    #[serde(default = "default_bcrypt_cost")]
    pub bcrypt_cost: u32,
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
    #[serde(default = "default_media_upload_limit_bytes")]
    pub media_upload_limit_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub root: PathBuf,
    pub cleanup_delay_seconds: u64,
}

/// Repository scaffolding run by Genesis provisioning when the approved
/// Charter carries a `scaffold` block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaffoldConfig {
    /// Whitespace-separated create-spark invocation; provisioning appends the
    /// target directory name, `--template`, `--yes`, and `--packs`/`--no-packs`.
    pub command: String,
}

impl Default for ScaffoldConfig {
    fn default() -> Self {
        Self {
            command: DEFAULT_SCAFFOLD_COMMAND.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefaults {
    pub max_concurrent_tasks: u32,
    pub heartbeat_interval_seconds: u64,
    pub max_missed_heartbeats: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TerminalConfig {
    pub enabled: bool,
    pub max_sessions_per_task: u32,
    pub max_sessions_per_user: u32,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    pub attach_token_ttl_secs: u64,
    pub reconnect_scrollback_bytes: usize,
}

impl ProviderDeclarations {
    pub fn validate(&self) -> Result<(), ConfigError> {
        const SUPPORTED_KINDS: [&str; 5] =
            ["openai", "openai_compatible", "openrouter", "xai", "gemini"];
        for (name, declaration) in &self.entries {
            if name.trim().is_empty() {
                return Err(ConfigError::InvalidConfig {
                    message: "provider declaration name must not be empty".to_owned(),
                });
            }
            if !SUPPORTED_KINDS.contains(&declaration.kind.as_str()) {
                return Err(ConfigError::InvalidConfig {
                    message: format!(
                        "provider `{name}` has unsupported kind `{}`; expected one of {}",
                        declaration.kind,
                        SUPPORTED_KINDS.join(", ")
                    ),
                });
            }
            match (&declaration.api_key, &declaration.api_key_env) {
                (None, None) => {
                    return Err(ConfigError::InvalidConfig {
                        message: format!("provider `{name}` must set api_key or api_key_env"),
                    });
                }
                (Some(_), Some(_)) => {
                    return Err(ConfigError::InvalidConfig {
                        message: format!(
                            "provider `{name}` sets both api_key and api_key_env; pick one"
                        ),
                    });
                }
                _ => {}
            }
            if declaration.kind == "openai_compatible"
                && declaration
                    .base_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err(ConfigError::InvalidConfig {
                    message: format!(
                        "provider `{name}` uses kind `openai_compatible` and must set base_url"
                    ),
                });
            }
            for model in &declaration.models {
                if model.model.trim().is_empty() {
                    return Err(ConfigError::InvalidConfig {
                        message: format!(
                            "provider `{name}` declares a model with an empty model id"
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ProjectSettings {
    pub values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigOverrides {
    pub server_bind: Option<String>,
    pub server_public_base_url: Option<String>,
    pub mcp_enabled: Option<bool>,
    pub data_dir: Option<PathBuf>,
    pub workspace_root: Option<PathBuf>,
    pub workspace_cleanup_delay_seconds: Option<u64>,
    pub agent_max_concurrent_tasks: Option<u32>,
    pub agent_heartbeat_interval_seconds: Option<u64>,
    pub agent_max_missed_heartbeats: Option<u32>,
    pub jwt_secret: Option<String>,
    pub bcrypt_cost: Option<u32>,
    pub cors_origins: Option<Vec<String>>,
    pub media_upload_limit_bytes: Option<u64>,
}

impl ForgeConfig {
    #[must_use]
    pub fn db_path(&self) -> PathBuf {
        self.forge.data_dir.join("forge.db")
    }

    #[must_use]
    pub fn sessions_dir(&self) -> PathBuf {
        self.forge.data_dir.join("sessions")
    }

    #[must_use]
    pub fn logs_dir(&self) -> PathBuf {
        self.sessions_dir()
    }

    #[must_use]
    pub fn trusted_origin(&self) -> String {
        self.server
            .public_base_url
            .as_deref()
            .and_then(parse_trusted_origin)
            .unwrap_or_else(|| format!("http://{}", self.server.bind))
    }

    /// Origins allowed to start a browser OAuth ceremony and receive its
    /// post-login bounce: the configured CORS origins plus the server's own
    /// serving origin. A loopback serving origin is added under both the
    /// `localhost` and `127.0.0.1` spellings, since browsers treat them as
    /// distinct origins.
    #[must_use]
    pub fn trusted_web_origins(&self) -> Vec<String> {
        let mut origins: Vec<String> = self
            .server
            .cors_origins
            .iter()
            .filter_map(|value| parse_trusted_origin(value))
            .collect();
        let mut push = |origin: String| {
            if !origins.contains(&origin) {
                origins.push(origin);
            }
        };
        let own = self.trusted_origin();
        if let Some(origin) = parse_trusted_origin(&own) {
            push(origin);
        }
        if let Ok(url) = Url::parse(&own) {
            let loopback = match url.host() {
                Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
                Some(url::Host::Ipv4(address)) => address.is_loopback(),
                Some(url::Host::Ipv6(address)) => address.is_loopback(),
                None => false,
            };
            if loopback {
                let scheme = url.scheme();
                for host in ["localhost", "127.0.0.1"] {
                    let candidate = match url.port_or_known_default() {
                        Some(port) => format!("{scheme}://{host}:{port}"),
                        None => format!("{scheme}://{host}"),
                    };
                    if let Some(origin) = parse_trusted_origin(&candidate) {
                        push(origin);
                    }
                }
            }
        }
        origins
    }

    #[must_use]
    pub fn mcp_resource_url(&self) -> String {
        format!("{}/mcp", self.trusted_origin())
    }

    #[must_use]
    pub fn workflows_dir(&self) -> PathBuf {
        self.forge.data_dir.join("workflows")
    }

    pub fn ensure_workflows_dir(&self) -> std::io::Result<PathBuf> {
        let dir = self.workflows_dir();
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        self.terminal.validate()?;
        self.public_search.validate()?;
        self.providers.validate()
    }

    #[must_use]
    pub fn jwt_secret_path(&self) -> PathBuf {
        self.forge.data_dir.join("jwt_secret.bin")
    }
}

impl Default for ForgeConfig {
    fn default() -> Self {
        Self {
            forge: ForgePaths {
                data_dir: default_data_dir(),
            },
            server: ServerConfig {
                bind: DEFAULT_SERVER_BIND.to_owned(),
                public_base_url: None,
                mcp_enabled: true,
                jwt_secret: None,
                bcrypt_cost: DEFAULT_BCRYPT_COST,
                cors_origins: vec![DEFAULT_CORS_ORIGIN.to_owned()],
                media_upload_limit_bytes: DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES,
            },
            workspace: WorkspaceConfig {
                root: default_workspace_root(),
                cleanup_delay_seconds: DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
            },
            agent: AgentDefaults {
                max_concurrent_tasks: DEFAULT_AGENT_MAX_CONCURRENT_TASKS,
                heartbeat_interval_seconds: DEFAULT_AGENT_HEARTBEAT_INTERVAL_SECONDS,
                max_missed_heartbeats: DEFAULT_AGENT_MAX_MISSED_HEARTBEATS,
            },
            public_search: PublicSearchConfig::default(),
            terminal: TerminalConfig::default(),
            scaffold: ScaffoldConfig::default(),
            project: ProjectSettings::default(),
            providers: ProviderDeclarations::default(),
            commands: CommandPolicyConfig::default(),
        }
    }
}

impl Default for PublicSearchConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            timeout_ms: 5_000,
            max_response_bytes: 256 * 1024,
        }
    }
}

impl PublicSearchConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(100..=30_000).contains(&self.timeout_ms) {
            return Err(ConfigError::InvalidConfig {
                message: "public_search.timeout_ms must be between 100 and 30000".to_owned(),
            });
        }
        if !(1024..=4 * 1024 * 1024).contains(&self.max_response_bytes) {
            return Err(ConfigError::InvalidConfig {
                message: "public_search.max_response_bytes must be between 1024 and 4194304"
                    .to_owned(),
            });
        }
        let Some(endpoint) = self.endpoint.as_deref() else {
            return Ok(());
        };
        if endpoint.chars().count() > 2048 {
            return Err(ConfigError::InvalidConfig {
                message: "public_search.endpoint is too long".to_owned(),
            });
        }
        let parsed = Url::parse(endpoint).map_err(|_| ConfigError::InvalidConfig {
            message: "public_search.endpoint must be an absolute HTTPS URL".to_owned(),
        })?;
        if parsed.scheme() != "https"
            || parsed.host().is_none()
            || parsed.username() != ""
            || parsed.password().is_some()
            || parsed.fragment().is_some()
            || parsed.query().is_some()
        {
            return Err(ConfigError::InvalidConfig {
                message: "public_search.endpoint must be an absolute HTTPS URL without credentials, query, or fragment".to_owned(),
            });
        }
        if parsed.host_str().is_some_and(is_private_or_local_host) {
            return Err(ConfigError::InvalidConfig {
                message: "public_search.endpoint must not target a local or private host"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

fn is_private_or_local_host(host: &str) -> bool {
    let normalized = host
        .trim_end_matches('.')
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if normalized.contains('%') {
        return true;
    }
    if matches!(normalized.as_str(), "localhost" | "localhost.localdomain")
        || normalized.ends_with(".localhost")
        || normalized.ends_with(".local")
    {
        return true;
    }
    let Ok(address) = normalized.parse::<IpAddr>() else {
        return false;
    };
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || octets[0] == 0
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0)
                || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                || octets[0] >= 224
        }
        IpAddr::V6(address) => is_blocked_public_ipv6(address),
    }
}

fn is_blocked_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    let first = segments[0];
    address.is_loopback()
        || address.is_unspecified()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        // Reject every IPv4-compatible/mapped representation, including a
        // mapped public address.  Allowing one would make endpoint policy
        // differ depending on the resolver's address-family choice.
        || address.to_ipv4().is_some()
        || (first & 0xffc0 == 0xfec0)
        || (first & 0xff00 == 0xff00)
        || (first == 0x2001 && segments[1] == 0x0db8)
        || (first == 0x2001 && segments[1] == 0x0002 && segments[2] == 0)
        || (first == 0x2001 && (0..=5).contains(&segments[1]))
        || (0x3ff0..=0x3fff).contains(&first)
        || (first == 0x2001 && segments[1] == 0)
        || (first == 0x2001 && (0x0010..=0x001f).contains(&segments[1]))
        || (first == 0x2001 && (0x0020..=0x002f).contains(&segments[1]))
        || first == 0x2002
        || (first == 0x0100
            && segments[1] == 0
            && segments[2] == 0
            && segments[3] == 0)
        || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 0)
        || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 1)
}

impl Default for TerminalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_sessions_per_task: 2,
            max_sessions_per_user: 4,
            idle_timeout_secs: 1800,
            max_lifetime_secs: 28800,
            attach_token_ttl_secs: 60,
            reconnect_scrollback_bytes: 65536,
        }
    }
}

impl TerminalConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_sessions_per_task > self.max_sessions_per_user {
            return Err(ConfigError::InvalidConfig {
                message: format!(
                    "terminal.max_sessions_per_task ({}) must be less than or equal to terminal.max_sessions_per_user ({})",
                    self.max_sessions_per_task, self.max_sessions_per_user
                ),
            });
        }
        Ok(())
    }
}

fn default_mcp_enabled() -> bool {
    true
}

fn default_bcrypt_cost() -> u32 {
    DEFAULT_BCRYPT_COST
}

fn default_cors_origins() -> Vec<String> {
    vec![DEFAULT_CORS_ORIGIN.to_owned()]
}

fn default_media_upload_limit_bytes() -> u64 {
    DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES
}

fn parse_trusted_origin(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let origin = url.origin();
    origin.is_tuple().then(|| origin.ascii_serialization())
}
