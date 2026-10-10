use crate::{
    data_dir_from_env, default_data_dir, default_workspace_root, read_server_state,
    server_state_path, write_server_state, ConfigError, ConfigOverrides, ForgeConfig,
    PublicSearchConfig, ServerState, TerminalConfig, DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES,
    DEFAULT_SERVER_BIND, DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
};
use std::{
    env, fs,
    sync::{Mutex, OnceLock},
};
use tempfile::tempdir;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn defaults_are_usable_without_a_config_file() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let missing_path = dir.path().join("missing-forge.yaml");
    let config = ForgeConfig::load(Some(&missing_path), test_overrides(dir.path()))
        .expect("default config loads");

    assert_eq!(config.server.bind, DEFAULT_SERVER_BIND);
    assert!(config.server.mcp_enabled);
    assert_eq!(
        config.server.media_upload_limit_bytes,
        DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES
    );
    assert_eq!(config.forge.data_dir, dir.path());
    assert_eq!(config.db_path(), dir.path().join("forge.db"));
    assert_eq!(config.workspace.root, default_workspace_root());
    assert_eq!(
        config.workspace.cleanup_delay_seconds,
        DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS
    );
}

#[test]
fn terminal_config_default_values_match_spec() {
    let terminal = TerminalConfig::default();

    assert!(!terminal.enabled);
    assert_eq!(terminal.max_sessions_per_task, 2);
    assert_eq!(terminal.max_sessions_per_user, 4);
    assert_eq!(terminal.idle_timeout_secs, 1800);
    assert_eq!(terminal.max_lifetime_secs, 28800);
    assert_eq!(terminal.attach_token_ttl_secs, 60);
    assert_eq!(terminal.reconnect_scrollback_bytes, 65536);
}

#[test]
fn partial_terminal_file_config_merges_with_defaults() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
terminal:
  enabled: true
  max_sessions_per_task: 3
  attach_token_ttl_secs: 45
"#,
    )
    .expect("write config");

    let config =
        ForgeConfig::load(Some(&config_path), test_overrides(dir.path())).expect("config loads");

    assert!(config.terminal.enabled);
    assert_eq!(config.terminal.max_sessions_per_task, 3);
    assert_eq!(config.terminal.max_sessions_per_user, 4);
    assert_eq!(config.terminal.idle_timeout_secs, 1800);
    assert_eq!(config.terminal.max_lifetime_secs, 28800);
    assert_eq!(config.terminal.attach_token_ttl_secs, 45);
    assert_eq!(config.terminal.reconnect_scrollback_bytes, 65536);
}

#[test]
fn terminal_config_rejects_task_limit_above_user_limit() {
    let terminal = TerminalConfig {
        max_sessions_per_task: 5,
        max_sessions_per_user: 4,
        ..Default::default()
    };

    let error = terminal
        .validate()
        .expect_err("terminal config rejects invalid limits");

    assert!(matches!(
        error,
        ConfigError::InvalidConfig { message }
            if message.contains("terminal.max_sessions_per_task")
    ));
}

#[test]
fn public_search_defaults_to_disabled_and_bounded_limits() {
    let config = PublicSearchConfig::default();

    assert_eq!(config.endpoint, None);
    assert_eq!(config.timeout_ms, 5_000);
    assert_eq!(config.max_response_bytes, 256 * 1024);
    config.validate().expect("default search config is valid");
}

#[test]
fn public_search_rejects_unsafe_endpoints_and_unbounded_limits() {
    for endpoint in [
        "http://search.example.test",
        "https://localhost/search",
        "https://127.0.0.1/search",
        "https://[::ffff:127.0.0.1]/search",
        "https://[::ffff:8.8.8.8]/search",
        "https://[::8.8.8.8]/search",
        "https://[64:ff9b::192.0.2.1]/search",
        "https://[fe80::1%25en0]/search",
        "https://[2001:2::1]/search",
        "https://192.0.2.1/search",
        "https://[2001:db8::1]/search",
        "https://search.example.test/?token=secret",
        "https://user:password@search.example.test/search",
    ] {
        let config = PublicSearchConfig {
            endpoint: Some(endpoint.to_owned()),
            ..Default::default()
        };
        assert!(
            config.validate().is_err(),
            "endpoint must be rejected: {endpoint}"
        );
    }

    for (timeout_ms, max_response_bytes) in [
        (99, 256 * 1024),
        (30_001, 256 * 1024),
        (5_000, 1023),
        (5_000, 4 * 1024 * 1024 + 1),
    ] {
        let config = PublicSearchConfig {
            endpoint: Some("https://search.example.test".to_owned()),
            timeout_ms,
            max_response_bytes,
        };
        assert!(config.validate().is_err());
    }
}

#[test]
fn public_search_file_and_environment_settings_are_loaded() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
public_search:
  endpoint: https://search.example.test/api
  timeout_ms: 2500
  max_response_bytes: 65536
"#,
    )
    .expect("write config");
    env::set_var("FORGE_PUBLIC_SEARCH_TIMEOUT_MS", "3000");

    let config = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
        .expect("search config loads");
    assert_eq!(
        config.public_search.endpoint.as_deref(),
        Some("https://search.example.test/api")
    );
    assert_eq!(config.public_search.timeout_ms, 3000);
    assert_eq!(config.public_search.max_response_bytes, 65536);
}

#[test]
fn config_load_validates_terminal_limits() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
terminal:
  max_sessions_per_task: 5
  max_sessions_per_user: 4
"#,
    )
    .expect("write config");

    let error = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
        .expect_err("invalid config rejects on load");

    assert!(matches!(
        error,
        ConfigError::InvalidConfig { message }
            if message.contains("terminal.max_sessions_per_task")
    ));
}

#[test]
fn provider_declarations_load_from_file() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
providers:
  zai:
    kind: openai_compatible
    base_url: https://api.z.ai/api/coding/paas/v4
    api_key_env: ZAI_API_KEY
    models:
      - model: glm-5.3
        context_tokens: 1000000
        max_output_tokens: 131072
      - model: glm-4.7
  google:
    kind: gemini
    api_key: test-key
    models:
      - model: gemini-3.8-flash
"#,
    )
    .expect("write config");

    let config = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
        .expect("provider config loads");

    assert_eq!(config.providers.entries.len(), 2);
    let zai = &config.providers.entries["zai"];
    assert_eq!(zai.kind, "openai_compatible");
    assert_eq!(
        zai.base_url.as_deref(),
        Some("https://api.z.ai/api/coding/paas/v4")
    );
    assert!(zai.api_key.is_none());
    assert_eq!(zai.api_key_env.as_deref(), Some("ZAI_API_KEY"));
    assert_eq!(zai.models.len(), 2);
    assert_eq!(zai.models[0].model, "glm-5.3");
    assert_eq!(zai.models[0].context_tokens, Some(1_000_000));
    assert_eq!(zai.models[0].max_output_tokens, Some(131_072));
    assert_eq!(zai.models[1].model, "glm-4.7");

    let google = &config.providers.entries["google"];
    assert_eq!(google.kind, "gemini");
    assert_eq!(google.api_key.as_deref(), Some("test-key"));
    assert!(google.base_url.is_none());
    assert_eq!(google.models.len(), 1);
    assert_eq!(google.models[0].model, "gemini-3.8-flash");
}

#[test]
fn provider_declarations_reject_invalid_kinds_and_credential_shapes() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let cases: [(&str, &str, &str); 4] = [
        (
            "unknown kind",
            "providers:\n  zai:\n    kind: anthropic\n    api_key: k\n    models: [{model: m}]\n",
            "unsupported kind",
        ),
        (
            "missing credential",
            "providers:\n  zai:\n    kind: openai\n    models: [{model: m}]\n",
            "must set api_key or api_key_env",
        ),
        (
            "both credentials",
            "providers:\n  zai:\n    kind: openai\n    api_key: k\n    api_key_env: K\n    models: [{model: m}]\n",
            "pick one",
        ),
        (
            "openai_compatible without base_url",
            "providers:\n  zai:\n    kind: openai_compatible\n    api_key: k\n    models: [{model: m}]\n",
            "must set base_url",
        ),
    ];
    for (name, body, expected) in cases {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("forge.yaml");
        fs::write(&config_path, body).expect("write config");
        let error = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
            .expect_err("invalid provider declaration rejects on load");
        assert!(
            matches!(
                &error,
                ConfigError::InvalidConfig { message } if message.contains(expected)
            ),
            "{name}: unexpected error {error:?}"
        );
    }
}

#[test]
fn precedence_is_cli_over_env_over_file_over_defaults() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
forge:
  data_dir: /file/data
server:
  bind: 127.0.0.1:9000
  public_base_url: https://file.example.com/app
workspace:
  root: /file/worktrees
  cleanup_delay_seconds: 10
agent:
  max_concurrent_tasks: 2
  heartbeat_interval_seconds: 15
  max_missed_heartbeats: 4
project:
  default_priority: normal
"#
        .replace(
            "/file/data",
            &dir.path().join("file-data").to_string_lossy(),
        )
        .replace(
            "/file/worktrees",
            &dir.path().join("file-worktrees").to_string_lossy(),
        ),
    )
    .expect("write config");

    env::set_var("FORGE_SERVER_BIND", "127.0.0.1:9100");
    env::set_var("FORGE_PUBLIC_BASE_URL", "https://env.example.com/app");
    env::set_var("FORGE_DATA_DIR", dir.path().join("env-data"));
    env::set_var("FORGE_WORKSPACE_ROOT", dir.path().join("env-worktrees"));
    env::set_var("FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS", "20");
    env::set_var("FORGE_AGENT_MAX_CONCURRENT_TASKS", "3");
    env::set_var("FORGE_AGENT_HEARTBEAT_INTERVAL_SECONDS", "25");
    env::set_var("FORGE_AGENT_MAX_MISSED_HEARTBEATS", "5");

    let config = ForgeConfig::load(
        Some(&config_path),
        ConfigOverrides {
            server_bind: Some("127.0.0.1:9200".to_owned()),
            server_public_base_url: Some("https://cli.example.com/app".to_owned()),
            mcp_enabled: None,
            data_dir: Some(dir.path().join("cli-data")),
            workspace_root: Some(dir.path().join("cli-worktrees")),
            workspace_cleanup_delay_seconds: Some(30),
            agent_max_concurrent_tasks: Some(4),
            agent_heartbeat_interval_seconds: Some(35),
            agent_max_missed_heartbeats: Some(6),
            ..Default::default()
        },
    )
    .expect("config loads");

    assert_eq!(config.server.bind, "127.0.0.1:9200");
    assert_eq!(
        config.server.public_base_url.as_deref(),
        Some("https://cli.example.com/app")
    );
    assert_eq!(config.forge.data_dir, dir.path().join("cli-data"));
    assert_eq!(config.db_path(), dir.path().join("cli-data/forge.db"));
    assert_eq!(config.workspace.root, dir.path().join("cli-worktrees"));
    assert_eq!(config.workspace.cleanup_delay_seconds, 30);
    assert_eq!(config.agent.max_concurrent_tasks, 4);
    assert_eq!(config.agent.heartbeat_interval_seconds, 35);
    assert_eq!(config.agent.max_missed_heartbeats, 6);
    assert_eq!(
        config.project.values.get("default_priority"),
        Some(&"normal".to_owned())
    );

    clear_forge_env();
}

#[test]
fn trusted_origin_uses_bind_when_public_base_url_is_absent() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.bind = "127.0.0.1:8080".to_owned();
    config.server.public_base_url = None;

    assert_eq!(config.trusted_origin(), "http://127.0.0.1:8080");
}

#[test]
fn trusted_origin_strips_public_base_url_path() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.bind = "127.0.0.1:8080".to_owned();
    config.server.public_base_url = Some("https://forge.example.com/something".to_owned());

    assert_eq!(config.trusted_origin(), "https://forge.example.com");
}

#[test]
fn mcp_resource_url_appends_mcp_to_trusted_origin() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.public_base_url = Some("https://forge.example.com/something".to_owned());

    assert_eq!(config.mcp_resource_url(), "https://forge.example.com/mcp");
}

#[test]
fn mcp_enabled_defaults_to_true_and_can_be_overridden() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let missing_path = dir.path().join("missing-forge.yaml");
    let default_config = ForgeConfig::load(Some(&missing_path), test_overrides(dir.path()))
        .expect("default config loads");
    assert!(default_config.server.mcp_enabled);

    let overridden = ForgeConfig::load(
        Some(&missing_path),
        ConfigOverrides {
            mcp_enabled: Some(false),
            ..test_overrides(dir.path())
        },
    )
    .expect("config loads with override");
    assert!(!overridden.server.mcp_enabled);
}

#[test]
fn env_overrides_file_when_cli_override_is_absent() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
forge:
  data_dir: /file/data
server:
  public_base_url: https://file.example.com/app
workspace:
  root: /file/worktrees
"#
        .replace(
            "/file/data",
            &dir.path().join("file-data").to_string_lossy(),
        )
        .replace(
            "/file/worktrees",
            &dir.path().join("file-worktrees").to_string_lossy(),
        ),
    )
    .expect("write config");

    env::set_var("FORGE_DATA_DIR", dir.path().join("env-data"));
    env::set_var("FORGE_PUBLIC_BASE_URL", "https://env.example.com/app");
    env::set_var("FORGE_WORKSPACE_ROOT", dir.path().join("env-worktrees"));

    let config =
        ForgeConfig::load(Some(&config_path), ConfigOverrides::default()).expect("config loads");

    assert_eq!(config.forge.data_dir, dir.path().join("env-data"));
    assert_eq!(
        config.server.public_base_url.as_deref(),
        Some("https://env.example.com/app")
    );
    assert_eq!(config.workspace.root, dir.path().join("env-worktrees"));

    clear_forge_env();
}

#[test]
fn file_overrides_defaults_when_no_env_or_cli_override_exists() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        r#"
forge:
  data_dir: /file/data
server:
  bind: 127.0.0.1:9000
  public_base_url: https://file.example.com/app
workspace:
  root: /file/worktrees
"#
        .replace(
            "/file/data",
            &dir.path().join("file-data").to_string_lossy(),
        )
        .replace(
            "/file/worktrees",
            &dir.path().join("file-worktrees").to_string_lossy(),
        ),
    )
    .expect("write config");

    let config =
        ForgeConfig::load(Some(&config_path), ConfigOverrides::default()).expect("config loads");

    assert_eq!(config.server.bind, "127.0.0.1:9000");
    assert_eq!(
        config.server.public_base_url.as_deref(),
        Some("https://file.example.com/app")
    );
    assert_eq!(config.forge.data_dir, dir.path().join("file-data"));
    assert_eq!(config.db_path(), dir.path().join("file-data/forge.db"));
    assert_eq!(config.workspace.root, dir.path().join("file-worktrees"));
}

#[test]
fn data_dir_from_env_expands_forge_data_dir() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let home = tempdir().expect("home tempdir");
    let previous_home = env::var_os("HOME");

    env::set_var("HOME", home.path());
    env::set_var("FORGE_DATA_DIR", "~/forge-data-from-env");

    assert_eq!(data_dir_from_env(), home.path().join("forge-data-from-env"));

    if let Some(previous_home) = previous_home {
        env::set_var("HOME", previous_home);
    } else {
        env::remove_var("HOME");
    }
    clear_forge_env();
}

#[test]
fn server_state_round_trips_in_data_dir() {
    let dir = tempdir().expect("tempdir");
    let state = ServerState::new("127.0.0.1:49152", "http://127.0.0.1:49152");

    assert_eq!(read_server_state(dir.path()).expect("state reads"), None);

    write_server_state(dir.path(), &state).expect("state writes");

    assert_eq!(
        read_server_state(dir.path()).expect("state reads"),
        Some(state)
    );
    assert!(server_state_path(dir.path()).ends_with("server.json"));
}

#[test]
fn resolve_jwt_secret_uses_config_value_when_set() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.jwt_secret = Some("configured-secret-value".to_owned());

    let secret = config.resolve_jwt_secret().expect("secret resolves");
    assert_eq!(secret, b"configured-secret-value");
    assert!(!config.jwt_secret_path().exists());
}

#[test]
fn resolve_jwt_secret_reads_persisted_file_when_config_is_unset() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    let secret_path = config.jwt_secret_path();
    let persisted = vec![7_u8; 32];
    fs::write(&secret_path, &persisted).expect("write secret file");

    let secret = config.resolve_jwt_secret().expect("secret resolves");
    assert_eq!(secret, persisted);
}

#[test]
fn resolve_jwt_secret_generates_persists_and_reuses_secret_file() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    let secret_path = config.jwt_secret_path();
    assert!(!secret_path.exists());

    let first = config.resolve_jwt_secret().expect("first resolve");
    assert!(secret_path.is_file());
    assert_eq!(first.len(), 32);
    assert_eq!(fs::read(&secret_path).expect("read secret file"), first);

    let second = config.resolve_jwt_secret().expect("second resolve");
    assert_eq!(second, first);
}

#[cfg(unix)]
#[test]
fn resolve_jwt_secret_persists_file_with_restricted_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let config = ForgeConfig::with_data_dir(dir.path().to_path_buf());

    config.resolve_jwt_secret().expect("secret resolves");

    let mode = fs::metadata(config.jwt_secret_path())
        .expect("secret file metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn trusted_web_origins_include_the_serving_origin_with_both_loopback_spellings() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.bind = "127.0.0.1:8080".to_owned();

    let origins = config.trusted_web_origins();

    assert!(origins.contains(&"http://localhost:5173".to_owned()));
    assert!(origins.contains(&"http://127.0.0.1:8080".to_owned()));
    assert!(origins.contains(&"http://localhost:8080".to_owned()));
}

#[test]
fn trusted_web_origins_use_the_public_base_url_origin_when_configured() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.server.public_base_url = Some("https://forge.example.com/app".to_owned());
    config.server.cors_origins = vec!["https://ui.example.com".to_owned()];

    let origins = config.trusted_web_origins();

    assert_eq!(
        origins,
        vec![
            "https://ui.example.com".to_owned(),
            "https://forge.example.com".to_owned(),
        ]
    );
}

fn clear_forge_env() {
    for key in [
        "FORGE_SERVER_CHECK_RUN_TIMEOUT_SECONDS",
        "FORGE_SERVER_USAGE_INDEX_BUDGET_MB",
        "FORGE_EVENT_CONSUMER_STALL_SECONDS",
        "FORGE_SERVER_BIND",
        "FORGE_SERVER_BUILD_JOBS_PER_RUN",
        "FORGE_SERVER_RUN_NICE",
        "FORGE_PUBLIC_BASE_URL",
        "FORGE_PUBLIC_SEARCH_ENDPOINT",
        "FORGE_PUBLIC_SEARCH_TIMEOUT_MS",
        "FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES",
        "FORGE_DATA_DIR",
        "FORGE_WORKSPACE_ROOT",
        "FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS",
        "FORGE_MAX_DISCONNECT_SECONDS",
        "FORGE_WORKSPACE_LOG_RETENTION_DAYS",
        "FORGE_WORKSPACE_MIN_FREE_BYTES",
        "FORGE_WORKSPACE_MIN_FREE_PERCENT",
        "FORGE_WORKSPACE_MIN_FREE_INODE_PERCENT",
        "FORGE_WORKSPACE_GC_FREE_BYTES",
        "FORGE_WORKSPACE_GC_FREE_PERCENT",
        "FORGE_WORKSPACE_COMPILER_CACHE_WRAPPER",
        "FORGE_WORKSPACE_COMPILER_CACHE_MAX_BYTES",
        "FORGE_WORKSPACE_COMPILER_CACHE_DIR",
        "FORGE_AGENT_MAX_CONCURRENT_TASKS",
        "FORGE_AGENT_HEARTBEAT_INTERVAL_SECONDS",
        "FORGE_AGENT_MAX_MISSED_HEARTBEATS",
        "FORGE_JWT_SECRET",
        "FORGE_BCRYPT_COST",
        "FORGE_CORS_ORIGINS",
        "FORGE_MEDIA_UPLOAD_LIMIT_BYTES",
        "FORGE_SCAFFOLD_COMMAND",
    ] {
        env::remove_var(key);
    }
}

#[test]
fn max_disconnect_defaults_and_obeys_file_env_override_precedence() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("forge.yaml");
    assert_eq!(
        ForgeConfig::with_data_dir(dir.path().to_path_buf())
            .workspace
            .max_disconnect_seconds,
        24 * 60 * 60
    );
    fs::write(&path, "workspace:\n  max_disconnect_seconds: 600\n").expect("config writes");
    let loaded = ForgeConfig::load(Some(&path), test_overrides(dir.path())).expect("file loads");
    assert_eq!(loaded.workspace.max_disconnect_seconds, 600);
    env::set_var("FORGE_MAX_DISCONNECT_SECONDS", "900");
    let loaded = ForgeConfig::load(Some(&path), test_overrides(dir.path())).expect("env loads");
    assert_eq!(loaded.workspace.max_disconnect_seconds, 900);
    let loaded = ForgeConfig::load(
        Some(&path),
        ConfigOverrides {
            workspace_max_disconnect_seconds: Some(1200),
            ..test_overrides(dir.path())
        },
    )
    .expect("override loads");
    assert_eq!(loaded.workspace.max_disconnect_seconds, 1200);
    clear_forge_env();
}

#[test]
fn compiler_cache_is_off_by_default_and_obeys_file_then_env() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("forge.yaml");
    let defaults = ForgeConfig::with_data_dir(dir.path().to_path_buf())
        .workspace
        .compiler_cache;
    assert_eq!(
        (defaults.wrapper, defaults.max_bytes, defaults.dir),
        (None, 20 * 1024 * 1024 * 1024, None)
    );
    fs::write(
        &path,
        "workspace:\n  compiler_cache:\n    wrapper: sccache\n    max_bytes: 5000\n    dir: /var/forge-cache\n",
    )
    .expect("config writes");
    let cache = ForgeConfig::load(Some(&path), test_overrides(dir.path()))
        .expect("file loads")
        .workspace
        .compiler_cache;
    assert_eq!(cache.wrapper.as_deref(), Some("sccache"));
    assert_eq!(cache.max_bytes, 5000);
    assert_eq!(
        cache.dir,
        Some(std::path::PathBuf::from("/var/forge-cache"))
    );

    env::set_var("FORGE_WORKSPACE_COMPILER_CACHE_WRAPPER", "/opt/bin/kache");
    env::set_var("FORGE_WORKSPACE_COMPILER_CACHE_MAX_BYTES", "7000");
    env::set_var("FORGE_WORKSPACE_COMPILER_CACHE_DIR", "/srv/cache");
    let cache = ForgeConfig::load(Some(&path), test_overrides(dir.path()))
        .expect("env loads")
        .workspace
        .compiler_cache;
    assert_eq!(cache.wrapper.as_deref(), Some("/opt/bin/kache"));
    assert_eq!(cache.max_bytes, 7000);
    assert_eq!(cache.dir, Some(std::path::PathBuf::from("/srv/cache")));
    env::set_var("FORGE_WORKSPACE_COMPILER_CACHE_MAX_BYTES", "many");
    assert!(ForgeConfig::load(Some(&path), test_overrides(dir.path())).is_err());
    clear_forge_env();

    // An empty wrapper in the file is the feature left off.
    fs::write(&path, "workspace:\n  compiler_cache:\n    wrapper: \"\"\n").expect("config writes");
    let cache = ForgeConfig::load(Some(&path), test_overrides(dir.path()))
        .expect("file loads")
        .workspace
        .compiler_cache;
    assert_eq!(cache.wrapper, None);
}

#[test]
fn workspace_gc_keys_default_and_obey_file_then_env() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("forge.yaml");
    let defaults = ForgeConfig::with_data_dir(dir.path().to_path_buf()).workspace;
    assert_eq!(
        (
            defaults.log_retention_days,
            defaults.min_free_bytes,
            defaults.min_free_percent
        ),
        (30, 10 * 1024 * 1024 * 1024, 5)
    );
    fs::write(
        &path,
        "workspace:\n  log_retention_days: 7\n  min_free_bytes: 1000\n  min_free_percent: 9\n",
    )
    .expect("config writes");
    let loaded = ForgeConfig::load(Some(&path), test_overrides(dir.path())).expect("file loads");
    assert_eq!(
        (
            loaded.workspace.log_retention_days,
            loaded.workspace.min_free_bytes,
            loaded.workspace.min_free_percent
        ),
        (7, 1000, 9)
    );
    env::set_var("FORGE_WORKSPACE_LOG_RETENTION_DAYS", "0");
    env::set_var("FORGE_WORKSPACE_MIN_FREE_BYTES", "2000");
    env::set_var("FORGE_WORKSPACE_MIN_FREE_PERCENT", "12");
    let loaded = ForgeConfig::load(Some(&path), test_overrides(dir.path())).expect("env loads");
    assert_eq!(
        (
            loaded.workspace.log_retention_days,
            loaded.workspace.min_free_bytes,
            loaded.workspace.min_free_percent
        ),
        (0, 2000, 12)
    );
    env::set_var("FORGE_WORKSPACE_MIN_FREE_PERCENT", "300");
    assert!(ForgeConfig::load(Some(&path), test_overrides(dir.path())).is_err());
    clear_forge_env();

    // The inode floor and the mark at which the collector runs at once.
    let floor = defaults;
    assert_eq!(
        (
            floor.min_free_inode_percent,
            floor.gc_free_bytes,
            floor.gc_free_percent
        ),
        (5, None, None)
    );
    fs::write(
        &path,
        "workspace:\n  min_free_inode_percent: 3\n  gc_free_bytes: 4000\n  gc_free_percent: 20\n",
    )
    .expect("config writes");
    let floor = ForgeConfig::load(Some(&path), test_overrides(dir.path()))
        .expect("file loads")
        .workspace;
    assert_eq!(
        (
            floor.min_free_inode_percent,
            floor.gc_free_bytes,
            floor.gc_free_percent
        ),
        (3, Some(4000), Some(20))
    );
    env::set_var("FORGE_WORKSPACE_MIN_FREE_INODE_PERCENT", "7");
    env::set_var("FORGE_WORKSPACE_GC_FREE_BYTES", "9000");
    env::set_var("FORGE_WORKSPACE_GC_FREE_PERCENT", "25");
    let floor = ForgeConfig::load(Some(&path), test_overrides(dir.path()))
        .expect("env loads")
        .workspace;
    assert_eq!(
        (
            floor.min_free_inode_percent,
            floor.gc_free_bytes,
            floor.gc_free_percent
        ),
        (7, Some(9000), Some(25))
    );
    env::set_var("FORGE_WORKSPACE_GC_FREE_PERCENT", "101");
    assert!(ForgeConfig::load(Some(&path), test_overrides(dir.path())).is_err());
    clear_forge_env();
}

#[test]
fn max_disconnect_rejects_zero() {
    let dir = tempdir().expect("test data directory");
    let mut config = ForgeConfig::with_data_dir(dir.path().to_path_buf());
    config.workspace.max_disconnect_seconds = 0;
    assert!(
        matches!(config.validate(), Err(ConfigError::InvalidConfig { message })
        if message.contains("max_disconnect_seconds"))
    );
}

#[test]
fn scaffold_command_defaults_then_file_then_env() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let missing_path = dir.path().join("missing-forge.yaml");
    let config = ForgeConfig::load(Some(&missing_path), test_overrides(dir.path()))
        .expect("default config loads");
    assert_eq!(config.scaffold.command, crate::DEFAULT_SCAFFOLD_COMMAND);

    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        "scaffold:\n  command: bun /opt/spark/packages/create-spark/src/cli.ts\n",
    )
    .expect("write config");
    let config = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
        .expect("file config loads");
    assert_eq!(
        config.scaffold.command,
        "bun /opt/spark/packages/create-spark/src/cli.ts"
    );

    env::set_var("FORGE_SCAFFOLD_COMMAND", "/usr/local/bin/create-spark-fake");
    let config = ForgeConfig::load(Some(&config_path), test_overrides(dir.path()))
        .expect("env config loads");
    assert_eq!(config.scaffold.command, "/usr/local/bin/create-spark-fake");

    clear_forge_env();
}

#[test]
fn consumer_stall_setting_follows_config_precedence_and_validates() {
    let _guard = env_lock().lock().expect("env lock");
    clear_forge_env();
    let dir = tempdir().unwrap();
    let path = dir.path().join("forge.yaml");
    let defaults = ForgeConfig::load(Some(&path), test_overrides(dir.path())).unwrap();
    assert_eq!(defaults.server.event_consumer_stall_seconds, 300);
    fs::write(&path, "server:\n  event_consumer_stall_seconds: 600\n").unwrap();
    let file = ForgeConfig::load(Some(&path), test_overrides(dir.path())).unwrap();
    assert_eq!(file.server.event_consumer_stall_seconds, 600);
    env::set_var("FORGE_EVENT_CONSUMER_STALL_SECONDS", "60");
    let env_config = ForgeConfig::load(Some(&path), test_overrides(dir.path())).unwrap();
    assert_eq!(env_config.server.event_consumer_stall_seconds, 60);
    let cli = ForgeConfig::load(
        Some(&path),
        ConfigOverrides {
            event_consumer_stall_seconds: Some(120),
            ..test_overrides(dir.path())
        },
    )
    .unwrap();
    assert_eq!(cli.server.event_consumer_stall_seconds, 120);
    clear_forge_env();
    assert!(ForgeConfig::load(
        Some(&path),
        ConfigOverrides {
            event_consumer_stall_seconds: Some(0),
            ..test_overrides(dir.path())
        }
    )
    .is_err());
}

fn test_overrides(data_dir: &std::path::Path) -> ConfigOverrides {
    ConfigOverrides {
        data_dir: Some(data_dir.to_path_buf()),
        ..Default::default()
    }
}

#[test]
#[should_panic(expected = "a test tried to use the real Forge data directory")]
fn default_data_dir_tripwire_panics_in_test_binary() {
    assert_eq!(
        env::var("FORGE_TEST_FORBID_DEFAULT_DATA_DIR").as_deref(),
        Ok("1")
    );
    let _ = default_data_dir();
}

#[test]
fn usage_index_budget_obeys_file_env_cli_precedence() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let dir = tempdir().unwrap();
    let path = dir.path().join("forge.yaml");
    let load = |budget| {
        ForgeConfig::load(
            Some(&path),
            ConfigOverrides {
                server_usage_index_budget_mb: budget,
                ..test_overrides(dir.path())
            },
        )
    };
    assert_eq!(load(None).unwrap().server.usage_index_budget_mb, None);
    assert_eq!(crate::DEFAULT_USAGE_INDEX_BUDGET_MB, 128);
    fs::write(&path, "server:\n  usage_index_budget_mb: 64\n").unwrap();
    assert_eq!(load(None).unwrap().server.usage_index_budget_mb, Some(64));
    env::set_var("FORGE_SERVER_USAGE_INDEX_BUDGET_MB", "256");
    assert_eq!(load(None).unwrap().server.usage_index_budget_mb, Some(256));
    assert_eq!(load(Some(0)).unwrap().server.usage_index_budget_mb, Some(0));
    env::set_var("FORGE_SERVER_USAGE_INDEX_BUDGET_MB", "0");
    assert_eq!(load(None).unwrap().server.usage_index_budget_mb, Some(0));
    assert_eq!(
        load(Some(32)).unwrap().server.usage_index_budget_mb,
        Some(32)
    );
    for invalid in ["-1", "1.5", "4294967296", "no"] {
        env::set_var("FORGE_SERVER_USAGE_INDEX_BUDGET_MB", invalid);
        assert!(load(None).is_err(), "{invalid}");
    }
    clear_forge_env();
    for invalid in ["-1", "1.5", "4294967296"] {
        fs::write(
            &path,
            format!("server:\n  usage_index_budget_mb: {invalid}\n"),
        )
        .unwrap();
        assert!(load(None).is_err(), "{invalid}");
    }
}

#[test]
fn run_budget_file_env_flag_precedence_and_validation() {
    let _guard = env_lock().lock().unwrap();
    clear_forge_env();
    let dir = tempdir().unwrap();
    let path = dir.path().join("forge.yaml");
    fs::write(&path, "server:\n  build_jobs_per_run: 3\n  run_nice: 7\n").unwrap();
    let load = |jobs, nice| {
        ForgeConfig::load(
            Some(&path),
            ConfigOverrides {
                server_build_jobs_per_run: jobs,
                server_run_nice: nice,
                ..test_overrides(dir.path())
            },
        )
    };
    let cfg = load(None, None).unwrap();
    assert_eq!(cfg.server.build_jobs_per_run, Some(3));
    assert_eq!(cfg.server.run_nice, 7);
    env::set_var("FORGE_SERVER_BUILD_JOBS_PER_RUN", "0");
    env::set_var("FORGE_SERVER_RUN_NICE", "0");
    let cfg = load(None, None).unwrap();
    assert_eq!(cfg.server.build_jobs_per_run, Some(0));
    assert_eq!(cfg.server.run_nice, 0);
    let cfg = load(Some(5), Some(19)).unwrap();
    assert_eq!(cfg.server.build_jobs_per_run, Some(5));
    assert_eq!(cfg.server.run_nice, 19);
    assert!(load(None, Some(20)).is_err());
    env::set_var("FORGE_SERVER_RUN_NICE", "20");
    assert!(load(None, None).is_err());
    env::set_var("FORGE_SERVER_BUILD_JOBS_PER_RUN", "-1");
    assert!(load(None, None).is_err());
    clear_forge_env();
    fs::write(&path, "{}").unwrap();
    let cfg = load(None, None).unwrap();
    assert_eq!(cfg.server.build_jobs_per_run, None);
    assert_eq!(cfg.server.run_nice, 10);
    fs::write(&path, "server:\n  build_jobs_per_run: 0\n  run_nice: 0\n").unwrap();
    let cfg = load(None, None).unwrap();
    assert_eq!(cfg.server.build_jobs_per_run, Some(0));
    assert_eq!(cfg.server.run_nice, 0);
}

#[test]
fn working_set_file_and_env_settings_are_validated() {
    let _guard = env_lock().lock().unwrap();
    clear_forge_env();
    let directory = tempdir().unwrap();
    let path = directory.path().join("forge.yaml");
    fs::write(&path, "server:\n  main_working_set_target_tokens: 32000\n  main_working_set_hard_tokens: 48000\n  project_working_set_target_tokens: 64000\n  project_working_set_hard_tokens: 96000\n").unwrap();
    let config = ForgeConfig::load(Some(&path), test_overrides(directory.path())).unwrap();
    assert_eq!(config.server.main_working_set_target_tokens, 32000);
    assert_eq!(config.server.project_working_set_hard_tokens, 96000);
    env::set_var("FORGE_SERVER_MAIN_WORKING_SET_HARD_TOKENS", "16000");
    assert!(ForgeConfig::load(Some(&path), test_overrides(directory.path())).is_err());
    env::remove_var("FORGE_SERVER_MAIN_WORKING_SET_HARD_TOKENS");
}

#[test]
fn check_run_timeout_obeys_file_env_cli_precedence_and_rejects_zero() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();
    let dir = tempdir().unwrap();
    let path = dir.path().join("forge.yaml");
    let load = |seconds| {
        ForgeConfig::load(
            Some(&path),
            ConfigOverrides {
                server_check_run_timeout_seconds: seconds,
                ..test_overrides(dir.path())
            },
        )
    };
    assert_eq!(
        load(None).unwrap().server.check_run_timeout_seconds,
        crate::DEFAULT_CHECK_RUN_TIMEOUT_SECONDS
    );
    fs::write(&path, "server:\n  check_run_timeout_seconds: 90\n").unwrap();
    assert_eq!(load(None).unwrap().server.check_run_timeout_seconds, 90);
    env::set_var("FORGE_SERVER_CHECK_RUN_TIMEOUT_SECONDS", "120");
    assert_eq!(load(None).unwrap().server.check_run_timeout_seconds, 120);
    assert_eq!(
        load(Some(180)).unwrap().server.check_run_timeout_seconds,
        180
    );
    assert!(load(Some(0)).is_err());
    for invalid in ["0", "-1", "1.5", "4294967296", "no"] {
        env::set_var("FORGE_SERVER_CHECK_RUN_TIMEOUT_SECONDS", invalid);
        assert!(load(None).is_err(), "{invalid}");
    }
    env::remove_var("FORGE_SERVER_CHECK_RUN_TIMEOUT_SECONDS");
    fs::write(&path, "server:\n  check_run_timeout_seconds: 0\n").unwrap();
    assert!(load(None).is_err());
    // A partial serde config retains the same default as the loader.
    let config: crate::ServerConfig =
        serde_yaml::from_str("bind: '127.0.0.1:0'\njwt_secret: null").unwrap();
    assert_eq!(
        config.check_run_timeout_seconds,
        crate::DEFAULT_CHECK_RUN_TIMEOUT_SECONDS
    );
    clear_forge_env();
}
