use crate::{
    data_dir_from_env, default_data_dir, default_workspace_root, read_server_state,
    server_state_path, write_server_state, ConfigError, ConfigOverrides, ForgeConfig,
    PublicSearchConfig, ServerState, TerminalConfig, DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES,
    DEFAULT_SERVER_BIND, DEFAULT_WORKSPACE_CLEANUP_DELAY_SECONDS,
};
use std::{
    env, fs,
    path::PathBuf,
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

    let missing_path = tempdir()
        .expect("tempdir")
        .path()
        .join("missing-forge.yaml");
    let config = ForgeConfig::load(Some(&missing_path), ConfigOverrides::default())
        .expect("default config loads");

    assert_eq!(config.server.bind, DEFAULT_SERVER_BIND);
    assert!(config.server.mcp_enabled);
    assert_eq!(
        config.server.media_upload_limit_bytes,
        DEFAULT_MEDIA_UPLOAD_LIMIT_BYTES
    );
    assert_eq!(config.forge.data_dir, default_data_dir());
    assert_eq!(config.db_path(), default_data_dir().join("forge.db"));
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
        ForgeConfig::load(Some(&config_path), ConfigOverrides::default()).expect("config loads");

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

    let config = ForgeConfig::load(Some(&config_path), ConfigOverrides::default())
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

    let error = ForgeConfig::load(Some(&config_path), ConfigOverrides::default())
        .expect_err("invalid config rejects on load");

    assert!(matches!(
        error,
        ConfigError::InvalidConfig { message }
            if message.contains("terminal.max_sessions_per_task")
    ));
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
"#,
    )
    .expect("write config");

    env::set_var("FORGE_SERVER_BIND", "127.0.0.1:9100");
    env::set_var("FORGE_PUBLIC_BASE_URL", "https://env.example.com/app");
    env::set_var("FORGE_DATA_DIR", "/env/data");
    env::set_var("FORGE_WORKSPACE_ROOT", "/env/worktrees");
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
            data_dir: Some(PathBuf::from("/cli/data")),
            workspace_root: Some(PathBuf::from("/cli/worktrees")),
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
    assert_eq!(config.forge.data_dir, PathBuf::from("/cli/data"));
    assert_eq!(config.db_path(), PathBuf::from("/cli/data/forge.db"));
    assert_eq!(config.workspace.root, PathBuf::from("/cli/worktrees"));
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
    let mut config = ForgeConfig::default();
    config.server.bind = "127.0.0.1:8080".to_owned();
    config.server.public_base_url = None;

    assert_eq!(config.trusted_origin(), "http://127.0.0.1:8080");
}

#[test]
fn trusted_origin_strips_public_base_url_path() {
    let mut config = ForgeConfig::default();
    config.server.bind = "127.0.0.1:8080".to_owned();
    config.server.public_base_url = Some("https://forge.example.com/something".to_owned());

    assert_eq!(config.trusted_origin(), "https://forge.example.com");
}

#[test]
fn mcp_resource_url_appends_mcp_to_trusted_origin() {
    let mut config = ForgeConfig::default();
    config.server.public_base_url = Some("https://forge.example.com/something".to_owned());

    assert_eq!(config.mcp_resource_url(), "https://forge.example.com/mcp");
}

#[test]
fn mcp_enabled_defaults_to_true_and_can_be_overridden() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let missing_path = tempdir()
        .expect("tempdir")
        .path()
        .join("missing-forge.yaml");
    let default_config = ForgeConfig::load(Some(&missing_path), ConfigOverrides::default())
        .expect("default config loads");
    assert!(default_config.server.mcp_enabled);

    let overridden = ForgeConfig::load(
        Some(&missing_path),
        ConfigOverrides {
            mcp_enabled: Some(false),
            ..Default::default()
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
"#,
    )
    .expect("write config");

    env::set_var("FORGE_DATA_DIR", "/env/data");
    env::set_var("FORGE_PUBLIC_BASE_URL", "https://env.example.com/app");
    env::set_var("FORGE_WORKSPACE_ROOT", "/env/worktrees");

    let config =
        ForgeConfig::load(Some(&config_path), ConfigOverrides::default()).expect("config loads");

    assert_eq!(config.forge.data_dir, PathBuf::from("/env/data"));
    assert_eq!(
        config.server.public_base_url.as_deref(),
        Some("https://env.example.com/app")
    );
    assert_eq!(config.workspace.root, PathBuf::from("/env/worktrees"));

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
"#,
    )
    .expect("write config");

    let config =
        ForgeConfig::load(Some(&config_path), ConfigOverrides::default()).expect("config loads");

    assert_eq!(config.server.bind, "127.0.0.1:9000");
    assert_eq!(
        config.server.public_base_url.as_deref(),
        Some("https://file.example.com/app")
    );
    assert_eq!(config.forge.data_dir, PathBuf::from("/file/data"));
    assert_eq!(config.db_path(), PathBuf::from("/file/data/forge.db"));
    assert_eq!(config.workspace.root, PathBuf::from("/file/worktrees"));
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
    let mut config = ForgeConfig::default();
    config.forge.data_dir = dir.path().to_path_buf();
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
    let mut config = ForgeConfig::default();
    config.forge.data_dir = dir.path().to_path_buf();
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
    let mut config = ForgeConfig::default();
    config.forge.data_dir = dir.path().to_path_buf();
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
    let mut config = ForgeConfig::default();
    config.forge.data_dir = dir.path().to_path_buf();

    config.resolve_jwt_secret().expect("secret resolves");

    let mode = fs::metadata(config.jwt_secret_path())
        .expect("secret file metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

#[test]
fn trusted_web_origins_include_the_serving_origin_with_both_loopback_spellings() {
    let mut config = ForgeConfig::default();
    config.server.bind = "127.0.0.1:8080".to_owned();

    let origins = config.trusted_web_origins();

    assert!(origins.contains(&"http://localhost:5173".to_owned()));
    assert!(origins.contains(&"http://127.0.0.1:8080".to_owned()));
    assert!(origins.contains(&"http://localhost:8080".to_owned()));
}

#[test]
fn trusted_web_origins_use_the_public_base_url_origin_when_configured() {
    let mut config = ForgeConfig::default();
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
        "FORGE_SERVER_BIND",
        "FORGE_PUBLIC_BASE_URL",
        "FORGE_PUBLIC_SEARCH_ENDPOINT",
        "FORGE_PUBLIC_SEARCH_TIMEOUT_MS",
        "FORGE_PUBLIC_SEARCH_MAX_RESPONSE_BYTES",
        "FORGE_DATA_DIR",
        "FORGE_WORKSPACE_ROOT",
        "FORGE_WORKSPACE_CLEANUP_DELAY_SECONDS",
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
fn scaffold_command_defaults_then_file_then_env() {
    let _guard = env_lock().lock().expect("env lock poisoned");
    clear_forge_env();

    let dir = tempdir().expect("tempdir");
    let missing_path = dir.path().join("missing-forge.yaml");
    let config = ForgeConfig::load(Some(&missing_path), ConfigOverrides::default())
        .expect("default config loads");
    assert_eq!(config.scaffold.command, crate::DEFAULT_SCAFFOLD_COMMAND);

    let config_path = dir.path().join("forge.yaml");
    fs::write(
        &config_path,
        "scaffold:\n  command: bun /opt/spark/packages/create-spark/src/cli.ts\n",
    )
    .expect("write config");
    let config = ForgeConfig::load(Some(&config_path), ConfigOverrides::default())
        .expect("file config loads");
    assert_eq!(
        config.scaffold.command,
        "bun /opt/spark/packages/create-spark/src/cli.ts"
    );

    env::set_var("FORGE_SCAFFOLD_COMMAND", "/usr/local/bin/create-spark-fake");
    let config = ForgeConfig::load(Some(&config_path), ConfigOverrides::default())
        .expect("env config loads");
    assert_eq!(config.scaffold.command, "/usr/local/bin/create-spark-fake");

    clear_forge_env();
}
