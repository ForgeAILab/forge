#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::sync::Arc;

use api::{build_router, AppState};
use api_types::SettingsResponse;
use axum::http::{Method, StatusCode};
use serde_json::json;

use common::{empty_request, TestDir};

#[tokio::test]
async fn settings_endpoint_reads_runtime_and_config_values() {
    let workspace_root = TestDir::new("settings-active-workspaces");
    let config_dir = TestDir::new("settings-config");
    let config_path = config_dir.path().join("forge.yaml");
    std::fs::write(
        &config_path,
        r#"
workspace:
  root: /configured/workspaces
"#,
    )
    .expect("config writes");

    let app = test_app(workspace_root.path(), config_path.clone()).await;
    let admin_token = common::admin_jwt();

    let response: SettingsResponse = common::empty_request_with_bearer(
        &app,
        Method::GET,
        "/api/v1/settings",
        &admin_token,
        StatusCode::OK,
    )
    .await;

    assert_eq!(response.config_path, config_path.to_string_lossy());
    let workspace_root_setting = setting(&response, "workspace.root");
    assert_eq!(
        workspace_root_setting.value,
        json!("/configured/workspaces")
    );
    assert_eq!(
        workspace_root_setting.effective_value,
        json!(workspace_root.path().to_string_lossy())
    );
    assert!(workspace_root_setting.restart_required);
    assert!(response.restart_required);
}

#[tokio::test]
async fn settings_endpoint_updates_forge_yaml() {
    let workspace_root = TestDir::new("settings-update-workspaces");
    let config_dir = TestDir::new("settings-update-config");
    let config_path = config_dir.path().join("forge.yaml");

    let app = test_app(workspace_root.path(), config_path.clone()).await;
    let admin_token = common::admin_jwt();

    let response: SettingsResponse = common::json_request_with_bearer(
        &app,
        Method::PUT,
        "/api/v1/settings",
        &admin_token,
        json!({
            "workspace": {
                "root": "/new/workspaces",
                "cleanup_delay_seconds": 3600
            },
            "agent": {
                "max_concurrent_tasks": 2
            }
        }),
        StatusCode::OK,
    )
    .await;

    assert!(response.restart_required);
    let contents = std::fs::read_to_string(&config_path).expect("config reads");
    let yaml: serde_yaml::Value = serde_yaml::from_str(&contents).expect("config parses");
    assert_eq!(yaml["workspace"]["root"].as_str(), Some("/new/workspaces"));
    assert_eq!(
        yaml["workspace"]["cleanup_delay_seconds"].as_u64(),
        Some(3600)
    );
    assert_eq!(yaml["agent"]["max_concurrent_tasks"].as_u64(), Some(2));
}

#[tokio::test]
async fn settings_endpoint_requires_admin() {
    let workspace_root = TestDir::new("settings-non-admin-workspaces");
    let config_dir = TestDir::new("settings-non-admin-config");
    let config_path = config_dir.path().join("forge.yaml");

    let app = test_app(workspace_root.path(), config_path).await;

    let error: serde_json::Value =
        empty_request(&app, Method::GET, "/api/v1/settings", StatusCode::FORBIDDEN).await;

    assert_eq!(error["code"], "admin_required");
}

fn setting<'a>(response: &'a SettingsResponse, key: &str) -> &'a api_types::ForgeSettingResponse {
    response
        .settings
        .iter()
        .find(|setting| setting.key == key)
        .expect("setting exists")
}

async fn test_app(
    workspace_root: &std::path::Path,
    config_path: std::path::PathBuf,
) -> axum::Router {
    let mut config: serde_yaml::Value = if config_path.exists() {
        serde_yaml::from_str(&std::fs::read_to_string(&config_path).expect("config reads"))
            .expect("config parses")
    } else {
        serde_yaml::from_str("{}").expect("empty config parses")
    };
    config["forge"]["data_dir"] =
        serde_yaml::Value::String(workspace_root.join("data").to_string_lossy().into_owned());
    std::fs::write(
        &config_path,
        serde_yaml::to_string(&config).expect("config serializes"),
    )
    .expect("isolated config writes");

    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::test_support::test_registry());
    let event_bus = Arc::new(events::EventBus::new(16));
    let merge_service = Arc::new(services::MergeService::new_for_test(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));
    let state = AppState::with_adapter_registry_services_and_shutdown(
        db,
        event_bus,
        true,
        adapter_registry,
        merge_service,
        cleanup_scheduler,
        review_runner,
        api::state::ShutdownSignal::new(),
        api::state::test_workflows_dir(),
        api::state::test_jwt_secret(),
        api::state::test_bcrypt_cost(),
    )
    .with_config_path(config_path);

    let web_dist_dir = TestDir::new("settings-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    build_router(state, web_dist_dir.path().to_path_buf())
}
