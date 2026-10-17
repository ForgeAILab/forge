use crate::common;

use api::build_router;
use api_types::SettingsResponse;
use axum::http::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn shared_harness_data_dir_is_owned_across_state_clones() {
    let workspace = tempfile::tempdir().unwrap();
    let harness = common::test_app(workspace.path(), "data-isolation").await;
    let state = std::sync::Arc::clone(&harness.state);
    let root = state.effective_config.forge.data_dir.clone();
    assert!(root.starts_with(std::env::temp_dir()));
    assert!(root.is_dir());
    assert_eq!(state.config_path.as_ref(), &root.join("forge.yaml"));
    drop(harness);
    assert!(
        root.is_dir(),
        "state clones must retain the temporary directory"
    );
    drop(state);
    assert!(
        !root.exists(),
        "the last state must release the temporary directory"
    );
}

#[tokio::test]
async fn settings_of_a_data_dir_launch_report_that_dir_without_a_restart() {
    let workspace = tempfile::tempdir().unwrap();
    let harness = common::test_app(workspace.path(), "settings-launch-data-dir").await;
    let data_dir = harness.state.effective_config.forge.data_dir.clone();
    // `forge --data-dir <dir>`: its forge.yaml sets no `forge.data_dir`.
    std::fs::write(
        data_dir.join("forge.yaml"),
        "agent:\n  max_concurrent_tasks: 7\n",
    )
    .unwrap();
    let state = (*harness.state)
        .clone()
        .with_launch_data_dir(Some(data_dir.clone()));
    assert_eq!(state.config_path.as_ref(), &data_dir.join("forge.yaml"));
    let web = workspace.path().join("web");
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), "<html></html>").unwrap();
    let app = build_router(state, web);
    let response: SettingsResponse = common::empty_request_with_bearer(
        &app,
        Method::GET,
        "/api/v1/settings",
        &common::admin_jwt(),
        StatusCode::OK,
    )
    .await;
    let setting = |key: &str| {
        response
            .settings
            .iter()
            .find(|setting| setting.key == key)
            .unwrap()
            .clone()
    };
    let data = setting("forge.data_dir");
    assert_eq!(data.value, json!(data_dir.display().to_string()));
    assert!(!data.restart_required, "{data:?}");
    // The launch's own forge.yaml is what the settings read.
    assert_eq!(setting("agent.max_concurrent_tasks").value, json!(7));
}
