//! Isolated process: the production demo policy is deliberately irreversible.
mod common;
#[path = "server_command_support/mod.rs"]
mod support;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

fn demo() {
    executors::server_commands::disable();
}

#[tokio::test]
async fn demo_registered_non_admin_cannot_save_script_hook() {
    demo();
    support::registered_owner_can_save_hook(true).await;
}
#[tokio::test]
async fn demo_stored_script_lifecycle_event_records_refusal_without_spawn() {
    demo();
    support::stored_lifecycle_event(true).await;
}
#[tokio::test]
async fn demo_blocking_script_records_refusal_without_spawn() {
    demo();
    support::blocking_hook(true).await;
}
#[tokio::test]
async fn demo_stored_ci_review_entry_parks_without_spawn() {
    demo();
    support::stored_ci_review_entry(true).await;
}
#[tokio::test]
async fn demo_environment_setup_and_probe_refused_without_spawn() {
    demo();
    support::environment_setup(true).await;
}

#[tokio::test]
async fn demo_settings_task_and_workflow_command_writes_are_422() {
    demo();
    let (_root, harness, project, _, task) = support::fixture("demo-command-config").await;
    for settings in [
        json!({"review":{"ci_steps":["true"]}}),
        json!({"default_review_config":{"setup_steps":["true"]}}),
        json!({"environment":{"checks":[{"name":"probe","command":"true"}]}}),
        json!({"command_allowlist":{"only":["sh"]}}),
    ] {
        let row = db::ProjectRepo::get_by_id(&*harness.state.db, &project)
            .await
            .unwrap()
            .unwrap();
        let error: Value = common::json_request(
            &harness.app,
            Method::PATCH,
            &format!("/api/v1/projects/{project}"),
            json!({"version":row.version,"settings":settings}),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
        assert_eq!(error["code"], "server_commands_disabled");
    }
    let error: Value = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{project}/tasks"),
        json!({"title":"command","review_config":{"ci_steps":["true"]}}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    assert_eq!(error["code"], "server_commands_disabled");
    let error: Value = common::json_request(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/tasks/{}", task["id"].as_str().unwrap()),
        json!({"version":task["version"],"task_state_config":{"review":{"setup_steps":["true"]}}}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    assert_eq!(error["code"], "server_commands_disabled");
    let row = db::ProjectRepo::get_by_id(&*harness.state.db, &project)
        .await
        .unwrap()
        .unwrap();
    let mut definition: Value = serde_json::from_str(&row.workflow_definition).unwrap();
    definition["states"][0]["config"] = json!({"ci_steps":["true"]});
    let error: Value = common::json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{project}/workflow"),
        json!({"definition":definition}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    assert_eq!(error["code"], "server_commands_disabled");
}

#[tokio::test]
async fn demo_check_executor_refuses_probe_commands_and_receipt() {
    demo();
    let root = common::TestDir::new("demo-check-source");
    let probes = check_executor::environment_probes();
    assert!(matches!(check_executor::inherited_environment().await,
        Err(ref error) if error.to_string().contains("server_commands_disabled")));
    assert_eq!(check_executor::environment_probes(), probes);
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = db::SqliteDb::new(pool);
    assert!(matches!(
        services::check_runner::policy::attest(&db, &Default::default()).await,
        Err(services::ServiceError::ServerCommandsDisabled)
    ));
    assert_eq!(check_executor::environment_probes(), probes);
    let spec = check_executor::legacy_ci_spec("touch check-marker", &Default::default(), 1, false);
    assert!(check_executor::command(
        root.path(),
        &spec.commands[0],
        &Default::default(),
        &spec.execution_policy
    )
    .unwrap_err()
    .contains("server_commands_disabled"));
    let receipt = check_executor::execute(check_executor::CheckExecution {
        operation_id: "demo-refusal",
        spec: &spec,
        target: check_executor::CheckoutTarget::Workspace(root.path()),
        owner: api_types::CheckOwnerIdentity {
            owner_kind: "server".into(),
            machine_id: None,
            runtime_id: "demo".into(),
        },
        input_revisions: None,
        environment: &Default::default(),
        deadline: None,
        cancel: &tokio_util::sync::CancellationToken::new(),
        permit: &check_executor::CheckPermit::already_admitted(),
        cleanup: check_executor::CleanupPlan {
            commands: &[],
            timeout: std::time::Duration::from_secs(1),
        },
        output_limit: 4096,
    })
    .await;
    assert!(serde_json::to_string(&receipt)
        .unwrap()
        .contains("server_commands_disabled"));
    assert!(receipt.commands.is_empty());
    assert!(!root.path().join("check-marker").exists());
}

#[tokio::test]
async fn demo_provider_credentials_and_chat_turns_are_422() {
    demo();
    support::provider_and_chat_turns(true).await;
}

#[tokio::test]
async fn demo_server_paths_and_non_public_remotes_are_refused() {
    demo();
    support::repository_inputs(true).await;
}

#[tokio::test]
async fn demo_environment_assets_are_refused_without_copy() {
    demo();
    support::environment_assets(true).await;
}

#[tokio::test]
async fn demo_integrations_and_agent_config_writes_are_422() {
    demo();
    support::integrations(true).await;
    let (_root, harness, project, _, _) = support::fixture("demo-mcp-config").await;
    let response = support::send(
        &harness.app,
        Method::POST,
        "/api/v1/config/mcp",
        json!({"agent":"codex","scope":"user","action":"install","project_id":project}),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        response.1
    );
    assert_eq!(response.1["code"], "server_location_refused");
}

/// The demo container's startup under the policy: `--demo` seeding, then
/// exactly the REST calls of `deploy/demo/forge-demo-entrypoint.sh`.
#[tokio::test]
async fn demo_seed_and_entrypoint_calls_succeed_under_the_policy() {
    demo();
    let root = common::TestDir::new("demo-seed-entrypoint");
    let harness = common::test_app(root.path(), "demo-seed-entrypoint").await;
    services::install_demo_data(&harness.state.db)
        .await
        .unwrap();
    // Seeding is idempotent, as on every container start.
    services::install_demo_data(&harness.state.db)
        .await
        .unwrap();
    let mut accounts = Vec::new();
    for (email, password, admin) in [
        ("admin@example.org", "AdminPassword123!", true),
        ("tester@example.org", "TesterPassword123!", false),
    ] {
        let _: Value = common::json_request(
            &harness.app,
            Method::POST,
            "/api/v1/auth/register",
            json!({"email":email,"password":password,"display_name":email}),
            StatusCode::CREATED,
        )
        .await;
        let login: api_types::AuthResponse = common::json_request(
            &harness.app,
            Method::POST,
            "/api/v1/auth/login",
            json!({"email":email,"password":password}),
            StatusCode::OK,
        )
        .await;
        let me: api_types::UserResponse = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            "/api/v1/auth/me",
            &login.access_token,
            StatusCode::OK,
        )
        .await;
        assert_eq!(me.is_admin, admin, "{email}");
        accounts.push((login.access_token, me.id));
    }
    let projects: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/projects",
        &accounts[0].0,
        StatusCode::OK,
    )
    .await;
    let demo_project = projects["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|project| project["name"] == "Demo")
        .expect("the seeded Demo Project belongs to the administrator");
    let (remote_url, local_path): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT remote_url, local_path FROM repo WHERE project_id = ?")
            .bind(demo_project["id"].as_str().unwrap())
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
    assert_eq!(remote_url.as_deref(), Some("https://example.com/demo.git"));
    assert!(local_path.is_none());
    let response = support::send_with_bearer(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/members",
            demo_project["id"].as_str().unwrap()
        ),
        &accounts[0].0,
        json!({"user_id":accounts[1].1,"role":"member"}),
    )
    .await;
    assert!(
        matches!(response.0, StatusCode::OK | StatusCode::CREATED),
        "{}",
        response.1
    );
    let projects: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/projects",
        &accounts[1].0,
        StatusCode::OK,
    )
    .await;
    assert!(projects.to_string().contains("\"name\":\"Demo\""));
}

#[tokio::test]
async fn demo_keeps_daemon_owned_repository_locations() {
    demo();
    support::daemon_owned_location(true).await;
}
