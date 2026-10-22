use crate::common;
use api_types::ErrorResponse;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use db::{DaemonRepo, ProjectMemberRepo, ProjectRepo, RepoRepo, UserRepo};
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn daemons_refuse_own_embedded_identity_on_registration_and_reregistration() {
    let root = common::TestDir::new("reserved-daemons");
    let harness = common::test_app(root.path(), "reserved-daemons").await;
    let machine_id = services::embedded_daemon::embedded_machine_id();
    // Check both a fresh identity and a reconnect of an existing internal row,
    // with no user token, an ordinary account, and an administrator.
    for existing in [false, true] {
        let registration = if existing {
            Some(
                harness
                    .state
                    .daemon_service
                    .register(services::DaemonRegisterInput {
                        max_concurrent_runs: None,
                        machine_id: machine_id.clone(),
                        hostname: "internal-host".into(),
                        os: "linux".into(),
                        arch: "x86_64".into(),
                        agent_version: None,
                        labels: json!({"mode":"embedded"}),
                        runtimes: vec![],
                        owner_id: None,
                        visibility: Some("global".into()),
                    })
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        for token in [None, Some(common::test_jwt()), Some(common::admin_jwt())] {
            for supplied_id in [
                &machine_id,
                &format!("  {machine_id}  "),
                "embedded:another-server",
            ] {
                let mut builder = Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/daemons/register")
                    .header(header::CONTENT_TYPE, "application/json");
                if let Some(token) = &token {
                    builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
                }
                let response = harness.app.clone().oneshot(builder.body(Body::from(json!({"machine_id":supplied_id,"hostname":"attacker","os":"linux","arch":"x86_64"}).to_string())).unwrap()).await.unwrap();
                assert_eq!(response.status(), StatusCode::CONFLICT);
                let body = to_bytes(response.into_body(), 4096).await.unwrap();
                let error: ErrorResponse = serde_json::from_slice(&body).unwrap();
                assert_eq!(error.code, "machine_id_reserved");
                assert_eq!(
                    error.message,
                    "This machine_id is reserved for the Forge server"
                );
                assert!(!error.request_id.is_empty());
            }
        }
        if let Some(registration) = registration {
            let daemon = harness
                .state
                .daemon_service
                .authenticate(&registration.daemon_id, &registration.plaintext_token)
                .await
                .expect("internal token was not rotated");
            assert_eq!(daemon.hostname, "internal-host");
            assert_eq!(daemon.owner_id, None);
        } else {
            assert!(
                DaemonRepo::get_by_machine_id(&*harness.state.db, &machine_id)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test]
async fn config_mcp_requires_admin_before_disclosing_server_paths_or_reading_files() {
    let root = common::TestDir::new("config-mcp-admin");
    let harness = common::test_app(root.path(), "config-mcp-admin").await;
    for scope in ["user", "project", "local"] {
        let error: ErrorResponse = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            &format!("/api/v1/config/mcp?agent=claude&scope={scope}&project_id=hidden"),
            &common::test_jwt(),
            StatusCode::FORBIDDEN,
        )
        .await;
        assert_eq!(error.code, "admin_required");
        assert!(!serde_json::to_string(&error)
            .unwrap()
            .contains("config_path"));
        // An invalid action keeps the pre-fix regression run from writing to
        // the server's cwd or HOME. Authorization must precede even parsing it.
        let error: ErrorResponse = common::json_request_with_bearer(
            &harness.app,
            Method::POST,
            "/api/v1/config/mcp",
            &common::test_jwt(),
            json!({"agent":"claude","scope":scope,"action":"invalid"}),
            StatusCode::FORBIDDEN,
        )
        .await;
        assert_eq!(error.code, "admin_required");
    }
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/config/mcp?agent=claude&scope=user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn config_mcp_checks_project_visibility_and_write_role_before_filesystem_access() {
    let root = common::TestDir::new("config-mcp-project");
    let harness = common::test_app(root.path(), "config-mcp-project").await;
    let db = &*harness.state.db;
    let now = db::now_rfc3339();
    UserRepo::create_user(
        db,
        &db::User {
            id: "other-owner".into(),
            email: "owner@example.invalid".into(),
            password_hash: "unused".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::create(
        db,
        db::CreateProject {
            id: "private-config".into(),
            name: "Private".into(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: Some("other-owner".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    RepoRepo::create(
        db,
        db::CreateRepo {
            id: "private-repo".into(),
            project_id: "private-config".into(),
            name: "Private".into(),
            remote_url: None,
            local_path: Some(root.path().to_string_lossy().into_owned()),
            default_branch: "main".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let project = ProjectRepo::get_by_id(db, "private-config")
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        db,
        db::UpdateProject {
            id: project.id,
            name: None,
            settings: None,
            primary_repo_id: Some(Some("private-repo".into())),
            paused_at: None,
            updated_at: now.clone(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    let config_path = root.path().join(".claude/settings.json");
    // Invalid JSON would expose a filesystem error if authorization were late.
    std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
    std::fs::write(&config_path, "private invalid config").unwrap();
    for scope in ["project", "local"] {
        for project in ["private-config", "missing"] {
            let error: ErrorResponse = common::empty_request_with_bearer(
                &harness.app,
                Method::GET,
                &format!("/api/v1/config/mcp?agent=claude&scope={scope}&project_id={project}"),
                &common::admin_jwt(),
                StatusCode::NOT_FOUND,
            )
            .await;
            assert_eq!(error.code, "not_found");
            assert!(!error
                .message
                .contains(&root.path().to_string_lossy().to_string()));
            let error: ErrorResponse = common::json_request_with_bearer(
                &harness.app,
                Method::POST,
                "/api/v1/config/mcp",
                &common::admin_jwt(),
                json!({"agent":"claude","scope":scope,"project_id":project,"action":"install"}),
                StatusCode::NOT_FOUND,
            )
            .await;
            assert_eq!(error.code, "not_found");
        }
    }
    std::fs::write(&config_path, "{}").unwrap();
    for role in ["viewer", "member", "admin", "owner"] {
        if ProjectMemberRepo::get_member(db, "private-config", "test-user-id")
            .await
            .unwrap()
            .is_some()
        {
            ProjectMemberRepo::remove_member(db, "private-config", "test-user-id")
                .await
                .unwrap();
        }
        ProjectMemberRepo::add_member(
            db,
            db::CreateProjectMember {
                id: db::new_uuid_v4(),
                project_id: "private-config".into(),
                user_id: "test-user-id".into(),
                role: role.into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let response: Value = common::empty_request_with_bearer(
            &harness.app,
            Method::GET,
            "/api/v1/config/mcp?agent=claude&scope=project&project_id=private-config",
            &common::admin_jwt(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            response["config_path"],
            config_path.to_string_lossy().as_ref()
        );
        let allowed = matches!(role, "admin" | "owner");
        let response: Value = common::json_request_with_bearer(&harness.app, Method::POST, "/api/v1/config/mcp", &common::admin_jwt(), json!({"agent":"claude","scope":"project","project_id":"private-config","action":"install"}), if allowed {StatusCode::OK} else {StatusCode::FORBIDDEN}).await;
        if allowed {
            assert_eq!(response["installed"], true);
        } else {
            assert_eq!(response["code"], "insufficient_role");
            assert_eq!(std::fs::read_to_string(&config_path).unwrap(), "{}");
        }
    }
}

#[tokio::test]
async fn providers_reject_legacy_transition_endpoints_before_sending_test_requests() {
    let root = common::TestDir::new("providers-transition");
    let harness = common::test_app(root.path(), "providers-transition").await;
    // Use legacy metadata with no secret: URL policy must reject it before
    // credential acquisition or a network operation. No public host is dialed.
    let now = db::now_rfc3339();
    for (index, endpoint) in [
        "https://0.1.2.3",
        "https://[2001:db8::1]",
        "https://[2001:2::1]",
        "https://[2001:10::1]",
        "https://[2001:20::1]",
        "https://[100::1]",
        "https://[3fff::1]",
        "https://[::ffff:0:10.0.0.1]",
        "https://[::ffff:0:127.0.0.1]",
        "https://[::ffff:0:169.254.169.254]",
        "https://[64:ff9b::7f00:1]",
        "https://[64:ff9b:1::1]",
        "https://[2002:7f00:1::]",
        "https://[2001::1]",
        "https://[::ffff:0.1.2.3]",
        "https://[::0.1.2.3]",
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("legacy-transition-{index}");
        sqlx::query("INSERT INTO credential_handle (id, owner_user_id, provider, label, metadata_json, created_at, updated_at) VALUES (?, 'test-user-id', 'openai_compatible', 'Legacy', ?, ?, ?)")
            .bind(&id).bind(json!({"base_url":endpoint}).to_string()).bind(&now).bind(&now).execute(harness.state.db.pool()).await.unwrap();
        let error: ErrorResponse = common::json_request_with_bearer(
            &harness.app,
            Method::POST,
            &format!("/api/v1/providers/{id}/test"),
            &common::test_jwt(),
            json!({}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(error.code, "validation_error");
        assert_eq!(
            error.message,
            "base_url must not target a private or local address"
        );
    }
}

#[tokio::test]
async fn mcp_endpoint_returns_only_configured_public_url_to_project_viewers_and_accounts() {
    let root = common::TestDir::new("mcp-endpoint-public");
    let harness = common::test_app(root.path(), "mcp-endpoint-public").await;
    let now = db::now_rfc3339();
    ProjectRepo::create(
        &*harness.state.db,
        db::CreateProject {
            id: "endpoint-project".into(),
            name: "Endpoint".into(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: Some("test-user-id".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    // No repository is configured, and invalid config files cannot be read.
    std::fs::create_dir_all(root.path().join(".claude")).unwrap();
    std::fs::write(
        root.path().join(".claude/settings.json"),
        "invalid private config",
    )
    .unwrap();
    let mut state = (*harness.state).clone();
    let mut config = (*state.effective_config).clone();
    config.server.public_base_url = Some("https://public.forge.example/app?token=private".into());
    state.effective_config = std::sync::Arc::new(config);
    let app = api::build_router(state, root.path());
    for (query, url) in [
        (
            "scope=project&project_id=endpoint-project",
            "https://public.forge.example/mcp?project_id=endpoint-project",
        ),
        (
            "scope=local&project_id=endpoint-project",
            "https://public.forge.example/mcp",
        ),
        ("scope=user", "https://public.forge.example/mcp"),
    ] {
        let response: Value = common::empty_request_with_bearer(
            &app,
            Method::GET,
            &format!("/api/v1/config/mcp/endpoint?{query}"),
            &common::test_jwt(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(response, json!({"url": url}));
    }
    // The public read cannot accept a caller-controlled base URL.
    let response = app.oneshot(Request::builder()
        .uri("/api/v1/config/mcp/endpoint?scope=user&public_base_url=https://attacker.invalid")
        .header(header::AUTHORIZATION, format!("Bearer {}", common::test_jwt()))
        .body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mcp_endpoint_requires_authentication_and_project_visibility() {
    let root = common::TestDir::new("mcp-endpoint-auth");
    let harness = common::test_app(root.path(), "mcp-endpoint-auth").await;
    let now = db::now_rfc3339();
    UserRepo::create_user(
        &*harness.state.db,
        &db::User {
            id: "endpoint-owner".into(),
            email: "endpoint-owner@example.invalid".into(),
            password_hash: "unused".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::create(
        &*harness.state.db,
        db::CreateProject {
            id: "hidden-endpoint".into(),
            name: "Hidden".into(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: Some("endpoint-owner".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/config/mcp/endpoint?scope=user")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    for token in [common::test_jwt(), common::admin_jwt()] {
        for scope in ["project", "local"] {
            for project in ["hidden-endpoint", "missing-endpoint"] {
                let error: ErrorResponse = common::empty_request_with_bearer(
                    &harness.app,
                    Method::GET,
                    &format!("/api/v1/config/mcp/endpoint?scope={scope}&project_id={project}"),
                    &token,
                    StatusCode::NOT_FOUND,
                )
                .await;
                assert_eq!(error.code, "not_found");
            }
        }
    }
    ProjectMemberRepo::add_member(
        &*harness.state.db,
        db::CreateProjectMember {
            id: db::new_uuid_v4(),
            project_id: "hidden-endpoint".into(),
            user_id: "test-user-id".into(),
            role: "viewer".into(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let response: Value = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/config/mcp/endpoint?scope=project&project_id=hidden-endpoint",
        &common::test_jwt(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response.as_object().unwrap().len(), 1);
    assert!(response["url"]
        .as_str()
        .unwrap()
        .ends_with("/mcp?project_id=hidden-endpoint"));
    let error: ErrorResponse = common::empty_request_with_bearer(
        &harness.app,
        Method::GET,
        "/api/v1/config/mcp/endpoint?scope=project",
        &common::test_jwt(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(error.code, "project_id_required");
}
