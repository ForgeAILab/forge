#![allow(dead_code)]
mod common;

use api_types::{
    AgentResponse, PaginatedResponse, ProjectMemberResponse, ProjectResponse, TokenResponse,
    UserResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

// ── helpers ───────────────────────────────────────────────────────────────

async fn register_user(app: &Router, email: &str, password: &str) -> api_types::AuthResponse {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/register")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({ "email": email, "password": password }))
                        .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).expect("parse AuthResponse")
}

async fn bearer_json<T: DeserializeOwned>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected: StatusCode,
) -> T {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        expected,
        "unexpected status; body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse response body")
}

async fn bearer_get<T: DeserializeOwned>(
    app: &Router,
    uri: &str,
    token: &str,
    expected: StatusCode,
) -> T {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        expected,
        "unexpected status; body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse response body")
}

async fn bearer_get_status(app: &Router, uri: &str, token: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn bearer_delete_status(app: &Router, uri: &str, token: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn bearer_json_status(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

fn jwt_for_missing_user() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": "deleted-user-id",
            "email": "deleted@example.test",
            "is_admin": false,
            "iat": now,
            "exp": now + 900,
        }),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("stale test jwt encodes")
}

// ── 6.1 ProjectMemberRepo unit tests ─────────────────────────────────────

#[tokio::test]
async fn project_member_repo_crud_uniqueness_and_role_update() {
    use db::{
        new_uuid_v4, now_rfc3339, CreateProject, CreateProjectMember, DbError, ProjectMemberRepo,
        ProjectRepo,
    };

    let ws = common::TestDir::new("member-repo-ws");
    let harness = common::test_app(ws.path(), "member-repo").await;
    let db = &*harness.state.db;

    let now = now_rfc3339();
    let project = ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "member-test-proj".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("test-user-id".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();

    // Project creation atomically materializes its authenticated owner.
    let member = ProjectMemberRepo::get_member(db, &project.id, "test-user-id")
        .await
        .unwrap()
        .expect("owner membership creates with the Project");
    assert_eq!(member.role, "owner");

    // List members
    let members = ProjectMemberRepo::list_members(db, &project.id)
        .await
        .unwrap();
    assert_eq!(members.len(), 1);

    // Duplicate insert → error (UNIQUE constraint)
    let dup = ProjectMemberRepo::add_member(
        db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            user_id: "test-user-id".to_owned(),
            role: "member".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await;
    assert!(dup.is_err(), "duplicate member must be rejected");

    // Update role
    let updated =
        ProjectMemberRepo::update_member_role(db, &project.id, "test-user-id", "admin", &now)
            .await
            .unwrap();
    assert_eq!(updated.role, "admin");

    // Confirm via get
    let confirmed = ProjectMemberRepo::get_member(db, &project.id, "test-user-id")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(confirmed.role, "admin");

    // Remove member
    ProjectMemberRepo::remove_member(db, &project.id, "test-user-id")
        .await
        .unwrap();
    let gone = ProjectMemberRepo::get_member(db, &project.id, "test-user-id")
        .await
        .unwrap();
    assert!(gone.is_none());

    // Remove again → NotFound
    let err = ProjectMemberRepo::remove_member(db, &project.id, "test-user-id").await;
    assert!(
        matches!(err, Err(DbError::NotFound)),
        "removing non-existent member must return NotFound"
    );
}

// ── 6.3 PAT unit tests ───────────────────────────────────────────────────

#[tokio::test]
async fn pat_api_create_has_fg_prefix_and_list_omits_raw_token() {
    let ws = common::TestDir::new("pat-prefix-ws");
    let harness = common::test_app(ws.path(), "pat-prefix").await;
    let app = &harness.app;
    let jwt = common::test_jwt();

    // Create PAT
    let created: TokenResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/auth/tokens",
        &jwt,
        json!({ "name": "my-token" }),
        StatusCode::CREATED,
    )
    .await;

    let raw = created
        .token
        .as_deref()
        .expect("token must be present on create");
    assert!(
        raw.starts_with("fg_"),
        "raw token must start with fg_, got: {raw}"
    );
    assert!(
        created.prefix.starts_with("fg_"),
        "prefix must start with fg_"
    );
    assert_eq!(&created.name, "my-token");

    // List PATs — raw token must NOT appear
    let list: Vec<TokenResponse> =
        common::empty_request(app, Method::GET, "/api/v1/auth/tokens", StatusCode::OK).await;
    assert_eq!(list.len(), 1);
    assert!(
        list[0].token.is_none(),
        "raw token must not be returned in list"
    );
    assert_eq!(list[0].id, created.id);
}

#[tokio::test]
async fn pat_expired_token_rejected_by_verify_pat() {
    use db::{new_uuid_v4, now_rfc3339, CreatePersonalAccessToken, PersonalAccessTokenRepo};
    use sha2::{Digest, Sha256};

    let ws = common::TestDir::new("pat-expiry-ws");
    let harness = common::test_app(ws.path(), "pat-expiry").await;
    let db = &*harness.state.db;

    let raw = format!("fg_{}", hex::encode([1u8; 20]));
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let token_hash = hex::encode(hasher.finalize());

    let now = now_rfc3339();
    PersonalAccessTokenRepo::create_pat(
        db,
        CreatePersonalAccessToken {
            id: new_uuid_v4(),
            user_id: "test-user-id".to_owned(),
            name: "expired-token".to_owned(),
            token_hash,
            prefix: "fg_0101".to_owned(),
            scopes: "*".to_owned(),
            expires_at: Some("2020-01-01T00:00:00Z".to_owned()),
            created_at: now,
        },
    )
    .await
    .unwrap();

    let result = harness.state.auth_service.verify_pat(&raw).await;
    assert!(result.is_err(), "expired token must be rejected");
    assert_eq!(result.unwrap_err(), "token_expired");
}

#[tokio::test]
async fn pat_delete_invalidates_token() {
    let ws = common::TestDir::new("pat-delete-ws");
    let harness = common::test_app(ws.path(), "pat-delete").await;
    let app = &harness.app;
    let jwt = common::test_jwt();

    // Create PAT
    let created: TokenResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/auth/tokens",
        &jwt,
        json!({ "name": "delete-me" }),
        StatusCode::CREATED,
    )
    .await;
    let raw = created.token.clone().unwrap();
    let id = &created.id;

    // verify_pat works before deletion
    assert!(
        harness.state.auth_service.verify_pat(&raw).await.is_ok(),
        "token should be valid before delete"
    );

    // Delete via API
    let status = bearer_delete_status(app, &format!("/api/v1/auth/tokens/{id}"), &jwt).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // verify_pat must fail after deletion
    let result = harness.state.auth_service.verify_pat(&raw).await;
    assert!(
        result.is_err(),
        "deleted token must be rejected by verify_pat"
    );
}

// ── 6.4 PAT auth integration ─────────────────────────────────────────────

#[tokio::test]
async fn pat_auth_flow_create_use_delete_rejected() {
    let ws = common::TestDir::new("pat-auth-ws");
    let harness = common::test_app(ws.path(), "pat-auth").await;
    let app = &harness.app;
    let jwt = common::test_jwt();

    // Create PAT with JWT
    let created: TokenResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/auth/tokens",
        &jwt,
        json!({ "name": "ci-token" }),
        StatusCode::CREATED,
    )
    .await;
    let raw = created.token.clone().unwrap();
    let id = &created.id;

    // Use PAT as Bearer to access a protected endpoint
    let me: UserResponse = bearer_get(app, "/api/v1/auth/me", &raw, StatusCode::OK).await;
    assert_eq!(me.id, "test-user-id");

    // Delete PAT via API
    let status = bearer_delete_status(app, &format!("/api/v1/auth/tokens/{id}"), &jwt).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // PAT must now be rejected
    let rejected = bearer_get_status(app, "/api/v1/auth/me", &raw).await;
    assert_eq!(
        rejected,
        StatusCode::UNAUTHORIZED,
        "deleted PAT must be rejected by auth middleware"
    );
}

// ── 6.5 Project auto-membership and scoping ───────────────────────────────

#[tokio::test]
async fn project_auto_membership_and_non_member_scoping() {
    let ws = common::TestDir::new("proj-scope-ws");
    let harness = common::test_app(ws.path(), "proj-scope").await;
    let app = &harness.app;
    let jwt_a = common::test_jwt();

    // User A creates a project → auto owner membership
    let project: ProjectResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "scoped-project" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        project.owner_id.as_deref(),
        Some("test-user-id"),
        "project must be owned by creator"
    );
    let project_id = &project.id;

    // Auto-created owner member
    let members: Vec<ProjectMemberResponse> = bearer_get(
        app,
        &format!("/api/v1/projects/{project_id}/members"),
        &jwt_a,
        StatusCode::OK,
    )
    .await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].user_id, "test-user-id");
    assert_eq!(members[0].role, "owner");

    // User A lists projects → sees the project
    let list_a: PaginatedResponse<ProjectResponse> =
        common::empty_request(app, Method::GET, "/api/v1/projects", StatusCode::OK).await;
    assert!(
        list_a.items.iter().any(|p| p.id == *project_id),
        "owner must see their project in list"
    );

    // Register User B
    let auth_b = register_user(app, "userb-proj@example.com", "Password123!").await;
    let token_b = auth_b.access_token;

    // User B lists projects → does NOT see User A's project
    let list_b: PaginatedResponse<ProjectResponse> =
        bearer_get(app, "/api/v1/projects", &token_b, StatusCode::OK).await;
    assert!(
        !list_b.items.iter().any(|p| p.id == *project_id),
        "non-member must not see owner-scoped project in list"
    );

    // User B gets project by ID → 404
    let status = bearer_get_status(app, &format!("/api/v1/projects/{project_id}"), &token_b).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "non-member must receive 404 on get_project"
    );
}

#[tokio::test]
async fn stale_jwt_cannot_create_an_ownerless_project() {
    let ws = common::TestDir::new("stale-project-owner-ws");
    let harness = common::test_app(ws.path(), "stale-project-owner").await;

    let status = bearer_json_status(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &jwt_for_missing_user(),
        json!({ "name": "must-not-exist" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project WHERE name = ?")
        .bind("must-not-exist")
        .fetch_one(harness.state.db.pool())
        .await
        .expect("project count loads");
    assert_eq!(project_count, 0);
}

// ── 6.6 Agent account-visibility scoping ─────────────────────────────────

#[tokio::test]
async fn agent_account_visibility_hidden_from_other_user() {
    let ws = common::TestDir::new("agent-vis-ws");
    let harness = common::test_app(ws.path(), "agent-vis").await;
    let app = &harness.app;

    // Create a shell agent as User A (test-user-id) — gets visibility=account by default
    let (agent_id, _) = common::create_shell_agents(app, ws.path(), "agent-vis").await;

    // User A lists agents → sees their account-scoped agent
    let agents_a: PaginatedResponse<AgentResponse> =
        common::empty_request(app, Method::GET, "/api/v1/agents", StatusCode::OK).await;
    assert!(
        agents_a.items.iter().any(|a| a.id == agent_id),
        "owner must see their account-scoped agent in list"
    );

    // User A gets agent by ID → 200
    let agent: AgentResponse = common::empty_request(
        app,
        Method::GET,
        &format!("/api/v1/agents/{agent_id}"),
        StatusCode::OK,
    )
    .await;
    assert_eq!(agent.visibility, "account");
    assert_eq!(agent.owner_id.as_deref(), Some("test-user-id"));

    // Register User B
    let auth_b = register_user(app, "userb-agent@example.com", "Password123!").await;
    let token_b = auth_b.access_token;

    // User B lists agents → does NOT see User A's account-scoped agent
    let agents_b: PaginatedResponse<AgentResponse> =
        bearer_get(app, "/api/v1/agents", &token_b, StatusCode::OK).await;
    assert!(
        !agents_b.items.iter().any(|a| a.id == agent_id),
        "non-owner must not see account-scoped agent in list"
    );

    // User B gets agent by ID → 404
    let status = bearer_get_status(app, &format!("/api/v1/agents/{agent_id}"), &token_b).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "non-owner must receive 404 on get_agent"
    );
}

#[tokio::test]
async fn non_owner_cannot_mutate_account_scoped_agent() {
    let ws = common::TestDir::new("agent-owner-mutate-ws");
    let harness = common::test_app(ws.path(), "agent-owner-mutate").await;
    let app = &harness.app;

    let (agent_id, _) = common::create_shell_agents(app, ws.path(), "agent-owner-mutate").await;
    let agent: AgentResponse = common::empty_request(
        app,
        Method::GET,
        &format!("/api/v1/agents/{agent_id}"),
        StatusCode::OK,
    )
    .await;

    let auth_b = register_user(app, "userb-agent-mutate@example.com", "Password123!").await;
    let token_b = auth_b.access_token;

    let patch_status = bearer_json_status(
        app,
        Method::PATCH,
        &format!("/api/v1/agents/{agent_id}"),
        &token_b,
        json!({
            "name": "stolen-agent",
            "version": agent.version
        }),
    )
    .await;
    assert_eq!(
        patch_status,
        StatusCode::NOT_FOUND,
        "non-owner must not update another user's agent"
    );

    let pause_status = bearer_json_status(
        app,
        Method::POST,
        &format!("/api/v1/agents/{agent_id}/pause"),
        &token_b,
        json!({}),
    )
    .await;
    assert_eq!(
        pause_status,
        StatusCode::NOT_FOUND,
        "non-owner must not pause another user's agent"
    );

    let delete_status =
        bearer_delete_status(app, &format!("/api/v1/agents/{agent_id}"), &token_b).await;
    assert_eq!(
        delete_status,
        StatusCode::NOT_FOUND,
        "non-owner must not delete another user's agent"
    );
}

#[tokio::test]
async fn non_admin_cannot_pin_agent_to_daemon() {
    let ws = common::TestDir::new("agent-daemon-pin-ws");
    let harness = common::test_app(ws.path(), "agent-daemon-pin").await;
    let app = &harness.app;

    let registration: api_types::DaemonRegisterResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": "agent-daemon-pin-machine",
            "hostname": "agent-daemon-pin-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "test"
        }),
        StatusCode::OK,
    )
    .await;

    let status = bearer_json_status(
        app,
        Method::POST,
        "/api/v1/agents",
        &common::test_jwt(),
        json!({
            "name": "pinned-by-non-admin",
            "executor_type": "shell",
            "daemon_id": registration.daemon_id
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "non-admin must not pin agents to daemons"
    );
}

#[tokio::test]
async fn non_admin_project_agent_roster_redacts_daemon_id() {
    let ws = common::TestDir::new("project-agent-redaction-ws");
    let harness = common::test_app(ws.path(), "project-agent-redaction").await;
    let app = &harness.app;

    let registration: api_types::DaemonRegisterResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        &common::test_jwt(),
        json!({
            "machine_id": "project-agent-redaction-machine",
            "hostname": "project-agent-redaction-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "test"
        }),
        StatusCode::OK,
    )
    .await;
    let agent: AgentResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/agents",
        &common::admin_jwt(),
        json!({
            "name": "project-agent-redaction",
            "executor_type": "shell",
            "daemon_id": registration.daemon_id
        }),
        StatusCode::OK,
    )
    .await;
    assert!(agent.daemon_id.is_some(), "administrators see daemon ids");

    let project: ProjectResponse = bearer_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &common::test_jwt(),
        json!({ "name": "project-agent-redaction" }),
        StatusCode::OK,
    )
    .await;
    let candidates: Vec<AgentResponse> = bearer_get(
        app,
        &format!("/api/v1/projects/{}/agents", project.id),
        &common::test_jwt(),
        StatusCode::OK,
    )
    .await;
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.id == agent.id)
        .expect("owned agent is selectable for the project");
    assert_eq!(candidate.daemon_id, None);
}

// ── 6.7 Membership CRUD ───────────────────────────────────────────────────

#[tokio::test]
async fn membership_crud_add_update_role_remove_and_duplicate_conflict() {
    let ws = common::TestDir::new("member-crud-ws");
    let harness = common::test_app(ws.path(), "member-crud").await;
    let app = &harness.app;
    let jwt_a = common::test_jwt();

    // User A creates project → auto owner member
    let project: ProjectResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "membership-test" }),
        StatusCode::OK,
    )
    .await;
    let project_id = &project.id;

    // Register User B
    let auth_b = register_user(app, "member-b@example.com", "Password123!").await;
    let token_b = auth_b.access_token;

    // Get User B's ID
    let me_b: UserResponse = bearer_get(app, "/api/v1/auth/me", &token_b, StatusCode::OK).await;
    let user_b_id = &me_b.id;

    // User A adds User B as "member"
    let new_member: ProjectMemberResponse = bearer_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/members"),
        &jwt_a,
        json!({ "user_id": user_b_id, "role": "member" }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(new_member.user_id, *user_b_id);
    assert_eq!(new_member.role, "member");

    // List → 2 members (User A as owner + User B as member)
    let members: Vec<ProjectMemberResponse> = bearer_get(
        app,
        &format!("/api/v1/projects/{project_id}/members"),
        &jwt_a,
        StatusCode::OK,
    )
    .await;
    assert_eq!(members.len(), 2, "expected 2 members after add");

    // Update User B's role to "admin"
    let updated: ProjectMemberResponse = bearer_json(
        app,
        Method::PATCH,
        &format!("/api/v1/projects/{project_id}/members/{user_b_id}"),
        &jwt_a,
        json!({ "role": "admin" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated.role, "admin");

    // List → B's role is now admin
    let members: Vec<ProjectMemberResponse> = bearer_get(
        app,
        &format!("/api/v1/projects/{project_id}/members"),
        &jwt_a,
        StatusCode::OK,
    )
    .await;
    let b_entry = members
        .iter()
        .find(|m| m.user_id == *user_b_id)
        .expect("user B must be in member list");
    assert_eq!(b_entry.role, "admin");

    // Remove User B
    let status = bearer_delete_status(
        app,
        &format!("/api/v1/projects/{project_id}/members/{user_b_id}"),
        &jwt_a,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // List → back to 1 member
    let members: Vec<ProjectMemberResponse> = bearer_get(
        app,
        &format!("/api/v1/projects/{project_id}/members"),
        &jwt_a,
        StatusCode::OK,
    )
    .await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].user_id, "test-user-id");

    // Remove non-existent → 404
    let not_found = bearer_delete_status(
        app,
        &format!("/api/v1/projects/{project_id}/members/{user_b_id}"),
        &jwt_a,
    )
    .await;
    assert_eq!(not_found, StatusCode::NOT_FOUND);

    // Add User A again → 409 Conflict (already a member)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/projects/{project_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {jwt_a}"))
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "user_id": "test-user-id",
                        "role": "member"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "adding existing member must return 409"
    );

    // Invalid role → 400
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/projects/{project_id}/members"))
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {jwt_a}"))
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "user_id": user_b_id,
                        "role": "superuser"
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid role must return 400"
    );
}
