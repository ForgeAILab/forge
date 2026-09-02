#![allow(dead_code)]
mod common;

use api_types::{CommitmentResponse, InboxItemResponse, QuestionResponse};
use axum::{http::Method, http::StatusCode};
use db::{
    new_uuid_v4, now_rfc3339, AgentRepo, AgentStatus, CreateAgentIdentity, CreateAgentProfile,
    CreateProject, CreateProjectAgentBinding, CreateProjectMember, ProjectAgentBindingRepo,
    ProjectMemberRepo, ProjectRepo, ReplaceProjectAgentBinding, User, UserRepo,
};
use serde_json::json;

#[tokio::test]
async fn coordination_routes_bind_identity_scope_and_require_evidence() {
    let workspace = common::TestDir::new("coordination-routes");
    let harness = common::test_app(workspace.path(), "coordination-routes").await;
    let app = &harness.app;
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        &*harness.state.db,
        CreateAgentIdentity {
            id: "coord-agent".to_owned(),
            name: "Coordination Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: r#"{"permissions":["read_account","propose_message"]}"#
                .to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: "coord-agent".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: r#"{"permissions":["read_account","propose_message"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity seeded");

    let commitment: CommitmentResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/agents/coord-agent/commitments",
        json!({
            "scope_type": "account",
            "scope_id": "test-user-id",
            "title": "Answer the user",
            "correlation_id": "coord-correlation"
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(commitment.owner_identity_id, "coord-agent");
    assert_eq!(commitment.status, "proposed");

    let missing_evidence = common::raw_json_request(
        app,
        Method::POST,
        &format!("/api/v1/commitments/{}/complete", commitment.id),
        json!({
            "expected_version": commitment.version,
            "evidence_type": "",
            "evidence_id": "",
            "metadata": {},
            "dedupe_key": "complete-1"
        }),
    )
    .await;
    assert_eq!(missing_evidence.status(), StatusCode::BAD_REQUEST);

    let question: QuestionResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/agents/coord-agent/questions",
        json!({
            "scope_type": "account",
            "scope_id": "test-user-id",
            "question": "Should I proceed?",
            "context": {"source": "test"},
            "correlation_id": "question-correlation",
            "inbox_dedupe_key": "question-1"
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(question.status, "open");

    let inbox: Vec<InboxItemResponse> = common::empty_request(
        app,
        Method::GET,
        "/api/v1/agents/coord-agent/inbox",
        StatusCode::OK,
    )
    .await;
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].kind, "question");
}

#[tokio::test]
async fn project_member_cannot_mutate_another_identity_commitment() {
    let workspace = common::TestDir::new("coordination-member-auth");
    let harness = common::test_app(workspace.path(), "coordination-member-auth").await;
    let db = &harness.state.db;
    let now = now_rfc3339();
    UserRepo::create_user(
        &**db,
        &User {
            id: "member-user-id".to_owned(),
            email: "member@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("member user seeded");
    ProjectRepo::create(
        &**db,
        CreateProject {
            id: "coord-project".to_owned(),
            name: "Coordination Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("test-user-id".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project seeded");
    ProjectMemberRepo::add_member(
        &**db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: "coord-project".to_owned(),
            user_id: "test-user-id".to_owned(),
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project owner seeded");
    ProjectMemberRepo::add_member(
        &**db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: "coord-project".to_owned(),
            user_id: "member-user-id".to_owned(),
            role: "member".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project member seeded");
    AgentRepo::create_identity_with_profile(
        &**db,
        CreateAgentIdentity {
            id: "project-owner-agent".to_owned(),
            name: "Project owner agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: r#"{"permissions":["propose_commitment"]}"#.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: "project-owner-agent".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "native".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: r#"{"permissions":["propose_commitment"]}"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project identity seeded");
    let agent = AgentRepo::get_by_id(&**db, "project-owner-agent")
        .await
        .expect("project identity lookup")
        .expect("project identity");
    let setup_binding = ProjectAgentBindingRepo::get_active_project_binding(&**db, "coord-project")
        .await
        .expect("setup binding lookup")
        .expect("setup binding");
    ProjectAgentBindingRepo::replace_project_binding(
        &**db,
        ReplaceProjectAgentBinding {
            project_id: "coord-project".to_owned(),
            expected_version: setup_binding.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: "coord-project".to_owned(),
                identity_id: Some(agent.id),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_commitment"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 10,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("coordination authorization fixture".to_owned()),
        },
    )
    .await
    .expect("project binding seeded");

    let commitment: CommitmentResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents/project-owner-agent/commitments",
        json!({
            "scope_type": "project",
            "scope_id": "coord-project",
            "title": "Owner-only obligation",
            "correlation_id": "member-auth-correlation"
        }),
        StatusCode::CREATED,
    )
    .await;
    let response = common::json_request_with_bearer::<serde_json::Value>(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/commitments/{}", commitment.id),
        &member_jwt(),
        json!({
            "expected_version": commitment.version,
            "status": "accepted",
            "reason": "member cannot take ownership",
            "dedupe_key": "member-mutation-1"
        }),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(response["code"], "coordination_mutation_forbidden");
}

fn member_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": "member-user-id",
            "email": "member@example.com",
            "is_admin": false,
            "iat": now,
            "exp": now + 900
        }),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("member jwt")
}
