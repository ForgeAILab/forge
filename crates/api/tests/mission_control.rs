mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use api_types::{
    AgentDetailResponse, AgentResponse, AttentionConsumerHealthResponse, ErrorResponse,
    MissionControlHomeResponse, ProjectResponse,
};
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use db::{
    new_uuid_v4, now_rfc3339, AttentionRepo, CreateAttentionProjection, CreateDomainEvent,
    CreateProject, DomainEventRepo, ProjectRepo, UpsertAttentionConsumerHealth,
};
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn mission_control_routes_require_authentication() {
    let workspace = common::TestDir::new("mission-control-auth-ws");
    let harness = common::test_app(workspace.path(), "mission-control-auth").await;

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/mission-control")
                .body(Body::empty())
                .expect("build unauthenticated request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn non_member_cannot_read_agent_detail() {
    let workspace = common::TestDir::new("mission-control-agent-auth-ws");
    let harness = common::test_app(workspace.path(), "mission-control-agent-auth").await;

    let agent: AgentResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/agents",
        json!({
            "name": "private-mission-control-agent",
            "executor_type": "shell"
        }),
        StatusCode::OK,
    )
    .await;

    let owner_detail: AgentDetailResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/mission-control/agents/{}", agent.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(owner_detail.identity_id, agent.id);

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/mission-control/agents/{}", agent.id))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", jwt_for("outsider-id")),
                )
                .body(Body::empty())
                .expect("build outsider request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: ErrorResponse = common::parse_response(response, StatusCode::NOT_FOUND).await;
    assert_eq!(error.code, "not_found");
}

#[tokio::test]
async fn mission_control_reports_visible_attention_count_and_stale_health() {
    let workspace = common::TestDir::new("mission-control-health-ws");
    let harness = common::test_app(workspace.path(), "mission-control-health").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Mission Control health project" }),
        StatusCode::OK,
    )
    .await;

    let source_event_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*harness.state.db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-for-mission-control-test".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project.id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!("test:{source_event_id}")),
            payload_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("source event persists");
    AttentionRepo::insert_attention(
        &*harness.state.db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "validation_failed".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project.id.clone(),
            identity_id: None,
            source_event_id,
            priority: 90,
            status: "open".to_owned(),
            summary: "Validation needs review".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: format!("attention:test:{}", project.id),
            occurred_at: "2020-01-01T00:00:00Z".to_owned(),
            updated_at: now_rfc3339(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".to_owned(),
            source_sequence: None,
        },
    )
    .await
    .expect("attention projection persists");
    let stale = "2020-01-01T00:00:00Z".to_owned();
    AttentionRepo::upsert_attention_consumer_health(
        &*harness.state.db,
        UpsertAttentionConsumerHealth {
            consumer_name: "attention_projection".to_owned(),
            last_sequence: 12,
            last_started_at: Some(stale.clone()),
            last_success_at: Some(stale.clone()),
            last_error_at: None,
            last_error_code: None,
            last_error_message: None,
            lease_owner: None,
            lease_until: None,
            processed_events_delta: 12,
            updated_at: stale,
        },
    )
    .await
    .expect("consumer health persists");

    let home: MissionControlHomeResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/mission-control?project_id={}", project.id),
        StatusCode::OK,
    )
    .await;

    assert_eq!(home.needs_attention.len(), 1);
    let health: AttentionConsumerHealthResponse =
        home.consumer_health.expect("consumer health is returned");
    assert!(health.stale);
    assert_eq!(health.last_sequence, 12);
    assert_eq!(health.processed_events, 12);
}

#[tokio::test]
async fn project_scope_denial_excludes_cross_project_attention() {
    let workspace = common::TestDir::new("mission-control-cross-project-ws");
    let harness = common::test_app(workspace.path(), "mission-control-cross-project").await;
    let now = now_rfc3339();
    let hidden_owner = "mission-control-hidden-owner";
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind(hidden_owner)
    .bind("mission-control-hidden-owner@example.test")
    .bind(&now)
    .bind(&now)
    .execute(harness.state.db.pool())
    .await
    .expect("hidden owner persists");

    let project_id = new_uuid_v4();
    ProjectRepo::create(
        &*harness.state.db,
        CreateProject {
            id: project_id.clone(),
            name: "Hidden Mission Control project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(hidden_owner.to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("hidden project persists");
    let source_event_id = new_uuid_v4();
    DomainEventRepo::append_event(
        &*harness.state.db,
        CreateDomainEvent {
            id: source_event_id.clone(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: new_uuid_v4(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            correlation_id: new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(new_uuid_v4()),
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("hidden source event persists");
    AttentionRepo::insert_attention(
        &*harness.state.db,
        CreateAttentionProjection {
            id: new_uuid_v4(),
            attention_type: "validation_failed".to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            identity_id: None,
            source_event_id,
            priority: 99,
            status: "open".to_owned(),
            summary: "Hidden validation failure".to_owned(),
            details_json: "{}".to_owned(),
            dedupe_key: format!("attention:hidden:{project_id}"),
            occurred_at: now.clone(),
            updated_at: now,
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".to_owned(),
            source_sequence: None,
        },
    )
    .await
    .expect("hidden attention persists");

    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/mission-control?project_id={project_id}"))
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::test_jwt()),
                )
                .body(Body::empty())
                .expect("build cross-project request"),
        )
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn mission_control_projects_authorized_receipts_and_approval_queue_without_payloads() {
    let workspace = common::TestDir::new("mission-control-coordination-activity-ws");
    let harness = common::test_app(workspace.path(), "mission-control-coordination-activity").await;
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Mission Control activity project" }),
        StatusCode::OK,
    )
    .await;
    let actor_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_identity WHERE owner_id IS NULL ORDER BY id LIMIT 1",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("default agent exists");

    let insert_event = |id: String, scope_type: String, scope_id: String, created_at: String| async {
        DomainEventRepo::append_event(
            &*harness.state.db,
            CreateDomainEvent {
                id,
                event_type: "coordination.command.committed".to_owned(),
                entity_type: "command_receipt".to_owned(),
                entity_id: new_uuid_v4(),
                actor_type: "user".to_owned(),
                actor_id: Some("test-user-id".to_owned()),
                scope_type,
                scope_id,
                correlation_id: new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(new_uuid_v4()),
                payload_json: "{}".to_owned(),
                created_at,
            },
        )
        .await
        .expect("command event persists");
    };

    let account_event_id = new_uuid_v4();
    insert_event(
        account_event_id.clone(),
        "account".to_owned(),
        "test-user-id".to_owned(),
        "2026-08-21T00:00:01Z".to_owned(),
    )
    .await;
    sqlx::query(
        r#"INSERT INTO command_receipt (
            id, principal_type, principal_id, scope_type, scope_id, operation,
            idempotency_key, input_digest, policy_result, correlation_id,
            causation_id, causation_depth, event_id, agent_action_execution_id,
            outcome_json, committed_at
         ) VALUES (?, 'user', 'test-user-id', 'account', 'test-user-id',
                   'charter.draft', 'activity-account-key', 'digest-account',
                   'allowed', 'correlation-account', NULL, 0, ?, NULL,
                   '{"revision_id":"revision-account"}', '2026-08-21T00:00:01Z')"#,
    )
    .bind(new_uuid_v4())
    .bind(&account_event_id)
    .execute(harness.state.db.pool())
    .await
    .expect("account command receipt persists");

    let project_event_id = new_uuid_v4();
    insert_event(
        project_event_id.clone(),
        "project".to_owned(),
        project.id.clone(),
        "2026-08-21T00:00:03Z".to_owned(),
    )
    .await;
    sqlx::query(
        r#"INSERT INTO command_receipt (
            id, principal_type, principal_id, scope_type, scope_id, operation,
            idempotency_key, input_digest, policy_result, correlation_id,
            causation_id, causation_depth, event_id, agent_action_execution_id,
            outcome_json, committed_at
         ) VALUES (?, 'agent', ?, 'project', ?, 'project.document.save',
                   'activity-project-key', 'digest-project', 'allowed',
                   'correlation-project', NULL, 0, ?, NULL,
                   '{"document_id":"document-project"}', '2026-08-21T00:00:03Z')"#,
    )
    .bind(new_uuid_v4())
    .bind(&actor_id)
    .bind(&project.id)
    .bind(&project_event_id)
    .execute(harness.state.db.pool())
    .await
    .expect("project command receipt persists");

    sqlx::query(
        "INSERT INTO agent_action (
            id, actor_identity_id, scope_type, scope_id, operation, payload_json,
            payload_hash, dedupe_key, correlation_id, causation_id, causation_depth,
            requested_permission, policy_result, policy_reason, status, target_type,
            target_id, version, created_at, updated_at
         ) VALUES (?, ?, 'project', ?, 'project.release',
                   '{\"secret\":\"payload-secret\"}', 'digest-pending',
                   'activity-action-key', 'correlation-pending', NULL, 0,
                   'release_project', 'approval_required', NULL,
                   'pending_approval', 'project', ?, 1,
                   '2026-08-21T00:00:02Z', '2026-08-21T00:00:02Z')",
    )
    .bind(new_uuid_v4())
    .bind(&actor_id)
    .bind(&project.id)
    .bind(&project.id)
    .execute(harness.state.db.pool())
    .await
    .expect("pending approval action persists");

    sqlx::query(
        "INSERT INTO agent_action (
            id, actor_identity_id, scope_type, scope_id, operation, payload_json,
            payload_hash, dedupe_key, correlation_id, causation_id, causation_depth,
            requested_permission, policy_result, policy_reason, status, target_type,
            target_id, version, created_at, updated_at
         ) VALUES (?, ?, 'account', 'test-user-id', 'project.create',
                   '{\"secret\":\"account-payload-secret\"}', 'digest-approved',
                   'activity-approved-key', 'correlation-approved', NULL, 0,
                   'propose_project', 'approval_required', NULL,
                   'approved', 'account', 'test-user-id', 1,
                   '2026-08-21T00:00:00Z', '2026-08-21T00:00:04Z')",
    )
    .bind(new_uuid_v4())
    .bind(&actor_id)
    .execute(harness.state.db.pool())
    .await
    .expect("approved account action persists");

    let hidden_owner = "mission-control-activity-hidden-owner";
    let hidden_now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind(hidden_owner)
    .bind("mission-control-activity-hidden-owner@example.test")
    .bind(&hidden_now)
    .bind(&hidden_now)
    .execute(harness.state.db.pool())
    .await
    .expect("hidden owner persists");
    let hidden_project_id = new_uuid_v4();
    ProjectRepo::create(
        &*harness.state.db,
        CreateProject {
            id: hidden_project_id.clone(),
            name: "Hidden Mission Control activity project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(hidden_owner.to_owned()),
            created_at: hidden_now.clone(),
            updated_at: hidden_now.clone(),
        },
    )
    .await
    .expect("hidden project persists");
    let hidden_event_id = new_uuid_v4();
    insert_event(
        hidden_event_id.clone(),
        "project".to_owned(),
        hidden_project_id.clone(),
        "2026-08-21T00:00:05Z".to_owned(),
    )
    .await;
    sqlx::query(
        r#"INSERT INTO command_receipt (
            id, principal_type, principal_id, scope_type, scope_id, operation,
            idempotency_key, input_digest, policy_result, correlation_id,
            causation_id, causation_depth, event_id, agent_action_execution_id,
            outcome_json, committed_at
         ) VALUES (?, 'user', ?, 'project', ?, 'hidden.operation',
                   'activity-hidden-key', 'digest-hidden', 'allowed',
                   'correlation-hidden', NULL, 0, ?, NULL,
                   '{"hidden":true}', '2026-08-21T00:00:05Z')"#,
    )
    .bind(new_uuid_v4())
    .bind(hidden_owner)
    .bind(&hidden_project_id)
    .bind(&hidden_event_id)
    .execute(harness.state.db.pool())
    .await
    .expect("hidden command receipt persists");

    let home: MissionControlHomeResponse = common::empty_request(
        &harness.app,
        Method::GET,
        "/api/v1/mission-control",
        StatusCode::OK,
    )
    .await;
    assert_eq!(home.coordination_activity.len(), 4);
    assert_eq!(
        home.coordination_activity[0].activity_kind,
        "approval_action"
    );
    assert_eq!(home.coordination_activity[0].status, "approved");
    assert_eq!(home.coordination_activity[0].actor_type, "agent");
    assert_eq!(
        home.coordination_activity[0].input_digest,
        "digest-approved"
    );
    assert_eq!(
        home.coordination_activity[1].activity_kind,
        "direct_command"
    );
    assert_eq!(home.coordination_activity[1].scope_type, "project");
    assert_eq!(home.coordination_activity[1].status, "committed");
    assert_eq!(
        home.coordination_activity[2].activity_kind,
        "approval_action"
    );
    assert_eq!(home.coordination_activity[2].status, "pending_approval");
    assert_eq!(home.coordination_activity[3].scope_type, "account");
    assert_eq!(home.coordination_activity[3].actor_id, "test-user-id");
    assert!(home
        .coordination_activity
        .iter()
        .all(|activity| activity.scope_id != hidden_project_id));

    let serialized = serde_json::to_string(&home).expect("home serializes");
    assert!(!serialized.contains("payload-secret"));
    assert!(!serialized.contains("account-payload-secret"));
    assert!(home.coordination_activity[1]
        .outcome
        .as_ref()
        .is_some_and(|outcome| outcome.get("document_id").is_some()));

    let filtered: MissionControlHomeResponse = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/mission-control?project_id={}", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(filtered.coordination_activity.len(), 2);
    assert!(filtered
        .coordination_activity
        .iter()
        .all(|activity| activity.scope_type == "project" && activity.scope_id == project.id));
}

fn jwt_for(user_id: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    let claims = json!({
        "sub": user_id,
        "email": format!("{user_id}@example.com"),
        "is_admin": false,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test JWT")
}
