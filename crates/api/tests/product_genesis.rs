#![allow(dead_code)]

mod common;

use api_types::{
    AgentChatMessageListResponse, ConnectedEmbeddedAgentResponse, ErrorResponse,
    ProductGenesisActiveResponse, ProductGenesisCharterResponse, ProductGenesisLifecycle,
    ProductGenesisSession, ProductGenesisStartResponse, ProjectCharterApproval,
    ProjectCharterRevision, ProjectResponse,
};
use axum::{http::Method, http::StatusCode, Router};
use serde_json::json;
use sqlx::Row;

#[tokio::test]
async fn product_genesis_uses_existing_main_chat_and_is_cancelable() {
    let workspace = common::TestDir::new("product-genesis-routes");
    let harness = common::test_app(workspace.path(), "product-genesis-routes").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let connected = connect_genesis_agent(app, &token, "genesis-main").await;

    let binding: api_types::MainAgentBindingResponse = common::json_request_with_bearer(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;

    let started: ProductGenesisStartResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "idempotency_key": "product-genesis-start-main",
            "maturity": "production",
            "initial_idea": "A bounded, durable product idea"
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(started.main_chat_id, binding.chat_id);
    assert_eq!(started.session.main_chat_id, binding.chat_id);
    assert!(matches!(
        started.session.lifecycle,
        api_types::ProductGenesisLifecycle::Discovering
    ));
    assert!(matches!(
        started.session.maturity,
        api_types::ProductMaturity::Production
    ));
    assert!(started.admitted_turn_id.is_some());
    assert_eq!(started.session.source_message_ids.len(), 1);

    let instruction = sqlx::query(
        "SELECT source_type, source_id, revision, body, created_by_type
         FROM agent_chat_instruction_revision
         WHERE chat_id = ? AND source_id = ?",
    )
    .bind(&binding.chat_id)
    .bind(&started.session.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("Genesis instruction revision is durable");
    assert_eq!(instruction.get::<String, _>("source_type"), "native");
    assert_eq!(
        instruction.get::<String, _>("source_id"),
        started.session.id
    );
    assert_eq!(
        instruction.get::<String, _>("created_by_type"),
        "product_genesis"
    );
    assert!(instruction
        .get::<String, _>("body")
        .contains("Forge Main Agent — Project Discovery and Portfolio Protocol v2"));

    let active: ProductGenesisActiveResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/account/main-agent/product-genesis/active",
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        active.session.as_ref().map(|session| &session.id),
        Some(&started.session.id)
    );

    let read: ProductGenesisSession = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}",
            started.session.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(read.id, started.session.id);
    assert_eq!(read.prompt_revision, started.session.prompt_revision);

    let duplicate: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "idempotency_key": "product-genesis-start-active-conflict",
            "maturity": "mvp"
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(duplicate.message.contains("already active"));

    let messages: AgentChatMessageListResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/agent-chats/{}/messages", binding.chat_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(messages
        .items
        .iter()
        .any(|message| message.id == started.session.source_message_ids[0]));

    let cancelled: ProductGenesisSession = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/cancel",
            started.session.id
        ),
        &token,
        json!({
            "expected_version": started.session.version,
            "reason": "user stopped discovery"
        }),
        StatusCode::OK,
    )
    .await;
    assert!(matches!(
        cancelled.lifecycle,
        api_types::ProductGenesisLifecycle::Cancelled
    ));
    assert_eq!(
        cancelled.failure_reason.as_deref(),
        Some("user stopped discovery")
    );
    let retained_instruction_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat_instruction_revision
         WHERE chat_id = ? AND source_id = ?",
    )
    .bind(&binding.chat_id)
    .bind(&started.session.id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("terminal Genesis retains immutable instruction history");
    assert_eq!(retained_instruction_count, 1);

    let stale: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/cancel",
            started.session.id
        ),
        &token,
        json!({ "expected_version": started.session.version, "reason": "stale" }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert!(stale.message.contains("invalid Product Genesis transition"));

    let empty: ProductGenesisActiveResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/account/main-agent/product-genesis/active",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(empty.session.is_none());
}

#[tokio::test]
async fn product_genesis_requires_main_binding_and_hides_cross_account_sessions() {
    let workspace = common::TestDir::new("product-genesis-setup");
    let harness = common::test_app(workspace.path(), "product-genesis-setup").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let missing: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "idempotency_key": "product-genesis-start-setup-required",
            "maturity": "mvp"
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(missing.message.contains("setup is required"));

    let active: ProductGenesisActiveResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        "/api/v1/account/main-agent/product-genesis/active",
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(active.session.is_none());
}

#[tokio::test]
async fn product_genesis_approval_creates_one_exact_project_and_handoff_and_can_delete_it() {
    let workspace = common::TestDir::new("product-genesis-charter-create");
    let harness = common::test_app(workspace.path(), "product-genesis-charter-create").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let connected = connect_genesis_agent(app, &token, "genesis-charter-create").await;
    let binding: api_types::MainAgentBindingResponse = common::json_request_with_bearer(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;
    let started: ProductGenesisStartResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "idempotency_key": "product-genesis-start-charter-flow",
            "maturity": "mvp",
            "initial_idea": "A small, durable project that needs an exact Charter handoff",
            "preferred_project_agent_identity_id": connected.agent.id
        }),
        StatusCode::CREATED,
    )
    .await;

    let initial_projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter",
            started.session.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    assert!(initial_projection.charter.is_none());
    let selected_agent = initial_projection
        .selected_project_agent
        .expect("Genesis must project the selected Project Agent");
    assert_eq!(selected_agent.identity_id, connected.agent.id);
    assert_eq!(selected_agent.profile_revision_id, connected.profile.id);
    assert_eq!(
        selected_agent.operating_skill_revision,
        "forge.project.orchestration/v1@13"
    );

    let content = exact_charter_content();
    let rendered = services::render_and_digest_charter(&content);
    let charter_id = "charter-genesis-exact";
    let save: ProjectCharterRevision = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter/revisions",
            started.session.id
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": "charter-revision-save-1",
                "authorization": user_authorization(
                    "project_charter.revision.save",
                    "charter-save-event"
                )
            },
            "charter_id": charter_id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": content,
            "rendered_view": rendered.rendered_view,
            "render_version": rendered.render_version,
            "provenance": {
                "author": { "kind": "user", "id": "test-user-id" },
                "operating_skill_revision": "forge.main.project-discovery/v2@1",
                "source_refs": [{
                    "source_kind": "main_chat",
                    "source_id": binding.chat_id,
                    "label": "Main Chat discovery"
                }],
                "change_summary": "Initial approved Project Charter"
            }
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(save.charter_id, charter_id);
    assert_eq!(save.revision_number, 1);
    assert_eq!(save.content_digest, rendered.content_digest);
    assert_eq!(save.render_digest, rendered.render_digest);
    assert!(matches!(
        save.lifecycle,
        api_types::CharterRevisionLifecycle::Proposed
    ));
    assert_eq!(
        save.readiness
            .as_ref()
            .expect("save evaluates Charter readiness")
            .status,
        api_types::CharterReadinessStatus::Ready
    );

    let projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter",
            started.session.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    let charter = projection.charter.expect("saved Charter is projected");
    assert_eq!(charter.id, charter_id);
    assert_eq!(charter.version, 2);
    assert_eq!(
        charter.current_draft_revision_id.as_deref(),
        Some(save.id.as_str())
    );
    assert_eq!(projection.revisions.len(), 1);
    assert_eq!(
        projection
            .current_draft_revision
            .as_ref()
            .expect("draft pointer is projected")
            .content_digest,
        rendered.content_digest
    );
    assert!(projection.approval.is_none());

    let approval_body =
        |content_digest: &str, approval_key: &str, agent: &api_types::ProductAgentSelection| {
            json!({
                "mutation": {
                    "expected_version": charter.version,
                    "expected_digest": rendered.content_digest,
                    "idempotency_key": approval_key,
                    "authorization": user_authorization(
                        "product_genesis.charter_approval",
                        format!("approval-event-{approval_key}")
                    )
                },
                "charter_id": charter_id,
                "revision_id": save.id,
                "content_digest": content_digest,
                "render_digest": rendered.render_digest,
                "expected_charter_version": charter.version,
                "approved_project_name": content.identity.working_name,
                "approved_project_slug": "exact-charter-project",
                "project_mode": "compact",
                "selected_project_agent_identity_id": agent.identity_id,
                "selected_project_agent_profile_revision_id": agent.profile_revision_id,
                "selected_project_agent_operating_skill_revision": agent.operating_skill_revision,
                "selected_project_agent_policy_digest": agent.policy_digest
            })
        };

    let stale_digest: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter/revisions/{}/approve",
            started.session.id, save.id
        ),
        &token,
        approval_body("stale-content-digest", "approval-stale", &selected_agent),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(stale_digest.message.contains("digest"));

    let other_user_token = jwt_for("other-user-id", "other@example.com");
    let mut cross_user_approval_body = approval_body(
        &rendered.content_digest,
        "approval-cross-user",
        &selected_agent,
    );
    cross_user_approval_body["mutation"]["authorization"]["principal"]["id"] =
        json!("other-user-id");
    let cross_user: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter/revisions/{}/approve",
            started.session.id, save.id
        ),
        &other_user_token,
        cross_user_approval_body,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert!(cross_user.message.contains("product_genesis_session"));

    let replacement = connect_genesis_agent(app, &token, "genesis-project-agent").await;
    sqlx::query(
        "UPDATE product_genesis_session
         SET preferred_project_agent_identity_id = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&replacement.agent.id)
    .bind(db::now_rfc3339())
    .bind(&started.session.id)
    .execute(harness.state.db.pool())
    .await
    .expect("rotate the selected Project Agent after the browser projection was read");

    let stale_selection: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter/revisions/{}/approve",
            started.session.id, save.id
        ),
        &token,
        approval_body(
            &rendered.content_digest,
            "approval-stale-agent",
            &selected_agent,
        ),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(stale_selection.code, "project_agent_selection_conflict");

    let refreshed_projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter",
            started.session.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    let selected_agent = refreshed_projection
        .selected_project_agent
        .expect("the refreshed projection uses the current Genesis selection");
    assert_eq!(selected_agent.identity_id, replacement.agent.id);
    assert_eq!(selected_agent.profile_revision_id, replacement.profile.id);

    let approval: ProjectCharterApproval = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter/revisions/{}/approve",
            started.session.id, save.id
        ),
        &token,
        approval_body(&rendered.content_digest, "approval-exact", &selected_agent),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(approval.charter_id, charter_id);
    assert_eq!(approval.charter_revision_id, save.id);
    assert_eq!(approval.charter_content_digest, rendered.content_digest);
    assert_eq!(approval.charter_render_digest, rendered.render_digest);
    assert_eq!(approval.expected_charter_version, charter.version);
    assert_eq!(
        approval.approved_project_name,
        content.identity.working_name
    );
    assert_eq!(
        approval.selected_project_agent_identity_id,
        replacement.agent.id
    );
    assert_eq!(
        approval.selected_project_agent_profile_revision_id,
        replacement.profile.id
    );
    assert!(matches!(
        approval.state,
        api_types::CharterApprovalState::Active
    ));

    let mut cross_user_create_body =
        create_from_approval_body(&approval.id, "create-cross-user", "other-user-event");
    cross_user_create_body["authorization"]["principal"]["id"] = json!("other-user-id");
    let cross_user_create: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &other_user_token,
        cross_user_create_body,
        StatusCode::NOT_FOUND,
    )
    .await;
    assert!(cross_user_create
        .message
        .contains("project_charter_approval"));

    let created: api_types::CreateProjectFromCharterApprovalResponse =
        common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/projects",
            &token,
            create_from_approval_body(&approval.id, "create-exact", "create-event-exact"),
            StatusCode::CREATED,
        )
        .await;
    assert_eq!(created.charter_id, charter_id);
    assert_eq!(created.charter_revision_id, save.id);
    assert!(!created.project_id.is_empty());
    assert!(!created.project_agent_binding_id.is_empty());
    assert!(!created.project_chat_id.is_empty());
    assert!(!created.handoff_id.is_empty());
    assert!(!created.target_message_id.is_empty());
    assert!(!created.target_turn_id.is_empty());

    let (
        principal_type,
        principal_id,
        scope_type,
        scope_id,
        input_digest,
        event_id,
        action_execution_id,
        outcome_json,
    ): (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        String,
    ) = sqlx::query_as(
        "SELECT principal_type, principal_id, scope_type, scope_id, input_digest,
                event_id, agent_action_execution_id, outcome_json
         FROM command_receipt
         WHERE operation = 'product_genesis.create_project_from_approval'
           AND idempotency_key = ?",
    )
    .bind("create-exact")
    .fetch_one(harness.state.db.pool())
    .await
    .expect("direct REST Project-create command receipt exists");
    assert_eq!(principal_type, "user");
    assert_eq!(principal_id, "test-user-id");
    assert_eq!(scope_type, "account");
    assert_eq!(scope_id, "test-user-id");
    assert!(!input_digest.is_empty());
    assert!(!event_id.is_empty());
    assert!(action_execution_id.is_none());
    let outcome: serde_json::Value =
        serde_json::from_str(&outcome_json).expect("frozen Project-create outcome is JSON");
    assert_eq!(
        outcome["project_id"].as_str(),
        Some(created.project_id.as_str())
    );
    assert_eq!(
        outcome["project_chat_id"].as_str(),
        Some(created.project_chat_id.as_str())
    );

    let genesis = services::ProductGenesisService::for_sqlite(harness.state.db.clone());
    let completed = genesis
        .get(&started.session.id)
        .await
        .expect("Genesis history remains readable");
    assert!(matches!(
        completed.lifecycle,
        ProductGenesisLifecycle::HandedOff
    ));
    assert_eq!(
        completed.project_id.as_deref(),
        Some(created.project_id.as_str())
    );
    assert_eq!(completed.charter_id.as_deref(), Some(charter_id));
    assert_eq!(
        completed.charter_revision_id.as_deref(),
        Some(save.id.as_str())
    );
    assert_eq!(
        completed.charter_approval_id.as_deref(),
        Some(approval.id.as_str())
    );
    assert_eq!(
        completed.handoff_id.as_deref(),
        Some(created.handoff_id.as_str())
    );

    let project: ProjectResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", created.project_id),
        &token,
        StatusCode::OK,
    )
    .await;
    assert_eq!(project.id, created.project_id);
    assert_eq!(project.name, content.identity.working_name);
    assert_eq!(project.charter_status, "charter_backed");
    assert!(!project.charter_setup_required);
    assert_eq!(project.current_charter_id.as_deref(), Some(charter_id));
    assert_eq!(
        project.current_charter_revision_id.as_deref(),
        Some(save.id.as_str())
    );
    assert_eq!(project.current_charter_version, charter.version + 1);
    assert!(project.primary_milestone_id.is_some());

    let approval_projection: ProductGenesisCharterResponse = common::empty_request_with_bearer(
        app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter",
            started.session.id
        ),
        &token,
        StatusCode::OK,
    )
    .await;
    let consumed = approval_projection
        .approval
        .expect("consumed receipt remains visible");
    assert_eq!(consumed.id, approval.id);
    assert_eq!(
        consumed.consumed_by_project_id.as_deref(),
        Some(created.project_id.as_str())
    );
    assert!(matches!(
        consumed.state,
        api_types::CharterApprovalState::Consumed
    ));

    let handoff = sqlx::query(
        "SELECT source_chat_id, target_chat_id, target_message_id, target_turn_job_id,
                status, content, source_revisions_json
         FROM agent_handoff WHERE id = ?",
    )
    .bind(&created.handoff_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("atomic Charter handoff exists");
    assert_eq!(handoff.get::<String, _>("source_chat_id"), binding.chat_id);
    assert_eq!(
        handoff.get::<String, _>("target_chat_id"),
        created.project_chat_id
    );
    assert_eq!(
        handoff.get::<String, _>("target_message_id"),
        created.target_message_id
    );
    assert_eq!(
        handoff.get::<String, _>("target_turn_job_id"),
        created.target_turn_id
    );
    assert_eq!(handoff.get::<String, _>("status"), "delivered");
    let handoff_content = handoff.get::<String, _>("content");
    assert!(handoff_content.contains(&save.id));
    assert!(!handoff_content.contains("No other material constraints are known at approval time."));
    let provenance: serde_json::Value =
        serde_json::from_str(&handoff.get::<String, _>("source_revisions_json"))
            .expect("handoff provenance is JSON");
    assert_eq!(provenance["charter"]["id"], charter_id);
    assert_eq!(provenance["charter"]["revision_id"], save.id);
    assert_eq!(
        provenance["charter"]["content_digest"],
        rendered.content_digest
    );
    assert_eq!(
        provenance["charter"]["render_digest"],
        rendered.render_digest
    );
    assert_eq!(provenance["approval"]["id"], approval.id);
    assert_eq!(
        provenance["redaction_manifest"]["excluded_knowledge_item_ids"],
        json!(["known-constraints"])
    );
    assert_eq!(provenance["target"]["chat_id"], created.project_chat_id);
    assert_eq!(
        provenance["target"]["binding_id"],
        created.project_agent_binding_id
    );

    let message_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message WHERE chat_id = ? AND id = ?")
            .bind(&created.project_chat_id)
            .bind(&created.target_message_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("target handoff message exists");
    assert_eq!(message_count, 1);
    let turn_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job WHERE chat_id = ? AND id = ?")
            .bind(&created.project_chat_id)
            .bind(&created.target_turn_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("target turn job exists");
    assert_eq!(turn_count, 1);
    let chat_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat WHERE project_id = ? AND kind = 'project'",
    )
    .bind(&created.project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("one Project Chat exists");
    assert_eq!(chat_count, 1);
    let active_binding_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created.project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("one active Project Agent binding exists");
    assert_eq!(active_binding_count, 1);
    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project WHERE id = ?")
        .bind(&created.project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("one Project exists");
    assert_eq!(project_count, 1);

    let replayed: api_types::CreateProjectFromCharterApprovalResponse =
        common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/projects",
            &token,
            create_from_approval_body(&approval.id, "create-exact", "create-event-exact"),
            StatusCode::CREATED,
        )
        .await;
    // The command receipt freezes the materialized Project/handoff identity,
    // but the response also carries the current provisioning projection.  A
    // response-loss replay reconciles that projection and may legitimately
    // advance its attempt/version/retry fields, so compare the frozen
    // identity separately from the live setup view.
    assert_eq!(replayed.project_id, created.project_id);
    assert_eq!(
        replayed.project_agent_binding_id,
        created.project_agent_binding_id
    );
    assert_eq!(replayed.project_chat_id, created.project_chat_id);
    assert_eq!(replayed.charter_id, created.charter_id);
    assert_eq!(replayed.charter_revision_id, created.charter_revision_id);
    assert_eq!(replayed.handoff_id, created.handoff_id);
    assert_eq!(replayed.target_message_id, created.target_message_id);
    assert_eq!(replayed.target_turn_id, created.target_turn_id);

    let replayed_setup =
        services::load_project_execution_setup(&harness.state.db, &replayed.project_id)
            .await
            .expect("replayed Project setup projection remains readable");
    assert_eq!(replayed.execution_setup, replayed_setup);
    assert_eq!(replayed.execution_setup.project_id, replayed.project_id);

    let (replayed_input_digest, replayed_outcome_json): (String, String) = sqlx::query_as(
        "SELECT input_digest, outcome_json
         FROM command_receipt
         WHERE operation = 'product_genesis.create_project_from_approval'
           AND idempotency_key = ?",
    )
    .bind("create-exact")
    .fetch_one(harness.state.db.pool())
    .await
    .expect("replayed Project-create receipt remains durable");
    assert_eq!(replayed_input_digest, input_digest);
    assert_eq!(replayed_outcome_json, outcome_json);

    let changed_authorization: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_from_approval_body(&approval.id, "create-exact", "create-event-replay"),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(changed_authorization.message.contains("idempotency"));

    let mismatched_key: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_from_approval_body(&approval.id, "create-different", "create-event-fork"),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(mismatched_key.message.contains("idempotency"));

    let project_count_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project")
        .fetch_one(harness.state.db.pool())
        .await
        .expect("project count remains queryable");
    assert_eq!(project_count_after, 1);

    let deleted = common::raw_empty_request(
        app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", created.project_id),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    for (table, project_column) in [
        ("project", "id"),
        ("project_agent_binding", "project_id"),
        ("project_admission_receipt", "project_id"),
    ] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE {project_column} = ?"
        ))
        .bind(&created.project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap_or_else(|error| panic!("{table} teardown count: {error}"));
        assert_eq!(count, 0, "{table} survives guarded Project teardown");
    }
    let guard_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_deletion_guard WHERE project_id = ?")
            .bind(&created.project_id)
            .fetch_one(harness.state.db.pool())
            .await
            .expect("Project teardown clears its guard");
    assert_eq!(guard_count, 0);
}

async fn connect_genesis_agent(
    app: &Router,
    token: &str,
    name: &str,
) -> ConnectedEmbeddedAgentResponse {
    common::connect_embedded_agent(
        app,
        token,
        name,
        "genesis-test",
        "fixture-secret",
        json!({"permissions": ["read_account", "read_project", "handoff"]}),
        json!({"allowed": ["read_account", "read_project", "handoff"]}),
    )
    .await
}

fn user_authorization(action: &str, event_id: impl Into<String>) -> serde_json::Value {
    static AUTHORIZATION_OCCURRED_AT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    json!({
        "principal": { "kind": "user", "id": "test-user-id" },
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id.into(),
        "occurred_at": AUTHORIZATION_OCCURRED_AT.get_or_init(db::now_rfc3339)
    })
}

fn create_from_approval_body(
    approval_id: &str,
    idempotency_key: &str,
    event_id: &str,
) -> serde_json::Value {
    json!({
        "approval_id": approval_id,
        "idempotency_key": idempotency_key,
        "authorization": user_authorization(
            "product_genesis.create_project_from_approval",
            event_id
        )
    })
}

fn exact_charter_content() -> api_types::ProjectCharterContent {
    api_types::ProjectCharterContent {
        identity: api_types::CharterIdentity {
            working_name: "Exact Charter Project".to_owned(),
            slug_proposal: Some("exact-charter-project".to_owned()),
            one_line_vision: "A focused project with an auditable handoff.".to_owned(),
            maturity: api_types::ProductMaturity::Mvp,
            lifecycle_intent: Some("validate the smallest useful workflow".to_owned()),
            project_type: Some("product".to_owned()),
            value_proposition: Some("Keep approved intent durable and traceable.".to_owned()),
        },
        problem_and_people: api_types::CharterProblemAndPeople {
            problem_or_opportunity:
                "Project intent is otherwise lost between discovery and execution.".to_owned(),
            target_users: vec!["Forge builders".to_owned()],
            beneficiaries: vec!["Project collaborators".to_owned()],
            jobs_pains_opportunity: vec!["Turn a rough idea into an executable brief.".to_owned()],
            current_alternatives: vec!["Unversioned chat notes".to_owned()],
            stakeholders: vec!["Project owner".to_owned()],
            excluded_audiences: vec!["Unrelated projects".to_owned()],
        },
        core_experience: api_types::CharterCoreExperience {
            primary_outcome: "A Project Agent starts from one approved, bounded Charter."
                .to_owned(),
            core_loop: Some("discover, approve, hand off, validate".to_owned()),
            principal_journeys: Vec::new(),
        },
        scope: api_types::CharterScope {
            must_have_outcomes: vec!["Persist the approved Charter and handoff.".to_owned()],
            required_deliverables: vec!["One Project Chat with one queued turn.".to_owned()],
            later_possibilities: vec![
                "Expand the Charter through Project-local revisions.".to_owned()
            ],
            explicit_non_goals: vec!["Managing unrelated Projects".to_owned()],
        },
        success: api_types::CharterSuccessBoundary {
            qualitative_outcome: Some(
                "The Project Agent can continue without restating intent.".to_owned(),
            ),
            success_signals: vec!["The handoff preserves exact Charter digests.".to_owned()],
            acceptance_statements: vec!["A replay does not create a second Project.".to_owned()],
            required_evidence: vec!["Database assertions and API integration test.".to_owned()],
            non_claims: vec!["This does not prove repository implementation quality.".to_owned()],
        },
        constraints_and_risks: api_types::CharterConstraintsAndRisks {
            product: vec!["Single-user local-first operation.".to_owned()],
            time_and_budget: Vec::new(),
            technology: vec!["SQLite and the existing Forge API.".to_owned()],
            data: vec!["Do not copy hidden Main Chat history.".to_owned()],
            integrations: Vec::new(),
            security_privacy_compliance: vec!["Require explicit user approval.".to_owned()],
            accessibility: Vec::new(),
            operations: Vec::new(),
            migration: Vec::new(),
            launch: Vec::new(),
            agent_authority: vec!["Project Agent remains Project-scoped.".to_owned()],
            risks: Vec::new(),
        },
        knowledge_ledger: api_types::CharterKnowledgeLedger {
            items: vec![api_types::CharterKnowledgeItem {
                id: "known-constraints".to_owned(),
                statement: "No other material constraints are known at approval time.".to_owned(),
                kind: api_types::CharterKnowledgeKind::UserDecision,
                normative: false,
                transfer_approved: false,
                provenance: Vec::new(),
                confidence: Some(api_types::CharterConfidence::NotApplicable),
                observed_at: None,
                freshness_expires_at: None,
                impact: None,
                owner: None,
                default_value: None,
                revisit_trigger: None,
                falsification_evidence: None,
                blocking: false,
            }],
        },
        handoff_note: Some(api_types::CharterHandoffNote {
            recommended_first_action: Some(
                "Create the first Project-local execution plan.".to_owned(),
            ),
            bounded_summary: Some(
                "Start by validating the approved problem and outcome.".to_owned(),
            ),
            unresolved_item_ids: Vec::new(),
        }),
    }
}

fn jwt_for(subject: &str, email: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after epoch")
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "sub": subject,
            "email": email,
            "is_admin": false,
            "iat": now,
            "exp": now + 900,
        }),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode alternate test jwt")
}

#[tokio::test]
async fn product_genesis_guided_setup_sets_maturity_once_with_optimistic_concurrency() {
    let workspace = common::TestDir::new("product-genesis-guided-setup");
    let harness = common::test_app(workspace.path(), "product-genesis-guided-setup").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let connected = connect_genesis_agent(app, &token, "genesis-main").await;

    let _binding: api_types::MainAgentBindingResponse = common::json_request_with_bearer(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({
            "identity_id": connected.agent.id,
            "profile_id": connected.profile.id,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        StatusCode::OK,
    )
    .await;

    // Start with omitted parameters defaults to mvp
    let started: ProductGenesisStartResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        &token,
        json!({
            "idempotency_key": "product-genesis-start-guided-setup",
            "initial_idea": "An MVP product idea"
        }),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(started.session.maturity, api_types::ProductMaturity::Mvp);
    assert_eq!(started.session.version, 1); // source provenance commits with creation

    // Apply guided setup with expected version
    let updated: ProductGenesisSession = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/guided-setup",
            started.session.id
        ),
        &token,
        json!({
            "expected_version": started.session.version,
            "maturity": "production",
            "provenance": "interaction-123"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(updated.maturity, api_types::ProductMaturity::Production);
    assert_eq!(updated.version, started.session.version + 1);

    // Re-applying guided setup fails with 409 Conflict (set-once)
    let reapply: ErrorResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/guided-setup",
            started.session.id
        ),
        &token,
        json!({
            "expected_version": updated.version,
            "maturity": "critical"
        }),
        StatusCode::CONFLICT,
    )
    .await;
    assert!(reapply.message.contains("already been applied"));
}
