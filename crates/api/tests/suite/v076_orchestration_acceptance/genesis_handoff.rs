//! Atomic Project Genesis handoff and explicit legacy adoption.

use super::*;

#[tokio::test]
async fn v076_genesis_handoff_is_atomic_and_legacy_adoption_is_explicit() {
    let workspace = common::TestDir::new("v076-genesis-handoff-adoption");
    let harness = common::test_app(workspace.path(), "v076-genesis-handoff-adoption").await;
    let app = &harness.app;
    let token = common::test_jwt();

    let genesis = create_genesis_project(app, &token, "v076-genesis").await;
    let created_project_id = genesis.project_id.clone();

    // Product Genesis no longer exposes the old mutable `/ready` transition;
    // exact Charter approval is the only route into Project creation.
    let legacy_ready = common::raw_empty_request(
        app,
        Method::POST,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/ready",
            genesis.genesis_session_id
        ),
    )
    .await;
    assert_eq!(legacy_ready.status(), StatusCode::NOT_FOUND);

    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project WHERE id = ?")
        .bind(&created_project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("created project count");
    let chat_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_chat WHERE project_id = ? AND kind = 'project'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("created project chat count");
    let binding_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("created project binding count");
    assert_eq!(project_count, 1);
    assert_eq!(chat_count, 1);
    assert_eq!(binding_count, 1);
    let approval = sqlx::query(
        "SELECT approval_type, lifecycle, approving_principal_type,
                approving_principal_id, consumed_project_id
         FROM project_charter_approval WHERE id = ?",
    )
    .bind(&genesis.approval_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("consumed user Charter approval");
    assert_eq!(
        approval.get::<String, _>("approval_type"),
        "project_creation"
    );
    assert_eq!(approval.get::<String, _>("lifecycle"), "consumed");
    assert_eq!(
        approval.get::<String, _>("approving_principal_type"),
        "user"
    );
    assert_eq!(
        approval.get::<String, _>("approving_principal_id"),
        "test-user-id"
    );
    assert_eq!(
        approval
            .get::<Option<String>, _>("consumed_project_id")
            .as_deref(),
        Some(created_project_id.as_str())
    );

    let handoff = sqlx::query(
        "SELECT source_chat_id, target_chat_id, target_message_id, target_turn_job_id, status
         FROM agent_handoff WHERE target_chat_id = ?",
    )
    .bind(&genesis.project_chat_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("atomic handoff row");
    assert_eq!(
        handoff.get::<String, _>("source_chat_id"),
        genesis.main_chat_id
    );
    assert_eq!(
        handoff.get::<String, _>("target_chat_id"),
        genesis.project_chat_id
    );
    assert!(!handoff.get::<String, _>("target_message_id").is_empty());
    assert!(!handoff.get::<String, _>("target_turn_job_id").is_empty());
    assert_eq!(handoff.get::<String, _>("status"), "delivered");

    // Rebinding rotates only current authority. The immutable admission
    // receipt and its Genesis handoff stay Project-owned and a fresh turn can
    // immediately admit against the replacement binding.
    let original_authority: (String, i64) = sqlx::query_as(
        "SELECT admission_receipt_id, version
         FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("original complete binding authority");
    let replacement_agent = connect_agent(
        app,
        &token,
        "v117-replacement-project-agent",
        &["read_project", "handoff", "propose_task"],
    )
    .await;
    let replacement_identity = required_string(&replacement_agent, &["agent", "id"]);
    let rebound = request_json(
        app,
        Method::PUT,
        &format!("/api/v1/projects/{created_project_id}/project-agent"),
        &token,
        json!({
            "identity_id": replacement_identity,
            "expected_version": original_authority.1,
            "permission_ceiling": {},
            "autonomy_policy": {},
            "subscriptions": [],
            "wake_budget": 10
        }),
        &[StatusCode::OK],
    )
    .await;
    let replacement_binding_id = required_string(&rebound, &["id"]);
    let rebound_authority: (String, String, String, String, i64) = sqlx::query_as(
        "SELECT admission_receipt_id, charter_approval_id, charter_id,
                charter_revision_id, charter_setup_required
         FROM project_agent_binding WHERE id = ?",
    )
    .bind(&replacement_binding_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("replacement complete binding authority");
    assert_eq!(rebound_authority.0, original_authority.0);
    assert_eq!(rebound_authority.1, genesis.approval_id);
    assert_eq!(rebound_authority.2, genesis.charter_id);
    assert_eq!(rebound_authority.3, genesis.charter_revision_id);
    assert_eq!(rebound_authority.4, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_admission_receipt WHERE project_id = ?",
        )
        .bind(&created_project_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("one stable admission receipt"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_handoff WHERE target_chat_id = ?",
        )
        .bind(&genesis.project_chat_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("one Genesis handoff after rebind"),
        1
    );
    let mcp_agent = connect_agent(
        app,
        &token,
        "v117-mcp-replacement-project-agent",
        &["read_project", "handoff", "propose_task"],
    )
    .await;
    let mcp_identity = required_string(&mcp_agent, &["agent", "id"]);
    let (mcp_status, mcp_body) = raw_request(
        app,
        Method::POST,
        "/mcp",
        &token,
        Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": "v117-mcp-rebind",
                "method": "tools/call",
                "params": {
                    "name": "forge_set_project_agent",
                    "arguments": {
                        "project_id": created_project_id,
                        "identity_id": mcp_identity,
                        "expected_version": rebound["version"],
                        "permission_ceiling": {},
                        "autonomy_policy": {},
                        "subscriptions": [],
                        "wake_budget": 10
                    }
                }
            })
            .to_string(),
        ),
        Some("application/json"),
    )
    .await;
    assert_eq!(mcp_status, StatusCode::OK);
    let mcp_result: Value =
        serde_json::from_slice(&mcp_body).expect("MCP binding response is JSON");
    assert!(mcp_result.get("error").is_none(), "{mcp_result}");
    let mcp_binding: (String, String, String, String, i64, i64) = sqlx::query_as(
        "SELECT id, identity_id, admission_receipt_id, charter_approval_id,
                charter_setup_required, version
         FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("MCP replacement complete binding authority");
    assert_eq!(mcp_binding.1, mcp_identity);
    assert_eq!(mcp_binding.2, original_authority.0);
    assert_eq!(mcp_binding.3, genesis.approval_id);
    assert_eq!(mcp_binding.4, 0);
    let admitted_after_rebind = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", genesis.project_chat_id),
        &token,
        json!({
            "content": "Continue from the approved Charter after rebinding.",
            "dedupe_key": "v117-fresh-turn-after-rebind"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let admitted_turn_id = required_string(&admitted_after_rebind, &["turn_job", "id"]);
    let admitted_authority: (String, String, i64, String) = sqlx::query_as(
        "SELECT responder_binding_id, responder_identity_id, profile_version,
                tool_policy_digest
         FROM agent_chat_turn_job WHERE id = ?",
    )
    .bind(&admitted_turn_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("fresh turn frozen to replacement authority");
    assert_eq!(admitted_authority.0, mcp_binding.0);
    assert_eq!(admitted_authority.1, mcp_identity);
    let edited_profile_policy =
        json!({"permissions": ["read_project"], "revision": "after-rebind"});
    let edited_profile = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agents/{mcp_identity}/profiles/connect"),
        &token,
        json!({
            "version": mcp_agent["agent"]["version"],
            "credential_id": mcp_agent["credential_handle"]["id"],
            "model": "v076-acceptance-model-after-rebind",
            "system_prompt": null,
            "permission_policy": null,
            "tool_policy": edited_profile_policy,
            "context_tokens": null,
            "max_input_tokens": null,
            "max_output_tokens": null
        }),
        &[StatusCode::OK],
    )
    .await;
    let edited_profile_id = required_string(&edited_profile, &["profile", "id"]);
    let admitted_after_profile_edit = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", genesis.project_chat_id),
        &token,
        json!({
            "content": "Continue using the replacement agent's current Profile.",
            "dedupe_key": "v117-fresh-turn-after-profile-edit"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let edited_turn_id = required_string(&admitted_after_profile_edit, &["turn_job", "id"]);
    let edited_authority: (String, String, i64, String) = sqlx::query_as(
        "SELECT responder_binding_id, profile_id, profile_version, tool_policy_digest
         FROM agent_chat_turn_job WHERE id = ?",
    )
    .bind(&edited_turn_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("fresh turn resolves edited current Profile");
    assert_eq!(edited_authority.0, mcp_binding.0);
    assert_eq!(edited_authority.1, edited_profile_id);
    assert!(edited_authority.2 > admitted_authority.2);
    assert_ne!(edited_authority.3, admitted_authority.3);

    // Publish a successor under the same skill key. Both ids are derived from
    // whatever the migrations currently ship, so a new published revision does
    // not turn this case into a primary-key collision on a hardcoded number.
    let (current_skill_revision_id, next_skill_revision_id, next_skill_revision) =
        project_skill_revisions(&harness).await;
    sqlx::query(
        "INSERT INTO operating_skill_revision (
            id, operating_skill_id, skill_key, revision, schema_version,
            render_version, canonical_body, policy_json, policy_digest,
            content_digest, created_by_type, created_at
         )
         SELECT ?, operating_skill_id,
                skill_key, ?, schema_version, render_version, canonical_body,
                policy_json, policy_digest, content_digest, 'system', ?
         FROM operating_skill_revision
         WHERE id = ?",
    )
    .bind(&next_skill_revision_id)
    .bind(next_skill_revision)
    .bind(Utc::now().to_rfc3339())
    .bind(&current_skill_revision_id)
    .execute(harness.state.db.pool())
    .await
    .expect("seed same-key Project operating-skill revision");
    sqlx::query(
        "UPDATE operating_skill
         SET current_revision_id = ?,
             version = version + 1, updated_at = ?
         WHERE skill_key = 'forge.project.orchestration/v1'",
    )
    .bind(&next_skill_revision_id)
    .bind(Utc::now().to_rfc3339())
    .execute(harness.state.db.pool())
    .await
    .expect("activate same-key Project operating-skill revision");
    let skill_rebound = request_json(
        app,
        Method::PUT,
        &format!("/api/v1/projects/{created_project_id}/project-agent"),
        &token,
        json!({
            "identity_id": mcp_identity,
            "expected_version": mcp_binding.5,
            "permission_ceiling": {},
            "autonomy_policy": {},
            "subscriptions": [],
            "wake_budget": 10
        }),
        &[StatusCode::OK],
    )
    .await;
    let skill_binding_id = required_string(&skill_rebound, &["id"]);
    let skill_authority: (String, String) = sqlx::query_as(
        "SELECT admission_receipt_id, operating_skill_revision_id
         FROM project_agent_binding WHERE id = ?",
    )
    .bind(&skill_binding_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("same-key skill replacement authority");
    assert_eq!(skill_authority.0, original_authority.0);
    assert_eq!(skill_authority.1, next_skill_revision_id);
    let admitted_after_skill_revision = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", genesis.project_chat_id),
        &token,
        json!({
            "content": "Continue under the current same-key operating skill.",
            "dedupe_key": "v117-fresh-turn-after-skill-revision"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let skill_turn_id = required_string(&admitted_after_skill_revision, &["turn_job", "id"]);
    let skill_turn: (String, String) = sqlx::query_as(
        "SELECT responder_binding_id, operating_skill_revision_id
         FROM agent_chat_turn_job WHERE id = ?",
    )
    .bind(&skill_turn_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("fresh turn uses current same-key operating skill");
    assert_eq!(skill_turn.0, skill_binding_id);
    assert_eq!(skill_turn.1, next_skill_revision_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_handoff WHERE target_chat_id = ?",
        )
        .bind(&genesis.project_chat_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("no handoff on operating-skill rotation"),
        1
    );
    sqlx::query(
        "UPDATE operating_skill
         SET current_revision_id = ?,
             version = version + 1, updated_at = ?
         WHERE skill_key = 'forge.project.orchestration/v1'",
    )
    .bind(&current_skill_revision_id)
    .bind(Utc::now().to_rfc3339())
    .execute(harness.state.db.pool())
    .await
    .expect("restore fixture Project operating skill");

    let amendment_projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{created_project_id}/charter"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let amendment_base_version = amendment_projection["charter"]["version"]
        .as_i64()
        .expect("Charter version before amendment");
    let amendment_content = charter_content(
        "v076-genesis Project",
        "the amended approved outcome remains observable",
    );
    let amendment_rendered = services::render_and_digest_charter(&amendment_content);
    let amendment_revision = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{created_project_id}/charter/revisions"),
        &token,
        json!({
            "mutation": {
                "expected_version": amendment_base_version,
                "expected_digest": genesis.charter_content_digest,
                "idempotency_key": "v117-charter-amendment-save",
                "authorization": user_authorization(
                    "project_charter.revision.save",
                    "v117-charter-amendment-save-event"
                )
            },
            "charter_id": genesis.charter_id,
            "base_revision_id": genesis.charter_revision_id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": amendment_content,
            "rendered_view": amendment_rendered.rendered_view,
            "render_version": amendment_rendered.render_version,
            "provenance": user_provenance("Project-local Charter amendment")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let amendment_revision_id = required_string(&amendment_revision, &["id"]);
    let amendment_projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{created_project_id}/charter"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let amendment_charter_version = amendment_projection["charter"]["version"]
        .as_i64()
        .expect("Charter version for amendment approval");
    let amendment_project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{created_project_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let amendment_project_version = amendment_project["version"]
        .as_i64()
        .expect("Project version for amendment approval");
    let amendment_approval = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{created_project_id}/charter/revisions/{amendment_revision_id}/approve"
        ),
        &token,
        json!({
            "mutation": {
                "expected_version": amendment_charter_version,
                "expected_digest": amendment_rendered.content_digest,
                "idempotency_key": "v117-charter-amendment-approve",
                "authorization": user_authorization(
                    "project_charter.approval",
                    "v117-charter-amendment-approve-event"
                )
            },
            "charter_id": genesis.charter_id,
            "revision_id": amendment_revision_id,
            "content_digest": amendment_rendered.content_digest,
            "render_digest": amendment_rendered.render_digest,
            "expected_charter_version": amendment_charter_version,
            "expected_project_version": amendment_project_version,
            "approved_project_name": "v076-genesis Project",
            "approved_project_slug": "v076-genesis-project",
            "project_mode": "compact",
            "selected_project_agent_identity_id": mcp_identity,
            "selected_project_agent_profile_revision_id": edited_profile_id,
            "selected_project_agent_operating_skill_revision":
                common::current_project_operating_skill_revision(&harness).await,
            "selected_project_agent_policy_digest": project_policy_digest(&edited_profile_policy)
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(
        amendment_approval["approval_type"],
        json!("charter_amendment")
    );
    let amendment_approval_id = required_string(&amendment_approval, &["id"]);
    let amendment_binding: (String, String, String, String) = sqlx::query_as(
        "SELECT id, admission_receipt_id, charter_approval_id,
                charter_revision_id
         FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&created_project_id)
    .fetch_one(harness.state.db.pool())
    .await
    .expect("Charter amendment rotates current binding authority");
    assert_eq!(amendment_binding.1, original_authority.0);
    assert_eq!(amendment_binding.2, amendment_approval_id);
    assert_eq!(amendment_binding.3, amendment_revision_id);
    let admitted_after_amendment = request_json(
        app,
        Method::POST,
        &format!("/api/v1/agent-chats/{}/messages", genesis.project_chat_id),
        &token,
        json!({
            "content": "Continue from the current amended Charter without a new Main handoff.",
            "dedupe_key": "v117-fresh-turn-after-charter-amendment"
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let amendment_turn_id = required_string(&admitted_after_amendment, &["turn_job", "id"]);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT responder_binding_id FROM agent_chat_turn_job WHERE id = ?",
        )
        .bind(&amendment_turn_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("fresh turn uses amendment binding"),
        amendment_binding.0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_handoff WHERE target_chat_id = ?",
        )
        .bind(&genesis.project_chat_id)
        .fetch_one(harness.state.db.pool())
        .await
        .expect("no handoff on Charter amendment"),
        1
    );

    // Replaying the exact approval receipt is a no-op and returns the exact
    // original response.  Reusing the key for a different receipt conflicts.
    let create_body = genesis.create_request.clone();
    let first = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_body.clone(),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let replay = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        create_body,
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    // The command receipt freezes the materialized Project/handoff identity,
    // while execution_setup is a live provisioning projection that may have
    // advanced between the original response and a response-loss replay.
    for field in [
        "project_id",
        "project_agent_binding_id",
        "project_chat_id",
        "charter_id",
        "charter_revision_id",
        "handoff_id",
        "target_message_id",
        "target_turn_id",
    ] {
        assert_eq!(
            first[field], genesis.create_response[field],
            "frozen {field}"
        );
        assert_eq!(
            replay[field], genesis.create_response[field],
            "frozen {field}"
        );
    }
    assert!(first["execution_setup"].is_object());
    assert!(replay["execution_setup"].is_object());
    let current_setup =
        services::load_project_execution_setup(&harness.state.db, &genesis.project_id)
            .await
            .expect("replayed Project setup projection remains readable");
    assert_eq!(
        replay["execution_setup"],
        serde_json::to_value(current_setup).expect("setup projection serializes")
    );
    let conflict = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({
            "approval_id": genesis.approval_id,
            "idempotency_key": genesis.create_idempotency_key,
            "authorization": {
                "principal": {"kind": "user", "id": "test-user-id"},
                "authorization_basis": "altered_authorization",
                "action": "product_genesis.create_project_from_approval",
                "event_id": "v076-create-altered-event",
                "occurred_at": Utc::now().to_rfc3339()
            }
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert!(conflict
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|message| message.contains("idempotency")));

    // Direct legacy projects remain setup_required until the user explicitly
    // adopts and approves a Charter revision.
    let legacy = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "V076 legacy project"}),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let legacy_id = required_string(&legacy, &["id"]);
    assert_eq!(
        required_string(&legacy, &["charter_status"]),
        "legacy_unverified"
    );
    assert_eq!(legacy["charter_setup_required"], json!(true));

    let legacy_agent = connect_agent(
        app,
        &token,
        "v076-legacy-project",
        &["read_project", "handoff", "propose_task"],
    )
    .await;
    let legacy_identity = required_string(&legacy_agent, &["agent", "id"]);
    let legacy_profile = required_string(&legacy_agent, &["profile", "id"]);
    let legacy_content = charter_content("V076 Legacy Adopted", "legacy adoption is explicit");
    let legacy_rendered = services::render_and_digest_charter(&legacy_content);
    let legacy_save = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{legacy_id}/charter/revisions"),
        &token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": "v076-legacy-charter-save",
                "authorization": user_authorization("project_charter.revision.save", "v076-legacy-save")
            },
            "charter_id": "v076-legacy-charter",
            "project_mode": "compact",
            "maturity": "mvp",
            "content": legacy_content,
            "rendered_view": legacy_rendered.rendered_view,
            "render_version": legacy_rendered.render_version,
            "provenance": user_provenance("legacy Charter adoption")
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    let legacy_revision_id = required_string(&legacy_save, &["id"]);
    let legacy_projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}/charter"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let legacy_charter_version = legacy_projection["charter"]["version"]
        .as_i64()
        .expect("legacy charter version");
    let legacy_project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let legacy_project_version = legacy_project["version"]
        .as_i64()
        .expect("legacy project version");
    let legacy_policy = project_policy_digest(&legacy_agent["profile"]["tool_policy"]);
    let adopted = request_json(
        app,
        Method::POST,
        &format!("/api/v1/projects/{legacy_id}/charter/revisions/{legacy_revision_id}/approve"),
        &token,
        json!({
            "mutation": {
                "expected_version": legacy_charter_version,
                "expected_digest": legacy_rendered.content_digest,
                "idempotency_key": "v076-legacy-charter-approve",
                "authorization": user_authorization("project_charter.approval", "v076-legacy-approve")
            },
            "charter_id": "v076-legacy-charter",
            "revision_id": legacy_revision_id,
            "content_digest": legacy_rendered.content_digest,
            "render_digest": legacy_rendered.render_digest,
            "expected_charter_version": legacy_charter_version,
            "expected_project_version": legacy_project_version,
            "approved_project_name": "V076 Legacy Adopted",
            "approved_project_slug": "v076-legacy-adopted",
            "project_mode": "compact",
            "selected_project_agent_identity_id": legacy_identity,
            "selected_project_agent_profile_revision_id": legacy_profile,
            "selected_project_agent_operating_skill_revision":
                common::current_project_operating_skill_revision(&harness).await,
            "selected_project_agent_policy_digest": legacy_policy
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await;
    assert_eq!(adopted["approval_type"], json!("adoption"));
    let legacy_after = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{legacy_id}"),
        &token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(legacy_after["charter_status"], json!("charter_backed"));
    assert_eq!(legacy_after["charter_setup_required"], json!(false));
}

#[tokio::test]
async fn genesis_charter_read_database_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "genesis-charter-read-errors").await;
    harness.stop_step_worker();
    let genesis =
        create_genesis_project(&harness.app, &common::test_jwt(), "genesis-read-error").await;
    sqlx::query(
        "ALTER TABLE project_charter_approval RENAME COLUMN created_at TO unavailable_created_at",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let body: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/account/main-agent/product-genesis/{}/charter",
            genesis.genesis_session_id
        ),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({"code":"internal_error", "message":"Project artifact mutation failed",
        "details":null, "request_id":body["request_id"]})
    );
}
