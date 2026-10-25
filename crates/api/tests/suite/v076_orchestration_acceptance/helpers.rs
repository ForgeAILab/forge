//! Shared fixtures for V076 orchestration acceptance tests.

use super::*;

pub(super) const PROVIDER_SECRET: &str = "v076-acceptance-provider-secret";

#[derive(Debug, Clone)]
pub(super) struct GenesisFixture {
    pub(super) project_id: String,
    pub(super) project_version: i64,
    pub(super) project_identity_id: String,
    pub(super) project_profile_id: String,
    pub(super) project_chat_id: String,
    pub(super) main_chat_id: String,
    pub(super) genesis_session_id: String,
    pub(super) create_response: Value,
    pub(super) create_request: Value,
    pub(super) approval_id: String,
    pub(super) create_idempotency_key: String,
    pub(super) charter_id: String,
    pub(super) charter_revision_id: String,
    pub(super) charter_version: i64,
    pub(super) charter_content_digest: String,
    pub(super) charter_render_digest: String,
    pub(super) milestone_id: String,
    pub(super) milestone_version: i64,
    pub(super) milestone_definition_revision_id: String,
    pub(super) milestone_acceptance_check_id: String,
    pub(super) milestone_acceptance_check_description: String,
}

#[derive(Debug, Clone)]
pub(super) struct BaselineFixture {
    pub(super) baseline_version: i64,
    pub(super) approval_expected_baseline_version: i64,
    pub(super) approval_expected_project_version: i64,
    pub(super) content_digest: String,
    pub(super) render_digest: String,
    pub(super) approval_id: String,
    pub(super) approval_authorization: Value,
}

pub(super) async fn chat_preview(
    harness: &common::Harness,
    chat_id: &str,
    turn_id: &str,
) -> services_agents::agent_chat_turn_worker::AgentChatPromptPreview {
    let job = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.chat_id, chat_id);
    services_agents::FederatedAgentChatTurnRunner::new(
        std::sync::Arc::clone(&harness.state.db),
        std::sync::Arc::clone(&harness.state.embedded_agent_service),
        std::sync::Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    )
    .preview_prompt(&job)
    .await
    .unwrap()
}

pub(super) async fn admit_preview_turn(
    harness: &common::Harness,
    chat_id: &str,
    text: &str,
) -> String {
    let sent: api_types::SendAgentChatMessageResponse = common::json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/agent-chats/{chat_id}/messages"),
        json!({"content": text}),
        StatusCode::CREATED,
    )
    .await;
    sent.turn_job.unwrap().id
}

pub(super) fn assert_preview_state_is_data(
    preview: &services_agents::agent_chat_turn_worker::AgentChatPromptPreview,
) {
    assert_eq!(preview.input_parts.len(), 2);
    let card = &preview.input_parts[1];
    assert!(card.starts_with("## SERVER-PROVIDED STATE CARD"));
    assert!(!card.contains("watermark"));
    assert!(!card.contains("Context-manifest references"));
    assert!(!card
        .as_bytes()
        .windows(64)
        .any(|b| b.iter().all(u8::is_ascii_hexdigit)));
    assert!(!preview
        .system_prompt
        .as_deref()
        .unwrap()
        .contains("Task summary:"));
}

pub(super) async fn record_passed_check(
    app: &Router,
    harness: &common::Harness,
    token: &str,
    fixture: &GenesisFixture,
) {
    let db = harness.state.db.clone();
    let project_id = fixture.project_id.clone();
    let milestone_id = fixture.milestone_id.clone();
    let check = sqlx::query(
        "SELECT id, definition_revision_id, version FROM project_milestone_check
         WHERE project_id = ? AND milestone_id = ? ORDER BY id LIMIT 1",
    )
    .bind(&project_id)
    .bind(&milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("default acceptance check");
    let check_id: String = check.get("id");
    let definition_revision_id: String = check.get("definition_revision_id");
    let check_version: i64 = check.get("version");
    let result_body = json!({
        "mutation": {
            "expected_version": check_version,
            "idempotency_key": "v076-check-result",
            "authorization": user_authorization(
                "project.milestone.check.record",
                "v076-check-event"
            )
        },
        "check_id": check_id,
        "definition_revision_id": definition_revision_id,
        "status": "pass",
        "result": "passed",
        "input_digest": "v076-check-input",
        "governing_revision_ids": [fixture.charter_revision_id]
    });
    let result = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        result_body.clone(),
        &[StatusCode::OK],
    )
    .await;
    assert_eq!(result["status"], json!("pass"));
    assert_eq!(result["principal"]["kind"], json!("user"));
    ensure_required_evidence(app, token, fixture).await;

    for (label, altered) in user_authorization_replay_variants(&result_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
            ),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("manual check definition revision", {
            let mut value = result_body.clone();
            value["definition_revision_id"] = json!("v076-different-check-revision");
            value
        }),
        ("manual check input digest", {
            let mut value = result_body.clone();
            value["input_digest"] = json!("v076-different-check-input");
            value
        }),
        ("manual check expected version", {
            let mut value = result_body.clone();
            value["mutation"]["expected_version"] = json!(check_version + 1);
            value
        }),
        ("manual check governing Charter", {
            let mut value = result_body.clone();
            value["governing_revision_ids"][0] = json!("v076-different-charter-revision");
            value
        }),
        ("manual check request target", {
            let mut value = result_body.clone();
            value["check_id"] = json!("v076-different-check-target");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!(
                "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
            ),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let result_path_target_conflict = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/v076-different-path-check/result"
        ),
        token,
        result_body.clone(),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(
        result_path_target_conflict["code"],
        json!("idempotency_conflict")
    );

    // Manual check attestation is explicitly user-only: a Project Agent's
    // principal-bound authorization is rejected before any check state is
    // touched.
    let agent_denial = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        json!({
            "mutation": {
                "expected_version": check_version + 1,
                "idempotency_key": "v076-agent-check-result",
                "authorization": {
                    "principal": {"kind": "agent", "id": fixture.project_identity_id},
                    "authorization_basis": "project_agent",
                    "action": "project.milestone.check.record",
                    "event_id": "v076-agent-check-event",
                    "occurred_at": Utc::now().to_rfc3339()
                }
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "agent must not attest",
            "input_digest": "v076-agent-check-input",
            "governing_revision_ids": [fixture.charter_revision_id]
        }),
        &[StatusCode::FORBIDDEN],
    )
    .await;
    assert_eq!(agent_denial["code"], json!("authorization.invalid"));

    // The same user key cannot be reused for a different result, even after
    // the check version has advanced from the original attestation.
    let altered = request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/checks/{check_id}/result"
        ),
        token,
        json!({
            "mutation": {
                "expected_version": check_version + 1,
                "idempotency_key": "v076-check-result",
                "authorization": user_authorization(
                    "project.milestone.check.record",
                    "v076-check-event"
                )
            },
            "check_id": check_id,
            "definition_revision_id": definition_revision_id,
            "status": "pass",
            "result": "altered result",
            "input_digest": "v076-altered-check-input",
            "governing_revision_ids": [fixture.charter_revision_id]
        }),
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(altered["code"], json!("idempotency_conflict"));
}

pub(super) async fn ensure_required_evidence(app: &Router, token: &str, fixture: &GenesisFixture) {
    let evidence = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    if evidence["items"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["availability"] == json!("available")
                && item["acceptance_check_ids"].as_array().is_some_and(|ids| {
                    ids.iter()
                        .any(|id| id == &json!(fixture.milestone_acceptance_check_id))
                })
        })
    }) {
        return;
    }

    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{}", fixture.project_id),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"]
        .as_i64()
        .expect("Project version for required evidence");
    let mut proof = b"\x89PNG\r\n\x1a\nrequired evidence ".to_vec();
    proof.extend_from_slice(fixture.milestone_acceptance_check_id.as_bytes());
    let asset = upload_project_media(
        app,
        token,
        &fixture.project_id,
        project_version,
        "v076-required-evidence-upload",
        &user_authorization(
            "project.media.upload",
            "v076-required-evidence-upload-event",
        ),
        &proof,
        StatusCode::CREATED,
    )
    .await;
    let asset_id = required_string(&asset, &["id"]);
    let checksum = required_string(&asset, &["checksum"]);
    let milestone = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{}/milestones/{}",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::POST,
        &format!(
            "/api/v1/projects/{}/milestones/{}/evidence",
            fixture.project_id, fixture.milestone_id
        ),
        token,
        json!({
            "mutation": {
                "expected_version": milestone["version"],
                "idempotency_key": "v076-required-evidence-attach",
                "authorization": user_authorization(
                    "project.evidence.attach",
                    "v076-required-evidence-attach-event"
                )
            },
            "milestone_id": fixture.milestone_id,
            "asset_id": asset_id,
            "acceptance_check_ids": [fixture.milestone_acceptance_check_id],
            "caption": "Required acceptance evidence fixture.",
            "kind": "screenshot",
            "checksum": checksum
        }),
        &[StatusCode::OK],
    )
    .await;
}

pub(super) async fn create_genesis_project(
    app: &Router,
    token: &str,
    prefix: &str,
) -> GenesisFixture {
    let main = connect_agent(
        app,
        token,
        &format!("{prefix}-main"),
        &["read_account", "read_project", "handoff"],
    )
    .await;
    let project_agent = connect_agent(
        app,
        token,
        &format!("{prefix}-project"),
        &[
            "read_project",
            "handoff",
            "propose_project",
            "propose_task",
            "propose_decision",
            "read_task",
        ],
    )
    .await;
    let main_identity = required_string(&main, &["agent", "id"]);
    let main_profile = required_string(&main, &["profile", "id"]);
    let project_identity = required_string(&project_agent, &["agent", "id"]);
    let project_profile = required_string(&project_agent, &["profile", "id"]);
    let binding = request_json(
        app,
        Method::PUT,
        "/api/v1/account/main-agent",
        token,
        json!({
            "identity_id": main_identity,
            "profile_id": main_profile,
            "expected_version": 0,
            "autonomy_policy": {}
        }),
        &[StatusCode::OK, StatusCode::CREATED],
    )
    .await;
    let main_chat_id = required_string(&binding, &["chat_id"]);
    let started = request_json(
        app,
        Method::POST,
        "/api/v1/account/main-agent/product-genesis",
        token,
        json!({
            "idempotency_key": format!("{prefix}-genesis-start"),
            "maturity": "mvp",
            "initial_idea": "A V076 bounded project",
            "preferred_project_agent_identity_id": project_identity
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let session_id = required_string(&started, &["session", "id"]);
    let content = charter_content(
        &format!("{prefix} Project"),
        "the approved V076 outcome is observable",
    );
    let rendered = services::render_and_digest_charter(&content);
    let charter_id = format!("{prefix}-charter");
    let saved = request_json(
        app,
        Method::POST,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions"),
        token,
        json!({
            "mutation": {
                "expected_version": 1,
                "idempotency_key": format!("{prefix}-charter-save"),
                "authorization": user_authorization("project_charter.revision.save", format!("{prefix}-save"))
            },
            "charter_id": charter_id,
            "project_mode": "compact",
            "maturity": "mvp",
            "content": content.clone(),
            "rendered_view": rendered.rendered_view.clone(),
            "render_version": rendered.render_version,
            "provenance": user_provenance("V076 approved Charter")
        }),
        &[StatusCode::CREATED],
    )
    .await;
    let revision_id = required_string(&saved, &["id"]);
    let projection = request_json(
        app,
        Method::GET,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let charter_version = projection["charter"]["version"]
        .as_i64()
        .expect("charter version");
    // Read the selection the server is offering rather than pinning a
    // revision: a migration that publishes a new Project operating skill
    // otherwise turns every approval here into
    // `project_agent_selection_conflict`.
    let operating_skill_revision = required_string(
        &projection,
        &["selected_project_agent", "operating_skill_revision"],
    );
    let policy_digest = project_policy_digest(&project_agent["profile"]["tool_policy"]);
    let approval_body = json!({
        "mutation": {
            "expected_version": charter_version,
            "expected_digest": rendered.content_digest,
            "idempotency_key": format!("{prefix}-charter-approve"),
            "authorization": user_authorization("product_genesis.charter_approval", format!("{prefix}-approve"))
        },
        "charter_id": charter_id,
        "revision_id": revision_id,
        "content_digest": rendered.content_digest,
        "render_digest": rendered.render_digest,
        "expected_charter_version": charter_version,
        "approved_project_name": format!("{prefix} Project"),
        "approved_project_slug": format!("{prefix}-project"),
        "project_mode": "compact",
        "selected_project_agent_identity_id": project_identity,
        "selected_project_agent_profile_revision_id": project_profile,
        "selected_project_agent_operating_skill_revision": operating_skill_revision,
        "selected_project_agent_policy_digest": policy_digest
    });
    let approval = request_json(
        app,
        Method::POST,
        &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
        token,
        approval_body.clone(),
        &[StatusCode::CREATED],
    )
    .await;
    // A Charter approval receipt is immutable. Reusing the same key with any
    // changed authority field or exact review target must be a conflict,
    // rather than a second approval or a successful replay.
    for (label, altered) in user_authorization_replay_variants(&approval_body, false) {
        let conflict = request_json(
            app,
            Method::POST,
            &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    for (label, altered) in [
        ("charter approval revision", {
            let mut value = approval_body.clone();
            value["revision_id"] = json!("v076-different-charter-revision");
            value
        }),
        ("charter approval content digest", {
            let mut value = approval_body.clone();
            value["content_digest"] = json!("v076-different-charter-digest");
            value
        }),
    ] {
        let conflict = request_json(
            app,
            Method::POST,
            &format!("/api/v1/account/main-agent/product-genesis/{session_id}/charter/revisions/{revision_id}/approve"),
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let approval_id = required_string(&approval, &["id"]);
    let create_body = json!({
        "approval_id": approval_id,
        "idempotency_key": format!("{prefix}-project-create"),
        "authorization": user_authorization("product_genesis.create_project_from_approval", format!("{prefix}-create"))
    });
    let created = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        token,
        create_body.clone(),
        &[StatusCode::CREATED],
    )
    .await;

    // Project creation consumes the exact approval receipt atomically. The
    // create key is independently replay-bound to its complete user receipt.
    // Once that key is committed, changing the approval target is an altered
    // replay and must return the shared idempotency conflict without looking
    // up or exposing the alternate target.
    for (label, altered) in user_authorization_replay_variants(&create_body, true) {
        let conflict = request_json(
            app,
            Method::POST,
            "/api/v1/projects",
            token,
            altered,
            &[StatusCode::CONFLICT],
        )
        .await;
        assert_eq!(conflict["code"], json!("idempotency_conflict"), "{label}");
    }
    let mut altered_create_target = create_body.clone();
    altered_create_target["approval_id"] = json!("v076-different-approval-receipt");
    let altered_target = request_json(
        app,
        Method::POST,
        "/api/v1/projects",
        token,
        altered_create_target,
        &[StatusCode::CONFLICT],
    )
    .await;
    assert_eq!(altered_target["code"], json!("idempotency_conflict"));
    let project_id = required_string(&created, &["project_id"]);
    let project_chat_id = required_string(&created, &["project_chat_id"]);
    let project = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let project_version = project["version"].as_i64().expect("project version");
    let milestone_id = required_string(&project, &["primary_milestone_id"]);
    let milestone = request_json(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}/milestones/{milestone_id}"),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_version = milestone["version"].as_i64().expect("milestone version");
    let milestone_definition_revision_id = required_string(&milestone, &["definition_revision_id"]);
    let milestone_definition = request_json(
        app,
        Method::GET,
        &format!(
            "/api/v1/projects/{project_id}/milestones/{milestone_id}/revisions/{milestone_definition_revision_id}"
        ),
        token,
        Value::Null,
        &[StatusCode::OK],
    )
    .await;
    let milestone_acceptance_check_id = required_string(
        &milestone_definition["content"]["acceptance_checks"][0],
        &["id"],
    );
    let milestone_acceptance_check_description = required_string(
        &milestone_definition["content"]["acceptance_checks"][0],
        &["description"],
    );
    GenesisFixture {
        project_id,
        project_version,
        project_identity_id: project_identity,
        project_profile_id: project_profile,
        project_chat_id,
        main_chat_id,
        genesis_session_id: session_id,
        create_response: created,
        create_request: create_body,
        approval_id,
        create_idempotency_key: format!("{prefix}-project-create"),
        charter_id,
        charter_revision_id: revision_id,
        charter_version,
        charter_content_digest: rendered.content_digest,
        charter_render_digest: rendered.render_digest,
        milestone_id,
        milestone_version,
        milestone_definition_revision_id,
        milestone_acceptance_check_id,
        milestone_acceptance_check_description,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn upload_project_media(
    app: &Router,
    token: &str,
    project_id: &str,
    expected_project_version: i64,
    idempotency_key: &str,
    authorization: &Value,
    bytes: &[u8],
    expected_status: StatusCode,
) -> Value {
    let boundary = format!("----v076-project-media-{idempotency_key}");
    let mutation = serde_json::to_vec(&json!({
        "mutation": {
            "expected_version": expected_project_version,
            "idempotency_key": idempotency_key,
            "authorization": authorization
        }
    }))
    .expect("media mutation serializes");
    let mut payload = Vec::new();
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"mutation\"\r\nContent-Type: application/json\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(&mutation);
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"v076-proof.png\"\r\nContent-Type: image/png\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let (status, response_body) = raw_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{project_id}/media"),
        token,
        Body::from(payload),
        Some(&format!("multipart/form-data; boundary={boundary}")),
    )
    .await;
    assert_eq!(
        status,
        expected_status,
        "unexpected media upload response: {}",
        String::from_utf8_lossy(&response_body)
    );
    serde_json::from_slice(&response_body).expect("media upload response JSON parses")
}

pub(super) async fn upload_task_media(
    app: &Router,
    token: &str,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    expected_status: StatusCode,
) -> Value {
    let boundary = format!("----v076-task-media-{task_id}");
    let mut payload = Vec::new();
    payload.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"author_name\"\r\n\r\n")
            .as_bytes(),
    );
    payload.extend_from_slice(b"V076 user\r\n");
    payload.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    payload.extend_from_slice(bytes);
    payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let (status, response_body) = raw_request(
        app,
        Method::POST,
        &format!("/api/v1/tasks/{task_id}/media"),
        token,
        Body::from(payload),
        Some(&format!("multipart/form-data; boundary={boundary}")),
    )
    .await;
    assert_eq!(
        status,
        expected_status,
        "unexpected Task media upload response: {}",
        String::from_utf8_lossy(&response_body)
    );
    serde_json::from_slice(&response_body).expect("Task media upload response JSON parses")
}

pub(super) async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Body,
    content_type: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("raw request builds"))
        .await
        .expect("raw request responds");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("raw response body reads")
        .to_vec();
    (status, bytes)
}

pub(super) async fn connect_agent(
    app: &Router,
    token: &str,
    name: &str,
    permissions: &[&str],
) -> Value {
    let entry = request_json(
        app,
        Method::POST,
        "/api/v1/providers",
        token,
        json!({
            "provider": "openai_compatible",
            "label": name,
            "credential": PROVIDER_SECRET,
            "base_url": "https://8.8.8.8"
        }),
        &[StatusCode::OK],
    )
    .await;
    request_json(
        app,
        Method::POST,
        "/api/v1/embedded-agents",
        token,
        json!({
            "name": name,
            "description": "V076 acceptance identity",
            "credential_id": entry["id"],
            "model": "v076-acceptance-model",
            "account_permission_ceiling": {"permissions": permissions},
            "tool_policy": {"allowed": permissions}
        }),
        &[StatusCode::OK],
    )
    .await
}

pub(super) fn charter_content(name: &str, acceptance: &str) -> ProjectCharterContent {
    serde_json::from_value(json!({
        "identity": {
            "working_name": name,
            "slug_proposal": "v076-project",
            "one_line_vision": "An auditable orchestration flow.",
            "maturity": "mvp"
        },
        "problem_and_people": {
            "problem_or_opportunity": "Approved intent must survive handoff.",
            "target_users": ["Forge users"],
            "beneficiaries": ["Project collaborators"]
        },
        "core_experience": {"primary_outcome": "A bounded Project starts from approved intent."},
        "scope": {
            "must_have_outcomes": ["Persist the exact approved Charter."],
            "explicit_non_goals": ["Cross-project mutation"]
        },
        "success": {
            "success_signals": ["The handoff is replay-safe."],
            "acceptance_statements": [acceptance]
        },
        "constraints_and_risks": {
            "product": ["Single-user local-first operation."],
            "technology": ["SQLite and the existing API."],
            "security_privacy_compliance": ["Explicit user approval is required."]
        },
        "knowledge_ledger": {"items": []}
    }))
    .expect("V076 Charter content parses")
}

pub(super) fn user_authorization(action: &str, event_id: impl Into<String>) -> Value {
    json!({
        "principal": {"kind": "user", "id": "test-user-id"},
        "authorization_basis": "explicit_user_authorization",
        "action": action,
        "event_id": event_id.into(),
        "occurred_at": Utc::now().to_rfc3339()
    })
}

/// Return replay requests that differ from `body` in exactly one authority
/// receipt field. `top_level_authorization` is true for the Project creation
/// envelope; all other orchestration mutations nest the receipt under
/// `mutation.authorization`.
pub(super) fn user_authorization_replay_variants(
    body: &Value,
    top_level_authorization: bool,
) -> Vec<(&'static str, Value)> {
    let mut variants = Vec::new();
    for (label, field, replacement) in [
        (
            "authorization action",
            "action",
            json!("altered.authority.action"),
        ),
        (
            "authorization occurred_at",
            "occurred_at",
            json!(Utc::now()
                .checked_add_signed(Duration::seconds(1))
                .expect("authorization timestamp remains representable")
                .to_rfc3339()),
        ),
        (
            "authorization basis",
            "authorization_basis",
            json!("altered_authorization_basis"),
        ),
        (
            "authorization event",
            "event_id",
            json!("altered-authority-event"),
        ),
    ] {
        let mut altered = body.clone();
        let authorization = if top_level_authorization {
            altered
                .get_mut("authorization")
                .expect("top-level authorization")
        } else {
            altered
                .get_mut("mutation")
                .and_then(Value::as_object_mut)
                .and_then(|mutation| mutation.get_mut("authorization"))
                .expect("mutation authorization")
        };
        authorization[field] = replacement;
        variants.push((label, altered));
    }
    let mut altered = body.clone();
    let authorization = if top_level_authorization {
        altered
            .get_mut("authorization")
            .expect("top-level authorization")
    } else {
        altered
            .get_mut("mutation")
            .and_then(Value::as_object_mut)
            .and_then(|mutation| mutation.get_mut("authorization"))
            .expect("mutation authorization")
    };
    authorization["principal"]["id"] = json!("different-principal");
    variants.push(("authorization principal", altered));
    variants
}

pub(super) fn user_provenance(summary: &str) -> Value {
    json!({
        "author": {"kind": "user", "id": "test-user-id"},
        "operating_skill_revision": "forge.project.orchestration/v1@15",
        "source_refs": [],
        "change_summary": summary
    })
}

/// The current Project operating-skill revision id, the id its same-key
/// successor would have, and that successor's revision number.
pub(super) async fn project_skill_revisions(harness: &common::Harness) -> (String, String, i64) {
    let (current_id, revision): (String, i64) = sqlx::query_as(
        "SELECT revision.id, revision.revision
         FROM operating_skill skill
         JOIN operating_skill_revision revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
         WHERE skill.skill_key = 'forge.project.orchestration/v1'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .expect("current Project operating-skill revision loads");
    let next = revision + 1;
    (
        current_id,
        format!("forge.project.orchestration/v1@{next}"),
        next,
    )
}

pub(super) fn project_policy_digest(policy: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"forge.project-agent-policy/v1\0");
    hasher.update(policy.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

pub(super) async fn request_json(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: Value,
    expected_statuses: &[StatusCode],
) -> Value {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(
            serde_json::to_vec(&body).expect("request JSON serializes"),
        ))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("router responds");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body reads");
    assert!(
        expected_statuses.contains(&status),
        "unexpected {status} from {uri}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("response JSON parses")
}

pub(super) fn required_string(value: &Value, path: &[&str]) -> String {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .unwrap_or_else(|| panic!("missing JSON field {}", path.join(".")));
    }
    current
        .as_str()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("JSON field {} is not a non-empty string", path.join(".")))
        .to_owned()
}

pub(super) fn git_default_branch(path: &std::path::Path) -> String {
    let output = std::process::Command::new("git")
        .args(["symbolic-ref", "--short", "HEAD"])
        .current_dir(path)
        .output()
        .expect("git default branch reads");
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}
