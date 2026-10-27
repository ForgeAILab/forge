use crate::common;

use api_types::ProjectResponse;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

async fn assert_database_error(app: &axum::Router, path: &str, message: &str) {
    let body: Value =
        common::empty_request(app, Method::GET, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    assert!(!body["request_id"].as_str().unwrap().is_empty());
    assert_eq!(
        body,
        json!({
            "code": "internal_error", "message": message,
            "details": null, "request_id": body["request_id"],
        }),
        "{path}"
    );
}

async fn project(harness: &common::Harness) -> ProjectResponse {
    common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Read error project"}),
        StatusCode::OK,
    )
    .await
}

#[tokio::test]
async fn artifact_read_database_errors_keep_the_historical_response() {
    for (table, suffixes) in [
        (
            "project_document",
            vec!["documents", "documents?cursor=6e6f7700646f63"],
        ),
        (
            "project_decision_candidate",
            vec![
                "decisions/candidates",
                "decisions/candidates?cursor=6e6f770063616e646964617465",
            ],
        ),
        (
            "project_decision",
            vec![
                "decisions",
                "decisions?cursor=6e6f77006465636973696f6e",
                "decisions/decision",
            ],
        ),
        (
            "project_document_revision",
            vec![
                "documents/document/revisions",
                "documents/document/revisions?cursor=31007265766973696f6e",
            ],
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let harness = common::test_app(root.path(), "artifact-read-errors").await;
        harness.stop_step_worker();
        let project = project(&harness).await;
        if table == "project_document_revision" {
            db::ProjectOrchestrationRepo::create_project_document(
                &*harness.state.db,
                db::CreateProjectDocument {
                    id: "document".into(),
                    project_id: project.id.clone(),
                    kind: "research".into(),
                    title: "Document".into(),
                    approval_policy: "user_or_project_agent".into(),
                    created_at: "now".into(),
                    updated_at: "now".into(),
                },
            )
            .await
            .unwrap();
        }
        sqlx::query(&format!("DROP TABLE {table}"))
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        for suffix in suffixes {
            assert_database_error(
                &harness.app,
                &format!("/api/v1/projects/{}/{suffix}", project.id),
                "Project artifact mutation failed",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn project_charter_read_database_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "charter-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    sqlx::query("DROP TABLE project_charter")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(
        &harness.app,
        &format!("/api/v1/projects/{}/charter", project.id),
        "Project artifact mutation failed",
    )
    .await;
}

#[tokio::test]
async fn overview_read_database_errors_keep_the_overview_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "overview-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    sqlx::query("DROP TABLE project_document")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(
        &harness.app,
        &format!("/api/v1/projects/{}/overview", project.id),
        "Project Overview is temporarily unavailable",
    )
    .await;
}

#[tokio::test]
async fn scoped_memory_pointer_database_errors_keep_the_historical_response() {
    for table in [
        "project_document",
        "project_milestone",
        "project_agent_binding",
    ] {
        let root = tempfile::tempdir().unwrap();
        let harness = common::test_app(root.path(), "context-pointer-errors").await;
        harness.stop_step_worker();
        let project = project(&harness).await;
        let identity_id = db::new_uuid_v4();
        let scope_id = db::new_uuid_v4();
        let manifest_id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        db::AgentRepo::create_identity_with_profile(
            &*harness.state.db,
            db::CreateAgentIdentity {
                id: identity_id.clone(),
                name: "Context inspector".into(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("test-user-id".into()),
                visibility: "account".into(),
                account_permission_ceiling: "{}".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::CreateAgentProfile {
                id: db::new_uuid_v4(),
                identity_id: identity_id.clone(),
                backend_kind: "native".into(),
                executor_type: "embedded".into(),
                provider: Some("test".into()),
                model: Some("test".into()),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".into(),
                tool_policy_json: "{}".into(),
                config_json: "{}".into(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        db::AgentContextScopeRepo::create_context_scope(
            &*harness.state.db,
            db::CreateAgentContextScope {
                id: scope_id.clone(),
                identity_id: identity_id.clone(),
                scope_type: "project".into(),
                scope_id: project.id.clone(),
                project_id: Some(project.id.clone()),
                task_id: None,
                task_role: None,
                workspace_access: "deny".into(),
                workspace_path: None,
                authority_json: "{}".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        db::ScopedMemoryRepository::create_context_manifest(
            &*harness.state.db,
            db::CreateContextManifest {
                id: manifest_id.clone(),
                identity_id: identity_id.clone(),
                agent_session_id: None,
                context_scope_id: scope_id.clone(),
                scope_type: "project".into(),
                scope_id: project.id,
                policy_revision: "policy".into(),
                domain_revision: "domain".into(),
                lcm_binding_revision: None,
                runtime_manifest_id: None,
                runtime_manifest_fingerprint: None,
                combined_fingerprint: "combined".into(),
                request_fingerprint: "request".into(),
                created_at: now,
            },
        )
        .await
        .unwrap();
        sqlx::query(&format!("DROP TABLE {table}"))
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        assert_database_error(&harness.app, &format!("/api/v1/context-manifests/{manifest_id}?identity_id={identity_id}&context_scope_id={scope_id}"), "Project artifact mutation failed").await;
    }
}

#[tokio::test]
async fn overview_skips_unknown_document_kind_before_decoding_later_fields() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "overview-decode-order").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    sqlx::query("DROP TABLE project_document")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query(
        "CREATE VIEW project_document AS
        SELECT id || '-document' AS id, id AS project_id, 'unknown' AS kind,
               X'80' AS lifecycle, X'80' AS current_draft_revision_id,
               NULL AS current_approved_revision_id, 'now' AS updated_at FROM project",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let overview: Value = common::empty_request(
        &harness.app,
        Method::GET,
        &format!("/api/v1/projects/{}/overview", project.id),
        StatusCode::OK,
    )
    .await;
    assert_eq!(overview["document_freshness"], json!([]));
    assert_eq!(overview["projection_state"], "stale");
}

#[tokio::test]
async fn operating_skill_lookup_database_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "operating-skill-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    let agent = common::connect_embedded_agent(
        &harness.app,
        &common::test_jwt(),
        "Charter inspector",
        "read-errors",
        "fixture-secret",
        json!({"permissions": ["read_project"]}),
        json!({"allowed": ["read_project"]}),
    )
    .await;
    let changed = sqlx::query(
        "UPDATE project_agent_binding SET state='active', identity_id=?, profile_id=? WHERE project_id=?",
    )
    .bind(&agent.agent.id)
    .bind(&agent.profile.id)
    .bind(&project.id)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);
    let path = format!("/api/v1/projects/{}/charter", project.id);
    let healthy: Value =
        common::empty_request(&harness.app, Method::GET, &path, StatusCode::OK).await;
    assert_eq!(
        healthy["selected_project_agent"]["identity_id"],
        agent.agent.id
    );
    sqlx::query("DROP TABLE operating_skill_revision")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(&harness.app, &path, "Project artifact mutation failed").await;
}

async fn insert_charter(harness: &common::Harness, project_id: &str) {
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, created_at, updated_at)
         VALUES ('charter', 'test-user-id', ?, 'compact', 'mvp', 'now', 'now')",
    )
    .bind(project_id)
    .execute(harness.state.db.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn charter_approval_lookup_database_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "charter-approval-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    insert_charter(&harness, &project.id).await;
    sqlx::query("DROP TABLE project_charter_approval")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    assert_database_error(
        &harness.app,
        &format!("/api/v1/projects/{}/charter", project.id),
        "Project artifact mutation failed",
    )
    .await;
}

#[tokio::test]
async fn genesis_approval_replay_database_errors_keep_the_historical_response() {
    for failure in ["approval_key", "genesis_session_id"] {
        let root = tempfile::tempdir().unwrap();
        let harness = common::test_app(root.path(), "genesis-replay-read-errors").await;
        harness.stop_step_worker();
        if failure == "approval_key" {
            sqlx::query("DROP TABLE project_charter_approval")
                .execute(harness.state.db.pool())
                .await
                .unwrap();
        } else {
            let project = project(&harness).await;
            insert_charter(&harness, &project.id).await;
            sqlx::query(
                "INSERT INTO project_charter_revision
                 (id, charter_id, revision, schema_version, render_version, content_json,
                  rendered_view, author_type, content_digest, rendered_digest, created_at)
                 VALUES ('revision', 'charter', 1, 'v1', 'v1', '{}', 'Charter',
                         'user', 'content', 'render', 'now')",
            )
            .execute(harness.state.db.pool())
            .await
            .unwrap();
            let stored_key = format!(
                "forge-idem-v1:{}:{}:{}:replay",
                hex::encode("charter-approval"),
                hex::encode("account:test-user-id"),
                hex::encode("test-user-id"),
            );
            sqlx::query(
                "INSERT INTO project_charter_approval
                 (id, approval_type, charter_id, revision_id, content_digest, rendered_digest,
                  expected_charter_version, approving_principal_type, approving_principal_id,
                  authorization_basis, authorization_action, explicit_event,
                  authorization_occurred_at, source_action, idempotency_key, created_at, updated_at)
                 VALUES ('approval', 'project_creation', 'charter', 'revision', 'content',
                         'render', 1, 'user', 'test-user-id', 'explicit_user_authorization',
                         'product_genesis.charter_approval', 'event', 'now', 'approve', ?, 'now', 'now')",
            )
            .bind(stored_key)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
            // Retain the referenced Charter while exposing a malformed read projection.
            sqlx::query("ALTER TABLE project_charter RENAME TO stored_project_charter")
                .execute(harness.state.db.pool())
                .await
                .unwrap();
            sqlx::query(
                "CREATE VIEW project_charter AS
                 SELECT CAST('session' AS BLOB) AS genesis_session_id, *
                 FROM stored_project_charter",
            )
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        }
        let body: Value = common::json_request(
            &harness.app,
            Method::POST,
            "/api/v1/account/main-agent/product-genesis/session/charter/revisions/revision/approve",
            json!({
                "mutation": {
                    "expected_version": 1, "idempotency_key": "replay",
                    "authorization": {
                        "principal": {"kind": "user", "id": "test-user-id"},
                        "authorization_basis": "explicit_user_authorization",
                        "action": "product_genesis.charter_approval",
                        "event_id": "event", "occurred_at": db::now_rfc3339()
                    }
                },
                "charter_id": "charter", "revision_id": "revision",
                "content_digest": "content", "render_digest": "render",
                "expected_charter_version": 1, "approved_project_name": "Read error",
                "project_mode": "compact", "selected_project_agent_identity_id": "agent",
                "selected_project_agent_profile_revision_id": "profile",
                "selected_project_agent_operating_skill_revision": "skill",
                "selected_project_agent_policy_digest": "policy"
            }),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        assert!(!body["request_id"].as_str().unwrap().is_empty());
        assert_eq!(
            body,
            json!({
                "code": "internal_error", "message": "Project artifact mutation failed",
                "details": null, "request_id": body["request_id"],
            }),
            "{failure}"
        );
    }
}

#[tokio::test]
async fn decision_replacement_decode_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "decision-replacement-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    for (id, supersedes, at) in [
        ("decision", None, "2026-10-02"),
        ("replacement", Some("decision"), "2026-10-01"),
    ] {
        sqlx::query(
            "INSERT INTO project_decision
             (id, project_id, state, decision_class, question, selected_outcome, rationale,
              principal_type, principal_id, authority_basis, authorization_action,
              explicit_event, authorization_occurred_at, supersedes_decision_id, created_at)
             VALUES (?, ?, 'active', 'project_implementation', 'Question', 'Outcome', 'Reason',
                     'user', 'test-user-id', 'explicit_user_authorization', 'decide',
                     'event', 'now', ?, ?)",
        )
        .bind(id)
        .bind(&project.id)
        .bind(supersedes)
        .bind(at)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    }
    sqlx::query("ALTER TABLE project_decision RENAME TO stored_project_decision")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query(
        "CREATE VIEW project_decision AS
         SELECT CASE WHEN id='replacement' THEN CAST(state AS BLOB) ELSE state END AS state, *
         FROM stored_project_decision",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let selected = harness
        .state
        .db
        .project_decision_page(&project.id, None, 1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .into_decision()
        .unwrap();
    assert_eq!(
        selected.id, "decision",
        "the selected row decodes before its replacement"
    );
    for suffix in ["decisions?limit=1", "decisions/decision"] {
        assert_database_error(
            &harness.app,
            &format!("/api/v1/projects/{}/{suffix}", project.id),
            "Project artifact mutation failed",
        )
        .await;
    }
}

#[tokio::test]
async fn overview_later_database_errors_keep_the_overview_response() {
    for table in [
        "project_milestone",
        "project_milestone_check",
        "project_media_attachment",
        "project_decision_candidate",
        "project_decision",
        "project_release",
        "project_reconciliation_record",
    ] {
        let root = tempfile::tempdir().unwrap();
        let harness = common::test_app(root.path(), "overview-later-read-errors").await;
        harness.stop_step_worker();
        let project = project(&harness).await;
        sqlx::query(&format!("DROP TABLE {table}"))
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        assert_database_error(
            &harness.app,
            &format!("/api/v1/projects/{}/overview", project.id),
            "Project Overview is temporarily unavailable",
        )
        .await;
    }
}

#[tokio::test]
async fn document_count_database_errors_keep_the_historical_response() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "document-count-read-errors").await;
    harness.stop_step_worker();
    let project = project(&harness).await;
    for (id, title, at) in [
        ("first", "First", "2026-10-03"),
        ("lookahead", "Lookahead", "2026-10-02"),
        ("broken", "{", "2026-10-01"),
    ] {
        db::ProjectOrchestrationRepo::create_project_document(
            &*harness.state.db,
            db::CreateProjectDocument {
                id: id.into(),
                project_id: project.id.clone(),
                kind: "research".into(),
                title: title.into(),
                approval_policy: "user_or_project_agent".into(),
                created_at: at.into(),
                updated_at: at.into(),
            },
        )
        .await
        .unwrap();
    }
    sqlx::query("ALTER TABLE project_document RENAME TO stored_project_document")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query(
        "CREATE INDEX count_fixture_order ON stored_project_document(updated_at DESC, id DESC)",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    // The indexed page and its lookahead stop before the malformed old row;
    // the count must evaluate that row's broken JSON projection.
    sqlx::query(
        "CREATE VIEW project_document AS
         SELECT id, CASE WHEN id='broken' THEN json_extract(title, '$.project_id')
                ELSE project_id END AS project_id, kind, title, lifecycle, approval_policy,
                current_draft_revision_id, current_approved_revision_id, version, created_at,
                updated_at FROM stored_project_document",
    )
    .execute(harness.state.db.pool())
    .await
    .unwrap();
    let page = harness
        .state
        .db
        .project_document_id_page(&project.id, None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].id().unwrap(), "first");
    assert_eq!(page[1].id().unwrap(), "lookahead");
    let selected = db::ProjectOrchestrationRepo::get_project_document(&*harness.state.db, "first")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.project_id, project.id);
    assert!(harness
        .state
        .db
        .project_document_count_before(&project.id, &selected.updated_at, &selected.id)
        .await
        .is_err());
    assert_database_error(
        &harness.app,
        &format!("/api/v1/projects/{}/documents?limit=1", project.id),
        "Project artifact mutation failed",
    )
    .await;
}
