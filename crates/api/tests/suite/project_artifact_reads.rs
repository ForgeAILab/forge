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
