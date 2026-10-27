use crate::common;

use api_types::ProjectResponse;
use axum::http::{Method, StatusCode};
use serde_json::{json, Value};

#[tokio::test]
async fn database_errors_preserve_project_artifact_body() {
    let root = tempfile::tempdir().unwrap();
    let harness = common::test_app(root.path(), "project-media-errors").await;
    harness.stop_step_worker();
    let project: ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Media database error"}),
        StatusCode::OK,
    )
    .await;
    sqlx::query("DROP TABLE project_media_attachment")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    sqlx::query("DROP TABLE media_asset")
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let cursor = hex::encode("2026-10-01T00:00:00Z\0asset");
    for suffix in [
        "media".to_owned(),
        format!("media?cursor={cursor}"),
        "milestones/milestone/evidence".to_owned(),
        format!("milestones/milestone/evidence?cursor={cursor}"),
        "milestones/milestone/evidence/evidence".to_owned(),
    ] {
        let body: Value = common::empty_request(
            &harness.app,
            Method::GET,
            &format!("/api/v1/projects/{}/{suffix}", project.id),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        assert!(!body["request_id"].as_str().unwrap().is_empty());
        assert_eq!(
            body,
            json!({
                "code": "internal_error",
                "message": "Project artifact mutation failed",
                "details": null,
                "request_id": body["request_id"],
            }),
            "{suffix}"
        );
    }
}
