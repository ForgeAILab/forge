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

#[tokio::test]
async fn per_row_database_errors_preserve_project_artifact_body() {
    use sha2::{Digest, Sha256};

    for failure in ["asset_id", "task_attachment_ids", "evidence_row"] {
        let root = tempfile::tempdir().unwrap();
        let harness = common::test_app(root.path(), "project-media-row-errors").await;
        harness.stop_step_worker();
        let project: ProjectResponse = common::json_request(
            &harness.app,
            Method::POST,
            "/api/v1/projects",
            json!({"name": "Media row error"}),
            StatusCode::OK,
        )
        .await;
        let bytes = b"media row fixture\n";
        let checksum = hex::encode(Sha256::digest(bytes));
        if failure == "asset_id" {
            // SQLite permits a BLOB in a TEXT column; the page succeeds and ID decoding fails.
            sqlx::query(
                "INSERT INTO media_asset
                (id, project_id, display_filename, content_type, byte_size, storage_key,
                 checksum, created_at, updated_at)
                VALUES (?, ?, 'fixture.txt', 'text/plain', ?, 'row-fixture.txt', ?, 'now', 'now')",
            )
            .bind(b"asset".as_slice())
            .bind(&project.id)
            .bind(bytes.len() as i64)
            .bind(&checksum)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
        } else {
            sqlx::query("INSERT INTO media_asset
                (id, project_id, display_filename, content_type, byte_size, storage_key,
                 checksum, created_at, updated_at)
                VALUES ('asset', ?, 'fixture.txt', 'text/plain', ?, 'row-fixture.txt', ?, 'now', 'now')")
                .bind(&project.id).bind(bytes.len() as i64).bind(&checksum)
                .execute(harness.state.db.pool()).await.unwrap();
        }
        let suffix = match failure {
            "asset_id" => "media",
            "task_attachment_ids" => {
                let media_dir = harness.state.effective_config.forge.data_dir.join("media");
                tokio::fs::create_dir_all(&media_dir).await.unwrap();
                tokio::fs::write(media_dir.join("row-fixture.txt"), bytes)
                    .await
                    .unwrap();
                sqlx::query("DROP TABLE project_media_attachment")
                    .execute(harness.state.db.pool())
                    .await
                    .unwrap();
                "media"
            }
            "evidence_row" => {
                sqlx::query("DROP TABLE project_media_attachment")
                    .execute(harness.state.db.pool())
                    .await
                    .unwrap();
                // Supply one selected row with an undecodable ID, rather than failing the SELECT.
                sqlx::query(
                    "CREATE VIEW project_media_attachment AS
                    SELECT CAST(id AS BLOB) AS id, project_id, id AS asset_id,
                           'milestone' AS milestone_id, 'evidence' AS attachment_kind,
                           created_at, deleted_at FROM media_asset",
                )
                .execute(harness.state.db.pool())
                .await
                .unwrap();
                "milestones/milestone/evidence"
            }
            _ => unreachable!(),
        };
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
                "code": "internal_error", "message": "Project artifact mutation failed",
                "details": null, "request_id": body["request_id"],
            }),
            "{failure}"
        );
    }
}
