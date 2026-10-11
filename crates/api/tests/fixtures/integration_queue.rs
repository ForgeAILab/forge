//! Seeding and reading helpers shared by the merge-queue read tests.
use crate::common;
use api_types::{IntegrationQueuesResponse, ProjectResponse};
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    response::Response,
};
use db::IntegrationQueueRepo;
use serde_json::{json, Value};
use tower::ServiceExt;

pub const T0: &str = "2026-10-01T00:00:00+00:00";

pub fn jwt_for(user_id: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &json!({"sub": user_id, "email": format!("{user_id}@example.com"), "is_admin": false, "iat": now, "exp": now + 900}),
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .unwrap()
}

pub async fn get_as(h: &common::Harness, uri: &str, jwt: &str, etag: Option<&str>) -> Response {
    let mut request = Request::builder()
        .uri(uri)
        .header("authorization", format!("Bearer {jwt}"));
    if let Some(etag) = etag {
        request = request.header("if-none-match", etag);
    }
    h.app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}
pub async fn get_json(h: &common::Harness, uri: &str) -> Value {
    let response = get_as(h, uri, &common::test_jwt(), None).await;
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}
pub async fn queues(h: &common::Harness, repo: &str, query: &str) -> IntegrationQueuesResponse {
    serde_json::from_value(
        get_json(
            h,
            &format!("/api/v1/repos/{repo}/integration-queues{query}"),
        )
        .await,
    )
    .unwrap()
}

/// A Project the test user owns, with one repository row.
pub async fn project_and_repo(h: &common::Harness, name: &str) -> (String, String) {
    let project: ProjectResponse = common::json_request(
        &h.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": name }),
        StatusCode::OK,
    )
    .await;
    let repo = db::new_uuid_v4();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES(?,?,?,'main',?,?)")
        .bind(&repo)
        .bind(&project.id)
        .bind(name)
        .bind(T0)
        .bind(T0)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    (project.id, repo)
}
pub async fn task(h: &common::Harness, project: &str, title: &str) -> String {
    let id = db::new_uuid_v4();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, ?, 'merging', ?, ?)")
        .bind(&id)
        .bind(project)
        .bind(title)
        .bind(T0)
        .bind(T0)
        .execute(h.state.db.pool())
        .await
        .unwrap();
    id
}
/// A queue for `branch`, opened whatever the repository's checkout says.
pub async fn open_queue(h: &common::Harness, repo: &str, branch: &str) -> String {
    let queue = h
        .state
        .db
        .create_or_get_integration_queue(repo, branch)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL WHERE id=?",
    )
    .bind(&queue.id)
    .execute(h.state.db.pool())
    .await
    .unwrap();
    queue.id
}
pub async fn admit(h: &common::Harness, queue: &str, project: &str, task: &str) -> String {
    h.state
        .db
        .admit_integration_attempt(db::IntegrationAttempt::new(
            Some(queue.to_owned()),
            task.to_owned(),
            project.to_owned(),
            format!("admission-{task}"),
            "merging".into(),
            0,
            1,
        ))
        .await
        .unwrap()
        .id
}
/// What the queue worker would have written, written directly: these tests
/// read the rows, they do not run the worker.
pub async fn set_attempt(h: &common::Harness, attempt: &str, assignments: &str) {
    sqlx::query(&format!(
        "UPDATE integration_attempt SET {assignments},revision=revision+1 WHERE id=?"
    ))
    .bind(attempt)
    .execute(h.state.db.pool())
    .await
    .unwrap();
}
pub async fn set_head(h: &common::Harness, queue: &str, attempt: Option<&str>) {
    sqlx::query("UPDATE integration_queue SET head_attempt_id=? WHERE id=?")
        .bind(attempt)
        .bind(queue)
        .execute(h.state.db.pool())
        .await
        .unwrap();
}
