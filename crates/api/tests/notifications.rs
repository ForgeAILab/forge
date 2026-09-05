#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{sync::Arc, time::Duration};

use api::{build_router, AppState};
use api_types::{
    NotificationResponse, PaginatedResponse, ProjectResponse, RepoResponse, UnreadCountResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{new_uuid_v4, now_rfc3339, CreateTask, NotificationListQuery, NotificationRepo, TaskRepo};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tower::ServiceExt;

#[tokio::test]
async fn event_bus_creates_notification_and_api_manages_inbox() {
    let harness = test_app().await;
    let (project, repo) = create_project_and_repo(&harness.app).await;

    let now = now_rfc3339();
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &*harness.state.db,
        CreateTask {
            id: task_id.clone(),
            project_id: project.id.clone(),
            repo_id: Some(repo.id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Blocked task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");

    let mut rx = harness.event_bus.subscribe();
    harness.event_bus.publish(ForgeEvent {
        event_type: "task.blocked".to_owned(),
        entity_id: task_id.clone(),
        timestamp: event_timestamp(),
        context: EventContext::TaskBlocked {
            project_id: project.id.clone(),
            reason: "waiting for product input".to_owned(),
            kind: None,
            source: None,
            execution_id: None,
        },
    });

    let created = wait_for_notification_created(&mut rx).await;
    assert_eq!(created.event_type, "task.blocked");
    assert_eq!(created.project_id, project.id);
    assert_eq!(created.task_id, Some(task_id));

    let list = NotificationRepo::list(
        &*harness.state.db,
        NotificationListQuery {
            project_id: Some(project.id.clone()),
            read: Some(false),
            page: db::PageRequest {
                cursor: None,
                limit: 10,
                include_total: true,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Desc,
            },
        },
    )
    .await
    .expect("notification list");
    assert_eq!(list.items.len(), 1);

    let unread: UnreadCountResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/notifications/unread-count?project_id={}",
            project.id
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(unread.count, 1);

    let page: PaginatedResponse<NotificationResponse> = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/notifications?project_id={}&read=false&limit=20",
            project.id
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(page.items.len(), 1);

    let marked: NotificationResponse = empty_request(
        &harness.app,
        Method::PATCH,
        &format!("/api/v1/notifications/{}/read", page.items[0].id),
        StatusCode::OK,
    )
    .await;
    assert!(marked.read);

    let _: Value = empty_request(
        &harness.app,
        Method::POST,
        &format!(
            "/api/v1/notifications/mark-all-read?project_id={}",
            project.id
        ),
        StatusCode::NO_CONTENT,
    )
    .await;

    let unread_after: UnreadCountResponse = empty_request(
        &harness.app,
        Method::GET,
        &format!(
            "/api/v1/notifications/unread-count?project_id={}",
            project.id
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(unread_after.count, 0);

    let _: Value = empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/notifications/{}", marked.id),
        StatusCode::NO_CONTENT,
    )
    .await;
}

#[derive(Debug, Clone)]
struct NotificationCreatedEvent {
    notification_id: String,
    project_id: String,
    task_id: Option<String>,
    event_type: String,
    title: String,
}

async fn wait_for_notification_created(
    rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>,
) -> NotificationCreatedEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("notification.created event not received");
        }

        match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
            Ok(Ok(event)) => {
                if let EventContext::NotificationCreated {
                    notification_id,
                    project_id,
                    task_id,
                    event_type,
                    title,
                } = event.context
                {
                    return NotificationCreatedEvent {
                        notification_id,
                        project_id,
                        task_id,
                        event_type,
                        title,
                    };
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("event bus closed")
            }
            Err(_) => continue,
        }
    }
}

struct Harness {
    app: Router,
    state: Arc<AppState>,
    event_bus: Arc<EventBus>,
    _web_dist_dir: common::TestDir,
}

async fn test_app() -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(128));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
    ));

    let web_dist_dir = common::TestDir::new("forge-notifications-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    Harness {
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_and_repo(app: &Router) -> (ProjectResponse, RepoResponse) {
    let project: ProjectResponse = json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Notifications" }),
        StatusCode::OK,
    )
    .await;
    let repo: RepoResponse = json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "remote_url": "https://example.com/repo.git",
            "local_path": null,
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;
    (project, repo)
}

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let token = test_jwt();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON")
}

async fn empty_request<T>(app: &Router, method: Method, uri: &str, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let token = test_jwt();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("router response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert_eq!(
        status,
        expected_status,
        "body: {}",
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        serde_json::from_value(json!({})).expect("empty json")
    } else {
        serde_json::from_slice(&bytes).expect("parse JSON")
    }
}
