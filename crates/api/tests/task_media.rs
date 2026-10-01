#![allow(dead_code, clippy::assertions_on_constants)]

mod common;

use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{
    AuthResponse, AuthorType, PaginatedResponse, ProjectMemberResponse, ProjectResponse,
    TaskMediaResponse, TaskResponse, UserResponse,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, HeaderMap, HeaderName, Method, Request, StatusCode},
    Router,
};
use events::{EventBus, EventContext, ForgeEvent};
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::broadcast;
use tower::ServiceExt;

const DEFAULT_MEDIA_LIMIT: u64 = 1024 * 1024;
const SAMPLE_MEDIA_LIMIT: u64 = 100 * 1024 * 1024;

#[tokio::test]
async fn upload_image_evidence() {
    let fixture = MediaFixture::new("forge-media-image");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Image evidence").await;

    let bytes = b"\x89PNG\r\n\x1a\nimage evidence";
    let media = upload_media_ok(&harness.app, &task.id, "evidence.png", "image/png", bytes).await;

    assert_eq!(media.task_id, task.id);
    assert_eq!(media.filename, "evidence.png");
    assert_eq!(media.content_type, "image/png");
    assert_eq!(media.byte_size, bytes.len() as i64);
    assert_eq!(media.url, format!("/api/v1/media/{}", media.id));
    assert_eq!(media.author_type, AuthorType::User);
    assert_eq!(media.author_id.as_deref(), Some("test-user-id"));
    assert_eq!(media.author_name, "Tester");
    assert!(stored_path(
        fixture.data_dir(),
        &media_row(&harness, &media.id, false).await
    )
    .exists());
}

#[tokio::test]
async fn upload_video_evidence() {
    let fixture = MediaFixture::new("forge-media-video");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Video evidence").await;

    let bytes = b"\0\0\0\x18ftypmp42video evidence";
    let media = upload_media_ok(&harness.app, &task.id, "clip.mp4", "video/mp4", bytes).await;

    assert_eq!(media.task_id, task.id);
    assert_eq!(media.filename, "clip.mp4");
    assert_eq!(media.content_type, "video/mp4");
    assert_eq!(media.byte_size, bytes.len() as i64);
    assert_eq!(media.url, format!("/api/v1/media/{}", media.id));
    assert!(stored_path(
        fixture.data_dir(),
        &media_row(&harness, &media.id, false).await
    )
    .exists());
}

#[tokio::test]
async fn e2e_uploads_sample_video_and_image_assets() {
    let fixture = MediaFixture::new("forge-media-sample-assets");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        SAMPLE_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Sample asset evidence").await;
    let image_bytes = read_asset("logo.png");
    let video_bytes = read_asset("demo.mp4");

    let image = upload_media_ok(
        &harness.app,
        &task.id,
        "logo.png",
        "image/png",
        &image_bytes,
    )
    .await;
    let video = upload_media_ok(
        &harness.app,
        &task.id,
        "demo.mp4",
        "video/mp4",
        &video_bytes,
    )
    .await;

    assert_eq!(image.byte_size, image_bytes.len() as i64);
    assert_eq!(video.byte_size, video_bytes.len() as i64);
    assert!(stored_path(
        fixture.data_dir(),
        &media_row(&harness, &image.id, false).await
    )
    .exists());
    assert!(stored_path(
        fixture.data_dir(),
        &media_row(&harness, &video.id, false).await
    )
    .exists());

    let listed = list_task_media(&harness.app, &task.id).await;
    assert_eq!(listed.items.len(), 2);
    assert!(listed.items.iter().any(|item| item.id == image.id
        && item.filename == "logo.png"
        && item.content_type == "image/png"));
    assert!(listed.items.iter().any(|item| item.id == video.id
        && item.filename == "demo.mp4"
        && item.content_type == "video/mp4"));

    let (status, headers, body) = get_media_body(&harness.app, &image.url).await;
    assert_eq!(status, StatusCode::OK);
    assert_header_eq(&headers, header::CONTENT_TYPE, "image/png");
    assert!(headers.get(header::CONTENT_DISPOSITION).is_none());
    assert_eq!(body.len(), image_bytes.len());
    assert!(
        body == image_bytes,
        "downloaded image bytes differ from fixture"
    );

    let (status, headers, body) = get_media_body(&harness.app, &video.url).await;
    assert_eq!(status, StatusCode::OK);
    assert_header_eq(&headers, header::CONTENT_TYPE, "video/mp4");
    assert!(headers.get(header::CONTENT_DISPOSITION).is_none());
    assert_eq!(body.len(), video_bytes.len());
    assert!(
        body == video_bytes,
        "downloaded video bytes differ from fixture"
    );
}

#[tokio::test]
async fn reject_unsupported_media_type() {
    let fixture = MediaFixture::new("forge-media-unsupported");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Unsupported media").await;

    let response = raw_upload_media(
        &harness.app,
        &task.id,
        "payload.bin",
        "application/x-msdownload",
        b"not allowed",
    )
    .await;
    assert_status(response, StatusCode::BAD_REQUEST).await;

    assert_no_media_created(&harness, fixture.data_dir(), &task.id).await;
}

#[tokio::test]
async fn reject_svg_media_type() {
    let fixture = MediaFixture::new("forge-media-svg");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "SVG media").await;

    let response = raw_upload_media(
        &harness.app,
        &task.id,
        "payload.svg",
        "image/svg+xml",
        b"<svg><script>alert(1)</script></svg>",
    )
    .await;
    assert_status(response, StatusCode::BAD_REQUEST).await;

    assert_no_media_created(&harness, fixture.data_dir(), &task.id).await;
}

#[tokio::test]
async fn reject_oversized_upload() {
    let fixture = MediaFixture::new("forge-media-oversized");
    let harness = test_app(fixture.workspace_root(), fixture.data_dir(), 4, None).await;
    let task = create_task(&harness.app, "Oversized media").await;

    let response = raw_upload_media(
        &harness.app,
        &task.id,
        "too-large.png",
        "image/png",
        b"12345",
    )
    .await;
    assert_status(response, StatusCode::BAD_REQUEST).await;

    assert_no_media_created(&harness, fixture.data_dir(), &task.id).await;
}

#[tokio::test]
async fn reject_oversized_author_name_field() {
    let fixture = MediaFixture::new("forge-media-author-limit");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Oversized author name").await;

    let author_name = vec![b'a'; 257];
    let response = raw_upload_media_with_author(
        &harness.app,
        &task.id,
        "evidence.png",
        "image/png",
        b"png",
        &author_name,
    )
    .await;
    assert_status(response, StatusCode::BAD_REQUEST).await;

    assert_no_media_created(&harness, fixture.data_dir(), &task.id).await;
}

#[tokio::test]
async fn normalize_unsafe_filename() {
    let fixture = MediaFixture::new("forge-media-filename");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Unsafe filename").await;

    let media = upload_media_ok(&harness.app, &task.id, "../evil.png", "image/png", b"png").await;
    let row = media_row(&harness, &media.id, false).await;
    let path = stored_path(fixture.data_dir(), &row);

    assert_eq!(media.filename, "..evil.png");
    assert_eq!(row.display_filename, "..evil.png");
    assert!(storage_key_is_safe(&row.storage_key));
    assert!(path.exists());
    assert!(path.starts_with(fixture.data_dir().join("media")));
}

#[tokio::test]
async fn avoid_storage_key_collision() {
    let fixture = MediaFixture::new("forge-media-collision");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Storage key collision").await;

    let first = upload_media_ok(&harness.app, &task.id, "same.png", "image/png", b"first").await;
    let second = upload_media_ok(&harness.app, &task.id, "same.png", "image/png", b"second").await;
    let first_row = media_row(&harness, &first.id, false).await;
    let second_row = media_row(&harness, &second.id, false).await;

    assert_eq!(first.filename, "same.png");
    assert_eq!(second.filename, "same.png");
    assert_ne!(first.id, second.id);
    assert_ne!(first_row.storage_key, second_row.storage_key);
    assert_eq!(
        get_media_body(&harness.app, &first.url).await.2,
        b"first".to_vec()
    );
    assert_eq!(
        get_media_body(&harness.app, &second.url).await.2,
        b"second".to_vec()
    );
}

#[tokio::test]
async fn reject_misleading_extension_mismatch() {
    let fixture = MediaFixture::new("forge-media-extension");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Misleading extension").await;

    let response = raw_upload_media(&harness.app, &task.id, "evil.exe", "image/png", b"png").await;
    assert_status(response, StatusCode::BAD_REQUEST).await;

    assert_no_media_created(&harness, fixture.data_dir(), &task.id).await;
}

#[tokio::test]
async fn list_task_media_returns_only_that_tasks_media() {
    let fixture = MediaFixture::new("forge-media-list");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let first_task = create_task(&harness.app, "First task").await;
    let second_task = create_task(&harness.app, "Second task").await;

    let first_media = upload_media_ok(
        &harness.app,
        &first_task.id,
        "first.png",
        "image/png",
        b"first",
    )
    .await;
    let second_media = upload_media_ok(
        &harness.app,
        &second_task.id,
        "second.png",
        "image/png",
        b"second",
    )
    .await;

    let listed = list_task_media(&harness.app, &first_task.id).await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].id, first_media.id);
    assert_ne!(listed.items[0].id, second_media.id);
}

#[tokio::test]
async fn media_routes_require_project_membership() {
    let fixture = MediaFixture::new("forge-media-project-access");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Scoped media").await;
    let media = upload_media_ok(&harness.app, &task.id, "scoped.png", "image/png", b"scoped").await;
    let outsider = register_user(&harness.app, "media-outsider@example.com").await;

    let list_response = raw_empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/tasks/{}/media", task.id),
        &outsider.access_token,
    )
    .await;
    assert_status(list_response, StatusCode::NOT_FOUND).await;

    let get_response = raw_empty_request_with_bearer(
        &harness.app,
        Method::GET,
        &format!("/api/v1/media/{}", media.id),
        &outsider.access_token,
    )
    .await;
    assert_status(get_response, StatusCode::NOT_FOUND).await;

    let delete_response = raw_empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/media/{}", media.id),
        &outsider.access_token,
    )
    .await;
    assert_status(delete_response, StatusCode::NOT_FOUND).await;

    let upload_response = raw_upload_media_with_bearer(
        &harness.app,
        &task.id,
        "outsider.png",
        "image/png",
        b"outsider",
        &outsider.access_token,
    )
    .await;
    assert_status(upload_response, StatusCode::NOT_FOUND).await;

    assert!(media_row(&harness, &media.id, false)
        .await
        .deleted_at
        .is_none());
    assert_eq!(
        get_media_body(&harness.app, &media.url).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn project_member_cannot_delete_media() {
    let fixture = MediaFixture::new("forge-media-delete-role");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Delete role media").await;
    let media = upload_media_ok(
        &harness.app,
        &task.id,
        "delete-role.png",
        "image/png",
        b"scoped",
    )
    .await;
    let member_auth = register_user(&harness.app, "media-member@example.com").await;
    let member: UserResponse = bearer_get(
        &harness.app,
        "/api/v1/auth/me",
        &member_auth.access_token,
        StatusCode::OK,
    )
    .await;
    let _: ProjectMemberResponse = bearer_json(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/members", task.project_id),
        &common::test_jwt(),
        json!({ "user_id": member.id, "role": "member" }),
        StatusCode::CREATED,
    )
    .await;

    assert_eq!(
        get_media_body_with_bearer(&harness.app, &media.url, &member_auth.access_token)
            .await
            .0,
        StatusCode::OK
    );

    let delete_response = raw_empty_request_with_bearer(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/media/{}", media.id),
        &member_auth.access_token,
    )
    .await;
    assert_status(delete_response, StatusCode::FORBIDDEN).await;

    assert!(media_row(&harness, &media.id, false)
        .await
        .deleted_at
        .is_none());
}

#[tokio::test]
async fn get_media_streams_bytes() {
    let fixture = MediaFixture::new("forge-media-stream");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Stream media").await;

    let image = upload_media_ok(&harness.app, &task.id, "inline.png", "image/png", b"inline").await;
    let text = upload_media_ok(&harness.app, &task.id, "notes.txt", "text/plain", b"notes").await;

    let (status, headers, body) = get_media_body(&harness.app, &image.url).await;
    assert_eq!(status, StatusCode::OK);
    assert_header_eq(&headers, header::CONTENT_TYPE, "image/png");
    assert!(headers.get(header::CONTENT_DISPOSITION).is_none());
    assert_eq!(body, b"inline".to_vec());

    let (status, headers, body) = get_media_body(&harness.app, &text.url).await;
    assert_eq!(status, StatusCode::OK);
    assert_header_eq(&headers, header::CONTENT_TYPE, "text/plain");
    let disposition = headers
        .get(header::CONTENT_DISPOSITION)
        .expect("content disposition header")
        .to_str()
        .expect("content disposition is valid");
    assert!(disposition.starts_with("attachment;"));
    assert!(disposition.contains("notes.txt"));
    assert_eq!(body, b"notes".to_vec());
}

#[tokio::test]
async fn delete_media() {
    let fixture = MediaFixture::new("forge-media-delete");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Delete media").await;
    let media = upload_media_ok(&harness.app, &task.id, "delete.png", "image/png", b"delete").await;
    let path = stored_path(
        fixture.data_dir(),
        &media_row(&harness, &media.id, false).await,
    );
    assert!(path.exists());

    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/media/{}", media.id),
    )
    .await;
    assert_status(response, StatusCode::NO_CONTENT).await;

    let deleted = media_row(&harness, &media.id, true).await;
    assert!(deleted.deleted_at.is_some());
    assert!(!path.exists());
    assert!(list_task_media(&harness.app, &task.id)
        .await
        .items
        .is_empty());
    assert_eq!(
        get_media_body(&harness.app, &media.url).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn task_soft_delete_tombstones_media() {
    let fixture = MediaFixture::new("forge-media-task-delete");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Delete task media").await;
    let media = upload_media_ok(&harness.app, &task.id, "task.png", "image/png", b"task").await;
    let path = stored_path(
        fixture.data_dir(),
        &media_row(&harness, &media.id, false).await,
    );
    assert!(path.exists());

    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/tasks/{}", task.id),
    )
    .await;
    assert_status(response, StatusCode::NO_CONTENT).await;

    let deleted = media_row(&harness, &media.id, true).await;
    assert!(deleted.deleted_at.is_some());
    assert!(!path.exists());
    assert!(
        db::TaskMediaRepo::list_active_media_for_task(&*harness.state.db, &task.id)
            .await
            .expect("list active task media")
            .is_empty()
    );
    assert_eq!(
        get_media_body(&harness.app, &media.url).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn sse_events() {
    let fixture = MediaFixture::new("forge-media-events");
    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        None,
    )
    .await;
    let task = create_task(&harness.app, "Media events").await;
    let mut events_rx = harness.event_bus.subscribe();

    let media = upload_media_ok(&harness.app, &task.id, "event.png", "image/png", b"event").await;
    let uploaded = next_event_of_type(&mut events_rx, "task.media.uploaded").await;
    match uploaded.context {
        EventContext::TaskMediaUploaded {
            task_id,
            media_id,
            content_type,
            byte_size,
            filename,
        } => {
            assert_eq!(task_id, task.id);
            assert_eq!(media_id, media.id);
            assert_eq!(content_type, "image/png");
            assert_eq!(byte_size, 5);
            assert_eq!(filename, "event.png");
        }
        other => panic!("unexpected upload event context: {other:?}"),
    }

    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/media/{}", media.id),
    )
    .await;
    assert_status(response, StatusCode::NO_CONTENT).await;
    let deleted = next_event_of_type(&mut events_rx, "task.media.deleted").await;
    match deleted.context {
        EventContext::TaskMediaDeleted { task_id, media_id } => {
            assert_eq!(task_id, task.id);
            assert_eq!(media_id, media.id);
        }
        other => panic!("unexpected delete event context: {other:?}"),
    }
}

#[tokio::test]
async fn stable_url_survives_restart() {
    let fixture = MediaFixture::new("forge-media-restart");
    let db_path = fixture.root().join("forge.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let harness = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        Some(&db_url),
    )
    .await;
    let task = create_task(&harness.app, "Restart media").await;
    let media = upload_media_ok(
        &harness.app,
        &task.id,
        "restart.png",
        "image/png",
        b"restart",
    )
    .await;
    drop(harness);

    let restarted = test_app(
        fixture.workspace_root(),
        fixture.data_dir(),
        DEFAULT_MEDIA_LIMIT,
        Some(&db_url),
    )
    .await;
    let (status, headers, body) = get_media_body(&restarted.app, &media.url).await;

    assert_eq!(status, StatusCode::OK);
    assert_header_eq(&headers, header::CONTENT_TYPE, "image/png");
    assert_eq!(body, b"restart".to_vec());
}

struct TestHarness {
    app: Router,
    state: Arc<AppState>,
    event_bus: Arc<EventBus>,
    _web_dist_dir: common::TestDir,
}

async fn test_app(
    workspace_root: &Path,
    data_dir: &Path,
    media_upload_limit_bytes: u64,
    database_url: Option<&str>,
) -> TestHarness {
    std::fs::create_dir_all(workspace_root).expect("workspace root creates");
    std::fs::create_dir_all(data_dir).expect("data dir creates");

    let pool = db::create_sqlite_pool(database_url.unwrap_or("sqlite::memory:"))
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");

    let db = Arc::new(db::SqliteDb::new(pool));
    seed_test_user(db.as_ref()).await;

    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(128));
    let merge_service = Arc::new(services::MergeService::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let cleanup_scheduler = Arc::new(services::WorkspaceCleanupScheduler::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        workspace_root.to_path_buf(),
    ));
    let review_runner = Arc::new(review::ReviewRunner::new(
        Arc::clone(&db),
        Arc::clone(&event_bus),
        Arc::clone(&adapter_registry),
    ));

    let mut config = config::ForgeConfig::with_data_dir(data_dir.to_path_buf());
    config.workspace.root = workspace_root.to_path_buf();
    config.server.media_upload_limit_bytes = media_upload_limit_bytes;

    let state = Arc::new(
        AppState::with_adapter_registry_services_and_shutdown(
            db,
            Arc::clone(&event_bus),
            true,
            adapter_registry,
            merge_service,
            cleanup_scheduler,
            review_runner,
            api::state::ShutdownSignal::new(),
            data_dir.join("workflows"),
            api::state::test_jwt_secret(),
            api::state::test_bcrypt_cost(),
        )
        .with_effective_config(config),
    );

    let web_dist_dir = common::TestDir::new("forge-media-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    TestHarness {
        app,
        state,
        event_bus,
        _web_dist_dir: web_dist_dir,
    }
}

async fn seed_test_user(db: &db::SqliteDb) {
    if db::UserRepo::get_user_by_id(db, "test-user-id")
        .await
        .expect("lookup test user")
        .is_some()
    {
        return;
    }

    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        db,
        &db::User {
            id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            password_hash: "$2b$04$placeholder".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("seed test user");
}

async fn create_task(app: &Router, title: &str) -> TaskResponse {
    let project: ProjectResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": title }),
        StatusCode::OK,
    )
    .await;

    common::json_request(
        app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": title,
            "description": "media test task"
        }),
        StatusCode::OK,
    )
    .await
}

async fn upload_media_ok(
    app: &Router,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
) -> TaskMediaResponse {
    let response = raw_upload_media(app, task_id, filename, content_type, bytes).await;
    parse_response(response, StatusCode::CREATED).await
}

async fn raw_upload_media(
    app: &Router,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
) -> axum::response::Response {
    raw_upload_media_with_bearer(
        app,
        task_id,
        filename,
        content_type,
        bytes,
        &common::test_jwt(),
    )
    .await
}

async fn raw_upload_media_with_bearer(
    app: &Router,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    token: &str,
) -> axum::response::Response {
    raw_upload_media_with_author_and_bearer(
        app,
        task_id,
        filename,
        content_type,
        bytes,
        b"Tester",
        token,
    )
    .await
}

async fn raw_upload_media_with_author(
    app: &Router,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    author_name: &[u8],
) -> axum::response::Response {
    raw_upload_media_with_author_and_bearer(
        app,
        task_id,
        filename,
        content_type,
        bytes,
        author_name,
        &common::test_jwt(),
    )
    .await
}

async fn raw_upload_media_with_author_and_bearer(
    app: &Router,
    task_id: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    author_name: &[u8],
    token: &str,
) -> axum::response::Response {
    let boundary = format!("forge-test-boundary-{}", uuid::Uuid::new_v4());
    let body = multipart_body(&boundary, filename, content_type, bytes, author_name);
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/tasks/{task_id}/media"))
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body))
                .expect("build multipart request"),
        )
        .await
        .expect("router response")
}

fn multipart_body(
    boundary: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
    author_name: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"author_name\"\r\n\r\n");
    body.extend_from_slice(author_name);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

async fn list_task_media(app: &Router, task_id: &str) -> PaginatedResponse<TaskMediaResponse> {
    common::empty_request(
        app,
        Method::GET,
        &format!("/api/v1/tasks/{task_id}/media"),
        StatusCode::OK,
    )
    .await
}

async fn media_row(harness: &TestHarness, media_id: &str, include_deleted: bool) -> db::TaskMedia {
    db::TaskMediaRepo::get_media_by_id(&*harness.state.db, media_id, include_deleted)
        .await
        .expect("load media row")
        .expect("media row exists")
}

fn stored_path(data_dir: &Path, media: &db::TaskMedia) -> PathBuf {
    data_dir.join("media").join(&media.storage_key)
}

fn read_asset(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../assets")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read asset {}: {error}", path.display()))
}

fn storage_key_is_safe(storage_key: &str) -> bool {
    Path::new(storage_key)
        .components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

async fn assert_no_media_created(harness: &TestHarness, data_dir: &Path, task_id: &str) {
    assert!(list_task_media(&harness.app, task_id)
        .await
        .items
        .is_empty());
    assert_eq!(media_file_count(&data_dir.join("media")), 0);
}

fn media_file_count(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }

    let mut count = 0;
    for entry in std::fs::read_dir(path).expect("read media directory") {
        let path = entry.expect("media directory entry").path();
        if path.is_dir() {
            count += media_file_count(&path);
        } else {
            count += 1;
        }
    }
    count
}

async fn get_media_body(app: &Router, url: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = common::raw_empty_request(app, Method::GET, url).await;
    response_body(response).await
}

async fn get_media_body_with_bearer(
    app: &Router,
    url: &str,
    token: &str,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = raw_empty_request_with_bearer(app, Method::GET, url, token).await;
    response_body(response).await
}

async fn response_body(response: axum::response::Response) -> (StatusCode, HeaderMap, Vec<u8>) {
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read media body")
        .to_vec();
    (status, headers, body)
}

async fn register_user(app: &Router, email: &str) -> AuthResponse {
    common::json_request(
        app,
        Method::POST,
        "/api/v1/auth/register",
        json!({ "email": email, "password": "Password123!" }),
        StatusCode::CREATED,
    )
    .await
}

async fn bearer_get<T: DeserializeOwned>(
    app: &Router,
    uri: &str,
    token: &str,
    expected_status: StatusCode,
) -> T {
    let response = raw_empty_request_with_bearer(app, Method::GET, uri, token).await;
    parse_response(response, expected_status).await
}

async fn bearer_json<T: DeserializeOwned>(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
    body: serde_json::Value,
    expected_status: StatusCode,
) -> T {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build authorized JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn raw_empty_request_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    token: &str,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .expect("build authorized empty request"),
        )
        .await
        .expect("router response")
}

fn assert_header_eq(headers: &HeaderMap, name: HeaderName, expected: &str) {
    assert_eq!(
        headers
            .get(name)
            .expect("header exists")
            .to_str()
            .expect("header value is valid"),
        expected
    );
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

async fn assert_status(response: axum::response::Response, expected_status: StatusCode) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
}

async fn next_event_of_type(
    rx: &mut broadcast::Receiver<ForgeEvent>,
    event_type: &str,
) -> ForgeEvent {
    for _ in 0..20 {
        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("event received before timeout")
            .expect("event received");
        if event.event_type == event_type {
            return event;
        }
    }
    panic!("did not receive event type {event_type}");
}

struct MediaFixture {
    root: common::TestDir,
    workspace_root: PathBuf,
    data_dir: PathBuf,
}

impl MediaFixture {
    fn new(prefix: &str) -> Self {
        let root = common::TestDir::new(prefix);
        let workspace_root = root.path().join("workspace");
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(&workspace_root).expect("workspace dir creates");
        std::fs::create_dir_all(&data_dir).expect("data dir creates");
        Self {
            root,
            workspace_root,
            data_dir,
        }
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}
