mod common;

use std::path::PathBuf;
use std::process::Command;
use std::{sync::Arc, time::Duration};

use api_types::{
    DaemonHandshakeNotification, PaginatedResponse, ProjectResponse, RepoLocationResponse,
    RepoLocationStatus, RepoLocationVerifyParams, RepoLocationVerifyResult, RepoResponse,
    DAEMON_CAPABILITY_JOURNAL_ACK, DAEMON_CAPABILITY_USAGE_REPORTS, DAEMON_CAPABILITY_WORKSPACE,
    DAEMON_PROTOCOL_REVISION, METHOD_DAEMON_HANDSHAKE, METHOD_REPO_LOCATION_VERIFY,
};
use axum::http::{Method, StatusCode};
use db::{
    CreateRepoLocation, CreateRuntime, CreateWorkspace, CreateWorkspacePlacement, DaemonRepo,
    DaemonStatus, PlacementOwnerKind, PlacementSelectedBy, PlacementState, RepoLocationKind,
    RepoLocationOwnerKind, RepoLocationRepo, RepoRepo, RuntimeRepo, RuntimeStatus, UpsertDaemon,
    WorkspacePlacementRepo, WorkspaceRepo, WorkspaceStatus,
};
use serde_json::{json, Value};

use common::{empty_request, json_request, Harness, TestDir};

struct Fixture {
    harness: Harness,
    repo_id: String,
    checkout: PathBuf,
    _root: TestDir,
}

impl Fixture {
    async fn new(prefix: &str) -> Self {
        let root = TestDir::new(prefix);
        let checkout = common::setup_git_repo(root.path());
        let harness = common::test_app(&root.path().join("workspaces"), prefix).await;
        let project: ProjectResponse = json_request(
            &harness.app,
            Method::POST,
            "/api/v1/projects",
            json!({ "name": prefix }),
            StatusCode::OK,
        )
        .await;
        let repo: RepoResponse = json_request(
            &harness.app, Method::POST, &format!("/api/v1/projects/{}/repos", project.id),
            json!({ "name": "repo", "remote_url": "https://example.test/repo.git", "default_branch": "main" }),
            StatusCode::OK,
        ).await;
        Self {
            harness,
            repo_id: repo.id,
            checkout,
            _root: root,
        }
    }

    fn locations_path(&self) -> String {
        format!("/api/v1/repos/{}/locations", self.repo_id)
    }

    fn location_path(&self, id: &str) -> String {
        format!("{}/{}", self.locations_path(), id)
    }

    async fn register_server(&self, path: &str, is_default: bool) -> RepoLocationResponse {
        json_request(
            &self.harness.app, Method::POST, &self.locations_path(),
            json!({ "owner_kind": "server", "path": path, "kind": "primary_checkout", "is_default": is_default }),
            StatusCode::OK,
        ).await
    }

    async fn daemon_runtime(&self) -> (String, String) {
        let now = db::now_rfc3339();
        let daemon_id = db::new_uuid_v4();
        DaemonRepo::upsert_by_machine_id(
            &*self.harness.state.db,
            UpsertDaemon {
                max_concurrent_runs: None,
                id: daemon_id.clone(),
                machine_id: db::new_uuid_v4(),
                hostname: "mac".to_owned(),
                os: "macos".to_owned(),
                arch: "aarch64".to_owned(),
                agent_version: None,
                labels_json: "{}".to_owned(),
                status: DaemonStatus::Online,
                registration_token_hash: None,
                owner_id: Some("test-user-id".to_owned()),
                visibility: "account".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("daemon creates");
        let runtime_id = db::new_uuid_v4();
        RuntimeRepo::create(
            &*self.harness.state.db,
            CreateRuntime {
                id: runtime_id.clone(),
                daemon_id: daemon_id.clone(),
                kind: "cli".to_owned(),
                workspace_root: "/remote/workspaces".to_owned(),
                status: RuntimeStatus::Ready,
                labels_json: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("runtime creates");
        (daemon_id, runtime_id)
    }

    async fn hide_daemon(&self, daemon_id: &str) {
        let now = db::now_rfc3339();
        db::UserRepo::create_user(
            &*self.harness.state.db,
            &db::User {
                id: "another-account".to_owned(),
                email: "another@example.test".to_owned(),
                password_hash: "$2b$04$placeholder".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("another account creates");
        sqlx::query("UPDATE daemon SET owner_id = 'another-account' WHERE id = ?")
            .bind(daemon_id)
            .execute(self.harness.state.db.pool())
            .await
            .expect("daemon hides");
    }
}

#[tokio::test]
async fn register_server_location_and_list_with_opaque_cursors() {
    let fixture = Fixture::new("forge-api-location-register").await;
    let path = fixture.checkout.to_string_lossy();
    let first = fixture.register_server(&path, true).await;
    let second = fixture.register_server(&path, false).await;
    assert_eq!(first.status, RepoLocationStatus::Ready);
    assert!(first.last_verified_at.is_some());
    assert_eq!(first.last_error, None);
    assert!(first.is_default);
    assert_eq!(first.repo_id, fixture.repo_id);

    let page: PaginatedResponse<RepoLocationResponse> = empty_request(
        &fixture.harness.app,
        Method::GET,
        &format!(
            "{}?limit=1&include_total=true&sort_by=id&sort_order=asc",
            fixture.locations_path()
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.total_count, Some(2));
    assert!(page.has_more);
    let cursor = page.next_cursor.as_ref().expect("next cursor");
    let next: PaginatedResponse<RepoLocationResponse> = empty_request(
        &fixture.harness.app,
        Method::GET,
        &format!(
            "{}?limit=1&sort_by=id&sort_order=asc&cursor={cursor}",
            fixture.locations_path()
        ),
        StatusCode::OK,
    )
    .await;
    assert_eq!(next.items.len(), 1);
    assert!(!next.has_more);
    assert_eq!(next.next_cursor, None);
    assert_ne!(page.items[0].id, next.items[0].id);
    let mut ids = vec![page.items[0].id.clone(), next.items[0].id.clone()];
    ids.sort();
    let mut expected = vec![first.id, second.id];
    expected.sort();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn verify_failure_persists_invalid_status_and_error() {
    let fixture = Fixture::new("forge-api-location-verify-failure").await;
    let missing = fixture.checkout.join("missing");
    let location = fixture
        .register_server(&missing.to_string_lossy(), false)
        .await;
    assert_eq!(location.status, RepoLocationStatus::Invalid);
    assert_eq!(location.last_error.as_deref(), Some("path_not_found"));
    let verified: RepoLocationResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        &format!("{}/verify", fixture.location_path(&location.id)),
        json!({ "version": location.version }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(verified.status, RepoLocationStatus::Invalid);
    assert_eq!(verified.last_error.as_deref(), Some("path_not_found"));
    assert!(verified.last_verified_at.is_some());
    assert_eq!(verified.version, location.version + 1);
    let stored = RepoLocationRepo::get_by_id(&*fixture.harness.state.db, &location.id)
        .await
        .expect("location loads")
        .expect("location exists");
    assert_eq!(stored.status, db::RepoLocationStatus::Invalid);
    assert_eq!(stored.last_error, verified.last_error);
}

#[tokio::test]
async fn register_daemon_location_marks_unavailable_without_reading_remote_path() {
    let fixture = Fixture::new("forge-api-location-daemon").await;
    let (daemon_id, runtime_id) = fixture.daemon_runtime().await;
    let location: RepoLocationResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "daemon", "daemon_id": daemon_id, "runtime_id": runtime_id,
            "path": "/remote/workspaces/repo", "kind": "primary_checkout" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(location.status, RepoLocationStatus::Unavailable);
    assert_eq!(location.last_error.as_deref(), Some("daemon_unavailable"));
    assert_eq!(location.path, "/remote/workspaces/repo");
    assert_eq!(location.daemon_id.as_deref(), Some(daemon_id.as_str()));
    assert_eq!(location.runtime_id.as_deref(), Some(runtime_id.as_str()));
}

#[tokio::test]
async fn register_daemon_location_outside_runtime_root_is_invalid() {
    let fixture = Fixture::new("forge-api-location-outside-root").await;
    let (daemon_id, runtime_id) = fixture.daemon_runtime().await;
    for path in [
        "/remote/workspaces-other/repo",
        "/remote/workspaces/../repo",
    ] {
        let location: RepoLocationResponse = json_request(
            &fixture.harness.app,
            Method::POST,
            &fixture.locations_path(),
            json!({ "owner_kind": "daemon", "daemon_id": daemon_id, "runtime_id": runtime_id,
                "path": path, "kind": "primary_checkout" }),
            StatusCode::OK,
        )
        .await;
        assert_eq!(location.status, RepoLocationStatus::Invalid);
        assert_eq!(
            location.last_error.as_deref(),
            Some("outside_workspace_root")
        );
    }
}

#[tokio::test]
async fn register_shared_mount_requires_daemon_verification() {
    let fixture = Fixture::new("forge-api-location-shared-mount").await;
    let (daemon_id, runtime_id) = fixture.daemon_runtime().await;
    sqlx::query("UPDATE runtime SET workspace_root = ? WHERE id = ?")
        .bind(fixture._root.path().to_string_lossy().as_ref())
        .bind(&runtime_id)
        .execute(fixture.harness.state.db.pool())
        .await
        .expect("runtime root changes");
    let location: RepoLocationResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "server", "daemon_id": daemon_id, "runtime_id": runtime_id,
            "path": fixture.checkout, "kind": "shared_mount" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(location.status, RepoLocationStatus::Unavailable);
    assert_eq!(location.last_error.as_deref(), Some("daemon_unavailable"));
}

#[tokio::test]
async fn accepted_daemon_handshake_retries_shared_mount_verification() {
    use common::fake_daemon::{
        next_daemon_request, register_daemon, report_remote_daemon_shell, send_daemon_notification,
        send_daemon_response, TestServer,
    };

    let fixture = Fixture::new("forge-api-location-reconnect").await;
    let registration = register_daemon(
        &fixture.harness.app,
        &db::new_uuid_v4(),
        "forge-api-location-reconnect",
    )
    .await;
    report_remote_daemon_shell(
        &fixture.harness.app,
        &registration.daemon_id,
        &registration.registration_token,
        fixture._root.path(),
        "forge-api-location-reconnect",
    )
    .await;
    let runtime_id: String =
        sqlx::query_scalar("SELECT id FROM runtime WHERE daemon_id = ? AND workspace_root = ?")
            .bind(&registration.daemon_id)
            .bind(fixture._root.path().to_string_lossy().as_ref())
            .fetch_one(fixture.harness.state.db.pool())
            .await
            .expect("reported runtime exists");
    let location: RepoLocationResponse = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "server", "daemon_id": registration.daemon_id,
            "runtime_id": runtime_id, "path": fixture.checkout, "kind": "shared_mount" }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(location.status, RepoLocationStatus::Unavailable);
    assert_eq!(location.last_error.as_deref(), Some("daemon_unavailable"));

    let server = TestServer::start(Arc::clone(&fixture.harness.state)).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!(
        "ws://{}/api/v1/daemons/{}/connect?token={}",
        server.addr, registration.daemon_id, registration.registration_token,
    ))
    .await
    .expect("daemon connects");
    send_daemon_notification(
        &mut socket,
        METHOD_DAEMON_HANDSHAKE,
        DaemonHandshakeNotification {
            protocol_revision: DAEMON_PROTOCOL_REVISION,
            capabilities: vec![
                DAEMON_CAPABILITY_USAGE_REPORTS.to_owned(),
                DAEMON_CAPABILITY_JOURNAL_ACK.to_owned(),
                DAEMON_CAPABILITY_WORKSPACE.to_owned(),
            ],
            executor_capabilities: Default::default(),
            workspace_run_policy: Default::default(),
        },
    )
    .await;
    let (request_id, params) = next_daemon_request(&mut socket, METHOD_REPO_LOCATION_VERIFY).await;
    let params: RepoLocationVerifyParams = serde_json::from_value(params).expect("verify params");
    assert_eq!(params.repo_location_id, location.id);
    assert_eq!(params.daemon_id, registration.daemon_id);
    assert_eq!(params.runtime_id, runtime_id);
    assert_eq!(params.path, fixture.checkout.to_string_lossy());
    assert_eq!(params.expected_version, location.version);
    let probe = params.probe.expect("shared mount carries probe");
    let probe_path = PathBuf::from(&probe.path);
    assert!(probe_path.starts_with(
        fixture
            .harness
            .state
            .cleanup_scheduler
            .workspace_root()
            .canonicalize()
            .expect("server worktree root exists")
    ));
    assert!(!probe_path.starts_with(fixture.checkout.canonicalize().unwrap()));
    assert_eq!(std::fs::read_to_string(&probe_path).unwrap(), probe.content);
    send_daemon_response(
        &mut socket,
        request_id,
        RepoLocationVerifyResult {
            repo_location_id: location.id.clone(),
            path: location.path.clone(),
            default_branch_sha: run_git(&fixture.checkout, &["rev-parse", "main"]),
            origin_url: None,
            probe_content: Some(probe.content),
        },
    )
    .await;
    let stored = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stored = RepoLocationRepo::get_by_id(&*fixture.harness.state.db, &location.id)
                .await
                .expect("location loads")
                .expect("location exists");
            if stored.status == db::RepoLocationStatus::Ready {
                break stored;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("socket reader receives verification response and status persists");
    assert_eq!(stored.version, location.version + 1);
    assert_eq!(stored.last_error, None);
    assert!(stored.last_verified_at.is_some());
    assert!(!probe_path.exists());
}

#[tokio::test]
async fn verify_server_location_checks_git_branch_and_remote() {
    let fixture = Fixture::new("forge-api-location-git-checks").await;
    let not_git = fixture._root.path().join("not-git");
    std::fs::create_dir(&not_git).expect("plain directory creates");
    let plain = fixture
        .register_server(&not_git.to_string_lossy(), false)
        .await;
    assert_eq!(plain.status, RepoLocationStatus::Invalid);
    assert_eq!(plain.last_error.as_deref(), Some("not_git_work_tree"));
    run_git(
        &fixture.checkout,
        &["remote", "add", "origin", "https://example.test/wrong.git"],
    );
    let wrong_remote = fixture
        .register_server(&fixture.checkout.to_string_lossy(), false)
        .await;
    assert_eq!(wrong_remote.last_error.as_deref(), Some("remote_mismatch"));
    run_git(&fixture.checkout, &["remote", "remove", "origin"]);
    run_git(&fixture.checkout, &["branch", "-m", "main", "other"]);
    let wrong_branch = fixture
        .register_server(&fixture.checkout.to_string_lossy(), false)
        .await;
    assert_eq!(
        wrong_branch.last_error.as_deref(),
        Some("default_branch_not_found")
    );
}

#[tokio::test]
async fn set_default_clears_previous_default_and_rejects_stale_versions() {
    let fixture = Fixture::new("forge-api-location-default").await;
    let first = fixture
        .register_server(&fixture.checkout.to_string_lossy(), true)
        .await;
    let second = fixture
        .register_server(&fixture.checkout.to_string_lossy(), false)
        .await;
    let updated: RepoLocationResponse = json_request(
        &fixture.harness.app,
        Method::PATCH,
        &fixture.location_path(&second.id),
        json!({ "version": second.version, "is_default": true }),
        StatusCode::OK,
    )
    .await;
    assert!(updated.is_default);
    assert_eq!(updated.version, second.version + 1);
    let old = RepoLocationRepo::get_by_id(&*fixture.harness.state.db, &first.id)
        .await
        .expect("old default loads")
        .expect("old default exists");
    assert!(!old.is_default);
    assert_eq!(old.version, first.version + 1);
    let conflict: Value = json_request(
        &fixture.harness.app,
        Method::PATCH,
        &fixture.location_path(&second.id),
        json!({ "version": second.version, "is_default": false }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(conflict["code"], "version_conflict");
    let conflict: Value = json_request(
        &fixture.harness.app,
        Method::POST,
        &format!("{}/verify", fixture.location_path(&second.id)),
        json!({ "version": second.version }),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(conflict["code"], "version_conflict");
}

#[tokio::test]
async fn delete_in_use_location_returns_409_naming_task() {
    let fixture = Fixture::new("forge-api-location-in-use").await;
    let location = fixture
        .register_server(&fixture.checkout.to_string_lossy(), false)
        .await;
    let db = &*fixture.harness.state.db;
    let repo = RepoRepo::get_by_id(db, &fixture.repo_id)
        .await
        .expect("repo loads")
        .expect("repo exists");
    let task_id = db::new_uuid_v4();
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, 'Location-using Task', 'todo', ?, ?)")
        .bind(&task_id).bind(&repo.project_id).bind(&now).bind(&now)
        .execute(db.pool()).await.expect("task inserts");
    let workspace = WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: db::new_uuid_v4(),
            task_id: task_id.clone(),
            repo_id: repo.id,
            worktree_path: fixture.checkout.to_string_lossy().into_owned(),
            branch: "main".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    let placement = WorkspacePlacementRepo::create(
        db,
        CreateWorkspacePlacement {
            id: db::new_uuid_v4(),
            workspace_id: workspace.id,
            task_id: task_id.clone(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: location.id.clone(),
            execution_daemon_id: None,
            workspace_handle: Some(fixture.checkout.to_string_lossy().into_owned()),
            generation: 1,
            state: PlacementState::Ready,
            selected_by: PlacementSelectedBy::Scheduler,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("placement creates");
    let error: Value = empty_request(
        &fixture.harness.app,
        Method::DELETE,
        &fixture.location_path(&location.id),
        StatusCode::CONFLICT,
    )
    .await;
    assert_eq!(error["code"], "conflict");
    let message = error["message"].as_str().expect("error message");
    assert!(message.contains(&task_id));
    assert!(message.contains("Location-using Task"));
    assert!(message.contains(&placement.id));
    assert!(RepoLocationRepo::get_by_id(db, &location.id)
        .await
        .expect("location loads")
        .is_some());
    sqlx::query("UPDATE workspace_placement SET state = 'cleaned', version = version + 1 WHERE id = ? AND version = ?")
        .bind(&placement.id).bind(placement.version).execute(db.pool()).await.expect("placement cleans");
    let response = common::raw_empty_request(
        &fixture.harness.app,
        Method::DELETE,
        &fixture.location_path(&location.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

fn run_git(path: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn location_registration_requires_owner_or_admin_and_visible_matching_runtime() {
    let fixture = Fixture::new("forge-api-location-authorization").await;
    let (daemon_id, runtime_id) = fixture.daemon_runtime().await;
    fixture.hide_daemon(&daemon_id).await;
    let error: Value = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "daemon", "daemon_id": daemon_id, "runtime_id": runtime_id,
            "path": "/remote/workspaces/repo", "kind": "primary_checkout" }),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error["code"], "not_found");
    sqlx::query("UPDATE daemon SET visibility = 'global' WHERE id = ?")
        .bind(&daemon_id)
        .execute(fixture.harness.state.db.pool())
        .await
        .expect("daemon publishes");
    let (_, other_runtime) = fixture.daemon_runtime().await;
    let error: Value = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "daemon", "daemon_id": daemon_id, "runtime_id": other_runtime,
            "path": "/remote/workspaces/repo", "kind": "primary_checkout" }),
        StatusCode::NOT_FOUND,
    )
    .await;
    assert_eq!(error["code"], "not_found");
    let repo = RepoRepo::get_by_id(&*fixture.harness.state.db, &fixture.repo_id)
        .await
        .expect("repo loads")
        .expect("repo exists");
    sqlx::query("UPDATE project_member SET role = 'member' WHERE project_id = ? AND user_id = 'test-user-id'")
        .bind(&repo.project_id).execute(fixture.harness.state.db.pool()).await.expect("role changes");
    let error: Value = json_request(
        &fixture.harness.app,
        Method::POST,
        &fixture.locations_path(),
        json!({ "owner_kind": "server", "path": fixture.checkout, "kind": "primary_checkout" }),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(error["code"], "authorization.invalid");
}

#[tokio::test]
async fn list_locations_filters_daemon_visibility_before_pagination_and_counts() {
    let fixture = Fixture::new("forge-api-location-visible-page").await;
    let visible = fixture
        .register_server(&fixture.checkout.to_string_lossy(), false)
        .await;
    let (daemon_id, runtime_id) = fixture.daemon_runtime().await;
    let now = db::now_rfc3339();
    RepoLocationRepo::create(
        &*fixture.harness.state.db,
        CreateRepoLocation {
            id: db::new_uuid_v4(),
            repo_id: fixture.repo_id.clone(),
            owner_kind: RepoLocationOwnerKind::Daemon,
            daemon_id: Some(daemon_id.clone()),
            runtime_id: Some(runtime_id),
            path: "/remote/workspaces/repo".to_owned(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: false,
            status: db::RepoLocationStatus::Unverified,
            last_verified_at: None,
            last_error: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("hidden location creates");
    fixture.hide_daemon(&daemon_id).await;
    let page: PaginatedResponse<RepoLocationResponse> = empty_request(
        &fixture.harness.app,
        Method::GET,
        &format!("{}?limit=1&include_total=true", fixture.locations_path()),
        StatusCode::OK,
    )
    .await;
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, visible.id);
    assert_eq!(page.total_count, Some(1));
    assert!(!page.has_more);
    assert_eq!(page.next_cursor, None);
}
