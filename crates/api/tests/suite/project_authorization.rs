use crate::common;
use api_types::{ProjectResponse, RepoResponse, TaskResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
};
use db::{ProjectMemberRepo, TaskRepo};
use serde_json::{json, Value};
use tower::ServiceExt;

struct Fixture {
    harness: common::Harness,
    project: ProjectResponse,
    task: TaskResponse,
    repo: RepoResponse,
    other_id: String,
    other_token: String,
    _root: common::TestDir,
}

impl Fixture {
    async fn new() -> Self {
        let root = common::TestDir::new("project-authorization");
        let harness = common::test_app(root.path(), "project-authorization").await;
        let other_id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        db::UserRepo::create_user(
            &*harness.state.db,
            &db::User {
                id: other_id.clone(),
                email: "other@authorization.test".into(),
                password_hash: "unused".into(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let other_token = token(&other_id);
        let project: ProjectResponse = common::json_request(
            &harness.app,
            Method::POST,
            "/api/v1/projects",
            json!({"name":"Private"}),
            StatusCode::OK,
        )
        .await;
        let repo: RepoResponse = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/projects/{}/repos", project.id),
            json!({"name":"repo","remote_url":"https://example.invalid/authz.git"}),
            StatusCode::OK,
        )
        .await;
        let task = common::json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/projects/{}/tasks", project.id),
            json!({"title":"Private task"}),
            StatusCode::OK,
        )
        .await;
        Self {
            harness,
            project,
            task,
            repo,
            other_id,
            other_token,
            _root: root,
        }
    }

    async fn role(&self, role: Option<&str>) {
        let db = &*self.harness.state.db;
        if ProjectMemberRepo::get_member(db, &self.project.id, &self.other_id)
            .await
            .unwrap()
            .is_some()
        {
            ProjectMemberRepo::remove_member(db, &self.project.id, &self.other_id)
                .await
                .unwrap();
        }
        if let Some(role) = role {
            let now = db::now_rfc3339();
            ProjectMemberRepo::add_member(
                db,
                db::CreateProjectMember {
                    id: db::new_uuid_v4(),
                    project_id: self.project.id.clone(),
                    user_id: self.other_id.clone(),
                    role: role.into(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        }
    }

    fn path(&self, path: &str) -> String {
        path.replace("{project}", &self.project.id)
            .replace("{task}", &self.task.id)
            .replace("{repo}", &self.repo.id)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        bearer: &str,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder()
            .method(method)
            .uri(self.path(path))
            .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        let body = match body {
            Some(body) => {
                builder = builder.header(header::CONTENT_TYPE, "application/json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        let response = self
            .harness
            .app
            .clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn reads(&self, paths: &[&str]) {
        for path in paths {
            let (status, body) = self
                .request(Method::GET, path, None, &self.other_token)
                .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "non-member {path}: {body}");
            assert_eq!(body["code"], "not_found", "{path}: {body}");
        }
        for role in ["viewer", "member"] {
            self.role(Some(role)).await;
            for path in paths {
                let (status, body) = self
                    .request(Method::GET, path, None, &self.other_token)
                    .await;
                assert_eq!(status, StatusCode::OK, "{role} {path}: {body}");
            }
        }
        for path in paths {
            let (status, body) = self
                .request(Method::GET, path, None, &common::test_jwt())
                .await;
            assert_eq!(status, StatusCode::OK, "owner {path}: {body}");
        }
    }

    async fn denied_writes(&self, writes: &[(Method, String, Option<Value>)]) {
        self.role(None).await;
        for (method, path, body) in writes {
            let (status, response) = self
                .request(method.clone(), path, body.clone(), &self.other_token)
                .await;
            assert_eq!(
                status,
                StatusCode::NOT_FOUND,
                "non-member {method} {path}: {response}"
            );
        }
        self.role(Some("viewer")).await;
        for (method, path, body) in writes {
            let (status, response) = self
                .request(method.clone(), path, body.clone(), &self.other_token)
                .await;
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "viewer {method} {path}: {response}"
            );
            assert_eq!(
                response["code"], "insufficient_role",
                "{method} {path}: {response}"
            );
        }
    }
}

fn token(user_id: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    jsonwebtoken::encode(&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256), &json!({
        "sub":user_id,"email":"other@authorization.test","is_admin":false,"iat":now,"exp":now+3600
    }), &jsonwebtoken::EncodingKey::from_secret(b"test-jwt-secret-for-development")).unwrap()
}

macro_rules! read_family {
    ($name:ident, [$($path:expr),+ $(,)?]) => {
        #[tokio::test]
        async fn $name() {
            Fixture::new().await.reads(&[$($path),+]).await;
        }
    };
}

read_family!(
    project_configuration_reads,
    [
        "/api/v1/projects/{project}",
        "/api/v1/projects/{project}/workflow",
        "/api/v1/projects/{project}/project_hook_runs"
    ]
);
read_family!(
    project_artifact_reads,
    [
        "/api/v1/projects/{project}/documents",
        "/api/v1/projects/{project}/decisions",
        "/api/v1/projects/{project}/decisions/candidates",
        "/api/v1/projects/{project}/milestones"
    ]
);
read_family!(
    project_analytics_reads,
    [
        "/api/v1/projects/{project}/analytics",
        "/api/v1/projects/{project}/overview"
    ]
);
read_family!(
    project_setup_and_members_reads,
    [
        "/api/v1/projects/{project}/execution-setup",
        "/api/v1/projects/{project}/members",
        "/api/v1/projects/{project}/agents"
    ]
);
read_family!(
    repository_and_integration_reads,
    [
        "/api/v1/projects/{project}/repos",
        "/api/v1/repos/{repo}",
        "/api/v1/repos/{repo}/integration-queues",
        "/api/v1/projects/{project}/integration"
    ]
);
read_family!(
    task_detail_and_actions_reads,
    [
        "/api/v1/tasks/{task}",
        "/api/v1/tasks/{task}/detail",
        "/api/v1/tasks/{task}/relations",
        "/api/v1/tasks/{task}/actions",
        "/api/v1/tasks/{task}/transitions"
    ]
);
read_family!(
    task_children_and_dependency_reads,
    [
        "/api/v1/tasks/{task}/dependencies",
        "/api/v1/tasks/{task}/dependents",
        "/api/v1/tasks/{task}/roles",
        "/api/v1/projects/{project}/tasks"
    ]
);
read_family!(
    comments_and_media_reads,
    [
        "/api/v1/tasks/{task}/comments",
        "/api/v1/tasks/{task}/media",
        "/api/v1/projects/{project}/media"
    ]
);
read_family!(
    execution_review_and_usage_reads,
    [
        "/api/v1/tasks/{task}/executions",
        "/api/v1/tasks/{task}/reviews",
        "/api/v1/tasks/{task}/usage"
    ]
);
read_family!(
    terminal_reads,
    [
        "/api/v1/tasks/{task}/terminals",
        "/api/v1/tasks/{task}/terminals/availability"
    ]
);
read_family!(
    memory_reads,
    ["/api/v1/projects/{project}/memory/search?query=authorization"]
);
read_family!(
    notification_and_coordination_reads,
    [
        "/api/v1/notifications?project_id={project}",
        "/api/v1/notifications/unread-count?project_id={project}",
        "/api/v1/mission-control?project_id={project}",
        "/api/v1/projects/{project}/reconciliations"
    ]
);

#[tokio::test]
async fn task_writes_require_member_and_owner_still_works() {
    let f = Fixture::new().await;
    let writes = vec![
        (
            Method::POST,
            "/api/v1/projects/{project}/tasks".into(),
            Some(json!({"title":"Member task"})),
        ),
        (
            Method::PATCH,
            "/api/v1/tasks/{task}".into(),
            Some(json!({"title":"Edited","version":f.task.version})),
        ),
        (Method::POST, "/api/v1/tasks/{task}/duplicate".into(), None),
        (
            Method::POST,
            "/api/v1/tasks/{task}/comments".into(),
            Some(json!({"content":"A comment","author_name":"Member"})),
        ),
        (
            Method::POST,
            "/api/v1/tasks/{task}/dependencies".into(),
            Some(json!({"depends_on_id":f.task.id})),
        ),
        (
            Method::PUT,
            "/api/v1/tasks/{task}/roles/coder".into(),
            Some(json!({"assignee_type":"user","assignee_id":f.other_id})),
        ),
        (
            Method::POST,
            "/api/v1/tasks/{task}/workspace/reset".into(),
            None,
        ),
        (Method::POST, "/api/v1/tasks/{task}/archive".into(), None),
        (Method::DELETE, "/api/v1/tasks/{task}".into(), None),
        (
            Method::POST,
            "/api/v1/tasks/{task}/actions".into(),
            Some(json!({"action":{"verb":"cancel"},"version":f.task.version})),
        ),
        (
            Method::POST,
            "/api/v1/tasks/{task}/transition".into(),
            Some(json!({"status":"cancelled","version":f.task.version})),
        ),
    ];
    f.denied_writes(&writes).await;
    for (role, bearer) in [
        (Some("member"), f.other_token.clone()),
        (None, common::test_jwt()),
    ] {
        f.role(role).await;
        for (method, path, original_body) in &writes[..4] {
            let mut body = original_body.clone();
            if let Some(body) = body.as_mut().filter(|body| body.get("version").is_some()) {
                body["version"] = json!(
                    TaskRepo::get_by_id(&*f.harness.state.db, &f.task.id, false)
                        .await
                        .unwrap()
                        .unwrap()
                        .version
                );
            }
            let (status, response) = f.request(method.clone(), path, body, &bearer).await;
            assert!(
                status.is_success(),
                "authorized {method} {path}: {status} {response}"
            );
        }
    }
}

#[tokio::test]
async fn settings_hooks_environment_workflow_and_repos_require_admin() {
    let f = Fixture::new().await;
    let workflow: Value =
        serde_json::to_value(services::workflow::default_workflow::default_workflow()).unwrap();
    let writes = vec![
        (
            Method::PATCH,
            "/api/v1/projects/{project}".into(),
            Some(json!({"name":"Edited","project_hooks":[],"version":f.project.version})),
        ),
        (
            Method::POST,
            "/api/v1/projects/{project}/pause".into(),
            None,
        ),
        (
            Method::POST,
            "/api/v1/projects/{project}/resume".into(),
            None,
        ),
        (
            Method::POST,
            "/api/v1/projects/{project}/environment/recheck".into(),
            None,
        ),
        (
            Method::PUT,
            "/api/v1/projects/{project}/workflow".into(),
            Some(json!({"definition":workflow})),
        ),
        (
            Method::POST,
            "/api/v1/projects/{project}/hooks/test".into(),
            Some(json!({"task_id":f.task.id,"event":"before_work","hook_index":0})),
        ),
        (
            Method::POST,
            "/api/v1/projects/{project}/repos".into(),
            Some(json!({"name":"Second"})),
        ),
        (
            Method::PATCH,
            "/api/v1/repos/{repo}".into(),
            Some(json!({"name":"Edited"})),
        ),
        (Method::POST, "/api/v1/repos/{repo}/sync".into(), None),
        (
            Method::POST,
            "/api/v1/projects/{project}/members".into(),
            Some(json!({"user_id":f.other_id,"role":"member"})),
        ),
        (Method::DELETE, "/api/v1/projects/{project}".into(), None),
    ];
    f.denied_writes(&writes).await;
    f.role(Some("member")).await;
    for (method, path, body) in &writes {
        let (status, response) = f
            .request(method.clone(), path, body.clone(), &f.other_token)
            .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "member {method} {path}: {response}"
        );
    }
    for (role, bearer) in [
        (Some("admin"), f.other_token.clone()),
        (None, common::test_jwt()),
    ] {
        f.role(role).await;
        let (status, project) = f
            .request(Method::GET, "/api/v1/projects/{project}", None, &bearer)
            .await;
        assert_eq!(status, StatusCode::OK);
        let (status, response) = f
            .request(
                Method::PATCH,
                "/api/v1/projects/{project}",
                Some(json!({"name":"Authorized","project_hooks":[],"version":project["version"]})),
                &bearer,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "admin/owner settings: {response}");
        let (status, response) = f
            .request(
                Method::PATCH,
                "/api/v1/repos/{repo}",
                Some(json!({"name":"Authorized repo"})),
                &bearer,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "admin/owner repo: {response}");
    }
}

#[tokio::test]
async fn comments_are_scoped_even_when_addressed_by_comment_id() {
    let f = Fixture::new().await;
    let (_, comment) = f
        .request(
            Method::POST,
            "/api/v1/tasks/{task}/comments",
            Some(json!({"content":"Private","author_name":"Owner"})),
            &common::test_jwt(),
        )
        .await;
    let path = format!("/api/v1/comments/{}", comment["id"].as_str().unwrap());
    f.denied_writes(&[(Method::DELETE, path.clone(), None)])
        .await;
    f.role(Some("member")).await;
    assert_eq!(
        f.request(Method::DELETE, &path, None, &f.other_token)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn dependency_links_cannot_reference_invisible_projects() {
    let f = Fixture::new().await;
    let (_, other_project) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Other"})),
            &f.other_token,
        )
        .await;
    let project_id = other_project["id"].as_str().unwrap();
    let (_, other_task) = f
        .request(
            Method::POST,
            &format!("/api/v1/projects/{project_id}/tasks"),
            Some(json!({"title":"Other task"})),
            &f.other_token,
        )
        .await;
    let (status, body) = f
        .request(
            Method::POST,
            "/api/v1/tasks/{task}/dependencies",
            Some(json!({"depends_on_id":other_task["id"]})),
            &common::test_jwt(),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["details"]["entity"], "task");
}

#[tokio::test]
async fn conditional_task_lists_authorize_before_returning_not_modified() {
    let f = Fixture::new().await;
    let uri = f.path("/api/v1/projects/{project}/tasks");
    let response = f
        .harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", common::test_jwt()),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let etag = response.headers()[header::ETAG].clone();
    let response = f
        .harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", f.other_token))
                .header(header::IF_NONE_MATCH, etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn notifications_filter_before_pagination_totals_and_bulk_writes() {
    let f = Fixture::new().await;
    let (_, foreign) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Foreign"})),
            &f.other_token,
        )
        .await;
    for (id, project_id) in [
        ("visible-notice", f.project.id.as_str()),
        ("hidden-notice", foreign["id"].as_str().unwrap()),
    ] {
        db::NotificationRepo::create(
            &*f.harness.state.db,
            db::CreateNotification {
                id: id.into(),
                project_id: project_id.into(),
                task_id: None,
                event_type: "test".into(),
                title: id.into(),
                body: None,
                read: false,
                created_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    let (status, body) = f
        .request(
            Method::GET,
            "/api/v1/notifications?limit=1&include_total=true",
            None,
            &common::test_jwt(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_count"], 1);
    assert_eq!(body["items"][0]["id"], "visible-notice");
    assert_eq!(body["has_more"], false);
    assert_eq!(
        f.request(
            Method::PATCH,
            "/api/v1/notifications/hidden-notice/read",
            None,
            &common::test_jwt()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        f.request(
            Method::POST,
            "/api/v1/notifications/mark-all-read",
            None,
            &common::test_jwt()
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        !f.harness
            .state
            .db
            .get_notification("hidden-notice")
            .await
            .unwrap()
            .unwrap()
            .read
    );
    f.role(Some("viewer")).await;
    assert_eq!(
        f.request(
            Method::PATCH,
            "/api/v1/notifications/visible-notice/read",
            None,
            &f.other_token
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn execution_and_review_ids_resolve_their_owning_task() {
    let f = Fixture::new().await;
    let now = db::now_rfc3339();
    let execution_id = db::new_uuid_v4();
    db::ExecutionRepo::create(
        &*f.harness.state.db,
        db::CreateExecution {
            id: execution_id.clone(),
            task_id: f.task.id.clone(),
            agent_id: None,
            role: "reviewer".into(),
            status: db::ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let review_id = db::new_uuid_v4();
    db::ReviewRepo::create(
        &*f.harness.state.db,
        db::CreateReview {
            id: review_id.clone(),
            task_id: f.task.id.clone(),
            execution_id: execution_id.clone(),
            attempt_number: 1,
            status: db::ReviewStatus::Passed,
            step_results_json: "[]".into(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    f.reads(&[
        &format!("/api/v1/executions/{execution_id}"),
        &format!("/api/v1/executions/{execution_id}/logs"),
        &format!("/api/v1/executions/{execution_id}/hook-logs"),
        &format!("/api/v1/executions/{execution_id}/usage"),
        &format!("/api/v1/reviews/{review_id}"),
    ])
    .await;
    f.denied_writes(&[
        (
            Method::POST,
            format!("/api/v1/executions/{execution_id}/stop"),
            Some(json!({"reason":"Stop"})),
        ),
        (
            Method::POST,
            format!("/api/v1/executions/{execution_id}/follow-up"),
            Some(json!({"message":"Continue"})),
        ),
    ])
    .await;
}

#[tokio::test]
async fn workspace_and_diff_ids_resolve_their_owning_project() {
    let f = Fixture::new().await;
    let now = db::now_rfc3339();
    let workspace_id = db::new_uuid_v4();
    db::WorkspaceRepo::create(
        &*f.harness.state.db,
        db::CreateWorkspace {
            id: workspace_id.clone(),
            task_id: f.task.id.clone(),
            repo_id: f.repo.id.clone(),
            worktree_path: f
                ._root
                .path()
                .join("missing-worktree")
                .to_string_lossy()
                .into_owned(),
            branch: "task-authz".into(),
            status: db::WorkspaceStatus::Error,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    f.reads(&["/api/v1/tasks/{task}/workspace"]).await;
    f.denied_writes(&[(
        Method::POST,
        "/api/v1/tasks/{task}/workspace/reset".into(),
        None,
    )])
    .await;
    for path in [
        "/api/v1/tasks/{task}/diff".to_owned(),
        format!("/api/v1/workspaces/{workspace_id}/diff"),
    ] {
        f.role(None).await;
        assert_eq!(
            f.request(Method::GET, &path, None, &f.other_token).await.0,
            StatusCode::NOT_FOUND
        );
        f.role(Some("viewer")).await;
        let viewer = f.request(Method::GET, &path, None, &f.other_token).await;
        let owner = f
            .request(Method::GET, &path, None, &common::test_jwt())
            .await;
        assert_eq!(
            viewer.0, owner.0,
            "viewer reaches the same scoped diff service: {}",
            viewer.1
        );
        assert_eq!(viewer.1["code"], owner.1["code"]);
        assert_ne!(viewer.0, StatusCode::FORBIDDEN);
    }
}

async fn upload_media(f: &Fixture, bearer: &str) -> (StatusCode, Value) {
    let response = f.harness.app.clone().oneshot(Request::builder().method(Method::POST).uri(f.path("/api/v1/tasks/{task}/media")).header(header::AUTHORIZATION,format!("Bearer {bearer}")).header(header::CONTENT_TYPE,"multipart/form-data; boundary=authz-test").body(Body::from("--authz-test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"note.txt\"\r\nContent-Type: text/plain\r\n\r\nPrivate attachment\r\n--authz-test--\r\n")).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn media_upload_read_and_delete_apply_the_existing_role_rules() {
    let f = Fixture::new().await;
    assert_eq!(
        upload_media(&f, &f.other_token).await.0,
        StatusCode::NOT_FOUND
    );
    f.role(Some("viewer")).await;
    assert_eq!(
        upload_media(&f, &f.other_token).await.0,
        StatusCode::FORBIDDEN
    );
    f.role(Some("member")).await;
    let (status, media) = upload_media(&f, &f.other_token).await;
    assert_eq!(status, StatusCode::CREATED, "{media}");
    let path = format!("/api/v1/media/{}", media["id"].as_str().unwrap());
    assert_eq!(
        f.request(Method::DELETE, &path, None, &f.other_token)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    f.role(None).await;
    assert_eq!(
        f.request(Method::GET, &path, None, &f.other_token).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        f.request(Method::DELETE, &path, None, &f.other_token)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    f.role(Some("viewer")).await;
    assert_eq!(
        f.request(Method::GET, &path, None, &f.other_token).await.0,
        StatusCode::OK
    );
    assert_eq!(
        upload_media(&f, &common::test_jwt()).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        f.request(Method::DELETE, &path, None, &common::test_jwt())
            .await
            .0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn project_chat_reads_and_messages_check_membership_and_write_role() {
    let f = Fixture::new().await;
    let chat = f
        .harness
        .state
        .agent_chat_service
        .ensure_project_chat(&f.project.id)
        .await
        .unwrap();
    let path = format!("/api/v1/agent-chats/{}", chat.id);
    f.reads(&[&path, &format!("{path}/messages"), &format!("{path}/turns")])
        .await;
    f.denied_writes(&[(
        Method::POST,
        format!("{path}/messages"),
        Some(json!({"content":"A message"})),
    )])
    .await;
}

#[tokio::test]
async fn hidden_ids_have_the_same_response_as_unknown_ids() {
    let f = Fixture::new().await;
    for (actual, missing) in [
        (
            f.path("/api/v1/tasks/{task}"),
            "/api/v1/tasks/missing".to_owned(),
        ),
        (
            f.path("/api/v1/repos/{repo}"),
            "/api/v1/repos/missing".to_owned(),
        ),
    ] {
        let known = f.request(Method::GET, &actual, None, &f.other_token).await;
        let unknown = f.request(Method::GET, &missing, None, &f.other_token).await;
        assert_eq!(known.0, unknown.0);
        assert_eq!(known.1["code"], unknown.1["code"]);
        assert_eq!(known.1["details"]["entity"], unknown.1["details"]["entity"]);
        assert!(!known.1.to_string().contains(&f.project.id));
    }
}

#[tokio::test]
async fn explicit_viewer_role_is_read_only_even_for_the_recorded_creator() {
    let f = Fixture::new().await;
    ProjectMemberRepo::update_member_role(
        &*f.harness.state.db,
        &f.project.id,
        "test-user-id",
        "viewer",
        &db::now_rfc3339(),
    )
    .await
    .unwrap();
    assert_eq!(
        f.request(
            Method::GET,
            "/api/v1/projects/{project}",
            None,
            &common::test_jwt()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        f.request(
            Method::PATCH,
            "/api/v1/projects/{project}",
            Some(json!({"name":"Denied","version":f.project.version})),
            &common::test_jwt()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.request(
            Method::POST,
            "/api/v1/projects/{project}/tasks",
            Some(json!({"title":"Denied"})),
            &common::test_jwt()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}

/// A legacy ownerless Project is visible to every signed-in user, and each of
/// them keeps owner rights on it: Task writes and Project settings alike.
#[tokio::test]
async fn ownerless_project_keeps_owner_rights_for_every_signed_in_user() {
    let f = Fixture::new().await;
    let now = db::now_rfc3339();
    let project_id = db::new_uuid_v4();
    db::ProjectRepo::create(
        &*f.harness.state.db,
        db::CreateProject {
            id: project_id.clone(),
            name: "Legacy".into(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    for bearer in [f.other_token.clone(), common::test_jwt()] {
        let (status, task) = f
            .request(
                Method::POST,
                &format!("/api/v1/projects/{project_id}/tasks"),
                Some(json!({"title":"Legacy task"})),
                &bearer,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "task create: {task}");
        let (status, response) = f
            .request(
                Method::PATCH,
                &format!("/api/v1/tasks/{}", task["id"].as_str().unwrap()),
                Some(json!({"title":"Edited","version":task["version"]})),
                &bearer,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "task edit: {response}");
        let project = db::ProjectRepo::get_by_id(&*f.harness.state.db, &project_id)
            .await
            .unwrap()
            .unwrap();
        let (status, response) = f
            .request(
                Method::PATCH,
                &format!("/api/v1/projects/{project_id}"),
                Some(json!({"name":"Legacy edited","version":project.version})),
                &bearer,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "project settings: {response}");
    }
}

/// The hook-test body names a second Task id. A Task of a Project the caller
/// cannot see must read exactly like an unknown Task (no 400/404 oracle).
#[tokio::test]
async fn hook_test_hides_a_foreign_task_behind_the_unknown_task_404() {
    let f = Fixture::new().await;
    let (status, foreign_project) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Foreign"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_project}");
    let (status, foreign_task) = f
        .request(
            Method::POST,
            &format!(
                "/api/v1/projects/{}/tasks",
                foreign_project["id"].as_str().unwrap()
            ),
            Some(json!({"title":"Foreign task"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_task}");
    let owner = common::test_jwt();
    let hook_test = |task_id: String| {
        f.request(
            Method::POST,
            "/api/v1/projects/{project}/hooks/test",
            Some(json!({"task_id":task_id,"event":"before_work","hook_index":0})),
            &owner,
        )
    };
    let foreign = hook_test(foreign_task["id"].as_str().unwrap().to_owned()).await;
    let unknown = hook_test("missing-task".to_owned()).await;
    assert_eq!(foreign.0, StatusCode::NOT_FOUND, "{}", foreign.1);
    assert_eq!(foreign.0, unknown.0);
    assert_eq!(foreign.1["code"], unknown.1["code"]);
    assert_eq!(
        foreign.1["details"]["entity"],
        unknown.1["details"]["entity"]
    );
}

/// A board move names neighbor Task ids. A neighbor in a Project the caller
/// cannot see must get the same answer as an unknown id.
#[tokio::test]
async fn task_move_does_not_confirm_a_foreign_neighbor_id() {
    let f = Fixture::new().await;
    let (status, foreign_project) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Foreign"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_project}");
    let (status, foreign_task) = f
        .request(
            Method::POST,
            &format!(
                "/api/v1/projects/{}/tasks",
                foreign_project["id"].as_str().unwrap()
            ),
            Some(json!({"title":"Foreign task"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_task}");
    let owner = common::test_jwt();
    let (status, board) = f
        .request(
            Method::GET,
            "/api/v1/projects/{project}/tasks",
            None,
            &owner,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{board}");
    let mv = |neighbor: String| {
        f.request(
            Method::POST,
            "/api/v1/tasks/{task}/move",
            Some(json!({
                "operation_id": db::new_uuid_v4(),
                "task_version": f.task.version,
                "board_revision": board["board_revision"],
                "target_status": f.task.status,
                "before_id": neighbor,
            })),
            &owner,
        )
    };
    let foreign_id = foreign_task["id"].as_str().unwrap().to_owned();
    let foreign = mv(foreign_id.clone()).await;
    let unknown = mv("missing-task".to_owned()).await;
    assert!(
        unknown.1["message"]
            .as_str()
            .is_some_and(|message| message.contains("neighbor task")),
        "the move reaches the neighbor check: {}",
        unknown.1
    );
    assert_eq!(foreign.0, unknown.0, "{} vs {}", foreign.1, unknown.1);
    assert_eq!(foreign.1["code"], unknown.1["code"]);
    assert_eq!(
        foreign.1["message"]
            .as_str()
            .unwrap()
            .replace(&foreign_id, "ID"),
        unknown.1["message"]
            .as_str()
            .unwrap()
            .replace("missing-task", "ID")
    );
}

async fn sse_stream(f: &Fixture, bearer: &str) -> axum::body::BodyDataStream {
    let response = f
        .harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/events?token={bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().into_data_stream()
}

/// Read SSE frames until the unscoped marker every subscriber receives.
async fn sse_until(stream: &mut axum::body::BodyDataStream, marker: &str) -> String {
    use futures_util::StreamExt;
    let mut seen = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !seen.contains(marker) {
            let bytes = stream.next().await.expect("stream open").expect("frame");
            seen.push_str(std::str::from_utf8(&bytes).unwrap());
        }
    })
    .await
    .expect("marker frame arrives");
    seen
}

fn publish_project_events(f: &Fixture, marker: &str) {
    let bus = &f.harness.state.event_bus;
    let event = |event_type: &str, entity_id: &str, context| events::ForgeEvent {
        event_type: event_type.into(),
        entity_id: entity_id.into(),
        timestamp: events::event_timestamp(),
        context,
    };
    bus.publish(event(
        "task.updated",
        &f.task.id,
        events::EventContext::TaskUpdated {
            project_id: f.project.id.clone(),
        },
    ));
    bus.publish(event(
        "execution.log",
        "private-execution-log",
        events::EventContext::ExecutionLog {
            task_id: f.task.id.clone(),
            log: json!({"line":"private-log-line"}),
            logs: None,
        },
    ));
    bus.publish(event(
        "domain_event.committed",
        "private-domain-event",
        events::EventContext::DomainEventCommitted {
            sequence: 0,
            domain_event_type: "project.updated".into(),
            domain_entity_id: f.project.id.clone(),
            entity_type: "project".into(),
            scope_type: "project".into(),
            scope_id: f.project.id.clone(),
        },
    ));
    bus.publish(event(
        "project.updated",
        &f.project.id,
        events::EventContext::ProjectUpdated {},
    ));
    bus.publish(event(
        "operations.status_changed",
        marker,
        events::EventContext::OperationsStatusChanged {
            trigger: "test".into(),
        },
    ));
}

/// The SSE stream delivers a Project's events only to subscribers who can
/// see that Project; instance-level events still reach everyone.
#[tokio::test]
async fn sse_delivers_project_events_only_to_subscribers_who_can_see_the_project() {
    let f = Fixture::new().await;
    let mut owner = sse_stream(&f, &common::test_jwt()).await;
    let mut stranger = sse_stream(&f, &f.other_token).await;
    publish_project_events(&f, "marker-one");
    let owner_frames = sse_until(&mut owner, "marker-one").await;
    let stranger_frames = sse_until(&mut stranger, "marker-one").await;
    for private in [
        f.task.id.as_str(),
        "private-log-line",
        "private-domain-event",
        f.project.id.as_str(),
    ] {
        assert!(owner_frames.contains(private), "owner misses {private}");
        assert!(
            !stranger_frames.contains(private),
            "non-member receives {private}: {stranger_frames}"
        );
    }

    // A new membership is honoured by a new connection immediately.
    f.role(Some("viewer")).await;
    let mut viewer = sse_stream(&f, &f.other_token).await;
    publish_project_events(&f, "marker-two");
    let viewer_frames = sse_until(&mut viewer, "marker-two").await;
    assert!(
        viewer_frames.contains("private-log-line"),
        "{viewer_frames}"
    );
}

/// A handoff addressed through a visible Project but targeting another
/// account's Chat must read exactly like an unknown handoff id.
#[tokio::test]
async fn handoff_into_a_hidden_chat_reads_like_an_unknown_handoff() {
    let f = Fixture::new().await;
    let (status, foreign_project) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Foreign"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_project}");
    let pool = f.harness.state.db.pool();
    let chat_of = |project_id: String| async move {
        sqlx::query_scalar::<_, String>("SELECT id FROM agent_chat WHERE project_id = ?")
            .bind(project_id)
            .fetch_one(pool)
            .await
            .expect("Project Chat exists")
    };
    let source = chat_of(f.project.id.clone()).await;
    let target = chat_of(foreign_project["id"].as_str().unwrap().to_owned()).await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO agent_handoff (id, source_chat_id, target_chat_id, content, correlation_id, dedupe_key, created_at, updated_at) VALUES ('hidden-handoff', ?, ?, 'Private brief', 'authz', 'authz-handoff', ?, ?)")
        .bind(&source)
        .bind(&target)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
    let owner = common::test_jwt();
    let foreign = f
        .request(
            Method::GET,
            "/api/v1/projects/{project}/agent-handoffs/hidden-handoff",
            None,
            &owner,
        )
        .await;
    let unknown = f
        .request(
            Method::GET,
            "/api/v1/projects/{project}/agent-handoffs/missing-handoff",
            None,
            &owner,
        )
        .await;
    assert_eq!(foreign.0, StatusCode::NOT_FOUND, "{}", foreign.1);
    assert_eq!(foreign.0, unknown.0);
    assert_eq!(foreign.1["code"], unknown.1["code"]);
    assert_eq!(
        foreign.1["details"]["entity"],
        unknown.1["details"]["entity"]
    );
}

/// Two reads keep their existing, stricter contracts: Project escalations are
/// owner-only and repository locations owner/admin-only. A caller who cannot
/// see the Project gets the unknown-id 404; a member who can gets the
/// existing 403; the owner reads.
#[tokio::test]
async fn owner_only_escalation_and_admin_only_location_reads_hide_the_project() {
    let f = Fixture::new().await;
    let reads = [
        (
            "/api/v1/projects/{project}/escalations",
            "/api/v1/projects/missing/escalations",
        ),
        (
            "/api/v1/projects/{project}/escalations/missing-escalation",
            "/api/v1/projects/missing/escalations/missing-escalation",
        ),
        (
            "/api/v1/repos/{repo}/locations",
            "/api/v1/repos/missing/locations",
        ),
    ];
    for (path, unknown_path) in reads {
        let hidden = f.request(Method::GET, path, None, &f.other_token).await;
        let unknown = f
            .request(Method::GET, unknown_path, None, &f.other_token)
            .await;
        assert_eq!(hidden.0, StatusCode::NOT_FOUND, "{path}: {}", hidden.1);
        assert_eq!(hidden.1["code"], unknown.1["code"], "{path}");
        assert_eq!(
            hidden.1["details"]["entity"], unknown.1["details"]["entity"],
            "{path}"
        );
        for role in ["viewer", "member"] {
            f.role(Some(role)).await;
            let (status, body) = f.request(Method::GET, path, None, &f.other_token).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{role} {path}: {body}");
        }
        f.role(None).await;
    }
    for (path, expected) in [
        ("/api/v1/projects/{project}/escalations", StatusCode::OK),
        (
            "/api/v1/projects/{project}/escalations/missing-escalation",
            StatusCode::NOT_FOUND,
        ),
        ("/api/v1/repos/{repo}/locations", StatusCode::OK),
    ] {
        let (status, body) = f
            .request(Method::GET, path, None, &common::test_jwt())
            .await;
        assert_eq!(status, expected, "owner {path}: {body}");
    }
}

/// A Project Chat of a Project the caller cannot see reads like an unknown
/// Chat id and never names its Project.
#[tokio::test]
async fn hidden_project_chat_reads_like_an_unknown_chat() {
    let f = Fixture::new().await;
    let chat_id: String = sqlx::query_scalar("SELECT id FROM agent_chat WHERE project_id = ?")
        .bind(&f.project.id)
        .fetch_one(f.harness.state.db.pool())
        .await
        .expect("Project Chat exists");
    for path in [
        "/api/v1/agent-chats/{chat}",
        "/api/v1/agent-chats/{chat}/messages",
    ] {
        let hidden = f
            .request(
                Method::GET,
                &path.replace("{chat}", &chat_id),
                None,
                &f.other_token,
            )
            .await;
        let unknown = f
            .request(
                Method::GET,
                &path.replace("{chat}", "missing-chat"),
                None,
                &f.other_token,
            )
            .await;
        assert_eq!(hidden.0, StatusCode::NOT_FOUND, "{path}: {}", hidden.1);
        assert_eq!(hidden.1["code"], unknown.1["code"], "{path}");
        assert_eq!(
            hidden.1["details"]["entity"], unknown.1["details"]["entity"],
            "{path}: {}",
            hidden.1
        );
        assert!(
            !hidden.1.to_string().contains(&f.project.id),
            "{}",
            hidden.1
        );
    }
}

/// Mission Control writes address an Attention item by id. An item of a
/// Project the caller cannot see must read like an unknown item and never
/// name its Project.
#[tokio::test]
async fn attention_of_a_hidden_project_reads_like_an_unknown_item() {
    use db::{AgentRepo, DomainEventRepo};
    let f = Fixture::new().await;
    let db = &*f.harness.state.db;
    let now = db::now_rfc3339();
    let identity = AgentRepo::create(
        db,
        db::CreateAgent {
            id: db::new_uuid_v4(),
            name: "Attention identity".into(),
            description: None,
            executor_type: "codex".into(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".into(),
            config_json: "{}".into(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("test-user-id".into()),
            visibility: "account".into(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let source = db
        .append_event(db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "validation.failed".into(),
            entity_type: "task".into(),
            entity_id: f.task.id.clone(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "project".into(),
            scope_id: f.project.id.clone(),
            correlation_id: "authz-attention".into(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("authz-attention".into()),
            payload_json: "{}".into(),
            created_at: now.clone(),
        })
        .await
        .unwrap();
    sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, identity_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at) VALUES ('hidden-attention', 'validation_failed', 'project', ?, ?, ?, 1, 'open', 'Needs work', '{}', 'authz-attention', ?, ?)")
        .bind(&f.project.id)
        .bind(&identity.id)
        .bind(&source.id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    for action in ["acknowledge", "resolve"] {
        let hidden = f
            .request(
                Method::POST,
                &format!("/api/v1/mission-control/attention/hidden-attention/{action}"),
                Some(json!({"expected_version":1})),
                &f.other_token,
            )
            .await;
        let unknown = f
            .request(
                Method::POST,
                &format!("/api/v1/mission-control/attention/missing-attention/{action}"),
                Some(json!({"expected_version":1})),
                &f.other_token,
            )
            .await;
        assert_eq!(hidden.0, StatusCode::NOT_FOUND, "{action}: {}", hidden.1);
        assert_eq!(hidden.0, unknown.0, "{action}");
        assert_eq!(hidden.1["code"], unknown.1["code"], "{action}");
        assert_eq!(
            hidden.1["details"]["entity"], unknown.1["details"]["entity"],
            "{action}: {}",
            hidden.1
        );
        assert!(
            !hidden.1.to_string().contains(&f.project.id),
            "{}",
            hidden.1
        );
    }
}

/// A Project's primary repository is chosen by id. A repository of a Project
/// the caller cannot see must read like an unknown repository and never name
/// its Project.
#[tokio::test]
async fn primary_repo_of_a_hidden_project_reads_like_an_unknown_repo() {
    let f = Fixture::new().await;
    let (status, foreign_project) = f
        .request(
            Method::POST,
            "/api/v1/projects",
            Some(json!({"name":"Foreign"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_project}");
    let foreign_project_id = foreign_project["id"].as_str().unwrap().to_owned();
    let (status, foreign_repo) = f
        .request(
            Method::POST,
            &format!("/api/v1/projects/{foreign_project_id}/repos"),
            Some(json!({"name":"foreign","remote_url":"https://example.invalid/foreign.git"})),
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{foreign_repo}");
    let owner = common::test_jwt();
    let patch = |repo_id: String| {
        f.request(
            Method::PATCH,
            "/api/v1/projects/{project}",
            Some(json!({"primary_repo_id":repo_id,"version":f.project.version})),
            &owner,
        )
    };
    let hidden = patch(foreign_repo["id"].as_str().unwrap().to_owned()).await;
    let unknown = patch("missing-repo".to_owned()).await;
    assert_eq!(hidden.0, StatusCode::NOT_FOUND, "{}", hidden.1);
    assert_eq!(hidden.0, unknown.0);
    assert_eq!(
        hidden.1["details"]["entity"],
        unknown.1["details"]["entity"]
    );
    assert!(
        !hidden.1.to_string().contains(&foreign_project_id),
        "{}",
        hidden.1
    );
}

#[tokio::test]
async fn findings_read_uses_reviews_authority_and_resets_with_the_budget_window() {
    use db::{CreateReview, ReviewRepo, ReviewStatus, TransitionLogRepo};
    let f = Fixture::new().await;
    let db = &*f.harness.state.db;
    let now = db::now_rfc3339();
    let candidate = db::new_uuid_v4();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','completed',?,?)")
        .bind(&candidate).bind(&f.task.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let first = json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":"problems","assessment":{"result":"fail","reason":"problems","findings":[{"summary":"one"},{"summary":"two"}]}}});
    let second = json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":"problems","assessment":{"result":"fail","reason":"problems","findings":[{"id":"F1","summary":"still one"},{"summary":"three","severity":"minor"}],"resolved":["F2"]}}});
    for (index, details) in [first, second].into_iter().enumerate() {
        ReviewRepo::create(
            db,
            CreateReview {
                id: db::new_uuid_v4(),
                task_id: f.task.id.clone(),
                execution_id: candidate.clone(),
                attempt_number: index as i64 + 1,
                status: ReviewStatus::Failed,
                step_results_json: details.to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
    }
    let reviews_refusal = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/reviews",
            None,
            &f.other_token,
        )
        .await;
    let findings_refusal = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(reviews_refusal.0, StatusCode::NOT_FOUND);
    // Correlation ids belong to individual requests, not the access refusal.
    let (reviews_status, mut reviews_body) = reviews_refusal;
    let (findings_status, mut findings_body) = findings_refusal;
    reviews_body.as_object_mut().unwrap().remove("request_id");
    findings_body.as_object_mut().unwrap().remove("request_id");
    assert_eq!(
        (findings_status, findings_body),
        (reviews_status, reviews_body)
    );
    f.role(Some("viewer")).await;
    let (status, body) = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let response: api_types::TaskFindingsResponse = serde_json::from_value(body).unwrap();
    assert_eq!(response.review_attempts, 2);
    assert_eq!(response.window_started_at, f.task.created_at);
    assert_eq!(
        response
            .items
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        ["F1", "F2", "F3"]
    );
    assert_eq!(response.items[0].streak, 2);
    assert_eq!(
        response.items[1].status,
        api_types::ReviewFindingStatus::Resolved
    );
    assert_eq!(
        response.items[2].severity,
        api_types::ReviewFindingSeverity::Minor
    );

    // A typed recovery reset, even from a state other than review, changes the
    // accounting window. It is not inferred from review failures or status.
    let boundary = "2030-01-01T00:00:00Z";
    TransitionLogRepo::insert(
        db,
        db::CreateTransitionLog {
            id: db::new_uuid_v4(),
            task_id: f.task.id.clone(),
            from_state: f.task.status.clone(),
            to_state: f.task.status.clone(),
            trigger_name: Some("reset_retry_window".into()),
            triggered_by: "test".into(),
            bridge: api_types::TransitionBridge::recovery("reset_retry_window", true),
            trigger_reason: "new budget".into(),
            hook_results_json: None,
            rejection: false,
            created_at: boundary.into(),
        },
    )
    .await
    .unwrap();
    let (_, empty) = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["review_attempts"], 0);
    assert_eq!(empty["window_started_at"], boundary);
    ReviewRepo::create(db, CreateReview {
        id: db::new_uuid_v4(), task_id: f.task.id.clone(), execution_id: candidate,
        attempt_number: 3, status: ReviewStatus::Failed,
        step_results_json: json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"new","findings":[{"id":"F3","summary":"new window"}]}}}).to_string(),
        started_at: "2030-01-01T00:00:01Z".into(), created_at: "2030-01-01T00:00:01Z".into(), updated_at: now,
    }).await.unwrap();
    let (_, body) = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(body["review_attempts"], 1);
    assert_eq!(body["items"][0]["id"], "F1");
    assert_eq!(body["items"][0]["first_attempt"], 3);
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn findings_read_caps_the_full_ledger_after_id_ordering() {
    let f = Fixture::new().await;
    let db = &*f.harness.state.db;
    f.role(Some("viewer")).await;
    let now = db::now_rfc3339();
    let candidate = db::new_uuid_v4();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','completed',?,?)")
        .bind(&candidate).bind(&f.task.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    for attempt in 1..=5 {
        db::ReviewRepo::create(db,db::CreateReview {
            id: db::new_uuid_v4(),task_id:f.task.id.clone(),execution_id:candidate.clone(),
            attempt_number:attempt,status:db::ReviewStatus::Failed,
            step_results_json:json!({"conformance":{"status":"failed","contract":null,"checks":[],"reason":null,"assessment":{"result":"fail","reason":"many","findings":(0..50).map(|n|json!({"summary":format!("{attempt}-{n}")})).collect::<Vec<_>>()}}}).to_string(),
            started_at:now.clone(),created_at:now.clone(),updated_at:now.clone(),
        }).await.unwrap();
    }
    let (status, body) = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let response: api_types::TaskFindingsResponse = serde_json::from_value(body).unwrap();
    assert_eq!(response.review_attempts, 5);
    assert_eq!(response.items.len(), 200);
    // Natural id order (F2 before F10) precedes the cap: F1..F200 survive.
    let ids = (1..=200).map(|n| format!("F{n}")).collect::<Vec<_>>();
    assert_eq!(
        response
            .items
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
}

#[tokio::test]
async fn findings_read_reports_replaced_step_as_superseded() {
    let f = Fixture::new().await;
    let db = &*f.harness.state.db;
    f.role(Some("viewer")).await;
    let now = db::now_rfc3339();
    let candidate = db::new_uuid_v4();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'coder','completed',?,?)")
        .bind(&candidate).bind(&f.task.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    for (attempt, signatures) in [(1, json!([])), (2, json!(["binary::test"]))] {
        db::ReviewRepo::create(db, db::CreateReview {
            id:db::new_uuid_v4(),task_id:f.task.id.clone(),execution_id:candidate.clone(),attempt_number:attempt,status:db::ReviewStatus::Failed,
            step_results_json:json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":101,"stderr_tail":"failed","failures":signatures}]}).to_string(),
            started_at:now.clone(),created_at:now.clone(),updated_at:now.clone(),
        }).await.unwrap();
    }
    let (status, body) = f
        .request(
            Method::GET,
            "/api/v1/tasks/{task}/findings",
            None,
            &f.other_token,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let step = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == "CI-0")
        .unwrap();
    assert_eq!(step["status"], "superseded");
    assert_eq!(step["streak"], 0);
}
