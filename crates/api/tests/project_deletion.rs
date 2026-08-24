mod common;

use api_types::ProjectResponse;
use axum::http::{Method, StatusCode};
use db::{new_uuid_v4, now_rfc3339, CreateRepo, RepoRepo, WorkMode};
use serde_json::json;

#[tokio::test]
async fn project_delete_removes_managed_repo_but_preserves_linked_repo() {
    let workspace = common::TestDir::new("project-delete-managed-repo");
    let harness = common::test_app(workspace.path(), "project-delete-managed-repo").await;
    let token = common::test_jwt();
    let project: ProjectResponse = common::json_request_with_bearer(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        &token,
        json!({"name": "Delete managed repository"}),
        StatusCode::OK,
    )
    .await;

    let managed_repo = workspace.path().join("repos").join("managed-repo");
    let linked_repo = workspace.path().join("linked-repo");
    std::fs::create_dir_all(&managed_repo).expect("managed repository directory");
    std::fs::create_dir_all(&linked_repo).expect("linked repository directory");
    std::fs::write(managed_repo.join("tracked"), "managed").expect("managed content");
    std::fs::write(linked_repo.join("tracked"), "linked").expect("linked content");

    let now = now_rfc3339();
    for (name, path) in [
        ("managed", managed_repo.as_path()),
        ("linked", linked_repo.as_path()),
    ] {
        RepoRepo::create(
            &*harness.state.db,
            CreateRepo {
                id: new_uuid_v4(),
                project_id: project.id.clone(),
                name: name.to_owned(),
                remote_url: path.to_string_lossy().into_owned(),
                local_path: Some(path.to_string_lossy().into_owned()),
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repository record");
    }

    let response = common::raw_empty_request(
        &harness.app,
        Method::DELETE,
        &format!("/api/v1/projects/{}", project.id),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(!managed_repo.exists());
    assert!(linked_repo.join("tracked").is_file());
}
