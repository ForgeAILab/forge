//! V107 regression coverage for Project teardown across the media graph.
//!
//! `task_media.asset_id` and `media_asset.legacy_task_media_id` point at each
//! other with ON DELETE SET NULL, and both were guarded by triggers that
//! aborted on any change at all. Every Project that had ever carried one task
//! attachment was therefore undeletable: whichever side the cascade reached
//! first, the foreign key wrote its NULL and the guard aborted the whole
//! transaction. `project_media_attachment` holds the asset with RESTRICT on top
//! of that, so it has to be cleared before the assets go.

use db::{
    create_sqlite_pool, new_uuid_v4, run_migrations, CommentAuthorType, CreateProject, CreateTask,
    CreateTaskMedia, ProjectRepo, SqliteDb, TaskMediaRepo, TaskRepo, User, UserRepo,
};

const NOW: &str = "2026-08-27T00:00:00.000Z";
const ACCOUNT_ID: &str = "media-teardown-account";

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

/// Build a Project holding one task attachment — the shape that the insert
/// triggers expand into a `media_asset` plus a `project_media_attachment`.
async fn project_with_task_media(db: &SqliteDb) -> String {
    UserRepo::create_user(
        db,
        &User {
            id: ACCOUNT_ID.to_owned(),
            email: "media-teardown@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("account creates");

    let project = ProjectRepo::create(
        db,
        CreateProject {
            id: new_uuid_v4(),
            name: "media teardown".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("project creates");

    let task = TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "task with a screenshot".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 100,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .expect("task creates");

    TaskMediaRepo::create_media(
        db,
        CreateTaskMedia {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            display_filename: "screenshot.png".to_owned(),
            content_type: "image/png".to_owned(),
            byte_size: 1024,
            storage_key: format!("media/{}/screenshot.png", task.id),
            author_type: CommentAuthorType::User,
            author_id: Some(ACCOUNT_ID.to_owned()),
            author_name: "E2E".to_owned(),
            created_at: NOW.to_owned(),
        },
    )
    .await
    .expect("task media creates");

    project.id
}

#[tokio::test]
async fn project_with_task_media_deletes() {
    let db = database().await;
    let project_id = project_with_task_media(&db).await;

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project with task media deletes");

    assert!(
        ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("lookup succeeds")
            .is_none(),
        "the Project is gone once its media graph has been torn down",
    );
}

#[tokio::test]
async fn asset_mapping_still_refuses_a_rewrite() {
    let db = database().await;
    let project_id = project_with_task_media(&db).await;

    let other_asset = new_uuid_v4();
    let error = sqlx::query("UPDATE task_media SET asset_id = ? WHERE asset_id IS NOT NULL")
        .bind(&other_asset)
        .execute(db.pool())
        .await
        .expect_err("re-pointing a task media asset stays forbidden");

    assert!(
        error
            .to_string()
            .contains("Task media asset mapping is immutable"),
        "unexpected error: {error}",
    );

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("cleanup");
}
