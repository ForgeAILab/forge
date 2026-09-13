use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateProject, CreateRepo,
    CreateTask, CreateTaskRoleAssignment, DbError, ProjectRepo, RepoRepo, SqliteDb,
    TaskMetadataMutation, TaskRepo, TaskRoleAssignmentRepo, UpdateProject, UpdateRepo, WorkMode,
};
use serde_json::json;

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn task(db: &SqliteDb, project_id: &str, task_id: &str, updated_at: &str) {
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.to_owned(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "metadata task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: updated_at.to_owned(),
            updated_at: updated_at.to_owned(),
        },
    )
    .await
    .expect("task creates");
}

async fn project(db: &SqliteDb, project_id: &str, updated_at: &str) {
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.to_owned(),
            name: "metadata project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: updated_at.to_owned(),
            updated_at: updated_at.to_owned(),
        },
    )
    .await
    .expect("project creates");
}

async fn seed_dispatch_markers(db: &SqliteDb, task_id: &str) {
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"dispatch_disposition":{"capability":"worker"},"deferred_dispatch":{"target_state":"todo"},"custom":"kept"}"#)
        .bind(task_id)
        .execute(db.pool())
        .await
        .expect("dispatch markers seed");
}

#[tokio::test]
async fn key_mutations_preserve_independent_markers_from_a_stale_snapshot() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;

    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"custom":"kept"}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("metadata seeds");
    let stale = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::Set {
            key: "dispatch_disposition".to_owned(),
            value: json!({"capability":"worker"}),
        }],
        &now_rfc3339(),
    )
    .await
    .expect("first marker writes");
    TaskRepo::mutate_metadata(
        &db,
        &stale.id,
        None,
        vec![TaskMetadataMutation::Set {
            key: "deferred_dispatch".to_owned(),
            value: json!({"target_state":"in_progress"}),
        }],
        &now_rfc3339(),
    )
    .await
    .expect("second marker writes from stale snapshot");

    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().expect("metadata exists"))
            .expect("metadata parses");
    assert_eq!(metadata["custom"], "kept");
    assert_eq!(metadata["dispatch_disposition"]["capability"], "worker");
    assert_eq!(metadata["deferred_dispatch"]["target_state"], "in_progress");

    let observed_deferred = metadata["deferred_dispatch"].clone();
    TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::Set {
            key: "deferred_dispatch".to_owned(),
            value: json!({"target_state":"review"}),
        }],
        &now_rfc3339(),
    )
    .await
    .expect("newer marker writes");
    TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::RemoveIf {
            key: "deferred_dispatch".to_owned(),
            expected: observed_deferred,
        }],
        &now_rfc3339(),
    )
    .await
    .expect("stale clear commits as a no-op");
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after conditional clear")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().expect("metadata exists"))
            .expect("metadata parses");
    assert_eq!(metadata["deferred_dispatch"]["target_state"], "review");

    let before_noop = current;
    let no_op = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::RemoveIf {
            key: "deferred_dispatch".to_owned(),
            expected: json!({"target_state":"stale"}),
        }],
        "2099-01-01T00:00:00Z",
    )
    .await
    .expect("conditional miss is a no-op");
    assert_eq!(no_op.updated_at, before_noop.updated_at);
    assert_eq!(no_op.metadata_json, before_noop.metadata_json);

    TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::SetIf {
            key: "deferred_dispatch".to_owned(),
            expected: json!({"target_state":"in_progress"}),
            value: json!({"target_state":"todo"}),
        }],
        &now_rfc3339(),
    )
    .await
    .expect("stale refresh commits as a no-op");
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after conditional refresh")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().expect("metadata exists"))
            .expect("metadata parses");
    assert_eq!(metadata["deferred_dispatch"]["target_state"], "review");
}

#[tokio::test]
async fn project_wake_clears_dispatch_markers_atomically_but_keeps_pause_marker() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(
            r#"{"custom":"kept","dispatch_disposition":{"capability":"worker"},"deferred_dispatch":{"target_state":"todo"},"paused_integration":{"state":"review"}}"#,
        )
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("metadata seeds");

    let stale_snapshot = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("stale task snapshot loads")
        .expect("task exists");
    let changed = TaskRepo::wake_dispatch_for_project(&db, &project_id, &now_rfc3339())
        .await
        .expect("project wake commits");
    assert_eq!(changed, 1);
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(current.version, stale_snapshot.version + 1);
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().expect("metadata exists"))
            .expect("metadata parses");
    assert_eq!(metadata["custom"], "kept");
    assert!(metadata.get("dispatch_disposition").is_none());
    assert!(metadata.get("deferred_dispatch").is_none());
    assert_eq!(metadata["paused_integration"]["state"], "review");

    let stale_record = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        Some(stale_snapshot.version),
        vec![TaskMetadataMutation::Set {
            key: "dispatch_disposition".to_owned(),
            value: json!({"capability":"stale-worker"}),
        }],
        &now_rfc3339(),
    )
    .await;
    assert!(matches!(stale_record, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn task_wake_bumps_version_and_rejects_a_stale_marker_writer() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"dispatch_disposition":{"capability":"worker"},"deferred_dispatch":{"target_state":"todo"}}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("metadata seeds");

    let stale = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let woken = TaskRepo::wake_dispatch_for_task(&db, &task_id, "2026-09-12T00:01:00Z")
        .await
        .expect("task wake commits");
    assert_eq!(woken.version, stale.version + 1);
    assert!(woken
        .metadata_json
        .as_deref()
        .is_none_or(|metadata| !metadata.contains("dispatch_disposition")));
    assert!(woken
        .metadata_json
        .as_deref()
        .is_none_or(|metadata| !metadata.contains("deferred_dispatch")));

    let stale_record = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        Some(stale.version),
        vec![TaskMetadataMutation::Set {
            key: "dispatch_disposition".to_owned(),
            value: json!({"capability":"stale-worker"}),
        }],
        "2026-09-12T00:02:00Z",
    )
    .await;
    assert!(matches!(stale_record, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn stale_same_assignment_confirmation_cannot_overwrite_a_reassignment() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;

    let original = TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "reviewer".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some("agent-old".to_owned()),
            created_at: initial_time.to_owned(),
            updated_at: initial_time.to_owned(),
        },
    )
    .await
    .expect("original assignment creates");

    // A legacy/direct assignment update does not advance Task.version. The
    // confirmation CAS must still bind to the exact assignment row so a
    // stale caller cannot put the old authority back.
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "reviewer".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some("agent-new".to_owned()),
            created_at: initial_time.to_owned(),
            updated_at: "2026-09-12T00:01:00Z".to_owned(),
        },
    )
    .await
    .expect("new assignment creates");

    let stale_confirmation = TaskRoleAssignmentRepo::assign_if_unchanged(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "reviewer".to_owned(),
            assignee_type: original.assignee_type.clone(),
            assignee_id: original.assignee_id.clone(),
            created_at: original.created_at.clone(),
            updated_at: "2026-09-12T00:02:00Z".to_owned(),
        },
        Some(&original),
    )
    .await;
    assert!(matches!(stale_confirmation, Err(DbError::VersionConflict)));

    let current = TaskRoleAssignmentRepo::get_by_task_and_role(&db, &task_id, "reviewer")
        .await
        .expect("current assignment loads")
        .expect("current assignment exists");
    assert_eq!(current.assignee_id.as_deref(), Some("agent-new"));
}

#[tokio::test]
async fn project_and_repository_authority_wakes_are_atomic_with_rollback() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;

    let current_project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    seed_dispatch_markers(&db, &task_id).await;
    let updated_project = ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(r#"{"default_review_config":{"ci_steps":[]}}"#.to_owned()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: "2026-09-12T00:01:00Z".to_owned(),
        },
        current_project.version,
        None,
    )
    .await
    .expect("settings update and wake commit");
    let task_after_settings = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(!task_after_settings
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    // A stale Project CAS aborts before either the Project row or Task cache is
    // changed. This is the rollback half of the same-transaction boundary.
    seed_dispatch_markers(&db, &task_id).await;
    let stale = ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(r#"{"stale":true}"#.to_owned()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: "2026-09-12T00:02:00Z".to_owned(),
        },
        current_project.version,
        None,
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
    let unchanged_project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    assert_eq!(unchanged_project.settings, updated_project.settings);
    let task_after_rollback = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after rollback")
        .expect("task exists");
    assert!(task_after_rollback
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    seed_dispatch_markers(&db, &task_id).await;
    ProjectRepo::update_workflow(
        &db,
        &project_id,
        r#"{"states":{}}"#,
        Some("custom"),
        updated_project.version,
        "2026-09-12T00:03:00Z",
    )
    .await
    .expect("workflow update and wake commit");
    let task_after_workflow = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after workflow")
        .expect("task exists");
    assert!(!task_after_workflow
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    let project_after_workflow = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project reloads")
        .expect("project exists");
    seed_dispatch_markers(&db, &task_id).await;
    ProjectRepo::set_paused_at(&db, &project_id, Some("2026-09-12T00:04:00Z".to_owned()))
        .await
        .expect("pause and wake commit");
    let task_after_pause = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after pause")
        .expect("task exists");
    assert!(!task_after_pause
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    seed_dispatch_markers(&db, &task_id).await;
    ProjectRepo::set_paused_at(&db, &project_id, None)
        .await
        .expect("resume and wake commit");
    let task_after_resume = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after resume")
        .expect("task exists");
    assert!(!task_after_resume
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    // Repository creation/linking, repository updates, and deletion all share
    // the same wake helper. A stale project version rolls the new repo and
    // its optional configuration back with the unchanged markers.
    let stale_repo_id = new_uuid_v4();
    let stale_repo = CreateRepo {
        id: stale_repo_id.clone(),
        project_id: project_id.clone(),
        name: "stale".to_owned(),
        remote_url: "https://example.test/stale.git".to_owned(),
        local_path: None,
        work_mode: WorkMode::DirectMerge,
        default_branch: "main".to_owned(),
        created_at: "2026-09-12T00:05:00Z".to_owned(),
        updated_at: "2026-09-12T00:05:00Z".to_owned(),
    };
    seed_dispatch_markers(&db, &task_id).await;
    let stale_repo_result = RepoRepo::create_primary_for_project(
        &db,
        stale_repo,
        None,
        project_after_workflow.version - 1,
        "2026-09-12T00:05:00Z".to_owned(),
    )
    .await;
    assert!(matches!(stale_repo_result, Err(DbError::VersionConflict)));
    assert!(RepoRepo::get_by_id(&db, &stale_repo_id)
        .await
        .expect("stale repo lookup")
        .is_none());
    let task_after_repo_rollback = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after repo rollback")
        .expect("task exists");
    assert!(task_after_repo_rollback
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    let project_before_repo = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project reloads before repo")
        .expect("project exists");
    let repo_id = new_uuid_v4();
    let repo = RepoRepo::create_primary_for_project(
        &db,
        CreateRepo {
            id: repo_id,
            project_id: project_id.clone(),
            name: "primary".to_owned(),
            remote_url: "https://example.test/primary.git".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: "2026-09-12T00:06:00Z".to_owned(),
            updated_at: "2026-09-12T00:06:00Z".to_owned(),
        },
        None,
        project_before_repo.version,
        "2026-09-12T00:06:00Z".to_owned(),
    )
    .await
    .expect("primary repo and link commit");
    let task_after_repo_create = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after repo create")
        .expect("task exists");
    assert!(!task_after_repo_create
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    seed_dispatch_markers(&db, &task_id).await;
    RepoRepo::update(
        &db,
        UpdateRepo {
            id: repo.id.clone(),
            name: Some("renamed".to_owned()),
            local_path: None,
            remote_url: None,
            work_mode: None,
            default_branch: None,
            updated_at: "2026-09-12T00:07:00Z".to_owned(),
        },
    )
    .await
    .expect("repo update and wake commit");
    let task_after_repo_update = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after repo update")
        .expect("task exists");
    assert!(!task_after_repo_update
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));

    seed_dispatch_markers(&db, &task_id).await;
    let project_before_repo_delete = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project reloads before repo delete")
        .expect("project exists");
    RepoRepo::delete(&db, &repo.id)
        .await
        .expect("repo delete and wake commit");
    let project_after_repo_delete = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("project reloads after repo delete")
        .expect("project exists");
    assert_eq!(
        project_after_repo_delete.version,
        project_before_repo_delete.version + 1,
        "primary-repository deletion advances the Project CAS generation"
    );
    let task_after_repo_delete = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after repo delete")
        .expect("task exists");
    assert!(!task_after_repo_delete
        .metadata_json
        .as_deref()
        .expect("metadata exists")
        .contains("dispatch_disposition"));
}

#[tokio::test]
async fn review_authority_cas_rejects_a_stale_task_revision() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let passed = TaskRepo::set_review_passed_at_cas(
        &db,
        &task_id,
        current.version,
        Some("2026-09-12T00:01:00Z".to_owned()),
        "2026-09-12T00:01:00Z",
    )
    .await
    .expect("authority CAS commits");
    assert_eq!(passed.version, current.version + 1);
    assert!(passed.review_passed_at.is_some());

    let stale = TaskRepo::set_review_passed_at_cas(
        &db,
        &task_id,
        current.version,
        None,
        "2026-09-12T00:02:00Z",
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn role_authority_invalidation_is_one_cas_transaction() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let current = TaskRepo::set_review_passed_at_cas(
        &db,
        &task_id,
        current.version,
        Some("2026-09-12T00:01:00Z".to_owned()),
        "2026-09-12T00:01:00Z",
    )
    .await
    .expect("authority seeds");

    let (assignment, cleared) = TaskRoleAssignmentRepo::assign_and_clear_review_authority(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: None,
            assignee_id: None,
            created_at: "2026-09-12T00:02:00Z".to_owned(),
            updated_at: "2026-09-12T00:02:00Z".to_owned(),
        },
        None,
        current.version,
        "2026-09-12T00:02:00Z",
    )
    .await
    .expect("assignment and authority clear");
    assert_eq!(assignment.role_name, "coder");
    assert_eq!(cleared.version, current.version + 1);
    assert!(cleared.review_passed_at.is_none());

    // Legacy assignment writers do not advance Task.version. The new
    // boundaries must still reject an operation holding the old assignment
    // snapshot, rather than overwriting/deleting the newer row.
    let newer_assignment = TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: None,
            assignee_id: Some("new-assignee".to_owned()),
            created_at: "2026-09-12T00:02:30Z".to_owned(),
            updated_at: "2026-09-12T00:02:30Z".to_owned(),
        },
    )
    .await
    .expect("newer assignment writes");
    let stale_assign = TaskRoleAssignmentRepo::assign_and_clear_review_authority(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: None,
            assignee_id: Some("stale-assignee".to_owned()),
            created_at: "2026-09-12T00:02:45Z".to_owned(),
            updated_at: "2026-09-12T00:02:45Z".to_owned(),
        },
        Some(&assignment),
        cleared.version,
        "2026-09-12T00:02:45Z",
    )
    .await;
    assert!(matches!(stale_assign, Err(DbError::VersionConflict)));

    let stale_remove = TaskRoleAssignmentRepo::remove_and_clear_review_authority(
        &db,
        &assignment,
        cleared.version,
        "2026-09-12T00:03:00Z",
    )
    .await;
    assert!(matches!(stale_remove, Err(DbError::VersionConflict)));
    assert_eq!(
        TaskRoleAssignmentRepo::get_by_task_and_role(&db, &task_id, "coder")
            .await
            .expect("assignment reloads")
            .expect("newer assignment exists")
            .assignee_id
            .as_deref(),
        newer_assignment.assignee_id.as_deref()
    );
}

#[tokio::test]
async fn metadata_increment_and_compare_mutate_are_atomic_and_conditional() {
    let db = database().await;
    let project_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let initial_time = "2026-09-12T00:00:00Z";
    project(&db, &project_id, initial_time).await;
    task(&db, &project_id, &task_id, initial_time).await;
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(r#"{"retry_count":0,"marker_id":"a","awaiting_human":true,"awaiting_human_reason":"manual_review"}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("metadata seeds");

    for _ in 0..2 {
        TaskRepo::mutate_metadata(
            &db,
            &task_id,
            None,
            vec![TaskMetadataMutation::Increment {
                key: "retry_count".to_owned(),
                by: 1,
            }],
            "2026-09-12T00:01:00Z",
        )
        .await
        .expect("counter increments");
    }
    let current = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    let metadata: serde_json::Value =
        serde_json::from_str(current.metadata_json.as_deref().expect("metadata exists"))
            .expect("metadata parses");
    assert_eq!(metadata["retry_count"], 2);

    let marker_writer = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::SetIfAbsent {
            key: "marker_id".to_owned(),
            value: json!("stale-writer"),
        }],
        "2099-01-01T00:01:00Z",
    )
    .await
    .expect("stale first-writer marker is a no-op");
    assert_eq!(marker_writer.updated_at, current.updated_at);

    let cleared = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::CompareAndMutate {
            key: "marker_id".to_owned(),
            expected: json!("a"),
            mutations: vec![
                TaskMetadataMutation::Remove {
                    key: "marker_id".to_owned(),
                },
                TaskMetadataMutation::Remove {
                    key: "awaiting_human".to_owned(),
                },
                TaskMetadataMutation::Remove {
                    key: "awaiting_human_reason".to_owned(),
                },
            ],
        }],
        "2026-09-12T00:02:00Z",
    )
    .await
    .expect("marker clears");
    let cleared_metadata: serde_json::Value =
        serde_json::from_str(cleared.metadata_json.as_deref().expect("counter remains"))
            .expect("metadata parses after marker clear");
    assert_eq!(cleared_metadata["retry_count"], 2);
    assert!(cleared_metadata.get("marker_id").is_none());
    assert!(cleared_metadata.get("awaiting_human").is_none());
    assert!(cleared_metadata.get("awaiting_human_reason").is_none());

    let no_op = TaskRepo::mutate_metadata(
        &db,
        &task_id,
        None,
        vec![TaskMetadataMutation::CompareAndMutate {
            key: "marker_id".to_owned(),
            expected: json!("a"),
            mutations: vec![TaskMetadataMutation::Remove {
                key: "awaiting_human_reason".to_owned(),
            }],
        }],
        "2099-01-01T00:00:00Z",
    )
    .await
    .expect("stale marker clear is a no-op");
    assert_eq!(no_op.updated_at, cleared.updated_at);
    assert_eq!(no_op.metadata_json, cleared.metadata_json);
}
