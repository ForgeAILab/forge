use super::*;
use db::{create_sqlite_pool, run_migrations, CreateProject, CreateRepo, UpdateProject};
use tempfile::TempDir;

#[test]
fn verification_workspace_boundary_is_written_once_and_names_the_checkout() {
    let root = std::env::temp_dir().join(format!(
        "forge-verify-boundary-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    ));
    std::fs::create_dir_all(&root).expect("workspace root");
    super::write_verification_workspace_boundary(&root).expect("boundary written");
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("manifest");
    assert!(manifest.contains("[workspace]"));
    assert!(manifest.contains("members = [\"checkout\"]"));
    // An existing manifest is the operator's; it is never overwritten.
    std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers = []\n").expect("rewrite");
    super::write_verification_workspace_boundary(&root).expect("idempotent");
    assert_eq!(
        std::fs::read_to_string(root.join("Cargo.toml")).expect("manifest"),
        "[workspace]\nmembers = []\n"
    );
    let _ = std::fs::remove_dir_all(&root);
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

#[tokio::test]
async fn project_agent_generation_reuse_quarantines_old_workspace_without_touching_replacement() {
    let db = sqlite_db().await;
    let root = TempDir::new().expect("workspace root creates");
    let project_id = new_uuid_v4();
    let old_created_at = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "old generation".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: old_created_at.clone(),
            updated_at: old_created_at,
        },
    )
    .await
    .expect("old Project creates");
    let (old_authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("old authority query")
        .expect("old authority exists");
    let workspace = root.path().join(&project_id);
    assert!(
        reserve_project_agent_workspace_path(&db, &old_authority, &workspace)
            .await
            .expect("old workspace reserves")
    );
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("old notes directory creates");
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
        .expect("old checkout directory creates");
    std::fs::write(
        workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"),
        "old generation",
    )
    .expect("old note writes");
    std::fs::write(
        workspace.join(PROJECT_AGENT_CHECKOUT_DIR).join("old-file"),
        "old generation",
    )
    .expect("old checkout writes");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("old Project deletes");
    // Project IDs are normally generated afresh. This low-level fixture
    // deliberately models an explicit ID-reuse operation after the old
    // creation event has been archived by the caller.
    sqlx::query("DELETE FROM domain_event WHERE dedupe_key = ?")
        .bind(format!("project-created:{project_id}"))
        .execute(db.pool())
        .await
        .expect("old creation event archives");
    let replacement_created_at = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "replacement generation".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: replacement_created_at,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("replacement Project creates with reused ID");
    let (replacement_authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("replacement authority query")
        .expect("replacement authority exists");
    assert_ne!(
        old_authority.provisioning_operation_id, replacement_authority.provisioning_operation_id,
        "Project ID reuse must create a new immutable generation"
    );

    // This is the late-admission ordering: the replacement reserves the
    // shared Project-ID path before the stale creator gets its final
    // retention check. Reservation quarantines the old tree, stamps the
    // replacement marker, and only then permits replacement data.
    assert!(
        reserve_project_agent_workspace_path(&db, &replacement_authority, &workspace)
            .await
            .expect("replacement workspace reserves")
    );
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("replacement notes directory creates");
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
        .expect("replacement checkout directory creates");
    std::fs::write(
        workspace
            .join(PROJECT_AGENT_DOCS_DIR)
            .join("replacement-note"),
        "replacement generation",
    )
    .expect("replacement note writes");
    std::fs::write(
        workspace
            .join(PROJECT_AGENT_CHECKOUT_DIR)
            .join("replacement-file"),
        "replacement generation",
    )
    .expect("replacement checkout writes");

    assert!(retain_project_agent_workspace_if_current(
        &db,
        &old_authority,
        workspace.clone(),
        None,
    )
    .await
    .expect("stale retention check succeeds")
    .is_none());
    assert!(!workspace
        .join(PROJECT_AGENT_DOCS_DIR)
        .join("old-note")
        .exists());
    assert!(!workspace
        .join(PROJECT_AGENT_CHECKOUT_DIR)
        .join("old-file")
        .exists());
    assert_eq!(
        std::fs::read_to_string(
            workspace
                .join(PROJECT_AGENT_DOCS_DIR)
                .join("replacement-note")
        )
        .expect("replacement note remains"),
        "replacement generation"
    );
    assert_eq!(
        std::fs::read_to_string(
            workspace
                .join(PROJECT_AGENT_CHECKOUT_DIR)
                .join("replacement-file")
        )
        .expect("replacement checkout remains"),
        "replacement generation"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
            .expect("replacement marker remains"),
        replacement_authority.generation_marker_contents()
    );
}

#[tokio::test]
async fn project_agent_adopts_markerless_workspace_without_losing_legacy_notes() {
    let db = sqlite_db().await;
    let root = TempDir::new().expect("workspace root creates");
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "legacy workspace".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Project creates");
    let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("authority query")
        .expect("authority exists");
    let workspace = root.path().join(&project_id);
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("legacy notes directory creates");
    std::fs::write(
        workspace.join(PROJECT_AGENT_DOCS_DIR).join("legacy-note"),
        "must survive upgrade",
    )
    .expect("legacy note writes");
    let legacy_checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    std::fs::create_dir_all(&legacy_checkout).expect("legacy checkout directory creates");
    std::fs::write(legacy_checkout.join("legacy-file"), "must be rebuilt")
        .expect("legacy checkout writes");

    assert!(
        reserve_project_agent_workspace_path(&db, &authority, &workspace)
            .await
            .expect("legacy workspace adopts")
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_DOCS_DIR).join("legacy-note"))
            .expect("legacy note remains"),
        "must survive upgrade"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
            .expect("generation marker stamped"),
        authority.generation_marker_contents()
    );
    assert!(
        !legacy_checkout.exists(),
        "markerless disposable checkout is not adopted with durable notes"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn project_agent_reservation_failure_restores_quarantined_checkout() {
    use std::os::unix::fs::PermissionsExt;

    let db = sqlite_db().await;
    let root = TempDir::new().expect("workspace root creates");
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "reservation rollback".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Project creates");
    let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("authority query")
        .expect("authority exists");
    let workspace = root.path().join(&project_id);
    assert!(
        reserve_project_agent_workspace_path(&db, &authority, &workspace)
            .await
            .expect("initial workspace reserves")
    );
    let checkout = workspace.join(PROJECT_AGENT_CHECKOUT_DIR);
    std::fs::create_dir_all(&checkout).expect("checkout directory creates");
    std::fs::write(checkout.join("old-file"), "old checkout").expect("old checkout writes");
    std::fs::write(
        workspace.join(PROJECT_AGENT_REPOSITORY_MARKER),
        "old-repository-marker",
    )
    .expect("old repository marker writes");
    let marker = workspace.join(PROJECT_AGENT_REPOSITORY_MARKER);
    let mut permissions = std::fs::metadata(&marker)
        .expect("repository marker metadata")
        .permissions();
    permissions.set_mode(0o444);
    std::fs::set_permissions(&marker, permissions).expect("repository marker locks");

    let error = reserve_project_agent_workspace_path(&db, &authority, &workspace)
        .await
        .expect_err("locked repository marker rejects reservation");
    assert!(error.to_string().contains("Permission denied"));
    assert_eq!(
        std::fs::read_to_string(checkout.join("old-file"))
            .expect("old checkout restores after failure"),
        "old checkout"
    );

    let mut permissions = std::fs::metadata(&marker)
        .expect("repository marker metadata after failure")
        .permissions();
    permissions.set_mode(0o644);
    std::fs::set_permissions(&marker, permissions).expect("repository marker unlocks");
}

#[tokio::test]
async fn project_agent_whole_workspace_rollback_restores_old_tree() {
    let root = TempDir::new().expect("workspace root creates");
    let workspace = root.path().join("project");
    let old_marker = "old-generation";
    let new_marker = "new-generation";
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("old notes directory creates");
    std::fs::write(workspace.join(PROJECT_AGENT_GENERATION_MARKER), old_marker)
        .expect("old generation marker writes");
    std::fs::write(
        workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"),
        "old durable note",
    )
    .expect("old note writes");

    let stale_workspace =
        quarantine_project_agent_workspace_if_marker_differs(&workspace, new_marker)
            .await
            .expect("old workspace quarantines")
            .expect("old workspace quarantine exists");
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("partial notes directory creates");
    std::fs::write(workspace.join(PROJECT_AGENT_GENERATION_MARKER), new_marker)
        .expect("partial generation marker writes");
    std::fs::write(
        workspace.join(PROJECT_AGENT_DOCS_DIR).join("partial-note"),
        "partial replacement",
    )
    .expect("partial note writes");

    rollback_project_agent_reservation(&workspace, new_marker, Some(stale_workspace), None).await;
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_DOCS_DIR).join("old-note"))
            .expect("old note restores"),
        "old durable note"
    );
    assert!(!workspace
        .join(PROJECT_AGENT_DOCS_DIR)
        .join("partial-note")
        .exists());
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
            .expect("old marker restores"),
        old_marker
    );
    assert!(
        std::fs::read_dir(root.path())
            .expect("workspace parent reads")
            .filter_map(|entry| entry.ok())
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(".project.forge-project-agent-reservation-failed-")),
        "partial replacement remains quarantined for recovery"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn project_agent_rejects_symlink_checkout_without_following() {
    use std::os::unix::fs::symlink;

    let db = sqlite_db().await;
    let root = TempDir::new().expect("workspace root creates");
    let project_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "symlink checkout".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("Project creates");
    let (authority, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("authority query")
        .expect("authority exists");
    let workspace = root.path().join(&project_id);
    assert!(
        reserve_project_agent_workspace_path(&db, &authority, &workspace)
            .await
            .expect("initial workspace reserves")
    );
    let external = root.path().join("external-checkout");
    std::fs::create_dir_all(&external).expect("external directory creates");
    std::fs::write(external.join("sentinel"), "must remain").expect("external sentinel writes");
    symlink(&external, workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
        .expect("checkout symlink creates");

    let error = reserve_project_agent_workspace_path(&db, &authority, &workspace)
        .await
        .expect_err("symlink checkout rejects reservation");
    assert!(error.to_string().contains("checkout is a symlink"));
    assert_eq!(
        std::fs::read_to_string(external.join("sentinel"))
            .expect("external target remains untouched"),
        "must remain"
    );
}

#[tokio::test]
async fn project_agent_version_and_repo_changes_preserve_notes_but_replace_checkout() {
    let db = sqlite_db().await;
    let root = TempDir::new().expect("workspace root creates");
    let project_id = new_uuid_v4();
    let repo_one_id = new_uuid_v4();
    let repo_two_id = new_uuid_v4();
    let now = now_rfc3339();
    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "mutable generation".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("Project creates");
    RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_one_id.clone(),
            project_id: project_id.clone(),
            name: "first repository".to_owned(),
            remote_url: "https://example.invalid/first.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("first repository creates");
    ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_one_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&db, &project_id)
            .await
            .expect("Project lookup")
            .expect("Project exists")
            .version,
        None,
    )
    .await
    .expect("first repository binds");
    let (authority_one, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("first authority query")
        .expect("first authority exists");
    let workspace = root.path().join(&project_id);
    assert!(
        reserve_project_agent_workspace_path(&db, &authority_one, &workspace)
            .await
            .expect("first workspace reserves")
    );
    let first_repository_marker =
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER))
            .expect("first repository marker is stamped");
    assert_eq!(first_repository_marker.len(), 64);
    assert!(!first_repository_marker.contains("example.invalid"));
    assert!(!first_repository_marker.contains("first.git"));
    assert!(!first_repository_marker.contains(&repo_one_id));
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
            .expect("first generation marker is stamped"),
        authority_one.generation_marker_contents()
    );
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_DOCS_DIR))
        .expect("notes directory creates");
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
        .expect("checkout directory creates");
    std::fs::write(
        workspace.join(PROJECT_AGENT_DOCS_DIR).join("note"),
        "keep this",
    )
    .expect("note writes");
    std::fs::write(
        workspace.join(PROJECT_AGENT_CHECKOUT_DIR).join("old-tree"),
        "replace this",
    )
    .expect("old checkout writes");

    let version_before_edit = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("Project lookup")
        .expect("Project exists")
        .version;
    ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: Some("renamed generation".to_owned()),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        version_before_edit,
        None,
    )
    .await
    .expect("Project version bumps");
    let (authority_after_version, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("version authority query")
        .expect("version authority exists");
    assert_eq!(
        authority_one.provisioning_operation_id,
        authority_after_version.provisioning_operation_id
    );
    assert!(
        reserve_project_agent_workspace_path(&db, &authority_after_version, &workspace)
            .await
            .expect("version-bumped workspace reserves")
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_GENERATION_MARKER))
            .expect("generation marker survives version bump"),
        authority_one.generation_marker_contents()
    );
    assert!(workspace.join(PROJECT_AGENT_DOCS_DIR).join("note").exists());
    assert!(workspace
        .join(PROJECT_AGENT_CHECKOUT_DIR)
        .join("old-tree")
        .exists());

    RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_two_id.clone(),
            project_id: project_id.clone(),
            name: "second repository".to_owned(),
            remote_url: "https://example.invalid/second.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second repository creates");
    let version_before_repo_change = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .expect("Project lookup")
        .expect("Project exists")
        .version;
    ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_two_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        version_before_repo_change,
        None,
    )
    .await
    .expect("second repository binds");
    let (authority_two, _) = capture_project_agent_workspace_authority(&db, &project_id)
        .await
        .expect("second authority query")
        .expect("second authority exists");
    assert!(
        reserve_project_agent_workspace_path(&db, &authority_two, &workspace)
            .await
            .expect("repository-changed workspace reserves")
    );
    let second_repository_marker =
        std::fs::read_to_string(workspace.join(PROJECT_AGENT_REPOSITORY_MARKER))
            .expect("second repository marker is stamped");
    assert_ne!(first_repository_marker, second_repository_marker);
    assert_eq!(second_repository_marker.len(), 64);
    assert!(!second_repository_marker.contains("example.invalid"));
    assert!(!second_repository_marker.contains("second.git"));
    assert!(!second_repository_marker.contains(&repo_two_id));
    assert!(workspace.join(PROJECT_AGENT_DOCS_DIR).join("note").exists());
    assert!(
        !workspace
            .join(PROJECT_AGENT_CHECKOUT_DIR)
            .join("old-tree")
            .exists(),
        "repository changes replace only the disposable checkout"
    );
    // The newer admission may finish building its replacement checkout
    // before an older creator gets its final retention check. The stale
    // finalizer must not mistake the newer repository marker for its own
    // checkout and remove the replacement.
    std::fs::create_dir_all(workspace.join(PROJECT_AGENT_CHECKOUT_DIR))
        .expect("replacement checkout directory creates");
    std::fs::write(
        workspace
            .join(PROJECT_AGENT_CHECKOUT_DIR)
            .join("replacement-tree"),
        "new repository",
    )
    .expect("replacement checkout writes");
    assert!(retain_project_agent_workspace_if_current(
        &db,
        &authority_one,
        workspace.clone(),
        None,
    )
    .await
    .expect("stale repository retention check succeeds")
    .is_none());
    assert_eq!(
        std::fs::read_to_string(
            workspace
                .join(PROJECT_AGENT_CHECKOUT_DIR)
                .join("replacement-tree")
        )
        .expect("replacement checkout remains"),
        "new repository"
    );
}
