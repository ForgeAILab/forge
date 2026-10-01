use db::{
    create_sqlite_pool, run_migrations, validate_uuid_v4, CreateRepoLocation,
    CreateWorkspacePlacement, DbError, PageRequest, PlacementFailureCause, PlacementOwnerKind,
    PlacementSelectedBy, PlacementState, RepoLocationKind, RepoLocationOwnerKind, RepoLocationRepo,
    RepoLocationStatus, SortBy, SortOrder, SqliteDb, UpdateRepoLocation, UpdateWorkspacePlacement,
    WorkspacePlacementRepo, WorkspaceRepo,
};
use std::{fs, path::Path};

const NOW: &str = "2026-09-29T00:00:00Z";
const LATER: &str = "2026-09-29T01:00:00Z";
const MIGRATION: &str = include_str!("../migrations/V202610010400__daemon_owned_workspaces.sql");

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    seed_project(&db).await;
    db
}

async fn legacy_database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut migrations = fs::read_dir(source)
        .expect("migration directory reads")
        .map(|entry| entry.expect("migration entry reads").path())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?;
            let version: i64 = name.strip_prefix('V')?.split_once("__")?.0.parse().ok()?;
            (version < 202610010400).then_some((version, path))
        })
        .collect::<Vec<_>>();
    migrations.sort_by_key(|(version, _)| *version);
    // Apply historical SQL to the single in-memory connection; no temporary
    // migration directory or on-disk database is needed for this backfill test.
    for (_, path) in migrations {
        let sql = fs::read_to_string(&path).expect("historical migration reads");
        sqlx::raw_sql(&sql)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    }
    let db = SqliteDb::new(pool);
    seed_project(&db).await;
    db
}

async fn seed_project(db: &SqliteDb) {
    sqlx::query(
        "INSERT INTO project (id, name, created_at, updated_at)
         VALUES ('project', 'Placement project', ?, ?)",
    )
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("project inserts");
}

async fn seed_repo(db: &SqliteDb, id: &str, local_path: Option<&str>) {
    sqlx::query(
        "INSERT INTO repo (id, project_id, name, remote_url, local_path, created_at, updated_at)
         VALUES (?, 'project', ?, NULL, ?, ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(local_path)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("repository inserts");
}

async fn seed_workspace(db: &SqliteDb, id: &str, repo_id: &str, status: &str) {
    sqlx::query(
        "INSERT INTO task (id, project_id, title, status, created_at, updated_at)
         VALUES (?, 'project', ?, 'todo', ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("task inserts");
    sqlx::query(
        "INSERT INTO workspace (
            id, task_id, repo_id, worktree_path, branch, status, before_sha, created_at, updated_at
         ) VALUES (?, ?, ?, ?, 'forge/task', ?, 'base-sha', ?, ?)",
    )
    .bind(id)
    .bind(id)
    .bind(repo_id)
    .bind(format!("/forge/worktrees/{id}/repo"))
    .bind(status)
    .bind(NOW)
    .bind(NOW)
    .execute(db.pool())
    .await
    .expect("workspace inserts");
}

fn location(id: &str, repo_id: &str) -> CreateRepoLocation {
    CreateRepoLocation {
        id: id.to_owned(),
        repo_id: repo_id.to_owned(),
        owner_kind: RepoLocationOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        path: "/checkout/repo".to_owned(),
        kind: RepoLocationKind::PrimaryCheckout,
        is_default: true,
        status: RepoLocationStatus::Ready,
        last_verified_at: Some(NOW.to_owned()),
        last_error: None,
        created_at: NOW.to_owned(),
        updated_at: NOW.to_owned(),
    }
}

fn placement(id: &str, workspace_id: &str, location_id: &str) -> CreateWorkspacePlacement {
    CreateWorkspacePlacement {
        id: id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        task_id: workspace_id.to_owned(),
        agent_id: None,
        owner_kind: PlacementOwnerKind::Server,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: location_id.to_owned(),
        execution_daemon_id: None,
        workspace_handle: None,
        generation: 1,
        state: PlacementState::Reserved,
        selected_by: PlacementSelectedBy::Scheduler,
        selection_reason: r#"{"rule":"only_eligible_location","rejected_candidates":[]}"#
            .to_owned(),
        reserved_until: Some(LATER.to_owned()),
        disconnected_at: None,
        failure_cause: None,
        created_at: NOW.to_owned(),
        updated_at: NOW.to_owned(),
    }
}

fn placement_update(id: &str, version: i64, state: PlacementState) -> UpdateWorkspacePlacement {
    UpdateWorkspacePlacement {
        id: id.to_owned(),
        expected_version: version,
        agent_id: None,
        owner_kind: None,
        daemon_id: None,
        runtime_id: None,
        repo_location_id: None,
        execution_daemon_id: None,
        workspace_handle: None,
        generation: None,
        state: Some(state),
        selected_by: None,
        selection_reason: None,
        reserved_until: None,
        disconnected_at: None,
        failure_cause: None,
        updated_at: LATER.to_owned(),
    }
}

fn page(cursor: Option<String>) -> PageRequest {
    PageRequest {
        cursor,
        limit: 1,
        include_total: true,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

#[tokio::test]
async fn migration_backfills_locations_and_non_cleaned_workspaces_without_data_loss() {
    let db = legacy_database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_repo(&db, "unused", Some("/checkout/unused")).await;
    seed_repo(&db, "remote", None).await;
    for status in ["creating", "ready", "error", "cleaning", "cleaned"] {
        seed_workspace(&db, status, "repo", status).await;
    }
    seed_workspace(&db, "remote-ready", "remote", "ready").await;
    seed_workspace(&db, "historical-ready", "deleted-repo", "ready").await;
    for id in ["old-agent", "latest-agent"] {
        sqlx::query(
            "INSERT INTO agent_identity (id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(id)
        .bind(NOW)
        .bind(NOW)
        .execute(db.pool())
        .await
        .expect("identity inserts");
    }
    for (id, agent_id, created_at) in [
        ("old", Some("old-agent"), NOW),
        ("latest-a", Some("old-agent"), LATER),
        ("latest-z", Some("latest-agent"), LATER),
    ] {
        sqlx::query(
            "INSERT INTO execution (id, task_id, agent_id, role, status, workspace_id, created_at, updated_at)
             VALUES (?, 'ready', ?, 'executor', 'completed', 'ready', ?, ?)",
        )
        .bind(id)
        .bind(agent_id)
        .bind(created_at)
        .bind(created_at)
        .execute(db.pool())
        .await
        .expect("execution inserts");
    }
    let before: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT id, repo_id, worktree_path, status FROM workspace ORDER BY id")
            .fetch_all(db.pool())
            .await
            .expect("legacy workspaces load");

    sqlx::raw_sql(MIGRATION)
        .execute(db.pool())
        .await
        .expect("V202610010400 backfill applies");

    let after: Vec<(String, String, String, String)> =
        sqlx::query_as("SELECT id, repo_id, worktree_path, status FROM workspace ORDER BY id")
            .fetch_all(db.pool())
            .await
            .expect("workspaces reload");
    assert_eq!(before, after);
    for repo_id in ["repo", "unused"] {
        let locations = RepoLocationRepo::list_by_repo(&db, repo_id, page(None))
            .await
            .expect("locations load");
        assert_eq!(locations.total_count, Some(1));
        let location = &locations.items[0];
        assert!(validate_uuid_v4(&location.id));
        assert_eq!(location.owner_kind, RepoLocationOwnerKind::Server);
        assert_eq!(location.kind, RepoLocationKind::PrimaryCheckout);
        assert_eq!(location.status, RepoLocationStatus::Ready);
        assert!(location.is_default);
        assert_eq!(location.path, format!("/checkout/{repo_id}"));
        assert_eq!(location.version, 1);
    }
    for (workspace_id, state) in [
        ("creating", PlacementState::Preparing),
        ("ready", PlacementState::Ready),
        ("error", PlacementState::Failed),
        ("cleaning", PlacementState::Cleaning),
        ("remote-ready", PlacementState::Ready),
        ("historical-ready", PlacementState::Ready),
    ] {
        let placement = WorkspacePlacementRepo::get_by_workspace_id(&db, workspace_id)
            .await
            .expect("placement loads")
            .expect("non-cleaned workspace has placement");
        assert!(validate_uuid_v4(&placement.id));
        assert_eq!(placement.task_id, workspace_id);
        assert_eq!(placement.owner_kind, PlacementOwnerKind::Server);
        assert_eq!(placement.daemon_id, None);
        assert_eq!(placement.runtime_id, None);
        assert_eq!(placement.execution_daemon_id, None);
        assert_eq!(placement.selected_by, PlacementSelectedBy::Backfill);
        assert_eq!(placement.state, state);
        assert_eq!(placement.version, 1);
        assert_eq!(placement.generation, 1);
        assert_eq!(placement.reserved_until, None);
        assert_eq!(placement.disconnected_at, None);
        assert_eq!(placement.created_at, NOW);
        assert_eq!(placement.updated_at, NOW);
        assert_eq!(
            placement.workspace_handle,
            Some(format!("/forge/worktrees/{workspace_id}/repo"))
        );
        assert_eq!(
            placement.agent_id.as_deref(),
            (workspace_id == "ready").then_some("latest-agent")
        );
        assert_eq!(
            placement.failure_cause,
            (workspace_id == "error").then_some(PlacementFailureCause::PrepareFailed)
        );
        let reason: serde_json::Value =
            serde_json::from_str(&placement.selection_reason).expect("reason is JSON");
        assert_eq!(reason["rule"], "backfill");
        if matches!(workspace_id, "remote-ready" | "historical-ready") {
            let location = RepoLocationRepo::get_by_id(&db, &placement.repo_location_id)
                .await
                .expect("legacy source loads")
                .expect("legacy source exists");
            let repo_id = if workspace_id == "remote-ready" {
                "remote"
            } else {
                "deleted-repo"
            };
            assert_eq!(location.repo_id, repo_id);
            assert_eq!(location.path, format!("/forge/worktrees/.repos/{repo_id}"));
            assert_eq!(location.status, RepoLocationStatus::Unverified);
        }
    }
    assert!(WorkspacePlacementRepo::get_by_workspace_id(&db, "cleaned")
        .await
        .expect("cleaned placement lookup")
        .is_none());
    let execution_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution")
        .fetch_one(db.pool())
        .await
        .expect("execution count loads");
    assert_eq!(execution_count, 3);
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(db.pool())
        .await
        .expect("foreign keys check")
        .is_empty());
}

#[tokio::test]
async fn placement_updates_conflict_at_the_same_version_and_clear_nullable_fields() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_workspace(&db, "workspace", "repo", "ready").await;
    RepoLocationRepo::create(&db, location("location", "repo"))
        .await
        .expect("location creates");
    let created =
        WorkspacePlacementRepo::create(&db, placement("placement", "workspace", "location"))
            .await
            .expect("placement creates");
    let mut update = placement_update(&created.id, created.version, PlacementState::Disconnected);
    update.workspace_handle = Some(Some("owner-issued-handle".to_owned()));
    update.generation = Some(2);
    update.disconnected_at = Some(Some(LATER.to_owned()));
    update.failure_cause = Some(Some(PlacementFailureCause::OwnerDisconnected));
    let (first, second) = tokio::join!(
        WorkspacePlacementRepo::update(&db, update.clone()),
        WorkspacePlacementRepo::update(&db, update),
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(DbError::VersionConflict)))
            .count(),
        1
    );
    let winner = results
        .into_iter()
        .find_map(Result::ok)
        .expect("one winner");
    assert_eq!(winner.version, created.version + 1);
    assert_eq!(winner.generation, 2);
    assert_eq!(
        winner.failure_cause,
        Some(PlacementFailureCause::OwnerDisconnected)
    );
    assert_eq!(winner.reserved_until.as_deref(), Some(LATER));
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&db, &winner.id)
            .await
            .expect("lookup"),
        Some(winner.clone())
    );
    assert_eq!(
        WorkspacePlacementRepo::list_by_state(&db, PlacementState::Disconnected)
            .await
            .expect("disconnected placements"),
        vec![winner.clone()]
    );

    let mut clear = placement_update(&winner.id, winner.version, PlacementState::Ready);
    clear.workspace_handle = Some(None);
    clear.reserved_until = Some(None);
    clear.disconnected_at = Some(None);
    clear.failure_cause = Some(None);
    let ready = WorkspacePlacementRepo::update(&db, clear)
        .await
        .expect("fields clear");
    assert_eq!(ready.workspace_handle, None);
    assert_eq!(ready.reserved_until, None);
    assert_eq!(ready.disconnected_at, None);
    assert_eq!(ready.failure_cause, None);
    assert_eq!(ready.generation, 2);
    assert_eq!(ready.version, winner.version + 1);
}

#[tokio::test]
async fn location_updates_conflict_at_the_same_version() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    let created = RepoLocationRepo::create(&db, location("location", "repo"))
        .await
        .expect("location creates");
    let update = UpdateRepoLocation {
        id: created.id.clone(),
        expected_version: created.version,
        path: Some("/checkout/moved".to_owned()),
        kind: None,
        is_default: Some(false),
        status: Some(RepoLocationStatus::Invalid),
        last_verified_at: Some(None),
        last_error: Some(Some("verification failed".to_owned())),
        updated_at: LATER.to_owned(),
    };
    let (first, second) = tokio::join!(
        RepoLocationRepo::update(&db, update.clone()),
        RepoLocationRepo::update(&db, update),
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(DbError::VersionConflict)))
            .count(),
        1
    );
    let winner = results
        .into_iter()
        .find_map(Result::ok)
        .expect("one winner");
    assert_eq!(winner.version, created.version + 1);
    assert_eq!(winner.path, "/checkout/moved");
    assert_eq!(winner.status, RepoLocationStatus::Invalid);
    assert_eq!(winner.last_verified_at, None);
    assert_eq!(winner.last_error.as_deref(), Some("verification failed"));
    assert!(!winner.is_default);
}

#[tokio::test]
async fn workspace_placement_is_unique_and_reservation_creation_is_transactional() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_workspace(&db, "workspace", "repo", "creating").await;
    RepoLocationRepo::create(&db, location("location", "repo"))
        .await
        .expect("location creates");
    let mut transaction = db::begin_immediate(db.pool()).await.expect("transaction");
    let created = WorkspacePlacementRepo::create_in_tx(
        &db,
        &mut transaction,
        placement("placement", "workspace", "location"),
    )
    .await
    .expect("placement creates in reservation");
    assert_eq!(
        WorkspacePlacementRepo::get_by_workspace_id_in_tx(&db, &mut transaction, "workspace")
            .await
            .expect("reservation lookup"),
        Some(created.clone())
    );
    transaction
        .rollback()
        .await
        .expect("reservation rolls back");
    assert!(WorkspacePlacementRepo::get_by_id(&db, "placement")
        .await
        .expect("lookup")
        .is_none());
    let created =
        WorkspacePlacementRepo::create(&db, placement("placement", "workspace", "location"))
            .await
            .expect("placement creates");
    let duplicate =
        WorkspacePlacementRepo::create(&db, placement("duplicate", "workspace", "location")).await;
    assert!(
        matches!(duplicate, Err(DbError::Sqlx(sqlx::Error::Database(ref error))) if error.is_unique_violation())
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_workspace_id(&db, "workspace")
            .await
            .expect("workspace lookup"),
        Some(created)
    );
}

#[tokio::test]
async fn location_delete_refuses_every_non_cleaned_state() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_workspace(&db, "workspace", "repo", "ready").await;
    RepoLocationRepo::create(&db, location("location", "repo"))
        .await
        .expect("location creates");
    let mut current =
        WorkspacePlacementRepo::create(&db, placement("placement", "workspace", "location"))
            .await
            .expect("placement creates");
    for state in [
        PlacementState::Reserved,
        PlacementState::Preparing,
        PlacementState::Ready,
        PlacementState::Disconnected,
        PlacementState::Cleaning,
        PlacementState::Failed,
    ] {
        current = WorkspacePlacementRepo::update(
            &db,
            placement_update(&current.id, current.version, state),
        )
        .await
        .expect("placement state changes");
        assert!(matches!(
            RepoLocationRepo::delete(&db, "location").await,
            Err(DbError::VersionConflict)
        ));
        assert_eq!(
            RepoLocationRepo::get_blocking_placement(&db, "location")
                .await
                .expect("blocking placement loads")
                .expect("in use")
                .task_id,
            "workspace"
        );
    }
    WorkspacePlacementRepo::update(
        &db,
        placement_update(&current.id, current.version, PlacementState::Cleaned),
    )
    .await
    .expect("owner cleanup ack records");
    RepoLocationRepo::delete(&db, "location")
        .await
        .expect("cleaned location deletes");
    assert!(WorkspacePlacementRepo::get_by_id(&db, "placement")
        .await
        .expect("lookup")
        .is_none());
    assert!(WorkspaceRepo::get_by_id(&db, "workspace")
        .await
        .expect("workspace lookup")
        .is_some());
}

#[tokio::test]
async fn repo_deletion_retains_owner_cleanup_and_workspace_deletion_cascades_placement() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_workspace(&db, "workspace", "repo", "ready").await;
    let location = RepoLocationRepo::create(&db, location("location", "repo"))
        .await
        .expect("location creates");
    let placement =
        WorkspacePlacementRepo::create(&db, placement("placement", "workspace", "location"))
            .await
            .expect("placement creates");
    sqlx::query("DELETE FROM repo WHERE id = 'repo'")
        .execute(db.pool())
        .await
        .expect("repo deletes");
    assert_eq!(
        RepoLocationRepo::get_by_id(&db, "location")
            .await
            .expect("location lookup"),
        Some(location)
    );
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&db, "placement")
            .await
            .expect("placement lookup"),
        Some(placement)
    );
    WorkspaceRepo::delete(&db, "workspace")
        .await
        .expect("workspace deletes");
    assert!(WorkspacePlacementRepo::get_by_id(&db, "placement")
        .await
        .expect("placement lookup")
        .is_none());
    RepoLocationRepo::delete(&db, "location")
        .await
        .expect("unreferenced location deletes");
}

#[tokio::test]
async fn location_pages_filter_before_pagination_and_counts() {
    let db = database().await;
    seed_repo(&db, "repo", Some("/checkout/repo")).await;
    seed_repo(&db, "other", None).await;
    for (id, repo_id) in [("a", "other"), ("b", "repo"), ("c", "other"), ("d", "repo")] {
        RepoLocationRepo::create(&db, location(id, repo_id))
            .await
            .expect("location creates");
    }
    let first = RepoLocationRepo::list_by_repo(&db, "repo", page(None))
        .await
        .expect("first page");
    assert_eq!(first.items[0].id, "b");
    assert_eq!(first.total_count, Some(2));
    assert!(first.next_cursor.is_some());
    let second = RepoLocationRepo::list_by_repo(&db, "repo", page(first.next_cursor))
        .await
        .expect("second page");
    assert_eq!(second.items[0].id, "d");
    assert_eq!(second.total_count, Some(2));
    assert_eq!(second.next_cursor, None);
    assert!(matches!(
        RepoLocationRepo::list_by_repo(&db, "repo", page(Some("invalid".to_owned()))).await,
        Err(DbError::InvalidCursor)
    ));
}
