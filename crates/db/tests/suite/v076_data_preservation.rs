use db::{create_sqlite_pool, run_migrations_from, SharedMediaRepo, SqliteDb};
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

type GenesisSnapshotRow = (String, String, Option<String>, Option<String>, String);
type ProjectCharterDefaultsRow = (String, String, i64, Option<String>, Option<String>, i64);

struct TaskMediaFixture<'a> {
    id: &'a str,
    task_id: &'a str,
    filename: &'a str,
    content_type: &'a str,
    bytes: &'a [u8],
    storage_key: &'a str,
    deleted_at: Option<&'a str>,
}

struct GenesisFixture<'a> {
    id: &'a str,
    account_id: &'a str,
    main_chat_id: &'a str,
    lifecycle: &'a str,
    project_id: Option<&'a str>,
    handoff_id: Option<&'a str>,
}

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("forge-v076-{name}-{}-{nanos}", std::process::id()))
}

fn copy_migrations_up_to(max_version: i64, destination: &Path) {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(source_dir).expect("migration dir reads") {
        let entry = entry.expect("migration entry reads");
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(version) = migration_version(filename) else {
            continue;
        };
        if version <= max_version {
            fs::copy(&path, destination.join(filename)).expect("migration copies");
        }
    }
}

fn migration_version(filename: &str) -> Option<i64> {
    filename.strip_prefix('V')?.split_once("__")?.0.parse().ok()
}

async fn apply_legacy_baseline(pool: &SqlitePool, migration_dir: &Path) {
    fs::create_dir_all(migration_dir).expect("migration dir creates");
    copy_migrations_up_to(75, migration_dir);
    run_migrations_from(pool, migration_dir)
        .await
        .expect("pre-V076 migrations apply");
}

fn copy_v076(migration_dir: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("migrations")
        .join("V076__project_charter_milestones_media.sql");
    fs::copy(
        source,
        migration_dir.join("V076__project_charter_milestones_media.sql"),
    )
    .expect("V076 migration copies");
}

async fn insert_user(pool: &SqlitePool, id: &str, now: &str) {
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', ?, ?, ?)",
    )
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("user inserts");
}

async fn insert_project(pool: &SqlitePool, id: &str, name: &str, owner_id: &str, now: &str) {
    sqlx::query(
        "INSERT INTO project
            (id, name, settings, workflow_definition, owner_id, created_at, updated_at)
         VALUES (?, ?, '{}', '{}', ?, ?, ?)",
    )
    .bind(id)
    .bind(name)
    .bind(owner_id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("project inserts");
}

async fn account_main_chat_id(pool: &SqlitePool, account_id: &str) -> String {
    sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'account_main' AND account_id = ?")
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("main chat lookup")
}

async fn project_chat_id(pool: &SqlitePool, project_id: &str) -> String {
    sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
        .bind(project_id)
        .fetch_one(pool)
        .await
        .expect("project chat lookup")
}

async fn insert_task(
    pool: &SqlitePool,
    id: &str,
    project_id: &str,
    title: &str,
    deleted_at: Option<&str>,
    archived_at: Option<&str>,
    now: &str,
) {
    sqlx::query(
        "INSERT INTO task
            (id, project_id, repo_id, title, task_type, status, deleted_at,
             archived_at, created_at, updated_at)
         VALUES (?, ?, NULL, ?, 'task', ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(project_id)
    .bind(title)
    .bind(if deleted_at.is_some() {
        "cancelled"
    } else {
        "todo"
    })
    .bind(deleted_at)
    .bind(archived_at)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("task inserts");
}

async fn insert_task_media(pool: &SqlitePool, fixture: TaskMediaFixture<'_>, now: &str) {
    sqlx::query(
        "INSERT INTO task_media
            (id, task_id, display_filename, content_type, byte_size, storage_key,
             author_type, author_id, author_name, created_at, deleted_at)
         VALUES (?, ?, ?, ?, ?, ?, 'user', 'preservation-user', 'Preservation Fixture', ?, ?)",
    )
    .bind(fixture.id)
    .bind(fixture.task_id)
    .bind(fixture.filename)
    .bind(fixture.content_type)
    .bind(fixture.bytes.len() as i64)
    .bind(fixture.storage_key)
    .bind(now)
    .bind(fixture.deleted_at)
    .execute(pool)
    .await
    .expect("task media inserts");
}

async fn insert_handoff(
    pool: &SqlitePool,
    id: &str,
    source_chat_id: &str,
    target_chat_id: &str,
    dedupe_key: &str,
    now: &str,
) {
    sqlx::query(
        "INSERT INTO agent_handoff
            (id, source_chat_id, target_chat_id, content, source_revisions_json,
             status, correlation_id, dedupe_key, created_at, updated_at)
         VALUES (?, ?, ?, 'bounded legacy handoff', '[\"legacy-source\"]',
                 'delivered', ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(source_chat_id)
    .bind(target_chat_id)
    .bind(format!("{id}-correlation"))
    .bind(dedupe_key)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("handoff inserts");
}

async fn insert_genesis(pool: &SqlitePool, fixture: GenesisFixture<'_>, now: &str) {
    sqlx::query(
        "INSERT INTO product_genesis_session
            (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
             initial_idea, lifecycle, source_message_ids_json, project_id, handoff_id,
             version, created_at, updated_at)
         VALUES (?, ?, ?, 'legacy-prompt@1', 'legacy prompt', 'mvp',
                 'preserve this Genesis row', ?, '[\"legacy-message\"]', ?, ?,
                 1, ?, ?)",
    )
    .bind(fixture.id)
    .bind(fixture.account_id)
    .bind(fixture.main_chat_id)
    .bind(fixture.lifecycle)
    .bind(fixture.project_id)
    .bind(fixture.handoff_id)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .expect("Genesis inserts");
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn count_rows(pool: &SqlitePool, table: &str) -> i64 {
    // All callers pass fixed table names from this test, not user input.
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .expect("row count reads")
}

async fn legacy_snapshot(pool: &SqlitePool) -> BTreeMap<&'static str, i64> {
    let mut snapshot = BTreeMap::new();
    for table in [
        "user",
        "project",
        "agent_chat",
        "agent_handoff",
        "product_genesis_session",
        "task",
        "task_media",
    ] {
        snapshot.insert(table, count_rows(pool, table).await);
    }
    snapshot
}

async fn media_rows(
    pool: &SqlitePool,
) -> Vec<(String, String, String, String, i64, String, Option<String>)> {
    sqlx::query_as(
        "SELECT id, task_id, display_filename, content_type, byte_size, storage_key,
                deleted_at
         FROM task_media ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("task media snapshot reads")
}

async fn task_rows(
    pool: &SqlitePool,
) -> Vec<(String, String, String, Option<String>, Option<String>)> {
    sqlx::query_as(
        "SELECT id, title, status, deleted_at, archived_at
         FROM task ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("task snapshot reads")
}

#[tokio::test]
async fn v076_empty_and_new_projects_get_explicit_legacy_defaults() {
    let migration_dir = unique_temp_path("empty-new-migrations");
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    apply_legacy_baseline(&pool, &migration_dir).await;

    // Empty baseline: there are no Projects, Tasks, Genesis rows, or media to
    // migrate. This also proves V076 does not synthesize a Charter or receipt.
    let before = legacy_snapshot(&pool).await;
    assert_eq!(before["project"], 0);
    assert_eq!(before["task"], 0);
    assert_eq!(before["task_media"], 0);
    copy_v076(&migration_dir);
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("empty baseline remains migratable");
    assert_eq!(count_rows(&pool, "project_charter").await, 0);
    assert_eq!(count_rows(&pool, "project_charter_approval").await, 0);
    assert_eq!(count_rows(&pool, "project_milestone").await, 0);
    assert_eq!(count_rows(&pool, "media_asset").await, 0);
    let skill_timestamps: Vec<(String, String)> = sqlx::query_as(
        "SELECT created_at, COALESCE(updated_at, created_at)
         FROM operating_skill ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("operating skill timestamps read");
    assert_eq!(skill_timestamps.len(), 2);
    for (created_at, updated_at) in skill_timestamps {
        assert!(chrono::DateTime::parse_from_rfc3339(&created_at).is_ok());
        assert!(chrono::DateTime::parse_from_rfc3339(&updated_at).is_ok());
    }

    // A Project created before V076 is "new" from the user's perspective but
    // has no historical Charter decision. It must remain usable and explicit
    // about setup rather than receiving fabricated approval state.
    let now = "2026-08-13T00:00:00Z";
    insert_user(&pool, "new-project-user", now).await;
    insert_project(&pool, "new-project", "New Project", "new-project-user", now).await;

    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migration is already applied and remains idempotent");
    // The previous call is intentionally a no-op because V076 has already
    // applied on this database. Verify the new row using the same schema.
    let project: (String, i64, Option<String>, Option<String>, i64) = sqlx::query_as(
        "SELECT charter_status, charter_setup_required, current_charter_id,
                current_charter_revision_id, current_charter_version
         FROM project WHERE id = 'new-project'",
    )
    .fetch_one(&pool)
    .await
    .expect("new Project reads");
    assert_eq!(project.0, "legacy_unverified");
    assert_eq!(project.1, 1);
    assert!(project.2.is_none());
    assert!(project.3.is_none());
    assert_eq!(project.4, 0);

    let _ = fs::remove_dir_all(migration_dir);
}

#[tokio::test]
async fn v076_preserves_projects_genesis_tasks_media_rows_and_file_bytes() {
    let migration_dir = unique_temp_path("fixture-migrations");
    let storage_root = unique_temp_path("fixture-media");
    fs::create_dir_all(storage_root.join("legacy")).expect("media root creates");
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    apply_legacy_baseline(&pool, &migration_dir).await;

    let now = "2026-08-13T00:00:00Z";
    insert_user(&pool, "preservation-user", now).await;
    insert_project(
        &pool,
        "project-existing",
        "Existing Project",
        "preservation-user",
        now,
    )
    .await;
    insert_project(
        &pool,
        "project-empty",
        "Empty Existing Project",
        "preservation-user",
        now,
    )
    .await;
    let main_chat_id = account_main_chat_id(&pool, "preservation-user").await;
    let project_chat_id = project_chat_id(&pool, "project-existing").await;

    insert_task(
        &pool,
        "task-active",
        "project-existing",
        "Active task",
        None,
        None,
        now,
    )
    .await;
    insert_task(
        &pool,
        "task-archived",
        "project-existing",
        "Archived task",
        None,
        Some("2026-08-12T00:00:00Z"),
        now,
    )
    .await;
    insert_task(
        &pool,
        "task-deleted",
        "project-existing",
        "Deleted task",
        Some("2026-08-11T00:00:00Z"),
        Some("2026-08-11T00:00:00Z"),
        now,
    )
    .await;

    let image_bytes = b"legacy image bytes\n";
    let video_bytes = b"legacy video bytes\n";
    let deleted_bytes = b"legacy deleted image bytes\n";
    let media_fixtures = [
        (
            "media-image",
            "task-active",
            "evidence.png",
            "image/png",
            image_bytes.as_slice(),
            "legacy/media-image",
            None,
        ),
        (
            "media-video",
            "task-archived",
            "evidence.png",
            "video/mp4",
            video_bytes.as_slice(),
            "legacy/media-video",
            None,
        ),
        (
            "media-deleted",
            "task-deleted",
            "evidence.png",
            "image/png",
            deleted_bytes.as_slice(),
            "legacy/media-deleted",
            Some("2026-08-11T00:00:00Z"),
        ),
    ];
    let mut file_digests = BTreeMap::new();
    for (id, task_id, filename, content_type, bytes, storage_key, deleted_at) in media_fixtures {
        let path = storage_root.join(storage_key);
        fs::write(&path, bytes).expect("fixture file writes");
        file_digests.insert(storage_key, sha256_hex(bytes));
        insert_task_media(
            &pool,
            TaskMediaFixture {
                id,
                task_id,
                filename,
                content_type,
                bytes,
                storage_key,
                deleted_at,
            },
            now,
        )
        .await;
    }

    // These rows exercise both the active Genesis path and an already handed-
    // off historical path. V076 may add pointers, but must not rewrite the
    // existing lifecycle/project/handoff relationships.
    insert_handoff(
        &pool,
        "legacy-handoff",
        &main_chat_id,
        &project_chat_id,
        "legacy-handoff-dedupe",
        now,
    )
    .await;
    insert_genesis(
        &pool,
        GenesisFixture {
            id: "genesis-active",
            account_id: "preservation-user",
            main_chat_id: &main_chat_id,
            lifecycle: "discovering",
            project_id: None,
            handoff_id: None,
        },
        now,
    )
    .await;
    insert_genesis(
        &pool,
        GenesisFixture {
            id: "genesis-handed-off",
            account_id: "preservation-user",
            main_chat_id: &main_chat_id,
            lifecycle: "handed_off",
            project_id: Some("project-existing"),
            handoff_id: Some("legacy-handoff"),
        },
        now,
    )
    .await;

    let before_counts = legacy_snapshot(&pool).await;
    let before_tasks = task_rows(&pool).await;
    let before_media = media_rows(&pool).await;
    let before_genesis: Vec<GenesisSnapshotRow> = sqlx::query_as(
        "SELECT id, lifecycle, project_id, handoff_id, source_message_ids_json
             FROM product_genesis_session ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("Genesis snapshot reads");
    let before_handoffs: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT id, source_chat_id, target_chat_id, content, dedupe_key
         FROM agent_handoff ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("handoff snapshot reads");

    copy_v076(&migration_dir);
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("V076 applies to populated fixture");

    assert_eq!(legacy_snapshot(&pool).await, before_counts);
    assert_eq!(task_rows(&pool).await, before_tasks);
    assert_eq!(media_rows(&pool).await, before_media);
    let after_genesis: Vec<GenesisSnapshotRow> = sqlx::query_as(
        "SELECT id, lifecycle, project_id, handoff_id, source_message_ids_json
             FROM product_genesis_session ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("Genesis rows read after V076");
    assert_eq!(after_genesis, before_genesis);
    let after_handoffs: Vec<(String, String, String, String, String)> = sqlx::query_as(
        "SELECT id, source_chat_id, target_chat_id, content, dedupe_key
         FROM agent_handoff ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("handoff rows read after V076");
    assert_eq!(after_handoffs, before_handoffs);

    for (storage_key, expected_digest) in &file_digests {
        let bytes = fs::read(storage_root.join(storage_key)).expect("fixture file reads");
        assert_eq!(sha256_hex(&bytes), *expected_digest);
    }

    // The additive media index keeps the same legacy IDs, storage keys, byte
    // sizes, and task ownership. Duplicate display filenames remain distinct.
    let assets: Vec<(String, String, String, i64, String, String, String)> = sqlx::query_as(
        "SELECT id, legacy_task_media_id, display_filename, byte_size, storage_key,
                availability, gc_state
         FROM media_asset ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("media asset rows read");
    assert_eq!(assets.len(), 3);
    assert_eq!(
        assets
            .iter()
            .map(|asset| asset.0.clone())
            .collect::<Vec<_>>(),
        vec![
            "media-deleted".to_owned(),
            "media-image".to_owned(),
            "media-video".to_owned()
        ]
    );
    assert_eq!(
        assets
            .iter()
            .map(|asset| asset.4.clone())
            .collect::<Vec<_>>(),
        vec![
            "legacy/media-deleted".to_owned(),
            "legacy/media-image".to_owned(),
            "legacy/media-video".to_owned()
        ]
    );
    let deleted_asset = assets
        .iter()
        .find(|asset| asset.0 == "media-deleted")
        .expect("deleted media asset exists");
    // V076 never claims that migration itself removed historical bytes.  The
    // deleted Task attachment is unavailable, while its still-present bytes
    // remain an eligible quarantined GC candidate for the restartable worker.
    assert_eq!(deleted_asset.5, "quarantined");
    assert_eq!(deleted_asset.6, "gc_candidate");
    assert_eq!(
        assets
            .iter()
            .filter(|asset| asset.2 == "evidence.png")
            .count(),
        3
    );
    assert_eq!(count_rows(&pool, "project_media_attachment").await, 3);
    assert_eq!(count_rows(&pool, "project_charter").await, 0);
    assert_eq!(count_rows(&pool, "project_charter_approval").await, 0);

    // A post-migration cleanup pass may now remove the orphan bytes using the
    // same guarded lease/CAS protocol as ordinary Task deletion.  It must not
    // touch the active or archived Task assets, which remain referenced.
    let db = SqliteDb::new(pool.clone());
    let cleanup_now = "2026-08-13T00:00:00Z";
    let candidates = SharedMediaRepo::claim_media_gc_candidates(
        &db,
        cleanup_now,
        "v076-preservation-test",
        "9999-12-31T00:00:00Z",
        10,
    )
    .await
    .expect("post-migration candidate claim");
    let deleted_candidate = candidates
        .iter()
        .find(|candidate| candidate.id == "media-deleted")
        .expect("deleted legacy media is claimable");
    let deleted_candidate_version = deleted_candidate.version;
    fs::remove_file(storage_root.join("legacy/media-deleted"))
        .expect("post-migration orphan bytes remove");
    SharedMediaRepo::complete_media_gc(
        &db,
        "media-deleted",
        "v076-preservation-test",
        deleted_candidate_version,
        cleanup_now,
    )
    .await
    .expect("post-migration candidate finalize")
    .expect("post-migration asset tombstone");
    assert!(!storage_root.join("legacy/media-deleted").exists());
    assert!(storage_root.join("legacy/media-image").exists());
    assert!(storage_root.join("legacy/media-video").exists());

    let projects: Vec<ProjectCharterDefaultsRow> = sqlx::query_as(
        "SELECT id, charter_status, charter_setup_required, current_charter_id,
                    current_charter_revision_id, current_charter_version
             FROM project ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("Project defaults read");
    assert_eq!(projects.len(), 2);
    for project in projects {
        assert_eq!(project.1, "legacy_unverified");
        assert_eq!(project.2, 1);
        assert!(project.3.is_none());
        assert!(project.4.is_none());
        assert_eq!(project.5, 0);
    }

    let _ = fs::remove_dir_all(migration_dir);
    let _ = fs::remove_dir_all(storage_root);
}

#[tokio::test]
async fn v076_interrupted_migration_rolls_back_and_recovers_without_legacy_loss() {
    let migration_dir = unique_temp_path("interrupted-migrations");
    let storage_root = unique_temp_path("interrupted-media");
    fs::create_dir_all(storage_root.join("legacy")).expect("recovery media root creates");
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    apply_legacy_baseline(&pool, &migration_dir).await;
    let now = "2026-08-13T00:00:00Z";
    insert_user(&pool, "recovery-user", now).await;
    insert_project(
        &pool,
        "recovery-project",
        "Recovery Project",
        "recovery-user",
        now,
    )
    .await;
    insert_task(
        &pool,
        "recovery-task",
        "recovery-project",
        "Recovery task",
        None,
        None,
        now,
    )
    .await;
    let recovery_bytes = b"bytes survive an interrupted V076 migration\n";
    let recovery_storage_key = "legacy/recovery-image";
    fs::write(storage_root.join(recovery_storage_key), recovery_bytes)
        .expect("recovery fixture file writes");
    let recovery_digest = sha256_hex(recovery_bytes);
    insert_task_media(
        &pool,
        TaskMediaFixture {
            id: "recovery-media",
            task_id: "recovery-task",
            filename: "recovery.png",
            content_type: "image/png",
            bytes: recovery_bytes,
            storage_key: recovery_storage_key,
            deleted_at: None,
        },
        now,
    )
    .await;
    let before = legacy_snapshot(&pool).await;
    let before_tasks = task_rows(&pool).await;
    let before_media = media_rows(&pool).await;

    let interrupted_path = migration_dir.join("V076__interrupted.sql");
    fs::write(
        &interrupted_path,
        "CREATE TABLE migration_probe (id TEXT PRIMARY KEY);\n\
         INSERT INTO migration_probe (id) VALUES ('partial');\n\
         CREATE TABLE migration_probe (id TEXT PRIMARY KEY);\n",
    )
    .expect("interrupted migration writes");
    let interrupted = run_migrations_from(&pool, &migration_dir).await;
    assert!(interrupted.is_err(), "interrupted migration must fail");
    assert_eq!(count_rows(&pool, "project").await, before["project"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration WHERE version = 76",)
            .fetch_one(&pool)
            .await
            .expect("migration marker reads"),
        0
    );
    assert_eq!(legacy_snapshot(&pool).await, before);
    assert_eq!(task_rows(&pool).await, before_tasks);
    assert_eq!(media_rows(&pool).await, before_media);
    assert_eq!(
        sha256_hex(
            &fs::read(storage_root.join(recovery_storage_key))
                .expect("recovery file reads after rollback"),
        ),
        recovery_digest
    );
    let probe_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'migration_probe'",
    )
    .fetch_one(&pool)
    .await
    .expect("probe table lookup");
    assert_eq!(probe_exists, 0, "partial DDL must roll back");

    fs::remove_file(&interrupted_path).expect("interrupted migration removes");
    copy_v076(&migration_dir);
    fs::rename(
        migration_dir.join("V076__project_charter_milestones_media.sql"),
        &interrupted_path,
    )
    .expect("valid V076 migration restores");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("valid V076 migration recovers");
    assert_eq!(legacy_snapshot(&pool).await, before);
    assert_eq!(task_rows(&pool).await, before_tasks);
    assert_eq!(media_rows(&pool).await, before_media);
    assert_eq!(
        sha256_hex(
            &fs::read(storage_root.join(recovery_storage_key))
                .expect("recovery file reads after V076"),
        ),
        recovery_digest
    );
    assert_eq!(count_rows(&pool, "operating_skill").await, 2);
    assert_eq!(count_rows(&pool, "project_charter").await, 0);

    let _ = fs::remove_dir_all(migration_dir);
    let _ = fs::remove_dir_all(storage_root);
}
