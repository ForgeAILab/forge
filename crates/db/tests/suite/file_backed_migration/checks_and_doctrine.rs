//! Check attestation, indexes, and registry doctrine migrations.

use super::*;

/// The registry doctrine cutover (Genesis @8, Project @22) on a file-backed
/// database that was already running on @7 / @21: every historical revision
/// keeps its exact bytes, the two new revisions are inserted with the digest of
/// their own body, only the current pointers move, and a second run of the
/// migration runner changes nothing.
#[tokio::test]
async fn registry_doctrine_cutover_adds_revisions_and_keeps_history_on_a_file_database() {
    use sha2::{Digest, Sha256};

    async fn revisions(pool: &sqlx::SqlitePool) -> Vec<(String, String, String, String, String)> {
        sqlx::query(
            "SELECT id, canonical_body, content_digest, policy_json, policy_digest
             FROM operating_skill_revision ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .expect("revisions read")
        .into_iter()
        .map(|row| {
            (
                row.get("id"),
                row.get("canonical_body"),
                row.get("content_digest"),
                row.get("policy_json"),
                row.get("policy_digest"),
            )
        })
        .collect()
    }
    async fn pointers(pool: &sqlx::SqlitePool) -> Vec<(String, String, i64)> {
        sqlx::query("SELECT id, current_revision_id, version FROM operating_skill ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("pointers read")
            .into_iter()
            .map(|row| {
                (
                    row.get("id"),
                    row.get("current_revision_id"),
                    row.get("version"),
                )
            })
            .collect()
    }

    let migration_dir = unique_temp_path("registry-doctrine-migrations");
    fs::create_dir_all(&migration_dir).expect("temp migration dir creates");
    copy_migrations_up_to(202610111630, &migration_dir);
    let db_path = unique_temp_path("registry-doctrine-db").with_extension("db");
    let url = format!("sqlite://{}", db_path.display());
    let pool = create_sqlite_pool(&url).await.expect("pool");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("pre-cutover migrations apply");

    let history = revisions(&pool).await;
    let before = pointers(&pool).await;
    let pointer = |rows: &[(String, String, i64)], skill: &str| {
        rows.iter()
            .find(|row| row.0 == skill)
            .map(|row| (row.1.clone(), row.2))
            .expect("seeded operating skill")
    };
    const MAIN: &str = "forge.main.project-discovery/v2";
    const PROJECT: &str = "forge.project.orchestration/v1";
    let (main_before, main_version) = pointer(&before, MAIN);
    let (project_before, project_version) = pointer(&before, PROJECT);
    assert_eq!(main_before, format!("{MAIN}@7"));
    assert_eq!(project_before, format!("{PROJECT}@21"));

    run_migrations(&pool).await.expect("cutover applies");
    let after = revisions(&pool).await;
    for row in &history {
        assert!(
            after.contains(row),
            "historical revision {} keeps its exact bytes",
            row.0
        );
    }
    let added = after
        .iter()
        .filter(|row| !history.contains(row))
        .collect::<Vec<_>>();
    assert_eq!(
        added.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
        [format!("{MAIN}@8"), format!("{PROJECT}@22")]
    );
    for (row, predecessor) in added.iter().zip([main_before, project_before]) {
        assert_eq!(
            row.2,
            hex::encode(Sha256::digest(row.1.as_bytes())),
            "{} carries the digest of its own body",
            row.0
        );
        let predecessor = history
            .iter()
            .find(|old| old.0 == predecessor)
            .expect("predecessor revision");
        assert_eq!(
            (&row.3, &row.4),
            (&predecessor.3, &predecessor.4),
            "{} keeps the policy of the revision it follows",
            row.0
        );
        assert_ne!(row.1, predecessor.1);
    }
    let moved = pointers(&pool).await;
    assert_eq!(
        pointer(&moved, MAIN),
        (format!("{MAIN}@8"), main_version + 1)
    );
    assert_eq!(
        pointer(&moved, PROJECT),
        (format!("{PROJECT}@22"), project_version + 1)
    );
    assert_eq!(
        moved.len(),
        before.len(),
        "no operating skill is added or removed"
    );

    run_migrations(&pool).await.expect("second run is a no-op");
    assert_eq!(revisions(&pool).await, after);
    assert_eq!(pointers(&pool).await, moved);
    pool.close().await;
    let reopened = create_sqlite_pool(&url).await.expect("file reopens");
    assert_eq!(pointers(&reopened).await, moved);
    reopened.close().await;
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_dir_all(&migration_dir);
}

/// `V202610120637` adds the owner environment attestation table to a
/// populated file database and changes nothing else. The table holds opaque
/// revisions only, and applying the bundle again is a no-op.
#[tokio::test]
async fn check_environment_attestation_migration_is_additive_on_a_file_database() {
    let migration_dir = unique_temp_path("attestation-migrations");
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_up_to(202610120418, &migration_dir);
    let db_path = unique_temp_path("attestation-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations before the attestation apply");
    assert!(!table_exists(&pool, "check_environment_attestation").await);
    let now = "2026-10-12T00:00:00Z";
    sqlx::query("INSERT INTO system_setting(key,value,updated_at) VALUES('check_value_revision_salt','existing-salt',?)")
        .bind(now)
        .execute(&pool)
        .await
        .expect("an installation's salt exists before the migration");
    let schema = |pool: db::SqlitePool| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT group_concat(type||':'||name||':'||COALESCE(sql,''),char(10)) FROM (SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE '%check_environment_attestation%' ORDER BY type,name)",
        )
        .fetch_one(&pool)
        .await
        .expect("schema reads")
    };
    let before = schema(pool.clone()).await;

    // Up to this migration: a later one (`V202610121119`) rebuilds an index
    // of `check_consumer`, which is its own change.
    let all = unique_temp_path("attestation-migrations-with");
    fs::create_dir_all(&all).expect("migration dir creates");
    copy_migrations_up_to(202610120637, &all);
    run_migrations_from(&pool, &all)
        .await
        .expect("the attestation migration applies to an existing database");
    run_migrations_from(&pool, &all)
        .await
        .expect("applying the bundle again changes nothing");
    assert_eq!(before, schema(pool.clone()).await);
    let salt: String = sqlx::query_scalar(
        "SELECT value FROM system_setting WHERE key='check_value_revision_salt'",
    )
    .fetch_one(&pool)
    .await
    .expect("salt reads");
    assert_eq!(salt, "existing-salt");
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(check_environment_attestation)")
        .fetch_all(&pool)
        .await
        .expect("columns read")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    assert_eq!(
        columns,
        [
            "owner_id",
            "environment_generation",
            "salt_revision",
            "inherited_revision",
            "shell_revision",
            "created_at"
        ]
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_environment_attestation")
        .fetch_one(&pool)
        .await
        .expect("rows read");
    assert_eq!(
        rows, 0,
        "the first identity build of the new process attests"
    );
    // The first answer of a generation is the one every reader gets.
    let store = db::SqliteDb::new(pool.clone());
    let answer = db::CheckEnvironmentAttestation {
        inherited_revision: "a".repeat(64),
        shell_revision: "b".repeat(64),
    };
    let stored = store
        .store_check_environment_attestation("server", "generation", "salt-revision", &answer)
        .await
        .expect("answer stores");
    assert_eq!(stored, answer);
    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(migration_dir);
}

/// `V202610121119` rebuilds the undelivered-result index of `check_consumer`
/// on a populated file database: no row changes, and applying the bundle
/// again is a no-op.
#[tokio::test]
async fn check_consumer_answered_index_migration_changes_the_index_only() {
    let migration_dir = unique_temp_path("answered-index-migrations");
    fs::create_dir_all(&migration_dir).expect("migration dir creates");
    copy_migrations_up_to(202610120637, &migration_dir);
    let db_path = unique_temp_path("answered-index-db").with_extension("db");
    let pool = create_sqlite_pool(&format!("sqlite://{}", db_path.display()))
        .await
        .expect("pool creates");
    run_migrations_from(&pool, &migration_dir)
        .await
        .expect("migrations before the index rebuild apply");
    let now = "2026-10-12T00:00:00Z";
    sqlx::raw_sql(&format!(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p','{now}','{now}');
         INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main','{now}','{now}');
         INSERT INTO check_consumer(id,project_id,repo_id,status_epoch,origin,purpose,request_key,identity_key,created_at) VALUES('c','p','r',0,'entry','entry_ci','key','identity','{now}');"
    ))
    .execute(&pool)
    .await
    .expect("a consumer exists before the migration");
    let index = |pool: db::SqlitePool| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='check_consumer_undelivered'",
        )
        .fetch_one(&pool)
        .await
        .expect("index reads")
    };
    let rows = |pool: db::SqlitePool| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT json_object('id',id,'request_key',request_key,'applied_at',applied_at,'delivery_step_id',delivery_step_id,'created_at',created_at) FROM check_consumer",
        )
        .fetch_all(&pool)
        .await
        .expect("rows read")
    };
    let before = rows(pool.clone()).await;
    assert!(!index(pool.clone()).await.contains("applied_at IS NULL"));

    copy_migrations_up_to(202610121119, &migration_dir);
    for _ in 0..2 {
        run_migrations_from(&pool, &migration_dir)
            .await
            .expect("the index rebuild applies, and again as a no-op");
    }
    assert!(index(pool.clone()).await.contains("applied_at IS NULL"));
    assert_eq!(rows(pool.clone()).await, before);
    let applied: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _migration WHERE version=202610121119")
            .fetch_one(&pool)
            .await
            .expect("migration row reads");
    assert_eq!(applied, 1);
    pool.close().await;
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_dir_all(&migration_dir);
}

/// Upgrading a populated database through the five logic-trigger drops keeps
/// every row, removes all fourteen triggers and records the five migrations;
/// today's writers then produce what the triggers did. The runner does not
/// refuse a database that carries migrations its bundle lacks, so a binary
/// from before the drops starts on the upgraded file with neither the
/// triggers nor the code (an owner decision, documented here).
#[tokio::test]
async fn logic_trigger_drops_upgrade_a_populated_database() {
    const LAST_WITH_TRIGGERS: i64 = 202_610_161_644;
    const MOVED: [&str; 14] = [
        "user_agent_chat_after_insert",
        "project_agent_chat_after_insert",
        "pricing_account_deletion_guard_begin",
        "pricing_account_deletion_guard_end",
        "agent_wake_blocker_rearm_on_reopen",
        "genesis_topic_rotation",
        "handoff_topic_rotation",
        "idle_topic_rotation",
        "published_handoff_topic_rotation",
        "topic_rotation_origin",
        "project_media_attachment_gc_guard_after_insert",
        "project_release_media_pin_gc_guard_after_insert",
        "task_media_project_asset_after_insert",
        "task_media_project_attachment_after_delete",
    ];
    async fn moved_triggers(pool: &sqlx::SqlitePool) -> i64 {
        let names = MOVED
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name IN ({names})"
        ))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    let old_dir = unique_temp_path("logic-trigger-drop-old");
    fs::create_dir_all(&old_dir).unwrap();
    copy_migrations_up_to(LAST_WITH_TRIGGERS, &old_dir);
    let (pool, db_path) = migration_test_pool("logic-trigger-drop-db").await;
    run_migrations_from(&pool, &old_dir).await.unwrap();
    assert_eq!(moved_triggers(&pool).await, 14);
    sqlx::raw_sql(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES ('drop-user', 'drop@example.test', 'test', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');
         INSERT INTO project (id, name, owner_id, created_at, updated_at)
         VALUES ('drop-project', 'Drop', 'drop-user', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z');",
    )
    .execute(&pool)
    .await
    .unwrap();
    let rows = |pool: sqlx::SqlitePool| async move {
        sqlx::query_scalar::<_, String>(
            "SELECT json_group_array(json_array(t, id)) FROM (
                SELECT 'chat' AS t, id FROM agent_chat
                UNION ALL SELECT 'binding', id FROM project_agent_binding
                UNION ALL SELECT 'user', id FROM user
                UNION ALL SELECT 'project', id FROM project
                ORDER BY 1, 2)",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    };
    let before = rows(pool.clone()).await;

    let full = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    run_migrations_from(&pool, &full).await.unwrap();
    assert_eq!(
        moved_triggers(&pool).await,
        0,
        "every moved trigger is dropped"
    );
    assert_eq!(rows(pool.clone()).await, before, "no row is rewritten");
    let recorded: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _migration WHERE version > ? AND version <= 202610180820 ORDER BY version")
            .bind(LAST_WITH_TRIGGERS)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        recorded,
        vec![
            202_610_180_752,
            202_610_180_758,
            202_610_180_804,
            202_610_180_812,
            202_610_180_820
        ]
    );
    let db = SqliteDb::new(pool.clone());
    UserRepo::create_user(
        &db,
        &User {
            id: "after-drop".to_owned(),
            email: "after-drop@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: "2026-10-18T00:00:00Z".to_owned(),
            updated_at: "2026-10-18T00:00:00Z".to_owned(),
        },
    )
    .await
    .unwrap();
    assert!(AgentChatRepo::get_main_chat(&db, "after-drop")
        .await
        .unwrap()
        .is_some());

    // An older bundle replays nothing and refuses nothing on the newer file.
    run_migrations_from(&pool, &old_dir)
        .await
        .expect("an older bundle starts on the newer database");
    assert_eq!(moved_triggers(&pool).await, 0);

    pool.close().await;
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_dir_all(old_dir);
}
