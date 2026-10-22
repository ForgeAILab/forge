use std::{fs, path::Path};

use db::{create_sqlite_pool, run_migrations_from, SqlitePool};
use sqlx::Row;

const MIGRATION: &str = "V202610200941__session_journal.sql";
const DELETION_MIGRATION: &str = "V202610201253__session_journal_deletion.sql";
const METADATA: [&str; 5] = ["session", "pin", "catalog", "membership", "operation_pack"];

// quote(BLOB) returns the complete X'hex' literal, with no lossy text decode.
// Capture every column, its SQLite type and NULLs, including future additions.
async fn raw_rows(pool: &SqlitePool, table: &str) -> Vec<Vec<String>> {
    let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await
        .unwrap();
    let fields = columns
        .iter()
        .flat_map(|column| {
            let name: String = column.get("name");
            [format!("typeof(\"{name}\")"), format!("quote(\"{name}\")")]
        })
        .collect::<Vec<_>>();
    sqlx::query(&format!(
        "SELECT {} FROM {table} ORDER BY 1, 2",
        fields.join(",")
    ))
    .fetch_all(pool)
    .await
    .unwrap()
    .iter()
    .map(|row| (0..fields.len()).map(|index| row.get(index)).collect())
    .collect()
}

async fn host_session(pool: &SqlitePool, id: &str, runtime: Option<&str>) {
    sqlx::query(
        "INSERT INTO agent_session
         (id, identity_id, profile_id, context_scope_id, backend_kind,
          runtime_session_id, status, created_at, updated_at)
         VALUES (?, 'schema-agent', 'schema-profile', 'schema-scope', 'native', ?, 'suspended', 'created', 'updated')",
    ).bind(id).bind(runtime).execute(pool).await.unwrap();
}

async fn session(pool: &SqlitePool, storage: &str, role: &str, runtime: &str, forge: &str) {
    sqlx::query(
        "INSERT INTO agent_session_journal_session
         (storage_id, role, runtime_session_id, forge_session_id, generation, revision, writer_epoch,
          collection_dirty, published, has_checkpoint, retired, key_revision, format, nonce, payload)
         VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, 1, 0, 1, 1, zeroblob(24), zeroblob(16))",
    ).bind(storage).bind(role).bind(runtime).bind(forge)
        .bind(u64::MAX.to_be_bytes().as_slice())
        .bind((i64::MAX as u64 + 1).to_be_bytes().as_slice())
        .bind(u64::MAX.to_be_bytes().as_slice())
        .execute(pool).await.unwrap();
}

/// Packs are owned by a runtime session (shared by its domains).
async fn pack(pool: &SqlitePool, runtime: &str, forge: &str) -> i64 {
    sqlx::query(
        "INSERT INTO agent_session_journal_pack
         (runtime_session_id, forge_session_id, generation, object_count,
          retained_canonical_bytes, key_revision, format, index_nonce, index_payload, body_nonce, body_payload)
         VALUES (?, ?, zeroblob(8), 8, 1024, 1, 1, zeroblob(24), zeroblob(16), zeroblob(24), zeroblob(16))",
    ).bind(runtime).bind(forge)
        .execute(pool).await.unwrap().last_insert_rowid()
}

async fn membership(
    pool: &SqlitePool,
    scope: (&str, &str, &str, &str),
    pack: i64,
    bits: &[u8],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO agent_session_journal_membership
         (storage_id, role, runtime_session_id, forge_session_id, pack_id, bitmap) VALUES (?, ?, ?, ?, ?, ?)",
    ).bind(scope.0).bind(scope.1).bind(scope.2).bind(scope.3).bind(pack).bind(bits)
        .execute(pool).await.map(|_| ())
}

fn constraint(error: sqlx::Error, expected: &str) {
    let error = error.as_database_error().expect("SQLite constraint error");
    assert!(
        error.message().contains(expected),
        "expected {expected}: {error}"
    );
}

#[tokio::test]
async fn migration_preserves_legacy_and_scoped_packs() {
    let directory = tempfile::tempdir().unwrap();
    let migrations = directory.path().join("migrations");
    fs::create_dir(&migrations).unwrap();
    let bundled = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    for entry in fs::read_dir(&bundled).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|ext| ext == "sql")
            && entry
                .file_name()
                .to_str()
                .unwrap()
                .split_once("__")
                .unwrap()
                .0[1..]
                .parse::<u64>()
                .unwrap()
                < 202610200941
        {
            fs::copy(entry.path(), migrations.join(entry.file_name())).unwrap();
        }
    }
    let url = format!("sqlite://{}", directory.path().join("legacy.db").display());
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    for sql in [
        "INSERT INTO agent_identity (id, name, created_at, updated_at) VALUES ('schema-agent', 'Schema fixture', 'c', 'u')",
        "INSERT INTO agent_profile (id, identity_id, backend_kind, executor_type, created_at, updated_at) VALUES ('schema-profile', 'schema-agent', 'native', 'embedded', 'c', 'u')",
        "INSERT INTO agent_context_scope (id, identity_id, scope_type, scope_id, created_at, updated_at) VALUES ('schema-scope', 'schema-agent', 'account', 'schema-owner', 'c', 'u')",
    ] { sqlx::query(sql).execute(&pool).await.unwrap(); }
    for (index, runtime) in [Some("duplicate"), Some("duplicate"), None, None]
        .into_iter()
        .enumerate()
    {
        let id = format!("legacy-{index}");
        host_session(&pool, &id, runtime).await;
        sqlx::query(
            "INSERT INTO protected_agent_session_state
             (session_id, snapshot_ciphertext, snapshot_nonce, checkpoint_ciphertext, checkpoint_nonce,
              checkpoint_turn_id, checkpoint_revision, checkpoint_fingerprint, key_revision,
              state_revision, updated_at, lcm_policy_revision, snapshot_digest, checkpoint_lcm_policy_revision)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 7, 91, 'byte-exact-time', 'old-policy', 'old-digest', 'checkpoint-policy')",
        ).bind(&id).bind(vec![0_u8, 255, 0, index as u8, 128, 39])
            .bind(vec![index as u8; 24])
            .bind((index != 3).then_some(vec![255_u8, 0, index as u8, 39, 128]))
            .bind((index != 3).then_some(vec![255_u8; 24]))
            .bind((index != 3).then_some("legacy-turn"))
            .bind((index != 3).then_some(42_i64))
            .bind((index != 3).then_some("legacy-fingerprint"))
            .execute(&pool).await.unwrap();
    }
    let legacy_before = raw_rows(&pool, "protected_agent_session_state").await;
    let sessions_before = raw_rows(&pool, "agent_session").await;
    let legacy_schema: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE name = 'protected_agent_session_state'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    pool.close().await;

    fs::copy(bundled.join(MIGRATION), migrations.join(MIGRATION)).unwrap();
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &migrations)
        .await
        .expect("upgrade with duplicates and NULL runtime ids");
    assert_eq!(
        raw_rows(&pool, "protected_agent_session_state").await,
        legacy_before
    );
    assert_eq!(raw_rows(&pool, "agent_session").await, sessions_before);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE name = 'protected_agent_session_state'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        legacy_schema
    );
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'agent_session_journal_%'")
        .fetch_one(&pool).await.unwrap(), 6);
    let index = sqlx::query("PRAGMA index_list(agent_session)")
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.get::<String, _>("name") == "idx_agent_session_runtime_session_id")
        .unwrap();
    assert_eq!(
        index.get::<i64, _>("unique"),
        0,
        "runtime mapping must never become unique"
    );
    host_session(&pool, "duplicate-after-upgrade", Some("duplicate")).await;
    host_session(&pool, "null-after-upgrade", None).await;

    host_session(&pool, "owner-a", Some("runtime-a")).await;
    host_session(&pool, "owner-b", Some("runtime-b")).await;
    for scope in [
        ("store", "protected", "runtime-a", "owner-a"),
        ("store", "ordinary", "runtime-a", "owner-a"),
        ("store", "protected", "runtime-b", "owner-b"),
        ("other-store", "protected", "runtime-a", "owner-a"),
    ] {
        session(&pool, scope.0, scope.1, scope.2, scope.3).await;
    }
    for (runtime, forge) in [("runtime-a", "owner-a"), ("runtime-b", "owner-b")] {
        for (table, key) in [
            ("pin", "pin_key"),
            ("catalog", "head_key"),
            ("operation_pack", "tier"),
        ] {
            let value = if key == "tier" { "64" } else { "zeroblob(32)" };
            sqlx::query(&format!(
                "INSERT INTO agent_session_journal_{table}
                 (storage_id, role, runtime_session_id, forge_session_id, {key}, key_revision, format, nonce, payload)
                 VALUES ('store', 'protected', ?, ?, {value}, 1, 1, zeroblob(24), zeroblob(16))"
            )).bind(runtime).bind(forge).execute(&pool).await.unwrap();
        }
    }
    let pack_a = pack(&pool, "runtime-a", "owner-a").await;
    let pack_b = pack(&pool, "runtime-b", "owner-b").await;
    let a = ("store", "protected", "runtime-a", "owner-a");
    for bits in [&[][..], &[0][..], &[0, 0][..]] {
        constraint(
            membership(&pool, a, pack_a, bits).await.unwrap_err(),
            "CHECK",
        );
    }
    // A pack is shared by its runtime session's domains, never by another
    // session or Forge owner.
    for scope in [
        ("store", "protected", "runtime-b", "owner-b"),
        ("store", "protected", "runtime-a", "owner-b"),
    ] {
        constraint(
            membership(&pool, scope, pack_a, &[1]).await.unwrap_err(),
            "FOREIGN KEY",
        );
    }
    membership(&pool, a, pack_a, &[128]).await.unwrap();
    for scope in [
        ("store", "ordinary", "runtime-a", "owner-a"),
        ("other-store", "protected", "runtime-a", "owner-a"),
    ] {
        membership(&pool, scope, pack_a, &[1]).await.unwrap();
    }
    membership(
        &pool,
        ("store", "protected", "runtime-b", "owner-b"),
        pack_b,
        &[1],
    )
    .await
    .unwrap();

    // Each invalid update must be refused in-row, without relying on storage.
    for change in [
        "published = 0",
        "retired = 1",
        "has_checkpoint = 2",
        "collection_dirty = -1",
        "generation = zeroblob(7)",
        "revision = 1",
        "writer_epoch = -1",
        "role = 'unknown'",
        "key_revision = 0",
        "format = 0",
        "nonce = zeroblob(23)",
        "payload = zeroblob(15)",
    ] {
        constraint(sqlx::query(&format!("UPDATE agent_session_journal_session SET {change} WHERE forge_session_id = 'owner-a'"))
            .execute(&pool).await.unwrap_err(), "CHECK");
    }
    for change in [
        "object_count = 0",
        "retained_canonical_bytes = -1",
        "retained_canonical_bytes = 1048577",
        "generation = zeroblob(7)",
        "index_nonce = zeroblob(23)",
        "body_nonce = zeroblob(23)",
        "index_payload = zeroblob(15)",
        "body_payload = zeroblob(15)",
    ] {
        constraint(
            sqlx::query(&format!(
                "UPDATE agent_session_journal_pack SET {change} WHERE pack_id = ?"
            ))
            .bind(pack_a)
            .execute(&pool)
            .await
            .unwrap_err(),
            "CHECK",
        );
    }
    constraint(
        sqlx::query("UPDATE agent_session_journal_pin SET pin_key = zeroblob(31)")
            .execute(&pool)
            .await
            .unwrap_err(),
        "CHECK",
    );
    constraint(
        sqlx::query("UPDATE agent_session_journal_catalog SET head_key = zeroblob(31)")
            .execute(&pool)
            .await
            .unwrap_err(),
        "CHECK",
    );
    constraint(
        sqlx::query("UPDATE agent_session_journal_operation_pack SET tier = -1")
            .execute(&pool)
            .await
            .unwrap_err(),
        "CHECK",
    );

    // Every host cascade can seek its child index. Packs retain an indexed label
    // but deliberately have no owner FK. The dirty cursor index is partial.
    for table in METADATA.into_iter().chain(["pack"]) {
        let full = format!("agent_session_journal_{table}");
        let plan = sqlx::query(&format!(
            "EXPLAIN QUERY PLAN SELECT 1 FROM {full} WHERE forge_session_id = 'owner-a'"
        ))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            plan.iter()
                .any(|row| row.get::<String, _>("detail").contains("SEARCH")),
            "{full}: indexed owner"
        );
        let fks = sqlx::query(&format!("PRAGMA foreign_key_list({full})"))
            .fetch_all(&pool)
            .await
            .unwrap();
        if table == "pack" {
            assert!(fks.is_empty());
        } else {
            assert!(fks
                .iter()
                .any(|row| row.get::<String, _>("table") == "agent_session"
                    && row.get::<String, _>("from") == "forge_session_id"
                    && row.get::<String, _>("on_delete") == "CASCADE"));
        }
    }
    let dirty_plan = sqlx::query("EXPLAIN QUERY PLAN SELECT runtime_session_id FROM agent_session_journal_session WHERE storage_id = 'store' AND role = 'protected' AND collection_dirty = 1 AND runtime_session_id > '' ORDER BY runtime_session_id LIMIT 2")
        .fetch_all(&pool).await.unwrap();
    assert!(dirty_plan.iter().any(|row| row
        .get::<String, _>("detail")
        .contains("idx_session_journal_dirty")));
    let membership_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT 1 FROM agent_session_journal_membership WHERE pack_id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(membership_plan.iter().any(|row| row
        .get::<String, _>("detail")
        .contains("idx_session_journal_membership_pack")));

    // Upgrade an already-populated journal without touching its sealed units,
    // legacy payloads or membership/metadata. Backfill uses clear indexed labels.
    let packs_before = raw_rows(&pool, "agent_session_journal_pack").await;
    let mut metadata_before = Vec::new();
    for table in METADATA {
        metadata_before.push(raw_rows(&pool, &format!("agent_session_journal_{table}")).await);
    }
    fs::copy(
        bundled.join(DELETION_MIGRATION),
        migrations.join(DELETION_MIGRATION),
    )
    .unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    let mut packs_after = raw_rows(&pool, "agent_session_journal_pack").await;
    for row in &mut packs_after {
        row.truncate(packs_before[0].len());
    }
    assert_eq!(packs_after, packs_before);
    for (table, before) in METADATA.into_iter().zip(metadata_before) {
        assert_eq!(
            raw_rows(&pool, &format!("agent_session_journal_{table}")).await,
            before
        );
    }
    assert_eq!(
        raw_rows(&pool, "protected_agent_session_state").await,
        legacy_before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM agent_session_journal_pack WHERE orphaned=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    sqlx::query("DELETE FROM agent_session WHERE id = 'owner-a'")
        .execute(&pool)
        .await
        .unwrap();
    for table in METADATA {
        let rows: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM agent_session_journal_{table} WHERE forge_session_id = 'owner-a'"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows, 0, "{table} cascades");
        let other: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM agent_session_journal_{table} WHERE forge_session_id = 'owner-b'"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(other, 1, "{table} preserves other owner");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_session_journal_pack")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT retained_canonical_bytes FROM agent_session_journal_pack WHERE pack_id = ?"
        )
        .bind(pack_a)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1024,
        "orphan retains physical-byte evidence"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT orphaned FROM agent_session_journal_pack WHERE pack_id=?"
        )
        .bind(pack_a)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    sqlx::query("DELETE FROM agent_session_journal_pack WHERE pack_id = ?")
        .bind(pack_a)
        .execute(&pool)
        .await
        .unwrap();
    let next_pack = pack(&pool, "runtime-b", "owner-b").await;
    assert!(
        next_pack > pack_b && next_pack > pack_a,
        "AUTOINCREMENT ids never reuse a deleted id"
    );
    sqlx::query("DELETE FROM agent_session_journal_pack WHERE pack_id = ?")
        .bind(next_pack)
        .execute(&pool)
        .await
        .unwrap();
    let last_pack = pack(&pool, "runtime-b", "owner-b").await;
    assert!(
        last_pack > next_pack,
        "even the highest deleted id is not reused"
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        raw_rows(&pool, "protected_agent_session_state").await,
        legacy_before
    );
    pool.close().await;
    let pool = create_sqlite_pool(&url).await.unwrap();
    run_migrations_from(&pool, &migrations).await.unwrap();
    assert_eq!(
        raw_rows(&pool, "protected_agent_session_state").await,
        legacy_before,
        "durable byte-exact legacy rows after reopen"
    );
    pool.close().await;
}
