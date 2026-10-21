use agent_runtime::core::store::SessionSnapshot;
use forge_agent_host::test_support::FileDb;
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct Drive {
    #[serde(skip)]
    pub snapshot: SessionSnapshot,
    pub opens_us: Vec<u64>,
    pub turns_us: Vec<u64>,
    pub maintenance_us: Vec<u64>,
    pub wal_bytes: Vec<u64>,
}
#[derive(Debug, Default, Clone, Serialize)]
pub struct Writes {
    pub object_pack_bytes: i64,
    pub operation_pack_bytes: i64,
    pub legacy_bytes: i64,
    pub largest_pack_write: i64,
    pub largest_pack_rewrite: i64,
    pub pack_write_calls: i64,
}
impl Writes {
    pub fn packs(&self) -> u64 {
        (self.object_pack_bytes + self.operation_pack_bytes) as u64
    }
}

// Connection-local TEMP triggers follow the production write transaction,
// including rollback and intermediate rewrites, without adding main-DB/WAL pages.
async fn connections(pool: &SqlitePool) -> Vec<sqlx::pool::PoolConnection<sqlx::Sqlite>> {
    let mut all = vec![];
    for _ in 0..5 {
        all.push(pool.acquire().await.unwrap());
    }
    all
}
pub async fn install_meter(pool: &SqlitePool, initial: &Writes) {
    let mut all = connections(pool).await;
    for (index, conn) in all.iter_mut().enumerate() {
        sqlx::raw_sql("CREATE TEMP TABLE bench_meter (object_bytes INTEGER,operation_bytes INTEGER,legacy_bytes INTEGER,largest INTEGER,rewrite INTEGER,calls INTEGER,phase INTEGER); INSERT INTO bench_meter VALUES (0,0,0,0,0,0,0);")
            .execute(&mut **conn).await.unwrap();
        if index == 0 {
            sqlx::query("UPDATE bench_meter SET object_bytes=?,operation_bytes=?,legacy_bytes=?,largest=?,rewrite=?,calls=?")
            .bind(initial.object_pack_bytes).bind(initial.operation_pack_bytes).bind(initial.legacy_bytes).bind(initial.largest_pack_write).bind(initial.largest_pack_rewrite).bind(initial.pack_write_calls).execute(&mut **conn).await.unwrap();
        }
        for (label, table, bytes, changed, counter, old_sealed) in [
            (
                "object",
                "agent_session_journal_pack",
                "length(NEW.index_payload)+length(NEW.index_nonce)+length(NEW.body_payload)+length(NEW.body_nonce)",
                "NEW.index_payload IS NOT OLD.index_payload OR NEW.body_payload IS NOT OLD.body_payload",
                "object_bytes",
                "length(OLD.index_payload)>16",
            ),
            (
                "operation",
                "agent_session_journal_operation_pack",
                "length(NEW.payload)+length(NEW.nonce)",
                "NEW.payload IS NOT OLD.payload",
                "operation_bytes",
                "1",
            ),
        ] {
            for action in ["INSERT", "UPDATE"] {
                let when = if action == "UPDATE" {
                    format!("WHEN {changed}")
                } else {
                    String::new()
                };
                let rewrite = if action == "UPDATE" {
                    format!(
                        "CASE WHEN phase=1 OR ({old_sealed}) THEN max(rewrite,{bytes}) ELSE rewrite END"
                    )
                } else {
                    format!("CASE WHEN phase=1 THEN max(rewrite,{bytes}) ELSE rewrite END")
                };
                sqlx::raw_sql(&format!("CREATE TEMP TRIGGER bench_{label}_{action} AFTER {action} ON main.{table} {when} BEGIN UPDATE bench_meter SET {counter}={counter}+{bytes},largest=max(largest,{bytes}),rewrite={rewrite},calls=calls+1; END;")).execute(&mut **conn).await.unwrap();
            }
        }
        for action in ["INSERT", "UPDATE"] {
            let bytes = if action == "INSERT" {
                "coalesce(length(NEW.snapshot_ciphertext)+length(NEW.snapshot_nonce),0)+coalesce(length(NEW.checkpoint_ciphertext)+length(NEW.checkpoint_nonce),0)"
            } else {
                "CASE WHEN NEW.snapshot_ciphertext IS NOT OLD.snapshot_ciphertext THEN coalesce(length(NEW.snapshot_ciphertext)+length(NEW.snapshot_nonce),0) ELSE 0 END + CASE WHEN NEW.checkpoint_ciphertext IS NOT OLD.checkpoint_ciphertext THEN coalesce(length(NEW.checkpoint_ciphertext)+length(NEW.checkpoint_nonce),0) ELSE 0 END"
            };
            sqlx::raw_sql(&format!("CREATE TEMP TRIGGER bench_legacy_{action} AFTER {action} ON main.protected_agent_session_state BEGIN UPDATE bench_meter SET legacy_bytes=legacy_bytes+{bytes}; END;")).execute(&mut **conn).await.unwrap();
        }
        // Exercise rollback on the exact TEMP counter transaction path.
        let mut tx = sqlx::Connection::begin(&mut **conn).await.unwrap();
        sqlx::query("UPDATE bench_meter SET object_bytes=object_bytes+1")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        let value: i64 = sqlx::query_scalar("SELECT object_bytes FROM bench_meter")
            .fetch_one(&mut **conn)
            .await
            .unwrap();
        assert_eq!(
            value,
            if index == 0 {
                initial.object_pack_bytes
            } else {
                0
            }
        );
    }
}
pub async fn writes(pool: &SqlitePool) -> Writes {
    let mut all = connections(pool).await;
    let mut total = Writes::default();
    for conn in &mut all {
        let row = sqlx::query("SELECT * FROM bench_meter")
            .fetch_one(&mut **conn)
            .await
            .unwrap();
        total.object_pack_bytes += row.get::<i64, _>(0);
        total.operation_pack_bytes += row.get::<i64, _>(1);
        total.legacy_bytes += row.get::<i64, _>(2);
        total.largest_pack_write = total.largest_pack_write.max(row.get(3));
        total.largest_pack_rewrite = total.largest_pack_rewrite.max(row.get(4));
        total.pack_write_calls += row.get::<i64, _>(5);
    }
    total
}
pub async fn rewrite_phase(pool: &SqlitePool, active: bool) {
    let mut all = connections(pool).await;
    for conn in &mut all {
        sqlx::query("UPDATE bench_meter SET phase=?")
            .bind(active)
            .execute(&mut **conn)
            .await
            .unwrap();
    }
}
pub async fn configure_wal(pool: &SqlitePool) {
    // Hold every connection so all five receive the connection-local setting.
    let mut connections = vec![];
    for _ in 0..5 {
        connections.push(pool.acquire().await.unwrap());
    }
    for conn in &mut connections {
        sqlx::query("PRAGMA wal_autocheckpoint=0")
            .execute(&mut **conn)
            .await
            .unwrap();
    }
}
pub async fn wal(pool: &SqlitePool) -> u64 {
    let row = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(
        row.get::<i64, _>(0),
        0,
        "no external readers in isolated fixture"
    );
    let frames = row.get::<i64, _>(1).max(0) as u64;
    let page: u64 = sqlx::query_scalar::<_, i64>("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .unwrap() as u64;
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(pool)
        .await
        .unwrap();
    frames * page
}
#[derive(Debug, Serialize)]
pub struct Size {
    pub empty_bytes: u64,
    pub file_bytes: u64,
    pub physical_bytes: u64,
    pub page_count: u64,
    pub page_size: u64,
    pub freelist_before_vacuum: u64,
    pub freelist_after_vacuum: u64,
    pub dbstat_bytes: u64,
    pub pointer_map_bytes: u64,
    pub retained_object_copies: u64,
    pub held_object_copies: u64,
    pub table_bytes: BTreeMap<String, u64>,
    pub rows: BTreeMap<String, u64>,
    pub sealed_payload_nonce_bytes: BTreeMap<String, u64>,
}
pub async fn size(file: &FileDb, empty: u64, _metered: bool) -> Size {
    let db = file.database();
    let pool = db.pool();
    let free_before = scalar(pool, "PRAGMA freelist_count").await;
    for sql in [
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "VACUUM",
        "PRAGMA wal_checkpoint(TRUNCATE)",
    ] {
        sqlx::query(sql).execute(pool).await.unwrap();
    }
    let page_count = scalar(pool, "PRAGMA page_count").await;
    let page_size = scalar(pool, "PRAGMA page_size").await;
    let file_bytes = std::fs::metadata(file.directory().join("journal.db"))
        .unwrap()
        .len();
    assert_eq!(file_bytes, page_count * page_size);
    let mut table_bytes = BTreeMap::new();
    for row in sqlx::query("SELECT name,sum(pgsize) FROM dbstat GROUP BY name")
        .fetch_all(pool)
        .await
        .unwrap()
    {
        table_bytes.insert(row.get::<String, _>(0), row.get::<i64, _>(1) as u64);
    }
    let dbstat_bytes = table_bytes.values().sum();
    let pages: std::collections::BTreeSet<i64> = sqlx::query_scalar("SELECT pageno FROM dbstat")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let missing: Vec<_> = (1..=page_count)
        .filter(|p| !pages.contains(&(*p as i64)))
        .collect();
    let auto_vacuum = scalar(pool, "PRAGMA auto_vacuum").await;
    for page in &missing {
        assert_ne!(auto_vacuum, 0);
        assert_eq!(
            (page - 2) % (page_size / 5 + 1),
            0,
            "only pointer-map pages may be absent from dbstat"
        );
    }
    let pointer_map_bytes = missing.len() as u64 * page_size;
    assert_eq!(file_bytes, dbstat_bytes + pointer_map_bytes);
    let mut rows = BTreeMap::new();
    let mut sealed_payload_nonce_bytes = BTreeMap::new();
    for (table, expr) in [
        (
            "protected_agent_session_state",
            "coalesce(length(snapshot_ciphertext)+length(snapshot_nonce),0)+coalesce(length(checkpoint_ciphertext)+length(checkpoint_nonce),0)",
        ),
        (
            "agent_session_journal_pack",
            "length(index_payload)+length(index_nonce)+length(body_payload)+length(body_nonce)",
        ),
        (
            "agent_session_journal_operation_pack",
            "length(payload)+length(nonce)",
        ),
        (
            "agent_session_journal_session",
            "length(payload)+length(nonce)",
        ),
        ("agent_session_journal_pin", "length(payload)+length(nonce)"),
        (
            "agent_session_journal_catalog",
            "length(payload)+length(nonce)",
        ),
        ("agent_session_journal_membership", "length(bitmap)"),
    ] {
        rows.insert(
            table.into(),
            scalar(pool, &format!("SELECT count(*) FROM {table}")).await,
        );
        sealed_payload_nonce_bytes.insert(
            table.into(),
            scalar(
                pool,
                &format!("SELECT coalesce(sum({expr}),0) FROM {table}"),
            )
            .await,
        );
    }
    let retained_object_copies = scalar(
        pool,
        "SELECT coalesce(sum(object_count),0) FROM agent_session_journal_pack",
    )
    .await;
    let bitmaps: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT bitmap FROM agent_session_journal_membership")
            .fetch_all(pool)
            .await
            .unwrap();
    let held_object_copies = bitmaps
        .iter()
        .flatten()
        .map(|byte| byte.count_ones() as u64)
        .sum();
    Size {
        empty_bytes: empty,
        file_bytes,
        physical_bytes: file_bytes.checked_sub(empty).unwrap(),
        page_count,
        page_size,
        freelist_before_vacuum: free_before,
        freelist_after_vacuum: scalar(pool, "PRAGMA freelist_count").await,
        dbstat_bytes,
        pointer_map_bytes,
        retained_object_copies,
        held_object_copies,
        table_bytes,
        rows,
        sealed_payload_nonce_bytes,
    }
}
pub async fn scalar(pool: &SqlitePool, sql: &str) -> u64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .unwrap() as u64
}
pub async fn setup(id: &agent_runtime::core::ids::SessionId) -> (FileDb, u64) {
    let file = FileDb::new().await.unwrap();
    file.ensure_session(id).await.unwrap();
    configure_wal(file.database().pool()).await;
    let empty = size(&file, 0, false).await.file_bytes;
    install_meter(file.database().pool(), &Writes::default()).await;
    let _ = wal(file.database().pool()).await;
    (file, empty)
}
pub fn ratio(n: u64, d: u64) -> Option<f64> {
    if d == 0 {
        None
    } else {
        Some(n as f64 / d as f64)
    }
}
pub fn mean(v: &[u64]) -> f64 {
    v.iter().sum::<u64>() as f64 / v.len().max(1) as f64
}
pub fn rss_bytes() -> Option<u64> {
    #[cfg(unix)]
    {
        let rss = nix::sys::resource::getrusage(nix::sys::resource::UsageWho::RUSAGE_SELF)
            .ok()?
            .max_rss() as u64;
        if cfg!(target_os = "macos") {
            Some(rss)
        } else {
            Some(rss * 1024)
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}
pub fn cache_json(s: &forge_agent_host::session_journal::ForgeJournalStorage) -> serde_json::Value {
    serde_json::to_value(s.cache_stats().unwrap()).unwrap()
}
