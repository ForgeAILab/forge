//! Explicitly invoked conversion on a disposable, offline database copy.
use forge_agent_host::SqliteProtectedRuntimeStore;
use services_agents::{
    session_journal_conversion::{ConversionCursor, SessionJournalConversion},
    session_journal_retention::{RetentionCursor, SessionJournalRetention},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Instant,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn refuse_install_path(path: &Path) -> TestResult<()> {
    // Reject every .forge directory, including a symlink's canonical target.
    // This is stricter than only protecting the invoking user's installation.
    if path
        .components()
        .any(|part| matches!(part, Component::Normal(name) if name == ".forge"))
    {
        return Err(
            "journal dry run refuses installation paths under .forge; use a disposable copy".into(),
        );
    }
    Ok(())
}

fn copy_file(path: &Path) -> TestResult<PathBuf> {
    refuse_install_path(path)?;
    let canonical = path.canonicalize()?;
    refuse_install_path(&canonical)?;
    if !canonical.is_file() {
        return Err("journal dry run needs an existing regular file".into());
    }
    Ok(canonical)
}

async fn physical_bytes(pool: &sqlx::SqlitePool) -> TestResult<serde_json::Value> {
    // Include the tables' own pages and all their indexes. These are allocated
    // dbstat pages, independent of readable canonical bytes or freelist space.
    let legacy: i64 = sqlx::query_scalar("SELECT COALESCE(sum(pgsize),0) FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name='protected_agent_session_state')")
        .fetch_one(pool).await?;
    let journal: i64 = sqlx::query_scalar("SELECT COALESCE(sum(pgsize),0) FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name GLOB 'agent_session_journal_*')")
        .fetch_one(pool).await?;
    Ok(serde_json::json!({"legacy_dbstat":legacy,"journal_dbstat":journal}))
}

async fn run_copy(path: &Path, secret_path: &Path) -> TestResult<serde_json::Value> {
    let path = copy_file(path)?;
    let secret_path = copy_file(secret_path)?;
    let secret = std::fs::read(secret_path)?;
    if secret.is_empty() {
        return Err("the copied JWT secret is empty".into());
    }
    let key: [u8; 32] = Sha256::digest(&secret).into();
    let migration_started = Instant::now();
    let pool = db::create_sqlite_pool(&format!("sqlite://{}", path.display())).await?;
    db::run_migrations(&pool).await?;
    let migration_ms = migration_started.elapsed().as_millis();
    let before = physical_bytes(&pool).await?;
    let db = Arc::new(db::SqliteDb::new(pool.clone()));
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), key, 1));
    let conversion = SessionJournalConversion::new(db.clone(), store.clone());
    let initial = db.legacy_journal_conversion_remaining(&[]).await?;
    let conversion_started = Instant::now();
    let mut cursor = ConversionCursor::default();
    let (mut ticks, mut converted, mut skipped_retired, mut skipped_active, mut unreadable) =
        (0, 0, 0, 0, 0);
    let mut parked = BTreeMap::<String, usize>::new();
    loop {
        // The production eight-candidate / two-second budget applies to every
        // tick. No runtime supervisor, executors, daemon or provider is started.
        let report = conversion.convert_once(&mut cursor).await?;
        ticks += 1;
        converted += report.converted;
        skipped_retired += report.skipped_retired;
        skipped_active += report.skipped_active;
        unreadable += report.skipped_unreadable;
        for (reason, count) in report.parked_by_reason {
            *parked.entry(reason).or_default() += count;
        }
        if report.remaining == 0 {
            break;
        }
        if ticks > initial.saturating_mul(2).saturating_add(2) {
            return Err("journal dry run did not converge; pending active sessions remain".into());
        }
        tokio::task::yield_now().await;
    }
    let conversion_ms = conversion_started.elapsed().as_millis();
    let retention_started = Instant::now();
    let retention = SessionJournalRetention::new(db.clone(), &store);
    let collected = retention
        .maintain_once(&mut RetentionCursor::default())
        .await?;
    let retention_ms = retention_started.elapsed().as_millis();
    let after = physical_bytes(&pool).await?;
    let summary = serde_json::json!({
        "converted":converted, "skipped":{"retirement_policy":skipped_retired,"active":skipped_active},
        "parked_by_reason":parked, "unreadable":{"conversion":unreadable,"retention":collected.unreadable_sessions},
        "remaining":0, "conversion_ticks":ticks, "bytes":{"before":before,"after":after},
        "elapsed_ms":{"migrations":migration_ms,"conversion":conversion_ms,"retention":retention_ms},
        "retention":{"retired_domains":collected.retired_domains,"retirement_errors":collected.retirement_errors,
            "collection_calls":collected.collection_calls,"deleted_packs":collected.deleted_packs,
            "budget_exhausted":collected.budget_exhausted}
    });
    drop(retention);
    drop(conversion);
    drop(store);
    drop(db);
    pool.close().await;
    Ok(summary)
}

#[tokio::test]
#[ignore = "mutates only an explicitly selected disposable database copy"]
async fn journal_real_data_dry_run() -> TestResult<()> {
    let Some(path) = std::env::var_os("FORGE_JOURNAL_DRYRUN_DB") else {
        eprintln!("FORGE_JOURNAL_DRYRUN_DB unset; journal dry run skipped");
        return Ok(());
    };
    let path = PathBuf::from(path);
    refuse_install_path(&path)?;
    let secret = std::env::var_os("FORGE_JOURNAL_DRYRUN_SECRET_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| path.with_file_name("jwt_secret.bin"));
    println!("{}", run_copy(&path, &secret).await?);
    Ok(())
}

#[test]
fn dry_run_refuses_installation_paths_before_opening() {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    assert!(copy_file(&home.join(".forge/forge.db")).is_err());
    assert!(copy_file(Path::new("copies/.forge/forge.db")).is_err());
}

#[tokio::test]
async fn dry_run_entrypoint_on_generated_database() {
    use agent_runtime::core::ids::SessionId;
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let db = fixture.database();
    let secret = b"journal-dryrun-fixture-secret-material";
    let key: [u8; 32] = Sha256::digest(secret).into();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), key, 1));
    for n in 0..10 {
        super::session_journal_conversion::seed(&fixture, &store, &format!("dryrun-{n:02}")).await;
    }
    sqlx::query("UPDATE agent_session SET status='replaced' WHERE runtime_session_id='dryrun-00'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=X'000102' WHERE session_id='journal-fixture:dryrun-01'").execute(db.pool()).await.unwrap();
    let empty = SessionId::new("empty-legacy");
    fixture.ensure_session(&empty).await.unwrap();
    sqlx::query("INSERT INTO protected_agent_session_state (session_id,key_revision,state_revision,updated_at) VALUES ('journal-fixture:empty-legacy',1,1,'c')")
        .execute(db.pool()).await.unwrap();
    std::fs::write(fixture.directory().join("jwt_secret.bin"), secret).unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "session_journal_dryrun::journal_real_data_dry_run",
            "--nocapture",
        ])
        .env(
            "FORGE_JOURNAL_DRYRUN_DB",
            fixture.directory().join("journal.db"),
        )
        .env_remove("FORGE_JOURNAL_DRYRUN_SECRET_FILE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .expect(&stdout);
    assert_eq!(summary["converted"], 8);
    assert_eq!(summary["skipped"]["retirement_policy"], 1);
    assert_eq!(summary["parked_by_reason"]["Serialization"], 1);
    assert_eq!(summary["unreadable"]["conversion"], 1);
    assert_eq!(summary["remaining"], 0);
    assert!(summary["conversion_ticks"].as_u64().unwrap() >= 2);
    assert!(
        summary["bytes"]["before"]["legacy_dbstat"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        summary["bytes"]["after"]["journal_dbstat"]
            .as_u64()
            .unwrap()
            > 0
    );
    println!("{summary}");
}
