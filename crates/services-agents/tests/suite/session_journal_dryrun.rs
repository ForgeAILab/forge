//! Explicitly invoked conversion on a disposable, offline database copy.
use forge_agent_host::SqliteProtectedRuntimeStore;
use services_agents::{
    session_journal_conversion::{
        ConversionCursor, ParkedSession, SessionJournalConversion, PARKED_SESSION_REPORT_LIMIT,
    },
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

async fn metadata_rows(
    pool: &sqlx::SqlitePool,
    query: &str,
    session: &ParkedSession,
) -> TestResult<Vec<serde_json::Value>> {
    let rows: Vec<String> = sqlx::query_scalar(query)
        .bind(&session.forge_session_id)
        .bind(&session.runtime_session_id)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?)
}

async fn parked_session_facts(
    pool: &sqlx::SqlitePool,
    session: &ParkedSession,
) -> TestResult<serde_json::Value> {
    // Explicit metadata allowlists: no ciphertext, nonces, transcripts, topic
    // labels/summaries, authority JSON, workspace paths, or rotation tokens.
    let legacy = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',s.id,'runtime_session_id',s.runtime_session_id,
            'status',s.status,'version',s.version,'backend_kind',s.backend_kind,
            'identity_id',s.identity_id,'profile_id',s.profile_id,'context_scope_id',s.context_scope_id,
            'scope_type',c.scope_type,'scope_id',c.scope_id,'project_id',c.project_id,
            'task_id',c.task_id,'task_role',c.task_role,'chat_kind',chat.kind,
            'predecessor_session_id',s.predecessor_session_id,'replaced_by_session_id',s.replaced_by_session_id,
            'legacy_state_present',l.session_id IS NOT NULL,'state_revision',l.state_revision,
            'checkpoint_revision',l.checkpoint_revision,'key_revision',l.key_revision,
            'snapshot_present',l.snapshot_ciphertext IS NOT NULL,
            'checkpoint_present',l.checkpoint_ciphertext IS NOT NULL)
         FROM agent_session s
         LEFT JOIN protected_agent_session_state l ON l.session_id=s.id
         LEFT JOIN agent_context_scope c ON c.id=s.context_scope_id
         LEFT JOIN agent_chat chat ON c.scope_type='agent_chat' AND chat.id=c.scope_id
         WHERE s.id=?1 OR s.runtime_session_id=?2 ORDER BY s.id", session).await?;
    let native = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',forge_session_id,'runtime_session_id',runtime_session_id,
            'storage_id',storage_id,'role',role,'published',published,'retired',retired,
            'has_checkpoint',has_checkpoint,'collection_dirty',collection_dirty,
            'generation_hex',hex(generation),'revision_hex',hex(revision),
            'writer_epoch_hex',hex(writer_epoch),'layout_generation',layout_generation)
         FROM agent_session_journal_session
         WHERE runtime_session_id=?2 OR forge_session_id=?1 ORDER BY storage_id,role,runtime_session_id",
        session).await?;
    let linked = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',s.id,'runtime_session_id',s.runtime_session_id,
            'status',s.status,'version',s.version,'context_scope_id',s.context_scope_id,
            'predecessor_session_id',s.predecessor_session_id,'replaced_by_session_id',s.replaced_by_session_id)
         FROM agent_session s WHERE s.predecessor_session_id=?1 OR s.replaced_by_session_id=?1
            OR s.id IN (SELECT predecessor_session_id FROM agent_session WHERE id=?1 AND runtime_session_id=?2
                        UNION SELECT replaced_by_session_id FROM agent_session WHERE id=?1 AND runtime_session_id=?2)
         ORDER BY s.id", session).await?;
    let rotations = metadata_rows(pool,
        "SELECT json_object(
            'rotation_id',r.id,'chat_id',r.chat_id,'source_session_id',r.source_session_id,
            'successor_session_id',r.successor_session_id,'successor_runtime_id',r.successor_runtime_id,
            'rotation_pending',r.rotation_pending,'attempt_count',r.attempt_count,'last_error_kind',r.last_error_kind)
         FROM agent_chat_topic_rotation r
         WHERE r.source_session_id=?1 OR r.successor_session_id=?1 OR r.successor_runtime_id=?2
            OR r.successor_session_id IN (SELECT id FROM agent_session WHERE predecessor_session_id=?1)
            OR r.chat_id IN (SELECT c.scope_id FROM agent_session s
                JOIN agent_context_scope c ON c.id=s.context_scope_id
                WHERE s.id=?1 AND c.scope_type='agent_chat')
         ORDER BY r.id", session).await?;
    Ok(serde_json::json!({
        "legacy_rows":legacy,"native_journal":{
            "exists":native.iter().any(|s| s["runtime_session_id"] == session.runtime_session_id),
            "sessions":native},
        "linked_sessions":linked,"rotations":rotations
    }))
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
    let mut parked_sessions = Vec::new();
    let mut parked_sessions_omitted = 0;
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
        for session in report.parked_sessions {
            if parked_sessions.len() < PARKED_SESSION_REPORT_LIMIT {
                let facts = parked_session_facts(&pool, &session).await?;
                let mut diagnostic = serde_json::to_value(session)?;
                diagnostic["facts"] = facts;
                parked_sessions.push(diagnostic);
            } else {
                parked_sessions_omitted += 1;
            }
        }
        if report.remaining == 0 {
            break;
        }
        if ticks > initial.saturating_mul(2).saturating_add(2) {
            return Err(
                "journal dry run did not converge; active sessions or retryable errors remain"
                    .into(),
            );
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
        "parked_sessions":parked_sessions,"parked_sessions_omitted":parked_sessions_omitted,
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "forge_session_journal=debug",
        ))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()?;
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
    // A mismatched host/runtime id is a genuine validation conflict. Keep
    // predecessor and pending rotation links available for the diagnostic.
    sqlx::query("UPDATE agent_session SET runtime_session_id='dryrun-conflict',version=7,predecessor_session_id='journal-fixture:dryrun-00' WHERE id='journal-fixture:dryrun-02'")
        .execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_session SET replaced_by_session_id='journal-fixture:dryrun-02' WHERE id='journal-fixture:dryrun-00'")
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_topic_rotation (id,chat_id,source_session_id,successor_session_id,successor_runtime_id,label,cause,created_at) VALUES ('dryrun-rotation','chat:dryrun-02','journal-fixture:dryrun-02','dryrun-successor','dryrun-successor-runtime','private topic label','idle','c')")
        .execute(db.pool()).await.unwrap();
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
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("background journal conversion parked until restart"),
        "{stderr}"
    );
    assert!(stderr.contains("forge_session_journal"), "{stderr}");
    eprint!("{stderr}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .expect(&stdout);
    assert_eq!(summary["converted"], 7);
    assert_eq!(summary["skipped"]["retirement_policy"], 1);
    assert_eq!(summary["parked_by_reason"]["Serialization"], 1);
    assert_eq!(summary["parked_by_reason"]["Conflict"], 1);
    assert_eq!(summary["parked_sessions_omitted"], 0);
    let parked = summary["parked_sessions"].as_array().unwrap();
    assert_eq!(parked.len(), 2);
    let conflict = parked
        .iter()
        .find(|s| s["error_kind"] == "Conflict")
        .unwrap();
    assert_eq!(conflict["runtime_session_id"], "dryrun-conflict");
    assert_eq!(conflict["owner_kind"], "agent_chat");
    assert_eq!(
        conflict["error_message"],
        "session store returned another session's state"
    );
    let facts = &conflict["facts"];
    assert_eq!(facts["legacy_rows"][0]["status"], "suspended");
    assert_eq!(facts["legacy_rows"][0]["version"], 7);
    assert_eq!(
        facts["legacy_rows"][0]["context_scope_id"],
        "scope:dryrun-02"
    );
    assert_eq!(
        facts["legacy_rows"][0]["predecessor_session_id"],
        "journal-fixture:dryrun-00"
    );
    assert_eq!(
        facts["linked_sessions"][0]["replaced_by_session_id"],
        "journal-fixture:dryrun-02"
    );
    assert_eq!(facts["rotations"][0]["rotation_id"], "dryrun-rotation");
    assert_eq!(facts["native_journal"]["exists"], true);
    let native = facts["native_journal"]["sessions"].as_array().unwrap();
    assert_eq!(native.len(), 2);
    assert!(native
        .iter()
        .all(|s| s["published"] == 0 && s["retired"] == 0));
    assert!(!stdout.contains("accepted legacy input"));
    assert!(!stdout.contains("private topic label"));
    assert!(!stdout.contains(std::str::from_utf8(secret).unwrap()));
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

#[tokio::test]
async fn dry_run_converts_pre_topic_main_chat_lcm() {
    use forge_agent_host::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let secret = b"synthetic-historical-lcm-secret";
    let key: [u8; 32] = Sha256::digest(secret).into();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), key, 1));
    let id = super::session_journal_conversion::seed(&f, &store, "dryrun-pre-topic-main").await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    old.bind_empty_replacement(&f).await;
    let before: (String,String) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    let secret_path = f.directory().join("jwt_secret.bin");
    std::fs::write(&secret_path, secret).unwrap();
    let summary = run_copy(&f.directory().join("journal.db"), &secret_path)
        .await
        .unwrap();
    assert_eq!(summary["converted"], 1);
    assert_eq!(summary["remaining"], 0);
    assert_eq!(summary["parked_by_reason"], serde_json::json!({}));
    assert_eq!(summary["parked_sessions"], serde_json::json!([]));
    let after: (String,String) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(after, before);
    let transcript: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message")
        .fetch_one(f.database().pool())
        .await
        .unwrap();
    assert_eq!(transcript, 120);
}

#[tokio::test]
async fn dry_run_facts_use_read_only_queries_without_native_state() {
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let store = SqliteProtectedRuntimeStore::new(fixture.database(), [7; 32], 1);
    super::session_journal_conversion::seed(&fixture, &store, "metadata-only").await;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(fixture.directory().join("journal.db"))
                .read_only(true),
        )
        .await
        .unwrap();
    let facts = parked_session_facts(
        &pool,
        &ParkedSession {
            forge_session_id: "journal-fixture:metadata-only".into(),
            runtime_session_id: "metadata-only".into(),
            owner_kind: "agent_chat".into(),
            error_kind: "Conflict".into(),
            error_message: "fixture diagnostic".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(facts["native_journal"]["exists"], false);
    assert_eq!(facts["native_journal"]["sessions"], serde_json::json!([]));
    assert_eq!(facts["legacy_rows"][0]["scope_type"], "agent_chat");
    assert_eq!(facts["legacy_rows"][0]["state_revision"], 1);
    assert_eq!(facts["rotations"], serde_json::json!([]));
    assert_eq!(facts["linked_sessions"], serde_json::json!([]));
    pool.close().await;
}

#[tokio::test]
async fn dry_run_caps_parked_details_across_conversion_ticks() {
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let db = fixture.database();
    let secret = b"capped-diagnostics-fixture-secret";
    let store = SqliteProtectedRuntimeStore::new(db.clone(), Sha256::digest(secret).into(), 1);
    let count = PARKED_SESSION_REPORT_LIMIT + 3;
    for n in 0..count {
        super::session_journal_conversion::seed(&fixture, &store, &format!("capped-{n:02}")).await;
    }
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=X'000102'")
        .execute(db.pool())
        .await
        .unwrap();
    let secret_path = fixture.directory().join("jwt_secret.bin");
    std::fs::write(&secret_path, secret).unwrap();
    let summary = run_copy(&fixture.directory().join("journal.db"), &secret_path)
        .await
        .unwrap();
    assert_eq!(summary["parked_by_reason"]["Serialization"], count);
    assert_eq!(
        summary["parked_sessions"].as_array().unwrap().len(),
        PARKED_SESSION_REPORT_LIMIT
    );
    assert_eq!(summary["parked_sessions_omitted"], 3);
    assert_eq!(summary["remaining"], 0);
}
