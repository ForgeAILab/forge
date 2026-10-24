use db::{create_sqlite_pool, run_migrations, SqliteDb};
use std::sync::Arc;

pub async fn fixture() -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    for sql in [
        "INSERT INTO agent_identity (id,name,created_at,updated_at) VALUES ('probe-agent','Probe','c','u')",
        "INSERT INTO agent_profile (id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('probe-profile','probe-agent','native','embedded','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('probe-scope','probe-agent','account','probe-owner','c','u')",
        "INSERT INTO agent_session (id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('probe-session','probe-agent','probe-profile','probe-scope','native','probe-runtime','suspended','c','u')",
    ] { sqlx::query(sql).execute(&pool).await.unwrap(); }
    Arc::new(SqliteDb::new(pool))
}

pub async fn native(db: &SqliteDb, role: &str, published: bool, checkpoint: bool, retired: bool) {
    let storage_id = if role == "protected" {
        db::SESSION_JOURNAL_PROTECTED_STORAGE_ID
    } else {
        db::SESSION_JOURNAL_ORDINARY_STORAGE_ID
    };
    // Invalid encrypted metadata deliberately proves the probe is purely scalar.
    sqlx::query("INSERT INTO agent_session_journal_session
        (storage_id,role,runtime_session_id,forge_session_id,generation,revision,writer_epoch,
         collection_dirty,published,has_checkpoint,retired,key_revision,format,nonce,payload)
        VALUES (?,?,'probe-runtime','probe-session',zeroblob(8),?,zeroblob(8),0,?,?,?,1,1,zeroblob(24),zeroblob(16))
        ON CONFLICT(storage_id,role,runtime_session_id) DO UPDATE SET revision=excluded.revision,
         published=excluded.published,has_checkpoint=excluded.has_checkpoint,retired=excluded.retired")
        .bind(storage_id).bind(role).bind(u64::from(published).to_be_bytes().as_slice())
        .bind(published).bind(checkpoint).bind(retired).execute(db.pool()).await.unwrap();
}

pub async fn legacy(db: &SqliteDb, checkpoint: bool) {
    sqlx::query(
        "INSERT INTO protected_agent_session_state
        (session_id,snapshot_ciphertext,snapshot_nonce,checkpoint_ciphertext,checkpoint_nonce,
         checkpoint_turn_id,key_revision,state_revision,updated_at)
        VALUES ('probe-session',zeroblob(16),zeroblob(24),?,?,?,1,1,'u')
        ON CONFLICT(session_id) DO UPDATE SET checkpoint_ciphertext=excluded.checkpoint_ciphertext,
         checkpoint_nonce=excluded.checkpoint_nonce,checkpoint_turn_id=excluded.checkpoint_turn_id",
    )
    .bind(checkpoint.then(|| vec![0u8; 16]))
    .bind(checkpoint.then(|| vec![0u8; 24]))
    .bind(checkpoint.then_some("turn"))
    .execute(db.pool())
    .await
    .unwrap();
}
