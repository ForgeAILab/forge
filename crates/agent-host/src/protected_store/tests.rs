//! Protected session, LCM ownership, permission, and legacy fixture tests.

use super::*;
pub(super) async fn test_store() -> (SqliteProtectedRuntimeStore, Arc<SqliteDb>) {
    use db::{User, UserRepo};

    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: "credential-owner".to_owned(),
            email: "credential-owner@example.com".to_owned(),
            password_hash: "hash".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("owner creates");
    (
        SqliteProtectedRuntimeStore::new(Arc::clone(&db), [7_u8; 32], 1),
        db,
    )
}

pub(super) async fn session_store() -> (SqliteProtectedRuntimeStore, Arc<SqliteDb>, SessionSnapshot)
{
    let (store, db) = test_store().await;
    let now = db::now_rfc3339();
    for statement in [
        "INSERT INTO agent_identity (id, name, created_at, updated_at)
             VALUES ('state-agent', 'State agent', ?, ?)",
        "INSERT INTO agent_profile (id, identity_id, backend_kind, executor_type, created_at, updated_at)
             VALUES ('state-profile', 'state-agent', 'native', 'embedded', ?, ?)",
        "INSERT INTO agent_context_scope (id, identity_id, scope_type, scope_id, created_at, updated_at)
             VALUES ('state-scope', 'state-agent', 'account', 'credential-owner', ?, ?)",
        "INSERT INTO agent_session (id, identity_id, profile_id, context_scope_id, backend_kind,
                                       runtime_session_id, created_at, updated_at)
             VALUES ('state-session', 'state-agent', 'state-profile', 'state-scope', 'native', 'state-runtime', ?, ?)",
    ] {
        sqlx::query(statement).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    }
    let snapshot = serde_json::from_value(serde_json::json!({
        "id": "state-runtime", "history": [], "updated": 0,
    }))
    .unwrap();
    (store, db, snapshot)
}

pub(super) async fn native_snapshot(
    store: &SqliteProtectedRuntimeStore,
    id: &SessionId,
) -> SessionSnapshot {
    use agent_runtime::core::journal::*;
    let journal = store.journals().ordinary();
    let lease = journal
        .open(
            id,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("fixture-read"),
                pin: JournalPinId::new("fixture-read"),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    let snapshot = JournalReader::new(
        &journal,
        &lease,
        JournalReadLimits {
            objects: 100_000,
            encoded_bytes: 64 * 1024 * 1024,
            entries: 100_000,
        },
    )
    .unwrap()
    .snapshot()
    .await
    .unwrap()
    .unwrap();
    journal.close(lease).await.unwrap();
    snapshot
}

fn session_checkpoint(mut snapshot: SessionSnapshot) -> TurnCheckpoint {
    let active_history_start = snapshot.history.len();
    snapshot
        .history
        .push(agent_runtime::core::content::Message::user("hello"));
    TurnCheckpoint::accepted(
        agent_runtime::core::ids::TurnId::new("state-turn"),
        agent_runtime::core::content::UserInput::text("hello"),
        snapshot,
        active_history_start,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap()
}

#[tokio::test]
async fn session_state_migration_uses_timestamp_version() {
    let (_, db, _) = session_store().await;
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM _migration WHERE name = 'protected_session_snapshot_digest'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(version, 202609302227);
}

#[tokio::test]
async fn legacy_read_policy_revision() {
    use agent_runtime::{core::store::VersionedSessionState, registry::RegistryRevision};
    let (store, db, mut snapshot) = session_store().await;
    snapshot.extension_state.insert(
        agent_runtime::harness::LCM_COMPONENT_ID.to_owned(),
        VersionedSessionState::new(
            RegistryRevision::new("live-lcm"),
            serde_json::json!({"live": true}),
        ),
    );
    let checkpoint = session_checkpoint(snapshot.clone());
    let snapshot = checkpoint.snapshot.clone();
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET lcm_policy_revision = 'forge-lcm-policy-2', checkpoint_lcm_policy_revision = 'forge-lcm-policy-2'")
        .execute(db.pool()).await.unwrap();
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot)
    );
    assert_eq!(
        CheckpointStore::load_latest(&store, &checkpoint.session)
            .await
            .unwrap(),
        Some(checkpoint)
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn lcm_binding_busy_is_retryable_and_retry_keeps_the_timeline() {
    use crate::test_support::{FileDb, LegacyLcmFixture};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    let f = FileDb::new().await.unwrap();
    let id = SessionId::new("busy-lcm-binding");
    f.ensure_session(&id).await.unwrap();
    let original = SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1);
    let old = LegacyLcmFixture::seed(&f, &original, &id, false).await;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(f.directory().join("journal.db"))
                .busy_timeout(std::time::Duration::ZERO),
        )
        .await
        .unwrap();
    let store = SqliteProtectedRuntimeStore::new(Arc::new(SqliteDb::new(pool)), [7; 32], 1);
    let held = db::begin_immediate(f.database().pool()).await.unwrap();
    let error = store
        .resolve_lcm_store(id.as_str(), "account", "journal-owner", false)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.retryable);
    held.rollback().await.unwrap();
    let resolved = store
        .resolve_lcm_store(id.as_str(), "account", "journal-owner", false)
        .await
        .unwrap();
    assert_eq!(resolved.timeline_id(), old.timeline_id);
    assert_eq!(
        SessionStore::load(&store, &id).await.unwrap().unwrap(),
        old.snapshot
    );
    assert_eq!(
        CheckpointStore::load_latest(&store, &id)
            .await
            .unwrap()
            .unwrap(),
        old.checkpoint
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn retired_chat_binding_resumes_without_shared_ownership_transfer() {
    use crate::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let id = SessionId::new("retired-chat-binding");
    let db = f.database();
    let store = SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1);
    let (old, _) = LegacyLcmFixture::target_owned_pre_topic_profile_switch(&f, &store, &id).await;
    let chat = format!("chat:{}", id.as_str());
    let retired = "2026-09-25T00:00:00Z";
    sqlx::query("UPDATE agent_lcm_timeline SET scope_id=canonical_scope_id || '#retired:' || id,retired_at=? WHERE id=?")
        .bind(retired).bind(&old.timeline_id).execute(db.pool()).await.unwrap();
    for keep_binding in [true, false] {
        if !keep_binding {
            sqlx::query("DELETE FROM agent_runtime_lcm_binding WHERE runtime_session_id=?")
                .bind(id.as_str())
                .execute(db.pool())
                .await
                .unwrap();
        }
        for _ in 0..2 {
            let resolved = store
                .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &chat)
                .await
                .unwrap();
            assert_eq!(resolved.timeline_id(), old.timeline_id);
            let state: (String, String, String, Option<String>, i64, String) = sqlx::query_as(
                "SELECT runtime_session_id,scope_id,canonical_scope_id,claim_owner,claim_generation,retired_at FROM agent_lcm_timeline WHERE id=?",
            ).bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
            assert_eq!(
                state,
                (
                    id.as_str().to_owned(),
                    format!("{chat}#retired:{}", old.timeline_id),
                    chat.clone(),
                    None,
                    0,
                    retired.to_owned()
                )
            );
            let receipts: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
            )
            .bind(&old.timeline_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(receipts, 0, "retired timelines bypass shared arbitration");
            let replaced: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agent_session WHERE replaced_by_session_id IS NOT NULL",
            )
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(replaced, 0);
        }
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn retired_chat_binding_rejects_foreign_owners_and_scopes() {
    use crate::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let id = SessionId::new("retired-chat-authorization");
    let db = f.database();
    let store = SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1);
    let (old, older) =
        LegacyLcmFixture::target_owned_pre_topic_profile_switch(&f, &store, &id).await;
    let chat = format!("chat:{}", id.as_str());
    for (runtime_owner, claim_owner, scope_type, canonical_scope) in [
        (older.as_str(), None, "agent_chat", chat.as_str()),
        (
            id.as_str(),
            Some(older.as_str()),
            "agent_chat",
            chat.as_str(),
        ),
        (id.as_str(), None, "account", chat.as_str()),
        (id.as_str(), None, "agent_chat", "another-chat"),
    ] {
        sqlx::query("UPDATE agent_lcm_timeline SET runtime_session_id=?,claim_owner=?,scope_type=?,canonical_scope_id=?,scope_id=? || '#retired:' || id,retired_at='2026-09-25T00:00:00Z' WHERE id=?")
            .bind(runtime_owner).bind(claim_owner).bind(scope_type).bind(canonical_scope).bind(canonical_scope)
            .bind(&old.timeline_id).execute(db.pool()).await.unwrap();
        let error = store
            .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &chat)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(
            error.message,
            "LCM timeline does not match its session binding"
        );
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn non_chat_empty_binding_continues_after_it_gains_entries() {
    use crate::test_support::FileDb;
    use agent_runtime::lcm::*;
    let f = FileDb::new().await.unwrap();
    for id in ["empty-source", "empty-successor"] {
        f.ensure_session(&SessionId::new(id)).await.unwrap();
    }
    let store = SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1);
    let source = store
        .resolve_lcm_store("empty-source", "account", "journal-owner", false)
        .await
        .unwrap();
    let successor = store
        .resolve_lcm_store("empty-successor", "account", "journal-owner", false)
        .await
        .unwrap();
    assert_eq!(successor.timeline_id(), source.timeline_id());
    successor
        .append(
            &successor.view(),
            LcmAppendRequest::new(
                LcmOperationId::new("continued-append"),
                vec![LcmEntry::new(
                    LcmTimelineId::new(successor.timeline_id()),
                    LcmEntryId::new("continued-entry"),
                    LcmSequence::new(0),
                    crate::Message::user("continued non-Chat history"),
                    LcmSourceMetadata::new(LcmClassification::new(
                        crate::Sensitivity::Sensitive,
                        crate::TrustClass::UserContent,
                    )),
                )],
            ),
        )
        .await
        .unwrap();
    let reopened = store
        .resolve_lcm_store("empty-successor", "account", "journal-owner", false)
        .await
        .unwrap();
    assert_eq!(reopened.timeline_id(), source.timeline_id());
    assert_eq!(
        reopened
            .current_revision(&reopened.view())
            .await
            .unwrap()
            .get(),
        1
    );
}

#[test]
fn lcm_binding_busy_and_locked_codes_are_retryable() {
    #[derive(Debug)]
    struct CodedSqliteError(u32);
    impl std::fmt::Display for CodedSqliteError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SQLite code {}", self.0)
        }
    }
    impl std::error::Error for CodedSqliteError {}
    impl sqlx::error::DatabaseError for CodedSqliteError {
        fn message(&self) -> &str {
            "synthetic SQLite error"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(self.0.to_string().into())
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
    for code in [5, 6, 261, 262, 517, 518] {
        let error =
            protected_database_error(sqlx::Error::Database(Box::new(CodedSqliteError(code))));
        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(error.retryable, "SQLite code {code}");
    }
    let constraint =
        protected_database_error(sqlx::Error::Database(Box::new(CodedSqliteError(19))));
    assert!(!constraint.retryable);
}

#[tokio::test]
async fn legacy_baseline_digest_is_keyed_and_timestamp_independent() {
    let (store, db, mut snapshot) = session_store().await;
    let digest = store.legacy_snapshot_digest(&snapshot).unwrap();
    let different_key = SqliteProtectedRuntimeStore::new(db.clone(), [8; 32], 1);
    assert_ne!(
        digest,
        different_key.legacy_snapshot_digest(&snapshot).unwrap()
    );
    // RFC 2104 HMAC, verified independently using its inner/outer pads.
    use sha2::{Digest, Sha256};
    let mut inner_pad = [0x36; 64];
    let mut outer_pad = [0x5c; 64];
    for index in 0..32 {
        inner_pad[index] ^= 7;
        outer_pad[index] ^= 7;
    }
    let bytes = serde_json::to_vec(&(
        &snapshot.id,
        &snapshot.history,
        &snapshot.usage,
        &snapshot.identity,
        &snapshot.extension_state,
    ))
    .unwrap();
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(b"forge/protected-session/snapshot-digest\0");
    inner.update(&bytes);
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner.finalize());
    assert_eq!(digest, hex::encode(outer.finalize()));
    snapshot.updated = Timestamp(123);
    assert_eq!(digest, store.legacy_snapshot_digest(&snapshot).unwrap());
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT snapshot_digest FROM protected_agent_session_state")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(stored, digest);
    snapshot
        .history
        .push(agent_runtime::core::content::Message::user("changed"));
    assert_ne!(digest, store.legacy_snapshot_digest(&snapshot).unwrap());
    let checkpoint = session_checkpoint(snapshot);
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT snapshot_digest FROM protected_agent_session_state")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored,
        store.legacy_snapshot_digest(&checkpoint.snapshot).unwrap()
    );
}

#[tokio::test]
async fn legacy_baseline_manifest_window_does_not_change_checkpoint_digest() {
    use agent_runtime::{
        core::{
            ids::TurnId,
            manifest::{CapabilityResolution, ModelResolution, RunManifest},
            provider::ModelId,
            store::{TurnManifest, VersionedSessionState},
        },
        registry::{Fingerprint, RegistryRevision},
        runtime::MANIFEST_BOUNDARY_NAMESPACE,
    };
    let (store, db, mut snapshot) = session_store().await;
    let fingerprint = Fingerprint::of(b"manifest-fixture");
    let manifest = RunManifest::new(
        fingerprint.clone(),
        fingerprint.clone(),
        ModelResolution::new(
            "fixture",
            ModelId::new("fixture-model"),
            fingerprint.clone(),
            Default::default(),
        ),
        CapabilityResolution::new(RegistryRevision::new("fixture-resolver")),
        fingerprint.clone(),
        fingerprint,
    );
    snapshot.manifests.push(TurnManifest::new(
        TurnId::new("earlier-turn"),
        manifest.clone(),
    ));
    snapshot.extension_state.insert(
        MANIFEST_BOUNDARY_NAMESPACE.to_owned(),
        VersionedSessionState::new(
            RegistryRevision::new("manifest-boundary-1"),
            serde_json::json!({"schema_version": 1, "planned_steps": 1}),
        ),
    );
    // The ordinary snapshot retains diagnostics and the protected boundary.
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot.clone())
    );
    let mut checkpoint = session_checkpoint(snapshot);
    let live = checkpoint.snapshot.clone();
    assert!(!live.manifests.is_empty());
    // Model Runtime's checkpoint capture, which preserves execution state
    // and its boundary extension but leaves diagnostics in the live window.
    checkpoint.snapshot.manifests.clear();
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    let loaded = SessionStore::load(&store, &live.id).await.unwrap().unwrap();
    // SessionStore references the checkpoint's execution state, whose
    // serialized snapshot omits the live diagnostic window.
    assert!(loaded.manifests.is_empty());
    assert_eq!(loaded.extension_state, live.extension_state);
    assert_eq!(
        store.legacy_snapshot_digest(&live).unwrap(),
        store.legacy_snapshot_digest(&loaded).unwrap()
    );
    let mut retained = live.clone();
    retained
        .manifests
        .push(TurnManifest::new(TurnId::new("latest-turn"), manifest));
    retained.manifests.remove(0);
    assert_eq!(
        store.legacy_snapshot_digest(&live).unwrap(),
        store.legacy_snapshot_digest(&retained).unwrap()
    );
    // Neither the restored state nor the live diagnostic window requires
    // a conflicting independent save over the checkpoint-backed row.
    sqlx::query("CREATE TRIGGER reject_state_write BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT, 'unexpected write'); END")
        .execute(db.pool()).await.unwrap();
    store.save_legacy_snapshot_baseline(&loaded).await.unwrap();
    store
        .save_legacy_snapshot_baseline(&retained)
        .await
        .unwrap();
    // The runtime-owned boundary remains protected even though manifests
    // are diagnostic: changing its frontier must change the digest.
    retained
        .extension_state
        .get_mut(MANIFEST_BOUNDARY_NAMESPACE)
        .unwrap()
        .value = serde_json::json!({"schema_version": 1, "planned_steps": 2});
    assert_ne!(
        store.legacy_snapshot_digest(&live).unwrap(),
        store.legacy_snapshot_digest(&retained).unwrap()
    );
}

#[tokio::test]
async fn legacy_read_checkpoint_decode_failure_names_session() {
    let (store, db, snapshot) = session_store().await;
    assert!(
        SessionStore::load(&store, &snapshot.id)
            .await
            .unwrap()
            .is_none()
    );
    let checkpoint = session_checkpoint(snapshot.clone());
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    let mut bad_format = SESSION_STATE_MAGIC.to_vec();
    bad_format.push(SESSION_STATE_FORMAT + 1);
    let (mut corrupt_ciphertext, nonce) = store.seal(b"{}").unwrap();
    corrupt_ciphertext[0] ^= 1;
    for (ciphertext, nonce) in [
        store.seal(b"invalid JSON").unwrap(),
        store.seal(&bad_format).unwrap(),
        (corrupt_ciphertext, nonce),
    ] {
        sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext = ?, checkpoint_nonce = ?")
            .bind(ciphertext).bind(nonce).execute(db.pool()).await.unwrap();
        let error = SessionStore::load(&store, &snapshot.id).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert!(error.message.contains(snapshot.id.as_str()), "{error}");
        let error = CheckpointStore::load_latest(&store, &snapshot.id)
            .await
            .unwrap_err();
        assert!(error.message.contains(snapshot.id.as_str()), "{error}");
    }
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_nonce = NULL")
        .execute(db.pool())
        .await
        .unwrap();
    let error = SessionStore::load(&store, &snapshot.id).await.unwrap_err();
    assert!(error.message.contains(snapshot.id.as_str()), "{error}");
    assert!(error.message.contains("incomplete"));
}

#[tokio::test]
async fn legacy_read_compressed_and_pre_envelope() {
    let (store, db, mut snapshot) = session_store().await;
    snapshot
        .history
        .push(agent_runtime::core::content::Message::user(
            "context ".repeat(100_000),
        ));
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let row = sqlx::query(
        "SELECT snapshot_ciphertext, snapshot_nonce FROM protected_agent_session_state",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let ciphertext: Vec<u8> = row.get("snapshot_ciphertext");
    let nonce: Vec<u8> = row.get("snapshot_nonce");
    assert!(ciphertext.len() < bytes.len() / 10);
    assert!(
        store
            .open(&ciphertext, &nonce)
            .unwrap()
            .starts_with(SESSION_STATE_MAGIC)
    );
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot.clone())
    );

    let (ciphertext, nonce) = store.seal(&bytes).unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET snapshot_ciphertext = ?, snapshot_nonce = ?, snapshot_digest = NULL")
        .bind(ciphertext).bind(nonce).execute(db.pool()).await.unwrap();
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot.clone())
    );
    sqlx::query("CREATE TRIGGER reject_state_write BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT, 'unexpected write'); END")
        .execute(db.pool()).await.unwrap();
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
}

#[tokio::test]
async fn legacy_baseline_unchanged_saves_do_not_write() {
    let (store, db, mut snapshot) = session_store().await;
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_state_write BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT, 'unexpected write'); END")
        .execute(db.pool()).await.unwrap();
    for updated in 1..4 {
        snapshot.updated = Timestamp(updated);
        store
            .clone()
            .save_legacy_snapshot_baseline(&snapshot)
            .await
            .unwrap();
    }
    sqlx::query("DROP TRIGGER reject_state_write")
        .execute(db.pool())
        .await
        .unwrap();
    snapshot
        .history
        .push(agent_runtime::core::content::Message::user("changed"));
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT state_revision FROM protected_agent_session_state")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn legacy_baseline_checkpoint_is_shared_idempotent_and_revision_checked() {
    use agent_runtime::core::{
        checkpoint::TurnState,
        content::{Message, UserInput},
        ids::TurnId,
    };
    let (store, db, mut snapshot) = session_store().await;
    snapshot.history.push(Message::user("hello"));
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let checkpoint = TurnCheckpoint::accepted(
        TurnId::new("state-turn"),
        UserInput::text("hello"),
        snapshot.clone(),
        0,
        Deadline::never(),
        1,
        0,
        Timestamp::ZERO,
    )
    .unwrap();
    // Rows from earlier Forge versions contain a standalone JSON checkpoint.
    let (ciphertext, nonce) = store
        .seal(&serde_json::to_vec(&checkpoint).unwrap())
        .unwrap();
    sqlx::query(
        "UPDATE protected_agent_session_state SET checkpoint_ciphertext = ?, checkpoint_nonce = ?",
    )
    .bind(ciphertext)
    .bind(nonce)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(
        CheckpointStore::load_latest(&store, &snapshot.id)
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    let row = sqlx::query("SELECT snapshot_ciphertext, snapshot_nonce, checkpoint_ciphertext FROM protected_agent_session_state")
        .fetch_one(db.pool()).await.unwrap();
    assert!(
        row.get::<Option<Vec<u8>>, _>("snapshot_ciphertext")
            .is_none()
    );
    assert!(row.get::<Option<Vec<u8>>, _>("snapshot_nonce").is_none());
    assert!(
        row.get::<Option<Vec<u8>>, _>("checkpoint_ciphertext")
            .is_some()
    );
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot.clone())
    );
    assert_eq!(
        CheckpointStore::load_latest(&store, &snapshot.id)
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );
    sqlx::query("CREATE TRIGGER reject_state_write BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT, 'unexpected write'); END")
        .execute(db.pool()).await.unwrap();
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    snapshot.updated = Timestamp(1);
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER reject_state_write")
        .execute(db.pool())
        .await
        .unwrap();
    let next = checkpoint
        .transition(
            TurnState::Planning { step: 0 },
            snapshot.clone(),
            1,
            Timestamp(1),
        )
        .unwrap();
    store.save_legacy_checkpoint_baseline(&next).await.unwrap();
    store
        .save_legacy_checkpoint_baseline(&checkpoint)
        .await
        .unwrap();
    let mut conflicting = next.clone();
    conflicting.operation_fingerprint = checkpoint.operation_fingerprint;
    store
        .save_legacy_checkpoint_baseline(&conflicting)
        .await
        .unwrap();
    assert_eq!(
        CheckpointStore::load_latest(&store, &next.session)
            .await
            .unwrap(),
        Some(next.clone())
    );
    // Independent extension/history progress must not replace the recovery checkpoint.
    snapshot.history.push(Message::user("later"));
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    assert_eq!(
        SessionStore::load(&store, &snapshot.id).await.unwrap(),
        Some(snapshot)
    );
    assert_eq!(
        CheckpointStore::load_latest(&store, &next.session)
            .await
            .unwrap(),
        Some(next)
    );
}

#[tokio::test]
async fn legacy_read_unknown_format_and_corruption() {
    let (store, _) = test_store().await;
    let mut payload = SESSION_STATE_MAGIC.to_vec();
    payload.push(SESSION_STATE_FORMAT + 1);
    let (ciphertext, nonce) = store.seal(&payload).unwrap();
    assert_eq!(
        store
            .open_session_state(ciphertext, nonce)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Serialization
    );
    let (mut ciphertext, nonce) = store.seal_session_state(b"{}".to_vec()).await.unwrap();
    ciphertext[0] ^= 1;
    assert_eq!(
        store
            .open_session_state(ciphertext, nonce)
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Serialization
    );
}

#[test]
fn effective_permission_intersection_fails_closed() {
    let mut effective = permission_set(r#"{"allowed":["read_project","task_write"]}"#);
    intersect_permissions(&mut effective, &permission_set(r#"["read_project"]"#));
    assert_eq!(effective, BTreeSet::from(["read_project".to_owned()]));

    let mut malformed = permission_set("not-json");
    intersect_permissions(
        &mut malformed,
        &scope_permission_set(
            crate::CanonicalScopeType::Project,
            crate::WorkspaceAccess::Deny,
            false,
            false,
        ),
    );
    assert!(malformed.is_empty());
}

#[test]
fn task_scope_ceiling_is_the_only_filesystem_policy() {
    let task = scope_permission_set(
        crate::CanonicalScopeType::Task,
        crate::WorkspaceAccess::TaskWrite,
        false,
        false,
    );
    assert!(task.contains("task_read"));
    assert!(task.contains("task_write"));

    let project = scope_permission_set(
        crate::CanonicalScopeType::Project,
        crate::WorkspaceAccess::Deny,
        false,
        false,
    );
    assert!(!project.contains("task_read"));
    assert!(!project.contains("task_write"));
}

#[test]
fn legacy_project_setup_scope_keeps_only_adoption_permission() {
    let setup = scope_permission_set(
        crate::CanonicalScopeType::Project,
        crate::WorkspaceAccess::Deny,
        true,
        true,
    );
    assert!(setup.contains("read_project"));
    assert!(setup.contains("propose_project"));
    assert!(setup.contains("propose_message"));
    assert!(!setup.contains("propose_task"));
    assert!(!setup.contains("propose_review"));
    assert!(!setup.contains("propose_session"));
}

#[derive(Debug)]
struct RuntimeV050FixtureSummaryModel {
    revision: agent_runtime::registry::RegistryRevision,
}

#[async_trait::async_trait]
impl agent_runtime::lcm::LcmSummaryModel for RuntimeV050FixtureSummaryModel {
    fn id(&self) -> &str {
        "fixture-summary"
    }

    fn revision(&self) -> &agent_runtime::registry::RegistryRevision {
        &self.revision
    }

    async fn summarize(
        &self,
        request: &agent_runtime::lcm::LcmSummaryModelRequest,
    ) -> std::result::Result<
        agent_runtime::lcm::LcmSummaryModelResponse,
        agent_runtime::lcm::LcmSummaryError,
    > {
        Ok(agent_runtime::lcm::LcmSummaryModelResponse {
            text: format!(
                "v0.5 resume summary for entries {} through {}.",
                request.source_range.start.get(),
                request.source_range.end.get()
            ),
            input_tokens: 120,
            output_tokens: 10,
        })
    }
}

fn fixture_leaf_commit(node: &agent_runtime::lcm::LcmNode) -> agent_runtime::lcm::LeafCommit {
    assert_eq!(node.kind, agent_runtime::lcm::LcmNodeKind::Leaf);
    let entry_ids = node
        .edges
        .iter()
        .map(|edge| match edge {
            agent_runtime::lcm::LcmEdge::Entry(id) => id.clone(),
            agent_runtime::lcm::LcmEdge::Node(_) => {
                panic!("old fixture leaf contains a node edge")
            }
        })
        .collect();
    agent_runtime::lcm::LeafCommit {
        expected_revision: agent_runtime::lcm::LcmRevision::new(
            node.revision.get().checked_sub(1).expect("leaf revision"),
        ),
        operation_id: node.operation_id.clone(),
        node_id: node.id.clone(),
        range: node.range,
        entry_ids,
        source_fingerprint: node.source_fingerprint.clone(),
        summary: node.summary.clone(),
        token_count: node.token_count,
        source_token_count: node.source_token_count,
        policy_revision: node.policy_revision.clone(),
        algorithm_revision: node.algorithm_revision.clone(),
        sizer_revision: node.sizer_revision.clone(),
        provenance: node.provenance.clone(),
        classification: node.classification.clone(),
        operation_fingerprint: Some(node.operation_fingerprint.clone()),
    }
}

/// The fixture was generated by Forge at agent-runtime 5ee41e4. It carries
/// exact encrypted Forge snapshot/checkpoint envelopes plus the LCM entries
/// and leaf nodes written during four completed turns. Resuming it rebuilds
/// the `-2` LCM algorithm state to `-3` without touching the old nodes, a
/// reasoning turn then stores the v0.5.0 producer field, and a second resume
/// continues without another rebuild.
#[tokio::test]
async fn runtime_5ee41e4_fixture_resumes_through_forge_stores() {
    use agent_runtime::{
        core::{
            content::{ContentPart, Message, ReasoningProducer, UserInput},
            ids::SessionId,
            provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent},
            store::SessionStore,
        },
        harness::LCM_COMPONENT_ID,
        lcm::{
            LcmAppendRequest, LcmEntry, LcmNode, LcmOperationId, LcmRange, LcmReader, LcmSequence,
            LcmWriter,
        },
        prelude::CheckpointStore,
        provider::fake::{FakeProvider, ScriptedStream},
        registry::RegistryRevision,
        runtime::StartSession,
    };

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/runtime_5ee41e4_session_checkpoint_lcm.json"
    ))
    .expect("old runtime fixture parses");
    assert_eq!(
        fixture["producer_revision"],
        "5ee41e4e2e830aaf17dd6b1baa4f50f58b1a1894"
    );
    assert_eq!(fixture["checkpoint_schema"], 3);
    assert_eq!(fixture["turn_transition_revision"], 4);
    assert_eq!(fixture["lcm_algorithm_revision"], "agent-runtime-lcm-2");

    let (store, db, _) = session_store().await;
    let runtime_session_id = "runtime-v050-old-session";
    sqlx::query("UPDATE agent_session SET runtime_session_id = ? WHERE id = 'state-session'")
        .bind(runtime_session_id)
        .execute(db.pool())
        .await
        .expect("fixture runtime id installs");

    let timeline = &fixture["timeline"];
    let timeline_id = timeline["id"].as_str().expect("timeline id");
    let binding_revision = timeline["binding_revision"]
        .as_str()
        .expect("binding revision");
    let now = db::now_rfc3339();
    sqlx::query(
        "INSERT INTO agent_lcm_timeline (
                id, identity_id, scope_type, scope_id, canonical_scope_id,
                runtime_session_id, authorization_revision, revision,
                claim_owner, claim_generation, created_at, updated_at
             ) VALUES (?, 'state-agent', 'account', ?, 'credential-owner', ?, ?, 0, NULL, 0, ?, ?)",
    )
    .bind(timeline_id)
    .bind(format!("credential-owner#topic:{runtime_session_id}"))
    .bind(runtime_session_id)
    .bind(binding_revision)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("fixture timeline creates");
    sqlx::query(
        "INSERT INTO agent_runtime_lcm_binding (runtime_session_id, timeline_id) VALUES (?, ?)",
    )
    .bind(runtime_session_id)
    .bind(timeline_id)
    .execute(db.pool())
    .await
    .expect("fixture timeline binds");

    let lcm_store = store
        .lcm_store_for_runtime_session(runtime_session_id, "account", "credential-owner")
        .await
        .expect("Forge resolves fixture LCM store");
    let session_id = SessionId::new(runtime_session_id);
    let view = lcm_store.view();
    lcm_store
        .claim(&view, &session_id, 0)
        .await
        .expect("fixture timeline claims");
    let owned_view = view.with_owner(session_id.clone(), 0);
    let entries: Vec<LcmEntry> =
        serde_json::from_value(timeline["entries"].clone()).expect("old LCM entries decode");
    let nodes: Vec<LcmNode> =
        serde_json::from_value(timeline["nodes"].clone()).expect("old LCM nodes decode");
    assert_eq!(entries.len(), 8);
    assert_eq!(nodes.len(), 2);

    for entry in entries.iter().take(7) {
        lcm_store
            .append(
                &owned_view,
                LcmAppendRequest::new(
                    LcmOperationId::new(format!("fixture-append-{}", entry.sequence.get())),
                    vec![entry.clone()],
                ),
            )
            .await
            .expect("old LCM entry appends through Forge");
    }
    lcm_store
        .commit_leaf(&owned_view, fixture_leaf_commit(&nodes[0]))
        .await
        .expect("first old LCM leaf commits through Forge");
    lcm_store
        .append(
            &owned_view,
            LcmAppendRequest::new(
                LcmOperationId::new("fixture-append-7"),
                vec![entries[7].clone()],
            ),
        )
        .await
        .expect("final old LCM entry appends through Forge");
    lcm_store
        .commit_leaf(&owned_view, fixture_leaf_commit(&nodes[1]))
        .await
        .expect("second old LCM leaf commits through Forge");
    assert_eq!(
        lcm_store
            .current_revision(&owned_view)
            .await
            .expect("fixture revision reads")
            .get(),
        timeline["revision"].as_u64().expect("timeline revision")
    );

    let protected = &fixture["protected_row"];
    let decode = |field: &str| {
        hex::decode(
            protected[field]
                .as_str()
                .unwrap_or_else(|| panic!("{field} is present")),
        )
        .unwrap_or_else(|_| panic!("{field} is hex"))
    };
    sqlx::query(
        "INSERT INTO protected_agent_session_state (
                session_id, snapshot_ciphertext, snapshot_nonce, snapshot_digest,
                checkpoint_ciphertext, checkpoint_nonce, checkpoint_turn_id,
                checkpoint_revision, checkpoint_fingerprint, key_revision,
                lcm_policy_revision, checkpoint_lcm_policy_revision,
                state_revision, updated_at
             ) VALUES ('state-session', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(decode("snapshot_ciphertext"))
    .bind(decode("snapshot_nonce"))
    .bind(protected["snapshot_digest"].as_str())
    .bind(decode("checkpoint_ciphertext"))
    .bind(decode("checkpoint_nonce"))
    .bind(protected["checkpoint_turn_id"].as_str())
    .bind(protected["checkpoint_revision"].as_i64())
    .bind(protected["checkpoint_fingerprint"].as_str())
    .bind(protected["key_revision"].as_i64())
    .bind(protected["lcm_policy_revision"].as_str())
    .bind(protected["checkpoint_lcm_policy_revision"].as_str())
    .bind(protected["state_revision"].as_i64())
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("old encrypted session state installs unchanged");

    let loaded = SessionStore::load(&store, &session_id)
        .await
        .expect("v0.5.0 loads old Forge snapshot")
        .expect("old snapshot exists");
    assert_eq!(
        loaded.history.len(),
        fixture["expected"]["history_len"]
            .as_u64()
            .expect("history length") as usize
    );
    assert_eq!(
        loaded.history.first().map(|message| message.joined_text()),
        fixture["expected"]["first_message"]
            .as_str()
            .map(str::to_owned)
    );
    // The decrypted old state really carries the `-2` marker, so the
    // rebuild asserted after resume is observable rather than vacuous.
    let old_algorithm = RegistryRevision::from_content("agent-runtime-lcm-2");
    let new_algorithm = RegistryRevision::from_content(agent_runtime::lcm::LCM_ALGORITHM_REVISION);
    assert_ne!(old_algorithm, new_algorithm);
    let algorithm = |snapshot: &agent_runtime::core::store::SessionSnapshot| {
        snapshot.extension_state[LCM_COMPONENT_ID].value["algorithm_revision"].clone()
    };
    assert_eq!(algorithm(&loaded), old_algorithm.as_str());
    let checkpoint = CheckpointStore::load_latest(&store, &session_id)
        .await
        .expect("v0.5.0 loads old Forge checkpoint")
        .expect("old checkpoint exists");
    checkpoint.validate().expect("old checkpoint validates");
    assert!(checkpoint.state.is_terminal());

    let reader = store
        .lcm_store_for_runtime_session(runtime_session_id, "account", "credential-owner")
        .await
        .expect("Forge resolves a reader for the fixture timeline");
    let node_texts = |nodes: &[LcmNode]| {
        nodes
            .iter()
            .map(|node| {
                (
                    node.id.as_str().to_owned(),
                    node.summary.clone(),
                    node.algorithm_revision.as_str().to_owned(),
                )
            })
            .collect::<Vec<_>>()
    };
    let old_nodes = node_texts(&nodes);

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::ReasoningDelta {
                text: "checking the old session".into(),
                redacted: false,
                signature: Some("sig-resume".into()),
            },
            ProviderStreamEvent::TextDelta {
                text: "resumed under runtime v0.5.0".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let runtime = runtime_v050_fixture_runtime(&store, runtime_session_id, provider.clone()).await;
    let session = runtime
        .start_session(StartSession::resume(session_id.clone()))
        .await
        .expect("v0.5.0 resumes old session/checkpoint/timeline");
    assert!(session.resumed());
    assert!(provider.requests().is_empty());
    assert_eq!(algorithm(&session.snapshot()), new_algorithm.as_str());
    // Resume rebuilds derived metadata only: the old nodes keep their ids,
    // text and `-2` algorithm provenance.
    assert_eq!(
        node_texts(
            &reader
                .active_nodes(&reader.view())
                .await
                .expect("old nodes read after resume")
        ),
        old_nodes
    );

    session
        .run(UserInput::text("continue the old session"))
        .await
        .expect("one mock-provider turn resumes old stored state");
    session.persist().await.expect("resumed session persists");
    assert_eq!(provider.requests().len(), 1);
    assert!(session.history().iter().any(|message| {
        message
            .joined_text()
            .contains("resumed under runtime v0.5.0")
    }));

    // v0.5.0 stamps its producer on new reasoning, in both the encrypted
    // snapshot and the SQLite LCM entry whose fingerprint covers it. The
    // 5ee41e4 runtime drops the field and can no longer verify such an
    // entry, which is why the upgrade is one-way for chats (CHANGELOG).
    let producer = ReasoningProducer {
        provider: "fake".into(),
        model: ModelId::new("fake"),
    };
    let has_producer = |message: &Message| {
        message.content.iter().any(|part| {
            matches!(
                part,
                ContentPart::Reasoning { producer: Some(recorded), .. } if *recorded == producer
            )
        })
    };
    let saved = native_snapshot(&store, &session_id).await;
    assert!(saved.history.iter().any(has_producer));
    assert_eq!(algorithm(&saved), new_algorithm.as_str());
    let last = u64::try_from(saved.history.len() - 1).expect("history index");
    let stored = reader
        .load_range(
            &reader.view(),
            LcmRange::new(LcmSequence::new(0), LcmSequence::new(last)).expect("entry range"),
            saved.history.len(),
        )
        .await
        .expect("LCM entries reload");
    assert_eq!(stored.len(), saved.history.len());
    assert!(stored.iter().any(|entry| has_producer(&entry.content)));
    drop(session);
    drop(runtime);

    // A second resume of the rebuilt, producer-bearing state needs no
    // further rebuild and still continues.
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "second resume under runtime v0.5.0".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let runtime = runtime_v050_fixture_runtime(&store, runtime_session_id, provider.clone()).await;
    let session = runtime
        .start_session(StartSession::resume(session_id.clone()))
        .await
        .expect("a second resume after the rebuild succeeds");
    assert!(session.resumed());
    assert!(provider.requests().is_empty());
    assert_eq!(session.history(), saved.history);
    assert_eq!(algorithm(&session.snapshot()), new_algorithm.as_str());
    session
        .run(UserInput::text("continue again"))
        .await
        .expect("a second mock-provider turn runs");
    assert_eq!(provider.requests().len(), 1);
    assert!(session.history().iter().any(|message| {
        message
            .joined_text()
            .contains("second resume under runtime v0.5.0")
    }));
}

pub(super) async fn runtime_v050_fixture_runtime(
    store: &SqliteProtectedRuntimeStore,
    runtime_session_id: &str,
    provider: Arc<agent_runtime::provider::fake::FakeProvider>,
) -> agent_runtime::runtime::Runtime {
    use agent_runtime::{
        core::{
            catalog::{ModelLimits, ResolvedModelProfile},
            ids::SessionId,
            provider::ModelId,
        },
        harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
        lcm::LcmPressurePolicy,
        registry::RegistryRevision,
        runtime::RuntimeBuilder,
    };

    let lcm_store = store
        .lcm_store_for_runtime_session(runtime_session_id, "account", "credential-owner")
        .await
        .expect("Forge resolves fixture LCM store");
    let binding = lcm_store
        .runtime_binding(SessionId::new(runtime_session_id))
        .expect("fixture binding builds");
    let coordinator = LcmCoordinator::new(
        Arc::new(lcm_store),
        Arc::new(RuntimeV050FixtureSummaryModel {
            revision: RegistryRevision::new("fixture-summary-1"),
        }),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 300,
            pressure: LcmPressurePolicy {
                revision: RegistryRevision::new("fixture-pressure-1"),
                soft_threshold_percent: 20,
                hard_threshold_percent: 95,
                leaf_target_tokens: 20,
                condensation_fanout: 2,
                retain_recent_entries: 2,
                max_rounds: 8,
                deterministic_token_cap: 16,
            },
            ..LcmCoordinatorPolicy::default()
        },
    )
    .expect("v0.5.0 LCM coordinator builds");
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(8_000, 8_000, 1_000),
        ))
        .session_store(Arc::new(store.clone()))
        .checkpoint_store(Arc::new(store.clone()))
        .lcm(Arc::new(coordinator))
        .build()
        .expect("v0.5.0 runtime builds")
}
