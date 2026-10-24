use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::CheckpointStore,
        content::UserInput,
        ids::SessionId,
        journal::*,
        provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent},
        store::{SessionSnapshot, SessionStore},
    },
    provider::fake::{FakeProvider, ScriptedStream},
    runtime::{Runtime, RuntimeBuilder, StartSession},
};
use db::AgentSessionRepo;
use forge_agent_host::{test_support::FileDb, SqliteProtectedRuntimeStore};
use services_agents::session_journal_retention::SessionJournalRetention;
use std::sync::Arc;

fn host(id: &str) -> String {
    format!("journal-fixture:{id}")
}
fn runtime(store: Arc<SqliteProtectedRuntimeStore>) -> Runtime {
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "retained reply".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(store.clone())
        .checkpoint_store(store)
        .build()
        .unwrap()
}
async fn publish(f: &FileDb, store: Arc<SqliteProtectedRuntimeStore>, id: &str) {
    f.ensure_session(&SessionId::new(id)).await.unwrap();
    let rt = runtime(store);
    let session = rt
        .start_session(StartSession::create(SessionId::new(id), vec![]))
        .await
        .unwrap();
    session
        .run(UserInput::text("retained conversation"))
        .await
        .unwrap()
        .completed()
        .await;
    session.shutdown().await.unwrap();
}
async fn retired(store: &SqliteProtectedRuntimeStore, id: &str) -> [bool; 2] {
    let journals = store.journals();
    let mut out = [false; 2];
    for (i, engine) in [journals.protected(), journals.ordinary()]
        .iter()
        .enumerate()
    {
        out[i] = journals
            .storage()
            .load(
                &JournalDomain {
                    storage_id: engine.storage_id(),
                    role: if i == 0 {
                        JournalRole::Protected
                    } else {
                        JournalRole::Ordinary
                    },
                },
                &SessionId::new(id),
            )
            .await
            .unwrap()
            .unwrap()
            .retirement
            .is_some();
    }
    out
}
async fn update(f: &FileDb, id: &str, scope: &str, profile: &str, status: &str, created: &str) {
    sqlx::query("UPDATE agent_session SET context_scope_id=?,profile_id=?,status=?,created_at=?,version=version+1 WHERE id=?")
        .bind(scope).bind(profile).bind(status).bind(created).bind(host(id)).execute(f.database().pool()).await.unwrap();
}

#[tokio::test]
async fn unresumable_and_dormant_restart() {
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    for sql in [
        "INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES ('retention-owner','retention@example.test','hash','c','u')",
        "INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES ('retention-chat','account_main','retention-owner','ready','c','u')",
        "INSERT INTO agent_profile (id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('profile-two','journal-agent','native','embedded','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('chat-scope','journal-agent','agent_chat','retention-chat','c','u')",
        "INSERT INTO project (id,name,settings,workflow_definition,created_at,updated_at) VALUES ('retention-project','Retention','{}','{}','c','u')",
        "INSERT INTO task (id,project_id,title,status,created_at,updated_at) VALUES ('retention-task','retention-project','Done task','done','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,project_id,task_id,task_role,workspace_access,created_at,updated_at) VALUES ('task-scope','journal-agent','task','retention-task','retention-project','retention-task','worker','task_write','c','u')",
    ] {sqlx::query(sql).execute(f.database().pool()).await.unwrap();}
    for id in [
        "chat-one-old",
        "chat-one-dormant",
        "chat-two-old",
        "chat-two-dormant",
        "restart-account",
        "restart-task",
        "reusable-task",
        "replaced",
        "partial",
        "active-writer",
    ] {
        publish(&f, store.clone(), id).await;
    }
    for (id, profile, created) in [
        ("chat-one-old", "journal-profile", "1"),
        ("chat-one-dormant", "journal-profile", "2"),
        ("chat-two-old", "profile-two", "3"),
        ("chat-two-dormant", "profile-two", "4"),
    ] {
        update(&f, id, "chat-scope", profile, "suspended", created).await;
    }
    update(
        &f,
        "restart-account",
        "journal-scope",
        "journal-profile",
        "ready",
        "5",
    )
    .await;
    update(
        &f,
        "restart-task",
        "task-scope",
        "journal-profile",
        "ready",
        "6",
    )
    .await;
    assert_eq!(
        f.database()
            .suspend_stale_native_sessions("restart")
            .await
            .unwrap(),
        2
    );
    // Created/ready after the startup sweep: Task completion is not retirement.
    update(
        &f,
        "reusable-task",
        "task-scope",
        "journal-profile",
        "ready",
        "7",
    )
    .await;
    update(
        &f,
        "replaced",
        "chat-scope",
        "journal-profile",
        "replaced",
        "0",
    )
    .await;
    let partial = store.journals().protected();
    let record = store
        .journals()
        .storage()
        .load(
            &JournalDomain {
                storage_id: partial.storage_id(),
                role: JournalRole::Protected,
            },
            &SessionId::new("partial"),
        )
        .await
        .unwrap()
        .unwrap();
    partial
        .authorize_retirement(
            &SessionId::new("partial"),
            record.revision,
            agent_runtime::core::clock::Timestamp(1),
        )
        .await
        .unwrap();
    partial
        .retire(&SessionId::new("partial"), record.revision)
        .await
        .unwrap();
    // Real active runtime writer while the host row looks abandoned. The shared
    // engine must prevent controls from retiring it.
    let rt = runtime(store.clone());
    let active = rt
        .start_session(StartSession::resume(SessionId::new("active-writer")))
        .await
        .unwrap();
    let legacy_id = SessionId::new("legacy-only-dead");
    f.ensure_session(&legacy_id).await.unwrap();
    let snapshot: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":legacy_id,"history":[],"updated":1}))
            .unwrap();
    let legacy_snapshot = snapshot.clone();
    let legacy = serde_json::to_vec(&snapshot).unwrap();
    store
        .seed_legacy_bytes(&legacy_id, Some(&legacy), None, true)
        .await
        .unwrap();
    // A stale legacy blob must not become fallback after native retirement.
    let stale = SessionSnapshot {
        id: SessionId::new("restart-account"),
        ..snapshot
    };
    store
        .seed_legacy_bytes(
            &stale.id,
            Some(&serde_json::to_vec(&stale).unwrap()),
            None,
            true,
        )
        .await
        .unwrap();
    let policy = SessionJournalRetention::new(f.database(), &store);
    for (profile, id) in [
        ("journal-profile", "chat-one-dormant"),
        ("profile-two", "chat-two-dormant"),
    ] {
        assert_eq!(
            f.database()
                .dormant_native_chat_session_id("journal-agent", "chat-scope", profile)
                .await
                .unwrap(),
            Some(host(id))
        );
        assert!(!policy.should_retire(&host(id)).await.unwrap());
    }
    let page = policy.retire_page("").await.unwrap();
    assert_eq!(page.selected, 7);
    assert_eq!(page.retired_domains, 11);
    assert_eq!(page.conflicts, 1);
    assert!(policy
        .retire_page(page.after.as_deref().unwrap())
        .await
        .unwrap()
        .after
        .is_none());
    for id in [
        "chat-one-old",
        "chat-two-old",
        "restart-account",
        "restart-task",
        "replaced",
        "partial",
    ] {
        assert_eq!(retired(&store, id).await, [true, true], "{id}");
    }
    for id in [
        "chat-one-dormant",
        "chat-two-dormant",
        "reusable-task",
        "active-writer",
    ] {
        assert_eq!(retired(&store, id).await, [false, false], "{id}");
    }
    for (engine, role) in [
        (store.journals().protected(), JournalRole::Protected),
        (store.journals().ordinary(), JournalRole::Ordinary),
    ] {
        assert!(store
            .journals()
            .storage()
            .load(
                &JournalDomain {
                    storage_id: engine.storage_id(),
                    role
                },
                &legacy_id
            )
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(
        SessionStore::load(&*store, &legacy_id).await.unwrap(),
        Some(legacy_snapshot)
    );
    active.shutdown().await.unwrap();
    drop(active);
    drop(rt);
    drop(policy);
    // Retry is restart-safe, including a previously conflicting active writer.
    drop(store);
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let retry = SessionJournalRetention::new(f.database(), &cold)
        .retire_page("")
        .await
        .unwrap();
    assert_eq!(retry.retired_domains, 2);
    assert_eq!(retry.conflicts, 0);
    let flags = f
        .database()
        .session_persistence_flags(&host("restart-account"))
        .await
        .unwrap();
    assert!(flags.retired && !flags.saved && !flags.legacy_eligible);
    assert!(runtime(cold.clone())
        .start_session(StartSession::resume(SessionId::new("restart-account")))
        .await
        .is_err());
    for id in ["chat-one-dormant", "chat-two-dormant", "reusable-task"] {
        let session = runtime(cold.clone())
            .start_session(StartSession::resume(SessionId::new(id)))
            .await
            .unwrap();
        assert!(!session.history().is_empty());
        session.shutdown().await.unwrap();
    }
    assert!(CheckpointStore::journal(&*cold).is_some());
}

fn read_open(label: &str) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(format!("{label}-open")),
        pin: JournalPinId::new(label),
        intent: JournalOpenIntent::ReadOnly,
    }
}
async fn retain(store: &SqliteProtectedRuntimeStore, id: &str) {
    for engine in [store.journals().protected(), store.journals().ordinary()] {
        let lease = engine
            .open(&SessionId::new(id), read_open("deletion-retain"))
            .await
            .unwrap();
        engine.retain_root(&lease).await.unwrap();
        engine.close(lease).await.unwrap();
    }
}
async fn count_owner(db: &db::SqliteDb, table: &str, owner: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM agent_session_journal_{table} WHERE forge_session_id=?"
    ))
    .bind(owner)
    .fetch_one(db.pool())
    .await
    .unwrap()
}
fn body(label: &str) -> JournalStoredObject {
    let object = JournalObject::encode(
        JournalObjectKind::JsonInline,
        &format!("{label}{}", "x".repeat(1024)),
    )
    .unwrap();
    JournalStoredObject {
        id: object.id,
        kind: JournalObjectKind::JsonInline,
        bytes: object.bytes.into(),
    }
}
async fn seed_copies(
    f: &FileDb,
    store: &SqliteProtectedRuntimeStore,
    id: &str,
    mut objects: Vec<JournalStoredObject>,
) {
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    f.ensure_session(&SessionId::new(id)).await.unwrap();
    for (storage_id, role) in [
        (
            db::SESSION_JOURNAL_PROTECTED_STORAGE_ID,
            JournalRole::Protected,
        ),
        (
            db::SESSION_JOURNAL_ORDINARY_STORAGE_ID,
            JournalRole::Ordinary,
        ),
    ] {
        let domain = JournalDomain {
            storage_id: JournalStorageId::new(storage_id),
            role,
        };
        let write = JournalWrite {
            domain,
            session: SessionId::new(id),
            kind: JournalWriteKind::Commit,
            expect: Default::default(),
            record: JournalSessionRecord {
                generation: 1,
                revision: JournalRevision(0),
                writer_epoch: 0,
                head: None,
                has_checkpoint: false,
                retirement: None,
                collection_dirty: true,
                engine: JournalEngineState {
                    version: 1,
                    bytes: vec![],
                },
            },
            objects: JournalObjectBatch {
                objects: objects.clone(),
            },
            operation: None,
            pins: vec![],
            catalog: vec![],
            release: vec![],
            prune_operations_before: None,
            inject: None,
        };
        assert_eq!(
            store.journals().storage().apply(write).await.unwrap(),
            JournalApplyReceipt::default()
        );
    }
}
async fn bulk_orphans(db: &db::SqliteDb, count: i64, bytes: i64) {
    // Deleted-owner units whose ciphertext is unreadable. Cleanup must rely on
    // indexed ownership/counters, never AEAD or an invented apply receipt.
    sqlx::query("WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<?)
        INSERT INTO agent_session_journal_pack
        (runtime_session_id,forge_session_id,generation,object_count,retained_canonical_bytes,key_revision,format,index_nonce,index_payload,body_nonce,body_payload)
        SELECT 'deleted-bulk-runtime','deleted-bulk-owner',zeroblob(8),1,?,1,1,zeroblob(24),zeroblob(16),zeroblob(24),zeroblob(16) FROM n")
        .bind(count).bind(bytes).execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn deletion_drains_owned_packs_preserves_other_copies() {
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    for sql in [
        "INSERT INTO project (id,name,settings,workflow_definition,created_at,updated_at) VALUES ('delete-project','Delete','{}','{}','c','u')",
        "INSERT INTO agent_chat (id,kind,project_id,status,created_at,updated_at) VALUES ('delete-project-chat','project','delete-project','ready','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,project_id,created_at,updated_at) VALUES ('delete-project-scope','journal-agent','agent_chat','delete-project-chat','delete-project','c','u')",
        "INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES ('delete-owner','delete@example.test','hash','c','u')",
        "INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES ('delete-chat','account_main','delete-owner','ready','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('delete-chat-scope','journal-agent','agent_chat','delete-chat','c','u')",
        "INSERT INTO agent_identity (id,name,created_at,updated_at) VALUES ('delete-agent','Delete agent','c','u')",
        "INSERT INTO agent_profile (id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('delete-profile','delete-agent','native','embedded','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('delete-agent-scope','delete-agent','account','delete-owner','c','u')",
    ] {sqlx::query(sql).execute(db.pool()).await.unwrap();}
    for id in [
        "delete-project-runtime",
        "delete-agent-runtime",
        "delete-chat-runtime",
        "surviving-runtime",
    ] {
        publish(&f, store.clone(), id).await;
        retain(&store, id).await;
    }
    update(
        &f,
        "delete-project-runtime",
        "delete-project-scope",
        "journal-profile",
        "suspended",
        "c",
    )
    .await;
    update(
        &f,
        "delete-chat-runtime",
        "delete-chat-scope",
        "journal-profile",
        "suspended",
        "c",
    )
    .await;
    sqlx::query("UPDATE agent_session SET identity_id='delete-agent',profile_id='delete-profile',context_scope_id='delete-agent-scope',version=version+1 WHERE id=?")
        .bind(host("delete-agent-runtime")).execute(db.pool()).await.unwrap();
    let objects: Vec<_> = (0..8).map(|n| body(&format!("copy-{n}"))).collect();
    for id in ["deferred-deleted", "copy-keeper"] {
        seed_copies(&f, &store, id, objects.clone()).await;
    }
    update(
        &f,
        "deferred-deleted",
        "delete-project-scope",
        "journal-profile",
        "suspended",
        "c",
    )
    .await;
    for (storage_id, role) in [
        (
            db::SESSION_JOURNAL_PROTECTED_STORAGE_ID,
            JournalRole::Protected,
        ),
        (
            db::SESSION_JOURNAL_ORDINARY_STORAGE_ID,
            JournalRole::Ordinary,
        ),
    ] {
        let domain = JournalDomain {
            storage_id: JournalStorageId::new(storage_id),
            role,
        };
        let storage = store.journals().storage();
        let mut record = storage
            .load(&domain, &SessionId::new("deferred-deleted"))
            .await
            .unwrap()
            .unwrap();
        let expect = JournalFence {
            generation: record.generation,
            revision: record.revision,
            writer_epoch: record.writer_epoch,
        };
        record.generation += 1;
        let before:i64=sqlx::query_scalar("SELECT SUM(retained_canonical_bytes) FROM agent_session_journal_pack WHERE forge_session_id=?").bind(host("deferred-deleted")).fetch_one(db.pool()).await.unwrap();
        let receipt = storage
            .apply(JournalWrite {
                domain: domain.clone(),
                session: SessionId::new("deferred-deleted"),
                kind: JournalWriteKind::Collect,
                expect,
                record,
                objects: JournalObjectBatch { objects: vec![] },
                operation: None,
                pins: vec![],
                catalog: vec![],
                release: vec![objects[0].id.clone()],
                prune_operations_before: None,
                inject: None,
            })
            .await
            .unwrap();
        assert_eq!(receipt, JournalApplyReceipt::default());
        let after:i64=sqlx::query_scalar("SELECT SUM(retained_canonical_bytes) FROM agent_session_journal_pack WHERE forge_session_id=?").bind(host("deferred-deleted")).fetch_one(db.pool()).await.unwrap();
        assert_eq!(before, after, "12.5% dead remains sealed");
        assert!(storage
            .objects(
                &domain,
                &SessionId::new("deferred-deleted"),
                &[objects[0].id.clone()]
            )
            .await
            .unwrap()[0]
            .is_none());
    }
    for id in [
        "delete-project-runtime",
        "delete-agent-runtime",
        "delete-chat-runtime",
    ] {
        for table in ["session", "pin", "catalog", "membership", "operation_pack"] {
            assert!(count_owner(&db, table, &host(id)).await > 0, "{id}/{table}");
        }
    }
    // Rollback has neither a durable deletion nudge nor an activity wake.
    let notify = db.session_journal_cleanup_notify();
    let wake = notify.notified();
    tokio::pin!(wake);
    wake.as_mut().enable();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("DELETE FROM agent_session WHERE id=?")
        .bind(host("delete-chat-runtime"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut wake)
            .await
            .is_err()
    );
    let wake = notify.notified();
    tokio::pin!(wake);
    wake.as_mut().enable();
    db::ProjectRepo::delete(&*db, "delete-project")
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), &mut wake)
        .await
        .unwrap();
    db::AgentRepo::archive(&*db, "delete-agent", "archived")
        .await
        .unwrap();
    assert!(db.delete_agent_chat("delete-chat").await.unwrap());
    for id in [
        "delete-project-runtime",
        "delete-agent-runtime",
        "delete-chat-runtime",
        "deferred-deleted",
    ] {
        for table in ["session", "pin", "catalog", "membership", "operation_pack"] {
            assert_eq!(count_owner(&db, table, &host(id)).await, 0, "{id}/{table}");
        }
        assert!(count_owner(&db, "pack", &host(id)).await > 0);
    }
    // Old packs deliberately block rebinding the same runtime id before drain.
    let old = "delete-chat-runtime";
    let new_owner = "reused-runtime-new-owner";
    sqlx::query("INSERT INTO agent_session (id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES (?,'journal-agent','journal-profile','journal-scope','native',?,'suspended','c','u')")
        .bind(new_owner).bind(old).execute(db.pool()).await.unwrap();
    let Err(blocked) = runtime(store.clone())
        .start_session(StartSession::create(SessionId::new(old), vec![]))
        .await
    else {
        panic!("old-owner packs block runtime id reuse");
    };
    assert_eq!(
        blocked.kind,
        agent_runtime::core::error::ErrorKind::Internal
    );
    assert!(blocked
        .message
        .contains("journal pack owner mapping changed"));
    let before = store.journals().storage().cache_stats().unwrap();
    let expected:(i64,i64)=sqlx::query_as("SELECT SUM(object_count),SUM(retained_canonical_bytes) FROM agent_session_journal_pack WHERE orphaned=1").fetch_one(db.pool()).await.unwrap();
    let policy = SessionJournalRetention::new(db.clone(), &store);
    let mut totals = (0u64, 0u64);
    let mut after = 0;
    loop {
        let batch = policy.drain_deleted_packs(after).await.unwrap();
        assert!(
            batch.deleted_packs <= 32
                && batch.deleted_canonical_bytes <= 8 * 1024 * 1024
                && batch.candidates <= 5000
        );
        totals.0 += batch.deleted_objects;
        totals.1 += batch.deleted_canonical_bytes;
        after = batch.after_pack_id;
        if !batch.has_more {
            break;
        }
    }
    assert_eq!(totals, (expected.0 as u64, expected.1 as u64));
    let after_stats = store.journals().storage().cache_stats().unwrap();
    assert_eq!(
        (before.index_decodes, before.body_decodes),
        (after_stats.index_decodes, after_stats.body_decodes)
    );
    for id in [
        "delete-project-runtime",
        "delete-agent-runtime",
        "delete-chat-runtime",
        "deferred-deleted",
    ] {
        assert_eq!(count_owner(&db, "pack", &host(id)).await, 0);
    }
    // The very same shared owner/engines recover after their failed first apply;
    // stale scoped directories must refresh rather than keep the old mapping.
    let session = runtime(store.clone())
        .start_session(StartSession::create(SessionId::new(old), vec![]))
        .await
        .unwrap();
    session
        .run(UserInput::text("new owner turn"))
        .await
        .unwrap()
        .completed()
        .await;
    session.shutdown().await.unwrap();
    assert!(count_owner(&db, "pack", new_owner).await > 0);
    // Indexed paging and pack bound: partial progress, then a fresh pool/store.
    bulk_orphans(&db, 5001, 128).await;
    let first = policy.drain_deleted_packs(0).await.unwrap();
    assert_eq!((first.candidates, first.deleted_packs), (5000, 32));
    assert!(first.has_more);
    drop(policy);
    drop(store);
    let cold_db = f.reopen().await.unwrap();
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(
        cold_db.clone(),
        [7; 32],
        1,
    ));
    let policy = SessionJournalRetention::new(cold_db.clone(), &cold);
    let mut removed = first.deleted_packs;
    let mut cursor = 0;
    loop {
        let b = policy.drain_deleted_packs(cursor).await.unwrap();
        assert!(
            b.candidates <= 5000
                && b.deleted_packs <= 32
                && b.deleted_canonical_bytes <= 8 * 1024 * 1024
        );
        removed += b.deleted_packs;
        cursor = b.after_pack_id;
        if !b.has_more {
            break;
        }
    }
    assert_eq!(removed, 5001);
    bulk_orphans(&cold_db, 10, 1024 * 1024).await;
    let b = policy.drain_deleted_packs(0).await.unwrap();
    assert_eq!(
        (b.deleted_packs, b.deleted_canonical_bytes),
        (8, 8 * 1024 * 1024)
    );
    assert!(b.has_more);
    let b = policy.drain_deleted_packs(b.after_pack_id).await.unwrap();
    assert_eq!(b.deleted_packs, 2);
    assert!(!b.has_more);
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let domain = JournalDomain {
            storage_id: JournalStorageId::new(if role == JournalRole::Protected {
                db::SESSION_JOURNAL_PROTECTED_STORAGE_ID
            } else {
                db::SESSION_JOURNAL_ORDINARY_STORAGE_ID
            }),
            role,
        };
        for object in &objects {
            assert_eq!(
                cold.journals()
                    .storage()
                    .objects(
                        &domain,
                        &SessionId::new("copy-keeper"),
                        std::slice::from_ref(&object.id)
                    )
                    .await
                    .unwrap(),
                vec![Some(object.bytes.clone())]
            );
        }
    }
    let survivor = runtime(cold.clone())
        .start_session(StartSession::resume(SessionId::new("surviving-runtime")))
        .await
        .unwrap();
    assert!(!survivor.history().is_empty());
    survivor.shutdown().await.unwrap();
    use sqlx::Row;
    let rows=sqlx::query("EXPLAIN QUERY PLAN SELECT pack_id FROM agent_session_journal_pack WHERE orphaned=1 AND pack_id>0 ORDER BY pack_id LIMIT 5000").fetch_all(cold_db.pool()).await.unwrap();
    assert!(rows.iter().any(|r| r
        .get::<String, _>("detail")
        .contains("idx_session_journal_pack_orphan")));
}

#[tokio::test]
async fn rotation_and_retry() {
    use agent_runtime::runtime::{ForkLcm, ForkSeed, ForkSession};
    use services_agents::session_journal_retention::{
        RetentionCursor, MEMBERSHIP_RELEASE_LIMIT, RETENTION_INTERVAL,
    };
    use std::time::Duration;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    for sql in [
        "INSERT INTO project (id,name,settings,workflow_definition,created_at,updated_at) VALUES ('worker-project','Worker','{}','{}','c','u')",
        "INSERT INTO task (id,project_id,title,status,created_at,updated_at) VALUES ('worker-task','worker-project','Completed reusable task','done','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,project_id,task_id,task_role,workspace_access,created_at,updated_at) VALUES ('worker-task-scope','journal-agent','task','worker-task','worker-project','worker-task','worker','task_write','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('worker-unreadable-scope','journal-agent','account','bad-owner','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('worker-safety-scope','journal-agent','account','safe-owner','c','u')",
    ] { sqlx::query(sql).execute(db.pool()).await.unwrap(); }
    for id in [
        "rotation-source",
        "active-source",
        "partial-source",
        "unreadable-source",
        "reusable-task",
        "deferred-source",
        "worker-deleted",
        "safety-source",
    ] {
        publish(&f, store.clone(), id).await;
    }
    update(
        &f,
        "reusable-task",
        "worker-task-scope",
        "journal-profile",
        "ready",
        "c",
    )
    .await;
    update(
        &f,
        "unreadable-source",
        "worker-unreadable-scope",
        "journal-profile",
        "ready",
        "c",
    )
    .await;
    update(
        &f,
        "safety-source",
        "worker-safety-scope",
        "journal-profile",
        "ready",
        "c",
    )
    .await;
    // Actual host rotation nudge after commit, followed by an actual runtime
    // fork; the source and successor have the real dependency/transfer state.
    let source = db
        .get_agent_session(&host("rotation-source"))
        .await
        .unwrap()
        .unwrap();
    let notify = db.session_journal_cleanup_notify();
    let wake = notify.notified();
    tokio::pin!(wake);
    wake.as_mut().enable();
    db.rotate_agent_session(db::RotateAgentSession {
        previous_session_id: source.id.clone(),
        expected_version: source.version,
        replacement: db::CreateAgentSession {
            id: host("rotation-successor"),
            identity_id: source.identity_id,
            profile_id: source.profile_id,
            context_scope_id: source.context_scope_id,
            backend_kind: "native".into(),
            runtime_session_id: Some("rotation-successor".into()),
            status: "ready".into(),
            capabilities_json: "{}".into(),
            connection_status: "unknown".into(),
            predecessor_session_id: Some(source.id),
            last_activity_at: None,
            created_at: "d".into(),
            updated_at: "d".into(),
        },
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), &mut wake)
        .await
        .unwrap();
    let fork_runtime = runtime(store.clone());
    let child = fork_runtime
        .fork_session(ForkSession {
            from: SessionId::new("rotation-source"),
            new_id: SessionId::new("rotation-successor"),
            seed: ForkSeed::FromIndex(0),
            lcm: ForkLcm::NewTimeline,
        })
        .await
        .unwrap();
    assert!(!child.history().is_empty());
    child.shutdown().await.unwrap();
    drop(child);
    drop(fork_runtime);

    let active_runtime = runtime(store.clone());
    let active = active_runtime
        .start_session(StartSession::resume(SessionId::new("active-source")))
        .await
        .unwrap();
    for id in ["active-source", "partial-source", "deferred-source"] {
        update(&f, id, "journal-scope", "journal-profile", "replaced", "c").await;
    }
    // This is exactly the durable cut left by a crash between domain retirements.
    let engine = store.journals().protected();
    let id = SessionId::new("partial-source");
    let domain = JournalDomain {
        storage_id: engine.storage_id(),
        role: JournalRole::Protected,
    };
    let revision = store
        .journals()
        .storage()
        .load(&domain, &id)
        .await
        .unwrap()
        .unwrap()
        .revision;
    engine
        .authorize_retirement(&id, revision, agent_runtime::core::clock::Timestamp(10))
        .await
        .unwrap();
    engine.retire(&id, revision).await.unwrap();
    assert_eq!(retired(&store, "partial-source").await, [true, false]);
    // Keep roots while retiring: already-dead bytes below 25% must be purged
    // in BOTH retired domains, even when the retained roots stay readable.
    retain(&store, "deferred-source").await;
    let object =
        JournalObject::encode(JournalObjectKind::JsonInline, &"deferred worker slot").unwrap();
    let dead = JournalStoredObject {
        id: object.id,
        kind: JournalObjectKind::JsonInline,
        bytes: object.bytes.into(),
    };
    for (engine, role) in [
        (store.journals().protected(), JournalRole::Protected),
        (store.journals().ordinary(), JournalRole::Ordinary),
    ] {
        let domain = JournalDomain {
            storage_id: engine.storage_id(),
            role,
        };
        store
            .journals()
            .storage()
            .add_deferred_slot_for_test(&domain, &SessionId::new("deferred-source"), dead.clone())
            .await
            .unwrap();
        assert!(
            store
                .journals()
                .storage()
                .byte_stats(&domain)
                .await
                .unwrap()
                .deferred_bytes
                > 0
        );
    }
    sqlx::query("DELETE FROM agent_session WHERE id=?")
        .bind(host("worker-deleted"))
        .execute(db.pool())
        .await
        .unwrap();
    let policy = Arc::new(
        SessionJournalRetention::new(db.clone(), &store).with_collection_budget_for_test(
            JournalGcBudget {
                max_work_units: 16,
                ..JournalGcBudget::default()
            },
        ),
    );
    let mut cursor = RetentionCursor::default();
    let zero = policy
        .maintain_with_budget_for_test(&mut cursor, Duration::ZERO)
        .await
        .unwrap();
    assert!(zero.budget_exhausted);
    assert_eq!(zero.collection_calls, 0);
    assert_eq!(retired(&store, "rotation-source").await, [false, false]);
    let diagnostics_before = store.journals().storage().diagnostic_calls_for_test().0;
    let pass = policy.maintain_once(&mut cursor).await.unwrap();
    assert!(
        pass.collection_calls > 2,
        "fixture must exercise continuations"
    );
    assert_eq!(
        store.journals().storage().diagnostic_calls_for_test().0 - diagnostics_before,
        0,
        "debug-disabled retention never runs byte diagnostics, including yielded calls"
    );
    assert_eq!(
        pass.collection_calls - pass.collection_continuations,
        2,
        "both domain phases advanced; yielded calls remain counted"
    );
    assert!(pass.collection_calls <= 32);
    assert!(
        pass.retired_domains >= 5 && pass.conflicts >= 1 && pass.unreadable_sessions == 0,
        "{pass:?}"
    );
    assert!(pass.reclaimed_bytes > 0 && pass.deleted_packs > 0);
    for id in ["rotation-source", "partial-source", "deferred-source"] {
        assert_eq!(retired(&store, id).await, [true, true], "{id}");
    }
    for id in ["rotation-successor", "active-source", "reusable-task"] {
        assert_eq!(retired(&store, id).await, [false, false], "{id}");
    }
    assert_eq!(count_owner(&db, "pack", &host("worker-deleted")).await, 0);
    for (engine, role) in [
        (store.journals().protected(), JournalRole::Protected),
        (store.journals().ordinary(), JournalRole::Ordinary),
    ] {
        let domain = JournalDomain {
            storage_id: engine.storage_id(),
            role,
        };
        assert_eq!(
            store
                .journals()
                .storage()
                .byte_stats_for_session_for_test(&domain, &SessionId::new("deferred-source"))
                .await
                .unwrap()
                .deferred_bytes,
            0
        );
    }
    // Exercise the actual periodic worker, its shared health registry, nudge
    // and shutdown. Closing the live writer makes the next pass eligible.
    let workers = services::worker_runtime::PeriodicWorkers::new(db.clone());
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let worker = policy.clone().start(&workers, shutdown);
    active.shutdown().await.unwrap();
    drop(active);
    drop(active_runtime);
    store.journals().post_terminal("active-source").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while retired(&store, "active-source").await != [true, true] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .unwrap()
        .unwrap();
    assert!(workers
        .status()
        .await
        .unwrap()
        .iter()
        .any(|w| w.worker_name == "session-journal-retention" && !w.running));
    // Corruption cannot justify retirement. A different session's unreadable
    // dependency metadata also conservatively defers ordinary retirement;
    // healthy collection still proceeds and must not mislabel that candidate.
    // Corrupt ordinary only. Preflight must leave protected unretired too.
    sqlx::query("UPDATE agent_session_journal_session SET payload=zeroblob(length(payload)),collection_dirty=1 WHERE runtime_session_id='unreadable-source' AND role='ordinary'").execute(db.pool()).await.unwrap();
    let corrupt_before: (Vec<u8>,Vec<u8>,i64) = sqlx::query_as("SELECT generation,payload,(SELECT count(*) FROM agent_session_journal_membership WHERE runtime_session_id='unreadable-source' AND role='ordinary') FROM agent_session_journal_session WHERE runtime_session_id='unreadable-source' AND role='ordinary'").fetch_one(db.pool()).await.unwrap();
    update(
        &f,
        "unreadable-source",
        "worker-unreadable-scope",
        "journal-profile",
        "replaced",
        "c",
    )
    .await;
    update(
        &f,
        "safety-source",
        "worker-safety-scope",
        "journal-profile",
        "replaced",
        "c",
    )
    .await;
    let damaged = policy.maintain_once(&mut cursor).await.unwrap();
    assert_eq!(
        damaged.collection_calls - damaged.collection_continuations,
        2,
        "unreadable isolation still advances both domain phases"
    );
    assert!(damaged.collection_calls <= 32);
    assert_eq!(damaged.unreadable_sessions, 1, "{damaged:?}");
    assert!(
        damaged.deferred_domains >= 1,
        "unknown dependencies must defer retirement: {damaged:?}"
    );
    assert_eq!(retired(&store, "safety-source").await, [true, false]);
    let corrupt_after: (Vec<u8>,Vec<u8>,i64) = sqlx::query_as("SELECT generation,payload,(SELECT count(*) FROM agent_session_journal_membership WHERE runtime_session_id='unreadable-source' AND role='ordinary') FROM agent_session_journal_session WHERE runtime_session_id='unreadable-source' AND role='ordinary'").fetch_one(db.pool()).await.unwrap();
    assert_eq!(corrupt_before, corrupt_after);
    let protected_bad: i64 = sqlx::query_scalar("SELECT retired FROM agent_session_journal_session WHERE runtime_session_id='unreadable-source' AND role='protected'").fetch_one(db.pool()).await.unwrap();
    assert_eq!(protected_bad, 0);
    assert_eq!(MEMBERSHIP_RELEASE_LIMIT, 20_000);
    assert_eq!(RETENTION_INTERVAL, Duration::from_secs(60));
    // Recreate owners only after the old worker/runtime leases have stopped.
    drop(policy);
    drop(store);
    drop(engine);
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    assert_eq!(retired(&cold, "partial-source").await, [true, true]);
    let resumed = runtime(cold)
        .start_session(StartSession::resume(SessionId::new("rotation-successor")))
        .await
        .unwrap();
    assert!(!resumed.history().is_empty());
    resumed.shutdown().await.unwrap();

    // A distinct restart fixture actually drops the old owner at the cut.
    let crashed = FileDb::new().await.unwrap();
    let before_crash = Arc::new(SqliteProtectedRuntimeStore::new(
        crashed.database(),
        [7; 32],
        1,
    ));
    publish(&crashed, before_crash.clone(), "crash-between-domains").await;
    let protected = before_crash.journals().protected();
    let sid = SessionId::new("crash-between-domains");
    let d = JournalDomain {
        storage_id: protected.storage_id(),
        role: JournalRole::Protected,
    };
    let revision = before_crash
        .journals()
        .storage()
        .load(&d, &sid)
        .await
        .unwrap()
        .unwrap()
        .revision;
    protected
        .authorize_retirement(&sid, revision, agent_runtime::core::clock::Timestamp(20))
        .await
        .unwrap();
    protected.retire(&sid, revision).await.unwrap();
    assert_eq!(retired(&before_crash, sid.as_str()).await, [true, false]);
    drop(protected);
    drop(before_crash);
    let reopened = Arc::new(SqliteProtectedRuntimeStore::new(
        crashed.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let retry = SessionJournalRetention::new(crashed.database(), &reopened);
    let progress = retry
        .maintain_once(&mut RetentionCursor::default())
        .await
        .unwrap();
    assert_eq!(progress.retired_domains, 1);
    assert_eq!(retired(&reopened, sid.as_str()).await, [true, true]);
}

#[tokio::test]
async fn chat_delete_drains_sessions() {
    use db::{ProjectRepo, UserRepo};
    use services_agents::session_journal_retention::RetentionCursor;
    use std::time::Duration;
    // The explicit chat path and both parent cascades must use repository
    // cleanup. No logic trigger supplies the polymorphic scope edge.
    for path in ["chat", "user", "project"] {
        let f = FileDb::new().await.unwrap();
        let db = f.database();
        let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
        sqlx::query("INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES ('chat-delete-owner','chat-delete@example.test','h','c','u')").execute(db.pool()).await.unwrap();
        if path == "project" {
            sqlx::query("INSERT INTO project (id,name,settings,workflow_definition,created_at,updated_at) VALUES ('chat-delete-project','Chat delete','{}','{}','c','u')").execute(db.pool()).await.unwrap();
            sqlx::query("INSERT INTO agent_chat (id,kind,project_id,status,created_at,updated_at) VALUES ('chat-delete','project','chat-delete-project','ready','c','u')").execute(db.pool()).await.unwrap();
        } else {
            sqlx::query("INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES ('chat-delete','account_main','chat-delete-owner','ready','c','u')").execute(db.pool()).await.unwrap();
        }
        sqlx::query("INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,project_id,created_at,updated_at) VALUES ('chat-delete-scope','journal-agent','agent_chat','chat-delete',?,'c','u')")
            .bind(if path == "project" { Some("chat-delete-project") } else { None }).execute(db.pool()).await.unwrap();
        publish(&f, store.clone(), "chat-delete-runtime").await;
        publish(&f, store.clone(), "chat-keeper").await;
        retain(&store, "chat-delete-runtime").await;
        update(
            &f,
            "chat-delete-runtime",
            "chat-delete-scope",
            "journal-profile",
            "suspended",
            "c",
        )
        .await;
        update(
            &f,
            "chat-keeper",
            "journal-scope",
            "journal-profile",
            "ready",
            "c",
        )
        .await;
        let logic_triggers: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name IN ('session_journal_chat_deleted','session_journal_agent_archived')").fetch_one(db.pool()).await.unwrap();
        assert_eq!(logic_triggers, 0);
        let notify = db.session_journal_cleanup_notify();
        if path == "chat" {
            // Refusal after deleting scopes must roll the entire transaction
            // back, including the session cascades and notification marker.
            sqlx::query("CREATE TRIGGER refuse_chat_delete BEFORE DELETE ON agent_chat WHEN OLD.id='chat-delete' BEGIN SELECT RAISE(ABORT,'injected delete refusal'); END").execute(db.pool()).await.unwrap();
            let rollback_wake = notify.notified();
            tokio::pin!(rollback_wake);
            rollback_wake.as_mut().enable();
            assert!(db.delete_agent_chat("chat-delete").await.is_err());
            assert!(db
                .get_agent_session(&host("chat-delete-runtime"))
                .await
                .unwrap()
                .is_some());
            assert!(count_owner(&db, "session", &host("chat-delete-runtime")).await > 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut rollback_wake)
                    .await
                    .is_err()
            );
            sqlx::query("DROP TRIGGER refuse_chat_delete")
                .execute(db.pool())
                .await
                .unwrap();
        }
        let wake = notify.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        match path {
            "chat" => assert!(db.delete_agent_chat("chat-delete").await.unwrap()),
            "user" => assert!(db.delete_user("chat-delete-owner").await.unwrap()),
            "project" => ProjectRepo::delete(&*db, "chat-delete-project")
                .await
                .unwrap(),
            _ => unreachable!(),
        }
        tokio::time::timeout(Duration::from_secs(1), &mut wake)
            .await
            .unwrap();
        assert!(db
            .get_agent_session(&host("chat-delete-runtime"))
            .await
            .unwrap()
            .is_none());
        for table in ["session", "pin", "catalog", "membership", "operation_pack"] {
            assert_eq!(
                count_owner(&db, table, &host("chat-delete-runtime")).await,
                0,
                "{path}/{table}"
            );
        }
        assert!(count_owner(&db, "pack", &host("chat-delete-runtime")).await > 0);
        SessionJournalRetention::new(db.clone(), &store)
            .maintain_once(&mut RetentionCursor::default())
            .await
            .unwrap();
        assert_eq!(
            count_owner(&db, "pack", &host("chat-delete-runtime")).await,
            0
        );
        let keeper = runtime(store)
            .start_session(StartSession::resume(SessionId::new("chat-keeper")))
            .await
            .unwrap();
        assert!(!keeper.history().is_empty());
        keeper.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn agent_archive_drains_sessions_including_running_cli() {
    use db::AgentRepo;
    use services_agents::session_journal_retention::RetentionCursor;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    for sql in [
        "INSERT INTO agent_identity (id,name,created_at,updated_at) VALUES ('archive-agent','Archived agent','c','u')",
        "INSERT INTO agent_profile (id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('archive-native','archive-agent','native','embedded','c','u')",
        "INSERT INTO agent_profile (id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('archive-cli','archive-agent','cli','codex','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('archive-native-scope','archive-agent','account','native-owner','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('archive-cli-scope','archive-agent','account','cli-owner','c','u')",
        "INSERT INTO agent_session (id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('archive-running-cli','archive-agent','archive-cli','archive-cli-scope','cli','cli-resume-id','running','c','u')",
    ] { sqlx::query(sql).execute(db.pool()).await.unwrap(); }
    publish(&f, store.clone(), "archive-native-runtime").await;
    publish(&f, store.clone(), "archive-keeper").await;
    sqlx::query("UPDATE agent_session SET identity_id='archive-agent',profile_id='archive-native',context_scope_id='archive-native-scope',version=version+1 WHERE id=?").bind(host("archive-native-runtime")).execute(db.pool()).await.unwrap();
    update(
        &f,
        "archive-keeper",
        "journal-scope",
        "journal-profile",
        "ready",
        "c",
    )
    .await;
    let cli = db
        .get_agent_session("archive-running-cli")
        .await
        .unwrap()
        .unwrap();
    // Service archival uses this transaction helper; direct repository archive
    // delegates to it too. Its saved-session deletions must roll back with it.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert_eq!(
        db.archive_agent_identity_in_tx(&mut tx, "rollback", "rollback", "archive-agent")
            .await
            .unwrap(),
        1
    );
    let inside: i64 =
        sqlx::query_scalar("SELECT count(*) FROM agent_session WHERE identity_id='archive-agent'")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(inside, 0);
    tx.rollback().await.unwrap();
    assert!(db
        .get_agent_session(&host("archive-native-runtime"))
        .await
        .unwrap()
        .is_some());
    assert!(db.get_agent_session(&cli.id).await.unwrap().is_some());
    AgentRepo::archive(&*db, "archive-agent", "archived")
        .await
        .unwrap();
    let archived: Option<String> =
        sqlx::query_scalar("SELECT archived_at FROM agent_identity WHERE id='archive-agent'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(archived.as_deref(), Some("archived"));
    assert!(db.get_agent_session(&cli.id).await.unwrap().is_none());
    assert!(db
        .get_agent_session(&host("archive-native-runtime"))
        .await
        .unwrap()
        .is_none());
    // A running CLI is represented here by its running row/resume handle. The
    // DB archive does not signal a process, and a late saved-session update
    // cannot recreate the deleted handle (normal cancellation is separate).
    let late = db
        .update_agent_session(db::UpdateAgentSession {
            id: cli.id,
            expected_version: cli.version,
            runtime_session_id: Some(Some("late-cli-resume-id".into())),
            status: Some("ready".into()),
            connection_status: None,
            last_activity_at: None,
            updated_at: "late".into(),
        })
        .await;
    assert!(matches!(late, Err(db::DbError::NotFound)));
    for table in ["session", "pin", "catalog", "membership", "operation_pack"] {
        assert_eq!(
            count_owner(&db, table, &host("archive-native-runtime")).await,
            0
        );
    }
    SessionJournalRetention::new(db.clone(), &store)
        .maintain_once(&mut RetentionCursor::default())
        .await
        .unwrap();
    assert_eq!(
        count_owner(&db, "pack", &host("archive-native-runtime")).await,
        0
    );
    assert!(db
        .get_agent_session(&host("archive-keeper"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn unreadable_counts_are_per_pass_across_repair_delete_and_restart() {
    use services_agents::session_journal_retention::RetentionCursor;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    for id in ["pass-unreadable-a", "pass-unreadable-b"] {
        publish(&f, store.clone(), id).await;
    }
    let original: Vec<u8> = sqlx::query_scalar("SELECT payload FROM agent_session_journal_session WHERE runtime_session_id='pass-unreadable-a' AND role='ordinary'")
        .fetch_one(db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_session_journal_session SET payload=zeroblob(length(payload)),collection_dirty=1 WHERE runtime_session_id='pass-unreadable-a' AND role='ordinary'")
        .execute(db.pool()).await.unwrap();
    let policy = SessionJournalRetention::new(db.clone(), &store);
    let mut cursor = RetentionCursor::default();
    for _ in 0..3 {
        let report = policy.maintain_once(&mut cursor).await.unwrap();
        assert_eq!(
            report.unreadable_sessions, 1,
            "each pass counts the still-unreadable session"
        );
        assert_eq!(report.preserved_sessions, 1);
    }
    let mut restarted = RetentionCursor::default();
    assert_eq!(
        policy
            .maintain_once(&mut restarted)
            .await
            .unwrap()
            .preserved_sessions,
        1
    );
    sqlx::query("UPDATE agent_session_journal_session SET payload=?,collection_dirty=1 WHERE runtime_session_id='pass-unreadable-a' AND role='ordinary'")
        .bind(original).execute(db.pool()).await.unwrap();
    let repaired = policy.maintain_once(&mut cursor).await.unwrap();
    assert_eq!(
        repaired.preserved_sessions, 0,
        "repaired ids are not lifetime cursor state"
    );
    assert_eq!(repaired.unreadable_sessions, 0);
    sqlx::query("UPDATE agent_session_journal_session SET payload=zeroblob(length(payload)),collection_dirty=1 WHERE runtime_session_id='pass-unreadable-b' AND role='ordinary'")
        .execute(db.pool()).await.unwrap();
    assert_eq!(
        policy
            .maintain_once(&mut cursor)
            .await
            .unwrap()
            .preserved_sessions,
        1
    );
    sqlx::query("DELETE FROM agent_session WHERE id=?")
        .bind(host("pass-unreadable-b"))
        .execute(db.pool())
        .await
        .unwrap();
    for cursor in [&mut cursor, &mut restarted, &mut RetentionCursor::default()] {
        let report = policy.maintain_once(cursor).await.unwrap();
        assert_eq!(
            report.preserved_sessions, 0,
            "deletion and worker restart retain no old ids"
        );
        assert_eq!(report.unreadable_sessions, 0);
    }
}

#[tokio::test]
async fn over_capacity_sessions_are_reported_and_preserved_per_pass() {
    use services_agents::session_journal_retention::RetentionCursor;
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    let sid = SessionId::new("capacity-preserved");
    publish(&f, store.clone(), sid.as_str()).await;
    update(
        &f,
        sid.as_str(),
        "journal-scope",
        "journal-profile",
        "ready",
        "c",
    )
    .await;
    let engine = store.journals().protected();
    engine.set_collection_cursor_cap(0).unwrap();
    let domain = JournalDomain {
        storage_id: engine.storage_id(),
        role: JournalRole::Protected,
    };
    let members = store
        .journals()
        .storage()
        .members(&domain, &sid)
        .await
        .unwrap();
    let bodies = store
        .journals()
        .storage()
        .objects(&domain, &sid, &members)
        .await
        .unwrap();
    let policy = SessionJournalRetention::new(f.database(), &store);
    let mut cursor = RetentionCursor::default();
    for _ in 0..2 {
        let report = policy.maintain_once(&mut cursor).await.unwrap();
        assert_eq!(report.over_capacity_sessions, 1, "{report:?}");
        assert_eq!(report.preserved_sessions, 1, "{report:?}");
        assert_eq!(report.unreadable_sessions, 0);
        assert_eq!(
            store
                .journals()
                .storage()
                .members(&domain, &sid)
                .await
                .unwrap(),
            members
        );
        assert_eq!(
            store
                .journals()
                .storage()
                .objects(&domain, &sid, &members)
                .await
                .unwrap(),
            bodies
        );
    }
    engine.set_collection_cursor_cap(16 * 1024 * 1024).unwrap();
    let report = policy.maintain_once(&mut cursor).await.unwrap();
    assert_eq!(report.over_capacity_sessions, 0);
    assert_eq!(report.preserved_sessions, 0);
}

#[tokio::test]
async fn domain_event_burst_is_debounced_without_debug_byte_scans() {
    use services_agents::session_journal_retention::RETENTION_DEBOUNCE;
    use std::time::Duration;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    let policy = Arc::new(SessionJournalRetention::new(db.clone(), &store));
    let registry = services::worker_runtime::PeriodicWorkers::new(db.clone());
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let running = policy.clone().start(&registry, shutdown);
    tokio::time::timeout(Duration::from_secs(5), async {
        while policy.completed_passes_for_test() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.journals().storage().diagnostic_calls_for_test().0,
        0,
        "debug disabled: no encrypted byte diagnostic scan"
    );
    for n in 0..10 {
        sqlx::query("INSERT INTO domain_event (id,event_type,entity_type,entity_id,actor_type,scope_type,scope_id,correlation_id,created_at) VALUES (?,'test','test','test','system','system','system','test','c')")
            .bind(format!("debounce-{n}")).execute(db.pool()).await.unwrap();
        db.domain_event_notify().notify_one();
        tokio::time::sleep(RETENTION_DEBOUNCE / 25).await;
        assert_eq!(
            policy.completed_passes_for_test(),
            1,
            "no pass inside debounce"
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while policy.completed_passes_for_test() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(RETENTION_DEBOUNCE * 2).await;
    assert_eq!(
        policy.completed_passes_for_test(),
        2,
        "ten domain events cause one pass"
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn pending_topic_rotation_survives_restart_and_retention_then_forks() {
    use agent_runtime::runtime::{ForkLcm, ForkSeed, ForkSession};
    use services_agents::session_journal_retention::RetentionCursor;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    publish(&f, store.clone(), "pending-source").await;
    for sql in [
        "INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES ('pending-owner','pending@example.test','hash','c','u')",
        "INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES ('pending-chat','account_main','pending-owner','ready','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES ('pending-scope','journal-agent','agent_chat','pending-chat','c','u')",
    ] { sqlx::query(sql).execute(db.pool()).await.unwrap(); }
    update(
        &f,
        "pending-source",
        "pending-scope",
        "journal-profile",
        "ready",
        "a",
    )
    .await;
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    db.reserve_topic_rotation_successor_session_in_tx(
        &mut tx,
        db::ReserveTopicRotationSuccessorSessionInTxInput {
            source_id: &host("pending-source"),
            successor_id: &host("pending-successor"),
            runtime_id: "pending-successor",
            created_at: "z",
            updated_at: "z",
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO agent_chat_topic_rotation (chat_id,id,successor_session_id,successor_runtime_id,label,cause,created_at) VALUES ('pending-chat','pending-intent',?,'pending-successor','Rotate','test','c')")
        .bind(host("pending-successor")).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    drop(store);
    let cold_db = f.reopen().await.unwrap();
    assert_eq!(
        cold_db
            .suspend_stale_native_sessions("restart")
            .await
            .unwrap(),
        1
    );
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(
        cold_db.clone(),
        [7; 32],
        1,
    ));
    let policy = SessionJournalRetention::new(cold_db.clone(), &cold);
    policy
        .maintain_once(&mut RetentionCursor::default())
        .await
        .unwrap();
    assert_eq!(
        retired(&cold, "pending-source").await,
        [false, false],
        "a pending rotation keeps its predecessor after the startup sweep"
    );
    let rt = runtime(cold.clone());
    let child = rt
        .fork_session(ForkSession {
            from: SessionId::new("pending-source"),
            new_id: SessionId::new("pending-successor"),
            seed: ForkSeed::FromIndex(0),
            lcm: ForkLcm::NewTimeline,
        })
        .await
        .unwrap();
    assert!(
        !child.history().is_empty(),
        "fork retains the native context"
    );
    child.shutdown().await.unwrap();
    drop(child);
    drop(rt);
    sqlx::query("DELETE FROM agent_chat_topic_rotation WHERE id='pending-intent'")
        .execute(cold_db.pool())
        .await
        .unwrap();
    assert!(
        policy.should_retire(&host("pending-source")).await.unwrap(),
        "protection lasts only while the rotation intent is open"
    );
}

#[tokio::test]
async fn debug_bytes_are_sampled_only_once_per_pass() {
    use services_agents::session_journal_retention::RetentionCursor;
    use tracing::instrument::WithSubscriber;
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    let policy = SessionJournalRetention::new(f.database(), &store);
    let mut cursor = RetentionCursor::default();
    let events = forge_agent_host::test_support::JournalEvents {
        debug: true,
        ..Default::default()
    };
    policy
        .maintain_once(&mut cursor)
        .with_subscriber(events)
        .await
        .unwrap();
    assert_eq!(
        store.journals().storage().diagnostic_calls_for_test().0,
        1,
        "one ordinary-domain byte sample per debug-enabled pass"
    );
}

#[tokio::test]
async fn retirement_errors_do_not_starve_collection_or_orphans_and_warn_once_per_cause() {
    use services_agents::session_journal_retention::RetentionCursor;
    use tracing::instrument::WithSubscriber;
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    publish(&f, store.clone(), "healthy-during-retire-error").await;
    let policy = SessionJournalRetention::new(db.clone(), &store);
    let events = forge_agent_host::test_support::JournalEvents::default();
    let mut cursor = RetentionCursor::default();
    for cause in [
        "retirement lock unavailable",
        "retirement lock unavailable",
        "retirement database unavailable",
    ] {
        bulk_orphans(&db, 1, 32).await;
        policy.fail_retirement_for_test(cause);
        let report = policy
            .maintain_once(&mut cursor)
            .with_subscriber(events.clone())
            .await
            .unwrap();
        assert_eq!(report.retirement_errors, 1);
        assert_eq!(
            report.collection_calls, 2,
            "both domains collect after retirement fails"
        );
        assert_eq!(
            report.deleted_packs, 1,
            "orphan cleanup still runs in the same pass"
        );
    }
    let rows = events.rows.lock().unwrap();
    let warnings: Vec<_> = rows
        .iter()
        .filter(|(level, fields)| {
            *level == tracing::Level::WARN
                && fields
                    .get("message")
                    .is_some_and(|message| message.contains("journal retirement failed"))
        })
        .collect();
    assert_eq!(
        warnings.len(),
        2,
        "each distinct cause warns once across passes"
    );
}
