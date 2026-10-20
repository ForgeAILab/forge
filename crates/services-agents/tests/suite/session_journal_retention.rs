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
        (storage_id,role,runtime_session_id,forge_session_id,generation,object_count,retained_canonical_bytes,key_revision,format,index_nonce,index_payload,body_nonce,body_payload)
        SELECT 'bulk','ordinary','deleted-bulk-runtime','deleted-bulk-owner',zeroblob(8),1,?,1,1,zeroblob(24),zeroblob(16),zeroblob(24),zeroblob(16) FROM n")
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
        let before:i64=sqlx::query_scalar("SELECT SUM(retained_canonical_bytes) FROM agent_session_journal_pack WHERE forge_session_id=? AND role=?").bind(host("deferred-deleted")).bind(if role==JournalRole::Protected {"protected"}else{"ordinary"}).fetch_one(db.pool()).await.unwrap();
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
        let after:i64=sqlx::query_scalar("SELECT SUM(retained_canonical_bytes) FROM agent_session_journal_pack WHERE forge_session_id=? AND role=?").bind(host("deferred-deleted")).bind(if role==JournalRole::Protected {"protected"}else{"ordinary"}).fetch_one(db.pool()).await.unwrap();
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
    sqlx::query("DELETE FROM agent_chat WHERE id='delete-chat'")
        .execute(db.pool())
        .await
        .unwrap();
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
