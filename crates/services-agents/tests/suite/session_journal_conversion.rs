use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::{CheckpointStore, TurnCheckpoint},
        clock::{Deadline, Timestamp},
        content::{Message, UserInput},
        error::RuntimeError,
        ids::{SessionId, TurnId},
        journal::*,
        provider::{Capabilities, ModelId},
        store::{SessionSnapshot, SessionStore},
    },
    provider::fake::FakeProvider,
    runtime::{CheckpointRecoveryPolicy, Runtime, RuntimeBuilder, StartSession},
};
use async_trait::async_trait;
use forge_agent_host::{test_support::FileDb, SqliteProtectedRuntimeStore};
use services_agents::session_journal_conversion::{
    ConversionCursor, SessionJournalConversion, CONVERSION_CANDIDATE_LIMIT,
};
use std::{sync::Arc, time::Duration};

async fn fixture() -> (
    FileDb,
    Arc<SqliteProtectedRuntimeStore>,
    Arc<SessionJournalConversion>,
) {
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    let worker = Arc::new(SessionJournalConversion::new(f.database(), store.clone()));
    (f, store, worker)
}
pub(super) async fn seed(f: &FileDb, store: &SqliteProtectedRuntimeStore, name: &str) -> SessionId {
    let id = SessionId::new(name);
    f.ensure_session(&id).await.unwrap();
    // Each legacy fixture is a resumable dormant chat, including bulk pages.
    let db = f.database();
    sqlx::query("INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES (?,?,'hash','c','u')")
        .bind(format!("owner:{name}")).bind(format!("{name}@example.test")).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES (?,'account_main',?,'ready','c','u')")
        .bind(format!("chat:{name}")).bind(format!("owner:{name}")).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,created_at,updated_at) VALUES (?,'journal-agent','agent_chat',?,'c','u')")
        .bind(format!("scope:{name}")).bind(format!("chat:{name}")).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_session SET context_scope_id=? WHERE runtime_session_id=?")
        .bind(format!("scope:{name}"))
        .bind(name)
        .execute(db.pool())
        .await
        .unwrap();
    let mut s: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":id,"history":[],"updated":1})).unwrap();
    s.history.push(Message::user("accepted legacy input"));
    let cp = TurnCheckpoint::accepted(
        TurnId::new(format!("turn:{name}")),
        UserInput::text("accepted legacy input"),
        s,
        0,
        Deadline::never(),
        1,
        0,
        Timestamp(1),
    )
    .unwrap();
    store.save_legacy_checkpoint_baseline(&cp).await.unwrap();
    id
}
fn runtime(store: Arc<dyn SessionStore>, checkpoints: Arc<dyn CheckpointStore>) -> Runtime {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![],
        )))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(store)
        .checkpoint_store(checkpoints)
        .build()
        .unwrap()
}
async fn publication_count(f: &FileDb, id: &SessionId) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agent_session_journal_session WHERE runtime_session_id=? AND published=1")
        .bind(id.as_str()).fetch_one(f.database().pool()).await.unwrap()
}
async fn legacy(f: &FileDb, id: &SessionId) -> (String, String, i64) {
    sqlx::query_as("SELECT quote(snapshot_ciphertext), quote(checkpoint_ciphertext), state_revision FROM protected_agent_session_state WHERE session_id=?")
        .bind(format!("journal-fixture:{}",id.as_str())).fetch_one(f.database().pool()).await.unwrap()
}
async fn lazy_open(store: Arc<SqliteProtectedRuntimeStore>, id: SessionId) {
    let _admission = store.foreground_session(&id).await.unwrap();
    let rt = runtime(store.clone(), store);
    let handle = rt
        .start_session(
            StartSession::resume(id).with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
    drop(handle);
}

#[derive(Debug)]
struct NativeOnly(Arc<SqliteProtectedRuntimeStore>);
#[async_trait]
impl SessionStore for NativeOnly {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        SessionStore::journal(&*self.0)
    }
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        panic!("next native open attempted legacy import")
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("legacy save")
    }
}
#[async_trait]
impl CheckpointStore for NativeOnly {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        CheckpointStore::journal(&*self.0)
    }
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        panic!("next native open attempted legacy checkpoint import")
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("legacy save")
    }
}

#[tokio::test]
async fn shared_pre_topic_chat_converts_resume_target_and_supersedes_owner() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    use services_agents::session_journal_retention::SessionJournalRetention;
    let (f, store, worker) = fixture().await;
    let id = SessionId::new("shared-pre-topic-worker");
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    let older_id = f.ensure_session(&older).await.unwrap();
    let target = f.ensure_session(&id).await.unwrap();
    let retention = SessionJournalRetention::new(f.database(), &store);
    assert!(retention.should_retire(&older_id).await.unwrap());
    assert!(!retention.should_retire(&target).await.unwrap());
    let before = legacy(&f, &older).await;
    let mut cursor = ConversionCursor::default();
    let report = worker
        .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(report.converted, 1);
    assert_eq!(report.skipped_retired, 1);
    assert!(report.parked_sessions.is_empty());
    assert_eq!(report.remaining, 0);
    assert_eq!(publication_count(&f, &id).await, 2);
    assert_eq!(publication_count(&f, &older).await, 0);
    assert_eq!(legacy(&f, &older).await, before);
    let timeline: (String, i64, i64, i64) = sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_operation) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(timeline, (id.to_string(), 131, 114, 121));
    sqlx::query("UPDATE agent_session SET status='ready',version=version+1 WHERE id=?")
        .bind(&target)
        .execute(f.database().pool())
        .await
        .unwrap();
    assert!(retention.should_retire(&older_id).await.unwrap());
    assert!(!retention.should_retire(&target).await.unwrap());
    let retry = worker
        .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(retry.converted, 0);
    assert_eq!(retry.remaining, 0);
    assert!(retry.parked_sessions.is_empty());
}

#[tokio::test]
async fn shared_pre_topic_chat_reports_deferred_and_ambiguous_owners() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    for (case, reason) in [
        ("active", "LCM sibling owner is active"),
        (
            "no-selected-profile",
            "LCM shared chat has no selected resume target",
        ),
        (
            "tie",
            "LCM chat ownership is ambiguous: suspended sessions have the same creation time",
        ),
    ] {
        let (f, store, worker) = fixture().await;
        let id = SessionId::new(format!("park-shared-{case}"));
        let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
        let db = f.database();
        match case {
            "active" => {
                sqlx::query("UPDATE agent_session SET status='ready' WHERE runtime_session_id=?")
                    .bind(older.as_str())
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "no-selected-profile" => {
                sqlx::query(
                    "UPDATE agent_identity SET selected_profile_id=NULL WHERE id='journal-agent'",
                )
                .execute(db.pool())
                .await
                .unwrap();
            }
            "tie" => {
                sqlx::query("UPDATE agent_session SET created_at='2026-09-24T08:34:00Z' WHERE runtime_session_id=?")
                    .bind(older.as_str()).execute(db.pool()).await.unwrap();
            }
            _ => unreachable!(),
        }
        let before = legacy(&f, &id).await;
        let mut cursor = ConversionCursor::default();
        let report = worker
            .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
            .await
            .unwrap();
        if case == "active" {
            assert!(report.parked_sessions.is_empty());
            assert!(report.remaining > 0);
            assert_eq!(report.converted, 0);
            assert_eq!(publication_count(&f, &id).await, 0);
            sqlx::query("UPDATE agent_session SET status='suspended' WHERE runtime_session_id=?")
                .bind(older.as_str())
                .execute(db.pool())
                .await
                .unwrap();
            let mut converted = 0;
            for _ in 0..4 {
                let retried = worker
                    .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
                    .await
                    .unwrap();
                assert!(retried.parked_sessions.is_empty());
                converted += retried.converted;
                if retried.remaining == 0 {
                    break;
                }
            }
            assert_eq!(converted, 1);
            assert_eq!(publication_count(&f, &id).await, 2);
            assert_eq!(legacy(&f, &id).await, before);
            continue;
        }
        if case == "no-selected-profile" {
            assert_eq!(report.converted, 0);
            assert_eq!(report.skipped_shared_lcm, 1);
            assert!(report.parked_sessions.is_empty());
            assert_eq!(report.skipped_unreadable, 0);
            assert!(report.remaining > 0);
            assert_eq!(publication_count(&f, &id).await, 0);
            assert_eq!(publication_count(&f, &older).await, 0);
            assert_eq!(legacy(&f, &id).await, before);
            let owner: String =
                sqlx::query_scalar("SELECT runtime_session_id FROM agent_lcm_timeline WHERE id=?")
                    .bind(&old.timeline_id)
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
            assert_eq!(owner, older.as_str());
            continue;
        }
        let parked = report
            .parked_sessions
            .iter()
            .find(|s| s.runtime_session_id == id.as_str())
            .expect(case);
        assert_eq!(parked.error_kind, "Conflict", "{case}");
        assert_eq!(parked.error_message, reason, "{case}");
        assert_eq!(publication_count(&f, &id).await, 0, "{case}");
        assert_eq!(legacy(&f, &id).await, before, "{case}");
        let owner: String =
            sqlx::query_scalar("SELECT runtime_session_id FROM agent_lcm_timeline WHERE id=?")
                .bind(&old.timeline_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, older.as_str(), "{case}");
    }
}

#[tokio::test]
async fn profile_switch_worker_converts_old_owner_before_target_and_keeps_it_resumable() {
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    use services_agents::session_journal_retention::SessionJournalRetention;
    let (f, store, worker) = fixture().await;
    let id = SessionId::new("profile-switch-worker");
    let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
    let db = f.database();
    let original_owner = f.ensure_session(&older).await.unwrap();
    // Put A before B in the worker's keyset page, as opaque production ids can.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("PRAGMA defer_foreign_keys=ON")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_session SET id='000-old-profile-owner' WHERE id=?")
        .bind(&original_owner)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET session_id='000-old-profile-owner' WHERE session_id=?")
        .bind(&original_owner).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let older_owner = f.ensure_session(&older).await.unwrap();
    assert_eq!(older_owner, "000-old-profile-owner");
    let protected = CheckpointStore::load_latest(&*store, &older)
        .await
        .unwrap()
        .unwrap();
    let before: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(
        store
            .convert_legacy_session(&older_owner, &older)
            .await
            .unwrap(),
        ConversionOutcome::Converted
    );
    let mut cursor = ConversionCursor::default();
    let (mut converted, mut retired) = (0, 0);
    let mut complete = false;
    for _ in 0..6 {
        let report = worker
            .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
            .await
            .unwrap();
        converted += report.converted;
        retired += report.skipped_retired;
        assert!(report.parked_sessions.is_empty());
        if report.remaining == 0 {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!((converted, retired), (1, 11));
    assert_eq!(publication_count(&f, &id).await, 2);
    assert_eq!(publication_count(&f, &older).await, 2);
    let a: (String, Option<String>, i64) = sqlx::query_as(
        "SELECT status,replaced_by_session_id,version FROM agent_session WHERE id=?",
    )
    .bind(&older_owner)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(a, ("suspended".into(), None, 1));
    let retention = SessionJournalRetention::new(db.clone(), &store);
    assert!(!retention.should_retire(&older_owner).await.unwrap());
    let retry = worker
        .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(retry.converted, 0);
    assert_eq!(retry.remaining, 0);
    assert!(retry.parked_sessions.is_empty());
    sqlx::query("UPDATE agent_identity SET selected_profile_id='glm-profile',version=version+1 WHERE id='journal-agent'")
        .execute(db.pool()).await.unwrap();
    let resumed = worker
        .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(resumed.converted, 0);
    assert_eq!(resumed.remaining, 0);
    assert!(resumed.parked_sessions.is_empty());
    assert_eq!(publication_count(&f, &older).await, 2);
    let mut expected = protected.snapshot;
    expected
        .extension_state
        .remove(agent_runtime::harness::LCM_COMPONENT_ID);
    let engine = store.journals().protected();
    let lease = engine
        .open(
            &older,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("profile-switch-read"),
                pin: JournalPinId::new("profile-switch-read"),
                intent: JournalOpenIntent::ReadOnly,
            },
        )
        .await
        .unwrap();
    let imported = JournalReader::new(
        &engine,
        &lease,
        JournalReadLimits {
            objects: 100_000,
            encoded_bytes: 32 * 1024 * 1024,
            entries: 100_000,
        },
    )
    .unwrap()
    .snapshot()
    .await
    .unwrap()
    .unwrap();
    assert_eq!(imported, expected);
    engine.close(lease).await.unwrap();
    let after: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(before, after);
    let owner: String =
        sqlx::query_scalar("SELECT runtime_session_id FROM agent_lcm_timeline WHERE id=?")
            .bind(&old.timeline_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(owner, id.as_str());
}

#[tokio::test]
async fn unowned_profile_switch_worker_handles_both_cursor_orders() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    for a_first in [true, false] {
        let (f, store, worker) = fixture().await;
        let id = SessionId::new(format!("unowned-worker-order-{a_first}"));
        let (old, older) =
            LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        let first = if a_first { &older } else { &id };
        let original = f.ensure_session(first).await.unwrap();
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("PRAGMA defer_foreign_keys=ON")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE agent_session SET id='000-first-owner' WHERE id=?")
            .bind(&original)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE protected_agent_session_state SET session_id='000-first-owner' WHERE session_id=?")
            .bind(&original).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
        let before: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
            .fetch_all(db.pool()).await.unwrap();
        for cold in [false, true] {
            let mut cursor = ConversionCursor::default();
            let (mut converted, mut retired, mut complete) = (0, 0, false);
            for _ in 0..6 {
                let report = worker
                    .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
                    .await
                    .unwrap();
                converted += report.converted;
                retired += report.skipped_retired;
                assert!(report.parked_sessions.is_empty());
                assert_eq!(report.skipped_unreadable, 0);
                assert_eq!(report.skipped_active, 0);
                if report.remaining == 0 {
                    complete = true;
                    break;
                }
            }
            assert!(complete);
            assert_eq!((converted, retired), (if cold { 0 } else { 2 }, 9));
        }
        assert_eq!(publication_count(&f, &id).await, 2);
        assert_eq!(publication_count(&f, &older).await, 2);
        let ownership: (String, i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?")
            .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(ownership, (id.to_string(), 10));
        let after: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
            .fetch_all(db.pool()).await.unwrap();
        assert_eq!(before, after);
    }
}

#[tokio::test]
async fn shared_profile_switch_defers_own_timeline_without_selected_resume_target() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    for empty_profile in [false, true] {
        let (f, store, worker) = fixture().await;
        let id = SessionId::new("missing-profile-target");
        let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        let selected = if empty_profile {
            sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('empty-profile','journal-agent','native','embedded','c','u')")
                .execute(db.pool()).await.unwrap();
            Some("empty-profile")
        } else {
            None
        };
        sqlx::query("UPDATE agent_identity SET selected_profile_id=? WHERE id='journal-agent'")
            .bind(selected)
            .execute(db.pool())
            .await
            .unwrap();
        let mut cursor = ConversionCursor::default();
        let mut deferred = 0;
        for _ in 0..6 {
            let report = worker
                .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
                .await
                .unwrap();
            assert_eq!(report.converted, 0);
            assert!(report.parked_sessions.is_empty());
            assert_eq!(report.skipped_unreadable, 0);
            assert!(report.remaining > 0);
            deferred += report.skipped_shared_lcm;
        }
        assert!(deferred >= 2, "same cursor must retry deferred siblings");
        assert_eq!(publication_count(&f, &id).await, 0);
        assert_eq!(publication_count(&f, &older).await, 0);
        let owner: String =
            sqlx::query_scalar("SELECT runtime_session_id FROM agent_lcm_timeline WHERE id=?")
                .bind(&old.timeline_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(owner, older.as_str());
        let receipts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(receipts, 0);
    }
}

#[tokio::test]
async fn unselected_shared_lcm_sweep_defers_until_foreground_resume_with_receipts() {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
        LCM_COMPONENT_ID,
    };
    use forge_agent_host::test_support::LegacyLcmFixture;
    for case in ["empty-profile", "topic", "failed", "cancelled"] {
        let (f, store, worker) = fixture().await;
        let id = SessionId::new(format!("deferred-shared-{case}"));
        let (old, older) =
            LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        if case == "topic" {
            let topic = old.bind_empty_replacement(&f).await;
            let mut target = old.checkpoint.snapshot.clone();
            target
                .extension_state
                .get_mut(LCM_COMPONENT_ID)
                .unwrap()
                .value["timeline_id"] = serde_json::json!(topic.timeline_id());
            let checkpoint = TurnCheckpoint::session_boundary(target.clone(), 6).unwrap();
            store
                .seed_legacy_bytes(
                    &id,
                    Some(&serde_json::to_vec(&target).unwrap()),
                    Some(&serde_json::to_vec(&checkpoint).unwrap()),
                    false,
                )
                .await
                .unwrap();
        } else {
            sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('empty-profile','journal-agent','native','embedded','c','u')").execute(db.pool()).await.unwrap();
            sqlx::query("UPDATE agent_identity SET selected_profile_id='empty-profile' WHERE id='journal-agent'").execute(db.pool()).await.unwrap();
        }
        if matches!(case, "failed" | "cancelled") {
            let failed = SessionId::new(format!("{id}:glm:0"));
            let owner = f.ensure_session(&failed).await.unwrap();
            let mut tx = db::begin_immediate(db.pool()).await.unwrap();
            sqlx::query("PRAGMA defer_foreign_keys=ON")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE agent_session SET id='000-first-owner',status=? WHERE id=?")
                .bind(case)
                .bind(&owner)
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE protected_agent_session_state SET session_id='000-first-owner' WHERE session_id=?").bind(&owner).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            let first = db.legacy_journal_conversion_page("", 1, &[]).await.unwrap();
            assert_eq!(first[0], ("000-first-owner".into(), failed.to_string()));
        }
        let before = legacy(&f, &older).await;
        let mut cursor = ConversionCursor::default();
        let mut deferred = 0;
        let mut converted = 0;
        for _ in 0..6 {
            let report = worker
                .convert_with_budget_for_test(&mut cursor, Duration::from_secs(60))
                .await
                .unwrap();
            assert!(report.parked_by_reason.is_empty(), "{case}");
            assert!(report.parked_sessions.is_empty(), "{case}");
            assert_eq!(report.skipped_unreadable, 0, "{case}");
            assert_eq!(report.conflicts, 0, "{case}");
            assert!(report.remaining > 0, "{case}");
            deferred += report.skipped_shared_lcm;
            converted += report.converted;
        }
        assert!(
            deferred >= 2,
            "{case} must remain retryable with the same cursor"
        );
        assert_eq!(converted, usize::from(case == "topic"), "{case}");
        let facts: (Option<String>, i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment) FROM agent_lcm_timeline WHERE id=?")
            .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(facts, (None, 0), "{case}");
        let other_publications: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session_journal_session WHERE published=1 AND runtime_session_id<>?")
            .bind(id.as_str()).fetch_one(db.pool()).await.unwrap();
        assert_eq!(other_publications, 0, "{case}");
        assert_eq!(publication_count(&f, &older).await, 0, "{case}");
        assert_eq!(legacy(&f, &older).await, before, "{case}");

        sqlx::query(
            "UPDATE agent_identity SET selected_profile_id='glm-profile' WHERE id='journal-agent'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let chosen: String =
            sqlx::query_scalar("SELECT id FROM agent_session WHERE runtime_session_id=?")
                .bind(older.as_str())
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            db.dormant_native_chat_session_id("journal-agent", "journal-scope", "glm-profile")
                .await
                .unwrap(),
            Some(chosen)
        );
        let expected = SessionStore::load(&*store, &older).await.unwrap().unwrap();
        let _admission = store.foreground_session(&older).await.unwrap();
        let lcm = store
            .lcm_store_for_runtime_session(older.as_str(), "agent_chat", &format!("chat:{id}"))
            .await
            .unwrap();
        assert_eq!(lcm.timeline_id(), old.timeline_id, "{case}");
        let binding = lcm.runtime_binding(older.clone()).unwrap();
        let coordinator = LcmCoordinator::new(
            Arc::new(lcm),
            Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
            Arc::new(StaticLcmTimelineResolver::new(binding)),
            LcmCoordinatorPolicy {
                input_budget_tokens: 28_000,
                ..Default::default()
            },
        )
        .unwrap();
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![],
        ));
        let rt = RuntimeBuilder::new(ModelId::new("fake"))
            .provider_name("fake")
            .provider(provider.clone())
            .model_profile(ResolvedModelProfile::explicit(
                "fake",
                ModelId::new("fake"),
                ModelLimits::new(32_000, 28_000, 4_000),
            ))
            .session_store(store.clone())
            .checkpoint_store(store.clone())
            .lcm(Arc::new(coordinator))
            .build()
            .unwrap();
        let session = rt
            .start_session(
                StartSession::resume(older.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer)
                    .with_lcm_policy(LcmRecoveryPolicy::Adopt),
            )
            .await
            .unwrap();
        assert_eq!(session.history(), expected.history.as_slice(), "{case}");
        assert!(provider.requests().is_empty(), "{case}");
        drop(session);
        drop(rt);
        let facts: (String, i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?")
            .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            facts,
            (older.to_string(), if case == "topic" { 9 } else { 10 }),
            "{case}"
        );
    }
}

#[tokio::test]
async fn background_converts_dormant_next_open_native_only() {
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "dormant").await;
    let before = legacy(&f, &id).await;
    let registry = services::worker_runtime::PeriodicWorkers::new(f.database());
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let running = worker.clone().start(&registry, shutdown);
    tokio::time::timeout(Duration::from_secs(5), async {
        while publication_count(&f, &id).await != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("supervised background worker must convert without an open");
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
    let report = worker
        .convert_once(&mut ConversionCursor::default())
        .await
        .unwrap();
    assert_eq!((report.converted, report.remaining), (0, 0));
    assert_eq!(publication_count(&f, &id).await, 2);
    assert_eq!(legacy(&f, &id).await, before);
    let only = Arc::new(NativeOnly(store));
    let rt = runtime(only.clone(), only);
    let handle = rt
        .start_session(
            StartSession::resume(id).with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .unwrap();
    assert_eq!(handle.history()[0].joined_text(), "accepted legacy input");
    drop(handle);
}

#[tokio::test]
async fn background_skips_active_turn_then_converts() {
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "active").await;
    let admission = store.foreground_session(&id).await.unwrap();
    let mut cursor = ConversionCursor::default();
    let report = worker.convert_once(&mut cursor).await.unwrap();
    assert_eq!(
        (report.skipped_active, report.converted, report.remaining),
        (1, 0, 1)
    );
    assert_eq!(publication_count(&f, &id).await, 0);
    drop(admission);
    // A writer bypassing host admission must also win over conversion.
    let engine = store.journals().protected();
    let lease = engine
        .open(
            &id,
            JournalOpen {
                root: JournalRootSelection::Latest,
                expected_revision: None,
                operation: JournalOperationId::new("held-turn"),
                pin: JournalPinId::new("held-turn"),
                intent: JournalOpenIntent::ExclusiveWriter,
            },
        )
        .await
        .unwrap();
    let report = worker
        .convert_once(&mut ConversionCursor::default())
        .await
        .unwrap();
    assert_eq!((report.skipped_active, report.converted), (1, 0));
    engine.close(lease).await.unwrap();
    worker.convert_once(&mut cursor).await.unwrap(); // wrap the exhausted cursor
    let report = worker.convert_once(&mut cursor).await.unwrap();
    assert_eq!((report.converted, report.remaining), (1, 0));
    assert_eq!(publication_count(&f, &id).await, 2);
}

#[tokio::test]
async fn background_lazy_import_races_both_orders() {
    for background_first in [false, true] {
        let (f, store, worker) = fixture().await;
        let id = seed(&f, &store, "race").await;
        let before = legacy(&f, &id).await;
        if background_first {
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            store.pause_conversion_for_test(barrier.clone());
            let converting = tokio::spawn({
                let worker = worker.clone();
                async move {
                    worker
                        .convert_once(&mut ConversionCursor::default())
                        .await
                        .unwrap()
                }
            });
            tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                .await
                .expect("conversion race barrier");
            let opening = tokio::spawn(lazy_open(store.clone(), id.clone()));
            tokio::task::yield_now().await;
            assert!(
                !opening.is_finished(),
                "foreground waits for in-flight background admission"
            );
            tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                .await
                .expect("conversion race barrier");
            assert_eq!(converting.await.unwrap().converted, 1);
            opening.await.unwrap();
        } else {
            let admission = store.foreground_session(&id).await.unwrap();
            let opening = tokio::spawn(lazy_open(store.clone(), id.clone()));
            let report = worker
                .convert_once(&mut ConversionCursor::default())
                .await
                .unwrap();
            assert_eq!(report.skipped_active, 1);
            opening.await.unwrap();
            drop(admission);
            // Feed a previously enumerated candidate to the idempotent path.
            assert_eq!(
                store
                    .convert_legacy_session(&format!("journal-fixture:{}", id.as_str()), &id)
                    .await
                    .unwrap(),
                forge_agent_host::session_journal_conversion::ConversionOutcome::AlreadyNative
            );
        }
        assert_eq!(publication_count(&f, &id).await, 2);
        let revisions: Vec<Vec<u8>> = sqlx::query_scalar("SELECT revision FROM agent_session_journal_session WHERE runtime_session_id=? AND published=1 ORDER BY role")
            .bind(id.as_str()).fetch_all(f.database().pool()).await.unwrap();
        assert_eq!(
            revisions,
            vec![1_u64.to_be_bytes().to_vec(), 1_u64.to_be_bytes().to_vec()],
            "each domain publishes the import exactly once"
        );
        assert_eq!(legacy(&f, &id).await, before);
        let report = worker
            .convert_once(&mut ConversionCursor::default())
            .await
            .unwrap();
        assert_eq!((report.converted, report.remaining), (0, 0));
    }
}

#[tokio::test]
async fn background_unreadable_legacy_preserved() {
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "a-unreadable").await;
    let good = seed(&f, &store, "b-healthy").await;
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=X'000102' WHERE session_id=?")
        .bind(format!("journal-fixture:{}",id.as_str())).execute(f.database().pool()).await.unwrap();
    let before = legacy(&f, &id).await;
    use tracing::instrument::WithSubscriber;
    let events = forge_agent_host::test_support::JournalEvents::default();
    let mut cursor = ConversionCursor::default();
    for pass in 0..3 {
        let report = worker
            .convert_once(&mut cursor)
            .with_subscriber(events.clone())
            .await
            .unwrap();
        assert_eq!(
            (report.skipped_unreadable, report.remaining),
            (usize::from(pass == 0), 0),
            "unreadable rows are parked rather than decoded every cycle"
        );
        assert_eq!(legacy(&f, &id).await, before);
        assert_eq!(publication_count(&f, &id).await, 0);
    }
    assert_eq!(publication_count(&f, &good).await, 2);
    let rows = events.rows.lock().unwrap();
    assert_eq!(
        rows.len(),
        1,
        "parking must not bypass the capped unreadable warning tracker"
    );
    assert_eq!(
        rows[0].1["runtime_session_id"],
        format!("{:?}", id.as_str())
    );
}

#[tokio::test]
async fn background_tick_time_budget_and_count_cap() {
    let (f, store, worker) = fixture().await;
    for n in 0..20 {
        seed(&f, &store, &format!("budget-{n:02}")).await;
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    store.pause_conversion_for_test(barrier.clone());
    let converting = tokio::spawn({
        let worker = worker.clone();
        async move {
            worker
                .convert_with_budget_for_test(
                    &mut ConversionCursor::default(),
                    Duration::from_secs(1),
                )
                .await
                .unwrap()
        }
    });
    tokio::time::timeout(Duration::from_secs(5), barrier.wait())
        .await
        .expect("conversion must start a candidate");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    barrier.wait().await;
    let report = converting.await.unwrap();
    assert_eq!(
        (report.examined, report.converted, report.remaining),
        (1, 1, 19)
    );
    assert!(
        report.budget_exhausted,
        "no second conversion after budget expired"
    );
    let report = worker
        .convert_with_budget_for_test(&mut ConversionCursor::default(), Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(report.examined, CONVERSION_CANDIDATE_LIMIT as usize);
    assert_eq!((report.converted, report.remaining), (8, 11));
}

#[tokio::test]
async fn background_worker_restart_resumes_durable_progress() {
    let (f, store, worker) = fixture().await;
    for n in 0..11 {
        seed(&f, &store, &format!("restart-{n:02}")).await;
    }
    let report = worker
        .convert_with_budget_for_test(&mut ConversionCursor::default(), Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!((report.converted, report.remaining), (8, 3));
    drop(worker);
    drop(store);
    let db = f.reopen().await.unwrap();
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
    let restarted = SessionJournalConversion::new(db, cold.clone());
    let report = restarted
        .convert_once(&mut ConversionCursor::default())
        .await
        .unwrap();
    assert_eq!(
        (report.examined, report.converted, report.remaining),
        (3, 3, 0)
    );
    for n in 0..11 {
        assert_eq!(
            publication_count(&f, &SessionId::new(format!("restart-{n:02}"))).await,
            2
        );
    }
}

#[tokio::test]
async fn background_skips_replaced_and_suspended_task_sessions() {
    let (f, store, worker) = fixture().await;
    let replaced = seed(&f, &store, "dead-replaced-task").await;
    let suspended = seed(&f, &store, "dead-suspended-task").await;
    let good = seed(&f, &store, "resumable-chat").await;
    let db = f.database();
    for sql in [
        "INSERT INTO project (id,name,settings,workflow_definition,created_at,updated_at) VALUES ('conversion-project','Conversion','{}','{}','c','u')",
        "INSERT INTO task (id,project_id,title,status,created_at,updated_at) VALUES ('conversion-task','conversion-project','Done task','done','c','u')",
        "INSERT INTO agent_context_scope (id,identity_id,scope_type,scope_id,project_id,task_id,task_role,workspace_access,created_at,updated_at) VALUES ('conversion-task-scope','journal-agent','task','conversion-task','conversion-project','conversion-task','worker','task_write','c','u')",
    ] { sqlx::query(sql).execute(db.pool()).await.unwrap(); }
    for (id, status) in [(&replaced, "replaced"), (&suspended, "suspended")] {
        sqlx::query("UPDATE agent_session SET context_scope_id='conversion-task-scope',status=? WHERE runtime_session_id=?")
            .bind(status).bind(id.as_str()).execute(db.pool()).await.unwrap();
    }
    let before = [legacy(&f, &replaced).await, legacy(&f, &suspended).await];
    let mut cursor = ConversionCursor::default();
    let report = worker.convert_once(&mut cursor).await.unwrap();
    assert_eq!(report.converted, 1, "only the resumable chat is imported");
    assert_eq!(
        report.remaining, 0,
        "unresumable sessions are excluded from pending work"
    );
    assert_eq!(publication_count(&f, &replaced).await, 0);
    assert_eq!(publication_count(&f, &suspended).await, 0);
    assert_eq!(publication_count(&f, &good).await, 2);
    assert_eq!(
        [legacy(&f, &replaced).await, legacy(&f, &suspended).await],
        before
    );
    assert_eq!(worker.convert_once(&mut cursor).await.unwrap().converted, 0);
}

#[tokio::test]
async fn nonretryable_conversion_errors_park_and_cursor_finishes() {
    for error in [
        RuntimeError::conflict("validation conflict"),
        RuntimeError::not_found("missing owner"),
        RuntimeError::config("invalid builder"),
        RuntimeError::internal("ambiguous host mapping"),
    ] {
        let (f, store, worker) = fixture().await;
        let bad = seed(&f, &store, "a-parked").await;
        let good = seed(&f, &store, "b-healthy").await;
        let before = legacy(&f, &bad).await;
        let reason = format!("{:?}", error.kind);
        let message = error.message.clone();
        store.fail_conversion_for_test(error);
        let mut cursor = ConversionCursor::default();
        let first = worker.convert_once(&mut cursor).await.unwrap();
        assert_eq!(first.parked_by_reason.get(&reason), Some(&1));
        assert_eq!(first.parked_sessions.len(), 1);
        let parked = &first.parked_sessions[0];
        assert_eq!(parked.forge_session_id, "journal-fixture:a-parked");
        assert_eq!(parked.runtime_session_id, bad.as_str());
        assert_eq!(parked.owner_kind, "agent_chat");
        assert_eq!(parked.error_kind, reason);
        assert_eq!(parked.error_message, message);
        assert_eq!(
            (first.examined, first.converted, first.remaining),
            (2, 1, 0)
        );
        for _ in 0..3 {
            let next = worker.convert_once(&mut cursor).await.unwrap();
            assert!(next.parked_sessions.is_empty());
            assert_eq!(
                (next.examined, next.converted, next.remaining),
                (0, 0, 0),
                "{reason} is not retried every cycle"
            );
        }
        assert_eq!(publication_count(&f, &bad).await, 0);
        assert_eq!(publication_count(&f, &good).await, 2);
        assert_eq!(legacy(&f, &bad).await, before);
    }
}

#[tokio::test]
async fn retryable_conversion_error_is_retried_with_same_cursor() {
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "retryable-lcm-binding").await;
    let before = legacy(&f, &id).await;
    store.fail_conversion_for_test(
        RuntimeError::internal("transient LCM binding failure").retryable(),
    );
    let mut cursor = ConversionCursor::default();
    let first = worker.convert_once(&mut cursor).await.unwrap();
    assert_eq!(
        (
            first.examined,
            first.converted,
            first.remaining,
            first.conflicts
        ),
        (1, 0, 1, 0)
    );
    assert!(first.parked_by_reason.is_empty());
    assert!(first.parked_sessions.is_empty());
    assert_eq!(publication_count(&f, &id).await, 0);
    // A keyset sweep resets at the end; the same worker/cursor retries next sweep.
    assert_eq!(worker.convert_once(&mut cursor).await.unwrap().remaining, 1);
    let retried = worker.convert_once(&mut cursor).await.unwrap();
    assert_eq!((retried.converted, retried.remaining), (1, 0));
    assert!(retried.parked_by_reason.is_empty());
    assert_eq!(legacy(&f, &id).await, before);
}

#[tokio::test]
async fn historical_main_chat_lcm_converts_with_unchanged_policy_markers_and_transcript() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    for (marker, snapshot_has_lcm) in [
        (None, false),
        (Some("old-policy"), true),
        (Some("forge-lcm-policy-2"), false),
    ] {
        let (f, store, worker) = fixture().await;
        let id = seed(&f, &store, "historical-main").await;
        let old = LegacyLcmFixture::seed(&f, &store, &id, snapshot_has_lcm).await;
        let transcript: Vec<(i64, String)> =
            sqlx::query_as("SELECT sequence,content FROM agent_chat_message ORDER BY sequence")
                .fetch_all(f.database().pool())
                .await
                .unwrap();
        assert_eq!(transcript.len(), 120);
        sqlx::query("UPDATE protected_agent_session_state SET lcm_policy_revision=?, checkpoint_lcm_policy_revision=NULL")
            .bind(marker).execute(f.database().pool()).await.unwrap();
        let before = legacy(&f, &id).await;
        let report = worker
            .convert_with_budget_for_test(&mut ConversionCursor::default(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            (report.converted, report.remaining, report.conflicts),
            (1, 0, 0)
        );
        assert!(report.parked_sessions.is_empty());
        assert_eq!(publication_count(&f, &id).await, 2);
        assert_eq!(legacy(&f, &id).await, before);
        let after_transcript: Vec<(i64, String)> =
            sqlx::query_as("SELECT sequence,content FROM agent_chat_message ORDER BY sequence")
                .fetch_all(f.database().pool())
                .await
                .unwrap();
        assert_eq!(after_transcript, transcript);
        let after: (Option<String>, Option<String>) = sqlx::query_as("SELECT lcm_policy_revision, checkpoint_lcm_policy_revision FROM protected_agent_session_state")
            .fetch_one(f.database().pool()).await.unwrap();
        assert_eq!(after, (marker.map(str::to_owned), None));
        let binding: String = sqlx::query_scalar(
            "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
        )
        .bind(id.as_str())
        .fetch_one(f.database().pool())
        .await
        .unwrap();
        assert_eq!(binding, old.timeline_id);
        let saved = f
            .database()
            .session_persistence_flags(&format!("journal-fixture:{}", id.as_str()))
            .await
            .unwrap();
        assert!(saved.saved && saved.checkpoint_present && !saved.retired);
    }
}

#[tokio::test]
async fn corrupt_historical_lcm_identity_binding_and_frontier_stay_parked() {
    use agent_runtime::harness::LCM_COMPONENT_ID;
    use forge_agent_host::test_support::LegacyLcmFixture;
    for (key, value) in [
        ("schema_version", serde_json::json!(999)),
        ("binding_revision", serde_json::json!("wrong-binding")),
        ("store_revision", serde_json::json!("wrong-store")),
        ("classifier_revision", serde_json::json!("wrong-classifier")),
        ("immutable_frontier", serde_json::json!(0)),
        ("history_len", serde_json::json!(121)),
        ("timeline_id", serde_json::json!("another-timeline")),
    ] {
        let (f, store, worker) = fixture().await;
        let id = seed(&f, &store, "corrupt-historical").await;
        let mut old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
        old.checkpoint
            .snapshot
            .extension_state
            .get_mut(LCM_COMPONENT_ID)
            .unwrap()
            .value[key] = value;
        old.write(&store).await;
        let before = legacy(&f, &id).await;
        let mut cursor = ConversionCursor::default();
        let report = worker
            .convert_with_budget_for_test(&mut cursor, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            (report.converted, report.conflicts, report.remaining),
            (0, 1, 0),
            "{key}"
        );
        assert_eq!(report.parked_by_reason.get("Conflict"), Some(&1));
        assert_eq!(report.parked_sessions[0].runtime_session_id, id.as_str());
        assert_eq!(publication_count(&f, &id).await, 0);
        assert_eq!(legacy(&f, &id).await, before);
        assert_eq!(worker.convert_once(&mut cursor).await.unwrap().examined, 0);
    }
}

#[tokio::test]
async fn populated_lcm_binding_mismatch_is_not_rebound() {
    use agent_runtime::{context::Sensitivity, lcm::*, registry::TrustClass};
    use forge_agent_host::test_support::LegacyLcmFixture;
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "occupied-mismatch").await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let occupied = old.bind_empty_replacement(&f).await;
    let occupied_id = occupied.timeline_id().to_owned();
    occupied
        .append(
            &occupied.view(),
            LcmAppendRequest::new(
                LcmOperationId::new("occupied-append"),
                vec![LcmEntry::new(
                    LcmTimelineId::new(&occupied_id),
                    LcmEntryId::new("occupied"),
                    LcmSequence::new(0),
                    Message::user("another timeline's retained content"),
                    LcmSourceMetadata::new(LcmClassification::new(
                        Sensitivity::Sensitive,
                        TrustClass::UserContent,
                    )),
                )],
            ),
        )
        .await
        .unwrap();
    let before = legacy(&f, &id).await;
    let report = worker
        .convert_with_budget_for_test(&mut ConversionCursor::default(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!((report.converted, report.conflicts), (0, 1));
    assert_eq!(publication_count(&f, &id).await, 0);
    assert_eq!(legacy(&f, &id).await, before);
    let binding: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(binding, occupied_id);
    assert_eq!(
        occupied
            .load_range(&occupied.view(), LcmRange::single(LcmSequence::new(0)), 1)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[derive(Debug)]
struct LegacyState(Arc<SqliteProtectedRuntimeStore>);
#[async_trait]
impl SessionStore for LegacyState {
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        SessionStore::load(&*self.0, id).await
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.0.save_legacy_snapshot_baseline(snapshot).await
    }
}
#[async_trait]
impl CheckpointStore for LegacyState {
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        CheckpointStore::load_latest(&*self.0, id).await
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        self.0.save_legacy_checkpoint_baseline(checkpoint).await
    }
}

async fn lcm_runtime(
    store: Arc<SqliteProtectedRuntimeStore>,
    sessions: Arc<dyn SessionStore>,
    checkpoints: Arc<dyn CheckpointStore>,
    id: &SessionId,
) -> Runtime {
    use agent_runtime::{
        core::provider::{FinishReason, ProviderStreamEvent},
        harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
        provider::fake::ScriptedStream,
    };
    let lcm_store = store
        .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &format!("chat:{}", id.as_str()))
        .await
        .unwrap();
    let binding = lcm_store.runtime_binding(id.clone()).unwrap();
    let coordinator = LcmCoordinator::new(
        Arc::new(lcm_store),
        Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 28_000,
            ..Default::default()
        },
    )
    .unwrap();
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            vec![ScriptedStream::new(vec![
                ProviderStreamEvent::TextDelta {
                    text: "LCM retained reply".into(),
                },
                ProviderStreamEvent::Finish {
                    reason: FinishReason::Stop,
                },
            ])],
        )))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(sessions)
        .checkpoint_store(checkpoints)
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap()
}

#[tokio::test]
async fn background_converts_agent_chat_lcm_and_next_turn_is_native_only() {
    use agent_runtime::harness::LCM_COMPONENT_ID;
    let (f, store, worker) = fixture().await;
    let id = seed(&f, &store, "lcm-chat").await;
    sqlx::query("DELETE FROM protected_agent_session_state WHERE session_id=?")
        .bind(format!("journal-fixture:{}", id.as_str()))
        .execute(f.database().pool())
        .await
        .unwrap();
    let old = Arc::new(LegacyState(store.clone()));
    let rt = lcm_runtime(store.clone(), old.clone(), old, &id).await;
    let session = rt
        .start_session(StartSession::create(
            id.clone(),
            vec![Message::user("legacy LCM history")],
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("legacy LCM turn"))
        .await
        .unwrap()
        .completed()
        .await;
    session.shutdown().await.unwrap();
    drop(session);
    drop(rt);
    let snapshot = SessionStore::load(&*store, &id).await.unwrap().unwrap();
    assert!(
        snapshot.extension_state.contains_key(LCM_COMPONENT_ID),
        "fixture must exercise needs_lcm"
    );
    let before = legacy(&f, &id).await;
    let report = worker
        .convert_once(&mut ConversionCursor::default())
        .await
        .unwrap();
    assert_eq!((report.converted, report.remaining), (1, 0));
    assert_eq!(publication_count(&f, &id).await, 2);
    assert_eq!(legacy(&f, &id).await, before);
    drop(worker);
    drop(store);
    let cold = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let only = Arc::new(NativeOnly(cold.clone()));
    let rt = lcm_runtime(cold, only.clone(), only, &id).await;
    let session = rt
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert_eq!(session.history(), snapshot.history.as_slice());
    session
        .run(UserInput::text("native-only LCM turn"))
        .await
        .unwrap()
        .completed()
        .await;
    assert!(session
        .history()
        .iter()
        .any(|message| message.joined_text() == "native-only LCM turn"));
    assert_eq!(
        session.history().last().unwrap().joined_text(),
        "LCM retained reply"
    );
    session.shutdown().await.unwrap();
    assert_eq!(
        legacy(&f, &id).await,
        before,
        "next native turn never rewrites legacy state"
    );
}

#[tokio::test]
async fn corrupt_sibling_is_parked_without_blocking_selected_target() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    let (f, store, worker) = fixture().await;
    let id = SessionId::new("corrupt-sibling-target");
    let (old, older) = LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
    let source = f.ensure_session(&older).await.unwrap();
    store
        .seed_legacy_bytes(
            &older,
            Some(b"{invalid sibling"),
            Some(b"{invalid sibling"),
            false,
        )
        .await
        .unwrap();
    let mut cursor = ConversionCursor::default();
    let mut parked = Vec::new();
    let mut unreadable = 0;
    let mut converted = 0;
    for _ in 0..5 {
        let report = worker
            .convert_with_budget_for_test(&mut cursor, Duration::from_secs(30))
            .await
            .unwrap();
        converted += report.converted;
        unreadable += report.skipped_unreadable;
        parked.extend(report.parked_sessions);
        if report.remaining == 0 {
            break;
        }
    }
    assert_eq!(converted, 1);
    assert_eq!(unreadable, 1);
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].forge_session_id, source);
    assert_eq!(parked[0].runtime_session_id, older.to_string());
    assert_eq!(parked[0].error_kind, "Serialization");
    let owner = f.ensure_session(&id).await.unwrap();
    assert!(
        f.database()
            .session_persistence_flags(&owner)
            .await
            .unwrap()
            .ordinary_published
    );
    let facts:(String,i64)=sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?").bind(&old.timeline_id).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(facts, (id.to_string(), 114));
}

#[tokio::test]
async fn active_selected_target_is_retried_by_the_same_worker_cursor() {
    use forge_agent_host::test_support::LegacyLcmFixture;
    let (f, store, worker) = fixture().await;
    let id = SessionId::new("active-selected-target");
    LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
    sqlx::query("UPDATE agent_session SET status='ready' WHERE runtime_session_id=?")
        .bind(id.as_str())
        .execute(f.database().pool())
        .await
        .unwrap();
    let mut cursor = ConversionCursor::default();
    let first = worker
        .convert_with_budget_for_test(&mut cursor, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(first.parked_sessions.is_empty());
    assert_eq!(first.skipped_unreadable, 0);
    assert!(first.remaining > 0);
    sqlx::query("UPDATE agent_session SET status='suspended' WHERE runtime_session_id=?")
        .bind(id.as_str())
        .execute(f.database().pool())
        .await
        .unwrap();
    let mut converted = first.converted;
    for _ in 0..5 {
        let report = worker
            .convert_with_budget_for_test(&mut cursor, Duration::from_secs(30))
            .await
            .unwrap();
        assert!(report.parked_sessions.is_empty());
        converted += report.converted;
        if report.remaining == 0 {
            break;
        }
    }
    assert_eq!(converted, 2);
}
