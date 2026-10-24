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
async fn every_conversion_error_kind_parks_and_cursor_finishes() {
    for error in [
        RuntimeError::conflict("validation conflict"),
        RuntimeError::not_found("missing owner"),
        RuntimeError::config("invalid builder"),
        RuntimeError::internal("ambiguous host mapping"),
        RuntimeError::internal("transient database failure").retryable(),
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
