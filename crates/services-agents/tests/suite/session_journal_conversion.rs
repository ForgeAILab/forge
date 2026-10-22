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
async fn seed(f: &FileDb, store: &SqliteProtectedRuntimeStore, name: &str) -> SessionId {
    let id = SessionId::new(name);
    f.ensure_session(&id).await.unwrap();
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
    for _ in 0..2 {
        let report = worker
            .convert_once(&mut ConversionCursor::default())
            .await
            .unwrap();
        assert_eq!((report.skipped_unreadable, report.remaining), (1, 1));
        assert_eq!(legacy(&f, &id).await, before);
        assert_eq!(publication_count(&f, &id).await, 0);
    }
    assert_eq!(publication_count(&f, &good).await, 2);
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
