//! Shared fixtures for session journal runtime tests.

use super::*;

pub(super) fn provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        (0..4)
            .map(|_| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: "reply".into(),
                    },
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect(),
    ))
}

pub(super) fn runtime_builder(
    store: Arc<SqliteProtectedRuntimeStore>,
    p: Arc<FakeProvider>,
) -> RuntimeBuilder {
    RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(p)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(store.clone())
        .checkpoint_store(store)
}

pub(super) fn runtime(store: Arc<SqliteProtectedRuntimeStore>, p: Arc<FakeProvider>) -> Runtime {
    runtime_builder(store, p).build().unwrap()
}

pub(super) fn open(label: &str, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(format!("{label}-open")),
        pin: JournalPinId::new(label),
        intent,
    }
}

pub(super) fn limits() -> JournalReadLimits {
    JournalReadLimits {
        objects: 100_000,
        encoded_bytes: 32 * 1024 * 1024,
        entries: 100_000,
    }
}

pub(super) async fn snapshot(journal: &dyn SessionJournal, id: &SessionId) -> SessionSnapshot {
    let lease = journal
        .open(id, open("snapshot", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    let value = JournalReader::new(journal, &lease, limits())
        .unwrap()
        .snapshot()
        .await
        .unwrap()
        .unwrap();
    journal.close(lease).await.unwrap();
    value
}

pub(super) async fn fixture(id: &SessionId) -> (FileDb, Arc<SqliteProtectedRuntimeStore>) {
    let f = FileDb::new().await.unwrap();
    f.ensure_session(id).await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    (f, store)
}

pub(super) async fn profile_lcm_runtime(
    store: Arc<SqliteProtectedRuntimeStore>,
    p: Arc<FakeProvider>,
    id: &SessionId,
    chat: &str,
) -> (Runtime, String) {
    use agent_runtime::harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver};
    let lcm = store
        .lcm_store_for_runtime_session(id.as_str(), "agent_chat", chat)
        .await
        .unwrap();
    let timeline = lcm.timeline_id().to_owned();
    let binding = lcm.runtime_binding(id.clone()).unwrap();
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
    (
        runtime_builder(store, p)
            .lcm(Arc::new(coordinator))
            .build()
            .unwrap(),
        timeline,
    )
}

pub(super) async fn drain_gc<S: JournalStorage>(
    journal: &JournalEngine<S>,
    mut request: JournalGcRequest,
) -> Result<JournalGcReport, agent_runtime::core::error::RuntimeError> {
    let mut total = journal.collect(request.clone()).await?;
    for _ in 0..10_000 {
        if !total.has_more {
            return Ok(total);
        }
        request.expected_root_epoch = journal.root_epoch()?;
        total.accumulate(journal.collect(request.clone()).await?);
    }
    panic!("collection continuation bound exhausted");
}
