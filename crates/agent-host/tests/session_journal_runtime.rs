#[path = "support/metered_journal.rs"]
mod metered;

use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::{CheckpointStore, TurnCheckpoint},
        clock::{Deadline, Timestamp},
        content::{Message, UserInput},
        ids::{SessionId, TurnId},
        journal::*,
        provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent},
        store::{SessionSnapshot, SessionStore, VersionedSessionState},
    },
    provider::fake::{FakeProvider, ScriptedStream},
    registry::RegistryRevision,
    runtime::{Runtime, RuntimeBuilder, StartSession},
};
use forge_agent_host::{
    SqliteProtectedRuntimeStore,
    session_journal::ForgeJournalStorage,
    session_journals::{ForgeJournalProjection, JOURNAL_MEMO_BYTES, SessionJournals},
    test_support::FileDb,
};
use std::sync::Arc;

fn provider() -> Arc<FakeProvider> {
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
fn runtime_builder(
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
fn runtime(store: Arc<SqliteProtectedRuntimeStore>, p: Arc<FakeProvider>) -> Runtime {
    runtime_builder(store, p).build().unwrap()
}
fn open(label: &str, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root: JournalRootSelection::Latest,
        expected_revision: None,
        operation: JournalOperationId::new(format!("{label}-open")),
        pin: JournalPinId::new(label),
        intent,
    }
}
fn limits() -> JournalReadLimits {
    JournalReadLimits {
        objects: 100_000,
        encoded_bytes: 32 * 1024 * 1024,
        entries: 100_000,
    }
}
async fn snapshot(journal: &dyn SessionJournal, id: &SessionId) -> SessionSnapshot {
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
async fn fixture(id: &SessionId) -> (FileDb, Arc<SqliteProtectedRuntimeStore>) {
    let f = FileDb::new().await.unwrap();
    f.ensure_session(id).await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    (f, store)
}

async fn profile_lcm_runtime(
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

#[tokio::test]
async fn selected_profile_owns_shared_pre_topic_lcm_after_profile_switch() {
    use agent_runtime::harness::{LCM_COMPONENT_ID, LcmRecoveryPolicy};
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("profile-switch-conversion");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
    let db = f.database();
    let older_snapshot = SessionStore::load(&*store, &older).await.unwrap().unwrap();
    let older_checkpoint = CheckpointStore::load_latest(&*store, &older)
        .await
        .unwrap()
        .unwrap();
    assert!(
        older_checkpoint
            .snapshot
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );
    let before: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    let shape: Vec<(String, i64)> = sqlx::query_as("SELECT profile_id,COUNT(*) FROM agent_session WHERE status='suspended' AND backend_kind='native' AND predecessor_session_id IS NULL AND replaced_by_session_id IS NULL GROUP BY profile_id ORDER BY profile_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(
        shape,
        vec![("gemini-profile".into(), 3), ("glm-profile".into(), 10)]
    );
    let owner = f.ensure_session(&id).await.unwrap();
    assert_eq!(
        db.dormant_native_chat_session_id("journal-agent", "journal-scope", "gemini-profile")
            .await
            .unwrap(),
        Some(owner.clone())
    );
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let retained: (String, Option<String>, i64) = sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(db.pool()).await.unwrap();
    assert_eq!(retained, ("suspended".into(), None, 1));
    let older_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE profile_id='glm-profile' AND status='suspended' AND replaced_by_session_id IS NULL AND predecessor_session_id IS NULL AND version=1")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(older_rows, 10);
    let replaced: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE profile_id='gemini-profile' AND status='replaced' AND replaced_by_session_id=? AND version=2")
        .bind(&owner).fetch_one(db.pool()).await.unwrap();
    assert_eq!(replaced, 2);
    let publications: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session_journal_session j JOIN agent_session s ON s.id=j.forge_session_id WHERE s.profile_id='glm-profile' AND (j.published=1 OR j.retired=1)")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(publications, 0);
    let receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
    )
    .bind(&old.timeline_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipts, 12);
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let timeline: (String,i64,i64,i64,i64) = sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_operation),(SELECT COUNT(*) FROM agent_lcm_timeline) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(timeline, (id.to_string(), 131, 114, 121, 1));
    drop(store);
    let store = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let p = provider();
    let chat = format!("chat:{id}");
    let (rt, b_timeline) = profile_lcm_runtime(store.clone(), p.clone(), &id, &chat).await;
    assert_eq!(b_timeline, old.timeline_id);
    let b = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(b.history(), old.snapshot.history.as_slice());
    assert!(p.requests().is_empty());
    drop(b);
    drop(rt);
    sqlx::query("UPDATE agent_identity SET selected_profile_id='glm-profile',version=version+1 WHERE id='journal-agent'")
        .execute(db.pool()).await.unwrap();
    let older_owner = f.ensure_session(&older).await.unwrap();
    assert_eq!(
        db.dormant_native_chat_session_id("journal-agent", "journal-scope", "glm-profile")
            .await
            .unwrap(),
        Some(older_owner.clone())
    );
    assert_eq!(
        store
            .convert_legacy_session(&older_owner, &older)
            .await
            .unwrap(),
        ConversionOutcome::Converted
    );
    let mut expected = older_snapshot;
    expected.extension_state.remove(LCM_COMPONENT_ID);
    assert_eq!(
        snapshot(&store.journals().ordinary(), &older).await,
        expected
    );
    let mut protected = older_checkpoint.snapshot;
    protected.extension_state.remove(LCM_COMPONENT_ID);
    assert_eq!(
        snapshot(&store.journals().protected(), &older).await,
        protected
    );
    assert!(protected.extension_state.contains_key("fixture.protected"));
    assert_eq!(
        store
            .convert_legacy_session(&older_owner, &older)
            .await
            .unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let a_provider = provider();
    let (a_rt, a_timeline) =
        profile_lcm_runtime(store.clone(), a_provider.clone(), &older, &chat).await;
    assert_ne!(a_timeline, old.timeline_id);
    let a = a_rt
        .start_session(StartSession::resume(older.clone()))
        .await
        .unwrap();
    assert_eq!(a.history(), protected.history.as_slice());
    assert!(a_provider.requests().is_empty());
    a.run(agent_runtime::core::content::UserInput::text(
        "Resume GLM with rebuilt LCM",
    ))
    .await
    .unwrap()
    .completed()
    .await;
    assert_eq!(a.history().last().unwrap().joined_text(), "reply");
    assert_eq!(a_provider.requests().len(), 1);
    a.shutdown().await.unwrap();
    let after: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(before, after);
    let shared: (String,i64,i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=t.id),(SELECT COUNT(*) FROM agent_lcm_operation WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(shared, (id.to_string(), 114, 121));
}

#[tokio::test]
async fn selected_profile_shared_lcm_lazy_resume_without_provider_calls() {
    use agent_runtime::harness::LcmRecoveryPolicy;
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("selected-profile-lazy");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
    sqlx::query(
        "UPDATE agent_session SET status='ready',version=version+1 WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .execute(f.database().pool())
    .await
    .unwrap();
    let _foreground = store.foreground_session(&id).await.unwrap();
    let p = provider();
    let (rt, timeline) =
        profile_lcm_runtime(store.clone(), p.clone(), &id, &format!("chat:{id}")).await;
    assert_eq!(timeline, old.timeline_id);
    let b = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(b.history(), old.snapshot.history.as_slice());
    assert_eq!(b.history().len(), 114);
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let a: (String,Option<String>,i64)=sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(a, ("suspended".into(), None, 1));
}

#[tokio::test]
async fn sibling_owned_pre_topic_lcm_converts_and_retries() {
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("sibling-owner-conversion");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    let owner = f.ensure_session(&id).await.unwrap();
    let selected = f
        .database()
        .dormant_native_chat_session_id("journal-agent", "journal-scope", "journal-profile")
        .await
        .unwrap();
    assert_eq!(selected.as_deref(), Some(owner.as_str()));
    let before: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let after: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(before, after);
    let bindings: Vec<(String, String)> =
        sqlx::query_as("SELECT runtime_session_id,timeline_id FROM agent_runtime_lcm_binding")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    assert_eq!(bindings, vec![(id.to_string(), old.timeline_id.clone())]);
    let rows: (String, i64, i64, i64, i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_timeline),revision,(SELECT COUNT(*) FROM agent_lcm_operation) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(rows, (id.to_string(), 114, 1, 131, 121));
    assert_eq!(
        f.ensure_session(&older).await.unwrap(),
        format!("journal-fixture:{older}")
    );
}

#[tokio::test]
async fn sibling_owned_pre_topic_lcm_lazy_resume_without_provider_calls() {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
    };
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("sibling-owner-lazy");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    // Foreground activation changes B to ready before preparing its LCM store.
    sqlx::query(
        "UPDATE agent_session SET status='ready',version=version+1 WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .execute(f.database().pool())
    .await
    .unwrap();
    let _foreground = store.foreground_session(&id).await.unwrap();
    let lcm = store
        .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &format!("chat:{id}"))
        .await
        .unwrap();
    let binding = lcm.runtime_binding(id.clone()).unwrap();
    assert_eq!(binding.timeline.as_str(), old.timeline_id);
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
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let session = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(session.id(), &id);
    assert_eq!(session.history(), old.snapshot.history.as_slice());
    assert_eq!(session.history().len(), 114);
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let superseded: (String, Option<String>, i64) = sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(
        superseded,
        ("replaced".into(), Some(format!("journal-fixture:{id}")), 2)
    );
    assert!(SessionStore::load(&*store, &older).await.unwrap().is_some());
}

#[tokio::test]
async fn pre_topic_checkpoint_lcm_converts_without_changing_legacy() {
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("pre-topic-conversion");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let before: (String, String) = sqlx::query_as(
        "SELECT quote(snapshot_ciphertext), quote(checkpoint_ciphertext) FROM protected_agent_session_state",
    ).fetch_one(f.database().pool()).await.unwrap();
    let owner = f.ensure_session(&id).await.unwrap();
    let result = store.convert_legacy_session(&owner, &id).await;
    assert_eq!(result.unwrap(), ConversionOutcome::Converted);
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    let after: (String, String) = sqlx::query_as(
        "SELECT quote(snapshot_ciphertext), quote(checkpoint_ciphertext) FROM protected_agent_session_state",
    ).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(before, after);
    let bound: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(bound, old.timeline_id);
}

#[tokio::test]
async fn pre_topic_checkpoint_lcm_lazy_resume_preserves_session() {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
    };
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("pre-topic-lazy");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let lcm = store
        .lcm_store_for_runtime_session(id.as_str(), "account", "journal-owner")
        .await
        .unwrap();
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
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let session = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(session.id(), &id);
    assert_eq!(session.history(), old.snapshot.history.as_slice());
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
}

#[tokio::test]
async fn pre_topic_failed_binding_and_unpublished_reservations_retry_successfully() {
    use agent_runtime::{
        core::error::ErrorKind,
        harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
        runtime::CheckpointRecoveryPolicy,
    };
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("pre-topic-retry");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let wrong = old.bind_empty_replacement(&f).await;
    let binding = wrong.runtime_binding(id.clone()).unwrap();
    assert_ne!(binding.timeline.as_str(), old.timeline_id);
    assert_eq!(binding.authorization_revision.as_str(), old.checkpoint.snapshot.extension_state[agent_runtime::harness::LCM_COMPONENT_ID].value["binding_revision"].as_str().unwrap());
    // Reproduce the old background preflight and real runtime failure, leaving
    // both encrypted reservations at epoch 2, without publishing either root.
    for engine in [store.journals().protected(), store.journals().ordinary()] {
        let lease = engine
            .open(&id, open("preflight", JournalOpenIntent::ExclusiveWriter))
            .await
            .unwrap();
        engine.close(lease).await.unwrap();
    }
    let coordinator = LcmCoordinator::new(
        Arc::new(wrong),
        Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 28_000,
            ..Default::default()
        },
    )
    .unwrap();
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let error = rt
        .start_session(
            StartSession::resume(id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.message,
        "LCM state failed identity, binding, or frontier validation"
    );
    assert!(p.requests().is_empty());
    drop(rt);
    let reservations: Vec<(String, Vec<u8>, bool)> = sqlx::query_as(
        "SELECT role,writer_epoch,published FROM agent_session_journal_session WHERE runtime_session_id=? ORDER BY role",
    ).bind(id.as_str()).fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(reservations.len(), 2);
    for (_, epoch, published) in reservations {
        assert_eq!(u64::from_be_bytes(epoch.try_into().unwrap()), 2);
        assert!(!published);
    }
    let owner = f.ensure_session(&id).await.unwrap();
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    let binding: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(binding, old.timeline_id);
    let original: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=?1),
                (SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id=?1),
                (SELECT COUNT(*) FROM agent_lcm_operation WHERE timeline_id=?1)",
    )
    .bind(&old.timeline_id)
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(original, (120, 1, 2));
}

#[tokio::test]
async fn native_delta_and_projection() {
    let id = SessionId::new("projection");
    let (f, store) = fixture(&id).await;
    assert_eq!(JOURNAL_MEMO_BYTES, 128 * 1024 * 1024);
    let ordinary = SessionStore::journal(&*store).unwrap();
    let protected = CheckpointStore::journal(&*store).unwrap();
    assert_ne!(ordinary.storage_id(), protected.storage_id());
    let mut base: SessionSnapshot = serde_json::from_value(serde_json::json!({
        "id": id, "history": [Message::user("restore-critical conversation")], "updated": 1
    }))
    .unwrap();
    for namespace in ["forge.diagnostics.request", "host.restore"] {
        base.extension_state.insert(
            namespace.into(),
            VersionedSessionState::new(
                RegistryRevision::new("fixture-1"),
                serde_json::json!({"text":"sensitive", "fixed_overhead_tokens":71}),
            ),
        );
    }
    let mut policy_only = base.clone();
    for namespace in [
        agent_runtime::harness::LCM_COMPONENT_ID,
        RUNTIME_PREVIOUS_CACHE_NAMESPACE,
    ] {
        policy_only.extension_state.insert(
            namespace.into(),
            VersionedSessionState::new(
                RegistryRevision::new("policy-only"),
                serde_json::json!({"content":"restore-critical"}),
            ),
        );
    }
    let projected = ForgeJournalProjection.project(policy_only.clone()).unwrap();
    for namespace in [
        "host.restore",
        agent_runtime::harness::LCM_COMPONENT_ID,
        RUNTIME_PREVIOUS_CACHE_NAMESPACE,
    ] {
        assert_eq!(
            projected.extension_state[namespace],
            policy_only.extension_state[namespace]
        );
    }
    assert!(
        !projected
            .extension_state
            .contains_key("forge.diagnostics.request")
    );
    assert_eq!(projected.history, policy_only.history);
    let mut exact = base.clone();
    exact.history.push(Message::user("accepted"));
    let cp = TurnCheckpoint::session_boundary(exact.clone(), 0).unwrap();
    store
        .seed_legacy_bytes(
            &id,
            Some(&serde_json::to_vec(&exact).unwrap()),
            Some(&serde_json::to_vec(&cp).unwrap()),
            true,
        )
        .await
        .unwrap();
    let p = provider();
    let rt = runtime(store.clone(), p.clone());
    let session = rt
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert!(p.requests().is_empty());
    assert_eq!(snapshot(&*protected, &id).await, cp.snapshot);
    assert_eq!(
        snapshot(&*ordinary, &id).await,
        ForgeJournalProjection.project(exact).unwrap()
    );
    drop(session);
    drop(rt);
    // The policy also preserves the only recovery source before any checkpoint.
    let ordinary_only = SessionId::new("ordinary-only-policy");
    f.ensure_session(&ordinary_only).await.unwrap();
    base.id = ordinary_only.clone();
    store
        .seed_legacy_bytes(
            &ordinary_only,
            Some(&serde_json::to_vec(&base).unwrap()),
            None,
            false,
        )
        .await
        .unwrap();
    let rt = runtime(store.clone(), p.clone());
    let session = rt
        .start_session(StartSession::resume(ordinary_only.clone()))
        .await
        .unwrap();
    assert!(p.requests().is_empty());
    assert_eq!(session.history(), base.history);
    let projected = snapshot(&*ordinary, &ordinary_only).await;
    assert_eq!(projected.history, base.history);
    assert_eq!(
        projected.extension_state["host.restore"],
        base.extension_state["host.restore"]
    );
    assert!(
        CheckpointStore::load_latest(&*store, &ordinary_only)
            .await
            .unwrap()
            .is_none()
    );
    drop(session);
    drop(rt);
    // Fresh sessions publish natively, then replace Runtime between turns.
    let fresh = SessionId::new("native-delta");
    f.ensure_session(&fresh).await.unwrap();
    let rt = runtime(store.clone(), p.clone());
    let session = rt
        .start_session(StartSession::create(fresh.clone(), vec![]))
        .await
        .unwrap();
    session
        .run(UserInput::text("first"))
        .await
        .unwrap()
        .completed()
        .await;
    session.persist().await.unwrap();
    let domain = JournalDomain {
        storage_id: ordinary.storage_id(),
        role: JournalRole::Ordinary,
    };
    let storage = store.journals().storage();
    let first = storage.members(&domain, &fresh).await.unwrap();
    let bodies = storage.objects(&domain, &fresh, &first).await.unwrap();
    let message_ids: Vec<_> = first
        .iter()
        .zip(bodies)
        .filter_map(|(id, bytes)| {
            let o = JournalObject {
                id: id.clone(),
                bytes: bytes.unwrap().to_vec(),
            };
            (o.kind().unwrap() == JournalObjectKind::Message).then_some(id.clone())
        })
        .collect();
    assert!(!message_ids.is_empty());
    drop(session);
    drop(rt);
    let rt = runtime(store.clone(), p.clone());
    let session = rt
        .start_session(StartSession::resume(fresh.clone()))
        .await
        .unwrap();
    session
        .run(UserInput::text("second"))
        .await
        .unwrap()
        .completed()
        .await;
    session.persist().await.unwrap();
    let second = storage.members(&domain, &fresh).await.unwrap();
    assert!(message_ids.iter().all(|id| second.contains(id)));
    // Storage admission guarantees one slot per id; physical copies are independent by domain.
    assert_eq!(snapshot(&*ordinary, &fresh).await.history.len(), 4);
    let legacy_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM protected_agent_session_state WHERE session_id=?")
            .bind(f.ensure_session(&fresh).await.unwrap())
            .fetch_one(f.database().pool())
            .await
            .unwrap();
    assert_eq!(legacy_count, 0);
    drop(session);
    drop(rt);
    let ephemeral = SessionId::new("ephemeral");
    let mut start = StartSession::ephemeral(vec![]);
    start.session_id = Some(ephemeral.clone());
    let rt = runtime(store, p);
    let session = rt.start_session(start).await.unwrap();
    session
        .run(UserInput::text("ephemeral"))
        .await
        .unwrap()
        .completed()
        .await;
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_session_journal_session WHERE runtime_session_id=?",
    )
    .bind(ephemeral.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn writer_change_memo_and_restart() {
    for budget in [0, 1, JOURNAL_MEMO_BYTES] {
        let id = SessionId::new(format!("memo-{budget}"));
        let (f, store) = fixture(&id).await;
        let rt = runtime(store.clone(), provider());
        let session = rt
            .start_session(StartSession::create(
                id.clone(),
                vec![Message::user("history".repeat(100))],
            ))
            .await
            .unwrap();
        session
            .run(UserInput::text("checkpoint"))
            .await
            .unwrap()
            .completed()
            .await;
        session.persist().await.unwrap();
        drop(session);
        drop(rt);
        drop(store);
        let raw =
            Arc::new(ForgeJournalStorage::new(f.reopen().await.unwrap(), [7; 32], 1).unwrap());
        let metered = Arc::new(metered::Metered::new(raw));
        let owner = SessionJournals::with_test_memo_budget(metered.clone(), budget);
        for protected in [false, true] {
            let engine = if protected {
                owner.protected()
            } else {
                owner.ordinary()
            };
            let lease = engine
                .open(&id, open("cold", JournalOpenIntent::ExclusiveWriter))
                .await
                .unwrap();
            assert!(!metered.take().is_empty(), "restart validates cold");
            engine.close(lease).await.unwrap();
            let cloned_owner = owner.clone();
            let clone = if protected {
                cloned_owner.protected()
            } else {
                cloned_owner.ordinary()
            };
            let lease = clone
                .open(&id, open("next-turn", JournalOpenIntent::ExclusiveWriter))
                .await
                .unwrap();
            let reads = metered.take();
            if budget == JOURNAL_MEMO_BYTES {
                assert!(reads.is_empty(), "warm writer reloads no bodies");
            } else {
                assert!(!reads.is_empty(), "zero/oversize budget validates cold");
            }
            let restored = JournalReader::new(&clone, &lease, limits())
                .unwrap()
                .snapshot()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(restored.history.len(), 3);
            clone.close(lease).await.unwrap();
            metered.take();
        }
    }
}

#[tokio::test]
async fn runtime_and_collector_share_engine_leases() {
    let id = SessionId::new("leases");
    let (f, store) = fixture(&id).await;
    let rt = runtime(store.clone(), provider());
    let session = rt
        .start_session(StartSession::create(
            id.clone(),
            vec![Message::user("held")],
        ))
        .await
        .unwrap();
    session.persist().await.unwrap();
    let collector = store.journals().clone().ordinary();
    let historical = collector
        .open(&id, open("historical", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    let historical_snapshot = JournalReader::new(&collector, &historical, limits())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    session
        .run(UserInput::text("advance with historical reader live"))
        .await
        .unwrap()
        .completed()
        .await;
    session.persist().await.unwrap();
    let mark = collector.root_epoch().unwrap();
    let competing = collector
        .open(&id, open("competing", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap_err();
    assert_eq!(
        competing.kind,
        agent_runtime::core::error::ErrorKind::Conflict
    );
    drain_gc(
        &collector,
        JournalGcRequest {
            storage_id: collector.storage_id(),
            expected_root_epoch: mark,
            max_objects: 20_000,
            work_budget: Default::default(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        JournalReader::new(&collector, &historical, limits())
            .unwrap()
            .snapshot()
            .await
            .unwrap(),
        historical_snapshot,
        "collector must retain a reader held by the runtime's shared registry"
    );
    collector.close(historical).await.unwrap();
    let lease = collector
        .open(&id, open("reader", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    assert!(collector.root_epoch().unwrap() > mark);
    let stale = drain_gc(
        &collector,
        JournalGcRequest {
            storage_id: collector.storage_id(),
            expected_root_epoch: mark,
            max_objects: 20_000,
            work_budget: Default::default(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(stale.kind, agent_runtime::core::error::ErrorKind::Conflict);
    collector.close(lease).await.unwrap();
    drop(session);
    drop(rt);
    let lease = collector
        .open(&id, open("after-drop", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    collector.close(lease).await.unwrap();
    drop(f);
}

#[tokio::test]
async fn native_probe_and_extension() {
    use agent_runtime::harness::{
        LCM_COMPONENT_ID, LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver,
    };
    let id = SessionId::new("probe");
    let (f, store) = fixture(&id).await;
    assert!(!store.persistence_flags(&id).await.unwrap().saved);
    assert!(
        !store
            .persistence_flags(&SessionId::new("absent"))
            .await
            .unwrap()
            .saved
    );
    let ordinary = store.journals().ordinary();
    let reservation = ordinary
        .open(&id, open("reservation", JournalOpenIntent::ExclusiveWriter))
        .await
        .unwrap();
    assert!(!store.persistence_flags(&id).await.unwrap().saved);
    ordinary.close(reservation).await.unwrap();
    let mut legacy: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":id,"history":[],"updated":1})).unwrap();
    legacy.extension_state.insert(
        LCM_COMPONENT_ID.into(),
        VersionedSessionState::new(
            RegistryRevision::new("probe-only"),
            serde_json::json!({"fixed_overhead_tokens":19}),
        ),
    );
    store
        .seed_legacy_bytes(&id, Some(&serde_json::to_vec(&legacy).unwrap()), None, true)
        .await
        .unwrap();
    let flags = store.persistence_flags(&id).await.unwrap();
    assert!(flags.saved && flags.legacy_eligible);
    assert!(store.lcm_overhead_measured(&id, flags).await.unwrap());
    // A genuine LCM turn publishes its opaque state and a large history.
    let native = SessionId::new("native-probe");
    f.ensure_session(&native).await.unwrap();
    let lcm_store = store
        .lcm_store_for_runtime_session(native.as_str(), "account", "journal-owner")
        .await
        .unwrap();
    let binding = lcm_store.runtime_binding(native.clone()).unwrap();
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
    let rt = runtime_builder(store.clone(), provider())
        .system_prompt("These are fixed host instructions for measuring request overhead.")
        .working_set_policy(agent_runtime::runtime::WorkingSetPolicy {
            target_tokens: 24_000,
            hard_tokens: 28_000,
        })
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let history: Vec<_> = (0..1000)
        .map(|i| Message::user(format!("message {i}")))
        .collect();
    let session = rt
        .start_session(StartSession::create(native.clone(), history))
        .await
        .unwrap();
    session
        .run(UserInput::text("plan with measured overhead"))
        .await
        .unwrap()
        .completed()
        .await;
    session.persist().await.unwrap();
    let flags = store.persistence_flags(&native).await.unwrap();
    assert!(flags.saved && flags.checkpoint_present && !flags.legacy_eligible);
    assert!(store.lcm_overhead_measured(&native, flags).await.unwrap());
    drop(session);
    drop(rt);
    // Reader operations stay bounded both warm and after cold lease acquisition.
    // Cold open intentionally validates the closure (task 13), measured separately.
    for cold in [false, true] {
        let journals = if cold {
            let raw =
                Arc::new(ForgeJournalStorage::new(f.reopen().await.unwrap(), [7; 32], 1).unwrap());
            SessionJournals::with_test_memo_budget(raw, 0)
        } else {
            store.journals()
        };
        for engine in [journals.ordinary(), journals.protected()] {
            let lease = engine
                .open(&native, open("extension", JournalOpenIntent::ReadOnly))
                .await
                .unwrap();
            let metered = metered::ReadMeter {
                inner: &engine,
                kinds: Default::default(),
            };
            let value = JournalReader::new(&metered, &lease, limits())
                .unwrap()
                .extension(LCM_COMPONENT_ID)
                .await
                .unwrap()
                .unwrap();
            assert!(value.value["fixed_overhead_tokens"].as_u64().unwrap() > 0);
            let kinds = metered.kinds.lock().unwrap().clone();
            assert!(kinds.len() <= 8, "one namespace stays bounded: {kinds:?}");
            assert!(kinds.iter().all(|kind| matches!(
                kind,
                JournalObjectKind::Head
                    | JournalObjectKind::Batch
                    | JournalObjectKind::Map
                    | JournalObjectKind::Extension
            )));
            engine.close(lease).await.unwrap();
        }
    }
    // The namespace-agnostic selector prefers protected when ordinary omits a value.
    let fallback = SessionId::new("protected-extension-fallback");
    f.ensure_session(&fallback).await.unwrap();
    let mut exact: SessionSnapshot = serde_json::from_value(
        serde_json::json!({"id":fallback,"history":[Message::user("accepted")],"updated":1}),
    )
    .unwrap();
    exact.extension_state.insert(
        "forge.diagnostics.request".into(),
        VersionedSessionState::new(
            RegistryRevision::new("diagnostic"),
            serde_json::json!({"sensitive":"exact"}),
        ),
    );
    let cp = TurnCheckpoint::session_boundary(exact.clone(), 0).unwrap();
    store
        .seed_legacy_bytes(
            &fallback,
            Some(&serde_json::to_vec(&exact).unwrap()),
            Some(&serde_json::to_vec(&cp).unwrap()),
            true,
        )
        .await
        .unwrap();
    let rt = runtime(store.clone(), provider());
    let session = rt
        .start_session(StartSession::resume(fallback.clone()))
        .await
        .unwrap();
    let flags = store.persistence_flags(&fallback).await.unwrap();
    assert!(
        !snapshot(&store.journals().ordinary(), &fallback)
            .await
            .extension_state
            .contains_key("forge.diagnostics.request")
    );
    assert_eq!(
        store
            .journals()
            .extension(&fallback, flags, "forge.diagnostics.request")
            .await
            .unwrap(),
        Some(exact.extension_state["forge.diagnostics.request"].clone())
    );
    session.shutdown().await.unwrap();
    drop(session);
    drop(rt);
    // Retirement suppresses legacy fallback in either native domain.
    let protected = store.journals().protected();
    let lease = protected
        .open(&fallback, open("retire-probe", JournalOpenIntent::ReadOnly))
        .await
        .unwrap();
    let revision = lease.head().unwrap().revision;
    protected.close(lease).await.unwrap();
    protected
        .authorize_retirement(&fallback, revision, Timestamp(99))
        .await
        .unwrap();
    protected.retire(&fallback, revision).await.unwrap();
    let error = store.persistence_flags(&fallback).await.unwrap_err();
    assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::Conflict);
    // Cold scalar reads ignore even corrupt packs and surviving legacy payloads.
    sqlx::query("UPDATE agent_session_journal_pack SET body_payload=zeroblob(length(body_payload)) WHERE runtime_session_id=?")
        .bind(native.as_str()).execute(f.database().pool()).await.unwrap();
    let restarted = SqliteProtectedRuntimeStore::new(f.reopen().await.unwrap(), [7; 32], 1);
    assert!(restarted.persistence_flags(&native).await.unwrap().saved);
    assert!(
        restarted
            .lcm_overhead_measured(&native, flags)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_writes_refuse_and_baseline_keeps_two_blobs() {
    use sqlx::Row;
    let id = SessionId::new("legacy-baseline");
    let (f, store) = fixture(&id).await;
    let mut snapshot: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":id,"history":[],"updated":1})).unwrap();
    snapshot.history.push(Message::user("accepted"));
    let cp = TurnCheckpoint::accepted(
        TurnId::new("baseline"),
        UserInput::text("accepted"),
        snapshot.clone(),
        0,
        Deadline::never(),
        1,
        0,
        Timestamp(1),
    )
    .unwrap();
    for result in [
        SessionStore::save(&*store, &snapshot).await,
        CheckpointStore::save(&*store, &cp).await,
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.kind, agent_runtime::core::error::ErrorKind::Conflict);
        assert_eq!(error.message, "legacy session store is read-only");
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM protected_agent_session_state")
        .fetch_one(f.database().pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Exact former ordering: checkpoint clears the standalone payload; unchanged
    // snapshots skip by digest; a later canonical boundary/cache change writes
    // the independently sealed ordinary copy, producing the real idle L2 pair.
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    store.save_legacy_checkpoint_baseline(&cp).await.unwrap();
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let one: bool = sqlx::query_scalar("SELECT snapshot_ciphertext IS NULL AND checkpoint_ciphertext IS NOT NULL FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert!(one);
    snapshot.extension_state.insert(
        "host.restore".into(),
        VersionedSessionState::new(
            RegistryRevision::new("idle"),
            serde_json::json!({"cache":"idle state"}),
        ),
    );
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    let row = sqlx::query("SELECT snapshot_ciphertext,checkpoint_ciphertext,quote(snapshot_ciphertext) AS snapshot,quote(checkpoint_ciphertext) AS checkpoint,state_revision FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    let ordinary: Vec<u8> = row.get("snapshot_ciphertext");
    let protected: Vec<u8> = row.get("checkpoint_ciphertext");
    assert!(!ordinary.is_empty() && !protected.is_empty());
    assert_ne!(ordinary, protected);
    assert_eq!(
        SessionStore::load(&*store, &id).await.unwrap(),
        Some(snapshot.clone())
    );
    assert_eq!(
        CheckpointStore::load_latest(&*store, &id).await.unwrap(),
        Some(cp.clone())
    );
    let before: (String, String, i64) = (
        row.get("snapshot"),
        row.get("checkpoint"),
        row.get("state_revision"),
    );
    sqlx::query("CREATE TRIGGER refuse_legacy_update BEFORE UPDATE ON protected_agent_session_state BEGIN SELECT RAISE(ABORT,'legacy write'); END")
        .execute(f.database().pool()).await.unwrap();
    snapshot.updated = Timestamp(999);
    store
        .save_legacy_snapshot_baseline(&snapshot)
        .await
        .unwrap();
    assert!(SessionStore::save(&*store, &snapshot).await.is_err());
    assert!(CheckpointStore::save(&*store, &cp).await.is_err());
    let after: (String,String,i64) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext),state_revision FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(
        before, after,
        "refusals and digest skips leave both sealed blobs byte-exact"
    );
}

#[path = "support/import_matrix.rs"]
mod import_matrix;

#[tokio::test]
async fn legacy_import_state_matrix() {
    use import_matrix::*;
    use std::sync::atomic::Ordering;
    let cases = cases().await;
    assert_eq!(cases.len(), 9);
    for case in cases {
        let (f, store) = fixture(&case.snapshot.id).await;
        seed(&store, &case).await;
        audit_publications(&f).await;
        let before = legacy_row(&f).await;
        let stores = Stores::new(store.clone());
        let io = Arc::new(IoCounts::default());
        let (rt, s, p) = deferred(stores.clone(), &case, io.clone()).await;
        assert_import(&store, &case).await;
        assert!(p.requests().is_empty());
        assert_eq!(stores.loads.load(Ordering::SeqCst), 2);
        let order: Vec<String> =
            sqlx::query_scalar("SELECT role FROM import_publication_order ORDER BY n")
                .fetch_all(f.database().pool())
                .await
                .unwrap();
        assert_eq!(
            order,
            if case.checkpoint.is_some() {
                vec!["protected", "ordinary"]
            } else {
                vec!["ordinary"]
            },
            "{} exact-first",
            case.label
        );
        drop(s);
        drop(rt);
        let loads = stores.loads.load(Ordering::SeqCst);
        let (rt, s, p) = deferred(stores.clone(), &case, io.clone()).await;
        assert_import(&store, &case).await;
        assert_eq!(
            stores.loads.load(Ordering::SeqCst),
            loads,
            "{} R5",
            case.label
        );
        assert_eq!(stores.saves.load(Ordering::SeqCst), 0);
        assert!(p.requests().is_empty());
        assert_eq!(io.tools.load(Ordering::SeqCst), 0);
        assert_eq!(io.interactions.load(Ordering::SeqCst), 0);
        assert_eq!(legacy_row(&f).await, before);
        println!(
            "IMPORT {} exact={} ordinary=projected second_legacy_loads=0 provider=0 tool=0 interaction=0 legacy=byte-unchanged",
            case.label,
            case.checkpoint.is_some()
        );
        drop(s);
        drop(rt);
    }
}

#[path = "support/crash_matrix.rs"]
mod crash_matrix;

#[tokio::test]
async fn import_and_commit_crash_cuts() {
    let imports = crash_matrix::import_cuts().await;
    crash_matrix::stale_legacy_is_not_overlaid().await;
    let turns = crash_matrix::turn_cuts().await;
    assert_eq!(imports, 119);
    println!(
        "CRASH MATRIX import={imports} turn={turns} cuts={}",
        JournalCommitFailurePoint::ALL.len()
    );
}

#[test]
fn async_controls_do_not_stall_worker() {
    for multi_thread in [false, true] {
        let mut builder = if multi_thread {
            tokio::runtime::Builder::new_multi_thread()
        } else {
            tokio::runtime::Builder::new_current_thread()
        };
        builder.enable_all();
        if multi_thread {
            builder.worker_threads(2);
        }
        builder.build().unwrap().block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10),async {
                let id=SessionId::new("suspended-sql");
                let (f,store)=fixture(&id).await;
                let rt=runtime(store.clone(),provider());
                let s=rt.start_session(StartSession::create(id.clone(),vec![])).await.unwrap();
                s.run(UserInput::text("publish")).await.unwrap();drop(s);drop(rt);drop(store);
                let raw=Arc::new(ForgeJournalStorage::new(f.reopen().await.unwrap(),[7;32],1).unwrap());
                for protected in [true,false] {
                    let meter=Arc::new(metered::Metered::new(raw.clone()));
                    let owner=SessionJournals::with_test_memo_budget(meter.clone(),JOURNAL_MEMO_BYTES);
                    let engine=if protected {owner.protected()}else{owner.ordinary()};
                    let retained=engine.open(&id,open("retained",JournalOpenIntent::ReadOnly)).await.unwrap();
                    let competing=engine.open(&id,open("competing",JournalOpenIntent::ReadOnly)).await.unwrap();
                    let closing=engine.open(&id,open("closing",JournalOpenIntent::ReadOnly)).await.unwrap();
                    // Hold a genuine independent SQLite write transaction. The
                    // control below enters Forge apply and suspends in sqlx.
                    let tx=db::begin_immediate(f.database().pool()).await.unwrap();
                    let first_engine=engine.clone();
                    let first=tokio::spawn(async move {first_engine.retain_root(&retained).await.unwrap();first_engine.close(retained).await.unwrap();});
                    meter.apply_entered.notified().await;
                    let other_engine=engine.clone();
                    let control=tokio::spawn(async move {other_engine.retain_root(&competing).await.unwrap();other_engine.close(competing).await.unwrap();});
                    let opener=engine.clone();let open_id=id.clone();
                    let opening=tokio::spawn(async move{opener.open(&open_id,open("racing-writer",JournalOpenIntent::ExclusiveWriter)).await});
                    // A local close cannot wait on the durable engine gate.
                    tokio::time::timeout(std::time::Duration::from_millis(250),engine.close(closing)).await.unwrap().unwrap();
                    // This timer is the worker-liveness witness; no sync polling.
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    assert!(!first.is_finished());assert!(!control.is_finished());assert!(!opening.is_finished());
                    tx.rollback().await.unwrap();first.await.unwrap();control.await.unwrap();
                    let writer=opening.await.unwrap().unwrap();engine.close(writer).await.unwrap();
                    println!("ASYNC multi_thread={multi_thread} domain={:?}: suspended SQL + controls/open/close + timer OK",engine.storage_id());
                }
            }).await.unwrap();
        });
    }
}

#[path = "support/credential_exclusion.rs"]
mod credential_exclusion;

#[tokio::test]
async fn journal_credential_exclusion() {
    credential_exclusion::run().await;
}

/// One exclusive writer per session across both journal domains: competing
/// turn, fork and abort-fork operations from other runtimes over the same
/// store conflict before any provider I/O; ephemeral inquiries over the same
/// session id coexist without taking or releasing that slot; dropping the
/// holder releases the slot, so the next writer opens without provider I/O.
#[tokio::test]
async fn writer_fence_and_drop() {
    use agent_runtime::core::error::ErrorKind;
    use agent_runtime::runtime::{ForkLcm, ForkSeed, ForkSession};

    let id = SessionId::new("writer-fence");
    let child = SessionId::new("writer-child");
    let (f, store) = fixture(&id).await;
    f.ensure_session(&child).await.unwrap();

    // A real published turn, so every competitor resumes native state.
    let seed_runtime = runtime(store.clone(), provider());
    let seed = seed_runtime
        .start_session(StartSession::create(id.clone(), vec![]))
        .await
        .unwrap();
    seed.run(UserInput::text("seed")).await.unwrap();
    drop(seed);
    drop(seed_runtime);

    // No scripted replies: any provider request by a contender is a failure.
    let p = import_matrix::fake(vec![], import_matrix::Profile::Plain);
    let a = runtime(store.clone(), p.clone());
    let b = runtime(store.clone(), p.clone());
    let (first, second) = tokio::join!(
        a.start_session(StartSession::resume(id.clone())),
        b.start_session(StartSession::resume(id.clone())),
    );
    assert_ne!(
        first.is_ok(),
        second.is_ok(),
        "exactly one of two concurrent turn writers opens"
    );
    let held = first.or(second).unwrap();

    let turn_runtime = runtime(store.clone(), p.clone());
    let fork_runtime = runtime(store.clone(), p.clone());
    let abort_runtime = runtime(store.clone(), p.clone());
    let (turn, fork, abort) = tokio::join!(
        turn_runtime.start_session(StartSession::resume(id.clone())),
        fork_runtime.fork_session(ForkSession {
            from: id.clone(),
            new_id: child.clone(),
            seed: ForkSeed::Empty,
            lcm: ForkLcm::NewTimeline,
        }),
        abort_runtime.abort_fork(&id),
    );
    for (operation, error) in [
        ("turn", turn.err()),
        ("fork", fork.err()),
        ("abort", abort.err()),
    ] {
        let error = error.unwrap_or_else(|| panic!("{operation} must lose to the held writer"));
        assert_eq!(error.kind, ErrorKind::Conflict, "{operation}: {error:?}");
    }
    assert!(
        p.requests().is_empty(),
        "every losing writer fails before provider I/O"
    );

    // Inquiries reuse authority metadata but are ephemeral: two of them run
    // concurrently beside the held writer and neither steals nor frees it.
    let inquiry_provider = provider();
    let inquiry_a = runtime(store.clone(), inquiry_provider.clone());
    let inquiry_b = runtime(store.clone(), inquiry_provider.clone());
    let mut start = StartSession::ephemeral(vec![]);
    start.session_id = Some(id.clone());
    let (ia, ib) = tokio::join!(
        inquiry_a.start_session(start.clone()),
        inquiry_b.start_session(start),
    );
    let (ia, ib) = (ia.unwrap(), ib.unwrap());
    let (ra, rb) = tokio::join!(
        ia.run(UserInput::text("inquiry-a")),
        ib.run(UserInput::text("inquiry-b")),
    );
    ra.unwrap();
    rb.unwrap();
    assert_eq!(inquiry_provider.requests().len(), 2);
    drop((ia, ib, inquiry_a, inquiry_b));
    assert_eq!(
        turn_runtime
            .start_session(StartSession::resume(id.clone()))
            .await
            .err()
            .map(|error| error.kind),
        Some(ErrorKind::Conflict),
        "finished inquiries leave the held writer in place"
    );

    // Dropping the holder releases both domain slots locally; each next
    // writer opens without reaching the provider.
    drop(held);
    drop((a, b));
    let restored = turn_runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    drop(restored);
    abort_runtime.abort_fork(&id).await.unwrap();
    let restored = turn_runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    drop(restored);
    let forked = fork_runtime
        .fork_session(ForkSession {
            from: id.clone(),
            new_id: child.clone(),
            seed: ForkSeed::Empty,
            lcm: ForkLcm::NewTimeline,
        })
        .await
        .unwrap();
    drop(forked);
    // A completed fork supersedes its parent; the successor released its
    // own slot on drop and opens again.
    let restored = turn_runtime
        .start_session(StartSession::resume(child))
        .await
        .unwrap();
    drop(restored);
    assert!(
        p.requests().is_empty(),
        "no writer hand-over needed provider I/O"
    );
}

/// Clear-index discovery must hand every id to the shared engine, including a
/// corrupt middle record and local dirty hints. Never mutate the bad scope.
#[tokio::test]
async fn dirty_corruption_does_not_block_shared_collector() {
    use sqlx::Row;
    async fn scope_image(
        f: &FileDb,
        domain: &JournalDomain,
        id: &SessionId,
    ) -> Vec<Vec<Vec<String>>> {
        let mut image = Vec::new();
        for suffix in [
            "session",
            "pin",
            "catalog",
            "pack",
            "membership",
            "operation_pack",
        ] {
            let table = format!("agent_session_journal_{suffix}");
            let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
                .fetch_all(f.database().pool())
                .await
                .unwrap();
            let fields: Vec<_> = columns
                .iter()
                .map(|c| format!("quote(\"{}\")", c.get::<String, _>("name")))
                .collect();
            // Packs are session-owned (no domain column); the session's image
            // then also covers packs shared with the other domain.
            let filter = if suffix == "pack" {
                "?1=?1 AND runtime_session_id=?2"
            } else {
                "storage_id=?1 AND runtime_session_id=?2"
            };
            let rows = sqlx::query(&format!(
                "SELECT {} FROM {table} WHERE {filter} ORDER BY 1,2,3",
                fields.join(",")
            ))
            .bind(domain.storage_id.as_str())
            .bind(id.as_str())
            .fetch_all(f.database().pool())
            .await
            .unwrap();
            image.push(
                rows.iter()
                    .map(|r| (0..fields.len()).map(|i| r.get(i)).collect())
                    .collect(),
            );
        }
        image
    }
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    let ids = ["d2-a", "d2-b", "d2-c"].map(SessionId::new);
    for id in &ids {
        f.ensure_session(id).await.unwrap();
        let rt = runtime(store.clone(), provider());
        let session = rt
            .start_session(StartSession::create(
                id.clone(),
                vec![Message::user("initial history".repeat(30))],
            ))
            .await
            .unwrap();
        session.persist().await.unwrap();
        for input in ["first turn", "second turn"] {
            session
                .run(UserInput::text(input))
                .await
                .unwrap()
                .completed()
                .await;
            session.persist().await.unwrap();
        }
        drop(session);
        drop(rt);
    }
    let journal = store.journals().ordinary();
    let storage = store.journals().storage();
    let domain = JournalDomain {
        storage_id: journal.storage_id(),
        role: JournalRole::Ordinary,
    };
    // Packs are session-owned and shared by both domains of a session.
    let before: Vec<(String,i64)> = sqlx::query_as("SELECT runtime_session_id,sum(retained_canonical_bytes) FROM agent_session_journal_pack GROUP BY runtime_session_id ORDER BY runtime_session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    sqlx::query("UPDATE agent_session_journal_session SET payload=zeroblob(length(payload)) WHERE storage_id=? AND runtime_session_id=?")
        .bind(domain.storage_id.as_str()).bind(ids[1].as_str()).execute(f.database().pool()).await.unwrap();
    let corrupt_image = scope_image(&f, &domain, &ids[1]).await;
    let loads = storage.session_record_loads_for_test(&domain, &ids[1]);
    assert_eq!(
        storage.dirty_sessions(&domain, None, 128).await.unwrap(),
        ids
    );
    assert_eq!(
        storage.session_record_loads_for_test(&domain, &ids[1]),
        loads,
        "discovery never authenticates session rows"
    );
    for run in 0..2 {
        let loads = storage.session_record_loads_for_test(&domain, &ids[1]);
        let report = drain_gc(
            &journal,
            JournalGcRequest {
                storage_id: journal.storage_id(),
                expected_root_epoch: journal.root_epoch().unwrap(),
                max_objects: 20_000,
                work_budget: Default::default(),
            },
        )
        .await
        .unwrap();
        assert_eq!(report.unreadable_sessions, vec![ids[1].clone()]);
        assert_eq!(
            storage.session_record_loads_for_test(&domain, &ids[1]) - loads,
            1,
            "one bad load per collection, including local hints"
        );
        assert_eq!(scope_image(&f, &domain, &ids[1]).await, corrupt_image);
        if run == 0 {
            assert!(report.reclaimed_bytes > 0);
        }
    }
    let after: Vec<(String,i64)> = sqlx::query_as("SELECT runtime_session_id,sum(retained_canonical_bytes) FROM agent_session_journal_pack GROUP BY runtime_session_id ORDER BY runtime_session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(before[1], after[1]);
    assert!(after[0].1 < before[0].1 && after[2].1 < before[2].1);
    let stats = storage.byte_stats(&domain).await.unwrap();
    assert_eq!(stats.skipped_sessions, 1);
    assert_eq!(stats.retained_bytes, (after[0].1 + after[2].1) as u64);
    assert_eq!(
        stats.live_bytes + stats.deferred_bytes,
        stats.retained_bytes
    );
    assert_eq!(
        storage.retained_bytes(&domain).await.unwrap(),
        Some(stats.retained_bytes)
    );
    journal.compact().await.unwrap();
    assert_eq!(scope_image(&f, &domain, &ids[1]).await, corrupt_image);
    assert!(
        storage.load(&domain, &ids[1]).await.is_err(),
        "corruption never becomes an absent session"
    );
    for id in [&ids[0], &ids[2]] {
        assert!(snapshot(&journal, id).await.history.len() >= 5);
    }
}

async fn drain_gc<S: JournalStorage>(
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

#[tokio::test]
async fn bounded_gc_reports_incomplete_live_membership() {
    let id = SessionId::new("incomplete-live-membership");
    let (f, store) = fixture(&id).await;
    let rt = runtime(store.clone(), provider());
    let session = rt
        .start_session(StartSession::create(
            id.clone(),
            vec![Message::user("keep this history")],
        ))
        .await
        .unwrap();
    session
        .run(UserInput::text("first"))
        .await
        .unwrap()
        .completed()
        .await;
    session.shutdown().await.unwrap();
    let journal = store.journals().protected();
    let raw = store.journals().storage();
    let domain = JournalDomain {
        storage_id: journal.storage_id(),
        role: JournalRole::Protected,
    };
    let record = raw.load(&domain, &id).await.unwrap().unwrap();
    assert!(record.collection_dirty);
    let packs: Vec<(i64,Vec<u8>,Vec<u8>)> = sqlx::query_as("SELECT pack_id,index_payload,body_payload FROM agent_session_journal_pack ORDER BY pack_id").fetch_all(f.database().pool()).await.unwrap();
    sqlx::query("DELETE FROM agent_session_journal_membership WHERE storage_id=? AND role='protected' AND runtime_session_id=?")
        .bind(domain.storage_id.as_str()).bind(id.as_str()).execute(f.database().pool()).await.unwrap();
    let report = drain_gc(
        &journal,
        JournalGcRequest::new(journal.storage_id(), journal.root_epoch().unwrap(), 20_000)
            .with_budget(JournalGcBudget {
                max_work_units: 1,
                ..Default::default()
            }),
    )
    .await
    .unwrap();
    assert_eq!(report.unreadable_sessions, vec![id.clone()]);
    assert!(
        report.work_units > 1,
        "the pass drains several bounded calls"
    );
    assert!(!report.has_more);
    assert_eq!(report.deleted, 0);
    assert_eq!(raw.load(&domain, &id).await.unwrap().unwrap(), record);
    let unchanged: Vec<(i64,Vec<u8>,Vec<u8>)> = sqlx::query_as("SELECT pack_id,index_payload,body_payload FROM agent_session_journal_pack ORDER BY pack_id").fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(
        packs, unchanged,
        "incomplete membership preserves every sealed pack"
    );
}
