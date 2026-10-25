//! Native journal deltas, projections, probes, and runtime leases.

use super::*;

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
