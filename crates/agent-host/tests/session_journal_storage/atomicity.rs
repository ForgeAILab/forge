use super::*;

fn cut_objects(w: &mut JournalWrite) {
    for kind in [JournalObjectKind::Checkpoint, JournalObjectKind::Batch] {
        let o = JournalObject::encode(kind, &serde_json::json!({"stage":kind.as_str()})).unwrap();
        w.objects.objects.push(JournalStoredObject {
            id: o.id,
            kind,
            bytes: o.bytes.into(),
        });
    }
    let o = w.record.head.as_ref().unwrap();
    w.objects.objects.push(JournalStoredObject {
        id: o.id.clone(),
        kind: JournalObjectKind::Head,
        bytes: o.bytes.clone().into(),
    });
    w.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
}

#[tokio::test]
async fn apply_atomic_cuts() {
    let f = FileDb::new().await.unwrap();
    let inner = storage(&f).await;
    let mapped = MappingStorage::new(inner.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    for existing in [false, true] {
        for &cut in JournalCommitFailurePoint::ALL {
            let sid = format!("atomic-{existing}-{cut:?}");
            let session = SessionId::new(&sid);
            f.ensure_session(&session).await.unwrap();
            let old = object("old immutable body");
            let before_record = if existing {
                let base = write(&d, &sid, vec![old.clone()]);
                mapped.apply(base.clone()).await.unwrap();
                Some(base.record)
            } else {
                None
            };
            let new = object(format!("new-{cut:?}"));
            let mut w = write(&d, &sid, vec![new.clone()]);
            cut_objects(&mut w);
            if let Some(old) = &before_record {
                w.expect = old.fence();
                w.record.generation = old.generation + 1;
            }
            w.operation.as_mut().unwrap().operation = JournalOperationId::new("next-operation");
            w.inject = Some(cut);
            let before = image(&f).await;
            let error = mapped.apply(w.clone()).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Internal);
            assert!(error.retryable);
            assert_eq!(
                error.class,
                FailureClass::HostComponent {
                    stage: FailureStage::PreProvider,
                    component: FailureComponent::Checkpoint
                }
            );
            let reopened = reopen(&f).await;
            if cut.is_after_commit() {
                assert_ne!(image(&f).await, before);
                assert_eq!(
                    reopened.load(&d, &session).await.unwrap(),
                    Some(w.record.clone())
                );
                let bodies = reopened
                    .objects(
                        &d,
                        &session,
                        &w.objects
                            .objects
                            .iter()
                            .map(|o| o.id.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .unwrap();
                assert!(bodies.iter().all(Option::is_some));
                assert_eq!(
                    reopened
                        .operation(&d, &session, &w.operation.as_ref().unwrap().operation)
                        .await
                        .unwrap(),
                    w.operation
                );
                let packs: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id=?",
                )
                .bind(&sid)
                .fetch_one(f.database().pool())
                .await
                .unwrap();
                assert_eq!(
                    packs,
                    if existing { 2 } else { 1 },
                    "kind stages must publish one final new pack"
                );
                state_conflict(mapped.apply(w.clone()).await.unwrap_err());
            } else {
                assert_eq!(
                    image(&f).await,
                    before,
                    "all rows, ciphertexts, bitmaps and allocated ids roll back at {cut:?}"
                );
                assert_eq!(reopened.load(&d, &session).await.unwrap(), before_record);
                assert_eq!(
                    inner
                        .objects(&d, &session, std::slice::from_ref(&new.id))
                        .await
                        .unwrap(),
                    vec![None]
                );
                assert!(
                    reopened
                        .operation(&d, &session, &JournalOperationId::new("next-operation"))
                        .await
                        .unwrap()
                        .is_none()
                );
                let mut retry = w;
                retry.inject = None;
                mapped.apply(retry.clone()).await.unwrap();
                assert_eq!(
                    reopened.load(&d, &session).await.unwrap(),
                    Some(retry.record)
                );
            }
            if existing {
                assert_eq!(
                    inner
                        .objects(&d, &session, std::slice::from_ref(&old.id))
                        .await
                        .unwrap(),
                    vec![Some(old.bytes)]
                );
            }
        }
    }
    // A large multi-kind apply segments its final S1 output at the logical cap.
    let large: Vec<_> = (0..3)
        .map(|n| object(format!("{n}{}", "x".repeat(600_000))))
        .collect();
    let mut w = write(&d, "capped-import", large);
    cut_objects(&mut w);
    mapped.apply(w.clone()).await.unwrap();
    let sizes:Vec<i64>=sqlx::query_scalar("SELECT retained_canonical_bytes FROM agent_session_journal_pack WHERE runtime_session_id='capped-import'").fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(sizes.len(), 3);
    assert!(sizes.iter().all(|s| *s > 0 && *s <= 1_048_576));
    assert_eq!(
        inner
            .objects(
                &d,
                &w.session,
                &w.objects
                    .objects
                    .iter()
                    .map(|o| o.id.clone())
                    .collect::<Vec<_>>()
            )
            .await
            .unwrap(),
        w.objects
            .objects
            .iter()
            .map(|o| Some(o.bytes.clone()))
            .collect::<Vec<_>>()
    );
    // Oversized and malformed immutable candidates fail without any SQL effects.
    let mut bad = write(&d, "oversized", vec![object("small")]);
    bad.objects.objects[0].bytes = vec![0; 1_048_577].into();
    f.ensure_session(&bad.session).await.unwrap();
    let before = image(&f).await;
    state_conflict(mapped.apply(bad).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    let mut bad = write(&d, "bad-id", vec![object("bytes")]);
    bad.objects.objects[0].id = JournalObjectId::from_bytes([0; 32]);
    f.ensure_session(&bad.session).await.unwrap();
    state_conflict(mapped.apply(bad).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // Trace the lost acknowledgement too: commit happened, the caller saw an
    // error, and lock hold remains measured without leaking session/body data.
    use tracing::instrument::WithSubscriber;
    let events = ApplyEvents::default();
    let mut traced = write(
        &d,
        "trace-private-session",
        vec![object("trace-private-value")],
    );
    f.ensure_session(&traced.session).await.unwrap();
    traced.inject = Some(JournalCommitFailurePoint::AfterCommit);
    assert!(
        inner
            .apply(traced)
            .with_subscriber(events.dispatch())
            .await
            .is_err()
    );
    {
        let captured = events.0.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let event = &captured[0];
        assert_eq!(
            event.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "committed",
                "duration_us",
                "kind",
                "lock_hold_us",
                "message",
                "reclaimed_bytes",
                "reclaimed_objects",
                "success"
            ]
        );
        assert_eq!(event["committed"], "true");
        assert_eq!(event["success"], "false");
        assert!(event["lock_hold_us"].parse::<u64>().unwrap() > 0);
        assert!(
            event["duration_us"].parse::<u64>().unwrap()
                >= event["lock_hold_us"].parse::<u64>().unwrap()
        );
        assert!(!format!("{event:?}").contains("private"));
    }
    // Operation conflicts are atomic after all earlier publication stages.
    let original = write(&d, "operation-conflict", vec![object("one")]);
    mapped.apply(original.clone()).await.unwrap();
    let mut changed = write(&d, "operation-conflict", vec![object("two")]);
    changed.expect = original.record.fence();
    changed.record.generation = 2;
    let before = image(&f).await;
    state_conflict(mapped.apply(changed).await.unwrap_err());
    assert_eq!(image(&f).await, before);
}

#[tokio::test]
async fn independent_connection_expect_fences() {
    let f = FileDb::new().await.unwrap();
    let a = storage(&f).await;
    let b = reopen(&f).await;
    let ma = MappingStorage::new(a.clone(), f.clone());
    let mb = MappingStorage::new(b.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    let initial = write(&d, "fences", vec![object("original")]);
    ma.apply(initial.clone()).await.unwrap();
    let session = &initial.session;
    let record = b.load(&d, session).await.unwrap().unwrap();
    let mut first = control(&d, session, record.clone());
    first.pins = vec![JournalPinChange::Put(JournalPinRecord {
        pin: JournalPinId::new("concurrent-a"),
        blocks_retirement: false,
        roots: vec![],
        engine: engine(),
    })];
    let mut second = first.clone();
    let JournalPinChange::Put(p) = &mut second.pins[0] else {
        unreachable!()
    };
    p.pin = JournalPinId::new("concurrent-b");
    let (ra, rb) = tokio::join!(ma.apply(first), mb.apply(second));
    assert_ne!(ra.is_ok(), rb.is_ok());
    if let Err(e) = ra {
        state_conflict(e);
    }
    if let Err(e) = rb {
        state_conflict(e);
    }
    let current = a.load(&d, session).await.unwrap().unwrap();
    assert_eq!(current.generation, 2);
    assert_eq!(current.revision, record.revision);
    assert_eq!(current.writer_epoch, record.writer_epoch);
    let pins = a.pins(&d, session).await.unwrap();
    assert_eq!(pins.len(), 2);
    // Each fence coordinate independently guards the transaction.
    for coordinate in 0..3 {
        let mut stale = control(&d, session, current.clone());
        match coordinate {
            0 => stale.expect.revision.0 += 1,
            1 => stale.expect.writer_epoch += 1,
            _ => {
                stale.expect.generation -= 1;
                stale.record.generation = stale.expect.generation + 1;
            }
        }
        let before = image(&f).await;
        state_conflict(mb.apply(stale).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    for generation in [current.generation, current.generation + 2] {
        let mut bad = control(&d, session, current.clone());
        bad.record.generation = generation;
        let before = image(&f).await;
        state_conflict(ma.apply(bad).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    // Another Forge subsystem's write is not a journal semantic conflict.
    sqlx::query("UPDATE agent_session SET updated_at='unrelated-subsystem' WHERE runtime_session_id='fences'").execute(f.database().pool()).await.unwrap();
    let mut exact = control(&d, session, current.clone());
    exact.record.revision = JournalRevision(u64::MAX);
    exact.record.writer_epoch = u64::MAX;
    mb.apply(exact.clone()).await.unwrap();
    assert_eq!(
        a.load(&d, session).await.unwrap(),
        Some(exact.record.clone())
    );
    // Warm indexes survive commits; only one new index is decoded per new pack.
    let id = initial.objects.objects[0].id.clone();
    a.objects(&d, session, std::slice::from_ref(&id))
        .await
        .unwrap();
    b.objects(&d, session, std::slice::from_ref(&id))
        .await
        .unwrap();
    let mut latest = exact.record;
    for n in 0..8 {
        let o = object(format!("independent-admission-{n}"));
        let mut next = control(&d, session, latest.clone());
        next.objects.objects = vec![o.clone()];
        let before = a.cache_stats().unwrap();
        ma.apply(next.clone()).await.unwrap();
        let after = a.cache_stats().unwrap();
        assert_eq!(after.index_decodes, before.index_decodes + 1);
        assert_eq!(
            after.body_decodes, before.body_decodes,
            "warm commit must not hydrate the closure"
        );
        latest = next.record;
        // B's directory is stale. It refreshes inside its own write transaction,
        // finds A's body and reuses that slot instead of adding another copy.
        let mut duplicate = control(&d, session, latest.clone());
        duplicate.objects.objects = vec![o.clone()];
        mb.apply(duplicate.clone()).await.unwrap();
        latest = duplicate.record;
        assert_eq!(
            b.objects(&d, session, std::slice::from_ref(&o.id))
                .await
                .unwrap(),
            vec![Some(o.bytes)]
        );
    }
    let packs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id='fences'",
    )
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(packs, 9);
    // Pin/catalog Put/Remove participate in the fenced transaction verbatim.
    let mut remove = control(&d, session, latest.clone());
    remove.pins = vec![JournalPinChange::Remove(JournalPinId::new("pin"))];
    remove.catalog = vec![JournalCatalogChange::Remove(
        initial.record.head.as_ref().unwrap().id.clone(),
    )];
    ma.apply(remove.clone()).await.unwrap();
    latest = remove.record;
    assert_eq!(a.pins(&d, session).await.unwrap().len(), 1);
    assert!(
        b.catalog(&d, session, &initial.record.head.as_ref().unwrap().id)
            .await
            .unwrap()
            .is_none()
    );
    // An absent record accepts only the complete zero fence.
    for coordinate in 0..3 {
        let mut absent = write(&d, &format!("absent-{coordinate}"), vec![]);
        match coordinate {
            0 => absent.expect.revision.0 = 1,
            1 => absent.expect.writer_epoch = 1,
            _ => {
                absent.expect.generation = 1;
                absent.record.generation = 2;
            }
        }
        f.ensure_session(&absent.session).await.unwrap();
        let before = image(&f).await;
        state_conflict(ma.apply(absent).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    // Full-width generation exhaustion cannot wrap through a zero successor.
    let mut exhausted = write(&d, "generation-exhausted", vec![]);
    exhausted.record.generation = u64::MAX;
    f.seed_journal(&a, exhausted.clone()).await.unwrap();
    exhausted.expect = exhausted.record.fence();
    exhausted.record.generation = 0;
    let before = image(&f).await;
    state_conflict(ma.apply(exhausted).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // A changed immutable body under the same full id remains a real conflict.
    let mut immutable = control(&d, session, latest);
    let mut o = initial.objects.objects[0].clone();
    o.bytes = object("different bytes").bytes;
    immutable.objects.objects = vec![o];
    let before = image(&f).await;
    state_conflict(ma.apply(immutable).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // Missing/ambiguous/changed host mappings are host failures, not conflicts.
    let missing = write(&d, "missing-mapping", vec![]);
    let before = image(&f).await;
    assert_eq!(
        a.apply(missing).await.unwrap_err().kind,
        ErrorKind::Internal
    );
    assert_eq!(image(&f).await, before);
    let duplicate_session = SessionId::new("ambiguous-mapping");
    f.ensure_session(&duplicate_session).await.unwrap();
    sqlx::query("INSERT INTO agent_session(id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('duplicate-host','journal-agent','journal-profile','journal-scope','native','ambiguous-mapping','suspended','now','now')").execute(f.database().pool()).await.unwrap();
    assert_eq!(
        a.apply(write(&d, "ambiguous-mapping", vec![]))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Internal
    );
    assert_eq!(image(&f).await, before);
    let mut moved = write(&d, "moved-mapping", vec![]);
    ma.apply(moved.clone()).await.unwrap();
    sqlx::query(
        "UPDATE agent_session SET runtime_session_id=NULL WHERE runtime_session_id='moved-mapping'",
    )
    .execute(f.database().pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO agent_session(id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('replacement-host','journal-agent','journal-profile','journal-scope','native','moved-mapping','suspended','now','now')").execute(f.database().pool()).await.unwrap();
    moved.expect = moved.record.fence();
    moved.record.generation += 1;
    let before = image(&f).await;
    assert_eq!(a.apply(moved).await.unwrap_err().kind, ErrorKind::Internal);
    assert_eq!(image(&f).await, before);
}

// Capture just the journal target without a new logging dependency. The field
// whitelist proves that ids, keys and decrypted values never enter apply traces.
type ApplyEvent = std::collections::BTreeMap<String, String>;
#[derive(Clone, Default)]
struct ApplyEvents(Arc<std::sync::Mutex<Vec<ApplyEvent>>>);
impl ApplyEvents {
    /// With exactly one scoped dispatcher registered, tracing-core resolves a
    /// callsite's first registration through the registering thread's default
    /// ("just one" mode). A parallel test reaching the apply event first then
    /// caches `Interest::never` and this capture silently sees no event. A
    /// second, permanently registered dispatcher that enables nothing forces
    /// `Interest::sometimes`, so every event is checked against the current
    /// thread's dispatcher. It is registered before the capture so the level
    /// filter only opens once both are present.
    fn dispatch(&self) -> tracing::Dispatch {
        static SILENT: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        SILENT.get_or_init(|| tracing::Dispatch::new(Silent));
        tracing::Dispatch::new(self.clone())
    }
}
struct Silent;
impl tracing::Subscriber for Silent {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::never()
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::OFF)
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
impl tracing::Subscriber for ApplyEvents {
    fn enabled(&self, m: &tracing::Metadata<'_>) -> bool {
        m.target() == "forge_session_journal"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(std::collections::BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.insert(f.name().into(), format!("{v:?}"));
            }
        }
        let mut fields = Fields(Default::default());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Drop the apply future after its `limit`-th poll. Ready(None) = cancelled.
async fn cancel_after<F: std::future::Future>(future: F, limit: usize) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    std::future::poll_fn(|cx| {
        if polls == limit {
            return std::task::Poll::Ready(None);
        }
        polls += 1;
        future.as_mut().poll(cx).map(Some)
    })
    .await
}

/// Cancelling an apply at every await point (staging, release/purge, commit)
/// leaves exactly the old or the new state, never releases the write lock
/// late, and never leaves the warm cache disagreeing with a fresh storage.
#[tokio::test]
async fn apply_cancelled_at_every_await_is_atomic() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("cancelled");
    let mut seed = write(&d, s.as_str(), vec![object("cancel-genesis")]);
    seed.operation = None;
    MappingStorage::new(raw.clone(), f.clone())
        .apply(seed)
        .await
        .unwrap();
    let mut previous: Option<JournalStoredObject> = None;
    let (mut cancelled, mut committed_while_cancelled) = (0, 0);
    for limit in 1.. {
        let before = raw.load(&d, &s).await.unwrap().unwrap();
        let body = object(format!("cancel-{limit}-{}", "c".repeat(2048)));
        let mut w = control(&d, &s, before.clone());
        w.kind = JournalWriteKind::Collect;
        w.objects = JournalObjectBatch {
            objects: vec![body.clone()],
        };
        // Releasing the previous body empties its pack: a purge to cancel in.
        w.release = previous.iter().map(|o| o.id.clone()).collect();
        w.operation = Some(JournalOperationRecord {
            operation: JournalOperationId::new(format!("cancel-{limit}")),
            digest: journal_history_chain(std::iter::empty()),
            revision: before.revision,
            result: head(&d, &s, body.id.clone()),
        });
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            cancel_after(raw.apply(w.clone()), limit),
        )
        .await
        .expect("apply hung");
        let fresh = ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
        let record = fresh.load(&d, &s).await.unwrap().unwrap();
        let committed = record == w.record;
        assert!(committed || record == before, "limit {limit}: mixed record");
        let ids: Vec<_> = std::iter::once(body.id.clone())
            .chain(previous.iter().map(|o| o.id.clone()))
            .collect();
        let found = fresh.objects(&d, &s, &ids).await.unwrap();
        let op = fresh
            .operation(&d, &s, &JournalOperationId::new(format!("cancel-{limit}")))
            .await
            .unwrap();
        if committed {
            assert_eq!(found[0].as_ref(), Some(&body.bytes), "limit {limit}");
            assert!(found.get(1).is_none_or(Option::is_none), "limit {limit}");
            assert!(op.is_some());
        } else {
            assert!(found[0].is_none(), "limit {limit}");
            assert!(found.get(1).is_none_or(Option::is_some), "limit {limit}");
            assert!(op.is_none());
        }
        // The warm storage agrees with a fresh one after the cancellation.
        assert_eq!(raw.load(&d, &s).await.unwrap(), Some(record.clone()));
        assert_eq!(raw.objects(&d, &s, &ids).await.unwrap(), found);
        assert_eq!(
            raw.members(&d, &s).await.unwrap(),
            fresh.members(&d, &s).await.unwrap()
        );
        assert_eq!(
            raw.retained_bytes(&d).await.unwrap(),
            fresh.retained_bytes(&d).await.unwrap()
        );
        match outcome {
            Some(result) => {
                result.unwrap();
                break;
            }
            None => {
                cancelled += 1;
                if committed {
                    committed_while_cancelled += 1;
                }
            }
        }
        if committed {
            previous = Some(body);
        }
    }
    assert!(cancelled > 5);
    eprintln!(
        "apply cancellation: {cancelled} await points, {committed_while_cancelled} committed before the drop"
    );
}
