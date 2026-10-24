use super::*;

/// Counts sqlx statements once armed. sqlx logs from each connection's worker
/// thread, so this is a process-wide subscriber: the count is exact only when
/// the measuring test runs alone (filtered); assertions use the per-storage
/// cache counters and the database image instead.
#[derive(Default)]
struct SqlCounter {
    armed: std::sync::atomic::AtomicBool,
    statements: std::sync::Mutex<Vec<String>>,
}
struct SqlCounterHandle(Arc<SqlCounter>);
impl tracing::Subscriber for SqlCounterHandle {
    fn register_callsite(
        &self,
        m: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if m.target() == "sqlx::query" {
            tracing::subscriber::Interest::sometimes()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
    fn enabled(&self, m: &tracing::Metadata<'_>) -> bool {
        m.target() == "sqlx::query" && self.0.armed.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Summary(String);
        impl tracing::field::Visit for Summary {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "summary" {
                    self.0 = format!("{v:?}");
                }
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                if f.name() == "summary" {
                    self.0 = v.to_owned();
                }
            }
        }
        let mut summary = Summary(String::new());
        event.record(&mut summary);
        self.0.statements.lock().unwrap().push(summary.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
fn sql_counter() -> Arc<SqlCounter> {
    static COUNTER: std::sync::OnceLock<Arc<SqlCounter>> = std::sync::OnceLock::new();
    COUNTER
        .get_or_init(|| {
            let counter = Arc::new(SqlCounter::default());
            let _ = tracing::subscriber::set_global_default(SqlCounterHandle(counter.clone()));
            counter
        })
        .clone()
}

/// D4/D5 performance shape: a warm commit of one turn on a long session
/// writes exactly one new pack, reads no existing body or index, leaves every
/// existing pack row byte-identical and stays a small fixed number of pages.
#[tokio::test]
async fn warm_commit_writes_only_its_delta() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("long-session");
    let turn = |n: usize, w: &mut JournalWrite| {
        // One turn: messages/state values, a batch and the head.
        let mut objects = vec![
            object(format!("user message {n} {}", "u".repeat(300))),
            object(format!("assistant reply {n} {}", "a".repeat(900))),
            object(serde_json::json!({"usage": n, "tokens": n * 17})),
        ];
        let batch =
            JournalObject::encode(JournalObjectKind::Batch, &serde_json::json!({"turn": n}))
                .unwrap();
        objects.push(JournalStoredObject {
            id: batch.id,
            kind: JournalObjectKind::Batch,
            bytes: batch.bytes.into(),
        });
        let mut h = head(&d, &s, objects[0].id.clone());
        h.revision = JournalRevision(n as u64 + 1);
        h.batch_sequence = n as u64;
        let ho = JournalObject::encode(JournalObjectKind::Head, &h).unwrap();
        objects.push(JournalStoredObject {
            id: ho.id.clone(),
            kind: JournalObjectKind::Head,
            bytes: ho.bytes.clone().into(),
        });
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        w.kind = JournalWriteKind::Commit;
        w.objects = JournalObjectBatch { objects };
        w.record.revision = h.revision;
        w.record.head = Some(ho);
        w.operation = Some(JournalOperationRecord {
            operation: JournalOperationId::new(format!("turn-{n}")),
            digest: journal_history_chain(std::iter::empty()),
            revision: h.revision,
            result: h,
        });
    };
    let mut first = write(&d, s.as_str(), vec![object("genesis")]);
    first.operation = None;
    mapped.apply(first).await.unwrap();
    for n in 0..200 {
        let mut w = edit(&raw, &d, &s).await;
        turn(n, &mut w);
        raw.apply(w).await.unwrap();
        raw.post_terminal(&d, &s).await.unwrap();
    }
    // Warm this storage's directory as a running session would have it.
    raw.members(&d, &s).await.unwrap();
    let before_rows = pack_rows(&f, &d, &s).await;
    let mut w = edit(&raw, &d, &s).await;
    turn(200, &mut w);
    let pool = f.database().pool().clone();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    let sequence = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT seq FROM sqlite_sequence WHERE name='agent_session_journal_pack'",
        )
        .fetch_one(f.database().pool())
        .await
        .unwrap()
    };
    let sequence_before = sequence().await;
    let wal = f.directory().join("journal.db-wal");
    let wal_before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let stats_before = raw.cache_stats().unwrap();
    let counter = sql_counter();
    counter.statements.lock().unwrap().clear();
    counter
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    raw.apply(w).await.unwrap();
    counter
        .armed
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let statements = std::mem::take(&mut *counter.statements.lock().unwrap());
    let stats = raw.cache_stats().unwrap();
    let wal_after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let after_rows = pack_rows(&f, &d, &s).await;
    // Every pre-existing pack row is byte-identical; exactly one row is new.
    for row in &before_rows {
        assert!(after_rows.contains(row), "pack {} was rewritten", row.0);
    }
    assert_eq!(after_rows.len(), before_rows.len() + 1);
    // The new bodies are encoded, sealed and inserted once: no intermediate
    // per-cut pack rows were written and deleted inside the transaction.
    assert_eq!(sequence().await, sequence_before + 1);
    assert_eq!(after_rows.last().unwrap().0, sequence_before + 1);
    // Only the new pack's index is decoded; no existing body is read.
    assert_eq!(stats.body_decodes - stats_before.body_decodes, 0);
    assert_eq!(stats.index_decodes - stats_before.index_decodes, 1);
    assert_eq!(stats.pack_queries - stats_before.pack_queries, 1);
    let frames = wal_after.saturating_sub(wal_before.max(32)) / (4096 + 24);
    let operation_bytes: i64 = sqlx::query_scalar("SELECT coalesce(sum(length(payload)),0) FROM agent_session_journal_operation_pack WHERE runtime_session_id='long-session'")
        .fetch_one(&pool).await.unwrap();
    let mut kinds = std::collections::BTreeMap::<String, usize>::new();
    for s in &statements {
        *kinds.entry(s.clone()).or_default() += 1;
    }
    eprintln!(
        "warm commit: {} existing packs, {} statements (exact only when run alone) {kinds:?}, WAL +{} bytes ({frames} pages), sealed operation tiers read {operation_bytes} bytes",
        before_rows.len(),
        statements.len(),
        wal_after.saturating_sub(wal_before)
    );
    assert!(frames <= 64, "{frames} WAL pages for one warm turn");
}

#[tokio::test]
async fn collection_apply_decrypts_each_pack_once_under_lock() {
    check_collection_apply_decrypts_under_lock(false).await;
}

#[tokio::test]
async fn collection_apply_uses_warm_cache_without_in_lock_decrypts() {
    check_collection_apply_decrypts_under_lock(true).await;
}

async fn check_collection_apply_decrypts_under_lock(warm: bool) {
    for released in [8, 2] {
        let f = FileDb::new().await.unwrap();
        let raw = storage(&f).await;
        if !warm {
            raw.set_cache_budget(0).unwrap();
        }
        let d = domain(JournalRole::Protected);
        let s = SessionId::new("collection-decrypt-once");
        let first = equal_objects("decode-first", 8);
        MappingStorage::new(raw.clone(), f.clone())
            .apply(write(&d, s.as_str(), first.clone()))
            .await
            .unwrap();
        add_objects(&raw, &d, &s, equal_objects("decode-second", 8)).await;
        add_objects(&raw, &d, &s, equal_objects("decode-third", 8)).await;
        for revision in [8, 9] {
            let mut w = edit(&raw, &d, &s).await;
            w.operation = Some(model_operation(
                &d,
                &s,
                &format!("decode-op-{revision}"),
                revision,
                11,
                revision,
            ));
            raw.apply(w).await.unwrap();
        }
        let packs = session_pack_ids(&f, &s).await;
        assert_eq!(packs.len(), 3);
        let tiers: Vec<_> = operation_rows(&f, &d, &s).await.into_keys().collect();
        assert_eq!(tiers.len(), 2);
        let mut w = edit(&raw, &d, &s).await;
        w.kind = JournalWriteKind::Collect;
        w.release = first.iter().take(released).map(|o| o.id.clone()).collect();
        w.prune_operations_before = Some(JournalRevision(10));
        let plan = JournalCollectionPlan {
            write: w,
            max_units: 32,
        };
        assert!(matches!(
            raw.prepare_collection_apply(&plan).await.unwrap(),
            JournalCollectionPreflight::Fits { .. }
        ));
        if warm {
            assert_eq!(raw.cached_body_packs(&s).unwrap().len(), packs.len());
        }
        raw.reset_decrypt_counts_for_test();
        let receipt = raw.apply_collection(plan).await.unwrap();
        assert_eq!(receipt.rewritten_units, 4);
        assert_eq!(receipt.receipt.reclaimed_objects, released as u64);
        let bodies: Vec<_> = packs
            .iter()
            .map(|&id| raw.decrypt_count_for_test(&s, TestSealedUnit::PackBody(id)))
            .collect();
        let operations: Vec<_> = tiers
            .iter()
            .map(|&tier| raw.decrypt_count_for_test(&s, TestSealedUnit::Operations(tier)))
            .collect();
        eprintln!("in-lock body decrypts={bodies:?}, operation decrypts={operations:?}");
        if warm {
            assert_eq!(bodies, vec![0, 0, 0]);
        } else {
            assert_eq!(bodies, vec![1, 1, 1]);
        }
        assert_eq!(operations, vec![1, 1]);
    }
}
