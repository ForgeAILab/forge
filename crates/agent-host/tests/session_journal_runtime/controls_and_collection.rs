//! Journal controls, writer fences, credentials, and collection.

use super::*;

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
