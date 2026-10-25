//! Ledger cancellation, cold rebuilds, overflow, deletion, and budget fallback.

use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_event_and_reprice_deltas_are_atomic() {
    use std::future::Future;
    use std::task::Poll;
    for reprice_delta in [false, true] {
        let mut completed = false;
        for cancel_after in 1..=200usize {
            let directory = tempfile::tempdir().unwrap();
            let db = file_db(&directory.path().join("cancel.db")).await;
            project(&db, "p").await;
            execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
            let i = admit(&db, "i", "p", "run", "a").await;
            let i = start(&db, &i).await;
            let i = settle(&db, &i).await;
            let e = event(&db, &i, "e0", "a", !reprice_delta, 0).await;
            if reprice_delta {
                reprice(&db, &e, 2000).await;
            }
            let index = UsageLedgerIndex::new(db.clone());
            assert_reference_full(&db, &index, 0, 0).await;
            if reprice_delta {
                reprice(&db, &e, 2001).await;
            } else {
                event(&db, &i, "e1", "a", true, 1).await;
            }
            completed = {
                let future = index.operations();
                tokio::pin!(future);
                let mut polls = 0;
                std::future::poll_fn(|cx| {
                    polls += 1;
                    if polls > cancel_after {
                        return Poll::Ready(false);
                    }
                    match future.as_mut().poll(cx) {
                        Poll::Ready(value) => {
                            value.unwrap();
                            Poll::Ready(true)
                        }
                        Poll::Pending => Poll::Pending,
                    }
                })
                .await
            };
            assert_reference_full(&db, &index, u64::from(reprice_delta), cancel_after).await;
            assert_reference_full(&db, &index, u64::from(reprice_delta), cancel_after).await;
            if completed {
                break;
            }
        }
        assert!(
            completed,
            "never reached completion reprice={reprice_delta}"
        );
    }
}

#[tokio::test]
async fn invariant_failure_and_small_deletion_use_memoized_unlocked_fallback() {
    let db = fixture().await;
    project(&db, "p").await;
    for n in 0..4 {
        execution(
            &db,
            &format!("run-{n}"),
            "p",
            "a",
            db::ExecutionStatus::Completed,
        )
        .await;
    }
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    // A corrupt internal counter never becomes an API InvalidTransition.
    index
        .state
        .lock()
        .await
        .scopes
        .get_mut(&None)
        .unwrap()
        .sources
        .insert("broken".into(), Citations::default());
    index
        .state
        .lock()
        .await
        .scopes
        .get_mut(&None)
        .unwrap()
        .cached = None;
    assert_reference(&db, &index, 0, 0).await;
    assert!(index.state.lock().await.overflow.is_some());
    assert!(index.fallback.lock().await.operations.is_some());
    sqlx::query("DELETE FROM execution WHERE id='run-0'")
        .execute(db.pool())
        .await
        .unwrap();
    // Exactly 25% shrink is enough to retry, and this small ledger fits again.
    assert_reference_full(&db, &index, 0, 1).await;
    assert!(index.state.lock().await.overflow.is_none());
}

#[tokio::test]
async fn cold_build_skips_change_markers_and_repairs_stale_rowid_header() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let inv = admit(&db, "i", "p", "run", "a").await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    event(&db, &inv, "e", "a", true, 0).await;
    sqlx::query("UPDATE execution SET updated_at='1970-01-01T00:04:00Z' WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE usage_ledger_revision SET invocation_rowid=0,event_rowid=0,execution_rowid=0 WHERE id=1").execute(db.pool()).await.unwrap();
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    let audit = index.state.lock().await.audit;
    assert_eq!(
        audit.payload_rows, 4,
        "one invocation, event, provenance join and execution; no change-marker replay"
    );
    assert_reference_full(&db, &index, 0, 0).await;
    let inv = UsageLedgerRepo::get_usage_invocation(&*db, "i")
        .await
        .unwrap()
        .unwrap();
    event(&db, &inv, "new", "a", true, 1).await;
    assert_reference_full(&db, &index, 0, 1).await;
    // A future table rebuild can also leave an oversized maximum. An insert
    // below that stale maximum still has to invalidate the warm projection.
    sqlx::query("UPDATE usage_ledger_revision SET event_rowid=1000 WHERE id=1")
        .execute(db.pool())
        .await
        .unwrap();
    let high = UsageLedgerIndex::new(db.clone());
    assert_reference_full(&db, &high, 0, 2).await;
    event(&db, &inv, "after-rebuild", "a", true, 2).await;
    assert_reference_full(&db, &high, 0, 3).await;
}

#[test]
fn cold_fit_probe_matches_hash_table_capacity_without_allocating_history() {
    for rows in [
        1usize, 3, 4, 7, 8, 14, 15, 28, 29, 100, 448, 449, 1792, 1793, 7168, 7169,
    ] {
        let map = HashMap::<u64, u64>::with_capacity(rows);
        let buckets = rows
            .saturating_mul(8)
            .div_ceil(7)
            .max(4)
            .next_power_of_two();
        assert_eq!(map.capacity(), buckets * 7 / 8, "rows={rows}");
    }
    let header = Watermarks {
        rows: [99_000, 99_000, 0, 33_000],
        owned_executions: 33_000,
        ..Watermarks::default()
    };
    assert!(UsageLedgerIndex::minimum_charge(header) > MAX_INDEX_BYTES);
}

#[tokio::test]
async fn overflow_source_batches_equal_reference_for_all_scopes() {
    // Reuse the batched random generator indirectly: build a mixed ledger, then compare.
    for seed in [0x1234_5678u64, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let mut rng = seed;
        project(&db, "p").await;
        let mut invs = Vec::new();
        let mut evs = Vec::new();
        for n in 0..300 {
            let id = format!("run-{n}");
            let status = if next(&mut rng).is_multiple_of(3) {
                db::ExecutionStatus::Running
            } else {
                db::ExecutionStatus::Completed
            };
            execution(&db, &id, "p", AGENTS[next(&mut rng) as usize % 3], status).await;
            for k in 0..(next(&mut rng) % 3) {
                let i = admit(
                    &db,
                    &format!("inv-{n}-{k}"),
                    "p",
                    &id,
                    AGENTS[next(&mut rng) as usize % 3],
                )
                .await;
                let i = start(&db, &i).await;
                let i = match next(&mut rng) % 4 {
                    0 => i,
                    1 => UsageLedgerRepo::mark_usage_invocation_unsettled(
                        &*db,
                        db::MarkUsageInvocationUnsettled {
                            id: i.id.clone(),
                            expected_version: i.version,
                            terminal_reason: "x".into(),
                            settled_at: AT.into(),
                            updated_at: AT.into(),
                        },
                    )
                    .await
                    .unwrap(),
                    _ => settle(&db, &i).await,
                };
                if i.lifecycle == UsageInvocationLifecycle::Settled {
                    for j in 0..(next(&mut rng) % 3) {
                        let e = event(
                            &db,
                            &i,
                            &format!("ev-{n}-{k}-{j}"),
                            AGENTS[next(&mut rng) as usize % 3],
                            next(&mut rng).is_multiple_of(2),
                            j as i64,
                        )
                        .await;
                        evs.push(e);
                    }
                }
                invs.push(i);
            }
        }
        let mut step = 0;
        for e in &evs {
            if e.cost_kind != UsageCostKind::ProviderReported && next(&mut rng).is_multiple_of(3) {
                step += 1;
                reprice(&db, e, 7000 + step).await;
            }
        }
        let mut tx = db.pool().begin().await.unwrap();
        let operations = overflow_usage_snapshot(&mut tx, None).await.unwrap();
        let ids = AGENTS.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        let agents = overflow_usage_snapshot(&mut tx, Some(&ids)).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            operations[&None],
            usage_aggregate_for_operations(&db).await.unwrap()
        );
        for id in AGENTS {
            assert_eq!(
                agents[&Some(Arc::from(id))],
                usage_aggregate_for_agent(&db, id).await.unwrap()
            );
            let new = usage_aggregate_for_agent(&db, id).await.unwrap();
            let old = old_agent_reference(&db, id).await.unwrap();
            assert_eq!(
                serde_json::to_value(new).unwrap(),
                serde_json::to_value(old).unwrap(),
                "agent {id} seed {seed}"
            );
        }
    }
}

#[tokio::test]
async fn zero_budget_uses_memoized_full_reads_even_for_an_empty_ledger() {
    let db = fixture().await;
    let index = UsageLedgerIndex::new(db.clone());
    index.set_budget_mb(Some(0));
    assert_reference(&db, &index, 0, 0).await;
    let status = index.status().await;
    assert!(status.fallback);
    assert_eq!(status.budget_bytes, 0);
    assert_eq!(status.current_size_bytes, 0);
    assert!(index.state.lock().await.watermarks.is_none());
    assert!(index.fallback.lock().await.operations.is_some());
    assert_eq!(index.state.lock().await.audit.payload_rows, 0);
    index.set_budget_mb(None);
    assert_reference(&db, &index, 0, 1).await;
    assert!(!index.status().await.fallback);
}

#[tokio::test]
async fn live_budget_changes_discard_and_rebuild_without_changing_results() {
    let db = fixture().await;
    project(&db, "p").await;
    let exec = execution(&db, "run", "p", "a", db::ExecutionStatus::Running).await;
    let invocation = admit(&db, "inv", "p", "run", "b").await;
    let invocation = start(&db, &invocation).await;
    let invocation = settle(&db, &invocation).await;
    let event = event(&db, &invocation, "e", "a", false, 5).await;
    let index = UsageLedgerIndex::new(db.clone());
    index.set_budget_mb(Some(16));
    assert_reference(&db, &index, 0, 0).await;
    let size = index.status().await.current_size_bytes;
    assert!(size > 1024 * 1024 && size < 16 * 1024 * 1024);
    // A fitting decrease preserves the warm index and performs no payload reads.
    index.set_budget_mb(Some(12));
    assert_reference(&db, &index, 0, 1).await;
    assert_eq!(index.status().await.current_size_bytes, size);
    assert_eq!(index.state.lock().await.audit.payload_rows, 0);
    // Even with no ledger mutation, the next read frees the oversized index.
    index.set_budget_mb(Some(1));
    assert_reference(&db, &index, 0, 2).await;
    assert!(index.status().await.fallback);
    assert_eq!(index.status().await.current_size_bytes, 0);
    assert!(index.state.lock().await.invocations.is_empty());
    assert!(index.fallback.lock().await.operations.is_some());
    reprice(&db, &event, 51).await;
    finish(&db, &exec, true).await;
    assert_reference(&db, &index, 0, 3).await;
    index.set_budget_mb(Some(0));
    assert_reference(&db, &index, 0, 4).await;
    assert_eq!(index.status().await.budget_bytes, 0);
    // Increasing the bound retries a cold build without requiring ledger changes.
    index.set_budget_mb(Some(16));
    index.operations().await.unwrap();
    assert!(index.state.lock().await.audit.payload_rows > 0);
    assert!(!index.status().await.fallback);
    assert_reference(&db, &index, 0, 5).await;
    index.set_budget_mb(None);
    assert_reference(&db, &index, 0, 6).await;
    assert_eq!(index.status().await.budget_bytes, 128 * 1024 * 1024);
}
