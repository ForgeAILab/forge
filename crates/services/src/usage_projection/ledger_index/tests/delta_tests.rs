//! Incremental ledger updates preserve ownership and provenance winners.

use super::*;

#[tokio::test]
async fn appended_events_read_only_deltas_at_two_history_sizes() {
    for history in [12, 420] {
        let db = fixture().await;
        project(&db, "p").await;
        for n in 0..history {
            let id = format!("run-{n}");
            execution(&db, &id, "p", "a", db::ExecutionStatus::Completed).await;
            let i = admit(&db, &format!("attempt-{n}"), "p", &id, "a").await;
            let i = start(&db, &i).await;
            let i = settle(&db, &i).await;
            event(&db, &i, &format!("old-{n}"), "a", true, 0).await;
        }
        let index = UsageLedgerIndex::new(db.clone());
        index.operations().await.unwrap();
        let before_b = index.agent("b").await.unwrap();
        let i = UsageLedgerRepo::get_usage_invocation(&*db, "attempt-0")
            .await
            .unwrap()
            .unwrap();
        for n in 1..=7 {
            event(&db, &i, &format!("delta-{n}"), "a", true, n).await;
        }
        index.operations().await.unwrap();
        let audit = index.state.lock().await.audit;
        assert_eq!(
            audit.payload_rows, 14,
            "only seven events and seven provenance joins, history={history}"
        );
        assert_eq!(audit.attempt_updates, 0);
        assert_reference(&db, &index, 0, history).await;
        assert_eq!(
            serde_json::to_value(index.agent("b").await.unwrap()).unwrap(),
            serde_json::to_value(before_b).unwrap()
        );
        let state = index.state.lock().await;
        assert!(state.bytes < MAX_INDEX_BYTES);
        assert!(!state.scopes.contains_key(&Some(Arc::from("b"))));
    }
}

#[tokio::test]
async fn old_execution_ownership_and_source_provenance_updates_are_exact() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let i = admit(&db, "i", "p", "run", "a").await;
    let i = start(&db, &i).await;
    let i = settle(&db, &i).await;
    let e = event(&db, &i, "e", "b", false, 1).await;
    let index = UsageLedgerIndex::new(db.clone());
    assert_reference(&db, &index, 1, 0).await;
    // Metadata edits below rowid watermarks cannot be inferred from running
    // work. The revision trigger captures them regardless of terminal status.
    sqlx::query("UPDATE execution SET agent_id='c' WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_reference(&db, &index, 1, 1).await;
    reprice(&db, &e, 1).await;
    assert_reference(&db, &index, 1, 2).await;
    reprice(&db, &e, 2).await;
    assert_reference(&db, &index, 1, 3).await;
    sqlx::query("DELETE FROM execution WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_reference(&db, &index, 1, 4).await;
    ProjectRepo::delete(&*db, "p").await.unwrap();
    assert_reference(&db, &index, 1, 5).await;
    // Rowid reuse after teardown must remain visible after a rebuild.
    project(&db, "next").await;
    execution(&db, "new", "next", "b", db::ExecutionStatus::Running).await;
    assert_reference(&db, &index, 1, 6).await;
}

#[tokio::test]
async fn a_long_single_run_and_capacity_fallback_remain_correct() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let i = admit(&db, "i", "p", "run", "a").await;
    let i = start(&db, &i).await;
    let i = settle(&db, &i).await;
    for n in 0..420 {
        event(&db, &i, &format!("history-{n}"), "a", true, n).await;
    }
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    event(&db, &i, "delta", "a", true, 421).await;
    index.operations().await.unwrap();
    assert_eq!(
        index.state.lock().await.audit.payload_rows,
        2,
        "one event plus its frozen provenance, even on a long run"
    );
    assert_reference(&db, &index, 0, 0).await;
    index.state.lock().await.bytes = MAX_INDEX_BYTES;
    event(&db, &i, "overflow", "a", true, 422).await;
    assert_reference(&db, &index, 0, 1).await;
    assert!(index.state.lock().await.overflow.is_some());
    ProjectRepo::delete(&*db, "p").await.unwrap();
    assert_reference(&db, &index, 0, 2).await;
    assert!(index.state.lock().await.overflow.is_none());
    println!(
        "summary bytes: Invocation={}, Events={}, Run={}, Execution={}, budget={}",
        std::mem::size_of::<Invocation>(),
        std::mem::size_of::<Events>(),
        std::mem::size_of::<Run>(),
        std::mem::size_of::<Execution>(),
        MAX_INDEX_BYTES
    );
}

#[test]
fn provenance_last_writer_is_ordered_not_additive() {
    let mut first = crate::task_usage_fixture::invocation("a");
    first.source_id = "run".into();
    first.agent_id = Some("a".into());
    let mut second = first.clone();
    second.id = "b".into();
    second.agent_id = Some("b".into());
    second.attempt_ordinal = 1;
    let make = |id: &str, inv: &str, freshness, retrospective, amount| {
        let mut event = crate::task_usage_fixture::event(id, inv);
        event.source_id = "run".into();
        event.agent_id = Some("a".into());
        event.input_tokens = Some(1);
        event.output_tokens = Some(0);
        event.provider_reported_nano_usd = None;
        event.estimated_nano_usd = Some(amount);
        event.cost_kind = UsageCostKind::Estimated;
        EffectiveUsageEvent {
            event,
            source: Some((
                "estimated:models_dev_catalog:rate:catalog".into(),
                CostSourceRef {
                    source_kind: CostSourceKind::ModelsDevCatalog,
                    rate_revision_id: Some("rate".into()),
                    catalog_snapshot_id: Some("catalog".into()),
                    catalog_digest: None,
                    effective_at: None,
                    fetched_at: None,
                    freshness,
                    retrospective,
                    formula_revision: Some("formula".into()),
                },
            )),
        }
    };
    let e1 = make("1", "a", CostSourceFreshness::Fresh, false, 1000);
    let e2 = make("2", "b", CostSourceFreshness::Stale, true, 2000);
    let mut state = State::default();
    state.invocation(first.clone()).unwrap();
    state.invocation(second.clone()).unwrap();
    state.event(e2.clone(), 1).unwrap();
    state.event(e1.clone(), 1).unwrap();
    let compare = |state: &mut State, second_event: EffectiveUsageEvent| {
        let expected = aggregate_usage_with_sources(
            &[first.clone(), second.clone()],
            &HashMap::from([
                ("a".into(), vec![e1.clone()]),
                ("b".into(), vec![second_event]),
            ]),
            &[],
        )
        .unwrap();
        let actual = state.scopes.get_mut(&None).unwrap().public().unwrap();
        assert_eq!(actual, expected);
    };
    compare(&mut state, e2.clone());
    state.event(e2, -1).unwrap();
    let changed = make("2", "b", CostSourceFreshness::RefreshFailed, true, 3000);
    state.event(changed.clone(), 1).unwrap();
    compare(&mut state, changed);
}

#[test]
fn retained_citation_winner_yields_to_newer_events_in_same_delta() {
    let reference = CostSourceRef {
        source_kind: CostSourceKind::ModelsDevCatalog,
        rate_revision_id: Some("rate".into()),
        catalog_snapshot_id: Some("catalog".into()),
        catalog_digest: None,
        effective_at: None,
        fetched_at: None,
        freshness: CostSourceFreshness::Fresh,
        retrospective: true,
        formula_revision: Some("formula".into()),
    };
    let order = |event: &str| SourceOrder {
        source: Arc::from("run"),
        ordinal: 0,
        invocation: Arc::from("inv"),
        occurred: Arc::from(AT),
        event: Arc::from(event),
    };
    let mut book = Citations::default();
    book.change(&reference, order("a"), 1, false).unwrap();
    book.change(&reference, order("b"), 1, false).unwrap();
    book.change(&reference, order("b"), -1, false).unwrap();
    book.change(&reference, order("b"), 1, false).unwrap();
    book.change(&reference, order("c"), 1, false).unwrap();
    book.repair(&reference, Some(order("b"))).unwrap();
    assert_eq!(book.winners.last_key_value().unwrap().0.event.as_ref(), "c");
    assert_eq!(book.winner().unwrap(), reference);
}

#[tokio::test]
async fn compact_citations_recover_removed_winners_without_retaining_events() {
    let db = fixture().await;
    project(&db, "p").await;
    admission_binding(&db).await;
    let inv = scoped_admit(
        &db,
        "priced",
        Some("p"),
        "z-run",
        Some("a"),
        DbUsageSurface::ProjectChat,
        2,
        true,
    )
    .await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    event(&db, &inv, "first", "a", false, 0).await;
    let last = event(&db, &inv, "last", "a", false, 1).await;
    let index = UsageLedgerIndex::new(db.clone());
    assert_reference_full(&db, &index, 0, 0).await;
    let before = index.state.lock().await.charged_bytes();
    for n in 2..502 {
        event(&db, &inv, &format!("more-{n}"), "a", false, n).await;
    }
    assert_reference_full(&db, &index, 0, 1).await;
    let state = index.state.lock().await;
    assert!(
        state.charged_bytes() < before + 4096,
        "same provenance must not retain event citations"
    );
    assert_eq!(
        state.scopes[&None]
            .sources
            .values()
            .map(|book| book.variants.len())
            .sum::<usize>(),
        1
    );
    let winning_id = state.scopes[&None]
        .sources
        .values()
        .next()
        .unwrap()
        .variants
        .values()
        .next()
        .unwrap()
        .winner
        .as_ref()
        .unwrap()
        .event
        .to_string();
    drop(state);
    // Reprice the current winning event. Its old key still has 501 events,
    // so the index must recover the next winner from the ledger snapshot.
    let winning = UsageLedgerRepo::get_usage_event(&*db, &winning_id)
        .await
        .unwrap()
        .unwrap();
    {
        let state = index.state.lock().await;
        let through = state.watermarks.unwrap();
        let mut connection = db.pool().acquire().await.unwrap();
        let old = effective_events(
            &mut connection,
            vec![winning.clone()],
            through.estimate_rowid,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let staged = Staged::Delta {
            changes: vec![Change::Event(Box::new(old.clone()), -1)],
            bytes: MAX_INDEX_BYTES - 1,
            audit: Audit::default(),
        };
        assert!(
            matches!(
                citation_repairs(&state, &staged, &mut connection, through, MAX_INDEX_BYTES)
                    .await
                    .unwrap(),
                CitationRepairs::Overflow(_)
            ),
            "repair buffers must share the delta's memory bound"
        );
        let mut repriced = old.clone();
        repriced.event.estimated_nano_usd = Some(old.event.estimated_nano_usd.unwrap() + 1);
        let changes = vec![
            Change::Event(Box::new(old), -1),
            Change::Event(Box::new(repriced), 1),
        ];
        let staged = Staged::Delta {
            bytes: state.charged_bytes() + changes.iter().map(Change::staging_bytes).sum::<usize>(),
            changes,
            audit: Audit::default(),
        };
        // A connection with no schema is a read seam: any historical lookup
        // fails. Amount-only repricing must retain the known citation winner.
        let mut empty = <sqlx::SqliteConnection as sqlx::Connection>::connect("sqlite::memory:")
            .await
            .unwrap();
        let CitationRepairs::Ready(repairs, _) =
            citation_repairs(&state, &staged, &mut empty, through, MAX_INDEX_BYTES)
                .await
                .unwrap()
        else {
            panic!("small reprice must fit")
        };
        assert_eq!(
            repairs.len(),
            2,
            "global and owning Agent keep the same winner"
        );
    }
    reprice(&db, &winning, 9200).await;
    assert_reference_full(&db, &index, 0, 2).await;
    reprice(&db, &last, 9201).await;
    assert_reference_full(&db, &index, 0, 3).await;
}

#[tokio::test]
async fn new_event_and_applied_revision_in_one_delta_are_folded_once() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    let inv = admit(&db, "i", "p", "run", "a").await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    let event = event(&db, &inv, "new", "b", false, 0).await;
    reprice(&db, &event, 9400).await;
    assert_reference_full(&db, &index, 0, 0).await;
}
