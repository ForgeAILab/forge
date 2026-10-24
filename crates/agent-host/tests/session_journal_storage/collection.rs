use super::*;

#[tokio::test]
async fn bounded_collection_pages_reject_repacked_positions() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("bounded-pages");
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), equal_objects("first", 4)))
        .await
        .unwrap();
    for i in 0..4 {
        add_objects(&raw, &d, &s, equal_objects(&format!("pack-{i}"), 4)).await;
    }
    let expected = raw.members(&d, &s).await.unwrap();
    let cold = reopen(&f).await;
    let fence = cold.load(&d, &s).await.unwrap().unwrap().fence();
    let first = cold
        .collection_page(
            &d,
            &s,
            JournalCollectionPageRequest {
                kind: JournalCollectionPageKind::Members,
                fence,
                after: None,
                max_items: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.items.len(), 1);
    assert_eq!(cold.cache_stats().unwrap().index_decodes, 1);
    assert_eq!(cold.cache_stats().unwrap().body_decodes, 1);
    let continuation = first.next.unwrap();
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(raw.load(&d, &s).await.unwrap().unwrap().fence(), fence);
    assert_eq!(
        cold.collection_page(
            &d,
            &s,
            JournalCollectionPageRequest {
                kind: JournalCollectionPageKind::Members,
                fence,
                after: Some(continuation),
                max_items: 1,
            }
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Conflict
    );
    let mut after = None;
    let mut ids = Vec::new();
    for call in 0..100 {
        let page = cold
            .collection_page(
                &d,
                &s,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after,
                    max_items: 3,
                },
            )
            .await
            .unwrap();
        let JournalCollectionItems::Members(batch) = page.items else {
            panic!("member page");
        };
        assert!(batch.len() <= 3);
        ids.extend(batch);
        after = page.next;
        if after.is_none() {
            break;
        }
        assert!(call < 99);
    }
    ids.sort();
    assert_eq!(ids, expected, "repacked page sequence is exact and unique");
}

#[tokio::test]
async fn bounded_collection_split_and_atomic_apply() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("bounded-apply");
    let first = equal_objects("one", 8);
    let second = equal_objects("two", 8);
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), first.clone()))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, second.clone()).await;
    let mut w = edit(&raw, &d, &s).await;
    w.kind = JournalWriteKind::Collect;
    w.release = first.iter().chain(&second).map(|o| o.id.clone()).collect();
    let mut plan = JournalCollectionPlan {
        write: w,
        max_units: 1,
    };
    let record = raw.load(&d, &s).await.unwrap();
    let packs = pack_rows(&f, &d, &s).await;
    let members = raw.members(&d, &s).await.unwrap();
    assert!(
        matches!(raw.prepare_collection_apply(&plan).await.unwrap(), JournalCollectionPreflight::Split { max_release } if max_release > 0)
    );
    assert_eq!(
        raw.apply_collection(plan.clone()).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    assert_eq!(pack_rows(&f, &d, &s).await, packs);
    assert_eq!(raw.members(&d, &s).await.unwrap(), members);
    plan.max_units = 32;
    let JournalCollectionPreflight::Fits { units } =
        raw.prepare_collection_apply(&plan).await.unwrap()
    else {
        panic!("fits");
    };
    assert_eq!(units, 2);
    for &cut in JournalCommitFailurePoint::ALL {
        if cut == JournalCommitFailurePoint::AfterCommit {
            continue;
        }
        let mut failed = plan.clone();
        failed.write.inject = Some(cut);
        raw.apply_collection(failed).await.unwrap_err();
        assert_eq!(raw.load(&d, &s).await.unwrap(), record);
        assert_eq!(raw.members(&d, &s).await.unwrap(), members);
        assert_eq!(pack_rows(&f, &d, &s).await, packs);
    }
    let receipt = raw.apply_collection(plan.clone()).await.unwrap();
    assert_eq!(receipt.rewritten_units, 2);
    assert_eq!(receipt.receipt.reclaimed_objects, 16);
    assert_eq!(
        receipt.receipt.reclaimed_bytes,
        first
            .iter()
            .chain(&second)
            .map(|o| o.bytes.len() as u64)
            .sum::<u64>()
    );
    assert!(raw.members(&d, &s).await.unwrap().is_empty());
    assert_eq!(
        raw.apply_collection(plan).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn bounded_collection_rechecks_fence_after_preflight() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("changed-after-preflight");
    let objects = equal_objects("preflight", 8);
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), objects.clone()))
        .await
        .unwrap();
    let mut w = edit(&raw, &d, &s).await;
    w.kind = JournalWriteKind::Collect;
    w.release = ids(&objects[..1]);
    let plan = JournalCollectionPlan {
        write: w,
        max_units: 32,
    };
    assert!(matches!(
        raw.prepare_collection_apply(&plan).await.unwrap(),
        JournalCollectionPreflight::Fits { .. }
    ));
    add_objects(&raw, &d, &s, equal_objects("racing-writer", 2)).await;
    let record = raw.load(&d, &s).await.unwrap();
    let members = raw.members(&d, &s).await.unwrap();
    let bodies = raw.objects(&d, &s, &members).await.unwrap();
    let packs = pack_rows(&f, &d, &s).await;
    assert_eq!(
        raw.apply_collection(plan).await.unwrap_err().kind,
        ErrorKind::Conflict
    );
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    assert_eq!(raw.members(&d, &s).await.unwrap(), members);
    assert_eq!(raw.objects(&d, &s, &members).await.unwrap(), bodies);
    assert_eq!(pack_rows(&f, &d, &s).await, packs);
}

#[tokio::test]
async fn bounded_member_continuations_reject_all_layout_writers() {
    for writer in [
        "purge-rewrite",
        "other-repack",
        "delete",
        "orphan-drain",
        "insert",
    ] {
        let f = FileDb::new().await.unwrap();
        let raw = storage(&f).await;
        let p = domain(JournalRole::Protected);
        let o = domain(JournalRole::Ordinary);
        let s = SessionId::new(format!("layout-{writer}"));
        let first = equal_objects("layout-first", 8);
        let second = equal_objects("layout-second", 8);
        let extra = equal_objects("layout-extra", 4);
        let mapped = MappingStorage::new(raw.clone(), f.clone());
        mapped
            .apply(write(&p, s.as_str(), first.clone()))
            .await
            .unwrap();
        add_objects(&raw, &p, &s, second.clone()).await;
        mapped
            .apply(write(
                &o,
                s.as_str(),
                first.iter().chain(&second).cloned().collect(),
            ))
            .await
            .unwrap();
        if writer == "purge-rewrite" {
            let mut w = edit(&raw, &p, &s).await;
            w.release = ids(&first[..2]);
            raw.apply(w).await.unwrap();
        }
        if matches!(writer, "delete" | "orphan-drain") {
            add_objects(&raw, &o, &s, extra.clone()).await;
        }
        let fence = raw.load(&p, &s).await.unwrap().unwrap().fence();
        let members = raw.members(&p, &s).await.unwrap();
        let bodies = raw.objects(&p, &s, &members).await.unwrap();
        let first_page = raw
            .collection_page(
                &p,
                &s,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after: None,
                    max_items: 1,
                },
            )
            .await
            .unwrap();
        assert!(first_page.next.is_some());
        match writer {
            "purge-rewrite" => {
                let mut w = edit(&raw, &o, &s).await;
                w.release = ids(&first[..2]);
                assert_eq!(raw.apply(w).await.unwrap().reclaimed_objects, 2);
            }
            "other-repack" => raw.post_terminal(&o, &s).await.unwrap(),
            "delete" => {
                let mut w = edit(&raw, &o, &s).await;
                w.release = ids(&extra);
                assert_eq!(raw.apply(w).await.unwrap().reclaimed_objects, 4);
            }
            "orphan-drain" => {
                // Same membership cascade used by owner deletion, scoped to
                // the ordinary record so protected readers remain present.
                sqlx::query("DELETE FROM agent_session_journal_session WHERE runtime_session_id=? AND role='ordinary'")
                    .bind(s.as_str()).execute(f.database().pool()).await.unwrap();
                assert_eq!(
                    f.database()
                        .drain_session_journal_orphans(0)
                        .await
                        .unwrap()
                        .deleted_packs,
                    1
                );
            }
            "insert" => add_objects(&raw, &o, &s, extra).await,
            _ => unreachable!(),
        }
        assert_eq!(raw.load(&p, &s).await.unwrap().unwrap().fence(), fence);
        let continuation = raw
            .collection_page(
                &p,
                &s,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Members,
                    fence,
                    after: first_page.next,
                    max_items: 1,
                },
            )
            .await;
        if writer == "insert" {
            assert!(
                continuation.is_ok(),
                "monotonic insertion preserves existing positions"
            );
        } else {
            assert_eq!(
                continuation.unwrap_err().kind,
                ErrorKind::Conflict,
                "{writer}"
            );
        }
        assert_eq!(raw.members(&p, &s).await.unwrap(), members, "{writer}");
        assert_eq!(
            raw.objects(&p, &s, &members).await.unwrap(),
            bodies,
            "{writer}"
        );
    }
}
