use super::*;

#[tokio::test]
async fn deferred_purge_threshold_and_receipts() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("threshold");
    let objects = equal_objects("equal", 8);
    let length = objects[0].bytes.len() as u64;
    assert!(objects.iter().all(|o| o.bytes.len() as u64 == length));
    mapped
        .apply(write(&d, s.as_str(), objects.clone()))
        .await
        .unwrap();
    let initial = pack_rows(&f, &d, &s).await;
    let ids: Vec<_> = objects.iter().map(|o| o.id.clone()).collect();
    assert!(
        raw.objects(&d, &s, &ids)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
    let mut release = edit(&raw, &d, &s).await;
    release.kind = JournalWriteKind::Collect;
    release.release = vec![ids[0].clone(), ids[0].clone()];
    assert_eq!(
        raw.apply(release).await.unwrap(),
        JournalApplyReceipt::default()
    );
    assert_eq!(
        pack_rows(&f, &d, &s).await,
        initial,
        "below 25% body units must stay sealed"
    );
    assert!(raw.objects(&d, &s, &ids[..1]).await.unwrap()[0].is_none());
    let independent = reopen(&f).await;
    assert!(independent.objects(&d, &s, &ids[..1]).await.unwrap()[0].is_none());
    assert_eq!(raw.retained_bytes(&d).await.unwrap(), Some(length * 8));
    let mut admit = edit(&independent, &d, &s).await;
    admit.objects.objects = vec![objects[0].clone()];
    assert_eq!(
        independent.apply(admit).await.unwrap(),
        JournalApplyReceipt::default()
    );
    assert_eq!(
        pack_rows(&f, &d, &s).await,
        initial,
        "same-scope re-admission must reuse the retained slot"
    );
    assert!(raw.objects(&d, &s, &ids[..1]).await.unwrap()[0].is_some());
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![ids[0].clone()];
    raw.apply(release).await.unwrap();
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![ids[0].clone(), ids[1].clone(), ids[1].clone()];
    let receipt = raw.apply(release).await.unwrap();
    assert_eq!(
        receipt,
        JournalApplyReceipt {
            reclaimed_objects: 2,
            reclaimed_bytes: length * 2
        }
    );
    let purged = pack_rows(&f, &d, &s).await;
    assert_eq!(purged.len(), 1);
    assert_eq!(purged[0].0, initial[0].0);
    assert_eq!(purged[0].1, 1u64.to_be_bytes());
    assert_eq!(purged[0].2, 6);
    assert_ne!(purged[0].4, initial[0].4);
    assert_ne!(purged[0].6, initial[0].6);
    assert_eq!(raw.retained_bytes(&d).await.unwrap(), Some(length * 6));
    let result = independent.objects(&d, &s, &ids).await.unwrap();
    assert!(result[..2].iter().all(Option::is_none));
    for (got, o) in result[2..].iter().zip(&objects[2..]) {
        assert_eq!(got.as_ref(), Some(&o.bytes));
    }
    let mut repeat = edit(&raw, &d, &s).await;
    repeat.release = ids[..2].to_vec();
    assert_eq!(
        raw.apply(repeat).await.unwrap(),
        JournalApplyReceipt::default()
    );
    // Canonical byte share, not slot share, determines the purge threshold.
    let s = SessionId::new("unequal");
    let mut unequal = equal_objects("small", 7);
    unequal.push(object("large".repeat(2000)));
    mapped
        .apply(write(&d, s.as_str(), unequal.clone()))
        .await
        .unwrap();
    let sealed = pack_rows(&f, &d, &s).await;
    let mut release = edit(&raw, &d, &s).await;
    release.release = unequal[..3].iter().map(|o| o.id.clone()).collect();
    assert_eq!(
        raw.apply(release).await.unwrap(),
        JournalApplyReceipt::default()
    );
    assert_eq!(pack_rows(&f, &d, &s).await, sealed);
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![unequal[7].id.clone()];
    assert_eq!(
        raw.apply(release).await.unwrap(),
        JournalApplyReceipt {
            reclaimed_objects: 4,
            reclaimed_bytes: unequal[..3]
                .iter()
                .chain(&unequal[7..])
                .map(|o| o.bytes.len() as u64)
                .sum()
        }
    );
}

#[tokio::test]
async fn retired_and_empty_scope_force_purge() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    for retire in [false, true] {
        let s = SessionId::new(if retire { "retired" } else { "empty" });
        let first = equal_objects("first", 8);
        let second = equal_objects("second", 8);
        mapped
            .apply(write(&d, s.as_str(), first.clone()))
            .await
            .unwrap();
        let mut add = edit(&raw, &d, &s).await;
        add.objects.objects = second.clone();
        add.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
        raw.apply(add).await.unwrap();
        for o in [&first[0], &second[0]] {
            let mut release = edit(&raw, &d, &s).await;
            release.release = vec![o.id.clone()];
            assert_eq!(
                raw.apply(release).await.unwrap(),
                JournalApplyReceipt::default()
            );
        }
        let before = raw.retained_bytes(&d).await.unwrap().unwrap();
        let mut release = edit(&raw, &d, &s).await;
        release.kind = JournalWriteKind::Collect;
        if retire {
            release.record.head = None;
            release.record.has_checkpoint = false;
            release.record.retirement = Some(JournalRetirement {
                session: s.clone(),
                revision: release.record.revision,
                root_epoch: 1,
                retired_at: Timestamp(100),
            });
            // No new release ids: both old deferred slots still must be found.
        } else {
            release.release = first[1..]
                .iter()
                .chain(&second[1..])
                .map(|o| o.id.clone())
                .collect();
        }
        let receipt = raw.apply(release.clone()).await.unwrap();
        let removed: Vec<_> = if retire {
            vec![&first[0], &second[0]]
        } else {
            first.iter().chain(&second).collect()
        };
        assert_eq!(receipt.reclaimed_objects, removed.len() as u64);
        assert_eq!(
            receipt.reclaimed_bytes,
            removed.iter().map(|o| o.bytes.len() as u64).sum::<u64>()
        );
        assert_eq!(
            before - raw.retained_bytes(&d).await.unwrap().unwrap(),
            receipt.reclaimed_bytes
        );
        if retire {
            assert_eq!(raw.members(&d, &s).await.unwrap().len(), 14);
        } else {
            assert!(pack_rows(&f, &d, &s).await.is_empty());
        }
        release.expect = release.record.fence();
        release.record.generation += 1;
        release.release.clear();
        assert_eq!(
            raw.apply(release).await.unwrap(),
            JournalApplyReceipt::default()
        );
    }
}

#[tokio::test]
async fn shared_domain_copy_and_independent_session_receipts() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let protected = domain(JournalRole::Protected);
    let ordinary = domain(JournalRole::Ordinary);
    let objects = equal_objects("shared", 8);
    let length: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    let a = SessionId::new("a");
    let b = SessionId::new("b");
    let scopes = [
        (&protected, &a),
        (&ordinary, &a),
        (&protected, &b),
        (&ordinary, &b),
    ];
    for (d, s) in scopes {
        let mut initial = write(d, s.as_str(), objects.clone());
        initial.record.collection_dirty = false;
        mapped.apply(initial).await.unwrap();
        raw.objects(d, s, &[objects[0].id.clone()]).await.unwrap();
    }
    let physical = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT coalesce(sum(retained_canonical_bytes),0) FROM agent_session_journal_pack",
        )
        .fetch_one(f.database().pool())
        .await
        .unwrap() as u64
    };
    // One physical copy per session, shared by both of its domains; the
    // sessions never share. The old layout stored four copies here.
    for s in [&a, &b] {
        let packs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id=?",
        )
        .bind(s.as_str())
        .fetch_one(f.database().pool())
        .await
        .unwrap();
        assert_eq!(packs, 1, "{s:?} stores one shared copy");
    }
    assert_eq!(physical().await, length * 2);
    assert_eq!(
        raw.retained_bytes(&protected).await.unwrap(),
        Some(length * 2)
    );
    assert_eq!(
        raw.retained_bytes(&ordinary).await.unwrap(),
        Some(length * 2)
    );
    let mut totals = JournalApplyReceipt::default();
    for (n, (d, s)) in scopes.into_iter().enumerate() {
        let untouched: Vec<_> = scopes[n + 1..].to_vec();
        let mut release = edit(&raw, d, s).await;
        release.release = objects
            .iter()
            .map(|o| o.id.clone())
            .chain(std::iter::once(objects[0].id.clone()))
            .collect();
        let r = raw.apply(release).await.unwrap();
        // The first domain's release leaves the other domain holding every
        // slot: nothing is removed. The second domain's release removes the
        // session's one copy, counted once.
        let removed = n % 2 == 1;
        assert_eq!(
            r,
            JournalApplyReceipt {
                reclaimed_objects: if removed { 8 } else { 0 },
                reclaimed_bytes: if removed { length } else { 0 },
            }
        );
        totals.reclaimed_objects += r.reclaimed_objects;
        totals.reclaimed_bytes += r.reclaimed_bytes;
        assert!(raw.objects(d, s, &[objects[0].id.clone()]).await.unwrap()[0].is_none());
        for (d, s) in untouched {
            assert_eq!(
                raw.objects(d, s, &[objects[0].id.clone()]).await.unwrap()[0].as_ref(),
                Some(&objects[0].bytes)
            );
        }
        // Receipts equal physical removal; a released-but-held copy remains.
        assert_eq!(length * 2 - physical().await, totals.reclaimed_bytes);
        let reopened = reopen(&f).await;
        for (d, s) in scopes[n + 1..].iter() {
            assert_eq!(
                reopened
                    .objects(d, s, &[objects[7].id.clone()])
                    .await
                    .unwrap()[0]
                    .as_ref(),
                Some(&objects[7].bytes)
            );
        }
    }
    assert_eq!(totals.reclaimed_objects, 16);
    // Engine-selected nonempty collection release now uses the physical path.
    let s = SessionId::new("engine-release");
    let engine = JournalEngine::protected(
        Arc::new(mapped),
        protected.storage_id.clone(),
        engine_config(),
    );
    let writer = engine
        .open(
            &s,
            open_request(
                "writer",
                JournalRootSelection::Latest,
                JournalOpenIntent::ExclusiveWriter,
            ),
        )
        .await
        .unwrap();
    let first = engine
        .commit(
            &writer,
            empty_commit(&s, writer.writer_epoch().unwrap(), "one", None, 1),
        )
        .await
        .unwrap();
    engine
        .commit(
            &writer,
            empty_commit(&s, writer.writer_epoch().unwrap(), "two", Some(&first), 2),
        )
        .await
        .unwrap();
    engine.close(writer).await.unwrap();
    let gc = drain_gc(
        &engine,
        JournalGcRequest {
            storage_id: engine.storage_id(),
            expected_root_epoch: engine.root_epoch().unwrap(),
            max_objects: u64::MAX,
            work_budget: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(gc.deleted > 0);
}

#[tokio::test]
async fn bounded_retired_purge_keeps_dirty_until_drained() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let d = domain(JournalRole::Protected);
    let other = domain(JournalRole::Ordinary);
    let s = SessionId::new("bounded-retired-purge");
    let first = equal_objects("first", 8);
    let second = equal_objects("second", 8);
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), first.clone()))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, second.clone()).await;
    let other_objects: Vec<_> = first[4..].iter().chain(&second[4..]).cloned().collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&other, s.as_str(), other_objects.clone()))
        .await
        .unwrap();
    let other_before = raw.objects(&other, &s, &ids(&other_objects)).await.unwrap();
    assert_eq!(
        other_before,
        other_objects
            .iter()
            .map(|o| Some(o.bytes.clone()))
            .collect::<Vec<_>>()
    );
    for o in [&first[0], &second[0]] {
        let mut w = edit(&raw, &d, &s).await;
        w.release = vec![o.id.clone()];
        assert_eq!(raw.apply(w).await.unwrap(), JournalApplyReceipt::default());
    }
    let members = raw.members(&d, &s).await.unwrap();
    let protected_before = raw.objects(&d, &s, &members).await.unwrap();
    let mut total = 0;
    for call in 0..2 {
        let mut w = edit(&raw, &d, &s).await;
        w.kind = JournalWriteKind::Collect;
        w.record.head = None;
        w.record.has_checkpoint = false;
        w.record.retirement = Some(JournalRetirement {
            session: s.clone(),
            revision: w.record.revision,
            root_epoch: 1,
            retired_at: Timestamp(100),
        });
        w.record.collection_dirty = false;
        let plan = JournalCollectionPlan {
            write: w,
            max_units: 1,
        };
        assert_eq!(
            raw.prepare_collection_apply(&plan).await.unwrap(),
            JournalCollectionPreflight::Fits { units: 1 }
        );
        let receipt = raw.apply_collection(plan).await.unwrap();
        assert_eq!(receipt.rewritten_units, 1);
        total += receipt.receipt.reclaimed_objects;
        assert_eq!(
            raw.load(&d, &s).await.unwrap().unwrap().collection_dirty,
            call == 0
        );
        assert_eq!(raw.members(&d, &s).await.unwrap(), members);
        assert_eq!(
            raw.objects(&d, &s, &members).await.unwrap(),
            protected_before
        );
        assert_eq!(
            raw.objects(&other, &s, &ids(&other_objects)).await.unwrap(),
            other_before
        );
        let reopened = reopen(&f).await;
        assert_eq!(
            reopened.objects(&d, &s, &members).await.unwrap(),
            protected_before
        );
        assert_eq!(
            reopened
                .objects(&other, &s, &ids(&other_objects))
                .await
                .unwrap(),
            other_before
        );
    }
    assert_eq!(total, 2, "each earlier deferred copy is counted once");
}

#[tokio::test]
async fn bounded_empty_domain_forces_deferred_purge_without_retirement() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let p = domain(JournalRole::Protected);
    let o = domain(JournalRole::Ordinary);
    let s = SessionId::new("empty-domain-forced-purge");
    let first = equal_objects("empty-first", 8);
    let second = equal_objects("empty-second", 8);
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    mapped.apply(write(&p, s.as_str(), vec![])).await.unwrap();
    mapped
        .apply(write(&o, s.as_str(), first.clone()))
        .await
        .unwrap();
    add_objects(&raw, &o, &s, second.clone()).await;
    for object in [&first[0], &second[0]] {
        let mut w = edit(&raw, &o, &s).await;
        w.release = vec![object.id.clone()];
        assert_eq!(raw.apply(w).await.unwrap(), JournalApplyReceipt::default());
    }
    let members = raw.members(&o, &s).await.unwrap();
    let bodies = raw.objects(&o, &s, &members).await.unwrap();
    let mut reclaimed = 0;
    for call in 0..2 {
        let mut w = edit(&raw, &p, &s).await;
        w.kind = JournalWriteKind::Collect;
        w.record.collection_dirty = false;
        assert!(w.record.retirement.is_none());
        assert!(raw.members(&p, &s).await.unwrap().is_empty());
        let receipt = raw
            .apply_collection(JournalCollectionPlan {
                write: w,
                max_units: 1,
            })
            .await
            .unwrap();
        assert_eq!(receipt.rewritten_units, 1);
        reclaimed += receipt.receipt.reclaimed_objects;
        assert_eq!(
            raw.load(&p, &s).await.unwrap().unwrap().collection_dirty,
            call == 0
        );
        assert_eq!(raw.objects(&o, &s, &members).await.unwrap(), bodies);
        assert_eq!(
            reopen(&f).await.objects(&o, &s, &members).await.unwrap(),
            bodies
        );
    }
    assert_eq!(reclaimed, 2);
}

#[tokio::test]
async fn deferred_forced_purge_dirty_hint_survives_engine_write() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    raw.set_repacking(false);
    let p = domain(JournalRole::Protected);
    let o = domain(JournalRole::Ordinary);
    let s = SessionId::new("dirty-hint-survives");
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let mut empty = write(&p, s.as_str(), vec![]);
    empty.record.head = None;
    empty.pins.clear();
    empty.catalog.clear();
    empty.operation = None;
    mapped.apply(empty).await.unwrap();
    let first = equal_objects("dirty-first", 8);
    let second = equal_objects("dirty-second", 8);
    mapped
        .apply(write(&o, s.as_str(), first.clone()))
        .await
        .unwrap();
    add_objects(&raw, &o, &s, second.clone()).await;
    for object in [&first[0], &second[0]] {
        let mut w = edit(&raw, &o, &s).await;
        w.release = vec![object.id.clone()];
        assert_eq!(raw.apply(w).await.unwrap(), JournalApplyReceipt::default());
    }
    let collect = |mut w: JournalWrite| {
        w.kind = JournalWriteKind::Collect;
        w.record.collection_dirty = false;
        JournalCollectionPlan {
            write: w,
            max_units: 1,
        }
    };
    let receipt = raw
        .apply_collection(collect(edit(&raw, &p, &s).await))
        .await
        .unwrap();
    assert_eq!(receipt.receipt.reclaimed_objects, 1);
    assert!(raw.load(&p, &s).await.unwrap().unwrap().collection_dirty);
    let mut later = edit(&raw, &p, &s).await;
    assert!(raw.members(&p, &s).await.unwrap().is_empty());
    assert!(later.record.retirement.is_none());
    assert!(later.record.head.is_none());
    assert!(raw.pins(&p, &s).await.unwrap().is_empty());
    // An admission without a retained root stops this write from forcing an
    // empty-bitmap purge. The engine still has no live roots in this domain.
    let unrooted = object("unrooted later admission");
    later.objects.objects = vec![unrooted.clone()];
    later.record.collection_dirty = false;
    raw.apply(later).await.unwrap();
    assert_eq!(
        raw.dirty_sessions(&p, None, 10).await.unwrap(),
        vec![s.clone()],
        "a later empty-domain write must preserve the unserviced purge hint"
    );
    let cold = reopen(&f).await;
    assert_eq!(
        cold.dirty_sessions(&p, None, 10).await.unwrap(),
        vec![s.clone()]
    );
    let mut next = collect(edit(&cold, &p, &s).await);
    next.max_units = 2;
    next.write.release = vec![unrooted.id];
    let receipt = cold.apply_collection(next).await.unwrap();
    assert_eq!(receipt.receipt.reclaimed_objects, 2);
    assert_eq!(
        receipt.receipt.reclaimed_bytes,
        (second[0].bytes.len() + unrooted.bytes.len()) as u64
    );
    assert!(cold.dirty_sessions(&p, None, 10).await.unwrap().is_empty());
    let kept: Vec<_> = first
        .iter()
        .skip(1)
        .chain(second.iter().skip(1))
        .cloned()
        .collect();
    let members = cold.members(&o, &s).await.unwrap();
    assert_eq!(members.len(), kept.len());
    assert!(
        cold.objects(&o, &s, &members)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
}
