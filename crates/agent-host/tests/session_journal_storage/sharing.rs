use super::*;

// ---- Within-session body sharing (spec amendment 2026-10-21, task 27) ----

#[tokio::test]
async fn ordinary_never_reads_protected_only_bodies() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let (p, o) = (
        domain(JournalRole::Protected),
        domain(JournalRole::Ordinary),
    );
    let s = SessionId::new("isolated");
    let shared = equal_objects("projected", 8);
    let secrets = equal_objects("exact-only", 4);
    let ordinary_only = equal_objects("ordinary-only", 1);
    let terminal_only = equal_objects("terminal-protected", 1);
    let protected_all: Vec<_> = shared.iter().chain(&secrets).cloned().collect();
    let ordinary_all: Vec<_> = shared.iter().chain(&ordinary_only).cloned().collect();
    mapped
        .apply(write(&p, s.as_str(), protected_all.clone()))
        .await
        .unwrap();
    mapped
        .apply(write(&o, s.as_str(), ordinary_all.clone()))
        .await
        .unwrap();
    // Shared bodies are stored once: 12 protected + 1 ordinary-only slots.
    // Independent domain copies stored 20.
    assert_eq!(session_object_count(&f, &s).await, 13);
    let check = |raw: Arc<ForgeJournalStorage>,
                 stage: &'static str,
                 protected_all: Vec<JournalStoredObject>| {
        let (p, o, s) = (p.clone(), o.clone(), s.clone());
        let secrets: Vec<_> = secrets.iter().chain(&terminal_only).cloned().collect();
        let (ordinary_only, shared, ordinary_all) =
            (ordinary_only.clone(), shared.clone(), ordinary_all.clone());
        async move {
            // Warm protected bodies first, including on the reopened storage.
            for (domain, expected) in [(&p, &protected_all), (&o, &ordinary_all)] {
                assert_eq!(
                    raw.members(domain, &s).await.unwrap(),
                    sorted_ids(expected),
                    "{stage}: complete {:?} membership",
                    domain.role
                );
                assert_eq!(
                    raw.objects(domain, &s, &ids(expected)).await.unwrap(),
                    expected
                        .iter()
                        .map(|o| Some(o.bytes.clone()))
                        .collect::<Vec<_>>(),
                    "{stage}: exact {:?} bytes",
                    domain.role
                );
            }
            assert!(
                raw.objects(&o, &s, &ids(&secrets))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_none),
                "{stage}: ordinary read a protected-only body"
            );
            assert!(
                raw.objects(&p, &s, &ids(&ordinary_only))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_none),
                "{stage}: protected read an ordinary-only body"
            );
            assert!(
                raw.objects(&o, &s, &ids(&shared))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_some),
                "{stage}"
            );
        }
    };
    check(raw.clone(), "fresh admission", protected_all).await;
    // Purge rewrite: three dead secret slots exceed 25% of the shared pack,
    // which is rewritten with both domains' bitmaps remapped.
    let before: Vec<i64> = pack_rows(&f, &p, &s).await.iter().map(|r| r.0).collect();
    let mut release = edit(&raw, &p, &s).await;
    release.release = ids(&secrets[..3]);
    let receipt = raw.apply(release).await.unwrap();
    assert_eq!(receipt.reclaimed_objects, 3);
    let purged = pack_rows(&f, &p, &s).await;
    assert!(
        purged
            .iter()
            .any(|r| before.contains(&r.0) && r.1 == 1u64.to_be_bytes()),
        "the shared pack was rewritten in place"
    );
    let cached = raw.cached_body_packs(&s).unwrap();
    for row in &purged {
        assert!(
            cached.contains(&(
                row.0,
                u64::from_be_bytes(row.1.as_slice().try_into().unwrap())
            )),
            "an in-place rewrite must publish its surviving generation"
        );
        assert!(
            !cached.contains(&(row.0, 0)),
            "the old generation was invalidated"
        );
    }
    let mut protected_live: Vec<_> = shared.iter().chain(&secrets[3..]).cloned().collect();
    check(raw.clone(), "after purge rewrite", protected_live.clone()).await;
    // Base repack: the shared pack still holds protected-only slots, so
    // ordinary's collection leaves it and the ordinary-only pack in place
    // (merges carrying both domains' bits:
    // `maintenance_moves_only_packs_the_domain_fully_holds`).
    let unmerged: Vec<i64> = pack_rows(&f, &o, &s).await.iter().map(|r| r.0).collect();
    assert_eq!(unmerged.len(), 2);
    let mut collect = edit(&raw, &o, &s).await;
    collect.kind = JournalWriteKind::Collect;
    raw.apply(collect).await.unwrap();
    let merged: Vec<i64> = pack_rows(&f, &o, &s).await.iter().map(|r| r.0).collect();
    assert_eq!(
        merged, unmerged,
        "ordinary's repack never moves protected-only slots"
    );
    assert!(before.contains(&merged[0]));
    check(raw.clone(), "after collection", protected_live.clone()).await;
    // A second, protected-only pack shares the mixed pack's eligible S2 tier.
    add_objects(&raw, &p, &s, terminal_only.clone()).await;
    protected_live.extend(terminal_only.clone());
    let terminal_before = pack_rows(&f, &p, &s).await;
    assert_eq!(
        terminal_before.len(),
        2,
        "terminal maintenance must start with two eligible packs"
    );
    assert_eq!(terminal_before[0].0, merged[0]);
    assert_eq!(
        (terminal_before[0].3 as u64).max(4096).ilog2(),
        (terminal_before[1].3 as u64).max(4096).ilog2(),
        "both packs must occupy the same geometric tier"
    );
    assert!(terminal_before.iter().map(|r| r.3).sum::<i64>() <= 1024 * 1024);
    let bitmaps: Vec<(i64, String, Vec<u8>)> = sqlx::query_as(
        "SELECT pack_id,role,bitmap FROM agent_session_journal_membership WHERE runtime_session_id=? ORDER BY pack_id,role",
    )
    .bind(s.as_str())
    .fetch_all(f.database().pool())
    .await
    .unwrap();
    // The ordinary-only pack (merged[1]) keeps its own single row.
    assert_eq!(bitmaps.len(), 4);
    assert_eq!(
        (bitmaps[2].0, bitmaps[2].1.as_str()),
        (merged[1], "ordinary")
    );
    assert_eq!(
        (bitmaps[0].0, bitmaps[0].1.as_str()),
        (merged[0], "ordinary")
    );
    assert_eq!(
        (bitmaps[1].0, bitmaps[1].1.as_str()),
        (merged[0], "protected")
    );
    assert_ne!(
        bitmaps[0].2, bitmaps[1].2,
        "the mixed pack has different domain bitmaps"
    );
    assert_eq!(
        bitmaps[3],
        (terminal_before[1].0, "protected".to_owned(), vec![1])
    );
    check(
        raw.clone(),
        "before terminal maintenance",
        protected_live.clone(),
    )
    .await;
    raw.post_terminal(&p, &s).await.unwrap();
    let terminal_after = pack_rows(&f, &p, &s).await;
    assert_eq!(terminal_after.len(), 1, "S2 must physically merge the pair");
    assert!(
        terminal_after
            .iter()
            .all(|after| terminal_before.iter().all(|before| before.0 != after.0)),
        "S2 must replace the input pack ids"
    );
    assert_ne!(
        terminal_after, terminal_before,
        "S2 must change the physical layout"
    );
    assert_eq!(
        pack_rows(&f, &o, &s)
            .await
            .iter()
            .map(|r| r.0)
            .filter(|id| *id == merged[1])
            .count(),
        1,
        "protected's S2 leaves the ordinary-only pack"
    );
    check(
        raw.clone(),
        "after terminal maintenance",
        protected_live.clone(),
    )
    .await;
    let reopened = reopen(&f).await;
    check(reopened.clone(), "after reopen", protected_live).await;
    assert!(reopened.objects(&o, &s, &ids(&secrets[3..])).await.unwrap()[0].is_none());
    assert!(reopened.objects(&p, &s, &ids(&secrets[3..])).await.unwrap()[0].is_some());
}

/// A domain's terminal hook and base repack move only packs that domain holds
/// entirely. A pack with a slot only the other domain holds is left to that
/// domain, so one domain never pairs the other's stable bodies with its own
/// short-lived ones (the B/N=1000 delta-write regression after sharing).
#[tokio::test]
async fn maintenance_moves_only_packs_the_domain_fully_holds() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let (p, o) = (
        domain(JournalRole::Protected),
        domain(JournalRole::Ordinary),
    );
    let s = SessionId::new("fully-held");
    let shared = equal_objects("shared", 8);
    let secrets = equal_objects("secret", 4);
    let protected_only = equal_objects("protected-only", 1);
    let ordinary_only = equal_objects("ordinary-only", 1);
    let extra = equal_objects("protected-extra", 1);
    // X: protected holds every slot, ordinary only the shared ones.
    mapped
        .apply(write(
            &p,
            s.as_str(),
            shared.iter().chain(&secrets).cloned().collect(),
        ))
        .await
        .unwrap();
    mapped
        .apply(write(&o, s.as_str(), shared.clone()))
        .await
        .unwrap();
    let [x] = session_pack_ids(&f, &s).await[..] else {
        panic!("ordinary admitted only shared bodies")
    };
    // Y: protected-only. Z: ordinary-only. All three share one S2 tier.
    add_objects(&raw, &p, &s, protected_only.clone()).await;
    add_objects(&raw, &o, &s, ordinary_only.clone()).await;
    let [_, y, z] = session_pack_ids(&f, &s).await[..] else {
        panic!("one new pack per domain")
    };
    let tiers: std::collections::BTreeSet<_> = sqlx::query_scalar::<_, i64>(
        "SELECT retained_canonical_bytes FROM agent_session_journal_pack WHERE runtime_session_id=?",
    )
    .bind(s.as_str())
    .fetch_all(f.database().pool())
    .await
    .unwrap()
    .into_iter()
    .map(|b| (b as u64).max(4096).ilog2())
    .collect();
    assert_eq!(tiers.len(), 1, "X, Y and Z must occupy one geometric tier");
    let protected_all: Vec<_> = shared
        .iter()
        .chain(&secrets)
        .chain(&protected_only)
        .cloned()
        .collect();
    let ordinary_all: Vec<_> = shared.iter().chain(&ordinary_only).cloned().collect();
    let check = |stage: &'static str, protected_all: Vec<JournalStoredObject>| {
        let (raw, p, o, s) = (raw.clone(), p.clone(), o.clone(), s.clone());
        let (secrets, ordinary_only, ordinary_all) =
            (secrets.clone(), ordinary_only.clone(), ordinary_all.clone());
        async move {
            for (domain, expected) in [(&p, &protected_all), (&o, &ordinary_all)] {
                assert_eq!(
                    raw.members(domain, &s).await.unwrap(),
                    sorted_ids(expected),
                    "{stage}: {:?} membership",
                    domain.role
                );
                assert_eq!(
                    raw.objects(domain, &s, &ids(expected)).await.unwrap(),
                    expected
                        .iter()
                        .map(|o| Some(o.bytes.clone()))
                        .collect::<Vec<_>>(),
                    "{stage}: {:?} bytes",
                    domain.role
                );
            }
            assert!(
                raw.objects(&o, &s, &ids(&secrets))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_none),
                "{stage}: ordinary read a protected-only body"
            );
            assert!(
                raw.objects(&p, &s, &ids(&ordinary_only))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_none),
                "{stage}: protected read an ordinary-only body"
            );
        }
    };
    check("before maintenance", protected_all.clone()).await;
    // Ordinary holds all of Z only: nothing to pair.
    raw.post_terminal(&o, &s).await.unwrap();
    assert_eq!(session_pack_ids(&f, &s).await, vec![x, y, z]);
    // Protected merges X and Y (carrying ordinary's X bits); Z is untouched.
    raw.post_terminal(&p, &s).await.unwrap();
    let after = session_pack_ids(&f, &s).await;
    assert_eq!(after.len(), 2, "S2 merged exactly the fully held pair");
    assert_eq!(after[0], z, "S2 never moves an ordinary-only pack");
    let xy = after[1];
    assert!(xy > z);
    check("after S2", protected_all.clone()).await;
    // Base repack: neither collection merges Z with the protected pack.
    for d in [&p, &o] {
        let mut collect = edit(&raw, d, &s).await;
        collect.kind = JournalWriteKind::Collect;
        raw.apply(collect).await.unwrap();
        assert_eq!(session_pack_ids(&f, &s).await, vec![z, xy], "{:?}", d.role);
    }
    for d in [&p, &o] {
        let mut collect = edit(&raw, d, &s).await;
        collect.kind = JournalWriteKind::Collect;
        raw.apply_collection(JournalCollectionPlan {
            write: collect,
            max_units: 32,
        })
        .await
        .unwrap();
        assert_eq!(
            session_pack_ids(&f, &s).await,
            vec![z, xy],
            "bounded {:?}",
            d.role
        );
    }
    // A second small protected pack is still merged by protected's repack.
    add_objects(&raw, &p, &s, extra.clone()).await;
    let mut collect = edit(&raw, &p, &s).await;
    collect.kind = JournalWriteKind::Collect;
    raw.apply(collect).await.unwrap();
    let repacked = session_pack_ids(&f, &s).await;
    assert_eq!(repacked.len(), 2);
    assert_eq!(repacked[0], z, "the base repack never moves Z");
    let mut protected_all = protected_all;
    protected_all.extend(extra);
    check("after base repack", protected_all.clone()).await;
    check_reopened(&f, &p, &o, &s, &protected_all, &ordinary_all).await;
}
async fn check_reopened(
    f: &FileDb,
    p: &JournalDomain,
    o: &JournalDomain,
    s: &SessionId,
    protected_all: &[JournalStoredObject],
    ordinary_all: &[JournalStoredObject],
) {
    let reopened = reopen(f).await;
    for (domain, expected) in [(p, protected_all), (o, ordinary_all)] {
        assert_eq!(
            reopened.members(domain, s).await.unwrap(),
            sorted_ids(expected)
        );
        assert_eq!(
            reopened.objects(domain, s, &ids(expected)).await.unwrap(),
            expected
                .iter()
                .map(|o| Some(o.bytes.clone()))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn two_domain_release_crash_cuts() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let (p, o) = (
        domain(JournalRole::Protected),
        domain(JournalRole::Ordinary),
    );
    let objects = equal_objects("cut", 8);
    let length: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    for (first_cut, &cut) in [true, false].into_iter().flat_map(|first| {
        JournalCommitFailurePoint::ALL
            .iter()
            .map(move |c| (first, c))
    }) {
        let s = SessionId::new(format!("cut-{first_cut}-{cut:?}"));
        for d in [&p, &o] {
            mapped
                .apply(write(d, s.as_str(), objects.clone()))
                .await
                .unwrap();
        }
        let physical = || async { session_object_count(&f, &s).await };
        assert_eq!(physical().await, 8);
        let release = |d: &JournalDomain| {
            let raw = raw.clone();
            let (d, s, objects) = (d.clone(), s.clone(), objects.clone());
            async move {
                let mut w = edit(&raw, &d, &s).await;
                w.kind = JournalWriteKind::Collect;
                w.release = ids(&objects);
                w
            }
        };
        if !first_cut {
            assert_eq!(
                raw.apply(release(&p).await).await.unwrap(),
                JournalApplyReceipt::default(),
                "the other domain still holds every slot"
            );
        }
        // Cut the first (non-removing) or second (removing) release.
        let target = if first_cut { &p } else { &o };
        let mut w = release(target).await;
        w.inject = Some(cut);
        let error = raw.apply(w.clone()).await.unwrap_err();
        assert!(error.retryable);
        let reopened = reopen(&f).await;
        let readable = |d: &JournalDomain| {
            let reopened = reopened.clone();
            let (d, s, objects) = (d.clone(), s.clone(), objects.clone());
            async move {
                reopened
                    .objects(&d, &s, &ids(&objects))
                    .await
                    .unwrap()
                    .iter()
                    .all(Option::is_some)
            }
        };
        if cut.is_after_commit() {
            assert!(!readable(target).await);
            assert_eq!(physical().await, if first_cut { 8 } else { 0 });
        } else {
            assert!(readable(target).await, "{cut:?} rolled back");
            assert_eq!(physical().await, 8, "{cut:?} removed nothing");
            // The rolled-back release retries from the durable fence.
            let receipt = reopened.apply(release(target).await).await.unwrap();
            assert_eq!(receipt.reclaimed_objects, if first_cut { 0 } else { 8 });
        }
        if first_cut {
            // The other domain was untouched by the cut release.
            assert!(readable(&o).await);
            let receipt = reopened.apply(release(&o).await).await.unwrap();
            assert_eq!(
                receipt,
                JournalApplyReceipt {
                    reclaimed_objects: 8,
                    reclaimed_bytes: length,
                }
            );
        }
        assert_eq!(physical().await, 0, "{first_cut} {cut:?}");
    }
}

#[tokio::test]
async fn deletion_drain_removes_shared_session_packs_once() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let (p, o) = (
        domain(JournalRole::Protected),
        domain(JournalRole::Ordinary),
    );
    let objects = equal_objects("deleted", 8);
    let length: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    let (x, y) = (SessionId::new("deleted-x"), SessionId::new("kept-y"));
    for s in [&x, &y] {
        for d in [&p, &o] {
            mapped
                .apply(write(d, s.as_str(), objects.clone()))
                .await
                .unwrap();
        }
    }
    let host = f.ensure_session(&x).await.unwrap();
    sqlx::query("DELETE FROM agent_session WHERE id=?")
        .bind(host)
        .execute(f.database().pool())
        .await
        .unwrap();
    // Both domains' memberships cascaded; the one shared pack is orphaned.
    let db = f.database();
    let mut deleted = (0, 0, 0);
    let mut after = 0;
    loop {
        let batch = db.drain_session_journal_orphans(after).await.unwrap();
        deleted.0 += batch.deleted_packs;
        deleted.1 += batch.deleted_objects;
        deleted.2 += batch.deleted_canonical_bytes;
        after = batch.after_pack_id;
        if !batch.has_more {
            break;
        }
    }
    assert_eq!(deleted, (1, 8, length), "one shared copy, counted once");
    assert_eq!(session_object_count(&f, &x).await, 0);
    let reopened = reopen(&f).await;
    for d in [&p, &o] {
        assert!(
            reopened
                .objects(d, &y, &ids(&objects))
                .await
                .unwrap()
                .iter()
                .all(Option::is_some)
        );
        assert!(reopened.members(d, &x).await.unwrap().is_empty());
    }
}
