use super::*;

#[tokio::test]
async fn terminal_geometric_repack() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("geometric");
    let objects: Vec<_> = (0..24)
        .map(|i| object(format!("{i:03}-{}", "x".repeat(1500))))
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), vec![objects[0].clone()]))
        .await
        .unwrap();
    for o in &objects[1..] {
        add_objects(&raw, &d, &s, vec![o.clone()]).await;
    }
    let record = raw.load(&d, &s).await.unwrap();
    let members = raw.members(&d, &s).await.unwrap();
    let retained = raw.retained_bytes(&d).await.unwrap();
    let before = pack_rows(&f, &d, &s).await;
    assert_eq!(before.len(), 24);
    raw.set_repacking(false);
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(pack_rows(&f, &d, &s).await, before);
    raw.set_repacking(true);
    raw.post_terminal(&d, &s).await.unwrap();
    let after = pack_rows(&f, &d, &s).await;
    assert!(after.len() <= 2);
    assert_eq!(
        raw.cached_body_packs(&s).unwrap(),
        after
            .iter()
            .map(|r| (r.0, u64::from_be_bytes(r.1.as_slice().try_into().unwrap())))
            .collect::<Vec<_>>(),
        "only final SQL pack generations may remain cached after multi-merge S2"
    );
    let mut tiers = std::collections::BTreeSet::new();
    for row in &after {
        assert!(row.3 <= 1024 * 1024);
        assert!(tiers.insert((row.3 as u64).max(4096).ilog2()));
    }
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    assert_eq!(raw.members(&d, &s).await.unwrap(), members);
    assert_eq!(raw.retained_bytes(&d).await.unwrap(), retained);
    let settled = pack_rows(&f, &d, &s).await;
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(pack_rows(&f, &d, &s).await, settled);
    // Equal tiers whose pair exceeds 1 MiB must stay sealed.
    let s = SessionId::new("large-pair");
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), vec![object("a".repeat(600_000))]))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, vec![object("b".repeat(600_000))]).await;
    let before = pack_rows(&f, &d, &s).await;
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(pack_rows(&f, &d, &s).await, before);
    // A failed physical transaction preserves the acknowledged journal; retry
    // succeeds after repairing the fixture's intentionally corrupted unit.
    let s = SessionId::new("maintenance-retry");
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), vec![object("retry-a")]))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, vec![object("retry-b")]).await;
    let rows = pack_rows(&f, &d, &s).await;
    let target = &rows[1];
    let record = raw.load(&d, &s).await.unwrap();
    let members = raw.members(&d, &s).await.unwrap();
    sqlx::query("UPDATE agent_session_journal_pack SET body_payload=zeroblob(length(body_payload)) WHERE pack_id=?").bind(target.0).execute(f.database().pool()).await.unwrap();
    raw.set_cache_budget(0).unwrap();
    let broken = image(&f).await;
    assert!(raw.post_terminal(&d, &s).await.is_err());
    assert_eq!(image(&f).await, broken);
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    sqlx::query("UPDATE agent_session_journal_pack SET body_payload=? WHERE pack_id=?")
        .bind(&target.7)
        .bind(target.0)
        .execute(f.database().pool())
        .await
        .unwrap();
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(raw.members(&d, &s).await.unwrap(), members);
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    // Repacking never removes below-threshold dead bytes or grants membership.
    let s = SessionId::new("deferred-terminal");
    let objects = equal_objects("deferred", 8);
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), objects.clone()))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, vec![object("small-second")]).await;
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![objects[0].id.clone()];
    raw.apply(release).await.unwrap();
    let before = pack_rows(&f, &d, &s).await;
    let retained = raw.retained_bytes(&d).await.unwrap();
    raw.post_terminal(&d, &s).await.unwrap();
    assert_eq!(pack_rows(&f, &d, &s).await, before);
    assert_eq!(raw.retained_bytes(&d).await.unwrap(), retained);
    assert!(raw.objects(&d, &s, &[objects[0].id.clone()]).await.unwrap()[0].is_none());
}

#[tokio::test]
async fn collection_repack_eligible_scoped_packs() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("base");
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    mapped
        .apply(write(
            &d,
            s.as_str(),
            vec![object("stable-a".repeat(12_000))],
        ))
        .await
        .unwrap();
    add_objects(&raw, &d, &s, vec![object("stable-b".repeat(12_000))]).await;
    let stable = pack_rows(&f, &d, &s).await;
    let deferred: Vec<_> = (0..8)
        .map(|i| object(format!("{i}-{}", "d".repeat(10_000))))
        .collect();
    add_objects(&raw, &d, &s, deferred.clone()).await;
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![deferred[0].id.clone()];
    raw.apply(release).await.unwrap();
    let deferred_row = pack_rows(&f, &d, &s).await.pop().unwrap();
    let purge: Vec<_> = (0..8)
        .map(|i| object(format!("{i}-{}", "p".repeat(20_000))))
        .collect();
    add_objects(&raw, &d, &s, purge.clone()).await;
    let purge_id = pack_rows(&f, &d, &s).await.last().unwrap().0;
    for i in 0..2 {
        add_objects(
            &raw,
            &d,
            &s,
            vec![object(format!("small-{i}{}", "s".repeat(20_000)))],
        )
        .await;
    }
    let other_d = domain(JournalRole::Ordinary);
    mapped
        .apply(write(&other_d, s.as_str(), equal_objects("other", 8)))
        .await
        .unwrap();
    let other_s = SessionId::new("base-other");
    mapped
        .apply(write(&d, other_s.as_str(), equal_objects("other", 8)))
        .await
        .unwrap();
    let foreign_members = raw.members(&other_d, &s).await.unwrap();
    let foreign_s = pack_rows(&f, &d, &other_s).await;
    let before = raw.retained_bytes(&d).await.unwrap().unwrap();
    let mut collect = edit(&raw, &d, &s).await;
    collect.kind = JournalWriteKind::Collect;
    collect.release = purge[..2].iter().map(|o| o.id.clone()).collect();
    let receipt = raw.apply(collect).await.unwrap();
    assert_eq!(receipt.reclaimed_objects, 2);
    assert_eq!(
        receipt.reclaimed_bytes,
        purge[..2].iter().map(|o| o.bytes.len() as u64).sum::<u64>()
    );
    let after = pack_rows(&f, &d, &s).await;
    for row in stable.iter().chain(std::iter::once(&deferred_row)) {
        assert!(after.contains(row), "stable/deferred large pack changed");
    }
    assert!(!after.iter().any(|r| r.0 == purge_id));
    // Packs are session-owned: the other domain's small pack may join the
    // base output, keeping exactly its own memberships and readable bodies.
    assert_eq!(after.len(), 4);
    assert!(after.iter().all(|r| r.3 <= 1024 * 1024));
    assert_eq!(
        before - raw.retained_bytes(&d).await.unwrap().unwrap(),
        receipt.reclaimed_bytes
    );
    assert_eq!(raw.members(&other_d, &s).await.unwrap(), foreign_members);
    assert!(
        raw.objects(&other_d, &s, &foreign_members)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
    assert!(
        raw.objects(&other_d, &s, &[purge[2].id.clone(), deferred[1].id.clone()])
            .await
            .unwrap()
            .iter()
            .all(Option::is_none),
        "a repack never grants the other domain a body it did not admit"
    );
    assert_eq!(pack_rows(&f, &d, &other_s).await, foreign_s);
    assert!(
        raw.objects(&d, &s, &[deferred[0].id.clone()])
            .await
            .unwrap()[0]
            .is_none()
    );
    // A second collection has no small or freshly purged inputs to rewrite.
    let mut collect = edit(&raw, &d, &s).await;
    collect.kind = JournalWriteKind::Collect;
    assert_eq!(
        raw.apply(collect).await.unwrap(),
        JournalApplyReceipt::default()
    );
    assert_eq!(pack_rows(&f, &d, &s).await, after);
}

#[tokio::test]
async fn purge_rewrites_only_capped_pack() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("bounded");
    let large: Vec<_> = (0..50)
        .map(|i| object(format!("{i:03}{}", "L".repeat(200_000))))
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), large))
        .await
        .unwrap();
    let stable = pack_rows(&f, &d, &s).await;
    assert!(stable.len() >= 10);
    let purge: Vec<_> = (0..8)
        .map(|i| object(format!("{i:03}{}", "P".repeat(20_000))))
        .collect();
    add_objects(&raw, &d, &s, purge.clone()).await;
    let purged_id = pack_rows(&f, &d, &s).await.last().unwrap().0;
    let mut release = edit(&raw, &d, &s).await;
    release.kind = JournalWriteKind::Collect;
    release.release = purge[..2].iter().map(|o| o.id.clone()).collect();
    let receipt = raw.apply(release).await.unwrap();
    assert_eq!(receipt.reclaimed_objects, 2);
    let after = pack_rows(&f, &d, &s).await;
    for row in &stable {
        assert!(after.contains(row));
    }
    assert_eq!(
        after.iter().find(|r| r.0 == purged_id).unwrap().1,
        1u64.to_be_bytes()
    );
    assert!(after.iter().all(|r| r.3 <= 1024 * 1024));
    // Explicit compaction spans >8 MiB using several single-scope transactions.
    // A deferred pack is deliberately excluded from every physical move.
    let deferred = equal_objects("keep-dead", 8);
    add_objects(&raw, &d, &s, deferred.clone()).await;
    let mut release = edit(&raw, &d, &s).await;
    release.release = vec![deferred[0].id.clone()];
    raw.apply(release).await.unwrap();
    let deferred_row = pack_rows(&f, &d, &s).await.pop().unwrap();
    let other_d = domain(JournalRole::Ordinary);
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&other_d, s.as_str(), equal_objects("foreign", 8)))
        .await
        .unwrap();
    let foreign = raw.members(&other_d, &s).await.unwrap();
    let before_members = raw.members(&d, &s).await.unwrap();
    let before_record = raw.load(&d, &s).await.unwrap();
    let before_bytes = raw.retained_bytes(&d).await.unwrap();
    raw.compact(&d).await.unwrap();
    let (transactions, max_bytes) = raw.compact_stats().unwrap();
    assert!(transactions >= 2);
    assert!(max_bytes <= 8 * 1024 * 1024);
    assert!(max_bytes > 7 * 1024 * 1024);
    assert_eq!(raw.members(&d, &s).await.unwrap(), before_members);
    assert_eq!(raw.load(&d, &s).await.unwrap(), before_record);
    assert_eq!(raw.retained_bytes(&d).await.unwrap(), before_bytes);
    assert!(pack_rows(&f, &d, &s).await.contains(&deferred_row));
    // Session-owned packs: compact may move the other domain's live bodies,
    // never its memberships or what it can read.
    assert_eq!(raw.members(&other_d, &s).await.unwrap(), foreign);
    assert!(
        raw.objects(&other_d, &s, &foreign)
            .await
            .unwrap()
            .iter()
            .all(Option::is_some)
    );
    let reopened = reopen(&f).await;
    assert_eq!(reopened.members(&d, &s).await.unwrap(), before_members);
    assert!(
        reopened
            .objects(&d, &s, &[deferred[0].id.clone()])
            .await
            .unwrap()[0]
            .is_none()
    );
    assert!(
        pack_rows(&f, &d, &s)
            .await
            .iter()
            .all(|r| r.3 <= 1024 * 1024)
    );
}

#[tokio::test]
async fn mixed_live_packs_merge_without_granting_membership() {
    for collection in [false, true] {
        let f = FileDb::new().await.unwrap();
        let raw = storage(&f).await;
        let p = domain(JournalRole::Protected);
        let o = domain(JournalRole::Ordinary);
        let s = SessionId::new(if collection {
            "mixed-collection"
        } else {
            "mixed-terminal"
        });
        let first = equal_objects("first-mixed", 8);
        let second = equal_objects("second-mixed", 8);
        let mapped = MappingStorage::new(raw.clone(), f.clone());
        mapped
            .apply(write(&p, s.as_str(), first.clone()))
            .await
            .unwrap();
        add_objects(&raw, &p, &s, second.clone()).await;
        let ordinary: Vec<_> = first[4..].iter().chain(&second[4..]).cloned().collect();
        mapped
            .apply(write(&o, s.as_str(), ordinary.clone()))
            .await
            .unwrap();
        let mut release = edit(&raw, &p, &s).await;
        release.release = ordinary.iter().map(|v| v.id.clone()).collect();
        assert_eq!(
            raw.apply(release).await.unwrap(),
            JournalApplyReceipt::default()
        );
        assert_eq!(session_pack_ids(&f, &s).await.len(), 2);
        let objects: Vec<_> = first.iter().chain(&second).cloned().collect();
        let ids: Vec<_> = objects.iter().map(|v| v.id.clone()).collect();
        let before_p = raw.objects(&p, &s, &ids).await.unwrap();
        let before_o = raw.objects(&o, &s, &ids).await.unwrap();
        for (index, object) in objects.iter().enumerate() {
            assert_eq!(
                before_p[index],
                (index % 8 < 4).then(|| object.bytes.clone())
            );
            assert_eq!(
                before_o[index],
                (index % 8 >= 4).then(|| object.bytes.clone())
            );
        }
        if collection {
            let mut w = edit(&raw, &o, &s).await;
            w.kind = JournalWriteKind::Collect;
            let plan = JournalCollectionPlan {
                write: w,
                max_units: 32,
            };
            assert_eq!(
                raw.prepare_collection_apply(&plan).await.unwrap(),
                JournalCollectionPreflight::Fits { units: 2 }
            );
            let receipt = raw.apply_collection(plan).await.unwrap();
            assert_eq!(receipt.receipt, JournalApplyReceipt::default());
            assert_eq!(receipt.rewritten_units, 2);
        } else {
            raw.post_terminal(&o, &s).await.unwrap();
        }
        assert_eq!(
            session_pack_ids(&f, &s).await.len(),
            1,
            "fully live mixed packs with no complete holder remain eligible"
        );
        assert_eq!(raw.objects(&p, &s, &ids).await.unwrap(), before_p);
        assert_eq!(raw.objects(&o, &s, &ids).await.unwrap(), before_o);
        let cold = reopen(&f).await;
        assert_eq!(cold.objects(&p, &s, &ids).await.unwrap(), before_p);
        assert_eq!(cold.objects(&o, &s, &ids).await.unwrap(), before_o);
    }
}
