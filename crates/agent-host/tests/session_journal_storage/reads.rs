use super::*;

#[tokio::test]
async fn read_membership_metadata_and_reopen() {
    let f = FileDb::new().await.unwrap();
    let storage = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let o = object("opaque signed reasoning\u{0000} exact");
    let stranger = SessionId::new("stranger");
    assert_eq!(storage.sharing(), JournalBodySharing::PerSession);
    assert_eq!(storage.load(&d, &stranger).await.unwrap(), None);
    assert_eq!(
        storage
            .objects(&d, &stranger, std::slice::from_ref(&o.id))
            .await
            .unwrap(),
        vec![None]
    );
    assert!(storage.pins(&d, &stranger).await.unwrap().is_empty());
    assert!(
        storage
            .catalog(&d, &stranger, &o.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .operation(&d, &stranger, &JournalOperationId::new("operation"))
            .await
            .unwrap()
            .is_none()
    );
    let w = write(&d, "reader", vec![o.clone()]);
    f.seed_journal(&storage, w.clone()).await.unwrap();
    assert_eq!(storage.cache_stats().unwrap().index_decodes, 0);
    let other = reopen(&f).await;
    for st in [&storage, &other] {
        assert_eq!(
            st.load(&d, &w.session).await.unwrap(),
            Some(w.record.clone())
        );
        assert_eq!(st.cache_stats().unwrap().index_decodes, 0);
        let requested = [
            o.id.clone(),
            JournalObjectId::from_bytes([0; 32]),
            o.id.clone(),
        ];
        assert_eq!(
            st.objects(&d, &w.session, &requested).await.unwrap(),
            vec![Some(o.bytes.clone()), None, Some(o.bytes.clone())]
        );
        assert_eq!(
            st.members(&d, &w.session).await.unwrap(),
            vec![o.id.clone()]
        );
        assert_eq!(
            st.operation(&d, &w.session, &w.operation.as_ref().unwrap().operation)
                .await
                .unwrap(),
            w.operation
        );
        let JournalPinChange::Put(pin) = &w.pins[0] else {
            unreachable!()
        };
        assert_eq!(st.pins(&d, &w.session).await.unwrap(), vec![pin.clone()]);
        let JournalCatalogChange::Put(entry) = &w.catalog[0] else {
            unreachable!()
        };
        assert_eq!(
            st.catalog(&d, &w.session, &entry.head).await.unwrap(),
            Some(entry.clone())
        );
        assert_eq!(
            st.objects(
                &domain(JournalRole::Ordinary),
                &w.session,
                std::slice::from_ref(&o.id)
            )
            .await
            .unwrap(),
            vec![None]
        );
    }
    // SQL membership is consulted even when the body and directory are warm.
    sqlx::query("DELETE FROM agent_session_journal_membership WHERE runtime_session_id='reader'")
        .execute(f.database().pool())
        .await
        .unwrap();
    assert_eq!(
        storage
            .objects(&d, &w.session, std::slice::from_ref(&o.id))
            .await
            .unwrap(),
        vec![None]
    );
    assert!(storage.members(&d, &w.session).await.unwrap().is_empty());
    assert_eq!(
        storage.retained_bytes(&d).await.unwrap(),
        Some(o.bytes.len() as u64)
    );
    // Every queryable scalar must agree with the authenticated record.
    for column in ["collection_dirty", "published", "has_checkpoint", "retired"] {
        let db = f.database();
        let saved: i64 = sqlx::query_scalar(&format!(
            "SELECT {column} FROM agent_session_journal_session WHERE runtime_session_id='reader'"
        ))
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(&format!(
            "UPDATE agent_session_journal_session SET {column}=? WHERE runtime_session_id='reader'"
        ))
        .bind(1 - saved)
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(
            storage.load(&d, &w.session).await.unwrap_err().kind,
            ErrorKind::Serialization
        );
        sqlx::query(&format!(
            "UPDATE agent_session_journal_session SET {column}=? WHERE runtime_session_id='reader'"
        ))
        .bind(saved)
        .execute(db.pool())
        .await
        .unwrap();
    }
    for column in ["revision", "writer_epoch", "generation"] {
        let db = f.database();
        let saved: Vec<u8> = sqlx::query_scalar(&format!(
            "SELECT {column} FROM agent_session_journal_session WHERE runtime_session_id='reader'"
        ))
        .fetch_one(db.pool())
        .await
        .unwrap();
        sqlx::query(&format!(
            "UPDATE agent_session_journal_session SET {column}=? WHERE runtime_session_id='reader'"
        ))
        .bind(u64::MAX.to_be_bytes().as_slice())
        .execute(db.pool())
        .await
        .unwrap();
        assert_eq!(
            storage.load(&d, &w.session).await.unwrap_err().kind,
            ErrorKind::Serialization
        );
        sqlx::query(&format!(
            "UPDATE agent_session_journal_session SET {column}=? WHERE runtime_session_id='reader'"
        ))
        .bind(saved)
        .execute(db.pool())
        .await
        .unwrap();
    }
    for (sid, owner) in [("z", "host-1"), ("a", "host-3"), ("m", "host-2")] {
        let id = f.ensure_session(&SessionId::new(sid)).await.unwrap();
        sqlx::query("UPDATE agent_session SET id=? WHERE id=?")
            .bind(owner)
            .bind(id)
            .execute(f.database().pool())
            .await
            .unwrap();
        f.seed_journal(&storage, write(&d, sid, vec![]))
            .await
            .unwrap();
    }
    assert!(
        storage
            .dirty_sessions(&d, None, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage.dirty_sessions(&d, None, 2).await.unwrap(),
        vec![SessionId::new("a"), SessionId::new("m")]
    );
    assert_eq!(
        storage
            .dirty_sessions(&d, Some(&SessionId::new("m")), 9)
            .await
            .unwrap(),
        vec![w.session.clone(), SessionId::new("z")]
    );
    assert!(
        storage
            .dirty_sessions(&domain(JournalRole::Ordinary), None, 9)
            .await
            .unwrap()
            .is_empty()
    );
    let wrong = ForgeJournalStorage::new(f.database(), [8; 32], 1).unwrap();
    assert!(wrong.load(&d, &w.session).await.is_err());
}

#[tokio::test]
async fn session_scoped_lazy_admission_and_reopen() {
    let f = FileDb::new().await.unwrap();
    let writer = storage(&f).await;
    let d = domain(JournalRole::Protected);
    for n in 0..12 {
        f.seed_journal(
            &writer,
            write(&d, &format!("unrelated-{n}"), vec![object(n)]),
        )
        .await
        .unwrap();
    }
    let collision = vec![object(42269), object(139372)];
    let w = write(&d, "target", collision.clone());
    f.seed_journal(&writer, w.clone()).await.unwrap();
    // An unrelated corrupted pack must never be hydrated for target reads.
    sqlx::query("UPDATE agent_session_journal_pack SET index_payload=zeroblob(32),body_payload=zeroblob(32) WHERE runtime_session_id='unrelated-0'").execute(f.database().pool()).await.unwrap();
    let reader = reopen(&f).await;
    assert_eq!(reader.cache_stats().unwrap().index_decodes, 0);
    assert_eq!(
        reader.load(&d, &w.session).await.unwrap(),
        Some(w.record.clone())
    );
    assert_eq!(reader.cache_stats().unwrap().index_decodes, 0);
    let mut requests = Vec::new();
    for _ in 0..50 {
        requests.extend(collision.iter().map(|o| o.id.clone()));
    }
    let result = reader.objects(&d, &w.session, &requests).await.unwrap();
    assert_eq!(result.len(), 100);
    for (n, value) in result.iter().enumerate() {
        assert_eq!(value.as_ref().unwrap(), &collision[n % 2].bytes);
    }
    let stats = reader.cache_stats().unwrap();
    assert_eq!(stats.index_decodes, 1);
    assert_eq!(stats.body_decodes, 1);
    assert_eq!(stats.pack_queries, 2);
    reader.objects(&d, &w.session, &requests).await.unwrap();
    let warm = reader.cache_stats().unwrap();
    assert_eq!(warm.pack_queries, stats.pack_queries);
    // Same-scope re-admission deduplicates, including across independent pools.
    let peer = reopen(&f).await;
    let mut again = w.clone();
    again.record.generation += 1;
    f.seed_journal(&peer, again).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id='target'",
    )
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(count, 1);
    let extra = object("new from independent connection");
    let mut update = write(&d, "target", vec![extra.clone()]);
    update.record.generation = 3;
    update.operation = None;
    f.seed_journal(&peer, update).await.unwrap();
    assert_eq!(
        reader
            .objects(&d, &w.session, &[extra.id.clone(), collision[0].id.clone()])
            .await
            .unwrap(),
        vec![Some(extra.bytes.clone()), Some(collision[0].bytes.clone())]
    );
    assert_eq!(reader.cache_stats().unwrap().index_decodes, 2);
    // Genuine LRU pressure evicts idle directories and bodies together.
    reader.set_cache_budget(2500).unwrap();
    reader.objects(&d, &w.session, &requests).await.unwrap();
    assert!(reader.cache_stats().unwrap().charged_bytes > 0);
    for n in 1..=3 {
        reader
            .objects(
                &d,
                &SessionId::new(format!("unrelated-{n}")),
                &[object(n).id],
            )
            .await
            .unwrap();
        let c = reader.cache_stats().unwrap();
        assert!(c.charged_bytes <= c.budget);
    }
    let before = reader.cache_stats().unwrap();
    reader.objects(&d, &w.session, &requests).await.unwrap();
    assert_eq!(
        reader.cache_stats().unwrap().index_decodes,
        before.index_decodes + 2
    );
    // Evicted/oversized directories rebuild only their own scope.
    reader.set_cache_budget(64).unwrap();
    reader.objects(&d, &w.session, &requests).await.unwrap();
    let small = reader.cache_stats().unwrap();
    assert_eq!(small.charged_bytes, 0);
    assert!(small.charged_bytes <= small.budget);
    reader.set_cache_budget(64 * 1024 * 1024).unwrap();
    reader.objects(&d, &w.session, &requests).await.unwrap();
    assert!(reader.cache_stats().unwrap().charged_bytes > 0);
    let corrupt = reopen(&f).await;
    assert!(
        corrupt
            .objects(&d, &SessionId::new("unrelated-0"), &[object(0).id])
            .await
            .is_err()
    );
    // A valid independent rewrite advances generation and uses fresh nonces.
    let nonce:Vec<u8>=sqlx::query_scalar("SELECT index_nonce FROM agent_session_journal_pack WHERE runtime_session_id='target' ORDER BY pack_id LIMIT 1").fetch_one(f.database().pool()).await.unwrap();
    f.reseal_journal_scope(&peer, &d, &w.session).await.unwrap();
    let changed_nonce:Vec<u8>=sqlx::query_scalar("SELECT index_nonce FROM agent_session_journal_pack WHERE runtime_session_id='target' ORDER BY pack_id LIMIT 1").fetch_one(f.database().pool()).await.unwrap();
    assert_ne!(nonce, changed_nonce);
    let before = reader.cache_stats().unwrap();
    reader.objects(&d, &w.session, &requests).await.unwrap();
    assert_eq!(
        reader.cache_stats().unwrap().index_decodes,
        before.index_decodes + 2
    );
    // A generation change cannot reuse the previous cached slots or ciphertext.
    sqlx::query(
        "UPDATE agent_session_journal_pack SET generation=? WHERE runtime_session_id='target'",
    )
    .bind(2u64.to_be_bytes().as_slice())
    .execute(f.database().pool())
    .await
    .unwrap();
    assert!(reader.objects(&d, &w.session, &requests).await.is_err());
}

#[tokio::test]
async fn retained_bytes_include_deferred_bodies() {
    let f = FileDb::new().await.unwrap();
    let storage = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let ordinary = domain(JournalRole::Ordinary);
    let objects: Vec<_> = (0..10)
        .map(|n| object(format!("{n}{}", "x".repeat(100))))
        .collect();
    let total = objects.iter().map(|o| o.bytes.len() as u64).sum::<u64>();
    let w = write(&d, "owner", objects.clone());
    f.seed_journal(&storage, w.clone()).await.unwrap();
    f.seed_journal(&storage, write(&d, "other", objects.clone()))
        .await
        .unwrap();
    f.seed_journal(&storage, write(&ordinary, "owner", objects.clone()))
        .await
        .unwrap();
    assert_eq!(storage.retained_bytes(&d).await.unwrap(), Some(total * 2));
    assert_eq!(
        storage.retained_bytes(&ordinary).await.unwrap(),
        Some(total)
    );
    // Physical-byte scalar metadata is checked against the sealed index too.
    // Session "owner" stores one pack shared by both domains.
    let row=sqlx::query("SELECT pack_id,retained_canonical_bytes FROM agent_session_journal_pack WHERE runtime_session_id='owner'").fetch_one(f.database().pool()).await.unwrap();
    let pack_id: i64 = row.get("pack_id");
    let retained: i64 = row.get("retained_canonical_bytes");
    sqlx::query("UPDATE agent_session_journal_pack SET retained_canonical_bytes=? WHERE pack_id=?")
        .bind(retained + 1)
        .bind(pack_id)
        .execute(f.database().pool())
        .await
        .unwrap();
    let partial = storage.byte_stats(&d).await.unwrap();
    assert_eq!(
        partial.skipped_sessions, 1,
        "bad scalar/index agreement is detected"
    );
    assert_eq!(
        partial.retained_bytes, total,
        "only the healthy session is counted"
    );
    // The bad pack is shared, so the ordinary domain's view of that session
    // is skipped too (it used to be an independent, healthy copy).
    let ordinary_partial = storage.byte_stats(&ordinary).await.unwrap();
    assert_eq!(ordinary_partial.skipped_sessions, 1);
    assert_eq!(ordinary_partial.retained_bytes, 0);
    sqlx::query("UPDATE agent_session_journal_pack SET retained_canonical_bytes=? WHERE pack_id=?")
        .bind(retained)
        .bind(pack_id)
        .execute(f.database().pool())
        .await
        .unwrap();
    let mut again = w.clone();
    again.record.generation = 2;
    f.seed_journal(&storage, again).await.unwrap();
    assert_eq!(storage.retained_bytes(&d).await.unwrap(), Some(total * 2));
    // Simulate a below-threshold release independently of task 9's collector.
    let row=sqlx::query("SELECT pack_id,bitmap FROM agent_session_journal_membership WHERE storage_id=? AND role='protected' AND runtime_session_id='owner'").bind(d.storage_id.as_str()).fetch_one(f.database().pool()).await.unwrap();
    let mut bits: Vec<u8> = row.get("bitmap");
    bits[0] &= !1;
    sqlx::query(
        "UPDATE agent_session_journal_membership SET bitmap=? WHERE pack_id=? AND role='protected'",
    )
    .bind(bits)
    .bind(row.get::<i64, _>("pack_id"))
    .execute(f.database().pool())
    .await
    .unwrap();
    let mut sorted = objects.clone();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(
        storage
            .objects(
                &d,
                &w.session,
                &[sorted[0].id.clone(), sorted[1].id.clone()]
            )
            .await
            .unwrap(),
        vec![None, Some(sorted[1].bytes.clone())]
    );
    assert_eq!(storage.retained_bytes(&d).await.unwrap(), Some(total * 2));
    let reopened = reopen(&f).await;
    assert_eq!(reopened.retained_bytes(&d).await.unwrap(), Some(total * 2));
    // Orphan packs after a cascade remain in the physical diagnostic.
    let host = f.ensure_session(&w.session).await.unwrap();
    sqlx::query("DELETE FROM agent_session WHERE id=?")
        .bind(host)
        .execute(f.database().pool())
        .await
        .unwrap();
    assert_eq!(storage.retained_bytes(&d).await.unwrap(), Some(total * 2));
    assert!(storage.members(&d, &w.session).await.unwrap().is_empty());
    assert_eq!(
        storage
            .objects(
                &d,
                &SessionId::new("other"),
                std::slice::from_ref(&sorted[0].id)
            )
            .await
            .unwrap(),
        vec![Some(sorted[0].bytes.clone())]
    );
}
