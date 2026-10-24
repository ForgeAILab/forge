use super::*;

/// One random structural edit: bit flip, byte overwrite, truncation, append,
/// swap, duplicated slice or (for the pack index) a rewritten header count.
fn mutate_bytes(seed: u64, bytes: &mut Vec<u8>) {
    let mut s = seed | 1;
    let pick = |s: &mut u64, n: usize| (xorshift(s) as usize) % n.max(1);
    match pick(&mut s, 7) {
        0 => {
            let i = pick(&mut s, bytes.len());
            bytes[i] ^= 1 << pick(&mut s, 8);
        }
        1 => {
            let i = pick(&mut s, bytes.len());
            bytes[i] = xorshift(&mut s) as u8;
        }
        2 => bytes.truncate(pick(&mut s, bytes.len())),
        3 => {
            for _ in 0..=pick(&mut s, 8) {
                bytes.push(xorshift(&mut s) as u8);
            }
        }
        4 => {
            let (i, j) = (pick(&mut s, bytes.len()), pick(&mut s, bytes.len()));
            bytes.swap(i, j);
        }
        5 => {
            let i = pick(&mut s, bytes.len());
            let end = (i + 1 + pick(&mut s, 16)).min(bytes.len());
            let slice = bytes[i..end].to_vec();
            let at = pick(&mut s, bytes.len());
            bytes.splice(at..at, slice);
        }
        _ => {
            if bytes.len() >= 12 {
                bytes[8..12].copy_from_slice(&(xorshift(&mut s) as u32 % 8).to_le_bytes());
            }
        }
    }
}

/// AEAD passes after resealing with the real key and binding, so the bounded
/// decoders alone must refuse the edit: no panic, no wrong bytes, and every
/// full-scope read (members, retained bytes, any decoded body) fails closed.
#[tokio::test]
async fn resealed_unit_corruption_fails_closed() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("fuzz");
    // A prefix-colliding pair plus mixed kinds, then a second pack.
    let mut first = vec![object(42269u64), object(139372u64), object("alpha")];
    let state = JournalObject::encode(
        JournalObjectKind::StateValue,
        &serde_json::json!({"k":[1,2,3]}),
    )
    .unwrap();
    first.push(JournalStoredObject {
        id: state.id,
        kind: JournalObjectKind::StateValue,
        bytes: state.bytes.into(),
    });
    let second = equal_objects("second", 5);
    f.seed_journal(&raw, write(&d, s.as_str(), first.clone()))
        .await
        .unwrap();
    let mut more = write(&d, s.as_str(), second.clone());
    more.operation = None;
    f.seed_journal(&raw, more).await.unwrap();
    let mut ids: Vec<_> = first.iter().chain(&second).map(|o| o.id.clone()).collect();
    ids.sort();
    let fresh = || ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
    let expected_objects = fresh().objects(&d, &s, &ids).await.unwrap();
    assert!(expected_objects.iter().all(Option::is_some));
    let expected_members = fresh().members(&d, &s).await.unwrap();
    let expected_retained = fresh().retained_bytes(&d).await.unwrap();
    let op = JournalOperationId::new("operation");
    assert!(fresh().operation(&d, &s, &op).await.unwrap().is_some());
    let packs: Vec<i64> = pack_rows(&f, &d, &s).await.iter().map(|p| p.0).collect();
    assert_eq!(packs.len(), 2);
    let original = image(&f).await;
    let pool = f.database().pool().clone();
    let saved_packs = pack_rows(&f, &d, &s).await;
    let saved_ops: Vec<(i64, Vec<u8>, Vec<u8>)> = sqlx::query("SELECT tier,nonce,payload FROM agent_session_journal_operation_pack WHERE runtime_session_id='fuzz'")
        .fetch_all(&pool).await.unwrap().into_iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect();
    let (mut detected, mut identical, mut false_absent) = (0, 0, 0);
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for round in 0..240u64 {
        let pack = packs[(round % 2) as usize];
        let unit = match round % 4 {
            0 => TestSealedUnit::PackIndex(pack),
            1 => TestSealedUnit::PackBody(pack),
            2 => TestSealedUnit::PackBodyCanonical(pack),
            _ => TestSealedUnit::Operations(0),
        };
        let mutation = xorshift(&mut seed);
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = changed.clone();
        raw.reseal_unit_for_test(&d, &s, unit, move |bytes| {
            let before = bytes.clone();
            mutate_bytes(mutation, bytes);
            flag.store(*bytes != before, std::sync::atomic::Ordering::SeqCst);
        })
        .await
        .unwrap();
        let changed = changed.load(std::sync::atomic::Ordering::SeqCst);
        // A fresh storage has no warm directory or body to hide the edit.
        let objects = fresh().objects(&d, &s, &ids).await;
        let members = fresh().members(&d, &s).await;
        let retained = fresh().retained_bytes(&d).await;
        let operation = fresh().operation(&d, &s, &op).await;
        if let TestSealedUnit::Operations(_) = unit {
            // Operation records are opaque authenticated data; the decoder
            // bounds the frame and the tier shape and must never panic.
            assert_eq!(objects.unwrap(), expected_objects);
            if !changed {
                assert!(operation.unwrap().is_some());
            }
        } else {
            assert!(operation.unwrap().is_some());
            if let Ok(found) = &objects {
                for (got, want) in found.iter().zip(&expected_objects) {
                    // Never different bytes; an index-only edit may hide a
                    // candidate (absent) until its body is decoded.
                    assert!(
                        got.is_none() || got == want,
                        "round {round}: wrong body served"
                    );
                    if got.is_none() {
                        false_absent += 1;
                    }
                }
            }
            if changed && matches!(unit, TestSealedUnit::PackBody(_)) {
                // A compressed frame edit may still inflate to the same bytes.
                assert!(members.is_err() || members.unwrap() == expected_members);
                detected += 1;
            } else if changed {
                assert!(
                    members.is_err(),
                    "round {round}: {unit:?} members accepted a changed unit"
                );
                if let TestSealedUnit::PackIndex(_) = unit {
                    // D-2 diagnostics omit a corrupt scope, with an explicit
                    // skipped count; reads and membership still fail closed.
                    let stats = fresh().byte_stats(&d).await.unwrap();
                    assert!(stats.skipped_sessions == 1 || retained.unwrap() == expected_retained);
                }
                if matches!(unit, TestSealedUnit::PackBodyCanonical(_)) {
                    assert!(
                        objects.is_err(),
                        "round {round}: changed canonical body served"
                    );
                }
                detected += 1;
            } else {
                assert_eq!(members.unwrap(), expected_members);
                assert_eq!(objects.unwrap(), expected_objects);
                identical += 1;
            }
        }
        // Restore the exact authenticated rows for the next round.
        for p in &saved_packs {
            sqlx::query("UPDATE agent_session_journal_pack SET index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=?")
                .bind(&p.4).bind(&p.5).bind(&p.6).bind(&p.7).bind(p.0).execute(&pool).await.unwrap();
        }
        for (tier, nonce, payload) in &saved_ops {
            sqlx::query("UPDATE agent_session_journal_operation_pack SET nonce=?,payload=? WHERE runtime_session_id='fuzz' AND tier=?")
                .bind(nonce).bind(payload).bind(tier).execute(&pool).await.unwrap();
        }
        assert_eq!(image(&f).await, original);
    }
    assert!(detected > 100, "{detected} detected, {identical} identical");
    eprintln!(
        "resealed corruption: {detected} detected, {identical} identity edits, {false_absent} index-hidden lookups"
    );
}

/// Every sealed unit binds its complete owning domain/runtime-session/Forge
/// key, not only its row id: rows relabelled into another scope, with
/// matching membership and scalar columns, fail closed instead of serving the
/// original scope's plaintext.
#[tokio::test]
async fn relabelled_rows_fail_closed() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let victim = object("victim secret");
    let (a, b) = (SessionId::new("relabel-a"), SessionId::new("relabel-b"));
    f.seed_journal(&raw, write(&d, a.as_str(), vec![victim.clone()]))
        .await
        .unwrap();
    f.seed_journal(&raw, write(&d, b.as_str(), vec![object("b own")]))
        .await
        .unwrap();
    let ordinary = domain(JournalRole::Ordinary);
    f.seed_journal(
        &raw,
        write(&ordinary, b.as_str(), vec![object("b ordinary")]),
    )
    .await
    .unwrap();
    let pool = f.database().pool().clone();
    let b_owner: String = sqlx::query_scalar(
        "SELECT forge_session_id FROM agent_session_journal_session WHERE runtime_session_id='relabel-b'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let fresh = || ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
    assert!(
        fresh()
            .objects(&d, &b, std::slice::from_ref(&victim.id))
            .await
            .unwrap()[0]
            .is_none()
    );
    // Session record: copy A's sealed record and every scalar column into B.
    sqlx::query("UPDATE agent_session_journal_session SET (generation,revision,writer_epoch,collection_dirty,published,has_checkpoint,retired,nonce,payload) = (SELECT generation,revision,writer_epoch,collection_dirty,published,has_checkpoint,retired,nonce,payload FROM agent_session_journal_session WHERE runtime_session_id='relabel-a') WHERE runtime_session_id='relabel-b'")
        .execute(&pool).await.unwrap();
    assert!(fresh().load(&d, &b).await.is_err());
    // Operation tier zero: same tier key, other scope.
    sqlx::query("UPDATE agent_session_journal_operation_pack SET (nonce,payload) = (SELECT nonce,payload FROM agent_session_journal_operation_pack WHERE runtime_session_id='relabel-a' AND tier=0) WHERE runtime_session_id='relabel-b' AND tier=0")
        .execute(&pool).await.unwrap();
    assert!(
        fresh()
            .operation(&d, &b, &JournalOperationId::new("operation"))
            .await
            .is_err()
    );
    // Pin payload under B's own lookup key.
    sqlx::query("UPDATE agent_session_journal_pin SET (nonce,payload) = (SELECT nonce,payload FROM agent_session_journal_pin WHERE runtime_session_id='relabel-a') WHERE runtime_session_id='relabel-b'")
        .execute(&pool).await.unwrap();
    assert!(fresh().pins(&d, &b).await.is_err());
    // Pack: move A's pack (id and generation unchanged) to B with a held bit.
    let a_pack: i64 = sqlx::query_scalar(
        "SELECT pack_id FROM agent_session_journal_pack WHERE runtime_session_id='relabel-a'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM agent_session_journal_membership WHERE pack_id=?")
        .bind(a_pack)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_session_journal_pack SET runtime_session_id='relabel-b', forge_session_id=? WHERE pack_id=?")
        .bind(&b_owner).bind(a_pack).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO agent_session_journal_membership (storage_id,role,runtime_session_id,forge_session_id,pack_id,bitmap) VALUES (?, 'protected', 'relabel-b', ?, ?, x'01')")
        .bind(d.storage_id.as_str()).bind(&b_owner).bind(a_pack).execute(&pool).await.unwrap();
    assert!(
        fresh()
            .objects(&d, &b, std::slice::from_ref(&victim.id))
            .await
            .is_err()
    );
    assert!(fresh().members(&d, &b).await.is_err());
    let stats = fresh().byte_stats(&d).await.unwrap();
    assert_eq!(stats.skipped_sessions, 1);
    assert_eq!(stats.retained_bytes, 0);
    // The same moved pack held by the other domain of B's session: packs are
    // session-sealed, so the foreign session binding still fails closed.
    sqlx::query("INSERT INTO agent_session_journal_membership (storage_id,role,runtime_session_id,forge_session_id,pack_id,bitmap) VALUES (?, 'ordinary', 'relabel-b', ?, ?, x'01')")
        .bind(ordinary.storage_id.as_str()).bind(&b_owner).bind(a_pack).execute(&pool).await.unwrap();
    assert!(
        fresh()
            .objects(&ordinary, &b, std::slice::from_ref(&victim.id))
            .await
            .is_err()
    );
    let stats = fresh().byte_stats(&ordinary).await.unwrap();
    assert_eq!(stats.skipped_sessions, 1);
    assert_eq!(stats.retained_bytes, 0);
    assert!(fresh().retained_bytes(&d).await.is_ok());
}

#[tokio::test]
async fn corrupt_shared_body_isolates_its_session() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = MappingStorage::new(raw.clone(), f.clone());
    let (p, o) = (
        domain(JournalRole::Protected),
        domain(JournalRole::Ordinary),
    );
    let objects = equal_objects("corrupt", 8);
    let length: u64 = objects.iter().map(|o| o.bytes.len() as u64).sum();
    let (bad, good) = (SessionId::new("bad"), SessionId::new("good"));
    for s in [&bad, &good] {
        for d in [&p, &o] {
            mapped
                .apply(write(d, s.as_str(), objects.clone()))
                .await
                .unwrap();
        }
    }
    sqlx::query("UPDATE agent_session_journal_pack SET body_payload=zeroblob(length(body_payload)) WHERE runtime_session_id='bad'")
        .execute(f.database().pool())
        .await
        .unwrap();
    let reopened = reopen(&f).await;
    for d in [&p, &o] {
        let error = reopened.objects(d, &bad, &ids(&objects)).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::Serialization);
        assert!(!error.retryable, "corruption is not a transient failure");
        assert!(
            reopened
                .objects(d, &good, &ids(&objects))
                .await
                .unwrap()
                .iter()
                .all(Option::is_some)
        );
        // Diagnostics authenticate index units only; both sessions' indexes
        // are intact, so the corrupt body is still counted as retained.
        let stats = reopened.byte_stats(d).await.unwrap();
        assert_eq!(stats.skipped_sessions, 0, "{d:?}");
        assert_eq!(stats.retained_bytes, length * 2, "{d:?}");
        // No maintenance rewrites or releases the unreadable session.
        reopened.compact(d).await.unwrap();
        let mut release = edit(&reopened, d, &bad).await;
        release.release = ids(&objects[..1]);
        assert!(reopened.apply(release).await.is_err());
    }
    assert_eq!(session_object_count(&f, &bad).await, 8);
}
