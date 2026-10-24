use super::*;

#[tokio::test]
async fn operation_pack_replay_restart() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("operations");
    let engine = JournalEngine::protected(
        Arc::new(MappingStorage::new(raw.clone(), f.clone())),
        d.storage_id.clone(),
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
    let mut previous = None;
    let mut commits = Vec::new();
    let mut heads = Vec::new();
    for i in 1..=6 {
        let commit = empty_commit(
            &s,
            writer.writer_epoch().unwrap(),
            &format!("op-{i}"),
            previous.as_ref(),
            i,
        );
        let h = engine.commit(&writer, commit.clone()).await.unwrap();
        commits.push(commit);
        heads.push(h.clone());
        previous = Some(h);
    }
    assert_eq!(
        operation_rows(&f, &d, &s)
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let before = image(&f).await;
    assert_eq!(
        engine.commit(&writer, commits[0].clone()).await.unwrap(),
        heads[0]
    );
    let mut changed = commits[0].clone();
    changed.delta.bootstrap.as_mut().unwrap().updated = Timestamp(999);
    changed.digest = changed.input_digest().unwrap();
    state_conflict(engine.commit(&writer, changed).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    drop(engine);
    let fresh = reopen(&f).await;
    let engine = JournalEngine::protected(
        Arc::new(MappingStorage::new(fresh.clone(), f.clone())),
        d.storage_id.clone(),
        engine_config(),
    );
    let writer = engine
        .open(
            &s,
            open_request(
                "successor",
                JournalRootSelection::Latest,
                JournalOpenIntent::ExclusiveWriter,
            ),
        )
        .await
        .unwrap();
    for (commit, expected) in commits.into_iter().zip(heads) {
        assert_eq!(
            engine.commit(&writer, commit.clone()).await.unwrap(),
            expected
        );
        assert_eq!(
            fresh
                .operation(&d, &s, &commit.batch.operation)
                .await
                .unwrap()
                .unwrap()
                .result,
            expected
        );
    }
    // Duplicate identical direct records preserve every sealed operation pack.
    let rows = operation_rows(&f, &d, &s).await;
    let record = fresh.load(&d, &s).await.unwrap().unwrap();
    let mut edit = write(&d, s.as_str(), vec![]);
    edit.expect = record.fence();
    edit.record = record;
    edit.record.generation += 1;
    edit.pins.clear();
    edit.catalog.clear();
    edit.operation = fresh
        .operation(&d, &s, &JournalOperationId::new("op-1"))
        .await
        .unwrap();
    fresh.apply(edit.clone()).await.unwrap();
    assert_eq!(operation_rows(&f, &d, &s).await, rows);
    edit.expect = edit.record.fence();
    edit.record.generation += 1;
    edit.operation.as_mut().unwrap().result.created = Timestamp(999);
    let before = image(&f).await;
    state_conflict(fresh.apply(edit).await.unwrap_err());
    assert_eq!(image(&f).await, before);
}

#[tokio::test]
async fn engine_replay_window_edges() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let s = SessionId::new("window");
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let d = domain(role);
        let mut outsider_domain = d.clone();
        outsider_domain.storage_id = JournalStorageId::new("other-storage");
        for (other_d, other_s) in [(&outsider_domain, s.as_str()), (&d, "window-other")] {
            let mut other = write(other_d, other_s, vec![]);
            other.record.collection_dirty = false;
            f.seed_journal(&raw, other).await.unwrap();
        }
        let outsider = operation_rows(&f, &outsider_domain, &s).await;
        let other_session = operation_rows(&f, &d, &SessionId::new("window-other")).await;
        let mapped = Arc::new(MappingStorage::new(raw.clone(), f.clone()));
        let engine = if role == JournalRole::Protected {
            JournalEngine::protected(mapped, d.storage_id.clone(), engine_config())
        } else {
            JournalEngine::ordinary(
                mapped,
                d.storage_id.clone(),
                Arc::new(IdentityJournalProjection),
                engine_config(),
            )
        };
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
        let mut previous = None;
        let mut commits = Vec::new();
        let mut heads = Vec::new();
        for revision in 1..=70 {
            let c = empty_commit(
                &s,
                writer.writer_epoch().unwrap(),
                &format!("op-{revision}"),
                previous.as_ref(),
                revision,
            );
            let h = engine.commit(&writer, c.clone()).await.unwrap();
            previous = Some(h.clone());
            heads.push(h);
            commits.push(c);
        }
        // Commits never prune, including an older record from this same epoch.
        assert!(
            raw.operation(&d, &s, &commits[0].batch.operation)
                .await
                .unwrap()
                .is_some()
        );
        let stable = operation_rows(&f, &d, &s).await;
        // Zero membership budget still executes the real engine's S5 directive.
        drain_gc(
            &engine,
            JournalGcRequest {
                storage_id: engine.storage_id(),
                expected_root_epoch: engine.root_epoch().unwrap(),
                max_objects: 0,
                work_budget: Default::default(),
            },
        )
        .await
        .unwrap();
        let after = operation_rows(&f, &d, &s).await;
        for tier in [1, 2] {
            assert_eq!(
                stable.get(&tier),
                after.get(&tier),
                "unchanged tier was resealed"
            );
        }
        for (i, c) in commits.iter().enumerate() {
            assert_eq!(
                raw.operation(&d, &s, &c.batch.operation)
                    .await
                    .unwrap()
                    .is_some(),
                i + 1 >= 6
            );
        }
        assert_eq!(
            engine.commit(&writer, commits[5].clone()).await.unwrap(),
            heads[5]
        );
        state_conflict(
            engine
                .commit(&writer, commits[4].clone())
                .await
                .unwrap_err(),
        );
        engine.close(writer).await.unwrap();
        drop(engine);
        let fresh = reopen(&f).await;
        let mapped = Arc::new(MappingStorage::new(fresh.clone(), f.clone()));
        let engine = if role == JournalRole::Protected {
            JournalEngine::protected(mapped, d.storage_id.clone(), engine_config())
        } else {
            JournalEngine::ordinary(
                mapped,
                d.storage_id.clone(),
                Arc::new(IdentityJournalProjection),
                engine_config(),
            )
        };
        let writer = engine
            .open(
                &s,
                open_request(
                    "new-writer",
                    JournalRootSelection::Latest,
                    JournalOpenIntent::ExclusiveWriter,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            engine.commit(&writer, commits[5].clone()).await.unwrap(),
            heads[5]
        );
        // A directive above every revision retains only the last operation of
        // the post-write epoch, then a newer empty epoch removes that exception.
        let record = fresh.load(&d, &s).await.unwrap().unwrap();
        let mut edit = write(&d, s.as_str(), vec![]);
        edit.expect = record.fence();
        edit.record = record;
        edit.record.generation += 1;
        edit.record.writer_epoch = heads[69].writer_epoch;
        edit.operation = None;
        edit.pins.clear();
        edit.catalog.clear();
        edit.prune_operations_before = Some(JournalRevision(100));
        edit.kind = JournalWriteKind::Collect;
        let before = image(&f).await;
        edit.inject = Some(JournalCommitFailurePoint::AfterIdempotency);
        assert!(fresh.apply(edit.clone()).await.is_err());
        assert_eq!(image(&f).await, before);
        edit.inject = None;
        fresh.apply(edit.clone()).await.unwrap();
        assert!(
            fresh
                .operation(&d, &s, &commits[68].batch.operation)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            fresh
                .operation(&d, &s, &commits[69].batch.operation)
                .await
                .unwrap()
                .is_some()
        );
        edit.expect = edit.record.fence();
        edit.record.generation += 1;
        edit.record.writer_epoch += 1;
        fresh.apply(edit).await.unwrap();
        assert!(operation_rows(&f, &d, &s).await.is_empty());
        assert_eq!(operation_rows(&f, &outsider_domain, &s).await, outsider);
        assert_eq!(
            operation_rows(&f, &d, &SessionId::new("window-other")).await,
            other_session
        );
    }
}

#[tokio::test]
async fn import_and_operation_pack_caps() {
    const CAP: usize = 1024 * 1024;
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("caps");
    let imported: Vec<_> = (0..7)
        .map(|i| object(format!("{i}{}", "import".repeat(70_000))))
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(write(&d, s.as_str(), imported))
        .await
        .unwrap();
    assert!(pack_rows(&f, &d, &s).await.len() >= 4);
    assert!(
        pack_rows(&f, &d, &s)
            .await
            .iter()
            .all(|r| r.3 as usize <= CAP)
    );
    let mut oversized = edit(&raw, &d, &s).await;
    oversized.objects.objects = vec![object("X".repeat(CAP))];
    let before = image(&f).await;
    assert!(raw.apply(oversized).await.is_err());
    assert_eq!(image(&f).await, before);
    let s = SessionId::new("operation-overflow");
    let mut initial = write(&d, s.as_str(), vec![]);
    initial.operation = None;
    MappingStorage::new(raw.clone(), f.clone())
        .apply(initial)
        .await
        .unwrap();
    for revision in 1..=4 {
        let mut add = edit(&raw, &d, &s).await;
        add.record.revision = JournalRevision(revision);
        let mut result = head(&d, &s, JournalObjectId::from_bytes([0; 32]));
        result.revision = JournalRevision(revision);
        add.operation = Some(JournalOperationRecord {
            operation: JournalOperationId::new(format!("{revision}{}", "operation".repeat(40_000))),
            digest: journal_history_chain(std::iter::empty()),
            revision: JournalRevision(revision),
            result,
        });
        raw.apply(add).await.unwrap();
        for (tier, count, len) in raw.operation_pack_layout(&d, &s).await.unwrap() {
            assert!(len <= CAP);
            if tier < 64 {
                assert_eq!(count, 1 << tier);
            }
        }
    }
    let before = operation_rows(&f, &d, &s).await;
    assert!(before.keys().any(|t| *t >= 64));
    let mut prune = edit(&raw, &d, &s).await;
    prune.kind = JournalWriteKind::Collect;
    prune.prune_operations_before = Some(JournalRevision(2));
    raw.apply(prune).await.unwrap();
    let after = operation_rows(&f, &d, &s).await;
    assert_eq!(before.get(&64), after.get(&64));
    for (tier, count, len) in raw.operation_pack_layout(&d, &s).await.unwrap() {
        assert!(len <= CAP);
        if tier < 64 {
            assert_eq!(count, 1 << tier);
        }
    }
    let mut huge = edit(&raw, &d, &s).await;
    huge.operation = Some(JournalOperationRecord {
        operation: JournalOperationId::new("x".repeat(CAP)),
        digest: journal_history_chain(std::iter::empty()),
        revision: JournalRevision(5),
        result: head(&d, &s, JournalObjectId::from_bytes([0; 32])),
    });
    let before = image(&f).await;
    state_conflict(raw.apply(huge).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    let fresh = reopen(&f).await;
    assert_eq!(
        fresh.operation_pack_layout(&d, &s).await.unwrap(),
        raw.operation_pack_layout(&d, &s).await.unwrap()
    );
}

/// S4/S5 against a plain map model (the volatile reference's rule): binary
/// carry and capped overflow under many appends, exact/conflicting replays,
/// explicit inclusive prune floors with the single current-epoch exception,
/// writer-epoch changes and independent-pool reopen. Every step compares every
/// id ever written and checks the sealed tier shape and 1 MiB cap.
#[tokio::test]
async fn operation_packs_match_model() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("ops-model");
    let mut seed_write = write(&d, s.as_str(), vec![object("ops-model")]);
    seed_write.operation = None;
    MappingStorage::new(raw.clone(), f.clone())
        .apply(seed_write)
        .await
        .unwrap();
    let mut st = raw.clone();
    let mut record = st.load(&d, &s).await.unwrap().unwrap();
    let mut model = std::collections::BTreeMap::<String, JournalOperationRecord>::new();
    let mut seen = Vec::<String>::new();
    let mut state = 0x1234_5678_9abc_def1u64;
    let (mut revision, mut epoch) = (record.revision.0, record.writer_epoch);
    let (mut overflow_rows, mut max_rows, mut conflicts) = (0usize, 0usize, 0usize);
    for step in 0..300u32 {
        let roll = xorshift(&mut state) % 100;
        let mut w = control(&d, &s, record.clone());
        w.kind = JournalWriteKind::Commit;
        // Phase one fills large records until carries overflow the cap;
        // phase two mixes small appends, replays, prunes and epoch changes.
        let large = step < 72;
        if !large && roll < 12 {
            let floor = if roll < 8 {
                revision.saturating_sub(64)
            } else {
                xorshift(&mut state) % (revision + 5)
            };
            if roll % 3 == 0 {
                epoch += 1;
                w.record.writer_epoch = epoch;
            }
            w.kind = JournalWriteKind::Collect;
            w.prune_operations_before = Some(JournalRevision(floor));
            st.apply(w.clone()).await.unwrap();
            let last = model
                .values()
                .filter(|o| o.result.writer_epoch == epoch)
                .map(|o| o.revision)
                .max();
            model.retain(|_, o| {
                o.revision.0 >= floor
                    || (o.result.writer_epoch == epoch && Some(o.revision) == last)
            });
            record = w.record;
        } else if !large && roll < 20 && !model.is_empty() {
            let pick = model
                .keys()
                .nth(xorshift(&mut state) as usize % model.len())
                .unwrap()
                .clone();
            let old = model[&pick].clone();
            if roll < 16 {
                // Identical replay is a no-op on the operation packs.
                let before = operation_rows(&f, &d, &s).await;
                w.operation = Some(old);
                st.apply(w.clone()).await.unwrap();
                assert_eq!(operation_rows(&f, &d, &s).await, before);
                record = w.record;
            } else {
                // Different input under a retained id conflicts with no effects.
                let mut changed = old.clone();
                changed.result.batch_sequence += 1;
                w.operation = Some(changed);
                let before = image(&f).await;
                state_conflict(st.apply(w).await.unwrap_err());
                assert_eq!(image(&f).await, before);
                conflicts += 1;
            }
        } else if !large && roll < 23 {
            epoch += 1;
            w.record.writer_epoch = epoch;
            st.apply(w.clone()).await.unwrap();
            record = w.record;
        } else if !large && roll < 26 {
            st = reopen(&f).await;
        } else {
            revision += 1;
            let size = if large {
                16 * 1024 + xorshift(&mut state) as usize % 512
            } else {
                xorshift(&mut state) as usize % 2048
            };
            let id = format!("op-{step:04}-{}", "p".repeat(size));
            let op = model_operation(&d, &s, &id, revision, epoch, step as u64);
            w.record.revision = JournalRevision(revision);
            w.operation = Some(op.clone());
            st.apply(w.clone()).await.unwrap();
            model.insert(id.clone(), op);
            seen.push(id);
            record = w.record;
        }
        if large && step % 4 != 3 {
            continue;
        }
        let layout = st.operation_pack_layout(&d, &s).await.unwrap();
        let mut total = 0;
        for (tier, count, bytes) in &layout {
            assert!(
                *bytes <= 1024 * 1024,
                "step {step}: tier {tier} holds {bytes} bytes"
            );
            if *tier < 64 {
                assert_eq!(*count, 1usize << tier, "step {step}: tier {tier}");
            }
            total += count;
        }
        assert_eq!(total, model.len(), "step {step}");
        overflow_rows = overflow_rows.max(layout.iter().filter(|l| l.0 >= 64).count());
        max_rows = max_rows.max(layout.len());
        if step % 10 == 3 || step >= 290 {
            for id in &seen {
                let id_key = JournalOperationId::new(id.as_str());
                assert_eq!(
                    st.operation(&d, &s, &id_key).await.unwrap().as_ref(),
                    model.get(id),
                    "step {step}"
                );
            }
        }
    }
    assert!(
        overflow_rows > 0,
        "the cap never forced an overflow segment"
    );
    assert!(conflicts > 0);
    // Steady state under the S5 window stays a handful of sealed rows.
    let final_rows = st.operation_pack_layout(&d, &s).await.unwrap().len();
    eprintln!(
        "operation model: {} retained, {final_rows} rows, max {max_rows} rows, max {overflow_rows} overflow rows, {conflicts} conflicts",
        model.len()
    );
}

/// Operation tiers use the same bounded inflation as pack bodies: a frame
/// declaring a window above 2 MiB or inflating past the 1 MiB record cap is
/// refused even though AEAD accepts the resealed unit.
#[tokio::test]
async fn operation_frames_are_bounded() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("operation-frames");
    f.seed_journal(&raw, write(&d, s.as_str(), vec![object("frames")]))
        .await
        .unwrap();
    let op = JournalOperationId::new("operation");
    let fresh = || ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
    let original = fresh().operation(&d, &s, &op).await.unwrap().unwrap();
    let pool = f.database().pool().clone();
    let saved: (Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT nonce,payload FROM agent_session_journal_operation_pack WHERE runtime_session_id='operation-frames' AND tier=0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let restore = || async {
        sqlx::query("UPDATE agent_session_journal_operation_pack SET nonce=?,payload=? WHERE runtime_session_id='operation-frames' AND tier=0")
            .bind(&saved.0).bind(&saved.1).execute(&pool).await.unwrap();
    };
    // Valid records in a raw-block frame that declares the given window.
    let window = |descriptor: u8| {
        move |bytes: &mut Vec<u8>| {
            let records = zstd::stream::decode_all(bytes.as_slice()).unwrap();
            let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, descriptor];
            frame.extend_from_slice(&(((records.len() as u32) << 3) | 1).to_le_bytes()[..3]);
            frame.extend_from_slice(&records);
            *bytes = frame;
        }
    };
    raw.reseal_unit_for_test(&d, &s, TestSealedUnit::OperationsFrame(0), window(11 << 3))
        .await
        .unwrap();
    assert_eq!(
        fresh().operation(&d, &s, &op).await.unwrap(),
        Some(original.clone())
    );
    for descriptor in [(11 << 3) | 1, 12 << 3] {
        restore().await;
        raw.reseal_unit_for_test(
            &d,
            &s,
            TestSealedUnit::OperationsFrame(0),
            window(descriptor),
        )
        .await
        .unwrap();
        assert!(fresh().operation(&d, &s, &op).await.is_err());
    }
    // Inflation past the 1 MiB serialized-record cap.
    restore().await;
    raw.reseal_unit_for_test(&d, &s, TestSealedUnit::OperationsFrame(0), |bytes| {
        *bytes = zstd::stream::encode_all(vec![b' '; 1024 * 1024 + 1].as_slice(), 3).unwrap();
    })
    .await
    .unwrap();
    assert!(fresh().operation(&d, &s, &op).await.is_err());
    restore().await;
    assert_eq!(
        fresh().operation(&d, &s, &op).await.unwrap(),
        Some(original)
    );
}
