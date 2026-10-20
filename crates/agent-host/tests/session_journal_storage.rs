use agent_runtime::core::{
    clock::Timestamp,
    error::{ErrorKind, FailureClass, FailureComponent, FailureStage},
    ids::SessionId,
    journal::*,
};
use forge_agent_host::{
    session_journal::ForgeJournalStorage,
    test_support::{FileDb, MappingStorage},
};
use sqlx::Row;
use std::sync::Arc;

fn domain(role: JournalRole) -> JournalDomain {
    JournalDomain {
        storage_id: JournalStorageId::new("forge.storage-tests"),
        role,
    }
}
fn object(value: impl serde::Serialize) -> JournalStoredObject {
    let o = JournalObject::encode(JournalObjectKind::JsonInline, &value).unwrap();
    JournalStoredObject {
        id: o.id,
        kind: JournalObjectKind::JsonInline,
        bytes: o.bytes.into(),
    }
}
fn engine() -> JournalEngineState {
    JournalEngineState {
        version: u32::MAX,
        bytes: vec![0, 255, 128, 0, 17],
    }
}
fn head(d: &JournalDomain, s: &SessionId, id: JournalObjectId) -> JournalHead {
    JournalHead {
        schema_version: JOURNAL_SCHEMA_VERSION,
        storage_id: d.storage_id.clone(),
        role: d.role,
        session: s.clone(),
        revision: JournalRevision(7),
        writer_epoch: 11,
        batch_sequence: 3,
        batch: id,
        snapshot: JournalSnapshotRoots {
            history: Default::default(),
            history_chain: journal_history_chain(std::iter::empty()),
            usage: Default::default(),
            extensions: Default::default(),
            manifests: Default::default(),
            planned_step_count: 23,
            identity: Default::default(),
            updated: Timestamp(123),
        },
        checkpoint: None,
        superseded_by: None,
        retirement: None,
        created: Timestamp(12),
        updated: Timestamp(124),
    }
}
fn write(d: &JournalDomain, s: &str, mut objects: Vec<JournalStoredObject>) -> JournalWrite {
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    let session = SessionId::new(s);
    let h = head(
        d,
        &session,
        objects
            .first()
            .map(|o| o.id.clone())
            .unwrap_or_else(|| JournalObjectId::from_bytes([9; 32])),
    );
    let ho = JournalObject::encode(JournalObjectKind::Head, &h).unwrap();
    JournalWrite {
        domain: d.clone(),
        session,
        kind: JournalWriteKind::Commit,
        expect: Default::default(),
        record: JournalSessionRecord {
            generation: 1,
            revision: h.revision,
            writer_epoch: h.writer_epoch,
            head: Some(ho.clone()),
            has_checkpoint: false,
            retirement: None,
            collection_dirty: true,
            engine: engine(),
        },
        objects: JournalObjectBatch { objects },
        operation: Some(JournalOperationRecord {
            operation: JournalOperationId::new("operation"),
            digest: journal_history_chain(std::iter::empty()),
            revision: h.revision,
            result: h,
        }),
        pins: vec![JournalPinChange::Put(JournalPinRecord {
            pin: JournalPinId::new("pin"),
            blocks_retirement: true,
            roots: vec![ho.id.clone()],
            engine: engine(),
        })],
        catalog: vec![JournalCatalogChange::Put(JournalCatalogEntry {
            head: ho.id,
            engine: engine(),
        })],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
async fn storage(f: &FileDb) -> Arc<ForgeJournalStorage> {
    Arc::new(ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap())
}
async fn reopen(f: &FileDb) -> Arc<ForgeJournalStorage> {
    Arc::new(ForgeJournalStorage::new(f.reopen().await.unwrap(), [7; 32], 1).unwrap())
}

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
    let row=sqlx::query("SELECT pack_id,retained_canonical_bytes FROM agent_session_journal_pack WHERE role='protected' AND runtime_session_id='owner'").fetch_one(f.database().pool()).await.unwrap();
    let pack_id: i64 = row.get("pack_id");
    let retained: i64 = row.get("retained_canonical_bytes");
    sqlx::query("UPDATE agent_session_journal_pack SET retained_canonical_bytes=? WHERE pack_id=?")
        .bind(retained + 1)
        .bind(pack_id)
        .execute(f.database().pool())
        .await
        .unwrap();
    assert_eq!(
        storage.retained_bytes(&d).await.unwrap_err().kind,
        ErrorKind::Serialization
    );
    assert_eq!(
        storage.retained_bytes(&ordinary).await.unwrap(),
        Some(total)
    );
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
    sqlx::query("UPDATE agent_session_journal_membership SET bitmap=? WHERE pack_id=?")
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

fn state_conflict(error: agent_runtime::core::error::RuntimeError) {
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.class,
        FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint
        }
    );
}
fn control(d: &JournalDomain, s: &SessionId, record: JournalSessionRecord) -> JournalWrite {
    let expect = record.fence();
    let mut record = record;
    record.generation = expect.generation.checked_add(1).unwrap();
    JournalWrite {
        domain: d.clone(),
        session: s.clone(),
        kind: JournalWriteKind::Pin,
        expect,
        record,
        objects: Default::default(),
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
async fn image(f: &FileDb) -> Vec<Vec<String>> {
    let db = f.database();
    let mut result = Vec::new();
    for table in [
        "session",
        "pack",
        "membership",
        "pin",
        "catalog",
        "operation_pack",
    ] {
        let table = format!("agent_session_journal_{table}");
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        let quoted = columns
            .into_iter()
            .map(|r| format!("quote({})", r.get::<String, _>("name")))
            .collect::<Vec<_>>()
            .join(",");
        result.push(
            sqlx::query_scalar(&format!(
                "SELECT json_array({quoted}) FROM {table} ORDER BY 1"
            ))
            .fetch_all(db.pool())
            .await
            .unwrap(),
        );
    }
    result.push(
        sqlx::query_scalar(
            "SELECT quote(seq) FROM sqlite_sequence WHERE name='agent_session_journal_pack'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap(),
    );
    result
}
fn cut_objects(w: &mut JournalWrite) {
    for kind in [JournalObjectKind::Checkpoint, JournalObjectKind::Batch] {
        let o = JournalObject::encode(kind, &serde_json::json!({"stage":kind.as_str()})).unwrap();
        w.objects.objects.push(JournalStoredObject {
            id: o.id,
            kind,
            bytes: o.bytes.into(),
        });
    }
    let o = w.record.head.as_ref().unwrap();
    w.objects.objects.push(JournalStoredObject {
        id: o.id.clone(),
        kind: JournalObjectKind::Head,
        bytes: o.bytes.clone().into(),
    });
    w.objects.objects.sort_by(|a, b| a.id.cmp(&b.id));
}

#[tokio::test]
async fn apply_atomic_cuts() {
    let f = FileDb::new().await.unwrap();
    let inner = storage(&f).await;
    let mapped = MappingStorage::new(inner.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    for existing in [false, true] {
        for &cut in JournalCommitFailurePoint::ALL {
            let sid = format!("atomic-{existing}-{cut:?}");
            let session = SessionId::new(&sid);
            f.ensure_session(&session).await.unwrap();
            let old = object("old immutable body");
            let before_record = if existing {
                let base = write(&d, &sid, vec![old.clone()]);
                mapped.apply(base.clone()).await.unwrap();
                Some(base.record)
            } else {
                None
            };
            let new = object(format!("new-{cut:?}"));
            let mut w = write(&d, &sid, vec![new.clone()]);
            cut_objects(&mut w);
            if let Some(old) = &before_record {
                w.expect = old.fence();
                w.record.generation = old.generation + 1;
            }
            w.operation.as_mut().unwrap().operation = JournalOperationId::new("next-operation");
            w.inject = Some(cut);
            let before = image(&f).await;
            let error = mapped.apply(w.clone()).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::Internal);
            assert!(error.retryable);
            assert_eq!(
                error.class,
                FailureClass::HostComponent {
                    stage: FailureStage::PreProvider,
                    component: FailureComponent::Checkpoint
                }
            );
            let reopened = reopen(&f).await;
            if cut.is_after_commit() {
                assert_ne!(image(&f).await, before);
                assert_eq!(
                    reopened.load(&d, &session).await.unwrap(),
                    Some(w.record.clone())
                );
                let bodies = reopened
                    .objects(
                        &d,
                        &session,
                        &w.objects
                            .objects
                            .iter()
                            .map(|o| o.id.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .unwrap();
                assert!(bodies.iter().all(Option::is_some));
                assert_eq!(
                    reopened
                        .operation(&d, &session, &w.operation.as_ref().unwrap().operation)
                        .await
                        .unwrap(),
                    w.operation
                );
                let packs: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id=?",
                )
                .bind(&sid)
                .fetch_one(f.database().pool())
                .await
                .unwrap();
                assert_eq!(
                    packs,
                    if existing { 2 } else { 1 },
                    "kind stages must publish one final new pack"
                );
                state_conflict(mapped.apply(w.clone()).await.unwrap_err());
            } else {
                assert_eq!(
                    image(&f).await,
                    before,
                    "all rows, ciphertexts, bitmaps and allocated ids roll back at {cut:?}"
                );
                assert_eq!(reopened.load(&d, &session).await.unwrap(), before_record);
                assert_eq!(
                    inner
                        .objects(&d, &session, std::slice::from_ref(&new.id))
                        .await
                        .unwrap(),
                    vec![None]
                );
                assert!(
                    reopened
                        .operation(&d, &session, &JournalOperationId::new("next-operation"))
                        .await
                        .unwrap()
                        .is_none()
                );
                let mut retry = w;
                retry.inject = None;
                mapped.apply(retry.clone()).await.unwrap();
                assert_eq!(
                    reopened.load(&d, &session).await.unwrap(),
                    Some(retry.record)
                );
            }
            if existing {
                assert_eq!(
                    inner
                        .objects(&d, &session, std::slice::from_ref(&old.id))
                        .await
                        .unwrap(),
                    vec![Some(old.bytes)]
                );
            }
        }
    }
    // A large multi-kind apply segments its final S1 output at the logical cap.
    let large: Vec<_> = (0..3)
        .map(|n| object(format!("{n}{}", "x".repeat(600_000))))
        .collect();
    let mut w = write(&d, "capped-import", large);
    cut_objects(&mut w);
    mapped.apply(w.clone()).await.unwrap();
    let sizes:Vec<i64>=sqlx::query_scalar("SELECT retained_canonical_bytes FROM agent_session_journal_pack WHERE runtime_session_id='capped-import'").fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(sizes.len(), 3);
    assert!(sizes.iter().all(|s| *s > 0 && *s <= 1_048_576));
    assert_eq!(
        inner
            .objects(
                &d,
                &w.session,
                &w.objects
                    .objects
                    .iter()
                    .map(|o| o.id.clone())
                    .collect::<Vec<_>>()
            )
            .await
            .unwrap(),
        w.objects
            .objects
            .iter()
            .map(|o| Some(o.bytes.clone()))
            .collect::<Vec<_>>()
    );
    // Oversized and malformed immutable candidates fail without any SQL effects.
    let mut bad = write(&d, "oversized", vec![object("small")]);
    bad.objects.objects[0].bytes = vec![0; 1_048_577].into();
    f.ensure_session(&bad.session).await.unwrap();
    let before = image(&f).await;
    state_conflict(mapped.apply(bad).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    let mut bad = write(&d, "bad-id", vec![object("bytes")]);
    bad.objects.objects[0].id = JournalObjectId::from_bytes([0; 32]);
    f.ensure_session(&bad.session).await.unwrap();
    state_conflict(mapped.apply(bad).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // Trace the lost acknowledgement too: commit happened, the caller saw an
    // error, and lock hold remains measured without leaking session/body data.
    use tracing::instrument::WithSubscriber;
    let events = ApplyEvents::default();
    let mut traced = write(
        &d,
        "trace-private-session",
        vec![object("trace-private-value")],
    );
    f.ensure_session(&traced.session).await.unwrap();
    traced.inject = Some(JournalCommitFailurePoint::AfterCommit);
    assert!(
        inner
            .apply(traced)
            .with_subscriber(events.clone())
            .await
            .is_err()
    );
    {
        let captured = events.0.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let event = &captured[0];
        assert_eq!(
            event.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![
                "committed",
                "duration_us",
                "kind",
                "lock_hold_us",
                "message",
                "reclaimed_bytes",
                "reclaimed_objects",
                "success"
            ]
        );
        assert_eq!(event["committed"], "true");
        assert_eq!(event["success"], "false");
        assert!(event["lock_hold_us"].parse::<u64>().unwrap() > 0);
        assert!(
            event["duration_us"].parse::<u64>().unwrap()
                >= event["lock_hold_us"].parse::<u64>().unwrap()
        );
        assert!(!format!("{event:?}").contains("private"));
    }
    // Operation conflicts are atomic after all earlier publication stages.
    let original = write(&d, "operation-conflict", vec![object("one")]);
    mapped.apply(original.clone()).await.unwrap();
    let mut changed = write(&d, "operation-conflict", vec![object("two")]);
    changed.expect = original.record.fence();
    changed.record.generation = 2;
    let before = image(&f).await;
    state_conflict(mapped.apply(changed).await.unwrap_err());
    assert_eq!(image(&f).await, before);
}

#[tokio::test]
async fn independent_connection_expect_fences() {
    let f = FileDb::new().await.unwrap();
    let a = storage(&f).await;
    let b = reopen(&f).await;
    let ma = MappingStorage::new(a.clone(), f.clone());
    let mb = MappingStorage::new(b.clone(), f.clone());
    let d = domain(JournalRole::Protected);
    let initial = write(&d, "fences", vec![object("original")]);
    ma.apply(initial.clone()).await.unwrap();
    let session = &initial.session;
    let record = b.load(&d, session).await.unwrap().unwrap();
    let mut first = control(&d, session, record.clone());
    first.pins = vec![JournalPinChange::Put(JournalPinRecord {
        pin: JournalPinId::new("concurrent-a"),
        blocks_retirement: false,
        roots: vec![],
        engine: engine(),
    })];
    let mut second = first.clone();
    let JournalPinChange::Put(p) = &mut second.pins[0] else {
        unreachable!()
    };
    p.pin = JournalPinId::new("concurrent-b");
    let (ra, rb) = tokio::join!(ma.apply(first), mb.apply(second));
    assert_ne!(ra.is_ok(), rb.is_ok());
    if let Err(e) = ra {
        state_conflict(e);
    }
    if let Err(e) = rb {
        state_conflict(e);
    }
    let current = a.load(&d, session).await.unwrap().unwrap();
    assert_eq!(current.generation, 2);
    assert_eq!(current.revision, record.revision);
    assert_eq!(current.writer_epoch, record.writer_epoch);
    let pins = a.pins(&d, session).await.unwrap();
    assert_eq!(pins.len(), 2);
    // Each fence coordinate independently guards the transaction.
    for coordinate in 0..3 {
        let mut stale = control(&d, session, current.clone());
        match coordinate {
            0 => stale.expect.revision.0 += 1,
            1 => stale.expect.writer_epoch += 1,
            _ => {
                stale.expect.generation -= 1;
                stale.record.generation = stale.expect.generation + 1;
            }
        }
        let before = image(&f).await;
        state_conflict(mb.apply(stale).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    for generation in [current.generation, current.generation + 2] {
        let mut bad = control(&d, session, current.clone());
        bad.record.generation = generation;
        let before = image(&f).await;
        state_conflict(ma.apply(bad).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    // Another Forge subsystem's write is not a journal semantic conflict.
    sqlx::query("UPDATE agent_session SET updated_at='unrelated-subsystem' WHERE runtime_session_id='fences'").execute(f.database().pool()).await.unwrap();
    let mut exact = control(&d, session, current.clone());
    exact.record.revision = JournalRevision(u64::MAX);
    exact.record.writer_epoch = u64::MAX;
    mb.apply(exact.clone()).await.unwrap();
    assert_eq!(
        a.load(&d, session).await.unwrap(),
        Some(exact.record.clone())
    );
    // Warm indexes survive commits; only one new index is decoded per new pack.
    let id = initial.objects.objects[0].id.clone();
    a.objects(&d, session, std::slice::from_ref(&id))
        .await
        .unwrap();
    b.objects(&d, session, std::slice::from_ref(&id))
        .await
        .unwrap();
    let mut latest = exact.record;
    for n in 0..8 {
        let o = object(format!("independent-admission-{n}"));
        let mut next = control(&d, session, latest.clone());
        next.objects.objects = vec![o.clone()];
        let before = a.cache_stats().unwrap();
        ma.apply(next.clone()).await.unwrap();
        let after = a.cache_stats().unwrap();
        assert_eq!(after.index_decodes, before.index_decodes + 1);
        assert_eq!(
            after.body_decodes, before.body_decodes,
            "warm commit must not hydrate the closure"
        );
        latest = next.record;
        // B's directory is stale. It refreshes inside its own write transaction,
        // finds A's body and reuses that slot instead of adding another copy.
        let mut duplicate = control(&d, session, latest.clone());
        duplicate.objects.objects = vec![o.clone()];
        mb.apply(duplicate.clone()).await.unwrap();
        latest = duplicate.record;
        assert_eq!(
            b.objects(&d, session, std::slice::from_ref(&o.id))
                .await
                .unwrap(),
            vec![Some(o.bytes)]
        );
    }
    let packs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agent_session_journal_pack WHERE runtime_session_id='fences'",
    )
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(packs, 9);
    // Pin/catalog Put/Remove participate in the fenced transaction verbatim.
    let mut remove = control(&d, session, latest.clone());
    remove.pins = vec![JournalPinChange::Remove(JournalPinId::new("pin"))];
    remove.catalog = vec![JournalCatalogChange::Remove(
        initial.record.head.as_ref().unwrap().id.clone(),
    )];
    ma.apply(remove.clone()).await.unwrap();
    latest = remove.record;
    assert_eq!(a.pins(&d, session).await.unwrap().len(), 1);
    assert!(
        b.catalog(&d, session, &initial.record.head.as_ref().unwrap().id)
            .await
            .unwrap()
            .is_none()
    );
    // An absent record accepts only the complete zero fence.
    for coordinate in 0..3 {
        let mut absent = write(&d, &format!("absent-{coordinate}"), vec![]);
        match coordinate {
            0 => absent.expect.revision.0 = 1,
            1 => absent.expect.writer_epoch = 1,
            _ => {
                absent.expect.generation = 1;
                absent.record.generation = 2;
            }
        }
        f.ensure_session(&absent.session).await.unwrap();
        let before = image(&f).await;
        state_conflict(ma.apply(absent).await.unwrap_err());
        assert_eq!(image(&f).await, before);
    }
    // Full-width generation exhaustion cannot wrap through a zero successor.
    let mut exhausted = write(&d, "generation-exhausted", vec![]);
    exhausted.record.generation = u64::MAX;
    f.seed_journal(&a, exhausted.clone()).await.unwrap();
    exhausted.expect = exhausted.record.fence();
    exhausted.record.generation = 0;
    let before = image(&f).await;
    state_conflict(ma.apply(exhausted).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // A changed immutable body under the same full id remains a real conflict.
    let mut immutable = control(&d, session, latest);
    let mut o = initial.objects.objects[0].clone();
    o.bytes = object("different bytes").bytes;
    immutable.objects.objects = vec![o];
    let before = image(&f).await;
    state_conflict(ma.apply(immutable).await.unwrap_err());
    assert_eq!(image(&f).await, before);
    // Missing/ambiguous/changed host mappings are host failures, not conflicts.
    let missing = write(&d, "missing-mapping", vec![]);
    let before = image(&f).await;
    assert_eq!(
        a.apply(missing).await.unwrap_err().kind,
        ErrorKind::Internal
    );
    assert_eq!(image(&f).await, before);
    let duplicate_session = SessionId::new("ambiguous-mapping");
    f.ensure_session(&duplicate_session).await.unwrap();
    sqlx::query("INSERT INTO agent_session(id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('duplicate-host','journal-agent','journal-profile','journal-scope','native','ambiguous-mapping','suspended','now','now')").execute(f.database().pool()).await.unwrap();
    assert_eq!(
        a.apply(write(&d, "ambiguous-mapping", vec![]))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Internal
    );
    assert_eq!(image(&f).await, before);
    let mut moved = write(&d, "moved-mapping", vec![]);
    ma.apply(moved.clone()).await.unwrap();
    sqlx::query(
        "UPDATE agent_session SET runtime_session_id=NULL WHERE runtime_session_id='moved-mapping'",
    )
    .execute(f.database().pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO agent_session(id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES ('replacement-host','journal-agent','journal-profile','journal-scope','native','moved-mapping','suspended','now','now')").execute(f.database().pool()).await.unwrap();
    moved.expect = moved.record.fence();
    moved.record.generation += 1;
    let before = image(&f).await;
    assert_eq!(a.apply(moved).await.unwrap_err().kind, ErrorKind::Internal);
    assert_eq!(image(&f).await, before);
}

// Capture just the journal target without a new logging dependency. The field
// whitelist proves that ids, keys and decrypted values never enter apply traces.
type ApplyEvent = std::collections::BTreeMap<String, String>;
#[derive(Clone, Default)]
struct ApplyEvents(Arc<std::sync::Mutex<Vec<ApplyEvent>>>);
impl tracing::Subscriber for ApplyEvents {
    fn enabled(&self, m: &tracing::Metadata<'_>) -> bool {
        m.target() == "forge_session_journal"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(std::collections::BTreeMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.insert(f.name().into(), format!("{v:?}"));
            }
        }
        let mut fields = Fields(Default::default());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
