use agent_runtime::core::{
    clock::Timestamp,
    error::{ErrorKind, FailureClass, FailureComponent, FailureStage},
    ids::SessionId,
    journal::*,
};
use forge_agent_host::{
    session_journal::{ForgeJournalStorage, TestSealedUnit},
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
            .with_subscriber(events.dispatch())
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
impl ApplyEvents {
    /// With exactly one scoped dispatcher registered, tracing-core resolves a
    /// callsite's first registration through the registering thread's default
    /// ("just one" mode). A parallel test reaching the apply event first then
    /// caches `Interest::never` and this capture silently sees no event. A
    /// second, permanently registered dispatcher that enables nothing forces
    /// `Interest::sometimes`, so every event is checked against the current
    /// thread's dispatcher. It is registered before the capture so the level
    /// filter only opens once both are present.
    fn dispatch(&self) -> tracing::Dispatch {
        static SILENT: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        SILENT.get_or_init(|| tracing::Dispatch::new(Silent));
        tracing::Dispatch::new(self.clone())
    }
}
struct Silent;
impl tracing::Subscriber for Silent {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::never()
    }
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::OFF)
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
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

fn engine_config() -> JournalEngineConfig {
    JournalEngineConfig {
        collection_enabled: true,
        validated_root_memo_bytes: 128 * 1024 * 1024,
    }
}
fn open_request(label: &str, root: JournalRootSelection, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root,
        expected_revision: None,
        operation: JournalOperationId::new(format!("{label}-open")),
        pin: JournalPinId::new(label),
        intent,
    }
}
// Public DTOs only: an empty snapshot has empty sequence/map roots. The engine
// encodes all durable control objects and computes its own dependency metadata.
fn empty_commit(
    s: &SessionId,
    epoch: u64,
    operation: &str,
    previous: Option<&JournalHead>,
    at: u64,
) -> JournalCommit {
    use agent_runtime::core::{store::SessionSnapshot, usage::UsageLedger};
    let roots = head(
        &domain(JournalRole::Protected),
        s,
        JournalObjectId::from_bytes([0; 32]),
    )
    .snapshot;
    let mut roots = roots;
    roots.planned_step_count = 0;
    roots.updated = Timestamp(at);
    let snapshot = SessionSnapshot {
        id: s.clone(),
        history: vec![],
        usage: UsageLedger::new(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: Timestamp(at),
    };
    let boundary = JournalBoundary {
        version: 1,
        session: s.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let batch = JournalBatch {
        session: s.clone(),
        sequence: previous.map_or(1, |h| h.batch_sequence + 1),
        predecessor: previous.map(|h| JournalDigest::parse(h.batch.as_str()).unwrap()),
        expected_revision: previous.map(|h| h.revision),
        writer_epoch: epoch,
        operation: JournalOperationId::new(operation),
        boundary: boundary.clone(),
        snapshot: roots,
        checkpoint: None,
    };
    let mut commit = JournalCommit {
        batch,
        digest: journal_history_chain(std::iter::empty()),
        objects: vec![],
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: vec![],
            usage_append: vec![],
            extension_updates: Default::default(),
            extension_references: Default::default(),
            manifests_append: vec![],
            manifest_references: vec![],
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

#[tokio::test]
async fn pins_catalog_stage_restart() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let mapped = Arc::new(MappingStorage::new(raw.clone(), f.clone()));
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("pins-stage");
    let engine = JournalEngine::protected(mapped, d.storage_id.clone(), engine_config());
    let writer = engine
        .open(
            &s,
            open_request(
                "retained",
                JournalRootSelection::Latest,
                JournalOpenIntent::ExclusiveWriter,
            ),
        )
        .await
        .unwrap();
    let first = engine
        .commit(
            &writer,
            empty_commit(&s, writer.writer_epoch().unwrap(), "first", None, 1),
        )
        .await
        .unwrap();
    engine.close(writer).await.unwrap();
    let reader = engine
        .open(
            &s,
            open_request(
                "retained",
                JournalRootSelection::Latest,
                JournalOpenIntent::ReadOnly,
            ),
        )
        .await
        .unwrap();
    engine.retain_root(&reader).await.unwrap();
    let leaf = JournalObject::encode(
        JournalObjectKind::Message,
        &agent_runtime::core::content::Message::user("staged leaf"),
    )
    .unwrap();
    engine
        .stage_leaves(&reader, JournalPinId::new("staging"), vec![leaf.clone()])
        .await
        .unwrap();
    engine.close(reader).await.unwrap();
    let writer = engine
        .open(
            &s,
            open_request(
                "advance",
                JournalRootSelection::Latest,
                JournalOpenIntent::ExclusiveWriter,
            ),
        )
        .await
        .unwrap();
    let second = engine
        .commit(
            &writer,
            empty_commit(
                &s,
                writer.writer_epoch().unwrap(),
                "second",
                Some(&first),
                2,
            ),
        )
        .await
        .unwrap();
    engine.close(writer).await.unwrap();
    let pins = raw.pins(&d, &s).await.unwrap();
    assert_eq!(pins.len(), 2);
    assert!(
        pins.iter()
            .any(|p| p.pin.as_str() == "retained" && !p.blocks_retirement)
    );
    assert!(
        pins.iter().any(|p| p.pin.as_str() == "staging"
            && p.roots.contains(&leaf.id)
            && p.blocks_retirement)
    );
    let root = JournalObject::encode(JournalObjectKind::Head, &first)
        .unwrap()
        .id;
    let catalog = raw.catalog(&d, &s, &root).await.unwrap().unwrap();
    drop(engine);
    let fresh = reopen(&f).await;
    assert_eq!(fresh.pins(&d, &s).await.unwrap(), pins);
    assert_eq!(fresh.catalog(&d, &s, &root).await.unwrap(), Some(catalog));
    assert_eq!(
        fresh
            .objects(&d, &s, std::slice::from_ref(&leaf.id))
            .await
            .unwrap(),
        vec![Some(Arc::from(leaf.bytes))]
    );
    let engine = JournalEngine::protected(
        Arc::new(MappingStorage::new(fresh.clone(), f.clone())),
        d.storage_id.clone(),
        engine_config(),
    );
    let retained = engine
        .open(
            &s,
            open_request(
                "read-retained",
                JournalRootSelection::Retained { root: root.clone() },
                JournalOpenIntent::ReadOnly,
            ),
        )
        .await
        .unwrap();
    assert_eq!(retained.head(), Some(&first));
    engine.close(retained).await.unwrap();
    let latest = engine
        .open(
            &s,
            open_request(
                "latest",
                JournalRootSelection::Latest,
                JournalOpenIntent::ReadOnly,
            ),
        )
        .await
        .unwrap();
    assert_eq!(latest.head(), Some(&second));
    engine.close(latest).await.unwrap();

    for p in pins {
        engine
            .release_pin(&s, &p.pin, engine.root_epoch().unwrap())
            .await
            .unwrap();
    }
    assert!(fresh.pins(&d, &s).await.unwrap().is_empty());
    // Unknown metadata and ordered Put/Remove are persisted verbatim by apply.
    let mut edit = write(&d, s.as_str(), vec![]);
    edit.expect = fresh.load(&d, &s).await.unwrap().unwrap().fence();
    edit.record.generation = edit.expect.generation + 1;
    edit.operation = None;
    let expected_pin = match edit.pins[0].clone() {
        JournalPinChange::Put(p) => p,
        _ => unreachable!(),
    };
    let expected_catalog = match edit.catalog[0].clone() {
        JournalCatalogChange::Put(c) => c,
        _ => unreachable!(),
    };
    fresh.apply(edit.clone()).await.unwrap();
    let reopened = reopen(&f).await;
    assert_eq!(
        reopened.load(&d, &s).await.unwrap().unwrap().engine,
        edit.record.engine
    );
    assert_eq!(
        reopened.pins(&d, &s).await.unwrap(),
        vec![expected_pin.clone()]
    );
    assert_eq!(
        reopened
            .catalog(&d, &s, &expected_catalog.head)
            .await
            .unwrap(),
        Some(expected_catalog.clone())
    );
    edit.expect = edit.record.fence();
    edit.record.generation += 1;
    edit.pins = vec![JournalPinChange::Remove(expected_pin.pin)];
    edit.catalog = vec![
        JournalCatalogChange::Remove(expected_catalog.head.clone()),
        JournalCatalogChange::Remove(root),
    ];
    reopened.apply(edit).await.unwrap();
    assert!(reopened.pins(&d, &s).await.unwrap().is_empty());
    assert!(
        reopened
            .catalog(&d, &s, &expected_catalog.head)
            .await
            .unwrap()
            .is_none()
    );
}

async fn operation_rows(
    f: &FileDb,
    d: &JournalDomain,
    s: &SessionId,
) -> std::collections::BTreeMap<i64, (Vec<u8>, Vec<u8>)> {
    sqlx::query("SELECT tier,nonce,payload FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY tier")
        .bind(d.storage_id.as_str()).bind(if d.role == JournalRole::Protected { "protected" } else { "ordinary" }).bind(s.as_str()).fetch_all(f.database().pool()).await.unwrap().into_iter().map(|r| (r.get("tier"), (r.get("nonce"), r.get("payload")))).collect()
}
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
        engine
            .collect(JournalGcRequest {
                storage_id: engine.storage_id(),
                expected_root_epoch: engine.root_epoch().unwrap(),
                max_objects: 0,
            })
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

type PackRows = Vec<(i64, Vec<u8>, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>;
async fn pack_rows(f: &FileDb, d: &JournalDomain, s: &SessionId) -> PackRows {
    sqlx::query("SELECT pack_id,generation,object_count,retained_canonical_bytes,index_nonce,index_payload,body_nonce,body_payload FROM agent_session_journal_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY pack_id")
        .bind(d.storage_id.as_str()).bind(if d.role == JournalRole::Protected { "protected" } else { "ordinary" }).bind(s.as_str()).fetch_all(f.database().pool()).await.unwrap().into_iter().map(|r| (r.get(0),r.get(1),r.get(2),r.get(3),r.get(4),r.get(5),r.get(6),r.get(7))).collect()
}
async fn edit(raw: &ForgeJournalStorage, d: &JournalDomain, s: &SessionId) -> JournalWrite {
    control(d, s, raw.load(d, s).await.unwrap().unwrap())
}
fn equal_objects(prefix: &str, count: usize) -> Vec<JournalStoredObject> {
    (0..count)
        .map(|i| object(format!("{prefix}-{i:03}-{}", "x".repeat(100))))
        .collect()
}
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
async fn independent_session_domain_copy_receipts() {
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
        assert_eq!(
            r,
            JournalApplyReceipt {
                reclaimed_objects: 8,
                reclaimed_bytes: length
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
        let remaining = raw.retained_bytes(&protected).await.unwrap().unwrap()
            + raw.retained_bytes(&ordinary).await.unwrap().unwrap();
        assert_eq!(length * 4 - remaining, totals.reclaimed_bytes);
    }
    assert_eq!(totals.reclaimed_objects, 32);
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
    let gc = engine
        .collect(JournalGcRequest {
            storage_id: engine.storage_id(),
            expected_root_epoch: engine.root_epoch().unwrap(),
            max_objects: u64::MAX,
        })
        .await
        .unwrap();
    assert!(gc.deleted > 0);
}

async fn add_objects(
    raw: &ForgeJournalStorage,
    d: &JournalDomain,
    s: &SessionId,
    mut objects: Vec<JournalStoredObject>,
) {
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    let mut add = edit(raw, d, s).await;
    add.objects.objects = objects;
    raw.apply(add).await.unwrap();
}
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
    let foreign_d = pack_rows(&f, &other_d, &s).await;
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
    assert_eq!(after.len(), 4);
    assert!(after.iter().all(|r| r.3 <= 1024 * 1024));
    assert_eq!(
        before - raw.retained_bytes(&d).await.unwrap().unwrap(),
        receipt.reclaimed_bytes
    );
    assert_eq!(pack_rows(&f, &other_d, &s).await, foreign_d);
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
    let foreign = pack_rows(&f, &other_d, &s).await;
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
    assert_eq!(pack_rows(&f, &other_d, &s).await, foreign);
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

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
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
            match &objects {
                Ok(found) => {
                    for (got, want) in found.iter().zip(&expected_objects) {
                        // Never different bytes; an index-only edit may hide
                        // a candidate (absent) until its body is decoded.
                        assert!(
                            got.is_none() || got == want,
                            "round {round}: wrong body served"
                        );
                        if got.is_none() {
                            false_absent += 1;
                        }
                    }
                }
                Err(_) => {}
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
                    assert!(retained.is_err() || retained.unwrap() == expected_retained);
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
    let pool = f.database().pool().clone();
    let b_owner: String = sqlx::query_scalar(
        "SELECT forge_session_id FROM agent_session_journal_session WHERE runtime_session_id='relabel-b'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let fresh = || ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
    assert!(fresh().objects(&d, &b, &[victim.id.clone()]).await.unwrap()[0].is_none());
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
    assert!(fresh().objects(&d, &b, &[victim.id.clone()]).await.is_err());
    assert!(fresh().members(&d, &b).await.is_err());
    assert!(fresh().retained_bytes(&d).await.is_err());
    // The same pack relabelled to the other role of B's session.
    sqlx::query("DELETE FROM agent_session_journal_membership WHERE pack_id=?")
        .bind(a_pack)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_session_journal_pack SET role='ordinary' WHERE pack_id=?")
        .bind(a_pack)
        .execute(&pool)
        .await
        .unwrap();
    let ordinary = domain(JournalRole::Ordinary);
    assert!(fresh().retained_bytes(&ordinary).await.is_err());
    assert!(fresh().retained_bytes(&d).await.is_ok());
}
