use super::*;

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

#[tokio::test]
async fn bounded_collection_upgrades_sealed_pin_order() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("pin-order-upgrade");
    let mut w = write(&d, s.as_str(), equal_objects("pins", 4));
    let JournalPinChange::Put(pin) = w.pins[0].clone() else {
        unreachable!()
    };
    w.pins = ["z-last", "a-first", "m-middle"]
        .into_iter()
        .map(|id| {
            JournalPinChange::Put(JournalPinRecord {
                pin: JournalPinId::new(id),
                ..pin.clone()
            })
        })
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(w)
        .await
        .unwrap();
    let sealed: Vec<(Vec<u8>, Vec<u8>)> =
        sqlx::query_as("SELECT nonce,payload FROM agent_session_journal_pin ORDER BY pin_key")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    sqlx::query("UPDATE agent_session_journal_pin SET pin_order=NULL")
        .execute(f.database().pool())
        .await
        .unwrap();
    let cold = reopen(&f).await;
    let writer = db::begin_immediate(f.database().pool()).await.unwrap();
    let record = tokio::time::timeout(std::time::Duration::from_secs(1), cold.load(&d, &s))
        .await
        .expect("load must not wait for the writer")
        .unwrap()
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM agent_session_journal_pin WHERE pin_order IS NULL"
        )
        .fetch_one(f.database().pool())
        .await
        .unwrap(),
        3
    );
    writer.rollback().await.unwrap();
    assert_eq!(cold.pins(&d, &s).await.unwrap().len(), 3);
    cold.apply(edit(&cold, &d, &s).await).await.unwrap();
    let fence = cold.load(&d, &s).await.unwrap().unwrap().fence();
    assert_eq!(fence.generation, record.generation + 1);
    let mut after = None;
    let mut pins = Vec::new();
    for _ in 0..3 {
        let page = cold
            .collection_page(
                &d,
                &s,
                JournalCollectionPageRequest {
                    kind: JournalCollectionPageKind::Pins,
                    fence,
                    after,
                    max_items: 1,
                },
            )
            .await
            .unwrap();
        let JournalCollectionItems::Pins(batch) = page.items else {
            panic!("pin page");
        };
        assert_eq!(batch.len(), 1);
        pins.extend(batch.into_iter().map(|p| p.pin.as_str().to_owned()));
        after = page.next;
    }
    assert!(after.is_none());
    assert_eq!(pins, ["a-first", "m-middle", "z-last"]);
    let unchanged: Vec<(Vec<u8>, Vec<u8>)> =
        sqlx::query_as("SELECT nonce,payload FROM agent_session_journal_pin ORDER BY pin_key")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    assert_eq!(
        sealed, unchanged,
        "upgrade changes no authenticated payload"
    );
}

#[tokio::test]
async fn bounded_pin_pages_reject_duplicate_and_reversed_ranks() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("invalid-pin-ranks");
    let mut w = write(&d, s.as_str(), equal_objects("pins", 4));
    let JournalPinChange::Put(template) = w.pins[0].clone() else {
        unreachable!()
    };
    w.pins = ["a", "b", "c"]
        .into_iter()
        .map(|id| {
            JournalPinChange::Put(JournalPinRecord {
                pin: JournalPinId::new(id),
                ..template.clone()
            })
        })
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(w)
        .await
        .unwrap();
    let fence = raw.load(&d, &s).await.unwrap().unwrap().fence();
    let saved: Vec<(Vec<u8>, i64)> = sqlx::query_as(
        "SELECT pin_key,pin_order FROM agent_session_journal_pin ORDER BY pin_order",
    )
    .fetch_all(f.database().pool())
    .await
    .unwrap();
    let request = JournalCollectionPageRequest {
        kind: JournalCollectionPageKind::Pins,
        fence,
        after: None,
        max_items: 1,
    };
    let first = raw.collection_page(&d, &s, request.clone()).await.unwrap();
    assert!(first.next.is_some());
    sqlx::query("UPDATE agent_session_journal_pin SET pin_order=? WHERE pin_key=?")
        .bind(saved[0].1)
        .bind(&saved[1].0)
        .execute(f.database().pool())
        .await
        .unwrap();
    assert_eq!(
        raw.collection_page(&d, &s, request.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Serialization,
        "lookahead must detect a duplicate at a one-item boundary"
    );
    assert_eq!(
        raw.collection_page(
            &d,
            &s,
            JournalCollectionPageRequest {
                after: first.next.clone(),
                ..request.clone()
            }
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Serialization,
        "a duplicated previous rank introduced between pages is unreadable"
    );
    sqlx::query("UPDATE agent_session_journal_pin SET pin_order=? WHERE pin_key=?")
        .bind(saved[1].1)
        .bind(&saved[1].0)
        .execute(f.database().pool())
        .await
        .unwrap();
    sqlx::query("UPDATE agent_session_journal_pin SET pin_order=? WHERE pin_key=?")
        .bind(saved[2].1 + 1)
        .bind(&saved[1].0)
        .execute(f.database().pool())
        .await
        .unwrap();
    assert_eq!(
        raw.collection_page(
            &d,
            &s,
            JournalCollectionPageRequest {
                after: first.next,
                ..request
            }
        )
        .await
        .unwrap_err()
        .kind,
        ErrorKind::Serialization,
        "decrypted ids must ascend across the continuation"
    );
}

#[tokio::test]
async fn pin_rank_writes_are_incremental_and_load_ignores_unreadable_rank() {
    let f = FileDb::new().await.unwrap();
    let raw = storage(&f).await;
    let d = domain(JournalRole::Protected);
    let s = SessionId::new("incremental-pin-ranks");
    let mut w = write(&d, s.as_str(), equal_objects("pins", 4));
    let JournalPinChange::Put(template) = w.pins[0].clone() else {
        unreachable!()
    };
    w.pins = (0..128)
        .map(|i| {
            JournalPinChange::Put(JournalPinRecord {
                pin: JournalPinId::new(format!("pin-{i:03}")),
                ..template.clone()
            })
        })
        .collect();
    MappingStorage::new(raw.clone(), f.clone())
        .apply(w)
        .await
        .unwrap();
    let before: Vec<(Vec<u8>, i64)> =
        sqlx::query_as("SELECT pin_key,pin_order FROM agent_session_journal_pin ORDER BY pin_key")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    let reads = raw.diagnostic_calls_for_test().1;
    let mut next = edit(&raw, &d, &s).await;
    next.pins = vec![JournalPinChange::Put(JournalPinRecord {
        pin: JournalPinId::new("pin-064-middle"),
        ..template
    })];
    raw.apply(next).await.unwrap();
    assert!(
        raw.diagnostic_calls_for_test().1 - reads <= 9,
        "one insertion uses logarithmic decrypts"
    );
    let after: Vec<(Vec<u8>, i64)> =
        sqlx::query_as("SELECT pin_key,pin_order FROM agent_session_journal_pin ORDER BY pin_key")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    assert!(
        before.iter().all(|entry| after.contains(entry)),
        "existing sparse ranks stay unchanged"
    );
    let record = raw.load(&d, &s).await.unwrap();
    sqlx::query("UPDATE agent_session_journal_pin SET pin_order=NULL,payload=zeroblob(length(payload)) WHERE pin_key=?")
        .bind(&before[0].0).execute(f.database().pool()).await.unwrap();
    assert_eq!(raw.load(&d, &s).await.unwrap(), record);
    assert_eq!(
        raw.pins(&d, &s).await.unwrap_err().kind,
        ErrorKind::Serialization
    );
}
