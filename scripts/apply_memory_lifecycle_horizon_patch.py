from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise RuntimeError(f"{path}: expected patch anchor not found")
    file_path.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "crates/db/src/sqlite/memory.rs",
    '''            let lifecycle = if query.include_retracted {
                String::new()
            } else {
                " AND NOT EXISTS (SELECT 1 FROM memory_lifecycle_assertion AS lifecycle WHERE lifecycle.memory_item_id = memory_item.id AND lifecycle.assertion_type IN ('superseded', 'retracted', 'expired'))".to_owned()
            };
''',
    '''            let lifecycle = if query.include_retracted {
                String::new()
            } else if query.not_after.is_some() {
                " AND NOT EXISTS (SELECT 1 FROM memory_lifecycle_assertion AS lifecycle WHERE lifecycle.memory_item_id = memory_item.id AND lifecycle.assertion_type IN ('superseded', 'retracted', 'expired') AND lifecycle.created_at <= ?)".to_owned()
            } else {
                " AND NOT EXISTS (SELECT 1 FROM memory_lifecycle_assertion AS lifecycle WHERE lifecycle.memory_item_id = memory_item.id AND lifecycle.assertion_type IN ('superseded', 'retracted', 'expired'))".to_owned()
            };
''',
)

replace_once(
    "crates/db/src/sqlite/memory.rs",
    '''            statement = statement.bind(query.identity_id.as_deref());
            if let Some(not_after) = query.not_after.as_deref() {
                statement = statement.bind(not_after);
            }
            statement = statement.bind(&fts_query);
''',
    '''            statement = statement.bind(query.identity_id.as_deref());
            if !query.include_retracted {
                if let Some(not_after) = query.not_after.as_deref() {
                    statement = statement.bind(not_after);
                }
            }
            if let Some(not_after) = query.not_after.as_deref() {
                statement = statement.bind(not_after);
            }
            statement = statement.bind(&fts_query);
''',
)

replace_once(
    "crates/db/src/sqlite/memory.rs",
    '''                    include_retracted: query.include_retracted,
''',
    '''                    // The candidate query already evaluated lifecycle at
                    // `not_after`. A later assertion must not make a retry's
                    // admitted context disappear during this body fetch.
                    include_retracted: query.include_retracted || query.not_after.is_some(),
''',
)

lifecycle_test = '''
#[tokio::test]
async fn test_scoped_memory_cutoff_freezes_lifecycle_at_admission() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Scoped lifecycle horizon", None).await;
    let mut item = memory_item(
        &project_id,
        "Lifecycle horizon marker",
        "horizon-sharedneedle",
    );
    item.created_at = "2026-08-12T00:00:00Z".to_owned();
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("horizon item inserts");
    ScopedMemoryRepository::insert_memory_lifecycle_assertion(
        &db,
        crate::CreateMemoryLifecycleAssertion {
            id: new_uuid_v4(),
            memory_item_id: item.id.clone(),
            assertion_type: "retracted".to_owned(),
            related_memory_id: None,
            reason: Some("retracted after turn admission".to_owned()),
            evidence_json: "{}".to_owned(),
            asserted_by_type: "user".to_owned(),
            asserted_by_id: Some("user-1".to_owned()),
            source_event_id: None,
            created_at: "2026-08-12T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("horizon lifecycle assertion inserts");
    let grant = MemoryScopeGrant {
        scope_type: "project".to_owned(),
        scope_id: project_id,
        visibility: vec!["project".to_owned()],
        identity_id: None,
    };

    let (admitted, _) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: None,
            grants: vec![grant.clone()],
            query: "sharedneedle".to_owned(),
            not_after: Some("2026-08-12T00:00:01Z".to_owned()),
            limit: 10,
            cursor: None,
            include_retracted: false,
        },
    )
    .await
    .expect("admission-frozen search succeeds");
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].id, item.id);

    let (after_retraction, _) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: None,
            grants: vec![grant],
            query: "sharedneedle".to_owned(),
            not_after: Some("2026-08-12T00:00:03Z".to_owned()),
            limit: 10,
            cursor: None,
            include_retracted: false,
        },
    )
    .await
    .expect("post-retraction search succeeds");
    assert!(after_retraction.is_empty());
}

'''
replace_once(
    "crates/db/src/tests.rs",
    '''#[tokio::test]
async fn test_scoped_memory_cursor_replays_ranked_pages() {
''',
    lifecycle_test
    + '''#[tokio::test]
async fn test_scoped_memory_cursor_replays_ranked_pages() {
''',
)

replace_once(
    "crates/services/src/memory_context.rs",
    "        let rendered = serde_json::to_string_pretty(&records).map_err(|error| {\n",
    "        let rendered = serde_json::to_string(&records).map_err(|error| {\n",
)

replace_once(
    "docs/memory-recall.md",
    '''Recall is frozen to `agent_chat_turn_job.created_at`. A retry therefore cannot
silently receive memories that appeared after its original admission, and the
same immutable context-manifest identity remains meaningful.
''',
    '''Recall is frozen to `agent_chat_turn_job.created_at`. Both memory creation and
supersession/retraction/expiry assertions are evaluated at that admission
horizon. A retry therefore cannot silently gain or lose memories after its
original admission, and the same immutable context-manifest identity remains
meaningful.
''',
)
