//! Memory repository tests.
use super::*;

#[tokio::test]
async fn test_memory_insert_and_fts_search() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory search", None).await;
    let item = memory_item(
        &project_id,
        "Lantern handoff",
        "The execution found a durable lantern clue.",
    );
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    let (items, has_more) =
        MemoryRepository::search_memory_items(&db, &project_id, "lantern", 10, None)
            .await
            .expect("memory search succeeds");

    assert!(!has_more);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, item.id);
    assert!(items[0].row_id > 0);
}

#[tokio::test]
async fn test_memory_search_escapes_fts_query_syntax() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory punctuation search", None).await;
    let item = memory_item(
        &project_id,
        "Punctuation handoff",
        "A user's note says can't reproduce foo or AND yet.",
    );
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    let (apostrophe_items, apostrophe_has_more) =
        MemoryRepository::search_memory_items(&db, &project_id, "can't", 10, None)
            .await
            .expect("apostrophe search succeeds");
    let (operator_items, operator_has_more) =
        MemoryRepository::search_memory_items(&db, &project_id, "foo or AND", 10, None)
            .await
            .expect("operator-looking search succeeds");

    assert!(!apostrophe_has_more);
    assert_eq!(apostrophe_items.len(), 1);
    assert_eq!(apostrophe_items[0].id, item.id);
    assert!(!operator_has_more);
    assert_eq!(operator_items.len(), 1);
    assert_eq!(operator_items[0].id, item.id);
}

#[tokio::test]
async fn test_memory_search_cursor_uses_same_order_as_results() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory pagination", None).await;
    let mut oldest = memory_item(
        &project_id,
        "Pager oldest",
        "pagerneedle pagerneedle pagerneedle pagerneedle",
    );
    oldest.created_at = "2026-06-08T00:00:00Z".to_owned();
    oldest.id = "00000000-0000-4000-8000-000000000001".to_owned();
    let mut middle = memory_item(&project_id, "Pager middle", "pagerneedle");
    middle.created_at = "2026-06-08T00:00:01Z".to_owned();
    middle.id = "00000000-0000-4000-8000-000000000002".to_owned();
    let mut newest = memory_item(&project_id, "Pager newest", "pagerneedle");
    newest.created_at = "2026-06-08T00:00:02Z".to_owned();
    newest.id = "00000000-0000-4000-8000-000000000003".to_owned();

    for item in [&oldest, &middle, &newest] {
        MemoryRepository::insert_memory_item(&db, item)
            .await
            .expect("memory item inserts");
    }

    let (page_one, page_one_has_more) =
        MemoryRepository::search_memory_items(&db, &project_id, "pagerneedle", 1, None)
            .await
            .expect("first page succeeds");
    assert!(page_one_has_more);
    assert_eq!(page_one[0].id, newest.id);

    let (page_two, page_two_has_more) = MemoryRepository::search_memory_items(
        &db,
        &project_id,
        "pagerneedle",
        1,
        Some(memory_cursor(&page_one[0])),
    )
    .await
    .expect("second page succeeds");
    assert!(page_two_has_more);
    assert_eq!(page_two[0].id, middle.id);
    assert_ne!(page_two[0].id, page_one[0].id);

    let (page_three, page_three_has_more) = MemoryRepository::search_memory_items(
        &db,
        &project_id,
        "pagerneedle",
        1,
        Some(memory_cursor(&page_two[0])),
    )
    .await
    .expect("third page succeeds");
    assert!(!page_three_has_more);
    assert_eq!(page_three[0].id, oldest.id);
}

#[tokio::test]
async fn test_memory_source_exists_uses_source_ref_field() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory source exists", None).await;
    let mut item = memory_item(&project_id, "Source ref", "source ref body");
    item.source_type = MemorySourceType::Review.to_string();
    item.metadata_json = serde_json::json!({
        "source_ref": "review-1",
        "extra": true,
    })
    .to_string();
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    assert!(MemoryRepository::memory_source_exists(
        &db,
        &project_id,
        &MemorySourceType::Review.to_string(),
        "review-1"
    )
    .await
    .expect("source exists check succeeds"));
}

#[tokio::test]
async fn test_memory_source_exists_with_confidence_filters_source_ref_and_confidence() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory source confidence exists", None).await;
    let mut item = memory_item(&project_id, "Source ref", "source ref body");
    item.source_type = MemorySourceType::Review.to_string();
    item.metadata_json = serde_json::json!({
        "source_ref": "review-1",
        "extra": true,
    })
    .to_string();
    item.confidence = Some(MemoryConfidence::Partial.to_string());
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    assert!(MemoryRepository::memory_source_exists_with_confidence(
        &db,
        &project_id,
        &MemorySourceType::Review.to_string(),
        "review-1",
        &MemoryConfidence::Partial.to_string()
    )
    .await
    .expect("source confidence check succeeds"));
    assert!(!MemoryRepository::memory_source_exists_with_confidence(
        &db,
        &project_id,
        &MemorySourceType::Review.to_string(),
        "review-1",
        &MemoryConfidence::Confirmed.to_string()
    )
    .await
    .expect("source confidence check succeeds"));
}

#[tokio::test]
async fn test_memory_project_isolation() {
    let db = sqlite_db().await;
    let project_a = seed_project(&db, "Memory A", None).await;
    let project_b = seed_project(&db, "Memory B", None).await;
    let item_a = memory_item(&project_a, "Shared needle A", "sharedneedle belongs to A");
    let item_b = memory_item(&project_b, "Shared needle B", "sharedneedle belongs to B");
    MemoryRepository::insert_memory_item(&db, &item_a)
        .await
        .expect("project A memory inserts");
    MemoryRepository::insert_memory_item(&db, &item_b)
        .await
        .expect("project B memory inserts");

    let (items, has_more) =
        MemoryRepository::search_memory_items(&db, &project_a, "sharedneedle", 10, None)
            .await
            .expect("project-scoped memory search succeeds");

    assert!(!has_more);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].project_id.as_deref(), Some(project_a.as_str()));
    assert_eq!(items[0].id, item_a.id);
}

#[tokio::test]
async fn test_scoped_memory_search_is_acl_first_and_bounded() {
    let db = sqlite_db().await;
    let project_a = seed_project(&db, "Scoped memory A", None).await;
    let project_b = seed_project(&db, "Scoped memory B", None).await;
    let mut private = memory_item(&project_a, "Private marker", "private-sharedneedle");
    private.visibility = "private".to_owned();
    private.metadata_json = serde_json::json!({"source_ref": "private-1"}).to_string();
    let project = memory_item(&project_a, "Project marker", "project-sharedneedle");
    let other = memory_item(&project_b, "Other marker", "sharedneedle");
    for item in [&private, &project, &other] {
        MemoryRepository::insert_memory_item(&db, item)
            .await
            .expect("scoped memory inserts");
    }

    let grant = MemoryScopeGrant {
        scope_type: "project".to_owned(),
        scope_id: project_a.clone(),
        visibility: vec!["project".to_owned()],
        identity_id: Some("identity-b".to_owned()),
    };
    let (items, has_more) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: Some("identity-b".to_owned()),
            grants: vec![grant.clone()],
            query: "sharedneedle".to_owned(),
            limit: 1,
            cursor: None,
            include_retracted: false,
        },
    )
    .await
    .expect("scoped search succeeds");
    assert!(!has_more);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, project.id);
    assert_ne!(items[0].id, private.id);
    assert_ne!(items[0].id, other.id);

    let private_grant = MemoryScopeGrant {
        scope_type: "project".to_owned(),
        scope_id: project_a,
        visibility: vec!["project".to_owned()],
        identity_id: Some("identity-b".to_owned()),
    };
    let hidden = ScopedMemoryRepository::get_memory_item_scoped(
        &db,
        MemoryGetQuery {
            id: private.id,
            identity_id: Some("identity-b".to_owned()),
            grants: vec![private_grant],
            include_retracted: false,
        },
    )
    .await
    .expect("scoped get succeeds");
    assert!(hidden.is_none());
}

#[tokio::test]
async fn test_scoped_memory_lifecycle_excludes_retracted_and_keeps_audit() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Scoped lifecycle", None).await;
    let item = memory_item(&project_id, "Lifecycle marker", "lifecycle-sharedneedle");
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("lifecycle item inserts");
    ScopedMemoryRepository::insert_memory_lifecycle_assertion(
        &db,
        crate::CreateMemoryLifecycleAssertion {
            id: new_uuid_v4(),
            memory_item_id: item.id.clone(),
            assertion_type: "retracted".to_owned(),
            related_memory_id: None,
            reason: Some("invalidated by evidence".to_owned()),
            evidence_json: "{\"ticket\":\"e-1\"}".to_owned(),
            asserted_by_type: "user".to_owned(),
            asserted_by_id: Some("user-1".to_owned()),
            source_event_id: None,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("lifecycle assertion inserts");
    let grant = MemoryScopeGrant {
        scope_type: "project".to_owned(),
        scope_id: project_id,
        visibility: vec!["project".to_owned()],
        identity_id: None,
    };
    let (items, _) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: None,
            grants: vec![grant],
            query: "sharedneedle".to_owned(),
            limit: 10,
            cursor: None,
            include_retracted: false,
        },
    )
    .await
    .expect("lifecycle search succeeds");
    assert!(items.is_empty());
    let assertions = ScopedMemoryRepository::list_memory_lifecycle_assertions(&db, &item.id)
        .await
        .expect("lifecycle audit loads");
    assert_eq!(assertions.len(), 1);
    assert_eq!(assertions[0].assertion_type, "retracted");
}

#[tokio::test]
async fn test_scoped_memory_cursor_replays_ranked_pages() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Scoped pagination", None).await;
    let mut first = memory_item(&project_id, "First ranked", "ranked-sharedneedle");
    first.id = "00000000-0000-4000-8000-000000000011".to_owned();
    first.created_at = "2026-08-12T00:00:01Z".to_owned();
    let mut second = memory_item(&project_id, "Second ranked", "ranked-sharedneedle");
    second.id = "00000000-0000-4000-8000-000000000012".to_owned();
    second.created_at = "2026-08-12T00:00:02Z".to_owned();
    for item in [&first, &second] {
        MemoryRepository::insert_memory_item(&db, item)
            .await
            .expect("ranked memory inserts");
    }
    let grant = MemoryScopeGrant {
        scope_type: "project".to_owned(),
        scope_id: project_id,
        visibility: vec!["project".to_owned()],
        identity_id: None,
    };
    let (page_one, more) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: None,
            grants: vec![grant.clone()],
            query: "sharedneedle".to_owned(),
            limit: 1,
            cursor: None,
            include_retracted: false,
        },
    )
    .await
    .expect("first scoped page succeeds");
    assert!(more);
    let cursor = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&serde_json::json!({
            "rank": page_one[0].retention_priority + 100,
            "created_at": page_one[0].created_at,
            "id": page_one[0].id,
        }))
        .expect("scoped cursor serializes"),
    );
    let (page_two, more) = ScopedMemoryRepository::search_memory_items_scoped(
        &db,
        MemoryAccessQuery {
            identity_id: None,
            grants: vec![grant],
            query: "sharedneedle".to_owned(),
            limit: 1,
            cursor: Some(cursor),
            include_retracted: false,
        },
    )
    .await
    .expect("second scoped page succeeds");
    assert!(!more);
    assert_eq!(page_two.len(), 1);
    assert_ne!(page_one[0].id, page_two[0].id);
}

#[tokio::test]
async fn test_memory_cascade_delete() {
    let db = sqlite_db().await;
    let project_id = seed_project(&db, "Memory cascade", None).await;
    let item = memory_item(
        &project_id,
        "Cascade marker",
        "cascadeneedle should disappear from memory FTS",
    );
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project deletes");

    let loaded = MemoryRepository::get_memory_item(&db, &item.id)
        .await
        .expect("memory item lookup succeeds");
    assert!(loaded.is_none());
    let fts_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM memory_item_fts WHERE memory_item_fts MATCH ?",
    )
    .bind("cascadeneedle")
    .fetch_one(db.pool())
    .await
    .expect("memory fts count succeeds");
    assert_eq!(fts_count, 0);
}

#[test]
fn test_memory_no_update_path() {
    fn assert_append_only_repository<T: MemoryRepository + ?Sized>() {}

    assert_append_only_repository::<SqliteDb>();
    // MemoryRepository intentionally exposes insert/get/search/list only; there is no update method.
}

#[test]
fn test_memory_enum_round_trips() {
    let kinds = [
        MemoryKind::Observation,
        MemoryKind::Decision,
        MemoryKind::Handoff,
        MemoryKind::Failure,
        MemoryKind::ReviewResult,
        MemoryKind::ExecutionSummary,
        MemoryKind::Comment,
        MemoryKind::Transition,
        MemoryKind::Artifact,
        MemoryKind::Lesson,
        MemoryKind::ContextPack,
    ];
    for kind in kinds {
        let value = kind.to_string();
        assert_eq!(value.parse::<MemoryKind>().unwrap(), kind);
    }

    let source_types = [
        MemorySourceType::Execution,
        MemorySourceType::Review,
        MemorySourceType::Comment,
        MemorySourceType::Transition,
    ];
    for source_type in source_types {
        let value = source_type.to_string();
        assert_eq!(value.parse::<MemorySourceType>().unwrap(), source_type);
    }

    let confidences = [
        MemoryConfidence::Confirmed,
        MemoryConfidence::Partial,
        MemoryConfidence::Unconfirmed,
    ];
    for confidence in confidences {
        let value = confidence.to_string();
        assert_eq!(value.parse::<MemoryConfidence>().unwrap(), confidence);
    }
}

fn memory_cursor(item: &MemoryItem) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&serde_json::json!({
            "created_at": &item.created_at,
            "id": &item.id,
        }))
        .expect("memory cursor serializes"),
    )
}
