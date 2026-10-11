use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, AttentionListQuery, AttentionRepo,
    CreateAttentionProjection, CreateDomainEvent, CreateProject, DomainEventRepo, PageRequest,
    ProjectRepo, SortBy, SortOrder, UpdateAttentionLifecycle,
};

async fn database() -> db::SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    db::SqliteDb::new(pool)
}

fn page() -> PageRequest {
    PageRequest {
        cursor: None,
        limit: 20,
        include_total: true,
        sort_by: SortBy::Priority,
        sort_order: SortOrder::Desc,
    }
}

#[tokio::test]
async fn attention_lifecycle_is_optimistic_and_bounded() {
    let db = database().await;
    let now = now_rfc3339();
    let source = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-attention-1".to_owned(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-1".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: "project-1".to_owned(),
            correlation_id: "corr-1".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("event-attention-1".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let item = AttentionRepo::insert_attention(
        &db,
        CreateAttentionProjection {
            id: "attention-1".to_owned(),
            attention_type: "validation_failed".to_owned(),
            scope_type: "account".to_owned(),
            scope_id: "user-1".to_owned(),
            identity_id: None,
            source_event_id: source.id,
            priority: 80,
            status: "open".to_owned(),
            summary: "Validation failed".to_owned(),
            details_json: r#"{"source_sequence":1}"#.to_owned(),
            dedupe_key: "incident-1".to_owned(),
            occurred_at: now.clone(),
            updated_at: now.clone(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect_validation".to_owned(),
            source_sequence: Some(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(item.version, 1);

    let listed = AttentionRepo::list_attention(
        &db,
        AttentionListQuery {
            account_id: Some("user-1".to_owned()),
            project_id: None,
            scope_type: None,
            status: None,
            include_snoozed: false,
            page: page(),
        },
    )
    .await
    .unwrap();
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.total_count, Some(1));

    let acknowledged = AttentionRepo::update_attention_lifecycle(
        &db,
        UpdateAttentionLifecycle {
            id: "attention-1".to_owned(),
            expected_version: 1,
            status: "acknowledged".to_owned(),
            acknowledged_at: Some(Some(now.clone())),
            snoozed_until: Some(Some("2999-01-01T00:00:00Z".to_owned())),
            resolved_at: Some(None),
            updated_by_user_id: None,
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(acknowledged.version, 2);

    let conflict = AttentionRepo::update_attention_lifecycle(
        &db,
        UpdateAttentionLifecycle {
            id: "attention-1".to_owned(),
            expected_version: 1,
            status: "resolved".to_owned(),
            acknowledged_at: None,
            snoozed_until: Some(None),
            resolved_at: Some(Some(now)),
            updated_by_user_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await;
    assert!(matches!(conflict, Err(db::DbError::VersionConflict)));

    let open_list = AttentionRepo::list_attention(
        &db,
        AttentionListQuery {
            account_id: Some("user-1".to_owned()),
            project_id: None,
            scope_type: None,
            status: None,
            include_snoozed: false,
            page: page(),
        },
    )
    .await
    .unwrap();
    assert!(open_list.items.is_empty());

    let all_list = AttentionRepo::list_attention(
        &db,
        AttentionListQuery {
            account_id: Some("user-1".to_owned()),
            project_id: None,
            scope_type: None,
            status: Some("acknowledged".to_owned()),
            include_snoozed: true,
            page: page(),
        },
    )
    .await
    .unwrap();
    assert_eq!(all_list.items.len(), 1);
}

#[tokio::test]
async fn account_attention_query_excludes_inaccessible_projects() {
    let db = database().await;
    let now = now_rfc3339();
    for user_id in ["user-visible", "user-owner"] {
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
             VALUES (?, ?, 'test', NULL, ?, ?)",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.test"))
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    }
    for (project_id, owner_id) in [
        ("project-public", None),
        ("project-member", Some("user-owner")),
        ("project-hidden", Some("user-owner")),
    ] {
        ProjectRepo::create(
            &db,
            CreateProject {
                id: project_id.to_owned(),
                name: project_id.to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: owner_id.map(str::to_owned),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO project_member (id, project_id, user_id, role, created_at, updated_at)
         VALUES ('member-1', 'project-member', 'user-visible', 'member', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .unwrap();

    for (event_id, project_id, attention_id) in [
        ("event-public", "project-public", "attention-public"),
        ("event-member", "project-member", "attention-member"),
        ("event-hidden", "project-hidden", "attention-hidden"),
    ] {
        let event = DomainEventRepo::append_event(
            &db,
            CreateDomainEvent {
                id: event_id.to_owned(),
                event_type: "task.validation_failed".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: format!("task-{project_id}"),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "project".to_owned(),
                scope_id: project_id.to_owned(),
                correlation_id: event_id.to_owned(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(event_id.to_owned()),
                payload_json: "{}".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
        AttentionRepo::insert_attention(
            &db,
            CreateAttentionProjection {
                id: attention_id.to_owned(),
                attention_type: "validation_failed".to_owned(),
                scope_type: "project".to_owned(),
                scope_id: project_id.to_owned(),
                identity_id: None,
                source_event_id: event.id,
                priority: 80,
                status: "open".to_owned(),
                summary: "Validation failed".to_owned(),
                details_json: "{}".to_owned(),
                dedupe_key: attention_id.to_owned(),
                occurred_at: now.clone(),
                updated_at: now.clone(),
                acknowledged_at: None,
                snoozed_until: None,
                resolved_at: None,
                updated_by_user_id: None,
                recommended_action: "inspect_validation".to_owned(),
                source_sequence: None,
            },
        )
        .await
        .unwrap();
    }

    let listed = AttentionRepo::list_attention(
        &db,
        AttentionListQuery {
            account_id: Some("user-visible".to_owned()),
            project_id: None,
            scope_type: None,
            status: None,
            include_snoozed: false,
            page: page(),
        },
    )
    .await
    .unwrap();
    let ids = listed
        .items
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["attention-member", "attention-public"]);
}
