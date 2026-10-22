use db::{
    begin_immediate, create_sqlite_pool, now_rfc3339, run_migrations, AttentionListQuery,
    AttentionRepo, CreateAttentionProjection, CreateDomainEvent, CreateProject, DomainEventRepo,
    PageRequest, ProjectRepo, SortBy, SortOrder, UpdateAttentionLifecycle,
};

async fn database() -> db::SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    db::SqliteDb::new(pool)
}

async fn source_event(db: &db::SqliteDb, id: &str, now: &str) -> db::DomainEvent {
    DomainEventRepo::append_event(
        db,
        CreateDomainEvent {
            id: id.to_owned(),
            event_type: "task.validation_failed".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: id.to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "project".to_owned(),
            scope_id: "wake-project".to_owned(),
            correlation_id: id.to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(id.to_owned()),
            payload_json: "{}".to_owned(),
            created_at: now.to_owned(),
        },
    )
    .await
    .expect("source event creates")
}

fn attention_input(
    id: &str,
    dedupe_key: &str,
    source_event_id: &str,
    status: &str,
    now: &str,
) -> CreateAttentionProjection {
    CreateAttentionProjection {
        id: id.to_owned(),
        attention_type: "validation_failed".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: "wake-project".to_owned(),
        identity_id: None,
        source_event_id: source_event_id.to_owned(),
        priority: 80,
        status: status.to_owned(),
        summary: "Validation failed".to_owned(),
        details_json: "{}".to_owned(),
        dedupe_key: dedupe_key.to_owned(),
        occurred_at: now.to_owned(),
        updated_at: now.to_owned(),
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: (status == "resolved").then(|| now.to_owned()),
        updated_by_user_id: None,
        recommended_action: "inspect_validation".to_owned(),
        source_sequence: None,
    }
}

#[tokio::test]
async fn attention_reopen_rearms_only_terminal_wakes_and_rolls_back() {
    let db = database().await;
    let now = "2026-10-18T08:04:00Z";
    ProjectRepo::create(
        &db,
        CreateProject {
            id: "wake-project".to_owned(),
            name: "Wake Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("Project creates");
    let chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = 'wake-project'",
    )
    .fetch_one(db.pool())
    .await
    .expect("Project Chat");
    for (ordinal, status) in [(1, "succeeded"), (2, "failed"), (3, "queued")] {
        let message_id = format!("wake-message-{ordinal}");
        let turn_id = format!("wake-turn-{ordinal}");
        sqlx::query(
            "INSERT INTO agent_chat_message (
                id, chat_id, sequence, author_type, content, status,
                correlation_id, created_at
             ) VALUES (?, ?, ?, 'system', 'wake fixture', 'complete', ?, ?)",
        )
        .bind(&message_id)
        .bind(&chat_id)
        .bind(ordinal)
        .bind(format!("wake-message-correlation-{ordinal}"))
        .bind(now)
        .execute(db.pool())
        .await
        .expect("turn message inserts");
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, canonical_scope_type,
                canonical_scope_id, status, dedupe_key, correlation_id,
                created_at, updated_at
             ) VALUES (?, ?, ?, 'agent_chat', ?, ?, ?, ?, ?, ?)",
        )
        .bind(&turn_id)
        .bind(&chat_id)
        .bind(&message_id)
        .bind(&chat_id)
        .bind(status)
        .bind(format!("wake-turn-dedupe-{ordinal}"))
        .bind(format!("wake-turn-correlation-{ordinal}"))
        .bind(now)
        .bind(now)
        .execute(db.pool())
        .await
        .expect("turn job inserts");
    }

    let event = source_event(&db, "wake-event-1", now).await;
    AttentionRepo::insert_attention(
        &db,
        attention_input(
            "wake-attention",
            "wake-incident",
            &event.id,
            "resolved",
            now,
        ),
    )
    .await
    .expect("resolved Attention inserts");
    for (digest, turn_id) in [
        ("succeeded-digest", "wake-turn-1"),
        ("failed-digest", "wake-turn-2"),
        ("live-digest", "wake-turn-3"),
    ] {
        sqlx::query(
            "INSERT INTO agent_wake_blocker (
                attention_id, incident_digest, turn_job_id, admitted_at
             ) VALUES ('wake-attention', ?, ?, ?)",
        )
        .bind(digest)
        .bind(turn_id)
        .bind(now)
        .execute(db.pool())
        .await
        .expect("wake blocker inserts");
    }
    sqlx::query(
        "INSERT INTO agent_wake_attention_latest (
            attention_id, incident_digest, disposition, reason, updated_at
         ) VALUES ('wake-attention', 'latest-digest', 'turn_admitted', 'fixture', ?)",
    )
    .bind(now)
    .execute(db.pool())
    .await
    .expect("latest wake inserts");

    let reopened = AttentionRepo::update_attention_lifecycle(
        &db,
        UpdateAttentionLifecycle {
            id: "wake-attention".to_owned(),
            expected_version: 1,
            status: "open".to_owned(),
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: Some(None),
            updated_by_user_id: None,
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("resolved Attention reopens");
    assert_eq!(reopened.version, 2);
    let blockers: Vec<String> = sqlx::query_scalar(
        "SELECT turn_job_id FROM agent_wake_blocker
         WHERE attention_id = 'wake-attention' ORDER BY turn_job_id",
    )
    .fetch_all(db.pool())
    .await
    .expect("remaining blockers");
    assert_eq!(blockers, vec!["wake-turn-3"]);
    let latest: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_wake_attention_latest
         WHERE attention_id = 'wake-attention'",
    )
    .fetch_one(db.pool())
    .await
    .expect("latest wake count");
    assert_eq!(latest, 0);

    sqlx::query(
        "INSERT INTO agent_wake_blocker (
            attention_id, incident_digest, turn_job_id, admitted_at
         ) VALUES ('wake-attention', 'terminal-readded', 'wake-turn-1', ?)",
    )
    .bind(now)
    .execute(db.pool())
    .await
    .expect("terminal blocker reinserted");
    sqlx::query(
        "INSERT INTO agent_wake_attention_latest (
            attention_id, incident_digest, disposition, reason, updated_at
         ) VALUES ('wake-attention', 'latest-again', 'turn_admitted', 'fixture', ?)",
    )
    .bind(now)
    .execute(db.pool())
    .await
    .expect("latest wake reinserted");
    for (expected_version, status) in [(2, "open"), (3, "resolved"), (4, "resolved")] {
        AttentionRepo::update_attention_lifecycle(
            &db,
            UpdateAttentionLifecycle {
                id: "wake-attention".to_owned(),
                expected_version,
                status: status.to_owned(),
                acknowledged_at: None,
                snoozed_until: None,
                resolved_at: None,
                updated_by_user_id: None,
                updated_at: now.to_owned(),
            },
        )
        .await
        .expect("non-reopen lifecycle update");
    }
    let unchanged: (i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT COUNT(*) FROM agent_wake_blocker WHERE attention_id = 'wake-attention'),
           (SELECT COUNT(*) FROM agent_wake_attention_latest WHERE attention_id = 'wake-attention')",
    )
    .fetch_one(db.pool())
    .await
    .expect("non-reopen wake rows");
    assert_eq!(unchanged, (2, 1));

    let rollback_event = source_event(&db, "wake-event-rollback", now).await;
    AttentionRepo::insert_attention(
        &db,
        attention_input(
            "rollback-attention",
            "rollback-incident",
            &rollback_event.id,
            "resolved",
            now,
        ),
    )
    .await
    .expect("rollback Attention inserts");
    sqlx::query(
        "INSERT INTO agent_wake_blocker (
            attention_id, incident_digest, turn_job_id, admitted_at
         ) VALUES ('rollback-attention', 'rollback-digest', 'wake-turn-1', ?)",
    )
    .bind(now)
    .execute(db.pool())
    .await
    .expect("rollback blocker inserts");
    sqlx::query(
        "INSERT INTO agent_wake_attention_latest (
            attention_id, incident_digest, disposition, reason, updated_at
         ) VALUES ('rollback-attention', 'rollback-digest', 'turn_admitted', 'fixture', ?)",
    )
    .bind(now)
    .execute(db.pool())
    .await
    .expect("rollback latest inserts");
    let reopen_event = source_event(&db, "wake-event-reopen", now).await;
    let mut tx = begin_immediate(db.pool())
        .await
        .expect("reopen transaction");
    db.insert_attention_in_tx(
        &mut tx,
        attention_input(
            "ignored-conflict-id",
            "rollback-incident",
            &reopen_event.id,
            "open",
            now,
        ),
    )
    .await
    .expect("transactional reopen");
    let inside: (String, i64, i64) = sqlx::query_as(
        "SELECT status,
           (SELECT COUNT(*) FROM agent_wake_blocker WHERE attention_id = 'rollback-attention'),
           (SELECT COUNT(*) FROM agent_wake_attention_latest WHERE attention_id = 'rollback-attention')
         FROM attention_projection WHERE id = 'rollback-attention'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("inside rollback transaction");
    assert_eq!(inside, ("open".to_owned(), 0, 0));
    tx.rollback().await.expect("source transaction rolls back");
    let after_rollback: (String, i64, i64) = sqlx::query_as(
        "SELECT status,
           (SELECT COUNT(*) FROM agent_wake_blocker WHERE attention_id = 'rollback-attention'),
           (SELECT COUNT(*) FROM agent_wake_attention_latest WHERE attention_id = 'rollback-attention')
         FROM attention_projection WHERE id = 'rollback-attention'",
    )
    .fetch_one(db.pool())
    .await
    .expect("rollback state");
    assert_eq!(after_rollback, ("resolved".to_owned(), 1, 1));

    let moved_trigger: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'trigger' AND name = 'agent_wake_blocker_rearm_on_reopen'",
    )
    .fetch_one(db.pool())
    .await
    .expect("trigger catalog");
    assert_eq!(moved_trigger, 0);
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
