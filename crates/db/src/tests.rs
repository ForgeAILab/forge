use crate::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, validate_uuid_v4,
    AgentContextScopeRepo, AgentListQuery, AgentProfileRepo, AgentRepo, AgentSessionRepo,
    AgentStatus, AgentTaskListQuery, ArchiveTask, ClaimDomainEvents, ClaimTask, CompareAndMoveTask,
    CompleteDomainEvent, CreateAgent, CreateAgentContextScope, CreateAgentIdentity,
    CreateAgentProfile, CreateAgentSession, CreateDomainEvent, CreateExecution, CreateProject,
    CreateProjectAgentBinding, CreateProjectCharter, CreateProjectCharterRevision,
    CreateProjectCharterRevisionAtomically, CreateProjectMember,
    CreateProviderAuthorizationOperation, CreateRepo, CreateReview, CreateSkill, CreateTask,
    CreateTaskRoleAssignment, CreateTerminalSession, CreateWorkspace, CreateWorkspaceLease,
    CredentialHandleRepo, DaemonRepo, DaemonStatus, DbError, DomainEventRepo, ExecutionRepo,
    ExecutionStatus, MemoryAccessQuery, MemoryConfidence, MemoryGetQuery, MemoryItem, MemoryKind,
    MemoryRepository, MemoryScopeGrant, MemorySourceType, MoveTaskIdentity, MoveTaskPersistence,
    NotificationListQuery, NotificationRepo, PageRequest, ProjectAgentBindingRepo,
    ProjectMemberRepo, ProjectOrchestrationRepo, ProjectRepo, ProviderAuthorizationRepo, RepoRepo,
    ReviewRepo, ReviewStatus, RotateAgentSession, ScopedMemoryRepository, SelectAgentProfile,
    SkillRepo, SortBy, SortOrder, SqliteDb, Task, TaskBoardRepo, TaskDependencyRepo, TaskListQuery,
    TaskRepo, TaskRoleAssignmentRepo, TerminalSessionRepo, TerminalSessionStatus, UpdateAgent,
    UpdateExecution, UpdateProject, UpdateProviderAuthorizationOperation, UpdateRepo, UpdateSkill,
    UpdateTask, UpdateTaskStatus, UpdateTerminalSessionStatus, UpsertDaemon, WorkMode,
    WorkspaceLeaseRepo, WorkspaceRepo, WorkspaceStatus,
};
use crate::{RefreshToken, RefreshTokenRepo, User, UserRepo};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

fn page(limit: i64) -> PageRequest {
    PageRequest {
        cursor: None,
        limit,
        include_total: true,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

#[tokio::test]
async fn profile_publication_and_selection_are_atomic_on_version_conflict() {
    let db = sqlite_db().await;
    let owner = seed_user(&db).await;
    let now = now_rfc3339();
    let identity_id = new_uuid_v4();
    let initial_profile_id = new_uuid_v4();
    let profile = |id: String| CreateAgentProfile {
        id,
        identity_id: identity_id.clone(),
        backend_kind: "native".to_owned(),
        executor_type: "embedded".to_owned(),
        provider: Some("openai".to_owned()),
        model: Some("gpt-test".to_owned()),
        reasoning_effort: None,
        permission_policy: Some("scoped_proposals".to_owned()),
        prompt_template: None,
        capabilities_json: "{}".to_owned(),
        tool_policy_json: "{}".to_owned(),
        config_json: "{}".to_owned(),
        credential_ref: None,
        daemon_id: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let agent = AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "Atomic identity".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(owner),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        profile(initial_profile_id.clone()),
    )
    .await
    .expect("identity creates");

    let stale_profile_id = new_uuid_v4();
    let error = AgentProfileRepo::create_and_select_profile(
        &db,
        profile(stale_profile_id.clone()),
        SelectAgentProfile {
            identity_id: identity_id.clone(),
            profile_id: stale_profile_id.clone(),
            expected_version: agent.version - 1,
            updated_at: now.clone(),
        },
    )
    .await
    .expect_err("stale publication conflicts");
    assert!(matches!(error, DbError::VersionConflict));
    assert!(
        AgentProfileRepo::get_profile(&db, &stale_profile_id)
            .await
            .expect("profile lookup succeeds")
            .is_none(),
        "the profile insert must roll back with the selection conflict"
    );

    let current = AgentRepo::get_by_id(&db, &identity_id)
        .await
        .expect("identity lookup succeeds")
        .expect("identity exists");
    assert_eq!(current.profile_id, initial_profile_id);

    let next_profile_id = new_uuid_v4();
    let (published, selected) = AgentProfileRepo::create_and_select_profile(
        &db,
        profile(next_profile_id.clone()),
        SelectAgentProfile {
            identity_id,
            profile_id: next_profile_id.clone(),
            expected_version: current.version,
            updated_at: now,
        },
    )
    .await
    .expect("current publication succeeds");
    assert_eq!(published.id, next_profile_id);
    assert_eq!(selected.profile_id, published.id);
}

#[tokio::test]
async fn provider_authorization_operations_are_owner_scoped_and_versioned() {
    let db = sqlite_db().await;
    let owner = seed_user(&db).await;
    let other = seed_user(&db).await;
    let now = now_rfc3339();
    let id = new_uuid_v4();
    let created = ProviderAuthorizationRepo::create_provider_authorization(
        &db,
        CreateProviderAuthorizationOperation {
            id: id.clone(),
            owner_user_id: owner.clone(),
            provider: "openai".to_owned(),
            method: "browser_oauth".to_owned(),
            status: "awaiting_browser".to_owned(),
            authorization_url: Some("https://auth.openai.com/oauth/authorize".to_owned()),
            user_code: None,
            redirect_origin: "http://localhost:5173".to_owned(),
            callback_state_hash: Some("state-hash".to_owned()),
            request_json: "{}".to_owned(),
            poll_interval_seconds: 5,
            expires_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("authorization creates");
    assert_eq!(created.version, 1);
    assert!(
        ProviderAuthorizationRepo::get_provider_authorization(&db, &id, &other)
            .await
            .expect("other-owner lookup succeeds")
            .is_none()
    );
    let updated = ProviderAuthorizationRepo::update_provider_authorization(
        &db,
        UpdateProviderAuthorizationOperation {
            id: id.clone(),
            expected_version: 1,
            status: "exchanging".to_owned(),
            authorization_url: created.authorization_url,
            user_code: None,
            poll_interval_seconds: 5,
            profile_id: None,
            credential_handle_id: None,
            error_code: None,
            error_message: None,
            updated_at: now.clone(),
            completed_at: None,
        },
    )
    .await
    .expect("authorization advances");
    assert_eq!(updated.version, 2);
    let conflict = ProviderAuthorizationRepo::update_provider_authorization(
        &db,
        UpdateProviderAuthorizationOperation {
            id,
            expected_version: 1,
            status: "cancelled".to_owned(),
            authorization_url: None,
            user_code: None,
            poll_interval_seconds: 5,
            profile_id: None,
            credential_handle_id: None,
            error_code: None,
            error_message: None,
            updated_at: now.clone(),
            completed_at: Some(now),
        },
    )
    .await;
    assert!(matches!(conflict, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn provider_credential_migration_preserves_legacy_handle_defaults() {
    let db = sqlite_db().await;
    let owner = seed_user(&db).await;
    let now = now_rfc3339();
    let id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO credential_handle (
            id, owner_user_id, provider, label, status, created_at, updated_at
         ) VALUES (?, ?, 'openai', 'Legacy key', 'configured', ?, ?)",
    )
    .bind(&id)
    .bind(&owner)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("legacy-shaped handle inserts");
    let handle = CredentialHandleRepo::get_credential_handle(&db, &id)
        .await
        .expect("handle reads")
        .expect("handle exists");
    assert_eq!(handle.credential_method, "api_key");
    assert_eq!(handle.metadata_json, "{}");
    assert_eq!(handle.version, 1);
}

#[tokio::test]
async fn atomic_first_charter_revision_rolls_back_new_ownership_on_failure() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (id, name, settings, created_at, updated_at)
         VALUES (?, ?, '{}', ?, ?)",
    )
    .bind("charter-rollback-project")
    .bind("Charter rollback project")
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("project fixture inserts");

    let result = ProjectOrchestrationRepo::create_project_charter_revision_atomically(
        &db,
        CreateProjectCharterRevisionAtomically {
            project_id: Some("charter-rollback-project".to_owned()),
            genesis_session_id: None,
            account_id: "test-user-id".to_owned(),
            charter: CreateProjectCharter {
                id: "charter-rollback".to_owned(),
                account_id: "test-user-id".to_owned(),
                genesis_session_id: None,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            revision: CreateProjectCharterRevision {
                id: "charter-rollback-revision".to_owned(),
                charter_id: "charter-rollback".to_owned(),
                expected_charter_version: 1,
                project_mode: "compact".to_owned(),
                maturity: "mvp".to_owned(),
                base_revision: 0,
                base_revision_id: None,
                lifecycle: "proposed".to_owned(),
                schema_version: "forge.project-charter/v1".to_owned(),
                render_version: "forge.project-charter-render/v1".to_owned(),
                content_json: "not-json".to_owned(),
                rendered_view: "invalid".to_owned(),
                change_summary: "rollback fixture".to_owned(),
                author_type: "user".to_owned(),
                author_id: Some("test-user-id".to_owned()),
                source_message_id: None,
                source_turn_job_id: None,
                source_refs_json: "[]".to_owned(),
                content_digest: "content-digest".to_owned(),
                rendered_digest: "rendered-digest".to_owned(),
                created_at: now,
            },
        },
    )
    .await;
    assert!(result.is_err());

    let charter_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM project_charter WHERE id = 'charter-rollback'")
            .fetch_one(db.pool())
            .await
            .expect("charter count queries");
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_charter_revision
         WHERE charter_id = 'charter-rollback'",
    )
    .fetch_one(db.pool())
    .await
    .expect("revision count queries");
    assert_eq!(charter_count, 0);
    assert_eq!(revision_count, 0);
}

#[tokio::test]
async fn project_delete_tears_down_charter_and_immutable_milestone_rows() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES ('delete-user', 'delete@example.test', 'test', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user fixture");
    let project_id = seed_project(&db, "Charter teardown", Some("delete-user".to_owned())).await;
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES ('delete-charter', 'delete-user', ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter fixture");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES ('delete-charter-r1', 'delete-charter', 1, 'approved', 'test', 'test',
                 '{}', 'charter', 'user', 'delete-user', 'content', 'rendered', ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision fixture");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at)
         VALUES ('delete-milestone', ?, 1, 'M001', 'planned', ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone fixture");
    sqlx::query(
        "INSERT INTO project_milestone_revision
         (id, milestone_id, revision, base_revision, lifecycle, outcome,
          included_scope_json, excluded_scope_json, document_revisions_json,
          task_selection_json, dependencies_json, risks_json, acceptance_checks_json,
          evidence_requirements_json, known_issues_json, change_summary, schema_version,
          render_version, rendered_view, content_digest, rendered_digest,
          author_type, source_refs_json, created_at)
         VALUES ('delete-milestone-r1', 'delete-milestone', 1, 0, 'approved', 'outcome',
                 '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '',
                 'test', 'test', 'milestone', 'content', 'rendered', 'user', '[]', ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone revision fixture");

    assert!(
        sqlx::query("DELETE FROM project_milestone_revision WHERE id = 'delete-milestone-r1'")
            .execute(db.pool())
            .await
            .is_err()
    );
    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("guarded Project teardown succeeds");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("project count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_deletion_guard")
            .fetch_one(db.pool())
            .await
            .expect("guard count"),
        0
    );
}

#[tokio::test]
async fn cli_agent_creation_gets_scope_bounded_permission_layers() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = SqliteDb::new(pool.clone());
    let now = now_rfc3339();
    let agent = AgentRepo::create(
        &db,
        CreateAgent {
            id: new_uuid_v4(),
            name: "Smith chat agent".to_owned(),
            description: None,
            executor_type: "smith".to_owned(),
            model: Some("gpt-5.6-luna".to_owned()),
            reasoning_effort: Some("max".to_owned()),
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: r#"{"profile":"luna"}"#.to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some("account-1".to_owned()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("CLI agent creates");

    let identity_ceiling: String =
        sqlx::query_scalar("SELECT account_permission_ceiling FROM agent_identity WHERE id = ?")
            .bind(&agent.id)
            .fetch_one(&pool)
            .await
            .expect("identity permission ceiling reads");

    for layer in [&identity_ceiling, &agent.tool_policy_json] {
        let value: serde_json::Value = serde_json::from_str(layer).expect("permission JSON");
        let permissions = value["permissions"].as_array().expect("permission array");
        assert!(permissions.iter().any(|value| value == "propose_task"));
        assert!(permissions.iter().any(|value| value == "task_write"));
        assert!(!permissions.iter().any(|value| value == "approve_actions"));
    }
}

async fn seed_daemon(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    daemon_id
}

async fn seed_project_repo_agent(db: &SqliteDb) -> (String, String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let agent_id = new_uuid_v4();
    let daemon_id = seed_daemon(db).await;

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: Some("/tmp/forge-test-repo".to_owned()),
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("project primary repo updates");
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: r#"["rust"]"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");

    (project_id, repo_id, agent_id)
}

async fn seed_project(db: &SqliteDb, name: &str, owner_id: Option<String>) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    project_id
}

fn memory_item(project_id: &str, title: &str, body: &str) -> MemoryItem {
    MemoryItem {
        row_id: 0,
        id: new_uuid_v4(),
        project_id: Some(project_id.to_owned()),
        task_id: None,
        execution_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        visibility: "project".to_owned(),
        owner_identity_id: None,
        authority: "observation".to_owned(),
        sensitivity: "internal".to_owned(),
        retention_priority: 10,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: None,
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("project".to_owned()),
        source_scope_id: Some(project_id.to_owned()),
        source_revision: None,
        source_type: MemorySourceType::Comment.to_string(),
        kind: MemoryKind::Observation.to_string(),
        title: title.to_owned(),
        summary: None,
        body: body.to_owned(),
        metadata_json: "{}".to_owned(),
        confidence: Some(MemoryConfidence::Confirmed.to_string()),
        quality_score: None,
        created_by_type: None,
        created_by_id: None,
        created_at: now_rfc3339(),
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

async fn seed_agent(
    db: &SqliteDb,
    name: &str,
    visibility: &str,
    owner_id: Option<String>,
) -> String {
    let now = now_rfc3339();
    let agent_id = new_uuid_v4();
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: name.to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id,
            visibility: visibility.to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

#[tokio::test]
async fn test_agent_session_rotation_is_atomic_and_preserves_lineage() {
    let db = sqlite_db().await;
    let identity_id = seed_agent(&db, "rotating agent", "account", None).await;
    let identity = AgentRepo::get_by_id(&db, &identity_id)
        .await
        .expect("identity lookup succeeds")
        .expect("identity exists");
    let now = now_rfc3339();
    let scope = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: new_uuid_v4(),
            identity_id: identity_id.clone(),
            scope_type: "account".to_owned(),
            scope_id: "account-owner".to_owned(),
            project_id: None,
            task_id: None,
            task_role: None,
            workspace_access: "deny".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("account scope creates");
    let original_id = new_uuid_v4();
    let original = AgentSessionRepo::create_agent_session(
        &db,
        CreateAgentSession {
            id: original_id.clone(),
            identity_id: identity_id.clone(),
            profile_id: identity.profile_id.clone(),
            context_scope_id: scope.id.clone(),
            backend_kind: "cli".to_owned(),
            runtime_session_id: Some("runtime-original".to_owned()),
            status: "ready".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("original session creates");
    let replacement_id = new_uuid_v4();
    let replacement = AgentSessionRepo::rotate_agent_session(
        &db,
        RotateAgentSession {
            previous_session_id: original.id.clone(),
            expected_version: original.version,
            replacement: CreateAgentSession {
                id: replacement_id.clone(),
                identity_id,
                profile_id: identity.profile_id,
                context_scope_id: scope.id,
                backend_kind: "cli".to_owned(),
                runtime_session_id: Some("runtime-replacement".to_owned()),
                status: "ready".to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: "healthy".to_owned(),
                predecessor_session_id: Some(original_id.clone()),
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now,
            },
        },
    )
    .await
    .expect("session rotates");
    assert_eq!(replacement.id, replacement_id);
    assert_eq!(
        replacement.predecessor_session_id.as_deref(),
        Some(original_id.as_str())
    );
    let replaced = AgentSessionRepo::get_agent_session(&db, &original_id)
        .await
        .expect("original session loads")
        .expect("original session remains");
    assert_eq!(replaced.status, "replaced");
    assert_eq!(
        replaced.replaced_by_session_id.as_deref(),
        Some(replacement_id.as_str())
    );
}

#[tokio::test]
async fn test_list_agents_usable_in_project() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let u1_id = seed_user(&db).await;
    let u2_id = seed_user(&db).await;
    let global_agent_id = seed_agent(&db, "global agent", "global", None).await;
    let u1_agent_id = seed_agent(&db, "u1 account agent", "account", Some(u1_id.clone())).await;
    let u2_agent_id = seed_agent(&db, "u2 account agent", "account", Some(u2_id.clone())).await;
    let project_id = seed_project(&db, "Usable agents", Some(u1_id.clone())).await;

    ProjectMemberRepo::add_member(
        &db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            user_id: u1_id.clone(),
            role: "owner".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("u1 project member creates");
    ProjectMemberRepo::add_member(
        &db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            user_id: u2_id.clone(),
            role: "member".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("u2 project member creates");

    let usable = SqliteDb::list_agents_usable_in_project(&db, &project_id, &u1_id)
        .await
        .expect("usable agents list");
    assert!(usable
        .iter()
        .any(|agent| agent.id.as_str() == global_agent_id.as_str()));
    assert!(usable
        .iter()
        .any(|agent| agent.id.as_str() == u1_agent_id.as_str()));
    assert!(!usable
        .iter()
        .any(|agent| agent.id.as_str() == u2_agent_id.as_str()));

    let setup_binding = ProjectAgentBindingRepo::get_active_project_binding(&db, &project_id)
        .await
        .expect("project setup binding loads")
        .expect("project setup binding exists");
    let u2_agent = AgentRepo::get_by_id(&db, &u2_agent_id)
        .await
        .expect("u2 agent loads")
        .expect("u2 agent exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &db,
        crate::ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: setup_binding.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                identity_id: Some(u2_agent.id),
                profile_id: Some(u2_agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 0,
                created_at: now.clone(),
                updated_at: now,
            },
            replacement_reason: Some("test project agent selection".to_owned()),
        },
    )
    .await
    .expect("project agent binding creates");

    let usable = SqliteDb::list_agents_usable_in_project(&db, &project_id, &u1_id)
        .await
        .expect("usable agents list after link");
    assert!(usable
        .iter()
        .any(|agent| agent.id.as_str() == global_agent_id.as_str()));
    assert!(usable
        .iter()
        .any(|agent| agent.id.as_str() == u1_agent_id.as_str()));
    assert!(usable
        .iter()
        .any(|agent| agent.id.as_str() == u2_agent_id.as_str()));
}

async fn seed_task(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    agent_id: Option<&str>,
    status: String,
    title: &str,
) -> String {
    let now = now_rfc3339();
    let task_id = new_uuid_v4();
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status,
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    if let Some(agent_id) = agent_id {
        TaskRoleAssignmentRepo::assign(
            db,
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                role_name: "coder".to_owned(),
                assignee_type: Some(crate::AssigneeKind::Agent),
                assignee_id: Some(agent_id.to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("role assignment creates");
    }
    task_id
}

async fn seed_workspace_for_task(db: &SqliteDb, task_id: &str, repo_id: &str) -> String {
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: format!("/tmp/forge/worktrees/{task_id}"),
            branch: format!("forge/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");
    workspace_id
}

#[tokio::test]
async fn active_workspace_lease_can_be_renewed_while_execution_is_running() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "in_progress".to_owned(),
        "Long-running repository task",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, true)
        .await
        .expect("task lookup")
        .expect("task exists");
    let now = chrono::Utc::now();
    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.to_rfc3339()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        },
    )
    .await
    .expect("running execution");
    let issued_at = (now - chrono::Duration::minutes(14)).to_rfc3339();
    let original_expiry = (now + chrono::Duration::minutes(1)).to_rfc3339();
    let lease =
        WorkspaceLeaseRepo::issue(
            &db,
            CreateWorkspaceLease {
                id: new_uuid_v4(),
                project_id,
                task_id,
                task_version: task.version,
                execution_id: execution.id,
                operation_idempotency_key: new_uuid_v4(),
                repository_binding_id: repo_id,
                base_ref: "main".to_owned(),
                role: "worker".to_owned(),
                capabilities_json: r#"["repository_write"]"#.to_owned(),
                assigned_principal_type: "agent".to_owned(),
                assigned_principal_id: agent_id,
                capability_profile_revision: "forge.capability-profile/v1".to_owned(),
                capability_profile_digest:
                    "sha256:eeb061a14ab862e1a7b16989ef637293ba538f46122ff28b30313d330dbae4a8"
                        .to_owned(),
                issuing_principal_type: "system".to_owned(),
                issuing_principal_id: "task-service-scheduler".to_owned(),
                issued_at: issued_at.clone(),
                expires_at: original_expiry,
                created_at: issued_at.clone(),
                updated_at: issued_at,
            },
        )
        .await
        .expect("lease issue");
    let renewed_expiry = (now + chrono::Duration::minutes(15)).to_rfc3339();
    let renewed = WorkspaceLeaseRepo::renew_active(
        &db,
        &now.to_rfc3339(),
        &(now + chrono::Duration::minutes(5)).to_rfc3339(),
        &renewed_expiry,
        10,
    )
    .await
    .expect("lease renewal");
    assert_eq!(renewed.len(), 1);
    assert_eq!(renewed[0].id, lease.id);
    assert_eq!(renewed[0].expires_at, renewed_expiry);
    assert_eq!(renewed[0].version, lease.version + 1);

    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&renewed[0].task_id)
        .execute(db.pool())
        .await
        .expect("make the lease's Task version stale");
    let retry_now = now + chrono::Duration::seconds(1);
    let rejected = WorkspaceLeaseRepo::renew_active(
        &db,
        &retry_now.to_rfc3339(),
        &(now + chrono::Duration::minutes(20)).to_rfc3339(),
        &(now + chrono::Duration::minutes(30)).to_rfc3339(),
        10,
    )
    .await
    .expect("stale lease is skipped instead of poisoning the heartbeat pass");
    assert!(rejected.is_empty());
}

#[tokio::test]
async fn prebaseline_discovery_task_is_admitted_to_running_execution() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let now = now_rfc3339();
    let user_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, created_at, updated_at)
         VALUES (?, ?, 'test', ?, ?)",
    )
    .bind(&user_id)
    .bind(format!("{user_id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user fixture");
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&user_id)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project owner fixture");
    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter fixture");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES (?, ?, 1, 'approved', 'test', 'test', '{}', 'charter',
                 'user', ?, 'content', 'rendered', ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision fixture");
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&charter_revision_id)
        .bind(&charter_id)
        .execute(db.pool())
        .await
        .expect("charter pointer");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 1
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&charter_revision_id)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("Project Charter binding");
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Read-only discovery".to_owned(),
            description: None,
            task_type: "discovery".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("discovery Task");
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, capability_class, risk_class,
          runnable, created_at, updated_at)
         VALUES (?, ?, ?, 'repository_read', 'low', 0, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("prebaseline governance");

    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id,
            agent_id: Some(agent_id),
            role: "coder".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: Some(now.clone()),
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("prebaseline discovery execution is admitted");
    assert_eq!(execution.status, ExecutionStatus::Running);
}

async fn seed_terminal_session(
    db: &SqliteDb,
    task_id: &str,
    workspace_id: &str,
    user_id: &str,
    created_at: &str,
) -> crate::TerminalSession {
    TerminalSessionRepo::create_terminal_session(
        db,
        CreateTerminalSession {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            daemon_id: None,
            created_by_user_id: user_id.to_owned(),
            rows: 24,
            cols: 80,
            created_at: created_at.to_owned(),
        },
    )
    .await
    .expect("terminal session creates")
}

#[tokio::test]
async fn terminal_session_create_get_and_list_filters_running_and_ended() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Terminal task",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;

    let running = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T00:00:00Z",
    )
    .await;
    let ended = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T00:01:00Z",
    )
    .await;

    let running = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &running.id,
        running.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T00:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T00:00:01Z".to_owned()),
            ended_at: None,
            pid: Some(100),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session starts running");
    let ended = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &ended.id,
        ended.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: Some("2026-05-20T00:01:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T00:01:05Z".to_owned()),
            ended_at: Some("2026-05-20T00:01:05Z".to_owned()),
            pid: Some(101),
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: Some("process exited".to_owned()),
        },
    )
    .await
    .expect("session exits");

    let loaded = TerminalSessionRepo::get_terminal_session(&db, &running.id)
        .await
        .expect("session loads")
        .expect("session exists");
    assert_eq!(loaded.status, TerminalSessionStatus::Running);
    assert_eq!(loaded.pid, Some(100));

    let active = TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, false)
        .await
        .expect("active sessions list");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, running.id);

    let all = TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, true)
        .await
        .expect("all sessions list");
    assert_eq!(
        all.iter().map(|session| &session.id).collect::<Vec<_>>(),
        vec![&running.id, &ended.id,]
    );

    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_task(&db, &task_id)
            .await
            .expect("task running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_user(&db, &user_id)
            .await
            .expect("user running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_running_terminal_sessions_for_workspace(&db, &workspace_id)
            .await
            .expect("workspace running sessions list")
            .len(),
        1
    );
    assert_eq!(
        TerminalSessionRepo::list_all_running_terminal_sessions(&db)
            .await
            .expect("all running sessions list")
            .len(),
        1
    );
}

#[tokio::test]
async fn terminal_session_status_updates_increment_version() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Terminal status",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T01:00:00Z",
    )
    .await;

    assert_eq!(session.status, TerminalSessionStatus::Starting);
    assert_eq!(session.version, 1);

    let running = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T01:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T01:00:02Z".to_owned()),
            ended_at: None,
            pid: Some(4242),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session transitions to running");
    assert_eq!(running.status, TerminalSessionStatus::Running);
    assert_eq!(running.version, 2);
    assert_eq!(running.pid, Some(4242));

    let exited = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &running.id,
        running.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: running.started_at.clone(),
            last_activity_at: Some("2026-05-20T01:01:00Z".to_owned()),
            ended_at: Some("2026-05-20T01:01:00Z".to_owned()),
            pid: running.pid,
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: Some("process exited".to_owned()),
        },
    )
    .await
    .expect("session transitions to exited");
    assert_eq!(exited.status, TerminalSessionStatus::Exited);
    assert_eq!(exited.version, 3);
    assert_eq!(exited.exit_code, Some(0));
    assert_eq!(exited.ended_at.as_deref(), Some("2026-05-20T01:01:00Z"));
}

#[tokio::test]
async fn terminal_session_size_update_touches_activity() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Terminal resize",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T02:00:00Z",
    )
    .await;

    let resized = TerminalSessionRepo::update_terminal_session_size(
        &db,
        &session.id,
        40,
        120,
        "2026-05-20T02:00:10Z",
    )
    .await
    .expect("session resizes");
    assert_eq!(resized.rows, 40);
    assert_eq!(resized.cols, 120);
    assert_eq!(
        resized.last_activity_at.as_deref(),
        Some("2026-05-20T02:00:10Z")
    );

    TerminalSessionRepo::touch_terminal_session_activity(&db, &session.id, "2026-05-20T02:00:20Z")
        .await
        .expect("session activity touches");
    let touched = TerminalSessionRepo::get_terminal_session(&db, &session.id)
        .await
        .expect("session loads")
        .expect("session exists");
    assert_eq!(
        touched.last_activity_at.as_deref(),
        Some("2026-05-20T02:00:20Z")
    );
}

#[tokio::test]
async fn terminal_session_status_update_detects_version_conflict() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Terminal conflict",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T03:00:00Z",
    )
    .await;

    TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Running,
            started_at: Some("2026-05-20T03:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T03:00:01Z".to_owned()),
            ended_at: None,
            pid: Some(500),
            exit_code: None,
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await
    .expect("session starts running");

    let stale = TerminalSessionRepo::update_terminal_session_status(
        &db,
        &session.id,
        session.version,
        UpdateTerminalSessionStatus {
            status: TerminalSessionStatus::Exited,
            started_at: Some("2026-05-20T03:00:01Z".to_owned()),
            last_activity_at: Some("2026-05-20T03:00:02Z".to_owned()),
            ended_at: Some("2026-05-20T03:00:02Z".to_owned()),
            pid: Some(500),
            exit_code: Some(0),
            exit_signal: None,
            exit_reason: None,
        },
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
}

#[tokio::test]
async fn terminal_sessions_cascade_when_workspace_is_deleted() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let user_id = seed_user(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Terminal cascade",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let session = seed_terminal_session(
        &db,
        &task_id,
        &workspace_id,
        &user_id,
        "2026-05-20T04:00:00Z",
    )
    .await;

    WorkspaceRepo::delete(&db, &workspace_id)
        .await
        .expect("workspace deletes");

    assert!(TerminalSessionRepo::get_terminal_session(&db, &session.id)
        .await
        .expect("session lookup succeeds")
        .is_none());
    assert!(
        TerminalSessionRepo::list_terminal_sessions_for_task(&db, &task_id, true)
            .await
            .expect("sessions list")
            .is_empty()
    );
}

async fn seed_ordered_task(
    db: &SqliteDb,
    project_id: &str,
    repo_id: &str,
    parent_task_id: Option<&str>,
    subtask_order: Option<i64>,
    title: &str,
    created_at: &str,
) -> Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            repo_id: Some(repo_id.to_owned()),
            parent_task_id: parent_task_id.map(str::to_owned),
            subtask_order,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("ordered task creates")
}

#[tokio::test]
async fn task_list_hides_cancelled_and_archived_by_default() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let visible_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Visible",
    )
    .await;
    let cancelled_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "cancelled".to_owned(),
        "Cancelled",
    )
    .await;
    let archived_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "done".to_owned(),
        "Archived",
    )
    .await;
    let archived = TaskRepo::get_by_id(&db, &archived_id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::archive(
        &db,
        ArchiveTask {
            id: archived_id.clone(),
            expected_version: archived.version,
            archived_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let default_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    let default_ids = default_page
        .items
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(default_ids, vec![visible_id.as_str()]);

    let included_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: vec![agent_id],
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: true,
            include_cancelled: true,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    let included_ids = included_page
        .items
        .iter()
        .map(|task| task.id.as_str())
        .collect::<Vec<_>>();
    assert!(included_ids.contains(&visible_id.as_str()));
    assert!(included_ids.contains(&cancelled_id.as_str()));
    assert!(included_ids.contains(&archived_id.as_str()));
}

#[tokio::test]
async fn task_list_filters_by_user_assignee() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let human_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Human task",
    )
    .await;
    let agent_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Agent task",
    )
    .await;
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: human_task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::User),
            assignee_id: Some("human".to_owned()),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();

    let user_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: vec!["user".to_owned()],
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    assert_eq!(user_page.items.len(), 1);
    assert_eq!(user_page.items[0].id, human_task_id);

    let human_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: vec!["user".to_owned()],
            assignee_ids: vec!["human".to_owned()],
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .unwrap();
    assert_eq!(human_page.items.len(), 1);
    assert_eq!(human_page.items[0].id, human_task_id);
    assert_ne!(human_page.items[0].id, agent_task_id);
}

#[tokio::test]
async fn task_list_filters_by_search_query() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let alpha_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Alpha release",
    )
    .await;
    let description_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Description only",
    )
    .await;
    let percent_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "100% literal",
    )
    .await;
    let wildcard_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "100x wildcard",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Beta rollout",
    )
    .await;

    let description_task = TaskRepo::get_by_id(&db, &description_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    TaskRepo::update(
        &db,
        UpdateTask {
            id: description_id.clone(),
            expected_version: description_task.version,
            title: None,
            description: Some(Some("Needle lives in this description".to_owned())),
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("description updates");

    let title_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: Some("release".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search by title");
    assert_eq!(title_page.items.len(), 1);
    assert_eq!(title_page.items[0].id, alpha_id);

    let description_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id: project_id.clone(),
            q: Some("needle".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search by description");
    assert_eq!(description_page.items.len(), 1);
    assert_eq!(description_page.items[0].id, description_id);

    let literal_page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: Some("100%".to_owned()),
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: page(10),
        },
    )
    .await
    .expect("search escapes wildcards");
    assert_eq!(literal_page.items.len(), 1);
    assert_eq!(literal_page.items[0].id, percent_id);
    assert_ne!(literal_page.items[0].id, wildcard_id);
}

#[tokio::test]
async fn migration_creates_schema_and_enforces_foreign_keys() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");

    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let agent_id = new_uuid_v4();
    let task_id = new_uuid_v4();
    let execution_id = new_uuid_v4();
    let review_id = new_uuid_v4();
    let db = SqliteDb::new(pool.clone());
    let daemon_id = seed_daemon(&db).await;

    assert!(validate_uuid_v4(&project_id));

    sqlx::query(
        "INSERT INTO project (id, name, settings, created_at, updated_at) VALUES (?, ?, '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind("Forge")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("project inserts");

    sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    .bind(&repo_id)
    .bind(&project_id)
    .bind("forge")
    .bind("https://example.com/forge.git")
    .bind("/tmp/forge-test-repo")
    .bind("direct_merge")
    .bind("main")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("repo inserts");

    AgentRepo::create(
        &db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent inserts");

    sqlx::query(
        "INSERT INTO task (id, project_id, repo_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&repo_id)
    .bind("Build DB foundation")
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("task inserts");

    sqlx::query(
        "INSERT INTO execution (id, task_id, agent_id, role, status, logs_path, created_at, updated_at) VALUES (?, ?, ?, 'executor', 'running', ?, ?, ?)",
    )
    .bind(&execution_id)
    .bind(&task_id)
    .bind(&agent_id)
    .bind(format!("sessions/{task_id}/processes/{execution_id}.jsonl"))
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("execution inserts");

    sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, 1, 'running', '[]', ?, ?, ?)",
    )
    .bind(&review_id)
    .bind(&task_id)
    .bind(&execution_id)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await
    .expect("review inserts");

    let bad_execution_result = sqlx::query(
        "INSERT INTO execution (id, task_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(new_uuid_v4())
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await;
    assert!(bad_execution_result.is_err());

    let bad_review_result = sqlx::query(
        "INSERT INTO review (id, task_id, execution_id, attempt_number, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(new_uuid_v4())
    .bind(&now)
    .bind(&now)
    .execute(&pool)
    .await;
    assert!(bad_review_result.is_err());
}

#[tokio::test]
async fn delete_lifecycle_foreign_keys_match_repository_operations() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Delete lifecycle",
    )
    .await;
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();

    WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge-delete-lifecycle".to_owned(),
            branch: workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");

    sqlx::query(
        "INSERT INTO transition_log (id, task_id, from_state, to_state, trigger_name, triggered_by, trigger_reason, created_at) VALUES (?, ?, 'todo', 'in_progress', NULL, 'system', 'test', ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("transition log creates");

    WorkspaceRepo::delete(&db, &workspace_id)
        .await
        .expect("workspace delete clears execution link");
    let execution = ExecutionRepo::get_by_id(&db, &execution.id)
        .await
        .expect("execution loads")
        .expect("execution remains");
    assert_eq!(execution.workspace_id, None);

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project delete cascades through task data");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("task count"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM transition_log WHERE task_id = ?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .expect("transition count"),
        0
    );

    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Repo cascade",
    )
    .await;
    RepoRepo::delete(&db, &repo_id)
        .await
        .expect("repo delete cascades task data");
    assert!(TaskRepo::get_by_id(&db, &task_id, true)
        .await
        .expect("task lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn sqlite_repo_create_round_trips_local_path() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let repo = RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_id,
            project_id,
            name: "forge".to_owned(),
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: Some("/tmp/forge-test-repo".to_owned()),
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("local repo creates");

    assert_eq!(repo.work_mode, WorkMode::DirectMerge);
    assert_eq!(repo.local_path, Some("/tmp/forge-test-repo".to_owned()));
    assert_eq!(repo.remote_url, "https://example.com/forge.git");
}

#[tokio::test]
async fn sqlite_repo_create_round_trips_remote_url() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let repo = RepoRepo::create(
        &db,
        CreateRepo {
            id: repo_id,
            project_id,
            name: "forge".to_owned(),
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: None,
            work_mode: WorkMode::DirectMerge,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("remote repo creates");

    assert_eq!(repo.work_mode, WorkMode::DirectMerge);
    assert_eq!(repo.local_path, None);
    assert_eq!(repo.remote_url, "https://example.com/forge.git");
}

#[tokio::test]
async fn sqlite_repo_create_rejects_missing_remote_url() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let project_id = new_uuid_v4();

    ProjectRepo::create(
        &db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    let result = sqlx::query("INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(new_uuid_v4())
        .bind(project_id)
        .bind("forge")
        .bind(None::<String>)
        .bind(None::<String>)
        .bind(WorkMode::DirectMerge.to_string())
        .bind("main")
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .map_err(crate::DbError::from);

    assert!(matches!(result, Err(DbError::Sqlx(_))));
}

#[tokio::test]
async fn sqlite_execution_role_auditor_round_trips() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "review".to_string(),
        "Audit me",
    )
    .await;
    let execution_id = new_uuid_v4();

    let created = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "auditor".to_string(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("auditor execution creates");

    assert_eq!(created.role, "auditor".to_string());

    let loaded = ExecutionRepo::get_by_id(&db, &execution_id)
        .await
        .expect("auditor execution loads")
        .expect("auditor execution exists");
    assert_eq!(loaded.role, "auditor".to_string());
    assert_eq!(
        ExecutionRepo::list_by_task(&db, &task_id, page(10))
            .await
            .expect("executions list")
            .items
            .first()
            .map(|execution| &execution.role),
        Some(&"auditor".to_string())
    );
}

#[tokio::test]
async fn migration_runner_is_idempotent() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");

    run_migrations(&pool).await.expect("first run succeeds");
    run_migrations(&pool).await.expect("second run succeeds");

    let applied_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration")
        .fetch_one(&pool)
        .await
        .expect("migration count loads");
    let expected_count = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"))
        .expect("migration directory reads")
        .filter(|entry| {
            entry
                .as_ref()
                .ok()
                .and_then(|entry| entry.path().file_name()?.to_str().map(str::to_owned))
                .is_some_and(|filename| filename.starts_with('V') && filename.ends_with(".sql"))
        })
        .count() as i64;
    assert_eq!(applied_count, expected_count);
}

#[tokio::test]
async fn task_board_revision_migration_preserves_tasks_and_tracks_board_changes() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    sqlx::raw_sql(
        r#"
        CREATE TABLE project (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE task (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            status TEXT NOT NULL,
            board_position REAL NOT NULL,
            deleted_at TEXT,
            archived_at TEXT
        );
        INSERT INTO project (id, name) VALUES ('project-1', 'Forge');
        INSERT INTO task (id, project_id, status, board_position)
        VALUES ('task-1', 'project-1', 'todo', 1.0);
        "#,
    )
    .execute(&pool)
    .await
    .expect("pre-v57 schema seeds");

    sqlx::raw_sql(include_str!("../migrations/V057__task_board_revision.sql"))
        .execute(&pool)
        .await
        .expect("v57 applies");

    let preserved = sqlx::query_as::<_, (String, String, f64)>(
        "SELECT id, status, board_position FROM task WHERE id = 'task-1'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing task remains");
    assert_eq!(preserved, ("task-1".to_owned(), "todo".to_owned(), 1.0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = 'project-1'",)
            .fetch_one(&pool)
            .await
            .expect("revision loads"),
        0
    );

    sqlx::query("UPDATE task SET status = 'review' WHERE id = 'task-1'")
        .execute(&pool)
        .await
        .expect("status updates");
    sqlx::query(
        "INSERT INTO task (id, project_id, status, board_position) VALUES ('task-2', 'project-1', 'todo', 2.0)",
    )
    .execute(&pool)
    .await
    .expect("task inserts");
    sqlx::query("UPDATE task SET archived_at = '2026-07-22T00:00:00Z' WHERE id = 'task-2'")
        .execute(&pool)
        .await
        .expect("task archives");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT board_revision FROM project WHERE id = 'project-1'",)
            .fetch_one(&pool)
            .await
            .expect("revision loads"),
        3
    );
}

#[tokio::test]
async fn compare_and_move_is_atomic_versioned_and_idempotent() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _) = seed_project_repo_agent(&db).await;
    let first_id = seed_task(&db, &project_id, &repo_id, None, "todo".to_owned(), "first").await;
    let moved_id = seed_task(&db, &project_id, &repo_id, None, "todo".to_owned(), "moved").await;
    let moved = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let operation_id = new_uuid_v4();
    let board_revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let input = CompareAndMoveTask {
        operation_id: operation_id.clone(),
        project_id: project_id.clone(),
        task_id: moved_id.clone(),
        task_version: moved.version,
        board_revision,
        target_status: "todo".to_owned(),
        target_column_statuses: vec!["todo".to_owned()],
        before_id: None,
        after_id: Some(first_id),
        entry_barrier_json: None,
        transition_log_id: new_uuid_v4(),
        trigger_name: None,
        triggered_by: "user:board_drag".to_owned(),
        trigger_reason: "board reorder".to_owned(),
        rejection: false,
        updated_at: now_rfc3339(),
    };

    let committed = TaskBoardRepo::compare_and_move_task(&db, input.clone())
        .await
        .expect("move commits");
    let result = match committed {
        MoveTaskPersistence::Committed { result, .. } => result,
        MoveTaskPersistence::Replayed(_) => panic!("first move must commit"),
    };
    assert_eq!(result.task.version, moved.version + 1);
    assert!(result.task.board_position < moved.board_position);
    assert!(result.board_revision > board_revision);
    TaskBoardRepo::complete_move_operation(&db, &operation_id, &result, &now_rfc3339())
        .await
        .expect("operation completes");

    let replayed = TaskBoardRepo::replay_move_task(&db, &operation_id, &input.identity())
        .await
        .expect("replay loads")
        .expect("completed result exists");
    assert_eq!(replayed, *result);
    let loaded = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(loaded.version, result.task.version);

    let conflicting_identity = MoveTaskIdentity {
        before_id: Some(new_uuid_v4()),
        ..input.identity()
    };
    assert!(matches!(
        TaskBoardRepo::replay_move_task(&db, &operation_id, &conflicting_identity).await,
        Err(DbError::MoveOperationConflict { .. })
    ));

    let stale_task = CompareAndMoveTask {
        operation_id: new_uuid_v4(),
        task_version: moved.version,
        board_revision: result.board_revision,
        before_id: None,
        after_id: None,
        ..input.clone()
    };
    assert!(matches!(
        TaskBoardRepo::compare_and_move_task(&db, stale_task).await,
        Err(DbError::TaskVersionConflict { .. })
    ));

    let stale_board = CompareAndMoveTask {
        operation_id: new_uuid_v4(),
        task_version: loaded.version,
        board_revision,
        before_id: None,
        after_id: None,
        ..input
    };
    assert!(matches!(
        TaskBoardRepo::compare_and_move_task(&db, stale_board).await,
        Err(DbError::BoardRevisionConflict { .. })
    ));
}

#[tokio::test]
async fn compare_and_move_validates_empty_columns_neighbors_and_renormalizes() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _) = seed_project_repo_agent(&db).await;
    let before_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "before",
    )
    .await;
    let after_id = seed_task(&db, &project_id, &repo_id, None, "todo".to_owned(), "after").await;
    let moved_id = seed_task(&db, &project_id, &repo_id, None, "todo".to_owned(), "moved").await;
    sqlx::query("UPDATE task SET board_position = 1.0 WHERE id = ?")
        .bind(&before_id)
        .execute(db.pool())
        .await
        .expect("before position sets");
    sqlx::query("UPDATE task SET board_position = 1.000000000001 WHERE id = ?")
        .bind(&after_id)
        .execute(db.pool())
        .await
        .expect("after position sets");
    sqlx::query("UPDATE task SET board_position = 3.0 WHERE id = ?")
        .bind(&moved_id)
        .execute(db.pool())
        .await
        .expect("moved position sets");
    let moved = TaskRepo::get_by_id(&db, &moved_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let renormalized = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            operation_id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: moved_id.clone(),
            task_version: moved.version,
            board_revision: revision,
            target_status: "todo".to_owned(),
            target_column_statuses: vec!["todo".to_owned()],
            before_id: Some(before_id.clone()),
            after_id: Some(after_id.clone()),
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            trigger_name: None,
            triggered_by: "user:board_drag".to_owned(),
            trigger_reason: "board reorder".to_owned(),
            rejection: false,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("tight gap move commits");
    let result = match renormalized {
        MoveTaskPersistence::Committed { result, .. } => result,
        MoveTaskPersistence::Replayed(_) => panic!("move must commit"),
    };
    assert_eq!(result.task.board_position, 1.5);
    assert!(result.board_revision > revision + 1);

    let source_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "source",
    )
    .await;
    let source = TaskRepo::get_by_id(&db, &source_id, false)
        .await
        .expect("source loads")
        .expect("source exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let empty_move = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            operation_id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: source_id,
            task_version: source.version,
            board_revision: revision,
            target_status: "review".to_owned(),
            target_column_statuses: vec!["review".to_owned()],
            before_id: None,
            after_id: None,
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            trigger_name: None,
            triggered_by: "user:board_drag".to_owned(),
            trigger_reason: "board move".to_owned(),
            rejection: false,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .expect("empty destination accepts null neighbors");
    assert!(matches!(empty_move, MoveTaskPersistence::Committed { .. }));

    let another_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "another",
    )
    .await;
    let another = TaskRepo::get_by_id(&db, &another_id, false)
        .await
        .expect("another loads")
        .expect("another exists");
    let revision = TaskBoardRepo::board_revision(&db, &project_id)
        .await
        .expect("revision loads");
    let nonempty = TaskBoardRepo::compare_and_move_task(
        &db,
        CompareAndMoveTask {
            operation_id: new_uuid_v4(),
            project_id,
            task_id: another_id,
            task_version: another.version,
            board_revision: revision,
            target_status: "review".to_owned(),
            target_column_statuses: vec!["review".to_owned()],
            before_id: None,
            after_id: None,
            entry_barrier_json: None,
            transition_log_id: new_uuid_v4(),
            trigger_name: None,
            triggered_by: "user:board_drag".to_owned(),
            trigger_reason: "board move".to_owned(),
            rejection: false,
            updated_at: now_rfc3339(),
        },
    )
    .await;
    assert!(matches!(nonempty, Err(DbError::InvalidTaskMove(_))));
}

#[tokio::test]
async fn notification_repo_crud_and_cascade_delete() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Notification target",
    )
    .await;

    let notification = NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "task.blocked".to_owned(),
            title: "Task blocked".to_owned(),
            body: Some("Need input".to_owned()),
            read: false,
            created_at: now.clone(),
        },
    )
    .await
    .expect("notification creates");
    assert!(!notification.read);

    let page = NotificationRepo::list(
        &db,
        NotificationListQuery {
            project_id: Some(project_id.clone()),
            read: Some(false),
            page: page(20),
        },
    )
    .await
    .expect("notifications list");
    assert_eq!(page.items.len(), 1);

    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count"),
        1
    );

    let marked = NotificationRepo::mark_read(&db, &notification.id)
        .await
        .expect("mark read");
    assert!(marked.read);
    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count after mark read"),
        0
    );

    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "review.failed".to_owned(),
            title: "Review failed".to_owned(),
            body: None,
            read: false,
            created_at: now.clone(),
        },
    )
    .await
    .expect("second notification creates");
    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id.clone()),
            event_type: "merge.failed".to_owned(),
            title: "Merge failed".to_owned(),
            body: Some("conflict".to_owned()),
            read: false,
            created_at: now,
        },
    )
    .await
    .expect("third notification creates");

    assert_eq!(
        NotificationRepo::mark_all_read(&db, Some(&project_id))
            .await
            .expect("mark all read"),
        2
    );
    assert_eq!(
        NotificationRepo::unread_count(&db, Some(&project_id))
            .await
            .expect("unread count after mark all"),
        0
    );

    NotificationRepo::delete(&db, &notification.id)
        .await
        .expect("notification delete");

    let remaining_before_cascade: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications before cascade");
    assert_eq!(remaining_before_cascade, 2);

    sqlx::query("DELETE FROM task WHERE id = ?")
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("hard delete task");
    let remaining_after_task_delete: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications after task delete");
    assert_eq!(remaining_after_task_delete, 0);

    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "Notification target 2",
    )
    .await;
    NotificationRepo::create(
        &db,
        crate::CreateNotification {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            task_id: Some(task_id),
            event_type: "task.done".to_owned(),
            title: "Done".to_owned(),
            body: None,
            read: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("notification for project cascade");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("project delete cascade");
    let remaining_after_project_delete: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM notification WHERE project_id = ?")
            .bind(&project_id)
            .fetch_one(db.pool())
            .await
            .expect("count notifications after project delete");
    assert_eq!(remaining_after_project_delete, 0);
}

#[tokio::test]
async fn sqlite_repositories_create_update_list_and_get_logs() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;

    let project = ProjectRepo::update(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: Some("Forge DB".to_owned()),
            settings: None,
            primary_repo_id: None,
            paused_at: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project updates");
    assert_eq!(project.name, "Forge DB");

    let repo = RepoRepo::update(
        &db,
        UpdateRepo {
            id: repo_id.clone(),
            name: None,
            local_path: None,
            remote_url: None,
            work_mode: None,
            default_branch: Some("trunk".to_owned()),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo updates");
    assert_eq!(repo.default_branch, "trunk");

    let skill_id = new_uuid_v4();
    SkillRepo::create(
        &db,
        CreateSkill {
            id: skill_id.clone(),
            project_id: project_id.clone(),
            name: "Rust".to_owned(),
            content: "Use cargo test".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("skill creates");
    let skill = SkillRepo::update(
        &db,
        UpdateSkill {
            id: skill_id,
            name: Some("SQLite".to_owned()),
            content: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("skill updates");
    assert_eq!(skill.name, "SQLite");

    let agent = AgentRepo::update(
        &db,
        UpdateAgent {
            id: agent_id.clone(),
            expected_version: 1,
            name: Some("codex".to_owned()),
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: None,
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: Some(AgentStatus::Busy),
            last_heartbeat_at: Some(Some(now.clone())),
            is_default: None,
            paused: None,
            prompt_template: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent updates");
    assert_eq!(agent.version, 2);

    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Implement repo".to_owned(),
            description: Some("SQLite".to_owned()),
            task_type: "task".to_owned(),
            status: "todo".to_string(),
            is_automation: false,
            priority: 10,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("role assignment creates");
    let task = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: 1,
            title: Some("Implement SQLite repo".to_owned()),
            description: None,
            priority: Some(20),
            merge_config: None,
            plan: Some(Some("Map rows manually".to_owned())),
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task updates");
    assert_eq!(task.version, 2);

    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_string(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("initial prompt".to_owned()),
            logs_path: Some("logs/run.jsonl".to_owned()),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution creates");
    let execution = ExecutionRepo::update(
        &db,
        UpdateExecution {
            id: execution_id.clone(),
            status: Some(ExecutionStatus::Completed),
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            agent_session_id: Some(Some("session".to_owned())),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some(Some("done".to_owned())),
            logs_path: None,
            before_sha: None,
            after_sha: Some(Some("abc123".to_owned())),
            error: None,
            executor_config_snapshot_json: None,
            updated_at: now.clone(),
        },
    )
    .await
    .expect("execution updates");
    assert_eq!(execution.status, ExecutionStatus::Completed);
    assert_eq!(execution.prompt.as_deref(), Some("initial prompt"));
    assert_eq!(execution.summary.as_deref(), Some("done"));
    assert_eq!(
        ExecutionRepo::get_logs_path(&db, &execution_id)
            .await
            .expect("logs path loads"),
        Some("logs/run.jsonl".to_owned())
    );

    let review_id = new_uuid_v4();
    ReviewRepo::create(
        &db,
        CreateReview {
            id: review_id.clone(),
            task_id: task_id.clone(),
            execution_id: execution_id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    assert_eq!(
        ReviewRepo::next_attempt_number(&db, &task_id)
            .await
            .expect("next attempt loads"),
        2
    );
    let review = ReviewRepo::update_status(
        &db,
        &review_id,
        ReviewStatus::Passed,
        r#"[{"index":0,"exit_code":0}]"#.to_owned(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review updates");
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.step_results_json, r#"[{"index":0,"exit_code":0}]"#);
    assert_eq!(review.finished_at, Some(now.clone()));
    let review_events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("review outcome event lists");
    assert!(review_events.iter().any(|event| {
        event.event_type == "review.status_changed"
            && event.entity_id == review_id
            && event.scope_id == task_id
    }));

    let workspace_id = new_uuid_v4();
    let workspace = WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge/worktrees/task/forge".to_owned(),
            branch: ::workspace::task_branch_name(&task_id),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");
    assert_eq!(workspace.cleanup_after, None);
    let cleanup_after = "2026-04-14T01:00:00Z".to_owned();
    let workspace =
        WorkspaceRepo::set_cleanup_after(&db, &workspace_id, Some(cleanup_after.clone()), &now)
            .await
            .expect("cleanup deadline sets");
    assert_eq!(workspace.cleanup_after, Some(cleanup_after));
    let pending = WorkspaceRepo::list_pending_cleanup(&db, "2026-04-14T02:00:00Z")
        .await
        .expect("pending cleanup lists");
    assert_eq!(pending.len(), 1);
    let workspace = WorkspaceRepo::mark_cleaned(&db, &workspace_id, &now)
        .await
        .expect("workspace marks cleaned");
    assert_eq!(workspace.status, WorkspaceStatus::Cleaned);
    assert_eq!(workspace.cleanup_after, None);

    assert_eq!(
        ProjectRepo::list(&db, page(10)).await.unwrap().total_count,
        Some(1)
    );
    assert_eq!(
        RepoRepo::list_by_project(&db, &project_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        SkillRepo::list_by_project(&db, &project_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        AgentRepo::list(
            &db,
            AgentListQuery {
                status: Some(AgentStatus::Busy),
                executor_type: None,
                capabilities: vec!["rust".to_owned()],
                page: page(10),
            },
        )
        .await
        .unwrap()
        .total_count,
        Some(1)
    );
    assert_eq!(
        TaskRepo::list(
            &db,
            TaskListQuery {
                project_id,
                q: None,
                statuses: vec!["todo".to_string()],
                agent_ids: vec![agent_id],
                assignee_types: Vec::new(),
                assignee_ids: Vec::new(),
                priority: Some(20),
                include_archived: false,
                include_cancelled: false,
                include_deleted: false,
                page: page(10),
            },
        )
        .await
        .unwrap()
        .total_count,
        Some(1)
    );
    assert_eq!(
        ExecutionRepo::list_by_task(&db, &task_id, page(10))
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        ReviewRepo::list_by_task(&db, &task_id).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn workspace_task_id_unique_is_preserved() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Workspace owner",
    )
    .await;

    WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            repo_id: repo_id.clone(),
            worktree_path: "/tmp/forge/worktrees/task/one".to_owned(),
            branch: format!("task/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    let duplicate = WorkspaceRepo::create(
        &db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id,
            repo_id,
            worktree_path: "/tmp/forge/worktrees/task/two".to_owned(),
            branch: "task/duplicate".to_owned(),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await;

    assert!(duplicate.is_err());
}

#[tokio::test]
async fn next_subtask_order_appends_after_existing_siblings() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;

    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        0
    );
    let first_order = TaskRepo::next_subtask_order(&db, &parent_task_id)
        .await
        .expect("first order loads");
    seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(first_order),
        "First",
        &now,
    )
    .await;
    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        1
    );
    let second_order = TaskRepo::next_subtask_order(&db, &parent_task_id)
        .await
        .expect("second order loads");
    seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(second_order),
        "Second",
        &now,
    )
    .await;

    assert_eq!(
        TaskRepo::next_subtask_order(&db, &parent_task_id)
            .await
            .expect("next order loads"),
        2
    );
}

#[tokio::test]
async fn list_subtasks_ordered_uses_subtask_order_before_tiebreakers() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;

    seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(2),
        "Third",
        "2026-04-18T00:00:00Z",
    )
    .await;
    seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(0),
        "First",
        "2026-04-18T00:02:00Z",
    )
    .await;
    seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(1),
        "Second",
        "2026-04-18T00:01:00Z",
    )
    .await;

    let titles = TaskRepo::list_subtasks_ordered(&db, &parent_task_id)
        .await
        .expect("subtasks list")
        .into_iter()
        .map(|task| task.title)
        .collect::<Vec<_>>();

    assert_eq!(titles, vec!["First", "Second", "Third"]);
}

#[tokio::test]
async fn reorder_subtasks_persists_and_rejects_invalid_orders() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let parent_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_owned(),
        "Parent",
    )
    .await;
    let first = seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(0),
        "First",
        "2026-04-18T00:00:00Z",
    )
    .await;
    let second = seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        Some(&parent_task_id),
        Some(1),
        "Second",
        "2026-04-18T00:01:00Z",
    )
    .await;

    let reordered_at = "2026-04-18T00:02:00Z";
    TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[second.id.clone(), first.id.clone()],
        reordered_at,
    )
    .await
    .expect("subtasks reorder");

    let ordered = TaskRepo::list_subtasks_ordered(&db, &parent_task_id)
        .await
        .expect("subtasks list");
    assert_eq!(ordered[0].id, second.id);
    assert_eq!(ordered[0].subtask_order, Some(0));
    assert_eq!(ordered[0].updated_at, reordered_at);
    assert_eq!(ordered[1].id, first.id);
    assert_eq!(ordered[1].subtask_order, Some(1));

    let unknown = TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[ordered[0].id.clone(), new_uuid_v4()],
        "2026-04-18T00:03:00Z",
    )
    .await;
    assert!(matches!(unknown, Err(DbError::NotFound)));

    let mismatched_length = TaskRepo::reorder_subtasks(
        &db,
        &parent_task_id,
        &[ordered[0].id.clone()],
        "2026-04-18T00:04:00Z",
    )
    .await;
    assert!(matches!(mismatched_length, Err(DbError::InvalidTransition)));
}

#[tokio::test]
async fn task_list_orders_equal_board_positions_by_created_at() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let later = seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        None,
        None,
        "Later",
        "2026-04-18T00:01:00Z",
    )
    .await;
    let earlier = seed_ordered_task(
        &db,
        &project_id,
        &repo_id,
        None,
        None,
        "Earlier",
        "2026-04-18T00:00:00Z",
    )
    .await;
    for task_id in [&later.id, &earlier.id] {
        sqlx::query("UPDATE task SET board_position = 10.0 WHERE id = ?")
            .bind(task_id)
            .execute(db.pool())
            .await
            .expect("board position updates");
    }

    let page = TaskRepo::list(
        &db,
        TaskListQuery {
            project_id,
            q: None,
            statuses: Vec::new(),
            agent_ids: Vec::new(),
            assignee_types: Vec::new(),
            assignee_ids: Vec::new(),
            priority: None,
            include_archived: false,
            include_cancelled: false,
            include_deleted: false,
            page: PageRequest {
                cursor: None,
                limit: 10,
                include_total: false,
                sort_by: SortBy::BoardPosition,
                sort_order: SortOrder::Asc,
            },
        },
    )
    .await
    .expect("tasks list");

    assert_eq!(page.items[0].id, earlier.id);
    assert_eq!(page.items[1].id, later.id);
}

#[tokio::test]
async fn sqlite_repositories_enforce_versions_transitions_claims_and_cursors() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = new_uuid_v4();
    TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Claim me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_string(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "coder".to_owned(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("role assignment creates");

    let bad_update = TaskRepo::update(
        &db,
        UpdateTask {
            id: task_id.clone(),
            expected_version: 99,
            title: Some("stale".to_owned()),
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            task_state_config: None,
            parent_task_id: None,
            updated_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(bad_update, Err(DbError::VersionConflict)));

    let mut tx = db.pool().begin().await.expect("transaction starts");
    let claimed = TaskRepo::claim(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: 1,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            capacity_statuses: vec![
                // The task being claimed may already be role-assigned in a
                // source state that participates in capacity accounting.
                // It must not consume a slot before its own first claim.
                "todo".to_owned(),
                "in_progress".to_owned(),
                "review".to_owned(),
                "merging".to_owned(),
            ],
            execution: CreateExecution {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                agent_id: Some(agent_id.clone()),
                role: "executor".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            max_concurrent_tasks: 1,
            claimed_at: now.clone(),
        },
    )
    .await
    .expect("task claims");
    tx.commit().await.expect("claim commits");
    assert_eq!(claimed.task.status, "in_progress".to_string());
    assert_eq!(
        AgentRepo::count_active_tasks(&db, &agent_id).await.unwrap(),
        1
    );

    let invalid_cursor = ProjectRepo::list(
        &db,
        PageRequest {
            cursor: Some("not-base64-json".to_owned()),
            limit: 10,
            include_total: false,
            sort_by: SortBy::Id,
            sort_order: SortOrder::Asc,
        },
    )
    .await;
    assert!(matches!(invalid_cursor, Err(DbError::InvalidCursor)));
}

#[tokio::test]
async fn agent_active_task_count_uses_workflow_state_kinds() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let workflow = serde_json::json!({
        "states": [
            { "name": "todo", "kind": "initial" },
            { "name": "running", "kind": "active" },
            { "name": "waiting_review", "kind": "gate" },
            { "name": "done", "kind": "terminal" }
        ]
    });

    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(workflow.to_string())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("workflow updates");

    seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "running".to_owned(),
        "custom active state",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "waiting_review".to_owned(),
        "custom gate state",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "done".to_owned(),
        "terminal state",
    )
    .await;

    assert_eq!(
        AgentRepo::count_active_tasks(&db, &agent_id).await.unwrap(),
        2
    );
}

#[tokio::test]
async fn agent_task_list_uses_execution_history() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let executed_task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "done".to_owned(),
        "executed task",
    )
    .await;
    seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "done".to_owned(),
        "assigned only task",
    )
    .await;
    let now = now_rfc3339();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: executed_task_id.clone(),
            agent_id: Some(agent_id.clone()),
            role: "coder".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    let page = TaskRepo::list_by_executing_agent(
        &db,
        AgentTaskListQuery {
            agent_id,
            include_archived: false,
            include_cancelled: true,
            include_deleted: false,
            page: PageRequest {
                cursor: None,
                limit: 10,
                include_total: true,
                sort_by: SortBy::UpdatedAt,
                sort_order: SortOrder::Desc,
            },
        },
    )
    .await
    .expect("agent task list loads");

    assert_eq!(page.total_count, Some(1));
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, executed_task_id);
}

#[tokio::test]
async fn task_claim_rejects_active_entry_barrier() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = new_uuid_v4();
    let task = TaskRepo::create(
        &db,
        CreateTask {
            id: task_id.clone(),
            project_id,
            repo_id: Some(repo_id),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Barrier claim".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");
    let task = TaskRepo::set_entry_barrier(
        &db,
        &task.id,
        task.version,
        Some(
            r#"{"state":"todo","status":"running","started_at":"2026-04-28T00:00:00Z"}"#.to_owned(),
        ),
        &now,
    )
    .await
    .expect("barrier sets");

    let mut tx = db.pool().begin().await.expect("transaction starts");
    let result = TaskRepo::claim(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: task.version,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            capacity_statuses: vec!["in_progress".to_owned()],
            execution: CreateExecution {
                id: new_uuid_v4(),
                task_id,
                agent_id: Some(agent_id),
                role: "executor".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
            max_concurrent_tasks: 1,
            claimed_at: now_rfc3339(),
        },
    )
    .await;

    assert!(matches!(result, Err(DbError::InvalidTransition)));
}

#[tokio::test]
async fn test_add_dependency_success() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let dependency_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_string(),
        "Dependent",
    )
    .await;

    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    assert_eq!(
        TaskDependencyRepo::list_dependencies(&db, &task_id)
            .await
            .expect("dependencies list"),
        vec![dependency_id.clone()]
    );
    assert_eq!(
        TaskDependencyRepo::list_dependents(&db, &dependency_id)
            .await
            .expect("dependents list"),
        vec![task_id]
    );
}

#[tokio::test]
async fn test_add_dependency_cycle_rejected() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let first_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "todo".to_string(),
        "First",
    )
    .await;
    let second_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_string(),
        "Second",
    )
    .await;

    TaskDependencyRepo::add_dependency(&db, &second_id, &first_id, &now)
        .await
        .expect("initial dependency adds");
    let result = TaskDependencyRepo::add_dependency(&db, &first_id, &second_id, &now).await;

    assert!(matches!(result, Err(DbError::CycleDetected)));
}

#[tokio::test]
async fn test_dependency_gate_blocks_non_context_holder() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, context_agent_id) = seed_project_repo_agent(&db).await;
    let other_agent_id = new_uuid_v4();
    let other_daemon_id = seed_daemon(&db).await;
    AgentRepo::create(
        &db,
        CreateAgent {
            id: other_agent_id.clone(),
            name: "other".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(other_daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second agent creates");
    let dependency_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&context_agent_id),
        "review".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_string(),
        "Dependent",
    )
    .await;
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: dependency_id.clone(),
            agent_id: Some(context_agent_id),
            role: "executor".to_string(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("dependency execution creates");
    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    let mut tx = db.pool().begin().await.expect("transaction starts");
    let result = TaskRepo::claim(
        &db,
        &mut tx,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(other_agent_id.clone()),
            expected_version: 1,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            capacity_statuses: vec![
                "in_progress".to_owned(),
                "review".to_owned(),
                "merging".to_owned(),
            ],
            execution: CreateExecution {
                id: new_uuid_v4(),
                task_id,
                agent_id: Some(other_agent_id),
                role: "executor".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            max_concurrent_tasks: 1,
            claimed_at: now,
        },
    )
    .await;

    assert!(matches!(result, Err(DbError::DependencyGate)));
}

#[tokio::test]
async fn test_unsatisfied_dependencies_empty_when_done() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let dependency_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        Some(&agent_id),
        "done".to_string(),
        "Dependency",
    )
    .await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_string(),
        "Dependent",
    )
    .await;
    TaskDependencyRepo::add_dependency(&db, &task_id, &dependency_id, &now)
        .await
        .expect("dependency adds");

    assert!(TaskDependencyRepo::unsatisfied_dependencies(&db, &task_id)
        .await
        .expect("unsatisfied dependencies list")
        .is_empty());
}

// ── User / RefreshToken tests ──────────────────────────────────────────────

async fn seed_user(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let id = new_uuid_v4();
    let user = User {
        id: id.clone(),
        email: format!("user-{}@example.com", id),
        password_hash: "hash".to_owned(),
        display_name: None,
        is_admin: false,
        created_at: now.clone(),
        updated_at: now,
    };
    UserRepo::create_user(db, &user).await.expect("seed user");
    id
}

#[tokio::test]
async fn user_crud() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user = User {
        id: new_uuid_v4(),
        email: "crud@example.com".to_owned(),
        password_hash: "hash".to_owned(),
        display_name: Some("Test User".to_owned()),
        is_admin: false,
        created_at: now.clone(),
        updated_at: now,
    };

    UserRepo::create_user(&db, &user)
        .await
        .expect("creates user");

    let by_id = UserRepo::get_user_by_id(&db, &user.id)
        .await
        .expect("no error")
        .expect("user exists");
    assert_eq!(by_id.email, user.email);
    assert_eq!(by_id.display_name.as_deref(), Some("Test User"));

    let by_email = UserRepo::get_user_by_email(&db, &user.email)
        .await
        .expect("no error")
        .expect("user found by email");
    assert_eq!(by_email.id, user.id);

    let deleted = UserRepo::delete_user(&db, &user.id)
        .await
        .expect("no error");
    assert!(deleted);

    let gone = UserRepo::get_user_by_id(&db, &user.id)
        .await
        .expect("no error");
    assert!(gone.is_none());
}

#[tokio::test]
async fn user_email_uniqueness() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let make_user = |id: String| User {
        id,
        email: "dup@example.com".to_owned(),
        password_hash: "hash".to_owned(),
        display_name: None,
        is_admin: false,
        created_at: now.clone(),
        updated_at: now.clone(),
    };

    UserRepo::create_user(&db, &make_user(new_uuid_v4()))
        .await
        .expect("first user creates");
    let err = UserRepo::create_user(&db, &make_user(new_uuid_v4())).await;
    assert!(err.is_err(), "duplicate email must fail");
}

#[tokio::test]
async fn refresh_token_lifecycle() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    let token = RefreshToken {
        id: new_uuid_v4(),
        user_id: user_id.clone(),
        token_hash: "hash-abc".to_owned(),
        family_id: "family-1".to_owned(),
        expires_at: "2099-01-01T00:00:00Z".to_owned(),
        created_at: now,
    };
    RefreshTokenRepo::create_refresh_token(&db, &token)
        .await
        .expect("creates token");

    let found = RefreshTokenRepo::delete_refresh_token_by_hash(&db, "hash-abc")
        .await
        .expect("no error")
        .expect("token returned on first delete");
    assert_eq!(found.user_id, user_id);

    let not_found = RefreshTokenRepo::delete_refresh_token_by_hash(&db, "hash-abc")
        .await
        .expect("no error");
    assert!(not_found.is_none(), "token must not exist after deletion");
}

#[tokio::test]
async fn refresh_token_concurrent_single_winner() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    let token = RefreshToken {
        id: new_uuid_v4(),
        user_id,
        token_hash: "race-hash".to_owned(),
        family_id: "family-race".to_owned(),
        expires_at: "2099-01-01T00:00:00Z".to_owned(),
        created_at: now,
    };
    RefreshTokenRepo::create_refresh_token(&db, &token)
        .await
        .expect("creates token");

    // Two concurrent DELETE RETURNING on the same hash: exactly one wins.
    let (r1, r2) = tokio::join!(
        RefreshTokenRepo::delete_refresh_token_by_hash(&db, "race-hash"),
        RefreshTokenRepo::delete_refresh_token_by_hash(&db, "race-hash"),
    );
    let r1 = r1.expect("no error on r1");
    let r2 = r2.expect("no error on r2");

    let winners = [r1.is_some(), r2.is_some()].iter().filter(|&&b| b).count();
    assert_eq!(
        winners, 1,
        "exactly one concurrent caller must win the token"
    );
}

#[tokio::test]
async fn refresh_token_family_revocation() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    for i in 0..3 {
        RefreshTokenRepo::create_refresh_token(
            &db,
            &RefreshToken {
                id: new_uuid_v4(),
                user_id: user_id.clone(),
                token_hash: format!("fam-hash-{i}"),
                family_id: "family-revoke".to_owned(),
                expires_at: "2099-01-01T00:00:00Z".to_owned(),
                created_at: now.clone(),
            },
        )
        .await
        .expect("creates token");
    }
    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "other-fam-hash".to_owned(),
            family_id: "family-other".to_owned(),
            expires_at: "2099-01-01T00:00:00Z".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("creates other token");

    let deleted = RefreshTokenRepo::delete_refresh_tokens_by_family(&db, "family-revoke")
        .await
        .expect("no error");
    assert_eq!(deleted, 3);

    let remaining = RefreshTokenRepo::get_refresh_tokens_by_user(&db, &user_id)
        .await
        .expect("no error");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].family_id, "family-other");
}

#[tokio::test]
async fn refresh_token_expired_cleanup() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let user_id = seed_user(&db).await;

    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "expired-hash".to_owned(),
            family_id: "family-exp".to_owned(),
            expires_at: "2020-01-01T00:00:00Z".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("creates expired token");

    RefreshTokenRepo::create_refresh_token(
        &db,
        &RefreshToken {
            id: new_uuid_v4(),
            user_id: user_id.clone(),
            token_hash: "valid-hash".to_owned(),
            family_id: "family-valid".to_owned(),
            expires_at: "2099-01-01T00:00:00Z".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("creates valid token");

    let cleaned = RefreshTokenRepo::delete_expired_refresh_tokens(&db)
        .await
        .expect("no error");
    assert_eq!(cleaned, 1);

    let remaining = RefreshTokenRepo::get_refresh_tokens_by_user(&db, &user_id)
        .await
        .expect("no error");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].token_hash, "valid-hash");
}

#[tokio::test]
async fn test_normalize_failure_kinds_migration_backfill() {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");

    // Seed legacy rows below the FK layer, then re-apply V056 over them.
    let mut conn = pool.acquire().await.expect("connection acquires");
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await
        .expect("fk off");
    let now = now_rfc3339();
    let insert = |id: &str,
                  error_annotation: Option<&str>,
                  blocked: Option<&str>,
                  failed: Option<&str>| {
        sqlx::query(
            "INSERT INTO task (id, project_id, repo_id, title, status, error_annotation, blocked_json, failed_json, created_at, updated_at)
             VALUES (?, 'p', 'r', 'legacy', 'blocked', ?, ?, ?, ?, ?)",
        )
        .bind(id.to_owned())
        .bind(error_annotation.map(str::to_owned))
        .bind(blocked.map(str::to_owned))
        .bind(failed.map(str::to_owned))
        .bind(now.clone())
        .bind(now.clone())
    };
    insert(
        "t-alias",
        Some(r#"{"type":"retry_budget_exhausted","blocking_reason":"x"}"#),
        None,
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("alias row inserts");
    insert(
        "t-prose",
        None,
        Some(r#"{"reason":"review retry budget exhausted after 3 attempts","created_at":"2026-01-01T00:00:00Z"}"#),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("prose row inserts");
    insert(
        "t-crash",
        Some(r#"{"type":"crash","message":"boom"}"#),
        None,
        Some(r#"{"reason":"crashed","created_at":"2026-01-01T00:00:00Z","kind":"crash"}"#),
    )
    .execute(&mut *conn)
    .await
    .expect("crash row inserts");
    insert(
        "t-malformed",
        Some("{not json"),
        Some("also not json"),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("malformed row inserts");
    insert(
        "t-unknown",
        None,
        Some(r#"{"reason":"strange","created_at":"2026-01-01T00:00:00Z","kind":"从未见过"}"#),
        None,
    )
    .execute(&mut *conn)
    .await
    .expect("unknown row inserts");
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *conn)
        .await
        .expect("fk on");
    drop(conn);

    sqlx::query("DELETE FROM _migration WHERE version = 56")
        .execute(&pool)
        .await
        .expect("migration marker clears");
    run_migrations(&pool).await.expect("migration re-applies");

    let field = |id: &str, column: &str| {
        let sql = format!("SELECT {column} FROM task WHERE id = ?");
        let pool = pool.clone();
        let id = id.to_owned();
        async move {
            sqlx::query_scalar::<_, Option<String>>(&sql)
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row fetches")
        }
    };

    // Alias type renamed.
    let annotation = field("t-alias", "error_annotation").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&annotation).unwrap()["type"],
        "retry_exhausted"
    );
    // Prose-only row gains the structured kind; other fields untouched.
    let blocked = field("t-prose", "blocked_json").await.unwrap();
    let blocked: serde_json::Value = serde_json::from_str(&blocked).unwrap();
    assert_eq!(blocked["kind"], "retry_exhausted");
    assert_eq!(
        blocked["reason"],
        "review retry budget exhausted after 3 attempts"
    );
    // crash maps to executor_failed in both carriers.
    let annotation = field("t-crash", "error_annotation").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&annotation).unwrap()["type"],
        "executor_failed"
    );
    let failed = field("t-crash", "failed_json").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&failed).unwrap()["kind"],
        "executor_failed"
    );
    // Malformed JSON is untouched.
    assert_eq!(
        field("t-malformed", "error_annotation").await.as_deref(),
        Some("{not json")
    );
    assert_eq!(
        field("t-malformed", "blocked_json").await.as_deref(),
        Some("also not json")
    );
    // Unmappable kinds are untouched (they deserialize to Unknown at read time).
    let blocked = field("t-unknown", "blocked_json").await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&blocked).unwrap()["kind"],
        "从未见过"
    );
}

#[tokio::test]
async fn domain_event_claims_are_ordered_replay_safe_and_deduplicated() {
    let db = sqlite_db().await;
    let created_at = "2026-08-12T20:00:00Z".to_owned();
    let first = CreateDomainEvent {
        id: "event-first".to_owned(),
        event_type: "task.transitioned".to_owned(),
        entity_type: "task".to_owned(),
        entity_id: "task-1".to_owned(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "task".to_owned(),
        scope_id: "task-1".to_owned(),
        correlation_id: "corr-1".to_owned(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some("task-transition:1".to_owned()),
        payload_json: r#"{"to_state":"review"}"#.to_owned(),
        created_at: created_at.clone(),
    };
    let first_row = DomainEventRepo::append_event(&db, first.clone())
        .await
        .expect("first event appends");
    let duplicate = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-duplicate".to_owned(),
            payload_json: r#"{"to_state":"review"}"#.to_owned(),
            ..first
        },
    )
    .await
    .expect("dedupe returns the committed event");
    assert_eq!(duplicate.id, first_row.id);
    let conflicting_dedupe = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-conflicting-dedupe".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-other".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: "task-other".to_owned(),
            correlation_id: "corr-other".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("task-transition:1".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-12T20:00:01Z".to_owned(),
        },
    )
    .await;
    assert!(matches!(conflicting_dedupe, Err(DbError::Check(_))));

    let second = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-second".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-2".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: "task-2".to_owned(),
            correlation_id: "corr-2".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("task-transition:2".to_owned()),
            payload_json: "{}".to_owned(),
            created_at,
        },
    )
    .await
    .expect("second event appends");

    let claim_input = |owner: &str, now: &str, leased_until: &str| ClaimDomainEvents {
        consumer_name: "projection".to_owned(),
        lease_owner: owner.to_owned(),
        now: now.to_owned(),
        leased_until: leased_until.to_owned(),
        limit: 10,
    };
    let claimed = DomainEventRepo::claim_event_batch(
        &db,
        claim_input("worker-a", "2026-08-12T20:01:00Z", "2026-08-12T20:02:00Z"),
    )
    .await
    .expect("events claim");
    assert_eq!(
        claimed
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        ["event-first", "event-second"]
    );

    let second_out_of_order = DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-a".to_owned(),
            event_sequence: second.sequence,
            event_id: second.id.clone(),
            dedupe_key: second.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:01:01Z".to_owned(),
        },
    )
    .await;
    assert!(second_out_of_order.is_err(), "cursor must remain ordered");

    assert!(DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-a".to_owned(),
            event_sequence: first_row.sequence,
            event_id: first_row.id.clone(),
            dedupe_key: first_row.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:01:02Z".to_owned(),
        },
    )
    .await
    .expect("first event completes"));
    assert!(DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-a".to_owned(),
            event_sequence: second.sequence,
            event_id: second.id.clone(),
            dedupe_key: second.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:01:03Z".to_owned(),
        },
    )
    .await
    .expect("second event completes"));
    assert_eq!(
        DomainEventRepo::get_consumer_cursor(&db, "projection")
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        second.sequence
    );
    assert!(!DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-a".to_owned(),
            event_sequence: second.sequence,
            event_id: second.id.clone(),
            dedupe_key: "task-transition:2".to_owned(),
            completed_at: "2026-08-12T20:01:04Z".to_owned(),
        },
    )
    .await
    .expect("duplicate completion is idempotent"));

    // A live lease at the cursor head blocks later sequences from being
    // claimed by another worker; otherwise that worker could never
    // checkpoint its out-of-order receipt.
    let third = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-third".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-3".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: "task-3".to_owned(),
            correlation_id: "corr-3".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("task-transition:3".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-12T20:01:05Z".to_owned(),
        },
    )
    .await
    .expect("third event appends");
    let fourth = DomainEventRepo::append_event(
        &db,
        CreateDomainEvent {
            id: "event-fourth".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-4".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: "task-4".to_owned(),
            correlation_id: "corr-4".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("task-transition:4".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-12T20:01:06Z".to_owned(),
        },
    )
    .await
    .expect("fourth event appends");
    let head_claim = DomainEventRepo::claim_event_batch(
        &db,
        claim_input(
            "worker-head",
            "2026-08-12T20:01:07Z",
            "2026-08-12T20:02:07Z",
        ),
    )
    .await
    .expect("head events claim");
    assert_eq!(head_claim.len(), 2);
    assert_eq!(head_claim[0].id, third.id);
    assert_eq!(head_claim[1].id, fourth.id);
    // The same consumer cannot be claimed concurrently by another owner.
    let blocked_claim = DomainEventRepo::claim_event_batch(
        &db,
        claim_input(
            "worker-other",
            "2026-08-12T20:01:08Z",
            "2026-08-12T20:02:08Z",
        ),
    )
    .await
    .expect("blocked claim succeeds");
    assert!(blocked_claim.is_empty());
    assert!(DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-head".to_owned(),
            event_sequence: third.sequence,
            event_id: third.id.clone(),
            dedupe_key: third.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:01:09Z".to_owned(),
        },
    )
    .await
    .expect("third event completes"));
    assert!(DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "projection".to_owned(),
            lease_owner: "worker-head".to_owned(),
            event_sequence: fourth.sequence,
            event_id: fourth.id.clone(),
            dedupe_key: fourth.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:01:10Z".to_owned(),
        },
    )
    .await
    .expect("fourth event completes"));

    // Simulate a legacy crash after writing a projection receipt but before
    // checkpointing the cursor. Claiming the next batch must repair the
    // contiguous receipt prefix instead of getting stuck behind event-first.
    sqlx::query(
        "INSERT INTO event_consumer_cursor (consumer_name, last_sequence, version, updated_at)
         VALUES ('repair', 0, 1, ?)",
    )
    .bind("2026-08-12T20:02:00Z")
    .execute(db.pool())
    .await
    .expect("repair cursor inserts");
    sqlx::query(
        "INSERT INTO event_projection_receipt (consumer_name, event_id, dedupe_key, processed_at)
         VALUES ('repair', ?, ?, ?)",
    )
    .bind(&first_row.id)
    .bind(first_row.dedupe_key.as_deref().unwrap())
    .bind("2026-08-12T20:02:00Z")
    .execute(db.pool())
    .await
    .expect("orphan receipt inserts");
    let repaired = DomainEventRepo::claim_event_batch(
        &db,
        ClaimDomainEvents {
            consumer_name: "repair".to_owned(),
            lease_owner: "repair-worker".to_owned(),
            now: "2026-08-12T20:02:01Z".to_owned(),
            leased_until: "2026-08-12T20:03:00Z".to_owned(),
            limit: 1,
        },
    )
    .await
    .expect("repair claim succeeds");
    assert_eq!(repaired.len(), 1);
    assert_eq!(repaired[0].id, second.id);
    assert_eq!(
        DomainEventRepo::get_consumer_cursor(&db, "repair")
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        first_row.sequence
    );
    assert!(DomainEventRepo::complete_claimed_event(
        &db,
        CompleteDomainEvent {
            consumer_name: "repair".to_owned(),
            lease_owner: "repair-worker".to_owned(),
            event_sequence: second.sequence,
            event_id: second.id.clone(),
            dedupe_key: second.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:02:02Z".to_owned(),
        },
    )
    .await
    .expect("repaired cursor completes next event"));

    let recovery_db = sqlite_db().await;
    let stale_event = DomainEventRepo::append_event(
        &recovery_db,
        CreateDomainEvent {
            id: "event-stale".to_owned(),
            event_type: "task.transitioned".to_owned(),
            entity_type: "task".to_owned(),
            entity_id: "task-3".to_owned(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: "task-3".to_owned(),
            correlation_id: "corr-3".to_owned(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some("task-transition:3".to_owned()),
            payload_json: "{}".to_owned(),
            created_at: "2026-08-12T20:03:00Z".to_owned(),
        },
    )
    .await
    .unwrap();
    let first_claim = DomainEventRepo::claim_event_batch(
        &recovery_db,
        ClaimDomainEvents {
            consumer_name: "recovery".to_owned(),
            lease_owner: "worker-a".to_owned(),
            now: "2026-08-12T20:03:01Z".to_owned(),
            leased_until: "2026-08-12T20:03:02Z".to_owned(),
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(first_claim[0].id, stale_event.id);
    let second_claim = DomainEventRepo::claim_event_batch(
        &recovery_db,
        ClaimDomainEvents {
            consumer_name: "recovery".to_owned(),
            lease_owner: "worker-b".to_owned(),
            now: "2026-08-12T20:03:03Z".to_owned(),
            leased_until: "2026-08-12T20:03:04Z".to_owned(),
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(second_claim[0].id, stale_event.id);
    let stale_completion = DomainEventRepo::complete_claimed_event(
        &recovery_db,
        CompleteDomainEvent {
            consumer_name: "recovery".to_owned(),
            lease_owner: "worker-a".to_owned(),
            event_sequence: stale_event.sequence,
            event_id: stale_event.id.clone(),
            dedupe_key: stale_event.dedupe_key.clone().unwrap(),
            completed_at: "2026-08-12T20:03:03Z".to_owned(),
        },
    )
    .await;
    assert!(matches!(stale_completion, Err(DbError::VersionConflict)));
    assert!(DomainEventRepo::complete_claimed_event(
        &recovery_db,
        CompleteDomainEvent {
            consumer_name: "recovery".to_owned(),
            lease_owner: "worker-b".to_owned(),
            event_sequence: stale_event.sequence,
            event_id: stale_event.id,
            dedupe_key: "task-transition:3".to_owned(),
            completed_at: "2026-08-12T20:03:04Z".to_owned(),
        },
    )
    .await
    .expect("replacement worker completes after lease expiry"));
}

#[tokio::test]
async fn domain_event_append_in_tx_rolls_back_with_the_mutation() {
    let db = sqlite_db().await;
    let event = CreateDomainEvent {
        id: "rolled-back-event".to_owned(),
        event_type: "task.transitioned".to_owned(),
        entity_type: "task".to_owned(),
        entity_id: "task-rollback".to_owned(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "task".to_owned(),
        scope_id: "task-rollback".to_owned(),
        correlation_id: "corr-rollback".to_owned(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some("rollback-event".to_owned()),
        payload_json: "{}".to_owned(),
        created_at: "2026-08-12T20:00:00Z".to_owned(),
    };
    let mut transaction = db.pool().begin().await.expect("transaction begins");
    DomainEventRepo::append_event_in_tx(&db, &mut transaction, &event)
        .await
        .expect("event appends inside transaction");
    transaction
        .rollback()
        .await
        .expect("transaction rolls back");
    assert!(DomainEventRepo::get_event(&db, &event.id)
        .await
        .expect("event lookup succeeds")
        .is_none());
}

#[tokio::test]
async fn direct_task_status_updates_emit_a_ledger_event_atomically() {
    let db = sqlite_db().await;
    let (project_id, repo_id, _) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "ledger task",
    )
    .await;
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task lookup succeeds")
        .expect("task exists");
    let updated = TaskRepo::update_status(
        &db,
        UpdateTaskStatus {
            id: task_id.clone(),
            expected_version: task.version,
            status: "in_progress".to_owned(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: "2026-08-12T20:00:00Z".to_owned(),
        },
    )
    .await
    .expect("task status updates");
    assert_eq!(updated.status, "in_progress");
    let events = DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("task event lists");
    let event = events
        .iter()
        .find(|event| event.event_type == "task.status_changed" && event.entity_id == task_id)
        .expect("task status event exists");
    let payload: serde_json::Value = serde_json::from_str(&event.payload_json).unwrap();
    assert_eq!(payload["from_status"], "todo");
    assert_eq!(payload["to_status"], "in_progress");
}

#[tokio::test]
async fn stale_execution_baseline_cannot_mint_a_running_execution_after_read_gate() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        &repo_id,
        None,
        "todo".to_owned(),
        "baseline race task",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = now_rfc3339();
    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
    let baseline_id = new_uuid_v4();
    let baseline_revision_id = new_uuid_v4();
    let approval_id = new_uuid_v4();
    let user_id = new_uuid_v4();

    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', 'Baseline Race User', ?, ?)",
    )
    .bind(&user_id)
    .bind(format!("{user_id}@example.test"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user creates");
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&user_id)
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("Project owner updates");
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES (?, ?, ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Charter creates");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, base_revision, lifecycle, schema_version,
          render_version, content_json, rendered_view, author_type, author_id,
          content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, 'approved', 'test-schema', 'test-render', '{}',
                 'approved Charter', 'user', ?, 'charter-content-digest',
                 'charter-render-digest', ?)",
    )
    .bind(&charter_revision_id)
    .bind(&charter_id)
    .bind(&user_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Charter revision creates");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?, version = 2, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&charter_id)
    .execute(db.pool())
    .await
    .expect("Charter approval pointer updates");
    sqlx::query(
        "UPDATE project
         SET charter_status = 'charter_backed', charter_setup_required = 0,
             current_charter_id = ?, current_charter_revision_id = ?,
             current_charter_version = 2, updated_at = ?
         WHERE id = ?",
    )
    .bind(&charter_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&project_id)
    .execute(db.pool())
    .await
    .expect("Project becomes Charter-backed");
    sqlx::query(
        "INSERT INTO project_execution_baseline
         (id, project_id, current_revision_id, lifecycle, version, created_at, updated_at)
         VALUES (?, ?, ?, 'active', 1, ?, ?)",
    )
    .bind(&baseline_id)
    .bind(&project_id)
    .bind(&baseline_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("active baseline creates");
    sqlx::query(
        "INSERT INTO project_execution_baseline_revision
         (id, baseline_id, revision, base_revision, lifecycle, charter_revision_id,
          release_policy_revision, release_policy_digest, schema_version, render_version,
          rendered_view, content_digest, rendered_digest, created_at)
         VALUES (?, ?, 1, 0, 'approved', ?, 'policy-1', 'policy-digest',
                 'schema-1', 'render-1', 'approved baseline', 'content-digest',
                 'render-digest', ?)",
    )
    .bind(&baseline_revision_id)
    .bind(&baseline_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("approved baseline revision creates");
    sqlx::query(
        "INSERT INTO project_execution_baseline_approval
         (id, baseline_id, revision_id, expected_project_version, principal_type,
          principal_id, authorization_basis, authorization_action,
          authorization_occurred_at, explicit_event, content_digest,
          rendered_digest, lifecycle, idempotency_key, created_at, updated_at)
         VALUES (?, ?, ?, 1, 'user', ?, 'test',
                 'project.execution_baseline.approve', '2026-08-13T00:00:00Z',
                 'approve exact baseline',
                 'content-digest', 'render-digest', 'consumed', ?, ?, ?)",
    )
    .bind(&approval_id)
    .bind(&baseline_id)
    .bind(&baseline_revision_id)
    .bind(&user_id)
    .bind(format!("approval-{approval_id}"))
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("baseline approval creates");
    sqlx::query(
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id, baseline_id, baseline_revision_id,
          runnable, provenance_json, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, 1, '{}', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&charter_revision_id)
    .bind(&baseline_id)
    .bind(&baseline_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("runnable governance creates");

    // This represents the race after the service's read-only admission gate:
    // the exact baseline is superseded before the authoritative execution
    // INSERT transaction starts.
    sqlx::query(
        "UPDATE project_execution_baseline
         SET lifecycle = 'superseded', version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(&now)
    .bind(&baseline_id)
    .execute(db.pool())
    .await
    .expect("baseline supersedes");
    sqlx::query(
        "UPDATE project_task_governance
         SET runnable = 0, version = version + 1, updated_at = ?
         WHERE task_id = ?",
    )
    .bind(&now)
    .bind(&task_id)
    .execute(db.pool())
    .await
    .expect("governance is revoked");

    let task_before = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reads")
        .expect("task exists");
    let mut claim_transaction = db.pool().begin().await.expect("claim transaction begins");
    let claim = TaskRepo::claim(
        &db,
        &mut claim_transaction,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: task_before.version,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            capacity_statuses: vec!["in_progress".to_owned()],
            execution: CreateExecution {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                agent_id: Some(agent_id.clone()),
                role: "executor".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: Some(workspace_id.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            max_concurrent_tasks: 1,
            claimed_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(claim, Err(DbError::InvalidTransition)));
    claim_transaction
        .rollback()
        .await
        .expect("claim transaction rolls back");
    let task_after = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task rereads")
        .expect("task remains");
    assert_eq!(task_after.version, task_before.version);
    assert_eq!(task_after.status, "todo");
    assert!(task_after.assignee_id.is_none());

    let execution = ExecutionRepo::create(
        &db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "executor".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(execution, Err(DbError::InvalidTransition)));
    let execution_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("execution count reads");
    assert_eq!(execution_count, 0, "stale baseline must not mint execution");
    let workspace_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace WHERE task_id = ?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .expect("workspace count reads");
    assert_eq!(
        workspace_count, 1,
        "race guard does not duplicate a workspace"
    );
}

#[tokio::test]
async fn operating_skills_point_at_their_latest_seeded_revisions() {
    let db = sqlite_db().await;
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT id, current_revision_id FROM operating_skill ORDER BY id")
            .fetch_all(db.pool())
            .await
            .expect("operating skills read");
    assert_eq!(
        rows,
        vec![
            (
                "forge.main.project-discovery/v2".to_owned(),
                "forge.main.project-discovery/v2@2".to_owned(),
            ),
            (
                "forge.project.orchestration/v1".to_owned(),
                "forge.project.orchestration/v1@1".to_owned(),
            ),
        ],
        "a seeded operating-skill revision must be repointed in the same release (V081 regression)"
    );
}
