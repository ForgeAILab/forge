use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AccountMainAgentBindingRepo,
    AgentRepo, AgentStatus, CreateAccountMainAgentBinding, CreateAgentIdentity, CreateAgentProfile,
    CreateProject, CreateProjectAgentBinding, ProjectAgentBindingRepo, ProjectRepo,
    ReplaceAccountMainAgentBinding, ReplaceProjectAgentBinding, SqliteDb,
};
use sqlx::Row;

async fn database() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    SqliteDb::new(pool)
}

async fn seed_binding_fixture(db: &SqliteDb) -> (String, String, String, String, String) {
    let now = now_rfc3339();
    let account_id = "binding-account".to_owned();
    let project_id = "binding-project".to_owned();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES (?, ?, 'test', NULL, ?, ?)",
    )
    .bind(&account_id)
    .bind("binding-account@example.test")
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("binding account creates");
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "binding project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some(account_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("binding project creates");

    let first_identity = "binding-identity-a".to_owned();
    let second_identity = "binding-identity-b".to_owned();
    let first_profile = new_uuid_v4();
    let second_profile = new_uuid_v4();
    for (identity_id, profile_id) in [
        (first_identity.clone(), first_profile.clone()),
        (second_identity, second_profile.clone()),
    ] {
        AgentRepo::create_identity_with_profile(
            db,
            CreateAgentIdentity {
                id: identity_id.clone(),
                name: identity_id.clone(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some(account_id.clone()),
                visibility: "account".to_owned(),
                account_permission_ceiling: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            CreateAgentProfile {
                id: profile_id,
                identity_id,
                backend_kind: "native".to_owned(),
                executor_type: "embedded".to_owned(),
                provider: Some("test".to_owned()),
                model: Some("test".to_owned()),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("binding identity creates");
    }
    (
        account_id,
        project_id,
        first_identity,
        first_profile,
        second_profile,
    )
}

#[tokio::test]
async fn singular_bindings_are_versioned_and_have_no_legacy_membership_roles() {
    let db = database().await;
    let (account_id, project_id, first_identity, first_profile, second_profile) =
        seed_binding_fixture(&db).await;
    let now = now_rfc3339();

    sqlx::query(
        "INSERT INTO account_main_agent_binding (
            id, account_id, identity_id, profile_id, state, version, created_at, updated_at
         ) VALUES ('main-binding-1', ?, ?, ?, 'active', 1, ?, ?)",
    )
    .bind(&account_id)
    .bind(&first_identity)
    .bind(&first_profile)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("Main binding creates");

    let left_db = db.clone();
    let right_db = db.clone();
    let (left, right) = tokio::join!(
        AccountMainAgentBindingRepo::replace_main_binding(
            &left_db,
            ReplaceAccountMainAgentBinding {
                account_id: account_id.clone(),
                expected_version: 1,
                replacement: CreateAccountMainAgentBinding {
                    id: "main-binding-left".to_owned(),
                    account_id: account_id.clone(),
                    identity_id: "binding-identity-b".to_owned(),
                    profile_id: second_profile.clone(),
                    autonomy_policy_json: "{}".to_owned(),
                    tool_policy_revision: "test".to_owned(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                replacement_reason: Some("concurrent replacement left".to_owned()),
            },
        ),
        AccountMainAgentBindingRepo::replace_main_binding(
            &right_db,
            ReplaceAccountMainAgentBinding {
                account_id: account_id.clone(),
                expected_version: 1,
                replacement: CreateAccountMainAgentBinding {
                    id: "main-binding-right".to_owned(),
                    account_id: account_id.clone(),
                    identity_id: first_identity.clone(),
                    profile_id: first_profile.clone(),
                    autonomy_policy_json: "{}".to_owned(),
                    tool_policy_revision: "test".to_owned(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                replacement_reason: Some("concurrent replacement right".to_owned()),
            },
        ),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);

    let setup_binding = ProjectAgentBindingRepo::get_active_project_binding(&db, &project_id)
        .await
        .expect("setup binding loads")
        .expect("Project setup binding exists");
    let project_binding = ProjectAgentBindingRepo::replace_project_binding(
        &db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: setup_binding.version,
            replacement: CreateProjectAgentBinding {
                id: "project-binding-1".to_owned(),
                project_id: project_id.clone(),
                identity_id: Some(first_identity.clone()),
                profile_id: Some(first_profile.clone()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: "{}".to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            replacement_reason: Some("initial Project Agent selection".to_owned()),
        },
    )
    .await
    .expect("Project binding creates");

    let left_db = db.clone();
    let right_db = db.clone();
    let (left, right) = tokio::join!(
        ProjectAgentBindingRepo::replace_project_binding(
            &left_db,
            ReplaceProjectAgentBinding {
                project_id: project_id.clone(),
                expected_version: project_binding.version,
                replacement: CreateProjectAgentBinding {
                    id: "project-binding-left".to_owned(),
                    project_id: project_id.clone(),
                    identity_id: Some("binding-identity-b".to_owned()),
                    profile_id: Some(second_profile.clone()),
                    state: "active".to_owned(),
                    autonomy_policy_json: "{}".to_owned(),
                    permission_ceiling_json: "{}".to_owned(),
                    subscriptions_json: "[]".to_owned(),
                    wake_budget: 1,
                    operating_skill_revision_id: None,
                    policy_revision: "default".to_owned(),
                    policy_digest: String::new(),
                    charter_id: None,
                    charter_revision_id: None,
                    charter_setup_required: true,
                    admission_receipt_id: None,
                    charter_approval_id: None,
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
                replacement_reason: Some("concurrent replacement left".to_owned()),
            },
        ),
        ProjectAgentBindingRepo::replace_project_binding(
            &right_db,
            ReplaceProjectAgentBinding {
                project_id: project_id.clone(),
                expected_version: project_binding.version,
                replacement: CreateProjectAgentBinding {
                    id: "project-binding-right".to_owned(),
                    project_id: project_id.clone(),
                    identity_id: Some(first_identity),
                    profile_id: Some(first_profile),
                    state: "active".to_owned(),
                    autonomy_policy_json: "{}".to_owned(),
                    permission_ceiling_json: "{}".to_owned(),
                    subscriptions_json: "[]".to_owned(),
                    wake_budget: 1,
                    operating_skill_revision_id: None,
                    policy_revision: "default".to_owned(),
                    policy_digest: String::new(),
                    charter_id: None,
                    charter_revision_id: None,
                    charter_setup_required: true,
                    admission_receipt_id: None,
                    charter_approval_id: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
                replacement_reason: Some("concurrent replacement right".to_owned()),
            },
        ),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);

    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_agent_binding
         WHERE project_id = ? AND state = 'active'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("active binding count");
    assert_eq!(active, 1);

    let columns = sqlx::query("PRAGMA table_info(project_agent_binding)")
        .fetch_all(db.pool())
        .await
        .expect("binding schema");
    let names = columns
        .iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect::<Vec<_>>();
    assert!(!names.iter().any(|name| name == "role"));
    assert!(!names.iter().any(|name| name == "is_primary"));

    let membership_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = 'legacy_project_agent_membership'",
    )
    .fetch_one(db.pool())
    .await
    .expect("legacy membership table inspection");
    assert_eq!(
        membership_table_count, 1,
        "legacy membership table remains quarantined migration data"
    );
}
