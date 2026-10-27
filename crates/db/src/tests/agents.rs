//! Agents repository tests.
use super::*;

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
            workspace_path: None,
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
async fn task_context_scopes_are_distinct_per_role_for_the_same_identity() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, identity_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        Some(&identity_id),
        "review".to_owned(),
        "same identity, different Task roles",
    )
    .await;
    let now = now_rfc3339();

    let worker = AgentContextScopeRepo::create_context_scope(
        &db,
        CreateAgentContextScope {
            id: new_uuid_v4(),
            identity_id: identity_id.clone(),
            scope_type: "task".to_owned(),
            scope_id: task_id.clone(),
            project_id: Some(project_id.clone()),
            task_id: Some(task_id.clone()),
            task_role: Some("worker".to_owned()),
            workspace_access: "task_write".to_owned(),
            workspace_path: None,
            authority_json: r#"{"scope":{"kind":"task","role":"worker"}}"#.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("worker scope creates");
    let reviewer_input = CreateAgentContextScope {
        id: new_uuid_v4(),
        identity_id: identity_id.clone(),
        scope_type: "task".to_owned(),
        scope_id: task_id.clone(),
        project_id: Some(project_id),
        task_id: Some(task_id.clone()),
        task_role: Some("reviewer".to_owned()),
        workspace_access: "task_read".to_owned(),
        workspace_path: None,
        authority_json: r#"{"scope":{"kind":"task","role":"reviewer"}}"#.to_owned(),
        created_at: now.clone(),
        updated_at: now,
    };
    let reviewer = AgentContextScopeRepo::create_context_scope(&db, reviewer_input.clone())
        .await
        .expect("reviewer scope creates beside worker scope");
    let replayed_reviewer = AgentContextScopeRepo::create_context_scope(&db, reviewer_input)
        .await
        .expect("reviewer scope creation replays");

    assert_ne!(worker.id, reviewer.id);
    assert_eq!(worker.scope_id, task_id);
    assert_eq!(reviewer.scope_id, task_id);
    assert_eq!(worker.task_role.as_deref(), Some("worker"));
    assert_eq!(reviewer.task_role.as_deref(), Some("reviewer"));
    assert_eq!(replayed_reviewer.id, reviewer.id);
    assert_eq!(
        AgentContextScopeRepo::list_context_scopes(&db, &identity_id)
            .await
            .expect("context scopes list")
            .into_iter()
            .filter(|scope| scope.scope_type == "task" && scope.scope_id == task_id)
            .count(),
        2
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

    let owner_member = ProjectMemberRepo::get_member(&db, &project_id, &u1_id)
        .await
        .expect("u1 owner membership loads")
        .expect("project creation materializes the owner membership");
    assert_eq!(owner_member.role, "owner");
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

#[tokio::test]
async fn operating_skills_point_at_their_latest_seeded_revisions() {
    use sha2::{Digest, Sha256};

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
                "forge.main.project-discovery/v2@8".to_owned(),
            ),
            (
                "forge.project.orchestration/v1".to_owned(),
                "forge.project.orchestration/v1@22".to_owned(),
            ),
        ],
        "a seeded operating-skill revision must be repointed in the same release (V081 regression)"
    );
    let (body, digest): (String, String) = sqlx::query_as(
        "SELECT canonical_body, content_digest
         FROM operating_skill_revision
         WHERE id = 'forge.project.orchestration/v1@22'",
    )
    .fetch_one(db.pool())
    .await
    .expect("latest Project operating skill reads");
    // @11 keeps only the authority core and standing invariants resident; the
    // detailed doctrine (documents, milestones, evidence, release) moved
    // behind the `skill.section` read and is pinned by services-side tests.
    assert!(body.contains("Never invent aliases such as `ac-1`"));
    assert!(body.contains("The approved Charter is the only implementation gate"));
    assert!(body.contains("On a delivery follow-up wake"));
    assert!(body.contains("OPERATING DOCTRINE (on-demand skill sections)"));
    assert!(body.contains("skill.section"));
    assert!(body.contains("read `project.charter` and `project.current_state`"));
    // @22: the per-verb cancel sentence gave way to the generated contract;
    // the doctrine keeps the one rule that every Task decision is an offer.
    assert!(body.contains("Task workflow decisions use the current `task.action` offers"));
    assert!(body.contains("Project Agents copy the action call from `action_schema`"));
    assert!(body.contains("`parent_task_id` establishes one-level coordination"));
    assert!(
        body.contains("Dependency edges only gate execution, never hierarchy or Workspace sharing")
    );
    assert!(body.contains("name owned modules/files in each Task"));
    assert!(body.contains("give one Task ownership of shared edits and make others depend on it"));
    assert!(!body.contains("Evidence is mandatory proof, not optional decoration"));
    assert!(!body.contains("MILESTONES AND EVIDENCE"));
    assert_eq!(hex::encode(Sha256::digest(body.as_bytes())), digest);
}

#[tokio::test]
async fn agent_list_active_assignments_match_scalar_for_workflow_shapes() {
    for (label, definition) in [
        (
            "custom",
            serde_json::json!({"states": [
                {"name": "todo", "kind": "initial"},
                {"name": "running", "kind": "active"},
                {"name": "waiting_review", "kind": "gate"},
                {"name": "in_progress", "kind": "terminal"},
                {"name": "dup", "kind": "terminal"},
                {"name": "dup", "kind": "gate"},
                {"kind": "active"},
                {"name": "done", "kind": "terminal"}
            ]})
            .to_string(),
        ),
        ("empty-object", "{}".to_owned()),
        ("not-json", "not json".to_owned()),
        ("states-string", "{\"states\": \"x\"}".to_owned()),
        (
            "states-array-of-strings",
            "{\"states\": [\"todo\", \"running\"]}".to_owned(),
        ),
        (
            "states-object",
            "{\"states\": {\"a\": {\"name\": \"running\", \"kind\": \"active\"}}}".to_owned(),
        ),
        ("array-root", "[1,2]".to_owned()),
        (
            "numeric-name",
            "{\"states\": [{\"name\": 5, \"kind\": \"active\"}]}".to_owned(),
        ),
    ] {
        let db = sqlite_db().await;
        let (p1, _r1, a) = seed_project_repo_agent(&db).await;
        let (_p2, _r2, b) = seed_project_repo_agent(&db).await;
        let set = sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(&definition)
            .bind(&p1)
            .execute(db.pool())
            .await;
        if let Err(error) = &set {
            println!("AUDIT assignments {label}: definition rejected by schema: {error}");
            continue;
        }
        let mut n = 0;
        for status in [
            "todo",
            "in_progress",
            "review",
            "merging",
            "running",
            "waiting_review",
            "done",
            "dup",
            "5",
        ] {
            for owner in [&a, &b] {
                n += 1;
                let task = seed_task(&db, &p1, Some(owner), status.to_owned(), "t").await;
                if n % 3 == 0 {
                    sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, 'reviewer', 'agent', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')")
                        .bind(new_uuid_v4()).bind(&task).bind(if n % 2 == 0 { &a } else { owner }).execute(db.pool()).await.unwrap();
                }
                if n % 5 == 0 {
                    sqlx::query("UPDATE task SET deleted_at = '2026-01-01T00:00:00Z' WHERE id = ?")
                        .bind(&task)
                        .execute(db.pool())
                        .await
                        .unwrap();
                }
                if n % 4 == 0 {
                    let parent = seed_task(&db, &p1, None, "todo".to_owned(), "parent").await;
                    sqlx::query("UPDATE task SET parent_task_id = ? WHERE id = ?")
                        .bind(&parent)
                        .bind(&task)
                        .execute(db.pool())
                        .await
                        .unwrap();
                }
            }
        }
        let ids = vec![a.clone(), b.clone(), "missing".to_owned()];
        let grouped = db.agent_list_active_assignments(&ids).await;
        for id in &ids {
            let single = AgentRepo::count_active_assigned_tasks(&db, id).await;
            let grouped = grouped.as_ref().map(|g| g[id]).map_err(|e| e.to_string());
            let single = single.map_err(|e| e.to_string());
            if ["states-string", "states-array-of-strings"].contains(&label) {
                // Both scalar and grouped contracts reject malformed state
                // elements when any assigned Agent visits this Project.
                assert!(grouped.is_err());
                if id != "missing" {
                    assert!(single.is_err());
                }
            } else {
                assert_eq!(single, grouped, "{label} {id}");
            }
        }
    }
}
