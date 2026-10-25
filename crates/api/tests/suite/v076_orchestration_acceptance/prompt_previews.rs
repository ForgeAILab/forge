//! Main and Project prompt previews and frozen manifest inputs.

use super::*;

#[tokio::test]
async fn split_prompt_preserves_turn_manifest_sources_revisions_and_digests() {
    use db::AgentChatTurnJobRepo;
    let workspace = common::TestDir::new("split-prompt-manifest");
    let harness = common::test_app(workspace.path(), "split-prompt-manifest").await;
    let project_id = "11111111-1111-4111-8111-111111111111";
    let time = "2026-10-01T00:00:00Z";
    db::ProjectRepo::create(
        &*harness.state.db,
        db::CreateProject {
            id: project_id.to_owned(),
            name: "Manifest Project".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: Some("test-user-id".to_owned()),
            created_at: time.to_owned(),
            updated_at: time.to_owned(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE project SET version = 4 WHERE id = ?")
        .bind(project_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let agent = connect_agent(
        &harness.app,
        &common::test_jwt(),
        "manifest-project",
        &["read_project", "propose_project"],
    )
    .await;
    let binding_version: i64 = sqlx::query_scalar("SELECT version FROM project_agent_binding WHERE project_id = ? AND state IN ('active', 'agent_setup_required')")
        .bind(project_id).fetch_one(harness.state.db.pool()).await.unwrap();
    let binding = request_json(&harness.app, Method::PUT, &format!("/api/v1/projects/{project_id}/project-agent"), &common::test_jwt(),
        json!({"identity_id": agent["agent"]["id"], "expected_version": binding_version, "autonomy_policy": {}, "permission_ceiling": {"permissions": ["read_project", "propose_project"]}}), &[StatusCode::OK]).await;
    let chat_id = required_string(&binding, &["chat_id"]);
    let turn_id = admit_preview_turn(&harness, &chat_id, "Explain adoption").await;
    let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &turn_id)
        .await
        .unwrap()
        .unwrap();
    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        std::sync::Arc::clone(&harness.state.db),
        std::sync::Arc::clone(&harness.state.embedded_agent_service),
        std::sync::Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    let preview = runner.preview_prompt(&job).await.unwrap();
    assert_preview_state_is_data(&preview);
    let manifest_id = runner.record_prompt_manifest_for_test(&job).await.unwrap();
    let manifest =
        db::ScopedMemoryRepository::get_context_manifest(&*harness.state.db, &manifest_id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(manifest.scope_id, chat_id);
    assert_eq!(
        Some(manifest.identity_id.as_str()),
        job.responder_identity_id.as_deref()
    );
    let rows =
        db::ScopedMemoryRepository::list_context_manifest_sources(&*harness.state.db, &manifest_id)
            .await
            .unwrap();
    let actual = json!({"manifest": {
        "scope_type": manifest.scope_type, "policy_revision": manifest.policy_revision, "domain_revision": manifest.domain_revision,
        "runtime_manifest_id": manifest.runtime_manifest_id, "runtime_manifest_fingerprint": manifest.runtime_manifest_fingerprint, "lcm_binding_revision": manifest.lcm_binding_revision
    }, "sources": rows.iter().map(|row| json!({
        "ordinal": row.ordinal, "source_type": row.source_type, "source_id": row.source_id,
        "source_revision": row.source_revision, "fragment_fingerprint": row.fragment_fingerprint,
        "selection_reason": row.selection_reason, "disposition": row.disposition,
        "retention_priority": row.retention_priority
    })).collect::<Vec<_>>()});
    let expected: Value = serde_json::from_str(include_str!(
        "../../../../services/tests/fixtures/project_chat_manifest.json"
    ))
    .unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn stable_prefix_project_chat_preview_tracks_state_without_creating_a_session() {
    let workspace = common::TestDir::new("stable-prefix-project-preview");
    let harness = common::test_app(workspace.path(), "stable-prefix-project").await;
    let fixture =
        create_genesis_project(&harness.app, &common::test_jwt(), "stable-prefix-project").await;
    let chat = &fixture.project_chat_id;
    let first_turn = admit_preview_turn(&harness, chat, "Review the delivery state").await;
    let before_sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session")
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap();
    let first = chat_preview(&harness, chat, &first_turn).await;
    let state_before = services::project_runtime::load_effective_project_state(
        &harness.state.db,
        &fixture.project_id,
        None,
    )
    .await
    .unwrap();
    request_json(&harness.app, Method::POST, &format!("/api/v1/projects/{}/tasks", fixture.project_id),
        &common::test_jwt(), json!({"title": "Deliver the initial experience", "description": "Implement the chartered experience in src/experience.rs"}), &[StatusCode::OK]).await;
    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&fixture.project_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    db::DomainEventRepo::append_event(
        &*harness.state.db,
        db::CreateDomainEvent {
            id: db::new_uuid_v4(),
            event_type: "project.metadata.updated".into(),
            entity_type: "project".into(),
            entity_id: fixture.project_id.clone(),
            actor_type: "user".into(),
            actor_id: Some("test-user-id".into()),
            scope_type: "project".into(),
            scope_id: fixture.project_id.clone(),
            correlation_id: db::new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: "{}".into(),
            created_at: db::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let second_turn = admit_preview_turn(&harness, chat, "Review the delivery state again").await;
    let second = chat_preview(&harness, chat, &second_turn).await;
    let state_after = services::project_runtime::load_effective_project_state(
        &harness.state.db,
        &fixture.project_id,
        None,
    )
    .await
    .unwrap();
    assert_ne!(
        state_before.source_event_watermark,
        state_after.source_event_watermark
    );
    assert_eq!(
        state_after.project.version,
        state_before.project.version + 1
    );
    assert_eq!(
        state_after.task_summary.total,
        state_before.task_summary.total + 1
    );
    assert_eq!(
        first.system_prompt.as_ref().unwrap().as_bytes(),
        second.system_prompt.as_ref().unwrap().as_bytes()
    );
    assert_ne!(first.input_parts[1], second.input_parts[1]);
    assert!(second.input_parts[1]
        .contains(&format!("Project version: {}", state_after.project.version)));
    assert_preview_state_is_data(&first);
    assert_preview_state_is_data(&second);
    let after_sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session")
        .fetch_one(harness.state.db.pool())
        .await
        .unwrap();
    assert_eq!(before_sessions, after_sessions);
}

#[tokio::test]
async fn stable_prefix_main_baseline_preview_tracks_portfolio_versions() {
    let workspace = common::TestDir::new("stable-prefix-main-preview");
    let harness = common::test_app(workspace.path(), "stable-prefix-main").await;
    let fixture =
        create_genesis_project(&harness.app, &common::test_jwt(), "stable-prefix-main").await;
    // Recency determines both bounded card selection and manifest sources.
    // Reverse lexical ordering would choose a different bounded portfolio.
    for index in 0..21 {
        sqlx::query("INSERT INTO project (id, name, owner_id, created_at, updated_at) VALUES (?, 'Portfolio entry', 'test-user-id', ?, ?)")
            .bind(format!("portfolio-{index:02}"))
            .bind(format!("2026-09-01T00:00:{index:02}Z"))
            .bind(format!("2026-09-01T00:00:{index:02}Z"))
            .execute(harness.state.db.pool()).await.unwrap();
    }
    let chat = &fixture.main_chat_id;
    let first_turn = admit_preview_turn(&harness, chat, "Which Projects exist?").await;
    let first = chat_preview(&harness, chat, &first_turn).await;
    sqlx::query("UPDATE project SET version = version + 1, updated_at = ? WHERE id = ?")
        .bind(db::now_rfc3339())
        .bind(&fixture.project_id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let second_turn = admit_preview_turn(&harness, chat, "Which Projects exist now?").await;
    let second = chat_preview(&harness, chat, &second_turn).await;
    assert_eq!(first.system_prompt, second.system_prompt);
    assert_ne!(first.input_parts[1], second.input_parts[1]);
    assert!(second.input_parts[1].contains(&fixture.project_id));
    assert!(second.input_parts[1].contains(&format!("version=v{}", fixture.project_version + 1)));
    assert_preview_state_is_data(&second);
    assert!(second.input_parts[1].contains("portfolio-20"));
    assert!(!second.input_parts[1].contains("portfolio-00"));
    assert!(
        second.input_parts[1].find("portfolio-20").unwrap()
            < second.input_parts[1].find("portfolio-19").unwrap()
    );
    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        std::sync::Arc::clone(&harness.state.db),
        std::sync::Arc::clone(&harness.state.embedded_agent_service),
        std::sync::Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    let job = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &second_turn)
        .await
        .unwrap()
        .unwrap();
    let manifest_id = runner.record_prompt_manifest_for_test(&job).await.unwrap();
    let sources: Vec<String> = sqlx::query_scalar("SELECT source_id FROM context_manifest_source WHERE manifest_id = ? AND source_type = 'main_portfolio_projection' ORDER BY ordinal")
        .bind(&manifest_id).fetch_all(harness.state.db.pool()).await.unwrap();
    assert_eq!(sources.len(), 20);
    assert_eq!(
        sources[0],
        format!(
            "main_baseline_context:main_portfolio_projection:{}",
            fixture.project_id
        )
    );
    assert_eq!(
        sources[1],
        "main_baseline_context:main_portfolio_projection:portfolio-20"
    );
    assert_eq!(
        sources[19],
        "main_baseline_context:main_portfolio_projection:portfolio-02"
    );
}

#[tokio::test]
async fn stable_prefix_main_genesis_preview_moves_evolving_instruction_state_to_input() {
    let workspace = common::TestDir::new("stable-prefix-genesis-preview");
    let harness = common::test_app(workspace.path(), "stable-prefix-genesis").await;
    let token = common::test_jwt();
    let agent = connect_agent(
        &harness.app,
        &token,
        "stable-prefix-genesis",
        &["read_account", "read_project", "handoff"],
    )
    .await;
    let binding = request_json(
        &harness.app,
        Method::PUT,
        "/api/v1/account/main-agent",
        &token,
        json!({"identity_id": agent["agent"]["id"], "expected_version": 0, "autonomy_policy": {}}),
        &[StatusCode::OK],
    )
    .await;
    let chat = required_string(&binding, &["chat_id"]);
    let started = request_json(&harness.app, Method::POST, "/api/v1/account/main-agent/product-genesis", &token,
        json!({"idempotency_key": "stable-prefix-genesis", "maturity": "mvp", "initial_idea": "A bounded useful tool"}), &[StatusCode::CREATED]).await;
    let genesis = required_string(&started, &["session", "id"]);
    let first_turn = admit_preview_turn(&harness, &chat, "The first audience is small teams").await;
    let first = chat_preview(&harness, &chat, &first_turn).await;
    let original: String = sqlx::query_scalar("SELECT body FROM agent_chat_instruction_revision WHERE chat_id = ? ORDER BY revision DESC LIMIT 1")
        .bind(&chat).fetch_one(harness.state.db.pool()).await.unwrap();
    let portfolio: api_types::ProjectResponse = common::json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({"name": "Genesis portfolio state"}),
        StatusCode::OK,
    )
    .await;
    sqlx::query("UPDATE project SET version = version + 1 WHERE id = ?")
        .bind(&portfolio.id)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let updated = services::render_product_genesis_prompt(
        api_types::ProductMaturity::Mvp,
        &services::GenesisPromptContext {
            genesis_session_id: Some(genesis.clone()),
            current_understanding: "An agreed team workflow".into(),
            observed_facts: vec!["The first audience is small teams".into()],
            assumptions: vec!["A narrow rollout is enough".into()],
            ..Default::default()
        },
    );
    // Simulate the next immutable discovery snapshot without touching an old
    // instruction or skill revision. The turn loader must separate both.
    sqlx::query("INSERT INTO agent_chat_instruction_revision (id, chat_id, revision, body, source_type, source_id, created_by_type, created_at)
        SELECT ?, ?, MAX(revision) + 1, ?, 'native', ?, 'agent', ? FROM agent_chat_instruction_revision WHERE chat_id = ?")
        .bind(db::new_uuid_v4()).bind(&chat).bind(&updated).bind(&genesis).bind(db::now_rfc3339()).bind(&chat)
        .execute(harness.state.db.pool()).await.unwrap();
    sqlx::query("UPDATE product_genesis_session SET version = version + 1 WHERE id = ?")
        .bind(&genesis)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let second_turn = admit_preview_turn(&harness, &chat, "Continue with that workflow").await;
    let second = chat_preview(&harness, &chat, &second_turn).await;
    assert_eq!(
        first.system_prompt.as_ref().unwrap().as_bytes(),
        second.system_prompt.as_ref().unwrap().as_bytes()
    );
    assert_ne!(first.input_parts[1], second.input_parts[1]);
    assert!(second.input_parts[1].contains("An agreed team workflow"));
    assert!(second.input_parts[1].contains("The first audience is small teams"));
    assert!(!second
        .system_prompt
        .as_deref()
        .unwrap()
        .contains("An agreed team workflow"));
    assert!(second.input_parts[1].contains(&portfolio.id));
    assert!(second.input_parts[1].contains(&format!("version=v{}", portfolio.version + 1)));
    assert_preview_state_is_data(&second);
    let retained: String = sqlx::query_scalar("SELECT body FROM agent_chat_instruction_revision WHERE chat_id = ? ORDER BY revision ASC LIMIT 1")
        .bind(&chat).fetch_one(harness.state.db.pool()).await.unwrap();
    assert_eq!(retained, original);
}
