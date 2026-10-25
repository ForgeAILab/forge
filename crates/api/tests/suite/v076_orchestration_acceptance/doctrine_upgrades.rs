//! Doctrine upgrades with preserved admissions and sessions.

use super::*;

#[tokio::test]
async fn layout_guidance_upgrade_preserves_frozen_turns_sessions_and_binding() {
    use db::AgentChatTurnJobRepo;
    use services_agents::AgentChatTurnRunner;
    use std::sync::Arc;

    let workspace = common::TestDir::new("layout-guidance-upgrade");
    let harness = common::test_app(workspace.path(), "layout-guidance-upgrade").await;
    for (key, revision) in [
        (
            "forge.main.project-discovery/v2",
            "forge.main.project-discovery/v2@6",
        ),
        (
            "forge.project.orchestration/v1",
            "forge.project.orchestration/v1@19",
        ),
    ] {
        sqlx::query("UPDATE operating_skill SET current_revision_id = ? WHERE skill_key = ?")
            .bind(revision)
            .bind(key)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
    }
    let genesis = create_genesis_project(&harness.app, &common::test_jwt(), "layout-upgrade").await;
    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.state.embedded_agent_service),
        Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    let mut frozen = Vec::new();
    for chat in [&genesis.main_chat_id, &genesis.project_chat_id] {
        let jobs = AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*harness.state.db, chat)
            .await
            .unwrap();
        for job in jobs {
            let preview = runner.preview_prompt(&job).await.unwrap();
            frozen.push((job, preview.system_prompt));
        }
    }
    let session_before: (String, String) = sqlx::query_as(
        "SELECT prompt_revision, prompt_body FROM product_genesis_session WHERE id = ?",
    )
    .bind(&genesis.genesis_session_id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    let migration =
        include_str!("../../../../db/migrations/V202610031431__merge_friendly_layout_guidance.sql")
            .replace(
                "INSERT INTO operating_skill_revision",
                "INSERT OR IGNORE INTO operating_skill_revision",
            );
    sqlx::raw_sql(&migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let binding_revision: String = sqlx::query_scalar(
        "SELECT operating_skill_revision_id FROM project_agent_binding WHERE id = ?",
    )
    .bind(required_string(
        &genesis.create_response,
        &["project_agent_binding_id"],
    ))
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(binding_revision, "forge.project.orchestration/v1@20");
    let session_after: (String, String) = sqlx::query_as(
        "SELECT prompt_revision, prompt_body FROM product_genesis_session WHERE id = ?",
    )
    .bind(&genesis.genesis_session_id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(session_before, session_after);
    assert!(!frozen.is_empty());
    for (job, prompt) in frozen {
        let retained = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained, job);
        assert_eq!(
            runner
                .preview_prompt(&retained)
                .await
                .unwrap()
                .system_prompt,
            prompt
        );
        runner
            .validate_admission_authority(&retained)
            .await
            .unwrap();
    }
    let turn = admit_preview_turn(&harness, &genesis.project_chat_id, "Plan the next module").await;
    let current = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &turn)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.operating_skill_revision_id.as_deref(),
        Some("forge.project.orchestration/v1@20")
    );
    assert!(runner
        .preview_prompt(&current)
        .await
        .unwrap()
        .system_prompt
        .unwrap()
        .contains(forge_agent_host::MERGE_FRIENDLY_LAYOUT_GUIDANCE));
    runner.validate_admission_authority(&current).await.unwrap();
}

#[tokio::test]
async fn merge_friendly_doctrine_preserves_old_admissions_and_genesis_sessions() {
    use db::AgentChatTurnJobRepo;
    use services_agents::AgentChatTurnRunner;
    use std::sync::Arc;

    let workspace = common::TestDir::new("merge-friendly-doctrine-upgrade");
    let harness = common::test_app(workspace.path(), "merge-friendly-doctrine-upgrade").await;
    // Model the pre-upgrade current pointers. Historical bodies remain seeded
    // by the same immutable migrations as on an upgraded user database.
    for (key, revision) in [
        (
            "forge.main.project-discovery/v2",
            "forge.main.project-discovery/v2@5",
        ),
        (
            "forge.project.orchestration/v1",
            "forge.project.orchestration/v1@16",
        ),
    ] {
        sqlx::query("UPDATE operating_skill SET current_revision_id = ? WHERE skill_key = ?")
            .bind(revision)
            .bind(key)
            .execute(harness.state.db.pool())
            .await
            .unwrap();
    }
    let genesis =
        create_genesis_project(&harness.app, &common::test_jwt(), "doctrine-upgrade").await;
    let main_jobs =
        AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*harness.state.db, &genesis.main_chat_id)
            .await
            .unwrap();
    let project_jobs = AgentChatTurnJobRepo::list_agent_chat_turn_jobs(
        &*harness.state.db,
        &genesis.project_chat_id,
    )
    .await
    .unwrap();
    let main_job = main_jobs
        .iter()
        .find(|job| {
            job.operating_skill_revision_id.as_deref() == Some("forge.main.project-discovery/v2@5")
        })
        .expect("old Genesis admission");
    let project_job = project_jobs
        .iter()
        .find(|job| {
            job.operating_skill_revision_id.as_deref() == Some("forge.project.orchestration/v1@16")
        })
        .expect("old handoff admission");
    let session_before: (String, String) = sqlx::query_as(
        "SELECT prompt_revision, prompt_body FROM product_genesis_session WHERE id = ?",
    )
    .bind(&genesis.genesis_session_id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();

    // The fixture already seeds successor rows. Replay the actual migration
    // with only those duplicate inserts ignored, exercising its activation
    // and binding update against admissions made under the old pointers.
    let migration =
        include_str!("../../../../db/migrations/V202610010500__merge_friendly_doctrine.sql")
            .replace(
                "INSERT INTO operating_skill_revision",
                "INSERT OR IGNORE INTO operating_skill_revision",
            );
    sqlx::raw_sql(&migration)
        .execute(harness.state.db.pool())
        .await
        .expect("doctrine upgrade");
    let binding_revision: String = sqlx::query_scalar(
        "SELECT operating_skill_revision_id FROM project_agent_binding WHERE id = ?",
    )
    .bind(required_string(
        &genesis.create_response,
        &["project_agent_binding_id"],
    ))
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(binding_revision, "forge.project.orchestration/v1@17");
    let session_after: (String, String) = sqlx::query_as(
        "SELECT prompt_revision, prompt_body FROM product_genesis_session WHERE id = ?",
    )
    .bind(&genesis.genesis_session_id)
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(session_before, session_after);

    let runner = services_agents::FederatedAgentChatTurnRunner::new(
        Arc::clone(&harness.state.db),
        Arc::clone(&harness.state.embedded_agent_service),
        Arc::clone(&harness.state.task_executor),
        harness.state.agent_chat_turn_logs.clone(),
    );
    for job in [main_job, project_job] {
        let persisted = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &job.id)
            .await
            .unwrap()
            .expect("frozen admission retained");
        assert_eq!(&persisted, job);
        let preview = runner
            .preview_prompt(&persisted)
            .await
            .expect("frozen prompt renders");
        let body: String =
            sqlx::query_scalar("SELECT canonical_body FROM operating_skill_revision WHERE id = ?")
                .bind(persisted.operating_skill_revision_id.as_deref().unwrap())
                .fetch_one(harness.state.db.pool())
                .await
                .unwrap();
        assert!(preview.system_prompt.as_deref().unwrap().contains(&body));
        assert_preview_state_is_data(&preview);
        if job.id == main_job.id {
            let charter_version: i64 = sqlx::query_scalar(
                "SELECT charter_version FROM product_genesis_session WHERE id = ?",
            )
            .bind(&genesis.genesis_session_id)
            .fetch_one(harness.state.db.pool())
            .await
            .unwrap();
            assert!(preview.input_parts[1]
                .contains(&format!("Current Charter version: {charter_version}")));
        }
        if job.id == project_job.id {
            let manifest_id = runner
                .record_prompt_manifest_for_test(&persisted)
                .await
                .unwrap();
            let (revision, digest): (String, String) = sqlx::query_as(
                "SELECT source_revision, fragment_fingerprint FROM context_manifest_source WHERE manifest_id = ? AND source_type = 'server_operating_skill'"
            ).bind(&manifest_id).fetch_one(harness.state.db.pool()).await.unwrap();
            let frozen_digest: String = sqlx::query_scalar("SELECT content_digest FROM operating_skill_revision WHERE id = 'forge.project.orchestration/v1@16'")
                .fetch_one(harness.state.db.pool()).await.unwrap();
            assert_eq!(revision, "forge.project.orchestration/v1@16");
            assert_eq!(digest, frozen_digest);
        }
        runner
            .validate_admission_authority(&persisted)
            .await
            .expect("old revision resolves through the real worker after upgrade");
    }

    let previous_turn = admit_preview_turn(
        &harness,
        &genesis.project_chat_id,
        "Check before the terminal-denial upgrade",
    )
    .await;
    let previous_job =
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &previous_turn)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        previous_job.operating_skill_revision_id.as_deref(),
        Some("forge.project.orchestration/v1@17")
    );
    let previous_preview = runner.preview_prompt(&previous_job).await.unwrap();
    assert!(previous_preview
        .system_prompt
        .as_deref()
        .unwrap()
        .contains(services::operating_skills::LEGACY_V17_PROJECT_PROTOCOL));
    let terminal_migration =
        include_str!("../../../../db/migrations/V202610010550__chat_session_denied_operations.sql")
            .replace(
                "CREATE TABLE chat_session_denied_operation",
                "CREATE TABLE IF NOT EXISTS chat_session_denied_operation",
            )
            .replace(
                "INSERT INTO operating_skill_revision",
                "INSERT OR IGNORE INTO operating_skill_revision",
            );
    sqlx::raw_sql(&terminal_migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let retained =
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &previous_turn)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(retained, previous_job);
    let retained_preview = runner.preview_prompt(&retained).await.unwrap();
    assert_eq!(
        previous_preview.system_prompt,
        retained_preview.system_prompt
    );
    // Replay successors after the historical @18 migration, retaining frozen admissions.
    let action_migration =
        include_str!("../../../../db/migrations/V202610030100__task_action_doctrine.sql").replace(
            "INSERT INTO operating_skill_revision",
            "INSERT OR IGNORE INTO operating_skill_revision",
        );
    sqlx::raw_sql(&action_migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let layout_migration =
        include_str!("../../../../db/migrations/V202610031431__merge_friendly_layout_guidance.sql")
            .replace(
                "INSERT INTO operating_skill_revision",
                "INSERT OR IGNORE INTO operating_skill_revision",
            );
    sqlx::raw_sql(&layout_migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    // The @21 doctrine is the tail of the level-triggered wakes migration; its
    // schema changes are already applied, so replay only the doctrine section.
    let wakes_migration =
        include_str!("../../../../db/migrations/V202610042145__level_triggered_wakes.sql");
    let doctrine_start = wakes_migration
        .find("-- Immutable current doctrine")
        .expect("@21 doctrine section");
    let doctrine_migration = wakes_migration[doctrine_start..].replace(
        "INSERT INTO operating_skill_revision",
        "INSERT OR IGNORE INTO operating_skill_revision",
    );
    sqlx::raw_sql(&doctrine_migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let binding_revision: String = sqlx::query_scalar(
        "SELECT operating_skill_revision_id FROM project_agent_binding WHERE id = ?",
    )
    .bind(required_string(
        &genesis.create_response,
        &["project_agent_binding_id"],
    ))
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(binding_revision, "forge.project.orchestration/v1@21");
    let current_turn = admit_preview_turn(
        &harness,
        &genesis.project_chat_id,
        "Check after the terminal-denial upgrade",
    )
    .await;
    let current_job =
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &current_turn)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        current_job.operating_skill_revision_id.as_deref(),
        Some("forge.project.orchestration/v1@21")
    );
    let v21_body: String = sqlx::query_scalar(
        "SELECT canonical_body FROM operating_skill_revision WHERE id = 'forge.project.orchestration/v1@21'",
    )
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    let v21_preview = runner.preview_prompt(&current_job).await.unwrap();
    let v21_prompt = v21_preview.system_prompt.clone().unwrap();
    assert!(v21_prompt.contains(&v21_body));
    assert!(v21_prompt.contains("Eight verbs:"));
    assert!(v21_prompt.contains("Never access a repository Workspace"));
    assert!(v21_prompt.contains("`retry: none` is final for the turn"));

    // The registry doctrine cutover (@22). A turn admitted on @21 is still in
    // flight when the server upgrades: it keeps the exact contract it was
    // admitted on, body and override section, and is neither refused nor
    // switched to the new text.
    let registry_migration =
        include_str!("../../../../db/migrations/V202610120418__operation_registry_doctrine.sql")
            .replace(
                "INSERT INTO operating_skill_revision",
                "INSERT OR IGNORE INTO operating_skill_revision",
            );
    sqlx::raw_sql(&registry_migration)
        .execute(harness.state.db.pool())
        .await
        .unwrap();
    let binding_revision: String = sqlx::query_scalar(
        "SELECT operating_skill_revision_id FROM project_agent_binding WHERE id = ?",
    )
    .bind(required_string(
        &genesis.create_response,
        &["project_agent_binding_id"],
    ))
    .fetch_one(harness.state.db.pool())
    .await
    .unwrap();
    assert_eq!(binding_revision, "forge.project.orchestration/v1@22");
    let in_flight =
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &current_turn)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(in_flight, current_job, "the admitted turn is untouched");
    assert_eq!(
        runner
            .preview_prompt(&in_flight)
            .await
            .unwrap()
            .system_prompt
            .as_deref(),
        Some(v21_prompt.as_str()),
        "an in-flight @21 turn renders byte-identically after the cutover"
    );
    let retained_after = runner.preview_prompt(&retained).await.unwrap();
    assert_eq!(previous_preview.system_prompt, retained_after.system_prompt);

    let next_turn = admit_preview_turn(
        &harness,
        &genesis.project_chat_id,
        "Check after the registry doctrine upgrade",
    )
    .await;
    let next_job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*harness.state.db, &next_turn)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next_job.operating_skill_revision_id.as_deref(),
        Some("forge.project.orchestration/v1@22")
    );
    let next_prompt = runner
        .preview_prompt(&next_job)
        .await
        .unwrap()
        .system_prompt
        .unwrap();
    assert!(
        next_prompt.contains(services::operating_skills::canonical_project_operating_skill_body())
    );
    assert!(next_prompt.contains("copy the action call from `action_schema`"));
    assert!(next_prompt.contains("`retry: none` is final for the turn"));
    assert!(!next_prompt.contains("Never access a repository Workspace"));
    assert_eq!(next_prompt.matches("## SERVER-OWNED OVERRIDES").count(), 1);
    for job in [&retained, &in_flight, &next_job] {
        runner.validate_admission_authority(job).await.unwrap();
    }
}
