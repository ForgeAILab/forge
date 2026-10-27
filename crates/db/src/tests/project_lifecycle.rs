//! Project lifecycle repository tests.
use super::*;

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
                command_receipt: None,
                action_execution: None,
            },
            command_receipt: None,
            action_execution: None,
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
async fn legacy_non_runnable_governance_flag_does_not_block_current_charter_execution() {
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "baseline race task",
    )
    .await;
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let now = now_rfc3339();
    let charter_id = new_uuid_v4();
    let charter_revision_id = new_uuid_v4();
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
        "INSERT INTO project_task_governance
         (task_id, project_id, charter_revision_id,
          runnable, provenance_json, created_at, updated_at)
         VALUES (?, ?, ?, 1, '{}', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&charter_revision_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("runnable governance creates");

    // `runnable` was historically derived from repository readiness. Repository
    // readiness now belongs to the Project, so this legacy projection must not
    // become an independent execution-admission gate. Exact current Charter
    // provenance remains authoritative.
    sqlx::query(
        "UPDATE project_task_governance
         SET runnable = 0, version = version + 1, updated_at = ?
         WHERE task_id = ?",
    )
    .bind(&now)
    .bind(&task_id)
    .execute(db.pool())
    .await
    .expect("legacy governance readiness projection updates");

    let task_before = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reads")
        .expect("task exists");
    let mut claim_transaction = crate::begin_immediate(db.pool())
        .await
        .expect("claim transaction begins");
    let execution_id = new_uuid_v4();
    let _claim = claim_in_test_step(
        &db,
        &mut claim_transaction,
        ClaimTask {
            task_id: task_id.clone(),
            assignee_type: "agent".to_owned(),
            assignee_id: Some(agent_id.clone()),
            expected_version: task_before.version,
            source_status: "todo".to_owned(),
            target_status: "in_progress".to_owned(),
            execution_admission: None,
            expected_project_version: None,
            expected_workflow_definition: None,
            execution: CreateExecution {
                id: execution_id.clone(),
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
            execution_lease: pending_claim_lease(&execution_id, &now),
            claimed_at: now.clone(),
        },
    )
    .await
    .expect("current Charter governance admits execution despite legacy flag");
    claim_transaction
        .commit()
        .await
        .expect("claim transaction commits");
    let task_after = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task rereads")
        .expect("task remains");
    assert_eq!(task_after.version, task_before.version + 1);
    assert_eq!(task_after.status, "in_progress");
    assert_eq!(task_after.assignee_id.as_deref(), Some(agent_id.as_str()));
    let execution_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND status = 'running'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .expect("execution count reads");
    assert_eq!(
        execution_count, 1,
        "current Charter provenance mints execution"
    );
    let workspace_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace WHERE task_id = ?")
            .bind(&task_id)
            .fetch_one(db.pool())
            .await
            .expect("workspace count reads");
    assert_eq!(
        workspace_count, 1,
        "execution admission does not duplicate the prepared workspace"
    );
}

#[tokio::test]
async fn append_agent_chat_message_allocates_sequences_and_replays_by_id() {
    use crate::{AgentChatMessageAuthorType, AgentChatMessageRepo, AgentChatMessageStatus};

    let db = sqlite_db().await;
    let account_id = seed_user(&db).await;
    // The V071 user-insert trigger provisions the account Main Chat.
    let chat_id: String = sqlx::query_scalar(
        "SELECT id FROM agent_chat WHERE account_id = ? AND kind = 'account_main'",
    )
    .bind(&account_id)
    .fetch_one(db.pool())
    .await
    .expect("provisioned Main Chat");
    let count_before: i64 = sqlx::query_scalar("SELECT message_count FROM agent_chat WHERE id = ?")
        .bind(&chat_id)
        .fetch_one(db.pool())
        .await
        .expect("message count");
    let message = |id: &str| crate::CreateAgentChatMessage {
        id: id.to_owned(),
        chat_id: chat_id.clone(),
        // Deliberately wrong: the store must allocate the real sequence.
        sequence: 999,
        author_type: AgentChatMessageAuthorType::System,
        author_id: None,
        content: "Charter revision 1 proposed".to_owned(),
        content_guard_json: "{}".to_owned(),
        sensitivity: "internal".to_owned(),
        status: AgentChatMessageStatus::Complete,
        outcome: None,
        model: None,
        profile_id: None,
        session_id: None,
        context_manifest_id: None,
        token_usage_json: None,
        duration_ms: None,
        error: None,
        correlation_id: new_uuid_v4(),
        causation_id: None,
        handoff_id: None,
        source_type: "native".to_owned(),
        source_id: None,
        source_message_id: None,
        source_room_id: None,
        source_conversation_id: None,
        source_sequence: None,
        source_metadata_json: "{}".to_owned(),
        created_at: now_rfc3339(),
    };

    let first = AgentChatMessageRepo::append_agent_chat_message(&db, message("anchor-1"))
        .await
        .expect("first append");
    let second = AgentChatMessageRepo::append_agent_chat_message(&db, message("anchor-2"))
        .await
        .expect("second append");
    assert_eq!(
        second.sequence,
        first.sequence + 1,
        "sequences are allocated"
    );
    assert_ne!(
        first.sequence, 999,
        "the caller-supplied sequence is a hint only"
    );

    let replay = AgentChatMessageRepo::append_agent_chat_message(&db, message("anchor-1"))
        .await
        .expect("same-id replay");
    assert_eq!(
        replay.sequence, first.sequence,
        "replay returns the stored row"
    );

    let count_after: i64 = sqlx::query_scalar("SELECT message_count FROM agent_chat WHERE id = ?")
        .bind(&chat_id)
        .fetch_one(db.pool())
        .await
        .expect("message count");
    assert_eq!(
        count_after,
        count_before + 2,
        "a replay never consumes a sequence"
    );
}

#[tokio::test]
async fn project_delete_tears_down_genesis_chat_and_handoff_rows() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    UserRepo::create_user(
        &db,
        &User {
            id: "genesis-user".to_owned(),
            email: "genesis@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("user fixture");
    let project_id = seed_project(&db, "Genesis teardown", Some("genesis-user".to_owned())).await;

    // Account creation materializes the account's Main Chat atomically.
    let main_chat_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM agent_chat WHERE account_id = 'genesis-user' AND kind = 'account_main'",
    )
    .fetch_one(db.pool())
    .await
    .expect("account main chat");
    let project_chat_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("project chat");

    AgentRepo::create_identity_with_profile(
        &db,
        CreateAgentIdentity {
            id: "teardown-agent".to_owned(),
            name: "Teardown agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("genesis-user".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: "teardown-profile".to_owned(),
            identity_id: "teardown-agent".to_owned(),
            backend_kind: "native".to_owned(),
            executor_type: "embedded".to_owned(),
            provider: None,
            model: None,
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
    .expect("teardown identity");
    let retired_chat_scope_id = format!("{project_chat_id}#retired:chat-lcm-retired");
    for (timeline_id, scope_type, scope_id, canonical_scope_id) in [
        (
            "project-lcm",
            "project",
            project_id.as_str(),
            project_id.as_str(),
        ),
        (
            "chat-lcm",
            "agent_chat",
            project_chat_id.as_str(),
            project_chat_id.as_str(),
        ),
        (
            "chat-lcm-retired",
            "agent_chat",
            retired_chat_scope_id.as_str(),
            project_chat_id.as_str(),
        ),
    ] {
        sqlx::query(
            "INSERT INTO agent_lcm_timeline
             (id, identity_id, scope_type, scope_id, canonical_scope_id,
              authorization_revision, revision, created_at, updated_at)
             VALUES (?, 'teardown-agent', ?, ?, ?, 'auth-v1', 1, ?, ?)",
        )
        .bind(timeline_id)
        .bind(scope_type)
        .bind(scope_id)
        .bind(canonical_scope_id)
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("LCM timeline fixture");
        sqlx::query(
            "INSERT INTO agent_lcm_entry
             (timeline_id, entry_id, sequence, content_json, content_fingerprint,
              source_json, created_at)
             VALUES (?, 'entry-0', 0, '{}', 'entry-digest', '{}', ?)",
        )
        .bind(timeline_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("LCM entry fixture");
        sqlx::query(
            "INSERT INTO agent_lcm_operation
             (timeline_id, operation_id, operation_kind, operation_fingerprint,
              result_revision, result_entries, result_node_id, created_at)
             VALUES (?, 'leaf-op', 'leaf', 'leaf-op-digest', 1, 1, 'node-0', ?)",
        )
        .bind(timeline_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("LCM operation fixture");
        sqlx::query(
            "INSERT INTO agent_lcm_node
             (timeline_id, node_id, kind, range_start, range_end, edges_json,
              source_fingerprint, summary_revision, summary, policy_revision,
              algorithm_revision, sizer_revision, provenance_json, token_count,
              source_token_count, classification_json, revision, superseded_by,
              operation_id, operation_fingerprint, created_at)
             VALUES (?, 'node-0', 'leaf', 0, 0, '[]', 'source-digest',
                     'summary-v1', 'summary', 'policy-v1', 'algorithm-v1',
                     'sizer-v1', '{}', 1, 2, '{}', 1, NULL, 'leaf-op',
                     'leaf-op-digest', ?)",
        )
        .bind(timeline_id)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("LCM node fixture");
    }

    sqlx::query(
        "INSERT INTO agent_chat_instruction_revision
         (id, chat_id, revision, body, created_by_type, created_at)
         VALUES ('project-instructions', ?, 1, 'build it', 'user', ?)",
    )
    .bind(&project_chat_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("instruction fixture");

    for (id, chat) in [
        ("main-message", main_chat_id.as_str()),
        ("project-message", project_chat_id.as_str()),
    ] {
        sqlx::query(
            "INSERT INTO agent_chat_message
             (id, chat_id, sequence, author_type, author_id, content, status,
              correlation_id, created_at)
             VALUES (?, ?, 0, 'user', 'genesis-user', 'hello', 'complete', 'corr', ?)",
        )
        .bind(id)
        .bind(chat)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("message fixture");
    }

    sqlx::query(
        "INSERT INTO agent_handoff
         (id, source_chat_id, target_chat_id, content, status, correlation_id,
          dedupe_key, created_at, updated_at)
         VALUES ('genesis-handoff', ?, ?, 'take it', 'delivered',
                 'corr', 'dedupe', ?, ?)",
    )
    .bind(&main_chat_id)
    .bind(&project_chat_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("handoff fixture");
    sqlx::query(
        "INSERT INTO agent_handoff_delivery
         (handoff_id, delivery_sequence, status, created_at)
         VALUES ('genesis-handoff', 1, 'delivered', ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("delivery fixture");

    // A wake disposition RESTRICT-references the Project Chat's turn job, which
    // the Project's removal takes with it.
    sqlx::query(
        "INSERT INTO agent_chat_turn_job
         (id, chat_id, triggering_message_id, canonical_scope_type, canonical_scope_id,
          status, dedupe_key, correlation_id, created_at, updated_at)
         VALUES ('project-turn', ?, 'project-message', 'agent_chat',
                 ?, 'succeeded', 'turn-dedupe', 'corr', ?, ?)",
    )
    .bind(&project_chat_id)
    .bind(&project_chat_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("turn job fixture");
    sqlx::query(
        "INSERT INTO domain_event
         (id, event_type, entity_type, entity_id, actor_type, scope_type, scope_id,
          correlation_id, created_at)
         VALUES ('wake-event', 'agent.woke', 'agent_chat', ?, 'system',
                 'agent_chat', ?, 'corr', ?)",
    )
    .bind(&project_chat_id)
    .bind(&project_chat_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("event fixture");
    sqlx::query(
        "INSERT INTO agent_wake_disposition
         (id, consumer_name, source_event_id, source_event_sequence, attempt_number,
          max_attempts, disposition, reason, turn_job_id, created_at, updated_at)
         SELECT 'wake-disposition', 'agent-wake', 'wake-event', sequence, 1, 3,
                'turn_admitted', 'admitted', 'project-turn', ?, ?
         FROM domain_event WHERE id = 'wake-event'",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("wake disposition fixture");
    sqlx::query(
        "INSERT INTO agent_wake_disposition_current
         (consumer_name, source_event_id, disposition_id, attempt_number, updated_at)
         VALUES ('agent-wake', 'wake-event', 'wake-disposition', 1, ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("current disposition fixture");

    sqlx::query(
        "INSERT INTO product_genesis_session
         (id, account_id, main_chat_id, prompt_revision, prompt_body, maturity,
          lifecycle, project_id, handoff_id, created_at, updated_at)
         VALUES ('genesis-session', 'genesis-user', ?, 'v1', 'prompt', 'mvp',
                 'handed_off', ?, 'genesis-handoff', ?, ?)",
    )
    .bind(&main_chat_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("genesis session fixture");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("genesis-born Project tears down");

    for (table, predicate) in [
        ("project", "id = 'PROJECT'"),
        ("product_genesis_session", "id = 'genesis-session'"),
        ("agent_chat", "id = 'PROJECT_CHAT'"),
        ("agent_chat_message", "id = 'project-message'"),
        (
            "agent_chat_instruction_revision",
            "id = 'project-instructions'",
        ),
        ("agent_handoff", "id = 'genesis-handoff'"),
        ("agent_handoff_delivery", "handoff_id = 'genesis-handoff'"),
        ("agent_wake_disposition", "id = 'wake-disposition'"),
        (
            "agent_wake_disposition_current",
            "disposition_id = 'wake-disposition'",
        ),
        (
            "agent_lcm_timeline",
            "id IN ('project-lcm', 'chat-lcm', 'chat-lcm-retired')",
        ),
        (
            "agent_lcm_entry",
            "timeline_id IN ('project-lcm', 'chat-lcm', 'chat-lcm-retired')",
        ),
        (
            "agent_lcm_node",
            "timeline_id IN ('project-lcm', 'chat-lcm', 'chat-lcm-retired')",
        ),
        (
            "agent_lcm_operation",
            "timeline_id IN ('project-lcm', 'chat-lcm', 'chat-lcm-retired')",
        ),
    ] {
        let sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE {}",
            predicate
                .replace("PROJECT_CHAT", &project_chat_id)
                .replace("PROJECT", &project_id)
        );
        let count = sqlx::query_scalar::<_, i64>(&sql)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|error| panic!("{table} count: {error}"));
        assert_eq!(count, 0, "{table} row survived Project teardown");
    }

    // The account's own Chat and its history are outside the Project's scope.
    let surviving = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM agent_chat_message WHERE id = 'main-message'",
    )
    .fetch_one(db.pool())
    .await
    .expect("main message count");
    assert_eq!(surviving, 1);

    // Immutability outside the teardown is unchanged.
    assert!(
        sqlx::query("DELETE FROM agent_chat_message WHERE id = 'main-message'")
            .execute(db.pool())
            .await
            .is_err()
    );
}

// `memory_item.task_id`, `agent_chat_message.profile_id`/`handoff_id`, and
// `agent_chat_topic.starting_message_id` were `ON DELETE SET NULL` -- a
// physical UPDATE -- on tables whose own `BEFORE UPDATE` trigger aborts
// unconditionally. The moment a Project's teardown reached the task (or
// profile, or message) one of these immutable rows pointed at, SQLite's own
// cascade collided with the immutability guard and aborted the whole
// delete. `agent_chat_topic`'s `BEFORE DELETE` trigger had the same shape
// one level up: unconditional, on a table that also cascades in from
// `agent_chat`. V129 drops the FK enforcement on those columns (they become
// plain, unenforced historical references) and scopes the topic's delete
// guard to `project_deletion_guard`, matching `agent_chat_message` and
// `agent_handoff`.
#[tokio::test]
async fn project_delete_survives_immutable_rows_that_reference_a_sibling() {
    let db = sqlite_db().await;
    let now = now_rfc3339();
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(
        &db,
        &project_id,
        None,
        "todo".to_owned(),
        "Task with an attached memory",
    )
    .await;

    let mut item = memory_item(
        &project_id,
        "Observed during the task",
        "The task revealed a durable fact worth keeping after it's gone.",
    );
    item.task_id = Some(task_id.clone());
    MemoryRepository::insert_memory_item(&db, &item)
        .await
        .expect("memory item inserts");

    let project_chat_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM agent_chat WHERE project_id = ? AND kind = 'project'",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("project chat");

    sqlx::query(
        "INSERT INTO agent_chat_message
         (id, chat_id, sequence, author_type, author_id, content, status,
          correlation_id, created_at)
         VALUES ('topic-message', ?, 0, 'user', 'nobody', 'hello', 'complete', 'corr', ?)",
    )
    .bind(&project_chat_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("message fixture");

    sqlx::query(
        "INSERT INTO agent_chat_topic
         (id, chat_id, sequence, label, starting_message_id, starting_message_sequence,
          principal_type, created_at)
         VALUES ('project-topic', ?, 0, 'Kickoff', 'topic-message', 0, 'user', ?)",
    )
    .bind(&project_chat_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("topic fixture");

    ProjectRepo::delete(&db, &project_id)
        .await
        .expect("Project with immutable cross-references tears down");

    for (table, predicate) in [
        ("project", "id = 'PROJECT'".to_owned()),
        ("memory_item", format!("id = '{}'", item.id)),
        ("agent_chat_topic", "id = 'project-topic'".to_owned()),
        ("agent_chat_message", "id = 'topic-message'".to_owned()),
    ] {
        let sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE {}",
            predicate.replace("PROJECT", &project_id)
        );
        let count = sqlx::query_scalar::<_, i64>(&sql)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|error| panic!("{table} count: {error}"));
        assert_eq!(count, 0, "{table} row survived Project teardown");
    }
}
